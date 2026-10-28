//! Versioned, fail-closed recovery of execution evidence, never order replay.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::{order_id, valid_venue_id, Bracket, ExecutionCoordinator, TrackedOrder};
use crate::execution::{ExecutionError, OrderKind, OrderRef, OrderRequest, OrderSide, OrderStatus};
use crate::risk::RiskPlanCheckpoint;
use crate::{Direction, Exchange, ReservationEvent, ReservationStatus, RiskPlan};

const FORMAT_VERSION: u32 = 1;
const MAX_RECORDS: usize = 100_000;

/// Versioned execution checkpoint containing historical replay tombstones.
///
/// Fields are deliberately opaque. Deserialization alone is not validation:
/// pass the result to [`ExecutionCoordinator::restore`]. This object contains
/// order/account metadata, but no exchange credentials or admission permission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionCheckpoint {
    version: u32,
    exchange: Exchange,
    brackets: Vec<BracketCheckpoint>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BracketCheckpoint {
    plan: RiskPlanCheckpoint,
    entry_id: String,
    orders: Vec<OrderCheckpoint>,
    #[serde(with = "crate::decimal_codec::required")]
    entry_filled: Decimal,
    #[serde(with = "crate::decimal_codec::required")]
    exit_filled: Decimal,
    released: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderCheckpoint {
    request: OrderRequest,
    #[serde(deserialize_with = "required_option")]
    venue_id: Option<String>,
    #[serde(with = "crate::decimal_codec::required")]
    filled: Decimal,
    status: OrderStatus,
    #[serde(deserialize_with = "required_option")]
    last_at: Option<DateTime<Utc>>,
    cancel_requested: bool,
    #[serde(deserialize_with = "required_option")]
    sibling: Option<String>,
    #[serde(with = "crate::decimal_codec::optional")]
    protection_cumulative: Option<Decimal>,
}

// Serde normally treats a missing Option field as None. Recovery snapshots must
// distinguish an explicit null from an accidentally omitted evidence field.
fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

impl ExecutionCoordinator {
    /// Captures complete state with stable ordering for journal comparisons.
    ///
    /// Admission is intentionally omitted. Persist this together with matching
    /// risk state and an outbox before dispatching any resulting network work.
    #[must_use]
    pub fn checkpoint(&self) -> ExecutionCheckpoint {
        let mut brackets: Vec<_> = self
            .brackets
            .values()
            .map(|bracket| {
                let mut orders: Vec<_> =
                    bracket.orders.values().map(OrderCheckpoint::from).collect();
                orders.sort_by(|a, b| a.request.client_order_id.cmp(&b.request.client_order_id));
                BracketCheckpoint {
                    plan: bracket.plan.checkpoint(),
                    entry_id: bracket.entry_id.clone(),
                    orders,
                    entry_filled: bracket.entry_filled,
                    exit_filled: bracket.exit_filled,
                    released: bracket.released,
                }
            })
            .collect();
        brackets.sort_by(|a, b| a.entry_id.cmp(&b.entry_id));
        ExecutionCheckpoint {
            version: FORMAT_VERSION,
            exchange: self.exchange,
            brackets,
        }
    }

    /// Restores validated evidence with entry admission **always blocked**.
    ///
    /// Restoration never places or automatically resubmits an order. Query the
    /// references from [`Self::reconciliation_orders`] and reconcile the entire
    /// account, including orders not owned by this coordinator. A lost response
    /// is not evidence that the original command was never submitted.
    ///
    /// # Errors
    /// Rejects unsupported versions, excessive counts, duplicate identities,
    /// inconsistent geometry, cumulative fills, protection slices or tombstones.
    pub fn restore(checkpoint: ExecutionCheckpoint) -> Result<Self, ExecutionError> {
        if checkpoint.version != FORMAT_VERSION || checkpoint.brackets.len() > MAX_RECORDS {
            return Err(invalid_checkpoint());
        }
        let mut coordinator = Self::new(checkpoint.exchange);
        let mut intents = HashSet::new();
        let mut venue_ids = HashSet::new();
        let mut count = 0_usize;
        for saved in checkpoint.brackets {
            count = count
                .checked_add(saved.orders.len())
                .ok_or_else(invalid_checkpoint)?;
            if count > MAX_RECORDS {
                return Err(invalid_checkpoint());
            }
            let bracket = saved.restore(checkpoint.exchange)?;
            let approval_id = bracket.plan.approval_id();
            if !intents.insert(bracket.plan.intent_id())
                || coordinator.brackets.contains_key(&approval_id)
            {
                return Err(invalid_checkpoint());
            }
            for id in bracket.orders.keys() {
                if coordinator.owners.insert(id.clone(), approval_id).is_some() {
                    return Err(invalid_checkpoint());
                }
            }
            for order in bracket.orders.values() {
                if let Some(venue_id) = &order.venue_id {
                    let regular = order.request.kind == OrderKind::Limit;
                    if !venue_ids.insert((
                        order.request.instrument.clone(),
                        regular,
                        venue_id.clone(),
                    )) {
                        return Err(invalid_checkpoint());
                    }
                }
            }
            coordinator.brackets.insert(approval_id, bracket);
        }
        Ok(coordinator)
    }

    /// References requiring authoritative reconciliation after a gap/restart.
    ///
    /// Includes every order of an unresolved bracket, even a terminal entry:
    /// late cumulative fills can change its exposure. A fully released bracket
    /// remains a replay tombstone but does not trigger an indefinite REST query.
    /// The caller may only query these identities, never replay their placements.
    #[must_use]
    pub fn reconciliation_orders(&self) -> Vec<OrderRef> {
        let mut references: Vec<_> = self
            .brackets
            .values()
            .flat_map(|bracket| {
                bracket
                    .orders
                    .values()
                    .filter(|order| !bracket.released || !order.status.is_terminal())
                    .map(|order| order.request.reference(order.venue_id.clone()))
            })
            .collect();
        references.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
        references
    }

    pub(crate) fn checkpoint_plan_refs(&self) -> impl Iterator<Item = &RiskPlan> {
        self.brackets.values().map(|bracket| &bracket.plan)
    }

    #[must_use]
    pub(crate) fn checkpoint_reservation_statuses(&self) -> Vec<(Uuid, ReservationStatus)> {
        let mut statuses: Vec<_> = self
            .brackets
            .values()
            .map(|bracket| {
                let status = if bracket.released {
                    ReservationStatus::Released
                } else if bracket.entry_filled > Decimal::ZERO {
                    ReservationStatus::PositionOpen
                } else {
                    ReservationStatus::Pending
                };
                (bracket.plan.approval_id(), status)
            })
            .collect();
        statuses.sort_by_key(|(approval_id, _)| *approval_id);
        statuses
    }

    pub(crate) fn checkpoint_release_events(&self) -> Vec<(Uuid, Option<ReservationEvent>)> {
        let mut events: Vec<_> = self
            .brackets
            .values()
            .map(|bracket| {
                let event = bracket
                    .released
                    .then_some(if bracket.entry_filled == Decimal::ZERO {
                        ReservationEvent::EntryCancelled
                    } else {
                        ReservationEvent::PositionClosed
                    });
                (bracket.plan.intent_id(), event)
            })
            .collect();
        events.sort_by_key(|(intent_id, _)| *intent_id);
        events
    }

    #[must_use]
    pub(crate) fn checkpoint_order_requests(&self) -> Vec<OrderRequest> {
        let mut requests: Vec<_> = self
            .brackets
            .values()
            .flat_map(|bracket| bracket.orders.values().map(|order| order.request.clone()))
            .collect();
        requests.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
        requests
    }

    #[must_use]
    pub(crate) fn checkpoint_order_references(&self) -> Vec<OrderRef> {
        let mut references: Vec<_> = self
            .brackets
            .values()
            .flat_map(|bracket| {
                bracket
                    .orders
                    .values()
                    .map(|order| order.request.reference(order.venue_id.clone()))
            })
            .collect();
        references.sort_by(|left, right| left.client_order_id.cmp(&right.client_order_id));
        references
    }
}

impl From<&TrackedOrder> for OrderCheckpoint {
    fn from(order: &TrackedOrder) -> Self {
        Self {
            request: order.request.clone(),
            venue_id: order.venue_id.clone(),
            filled: order.filled,
            status: order.status,
            last_at: order.last_at,
            cancel_requested: order.cancel_requested,
            sibling: order.sibling.clone(),
            protection_cumulative: order.protection_cumulative,
        }
    }
}

impl OrderCheckpoint {
    fn restore(self, plan: &RiskPlan) -> Result<TrackedOrder, ExecutionError> {
        self.request.validate()?;
        if self.request.instrument != *plan.instrument()
            || self.filled < Decimal::ZERO
            || self.filled > self.request.quantity
            || (self.status == OrderStatus::Filled && self.filled != self.request.quantity)
            || (self.status == OrderStatus::PartiallyFilled
                && (self.filled <= Decimal::ZERO || self.filled >= self.request.quantity))
            || (self.status == OrderStatus::New && self.filled != Decimal::ZERO)
            || (self.status == OrderStatus::Triggered && self.request.kind == OrderKind::Limit)
            || self.last_at.is_some_and(|at| {
                at < plan.approved_at()
                    && (self.filled > Decimal::ZERO || self.status.is_terminal())
            })
            || (self.last_at.is_none()
                && (self.filled != Decimal::ZERO || self.status != OrderStatus::New))
            || self.venue_id.as_ref().is_some_and(|id| !valid_venue_id(id))
            || (self.request.kind == OrderKind::Limit
                && self.last_at.is_some()
                && self.venue_id.is_none())
        {
            return Err(invalid_checkpoint());
        }
        Ok(TrackedOrder {
            request: self.request,
            venue_id: self.venue_id,
            filled: self.filled,
            status: self.status,
            last_at: self.last_at,
            cancel_requested: self.cancel_requested,
            sibling: self.sibling,
            protection_cumulative: self.protection_cumulative,
        })
    }
}

impl BracketCheckpoint {
    fn restore(self, exchange: Exchange) -> Result<Bracket, ExecutionError> {
        let plan = RiskPlan::restore_checkpoint(self.plan).map_err(|_| invalid_checkpoint())?;
        if plan.instrument().exchange != exchange
            || self.entry_id != order_id(plan.approval_id(), "entry")
            || self.orders.is_empty()
            || self.entry_filled < Decimal::ZERO
            || self.entry_filled > plan.quantity()
            || self.exit_filled < Decimal::ZERO
            || self.exit_filled > self.entry_filled
        {
            return Err(invalid_checkpoint());
        }
        let mut orders = HashMap::new();
        for saved in self.orders {
            let order = saved.restore(&plan)?;
            if orders
                .insert(order.request.client_order_id.clone(), order)
                .is_some()
            {
                return Err(invalid_checkpoint());
            }
        }
        let bracket = Bracket {
            plan,
            entry_id: self.entry_id,
            orders,
            entry_filled: self.entry_filled,
            exit_filled: self.exit_filled,
            released: self.released,
        };
        validate_bracket(&bracket)?;
        Ok(bracket)
    }
}

fn validate_bracket(bracket: &Bracket) -> Result<(), ExecutionError> {
    let entry = bracket
        .orders
        .get(&bracket.entry_id)
        .ok_or_else(invalid_checkpoint)?;
    let side = match bracket.plan.direction() {
        Direction::Bullish => OrderSide::Buy,
        Direction::Bearish => OrderSide::Sell,
    };
    if entry.request.kind != OrderKind::Limit
        || entry.request.side != side
        || entry.request.reduce_only
        || entry.request.quantity != bracket.plan.quantity()
        || entry.request.price != Some(bracket.plan.entry_price())
        || entry.request.trigger_price.is_some()
        || entry.sibling.is_some()
        || entry.protection_cumulative.is_some()
        || entry.filled != bracket.entry_filled
    {
        return Err(invalid_checkpoint());
    }
    let mut slices = Vec::new();
    let mut exit_filled = Decimal::ZERO;
    for (id, order) in &bracket.orders {
        if *id == bracket.entry_id {
            continue;
        }
        validate_exit(bracket, order, side.opposite())?;
        exit_filled = exit_filled
            .checked_add(order.filled)
            .ok_or_else(invalid_checkpoint)?;
        if order.request.kind == OrderKind::StopMarket {
            slices.push((
                order.protection_cumulative.ok_or_else(invalid_checkpoint)?,
                order.request.quantity,
            ));
        }
    }
    slices.sort_by_key(|(cumulative, _)| *cumulative);
    let mut protected = Decimal::ZERO;
    for (cumulative, quantity) in slices {
        protected = protected
            .checked_add(quantity)
            .ok_or_else(invalid_checkpoint)?;
        if protected != cumulative {
            return Err(invalid_checkpoint());
        }
    }
    let terminal_and_flat = bracket.entry_filled == bracket.exit_filled
        && bracket
            .orders
            .values()
            .all(|order| order.status.is_terminal());
    if protected != bracket.entry_filled
        || exit_filled != bracket.exit_filled
        || bracket.released != terminal_and_flat
    {
        return Err(invalid_checkpoint());
    }
    Ok(())
}

fn validate_exit(
    bracket: &Bracket,
    order: &TrackedOrder,
    side: OrderSide,
) -> Result<(), ExecutionError> {
    let (role, sibling_role, trigger, sibling_kind) = match order.request.kind {
        OrderKind::StopMarket => (
            "sl",
            "tp",
            bracket.plan.stop_loss_price(),
            OrderKind::TakeProfitMarket,
        ),
        OrderKind::TakeProfitMarket => (
            "tp",
            "sl",
            bracket.plan.take_profit_price(),
            OrderKind::StopMarket,
        ),
        OrderKind::Limit | OrderKind::Market => return Err(invalid_checkpoint()),
    };
    let cumulative = order.protection_cumulative.ok_or_else(invalid_checkpoint)?;
    let expected_id = order_id(
        bracket.plan.approval_id(),
        &format!("{role}:{}", cumulative.normalize()),
    );
    let expected_sibling = order_id(
        bracket.plan.approval_id(),
        &format!("{sibling_role}:{}", cumulative.normalize()),
    );
    let sibling = bracket
        .orders
        .get(&expected_sibling)
        .ok_or_else(invalid_checkpoint)?;
    if cumulative <= Decimal::ZERO
        || cumulative > bracket.entry_filled
        || order.request.quantity > cumulative
        || order.request.client_order_id != expected_id
        || order.sibling.as_deref() != Some(expected_sibling.as_str())
        || order.request.side != side
        || !order.request.reduce_only
        || order.request.price.is_some()
        || order.request.trigger_price != Some(trigger)
        || sibling.request.kind != sibling_kind
        || sibling.sibling.as_deref() != Some(expected_id.as_str())
        || sibling.protection_cumulative != Some(cumulative)
        || sibling.request.quantity != order.request.quantity
    {
        return Err(invalid_checkpoint());
    }
    Ok(())
}

fn invalid_checkpoint() -> ExecutionError {
    ExecutionError::Protocol("invalid execution checkpoint")
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use crate::execution::coordinator::tests::{
        approval_for, at, entry, flat, placed, ready, update,
    };
    use crate::execution::coordinator::ExecutionAction;
    use crate::execution::{OrderAck, OrderCommand};

    type TestResult = Result<(), Box<dyn Error>>;

    fn round_trip(
        coordinator: &ExecutionCoordinator,
    ) -> Result<ExecutionCoordinator, Box<dyn Error>> {
        let checkpoint = serde_json::from_slice(&serde_json::to_vec(&coordinator.checkpoint())?)?;
        Ok(ExecutionCoordinator::restore(checkpoint)?)
    }

    #[test]
    fn restart_never_restores_entry_admission() -> TestResult {
        let coordinator = ready()?;
        assert!(coordinator.is_ready());
        let restored = round_trip(&coordinator)?;
        assert!(!restored.is_ready());
        assert!(restored.reconciliation_orders().is_empty());
        assert_eq!(coordinator.checkpoint(), restored.checkpoint());
        Ok(())
    }

    #[test]
    fn zero_fill_clock_skew_round_trips_but_impossible_limit_status_is_rejected() -> TestResult {
        let (_, plan) = approval_for(Exchange::Binance)?;
        let mut coordinator = ready()?;
        let entry = entry(&mut coordinator, &plan)?;
        coordinator.on_user_event(&update(&entry, OrderStatus::New, Decimal::ZERO, 299)?)?;
        assert_eq!(
            coordinator.checkpoint(),
            round_trip(&coordinator)?.checkpoint()
        );
        assert!(!coordinator.can_place(&entry));
        assert!(coordinator
            .on_user_event(&update(&entry, OrderStatus::Triggered, Decimal::ZERO, 301)?)
            .is_err());
        Ok(())
    }

    #[test]
    fn invalid_venue_ack_cannot_enter_checkpoint_state() -> TestResult {
        let (_, plan) = approval_for(Exchange::Binance)?;
        let mut coordinator = ready()?;
        let entry = entry(&mut coordinator, &plan)?;
        let before = coordinator.checkpoint();
        for venue_id in [String::new(), "x".repeat(129), "bad\nvalue".to_owned()] {
            assert!(coordinator
                .acknowledge(
                    &OrderCommand::Place(entry.clone()),
                    &OrderAck {
                        client_order_id: entry.client_order_id.clone(),
                        venue_order_id: Some(venue_id),
                        status: Some(OrderStatus::New),
                    }
                )
                .is_err());
            assert_eq!(before, coordinator.checkpoint());
        }
        Ok(())
    }

    #[test]
    fn partial_fill_recovery_protects_only_new_cumulative_quantity() -> TestResult {
        let (_, plan) = approval_for(Exchange::Binance)?;
        let mut coordinator = ready()?;
        let entry = entry(&mut coordinator, &plan)?;
        let first = update(&entry, OrderStatus::PartiallyFilled, Decimal::ONE, 301)?;
        let actions = coordinator.on_user_event(&first)?;
        assert_eq!(placed(&actions).len(), 2);
        let mut restored = round_trip(&coordinator)?;
        assert!(!restored.is_ready());
        assert_eq!(restored.reconciliation_orders().len(), 3);
        assert!(placed(&restored.on_user_event(&first)?).is_empty());
        let second = update(&entry, OrderStatus::PartiallyFilled, Decimal::TWO, 302)?;
        let actions = restored.on_user_event(&second)?;
        let exits = placed(&actions);
        assert_eq!(exits.len(), 2);
        assert!(exits.iter().all(|order| order.quantity == Decimal::ONE));
        assert!(actions
            .iter()
            .all(|action| !matches!(action, ExecutionAction::Lifecycle { .. })));
        assert_eq!(restored.reconciliation_orders().len(), 5);
        let saved = restored.checkpoint();
        assert_eq!(saved, round_trip(&restored)?.checkpoint());
        Ok(())
    }

    #[test]
    fn released_tombstone_survives_restart_and_rejects_replay() -> TestResult {
        let (_, plan) = approval_for(Exchange::Binance)?;
        let mut coordinator = ready()?;
        let entry = entry(&mut coordinator, &plan)?;
        coordinator.on_user_event(&update(&entry, OrderStatus::Cancelled, Decimal::ZERO, 301)?)?;
        let mut restored = round_trip(&coordinator)?;
        assert!(restored.reconciliation_orders().is_empty());
        assert!(restored.approve(plan.clone(), at(302)?)?.is_empty());
        let actions =
            restored.on_user_event(&update(&entry, OrderStatus::Cancelled, Decimal::ONE, 303)?)?;
        assert_eq!(placed(&actions).len(), 2);
        assert!(actions.contains(&ExecutionAction::ReconciliationRequired));
        assert_eq!(restored.reconciliation_orders().len(), 3);
        assert!(restored.reconcile_flat(flat()?, at(300)?).is_err());
        Ok(())
    }

    #[test]
    fn parent_ids_and_cancellation_state_survive_restart() -> TestResult {
        let (_, plan) = approval_for(Exchange::Okx)?;
        let mut coordinator = ExecutionCoordinator::new(Exchange::Okx);
        let mut snapshot = flat()?;
        snapshot.exchange = Exchange::Okx;
        coordinator.reconcile_flat(snapshot, at(300)?)?;
        let entry = entry(&mut coordinator, &plan)?;
        let exits = placed(&coordinator.on_user_event(&update(
            &entry,
            OrderStatus::PartiallyFilled,
            Decimal::ONE,
            301,
        )?)?);
        for (index, exit) in exits.iter().enumerate() {
            coordinator.acknowledge(
                &OrderCommand::Place(exit.clone()),
                &OrderAck {
                    client_order_id: exit.client_order_id.clone(),
                    venue_order_id: Some(format!("algo{index}")),
                    status: Some(OrderStatus::New),
                },
            )?;
        }
        coordinator.halt();
        let checkpoint = coordinator.checkpoint();
        let restored = round_trip(&coordinator)?;
        assert_eq!(checkpoint, restored.checkpoint());
        assert_eq!(restored.reconciliation_orders().len(), 3);
        assert!(restored
            .reconciliation_orders()
            .iter()
            .filter(|order| order.kind != OrderKind::Limit)
            .all(|order| order
                .venue_order_id
                .as_ref()
                .is_some_and(|id| id.starts_with("algo"))));
        Ok(())
    }

    #[test]
    fn exit_fill_recovery_deduplicates_and_releases_only_after_sibling_terminal() -> TestResult {
        let (_, plan) = approval_for(Exchange::Binance)?;
        let mut coordinator = ready()?;
        let entry = entry(&mut coordinator, &plan)?;
        let exits = placed(&coordinator.on_user_event(&update(
            &entry,
            OrderStatus::Filled,
            entry.quantity,
            301,
        )?)?);
        let stop = exits
            .iter()
            .find(|order| order.kind == OrderKind::StopMarket)
            .ok_or("missing stop")?;
        let target = exits
            .iter()
            .find(|order| order.kind == OrderKind::TakeProfitMarket)
            .ok_or("missing target")?;
        let partial = update(stop, OrderStatus::PartiallyFilled, Decimal::ONE, 302)?;
        coordinator.on_user_event(&partial)?;
        let mut restored = round_trip(&coordinator)?;
        assert!(restored.on_user_event(&partial)?.is_empty());
        let actions =
            restored.on_user_event(&update(stop, OrderStatus::Filled, stop.quantity, 303)?)?;
        assert!(actions.iter().any(|action| matches!(action,
            ExecutionAction::Dispatch(OrderCommand::Cancel(reference))
                if reference.client_order_id == target.client_order_id)));
        assert_eq!(restored.reconciliation_orders().len(), 3);
        let actions =
            restored.on_user_event(&update(target, OrderStatus::Cancelled, Decimal::ZERO, 304)?)?;
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(
                    action,
                    ExecutionAction::Lifecycle {
                        event: crate::ReservationEvent::PositionClosed,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert!(restored.reconciliation_orders().is_empty());
        assert!(round_trip(&restored)?.reconciliation_orders().is_empty());
        Ok(())
    }

    #[test]
    fn rejects_corrupt_counts_ids_geometry_and_totals() -> TestResult {
        let (_, plan) = approval_for(Exchange::Binance)?;
        let mut coordinator = ready()?;
        let entry = entry(&mut coordinator, &plan)?;
        coordinator.on_user_event(&update(
            &entry,
            OrderStatus::PartiallyFilled,
            Decimal::ONE,
            301,
        )?)?;
        let baseline = coordinator.checkpoint();
        let mut bad = baseline.clone();
        bad.version = 99;
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets.push(bad.brackets[0].clone());
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0].entry_filled = Decimal::TWO;
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0].exit_filled = Decimal::ONE;
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0].entry_id = "wrong".to_owned();
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0]
            .orders
            .retain(|order| order.request.kind != OrderKind::StopMarket);
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0]
            .orders
            .retain(|order| order.request.kind != OrderKind::Limit);
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0].orders[0].request.quantity = Decimal::new(999, 0);
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        let duplicate = bad.brackets[0].orders[0].clone();
        bad.brackets[0].orders.push(duplicate);
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0].released = true;
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline;
        let repeated = bad.brackets[0].orders[0].clone();
        bad.brackets[0].orders = vec![repeated; MAX_RECORDS + 1];
        assert!(ExecutionCoordinator::restore(bad).is_err());
        Ok(())
    }

    #[test]
    fn rejects_forged_protective_pair_and_venue_identity() -> TestResult {
        let (_, plan) = approval_for(Exchange::Binance)?;
        let mut coordinator = ready()?;
        let entry = entry(&mut coordinator, &plan)?;
        coordinator.on_user_event(&update(
            &entry,
            OrderStatus::PartiallyFilled,
            Decimal::ONE,
            301,
        )?)?;
        let baseline = coordinator.checkpoint();
        let stop_index = baseline.brackets[0]
            .orders
            .iter()
            .position(|order| order.request.kind == OrderKind::StopMarket)
            .ok_or("missing stop")?;
        let tp_index = baseline.brackets[0]
            .orders
            .iter()
            .position(|order| order.request.kind == OrderKind::TakeProfitMarket)
            .ok_or("missing target")?;
        let mut bad = baseline.clone();
        bad.brackets[0].orders[stop_index].request.side = entry.side;
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0].orders[stop_index].request.trigger_price = Some(plan.entry_price());
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0].orders[stop_index].protection_cumulative = Some(Decimal::TWO);
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0].orders[stop_index].sibling = Some(entry.client_order_id.clone());
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline.clone();
        bad.brackets[0].orders[stop_index].status = OrderStatus::Filled;
        assert!(ExecutionCoordinator::restore(bad).is_err());
        let mut bad = baseline;
        bad.brackets[0].orders[stop_index].venue_id = Some("duplicate".to_owned());
        bad.brackets[0].orders[tp_index].venue_id = Some("duplicate".to_owned());
        assert!(ExecutionCoordinator::restore(bad).is_err());
        Ok(())
    }

    #[test]
    fn checkpoint_decimal_encoding_rejects_numeric_tokens_and_rounding() -> TestResult {
        let (_, plan) = approval_for(Exchange::Binance)?;
        let mut coordinator = ready()?;
        entry(&mut coordinator, &plan)?;
        let baseline = serde_json::to_value(coordinator.checkpoint())?;
        assert_eq!(baseline["brackets"][0]["entry_filled"], "0");
        let mut bad = baseline.clone();
        bad["brackets"][0]["entry_filled"] = serde_json::json!(0);
        assert!(serde_json::from_value::<ExecutionCheckpoint>(bad).is_err());
        let mut bad = baseline.clone();
        bad["brackets"][0]["entry_filled"] =
            serde_json::json!("0.123456789012345678901234567890123456789");
        assert!(serde_json::from_value::<ExecutionCheckpoint>(bad).is_err());
        let mut bad = baseline;
        bad["brackets"][0]["orders"][0]["request"]["quantity"] = serde_json::json!(1);
        assert!(serde_json::from_value::<ExecutionCheckpoint>(bad).is_err());
        Ok(())
    }

    #[test]
    fn checkpoint_requires_explicit_nullable_evidence_fields() -> TestResult {
        let (_, plan) = approval_for(Exchange::Binance)?;
        let mut coordinator = ready()?;
        entry(&mut coordinator, &plan)?;
        let baseline = serde_json::to_value(coordinator.checkpoint())?;
        for field in ["venue_id", "last_at", "sibling", "protection_cumulative"] {
            let mut bad = baseline.clone();
            bad["brackets"][0]["orders"][0]
                .as_object_mut()
                .ok_or("missing order object")?
                .remove(field);
            assert!(
                serde_json::from_value::<ExecutionCheckpoint>(bad).is_err(),
                "omitted {field} must not silently become null"
            );
        }
        // Explicit null remains a valid representation of pre-ACK state.
        let checkpoint = serde_json::from_value(baseline)?;
        assert_eq!(
            ExecutionCoordinator::restore(checkpoint)?
                .reconciliation_orders()
                .len(),
            1
        );
        Ok(())
    }
}
