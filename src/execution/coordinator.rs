//! Deterministic, single-account execution state, separate from network I/O.
//!
//! This coordinator restores tracked exposure from validated checkpoints while
//! leaving entry admission blocked until authoritative reconciliation. A
//! supervisor must durably journal commands/events before enabling live use.
//! Only one bracket at a time is admitted, while historical IDs are retained
//! to detect duplicates and delayed fills. SL/TP requests are separate,
//! reduce-only orders for each newly filled entry slice; they are not atomic.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ExecutionError, OrderAck, OrderCommand, OrderKind, OrderRequest, OrderSide, OrderStatus,
    OrderUpdate, UserEvent,
};
use crate::{Direction, Exchange, ReservationEvent, RiskPlan};

mod checkpoint;
pub use checkpoint::ExecutionCheckpoint;

/// Caller-verified, account-wide REST reconciliation taken after subscribing.
///
/// Counts must include every symbol and conditional/algo orders, not only the
/// current strategy's instrument. Do not construct this from WS position deltas.
#[derive(Clone, Copy, Debug)]
pub struct FlatAccountSnapshot {
    /// Account venue.
    pub exchange: Exchange,
    /// Time the complete snapshot was obtained.
    pub observed_at: DateTime<Utc>,
    /// Regular open orders across the account.
    pub open_orders: usize,
    /// Open conditional/algo orders across the account.
    pub open_algo_orders: usize,
    /// Number of nonzero positions across the account.
    pub nonzero_positions: usize,
    /// Confirmed one-way (net) position mode.
    pub one_way_mode: bool,
}

/// Side effects to deliver in order using bounded channels.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ExecutionAction {
    /// Submit exactly once; query, never blindly retry, after uncertainty.
    Dispatch(OrderCommand),
    /// Forward to risk and strategy lifecycle handlers.
    Lifecycle {
        /// Reservation identity.
        approval_id: Uuid,
        /// Strategy intent identity.
        intent_id: Uuid,
        /// Confirmed lifecycle event.
        event: ReservationEvent,
        /// Exchange event time.
        at: DateTime<Utc>,
    },
    /// Entry admission disabled; reconcile the account and alert an operator.
    ReconciliationRequired,
}

/// Input to the optional serial actor.
#[derive(Clone, Debug)]
pub enum ExecutionInput {
    /// Reconciled startup gate; only a genuinely flat account is supported.
    FlatSnapshot(FlatAccountSnapshot, DateTime<Utc>),
    /// Newly reserved risk approval, with dispatch-time validation.
    Approved(Box<RiskPlan>, DateTime<Utc>),
    /// Parsed private event or authoritative cumulative REST query result.
    User(UserEvent),
    /// REST acceptance (not execution evidence).
    Acknowledged(OrderCommand, OrderAck),
    /// Any uncertain/rejected operation requires inspection; no automatic retry.
    Failed(OrderCommand),
    /// Cancel pending entries after expiry; protect any later fills.
    Tick(DateTime<Utc>),
    /// Daily breaker, shutdown, lost stream, or operator stop.
    Halt,
}

#[derive(Clone, Debug)]
struct TrackedOrder {
    request: OrderRequest,
    venue_id: Option<String>,
    filled: Decimal,
    status: OrderStatus,
    last_at: Option<DateTime<Utc>>,
    cancel_requested: bool,
    sibling: Option<String>,
    // The entry cumulative fill at creation is part of each exit's stable ID.
    // Persist it explicitly: arbitrary HashMap order cannot reconstruct slices.
    protection_cumulative: Option<Decimal>,
}

impl TrackedOrder {
    fn new(request: OrderRequest) -> Self {
        Self {
            request,
            venue_id: None,
            filled: Decimal::ZERO,
            status: OrderStatus::New,
            last_at: None,
            cancel_requested: false,
            sibling: None,
            protection_cumulative: None,
        }
    }
}

#[derive(Clone, Debug)]
struct Bracket {
    plan: RiskPlan,
    entry_id: String,
    orders: HashMap<String, TrackedOrder>,
    entry_filled: Decimal,
    exit_filled: Decimal,
    released: bool,
}

impl Bracket {
    fn lifecycle(&self, event: ReservationEvent, at: DateTime<Utc>) -> ExecutionAction {
        ExecutionAction::Lifecycle {
            approval_id: self.plan.approval_id(),
            intent_id: self.plan.intent_id(),
            event,
            at,
        }
    }

    fn cancel_order(&mut self, id: &str, actions: &mut Vec<ExecutionAction>) {
        if let Some(order) = self.orders.get_mut(id) {
            if !order.status.is_terminal() && !order.cancel_requested {
                // OKX algo cancellation needs its exchange ID. ACK processing
                // retries the cancellation decision once that ID is available.
                if order.request.instrument.exchange == Exchange::Okx
                    && matches!(
                        order.request.kind,
                        OrderKind::StopMarket | OrderKind::TakeProfitMarket
                    )
                    && order.venue_id.is_none()
                {
                    return;
                }
                order.cancel_requested = true;
                actions.push(ExecutionAction::Dispatch(OrderCommand::Cancel(
                    order.request.reference(order.venue_id.clone()),
                )));
            }
        }
    }

    fn cancel_entry(&mut self, actions: &mut Vec<ExecutionAction>) {
        self.cancel_order(&self.entry_id.clone(), actions);
    }

    fn cancel_exits_if_flat(&mut self, actions: &mut Vec<ExecutionAction>) {
        if self.entry_filled > Decimal::ZERO && self.entry_filled == self.exit_filled {
            let ids: Vec<_> = self
                .orders
                .keys()
                .filter(|id| **id != self.entry_id)
                .cloned()
                .collect();
            for id in ids {
                self.cancel_order(&id, actions);
            }
        }
    }

    fn protect(
        &mut self,
        delta: Decimal,
        actions: &mut Vec<ExecutionAction>,
    ) -> Result<(), ExecutionError> {
        let side = match self.plan.direction() {
            Direction::Bullish => OrderSide::Sell,
            Direction::Bearish => OrderSide::Buy,
        };
        for (role, sibling_role, kind, trigger) in [
            (
                "sl",
                "tp",
                OrderKind::StopMarket,
                self.plan.stop_loss_price(),
            ),
            (
                "tp",
                "sl",
                OrderKind::TakeProfitMarket,
                self.plan.take_profit_price(),
            ),
        ] {
            let id = order_id(
                self.plan.approval_id(),
                &format!("{role}:{}", self.entry_filled.normalize()),
            );
            let request = OrderRequest {
                instrument: self.plan.instrument().clone(),
                client_order_id: id.clone(),
                side,
                kind,
                quantity: delta,
                price: None,
                trigger_price: Some(trigger),
                reduce_only: true,
            };
            request.validate()?;
            let mut tracked = TrackedOrder::new(request.clone());
            tracked.protection_cumulative = Some(self.entry_filled);
            tracked.sibling = Some(order_id(
                self.plan.approval_id(),
                &format!("{sibling_role}:{}", self.entry_filled.normalize()),
            ));
            self.orders.insert(id, tracked);
            actions.push(ExecutionAction::Dispatch(OrderCommand::Place(request)));
        }
        Ok(())
    }

    fn cancel_completed_pairs(
        &mut self,
        actions: &mut Vec<ExecutionAction>,
    ) -> Result<(), ExecutionError> {
        let mut completed = Vec::new();
        for (id, order) in &self.orders {
            if let Some(sibling) = order.sibling.as_ref().and_then(|id| self.orders.get(id)) {
                let filled = order
                    .filled
                    .checked_add(sibling.filled)
                    .ok_or(ExecutionError::Arithmetic)?;
                if filled >= order.request.quantity {
                    completed.push(id.clone());
                }
                if filled > order.request.quantity
                    && !actions.contains(&ExecutionAction::ReconciliationRequired)
                {
                    actions.push(ExecutionAction::ReconciliationRequired);
                }
            }
        }
        for id in completed {
            self.cancel_order(&id, actions);
        }
        Ok(())
    }

    fn release_if_complete(&mut self, at: DateTime<Utc>, actions: &mut Vec<ExecutionAction>) {
        if !self.released
            && self.entry_filled == self.exit_filled
            && self.orders.values().all(|order| order.status.is_terminal())
        {
            self.released = true;
            let event = if self.entry_filled == Decimal::ZERO {
                ReservationEvent::EntryCancelled
            } else {
                ReservationEvent::PositionClosed
            };
            actions.push(self.lifecycle(event, at));
        }
    }

    fn apply(&mut self, update: &OrderUpdate) -> Result<Vec<ExecutionAction>, ExecutionError> {
        let mut actions = Vec::new();
        let order = self
            .orders
            .get_mut(&update.client_order_id)
            .ok_or(ExecutionError::ReconciliationRequired)?;
        if update.quantity != order.request.quantity
            || update.symbol != order.request.instrument.venue_symbol.as_str()
            || !valid_venue_id(&update.venue_order_id)
            || (order.request.kind == OrderKind::Limit && update.status == OrderStatus::Triggered)
        {
            return Err(ExecutionError::Protocol(
                "order identity or quantity changed",
            ));
        }
        let regular = matches!(order.request.kind, OrderKind::Limit | OrderKind::Market);
        if regular
            && order
                .venue_id
                .as_ref()
                .is_some_and(|id| *id != update.venue_order_id)
        {
            return Err(ExecutionError::Protocol("regular order venue ID changed"));
        }
        if update.cumulative_filled < order.filled {
            // Older cumulative snapshots cannot undo a fill. A newer regression
            // is a protocol fault, not evidence that exposure disappeared.
            if order.last_at.is_some_and(|at| update.at < at) {
                return Ok(actions);
            }
            return Err(ExecutionError::Protocol("cumulative fill regressed"));
        }
        if order.last_at.is_some_and(|at| update.at < at)
            && update.cumulative_filled == order.filled
        {
            return Ok(actions);
        }
        let delta = update
            .cumulative_filled
            .checked_sub(order.filled)
            .ok_or(ExecutionError::Arithmetic)?;
        let was_terminal = order.status.is_terminal();
        order.filled = update.cumulative_filled;
        // A delayed New event may not reopen a terminal order. A late positive
        // fill, however, must always be accounted for and protected.
        if !was_terminal || update.status.is_terminal() {
            order.status = update.status;
        }
        order.last_at = Some(order.last_at.map_or(update.at, |at| at.max(update.at)));
        // An algo's triggered child can have a different ID. Only its REST
        // parent ACK/query may populate the ID used to cancel an algo order.
        if regular && order.venue_id.is_none() {
            order.venue_id = Some(update.venue_order_id.clone());
        }
        if update.client_order_id == self.entry_id {
            if delta > Decimal::ZERO {
                if self.entry_filled == Decimal::ZERO || self.released {
                    let event = if self.released && self.entry_filled > Decimal::ZERO {
                        ReservationEvent::EntryExposureReopened
                    } else {
                        ReservationEvent::EntryFilled
                    };
                    actions.push(self.lifecycle(event, update.at));
                }
                if self.released || was_terminal {
                    actions.push(ExecutionAction::ReconciliationRequired);
                }
                self.released = false;
                self.entry_filled = self
                    .entry_filled
                    .checked_add(delta)
                    .ok_or(ExecutionError::Arithmetic)?;
                self.protect(delta, &mut actions)?;
            }
        } else {
            self.exit_filled = self
                .exit_filled
                .checked_add(delta)
                .ok_or(ExecutionError::Arithmetic)?;
            if self.exit_filled > self.entry_filled {
                return Err(ExecutionError::Protocol("exit fills exceed entry fills"));
            }
            if delta > Decimal::ZERO {
                self.cancel_entry(&mut actions);
            }
            // An unexpectedly cancelled/rejected protective leg leaves exposure
            // potentially unprotected. Do not recreate it blindly.
            let pair_covered = self.pair_covered(&update.client_order_id);
            if matches!(
                update.status,
                OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Expired
            ) && !pair_covered
                && self.exit_filled < self.entry_filled
            {
                actions.push(ExecutionAction::ReconciliationRequired);
            }
        }
        self.cancel_completed_pairs(&mut actions)?;
        self.cancel_exits_if_flat(&mut actions);
        self.release_if_complete(update.at, &mut actions);
        Ok(actions)
    }

    fn pair_covered(&self, client_id: &str) -> bool {
        self.orders.get(client_id).is_some_and(|order| {
            order
                .sibling
                .as_ref()
                .and_then(|id| self.orders.get(id))
                .is_some_and(|sibling| {
                    order
                        .filled
                        .checked_add(sibling.filled)
                        .is_some_and(|filled| filled >= order.request.quantity)
                })
        })
    }
}

/// Serial in-memory execution coordinator for an exclusively owned venue account.
#[derive(Clone, Debug)]
pub struct ExecutionCoordinator {
    exchange: Exchange,
    ready: bool,
    brackets: HashMap<Uuid, Bracket>,
    owners: HashMap<String, Uuid>,
}

impl ExecutionCoordinator {
    /// Starts blocked. Private authentication alone does not unlock entries.
    #[must_use]
    pub fn new(exchange: Exchange) -> Self {
        Self {
            exchange,
            ready: false,
            brackets: HashMap::new(),
            owners: HashMap::new(),
        }
    }

    /// Whether account reconciliation permits a new entry (subject to limits).
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    /// Venue bound to this coordinator, including when no approvals exist.
    #[must_use]
    pub const fn exchange(&self) -> Exchange {
        self.exchange
    }

    pub(crate) fn can_place(&self, request: &OrderRequest) -> bool {
        let Some(bracket) = self
            .owners
            .get(&request.client_order_id)
            .and_then(|owner| self.brackets.get(owner))
        else {
            return false;
        };
        let Some(order) = bracket.orders.get(&request.client_order_id) else {
            return false;
        };
        if order.request != *request
            || order.status != OrderStatus::New
            || order.filled != Decimal::ZERO
            || order.last_at.is_some()
            || order.venue_id.is_some()
            || order.cancel_requested
            || bracket.released
        {
            return false;
        }
        match request.kind {
            OrderKind::Limit => self.ready,
            OrderKind::StopMarket | OrderKind::TakeProfitMarket => {
                bracket.entry_filled > bracket.exit_filled
                    && !bracket.pair_covered(&request.client_order_id)
            }
            OrderKind::Market => false,
        }
    }

    /// Unlocks admission only after complete, fresh, flat reconciliation.
    ///
    /// # Errors
    /// Nonflat, stale, future-dated, wrong-venue, hedge-mode snapshots and local
    /// unresolved orders are rejected. No account recovery is inferred.
    pub fn reconcile_flat(
        &mut self,
        snapshot: FlatAccountSnapshot,
        now: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        self.ready = false;
        if snapshot.exchange != self.exchange
            || !snapshot.one_way_mode
            || snapshot.open_orders != 0
            || snapshot.open_algo_orders != 0
            || snapshot.nonzero_positions != 0
            || snapshot.observed_at > now
            || now.signed_duration_since(snapshot.observed_at) > Duration::seconds(5)
            || self.brackets.values().any(|b| !b.released)
        {
            return Err(ExecutionError::ReconciliationRequired);
        }
        self.ready = true;
        Ok(())
    }

    /// Admits one immutable risk approval, retaining IDs for lifetime replay protection.
    ///
    /// # Errors
    /// Rejects stale approvals, unresolved exposure, blocked admission and wrong
    /// venue. Rejected, never-dispatched approvals remain the caller's risk
    /// reservation responsibility. At 4096 tracked IDs, rotate only after flat
    /// reconciliation and durable archival; existing exposure still gets managed.
    pub fn approve(
        &mut self,
        plan: RiskPlan,
        now: DateTime<Utc>,
    ) -> Result<Vec<ExecutionAction>, ExecutionError> {
        if self
            .brackets
            .values()
            .any(|b| b.plan.intent_id() == plan.intent_id())
        {
            return Ok(Vec::new());
        }
        if !self.ready || self.brackets.values().any(|b| !b.released) || self.owners.len() >= 4096 {
            return Err(ExecutionError::ReconciliationRequired);
        }
        if plan.instrument().exchange != self.exchange
            || now < plan.approved_at()
            || now >= plan.expires_at()
        {
            return Err(ExecutionError::InvalidRequest(
                "wrong venue or expired risk approval",
            ));
        }
        let id = order_id(plan.approval_id(), "entry");
        let request = OrderRequest {
            instrument: plan.instrument().clone(),
            client_order_id: id.clone(),
            side: match plan.direction() {
                Direction::Bullish => OrderSide::Buy,
                Direction::Bearish => OrderSide::Sell,
            },
            kind: OrderKind::Limit,
            quantity: plan.quantity(),
            price: Some(plan.entry_price()),
            trigger_price: None,
            reduce_only: false,
        };
        request.validate()?;
        self.owners.insert(id.clone(), plan.approval_id());
        self.brackets.insert(
            plan.approval_id(),
            Bracket {
                plan,
                entry_id: id.clone(),
                orders: HashMap::from([(id, TrackedOrder::new(request.clone()))]),
                entry_filled: Decimal::ZERO,
                exit_filled: Decimal::ZERO,
                released: false,
            },
        );
        Ok(vec![ExecutionAction::Dispatch(OrderCommand::Place(
            request,
        ))])
    }

    /// Records a REST ACK without changing fills, terminal state, or risk.
    ///
    /// # Errors
    /// Unknown/mismatched acknowledgements block new entries.
    pub fn acknowledge(
        &mut self,
        command: &OrderCommand,
        ack: &OrderAck,
    ) -> Result<Vec<ExecutionAction>, ExecutionError> {
        if ack.client_order_id != command.client_order_id()
            || ack
                .venue_order_id
                .as_ref()
                .is_some_and(|id| !valid_venue_id(id))
        {
            self.ready = false;
            return Err(ExecutionError::ReconciliationRequired);
        }
        let Some(owner) = self.owners.get(&ack.client_order_id) else {
            self.ready = false;
            return Err(ExecutionError::ReconciliationRequired);
        };
        let bracket = self
            .brackets
            .get_mut(owner)
            .ok_or(ExecutionError::ReconciliationRequired)?;
        let order = bracket
            .orders
            .get_mut(&ack.client_order_id)
            .ok_or(ExecutionError::ReconciliationRequired)?;
        if command.instrument() != &order.request.instrument {
            self.ready = false;
            return Err(ExecutionError::ReconciliationRequired);
        }
        if let OrderCommand::Place(request) = command {
            if request != &order.request {
                self.ready = false;
                return Err(ExecutionError::ReconciliationRequired);
            }
        }
        if let (Some(existing), Some(incoming)) = (&order.venue_id, &ack.venue_order_id) {
            if existing != incoming {
                self.ready = false;
                return Err(ExecutionError::ReconciliationRequired);
            }
        }
        if order.venue_id.is_none() {
            order.venue_id.clone_from(&ack.venue_order_id);
        }
        let mut actions = Vec::new();
        bracket.cancel_completed_pairs(&mut actions)?;
        bracket.cancel_exits_if_flat(&mut actions);
        if !self.ready {
            bracket.cancel_entry(&mut actions);
        }
        Ok(actions)
    }

    /// Applies authoritative cumulative evidence, deduplicating fill increments.
    ///
    /// # Errors
    /// Invalid events leave bracket state unchanged and block admission. Caller
    /// must alert/reconcile and may invoke [`Self::halt`] to cancel entries.
    pub fn on_user_event(
        &mut self,
        event: &UserEvent,
    ) -> Result<Vec<ExecutionAction>, ExecutionError> {
        match event {
            UserEvent::Order(update) => {
                let result = self.apply_order(update);
                if result.is_err() {
                    self.ready = false;
                }
                result
            }
            UserEvent::ReconciliationRequired => Ok(self.halt()),
            UserEvent::Position(position) => {
                // Position messages are deltas, can arrive out of order, and
                // cannot release risk. Unexpected positions require inspection.
                if position.exchange != self.exchange
                    || (position.signed_quantity != Decimal::ZERO
                        && !self.brackets.values().any(|b| {
                            !b.released
                                && b.plan.instrument().venue_symbol.as_str() == position.symbol
                                && match b.plan.direction() {
                                    Direction::Bullish => position.signed_quantity > Decimal::ZERO,
                                    Direction::Bearish => position.signed_quantity < Decimal::ZERO,
                                }
                        }))
                {
                    Ok(self.halt())
                } else {
                    Ok(Vec::new())
                }
            }
            UserEvent::Authenticated
            | UserEvent::Subscribed
            | UserEvent::Heartbeat
            | UserEvent::Ignored => Ok(Vec::new()),
        }
    }

    fn apply_order(
        &mut self,
        update: &OrderUpdate,
    ) -> Result<Vec<ExecutionAction>, ExecutionError> {
        update.validate()?;
        if update.exchange != self.exchange {
            return Err(ExecutionError::ReconciliationRequired);
        }
        let owner = *self
            .owners
            .get(&update.client_order_id)
            .ok_or(ExecutionError::ReconciliationRequired)?;
        let mut bracket = self
            .brackets
            .get(&owner)
            .ok_or(ExecutionError::ReconciliationRequired)?
            .clone();
        let mut actions = bracket.apply(update)?;
        if actions.contains(&ExecutionAction::ReconciliationRequired) {
            self.ready = false;
        }
        if !self.ready {
            bracket.cancel_entry(&mut actions);
        }
        for id in bracket.orders.keys() {
            self.owners.insert(id.clone(), owner);
        }
        self.brackets.insert(owner, bracket);
        if !self.ready {
            // Late fills of historical brackets must also stop a newer bracket.
            for other in self.brackets.values_mut() {
                other.cancel_entry(&mut actions);
            }
        }
        Ok(actions)
    }

    /// Stops entries and requests cancellation, retaining all exposure/risk state.
    /// Existing protective exits remain active and late fills are still protected.
    pub fn halt(&mut self) -> Vec<ExecutionAction> {
        self.ready = false;
        let mut actions = vec![ExecutionAction::ReconciliationRequired];
        for bracket in self.brackets.values_mut() {
            bracket.cancel_entry(&mut actions);
        }
        actions
    }

    /// Requests expiry cancellations; a cancel ACK does not release a reservation.
    pub fn tick(&mut self, now: DateTime<Utc>) -> Vec<ExecutionAction> {
        let mut actions = Vec::new();
        for bracket in self.brackets.values_mut() {
            if now >= bracket.plan.expires_at() {
                bracket.cancel_entry(&mut actions);
            }
        }
        actions
    }

    /// Explicitly retries a cancellation after a fresh, authoritative open query.
    ///
    /// Unlike placement, cancellation can be retried after reconciling its
    /// result. The supervisor owns retry limits/backoff. This method never
    /// releases risk or admits entries and always uses the stored parent ID.
    ///
    /// # Errors
    /// Rejects unknown IDs, terminal orders, or stale query observation times.
    /// `observed_at` is when REST returned, not the order's last-update time.
    pub fn retry_cancellation(
        &mut self,
        authoritative_open: &OrderUpdate,
        observed_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Vec<ExecutionAction>, ExecutionError> {
        if observed_at > now
            || now.signed_duration_since(observed_at) > Duration::seconds(5)
            || authoritative_open.at > observed_at
            || authoritative_open.status.is_terminal()
        {
            return Err(ExecutionError::ReconciliationRequired);
        }
        let mut actions = self.on_user_event(&UserEvent::Order(authoritative_open.clone()))?;
        let client_id = &authoritative_open.client_order_id;
        if actions.iter().any(|action| matches!(action, ExecutionAction::Dispatch(OrderCommand::Cancel(reference)) if reference.client_order_id == *client_id)) {
            return Ok(actions);
        }
        let owner = self
            .owners
            .get(client_id)
            .ok_or(ExecutionError::ReconciliationRequired)?;
        let bracket = self
            .brackets
            .get_mut(owner)
            .ok_or(ExecutionError::ReconciliationRequired)?;
        let order = bracket
            .orders
            .get_mut(client_id)
            .ok_or(ExecutionError::ReconciliationRequired)?;
        if order.status.is_terminal() || order.filled != authoritative_open.cumulative_filled {
            // A concurrent stream update is newer; do not retry from a stale
            // REST view. Retain any reconciliation/lifecycle actions already made.
            actions.extend(self.halt());
            return Ok(actions);
        }
        order.cancel_requested = false;
        bracket.cancel_order(client_id, &mut actions);
        Ok(actions)
    }

    /// Handles actor input synchronously; use output as a journal/outbox boundary.
    ///
    /// # Errors
    /// Returns validation failures without submitting anything to a venue.
    pub fn handle(
        &mut self,
        input: ExecutionInput,
    ) -> Result<Vec<ExecutionAction>, ExecutionError> {
        match input {
            ExecutionInput::FlatSnapshot(snapshot, now) => {
                self.reconcile_flat(snapshot, now)?;
                Ok(Vec::new())
            }
            ExecutionInput::Approved(plan, now) => self.approve(*plan, now),
            ExecutionInput::User(event) => self.on_user_event(&event),
            ExecutionInput::Acknowledged(command, ack) => self.acknowledge(&command, &ack),
            ExecutionInput::Failed(command) => {
                let mut actions = self.halt();
                // A failed query is not automatically queried again. A bounded
                // recovery supervisor chooses any subsequent retry explicitly.
                let reference = match command {
                    OrderCommand::Place(request) => Some(request.reference(None)),
                    OrderCommand::Cancel(reference) => Some(reference),
                    OrderCommand::Query(_) => None,
                };
                if let Some(reference) = reference {
                    actions.push(ExecutionAction::Dispatch(OrderCommand::Query(reference)));
                }
                Ok(actions)
            }
            ExecutionInput::Tick(now) => Ok(self.tick(now)),
            ExecutionInput::Halt => Ok(self.halt()),
        }
    }

    /// Serial actor with bounded backpressure; network execution is external.
    ///
    /// Shutdown emits cancellation requests, but the supervisor must keep its
    /// private-stream consumer alive until actual terminal confirmations arrive.
    ///
    /// # Errors
    /// Returns if downstream closes. A validation failure halts admission and
    /// emits reconciliation, leaving reservations held for supervisor recovery.
    pub async fn run(
        mut self,
        mut input: mpsc::Receiver<ExecutionInput>,
        output: mpsc::Sender<ExecutionAction>,
        shutdown: CancellationToken,
    ) -> Result<(), ExecutionError> {
        loop {
            let next = tokio::select! { biased;
                () = shutdown.cancelled() => None,
                command = input.recv() => command,
            };
            let ending = next.is_none();
            let actions = match next {
                Some(command) => match self.handle(command) {
                    Ok(actions) => actions,
                    Err(_) => self.halt(),
                },
                None => self.halt(),
            };
            for action in actions {
                tokio::time::timeout(std::time::Duration::from_secs(10), output.send(action))
                    .await
                    .map_err(|_| ExecutionError::OutputClosed)?
                    .map_err(|_| ExecutionError::OutputClosed)?;
            }
            if ending {
                return Ok(());
            }
        }
    }
}

fn order_id(approval_id: Uuid, role: &str) -> String {
    Uuid::new_v5(&approval_id, role.as_bytes())
        .simple()
        .to_string()
}

fn valid_venue_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 128 && id.bytes().all(|byte| byte.is_ascii_graphic())
}

#[cfg(test)]
pub(crate) mod tests;
