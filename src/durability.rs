//! Journal-before-delivery coordination of risk, execution and an outbox.
//!
//! No HTTP runs here. A sender must durably claim each outbox delivery before
//! transmission. Read-only recovery queries are separate from outbox delivery.
//! Recovery never retries an old placement: its outcome may be unknown. Query
//! the venue, feed cumulative evidence back, and reconcile the complete account.
//! Full checkpoints favor auditability over throughput; compaction and latency
//! benchmarking are deliberately separate deployment requirements.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::{
    execution::{
        coordinator::{
            ExecutionAction, ExecutionCheckpoint, ExecutionCoordinator, ExecutionInput,
            FlatAccountSnapshot,
        },
        ExecutionError, OrderAck, OrderCommand, OrderRef, UserEvent,
    },
    journal::{Journal, JournalError, JournalIdentity},
    risk::{RiskCheckpoint, RiskManager},
    CircuitBreakerState, InstrumentRiskSpec, LiquidityTarget, RiskConfig, RiskError,
    TradeAssessment, TradeIntent,
};

const VERSION: u16 = 1;
const MAX_OUTBOX: usize = 8192;

/// Persisted delivery progress. None of these states implies an order fill.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DeliveryState {
    /// Committed but not yet claimed by a sender in this process.
    Queued,
    /// Durably claimed; the operation may or may not have reached the exchange.
    Attempted,
    /// Recovered operation; inspect/query, never blindly resend a placement.
    RecoveryRequired,
    /// Transport response or non-network notification was handled.
    Completed,
}

/// Durable, stable-ID outbox entry, retained as a receipt tombstone after completion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxEntry {
    id: Uuid,
    action: ExecutionAction,
    state: DeliveryState,
    #[serde(deserialize_with = "required_option")]
    receipt: Option<OrderAck>,
}

impl OutboxEntry {
    /// Stable notification/operation ID for deduplication.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }
    /// Delivery status, not venue execution status.
    #[must_use]
    pub const fn state(&self) -> DeliveryState {
        self.state
    }
    /// Inspect only. Network senders must use [`DurableAccount::claim_next`].
    #[must_use]
    pub const fn action(&self) -> &ExecutionAction {
        &self.action
    }
}

/// A durably claimed operation/notification, safe to attempt once in this process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivery {
    /// Correlates the response with the outbox.
    pub id: Uuid,
    /// Risk lifecycle notifications have already been applied internally.
    pub action: ExecutionAction,
}

/// Inputs whose state and resulting side effects commit in one journal record.
#[derive(Clone, Debug)]
pub enum DurableInput {
    /// Install current venue filters for this account's venue.
    RegisterSpec(InstrumentRiskSpec),
    /// Supply an authoritative UTC baseline; recovery never invents one.
    StartDay {
        /// True start-of-day equity.
        opening_equity: Decimal,
        /// Latest account equity.
        current_equity: Decimal,
        /// Exchange observation time.
        at: DateTime<Utc>,
        /// Previously tripped state from an authoritative recovery source.
        already_tripped: bool,
    },
    /// Update equity and cancel/block entry when a risk limit is breached.
    Equity(Decimal, DateTime<Utc>),
    /// Caller verified complete account snapshot after private subscriptions.
    ReconcileFlat(FlatAccountSnapshot, DateTime<Utc>),
    /// Atomically reserve risk and prepare the entry; no direct approval bypass.
    Assess {
        /// Validated strategy intent.
        intent: Box<TradeIntent>,
        /// Opposing liquidity target.
        target: LiquidityTarget,
        /// Current decision time.
        at: DateTime<Utc>,
    },
    /// Private stream or authoritative cumulative order-query evidence.
    User(UserEvent),
    /// Expire resting entries without inferring a successful cancellation.
    Tick(DateTime<Utc>),
    /// Operator stop, lost continuity, or emergency.
    Halt,
}

#[derive(Clone)]
struct AccountState {
    risk: RiskManager,
    execution: ExecutionCoordinator,
    outbox: Vec<OutboxEntry>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountCheckpoint {
    version: u16,
    identity: JournalIdentity,
    risk: RiskCheckpoint,
    execution: ExecutionCheckpoint,
    outbox: Vec<OutboxEntry>,
}

/// Single-writer, account/environment-bound local durability boundary.
///
/// Methods perform blocking filesystem sync. Use [`Self::spawn`] from Tokio,
/// or invoke this synchronous API on a dedicated blocking thread. The account
/// directory must be operator-owned; no credentials are stored in the journal.
pub struct DurableAccount {
    journal: Journal,
    state: AccountState,
    poisoned: bool,
}

impl DurableAccount {
    /// Explicitly initializes a new account journal without replacing a file.
    ///
    /// # Errors
    /// Fails on existing path, invalid identity, lock or durability failure.
    pub fn create(
        path: impl AsRef<Path>,
        identity: JournalIdentity,
        config: RiskConfig,
    ) -> Result<Self, DurabilityError> {
        let exchange = identity.exchange();
        let journal = Journal::create(path, identity)?;
        let state = AccountState {
            risk: RiskManager::new(config),
            execution: ExecutionCoordinator::new(exchange),
            outbox: Vec::new(),
        };
        let mut account = Self {
            journal,
            state,
            poisoned: false,
        };
        account.commit(account.state.clone())?;
        Ok(account)
    }

    /// Opens an existing journal, restores all risk and tracked order state,
    /// and durably marks unresolved operations as recovery-only.
    ///
    /// Admission is always blocked. Missing/corrupt journals never create a
    /// fresh account. Recovered notifications can be redelivered by their ID;
    /// recovered network operations cannot be claimed for retransmission.
    ///
    /// # Errors
    /// Fails closed on wrong scope/version, malformed state, or storage failure.
    pub fn open(
        path: impl AsRef<Path>,
        expected: &JournalIdentity,
    ) -> Result<Self, DurabilityError> {
        let journal = Journal::open(path, expected)?;
        let checkpoint: AccountCheckpoint =
            serde_json::from_slice(journal.latest().ok_or(DurabilityError::InvalidCheckpoint)?)
                .map_err(|_| DurabilityError::InvalidCheckpoint)?;
        if checkpoint.version != VERSION || &checkpoint.identity != expected {
            return Err(DurabilityError::InvalidCheckpoint);
        }
        let mut state = AccountState {
            risk: RiskManager::restore(checkpoint.risk)?,
            execution: ExecutionCoordinator::restore(checkpoint.execution)?,
            outbox: checkpoint.outbox,
        };
        validate_state(&state, expected)?;
        for entry in &mut state.outbox {
            if entry.state != DeliveryState::Completed {
                entry.state = if matches!(entry.action, ExecutionAction::Dispatch(_)) {
                    DeliveryState::RecoveryRequired
                } else {
                    DeliveryState::Queued
                };
            }
        }
        let mut account = Self {
            journal,
            state,
            poisoned: false,
        };
        account.commit(account.state.clone())?;
        Ok(account)
    }

    /// Immutable reservation/equity state; all mutations must pass the journal.
    #[must_use]
    pub const fn risk(&self) -> &RiskManager {
        &self.state.risk
    }
    /// Immutable execution state.
    #[must_use]
    pub const fn execution(&self) -> &ExecutionCoordinator {
        &self.state.execution
    }
    /// Inspect pending deliveries and completion tombstones without sending.
    #[must_use]
    pub fn outbox(&self) -> &[OutboxEntry] {
        &self.state.outbox
    }
    /// Last acknowledged journal sequence (not an independently stored anchor).
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.journal.sequence()
    }
    /// Whether a local storage/state-encoding failure prevents further work.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Atomically commits risk/execution updates and newly queued side effects.
    ///
    /// # Errors
    /// Invalid input yields no placement. State-validation/execution failures
    /// halt the existing account; failed storage poisons this handle. Inspect
    /// the outbox after an error: a safety halt may itself have been committed.
    pub fn apply(&mut self, input: DurableInput) -> Result<u64, DurabilityError> {
        self.ensure_healthy()?;
        let mut next = self.state.clone();
        let preserve_registry_rejection = matches!(&input, DurableInput::RegisterSpec(_));
        let result = self.apply_to(&mut next, input);
        match result {
            Ok(actions) => {
                if let Err(error) = self.stage_actions(&mut next, actions) {
                    self.commit_halt()?;
                    return Err(error);
                }
                self.commit(next)
            }
            Err(error) => {
                if preserve_registry_rejection {
                    let actions = next.execution.halt();
                    if let Err(staging) = self.stage_actions(&mut next, actions) {
                        self.poisoned = true;
                        return Err(staging);
                    }
                    self.commit(next)?;
                } else {
                    self.commit_halt()?;
                }
                Err(error)
            }
        }
    }

    fn apply_to(
        &self,
        next: &mut AccountState,
        input: DurableInput,
    ) -> Result<Vec<ExecutionAction>, DurabilityError> {
        let actions = match input {
            DurableInput::RegisterSpec(spec) => {
                if spec.instrument().exchange != self.journal.identity().exchange() {
                    return Err(DurabilityError::WrongVenue);
                }
                // A rejected filter update may mark the risk registry stale;
                // apply() preserves that safety mutation even on rejection.
                next.risk.register_instrument_spec(spec)?;
                Vec::new()
            }
            DurableInput::StartDay {
                opening_equity,
                current_equity,
                at,
                already_tripped,
            } => {
                next.risk
                    .start_trading_day(opening_equity, current_equity, at, already_tripped)?;
                Vec::new()
            }
            DurableInput::Equity(equity, at) => {
                next.risk.update_equity(equity, at)?;
                Vec::new()
            }
            DurableInput::ReconcileFlat(snapshot, now) => {
                if next.outbox.iter().any(|entry| {
                    matches!(entry.action, ExecutionAction::Dispatch(_))
                        && entry.state != DeliveryState::Completed
                }) {
                    return Err(DurabilityError::RecoveryRequired);
                }
                next.execution.reconcile_flat(snapshot, now)?;
                Vec::new()
            }
            DurableInput::Assess { intent, target, at } => {
                if intent.instrument().exchange != self.journal.identity().exchange() {
                    return Err(DurabilityError::WrongVenue);
                }
                match next.risk.assess_trade(&intent, &target, at)? {
                    TradeAssessment::Approved(plan) => {
                        if next.outbox.iter().any(|entry| {
                            matches!(entry.action, ExecutionAction::Dispatch(_))
                                && entry.state != DeliveryState::Completed
                        }) {
                            return Err(DurabilityError::RecoveryRequired);
                        }
                        next.execution.approve(*plan, at)?
                    }
                    TradeAssessment::Replay { .. } => Vec::new(),
                }
            }
            DurableInput::User(event) => next.execution.on_user_event(&event)?,
            DurableInput::Tick(at) => next.execution.tick(at),
            DurableInput::Halt => next.execution.halt(),
        };
        Ok(actions)
    }

    /// Persists the attempted marker before returning a side effect to a sender.
    ///
    /// Expired/blocked queued entries become recovery-only; protective exits are
    /// still claimable in this process. No recovered network item is returned.
    /// A caller that loses the response must query, not repeat the placement.
    ///
    /// # Errors
    /// Returns storage/state errors before authorizing network transmission.
    pub fn claim_next(&mut self, now: DateTime<Utc>) -> Result<Option<Delivery>, DurabilityError> {
        self.ensure_healthy()?;
        let mut next = self.state.clone();
        let mut changed = false;
        let mut delivery = None;
        for entry in &mut next.outbox {
            if entry.state != DeliveryState::Queued {
                continue;
            }
            if let ExecutionAction::Dispatch(OrderCommand::Place(request)) = &entry.action {
                if !next.execution.can_place(request) {
                    entry.state = DeliveryState::RecoveryRequired;
                    changed = true;
                    continue;
                }
                if !request.reduce_only {
                    let valid = next.risk.validate_dispatch_time(now).is_ok()
                        && next.execution.is_ready()
                        && next.execution.checkpoint_plan_refs().any(|plan| {
                            Uuid::new_v5(&plan.approval_id(), b"entry")
                                .simple()
                                .to_string()
                                == request.client_order_id
                                && now >= plan.approved_at()
                                && now < plan.expires_at()
                        });
                    if !valid {
                        entry.state = DeliveryState::RecoveryRequired;
                        changed = true;
                        continue;
                    }
                }
            }
            entry.state = DeliveryState::Attempted;
            changed = true;
            delivery = Some(Delivery {
                id: entry.id,
                action: entry.action.clone(),
            });
            break;
        }
        if changed {
            self.commit(next)?;
        }
        Ok(delivery)
    }

    /// Records REST acceptance or authoritative query identity for an old item.
    /// This never implies a fill; cumulative query evidence is a separate input.
    ///
    /// # Errors
    /// Rejects unclaimed IDs, mismatches, and conflicting completion replays.
    pub fn acknowledge(&mut self, id: Uuid, ack: OrderAck) -> Result<u64, DurabilityError> {
        self.ensure_healthy()?;
        let mut next = self.state.clone();
        let entry = find_entry(&mut next, id)?;
        if entry.state == DeliveryState::Completed {
            return if entry.receipt.as_ref() == Some(&ack) {
                Ok(self.sequence())
            } else {
                Err(DurabilityError::ConflictingReceipt)
            };
        }
        if entry.state == DeliveryState::Queued {
            return Err(DurabilityError::UnclaimedDelivery);
        }
        let ExecutionAction::Dispatch(command) = &entry.action else {
            return Err(DurabilityError::ConflictingReceipt);
        };
        let command = command.clone();
        let actions = match next.execution.acknowledge(&command, &ack) {
            Ok(actions) => actions,
            Err(error) => {
                self.commit_halt()?;
                return Err(error.into());
            }
        };
        let entry = find_entry(&mut next, id)?;
        entry.state = DeliveryState::Completed;
        entry.receipt = Some(ack);
        if let Err(error) = self.stage_actions(&mut next, actions) {
            self.poisoned = true;
            return Err(error);
        }
        self.commit(next)
    }

    /// Records a transport/rejection failure without releasing any reservation.
    /// Queries are bounded by the external supervisor; failed queries do not
    /// recursively queue themselves. A failed placement is never retried here.
    ///
    /// # Errors
    /// Rejects unknown/unclaimed deliveries and storage failures.
    pub fn failed(&mut self, id: Uuid) -> Result<u64, DurabilityError> {
        self.ensure_healthy()?;
        let mut next = self.state.clone();
        let entry = find_entry(&mut next, id)?;
        let ExecutionAction::Dispatch(command) = &entry.action else {
            return Err(DurabilityError::ConflictingReceipt);
        };
        if entry.state == DeliveryState::Completed {
            return if entry.receipt.is_none() {
                Ok(self.sequence())
            } else {
                Err(DurabilityError::ConflictingReceipt)
            };
        }
        if entry.state == DeliveryState::Queued {
            return Err(DurabilityError::UnclaimedDelivery);
        }
        let command = command.clone();
        entry.state = DeliveryState::Completed;
        let actions = next.execution.handle(ExecutionInput::Failed(command))?;
        if let Err(error) = self.stage_actions(&mut next, actions) {
            self.poisoned = true;
            return Err(error);
        }
        self.commit(next)
    }

    /// Acknowledges a claimed lifecycle/reconciliation notification, not a REST operation.
    ///
    /// # Errors
    /// Rejects network operations and unclaimed or unknown notification IDs.
    pub fn handled(&mut self, id: Uuid) -> Result<u64, DurabilityError> {
        self.ensure_healthy()?;
        let mut next = self.state.clone();
        let entry = find_entry(&mut next, id)?;
        if matches!(entry.action, ExecutionAction::Dispatch(_)) {
            return Err(DurabilityError::ConflictingReceipt);
        }
        if entry.state == DeliveryState::Completed {
            return Ok(self.sequence());
        }
        if entry.state != DeliveryState::Attempted {
            return Err(DurabilityError::UnclaimedDelivery);
        }
        entry.state = DeliveryState::Completed;
        self.commit(next)
    }

    /// Read-only recovery queries for unresolved orders and operations, never placements.
    /// Does not prove account completeness; external orders/positions also need scanning.
    #[must_use]
    pub fn recovery_queries(&self) -> Vec<OrderCommand> {
        let mut refs: HashMap<String, OrderRef> = self
            .state
            .execution
            .reconciliation_orders()
            .into_iter()
            .map(|reference| (reference.client_order_id.clone(), reference))
            .collect();
        for entry in &self.state.outbox {
            if entry.state == DeliveryState::Completed {
                continue;
            }
            if let ExecutionAction::Dispatch(command) = &entry.action {
                let reference = match command {
                    OrderCommand::Place(request) => request.reference(None),
                    OrderCommand::Cancel(reference) | OrderCommand::Query(reference) => {
                        reference.clone()
                    }
                };
                refs.entry(reference.client_order_id.clone())
                    .or_insert(reference);
            }
        }
        let mut queries: Vec<_> = refs.into_values().map(OrderCommand::Query).collect();
        queries.sort_by(|left, right| left.client_order_id().cmp(right.client_order_id()));
        queries
    }

    fn stage_actions(
        &self,
        next: &mut AccountState,
        mut actions: Vec<ExecutionAction>,
    ) -> Result<(), DurabilityError> {
        for action in &actions {
            if let ExecutionAction::Lifecycle {
                intent_id,
                event,
                at,
                ..
            } = action
            {
                next.risk.apply_reservation_event(*intent_id, *event, *at)?;
            }
        }
        if risk_blocks(&next.risk)? && next.execution.is_ready() {
            actions.extend(next.execution.halt());
        }
        if next
            .outbox
            .len()
            .checked_add(actions.len())
            .is_none_or(|len| len > MAX_OUTBOX)
        {
            return Err(DurabilityError::Capacity);
        }
        let sequence = self
            .sequence()
            .checked_add(1)
            .ok_or(DurabilityError::Capacity)?;
        for (index, action) in actions.into_iter().enumerate() {
            let id = Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                format!(
                    "{:?}:{}:{}:{sequence}:{index}",
                    self.journal.identity().exchange(),
                    self.journal.identity().account_label(),
                    self.journal.identity().demo()
                )
                .as_bytes(),
            );
            next.outbox.push(OutboxEntry {
                id,
                action,
                state: DeliveryState::Queued,
                receipt: None,
            });
        }
        Ok(())
    }

    fn commit_halt(&mut self) -> Result<u64, DurabilityError> {
        let mut next = self.state.clone();
        let actions = next.execution.halt();
        if let Err(error) = self.stage_actions(&mut next, actions) {
            self.poisoned = true;
            return Err(error);
        }
        self.commit(next)
    }

    fn commit(&mut self, next: AccountState) -> Result<u64, DurabilityError> {
        self.ensure_healthy()?;
        if let Err(error) = validate_state(&next, self.journal.identity()) {
            self.poisoned = true;
            return Err(error);
        }
        let checkpoint = AccountCheckpoint {
            version: VERSION,
            identity: self.journal.identity().clone(),
            risk: next.risk.checkpoint(),
            execution: next.execution.checkpoint(),
            outbox: next.outbox.clone(),
        };
        let bytes = serde_json::to_vec(&checkpoint).map_err(|_| {
            self.poisoned = true;
            DurabilityError::InvalidCheckpoint
        })?;
        let sequence = self.journal.append(&bytes).map_err(|error| {
            self.poisoned = true;
            DurabilityError::Journal(error)
        })?;
        self.state = next;
        Ok(sequence)
    }

    fn ensure_healthy(&self) -> Result<(), DurabilityError> {
        if self.poisoned {
            Err(DurabilityError::Poisoned)
        } else {
            Ok(())
        }
    }

    /// Moves blocking journal I/O onto a single blocking worker with bounded input.
    /// Dropping all handles drains queued requests and closes the journal lock.
    ///
    /// # Errors
    /// Requires a Tokio runtime and a channel capacity between one and 4096.
    pub fn spawn(self, capacity: usize) -> Result<DurableHandle, DurabilityError> {
        if !(1..=4096).contains(&capacity) {
            return Err(DurabilityError::Capacity);
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| DurabilityError::WorkerUnavailable)?;
        let (sender, mut receiver) = mpsc::channel::<Work>(capacity);
        runtime.spawn_blocking(move || {
            let mut account = self;
            while let Some(work) = receiver.blocking_recv() {
                let result = account.perform(work.operation);
                let _delivered = work.reply.send(result);
            }
        });
        Ok(DurableHandle { sender })
    }

    fn perform(&mut self, operation: DurableOperation) -> Result<DurableResult, DurabilityError> {
        let sequence = match operation {
            DurableOperation::Apply(input) => self.apply(input)?,
            DurableOperation::Claim(now) => {
                return self.claim_next(now).map(DurableResult::Delivery)
            }
            DurableOperation::Acknowledge(id, ack) => self.acknowledge(id, ack)?,
            DurableOperation::Failed(id) => self.failed(id)?,
            DurableOperation::Handled(id) => self.handled(id)?,
        };
        Ok(DurableResult::Committed(sequence))
    }
}

fn find_entry(state: &mut AccountState, id: Uuid) -> Result<&mut OutboxEntry, DurabilityError> {
    state
        .outbox
        .iter_mut()
        .find(|entry| entry.id == id)
        .ok_or(DurabilityError::UnknownDelivery)
}

fn risk_blocks(risk: &RiskManager) -> Result<bool, DurabilityError> {
    match risk.circuit_breaker_state() {
        CircuitBreakerState::Armed { current_equity, .. } => {
            let limit = current_equity
                .checked_mul(risk.config().max_portfolio_risk_fraction())
                .ok_or(DurabilityError::InvalidCheckpoint)?;
            Ok(risk.active_reserved_risk()? > limit)
        }
        CircuitBreakerState::AwaitingEquity | CircuitBreakerState::Tripped { .. } => Ok(true),
    }
}

fn validate_state(state: &AccountState, identity: &JournalIdentity) -> Result<(), DurabilityError> {
    let risk_checkpoint = state.risk.checkpoint();
    let plans: Vec<_> = state.execution.checkpoint_plan_refs().collect();
    if state.execution.exchange() != identity.exchange()
        || risk_checkpoint
            .instruments()
            .any(|instrument| instrument.exchange != identity.exchange())
        || plans.len() != risk_checkpoint.plans().count()
        || plans
            .iter()
            .any(|plan| state.risk.reservation_plan(plan.intent_id()) != Some(*plan))
    {
        return Err(DurabilityError::InvalidCheckpoint);
    }
    let statuses: HashMap<_, _> = state
        .execution
        .checkpoint_reservation_statuses()
        .into_iter()
        .collect();
    for plan in state.execution.checkpoint_plan_refs() {
        if plan.instrument().exchange != identity.exchange()
            || state.risk.reservation_status(plan.intent_id())
                != statuses.get(&plan.approval_id()).copied()
        {
            return Err(DurabilityError::InvalidCheckpoint);
        }
    }
    for (intent_id, cause) in state.execution.checkpoint_release_events() {
        if state.risk.reservation_release_event(intent_id) != cause {
            return Err(DurabilityError::InvalidCheckpoint);
        }
    }
    let requests: HashMap<_, _> = state
        .execution
        .checkpoint_order_requests()
        .into_iter()
        .map(|request| (request.client_order_id.clone(), request))
        .collect();
    let references: HashMap<_, _> = state
        .execution
        .checkpoint_order_references()
        .into_iter()
        .map(|reference| (reference.client_order_id.clone(), reference))
        .collect();
    validate_outbox(state, &requests, &references)
}

fn validate_outbox(
    state: &AccountState,
    requests: &HashMap<String, crate::OrderRequest>,
    references: &HashMap<String, OrderRef>,
) -> Result<(), DurabilityError> {
    if state.outbox.len() > MAX_OUTBOX {
        return Err(DurabilityError::Capacity);
    }
    let mut ids = HashSet::new();
    for entry in &state.outbox {
        if entry.id.is_nil()
            || !ids.insert(entry.id)
            || (entry.receipt.is_some() && entry.state != DeliveryState::Completed)
        {
            return Err(DurabilityError::InvalidCheckpoint);
        }
        if let ExecutionAction::Dispatch(command) = &entry.action {
            let request = requests
                .get(command.client_order_id())
                .ok_or(DurabilityError::InvalidCheckpoint)?;
            if command.instrument() != &request.instrument {
                return Err(DurabilityError::InvalidCheckpoint);
            }
            match command {
                OrderCommand::Place(placed) if placed != request => {
                    return Err(DurabilityError::InvalidCheckpoint)
                }
                OrderCommand::Cancel(reference) | OrderCommand::Query(reference)
                    if reference.kind != request.kind
                        || reference.venue_order_id.as_ref().is_some_and(|venue_id| {
                            references
                                .get(&reference.client_order_id)
                                .and_then(|tracked| tracked.venue_order_id.as_ref())
                                != Some(venue_id)
                        }) =>
                {
                    return Err(DurabilityError::InvalidCheckpoint)
                }
                _ => {}
            }
            if let Some(ack) = &entry.receipt {
                if ack.client_order_id != command.client_order_id()
                    || ack.venue_order_id.as_ref().is_some_and(|venue_id| {
                        references
                            .get(&ack.client_order_id)
                            .and_then(|tracked| tracked.venue_order_id.as_ref())
                            != Some(venue_id)
                    })
                {
                    return Err(DurabilityError::InvalidCheckpoint);
                }
            }
        } else if let ExecutionAction::Lifecycle {
            approval_id,
            intent_id,
            at,
            ..
        } = &entry.action
        {
            let plan = state
                .risk
                .reservation_plan(*intent_id)
                .ok_or(DurabilityError::InvalidCheckpoint)?;
            if plan.approval_id() != *approval_id
                || *at < plan.approved_at()
                || entry.receipt.is_some()
            {
                return Err(DurabilityError::InvalidCheckpoint);
            }
        } else if entry.receipt.is_some() {
            return Err(DurabilityError::InvalidCheckpoint);
        }
    }
    Ok(())
}

fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Sanitized durability failure; JSON bodies and paths never appear in errors.
#[derive(Debug, thiserror::Error)]
pub enum DurabilityError {
    /// Journal storage/integrity/identity failure.
    #[error(transparent)]
    Journal(#[from] JournalError),
    /// Domain risk rejection.
    #[error(transparent)]
    Risk(#[from] RiskError),
    /// Domain execution rejection.
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    /// Invalid combined checkpoint or outbox identity/economics.
    #[error("invalid account checkpoint")]
    InvalidCheckpoint,
    /// One handle encountered uncertain storage and must not continue.
    #[error("durability handle is poisoned; recover and reconcile")]
    Poisoned,
    /// Bounded state/queue/file capacity exhausted.
    #[error("durability capacity exceeded")]
    Capacity,
    /// Command belongs to another account venue.
    #[error("account venue mismatch")]
    WrongVenue,
    /// Outstanding operations must be reconciled first.
    #[error("unresolved operations require reconciliation")]
    RecoveryRequired,
    /// Delivery ID does not exist.
    #[error("unknown delivery")]
    UnknownDelivery,
    /// Delivery was never durably claimed in this process.
    #[error("delivery not claimed")]
    UnclaimedDelivery,
    /// Duplicate response conflicts with already recorded evidence.
    #[error("conflicting delivery receipt")]
    ConflictingReceipt,
    /// Runtime/channel/worker unavailable; results may need inspection.
    #[error("durability worker unavailable")]
    WorkerUnavailable,
}

/// Operation accepted by the bounded blocking-I/O worker.
#[derive(Clone, Debug)]
pub enum DurableOperation {
    /// Apply a risk/execution input.
    Apply(DurableInput),
    /// Persist an attempted marker before yielding one side effect.
    Claim(DateTime<Utc>),
    /// Commit REST acceptance or a correlated query identity.
    Acknowledge(Uuid, OrderAck),
    /// Commit a failed/unknown network result and safety actions.
    Failed(Uuid),
    /// Complete a claimed non-network notification.
    Handled(Uuid),
}

/// Acknowledged result from the worker; cancellation of a waiting caller does
/// not roll back already committed work. Reconcile an unknown claim result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableResult {
    /// Resulting durable sequence.
    Committed(u64),
    /// One attempted delivery, or no eligible queued side effect.
    Delivery(Option<Delivery>),
}

struct Work {
    operation: DurableOperation,
    reply: oneshot::Sender<Result<DurableResult, DurabilityError>>,
}

/// Cloneable bounded sender to one serialized journal worker.
#[derive(Clone)]
pub struct DurableHandle {
    sender: mpsc::Sender<Work>,
}

impl DurableHandle {
    /// Waits for the journal commit/claim result without blocking a Tokio worker.
    ///
    /// # Errors
    /// Returns domain/storage errors or channel loss. A lost response is not
    /// proof an operation was rolled back; never repeat a placement blindly.
    pub async fn request(
        &self,
        operation: DurableOperation,
    ) -> Result<DurableResult, DurabilityError> {
        let (reply, receiver) = oneshot::channel();
        self.sender
            .send(Work { operation, reply })
            .await
            .map_err(|_| DurabilityError::WorkerUnavailable)?;
        receiver
            .await
            .map_err(|_| DurabilityError::WorkerUnavailable)?
    }
}

#[cfg(test)]
mod tests;
