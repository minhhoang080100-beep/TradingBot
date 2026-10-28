//! Deterministic ICT strategy state machine.
//!
//! This layer consumes the atomic candle-analysis messages produced by the ICT
//! engine. It never infers a position from a generated order intent: only an
//! explicit execution lifecycle command may move a series to `InPosition`.
//!
//! With closed candles as its only market input, retracement detection is
//! necessarily post-close. A production low-latency deployment should feed an
//! intrabar price trigger into this layer or place a resting limit as soon as
//! the FVG is armed; a post-close intent does not prove the quoted price is
//! still executable.

use std::{collections::HashMap, num::NonZeroU16, time::Duration as StdDuration};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    candle_store::CandleKey,
    ict_engine::{IctDetectionBatch, IctEngineEvent, IctError},
    models::{
        Candle, Direction, DomainEvent, FairValueGap, FvgEntryMode, LiquiditySide, LiquiditySweep,
        MarketStructureShift, ModelError, SignalEvent, TradeIntent, TradeIntentInput, ZoneState,
    },
};

const STRATEGY_ALGORITHM_VERSION: &str = "ict-strategy-v1";
const CANCELLATION_RETRY_INTERVAL: ChronoDuration = ChronoDuration::seconds(5);
const INTENT_NAMESPACE: Uuid = Uuid::from_bytes([
    0x26, 0x81, 0x42, 0x0f, 0x2c, 0x8b, 0x4e, 0x75, 0xa9, 0x50, 0x2b, 0x51, 0xd4, 0x81, 0x91, 0x03,
]);

/// Public lifecycle of one independently ordered candle series.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StrategyState {
    /// No usable setup or live position exists.
    #[default]
    Idle,
    /// A sweep/setup, armed FVG, or unfilled intent is being tracked.
    WaitingForRetracement,
    /// Execution explicitly confirmed that the entry was filled.
    InPosition,
}

/// How the New York/London session classification affects entries.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum KillzonePolicy {
    /// Setup formation is unrestricted, but a touched entry outside a killzone
    /// is considered missed and the setup is discarded.
    #[default]
    EntryOnly,
    /// Killzones have no effect on setup formation or entry.
    Disabled,
}

/// Immutable strategy settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StrategyConfig {
    entry_mode: FvgEntryMode,
    time_to_live_bars: NonZeroU16,
    killzone_policy: KillzonePolicy,
}

impl StrategyConfig {
    /// Creates a strategy configuration.
    #[must_use]
    pub const fn new(
        entry_mode: FvgEntryMode,
        time_to_live_bars: NonZeroU16,
        killzone_policy: KillzonePolicy,
    ) -> Self {
        Self {
            entry_mode,
            time_to_live_bars,
            killzone_policy,
        }
    }

    /// FVG price level required for entry.
    #[must_use]
    pub const fn entry_mode(&self) -> FvgEntryMode {
        self.entry_mode
    }

    /// Number of accepted bars for which setup/order state remains usable.
    #[must_use]
    pub const fn time_to_live_bars(&self) -> NonZeroU16 {
        self.time_to_live_bars
    }

    /// Configured killzone behavior.
    #[must_use]
    pub const fn killzone_policy(&self) -> KillzonePolicy {
        self.killzone_policy
    }
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            entry_mode: FvgEntryMode::ConsequentEncroachment,
            time_to_live_bars: NonZeroU16::new(12).unwrap_or(NonZeroU16::MIN),
            killzone_policy: KillzonePolicy::EntryOnly,
        }
    }
}

/// Commands accepted by the strategy actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StrategyCommand {
    /// Atomic output from the ICT detector.
    Ict(IctEngineEvent),
    /// Execution confirmed that a previously generated entry was filled.
    EntryFilled {
        /// Candle series owning the intent.
        key: CandleKey,
        /// Filled intent identifier.
        intent_id: Uuid,
        /// Exchange-confirmed fill time.
        filled_at: DateTime<Utc>,
    },
    /// Execution confirmed cancellation or rejection of a pending entry.
    EntryCancelled {
        /// Candle series owning the intent.
        key: CandleKey,
        /// Cancelled intent identifier.
        intent_id: Uuid,
        /// Exchange-confirmed cancellation time.
        cancelled_at: DateTime<Utc>,
    },
    /// Execution confirmed that an open position was fully closed.
    PositionClosed {
        /// Candle series owning the position.
        key: CandleKey,
        /// Intent from which the position originated.
        intent_id: Uuid,
        /// Exchange-confirmed close time.
        closed_at: DateTime<Utc>,
    },
}

/// Why the strategy asks execution to cancel an outstanding entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationReason {
    /// A source-candle gap invalidated the pre-fill context.
    SeriesReset,
    /// Too many accepted bars elapsed without a fill.
    TimeToLiveExpired,
    /// Detection was ambiguous and fail-closed behavior invalidated the order.
    AmbiguousInput,
    /// The entry FVG was invalidated before the order filled.
    FairValueGapInvalidated,
    /// Execution itself reported cancellation or rejection.
    ExecutionCancelled,
}

/// Why an unfilled setup was discarded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetupDiscardReason {
    /// Too many accepted bars elapsed.
    TimeToLiveExpired,
    /// The required FVG did not form on the candle immediately after MSS.
    MissingNextCandleFvg,
    /// More than one FVG claimed the same MSS/displacement context.
    AmbiguousNextCandleFvg,
    /// Price closed through the FVG's far boundary before entry.
    FarBoundaryViolated,
    /// Entry was first reached outside every enabled killzone.
    EntryTouchedOutsideKillzone,
    /// The ICT engine reset this series after a candle gap.
    SeriesReset,
    /// The entry FVG reached a terminal invalidated state.
    FairValueGapInvalidated,
    /// A newer liquidity sweep replaced the unfinished setup.
    SupersededByNewSweep,
    /// The atomic detection batch was ambiguous or internally inconsistent.
    AmbiguousInput,
    /// Internally assembled setup evidence failed domain validation.
    InvalidSetup,
}

/// Recoverable reason for rejecting one strategy input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StrategyInputRejection {
    /// The ICT engine rejected the source candle.
    Upstream(IctError),
    /// A signal in an atomic batch belonged to another series or future time.
    BatchSeriesMismatch,
    /// More than one liquidity sweep appeared in one atomic batch.
    AmbiguousLiquiditySweeps,
    /// Structure was explicitly ambiguous or contained multiple MSS events.
    AmbiguousStructure,
    /// An FVG update conflicted with its immutable definition or lifecycle.
    ConflictingFvgUpdate,
    /// A standalone signal cannot provide the atomic ICT metadata required by
    /// this strategy.
    UnbatchedSignal,
    /// A lifecycle command did not match the current series state or intent.
    InvalidLifecycle,
    /// Complete setup evidence failed final trade-intent validation.
    InvalidTradeIntent(ModelError),
}

/// Why a live position's market-data context is no longer continuous.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataDegradationReason {
    /// The ICT engine observed a forward candle gap and reset its detector.
    SeriesReset,
    /// The analysis batch was internally ambiguous or inconsistent.
    AmbiguousInput,
}

/// Observable outputs from the strategy state machine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StrategyAction {
    /// A complete, validated setup reached its configured FVG entry.
    TradeIntentGenerated(Box<TradeIntent>),
    /// Execution should cancel an unfilled entry intent.
    CancelPendingIntent {
        /// Series owning the pending intent.
        key: CandleKey,
        /// Intent to cancel.
        intent_id: Uuid,
        /// Market or execution time of cancellation.
        at: DateTime<Utc>,
        /// Cancellation cause.
        reason: CancellationReason,
    },
    /// A pre-intent setup was safely discarded.
    SetupDiscarded {
        /// Series whose setup was discarded.
        key: CandleKey,
        /// Market time of the decision.
        at: DateTime<Utc>,
        /// Discard cause.
        reason: SetupDiscardReason,
    },
    /// One recoverable input was rejected without stopping other series.
    InputRejected {
        /// Series when it can be determined.
        key: Option<CandleKey>,
        /// Event time attached to the rejected input.
        at: DateTime<Utc>,
        /// Rejection cause.
        reason: StrategyInputRejection,
    },
    /// A live position is retained, but its market-data context is degraded.
    DataDegraded {
        /// Affected series.
        key: CandleKey,
        /// Time degradation became known.
        at: DateTime<Utc>,
        /// Degradation cause.
        reason: DataDegradationReason,
    },
    /// A pending intent became an execution-confirmed position.
    PositionOpened {
        /// Position series.
        key: CandleKey,
        /// Originating intent.
        intent_id: Uuid,
        /// Confirmed fill time.
        filled_at: DateTime<Utc>,
    },
    /// Execution confirmed that a position was fully closed.
    PositionClosed {
        /// Position series.
        key: CandleKey,
        /// Originating intent.
        intent_id: Uuid,
        /// Confirmed close time.
        closed_at: DateTime<Utc>,
    },
    /// Execution acknowledged cancellation of a pending entry.
    EntryCancellationConfirmed {
        /// Series owning the cancelled intent.
        key: CandleKey,
        /// Cancelled intent identifier.
        intent_id: Uuid,
        /// Exchange-confirmed cancellation time.
        cancelled_at: DateTime<Utc>,
    },
}

#[derive(Clone, Debug)]
struct PendingSweep {
    sweep: LiquiditySweep,
    direction: Direction,
    age_bars: u16,
}

#[derive(Clone, Debug)]
struct PendingImpulse {
    sweep: LiquiditySweep,
    mss: MarketStructureShift,
    direction: Direction,
    age_bars: u16,
}

#[derive(Clone, Debug)]
struct ArmedSetup {
    sweep: LiquiditySweep,
    mss: MarketStructureShift,
    fvg: FairValueGap,
    direction: Direction,
    age_bars: u16,
}

#[derive(Clone, Debug)]
struct PendingIntent {
    intent: TradeIntent,
    age_bars: u16,
}

#[derive(Clone, Debug)]
struct CancelRequested {
    pending: PendingIntent,
    requested_at: DateTime<Utc>,
    reason: CancellationReason,
}

#[derive(Clone, Debug)]
struct Position {
    intent: TradeIntent,
    filled_at: DateTime<Utc>,
    data_degraded: bool,
}

#[derive(Clone, Debug)]
struct CancelledIntent {
    intent: TradeIntent,
    cancelled_at: DateTime<Utc>,
    reason: CancellationReason,
}

#[derive(Clone, Copy, Debug)]
struct ClosedPosition {
    intent_id: Uuid,
    closed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
enum SeriesPhase {
    #[default]
    Idle,
    Swept(PendingSweep),
    Impulse(PendingImpulse),
    Armed(ArmedSetup),
    IntentPending(PendingIntent),
    CancelRequested(CancelRequested),
    Position(Position),
}

impl SeriesPhase {
    const fn public_state(&self) -> StrategyState {
        match self {
            Self::Idle => StrategyState::Idle,
            Self::Swept(_)
            | Self::Impulse(_)
            | Self::Armed(_)
            | Self::IntentPending(_)
            | Self::CancelRequested(_) => StrategyState::WaitingForRetracement,
            Self::Position(_) => StrategyState::InPosition,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SeriesState {
    phase: SeriesPhase,
    last_cancelled: Option<CancelledIntent>,
    last_closed: Option<ClosedPosition>,
}

#[derive(Clone, Debug, Default)]
struct NormalizedBatch {
    sweep: Option<LiquiditySweep>,
    mss: Option<MarketStructureShift>,
    fvgs: Vec<FairValueGap>,
    fvg_updates: Vec<FairValueGap>,
}

#[derive(Clone, Copy, Debug)]
struct DiscontinuityReasons {
    cancellation: CancellationReason,
    discard: SetupDiscardReason,
    degradation: DataDegradationReason,
}

enum ActorWake {
    Shutdown,
    Input(Box<StrategyCommand>),
    Expiry(DateTime<Utc>),
}

/// Stateful, exchange-neutral ICT strategy engine.
#[derive(Debug)]
pub struct StrategyEngine {
    config: StrategyConfig,
    series: HashMap<CandleKey, SeriesState>,
}

impl StrategyEngine {
    /// Creates an empty strategy engine.
    #[must_use]
    pub fn new(config: StrategyConfig) -> Self {
        Self {
            config,
            series: HashMap::new(),
        }
    }

    /// Returns the public state of one candle series.
    #[must_use]
    pub fn state(&self, key: &CandleKey) -> StrategyState {
        self.series
            .get(key)
            .map_or(StrategyState::Idle, |state| state.phase.public_state())
    }

    /// Returns an outstanding immutable intent, if execution has not yet
    /// confirmed a fill or cancellation.
    #[must_use]
    pub fn pending_intent(&self, key: &CandleKey) -> Option<&TradeIntent> {
        match self.series.get(key).map(|state| &state.phase) {
            Some(SeriesPhase::IntentPending(pending)) => Some(&pending.intent),
            Some(SeriesPhase::CancelRequested(requested)) => Some(&requested.pending.intent),
            _ => None,
        }
    }

    /// Returns whether a retained live position has degraded candle context.
    #[must_use]
    pub fn is_data_degraded(&self, key: &CandleKey) -> bool {
        matches!(
            self.series.get(key).map(|state| &state.phase),
            Some(SeriesPhase::Position(Position {
                data_degraded: true,
                ..
            }))
        )
    }

    /// Processes one atomic ICT engine output synchronously.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] only for an internal model/timestamp failure.
    /// Bad external inputs are represented as [`StrategyAction::InputRejected`]
    /// so unrelated series can continue.
    pub fn process_event(
        &mut self,
        event: IctEngineEvent,
    ) -> Result<Vec<StrategyAction>, StrategyError> {
        match event {
            IctEngineEvent::CandleAnalyzed { candle, batch } => {
                self.process_analyzed_candle(&candle, &batch)
            }
            IctEngineEvent::CandleRejected { candle, error } => {
                Ok(Self::process_rejected_candle(&candle, error))
            }
            IctEngineEvent::DuplicateIgnored { .. } => Ok(Vec::new()),
            IctEngineEvent::Passthrough(signal) => {
                let key = CandleKey {
                    instrument: signal.instrument().clone(),
                    timeframe: signal.timeframe(),
                };
                Ok(vec![StrategyAction::InputRejected {
                    key: Some(key),
                    at: signal.occurred_at(),
                    reason: StrategyInputRejection::UnbatchedSignal,
                }])
            }
        }
    }

    /// Processes an ICT or explicit execution-lifecycle command.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] for an internal model/timestamp failure.
    pub fn process_command(
        &mut self,
        command: StrategyCommand,
    ) -> Result<Vec<StrategyAction>, StrategyError> {
        match command {
            StrategyCommand::Ict(event) => self.process_event(event),
            StrategyCommand::EntryFilled {
                key,
                intent_id,
                filled_at,
            } => Ok(self.entry_filled(key, intent_id, filled_at)),
            StrategyCommand::EntryCancelled {
                key,
                intent_id,
                cancelled_at,
            } => Ok(self.entry_cancelled(key, intent_id, cancelled_at)),
            StrategyCommand::PositionClosed {
                key,
                intent_id,
                closed_at,
            } => Ok(self.position_closed(key, intent_id, closed_at)),
        }
    }

    /// Runs the strategy as a bounded Tokio-channel actor.
    ///
    /// Cancellation is graceful. Each input command is processed atomically,
    /// and its actions retain deterministic order.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] for an internal strategy failure or when the
    /// output receiver closes while the actor is active.
    pub async fn run(
        mut self,
        mut input: mpsc::Receiver<StrategyCommand>,
        output: mpsc::Sender<StrategyAction>,
        shutdown: CancellationToken,
    ) -> Result<(), StrategyError> {
        loop {
            let wake = if let Some(expires_at) = self.next_deadline() {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => ActorWake::Shutdown,
                    () = tokio::time::sleep(duration_until(expires_at, Utc::now())) => {
                        ActorWake::Expiry(Utc::now())
                    },
                    command = input.recv() => match command {
                        Some(command) => ActorWake::Input(Box::new(command)),
                        None => ActorWake::Shutdown,
                    },
                }
            } else {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => ActorWake::Shutdown,
                    command = input.recv() => match command {
                        Some(command) => ActorWake::Input(Box::new(command)),
                        None => ActorWake::Shutdown,
                    },
                }
            };

            let actions = match wake {
                ActorWake::Shutdown => return Ok(()),
                ActorWake::Input(command) => self.process_command(*command)?,
                ActorWake::Expiry(now) => self.expire_due_intents(now),
            };
            if !send_actions(&output, actions, &shutdown).await? {
                return Ok(());
            }
        }
    }

    fn next_deadline(&self) -> Option<DateTime<Utc>> {
        self.series
            .values()
            .filter_map(|state| match &state.phase {
                SeriesPhase::IntentPending(pending) => Some(pending.intent.expires_at()),
                SeriesPhase::CancelRequested(requested) => Some(cancellation_retry_at(requested)),
                _ => None,
            })
            .min()
    }

    fn expire_due_intents(&mut self, now: DateTime<Utc>) -> Vec<StrategyAction> {
        let mut due_keys: Vec<_> = self
            .series
            .iter()
            .filter_map(|(key, state)| match &state.phase {
                SeriesPhase::IntentPending(pending) if pending.intent.expires_at() <= now => {
                    Some(key.clone())
                }
                SeriesPhase::CancelRequested(requested)
                    if cancellation_retry_at(requested) <= now =>
                {
                    Some(key.clone())
                }
                _ => None,
            })
            .collect();
        due_keys.sort();

        let mut actions = Vec::with_capacity(due_keys.len());
        for key in due_keys {
            let Some(state) = self.series.get_mut(&key) else {
                continue;
            };
            let phase = std::mem::take(&mut state.phase);
            match phase {
                SeriesPhase::IntentPending(pending) if pending.intent.expires_at() <= now => {
                    let intent_id = pending.intent.id();
                    actions.push(StrategyAction::CancelPendingIntent {
                        key,
                        intent_id,
                        at: now,
                        reason: CancellationReason::TimeToLiveExpired,
                    });
                    state.phase = SeriesPhase::CancelRequested(CancelRequested {
                        pending,
                        requested_at: now,
                        reason: CancellationReason::TimeToLiveExpired,
                    });
                }
                SeriesPhase::CancelRequested(mut requested)
                    if cancellation_retry_at(&requested) <= now =>
                {
                    actions.push(StrategyAction::CancelPendingIntent {
                        key,
                        intent_id: requested.pending.intent.id(),
                        at: now,
                        reason: requested.reason,
                    });
                    requested.requested_at = now;
                    state.phase = SeriesPhase::CancelRequested(requested);
                }
                other => state.phase = other,
            }
        }
        actions
    }

    fn process_analyzed_candle(
        &mut self,
        candle: &Candle,
        batch: &IctDetectionBatch,
    ) -> Result<Vec<StrategyAction>, StrategyError> {
        if batch.is_duplicate() {
            return Ok(Vec::new());
        }

        let key = CandleKey::from_candle(candle);
        let mut state = self.series.remove(&key).unwrap_or_default();
        let mut actions = Vec::new();

        if batch.series_reset() {
            handle_discontinuity(
                &mut state,
                &key,
                candle.close_time(),
                DiscontinuityReasons {
                    cancellation: CancellationReason::SeriesReset,
                    discard: SetupDiscardReason::SeriesReset,
                    degradation: DataDegradationReason::SeriesReset,
                },
                &mut actions,
            );
        }

        let normalized = match normalize_batch(candle, batch) {
            Ok(normalized) => normalized,
            Err(reason) => {
                fail_closed_ambiguity(&mut state, &key, candle.close_time(), &reason, &mut actions);
                actions.push(StrategyAction::InputRejected {
                    key: Some(key.clone()),
                    at: candle.close_time(),
                    reason,
                });
                self.series.insert(key, state);
                return Ok(actions);
            }
        };

        apply_fvg_updates(
            &mut state,
            &key,
            candle.close_time(),
            &normalized.fvg_updates,
            &mut actions,
        );
        cancel_invalid_pending_intent(&mut state, &key, candle, &mut actions);

        if let Some(action) = advance_age(
            &mut state.phase,
            self.config.time_to_live_bars.get(),
            &key,
            candle.close_time(),
        ) {
            actions.push(action);
        }

        accept_sweep(
            &mut state,
            normalized.sweep.clone(),
            &key,
            candle.close_time(),
            &mut actions,
        );
        if let Err(error) = self.evaluate_armed_setup(&mut state, &key, candle, batch, &mut actions)
        {
            self.series.insert(key, state);
            return Err(error);
        }
        progress_impulse(&mut state, &key, candle, batch, &normalized, &mut actions);
        qualify_mss(&mut state, candle, batch, normalized.mss);

        self.series.insert(key, state);
        Ok(actions)
    }

    fn evaluate_armed_setup(
        &self,
        state: &mut SeriesState,
        key: &CandleKey,
        candle: &Candle,
        batch: &IctDetectionBatch,
        actions: &mut Vec<StrategyAction>,
    ) -> Result<(), StrategyError> {
        let phase = std::mem::take(&mut state.phase);
        let SeriesPhase::Armed(armed) = phase else {
            state.phase = phase;
            return Ok(());
        };

        if candle.open_time() <= armed.fvg.formed_from()[2] {
            state.phase = SeriesPhase::Armed(armed);
            return Ok(());
        }

        if far_boundary_violated(&armed, candle) {
            actions.push(StrategyAction::SetupDiscarded {
                key: key.clone(),
                at: candle.close_time(),
                reason: SetupDiscardReason::FarBoundaryViolated,
            });
            return Ok(());
        }

        if !entry_reached(&armed, candle, self.config.entry_mode) {
            state.phase = SeriesPhase::Armed(armed);
            return Ok(());
        }

        if self.config.killzone_policy == KillzonePolicy::EntryOnly && batch.killzone().is_none() {
            actions.push(StrategyAction::SetupDiscarded {
                key: key.clone(),
                at: candle.close_time(),
                reason: SetupDiscardReason::EntryTouchedOutsideKillzone,
            });
            return Ok(());
        }

        let expires_at = match intent_expiry(candle, self.config.time_to_live_bars) {
            Ok(expires_at) => expires_at,
            Err(error) => {
                state.phase = SeriesPhase::Armed(armed);
                return Err(error);
            }
        };
        let intent_result = TradeIntent::new(TradeIntentInput {
            id: trade_intent_id(&armed, candle, self.config.entry_mode, expires_at),
            instrument: candle.instrument().clone(),
            timeframe: candle.timeframe(),
            direction: armed.direction,
            liquidity_sweep: armed.sweep.clone(),
            market_structure_shift: armed.mss.clone(),
            fair_value_gap: armed.fvg.clone(),
            entry_mode: self.config.entry_mode,
            trigger_candle: candle.clone(),
            expires_at,
        });
        let intent = match intent_result {
            Ok(intent) => intent,
            Err(error) => {
                actions.push(StrategyAction::SetupDiscarded {
                    key: key.clone(),
                    at: candle.close_time(),
                    reason: SetupDiscardReason::InvalidSetup,
                });
                actions.push(StrategyAction::InputRejected {
                    key: Some(key.clone()),
                    at: candle.close_time(),
                    reason: StrategyInputRejection::InvalidTradeIntent(error),
                });
                return Ok(());
            }
        };
        actions.push(StrategyAction::TradeIntentGenerated(Box::new(
            intent.clone(),
        )));
        state.phase = SeriesPhase::IntentPending(PendingIntent {
            intent,
            age_bars: 0,
        });
        Ok(())
    }

    fn process_rejected_candle(candle: &Candle, error: IctError) -> Vec<StrategyAction> {
        let key = CandleKey::from_candle(candle);
        vec![StrategyAction::InputRejected {
            key: Some(key),
            at: candle.close_time(),
            reason: StrategyInputRejection::Upstream(error),
        }]
    }

    fn entry_filled(
        &mut self,
        key: CandleKey,
        intent_id: Uuid,
        filled_at: DateTime<Utc>,
    ) -> Vec<StrategyAction> {
        let mut state = self.series.remove(&key).unwrap_or_default();
        let phase = std::mem::take(&mut state.phase);
        let mut actions = Vec::new();
        match phase {
            SeriesPhase::IntentPending(pending)
                if pending.intent.id() == intent_id
                    && filled_at >= pending.intent.triggered_at() =>
            {
                open_position(
                    &mut state,
                    &key,
                    pending.intent,
                    filled_at,
                    None,
                    &mut actions,
                );
            }
            SeriesPhase::CancelRequested(requested)
                if requested.pending.intent.id() == intent_id
                    && filled_at >= requested.pending.intent.triggered_at() =>
            {
                let degradation = cancellation_degradation(requested.reason);
                open_position(
                    &mut state,
                    &key,
                    requested.pending.intent,
                    filled_at,
                    degradation,
                    &mut actions,
                );
            }
            SeriesPhase::Position(position) if position.intent.id() == intent_id => {
                state.phase = SeriesPhase::Position(position);
            }
            other => {
                state.phase = other;
                if state
                    .last_closed
                    .is_some_and(|closed| closed.intent_id == intent_id)
                {
                    // Idempotent replay after the position already closed.
                } else {
                    let cancelled = state.last_cancelled.take();
                    match cancelled {
                        Some(cancelled)
                            if cancelled.intent.id() == intent_id
                                && filled_at >= cancelled.intent.triggered_at()
                                && filled_at <= cancelled.cancelled_at =>
                        {
                            let degradation = cancellation_degradation(cancelled.reason);
                            open_position(
                                &mut state,
                                &key,
                                cancelled.intent,
                                filled_at,
                                degradation,
                                &mut actions,
                            );
                        }
                        other_cancelled => {
                            state.last_cancelled = other_cancelled;
                            actions.push(lifecycle_rejection(key.clone(), filled_at));
                        }
                    }
                }
            }
        }
        self.series.insert(key, state);
        actions
    }

    fn entry_cancelled(
        &mut self,
        key: CandleKey,
        intent_id: Uuid,
        cancelled_at: DateTime<Utc>,
    ) -> Vec<StrategyAction> {
        let mut state = self.series.remove(&key).unwrap_or_default();
        let phase = std::mem::take(&mut state.phase);
        let mut actions = Vec::new();
        match phase {
            SeriesPhase::IntentPending(pending)
                if pending.intent.id() == intent_id
                    && cancelled_at >= pending.intent.triggered_at() =>
            {
                state.last_cancelled = Some(CancelledIntent {
                    intent: pending.intent,
                    cancelled_at,
                    reason: CancellationReason::ExecutionCancelled,
                });
                actions.push(StrategyAction::EntryCancellationConfirmed {
                    key: key.clone(),
                    intent_id,
                    cancelled_at,
                });
            }
            SeriesPhase::CancelRequested(requested)
                if requested.pending.intent.id() == intent_id
                    && cancelled_at >= requested.requested_at =>
            {
                state.last_cancelled = Some(CancelledIntent {
                    intent: requested.pending.intent,
                    cancelled_at,
                    reason: requested.reason,
                });
                actions.push(StrategyAction::EntryCancellationConfirmed {
                    key: key.clone(),
                    intent_id,
                    cancelled_at,
                });
            }
            SeriesPhase::Position(position) if position.intent.id() == intent_id => {
                state.phase = SeriesPhase::Position(position);
            }
            other => {
                state.phase = other;
                let already_terminal = state
                    .last_cancelled
                    .as_ref()
                    .is_some_and(|cancelled| cancelled.intent.id() == intent_id)
                    || state
                        .last_closed
                        .is_some_and(|closed| closed.intent_id == intent_id);
                if !already_terminal {
                    actions.push(lifecycle_rejection(key.clone(), cancelled_at));
                }
            }
        }
        self.series.insert(key, state);
        actions
    }

    fn position_closed(
        &mut self,
        key: CandleKey,
        intent_id: Uuid,
        closed_at: DateTime<Utc>,
    ) -> Vec<StrategyAction> {
        let mut state = self.series.remove(&key).unwrap_or_default();
        let phase = std::mem::take(&mut state.phase);
        let mut actions = Vec::new();
        match phase {
            SeriesPhase::Position(position)
                if position.intent.id() == intent_id && closed_at >= position.filled_at =>
            {
                state.last_closed = Some(ClosedPosition {
                    intent_id,
                    closed_at,
                });
                actions.push(StrategyAction::PositionClosed {
                    key: key.clone(),
                    intent_id,
                    closed_at,
                });
            }
            other => {
                state.phase = other;
                let replay = state.last_closed.is_some_and(|closed| {
                    closed.intent_id == intent_id && closed_at >= closed.closed_at
                });
                if !replay {
                    actions.push(lifecycle_rejection(key.clone(), closed_at));
                }
            }
        }
        self.series.insert(key, state);
        actions
    }
}

fn duration_until(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> StdDuration {
    (expires_at - now).to_std().unwrap_or(StdDuration::ZERO)
}

fn cancellation_retry_at(requested: &CancelRequested) -> DateTime<Utc> {
    requested
        .requested_at
        .checked_add_signed(CANCELLATION_RETRY_INTERVAL)
        .unwrap_or(requested.requested_at)
}

async fn send_actions(
    output: &mpsc::Sender<StrategyAction>,
    actions: Vec<StrategyAction>,
    shutdown: &CancellationToken,
) -> Result<bool, StrategyError> {
    for action in actions {
        let sent = tokio::select! {
            () = shutdown.cancelled() => false,
            result = output.send(action) => {
                result.map_err(|_source| StrategyError::OutputChannelClosed)?;
                true
            }
        };
        if !sent {
            return Ok(false);
        }
    }
    Ok(true)
}

fn open_position(
    state: &mut SeriesState,
    key: &CandleKey,
    intent: TradeIntent,
    filled_at: DateTime<Utc>,
    degradation: Option<DataDegradationReason>,
    actions: &mut Vec<StrategyAction>,
) {
    let intent_id = intent.id();
    state.last_cancelled = None;
    state.phase = SeriesPhase::Position(Position {
        intent,
        filled_at,
        data_degraded: degradation.is_some(),
    });
    actions.push(StrategyAction::PositionOpened {
        key: key.clone(),
        intent_id,
        filled_at,
    });
    if let Some(reason) = degradation {
        actions.push(StrategyAction::DataDegraded {
            key: key.clone(),
            at: filled_at,
            reason,
        });
    }
}

fn normalize_batch(
    candle: &Candle,
    batch: &IctDetectionBatch,
) -> Result<NormalizedBatch, StrategyInputRejection> {
    if batch.structure_ambiguous() {
        return Err(StrategyInputRejection::AmbiguousStructure);
    }

    let mut sweeps = Vec::new();
    let mut shifts = Vec::new();
    let mut fvgs = Vec::new();
    let mut fvg_updates = Vec::new();
    for signal in batch.signals() {
        if signal.instrument() != candle.instrument()
            || signal.timeframe() != candle.timeframe()
            || signal.occurred_at() > candle.close_time()
        {
            return Err(StrategyInputRejection::BatchSeriesMismatch);
        }
        if let SignalEvent::LiquiditySwept(sweep) = signal {
            sweeps.push(sweep.clone());
        }
        if let SignalEvent::MarketStructureShiftDetected(mss) = signal {
            shifts.push(mss.clone());
        }
        if let SignalEvent::FairValueGapDetected(fvg) = signal {
            fvgs.push(fvg.clone());
        }
        if let SignalEvent::FairValueGapUpdated(fvg) = signal {
            fvg_updates.push(fvg.clone());
        }
    }

    if sweeps.len() > 1 {
        return Err(StrategyInputRejection::AmbiguousLiquiditySweeps);
    }
    if shifts.len() > 1 {
        return Err(StrategyInputRejection::AmbiguousStructure);
    }

    Ok(NormalizedBatch {
        sweep: sweeps.pop(),
        mss: shifts.pop(),
        fvgs,
        fvg_updates,
    })
}

fn apply_fvg_updates(
    state: &mut SeriesState,
    key: &CandleKey,
    at: DateTime<Utc>,
    updates: &[FairValueGap],
    actions: &mut Vec<StrategyAction>,
) {
    let phase = std::mem::take(&mut state.phase);
    match phase {
        SeriesPhase::Armed(mut armed) => match validated_fvg_update(&armed.fvg, updates) {
            Err(reason) => {
                actions.push(discarded(key, at, SetupDiscardReason::AmbiguousInput));
                actions.push(StrategyAction::InputRejected {
                    key: Some(key.clone()),
                    at,
                    reason,
                });
            }
            Ok(update) => {
                if let Some(update) = update {
                    armed.fvg = update.clone();
                }
                if matches!(armed.fvg.state(), ZoneState::Invalidated { .. }) {
                    actions.push(discarded(
                        key,
                        at,
                        SetupDiscardReason::FairValueGapInvalidated,
                    ));
                } else {
                    state.phase = SeriesPhase::Armed(armed);
                }
            }
        },
        SeriesPhase::IntentPending(pending) => {
            match validated_fvg_update(pending.intent.fair_value_gap(), updates) {
                Err(reason) => {
                    request_intent_cancellation(
                        state,
                        key,
                        pending,
                        at,
                        CancellationReason::AmbiguousInput,
                        actions,
                    );
                    actions.push(StrategyAction::InputRejected {
                        key: Some(key.clone()),
                        at,
                        reason,
                    });
                }
                Ok(Some(update)) if matches!(update.state(), ZoneState::Invalidated { .. }) => {
                    request_intent_cancellation(
                        state,
                        key,
                        pending,
                        at,
                        CancellationReason::FairValueGapInvalidated,
                        actions,
                    );
                }
                Ok(_) => state.phase = SeriesPhase::IntentPending(pending),
            }
        }
        other => state.phase = other,
    }
}

fn validated_fvg_update<'a>(
    current: &FairValueGap,
    updates: &'a [FairValueGap],
) -> Result<Option<&'a FairValueGap>, StrategyInputRejection> {
    let mut matching = updates.iter().filter(|update| update.id() == current.id());
    let first = matching.next();
    if matching.next().is_some() {
        return Err(StrategyInputRejection::ConflictingFvgUpdate);
    }
    let Some(update) = first else {
        return Ok(None);
    };
    if !same_fvg_definition(current, update)
        || !valid_fvg_state_update(current.state(), update.state())
    {
        return Err(StrategyInputRejection::ConflictingFvgUpdate);
    }
    Ok(Some(update))
}

fn same_fvg_definition(current: &FairValueGap, update: &FairValueGap) -> bool {
    current.instrument() == update.instrument()
        && current.timeframe() == update.timeframe()
        && current.direction() == update.direction()
        && current.zone() == update.zone()
        && current.formed_from() == update.formed_from()
        && current.formed_at() == update.formed_at()
}

fn valid_fvg_state_update(current: &ZoneState, update: &ZoneState) -> bool {
    if current == update {
        return true;
    }
    match (current, update) {
        (
            ZoneState::Fresh,
            ZoneState::Touched { .. } | ZoneState::Mitigated { .. } | ZoneState::Invalidated { .. },
        ) => true,
        (
            ZoneState::Touched { at: current_at },
            ZoneState::Mitigated { at: update_at } | ZoneState::Invalidated { at: update_at, .. },
        ) => update_at >= current_at,
        _ => false,
    }
}

fn request_intent_cancellation(
    state: &mut SeriesState,
    key: &CandleKey,
    pending: PendingIntent,
    at: DateTime<Utc>,
    reason: CancellationReason,
    actions: &mut Vec<StrategyAction>,
) {
    let intent_id = pending.intent.id();
    actions.push(StrategyAction::CancelPendingIntent {
        key: key.clone(),
        intent_id,
        at,
        reason,
    });
    state.phase = SeriesPhase::CancelRequested(CancelRequested {
        pending,
        requested_at: at,
        reason,
    });
}

fn cancel_invalid_pending_intent(
    state: &mut SeriesState,
    key: &CandleKey,
    candle: &Candle,
    actions: &mut Vec<StrategyAction>,
) {
    let phase = std::mem::take(&mut state.phase);
    let SeriesPhase::IntentPending(pending) = phase else {
        state.phase = phase;
        return;
    };

    let gap = pending.intent.fair_value_gap();
    let violated = match pending.intent.direction() {
        Direction::Bullish => candle.close() < gap.zone().lower(),
        Direction::Bearish => candle.close() > gap.zone().upper(),
    };
    if violated {
        request_intent_cancellation(
            state,
            key,
            pending,
            candle.close_time(),
            CancellationReason::FairValueGapInvalidated,
            actions,
        );
    } else {
        state.phase = SeriesPhase::IntentPending(pending);
    }
}

fn advance_age(
    phase: &mut SeriesPhase,
    ttl: u16,
    key: &CandleKey,
    at: DateTime<Utc>,
) -> Option<StrategyAction> {
    let current = std::mem::take(phase);
    match current {
        SeriesPhase::Swept(mut pending) => {
            pending.age_bars = pending.age_bars.saturating_add(1);
            if pending.age_bars > ttl {
                Some(discarded(key, at, SetupDiscardReason::TimeToLiveExpired))
            } else {
                *phase = SeriesPhase::Swept(pending);
                None
            }
        }
        SeriesPhase::Impulse(mut pending) => {
            pending.age_bars = pending.age_bars.saturating_add(1);
            if pending.age_bars > ttl {
                Some(discarded(key, at, SetupDiscardReason::TimeToLiveExpired))
            } else {
                *phase = SeriesPhase::Impulse(pending);
                None
            }
        }
        SeriesPhase::Armed(mut armed) => {
            armed.age_bars = armed.age_bars.saturating_add(1);
            if armed.age_bars > ttl {
                Some(discarded(key, at, SetupDiscardReason::TimeToLiveExpired))
            } else {
                *phase = SeriesPhase::Armed(armed);
                None
            }
        }
        SeriesPhase::IntentPending(mut pending) => {
            pending.age_bars = pending.age_bars.saturating_add(1);
            if pending.age_bars >= ttl {
                let action = StrategyAction::CancelPendingIntent {
                    key: key.clone(),
                    intent_id: pending.intent.id(),
                    at,
                    reason: CancellationReason::TimeToLiveExpired,
                };
                *phase = SeriesPhase::CancelRequested(CancelRequested {
                    pending,
                    requested_at: at,
                    reason: CancellationReason::TimeToLiveExpired,
                });
                Some(action)
            } else {
                *phase = SeriesPhase::IntentPending(pending);
                None
            }
        }
        other => {
            *phase = other;
            None
        }
    }
}

fn progress_impulse(
    state: &mut SeriesState,
    key: &CandleKey,
    candle: &Candle,
    batch: &IctDetectionBatch,
    normalized: &NormalizedBatch,
    actions: &mut Vec<StrategyAction>,
) {
    let phase = std::mem::take(&mut state.phase);
    let SeriesPhase::Impulse(impulse) = phase else {
        state.phase = phase;
        return;
    };

    if candle.open_time() != impulse.mss.confirmed_at() {
        state.phase = SeriesPhase::Impulse(impulse);
        return;
    }

    let mut eligible = normalized.fvgs.iter().filter(|fvg| {
        fvg.direction() == impulse.direction
            && fvg.formed_from()[1] == impulse.mss.confirming_candle_open_time()
            && fvg.formed_from()[2] == candle.open_time()
            && batch.fvg_formations().iter().any(|context| {
                context.fvg_id() == fvg.id()
                    && context.displacement_candle_open_time()
                        == impulse.mss.confirming_candle_open_time()
                    && context.displacement() == Some(impulse.direction)
            })
    });
    match (eligible.next().cloned(), eligible.next()) {
        (Some(fvg), None) => {
            state.phase = SeriesPhase::Armed(ArmedSetup {
                sweep: impulse.sweep,
                mss: impulse.mss,
                fvg,
                direction: impulse.direction,
                age_bars: impulse.age_bars,
            });
        }
        (None, _) => actions.push(discarded(
            key,
            candle.close_time(),
            SetupDiscardReason::MissingNextCandleFvg,
        )),
        (Some(_), Some(_)) => actions.push(discarded(
            key,
            candle.close_time(),
            SetupDiscardReason::AmbiguousNextCandleFvg,
        )),
    }
}

fn accept_sweep(
    state: &mut SeriesState,
    sweep: Option<LiquiditySweep>,
    key: &CandleKey,
    at: DateTime<Utc>,
    actions: &mut Vec<StrategyAction>,
) {
    let Some(sweep) = sweep else {
        return;
    };
    if matches!(
        state.phase,
        SeriesPhase::IntentPending(_) | SeriesPhase::CancelRequested(_) | SeriesPhase::Position(_)
    ) {
        return;
    }
    if matches!(
        state.phase,
        SeriesPhase::Swept(_) | SeriesPhase::Impulse(_) | SeriesPhase::Armed(_)
    ) {
        actions.push(discarded(key, at, SetupDiscardReason::SupersededByNewSweep));
    }
    let direction = direction_after_sweep(sweep.side());
    state.phase = SeriesPhase::Swept(PendingSweep {
        sweep,
        direction,
        age_bars: 0,
    });
}

fn qualify_mss(
    state: &mut SeriesState,
    candle: &Candle,
    batch: &IctDetectionBatch,
    mss: Option<MarketStructureShift>,
) {
    let Some(mss) = mss else {
        return;
    };
    let phase = std::mem::take(&mut state.phase);
    let SeriesPhase::Swept(swept) = phase else {
        state.phase = phase;
        return;
    };
    let qualifies = mss.direction() == swept.direction
        && batch.displacement() == Some(swept.direction)
        && mss.confirming_candle_open_time() == candle.open_time()
        && mss.confirmed_at() == candle.close_time();
    if qualifies {
        state.phase = SeriesPhase::Impulse(PendingImpulse {
            sweep: swept.sweep,
            mss,
            direction: swept.direction,
            age_bars: swept.age_bars,
        });
    } else {
        state.phase = SeriesPhase::Swept(swept);
    }
}

const fn direction_after_sweep(side: LiquiditySide) -> Direction {
    match side {
        LiquiditySide::BuySide => Direction::Bearish,
        LiquiditySide::SellSide => Direction::Bullish,
    }
}

const fn cancellation_degradation(reason: CancellationReason) -> Option<DataDegradationReason> {
    match reason {
        CancellationReason::SeriesReset => Some(DataDegradationReason::SeriesReset),
        CancellationReason::AmbiguousInput => Some(DataDegradationReason::AmbiguousInput),
        CancellationReason::TimeToLiveExpired
        | CancellationReason::FairValueGapInvalidated
        | CancellationReason::ExecutionCancelled => None,
    }
}

fn far_boundary_violated(armed: &ArmedSetup, candle: &Candle) -> bool {
    match armed.direction {
        Direction::Bullish => candle.close() < armed.fvg.zone().lower(),
        Direction::Bearish => candle.close() > armed.fvg.zone().upper(),
    }
}

fn entry_reached(armed: &ArmedSetup, candle: &Candle, mode: FvgEntryMode) -> bool {
    let entry = match mode {
        FvgEntryMode::NearBoundary => match armed.direction {
            Direction::Bullish => armed.fvg.zone().upper(),
            Direction::Bearish => armed.fvg.zone().lower(),
        },
        FvgEntryMode::ConsequentEncroachment => armed.fvg.consequent_encroachment(),
    };
    candle.low() <= entry && candle.high() >= entry
}

fn intent_expiry(candle: &Candle, ttl: NonZeroU16) -> Result<DateTime<Utc>, StrategyError> {
    let duration = candle
        .timeframe()
        .duration()
        .checked_mul(i32::from(ttl.get()))
        .ok_or(StrategyError::TimestampOverflow)?;
    candle
        .close_time()
        .checked_add_signed(duration)
        .ok_or(StrategyError::TimestampOverflow)
}

fn trade_intent_id(
    armed: &ArmedSetup,
    candle: &Candle,
    mode: FvgEntryMode,
    expires_at: DateTime<Utc>,
) -> Uuid {
    let name = format!(
        "{STRATEGY_ALGORITHM_VERSION}|{:?}|{:?}|{}|{}|{}|{}|{}|{:?}|{}|{}",
        candle.instrument().exchange,
        candle.instrument().market,
        candle.instrument().venue_symbol,
        candle.timeframe(),
        armed.sweep.id(),
        armed.mss.id(),
        armed.fvg.id(),
        mode,
        candle.open_time().timestamp_millis(),
        expires_at.timestamp_millis(),
    );
    Uuid::new_v5(&INTENT_NAMESPACE, name.as_bytes())
}

fn handle_discontinuity(
    state: &mut SeriesState,
    key: &CandleKey,
    at: DateTime<Utc>,
    reasons: DiscontinuityReasons,
    actions: &mut Vec<StrategyAction>,
) {
    let phase = std::mem::take(&mut state.phase);
    match phase {
        SeriesPhase::Idle => {}
        SeriesPhase::Swept(_) | SeriesPhase::Impulse(_) | SeriesPhase::Armed(_) => {
            actions.push(discarded(key, at, reasons.discard));
        }
        SeriesPhase::IntentPending(pending) => {
            let intent_id = pending.intent.id();
            actions.push(StrategyAction::CancelPendingIntent {
                key: key.clone(),
                intent_id,
                at,
                reason: reasons.cancellation,
            });
            state.phase = SeriesPhase::CancelRequested(CancelRequested {
                pending,
                requested_at: at,
                reason: reasons.cancellation,
            });
        }
        SeriesPhase::CancelRequested(requested) => {
            state.phase = SeriesPhase::CancelRequested(requested);
        }
        SeriesPhase::Position(mut position) => {
            position.data_degraded = true;
            state.phase = SeriesPhase::Position(position);
            actions.push(StrategyAction::DataDegraded {
                key: key.clone(),
                at,
                reason: reasons.degradation,
            });
        }
    }
}

fn fail_closed_ambiguity(
    state: &mut SeriesState,
    key: &CandleKey,
    at: DateTime<Utc>,
    rejection: &StrategyInputRejection,
    actions: &mut Vec<StrategyAction>,
) {
    let phase = std::mem::take(&mut state.phase);
    match phase {
        SeriesPhase::IntentPending(pending) => {
            let intent_id = pending.intent.id();
            actions.push(StrategyAction::CancelPendingIntent {
                key: key.clone(),
                intent_id,
                at,
                reason: CancellationReason::AmbiguousInput,
            });
            state.phase = SeriesPhase::CancelRequested(CancelRequested {
                pending,
                requested_at: at,
                reason: CancellationReason::AmbiguousInput,
            });
        }
        SeriesPhase::CancelRequested(requested) => {
            state.phase = SeriesPhase::CancelRequested(requested);
        }
        SeriesPhase::Position(mut position) => {
            position.data_degraded = true;
            state.phase = SeriesPhase::Position(position);
            actions.push(StrategyAction::DataDegraded {
                key: key.clone(),
                at,
                reason: DataDegradationReason::AmbiguousInput,
            });
        }
        SeriesPhase::Idle => {}
        SeriesPhase::Swept(_) | SeriesPhase::Impulse(_) | SeriesPhase::Armed(_) => {
            actions.push(discarded(key, at, SetupDiscardReason::AmbiguousInput));
        }
    }

    if !matches!(
        rejection,
        StrategyInputRejection::AmbiguousLiquiditySweeps
            | StrategyInputRejection::AmbiguousStructure
            | StrategyInputRejection::BatchSeriesMismatch
    ) {
        state.phase = SeriesPhase::Idle;
    }
}

fn discarded(key: &CandleKey, at: DateTime<Utc>, reason: SetupDiscardReason) -> StrategyAction {
    StrategyAction::SetupDiscarded {
        key: key.clone(),
        at,
        reason,
    }
}

fn lifecycle_rejection(key: CandleKey, at: DateTime<Utc>) -> StrategyAction {
    StrategyAction::InputRejected {
        key: Some(key),
        at,
        reason: StrategyInputRejection::InvalidLifecycle,
    }
}

/// Fatal strategy engine failure.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StrategyError {
    /// A validated trade-intent model rejected internally assembled evidence.
    #[error(transparent)]
    Model(#[from] ModelError),
    /// A market-time expiry calculation exceeded Chrono's representable range.
    #[error("strategy timestamp arithmetic overflow")]
    TimestampOverflow,
    /// The strategy actor's downstream receiver was dropped while active.
    #[error("strategy output channel is closed")]
    OutputChannelClosed,
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io, num::NonZeroU16};

    use rust_decimal::Decimal;

    use crate::{
        ict_engine::{IctEngine, IctEngineConfig},
        models::{CandleInput, Exchange, InstrumentId, MarketKind, Timeframe, VolumeUnit},
    };

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    fn at_minute(minute: i64) -> Result<DateTime<Utc>, io::Error> {
        DateTime::from_timestamp(minute.saturating_mul(60), 0)
            .ok_or_else(|| io::Error::other("test time is outside Chrono's range"))
    }

    fn instrument() -> Result<InstrumentId, ModelError> {
        InstrumentId::new(
            Exchange::Binance,
            MarketKind::LinearPerpetual,
            "BTCUSDT",
            "BTC-USDT-PERP",
        )
    }

    fn alternate_instrument() -> Result<InstrumentId, ModelError> {
        InstrumentId::new(
            Exchange::Bybit,
            MarketKind::LinearPerpetual,
            "ETHUSDT",
            "ETH-USDT-PERP",
        )
    }

    fn non_zero_u16(value: u16) -> NonZeroU16 {
        NonZeroU16::new(value).unwrap_or(NonZeroU16::MIN)
    }

    fn strategy_config(entry_mode: FvgEntryMode, ttl: u16) -> StrategyConfig {
        StrategyConfig::new(entry_mode, non_zero_u16(ttl), KillzonePolicy::Disabled)
    }

    fn key_for(instrument: InstrumentId) -> CandleKey {
        CandleKey {
            instrument,
            timeframe: Timeframe::M1,
        }
    }

    fn candle(minute: i64, low: i64, high: i64, close: i64) -> Result<Candle, Box<dyn Error>> {
        Ok(Candle::new(CandleInput {
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            open_time: at_minute(minute)?,
            close_time: at_minute(minute + 1)?,
            open: Decimal::from(close),
            high: Decimal::from(high),
            low: Decimal::from(low),
            close: Decimal::from(close),
            volume: Decimal::ONE,
            volume_unit: VolumeUnit::Contracts,
            trade_count: Some(1),
            is_closed: true,
        })?)
    }

    fn empty_batch(candle: &Candle) -> Result<IctDetectionBatch, IctError> {
        IctEngine::new(IctEngineConfig::default()).process_candle(candle)
    }

    fn fixture_fvg() -> Result<FairValueGap, Box<dyn Error>> {
        Ok(FairValueGap::new(crate::models::FairValueGapInput {
            id: Uuid::new_v4(),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            zone: crate::models::PriceZone::new(Decimal::from(100), Decimal::from(110))?,
            formed_from: [at_minute(1)?, at_minute(2)?, at_minute(3)?],
            formed_at: at_minute(4)?,
        })?)
    }

    fn fixture_intent(entry_mode: FvgEntryMode) -> Result<TradeIntent, Box<dyn Error>> {
        let armed = fixture_armed(fixture_fvg()?)?;
        let low = match entry_mode {
            FvgEntryMode::NearBoundary => 110,
            FvgEntryMode::ConsequentEncroachment => 105,
        };
        let trigger = candle(4, low, 120, 111)?;
        Ok(TradeIntent::new(TradeIntentInput {
            id: Uuid::new_v4(),
            instrument: trigger.instrument().clone(),
            timeframe: trigger.timeframe(),
            direction: armed.direction,
            liquidity_sweep: armed.sweep,
            market_structure_shift: armed.mss,
            fair_value_gap: armed.fvg,
            entry_mode,
            trigger_candle: trigger,
            expires_at: at_minute(10)?,
        })?)
    }

    fn state_with_pending(intent: TradeIntent) -> SeriesState {
        SeriesState {
            phase: SeriesPhase::IntentPending(PendingIntent {
                intent,
                age_bars: 0,
            }),
            ..SeriesState::default()
        }
    }

    #[test]
    fn liquidity_direction_mapping_is_fail_closed_and_explicit() {
        assert_eq!(
            direction_after_sweep(LiquiditySide::BuySide),
            Direction::Bearish
        );
        assert_eq!(
            direction_after_sweep(LiquiditySide::SellSide),
            Direction::Bullish
        );
    }

    #[test]
    fn far_boundary_is_checked_before_entry() -> TestResult {
        let source = [at_minute(1)?, at_minute(2)?, at_minute(3)?];
        let fvg = FairValueGap::new(crate::models::FairValueGapInput {
            id: Uuid::new_v4(),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            zone: crate::models::PriceZone::new(Decimal::from(100), Decimal::from(110))?,
            formed_from: source,
            formed_at: at_minute(4)?,
        })?;
        let trigger = candle(4, 90, 120, 99)?;
        let armed = fixture_armed(fvg)?;
        assert!(entry_reached(
            &armed,
            &trigger,
            FvgEntryMode::ConsequentEncroachment
        ));
        assert!(far_boundary_violated(&armed, &trigger));
        Ok(())
    }

    #[test]
    fn ttl_counts_only_calls_for_the_same_series_phase() -> TestResult {
        let mut phase = SeriesPhase::Swept(PendingSweep {
            sweep: fixture_sweep()?,
            direction: Direction::Bullish,
            age_bars: 0,
        });
        let key = CandleKey {
            instrument: instrument()?,
            timeframe: Timeframe::M1,
        };
        assert!(advance_age(&mut phase, 1, &key, at_minute(4)?).is_none());
        assert!(matches!(phase, SeriesPhase::Swept(_)));
        assert!(matches!(
            advance_age(&mut phase, 1, &key, at_minute(5)?),
            Some(StrategyAction::SetupDiscarded {
                reason: SetupDiscardReason::TimeToLiveExpired,
                ..
            })
        ));
        assert!(matches!(phase, SeriesPhase::Idle));
        Ok(())
    }

    #[test]
    fn entry_modes_include_the_exact_boundary_and_reject_a_shallow_miss() -> TestResult {
        let armed = fixture_armed(fixture_fvg()?)?;
        let exact_ce = candle(4, 105, 120, 111)?;
        let above_ce = candle(4, 106, 120, 111)?;
        let exact_near = candle(4, 110, 120, 111)?;
        let above_near = candle(4, 111, 120, 111)?;

        assert!(entry_reached(
            &armed,
            &exact_ce,
            FvgEntryMode::ConsequentEncroachment
        ));
        assert!(!entry_reached(
            &armed,
            &above_ce,
            FvgEntryMode::ConsequentEncroachment
        ));
        assert!(entry_reached(
            &armed,
            &exact_near,
            FvgEntryMode::NearBoundary
        ));
        assert!(!entry_reached(
            &armed,
            &above_near,
            FvgEntryMode::NearBoundary
        ));
        Ok(())
    }

    #[test]
    fn armed_retracement_requires_execution_fill_before_position() -> TestResult {
        let mut engine =
            StrategyEngine::new(strategy_config(FvgEntryMode::ConsequentEncroachment, 6));
        let key = key_for(instrument()?);
        let armed = fixture_armed(fixture_fvg()?)?;
        engine.series.insert(
            key.clone(),
            SeriesState {
                phase: SeriesPhase::Armed(armed),
                ..SeriesState::default()
            },
        );

        let trigger = candle(4, 105, 120, 111)?;
        let batch = empty_batch(&trigger)?;
        let actions = engine.process_event(IctEngineEvent::CandleAnalyzed {
            candle: trigger,
            batch,
        })?;
        let intent = actions
            .iter()
            .find_map(|action| match action {
                StrategyAction::TradeIntentGenerated(intent) => Some(intent.clone()),
                _ => None,
            })
            .ok_or_else(|| io::Error::other("expected a generated trade intent"))?;

        assert_eq!(engine.state(&key), StrategyState::WaitingForRetracement);
        assert_eq!(engine.pending_intent(&key), Some(intent.as_ref()));

        let filled_at = at_minute(6)?;
        let opened = engine.process_command(StrategyCommand::EntryFilled {
            key: key.clone(),
            intent_id: intent.id(),
            filled_at,
        })?;
        assert_eq!(
            opened,
            vec![StrategyAction::PositionOpened {
                key: key.clone(),
                intent_id: intent.id(),
                filled_at,
            }]
        );
        assert_eq!(engine.state(&key), StrategyState::InPosition);
        assert!(engine.pending_intent(&key).is_none());

        let fill_replay = engine.process_command(StrategyCommand::EntryFilled {
            key: key.clone(),
            intent_id: intent.id(),
            filled_at,
        })?;
        assert!(fill_replay.is_empty());

        let closed_at = at_minute(7)?;
        let closed = engine.process_command(StrategyCommand::PositionClosed {
            key: key.clone(),
            intent_id: intent.id(),
            closed_at,
        })?;
        assert_eq!(
            closed,
            vec![StrategyAction::PositionClosed {
                key: key.clone(),
                intent_id: intent.id(),
                closed_at,
            }]
        );
        assert_eq!(engine.state(&key), StrategyState::Idle);

        let close_replay = engine.process_command(StrategyCommand::PositionClosed {
            key,
            intent_id: intent.id(),
            closed_at: at_minute(8)?,
        })?;
        assert!(close_replay.is_empty());
        Ok(())
    }

    #[test]
    fn fvg_invalidation_wins_before_entry_and_cancels_pending_intent() -> TestResult {
        let key = key_for(instrument()?);
        let fresh = fixture_fvg()?;
        let mut invalidated = fresh.clone();
        invalidated.invalidate(at_minute(5)?, "closed through far boundary")?;
        let mut armed_state = SeriesState {
            phase: SeriesPhase::Armed(fixture_armed(fresh)?),
            ..SeriesState::default()
        };
        let mut armed_actions = Vec::new();

        apply_fvg_updates(
            &mut armed_state,
            &key,
            at_minute(5)?,
            &[invalidated],
            &mut armed_actions,
        );
        assert!(matches!(armed_state.phase, SeriesPhase::Idle));
        assert_eq!(
            armed_actions,
            vec![StrategyAction::SetupDiscarded {
                key: key.clone(),
                at: at_minute(5)?,
                reason: SetupDiscardReason::FairValueGapInvalidated,
            }]
        );

        let intent = fixture_intent(FvgEntryMode::ConsequentEncroachment)?;
        let mut pending_update = intent.fair_value_gap().clone();
        pending_update.invalidate(at_minute(6)?, "late invalidation update")?;
        let mut pending_state = state_with_pending(intent.clone());
        let mut pending_actions = Vec::new();
        apply_fvg_updates(
            &mut pending_state,
            &key,
            at_minute(6)?,
            &[pending_update],
            &mut pending_actions,
        );
        assert!(matches!(
            pending_state.phase,
            SeriesPhase::CancelRequested(CancelRequested {
                reason: CancellationReason::FairValueGapInvalidated,
                ..
            })
        ));
        assert_eq!(
            pending_actions,
            vec![StrategyAction::CancelPendingIntent {
                key,
                intent_id: intent.id(),
                at: at_minute(6)?,
                reason: CancellationReason::FairValueGapInvalidated,
            }]
        );
        Ok(())
    }

    #[test]
    fn intent_timer_expires_at_deadline_and_retries_unacknowledged_cancel() -> TestResult {
        let intent = fixture_intent(FvgEntryMode::ConsequentEncroachment)?;
        let key = key_for(instrument()?);
        let mut engine =
            StrategyEngine::new(strategy_config(FvgEntryMode::ConsequentEncroachment, 5));
        engine
            .series
            .insert(key.clone(), state_with_pending(intent.clone()));

        assert!(engine.expire_due_intents(at_minute(9)?).is_empty());
        assert_eq!(engine.pending_intent(&key), Some(&intent));

        let expired = engine.expire_due_intents(intent.expires_at());
        assert_eq!(
            expired,
            vec![StrategyAction::CancelPendingIntent {
                key: key.clone(),
                intent_id: intent.id(),
                at: intent.expires_at(),
                reason: CancellationReason::TimeToLiveExpired,
            }]
        );
        assert!(matches!(
            engine.series.get(&key).map(|state| &state.phase),
            Some(SeriesPhase::CancelRequested(_))
        ));
        let retry_at = intent
            .expires_at()
            .checked_add_signed(CANCELLATION_RETRY_INTERVAL)
            .ok_or_else(|| io::Error::other("retry time overflow"))?;
        let before_retry = retry_at
            .checked_sub_signed(ChronoDuration::milliseconds(1))
            .ok_or_else(|| io::Error::other("retry time underflow"))?;
        assert!(engine.expire_due_intents(before_retry).is_empty());
        assert_eq!(
            engine.expire_due_intents(retry_at),
            vec![StrategyAction::CancelPendingIntent {
                key,
                intent_id: intent.id(),
                at: retry_at,
                reason: CancellationReason::TimeToLiveExpired,
            }]
        );
        Ok(())
    }

    #[test]
    fn pending_intent_is_cancelled_when_a_later_close_breaks_the_fvg() -> TestResult {
        let intent = fixture_intent(FvgEntryMode::ConsequentEncroachment)?;
        let key = key_for(instrument()?);
        let invalidating_candle = candle(5, 90, 120, 99)?;
        let mut state = state_with_pending(intent.clone());
        let mut actions = Vec::new();

        cancel_invalid_pending_intent(&mut state, &key, &invalidating_candle, &mut actions);

        assert!(matches!(
            state.phase,
            SeriesPhase::CancelRequested(CancelRequested {
                reason: CancellationReason::FairValueGapInvalidated,
                ..
            })
        ));
        assert_eq!(
            actions,
            vec![StrategyAction::CancelPendingIntent {
                key,
                intent_id: intent.id(),
                at: invalidating_candle.close_time(),
                reason: CancellationReason::FairValueGapInvalidated,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn actor_expires_an_intent_even_without_more_candles() -> TestResult {
        let intent = fixture_intent(FvgEntryMode::ConsequentEncroachment)?;
        let key = key_for(instrument()?);
        let mut engine = StrategyEngine::new(StrategyConfig::default());
        engine
            .series
            .insert(key.clone(), state_with_pending(intent.clone()));

        let shutdown = CancellationToken::new();
        let (_input_tx, input_rx) = mpsc::channel(1);
        let (output_tx, mut output_rx) = mpsc::channel(1);
        let task = tokio::spawn(engine.run(input_rx, output_tx, shutdown.clone()));

        let action = tokio::time::timeout(StdDuration::from_secs(1), output_rx.recv())
            .await?
            .ok_or_else(|| io::Error::other("strategy actor closed its output unexpectedly"))?;
        assert!(matches!(
            action,
            StrategyAction::CancelPendingIntent {
                intent_id,
                reason: CancellationReason::TimeToLiveExpired,
                ..
            } if intent_id == intent.id()
        ));

        shutdown.cancel();
        task.await??;
        Ok(())
    }

    #[test]
    fn rejected_and_duplicate_candles_do_not_age_or_replace_setup() -> TestResult {
        let key = key_for(instrument()?);
        let sweep = fixture_sweep()?;
        let sweep_id = sweep.id();
        let mut engine = StrategyEngine::new(StrategyConfig::default());
        engine.series.insert(
            key.clone(),
            SeriesState {
                phase: SeriesPhase::Swept(PendingSweep {
                    sweep,
                    direction: Direction::Bullish,
                    age_bars: 3,
                }),
                ..SeriesState::default()
            },
        );
        let replay = candle(4, 100, 120, 110)?;

        assert!(engine
            .process_event(IctEngineEvent::DuplicateIgnored {
                candle: replay.clone(),
            })?
            .is_empty());
        let rejected = engine.process_event(IctEngineEvent::CandleRejected {
            candle: replay,
            error: IctError::ConflictingCandle {
                open_time: at_minute(4)?,
            },
        })?;
        assert!(matches!(
            rejected.as_slice(),
            [StrategyAction::InputRejected {
                reason: StrategyInputRejection::Upstream(IctError::ConflictingCandle { .. }),
                ..
            }]
        ));

        let retained = engine
            .series
            .get(&key)
            .ok_or_else(|| io::Error::other("expected retained series state"))?;
        assert!(matches!(
            &retained.phase,
            SeriesPhase::Swept(PendingSweep {
                sweep,
                age_bars: 3,
                ..
            }) if sweep.id() == sweep_id
        ));
        Ok(())
    }

    #[test]
    fn fill_reported_after_cancel_request_wins_the_race() -> TestResult {
        let intent = fixture_intent(FvgEntryMode::ConsequentEncroachment)?;
        let key = key_for(instrument()?);
        let mut engine = StrategyEngine::new(StrategyConfig::default());
        engine
            .series
            .insert(key.clone(), state_with_pending(intent.clone()));

        let request_at = intent.expires_at();
        let cancellation = engine.expire_due_intents(request_at);
        assert_eq!(cancellation.len(), 1);

        let exchange_fill_time = at_minute(9)?;
        let fill = engine.process_command(StrategyCommand::EntryFilled {
            key: key.clone(),
            intent_id: intent.id(),
            filled_at: exchange_fill_time,
        })?;
        assert_eq!(
            fill,
            vec![StrategyAction::PositionOpened {
                key: key.clone(),
                intent_id: intent.id(),
                filled_at: exchange_fill_time,
            }]
        );
        assert_eq!(engine.state(&key), StrategyState::InPosition);

        let stale_cancel_ack = engine.process_command(StrategyCommand::EntryCancelled {
            key,
            intent_id: intent.id(),
            cancelled_at: at_minute(11)?,
        })?;
        assert!(stale_cancel_ack.is_empty());
        Ok(())
    }

    #[test]
    fn fill_event_that_predates_cancel_ack_recovers_cancelled_intent() -> TestResult {
        let intent = fixture_intent(FvgEntryMode::ConsequentEncroachment)?;
        let key = key_for(instrument()?);
        let mut engine = StrategyEngine::new(StrategyConfig::default());
        engine
            .series
            .insert(key.clone(), state_with_pending(intent.clone()));

        let cancelled_at = at_minute(8)?;
        let cancelled = engine.process_command(StrategyCommand::EntryCancelled {
            key: key.clone(),
            intent_id: intent.id(),
            cancelled_at,
        })?;
        assert_eq!(
            cancelled,
            vec![StrategyAction::EntryCancellationConfirmed {
                key: key.clone(),
                intent_id: intent.id(),
                cancelled_at,
            }]
        );
        assert_eq!(engine.state(&key), StrategyState::Idle);

        let fill_time = at_minute(7)?;
        let recovered = engine.process_command(StrategyCommand::EntryFilled {
            key: key.clone(),
            intent_id: intent.id(),
            filled_at: fill_time,
        })?;
        assert_eq!(
            recovered,
            vec![StrategyAction::PositionOpened {
                key: key.clone(),
                intent_id: intent.id(),
                filled_at: fill_time,
            }]
        );
        assert_eq!(engine.state(&key), StrategyState::InPosition);
        Ok(())
    }

    #[test]
    fn cancellation_ack_replay_is_idempotent_but_late_fill_is_rejected() -> TestResult {
        let intent = fixture_intent(FvgEntryMode::NearBoundary)?;
        let key = key_for(instrument()?);
        let mut engine = StrategyEngine::new(StrategyConfig::default());
        engine
            .series
            .insert(key.clone(), state_with_pending(intent.clone()));
        let cancelled_at = at_minute(7)?;

        let first = engine.process_command(StrategyCommand::EntryCancelled {
            key: key.clone(),
            intent_id: intent.id(),
            cancelled_at,
        })?;
        assert_eq!(first.len(), 1);
        let replay = engine.process_command(StrategyCommand::EntryCancelled {
            key: key.clone(),
            intent_id: intent.id(),
            cancelled_at: at_minute(8)?,
        })?;
        assert!(replay.is_empty());

        let late_fill = engine.process_command(StrategyCommand::EntryFilled {
            key: key.clone(),
            intent_id: intent.id(),
            filled_at: at_minute(9)?,
        })?;
        assert_eq!(
            late_fill,
            vec![StrategyAction::InputRejected {
                key: Some(key.clone()),
                at: at_minute(9)?,
                reason: StrategyInputRejection::InvalidLifecycle,
            }]
        );
        assert_eq!(engine.state(&key), StrategyState::Idle);
        Ok(())
    }

    #[test]
    fn lifecycle_transition_is_isolated_per_candle_key() -> TestResult {
        let intent = fixture_intent(FvgEntryMode::ConsequentEncroachment)?;
        let first_key = key_for(instrument()?);
        let second_instrument = alternate_instrument()?;
        let second_key = key_for(second_instrument.clone());
        let second_sweep = fixture_sweep_for(second_instrument)?;
        let second_sweep_id = second_sweep.id();
        let mut engine = StrategyEngine::new(StrategyConfig::default());
        engine
            .series
            .insert(first_key.clone(), state_with_pending(intent.clone()));
        engine.series.insert(
            second_key.clone(),
            SeriesState {
                phase: SeriesPhase::Swept(PendingSweep {
                    sweep: second_sweep,
                    direction: Direction::Bullish,
                    age_bars: 4,
                }),
                ..SeriesState::default()
            },
        );

        let filled_at = at_minute(6)?;
        let actions = engine.process_command(StrategyCommand::EntryFilled {
            key: first_key.clone(),
            intent_id: intent.id(),
            filled_at,
        })?;
        assert_eq!(actions.len(), 1);
        assert_eq!(engine.state(&first_key), StrategyState::InPosition);
        assert_eq!(
            engine.state(&second_key),
            StrategyState::WaitingForRetracement
        );

        let second_state = engine
            .series
            .get(&second_key)
            .ok_or_else(|| io::Error::other("expected second series state"))?;
        assert!(matches!(
            &second_state.phase,
            SeriesPhase::Swept(PendingSweep {
                sweep,
                age_bars: 4,
                ..
            }) if sweep.id() == second_sweep_id
        ));
        Ok(())
    }

    #[test]
    fn discontinuity_clears_setup_but_retains_and_degrades_position() -> TestResult {
        let key = key_for(instrument()?);
        let at = at_minute(7)?;
        let reasons = DiscontinuityReasons {
            cancellation: CancellationReason::SeriesReset,
            discard: SetupDiscardReason::SeriesReset,
            degradation: DataDegradationReason::SeriesReset,
        };
        let mut setup = SeriesState {
            phase: SeriesPhase::Armed(fixture_armed(fixture_fvg()?)?),
            ..SeriesState::default()
        };
        let mut setup_actions = Vec::new();
        handle_discontinuity(&mut setup, &key, at, reasons, &mut setup_actions);
        assert!(matches!(setup.phase, SeriesPhase::Idle));
        assert_eq!(
            setup_actions,
            vec![StrategyAction::SetupDiscarded {
                key: key.clone(),
                at,
                reason: SetupDiscardReason::SeriesReset,
            }]
        );

        let intent = fixture_intent(FvgEntryMode::ConsequentEncroachment)?;
        let mut position = SeriesState {
            phase: SeriesPhase::Position(Position {
                intent: intent.clone(),
                filled_at: at_minute(6)?,
                data_degraded: false,
            }),
            ..SeriesState::default()
        };
        let mut position_actions = Vec::new();
        handle_discontinuity(&mut position, &key, at, reasons, &mut position_actions);
        assert!(matches!(
            position.phase,
            SeriesPhase::Position(Position {
                data_degraded: true,
                ..
            })
        ));
        assert_eq!(
            position_actions,
            vec![StrategyAction::DataDegraded {
                key,
                at,
                reason: DataDegradationReason::SeriesReset,
            }]
        );
        Ok(())
    }

    fn fixture_sweep() -> Result<LiquiditySweep, ModelError> {
        fixture_sweep_for(instrument()?)
    }

    fn fixture_sweep_for(instrument: InstrumentId) -> Result<LiquiditySweep, ModelError> {
        LiquiditySweep::new(crate::models::LiquiditySweepInput {
            id: Uuid::new_v4(),
            instrument,
            timeframe: Timeframe::M1,
            side: LiquiditySide::SellSide,
            reference_price: Decimal::from(95),
            sweep_price: Decimal::from(90),
            sweep_candle_open_time: at_minute(1).map_err(|_| ModelError::InvalidCandleTime)?,
            detected_at: at_minute(2).map_err(|_| ModelError::InvalidCandleTime)?,
        })
    }

    fn fixture_armed(fvg: FairValueGap) -> Result<ArmedSetup, Box<dyn Error>> {
        use crate::models::{BreakBasis, FractalModel, SwingKind, SwingPoint};

        let swing = SwingPoint::new(
            instrument()?,
            Timeframe::M1,
            SwingKind::High,
            Decimal::from(105),
            at_minute(-2)?,
            at_minute(1)?,
            FractalModel::FiveCandle,
        )?;
        let mss = MarketStructureShift::new(crate::models::MarketStructureShiftInput {
            id: Uuid::new_v4(),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            broken_swing: swing,
            break_price: Decimal::from(106),
            break_basis: BreakBasis::Close,
            confirming_candle_open_time: at_minute(2)?,
            confirmed_at: at_minute(3)?,
        })?;
        Ok(ArmedSetup {
            sweep: fixture_sweep()?,
            mss,
            fvg,
            direction: Direction::Bullish,
            age_bars: 0,
        })
    }
}
