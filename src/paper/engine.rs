//! Single-series, credential-free ICT paper-trading orchestration.
//!
//! Existing orders see a closed candle before that candle can create a new
//! intent. Historical warm-up never submits orders. Missing intervals are fatal:
//! inventing candles would hide stop execution and corrupt the daily baseline.

use std::collections::{BTreeMap, VecDeque};

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    candle_store::CandleKey,
    ict_engine::{IctEngine, IctEngineConfig, IctEngineEvent, IctError},
    models::{Candle, Direction, LiquiditySide, SwingKind, SwingPoint, Timeframe, TradeIntent},
    risk::{
        InstrumentRiskSpec, LiquidityTarget, ReservationEvent, RiskConfig, RiskError, RiskManager,
        TradeAssessment,
    },
    strategy::{StrategyAction, StrategyCommand, StrategyConfig, StrategyEngine, StrategyError},
};

use super::broker::{PaperBroker, PaperConfig, PaperError, PaperEvent, PaperSnapshot};

const MAX_RETAINED_CANDLES: usize = 100_000;

/// Results attributable to one accepted candle, not cumulative counters.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PaperStep {
    /// Simulated fills, cancellations, and completed protective exits.
    pub events: Vec<PaperEvent>,
    /// Current cash, mark-to-market equity, and exposure.
    pub snapshot: PaperSnapshot,
    /// ICT signals observed on this candle.
    pub signals: usize,
    /// Strategy entry intents generated on this candle.
    pub intents: usize,
    /// Suppressed warm-up intents, rejected entries, or ambiguous strategy inputs.
    pub rejected: usize,
}

/// Fatal input or internal-state failure; ordinary entry rejection is in `PaperStep`.
#[derive(Debug, thiserror::Error)]
pub enum PaperEngineError {
    /// The engine cannot resume after a potentially partial internal transition.
    #[error("paper engine is poisoned after a prior fatal failure")]
    Poisoned,
    /// No candles may be accepted after explicit shutdown.
    #[error("paper engine has been shut down")]
    Stopped,
    /// Warm-up cannot be used to skip execution after trading has begun.
    #[error("paper warm-up is only allowed before trading starts")]
    WarmupAfterTrading,
    /// Candle or execution invariant failed.
    #[error(transparent)]
    Broker(#[from] PaperError),
    /// Detector failed; its partial mutable state cannot be reused safely.
    #[error(transparent)]
    Ict(#[from] IctError),
    /// Strategy failed while advancing its state machine.
    #[error(transparent)]
    Strategy(#[from] StrategyError),
    /// Account or lifecycle state failed, rather than an ordinary entry filter.
    #[error(transparent)]
    Risk(#[from] RiskError),
    /// Internal components disagreed about ownership or event order.
    #[error("paper pipeline invariant failed: {0}")]
    Invariant(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    WarmingUp,
    Trading,
    Stopped,
    Poisoned,
}

/// Deterministic default-ICT/default-killzone pipeline for one instrument/timeframe.
///
/// Exact retained candle replays are no-ops. Replay identity is retained for at
/// most 100,000 candles, after which a new session is required. No persistence,
/// leverage, private API calls, or forced end-of-data liquidation is implied.
#[derive(Debug)]
pub struct PaperEngine {
    key: CandleKey,
    ict: IctEngine,
    strategy: StrategyEngine,
    risk: RiskManager,
    broker: PaperBroker,
    history: BTreeMap<DateTime<Utc>, Candle>,
    liquidity: VecDeque<SwingPoint>,
    liquidity_capacity: usize,
    phase: Phase,
    insolvent: bool,
}

impl PaperEngine {
    /// Builds a flat session using real venue filters and explicit paper costs.
    ///
    /// # Errors
    ///
    /// Rejects invalid broker assumptions, minimum notional, or risk specification.
    pub fn new(
        spec: InstrumentRiskSpec,
        min_notional: Decimal,
        paper_config: PaperConfig,
        timeframe: Timeframe,
    ) -> Result<Self, PaperEngineError> {
        let key = CandleKey {
            instrument: spec.instrument().clone(),
            timeframe,
        };
        let mut risk = RiskManager::new(RiskConfig::default());
        risk.register_instrument_spec(spec.clone())?;
        let config = IctEngineConfig::default();
        let liquidity_capacity = config.limits().swing_capacity().get();
        Ok(Self {
            key,
            ict: IctEngine::new(config),
            strategy: StrategyEngine::new(StrategyConfig::default()),
            risk,
            broker: PaperBroker::new(paper_config, spec, min_notional)?,
            history: BTreeMap::new(),
            liquidity: VecDeque::new(),
            liquidity_capacity,
            phase: Phase::WarmingUp,
            insolvent: false,
        })
    }

    /// Observes history without creating fills, fees, positions, or simulated `PnL`.
    ///
    /// Setup context may carry into live candles, but generated historical intents
    /// are explicitly cancelled in the strategy instead of becoming resting orders.
    ///
    /// # Errors
    ///
    /// Rejects warm-up after trading starts or any invalid/discontinuous input.
    /// A fatal failure permanently poisons this session.
    pub fn warm_up(&mut self, candle: &Candle) -> Result<PaperStep, PaperEngineError> {
        self.run(candle, true)
    }

    /// Advances existing simulated exposure, then evaluates new close-time signals.
    ///
    /// Entry filters are counted in `rejected`; they do not poison the engine.
    ///
    /// # Errors
    ///
    /// Invalid input, arithmetic failure, or lifecycle disagreement permanently
    /// poisons the session so partially mutated detectors cannot silently resume.
    pub fn process_candle(&mut self, candle: &Candle) -> Result<PaperStep, PaperEngineError> {
        self.run(candle, false)
    }

    /// Returns current accounting even after shutdown or a fatal failure.
    #[must_use]
    pub fn snapshot(&self) -> PaperSnapshot {
        self.broker.snapshot()
    }

    /// Cancels only resting entry risk and stops the session; never closes positions.
    ///
    /// Cancellation remains available after a fatal market-data failure. Open
    /// positions remain visible in the final snapshot and are not called realized.
    ///
    /// # Errors
    ///
    /// Rejects invalid cancellation time or mismatched lifecycle state.
    pub fn shutdown(&mut self, at: DateTime<Utc>) -> Result<Vec<PaperEvent>, PaperEngineError> {
        let result = self.cancel_pending(at);
        self.phase = if result.is_ok() {
            Phase::Stopped
        } else {
            Phase::Poisoned
        };
        result
    }

    fn run(&mut self, candle: &Candle, warm_up: bool) -> Result<PaperStep, PaperEngineError> {
        match self.phase {
            Phase::Poisoned => return Err(PaperEngineError::Poisoned),
            Phase::Stopped => return Err(PaperEngineError::Stopped),
            Phase::Trading if warm_up => {
                self.phase = Phase::Poisoned;
                return Err(PaperEngineError::WarmupAfterTrading);
            }
            Phase::WarmingUp | Phase::Trading => {}
        }
        if !warm_up {
            self.phase = Phase::Trading;
        }
        let result = self.advance(candle, warm_up);
        if result.is_err() {
            self.phase = Phase::Poisoned;
        }
        result
    }

    fn advance(&mut self, candle: &Candle, warm_up: bool) -> Result<PaperStep, PaperEngineError> {
        if self.validate_candle(candle)? {
            return Ok(PaperStep {
                events: Vec::new(),
                snapshot: self.snapshot(),
                signals: 0,
                intents: 0,
                rejected: 0,
            });
        }
        if warm_up && !self.broker.is_flat() {
            return Err(PaperEngineError::Invariant("warm-up must remain flat"));
        }
        let mut events = self.broker.advance(candle)?;
        self.route_events(&events)?;
        if self.mark_equity(candle.close_time())? {
            events.extend(self.cancel_pending(candle.close_time())?);
        }

        // A wick through a level consumes liquidity even if no CloseBack sweep
        // signal is emitted. Removed levels can never be selected as targets.
        self.consume_liquidity(candle);
        let batch = self.ict.process_candle(candle)?;
        if batch.series_reset() || batch.is_duplicate() {
            return Err(PaperEngineError::Invariant(
                "detector disagreed with validated chronology",
            ));
        }
        for swing in batch.confirmed_swings() {
            if swing.confirmed_at() > candle.close_time() {
                return Err(PaperEngineError::Invariant("future swing confirmation"));
            }
            self.liquidity.push_back(swing.clone());
            while self.liquidity.len() > self.liquidity_capacity {
                self.liquidity.pop_front();
            }
        }
        let signals = batch.signals().len();
        let actions = self
            .strategy
            .process_event(IctEngineEvent::CandleAnalyzed {
                candle: candle.clone(),
                batch,
            })?;
        let mut step = PaperStep {
            events,
            snapshot: self.snapshot(),
            signals,
            intents: 0,
            rejected: 0,
        };
        self.handle_actions(actions, candle.close_time(), warm_up, &mut step)?;
        self.history.insert(candle.open_time(), candle.clone());
        step.snapshot = self.snapshot();
        Ok(step)
    }

    fn validate_candle(&self, candle: &Candle) -> Result<bool, PaperEngineError> {
        if CandleKey::from_candle(candle) != self.key
            || !candle.is_closed()
            || candle
                .close_time()
                .signed_duration_since(candle.open_time())
                != self.key.timeframe.duration()
            || candle.open_time().timestamp_subsec_nanos() != 0
            || candle
                .open_time()
                .timestamp()
                .rem_euclid(self.key.timeframe.duration().num_seconds())
                != 0
        {
            return Err(
                PaperError::InvalidCandle("expected final, UTC-aligned configured series").into(),
            );
        }
        if let Some(previous) = self.history.get(&candle.open_time()) {
            return if previous == candle {
                Ok(true)
            } else {
                Err(PaperError::ConflictingDuplicate.into())
            };
        }
        if let Some((_, previous)) = self.history.last_key_value() {
            if candle.open_time() < previous.close_time() {
                return Err(PaperError::OutOfOrder.into());
            }
            if candle.open_time() > previous.close_time() {
                return Err(PaperError::Gap.into());
            }
        }
        if self.history.len() >= MAX_RETAINED_CANDLES {
            return Err(PaperError::Capacity.into());
        }
        Ok(false)
    }

    fn mark_equity(&mut self, at: DateTime<Utc>) -> Result<bool, PaperEngineError> {
        let equity = self.snapshot().equity;
        // Insolvency is permanent for this session; negative PnL remains visible.
        // Never invent a positive next-day opening balance to re-arm risk.
        if self.insolvent {
            return Ok(true);
        }
        let mut cancel = false;
        match self.risk.circuit_breaker_state().trading_day() {
            None => {
                self.risk.start_trading_day(
                    self.broker.config().initial_balance(),
                    equity,
                    at,
                    false,
                )?;
            }
            Some(day) if day == at.date_naive() => {
                self.risk.update_equity(equity, at)?;
            }
            Some(_) => {
                // The candle ending exactly at midnight belongs to the preceding
                // interval. Its final mark is also the new UTC day's opening.
                let before_boundary = at
                    .checked_sub_signed(Duration::nanoseconds(1))
                    .ok_or(PaperEngineError::Invariant("midnight timestamp overflow"))?;
                self.risk.update_equity(equity, before_boundary)?;
                cancel = self.risk.circuit_breaker_state().is_tripped();
                if equity > Decimal::ZERO {
                    self.risk.start_trading_day(equity, equity, at, false)?;
                }
            }
        }
        self.insolvent = equity <= Decimal::ZERO;
        Ok(cancel || self.insolvent || self.risk.circuit_breaker_state().is_tripped())
    }

    fn consume_liquidity(&mut self, candle: &Candle) {
        self.liquidity.retain(|swing| match swing.kind() {
            SwingKind::High => candle.high() <= swing.price(),
            SwingKind::Low => candle.low() >= swing.price(),
        });
    }

    fn nearest_target(
        &self,
        intent: &TradeIntent,
        at: DateTime<Utc>,
    ) -> Result<Option<LiquidityTarget>, RiskError> {
        let selected = self
            .liquidity
            .iter()
            .filter(|swing| swing.confirmed_at() <= at)
            .filter(|swing| match intent.direction() {
                Direction::Bullish => {
                    swing.kind() == SwingKind::High && swing.price() > intent.entry_price()
                }
                Direction::Bearish => {
                    swing.kind() == SwingKind::Low && swing.price() < intent.entry_price()
                }
            })
            .min_by(|left, right| match intent.direction() {
                Direction::Bullish => left.price().cmp(&right.price()),
                Direction::Bearish => right.price().cmp(&left.price()),
            });
        selected
            .map(|swing| {
                let identity = format!(
                    "paper-liquidity:{:?}:{:?}:{}:{}",
                    self.key,
                    swing.kind(),
                    swing.candle_open_time(),
                    swing.price().normalize()
                );
                LiquidityTarget::new(
                    Uuid::new_v5(&Uuid::NAMESPACE_OID, identity.as_bytes()),
                    self.key.instrument.clone(),
                    match swing.kind() {
                        SwingKind::High => LiquiditySide::BuySide,
                        SwingKind::Low => LiquiditySide::SellSide,
                    },
                    swing.price(),
                    swing.confirmed_at(),
                )
            })
            .transpose()
    }

    fn handle_actions(
        &mut self,
        actions: Vec<StrategyAction>,
        at: DateTime<Utc>,
        warm_up: bool,
        step: &mut PaperStep,
    ) -> Result<(), PaperEngineError> {
        for action in actions {
            match action {
                StrategyAction::TradeIntentGenerated(intent) => {
                    step.intents += 1;
                    if !self.submit_intent(&intent, at, warm_up)? {
                        step.rejected += 1;
                    }
                }
                StrategyAction::CancelPendingIntent {
                    key, intent_id, at, ..
                } => {
                    if key != self.key || self.broker.pending_intent() != Some(intent_id) {
                        return Err(PaperEngineError::Invariant(
                            "strategy cancellation has no matching broker entry",
                        ));
                    }
                    step.events.extend(self.cancel_pending(at)?);
                }
                StrategyAction::InputRejected { .. } => {
                    step.rejected += 1;
                }
                StrategyAction::SetupDiscarded { .. } | StrategyAction::DataDegraded { .. } => {}
                StrategyAction::PositionOpened { .. }
                | StrategyAction::PositionClosed { .. }
                | StrategyAction::EntryCancellationConfirmed { .. } => {
                    return Err(PaperEngineError::Invariant(
                        "market analysis emitted an execution lifecycle",
                    ));
                }
            }
        }
        Ok(())
    }

    fn submit_intent(
        &mut self,
        intent: &TradeIntent,
        at: DateTime<Utc>,
        warm_up: bool,
    ) -> Result<bool, PaperEngineError> {
        if warm_up || self.insolvent || self.risk.circuit_breaker_state().is_tripped() {
            self.reject_intent(intent.id(), at, false)?;
            return Ok(false);
        }
        let Some(target) = self.nearest_target(intent, at)? else {
            self.reject_intent(intent.id(), at, false)?;
            return Ok(false);
        };
        let assessment = match self.risk.assess_trade(intent, &target, at) {
            Ok(assessment) => assessment,
            Err(error) if is_entry_rejection(&error) => {
                self.reject_intent(intent.id(), at, false)?;
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        };
        let TradeAssessment::Approved(plan) = assessment else {
            return Err(PaperEngineError::Invariant(
                "unexpected risk replay after candle deduplication",
            ));
        };
        match self.broker.submit(*plan) {
            Ok(events) if events.is_empty() => Ok(true),
            Ok(_) => Err(PaperEngineError::Invariant(
                "submission filled without a future candle",
            )),
            Err(error) if error.is_rejection() => {
                self.reject_intent(intent.id(), at, true)?;
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn reject_intent(
        &mut self,
        intent_id: Uuid,
        at: DateTime<Utc>,
        reserved: bool,
    ) -> Result<(), PaperEngineError> {
        if reserved {
            self.risk
                .apply_reservation_event(intent_id, ReservationEvent::EntryCancelled, at)?;
        }
        self.route_strategy(StrategyCommand::EntryCancelled {
            key: self.key.clone(),
            intent_id,
            cancelled_at: at,
        })
    }

    fn cancel_pending(&mut self, at: DateTime<Utc>) -> Result<Vec<PaperEvent>, PaperEngineError> {
        let events = self.broker.cancel_pending(at)?;
        self.route_events(&events)?;
        Ok(events)
    }

    fn route_events(&mut self, events: &[PaperEvent]) -> Result<(), PaperEngineError> {
        for event in events {
            let (intent_id, at, reservation, command) = match *event {
                PaperEvent::EntryFilled { intent_id, at, .. } => (
                    intent_id,
                    at,
                    ReservationEvent::EntryFilled,
                    StrategyCommand::EntryFilled {
                        key: self.key.clone(),
                        intent_id,
                        filled_at: at,
                    },
                ),
                PaperEvent::EntryCancelled { intent_id, at, .. } => (
                    intent_id,
                    at,
                    ReservationEvent::EntryCancelled,
                    StrategyCommand::EntryCancelled {
                        key: self.key.clone(),
                        intent_id,
                        cancelled_at: at,
                    },
                ),
                PaperEvent::PositionClosed { intent_id, at, .. } => (
                    intent_id,
                    at,
                    ReservationEvent::PositionClosed,
                    StrategyCommand::PositionClosed {
                        key: self.key.clone(),
                        intent_id,
                        closed_at: at,
                    },
                ),
            };
            self.risk
                .apply_reservation_event(intent_id, reservation, at)?;
            self.route_strategy(command)?;
        }
        Ok(())
    }

    fn route_strategy(&mut self, command: StrategyCommand) -> Result<(), PaperEngineError> {
        let actions = self.strategy.process_command(command)?;
        if actions.iter().any(|action| {
            !matches!(
                action,
                StrategyAction::PositionOpened { .. }
                    | StrategyAction::PositionClosed { .. }
                    | StrategyAction::EntryCancellationConfirmed { .. }
            )
        }) {
            return Err(PaperEngineError::Invariant(
                "strategy rejected broker lifecycle",
            ));
        }
        Ok(())
    }
}

fn is_entry_rejection(error: &RiskError) -> bool {
    matches!(
        error,
        RiskError::CircuitBreakerTripped
            | RiskError::IntentExpired
            | RiskError::InvalidPriceGeometry
            | RiskError::MinimumRiskRewardNotMet { .. }
            | RiskError::PortfolioRiskLimitReached
            | RiskError::QuantityBelowMinimum { .. }
    )
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use super::*;
    use crate::{
        execution::coordinator::tests::instrument,
        models::{CandleInput, Exchange, VolumeUnit},
        risk::{CircuitBreakerState, ReservationStatus},
        strategy::StrategyState,
    };

    type TestResult = Result<(), Box<dyn Error>>;

    fn spec() -> Result<InstrumentRiskSpec, Box<dyn Error>> {
        Ok(InstrumentRiskSpec::new(
            instrument(Exchange::Binance)?,
            Decimal::new(1, 1),
            Decimal::new(1, 3),
            Decimal::new(1, 3),
            None,
            Decimal::ONE,
        )?)
    }

    fn engine() -> Result<PaperEngine, Box<dyn Error>> {
        Ok(PaperEngine::new(
            spec()?,
            Decimal::ONE,
            PaperConfig::default(),
            Timeframe::M1,
        )?)
    }

    fn at(minute: i64) -> Result<DateTime<Utc>, Box<dyn Error>> {
        // Wednesday, 07:00 New York: the real default NY killzone is enabled.
        Ok(DateTime::parse_from_rfc3339("2026-10-28T11:00:00Z")?
            .with_timezone(&Utc)
            .checked_add_signed(Duration::minutes(minute))
            .ok_or_else(|| io::Error::other("test timestamp overflow"))?)
    }

    fn candle(minute: i64, prices: [i64; 4]) -> Result<Candle, Box<dyn Error>> {
        Ok(Candle::new(CandleInput {
            instrument: instrument(Exchange::Binance)?,
            timeframe: Timeframe::M1,
            open_time: at(minute)?,
            close_time: at(minute + 1)?,
            open: Decimal::from(prices[0]),
            high: Decimal::from(prices[1]),
            low: Decimal::from(prices[2]),
            close: Decimal::from(prices[3]),
            volume: Decimal::ONE,
            volume_unit: VolumeUnit::BaseAsset,
            trade_count: None,
            is_closed: true,
        })?)
    }

    fn setup(target: i64) -> Result<Vec<Candle>, Box<dyn Error>> {
        // Real five-candle fractals and 20-body displacement, not fabricated
        // SignalEvents: initial bearish BOS -> SSL sweep -> bullish MSS/FVG -> CE.
        let mut prices = vec![[100, 102, 98, 101]; 9];
        prices[2] = [100, target, 98, 101];
        prices.extend([
            [100, 102, 95, 101],
            [100, 102, 97, 101],
            [100, 102, 98, 101],
            [100, 105, 98, 101],
            [100, 103, 97, 100],
            [99, 102, 96, 98],
            [98, 99, 93, 94],
            [94, 97, 92, 94],
            [94, 97, 91, 93],
            [93, 96, 90, 93],
            [94, 98, 92, 96],
            [96, 99, 93, 97],
            [97, 100, 89, 94],
            [94, 112, 94, 111],
            [110, 113, 108, 111],
            [110, 112, 103, 109],
        ]);
        prices
            .into_iter()
            .enumerate()
            .map(|(index, value)| candle(i64::try_from(index)?, value))
            .collect()
    }

    fn ready_pending() -> Result<PaperEngine, Box<dyn Error>> {
        let mut engine = engine()?;
        for candle in setup(150)? {
            engine.process_candle(&candle)?;
        }
        if engine.snapshot().pending.is_none() {
            return Err(io::Error::other("default ICT setup did not create resting entry").into());
        }
        Ok(engine)
    }

    #[test]
    fn default_pipeline_approves_then_fills_only_a_future_bar_and_releases_on_target() -> TestResult
    {
        let mut engine = ready_pending()?;
        let pending = engine
            .snapshot()
            .pending
            .ok_or_else(|| io::Error::other("pending missing"))?;
        assert_eq!(pending.entry_price, Decimal::from(104));
        assert_eq!(pending.take_profit_price, Decimal::from(150));
        assert!(pending.stop_loss_price < Decimal::from(89));
        assert_eq!(engine.snapshot().fees_paid, Decimal::ZERO);
        assert_eq!(
            engine.risk.reservation_status(pending.intent_id),
            Some(ReservationStatus::Pending)
        );
        let fill = engine.process_candle(&candle(25, [109, 110, 103, 109])?)?;
        assert!(matches!(
            fill.events.as_slice(),
            [PaperEvent::EntryFilled { .. }]
        ));
        assert_eq!(
            engine.strategy.state(&engine.key),
            StrategyState::InPosition
        );
        assert_eq!(
            engine.risk.reservation_status(pending.intent_id),
            Some(ReservationStatus::PositionOpen)
        );
        assert!(fill.snapshot.position.is_some());
        let close = engine.process_candle(&candle(26, [109, 151, 109, 150])?)?;
        assert!(matches!(
            close.events.as_slice(),
            [PaperEvent::PositionClosed { .. }]
        ));
        assert_eq!(close.snapshot.completed_trades, 1);
        assert!(close.snapshot.balance > Decimal::from(100));
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        // The exit candle may legitimately arm a fresh setup after routing the
        // closure; only lingering position ownership would be incorrect.
        assert_ne!(
            engine.strategy.state(&engine.key),
            StrategyState::InPosition
        );
        Ok(())
    }

    #[test]
    fn warmup_suppresses_actual_setup_without_historical_pnl_or_wedged_intent() -> TestResult {
        let mut engine = engine()?;
        let mut generated = 0;
        for candle in setup(150)? {
            let step = engine.warm_up(&candle)?;
            generated += step.intents;
            assert!(step.events.is_empty());
            assert_eq!(step.snapshot.balance, Decimal::from(100));
            assert_eq!(step.snapshot.equity, Decimal::from(100));
            assert_eq!(step.snapshot.fees_paid, Decimal::ZERO);
            assert!(step.snapshot.pending.is_none() && step.snapshot.position.is_none());
        }
        assert_eq!(generated, 1);
        assert!(engine.strategy.pending_intent(&engine.key).is_none());
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        let live = engine.process_candle(&candle(25, [109, 110, 103, 109])?)?;
        assert!(live.events.is_empty());
        assert_eq!(live.snapshot.completed_trades, 0);
        assert_eq!(live.snapshot.equity, Decimal::from(100));
        assert!(matches!(
            engine.warm_up(&candle(26, [109, 110, 103, 109])?),
            Err(PaperEngineError::WarmupAfterTrading)
        ));
        Ok(())
    }

    #[test]
    fn setup_warmed_before_retracement_can_create_a_new_live_entry() -> TestResult {
        let mut engine = engine()?;
        let bars = setup(150)?;
        for bar in bars.iter().take(24) {
            engine.warm_up(bar)?;
        }
        let trigger = bars
            .last()
            .ok_or_else(|| io::Error::other("fixture empty"))?;
        let step = engine.process_candle(trigger)?;
        assert_eq!(step.intents, 1);
        assert_eq!(step.rejected, 0);
        assert!(step.snapshot.pending.is_some());
        assert!(step.events.is_empty());
        Ok(())
    }

    #[test]
    fn nearest_target_failing_rr_is_rejected_without_synthetic_farther_target() -> TestResult {
        let mut engine = engine()?;
        let mut rejected = 0;
        for bar in setup(120)? {
            rejected += engine.process_candle(&bar)?.rejected;
        }
        assert!(rejected > 0);
        assert!(engine.snapshot().pending.is_none());
        assert!(engine.strategy.pending_intent(&engine.key).is_none());
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn absent_liquidity_target_does_not_invent_take_profit() -> TestResult {
        let mut engine = engine()?;
        let bars = setup(150)?;
        for bar in bars.iter().take(24) {
            engine.warm_up(bar)?;
        }
        engine.liquidity.clear();
        let step = engine.process_candle(
            bars.last()
                .ok_or_else(|| io::Error::other("fixture empty"))?,
        )?;
        assert_eq!(step.intents, 1);
        assert_eq!(step.rejected, 1);
        assert!(engine.strategy.pending_intent(&engine.key).is_none());
        assert!(step.snapshot.pending.is_none());
        Ok(())
    }

    #[test]
    fn broker_minimum_rejection_releases_risk_and_strategy_reservations() -> TestResult {
        let mut engine = PaperEngine::new(
            spec()?,
            Decimal::from(100),
            PaperConfig::default(),
            Timeframe::M1,
        )?;
        let mut generated = 0;
        let mut rejected = 0;
        for bar in setup(150)? {
            let step = engine.process_candle(&bar)?;
            generated += step.intents;
            rejected += step.rejected;
        }
        assert_eq!(generated, 1);
        assert_eq!(rejected, 1);
        assert!(engine.strategy.pending_intent(&engine.key).is_none());
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        assert!(engine.broker.is_flat());
        Ok(())
    }

    #[test]
    fn exact_old_replays_are_noops_and_changed_replay_poisons_before_mutation() -> TestResult {
        let mut engine = ready_pending()?;
        let snapshot = engine.snapshot();
        let replay = engine.process_candle(&candle(0, [100, 102, 98, 101])?)?;
        assert_eq!(replay.snapshot, snapshot);
        assert_eq!((replay.intents, replay.signals, replay.rejected), (0, 0, 0));
        assert!(replay.events.is_empty());
        assert!(matches!(
            engine.process_candle(&candle(0, [100, 102, 98, 100])?),
            Err(PaperEngineError::Broker(PaperError::ConflictingDuplicate))
        ));
        assert_eq!(engine.snapshot(), snapshot);
        assert!(matches!(
            engine.process_candle(&candle(25, [109, 110, 103, 109])?),
            Err(PaperEngineError::Poisoned)
        ));
        let cancelled = engine.shutdown(at(25)?)?;
        assert!(matches!(
            cancelled.as_slice(),
            [PaperEvent::EntryCancelled { .. }]
        ));
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn gap_is_fatal_before_broker_can_invent_a_fill() -> TestResult {
        let mut engine = ready_pending()?;
        let before = engine.snapshot();
        assert!(matches!(
            engine.process_candle(&candle(26, [109, 110, 103, 109])?),
            Err(PaperEngineError::Broker(PaperError::Gap))
        ));
        assert_eq!(engine.snapshot(), before);
        Ok(())
    }

    #[test]
    fn consumed_and_future_liquidity_are_not_targets() -> TestResult {
        let mut engine = ready_pending()?;
        let intent = engine
            .strategy
            .pending_intent(&engine.key)
            .cloned()
            .ok_or_else(|| io::Error::other("intent missing"))?;
        assert_eq!(
            engine
                .nearest_target(&intent, at(25)?)?
                .map(|target| target.price()),
            Some(Decimal::from(150))
        );
        assert!(engine.nearest_target(&intent, at(0)?)?.is_none());
        engine.consume_liquidity(&candle(25, [109, 151, 103, 109])?);
        assert!(engine.nearest_target(&intent, at(26)?)?.is_none());
        Ok(())
    }

    #[test]
    fn shutdown_never_fabricates_realized_pnl_for_open_position() -> TestResult {
        let mut engine = ready_pending()?;
        engine.process_candle(&candle(25, [109, 110, 103, 109])?)?;
        let before = engine.snapshot();
        assert!(engine.shutdown(at(26)?)?.is_empty());
        assert_eq!(engine.snapshot(), before);
        assert!(engine.snapshot().position.is_some());
        assert_eq!(engine.snapshot().completed_trades, 0);
        assert!(matches!(
            engine.process_candle(&candle(26, [109, 151, 103, 150])?),
            Err(PaperEngineError::Stopped)
        ));
        Ok(())
    }

    #[test]
    fn entry_and_stop_on_same_bar_route_both_lifecycles_in_order() -> TestResult {
        let mut engine = ready_pending()?;
        let step = engine.process_candle(&candle(25, [109, 110, 80, 100])?)?;
        assert!(matches!(
            step.events.as_slice(),
            [
                PaperEvent::EntryFilled { .. },
                PaperEvent::PositionClosed { .. }
            ]
        ));
        assert_eq!(step.snapshot.completed_trades, 1);
        assert_eq!(step.snapshot.losses, 1);
        assert!(engine.broker.is_flat());
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        assert_ne!(
            engine.strategy.state(&engine.key),
            StrategyState::InPosition
        );
        Ok(())
    }

    #[test]
    fn gap_loss_is_not_clipped_and_trips_daily_drawdown_latch() -> TestResult {
        let mut engine = ready_pending()?;
        engine.process_candle(&candle(25, [109, 110, 103, 109])?)?;
        let loss = engine.process_candle(&candle(26, [2, 3, 1, 2])?)?;
        assert!(loss.snapshot.equity < Decimal::from(95));
        assert!(engine.risk.circuit_breaker_state().is_tripped());
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        engine.process_candle(&candle(27, [2, 3, 1, 2])?)?;
        assert!(engine.risk.circuit_breaker_state().is_tripped());
        assert!(engine.broker.is_flat());
        Ok(())
    }

    #[test]
    fn midnight_rollover_uses_boundary_equity_and_intraday_latch_does_not_reset() -> TestResult {
        let mut engine = engine()?;
        // 12:59 hours after the 11:00 fixture origin ends exactly at UTC midnight.
        engine.warm_up(&candle(778, [100, 101, 99, 100])?)?;
        engine
            .risk
            .update_equity(Decimal::from(94), at(779)? + Duration::seconds(1))?;
        engine
            .risk
            .update_equity(Decimal::from(100), at(779)? + Duration::seconds(2))?;
        assert!(engine.risk.circuit_breaker_state().is_tripped());
        engine.warm_up(&candle(779, [100, 101, 99, 100])?)?;
        assert!(
            matches!(engine.risk.circuit_breaker_state(), CircuitBreakerState::Armed { opening_equity, trading_day, .. }
            if opening_equity == Decimal::from(100) && trading_day == at(780)?.date_naive())
        );
        assert_eq!(engine.snapshot().balance, Decimal::from(100));
        Ok(())
    }
}
