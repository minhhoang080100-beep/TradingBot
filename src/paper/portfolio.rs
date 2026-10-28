//! Shared-account, synchronized-batch PAPER orchestration for BTC and ETH.
//!
//! One broker and one risk manager own all cash and exposure. Each symbol keeps
//! independent ICT, strategy and unswept-liquidity state. A complete M1 batch is
//! validated before any fill, and the existing owner advances before new ideas.
//! BTC wins simultaneous eligible entries; at most one global order/position is
//! permitted. Leverage changes simulated margin capacity, not the 1% risk rule.

use std::collections::{BTreeMap, VecDeque};

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    candle_store::CandleKey,
    ict_engine::{IctEngine, IctEngineConfig, IctEngineEvent, IctError},
    models::{
        Candle, Direction, Exchange, LiquiditySide, MarketKind, SwingKind, SwingPoint, Timeframe,
        TradeIntent, VolumeUnit,
    },
    risk::{LiquidityTarget, ReservationEvent, RiskConfig, RiskError, RiskManager, TradeAssessment},
    strategy::{StrategyAction, StrategyCommand, StrategyConfig, StrategyEngine, StrategyError},
};

use super::{
    broker::{PaperBroker, PaperConfig, PaperError, PaperEvent, PaperOrderSnapshot, PaperSnapshot},
    market::PaperInstrument,
};

const MAX_RETAINED_BATCHES: usize = 100_000;

/// One shared account, never a sum of independent symbol balances.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortfolioSnapshot {
    /// Shared cash, equity, accounting counters and single active geometry.
    pub account: PaperSnapshot,
    /// Owner of the pending entry or position, absent only while globally flat.
    pub active_symbol: Option<String>,
    /// Explicit simulation leverage; this does not multiply price profit/loss.
    pub leverage: u32,
    /// New entries are blocked by drawdown, insolvency, failure or shutdown.
    pub halted: bool,
}

/// Attributable lifecycle records retaining order geometry after an exit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PortfolioEvent {
    /// A new resting entry was accepted; it cannot fill on its signal candle.
    EntryPlaced {
        /// Owning venue symbol.
        symbol: String,
        /// Signal candle close.
        at: DateTime<Utc>,
        /// Actual step-rounded quantity and approved price geometry.
        order: PaperOrderSnapshot,
    },
    /// A broker transition with geometry captured before advancing the bar.
    Execution {
        /// Owning venue symbol, not the symbol most recently analyzed.
        symbol: String,
        /// Simulated fill, cancellation or exit.
        event: PaperEvent,
        /// Pre-transition geometry, including for same-bar fill plus stop.
        order: PaperOrderSnapshot,
    },
    /// New entries became blocked, or an otherwise running session shut down.
    Halted {
        /// Observation time; no forced liquidation is implied.
        at: DateTime<Utc>,
    },
}

/// Recoverable per-symbol rejection categories for strategy observability.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioRejection {
    /// Historical input cannot submit an order.
    Warmup,
    /// The other symbol or this symbol already owns global exposure.
    GlobalBusy,
    /// Account drawdown or insolvency prevents new risk.
    Halted,
    /// No confirmed unswept opposing liquidity exists.
    NoOpposingLiquidity,
    /// A risk-manager filter rejected the geometry or risk budget.
    RiskRule,
    /// Venue minimum quantity or notional was not reached.
    MinimumOrder,
    /// Simulated margin/cash cannot support an entry.
    InsufficientBalance,
    /// Fees and adverse stop slippage violate minimum net RR.
    NetRiskReward,
    /// Strategy rejected an ambiguous or inconsistent analysis input.
    StrategyInput,
}

/// Analysis counters for one symbol in the accepted batch, not session totals.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SymbolAnalysis {
    /// Exact venue symbol; reports are sorted BTC before ETH.
    pub symbol: String,
    /// Number of ICT signals observed on this candle.
    pub signals: usize,
    /// Number of strategy entry intents generated on this candle.
    pub intents: usize,
    /// Number of suppressed/rejected intents or strategy inputs.
    pub rejected: usize,
    /// One category for each rejection, in processing order.
    pub rejection_reasons: Vec<PortfolioRejection>,
}

impl SymbolAnalysis {
    fn empty(symbol: String) -> Self {
        Self { symbol, signals: 0, intents: 0, rejected: 0, rejection_reasons: Vec::new() }
    }

    fn reject(&mut self, reason: PortfolioRejection) {
        self.rejected += 1;
        self.rejection_reasons.push(reason);
    }
}

/// Result of one complete synchronized batch or an exact duplicate no-op.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortfolioStep {
    /// Shared account after fills, analysis and any new resting entry.
    pub snapshot: PortfolioSnapshot,
    /// Lifecycle events in deterministic execution/analysis order.
    pub events: Vec<PortfolioEvent>,
    /// Per-symbol analysis, always in sorted symbol order.
    pub analysis: Vec<SymbolAnalysis>,
}

/// Fatal validation or internal-state error; ordinary rejection is in the step.
#[derive(Debug, thiserror::Error)]
pub enum PortfolioError {
    /// A previous fatal transition prevents further market processing.
    #[error("paper portfolio is poisoned after a prior fatal failure")]
    Poisoned,
    /// No further market batches may follow explicit shutdown.
    #[error("paper portfolio has been shut down")]
    Stopped,
    /// Warm-up cannot suppress fills after trading has started.
    #[error("paper portfolio warm-up is only allowed before trading starts")]
    WarmupAfterTrading,
    /// Only one or two unique Bybit BTCUSDT/ETHUSDT linear M1 assets are allowed.
    #[error("invalid paper portfolio configuration: {0}")]
    Configuration(&'static str),
    /// Incomplete, duplicate-symbol or unsynchronized batch.
    #[error("invalid paper portfolio batch: {0}")]
    InvalidBatch(&'static str),
    /// Broker/input arithmetic or lifecycle validation failed.
    #[error(transparent)]
    Broker(#[from] PaperError),
    /// Detector processing failed.
    #[error(transparent)]
    Ict(#[from] IctError),
    /// Strategy processing failed.
    #[error(transparent)]
    Strategy(#[from] StrategyError),
    /// Shared risk/account processing failed.
    #[error(transparent)]
    Risk(#[from] RiskError),
    /// Components disagreed about ownership or chronology.
    #[error("paper portfolio invariant failed: {0}")]
    Invariant(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase { WarmingUp, Trading, Stopped, Poisoned }

#[derive(Debug)]
struct SymbolState {
    instrument: PaperInstrument,
    key: CandleKey,
    ict: IctEngine,
    strategy: StrategyEngine,
    liquidity: VecDeque<SwingPoint>,
    liquidity_capacity: usize,
}

impl SymbolState {
    fn new(instrument: PaperInstrument, timeframe: Timeframe) -> Self {
        let config = IctEngineConfig::default();
        let liquidity_capacity = config.limits().swing_capacity().get();
        Self {
            key: CandleKey { instrument: instrument.spec.instrument().clone(), timeframe },
            instrument,
            ict: IctEngine::new(config),
            strategy: StrategyEngine::new(StrategyConfig::default()),
            liquidity: VecDeque::new(),
            liquidity_capacity,
        }
    }

    fn analyze(&mut self, candle: &Candle) -> Result<(usize, Vec<StrategyAction>), PortfolioError> {
        // A strict wick break consumes a target even without a close-back sweep.
        self.liquidity.retain(|swing| match swing.kind() {
            SwingKind::High => candle.high() <= swing.price(),
            SwingKind::Low => candle.low() >= swing.price(),
        });
        let batch = self.ict.process_candle(candle)?;
        if batch.series_reset() || batch.is_duplicate() {
            return Err(PortfolioError::Invariant("detector disagreed with validated chronology"));
        }
        for swing in batch.confirmed_swings() {
            if swing.confirmed_at() > candle.close_time() {
                return Err(PortfolioError::Invariant("future swing confirmation"));
            }
            self.liquidity.push_back(swing.clone());
            while self.liquidity.len() > self.liquidity_capacity {
                self.liquidity.pop_front();
            }
        }
        let signals = batch.signals().len();
        let actions = self.strategy.process_event(IctEngineEvent::CandleAnalyzed {
            candle: candle.clone(), batch,
        })?;
        Ok((signals, actions))
    }

    fn nearest_target(&self, intent: &TradeIntent, at: DateTime<Utc>) -> Result<Option<LiquidityTarget>, RiskError> {
        self.liquidity.iter()
            .filter(|swing| swing.confirmed_at() <= at)
            .filter(|swing| match intent.direction() {
                Direction::Bullish => swing.kind() == SwingKind::High && swing.price() > intent.entry_price(),
                Direction::Bearish => swing.kind() == SwingKind::Low && swing.price() < intent.entry_price(),
            })
            .min_by(|left, right| match intent.direction() {
                Direction::Bullish => left.price().cmp(&right.price()),
                Direction::Bearish => right.price().cmp(&left.price()),
            })
            .map(|swing| {
                let identity = format!("paper-portfolio-liquidity:{:?}:{:?}:{}:{}", self.key,
                    swing.kind(), swing.candle_open_time(), swing.price().normalize());
                LiquidityTarget::new(Uuid::new_v5(&Uuid::NAMESPACE_OID, identity.as_bytes()),
                    self.key.instrument.clone(), match swing.kind() {
                        SwingKind::High => LiquiditySide::BuySide,
                        SwingKind::Low => LiquiditySide::SellSide,
                    }, swing.price(), swing.confirmed_at())
            }).transpose()
    }
}

/// Conservative shared PAPER account for synchronized BTC/ETH M1 batches.
///
/// Exact complete historical batch replays are no-ops, retained for at most
/// 100,000 batches. A fatal input poisons processing; shutdown may still cancel
/// a resting entry. Open exposure is never silently flattened or resumed.
#[derive(Debug)]
pub struct PaperPortfolio {
    symbols: BTreeMap<String, SymbolState>,
    timeframe: Timeframe,
    risk: RiskManager,
    broker: PaperBroker,
    history: BTreeMap<DateTime<Utc>, Vec<Candle>>,
    phase: Phase,
    insolvent: bool,
}

impl PaperPortfolio {
    /// Builds one shared account and independent default-ICT symbol states.
    ///
    /// # Errors
    /// Rejects empty/duplicate/unsupported instruments, non-M1 input, invalid
    /// metadata or configuration. Only canonical Bybit linear BTC/ETH is allowed.
    pub fn new(instruments: Vec<PaperInstrument>, config: PaperConfig, timeframe: Timeframe) -> Result<Self, PortfolioError> {
        if instruments.is_empty() || instruments.len() > 2 || timeframe != Timeframe::M1 {
            return Err(PortfolioError::Configuration("expected one or two M1 instruments"));
        }
        let mut symbols = BTreeMap::new();
        let mut risk = RiskManager::new(RiskConfig::default());
        for instrument in instruments {
            let identity = instrument.spec.instrument();
            let canonical = match identity.venue_symbol.as_str() {
                "BTCUSDT" => "BTC-USDT-PERP",
                "ETHUSDT" => "ETH-USDT-PERP",
                _ => return Err(PortfolioError::Configuration("only BTCUSDT and ETHUSDT are supported")),
            };
            if identity.exchange != Exchange::Bybit || identity.market != MarketKind::LinearPerpetual
                || identity.canonical_symbol.as_str() != canonical
                || instrument.spec.quote_value_per_price_unit() != Decimal::ONE
                || instrument.min_notional < Decimal::ZERO {
                return Err(PortfolioError::Configuration("expected canonical Bybit linear USDT metadata"));
            }
            let symbol = identity.venue_symbol.as_str().to_owned();
            risk.register_instrument_spec(instrument.spec.clone())?;
            if symbols.insert(symbol, SymbolState::new(instrument, timeframe)).is_some() {
                return Err(PortfolioError::Configuration("duplicate instrument"));
            }
        }
        let first = symbols.first_key_value().ok_or(PortfolioError::Configuration("no instruments"))?.1;
        let broker = PaperBroker::new(config, first.instrument.spec.clone(), first.instrument.min_notional)?;
        Ok(Self { symbols, timeframe, risk, broker, history: BTreeMap::new(), phase: Phase::WarmingUp, insolvent: false })
    }

    /// Initializes all symbol contexts without creating historical fills/orders.
    ///
    /// # Errors
    /// Invalid batches or warm-up after trading poison the session.
    pub fn warm_up(&mut self, candles: &[Candle]) -> Result<PortfolioStep, PortfolioError> {
        self.run(candles, true)
    }

    /// Advances the current owner, marks shared equity, then analyzes BTC first.
    ///
    /// # Errors
    /// Missing/changed/misaligned/discontinuous input or internal failure poisons
    /// the session. Ordinary entry rejections are reported without poisoning.
    pub fn process_batch(&mut self, candles: &[Candle]) -> Result<PortfolioStep, PortfolioError> {
        self.run(candles, false)
    }

    /// Reports the single shared account, including after fatal failure/shutdown.
    #[must_use]
    pub fn snapshot(&self) -> PortfolioSnapshot {
        PortfolioSnapshot {
            account: self.broker.snapshot(),
            active_symbol: (!self.broker.is_flat()).then(|| self.broker.spec().instrument().venue_symbol.as_str().to_owned()),
            leverage: self.broker.config().leverage(),
            halted: self.entry_halted() || matches!(self.phase, Phase::Stopped | Phase::Poisoned),
        }
    }

    /// Whether a fatal input or transition currently prevents further batches.
    ///
    /// Capture this before shutdown: a successful shutdown changes the phase to
    /// stopped. A poisoned session must be recorded as diagnostic-only, even if
    /// cancelling its pending entry during shutdown subsequently succeeds.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        matches!(self.phase, Phase::Poisoned)
    }

    /// Cancels a pending entry and halts; open positions remain marked, not closed.
    ///
    /// # Errors
    /// Rejects time before the latest batch or mismatched execution ownership.
    /// Repeating a successful shutdown is a no-op, without duplicate halt events.
    pub fn shutdown(&mut self, at: DateTime<Utc>) -> Result<Vec<PortfolioEvent>, PortfolioError> {
        if self.phase == Phase::Stopped { return Ok(Vec::new()); }
        let was_halted = self.snapshot().halted;
        let result = self.shutdown_inner(at, was_halted);
        self.phase = if result.is_ok() { Phase::Stopped } else { Phase::Poisoned };
        result
    }

    fn shutdown_inner(&mut self, at: DateTime<Utc>, was_halted: bool) -> Result<Vec<PortfolioEvent>, PortfolioError> {
        if self.history.last_key_value().and_then(|(_, batch)| batch.first()).is_some_and(|last| at < last.close_time()) {
            return Err(PortfolioError::InvalidBatch("shutdown predates accepted market data"));
        }
        let mut events = self.cancel_pending(at)?;
        if !was_halted { events.push(PortfolioEvent::Halted { at }); }
        Ok(events)
    }

    fn run(&mut self, candles: &[Candle], warmup: bool) -> Result<PortfolioStep, PortfolioError> {
        match self.phase {
            Phase::Poisoned => return Err(PortfolioError::Poisoned),
            Phase::Stopped => return Err(PortfolioError::Stopped),
            Phase::Trading if warmup => {
                self.phase = Phase::Poisoned;
                return Err(PortfolioError::WarmupAfterTrading);
            }
            Phase::WarmingUp | Phase::Trading => {}
        }
        if !warmup { self.phase = Phase::Trading; }
        let result = self.advance(candles, warmup);
        if result.is_err() { self.phase = Phase::Poisoned; }
        result
    }

    fn advance(&mut self, candles: &[Candle], warmup: bool) -> Result<PortfolioStep, PortfolioError> {
        let ordered = self.validate_batch(candles)?;
        let mut step = PortfolioStep { snapshot: self.snapshot(), events: Vec::new(),
            analysis: self.symbols.keys().cloned().map(SymbolAnalysis::empty).collect() };
        if self.is_replay(&ordered)? { return Ok(step); }
        if warmup && !self.broker.is_flat() {
            return Err(PortfolioError::Invariant("warm-up must remain flat"));
        }
        let first = ordered.first().ok_or(PortfolioError::InvalidBatch("empty batch"))?;
        let at = first.close_time();
        let owner = self.broker.spec().instrument().venue_symbol.as_str().to_owned();
        let execution_candle = ordered.iter().find(|bar| bar.instrument().venue_symbol.as_str() == owner)
            .ok_or(PortfolioError::Invariant("broker instrument absent from complete batch"))?;
        let geometry = active_order(&self.broker.snapshot());
        let execution = self.broker.advance(execution_candle)?;
        step.events.extend(self.route_events(&owner, execution, geometry.as_ref())?);
        let was_halted = self.entry_halted();
        if self.mark_equity(at)? { step.events.extend(self.cancel_pending(at)?); }
        if !was_halted && self.entry_halted() { step.events.push(PortfolioEvent::Halted { at }); }
        for (candle, analysis) in ordered.iter().zip(&mut step.analysis) {
            let symbol = candle.instrument().venue_symbol.as_str();
            let (signals, actions) = self.state_mut(symbol)?.analyze(candle)?;
            analysis.signals = signals;
            self.handle_actions(symbol, actions, candle, warmup, analysis, &mut step.events)?;
        }
        self.history.insert(first.open_time(), ordered.into_iter().cloned().collect());
        step.snapshot = self.snapshot();
        Ok(step)
    }

    fn validate_batch<'a>(&self, candles: &'a [Candle]) -> Result<Vec<&'a Candle>, PortfolioError> {
        if candles.len() != self.symbols.len() {
            return Err(PortfolioError::InvalidBatch("exactly one candle per configured symbol is required"));
        }
        let mut ordered: Vec<_> = candles.iter().collect();
        ordered.sort_unstable_by(|left, right| left.instrument().venue_symbol.cmp(&right.instrument().venue_symbol));
        let first = ordered.first().ok_or(PortfolioError::InvalidBatch("empty batch"))?;
        for (candle, (symbol, state)) in ordered.iter().zip(&self.symbols) {
            if candle.instrument().venue_symbol.as_str() != symbol || CandleKey::from_candle(candle) != state.key {
                return Err(PortfolioError::InvalidBatch("missing, duplicate or wrong instrument"));
            }
            if !candle.is_closed() || candle.volume_unit() != VolumeUnit::BaseAsset
                || candle.close_time().signed_duration_since(candle.open_time()) != self.timeframe.duration()
                || candle.open_time().timestamp_subsec_nanos() != 0
                || candle.open_time().timestamp().rem_euclid(self.timeframe.duration().num_seconds()) != 0 {
                return Err(PortfolioError::InvalidBatch("expected final UTC-aligned M1 base-volume candles"));
            }
            if candle.open_time() != first.open_time() || candle.close_time() != first.close_time() {
                return Err(PortfolioError::InvalidBatch("symbol candles must share the same interval"));
            }
        }
        Ok(ordered)
    }

    fn is_replay(&self, ordered: &[&Candle]) -> Result<bool, PortfolioError> {
        let first = ordered.first().ok_or(PortfolioError::InvalidBatch("empty batch"))?;
        if let Some(previous) = self.history.get(&first.open_time()) {
            if previous.iter().eq(ordered.iter().copied()) { return Ok(true); }
            return Err(PaperError::ConflictingDuplicate.into());
        }
        if let Some(last) = self.history.last_key_value().and_then(|(_, batch)| batch.first()) {
            if first.open_time() < last.close_time() { return Err(PaperError::OutOfOrder.into()); }
            if first.open_time() > last.close_time() { return Err(PaperError::Gap.into()); }
        }
        if self.history.len() >= MAX_RETAINED_BATCHES { return Err(PaperError::Capacity.into()); }
        Ok(false)
    }

    fn entry_halted(&self) -> bool {
        self.insolvent || self.risk.circuit_breaker_state().is_tripped()
    }

    fn mark_equity(&mut self, at: DateTime<Utc>) -> Result<bool, PortfolioError> {
        let equity = self.broker.snapshot().equity;
        if self.insolvent { return Ok(true); }
        let mut cancel = false;
        match self.risk.circuit_breaker_state().trading_day() {
            None => { self.risk.start_trading_day(self.broker.config().initial_balance(), equity, at, false)?; }
            Some(day) if day == at.date_naive() => { self.risk.update_equity(equity, at)?; }
            Some(_) => {
                // The midnight-closing candle ends yesterday and establishes
                // today's opening mark; its old-day breach cannot evade cancel.
                let before_boundary = at.checked_sub_signed(Duration::nanoseconds(1))
                    .ok_or(PortfolioError::Invariant("midnight timestamp overflow"))?;
                self.risk.update_equity(equity, before_boundary)?;
                cancel = self.risk.circuit_breaker_state().is_tripped();
                if equity > Decimal::ZERO { self.risk.start_trading_day(equity, equity, at, false)?; }
            }
        }
        self.insolvent = equity <= Decimal::ZERO;
        Ok(cancel || self.entry_halted())
    }

    fn handle_actions(&mut self, symbol: &str, actions: Vec<StrategyAction>, candle: &Candle,
        warmup: bool, analysis: &mut SymbolAnalysis, events: &mut Vec<PortfolioEvent>) -> Result<(), PortfolioError> {
        for action in actions {
            match action {
                StrategyAction::TradeIntentGenerated(intent) => {
                    analysis.intents += 1;
                    if let Some(reason) = self.submit_intent(symbol, &intent, candle, warmup, events)? {
                        analysis.reject(reason);
                    }
                }
                StrategyAction::CancelPendingIntent { key, intent_id, at, .. } => {
                    if key != self.state(symbol)?.key || self.broker.pending_intent() != Some(intent_id)
                        || self.broker.spec().instrument() != &key.instrument {
                        return Err(PortfolioError::Invariant("strategy cancellation has no matching broker owner"));
                    }
                    events.extend(self.cancel_pending(at)?);
                }
                StrategyAction::InputRejected { .. } => analysis.reject(PortfolioRejection::StrategyInput),
                StrategyAction::SetupDiscarded { .. } | StrategyAction::DataDegraded { .. } => {}
                StrategyAction::PositionOpened { .. } | StrategyAction::PositionClosed { .. }
                    | StrategyAction::EntryCancellationConfirmed { .. } => {
                    return Err(PortfolioError::Invariant("analysis emitted an execution lifecycle"));
                }
            }
        }
        Ok(())
    }

    fn submit_intent(&mut self, symbol: &str, intent: &TradeIntent, candle: &Candle, warmup: bool,
        events: &mut Vec<PortfolioEvent>) -> Result<Option<PortfolioRejection>, PortfolioError> {
        let at = candle.close_time();
        if intent.instrument() != self.state(symbol)?.instrument.spec.instrument() {
            return Err(PortfolioError::Invariant("strategy intent instrument differs from owner"));
        }
        let rejection = if warmup { Some(PortfolioRejection::Warmup) }
            else if self.entry_halted() { Some(PortfolioRejection::Halted) }
            else if !self.broker.is_flat() { Some(PortfolioRejection::GlobalBusy) }
            else { None };
        if let Some(reason) = rejection {
            self.reject_intent(symbol, intent.id(), at, false)?;
            return Ok(Some(reason));
        }
        let Some(target) = self.state(symbol)?.nearest_target(intent, at)? else {
            self.reject_intent(symbol, intent.id(), at, false)?;
            return Ok(Some(PortfolioRejection::NoOpposingLiquidity));
        };
        let assessment = match self.risk.assess_trade(intent, &target, at) {
            Ok(assessment) => assessment,
            Err(error) if is_entry_rejection(&error) => {
                self.reject_intent(symbol, intent.id(), at, false)?;
                return Ok(Some(PortfolioRejection::RiskRule));
            }
            Err(error) => return Err(error.into()),
        };
        let TradeAssessment::Approved(plan) = assessment else {
            return Err(PortfolioError::Invariant("unexpected risk replay after batch deduplication"));
        };
        let metadata = self.state(symbol)?.instrument.clone();
        self.broker.select_instrument(metadata.spec, metadata.min_notional, candle)?;
        match self.broker.submit(*plan) {
            Ok(submitted) if submitted.is_empty() => {
                let order = self.broker.snapshot().pending.ok_or(PortfolioError::Invariant("accepted entry missing"))?;
                events.push(PortfolioEvent::EntryPlaced { symbol: symbol.to_owned(), at, order });
                Ok(None)
            }
            Ok(_) => Err(PortfolioError::Invariant("submission filled without a future candle")),
            Err(error) if error.is_rejection() => {
                let reason = match error {
                    PaperError::MinimumOrder => PortfolioRejection::MinimumOrder,
                    PaperError::InsufficientBalance => PortfolioRejection::InsufficientBalance,
                    PaperError::NetRiskReward => PortfolioRejection::NetRiskReward,
                    _ => PortfolioRejection::GlobalBusy,
                };
                self.reject_intent(symbol, intent.id(), at, true)?;
                Ok(Some(reason))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn reject_intent(&mut self, symbol: &str, intent_id: Uuid, at: DateTime<Utc>, reserved: bool) -> Result<(), PortfolioError> {
        if reserved { self.risk.apply_reservation_event(intent_id, ReservationEvent::EntryCancelled, at)?; }
        let key = self.state(symbol)?.key.clone();
        self.route_strategy(symbol, StrategyCommand::EntryCancelled { key, intent_id, cancelled_at: at })
    }

    fn cancel_pending(&mut self, at: DateTime<Utc>) -> Result<Vec<PortfolioEvent>, PortfolioError> {
        let symbol = self.broker.spec().instrument().venue_symbol.as_str().to_owned();
        let order = self.broker.snapshot().pending;
        let events = self.broker.cancel_pending(at)?;
        self.route_events(&symbol, events, order.as_ref())
    }

    fn route_events(&mut self, symbol: &str, events: Vec<PaperEvent>, order: Option<&PaperOrderSnapshot>) -> Result<Vec<PortfolioEvent>, PortfolioError> {
        let mut routed = Vec::with_capacity(events.len());
        for event in events {
            let order = order.ok_or(PortfolioError::Invariant("execution event has no pre-transition geometry"))?;
            let key = self.state(symbol)?.key.clone();
            let (intent_id, at, reservation, command) = match event {
                PaperEvent::EntryFilled { intent_id, at, .. } => (intent_id, at, ReservationEvent::EntryFilled,
                    StrategyCommand::EntryFilled { key, intent_id, filled_at: at }),
                PaperEvent::EntryCancelled { intent_id, at, .. } => (intent_id, at, ReservationEvent::EntryCancelled,
                    StrategyCommand::EntryCancelled { key, intent_id, cancelled_at: at }),
                PaperEvent::PositionClosed { intent_id, at, .. } => (intent_id, at, ReservationEvent::PositionClosed,
                    StrategyCommand::PositionClosed { key, intent_id, closed_at: at }),
            };
            if order.intent_id != intent_id {
                return Err(PortfolioError::Invariant("execution intent differs from captured order"));
            }
            self.risk.apply_reservation_event(intent_id, reservation, at)?;
            self.route_strategy(symbol, command)?;
            routed.push(PortfolioEvent::Execution { symbol: symbol.to_owned(), event, order: order.clone() });
        }
        Ok(routed)
    }

    fn route_strategy(&mut self, symbol: &str, command: StrategyCommand) -> Result<(), PortfolioError> {
        let actions = self.state_mut(symbol)?.strategy.process_command(command)?;
        if !matches!(actions.as_slice(), [StrategyAction::PositionOpened { .. }
            | StrategyAction::PositionClosed { .. } | StrategyAction::EntryCancellationConfirmed { .. }]) {
            return Err(PortfolioError::Invariant("strategy rejected execution lifecycle"));
        }
        Ok(())
    }

    fn state(&self, symbol: &str) -> Result<&SymbolState, PortfolioError> {
        self.symbols.get(symbol).ok_or(PortfolioError::Invariant("unknown symbol owner"))
    }

    fn state_mut(&mut self, symbol: &str) -> Result<&mut SymbolState, PortfolioError> {
        self.symbols.get_mut(symbol).ok_or(PortfolioError::Invariant("unknown symbol owner"))
    }
}

fn active_order(snapshot: &PaperSnapshot) -> Option<PaperOrderSnapshot> {
    snapshot.pending.as_ref().or(snapshot.position.as_ref()).cloned()
}

fn is_entry_rejection(error: &RiskError) -> bool {
    matches!(error, RiskError::CircuitBreakerTripped | RiskError::IntentExpired
        | RiskError::InvalidPriceGeometry | RiskError::MinimumRiskRewardNotMet { .. }
        | RiskError::PortfolioRiskLimitReached | RiskError::QuantityBelowMinimum { .. })
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use super::*;
    use crate::{
        models::{CandleInput, InstrumentId},
        risk::{CircuitBreakerState, InstrumentRiskSpec, ReservationStatus},
        strategy::StrategyState,
    };

    type TestResult = Result<(), Box<dyn Error>>;

    fn instrument(symbol: &str) -> Result<InstrumentId, Box<dyn Error>> {
        let base = symbol.strip_suffix("USDT").ok_or("test quote")?;
        Ok(InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual, symbol,
            format!("{base}-USDT-PERP"))?)
    }

    fn metadata(symbol: &str) -> Result<PaperInstrument, Box<dyn Error>> {
        Ok(PaperInstrument {
            spec: InstrumentRiskSpec::new(instrument(symbol)?, Decimal::new(1, 1),
                Decimal::new(1, 3), Decimal::new(1, 3), Some(Decimal::from(100)), Decimal::ONE)?,
            min_notional: Decimal::ONE,
        })
    }

    fn portfolio() -> Result<PaperPortfolio, Box<dyn Error>> {
        Ok(PaperPortfolio::new(vec![metadata("ETHUSDT")?, metadata("BTCUSDT")?],
            PaperConfig::default().with_leverage(10)?, Timeframe::M1)?)
    }

    fn at(minute: i64) -> Result<DateTime<Utc>, Box<dyn Error>> {
        Ok(DateTime::parse_from_rfc3339("2026-10-28T11:00:00Z")?.with_timezone(&Utc)
            .checked_add_signed(Duration::minutes(minute)).ok_or_else(|| io::Error::other("test time overflow"))?)
    }

    fn input(symbol: &str, minute: i64, prices: [i64; 4]) -> Result<CandleInput, Box<dyn Error>> {
        Ok(CandleInput { instrument: instrument(symbol)?, timeframe: Timeframe::M1,
            open_time: at(minute)?, close_time: at(minute + 1)?,
            open: Decimal::from(prices[0]), high: Decimal::from(prices[1]),
            low: Decimal::from(prices[2]), close: Decimal::from(prices[3]),
            volume: Decimal::ONE, volume_unit: VolumeUnit::BaseAsset, trade_count: None, is_closed: true })
    }

    fn candle(symbol: &str, minute: i64, prices: [i64; 4]) -> Result<Candle, Box<dyn Error>> {
        Ok(Candle::new(input(symbol, minute, prices)?)?)
    }

    fn batch(minute: i64, btc: [i64; 4], eth: [i64; 4]) -> Result<Vec<Candle>, Box<dyn Error>> {
        // Deliberately reverse the input order; BTC priority must not depend on it.
        Ok(vec![candle("ETHUSDT", minute, eth)?, candle("BTCUSDT", minute, btc)?])
    }

    fn setup(target: i64) -> Vec<[i64; 4]> {
        // Actual 5-candle fractals and 20-body displacement: bearish BOS, SSL
        // sweep, bullish MSS/FVG, then retracement. No injected SignalEvents.
        let mut prices = vec![[100, 102, 98, 101]; 9];
        prices[2] = [100, target, 98, 101];
        prices.extend([
            [100, 102, 95, 101], [100, 102, 97, 101], [100, 102, 98, 101],
            [100, 105, 98, 101], [100, 103, 97, 100], [99, 102, 96, 98],
            [98, 99, 93, 94], [94, 97, 92, 94], [94, 97, 91, 93],
            [93, 96, 90, 93], [94, 98, 92, 96], [96, 99, 93, 97],
            [97, 100, 89, 94], [94, 112, 94, 111], [110, 113, 108, 111],
            [110, 112, 103, 109],
        ]);
        prices
    }

    fn ready_pending() -> Result<PaperPortfolio, Box<dyn Error>> {
        let mut engine = portfolio()?;
        for (minute, prices) in setup(150).into_iter().enumerate() {
            engine.process_batch(&batch(i64::try_from(minute)?, prices, prices)?)?;
        }
        if engine.snapshot().account.pending.is_none() { return Err("fixture did not create pending entry".into()); }
        Ok(engine)
    }

    #[test]
    fn construction_rejects_duplicate_unknown_wrong_venue_and_non_m1_assets() -> TestResult {
        assert!(PaperPortfolio::new(Vec::new(), PaperConfig::default(), Timeframe::M1).is_err());
        assert!(PaperPortfolio::new(vec![metadata("BTCUSDT")?, metadata("BTCUSDT")?],
            PaperConfig::default(), Timeframe::M1).is_err());
        assert!(PaperPortfolio::new(vec![metadata("SOLUSDT")?], PaperConfig::default(), Timeframe::M1).is_err());
        assert!(PaperPortfolio::new(vec![metadata("BTCUSDT")?], PaperConfig::default(), Timeframe::M5).is_err());
        let wrong = InstrumentId::new(Exchange::Binance, MarketKind::LinearPerpetual, "BTCUSDT", "BTC-USDT-PERP")?;
        let spec = InstrumentRiskSpec::new(wrong, Decimal::ONE, Decimal::ONE, Decimal::ONE, None, Decimal::ONE)?;
        assert!(PaperPortfolio::new(vec![PaperInstrument { spec, min_notional: Decimal::ONE }],
            PaperConfig::default(), Timeframe::M1).is_err());
        let snapshot = portfolio()?.snapshot();
        assert_eq!(snapshot.account.initial_balance, Decimal::from(100));
        assert_eq!(snapshot.account.balance, Decimal::from(100));
        assert_eq!(snapshot.leverage, 10);
        assert!(!snapshot.halted);
        assert!(snapshot.active_symbol.is_none());
        Ok(())
    }

    #[test]
    fn entire_batch_is_checked_before_pending_broker_can_fill() -> TestResult {
        for invalid in [
            vec![candle("BTCUSDT", 25, [109, 110, 103, 109])?],
            vec![candle("BTCUSDT", 25, [109, 110, 103, 109])?, candle("BTCUSDT", 25, [109, 110, 103, 109])?],
            vec![candle("BTCUSDT", 25, [109, 110, 103, 109])?, candle("ETHUSDT", 26, [109, 110, 103, 109])?],
            vec![candle("BTCUSDT", 25, [109, 110, 103, 109])?, candle("SOLUSDT", 25, [109, 110, 103, 109])?],
        ] {
            let mut engine = ready_pending()?;
            let before = engine.snapshot().account;
            assert!(engine.process_batch(&invalid).is_err());
            assert_eq!(engine.snapshot().account, before);
            assert!(engine.snapshot().halted);
            assert!(matches!(engine.process_batch(&batch(25, [109, 110, 103, 109], [109, 110, 103, 109])?),
                Err(PortfolioError::Poisoned)));
        }
        Ok(())
    }

    #[test]
    fn nonfinal_wrong_units_and_misaligned_candles_fail_before_analysis() -> TestResult {
        let mut unfinished = input("ETHUSDT", 0, [100, 102, 98, 101])?;
        unfinished.is_closed = false;
        let mut wrong_units = input("ETHUSDT", 0, [100, 102, 98, 101])?;
        wrong_units.volume_unit = VolumeUnit::QuoteAsset;
        let mut shifted = input("ETHUSDT", 0, [100, 102, 98, 101])?;
        shifted.open_time += Duration::seconds(1);
        shifted.close_time += Duration::seconds(1);
        for invalid in [unfinished, wrong_units, shifted] {
            let mut engine = portfolio()?;
            let before = engine.snapshot().account;
            let bars = vec![candle("BTCUSDT", 0, [100, 102, 98, 101])?, Candle::new(invalid)?];
            assert!(engine.process_batch(&bars).is_err());
            assert_eq!(engine.snapshot().account, before);
            assert!(engine.history.is_empty());
        }
        Ok(())
    }

    #[test]
    fn deterministic_batch_order_and_exact_old_replay_preserve_shared_state() -> TestResult {
        let mut first = portfolio()?;
        let mut second = portfolio()?;
        for (minute, prices) in setup(150).into_iter().enumerate() {
            let mut bars = batch(i64::try_from(minute)?, prices, prices)?;
            let expected = first.process_batch(&bars)?;
            bars.reverse();
            assert_eq!(expected, second.process_batch(&bars)?);
        }
        let before = first.snapshot();
        let replay = first.process_batch(&batch(0, [100, 102, 98, 101], [100, 102, 98, 101])?)?;
        assert_eq!(replay.snapshot, before);
        assert!(replay.events.is_empty());
        assert!(replay.analysis.iter().all(|row| row.signals == 0 && row.intents == 0 && row.rejected == 0));
        assert_eq!(replay.analysis[0].symbol, "BTCUSDT");
        assert_eq!(replay.analysis[1].symbol, "ETHUSDT");
        Ok(())
    }

    #[test]
    fn changed_replay_and_gap_poison_without_account_mutation() -> TestResult {
        let mut engine = ready_pending()?;
        let before = engine.snapshot().account;
        assert!(matches!(engine.process_batch(&batch(0, [100, 102, 98, 100], [100, 102, 98, 101])?),
            Err(PortfolioError::Broker(PaperError::ConflictingDuplicate))));
        assert_eq!(engine.snapshot().account, before);
        let cancel = engine.shutdown(at(25)?)?;
        assert!(matches!(cancel.as_slice(), [PortfolioEvent::Execution { event: PaperEvent::EntryCancelled { .. }, .. }]));
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        let mut engine = ready_pending()?;
        let before = engine.snapshot().account;
        assert!(matches!(engine.process_batch(&batch(26, [109, 110, 103, 109], [109, 110, 103, 109])?),
            Err(PortfolioError::Broker(PaperError::Gap))));
        assert_eq!(engine.snapshot().account, before);
        Ok(())
    }

    #[test]
    fn warmup_never_duplicates_balance_or_generates_historical_orders() -> TestResult {
        let mut engine = portfolio()?;
        let mut intents = 0;
        for (minute, prices) in setup(150).into_iter().enumerate() {
            let step = engine.warm_up(&batch(i64::try_from(minute)?, prices, prices)?)?;
            intents += step.analysis.iter().map(|row| row.intents).sum::<usize>();
            assert!(step.events.is_empty());
            assert_eq!(step.snapshot.account.balance, Decimal::from(100));
            assert_eq!(step.snapshot.account.fees_paid, Decimal::ZERO);
            assert!(step.snapshot.active_symbol.is_none());
            assert!(step.analysis.iter().flat_map(|row| &row.rejection_reasons)
                .all(|reason| *reason == PortfolioRejection::Warmup));
        }
        assert_eq!(intents, 2);
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        engine.process_batch(&batch(25, [109, 110, 103, 109], [109, 110, 103, 109])?)?;
        assert!(matches!(engine.warm_up(&batch(26, [109, 110, 103, 109], [109, 110, 103, 109])?),
            Err(PortfolioError::WarmupAfterTrading)));
        Ok(())
    }

    #[test]
    fn simultaneous_signals_prioritize_btc_and_do_not_reserve_eth_risk() -> TestResult {
        let mut engine = portfolio()?;
        let prices = setup(150);
        for (minute, value) in prices.iter().take(24).enumerate() {
            engine.warm_up(&batch(i64::try_from(minute)?, *value, *value)?)?;
        }
        let trigger = *prices.last().ok_or("fixture empty")?;
        let step = engine.process_batch(&batch(24, trigger, trigger)?)?;
        assert_eq!(step.snapshot.active_symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(step.analysis[0].intents, 1);
        assert_eq!(step.analysis[0].rejected, 0);
        assert_eq!(step.analysis[1].rejection_reasons, vec![PortfolioRejection::GlobalBusy]);
        assert!(matches!(step.events.as_slice(), [PortfolioEvent::EntryPlaced { symbol, .. }] if symbol == "BTCUSDT"));
        let pending = step.snapshot.account.pending.ok_or("pending missing")?;
        assert_eq!(engine.risk.reservation_status(pending.intent_id), Some(ReservationStatus::Pending));
        assert!(engine.risk.active_reserved_risk()? <= Decimal::ONE);
        let eth = engine.state("ETHUSDT")?;
        assert!(eth.strategy.pending_intent(&eth.key).is_none());
        assert_eq!(step.snapshot.account.initial_balance, Decimal::from(100));
        Ok(())
    }

    #[test]
    fn fill_and_same_bar_stop_keep_geometry_and_route_only_owning_strategy() -> TestResult {
        let mut engine = ready_pending()?;
        let pending = engine.snapshot().account.pending.ok_or("pending missing")?;
        let step = engine.process_batch(&batch(25, [109, 110, 80, 100], [109, 110, 103, 109])?)?;
        assert!(matches!(step.events.as_slice(), [
            PortfolioEvent::Execution { symbol: a, event: PaperEvent::EntryFilled { .. }, order: entry },
            PortfolioEvent::Execution { symbol: b, event: PaperEvent::PositionClosed { .. }, order: exit },
        ] if a == "BTCUSDT" && b == "BTCUSDT" && entry == &pending && exit == &pending));
        assert_eq!(step.snapshot.account.completed_trades, 1);
        assert_eq!(step.snapshot.account.losses, 1);
        assert_eq!(step.snapshot.active_symbol, None);
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        assert!(engine.symbols.values().all(|state| state.strategy.state(&state.key) != StrategyState::InPosition));
        let encoded = serde_json::to_value(&step.events)?;
        assert_eq!(encoded[0]["kind"], "execution");
        assert_eq!(encoded[0]["event"]["event"], "entry_filled");
        Ok(())
    }

    #[test]
    fn eth_owner_advances_its_candle_before_unrelated_btc_price_move() -> TestResult {
        let mut engine = portfolio()?;
        for (minute, prices) in setup(150).into_iter().enumerate() {
            engine.process_batch(&batch(i64::try_from(minute)?, [100, 102, 98, 101], prices)?)?;
        }
        assert_eq!(engine.snapshot().active_symbol.as_deref(), Some("ETHUSDT"));
        let fill = engine.process_batch(&batch(25, [2, 3, 1, 2], [109, 110, 103, 109])?)?;
        assert!(matches!(fill.events.as_slice(), [PortfolioEvent::Execution { symbol, event: PaperEvent::EntryFilled { .. }, .. }] if symbol == "ETHUSDT"));
        let close = engine.process_batch(&batch(26, [2, 3, 1, 2], [109, 151, 109, 150])?)?;
        assert_eq!(close.snapshot.account.completed_trades, 1);
        assert!(close.snapshot.account.balance > Decimal::from(100));
        assert_eq!(close.snapshot.active_symbol, None);
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn symbol_switch_after_completed_trade_preserves_shared_profit_and_counters() -> TestResult {
        let mut engine = portfolio()?;
        // ETH remains neutral during the BTC trade. Repeating a consumed ETH
        // setup would not imply another valid MSS in its retained ICT context.
        for (minute, prices) in setup(150).into_iter().enumerate() {
            engine.process_batch(&batch(i64::try_from(minute)?, prices, [100, 102, 98, 101])?)?;
        }
        engine.process_batch(&batch(25, [109, 110, 103, 109], [100, 102, 98, 101])?)?;
        engine.process_batch(&batch(26, [109, 151, 109, 150], [100, 102, 98, 101])?)?;
        let cash = engine.snapshot().account.balance;
        assert!(cash > Decimal::from(100));
        let mut last_step = None;
        for (index, prices) in setup(150).into_iter().enumerate() {
            last_step = Some(engine.process_batch(&batch(27 + i64::try_from(index)?, [110, 112, 109, 111], prices)?)?);
        }
        let snapshot = engine.snapshot();
        assert_eq!(snapshot.active_symbol.as_deref(), Some("ETHUSDT"), "last analysis: {last_step:?}");
        assert_eq!(snapshot.account.balance, cash);
        assert_eq!(snapshot.account.initial_balance, Decimal::from(100));
        assert_eq!(snapshot.account.completed_trades, 1);
        assert!(snapshot.account.fees_paid > Decimal::ZERO);
        assert!(engine.risk.active_reserved_risk()? <= cash.checked_div(Decimal::from(100)).ok_or("test division")?);
        Ok(())
    }

    #[test]
    fn rejected_btc_minimum_releases_reservation_before_eligible_eth_entry() -> TestResult {
        let mut btc = metadata("BTCUSDT")?;
        btc.min_notional = Decimal::from(100);
        let mut engine = PaperPortfolio::new(vec![btc, metadata("ETHUSDT")?],
            PaperConfig::default().with_leverage(10)?, Timeframe::M1)?;
        let mut last = None;
        for (minute, prices) in setup(150).into_iter().enumerate() {
            last = Some(engine.process_batch(&batch(i64::try_from(minute)?, prices, prices)?)?);
        }
        let step = last.ok_or("fixture empty")?;
        assert_eq!(step.analysis[0].rejection_reasons, vec![PortfolioRejection::MinimumOrder]);
        assert_eq!(step.analysis[1].rejected, 0);
        assert_eq!(step.snapshot.active_symbol.as_deref(), Some("ETHUSDT"));
        assert!(engine.risk.active_reserved_risk()? <= Decimal::ONE);
        let btc = engine.state("BTCUSDT")?;
        assert!(btc.strategy.pending_intent(&btc.key).is_none());
        Ok(())
    }

    #[test]
    fn nearest_liquidity_rr_rejection_does_not_invent_a_farther_target() -> TestResult {
        let mut engine = portfolio()?;
        let mut reasons = Vec::new();
        for (minute, prices) in setup(120).into_iter().enumerate() {
            let step = engine.process_batch(&batch(i64::try_from(minute)?, prices, prices)?)?;
            reasons.extend(step.analysis.into_iter().flat_map(|row| row.rejection_reasons));
        }
        assert_eq!(reasons, vec![PortfolioRejection::RiskRule, PortfolioRejection::RiskRule]);
        assert!(engine.broker.is_flat());
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn absent_opposing_liquidity_is_reported_without_risk_reservation() -> TestResult {
        let mut engine = portfolio()?;
        let prices = setup(150);
        for (minute, value) in prices.iter().take(24).enumerate() {
            engine.warm_up(&batch(i64::try_from(minute)?, *value, *value)?)?;
        }
        for state in engine.symbols.values_mut() { state.liquidity.clear(); }
        let trigger = *prices.last().ok_or("fixture empty")?;
        let step = engine.process_batch(&batch(24, trigger, trigger)?)?;
        assert!(step.analysis.iter().all(|row| row.rejection_reasons == vec![PortfolioRejection::NoOpposingLiquidity]));
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        assert!(engine.broker.is_flat());
        Ok(())
    }

    #[test]
    fn shutdown_cancels_only_pending_and_emits_no_duplicate_halt() -> TestResult {
        let mut engine = ready_pending()?;
        let events = engine.shutdown(at(25)?)?;
        assert!(matches!(events.as_slice(), [PortfolioEvent::Execution { event: PaperEvent::EntryCancelled { .. }, .. }, PortfolioEvent::Halted { .. }]));
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        assert!(engine.snapshot().halted);
        assert!(engine.shutdown(at(25)?)?.is_empty());
        let mut engine = ready_pending()?;
        engine.process_batch(&batch(25, [109, 110, 103, 109], [109, 110, 103, 109])?)?;
        let before = engine.snapshot().account;
        assert!(matches!(engine.shutdown(at(26)?)?.as_slice(), [PortfolioEvent::Halted { .. }]));
        assert_eq!(engine.snapshot().account, before);
        assert_eq!(engine.snapshot().active_symbol.as_deref(), Some("BTCUSDT"));
        assert!(engine.risk.active_reserved_risk()? > Decimal::ZERO);
        assert!(matches!(engine.process_batch(&batch(26, [109, 151, 109, 150], [109, 110, 103, 109])?), Err(PortfolioError::Stopped)));
        Ok(())
    }

    #[test]
    fn shared_gap_loss_trips_one_account_and_shutdown_does_not_repeat_halt() -> TestResult {
        let mut engine = ready_pending()?;
        engine.process_batch(&batch(25, [109, 110, 103, 109], [109, 110, 103, 109])?)?;
        let loss = engine.process_batch(&batch(26, [2, 3, 1, 2], [109, 110, 103, 109])?)?;
        assert!(loss.snapshot.account.equity < Decimal::from(95));
        assert!(loss.snapshot.halted);
        assert_eq!(loss.events.iter().filter(|event| matches!(event, PortfolioEvent::Halted { .. })).count(), 1);
        let after = engine.process_batch(&batch(27, [2, 3, 1, 2], [109, 110, 103, 109])?)?;
        assert!(after.snapshot.halted);
        assert!(!after.events.iter().any(|event| matches!(event, PortfolioEvent::Halted { .. })));
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        assert!(engine.shutdown(at(28)?)?.is_empty());
        Ok(())
    }

    #[test]
    fn midnight_rollover_marks_shared_equity_once_and_rearms_positive_account() -> TestResult {
        let mut engine = portfolio()?;
        engine.warm_up(&batch(778, [100, 101, 99, 100], [200, 201, 199, 200])?)?;
        engine.risk.update_equity(Decimal::from(94), at(779)? + Duration::seconds(1))?;
        engine.risk.update_equity(Decimal::from(100), at(779)? + Duration::seconds(2))?;
        assert!(engine.snapshot().halted);
        let step = engine.warm_up(&batch(779, [100, 101, 99, 100], [200, 201, 199, 200])?)?;
        assert!(!step.snapshot.halted);
        assert!(matches!(engine.risk.circuit_breaker_state(), CircuitBreakerState::Armed { opening_equity, trading_day, .. }
            if opening_equity == Decimal::from(100) && trading_day == at(780)?.date_naive()));
        assert_eq!(step.snapshot.account.balance, Decimal::from(100));
        Ok(())
    }

    #[test]
    fn insolvent_short_gap_is_not_clipped_or_rearmed_at_a_later_mark() -> TestResult {
        let mut engine = portfolio()?;
        for (minute, prices) in setup(150).into_iter().enumerate() {
            let short = [200 - prices[0], 200 - prices[2], 200 - prices[1], 200 - prices[3]];
            engine.process_batch(&batch(i64::try_from(minute)?, short, [100, 102, 98, 101])?)?;
        }
        let pending = engine.snapshot().account.pending.ok_or("short fixture missing entry")?;
        assert_eq!(pending.direction, Direction::Bearish);
        engine.process_batch(&batch(25, [91, 97, 90, 91], [100, 102, 98, 101])?)?;
        let loss = engine.process_batch(&batch(26, [10_000, 10_001, 9999, 10_000], [100, 102, 98, 101])?)?;
        assert!(loss.snapshot.account.equity < Decimal::ZERO);
        assert!(engine.insolvent);
        assert!(loss.snapshot.halted);
        assert!(engine.mark_equity(at(800)?)?);
        assert!(engine.snapshot().halted);
        assert_eq!(engine.snapshot().account, loss.snapshot.account);
        Ok(())
    }
}
