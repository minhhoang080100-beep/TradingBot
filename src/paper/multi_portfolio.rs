//! V3 PAPER orchestration with independent setup discovery and real ownership.
//!
//! A shared book and risk manager own all cash and reservations. Each accepted
//! intent keeps the actual strategy that produced it until its real cancellation
//! or exit; a fresh discovery strategy receives subsequent analyses from the
//! same persistent ICT detector. No synthetic fill or close frees a strategy.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    candle_store::CandleKey,
    ict_engine::{IctDetectionBatch, IctEngine, IctEngineConfig, IctEngineEvent},
    models::{Candle, Direction, Exchange, LiquiditySide, MarketKind, SwingKind, SwingPoint,
        Timeframe, TradeIntent, VolumeUnit},
    risk::{LiquidityTarget, ReservationEvent, RiskConfig, RiskError, RiskManager, TradeAssessment},
    strategy::{SetupDiscardReason, StrategyAction, StrategyCommand, StrategyConfig, StrategyEngine, StrategyStage},
};

use super::{
    book::{BookError, BookSnapshot, PaperBook},
    broker::{PaperConfig, PaperError, PaperEvent},
    market::PaperInstrument,
    portfolio::{PortfolioError, PortfolioEvent},
};

const MAX_RETAINED_BATCHES: usize = 100_000;
const MAX_CONSUMED_SETUPS: usize = 100_000;

/// The single shared V3 account and its entry-admission status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MultiPortfolioSnapshot {
    /// Shared cash, equity, reservations and every pending/open order.
    pub account: BookSnapshot,
    /// Entries are blocked by drawdown, insolvency, failure or shutdown.
    pub halted: bool,
}

/// The most recent pre-intent setup discard, retained as historical context.
///
/// This observation does not imply that its reason still prevents an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SetupDiscardDiagnostic {
    /// Candle close at which the discovery strategy discarded the setup.
    pub at: DateTime<Utc>,
    /// The exact strategy reason, never inferred from the absence of an order.
    pub reason: SetupDiscardReason,
}

/// Bounded, read-only discovery diagnostics outside the deterministic journal.
///
/// Existing positions belong to separate owner strategies and remain described
/// by the account snapshot. This structure describes the next setup search.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultiSymbolDiagnostics {
    /// Exact configured venue symbol.
    pub symbol: String,
    /// Most recent candle close analyzed for this symbol, including warm-up.
    pub last_close: Option<DateTime<Utc>>,
    /// Current discovery stage; it does not promise an executable entry.
    pub discovery_stage: StrategyStage,
    /// Latest historical discard, absent until a setup has been discarded.
    pub last_discard: Option<SetupDiscardDiagnostic>,
}

/// Recoverable V3 rejection reasons; slot, hedge and risk limits are distinct.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MultiRejection {
    /// Historical input must not trade.
    Warmup,
    /// Account drawdown or insolvency prevents new risk.
    Halted,
    /// This MSS or FVG already produced a candidate, even if it was rejected.
    DuplicateSetup,
    /// Four global pending/open slots are already occupied.
    GlobalSlots,
    /// Two pending/open slots for this symbol are already occupied.
    SymbolSlots,
    /// Opposite-direction exposure on one symbol is deliberately unsupported.
    OpposingDirection,
    /// Prospective portfolio risk capacity is exhausted.
    PortfolioRisk,
    /// No confirmed, unswept opposing liquidity exists.
    NoOpposingLiquidity,
    /// Another risk-manager rule rejected the candidate.
    RiskRule,
    /// Venue minimum quantity or notional was not reached.
    MinimumOrder,
    /// Shared cash cannot fund the conservative margin/fee reservation.
    InsufficientBalance,
    /// Fees and adverse stop slippage violate the net RR floor.
    NetRiskReward,
    /// Strategy rejected an inconsistent analysis input.
    StrategyInput,
}

/// Per-symbol counters for this batch, not cumulative session totals.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MultiSymbolAnalysis {
    /// Exact venue symbol, sorted deterministically BTC before ETH.
    pub symbol: String,
    /// ICT signals produced once by the persistent detector.
    pub signals: usize,
    /// Newly discovered entry intents, including subsequently rejected ones.
    pub intents: usize,
    /// Number of rejected intents or inputs.
    pub rejected: usize,
    /// Rejection categories in processing order.
    pub rejection_reasons: Vec<MultiRejection>,
}

impl MultiSymbolAnalysis {
    fn empty(symbol: String) -> Self {
        Self { symbol, signals: 0, intents: 0, rejected: 0, rejection_reasons: Vec::new() }
    }

    fn reject(&mut self, reason: MultiRejection) {
        self.rejected += 1;
        self.rejection_reasons.push(reason);
    }
}

/// Deterministic effects of one complete batch, or an exact replay no-op.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MultiPortfolioStep {
    /// Shared account after execution, equity marking and discovery.
    pub snapshot: MultiPortfolioSnapshot,
    /// Actual book lifecycle events, retaining owning-symbol order geometry.
    pub events: Vec<PortfolioEvent>,
    /// Independent symbol analyses in sorted symbol order.
    pub analysis: Vec<MultiSymbolAnalysis>,
}

/// Fatal V3 failure. Ordinary admission rejections are reported in the step.
#[derive(Debug, thiserror::Error)]
pub enum MultiPortfolioError {
    /// Shared chronology, strategy, detector or risk failure.
    #[error(transparent)]
    Portfolio(#[from] PortfolioError),
    /// Shared book accounting or lifecycle failure.
    #[error(transparent)]
    Book(#[from] BookError),
}

impl From<RiskError> for MultiPortfolioError {
    fn from(error: RiskError) -> Self { Self::Portfolio(PortfolioError::Risk(error)) }
}

impl From<PaperError> for MultiPortfolioError {
    fn from(error: PaperError) -> Self { Self::Portfolio(PortfolioError::Broker(error)) }
}

impl From<crate::strategy::StrategyError> for MultiPortfolioError {
    fn from(error: crate::strategy::StrategyError) -> Self { Self::Portfolio(PortfolioError::Strategy(error)) }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase { WarmingUp, Trading, Stopped, Poisoned }

#[derive(Debug)]
struct SymbolState {
    instrument: PaperInstrument,
    key: CandleKey,
    ict: IctEngine,
    discovery: StrategyEngine,
    liquidity: VecDeque<SwingPoint>,
    liquidity_capacity: usize,
    last_analyzed_close: Option<DateTime<Utc>>,
    last_discard: Option<SetupDiscardDiagnostic>,
}

impl SymbolState {
    fn new(instrument: PaperInstrument, timeframe: Timeframe) -> Self {
        let config = IctEngineConfig::default();
        let liquidity_capacity = config.limits().swing_capacity().get();
        Self { key: CandleKey { instrument: instrument.spec.instrument().clone(), timeframe },
            instrument, ict: IctEngine::new(config),
            discovery: StrategyEngine::new(StrategyConfig::default()),
            liquidity: VecDeque::new(), liquidity_capacity,
            last_analyzed_close: None, last_discard: None }
    }

    fn analyze(&mut self, candle: &Candle) -> Result<IctDetectionBatch, PortfolioError> {
        // Every strict wick break consumes liquidity, including a continuation
        // which is not a close-back liquidity sweep.
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
            while self.liquidity.len() > self.liquidity_capacity { self.liquidity.pop_front(); }
        }
        self.last_analyzed_close = Some(candle.close_time());
        Ok(batch)
    }

    fn nearest_target(&self, intent: &TradeIntent, at: DateTime<Utc>) -> Result<Option<LiquidityTarget>, RiskError> {
        self.liquidity.iter().filter(|swing| swing.confirmed_at() <= at)
            .filter(|swing| match intent.direction() {
                Direction::Bullish => swing.kind() == SwingKind::High && swing.price() > intent.entry_price(),
                Direction::Bearish => swing.kind() == SwingKind::Low && swing.price() < intent.entry_price(),
            })
            .min_by(|left, right| match intent.direction() {
                Direction::Bullish => left.price().cmp(&right.price()),
                Direction::Bearish => right.price().cmp(&left.price()),
            })
            .map(|swing| {
                let identity = format!("paper-multi-liquidity:{:?}:{:?}:{}:{}", self.key,
                    swing.kind(), swing.candle_open_time(), swing.price().normalize());
                LiquidityTarget::new(Uuid::new_v5(&Uuid::NAMESPACE_OID, identity.as_bytes()),
                    self.key.instrument.clone(), match swing.kind() {
                        SwingKind::High => LiquiditySide::BuySide,
                        SwingKind::Low => LiquiditySide::SellSide,
                    }, swing.price(), swing.confirmed_at())
            }).transpose()
    }
}

/// The actual strategy that produced one intent, never a synthetic replacement.
#[derive(Debug)]
struct IntentOwner {
    key: CandleKey,
    strategy: StrategyEngine,
}

/// Shared PAPER book with up to four orders, at most two per BTC/ETH symbol.
///
/// Risk is at most 1% for a new trade and 3% prospective aggregate risk. Already
/// open positions are not liquidated merely because subsequent equity falls.
/// Exact batches and consumed MSS/FVG identities are retained with hard 100,000
/// limits; capacity exhaustion fails closed instead of forgetting ownership.
#[derive(Debug)]
pub struct MultiPortfolio {
    symbols: BTreeMap<String, SymbolState>,
    owners: BTreeMap<Uuid, IntentOwner>,
    consumed_mss: BTreeSet<(CandleKey, Uuid)>,
    consumed_fvg: BTreeSet<(CandleKey, Uuid)>,
    timeframe: Timeframe,
    risk: RiskManager,
    book: PaperBook,
    history: BTreeMap<DateTime<Utc>, Vec<Candle>>,
    phase: Phase,
    insolvent: bool,
}

impl MultiPortfolio {
    /// Creates one shared account with independent persistent ICT contexts.
    ///
    /// # Errors
    /// Rejects unsupported/duplicate instruments, non-M1 input and bad metadata.
    pub fn new(instruments: Vec<PaperInstrument>, config: PaperConfig, timeframe: Timeframe) -> Result<Self, MultiPortfolioError> {
        if instruments.is_empty() || instruments.len() > 2 || timeframe != Timeframe::M1 {
            return Err(PortfolioError::Configuration("expected one or two M1 instruments").into());
        }
        let risk_config = RiskConfig::new_with_safety_limits(Decimal::new(1, 2), Decimal::new(3, 2),
            Decimal::new(5, 2), Decimal::from(2), 1, Duration::seconds(5))?;
        let mut risk = RiskManager::new(risk_config);
        let mut symbols = BTreeMap::new();
        for instrument in instruments {
            let identity = instrument.spec.instrument();
            let canonical = match identity.venue_symbol.as_str() {
                "BTCUSDT" => "BTC-USDT-PERP",
                "ETHUSDT" => "ETH-USDT-PERP",
                _ => return Err(PortfolioError::Configuration("only BTCUSDT and ETHUSDT are supported").into()),
            };
            if identity.exchange != Exchange::Bybit || identity.market != MarketKind::LinearPerpetual
                || identity.canonical_symbol.as_str() != canonical
                || instrument.spec.quote_value_per_price_unit() != Decimal::ONE
                || instrument.min_notional < Decimal::ZERO {
                return Err(PortfolioError::Configuration("expected canonical Bybit linear USDT metadata").into());
            }
            let symbol = identity.venue_symbol.as_str().to_owned();
            risk.register_instrument_spec(instrument.spec.clone())?;
            if symbols.insert(symbol, SymbolState::new(instrument, timeframe)).is_some() {
                return Err(PortfolioError::Configuration("duplicate instrument").into());
            }
        }
        Ok(Self { symbols, owners: BTreeMap::new(), consumed_mss: BTreeSet::new(),
            consumed_fvg: BTreeSet::new(), timeframe, risk, book: PaperBook::new(config)?,
            history: BTreeMap::new(), phase: Phase::WarmingUp, insolvent: false })
    }

    /// Initializes symbol contexts without creating historical orders or fills.
    ///
    /// # Errors
    /// Invalid input or warm-up after trading poisons further market processing.
    pub fn warm_up(&mut self, candles: &[Candle]) -> Result<MultiPortfolioStep, MultiPortfolioError> {
        self.run(candles, true)
    }

    /// Executes all current owners, marks shared equity once, then discovers ideas.
    ///
    /// # Errors
    /// Incomplete, changed or discontinuous input and internal failures poison
    /// the session. Ordinary admission rejections remain observable in the step.
    pub fn process_batch(&mut self, candles: &[Candle]) -> Result<MultiPortfolioStep, MultiPortfolioError> {
        self.run(candles, false)
    }

    /// Reports shared accounting, never a sum of independent symbol balances.
    #[must_use]
    pub fn snapshot(&self) -> MultiPortfolioSnapshot {
        MultiPortfolioSnapshot { account: self.book.snapshot(),
            halted: self.entry_halted() || matches!(self.phase, Phase::Stopped | Phase::Poisoned) }
    }

    /// Reports the next-setup search for each symbol in stable symbol order.
    ///
    /// Retains only one discard per symbol. Reading diagnostics neither changes
    /// strategy/account state nor adds fields to versioned replay records.
    #[must_use]
    pub fn diagnostics(&self) -> Vec<MultiSymbolDiagnostics> {
        self.symbols.iter().map(|(symbol, state)| MultiSymbolDiagnostics {
            symbol: symbol.clone(),
            last_close: state.last_analyzed_close,
            discovery_stage: state.discovery.stage(&state.key),
            last_discard: state.last_discard,
        }).collect()
    }

    /// Whether a fatal transition requires diagnostic-only session recording.
    /// Capture before shutdown, which can successfully change the phase to stopped.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool { matches!(self.phase, Phase::Poisoned) }

    /// Cancels resting entries and halts, without flattening open positions.
    ///
    /// # Errors
    /// Rejects time before the last accepted batch or inconsistent ownership.
    /// A repeated successful shutdown is a no-op.
    pub fn shutdown(&mut self, at: DateTime<Utc>) -> Result<Vec<PortfolioEvent>, MultiPortfolioError> {
        if self.phase == Phase::Stopped { return Ok(Vec::new()); }
        let was_halted = self.snapshot().halted;
        let result = self.shutdown_inner(at, was_halted);
        self.phase = if result.is_ok() { Phase::Stopped } else { Phase::Poisoned };
        result
    }

    fn shutdown_inner(&mut self, at: DateTime<Utc>, was_halted: bool) -> Result<Vec<PortfolioEvent>, MultiPortfolioError> {
        if self.history.last_key_value().and_then(|(_, batch)| batch.first()).is_some_and(|last| at < last.close_time()) {
            return Err(PortfolioError::InvalidBatch("shutdown predates accepted market data").into());
        }
        let mut events = self.book.cancel_pending(at)?;
        self.route_events(&events)?;
        if !was_halted { events.push(PortfolioEvent::Halted { at }); }
        Ok(events)
    }

    fn run(&mut self, candles: &[Candle], warmup: bool) -> Result<MultiPortfolioStep, MultiPortfolioError> {
        match self.phase {
            Phase::Poisoned => return Err(PortfolioError::Poisoned.into()),
            Phase::Stopped => return Err(PortfolioError::Stopped.into()),
            Phase::Trading if warmup => {
                self.phase = Phase::Poisoned;
                return Err(PortfolioError::WarmupAfterTrading.into());
            }
            Phase::WarmingUp | Phase::Trading => {}
        }
        if !warmup { self.phase = Phase::Trading; }
        let result = self.advance(candles, warmup);
        if result.is_err() { self.phase = Phase::Poisoned; }
        result
    }

    fn advance(&mut self, candles: &[Candle], warmup: bool) -> Result<MultiPortfolioStep, MultiPortfolioError> {
        let ordered = self.validate_batch(candles)?;
        let mut step = MultiPortfolioStep { snapshot: self.snapshot(), events: Vec::new(),
            analysis: self.symbols.keys().cloned().map(MultiSymbolAnalysis::empty).collect() };
        if self.is_replay(&ordered)? { return Ok(step); }
        if warmup && !self.owners.is_empty() {
            return Err(PortfolioError::Invariant("warm-up must remain flat").into());
        }
        let first = ordered.first().ok_or(PortfolioError::InvalidBatch("empty batch"))?;
        let at = first.close_time();
        step.events = self.book.advance_batch(candles)?;
        self.route_events(&step.events)?;
        let was_halted = self.entry_halted();
        if self.mark_equity(at)? {
            let cancellations = self.book.cancel_pending(at)?;
            self.route_events(&cancellations)?;
            step.events.extend(cancellations);
        }
        if !was_halted && self.entry_halted() { step.events.push(PortfolioEvent::Halted { at }); }
        for (candle, analysis) in ordered.iter().zip(&mut step.analysis) {
            let symbol = candle.instrument().venue_symbol.as_str();
            let batch = self.state_mut(symbol)?.analyze(candle)?;
            analysis.signals = batch.signals().len();
            let event = IctEngineEvent::CandleAnalyzed { candle: (*candle).clone(), batch };
            self.advance_owners(symbol, &event, analysis, &mut step.events)?;
            let actions = self.state_mut(symbol)?.discovery.process_event(event)?;
            self.discovery_actions(symbol, actions, candle, warmup, analysis, &mut step.events)?;
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

    fn entry_halted(&self) -> bool { self.insolvent || self.risk.circuit_breaker_state().is_tripped() }

    fn mark_equity(&mut self, at: DateTime<Utc>) -> Result<bool, PortfolioError> {
        let equity = self.book.snapshot().equity;
        if self.insolvent { return Ok(true); }
        let mut cancel = false;
        match self.risk.circuit_breaker_state().trading_day() {
            None => { self.risk.start_trading_day(self.book.config().initial_balance(), equity, at, false)?; }
            Some(day) if day == at.date_naive() => { self.risk.update_equity(equity, at)?; }
            Some(_) => {
                let before = at.checked_sub_signed(Duration::nanoseconds(1))
                    .ok_or(PortfolioError::Invariant("midnight timestamp overflow"))?;
                self.risk.update_equity(equity, before)?;
                cancel = self.risk.circuit_breaker_state().is_tripped();
                if equity > Decimal::ZERO { self.risk.start_trading_day(equity, equity, at, false)?; }
            }
        }
        self.insolvent = equity <= Decimal::ZERO;
        Ok(cancel || self.entry_halted())
    }

    fn advance_owners(&mut self, symbol: &str, event: &IctEngineEvent, analysis: &mut MultiSymbolAnalysis,
        events: &mut Vec<PortfolioEvent>) -> Result<(), MultiPortfolioError> {
        let ids: Vec<_> = self.owners.iter().filter(|(_, owner)| owner.key.instrument.venue_symbol.as_str() == symbol)
            .map(|(id, _)| *id).collect();
        for id in ids {
            let owner = self.owners.get_mut(&id).ok_or(PortfolioError::Invariant("missing strategy owner"))?;
            let actions = owner.strategy.process_event(event.clone())?;
            for action in actions {
                match action {
                    StrategyAction::CancelPendingIntent { key, intent_id, at, .. } => {
                        if id != intent_id || key != self.state(symbol)?.key {
                            return Err(PortfolioError::Invariant("strategy cancelled another intent").into());
                        }
                        let cancelled = self.book.cancel_intent(intent_id, at)?;
                        self.route_events(&cancelled)?;
                        events.extend(cancelled);
                    }
                    StrategyAction::InputRejected { .. } => analysis.reject(MultiRejection::StrategyInput),
                    StrategyAction::SetupDiscarded { .. } | StrategyAction::DataDegraded { .. } => {}
                    _ => return Err(PortfolioError::Invariant("owned strategy emitted discovery or execution").into()),
                }
            }
        }
        Ok(())
    }

    fn discovery_actions(&mut self, symbol: &str, actions: Vec<StrategyAction>, candle: &Candle,
        warmup: bool, analysis: &mut MultiSymbolAnalysis, events: &mut Vec<PortfolioEvent>) -> Result<(), MultiPortfolioError> {
        for action in actions {
            match action {
                StrategyAction::TradeIntentGenerated(intent) => {
                    analysis.intents += 1;
                    // Move the actual pending strategy; never fake a fill/exit
                    // merely to let the symbol search for its next setup.
                    let state = self.state_mut(symbol)?;
                    let owner = IntentOwner { key: state.key.clone(), strategy: std::mem::replace(
                        &mut state.discovery, StrategyEngine::new(StrategyConfig::default())) };
                    if let Some(reason) = self.submit_intent(symbol, &intent, owner, candle, warmup, events)? {
                        analysis.reject(reason);
                    }
                }
                StrategyAction::InputRejected { .. } => analysis.reject(MultiRejection::StrategyInput),
                StrategyAction::SetupDiscarded { at, reason, .. } => {
                    self.state_mut(symbol)?.last_discard = Some(SetupDiscardDiagnostic { at, reason });
                }
                StrategyAction::DataDegraded { .. } => {}
                _ => return Err(PortfolioError::Invariant("discovery emitted an owned lifecycle").into()),
            }
        }
        Ok(())
    }

    fn consume_setup(&mut self, key: &CandleKey, intent: &TradeIntent) -> Result<bool, PortfolioError> {
        self.consume_sources(key, intent.market_structure_shift().id(), intent.fair_value_gap().id())
    }

    fn consume_sources(&mut self, key: &CandleKey, mss_id: Uuid, fvg_id: Uuid) -> Result<bool, PortfolioError> {
        let mss = (key.clone(), mss_id);
        let fvg = (key.clone(), fvg_id);
        let seen_mss = self.consumed_mss.contains(&mss);
        let seen_fvg = self.consumed_fvg.contains(&fvg);
        if (!seen_mss && self.consumed_mss.len() >= MAX_CONSUMED_SETUPS)
            || (!seen_fvg && self.consumed_fvg.len() >= MAX_CONSUMED_SETUPS) {
            return Err(PaperError::Capacity.into());
        }
        // A rejected mixed pair consumes its new half too: it must not later
        // be recombined with another source to retry the same first touch.
        self.consumed_mss.insert(mss);
        self.consumed_fvg.insert(fvg);
        Ok(!seen_mss && !seen_fvg)
    }

    fn submit_intent(&mut self, symbol: &str, intent: &TradeIntent, mut owner: IntentOwner,
        candle: &Candle, warmup: bool, events: &mut Vec<PortfolioEvent>) -> Result<Option<MultiRejection>, MultiPortfolioError> {
        let at = candle.close_time();
        if intent.instrument() != &owner.key.instrument || owner.key != self.state(symbol)?.key
            || owner.strategy.pending_intent(&owner.key).map(TradeIntent::id) != Some(intent.id()) {
            return Err(PortfolioError::Invariant("strategy intent differs from transferred owner").into());
        }
        let rejection = if !self.consume_setup(&owner.key, intent)? { Some(MultiRejection::DuplicateSetup) }
            else if warmup { Some(MultiRejection::Warmup) }
            else if self.entry_halted() { Some(MultiRejection::Halted) }
            else { None };
        if let Some(reason) = rejection {
            Self::reject_owner(&mut owner, intent.id(), at)?;
            return Ok(Some(reason));
        }
        if self.owners.contains_key(&intent.id()) {
            return Err(PortfolioError::Invariant("intent identifier already has an owner").into());
        }
        let Some(target) = self.state(symbol)?.nearest_target(intent, at)? else {
            Self::reject_owner(&mut owner, intent.id(), at)?;
            return Ok(Some(MultiRejection::NoOpposingLiquidity));
        };
        let assessment = match self.risk.assess_trade(intent, &target, at) {
            Ok(assessment) => assessment,
            Err(error) if is_entry_rejection(&error) => {
                let reason = if error == RiskError::PortfolioRiskLimitReached { MultiRejection::PortfolioRisk }
                    else { MultiRejection::RiskRule };
                Self::reject_owner(&mut owner, intent.id(), at)?;
                return Ok(Some(reason));
            }
            Err(error) => return Err(error.into()),
        };
        let TradeAssessment::Approved(plan) = assessment else {
            return Err(PortfolioError::Invariant("unexpected risk replay after setup deduplication").into());
        };
        let instrument = self.state(symbol)?.instrument.clone();
        match self.book.admit(&instrument, *plan, candle) {
            Ok(placed) => {
                if !matches!(placed.as_slice(), [PortfolioEvent::EntryPlaced { symbol: placed_symbol, at: placed_at, order }]
                    if placed_symbol == symbol && *placed_at == at && order.intent_id == intent.id()) {
                    return Err(PortfolioError::Invariant("admission did not create exactly one resting entry").into());
                }
                self.owners.insert(intent.id(), owner);
                events.extend(placed);
                Ok(None)
            }
            Err(error) if error.is_rejection() => {
                let reason = book_rejection(&error);
                self.risk.apply_reservation_event(intent.id(), ReservationEvent::EntryCancelled, at)?;
                Self::reject_owner(&mut owner, intent.id(), at)?;
                Ok(Some(reason))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn reject_owner(owner: &mut IntentOwner, intent_id: Uuid, at: DateTime<Utc>) -> Result<(), PortfolioError> {
        let actions = owner.strategy.process_command(StrategyCommand::EntryCancelled {
            key: owner.key.clone(), intent_id, cancelled_at: at })?;
        if !matches!(actions.as_slice(), [StrategyAction::EntryCancellationConfirmed { intent_id: cancelled, .. }] if *cancelled == intent_id) {
            return Err(PortfolioError::Invariant("unsubmitted strategy refused cancellation"));
        }
        Ok(())
    }

    fn route_events(&mut self, events: &[PortfolioEvent]) -> Result<(), MultiPortfolioError> {
        for record in events {
            let PortfolioEvent::Execution { symbol, event, order } = record else {
                return Err(PortfolioError::Invariant("execution emitted a non-lifecycle record").into());
            };
            let (intent_id, at, reservation, terminal) = match event {
                PaperEvent::EntryFilled { intent_id, at, .. } => (*intent_id, *at, ReservationEvent::EntryFilled, false),
                PaperEvent::EntryCancelled { intent_id, at, .. } => (*intent_id, *at, ReservationEvent::EntryCancelled, true),
                PaperEvent::PositionClosed { intent_id, at, .. } => (*intent_id, *at, ReservationEvent::PositionClosed, true),
            };
            let owner = self.owners.get_mut(&intent_id).ok_or(PortfolioError::Invariant("execution has no strategy owner"))?;
            if order.intent_id != intent_id || owner.key.instrument.venue_symbol.as_str() != symbol {
                return Err(PortfolioError::Invariant("execution intent or symbol differs from captured owner").into());
            }
            let key = owner.key.clone();
            let command = match event {
                PaperEvent::EntryFilled { .. } => StrategyCommand::EntryFilled { key, intent_id, filled_at: at },
                PaperEvent::EntryCancelled { .. } => StrategyCommand::EntryCancelled { key, intent_id, cancelled_at: at },
                PaperEvent::PositionClosed { .. } => StrategyCommand::PositionClosed { key, intent_id, closed_at: at },
            };
            self.risk.apply_reservation_event(intent_id, reservation, at)?;
            let actions = owner.strategy.process_command(command)?;
            let valid = match (event, actions.as_slice()) {
                (PaperEvent::EntryFilled { .. }, [StrategyAction::PositionOpened { intent_id: routed, .. }])
                | (PaperEvent::EntryCancelled { .. }, [StrategyAction::EntryCancellationConfirmed { intent_id: routed, .. }])
                | (PaperEvent::PositionClosed { .. }, [StrategyAction::PositionClosed { intent_id: routed, .. }]) => *routed == intent_id,
                _ => false,
            };
            if !valid { return Err(PortfolioError::Invariant("owned strategy rejected real execution lifecycle").into()); }
            if terminal { self.owners.remove(&intent_id); }
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

fn is_entry_rejection(error: &RiskError) -> bool {
    matches!(error, RiskError::CircuitBreakerTripped | RiskError::IntentExpired
        | RiskError::InvalidPriceGeometry | RiskError::MinimumRiskRewardNotMet { .. }
        | RiskError::PortfolioRiskLimitReached | RiskError::QuantityBelowMinimum { .. })
}

fn book_rejection(error: &BookError) -> MultiRejection {
    match error {
        BookError::ActiveLimit => MultiRejection::GlobalSlots,
        BookError::SymbolLimit => MultiRejection::SymbolSlots,
        BookError::OpposingDirection => MultiRejection::OpposingDirection,
        BookError::RiskLimit => MultiRejection::PortfolioRisk,
        BookError::InsufficientCash | BookError::Broker(PaperError::InsufficientBalance) => MultiRejection::InsufficientBalance,
        BookError::Broker(PaperError::MinimumOrder) => MultiRejection::MinimumOrder,
        BookError::Broker(PaperError::NetRiskReward) => MultiRejection::NetRiskReward,
        _ => MultiRejection::RiskRule,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use crate::{
        models::{CandleInput, InstrumentId},
        paper::book::BookOrderState,
        risk::{CircuitBreakerState, InstrumentRiskSpec, ReservationStatus},
        strategy::StrategyState,
    };

    type TestResult = Result<(), Box<dyn Error>>;
    const NEUTRAL: [i64; 4] = [100, 102, 98, 101];

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

    fn portfolio() -> Result<MultiPortfolio, Box<dyn Error>> {
        Ok(MultiPortfolio::new(vec![metadata("ETHUSDT")?, metadata("BTCUSDT")?],
            PaperConfig::default().with_leverage(10)?, Timeframe::M1)?)
    }

    fn at(minute: i64) -> Result<DateTime<Utc>, Box<dyn Error>> {
        DateTime::parse_from_rfc3339("2026-10-28T11:00:00Z")?.with_timezone(&Utc)
            .checked_add_signed(Duration::minutes(minute)).ok_or_else(|| "test timestamp overflow".into())
    }

    fn candle(symbol: &str, minute: i64, prices: [i64; 4]) -> Result<Candle, Box<dyn Error>> {
        Ok(Candle::new(CandleInput { instrument: instrument(symbol)?, timeframe: Timeframe::M1,
            open_time: at(minute)?, close_time: at(minute + 1)?, open: Decimal::from(prices[0]),
            high: Decimal::from(prices[1]), low: Decimal::from(prices[2]), close: Decimal::from(prices[3]),
            volume: Decimal::ONE, volume_unit: VolumeUnit::BaseAsset, trade_count: None, is_closed: true })?)
    }

    fn batch(minute: i64, btc: [i64; 4], eth: [i64; 4]) -> Result<Vec<Candle>, Box<dyn Error>> {
        Ok(vec![candle("ETHUSDT", minute, eth)?, candle("BTCUSDT", minute, btc)?])
    }

    fn setup(target: i64) -> Vec<[i64; 4]> {
        // Real default detector: 5-candle swings, bearish BOS, SSL sweep,
        // displacement/MSS/FVG and CE retracement. No fabricated strategy fill.
        let mut prices = vec![NEUTRAL; 9];
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

    fn ready_pending(both: bool, target: i64) -> Result<MultiPortfolio, Box<dyn Error>> {
        let mut engine = portfolio()?;
        for (minute, prices) in setup(target).into_iter().enumerate() {
            engine.process_batch(&batch(i64::try_from(minute)?, prices, if both { prices } else { NEUTRAL })?)?;
        }
        assert_eq!(engine.snapshot().account.orders.len(), if both { 2 } else { 1 });
        Ok(engine)
    }

    fn subsequent_setup(target: i64, offset: i64) -> Vec<[i64; 4]> {
        let mut prices: Vec<_> = setup(target).into_iter().map(|bar| bar.map(|price| price + offset)).collect();
        // Update the persistent bullish protected low through a genuine BOS
        // after a higher swing low is confirmed, before the bearish reversal.
        prices.insert(15, [100 + offset, 107 + offset, 99 + offset, 106 + offset]);
        prices
    }

    fn ready_two_btc() -> Result<MultiPortfolio, Box<dyn Error>> {
        let mut engine = ready_pending(false, 1_000)?;
        engine.process_batch(&batch(25, [109, 110, 103, 109], NEUTRAL)?)?;
        for (index, prices) in subsequent_setup(700, 50).into_iter().enumerate() {
            engine.process_batch(&batch(26 + i64::try_from(index)?, prices, NEUTRAL)?)?;
        }
        assert_eq!(engine.snapshot().account.orders.len(), 2);
        Ok(engine)
    }

    #[test]
    fn diagnostics_follow_discovery_without_changing_serialized_steps() -> TestResult {
        let mut observed = portfolio()?;
        let mut control = portfolio()?;
        assert!(observed.diagnostics().iter().all(|row| row.last_close.is_none()
            && row.last_discard.is_none() && row.discovery_stage == StrategyStage::WaitingForSweep));
        let mut stages = Vec::new();
        for (minute, prices) in setup(150).into_iter().enumerate() {
            let minute = i64::try_from(minute)?;
            let candles = batch(minute, prices, NEUTRAL)?;
            let before = observed.snapshot();
            assert_eq!(observed.diagnostics(), observed.diagnostics());
            assert_eq!(observed.snapshot(), before);
            let (actual, expected) = if minute < 9 {
                (observed.warm_up(&candles)?, control.warm_up(&candles)?)
            } else {
                (observed.process_batch(&candles)?, control.process_batch(&candles)?)
            };
            let actual_json = serde_json::to_value(&actual)?;
            assert_eq!(actual_json, serde_json::to_value(&expected)?);
            // Observer data must not silently change the v3 replay schema.
            let step_keys: Vec<_> = actual_json.as_object().ok_or("missing step")?.keys().map(String::as_str).collect();
            assert_eq!(step_keys, ["analysis", "events", "snapshot"]);
            let analysis_keys: Vec<_> = actual_json["analysis"][0].as_object().ok_or("missing analysis")?
                .keys().map(String::as_str).collect();
            assert_eq!(analysis_keys, ["intents", "rejected", "rejection_reasons", "signals", "symbol"]);
            let diagnostics = observed.diagnostics();
            assert_eq!(diagnostics.iter().map(|row| row.symbol.as_str()).collect::<Vec<_>>(), ["BTCUSDT", "ETHUSDT"]);
            let expected_close = at(minute + 1)?;
            assert!(diagnostics.iter().all(|row| row.last_close == Some(expected_close)));
            stages.push(diagnostics[0].discovery_stage);
        }
        for expected in [StrategyStage::WaitingForSweep, StrategyStage::WaitingForMss,
            StrategyStage::WaitingForFvg, StrategyStage::WaitingForRetracement] {
            assert!(stages.contains(&expected), "missing {expected:?}: {stages:?}");
        }
        assert_eq!(observed.snapshot(), control.snapshot());
        assert_eq!(observed.snapshot().account.orders.len(), 1);
        // An existing order's owner is independent of the fresh discovery phase.
        assert_eq!(observed.diagnostics()[0].discovery_stage, StrategyStage::WaitingForSweep);
        Ok(())
    }

    #[test]
    fn diagnostics_report_actual_outside_window_discard_as_historical_context() -> TestResult {
        let mut engine = portfolio()?;
        for (minute, prices) in setup(150).into_iter().enumerate() {
            // 05:00 UTC is 01:00 New York on the fixture date, outside both windows.
            engine.process_batch(&batch(i64::try_from(minute)? - 360, prices, NEUTRAL)?)?;
        }
        let diagnostics = engine.diagnostics();
        assert_eq!(diagnostics[0].last_close, Some(at(-335)?));
        assert_eq!(diagnostics[0].discovery_stage, StrategyStage::WaitingForSweep);
        let discard = SetupDiscardDiagnostic { at: at(-335)?, reason: SetupDiscardReason::EntryTouchedOutsideKillzone };
        assert_eq!(diagnostics[0].last_discard, Some(discard));
        assert!(diagnostics[1].last_discard.is_none());
        assert!(engine.snapshot().account.orders.is_empty());
        assert!(!engine.snapshot().halted);
        engine.process_batch(&batch(-335, NEUTRAL, NEUTRAL)?)?;
        assert_eq!(engine.diagnostics()[0].last_close, Some(at(-334)?));
        assert_eq!(engine.diagnostics()[0].last_discard, Some(discard));
        Ok(())
    }

    #[test]
    fn shared_cash_supports_two_simultaneous_real_symbol_setups() -> TestResult {
        let mut engine = ready_pending(true, 150)?;
        let snapshot = engine.snapshot();
        assert_eq!(snapshot.account.initial_balance, Decimal::from(100));
        assert_eq!(snapshot.account.balance, Decimal::from(100));
        assert_eq!(snapshot.account.leverage, 10);
        assert_eq!(engine.owners.len(), 2);
        assert!(engine.risk.active_reserved_risk()? <= Decimal::from(2));
        let step = engine.process_batch(&batch(25, [109, 110, 103, 109], [109, 110, 103, 109])?)?;
        assert_eq!(step.snapshot.account.orders.len(), 2);
        assert!(step.snapshot.account.orders.iter().all(|order| order.state == BookOrderState::Open));
        assert_eq!(step.events.iter().filter(|event| matches!(event,
            PortfolioEvent::Execution { event: PaperEvent::EntryFilled { .. }, .. })).count(), 2);
        assert!(engine.owners.values().all(|owner| owner.strategy.state(&owner.key) == StrategyState::InPosition));
        assert!(engine.symbols.values().all(|state| state.discovery.state(&state.key) != StrategyState::InPosition));
        Ok(())
    }

    #[test]
    fn a_second_independent_btc_setup_is_admitted_while_first_btc_remains_open() -> TestResult {
        let mut engine = ready_pending(false, 1_000)?;
        let first = engine.snapshot().account.orders[0].order.clone();
        engine.process_batch(&batch(25, [109, 110, 103, 109], NEUTRAL)?)?;
        let mut placed = Vec::new();
        let mut last_analysis = Vec::new();
        // The persistent bullish structure still protects the first setup's
        // low. A real continuation BOS after the new 145 swing is confirmed
        // first raises that protected low; the next close below 145 can then
        // reverse the bias. Merely repeating the shape would produce BOS, not
        // a second MSS, and must not be admitted by the unchanged strategy.
        for (index, shifted) in subsequent_setup(700, 50).into_iter().enumerate() {
            let step = engine.process_batch(&batch(26 + i64::try_from(index)?, shifted, NEUTRAL)?)?;
            placed.extend(step.events);
            last_analysis = step.analysis;
        }
        let snapshot = engine.snapshot();
        assert_eq!(snapshot.account.orders.len(), 2, "last analysis: {last_analysis:?}");
        assert!(snapshot.account.orders.iter().all(|row| row.symbol == "BTCUSDT"));
        let existing = snapshot.account.orders.iter().find(|row| row.order.intent_id == first.intent_id).ok_or("first owner lost")?;
        assert_eq!(existing.state, BookOrderState::Open);
        assert_eq!(existing.order, first);
        let second = snapshot.account.orders.iter().find(|row| row.order.intent_id != first.intent_id).ok_or("second owner missing")?;
        assert_eq!(second.state, BookOrderState::Pending);
        assert_ne!(second.order.entry_price, first.entry_price);
        assert_eq!(snapshot.account.completed_trades, 0);
        assert!(matches!(placed.as_slice(), [PortfolioEvent::EntryPlaced { symbol, .. }] if symbol == "BTCUSDT"));
        assert_eq!(engine.owners.len(), 2);
        let owners: Vec<_> = engine.owners.values().collect();
        assert_eq!(owners.iter().filter(|owner| owner.strategy.state(&owner.key) == StrategyState::InPosition).count(), 1);
        assert_eq!(engine.risk.reservation_status(first.intent_id), Some(ReservationStatus::PositionOpen));
        let second_id = second.order.intent_id;
        let second_fill = engine.process_batch(&batch(52, [159, 160, 153, 159], NEUTRAL)?)?;
        assert_eq!(second_fill.snapshot.account.orders.iter().filter(|row| row.state == BookOrderState::Open).count(), 2);
        assert!(second_fill.events.iter().any(|event| matches!(event,
            PortfolioEvent::Execution { event: PaperEvent::EntryFilled { intent_id, .. }, .. } if *intent_id == second_id)));
        assert_eq!(engine.risk.reservation_status(second_id), Some(ReservationStatus::PositionOpen));
        // A stop on the newer, higher entry must not flatten or release the
        // older BTC owner simply because both share one venue symbol.
        let stopped = engine.process_batch(&batch(53, [159, 160, 138, 140], NEUTRAL)?)?;
        assert_eq!(stopped.snapshot.account.orders.len(), 1);
        assert_eq!(stopped.snapshot.account.orders[0].order, first);
        assert_eq!(stopped.snapshot.account.orders[0].state, BookOrderState::Open);
        assert_eq!(stopped.snapshot.account.completed_trades, 1);
        assert!(matches!(stopped.events.as_slice(), [PortfolioEvent::Execution {
            event: PaperEvent::PositionClosed { intent_id, .. }, .. }] if *intent_id == second_id));
        assert_eq!(engine.risk.reservation_status(first.intent_id), Some(ReservationStatus::PositionOpen));
        assert_eq!(engine.risk.reservation_status(second_id), Some(ReservationStatus::Released));
        assert_eq!(engine.owners.len(), 1);
        Ok(())
    }

    #[test]
    fn later_pending_btc_expiry_leaves_first_btc_open_and_reserved() -> TestResult {
        let mut engine = ready_two_btc()?;
        let before = engine.snapshot().account;
        let first = before.orders.iter().find(|row| row.state == BookOrderState::Open).ok_or("open BTC missing")?;
        let second = before.orders.iter().find(|row| row.state == BookOrderState::Pending).ok_or("pending BTC missing")?;
        let mut cancellations = Vec::new();
        for minute in 52..64 {
            let step = engine.process_batch(&batch(minute, [159, 160, 158, 159], NEUTRAL)?)?;
            cancellations.extend(step.events);
        }
        assert!(matches!(cancellations.as_slice(), [PortfolioEvent::Execution {
            symbol, event: PaperEvent::EntryCancelled { intent_id, .. }, .. }]
            if symbol == "BTCUSDT" && *intent_id == second.order.intent_id));
        let after = engine.snapshot();
        assert_eq!(after.account.orders.len(), 1);
        assert_eq!(after.account.orders[0].order, first.order);
        assert_eq!(after.account.completed_trades, 0);
        assert_eq!(engine.risk.active_reserved_risk()?, first.reserved_risk);
        assert_eq!(engine.owners.len(), 1);
        assert_eq!(engine.risk.reservation_status(first.order.intent_id), Some(ReservationStatus::PositionOpen));
        assert_eq!(engine.risk.reservation_status(second.order.intent_id), Some(ReservationStatus::Released));
        Ok(())
    }

    #[test]
    fn third_genuine_btc_setup_hits_symbol_slots_without_releasing_existing_owners() -> TestResult {
        let mut engine = ready_two_btc()?;
        engine.process_batch(&batch(52, [159, 160, 153, 159], NEUTRAL)?)?;
        let existing = engine.snapshot().account.orders;
        let reserved = engine.risk.active_reserved_risk()?;
        let mut rejections = Vec::new();
        for (index, prices) in subsequent_setup(400, 100).into_iter().enumerate() {
            let step = engine.process_batch(&batch(53 + i64::try_from(index)?, prices, NEUTRAL)?)?;
            assert!(step.events.is_empty(), "existing positions must not close during setup");
            rejections.extend(step.analysis.into_iter().flat_map(|row| row.rejection_reasons));
        }
        assert_eq!(rejections, vec![MultiRejection::SymbolSlots]);
        assert_eq!(engine.owners.len(), 2);
        assert_eq!(engine.snapshot().account.orders, existing);
        assert_eq!(engine.risk.active_reserved_risk()?, reserved);
        assert_eq!(engine.consumed_mss.len(), 3);
        Ok(())
    }

    #[test]
    fn rejected_duplicate_does_not_cancel_the_real_pending_owner() -> TestResult {
        let mut engine = ready_pending(false, 150)?;
        // A separate real run reconstructs the duplicate strategy, not a fake
        // lifecycle for the existing owner. The duplicate must fail before book.
        let mut duplicate = ready_pending(false, 150)?;
        let id = engine.snapshot().account.orders[0].order.intent_id;
        let owner = duplicate.owners.remove(&id).ok_or("duplicate owner missing")?;
        let intent = owner.strategy.pending_intent(&owner.key).ok_or("pending intent missing")?.clone();
        let before = engine.snapshot();
        let risk = engine.risk.active_reserved_risk()?;
        let mut events = Vec::new();
        let reason = engine.submit_intent("BTCUSDT", &intent, owner,
            &candle("BTCUSDT", 24, [110, 112, 103, 109])?, false, &mut events)?;
        assert_eq!(reason, Some(MultiRejection::DuplicateSetup));
        assert!(events.is_empty());
        assert_eq!(engine.snapshot(), before);
        assert_eq!(engine.risk.active_reserved_risk()?, risk);
        assert_eq!(engine.owners.len(), 1);
        Ok(())
    }

    #[test]
    fn rejected_mixed_sources_cannot_be_recombined_into_a_fresh_setup() -> TestResult {
        let mut engine = portfolio()?;
        let key = engine.state("BTCUSDT")?.key.clone();
        let mss1 = Uuid::from_u128(1);
        let mss2 = Uuid::from_u128(2);
        let mss3 = Uuid::from_u128(3);
        let fvg1 = Uuid::from_u128(11);
        let fvg2 = Uuid::from_u128(12);
        let fvg3 = Uuid::from_u128(13);
        assert!(engine.consume_sources(&key, mss1, fvg1)?);
        assert!(!engine.consume_sources(&key, mss1, fvg2)?);
        assert!(!engine.consume_sources(&key, mss2, fvg2)?);
        assert!(!engine.consume_sources(&key, mss2, fvg3)?);
        assert!(!engine.consume_sources(&key, mss3, fvg3)?);
        assert_eq!(engine.consumed_mss.len(), 3);
        assert_eq!(engine.consumed_fvg.len(), 3);
        // Identical identifiers on a different venue instrument do not collide.
        let eth_key = engine.state("ETHUSDT")?.key.clone();
        assert!(engine.consume_sources(&eth_key, mss1, fvg1)?);
        Ok(())
    }

    #[test]
    fn invalid_complete_batch_poisoning_happens_before_either_owner_fills() -> TestResult {
        for invalid in [
            vec![candle("BTCUSDT", 25, [109, 110, 103, 109])?],
            vec![candle("BTCUSDT", 25, [109, 110, 103, 109])?, candle("BTCUSDT", 25, [109, 110, 103, 109])?],
            vec![candle("BTCUSDT", 25, [109, 110, 103, 109])?, candle("ETHUSDT", 26, [109, 110, 103, 109])?],
            vec![candle("BTCUSDT", 25, [109, 110, 103, 109])?, candle("SOLUSDT", 25, [109, 110, 103, 109])?],
        ] {
            let mut engine = ready_pending(true, 150)?;
            let before = engine.snapshot().account;
            assert!(engine.process_batch(&invalid).is_err());
            assert_eq!(engine.snapshot().account, before);
            assert!(engine.is_poisoned());
            assert!(matches!(engine.process_batch(&batch(25, [109, 110, 103, 109], [109, 110, 103, 109])?),
                Err(MultiPortfolioError::Portfolio(PortfolioError::Poisoned))));
        }
        Ok(())
    }

    #[test]
    fn deterministic_sorting_and_historical_replay_never_duplicate_orders() -> TestResult {
        let mut first = portfolio()?;
        let mut second = portfolio()?;
        for (minute, prices) in setup(150).into_iter().enumerate() {
            let mut bars = batch(i64::try_from(minute)?, prices, prices)?;
            let expected = first.process_batch(&bars)?;
            bars.reverse();
            assert_eq!(expected, second.process_batch(&bars)?);
        }
        let before = first.snapshot();
        let step = first.process_batch(&batch(0, NEUTRAL, NEUTRAL)?)?;
        assert_eq!(step.snapshot, before);
        assert!(step.events.is_empty());
        assert!(step.analysis.iter().all(|row| row.signals == 0 && row.intents == 0 && row.rejected == 0));
        Ok(())
    }

    #[test]
    fn changed_replay_or_gap_cannot_change_shared_cash() -> TestResult {
        let mut engine = ready_pending(true, 150)?;
        let before = engine.snapshot().account;
        assert!(matches!(engine.process_batch(&batch(0, [100, 102, 98, 100], NEUTRAL)?),
            Err(MultiPortfolioError::Portfolio(PortfolioError::Broker(PaperError::ConflictingDuplicate)))));
        assert_eq!(engine.snapshot().account, before);
        let cancelled = engine.shutdown(at(25)?)?;
        assert_eq!(cancelled.len(), 2);
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        let mut engine = ready_pending(true, 150)?;
        let before = engine.snapshot().account;
        assert!(matches!(engine.process_batch(&batch(26, NEUTRAL, NEUTRAL)?),
            Err(MultiPortfolioError::Portfolio(PortfolioError::Broker(PaperError::Gap)))));
        assert_eq!(engine.snapshot().account, before);
        Ok(())
    }

    #[test]
    fn historical_intents_are_consumed_without_orders_or_risk_reservations() -> TestResult {
        let mut engine = portfolio()?;
        let mut intents = 0;
        for (minute, prices) in setup(150).into_iter().enumerate() {
            let step = engine.warm_up(&batch(i64::try_from(minute)?, prices, prices)?)?;
            intents += step.analysis.iter().map(|row| row.intents).sum::<usize>();
            assert!(step.events.is_empty());
            assert!(step.snapshot.account.orders.is_empty());
            assert_eq!(step.snapshot.account.balance, Decimal::from(100));
            assert!(step.analysis.iter().flat_map(|row| &row.rejection_reasons).all(|reason| *reason == MultiRejection::Warmup));
        }
        assert_eq!(intents, 2);
        assert_eq!(engine.consumed_mss.len(), 2);
        assert_eq!(engine.owners.len(), 0);
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        engine.process_batch(&batch(25, NEUTRAL, NEUTRAL)?)?;
        assert!(matches!(engine.warm_up(&batch(26, NEUTRAL, NEUTRAL)?),
            Err(MultiPortfolioError::Portfolio(PortfolioError::WarmupAfterTrading))));
        Ok(())
    }

    #[test]
    fn cancelled_pending_owner_does_not_release_another_open_position() -> TestResult {
        let mut engine = ready_pending(true, 150)?;
        engine.process_batch(&batch(25, [109, 110, 103, 109], [110, 112, 109, 111])?)?;
        let before = engine.snapshot().account;
        let btc = before.orders.iter().find(|row| row.symbol == "BTCUSDT").ok_or("BTC missing")?;
        let eth = before.orders.iter().find(|row| row.symbol == "ETHUSDT").ok_or("ETH missing")?;
        assert_eq!(btc.state, BookOrderState::Open);
        assert_eq!(eth.state, BookOrderState::Pending);
        let events = engine.shutdown(at(26)?)?;
        assert!(matches!(events.as_slice(), [PortfolioEvent::Execution {
            symbol, event: PaperEvent::EntryCancelled { intent_id, .. }, .. }, PortfolioEvent::Halted { .. }]
            if symbol == "ETHUSDT" && *intent_id == eth.order.intent_id));
        assert_eq!(engine.risk.reservation_status(btc.order.intent_id), Some(ReservationStatus::PositionOpen));
        assert_eq!(engine.risk.reservation_status(eth.order.intent_id), Some(ReservationStatus::Released));
        assert_eq!(engine.risk.active_reserved_risk()?, btc.reserved_risk);
        assert_eq!(engine.snapshot().account.orders.len(), 1);
        assert_eq!(engine.owners.len(), 1);
        assert!(engine.shutdown(at(26)?)?.is_empty());
        Ok(())
    }

    #[test]
    fn gap_loss_trips_shared_breaker_and_cancels_other_symbol_pending() -> TestResult {
        let mut engine = ready_pending(true, 150)?;
        engine.process_batch(&batch(25, [109, 110, 103, 109], [110, 112, 109, 111])?)?;
        let step = engine.process_batch(&batch(26, [2, 3, 1, 2], [110, 112, 109, 111])?)?;
        assert!(step.snapshot.account.equity < Decimal::from(95));
        assert!(step.snapshot.halted);
        assert!(step.snapshot.account.orders.is_empty());
        assert_eq!(step.snapshot.account.completed_trades, 1);
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        assert_eq!(engine.owners.len(), 0);
        assert!(step.events.iter().any(|event| matches!(event,
            PortfolioEvent::Execution { symbol, event: PaperEvent::EntryCancelled { .. }, .. } if symbol == "ETHUSDT")));
        assert_eq!(step.events.iter().filter(|event| matches!(event, PortfolioEvent::Halted { .. })).count(), 1);
        assert!(engine.shutdown(at(27)?)?.is_empty());
        Ok(())
    }

    #[test]
    fn same_bar_fill_and_stop_keep_geometry_and_terminate_only_that_owner() -> TestResult {
        let mut engine = ready_pending(true, 150)?;
        let btc = engine.snapshot().account.orders.into_iter().find(|row| row.symbol == "BTCUSDT").ok_or("BTC missing")?;
        let step = engine.process_batch(&batch(25, [109, 110, 80, 100], [110, 112, 109, 111])?)?;
        assert!(matches!(step.events.as_slice(), [
            PortfolioEvent::Execution { symbol: first, event: PaperEvent::EntryFilled { .. }, order: entry },
            PortfolioEvent::Execution { symbol: second, event: PaperEvent::PositionClosed { .. }, order: exit },
        ] if first == "BTCUSDT" && second == "BTCUSDT" && entry == &btc.order && exit == &btc.order));
        assert_eq!(step.snapshot.account.completed_trades, 1);
        assert_eq!(step.snapshot.account.orders.len(), 1);
        assert_eq!(step.snapshot.account.orders[0].symbol, "ETHUSDT");
        assert_eq!(engine.owners.len(), 1);
        Ok(())
    }

    #[test]
    fn failed_btc_minimum_admission_releases_only_its_approved_risk() -> TestResult {
        let mut btc = metadata("BTCUSDT")?;
        btc.min_notional = Decimal::from(100);
        let mut engine = MultiPortfolio::new(vec![btc, metadata("ETHUSDT")?],
            PaperConfig::default().with_leverage(10)?, Timeframe::M1)?;
        let mut last = None;
        for (minute, prices) in setup(150).into_iter().enumerate() {
            last = Some(engine.process_batch(&batch(i64::try_from(minute)?, prices, prices)?)?);
        }
        let step = last.ok_or("fixture empty")?;
        assert_eq!(step.analysis[0].rejection_reasons, vec![MultiRejection::MinimumOrder]);
        assert_eq!(step.analysis[1].rejected, 0);
        assert_eq!(step.snapshot.account.orders.len(), 1);
        assert_eq!(step.snapshot.account.orders[0].symbol, "ETHUSDT");
        assert!(engine.risk.active_reserved_risk()? <= Decimal::ONE);
        assert_eq!(engine.owners.len(), 1);
        Ok(())
    }

    #[test]
    fn midnight_rollover_preserves_one_balance_and_rearms_only_positive_equity() -> TestResult {
        let mut engine = portfolio()?;
        engine.warm_up(&batch(778, NEUTRAL, NEUTRAL)?)?;
        engine.risk.update_equity(Decimal::from(94), at(779)? + Duration::seconds(1))?;
        engine.risk.update_equity(Decimal::from(100), at(779)? + Duration::seconds(2))?;
        assert!(engine.snapshot().halted);
        let step = engine.warm_up(&batch(779, NEUTRAL, NEUTRAL)?)?;
        assert!(!step.snapshot.halted);
        assert_eq!(step.snapshot.account.balance, Decimal::from(100));
        assert!(matches!(engine.risk.circuit_breaker_state(), CircuitBreakerState::Armed { opening_equity, trading_day, .. }
            if opening_equity == Decimal::from(100) && trading_day == at(780)?.date_naive()));
        Ok(())
    }

    #[test]
    fn slot_risk_hedge_and_cash_rejections_remain_distinguishable() {
        assert_eq!(book_rejection(&BookError::ActiveLimit), MultiRejection::GlobalSlots);
        assert_eq!(book_rejection(&BookError::SymbolLimit), MultiRejection::SymbolSlots);
        assert_eq!(book_rejection(&BookError::OpposingDirection), MultiRejection::OpposingDirection);
        assert_eq!(book_rejection(&BookError::RiskLimit), MultiRejection::PortfolioRisk);
        assert_eq!(book_rejection(&BookError::InsufficientCash), MultiRejection::InsufficientBalance);
    }
}
