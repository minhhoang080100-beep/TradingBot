//! V3 PAPER orchestration with independent setup discovery and real ownership.
//!
//! A shared book and risk manager own all cash and reservations. Each accepted
//! intent keeps the actual strategy that produced it until its real cancellation
//! or exit; a fresh discovery strategy receives subsequent analyses from the
//! same persistent ICT detector. No synthetic fill or close frees a strategy.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    candle_store::CandleKey,
    ict_engine::{IctDetectionBatch, IctEngine, IctEngineConfig, IctEngineEvent, KillzoneFilter},
    models::{Candle, Direction, Exchange, LiquiditySide, MarketKind, SignalEvent, SwingKind, SwingPoint,
        Timeframe, TradeIntent, VolumeUnit},
    risk::{LiquidityTarget, ReservationEvent, RiskConfig, RiskError, RiskManager, TradeAssessment},
    strategy::{KillzonePolicy, SetupDiscardReason, StrategyAction, StrategyCommand, StrategyConfig, StrategyEngine, StrategyEntryPolicy, StrategyStage},
};

use super::{
    book::{BookError, BookSnapshot, PaperBook},
    broker::{PaperConfig, PaperError, PaperEvent, PaperFillTiming},
    market::PaperInstrument,
    portfolio::{PortfolioError, PortfolioEvent},
    smt_research::{SmtGateFailure, SmtResearchConfig, SmtResearchError, SmtResearchReport, SmtResearchState},
};

const MAX_RETAINED_BATCHES: usize = 100_000;
const MAX_CONSUMED_SETUPS: usize = 100_000;

/// Immutable entry-time policy for a multi-symbol paper session.
///
/// This controls only when a valid setup may generate an entry. It does not
/// relax setup confirmation, risk, margin, slot limits or data validation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperEntryWindow {
    /// Weekday London/New York windows, using DST-aware New York wall time.
    #[default]
    Killzones,
    /// Every hour and every day, including crypto weekends.
    Always,
}

/// Explicit target-selection hypothesis available only to research callers.
///
/// Live CLI sessions and every historical journal keep [`Self::Nearest`]. The
/// alternative does not claim profitability or change the available liquidity,
/// fee model, entry timing, minimum RR, account risk or order-slot limits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperTargetPolicy {
    /// Historical behavior: assess only the nearest opposing liquidity level.
    #[default]
    Nearest,
    /// Research hypothesis: try farther existing levels only after RR rejection.
    NearestViable,
}

impl PaperTargetPolicy {
    /// Stable research configuration name; not a live CLI parameter.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self { Self::Nearest => "nearest", Self::NearestViable => "nearest_viable" }
    }
}

impl PaperEntryWindow {
    /// Stable CLI and journal value for this policy.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self { Self::Killzones => "killzones", Self::Always => "always" }
    }

    /// Whether a candle close passes this entry-time policy.
    #[must_use]
    pub fn allows(self, at: DateTime<Utc>) -> bool {
        match self { Self::Killzones => KillzoneFilter::default().allows(at), Self::Always => true }
    }

    fn strategy_config(self, entry_policy: StrategyEntryPolicy) -> StrategyConfig {
        let defaults = StrategyConfig::default();
        StrategyConfig::new(defaults.entry_mode(), defaults.time_to_live_bars(), match self {
            Self::Killzones => KillzonePolicy::EntryOnly,
            Self::Always => KillzonePolicy::Disabled,
        }).with_entry_policy(entry_policy)
    }
}

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

/// A cumulative observer count for one finite reason enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticCount<R> {
    /// Exact emitted reason; no category is inferred from an absent order.
    pub reason: R,
    /// Number of observations, saturating instead of overflowing.
    pub count: u64,
}

/// Exact terminal admission failure, separate from the stable journal categories.
///
/// This finite enum bounds diagnostic storage. It describes the error actually
/// returned by admission, not a guess based on a setup that never became an intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionFailureKind {
    /// Rounded target reward is below the risk manager's gross RR floor.
    GrossRiskReward,
    /// Fees and stop slippage leave insufficient net reward/risk.
    NetRiskReward,
    /// Rounded entry, stop or target no longer has valid directional geometry.
    InvalidPriceGeometry,
    /// Risk-sized quantity is below the instrument's minimum quantity.
    QuantityBelowMinimum,
    /// The intent expired before admission.
    IntentExpired,
    /// The risk manager's daily circuit breaker is tripped.
    CircuitBreaker,
    /// The shared portfolio has insufficient remaining risk capacity.
    PortfolioRisk,
    /// Four pending/open slots are already used.
    GlobalSlots,
    /// Two pending/open slots are already used for this symbol.
    SymbolSlots,
    /// This symbol already has exposure in the opposite direction.
    OpposingDirection,
    /// Broker sizing cannot satisfy minimum quantity or notional.
    MinimumOrder,
    /// Margin and fee reserves cannot be funded from available cash.
    InsufficientBalance,
    /// The broker reported an existing pending entry or position.
    BrokerBusy,
    /// No confirmed, unswept opposing liquidity target exists.
    NoOpposingLiquidity,
    /// This MSS/FVG setup has already been consumed.
    DuplicateSetup,
    /// The session is already blocking new risk.
    SessionHalted,
    /// Required paired divergence expired or was invalidated before admission.
    SmtRequired,
    /// Completed M5 structure did not align with the proposed direction.
    M5Alignment,
}

/// Most recent rejected live intent, retained as historical context only.
///
/// Decimal values are copied from the actual error, without re-running sizing.
/// The broker does not return a numeric net RR; a net-RR rejection therefore
/// deliberately has no invented actual ratio. Research keeps only the terminal
/// failed target, and records nothing if a later target admits the intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionFailureDiagnostic {
    /// Candle close when the intent was rejected, not notification wall time.
    pub at: DateTime<Utc>,
    /// Full immutable intent identity; display layers may abbreviate it.
    pub intent_id: Uuid,
    /// Exact terminal admission failure.
    pub kind: AdmissionFailureKind,
    /// Raw confirmed liquidity target, before risk-manager tick rounding.
    pub target_price: Option<Decimal>,
    /// Gross RR after rounding, when supplied by the risk-manager error.
    pub actual_gross_rr: Option<Decimal>,
    /// Gross RR floor associated with `actual_gross_rr`.
    pub required_gross_rr: Option<Decimal>,
    /// Rounded risk-sized quantity, when below the instrument minimum.
    pub calculated_quantity: Option<Decimal>,
    /// Instrument minimum associated with `calculated_quantity`.
    pub minimum_quantity: Option<Decimal>,
}

/// Live-only cumulative observations outside the versioned execution journal.
///
/// Raw detector events are independent observations, not a linked trading setup.
/// Warm-up bars, duplicate batches and unsuccessful batches are excluded. Reason
/// lists contain at most one entry per enum variant, in first-observed order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LiveSetupCounters {
    /// Close of the first successfully processed live candle, not process start.
    pub first_close: Option<DateTime<Utc>>,
    /// Successfully processed live candles for this symbol.
    pub candles: u64,
    /// Raw liquidity-sweep events; one candle can sweep multiple levels.
    pub liquidity_sweeps: u64,
    /// Raw MSS events, before strategy confluence requirements.
    pub market_structure_shifts: u64,
    /// Newly detected FVG events; lifecycle updates are not counted again.
    pub fair_value_gaps: u64,
    /// Candles with a numerically confirmed displacement in either direction.
    pub displacement_bars: u64,
    /// Complete strategy entry intents, including subsequently rejected ones.
    pub intents: u64,
    /// Entries actually admitted to the shared paper book; not execution fills.
    pub admitted: u64,
    /// Rejected intents or strategy inputs, matching the per-batch analysis.
    pub rejected: u64,
    /// Exact admission/input rejection categories and their observed counts.
    pub rejection_reasons: Vec<DiagnosticCount<MultiRejection>>,
    /// Exact pre-intent discovery discard categories and their observed counts.
    pub discards: Vec<DiagnosticCount<SetupDiscardReason>>,
    /// Terminal rejected-intent counts, bounded by the finite failure enum.
    pub admission_failures: Vec<DiagnosticCount<AdmissionFailureKind>>,
    /// Last rejected live intent; not evidence that its condition is still active.
    pub last_admission_failure: Option<AdmissionFailureDiagnostic>,
}

impl LiveSetupCounters {
    fn observe_detection(&mut self, batch: &IctDetectionBatch, at: DateTime<Utc>) {
        self.first_close.get_or_insert(at);
        self.candles = self.candles.saturating_add(1);
        if batch.displacement().is_some() {
            self.displacement_bars = self.displacement_bars.saturating_add(1);
        }
        for signal in batch.signals() {
            let counter = match signal {
                SignalEvent::LiquiditySwept(_) => &mut self.liquidity_sweeps,
                SignalEvent::MarketStructureShiftDetected(_) => &mut self.market_structure_shifts,
                SignalEvent::FairValueGapDetected(_) => &mut self.fair_value_gaps,
                _ => continue,
            };
            *counter = counter.saturating_add(1);
        }
    }

    fn observe_actions(&mut self, actions: &[StrategyAction]) {
        for action in actions {
            if let StrategyAction::SetupDiscarded { reason, .. } = action {
                increment_reason(&mut self.discards, *reason);
            }
        }
    }

    fn observe_outcome(&mut self, analysis: &MultiSymbolAnalysis, events: &[PortfolioEvent]) {
        self.intents = self.intents.saturating_add(count_as_u64(analysis.intents));
        self.rejected = self.rejected.saturating_add(count_as_u64(analysis.rejected));
        for reason in &analysis.rejection_reasons {
            increment_reason(&mut self.rejection_reasons, *reason);
        }
        let admitted = events.iter().filter(|event| matches!(event,
            PortfolioEvent::EntryPlaced { symbol, .. } if symbol == &analysis.symbol)).count();
        self.admitted = self.admitted.saturating_add(count_as_u64(admitted));
    }

    fn observe_admission_failure(&mut self, failure: AdmissionFailureDiagnostic) {
        increment_reason(&mut self.admission_failures, failure.kind);
        self.last_admission_failure = Some(failure);
    }
}

fn count_as_u64(count: usize) -> u64 { u64::try_from(count).unwrap_or(u64::MAX) }

fn increment_reason<R: Copy + Eq>(counts: &mut Vec<DiagnosticCount<R>>, reason: R) {
    if let Some(item) = counts.iter_mut().find(|item| item.reason == reason) {
        item.count = item.count.saturating_add(1);
    } else {
        counts.push(DiagnosticCount { reason, count: 1 });
    }
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
    /// Cumulative live observations; historical warm-up is deliberately excluded.
    pub live: LiveSetupCounters,
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
    /// Required active BTC-ETH SMT proof is absent at entry.
    SmtRequired,
    /// Entry does not align with the last fully closed M5 structure.
    M5Alignment,
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

struct RejectedAdmission {
    reason: MultiRejection,
    diagnostic: Option<AdmissionFailureDiagnostic>,
}

impl RejectedAdmission {
    fn simple(reason: MultiRejection, intent: &TradeIntent, at: DateTime<Utc>,
        target_price: Option<Decimal>,
    ) -> Self {
        let kind = match reason {
            MultiRejection::DuplicateSetup => Some(AdmissionFailureKind::DuplicateSetup),
            MultiRejection::Halted => Some(AdmissionFailureKind::SessionHalted),
            MultiRejection::GlobalSlots => Some(AdmissionFailureKind::GlobalSlots),
            MultiRejection::SymbolSlots => Some(AdmissionFailureKind::SymbolSlots),
            MultiRejection::OpposingDirection => Some(AdmissionFailureKind::OpposingDirection),
            MultiRejection::PortfolioRisk => Some(AdmissionFailureKind::PortfolioRisk),
            MultiRejection::NoOpposingLiquidity => Some(AdmissionFailureKind::NoOpposingLiquidity),
            MultiRejection::MinimumOrder => Some(AdmissionFailureKind::MinimumOrder),
            MultiRejection::InsufficientBalance => Some(AdmissionFailureKind::InsufficientBalance),
            MultiRejection::NetRiskReward => Some(AdmissionFailureKind::NetRiskReward),
            MultiRejection::SmtRequired => Some(AdmissionFailureKind::SmtRequired),
            MultiRejection::M5Alignment => Some(AdmissionFailureKind::M5Alignment),
            MultiRejection::Warmup | MultiRejection::RiskRule | MultiRejection::StrategyInput => None,
        };
        Self { reason, diagnostic: kind.map(|kind| Self::detail(kind, intent, at, target_price)) }
    }

    fn detail(kind: AdmissionFailureKind, intent: &TradeIntent, at: DateTime<Utc>,
        target_price: Option<Decimal>,
    ) -> AdmissionFailureDiagnostic {
        AdmissionFailureDiagnostic { at, intent_id: intent.id(), kind, target_price,
            actual_gross_rr: None, required_gross_rr: None,
            calculated_quantity: None, minimum_quantity: None }
    }

    fn risk(error: &RiskError, intent: &TradeIntent, target: &LiquidityTarget, at: DateTime<Utc>) -> Self {
        let reason = if *error == RiskError::PortfolioRiskLimitReached { MultiRejection::PortfolioRisk }
            else { MultiRejection::RiskRule };
        let kind = match error {
            RiskError::CircuitBreakerTripped => AdmissionFailureKind::CircuitBreaker,
            RiskError::IntentExpired => AdmissionFailureKind::IntentExpired,
            RiskError::InvalidPriceGeometry => AdmissionFailureKind::InvalidPriceGeometry,
            RiskError::MinimumRiskRewardNotMet { .. } => AdmissionFailureKind::GrossRiskReward,
            RiskError::PortfolioRiskLimitReached => AdmissionFailureKind::PortfolioRisk,
            RiskError::QuantityBelowMinimum { .. } => AdmissionFailureKind::QuantityBelowMinimum,
            _ => return Self { reason, diagnostic: None },
        };
        let mut diagnostic = Self::detail(kind, intent, at, Some(target.price()));
        match error {
            RiskError::MinimumRiskRewardNotMet { actual, required } => {
                diagnostic.actual_gross_rr = Some(*actual);
                diagnostic.required_gross_rr = Some(*required);
            }
            RiskError::QuantityBelowMinimum { calculated, minimum } => {
                diagnostic.calculated_quantity = Some(*calculated);
                diagnostic.minimum_quantity = Some(*minimum);
            }
            _ => {}
        }
        Self { reason, diagnostic: Some(diagnostic) }
    }

    fn book(error: &BookError, intent: &TradeIntent, target: &LiquidityTarget, at: DateTime<Utc>) -> Self {
        let mut rejected = Self::simple(book_rejection(error), intent, at, Some(target.price()));
        if *error == BookError::Broker(PaperError::Busy) {
            rejected.diagnostic = Some(Self::detail(AdmissionFailureKind::BrokerBusy, intent, at, Some(target.price())));
        }
        rejected
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
    /// Paired SMT or completed M5 research validation failed.
    #[error(transparent)]
    SmtResearch(#[from] SmtResearchError),
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
    live: LiveSetupCounters,
}

impl SymbolState {
    fn new(instrument: PaperInstrument, timeframe: Timeframe, entry_window: PaperEntryWindow,
        entry_policy: StrategyEntryPolicy) -> Self {
        let config = IctEngineConfig::default();
        let liquidity_capacity = config.limits().swing_capacity().get();
        Self { key: CandleKey { instrument: instrument.spec.instrument().clone(), timeframe },
            instrument, ict: IctEngine::new(config),
            discovery: StrategyEngine::new(entry_window.strategy_config(entry_policy)),
            liquidity: VecDeque::new(), liquidity_capacity,
            last_analyzed_close: None, last_discard: None, live: LiveSetupCounters::default() }
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

    fn research_targets(&self, intent: &TradeIntent, at: DateTime<Utc>) -> Result<Vec<LiquidityTarget>, RiskError> {
        let mut swings: Vec<_> = self.liquidity.iter().filter(|swing| swing.confirmed_at() <= at)
            .filter(|swing| match intent.direction() {
                Direction::Bullish => swing.kind() == SwingKind::High && swing.price() > intent.entry_price(),
                Direction::Bearish => swing.kind() == SwingKind::Low && swing.price() < intent.entry_price(),
            }).collect();
        swings.sort_by(|left, right| match intent.direction() {
            Direction::Bullish => left.price().cmp(&right.price()),
            Direction::Bearish => right.price().cmp(&left.price()),
        });
        swings.into_iter().map(|swing| {
            let identity = format!("paper-multi-liquidity:{:?}:{:?}:{}:{}", self.key,
                swing.kind(), swing.candle_open_time(), swing.price().normalize());
            LiquidityTarget::new(Uuid::new_v5(&Uuid::NAMESPACE_OID, identity.as_bytes()),
                self.key.instrument.clone(), match swing.kind() {
                    SwingKind::High => LiquiditySide::BuySide,
                    SwingKind::Low => LiquiditySide::SellSide,
                }, swing.price(), swing.confirmed_at())
        }).collect()
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
    entry_window: PaperEntryWindow,
    target_policy: PaperTargetPolicy,
    entry_policy: StrategyEntryPolicy,
    smt_research: Option<SmtResearchState>,
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
        Self::new_with_entry_window(instruments, config, timeframe, PaperEntryWindow::Killzones)
    }

    /// Creates one shared account with an explicit immutable entry-time policy.
    ///
    /// Setup detection, owner lifecycle and every financial limit are the same
    /// for both policies. Each subsequent discovery strategy inherits this
    /// policy, including after an earlier setup was admitted or rejected.
    ///
    /// # Errors
    /// Rejects unsupported/duplicate instruments, non-M1 input and bad metadata.
    pub fn new_with_entry_window(instruments: Vec<PaperInstrument>, config: PaperConfig,
        timeframe: Timeframe, entry_window: PaperEntryWindow) -> Result<Self, MultiPortfolioError> {
        Self::new_for_research(instruments, config, timeframe, entry_window, PaperTargetPolicy::Nearest)
    }

    /// Creates an explicit research engine without changing live/replay defaults.
    ///
    /// `NearestViable` considers only confirmed, unswept liquidity already known
    /// at entry time. Each candidate stages risk and book admission, committing
    /// only the first success. Only gross/net RR failures permit trying farther
    /// targets; cash, sizing, direction, slot and risk failures stop the search.
    /// This hypothesis requires independent evaluation before live promotion.
    ///
    /// # Errors
    /// Rejects unsupported/duplicate instruments, non-M1 input and bad metadata.
    pub fn new_for_research(instruments: Vec<PaperInstrument>, config: PaperConfig,
        timeframe: Timeframe, entry_window: PaperEntryWindow, target_policy: PaperTargetPolicy,
    ) -> Result<Self, MultiPortfolioError> {
        Self::new_for_research_with_fill_timing(instruments, config, timeframe, entry_window,
            target_policy, PaperFillTiming::ImmediateNextBar)
    }

    /// Creates an offline experiment with explicit target and entry-timing policies.
    ///
    /// A fixed one-bar embargo is only a sensitivity test, not a model of actual
    /// admission timestamps or delayed-feed catch-up. Live and replay defaults
    /// remain unchanged; research reports must record both policies.
    ///
    /// # Errors
    /// Rejects unsupported/duplicate instruments, non-M1 input and bad metadata.
    pub fn new_for_research_with_fill_timing(instruments: Vec<PaperInstrument>, config: PaperConfig,
        timeframe: Timeframe, entry_window: PaperEntryWindow, target_policy: PaperTargetPolicy,
        fill_timing: PaperFillTiming,
    ) -> Result<Self, MultiPortfolioError> {
        Self::new_for_research_with_entry_policy(instruments, config, timeframe, entry_window,
            target_policy, fill_timing, StrategyEntryPolicy::LegacyRetracement)
    }

    /// Creates a research portfolio with an explicit entry-state-machine policy.
    ///
    /// Resting-on-confirmation may create a pending order before any retracement.
    /// The book still owns all fills, cash, margin and risk limits. This policy is
    /// not selectable in V1-V4 journals or the existing live CLI.
    ///
    /// # Errors
    /// Rejects unsupported/duplicate instruments, non-M1 input and bad metadata.
    pub fn new_for_research_with_entry_policy(instruments: Vec<PaperInstrument>, config: PaperConfig,
        timeframe: Timeframe, entry_window: PaperEntryWindow, target_policy: PaperTargetPolicy,
        fill_timing: PaperFillTiming, entry_policy: StrategyEntryPolicy,
    ) -> Result<Self, MultiPortfolioError> {
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
            if symbols.insert(symbol, SymbolState::new(instrument, timeframe, entry_window, entry_policy)).is_some() {
                return Err(PortfolioError::Configuration("duplicate instrument").into());
            }
        }
        Ok(Self { symbols, owners: BTreeMap::new(), consumed_mss: BTreeSet::new(),
            consumed_fvg: BTreeSet::new(), timeframe, entry_window, target_policy, entry_policy, smt_research: None,
            risk, book: PaperBook::new_for_research(config, fill_timing)?,
            history: BTreeMap::new(), phase: Phase::WarmingUp, insolvent: false })
    }

    /// Creates an offline BTC/ETH portfolio requiring causal paired SMT evidence.
    ///
    /// Every intent still requires genuine M1 MSS, displacement and next-candle
    /// FVG. The optional M5 alignment is a trend-pullback hypothesis, not a BOS
    /// mislabeled as MSS. Entry stays 24/7; the fixed risk/slot limits apply.
    /// No live CLI or V1-V4 journal selects this policy.
    ///
    /// # Errors
    /// Requires both assets, 100 USDT, x10 and a net RR floor of at least two.
    pub fn new_for_smt_research(instruments: Vec<PaperInstrument>, config: PaperConfig,
        research: SmtResearchConfig) -> Result<Self, MultiPortfolioError> {
        if instruments.len() != 2 || config.initial_balance() != Decimal::from(100)
            || config.leverage() != 10 || config.min_net_rr() < Decimal::TWO {
            return Err(PortfolioError::Configuration("SMT research requires BTC+ETH, 100 USDT, x10 and net RR >=2").into());
        }
        let mut engine = Self::new_for_research_with_entry_policy(instruments, config, Timeframe::M1,
            PaperEntryWindow::Always, PaperTargetPolicy::NearestViable, research.fill_timing,
            StrategyEntryPolicy::RestingOnConfirmation)?;
        let default = IctEngineConfig::default();
        let ict_config = IctEngineConfig::new(research.fractal_model, default.break_basis(), default.fvg_mitigation(),
            default.sweep_confirmation(), default.limits(), default.displacement().clone(), default.killzones())
            .map_err(PortfolioError::from)?;
        for state in engine.symbols.values_mut() { state.ict = IctEngine::new(ict_config.clone()); }
        engine.smt_research = Some(SmtResearchState::new(research));
        Ok(engine)
    }

    /// Immutable research proof log; absent for ordinary or poisoned sessions.
    #[must_use]
    pub fn smt_research_report(&self) -> Option<&SmtResearchReport> {
        if self.is_poisoned() { None } else { self.smt_research.as_ref().map(|state| &state.report) }
    }

    /// Returns the policy retained by this session and all fresh discoveries.
    #[must_use]
    pub const fn entry_window(&self) -> PaperEntryWindow { self.entry_window }

    /// Reports the explicit research policy; ordinary sessions return `Nearest`.
    #[must_use]
    pub const fn target_policy(&self) -> PaperTargetPolicy { self.target_policy }

    /// Reports the fixed research timing; ordinary live/replay sessions are immediate.
    #[must_use]
    pub const fn fill_timing(&self) -> PaperFillTiming { self.book.fill_timing() }

    /// Entry algorithm inherited by discovery and every transferred owner.
    #[must_use]
    pub const fn entry_policy(&self) -> StrategyEntryPolicy { self.entry_policy }

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
            live: state.live.clone(),
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
        // Analyze both assets at the same close before any paired decision.
        // Sequential admission remains stable BTC then ETH, but neither reads
        // a peer detector from the preceding minute.
        let mut analyzed = Vec::with_capacity(ordered.len());
        for candle in &ordered {
            let batch = self.state_mut(candle.instrument().venue_symbol.as_str())?.analyze(candle)?;
            analyzed.push((*candle, batch));
        }
        if let Some(research) = &mut self.smt_research { research.analyze(&analyzed, warmup)?; }
        self.cancel_lost_smt_pending(at, &mut step.events)?;
        let allowed_sweeps = self.smt_research.as_ref().map(SmtResearchState::allowed_sweeps);
        let mut live_updates = Vec::with_capacity(self.symbols.len());
        for ((candle, batch), analysis) in analyzed.into_iter().zip(&mut step.analysis) {
            let symbol = candle.instrument().venue_symbol.as_str();
            let state = self.state_mut(symbol)?;
            let mut live = (!warmup).then(|| state.live.clone());
            analysis.signals = batch.signals().len();
            if let Some(counters) = &mut live { counters.observe_detection(&batch, at); }
            let event = IctEngineEvent::CandleAnalyzed { candle: (*candle).clone(), batch };
            self.advance_owners(symbol, &event, analysis, &mut step.events)?;
            let discovery = &mut self.state_mut(symbol)?.discovery;
            let actions = match &allowed_sweeps {
                Some(allowed) => discovery.process_event_with_sweep_filter(event, allowed)?,
                None => discovery.process_event(event)?,
            };
            if let Some(counters) = &mut live { counters.observe_actions(&actions); }
            let admission_failures = self.discovery_actions(symbol, actions, candle, warmup, analysis, &mut step.events)?;
            if let Some(mut counters) = live {
                counters.observe_outcome(analysis, &step.events);
                for failure in admission_failures { counters.observe_admission_failure(failure); }
                live_updates.push(counters);
            }
        }
        self.history.insert(first.open_time(), ordered.into_iter().cloned().collect());
        // Commit observations only after every symbol completed successfully.
        // Both vectors follow the same validated, stable symbol ordering.
        for (state, counters) in self.symbols.values_mut().zip(live_updates) {
            state.live = counters;
        }
        step.snapshot = self.snapshot();
        Ok(step)
    }

    fn cancel_lost_smt_pending(&mut self, at: DateTime<Utc>, events: &mut Vec<PortfolioEvent>) -> Result<(), MultiPortfolioError> {
        let Some(research) = &self.smt_research else { return Ok(()); };
        let pending = self.book.snapshot().orders.iter().filter(|order| order.state == super::book::BookOrderState::Pending)
            .map(|order| order.order.intent_id).collect();
        let lost = research.lost_pending(&pending);
        // The book already processed this bar. Never retroactively erase a fill
        // using a peer breach only observed at the end of that same bar.
        for intent_id in lost {
            let cancelled = self.book.cancel_intent(intent_id, at)?;
            self.route_events(&cancelled)?;
            events.extend(cancelled);
            if let Some(research) = &mut self.smt_research {
                research.report.pending_cancelled_after_smt_lost = research.report.pending_cancelled_after_smt_lost.saturating_add(1);
            }
        }
        Ok(())
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
        warmup: bool, analysis: &mut MultiSymbolAnalysis, events: &mut Vec<PortfolioEvent>) -> Result<Vec<AdmissionFailureDiagnostic>, MultiPortfolioError> {
        let mut failures = Vec::new();
        for action in actions {
            match action {
                StrategyAction::TradeIntentGenerated(intent) => {
                    analysis.intents += 1;
                    // Move the actual pending strategy; never fake a fill/exit
                    // merely to let the symbol search for its next setup.
                    let strategy_config = self.entry_window.strategy_config(self.entry_policy);
                    let state = self.state_mut(symbol)?;
                    let owner = IntentOwner { key: state.key.clone(), strategy: std::mem::replace(
                        &mut state.discovery, StrategyEngine::new(strategy_config)) };
                    if let Some(rejected) = self.submit_intent(symbol, &intent, owner, candle, warmup, events)? {
                        analysis.reject(rejected.reason);
                        if let Some(diagnostic) = rejected.diagnostic { failures.push(diagnostic); }
                    } else if let Some(research) = &mut self.smt_research {
                        let order = events.iter().rev().find_map(|event| match event {
                            PortfolioEvent::EntryPlaced { order, .. } if order.intent_id == intent.id() => Some(order.clone()),
                            _ => None,
                        }).ok_or(PortfolioError::Invariant("admitted SMT intent missing its entry event"))?;
                        research.record_admission(&intent, candle.close_time(), order)?;
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
        Ok(failures)
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
        candle: &Candle, warmup: bool, events: &mut Vec<PortfolioEvent>) -> Result<Option<RejectedAdmission>, MultiPortfolioError> {
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
            return Ok(Some(RejectedAdmission::simple(reason, intent, at, None)));
        }
        if self.owners.contains_key(&intent.id()) {
            return Err(PortfolioError::Invariant("intent identifier already has an owner").into());
        }
        if let Some(research) = &self.smt_research {
            if let Some(failure) = research.gate(intent, at)? {
                Self::reject_owner(&mut owner, intent.id(), at)?;
                let reason = match failure { SmtGateFailure::Missing => MultiRejection::SmtRequired,
                    SmtGateFailure::M5Alignment => MultiRejection::M5Alignment };
                return Ok(Some(RejectedAdmission::simple(reason, intent, at, None)));
            }
        }
        if self.target_policy == PaperTargetPolicy::NearestViable {
            return self.submit_research_target(symbol, intent, owner, candle, events);
        }
        let Some(target) = self.state(symbol)?.nearest_target(intent, at)? else {
            Self::reject_owner(&mut owner, intent.id(), at)?;
            return Ok(Some(RejectedAdmission::simple(MultiRejection::NoOpposingLiquidity, intent, at, None)));
        };
        let assessment = match self.risk.assess_trade(intent, &target, at) {
            Ok(assessment) => assessment,
            Err(error) if is_entry_rejection(&error) => {
                Self::reject_owner(&mut owner, intent.id(), at)?;
                return Ok(Some(RejectedAdmission::risk(&error, intent, &target, at)));
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
                self.risk.apply_reservation_event(intent.id(), ReservationEvent::EntryCancelled, at)?;
                Self::reject_owner(&mut owner, intent.id(), at)?;
                Ok(Some(RejectedAdmission::book(&error, intent, &target, at)))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn submit_research_target(&mut self, symbol: &str, intent: &TradeIntent, mut owner: IntentOwner,
        candle: &Candle, events: &mut Vec<PortfolioEvent>,
    ) -> Result<Option<RejectedAdmission>, MultiPortfolioError> {
        let at = candle.close_time();
        let state = self.state(symbol)?;
        let targets = state.research_targets(intent, at)?;
        let instrument = state.instrument.clone();
        let mut rejection = RejectedAdmission::simple(MultiRejection::NoOpposingLiquidity, intent, at, None);
        for target in targets {
            // A failed trial must not reserve this intent ID, leave an approval
            // tombstone, consume margin, or prevent assessing the next target.
            let mut risk = self.risk.clone();
            let assessment = match risk.assess_trade(intent, &target, at) {
                Ok(assessment) => assessment,
                Err(error @ RiskError::MinimumRiskRewardNotMet { .. }) => {
                    rejection = RejectedAdmission::risk(&error, intent, &target, at);
                    continue;
                }
                Err(error) if is_entry_rejection(&error) => {
                    rejection = RejectedAdmission::risk(&error, intent, &target, at);
                    break;
                }
                Err(error) => return Err(error.into()),
            };
            let TradeAssessment::Approved(plan) = assessment else {
                return Err(PortfolioError::Invariant("unexpected research risk replay after setup deduplication").into());
            };
            let mut book = self.book.clone();
            match book.admit(&instrument, *plan, candle) {
                Ok(placed) => {
                    if !matches!(placed.as_slice(), [PortfolioEvent::EntryPlaced { symbol: placed_symbol, at: placed_at, order }]
                        if placed_symbol == symbol && *placed_at == at && order.intent_id == intent.id()) {
                        return Err(PortfolioError::Invariant("research admission did not create exactly one resting entry").into());
                    }
                    self.risk = risk;
                    self.book = book;
                    self.owners.insert(intent.id(), owner);
                    events.extend(placed);
                    return Ok(None);
                }
                Err(error @ BookError::Broker(PaperError::NetRiskReward)) => {
                    rejection = RejectedAdmission::book(&error, intent, &target, at);
                }
                Err(error) if error.is_rejection() => {
                    rejection = RejectedAdmission::book(&error, intent, &target, at);
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Self::reject_owner(&mut owner, intent.id(), at)?;
        Ok(Some(rejection))
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

    fn research_portfolio(policy: PaperTargetPolicy, config: PaperConfig) -> Result<MultiPortfolio, Box<dyn Error>> {
        Ok(MultiPortfolio::new_for_research(vec![metadata("ETHUSDT")?, metadata("BTCUSDT")?],
            config, Timeframe::M1, PaperEntryWindow::Always, policy)?)
    }

    fn resting_portfolio() -> Result<MultiPortfolio, Box<dyn Error>> {
        Ok(MultiPortfolio::new_for_research_with_entry_policy(
            vec![metadata("ETHUSDT")?, metadata("BTCUSDT")?],
            PaperConfig::default().with_leverage(10)?, Timeframe::M1,
            PaperEntryWindow::Always, PaperTargetPolicy::Nearest,
            PaperFillTiming::ImmediateNextBar, StrategyEntryPolicy::RestingOnConfirmation)?)
    }

    #[test]
    fn resting_entry_is_pending_on_confirmation_and_fills_only_a_later_bar() -> TestResult {
        let mut resting = resting_portfolio()?;
        let mut legacy = research_portfolio(PaperTargetPolicy::Nearest,
            PaperConfig::default().with_leverage(10)?)?;
        let prices = setup(150);
        for (index, row) in prices.iter().take(24).enumerate() {
            let candles = batch(i64::try_from(index)?, *row, NEUTRAL)?;
            resting.process_batch(&candles)?;
            legacy.process_batch(&candles)?;
        }
        let confirmed = resting.snapshot().account;
        assert_eq!(confirmed.orders.len(), 1);
        assert_eq!(confirmed.orders[0].state, BookOrderState::Pending);
        assert_eq!(confirmed.fees_paid, Decimal::ZERO);
        assert!(legacy.snapshot().account.orders.is_empty());
        assert_eq!(resting.diagnostics()[0].live.intents, 1);
        // A real retracement fills the previously resting limit, while the
        // legacy strategy only now submits its limit for another bar.
        resting.process_batch(&batch(24, prices[24], NEUTRAL)?)?;
        legacy.process_batch(&batch(24, prices[24], NEUTRAL)?)?;
        assert_eq!(resting.snapshot().account.orders[0].state, BookOrderState::Open);
        assert_eq!(legacy.snapshot().account.orders[0].state, BookOrderState::Pending);
        assert!(resting.snapshot().account.reserved_risk <= Decimal::ONE);
        Ok(())
    }

    #[test]
    fn resting_policy_survives_owner_transfer_and_warmup_without_spending_in_warmup() -> TestResult {
        for first_is_warmup in [false, true] {
            let mut engine = resting_portfolio()?;
            for (index, row) in setup(1_000).into_iter().enumerate() {
                let candles = batch(i64::try_from(index)?, row, NEUTRAL)?;
                if first_is_warmup { engine.warm_up(&candles)?; }
                else { engine.process_batch(&candles)?; }
            }
            assert_eq!(engine.snapshot().account.orders.len(), usize::from(!first_is_warmup));
            if first_is_warmup {
                assert_eq!(engine.snapshot().account.balance, Decimal::from(100));
                assert_eq!(engine.snapshot().account.reserved_risk, Decimal::ZERO);
            }
            engine.process_batch(&batch(25, [109, 110, 103, 109], NEUTRAL)?)?;
            // Stop at FVG confirmation, before the second CE retracement.
            let rows = subsequent_setup(700, 50);
            for (index, row) in rows.iter().take(rows.len() - 1).enumerate() {
                engine.process_batch(&batch(26 + i64::try_from(index)?, *row, NEUTRAL)?)?;
            }
            let account = engine.snapshot().account;
            assert_eq!(account.orders.len(), if first_is_warmup { 1 } else { 2 });
            assert_eq!(account.orders.iter().filter(|order| order.state == BookOrderState::Pending).count(), 1);
            assert_eq!(engine.entry_policy(), StrategyEntryPolicy::RestingOnConfirmation);
            assert!(account.reserved_risk <= Decimal::from(3));
        }
        Ok(())
    }

    fn smt_portfolio(alignment: bool) -> Result<MultiPortfolio, Box<dyn Error>> {
        Ok(MultiPortfolio::new_for_smt_research(vec![metadata("BTCUSDT")?, metadata("ETHUSDT")?],
            PaperConfig::default().with_leverage(10)?, SmtResearchConfig {
                fractal_model: crate::models::FractalModel::FiveCandle,
                require_m5_alignment: alignment, fill_timing: PaperFillTiming::ImmediateNextBar })?)
    }

    fn smt_peer(minute: usize) -> [i64; 4] {
        if minute == 18 { [100, 102, 95, 101] } else { NEUTRAL }
    }

    fn smt_ready_pending() -> Result<MultiPortfolio, Box<dyn Error>> {
        let mut engine = smt_portfolio(false)?;
        for (index, row) in setup(150).into_iter().take(24).enumerate() {
            engine.process_batch(&batch(i64::try_from(index)?, row, smt_peer(index))?)?;
        }
        assert_eq!(engine.snapshot().account.orders.len(), 1);
        assert_eq!(engine.snapshot().account.orders[0].state, BookOrderState::Pending);
        Ok(engine)
    }

    #[test]
    fn smt_admission_has_frozen_paired_proof_and_is_input_order_independent() -> TestResult {
        let mut forward = smt_portfolio(false)?;
        let mut reversed = smt_portfolio(false)?;
        for (index, row) in setup(150).into_iter().take(24).enumerate() {
            let mut candles = batch(i64::try_from(index)?, row, smt_peer(index))?;
            let expected = forward.process_batch(&candles)?;
            candles.reverse();
            let actual = reversed.process_batch(&candles)?;
            assert_eq!(serde_json::to_value(expected)?, serde_json::to_value(actual)?);
        }
        let report = forward.smt_research_report().ok_or("missing SMT research")?;
        assert_eq!(report.admissions.len(), 1);
        let proof = &report.admissions[0];
        assert_eq!(proof.approved_at, at(24)?);
        assert_eq!(proof.smt.focus_anchor().candle_open_time(), at(18)?);
        assert_eq!(proof.smt.peer_anchor().candle_open_time(), at(18)?);
        assert_eq!(proof.smt.detected_at(), at(22)?);
        assert_eq!(proof.smt.focus_sweep(), proof.intent.liquidity_sweep());
        assert_eq!(proof.peer_at_admission.close_time(), proof.approved_at);
        assert_eq!(proof.order.intent_id, proof.intent.id());
        assert_eq!(serde_json::to_value(report)?, serde_json::to_value(reversed.smt_research_report())?);
        assert!(forward.snapshot().account.reserved_risk <= Decimal::ONE);
        Ok(())
    }

    #[test]
    fn smt_missing_peer_both_swept_and_unknown_m5_do_not_admit() -> TestResult {
        for (paired, both_swept, alignment) in [(false, false, false), (true, true, false), (true, false, true)] {
            let mut engine = smt_portfolio(alignment)?;
            let mut reasons = Vec::new();
            for (index, row) in setup(150).into_iter().enumerate() {
                let peer = if both_swept && index == 21 { [100, 102, 94, 101] }
                    else if paired { smt_peer(index) } else { NEUTRAL };
                let step = engine.process_batch(&batch(i64::try_from(index)?, row, peer)?)?;
                reasons.extend(step.analysis.into_iter().flat_map(|analysis| analysis.rejection_reasons));
            }
            assert!(engine.snapshot().account.orders.is_empty());
            assert_eq!(engine.snapshot().account.available_cash, Decimal::from(100));
            assert!(engine.smt_research_report().ok_or("missing research")?.admissions.is_empty());
            if alignment { assert!(reasons.contains(&MultiRejection::M5Alignment)); }
        }
        Ok(())
    }

    #[test]
    fn smt_loss_cancels_unfilled_limit_but_never_retroactively_erases_same_bar_fill() -> TestResult {
        for fill in [false, true] {
            let mut engine = smt_ready_pending()?;
            let btc = if fill { [110,112,103,109] } else { [110,112,106,109] };
            engine.process_batch(&batch(24, btc, [100,102,94,101])?)?;
            let account = engine.snapshot().account;
            let report = engine.smt_research_report().ok_or("missing research")?;
            if fill {
                assert_eq!(account.orders.len(), 1);
                assert_eq!(account.orders[0].state, BookOrderState::Open);
                assert_eq!(report.pending_cancelled_after_smt_lost, 0);
            } else {
                assert!(account.orders.is_empty());
                assert_eq!(account.fees_paid, Decimal::ZERO);
                assert_eq!(account.reserved_risk, Decimal::ZERO);
                assert_eq!(report.pending_cancelled_after_smt_lost, 1);
            }
        }
        Ok(())
    }

    #[test]
    fn smt_warmup_never_creates_historical_orders_or_live_counts() -> TestResult {
        let mut engine = smt_portfolio(false)?;
        for (index, row) in setup(150).into_iter().enumerate() {
            engine.warm_up(&batch(i64::try_from(index)?, row, smt_peer(index))?)?;
        }
        let report = engine.smt_research_report().ok_or("missing research")?;
        assert_eq!(report.confirmed_smt, 0);
        assert!(report.admissions.is_empty());
        assert!(engine.snapshot().account.orders.is_empty());
        assert_eq!(engine.snapshot().account.balance, Decimal::from(100));
        Ok(())
    }

    #[test]
    fn smt_research_cannot_relax_pair_account_leverage_or_net_rr_requirements() -> TestResult {
        let research = SmtResearchConfig { fractal_model: crate::models::FractalModel::ThreeCandle,
            require_m5_alignment: false, fill_timing: PaperFillTiming::ImmediateNextBar };
        assert!(PaperConfig::new(Decimal::from(100), Decimal::new(6,4), Decimal::TWO,
            Decimal::ONE, Decimal::ONE).is_err());
        for config in [PaperConfig::default(),
            PaperConfig::new(Decimal::from(200), Decimal::new(6,4), Decimal::TWO, Decimal::ONE, Decimal::TWO)?.with_leverage(10)?] {
            assert!(MultiPortfolio::new_for_smt_research(vec![metadata("BTCUSDT")?, metadata("ETHUSDT")?],
                config, research).is_err());
        }
        assert!(MultiPortfolio::new_for_smt_research(vec![metadata("BTCUSDT")?],
            PaperConfig::default().with_leverage(10)?, research).is_err());
        Ok(())
    }

    fn two_targets(near: i64, far: i64) -> Vec<[i64; 4]> {
        let mut prices = setup(far);
        // Both highs form strict five-candle swings before the later sweep.
        // Neither is invented when the entry is assessed.
        prices[6] = [100, near, 98, 101];
        prices
    }

    fn run_target_setup(engine: &mut MultiPortfolio, prices: &[[i64; 4]]) -> Result<MultiPortfolioStep, Box<dyn Error>> {
        let mut final_step = None;
        for (index, bar) in prices.iter().enumerate() {
            final_step = Some(engine.process_batch(&batch(i64::try_from(index)?, *bar, NEUTRAL)?)?);
        }
        final_step.ok_or_else(|| "empty target fixture".into())
    }

    #[test]
    fn research_nearest_preserves_the_normal_constructor_and_all_serialized_steps() -> TestResult {
        let config = PaperConfig::default().with_leverage(10)?;
        let mut ordinary = MultiPortfolio::new_with_entry_window(vec![metadata("BTCUSDT")?, metadata("ETHUSDT")?],
            config, Timeframe::M1, PaperEntryWindow::Always)?;
        let mut explicit = research_portfolio(PaperTargetPolicy::Nearest, config)?;
        assert_eq!(ordinary.target_policy(), PaperTargetPolicy::Nearest);
        assert_eq!(ordinary.entry_policy(), StrategyEntryPolicy::LegacyRetracement);
        assert_eq!(explicit.entry_policy(), StrategyEntryPolicy::LegacyRetracement);
        assert_eq!(portfolio()?.target_policy(), PaperTargetPolicy::Nearest);
        for (index, prices) in two_targets(120, 150).into_iter().enumerate() {
            let candles = batch(i64::try_from(index)?, prices, NEUTRAL)?;
            assert_eq!(serde_json::to_value(ordinary.process_batch(&candles)?)?,
                serde_json::to_value(explicit.process_batch(&candles)?)?);
        }
        assert!(ordinary.snapshot().account.orders.is_empty());
        Ok(())
    }

    #[test]
    fn research_can_choose_farther_existing_liquidity_after_gross_rr_rejection() -> TestResult {
        let config = PaperConfig::default().with_leverage(10)?;
        let mut nearest = research_portfolio(PaperTargetPolicy::Nearest, config)?;
        let mut viable = research_portfolio(PaperTargetPolicy::NearestViable, config)?;
        let prices = two_targets(120, 150);
        let baseline = run_target_setup(&mut nearest, &prices)?;
        let candidate = run_target_setup(&mut viable, &prices)?;
        assert_eq!(baseline.analysis[0].rejection_reasons, [MultiRejection::RiskRule]);
        assert_eq!(baseline.analysis[0].intents, 1);
        assert_eq!(candidate.analysis[0].intents, 1);
        assert_eq!(candidate.analysis[0].rejected, 0);
        // Failed near targets are trials, not rejected intents, when a farther
        // target succeeds. Do not misreport them in live diagnostics.
        assert!(viable.diagnostics()[0].live.admission_failures.is_empty());
        assert!(viable.diagnostics()[0].live.last_admission_failure.is_none());
        let [order] = candidate.snapshot.account.orders.as_slice() else { return Err("expected one research entry".into()); };
        assert_eq!(order.order.take_profit_price, Decimal::from(150));
        assert_eq!(order.state, BookOrderState::Pending);
        assert!(order.reserved_risk <= Decimal::ONE);
        assert_eq!(viable.risk.active_reserved_risk()?, order.reserved_risk);
        assert_eq!(candidate.snapshot.account.balance, Decimal::from(100));
        let entry_at = at(25)?;
        assert!(viable.state("BTCUSDT")?.liquidity.iter().any(|swing| swing.kind() == SwingKind::High
            && swing.price() == Decimal::from(150) && swing.confirmed_at() <= entry_at));
        Ok(())
    }

    #[test]
    fn research_retries_fee_adjusted_rr_without_retaining_failed_approval() -> TestResult {
        // An elevated but valid fee assumption makes the near 135 target pass
        // gross RR and fail the broker's actual net RR check.
        let config = PaperConfig::new(Decimal::from(100), Decimal::new(2, 3), Decimal::TWO,
            Decimal::ONE, Decimal::TWO)?.with_leverage(10)?;
        let mut nearest = research_portfolio(PaperTargetPolicy::Nearest, config)?;
        let mut viable = research_portfolio(PaperTargetPolicy::NearestViable, config)?;
        let prices = two_targets(135, 150);
        let baseline = run_target_setup(&mut nearest, &prices)?;
        let candidate = run_target_setup(&mut viable, &prices)?;
        assert_eq!(baseline.analysis[0].rejection_reasons, [MultiRejection::NetRiskReward]);
        let [order] = candidate.snapshot.account.orders.as_slice() else { return Err("expected fee-eligible target".into()); };
        assert_eq!(order.order.take_profit_price, Decimal::from(150));
        assert_eq!(viable.risk.reservation_status(order.order.intent_id), Some(ReservationStatus::Pending));
        assert_eq!(viable.owners.len(), 1);
        assert_eq!(viable.risk.active_reserved_risk()?, order.reserved_risk);
        assert!(order.reserved_risk <= Decimal::ONE);
        assert!(viable.diagnostics()[0].live.admission_failures.is_empty());
        Ok(())
    }

    #[test]
    fn admission_diagnostics_distinguish_exact_gross_rr_from_fee_adjusted_rejection() -> TestResult {
        let mut gross = research_portfolio(PaperTargetPolicy::Nearest,
            PaperConfig::default().with_leverage(10)?)?;
        let prices = two_targets(120, 150);
        let step = run_target_setup(&mut gross, &prices)?;
        assert_eq!(step.analysis[0].rejection_reasons, [MultiRejection::RiskRule]);
        let live = gross.diagnostics()[0].live.clone();
        assert_eq!(live.admission_failures, [DiagnosticCount { reason: AdmissionFailureKind::GrossRiskReward, count: 1 }]);
        let failure = live.last_admission_failure.ok_or("missing gross RR diagnostic")?;
        assert_eq!(failure.at, at(25)?);
        assert_eq!(failure.target_price, Some(Decimal::from(120)));
        // The real default detector forms CE=104 and the risk manager rounds
        // the stop one 0.1 tick below the sweep's 89 extreme.
        assert_eq!(failure.actual_gross_rr, Decimal::from(16).checked_div(Decimal::new(151, 1)));
        assert_eq!(failure.required_gross_rr, Some(Decimal::TWO));
        assert!(failure.calculated_quantity.is_none() && failure.minimum_quantity.is_none());
        assert_eq!(step.snapshot.account.balance, Decimal::from(100));
        assert_eq!(gross.risk.active_reserved_risk()?, Decimal::ZERO);
        assert!(step.snapshot.account.orders.is_empty());
        // A replayed input neither increments counts nor replaces the last failure.
        gross.process_batch(&batch(24, prices[24], NEUTRAL)?)?;
        assert_eq!(gross.diagnostics()[0].live, live);
        let encoded = serde_json::to_value(step)?;
        assert_eq!(encoded["analysis"][0]["rejection_reasons"], serde_json::json!(["risk_rule"]));
        assert!(encoded["analysis"][0].get("admission_failures").is_none());

        let fees = PaperConfig::new(Decimal::from(100), Decimal::new(2, 3), Decimal::TWO,
            Decimal::ONE, Decimal::TWO)?.with_leverage(10)?;
        let mut net = research_portfolio(PaperTargetPolicy::Nearest, fees)?;
        let step = run_target_setup(&mut net, &two_targets(135, 150))?;
        assert_eq!(step.analysis[0].rejection_reasons, [MultiRejection::NetRiskReward]);
        let live = net.diagnostics()[0].live.clone();
        assert_eq!(live.admission_failures, [DiagnosticCount { reason: AdmissionFailureKind::NetRiskReward, count: 1 }]);
        let failure = live.last_admission_failure.ok_or("missing net RR diagnostic")?;
        assert_eq!(failure.target_price, Some(Decimal::from(135)));
        assert!(failure.actual_gross_rr.is_none() && failure.required_gross_rr.is_none());
        assert_eq!(net.risk.active_reserved_risk()?, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn admission_diagnostics_report_actual_risk_sized_minimum_quantity() -> TestResult {
        let mut btc = metadata("BTCUSDT")?;
        btc.spec = InstrumentRiskSpec::new(instrument("BTCUSDT")?, Decimal::new(1, 1),
            Decimal::ONE, Decimal::ONE, Some(Decimal::from(100)), Decimal::ONE)?;
        let mut engine = MultiPortfolio::new_with_entry_window(vec![btc, metadata("ETHUSDT")?],
            PaperConfig::default().with_leverage(10)?, Timeframe::M1, PaperEntryWindow::Always)?;
        let step = run_target_setup(&mut engine, &setup(150))?;
        assert_eq!(step.analysis[0].rejection_reasons, [MultiRejection::RiskRule]);
        let failure = engine.diagnostics()[0].live.last_admission_failure.ok_or("missing quantity diagnostic")?;
        assert_eq!(failure.kind, AdmissionFailureKind::QuantityBelowMinimum);
        assert_eq!(failure.calculated_quantity, Some(Decimal::ZERO));
        assert_eq!(failure.minimum_quantity, Some(Decimal::ONE));
        assert!(failure.actual_gross_rr.is_none() && failure.required_gross_rr.is_none());
        assert_eq!(step.snapshot.account.available_cash, Decimal::from(100));
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn research_admission_diagnostics_count_only_the_terminal_failed_target() -> TestResult {
        let mut engine = research_portfolio(PaperTargetPolicy::NearestViable,
            PaperConfig::default().with_leverage(10)?)?;
        run_target_setup(&mut engine, &two_targets(120, 125))?;
        let live = engine.diagnostics()[0].live.clone();
        assert_eq!((live.intents, live.rejected), (1, 1));
        assert_eq!(live.admission_failures, [DiagnosticCount { reason: AdmissionFailureKind::GrossRiskReward, count: 1 }]);
        let failure = live.last_admission_failure.ok_or("missing terminal target failure")?;
        assert_eq!(failure.target_price, Some(Decimal::from(125)));
        assert_eq!(failure.actual_gross_rr, Decimal::from(21).checked_div(Decimal::new(151, 1)));
        assert_eq!(failure.required_gross_rr, Some(Decimal::TWO));
        assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn failed_joint_batch_does_not_publish_an_earlier_symbol_admission_failure() -> TestResult {
        let mut engine = research_portfolio(PaperTargetPolicy::Nearest,
            PaperConfig::default().with_leverage(10)?)?;
        let prices = two_targets(120, 150);
        for (index, row) in prices.iter().take(24).enumerate() {
            engine.process_batch(&batch(i64::try_from(index)?, *row, NEUTRAL)?)?;
        }
        let before: Vec<_> = engine.diagnostics().into_iter().map(|row| row.live).collect();
        assert!(before[0].last_admission_failure.is_none());
        // BTC will genuinely reject a completed setup before ETH detects this
        // internal duplicate. No observer from the unsuccessful batch commits.
        engine.state_mut("ETHUSDT")?.ict.process_candle(&candle("ETHUSDT", 24, NEUTRAL)?)?;
        assert!(engine.process_batch(&batch(24, prices[24], NEUTRAL)?).is_err());
        assert!(engine.is_poisoned());
        let after: Vec<_> = engine.diagnostics().into_iter().map(|row| row.live).collect();
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn research_without_a_viable_target_leaves_cash_and_reservations_unchanged() -> TestResult {
        for (prices, remove_liquidity, expected) in [
            (two_targets(120, 125), false, MultiRejection::RiskRule),
            (two_targets(120, 150), true, MultiRejection::NoOpposingLiquidity),
        ] {
            let mut engine = research_portfolio(PaperTargetPolicy::NearestViable,
                PaperConfig::default().with_leverage(10)?)?;
            let mut final_step = None;
            for (index, prices) in prices.iter().enumerate() {
                // Model an empty retained target set at the actual entry; the
                // strategy still has a real previously armed MSS/FVG setup.
                if remove_liquidity && index == 24 { engine.state_mut("BTCUSDT")?.liquidity.clear(); }
                final_step = Some(engine.process_batch(&batch(i64::try_from(index)?, *prices, NEUTRAL)?)?);
            }
            let step = final_step.ok_or("missing target rejection")?;
            assert_eq!(step.analysis[0].intents, 1);
            assert_eq!(step.analysis[0].rejection_reasons, [expected]);
            assert_eq!(step.snapshot.account.balance, Decimal::from(100));
            assert_eq!(step.snapshot.account.available_cash, Decimal::from(100));
            assert_eq!(engine.risk.active_reserved_risk()?, Decimal::ZERO);
            assert!(step.snapshot.account.orders.is_empty() && engine.owners.is_empty());
        }
        Ok(())
    }

    #[test]
    fn research_does_not_override_cash_minimum_or_symbol_slots() -> TestResult {
        // A real setup with a valid TP still cannot consume more than the
        // configured cash cap, even if a farther price exists.
        let config = PaperConfig::new(Decimal::from(100), Decimal::new(6, 4), Decimal::TWO,
            Decimal::new(1, 6), Decimal::TWO)?.with_leverage(10)?;
        let mut cash_limited = research_portfolio(PaperTargetPolicy::NearestViable, config)?;
        let rejected = run_target_setup(&mut cash_limited, &two_targets(150, 200))?;
        assert_eq!(rejected.analysis[0].rejection_reasons, [MultiRejection::MinimumOrder]);
        assert_eq!(cash_limited.risk.active_reserved_risk()?, Decimal::ZERO);
        assert_eq!(rejected.snapshot.account.balance, Decimal::from(100));
        assert!(rejected.snapshot.account.orders.is_empty());

        let mut slots = ready_two_btc()?;
        slots.target_policy = PaperTargetPolicy::NearestViable;
        slots.process_batch(&batch(52, [159, 160, 153, 159], NEUTRAL)?)?;
        let reserved = slots.risk.active_reserved_risk()?;
        let mut final_step = None;
        for (index, prices) in subsequent_setup(400, 100).into_iter().enumerate() {
            final_step = Some(slots.process_batch(&batch(53 + i64::try_from(index)?, prices, NEUTRAL)?)?);
        }
        let rejected = final_step.ok_or("missing slot rejection")?;
        assert_eq!(rejected.analysis[0].rejection_reasons, [MultiRejection::SymbolSlots]);
        assert_eq!(slots.risk.active_reserved_risk()?, reserved);
        assert_eq!(slots.owners.len(), 2);
        assert_eq!(rejected.snapshot.account.orders.len(), 2);
        Ok(())
    }

    fn portfolio_with_window(entry_window: PaperEntryWindow) -> Result<MultiPortfolio, Box<dyn Error>> {
        Ok(MultiPortfolio::new_with_entry_window(vec![metadata("ETHUSDT")?, metadata("BTCUSDT")?],
            PaperConfig::default().with_leverage(10)?, Timeframe::M1, entry_window)?)
    }

    #[test]
    fn always_allows_real_night_and_weekend_setups_while_legacy_does_not() -> TestResult {
        for offset in [-360, 3 * 24 * 60] {
            let mut legacy = portfolio()?;
            let mut continuous = portfolio_with_window(PaperEntryWindow::Always)?;
            assert_eq!(legacy.entry_window(), PaperEntryWindow::Killzones);
            assert_eq!(continuous.entry_window(), PaperEntryWindow::Always);
            for (minute, prices) in setup(150).into_iter().enumerate() {
                let candles = batch(offset + i64::try_from(minute)?, prices, NEUTRAL)?;
                assert!(!PaperEntryWindow::Killzones.allows(candles[0].close_time()));
                assert!(PaperEntryWindow::Always.allows(candles[0].close_time()));
                legacy.process_batch(&candles)?;
                continuous.process_batch(&candles)?;
            }
            assert!(legacy.snapshot().account.orders.is_empty());
            assert_eq!(legacy.diagnostics()[0].last_discard.map(|last| last.reason),
                Some(SetupDiscardReason::EntryTouchedOutsideKillzone));
            let account = continuous.snapshot().account;
            assert_eq!(account.orders.len(), 1);
            assert_eq!(account.orders[0].state, BookOrderState::Pending);
            assert!(account.orders[0].reserved_risk <= Decimal::ONE);
            assert!(account.reserved_risk <= Decimal::from(3));
            assert_eq!(account.balance, Decimal::from(100));
            assert_eq!(account.leverage, 10);
        }
        Ok(())
    }

    #[test]
    fn always_survives_discovery_replacement_after_admission_and_warmup_rejection() -> TestResult {
        for first_is_warmup in [false, true] {
            let mut engine = portfolio_with_window(PaperEntryWindow::Always)?;
            let offset = -360;
            for (minute, prices) in setup(1_000).into_iter().enumerate() {
                let candles = batch(offset + i64::try_from(minute)?, prices, NEUTRAL)?;
                if first_is_warmup { engine.warm_up(&candles)?; }
                else { engine.process_batch(&candles)?; }
            }
            assert_eq!(engine.snapshot().account.orders.len(), usize::from(!first_is_warmup));
            engine.process_batch(&batch(offset + 25, [109, 110, 103, 109], NEUTRAL)?)?;
            for (index, prices) in subsequent_setup(700, 50).into_iter().enumerate() {
                let candles = batch(offset + 26 + i64::try_from(index)?, prices, NEUTRAL)?;
                assert!(!PaperEntryWindow::Killzones.allows(candles[0].close_time()));
                engine.process_batch(&candles)?;
            }
            let account = engine.snapshot().account;
            assert_eq!(account.orders.len(), if first_is_warmup { 1 } else { 2 });
            assert_eq!(account.orders.iter().filter(|order| order.state == BookOrderState::Pending).count(), 1);
            assert_eq!(engine.entry_window(), PaperEntryWindow::Always);
            assert!(account.reserved_risk <= Decimal::from(3));
            assert!(account.orders.iter().all(|order| order.symbol == "BTCUSDT"));
        }
        Ok(())
    }

    #[test]
    fn policies_have_identical_execution_and_risk_during_an_enabled_window() -> TestResult {
        let mut legacy = portfolio()?;
        let mut continuous = portfolio_with_window(PaperEntryWindow::Always)?;
        let mut candles = setup(1_000);
        candles.push([109, 110, 103, 109]);
        candles.extend(subsequent_setup(700, 50));
        candles.push([159, 160, 153, 159]);
        candles.extend(subsequent_setup(400, 100));
        let mut saw_symbol_limit = false;
        for (minute, prices) in candles.into_iter().enumerate() {
            let candles = batch(i64::try_from(minute)?, prices, NEUTRAL)?;
            assert!(PaperEntryWindow::Killzones.allows(candles[0].close_time()));
            let expected = legacy.process_batch(&candles)?;
            let actual = continuous.process_batch(&candles)?;
            assert_eq!(serde_json::to_value(&actual)?, serde_json::to_value(&expected)?);
            saw_symbol_limit |= actual.analysis.iter().any(|row| row.rejection_reasons.contains(&MultiRejection::SymbolSlots));
        }
        assert!(saw_symbol_limit);
        assert_eq!(continuous.snapshot().account.orders.len(), 2);
        Ok(())
    }

    #[test]
    fn entry_window_serialization_is_explicit_and_rejects_unknown_values() -> TestResult {
        for policy in [PaperEntryWindow::Killzones, PaperEntryWindow::Always] {
            let value = serde_json::to_value(policy)?;
            assert_eq!(value, serde_json::Value::String(policy.as_str().to_owned()));
            assert_eq!(serde_json::from_value::<PaperEntryWindow>(value)?, policy);
        }
        for value in [serde_json::json!("24/7"), serde_json::json!("ALWAYS"), serde_json::json!(null), serde_json::json!(true)] {
            assert!(serde_json::from_value::<PaperEntryWindow>(value).is_err());
        }
        Ok(())
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
    fn live_counters_exclude_warmup_and_duplicates_and_match_actual_detector_output() -> TestResult {
        let mut engine = portfolio()?;
        let mut detector = IctEngine::new(IctEngineConfig::default());
        let mut expected_sweeps = 0;
        let mut expected_mss = 0;
        let mut expected_fvgs = 0;
        let mut expected_displacements = 0;
        for (index, prices) in setup(150).into_iter().enumerate() {
            let minute = i64::try_from(index)?;
            let candles = batch(minute, prices, NEUTRAL)?;
            let detected = detector.process_candle(&candle("BTCUSDT", minute, prices)?)?;
            if index < 9 {
                engine.warm_up(&candles)?;
                assert!(engine.diagnostics().iter().all(|row| row.live == LiveSetupCounters::default()));
            } else {
                engine.process_batch(&candles)?;
                expected_sweeps += detected.signals().iter().filter(|signal| matches!(signal, SignalEvent::LiquiditySwept(_))).count();
                expected_mss += detected.signals().iter().filter(|signal| matches!(signal, SignalEvent::MarketStructureShiftDetected(_))).count();
                expected_fvgs += detected.signals().iter().filter(|signal| matches!(signal, SignalEvent::FairValueGapDetected(_))).count();
                expected_displacements += usize::from(detected.displacement().is_some());
            }
        }
        let diagnostics = engine.diagnostics();
        let btc = &diagnostics[0].live;
        assert_eq!(btc.first_close, Some(at(10)?));
        assert_eq!(btc.candles, 16);
        assert_eq!(btc.liquidity_sweeps, u64::try_from(expected_sweeps)?);
        assert_eq!(btc.market_structure_shifts, u64::try_from(expected_mss)?);
        assert_eq!(btc.fair_value_gaps, u64::try_from(expected_fvgs)?);
        assert_eq!(btc.displacement_bars, u64::try_from(expected_displacements)?);
        assert!(btc.liquidity_sweeps > 0 && btc.market_structure_shifts > 0 && btc.fair_value_gaps > 0);
        assert_eq!((btc.intents, btc.admitted, btc.rejected), (1, 1, 0));
        assert_eq!(diagnostics[1].live.candles, 16);
        assert_eq!(diagnostics[1].live.intents, 0);
        // Replaying either an old warm-up bar or the last live bar is a no-op.
        engine.process_batch(&batch(0, NEUTRAL, NEUTRAL)?)?;
        engine.process_batch(&batch(24, [110, 112, 103, 109], NEUTRAL)?)?;
        assert_eq!(engine.diagnostics(), diagnostics);
        // A later actual fill must not increment the entry-admission count.
        let filled = engine.process_batch(&batch(25, [109, 110, 103, 109], NEUTRAL)?)?;
        assert!(filled.events.iter().any(|event| matches!(event,
            PortfolioEvent::Execution { event: PaperEvent::EntryFilled { .. }, .. })));
        assert_eq!(engine.diagnostics()[0].live.admitted, 1);
        Ok(())
    }

    #[test]
    fn warmup_intents_and_discards_are_not_live_observations() -> TestResult {
        for entry_window in [PaperEntryWindow::Always, PaperEntryWindow::Killzones] {
            let mut engine = portfolio_with_window(entry_window)?;
            let mut saw_warmup_rejection = false;
            for (index, prices) in setup(150).into_iter().enumerate() {
                let step = engine.warm_up(&batch(i64::try_from(index)? - 360, prices, NEUTRAL)?)?;
                saw_warmup_rejection |= step.analysis[0].rejection_reasons.contains(&MultiRejection::Warmup);
            }
            assert_eq!(saw_warmup_rejection, entry_window == PaperEntryWindow::Always);
            assert!(engine.diagnostics().iter().all(|row| row.live == LiveSetupCounters::default()));
            engine.process_batch(&batch(-335, [109, 110, 103, 109], NEUTRAL)?)?;
            let live = engine.diagnostics()[0].live.clone();
            assert_eq!(live.candles, 1);
            assert_eq!(live.first_close, Some(at(-334)?));
            assert_eq!((live.intents, live.admitted, live.rejected), (0, 0, 0));
            assert!(live.discards.is_empty() && live.rejection_reasons.is_empty());
        }
        Ok(())
    }

    #[test]
    fn live_counters_report_actual_admission_rejections_and_discard_categories() -> TestResult {
        let mut engine = ready_two_btc()?;
        engine.process_batch(&batch(52, [159, 160, 153, 159], NEUTRAL)?)?;
        for (index, prices) in subsequent_setup(400, 100).into_iter().enumerate() {
            engine.process_batch(&batch(53 + i64::try_from(index)?, prices, NEUTRAL)?)?;
        }
        let live = engine.diagnostics()[0].live.clone();
        assert_eq!((live.intents, live.admitted, live.rejected), (3, 2, 1));
        assert_eq!(live.rejection_reasons, vec![DiagnosticCount { reason: MultiRejection::SymbolSlots, count: 1 }]);

        let mut gated = portfolio()?;
        for (index, prices) in setup(150).into_iter().enumerate() {
            gated.process_batch(&batch(i64::try_from(index)? - 360, prices, NEUTRAL)?)?;
        }
        let live = gated.diagnostics()[0].live.clone();
        assert_eq!((live.intents, live.admitted, live.rejected), (0, 0, 0));
        assert!(live.discards.contains(&DiagnosticCount { reason: SetupDiscardReason::EntryTouchedOutsideKillzone, count: 1 }));
        assert!(!gated.snapshot().halted);
        Ok(())
    }

    #[test]
    fn partial_failed_batch_does_not_commit_live_counters() -> TestResult {
        let mut engine = portfolio()?;
        engine.process_batch(&batch(0, NEUTRAL, NEUTRAL)?)?;
        let before: Vec<_> = engine.diagnostics().into_iter().map(|row| row.live).collect();
        // Simulate an internal ETH detector chronology disagreement, after the
        // BTC branch can process its next bar. The engine must fail closed.
        engine.state_mut("ETHUSDT")?.ict.process_candle(&candle("ETHUSDT", 1, NEUTRAL)?)?;
        assert!(engine.process_batch(&batch(1, NEUTRAL, NEUTRAL)?).is_err());
        assert!(engine.is_poisoned());
        let after: Vec<_> = engine.diagnostics().into_iter().map(|row| row.live).collect();
        assert_eq!(before, after);
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
        assert_eq!(reason.map(|rejected| rejected.reason), Some(MultiRejection::DuplicateSetup));
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
