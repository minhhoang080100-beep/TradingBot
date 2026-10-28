//! Deterministic ICT/Smart-Money-Concept pattern detection.
//!
//! The engine consumes final candles only. Every price calculation uses
//! [`Decimal`], and every series is isolated by exchange, instrument, and
//! timeframe. Primitive detection remains independent from entry timing so a
//! strategy can retain market context while using [`KillzoneFilter`] to gate
//! actual orders.

use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
};

use chrono::{DateTime, Datelike, Timelike, Utc, Weekday};
use chrono_tz::America::New_York;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    candle_store::CandleKey,
    models::{
        BreakBasis, BreakOfStructure, BreakOfStructureInput, Candle, Direction, Exchange,
        FairValueGap, FairValueGapInput, FractalModel, InstrumentId, LiquiditySide, LiquiditySweep,
        LiquiditySweepInput, MarketKind, MarketStructureShift, MarketStructureShiftInput,
        ModelError, PriceZone, SignalEvent, SwingKind, SwingPoint, Timeframe, ZoneState,
    },
};

const BASIS_POINTS: u32 = 10_000;
const ICT_ALGORITHM_VERSION: &str = "ict-v1";
const ICT_EVENT_NAMESPACE: Uuid = Uuid::from_u128(0x51f3_9ba1_9aae_4b59_a501_7f31_f87f_c71d);

/// ICT trading session expressed in New York wall-clock time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Killzone {
    /// London session window `[02:00, 05:00)` New York time.
    London,
    /// New York session window `[07:00, 10:00)` New York time.
    NewYork,
}

/// Compact validated set of weekdays used by killzone schedules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TradingDays {
    bits: u8,
}

impl TradingDays {
    /// Monday through Friday.
    pub const WEEKDAYS: Self = Self { bits: 0b0001_1111 };
    /// Every day, useful for explicitly configured crypto-weekend trading.
    pub const ALL: Self = Self { bits: 0b0111_1111 };
    /// No enabled trading day.
    pub const NONE: Self = Self { bits: 0 };

    /// Creates a set from Chrono weekday values.
    #[must_use]
    pub fn from_days(days: &[Weekday]) -> Self {
        let mut bits = 0_u8;
        for day in days {
            bits |= weekday_bit(*day);
        }
        Self { bits }
    }

    /// Returns whether a localized weekday is enabled.
    #[must_use]
    pub const fn contains(self, day: Weekday) -> bool {
        self.bits & weekday_bit(day) != 0
    }
}

const fn weekday_bit(day: Weekday) -> u8 {
    match day {
        Weekday::Mon => 1 << 0,
        Weekday::Tue => 1 << 1,
        Weekday::Wed => 1 << 2,
        Weekday::Thu => 1 << 3,
        Weekday::Fri => 1 << 4,
        Weekday::Sat => 1 << 5,
        Weekday::Sun => 1 << 6,
    }
}

/// DST-aware London and New York ICT killzone filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KillzoneFilter {
    london_days: TradingDays,
    new_york_days: TradingDays,
}

impl KillzoneFilter {
    /// Creates a schedule with independent day sets for both sessions.
    #[must_use]
    pub const fn new(london_days: TradingDays, new_york_days: TradingDays) -> Self {
        Self {
            london_days,
            new_york_days,
        }
    }

    /// Creates the conventional Monday-through-Friday schedule.
    #[must_use]
    pub const fn weekdays() -> Self {
        Self::new(TradingDays::WEEKDAYS, TradingDays::WEEKDAYS)
    }

    /// Classifies a UTC instant using `America/New_York`, including EST/EDT.
    ///
    /// Windows are half-open. A signal exactly at 05:00 or 10:00 New York
    /// time is outside its corresponding session.
    #[must_use]
    pub fn classify(self, at: DateTime<Utc>) -> Option<Killzone> {
        let local = at.with_timezone(&New_York);
        let second = local.num_seconds_from_midnight();
        let day = local.weekday();

        if self.london_days.contains(day) && (7_200..18_000).contains(&second) {
            Some(Killzone::London)
        } else if self.new_york_days.contains(day) && (25_200..36_000).contains(&second) {
            Some(Killzone::NewYork)
        } else {
            None
        }
    }

    /// Returns whether the instant belongs to either enabled killzone.
    #[must_use]
    pub fn allows(self, at: DateTime<Utc>) -> bool {
        self.classify(at).is_some()
    }
}

impl Default for KillzoneFilter {
    fn default() -> Self {
        Self::weekdays()
    }
}

/// Price penetration required to complete an FVG lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FvgMitigationRule {
    /// Price must trade to the exact 50% consequent-encroachment level.
    ConsequentEncroachment,
    /// Price must reach the far boundary of the gap.
    FullFill,
}

/// Confirmation used to classify a wick crossing as a liquidity sweep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SweepConfirmation {
    /// Any strict wick crossing consumes and sweeps the level.
    Wick,
    /// The candle must cross by wick and close back on the original side.
    CloseBack,
}

/// Decimal-only displacement thresholds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisplacementConfig {
    body_lookback: NonZeroUsize,
    minimum_body_to_range_bps: u16,
    maximum_close_from_extreme_bps: u16,
    expansion_numerator: u32,
    expansion_denominator: NonZeroUsize,
}

impl DisplacementConfig {
    /// Creates validated displacement thresholds.
    ///
    /// `expansion_numerator / expansion_denominator` is the minimum current
    /// body relative to the mean body over `body_lookback` candles.
    ///
    /// # Errors
    ///
    /// Returns [`IctError::InvalidBasisPoints`] when either basis-point value
    /// exceeds 10,000 or [`IctError::ZeroExpansionNumerator`] when the expansion
    /// numerator is zero.
    pub fn new(
        body_lookback: NonZeroUsize,
        minimum_body_to_range_bps: u16,
        maximum_close_from_extreme_bps: u16,
        expansion_numerator: u32,
        expansion_denominator: NonZeroUsize,
    ) -> Result<Self, IctError> {
        if u32::from(minimum_body_to_range_bps) > BASIS_POINTS {
            return Err(IctError::InvalidBasisPoints {
                field: "minimum_body_to_range_bps",
                value: minimum_body_to_range_bps,
            });
        }
        if u32::from(maximum_close_from_extreme_bps) > BASIS_POINTS {
            return Err(IctError::InvalidBasisPoints {
                field: "maximum_close_from_extreme_bps",
                value: maximum_close_from_extreme_bps,
            });
        }
        if expansion_numerator == 0 {
            return Err(IctError::ZeroExpansionNumerator);
        }

        Ok(Self {
            body_lookback,
            minimum_body_to_range_bps,
            maximum_close_from_extreme_bps,
            expansion_numerator,
            expansion_denominator,
        })
    }

    /// Number of prior candle bodies required for expansion comparison.
    #[must_use]
    pub const fn body_lookback(&self) -> NonZeroUsize {
        self.body_lookback
    }

    /// Minimum candle-body share of the full range, in basis points.
    #[must_use]
    pub const fn minimum_body_to_range_bps(&self) -> u16 {
        self.minimum_body_to_range_bps
    }

    /// Maximum close distance from the directional extreme, in basis points.
    #[must_use]
    pub const fn maximum_close_from_extreme_bps(&self) -> u16 {
        self.maximum_close_from_extreme_bps
    }

    /// Numerator of the body-expansion ratio over the prior mean.
    #[must_use]
    pub const fn expansion_numerator(&self) -> u32 {
        self.expansion_numerator
    }

    /// Denominator of the body-expansion ratio over the prior mean.
    #[must_use]
    pub const fn expansion_denominator(&self) -> NonZeroUsize {
        self.expansion_denominator
    }
}

impl Default for DisplacementConfig {
    fn default() -> Self {
        Self {
            body_lookback: NonZeroUsize::new(20).unwrap_or(NonZeroUsize::MIN),
            minimum_body_to_range_bps: 6_000,
            maximum_close_from_extreme_bps: 2_500,
            expansion_numerator: 3,
            expansion_denominator: NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

/// Per-series memory limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IctTrackingLimits {
    history: NonZeroUsize,
    swings: NonZeroUsize,
    fvgs: NonZeroUsize,
}

impl IctTrackingLimits {
    /// Creates bounded history, swing, and FVG capacities.
    #[must_use]
    pub const fn new(
        history_capacity: NonZeroUsize,
        swing_capacity: NonZeroUsize,
        fvg_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            history: history_capacity,
            swings: swing_capacity,
            fvgs: fvg_capacity,
        }
    }

    /// Maximum number of source candles retained per series.
    #[must_use]
    pub const fn history_capacity(&self) -> NonZeroUsize {
        self.history
    }

    /// Maximum number of swing points retained per series.
    #[must_use]
    pub const fn swing_capacity(&self) -> NonZeroUsize {
        self.swings
    }

    /// Maximum number of FVG lifecycle records retained per series.
    #[must_use]
    pub const fn fvg_capacity(&self) -> NonZeroUsize {
        self.fvgs
    }
}

impl Default for IctTrackingLimits {
    fn default() -> Self {
        Self {
            history: NonZeroUsize::new(128).unwrap_or(NonZeroUsize::MIN),
            swings: NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN),
            fvgs: NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

/// Deterministic detector configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IctEngineConfig {
    fractal_model: FractalModel,
    break_basis: BreakBasis,
    fvg_mitigation: FvgMitigationRule,
    sweep_confirmation: SweepConfirmation,
    limits: IctTrackingLimits,
    displacement: DisplacementConfig,
    killzones: KillzoneFilter,
}

impl IctEngineConfig {
    /// Creates and validates an ICT engine configuration.
    ///
    /// # Errors
    ///
    /// Returns [`IctError::InsufficientHistoryCapacity`] when history cannot
    /// hold the selected fractal window or displacement lookback plus the
    /// current candle.
    pub fn new(
        fractal_model: FractalModel,
        break_basis: BreakBasis,
        fvg_mitigation: FvgMitigationRule,
        sweep_confirmation: SweepConfirmation,
        limits: IctTrackingLimits,
        displacement: DisplacementConfig,
        killzones: KillzoneFilter,
    ) -> Result<Self, IctError> {
        let required = fractal_window_size(fractal_model)
            .max(displacement.body_lookback().get().saturating_add(1));
        if limits.history.get() < required {
            return Err(IctError::InsufficientHistoryCapacity {
                required,
                actual: limits.history.get(),
            });
        }

        Ok(Self {
            fractal_model,
            break_basis,
            fvg_mitigation,
            sweep_confirmation,
            limits,
            displacement,
            killzones,
        })
    }

    /// Selected swing-fractal model.
    #[must_use]
    pub const fn fractal_model(&self) -> FractalModel {
        self.fractal_model
    }

    /// Selected structure-break confirmation basis.
    #[must_use]
    pub const fn break_basis(&self) -> BreakBasis {
        self.break_basis
    }

    /// Selected FVG mitigation threshold.
    #[must_use]
    pub const fn fvg_mitigation(&self) -> FvgMitigationRule {
        self.fvg_mitigation
    }

    /// Selected liquidity-sweep confirmation rule.
    #[must_use]
    pub const fn sweep_confirmation(&self) -> SweepConfirmation {
        self.sweep_confirmation
    }

    /// Per-series bounded-memory limits.
    #[must_use]
    pub const fn limits(&self) -> IctTrackingLimits {
        self.limits
    }

    /// Numeric displacement detector configuration.
    #[must_use]
    pub const fn displacement(&self) -> &DisplacementConfig {
        &self.displacement
    }

    /// Configured session filter.
    #[must_use]
    pub const fn killzones(&self) -> KillzoneFilter {
        self.killzones
    }
}

impl Default for IctEngineConfig {
    fn default() -> Self {
        Self {
            fractal_model: FractalModel::FiveCandle,
            break_basis: BreakBasis::Close,
            fvg_mitigation: FvgMitigationRule::ConsequentEncroachment,
            sweep_confirmation: SweepConfirmation::CloseBack,
            limits: IctTrackingLimits::default(),
            displacement: DisplacementConfig::default(),
            killzones: KillzoneFilter::default(),
        }
    }
}

/// Correlates a detected FVG with its middle candle's displacement result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FvgFormationContext {
    fvg_id: Uuid,
    displacement_candle_open_time: DateTime<Utc>,
    displacement: Option<Direction>,
}

impl FvgFormationContext {
    /// Identifier of the FVG created in the same detection batch.
    #[must_use]
    pub const fn fvg_id(&self) -> Uuid {
        self.fvg_id
    }

    /// Open time of the middle candle in the three-candle FVG pattern.
    #[must_use]
    pub const fn displacement_candle_open_time(&self) -> DateTime<Utc> {
        self.displacement_candle_open_time
    }

    /// Displacement direction of the middle candle, when numerically confirmed.
    #[must_use]
    pub const fn displacement(&self) -> Option<Direction> {
        self.displacement
    }
}

/// Results produced by processing one final candle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IctDetectionBatch {
    signals: Vec<SignalEvent>,
    confirmed_swings: Vec<SwingPoint>,
    fvg_formations: Vec<FvgFormationContext>,
    displacement: Option<Direction>,
    killzone: Option<Killzone>,
    structure_ambiguous: bool,
    series_reset: bool,
    duplicate: bool,
}

impl IctDetectionBatch {
    fn new(killzone: Option<Killzone>) -> Self {
        Self {
            signals: Vec::new(),
            confirmed_swings: Vec::new(),
            fvg_formations: Vec::new(),
            displacement: None,
            killzone,
            structure_ambiguous: false,
            series_reset: false,
            duplicate: false,
        }
    }

    /// Derived events in deterministic processing order.
    #[must_use]
    pub fn signals(&self) -> &[SignalEvent] {
        &self.signals
    }

    /// Newly confirmed swings, ordered high then low for an outside pivot.
    #[must_use]
    pub fn confirmed_swings(&self) -> &[SwingPoint] {
        &self.confirmed_swings
    }

    /// FVG-to-middle-candle displacement correlations created by this batch.
    #[must_use]
    pub fn fvg_formations(&self) -> &[FvgFormationContext] {
        &self.fvg_formations
    }

    /// Direction of a numerically confirmed displacement candle.
    #[must_use]
    pub const fn displacement(&self) -> Option<Direction> {
        self.displacement
    }

    /// Killzone containing the candle close, if any.
    #[must_use]
    pub const fn killzone(&self) -> Option<Killzone> {
        self.killzone
    }

    /// Whether one wick broke both relevant structure directions.
    #[must_use]
    pub const fn structure_ambiguous(&self) -> bool {
        self.structure_ambiguous
    }

    /// Whether a time gap forced this series to discard stale pattern state.
    #[must_use]
    pub const fn series_reset(&self) -> bool {
        self.series_reset
    }

    /// Whether this candle was an exact idempotent replay.
    #[must_use]
    pub const fn is_duplicate(&self) -> bool {
        self.duplicate
    }

    /// Consumes the batch and returns its signals.
    #[must_use]
    pub fn into_signals(self) -> Vec<SignalEvent> {
        self.signals
    }
}

/// Atomic output produced by the asynchronous ICT service.
///
/// A closed candle and its complete analysis travel in one message so the
/// strategy layer cannot observe a structure signal without the corresponding
/// displacement, killzone, swing, duplicate, or series-reset metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IctEngineEvent {
    /// A final candle was processed together with every derived result.
    CandleAnalyzed {
        /// Source candle received from the data layer.
        candle: Candle,
        /// Complete deterministic analysis for the source candle.
        batch: IctDetectionBatch,
    },
    /// One externally supplied candle was rejected without stopping the actor.
    CandleRejected {
        /// Rejected source candle.
        candle: Candle,
        /// Recoverable validation or ordering failure.
        error: IctError,
    },
    /// An exact replay already present in retained history was ignored.
    DuplicateIgnored {
        /// Idempotently ignored source candle.
        candle: Candle,
    },
    /// A non-candle upstream event was not interpreted by this layer.
    Passthrough(SignalEvent),
}

#[derive(Clone, Debug)]
struct TrackedSwing {
    point: SwingPoint,
    liquidity_consumed: bool,
    structure_consumed: bool,
}

#[derive(Debug, Default)]
struct SeriesState {
    history: VecDeque<Candle>,
    swings: VecDeque<TrackedSwing>,
    fvgs: VecDeque<FairValueGap>,
    bias: Option<Direction>,
    protected_high: Option<SwingPoint>,
    protected_low: Option<SwingPoint>,
    last_displacement: Option<(DateTime<Utc>, Direction)>,
}

impl SeriesState {
    fn reset(&mut self) {
        self.history.clear();
        self.swings.clear();
        self.fvgs.clear();
        self.bias = None;
        self.protected_high = None;
        self.protected_low = None;
        self.last_displacement = None;
    }
}

/// Stateful multi-instrument ICT detector.
#[derive(Debug)]
pub struct IctEngine {
    config: IctEngineConfig,
    series: HashMap<CandleKey, SeriesState>,
}

impl IctEngine {
    /// Creates an empty detector with validated configuration.
    #[must_use]
    pub fn new(config: IctEngineConfig) -> Self {
        Self {
            config,
            series: HashMap::new(),
        }
    }

    /// Returns the current directional structure bias for one series.
    #[must_use]
    pub fn bias(&self, key: &CandleKey) -> Option<Direction> {
        self.series.get(key).and_then(|state| state.bias)
    }

    /// Returns a clone of all retained swing points for one series.
    #[must_use]
    pub fn swings(&self, key: &CandleKey) -> Vec<SwingPoint> {
        self.series
            .get(key)
            .map(|state| {
                state
                    .swings
                    .iter()
                    .map(|tracked| tracked.point.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns retained non-terminal FVGs for one series.
    #[must_use]
    pub fn active_fvgs(&self, key: &CandleKey) -> Vec<FairValueGap> {
        self.series
            .get(key)
            .map(|state| {
                state
                    .fvgs
                    .iter()
                    .filter(|gap| !gap.state().is_terminal())
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Processes one final candle without performing I/O.
    ///
    /// An exact replay still present in bounded history is idempotent. A
    /// forward time gap resets only the affected series and warms it up again;
    /// overlapping, stale, conflicting, or partial candles are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`IctError`] when candle ordering, normalization, arithmetic, or
    /// a derived domain model is invalid.
    pub fn process_candle(&mut self, candle: &Candle) -> Result<IctDetectionBatch, IctError> {
        validate_final_candle(candle)?;
        let config = self.config.clone();
        let key = CandleKey::from_candle(candle);
        let mut batch = IctDetectionBatch::new(config.killzones.classify(candle.close_time()));
        let state = self.series.entry(key).or_default();

        if let Some(latest) = state.history.back() {
            if candle.open_time() <= latest.open_time() {
                if let Some(existing) = state
                    .history
                    .iter()
                    .find(|existing| existing.open_time() == candle.open_time())
                {
                    if candle == existing {
                        batch.duplicate = true;
                        return Ok(batch);
                    }
                    return Err(IctError::ConflictingCandle {
                        open_time: candle.open_time(),
                    });
                }
            }
            if candle.open_time() < latest.close_time() {
                return Err(IctError::OutOfOrderCandle {
                    previous_open_time: latest.open_time(),
                    received_open_time: candle.open_time(),
                });
            }
            if candle.open_time() > latest.close_time() {
                state.reset();
                batch.series_reset = true;
            }
        }

        batch.displacement = detect_displacement(candle, &state.history, &config.displacement)?;
        let current_displacement = batch.displacement;
        advance_fvgs(state, candle, config.fvg_mitigation, &mut batch)?;
        detect_liquidity_sweeps(state, candle, config.sweep_confirmation, &mut batch)?;

        state.history.push_back(candle.clone());
        while state.history.len() > config.limits.history.get() {
            let _removed = state.history.pop_front();
        }

        if state.history.len() >= 3 {
            let three = trailing_three(&state.history)?;
            if let Some(gap) = detect_fair_value_gap(&three)? {
                retain_fvg_capacity(state, config.limits.fvgs, candle.close_time(), &mut batch)?;
                let middle_open_time = three[1].open_time();
                let forming_displacement =
                    state
                        .last_displacement
                        .as_ref()
                        .and_then(|(open_time, direction)| {
                            (*open_time == middle_open_time).then_some(*direction)
                        });
                batch.fvg_formations.push(FvgFormationContext {
                    fvg_id: gap.id(),
                    displacement_candle_open_time: middle_open_time,
                    displacement: forming_displacement,
                });
                state.fvgs.push_back(gap.clone());
                batch.signals.push(SignalEvent::FairValueGapDetected(gap));
            }
        }

        let window_size = fractal_window_size(config.fractal_model);
        if state.history.len() >= window_size {
            let window = trailing_window(&state.history, window_size)?;
            let swings = detect_swing_points(&window, config.fractal_model)?;
            for swing in swings {
                register_swing(state, swing.clone(), config.limits.swings);
                batch.confirmed_swings.push(swing);
            }
        }

        detect_structure_break(state, candle, config.break_basis, &mut batch)?;
        state.last_displacement =
            current_displacement.map(|direction| (candle.open_time(), direction));

        Ok(batch)
    }

    /// Runs the ICT layer between bounded Tokio channels.
    ///
    /// Each closed candle and its complete detection batch are emitted as one
    /// atomic [`IctEngineEvent::CandleAnalyzed`] message. Non-candle events are
    /// wrapped unchanged. Cancellation is graceful.
    ///
    /// # Errors
    ///
    /// Recoverable per-candle validation and ordering failures are emitted as
    /// [`IctEngineEvent::CandleRejected`] and do not stop other series. Exact
    /// retained replays become [`IctEngineEvent::DuplicateIgnored`]. Returns
    /// [`IctError`] only for an internal detection failure or when the
    /// downstream receiver closes while the service is active.
    pub async fn run(
        mut self,
        mut input: mpsc::Receiver<SignalEvent>,
        output: mpsc::Sender<IctEngineEvent>,
        shutdown: CancellationToken,
    ) -> Result<(), IctError> {
        loop {
            let event = tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                event = input.recv() => match event {
                    Some(event) => event,
                    None => return Ok(()),
                },
            };

            match event {
                SignalEvent::CandleClosed(candle) => {
                    let output_event = match self.process_candle(&candle) {
                        Ok(batch) if batch.is_duplicate() => {
                            IctEngineEvent::DuplicateIgnored { candle }
                        }
                        Ok(batch) => IctEngineEvent::CandleAnalyzed { candle, batch },
                        Err(error) if error.is_recoverable_input() => {
                            IctEngineEvent::CandleRejected { candle, error }
                        }
                        Err(error) => return Err(error),
                    };
                    if !send_output(&output, output_event, &shutdown).await? {
                        return Ok(());
                    }
                }
                other => {
                    if !send_output(&output, IctEngineEvent::Passthrough(other), &shutdown).await? {
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn send_output(
    output: &mpsc::Sender<IctEngineEvent>,
    event: IctEngineEvent,
    shutdown: &CancellationToken,
) -> Result<bool, IctError> {
    tokio::select! {
        () = shutdown.cancelled() => Ok(false),
        result = output.send(event) => result
            .map(|()| true)
            .map_err(|_source| IctError::OutputChannelClosed),
    }
}

/// Detects strict three- or five-candle fractal swing points.
///
/// # Errors
///
/// Returns [`IctError`] unless the exact required number of final, contiguous
/// candles from one instrument/timeframe is supplied.
pub fn detect_swing_points(
    candles: &[Candle],
    model: FractalModel,
) -> Result<Vec<SwingPoint>, IctError> {
    let expected = fractal_window_size(model);
    validate_candle_window(candles, expected)?;
    let radius = expected / 2;
    let candidate = candles.get(radius).ok_or(IctError::InvalidWindowLength {
        expected,
        actual: candles.len(),
    })?;
    let confirmed_at = candles
        .last()
        .ok_or(IctError::InvalidWindowLength {
            expected,
            actual: candles.len(),
        })?
        .close_time();
    let is_high = candles
        .iter()
        .enumerate()
        .all(|(index, candle)| index == radius || candidate.high() > candle.high());
    let is_low = candles
        .iter()
        .enumerate()
        .all(|(index, candle)| index == radius || candidate.low() < candle.low());
    let mut swings = Vec::with_capacity(2);

    if is_high {
        swings.push(SwingPoint::new(
            candidate.instrument().clone(),
            candidate.timeframe(),
            SwingKind::High,
            candidate.high(),
            candidate.open_time(),
            confirmed_at,
            model,
        )?);
    }
    if is_low {
        swings.push(SwingPoint::new(
            candidate.instrument().clone(),
            candidate.timeframe(),
            SwingKind::Low,
            candidate.low(),
            candidate.open_time(),
            confirmed_at,
            model,
        )?);
    }

    Ok(swings)
}

/// Detects a strict three-candle bullish or bearish fair value gap.
///
/// The middle candle's direction and size are intentionally not part of this
/// pure three-candle primitive. [`IctEngine`] records their correlation in
/// [`IctDetectionBatch::fvg_formations`] so strategy code cannot accidentally
/// use the third candle's displacement.
///
/// # Errors
///
/// Returns [`IctError`] when candles are partial, mixed, or non-contiguous, or
/// when the resulting domain model cannot be constructed.
pub fn detect_fair_value_gap(candles: &[Candle; 3]) -> Result<Option<FairValueGap>, IctError> {
    validate_candle_window(candles.as_slice(), 3)?;
    let left = &candles[0];
    let right = &candles[2];

    let (direction, zone) = if right.low() > left.high() {
        (
            Direction::Bullish,
            PriceZone::new(left.high(), right.low())?,
        )
    } else if right.high() < left.low() {
        (
            Direction::Bearish,
            PriceZone::new(right.high(), left.low())?,
        )
    } else {
        return Ok(None);
    };

    let fvg_qualifier = format!(
        "{}|{}|{}|{}",
        direction_code(direction),
        candles[1].open_time().timestamp_millis(),
        zone.lower().normalize(),
        zone.upper().normalize(),
    );
    let id = stable_pattern_id(
        "fvg",
        left.instrument(),
        left.timeframe(),
        right.open_time(),
        &fvg_qualifier,
    );
    Ok(Some(FairValueGap::new(FairValueGapInput {
        id,
        instrument: left.instrument().clone(),
        timeframe: left.timeframe(),
        direction,
        zone,
        formed_from: [
            candles[0].open_time(),
            candles[1].open_time(),
            candles[2].open_time(),
        ],
        formed_at: right.close_time(),
    })?))
}

fn detect_displacement(
    candle: &Candle,
    history: &VecDeque<Candle>,
    config: &DisplacementConfig,
) -> Result<Option<Direction>, IctError> {
    let lookback = config.body_lookback.get();
    if history.len() < lookback {
        return Ok(None);
    }

    let range = candle.high() - candle.low();
    let body = decimal_abs(candle.close() - candle.open());
    if range <= Decimal::ZERO || body <= Decimal::ZERO {
        return Ok(None);
    }

    let body_scaled = checked_mul(body, Decimal::from(BASIS_POINTS))?;
    let minimum_body = checked_mul(
        range,
        Decimal::from(u32::from(config.minimum_body_to_range_bps)),
    )?;
    if body_scaled < minimum_body {
        return Ok(None);
    }

    let direction = if candle.close() > candle.open() {
        Direction::Bullish
    } else {
        Direction::Bearish
    };
    let distance_from_extreme = match direction {
        Direction::Bullish => candle.high() - candle.close(),
        Direction::Bearish => candle.close() - candle.low(),
    };
    let distance_scaled = checked_mul(distance_from_extreme, Decimal::from(BASIS_POINTS))?;
    let maximum_distance = checked_mul(
        range,
        Decimal::from(u32::from(config.maximum_close_from_extreme_bps)),
    )?;
    if distance_scaled > maximum_distance {
        return Ok(None);
    }

    let previous_body_sum =
        history
            .iter()
            .rev()
            .take(lookback)
            .try_fold(Decimal::ZERO, |sum, previous| {
                sum.checked_add(decimal_abs(previous.close() - previous.open()))
                    .ok_or(IctError::DecimalOverflow)
            })?;
    let current_expansion = checked_mul(
        checked_mul(
            body,
            Decimal::from(u32::try_from(lookback).unwrap_or(u32::MAX)),
        )?,
        Decimal::from(u32::try_from(config.expansion_denominator.get()).unwrap_or(u32::MAX)),
    )?;
    let required_expansion =
        checked_mul(previous_body_sum, Decimal::from(config.expansion_numerator))?;
    if current_expansion < required_expansion {
        return Ok(None);
    }

    Ok(Some(direction))
}

fn advance_fvgs(
    state: &mut SeriesState,
    candle: &Candle,
    rule: FvgMitigationRule,
    batch: &mut IctDetectionBatch,
) -> Result<(), IctError> {
    for gap in &mut state.fvgs {
        if gap.state().is_terminal() || gap.formed_from()[2] >= candle.open_time() {
            continue;
        }

        let overlaps = candle.high() >= gap.zone().lower() && candle.low() <= gap.zone().upper();
        if !overlaps {
            continue;
        }
        let mitigated = match (gap.direction(), rule) {
            (Direction::Bullish, FvgMitigationRule::ConsequentEncroachment) => {
                candle.low() <= gap.consequent_encroachment()
            }
            (Direction::Bearish, FvgMitigationRule::ConsequentEncroachment) => {
                candle.high() >= gap.consequent_encroachment()
            }
            (Direction::Bullish, FvgMitigationRule::FullFill) => candle.low() <= gap.zone().lower(),
            (Direction::Bearish, FvgMitigationRule::FullFill) => {
                candle.high() >= gap.zone().upper()
            }
        };

        if mitigated {
            gap.mark_mitigated(candle.close_time())?;
            batch
                .signals
                .push(SignalEvent::FairValueGapUpdated(gap.clone()));
        } else if matches!(gap.state(), ZoneState::Fresh) {
            gap.mark_touched(candle.close_time())?;
            batch
                .signals
                .push(SignalEvent::FairValueGapUpdated(gap.clone()));
        }
    }
    Ok(())
}

fn detect_liquidity_sweeps(
    state: &mut SeriesState,
    candle: &Candle,
    confirmation: SweepConfirmation,
    batch: &mut IctDetectionBatch,
) -> Result<(), IctError> {
    let mut selected_buy: Option<SwingPoint> = None;
    let mut selected_sell: Option<SwingPoint> = None;

    for tracked in &mut state.swings {
        if tracked.liquidity_consumed || tracked.point.confirmed_at() > candle.open_time() {
            continue;
        }

        let (crossed, eligible) = match tracked.point.kind() {
            SwingKind::High => {
                let crossed = candle.high() > tracked.point.price();
                let eligible = crossed
                    && (matches!(confirmation, SweepConfirmation::Wick)
                        || candle.close() <= tracked.point.price());
                (crossed, eligible)
            }
            SwingKind::Low => {
                let crossed = candle.low() < tracked.point.price();
                let eligible = crossed
                    && (matches!(confirmation, SweepConfirmation::Wick)
                        || candle.close() >= tracked.point.price());
                (crossed, eligible)
            }
        };
        if !crossed {
            continue;
        }
        tracked.liquidity_consumed = true;
        if !eligible {
            continue;
        }

        match tracked.point.kind() {
            SwingKind::High => {
                if selected_buy
                    .as_ref()
                    .is_none_or(|selected| tracked.point.price() > selected.price())
                {
                    selected_buy = Some(tracked.point.clone());
                }
            }
            SwingKind::Low => {
                if selected_sell
                    .as_ref()
                    .is_none_or(|selected| tracked.point.price() < selected.price())
                {
                    selected_sell = Some(tracked.point.clone());
                }
            }
        }
    }

    if let Some(reference) = selected_buy {
        batch.signals.push(SignalEvent::LiquiditySwept(build_sweep(
            candle,
            &reference,
            LiquiditySide::BuySide,
        )?));
    }
    if let Some(reference) = selected_sell {
        batch.signals.push(SignalEvent::LiquiditySwept(build_sweep(
            candle,
            &reference,
            LiquiditySide::SellSide,
        )?));
    }
    Ok(())
}

fn build_sweep(
    candle: &Candle,
    reference: &SwingPoint,
    side: LiquiditySide,
) -> Result<LiquiditySweep, IctError> {
    let sweep_price = match side {
        LiquiditySide::BuySide => candle.high(),
        LiquiditySide::SellSide => candle.low(),
    };
    let sweep_qualifier = format!(
        "{}|{}|{}|{}",
        liquidity_side_code(side),
        reference.candle_open_time().timestamp_millis(),
        reference.price().normalize(),
        fractal_model_code(reference.model()),
    );
    Ok(LiquiditySweep::new(LiquiditySweepInput {
        id: stable_pattern_id(
            "sweep",
            candle.instrument(),
            candle.timeframe(),
            candle.open_time(),
            &sweep_qualifier,
        ),
        instrument: candle.instrument().clone(),
        timeframe: candle.timeframe(),
        side,
        reference_price: reference.price(),
        sweep_price,
        sweep_candle_open_time: candle.open_time(),
        detected_at: candle.close_time(),
    })?)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StructureClassification {
    Bos,
    Mss,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StructureCandidate {
    Unchanged,
    Ambiguous,
    Classified(StructureClassification, Direction, SwingPoint),
}

fn detect_structure_break(
    state: &mut SeriesState,
    candle: &Candle,
    basis: BreakBasis,
    batch: &mut IctDetectionBatch,
) -> Result<(), IctError> {
    let (classification, direction, broken_swing) =
        match select_structure_candidate(state, candle, basis) {
            StructureCandidate::Unchanged => return Ok(()),
            StructureCandidate::Ambiguous => {
                consume_ambiguous_structure_levels(state, candle);
                state.bias = None;
                state.protected_high = None;
                state.protected_low = None;
                batch.structure_ambiguous = true;
                return Ok(());
            }
            StructureCandidate::Classified(classification, direction, broken_swing) => {
                (classification, direction, broken_swing)
            }
        };
    let break_price = match (direction, basis) {
        (Direction::Bullish, BreakBasis::Wick) => candle.high(),
        (Direction::Bearish, BreakBasis::Wick) => candle.low(),
        (_, BreakBasis::Close) => candle.close(),
    };
    consume_crossed_structure_levels(state, direction, break_price);
    let pattern = match classification {
        StructureClassification::Bos => "bos",
        StructureClassification::Mss => "mss",
    };
    let structure_qualifier = format!(
        "{}|{}|{}|{}|{}|{}",
        direction_code(direction),
        break_basis_code(basis),
        swing_kind_code(broken_swing.kind()),
        broken_swing.candle_open_time().timestamp_millis(),
        broken_swing.price().normalize(),
        fractal_model_code(broken_swing.model()),
    );
    let event_id = stable_pattern_id(
        pattern,
        candle.instrument(),
        candle.timeframe(),
        candle.open_time(),
        &structure_qualifier,
    );

    let event = match classification {
        StructureClassification::Bos => {
            let bos = BreakOfStructure::new(BreakOfStructureInput {
                id: event_id,
                instrument: candle.instrument().clone(),
                timeframe: candle.timeframe(),
                direction,
                broken_swing,
                break_price,
                break_basis: basis,
                confirming_candle_open_time: candle.open_time(),
                confirmed_at: candle.close_time(),
            })?;
            SignalEvent::BreakOfStructureDetected(bos)
        }
        StructureClassification::Mss => {
            let shift = MarketStructureShift::new(MarketStructureShiftInput {
                id: event_id,
                instrument: candle.instrument().clone(),
                timeframe: candle.timeframe(),
                direction,
                broken_swing,
                break_price,
                break_basis: basis,
                confirming_candle_open_time: candle.open_time(),
                confirmed_at: candle.close_time(),
            })?;
            SignalEvent::MarketStructureShiftDetected(shift)
        }
    };

    state.bias = Some(direction);
    match direction {
        Direction::Bullish => {
            state.protected_low = latest_swing(state, SwingKind::Low);
        }
        Direction::Bearish => {
            state.protected_high = latest_swing(state, SwingKind::High);
        }
    }
    batch.signals.push(event);
    Ok(())
}

fn select_structure_candidate(
    state: &SeriesState,
    candle: &Candle,
    basis: BreakBasis,
) -> StructureCandidate {
    let latest_high = latest_unconsumed_swing(state, SwingKind::High);
    let latest_low = latest_unconsumed_swing(state, SwingKind::Low);
    let continuation_high = latest_high
        .as_ref()
        .is_some_and(|point| breaks_swing(candle, point, basis));
    let continuation_low = latest_low
        .as_ref()
        .is_some_and(|point| breaks_swing(candle, point, basis));
    let reversal_high = state
        .protected_high
        .as_ref()
        .is_some_and(|point| breaks_swing(candle, point, basis));
    let reversal_low = state
        .protected_low
        .as_ref()
        .is_some_and(|point| breaks_swing(candle, point, basis));

    match state.bias {
        None => {
            if continuation_high && continuation_low && matches!(basis, BreakBasis::Wick) {
                StructureCandidate::Ambiguous
            } else {
                classify_structure_break(choose_initial_break(
                    candle,
                    basis,
                    latest_high,
                    continuation_high,
                    latest_low,
                    continuation_low,
                ))
            }
        }
        Some(Direction::Bullish) => {
            if reversal_low && continuation_high && matches!(basis, BreakBasis::Wick) {
                StructureCandidate::Ambiguous
            } else if reversal_low {
                classify_structure_break(
                    state
                        .protected_low
                        .clone()
                        .map(|point| (StructureClassification::Mss, Direction::Bearish, point)),
                )
            } else if continuation_high {
                classify_structure_break(
                    latest_high
                        .map(|point| (StructureClassification::Bos, Direction::Bullish, point)),
                )
            } else {
                StructureCandidate::Unchanged
            }
        }
        Some(Direction::Bearish) => {
            if reversal_high && continuation_low && matches!(basis, BreakBasis::Wick) {
                StructureCandidate::Ambiguous
            } else if reversal_high {
                classify_structure_break(
                    state
                        .protected_high
                        .clone()
                        .map(|point| (StructureClassification::Mss, Direction::Bullish, point)),
                )
            } else if continuation_low {
                classify_structure_break(
                    latest_low
                        .map(|point| (StructureClassification::Bos, Direction::Bearish, point)),
                )
            } else {
                StructureCandidate::Unchanged
            }
        }
    }
}

fn classify_structure_break(
    candidate: Option<(StructureClassification, Direction, SwingPoint)>,
) -> StructureCandidate {
    candidate.map_or(
        StructureCandidate::Unchanged,
        |(classification, direction, swing)| {
            StructureCandidate::Classified(classification, direction, swing)
        },
    )
}

fn choose_initial_break(
    candle: &Candle,
    basis: BreakBasis,
    high: Option<SwingPoint>,
    high_broken: bool,
    low: Option<SwingPoint>,
    low_broken: bool,
) -> Option<(StructureClassification, Direction, SwingPoint)> {
    if high_broken && low_broken && matches!(basis, BreakBasis::Wick) {
        return None;
    }
    if high_broken && low_broken {
        if candle.close() > candle.open() {
            return high.map(|point| (StructureClassification::Bos, Direction::Bullish, point));
        }
        if candle.close() < candle.open() {
            return low.map(|point| (StructureClassification::Bos, Direction::Bearish, point));
        }
        return None;
    }
    if high_broken {
        high.map(|point| (StructureClassification::Bos, Direction::Bullish, point))
    } else if low_broken {
        low.map(|point| (StructureClassification::Bos, Direction::Bearish, point))
    } else {
        None
    }
}

fn breaks_swing(candle: &Candle, swing: &SwingPoint, basis: BreakBasis) -> bool {
    match (swing.kind(), basis) {
        (SwingKind::High, BreakBasis::Close) => candle.close() > swing.price(),
        (SwingKind::Low, BreakBasis::Close) => candle.close() < swing.price(),
        (SwingKind::High, BreakBasis::Wick) => candle.high() > swing.price(),
        (SwingKind::Low, BreakBasis::Wick) => candle.low() < swing.price(),
    }
}

fn consume_crossed_structure_levels(
    state: &mut SeriesState,
    direction: Direction,
    break_price: Decimal,
) {
    for tracked in &mut state.swings {
        let crossed = match direction {
            Direction::Bullish => {
                tracked.point.kind() == SwingKind::High && break_price > tracked.point.price()
            }
            Direction::Bearish => {
                tracked.point.kind() == SwingKind::Low && break_price < tracked.point.price()
            }
        };
        if crossed {
            tracked.structure_consumed = true;
        }
    }
}

fn consume_ambiguous_structure_levels(state: &mut SeriesState, candle: &Candle) {
    for tracked in &mut state.swings {
        let crossed = match tracked.point.kind() {
            SwingKind::High => candle.high() > tracked.point.price(),
            SwingKind::Low => candle.low() < tracked.point.price(),
        };
        if crossed {
            tracked.structure_consumed = true;
        }
    }
}

fn latest_unconsumed_swing(state: &SeriesState, kind: SwingKind) -> Option<SwingPoint> {
    state
        .swings
        .iter()
        .rev()
        .find(|tracked| tracked.point.kind() == kind && !tracked.structure_consumed)
        .map(|tracked| tracked.point.clone())
}

fn latest_swing(state: &SeriesState, kind: SwingKind) -> Option<SwingPoint> {
    state
        .swings
        .iter()
        .rev()
        .find(|tracked| tracked.point.kind() == kind)
        .map(|tracked| tracked.point.clone())
}

fn register_swing(state: &mut SeriesState, point: SwingPoint, capacity: NonZeroUsize) {
    let exists = state.swings.iter().any(|tracked| {
        tracked.point.kind() == point.kind()
            && tracked.point.candle_open_time() == point.candle_open_time()
            && tracked.point.model() == point.model()
    });
    if exists {
        return;
    }

    state.swings.push_back(TrackedSwing {
        point,
        liquidity_consumed: false,
        structure_consumed: false,
    });
    while state.swings.len() > capacity.get() {
        let _removed = state.swings.pop_front();
    }
}

fn retain_fvg_capacity(
    state: &mut SeriesState,
    capacity: NonZeroUsize,
    invalidated_at: DateTime<Utc>,
    batch: &mut IctDetectionBatch,
) -> Result<(), IctError> {
    while state.fvgs.len() >= capacity.get() {
        if let Some(index) = state.fvgs.iter().position(|gap| gap.state().is_terminal()) {
            let _removed = state.fvgs.remove(index);
        } else if let Some(mut evicted) = state.fvgs.pop_front() {
            evicted.invalidate(invalidated_at, "ICT FVG tracking capacity exceeded")?;
            batch
                .signals
                .push(SignalEvent::FairValueGapUpdated(evicted));
        }
    }
    Ok(())
}

fn trailing_three(history: &VecDeque<Candle>) -> Result<[Candle; 3], IctError> {
    let length = history.len();
    if length < 3 {
        return Err(IctError::InvalidWindowLength {
            expected: 3,
            actual: length,
        });
    }
    Ok([
        history[length - 3].clone(),
        history[length - 2].clone(),
        history[length - 1].clone(),
    ])
}

fn trailing_window(history: &VecDeque<Candle>, length: usize) -> Result<Vec<Candle>, IctError> {
    if history.len() < length {
        return Err(IctError::InvalidWindowLength {
            expected: length,
            actual: history.len(),
        });
    }
    Ok(history
        .iter()
        .skip(history.len() - length)
        .cloned()
        .collect())
}

fn validate_final_candle(candle: &Candle) -> Result<(), IctError> {
    if !candle.is_closed() {
        return Err(IctError::PartialCandle);
    }
    if candle.close_time() - candle.open_time() != candle.timeframe().duration() {
        return Err(IctError::InvalidCandleDuration {
            timeframe: candle.timeframe(),
            open_time: candle.open_time(),
            close_time: candle.close_time(),
        });
    }
    Ok(())
}

fn validate_candle_window(candles: &[Candle], expected: usize) -> Result<(), IctError> {
    if candles.len() != expected {
        return Err(IctError::InvalidWindowLength {
            expected,
            actual: candles.len(),
        });
    }
    let first = candles.first().ok_or(IctError::InvalidWindowLength {
        expected,
        actual: 0,
    })?;
    validate_final_candle(first)?;
    for candle in candles.iter().skip(1) {
        validate_final_candle(candle)?;
        if candle.instrument() != first.instrument() || candle.timeframe() != first.timeframe() {
            return Err(IctError::MixedCandleSeries);
        }
    }
    for pair in candles.windows(2) {
        if pair[0].close_time() != pair[1].open_time() {
            return Err(IctError::NonContiguousCandles {
                previous_close_time: pair[0].close_time(),
                next_open_time: pair[1].open_time(),
            });
        }
    }
    Ok(())
}

const fn fractal_window_size(model: FractalModel) -> usize {
    match model {
        FractalModel::ThreeCandle => 3,
        FractalModel::FiveCandle => 5,
    }
}

fn decimal_abs(value: Decimal) -> Decimal {
    if value.is_sign_negative() {
        -value
    } else {
        value
    }
}

fn checked_mul(left: Decimal, right: Decimal) -> Result<Decimal, IctError> {
    left.checked_mul(right).ok_or(IctError::DecimalOverflow)
}

fn stable_pattern_id(
    pattern: &str,
    instrument: &InstrumentId,
    timeframe: Timeframe,
    source_time: DateTime<Utc>,
    qualifier: &str,
) -> Uuid {
    let name = format!(
        "{ICT_ALGORITHM_VERSION}|{pattern}|{}|{}|{}|{}|{}|{}|{}|{qualifier}",
        exchange_code(instrument.exchange),
        market_code(instrument.market),
        instrument.venue_symbol,
        instrument.canonical_symbol,
        timeframe.as_str(),
        source_time.timestamp(),
        source_time.timestamp_subsec_nanos(),
    );
    Uuid::new_v5(&ICT_EVENT_NAMESPACE, name.as_bytes())
}

const fn exchange_code(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Binance => "binance",
        Exchange::Bybit => "bybit",
        Exchange::Okx => "okx",
    }
}

const fn market_code(market: MarketKind) -> &'static str {
    match market {
        MarketKind::LinearPerpetual => "linear_perpetual",
        MarketKind::InversePerpetual => "inverse_perpetual",
        MarketKind::DatedFuture => "dated_future",
    }
}

const fn direction_code(direction: Direction) -> &'static str {
    match direction {
        Direction::Bullish => "bullish",
        Direction::Bearish => "bearish",
    }
}

const fn break_basis_code(basis: BreakBasis) -> &'static str {
    match basis {
        BreakBasis::Wick => "wick",
        BreakBasis::Close => "close",
    }
}

const fn swing_kind_code(kind: SwingKind) -> &'static str {
    match kind {
        SwingKind::High => "high",
        SwingKind::Low => "low",
    }
}

const fn liquidity_side_code(side: LiquiditySide) -> &'static str {
    match side {
        LiquiditySide::BuySide => "bsl",
        LiquiditySide::SellSide => "ssl",
    }
}

const fn fractal_model_code(model: FractalModel) -> &'static str {
    match model {
        FractalModel::ThreeCandle => "three_candle",
        FractalModel::FiveCandle => "five_candle",
    }
}

/// ICT detector or pipeline failure.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IctError {
    /// A detector received a candle that was not final.
    #[error("ICT detection requires final candles")]
    PartialCandle,
    /// Candle duration disagreed with its timeframe.
    #[error(
        "{timeframe} candle duration is invalid: {open_time} must close at the exact interval before {close_time}"
    )]
    InvalidCandleDuration {
        /// Declared timeframe.
        timeframe: Timeframe,
        /// Candle open time.
        open_time: DateTime<Utc>,
        /// Candle close time.
        close_time: DateTime<Utc>,
    },
    /// A pure detector received the wrong window size.
    #[error("detector expected {expected} candles but received {actual}")]
    InvalidWindowLength {
        /// Required candle count.
        expected: usize,
        /// Supplied candle count.
        actual: usize,
    },
    /// Candles in one primitive window belonged to different series.
    #[error("detector window mixed instruments or timeframes")]
    MixedCandleSeries,
    /// Candles in one primitive window contained a time gap.
    #[error(
        "non-contiguous candles: previous close {previous_close_time}, next open {next_open_time}"
    )]
    NonContiguousCandles {
        /// Expected next open time.
        previous_close_time: DateTime<Utc>,
        /// Actual next open time.
        next_open_time: DateTime<Utc>,
    },
    /// A stale or overlapping candle arrived after a newer one.
    #[error(
        "out-of-order candle: previous open {previous_open_time}, received {received_open_time}"
    )]
    OutOfOrderCandle {
        /// Newest accepted open time.
        previous_open_time: DateTime<Utc>,
        /// Rejected candle open time.
        received_open_time: DateTime<Utc>,
    },
    /// A replay reused an open time with different content.
    #[error("conflicting candle replay at {open_time}")]
    ConflictingCandle {
        /// Duplicated candle open time.
        open_time: DateTime<Utc>,
    },
    /// History was too small for configured algorithms.
    #[error("history capacity {actual} is below required minimum {required}")]
    InsufficientHistoryCapacity {
        /// Minimum required capacity.
        required: usize,
        /// Configured capacity.
        actual: usize,
    },
    /// A basis-point threshold exceeded 100%.
    #[error("{field} must be at most 10000 basis points; received {value}")]
    InvalidBasisPoints {
        /// Invalid configuration field.
        field: &'static str,
        /// Invalid value.
        value: u16,
    },
    /// Expansion ratio used a zero numerator.
    #[error("displacement expansion numerator must be greater than zero")]
    ZeroExpansionNumerator,
    /// Exact Decimal arithmetic exceeded representable range.
    #[error("Decimal arithmetic overflow during ICT detection")]
    DecimalOverflow,
    /// Downstream processing was dropped while the engine was active.
    #[error("ICT output channel is closed")]
    OutputChannelClosed,
    /// A derived domain model rejected its inputs.
    #[error(transparent)]
    Model(#[from] ModelError),
}

impl IctError {
    /// Returns whether an actor can reject only the offending input and safely
    /// continue processing this and other series.
    #[must_use]
    pub const fn is_recoverable_input(&self) -> bool {
        matches!(
            self,
            Self::PartialCandle
                | Self::InvalidCandleDuration { .. }
                | Self::OutOfOrderCandle { .. }
                | Self::ConflictingCandle { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io, str::FromStr};

    use chrono::DateTime;

    use super::*;
    use crate::models::{CandleInput, MarketKind, VolumeUnit};

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    fn non_zero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
    }

    fn at_minute(minute: i64) -> Result<DateTime<Utc>, io::Error> {
        DateTime::from_timestamp(minute * 60, 0)
            .ok_or_else(|| io::Error::other("invalid test timestamp"))
    }

    fn utc(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
        Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
    }

    fn instrument() -> Result<InstrumentId, ModelError> {
        InstrumentId::new(
            Exchange::Binance,
            MarketKind::LinearPerpetual,
            "BTCUSDT",
            "BTC-USDT-PERP",
        )
    }

    fn candle(
        minute: i64,
        open: &str,
        high: &str,
        low: &str,
        close: &str,
    ) -> Result<Candle, Box<dyn Error + Send + Sync>> {
        candle_for(instrument()?, minute, open, high, low, close, true)
    }

    fn candle_for(
        instrument: InstrumentId,
        minute: i64,
        open: &str,
        high: &str,
        low: &str,
        close: &str,
        is_closed: bool,
    ) -> Result<Candle, Box<dyn Error + Send + Sync>> {
        Ok(Candle::new(CandleInput {
            instrument,
            timeframe: Timeframe::M1,
            open_time: at_minute(minute)?,
            close_time: at_minute(minute + 1)?,
            open: Decimal::from_str(open)?,
            high: Decimal::from_str(high)?,
            low: Decimal::from_str(low)?,
            close: Decimal::from_str(close)?,
            volume: Decimal::ONE,
            volume_unit: VolumeUnit::BaseAsset,
            trade_count: None,
            is_closed,
        })?)
    }

    fn three_candle_config() -> Result<IctEngineConfig, IctError> {
        IctEngineConfig::new(
            FractalModel::ThreeCandle,
            BreakBasis::Close,
            FvgMitigationRule::ConsequentEncroachment,
            SweepConfirmation::CloseBack,
            IctTrackingLimits::new(non_zero(32), non_zero(16), non_zero(16)),
            DisplacementConfig::new(non_zero(2), 6_000, 2_500, 3, non_zero(2))?,
            KillzoneFilter::new(TradingDays::ALL, TradingDays::ALL),
        )
    }

    fn bullish_fvg_candles() -> Result<[Candle; 3], Box<dyn Error + Send + Sync>> {
        Ok([
            candle(0, "100", "101", "99", "100")?,
            candle(1, "100", "104", "100", "103")?,
            candle(2, "103", "106", "102", "105")?,
        ])
    }

    #[test]
    fn detects_strict_three_candle_outside_swing() -> TestResult {
        let candles = vec![
            candle(0, "100", "102", "98", "101")?,
            candle(1, "101", "105", "95", "100")?,
            candle(2, "100", "103", "97", "102")?,
        ];
        let swings = detect_swing_points(&candles, FractalModel::ThreeCandle)?;

        assert_eq!(swings.len(), 2);
        assert_eq!(swings[0].kind(), SwingKind::High);
        assert_eq!(swings[0].price(), Decimal::from(105_u32));
        assert_eq!(swings[1].kind(), SwingKind::Low);
        assert_eq!(swings[1].price(), Decimal::from(95_u32));
        assert_eq!(swings[0].confirmed_at(), at_minute(3)?);
        Ok(())
    }

    #[test]
    fn equal_high_plateau_is_not_a_swing_high() -> TestResult {
        let candles = vec![
            candle(0, "100", "105", "98", "101")?,
            candle(1, "101", "105", "97", "100")?,
            candle(2, "100", "103", "99", "102")?,
        ];
        let swings = detect_swing_points(&candles, FractalModel::ThreeCandle)?;

        assert!(swings.iter().all(|swing| swing.kind() != SwingKind::High));
        Ok(())
    }

    #[test]
    fn detects_five_candle_swing() -> TestResult {
        let candles = vec![
            candle(0, "100", "102", "98", "101")?,
            candle(1, "101", "103", "97", "102")?,
            candle(2, "102", "110", "90", "100")?,
            candle(3, "100", "104", "96", "101")?,
            candle(4, "101", "105", "95", "102")?,
        ];
        let swings = detect_swing_points(&candles, FractalModel::FiveCandle)?;

        assert_eq!(swings.len(), 2);
        assert_eq!(swings[0].price(), Decimal::from(110_u32));
        assert_eq!(swings[1].price(), Decimal::from(90_u32));
        Ok(())
    }

    #[test]
    fn detects_bullish_and_bearish_fvg_with_exact_decimal_boundaries() -> TestResult {
        let bullish = detect_fair_value_gap(&bullish_fvg_candles()?)?
            .ok_or_else(|| io::Error::other("expected bullish FVG"))?;
        assert_eq!(bullish.direction(), Direction::Bullish);
        assert_eq!(bullish.zone().lower(), Decimal::from(101_u32));
        assert_eq!(bullish.zone().upper(), Decimal::from(102_u32));
        assert_eq!(bullish.consequent_encroachment(), Decimal::new(1_015, 1));
        assert_eq!(bullish.formed_at(), at_minute(3)?);

        let bearish_candles = [
            candle(10, "105", "106", "104", "105")?,
            candle(11, "105", "105", "100", "101")?,
            candle(12, "101", "103", "98", "99")?,
        ];
        let bearish = detect_fair_value_gap(&bearish_candles)?
            .ok_or_else(|| io::Error::other("expected bearish FVG"))?;
        assert_eq!(bearish.direction(), Direction::Bearish);
        assert_eq!(bearish.zone().lower(), Decimal::from(103_u32));
        assert_eq!(bearish.zone().upper(), Decimal::from(104_u32));
        Ok(())
    }

    #[test]
    fn equality_and_non_contiguous_candles_do_not_form_fvg() -> TestResult {
        let equality = [
            candle(0, "100", "101", "99", "100")?,
            candle(1, "100", "103", "100", "102")?,
            candle(2, "102", "104", "101", "103")?,
        ];
        assert!(detect_fair_value_gap(&equality)?.is_none());

        let gap = [
            candle(0, "100", "101", "99", "100")?,
            candle(2, "100", "104", "100", "103")?,
            candle(3, "103", "106", "102", "105")?,
        ];
        assert!(matches!(
            detect_fair_value_gap(&gap),
            Err(IctError::NonContiguousCandles { .. })
        ));
        Ok(())
    }

    #[test]
    fn fvg_transitions_touch_then_consequent_encroachment_once() -> TestResult {
        let mut engine = IctEngine::new(three_candle_config()?);
        for source in bullish_fvg_candles()? {
            let batch = engine.process_candle(&source)?;
            if source.open_time() == at_minute(2)? {
                assert!(matches!(
                    batch.signals(),
                    [SignalEvent::FairValueGapDetected(_)]
                ));
            }
        }

        let touched = engine.process_candle(&candle(3, "102.5", "103", "101.75", "102")?)?;
        assert!(matches!(
            touched.signals(),
            [SignalEvent::FairValueGapUpdated(gap)]
                if matches!(gap.state(), ZoneState::Touched { .. })
        ));

        let mitigated = engine.process_candle(&candle(4, "102", "102.5", "101.5", "102")?)?;
        assert!(mitigated.signals().iter().any(|event| matches!(
            event,
            SignalEvent::FairValueGapUpdated(gap) if gap.is_mitigated()
        )));
        let terminal = engine.process_candle(&candle(5, "102", "103", "101", "102")?)?;
        assert!(!terminal
            .signals()
            .iter()
            .any(|event| matches!(event, SignalEvent::FairValueGapUpdated(_))));
        Ok(())
    }

    #[test]
    fn close_back_liquidity_sweep_emits_once() -> TestResult {
        let mut engine = IctEngine::new(three_candle_config()?);
        let warmup = [
            candle(0, "100", "102", "98", "101")?,
            candle(1, "101", "105", "99", "100")?,
            candle(2, "100", "103", "100", "102")?,
        ];
        for value in warmup {
            let _batch = engine.process_candle(&value)?;
        }

        let swept = engine.process_candle(&candle(3, "102", "106", "101", "104.5")?)?;
        assert!(swept.signals().iter().any(|event| matches!(
            event,
            SignalEvent::LiquiditySwept(sweep)
                if sweep.side() == LiquiditySide::BuySide
                    && sweep.reference_price() == Decimal::from(105_u32)
                    && sweep.sweep_price() == Decimal::from(106_u32)
        )));

        let repeated = engine.process_candle(&candle(4, "104.5", "107", "102", "104")?)?;
        assert!(!repeated
            .signals()
            .iter()
            .any(|event| matches!(event, SignalEvent::LiquiditySwept(_))));
        Ok(())
    }

    #[test]
    fn first_break_is_bos_then_protected_low_break_is_mss() -> TestResult {
        let mut engine = IctEngine::new(three_candle_config()?);
        let warmup = [
            candle(0, "100", "102", "98", "101")?,
            candle(1, "101", "105", "95", "100")?,
            candle(2, "100", "103", "97", "102")?,
        ];
        for value in warmup {
            let _batch = engine.process_candle(&value)?;
        }
        let key = CandleKey::from_candle(&candle(2, "100", "103", "97", "102")?);

        let bullish = engine.process_candle(&candle(3, "102", "107", "100", "106")?)?;
        assert!(bullish.signals().iter().any(|event| matches!(
            event,
            SignalEvent::BreakOfStructureDetected(bos)
                if bos.direction() == Direction::Bullish
        )));
        assert_eq!(engine.bias(&key), Some(Direction::Bullish));

        let bearish = engine.process_candle(&candle(4, "106", "107", "93", "94")?)?;
        assert!(bearish.signals().iter().any(|event| matches!(
            event,
            SignalEvent::MarketStructureShiftDetected(mss)
                if mss.direction() == Direction::Bearish
        )));
        assert_eq!(engine.bias(&key), Some(Direction::Bearish));
        Ok(())
    }

    #[test]
    fn unconfirmed_continuation_does_not_move_the_protected_low() -> TestResult {
        let mut engine = IctEngine::new(three_candle_config()?);
        for value in [
            candle(0, "100", "102", "98", "101")?,
            candle(1, "101", "105", "95", "100")?,
            candle(2, "100", "103", "97", "102")?,
            candle(3, "102", "107", "100", "106")?,
            candle(4, "106", "106", "97", "100")?,
            candle(5, "100", "101", "90", "96")?,
            candle(6, "96", "100", "96", "98")?,
        ] {
            let _batch = engine.process_candle(&value)?;
        }

        let reversal = engine.process_candle(&candle(7, "98", "99", "93", "94")?)?;
        assert!(reversal.signals().iter().any(|event| matches!(
            event,
            SignalEvent::MarketStructureShiftDetected(mss)
                if mss.direction() == Direction::Bearish
                    && mss.broken_swing().price() == Decimal::from(95_u32)
        )));
        Ok(())
    }

    #[test]
    fn continuation_bos_protects_a_swing_confirmed_by_the_same_candle() -> TestResult {
        let mut engine = IctEngine::new(three_candle_config()?);
        for value in [
            candle(0, "100", "102", "98", "101")?,
            candle(1, "101", "105", "95", "100")?,
            candle(2, "100", "103", "97", "102")?,
            candle(3, "102", "110", "100", "106")?,
            candle(4, "106", "107", "100", "104")?,
            candle(5, "104", "106", "98", "103")?,
        ] {
            let _batch = engine.process_candle(&value)?;
        }

        let continuation = engine.process_candle(&candle(6, "103", "112", "99", "111")?)?;
        assert!(continuation.signals().iter().any(|event| matches!(
            event,
            SignalEvent::BreakOfStructureDetected(bos)
                if bos.direction() == Direction::Bullish
        )));

        let reversal = engine.process_candle(&candle(7, "111", "111", "96", "97")?)?;
        assert!(reversal.signals().iter().any(|event| matches!(
            event,
            SignalEvent::MarketStructureShiftDetected(mss)
                if mss.direction() == Direction::Bearish
                    && mss.broken_swing().price() == Decimal::from(98_u32)
        )));
        Ok(())
    }

    #[test]
    fn dual_wick_break_is_explicitly_ambiguous_and_consumes_levels() -> TestResult {
        let config = IctEngineConfig::new(
            FractalModel::ThreeCandle,
            BreakBasis::Wick,
            FvgMitigationRule::ConsequentEncroachment,
            SweepConfirmation::CloseBack,
            IctTrackingLimits::new(non_zero(32), non_zero(16), non_zero(16)),
            DisplacementConfig::new(non_zero(2), 6_000, 2_500, 3, non_zero(2))?,
            KillzoneFilter::new(TradingDays::ALL, TradingDays::ALL),
        )?;
        let mut engine = IctEngine::new(config);
        for value in [
            candle(0, "100", "102", "98", "101")?,
            candle(1, "101", "105", "95", "100")?,
            candle(2, "100", "103", "97", "102")?,
        ] {
            let _batch = engine.process_candle(&value)?;
        }
        let key = CandleKey::from_candle(&candle(2, "100", "103", "97", "102")?);

        let ambiguous = engine.process_candle(&candle(3, "102", "106", "94", "100")?)?;
        assert!(ambiguous.structure_ambiguous());
        assert_eq!(engine.bias(&key), None);
        assert!(!ambiguous.signals().iter().any(|event| matches!(
            event,
            SignalEvent::BreakOfStructureDetected(_) | SignalEvent::MarketStructureShiftDetected(_)
        )));

        let later = engine.process_candle(&candle(4, "100", "107", "93", "101")?)?;
        assert!(!later.signals().iter().any(|event| matches!(
            event,
            SignalEvent::BreakOfStructureDetected(_) | SignalEvent::MarketStructureShiftDetected(_)
        )));
        Ok(())
    }

    #[test]
    fn displacement_uses_body_range_location_and_prior_mean() -> TestResult {
        let mut engine = IctEngine::new(three_candle_config()?);
        let first = engine.process_candle(&candle(0, "100", "102", "99", "101")?)?;
        let second = engine.process_candle(&candle(1, "101", "103", "100", "102")?)?;
        let displacement = engine.process_candle(&candle(2, "102", "106", "102", "105")?)?;

        assert_eq!(first.displacement(), None);
        assert_eq!(second.displacement(), None);
        assert_eq!(displacement.displacement(), Some(Direction::Bullish));
        Ok(())
    }

    #[test]
    fn fvg_context_uses_the_middle_candles_displacement() -> TestResult {
        let mut engine = IctEngine::new(three_candle_config()?);
        for value in [
            candle(0, "100", "102", "99", "101")?,
            candle(1, "101", "102", "100", "101")?,
            candle(2, "101", "106", "101", "105")?,
        ] {
            let _batch = engine.process_candle(&value)?;
        }

        let fvg_batch = engine.process_candle(&candle(3, "105", "107", "103", "104")?)?;
        assert_eq!(fvg_batch.displacement(), None);
        assert!(matches!(
            fvg_batch.fvg_formations(),
            [context]
                if context.displacement_candle_open_time() == at_minute(2)?
                    && context.displacement() == Some(Direction::Bullish)
        ));
        Ok(())
    }

    #[test]
    fn active_fvg_capacity_eviction_emits_invalidation() -> TestResult {
        let config = IctEngineConfig::new(
            FractalModel::ThreeCandle,
            BreakBasis::Close,
            FvgMitigationRule::ConsequentEncroachment,
            SweepConfirmation::CloseBack,
            IctTrackingLimits::new(non_zero(32), non_zero(16), non_zero(1)),
            DisplacementConfig::new(non_zero(2), 6_000, 2_500, 3, non_zero(2))?,
            KillzoneFilter::new(TradingDays::ALL, TradingDays::ALL),
        )?;
        let mut engine = IctEngine::new(config);
        for value in [
            candle(0, "100", "101", "99", "100")?,
            candle(1, "100", "104", "100", "103")?,
            candle(2, "103", "106", "102", "105")?,
        ] {
            let _batch = engine.process_candle(&value)?;
        }

        let replacement = engine.process_candle(&candle(3, "105", "108", "105", "107")?)?;
        assert!(matches!(
            replacement.signals(),
            [SignalEvent::FairValueGapUpdated(evicted), SignalEvent::FairValueGapDetected(_)]
                if matches!(evicted.state(), ZoneState::Invalidated { .. })
        ));
        Ok(())
    }

    #[test]
    fn duplicate_is_idempotent_and_gap_resets_only_the_series() -> TestResult {
        let mut engine = IctEngine::new(three_candle_config()?);
        let first = candle(0, "100", "102", "99", "101")?;
        let _initial = engine.process_candle(&first)?;
        let duplicate = engine.process_candle(&first)?;
        assert!(duplicate.is_duplicate());
        assert!(duplicate.signals().is_empty());

        let _next = engine.process_candle(&candle(1, "101", "103", "100", "102")?)?;
        let retained_replay = engine.process_candle(&first)?;
        assert!(retained_replay.is_duplicate());

        let reset = engine.process_candle(&candle(3, "102", "104", "101", "103")?)?;
        assert!(reset.series_reset());
        assert!(reset.signals().is_empty());
        Ok(())
    }

    #[test]
    fn deterministic_fvg_id_survives_replay_in_a_fresh_engine() -> TestResult {
        let candles = bullish_fvg_candles()?;
        let mut first_engine = IctEngine::new(three_candle_config()?);
        let mut second_engine = IctEngine::new(three_candle_config()?);
        let mut first_id = None;
        let mut second_id = None;

        for value in &candles {
            for event in first_engine.process_candle(value)?.signals() {
                if let SignalEvent::FairValueGapDetected(gap) = event {
                    first_id = Some(gap.id());
                }
            }
            for event in second_engine.process_candle(value)?.signals() {
                if let SignalEvent::FairValueGapDetected(gap) = event {
                    second_id = Some(gap.id());
                }
            }
        }
        assert!(first_id.is_some());
        assert_eq!(first_id, second_id);
        Ok(())
    }

    #[test]
    fn killzones_follow_new_york_dst_and_half_open_boundaries() -> TestResult {
        let all_days = KillzoneFilter::new(TradingDays::ALL, TradingDays::ALL);
        assert_eq!(
            all_days.classify(utc("2026-01-15T07:00:00Z")?),
            Some(Killzone::London)
        );
        assert_eq!(all_days.classify(utc("2026-01-15T10:00:00Z")?), None);
        assert_eq!(
            all_days.classify(utc("2026-07-15T11:00:00Z")?),
            Some(Killzone::NewYork)
        );
        assert_eq!(all_days.classify(utc("2026-07-15T14:00:00Z")?), None);

        assert_eq!(all_days.classify(utc("2026-03-08T06:59:59Z")?), None);
        assert_eq!(
            all_days.classify(utc("2026-03-08T07:00:00Z")?),
            Some(Killzone::London)
        );
        assert_eq!(
            all_days.classify(utc("2026-11-01T07:00:00Z")?),
            Some(Killzone::London)
        );
        assert_eq!(all_days.classify(utc("2026-11-01T10:00:00Z")?), None);
        Ok(())
    }

    #[test]
    fn weekday_schedule_rejects_crypto_weekend_until_explicitly_enabled() -> TestResult {
        let saturday = utc("2026-01-17T07:30:00Z")?;
        assert!(!KillzoneFilter::default().allows(saturday));
        assert!(KillzoneFilter::new(TradingDays::ALL, TradingDays::ALL).allows(saturday));
        Ok(())
    }

    #[tokio::test]
    async fn async_pipeline_emits_atomic_candle_analysis() -> TestResult {
        let engine = IctEngine::new(three_candle_config()?);
        let shutdown = CancellationToken::new();
        let (input_tx, input_rx) = mpsc::channel(8);
        let (output_tx, mut output_rx) = mpsc::channel(8);
        let task = tokio::spawn(engine.run(input_rx, output_tx, shutdown));

        for value in bullish_fvg_candles()? {
            input_tx.send(SignalEvent::CandleClosed(value)).await?;
        }
        drop(input_tx);

        let mut candle_count = 0_usize;
        let mut fvg_count = 0_usize;
        while let Some(event) = output_rx.recv().await {
            if let IctEngineEvent::CandleAnalyzed { batch, .. } = event {
                candle_count += 1;
                fvg_count += batch
                    .signals()
                    .iter()
                    .filter(|signal| matches!(signal, SignalEvent::FairValueGapDetected(_)))
                    .count();
            }
        }
        task.await??;

        assert_eq!(candle_count, 3);
        assert_eq!(fvg_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn async_pipeline_rejects_one_bad_candle_and_keeps_running() -> TestResult {
        let engine = IctEngine::new(three_candle_config()?);
        let shutdown = CancellationToken::new();
        let (input_tx, input_rx) = mpsc::channel(8);
        let (output_tx, mut output_rx) = mpsc::channel(8);
        let task = tokio::spawn(engine.run(input_rx, output_tx, shutdown));

        input_tx
            .send(SignalEvent::CandleClosed(candle(
                0, "100", "102", "99", "101",
            )?))
            .await?;
        input_tx
            .send(SignalEvent::CandleClosed(candle(
                0, "100", "102", "99", "101",
            )?))
            .await?;
        input_tx
            .send(SignalEvent::CandleClosed(candle(
                0, "100", "103", "99", "102",
            )?))
            .await?;
        input_tx
            .send(SignalEvent::CandleClosed(candle(
                1, "101", "103", "100", "102",
            )?))
            .await?;
        drop(input_tx);

        let mut analyzed = 0_usize;
        let mut rejected = 0_usize;
        let mut ignored = 0_usize;
        while let Some(event) = output_rx.recv().await {
            match event {
                IctEngineEvent::CandleAnalyzed { .. } => analyzed += 1,
                IctEngineEvent::CandleRejected {
                    error: IctError::ConflictingCandle { .. },
                    ..
                } => rejected += 1,
                IctEngineEvent::DuplicateIgnored { .. } => ignored += 1,
                _ => {}
            }
        }
        task.await??;

        assert_eq!(analyzed, 2);
        assert_eq!(rejected, 1);
        assert_eq!(ignored, 1);
        Ok(())
    }

    #[test]
    fn rejects_partial_and_mixed_series_windows() -> TestResult {
        let partial = candle_for(instrument()?, 0, "100", "102", "99", "101", false)?;
        let mut engine = IctEngine::new(three_candle_config()?);
        assert_eq!(
            engine.process_candle(&partial),
            Err(IctError::PartialCandle)
        );

        let bybit = InstrumentId::new(
            Exchange::Bybit,
            MarketKind::LinearPerpetual,
            "BTCUSDT",
            "BTC-USDT-PERP",
        )?;
        let mixed = [
            candle(0, "100", "101", "99", "100")?,
            candle_for(bybit, 1, "100", "104", "100", "103", true)?,
            candle(2, "103", "106", "102", "105")?,
        ];
        assert_eq!(
            detect_fair_value_gap(&mixed),
            Err(IctError::MixedCandleSeries)
        );
        Ok(())
    }
}
