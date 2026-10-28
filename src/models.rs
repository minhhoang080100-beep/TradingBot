//! Validated, exchange-neutral domain models.
//!
//! Prices and quantities always use [`Decimal`]. Venue adapters must parse
//! exchange strings directly into `Decimal`; converting through binary floating
//! point is never acceptable because it can silently change an order's price or
//! quantity.

use std::{fmt, str::FromStr};

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{
    de::{self, Deserializer},
    Deserialize, Serialize, Serializer,
};
use uuid::Uuid;

/// Supported centralized exchanges.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    /// Binance Futures.
    Binance,
    /// Bybit derivatives.
    Bybit,
    /// OKX derivatives.
    Okx,
}

/// The contract family used by an instrument.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    /// Stablecoin- or fiat-margined perpetual contract.
    LinearPerpetual,
    /// Coin-margined perpetual contract.
    InversePerpetual,
    /// Future with a fixed expiry date.
    DatedFuture,
}

/// A validated exchange or canonical symbol code.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct SymbolCode(String);

impl SymbolCode {
    /// Builds a symbol without changing its exchange-specific spelling.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidSymbol`] for an empty value or surrounding
    /// whitespace.
    pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        if value.is_empty() || value.trim() != value {
            return Err(ModelError::InvalidSymbol(value));
        }

        Ok(Self(value))
    }

    /// Returns the exact stored symbol.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SymbolCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for SymbolCode {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for SymbolCode {
    type Error = ModelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for SymbolCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for SymbolCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Unique instrument identity across all supported exchanges.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub struct InstrumentId {
    /// Venue on which this instrument trades.
    pub exchange: Exchange,
    /// Contract family.
    pub market: MarketKind,
    /// Exact identifier required by the venue, such as `BTCUSDT` or
    /// `BTC-USDT-SWAP`.
    pub venue_symbol: SymbolCode,
    /// Stable cross-venue identifier, such as `BTC-USDT-PERP`.
    pub canonical_symbol: SymbolCode,
}

impl InstrumentId {
    /// Creates an instrument identifier while validating both symbols.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidSymbol`] when either symbol is empty or has
    /// surrounding whitespace.
    pub fn new(
        exchange: Exchange,
        market: MarketKind,
        venue_symbol: impl Into<String>,
        canonical_symbol: impl Into<String>,
    ) -> Result<Self, ModelError> {
        Ok(Self {
            exchange,
            market,
            venue_symbol: SymbolCode::new(venue_symbol)?,
            canonical_symbol: SymbolCode::new(canonical_symbol)?,
        })
    }
}

/// Normalized chart interval.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Timeframe {
    /// One-minute candle.
    #[serde(rename = "1m")]
    M1,
    /// Three-minute candle.
    #[serde(rename = "3m")]
    M3,
    /// Five-minute candle.
    #[serde(rename = "5m")]
    M5,
    /// Fifteen-minute candle.
    #[serde(rename = "15m")]
    M15,
}

impl Timeframe {
    /// Returns the exact interval duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        match self {
            Self::M1 => Duration::minutes(1),
            Self::M3 => Duration::minutes(3),
            Self::M5 => Duration::minutes(5),
            Self::M15 => Duration::minutes(15),
        }
    }

    /// Returns the canonical wire representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M3 => "3m",
            Self::M5 => "5m",
            Self::M15 => "15m",
        }
    }
}

impl fmt::Display for Timeframe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Expected price direction after an ICT event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Price is expected to move higher.
    Bullish,
    /// Price is expected to move lower.
    Bearish,
}

/// Unit attached to a venue's primary candle-volume field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeUnit {
    /// Quantity of the base asset.
    BaseAsset,
    /// Notional quantity in the quote asset.
    QuoteAsset,
    /// Number of derivative contracts.
    Contracts,
}

/// Unvalidated input used by exchange adapters to create a [`Candle`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandleInput {
    /// Instrument identity.
    pub instrument: InstrumentId,
    /// Candle interval.
    pub timeframe: Timeframe,
    /// Inclusive start of the normalized candle interval.
    pub open_time: DateTime<Utc>,
    /// Exclusive end of the normalized candle interval.
    pub close_time: DateTime<Utc>,
    /// Opening price.
    pub open: Decimal,
    /// Highest traded price.
    pub high: Decimal,
    /// Lowest traded price.
    pub low: Decimal,
    /// Latest or final closing price.
    pub close: Decimal,
    /// Venue-reported primary volume value.
    pub volume: Decimal,
    /// Unit of `volume`.
    pub volume_unit: VolumeUnit,
    /// Venue-reported trade count when available.
    pub trade_count: Option<u64>,
    /// Whether the exchange declared the interval final.
    pub is_closed: bool,
}

/// Exchange-neutral OHLCV candle.
///
/// Time is normalized to the half-open interval `[open_time, close_time)`.
/// Binance's inclusive millisecond close timestamp must therefore be adjusted
/// in its adapter before this model is built.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Candle {
    instrument: InstrumentId,
    timeframe: Timeframe,
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    #[serde(with = "rust_decimal::serde::str")]
    open: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    high: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    low: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    close: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    volume: Decimal,
    volume_unit: VolumeUnit,
    trade_count: Option<u64>,
    is_closed: bool,
}

impl Candle {
    /// Validates and creates a normalized candle.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] if the time range, OHLC bounds, prices, or volume
    /// violate candle invariants.
    pub fn new(input: CandleInput) -> Result<Self, ModelError> {
        if input.open_time >= input.close_time {
            return Err(ModelError::InvalidCandleTime);
        }

        for (field, price) in [
            ("open", input.open),
            ("high", input.high),
            ("low", input.low),
            ("close", input.close),
        ] {
            if price <= Decimal::ZERO {
                return Err(ModelError::NonPositivePrice { field });
            }
        }

        if input.low > input.high
            || input.open < input.low
            || input.open > input.high
            || input.close < input.low
            || input.close > input.high
        {
            return Err(ModelError::InvalidOhlc);
        }

        if input.volume < Decimal::ZERO {
            return Err(ModelError::NegativeVolume);
        }

        Ok(Self {
            instrument: input.instrument,
            timeframe: input.timeframe,
            open_time: input.open_time,
            close_time: input.close_time,
            open: input.open,
            high: input.high,
            low: input.low,
            close: input.close,
            volume: input.volume,
            volume_unit: input.volume_unit,
            trade_count: input.trade_count,
            is_closed: input.is_closed,
        })
    }

    /// Returns the instrument identity.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Returns the candle interval.
    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Returns the inclusive interval start.
    #[must_use]
    pub const fn open_time(&self) -> DateTime<Utc> {
        self.open_time
    }

    /// Returns the exclusive interval end.
    #[must_use]
    pub const fn close_time(&self) -> DateTime<Utc> {
        self.close_time
    }

    /// Returns the opening price.
    #[must_use]
    pub const fn open(&self) -> Decimal {
        self.open
    }

    /// Returns the high price.
    #[must_use]
    pub const fn high(&self) -> Decimal {
        self.high
    }

    /// Returns the low price.
    #[must_use]
    pub const fn low(&self) -> Decimal {
        self.low
    }

    /// Returns the latest or final close price.
    #[must_use]
    pub const fn close(&self) -> Decimal {
        self.close
    }

    /// Returns the primary volume.
    #[must_use]
    pub const fn volume(&self) -> Decimal {
        self.volume
    }

    /// Returns the primary volume's unit.
    #[must_use]
    pub const fn volume_unit(&self) -> VolumeUnit {
        self.volume_unit
    }

    /// Returns the trade count when supplied by the venue.
    #[must_use]
    pub const fn trade_count(&self) -> Option<u64> {
        self.trade_count
    }

    /// Returns whether this candle is final and safe for pattern confirmation.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.is_closed
    }
}

#[derive(Deserialize)]
struct CandleUnchecked {
    instrument: InstrumentId,
    timeframe: Timeframe,
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    #[serde(with = "rust_decimal::serde::str")]
    open: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    high: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    low: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    close: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    volume: Decimal,
    volume_unit: VolumeUnit,
    trade_count: Option<u64>,
    is_closed: bool,
}

impl<'de> Deserialize<'de> for Candle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = CandleUnchecked::deserialize(deserializer)?;
        Self::new(CandleInput {
            instrument: value.instrument,
            timeframe: value.timeframe,
            open_time: value.open_time,
            close_time: value.close_time,
            open: value.open,
            high: value.high,
            low: value.low,
            close: value.close,
            volume: value.volume,
            volume_unit: value.volume_unit,
            trade_count: value.trade_count,
            is_closed: value.is_closed,
        })
        .map_err(de::Error::custom)
    }
}

/// Inclusive lower and upper prices of an ICT zone.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PriceZone {
    #[serde(with = "rust_decimal::serde::str")]
    lower: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    upper: Decimal,
}

impl PriceZone {
    /// Creates a zone satisfying `0 < lower < upper`.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidPriceZone`] when a boundary is non-positive
    /// or the boundaries are not strictly ordered.
    pub fn new(lower: Decimal, upper: Decimal) -> Result<Self, ModelError> {
        if lower <= Decimal::ZERO || lower >= upper {
            return Err(ModelError::InvalidPriceZone { lower, upper });
        }

        Ok(Self { lower, upper })
    }

    /// Returns the inclusive lower boundary.
    #[must_use]
    pub const fn lower(&self) -> Decimal {
        self.lower
    }

    /// Returns the inclusive upper boundary.
    #[must_use]
    pub const fn upper(&self) -> Decimal {
        self.upper
    }

    /// Returns the exact 50% level, also called consequent encroachment.
    #[must_use]
    pub fn midpoint(&self) -> Decimal {
        self.lower + (self.upper - self.lower) / Decimal::from(2_u32)
    }

    /// Checks whether a price is inside the zone, including both boundaries.
    #[must_use]
    pub fn contains(&self, price: Decimal) -> bool {
        price >= self.lower && price <= self.upper
    }
}

#[derive(Deserialize)]
struct PriceZoneUnchecked {
    #[serde(with = "rust_decimal::serde::str")]
    lower: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    upper: Decimal,
}

impl<'de> Deserialize<'de> for PriceZone {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = PriceZoneUnchecked::deserialize(deserializer)?;
        Self::new(value.lower, value.upper).map_err(de::Error::custom)
    }
}

/// Common interface for models backed by a bounded price zone.
pub trait PriceRange {
    /// Returns the model's validated price zone.
    fn price_zone(&self) -> &PriceZone;

    /// Checks whether a price is within the zone.
    fn contains_price(&self, price: Decimal) -> bool {
        self.price_zone().contains(price)
    }
}

/// Lifecycle of an FVG or order-block zone.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ZoneState {
    /// Price has not returned to the zone.
    Fresh,
    /// Price entered the zone but did not satisfy full mitigation rules.
    Touched {
        /// First touch time.
        at: DateTime<Utc>,
    },
    /// The configured mitigation condition was satisfied.
    Mitigated {
        /// Mitigation time.
        at: DateTime<Utc>,
    },
    /// The zone can no longer be traded.
    Invalidated {
        /// Invalidation time.
        at: DateTime<Utc>,
        /// Human-readable strategy reason.
        reason: String,
    },
}

impl ZoneState {
    /// Returns whether the lifecycle reached the mitigated terminal state.
    #[must_use]
    pub const fn is_mitigated(&self) -> bool {
        matches!(self, Self::Mitigated { .. })
    }

    /// Returns whether no further lifecycle transition is valid.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Mitigated { .. } | Self::Invalidated { .. })
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Touched { .. } => "touched",
            Self::Mitigated { .. } => "mitigated",
            Self::Invalidated { .. } => "invalidated",
        }
    }

    fn occurred_at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Fresh => None,
            Self::Touched { at } | Self::Mitigated { at } | Self::Invalidated { at, .. } => {
                Some(*at)
            }
        }
    }
}

fn validate_zone_state(state: &ZoneState, created_at: DateTime<Utc>) -> Result<(), ModelError> {
    if state
        .occurred_at()
        .is_some_and(|occurred_at| occurred_at < created_at)
    {
        return Err(ModelError::InvalidLifecycleTimestamp);
    }

    if let ZoneState::Invalidated { reason, .. } = state {
        if reason.trim().is_empty() {
            return Err(ModelError::EmptyInvalidationReason);
        }
    }

    Ok(())
}

fn transition_zone_state(
    state: &mut ZoneState,
    created_at: DateTime<Utc>,
    next: ZoneState,
) -> Result<(), ModelError> {
    validate_zone_state(&next, created_at)?;

    let valid = matches!(
        (&*state, &next),
        (
            ZoneState::Fresh,
            ZoneState::Touched { .. } | ZoneState::Mitigated { .. } | ZoneState::Invalidated { .. }
        ) | (
            ZoneState::Touched { .. },
            ZoneState::Mitigated { .. } | ZoneState::Invalidated { .. }
        )
    );

    if !valid {
        return Err(ModelError::InvalidZoneTransition {
            from: state.label(),
            to: next.label(),
        });
    }

    if let (Some(current_time), Some(next_time)) = (state.occurred_at(), next.occurred_at()) {
        if next_time < current_time {
            return Err(ModelError::InvalidLifecycleTimestamp);
        }
    }

    *state = next;
    Ok(())
}

/// Validated construction data for a [`FairValueGap`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueGapInput {
    /// Stable pattern identifier.
    pub id: Uuid,
    /// Instrument on which the gap formed.
    pub instrument: InstrumentId,
    /// Detection timeframe.
    pub timeframe: Timeframe,
    /// Expected reaction direction.
    pub direction: Direction,
    /// Tradable gap boundaries.
    pub zone: PriceZone,
    /// Open times of the left, displacement, and right candles.
    pub formed_from: [DateTime<Utc>; 3],
    /// Time at which the right candle made the pattern observable.
    pub formed_at: DateTime<Utc>,
}

/// Three-candle fair value gap (FVG).
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FairValueGap {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    direction: Direction,
    zone: PriceZone,
    formed_from: [DateTime<Utc>; 3],
    formed_at: DateTime<Utc>,
    state: ZoneState,
}

/// Concise alias commonly used by ICT literature.
pub type Fvg = FairValueGap;

impl FairValueGap {
    /// Creates a fresh FVG after validating candle ordering and timestamps.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] for a nil identifier, non-contiguous anchors,
    /// or a detection timestamp other than the final candle close.
    pub fn new(input: FairValueGapInput) -> Result<Self, ModelError> {
        if input.id.is_nil() {
            return Err(ModelError::NilIdentifier { model: "fvg" });
        }

        if !(input.formed_from[0] < input.formed_from[1]
            && input.formed_from[1] < input.formed_from[2])
        {
            return Err(ModelError::InvalidPatternAnchors { model: "fvg" });
        }

        let interval = input.timeframe.duration();
        if input.formed_from[0].checked_add_signed(interval) != Some(input.formed_from[1])
            || input.formed_from[1].checked_add_signed(interval) != Some(input.formed_from[2])
        {
            return Err(ModelError::NonContiguousPatternAnchors { model: "fvg" });
        }

        if input.formed_from[2].checked_add_signed(interval) != Some(input.formed_at) {
            return Err(ModelError::InvalidPatternDetectionTime { model: "fvg" });
        }

        Ok(Self {
            id: input.id,
            instrument: input.instrument,
            timeframe: input.timeframe,
            direction: input.direction,
            zone: input.zone,
            formed_from: input.formed_from,
            formed_at: input.formed_at,
            state: ZoneState::Fresh,
        })
    }

    /// Returns the pattern identifier.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the instrument identity.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Returns the detection timeframe.
    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Returns the expected reaction direction.
    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
    }

    /// Returns the gap boundaries.
    #[must_use]
    pub const fn zone(&self) -> &PriceZone {
        &self.zone
    }

    /// Returns the three source candle open times.
    #[must_use]
    pub const fn formed_from(&self) -> &[DateTime<Utc>; 3] {
        &self.formed_from
    }

    /// Returns the detection time.
    #[must_use]
    pub const fn formed_at(&self) -> DateTime<Utc> {
        self.formed_at
    }

    /// Returns the zone lifecycle.
    #[must_use]
    pub const fn state(&self) -> &ZoneState {
        &self.state
    }

    /// Returns the 50% consequent-encroachment level.
    #[must_use]
    pub fn consequent_encroachment(&self) -> Decimal {
        self.zone.midpoint()
    }

    /// Returns whether the configured mitigation rule has completed.
    #[must_use]
    pub const fn is_mitigated(&self) -> bool {
        self.state.is_mitigated()
    }

    /// Records the first price return into the gap.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] when the timestamp is invalid or the current
    /// lifecycle state cannot transition to `Touched`.
    pub fn mark_touched(&mut self, at: DateTime<Utc>) -> Result<(), ModelError> {
        transition_zone_state(&mut self.state, self.formed_at, ZoneState::Touched { at })
    }

    /// Marks the gap as fully mitigated.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] when the timestamp is invalid or the current
    /// lifecycle state cannot transition to `Mitigated`.
    pub fn mark_mitigated(&mut self, at: DateTime<Utc>) -> Result<(), ModelError> {
        transition_zone_state(&mut self.state, self.formed_at, ZoneState::Mitigated { at })
    }

    /// Marks the gap as unusable for a documented strategy reason.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] for a blank reason, invalid timestamp, or an
    /// invalid lifecycle transition.
    pub fn invalidate(
        &mut self,
        at: DateTime<Utc>,
        reason: impl Into<String>,
    ) -> Result<(), ModelError> {
        transition_zone_state(
            &mut self.state,
            self.formed_at,
            ZoneState::Invalidated {
                at,
                reason: reason.into(),
            },
        )
    }

    fn restore_state(&mut self, state: ZoneState) -> Result<(), ModelError> {
        validate_zone_state(&state, self.formed_at)?;
        self.state = state;
        Ok(())
    }
}

impl PriceRange for FairValueGap {
    fn price_zone(&self) -> &PriceZone {
        &self.zone
    }
}

#[derive(Deserialize)]
struct FairValueGapUnchecked {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    direction: Direction,
    zone: PriceZone,
    formed_from: [DateTime<Utc>; 3],
    formed_at: DateTime<Utc>,
    state: ZoneState,
}

impl<'de> Deserialize<'de> for FairValueGap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = FairValueGapUnchecked::deserialize(deserializer)?;
        let mut gap = Self::new(FairValueGapInput {
            id: value.id,
            instrument: value.instrument,
            timeframe: value.timeframe,
            direction: value.direction,
            zone: value.zone,
            formed_from: value.formed_from,
            formed_at: value.formed_at,
        })
        .map_err(de::Error::custom)?;
        gap.restore_state(value.state).map_err(de::Error::custom)?;
        Ok(gap)
    }
}

/// Price range used when deriving an order block from its origin candle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderBlockZoneBasis {
    /// Entire candle range from low to high.
    FullCandle,
    /// Candle body only, excluding wicks.
    CandleBody,
}

/// Validated construction data for an [`OrderBlock`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBlockInput {
    /// Stable pattern identifier.
    pub id: Uuid,
    /// Instrument on which the block formed.
    pub instrument: InstrumentId,
    /// Detection timeframe.
    pub timeframe: Timeframe,
    /// Expected reaction direction, not the origin candle direction.
    pub direction: Direction,
    /// Tradable block boundaries.
    pub zone: PriceZone,
    /// Whether the zone uses the full candle or its body.
    pub zone_basis: OrderBlockZoneBasis,
    /// Origin candle's open time.
    pub origin_candle_open_time: DateTime<Utc>,
    /// Displacement candle's open time.
    pub displacement_candle_open_time: DateTime<Utc>,
    /// Time when the block was confirmed.
    pub created_at: DateTime<Utc>,
}

/// ICT order block associated with a displacement move.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OrderBlock {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    direction: Direction,
    zone: PriceZone,
    zone_basis: OrderBlockZoneBasis,
    origin_candle_open_time: DateTime<Utc>,
    displacement_candle_open_time: DateTime<Utc>,
    created_at: DateTime<Utc>,
    state: ZoneState,
}

impl OrderBlock {
    /// Creates a fresh order block with ordered origin and displacement times.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] for a nil identifier, unordered candle anchors,
    /// or a confirmation timestamp before displacement.
    pub fn new(input: OrderBlockInput) -> Result<Self, ModelError> {
        if input.id.is_nil() {
            return Err(ModelError::NilIdentifier {
                model: "order_block",
            });
        }

        if input.origin_candle_open_time >= input.displacement_candle_open_time {
            return Err(ModelError::InvalidPatternAnchors {
                model: "order_block",
            });
        }

        if input.created_at < input.displacement_candle_open_time {
            return Err(ModelError::PatternBeforeFinalAnchor {
                model: "order_block",
            });
        }

        Ok(Self {
            id: input.id,
            instrument: input.instrument,
            timeframe: input.timeframe,
            direction: input.direction,
            zone: input.zone,
            zone_basis: input.zone_basis,
            origin_candle_open_time: input.origin_candle_open_time,
            displacement_candle_open_time: input.displacement_candle_open_time,
            created_at: input.created_at,
            state: ZoneState::Fresh,
        })
    }

    /// Returns the pattern identifier.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the instrument identity.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Returns the detection timeframe.
    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Returns the expected reaction direction.
    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
    }

    /// Returns the block boundaries.
    #[must_use]
    pub const fn zone(&self) -> &PriceZone {
        &self.zone
    }

    /// Returns whether the range uses the full candle or only its body.
    #[must_use]
    pub const fn zone_basis(&self) -> OrderBlockZoneBasis {
        self.zone_basis
    }

    /// Returns the origin candle open time.
    #[must_use]
    pub const fn origin_candle_open_time(&self) -> DateTime<Utc> {
        self.origin_candle_open_time
    }

    /// Returns the displacement candle open time.
    #[must_use]
    pub const fn displacement_candle_open_time(&self) -> DateTime<Utc> {
        self.displacement_candle_open_time
    }

    /// Returns the confirmation time.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns the zone lifecycle.
    #[must_use]
    pub const fn state(&self) -> &ZoneState {
        &self.state
    }

    /// Returns whether the order block is fully mitigated.
    #[must_use]
    pub const fn is_mitigated(&self) -> bool {
        self.state.is_mitigated()
    }

    /// Records the first price return into the block.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] when the timestamp is invalid or the current
    /// lifecycle state cannot transition to `Touched`.
    pub fn mark_touched(&mut self, at: DateTime<Utc>) -> Result<(), ModelError> {
        transition_zone_state(&mut self.state, self.created_at, ZoneState::Touched { at })
    }

    /// Marks the order block as fully mitigated.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] when the timestamp is invalid or the current
    /// lifecycle state cannot transition to `Mitigated`.
    pub fn mark_mitigated(&mut self, at: DateTime<Utc>) -> Result<(), ModelError> {
        transition_zone_state(
            &mut self.state,
            self.created_at,
            ZoneState::Mitigated { at },
        )
    }

    /// Invalidates the block for a documented strategy reason.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] for a blank reason, invalid timestamp, or an
    /// invalid lifecycle transition.
    pub fn invalidate(
        &mut self,
        at: DateTime<Utc>,
        reason: impl Into<String>,
    ) -> Result<(), ModelError> {
        transition_zone_state(
            &mut self.state,
            self.created_at,
            ZoneState::Invalidated {
                at,
                reason: reason.into(),
            },
        )
    }

    fn restore_state(&mut self, state: ZoneState) -> Result<(), ModelError> {
        validate_zone_state(&state, self.created_at)?;
        self.state = state;
        Ok(())
    }
}

impl PriceRange for OrderBlock {
    fn price_zone(&self) -> &PriceZone {
        &self.zone
    }
}

#[derive(Deserialize)]
struct OrderBlockUnchecked {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    direction: Direction,
    zone: PriceZone,
    zone_basis: OrderBlockZoneBasis,
    origin_candle_open_time: DateTime<Utc>,
    displacement_candle_open_time: DateTime<Utc>,
    created_at: DateTime<Utc>,
    state: ZoneState,
}

impl<'de> Deserialize<'de> for OrderBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = OrderBlockUnchecked::deserialize(deserializer)?;
        let mut order_block = Self::new(OrderBlockInput {
            id: value.id,
            instrument: value.instrument,
            timeframe: value.timeframe,
            direction: value.direction,
            zone: value.zone,
            zone_basis: value.zone_basis,
            origin_candle_open_time: value.origin_candle_open_time,
            displacement_candle_open_time: value.displacement_candle_open_time,
            created_at: value.created_at,
        })
        .map_err(de::Error::custom)?;
        order_block
            .restore_state(value.state)
            .map_err(de::Error::custom)?;
        Ok(order_block)
    }
}

/// Swing side identified by a fractal detector.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwingKind {
    /// Local high and potential buy-side liquidity.
    High,
    /// Local low and potential sell-side liquidity.
    Low,
}

/// Number of candles used to confirm a swing point.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FractalModel {
    /// One candle on each side of the candidate.
    ThreeCandle,
    /// Two candles on each side of the candidate.
    FiveCandle,
}

/// Confirmed swing high or swing low.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SwingPoint {
    instrument: InstrumentId,
    timeframe: Timeframe,
    kind: SwingKind,
    #[serde(with = "rust_decimal::serde::str")]
    price: Decimal,
    candle_open_time: DateTime<Utc>,
    confirmed_at: DateTime<Utc>,
    model: FractalModel,
}

impl SwingPoint {
    /// Creates a confirmed swing point.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] when the price is non-positive or confirmation
    /// does not follow the candidate candle.
    pub fn new(
        instrument: InstrumentId,
        timeframe: Timeframe,
        kind: SwingKind,
        price: Decimal,
        candle_open_time: DateTime<Utc>,
        confirmed_at: DateTime<Utc>,
        model: FractalModel,
    ) -> Result<Self, ModelError> {
        if price <= Decimal::ZERO {
            return Err(ModelError::NonPositivePrice {
                field: "swing_price",
            });
        }

        let confirmation_intervals = match model {
            FractalModel::ThreeCandle => 2,
            FractalModel::FiveCandle => 3,
        };
        let expected_confirmation = timeframe
            .duration()
            .checked_mul(confirmation_intervals)
            .and_then(|offset| candle_open_time.checked_add_signed(offset));
        if expected_confirmation != Some(confirmed_at) {
            return Err(ModelError::InvalidSwingTime);
        }

        Ok(Self {
            instrument,
            timeframe,
            kind,
            price,
            candle_open_time,
            confirmed_at,
            model,
        })
    }

    /// Returns the instrument on which the swing was confirmed.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Returns the source timeframe.
    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Returns whether this is a swing high or low.
    #[must_use]
    pub const fn kind(&self) -> SwingKind {
        self.kind
    }

    /// Returns the swing price.
    #[must_use]
    pub const fn price(&self) -> Decimal {
        self.price
    }

    /// Returns the candidate candle's open time.
    #[must_use]
    pub const fn candle_open_time(&self) -> DateTime<Utc> {
        self.candle_open_time
    }

    /// Returns when enough right-hand candles existed to confirm the swing.
    #[must_use]
    pub const fn confirmed_at(&self) -> DateTime<Utc> {
        self.confirmed_at
    }

    /// Returns the fractal model used for confirmation.
    #[must_use]
    pub const fn model(&self) -> FractalModel {
        self.model
    }
}

#[derive(Deserialize)]
struct SwingPointUnchecked {
    instrument: InstrumentId,
    timeframe: Timeframe,
    kind: SwingKind,
    #[serde(with = "rust_decimal::serde::str")]
    price: Decimal,
    candle_open_time: DateTime<Utc>,
    confirmed_at: DateTime<Utc>,
    model: FractalModel,
}

impl<'de> Deserialize<'de> for SwingPoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = SwingPointUnchecked::deserialize(deserializer)?;
        Self::new(
            value.instrument,
            value.timeframe,
            value.kind,
            value.price,
            value.candle_open_time,
            value.confirmed_at,
            value.model,
        )
        .map_err(de::Error::custom)
    }
}

/// Price component required to break before structure is confirmed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakBasis {
    /// Any wick through the level confirms the break.
    Wick,
    /// A candle close through the level confirms the break.
    Close,
}

/// Validated construction data for a [`MarketStructureShift`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketStructureShiftInput {
    /// Stable event identifier.
    pub id: Uuid,
    /// Instrument on which structure shifted.
    pub instrument: InstrumentId,
    /// Detection timeframe.
    pub timeframe: Timeframe,
    /// New directional bias.
    pub direction: Direction,
    /// Swing point broken by the confirming price move.
    pub broken_swing: SwingPoint,
    /// Wick or close price that confirmed the break.
    pub break_price: Decimal,
    /// Whether a wick or close is required.
    pub break_basis: BreakBasis,
    /// Open time of the confirming candle.
    pub confirming_candle_open_time: DateTime<Utc>,
    /// Time when the shift became confirmed.
    pub confirmed_at: DateTime<Utc>,
}

/// Confirmed ICT market structure shift (MSS).
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MarketStructureShift {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    direction: Direction,
    broken_swing: SwingPoint,
    #[serde(with = "rust_decimal::serde::str")]
    break_price: Decimal,
    break_basis: BreakBasis,
    confirming_candle_open_time: DateTime<Utc>,
    confirmed_at: DateTime<Utc>,
}

/// Concise alias commonly used by ICT literature.
pub type Mss = MarketStructureShift;

impl MarketStructureShift {
    /// Creates a structure shift and enforces direction/swing consistency.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] for an invalid identifier, price, direction,
    /// broken swing, or timestamp ordering.
    pub fn new(input: MarketStructureShiftInput) -> Result<Self, ModelError> {
        if input.id.is_nil() {
            return Err(ModelError::NilIdentifier { model: "mss" });
        }

        if input.break_price <= Decimal::ZERO {
            return Err(ModelError::NonPositivePrice {
                field: "break_price",
            });
        }

        if input.broken_swing.instrument() != &input.instrument
            || input.broken_swing.timeframe() != input.timeframe
        {
            return Err(ModelError::StructureSeriesMismatch);
        }

        let valid_break = matches!(
            (input.direction, input.broken_swing.kind()),
            (Direction::Bullish, SwingKind::High)
                if input.break_price > input.broken_swing.price()
        ) || matches!(
            (input.direction, input.broken_swing.kind()),
            (Direction::Bearish, SwingKind::Low)
                if input.break_price < input.broken_swing.price()
        );

        if !valid_break {
            return Err(ModelError::InvalidStructureBreak);
        }

        let expected_confirmation = input
            .confirming_candle_open_time
            .checked_add_signed(input.timeframe.duration());
        if input.confirming_candle_open_time < input.broken_swing.confirmed_at()
            || expected_confirmation != Some(input.confirmed_at)
        {
            return Err(ModelError::InvalidStructureTime);
        }

        Ok(Self {
            id: input.id,
            instrument: input.instrument,
            timeframe: input.timeframe,
            direction: input.direction,
            broken_swing: input.broken_swing,
            break_price: input.break_price,
            break_basis: input.break_basis,
            confirming_candle_open_time: input.confirming_candle_open_time,
            confirmed_at: input.confirmed_at,
        })
    }

    /// Returns the event identifier.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the instrument identity.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Returns the detection timeframe.
    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Returns the new directional bias.
    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
    }

    /// Returns the swing that was broken.
    #[must_use]
    pub const fn broken_swing(&self) -> &SwingPoint {
        &self.broken_swing
    }

    /// Returns the break-confirmation price.
    #[must_use]
    pub const fn break_price(&self) -> Decimal {
        self.break_price
    }

    /// Returns whether wick or close confirmation was required.
    #[must_use]
    pub const fn break_basis(&self) -> BreakBasis {
        self.break_basis
    }

    /// Returns the confirming candle open time.
    #[must_use]
    pub const fn confirming_candle_open_time(&self) -> DateTime<Utc> {
        self.confirming_candle_open_time
    }

    /// Returns the event confirmation time.
    #[must_use]
    pub const fn confirmed_at(&self) -> DateTime<Utc> {
        self.confirmed_at
    }
}

#[derive(Deserialize)]
struct MarketStructureShiftUnchecked {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    direction: Direction,
    broken_swing: SwingPoint,
    #[serde(with = "rust_decimal::serde::str")]
    break_price: Decimal,
    break_basis: BreakBasis,
    confirming_candle_open_time: DateTime<Utc>,
    confirmed_at: DateTime<Utc>,
}

impl<'de> Deserialize<'de> for MarketStructureShift {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = MarketStructureShiftUnchecked::deserialize(deserializer)?;
        Self::new(MarketStructureShiftInput {
            id: value.id,
            instrument: value.instrument,
            timeframe: value.timeframe,
            direction: value.direction,
            broken_swing: value.broken_swing,
            break_price: value.break_price,
            break_basis: value.break_basis,
            confirming_candle_open_time: value.confirming_candle_open_time,
            confirmed_at: value.confirmed_at,
        })
        .map_err(de::Error::custom)
    }
}

/// Validated construction data for a [`BreakOfStructure`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BreakOfStructureInput {
    /// Stable event identifier.
    pub id: Uuid,
    /// Instrument on which structure was broken.
    pub instrument: InstrumentId,
    /// Detection timeframe.
    pub timeframe: Timeframe,
    /// Direction in which structure was broken.
    pub direction: Direction,
    /// Swing point broken by price.
    pub broken_swing: SwingPoint,
    /// Wick or close price that confirmed the break.
    pub break_price: Decimal,
    /// Whether a wick or close is required.
    pub break_basis: BreakBasis,
    /// Open time of the confirming candle.
    pub confirming_candle_open_time: DateTime<Utc>,
    /// Time when the break became confirmed.
    pub confirmed_at: DateTime<Utc>,
}

/// Confirmed ICT break of structure (BOS).
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BreakOfStructure {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    direction: Direction,
    broken_swing: SwingPoint,
    #[serde(with = "rust_decimal::serde::str")]
    break_price: Decimal,
    break_basis: BreakBasis,
    confirming_candle_open_time: DateTime<Utc>,
    confirmed_at: DateTime<Utc>,
}

/// Concise alias commonly used by ICT literature.
pub type Bos = BreakOfStructure;

impl BreakOfStructure {
    /// Creates a structure break and enforces direction/swing consistency.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] for an invalid identifier, price, direction,
    /// broken swing, or timestamp ordering.
    pub fn new(input: BreakOfStructureInput) -> Result<Self, ModelError> {
        if input.id.is_nil() {
            return Err(ModelError::NilIdentifier { model: "bos" });
        }

        if input.break_price <= Decimal::ZERO {
            return Err(ModelError::NonPositivePrice {
                field: "break_price",
            });
        }

        if input.broken_swing.instrument() != &input.instrument
            || input.broken_swing.timeframe() != input.timeframe
        {
            return Err(ModelError::StructureSeriesMismatch);
        }

        let valid_break = matches!(
            (input.direction, input.broken_swing.kind()),
            (Direction::Bullish, SwingKind::High)
                if input.break_price > input.broken_swing.price()
        ) || matches!(
            (input.direction, input.broken_swing.kind()),
            (Direction::Bearish, SwingKind::Low)
                if input.break_price < input.broken_swing.price()
        );

        if !valid_break {
            return Err(ModelError::InvalidStructureBreak);
        }

        let expected_confirmation = input
            .confirming_candle_open_time
            .checked_add_signed(input.timeframe.duration());
        if input.confirming_candle_open_time < input.broken_swing.confirmed_at()
            || expected_confirmation != Some(input.confirmed_at)
        {
            return Err(ModelError::InvalidStructureTime);
        }

        Ok(Self {
            id: input.id,
            instrument: input.instrument,
            timeframe: input.timeframe,
            direction: input.direction,
            broken_swing: input.broken_swing,
            break_price: input.break_price,
            break_basis: input.break_basis,
            confirming_candle_open_time: input.confirming_candle_open_time,
            confirmed_at: input.confirmed_at,
        })
    }

    /// Returns the event identifier.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the instrument identity.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Returns the detection timeframe.
    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Returns the break direction.
    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
    }

    /// Returns the swing that was broken.
    #[must_use]
    pub const fn broken_swing(&self) -> &SwingPoint {
        &self.broken_swing
    }

    /// Returns the break-confirmation price.
    #[must_use]
    pub const fn break_price(&self) -> Decimal {
        self.break_price
    }

    /// Returns whether wick or close confirmation was required.
    #[must_use]
    pub const fn break_basis(&self) -> BreakBasis {
        self.break_basis
    }

    /// Returns the confirming candle open time.
    #[must_use]
    pub const fn confirming_candle_open_time(&self) -> DateTime<Utc> {
        self.confirming_candle_open_time
    }

    /// Returns the event confirmation time.
    #[must_use]
    pub const fn confirmed_at(&self) -> DateTime<Utc> {
        self.confirmed_at
    }
}

#[derive(Deserialize)]
struct BreakOfStructureUnchecked {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    direction: Direction,
    broken_swing: SwingPoint,
    #[serde(with = "rust_decimal::serde::str")]
    break_price: Decimal,
    break_basis: BreakBasis,
    confirming_candle_open_time: DateTime<Utc>,
    confirmed_at: DateTime<Utc>,
}

impl<'de> Deserialize<'de> for BreakOfStructure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = BreakOfStructureUnchecked::deserialize(deserializer)?;
        Self::new(BreakOfStructureInput {
            id: value.id,
            instrument: value.instrument,
            timeframe: value.timeframe,
            direction: value.direction,
            broken_swing: value.broken_swing,
            break_price: value.break_price,
            break_basis: value.break_basis,
            confirming_candle_open_time: value.confirming_candle_open_time,
            confirmed_at: value.confirmed_at,
        })
        .map_err(de::Error::custom)
    }
}

/// Liquidity pool swept by price.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiquiditySide {
    /// Buy stops resting above a reference high (BSL).
    BuySide,
    /// Sell stops resting below a reference low (SSL).
    SellSide,
}

/// Validated construction data for a [`LiquiditySweep`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiquiditySweepInput {
    /// Stable event identifier.
    pub id: Uuid,
    /// Instrument on which liquidity was swept.
    pub instrument: InstrumentId,
    /// Detection timeframe.
    pub timeframe: Timeframe,
    /// Buy-side or sell-side liquidity.
    pub side: LiquiditySide,
    /// Swing or equal-high/low level holding liquidity.
    pub reference_price: Decimal,
    /// Extreme price that moved through the reference.
    pub sweep_price: Decimal,
    /// Open time of the sweep candle.
    pub sweep_candle_open_time: DateTime<Utc>,
    /// Time when the sweep was detected.
    pub detected_at: DateTime<Utc>,
}

/// Confirmed break through a buy-side or sell-side liquidity level.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LiquiditySweep {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    side: LiquiditySide,
    #[serde(with = "rust_decimal::serde::str")]
    reference_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    sweep_price: Decimal,
    sweep_candle_open_time: DateTime<Utc>,
    detected_at: DateTime<Utc>,
}

impl LiquiditySweep {
    /// Creates a sweep while enforcing the correct side of the reference level.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] for an invalid identifier, price, crossing
    /// direction, or timestamp ordering.
    pub fn new(input: LiquiditySweepInput) -> Result<Self, ModelError> {
        if input.id.is_nil() {
            return Err(ModelError::NilIdentifier {
                model: "liquidity_sweep",
            });
        }

        if input.reference_price <= Decimal::ZERO || input.sweep_price <= Decimal::ZERO {
            return Err(ModelError::NonPositivePrice {
                field: "liquidity_price",
            });
        }

        let crossed_reference = match input.side {
            LiquiditySide::BuySide => input.sweep_price > input.reference_price,
            LiquiditySide::SellSide => input.sweep_price < input.reference_price,
        };
        if !crossed_reference {
            return Err(ModelError::InvalidLiquiditySweep);
        }

        if input.detected_at < input.sweep_candle_open_time {
            return Err(ModelError::InvalidLiquiditySweepTime);
        }

        Ok(Self {
            id: input.id,
            instrument: input.instrument,
            timeframe: input.timeframe,
            side: input.side,
            reference_price: input.reference_price,
            sweep_price: input.sweep_price,
            sweep_candle_open_time: input.sweep_candle_open_time,
            detected_at: input.detected_at,
        })
    }

    /// Returns the event identifier.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the instrument identity.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Returns the detection timeframe.
    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Returns which side of liquidity was swept.
    #[must_use]
    pub const fn side(&self) -> LiquiditySide {
        self.side
    }

    /// Returns the liquidity reference level.
    #[must_use]
    pub const fn reference_price(&self) -> Decimal {
        self.reference_price
    }

    /// Returns the sweep candle's extreme price.
    #[must_use]
    pub const fn sweep_price(&self) -> Decimal {
        self.sweep_price
    }

    /// Returns the sweep candle open time.
    #[must_use]
    pub const fn sweep_candle_open_time(&self) -> DateTime<Utc> {
        self.sweep_candle_open_time
    }

    /// Returns the detection time.
    #[must_use]
    pub const fn detected_at(&self) -> DateTime<Utc> {
        self.detected_at
    }
}

#[derive(Deserialize)]
struct LiquiditySweepUnchecked {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    side: LiquiditySide,
    #[serde(with = "rust_decimal::serde::str")]
    reference_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    sweep_price: Decimal,
    sweep_candle_open_time: DateTime<Utc>,
    detected_at: DateTime<Utc>,
}

impl<'de> Deserialize<'de> for LiquiditySweep {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = LiquiditySweepUnchecked::deserialize(deserializer)?;
        Self::new(LiquiditySweepInput {
            id: value.id,
            instrument: value.instrument,
            timeframe: value.timeframe,
            side: value.side,
            reference_price: value.reference_price,
            sweep_price: value.sweep_price,
            sweep_candle_open_time: value.sweep_candle_open_time,
            detected_at: value.detected_at,
        })
        .map_err(de::Error::custom)
    }
}

/// Price within an FVG at which a strategy requests a limit entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FvgEntryMode {
    /// Enter at the boundary first encountered during a retracement.
    NearBoundary,
    /// Enter at the exact 50% consequent-encroachment level.
    ConsequentEncroachment,
}

/// Validated construction data for a [`TradeIntent`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradeIntentInput {
    /// Stable strategy-decision identifier.
    pub id: Uuid,
    /// Instrument on which the setup occurred.
    pub instrument: InstrumentId,
    /// Timeframe used by every piece of setup evidence.
    pub timeframe: Timeframe,
    /// Intended position direction.
    pub direction: Direction,
    /// Liquidity event whose extreme anchors the protective stop.
    pub liquidity_sweep: LiquiditySweep,
    /// Structure shift confirmed by the FVG's displacement candle.
    pub market_structure_shift: MarketStructureShift,
    /// FVG into which price retraced.
    pub fair_value_gap: FairValueGap,
    /// Rule used to derive the limit-entry price from the FVG.
    pub entry_mode: FvgEntryMode,
    /// Final candle that reached the derived entry price.
    pub trigger_candle: Candle,
    /// Time after which downstream risk code must reject the intent.
    pub expires_at: DateTime<Utc>,
}

/// Strategy-approved ICT setup awaiting independent risk approval.
///
/// This model intentionally contains no quantity, account risk, leverage, or
/// final stop/take-profit price. The risk layer derives those values from this
/// immutable market context and current account/exchange constraints.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TradeIntent {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    direction: Direction,
    liquidity_sweep: LiquiditySweep,
    market_structure_shift: MarketStructureShift,
    fair_value_gap: FairValueGap,
    entry_mode: FvgEntryMode,
    trigger_candle: Candle,
    expires_at: DateTime<Utc>,
}

impl TradeIntent {
    /// Creates a strategy intent after validating its complete ICT evidence.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] when identifiers, series, direction, event
    /// ordering, FVG state, trigger candle, entry reach, stop anchor, or expiry
    /// are inconsistent.
    pub fn new(input: TradeIntentInput) -> Result<Self, ModelError> {
        if input.id.is_nil() {
            return Err(ModelError::NilIdentifier {
                model: "trade_intent",
            });
        }

        validate_trade_intent_series(&input)?;
        validate_trade_intent_direction(&input)?;
        validate_trade_intent_timing(&input)?;

        let entry_price =
            trade_intent_entry_price(input.direction, &input.fair_value_gap, input.entry_mode);
        validate_trade_intent_trigger(&input, entry_price)?;

        Ok(Self {
            id: input.id,
            instrument: input.instrument,
            timeframe: input.timeframe,
            direction: input.direction,
            liquidity_sweep: input.liquidity_sweep,
            market_structure_shift: input.market_structure_shift,
            fair_value_gap: input.fair_value_gap,
            entry_mode: input.entry_mode,
            trigger_candle: input.trigger_candle,
            expires_at: input.expires_at,
        })
    }

    /// Returns the stable strategy-decision identifier.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the traded instrument.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Returns the setup timeframe.
    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Returns the intended position direction.
    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
    }

    /// Returns the sweep whose extreme anchors the protective stop.
    #[must_use]
    pub const fn liquidity_sweep(&self) -> &LiquiditySweep {
        &self.liquidity_sweep
    }

    /// Returns the displacement-confirmed market structure shift.
    #[must_use]
    pub const fn market_structure_shift(&self) -> &MarketStructureShift {
        &self.market_structure_shift
    }

    /// Returns the FVG used for entry.
    #[must_use]
    pub const fn fair_value_gap(&self) -> &FairValueGap {
        &self.fair_value_gap
    }

    /// Returns the configured FVG entry rule.
    #[must_use]
    pub const fn entry_mode(&self) -> FvgEntryMode {
        self.entry_mode
    }

    /// Derives the exact limit-entry price from the validated FVG.
    #[must_use]
    pub fn entry_price(&self) -> Decimal {
        trade_intent_entry_price(self.direction, &self.fair_value_gap, self.entry_mode)
    }

    /// Returns the sweep extreme beyond which risk code places the stop.
    #[must_use]
    pub const fn stop_anchor_price(&self) -> Decimal {
        self.liquidity_sweep.sweep_price()
    }

    /// Returns the final candle that triggered the entry decision.
    #[must_use]
    pub const fn trigger_candle(&self) -> &Candle {
        &self.trigger_candle
    }

    /// Returns the market time at which the intent became observable.
    #[must_use]
    pub const fn triggered_at(&self) -> DateTime<Utc> {
        self.trigger_candle.close_time()
    }

    /// Returns the deadline after which the intent is stale.
    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

fn trade_intent_entry_price(
    direction: Direction,
    gap: &FairValueGap,
    mode: FvgEntryMode,
) -> Decimal {
    match mode {
        FvgEntryMode::NearBoundary => match direction {
            Direction::Bullish => gap.zone().upper(),
            Direction::Bearish => gap.zone().lower(),
        },
        FvgEntryMode::ConsequentEncroachment => gap.consequent_encroachment(),
    }
}

fn validate_trade_intent_series(input: &TradeIntentInput) -> Result<(), ModelError> {
    let same_series = input.liquidity_sweep.instrument() == &input.instrument
        && input.liquidity_sweep.timeframe() == input.timeframe
        && input.market_structure_shift.instrument() == &input.instrument
        && input.market_structure_shift.timeframe() == input.timeframe
        && input.fair_value_gap.instrument() == &input.instrument
        && input.fair_value_gap.timeframe() == input.timeframe
        && input.trigger_candle.instrument() == &input.instrument
        && input.trigger_candle.timeframe() == input.timeframe;

    if !same_series {
        return Err(ModelError::TradeIntentSeriesMismatch);
    }

    Ok(())
}

fn validate_trade_intent_direction(input: &TradeIntentInput) -> Result<(), ModelError> {
    if input.market_structure_shift.direction() != input.direction
        || input.fair_value_gap.direction() != input.direction
    {
        return Err(ModelError::TradeIntentDirectionMismatch);
    }

    let valid_sweep_side = matches!(
        (input.direction, input.liquidity_sweep.side()),
        (Direction::Bullish, LiquiditySide::SellSide)
            | (Direction::Bearish, LiquiditySide::BuySide)
    );
    if !valid_sweep_side {
        return Err(ModelError::TradeIntentSweepSideMismatch);
    }

    if matches!(input.fair_value_gap.state(), ZoneState::Invalidated { .. }) {
        return Err(ModelError::TradeIntentInvalidatedGap);
    }

    Ok(())
}

fn validate_trade_intent_timing(input: &TradeIntentInput) -> Result<(), ModelError> {
    if input.market_structure_shift.confirming_candle_open_time()
        != input.fair_value_gap.formed_from()[1]
    {
        return Err(ModelError::TradeIntentDisplacementMismatch);
    }

    if input.liquidity_sweep.detected_at() > input.market_structure_shift.confirmed_at()
        || input.market_structure_shift.confirmed_at() > input.fair_value_gap.formed_at()
    {
        return Err(ModelError::InvalidTradeIntentSequence);
    }

    let expected_close = input
        .trigger_candle
        .open_time()
        .checked_add_signed(input.timeframe.duration());
    if expected_close != Some(input.trigger_candle.close_time()) {
        return Err(ModelError::InvalidTradeIntentTriggerDuration);
    }
    if !input.trigger_candle.is_closed() {
        return Err(ModelError::OpenTradeIntentTrigger);
    }
    if input.trigger_candle.open_time() <= input.fair_value_gap.formed_from()[2]
        || input.trigger_candle.close_time() < input.fair_value_gap.formed_at()
    {
        return Err(ModelError::InvalidTradeIntentTriggerTime);
    }
    if input.expires_at <= input.trigger_candle.close_time() {
        return Err(ModelError::InvalidTradeIntentExpiry);
    }

    Ok(())
}

fn validate_trade_intent_trigger(
    input: &TradeIntentInput,
    entry_price: Decimal,
) -> Result<(), ModelError> {
    let entry_reached =
        input.trigger_candle.low() <= entry_price && input.trigger_candle.high() >= entry_price;
    if !entry_reached {
        return Err(ModelError::TradeIntentEntryNotReached);
    }

    let zone = input.fair_value_gap.zone();
    let gap_holds = match input.direction {
        Direction::Bullish => input.trigger_candle.close() >= zone.lower(),
        Direction::Bearish => input.trigger_candle.close() <= zone.upper(),
    };
    if !gap_holds {
        return Err(ModelError::TradeIntentGapViolated);
    }

    let valid_stop_anchor = match input.direction {
        Direction::Bullish => input.liquidity_sweep.sweep_price() < entry_price,
        Direction::Bearish => input.liquidity_sweep.sweep_price() > entry_price,
    };
    if !valid_stop_anchor {
        return Err(ModelError::InvalidTradeIntentStopAnchor);
    }

    Ok(())
}

#[derive(Deserialize)]
struct TradeIntentUnchecked {
    id: Uuid,
    instrument: InstrumentId,
    timeframe: Timeframe,
    direction: Direction,
    liquidity_sweep: LiquiditySweep,
    market_structure_shift: MarketStructureShift,
    fair_value_gap: FairValueGap,
    entry_mode: FvgEntryMode,
    trigger_candle: Candle,
    expires_at: DateTime<Utc>,
}

impl<'de> Deserialize<'de> for TradeIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = TradeIntentUnchecked::deserialize(deserializer)?;
        Self::new(TradeIntentInput {
            id: value.id,
            instrument: value.instrument,
            timeframe: value.timeframe,
            direction: value.direction,
            liquidity_sweep: value.liquidity_sweep,
            market_structure_shift: value.market_structure_shift,
            fair_value_gap: value.fair_value_gap,
            entry_mode: value.entry_mode,
            trigger_candle: value.trigger_candle,
            expires_at: value.expires_at,
        })
        .map_err(de::Error::custom)
    }
}

impl DomainEvent for TradeIntent {
    fn instrument(&self) -> &InstrumentId {
        self.instrument()
    }

    fn timeframe(&self) -> Timeframe {
        self.timeframe()
    }

    fn occurred_at(&self) -> DateTime<Utc> {
        self.triggered_at()
    }
}

/// Strongly typed events passed between the data, ICT, strategy, and risk
/// layers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum SignalEvent {
    /// A venue declared a candle final.
    CandleClosed(Candle),
    /// Price swept a tracked liquidity level.
    LiquiditySwept(LiquiditySweep),
    /// A new FVG was created by a three-candle imbalance.
    FairValueGapDetected(FairValueGap),
    /// An existing FVG changed lifecycle state.
    FairValueGapUpdated(FairValueGap),
    /// A new order block was confirmed.
    OrderBlockDetected(OrderBlock),
    /// Raw structure logic detected an MSS; strategy code separately requires
    /// displacement and any other entry confluence.
    MarketStructureShiftDetected(MarketStructureShift),
    /// Continuation price action confirmed a break of structure.
    BreakOfStructureDetected(BreakOfStructure),
    /// A complete ICT setup reached its strategy-defined FVG entry.
    TradeIntentGenerated(Box<TradeIntent>),
}

impl SignalEvent {
    /// Validates event-level invariants not owned by an individual payload.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::OpenCandleInClosedEvent`] when a partial candle is
    /// wrapped as a closed-candle event.
    pub fn validate(&self) -> Result<(), ModelError> {
        if let Self::CandleClosed(candle) = self {
            if !candle.is_closed() {
                return Err(ModelError::OpenCandleInClosedEvent);
            }
        }

        Ok(())
    }
}

/// Common routing metadata exposed by all signal payloads.
pub trait DomainEvent {
    /// Instrument to which the event belongs.
    fn instrument(&self) -> &InstrumentId;

    /// Timeframe on which the event was detected.
    fn timeframe(&self) -> Timeframe;

    /// Market time at which the event became observable.
    fn occurred_at(&self) -> DateTime<Utc>;
}

impl DomainEvent for SignalEvent {
    fn instrument(&self) -> &InstrumentId {
        match self {
            Self::CandleClosed(candle) => candle.instrument(),
            Self::LiquiditySwept(sweep) => sweep.instrument(),
            Self::FairValueGapDetected(gap) | Self::FairValueGapUpdated(gap) => gap.instrument(),
            Self::OrderBlockDetected(order_block) => order_block.instrument(),
            Self::MarketStructureShiftDetected(shift) => shift.instrument(),
            Self::BreakOfStructureDetected(structure_break) => structure_break.instrument(),
            Self::TradeIntentGenerated(intent) => intent.instrument(),
        }
    }

    fn timeframe(&self) -> Timeframe {
        match self {
            Self::CandleClosed(candle) => candle.timeframe(),
            Self::LiquiditySwept(sweep) => sweep.timeframe(),
            Self::FairValueGapDetected(gap) | Self::FairValueGapUpdated(gap) => gap.timeframe(),
            Self::OrderBlockDetected(order_block) => order_block.timeframe(),
            Self::MarketStructureShiftDetected(shift) => shift.timeframe(),
            Self::BreakOfStructureDetected(structure_break) => structure_break.timeframe(),
            Self::TradeIntentGenerated(intent) => intent.timeframe(),
        }
    }

    fn occurred_at(&self) -> DateTime<Utc> {
        match self {
            Self::CandleClosed(candle) => candle.close_time(),
            Self::LiquiditySwept(sweep) => sweep.detected_at(),
            Self::FairValueGapDetected(gap) => gap.formed_at(),
            Self::FairValueGapUpdated(gap) => {
                gap.state().occurred_at().unwrap_or_else(|| gap.formed_at())
            }
            Self::OrderBlockDetected(order_block) => order_block.created_at(),
            Self::MarketStructureShiftDetected(shift) => shift.confirmed_at(),
            Self::BreakOfStructureDetected(structure_break) => structure_break.confirmed_at(),
            Self::TradeIntentGenerated(intent) => intent.triggered_at(),
        }
    }
}

/// Versioned event envelope suitable for an internal `mpsc` bus or durable log.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SignalEnvelope {
    schema_version: u16,
    event_id: Uuid,
    correlation_id: Option<Uuid>,
    emitted_at: DateTime<Utc>,
    event: SignalEvent,
}

impl SignalEnvelope {
    /// Current serialized envelope schema.
    pub const CURRENT_SCHEMA_VERSION: u16 = 1;

    /// Creates an envelope with a random event identifier.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] when event validation fails, the correlation ID
    /// is nil, or emission precedes the domain event.
    pub fn new(
        event: SignalEvent,
        emitted_at: DateTime<Utc>,
        correlation_id: Option<Uuid>,
    ) -> Result<Self, ModelError> {
        Self::with_id(Uuid::new_v4(), event, emitted_at, correlation_id)
    }

    /// Creates an envelope with a caller-provided identifier.
    ///
    /// This is useful for deterministic replay and tests.
    ///
    /// # Errors
    ///
    /// Returns a [`ModelError`] when an identifier or event is invalid, or when
    /// emission precedes the domain event.
    pub fn with_id(
        event_id: Uuid,
        event: SignalEvent,
        emitted_at: DateTime<Utc>,
        correlation_id: Option<Uuid>,
    ) -> Result<Self, ModelError> {
        Self::from_parts(
            Self::CURRENT_SCHEMA_VERSION,
            event_id,
            correlation_id,
            emitted_at,
            event,
        )
    }

    fn from_parts(
        schema_version: u16,
        event_id: Uuid,
        correlation_id: Option<Uuid>,
        emitted_at: DateTime<Utc>,
        event: SignalEvent,
    ) -> Result<Self, ModelError> {
        if schema_version == 0 || schema_version > Self::CURRENT_SCHEMA_VERSION {
            return Err(ModelError::UnsupportedSchemaVersion(schema_version));
        }
        if event_id.is_nil() {
            return Err(ModelError::NilIdentifier {
                model: "signal_envelope",
            });
        }
        if correlation_id.is_some_and(|id| id.is_nil()) {
            return Err(ModelError::NilIdentifier {
                model: "correlation",
            });
        }

        event.validate()?;
        if emitted_at < event.occurred_at() {
            return Err(ModelError::EventEmittedBeforeOccurrence);
        }

        Ok(Self {
            schema_version,
            event_id,
            correlation_id,
            emitted_at,
            event,
        })
    }

    /// Returns the serialized schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    /// Returns the unique event identifier.
    #[must_use]
    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }

    /// Returns the optional workflow correlation identifier.
    #[must_use]
    pub const fn correlation_id(&self) -> Option<Uuid> {
        self.correlation_id
    }

    /// Returns the UTC emission timestamp.
    #[must_use]
    pub const fn emitted_at(&self) -> DateTime<Utc> {
        self.emitted_at
    }

    /// Returns the strongly typed event payload.
    #[must_use]
    pub const fn event(&self) -> &SignalEvent {
        &self.event
    }
}

#[derive(Deserialize)]
struct SignalEnvelopeUnchecked {
    schema_version: u16,
    event_id: Uuid,
    correlation_id: Option<Uuid>,
    emitted_at: DateTime<Utc>,
    event: SignalEvent,
}

impl<'de> Deserialize<'de> for SignalEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = SignalEnvelopeUnchecked::deserialize(deserializer)?;
        Self::from_parts(
            value.schema_version,
            value.event_id,
            value.correlation_id,
            value.emitted_at,
            value.event,
        )
        .map_err(de::Error::custom)
    }
}

/// Validation failures produced while constructing domain models.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ModelError {
    /// A symbol was blank or contained surrounding whitespace.
    #[error("symbol must be non-empty and contain no surrounding whitespace: {0:?}")]
    InvalidSymbol(String),
    /// A model used a nil UUID where stable identity is required.
    #[error("{model} identifier must not be nil")]
    NilIdentifier {
        /// Model whose identifier was invalid.
        model: &'static str,
    },
    /// Candle interval was empty or reversed.
    #[error("candle open time must precede its exclusive close time")]
    InvalidCandleTime,
    /// A price was zero or negative.
    #[error("{field} price must be greater than zero")]
    NonPositivePrice {
        /// Name of the invalid price field.
        field: &'static str,
    },
    /// OHLC fields were inconsistent with the candle's high/low.
    #[error("OHLC values must satisfy low <= open, close <= high and low <= high")]
    InvalidOhlc,
    /// Volume was negative.
    #[error("volume cannot be negative")]
    NegativeVolume,
    /// Price-zone bounds were non-positive or reversed.
    #[error("price zone must satisfy 0 < lower < upper; received {lower}..{upper}")]
    InvalidPriceZone {
        /// Invalid lower price.
        lower: Decimal,
        /// Invalid upper price.
        upper: Decimal,
    },
    /// Pattern anchor candles were not strictly chronological.
    #[error("{model} anchors must be strictly chronological")]
    InvalidPatternAnchors {
        /// Pattern whose anchors were invalid.
        model: &'static str,
    },
    /// Pattern source candles were not exactly one declared interval apart.
    #[error("{model} anchors must be contiguous at the declared timeframe")]
    NonContiguousPatternAnchors {
        /// Pattern whose anchor spacing was invalid.
        model: &'static str,
    },
    /// Pattern detection time was not the close of its final source candle.
    #[error("{model} detection time must equal the final source candle close")]
    InvalidPatternDetectionTime {
        /// Pattern whose detection time was invalid.
        model: &'static str,
    },
    /// Detection time preceded the last candle needed for a pattern.
    #[error("{model} cannot be detected before its final anchor")]
    PatternBeforeFinalAnchor {
        /// Pattern with the invalid detection time.
        model: &'static str,
    },
    /// A lifecycle change predated zone creation or the preceding transition.
    #[error("zone lifecycle timestamps must be monotonic and follow creation")]
    InvalidLifecycleTimestamp,
    /// A terminal state was changed or a transition skipped required ordering.
    #[error("invalid zone transition from {from} to {to}")]
    InvalidZoneTransition {
        /// Current state.
        from: &'static str,
        /// Requested state.
        to: &'static str,
    },
    /// An invalidated zone had no useful explanation.
    #[error("zone invalidation reason must not be blank")]
    EmptyInvalidationReason,
    /// Swing confirmation did not occur after the candidate candle opened.
    #[error("swing confirmation must follow the candidate candle")]
    InvalidSwingTime,
    /// MSS/BOS direction, swing side, and break price disagreed.
    #[error(
        "bullish structure must break above a swing high; bearish structure below a swing low"
    )]
    InvalidStructureBreak,
    /// A structure event combined a swing from another instrument or timeframe.
    #[error("broken swing must belong to the structure event's instrument and timeframe")]
    StructureSeriesMismatch,
    /// MSS/BOS timestamps did not follow the broken swing chronologically.
    #[error("structure confirmation must occur after the broken swing")]
    InvalidStructureTime,
    /// Sweep price did not cross the reference on the declared liquidity side.
    #[error("buy-side sweep must exceed its reference; sell-side sweep must fall below it")]
    InvalidLiquiditySweep,
    /// Sweep detection preceded its candle.
    #[error("liquidity sweep detection cannot precede the sweep candle")]
    InvalidLiquiditySweepTime,
    /// Strategy evidence came from different instruments or timeframes.
    #[error("trade intent evidence and trigger candle must belong to one series")]
    TradeIntentSeriesMismatch,
    /// MSS or FVG direction disagreed with the intended position direction.
    #[error("trade intent, MSS, and FVG directions must agree")]
    TradeIntentDirectionMismatch,
    /// The swept liquidity side cannot precede the requested trade direction.
    #[error("bullish intent requires an SSL sweep; bearish intent requires a BSL sweep")]
    TradeIntentSweepSideMismatch,
    /// The MSS candle was not the FVG's displacement candle.
    #[error("MSS confirming candle must be the FVG middle candle")]
    TradeIntentDisplacementMismatch,
    /// Sweep, MSS, and FVG timestamps were not chronological.
    #[error("trade intent evidence must be ordered sweep, MSS, then FVG")]
    InvalidTradeIntentSequence,
    /// The trigger interval did not match the declared timeframe.
    #[error("trade intent trigger candle duration must equal its timeframe")]
    InvalidTradeIntentTriggerDuration,
    /// A partial candle was used to trigger a trade intent.
    #[error("trade intent requires a final trigger candle")]
    OpenTradeIntentTrigger,
    /// The trigger did not occur after the FVG became observable.
    #[error("trade intent trigger must follow the completed FVG")]
    InvalidTradeIntentTriggerTime,
    /// Price did not reach the entry derived from the FVG.
    #[error("trade intent trigger candle did not reach the derived entry price")]
    TradeIntentEntryNotReached,
    /// The trigger closed beyond the FVG's far boundary.
    #[error("trade intent trigger candle closed through the FVG")]
    TradeIntentGapViolated,
    /// The sweep extreme cannot protect the intended entry direction.
    #[error("trade intent sweep extreme must lie beyond the entry on the stop side")]
    InvalidTradeIntentStopAnchor,
    /// An invalidated FVG cannot be used for entry.
    #[error("trade intent cannot use an invalidated FVG")]
    TradeIntentInvalidatedGap,
    /// The intent deadline did not follow the trigger candle.
    #[error("trade intent expiry must follow the trigger candle close")]
    InvalidTradeIntentExpiry,
    /// A partial candle was mislabeled as a closed-candle event.
    #[error("candle_closed event requires a final candle")]
    OpenCandleInClosedEvent,
    /// An envelope used an unknown or invalid schema version.
    #[error("unsupported signal envelope schema version {0}")]
    UnsupportedSchemaVersion(u16),
    /// The envelope emission timestamp preceded the domain event.
    #[error("signal envelope cannot be emitted before its event occurred")]
    EventEmittedBeforeOccurrence,
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use serde_json::json;

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    fn at_minute(minute: i64) -> Result<DateTime<Utc>, io::Error> {
        DateTime::from_timestamp(minute * 60, 0)
            .ok_or_else(|| io::Error::other("test timestamp is outside chrono's range"))
    }

    fn instrument() -> Result<InstrumentId, ModelError> {
        InstrumentId::new(
            Exchange::Binance,
            MarketKind::LinearPerpetual,
            "BTCUSDT",
            "BTC-USDT-PERP",
        )
    }

    fn closed_candle() -> Result<Candle, Box<dyn Error>> {
        Ok(Candle::new(CandleInput {
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            open_time: at_minute(1)?,
            close_time: at_minute(2)?,
            open: Decimal::new(10_000, 2),
            high: Decimal::new(10_200, 2),
            low: Decimal::new(9_900, 2),
            close: Decimal::new(10_100, 2),
            volume: Decimal::new(25_000, 3),
            volume_unit: VolumeUnit::BaseAsset,
            trade_count: Some(42),
            is_closed: true,
        })?)
    }

    fn bullish_gap() -> Result<FairValueGap, Box<dyn Error>> {
        Ok(FairValueGap::new(FairValueGapInput {
            id: Uuid::from_u128(1),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            zone: PriceZone::new(Decimal::new(10_000, 2), Decimal::new(10_200, 2))?,
            formed_from: [at_minute(1)?, at_minute(2)?, at_minute(3)?],
            formed_at: at_minute(4)?,
        })?)
    }

    fn bullish_trade_intent_input() -> Result<TradeIntentInput, Box<dyn Error>> {
        let broken_swing = SwingPoint::new(
            instrument()?,
            Timeframe::M1,
            SwingKind::High,
            Decimal::new(10_100, 2),
            at_minute(-1)?,
            at_minute(1)?,
            FractalModel::ThreeCandle,
        )?;
        let market_structure_shift = MarketStructureShift::new(MarketStructureShiftInput {
            id: Uuid::from_u128(12),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            broken_swing,
            break_price: Decimal::new(10_300, 2),
            break_basis: BreakBasis::Close,
            confirming_candle_open_time: at_minute(2)?,
            confirmed_at: at_minute(3)?,
        })?;
        let liquidity_sweep = LiquiditySweep::new(LiquiditySweepInput {
            id: Uuid::from_u128(11),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            side: LiquiditySide::SellSide,
            reference_price: Decimal::new(9_900, 2),
            sweep_price: Decimal::new(9_800, 2),
            sweep_candle_open_time: at_minute(0)?,
            detected_at: at_minute(1)?,
        })?;
        let trigger_candle = Candle::new(CandleInput {
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            open_time: at_minute(4)?,
            close_time: at_minute(5)?,
            open: Decimal::new(10_300, 2),
            high: Decimal::new(10_400, 2),
            low: Decimal::new(10_100, 2),
            close: Decimal::new(10_150, 2),
            volume: Decimal::ZERO,
            volume_unit: VolumeUnit::Contracts,
            trade_count: None,
            is_closed: true,
        })?;

        Ok(TradeIntentInput {
            id: Uuid::from_u128(13),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            liquidity_sweep,
            market_structure_shift,
            fair_value_gap: bullish_gap()?,
            entry_mode: FvgEntryMode::NearBoundary,
            trigger_candle,
            expires_at: at_minute(10)?,
        })
    }

    #[test]
    fn fvg_requires_exact_timeframe_contiguous_anchors_and_final_close() -> TestResult {
        let valid = FairValueGapInput {
            id: Uuid::from_u128(21),
            instrument: instrument()?,
            timeframe: Timeframe::M5,
            direction: Direction::Bullish,
            zone: PriceZone::new(Decimal::new(10_000, 2), Decimal::new(10_200, 2))?,
            formed_from: [at_minute(0)?, at_minute(5)?, at_minute(10)?],
            formed_at: at_minute(15)?,
        };
        assert!(FairValueGap::new(valid.clone()).is_ok());

        let invalid_anchor_sets = [
            [at_minute(0)?, at_minute(4)?, at_minute(10)?],
            [at_minute(0)?, at_minute(6)?, at_minute(10)?],
            [at_minute(0)?, at_minute(5)?, at_minute(9)?],
            [at_minute(0)?, at_minute(5)?, at_minute(11)?],
        ];
        for formed_from in invalid_anchor_sets {
            let mut non_contiguous = valid.clone();
            non_contiguous.formed_from = formed_from;
            assert_eq!(
                FairValueGap::new(non_contiguous),
                Err(ModelError::NonContiguousPatternAnchors { model: "fvg" })
            );
        }

        for formed_at_minute in [14, 16] {
            let mut wrong_detection_time = valid.clone();
            wrong_detection_time.formed_at = at_minute(formed_at_minute)?;
            assert_eq!(
                FairValueGap::new(wrong_detection_time),
                Err(ModelError::InvalidPatternDetectionTime { model: "fvg" })
            );
        }
        Ok(())
    }

    #[test]
    fn trade_intent_derives_entry_and_routes_event_metadata() -> TestResult {
        let intent = TradeIntent::new(bullish_trade_intent_input()?)?;

        assert_eq!(intent.id(), Uuid::from_u128(13));
        assert_eq!(intent.entry_price(), Decimal::new(10_200, 2));
        assert_eq!(intent.stop_anchor_price(), Decimal::new(9_800, 2));
        assert_eq!(intent.triggered_at(), at_minute(5)?);

        let event = SignalEvent::TradeIntentGenerated(Box::new(intent));
        event.validate()?;
        assert_eq!(event.instrument(), &instrument()?);
        assert_eq!(event.timeframe(), Timeframe::M1);
        assert_eq!(event.occurred_at(), at_minute(5)?);
        Ok(())
    }

    #[test]
    fn trade_intent_round_trip_revalidates_tampered_payloads() -> TestResult {
        let intent = TradeIntent::new(bullish_trade_intent_input()?)?;
        let value = serde_json::to_value(&intent)?;
        let decoded: TradeIntent = serde_json::from_value(value.clone())?;
        assert_eq!(decoded, intent);

        let mut tampered = value;
        tampered["expires_at"] = json!(at_minute(5)?);
        let result = serde_json::from_value::<TradeIntent>(tampered);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn trade_intent_requires_the_directional_sweep_side() -> TestResult {
        let mut input = bullish_trade_intent_input()?;
        input.liquidity_sweep = LiquiditySweep::new(LiquiditySweepInput {
            id: Uuid::from_u128(14),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            side: LiquiditySide::BuySide,
            reference_price: Decimal::new(10_500, 2),
            sweep_price: Decimal::new(10_600, 2),
            sweep_candle_open_time: at_minute(0)?,
            detected_at: at_minute(1)?,
        })?;

        let result = TradeIntent::new(input);
        assert_eq!(result, Err(ModelError::TradeIntentSweepSideMismatch));
        Ok(())
    }

    #[test]
    fn trade_intent_binds_mss_to_fvg_displacement_candle() -> TestResult {
        let mut input = bullish_trade_intent_input()?;
        input.market_structure_shift = MarketStructureShift::new(MarketStructureShiftInput {
            id: Uuid::from_u128(15),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            broken_swing: input.market_structure_shift.broken_swing().clone(),
            break_price: Decimal::new(10_300, 2),
            break_basis: BreakBasis::Close,
            confirming_candle_open_time: at_minute(3)?,
            confirmed_at: at_minute(4)?,
        })?;

        let result = TradeIntent::new(input);
        assert_eq!(result, Err(ModelError::TradeIntentDisplacementMismatch));
        Ok(())
    }

    #[test]
    fn trade_intent_accepts_mitigated_gap_and_exact_ce() -> TestResult {
        let mut input = bullish_trade_intent_input()?;
        input.fair_value_gap.mark_mitigated(at_minute(4)?)?;
        input.entry_mode = FvgEntryMode::ConsequentEncroachment;

        let intent = TradeIntent::new(input)?;
        assert!(intent.fair_value_gap().is_mitigated());
        assert_eq!(intent.entry_price(), Decimal::new(10_100, 2));
        Ok(())
    }

    #[test]
    fn trade_intent_trigger_range_must_bracket_the_derived_entry() -> TestResult {
        let cases = [
            (
                Decimal::new(10_000, 2),
                Decimal::new(10_090, 2),
                Decimal::new(10_050, 2),
            ),
            (
                Decimal::new(10_110, 2),
                Decimal::new(10_300, 2),
                Decimal::new(10_200, 2),
            ),
        ];

        for (low, high, close) in cases {
            let mut input = bullish_trade_intent_input()?;
            input.entry_mode = FvgEntryMode::ConsequentEncroachment;
            input.trigger_candle = Candle::new(CandleInput {
                instrument: instrument()?,
                timeframe: Timeframe::M1,
                open_time: at_minute(4)?,
                close_time: at_minute(5)?,
                open: close,
                high,
                low,
                close,
                volume: Decimal::ZERO,
                volume_unit: VolumeUnit::Contracts,
                trade_count: None,
                is_closed: true,
            })?;

            assert_eq!(
                TradeIntent::new(input),
                Err(ModelError::TradeIntentEntryNotReached)
            );
        }
        Ok(())
    }

    #[test]
    fn trade_intent_rejects_invalidated_gap() -> TestResult {
        let mut input = bullish_trade_intent_input()?;
        input
            .fair_value_gap
            .invalidate(at_minute(4)?, "closed through zone")?;

        let result = TradeIntent::new(input);
        assert_eq!(result, Err(ModelError::TradeIntentInvalidatedGap));
        Ok(())
    }

    #[test]
    fn candle_serialization_keeps_decimal_values_as_strings() -> TestResult {
        let candle = closed_candle()?;
        let value = serde_json::to_value(&candle)?;

        assert!(value.get("open").is_some_and(serde_json::Value::is_string));
        assert!(value
            .get("volume")
            .is_some_and(serde_json::Value::is_string));

        let decoded: Candle = serde_json::from_value(value)?;
        assert_eq!(decoded, candle);
        Ok(())
    }

    #[test]
    fn candle_rejects_close_outside_high_low_range() -> TestResult {
        let result = Candle::new(CandleInput {
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            open_time: at_minute(1)?,
            close_time: at_minute(2)?,
            open: Decimal::new(100, 0),
            high: Decimal::new(101, 0),
            low: Decimal::new(99, 0),
            close: Decimal::new(102, 0),
            volume: Decimal::ONE,
            volume_unit: VolumeUnit::Contracts,
            trade_count: None,
            is_closed: true,
        });

        assert_eq!(result, Err(ModelError::InvalidOhlc));
        Ok(())
    }

    #[test]
    fn price_zone_midpoint_is_exact_decimal() -> TestResult {
        let zone = PriceZone::new(Decimal::new(10_001, 2), Decimal::new(10_003, 2))?;
        assert_eq!(zone.midpoint(), Decimal::new(10_002, 2));
        Ok(())
    }

    #[test]
    fn fvg_lifecycle_is_monotonic_and_terminal() -> TestResult {
        let mut gap = bullish_gap()?;
        gap.mark_touched(at_minute(5)?)?;
        gap.mark_mitigated(at_minute(6)?)?;

        assert!(gap.is_mitigated());
        let result = gap.invalidate(at_minute(7)?, "closed through zone");
        assert!(matches!(
            result,
            Err(ModelError::InvalidZoneTransition {
                from: "mitigated",
                to: "invalidated"
            })
        ));
        Ok(())
    }

    #[test]
    fn bearish_mss_must_break_a_swing_low() -> TestResult {
        let swing_high = SwingPoint::new(
            instrument()?,
            Timeframe::M1,
            SwingKind::High,
            Decimal::new(100, 0),
            at_minute(1)?,
            at_minute(3)?,
            FractalModel::ThreeCandle,
        )?;
        let result = MarketStructureShift::new(MarketStructureShiftInput {
            id: Uuid::from_u128(2),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            direction: Direction::Bearish,
            broken_swing: swing_high,
            break_price: Decimal::new(99, 0),
            break_basis: BreakBasis::Close,
            confirming_candle_open_time: at_minute(4)?,
            confirmed_at: at_minute(5)?,
        });

        assert_eq!(result, Err(ModelError::InvalidStructureBreak));
        Ok(())
    }

    #[test]
    fn bullish_bos_round_trips_and_routes_domain_metadata() -> TestResult {
        let broken_swing = SwingPoint::new(
            instrument()?,
            Timeframe::M5,
            SwingKind::High,
            Decimal::new(100, 0),
            at_minute(0)?,
            at_minute(10)?,
            FractalModel::ThreeCandle,
        )?;
        let bos: Bos = BreakOfStructure::new(BreakOfStructureInput {
            id: Uuid::from_u128(5),
            instrument: instrument()?,
            timeframe: Timeframe::M5,
            direction: Direction::Bullish,
            broken_swing: broken_swing.clone(),
            break_price: Decimal::new(101, 0),
            break_basis: BreakBasis::Close,
            confirming_candle_open_time: at_minute(10)?,
            confirmed_at: at_minute(15)?,
        })?;

        assert_eq!(bos.id(), Uuid::from_u128(5));
        assert_eq!(bos.instrument(), &instrument()?);
        assert_eq!(bos.timeframe(), Timeframe::M5);
        assert_eq!(bos.direction(), Direction::Bullish);
        assert_eq!(bos.broken_swing(), &broken_swing);
        assert_eq!(bos.break_price(), Decimal::new(101, 0));
        assert_eq!(bos.break_basis(), BreakBasis::Close);
        assert_eq!(bos.confirming_candle_open_time(), at_minute(10)?);
        assert_eq!(bos.confirmed_at(), at_minute(15)?);

        let serialized = serde_json::to_value(&bos)?;
        assert!(serialized
            .get("break_price")
            .is_some_and(serde_json::Value::is_string));
        let decoded: BreakOfStructure = serde_json::from_value(serialized)?;
        assert_eq!(decoded, bos);

        let event = SignalEvent::BreakOfStructureDetected(bos);
        event.validate()?;
        assert_eq!(event.instrument(), &instrument()?);
        assert_eq!(event.timeframe(), Timeframe::M5);
        assert_eq!(event.occurred_at(), at_minute(15)?);
        Ok(())
    }

    #[test]
    fn bearish_bos_must_break_a_swing_low() -> TestResult {
        let swing_high = SwingPoint::new(
            instrument()?,
            Timeframe::M15,
            SwingKind::High,
            Decimal::new(100, 0),
            at_minute(0)?,
            at_minute(45)?,
            FractalModel::FiveCandle,
        )?;
        let result = BreakOfStructure::new(BreakOfStructureInput {
            id: Uuid::from_u128(6),
            instrument: instrument()?,
            timeframe: Timeframe::M15,
            direction: Direction::Bearish,
            broken_swing: swing_high,
            break_price: Decimal::new(99, 0),
            break_basis: BreakBasis::Wick,
            confirming_candle_open_time: at_minute(45)?,
            confirmed_at: at_minute(60)?,
        });

        assert_eq!(result, Err(ModelError::InvalidStructureBreak));
        Ok(())
    }

    #[test]
    fn bos_confirmation_must_follow_the_broken_swing() -> TestResult {
        let swing_low = SwingPoint::new(
            instrument()?,
            Timeframe::M1,
            SwingKind::Low,
            Decimal::new(100, 0),
            at_minute(2)?,
            at_minute(4)?,
            FractalModel::ThreeCandle,
        )?;
        let result = BreakOfStructure::new(BreakOfStructureInput {
            id: Uuid::from_u128(7),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            direction: Direction::Bearish,
            broken_swing: swing_low,
            break_price: Decimal::new(99, 0),
            break_basis: BreakBasis::Close,
            confirming_candle_open_time: at_minute(3)?,
            confirmed_at: at_minute(4)?,
        });

        assert_eq!(result, Err(ModelError::InvalidStructureTime));
        Ok(())
    }

    #[test]
    fn structure_rejects_a_swing_from_another_series() -> TestResult {
        let swing_high = SwingPoint::new(
            instrument()?,
            Timeframe::M5,
            SwingKind::High,
            Decimal::new(100, 0),
            at_minute(1)?,
            at_minute(11)?,
            FractalModel::ThreeCandle,
        )?;
        let result = BreakOfStructure::new(BreakOfStructureInput {
            id: Uuid::from_u128(8),
            instrument: instrument()?,
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            broken_swing: swing_high,
            break_price: Decimal::new(101, 0),
            break_basis: BreakBasis::Close,
            confirming_candle_open_time: at_minute(11)?,
            confirmed_at: at_minute(12)?,
        });

        assert_eq!(result, Err(ModelError::StructureSeriesMismatch));
        Ok(())
    }

    #[test]
    fn decimal_deserializer_rejects_json_numbers() {
        let result = serde_json::from_value::<PriceZone>(json!({
            "lower": 100,
            "upper": 101
        }));
        assert!(result.is_err());
    }

    #[test]
    fn signal_envelope_round_trips_with_validation() -> TestResult {
        let candle = closed_candle()?;
        let envelope = SignalEnvelope::with_id(
            Uuid::from_u128(3),
            SignalEvent::CandleClosed(candle),
            at_minute(3)?,
            Some(Uuid::from_u128(4)),
        )?;

        let serialized = serde_json::to_vec(&envelope)?;
        let decoded: SignalEnvelope = serde_json::from_slice(&serialized)?;
        assert_eq!(decoded, envelope);
        Ok(())
    }

    #[test]
    fn closed_event_rejects_partial_candle() -> TestResult {
        let candle = Candle::new(CandleInput {
            instrument: instrument()?,
            timeframe: Timeframe::M5,
            open_time: at_minute(5)?,
            close_time: at_minute(10)?,
            open: Decimal::new(100, 0),
            high: Decimal::new(101, 0),
            low: Decimal::new(99, 0),
            close: Decimal::new(100, 0),
            volume: Decimal::ZERO,
            volume_unit: VolumeUnit::Contracts,
            trade_count: None,
            is_closed: false,
        })?;

        let result = SignalEvent::CandleClosed(candle).validate();
        assert_eq!(result, Err(ModelError::OpenCandleInClosedEvent));
        Ok(())
    }
}
