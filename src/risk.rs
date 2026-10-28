//! Deterministic account-risk approval and daily drawdown protection.
//!
//! The risk layer is deliberately independent from exchange I/O. It consumes a
//! validated [`TradeIntent`], a venue contract specification, the current
//! account equity, and an opposing-liquidity target. All price and quantity
//! calculations use [`Decimal`]; quantities are always rounded down so a venue
//! step cannot increase the configured risk budget.

use std::collections::HashMap;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::models::{Direction, InstrumentId, LiquiditySide, MarketKind, TradeIntent};

mod checkpoint;
pub use checkpoint::{RiskCheckpoint, RiskPlanCheckpoint};

const MINIMUM_ALLOWED_RISK_REWARD: Decimal = Decimal::TWO;
const APPROVAL_NAMESPACE: Uuid = Uuid::from_bytes([
    0xd4, 0x5e, 0x39, 0x70, 0x91, 0x3d, 0x4f, 0xb2, 0xa2, 0x52, 0xb9, 0x67, 0x86, 0x14, 0x25, 0x17,
]);

/// Immutable account-level risk limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskConfig {
    account_risk_fraction: Decimal,
    max_portfolio_risk_fraction: Decimal,
    max_daily_drawdown_fraction: Decimal,
    minimum_risk_reward: Decimal,
    stop_buffer_ticks: u32,
    max_equity_snapshot_age: Duration,
}

impl RiskConfig {
    /// Creates validated account-level risk limits.
    ///
    /// Fractions use decimal form, so one percent is `0.01`. At least one stop
    /// buffer tick is mandatory and the configured minimum reward-to-risk ratio
    /// cannot be below `2`.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError`] when a fraction is outside `(0, 1]`, the minimum
    /// reward-to-risk ratio is below two, or the stop buffer is zero.
    pub fn new(
        account_risk_fraction: Decimal,
        max_daily_drawdown_fraction: Decimal,
        minimum_risk_reward: Decimal,
        stop_buffer_ticks: u32,
    ) -> Result<Self, RiskError> {
        Self::new_with_safety_limits(
            account_risk_fraction,
            Decimal::new(5, 2),
            max_daily_drawdown_fraction,
            minimum_risk_reward,
            stop_buffer_ticks,
            Duration::seconds(5),
        )
    }

    /// Creates risk limits including portfolio exposure and equity freshness.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError`] when a fraction is outside `(0, 1]`, the portfolio
    /// cap is below per-trade risk, the reward-to-risk ratio is below two, the
    /// stop buffer is zero, or equity freshness is not positive.
    pub fn new_with_safety_limits(
        account_risk_fraction: Decimal,
        max_portfolio_risk_fraction: Decimal,
        max_daily_drawdown_fraction: Decimal,
        minimum_risk_reward: Decimal,
        stop_buffer_ticks: u32,
        max_equity_snapshot_age: Duration,
    ) -> Result<Self, RiskError> {
        validate_fraction("account_risk_fraction", account_risk_fraction)?;
        validate_fraction("max_portfolio_risk_fraction", max_portfolio_risk_fraction)?;
        validate_fraction("max_daily_drawdown_fraction", max_daily_drawdown_fraction)?;
        if max_portfolio_risk_fraction < account_risk_fraction {
            return Err(RiskError::PortfolioRiskBelowTradeRisk);
        }
        if minimum_risk_reward < MINIMUM_ALLOWED_RISK_REWARD {
            return Err(RiskError::MinimumRiskRewardBelowTwo);
        }
        if stop_buffer_ticks == 0 {
            return Err(RiskError::ZeroStopBuffer);
        }
        if max_equity_snapshot_age <= Duration::zero() {
            return Err(RiskError::NonPositiveEquityFreshness);
        }

        Ok(Self {
            account_risk_fraction,
            max_portfolio_risk_fraction,
            max_daily_drawdown_fraction,
            minimum_risk_reward,
            stop_buffer_ticks,
            max_equity_snapshot_age,
        })
    }

    /// Fraction of current equity that one approved trade may lose.
    #[must_use]
    pub const fn account_risk_fraction(&self) -> Decimal {
        self.account_risk_fraction
    }

    /// Maximum combined planned loss of all active reservations as equity share.
    #[must_use]
    pub const fn max_portfolio_risk_fraction(&self) -> Decimal {
        self.max_portfolio_risk_fraction
    }

    /// Drawdown from UTC start-of-day equity that trips the breaker.
    #[must_use]
    pub const fn max_daily_drawdown_fraction(&self) -> Decimal {
        self.max_daily_drawdown_fraction
    }

    /// Smallest reward-to-risk ratio accepted after tick rounding.
    #[must_use]
    pub const fn minimum_risk_reward(&self) -> Decimal {
        self.minimum_risk_reward
    }

    /// Number of ticks placed beyond the liquidity-sweep extreme.
    #[must_use]
    pub const fn stop_buffer_ticks(&self) -> u32 {
        self.stop_buffer_ticks
    }

    /// Maximum acceptable age of the sizing equity snapshot.
    #[must_use]
    pub const fn max_equity_snapshot_age(&self) -> Duration {
        self.max_equity_snapshot_age
    }
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            account_risk_fraction: Decimal::new(1, 2),
            max_portfolio_risk_fraction: Decimal::new(5, 2),
            max_daily_drawdown_fraction: Decimal::new(5, 2),
            minimum_risk_reward: Decimal::TWO,
            stop_buffer_ticks: 1,
            max_equity_snapshot_age: Duration::seconds(5),
        }
    }
}

/// Venue constraints for a linear, quote-margined futures contract.
///
/// `quote_value_per_price_unit` converts one unit of venue quantity and one
/// price-unit move into quote-currency `PnL`. It is normally `1` when quantity is
/// expressed in base asset. Inverse contracts are rejected because their `PnL`
/// is not linear in price and cannot safely use the requested sizing formula.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentRiskSpec {
    instrument: InstrumentId,
    price_tick: Decimal,
    quantity_step: Decimal,
    minimum_quantity: Decimal,
    maximum_quantity: Option<Decimal>,
    quote_value_per_price_unit: Decimal,
}

impl InstrumentRiskSpec {
    /// Creates a validated linear-contract risk specification.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError`] for a non-linear market, non-positive constraint,
    /// maximum quantity below the minimum quantity, or a symbol longer than
    /// 128 bytes or containing control characters.
    pub fn new(
        instrument: InstrumentId,
        price_tick: Decimal,
        quantity_step: Decimal,
        minimum_quantity: Decimal,
        maximum_quantity: Option<Decimal>,
        quote_value_per_price_unit: Decimal,
    ) -> Result<Self, RiskError> {
        if instrument.market != MarketKind::LinearPerpetual {
            return Err(RiskError::UnsupportedMarketKind(instrument.market));
        }
        for symbol in [&instrument.venue_symbol, &instrument.canonical_symbol] {
            if symbol.as_str().len() > 128 || symbol.as_str().chars().any(char::is_control) {
                return Err(RiskError::InvalidInstrumentSpec("invalid symbol"));
            }
        }
        for (field, value) in [
            ("price_tick", price_tick),
            ("quantity_step", quantity_step),
            ("minimum_quantity", minimum_quantity),
            ("quote_value_per_price_unit", quote_value_per_price_unit),
        ] {
            if value <= Decimal::ZERO {
                return Err(RiskError::NonPositiveContractConstraint { field });
            }
        }
        if maximum_quantity
            .is_some_and(|maximum| maximum <= Decimal::ZERO || maximum < minimum_quantity)
        {
            return Err(RiskError::InvalidQuantityRange);
        }

        Ok(Self {
            instrument,
            price_tick,
            quantity_step,
            minimum_quantity,
            maximum_quantity,
            quote_value_per_price_unit,
        })
    }

    /// Instrument to which these venue constraints apply.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Smallest valid price increment.
    #[must_use]
    pub const fn price_tick(&self) -> Decimal {
        self.price_tick
    }

    /// Smallest valid quantity increment.
    #[must_use]
    pub const fn quantity_step(&self) -> Decimal {
        self.quantity_step
    }

    /// Venue minimum order quantity.
    #[must_use]
    pub const fn minimum_quantity(&self) -> Decimal {
        self.minimum_quantity
    }

    /// Venue maximum order quantity, when published.
    #[must_use]
    pub const fn maximum_quantity(&self) -> Option<Decimal> {
        self.maximum_quantity
    }

    /// Quote-currency `PnL` multiplier for one quantity and one price unit.
    #[must_use]
    pub const fn quote_value_per_price_unit(&self) -> Decimal {
        self.quote_value_per_price_unit
    }
}

/// Typed opposing-liquidity objective selected by market analysis.
///
/// A target deliberately has no timeframe restriction: higher-timeframe
/// liquidity may be used for a lower-timeframe entry, but instrument identity,
/// side, positive price, and observation time are always explicit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LiquidityTarget {
    id: Uuid,
    instrument: InstrumentId,
    side: LiquiditySide,
    #[serde(with = "rust_decimal::serde::str")]
    price: Decimal,
    observed_at: DateTime<Utc>,
}

impl LiquidityTarget {
    /// Creates a validated liquidity target.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError`] for a nil identifier or non-positive price.
    pub fn new(
        id: Uuid,
        instrument: InstrumentId,
        side: LiquiditySide,
        price: Decimal,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, RiskError> {
        if id.is_nil() {
            return Err(RiskError::NilLiquidityTargetIdentifier);
        }
        if price <= Decimal::ZERO {
            return Err(RiskError::NonPositiveTarget);
        }
        Ok(Self {
            id,
            instrument,
            side,
            price,
            observed_at,
        })
    }

    /// Stable target identifier.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Instrument whose resting liquidity is targeted.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Buy-side target for a long or sell-side target for a short.
    #[must_use]
    pub const fn side(&self) -> LiquiditySide {
        self.side
    }

    /// Exact target price before venue tick rounding.
    #[must_use]
    pub const fn price(&self) -> Decimal {
        self.price
    }

    /// Time at which analysis observed this target.
    #[must_use]
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
}

/// Observable daily circuit-breaker status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CircuitBreakerState {
    /// No valid equity snapshot has established the UTC daily baseline.
    AwaitingEquity,
    /// Trading remains allowed for the current UTC day.
    Armed {
        /// UTC date owning the baseline.
        trading_day: NaiveDate,
        /// First observed equity on this UTC date.
        #[serde(with = "rust_decimal::serde::str")]
        opening_equity: Decimal,
        /// Equity from the latest accepted snapshot.
        #[serde(with = "rust_decimal::serde::str")]
        current_equity: Decimal,
        /// Current decline from opening equity, or zero while above it.
        #[serde(with = "rust_decimal::serde::str")]
        drawdown_fraction: Decimal,
    },
    /// New entries are latched off until the next UTC date.
    Tripped {
        /// UTC date on which trading was stopped.
        trading_day: NaiveDate,
        /// First observed equity on this UTC date.
        #[serde(with = "rust_decimal::serde::str")]
        opening_equity: Decimal,
        /// Equity from the latest accepted snapshot.
        #[serde(with = "rust_decimal::serde::str")]
        current_equity: Decimal,
        /// Current decline from opening equity.
        #[serde(with = "rust_decimal::serde::str")]
        drawdown_fraction: Decimal,
        /// First snapshot time at which the threshold was reached.
        tripped_at: DateTime<Utc>,
    },
}

impl CircuitBreakerState {
    /// Returns whether new risk must be rejected.
    #[must_use]
    pub const fn is_tripped(&self) -> bool {
        matches!(self, Self::Tripped { .. })
    }

    /// Returns the UTC trading day after a baseline exists.
    #[must_use]
    pub const fn trading_day(&self) -> Option<NaiveDate> {
        match self {
            Self::AwaitingEquity => None,
            Self::Armed { trading_day, .. } | Self::Tripped { trading_day, .. } => {
                Some(*trading_day)
            }
        }
    }
}

/// Fully sized order parameters approved for the execution layer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RiskPlan {
    approval_id: Uuid,
    intent_id: Uuid,
    target_id: Uuid,
    instrument: InstrumentId,
    direction: Direction,
    #[serde(with = "rust_decimal::serde::str")]
    entry_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    stop_loss_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    take_profit_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    risk_budget: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    planned_loss: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    planned_reward: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    risk_reward_ratio: Decimal,
    approved_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    // Preserve the approved venue contract even after released specs change.
    #[serde(skip)]
    approved_spec: InstrumentRiskSpec,
    #[serde(skip)]
    approved_config: RiskConfig,
    #[serde(skip)]
    approved_equity: Decimal,
    #[serde(skip)]
    approved_equity_at: DateTime<Utc>,
    #[serde(skip)]
    approved_fingerprint: Uuid,
}

impl RiskPlan {
    /// Exact venue specification used during approval (for internal simulators).
    pub(crate) const fn approved_spec(&self) -> &InstrumentRiskSpec {
        &self.approved_spec
    }

    /// Deterministic identifier for this intent, equity snapshot, and sizing.
    #[must_use]
    pub const fn approval_id(&self) -> Uuid {
        self.approval_id
    }

    /// Strategy intent that received risk approval.
    #[must_use]
    pub const fn intent_id(&self) -> Uuid {
        self.intent_id
    }

    /// Opposing-liquidity target used to derive take profit.
    #[must_use]
    pub const fn target_id(&self) -> Uuid {
        self.target_id
    }

    /// Approved instrument.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Approved position direction.
    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
    }

    /// Tick-aligned limit-entry price.
    #[must_use]
    pub const fn entry_price(&self) -> Decimal {
        self.entry_price
    }

    /// Step-aligned quantity that cannot exceed the risk budget.
    #[must_use]
    pub const fn quantity(&self) -> Decimal {
        self.quantity
    }

    /// Tick-aligned stop strictly beyond the sweep extreme.
    #[must_use]
    pub const fn stop_loss_price(&self) -> Decimal {
        self.stop_loss_price
    }

    /// Tick-aligned opposing-liquidity take-profit trigger.
    #[must_use]
    pub const fn take_profit_price(&self) -> Decimal {
        self.take_profit_price
    }

    /// Maximum configured gross price risk for this trade.
    #[must_use]
    pub const fn risk_budget(&self) -> Decimal {
        self.risk_budget
    }

    /// Gross loss at the stop before fees and execution slippage.
    #[must_use]
    pub const fn planned_loss(&self) -> Decimal {
        self.planned_loss
    }

    /// Gross reward at the target before fees and execution slippage.
    #[must_use]
    pub const fn planned_reward(&self) -> Decimal {
        self.planned_reward
    }

    /// Reward divided by risk after all venue rounding.
    #[must_use]
    pub const fn risk_reward_ratio(&self) -> Decimal {
        self.risk_reward_ratio
    }

    /// Time at which risk approval was calculated.
    #[must_use]
    pub const fn approved_at(&self) -> DateTime<Utc> {
        self.approved_at
    }

    /// Deadline inherited from the strategy intent.
    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

/// Result of assessing a strategy intent.
///
/// Only [`Self::Approved`] authorizes a new execution request. A replay carries
/// identifiers only, so an at-least-once command transport cannot accidentally
/// place the same order twice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TradeAssessment {
    /// A new reservation was created and may be sent to execution exactly once.
    Approved(Box<RiskPlan>),
    /// The exact request already owns an active reservation.
    Replay {
        /// Existing approval identifier.
        approval_id: Uuid,
        /// Strategy intent owning the approval.
        intent_id: Uuid,
    },
}

/// Execution lifecycle state associated with one approved reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservationStatus {
    /// Entry order may still fill; planned loss remains reserved.
    Pending,
    /// Entry filled and position risk remains reserved until full closure.
    PositionOpen,
    /// Entry was cancelled before fill or the filled position was fully closed.
    Released,
}

/// Typed execution event accepted by the risk lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReservationEvent {
    /// The pending entry filled.
    EntryFilled,
    /// New cumulative entry-fill evidence restored previously closed exposure.
    ///
    /// Only an execution coordinator that observed a positive cumulative fill
    /// increase may emit this event. It is not an ordinary entry-fill replay.
    EntryExposureReopened,
    /// The pending entry was definitively cancelled without a fill.
    EntryCancelled,
    /// The filled position was fully closed.
    PositionClosed,
}

/// Observable result of applying an execution lifecycle event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReservationTransition {
    /// Resulting lifecycle state.
    pub status: ReservationStatus,
    /// Whether this was an exact, side-effect-free replay of the last event.
    pub replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DailyEquity {
    trading_day: NaiveDate,
    opening_equity: Decimal,
    current_equity: Decimal,
    drawdown_fraction: Decimal,
    tripped_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
struct RiskReservation {
    plan: RiskPlan,
    fingerprint: Uuid,
    lifecycle: ReservationLifecycle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReservationLifecycle {
    Pending,
    PositionOpen {
        filled_at: DateTime<Utc>,
        /// A replay of the previous closure cannot release restored exposure.
        reopened_after: Option<DateTime<Utc>>,
    },
    Released {
        event: ReservationEvent,
        released_at: DateTime<Utc>,
    },
}

impl ReservationLifecycle {
    const fn is_active(self) -> bool {
        !matches!(self, Self::Released { .. })
    }
}

/// Serial, deterministic risk calculator and UTC daily circuit breaker.
#[derive(Clone, Debug)]
pub struct RiskManager {
    config: RiskConfig,
    instrument_specs: HashMap<InstrumentId, InstrumentRiskSpec>,
    stale_instrument_specs: HashMap<InstrumentId, InstrumentRiskSpec>,
    reservations: HashMap<Uuid, RiskReservation>,
    daily_equity: Option<DailyEquity>,
    latest_equity_at: Option<DateTime<Utc>>,
    portfolio_breach_active: bool,
}

impl RiskManager {
    /// Creates a manager with no established equity baseline.
    #[must_use]
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            instrument_specs: HashMap::new(),
            stale_instrument_specs: HashMap::new(),
            reservations: HashMap::new(),
            daily_equity: None,
            latest_equity_at: None,
            portfolio_breach_active: false,
        }
    }

    /// Returns immutable account-level limits.
    #[must_use]
    pub const fn config(&self) -> &RiskConfig {
        &self.config
    }

    /// Registers or refreshes one venue specification inside the manager.
    ///
    /// An identical replay is idempotent. If a changed specification arrives
    /// while risk is active, the instrument is marked stale immediately. New
    /// approvals remain blocked until all dependent reservations are released
    /// and the changed specification is registered again.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError::InstrumentSpecInUse`] when active risk depends on a
    /// different existing specification, or [`RiskError::InvalidInstrumentSpec`]
    /// when a conflicting canonical alias already owns the same venue symbol.
    pub fn register_instrument_spec(&mut self, spec: InstrumentRiskSpec) -> Result<(), RiskError> {
        let instrument = spec.instrument().clone();
        if self.instrument_specs.keys().any(|existing| {
            existing.exchange == instrument.exchange
                && existing.market == instrument.market
                && existing.venue_symbol == instrument.venue_symbol
                && existing != &instrument
        }) {
            return Err(RiskError::InvalidInstrumentSpec(
                "conflicting venue symbol alias",
            ));
        }
        let is_identical = self
            .instrument_specs
            .get(&instrument)
            .is_some_and(|existing| existing == &spec);
        let has_active_risk = self.has_active_reservation_for(&instrument);

        if is_identical {
            if self.stale_instrument_specs.contains_key(&instrument) {
                return Err(RiskError::InstrumentSpecStale);
            }
            return Ok(());
        }
        if has_active_risk {
            self.stale_instrument_specs.insert(instrument, spec);
            return Err(RiskError::InstrumentSpecInUse);
        }

        self.instrument_specs.insert(instrument.clone(), spec);
        self.stale_instrument_specs.remove(&instrument);
        Ok(())
    }

    fn has_active_reservation_for(&self, instrument: &InstrumentId) -> bool {
        self.reservations.values().any(|reservation| {
            reservation.lifecycle.is_active() && reservation.plan.instrument() == instrument
        })
    }

    /// Returns the latest observable breaker state.
    #[must_use]
    pub fn circuit_breaker_state(&self) -> CircuitBreakerState {
        self.daily_equity
            .as_ref()
            .map_or(CircuitBreakerState::AwaitingEquity, |daily| {
                match daily.tripped_at {
                    Some(tripped_at) => CircuitBreakerState::Tripped {
                        trading_day: daily.trading_day,
                        opening_equity: daily.opening_equity,
                        current_equity: daily.current_equity,
                        drawdown_fraction: daily.drawdown_fraction,
                        tripped_at,
                    },
                    None => CircuitBreakerState::Armed {
                        trading_day: daily.trading_day,
                        opening_equity: daily.opening_equity,
                        current_equity: daily.current_equity,
                        drawdown_fraction: daily.drawdown_fraction,
                    },
                }
            })
    }

    /// Establishes authoritative UTC start-of-day state.
    ///
    /// This explicit command is required after startup and at each UTC date
    /// boundary. `already_tripped` restores a latch known by persistent account
    /// state; otherwise a current drawdown already at the threshold trips it.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError`] for invalid equity, timestamp regression, a
    /// conflicting same-day baseline, or decimal arithmetic overflow.
    pub fn start_trading_day(
        &mut self,
        opening_equity: Decimal,
        current_equity: Decimal,
        observed_at: DateTime<Utc>,
        already_tripped: bool,
    ) -> Result<CircuitBreakerState, RiskError> {
        if opening_equity <= Decimal::ZERO {
            return Err(RiskError::NonPositiveOpeningEquity);
        }
        self.validate_equity_observation(current_equity, observed_at)?;

        let trading_day = observed_at.date_naive();
        if self.daily_equity.as_ref().is_some_and(|daily| {
            daily.trading_day == trading_day && daily.opening_equity != opening_equity
        }) {
            return Err(RiskError::ConflictingDailyBaseline);
        }

        let drawdown = drawdown_fraction(opening_equity, current_equity)?;
        let must_trip = already_tripped
            || current_equity <= Decimal::ZERO
            || drawdown >= self.config.max_daily_drawdown_fraction;
        let preserved_trip = self.daily_equity.as_ref().and_then(|daily| {
            (daily.trading_day == trading_day)
                .then_some(daily.tripped_at)
                .flatten()
        });
        self.daily_equity = Some(DailyEquity {
            trading_day,
            opening_equity,
            current_equity,
            drawdown_fraction: drawdown,
            tripped_at: preserved_trip.or_else(|| must_trip.then_some(observed_at)),
        });
        self.latest_equity_at = Some(observed_at);
        Ok(self.circuit_breaker_state())
    }

    /// Applies one intraday account-equity snapshot in timestamp order.
    ///
    /// A normal snapshot never creates or resets a daily baseline. Once the
    /// threshold is reached, the latch survives later equity recovery.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError`] for a missing baseline for the snapshot's UTC
    /// date, timestamp regression/conflict, or arithmetic overflow. Zero and
    /// negative equity are accepted as authoritative insolvency signals and
    /// trip the breaker fail-closed.
    pub fn update_equity(
        &mut self,
        equity: Decimal,
        observed_at: DateTime<Utc>,
    ) -> Result<CircuitBreakerState, RiskError> {
        self.validate_equity_observation(equity, observed_at)?;
        let daily = self
            .daily_equity
            .as_mut()
            .ok_or(RiskError::DailyBaselineRequired)?;
        if daily.trading_day != observed_at.date_naive() {
            return Err(RiskError::DailyBaselineRequired);
        }

        daily.current_equity = equity;
        daily.drawdown_fraction = drawdown_fraction(daily.opening_equity, equity)?;
        if (equity <= Decimal::ZERO
            || daily.drawdown_fraction >= self.config.max_daily_drawdown_fraction)
            && daily.tripped_at.is_none()
        {
            daily.tripped_at = Some(observed_at);
        }
        self.latest_equity_at = Some(observed_at);
        Ok(self.circuit_breaker_state())
    }

    fn validate_equity_observation(
        &self,
        equity: Decimal,
        observed_at: DateTime<Utc>,
    ) -> Result<(), RiskError> {
        let Some(latest_at) = self.latest_equity_at else {
            return Ok(());
        };
        if observed_at < latest_at {
            return Err(RiskError::StaleEquitySnapshot);
        }
        if observed_at == latest_at
            && self
                .daily_equity
                .as_ref()
                .is_some_and(|daily| daily.current_equity != equity)
        {
            return Err(RiskError::ConflictingEquitySnapshot);
        }
        Ok(())
    }

    /// Applies one typed execution event to an approved reservation.
    ///
    /// Pending risk is retained after an entry fill and is released only by a
    /// pre-fill cancellation or a full position closure. Only an exact replay
    /// of the immediately materialized event is idempotent. Trusted cumulative
    /// reopening evidence restores the full original reservation even when its
    /// exchange timestamp precedes the previous release. A fresh closure must
    /// then be newer than that release.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError`] for an unknown intent, a timestamp before approval
    /// or an invalid lifecycle transition. Reopening evidence may predate the
    /// prior release but must never predate the original approval.
    pub fn apply_reservation_event(
        &mut self,
        intent_id: Uuid,
        event: ReservationEvent,
        occurred_at: DateTime<Utc>,
    ) -> Result<ReservationTransition, RiskError> {
        let reservation = self
            .reservations
            .get_mut(&intent_id)
            .ok_or(RiskError::UnknownReservation)?;
        if occurred_at < reservation.plan.approved_at() {
            return Err(RiskError::LifecycleBeforeApproval);
        }
        let (lifecycle, outcome) = transition_lifecycle(reservation.lifecycle, event, occurred_at)?;
        reservation.lifecycle = lifecycle;
        self.refresh_portfolio_breach_flag()?;
        Ok(outcome)
    }

    /// Returns combined planned loss of all active approvals.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError::ArithmeticOverflow`] if exact summation overflows.
    pub fn active_reserved_risk(&self) -> Result<Decimal, RiskError> {
        self.reservations
            .values()
            .filter(|reservation| reservation.lifecycle.is_active())
            .try_fold(Decimal::ZERO, |total, reservation| {
                total
                    .checked_add(reservation.plan.planned_loss())
                    .ok_or(RiskError::ArithmeticOverflow)
            })
    }

    fn refresh_portfolio_breach_flag(&mut self) -> Result<(), RiskError> {
        self.portfolio_breach_active = self.portfolio_risk_breach()?.is_some();
        Ok(())
    }

    fn portfolio_risk_breach(&self) -> Result<Option<(Decimal, Decimal)>, RiskError> {
        let Some(daily) = &self.daily_equity else {
            return Ok(None);
        };
        let limit = daily
            .current_equity
            .checked_mul(self.config.max_portfolio_risk_fraction)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let reserved = self.active_reserved_risk()?;
        Ok((reserved > limit).then_some((reserved, limit)))
    }

    /// Sizes and validates a strategy intent against current account risk.
    ///
    /// Entry and target rounding is fill-conservative; stop rounding is always
    /// farther beyond the sweep. Reward-to-risk is checked only after all
    /// rounding. A successful approval atomically reserves its planned loss.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError`] if equity is unavailable, the breaker is tripped,
    /// the intent is stale or inconsistent with the venue spec, price geometry
    /// is invalid, reward-to-risk is too low, the rounded size is below venue
    /// minimum, or exact decimal arithmetic overflows.
    pub fn assess_trade(
        &mut self,
        intent: &TradeIntent,
        target: &LiquidityTarget,
        assessed_at: DateTime<Utc>,
    ) -> Result<TradeAssessment, RiskError> {
        let context = self.assessment_context(assessed_at)?;
        validate_trade_request(intent, target, assessed_at)?;
        let spec = self
            .instrument_specs
            .get(intent.instrument())
            .cloned()
            .ok_or(RiskError::InstrumentSpecUnavailable)?;
        let fingerprint = request_fingerprint(intent, target, &spec);
        if let Some(existing) = self.reservations.get(&intent.id()) {
            return replay_assessment(existing, fingerprint);
        }
        if self
            .stale_instrument_specs
            .contains_key(intent.instrument())
        {
            return Err(RiskError::InstrumentSpecStale);
        }

        let plan = self.calculate_plan(intent, target, &spec, context, assessed_at, fingerprint)?;
        self.reservations.insert(
            intent.id(),
            RiskReservation {
                plan: plan.clone(),
                fingerprint,
                lifecycle: ReservationLifecycle::Pending,
            },
        );
        Ok(TradeAssessment::Approved(Box::new(plan)))
    }

    fn assessment_context(
        &self,
        assessed_at: DateTime<Utc>,
    ) -> Result<AssessmentContext, RiskError> {
        let daily = self
            .daily_equity
            .as_ref()
            .ok_or(RiskError::EquityUnavailable)?;
        let latest_equity_at = self.latest_equity_at.ok_or(RiskError::EquityUnavailable)?;
        if daily.tripped_at.is_some() || daily.current_equity <= Decimal::ZERO {
            return Err(RiskError::CircuitBreakerTripped);
        }
        if assessed_at < latest_equity_at {
            return Err(RiskError::AssessmentBeforeEquitySnapshot);
        }
        let freshness_deadline = latest_equity_at
            .checked_add_signed(self.config.max_equity_snapshot_age)
            .ok_or(RiskError::TimestampOverflow)?;
        if assessed_at > freshness_deadline {
            return Err(RiskError::StaleEquityForAssessment);
        }
        if daily.trading_day != assessed_at.date_naive() {
            return Err(RiskError::DailyBaselineRequired);
        }
        Ok(AssessmentContext {
            current_equity: daily.current_equity,
            latest_equity_at,
        })
    }

    fn calculate_plan(
        &self,
        intent: &TradeIntent,
        target: &LiquidityTarget,
        spec: &InstrumentRiskSpec,
        context: AssessmentContext,
        assessed_at: DateTime<Utc>,
        fingerprint: Uuid,
    ) -> Result<RiskPlan, RiskError> {
        let prices = rounded_prices(
            intent.direction(),
            intent.entry_price(),
            intent.stop_anchor_price(),
            target.price(),
            spec.price_tick,
            self.config.stop_buffer_ticks,
        )?;
        let economics = self.calculate_economics(prices, spec, context.current_equity)?;
        let mut plan = RiskPlan {
            approval_id: Uuid::nil(),
            intent_id: intent.id(),
            target_id: target.id(),
            instrument: intent.instrument().clone(),
            direction: intent.direction(),
            entry_price: prices.entry,
            quantity: economics.quantity,
            stop_loss_price: prices.stop,
            take_profit_price: prices.target,
            risk_budget: economics.risk_budget,
            planned_loss: economics.planned_loss,
            planned_reward: economics.planned_reward,
            risk_reward_ratio: economics.risk_reward_ratio,
            approved_at: assessed_at,
            expires_at: intent.expires_at(),
            approved_spec: spec.clone(),
            approved_config: self.config,
            approved_equity: context.current_equity,
            approved_equity_at: context.latest_equity_at,
            approved_fingerprint: fingerprint,
        };
        plan.approval_id = approval_id(&plan);
        Ok(plan)
    }

    fn calculate_economics(
        &self,
        prices: RoundedPrices,
        spec: &InstrumentRiskSpec,
        current_equity: Decimal,
    ) -> Result<ApprovedEconomics, RiskError> {
        let minimum_reward = prices
            .risk_distance
            .checked_mul(self.config.minimum_risk_reward)
            .ok_or(RiskError::ArithmeticOverflow)?;
        if prices.reward_distance < minimum_reward {
            let actual = prices
                .reward_distance
                .checked_div(prices.risk_distance)
                .ok_or(RiskError::ArithmeticOverflow)?;
            return Err(RiskError::MinimumRiskRewardNotMet {
                actual,
                required: self.config.minimum_risk_reward,
            });
        }
        let risk_reward_ratio = prices
            .reward_distance
            .checked_div(prices.risk_distance)
            .ok_or(RiskError::ArithmeticOverflow)?;

        let per_trade_budget = current_equity
            .checked_mul(self.config.account_risk_fraction)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let portfolio_budget = current_equity
            .checked_mul(self.config.max_portfolio_risk_fraction)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let available_portfolio_risk = portfolio_budget
            .checked_sub(self.active_reserved_risk()?)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let risk_budget = per_trade_budget.min(available_portfolio_risk);
        if risk_budget <= Decimal::ZERO {
            return Err(RiskError::PortfolioRiskLimitReached);
        }
        let risk_per_quantity = prices
            .risk_distance
            .checked_mul(spec.quote_value_per_price_unit)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let raw_quantity = risk_budget
            .checked_div(risk_per_quantity)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let mut quantity = floor_to_increment(raw_quantity, spec.quantity_step)?;
        if let Some(maximum) = spec.maximum_quantity {
            let rounded_maximum = floor_to_increment(maximum, spec.quantity_step)?;
            quantity = quantity.min(rounded_maximum);
        }
        if quantity < spec.minimum_quantity {
            return Err(RiskError::QuantityBelowMinimum {
                calculated: quantity,
                minimum: spec.minimum_quantity,
            });
        }

        let planned_loss = quantity
            .checked_mul(risk_per_quantity)
            .ok_or(RiskError::ArithmeticOverflow)?;
        if planned_loss > risk_budget {
            return Err(RiskError::PlannedLossExceedsBudget);
        }
        let reward_per_quantity = prices
            .reward_distance
            .checked_mul(spec.quote_value_per_price_unit)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let planned_reward = quantity
            .checked_mul(reward_per_quantity)
            .ok_or(RiskError::ArithmeticOverflow)?;
        Ok(ApprovedEconomics {
            quantity,
            risk_budget,
            planned_loss,
            planned_reward,
            risk_reward_ratio,
        })
    }

    /// Processes one actor command without performing I/O.
    #[must_use]
    pub fn process_command(&mut self, command: RiskCommand) -> Vec<RiskAction> {
        match command {
            RiskCommand::RegisterInstrumentSpec { spec } => {
                let instrument = spec.instrument().clone();
                match self.register_instrument_spec(spec) {
                    Ok(()) => vec![RiskAction::InstrumentSpecRegistered { instrument }],
                    Err(reason) => vec![RiskAction::InstrumentSpecRejected { instrument, reason }],
                }
            }
            RiskCommand::StartTradingDay {
                opening_equity,
                current_equity,
                observed_at,
                already_tripped,
            } => {
                let previous_state = self.circuit_breaker_state();
                match self.start_trading_day(
                    opening_equity,
                    current_equity,
                    observed_at,
                    already_tripped,
                ) {
                    Ok(state) => {
                        self.equity_transition_actions(&previous_state, state, observed_at)
                    }
                    Err(reason) => vec![RiskAction::EquitySnapshotRejected {
                        observed_at,
                        reason,
                    }],
                }
            }
            RiskCommand::EquitySnapshot {
                equity,
                observed_at,
            } => {
                let previous_state = self.circuit_breaker_state();
                match self.update_equity(equity, observed_at) {
                    Ok(state) => {
                        self.equity_transition_actions(&previous_state, state, observed_at)
                    }
                    Err(reason) => vec![RiskAction::EquitySnapshotRejected {
                        observed_at,
                        reason,
                    }],
                }
            }
            RiskCommand::AssessTrade {
                intent,
                target,
                assessed_at,
            } => {
                let intent_id = intent.id();
                match self.assess_trade(&intent, &target, assessed_at) {
                    Ok(TradeAssessment::Approved(plan)) => {
                        vec![RiskAction::TradeApproved(plan)]
                    }
                    Ok(TradeAssessment::Replay {
                        approval_id,
                        intent_id,
                    }) => vec![RiskAction::TradeApprovalReplayed {
                        approval_id,
                        intent_id,
                        at: assessed_at,
                    }],
                    Err(reason) => vec![RiskAction::TradeRejected {
                        intent_id,
                        at: assessed_at,
                        reason,
                    }],
                }
            }
            RiskCommand::ReservationEvent {
                intent_id,
                event,
                occurred_at,
            } => {
                let was_breached = self.portfolio_breach_active;
                match self.apply_reservation_event(intent_id, event, occurred_at) {
                    Ok(transition) => self.reservation_transition_actions(
                        intent_id,
                        event,
                        occurred_at,
                        transition,
                        was_breached,
                    ),
                    Err(reason) => vec![RiskAction::ReservationTransitionRejected {
                        intent_id,
                        event,
                        occurred_at,
                        reason,
                    }],
                }
            }
        }
    }

    fn reservation_transition_actions(
        &self,
        intent_id: Uuid,
        event: ReservationEvent,
        occurred_at: DateTime<Utc>,
        transition: ReservationTransition,
        was_breached: bool,
    ) -> Vec<RiskAction> {
        let mut actions = Vec::with_capacity(2);
        if let (false, Some((reserved_risk, limit))) =
            (was_breached, self.portfolio_risk_breach().ok().flatten())
        {
            actions.push(RiskAction::PortfolioRiskBreach {
                reserved_risk,
                limit,
                observed_at: occurred_at,
            });
        }
        actions.push(RiskAction::ReservationTransitioned {
            intent_id,
            event,
            occurred_at,
            transition,
        });
        actions
    }

    fn equity_transition_actions(
        &mut self,
        previous_state: &CircuitBreakerState,
        state: CircuitBreakerState,
        observed_at: DateTime<Utc>,
    ) -> Vec<RiskAction> {
        let breach = self.portfolio_risk_breach().ok().flatten();
        let new_breach = !self.portfolio_breach_active && breach.is_some();
        self.portfolio_breach_active = breach.is_some();
        let breach_action = if new_breach {
            breach.map(|values| (values, observed_at))
        } else {
            None
        };
        breaker_actions(previous_state, state, breach_action)
    }

    /// Runs the manager as a bounded Tokio-channel actor.
    ///
    /// Commands are serialized in receive order so an equity update and a trade
    /// decision cannot race inside the manager.
    ///
    /// # Errors
    ///
    /// Returns [`RiskActorError::OutputChannelClosed`] when the downstream
    /// receiver closes before shutdown.
    pub async fn run(
        mut self,
        mut input: mpsc::Receiver<RiskCommand>,
        output: mpsc::Sender<RiskAction>,
        shutdown: CancellationToken,
    ) -> Result<(), RiskActorError> {
        loop {
            let command = tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                command = input.recv() => match command {
                    Some(command) => command,
                    None => return Ok(()),
                },
            };
            let actions = self.process_command(command);
            for action in actions {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return Ok(()),
                    result = output.send(action) => {
                        if result.is_err() {
                            return Err(RiskActorError::OutputChannelClosed);
                        }
                    }
                }
            }
        }
    }
}

/// Input accepted by the bounded risk actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RiskCommand {
    /// Installs trusted venue filters before trade assessment.
    RegisterInstrumentSpec {
        /// Validated linear-contract specification.
        spec: InstrumentRiskSpec,
    },
    /// Establishes authoritative state at startup or a UTC day boundary.
    StartTradingDay {
        /// Account equity at the start of this UTC day.
        opening_equity: Decimal,
        /// Current equity from the same authoritative state response.
        current_equity: Decimal,
        /// Observation time whose UTC date owns the baseline.
        observed_at: DateTime<Utc>,
        /// Whether persisted/exchange state says the breaker already tripped.
        already_tripped: bool,
    },
    /// Refreshes account equity and daily drawdown.
    EquitySnapshot {
        /// Total account equity in the sizing quote currency.
        equity: Decimal,
        /// Exchange/account timestamp of the observation.
        observed_at: DateTime<Utc>,
    },
    /// Requests independent risk approval for one strategy intent.
    AssessTrade {
        /// Validated strategy setup.
        intent: Box<TradeIntent>,
        /// Typed target chosen from opposing market liquidity.
        target: LiquidityTarget,
        /// Decision time.
        assessed_at: DateTime<Utc>,
    },
    /// Applies an execution-confirmed reservation lifecycle event.
    ReservationEvent {
        /// Approved strategy intent.
        intent_id: Uuid,
        /// Typed entry/position event.
        event: ReservationEvent,
        /// Exchange-confirmed lifecycle time.
        occurred_at: DateTime<Utc>,
    },
}

/// Output emitted by the bounded risk actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RiskAction {
    /// A trusted venue specification was installed.
    InstrumentSpecRegistered {
        /// Registered instrument.
        instrument: InstrumentId,
    },
    /// A specification change conflicted with active risk.
    InstrumentSpecRejected {
        /// Instrument whose update failed.
        instrument: InstrumentId,
        /// Exact rejection reason.
        reason: RiskError,
    },
    /// A valid equity snapshot updated the public breaker state.
    CircuitBreakerUpdated(CircuitBreakerState),
    /// The daily breaker transitioned from untripped to tripped.
    ///
    /// This transition is emitted once; execution should cancel all pending
    /// entries and block any independent order path immediately.
    EmergencyStop {
        /// Latched breaker state at transition time.
        state: CircuitBreakerState,
    },
    /// An invalid or out-of-order equity snapshot was ignored.
    EquitySnapshotRejected {
        /// Timestamp attached to the rejected snapshot.
        observed_at: DateTime<Utc>,
        /// Validation failure.
        reason: RiskError,
    },
    /// Execution may place the fully sized bracket order.
    TradeApproved(Box<RiskPlan>),
    /// The exact assessment command was replayed; no execution is authorized.
    TradeApprovalReplayed {
        /// Existing approval identifier.
        approval_id: Uuid,
        /// Strategy intent owning the active reservation.
        intent_id: Uuid,
        /// Time of the replayed assessment.
        at: DateTime<Utc>,
    },
    /// An intent failed current account or venue risk checks.
    TradeRejected {
        /// Rejected strategy intent.
        intent_id: Uuid,
        /// Time of the decision.
        at: DateTime<Utc>,
        /// Exact rejection cause.
        reason: RiskError,
    },
    /// Existing reservations exceed the risk cap at current equity.
    ///
    /// Execution should reduce/cancel risk; this action never authorizes an
    /// order and remains subordinate to an [`Self::EmergencyStop`].
    PortfolioRiskBreach {
        /// Combined planned loss still reserved.
        reserved_risk: Decimal,
        /// Maximum portfolio risk permitted by current equity.
        limit: Decimal,
        /// Equity observation that caused the breach.
        observed_at: DateTime<Utc>,
    },
    /// A typed reservation event advanced state or was replayed exactly.
    ReservationTransitioned {
        /// Strategy intent owning the reservation.
        intent_id: Uuid,
        /// Applied event.
        event: ReservationEvent,
        /// Exchange-confirmed lifecycle time.
        occurred_at: DateTime<Utc>,
        /// Resulting state and replay marker.
        transition: ReservationTransition,
    },
    /// A lifecycle event was unknown, stale, or invalid for current state.
    ReservationTransitionRejected {
        /// Referenced strategy intent.
        intent_id: Uuid,
        /// Rejected event.
        event: ReservationEvent,
        /// Exchange-confirmed lifecycle time.
        occurred_at: DateTime<Utc>,
        /// Exact rejection reason.
        reason: RiskError,
    },
}

/// Recoverable configuration, account-state, or trade-assessment failure.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RiskError {
    /// Persisted risk data failed version, identity, or economic validation.
    #[error("invalid risk checkpoint: {0}")]
    InvalidCheckpoint(&'static str),
    /// An unsafe symbol or conflicting alias would split one venue identity.
    #[error("invalid instrument risk specification: {0}")]
    InvalidInstrumentSpec(&'static str),
    /// A configured fraction was not in `(0, 1]`.
    #[error("{field} must be in the interval (0, 1]")]
    InvalidFraction {
        /// Configuration field name.
        field: &'static str,
    },
    /// Aggregate portfolio cap was below the allowed per-trade risk.
    #[error("maximum portfolio risk cannot be below per-trade risk")]
    PortfolioRiskBelowTradeRisk,
    /// Equity freshness window was zero or negative.
    #[error("maximum equity snapshot age must be greater than zero")]
    NonPositiveEquityFreshness,
    /// The configured minimum reward-to-risk ratio was below `2`.
    #[error("minimum risk-reward ratio must be at least 2")]
    MinimumRiskRewardBelowTwo,
    /// A stop at the sweep extreme would provide no protective buffer.
    #[error("stop buffer must contain at least one tick")]
    ZeroStopBuffer,
    /// The linear sizing formula cannot safely size this contract family.
    #[error("market kind {0:?} is not supported by linear risk sizing")]
    UnsupportedMarketKind(MarketKind),
    /// A venue constraint was zero or negative.
    #[error("contract constraint {field} must be greater than zero")]
    NonPositiveContractConstraint {
        /// Invalid venue field name.
        field: &'static str,
    },
    /// Maximum quantity was invalid or smaller than minimum quantity.
    #[error("maximum quantity must be positive and no smaller than minimum quantity")]
    InvalidQuantityRange,
    /// A changed venue filter would invalidate an active reservation.
    #[error("cannot change an instrument specification while its risk is active")]
    InstrumentSpecInUse,
    /// A venue filter changed while active risk still used its predecessor.
    #[error(
        "instrument specification is stale and must be re-registered after active risk clears"
    )]
    InstrumentSpecStale,
    /// No trusted venue filters were registered for the intent instrument.
    #[error("instrument risk specification is unavailable")]
    InstrumentSpecUnavailable,
    /// A typed liquidity target used a nil identifier.
    #[error("liquidity target identifier cannot be nil")]
    NilLiquidityTargetIdentifier,
    /// The first equity value of a UTC day must be positive.
    #[error("opening equity for a UTC trading day must be greater than zero")]
    NonPositiveOpeningEquity,
    /// An authoritative baseline disagreed with this UTC day's stored opening.
    #[error("authoritative daily opening equity conflicts with stored state")]
    ConflictingDailyBaseline,
    /// A normal snapshot or assessment crossed a day without initialization.
    #[error("authoritative UTC daily baseline is required")]
    DailyBaselineRequired,
    /// An equity snapshot predated the latest accepted snapshot.
    #[error("equity snapshot timestamp moved backwards")]
    StaleEquitySnapshot,
    /// The same equity timestamp was replayed with a different value.
    #[error("equity snapshot timestamp was replayed with conflicting equity")]
    ConflictingEquitySnapshot,
    /// No UTC daily baseline has been established.
    #[error("risk assessment requires an equity snapshot")]
    EquityUnavailable,
    /// The daily loss threshold has disabled new risk.
    #[error("daily drawdown circuit breaker is tripped")]
    CircuitBreakerTripped,
    /// Assessment time was earlier than its account snapshot.
    #[error("trade assessment cannot predate the equity snapshot it uses")]
    AssessmentBeforeEquitySnapshot,
    /// The latest account snapshot exceeded the configured freshness window.
    #[error("equity snapshot is too old for trade assessment")]
    StaleEquityForAssessment,
    /// Date-time arithmetic exceeded Chrono's representable range.
    #[error("timestamp arithmetic overflow while evaluating risk")]
    TimestampOverflow,
    /// Assessment time was earlier than the observable strategy event.
    #[error("trade intent was not yet observable at assessment time")]
    IntentNotYetObservable,
    /// The strategy deadline was reached before risk approval.
    #[error("trade intent has expired")]
    IntentExpired,
    /// The target was observed after the requested assessment time.
    #[error("liquidity target was not yet observable at assessment time")]
    TargetNotYetObservable,
    /// The target belonged to another instrument.
    #[error("trade intent and liquidity target instruments differ")]
    TargetInstrumentMismatch,
    /// The target was not opposing liquidity for the intended direction.
    #[error("liquidity target side does not oppose the intended position")]
    TargetSideMismatch,
    /// Opposing-liquidity target was zero or negative.
    #[error("opposing-liquidity target must be greater than zero")]
    NonPositiveTarget,
    /// Rounded entry, stop, or target was on the wrong side for the trade.
    #[error("rounded entry, stop, and target do not form valid directional risk")]
    InvalidPriceGeometry,
    /// Rounded target did not retain the configured minimum reward-to-risk.
    #[error("risk-reward ratio {actual} is below required minimum {required}")]
    MinimumRiskRewardNotMet {
        /// Ratio after all tick rounding.
        actual: Decimal,
        /// Configured minimum ratio.
        required: Decimal,
    },
    /// Active reservations consumed the configured portfolio risk budget.
    #[error("maximum aggregate portfolio risk is already reserved")]
    PortfolioRiskLimitReached,
    /// Step-rounded quantity did not reach the venue minimum.
    #[error("calculated quantity {calculated} is below venue minimum {minimum}")]
    QuantityBelowMinimum {
        /// Quantity after rounding down and applying a venue maximum.
        calculated: Decimal,
        /// Venue minimum quantity.
        minimum: Decimal,
    },
    /// A supposedly conservative quantity violated its exact loss ceiling.
    #[error("planned loss exceeds the approved risk budget")]
    PlannedLossExceedsBudget,
    /// An intent identifier was replayed with different economic inputs.
    #[error("intent identifier was replayed with conflicting target or venue inputs")]
    ConflictingIntentReplay,
    /// A completed reservation cannot be reopened by replaying its intent.
    #[error("released intent cannot reserve risk again")]
    ReleasedIntentReplay,
    /// A release referenced an intent that never received approval.
    #[error("risk reservation does not exist")]
    UnknownReservation,
    /// An execution event predates the risk approval it references.
    #[error("reservation lifecycle event predates risk approval")]
    LifecycleBeforeApproval,
    /// An execution event predates the latest accepted lifecycle transition.
    #[error("reservation lifecycle event is stale")]
    StaleLifecycleEvent,
    /// Position closure was reported before an entry fill.
    #[error("position cannot close before the entry fills")]
    PositionCloseBeforeFill,
    /// Cancellation cannot release risk after the entry has filled.
    #[error("entry cancellation arrived after the entry fill")]
    EntryCancelAfterFill,
    /// A non-identical lifecycle replay conflicts with materialized state.
    #[error("reservation lifecycle replay conflicts with stored state")]
    ConflictingLifecycleReplay,
    /// Exact decimal arithmetic exceeded its representable range.
    #[error("decimal arithmetic overflow while evaluating risk")]
    ArithmeticOverflow,
}

/// Fatal bounded-actor transport failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RiskActorError {
    /// The downstream action receiver was dropped while the actor was active.
    #[error("risk output channel is closed")]
    OutputChannelClosed,
}

#[derive(Clone, Copy, Debug)]
struct RoundedPrices {
    entry: Decimal,
    stop: Decimal,
    target: Decimal,
    risk_distance: Decimal,
    reward_distance: Decimal,
}

#[derive(Clone, Copy, Debug)]
struct AssessmentContext {
    current_equity: Decimal,
    latest_equity_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug)]
struct ApprovedEconomics {
    quantity: Decimal,
    risk_budget: Decimal,
    planned_loss: Decimal,
    planned_reward: Decimal,
    risk_reward_ratio: Decimal,
}

fn breaker_actions(
    previous: &CircuitBreakerState,
    state: CircuitBreakerState,
    portfolio_breach: Option<((Decimal, Decimal), DateTime<Utc>)>,
) -> Vec<RiskAction> {
    let transitioned = trip_identity(previous) != trip_identity(&state) && state.is_tripped();
    let mut actions = Vec::with_capacity(3);
    if transitioned {
        actions.push(RiskAction::EmergencyStop {
            state: state.clone(),
        });
    }
    if let Some(((reserved_risk, limit), observed_at)) = portfolio_breach {
        actions.push(RiskAction::PortfolioRiskBreach {
            reserved_risk,
            limit,
            observed_at,
        });
    }
    actions.push(RiskAction::CircuitBreakerUpdated(state));
    actions
}

fn trip_identity(state: &CircuitBreakerState) -> Option<(NaiveDate, DateTime<Utc>)> {
    match state {
        CircuitBreakerState::Tripped {
            trading_day,
            tripped_at,
            ..
        } => Some((*trading_day, *tripped_at)),
        CircuitBreakerState::AwaitingEquity | CircuitBreakerState::Armed { .. } => None,
    }
}

fn validate_fraction(field: &'static str, value: Decimal) -> Result<(), RiskError> {
    if value <= Decimal::ZERO || value > Decimal::ONE {
        return Err(RiskError::InvalidFraction { field });
    }
    Ok(())
}

fn drawdown_fraction(opening: Decimal, current: Decimal) -> Result<Decimal, RiskError> {
    if current <= Decimal::ZERO {
        return Ok(Decimal::ONE);
    }
    if current >= opening {
        return Ok(Decimal::ZERO);
    }
    opening
        .checked_sub(current)
        .and_then(|decline| decline.checked_div(opening))
        .ok_or(RiskError::ArithmeticOverflow)
}

fn rounded_prices(
    direction: Direction,
    raw_entry: Decimal,
    sweep_extreme: Decimal,
    raw_target: Decimal,
    tick: Decimal,
    stop_buffer_ticks: u32,
) -> Result<RoundedPrices, RiskError> {
    let buffer = tick
        .checked_mul(Decimal::from(stop_buffer_ticks))
        .ok_or(RiskError::ArithmeticOverflow)?;
    let (entry, stop, target) = match direction {
        Direction::Bullish => {
            let raw_stop = sweep_extreme
                .checked_sub(buffer)
                .ok_or(RiskError::ArithmeticOverflow)?;
            (
                floor_to_increment(raw_entry, tick)?,
                floor_to_increment(raw_stop, tick)?,
                floor_to_increment(raw_target, tick)?,
            )
        }
        Direction::Bearish => {
            let raw_stop = sweep_extreme
                .checked_add(buffer)
                .ok_or(RiskError::ArithmeticOverflow)?;
            (
                ceil_to_increment(raw_entry, tick)?,
                ceil_to_increment(raw_stop, tick)?,
                ceil_to_increment(raw_target, tick)?,
            )
        }
    };

    let (risk_distance, reward_distance, valid) = match direction {
        Direction::Bullish => (
            entry.checked_sub(stop),
            target.checked_sub(entry),
            stop > Decimal::ZERO && stop < sweep_extreme && stop < entry && target > entry,
        ),
        Direction::Bearish => (
            stop.checked_sub(entry),
            entry.checked_sub(target),
            stop > sweep_extreme && stop > entry && target > Decimal::ZERO && target < entry,
        ),
    };
    if !valid {
        return Err(RiskError::InvalidPriceGeometry);
    }

    Ok(RoundedPrices {
        entry,
        stop,
        target,
        risk_distance: risk_distance.ok_or(RiskError::ArithmeticOverflow)?,
        reward_distance: reward_distance.ok_or(RiskError::ArithmeticOverflow)?,
    })
}

fn floor_to_increment(value: Decimal, increment: Decimal) -> Result<Decimal, RiskError> {
    if value <= Decimal::ZERO || increment <= Decimal::ZERO {
        return Err(RiskError::InvalidPriceGeometry);
    }
    value
        .checked_rem(increment)
        .and_then(|remainder| value.checked_sub(remainder))
        .ok_or(RiskError::ArithmeticOverflow)
}

fn ceil_to_increment(value: Decimal, increment: Decimal) -> Result<Decimal, RiskError> {
    let floor = floor_to_increment(value, increment)?;
    if floor == value {
        Ok(value)
    } else {
        floor
            .checked_add(increment)
            .ok_or(RiskError::ArithmeticOverflow)
    }
}

fn approval_id(plan: &RiskPlan) -> Uuid {
    let maximum = plan
        .approved_spec
        .maximum_quantity
        .map_or_else(|| "none".to_owned(), |value| value.normalize().to_string());
    let fingerprint = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        plan.approved_fingerprint,
        plan.intent_id,
        plan.target_id,
        plan.approved_equity_at,
        plan.approved_equity.normalize(),
        plan.approved_at,
        plan.expires_at,
        plan.approved_config.account_risk_fraction.normalize(),
        plan.approved_config.max_portfolio_risk_fraction.normalize(),
        plan.approved_config.max_daily_drawdown_fraction.normalize(),
        plan.approved_config.minimum_risk_reward.normalize(),
        plan.approved_config.stop_buffer_ticks,
        plan.approved_config
            .max_equity_snapshot_age
            .num_milliseconds(),
        plan.approved_spec.price_tick.normalize(),
        plan.approved_spec.quantity_step.normalize(),
        plan.approved_spec.minimum_quantity.normalize(),
        maximum,
        plan.approved_spec.quote_value_per_price_unit.normalize(),
        plan.entry_price.normalize(),
        plan.stop_loss_price.normalize(),
        plan.take_profit_price.normalize(),
        plan.quantity.normalize(),
        plan.risk_budget.normalize(),
        plan.planned_loss.normalize(),
        plan.planned_reward.normalize(),
        plan.risk_reward_ratio.normalize(),
    );
    let instrument_bound_fingerprint = format!(
        "{fingerprint}|{:?}|{:?}|{}:{}|{}:{}|{:?}|{}:{}",
        plan.instrument.exchange,
        plan.instrument.market,
        plan.instrument.venue_symbol.as_str().len(),
        plan.instrument.venue_symbol,
        plan.instrument.canonical_symbol.as_str().len(),
        plan.instrument.canonical_symbol,
        plan.direction,
        plan.approved_config.max_equity_snapshot_age.num_seconds(),
        plan.approved_config.max_equity_snapshot_age.subsec_nanos(),
    );
    Uuid::new_v5(&APPROVAL_NAMESPACE, instrument_bound_fingerprint.as_bytes())
}

fn validate_trade_request(
    intent: &TradeIntent,
    target: &LiquidityTarget,
    assessed_at: DateTime<Utc>,
) -> Result<(), RiskError> {
    if assessed_at < intent.triggered_at() {
        return Err(RiskError::IntentNotYetObservable);
    }
    if assessed_at >= intent.expires_at() {
        return Err(RiskError::IntentExpired);
    }
    if target.observed_at() > assessed_at {
        return Err(RiskError::TargetNotYetObservable);
    }
    if intent.instrument() != target.instrument() {
        return Err(RiskError::TargetInstrumentMismatch);
    }
    let expected_target_side = match intent.direction() {
        Direction::Bullish => LiquiditySide::BuySide,
        Direction::Bearish => LiquiditySide::SellSide,
    };
    if target.side() != expected_target_side {
        return Err(RiskError::TargetSideMismatch);
    }
    Ok(())
}

fn replay_assessment(
    existing: &RiskReservation,
    fingerprint: Uuid,
) -> Result<TradeAssessment, RiskError> {
    if existing.fingerprint != fingerprint {
        return Err(RiskError::ConflictingIntentReplay);
    }
    if !existing.lifecycle.is_active() {
        return Err(RiskError::ReleasedIntentReplay);
    }
    Ok(TradeAssessment::Replay {
        approval_id: existing.plan.approval_id(),
        intent_id: existing.plan.intent_id(),
    })
}

// Keep every ordinary fill/cancel/close transition together for auditability.
#[allow(clippy::too_many_lines)]
fn transition_lifecycle(
    lifecycle: ReservationLifecycle,
    event: ReservationEvent,
    occurred_at: DateTime<Utc>,
) -> Result<(ReservationLifecycle, ReservationTransition), RiskError> {
    let transitioned = |lifecycle: ReservationLifecycle,
                        status: ReservationStatus|
     -> Result<(ReservationLifecycle, ReservationTransition), RiskError> {
        Ok((
            lifecycle,
            ReservationTransition {
                status,
                replayed: false,
            },
        ))
    };
    match (lifecycle, event) {
        (current, ReservationEvent::EntryExposureReopened) => reopen_exposure(current, occurred_at),
        (
            ReservationLifecycle::Pending
            | ReservationLifecycle::Released {
                event: ReservationEvent::EntryCancelled,
                ..
            },
            ReservationEvent::EntryFilled,
        ) => transitioned(
            ReservationLifecycle::PositionOpen {
                filled_at: occurred_at,
                reopened_after: None,
            },
            ReservationStatus::PositionOpen,
        ),
        (ReservationLifecycle::Pending, ReservationEvent::EntryCancelled) => transitioned(
            ReservationLifecycle::Released {
                event,
                released_at: occurred_at,
            },
            ReservationStatus::Released,
        ),
        (ReservationLifecycle::Pending, ReservationEvent::PositionClosed) => {
            Err(RiskError::PositionCloseBeforeFill)
        }
        (
            current @ ReservationLifecycle::PositionOpen { filled_at, .. },
            ReservationEvent::EntryFilled,
        ) if occurred_at == filled_at => Ok((
            current,
            ReservationTransition {
                status: ReservationStatus::PositionOpen,
                replayed: true,
            },
        )),
        (
            ReservationLifecycle::PositionOpen {
                reopened_after: Some(previous_release),
                ..
            },
            ReservationEvent::PositionClosed,
        ) if occurred_at <= previous_release => Err(RiskError::StaleLifecycleEvent),
        (
            ReservationLifecycle::PositionOpen { filled_at, .. },
            ReservationEvent::PositionClosed,
        ) if occurred_at >= filled_at => transitioned(
            ReservationLifecycle::Released {
                event,
                released_at: occurred_at,
            },
            ReservationStatus::Released,
        ),
        (
            ReservationLifecycle::PositionOpen { filled_at, .. },
            ReservationEvent::EntryCancelled | ReservationEvent::PositionClosed,
        ) if occurred_at < filled_at => Err(RiskError::StaleLifecycleEvent),
        (ReservationLifecycle::PositionOpen { filled_at, .. }, ReservationEvent::EntryFilled)
            if occurred_at < filled_at =>
        {
            Err(RiskError::StaleLifecycleEvent)
        }
        (ReservationLifecycle::PositionOpen { .. }, ReservationEvent::EntryCancelled) => {
            Err(RiskError::EntryCancelAfterFill)
        }
        (
            current @ ReservationLifecycle::Released {
                event: released_event,
                released_at,
            },
            replay_event,
        ) if replay_event == released_event && occurred_at == released_at => Ok((
            current,
            ReservationTransition {
                status: ReservationStatus::Released,
                replayed: true,
            },
        )),
        (ReservationLifecycle::Released { released_at, .. }, _) if occurred_at < released_at => {
            Err(RiskError::StaleLifecycleEvent)
        }
        (
            ReservationLifecycle::PositionOpen { .. },
            ReservationEvent::PositionClosed | ReservationEvent::EntryFilled,
        )
        | (ReservationLifecycle::Released { .. }, _) => Err(RiskError::ConflictingLifecycleReplay),
    }
}

fn reopen_exposure(
    lifecycle: ReservationLifecycle,
    occurred_at: DateTime<Utc>,
) -> Result<(ReservationLifecycle, ReservationTransition), RiskError> {
    let (filled_at, reopened_after, replayed) = match lifecycle {
        ReservationLifecycle::Pending => return Err(RiskError::ConflictingLifecycleReplay),
        ReservationLifecycle::Released { released_at, .. } => {
            (occurred_at, Some(released_at), false)
        }
        ReservationLifecycle::PositionOpen {
            filled_at,
            reopened_after,
        } => (
            filled_at.max(occurred_at),
            reopened_after,
            occurred_at == filled_at,
        ),
    };
    Ok((
        ReservationLifecycle::PositionOpen {
            filled_at,
            reopened_after,
        },
        ReservationTransition {
            status: ReservationStatus::PositionOpen,
            replayed,
        },
    ))
}

fn request_fingerprint(
    intent: &TradeIntent,
    target: &LiquidityTarget,
    spec: &InstrumentRiskSpec,
) -> Uuid {
    let maximum = spec
        .maximum_quantity
        .map_or_else(|| "none".to_owned(), |value| value.normalize().to_string());
    let trigger = intent.trigger_candle();
    let intent_key = format!(
        "{}|{:?}|{:?}|{}|{}|{:?}|{:?}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        intent.id(),
        intent.instrument().exchange,
        intent.instrument().market,
        intent.instrument().venue_symbol,
        intent.instrument().canonical_symbol,
        intent.timeframe(),
        intent.direction(),
        intent.entry_price().normalize(),
        intent.stop_anchor_price().normalize(),
        intent.triggered_at(),
        trigger.open_time(),
        trigger.close_time(),
        trigger.open().normalize(),
        trigger.high().normalize(),
        trigger.low().normalize(),
        trigger.close().normalize(),
        intent.liquidity_sweep().id(),
        intent.market_structure_shift().id(),
        intent.fair_value_gap().id(),
        intent.expires_at(),
    );
    let target_key = format!(
        "{}|{:?}|{:?}|{}|{}|{:?}|{}|{}",
        target.id(),
        target.instrument().exchange,
        target.instrument().market,
        target.instrument().venue_symbol,
        target.instrument().canonical_symbol,
        target.side(),
        target.price().normalize(),
        target.observed_at(),
    );
    let spec_key = format!(
        "{:?}|{:?}|{}|{}|{}|{}|{}|{}|{}",
        spec.instrument().exchange,
        spec.instrument().market,
        spec.instrument().venue_symbol,
        spec.instrument().canonical_symbol,
        spec.price_tick.normalize(),
        spec.quantity_step.normalize(),
        spec.minimum_quantity.normalize(),
        maximum,
        spec.quote_value_per_price_unit.normalize(),
    );
    let fingerprint = format!("{intent_key}|{target_key}|{spec_key}");
    Uuid::new_v5(&APPROVAL_NAMESPACE, fingerprint.as_bytes())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use crate::models::{
        BreakBasis, Candle, CandleInput, Exchange, FairValueGap, FairValueGapInput, FractalModel,
        FvgEntryMode, LiquiditySide, LiquiditySweep, LiquiditySweepInput, MarketStructureShift,
        MarketStructureShiftInput, PriceZone, SwingKind, SwingPoint, Timeframe, TradeIntentInput,
        VolumeUnit,
    };

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    fn at_minute(minute: i64) -> Result<DateTime<Utc>, io::Error> {
        DateTime::from_timestamp(minute.saturating_mul(60), 0)
            .ok_or_else(|| io::Error::other("test timestamp is outside Chrono's range"))
    }

    fn instrument() -> Result<InstrumentId, crate::models::ModelError> {
        InstrumentId::new(
            Exchange::Binance,
            MarketKind::LinearPerpetual,
            "BTCUSDT",
            "BTC-USDT-PERP",
        )
    }

    fn risk_spec() -> Result<InstrumentRiskSpec, Box<dyn Error>> {
        Ok(InstrumentRiskSpec::new(
            instrument()?,
            Decimal::new(1, 1),
            Decimal::new(1, 3),
            Decimal::new(1, 3),
            None,
            Decimal::ONE,
        )?)
    }

    fn target(price: Decimal) -> Result<LiquidityTarget, Box<dyn Error>> {
        Ok(LiquidityTarget::new(
            Uuid::from_u128(14),
            instrument()?,
            LiquiditySide::BuySide,
            price,
            at_minute(4)?,
        )?)
    }

    fn fresh_snapshot_time() -> Result<DateTime<Utc>, Box<dyn Error>> {
        before_minute(5)
    }

    fn before_minute(minute: i64) -> Result<DateTime<Utc>, Box<dyn Error>> {
        at_minute(minute)?
            .checked_sub_signed(Duration::seconds(1))
            .ok_or_else(|| io::Error::other("test snapshot timestamp overflow").into())
    }

    fn ready_manager(equity: Decimal) -> Result<RiskManager, Box<dyn Error>> {
        let mut manager = RiskManager::new(RiskConfig::default());
        manager.register_instrument_spec(risk_spec()?)?;
        manager.start_trading_day(equity, equity, fresh_snapshot_time()?, false)?;
        Ok(manager)
    }

    fn approved_plan(outcome: TradeAssessment) -> Result<RiskPlan, io::Error> {
        match outcome {
            TradeAssessment::Approved(plan) => Ok(*plan),
            TradeAssessment::Replay { .. } => Err(io::Error::other("expected a new risk approval")),
        }
    }

    fn bullish_intent_with_id(id: Uuid) -> Result<TradeIntent, Box<dyn Error>> {
        let series = instrument()?;
        let swing = SwingPoint::new(
            series.clone(),
            Timeframe::M1,
            SwingKind::High,
            Decimal::new(101, 0),
            at_minute(-1)?,
            at_minute(1)?,
            FractalModel::ThreeCandle,
        )?;
        let shift = MarketStructureShift::new(MarketStructureShiftInput {
            id: Uuid::from_u128(12),
            instrument: series.clone(),
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            broken_swing: swing,
            break_price: Decimal::new(103, 0),
            break_basis: BreakBasis::Close,
            confirming_candle_open_time: at_minute(2)?,
            confirmed_at: at_minute(3)?,
        })?;
        let sweep = LiquiditySweep::new(LiquiditySweepInput {
            id: Uuid::from_u128(11),
            instrument: series.clone(),
            timeframe: Timeframe::M1,
            side: LiquiditySide::SellSide,
            reference_price: Decimal::new(99, 0),
            sweep_price: Decimal::new(98, 0),
            sweep_candle_open_time: at_minute(0)?,
            detected_at: at_minute(1)?,
        })?;
        let gap = FairValueGap::new(FairValueGapInput {
            id: Uuid::from_u128(10),
            instrument: series.clone(),
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            zone: PriceZone::new(Decimal::new(100, 0), Decimal::new(102, 0))?,
            formed_from: [at_minute(1)?, at_minute(2)?, at_minute(3)?],
            formed_at: at_minute(4)?,
        })?;
        let trigger = Candle::new(CandleInput {
            instrument: series.clone(),
            timeframe: Timeframe::M1,
            open_time: at_minute(4)?,
            close_time: at_minute(5)?,
            open: Decimal::new(103, 0),
            high: Decimal::new(104, 0),
            low: Decimal::new(101, 0),
            close: Decimal::new(102, 0),
            volume: Decimal::ONE,
            volume_unit: VolumeUnit::Contracts,
            trade_count: Some(1),
            is_closed: true,
        })?;

        Ok(TradeIntent::new(TradeIntentInput {
            id,
            instrument: series,
            timeframe: Timeframe::M1,
            direction: Direction::Bullish,
            liquidity_sweep: sweep,
            market_structure_shift: shift,
            fair_value_gap: gap,
            entry_mode: FvgEntryMode::NearBoundary,
            trigger_candle: trigger,
            expires_at: at_minute(10)?,
        })?)
    }

    fn bullish_intent() -> Result<TradeIntent, Box<dyn Error>> {
        bullish_intent_with_id(Uuid::from_u128(13))
    }

    #[test]
    fn default_limits_are_one_percent_five_percent_and_two_r() {
        let config = RiskConfig::default();
        assert_eq!(config.account_risk_fraction(), Decimal::new(1, 2));
        assert_eq!(config.max_portfolio_risk_fraction(), Decimal::new(5, 2));
        assert_eq!(config.max_daily_drawdown_fraction(), Decimal::new(5, 2));
        assert_eq!(config.minimum_risk_reward(), Decimal::TWO);
        assert_eq!(config.stop_buffer_ticks(), 1);
    }

    #[test]
    fn invalid_limits_and_inverse_contracts_fail_closed() -> TestResult {
        assert_eq!(
            RiskConfig::new(Decimal::ZERO, Decimal::new(5, 2), Decimal::TWO, 1),
            Err(RiskError::InvalidFraction {
                field: "account_risk_fraction"
            })
        );
        assert_eq!(
            RiskConfig::new(
                Decimal::new(1, 2),
                Decimal::new(5, 2),
                Decimal::new(199, 2),
                1
            ),
            Err(RiskError::MinimumRiskRewardBelowTwo)
        );
        let inverse = InstrumentId::new(
            Exchange::Binance,
            MarketKind::InversePerpetual,
            "BTCUSD_PERP",
            "BTC-USD-INVERSE-PERP",
        )?;
        assert_eq!(
            InstrumentRiskSpec::new(
                inverse,
                Decimal::ONE,
                Decimal::ONE,
                Decimal::ONE,
                None,
                Decimal::ONE
            ),
            Err(RiskError::UnsupportedMarketKind(
                MarketKind::InversePerpetual
            ))
        );
        Ok(())
    }

    #[test]
    fn approval_rounds_outward_and_never_exceeds_risk_budget() -> TestResult {
        let intent = bullish_intent()?;
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let target = target(Decimal::new(11_249, 2))?;

        let plan = approved_plan(manager.assess_trade(&intent, &target, at_minute(5)?)?)?;

        assert_eq!(plan.entry_price(), Decimal::new(102, 0));
        assert_eq!(plan.stop_loss_price(), Decimal::new(979, 1));
        assert!(plan.stop_loss_price() < intent.stop_anchor_price());
        assert_eq!(plan.take_profit_price(), Decimal::new(1124, 1));
        assert_eq!(plan.quantity(), Decimal::new(24_390, 3));
        assert!(plan.planned_loss() <= plan.risk_budget());
        assert!(plan.risk_reward_ratio() >= Decimal::TWO);

        let repeated = manager.assess_trade(&intent, &target, at_minute(5)?)?;
        assert_eq!(
            repeated,
            TradeAssessment::Replay {
                approval_id: plan.approval_id(),
                intent_id: intent.id(),
            }
        );
        assert_eq!(manager.active_reserved_risk()?, plan.planned_loss());
        Ok(())
    }

    #[test]
    fn bearish_prices_round_in_safe_directions() -> TestResult {
        let prices = rounded_prices(
            Direction::Bearish,
            Decimal::new(10_211, 2),
            Decimal::new(10_501, 2),
            Decimal::new(8_001, 2),
            Decimal::new(5, 1),
            1,
        )?;
        assert_eq!(prices.entry, Decimal::new(1025, 1));
        assert_eq!(prices.stop, Decimal::new(106, 0));
        assert_eq!(prices.target, Decimal::new(805, 1));
        assert!(prices.stop > Decimal::new(10_501, 2));
        Ok(())
    }

    #[test]
    fn reward_check_occurs_after_tick_rounding() -> TestResult {
        let intent = bullish_intent()?;
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let error = manager.assess_trade(&intent, &target(Decimal::new(11_019, 2))?, at_minute(5)?);
        assert!(matches!(
            error,
            Err(RiskError::MinimumRiskRewardNotMet { .. })
        ));
        Ok(())
    }

    #[test]
    fn drawdown_trips_at_threshold_latches_and_resets_next_utc_day() -> TestResult {
        let mut manager = RiskManager::new(RiskConfig::default());
        manager.register_instrument_spec(risk_spec()?)?;
        manager.start_trading_day(
            Decimal::new(1_000, 0),
            Decimal::new(1_000, 0),
            at_minute(0)?,
            false,
        )?;
        let tripped = manager.update_equity(Decimal::new(950, 0), at_minute(1)?)?;
        assert!(tripped.is_tripped());

        let recovered = manager.update_equity(Decimal::new(1_100, 0), at_minute(2)?)?;
        assert!(recovered.is_tripped());
        assert_eq!(
            manager.assess_trade(
                &bullish_intent()?,
                &target(Decimal::new(112, 0))?,
                at_minute(5)?
            ),
            Err(RiskError::CircuitBreakerTripped)
        );

        assert_eq!(
            manager.update_equity(Decimal::new(900, 0), at_minute(1_440)?),
            Err(RiskError::DailyBaselineRequired)
        );
        let next_day = manager.start_trading_day(
            Decimal::new(900, 0),
            Decimal::new(900, 0),
            at_minute(1_440)?,
            false,
        )?;
        assert!(!next_day.is_tripped());
        assert_eq!(next_day.trading_day(), Some(at_minute(1_440)?.date_naive()));
        Ok(())
    }

    #[test]
    fn stale_snapshots_and_expired_intents_are_rejected() -> TestResult {
        let mut manager = RiskManager::new(RiskConfig::default());
        manager.register_instrument_spec(risk_spec()?)?;
        manager.start_trading_day(
            Decimal::new(1_000, 0),
            Decimal::new(1_000, 0),
            at_minute(4)?,
            false,
        )?;
        assert_eq!(
            manager.update_equity(Decimal::new(999, 0), at_minute(3)?),
            Err(RiskError::StaleEquitySnapshot)
        );
        assert_eq!(
            manager.assess_trade(
                &bullish_intent()?,
                &target(Decimal::new(112, 0))?,
                at_minute(10)?
            ),
            Err(RiskError::StaleEquityForAssessment)
        );
        manager.update_equity(Decimal::new(1_000, 0), before_minute(10)?)?;
        assert_eq!(
            manager.assess_trade(
                &bullish_intent()?,
                &target(Decimal::new(112, 0))?,
                at_minute(10)?
            ),
            Err(RiskError::IntentExpired)
        );
        Ok(())
    }

    #[test]
    fn reservations_are_idempotent_bounded_and_release_only_once() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let target = target(Decimal::new(112, 0))?;
        for identifier in 20_u128..26 {
            manager.assess_trade(
                &bullish_intent_with_id(Uuid::from_u128(identifier))?,
                &target,
                at_minute(5)?,
            )?;
        }
        let portfolio_cap = Decimal::new(10_000, 0)
            .checked_mul(RiskConfig::default().max_portfolio_risk_fraction())
            .ok_or(RiskError::ArithmeticOverflow)?;
        assert!(manager.active_reserved_risk()? <= portfolio_cap);
        assert!(manager
            .assess_trade(
                &bullish_intent_with_id(Uuid::from_u128(26))?,
                &target,
                at_minute(5)?,
            )
            .is_err());

        let cancelled_at = at_minute(6)?;
        let released = manager.apply_reservation_event(
            Uuid::from_u128(20),
            ReservationEvent::EntryCancelled,
            cancelled_at,
        )?;
        assert_eq!(released.status, ReservationStatus::Released);
        assert!(!released.replayed);
        let replayed = manager.apply_reservation_event(
            Uuid::from_u128(20),
            ReservationEvent::EntryCancelled,
            cancelled_at,
        )?;
        assert!(replayed.replayed);
        assert_eq!(
            manager.assess_trade(
                &bullish_intent_with_id(Uuid::from_u128(20))?,
                &target,
                at_minute(5)?,
            ),
            Err(RiskError::ReleasedIntentReplay)
        );
        Ok(())
    }

    #[test]
    fn entry_fill_retains_risk_until_position_close() -> TestResult {
        let intent = bullish_intent()?;
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let plan = approved_plan(manager.assess_trade(
            &intent,
            &target(Decimal::new(112, 0))?,
            at_minute(5)?,
        )?)?;
        let filled_at = at_minute(6)?;

        let fill = manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryFilled,
            filled_at,
        )?;
        assert_eq!(fill.status, ReservationStatus::PositionOpen);
        assert!(!fill.replayed);
        assert_eq!(manager.active_reserved_risk()?, plan.planned_loss());

        let fill_replay = manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryFilled,
            filled_at,
        )?;
        assert!(fill_replay.replayed);
        assert_eq!(
            manager.apply_reservation_event(
                intent.id(),
                ReservationEvent::EntryCancelled,
                at_minute(7)?,
            ),
            Err(RiskError::EntryCancelAfterFill)
        );

        let closed_at = at_minute(8)?;
        let closed = manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::PositionClosed,
            closed_at,
        )?;
        assert_eq!(closed.status, ReservationStatus::Released);
        assert_eq!(manager.active_reserved_risk()?, Decimal::ZERO);
        let close_replay = manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::PositionClosed,
            closed_at,
        )?;
        assert!(close_replay.replayed);
        Ok(())
    }

    #[test]
    fn late_trusted_fill_reactivates_cancelled_risk_and_emits_breach() -> TestResult {
        let config = RiskConfig::new_with_safety_limits(
            Decimal::new(1, 2),
            Decimal::new(1, 2),
            Decimal::new(5, 2),
            Decimal::TWO,
            1,
            Duration::seconds(120),
        )?;
        let mut manager = RiskManager::new(config);
        manager.register_instrument_spec(risk_spec()?)?;
        manager.start_trading_day(
            Decimal::new(10_000, 0),
            Decimal::new(10_000, 0),
            fresh_snapshot_time()?,
            false,
        )?;
        let target = target(Decimal::new(112, 0))?;
        let cancelled_intent = bullish_intent_with_id(Uuid::from_u128(40))?;
        manager.assess_trade(&cancelled_intent, &target, at_minute(5)?)?;

        let cancellation = manager.process_command(RiskCommand::ReservationEvent {
            intent_id: cancelled_intent.id(),
            event: ReservationEvent::EntryCancelled,
            occurred_at: at_minute(7)?,
        });
        assert!(matches!(
            cancellation.first(),
            Some(RiskAction::ReservationTransitioned { .. })
        ));
        let other_intent = bullish_intent_with_id(Uuid::from_u128(41))?;
        manager.assess_trade(&other_intent, &target, at_minute(5)?)?;

        let late_fill = manager.process_command(RiskCommand::ReservationEvent {
            intent_id: cancelled_intent.id(),
            event: ReservationEvent::EntryFilled,
            occurred_at: at_minute(6)?,
        });
        assert!(matches!(
            late_fill.first(),
            Some(RiskAction::PortfolioRiskBreach { .. })
        ));
        assert!(matches!(
            late_fill.get(1),
            Some(RiskAction::ReservationTransitioned {
                transition: ReservationTransition {
                    status: ReservationStatus::PositionOpen,
                    replayed: false,
                },
                ..
            })
        ));
        let limit = Decimal::new(10_000, 0)
            .checked_mul(config.max_portfolio_risk_fraction())
            .ok_or(RiskError::ArithmeticOverflow)?;
        assert!(manager.active_reserved_risk()? > limit);
        Ok(())
    }

    #[test]
    fn trusted_reopened_exposure_restores_risk_and_blocks_old_closure_replay() -> TestResult {
        let intent = bullish_intent()?;
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let plan = approved_plan(manager.assess_trade(
            &intent,
            &target(Decimal::new(112, 0))?,
            at_minute(5)?,
        )?)?;
        manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryFilled,
            at_minute(6)?,
        )?;
        manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::PositionClosed,
            at_minute(8)?,
        )?;
        assert_eq!(manager.active_reserved_risk()?, Decimal::ZERO);

        // A replay of the old EntryFilled is not new cumulative-fill evidence.
        assert_eq!(
            manager.apply_reservation_event(
                intent.id(),
                ReservationEvent::EntryFilled,
                at_minute(7)?
            ),
            Err(RiskError::StaleLifecycleEvent)
        );
        assert_eq!(
            manager.apply_reservation_event(
                intent.id(),
                ReservationEvent::EntryFilled,
                at_minute(9)?
            ),
            Err(RiskError::ConflictingLifecycleReplay)
        );
        assert_eq!(manager.active_reserved_risk()?, Decimal::ZERO);

        let reopened = manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryExposureReopened,
            at_minute(7)?,
        )?;
        assert_eq!(reopened.status, ReservationStatus::PositionOpen);
        assert!(!reopened.replayed);
        assert_eq!(manager.active_reserved_risk()?, plan.planned_loss());
        let duplicate = manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryExposureReopened,
            at_minute(7)?,
        )?;
        assert!(duplicate.replayed);
        assert_eq!(manager.active_reserved_risk()?, plan.planned_loss());

        // The old closure was later on the exchange clock than the delayed fill.
        // Its replay still cannot prove that the newly discovered exposure closed.
        assert_eq!(
            manager.apply_reservation_event(
                intent.id(),
                ReservationEvent::PositionClosed,
                at_minute(8)?
            ),
            Err(RiskError::StaleLifecycleEvent)
        );
        assert_eq!(manager.active_reserved_risk()?, plan.planned_loss());
        let additional = manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryExposureReopened,
            at_minute(9)?,
        )?;
        assert!(!additional.replayed);
        let older = manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryExposureReopened,
            at_minute(7)?,
        )?;
        assert_eq!(older.status, ReservationStatus::PositionOpen);
        assert_eq!(manager.active_reserved_risk()?, plan.planned_loss());
        manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::PositionClosed,
            at_minute(10)?,
        )?;
        assert_eq!(manager.active_reserved_risk()?, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn reopened_evidence_rejects_pending_and_preapproval_but_accepts_cancelled_release(
    ) -> TestResult {
        let intent = bullish_intent()?;
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let plan = approved_plan(manager.assess_trade(
            &intent,
            &target(Decimal::new(112, 0))?,
            at_minute(5)?,
        )?)?;
        assert_eq!(
            manager.apply_reservation_event(
                intent.id(),
                ReservationEvent::EntryExposureReopened,
                at_minute(6)?
            ),
            Err(RiskError::ConflictingLifecycleReplay)
        );
        assert_eq!(manager.active_reserved_risk()?, plan.planned_loss());
        manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryCancelled,
            at_minute(8)?,
        )?;
        assert_eq!(
            manager.apply_reservation_event(
                intent.id(),
                ReservationEvent::EntryExposureReopened,
                at_minute(4)?
            ),
            Err(RiskError::LifecycleBeforeApproval)
        );
        assert_eq!(manager.active_reserved_risk()?, Decimal::ZERO);
        let reopened = manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryExposureReopened,
            at_minute(6)?,
        )?;
        assert_eq!(reopened.status, ReservationStatus::PositionOpen);
        assert_eq!(manager.active_reserved_risk()?, plan.planned_loss());
        Ok(())
    }

    #[test]
    fn reopened_closed_exposure_reclaims_full_portfolio_risk_and_emits_breach() -> TestResult {
        let config = RiskConfig::new_with_safety_limits(
            Decimal::new(1, 2),
            Decimal::new(1, 2),
            Decimal::new(5, 2),
            Decimal::TWO,
            1,
            Duration::seconds(120),
        )?;
        let mut manager = RiskManager::new(config);
        manager.register_instrument_spec(risk_spec()?)?;
        manager.start_trading_day(
            Decimal::from(10_000),
            Decimal::from(10_000),
            fresh_snapshot_time()?,
            false,
        )?;
        let target = target(Decimal::from(112))?;
        let first = bullish_intent_with_id(Uuid::from_u128(50))?;
        let first_plan = approved_plan(manager.assess_trade(&first, &target, at_minute(5)?)?)?;
        manager.apply_reservation_event(
            first.id(),
            ReservationEvent::EntryFilled,
            at_minute(6)?,
        )?;
        manager.apply_reservation_event(
            first.id(),
            ReservationEvent::PositionClosed,
            at_minute(8)?,
        )?;
        let second = bullish_intent_with_id(Uuid::from_u128(51))?;
        let second_plan = approved_plan(manager.assess_trade(&second, &target, at_minute(5)?)?)?;

        let actions = manager.process_command(RiskCommand::ReservationEvent {
            intent_id: first.id(),
            event: ReservationEvent::EntryExposureReopened,
            occurred_at: at_minute(7)?,
        });
        assert!(matches!(
            actions.first(),
            Some(RiskAction::PortfolioRiskBreach { .. })
        ));
        assert!(matches!(
            actions.get(1),
            Some(RiskAction::ReservationTransitioned {
                transition: ReservationTransition {
                    status: ReservationStatus::PositionOpen,
                    replayed: false
                },
                ..
            })
        ));
        let total = first_plan
            .planned_loss()
            .checked_add(second_plan.planned_loss())
            .ok_or(RiskError::ArithmeticOverflow)?;
        assert_eq!(manager.active_reserved_risk()?, total);
        let duplicate = manager.process_command(RiskCommand::ReservationEvent {
            intent_id: first.id(),
            event: ReservationEvent::EntryExposureReopened,
            occurred_at: at_minute(7)?,
        });
        assert_eq!(duplicate.len(), 1);
        assert_eq!(manager.active_reserved_risk()?, total);
        assert_eq!(
            manager.assess_trade(
                &bullish_intent_with_id(Uuid::from_u128(52))?,
                &target,
                at_minute(5)?
            ),
            Err(RiskError::PortfolioRiskLimitReached)
        );
        Ok(())
    }

    #[test]
    fn changed_active_spec_stays_stale_until_release_and_reregistration() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let target = target(Decimal::new(112, 0))?;
        let first = bullish_intent_with_id(Uuid::from_u128(30))?;
        manager.assess_trade(&first, &target, at_minute(5)?)?;
        let changed = InstrumentRiskSpec::new(
            instrument()?,
            Decimal::new(2, 1),
            Decimal::new(1, 3),
            Decimal::new(1, 3),
            None,
            Decimal::ONE,
        )?;

        assert_eq!(
            manager.register_instrument_spec(changed.clone()),
            Err(RiskError::InstrumentSpecInUse)
        );
        let second = bullish_intent_with_id(Uuid::from_u128(31))?;
        assert_eq!(
            manager.assess_trade(&second, &target, at_minute(5)?),
            Err(RiskError::InstrumentSpecStale)
        );
        manager.apply_reservation_event(
            first.id(),
            ReservationEvent::EntryCancelled,
            at_minute(5)?,
        )?;
        assert_eq!(
            manager.assess_trade(&second, &target, at_minute(5)?),
            Err(RiskError::InstrumentSpecStale)
        );
        manager.register_instrument_spec(changed)?;
        assert!(matches!(
            manager.assess_trade(&second, &target, at_minute(5)?),
            Ok(TradeAssessment::Approved(_))
        ));
        Ok(())
    }

    #[test]
    fn equity_insolvency_and_same_timestamp_conflicts_fail_closed() -> TestResult {
        let mut insolvent = RiskManager::new(RiskConfig::default());
        let state = insolvent.start_trading_day(
            Decimal::new(1_000, 0),
            Decimal::ZERO,
            at_minute(0)?,
            false,
        )?;
        assert!(state.is_tripped());

        let mut manager = RiskManager::new(RiskConfig::default());
        manager.start_trading_day(
            Decimal::new(1_000, 0),
            Decimal::new(1_000, 0),
            at_minute(0)?,
            false,
        )?;
        assert_eq!(
            manager.update_equity(Decimal::new(999, 0), at_minute(0)?),
            Err(RiskError::ConflictingEquitySnapshot)
        );
        let negative = manager.update_equity(Decimal::NEGATIVE_ONE, at_minute(1)?)?;
        assert!(negative.is_tripped());
        Ok(())
    }

    #[test]
    fn actor_replay_is_non_executable_and_fingerprint_conflicts_reject() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let intent = bullish_intent()?;
        let first_target = target(Decimal::new(112, 0))?;
        let first = manager.process_command(RiskCommand::AssessTrade {
            intent: Box::new(intent.clone()),
            target: first_target.clone(),
            assessed_at: at_minute(5)?,
        });
        assert!(matches!(first.first(), Some(RiskAction::TradeApproved(_))));

        let replay = manager.process_command(RiskCommand::AssessTrade {
            intent: Box::new(intent.clone()),
            target: first_target,
            assessed_at: at_minute(5)?,
        });
        assert!(matches!(
            replay.first(),
            Some(RiskAction::TradeApprovalReplayed { .. })
        ));

        let conflict = manager.assess_trade(&intent, &target(Decimal::new(113, 0))?, at_minute(5)?);
        assert_eq!(conflict, Err(RiskError::ConflictingIntentReplay));
        Ok(())
    }

    #[test]
    fn equity_decline_emits_portfolio_breach_without_an_execution_replay() -> TestResult {
        let config = RiskConfig::new_with_safety_limits(
            Decimal::new(1, 2),
            Decimal::new(1, 2),
            Decimal::new(5, 2),
            Decimal::TWO,
            1,
            Duration::seconds(120),
        )?;
        let mut manager = RiskManager::new(config);
        manager.register_instrument_spec(risk_spec()?)?;
        manager.start_trading_day(
            Decimal::new(10_000, 0),
            Decimal::new(10_000, 0),
            fresh_snapshot_time()?,
            false,
        )?;
        manager.assess_trade(
            &bullish_intent()?,
            &target(Decimal::new(112, 0))?,
            at_minute(5)?,
        )?;

        let actions = manager.process_command(RiskCommand::EquitySnapshot {
            equity: Decimal::new(9_900, 0),
            observed_at: at_minute(6)?,
        });
        assert!(matches!(
            actions.first(),
            Some(RiskAction::PortfolioRiskBreach { .. })
        ));
        assert!(actions
            .iter()
            .all(|action| !matches!(action, RiskAction::TradeApproved(_))));
        Ok(())
    }

    #[test]
    fn quantization_uses_exact_remainder() -> TestResult {
        assert_eq!(
            floor_to_increment(Decimal::ONE, Decimal::new(3, 1))?,
            Decimal::new(9, 1)
        );
        assert_eq!(
            ceil_to_increment(Decimal::ONE, Decimal::new(3, 1))?,
            Decimal::new(12, 1)
        );
        Ok(())
    }

    #[test]
    fn emergency_stop_is_emitted_once_per_latched_day() -> TestResult {
        let mut manager = RiskManager::new(RiskConfig::default());
        let initial = manager.process_command(RiskCommand::StartTradingDay {
            opening_equity: Decimal::new(1_000, 0),
            current_equity: Decimal::new(1_000, 0),
            observed_at: at_minute(0)?,
            already_tripped: false,
        });
        assert_eq!(initial.len(), 1);

        let transition = manager.process_command(RiskCommand::EquitySnapshot {
            equity: Decimal::new(950, 0),
            observed_at: at_minute(1)?,
        });
        assert_eq!(transition.len(), 2);
        assert!(matches!(
            transition.first(),
            Some(RiskAction::EmergencyStop { .. })
        ));

        let latched = manager.process_command(RiskCommand::EquitySnapshot {
            equity: Decimal::new(940, 0),
            observed_at: at_minute(2)?,
        });
        assert_eq!(latched.len(), 1);
        Ok(())
    }

    #[test]
    fn emergency_transition_identity_includes_the_utc_trading_day() -> TestResult {
        let mut manager = RiskManager::new(RiskConfig::default());
        let first_day = manager.process_command(RiskCommand::StartTradingDay {
            opening_equity: Decimal::new(1_000, 0),
            current_equity: Decimal::new(940, 0),
            observed_at: at_minute(0)?,
            already_tripped: true,
        });
        assert!(matches!(
            first_day.first(),
            Some(RiskAction::EmergencyStop { .. })
        ));

        let next_day = manager.process_command(RiskCommand::StartTradingDay {
            opening_equity: Decimal::new(940, 0),
            current_equity: Decimal::new(880, 0),
            observed_at: at_minute(1_440)?,
            already_tripped: true,
        });
        assert!(matches!(
            next_day.first(),
            Some(RiskAction::EmergencyStop { .. })
        ));
        Ok(())
    }

    #[test]
    fn checkpoint_roundtrip_preserves_pending_open_and_released_replays() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let target = target(Decimal::new(112, 0))?;
        let pending = bullish_intent_with_id(Uuid::from_u128(100))?;
        let open = bullish_intent_with_id(Uuid::from_u128(101))?;
        let released = bullish_intent_with_id(Uuid::from_u128(102))?;
        for intent in [&pending, &open, &released] {
            manager.assess_trade(intent, &target, at_minute(5)?)?;
        }
        manager.apply_reservation_event(open.id(), ReservationEvent::EntryFilled, at_minute(6)?)?;
        manager.apply_reservation_event(
            released.id(),
            ReservationEvent::EntryCancelled,
            at_minute(6)?,
        )?;
        let checkpoint = manager.checkpoint();
        let bytes = serde_json::to_vec(&checkpoint)?;
        let mut restored = RiskManager::restore(serde_json::from_slice(&bytes)?)?;
        assert_eq!(restored.checkpoint(), checkpoint);
        assert_eq!(
            restored.active_reserved_risk()?,
            manager.active_reserved_risk()?
        );
        assert!(matches!(
            restored.assess_trade(&pending, &target, at_minute(5)?)?,
            TradeAssessment::Replay { .. }
        ));
        assert_eq!(
            restored.assess_trade(&released, &target, at_minute(5)?),
            Err(RiskError::ReleasedIntentReplay)
        );
        assert!(
            restored
                .apply_reservation_event(open.id(), ReservationEvent::EntryFilled, at_minute(6)?)?
                .replayed
        );
        assert!(
            restored
                .apply_reservation_event(
                    released.id(),
                    ReservationEvent::EntryCancelled,
                    at_minute(6)?
                )?
                .replayed
        );
        assert_eq!(
            restored.reservation_status(pending.id()),
            Some(ReservationStatus::Pending)
        );
        assert_eq!(
            restored.reservation_status(open.id()),
            Some(ReservationStatus::PositionOpen)
        );
        assert_eq!(
            restored.reservation_status(released.id()),
            Some(ReservationStatus::Released)
        );
        Ok(())
    }

    #[test]
    fn checkpoint_preserves_late_fill_and_reopening_closure_watermark() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let intent = bullish_intent()?;
        let plan = approved_plan(manager.assess_trade(
            &intent,
            &target(Decimal::new(112, 0))?,
            at_minute(5)?,
        )?)?;
        manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryCancelled,
            at_minute(7)?,
        )?;
        let mut restored = RiskManager::restore(manager.checkpoint())?;
        restored.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryFilled,
            at_minute(6)?,
        )?;
        restored.apply_reservation_event(
            intent.id(),
            ReservationEvent::PositionClosed,
            at_minute(8)?,
        )?;
        restored = RiskManager::restore(restored.checkpoint())?;
        restored.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryExposureReopened,
            at_minute(7)?,
        )?;
        let mut reopened = RiskManager::restore(restored.checkpoint())?;
        assert_eq!(reopened.active_reserved_risk()?, plan.planned_loss());
        assert_eq!(
            reopened.apply_reservation_event(
                intent.id(),
                ReservationEvent::PositionClosed,
                at_minute(8)?
            ),
            Err(RiskError::StaleLifecycleEvent)
        );
        assert!(
            reopened
                .apply_reservation_event(
                    intent.id(),
                    ReservationEvent::EntryExposureReopened,
                    at_minute(7)?
                )?
                .replayed
        );
        reopened.apply_reservation_event(
            intent.id(),
            ReservationEvent::PositionClosed,
            at_minute(9)?,
        )?;
        assert_eq!(reopened.active_reserved_risk()?, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn checkpoint_keeps_drawdown_latch_baseline_and_emission_identity() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let actions = manager.process_command(RiskCommand::EquitySnapshot {
            equity: Decimal::new(9_400, 0),
            observed_at: at_minute(6)?,
        });
        assert!(matches!(
            actions.first(),
            Some(RiskAction::EmergencyStop { .. })
        ));
        manager.update_equity(Decimal::new(11_000, 0), at_minute(7)?)?;
        let expected = manager.circuit_breaker_state();
        let mut restored = RiskManager::restore(manager.checkpoint())?;
        assert_eq!(restored.circuit_breaker_state(), expected);
        assert!(expected.is_tripped());
        assert_eq!(
            restored.start_trading_day(
                Decimal::new(11_000, 0),
                Decimal::new(11_000, 0),
                at_minute(8)?,
                false
            ),
            Err(RiskError::ConflictingDailyBaseline)
        );
        let repeated = restored.process_command(RiskCommand::EquitySnapshot {
            equity: Decimal::new(10_500, 0),
            observed_at: at_minute(8)?,
        });
        assert!(!repeated
            .iter()
            .any(|action| matches!(action, RiskAction::EmergencyStop { .. })));
        Ok(())
    }

    #[test]
    fn checkpoint_preserves_portfolio_breach_emission_latch() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        manager.assess_trade(
            &bullish_intent()?,
            &target(Decimal::new(112, 0))?,
            at_minute(5)?,
        )?;
        let transition = manager.process_command(RiskCommand::EquitySnapshot {
            equity: Decimal::new(1_000, 0),
            observed_at: at_minute(6)?,
        });
        assert!(transition
            .iter()
            .any(|action| matches!(action, RiskAction::PortfolioRiskBreach { .. })));
        let mut restored = RiskManager::restore(manager.checkpoint())?;
        let repeated = restored.process_command(RiskCommand::EquitySnapshot {
            equity: Decimal::new(900, 0),
            observed_at: at_minute(7)?,
        });
        assert!(!repeated
            .iter()
            .any(|action| matches!(action, RiskAction::PortfolioRiskBreach { .. })));
        Ok(())
    }

    #[test]
    fn checkpoint_rejects_missing_latch_and_unknown_fields() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        manager.update_equity(Decimal::new(9_400, 0), at_minute(6)?)?;
        let snapshot = serde_json::to_value(manager.checkpoint())?;
        let mut missing_latch = snapshot.clone();
        missing_latch["daily_equity"]["tripped_at"] = serde_json::Value::Null;
        assert!(RiskManager::restore(serde_json::from_value(missing_latch)?).is_err());
        let mut unknown = snapshot;
        unknown["reset_breaker"] = serde_json::json!(true);
        assert!(serde_json::from_value::<RiskCheckpoint>(unknown).is_err());
        Ok(())
    }

    #[test]
    fn checkpoint_requires_explicit_optional_state_fields() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let intent = bullish_intent()?;
        manager.assess_trade(&intent, &target(Decimal::new(112, 0))?, at_minute(5)?)?;
        manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryFilled,
            at_minute(6)?,
        )?;
        manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::PositionClosed,
            at_minute(7)?,
        )?;
        manager.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryExposureReopened,
            at_minute(6)?,
        )?;
        manager.update_equity(Decimal::new(9_400, 0), at_minute(7)?)?;
        manager.update_equity(Decimal::new(11_000, 0), at_minute(8)?)?;
        let snapshot = serde_json::to_value(manager.checkpoint())?;
        for (object, field) in [
            ("", "daily_equity"),
            ("", "latest_equity_at"),
            ("/daily_equity", "tripped_at"),
            ("/reservations/0/lifecycle", "reopened_after"),
            ("/instrument_specs/0", "maximum_quantity"),
            ("/reservations/0/plan/approved_spec", "maximum_quantity"),
        ] {
            let mut missing = snapshot.clone();
            missing
                .pointer_mut(object)
                .and_then(serde_json::Value::as_object_mut)
                .ok_or_else(|| io::Error::other("missing optional field fixture"))?
                .remove(field);
            assert!(
                serde_json::from_value::<RiskCheckpoint>(missing).is_err(),
                "accepted omitted {object}/{field}"
            );
        }
        assert!(RiskManager::restore(serde_json::from_value(snapshot)?).is_ok());
        let pristine = RiskManager::new(RiskConfig::default()).checkpoint();
        assert!(
            RiskManager::restore(serde_json::from_slice(&serde_json::to_vec(&pristine)?)?).is_ok()
        );
        Ok(())
    }

    #[test]
    fn checkpoint_retains_stale_spec_and_historical_approved_contract() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let intent = bullish_intent()?;
        let target = target(Decimal::new(112, 0))?;
        manager.assess_trade(&intent, &target, at_minute(5)?)?;
        let replacement = InstrumentRiskSpec::new(
            instrument()?,
            Decimal::ONE,
            Decimal::ONE,
            Decimal::ONE,
            None,
            Decimal::new(2, 0),
        )?;
        assert_eq!(
            manager.register_instrument_spec(replacement.clone()),
            Err(RiskError::InstrumentSpecInUse)
        );
        let mut restored = RiskManager::restore(manager.checkpoint())?;
        assert_eq!(
            restored.register_instrument_spec(risk_spec()?),
            Err(RiskError::InstrumentSpecStale)
        );
        restored.apply_reservation_event(
            intent.id(),
            ReservationEvent::EntryCancelled,
            at_minute(6)?,
        )?;
        restored.register_instrument_spec(replacement)?;
        let checkpoint = restored.checkpoint();
        assert_eq!(
            RiskManager::restore(checkpoint.clone())?.checkpoint(),
            checkpoint
        );
        Ok(())
    }

    #[test]
    fn checkpoint_rejects_duplicate_identity_invalid_economics_and_lifecycle() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        manager.assess_trade(
            &bullish_intent()?,
            &target(Decimal::new(112, 0))?,
            at_minute(5)?,
        )?;
        let checkpoint = serde_json::to_value(manager.checkpoint())?;
        let mutations = [
            ("/version", serde_json::json!(999)),
            ("/reservations/0/plan/quantity", serde_json::json!("999")),
            (
                "/reservations/0/plan/planned_loss",
                serde_json::json!("0.01"),
            ),
            (
                "/reservations/0/plan/risk_budget",
                serde_json::json!("999999"),
            ),
            (
                "/reservations/0/plan/approval_id",
                serde_json::json!(Uuid::from_u128(444)),
            ),
            (
                "/reservations/0/plan/expires_at",
                serde_json::json!(at_minute(4)?),
            ),
            (
                "/reservations/0/fingerprint",
                serde_json::json!(Uuid::nil()),
            ),
            (
                "/reservations/0/lifecycle",
                serde_json::json!({"state":"position_open","filled_at":at_minute(4)?,"reopened_after":null}),
            ),
            ("/daily_equity/drawdown_fraction", serde_json::json!("0.5")),
            ("/latest_equity_at", serde_json::Value::Null),
            ("/config/account_risk_fraction", serde_json::json!("0")),
        ];
        for (pointer, replacement) in mutations {
            let mut corrupted = checkpoint.clone();
            *corrupted
                .pointer_mut(pointer)
                .ok_or_else(|| io::Error::other("missing fixture pointer"))? = replacement;
            assert!(
                RiskManager::restore(serde_json::from_value(corrupted)?).is_err(),
                "accepted corrupted {pointer}"
            );
        }
        for key in ["reservations", "instrument_specs"] {
            let mut duplicate = checkpoint.clone();
            let entries = duplicate
                .get_mut(key)
                .and_then(serde_json::Value::as_array_mut)
                .ok_or_else(|| io::Error::other("missing fixture array"))?;
            let entry = entries
                .first()
                .ok_or_else(|| io::Error::other("empty fixture array"))?
                .clone();
            entries.push(entry);
            assert!(RiskManager::restore(serde_json::from_value(duplicate)?).is_err());
        }
        Ok(())
    }

    #[test]
    fn checkpoint_decimal_strings_and_subsecond_limits_are_lossless() -> TestResult {
        let config = RiskConfig::new_with_safety_limits(
            Decimal::new(1, 2),
            Decimal::new(5, 2),
            Decimal::new(5, 2),
            Decimal::TWO,
            1,
            Duration::seconds(5) + Duration::nanoseconds(123),
        )?;
        let manager = RiskManager::new(config);
        let snapshot = manager.checkpoint();
        assert_eq!(
            RiskManager::restore(snapshot.clone())?.checkpoint(),
            snapshot
        );
        let valid = serde_json::to_value(snapshot)?;
        for invalid in [
            serde_json::json!(1),
            serde_json::json!("0.12345678901234567890123456789"),
        ] {
            let mut changed = valid.clone();
            changed["config"]["account_risk_fraction"] = invalid;
            assert!(serde_json::from_value::<RiskCheckpoint>(changed).is_err());
        }
        let mut exact = valid;
        exact["config"]["account_risk_fraction"] =
            serde_json::json!("0.0123456789012345678901234567");
        let restored = RiskManager::restore(serde_json::from_value(exact)?)?;
        assert_eq!(
            restored.config().account_risk_fraction().to_string(),
            "0.0123456789012345678901234567"
        );
        Ok(())
    }

    #[test]
    fn checkpoint_approval_hash_binds_explicit_instrument_identity() -> TestResult {
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let plan = approved_plan(manager.assess_trade(
            &bullish_intent()?,
            &target(Decimal::new(112, 0))?,
            at_minute(5)?,
        )?)?;
        let mut corrupted = serde_json::to_value(plan.checkpoint())?;
        for pointer in ["/instrument", "/approved_spec/instrument"] {
            let instrument = corrupted
                .pointer_mut(pointer)
                .ok_or_else(|| io::Error::other("missing instrument fixture"))?;
            instrument["venue_symbol"] = serde_json::json!("ETHUSDT");
            instrument["canonical_symbol"] = serde_json::json!("ETH-USDT-PERP");
        }
        assert!(RiskPlan::restore_checkpoint(serde_json::from_value(corrupted)?).is_err());
        Ok(())
    }

    #[test]
    fn instrument_specs_reject_unsafe_symbols_and_conflicting_venue_aliases() -> TestResult {
        let build = |instrument| {
            InstrumentRiskSpec::new(
                instrument,
                Decimal::ONE,
                Decimal::ONE,
                Decimal::ONE,
                None,
                Decimal::ONE,
            )
        };
        for symbol in ["A".repeat(129), "BTC\nUSDT".to_owned()] {
            let instrument = InstrumentId::new(
                Exchange::Binance,
                MarketKind::LinearPerpetual,
                symbol,
                "BTC-USDT-PERP",
            )?;
            assert!(matches!(
                build(instrument),
                Err(RiskError::InvalidInstrumentSpec(_))
            ));
        }
        let mut manager = ready_manager(Decimal::new(10_000, 0))?;
        let alias = InstrumentId::new(
            Exchange::Binance,
            MarketKind::LinearPerpetual,
            "BTCUSDT",
            "BTC-ALIAS",
        )?;
        let before = manager.checkpoint();
        assert!(matches!(
            manager.register_instrument_spec(build(alias)?),
            Err(RiskError::InvalidInstrumentSpec(_))
        ));
        assert_eq!(manager.checkpoint(), before);
        assert!(RiskManager::restore(manager.checkpoint()).is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn actor_serializes_equity_before_approval() -> TestResult {
        let manager = RiskManager::new(RiskConfig::default());
        let (input_tx, input_rx) = mpsc::channel(4);
        let (output_tx, mut output_rx) = mpsc::channel(4);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(manager.run(input_rx, output_tx, shutdown.clone()));

        input_tx
            .send(RiskCommand::RegisterInstrumentSpec { spec: risk_spec()? })
            .await?;
        input_tx
            .send(RiskCommand::StartTradingDay {
                opening_equity: Decimal::new(10_000, 0),
                current_equity: Decimal::new(10_000, 0),
                observed_at: fresh_snapshot_time()?,
                already_tripped: false,
            })
            .await?;
        input_tx
            .send(RiskCommand::AssessTrade {
                intent: Box::new(bullish_intent()?),
                target: target(Decimal::new(112, 0))?,
                assessed_at: at_minute(5)?,
            })
            .await?;

        assert!(matches!(
            output_rx.recv().await,
            Some(RiskAction::InstrumentSpecRegistered { .. })
        ));
        assert!(matches!(
            output_rx.recv().await,
            Some(RiskAction::CircuitBreakerUpdated(_))
        ));
        assert!(matches!(
            output_rx.recv().await,
            Some(RiskAction::TradeApproved(_))
        ));
        shutdown.cancel();
        task.await??;
        Ok(())
    }
}
