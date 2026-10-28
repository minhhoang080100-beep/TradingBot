//! Lossless, versioned persistence for the deterministic risk state machine.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use super::{
    approval_id, drawdown_fraction, DailyEquity, InstrumentRiskSpec, ReservationEvent,
    ReservationLifecycle, ReservationStatus, RiskConfig, RiskError, RiskManager, RiskPlan,
    RiskReservation, MINIMUM_ALLOWED_RISK_REWARD,
};
use crate::models::{Direction, InstrumentId};

const VERSION: u32 = 1;
const MAX_RECORDS: usize = 100_000;

/// An opaque, untrusted snapshot; only [`RiskManager::restore`] grants validity.
///
/// Serialize this value inside an account-bound, integrity-protected durable
/// envelope. Decimal values are strings and restore never resets the daily
/// baseline, the drawdown latch, or completed reservation tombstones.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskCheckpoint {
    version: u32,
    config: ConfigCheckpoint,
    instrument_specs: Vec<SpecCheckpoint>,
    stale_instrument_specs: Vec<SpecCheckpoint>,
    reservations: Vec<ReservationCheckpoint>,
    #[serde(deserialize_with = "required_option")]
    daily_equity: Option<DailyCheckpoint>,
    #[serde(deserialize_with = "required_option")]
    latest_equity_at: Option<DateTime<Utc>>,
    portfolio_breach_active: bool,
}

impl RiskCheckpoint {
    pub(crate) fn plans(&self) -> impl Iterator<Item = &RiskPlanCheckpoint> {
        self.reservations
            .iter()
            .map(|reservation| &reservation.plan)
    }

    pub(crate) fn instruments(&self) -> impl Iterator<Item = &InstrumentId> {
        self.instrument_specs
            .iter()
            .chain(&self.stale_instrument_specs)
            .map(|spec| &spec.instrument)
    }
}

/// Opaque approval data revalidated before execution can recover a risk plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskPlanCheckpoint {
    version: u32,
    approval_id: Uuid,
    intent_id: Uuid,
    target_id: Uuid,
    instrument: InstrumentId,
    direction: Direction,
    entry_price: ExactDecimal,
    quantity: ExactDecimal,
    stop_loss_price: ExactDecimal,
    take_profit_price: ExactDecimal,
    risk_budget: ExactDecimal,
    planned_loss: ExactDecimal,
    planned_reward: ExactDecimal,
    risk_reward_ratio: ExactDecimal,
    approved_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    approved_spec: SpecCheckpoint,
    approved_config: ConfigCheckpoint,
    approved_equity: ExactDecimal,
    approved_equity_at: DateTime<Utc>,
    approved_fingerprint: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExactDecimal(Decimal);

impl Serialize for ExactDecimal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        crate::decimal_codec::required::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for ExactDecimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        crate::decimal_codec::required::deserialize(deserializer).map(Self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigCheckpoint {
    account_risk_fraction: ExactDecimal,
    max_portfolio_risk_fraction: ExactDecimal,
    max_daily_drawdown_fraction: ExactDecimal,
    minimum_risk_reward: ExactDecimal,
    stop_buffer_ticks: u32,
    max_equity_age_seconds: i64,
    max_equity_age_nanoseconds: i32,
}

impl From<RiskConfig> for ConfigCheckpoint {
    fn from(config: RiskConfig) -> Self {
        Self {
            account_risk_fraction: ExactDecimal(config.account_risk_fraction),
            max_portfolio_risk_fraction: ExactDecimal(config.max_portfolio_risk_fraction),
            max_daily_drawdown_fraction: ExactDecimal(config.max_daily_drawdown_fraction),
            minimum_risk_reward: ExactDecimal(config.minimum_risk_reward),
            stop_buffer_ticks: config.stop_buffer_ticks,
            max_equity_age_seconds: config.max_equity_snapshot_age.num_seconds(),
            max_equity_age_nanoseconds: config.max_equity_snapshot_age.subsec_nanos(),
        }
    }
}

impl ConfigCheckpoint {
    fn restore(self) -> Result<RiskConfig, RiskError> {
        let nanos = u32::try_from(self.max_equity_age_nanoseconds)
            .map_err(|_| invalid("equity freshness"))?;
        let duration = Duration::new(self.max_equity_age_seconds, nanos)
            .ok_or_else(|| invalid("equity freshness"))?;
        RiskConfig::new_with_safety_limits(
            self.account_risk_fraction.0,
            self.max_portfolio_risk_fraction.0,
            self.max_daily_drawdown_fraction.0,
            self.minimum_risk_reward.0,
            self.stop_buffer_ticks,
            duration,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpecCheckpoint {
    instrument: InstrumentId,
    price_tick: ExactDecimal,
    quantity_step: ExactDecimal,
    minimum_quantity: ExactDecimal,
    #[serde(deserialize_with = "required_option")]
    maximum_quantity: Option<ExactDecimal>,
    quote_value_per_price_unit: ExactDecimal,
}

impl From<&InstrumentRiskSpec> for SpecCheckpoint {
    fn from(spec: &InstrumentRiskSpec) -> Self {
        Self {
            instrument: spec.instrument.clone(),
            price_tick: ExactDecimal(spec.price_tick),
            quantity_step: ExactDecimal(spec.quantity_step),
            minimum_quantity: ExactDecimal(spec.minimum_quantity),
            maximum_quantity: spec.maximum_quantity.map(ExactDecimal),
            quote_value_per_price_unit: ExactDecimal(spec.quote_value_per_price_unit),
        }
    }
}

impl SpecCheckpoint {
    fn restore(self) -> Result<InstrumentRiskSpec, RiskError> {
        InstrumentRiskSpec::new(
            self.instrument,
            self.price_tick.0,
            self.quantity_step.0,
            self.minimum_quantity.0,
            self.maximum_quantity.map(|value| value.0),
            self.quote_value_per_price_unit.0,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DailyCheckpoint {
    trading_day: NaiveDate,
    opening_equity: ExactDecimal,
    current_equity: ExactDecimal,
    drawdown_fraction: ExactDecimal,
    #[serde(deserialize_with = "required_option")]
    tripped_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReservationCheckpoint {
    plan: RiskPlanCheckpoint,
    fingerprint: Uuid,
    lifecycle: LifecycleCheckpoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum LifecycleCheckpoint {
    Pending,
    PositionOpen {
        filled_at: DateTime<Utc>,
        #[serde(deserialize_with = "required_option")]
        reopened_after: Option<DateTime<Utc>>,
    },
    Cancelled {
        released_at: DateTime<Utc>,
    },
    Closed {
        released_at: DateTime<Utc>,
    },
}

impl From<ReservationLifecycle> for LifecycleCheckpoint {
    fn from(lifecycle: ReservationLifecycle) -> Self {
        match lifecycle {
            ReservationLifecycle::Pending => Self::Pending,
            ReservationLifecycle::PositionOpen {
                filled_at,
                reopened_after,
            } => Self::PositionOpen {
                filled_at,
                reopened_after,
            },
            ReservationLifecycle::Released {
                event: ReservationEvent::EntryCancelled,
                released_at,
            } => Self::Cancelled { released_at },
            ReservationLifecycle::Released { released_at, .. } => Self::Closed { released_at },
        }
    }
}

impl LifecycleCheckpoint {
    fn restore(self, approved_at: DateTime<Utc>) -> Result<ReservationLifecycle, RiskError> {
        match self {
            Self::Pending => Ok(ReservationLifecycle::Pending),
            Self::PositionOpen {
                filled_at,
                reopened_after,
            } => {
                if filled_at < approved_at || reopened_after.is_some_and(|at| at < approved_at) {
                    return Err(invalid("reservation lifecycle time"));
                }
                Ok(ReservationLifecycle::PositionOpen {
                    filled_at,
                    reopened_after,
                })
            }
            Self::Cancelled { released_at } | Self::Closed { released_at } => {
                if released_at < approved_at {
                    return Err(invalid("reservation release time"));
                }
                let event = if matches!(self, Self::Cancelled { .. }) {
                    ReservationEvent::EntryCancelled
                } else {
                    ReservationEvent::PositionClosed
                };
                Ok(ReservationLifecycle::Released { event, released_at })
            }
        }
    }
}

impl RiskPlan {
    /// Captures a lossless approval snapshot without granting restore authority.
    #[must_use]
    pub fn checkpoint(&self) -> RiskPlanCheckpoint {
        RiskPlanCheckpoint {
            version: VERSION,
            approval_id: self.approval_id,
            intent_id: self.intent_id,
            target_id: self.target_id,
            instrument: self.instrument.clone(),
            direction: self.direction,
            entry_price: ExactDecimal(self.entry_price),
            quantity: ExactDecimal(self.quantity),
            stop_loss_price: ExactDecimal(self.stop_loss_price),
            take_profit_price: ExactDecimal(self.take_profit_price),
            risk_budget: ExactDecimal(self.risk_budget),
            planned_loss: ExactDecimal(self.planned_loss),
            planned_reward: ExactDecimal(self.planned_reward),
            risk_reward_ratio: ExactDecimal(self.risk_reward_ratio),
            approved_at: self.approved_at,
            expires_at: self.expires_at,
            approved_spec: SpecCheckpoint::from(&self.approved_spec),
            approved_config: self.approved_config.into(),
            approved_equity: ExactDecimal(self.approved_equity),
            approved_equity_at: self.approved_equity_at,
            approved_fingerprint: self.approved_fingerprint,
        }
    }

    pub(crate) fn restore_checkpoint(snapshot: RiskPlanCheckpoint) -> Result<Self, RiskError> {
        if snapshot.version != VERSION {
            return Err(invalid("unsupported plan version"));
        }
        let plan = Self {
            approval_id: snapshot.approval_id,
            intent_id: snapshot.intent_id,
            target_id: snapshot.target_id,
            instrument: snapshot.instrument,
            direction: snapshot.direction,
            entry_price: snapshot.entry_price.0,
            quantity: snapshot.quantity.0,
            stop_loss_price: snapshot.stop_loss_price.0,
            take_profit_price: snapshot.take_profit_price.0,
            risk_budget: snapshot.risk_budget.0,
            planned_loss: snapshot.planned_loss.0,
            planned_reward: snapshot.planned_reward.0,
            risk_reward_ratio: snapshot.risk_reward_ratio.0,
            approved_at: snapshot.approved_at,
            expires_at: snapshot.expires_at,
            approved_spec: snapshot.approved_spec.restore()?,
            approved_config: snapshot.approved_config.restore()?,
            approved_equity: snapshot.approved_equity.0,
            approved_equity_at: snapshot.approved_equity_at,
            approved_fingerprint: snapshot.approved_fingerprint,
        };
        validate_plan(&plan)?;
        Ok(plan)
    }
}

impl RiskManager {
    pub(crate) fn validate_dispatch_time(&self, now: DateTime<Utc>) -> Result<(), RiskError> {
        self.assessment_context(now).map(|_| ())
    }

    /// Captures all account limits, daily state, and reservation tombstones.
    ///
    /// This does not perform I/O. The caller must durably commit the surrounding
    /// account envelope before dispatching any action generated by this state.
    #[must_use]
    pub fn checkpoint(&self) -> RiskCheckpoint {
        let mut reservations: Vec<_> = self
            .reservations
            .values()
            .map(|reservation| ReservationCheckpoint {
                plan: reservation.plan.checkpoint(),
                fingerprint: reservation.fingerprint,
                lifecycle: reservation.lifecycle.into(),
            })
            .collect();
        reservations.sort_unstable_by_key(|reservation| reservation.plan.intent_id);
        RiskCheckpoint {
            version: VERSION,
            config: self.config.into(),
            instrument_specs: sorted_specs(&self.instrument_specs),
            stale_instrument_specs: sorted_specs(&self.stale_instrument_specs),
            reservations,
            daily_equity: self.daily_equity.as_ref().map(|daily| DailyCheckpoint {
                trading_day: daily.trading_day,
                opening_equity: ExactDecimal(daily.opening_equity),
                current_equity: ExactDecimal(daily.current_equity),
                drawdown_fraction: ExactDecimal(daily.drawdown_fraction),
                tripped_at: daily.tripped_at,
            }),
            latest_equity_at: self.latest_equity_at,
            portfolio_breach_active: self.portfolio_breach_active,
        }
    }

    /// Rebuilds a manager only after validating every persisted invariant.
    ///
    /// Restore does not authorize venue execution or make old equity fresh.
    /// Reconciliation, account identity, integrity, and durable write ordering
    /// remain responsibilities of the owning runtime.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError`] for an unsupported version, duplicate identity,
    /// invalid economics, malformed lifecycle, or arithmetic overflow.
    pub fn restore(snapshot: RiskCheckpoint) -> Result<Self, RiskError> {
        if snapshot.version != VERSION || snapshot.reservations.len() > MAX_RECORDS {
            return Err(invalid("unsupported version or record limit"));
        }
        let mut manager = Self::new(snapshot.config.restore()?);
        manager.instrument_specs = restore_specs(snapshot.instrument_specs)?;
        manager.stale_instrument_specs = restore_specs(snapshot.stale_instrument_specs)?;
        for (instrument, stale) in &manager.stale_instrument_specs {
            if manager
                .instrument_specs
                .get(instrument)
                .is_none_or(|current| current == stale)
            {
                return Err(invalid("stale instrument specification"));
            }
        }
        manager.daily_equity = restore_daily(
            snapshot.daily_equity,
            snapshot.latest_equity_at,
            &manager.config,
        )?;
        manager.latest_equity_at = snapshot.latest_equity_at;
        let mut approval_ids = HashSet::new();
        for reservation in snapshot.reservations {
            let plan = RiskPlan::restore_checkpoint(reservation.plan)?;
            if reservation.fingerprint.is_nil()
                || reservation.fingerprint != plan.approved_fingerprint
                || plan.approved_config != manager.config
                || !approval_ids.insert(plan.approval_id)
                || manager.reservations.contains_key(&plan.intent_id)
                || !manager.instrument_specs.contains_key(&plan.instrument)
                || manager.daily_equity.is_none()
                || manager
                    .latest_equity_at
                    .is_none_or(|latest_at| plan.approved_equity_at > latest_at)
                || plan.risk_reward_ratio < manager.config.minimum_risk_reward
            {
                return Err(invalid("reservation identity or context"));
            }
            let lifecycle = reservation.lifecycle.restore(plan.approved_at)?;
            manager.reservations.insert(
                plan.intent_id,
                RiskReservation {
                    plan,
                    fingerprint: reservation.fingerprint,
                    lifecycle,
                },
            );
        }
        manager.portfolio_breach_active = snapshot.portfolio_breach_active;
        // Validate aggregate arithmetic without rewriting an emission latch.
        manager.active_reserved_risk()?;
        manager.portfolio_risk_breach()?;
        Ok(manager)
    }

    /// Finds an approval, including a released tombstone, for recovery checks.
    #[must_use]
    pub fn reservation_plan(&self, intent_id: Uuid) -> Option<&RiskPlan> {
        self.reservations
            .get(&intent_id)
            .map(|reservation| &reservation.plan)
    }

    /// Returns persisted exposure state without mutating its replay history.
    #[must_use]
    pub fn reservation_status(&self, intent_id: Uuid) -> Option<ReservationStatus> {
        self.reservations
            .get(&intent_id)
            .map(|reservation| match reservation.lifecycle {
                ReservationLifecycle::Pending => ReservationStatus::Pending,
                ReservationLifecycle::PositionOpen { .. } => ReservationStatus::PositionOpen,
                ReservationLifecycle::Released { .. } => ReservationStatus::Released,
            })
    }

    pub(crate) fn reservation_release_event(&self, intent_id: Uuid) -> Option<ReservationEvent> {
        self.reservations
            .get(&intent_id)
            .and_then(|reservation| match reservation.lifecycle {
                ReservationLifecycle::Released { event, .. } => Some(event),
                ReservationLifecycle::Pending | ReservationLifecycle::PositionOpen { .. } => None,
            })
    }
}

fn invalid(reason: &'static str) -> RiskError {
    RiskError::InvalidCheckpoint(reason)
}

// Unlike Serde's default Option handling, deserialize_with makes an omitted
// field an error while preserving explicit null as a valid, recorded value.
fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn sorted_specs(specs: &HashMap<InstrumentId, InstrumentRiskSpec>) -> Vec<SpecCheckpoint> {
    let mut snapshots: Vec<_> = specs.values().map(SpecCheckpoint::from).collect();
    snapshots.sort_unstable_by(|left, right| left.instrument.cmp(&right.instrument));
    snapshots
}

fn restore_specs(
    snapshots: Vec<SpecCheckpoint>,
) -> Result<HashMap<InstrumentId, InstrumentRiskSpec>, RiskError> {
    if snapshots.len() > MAX_RECORDS {
        return Err(invalid("instrument record limit"));
    }
    let mut specs = HashMap::new();
    let mut venue_symbols = HashSet::new();
    for snapshot in snapshots {
        let spec = snapshot.restore()?;
        let instrument = spec.instrument.clone();
        if !venue_symbols.insert((
            instrument.exchange,
            instrument.market,
            instrument.venue_symbol.clone(),
        )) || specs.insert(instrument, spec).is_some()
        {
            return Err(invalid("duplicate instrument identity"));
        }
    }
    Ok(specs)
}

fn restore_daily(
    snapshot: Option<DailyCheckpoint>,
    latest_at: Option<DateTime<Utc>>,
    config: &RiskConfig,
) -> Result<Option<DailyEquity>, RiskError> {
    let (snapshot, latest_at) = match (snapshot, latest_at) {
        (None, None) => return Ok(None),
        (Some(snapshot), Some(at)) => (snapshot, at),
        _ => return Err(invalid("equity observation pairing")),
    };
    if snapshot.opening_equity.0 <= Decimal::ZERO
        || snapshot.trading_day != latest_at.date_naive()
        || snapshot
            .tripped_at
            .is_some_and(|at| at.date_naive() != snapshot.trading_day || at > latest_at)
    {
        return Err(invalid("daily equity baseline or time"));
    }
    let drawdown = drawdown_fraction(snapshot.opening_equity.0, snapshot.current_equity.0)?;
    if drawdown != snapshot.drawdown_fraction.0
        || (drawdown >= config.max_daily_drawdown_fraction && snapshot.tripped_at.is_none())
    {
        return Err(invalid("daily drawdown or latch"));
    }
    Ok(Some(DailyEquity {
        trading_day: snapshot.trading_day,
        opening_equity: snapshot.opening_equity.0,
        current_equity: snapshot.current_equity.0,
        drawdown_fraction: drawdown,
        tripped_at: snapshot.tripped_at,
    }))
}

fn validate_plan(plan: &RiskPlan) -> Result<(), RiskError> {
    let spec = &plan.approved_spec;
    if plan.approval_id.is_nil()
        || plan.intent_id.is_nil()
        || plan.target_id.is_nil()
        || plan.instrument != spec.instrument
        || plan.expires_at <= plan.approved_at
        || plan.risk_budget <= Decimal::ZERO
        || plan.planned_loss <= Decimal::ZERO
        || plan.planned_loss > plan.risk_budget
        || plan.quantity < spec.minimum_quantity
        || spec
            .maximum_quantity
            .is_some_and(|maximum| plan.quantity > maximum)
        || plan.risk_reward_ratio < MINIMUM_ALLOWED_RISK_REWARD
    {
        return Err(invalid("plan identity, bounds, or expiry"));
    }
    for (value, increment) in [
        (plan.entry_price, spec.price_tick),
        (plan.stop_loss_price, spec.price_tick),
        (plan.take_profit_price, spec.price_tick),
        (plan.quantity, spec.quantity_step),
    ] {
        if value <= Decimal::ZERO || value.checked_rem(increment) != Some(Decimal::ZERO) {
            return Err(invalid("plan venue quantization"));
        }
    }
    let (risk_distance, reward_distance) = match plan.direction {
        Direction::Bullish => (
            plan.entry_price.checked_sub(plan.stop_loss_price),
            plan.take_profit_price.checked_sub(plan.entry_price),
        ),
        Direction::Bearish => (
            plan.stop_loss_price.checked_sub(plan.entry_price),
            plan.entry_price.checked_sub(plan.take_profit_price),
        ),
    };
    let risk_distance = risk_distance.ok_or(RiskError::ArithmeticOverflow)?;
    let reward_distance = reward_distance.ok_or(RiskError::ArithmeticOverflow)?;
    if risk_distance <= Decimal::ZERO || reward_distance <= Decimal::ZERO {
        return Err(invalid("plan directional geometry"));
    }
    let required_reward = risk_distance
        .checked_mul(plan.approved_config.minimum_risk_reward)
        .ok_or(RiskError::ArithmeticOverflow)?;
    if reward_distance < required_reward {
        return Err(invalid("plan minimum risk reward"));
    }
    let loss = risk_distance
        .checked_mul(spec.quote_value_per_price_unit)
        .and_then(|value| plan.quantity.checked_mul(value))
        .ok_or(RiskError::ArithmeticOverflow)?;
    let reward = reward_distance
        .checked_mul(spec.quote_value_per_price_unit)
        .and_then(|value| plan.quantity.checked_mul(value))
        .ok_or(RiskError::ArithmeticOverflow)?;
    let ratio = reward_distance
        .checked_div(risk_distance)
        .ok_or(RiskError::ArithmeticOverflow)?;
    if plan.planned_loss != loss || plan.planned_reward != reward || plan.risk_reward_ratio != ratio
    {
        return Err(invalid("plan economic consistency"));
    }
    validate_approval_context(plan)
}

fn validate_approval_context(plan: &RiskPlan) -> Result<(), RiskError> {
    let freshness_deadline = plan
        .approved_equity_at
        .checked_add_signed(plan.approved_config.max_equity_snapshot_age)
        .ok_or(RiskError::TimestampOverflow)?;
    let trade_limit = plan
        .approved_equity
        .checked_mul(plan.approved_config.account_risk_fraction)
        .ok_or(RiskError::ArithmeticOverflow)?;
    if plan.approved_equity <= Decimal::ZERO
        || plan.risk_budget > trade_limit
        || plan.approved_equity_at > plan.approved_at
        || plan.approved_equity_at.date_naive() != plan.approved_at.date_naive()
        || plan.approved_at > freshness_deadline
        || plan.approved_fingerprint.is_nil()
        || approval_id(plan) != plan.approval_id
    {
        return Err(invalid("approval context or identity"));
    }
    Ok(())
}
