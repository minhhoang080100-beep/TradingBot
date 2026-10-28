//! Deterministic, conservative OHLC paper execution without exchange I/O.
//!
//! This is a simulator, not a venue fill model: funding, liquidation, queue
//! priority, order books, and partial fills are not modeled. Fees are explicit
//! assumptions. An adverse gap can exceed the planned risk or exhaust equity;
//! losses are never clipped. End-of-data never liquidates an open position.
//! Leverage changes initial-margin affordability only, never the approved risk
//! budget or the price-movement `PnL` formula. There is no maintenance-margin or
//! isolated-liquidation model; estimated exit-fee reserves are not guarantees.

use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    models::{Candle, Direction},
    risk::{InstrumentRiskSpec, RiskPlan},
};

const MAX_ACCEPTED_INTENTS: usize = 100_000;

/// Validated simulation assumptions, independent from actual venue fee tiers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PaperConfig {
    #[serde(with = "crate::decimal_codec::required")]
    initial_balance: Decimal,
    #[serde(with = "crate::decimal_codec::required")]
    fee_rate: Decimal,
    #[serde(with = "crate::decimal_codec::required")]
    stop_slippage_bps: Decimal,
    #[serde(with = "crate::decimal_codec::required")]
    max_notional_fraction: Decimal,
    #[serde(with = "crate::decimal_codec::required")]
    min_net_rr: Decimal,
    // Preserve the exact v1 serialization when the legacy 1x default is used.
    #[serde(skip_serializing_if = "is_one")]
    leverage: u32,
}

impl PaperConfig {
    /// Creates cash-capped assumptions. Fractions use decimal, not percent.
    ///
    /// # Errors
    ///
    /// Rejects nonpositive cash, fees outside `[0, 1)`, slippage outside
    /// `[0, 10000)` basis points, notional caps outside `(0, 1]`, or net RR < 2.
    pub fn new(
        initial_balance: Decimal,
        fee_rate: Decimal,
        stop_slippage_bps: Decimal,
        max_notional_fraction: Decimal,
        min_net_rr: Decimal,
    ) -> Result<Self, PaperError> {
        if initial_balance <= Decimal::ZERO {
            return Err(PaperError::InvalidConfig(
                "initial balance must be positive",
            ));
        }
        if fee_rate < Decimal::ZERO || fee_rate >= Decimal::ONE {
            return Err(PaperError::InvalidConfig("fee rate must be in [0, 1)"));
        }
        if stop_slippage_bps < Decimal::ZERO || stop_slippage_bps >= Decimal::new(10_000, 0) {
            return Err(PaperError::InvalidConfig(
                "stop slippage must be in [0, 10000) bps",
            ));
        }
        if max_notional_fraction <= Decimal::ZERO || max_notional_fraction > Decimal::ONE {
            return Err(PaperError::InvalidConfig("notional cap must be in (0, 1]"));
        }
        if min_net_rr < Decimal::TWO {
            return Err(PaperError::InvalidConfig(
                "minimum net reward/risk must be at least two",
            ));
        }
        Ok(Self {
            initial_balance,
            fee_rate,
            stop_slippage_bps,
            max_notional_fraction,
            min_net_rr,
            leverage: 1,
        })
    }

    /// Selects initial-margin leverage without changing risk-approved quantity.
    ///
    /// This is an affordability assumption, not an exchange margin, funding, or
    /// liquidation model. The default remains one for legacy session replay.
    ///
    /// # Errors
    ///
    /// Rejects leverage outside `1..=10`.
    pub fn with_leverage(mut self, leverage: u32) -> Result<Self, PaperError> {
        if !(1..=10).contains(&leverage) {
            return Err(PaperError::InvalidConfig("leverage must be in 1..=10"));
        }
        self.leverage = leverage;
        Ok(self)
    }

    /// Initial notional divided by this factor determines simulated margin.
    #[must_use]
    pub const fn leverage(&self) -> u32 {
        self.leverage
    }

    /// Initial quote-currency paper cash, defaulting to 100.
    #[must_use]
    pub const fn initial_balance(&self) -> Decimal {
        self.initial_balance
    }

    /// Assumed fee fraction applied independently on both entry and exit.
    #[must_use]
    pub const fn fee_rate(&self) -> Decimal {
        self.fee_rate
    }

    /// Adverse stop slippage in basis points, before adverse tick rounding.
    #[must_use]
    pub const fn stop_slippage_bps(&self) -> Decimal {
        self.stop_slippage_bps
    }

    /// Maximum fraction of cash available for entry capital and cost reserves.
    ///
    /// At 1x this retains the legacy notional-plus-entry-fee cap. Above 1x it
    /// caps initial margin plus entry and estimated protective-exit fees.
    #[must_use]
    pub const fn max_notional_fraction(&self) -> Decimal {
        self.max_notional_fraction
    }

    /// Minimum planned net reward divided by fee-inclusive adverse stop loss.
    #[must_use]
    pub const fn min_net_rr(&self) -> Decimal {
        self.min_net_rr
    }
}

impl Default for PaperConfig {
    fn default() -> Self {
        Self {
            initial_balance: Decimal::new(100, 0),
            fee_rate: Decimal::new(6, 4),
            stop_slippage_bps: Decimal::TWO,
            max_notional_fraction: Decimal::ONE,
            min_net_rr: Decimal::TWO,
            leverage: 1,
        }
    }
}

// Serde's skip_serializing_if callback requires a shared reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_one(value: &u32) -> bool {
    *value == 1
}

/// Deterministic broker validation or arithmetic failure.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PaperError {
    /// Invalid simulation assumptions.
    #[error("invalid paper configuration: {0}")]
    InvalidConfig(&'static str),
    /// An approval does not match this broker's contract or current timeline.
    #[error("invalid paper approval: {0}")]
    InvalidApproval(&'static str),
    /// This single-position broker already owns an entry or position.
    #[error("paper broker already has a pending entry or position")]
    Busy,
    /// No valid step-aligned minimum order remains after conservative sizing.
    #[error("paper order is below minimum quantity or notional")]
    MinimumOrder,
    /// There is no positive cash budget for initial margin and execution costs.
    #[error("paper cash is insufficient for another entry")]
    InsufficientBalance,
    /// The approval's target does not meet net RR after assumed execution costs.
    #[error("paper order fails minimum net reward/risk after fees and slippage")]
    NetRiskReward,
    /// Reusing an intent identifier with different immutable approval data.
    #[error("conflicting paper approval replay")]
    ConflictingReplay,
    /// Candle shape, finality, instrument, or timeframe is inconsistent.
    #[error("invalid paper candle: {0}")]
    InvalidCandle(&'static str),
    /// A previously processed candle changed its content.
    #[error("conflicting duplicate paper candle")]
    ConflictingDuplicate,
    /// Candles arrived out of chronological order.
    #[error("paper candle is out of order")]
    OutOfOrder,
    /// Missing candle intervals prevent conservative stop execution.
    #[error("paper candle sequence has a gap")]
    Gap,
    /// A checked decimal or counter operation could not be represented.
    #[error("paper arithmetic overflow or invalid division")]
    Arithmetic,
    /// Replay retention reached its explicit bound; history cannot be dropped.
    #[error("paper replay retention capacity reached")]
    Capacity,
}

impl PaperError {
    /// Whether an entry may be rejected without treating market state as corrupt.
    #[must_use]
    pub const fn is_rejection(&self) -> bool {
        matches!(
            self,
            Self::Busy | Self::MinimumOrder | Self::InsufficientBalance | Self::NetRiskReward
        )
    }
}

/// Why a resting simulated entry was cancelled.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperCancelReason {
    /// Its entry deadline passed before the candle began.
    Expired,
    /// The caller explicitly cancelled it, for example during shutdown.
    Requested,
}

/// Which protective exit completed the simulated position.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperExitReason {
    /// Adverse stop execution, including configured slippage.
    StopLoss,
    /// Limit-like take profit at exactly the approved target.
    TakeProfit,
}

/// Observed simulated lifecycle events; candle fills are timestamped at close.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum PaperEvent {
    /// Complete entry fill, with no exchange order or partial-fill implication.
    EntryFilled {
        /// Owning strategy intent.
        intent_id: Uuid,
        /// Candle close when the simulated fill became observable.
        at: DateTime<Utc>,
        /// Approved resting limit, without favorable price improvement.
        #[serde(with = "crate::decimal_codec::required")]
        price: Decimal,
        /// Actual simulated quantity, never exceeding the risk-approved amount.
        #[serde(with = "crate::decimal_codec::required")]
        quantity: Decimal,
        /// Entry fee deducted from cash immediately.
        #[serde(with = "crate::decimal_codec::required")]
        fee: Decimal,
    },
    /// Pending entry released without a position fill.
    EntryCancelled {
        /// Owning strategy intent.
        intent_id: Uuid,
        /// Cancellation observation time.
        at: DateTime<Utc>,
        /// Cancellation cause.
        reason: PaperCancelReason,
    },
    /// Complete protective exit.
    PositionClosed {
        /// Owning strategy intent.
        intent_id: Uuid,
        /// Candle close when the exit became observable.
        at: DateTime<Utc>,
        /// Simulated exit after adverse stop slippage, if applicable.
        #[serde(with = "crate::decimal_codec::required")]
        exit_price: Decimal,
        /// Complete position quantity.
        #[serde(with = "crate::decimal_codec::required")]
        quantity: Decimal,
        /// Directional price movement before either fee.
        #[serde(with = "crate::decimal_codec::required")]
        gross_pnl: Decimal,
        /// Whole-trade profit after both entry and exit fees.
        #[serde(with = "crate::decimal_codec::required")]
        net_pnl: Decimal,
        /// Exit fee only; entry fee was already recorded on entry.
        #[serde(with = "crate::decimal_codec::required")]
        fee: Decimal,
        /// Protective exit cause.
        reason: PaperExitReason,
    },
}

/// Read-only geometry of the one pending entry or open paper position.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PaperOrderSnapshot {
    /// Owning strategy intent.
    pub intent_id: Uuid,
    /// Long or short direction.
    pub direction: Direction,
    /// Approved resting entry price.
    #[serde(with = "crate::decimal_codec::required")]
    pub entry_price: Decimal,
    /// Conservative actual paper quantity.
    #[serde(with = "crate::decimal_codec::required")]
    pub quantity: Decimal,
    /// Stop trigger before assumed slippage.
    #[serde(with = "crate::decimal_codec::required")]
    pub stop_loss_price: Decimal,
    /// Exact take-profit execution price.
    #[serde(with = "crate::decimal_codec::required")]
    pub take_profit_price: Decimal,
    /// Deadline for entry only; open positions do not expire.
    pub expires_at: DateTime<Utc>,
}

/// Cash and mark-to-market values after the latest accepted closed candle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PaperSnapshot {
    /// Starting quote-currency cash.
    #[serde(with = "crate::decimal_codec::required")]
    pub initial_balance: Decimal,
    /// Settled cash, including paid entry fees on an open position.
    #[serde(with = "crate::decimal_codec::required")]
    pub balance: Decimal,
    /// Cash plus open price profit minus the estimated exit fee at latest close.
    #[serde(with = "crate::decimal_codec::required")]
    pub equity: Decimal,
    /// Cash minus initial cash, including all fees already paid.
    #[serde(with = "crate::decimal_codec::required")]
    pub realized_pnl: Decimal,
    /// Open gross price profit minus the estimated mark-price exit fee.
    #[serde(with = "crate::decimal_codec::required")]
    pub unrealized_pnl: Decimal,
    /// Sum of fees actually paid; estimated exit fees are excluded.
    #[serde(with = "crate::decimal_codec::required")]
    pub fees_paid: Decimal,
    /// Number of fully closed trades.
    pub completed_trades: u64,
    /// Fully closed trades with strictly positive whole-trade net profit.
    pub wins: u64,
    /// Fully closed trades with strictly negative whole-trade net profit.
    pub losses: u64,
    /// Resting entry, if any.
    pub pending: Option<PaperOrderSnapshot>,
    /// Open position, if any; end-of-data does not remove it.
    pub position: Option<PaperOrderSnapshot>,
}

#[derive(Clone, Debug)]
struct PaperOrder {
    plan: RiskPlan,
    quantity: Decimal,
    entry_fee: Decimal,
}

impl PaperOrder {
    fn snapshot(&self) -> PaperOrderSnapshot {
        PaperOrderSnapshot {
            intent_id: self.plan.intent_id(),
            direction: self.plan.direction(),
            entry_price: self.plan.entry_price(),
            quantity: self.quantity,
            stop_loss_price: self.plan.stop_loss_price(),
            take_profit_price: self.plan.take_profit_price(),
            expires_at: self.plan.expires_at(),
        }
    }
}

/// Single-exposure, deterministic simulation state with shared cash accounting.
///
/// Mutating methods stage all changes, so any error preserves the old state.
/// An exact latest-candle duplicate and an exact accepted-approval replay are
/// idempotent. Older candles, gaps, and conflicting duplicates fail closed.
/// Instrument selection may change only while completely flat; the caller must
/// supply instruments using the same account quote currency and synchronized
/// closed candles. Leverage models initial margin only, not liquidation.
#[derive(Clone, Debug)]
pub struct PaperBroker {
    config: PaperConfig,
    spec: InstrumentRiskSpec,
    min_notional: Decimal,
    pending: Option<PaperOrder>,
    position: Option<PaperOrder>,
    // Staging each candle shares immutable tombstones instead of cloning the
    // complete trade history. Copy-on-write is needed only for a new approval.
    accepted: Arc<HashMap<Uuid, RiskPlan>>,
    last_candle: Option<Candle>,
    metrics: PaperSnapshot,
}

impl PaperBroker {
    /// Funds a one-shot v3 execution sleeve from an already account-capped budget.
    /// The returned allocation, not the sizing budget, is its actual cash seed.
    /// Existing v1/v2 construction, sizing and serialized accounting are unchanged.
    pub(crate) fn allocate_order(
        config: PaperConfig,
        spec: InstrumentRiskSpec,
        min_notional: Decimal,
        plan: RiskPlan,
        signal_candle: &Candle,
        cash_budget: Decimal,
    ) -> Result<(Self, Decimal), PaperError> {
        if cash_budget <= Decimal::ZERO {
            return Err(PaperError::InsufficientBalance);
        }
        let sizing_config = PaperConfig::new(
            cash_budget, config.fee_rate, config.stop_slippage_bps,
            Decimal::ONE, config.min_net_rr,
        )?.with_leverage(config.leverage)?;
        let mut sleeve = Self::new(sizing_config, spec, min_notional)?;
        sleeve.select_instrument(sleeve.spec.clone(), min_notional, signal_candle)?;
        sleeve.submit(plan)?;
        let mut order = sleeve.pending.take().ok_or(PaperError::Arithmetic)?;
        let stop = sleeve.stop_price(order.plan.stop_loss_price(), order.plan.direction())?;
        // RiskManager reserves planned_loss, which can be below risk_budget
        // after step rounding. All execution costs must fit that reservation.
        let unit_risk = sleeve.stop_risk(&order.plan, Decimal::ONE, stop)?;
        let unit_capital = sleeve.allocation_capital(&order.plan, Decimal::ONE, true)?;
        order.quantity = floor_step(order.quantity
            .min(div(order.plan.planned_loss(), unit_risk)?)
            .min(div(cash_budget, unit_capital)?), sleeve.spec.quantity_step())?;
        if sleeve.stop_risk(&order.plan, order.quantity, stop)? > order.plan.planned_loss()
            || sleeve.allocation_capital(&order.plan, order.quantity, true)? > cash_budget
        {
            order.quantity = sub(order.quantity, sleeve.spec.quantity_step())?;
        }
        if order.quantity <= Decimal::ZERO || order.quantity < sleeve.spec.minimum_quantity()
            || sleeve.notional(order.plan.entry_price(), order.quantity)? < min_notional
        {
            return Err(PaperError::MinimumOrder);
        }
        let actual_risk = sleeve.stop_risk(&order.plan, order.quantity, stop)?;
        let allocation = sleeve.allocation_capital(&order.plan, order.quantity, true)?;
        if order.quantity > order.plan.quantity() || actual_risk > order.plan.planned_loss()
            || allocation > cash_budget || allocation <= Decimal::ZERO
        {
            return Err(PaperError::Arithmetic);
        }
        order.entry_fee = sleeve.fee(order.plan.entry_price(), order.quantity)?;
        let reward = sub(sub(sleeve.gross_pnl(&order, order.plan.take_profit_price())?, order.entry_fee)?,
            sleeve.fee(order.plan.take_profit_price(), order.quantity)?)?;
        if reward < mul(actual_risk, config.min_net_rr)? {
            return Err(PaperError::NetRiskReward);
        }
        // No fill or accounting event has happened: rebasing this pristine
        // pending sleeve transfers exactly the capital supplied by its owner.
        sleeve.config = PaperConfig::new(allocation, config.fee_rate,
            config.stop_slippage_bps, Decimal::ONE, config.min_net_rr)?
            .with_leverage(config.leverage)?;
        sleeve.metrics.initial_balance = allocation;
        sleeve.metrics.balance = allocation;
        sleeve.metrics.equity = allocation;
        sleeve.metrics.pending = Some(order.snapshot());
        sleeve.pending = Some(order);
        Ok((sleeve, allocation))
    }

    /// Remaining initial margin and fee reserve; entry fees already paid are
    /// excluded for open positions. This is not a liquidation-price model.
    pub(crate) fn reserved_capital(&self) -> Result<Decimal, PaperError> {
        if let Some(order) = &self.pending {
            self.allocation_capital(&order.plan, order.quantity, true)
        } else if let Some(order) = &self.position {
            self.allocation_capital(&order.plan, order.quantity, false)
        } else {
            Ok(Decimal::ZERO)
        }
    }

    fn allocation_capital(&self, plan: &RiskPlan, quantity: Decimal, pending: bool) -> Result<Decimal, PaperError> {
        let stop = self.stop_price(plan.stop_loss_price(), plan.direction())?;
        let exit_fee = self.fee(stop, quantity)?.max(self.fee(plan.take_profit_price(), quantity)?);
        let entry_fee = if pending { self.fee(plan.entry_price(), quantity)? } else { Decimal::ZERO };
        let mut margin = self.initial_margin(plan.entry_price(), quantity)?;
        // A rounded division must never allocate less cash than the notional
        // requires. This upward reserve applies only to new v3 sleeves.
        if mul(margin, Decimal::from(self.config.leverage))? < self.notional(plan.entry_price(), quantity)? {
            margin = add(margin, Decimal::new(1, margin.scale()))?;
        }
        add(margin, add(entry_fee, exit_fee)?)
    }

    /// Starts a fresh broker, without credentials or any exchange transport.
    ///
    /// # Errors
    ///
    /// Returns [`PaperError::InvalidConfig`] for a negative minimum notional.
    pub fn new(
        config: PaperConfig,
        spec: InstrumentRiskSpec,
        min_notional: Decimal,
    ) -> Result<Self, PaperError> {
        if min_notional < Decimal::ZERO {
            return Err(PaperError::InvalidConfig(
                "minimum notional cannot be negative",
            ));
        }
        Ok(Self {
            config,
            spec,
            min_notional,
            pending: None,
            position: None,
            accepted: Arc::new(HashMap::new()),
            last_candle: None,
            metrics: PaperSnapshot {
                initial_balance: config.initial_balance,
                balance: config.initial_balance,
                equity: config.initial_balance,
                realized_pnl: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                fees_paid: Decimal::ZERO,
                completed_trades: 0,
                wins: 0,
                losses: 0,
                pending: None,
                position: None,
            },
        })
    }

    /// Validated execution assumptions.
    #[must_use]
    pub const fn config(&self) -> &PaperConfig {
        &self.config
    }

    /// Instrument constraints used for sizing and adverse tick rounding.
    #[must_use]
    pub const fn spec(&self) -> &InstrumentRiskSpec {
        &self.spec
    }

    /// Selects another contract while preserving the shared account and history.
    ///
    /// The supplied final candle becomes the execution cursor, so a subsequent
    /// approval cannot fill on the already observed candle. A different symbol
    /// may use the same close time as the previous symbol. This method does not
    /// reset cash, fees, trade counters, or accepted-approval tombstones.
    ///
    /// The caller must ensure both contracts share the account quote currency.
    /// It must also validate each independent symbol's candle continuity before
    /// selecting it; a flat broker does not reconstruct the inactive series.
    ///
    /// # Errors
    ///
    /// Rejects occupied state, invalid minimum notional, exchange/market changes,
    /// mismatched/nonfinal/misaligned candles, timeframe changes, regressions, or
    /// a conflicting same-symbol duplicate. Errors leave all state unchanged.
    pub fn select_instrument(
        &mut self,
        spec: InstrumentRiskSpec,
        min_notional: Decimal,
        last_closed_candle: &Candle,
    ) -> Result<(), PaperError> {
        if !self.is_flat() {
            return Err(PaperError::Busy);
        }
        if min_notional < Decimal::ZERO {
            return Err(PaperError::InvalidConfig("minimum notional cannot be negative"));
        }
        if spec.instrument().exchange != self.spec.instrument().exchange
            || spec.instrument().market != self.spec.instrument().market
        {
            return Err(PaperError::InvalidConfig("instrument selection must stay in one venue and market"));
        }
        if last_closed_candle.instrument() != spec.instrument()
            || !last_closed_candle.is_closed()
            || last_closed_candle.close_time().signed_duration_since(last_closed_candle.open_time())
                != last_closed_candle.timeframe().duration()
            || last_closed_candle.open_time().timestamp_subsec_nanos() != 0
            || last_closed_candle.open_time().timestamp()
                .rem_euclid(last_closed_candle.timeframe().duration().num_seconds()) != 0
        {
            return Err(PaperError::InvalidCandle("instrument selection needs its final aligned candle"));
        }
        if let Some(previous) = &self.last_candle {
            if previous.timeframe() != last_closed_candle.timeframe() {
                return Err(PaperError::InvalidCandle("timeframe changed"));
            }
            if last_closed_candle.close_time() < previous.close_time() {
                return Err(PaperError::OutOfOrder);
            }
            if previous.instrument() == last_closed_candle.instrument()
                && previous.open_time() == last_closed_candle.open_time()
                && previous != last_closed_candle
            {
                return Err(PaperError::ConflictingDuplicate);
            }
        }
        self.spec = spec;
        self.min_notional = min_notional;
        self.last_candle = Some(last_closed_candle.clone());
        Ok(())
    }

    /// Returns notional divided by configured leverage, excluding all fee reserves.
    ///
    /// This reports initial-margin accounting only and does not estimate the
    /// exchange's maintenance margin or liquidation price.
    ///
    /// # Errors
    ///
    /// Rejects nonpositive prices, negative quantities, or arithmetic overflow.
    pub fn initial_margin(&self, price: Decimal, quantity: Decimal) -> Result<Decimal, PaperError> {
        if price <= Decimal::ZERO || quantity < Decimal::ZERO {
            return Err(PaperError::InvalidApproval("invalid margin-report price or quantity"));
        }
        let margin = div(self.notional(price, quantity)?, Decimal::from(self.config.leverage))?;
        if quantity > Decimal::ZERO && margin <= Decimal::ZERO {
            return Err(PaperError::Arithmetic);
        }
        Ok(margin)
    }

    /// Whether neither a pending entry nor an open position is owned.
    #[must_use]
    pub const fn is_flat(&self) -> bool {
        self.pending.is_none() && self.position.is_none()
    }

    /// Whether there is an open simulated position.
    #[must_use]
    pub const fn has_position(&self) -> bool {
        self.position.is_some()
    }

    /// Identifier of the resting entry, if any.
    #[must_use]
    pub fn pending_intent(&self) -> Option<Uuid> {
        self.pending.as_ref().map(|order| order.plan.intent_id())
    }

    /// Clones the current report; decimal JSON values are always strings.
    #[must_use]
    pub fn snapshot(&self) -> PaperSnapshot {
        self.metrics.clone()
    }

    /// Rests a risk-approved limit; submission itself can never fill an order.
    ///
    /// Quantity is reduced for fees/slippage and the configured entry-capital cap.
    /// Leverage changes affordability only; the original quantity and complete
    /// fee-inclusive stop-risk budget remain ceilings, never multiplied by leverage.
    /// The original external risk reservation must remain until cancellation
    /// or complete close, even when this simulator selects a smaller quantity.
    ///
    /// # Errors
    ///
    /// Rejects mismatched approvals, conflicting replays, occupied state,
    /// insufficient cash/minimum size/net RR, or unrepresentable arithmetic.
    pub fn submit(&mut self, plan: RiskPlan) -> Result<Vec<PaperEvent>, PaperError> {
        if let Some(previous) = self.accepted.get(&plan.intent_id()) {
            return if previous == &plan {
                Ok(Vec::new())
            } else {
                Err(PaperError::ConflictingReplay)
            };
        }
        if !self.is_flat() {
            return Err(PaperError::Busy);
        }
        if self.accepted.len() >= MAX_ACCEPTED_INTENTS {
            return Err(PaperError::Capacity);
        }
        self.validate_plan(&plan)?;
        let quantity = self.size(&plan)?;
        let entry_fee = self.fee(plan.entry_price(), quantity)?;
        let order = PaperOrder {
            plan,
            quantity,
            entry_fee,
        };
        // All fallible validation/arithmetic precedes this commit.
        Arc::make_mut(&mut self.accepted).insert(order.plan.intent_id(), order.plan.clone());
        self.metrics.pending = Some(order.snapshot());
        self.pending = Some(order);
        Ok(Vec::new())
    }

    /// Processes one consecutive final candle, applying adverse-first OHLC rules.
    ///
    /// Limits require a strict trade through their price; a touch is not a fill.
    /// The entry candle cannot take profit, because intra-bar ordering is unknown.
    /// A stop on that candle is nevertheless assumed to execute. Both-hit bars
    /// execute the stop. Fills are observed at candle close, never backdated.
    ///
    /// # Errors
    ///
    /// Rejects gaps, changed duplicates, out-of-order/inconsistent/nonfinal bars,
    /// or arithmetic failure; all state remains unchanged after an error.
    pub fn advance(&mut self, candle: &Candle) -> Result<Vec<PaperEvent>, PaperError> {
        if self.validate_candle(candle)? {
            return Ok(Vec::new());
        }
        let mut staged = self.clone();
        let events = staged.advance_inner(candle)?;
        staged.last_candle = Some(candle.clone());
        staged.refresh(candle.close())?;
        *self = staged;
        Ok(events)
    }

    /// Cancels only a pending entry, never forcibly closing an open position.
    ///
    /// # Errors
    ///
    /// Rejects a cancellation earlier than approval or the latest observed bar.
    pub fn cancel_pending(&mut self, at: DateTime<Utc>) -> Result<Vec<PaperEvent>, PaperError> {
        let Some(order) = self.pending.as_ref() else {
            return Ok(Vec::new());
        };
        if at < order.plan.approved_at()
            || self
                .last_candle
                .as_ref()
                .is_some_and(|bar| at < bar.close_time())
        {
            return Err(PaperError::InvalidApproval(
                "cancellation predates observed state",
            ));
        }
        let event = PaperEvent::EntryCancelled {
            intent_id: order.plan.intent_id(),
            at,
            reason: PaperCancelReason::Requested,
        };
        self.pending = None;
        self.metrics.pending = None;
        Ok(vec![event])
    }

    fn validate_plan(&self, plan: &RiskPlan) -> Result<(), PaperError> {
        if plan.instrument() != self.spec.instrument() || plan.approved_spec() != &self.spec {
            return Err(PaperError::InvalidApproval(
                "instrument specification differs from approval",
            ));
        }
        if self
            .last_candle
            .as_ref()
            .is_some_and(|bar| plan.approved_at() < bar.close_time())
        {
            return Err(PaperError::InvalidApproval(
                "approval predates already processed candles",
            ));
        }
        Ok(())
    }

    fn size(&self, plan: &RiskPlan) -> Result<Decimal, PaperError> {
        if self.metrics.balance <= Decimal::ZERO {
            return Err(PaperError::InsufficientBalance);
        }
        let stop = self.stop_price(plan.stop_loss_price(), plan.direction())?;
        let entry_fee = self.fee(plan.entry_price(), Decimal::ONE)?;
        let stop_fee = self.fee(stop, Decimal::ONE)?;
        let target_fee = self.fee(plan.take_profit_price(), Decimal::ONE)?;
        let stop_move = directional_move(plan.direction(), stop, plan.entry_price())?;
        let risk = add(
            mul(stop_move, self.spec.quote_value_per_price_unit())?,
            add(entry_fee, stop_fee)?,
        )?;
        let reward_move = directional_move(
            plan.direction(),
            plan.entry_price(),
            plan.take_profit_price(),
        )?;
        let reward = sub(
            mul(reward_move, self.spec.quote_value_per_price_unit())?,
            add(entry_fee, target_fee)?,
        )?;
        if risk <= Decimal::ZERO || reward < mul(risk, self.config.min_net_rr)? {
            return Err(PaperError::NetRiskReward);
        }
        let cash = mul(self.metrics.balance, self.config.max_notional_fraction)?;
        let unit_cash = if self.config.leverage == 1 {
            // Keep the v1 arithmetic path byte-for-byte equivalent for replay.
            add(
                mul(plan.entry_price(), self.spec.quote_value_per_price_unit())?,
                entry_fee,
            )?
        } else {
            self.entry_capital(plan, Decimal::ONE, stop)?
        };
        let quantity = plan
            .quantity()
            .min(div(plan.risk_budget(), risk)?)
            .min(div(cash, unit_cash)?);
        let mut quantity = floor_step(quantity, self.spec.quantity_step())?;
        // Decimal division can round at its final digit; verify the final cash
        // and risk products instead of trusting a rounded quotient.
        if !self.quantity_fits(plan, quantity, stop, cash)? {
            quantity = sub(quantity, self.spec.quantity_step())?;
        }
        if quantity <= Decimal::ZERO
            || quantity < self.spec.minimum_quantity()
            || self.notional(plan.entry_price(), quantity)? < self.min_notional
        {
            return Err(PaperError::MinimumOrder);
        }
        if !self.quantity_fits(plan, quantity, stop, cash)? {
            return Err(PaperError::Arithmetic);
        }
        // Recheck actual-size economics: Decimal multiplication may round at
        // its maximum scale, so unit-cost products alone are not authoritative.
        let actual_risk = self.stop_risk(plan, quantity, stop)?;
        let actual_reward = sub(
            mul(
                mul(reward_move, quantity)?,
                self.spec.quote_value_per_price_unit(),
            )?,
            add(
                self.fee(plan.entry_price(), quantity)?,
                self.fee(plan.take_profit_price(), quantity)?,
            )?,
        )?;
        if actual_reward < mul(actual_risk, self.config.min_net_rr)? {
            return Err(PaperError::NetRiskReward);
        }
        Ok(quantity)
    }

    fn quantity_fits(
        &self,
        plan: &RiskPlan,
        quantity: Decimal,
        stop: Decimal,
        cash: Decimal,
    ) -> Result<bool, PaperError> {
        let capital_fits = if self.config.leverage == 1 {
            self.entry_capital(plan, quantity, stop)? <= cash
        } else {
            let costs = add(
                self.fee(plan.entry_price(), quantity)?,
                self.fee(stop, quantity)?.max(self.fee(plan.take_profit_price(), quantity)?),
            )?;
            // Do not approve a last-digit overrun hidden by division rounding
            // in the candidate's initial-margin estimate.
            costs <= cash && self.notional(plan.entry_price(), quantity)?
                <= mul(sub(cash, costs)?, Decimal::from(self.config.leverage))?
        };
        Ok(quantity <= plan.quantity()
            && capital_fits
            && self.stop_risk(plan, quantity, stop)? <= plan.risk_budget())
    }

    fn entry_capital(
        &self,
        plan: &RiskPlan,
        quantity: Decimal,
        stop: Decimal,
    ) -> Result<Decimal, PaperError> {
        let entry_fee = self.fee(plan.entry_price(), quantity)?;
        if self.config.leverage == 1 {
            return add(self.notional(plan.entry_price(), quantity)?, entry_fee);
        }
        // Reserve the greater planned exit fee, not just the cheaper stop fee.
        // Adverse gaps may still exceed this estimate; there is no loss clipping.
        let exit_fee = self.fee(stop, quantity)?.max(self.fee(plan.take_profit_price(), quantity)?);
        add(self.initial_margin(plan.entry_price(), quantity)?, add(entry_fee, exit_fee)?)
    }

    fn stop_risk(
        &self,
        plan: &RiskPlan,
        quantity: Decimal,
        stop: Decimal,
    ) -> Result<Decimal, PaperError> {
        let loss = mul(
            mul(
                directional_move(plan.direction(), stop, plan.entry_price())?,
                quantity,
            )?,
            self.spec.quote_value_per_price_unit(),
        )?;
        add(
            loss,
            add(
                self.fee(plan.entry_price(), quantity)?,
                self.fee(stop, quantity)?,
            )?,
        )
    }

    fn validate_candle(&self, candle: &Candle) -> Result<bool, PaperError> {
        if !candle.is_closed() || candle.instrument() != self.spec.instrument() {
            return Err(PaperError::InvalidCandle(
                "expected final candle for configured instrument",
            ));
        }
        if candle
            .close_time()
            .signed_duration_since(candle.open_time())
            != candle.timeframe().duration()
        {
            return Err(PaperError::InvalidCandle(
                "candle duration differs from timeframe",
            ));
        }
        if let Some(previous) = &self.last_candle {
            if candle.timeframe() != previous.timeframe() {
                return Err(PaperError::InvalidCandle("timeframe changed"));
            }
            if candle.open_time() == previous.open_time() {
                return if candle == previous {
                    Ok(true)
                } else {
                    Err(PaperError::ConflictingDuplicate)
                };
            }
            if candle.open_time() < previous.close_time() {
                return Err(PaperError::OutOfOrder);
            }
            if candle.open_time() > previous.close_time() {
                return Err(PaperError::Gap);
            }
        }
        Ok(false)
    }

    fn advance_inner(&mut self, candle: &Candle) -> Result<Vec<PaperEvent>, PaperError> {
        let mut events = Vec::new();
        let preexisting = self.position.is_some();
        if let Some(order) = &self.pending {
            if candle.open_time() >= order.plan.expires_at() {
                events.push(PaperEvent::EntryCancelled {
                    intent_id: order.plan.intent_id(),
                    at: candle.close_time(),
                    reason: PaperCancelReason::Expired,
                });
                self.pending = None;
            } else if candle.open_time() >= order.plan.approved_at() && crosses_entry(order, candle)
            {
                let order = self.pending.take().ok_or(PaperError::Arithmetic)?;
                self.metrics.balance = sub(self.metrics.balance, order.entry_fee)?;
                self.metrics.fees_paid = add(self.metrics.fees_paid, order.entry_fee)?;
                events.push(PaperEvent::EntryFilled {
                    intent_id: order.plan.intent_id(),
                    at: candle.close_time(),
                    price: order.plan.entry_price(),
                    quantity: order.quantity,
                    fee: order.entry_fee,
                });
                self.position = Some(order);
            }
        }
        if let Some(order) = &self.position {
            if let Some((price, reason)) = self.exit_price(order, candle, preexisting)? {
                events.push(self.close_position(price, reason, candle.close_time())?);
            }
        }
        Ok(events)
    }

    fn exit_price(
        &self,
        order: &PaperOrder,
        candle: &Candle,
        allow_target: bool,
    ) -> Result<Option<(Decimal, PaperExitReason)>, PaperError> {
        let plan = &order.plan;
        let stop = plan.stop_loss_price();
        let (stop_hit, target_hit, adverse_base) = match plan.direction() {
            Direction::Bullish => (
                candle.low() <= stop,
                candle.high() >= plan.take_profit_price(),
                candle.open().min(stop),
            ),
            Direction::Bearish => (
                candle.high() >= stop,
                candle.low() <= plan.take_profit_price(),
                candle.open().max(stop),
            ),
        };
        if stop_hit {
            return Ok(Some((
                self.stop_price(adverse_base, plan.direction())?,
                PaperExitReason::StopLoss,
            )));
        }
        Ok((allow_target && target_hit)
            .then_some((plan.take_profit_price(), PaperExitReason::TakeProfit)))
    }

    fn close_position(
        &mut self,
        price: Decimal,
        reason: PaperExitReason,
        at: DateTime<Utc>,
    ) -> Result<PaperEvent, PaperError> {
        let order = self.position.take().ok_or(PaperError::Arithmetic)?;
        let gross_pnl = self.gross_pnl(&order, price)?;
        let fee = self.fee(price, order.quantity)?;
        let net_pnl = sub(sub(gross_pnl, fee)?, order.entry_fee)?;
        self.metrics.balance = add(self.metrics.balance, sub(gross_pnl, fee)?)?;
        self.metrics.fees_paid = add(self.metrics.fees_paid, fee)?;
        self.metrics.completed_trades = self
            .metrics
            .completed_trades
            .checked_add(1)
            .ok_or(PaperError::Arithmetic)?;
        if net_pnl > Decimal::ZERO {
            self.metrics.wins = self
                .metrics
                .wins
                .checked_add(1)
                .ok_or(PaperError::Arithmetic)?;
        }
        if net_pnl < Decimal::ZERO {
            self.metrics.losses = self
                .metrics
                .losses
                .checked_add(1)
                .ok_or(PaperError::Arithmetic)?;
        }
        Ok(PaperEvent::PositionClosed {
            intent_id: order.plan.intent_id(),
            at,
            exit_price: price,
            quantity: order.quantity,
            gross_pnl,
            net_pnl,
            fee,
            reason,
        })
    }

    fn refresh(&mut self, mark: Decimal) -> Result<(), PaperError> {
        self.metrics.unrealized_pnl = match &self.position {
            Some(order) => sub(
                self.gross_pnl(order, mark)?,
                self.fee(mark, order.quantity)?,
            )?,
            None => Decimal::ZERO,
        };
        self.metrics.realized_pnl = sub(self.metrics.balance, self.config.initial_balance)?;
        self.metrics.equity = add(self.metrics.balance, self.metrics.unrealized_pnl)?;
        self.metrics.pending = self.pending.as_ref().map(PaperOrder::snapshot);
        self.metrics.position = self.position.as_ref().map(PaperOrder::snapshot);
        Ok(())
    }

    fn stop_price(&self, base: Decimal, direction: Direction) -> Result<Decimal, PaperError> {
        let slip = div(self.config.stop_slippage_bps, Decimal::new(10_000, 0))?;
        let factor = match direction {
            Direction::Bullish => sub(Decimal::ONE, slip)?,
            Direction::Bearish => add(Decimal::ONE, slip)?,
        };
        let ticks = div(mul(base, factor)?, self.spec.price_tick())?;
        let rounded = match direction {
            Direction::Bullish => ticks.floor(),
            Direction::Bearish => ticks.ceil(),
        };
        let price = mul(rounded, self.spec.price_tick())?;
        if price <= Decimal::ZERO {
            return Err(PaperError::Arithmetic);
        }
        Ok(price)
    }

    fn notional(&self, price: Decimal, quantity: Decimal) -> Result<Decimal, PaperError> {
        mul(
            mul(price, quantity)?,
            self.spec.quote_value_per_price_unit(),
        )
    }

    fn fee(&self, price: Decimal, quantity: Decimal) -> Result<Decimal, PaperError> {
        mul(self.notional(price, quantity)?, self.config.fee_rate)
    }

    fn gross_pnl(&self, order: &PaperOrder, price: Decimal) -> Result<Decimal, PaperError> {
        mul(
            mul(
                directional_move(order.plan.direction(), order.plan.entry_price(), price)?,
                order.quantity,
            )?,
            self.spec.quote_value_per_price_unit(),
        )
    }
}

fn crosses_entry(order: &PaperOrder, candle: &Candle) -> bool {
    match order.plan.direction() {
        Direction::Bullish => candle.low() < order.plan.entry_price(),
        Direction::Bearish => candle.high() > order.plan.entry_price(),
    }
}

fn directional_move(
    direction: Direction,
    entry: Decimal,
    exit: Decimal,
) -> Result<Decimal, PaperError> {
    match direction {
        Direction::Bullish => sub(exit, entry),
        Direction::Bearish => sub(entry, exit),
    }
}

fn floor_step(value: Decimal, step: Decimal) -> Result<Decimal, PaperError> {
    mul(div(value, step)?.floor(), step)
}
fn add(left: Decimal, right: Decimal) -> Result<Decimal, PaperError> {
    left.checked_add(right).ok_or(PaperError::Arithmetic)
}
fn sub(left: Decimal, right: Decimal) -> Result<Decimal, PaperError> {
    left.checked_sub(right).ok_or(PaperError::Arithmetic)
}
fn mul(left: Decimal, right: Decimal) -> Result<Decimal, PaperError> {
    left.checked_mul(right).ok_or(PaperError::Arithmetic)
}
fn div(left: Decimal, right: Decimal) -> Result<Decimal, PaperError> {
    left.checked_div(right).ok_or(PaperError::Arithmetic)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{error::Error, io};

    use super::*;
    use crate::execution::coordinator::tests::{at, instrument};
    use crate::{
        BreakBasis, CandleInput, Exchange, FairValueGap, FairValueGapInput, FractalModel,
        FvgEntryMode, LiquiditySide, LiquiditySweep, LiquiditySweepInput, LiquidityTarget,
        MarketStructureShift, MarketStructureShiftInput, PriceZone, RiskConfig, RiskManager,
        SwingKind, SwingPoint, Timeframe, TradeAssessment, TradeIntent, TradeIntentInput,
        VolumeUnit,
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

    fn intent(direction: Direction) -> Result<TradeIntent, Box<dyn Error>> {
        intent_with_offset(direction, 0)
    }

    pub(crate) fn intent_with_offset(direction: Direction, offset: i64) -> Result<TradeIntent, Box<dyn Error>> {
        let bullish = direction == Direction::Bullish;
        let series = instrument(Exchange::Binance)?;
        let price = |value: i64| Decimal::new(offset + if bullish { value } else { 200 - value }, 0);
        let shift = MarketStructureShift::new(MarketStructureShiftInput {
            id: Uuid::from_u128(12),
            instrument: series.clone(),
            timeframe: Timeframe::M1,
            direction,
            broken_swing: SwingPoint::new(
                series.clone(),
                Timeframe::M1,
                if bullish {
                    SwingKind::High
                } else {
                    SwingKind::Low
                },
                price(101),
                at(-60)?,
                at(60)?,
                FractalModel::ThreeCandle,
            )?,
            break_price: price(103),
            break_basis: BreakBasis::Close,
            confirming_candle_open_time: at(120)?,
            confirmed_at: at(180)?,
        })?;
        let sweep = LiquiditySweep::new(LiquiditySweepInput {
            id: Uuid::from_u128(11),
            instrument: series.clone(),
            timeframe: Timeframe::M1,
            side: if bullish {
                LiquiditySide::SellSide
            } else {
                LiquiditySide::BuySide
            },
            reference_price: price(99),
            sweep_price: price(98),
            sweep_candle_open_time: at(0)?,
            detected_at: at(60)?,
        })?;
        let gap = FairValueGap::new(FairValueGapInput {
            id: Uuid::from_u128(10),
            instrument: series.clone(),
            timeframe: Timeframe::M1,
            direction,
            zone: PriceZone::new(
                price(if bullish { 100 } else { 102 }),
                price(if bullish { 102 } else { 100 }),
            )?,
            formed_from: [at(60)?, at(120)?, at(180)?],
            formed_at: at(240)?,
        })?;
        let trigger = Candle::new(CandleInput {
            instrument: series.clone(),
            timeframe: Timeframe::M1,
            open_time: at(240)?,
            close_time: at(300)?,
            open: price(103),
            high: price(if bullish { 104 } else { 101 }),
            low: price(if bullish { 101 } else { 104 }),
            close: price(102),
            volume: Decimal::ONE,
            volume_unit: VolumeUnit::Contracts,
            trade_count: Some(1),
            is_closed: true,
        })?;
        Ok(TradeIntent::new(TradeIntentInput {
            id: Uuid::from_u128(13),
            instrument: series,
            timeframe: Timeframe::M1,
            direction,
            liquidity_sweep: sweep,
            market_structure_shift: shift,
            fair_value_gap: gap,
            entry_mode: FvgEntryMode::NearBoundary,
            trigger_candle: trigger,
            expires_at: at(600)?,
        })?)
    }

    fn approval_with(
        direction: Direction,
        equity: Decimal,
        target_price: Decimal,
        venue_spec: &InstrumentRiskSpec,
    ) -> Result<RiskPlan, Box<dyn Error>> {
        let mut manager = RiskManager::new(RiskConfig::default());
        manager.register_instrument_spec(venue_spec.clone())?;
        manager.start_trading_day(equity, equity, at(299)?, false)?;
        let target = LiquidityTarget::new(
            Uuid::from_u128(14),
            instrument(Exchange::Binance)?,
            if direction == Direction::Bullish {
                LiquiditySide::BuySide
            } else {
                LiquiditySide::SellSide
            },
            target_price,
            at(240)?,
        )?;
        match manager.assess_trade(&intent(direction)?, &target, at(300)?)? {
            TradeAssessment::Approved(plan) => Ok(*plan),
            TradeAssessment::Replay { .. } => Err(io::Error::other("expected new approval").into()),
        }
    }

    fn approval(direction: Direction) -> Result<RiskPlan, Box<dyn Error>> {
        approval_with(
            direction,
            Decimal::new(100, 0),
            Decimal::new(
                if direction == Direction::Bullish {
                    112
                } else {
                    88
                },
                0,
            ),
            &spec()?,
        )
    }

    fn broker() -> Result<PaperBroker, Box<dyn Error>> {
        Ok(PaperBroker::new(
            PaperConfig::default(),
            spec()?,
            Decimal::new(5, 0),
        )?)
    }

    fn candle(
        start: i64,
        open: i64,
        high: i64,
        low: i64,
        close: i64,
    ) -> Result<Candle, Box<dyn Error>> {
        Ok(Candle::new(CandleInput {
            instrument: instrument(Exchange::Binance)?,
            timeframe: Timeframe::M1,
            open_time: at(start)?,
            close_time: at(start + 60)?,
            open: Decimal::new(open, 0),
            high: Decimal::new(high, 0),
            low: Decimal::new(low, 0),
            close: Decimal::new(close, 0),
            volume: Decimal::ONE,
            volume_unit: VolumeUnit::Contracts,
            trade_count: Some(1),
            is_closed: true,
        })?)
    }

    fn order(snapshot: &PaperSnapshot) -> Result<&PaperOrderSnapshot, io::Error> {
        snapshot
            .pending
            .as_ref()
            .or(snapshot.position.as_ref())
            .ok_or_else(|| io::Error::other("missing paper order"))
    }

    fn high_price_approval() -> Result<RiskPlan, Box<dyn Error>> {
        let mut manager = RiskManager::new(RiskConfig::default());
        manager.register_instrument_spec(spec()?)?;
        manager.start_trading_day(Decimal::from(100), Decimal::from(100), at(299)?, false)?;
        let intent = intent_with_offset(Direction::Bullish, 10_000)?;
        let target = LiquidityTarget::new(
            Uuid::from_u128(14), instrument(Exchange::Binance)?, LiquiditySide::BuySide,
            Decimal::from(10_500), at(240)?,
        )?;
        match manager.assess_trade(&intent, &target, at(300)?)? {
            TradeAssessment::Approved(plan) => Ok(*plan),
            TradeAssessment::Replay { .. } => Err(io::Error::other("expected fresh high-price approval").into()),
        }
    }

    fn ethereum_spec() -> Result<InstrumentRiskSpec, Box<dyn Error>> {
        Ok(InstrumentRiskSpec::new(
            crate::InstrumentId::new(Exchange::Binance, crate::MarketKind::LinearPerpetual, "ETHUSDT", "ETH-USDT-PERP")?,
            Decimal::new(1, 1), Decimal::new(1, 3), Decimal::new(1, 3), None, Decimal::ONE,
        )?)
    }

    fn ethereum_candle(start: i64) -> Result<Candle, Box<dyn Error>> {
        let mut value = serde_json::to_value(candle(start, 103, 104, 101, 103)?)?;
        value["instrument"] = serde_json::to_value(ethereum_spec()?.instrument())?;
        Ok(serde_json::from_value(value)?)
    }

    #[test]
    fn default_is_one_hundred_cash_with_string_decimal_reporting() -> TestResult {
        let broker = broker()?;
        let snapshot = broker.snapshot();
        assert_eq!(snapshot.balance, Decimal::new(100, 0));
        assert_eq!(snapshot.equity, snapshot.balance);
        assert_eq!(snapshot.fees_paid, Decimal::ZERO);
        assert!(broker.is_flat());
        assert_eq!(serde_json::to_value(snapshot)?["balance"], "100");
        assert_eq!(broker.config().fee_rate(), Decimal::new(6, 4));
        assert_eq!(broker.config().stop_slippage_bps(), Decimal::TWO);
        assert!(PaperConfig::new(
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::TWO,
            Decimal::TWO
        )
        .is_err());
        assert!(PaperConfig::new(
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ONE,
            Decimal::ONE
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn sizing_covers_both_fees_slippage_and_step_without_exceeding_risk() -> TestResult {
        let mut broker = broker()?;
        let plan = approval(Direction::Bullish)?;
        assert!(broker.submit(plan.clone())?.is_empty());
        let snapshot = broker.snapshot();
        let quantity = order(&snapshot)?.quantity;
        assert!(quantity < plan.quantity());
        assert_eq!(
            quantity.checked_rem(broker.spec().quantity_step()),
            Some(Decimal::ZERO)
        );
        let events = broker.advance(&candle(300, 103, 104, 97, 100)?)?;
        assert_eq!(events.len(), 2);
        let PaperEvent::PositionClosed {
            net_pnl,
            exit_price,
            ..
        } = events[1]
        else {
            return Err(io::Error::other("expected same-bar stop").into());
        };
        assert_eq!(exit_price, Decimal::new(978, 1));
        assert!(net_pnl < Decimal::ZERO);
        assert!(sub(Decimal::ZERO, net_pnl)? <= plan.risk_budget());
        assert_eq!(broker.snapshot().realized_pnl, net_pnl);
        assert_eq!(broker.snapshot().losses, 1);
        Ok(())
    }

    #[test]
    fn notional_plus_entry_fee_cannot_leverage_one_hundred_cash() -> TestResult {
        let mut broker = broker()?;
        let plan = approval_with(
            Direction::Bullish,
            Decimal::new(10_000, 0),
            Decimal::new(112, 0),
            &spec()?,
        )?;
        broker.submit(plan)?;
        let snapshot = broker.snapshot();
        let order = order(&snapshot)?;
        let cost = add(
            broker.notional(order.entry_price, order.quantity)?,
            broker.fee(order.entry_price, order.quantity)?,
        )?;
        assert!(cost <= Decimal::new(100, 0));
        assert_eq!(order.quantity, Decimal::new(979, 3));
        Ok(())
    }

    #[test]
    fn signal_bar_and_limit_touch_do_not_fill() -> TestResult {
        let mut broker = broker()?;
        broker.submit(approval(Direction::Bullish)?)?;
        assert!(broker
            .advance(&candle(240, 103, 104, 101, 102)?)?
            .is_empty());
        assert!(broker
            .advance(&candle(300, 103, 104, 102, 103)?)?
            .is_empty());
        assert!(broker.pending_intent().is_some());
        let events = broker.advance(&candle(360, 103, 104, 101, 103)?)?;
        assert!(
            matches!(&events[..], [PaperEvent::EntryFilled { price, at: time, .. }] if *price == Decimal::new(102, 0) && *time == at(420)?)
        );
        Ok(())
    }

    #[test]
    fn entry_bar_target_is_disallowed_and_next_bar_target_has_no_improvement() -> TestResult {
        let mut broker = broker()?;
        broker.submit(approval(Direction::Bullish)?)?;
        let events = broker.advance(&candle(300, 103, 115, 101, 114)?)?;
        assert!(matches!(&events[..], [PaperEvent::EntryFilled { .. }]));
        assert!(broker.has_position());
        let after_entry = broker.snapshot();
        assert!(after_entry.balance < after_entry.initial_balance);
        assert!(after_entry.equity > after_entry.initial_balance);
        let exit = broker.advance(&candle(360, 116, 117, 113, 115)?)?;
        let [PaperEvent::PositionClosed {
            exit_price,
            net_pnl,
            reason: PaperExitReason::TakeProfit,
            ..
        }] = &exit[..]
        else {
            return Err(io::Error::other("expected target").into());
        };
        assert_eq!(*exit_price, Decimal::new(112, 0));
        assert_eq!(broker.snapshot().realized_pnl, *net_pnl);
        assert_eq!(broker.snapshot().wins, 1);
        assert_eq!(broker.snapshot().unrealized_pnl, Decimal::ZERO);
        assert!(broker.is_flat());
        Ok(())
    }

    #[test]
    fn existing_position_both_hit_bar_stops_before_taking_profit() -> TestResult {
        let mut broker = broker()?;
        broker.submit(approval(Direction::Bullish)?)?;
        broker.advance(&candle(300, 103, 104, 101, 103)?)?;
        let events = broker.advance(&candle(360, 103, 115, 97, 110)?)?;
        assert!(matches!(
            &events[..],
            [PaperEvent::PositionClosed {
                reason: PaperExitReason::StopLoss,
                ..
            }]
        ));
        Ok(())
    }

    #[test]
    fn long_gap_stop_uses_adverse_open_and_can_exceed_budget() -> TestResult {
        let mut broker = broker()?;
        let plan = approval(Direction::Bullish)?;
        broker.submit(plan.clone())?;
        broker.advance(&candle(300, 103, 104, 101, 103)?)?;
        let events = broker.advance(&candle(360, 90, 96, 88, 94)?)?;
        assert!(
            matches!(&events[..], [PaperEvent::PositionClosed { exit_price, net_pnl, .. }] if *exit_price == Decimal::new(899, 1) && sub(Decimal::ZERO, *net_pnl)? > plan.risk_budget())
        );
        Ok(())
    }

    #[test]
    fn short_gap_stop_rounds_up_and_never_clips_negative_equity() -> TestResult {
        let mut broker = broker()?;
        broker.submit(approval(Direction::Bearish)?)?;
        assert!(matches!(
            &broker.advance(&candle(300, 97, 99, 96, 97)?)?[..],
            [PaperEvent::EntryFilled { .. }]
        ));
        let events = broker.advance(&candle(360, 1000, 1001, 999, 1000)?)?;
        assert!(
            matches!(&events[..], [PaperEvent::PositionClosed { exit_price, reason: PaperExitReason::StopLoss, .. }] if *exit_price == Decimal::new(10_002, 1))
        );
        assert!(broker.snapshot().equity < Decimal::ZERO);
        assert!(broker.is_flat());
        Ok(())
    }

    #[test]
    fn short_target_pays_fees_and_mark_value_uses_direction() -> TestResult {
        let mut broker = broker()?;
        broker.submit(approval(Direction::Bearish)?)?;
        broker.advance(&candle(300, 97, 99, 94, 95)?)?;
        assert!(broker.snapshot().unrealized_pnl > Decimal::ZERO);
        let events = broker.advance(&candle(360, 95, 96, 87, 88)?)?;
        assert!(
            matches!(&events[..], [PaperEvent::PositionClosed { exit_price, reason: PaperExitReason::TakeProfit, .. }] if *exit_price == Decimal::new(88, 0))
        );
        assert_eq!(broker.snapshot().wins, 1);
        assert!(broker.snapshot().fees_paid > Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn replay_and_candle_duplicate_are_exactly_once_and_corruption_is_atomic() -> TestResult {
        let mut broker = broker()?;
        let plan = approval(Direction::Bullish)?;
        broker.submit(plan.clone())?;
        assert!(broker.submit(plan.clone())?.is_empty());
        let bar = candle(300, 103, 104, 101, 103)?;
        broker.advance(&bar)?;
        let snapshot = broker.snapshot();
        assert!(broker.advance(&bar)?.is_empty());
        assert_eq!(
            broker.advance(&candle(300, 103, 105, 101, 103)?),
            Err(PaperError::ConflictingDuplicate)
        );
        assert_eq!(
            broker.advance(&candle(420, 103, 104, 101, 103)?),
            Err(PaperError::Gap)
        );
        assert_eq!(
            broker.advance(&candle(240, 103, 104, 101, 103)?),
            Err(PaperError::OutOfOrder)
        );
        assert_eq!(broker.snapshot(), snapshot);
        broker.advance(&candle(360, 103, 115, 97, 110)?)?;
        let closed = broker.snapshot();
        assert!(broker.submit(plan)?.is_empty());
        assert_eq!(broker.snapshot(), closed);
        assert_eq!(closed.completed_trades, 1);
        Ok(())
    }

    #[test]
    fn expiry_precedes_fill_and_shutdown_never_flattens_a_position() -> TestResult {
        let mut expired = broker()?;
        expired.submit(approval(Direction::Bullish)?)?;
        let events = expired.advance(&candle(600, 103, 104, 101, 103)?)?;
        assert!(matches!(
            &events[..],
            [PaperEvent::EntryCancelled {
                reason: PaperCancelReason::Expired,
                ..
            }]
        ));
        assert!(expired.is_flat());
        assert_eq!(expired.snapshot().fees_paid, Decimal::ZERO);
        let mut open = broker()?;
        open.submit(approval(Direction::Bullish)?)?;
        open.advance(&candle(300, 103, 104, 101, 103)?)?;
        assert!(open.cancel_pending(at(360)?)?.is_empty());
        assert!(open.has_position());
        assert!(!open.is_flat());
        let mut pending = broker()?;
        pending.submit(approval(Direction::Bullish)?)?;
        assert!(matches!(
            &pending.cancel_pending(at(300)?)?[..],
            [PaperEvent::EntryCancelled {
                reason: PaperCancelReason::Requested,
                ..
            }]
        ));
        assert!(pending.is_flat());
        Ok(())
    }

    #[test]
    fn net_rr_and_minimum_notional_reject_without_state_changes() -> TestResult {
        let mut broker = broker()?;
        let weak = approval_with(
            Direction::Bullish,
            Decimal::new(100, 0),
            Decimal::new(1103, 1),
            &spec()?,
        )?;
        let before = broker.snapshot();
        assert_eq!(broker.submit(weak), Err(PaperError::NetRiskReward));
        assert_eq!(broker.snapshot(), before);
        let mut high_minimum =
            PaperBroker::new(PaperConfig::default(), spec()?, Decimal::new(30, 0))?;
        assert_eq!(
            high_minimum.submit(approval(Direction::Bullish)?),
            Err(PaperError::MinimumOrder)
        );
        assert!(high_minimum.is_flat());
        Ok(())
    }

    #[test]
    fn post_cost_quantity_must_still_meet_minimum_step_and_contract_identity() -> TestResult {
        let coarse = InstrumentRiskSpec::new(
            instrument(Exchange::Binance)?,
            Decimal::new(1, 1),
            Decimal::new(24, 2),
            Decimal::new(24, 2),
            None,
            Decimal::ONE,
        )?;
        let plan = approval_with(
            Direction::Bullish,
            Decimal::new(100, 0),
            Decimal::new(112, 0),
            &coarse,
        )?;
        let mut broker = PaperBroker::new(PaperConfig::default(), coarse, Decimal::ZERO)?;
        assert_eq!(broker.submit(plan), Err(PaperError::MinimumOrder));
        assert!(matches!(
            broker.submit(approval(Direction::Bullish)?),
            Err(PaperError::InvalidApproval(_))
        ));
        Ok(())
    }

    #[test]
    fn incomplete_and_wrong_timeframe_candles_are_rejected() -> TestResult {
        let mut broker = broker()?;
        broker.advance(&candle(240, 103, 104, 101, 103)?)?;
        let mut json = serde_json::to_value(candle(300, 103, 104, 101, 103)?)?;
        json["is_closed"] = serde_json::Value::Bool(false);
        let provisional: Candle = serde_json::from_value(json)?;
        assert!(matches!(
            broker.advance(&provisional),
            Err(PaperError::InvalidCandle(_))
        ));
        let wrong_width = Candle::new(CandleInput {
            instrument: instrument(Exchange::Binance)?,
            timeframe: Timeframe::M5,
            open_time: at(300)?,
            close_time: at(600)?,
            open: Decimal::new(103, 0),
            high: Decimal::new(104, 0),
            low: Decimal::new(101, 0),
            close: Decimal::new(103, 0),
            volume: Decimal::ONE,
            volume_unit: VolumeUnit::Contracts,
            trade_count: Some(1),
            is_closed: true,
        })?;
        assert!(matches!(
            broker.advance(&wrong_width),
            Err(PaperError::InvalidCandle(_))
        ));
        Ok(())
    }

    #[test]
    fn leverage_configuration_preserves_legacy_json_and_rejects_unsupported_values() -> TestResult {
        let original = PaperConfig::default();
        assert_eq!(original.leverage(), 1);
        assert_eq!(serde_json::to_value(original)?, serde_json::json!({
            "initial_balance": "100", "fee_rate": "0.0006", "stop_slippage_bps": "2",
            "max_notional_fraction": "1", "min_net_rr": "2"
        }));
        assert_eq!(serde_json::to_string(&original)?, serde_json::to_string(&original.with_leverage(1)?)?);
        assert_eq!(serde_json::to_value(original.with_leverage(10)?)?["leverage"], 10);
        for leverage in [0, 11, u32::MAX] {
            assert!(matches!(original.with_leverage(leverage), Err(PaperError::InvalidConfig(_))));
        }
        Ok(())
    }

    #[test]
    fn v3_allocated_margin_rounds_up_at_decimal_precision_without_changing_legacy_report() -> TestResult {
        let config = PaperConfig::new(Decimal::from(100), Decimal::ZERO, Decimal::ZERO,
            Decimal::ONE, Decimal::TWO)?.with_leverage(7)?;
        let broker = PaperBroker::new(config, spec()?, Decimal::ZERO)?;
        let plan = approval(Direction::Bullish)?;
        let quantity = Decimal::new(2, 28);
        let notional = broker.notional(plan.entry_price(), quantity)?;
        let legacy = broker.initial_margin(plan.entry_price(), quantity)?;
        assert!(mul(legacy, Decimal::from(7))? < notional);
        let funded = broker.allocation_capital(&plan, quantity, true)?;
        assert!(mul(funded, Decimal::from(7))? >= notional);
        assert!(funded > legacy);
        assert_eq!(broker.initial_margin(plan.entry_price(), quantity)?, legacy);
        Ok(())
    }

    #[test]
    fn leverage_changes_margin_affordability_but_never_the_one_percent_risk_budget() -> TestResult {
        let plan = high_price_approval()?;
        assert_eq!(plan.risk_budget(), Decimal::ONE);
        let mut unleveraged = broker()?;
        let mut leveraged = PaperBroker::new(PaperConfig::default().with_leverage(10)?, spec()?, Decimal::from(5))?;
        unleveraged.submit(plan.clone())?;
        leveraged.submit(plan.clone())?;
        let unleveraged_snapshot = unleveraged.snapshot();
        let leveraged_snapshot = leveraged.snapshot();
        let unleveraged_quantity = order(&unleveraged_snapshot)?.quantity;
        let sized = order(&leveraged_snapshot)?;
        assert!(sized.quantity > unleveraged_quantity);
        assert!(sized.quantity <= plan.quantity());
        assert!(leveraged.notional(sized.entry_price, sized.quantity)? > Decimal::from(100));
        let stop = leveraged.stop_price(plan.stop_loss_price(), plan.direction())?;
        assert!(leveraged.stop_risk(&plan, sized.quantity, stop)? <= Decimal::ONE);
        assert!(leveraged.entry_capital(&plan, sized.quantity, stop)? <= Decimal::from(100));
        assert_eq!(sized.quantity.checked_rem(spec()?.quantity_step()), Some(Decimal::ZERO));
        Ok(())
    }

    #[test]
    fn leveraged_cash_cap_reserves_entry_and_larger_protective_exit_fee() -> TestResult {
        // A higher-equity approval deliberately makes the broker's independent
        // 100-cash margin check bind first, rather than its external risk ceiling.
        let plan = approval_with(Direction::Bullish, Decimal::from(10_000), Decimal::from(112), &spec()?)?;
        let mut broker = PaperBroker::new(PaperConfig::default().with_leverage(10)?, spec()?, Decimal::ZERO)?;
        broker.submit(plan.clone())?;
        let snapshot = broker.snapshot();
        let sized = order(&snapshot)?;
        let stop = broker.stop_price(plan.stop_loss_price(), plan.direction())?;
        let margin = broker.initial_margin(sized.entry_price, sized.quantity)?;
        let entry_fee = broker.fee(sized.entry_price, sized.quantity)?;
        let exit_fee = broker.fee(plan.take_profit_price(), sized.quantity)?.max(broker.fee(stop, sized.quantity)?);
        assert!(add(margin, add(entry_fee, exit_fee)?)? <= Decimal::from(100));
        let next = add(sized.quantity, spec()?.quantity_step())?;
        assert!(!broker.quantity_fits(&plan, next, stop, Decimal::from(100))?);
        assert!(broker.notional(sized.entry_price, sized.quantity)? < Decimal::from(1000));
        Ok(())
    }

    #[test]
    fn ten_times_margin_without_costs_still_does_not_multiply_approved_quantity() -> TestResult {
        let config = PaperConfig::new(Decimal::from(100), Decimal::ZERO, Decimal::ZERO, Decimal::ONE, Decimal::TWO)?;
        let plan = high_price_approval()?;
        let mut broker = PaperBroker::new(config.with_leverage(10)?, spec()?, Decimal::ZERO)?;
        broker.submit(plan.clone())?;
        let snapshot = broker.snapshot();
        let sized = order(&snapshot)?;
        assert_eq!(sized.quantity, Decimal::new(98, 3));
        assert!(sized.quantity < plan.quantity());
        assert!(broker.initial_margin(sized.entry_price, sized.quantity)? <= Decimal::from(100));
        assert!(broker.stop_risk(&plan, sized.quantity, plan.stop_loss_price())? <= Decimal::ONE);
        Ok(())
    }

    #[test]
    fn leveraged_profit_is_price_difference_times_quantity_without_extra_multiplier() -> TestResult {
        let plan = high_price_approval()?;
        let entry = plan.entry_price();
        let target = plan.take_profit_price();
        let mut broker = PaperBroker::new(PaperConfig::default().with_leverage(10)?, spec()?, Decimal::from(5))?;
        broker.submit(plan)?;
        let snapshot = broker.snapshot();
        let quantity = order(&snapshot)?.quantity;
        let expected_gross = mul(sub(target, entry)?, quantity)?;
        let expected_fees = add(broker.fee(entry, quantity)?, broker.fee(target, quantity)?)?;
        let fill = broker.advance(&candle(300, 10_103, 10_104, 10_101, 10_102)?)?;
        assert!(matches!(fill.as_slice(), [PaperEvent::EntryFilled { .. }]));
        let close = broker.advance(&candle(360, 10_103, 10_501, 10_102, 10_500)?)?;
        let [PaperEvent::PositionClosed { gross_pnl, net_pnl, .. }] = close.as_slice() else {
            return Err(io::Error::other("expected leveraged target exit").into());
        };
        assert_eq!(*gross_pnl, expected_gross);
        assert_eq!(*net_pnl, sub(expected_gross, expected_fees)?);
        assert_eq!(broker.snapshot().balance, add(Decimal::from(100), *net_pnl)?);
        assert_eq!(broker.snapshot().fees_paid, expected_fees);
        Ok(())
    }

    #[test]
    fn leverage_does_not_bypass_minimum_orders_or_insufficient_cash() -> TestResult {
        let config = PaperConfig::default().with_leverage(10)?;
        let mut minimum = PaperBroker::new(config, spec()?, Decimal::from(1001))?;
        assert_eq!(minimum.submit(high_price_approval()?), Err(PaperError::MinimumOrder));
        assert!(minimum.is_flat());
        let mut empty = PaperBroker::new(config, spec()?, Decimal::ZERO)?;
        empty.metrics.balance = Decimal::ZERO;
        assert_eq!(empty.submit(high_price_approval()?), Err(PaperError::InsufficientBalance));
        assert!(empty.is_flat());
        assert!(matches!(empty.initial_margin(Decimal::ZERO, Decimal::ONE), Err(PaperError::InvalidApproval(_))));
        assert!(matches!(empty.initial_margin(Decimal::ONE, -Decimal::ONE), Err(PaperError::InvalidApproval(_))));
        assert_eq!(empty.initial_margin(Decimal::ONE, Decimal::ZERO)?, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn explicit_one_times_simulation_retains_all_legacy_events_and_snapshot_bytes() -> TestResult {
        let mut original = broker()?;
        let mut explicit = PaperBroker::new(PaperConfig::default().with_leverage(1)?, spec()?, Decimal::from(5))?;
        let plan = approval(Direction::Bullish)?;
        assert_eq!(original.submit(plan.clone())?, explicit.submit(plan)?);
        for bar in [candle(300, 103, 104, 101, 103)?, candle(360, 103, 113, 102, 112)?] {
            assert_eq!(original.advance(&bar)?, explicit.advance(&bar)?);
            assert_eq!(serde_json::to_vec(&original.snapshot())?, serde_json::to_vec(&explicit.snapshot())?);
        }
        Ok(())
    }

    #[test]
    fn switching_at_same_close_preserves_cash_metrics_and_completed_tombstones() -> TestResult {
        let mut broker = broker()?;
        let plan = approval(Direction::Bullish)?;
        broker.submit(plan.clone())?;
        broker.advance(&candle(300, 103, 104, 101, 103)?)?;
        broker.advance(&candle(360, 103, 113, 102, 112)?)?;
        let before = broker.snapshot();
        assert_eq!(before.completed_trades, 1);
        assert!(before.fees_paid > Decimal::ZERO);
        broker.select_instrument(ethereum_spec()?, Decimal::from(5), &ethereum_candle(360)?)?;
        assert_eq!(broker.snapshot(), before);
        assert_eq!(broker.spec(), &ethereum_spec()?);
        assert_eq!(broker.accepted.len(), 1);
        // Completed BTC approvals stay terminal even while ETH is selected.
        assert!(broker.submit(plan)?.is_empty());
        assert!(broker.is_flat());
        assert_eq!(broker.snapshot(), before);
        Ok(())
    }

    #[test]
    fn switching_refuses_pending_and_open_exposure_without_mutating_account() -> TestResult {
        let mut broker = broker()?;
        broker.submit(approval(Direction::Bullish)?)?;
        let pending = broker.snapshot();
        assert_eq!(broker.select_instrument(ethereum_spec()?, Decimal::from(5), &ethereum_candle(240)?), Err(PaperError::Busy));
        assert_eq!(broker.snapshot(), pending);
        broker.advance(&candle(300, 103, 104, 101, 103)?)?;
        let open = broker.snapshot();
        assert_eq!(broker.select_instrument(ethereum_spec()?, Decimal::from(5), &ethereum_candle(300)?), Err(PaperError::Busy));
        assert_eq!(broker.snapshot(), open);
        assert_eq!(broker.spec(), &spec()?);
        Ok(())
    }

    #[test]
    fn selected_cursor_prevents_fill_on_the_already_observed_signal_bar() -> TestResult {
        let mut broker = broker()?;
        let signal = candle(240, 103, 104, 101, 102)?;
        broker.select_instrument(spec()?, Decimal::from(5), &signal)?;
        broker.submit(approval(Direction::Bullish)?)?;
        assert!(broker.advance(&signal)?.is_empty());
        assert!(broker.pending_intent().is_some());
        assert!(matches!(broker.advance(&candle(300, 103, 104, 101, 103)?)?.as_slice(), [PaperEvent::EntryFilled { .. }]));
        Ok(())
    }

    #[test]
    fn switching_rejects_invalid_metadata_identity_and_regression_atomically() -> TestResult {
        let mut broker = broker()?;
        let original = candle(300, 103, 104, 101, 103)?;
        broker.advance(&original)?;
        let before = broker.snapshot();
        assert!(matches!(broker.select_instrument(ethereum_spec()?, -Decimal::ONE, &ethereum_candle(300)?), Err(PaperError::InvalidConfig(_))));
        assert!(matches!(broker.select_instrument(ethereum_spec()?, Decimal::ZERO, &original), Err(PaperError::InvalidCandle(_))));
        assert_eq!(broker.select_instrument(ethereum_spec()?, Decimal::ZERO, &ethereum_candle(240)?), Err(PaperError::OutOfOrder));
        assert_eq!(broker.select_instrument(spec()?, Decimal::ZERO, &candle(300, 103, 105, 101, 103)?), Err(PaperError::ConflictingDuplicate));
        let mut provisional = serde_json::to_value(ethereum_candle(300)?)?;
        provisional["is_closed"] = false.into();
        assert!(matches!(broker.select_instrument(ethereum_spec()?, Decimal::ZERO, &serde_json::from_value(provisional)?), Err(PaperError::InvalidCandle(_))));
        assert!(matches!(broker.select_instrument(spec()?, Decimal::ZERO, &candle(301, 103, 104, 101, 103)?), Err(PaperError::InvalidCandle(_))));
        assert_eq!(broker.snapshot(), before);
        assert_eq!(broker.spec(), &spec()?);
        assert_eq!(broker.last_candle.as_ref(), Some(&original));
        Ok(())
    }
}
