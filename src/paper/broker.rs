//! Deterministic, conservative OHLC paper execution without exchange I/O.
//!
//! This is a simulator, not a venue fill model: funding, liquidation, queue
//! priority, order books, and partial fills are not modeled. Fees are explicit
//! assumptions. An adverse gap can exceed the planned risk or exhaust equity;
//! losses are never clipped. End-of-data never liquidates an open position.

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
        })
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

    /// Maximum entry notional plus entry fee divided by available cash.
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
        }
    }
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
    /// There is no positive unleveraged cash budget.
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

/// Single-instrument, unleveraged, deterministic simulation state.
///
/// Mutating methods stage all changes, so any error preserves the old state.
/// An exact latest-candle duplicate and an exact accepted-approval replay are
/// idempotent. Older candles, gaps, and conflicting duplicates fail closed.
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
    /// Quantity is reduced for assumed fees/slippage and a one-times cash cap.
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
        let unit_cash = add(
            mul(plan.entry_price(), self.spec.quote_value_per_price_unit())?,
            entry_fee,
        )?;
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
        let actual_cash = add(
            self.notional(plan.entry_price(), quantity)?,
            self.fee(plan.entry_price(), quantity)?,
        )?;
        Ok(quantity <= plan.quantity()
            && actual_cash <= cash
            && self.stop_risk(plan, quantity, stop)? <= plan.risk_budget())
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
mod tests {
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
        let bullish = direction == Direction::Bullish;
        let series = instrument(Exchange::Binance)?;
        let price = |value: i64| Decimal::new(if bullish { value } else { 200 - value }, 0);
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
}
