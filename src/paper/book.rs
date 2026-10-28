//! Independently owned PAPER orders funded from one conserved cash account.
//!
//! Each one-shot execution sleeve receives only its margin and fee allocation.
//! Cash is never multiplied by the number of orders. Admissions enforce four
//! active orders, two per symbol, same-side ownership and prospective 1%/3%
//! risk limits. Market losses may exceed those plans; there is no liquidation
//! model, loss clipping, cross-order netting or forced close on shutdown.

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use uuid::Uuid;

use crate::{Candle, Exchange, InstrumentId, MarketKind, RiskPlan, Timeframe, VolumeUnit};

use super::{PaperBroker, PaperConfig, PaperError, PaperInstrument, PaperOrderSnapshot, PortfolioEvent};

const MAX_ORDERS: usize = 4;
const MAX_SYMBOL_ORDERS: usize = 2;
const MAX_ACCEPTED: usize = 100_000;

/// Independently owned entry or filled position in the shared account.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BookOrderState {
    /// A resting, unfilled limit entry.
    Pending,
    /// A filled position with its own protective prices.
    Open,
}

/// One independent order's geometry, owner and conservative reservations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BookOrderSnapshot {
    /// Exact venue symbol.
    pub symbol: String,
    /// Whether the entry is resting or filled.
    pub state: BookOrderState,
    /// Actual paper quantity and unchanged approved prices.
    pub order: PaperOrderSnapshot,
    /// Original risk-manager planned loss, retained until full release.
    #[serde(with = "crate::decimal_codec::required")]
    pub reserved_risk: Decimal,
    /// Initial margin and remaining estimated fees, not a liquidation guarantee.
    #[serde(with = "crate::decimal_codec::required")]
    pub reserved_capital: Decimal,
}

/// Cached scalar account totals; sleeve seeds are allocations, never extra cash.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BookSnapshot {
    /// The account's one original starting balance.
    #[serde(with = "crate::decimal_codec::required")]
    pub initial_balance: Decimal,
    /// Settled shared cash, including entry fees already paid.
    #[serde(with = "crate::decimal_codec::required")]
    pub balance: Decimal,
    /// Cash plus all open marks, net of estimated mark-price exit fees.
    #[serde(with = "crate::decimal_codec::required")]
    pub equity: Decimal,
    /// Balance minus the single initial account balance.
    #[serde(with = "crate::decimal_codec::required")]
    pub realized_pnl: Decimal,
    /// Sum of all marked open profits/losses, including estimated exit fees.
    #[serde(with = "crate::decimal_codec::required")]
    pub unrealized_pnl: Decimal,
    /// Actual paid fees across completed and active orders.
    #[serde(with = "crate::decimal_codec::required")]
    pub fees_paid: Decimal,
    /// Total fully closed trades.
    pub completed_trades: u64,
    /// Closed trades with strictly positive whole-trade net profit.
    pub wins: u64,
    /// Closed trades with strictly negative whole-trade net profit.
    pub losses: u64,
    /// Initial-margin affordability only; never a profit multiplier.
    pub leverage: u32,
    /// Active orders sorted by symbol then intent identity.
    pub orders: Vec<BookOrderSnapshot>,
    /// Sum of original planned-loss reservations, not netted across orders.
    #[serde(with = "crate::decimal_codec::required")]
    pub reserved_risk: Decimal,
    /// Remaining allocated initial margin and conservative estimated fees.
    #[serde(with = "crate::decimal_codec::required")]
    pub reserved_capital: Decimal,
    /// Unallocated cash capped by current marked equity and account capital cap.
    /// Positive unrealized profit is not made available for another order.
    #[serde(with = "crate::decimal_codec::required")]
    pub available_cash: Decimal,
}

/// Book validation errors and ordinary admission rejections.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BookError {
    /// The global pending-plus-open count has reached four.
    #[error("paper account active-order limit reached")]
    ActiveLimit,
    /// This symbol already has two pending/open orders.
    #[error("paper symbol active-order limit reached")]
    SymbolLimit,
    /// An active order on the same symbol faces the opposite direction.
    #[error("opposing active paper directions are not supported")]
    OpposingDirection,
    /// Prospective shared 1% per-trade or 3% aggregate planned risk would be exceeded.
    #[error("paper shared account risk limit reached")]
    RiskLimit,
    /// No positive, unallocated and equity-supported capital remains.
    #[error("paper shared account has no available cash")]
    InsufficientCash,
    /// Chronology, identity or ownership was inconsistent.
    #[error("invalid paper book input: {0}")]
    InvalidInput(&'static str),
    /// An existing intent identity was reused for different approval/input data.
    #[error("conflicting paper book approval replay")]
    ConflictingReplay,
    /// Checked account arithmetic failed.
    #[error("paper book arithmetic failed")]
    Arithmetic,
    /// The existing conservative execution model rejected or failed an operation.
    #[error(transparent)]
    Broker(#[from] PaperError),
}

impl BookError {
    /// Whether an admission can be rejected without poisoning its owning engine.
    #[must_use]
    pub fn is_rejection(&self) -> bool {
        match self {
            Self::ActiveLimit | Self::SymbolLimit | Self::OpposingDirection
                | Self::RiskLimit | Self::InsufficientCash => true,
            Self::Broker(error) => error.is_rejection(),
            Self::InvalidInput(_) | Self::ConflictingReplay | Self::Arithmetic => false,
        }
    }
}

#[derive(Clone, Debug)]
struct Sleeve {
    broker: PaperBroker,
    reserved_risk: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcceptedOrder {
    plan: RiskPlan,
    instrument: PaperInstrument,
    signal: Candle,
}

/// One shared-cash multi-order PAPER book, independent from v1/v2 sessions.
///
/// Every fallible mutation stages a full at-most-four-order transition. Errors
/// preserve the entire prior book. Original approval tombstones are immutable
/// and bounded. A caller must process complete synchronized M1 batches before
/// admitting any idea from that batch. Shutdown cancels entries only.
#[derive(Clone, Debug)]
pub struct PaperBook {
    config: PaperConfig,
    free_cash: Decimal,
    sleeves: BTreeMap<Uuid, Sleeve>,
    accepted: Arc<BTreeMap<Uuid, AcceptedOrder>>,
    last_batch: Option<Vec<Candle>>,
    latest_activity: Option<DateTime<Utc>>,
    settled_fees: Decimal,
    settled_trades: u64,
    settled_wins: u64,
    settled_losses: u64,
    metrics: BookSnapshot,
}

impl PaperBook {
    /// Creates the one cash account without credentials or exchange transports.
    ///
    /// # Errors
    /// Checked capital-cap arithmetic can fail for unrepresentable inputs.
    pub fn new(config: PaperConfig) -> Result<Self, BookError> {
        let initial = config.initial_balance();
        let mut book = Self {
            config, free_cash: initial, sleeves: BTreeMap::new(),
            accepted: Arc::new(BTreeMap::new()), last_batch: None, latest_activity: None,
            settled_fees: Decimal::ZERO, settled_trades: 0, settled_wins: 0, settled_losses: 0,
            metrics: BookSnapshot {
                initial_balance: initial, balance: initial, equity: initial,
                realized_pnl: Decimal::ZERO, unrealized_pnl: Decimal::ZERO,
                fees_paid: Decimal::ZERO, completed_trades: 0, wins: 0, losses: 0,
                leverage: config.leverage(), orders: Vec::new(),
                reserved_risk: Decimal::ZERO, reserved_capital: Decimal::ZERO,
                available_cash: Decimal::ZERO,
            },
        };
        book.refresh()?;
        Ok(book)
    }

    /// Account-wide simulation assumptions; sleeve allocations are not accounts.
    #[must_use]
    pub const fn config(&self) -> &PaperConfig { &self.config }

    /// Clones the cached, checked account snapshot with lossless decimal strings.
    #[must_use]
    pub fn snapshot(&self) -> BookSnapshot { self.metrics.clone() }

    /// Funds one independent resting entry, never a signal-candle fill.
    ///
    /// # Errors
    /// Rejects count, direction, risk, margin and execution-economic limits.
    /// Wrong identities or a signal other than the latest accepted batch fail
    /// closed. Exact accepted approvals replay as a no-op, including after exit.
    pub fn admit(&mut self, instrument: &PaperInstrument, plan: RiskPlan, signal_candle: &Candle)
        -> Result<Vec<PortfolioEvent>, BookError>
    {
        let identity = AcceptedOrder { plan, instrument: instrument.clone(), signal: signal_candle.clone() };
        if let Some(previous) = self.accepted.get(&identity.plan.intent_id()) {
            return if previous == &identity { Ok(Vec::new()) } else { Err(BookError::ConflictingReplay) };
        }
        self.validate_admission(&identity)?;
        let (broker, allocation) = PaperBroker::allocate_order(self.config,
            instrument.spec.clone(), instrument.min_notional, identity.plan.clone(), signal_candle,
            self.metrics.available_cash)?;
        let order = broker.snapshot().pending.ok_or(BookError::InvalidInput("allocated entry missing"))?;
        let symbol = instrument.spec.instrument().venue_symbol.as_str().to_owned();
        let mut staged = self.clone();
        staged.free_cash = sub(staged.free_cash, allocation)?;
        staged.sleeves.insert(identity.plan.intent_id(), Sleeve {
            broker, reserved_risk: identity.plan.planned_loss(),
        });
        Arc::make_mut(&mut staged.accepted).insert(identity.plan.intent_id(), identity);
        staged.latest_activity = Some(signal_candle.close_time());
        staged.refresh()?;
        if staged.metrics.balance != self.metrics.balance || staged.metrics.equity != self.metrics.equity {
            return Err(BookError::Arithmetic);
        }
        *self = staged;
        Ok(vec![PortfolioEvent::EntryPlaced { symbol, at: signal_candle.close_time(), order }])
    }

    fn validate_admission(&self, identity: &AcceptedOrder) -> Result<(), BookError> {
        let plan = &identity.plan;
        let instrument = &identity.instrument;
        validate_identity(instrument.spec.instrument())?;
        if instrument.spec.quote_value_per_price_unit() != Decimal::ONE || instrument.min_notional < Decimal::ZERO
            || instrument.spec.instrument() != plan.instrument() || plan.approved_spec() != &instrument.spec
            || plan.approved_at() != identity.signal.close_time()
            || !self.last_batch.as_ref().is_some_and(|batch| batch.contains(&identity.signal))
            || self.latest_activity.is_some_and(|at| identity.signal.close_time() < at)
        {
            return Err(BookError::InvalidInput("approval, metadata or current signal mismatch"));
        }
        if self.accepted.len() >= MAX_ACCEPTED { return Err(PaperError::Capacity.into()); }
        if self.sleeves.len() >= MAX_ORDERS { return Err(BookError::ActiveLimit); }
        let same_symbol: Vec<_> = self.metrics.orders.iter()
            .filter(|owned| owned.symbol == plan.instrument().venue_symbol.as_str()).collect();
        if same_symbol.iter().any(|owned| owned.order.direction != plan.direction()) {
            return Err(BookError::OpposingDirection);
        }
        if same_symbol.len() >= MAX_SYMBOL_ORDERS { return Err(BookError::SymbolLimit); }
        if self.sleeves.values().any(|sleeve| sleeve.broker.spec().instrument() == plan.instrument()
            && sleeve.broker.spec() != &instrument.spec) {
            return Err(BookError::InvalidInput("active instrument metadata changed"));
        }
        if self.metrics.equity <= Decimal::ZERO || plan.planned_loss() <= Decimal::ZERO
            || plan.planned_loss() > mul(self.metrics.equity, Decimal::new(1, 2))?
            || add(self.metrics.reserved_risk, plan.planned_loss())? > mul(self.metrics.equity, Decimal::new(3, 2))?
        {
            return Err(BookError::RiskLimit);
        }
        if self.metrics.available_cash <= Decimal::ZERO { return Err(BookError::InsufficientCash); }
        Ok(())
    }

    /// Advances all existing owners atomically before any new batch admission.
    ///
    /// # Errors
    /// Rejects missing/changed owners, gaps, revisions, wrong symbols or nonfinal
    /// non-M1 input. An exact latest complete batch is an idempotent no-op.
    pub fn advance_batch(&mut self, candles: &[Candle]) -> Result<Vec<PortfolioEvent>, BookError> {
        let batch = self.validate_batch(candles)?;
        if self.last_batch.as_ref() == Some(&batch) { return Ok(Vec::new()); }
        let close = batch.first().ok_or(BookError::InvalidInput("empty batch"))?.close_time();
        if self.latest_activity.is_some_and(|at| close < at) {
            return Err(BookError::InvalidInput("batch predates account activity"));
        }
        let mut staged = self.clone();
        let mut events = Vec::new();
        for owned in &self.metrics.orders {
            let candle = batch.iter().find(|bar| bar.instrument().venue_symbol.as_str() == owned.symbol)
                .ok_or(BookError::InvalidInput("active owner absent from batch"))?;
            let sleeve = staged.sleeves.get_mut(&owned.order.intent_id)
                .ok_or(BookError::InvalidInput("active sleeve missing"))?;
            events.extend(sleeve.broker.advance(candle)?.into_iter().map(|event| PortfolioEvent::Execution {
                symbol: owned.symbol.clone(), event, order: owned.order.clone(),
            }));
        }
        staged.retire_terminal()?;
        staged.last_batch = Some(batch);
        staged.latest_activity = Some(close);
        staged.refresh()?;
        *self = staged;
        Ok(events)
    }

    fn validate_batch(&self, candles: &[Candle]) -> Result<Vec<Candle>, BookError> {
        if candles.is_empty() || candles.len() > 2 { return Err(BookError::InvalidInput("expected one or two symbol candles")); }
        let mut batch = candles.to_vec();
        batch.sort_unstable_by(|left, right| left.instrument().venue_symbol.cmp(&right.instrument().venue_symbol));
        let first = batch.first().ok_or(BookError::InvalidInput("empty batch"))?;
        for (index, candle) in batch.iter().enumerate() {
            validate_identity(candle.instrument())?;
            if candle.timeframe() != Timeframe::M1 || !candle.is_closed() || candle.volume_unit() != VolumeUnit::BaseAsset
                || candle.close_time().signed_duration_since(candle.open_time()) != Timeframe::M1.duration()
                || candle.open_time().timestamp_subsec_nanos() != 0 || candle.open_time().timestamp().rem_euclid(60) != 0
                || candle.open_time() != first.open_time() || candle.close_time() != first.close_time()
                || batch[..index].iter().any(|prior| prior.instrument() == candle.instrument()) {
                return Err(BookError::InvalidInput("expected unique synchronized final M1 base-volume candles"));
            }
        }
        if let Some(previous) = &self.last_batch {
            if batch.len() != previous.len() || batch.iter().zip(previous).any(|(left, right)| left.instrument() != right.instrument()) {
                return Err(BookError::InvalidInput("batch instrument coverage changed"));
            }
            let prior = previous.first().ok_or(BookError::InvalidInput("empty previous batch"))?;
            if first.open_time() == prior.open_time() && batch != *previous { return Err(PaperError::ConflictingDuplicate.into()); }
            if batch != *previous {
                if first.open_time() < prior.close_time() { return Err(PaperError::OutOfOrder.into()); }
                if first.open_time() > prior.close_time() { return Err(PaperError::Gap.into()); }
            }
        }
        Ok(batch)
    }

    /// Cancels exactly one owned pending entry; an open position is never closed.
    ///
    /// # Errors
    /// Rejects an unknown/open identity or a time before accepted market data.
    pub fn cancel_intent(&mut self, intent_id: Uuid, at: DateTime<Utc>) -> Result<Vec<PortfolioEvent>, BookError> {
        self.validate_cancel_time(at)?;
        let order = self.metrics.orders.iter().find(|order| order.order.intent_id == intent_id
            && order.state == BookOrderState::Pending).cloned()
            .ok_or(BookError::InvalidInput("cancellation does not own a pending entry"))?;
        let mut staged = self.clone();
        let events = staged.cancel_owned(&order, at)?;
        staged.retire_terminal()?;
        staged.latest_activity = Some(at);
        staged.refresh()?;
        *self = staged;
        Ok(events)
    }

    /// Cancels all resting entries atomically, retaining every open position.
    ///
    /// # Errors
    /// Rejects regressing time, ownership mismatch or checked accounting failure.
    pub fn cancel_pending(&mut self, at: DateTime<Utc>) -> Result<Vec<PortfolioEvent>, BookError> {
        self.validate_cancel_time(at)?;
        let mut staged = self.clone();
        let mut events = Vec::new();
        for owned in self.metrics.orders.iter().filter(|order| order.state == BookOrderState::Pending) {
            events.extend(staged.cancel_owned(owned, at)?);
        }
        staged.retire_terminal()?;
        staged.latest_activity = Some(at);
        staged.refresh()?;
        *self = staged;
        Ok(events)
    }

    fn validate_cancel_time(&self, at: DateTime<Utc>) -> Result<(), BookError> {
        if self.latest_activity.is_some_and(|latest| at < latest)
            || self.last_batch.as_ref().and_then(|batch| batch.first()).is_some_and(|bar| at < bar.close_time()) {
            return Err(BookError::InvalidInput("cancellation predates market data"));
        }
        Ok(())
    }

    fn cancel_owned(&mut self, owned: &BookOrderSnapshot, at: DateTime<Utc>) -> Result<Vec<PortfolioEvent>, BookError> {
        let sleeve = self.sleeves.get_mut(&owned.order.intent_id).ok_or(BookError::InvalidInput("pending sleeve missing"))?;
        let events = sleeve.broker.cancel_pending(at)?;
        if events.len() != 1 { return Err(BookError::InvalidInput("pending cancellation emitted no event")); }
        Ok(events.into_iter().map(|event| PortfolioEvent::Execution {
            symbol: owned.symbol.clone(), event, order: owned.order.clone(),
        }).collect())
    }

    fn retire_terminal(&mut self) -> Result<(), BookError> {
        let terminal: Vec<_> = self.sleeves.iter().filter(|(_, sleeve)| sleeve.broker.is_flat()).map(|(id, _)| *id).collect();
        for id in terminal {
            let sleeve = self.sleeves.remove(&id).ok_or(BookError::InvalidInput("terminal sleeve missing"))?;
            let metrics = sleeve.broker.snapshot();
            self.free_cash = add(self.free_cash, metrics.balance)?;
            self.settled_fees = add(self.settled_fees, metrics.fees_paid)?;
            self.settled_trades = count_add(self.settled_trades, metrics.completed_trades)?;
            self.settled_wins = count_add(self.settled_wins, metrics.wins)?;
            self.settled_losses = count_add(self.settled_losses, metrics.losses)?;
        }
        Ok(())
    }

    fn refresh(&mut self) -> Result<(), BookError> {
        let mut metrics = BookSnapshot {
            initial_balance: self.config.initial_balance(), balance: self.free_cash, equity: Decimal::ZERO,
            realized_pnl: Decimal::ZERO, unrealized_pnl: Decimal::ZERO, fees_paid: self.settled_fees,
            completed_trades: self.settled_trades, wins: self.settled_wins, losses: self.settled_losses,
            leverage: self.config.leverage(), orders: Vec::with_capacity(self.sleeves.len()),
            reserved_risk: Decimal::ZERO, reserved_capital: Decimal::ZERO, available_cash: Decimal::ZERO,
        };
        for sleeve in self.sleeves.values() {
            let local = sleeve.broker.snapshot();
            metrics.balance = add(metrics.balance, local.balance)?;
            metrics.unrealized_pnl = add(metrics.unrealized_pnl, local.unrealized_pnl)?;
            metrics.fees_paid = add(metrics.fees_paid, local.fees_paid)?;
            metrics.completed_trades = count_add(metrics.completed_trades, local.completed_trades)?;
            metrics.wins = count_add(metrics.wins, local.wins)?;
            metrics.losses = count_add(metrics.losses, local.losses)?;
            let (state, order) = match (local.pending, local.position) {
                (Some(order), None) => (BookOrderState::Pending, order),
                (None, Some(order)) => (BookOrderState::Open, order),
                _ => return Err(BookError::InvalidInput("sleeve does not own exactly one exposure")),
            };
            let reserved_capital = sleeve.broker.reserved_capital()?;
            metrics.reserved_risk = add(metrics.reserved_risk, sleeve.reserved_risk)?;
            metrics.reserved_capital = add(metrics.reserved_capital, reserved_capital)?;
            metrics.orders.push(BookOrderSnapshot {
                symbol: sleeve.broker.spec().instrument().venue_symbol.as_str().to_owned(),
                state, order, reserved_risk: sleeve.reserved_risk, reserved_capital,
            });
        }
        metrics.orders.sort_unstable_by(|left, right| left.symbol.cmp(&right.symbol).then(left.order.intent_id.cmp(&right.order.intent_id)));
        metrics.realized_pnl = sub(metrics.balance, metrics.initial_balance)?;
        metrics.equity = add(metrics.balance, metrics.unrealized_pnl)?;
        let cap = mul(metrics.balance.min(metrics.equity).max(Decimal::ZERO), self.config.max_notional_fraction())?;
        metrics.available_cash = self.free_cash.max(Decimal::ZERO).min(sub(cap, metrics.reserved_capital)?.max(Decimal::ZERO));
        self.metrics = metrics;
        Ok(())
    }
}

fn validate_identity(instrument: &InstrumentId) -> Result<(), BookError> {
    let canonical = match instrument.venue_symbol.as_str() {
        "BTCUSDT" => "BTC-USDT-PERP", "ETHUSDT" => "ETH-USDT-PERP",
        _ => return Err(BookError::InvalidInput("only BTCUSDT and ETHUSDT are supported")),
    };
    if instrument.exchange != Exchange::Bybit || instrument.market != MarketKind::LinearPerpetual
        || instrument.canonical_symbol.as_str() != canonical {
        return Err(BookError::InvalidInput("expected canonical Bybit linear USDT identity"));
    }
    Ok(())
}

fn add(left: Decimal, right: Decimal) -> Result<Decimal, BookError> {
    left.checked_add(right).ok_or(BookError::Arithmetic)
}
fn sub(left: Decimal, right: Decimal) -> Result<Decimal, BookError> {
    left.checked_sub(right).ok_or(BookError::Arithmetic)
}
fn mul(left: Decimal, right: Decimal) -> Result<Decimal, BookError> {
    left.checked_mul(right).ok_or(BookError::Arithmetic)
}
fn count_add(left: u64, right: u64) -> Result<u64, BookError> {
    left.checked_add(right).ok_or(BookError::Arithmetic)
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use super::*;
    use crate::{CandleInput, Direction, InstrumentRiskSpec, LiquiditySide, LiquidityTarget,
        RiskConfig, RiskManager, TradeAssessment, TradeIntent};
    use crate::execution::coordinator::tests::at;
    use crate::paper::{PaperEvent, PaperExitReason};

    type TestResult = Result<(), Box<dyn Error>>;

    fn instrument(asset: &str) -> Result<PaperInstrument, Box<dyn Error>> {
        Ok(PaperInstrument {
            spec: InstrumentRiskSpec::new(InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual,
                format!("{asset}USDT"), format!("{asset}-USDT-PERP"))?, Decimal::new(1, 1),
                Decimal::new(1, 3), Decimal::new(1, 3), None, Decimal::ONE)?,
            min_notional: Decimal::from(5),
        })
    }

    fn replace_instruments(value: &mut serde_json::Value, identity: &serde_json::Value) {
        match value {
            serde_json::Value::Object(fields) => {
                for (key, field) in fields {
                    if key == "instrument" { field.clone_from(identity); }
                    else { replace_instruments(field, identity); }
                }
            }
            serde_json::Value::Array(items) => {
                for item in items { replace_instruments(item, identity); }
            }
            _ => {}
        }
    }

    fn approval(asset: &str, id: u128, direction: Direction, fraction: Decimal)
        -> Result<(PaperInstrument, RiskPlan, Candle), Box<dyn Error>>
    {
        let instrument = instrument(asset)?;
        let source = crate::paper::broker::tests::intent_with_offset(direction, 0)?;
        let mut value = serde_json::to_value(source)?;
        replace_instruments(&mut value, &serde_json::to_value(instrument.spec.instrument())?);
        value["id"] = serde_json::to_value(Uuid::from_u128(id))?;
        value["trigger_candle"]["volume_unit"] = serde_json::to_value(VolumeUnit::BaseAsset)?;
        let intent: TradeIntent = serde_json::from_value(value)?;
        let config = RiskConfig::new_with_safety_limits(fraction, Decimal::new(3, 2), Decimal::new(5, 2),
            Decimal::TWO, 1, chrono::Duration::seconds(5))?;
        let mut risk = RiskManager::new(config);
        risk.register_instrument_spec(instrument.spec.clone())?;
        risk.start_trading_day(Decimal::from(100), Decimal::from(100), at(299)?, false)?;
        let bullish = direction == Direction::Bullish;
        let target = LiquidityTarget::new(Uuid::from_u128(id + 1000), instrument.spec.instrument().clone(),
            if bullish { LiquiditySide::BuySide } else { LiquiditySide::SellSide },
            Decimal::from(if bullish { 112 } else { 88 }), at(240)?)?;
        let TradeAssessment::Approved(plan) = risk.assess_trade(&intent, &target, at(300)?)? else {
            return Err(io::Error::other("fixture approval unexpectedly replayed").into());
        };
        Ok((instrument, *plan, intent.trigger_candle().clone()))
    }

    fn candle(asset: &str, start: i64, prices: [i64; 4]) -> Result<Candle, Box<dyn Error>> {
        Ok(Candle::new(CandleInput {
            instrument: instrument(asset)?.spec.instrument().clone(), timeframe: Timeframe::M1,
            open_time: at(start)?, close_time: at(start + 60)?, open: Decimal::from(prices[0]),
            high: Decimal::from(prices[1]), low: Decimal::from(prices[2]), close: Decimal::from(prices[3]),
            volume: Decimal::ONE, volume_unit: VolumeUnit::BaseAsset, trade_count: Some(1), is_closed: true,
        })?)
    }

    fn ready() -> Result<PaperBook, Box<dyn Error>> {
        let mut book = PaperBook::new(PaperConfig::default().with_leverage(10)?)?;
        let (_, _, btc) = approval("BTC", 1, Direction::Bullish, Decimal::new(5, 3))?;
        let (_, _, eth) = approval("ETH", 2, Direction::Bullish, Decimal::new(5, 3))?;
        book.advance_batch(&[btc, eth])?;
        Ok(book)
    }

    fn admit(book: &mut PaperBook, asset: &str, id: u128, fraction: Decimal) -> TestResult {
        let (instrument, plan, signal) = approval(asset, id, Direction::Bullish, fraction)?;
        assert!(matches!(book.admit(&instrument, plan, &signal)?.as_slice(), [PortfolioEvent::EntryPlaced { .. }]));
        Ok(())
    }

    #[test]
    fn four_funded_orders_share_exactly_one_hundred_and_preserve_per_order_risk() -> TestResult {
        let mut book = ready()?;
        for (asset, id) in [("BTC", 1), ("BTC", 2), ("ETH", 3), ("ETH", 4)] {
            admit(&mut book, asset, id, Decimal::new(5, 3))?;
        }
        let snapshot = book.snapshot();
        assert_eq!(snapshot.orders.len(), 4);
        assert_eq!(snapshot.balance, Decimal::from(100));
        assert_eq!(snapshot.equity, Decimal::from(100));
        assert!(snapshot.reserved_risk <= Decimal::from(3));
        assert_eq!(add(book.free_cash, snapshot.reserved_capital)?, Decimal::from(100));
        for owned in &snapshot.orders {
            let accepted = book.accepted.get(&owned.order.intent_id).ok_or("missing approval")?;
            assert_eq!(owned.reserved_risk, accepted.plan.planned_loss());
            assert!(owned.order.quantity <= accepted.plan.quantity());
            let sleeve = book.sleeves.get(&owned.order.intent_id).ok_or("missing sleeve")?;
            assert_eq!(sleeve.broker.snapshot().initial_balance, owned.reserved_capital);
            assert!(sleeve.broker.snapshot().initial_balance < Decimal::from(100));
            let stop = accepted.plan.stop_loss_price().checked_mul(Decimal::new(9998, 4)).ok_or("stop overflow")?
                .checked_div(accepted.instrument.spec.price_tick()).ok_or("tick division")?.floor()
                .checked_mul(accepted.instrument.spec.price_tick()).ok_or("tick product")?;
            let price_loss = mul(sub(owned.order.entry_price, stop)?, owned.order.quantity)?;
            let fees = mul(mul(add(owned.order.entry_price, stop)?, owned.order.quantity)?, book.config.fee_rate())?;
            assert!(add(price_loss, fees)? <= owned.reserved_risk);
        }
        let json = serde_json::to_value(snapshot)?;
        assert!(json["balance"].is_string());
        assert!(json["orders"][0]["reserved_risk"].is_string());
        assert_eq!(json["orders"][0]["state"], "pending");
        Ok(())
    }

    #[test]
    fn count_direction_and_three_percent_caps_reject_without_changing_cash() -> TestResult {
        let mut book = ready()?;
        admit(&mut book, "BTC", 1, Decimal::new(5, 3))?;
        admit(&mut book, "BTC", 2, Decimal::new(5, 3))?;
        let before = book.snapshot();
        let (instrument, plan, signal) = approval("BTC", 3, Direction::Bullish, Decimal::new(5, 3))?;
        assert_eq!(book.admit(&instrument, plan, &signal), Err(BookError::SymbolLimit));
        assert_eq!(book.snapshot(), before);
        admit(&mut book, "ETH", 4, Decimal::new(5, 3))?;
        admit(&mut book, "ETH", 5, Decimal::new(5, 3))?;
        let (instrument, plan, signal) = approval("ETH", 6, Direction::Bullish, Decimal::new(5, 3))?;
        assert_eq!(book.admit(&instrument, plan, &signal), Err(BookError::ActiveLimit));

        let mut book = ready()?;
        for (asset, id) in [("BTC", 1), ("BTC", 2), ("ETH", 3)] {
            admit(&mut book, asset, id, Decimal::new(1, 2))?;
        }
        let (instrument, plan, signal) = approval("ETH", 4, Direction::Bullish, Decimal::new(1, 2))?;
        let before = book.snapshot();
        assert_eq!(book.admit(&instrument, plan, &signal), Err(BookError::RiskLimit));
        assert_eq!(book.snapshot(), before);
        Ok(())
    }

    #[test]
    fn external_approval_cannot_bypass_one_percent_account_risk() -> TestResult {
        let mut book = ready()?;
        let (instrument, plan, signal) = approval("BTC", 1, Direction::Bullish, Decimal::new(2, 2))?;
        assert!(plan.planned_loss() > Decimal::ONE);
        assert_eq!(book.admit(&instrument, plan, &signal), Err(BookError::RiskLimit));
        assert_eq!(book.snapshot().balance, Decimal::from(100));
        assert!(book.snapshot().orders.is_empty());
        Ok(())
    }

    #[test]
    fn opposing_same_symbol_direction_is_rejected_before_funding() -> TestResult {
        let mut book = PaperBook::new(PaperConfig::default().with_leverage(10)?)?;
        let (instrument, long, signal) = approval("BTC", 1, Direction::Bullish, Decimal::new(5, 3))?;
        book.advance_batch(std::slice::from_ref(&signal))?;
        book.admit(&instrument, long, &signal)?;
        let (_, short, short_signal) = approval("BTC", 2, Direction::Bearish, Decimal::new(5, 3))?;
        // A genuine strategy can emit both directions from one observed candle;
        // use the already observed signal with the independently approved side.
        assert_eq!(short_signal.close_time(), signal.close_time());
        let before = book.snapshot();
        assert_eq!(book.admit(&instrument, short, &signal), Err(BookError::OpposingDirection));
        assert_eq!(book.snapshot(), before);
        Ok(())
    }

    #[test]
    fn independent_fills_exits_and_fees_reconcile_to_one_account() -> TestResult {
        let mut book = ready()?;
        for (asset, id) in [("BTC", 1), ("BTC", 2), ("ETH", 3), ("ETH", 4)] {
            admit(&mut book, asset, id, Decimal::new(5, 3))?;
        }
        let fill = book.advance_batch(&[candle("BTC", 300, [103, 104, 101, 103])?, candle("ETH", 300, [103, 104, 101, 103])?])?;
        assert_eq!(fill.len(), 4);
        let entry_fees = fill.iter().try_fold(Decimal::ZERO, |sum, event| match event {
            PortfolioEvent::Execution { event: PaperEvent::EntryFilled { fee, .. }, .. } => add(sum, *fee),
            _ => Err(BookError::InvalidInput("expected entry fill")),
        })?;
        assert_eq!(book.snapshot().balance, sub(Decimal::from(100), entry_fees)?);
        assert_eq!(book.snapshot().fees_paid, entry_fees);
        assert!(book.snapshot().orders.iter().all(|order| order.state == BookOrderState::Open));
        let exits = book.advance_batch(&[candle("BTC", 360, [103, 113, 102, 112])?, candle("ETH", 360, [103, 104, 97, 98])?])?;
        assert_eq!(exits.len(), 4);
        let net = exits.iter().try_fold(Decimal::ZERO, |sum, event| match event {
            PortfolioEvent::Execution { event: PaperEvent::PositionClosed { net_pnl, .. }, .. } => add(sum, *net_pnl),
            _ => Err(BookError::InvalidInput("expected exit")),
        })?;
        let snapshot = book.snapshot();
        assert_eq!(snapshot.balance, add(Decimal::from(100), net)?);
        assert_eq!(snapshot.equity, snapshot.balance);
        assert_eq!(snapshot.realized_pnl, net);
        assert_eq!(snapshot.completed_trades, 4);
        assert_eq!(snapshot.wins, 2);
        assert_eq!(snapshot.losses, 2);
        assert!(snapshot.orders.is_empty());
        assert_eq!(snapshot.reserved_capital, Decimal::ZERO);
        assert_eq!(snapshot.reserved_risk, Decimal::ZERO);
        assert_eq!(snapshot.balance, book.free_cash);
        Ok(())
    }

    #[test]
    fn one_intent_cancellation_does_not_cancel_another_and_replay_never_refunds_twice() -> TestResult {
        let mut book = ready()?;
        let (instrument, plan, signal) = approval("BTC", 1, Direction::Bullish, Decimal::new(5, 3))?;
        book.admit(&instrument, plan.clone(), &signal)?;
        admit(&mut book, "BTC", 2, Decimal::new(5, 3))?;
        let events = book.cancel_intent(plan.intent_id(), at(300)?)?;
        assert!(matches!(events.as_slice(), [PortfolioEvent::Execution { event: PaperEvent::EntryCancelled { intent_id, .. }, .. }] if *intent_id == plan.intent_id()));
        assert_eq!(book.snapshot().orders.len(), 1);
        assert_eq!(book.snapshot().balance, Decimal::from(100));
        let before = book.snapshot();
        assert!(book.admit(&instrument, plan, &signal)?.is_empty());
        assert_eq!(book.snapshot(), before);
        assert!(matches!(book.cancel_intent(Uuid::from_u128(1), at(300)?), Err(BookError::InvalidInput(_))));
        assert_eq!(book.cancel_pending(at(300)?)?.len(), 1);
        assert_eq!(book.snapshot().available_cash, Decimal::from(100));
        assert!(book.cancel_pending(at(300)?)?.is_empty());
        Ok(())
    }

    #[test]
    fn shutdown_retains_open_exposure_while_cancelling_other_pending_owners() -> TestResult {
        let mut book = ready()?;
        admit(&mut book, "BTC", 1, Decimal::new(5, 3))?;
        admit(&mut book, "ETH", 2, Decimal::new(5, 3))?;
        book.advance_batch(&[candle("BTC", 300, [103, 104, 101, 103])?, candle("ETH", 300, [104, 105, 103, 104])?])?;
        let before = book.snapshot();
        let events = book.cancel_pending(at(360)?)?;
        assert_eq!(events.len(), 1);
        assert_eq!(book.snapshot().orders.len(), 1);
        assert_eq!(book.snapshot().orders[0].symbol, "BTCUSDT");
        assert_eq!(book.snapshot().orders[0].state, BookOrderState::Open);
        assert_eq!(book.snapshot().balance, before.balance);
        assert!(matches!(book.cancel_intent(Uuid::from_u128(1), at(360)?), Err(BookError::InvalidInput(_))));
        Ok(())
    }

    #[test]
    fn unrealized_losses_reduce_available_margin_but_profits_do_not_fund_new_orders() -> TestResult {
        let mut book = ready()?;
        admit(&mut book, "BTC", 1, Decimal::new(5, 3))?;
        book.advance_batch(&[candle("BTC", 300, [103, 104, 101, 103])?, candle("ETH", 300, [103, 104, 101, 103])?])?;
        book.advance_batch(&[candle("BTC", 360, [103, 104, 98, 98])?, candle("ETH", 360, [103, 104, 101, 103])?])?;
        let loss = book.snapshot();
        assert!(loss.unrealized_pnl < Decimal::ZERO);
        assert!(loss.available_cash < book.free_cash);
        assert!(loss.available_cash <= sub(loss.equity, loss.reserved_capital)?);
        book.advance_batch(&[candle("BTC", 420, [103, 111, 103, 111])?, candle("ETH", 420, [103, 104, 101, 103])?])?;
        let profit = book.snapshot();
        assert!(profit.unrealized_pnl > Decimal::ZERO);
        assert_eq!(profit.available_cash, book.free_cash);
        Ok(())
    }

    #[test]
    fn second_symbol_arithmetic_failure_cannot_partially_commit_first_symbols_profit() -> TestResult {
        let mut book = PaperBook::new(PaperConfig::default().with_leverage(10)?)?;
        let (btc_spec, btc, btc_signal) = approval("BTC", 1, Direction::Bullish, Decimal::new(5, 3))?;
        let (eth_spec, eth, eth_signal) = approval("ETH", 2, Direction::Bearish, Decimal::new(5, 3))?;
        book.advance_batch(&[btc_signal.clone(), eth_signal.clone()])?;
        book.admit(&btc_spec, btc, &btc_signal)?;
        book.admit(&eth_spec, eth, &eth_signal)?;
        book.advance_batch(&[candle("BTC", 300, [103, 104, 101, 103])?, candle("ETH", 300, [97, 99, 96, 97])?])?;
        let before = book.snapshot();
        let mut extreme = serde_json::to_value(candle("ETH", 360, [100, 101, 99, 100])?)?;
        for key in ["open", "high", "low", "close"] { extreme[key] = Decimal::MAX.to_string().into(); }
        let extreme: Candle = serde_json::from_value(extreme)?;
        assert!(book.advance_batch(&[candle("BTC", 360, [103, 113, 102, 112])?, extreme]).is_err());
        assert_eq!(book.snapshot(), before);
        assert_eq!(book.snapshot().completed_trades, 0);
        Ok(())
    }

    #[test]
    fn gaps_missing_symbols_changed_duplicates_and_stale_admissions_are_atomic() -> TestResult {
        let mut book = ready()?;
        admit(&mut book, "BTC", 1, Decimal::new(5, 3))?;
        let before = book.snapshot();
        assert!(book.advance_batch(&[candle("BTC", 300, [103, 104, 101, 103])?]).is_err());
        assert!(book.advance_batch(&[candle("BTC", 360, [103, 104, 101, 103])?, candle("ETH", 360, [103, 104, 101, 103])?]).is_err());
        assert!(book.advance_batch(&[candle("BTC", 240, [103, 104, 101, 103])?, candle("ETH", 240, [103, 104, 101, 103])?]).is_err());
        assert_eq!(book.snapshot(), before);
        let batch = book.last_batch.clone().ok_or("missing batch")?;
        assert!(book.advance_batch(&batch)?.is_empty());
        assert_eq!(book.snapshot(), before);
        book.advance_batch(&[candle("BTC", 300, [104, 105, 103, 104])?, candle("ETH", 300, [104, 105, 103, 104])?])?;
        let (instrument, plan, signal) = approval("ETH", 2, Direction::Bullish, Decimal::new(5, 3))?;
        assert!(matches!(book.admit(&instrument, plan, &signal), Err(BookError::InvalidInput(_))));
        Ok(())
    }

    #[test]
    fn negative_gap_losses_are_not_clipped_and_no_remaining_cash_is_invented() -> TestResult {
        let mut book = PaperBook::new(PaperConfig::default().with_leverage(10)?)?;
        let (instrument, plan, signal) = approval("ETH", 1, Direction::Bearish, Decimal::new(1, 2))?;
        book.advance_batch(std::slice::from_ref(&signal))?;
        book.admit(&instrument, plan, &signal)?;
        book.advance_batch(&[candle("ETH", 300, [97, 99, 96, 97])?])?;
        let events = book.advance_batch(&[candle("ETH", 360, [100_000, 100_001, 99_999, 100_000])?])?;
        assert!(matches!(events.as_slice(), [PortfolioEvent::Execution { event: PaperEvent::PositionClosed { reason: PaperExitReason::StopLoss, .. }, .. }]));
        let snapshot = book.snapshot();
        assert!(snapshot.balance < Decimal::ZERO);
        assert_eq!(snapshot.available_cash, Decimal::ZERO);
        assert_eq!(snapshot.balance, book.free_cash);
        assert_eq!(snapshot.equity, snapshot.balance);
        assert_eq!(snapshot.losses, 1);
        Ok(())
    }

    #[test]
    fn minimum_order_failure_and_conflicting_replay_never_leak_allocations() -> TestResult {
        let mut book = ready()?;
        let (mut instrument, plan, signal) = approval("BTC", 1, Direction::Bullish, Decimal::new(5, 3))?;
        instrument.min_notional = Decimal::from(10_000);
        let before = book.snapshot();
        assert_eq!(book.admit(&instrument, plan.clone(), &signal), Err(BookError::Broker(PaperError::MinimumOrder)));
        assert_eq!(book.snapshot(), before);
        assert!(book.accepted.is_empty());
        instrument.min_notional = Decimal::from(5);
        book.admit(&instrument, plan.clone(), &signal)?;
        instrument.min_notional = Decimal::from(6);
        let before = book.snapshot();
        assert_eq!(book.admit(&instrument, plan, &signal), Err(BookError::ConflictingReplay));
        assert_eq!(book.snapshot(), before);
        Ok(())
    }

    #[test]
    fn capital_fraction_is_an_account_cap_not_reapplied_to_each_independent_seed() -> TestResult {
        let config = PaperConfig::new(Decimal::from(100), Decimal::new(6, 4), Decimal::TWO,
            Decimal::new(1, 2), Decimal::TWO)?.with_leverage(10)?;
        let mut book = PaperBook::new(config)?;
        let (instrument, plan, signal) = approval("BTC", 1, Direction::Bullish, Decimal::new(5, 3))?;
        book.advance_batch(std::slice::from_ref(&signal))?;
        book.admit(&instrument, plan, &signal)?;
        let snapshot = book.snapshot();
        assert!(snapshot.reserved_capital <= Decimal::ONE);
        assert!(snapshot.reserved_capital > Decimal::new(9, 1));
        let (instrument, plan, signal) = approval("BTC", 2, Direction::Bullish, Decimal::new(5, 3))?;
        assert!(book.admit(&instrument, plan, &signal).is_err());
        assert_eq!(book.snapshot(), snapshot);
        Ok(())
    }

    #[test]
    fn future_cancellation_cannot_fund_an_admission_or_bar_from_an_earlier_time() -> TestResult {
        let mut book = ready()?;
        admit(&mut book, "BTC", 1, Decimal::new(5, 3))?;
        book.cancel_intent(Uuid::from_u128(1), at(361)?)?;
        let before = book.snapshot();
        let (instrument, plan, signal) = approval("BTC", 2, Direction::Bullish, Decimal::new(5, 3))?;
        assert!(matches!(book.admit(&instrument, plan, &signal), Err(BookError::InvalidInput(_))));
        assert!(matches!(book.advance_batch(&[candle("BTC", 300, [103, 104, 101, 103])?,
            candle("ETH", 300, [103, 104, 101, 103])?]), Err(BookError::InvalidInput(_))));
        assert!(matches!(book.cancel_pending(at(360)?), Err(BookError::InvalidInput(_))));
        assert_eq!(book.snapshot(), before);
        Ok(())
    }
}
