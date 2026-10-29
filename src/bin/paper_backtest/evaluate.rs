//! Fixed-profile, chronological development/holdout evaluation without tuning.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use ict_trading_bot::{Candle, FractalModel, Timeframe, strategy::StrategyEntryPolicy,
    paper::{BookOrderState, MultiPortfolio, MultiPortfolioSnapshot, MultiSymbolDiagnostics,
        PaperConfig, PaperEntryWindow, PaperEvent, PaperFillTiming, PaperTargetPolicy, PortfolioEvent,
        SmtResearchConfig}};
use rust_decimal::Decimal;
use serde_json::{json, Value};

use super::{BoxError, dataset::{Dataset, hash}};

const WARMUP: usize = 500;
const MIN_SCREENING_TRADES: u64 = 30;

#[derive(Clone, Copy)]
struct Profile {
    name: &'static str,
    window: PaperEntryWindow,
    fee_bps: u32,
    stop_bps: u32,
    target: PaperTargetPolicy,
    fill_timing: PaperFillTiming,
    entry: StrategyEntryPolicy,
    smt: Option<SmtResearchConfig>,
}

const PROFILES: [Profile; 34] = [
    Profile { name: "always_base", window: PaperEntryWindow::Always, fee_bps: 6, stop_bps: 2, target: PaperTargetPolicy::Nearest, fill_timing: PaperFillTiming::ImmediateNextBar, entry: StrategyEntryPolicy::LegacyRetracement, smt: None },
    Profile { name: "always_stressed_costs", window: PaperEntryWindow::Always, fee_bps: 10, stop_bps: 5, target: PaperTargetPolicy::Nearest, fill_timing: PaperFillTiming::ImmediateNextBar, entry: StrategyEntryPolicy::LegacyRetracement, smt: None },
    Profile { name: "killzones_base", window: PaperEntryWindow::Killzones, fee_bps: 6, stop_bps: 2, target: PaperTargetPolicy::Nearest, fill_timing: PaperFillTiming::ImmediateNextBar, entry: StrategyEntryPolicy::LegacyRetracement, smt: None },
    Profile { name: "killzones_stressed_costs", window: PaperEntryWindow::Killzones, fee_bps: 10, stop_bps: 5, target: PaperTargetPolicy::Nearest, fill_timing: PaperFillTiming::ImmediateNextBar, entry: StrategyEntryPolicy::LegacyRetracement, smt: None },
    Profile { name: "research_nearest_viable_base", window: PaperEntryWindow::Always, fee_bps: 6, stop_bps: 2, target: PaperTargetPolicy::NearestViable, fill_timing: PaperFillTiming::ImmediateNextBar, entry: StrategyEntryPolicy::LegacyRetracement, smt: None },
    Profile { name: "research_nearest_viable_stressed_costs", window: PaperEntryWindow::Always, fee_bps: 10, stop_bps: 5, target: PaperTargetPolicy::NearestViable, fill_timing: PaperFillTiming::ImmediateNextBar, entry: StrategyEntryPolicy::LegacyRetracement, smt: None },
    Profile { name: "always_delayed_one_bar_base", window: PaperEntryWindow::Always, fee_bps: 6, stop_bps: 2, target: PaperTargetPolicy::Nearest, fill_timing: PaperFillTiming::SkipFirstEligibleBar, entry: StrategyEntryPolicy::LegacyRetracement, smt: None },
    Profile { name: "always_delayed_one_bar_stressed_costs", window: PaperEntryWindow::Always, fee_bps: 10, stop_bps: 5, target: PaperTargetPolicy::Nearest, fill_timing: PaperFillTiming::SkipFirstEligibleBar, entry: StrategyEntryPolicy::LegacyRetracement, smt: None },
    Profile { name: "research_nearest_viable_delayed_one_bar_base", window: PaperEntryWindow::Always, fee_bps: 6, stop_bps: 2, target: PaperTargetPolicy::NearestViable, fill_timing: PaperFillTiming::SkipFirstEligibleBar, entry: StrategyEntryPolicy::LegacyRetracement, smt: None },
    Profile { name: "research_nearest_viable_delayed_one_bar_stressed_costs", window: PaperEntryWindow::Always, fee_bps: 10, stop_bps: 5, target: PaperTargetPolicy::NearestViable, fill_timing: PaperFillTiming::SkipFirstEligibleBar, entry: StrategyEntryPolicy::LegacyRetracement, smt: None },
    Profile { name: "entry_v2_nearest_base", window: PaperEntryWindow::Always, fee_bps: 6, stop_bps: 2, target: PaperTargetPolicy::Nearest, fill_timing: PaperFillTiming::ImmediateNextBar, entry: StrategyEntryPolicy::RestingOnConfirmation, smt: None },
    Profile { name: "entry_v2_nearest_stressed_costs", window: PaperEntryWindow::Always, fee_bps: 10, stop_bps: 5, target: PaperTargetPolicy::Nearest, fill_timing: PaperFillTiming::ImmediateNextBar, entry: StrategyEntryPolicy::RestingOnConfirmation, smt: None },
    Profile { name: "entry_v2_nearest_delayed_base", window: PaperEntryWindow::Always, fee_bps: 6, stop_bps: 2, target: PaperTargetPolicy::Nearest, fill_timing: PaperFillTiming::SkipFirstEligibleBar, entry: StrategyEntryPolicy::RestingOnConfirmation, smt: None },
    Profile { name: "entry_v2_nearest_delayed_stressed_costs", window: PaperEntryWindow::Always, fee_bps: 10, stop_bps: 5, target: PaperTargetPolicy::Nearest, fill_timing: PaperFillTiming::SkipFirstEligibleBar, entry: StrategyEntryPolicy::RestingOnConfirmation, smt: None },
    Profile { name: "entry_v2_viable_base", window: PaperEntryWindow::Always, fee_bps: 6, stop_bps: 2, target: PaperTargetPolicy::NearestViable, fill_timing: PaperFillTiming::ImmediateNextBar, entry: StrategyEntryPolicy::RestingOnConfirmation, smt: None },
    Profile { name: "entry_v2_viable_stressed_costs", window: PaperEntryWindow::Always, fee_bps: 10, stop_bps: 5, target: PaperTargetPolicy::NearestViable, fill_timing: PaperFillTiming::ImmediateNextBar, entry: StrategyEntryPolicy::RestingOnConfirmation, smt: None },
    Profile { name: "entry_v2_viable_delayed_base", window: PaperEntryWindow::Always, fee_bps: 6, stop_bps: 2, target: PaperTargetPolicy::NearestViable, fill_timing: PaperFillTiming::SkipFirstEligibleBar, entry: StrategyEntryPolicy::RestingOnConfirmation, smt: None },
    Profile { name: "entry_v2_viable_delayed_stressed_costs", window: PaperEntryWindow::Always, fee_bps: 10, stop_bps: 5, target: PaperTargetPolicy::NearestViable, fill_timing: PaperFillTiming::SkipFirstEligibleBar, entry: StrategyEntryPolicy::RestingOnConfirmation, smt: None },
    smt_profile("smt_f3_base", FractalModel::ThreeCandle, false, false, false),
    smt_profile("smt_f3_stressed_costs", FractalModel::ThreeCandle, false, true, false),
    smt_profile("smt_f3_delayed_base", FractalModel::ThreeCandle, false, false, true),
    smt_profile("smt_f3_delayed_stressed_costs", FractalModel::ThreeCandle, false, true, true),
    smt_profile("smt_f5_base", FractalModel::FiveCandle, false, false, false),
    smt_profile("smt_f5_stressed_costs", FractalModel::FiveCandle, false, true, false),
    smt_profile("smt_f5_delayed_base", FractalModel::FiveCandle, false, false, true),
    smt_profile("smt_f5_delayed_stressed_costs", FractalModel::FiveCandle, false, true, true),
    smt_profile("smt_f3_m5_aligned_base", FractalModel::ThreeCandle, true, false, false),
    smt_profile("smt_f3_m5_aligned_stressed_costs", FractalModel::ThreeCandle, true, true, false),
    smt_profile("smt_f3_m5_aligned_delayed_base", FractalModel::ThreeCandle, true, false, true),
    smt_profile("smt_f3_m5_aligned_delayed_stressed_costs", FractalModel::ThreeCandle, true, true, true),
    smt_profile("smt_f5_m5_aligned_base", FractalModel::FiveCandle, true, false, false),
    smt_profile("smt_f5_m5_aligned_stressed_costs", FractalModel::FiveCandle, true, true, false),
    smt_profile("smt_f5_m5_aligned_delayed_base", FractalModel::FiveCandle, true, false, true),
    smt_profile("smt_f5_m5_aligned_delayed_stressed_costs", FractalModel::FiveCandle, true, true, true),
];

const fn smt_profile(name: &'static str, fractal_model: FractalModel,
    require_m5_alignment: bool, stressed: bool, delayed: bool) -> Profile {
    let fill_timing = if delayed { PaperFillTiming::SkipFirstEligibleBar }
        else { PaperFillTiming::ImmediateNextBar };
    Profile { name, window: PaperEntryWindow::Always,
        fee_bps: if stressed { 10 } else { 6 }, stop_bps: if stressed { 5 } else { 2 },
        target: PaperTargetPolicy::NearestViable, fill_timing,
        entry: StrategyEntryPolicy::RestingOnConfirmation,
        smt: Some(SmtResearchConfig { fractal_model, require_m5_alignment, fill_timing }) }
}

impl Profile {
    fn config(self) -> Result<PaperConfig, BoxError> {
        Ok(PaperConfig::new(Decimal::from(100), Decimal::from(self.fee_bps).checked_div(Decimal::from(10_000)).ok_or("fee overflow")?,
            Decimal::from(self.stop_bps), Decimal::ONE, Decimal::TWO)?.with_leverage(10)?)
    }

    fn assumptions(self) -> Value {
        json!({
            "name": self.name, "entry_window": self.window, "fee_bps_per_side": self.fee_bps,
            "stop_slippage_bps": self.stop_bps, "initial_balance": "100", "leverage": 10,
            "risk_per_trade": "0.01", "max_total_risk": "0.03", "daily_drawdown_limit": "0.05",
            "minimum_net_rr": "2", "max_active_orders": 4, "max_active_per_symbol": 2,
            "target_policy": self.target, "target_source": "confirmed_unswept_opposing_liquidity",
            "entry_policy": self.entry,
            "entry_description": match self.entry {
                StrategyEntryPolicy::LegacyRetracement => "closed_candle_ce_touch_then_later_strict_trade_through",
                StrategyEntryPolicy::RestingOnConfirmation => "resting_ce_limit_at_confirmed_fvg_close_then_later_strict_trade_through",
            },
            "fill_timing": self.fill_timing,
            "smt_research": self.smt,
            "smt_required": self.smt.is_some(),
            "take_profit_on_fill_candle": false,
        })
    }
}

#[derive(Debug)]
struct Metrics {
    peak_equity: Decimal,
    maximum_drawdown: Decimal,
    maximum_drawdown_percent: Decimal,
    gross_wins: Decimal,
    gross_losses: Decimal,
    closed_net_pnl: Decimal,
    entry_fees_paid: Decimal,
    exit_fees_paid: Decimal,
    placed: u64,
    filled: u64,
    cancelled: u64,
    closed: u64,
    entry_halted_bars: u64,
    daily: BTreeMap<NaiveDate, Value>,
}

impl Metrics {
    fn new() -> Self {
        Self { peak_equity: Decimal::from(100), maximum_drawdown: Decimal::ZERO,
            maximum_drawdown_percent: Decimal::ZERO, gross_wins: Decimal::ZERO,
            gross_losses: Decimal::ZERO, closed_net_pnl: Decimal::ZERO,
            entry_fees_paid: Decimal::ZERO, exit_fees_paid: Decimal::ZERO,
            placed: 0, filled: 0, cancelled: 0, closed: 0, entry_halted_bars: 0,
            daily: BTreeMap::new() }
    }

    fn observe(&mut self, at: DateTime<Utc>, snapshot: &MultiPortfolioSnapshot,
        events: &[PortfolioEvent]) -> Result<(), BoxError> {
        let equity = snapshot.account.equity;
        self.peak_equity = self.peak_equity.max(equity);
        let drawdown = self.peak_equity.checked_sub(equity).ok_or("drawdown overflow")?;
        self.maximum_drawdown = self.maximum_drawdown.max(drawdown);
        self.maximum_drawdown_percent = self.maximum_drawdown_percent.max(percent(drawdown, self.peak_equity)?);
        if snapshot.halted { self.entry_halted_bars = self.entry_halted_bars.checked_add(1).ok_or("counter overflow")?; }
        for event in events {
            match event {
                PortfolioEvent::EntryPlaced { .. } => self.placed = self.placed.checked_add(1).ok_or("counter overflow")?,
                PortfolioEvent::Execution { event, .. } => match event {
                    PaperEvent::EntryFilled { fee, .. } => {
                        self.filled = self.filled.checked_add(1).ok_or("counter overflow")?;
                        self.entry_fees_paid = self.entry_fees_paid.checked_add(*fee).ok_or("entry fee overflow")?;
                    }
                    PaperEvent::EntryCancelled { .. } => self.cancelled = self.cancelled.checked_add(1).ok_or("counter overflow")?,
                    PaperEvent::PositionClosed { net_pnl, fee, .. } => {
                        self.closed = self.closed.checked_add(1).ok_or("counter overflow")?;
                        self.exit_fees_paid = self.exit_fees_paid.checked_add(*fee).ok_or("exit fee overflow")?;
                        self.closed_net_pnl = self.closed_net_pnl.checked_add(*net_pnl).ok_or("closed PnL overflow")?;
                        if *net_pnl > Decimal::ZERO {
                            self.gross_wins = self.gross_wins.checked_add(*net_pnl).ok_or("winning PnL overflow")?;
                        } else if *net_pnl < Decimal::ZERO {
                            self.gross_losses = self.gross_losses.checked_sub(*net_pnl).ok_or("losing PnL overflow")?;
                        }
                    }
                },
                PortfolioEvent::Halted { .. } => {}
            }
        }
        // The exclusive midnight close belongs to the bar's UTC day.
        let day = at.checked_sub_signed(chrono::Duration::nanoseconds(1)).ok_or("day overflow")?.date_naive();
        self.daily.insert(day, json!({"last_close": at, "equity": equity.to_string(),
            "balance": snapshot.account.balance.to_string(), "completed_trades": snapshot.account.completed_trades}));
        Ok(())
    }

    fn summary(&self, final_state: &MultiPortfolioSnapshot) -> Result<Value, BoxError> {
        let account = &final_state.account;
        if self.closed != account.completed_trades { return Err("trade ledger disagrees with account count".into()); }
        let net_equity_pnl = account.equity.checked_sub(account.initial_balance).ok_or("return overflow")?;
        let profit_factor = if self.gross_losses > Decimal::ZERO {
            Some(self.gross_wins.checked_div(self.gross_losses).ok_or("profit factor overflow")?.to_string())
        } else { None };
        let expectancy = if self.closed == 0 { None } else {
            Some(self.closed_net_pnl.checked_div(Decimal::from(self.closed)).ok_or("expectancy overflow")?.to_string())
        };
        let win_rate = if self.closed == 0 { None } else {
            Some(percent(Decimal::from(account.wins), Decimal::from(self.closed))?.to_string())
        };
        Ok(json!({
            "initial_balance": account.initial_balance.to_string(),
            "ending_balance": account.balance.to_string(), "ending_equity": account.equity.to_string(),
            "net_equity_pnl": net_equity_pnl.to_string(),
            "net_equity_return_percent": percent(net_equity_pnl, account.initial_balance)?.to_string(),
            "realized_account_pnl": account.realized_pnl.to_string(),
            "unrealized_mark_pnl": account.unrealized_pnl.to_string(),
            "fees_paid": account.fees_paid.to_string(), "closed_trade_net_pnl": self.closed_net_pnl.to_string(),
            "entry_fees_paid": self.entry_fees_paid.to_string(), "exit_fees_paid": self.exit_fees_paid.to_string(),
            "max_close_equity_drawdown": self.maximum_drawdown.to_string(),
            "max_close_equity_drawdown_percent": self.maximum_drawdown_percent.to_string(),
            "placed_entries": self.placed, "filled_entries": self.filled, "cancelled_entries": self.cancelled,
            "completed_trades": self.closed, "wins": account.wins, "losses": account.losses,
            "breakeven_trades": self.closed.checked_sub(account.wins).and_then(|n| n.checked_sub(account.losses)).ok_or("win/loss count mismatch")?,
            "win_rate_percent": win_rate, "net_profit_factor": profit_factor,
            "mean_closed_trade_net_pnl": expectancy,
            "pending_at_end": account.orders.iter().filter(|row| row.state == BookOrderState::Pending).count(),
            "open_positions_at_end": account.orders.iter().filter(|row| row.state == BookOrderState::Open).count(),
            "entry_halted_bars": self.entry_halted_bars,
        }))
    }
}

fn percent(numerator: Decimal, denominator: Decimal) -> Result<Decimal, BoxError> {
    numerator.checked_div(denominator).and_then(|value| value.checked_mul(Decimal::from(100)))
        .ok_or_else(|| "invalid percentage arithmetic".into())
}

// Entry frequency is measured from fills, never from intents or accepted limits.
// The calendar includes every observed day, including days with no executions.
fn frequency_summary(first_open: DateTime<Utc>, last_close: DateTime<Utc>,
    events: &[PortfolioEvent]) -> Result<Value, BoxError> {
    let seconds = last_close.signed_duration_since(first_open).num_seconds();
    if seconds <= 0 { return Err("frequency requires a positive observation window".into()); }
    let last_day = last_close.checked_sub_signed(chrono::Duration::nanoseconds(1))
        .ok_or("frequency day overflow")?.date_naive();
    let mut daily = BTreeMap::new();
    let mut day = first_open.date_naive();
    while day <= last_day {
        daily.insert(day, (0_u64, 0_u64));
        day = day.succ_opt().ok_or("frequency day overflow")?;
    }
    let mut fills = 0_u64;
    let mut closes = 0_u64;
    for event in events {
        let (at, entry_increment, exit_increment) = match event {
            PortfolioEvent::Execution { event: PaperEvent::EntryFilled { at, .. }, .. } => (*at, 1_u64, 0_u64),
            PortfolioEvent::Execution { event: PaperEvent::PositionClosed { at, .. }, .. } => (*at, 0_u64, 1_u64),
            _ => continue,
        };
        if at <= first_open || at > last_close { return Err("execution falls outside frequency window".into()); }
        let event_day = at.checked_sub_signed(chrono::Duration::nanoseconds(1))
            .ok_or("execution day overflow")?.date_naive();
        let row = daily.get_mut(&event_day).ok_or("execution date missing from observed days")?;
        row.0 = row.0.checked_add(entry_increment).ok_or("daily fill count overflow")?;
        row.1 = row.1.checked_add(exit_increment).ok_or("daily close count overflow")?;
        fills = fills.checked_add(entry_increment).ok_or("fill count overflow")?;
        closes = closes.checked_add(exit_increment).ok_or("close count overflow")?;
    }
    let mut zero_run = 0_u64;
    let mut longest_zero_run = 0_u64;
    for (filled, _) in daily.values() {
        zero_run = if *filled == 0 { zero_run.checked_add(1).ok_or("zero-day count overflow")? } else { 0 };
        longest_zero_run = longest_zero_run.max(zero_run);
    }
    let calendar_days = u64::try_from(daily.len())?;
    let elapsed_days = Decimal::from(seconds).checked_div(Decimal::from(86_400)).ok_or("elapsed days overflow")?;
    let filled_per_day = Decimal::from(fills).checked_div(elapsed_days).ok_or("fill frequency overflow")?;
    let closed_per_day = Decimal::from(closes).checked_div(elapsed_days).ok_or("close frequency overflow")?;
    Ok(json!({
        "calendar_days_observed": calendar_days, "elapsed_days": elapsed_days.to_string(),
        "filled_entries": fills, "closed_trades": closes,
        "filled_entries_per_elapsed_day": filled_per_day.to_string(),
        "closed_trades_per_elapsed_day": closed_per_day.to_string(),
        "filled_entries_per_calendar_day": Decimal::from(fills).checked_div(Decimal::from(calendar_days)).ok_or("calendar frequency overflow")?.to_string(),
        "entry_active_days": daily.values().filter(|(filled, _)| *filled > 0).count(),
        "days_without_entry": daily.values().filter(|(filled, _)| *filled == 0).count(),
        "max_consecutive_days_without_entry": longest_zero_run,
        "target_filled_entries_per_elapsed_day_exclusive": "10",
        "meets_average_frequency_target": filled_per_day > Decimal::TEN,
        "calendar_days_with_more_than_ten_fills": daily.values().filter(|(filled, _)| *filled > 10).count(),
        "daily": daily.iter().map(|(date, (filled, closed))| json!({"date":date,"filled_entries":filled,"closed_trades":closed})).collect::<Vec<_>>(),
        "holding": holding_summary(events, last_close)?,
        "interpretation": "Portfolio total across BTC and ETH. Rates include all observed time; calendar-day counts include zero-entry days and any partial boundary days. An average above ten does not imply more than ten fills on every day.",
    }))
}

fn holding_summary(events: &[PortfolioEvent], last_close: DateTime<Utc>) -> Result<Value, BoxError> {
    let mut open = BTreeMap::new();
    let mut held_seconds = 0_i64;
    let mut closed = 0_u64;
    for event in events {
        match event {
            PortfolioEvent::Execution { event: PaperEvent::EntryFilled { intent_id, at, .. }, .. } => {
                if open.insert(*intent_id, *at).is_some() { return Err("duplicate fill in holding ledger".into()); }
            }
            PortfolioEvent::Execution { event: PaperEvent::PositionClosed { intent_id, at, .. }, .. } => {
                let entry = open.remove(intent_id).ok_or("close lacks an observed entry fill")?;
                let elapsed = at.signed_duration_since(entry).num_seconds();
                if elapsed < 0 { return Err("position exit predates entry".into()); }
                held_seconds = held_seconds.checked_add(elapsed).ok_or("holding duration overflow")?;
                closed = closed.checked_add(1).ok_or("holding count overflow")?;
            }
            _ => {}
        }
    }
    let mean_minutes = if closed == 0 { None } else {
        Some(Decimal::from(held_seconds).checked_div(Decimal::from(closed))
            .and_then(|value| value.checked_div(Decimal::from(60))).ok_or("mean holding overflow")?.to_string())
    };
    let remaining = open.iter().map(|(id, at)| {
        let seconds = last_close.signed_duration_since(*at).num_seconds();
        if seconds < 0 { return Err("open entry occurs after boundary".into()); }
        Ok(json!({"intent_id":id,"entry_observed_at":at,
            "observed_minutes_at_boundary": Decimal::from(seconds).checked_div(Decimal::from(60)).ok_or("open holding overflow")?.to_string()}))
    }).collect::<Result<Vec<_>, BoxError>>()?;
    Ok(json!({"closed_trade_count":closed,"mean_closed_trade_observed_minutes":mean_minutes,
        "mean_closed_trade_observed_m1_bars":mean_minutes,"open_at_boundary":remaining,
        "interpretation":"Durations use fill/exit observation candle closes, not unknown intrabar execution times. Same-bar stop-outs have zero observed elapsed bars. Open positions are excluded from closed-trade means and retain their boundary age."}))
}

fn diagnostic(row: &MultiSymbolDiagnostics) -> Value {
    let live = &row.live;
    json!({"symbol": row.symbol, "last_close": row.last_close,
        "discovery_stage": format!("{:?}", row.discovery_stage),
        "last_discard": row.last_discard.map(|last| json!({"at":last.at,"reason":format!("{:?}",last.reason)})),
        "live_counters": {"first_close":live.first_close,"candles":live.candles,
            "liquidity_sweeps":live.liquidity_sweeps,"market_structure_shifts":live.market_structure_shifts,
            "fair_value_gaps":live.fair_value_gaps,"displacement_bars":live.displacement_bars,
            "intents":live.intents,"admitted":live.admitted,"rejected":live.rejected,
            "rejection_reasons":live.rejection_reasons.iter().map(|row| json!({"reason":format!("{:?}",row.reason),"count":row.count})).collect::<Vec<_>>(),
            "discards":live.discards.iter().map(|row| json!({"reason":format!("{:?}",row.reason),"count":row.count})).collect::<Vec<_>>(),
            "admission_failures":live.admission_failures.iter().map(|row| json!({"reason":format!("{:?}",row.reason),"count":row.count})).collect::<Vec<_>>(),
            "last_admission_failure":live.last_admission_failure.map(|failure| json!({
                "at":failure.at,"intent_id":failure.intent_id,"reason":format!("{:?}",failure.kind),
                "raw_target_price":failure.target_price.map(|value|value.to_string()),
                "actual_gross_rr":failure.actual_gross_rr.map(|value|value.to_string()),
                "required_gross_rr":failure.required_gross_rr.map(|value|value.to_string()),
                "calculated_quantity":failure.calculated_quantity.map(|value|value.to_string()),
                "minimum_quantity":failure.minimum_quantity.map(|value|value.to_string()),
            }))}})
}

fn segment(data: &Dataset, start: usize, end: usize, warmup: usize, profile: Profile,
    name: &str) -> Result<Value, BoxError> {
    if start < warmup || end <= start || end > data.series[0].len() { return Err("invalid evaluation segment".into()); }
    let mut engine = if let Some(smt) = profile.smt {
        MultiPortfolio::new_for_smt_research(data.instruments.clone(), profile.config()?, smt)?
    } else {
        MultiPortfolio::new_for_research_with_entry_policy(data.instruments.clone(), profile.config()?, Timeframe::M1, profile.window, profile.target, profile.fill_timing, profile.entry)?
    };
    for index in start - warmup..start { engine.warm_up(&batch(data, index))?; }
    let mut metrics = Metrics::new();
    let mut ledger = Vec::new();
    for index in start..end {
        let candles = batch(data, index);
        let step = engine.process_batch(&candles)?;
        metrics.observe(candles[0].close_time(), &step.snapshot, &step.events)?;
        ledger.extend(step.events);
    }
    let final_state = engine.snapshot();
    let mut warnings = vec!["UNVALIDATED: this is an OHLC paper experiment, not evidence of live profitability."];
    if metrics.closed < MIN_SCREENING_TRADES { warnings.push("Fewer than 30 closed trades; sample is below the screening minimum, which itself is not statistical validation."); }
    if !final_state.account.orders.is_empty() { warnings.push("Open/pending exposure remains at the boundary; no forced exit or cancellation is fabricated."); }
    if metrics.gross_losses == Decimal::ZERO { warnings.push("Profit factor is undefined without a losing closed trade; null does not mean infinite profitability."); }
    Ok(json!({
        "profile":profile.name,"segment":name,"status":"UNVALIDATED",
        "warmup_first_open":data.series[0][start-warmup].open_time(),
        "warmup_last_close":data.series[0][start-1].close_time(),"warmup_bars":warmup,
        "first_live_open":data.series[0][start].open_time(),"last_live_close":data.series[0][end-1].close_time(),
        "live_bars_per_symbol":end-start,"metrics":metrics.summary(&final_state)?,
        "frequency":frequency_summary(data.series[0][start].open_time(), data.series[0][end-1].close_time(), &ledger)?,
        "smt_research":engine.smt_research_report(),
        "daily_last_marks":metrics.daily,"final_snapshot":final_state,
        "diagnostics":engine.diagnostics().iter().map(diagnostic).collect::<Vec<_>>(),
        "trade_events":ledger,"warnings":warnings,
    }))
}

fn batch(data: &Dataset, index: usize) -> Vec<Candle> {
    data.series.iter().map(|series| series[index].clone()).collect()
}

// Bind reports to the compiled evaluator and the implementation it exercises,
// including uncommitted source. A package version or Git label alone would not
// identify a locally modified engine. These are source hashes, not a binary
// reproducibility claim across compilers or operating systems.
fn code_provenance() -> Value {
    let sources: &[(&str, &[u8])] = &[
        ("Cargo.toml", include_bytes!("../../../Cargo.toml")),
        ("Cargo.lock", include_bytes!("../../../Cargo.lock")),
        ("src/lib.rs", include_bytes!("../../lib.rs")),
        ("src/models.rs", include_bytes!("../../models.rs")),
        ("src/decimal_codec.rs", include_bytes!("../../decimal_codec.rs")),
        ("src/candle_store.rs", include_bytes!("../../candle_store.rs")),
        ("src/ict_engine.rs", include_bytes!("../../ict_engine.rs")),
        ("src/strategy.rs", include_bytes!("../../strategy.rs")),
        ("src/smt.rs", include_bytes!("../../smt.rs")),
        ("src/risk.rs", include_bytes!("../../risk.rs")),
        ("src/risk/checkpoint.rs", include_bytes!("../../risk/checkpoint.rs")),
        ("src/paper.rs", include_bytes!("../../paper.rs")),
        ("src/paper/book.rs", include_bytes!("../../paper/book.rs")),
        ("src/paper/broker.rs", include_bytes!("../../paper/broker.rs")),
        ("src/paper/engine.rs", include_bytes!("../../paper/engine.rs")),
        ("src/paper/market.rs", include_bytes!("../../paper/market.rs")),
        ("src/paper/multi_portfolio.rs", include_bytes!("../../paper/multi_portfolio.rs")),
        ("src/paper/research_context.rs", include_bytes!("../../paper/research_context.rs")),
        ("src/paper/smt_research.rs", include_bytes!("../../paper/smt_research.rs")),
        ("src/paper/portfolio.rs", include_bytes!("../../paper/portfolio.rs")),
        ("src/bin/paper_backtest.rs", include_bytes!("../paper_backtest.rs")),
        ("src/bin/paper_backtest/dataset.rs", include_bytes!("dataset.rs")),
        ("src/bin/paper_backtest/evaluate.rs", include_bytes!("evaluate.rs")),
    ];
    Value::Array(sources.iter().map(|(file, bytes)| json!({"file":file,"sha256":hash(bytes)})).collect())
}

pub(super) fn report(data: &Dataset, split_at: DateTime<Utc>) -> Result<Value, BoxError> {
    let first = data.series.first().ok_or("empty dataset")?;
    let split = first.iter().position(|bar| bar.open_time() == split_at).ok_or("split boundary is outside the dataset")?;
    if split <= WARMUP || split >= first.len() { return Err("split needs nonempty development and holdout plus 500 warmup bars".into()); }
    let code = code_provenance();
    let configuration = json!({
        "evaluation_engine":"paper-research-v4", "baseline_execution_family":"ict-paper-portfolio-v4",
        "compiled_sources_sha256":hash(&serde_json::to_vec(&code)?),
        "warmup_bars_per_segment":WARMUP,
        "split_at_first_holdout_open":split_at,
        "segments_have_independent_accounts":true,"profiles":PROFILES.iter().map(|p| p.assumptions()).collect::<Vec<_>>(),
        "minimum_screening_closed_trades":MIN_SCREENING_TRADES,"dataset_sha256":data.provenance.combined_sha256,
        "frequency_goal":{"portfolio":"BTCUSDT+ETHUSDT","unit":"actual_filled_entries_per_elapsed_day","strictly_greater_than":"10","risk_limits_unchanged":true},
        "smt_definition":{"required_for_smt_profiles":true,"pair":"BTCUSDT/ETHUSDT",
            "observations":"synchronized_closed_M1_candles","anchors":"prior_confirmed_unconsumed_same_kind_fractal_pivots",
            "maximum_anchor_skew_m1_bars":3,"maximum_anchor_age_m1_bars":60,"evidence_ttl_m1_bars":12,
            "divergence":"one_asset_closes_back_after_a_strict_sweep_while_the_peer_does_not_break_its_corresponding_level",
            "entry":"genuine_M1_MSS_displacement_and_next_candle_FVG_confirmation",
            "m5_structure_fractal":"five_candle",
            "m5_alignment":"optional_direction_from_completed_M5_BOS_or_MSS_only; not a synthetic_M1_MSS"},
    });
    let mut results = Vec::new();
    for profile in PROFILES {
        results.push(segment(data, WARMUP, split, WARMUP, profile, "development")?);
        results.push(segment(data, split, first.len(), WARMUP, profile, "holdout")?);
    }
    Ok(json!({
        "report_version":4,"status":"UNVALIDATED","configuration_sha256":hash(&serde_json::to_vec(&configuration)?),
        "configuration":configuration,"compiled_sources":code,"provenance":data.provenance,"manifest":data.manifest,
        "results":results,
        "limitations":[
            "Historical reconstruction uses the supplied metadata snapshot; historical tick/quantity/minimum filters may differ.",
            "No funding, mark-price maintenance margin, liquidation, order-book queue, partial fills or market-impact model.",
            "REST settlement, polling and admission latency are not simulated; next-bar OHLC may include prices occurring before an order could actually be admitted.",
            "Delayed profiles skip the first complete entry-eligible bar as a fixed sensitivity test. They do not reproduce actual latency or eliminate retrospective fills during delayed-data catch-up, and cannot establish a lower bound on losses or returns.",
            "Drawdown uses candle-close equity; it is not a complete intrabar drawdown or liquidation-risk measure.",
            "Holdout is chronological, with a fresh 100-USDT account and preceding-only warmup. Development and holdout returns must not be compounded as one account.",
            "All candidate profiles are fixed before evaluation. Comparing several profiles on the same holdout does not make a chosen winner independently validated.",
            "Dataset dates do not establish data novelty. A previously examined holdout remains reused; an earlier-period replay is a retrospective robustness check, not forward validation.",
            "Raw sweep/MSS/FVG counts are independent events, not necessarily linked setup confirmations.",
            "SMT is a precisely defined BTC/ETH cross-market divergence test, not proof that correlation persists or that a trade has an advantage. M5-aligned variants remain genuine M1 MSS/FVG reversals aligned with completed M5 structure.",
            "SMT is mandatory at admission and rechecked on every completed pair. Loss or expiry cancels only entries still pending after that bar is processed; a peer breach at that close never retroactively erases an earlier possible same-bar fill. Intrabar SMT validity at the exact fill instant cannot be inferred from OHLC.",
            "Frequency is measured from actual simulated fills. No parameter is relaxed to force the requested greater-than-ten-fills-per-day target; unmet targets are reported explicitly.",
            "Fees and stop slippage are explicit scenario assumptions, not the account's verified exchange fee tier.",
            "No parameter search, strategy promotion or live deployment occurs. Positive historical PnL does not guarantee future profit."
        ]
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use ict_trading_bot::{CandleInput, Exchange, InstrumentId, MarketKind, VolumeUnit, risk::InstrumentRiskSpec,
        paper::{PaperInstrument, PaperOrderSnapshot, PaperExitReason}};
    use uuid::Uuid;
    use super::super::dataset::{Manifest, Provenance};

    fn at(minute: i64) -> Result<DateTime<Utc>, BoxError> {
        DateTime::parse_from_rfc3339("2026-10-28T11:00:00Z")?.with_timezone(&Utc)
            .checked_add_signed(Duration::minutes(minute)).ok_or_else(|| "fixture time overflow".into())
    }

    fn fixture(prices: &[[i64; 4]]) -> Result<Dataset, BoxError> {
        let mut instruments = Vec::new();
        let mut series = Vec::new();
        for (symbol, canonical) in [("BTCUSDT","BTC-USDT-PERP"),("ETHUSDT","ETH-USDT-PERP")] {
            let instrument = InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual, symbol, canonical)?;
            instruments.push(PaperInstrument { spec:InstrumentRiskSpec::new(instrument.clone(), Decimal::new(1,1),
                Decimal::new(1,3),Decimal::new(1,3),Some(Decimal::from(100)),Decimal::ONE)?,min_notional:Decimal::ONE });
            let candles = prices.iter().enumerate().map(|(index, row)| {
                let row = if symbol == "BTCUSDT" { *row } else { [100,102,98,101] };
                let minute = i64::try_from(index)?;
                Ok(Candle::new(CandleInput { instrument:instrument.clone(),timeframe:Timeframe::M1,
                    open_time:at(minute)?,close_time:at(minute+1)?,open:Decimal::from(row[0]),high:Decimal::from(row[1]),
                    low:Decimal::from(row[2]),close:Decimal::from(row[3]),volume:Decimal::ONE,
                    volume_unit:VolumeUnit::BaseAsset,trade_count:None,is_closed:true })?)
            }).collect::<Result<Vec<_>,BoxError>>()?;
            series.push(candles);
        }
        Ok(Dataset {manifest:Manifest {version:1,source:"deterministic fixture".to_owned(),captured_at:at(10_000)?,metadata_as_of:at(10_000)?,instruments:Vec::new()},
            provenance:Provenance {manifest_sha256:"fixture".to_owned(),files:Vec::new(),combined_sha256:"fixture".to_owned()},instruments,series})
    }

    fn setup() -> Vec<[i64;4]> {
        let mut prices=vec![[100,102,98,101];9];
        prices[2]=[100,150,98,101];
        prices.extend([[100,102,95,101],[100,102,97,101],[100,102,98,101],[100,105,98,101],
            [100,103,97,100],[99,102,96,98],[98,99,93,94],[94,97,92,94],[94,97,91,93],
            [93,96,90,93],[94,98,92,96],[96,99,93,97],[97,100,89,94],[94,112,94,111],
            [110,113,108,111],[110,112,103,109]]);
        prices
    }

    fn frequency_event(id: u128, at: DateTime<Utc>, closed: bool) -> Result<PortfolioEvent,BoxError> {
        let order=PaperOrderSnapshot {intent_id:Uuid::from_u128(id),direction:ict_trading_bot::Direction::Bullish,
            entry_price:Decimal::from(100),quantity:Decimal::ONE,stop_loss_price:Decimal::from(99),
            take_profit_price:Decimal::from(103),expires_at:at.checked_add_signed(Duration::minutes(12)).ok_or("expiry")?};
        let event=if closed {
            PaperEvent::PositionClosed {intent_id:order.intent_id,at,exit_price:Decimal::from(103),
                quantity:Decimal::ONE,gross_pnl:Decimal::from(3),net_pnl:Decimal::from(3),fee:Decimal::ZERO,
                reason:PaperExitReason::TakeProfit}
        } else {
            PaperEvent::EntryFilled {intent_id:order.intent_id,at,price:Decimal::from(100),quantity:Decimal::ONE,fee:Decimal::ZERO}
        };
        Ok(PortfolioEvent::Execution {symbol:"BTCUSDT".to_owned(),order,event})
    }

    #[test]
    fn frequency_counts_fills_zero_days_and_closed_only_holding_means() -> Result<(),BoxError> {
        let start=at(-660)?;
        let end=start+Duration::days(5);
        let first=start+Duration::days(2)+Duration::minutes(10);
        let second=first+Duration::minutes(120);
        let third=start+Duration::days(4)+Duration::minutes(10);
        let mut events=vec![frequency_event(1,first,false)?,frequency_event(1,first+Duration::minutes(60),true)?,
            frequency_event(2,second,false)?,frequency_event(2,second,true)?,frequency_event(3,third,false)?];
        // A placed limit on an otherwise idle day must not count as a fill.
        let PortfolioEvent::Execution { order, .. }=frequency_event(4,start+Duration::minutes(60),false)?
            else { return Err("fixture order".into()); };
        events.insert(0,PortfolioEvent::EntryPlaced {symbol:"BTCUSDT".to_owned(),at:start+Duration::minutes(60),order});
        let summary=frequency_summary(start,end,&events)?;
        assert_eq!(summary["calendar_days_observed"],5);
        assert_eq!(summary["filled_entries"],3);
        assert_eq!(summary["closed_trades"],2);
        assert_eq!(Decimal::from_str_exact(summary["filled_entries_per_elapsed_day"].as_str().ok_or("fill frequency")?)?,Decimal::new(6,1));
        assert_eq!(Decimal::from_str_exact(summary["closed_trades_per_elapsed_day"].as_str().ok_or("close frequency")?)?,Decimal::new(4,1));
        assert_eq!(summary["entry_active_days"],2);
        assert_eq!(summary["days_without_entry"],3);
        assert_eq!(summary["max_consecutive_days_without_entry"],2);
        assert_eq!(summary["meets_average_frequency_target"],false);
        assert_eq!(summary["holding"]["mean_closed_trade_observed_minutes"],"30");
        assert_eq!(summary["holding"]["mean_closed_trade_observed_m1_bars"],"30");
        assert_eq!(summary["holding"]["open_at_boundary"][0]["observed_minutes_at_boundary"],"1430");
        assert_eq!(summary["daily"][0]["filled_entries"],0);
        assert_eq!(summary["daily"][2]["filled_entries"],2);
        Ok(())
    }

    #[test]
    fn frequency_assigns_midnight_to_its_candle_day_and_target_is_exclusive() -> Result<(),BoxError> {
        let start=at(-660)?;
        let end=start+Duration::days(1);
        let empty=frequency_summary(start,end,&[])?;
        assert_eq!(empty["max_consecutive_days_without_entry"],1);
        assert_eq!(empty["holding"]["mean_closed_trade_observed_minutes"],Value::Null);
        let mut events=Vec::new();
        for id in 1_u32..=10 {
            let time=start+Duration::minutes(i64::from(id));
            events.push(frequency_event(u128::from(id),time,false)?);
            events.push(frequency_event(u128::from(id),time,true)?);
        }
        assert_eq!(frequency_summary(start,end,&events)?["meets_average_frequency_target"],false);
        events.push(frequency_event(11,end,false)?);
        let summary=frequency_summary(start,end,&events)?;
        assert_eq!(summary["meets_average_frequency_target"],true);
        assert_eq!(summary["calendar_days_with_more_than_ten_fills"],1);
        assert_eq!(summary["daily"][0]["filled_entries"],11);
        assert_eq!(summary["holding"]["open_at_boundary"][0]["observed_minutes_at_boundary"],"0");
        assert!(frequency_summary(start,end,&[frequency_event(1,start,false)?]).is_err());
        assert!(frequency_summary(start,end,&[frequency_event(1,end,true)?]).is_err());
        Ok(())
    }

    #[test]
    fn smt_matrix_is_fixed_complete_and_retains_rr_risk_and_fill_policies() -> Result<(),BoxError> {
        let mut combinations=std::collections::BTreeSet::new();
        for profile in &PROFILES[18..] {
            let smt=profile.smt.ok_or("SMT profile missing policy")?;
            assert_eq!(profile.window,PaperEntryWindow::Always);
            assert_eq!(profile.target,PaperTargetPolicy::NearestViable);
            assert_eq!(profile.entry,StrategyEntryPolicy::RestingOnConfirmation);
            assert_eq!(profile.fill_timing,smt.fill_timing);
            let description=profile.assumptions();
            assert_eq!(description["minimum_net_rr"],"2");
            assert_eq!(description["risk_per_trade"],"0.01");
            assert_eq!(description["max_total_risk"],"0.03");
            combinations.insert(format!("{:?}:{}:{}:{:?}",smt.fractal_model,smt.require_m5_alignment,profile.fee_bps,profile.fill_timing));
        }
        assert_eq!(combinations.len(),16);
        assert!(PROFILES[..18].iter().all(|profile| profile.smt.is_none()));
        Ok(())
    }

    #[test]
    fn boundary_keeps_pending_and_open_exposure_without_inventing_a_win() -> Result<(),BoxError> {
        let mut prices=vec![[100,102,98,101];WARMUP];
        prices.extend(setup());
        let data=fixture(&prices)?;
        let pending=segment(&data,WARMUP,prices.len(),WARMUP,PROFILES[0],"fixture")?;
        assert_eq!(pending["metrics"]["placed_entries"],1);
        assert_eq!(pending["metrics"]["pending_at_end"],1);
        assert_eq!(pending["metrics"]["completed_trades"],0);
        assert_eq!(pending["metrics"]["net_profit_factor"],Value::Null);
        prices.push([109,110,103,109]);
        let data=fixture(&prices)?;
        let open=segment(&data,WARMUP,prices.len(),WARMUP,PROFILES[0],"fixture")?;
        assert_eq!(open["metrics"]["open_positions_at_end"],1);
        assert_eq!(open["metrics"]["completed_trades"],0);
        assert_ne!(open["metrics"]["fees_paid"],"0");
        assert_eq!(open["metrics"]["closed_trade_net_pnl"],"0");
        Ok(())
    }

    #[test]
    fn holdout_uses_only_preceding_warmup_and_does_not_inherit_development_positions() -> Result<(),BoxError> {
        let mut prices=vec![[100,102,98,101];WARMUP];
        prices.extend(setup());
        let split=prices.len();
        prices.extend([[109,110,103,109],[110,151,109,150]]);
        let data=fixture(&prices)?;
        let result=report(&data,at(i64::try_from(split)?)?)?;
        assert_eq!(result["results"].as_array().ok_or("results")?.len(),PROFILES.len()*2);
        let dev=&result["results"][0];
        let hold=&result["results"][1];
        assert_eq!(dev["metrics"]["pending_at_end"],1);
        assert_eq!(hold["metrics"]["placed_entries"],0);
        assert_eq!(hold["metrics"]["completed_trades"],0);
        assert_eq!(dev["last_live_close"],hold["first_live_open"]);
        assert_eq!(hold["warmup_last_close"],hold["first_live_open"]);
        // Changing future holdout prices must not affect any development result.
        prices[split]=[100,102,98,101];
        prices[split+1]=[100,102,98,101];
        let changed=report(&fixture(&prices)?,at(i64::try_from(split)?)?)?;
        for index in (0..PROFILES.len()*2).step_by(2) { assert_eq!(result["results"][index],changed["results"][index]); }
        Ok(())
    }

    #[test]
    fn delayed_profile_embargo_reaches_actual_portfolio_execution() -> Result<(),BoxError> {
        let mut prices=vec![[100,102,98,101];WARMUP];
        prices.extend(setup());
        prices.push([109,110,103,109]);
        let data=fixture(&prices)?;
        let default=segment(&data,WARMUP,prices.len(),WARMUP,PROFILES[0],"fixture")?;
        let delayed=segment(&data,WARMUP,prices.len(),WARMUP,PROFILES[6],"fixture")?;
        assert_eq!(default["metrics"]["open_positions_at_end"],1);
        assert_eq!(delayed["metrics"]["open_positions_at_end"],0);
        assert_eq!(delayed["metrics"]["pending_at_end"],1);
        assert_eq!(delayed["metrics"]["fees_paid"],"0");
        prices.push([109,110,103,109]);
        let later=segment(&fixture(&prices)?,WARMUP,prices.len(),WARMUP,PROFILES[6],"fixture")?;
        assert_eq!(later["metrics"]["open_positions_at_end"],1);
        assert_eq!(later["metrics"]["filled_entries"],1);
        Ok(())
    }

    #[test]
    fn entry_v2_profiles_reach_confirmation_and_respect_the_fill_embargo() -> Result<(),BoxError> {
        let mut prices=vec![[100,102,98,101];WARMUP];
        let formation=setup();
        prices.extend_from_slice(&formation[..formation.len()-1]);
        let confirmed=segment(&fixture(&prices)?,WARMUP,prices.len(),WARMUP,PROFILES[10],"fixture")?;
        let legacy=segment(&fixture(&prices)?,WARMUP,prices.len(),WARMUP,PROFILES[0],"fixture")?;
        assert_eq!(confirmed["metrics"]["pending_at_end"],1);
        assert_eq!(confirmed["metrics"]["filled_entries"],0);
        assert_eq!(legacy["metrics"]["placed_entries"],0);
        prices.push([110,112,103,109]);
        let data=fixture(&prices)?;
        let normal=segment(&data,WARMUP,prices.len(),WARMUP,PROFILES[10],"fixture")?;
        let delayed=segment(&data,WARMUP,prices.len(),WARMUP,PROFILES[12],"fixture")?;
        assert_eq!(normal["metrics"]["open_positions_at_end"],1);
        assert_eq!(delayed["metrics"]["pending_at_end"],1);
        assert_eq!(delayed["metrics"]["filled_entries"],0);
        prices.push([109,110,103,109]);
        let later=segment(&fixture(&prices)?,WARMUP,prices.len(),WARMUP,PROFILES[12],"fixture")?;
        assert_eq!(later["metrics"]["filled_entries"],1);
        Ok(())
    }

    #[test]
    fn metrics_count_net_trade_pnl_once_and_drawdown_from_marked_equity() -> Result<(),BoxError> {
        let data=fixture(&[[100,102,98,101]])?;
        let engine=MultiPortfolio::new_with_entry_window(data.instruments,PROFILES[0].config()?,Timeframe::M1,PaperEntryWindow::Always)?;
        let mut state=engine.snapshot();
        let mut metrics=Metrics::new();
        state.account.equity=Decimal::from(110);
        metrics.observe(at(1)?,&state,&[])?;
        state.account.equity=Decimal::from(99);
        state.account.completed_trades=1;
        state.account.losses=1;
        let order=PaperOrderSnapshot {intent_id:Uuid::from_u128(1),direction:ict_trading_bot::Direction::Bullish,
            entry_price:Decimal::from(100),quantity:Decimal::ONE,stop_loss_price:Decimal::from(99),take_profit_price:Decimal::from(103),expires_at:at(10)?};
        let event=PortfolioEvent::Execution {symbol:"BTCUSDT".to_owned(),order,event:PaperEvent::PositionClosed {
            intent_id:Uuid::from_u128(1),at:at(2)?,exit_price:Decimal::from(99),quantity:Decimal::ONE,
            gross_pnl:Decimal::from(-1),net_pnl:Decimal::new(-112,2),fee:Decimal::new(6,2),reason:PaperExitReason::StopLoss}};
        metrics.observe(at(2)?,&state,&[event])?;
        let summary=metrics.summary(&state)?;
        assert_eq!(summary["max_close_equity_drawdown"],"11");
        assert_eq!(Decimal::from_str_exact(summary["max_close_equity_drawdown_percent"].as_str().ok_or("drawdown")?)?,Decimal::TEN);
        assert_eq!(summary["mean_closed_trade_net_pnl"],"-1.12");
        assert_eq!(summary["closed_trade_net_pnl"],"-1.12");
        assert_eq!(summary["net_profit_factor"],"0");
        Ok(())
    }
}
