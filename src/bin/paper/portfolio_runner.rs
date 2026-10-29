//! Public-data portfolio runner. Telegram is an optional, best-effort observer.

use std::{collections::{BTreeMap, VecDeque}, future::Future, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::future::try_join_all;
use ict_trading_bot::{
    ict_engine::{Killzone, KillzoneFilter},
    paper::{BookOrderState, PaperConfig, PaperEvent, PaperInstrument, PaperMarketError, PaperOrderSnapshot, MultiPortfolio as PaperPortfolio, PortfolioEvent, MultiPortfolioSnapshot as PortfolioSnapshot, PublicPaperMarket},
    telegram::{TelegramConfig, TelegramNotifier},
    Candle, Direction, Timeframe,
};
use uuid::Uuid;

use super::{
    check_freshness, ensure_feed_current, feed_deadline, shutdown_signal,
    multi_session::{MultiHeader as PortfolioHeader, MultiLog as PortfolioLog},
    recovery::{self, RecoveryError, RecoveryNotice},
    session::BoxError, PortfolioOptions,
};

// REST can publish a boundary candle before its final OHLC/volume settles.
// This is a conservative observation delay, not an exchange finality promise.
const SETTLEMENT_DELAY_SECONDS: i64 = 10;

pub async fn live(options: PortfolioOptions) -> Result<(), BoxError> {
    // Only Telegram secrets are read here. No exchange credentials are accepted.
    let notifier = TelegramConfig::from_env()?.map(TelegramNotifier::spawn).transpose()?;
    let session = Uuid::new_v4();
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let result = run_session(&options, notifier.as_ref(), session, &mut shutdown).await;
    if result.is_err() {
        // Error objects are not sent externally: they could include paths or IDs.
        notify(notifier.as_ref(), format!("[PAPER] STOPPED / ERROR\nSession: {session}\nBot has stopped. Inspect Railway logs; do not assume existing exposure was closed. Restart creates a NEW account."));
    }
    if let Some(notifier) = notifier {
        let stats = notifier.shutdown().await;
        eprintln!("Telegram delivery summary: {stats:?}");
    }
    result
}

async fn run_session(options: &PortfolioOptions, notifier: Option<&TelegramNotifier>, session: Uuid,
    shutdown: &mut (impl Future<Output = std::io::Result<()>> + Unpin),
) -> Result<(), BoxError> {
    let market = PublicPaperMarket::new()?;
    let mut recovery_state = FeedRecovery::default();
    let startup = async {
        let instruments = recovery::public_get("portfolio_metadata", || async {
            try_join_all(options.symbols.iter().map(|symbol| market.instrument(symbol))).await
        }, |notice| recovery_state.observe(&notice, notifier, session)).await?;
        // One spare candle tolerates a one-bar publishing difference at the boundary.
        let (histories, requested_at) = fetch(&market, &instruments,
            options.warmup.saturating_add(1).min(1000),
            |notice| recovery_state.observe(&notice, notifier, session)).await?;
        Ok::<_, BoxError>((instruments, histories, requested_at))
    };
    // Poll the signal listener first, before any network or warm-up work.
    let (instruments, histories, requested_at) = tokio::select! {
        biased;
        result = &mut *shutdown => {
            result?;
            notify(notifier, format!("[PAPER] STOPPED during startup\nSession: {session}\nNo paper orders were opened; no session log was started."));
            return Ok(());
        }
        result = startup => result?,
    };
    let mut batches = settled_batches(&histories, instruments.len(), requested_at);
    if batches.len() < usize::from(options.warmup) {
        return Err("not enough synchronized closed candles for portfolio warmup".into());
    }
    let batches = batches.split_off(batches.len() - usize::from(options.warmup));
    let last_batch = batches.last().ok_or("missing portfolio warmup batch")?;
    for candle in last_batch { check_freshness(candle)?; }
    let mut cursor = last_batch.first().ok_or("empty portfolio warmup batch")?.close_time();
    let header = PortfolioHeader::new(&instruments, options.config, session);
    let mut engine = header.engine()?;
    let mut log = tokio::task::block_in_place(|| PortfolioLog::create(&options.output, &header))?;
    let mut recent = VecDeque::new();
    for batch in batches {
        tokio::select! {
            biased;
            result = &mut *shutdown => {
                result?;
                let at = Utc::now();
                let events = engine.shutdown(at)?;
                let snapshot = engine.snapshot();
                tokio::task::block_in_place(|| log.finish(at, "shutdown_signal", &snapshot, &events))?;
                notify(notifier, status_message(session, "STOPPED during warmup", recent.back().and_then(|batch: &Vec<Candle>| batch.first()).map(Candle::close_time), &snapshot));
                return Ok(());
            }
            () = tokio::task::yield_now() => {}
        }
        let step = engine.warm_up(&batch)?;
        tokio::task::block_in_place(|| log.append(&batch, true, &step))?;
        retain(&mut recent, batch);
    }
    // Startup retries are resolved only after the complete warmup is journaled.
    recovery_state.committed(notifier, session, cursor);
    println!("PAPER PORTFOLIO V3 | max_active=4 | max_per_symbol=2 | total_risk_cap=3% | symbols={} | initial={} USDT | leverage={}x | warmup={} each | candle_delay=10s | session={} | log={}",
        options.symbols.join(","), options.config.initial_balance(), options.config.leverage(), options.warmup, session, options.output.display());
    println!("{}", serde_json::to_string(&engine.snapshot())?);
    notify(notifier, format!("[PAPER V3] STARTED\nSession: {session}\nSymbols: {} / M1\nShared balance: {} USDT\nLeverage: {}x (initial-margin simulation only)\nRisk: <=1% per trade, <=3% combined at admission; daily breaker: 5%; net RR >= 2\nMax 4 active (pending + open), max 2 per symbol. Each setup has its own Entry/TP/SL. No opposite-side hedging on the same symbol.\nSlots do not guarantee available risk or margin.\nFunding/venue liquidation not modeled. Restart does not resume.\nLast closed candle: {cursor}",
        options.symbols.join(", "), options.config.initial_balance(), options.config.leverage()));
    let outcome = collect(&market, &instruments, options, &mut engine, &mut log, &mut recent, &mut cursor,
        notifier, session, shutdown, &mut recovery_state).await;
    let reason = outcome.as_ref().map_or("error", |reason| *reason);
    let at = Utc::now();
    let failed_engine = engine.is_poisoned();
    let events = match engine.shutdown(at) {
        Ok(events) => events,
        Err(error) => {
            tokio::task::block_in_place(|| log.abort(at, "error", &engine.snapshot(), &[]))?;
            return Err(error.into());
        }
    };
    let snapshot = engine.snapshot();
    tokio::task::block_in_place(|| {
        if failed_engine { log.abort(at, "error", &snapshot, &events) }
        else { log.finish(at, reason, &snapshot, &events) }
    })?;
    if failed_engine { eprintln!("Portfolio engine failed; aborted journal is diagnostic-only, not replay-complete."); }
    publish_events(notifier, session, &events, &snapshot, options.config);
    println!("Stopped ({reason}); portfolio open positions are marked, not forcibly closed.");
    println!("{}", serde_json::to_string(&snapshot)?);
    notify(notifier, status_message(session, reason, Some(cursor), &snapshot));
    outcome.map(|_| ())
}

#[allow(clippy::too_many_arguments)]
async fn collect(
    market: &PublicPaperMarket, instruments: &[PaperInstrument], options: &PortfolioOptions,
    engine: &mut PaperPortfolio, log: &mut PortfolioLog, recent: &mut VecDeque<Vec<Candle>>,
    cursor: &mut DateTime<Utc>, notifier: Option<&TelegramNotifier>, session: Uuid,
    shutdown: &mut (impl Future<Output = std::io::Result<()>> + Unpin),
    recovery_state: &mut FeedRecovery,
) -> Result<&'static str, BoxError> {
    let mut poll = tokio::time::interval(Duration::from_secs(options.poll_seconds));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + Duration::from_secs(options.heartbeat_seconds), Duration::from_secs(options.heartbeat_seconds));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let deadline = async {
        if let Some(seconds) = options.seconds { tokio::time::sleep(Duration::from_secs(seconds)).await; }
        else { std::future::pending::<()>().await; }
    };
    tokio::pin!(deadline);
    let mut stale_at = feed_deadline(*cursor)?;
    loop {
        // This absolute cursor deadline also covers heartbeat/poll waits and
        // every retry/backoff. Keep its monotonic value until the cursor really
        // advances: neither a heartbeat nor backward UTC adjustment extends it.
        ensure_joint_feed_current(*cursor, stale_at)?;
        tokio::select! {
            biased;
            result = &mut *shutdown => { result?; return Ok("shutdown_signal"); }
            () = &mut deadline => return Ok("duration_elapsed"),
            () = tokio::time::sleep_until(stale_at) => return Err("synchronized portfolio feed is stale; recovery stopped".into()),
            _ = heartbeat.tick() => {
                ensure_joint_feed_current(*cursor, stale_at)?;
                let message = format!("{}\n{}", heartbeat_message(session, *cursor, &engine.snapshot(), recovery_state), diagnostics_message(engine));
                notify(notifier, message);
                continue;
            }
            _ = poll.tick() => {}
        }
        // Closed M1 bars cannot add a usable batch until the next close plus
        // settlement delay. Avoid repeatedly downloading the same ten bars.
        // Poll/heartbeat/shutdown and the absolute freshness guard stay active.
        if !next_batch_due(*cursor, Utc::now()) { continue; }
        let (rows, requested_at) = tokio::select! {
            biased;
            result = &mut *shutdown => { result?; return Ok("shutdown_signal"); }
            () = &mut deadline => return Ok("duration_elapsed"),
            () = tokio::time::sleep_until(stale_at) => return Err("synchronized portfolio feed is stale; recovery stopped".into()),
            result = fetch(market, instruments, 10,
                |notice| recovery_state.observe(&notice, notifier, session)) => result?,
        };
        // A response becoming ready at the stale boundary must not bypass the
        // timer. Validate the complete recovered round before any engine/log
        // mutation; then preserve the successful attempt's request-start guard.
        ensure_joint_feed_current(*cursor, stale_at)?;
        let batches = validate_round(&rows, instruments.len(), recent, *cursor, requested_at)?;
        let advanced = !batches.is_empty();
        for batch in batches {
            ensure_joint_feed_current(*cursor, stale_at)?;
            let at = batch.first().ok_or("empty synchronized batch")?.close_time();
            let step = engine.process_batch(&batch)?;
            // Journal evidence precedes best-effort external notification.
            tokio::task::block_in_place(|| log.append(&batch, false, &step))?;
            println!("{}", serde_json::to_string(&step)?);
            println!("Paper setup diagnostics: {}", diagnostics_message(engine).replace('\n', " | "));
            publish_events(notifier, session, &step.events, &step.snapshot, options.config);
            *cursor = at;
            stale_at = feed_deadline(*cursor)?;
            retain(recent, batch);
        }
        ensure_joint_feed_current(*cursor, stale_at)?;
        // HTTP success alone cannot announce recovery: a missing, revised or
        // discontinuous candle must fail before this post-commit notification.
        if advanced { recovery_state.committed(notifier, session, *cursor); }
    }
}

fn next_batch_due(cursor: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now.signed_duration_since(cursor)
        >= Timeframe::M1.duration() + chrono::Duration::seconds(SETTLEMENT_DELAY_SECONDS)
}

fn ensure_joint_feed_current(cursor: DateTime<Utc>, stale_at: tokio::time::Instant) -> Result<(), BoxError> {
    ensure_feed_current(cursor)?;
    if tokio::time::Instant::now() >= stale_at {
        return Err("synchronized portfolio feed exceeded its monotonic freshness deadline".into());
    }
    Ok(())
}

async fn fetch(market: &PublicPaperMarket, instruments: &[PaperInstrument], count: u16,
    on_notice: impl FnMut(RecoveryNotice),
) -> Result<(Vec<Vec<Candle>>, DateTime<Utc>), RecoveryError> {
    fetch_round(|requested_at| async move {
        // Retry the entire concurrent round. No successful BTC response from
        // an earlier failed attempt is reused alongside a later ETH response.
        try_join_all(instruments.iter().map(|instrument| market.recent_closed_candles(
            instrument.spec.instrument(), Timeframe::M1, count, requested_at))).await
    }, on_notice).await
}

async fn fetch_round<F, Fut>(mut request: F, on_notice: impl FnMut(RecoveryNotice))
    -> Result<(Vec<Vec<Candle>>, DateTime<Utc>), RecoveryError>
where
    F: FnMut(DateTime<Utc>) -> Fut,
    Fut: Future<Output = Result<Vec<Vec<Candle>>, PaperMarketError>>,
{
    recovery::public_get("portfolio_candles", || {
        let requested_at = Utc::now();
        let round = request(requested_at);
        async move { Ok((round.await?, requested_at)) }
    }, on_notice).await
}

fn validate_round(rows: &[Vec<Candle>], count: usize, recent: &VecDeque<Vec<Candle>>,
    cursor: DateTime<Utc>, requested_at: DateTime<Utc>,
) -> Result<Vec<Vec<Candle>>, BoxError> {
    if rows.len() != count { return Err("incomplete portfolio public-data round".into()); }
    for series in rows {
        check_freshness(series.last().ok_or("missing public candles for portfolio symbol")?)?;
        for candle in series {
            if recent.iter().flatten().any(|prior| prior.instrument() == candle.instrument()
                && prior.open_time() == candle.open_time() && prior != candle) {
                return Err("venue revised a previously closed portfolio candle".into());
            }
        }
    }
    let batches: Vec<_> = settled_batches(rows, count, requested_at).into_iter()
        .filter(|batch| batch.first().is_some_and(|candle| candle.close_time() > cursor)).collect();
    let mut expected_open = cursor;
    for batch in &batches {
        if batch.iter().any(|candle| candle.open_time() != expected_open) {
            return Err("recovered synchronized portfolio candles contain a gap".into());
        }
        expected_open = batch.first().ok_or("empty synchronized batch")?.close_time();
    }
    Ok(batches)
}

#[derive(Debug, Default)]
struct FeedRecovery {
    outage_announced: bool,
    started: Option<tokio::time::Instant>,
    episode_retries: u32,
    total_retries: u64,
    recoveries: u64,
}

impl FeedRecovery {
    fn observe(&mut self, notice: &RecoveryNotice, notifier: Option<&TelegramNotifier>, session: Uuid) {
        recovery::log_notice(notice);
        if let RecoveryNotice::Retrying { operation, next_attempt, max_attempts, delay, error, .. } = notice {
            self.episode_retries = self.episode_retries.saturating_add(1);
            self.total_retries = self.total_retries.saturating_add(1);
            if !self.outage_announced {
                self.outage_announced = true;
                self.started = Some(tokio::time::Instant::now());
                notify(notifier, format!("[PAPER] PUBLIC DATA RETRY (temporary)\nSession: {session}\nOperation: {operation}\nReason: {error}\nNext attempt: {next_attempt}/{max_attempts}; wait {} ms\nNew entry processing is paused until synchronized data is validated and journaled. Same account retained.\nPaper TP/SL uses closed-candle OHLC; no realtime exchange protection. Stale or invalid data stops the session.", delay.as_millis()));
            }
        }
        // Recovered here means only that HTTP parsing completed. Announce
        // resumed data processing only after a complete validated batch commits.
    }

    fn committed(&mut self, notifier: Option<&TelegramNotifier>, session: Uuid, cursor: DateTime<Utc>) {
        if self.outage_announced {
            self.outage_announced = false;
            let elapsed = self.started.take().map_or(0, |at| at.elapsed().as_secs());
            self.recoveries = self.recoveries.saturating_add(1);
            notify(notifier, format!("[PAPER] PUBLIC DATA RECOVERED\nSession: {session}\nRetries: {}; elapsed to validated batch: {elapsed}s\nComplete synchronized data was validated and journaled. Same account retained; data processing resumed subject to entry windows and risk limits.\nLast processed close: {cursor}\nAny catch-up fills are closed-candle OHLC simulation, not realtime protective execution.", self.episode_retries));
            self.episode_retries = 0;
        }
    }
}

fn joint_batches(histories: &[Vec<Candle>], count: usize) -> Vec<Vec<Candle>> {
    let mut batches: BTreeMap<DateTime<Utc>, Vec<Candle>> = BTreeMap::new();
    for series in histories {
        for candle in series { batches.entry(candle.close_time()).or_default().push(candle.clone()); }
    }
    batches.into_values().filter(|batch| batch.len() == count).collect()
}

// Use request-start time, never receipt time: a slow request must not turn an
// early snapshot into a post-delay observation merely by taking longer.
fn settled_batches(histories: &[Vec<Candle>], count: usize, now: DateTime<Utc>) -> Vec<Vec<Candle>> {
    joint_batches(histories, count).into_iter().filter(|batch| {
        batch.iter().all(|candle| now.signed_duration_since(candle.close_time())
            >= chrono::Duration::seconds(SETTLEMENT_DELAY_SECONDS))
    }).collect()
}

fn retain(recent: &mut VecDeque<Vec<Candle>>, batch: Vec<Candle>) {
    recent.push_back(batch);
    while recent.len() > 10 { recent.pop_front(); }
}

fn notify(notifier: Option<&TelegramNotifier>, message: String) {
    if let Some(notifier) = notifier {
        if let Err(error) = notifier.try_send(message) { eprintln!("Telegram notification not queued: {error}"); }
    }
}

fn publish_events(notifier: Option<&TelegramNotifier>, session: Uuid, events: &[PortfolioEvent], snapshot: &PortfolioSnapshot, config: PaperConfig) {
    for event in events {
        notify(notifier, event_message(session, event, snapshot, config));
    }
}

fn order_details(order: &PaperOrderSnapshot, leverage: u32) -> String {
    let side = match order.direction { Direction::Bullish => "LONG", Direction::Bearish => "SHORT" };
    let notional = order.entry_price.checked_mul(order.quantity);
    let margin = notional.and_then(|value| value.checked_div(leverage.into()));
    let amount = |value: Option<rust_decimal::Decimal>| value.map_or_else(|| "unavailable".to_owned(), |value| value.normalize().to_string());
    format!("Side: {side}\nEntry: {}\nTP: {}\nSL: {}\nQuantity: {}\nLeverage: {leverage}x\nEntry notional: {} USDT\nInitial margin estimate: {} USDT (fees additional)\nTrade ID: {}",
        order.entry_price, order.take_profit_price, order.stop_loss_price, order.quantity, amount(notional), amount(margin), order.intent_id)
}

fn event_message(session: Uuid, event: &PortfolioEvent, snapshot: &PortfolioSnapshot, config: PaperConfig) -> String {
    let body = match event {
        PortfolioEvent::EntryPlaced { symbol, at, order } => format!("LIMIT PLACED / NOT FILLED\nSymbol: {symbol}\nObserved: {at}\n{}\nEntry expires: {}", order_details(order, config.leverage()), order.expires_at),
        PortfolioEvent::Execution { symbol, event, order } => {
            let state = match event {
                PaperEvent::EntryFilled { at, price, fee, .. } => format!("ENTRY FILLED (simulated)\nObserved: {at}\nFill price: {price}\nEntry fee: {fee} USDT"),
                PaperEvent::EntryCancelled { at, reason, .. } => format!("ENTRY CANCELLED\nObserved: {at}\nReason: {reason:?}"),
                PaperEvent::PositionClosed { at, exit_price, net_pnl, fee, reason, .. } => format!("POSITION CLOSED / {reason:?}\nObserved: {at}\nExit: {exit_price}\nNet trade PnL: {net_pnl} USDT\nExit fee: {fee} USDT"),
            };
            format!("{state}\nSymbol: {symbol}\n{}", order_details(order, config.leverage()))
        }
        PortfolioEvent::Halted { at } => format!("ENTRIES HALTED (risk or session stop)\nObserved: {at}\nInspect portfolio logs; new entries are blocked."),
    };
    format!("[PAPER] {body}\nSession: {session}\nPost-batch balance/equity: {}/{} USDT\nPost-batch active: {}/4; reserved risk: {} USDT\nPost-batch reserved capital: {} USDT; available cash: {} USDT\nOHLC simulation; not exchange execution.", snapshot.account.balance, snapshot.account.equity, snapshot.account.orders.len(), snapshot.account.reserved_risk, snapshot.account.reserved_capital, snapshot.account.available_cash)
}

fn status_message(session: Uuid, reason: &str, last_close: Option<DateTime<Utc>>, snapshot: &PortfolioSnapshot) -> String {
    let detail = if snapshot.account.orders.is_empty() {
        "Exposure: FLAT".to_owned()
    } else {
        snapshot.account.orders.iter().map(|active| {
            let state = match active.state { BookOrderState::Pending => "PENDING", BookOrderState::Open => "OPEN" };
            let side = match active.order.direction { Direction::Bullish => "LONG", Direction::Bearish => "SHORT" };
            format!("Exposure: {state} {} {side}\nTrade ID: {}\nEntry: {} | TP: {} | SL: {}\nQuantity: {} | Reserved risk: {} USDT\nReserved capital (margin + fees): {} USDT",
                active.symbol, active.order.intent_id, active.order.entry_price, active.order.take_profit_price,
                active.order.stop_loss_price, active.order.quantity, active.reserved_risk, active.reserved_capital)
        }).collect::<Vec<_>>().join("\n\n")
    };
    let btc = snapshot.account.orders.iter().filter(|order| order.symbol == "BTCUSDT").count();
    let eth = snapshot.account.orders.iter().filter(|order| order.symbol == "ETHUSDT").count();
    format!("[PAPER] {reason}\nSession: {session}\nBalance: {} USDT\nEquity: {} USDT\nRealized PnL: {} USDT\nUnrealized PnL: {} USDT\nFees paid: {} USDT\nCompleted trades: {}\nRisk/session halt: {}\nLast processed close: {}\nLeverage: {}x\nActive (pending + open): {}/4; BTC {btc}/2; ETH {eth}/2\nReserved risk: {} USDT (3% cap at admission)\nReserved capital: {} USDT; available cash: {} USDT\n{detail}",
        snapshot.account.balance, snapshot.account.equity, snapshot.account.realized_pnl, snapshot.account.unrealized_pnl, snapshot.account.fees_paid,
        snapshot.account.completed_trades, snapshot.halted, last_close.map_or_else(|| "none".to_owned(), |at| at.to_string()),
        snapshot.account.leverage, snapshot.account.orders.len(), snapshot.account.reserved_risk, snapshot.account.reserved_capital, snapshot.account.available_cash)
}

fn heartbeat_message(session: Uuid, cursor: DateTime<Utc>, snapshot: &PortfolioSnapshot,
    recovery: &FeedRecovery) -> String {
    let window = KillzoneFilter::default().classify(cursor);
    let window_name = match window {
        Some(Killzone::London) => "OPEN (London)",
        Some(Killzone::NewYork) => "OPEN (New York)",
        None => "CLOSED (outside configured entry hours)",
    };
    let feed = if recovery.outage_announced { "RETRYING / awaiting validated batch" }
        else { "CURRENT (last processed batch validated)" };
    let gate = if snapshot.halted { "PAUSED: risk/session halt" }
        else if recovery.outage_announced { "PAUSED: public data recovery" }
        else if window.is_none() { "PAUSED: outside entry window" }
        else { "OPEN: still requires a valid setup, free slots and risk/margin approval" };
    format!("{}\n\nFeed: {feed}\nFeed retries/recoveries this session: {}/{}\nEntry window at last candle: {window_name}\nNew entry timing/data gate: {gate}\nLast candle (Vietnam): {}\nSchedule: weekdays 02:00-05:00 / 07:00-10:00 America/New_York (DST-aware). Existing paper orders continue to be simulated outside entry windows.",
        status_message(session, "HEARTBEAT", Some(cursor), snapshot), recovery.total_retries,
        recovery.recoveries, cursor.with_timezone(&chrono_tz::Asia::Ho_Chi_Minh).format("%Y-%m-%d %H:%M:%S %Z"))
}

fn diagnostics_message(engine: &PaperPortfolio) -> String {
    use ict_trading_bot::strategy::StrategyStage;
    engine.diagnostics().iter().map(|item| {
        let stage = match item.discovery_stage {
            StrategyStage::WaitingForSweep => "waiting for liquidity sweep",
            StrategyStage::WaitingForMss => "sweep seen; waiting for displacement + MSS",
            StrategyStage::WaitingForFvg => "MSS seen; waiting for FVG confirmation",
            StrategyStage::WaitingForRetracement => "FVG armed; waiting for retracement",
            StrategyStage::EntryPending => "limit entry pending",
            StrategyStage::CancellationPending => "entry cancellation pending",
            StrategyStage::InPosition => "position open",
        };
        let discard = item.last_discard.as_ref().map_or_else(String::new, |last|
            format!("; last discarded setup: {:?} at {} (historical)", last.reason, last.at));
        format!("{} discovery: {stage}{discard}", item.symbol)
    }).collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
mod notification_tests {
    use std::cell::{Cell, RefCell};

    use super::*;
    use ict_trading_bot::{
        paper::{BookOrderSnapshot, BookSnapshot, PaperCancelReason, PaperExitReason},
        CandleInput, Exchange, InstrumentId, MarketKind, VolumeUnit,
    };
    use rust_decimal::Decimal;

    fn at(seconds: i64) -> Result<DateTime<Utc>, BoxError> {
        DateTime::from_timestamp(seconds, 0).ok_or_else(|| "invalid fixture time".into())
    }

    fn order() -> Result<PaperOrderSnapshot, BoxError> {
        Ok(PaperOrderSnapshot {
            intent_id: Uuid::from_u128(42),
            direction: Direction::Bullish,
            entry_price: Decimal::new(100, 0),
            quantity: Decimal::new(2, 0),
            stop_loss_price: Decimal::new(95, 0),
            take_profit_price: Decimal::new(110, 0),
            expires_at: at(600)?,
        })
    }

    fn snapshot() -> PortfolioSnapshot {
        PortfolioSnapshot {
            account: BookSnapshot {
                initial_balance: Decimal::new(100, 0),
                balance: Decimal::new(9988, 2),
                equity: Decimal::new(10012, 2),
                realized_pnl: Decimal::new(-12, 2),
                unrealized_pnl: Decimal::new(24, 2),
                fees_paid: Decimal::new(12, 2),
                completed_trades: 0,
                wins: 0,
                losses: 0,
                orders: Vec::new(),
                leverage: 10,
                reserved_risk: Decimal::ZERO,
                reserved_capital: Decimal::ZERO,
                available_cash: Decimal::new(9988, 2),
            },
            halted: false,
        }
    }

    fn active_order(symbol: &str, state: BookOrderState) -> Result<BookOrderSnapshot, BoxError> {
        Ok(BookOrderSnapshot {
            symbol: symbol.to_owned(), state, order: order()?,
            reserved_risk: Decimal::ONE, reserved_capital: Decimal::new(20, 0),
        })
    }

    #[test]
    fn placed_and_filled_alerts_distinguish_lifecycle_and_shared_account() -> Result<(), BoxError> {
        let config = PaperConfig::default().with_leverage(10)?;
        let session = Uuid::from_u128(7);
        let order = order()?;
        let mut snapshot = snapshot();
        snapshot.account.orders.push(active_order("BTCUSDT", BookOrderState::Open)?);
        let placed = event_message(
            session,
            &PortfolioEvent::EntryPlaced {
                symbol: "BTCUSDT".to_owned(),
                at: at(300)?,
                order: order.clone(),
            },
            &snapshot,
            config,
        );
        assert!(placed.contains("LIMIT PLACED / NOT FILLED"));
        assert!(!placed.contains("ENTRY FILLED (simulated)"));
        let filled = event_message(
            session,
            &PortfolioEvent::Execution {
                symbol: "BTCUSDT".to_owned(),
                event: PaperEvent::EntryFilled {
                    intent_id: order.intent_id,
                    at: at(360)?,
                    price: order.entry_price,
                    quantity: order.quantity,
                    fee: Decimal::new(12, 2),
                },
                order: order.clone(),
            },
            &snapshot,
            config,
        );
        for required in [
            "[PAPER] ENTRY FILLED (simulated)",
            "Symbol: BTCUSDT",
            "Side: LONG",
            "Entry: 100",
            "TP: 110",
            "SL: 95",
            "Quantity: 2",
            "Leverage: 10x",
            "Entry notional: 200 USDT",
            "Initial margin estimate: 20 USDT (fees additional)",
            "Post-batch balance/equity: 99.88/100.12 USDT",
            "OHLC simulation; not exchange execution.",
        ] {
            assert!(filled.contains(required), "missing expected notification field: {required}");
        }
        assert!(filled.contains(&format!("Trade ID: {}", order.intent_id)));
        assert!(filled.contains(&format!("Session: {session}")));
        assert!(filled.chars().count() <= 4096);
        Ok(())
    }

    #[test]
    fn take_profit_and_stop_loss_alerts_keep_exit_identity_and_unmultiplied_pnl() -> Result<(), BoxError> {
        let config = PaperConfig::default().with_leverage(10)?;
        let mut order = order()?;
        order.direction = Direction::Bearish;
        order.stop_loss_price = Decimal::new(105, 0);
        order.take_profit_price = Decimal::new(90, 0);
        for (reason, exit_price, net_pnl, label) in [
            (PaperExitReason::TakeProfit, Decimal::new(90, 0), Decimal::new(19772, 3), "TakeProfit"),
            (PaperExitReason::StopLoss, Decimal::new(105_021, 3), Decimal::new(-102_880_252, 7), "StopLoss"),
        ] {
            // The exit alert uses geometry retained in the event even when the
            // shared post-batch account has become flat or acquired a new owner.
            let mut snapshot = snapshot();
            snapshot.account.orders.push(active_order("BTCUSDT", BookOrderState::Open)?);
            let message = event_message(
                Uuid::from_u128(7),
                &PortfolioEvent::Execution {
                    symbol: "ETHUSDT".to_owned(),
                    event: PaperEvent::PositionClosed {
                        intent_id: order.intent_id,
                        at: at(420)?,
                        exit_price,
                        quantity: order.quantity,
                        gross_pnl: Decimal::new(20, 0),
                        net_pnl,
                        fee: Decimal::new(108, 3),
                        reason,
                    },
                    order: order.clone(),
                },
                &snapshot,
                config,
            );
            assert!(message.contains(&format!("POSITION CLOSED / {label}")));
            assert!(message.contains("Symbol: ETHUSDT"));
            assert!(!message.contains("Symbol: BTCUSDT"));
            assert!(message.contains("Side: SHORT"));
            assert!(message.contains("TP: 90\nSL: 105"));
            assert!(message.contains(&format!("Exit: {exit_price}")));
            assert!(message.contains(&format!("Net trade PnL: {net_pnl} USDT")));
            assert!(message.contains("Leverage: 10x"));
            assert!(message.contains("Initial margin estimate: 20 USDT"));
            assert!(message.chars().count() <= 4096);
        }
        Ok(())
    }

    #[test]
    fn cancellation_and_session_halt_do_not_claim_fill_or_risk_breach() -> Result<(), BoxError> {
        let config = PaperConfig::default().with_leverage(10)?;
        let order = order()?;
        let cancelled = event_message(
            Uuid::from_u128(7),
            &PortfolioEvent::Execution {
                symbol: "ETHUSDT".to_owned(),
                event: PaperEvent::EntryCancelled {
                    intent_id: order.intent_id,
                    at: at(420)?,
                    reason: PaperCancelReason::Requested,
                },
                order,
            },
            &snapshot(),
            config,
        );
        assert!(cancelled.contains("ENTRY CANCELLED"));
        assert!(!cancelled.contains("ENTRY FILLED"));
        let halted = event_message(
            Uuid::from_u128(7),
            &PortfolioEvent::Halted { at: at(420)? },
            &snapshot(),
            config,
        );
        assert!(halted.contains("ENTRIES HALTED (risk or session stop)"));
        assert!(!halted.contains("RISK HALTED"));
        Ok(())
    }

    #[test]
    fn status_reports_exact_shared_account_and_retains_open_exposure_on_stop() -> Result<(), BoxError> {
        let mut snapshot = snapshot();
        snapshot.account.orders.push(active_order("ETHUSDT", BookOrderState::Open)?);
        snapshot.halted = true;
        let message = status_message(Uuid::from_u128(7), "duration_elapsed", Some(at(420)?), &snapshot);
        assert!(message.contains("[PAPER] duration_elapsed"));
        assert!(message.contains("Balance: 99.88 USDT\nEquity: 100.12 USDT"));
        assert!(message.contains("Exposure: OPEN ETHUSDT"));
        assert!(message.contains("Leverage: 10x"));
        assert!(!message.contains("Exposure: FLAT"));
        assert!(!message.contains("POSITION CLOSED"));
        snapshot.account.orders[0].state = BookOrderState::Pending;
        let message = status_message(Uuid::from_u128(7), "HEARTBEAT", Some(at(420)?), &snapshot);
        assert!(message.contains("Exposure: PENDING ETHUSDT"));
        snapshot.account.orders.clear();
        let message = status_message(Uuid::from_u128(7), "HEARTBEAT", Some(at(420)?), &snapshot);
        assert!(message.contains("Exposure: FLAT"));
        Ok(())
    }

    #[test]
    fn heartbeat_lists_four_distinct_trades_within_telegram_limit() -> Result<(), BoxError> {
        let mut snapshot = snapshot();
        for (index, symbol) in ["BTCUSDT", "BTCUSDT", "ETHUSDT", "ETHUSDT"].iter().enumerate() {
            let state = if index % 2 == 0 { BookOrderState::Pending } else { BookOrderState::Open };
            let mut active = active_order(symbol, state)?;
            active.order.intent_id = Uuid::from_u128(u128::try_from(index)? + 100);
            // Even maximum decimal display widths keep a four-trade heartbeat
            // under Telegram's limit. This is a formatting fixture, not pricing.
            active.order.entry_price = Decimal::MAX;
            active.order.take_profit_price = Decimal::MAX;
            active.order.stop_loss_price = Decimal::MAX;
            active.order.quantity = Decimal::MAX;
            active.reserved_capital = Decimal::MAX;
            snapshot.account.orders.push(active);
        }
        let recovery = FeedRecovery { total_retries: u64::MAX, recoveries: u64::MAX, ..FeedRecovery::default() };
        let mut message = heartbeat_message(Uuid::from_u128(7), at(420)?, &snapshot, &recovery);
        // Reserve ample space for the bounded two-symbol diagnostics suffix.
        message.push_str(&"x".repeat(512));
        assert!(message.contains("Active (pending + open): 4/4; BTC 2/2; ETH 2/2"));
        assert_eq!(message.matches("Trade ID:").count(), 4);
        for active in &snapshot.account.orders {
            assert!(message.contains(&active.order.intent_id.to_string()));
        }
        assert!(message.contains("Exposure: PENDING BTCUSDT"));
        assert!(message.contains("Exposure: OPEN ETHUSDT"));
        assert!(!message.contains("Exposure: FLAT"));
        assert!(message.chars().count() <= 4096);
        Ok(())
    }

    #[test]
    fn exiting_one_btc_trade_does_not_borrow_the_other_btc_entry() -> Result<(), BoxError> {
        let mut snapshot = snapshot();
        let closed = order()?;
        let mut remaining = active_order("BTCUSDT", BookOrderState::Open)?;
        remaining.order.intent_id = Uuid::from_u128(43);
        remaining.order.entry_price = Decimal::new(200, 0);
        remaining.order.take_profit_price = Decimal::new(220, 0);
        remaining.order.stop_loss_price = Decimal::new(190, 0);
        snapshot.account.orders.push(remaining);
        let event = PortfolioEvent::Execution {
            symbol: "BTCUSDT".to_owned(),
            event: PaperEvent::PositionClosed {
                intent_id: closed.intent_id, at: at(420)?,
                exit_price: closed.take_profit_price, quantity: closed.quantity,
                gross_pnl: Decimal::new(20, 0), net_pnl: Decimal::new(19748, 3),
                fee: Decimal::new(132, 3), reason: PaperExitReason::TakeProfit,
            },
            order: closed,
        };
        let message = event_message(Uuid::from_u128(7), &event, &snapshot, PaperConfig::default().with_leverage(10)?);
        assert!(message.contains("Entry: 100\nTP: 110\nSL: 95"));
        assert!(!message.contains("Entry: 200"));
        assert!(message.contains("Post-batch active: 1/4"));
        let heartbeat = status_message(Uuid::from_u128(7), "HEARTBEAT", Some(at(420)?), &snapshot);
        assert!(heartbeat.contains("Entry: 200 | TP: 220 | SL: 190"));
        assert!(!heartbeat.contains("Entry: 100"));
        Ok(())
    }

    fn candle(symbol: &str, seconds: i64) -> Result<Candle, BoxError> {
        let canonical = match symbol {
            "BTCUSDT" => "BTC-USDT-PERP",
            "ETHUSDT" => "ETH-USDT-PERP",
            _ => return Err("unexpected fixture symbol".into()),
        };
        Ok(Candle::new(CandleInput {
            instrument: InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual, symbol, canonical)?,
            timeframe: Timeframe::M1,
            open_time: at(seconds)?,
            close_time: at(seconds + 60)?,
            open: Decimal::new(100, 0),
            high: Decimal::new(101, 0),
            low: Decimal::new(99, 0),
            close: Decimal::new(100, 0),
            volume: Decimal::ONE,
            volume_unit: VolumeUnit::BaseAsset,
            trade_count: None,
            is_closed: true,
        })?)
    }

    #[test]
    fn joint_batches_only_emit_shared_intervals_in_time_order() -> Result<(), BoxError> {
        let histories = vec![
            vec![candle("ETHUSDT", 60)?, candle("ETHUSDT", 120)?, candle("ETHUSDT", 180)?],
            vec![candle("BTCUSDT", 0)?, candle("BTCUSDT", 60)?, candle("BTCUSDT", 120)?],
        ];
        let batches = joint_batches(&histories, 2);
        assert_eq!(batches.len(), 2);
        for (index, batch) in batches.iter().enumerate() {
            let expected = at(120 + i64::try_from(index)? * 60)?;
            assert_eq!(batch.len(), 2);
            assert!(batch.iter().all(|candle| candle.close_time() == expected));
            assert!(batch.iter().any(|candle| candle.instrument().venue_symbol.as_str() == "BTCUSDT"));
            assert!(batch.iter().any(|candle| candle.instrument().venue_symbol.as_str() == "ETHUSDT"));
        }
        assert!(joint_batches(&[histories[0].clone(), Vec::new()], 2).is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_ready_at_startup_does_not_open_a_session_log() -> Result<(), BoxError> {
        let output = std::env::temp_dir().join(format!("paper-startup-stop-{}.jsonl", Uuid::new_v4()));
        let options = PortfolioOptions {
            symbols: vec!["BTCUSDT".to_owned(), "ETHUSDT".to_owned()],
            config: PaperConfig::default().with_leverage(10)?,
            seconds: Some(1),
            poll_seconds: 5,
            warmup: 50,
            output: output.clone(),
            heartbeat_seconds: 1800,
        };
        let mut shutdown = std::future::ready(Ok(()));
        // Biased shutdown wins before the public-data future is ever polled.
        run_session(&options, None, Uuid::new_v4(), &mut shutdown).await?;
        assert!(!output.exists());
        Ok(())
    }

    #[test]
    fn boundary_candles_wait_for_settlement_without_skipping_prior_batches() -> Result<(), BoxError> {
        let histories = vec![
            vec![candle("BTCUSDT", 0)?, candle("BTCUSDT", 60)?],
            vec![candle("ETHUSDT", 0)?, candle("ETHUSDT", 60)?],
        ];
        assert!(settled_batches(&histories, 2, at(69)?).is_empty());
        assert!(settled_batches(&histories, 2, at(70)? - chrono::Duration::milliseconds(1)).is_empty());
        assert_eq!(settled_batches(&histories, 2, at(70)?).len(), 1);
        assert_eq!(settled_batches(&histories, 2, at(129)?).len(), 1);
        let ready = settled_batches(&histories, 2, at(130)?);
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0][0].close_time(), at(60)?);
        assert_eq!(ready[1][0].close_time(), at(120)?);
        Ok(())
    }

    fn metadata(symbol: &str) -> Result<PaperInstrument, BoxError> {
        Ok(PaperInstrument {
            spec: ict_trading_bot::risk::InstrumentRiskSpec::new(candle(symbol, 0)?.instrument().clone(),
                Decimal::new(1, 1), Decimal::new(1, 3), Decimal::new(1, 3),
                Some(Decimal::from(100)), Decimal::ONE)?,
            min_notional: Decimal::ONE,
        })
    }

    #[tokio::test]
    async fn whole_round_retry_uses_fresh_request_time_and_never_resets_or_updates_account_early() -> Result<(), BoxError> {
        // Use a distinctive balance so silently recreating a default 100-USDT
        // engine would be detectable. This is a pure offline data-round test.
        let config = PaperConfig::new(Decimal::new(12345, 2), Decimal::new(6, 4),
            Decimal::TWO, Decimal::ONE, Decimal::TWO)?.with_leverage(10)?;
        let mut engine = PaperPortfolio::new(vec![metadata("BTCUSDT")?, metadata("ETHUSDT")?], config, Timeframe::M1)?;
        let close = Utc::now().timestamp().div_euclid(60) * 60 - 60;
        let cursor = at(close - 60)?;
        let seed = vec![candle("BTCUSDT", close - 120)?, candle("ETHUSDT", close - 120)?];
        engine.warm_up(&seed)?;
        let before = engine.snapshot();
        let fresh_btc = vec![candle("BTCUSDT", close - 60)?];
        let fresh_eth = vec![candle("ETHUSDT", close - 60)?];
        let old_btc = vec![candle("BTCUSDT", close - 120)?];
        let attempts = Cell::new(0_u32);
        let timestamps = RefCell::new(Vec::new());
        let notices = RefCell::new(Vec::new());
        let (rows, requested_at) = fetch_round(|request_time| {
            let attempt = attempts.get() + 1;
            attempts.set(attempt);
            timestamps.borrow_mut().push(request_time);
            let btc = if attempt == 1 { old_btc.clone() } else { fresh_btc.clone() };
            let eth = if attempt == 1 { Err(PaperMarketError::Transport) } else { Ok(fresh_eth.clone()) };
            async move {
                // A successful first BTC response must be discarded when the
                // other symbol fails, not mixed into the second attempt.
                try_join_all([std::future::ready(Ok(btc)), std::future::ready(eth)]).await
            }
        }, |notice| {
            assert_eq!(engine.snapshot(), before);
            notices.borrow_mut().push(notice);
        }).await?;
        assert_eq!(attempts.get(), 2);
        assert_eq!(rows, vec![fresh_btc, fresh_eth]);
        assert_eq!(engine.snapshot(), before);
        let timestamps = timestamps.into_inner();
        assert_eq!(timestamps.len(), 2);
        assert!(timestamps[1] > timestamps[0]);
        assert_eq!(requested_at, timestamps[1]);
        assert!(matches!(notices.into_inner().as_slice(),
            [RecoveryNotice::Retrying { attempt: 1, .. }, RecoveryNotice::Recovered { attempts: 2, .. }]));
        let recent = VecDeque::from([seed]);
        let batches = validate_round(&rows, 2, &recent, cursor, requested_at)?;
        assert_eq!(batches.len(), 1);
        for batch in batches { engine.process_batch(&batch)?; }
        assert_eq!(engine.snapshot().account.balance, Decimal::new(12345, 2));
        assert!(engine.snapshot().account.orders.is_empty());
        Ok(())
    }

    #[test]
    fn recovered_round_gap_is_rejected_before_any_candidate_batch_is_applied() -> Result<(), BoxError> {
        let close = Utc::now().timestamp().div_euclid(60) * 60 - 60;
        let cursor = at(close - 180)?;
        let rows = vec![
            vec![candle("BTCUSDT", close - 180)?, candle("BTCUSDT", close - 60)?],
            vec![candle("ETHUSDT", close - 180)?, candle("ETHUSDT", close - 60)?],
        ];
        // The first complete pair is valid by itself, but a later missing
        // interval makes the entire round unsuitable for an atomic handoff.
        assert_eq!(settled_batches(&rows, 2, Utc::now()).len(), 2);
        assert!(validate_round(&rows, 2, &VecDeque::new(), cursor, Utc::now()).is_err());
        assert!(validate_round(&rows[..1], 2, &VecDeque::new(), cursor, Utc::now()).is_err());
        Ok(())
    }

    #[test]
    fn http_recovery_does_not_clear_outage_until_validated_batch_commits() {
        let mut state = FeedRecovery::default();
        let session = Uuid::from_u128(7);
        let retry = RecoveryNotice::Retrying {
            operation: "portfolio_candles", attempt: 1, next_attempt: 2, max_attempts: 4,
            delay: Duration::from_secs(1), error: PaperMarketError::Transport,
        };
        state.observe(&retry, None, session);
        assert!(state.outage_announced);
        state.observe(&retry, None, session);
        state.observe(&RecoveryNotice::Recovered { operation: "portfolio_candles", attempts: 2 }, None, session);
        assert!(state.outage_announced, "HTTP success is not validated data recovery");
        assert_eq!(state.total_retries, 2);
        assert_eq!(state.recoveries, 0);
        state.committed(None, session, Utc::now());
        assert!(!state.outage_announced);
        assert_eq!(state.recoveries, 1);
        assert_eq!(state.episode_retries, 0);
        assert!(state.started.is_none());
        state.committed(None, session, Utc::now());
        assert!(!state.outage_announced);
        assert_eq!(state.recoveries, 1);
    }

    #[test]
    fn settled_polling_skips_duplicate_rounds_but_retries_late_bars() -> Result<(), BoxError> {
        let mut cursor = at(0)?;
        let mut requests = Vec::new();
        for second in (10..=190).step_by(5) {
            if next_batch_due(cursor, at(second)?) {
                requests.push(second);
                // Simulate one delayed publication: the 130s request yields
                // no new batch; only a real cursor advance suppresses polling.
                if second != 130 { cursor = at(second / 60 * 60)?; }
            }
        }
        assert_eq!(requests, [70, 130, 135, 190]);
        assert!(!next_batch_due(at(60)?, at(129)?));
        assert!(next_batch_due(at(60)?, at(130)?));
        assert!(!next_batch_due(at(60)?, at(59)?));
        Ok(())
    }

    #[test]
    fn heartbeat_separates_risk_feed_and_entry_window() -> Result<(), BoxError> {
        let outside = DateTime::parse_from_rfc3339("2026-10-29T00:35:00Z")?.with_timezone(&Utc);
        let inside = DateTime::parse_from_rfc3339("2026-10-29T06:30:00Z")?.with_timezone(&Utc);
        let mut snapshot = snapshot();
        let mut recovery = FeedRecovery::default();
        let message = heartbeat_message(Uuid::nil(), outside, &snapshot, &recovery);
        assert!(message.contains("Risk/session halt: false"));
        assert!(message.contains("Entry window at last candle: CLOSED"));
        assert!(message.contains("PAUSED: outside entry window"));
        assert!(message.contains("2026-10-29 07:35:00"));
        assert!(!message.contains("Entries blocked: false"));
        recovery.outage_announced = true;
        let message = heartbeat_message(Uuid::nil(), inside, &snapshot, &recovery);
        assert!(message.contains("RETRYING / awaiting validated batch"));
        assert!(message.contains("PAUSED: public data recovery"));
        recovery.outage_announced = false;
        snapshot.halted = true;
        assert!(heartbeat_message(Uuid::nil(), inside, &snapshot, &recovery).contains("PAUSED: risk/session halt"));
        snapshot.halted = false;
        assert!(heartbeat_message(Uuid::nil(), inside, &snapshot, &recovery).contains("OPEN: still requires a valid setup"));
        Ok(())
    }

    #[test]
    fn heartbeat_entry_windows_follow_dst_weekdays_and_exclusive_end() -> Result<(), BoxError> {
        for (timestamp, expected) in [
            ("2026-10-29T06:00:00Z", "OPEN (London)"),
            ("2026-10-29T09:00:00Z", "CLOSED"),
            ("2026-10-29T11:00:00Z", "OPEN (New York)"),
            ("2026-10-29T14:00:00Z", "CLOSED"),
            ("2026-11-02T06:30:00Z", "CLOSED"),
            ("2026-11-02T07:00:00Z", "OPEN (London)"),
            ("2026-10-31T06:30:00Z", "CLOSED"),
        ] {
            let at = DateTime::parse_from_rfc3339(timestamp)?.with_timezone(&Utc);
            let message = heartbeat_message(Uuid::nil(), at, &snapshot(), &FeedRecovery::default());
            assert!(message.contains(&format!("Entry window at last candle: {expected}")), "{timestamp}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_interrupts_backoff_without_a_second_round() -> Result<(), BoxError> {
        let attempts = Cell::new(0_u32);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let mut shutdown_tx = Some(shutdown_tx);
        {
            let round = fetch_round(|_| {
                attempts.set(attempts.get() + 1);
                std::future::ready(Err(PaperMarketError::Transport))
            }, |notice| {
                if matches!(notice, RecoveryNotice::Retrying { .. }) {
                    if let Some(sender) = shutdown_tx.take() { let _ = sender.send(()); }
                }
            });
            tokio::pin!(round);
            tokio::select! {
                biased;
                result = shutdown_rx => { result?; }
                result = &mut round => { return Err(format!("unexpected completed recovery: {result:?}").into()); }
            }
            assert_eq!(attempts.get(), 1);
        }
        // No retry task is detached when its caller drops the future.
        tokio::task::yield_now().await;
        assert_eq!(attempts.get(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn joint_feed_deadline_interrupts_retry_even_with_heartbeat_ready() -> Result<(), BoxError> {
        let attempts = Cell::new(0_u32);
        let cursor = Utc::now() - chrono::Duration::seconds(120) + chrono::Duration::milliseconds(40);
        let stale_at = feed_deadline(cursor)?;
        let round = fetch_round(|_| {
            attempts.set(attempts.get() + 1);
            std::future::ready(Err(PaperMarketError::Transport))
        }, |_| {});
        tokio::pin!(round);
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(stale_at) => {}
            result = &mut round => { return Err(format!("unexpected completed recovery: {result:?}").into()); }
        }
        assert_eq!(attempts.get(), 1);
        assert!(ensure_feed_current(cursor).is_err());
        // Mirrors the production biased select: a ready heartbeat cannot keep
        // bypassing an already expired joint-feed deadline.
        let stopped = tokio::select! {
            biased;
            () = tokio::time::sleep_until(stale_at) => true,
            () = std::future::ready(()) => false,
        };
        assert!(stopped);
        Ok(())
    }

    #[tokio::test]
    async fn expired_monotonic_guard_rejects_a_response_even_if_utc_still_looks_fresh() -> Result<(), BoxError> {
        let cursor = Utc::now() - chrono::Duration::seconds(30);
        ensure_feed_current(cursor)?;
        let expired = tokio::time::Instant::now();
        assert!(ensure_joint_feed_current(cursor, expired).is_err());
        // A heartbeat or successful HTTP retry cannot replace the expired
        // instant with a fresh deadline for the unchanged cursor.
        let extended = feed_deadline(cursor)?;
        assert!(extended > expired);
        assert!(ensure_joint_feed_current(cursor, expired).is_err());
        Ok(())
    }
}
