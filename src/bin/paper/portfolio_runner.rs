//! Public-data portfolio runner. Telegram is an optional, best-effort observer.

use std::{collections::{BTreeMap, VecDeque}, future::Future, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::future::try_join_all;
use ict_trading_bot::{
    ict_engine::{Killzone, KillzoneFilter},
    paper::{BookOrderState, PaperConfig, PaperEntryWindow, PaperEvent, PaperInstrument, PaperMarketError, PaperOrderSnapshot, MultiPortfolio as PaperPortfolio, PortfolioEvent, MultiPortfolioSnapshot as PortfolioSnapshot, PublicPaperMarket},
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
    let mut stopped_notice_attempted = false;
    let result = run_session(&options, notifier.as_ref(), session, &mut shutdown, &mut stopped_notice_attempted).await;
    if result.is_err() && !stopped_notice_attempted {
        // Error objects are not sent externally: they could include paths or IDs.
        notify(notifier.as_ref(), format!("[PAPER] BOT ĐÃ DỪNG DO LỖI\nCần kiểm tra log Railway. Không xem vị thế cũ là đã đóng.\nKhởi động lại tạo tài khoản paper mới.\nPhiên: {}", short_id(session)));
    }
    if let Some(notifier) = notifier {
        let stats = notifier.shutdown().await;
        eprintln!("Telegram delivery summary: {stats:?}");
    }
    result
}

async fn run_session(options: &PortfolioOptions, notifier: Option<&TelegramNotifier>, session: Uuid,
    shutdown: &mut (impl Future<Output = std::io::Result<()>> + Unpin),
    stopped_notice_attempted: &mut bool,
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
            notify(notifier, format!("[PAPER] ĐÃ DỪNG KHỞI ĐỘNG\nChưa tạo lệnh hoặc bắt đầu nhật ký phiên.\nPhiên: {}", short_id(session)));
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
    let header = PortfolioHeader::new_with_entry_window(&instruments, options.config, session, options.entry_window);
    let mut engine = header.engine()?;
    let mut log = tokio::task::block_in_place(|| PortfolioLog::create(&options.output, &header))?;
    let mut recent = VecDeque::new();
    for batch in batches {
        tokio::select! {
            biased;
            result = &mut *shutdown => {
                result?;
                let at = Utc::now();
                let (snapshot, _) = tokio::task::block_in_place(|| finish_session_log(
                    &mut engine, &mut log, at, "shutdown_signal"))?;
                notify(notifier, status_message(session, "STOPPED during warmup", recent.back().and_then(|batch: &Vec<Candle>| batch.first()).map(Candle::close_time), &snapshot));
                return Ok(());
            }
            () = tokio::task::yield_now() => {}
        }
        if let Err(error) = tokio::task::block_in_place(|| log.process_batch(&mut engine, &batch, true)) {
            tokio::task::block_in_place(|| finish_session_log(&mut engine, &mut log, Utc::now(), "error"))?;
            return Err(error);
        }
        retain(&mut recent, batch);
    }
    // Startup retries are resolved only after the complete warmup is journaled.
    recovery_state.committed(notifier, session, cursor);
    println!("PAPER PORTFOLIO V4 | max_active=4 | max_per_symbol=2 | total_risk_cap=3% | symbols={} | initial={} USDT | leverage={}x | entry_window={} | warmup={} each | candle_delay=10s | session={} | log={}",
        options.symbols.join(","), options.config.initial_balance(), options.config.leverage(), options.entry_window.as_str(), options.warmup, session, options.output.display());
    println!("{}", serde_json::to_string(&engine.snapshot())?);
    notify(notifier, startup_message(session, options, cursor));
    let outcome = collect(&market, &instruments, options, &mut engine, &mut log, &mut recent, &mut cursor,
        notifier, session, shutdown, &mut recovery_state).await;
    let reason = outcome.as_ref().map_or("error", |reason| *reason);
    let at = Utc::now();
    let (snapshot, events) = tokio::task::block_in_place(|| finish_session_log(
        &mut engine, &mut log, at, reason))?;
    publish_events(notifier, session, &events, &snapshot, options.config, false);
    println!("Stopped ({reason}); portfolio open positions are marked, not forcibly closed.");
    println!("{}", serde_json::to_string(&snapshot)?);
    notify(notifier, status_message(session, reason, Some(cursor), &snapshot));
    *stopped_notice_attempted = true;
    outcome.map(|_| ())
}

// A failed append after an engine transition is as non-replayable as a failed
// engine transition. Capacity rejected before processing remains reproducible.
pub(super) fn finish_session_log(engine: &mut PaperPortfolio, log: &mut PortfolioLog,
    at: DateTime<Utc>, reason: &str) -> Result<(PortfolioSnapshot, Vec<PortfolioEvent>), BoxError> {
    let must_abort = engine.is_poisoned() || log.requires_abort();
    let events = match engine.shutdown(at) {
        Ok(events) => events,
        Err(error) => {
            log.abort(at, "error", &engine.snapshot(), &[])?;
            return Err(error.into());
        }
    };
    let snapshot = engine.snapshot();
    if must_abort {
        log.abort(at, "error", &snapshot, &events)?;
        eprintln!("Portfolio transition or journal commit failed; aborted journal is diagnostic-only, not replay-complete.");
    } else {
        log.finish(at, reason, &snapshot, &events)?;
    }
    Ok((snapshot, events))
}

#[allow(clippy::too_many_arguments)]
async fn collect(
    market: &PublicPaperMarket, instruments: &[PaperInstrument], options: &PortfolioOptions,
    engine: &mut PaperPortfolio, log: &mut PortfolioLog, recent: &mut VecDeque<Vec<Candle>>,
    cursor: &mut DateTime<Utc>, notifier: Option<&TelegramNotifier>, session: Uuid,
    shutdown: &mut (impl Future<Output = std::io::Result<()>> + Unpin),
    recovery_state: &mut FeedRecovery,
) -> Result<&'static str, BoxError> {
    let poll_period = Duration::from_secs(options.poll_seconds);
    let mut next_poll = tokio::time::Instant::now();
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
        // Persist the earlier deadline before selecting: a heartbeat winning
        // at readiness must not restore the later cadence on the next loop.
        next_poll = next_poll_at(*cursor, Utc::now(), tokio::time::Instant::now(), next_poll)?;
        tokio::select! {
            biased;
            result = &mut *shutdown => { result?; return Ok("shutdown_signal"); }
            () = &mut deadline => return Ok("duration_elapsed"),
            () = tokio::time::sleep_until(stale_at) => return Err("synchronized portfolio feed is stale; recovery stopped".into()),
            _ = heartbeat.tick() => {
                ensure_joint_feed_current(*cursor, stale_at)?;
                let diagnostics = engine.diagnostics();
                let message = heartbeat_message(session, *cursor, &engine.snapshot(), recovery_state, engine.entry_window(), &diagnostics);
                notify(notifier, message);
                // Technical counters stay in console; Telegram receives one
                // bounded account/position/setup digest instead of three texts.
                for diagnostic in diagnostics {
                    let message = setup_counters_message(session, &diagnostic);
                    println!("{}", message.replace('\n', " | "));
                }
                continue;
            }
            () = tokio::time::sleep_until(next_poll) => {}
        }
        next_poll = tokio::time::Instant::now().checked_add(poll_period)
            .ok_or("portfolio poll deadline overflow")?;
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
            // Capacity is checked before advancing the engine, and durable
            // journal evidence precedes best-effort external notification.
            let step = tokio::task::block_in_place(|| log.process_batch(engine, &batch, false))?;
            // Complete inputs/outcomes remain in the fsynced journal above.
            // Console output summarizes idle bars; lifecycle evidence is still
            // emitted in full when an actual order event occurs.
            println!("{}", compact_batch_log(at, &step, &engine.diagnostics()));
            if !step.events.is_empty() {
                println!("PAPER EVENTS {}", serde_json::to_string(&step.events)?);
            }
            publish_events(notifier, session, &step.events, &step.snapshot, options.config, true);
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

fn compact_batch_log(at: DateTime<Utc>, step: &ict_trading_bot::paper::MultiPortfolioStep,
    diagnostics: &[ict_trading_bot::paper::MultiSymbolDiagnostics],
) -> String {
    let account = &step.snapshot.account;
    let symbols = step.analysis.iter().map(|analysis| {
        let diagnostic = diagnostics.iter().find(|row| row.symbol == analysis.symbol);
        let stage = diagnostic.map_or_else(|| "unknown".to_owned(), |row| format!("{:?}", row.discovery_stage));
        let detail = diagnostic.and_then(|row| row.live.last_admission_failure)
            .filter(|failure| failure.at == at)
            .map_or_else(String::new, |failure| format!(" admission={:?} ({})", failure.kind, admission_failure_detail(failure)));
        let rejected = if analysis.rejection_reasons.is_empty() { String::new() }
            else { format!(" {:?}", analysis.rejection_reasons) };
        format!("{} signals={} intents={} rejected={}{rejected} stage={stage}{detail}",
            analysis.symbol, analysis.signals, analysis.intents, analysis.rejected)
    }).collect::<Vec<_>>().join(" | ");
    format!("PAPER M1 close={} balance={} equity={} active={}/4 risk={} halted={} | {symbols}",
        at.format("%Y-%m-%dT%H:%M:%SZ"), account.balance.normalize(), account.equity.normalize(),
        account.orders.len(), account.reserved_risk.normalize(), step.snapshot.halted)
}

fn next_batch_due(cursor: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now.signed_duration_since(cursor)
        >= Timeframe::M1.duration() + chrono::Duration::seconds(SETTLEMENT_DELAY_SECONDS)
}

// Wake at candle readiness even when the configured polling cadence would skip
// over it. For example, startup at cursor + 65s with a 60s cadence must fetch at
// +70s, not wait until +125s and trip the unchanged +120s freshness deadline.
// Once a batch is already due, respect the cadence for an empty/late response;
// using an expired readiness instant here would create a busy request loop.
fn next_poll_at(cursor: DateTime<Utc>, now: DateTime<Utc>, monotonic_now: tokio::time::Instant,
    next_cadence: tokio::time::Instant,
) -> Result<tokio::time::Instant, BoxError> {
    let until_ready = Timeframe::M1.duration() + chrono::Duration::seconds(SETTLEMENT_DELAY_SECONDS)
        - now.signed_duration_since(cursor);
    if until_ready <= chrono::Duration::zero() { return Ok(next_cadence); }
    let ready_at = monotonic_now.checked_add(until_ready.to_std()?)
        .ok_or("portfolio candle readiness deadline overflow")?;
    Ok(next_cadence.min(ready_at))
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
        if let RecoveryNotice::Retrying { next_attempt, max_attempts, delay, error, .. } = notice {
            self.episode_retries = self.episode_retries.saturating_add(1);
            self.total_retries = self.total_retries.saturating_add(1);
            if !self.outage_announced {
                self.outage_announced = true;
                self.started = Some(tokio::time::Instant::now());
                notify(notifier, retry_message(session, error, *next_attempt, *max_attempts, *delay));
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
            notify(notifier, format!("[PAPER] DỮ LIỆU ĐÃ PHỤC HỒI\nNến mới đã kiểm tra: {}\nĐã thử lại {} lần trong {elapsed} giây; giữ nguyên vốn và lệnh.\nBot tiếp tục xét setup theo giới hạn rủi ro. TP/SL paper được mô phỏng theo nến đóng, không theo tick.\nPhiên: {}", vietnam_time(cursor), self.episode_retries, short_id(session)));
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

fn publish_events(notifier: Option<&TelegramNotifier>, session: Uuid, events: &[PortfolioEvent], snapshot: &PortfolioSnapshot, config: PaperConfig, include_halt: bool) {
    for event in events {
        if !include_halt && matches!(event, PortfolioEvent::Halted { .. }) { continue; }
        notify(notifier, event_message(session, event, snapshot, config));
    }
}

// Display-only suffix; journal and console retain complete UUIDs for identity.
fn short_id(id: Uuid) -> String { id.simple().to_string().chars().skip(20).collect() }

fn vietnam_time(at: DateTime<Utc>) -> String {
    at.with_timezone(&chrono_tz::Asia::Ho_Chi_Minh).format("%d/%m/%Y %H:%M:%S VN").to_string()
}

fn startup_message(session: Uuid, options: &PortfolioOptions, cursor: DateTime<Utc>) -> String {
    format!("[PAPER] ĐÃ KHỞI ĐỘNG\n{} · M1 · x{}\nVốn mới: {} USDT | Lịch: {}\nTối đa 4 lệnh, 2/cặp; rủi ro 1%/lệnh, tổng 3% khi nhận lệnh; ngắt lỗ ngày 5%.\nChỉ đặt lệnh khi đủ setup và lời/lỗ sau chi phí đạt 2:1.\nNến đã xử lý: {}\nĐây là mô phỏng, không đặt lệnh thật. Khởi động lại tạo phiên mới; chưa mô phỏng funding/thanh lý.\nPhiên: {}",
        options.symbols.join(" + "), options.config.leverage(), options.config.initial_balance(),
        entry_schedule(options.entry_window), vietnam_time(cursor), short_id(session))
}

fn retry_message(session: Uuid, error: &PaperMarketError, next_attempt: u8, max_attempts: u8, delay: Duration) -> String {
    let cause = match error {
        PaperMarketError::Venue(10006) | PaperMarketError::HttpStatus(429)
        | PaperMarketError::HttpRetryAfter { status: 429, .. } => "sàn đang giới hạn tần suất truy cập".to_owned(),
        PaperMarketError::Timeout => "sàn phản hồi quá chậm".to_owned(),
        PaperMarketError::Transport | PaperMarketError::Connection => "kết nối tới sàn bị gián đoạn".to_owned(),
        PaperMarketError::ResponseBody | PaperMarketError::MalformedResponse { .. } => "phản hồi từ sàn chưa đọc được đầy đủ".to_owned(),
        PaperMarketError::Venue(code) => format!("sàn tạm báo lỗi {code}"),
        PaperMarketError::HttpStatus(status) | PaperMarketError::HttpRetryAfter { status, .. }
        | PaperMarketError::InvalidRetryAfter(status) => format!("dịch vụ dữ liệu báo HTTP {status}"),
        _ => "dữ liệu chưa đạt kiểm tra an toàn".to_owned(),
    };
    let seconds = delay.as_secs().saturating_add(u64::from(delay.subsec_nanos() > 0));
    format!("[PAPER] TẠM CHỜ DỮ LIỆU\nNguyên nhân: {cause}.\nTự thử lại lần {next_attempt}/{max_attempts} sau {seconds} giây. Chưa cần thao tác.\nGiữ nguyên vốn/lệnh; tạm ngừng xử lý nến. TP/SL paper không chạy theo tick trong lúc mất dữ liệu.\nNếu không phục hồi đúng hạn, bot sẽ dừng và báo lại.\nPhiên: {}", short_id(session))
}

fn order_details(order: &PaperOrderSnapshot, leverage: u32) -> String {
    let side = match order.direction { Direction::Bullish => "LONG", Direction::Bearish => "SHORT" };
    let notional = order.entry_price.checked_mul(order.quantity);
    let margin = notional.and_then(|value| value.checked_div(leverage.into()));
    let amount = |value: Option<rust_decimal::Decimal>| value.map_or_else(|| "chưa xác định".to_owned(), |value| value.normalize().to_string());
    format!("{side} · x{leverage} · Mã lệnh: {}\nEntry: {} | TP: {} | SL: {}\nKhối lượng: {} | Ký quỹ ước tính: {} USDT + phí",
        short_id(order.intent_id), order.entry_price, order.take_profit_price, order.stop_loss_price, order.quantity, amount(margin))
}

fn event_message(session: Uuid, event: &PortfolioEvent, snapshot: &PortfolioSnapshot, config: PaperConfig) -> String {
    let body = match event {
        PortfolioEvent::EntryPlaced { symbol, at, order } => format!("ĐẶT LIMIT — CHƯA KHỚP\n{symbol} | {}\n{}\nHết hạn chờ: {}", vietnam_time(*at), order_details(order, config.leverage()), vietnam_time(order.expires_at)),
        PortfolioEvent::Execution { symbol, event, order } => {
            let state = match event {
                PaperEvent::EntryFilled { at, price, fee, .. } => format!("ĐÃ KHỚP LỆNH MÔ PHỎNG\n{symbol} | {}\nGiá khớp: {price} | Phí vào: {fee} USDT", vietnam_time(*at)),
                PaperEvent::EntryCancelled { at, reason, .. } => {
                    let reason = match reason {
                        ict_trading_bot::paper::PaperCancelReason::Expired => "hết thời gian chờ khớp",
                        ict_trading_bot::paper::PaperCancelReason::Requested => "strategy hoặc phiên yêu cầu hủy",
                    };
                    format!("ĐÃ HỦY LIMIT CHƯA KHỚP\n{symbol} | {}\nLý do: {reason}", vietnam_time(*at))
                }
                PaperEvent::PositionClosed { at, exit_price, net_pnl, reason, .. } => {
                    let reason = match reason {
                        ict_trading_bot::paper::PaperExitReason::TakeProfit => "ĐÃ CHỐT TP",
                        ict_trading_bot::paper::PaperExitReason::StopLoss => "ĐÃ CẮT SL",
                    };
                    format!("{reason}\n{symbol} | {}\nGiá thoát: {exit_price}\nLãi/lỗ ròng lệnh: {net_pnl} USDT (đã trừ phí)", vietnam_time(*at))
                }
            };
            format!("{state}\n{}", order_details(order, config.leverage()))
        }
        PortfolioEvent::Halted { at } => format!("TẠM NGỪNG LỆNH MỚI\n{}\nGiới hạn rủi ro hoặc trạng thái phiên đang chặn vào lệnh. Vị thế đang mở chưa được xem là đã đóng.", vietnam_time(*at)),
    };
    format!("[PAPER] {body}\nSau nến: số dư {} | tổng tài sản {} USDT\nĐang chờ/mở: {}/4 | Vốn khả dụng: {} USDT\nPhiên: {} · Mô phỏng theo nến đóng, không phải khớp lệnh trên sàn.",
        snapshot.account.balance, snapshot.account.equity, snapshot.account.orders.len(), snapshot.account.available_cash, short_id(session))
}

fn status_message(session: Uuid, reason: &str, last_close: Option<DateTime<Utc>>, snapshot: &PortfolioSnapshot) -> String {
    let detail = if snapshot.account.orders.is_empty() {
        "Chưa có lệnh chờ hoặc vị thế mở.".to_owned()
    } else {
        snapshot.account.orders.iter().map(|active| {
            let state = match active.state { BookOrderState::Pending => "CHỜ KHỚP", BookOrderState::Open => "ĐANG MỞ" };
            let side = match active.order.direction { Direction::Bullish => "LONG", Direction::Bearish => "SHORT" };
            format!("{state} {} {side} · Mã lệnh: {}\nEntry: {} | TP: {} | SL: {}\nKL: {} | Rủi ro giữ chỗ: {} | Vốn giữ chỗ: {} USDT",
                active.symbol, short_id(active.order.intent_id), active.order.entry_price, active.order.take_profit_price,
                active.order.stop_loss_price, active.order.quantity, active.reserved_risk, active.reserved_capital)
        }).collect::<Vec<_>>().join("\n\n")
    };
    let btc = snapshot.account.orders.iter().filter(|order| order.symbol == "BTCUSDT").count();
    let eth = snapshot.account.orders.iter().filter(|order| order.symbol == "ETHUSDT").count();
    let label = match reason {
        "HEARTBEAT" => "CẬP NHẬT",
        "duration_elapsed" => "ĐÃ DỪNG — HẾT THỜI GIAN CHẠY THỬ",
        "shutdown_signal" => "ĐÃ DỪNG THEO YÊU CẦU",
        "STOPPED during warmup" => "ĐÃ DỪNG KHI NẠP DỮ LIỆU",
        _ => "ĐÃ DỪNG DO LỖI — KIỂM TRA RAILWAY",
    };
    let stop_note = if reason == "HEARTBEAT" { "" } else { "\nPending đã hủy; vị thế còn mở chỉ được ghi nhận, không tự đóng. Khởi động lại tạo phiên mới." };
    format!("[PAPER] {label}\nNến: {}\nSố dư: {} | Tổng tài sản: {} USDT\nLãi/lỗ đã ghi nhận (sau phí): {} | Đang mở: {} | Phí: {} USDT\nĐã đóng: {} lệnh (thắng {}/thua {})\nĐòn bẩy: x{} | Lệnh chờ + mở: {}/4 (BTC {btc}/2, ETH {eth}/2)\nRủi ro giữ chỗ: {} USDT | Vốn khả dụng: {} USDT\n\n{detail}{stop_note}\nPhiên: {}",
        last_close.map_or_else(|| "chưa có".to_owned(), vietnam_time),
        snapshot.account.balance, snapshot.account.equity, snapshot.account.realized_pnl, snapshot.account.unrealized_pnl, snapshot.account.fees_paid,
        snapshot.account.completed_trades, snapshot.account.wins, snapshot.account.losses,
        snapshot.account.leverage, snapshot.account.orders.len(), snapshot.account.reserved_risk, snapshot.account.available_cash, short_id(session))
}

fn heartbeat_message(session: Uuid, cursor: DateTime<Utc>, snapshot: &PortfolioSnapshot,
    recovery: &FeedRecovery, entry_window: PaperEntryWindow, diagnostics: &[ict_trading_bot::paper::MultiSymbolDiagnostics]) -> String {
    let window = KillzoneFilter::default().classify(cursor);
    let window_name = match (entry_window, window) {
        (PaperEntryWindow::Always, _) => "24/7",
        (_, Some(Killzone::London)) => "đang trong phiên London",
        (_, Some(Killzone::NewYork)) => "đang trong phiên New York",
        (_, None) => "ngoài giờ vào lệnh",
    };
    let feed = if recovery.outage_announced { "đang thử kết nối lại" } else { "nến mới đã kiểm tra" };
    let gate = if snapshot.halted { "tạm dừng do rủi ro/trạng thái phiên" }
        else if recovery.outage_announced { "tạm chờ dữ liệu" }
        else if !entry_window.allows(cursor) { "tạm chờ đúng phiên" }
        else { "sẵn sàng khi đủ setup và vốn/rủi ro" };
    let setups = diagnostics.iter().map(setup_digest).collect::<Vec<_>>().join("\n");
    format!("{}\n\nDữ liệu: {feed} | Lịch: {window_name}\nLệnh mới: {gate}\nKết nối: {} lần thử lại / {} lần phục hồi\n\n{setups}\nSố liệu setup tính từ đầu phiên, bỏ warmup. Tín hiệu rời rạc chưa đồng nghĩa có lệnh.\nPAPER: mô phỏng theo nến đóng; TP/SL không phải lệnh thật trên sàn.",
        status_message(session, "HEARTBEAT", Some(cursor), snapshot), recovery.total_retries,
        recovery.recoveries)
}

fn entry_schedule(entry_window: PaperEntryWindow) -> &'static str {
    match entry_window {
        PaperEntryWindow::Always => "24/7, cả cuối tuần",
        PaperEntryWindow::Killzones => "02–05h và 07–10h New York, thứ Hai–Sáu (có DST)",
    }
}

fn setup_digest(item: &ict_trading_bot::paper::MultiSymbolDiagnostics) -> String {
    use ict_trading_bot::strategy::StrategyStage;
    let stage = match item.discovery_stage {
        StrategyStage::WaitingForSweep => "chờ quét thanh khoản",
        StrategyStage::WaitingForMss => "đã có sweep; chờ MSS + nến đẩy mạnh",
        StrategyStage::WaitingForFvg => "đã có MSS; chờ FVG",
        StrategyStage::WaitingForRetracement => "đã có FVG; chờ giá hồi về entry",
        StrategyStage::EntryPending => "limit đang chờ khớp",
        StrategyStage::CancellationPending => "đang chờ xác nhận hủy",
        StrategyStage::InPosition => "đang có vị thế",
    };
    let stats = &item.live;
    let rejection = if stats.admission_failures.is_empty() {
        dominant_reason(&stats.rejection_reasons, rejection_label)
    } else { dominant_reason(&stats.admission_failures, admission_failure_label) };
    let rejection_scope = if stats.admission_failures.is_empty() { "Lý do từ chối nhiều nhất" }
        else { "Lý do không nhận intent nhiều nhất" };
    let discard = dominant_reason(&stats.discards, discard_label);
    let last_failure = stats.last_admission_failure.map_or_else(String::new, |failure| {
        format!("\n  Lần từ chối gần nhất {}: {}", vietnam_time(failure.at), admission_failure_detail(failure))
    });
    format!("{}: {stage}\n  Setup đủ chuỗi: {} | Limit nhận: {} | Từ chối: {}\n  Tín hiệu: sweep {} / MSS {} / FVG {}\n  {rejection_scope}: {rejection}\n  Setup bị loại nhiều nhất: {discard}{last_failure}",
        item.symbol, stats.intents, stats.admitted, stats.rejected, stats.liquidity_sweeps,
        stats.market_structure_shifts, stats.fair_value_gaps)
}

fn admission_failure_label(kind: ict_trading_bot::paper::AdmissionFailureKind) -> &'static str {
    use ict_trading_bot::paper::AdmissionFailureKind as K;
    match kind {
        K::GrossRiskReward => "RR trước phí không đạt",
        K::NetRiskReward => "RR sau chi phí không đạt",
        K::InvalidPriceGeometry => "Entry/SL/TP sau làm tròn không hợp lệ",
        K::QuantityBelowMinimum => "khối lượng tính theo risk dưới mức tối thiểu",
        K::IntentExpired => "setup đã hết hạn",
        K::CircuitBreaker => "đã chạm ngưỡng lỗ ngày",
        K::PortfolioRisk => "hết hạn mức rủi ro chung",
        K::GlobalSlots => "đã đủ 4 lệnh", K::SymbolSlots => "cặp đã đủ 2 lệnh",
        K::OpposingDirection => "đã có lệnh ngược chiều",
        K::MinimumOrder => "không đạt giá trị/khối lượng lệnh tối thiểu",
        K::InsufficientBalance => "thiếu vốn ký quỹ và phí",
        K::BrokerBusy => "bộ mô phỏng đang bận",
        K::NoOpposingLiquidity => "thiếu thanh khoản đối diện để đặt TP",
        K::DuplicateSetup => "setup đã được xét",
        K::SessionHalted => "phiên đang chặn lệnh mới",
        K::SmtRequired => "SMT BTC–ETH chưa đủ hoặc đã mất hiệu lực",
        K::M5Alignment => "hướng M5 chưa đồng thuận",
    }
}

fn admission_failure_detail(failure: ict_trading_bot::paper::AdmissionFailureDiagnostic) -> String {
    if let (Some(actual), Some(required)) = (failure.actual_gross_rr, failure.required_gross_rr) {
        // Keep exact values: rounding 1.999 to 2.00 would misrepresent why the
        // comparison failed. This observer never changes risk arithmetic.
        return format!("RR trước phí {}:1; cần ≥{}:1", actual.normalize(), required.normalize());
    }
    if let (Some(calculated), Some(minimum)) = (failure.calculated_quantity, failure.minimum_quantity) {
        return format!("KL {}; tối thiểu {}", calculated.normalize(), minimum.normalize());
    }
    admission_failure_label(failure.kind).to_owned()
}

fn dominant_reason<R: Copy>(counts: &[ict_trading_bot::paper::DiagnosticCount<R>], label: impl Fn(R) -> &'static str) -> String {
    counts.iter().filter(|item| item.count > 0).max_by_key(|item| item.count)
        .map_or_else(|| "chưa có".to_owned(), |item| format!("{} ({})", label(item.reason), item.count))
}

fn rejection_label(reason: ict_trading_bot::paper::MultiRejection) -> &'static str {
    use ict_trading_bot::paper::MultiRejection as R;
    match reason {
        R::Warmup => "đang nạp nến lịch sử", R::Halted => "đang chặn rủi ro mới",
        R::DuplicateSetup => "setup đã được xét", R::GlobalSlots => "đã đủ 4 lệnh",
        R::SymbolSlots => "cặp đã đủ 2 lệnh", R::OpposingDirection => "đã có lệnh ngược chiều",
        R::PortfolioRisk => "đã hết hạn mức rủi ro chung", R::NoOpposingLiquidity => "thiếu mục tiêu thanh khoản để đặt TP",
        R::RiskRule => "chưa đạt điều kiện rủi ro/TP", R::MinimumOrder => "nhỏ hơn khối lượng tối thiểu",
        R::InsufficientBalance => "thiếu vốn ký quỹ và phí", R::NetRiskReward => "lời/lỗ sau chi phí dưới 2:1",
        R::StrategyInput => "dữ liệu setup không nhất quán",
        R::SmtRequired => "SMT BTC–ETH chưa đủ hoặc đã mất hiệu lực",
        R::M5Alignment => "hướng M5 chưa đồng thuận",
    }
}

fn discard_label(reason: ict_trading_bot::strategy::SetupDiscardReason) -> &'static str {
    use ict_trading_bot::strategy::SetupDiscardReason as R;
    match reason {
        R::TimeToLiveExpired => "setup hết thời gian chờ", R::MissingNextCandleFvg => "không có FVG ngay sau MSS",
        R::AmbiguousNextCandleFvg => "FVG xác nhận không rõ ràng", R::FarBoundaryViolated => "giá đóng vượt biên FVG",
        R::EntryTouchedOutsideKillzone => "chạm entry ngoài giờ cho phép", R::SeriesReset => "chuỗi nến bị gián đoạn",
        R::ConfirmationOutsideKillzone => "xác nhận FVG ngoài giờ cho phép",
        R::SweepExtremeViolated => "giá phá cực trị sweep trước khi đặt lệnh",
        R::FairValueGapInvalidated => "FVG đã mất hiệu lực", R::SupersededByNewSweep => "có sweep mới thay setup cũ",
        R::AmbiguousInput => "tín hiệu mâu thuẫn", R::InvalidSetup => "setup không hợp lệ",
    }
}

fn setup_counters_message(session: Uuid, diagnostic: &ict_trading_bot::paper::MultiSymbolDiagnostics) -> String {
    let stats = &diagnostic.live;
    let first = stats.first_close.map_or_else(|| "none yet".to_owned(), |at| at.to_string());
    let rejections = diagnostic_counts(&stats.rejection_reasons);
    let discards = diagnostic_counts(&stats.discards);
    format!("[PAPER] SETUP COUNTERS / {}\nSession: {session}\nLive candles: {}\nFirst live close: {first}\nRaw detections: sweeps={}; MSS={}; new FVG={}; displacement bars={}\nRaw detections may belong to different setups; they are not confirmed entry chains.\nEntry intents: {}\nLimits placed: {} (not necessarily filled)\nRejection events: {} (including strategy input rejections)\nRejection reasons: {rejections}\nDiscarded setups: {discards}\nScope: live candles only; warmup excluded. Cumulative within this session; resets on restart.",
        diagnostic.symbol, stats.candles, stats.liquidity_sweeps, stats.market_structure_shifts,
        stats.fair_value_gaps, stats.displacement_bars, stats.intents, stats.admitted, stats.rejected)
}

fn diagnostic_counts<R: std::fmt::Debug>(counts: &[ict_trading_bot::paper::DiagnosticCount<R>]) -> String {
    if counts.is_empty() { return "none".to_owned(); }
    counts.iter().map(|item| format!("{:?}={}", item.reason, item.count)).collect::<Vec<_>>().join(", ")
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
        assert!(placed.contains("ĐẶT LIMIT — CHƯA KHỚP"));
        assert!(!placed.contains("ĐÃ KHỚP LỆNH MÔ PHỎNG"));
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
            "[PAPER] ĐÃ KHỚP LỆNH MÔ PHỎNG",
            "BTCUSDT |",
            "LONG · x10",
            "Entry: 100",
            "TP: 110",
            "SL: 95",
            "Khối lượng: 2",
            "x10",
            "Entry: 100 | TP: 110 | SL: 95",
            "Ký quỹ ước tính: 20 USDT + phí",
            "Sau nến: số dư 99.88 | tổng tài sản 100.12 USDT",
            "Mô phỏng theo nến đóng, không phải khớp lệnh trên sàn.",
        ] {
            assert!(filled.contains(required), "missing expected notification field: {required}");
        }
        assert!(filled.contains(&format!("Mã lệnh: {}", short_id(order.intent_id))));
        assert!(filled.contains(&format!("Phiên: {}", short_id(session))));
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
            (PaperExitReason::TakeProfit, Decimal::new(90, 0), Decimal::new(19772, 3), "ĐÃ CHỐT TP"),
            (PaperExitReason::StopLoss, Decimal::new(105_021, 3), Decimal::new(-102_880_252, 7), "ĐÃ CẮT SL"),
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
            assert!(message.contains(label));
            assert!(message.contains("ETHUSDT |"));
            assert!(!message.contains("BTCUSDT |"));
            assert!(message.contains("SHORT · x10"));
            assert!(message.contains("TP: 90 | SL: 105"));
            assert!(message.contains(&format!("Giá thoát: {exit_price}")));
            assert!(message.contains(&format!("Lãi/lỗ ròng lệnh: {net_pnl} USDT")));
            assert!(message.contains("x10"));
            assert!(message.contains("Ký quỹ ước tính: 20 USDT"));
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
        assert!(cancelled.contains("ĐÃ HỦY LIMIT CHƯA KHỚP"));
        assert!(!cancelled.contains("ĐÃ KHỚP LỆNH MÔ PHỎNG"));
        let halted = event_message(
            Uuid::from_u128(7),
            &PortfolioEvent::Halted { at: at(420)? },
            &snapshot(),
            config,
        );
        assert!(halted.contains("TẠM NGỪNG LỆNH MỚI"));
        assert!(!halted.contains("ĐÃ CẮT SL"));
        Ok(())
    }

    #[test]
    fn status_reports_exact_shared_account_and_retains_open_exposure_on_stop() -> Result<(), BoxError> {
        let mut snapshot = snapshot();
        snapshot.account.orders.push(active_order("ETHUSDT", BookOrderState::Open)?);
        snapshot.halted = true;
        let message = status_message(Uuid::from_u128(7), "duration_elapsed", Some(at(420)?), &snapshot);
        assert!(message.contains("[PAPER] ĐÃ DỪNG — HẾT THỜI GIAN CHẠY THỬ"));
        assert!(message.contains("Số dư: 99.88 | Tổng tài sản: 100.12 USDT"));
        assert!(message.contains("ĐANG MỞ ETHUSDT"));
        assert!(message.contains("x10"));
        assert!(!message.contains("Chưa có lệnh chờ hoặc vị thế mở."));
        assert!(message.contains("vị thế còn mở chỉ được ghi nhận, không tự đóng"));
        snapshot.account.orders[0].state = BookOrderState::Pending;
        let message = status_message(Uuid::from_u128(7), "HEARTBEAT", Some(at(420)?), &snapshot);
        assert!(message.contains("CHỜ KHỚP ETHUSDT"));
        snapshot.account.orders.clear();
        let message = status_message(Uuid::from_u128(7), "HEARTBEAT", Some(at(420)?), &snapshot);
        assert!(message.contains("Chưa có lệnh chờ hoặc vị thế mở."));
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
        for value in [&mut snapshot.account.balance, &mut snapshot.account.equity,
            &mut snapshot.account.realized_pnl, &mut snapshot.account.unrealized_pnl,
            &mut snapshot.account.fees_paid, &mut snapshot.account.reserved_risk,
            &mut snapshot.account.available_cash] {
            *value = Decimal::MIN;
        }
        snapshot.account.completed_trades = u64::MAX;
        snapshot.account.wins = u64::MAX;
        snapshot.account.losses = u64::MAX;
        let rows = [max_diagnostic("BTCUSDT")?, max_diagnostic("ETHUSDT")?];
        let message = heartbeat_message(Uuid::from_u128(7), at(420)?, &snapshot, &recovery, PaperEntryWindow::Killzones, &rows);
        assert!(message.contains("Lệnh chờ + mở: 4/4 (BTC 2/2, ETH 2/2)"));
        assert_eq!(message.matches("Mã lệnh:").count(), 4);
        for active in &snapshot.account.orders {
            assert!(message.contains(&short_id(active.order.intent_id)));
        }
        assert!(message.contains("CHỜ KHỚP BTCUSDT"));
        assert!(message.contains("ĐANG MỞ ETHUSDT"));
        assert!(!message.contains("Chưa có lệnh chờ hoặc vị thế mở."));
        assert!(message.chars().count() <= 4096);
        assert_eq!(message.matches("[PAPER]").count(), 1);
        assert!(message.contains("thiếu mục tiêu thanh khoản để đặt TP"));
        assert!(!message.contains("NoOpposingLiquidity"));
        Ok(())
    }

    fn max_diagnostic(symbol: &str) -> Result<ict_trading_bot::paper::MultiSymbolDiagnostics, BoxError> {
        use ict_trading_bot::paper::{DiagnosticCount, LiveSetupCounters, MultiRejection, MultiSymbolDiagnostics};
        use ict_trading_bot::strategy::{SetupDiscardReason, StrategyStage};
        Ok(MultiSymbolDiagnostics {
            symbol: symbol.to_owned(), last_close: Some(at(420)?),
            discovery_stage: StrategyStage::WaitingForRetracement, last_discard: None,
            live: LiveSetupCounters {
                first_close: Some(at(120)?), candles: u64::MAX, liquidity_sweeps: u64::MAX,
                market_structure_shifts: u64::MAX, fair_value_gaps: u64::MAX, displacement_bars: u64::MAX,
                intents: u64::MAX, admitted: u64::MAX, rejected: u64::MAX,
                rejection_reasons: vec![DiagnosticCount { reason: MultiRejection::NoOpposingLiquidity, count: u64::MAX }],
                discards: vec![DiagnosticCount { reason: SetupDiscardReason::EntryTouchedOutsideKillzone, count: u64::MAX }],
                admission_failures: Vec::new(),
                last_admission_failure: Some(ict_trading_bot::paper::AdmissionFailureDiagnostic {
                    at: at(419)?, intent_id: Uuid::nil(), kind: ict_trading_bot::paper::AdmissionFailureKind::GrossRiskReward,
                    target_price: Some(Decimal::MAX), actual_gross_rr: Some(Decimal::MAX), required_gross_rr: Some(Decimal::MAX),
                    calculated_quantity: None, minimum_quantity: None,
                }),
            },
        })
    }

    #[test]
    fn compact_setup_digest_explains_actual_rejections_without_claiming_fills() -> Result<(), BoxError> {
        let mut row = max_diagnostic("BTCUSDT")?;
        row.live.intents = 1;
        row.live.admitted = 0;
        row.live.rejected = 1;
        row.live.rejection_reasons[0].count = 1;
        row.live.discards.clear();
        let message = setup_digest(&row);
        assert!(message.contains("BTCUSDT: đã có FVG; chờ giá hồi về entry"));
        assert!(message.contains("Setup đủ chuỗi: 1 | Limit nhận: 0 | Từ chối: 1"));
        assert!(message.contains("thiếu mục tiêu thanh khoản để đặt TP (1)"));
        assert!(message.contains("Setup bị loại nhiều nhất: chưa có"));
        assert!(!message.contains("NoOpposingLiquidity"));
        assert!(!message.contains("ĐÃ KHỚP"));
        Ok(())
    }

    #[test]
    fn admission_diagnostic_preserves_exact_rr_and_marks_historical_time() -> Result<(), BoxError> {
        use ict_trading_bot::paper::{AdmissionFailureKind, DiagnosticCount};
        let mut row = max_diagnostic("BTCUSDT")?;
        let failure = row.live.last_admission_failure.as_mut().ok_or("missing fixture failure")?;
        failure.actual_gross_rr = Some(Decimal::new(19999, 4));
        failure.required_gross_rr = Some(Decimal::TWO);
        row.live.admission_failures = vec![DiagnosticCount { reason: AdmissionFailureKind::GrossRiskReward, count: 3 }];
        row.live.rejection_reasons = vec![DiagnosticCount { reason: ict_trading_bot::paper::MultiRejection::StrategyInput, count: 100 }];
        let message = setup_digest(&row);
        assert!(message.contains("Lý do không nhận intent nhiều nhất: RR trước phí không đạt (3)"));
        assert!(message.contains("RR trước phí không đạt (3)"));
        assert!(message.contains("RR trước phí 1.9999:1; cần ≥2:1"));
        assert!(message.contains(&format!("Lần từ chối gần nhất {}", vietnam_time(at(419)?))));
        assert!(!message.contains("RR sau chi phí 1.9999"));
        let failure = row.live.last_admission_failure.as_mut().ok_or("missing fixture failure")?;
        failure.kind = AdmissionFailureKind::NetRiskReward;
        failure.actual_gross_rr = None;
        failure.required_gross_rr = None;
        assert_eq!(admission_failure_detail(*failure), "RR sau chi phí không đạt");
        Ok(())
    }

    #[test]
    fn retry_notice_is_actionable_sanitized_and_uses_short_session_reference() {
        let session = Uuid::from_u128(7);
        let message = retry_message(session, &PaperMarketError::Venue(10006), 2, 4, Duration::from_millis(1250));
        assert!(message.contains("[PAPER] TẠM CHỜ DỮ LIỆU"));
        assert!(message.contains("sàn đang giới hạn tần suất truy cập"));
        assert!(message.contains("lần 2/4 sau 2 giây. Chưa cần thao tác"));
        assert!(message.contains("Giữ nguyên vốn/lệnh"));
        assert!(message.contains("TP/SL paper không chạy theo tick"));
        assert!(message.contains(&short_id(session)));
        assert!(!message.contains(&session.to_string()));
        assert!(message.chars().count() < 700);
        assert!(retry_message(session, &PaperMarketError::Timeout, 2, 4, Duration::from_secs(1)).contains("sàn phản hồi quá chậm"));
    }

    #[test]
    fn startup_notice_gives_policy_and_warns_that_restart_is_a_new_account() -> Result<(), BoxError> {
        let super::super::Command::Portfolio(options) = super::super::parse_args(VecDeque::from(["portfolio".to_owned()]))? else {
            return Err("expected portfolio fixture".into());
        };
        let cursor = DateTime::parse_from_rfc3339("2026-10-29T03:14:00Z")?.with_timezone(&Utc);
        let message = startup_message(Uuid::from_u128(7), &options, cursor);
        for field in ["[PAPER] ĐÃ KHỞI ĐỘNG", "BTCUSDT + ETHUSDT · M1 · x10",
            "Vốn mới: 100 USDT", "Lịch: 24/7, cả cuối tuần", "tổng 3%", "ngắt lỗ ngày 5%",
            "không đặt lệnh thật", "Khởi động lại tạo phiên mới", "29/10/2026 10:14:00 VN"] {
            assert!(message.contains(field), "missing {field}");
        }
        assert!(message.chars().count() < 750);
        Ok(())
    }

    #[test]
    fn setup_summary_distinguishes_raw_signals_intents_and_admissions() -> Result<(), BoxError> {
        use ict_trading_bot::paper::{DiagnosticCount, LiveSetupCounters, MultiRejection, MultiSymbolDiagnostics};
        use ict_trading_bot::strategy::{SetupDiscardReason, StrategyStage};
        let mut row = MultiSymbolDiagnostics {
            symbol: "BTCUSDT".to_owned(), last_close: Some(at(420)?),
            discovery_stage: StrategyStage::WaitingForMss, last_discard: None,
            live: LiveSetupCounters::default(),
        };
        let empty = setup_counters_message(Uuid::from_u128(7), &row);
        assert!(empty.contains("Live candles: 0\nFirst live close: none yet"));
        assert!(empty.contains("Rejection reasons: none\nDiscarded setups: none"));
        row.live = LiveSetupCounters {
            first_close: Some(at(120)?), candles: 6, liquidity_sweeps: 4,
            market_structure_shifts: 2, fair_value_gaps: 9, displacement_bars: 3,
            intents: 2, admitted: 1, rejected: 1,
            rejection_reasons: vec![DiagnosticCount { reason: MultiRejection::NetRiskReward, count: 1 }],
            discards: vec![DiagnosticCount { reason: SetupDiscardReason::TimeToLiveExpired, count: 2 }],
            admission_failures: Vec::new(), last_admission_failure: None,
        };
        let message = setup_counters_message(Uuid::from_u128(7), &row);
        for required in ["[PAPER] SETUP COUNTERS / BTCUSDT", "Live candles: 6",
            "Raw detections: sweeps=4; MSS=2; new FVG=9; displacement bars=3",
            "not confirmed entry chains", "Entry intents: 2", "Limits placed: 1 (not necessarily filled)",
            "Rejection reasons: NetRiskReward=1", "Discarded setups: TimeToLiveExpired=2",
            "warmup excluded", "resets on restart"] {
            assert!(message.contains(required), "missing {required}");
        }
        assert!(!message.contains("ENTRY FILLED"));
        Ok(())
    }

    #[test]
    fn every_counter_reason_at_max_width_fits_one_symbol_message() -> Result<(), BoxError> {
        use ict_trading_bot::paper::{DiagnosticCount, LiveSetupCounters, MultiRejection, MultiSymbolDiagnostics};
        use ict_trading_bot::strategy::{SetupDiscardReason, StrategyStage};
        let rejections = [MultiRejection::Warmup, MultiRejection::Halted, MultiRejection::DuplicateSetup,
            MultiRejection::GlobalSlots, MultiRejection::SymbolSlots, MultiRejection::OpposingDirection,
            MultiRejection::PortfolioRisk, MultiRejection::NoOpposingLiquidity, MultiRejection::RiskRule,
            MultiRejection::MinimumOrder, MultiRejection::InsufficientBalance, MultiRejection::NetRiskReward,
            MultiRejection::StrategyInput];
        let discards = [SetupDiscardReason::TimeToLiveExpired, SetupDiscardReason::MissingNextCandleFvg,
            SetupDiscardReason::AmbiguousNextCandleFvg, SetupDiscardReason::FarBoundaryViolated,
            SetupDiscardReason::EntryTouchedOutsideKillzone, SetupDiscardReason::SeriesReset,
            SetupDiscardReason::ConfirmationOutsideKillzone, SetupDiscardReason::SweepExtremeViolated,
            SetupDiscardReason::FairValueGapInvalidated, SetupDiscardReason::SupersededByNewSweep,
            SetupDiscardReason::AmbiguousInput, SetupDiscardReason::InvalidSetup];
        for symbol in ["BTCUSDT", "ETHUSDT"] {
            let row = MultiSymbolDiagnostics {
                symbol: symbol.to_owned(), last_close: Some(at(420)?),
                discovery_stage: StrategyStage::WaitingForSweep, last_discard: None,
                live: LiveSetupCounters {
                    first_close: Some(at(120)?), candles: u64::MAX, liquidity_sweeps: u64::MAX,
                    market_structure_shifts: u64::MAX, fair_value_gaps: u64::MAX, displacement_bars: u64::MAX,
                    intents: u64::MAX, admitted: u64::MAX, rejected: u64::MAX,
                    rejection_reasons: rejections.iter().map(|reason| DiagnosticCount { reason: *reason, count: u64::MAX }).collect(),
                    discards: discards.iter().map(|reason| DiagnosticCount { reason: *reason, count: u64::MAX }).collect(),
                    admission_failures: Vec::new(), last_admission_failure: None,
                },
            };
            let message = setup_counters_message(Uuid::from_u128(7), &row);
            assert!(message.chars().count() <= 4096);
            assert!(message.contains(&format!("/ {symbol}")));
            for reason in &rejections {
                assert!(message.contains(&format!("{reason:?}={}", u64::MAX)));
                assert!(!rejection_label(*reason).is_empty());
                assert_ne!(rejection_label(*reason), format!("{reason:?}"));
            }
            for reason in &discards {
                assert!(message.contains(&format!("{reason:?}={}", u64::MAX)));
                assert!(!discard_label(*reason).is_empty());
                assert_ne!(discard_label(*reason), format!("{reason:?}"));
            }
        }
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
        assert!(message.contains("Entry: 100 | TP: 110 | SL: 95"));
        assert!(!message.contains("Entry: 200"));
        assert!(message.contains("Đang chờ/mở: 1/4"));
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
            entry_window: PaperEntryWindow::Always,
        };
        let mut shutdown = std::future::ready(Ok(()));
        let mut stopped_notice_attempted = false;
        // Biased shutdown wins before the public-data future is ever polled.
        run_session(&options, None, Uuid::new_v4(), &mut shutdown, &mut stopped_notice_attempted).await?;
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
        // Keep the mock's newest bar settled but safely inside freshness even
        // when the real retry delay crosses a UTC minute boundary.
        let close = (Utc::now().timestamp() - SETTLEMENT_DELAY_SECONDS).div_euclid(60) * 60;
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
    fn polling_reaches_ready_candles_for_every_allowed_cadence_and_start_phase() -> Result<(), BoxError> {
        // Model the runner's actual wait/advance loop with a deterministic pair
        // of clocks. The last settled startup candle can be 10..69 seconds old.
        // Every supported CLI cadence must reach readiness before +120s stale.
        let base = tokio::time::Instant::now();
        for period in 1..=60_u64 {
            for startup_age in 10..70_u64 {
                let mut now = base + Duration::from_secs(startup_age);
                let mut next_cadence = now;
                let mut cursor = at(0)?;
                let mut requests = Vec::new();
                while requests.len() < 3 {
                    let elapsed = now.duration_since(base).as_secs();
                    let utc = at(i64::try_from(elapsed)?)?;
                    let wake = next_poll_at(cursor, utc, now, next_cadence)?;
                    now = wake;
                    next_cadence = now + Duration::from_secs(period);
                    let utc = at(i64::try_from(now.duration_since(base).as_secs())?)?;
                    assert!(super::super::remaining_feed_budget(cursor, utc).is_ok());
                    if next_batch_due(cursor, utc) {
                        requests.push(utc.timestamp());
                        cursor = at(utc.timestamp() / 60 * 60)?;
                    }
                }
                assert_eq!(requests, [70, 130, 190], "period={period} phase={startup_age}");
            }
        }
        Ok(())
    }

    #[test]
    fn ready_but_empty_round_keeps_retry_cadence_without_spinning() -> Result<(), BoxError> {
        let base = tokio::time::Instant::now();
        let cursor = at(0)?;
        let next_cadence = base + Duration::from_secs(5);
        for elapsed in 0..5_u64 {
            // A successful HTTP round at +70s had no new complete batch.
            // Heartbeats between +70 and +75 must not bypass its retry cadence.
            let now = base + Duration::from_secs(elapsed);
            assert_eq!(next_poll_at(cursor, at(70 + i64::try_from(elapsed)?)?, now,
                next_cadence)?, next_cadence);
        }
        // The schedule does not manufacture a fresh cursor or extend the feed
        // budget: the existing absolute stale guard still rejects +120s.
        assert!(super::super::remaining_feed_budget(cursor, at(120)?).is_err());
        Ok(())
    }

    #[test]
    fn heartbeat_at_candle_readiness_does_not_restore_the_later_poll_deadline() -> Result<(), BoxError> {
        let base = tokio::time::Instant::now();
        let cursor = at(0)?;
        let mut next_poll = base + Duration::from_secs(125);
        next_poll = next_poll_at(cursor, at(65)?, base + Duration::from_secs(65), next_poll)?;
        assert_eq!(next_poll, base + Duration::from_secs(70));
        // The biased select services a simultaneously-ready heartbeat first.
        // Re-entering the loop retains readiness, even a second after it was due.
        for age in [70_u64, 71] {
            let now = base + Duration::from_secs(age);
            next_poll = next_poll_at(cursor, at(i64::try_from(age)?)?, now, next_poll)?;
            assert!(next_poll <= now);
            assert_eq!(next_poll, base + Duration::from_secs(70));
            assert!(next_batch_due(cursor, at(i64::try_from(age)?)?));
        }
        Ok(())
    }

    #[test]
    fn heartbeat_separates_risk_feed_and_entry_window() -> Result<(), BoxError> {
        let outside = DateTime::parse_from_rfc3339("2026-10-29T00:35:00Z")?.with_timezone(&Utc);
        let inside = DateTime::parse_from_rfc3339("2026-10-29T06:30:00Z")?.with_timezone(&Utc);
        let mut snapshot = snapshot();
        let mut recovery = FeedRecovery::default();
        let message = heartbeat_message(Uuid::nil(), outside, &snapshot, &recovery, PaperEntryWindow::Killzones, &[]);
        assert!(message.contains("Lệnh mới: tạm chờ đúng phiên"));
        assert!(message.contains("Lịch: ngoài giờ vào lệnh"));
        assert!(message.contains("tạm chờ đúng phiên"));
        assert!(message.contains("29/10/2026 07:35:00 VN"));
        assert!(!message.contains("Entries blocked: false"));
        recovery.outage_announced = true;
        let message = heartbeat_message(Uuid::nil(), inside, &snapshot, &recovery, PaperEntryWindow::Killzones, &[]);
        assert!(message.contains("Dữ liệu: đang thử kết nối lại"));
        assert!(message.contains("Lệnh mới: tạm chờ dữ liệu"));
        recovery.outage_announced = false;
        snapshot.halted = true;
        assert!(heartbeat_message(Uuid::nil(), inside, &snapshot, &recovery, PaperEntryWindow::Killzones, &[]).contains("Lệnh mới: tạm dừng do rủi ro/trạng thái phiên"));
        snapshot.halted = false;
        assert!(heartbeat_message(Uuid::nil(), inside, &snapshot, &recovery, PaperEntryWindow::Killzones, &[]).contains("Lệnh mới: sẵn sàng khi đủ setup"));
        Ok(())
    }

    #[test]
    fn heartbeat_entry_windows_follow_dst_weekdays_and_exclusive_end() -> Result<(), BoxError> {
        for (timestamp, expected) in [
            ("2026-10-29T06:00:00Z", "đang trong phiên London"),
            ("2026-10-29T09:00:00Z", "ngoài giờ vào lệnh"),
            ("2026-10-29T11:00:00Z", "đang trong phiên New York"),
            ("2026-10-29T14:00:00Z", "ngoài giờ vào lệnh"),
            ("2026-11-02T06:30:00Z", "ngoài giờ vào lệnh"),
            ("2026-11-02T07:00:00Z", "đang trong phiên London"),
            ("2026-10-31T06:30:00Z", "ngoài giờ vào lệnh"),
        ] {
            let at = DateTime::parse_from_rfc3339(timestamp)?.with_timezone(&Utc);
            let message = heartbeat_message(Uuid::nil(), at, &snapshot(), &FeedRecovery::default(), PaperEntryWindow::Killzones, &[]);
            assert!(message.contains(&format!("Lịch: {expected}")), "{timestamp}");
        }
        Ok(())
    }

    #[test]
    fn always_entry_heartbeat_opens_nights_and_weekends_but_preserves_safety_gates() -> Result<(), BoxError> {
        for timestamp in ["2026-10-29T01:00:00Z", "2026-10-31T08:00:00Z", "2026-11-01T08:00:00Z"] {
            let at = DateTime::parse_from_rfc3339(timestamp)?.with_timezone(&Utc);
            let mut snapshot = snapshot();
            let mut recovery = FeedRecovery::default();
            let message = heartbeat_message(Uuid::nil(), at, &snapshot, &recovery, PaperEntryWindow::Always, &[]);
            assert!(message.contains("Lịch: 24/7"));
            assert!(message.contains("Lệnh mới: sẵn sàng khi đủ setup"));
            assert!(!message.contains("tạm chờ đúng phiên"));
            assert!(message.contains("Lịch: 24/7"));
            snapshot.halted = true;
            assert!(heartbeat_message(Uuid::nil(), at, &snapshot, &recovery, PaperEntryWindow::Always, &[])
                .contains("Lệnh mới: tạm dừng do rủi ro/trạng thái phiên"));
            snapshot.halted = false;
            recovery.outage_announced = true;
            assert!(heartbeat_message(Uuid::nil(), at, &snapshot, &recovery, PaperEntryWindow::Always, &[])
                .contains("Lệnh mới: tạm chờ dữ liệu"));
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

#[cfg(test)]
mod compact_console_tests {
    use super::*;
    use ict_trading_bot::{
        paper::{BookSnapshot, LiveSetupCounters, MultiPortfolioStep, MultiRejection,
            MultiSymbolAnalysis, MultiSymbolDiagnostics, SetupDiscardDiagnostic},
        strategy::{SetupDiscardReason, StrategyStage},
    };
    use rust_decimal::Decimal;

    fn fixture() -> Result<(DateTime<Utc>, MultiPortfolioStep, Vec<MultiSymbolDiagnostics>), BoxError> {
        let at = DateTime::parse_from_rfc3339("2026-10-29T02:33:00Z")?.with_timezone(&Utc);
        let step = MultiPortfolioStep {
            snapshot: PortfolioSnapshot {
                account: BookSnapshot {
                    initial_balance: Decimal::from(100), balance: Decimal::from(100),
                    equity: Decimal::from(100), realized_pnl: Decimal::ZERO,
                    unrealized_pnl: Decimal::ZERO, fees_paid: Decimal::ZERO,
                    completed_trades: 0, wins: 0, losses: 0, leverage: 10,
                    orders: Vec::new(), reserved_risk: Decimal::ZERO,
                    reserved_capital: Decimal::ZERO, available_cash: Decimal::from(100),
                },
                halted: false,
            },
            events: Vec::new(),
            analysis: vec![
                MultiSymbolAnalysis { symbol: "BTCUSDT".to_owned(), signals: 1, intents: 1,
                    rejected: 1, rejection_reasons: vec![MultiRejection::NoOpposingLiquidity] },
                MultiSymbolAnalysis { symbol: "ETHUSDT".to_owned(), signals: 0, intents: 0,
                    rejected: 0, rejection_reasons: Vec::new() },
            ],
        };
        let diagnostics = vec![
            MultiSymbolDiagnostics {
                symbol: "BTCUSDT".to_owned(), last_close: Some(at),
                discovery_stage: StrategyStage::WaitingForSweep,
                last_discard: Some(SetupDiscardDiagnostic { at,
                    reason: SetupDiscardReason::TimeToLiveExpired }),
                live: LiveSetupCounters::default(),
            },
            MultiSymbolDiagnostics {
                symbol: "ETHUSDT".to_owned(), last_close: Some(at),
                discovery_stage: StrategyStage::WaitingForMss, last_discard: None,
                live: LiveSetupCounters::default(),
            },
        ];
        Ok((at, step, diagnostics))
    }

    #[test]
    fn concise_console_keeps_actual_rejection_account_and_current_stages() -> Result<(), BoxError> {
        let (at, step, diagnostics) = fixture()?;
        let before = serde_json::to_value(&step)?;
        let line = compact_batch_log(at, &step, &diagnostics);
        for expected in ["PAPER M1 close=2026-10-29T02:33:00Z", "balance=100 equity=100 active=0/4 risk=0 halted=false",
            "BTCUSDT signals=1 intents=1 rejected=1 [NoOpposingLiquidity] stage=WaitingForSweep",
            "ETHUSDT signals=0 intents=0 rejected=0 stage=WaitingForMss"] {
            assert!(line.contains(expected), "missing {expected}");
        }
        assert!(!line.contains('\n'));
        assert!(!line.contains("TimeToLiveExpired"), "historical discards must not repeat every minute");
        assert_eq!(serde_json::to_value(&step)?, before);
        assert!(compact_batch_log(at, &step, &[]).contains("stage=unknown"));
        Ok(())
    }

    #[test]
    fn compact_console_reduces_serialized_no_event_output() -> Result<(), BoxError> {
        let (at, mut step, diagnostics) = fixture()?;
        for analysis in &mut step.analysis {
            analysis.intents = 0;
            analysis.rejected = 0;
            analysis.rejection_reasons.clear();
        }
        let old_json = serde_json::to_string(&step)?;
        let compact = compact_batch_log(at, &step, &diagnostics);
        // Conservative comparison: the old logger additionally repeated two
        // long diagnostics lines, which are not included in this baseline.
        assert!(compact.len() < old_json.len());
        println!("console fixture bytes: prior_step_json={} compact={}", old_json.len(), compact.len());
        Ok(())
    }
}
