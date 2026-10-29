//! Credential-free, foreground ICT paper runner. Never submits exchange orders.

#[path = "paper/session.rs"]
mod session;
#[path = "paper/portfolio_session.rs"]
// Keep the complete v2 codec for historical replay and regression fixtures.
// New foreground sessions use v3; some legacy writer APIs are test-only users.
#[allow(dead_code)]
mod portfolio_session;
#[path = "paper/multi_session.rs"]
mod multi_session;
#[path = "paper/portfolio_runner.rs"]
mod portfolio_runner;
#[path = "paper/recovery.rs"]
mod recovery;

use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    path::PathBuf,
    process::ExitCode,
    time::Duration,
};

use chrono::{DateTime, Utc};
use ict_trading_bot::{
    models::{Candle, Timeframe},
    paper::{PaperConfig, PaperEngine, PaperEntryWindow, PaperSnapshot, PublicPaperMarket},
};
use rust_decimal::Decimal;
use session::{BoxError, Header, SessionLog};
use uuid::Uuid;

const HELP: &str = "Local paper trading ONLY; no API keys, deposits, or real orders.

paper live [--symbol ETHUSDT] [--balance 100] [--seconds N]
           [--poll-seconds 5] [--warmup 500] [--fee-bps 6]
           [--stop-slippage-bps 2] [--output paper-runs/NAME.jsonl]
paper replay --input paper-runs/NAME.jsonl [--output paper-runs/REPLAY.jsonl]
paper portfolio [--symbols BTCUSDT,ETHUSDT] [--leverage 10] [--balance 100]
                [--entry-window always|killzones]
                [--seconds N] [--poll-seconds 5] [--warmup 500]
                [--fee-bps 6] [--stop-slippage-bps 2] [--heartbeat-seconds 1800]
                [--output paper-runs/NAME.jsonl]

Both modes use public Bybit USDT perpetual M1 data; no exchange credentials.
Legacy live is single-symbol 1x, entry notional+fee <= cash.
Portfolio v4 shares ONE account across BTC/ETH; max 4 active, max 2 per symbol.
New portfolio sessions default to entry-window always (24/7, including weekends).
Use --entry-window killzones for weekday New York session filtering.
Portfolio --warmup accepts 50..=999, reserving one REST bar for settlement.
Only the time gate changes; a complete ICT setup and risk approval are required.
Pending entries and open positions both occupy slots; no opposite-side hedging.
Combined planned risk <= 3% at admission; each trade <= 1% (not 4 full-risk trades).
Portfolio leverage changes initial margin only, not the risk budget or PnL.
Every launch starts a NEW account; no automatic resume. Ctrl+C stops it.
Default risk: 1%/trade, 5% daily drawdown circuit breaker, net RR >= 2.
Fees/slippage are simulation assumptions, not your exchange fee tier.
Warmup only initializes signals; it cannot spend initial paper cash.
Closed-bar full fills are conservative, not order-book/partial-fill simulation.
Funding/maintenance margin/liquidation are not modeled.
Transient public GET failures retry at most 4 attempts within 60 seconds.
No account reset or trading during retry. Stale/gapped/revised data still stops.
Portfolio Telegram alerts are opt-in: TELEGRAM_ENABLED=true plus runtime
TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID. Never pass secrets as CLI arguments.
Replay is entirely offline and checks recorded outcomes against the same algorithm.
Logs are create-new only. Keep the foreground process running to collect data.";

#[derive(Debug)]
struct LiveOptions {
    symbol: String,
    config: PaperConfig,
    seconds: Option<u64>,
    poll_seconds: u64,
    warmup: u16,
    output: PathBuf,
}

#[derive(Debug)]
struct PortfolioOptions {
    symbols: Vec<String>,
    config: PaperConfig,
    seconds: Option<u64>,
    poll_seconds: u64,
    warmup: u16,
    output: PathBuf,
    heartbeat_seconds: u64,
    entry_window: PaperEntryWindow,
}

#[derive(Debug)]
enum Command {
    Help,
    Live(LiveOptions),
    Portfolio(PortfolioOptions),
    Replay { input: PathBuf, output: PathBuf },
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Paper stopped: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), BoxError> {
    match parse_args(std::env::args().skip(1).collect())? {
        Command::Help => println!("{HELP}"),
        Command::Live(options) => live(options).await?,
        Command::Portfolio(options) => portfolio_runner::live(options).await?,
        Command::Replay { input, output } => {
            if multi_session::is_multi(&input)? {
                let snapshot = tokio::task::block_in_place(|| multi_session::replay(&input, &output))?;
                println!("{}", serde_json::to_string(&snapshot)?);
            } else if portfolio_session::is_portfolio(&input)? {
                let snapshot = tokio::task::block_in_place(|| portfolio_session::replay(&input, &output))?;
                println!("{}", serde_json::to_string(&snapshot)?);
            } else {
                let snapshot = tokio::task::block_in_place(|| session::replay(&input, &output))?;
                report(&snapshot)?;
            }
            println!("Replay verified; derived log: {}", output.display());
        }
    }
    Ok(())
}

fn parse_args(mut args: VecDeque<String>) -> Result<Command, BoxError> {
    let Some(mode) = args.pop_front() else {
        return Ok(Command::Help);
    };
    if matches!(mode.as_str(), "--help" | "-h") && args.is_empty() {
        return Ok(Command::Help);
    }
    if !matches!(mode.as_str(), "live" | "portfolio" | "replay") {
        return Err("expected live, portfolio, replay, or --help".into());
    }
    let mut flags = BTreeMap::new();
    while let Some(key) = args.pop_front() {
        if !key.starts_with("--") {
            return Err("expected a named --option".into());
        }
        let value = args.pop_front().ok_or("missing option value")?;
        if value.starts_with("--") || flags.insert(key, value).is_some() {
            return Err("missing value or duplicate option".into());
        }
    }
    let output = flags
        .remove("--output")
        .map_or_else(|| output_path(&mode), PathBuf::from);
    let command = if mode == "replay" {
        Command::Replay {
            input: PathBuf::from(flags.remove("--input").ok_or("replay requires --input")?),
            output,
        }
    } else {
        let symbol = if mode == "portfolio" { "ETHUSDT".to_owned() } else {
            flags.remove("--symbol").unwrap_or_else(|| "ETHUSDT".to_owned())
        };
        if symbol.len() > 32
            || !symbol.ends_with("USDT")
            || symbol.len() <= 4
            || !symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            return Err("symbol must be an uppercase USDT perpetual symbol".into());
        }
        let balance = option_decimal(&mut flags, "--balance", "100")?;
        let fee_bps = option_decimal(&mut flags, "--fee-bps", "6")?;
        let slippage = option_decimal(&mut flags, "--stop-slippage-bps", "2")?;
        let fee_rate = fee_bps
            .checked_div(Decimal::new(10_000, 0))
            .ok_or("fee overflow")?;
        if fee_rate.checked_mul(Decimal::new(10_000, 0)) != Some(fee_bps) {
            return Err("fee basis-point conversion would lose decimal precision".into());
        }
        let config = PaperConfig::new(balance, fee_rate, slippage, Decimal::ONE, Decimal::TWO)?;
        let seconds = flags
            .remove("--seconds")
            .map(|value| value.parse::<u64>())
            .transpose()?;
        if seconds.is_some_and(|value| value == 0 || value > 604_800) {
            return Err("--seconds must be in 1..=604800".into());
        }
        let poll_seconds = flags
            .remove("--poll-seconds")
            .unwrap_or_else(|| "5".into())
            .parse()?;
        if !(1..=60).contains(&poll_seconds) {
            return Err("--poll-seconds must be in 1..=60".into());
        }
        let warmup = flags
            .remove("--warmup")
            .unwrap_or_else(|| "500".into())
            .parse()?;
        if !(50..=1000).contains(&warmup) {
            return Err("--warmup must be in 50..=1000".into());
        }
        if mode == "portfolio" {
            if warmup > 999 {
                return Err("portfolio --warmup must be in 50..=999; one REST bar is reserved for settlement".into());
            }
            let (symbols, leverage, heartbeat_seconds) = portfolio_parameters(&mut flags)?;
            let entry_window = match flags.remove("--entry-window").as_deref().unwrap_or("always") {
                "always" => PaperEntryWindow::Always,
                "killzones" => PaperEntryWindow::Killzones,
                _ => return Err("--entry-window must be always or killzones".into()),
            };
            Command::Portfolio(PortfolioOptions {
                symbols, config: config.with_leverage(leverage)?, seconds,
                poll_seconds, warmup, output, heartbeat_seconds, entry_window,
            })
        } else { Command::Live(LiveOptions {
            symbol,
            config,
            seconds,
            poll_seconds,
            warmup,
            output,
        }) }
    };
    if !flags.is_empty() {
        return Err("unsupported option; use --help".into());
    }
    Ok(command)
}

fn portfolio_parameters(flags: &mut BTreeMap<String, String>) -> Result<(Vec<String>, u32, u64), BoxError> {
    let mut symbols: Vec<String> = flags
        .remove("--symbols")
        .unwrap_or_else(|| "BTCUSDT,ETHUSDT".to_owned())
        .split(',')
        .map(str::to_owned)
        .collect();
    symbols.sort();
    if symbols.is_empty() || symbols.len() > 2
        || symbols.iter().any(|symbol| !matches!(symbol.as_str(), "BTCUSDT" | "ETHUSDT"))
        || symbols.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err("portfolio supports unique BTCUSDT and/or ETHUSDT only".into());
    }
    let leverage = flags.remove("--leverage")
        .unwrap_or_else(|| "10".to_owned()).parse::<u32>()?;
    let heartbeat_seconds = flags.remove("--heartbeat-seconds")
        .unwrap_or_else(|| "1800".to_owned()).parse::<u64>()?;
    if !(60..=86_400).contains(&heartbeat_seconds) {
        return Err("--heartbeat-seconds must be in 60..=86400".into());
    }
    Ok((symbols, leverage, heartbeat_seconds))
}

fn option_decimal(
    flags: &mut BTreeMap<String, String>,
    key: &str,
    default: &str,
) -> Result<Decimal, BoxError> {
    let value = flags.remove(key).unwrap_or_else(|| default.to_owned());
    let unsigned = value.strip_prefix('-').unwrap_or(&value);
    let (whole, fraction) = unsigned
        .split_once('.')
        .map_or((unsigned, None), |(a, b)| (a, Some(b)));
    if value.len() > 64
        || whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.is_some_and(|part| {
            part.is_empty() || part.len() > 28 || !part.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(format!("{key} requires an exact decimal string").into());
    }
    Ok(Decimal::from_str_exact(&value)?)
}

fn output_path(mode: &str) -> PathBuf {
    PathBuf::from("paper-runs").join(format!(
        "{mode}-{}-{}.jsonl",
        Utc::now().format("%Y%m%dT%H%M%SZ"),
        Uuid::new_v4()
    ))
}

async fn live(options: LiveOptions) -> Result<(), BoxError> {
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    run_live(&options, &mut shutdown).await
}

async fn run_live(options: &LiveOptions,
    shutdown: &mut (impl Future<Output = std::io::Result<()>> + Unpin),
) -> Result<(), BoxError> {
    // This client has only fixed-origin public GET endpoints. No credential is
    // read, nor is any authenticated execution engine instantiated here.
    let market = PublicPaperMarket::new()?;
    let startup = async {
        let instrument = recovery::public_get("instrument_metadata",
            || market.instrument(&options.symbol), |notice| recovery::log_notice(&notice)).await?;
        let candles = recovery::public_get("warmup_klines", || {
            // A retry is a new observation: never reuse the original clock.
            market.recent_closed_candles(instrument.spec.instrument(),
                Timeframe::M1, options.warmup, Utc::now())
        }, |notice| recovery::log_notice(&notice)).await?;
        Ok::<_, BoxError>((instrument, candles))
    };
    let (instrument, candles) = tokio::select! {
        biased;
        result = &mut *shutdown => {
            result?;
            println!("Stopped during startup; no paper orders or session log created.");
            return Ok(());
        }
        result = startup => result?,
    };
    if candles.len() != usize::from(options.warmup) {
        return Err("insufficient history for requested warmup".into());
    }
    let last = candles.last().ok_or("empty warmup")?;
    check_freshness(last)?;
    let header = Header::new(
        &instrument,
        options.config,
        Timeframe::M1,
        Utc::now(),
        "live",
    );
    let mut engine = header.engine()?;
    let mut log = tokio::task::block_in_place(|| SessionLog::create(&options.output, &header))?;
    // Warmup cannot submit paper orders. Sync each observed record before
    // announcing completion; no tail is silently considered recoverable.
    for candle in &candles {
        tokio::select! {
            biased;
            result = &mut *shutdown => {
                result?;
                let at = Utc::now();
                let events = engine.shutdown(at)?;
                tokio::task::block_in_place(|| log.finish(at, "shutdown_signal", &engine.snapshot(), &events))?;
                println!("Stopped during warmup; no paper orders opened.");
                return Ok(());
            }
            () = tokio::task::yield_now() => {}
        }
        let step = engine.warm_up(candle)?;
        tokio::task::block_in_place(|| log.append_candle(candle, true, &step))?;
    }
    println!(
        "PAPER ONLY | {} M1 | initial={} USDT | warmup={} | log={}",
        options.symbol,
        options.config.initial_balance(),
        candles.len(),
        options.output.display()
    );
    report(&engine.snapshot())?;
    let mut recent: VecDeque<Candle> = candles.into_iter().rev().take(10).collect();
    recent.make_contiguous().reverse();
    let outcome = collect_live(&market, options, &mut engine, &mut log, &mut recent, shutdown).await;
    // Keep persisted reason bounded and locale-independent; the complete error
    // is returned to main for stderr instead of risking an invalid final record.
    let reason = outcome.as_ref().map_or("error", |reason| *reason);
    let at = Utc::now();
    let events = engine.shutdown(at)?;
    let snapshot = engine.snapshot();
    tokio::task::block_in_place(|| log.finish(at, reason, &snapshot, &events))?;
    println!("Stopped ({reason}); pending entries cancelled; any open position stays marked, not forcibly closed.");
    report(&snapshot)?;
    outcome.map(|_| ())
}

async fn collect_live(
    market: &PublicPaperMarket,
    options: &LiveOptions,
    engine: &mut PaperEngine,
    log: &mut SessionLog,
    recent: &mut VecDeque<Candle>,
    shutdown: &mut (impl Future<Output = std::io::Result<()>> + Unpin),
) -> Result<&'static str, BoxError> {
    let mut interval = tokio::time::interval(Duration::from_secs(options.poll_seconds));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let deadline = async {
        match options.seconds {
            Some(seconds) => tokio::time::sleep(Duration::from_secs(seconds)).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(deadline);
    let mut fresh_until = feed_deadline(recent.back().ok_or("missing candle cursor")?.close_time())?;
    loop {
        let cursor = recent.back().ok_or("missing candle cursor")?.close_time();
        // Preserve the monotonic deadline while this cursor is unchanged,
        // including across polls and backward wall-clock adjustments.
        ensure_live_feed_current(cursor, fresh_until)?;
        tokio::select! {
            biased;
            result = &mut *shutdown => { result?; return Ok("shutdown_signal"); }
            () = &mut deadline => return Ok("duration_elapsed"),
            () = tokio::time::sleep_until(fresh_until) => return Err("paper feed is stale; public-data recovery stopped".into()),
            _ = interval.tick() => {}
        }
        let instrument = recent
            .back()
            .ok_or("missing candle cursor")?
            .instrument()
            .clone();
        let candles = tokio::select! {
            biased;
            result = &mut *shutdown => { result?; return Ok("shutdown_signal"); }
            () = &mut deadline => return Ok("duration_elapsed"),
            () = tokio::time::sleep_until(fresh_until) => return Err("paper feed is stale; public-data recovery stopped".into()),
            result = recovery::public_get("live_klines", || {
                market.recent_closed_candles(&instrument, Timeframe::M1, 10, Utc::now())
            }, |notice| recovery::log_notice(&notice)) => result?,
        };
        ensure_live_feed_current(cursor, fresh_until)?;
        check_freshness(candles.last().ok_or("empty public candle response")?)?;
        validate_live_window(&candles, recent)?;
        for candle in candles {
            if let Some(prior) = recent
                .iter()
                .find(|prior| prior.open_time() == candle.open_time())
            {
                if prior != &candle {
                    return Err("venue revised a previously closed candle; stopping".into());
                }
                continue;
            }
            let cursor = recent.back().ok_or("missing candle cursor")?.close_time();
            if candle.close_time() <= cursor {
                continue;
            }
            ensure_live_feed_current(cursor, fresh_until)?;
            // Engine enforces exact continuity and evaluates prior orders before
            // detecting signals on this candle. No signal-bar entry fill exists.
            let step = engine.process_candle(&candle)?;
            tokio::task::block_in_place(|| log.append_candle(&candle, false, &step))?;
            println!(
                "{} close={} signals={} intents={} rejected={} events={}",
                candle.close_time(),
                candle.close(),
                step.signals,
                step.intents,
                step.rejected,
                serde_json::to_string(&step.events)?
            );
            report(&step.snapshot)?;
            recent.push_back(candle);
            fresh_until = feed_deadline(recent.back().ok_or("missing candle cursor")?.close_time())?;
            while recent.len() > 10 {
                recent.pop_front();
            }
        }
    }
}

fn ensure_live_feed_current(cursor: DateTime<Utc>, fresh_until: tokio::time::Instant) -> Result<(), BoxError> {
    ensure_feed_current(cursor)?;
    if tokio::time::Instant::now() >= fresh_until {
        return Err("paper feed exceeded its monotonic freshness deadline".into());
    }
    Ok(())
}

// Preflight the entire recovered window before any accounting or strategy work.
// No historical observation may be revised and no missing interval is invented.
fn validate_live_window(candles: &[Candle], recent: &VecDeque<Candle>) -> Result<(), BoxError> {
    let cursor = recent.back().ok_or("missing candle cursor")?.close_time();
    for candle in candles {
        if recent.iter().any(|prior| prior.open_time() == candle.open_time() && prior != candle) {
            return Err("venue revised a previously closed candle; stopping".into());
        }
    }
    let mut expected = cursor;
    for candle in candles.iter().filter(|candle| candle.close_time() > cursor) {
        if candle.open_time() != expected {
            return Err("gap in recovered paper candles; stopping".into());
        }
        expected = candle.close_time();
    }
    Ok(())
}

/// A monotonic deadline anchored to the last processed candle, not to a retry.
fn feed_deadline(last_close: DateTime<Utc>) -> Result<tokio::time::Instant, BoxError> {
    let remaining = remaining_feed_budget(last_close, Utc::now())?;
    tokio::time::Instant::now().checked_add(remaining).ok_or_else(|| "paper feed deadline overflow".into())
}

fn ensure_feed_current(last_close: DateTime<Utc>) -> Result<(), BoxError> {
    remaining_feed_budget(last_close, Utc::now()).map(|_| ())
}

fn remaining_feed_budget(last_close: DateTime<Utc>, now: DateTime<Utc>) -> Result<Duration, BoxError> {
    let age = now.signed_duration_since(last_close);
    if age < chrono::Duration::zero() || age >= chrono::Duration::seconds(120) {
        return Err("paper feed is stale or local clock regressed; recovery stopped".into());
    }
    Ok((chrono::Duration::seconds(120) - age).to_std()?)
}

// Docker stop sends SIGTERM; observe it as well as interactive Ctrl+C so normal
// container shutdown can write the final audit record before exiting.
#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

fn check_freshness(candle: &Candle) -> Result<(), BoxError> {
    let age = Utc::now().signed_duration_since(candle.close_time());
    if age < chrono::Duration::zero() || age > chrono::Duration::seconds(120) {
        return Err("public closed candles are stale or local UTC clock is incorrect".into());
    }
    Ok(())
}

fn report(snapshot: &PaperSnapshot) -> Result<(), BoxError> {
    println!("{}", serde_json::to_string(snapshot)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(args: &[&str]) -> Result<Command, BoxError> {
        parse_args(args.iter().map(|value| (*value).to_owned()).collect())
    }

    #[test]
    fn defaults_are_keyless_cash_capped_one_hundred() -> Result<(), BoxError> {
        let Command::Live(options) = command(&["live"])? else {
            return Err("wrong command".into());
        };
        assert_eq!(options.config.initial_balance(), Decimal::new(100, 0));
        assert_eq!(options.config.fee_rate(), Decimal::new(6, 4));
        assert_eq!(options.symbol, "ETHUSDT");
        assert_eq!(options.seconds, None);
        assert_eq!(options.warmup, 500);
        Ok(())
    }

    #[test]
    fn invalid_or_secret_options_fail_closed() {
        for args in [
            vec!["live", "--api-key", "never"],
            vec!["live", "--balance", "NaN"],
            vec!["live", "--balance", "0"],
            vec!["live", "--balance", "1.12345678901234567890123456789"],
            vec!["live", "--seconds", "0"],
            vec!["live", "--warmup", "1"],
            vec!["live", "--symbol", "ETHUSDT", "--symbol", "BTCUSDT"],
            vec!["replay"],
            vec!["replay", "--input", "x", "--balance", "200"],
            vec!["portfolio", "--symbols", "BTCUSDT,BTCUSDT"],
            vec!["portfolio", "--symbols", "SOLUSDT"],
            vec!["portfolio", "--symbol", "ETHUSDT"],
            vec!["portfolio", "--leverage", "11"],
            vec!["portfolio", "--leverage", "0"],
            vec!["live", "--leverage", "10"],
            vec!["portfolio", "--telegram-token", "not-a-real-token"],
            vec!["portfolio", "--heartbeat-seconds", "1"],
            vec!["portfolio", "--entry-window", "24h"],
            vec!["portfolio", "--entry-window", "Always"],
            vec!["portfolio", "--entry-window", "always", "--entry-window", "killzones"],
            vec!["live", "--entry-window", "always"],
            vec!["replay", "--input", "x", "--entry-window", "always"],
        ] {
            assert!(command(&args).is_err());
        }
    }

    #[test]
    fn portfolio_defaults_share_one_hundred_and_ten_times_margin() -> Result<(), BoxError> {
        let Command::Portfolio(options) = command(&["portfolio"])? else { return Err("wrong command".into()); };
        assert_eq!(options.symbols, ["BTCUSDT", "ETHUSDT"]);
        assert_eq!(options.config.initial_balance(), Decimal::new(100, 0));
        assert_eq!(options.config.leverage(), 10);
        assert_eq!(options.heartbeat_seconds, 1800);
        assert_eq!(options.entry_window, PaperEntryWindow::Always);
        Ok(())
    }

    #[test]
    fn portfolio_warmup_reserves_a_settlement_bar_before_any_network_request() -> Result<(), BoxError> {
        let Command::Portfolio(options) = command(&["portfolio", "--warmup", "999"])? else {
            return Err("wrong command".into());
        };
        assert_eq!(options.warmup, 999);
        let error = command(&["portfolio", "--warmup", "1000"]).err().ok_or("accepted impossible warmup capacity")?;
        assert!(error.to_string().contains("one REST bar is reserved for settlement"));
        // The older single-symbol runner does not apply the settlement filter.
        assert!(matches!(command(&["live", "--warmup", "1000"]), Ok(Command::Live(_))));
        Ok(())
    }

    #[test]
    fn portfolio_entry_window_is_explicit_and_does_not_change_risk_or_leverage() -> Result<(), BoxError> {
        for (value, expected) in [("always", PaperEntryWindow::Always), ("killzones", PaperEntryWindow::Killzones)] {
            let Command::Portfolio(options) = command(&["portfolio", "--entry-window", value])? else {
                return Err("wrong command".into());
            };
            assert_eq!(options.entry_window, expected);
            assert_eq!(options.config.initial_balance(), Decimal::new(100, 0));
            assert_eq!(options.config.leverage(), 10);
            assert_eq!(options.config.min_net_rr(), Decimal::TWO);
        }
        Ok(())
    }

    #[test]
    fn retry_feed_budget_is_anchored_to_processed_close_not_last_attempt() -> Result<(), BoxError> {
        let closed = DateTime::from_timestamp(1_793_185_200, 0).ok_or("bad fixture timestamp")?;
        assert_eq!(remaining_feed_budget(closed, closed + chrono::Duration::seconds(30))?, Duration::from_secs(90));
        assert_eq!(remaining_feed_budget(closed, closed + chrono::Duration::seconds(119))?, Duration::from_secs(1));
        assert!(remaining_feed_budget(closed, closed + chrono::Duration::seconds(120)).is_err());
        assert!(remaining_feed_budget(closed, closed - chrono::Duration::nanoseconds(1)).is_err());
        Ok(())
    }

    #[test]
    fn legacy_feed_rejects_expired_monotonic_deadline_despite_fresh_utc() -> Result<(), BoxError> {
        let cursor = Utc::now() - chrono::Duration::seconds(30);
        ensure_feed_current(cursor)?;
        let expired = tokio::time::Instant::now() - Duration::from_secs(1);
        assert!(ensure_live_feed_current(cursor, expired).is_err());
        let still_current = tokio::time::Instant::now() + Duration::from_secs(30);
        ensure_live_feed_current(cursor, still_current)?;
        Ok(())
    }

    fn recovery_candle(minute: i64, close: i64) -> Result<Candle, BoxError> {
        use ict_trading_bot::{CandleInput, Exchange, InstrumentId, MarketKind, VolumeUnit};
        let at = DateTime::from_timestamp(1_793_185_200 + minute * 60, 0).ok_or("bad fixture timestamp")?;
        Ok(Candle::new(CandleInput {
            instrument: InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual, "ETHUSDT", "ETH-USDT-PERP")?,
            timeframe: Timeframe::M1, open_time: at, close_time: at + chrono::Duration::minutes(1),
            open: Decimal::new(100, 0), high: Decimal::new(102, 0), low: Decimal::new(98, 0),
            close: Decimal::from(close), volume: Decimal::ONE, volume_unit: VolumeUnit::BaseAsset,
            trade_count: None, is_closed: true,
        })?)
    }

    #[test]
    fn recovered_live_window_checks_all_revisions_and_gaps_before_engine_work() -> Result<(), BoxError> {
        let history = VecDeque::from([recovery_candle(0, 100)?, recovery_candle(1, 100)?]);
        let before = history.clone();
        validate_live_window(&[recovery_candle(1, 100)?, recovery_candle(2, 100)?], &history)?;
        assert!(validate_live_window(&[recovery_candle(1, 101)?, recovery_candle(2, 100)?], &history).is_err());
        assert!(validate_live_window(&[recovery_candle(1, 100)?, recovery_candle(3, 100)?], &history).is_err());
        assert!(validate_live_window(&[recovery_candle(2, 100)?, recovery_candle(4, 100)?], &history).is_err());
        assert_eq!(history, before);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ready_shutdown_preempts_legacy_startup_and_never_creates_an_account_log() -> Result<(), BoxError> {
        let Command::Live(mut options) = command(&["live"])? else { return Err("wrong command".into()); };
        options.output = std::env::temp_dir().join(format!("paper-legacy-stop-{}.jsonl", Uuid::new_v4()));
        let mut shutdown = std::future::ready(Ok(()));
        run_live(&options, &mut shutdown).await?;
        assert!(!options.output.exists());
        Ok(())
    }
}
