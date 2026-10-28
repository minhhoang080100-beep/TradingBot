//! Credential-free, foreground ICT paper runner. Never submits exchange orders.

#[path = "paper/session.rs"]
mod session;

use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    process::ExitCode,
    time::Duration,
};

use chrono::Utc;
use ict_trading_bot::{
    models::{Candle, Timeframe},
    paper::{PaperConfig, PaperEngine, PaperSnapshot, PublicPaperMarket},
};
use rust_decimal::Decimal;
use session::{BoxError, Header, SessionLog};
use uuid::Uuid;

const HELP: &str = "Local paper trading ONLY; no API keys, deposits, or real orders.

paper live [--symbol ETHUSDT] [--balance 100] [--seconds N]
           [--poll-seconds 5] [--warmup 500] [--fee-bps 6]
           [--stop-slippage-bps 2] [--output paper-runs/NAME.jsonl]
paper replay --input paper-runs/NAME.jsonl [--output paper-runs/REPLAY.jsonl]

Live uses public Bybit USDT perpetual data, M1, entry notional+fee <= cash.
Every live launch starts a NEW account; no automatic resume. Ctrl+C stops it.
Default risk: 1%/trade, 5% daily drawdown circuit breaker, net RR >= 2.
Fees/slippage are simulation assumptions, not your exchange fee tier.
Warmup only initializes signals; it cannot spend initial paper cash.
Closed-bar full fills are conservative, not order-book/partial-fill simulation.
Funding/liquidation are not modeled. Gaps or HTTP errors stop the run.
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
enum Command {
    Help,
    Live(LiveOptions),
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
        Command::Replay { input, output } => {
            let snapshot = tokio::task::block_in_place(|| session::replay(&input, &output))?;
            println!("Replay verified; derived log: {}", output.display());
            report(&snapshot)?;
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
    if !matches!(mode.as_str(), "live" | "replay") {
        return Err("expected live, replay, or --help".into());
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
        let symbol = flags
            .remove("--symbol")
            .unwrap_or_else(|| "ETHUSDT".to_owned());
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
        Command::Live(LiveOptions {
            symbol,
            config,
            seconds,
            poll_seconds,
            warmup,
            output,
        })
    };
    if !flags.is_empty() {
        return Err("unsupported option; use --help".into());
    }
    Ok(command)
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
    // This client has only fixed-origin public GET endpoints. No credential is
    // read, nor is any authenticated execution engine instantiated here.
    let market = PublicPaperMarket::new()?;
    let instrument = market.instrument(&options.symbol).await?;
    let candles = market
        .recent_closed_candles(
            instrument.spec.instrument(),
            Timeframe::M1,
            options.warmup,
            Utc::now(),
        )
        .await?;
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
    tokio::task::block_in_place(|| -> Result<(), BoxError> {
        for candle in &candles {
            let step = engine.warm_up(candle)?;
            log.append_candle(candle, true, &step)?;
        }
        Ok(())
    })?;
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
    let outcome = collect_live(&market, &options, &mut engine, &mut log, &mut recent).await;
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
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            result = &mut shutdown => { result?; return Ok("shutdown_signal"); }
            () = &mut deadline => return Ok("duration_elapsed"),
            _ = interval.tick() => {}
        }
        let instrument = recent
            .back()
            .ok_or("missing candle cursor")?
            .instrument()
            .clone();
        let candles = tokio::select! {
            result = &mut shutdown => { result?; return Ok("shutdown_signal"); }
            () = &mut deadline => return Ok("duration_elapsed"),
            result = market.recent_closed_candles(&instrument, Timeframe::M1, 10, Utc::now()) => result?,
        };
        check_freshness(candles.last().ok_or("empty public candle response")?)?;
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
            while recent.len() > 10 {
                recent.pop_front();
            }
        }
    }
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
        ] {
            assert!(command(&args).is_err());
        }
    }
}
