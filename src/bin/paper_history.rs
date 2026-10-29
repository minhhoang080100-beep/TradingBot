//! Public-only historical dataset collection for the offline paper evaluator.
//! No credentials, private endpoints, Telegram or simulated orders are used.

use std::{collections::BTreeMap, error::Error, fs::{self, OpenOptions}, io::Write, path::{Path, PathBuf}, time::Duration};

use chrono::{DateTime, Utc};
use ict_trading_bot::{paper::{PaperInstrument, PublicPaperMarket}, Candle, Timeframe};
use serde_json::json;

#[path = "paper/recovery.rs"]
mod recovery;

type BoxError = Box<dyn Error + Send + Sync>;
const MAX_BARS: i64 = 100_000;

struct Options { start: DateTime<Utc>, end: DateTime<Utc>, output: PathBuf }

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("History collection stopped: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), BoxError> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!("paper_history --start <UTC RFC3339> --end <UTC RFC3339> --output <NEW directory>\nPublic Bybit BTCUSDT/ETHUSDT M1; interval [start,end), max 100000 bars each.\nInclude 500 preceding warmup bars before the intended evaluation start.\nMetadata is captured now, not reconstructed historically. No API keys or orders.");
        return Ok(());
    }
    let options = parse_options(&args, Utc::now())?;
    let parent = options.output.parent().filter(|path| !path.as_os_str().is_empty());
    if let Some(parent) = parent { fs::create_dir_all(parent)?; }
    // Never reuse or overwrite evidence from another collection.
    fs::create_dir(&options.output)?;
    let market = PublicPaperMarket::new()?;
    let metadata_as_of = Utc::now();
    let mut metadata = Vec::new();
    for symbol in ["BTCUSDT", "ETHUSDT"] {
        let instrument = recovery::public_get("history_metadata", || market.instrument(symbol), |notice| recovery::log_notice(&notice)).await?;
        let candles = collect_symbol(&market, &instrument, options.start, options.end).await?;
        let filename = format!("{symbol}.json");
        write_new_json(&options.output.join(&filename), &candles)?;
        let spec = &instrument.spec;
        metadata.push(json!({"symbol":symbol, "price_tick":spec.price_tick().to_string(),
            "quantity_step":spec.quantity_step().to_string(), "minimum_quantity":spec.minimum_quantity().to_string(),
            "maximum_quantity":spec.maximum_quantity().map(|value| value.to_string()),
            "min_notional":instrument.min_notional.to_string(), "candles_file":filename}));
        println!("{symbol}: {} complete M1 candles saved", candles.len());
    }
    // A manifest is published only once both full series have been validated.
    write_new_json(&options.output.join("manifest.json"), &json!({
        "version":1, "source":"https://api.bybit.com/v5/market/kline (public linear M1); current instrument metadata",
        "captured_at":Utc::now(), "metadata_as_of":metadata_as_of, "instruments":metadata,
    }))?;
    println!("Complete dataset: {}", options.output.join("manifest.json").display());
    Ok(())
}

fn parse_options(args: &[String], now: DateTime<Utc>) -> Result<Options, BoxError> {
    if args.len() != 6 { return Err("expected --start, --end and --output; use --help".into()); }
    let mut flags = BTreeMap::new();
    for pair in args.chunks_exact(2) {
        if flags.insert(pair[0].as_str(), pair[1].as_str()).is_some() { return Err("duplicate option".into()); }
    }
    let start = DateTime::parse_from_rfc3339(flags.remove("--start").ok_or("missing --start")?)?.with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339(flags.remove("--end").ok_or("missing --end")?)?.with_timezone(&Utc);
    let output = PathBuf::from(flags.remove("--output").ok_or("missing --output")?);
    if !flags.is_empty() || output.as_os_str().is_empty() { return Err("invalid option or output".into()); }
    if start.timestamp() < 0 || start >= end || end > now - chrono::Duration::seconds(10)
        || start.timestamp_subsec_nanos() != 0 || end.timestamp_subsec_nanos() != 0
        || start.timestamp().rem_euclid(60) != 0 || end.timestamp().rem_euclid(60) != 0
        || (end - start).num_minutes() > MAX_BARS {
        return Err("range must be final UTC minute boundaries, 1..=100000 M1 bars".into());
    }
    Ok(Options { start, end, output })
}

async fn collect_symbol(market: &PublicPaperMarket, instrument: &PaperInstrument,
    start: DateTime<Utc>, end: DateTime<Utc>,
) -> Result<Vec<Candle>, BoxError> {
    let mut cursor = end;
    let mut all = Vec::new();
    let mut pages = 0_u32;
    while cursor > start {
        // Sequential, paced public GETs. Recovery is bounded and honours venue
        // retry hints; it never changes the symbol, origin or requested range.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let page = recovery::public_get("history_klines", || market.historical_closed_candles(
            instrument.spec.instrument(), Timeframe::M1, start, cursor, 1000, Utc::now()), |notice| recovery::log_notice(&notice)).await?;
        let first = page.first().ok_or("empty historical page before requested start")?;
        if first.open_time() >= cursor { return Err("historical pagination made no progress".into()); }
        cursor = first.open_time();
        all.extend(page);
        if all.len() > usize::try_from(MAX_BARS)? { return Err("history capacity exceeded".into()); }
        pages += 1;
        if pages % 10 == 0 { println!("{}: {} bars collected", instrument.spec.instrument().venue_symbol, all.len()); }
    }
    all.sort_unstable_by_key(Candle::open_time);
    validate_coverage(&all, start, end)?;
    Ok(all)
}

fn validate_coverage(candles: &[Candle], start: DateTime<Utc>, end: DateTime<Utc>) -> Result<(), BoxError> {
    let mut expected = start;
    for candle in candles {
        if candle.open_time() != expected || candle.close_time() != expected + chrono::Duration::minutes(1)
            || candle.timeframe() != Timeframe::M1 || !candle.is_closed() {
            return Err("historical series has duplicate, missing or invalid bars".into());
        }
        expected = candle.close_time();
    }
    if expected != end || candles.is_empty() { return Err("historical series does not cover requested range".into()); }
    Ok(())
}

fn write_new_json(path: &Path, value: &impl serde::Serialize) -> Result<(), BoxError> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ict_trading_bot::{CandleInput, Exchange, InstrumentId, MarketKind, VolumeUnit};
    use rust_decimal::Decimal;

    #[test]
    fn collection_requires_explicit_bounded_settled_range() -> Result<(), BoxError> {
        let now = DateTime::parse_from_rfc3339("2026-10-29T00:00:09Z")?.with_timezone(&Utc);
        let mut args: Vec<String> = ["--start", "2026-09-28T15:40:00Z", "--end", "2026-10-29T00:00:00Z", "--output", "new-data"].iter().map(|arg| (*arg).to_owned()).collect();
        assert!(parse_options(&args, now).is_err());
        assert!(parse_options(&args, now + chrono::Duration::seconds(1)).is_ok());
        args[1] = "2026-01-01T00:00:00Z".to_owned();
        assert!(parse_options(&args, now).is_err());
        args[1] = "2026-10-28T00:00:01Z".to_owned();
        assert!(parse_options(&args, now).is_err());
        args[0] = "--api-key".to_owned();
        assert!(parse_options(&args, now).is_err());
        Ok(())
    }

    #[test]
    fn coverage_rejects_gaps_duplicates_and_partial_history() -> Result<(), BoxError> {
        let start = DateTime::from_timestamp(0, 0).ok_or("fixture time")?;
        let instrument = InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual, "BTCUSDT", "BTC-USDT-PERP")?;
        let make = |minute: i64| Candle::new(CandleInput { instrument: instrument.clone(), timeframe: Timeframe::M1,
            open_time: start + chrono::Duration::minutes(minute), close_time: start + chrono::Duration::minutes(minute + 1),
            open: Decimal::ONE, high: Decimal::ONE, low: Decimal::ONE, close: Decimal::ONE,
            volume: Decimal::ONE, volume_unit: VolumeUnit::BaseAsset, trade_count: None, is_closed: true });
        let end = start + chrono::Duration::minutes(2);
        assert!(validate_coverage(&[make(0)?, make(1)?], start, end).is_ok());
        assert!(validate_coverage(&[make(0)?, make(0)?], start, end).is_err());
        assert!(validate_coverage(&[make(0)?, make(2)?], start, end).is_err());
        assert!(validate_coverage(&[make(0)?], start, end).is_err());
        assert!(validate_coverage(&[], start, end).is_err());
        Ok(())
    }
}
