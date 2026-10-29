//! Offline, reproducible screening of the existing PAPER strategy.
//!
//! This executable never creates a network client, reads Telegram credentials,
//! or changes the running bot. A favorable report is not a deployment signal.

#[path = "paper_backtest/dataset.rs"]
mod dataset;
#[path = "paper_backtest/evaluate.rs"]
mod evaluate;

use std::{collections::BTreeMap, env, error::Error, fs::OpenOptions, io::Write, path::PathBuf};

use chrono::{DateTime, Utc};
use serde_json::json;

type BoxError = Box<dyn Error + Send + Sync>;

const HELP: &str = "Offline PAPER strategy screening; no exchange or Telegram access.

paper_backtest --dataset DATASET.json --split-at 2026-10-22T00:00:00Z --output NEW_REPORT.json

Dataset: version 1 manifest with two normalized M1 candle JSON arrays and exact
string-valued instrument metadata. Exactly BTCUSDT + ETHUSDT are required.
The first 500 candles warm up development; holdout starts at --split-at OPEN,
with its own 100-USDT account and the preceding 500 historical warmup candles.
Fixed comparisons: Always / Killzones, each at 6/2 and stressed 10/5 bps
fees/stop slippage, plus a research-only nearest-viable-target hypothesis
under Always at both costs. Four extra Always profiles test a fixed one-bar
entry embargo for both target policies and cost assumptions. This is a timing
sensitivity test, not a real latency model. Eight entry-v2 profiles additionally
place a resting CE limit at FVG confirmation, retaining confirmed setup context;
both target policies, costs and fill timings are compared. Risk is not tuned.
Reports include source/config hashes, costs, drawdown, unclosed exposure,
trade events, and explicit limitations. Output must not already exist.
Results are UNVALIDATED: OHLC simulation excludes funding and liquidation.
";

#[derive(Debug)]
struct Options {
    dataset: PathBuf,
    split_at: DateTime<Utc>,
    output: PathBuf,
}

fn options(args: impl IntoIterator<Item = String>) -> Result<Option<Options>, BoxError> {
    let mut args = args.into_iter();
    let mut flags = BTreeMap::new();
    while let Some(key) = args.next() {
        if matches!(key.as_str(), "--help" | "-h") {
            if !flags.is_empty() || args.next().is_some() { return Err("--help must stand alone".into()); }
            return Ok(None);
        }
        if !matches!(key.as_str(), "--dataset" | "--split-at" | "--output") {
            return Err("unsupported backtest option; use --help".into());
        }
        let value = args.next().ok_or("missing backtest option value")?;
        if flags.insert(key, value).is_some() { return Err("duplicate backtest option".into()); }
    }
    let split_at = DateTime::parse_from_rfc3339(&flags.remove("--split-at").ok_or("--split-at is required")?)?
        .with_timezone(&Utc);
    if split_at.timestamp().rem_euclid(60) != 0 || split_at.timestamp_subsec_nanos() != 0 {
        return Err("--split-at must be a UTC-aligned M1 opening boundary".into());
    }
    Ok(Some(Options {
        dataset: PathBuf::from(flags.remove("--dataset").ok_or("--dataset is required")?),
        split_at,
        output: PathBuf::from(flags.remove("--output").ok_or("--output is required")?),
    }))
}

fn run(options: &Options) -> Result<(), BoxError> {
    if options.output.try_exists()? { return Err("backtest output already exists; choose a new path".into()); }
    let data = dataset::load(&options.dataset)?;
    let report = evaluate::report(&data, options.split_at)?;
    let bytes = serde_json::to_vec_pretty(&report)?;
    // All input validation/evaluation completes before creating an artifact.
    // create_new also prevents a later filesystem race from replacing evidence.
    let mut file = OpenOptions::new().write(true).create_new(true).open(&options.output)?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    println!("{}", serde_json::to_string_pretty(&json!({
        "status": "UNVALIDATED",
        "output": options.output,
        "dataset_sha256": data.provenance.combined_sha256,
        "split_at": options.split_at,
        "results": report["results"].as_array().map(|rows| rows.iter().map(|row| json!({
            "profile": row["profile"], "segment": row["segment"], "metrics": row["metrics"],
            "warnings": row["warnings"],
        })).collect::<Vec<_>>()),
    }))?);
    Ok(())
}

fn main() -> Result<(), BoxError> {
    if let Some(options) = options(env::args().skip(1))? { run(&options) }
    else { print!("{HELP}"); Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_requires_an_explicit_chronological_split_and_rejects_secrets() {
        for args in [
            vec!["--dataset", "a", "--output", "b"],
            vec!["--api-key", "never"],
            vec!["--dataset", "a", "--dataset", "b"],
            vec!["--dataset", "a", "--output", "b", "--split-at", "2026-10-22T00:00:01Z"],
        ] {
            assert!(options(args.into_iter().map(str::to_owned)).is_err());
        }
        assert!(options(["--help".to_owned()]).is_ok());
    }
}
