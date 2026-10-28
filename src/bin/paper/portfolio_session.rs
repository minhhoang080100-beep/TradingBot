//! Versioned multi-symbol audit; intentionally separate from v1 single-series logs.
//! A complete v2 replay checks all batches and accounting before creating output.
//! Aborted sessions preserve diagnostic evidence but can never produce a verified replay.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Write},
    path::Path,
};

use chrono::{DateTime, Utc};
use ict_trading_bot::{
    paper::{PaperConfig, PaperInstrument, PaperPortfolio, PortfolioEvent, PortfolioSnapshot, PortfolioStep},
    Candle, Timeframe,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::session::{
    decimal, invalid, next_line, parse_candle, validate_summary, BoxError, InstrumentDto, UniqueValue,
};

const ALGORITHM: &str = "ict-paper-portfolio-v2";
const MAX_BYTES: u64 = 256 * 1024 * 1024;
const MAX_LINE: usize = 64 * 1024;
const MAX_BATCHES: usize = 100_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioHeader {
    version: u16,
    algorithm: String,
    session_id: Uuid,
    started_at: DateTime<Utc>,
    timeframe: Timeframe,
    instruments: Vec<InstrumentDto>,
    initial_balance: String,
    fee_rate: String,
    stop_slippage_bps: String,
    margin_fraction: String,
    min_net_rr: String,
    leverage: u32,
}

impl PortfolioHeader {
    pub fn new(instruments: &[PaperInstrument], config: PaperConfig, session_id: Uuid) -> Self {
        Self {
            version: 2,
            algorithm: ALGORITHM.to_owned(),
            session_id,
            started_at: Utc::now(),
            timeframe: Timeframe::M1,
            instruments: instruments.iter().map(InstrumentDto::capture).collect(),
            initial_balance: config.initial_balance().to_string(),
            fee_rate: config.fee_rate().to_string(),
            stop_slippage_bps: config.stop_slippage_bps().to_string(),
            margin_fraction: config.max_notional_fraction().to_string(),
            min_net_rr: config.min_net_rr().to_string(),
            leverage: config.leverage(),
        }
    }

    pub fn engine(&self) -> Result<PaperPortfolio, BoxError> {
        if self.version != 2 || self.algorithm != ALGORITHM || self.session_id.is_nil() {
            return Err(invalid("unsupported portfolio session header"));
        }
        let config = PaperConfig::new(
            decimal(&self.initial_balance)?,
            decimal(&self.fee_rate)?,
            decimal(&self.stop_slippage_bps)?,
            decimal(&self.margin_fraction)?,
            decimal(&self.min_net_rr)?,
        )?.with_leverage(self.leverage)?;
        let instruments = self.instruments.iter().map(InstrumentDto::restore).collect::<Result<Vec<_>, _>>()?;
        Ok(PaperPortfolio::new(instruments, config, self.timeframe)?)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header { header: Box<PortfolioHeader> },
    Batch { candles: Vec<Value>, warmup: bool, step: Value },
    Summary { at: DateTime<Utc>, reason: String, snapshot: Value, events: Value },
    Aborted { at: DateTime<Utc>, reason: String, snapshot: Value, events: Value },
}

pub struct PortfolioLog {
    file: File,
    bytes: u64,
    batches: usize,
    last_close: Option<DateTime<Utc>>,
    finished: bool,
    poisoned: bool,
}

impl PortfolioLog {
    pub fn create(path: &Path, header: &PortfolioHeader) -> Result<Self, BoxError> {
        header.engine()?;
        let bytes = encode(&Record::Header { header: Box::new(header.clone()) })?;
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let mut log = Self {
            file: OpenOptions::new().write(true).create_new(true).open(path)?,
            bytes: 0, batches: 0, last_close: None, finished: false, poisoned: false,
        };
        log.write(&bytes)?;
        Ok(log)
    }

    pub fn append(&mut self, candles: &[Candle], warmup: bool, step: &PortfolioStep) -> Result<(), BoxError> {
        if self.batches >= MAX_BATCHES || candles.is_empty() || candles.len() > 2 {
            return Err(invalid("portfolio session batch limit or shape invalid"));
        }
        self.write(&encode(&Record::Batch {
            candles: candles.iter().map(serde_json::to_value).collect::<Result<Vec<_>, _>>()?,
            warmup,
            step: serde_json::to_value(step)?,
        })?)?;
        self.batches += 1;
        self.last_close = candles.iter().map(Candle::close_time).chain(self.last_close).max();
        Ok(())
    }

    pub fn finish(&mut self, at: DateTime<Utc>, reason: &str, snapshot: &PortfolioSnapshot, events: &[PortfolioEvent]) -> Result<(), BoxError> {
        validate_summary(at, reason, self.last_close)?;
        self.write(&encode(&Record::Summary {
            at, reason: reason.to_owned(), snapshot: serde_json::to_value(snapshot)?, events: serde_json::to_value(events)?,
        })?)?;
        self.finished = true;
        Ok(())
    }

    /// Writes a terminal diagnostic record after a fatal engine/shutdown error.
    ///
    /// The failed input or partial transition may not have a reproducible batch
    /// record. This preserves the observed snapshot and actual shutdown events
    /// without presenting them as a verified complete session. Use a sanitized
    /// reason label, not a raw transport error or credential-bearing payload.
    ///
    /// # Errors
    /// Rejects invalid time/reason, a closed/poisoned log, limits or write failure.
    pub fn abort(&mut self, at: DateTime<Utc>, reason: &str, snapshot: &PortfolioSnapshot, events: &[PortfolioEvent]) -> Result<(), BoxError> {
        validate_summary(at, reason, self.last_close)?;
        self.write(&encode(&Record::Aborted {
            at, reason: reason.to_owned(), snapshot: serde_json::to_value(snapshot)?, events: serde_json::to_value(events)?,
        })?)?;
        self.finished = true;
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), BoxError> {
        let count = u64::try_from(bytes.len())?;
        if self.finished || self.poisoned || bytes.len() > MAX_LINE || bytes.last() != Some(&b'\n')
            || count > MAX_BYTES.saturating_sub(self.bytes) {
            return Err(invalid("portfolio log is closed, poisoned, or exceeds bounds"));
        }
        self.poisoned = true;
        self.file.write_all(bytes)?;
        self.file.flush()?;
        self.file.sync_data()?;
        self.bytes += count;
        self.poisoned = false;
        Ok(())
    }
}

pub fn is_portfolio(input: &Path) -> Result<bool, BoxError> {
    let mut reader = BufReader::new(File::open(input)?);
    let first = next_line(&mut reader, &mut 0)?.ok_or_else(|| invalid("empty paper session"))?;
    let value = serde_json::from_slice::<UniqueValue>(&first)?.0;
    Ok(value.get("header").and_then(|header| header.get("algorithm")).and_then(Value::as_str) == Some(ALGORITHM))
}

pub fn replay(input: &Path, output: &Path) -> Result<PortfolioSnapshot, BoxError> {
    let file = File::open(input)?;
    if file.metadata()?.len() > MAX_BYTES {
        return Err(invalid("portfolio replay exceeds file limit"));
    }
    let mut reader = BufReader::new(file);
    let mut total = 0;
    let first = next_line(&mut reader, &mut total)?.ok_or_else(|| invalid("empty portfolio session"))?;
    let Record::Header { header } = decode(&first)? else {
        return Err(invalid("portfolio session must start with header"));
    };
    let mut engine = header.engine()?;
    let mut tail = Vec::new();
    let mut result = None;
    let mut batches = 0;
    let mut last_close = None;
    while let Some(line) = next_line(&mut reader, &mut total)? {
        if result.is_some() { return Err(invalid("records after portfolio summary")); }
        match decode(&line)? {
            Record::Header { .. } => return Err(invalid("repeated portfolio header")),
            Record::Aborted { .. } => return Err(invalid("aborted portfolio session is diagnostic-only and cannot be replayed")),
            Record::Batch { candles, warmup, step } => {
                batches += 1;
                if batches > MAX_BATCHES || candles.is_empty() || candles.len() > 2 {
                    return Err(invalid("portfolio replay batch limit or shape invalid"));
                }
                let candles = candles.iter().map(parse_candle).collect::<Result<Vec<_>, _>>()?;
                let actual = if warmup { engine.warm_up(&candles)? } else { engine.process_batch(&candles)? };
                if serde_json::to_value(actual)? != step {
                    return Err(invalid("portfolio batch outcome does not reproduce"));
                }
                last_close = candles.iter().map(Candle::close_time).chain(last_close).max();
            }
            Record::Summary { at, reason, snapshot, events } => {
                validate_summary(at, &reason, last_close)?;
                let actual_events = engine.shutdown(at)?;
                let actual = engine.snapshot();
                if serde_json::to_value(&actual)? != snapshot || serde_json::to_value(actual_events)? != events {
                    return Err(invalid("portfolio final accounting does not reproduce"));
                }
                result = Some(actual);
            }
        }
        tail.extend_from_slice(&line);
    }
    let snapshot = result.ok_or_else(|| invalid("portfolio session missing final summary"))?;
    let header_bytes = encode(&Record::Header { header: header.clone() })?;
    if u64::try_from(header_bytes.len())? + u64::try_from(tail.len())? > MAX_BYTES {
        return Err(invalid("derived portfolio session exceeds file limit"));
    }
    let mut log = PortfolioLog::create(output, &header)?;
    for line in tail.split_inclusive(|byte| *byte == b'\n') { log.write(line)?; }
    Ok(snapshot)
}

fn encode(record: &Record) -> Result<Vec<u8>, BoxError> {
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_LINE { return Err(invalid("portfolio record exceeds line limit")); }
    Ok(bytes)
}

fn decode(line: &[u8]) -> Result<Record, BoxError> {
    Ok(serde_json::from_value(serde_json::from_slice::<UniqueValue>(line)?.0)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ict_trading_bot::{CandleInput, Exchange, InstrumentId, InstrumentRiskSpec, MarketKind, VolumeUnit};
    use rust_decimal::Decimal;
    use std::path::PathBuf;

    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Result<Self, BoxError> {
            let path = std::env::temp_dir().join(format!("ict-portfolio-replay-{}", Uuid::new_v4()));
            fs::create_dir(&path)?;
            Ok(Self(path))
        }
        fn path(&self, name: &str) -> PathBuf { self.0.join(name) }
    }
    impl Drop for Temp { fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); } }

    fn instruments() -> Result<Vec<PaperInstrument>, BoxError> {
        ["BTC", "ETH"].iter().map(|asset| Ok(PaperInstrument {
            spec: InstrumentRiskSpec::new(
                InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual, format!("{asset}USDT"), format!("{asset}-USDT-PERP"))?,
                Decimal::new(1, 1), Decimal::new(1, 3), Decimal::new(1, 3), None, Decimal::ONE,
            )?, min_notional: Decimal::ONE,
        })).collect()
    }
    fn batch(seconds: i64) -> Result<Vec<Candle>, BoxError> {
        instruments()?.iter().map(|instrument| Ok(Candle::new(CandleInput {
            instrument: instrument.spec.instrument().clone(), timeframe: Timeframe::M1,
            open_time: DateTime::from_timestamp(seconds, 0).ok_or("bad fixture time")?,
            close_time: DateTime::from_timestamp(seconds + 60, 0).ok_or("bad fixture time")?,
            open: Decimal::new(100, 0), high: Decimal::new(101, 0), low: Decimal::new(99, 0), close: Decimal::new(100, 0),
            volume: Decimal::ONE, volume_unit: VolumeUnit::BaseAsset, trade_count: None, is_closed: true,
        })?)).collect()
    }
    fn complete(path: &Path) -> Result<PortfolioSnapshot, BoxError> {
        let header = PortfolioHeader::new(&instruments()?, PaperConfig::default().with_leverage(10)?, Uuid::new_v4());
        let mut engine = header.engine()?;
        let mut log = PortfolioLog::create(path, &header)?;
        for (seconds, warmup) in [(0, true), (60, true), (120, false)] {
            let batch = batch(seconds)?;
            let step = if warmup { engine.warm_up(&batch)? } else { engine.process_batch(&batch)? };
            log.append(&batch, warmup, &step)?;
        }
        let at = DateTime::from_timestamp(181, 0).ok_or("bad fixture time")?;
        let events = engine.shutdown(at)?;
        let snapshot = engine.snapshot();
        log.finish(at, "duration_elapsed", &snapshot, &events)?;
        Ok(snapshot)
    }

    fn trade_prices() -> Vec<[i64; 4]> {
        // Five-candle fractals and the default 20-body displacement establish
        // bearish BOS -> SSL sweep -> bullish MSS/FVG -> live CE retracement.
        let mut prices = vec![[100, 102, 98, 101]; 9];
        prices[2] = [100, 150, 98, 101];
        prices.extend([
            [100, 102, 95, 101], [100, 102, 97, 101], [100, 102, 98, 101],
            [100, 105, 98, 101], [100, 103, 97, 100], [99, 102, 96, 98],
            [98, 99, 93, 94], [94, 97, 92, 94], [94, 97, 91, 93],
            [93, 96, 90, 93], [94, 98, 92, 96], [96, 99, 93, 97],
            [97, 100, 89, 94], [94, 112, 94, 111], [110, 113, 108, 111],
            [110, 112, 103, 109],
        ]);
        prices
    }

    fn trade_at(minute: i64) -> Result<DateTime<Utc>, BoxError> {
        // A real default NY killzone, Wednesday 07:00 local daylight time.
        DateTime::parse_from_rfc3339("2026-10-28T11:00:00Z")?.with_timezone(&Utc)
            .checked_add_signed(chrono::Duration::minutes(minute))
            .ok_or_else(|| invalid("test timestamp overflow"))
    }

    fn trade_batch(minute: i64, btc: [i64; 4], eth: [i64; 4]) -> Result<Vec<Candle>, BoxError> {
        instruments()?.into_iter().zip([btc, eth]).map(|(instrument, prices)| {
            Ok(Candle::new(CandleInput {
                instrument: instrument.spec.instrument().clone(), timeframe: Timeframe::M1,
                open_time: trade_at(minute)?, close_time: trade_at(minute + 1)?,
                open: Decimal::from(prices[0]), high: Decimal::from(prices[1]),
                low: Decimal::from(prices[2]), close: Decimal::from(prices[3]),
                volume: Decimal::ONE, volume_unit: VolumeUnit::BaseAsset, trade_count: None, is_closed: true,
            })?)
        }).collect()
    }

    fn append_trade_batch(log: &mut PortfolioLog, engine: &mut PaperPortfolio, batch: &[Candle], warmup: bool) -> Result<Vec<PortfolioEvent>, BoxError> {
        let step = if warmup { engine.warm_up(batch)? } else { engine.process_batch(batch)? };
        log.append(batch, warmup, &step)?;
        Ok(step.events)
    }

    fn complete_trade_session(path: &Path, same_bar_stop: bool) -> Result<(PortfolioSnapshot, Vec<PortfolioEvent>), BoxError> {
        let header = PortfolioHeader::new(&instruments()?, PaperConfig::default().with_leverage(10)?, Uuid::new_v4());
        let mut engine = header.engine()?;
        let mut log = PortfolioLog::create(path, &header)?;
        let mut events = Vec::new();
        for (minute, prices) in trade_prices().into_iter().enumerate() {
            // Keep ETH neutral until its own later setup. A previously consumed
            // setup cannot simply be replayed as if its ICT state were fresh.
            let eth = if same_bar_stop { prices } else { [100, 102, 98, 101] };
            events.extend(append_trade_batch(&mut log, &mut engine,
                &trade_batch(i64::try_from(minute)?, prices, eth)?, minute < 24)?);
        }
        let at = if same_bar_stop {
            events.extend(append_trade_batch(&mut log, &mut engine,
                &trade_batch(25, [109, 110, 80, 100], [109, 110, 103, 109])?, false)?);
            trade_at(26)?
        } else {
            events.extend(append_trade_batch(&mut log, &mut engine,
                &trade_batch(25, [109, 110, 103, 109], [100, 102, 98, 101])?, false)?);
            events.extend(append_trade_batch(&mut log, &mut engine,
                &trade_batch(26, [109, 151, 109, 150], [100, 102, 98, 101])?, false)?);
            for (minute, prices) in trade_prices().into_iter().enumerate() {
                events.extend(append_trade_batch(&mut log, &mut engine,
                    &trade_batch(27 + i64::try_from(minute)?, [110, 112, 109, 111], prices)?, false)?);
            }
            events.extend(append_trade_batch(&mut log, &mut engine,
                &trade_batch(52, [110, 112, 109, 111], [109, 110, 103, 109])?, false)?);
            events.extend(append_trade_batch(&mut log, &mut engine,
                &trade_batch(53, [110, 112, 109, 111], [109, 151, 109, 150])?, false)?);
            trade_at(54)?
        };
        let shutdown = engine.shutdown(at)?;
        let snapshot = engine.snapshot();
        log.finish(at, "duration_elapsed", &snapshot, &shutdown)?;
        events.extend(shutdown);
        Ok((snapshot, events))
    }

    #[test]
    fn portfolio_replay_preserves_shared_cash_leverage_and_original_bytes() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("source.jsonl");
        let output = temp.path("derived.jsonl");
        let expected = complete(&input)?;
        let original = fs::read(&input)?;
        assert!(is_portfolio(&input)?);
        assert_eq!(expected.account.initial_balance, Decimal::new(100, 0));
        assert_eq!(expected.account.balance, Decimal::new(100, 0));
        assert_eq!(expected.leverage, 10);
        assert_eq!(replay(&input, &output)?, expected);
        assert_eq!(fs::read(&input)?, original);
        assert_eq!(replay(&output, &temp.path("again.jsonl"))?, expected);
        assert!(replay(&input, &output).is_err());
        Ok(())
    }

    #[test]
    fn portfolio_replay_rejects_tampering_truncation_and_missing_symbols_before_output() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let source = temp.path("source.jsonl");
        complete(&source)?;
        let original = fs::read_to_string(&source)?;
        let original_rows = original.lines().map(serde_json::from_str::<Value>).collect::<Result<Vec<_>, _>>()?;
        for case in 0..7 {
            let mut rows = original_rows.clone();
            match case {
                0 => { rows.pop(); }
                1 => { rows[0]["header"]["leverage"] = 11.into(); }
                2 => { rows[1]["candles"].as_array_mut().ok_or("bad fixture")?.pop(); }
                3 => { rows[1]["step"]["snapshot"]["account"]["balance"] = "200".into(); }
                4 => { rows[0]["header"].as_object_mut().ok_or("bad fixture")?.remove("leverage"); }
                5 => { rows[1]["candles"][0]["open"] = 100.into(); }
                _ => { rows[0]["header"]["margin_fraction"] = "1.12345678901234567890123456789".into(); }
            }
            let text = rows.iter().map(serde_json::to_string).collect::<Result<Vec<_>, _>>()?.join("\n") + "\n";
            let input = temp.path(&format!("bad-{case}.jsonl"));
            let output = temp.path(&format!("out-{case}.jsonl"));
            fs::write(&input, text)?;
            assert!(replay(&input, &output).is_err(), "case {case}");
            assert!(!output.exists());
        }
        let truncated = temp.path("truncated.jsonl");
        fs::write(&truncated, original.trim_end())?;
        assert!(replay(&truncated, &temp.path("never-created.jsonl")).is_err());
        assert!(decode(br#"{"record":"batch","record":"summary"}"#).is_err());
        assert!(decode(br#"{"record":"batch","candles":[],"warmup":false,"step":{"price":1.5}}"#).is_err());
        Ok(())
    }

    #[test]
    fn genuine_btc_then_eth_lifecycles_replay_one_shared_account_exactly() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("two-trades.jsonl");
        let output = temp.path("two-trades-replay.jsonl");
        let (expected, events) = complete_trade_session(&input, false)?;
        let placed: Vec<_> = events.iter().filter_map(|event| match event {
            PortfolioEvent::EntryPlaced { symbol, .. } => Some(symbol.as_str()),
            _ => None,
        }).collect();
        assert_eq!(placed, ["BTCUSDT", "ETHUSDT"]);
        assert_eq!(expected.account.initial_balance, Decimal::from(100));
        assert!(expected.account.balance > Decimal::from(100));
        assert_eq!(expected.account.completed_trades, 2);
        assert_eq!(expected.account.wins, 2);
        assert!(expected.account.fees_paid > Decimal::ZERO);
        assert!(expected.active_symbol.is_none());
        let original = fs::read(&input)?;
        assert_eq!(replay(&input, &output)?, expected);
        assert_eq!(replay(&output, &temp.path("again.jsonl"))?, expected);
        assert_eq!(fs::read(&input)?, original);
        Ok(())
    }

    #[test]
    fn same_bar_entry_and_stop_replay_both_events_with_original_geometry() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("entry-stop.jsonl");
        let (expected, events) = complete_trade_session(&input, true)?;
        let geometry: Vec<_> = events.iter().filter_map(|event| match event {
            PortfolioEvent::Execution { symbol, order, .. } => Some((symbol, order)),
            _ => None,
        }).collect();
        assert_eq!(geometry.len(), 2);
        assert_eq!(geometry[0], geometry[1]);
        assert_eq!(geometry[0].0, "BTCUSDT");
        assert_eq!(expected.account.completed_trades, 1);
        assert_eq!(expected.account.losses, 1);
        assert!(expected.account.balance < Decimal::from(100));
        assert_eq!(replay(&input, &temp.path("verified.jsonl"))?, expected);
        Ok(())
    }

    #[test]
    fn lifecycle_symbol_geometry_and_account_tampering_fail_before_output() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let source = temp.path("original.jsonl");
        complete_trade_session(&source, true)?;
        let original = fs::read_to_string(&source)?;
        let rows = original.lines().map(serde_json::from_str::<Value>).collect::<Result<Vec<_>, _>>()?;
        let lifecycle_index = rows.iter().position(|row| row["step"]["events"][0]["kind"] == "execution")
            .ok_or_else(|| invalid("fixture missing execution events"))?;
        for case in 0..5 {
            let mut changed = rows.clone();
            match case {
                0 => { changed[lifecycle_index]["step"]["events"][0]["symbol"] = "ETHUSDT".into(); }
                1 => { changed[lifecycle_index]["step"]["events"][1]["order"]["stop_loss_price"] = "1".into(); }
                2 => { changed[lifecycle_index]["step"]["events"][0]["order"].as_object_mut().ok_or("fixture object")?.remove("quantity"); }
                3 => { changed[lifecycle_index]["step"]["snapshot"]["account"]["completed_trades"] = 0.into(); }
                _ => { let summary = changed.last().cloned().ok_or("fixture summary")?; changed.push(summary); }
            }
            let encoded = changed.iter().map(serde_json::to_string).collect::<Result<Vec<_>, _>>()?.join("\n") + "\n";
            let input = temp.path(&format!("tampered-{case}.jsonl"));
            let output = temp.path(&format!("rejected-{case}.jsonl"));
            fs::write(&input, encoded)?;
            assert!(replay(&input, &output).is_err(), "case {case}");
            assert!(!output.exists());
        }
        assert_eq!(fs::read_to_string(&source)?, original);
        Ok(())
    }

    #[test]
    fn writer_rejects_unterminated_bytes_without_changing_the_log() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let path = temp.path("bounded.jsonl");
        let header = PortfolioHeader::new(&instruments()?, PaperConfig::default().with_leverage(10)?, Uuid::new_v4());
        let mut log = PortfolioLog::create(&path, &header)?;
        let original = fs::read(&path)?;
        assert!(log.write(b"{}").is_err());
        assert_eq!(fs::read(&path)?, original);
        assert!(PortfolioLog::create(&path, &header).is_err());
        assert_eq!(fs::read(&path)?, original);
        Ok(())
    }

    #[test]
    fn fatal_unlogged_batch_is_explicitly_aborted_and_never_replayed() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("aborted.jsonl");
        let output = temp.path("must-not-exist.jsonl");
        let header = PortfolioHeader::new(&instruments()?, PaperConfig::default().with_leverage(10)?, Uuid::new_v4());
        let mut engine = header.engine()?;
        let mut log = PortfolioLog::create(&input, &header)?;
        let first = batch(0)?;
        let step = engine.process_batch(&first)?;
        log.append(&first, false, &step)?;
        assert!(!engine.is_poisoned());
        let malformed = batch(60)?;
        assert!(engine.process_batch(&malformed[..1]).is_err());
        assert!(engine.is_poisoned());
        let at = DateTime::from_timestamp(120, 0).ok_or("bad test time")?;
        let events = engine.shutdown(at)?;
        assert!(!engine.is_poisoned());
        assert!(events.is_empty());
        let snapshot = engine.snapshot();
        log.abort(at, "engine_failure", &snapshot, &events)?;
        assert!(log.finish(at, "duration_elapsed", &snapshot, &events).is_err());
        assert!(log.abort(at, "again", &snapshot, &events).is_err());
        let bytes = fs::read_to_string(&input)?;
        let final_line = bytes.lines().last().ok_or("missing abort record")?;
        assert!(matches!(decode(final_line.as_bytes())?, Record::Aborted { .. }));
        let error = replay(&input, &output).err().ok_or("aborted replay unexpectedly accepted")?;
        assert!(error.to_string().contains("diagnostic-only"));
        assert!(!output.exists());
        Ok(())
    }

    #[test]
    fn abort_rejects_invalid_metadata_without_closing_the_writer() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("abort-validation.jsonl");
        let header = PortfolioHeader::new(&instruments()?, PaperConfig::default().with_leverage(10)?, Uuid::new_v4());
        let mut engine = header.engine()?;
        let mut log = PortfolioLog::create(&input, &header)?;
        let candles = batch(0)?;
        let step = engine.process_batch(&candles)?;
        log.append(&candles, false, &step)?;
        let before = fs::read(&input)?;
        let at = DateTime::from_timestamp(60, 0).ok_or("bad test time")?;
        assert!(log.abort(at - chrono::Duration::seconds(1), "engine_failure", &engine.snapshot(), &[]).is_err());
        assert!(log.abort(at, "", &engine.snapshot(), &[]).is_err());
        assert_eq!(fs::read(&input)?, before);
        log.abort(at, "engine_failure", &engine.snapshot(), &[])?;
        assert!(replay(&input, &temp.path("refused.jsonl")).is_err());
        assert!(!temp.path("refused.jsonl").exists());
        Ok(())
    }
}
