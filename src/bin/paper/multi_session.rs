//! Version 3 audit for one shared paper account with multiple active orders.
//!
//! Legacy v1/v2 readers and engines remain unchanged. Complete replay verifies
//! every batch and final accounting before creating an output file. An aborted
//! journal is diagnostic evidence, never a verified replay or a resume file.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Write},
    path::Path,
};

use chrono::{DateTime, Utc};
use ict_trading_bot::{
    paper::{
        MultiPortfolio, MultiPortfolioSnapshot, MultiPortfolioStep, PaperConfig, PaperInstrument,
        PortfolioEvent,
    },
    Candle, Timeframe,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::session::{
    decimal, invalid, next_line, parse_candle, validate_summary, BoxError, InstrumentDto, UniqueValue,
};

const ALGORITHM: &str = "ict-paper-portfolio-v3";
const MAX_BYTES: u64 = 256 * 1024 * 1024;
const MAX_LINE: usize = 64 * 1024;
const MAX_BATCHES: usize = 100_000;
const MAX_CANDLES_PER_BATCH: usize = 2;
const MAX_ACTIVE_TOTAL: u32 = 4;
const MAX_ACTIVE_PER_SYMBOL: u32 = 2;

/// Fully specified assumptions for the fixed v3 multi-order algorithm.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiHeader {
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
    max_active_total: u32,
    max_active_per_symbol: u32,
    risk_per_trade: String,
    max_total_risk: String,
    daily_drawdown_limit: String,
}

impl MultiHeader {
    /// Captures instrument metadata, costs and the immutable v3 risk policy.
    pub fn new(instruments: &[PaperInstrument], config: PaperConfig, session_id: Uuid) -> Self {
        Self {
            version: 3,
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
            max_active_total: MAX_ACTIVE_TOTAL,
            max_active_per_symbol: MAX_ACTIVE_PER_SYMBOL,
            risk_per_trade: "0.01".to_owned(),
            max_total_risk: "0.03".to_owned(),
            daily_drawdown_limit: "0.05".to_owned(),
        }
    }

    /// Restores a fresh deterministic engine, never live positions or secrets.
    ///
    /// # Errors
    /// Rejects unsupported versions/policy, malformed decimals or metadata,
    /// invalid simulation assumptions, and an invalid instrument universe.
    pub fn engine(&self) -> Result<MultiPortfolio, BoxError> {
        if self.version != 3 || self.algorithm != ALGORITHM || self.session_id.is_nil() {
            return Err(invalid("unsupported multi-order session header"));
        }
        if self.max_active_total != MAX_ACTIVE_TOTAL
            || self.max_active_per_symbol != MAX_ACTIVE_PER_SYMBOL
            || decimal(&self.risk_per_trade)? != Decimal::new(1, 2)
            || decimal(&self.max_total_risk)? != Decimal::new(3, 2)
            || decimal(&self.daily_drawdown_limit)? != Decimal::new(5, 2)
        {
            return Err(invalid("unsupported multi-order session risk policy"));
        }
        let config = PaperConfig::new(
            decimal(&self.initial_balance)?,
            decimal(&self.fee_rate)?,
            decimal(&self.stop_slippage_bps)?,
            decimal(&self.margin_fraction)?,
            decimal(&self.min_net_rr)?,
        )?
        .with_leverage(self.leverage)?;
        let instruments = self
            .instruments
            .iter()
            .map(InstrumentDto::restore)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MultiPortfolio::new(instruments, config, self.timeframe)?)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header { header: Box<MultiHeader> },
    Batch { candles: Vec<Value>, warmup: bool, step: Value },
    Summary { at: DateTime<Utc>, reason: String, snapshot: Value, events: Value },
    Aborted { at: DateTime<Utc>, reason: String, snapshot: Value, events: Value },
}

/// Append-only bounded journal; each successful append is flushed and synced.
pub struct MultiLog {
    file: File,
    bytes: u64,
    batches: usize,
    last_close: Option<DateTime<Utc>>,
    finished: bool,
    poisoned: bool,
}

impl MultiLog {
    /// Creates a new journal without replacing any existing file.
    ///
    /// # Errors
    /// Rejects an invalid header, an existing destination or a filesystem error.
    pub fn create(path: &Path, header: &MultiHeader) -> Result<Self, BoxError> {
        header.engine()?;
        let bytes = encode(&Record::Header { header: Box::new(header.clone()) })?;
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let mut log = Self {
            file: OpenOptions::new().write(true).create_new(true).open(path)?,
            bytes: 0,
            batches: 0,
            last_close: None,
            finished: false,
            poisoned: false,
        };
        log.write(&bytes)?;
        Ok(log)
    }

    /// Records one complete market batch and its already-computed outcome.
    ///
    /// # Errors
    /// Rejects empty/oversized batches, journal limits or a closed/failed log.
    pub fn append(
        &mut self,
        candles: &[Candle],
        warmup: bool,
        step: &MultiPortfolioStep,
    ) -> Result<(), BoxError> {
        if self.batches >= MAX_BATCHES
            || candles.is_empty()
            || candles.len() > MAX_CANDLES_PER_BATCH
        {
            return Err(invalid("multi-order session batch limit or shape invalid"));
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

    /// Records a normal shutdown outcome, including any open paper positions.
    ///
    /// # Errors
    /// Rejects invalid shutdown metadata, limits or a closed/failed log.
    pub fn finish(
        &mut self,
        at: DateTime<Utc>,
        reason: &str,
        snapshot: &MultiPortfolioSnapshot,
        events: &[PortfolioEvent],
    ) -> Result<(), BoxError> {
        validate_summary(at, reason, self.last_close)?;
        self.write(&encode(&Record::Summary {
            at,
            reason: reason.to_owned(),
            snapshot: serde_json::to_value(snapshot)?,
            events: serde_json::to_value(events)?,
        })?)?;
        self.finished = true;
        Ok(())
    }

    /// Stores diagnostic-only evidence after a fatal engine/shutdown failure.
    ///
    /// A failed or partially applied transition may lack a reproducible batch.
    /// Supply a sanitized reason label, never a credential-bearing error body.
    ///
    /// # Errors
    /// Rejects invalid shutdown metadata, limits or a closed/failed log.
    pub fn abort(
        &mut self,
        at: DateTime<Utc>,
        reason: &str,
        snapshot: &MultiPortfolioSnapshot,
        events: &[PortfolioEvent],
    ) -> Result<(), BoxError> {
        validate_summary(at, reason, self.last_close)?;
        self.write(&encode(&Record::Aborted {
            at,
            reason: reason.to_owned(),
            snapshot: serde_json::to_value(snapshot)?,
            events: serde_json::to_value(events)?,
        })?)?;
        self.finished = true;
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), BoxError> {
        let count = u64::try_from(bytes.len())?;
        if self.finished
            || self.poisoned
            || bytes.len() > MAX_LINE
            || bytes.last() != Some(&b'\n')
            || count > MAX_BYTES.saturating_sub(self.bytes)
        {
            return Err(invalid("multi-order log is closed, poisoned, or exceeds bounds"));
        }
        // A partial write or failed sync must never be followed by more records.
        self.poisoned = true;
        self.file.write_all(bytes)?;
        self.file.flush()?;
        self.file.sync_data()?;
        self.bytes += count;
        self.poisoned = false;
        Ok(())
    }
}

/// Identifies v3 without interpreting it as a legacy one-position session.
///
/// # Errors
/// Rejects unreadable, oversized, truncated or duplicate-key first records.
pub fn is_multi(input: &Path) -> Result<bool, BoxError> {
    let mut reader = BufReader::new(File::open(input)?);
    let first = next_line(&mut reader, &mut 0)?.ok_or_else(|| invalid("empty paper session"))?;
    let value = serde_json::from_slice::<UniqueValue>(&first)?.0;
    Ok(value.get("header").and_then(|header| header.get("algorithm")).and_then(Value::as_str)
        == Some(ALGORITHM))
}

/// Verifies all deterministic outcomes before creating a derived v3 journal.
///
/// # Errors
/// Rejects unsupported/tampered/aborted/incomplete sessions, input bounds,
/// irreproducible accounting and output filesystem errors. Existing output is
/// never overwritten. This is offline verification, not live-session resume.
pub fn replay(input: &Path, output: &Path) -> Result<MultiPortfolioSnapshot, BoxError> {
    let file = File::open(input)?;
    if file.metadata()?.len() > MAX_BYTES {
        return Err(invalid("multi-order replay exceeds file limit"));
    }
    let mut reader = BufReader::new(file);
    let mut total = 0;
    let first = next_line(&mut reader, &mut total)?.ok_or_else(|| invalid("empty multi-order session"))?;
    let Record::Header { header } = decode(&first)? else {
        return Err(invalid("multi-order session must start with header"));
    };
    let mut engine = header.engine()?;
    let mut tail = Vec::new();
    let mut result = None;
    let mut batches = 0;
    let mut last_close = None;
    while let Some(line) = next_line(&mut reader, &mut total)? {
        if result.is_some() {
            return Err(invalid("records after multi-order summary"));
        }
        match decode(&line)? {
            Record::Header { .. } => return Err(invalid("repeated multi-order header")),
            Record::Aborted { .. } => {
                return Err(invalid("aborted multi-order session is diagnostic-only and cannot be replayed"));
            }
            Record::Batch { candles, warmup, step } => {
                batches += 1;
                if batches > MAX_BATCHES
                    || candles.is_empty()
                    || candles.len() > MAX_CANDLES_PER_BATCH
                {
                    return Err(invalid("multi-order replay batch limit or shape invalid"));
                }
                let candles = candles.iter().map(parse_candle).collect::<Result<Vec<_>, _>>()?;
                let actual = if warmup {
                    engine.warm_up(&candles)?
                } else {
                    engine.process_batch(&candles)?
                };
                if serde_json::to_value(actual)? != step {
                    return Err(invalid("multi-order batch outcome does not reproduce"));
                }
                last_close = candles.iter().map(Candle::close_time).chain(last_close).max();
            }
            Record::Summary { at, reason, snapshot, events } => {
                validate_summary(at, &reason, last_close)?;
                let actual_events = engine.shutdown(at)?;
                let actual = engine.snapshot();
                if serde_json::to_value(&actual)? != snapshot
                    || serde_json::to_value(actual_events)? != events
                {
                    return Err(invalid("multi-order final accounting does not reproduce"));
                }
                result = Some(actual);
            }
        }
        tail.extend_from_slice(&line);
    }
    let snapshot = result.ok_or_else(|| invalid("multi-order session missing final summary"))?;
    let header_bytes = encode(&Record::Header { header: header.clone() })?;
    if u64::try_from(header_bytes.len())? + u64::try_from(tail.len())? > MAX_BYTES {
        return Err(invalid("derived multi-order session exceeds file limit"));
    }
    let mut log = MultiLog::create(output, &header)?;
    for line in tail.split_inclusive(|byte| *byte == b'\n') {
        log.write(line)?;
    }
    Ok(snapshot)
}

fn encode(record: &Record) -> Result<Vec<u8>, BoxError> {
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_LINE {
        return Err(invalid("multi-order record exceeds line limit"));
    }
    Ok(bytes)
}

fn decode(line: &[u8]) -> Result<Record, BoxError> {
    Ok(serde_json::from_value(serde_json::from_slice::<UniqueValue>(line)?.0)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ict_trading_bot::{CandleInput, Exchange, InstrumentId, InstrumentRiskSpec, MarketKind, VolumeUnit};
    use std::path::PathBuf;

    struct Temp(PathBuf);

    impl Temp {
        fn new() -> Result<Self, BoxError> {
            let path = std::env::temp_dir().join(format!("ict-multi-replay-{}", Uuid::new_v4()));
            fs::create_dir(&path)?;
            Ok(Self(path))
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn instruments() -> Result<Vec<PaperInstrument>, BoxError> {
        ["BTC", "ETH"].iter().map(|asset| {
            Ok(PaperInstrument {
                spec: InstrumentRiskSpec::new(
                    InstrumentId::new(
                        Exchange::Bybit,
                        MarketKind::LinearPerpetual,
                        format!("{asset}USDT"),
                        format!("{asset}-USDT-PERP"),
                    )?,
                    Decimal::new(1, 1),
                    Decimal::new(1, 3),
                    Decimal::new(1, 3),
                    None,
                    Decimal::ONE,
                )?,
                min_notional: Decimal::ONE,
            })
        }).collect()
    }

    fn header() -> Result<MultiHeader, BoxError> {
        Ok(MultiHeader::new(
            &instruments()?,
            PaperConfig::default().with_leverage(10)?,
            Uuid::new_v4(),
        ))
    }

    fn at(seconds: i64) -> Result<DateTime<Utc>, BoxError> {
        DateTime::from_timestamp(seconds, 0).ok_or_else(|| invalid("bad fixture time"))
    }

    fn batch(seconds: i64) -> Result<Vec<Candle>, BoxError> {
        instruments()?.iter().map(|instrument| {
            Ok(Candle::new(CandleInput {
                instrument: instrument.spec.instrument().clone(),
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
        }).collect()
    }

    fn complete(path: &Path) -> Result<MultiPortfolioSnapshot, BoxError> {
        let header = header()?;
        let mut engine = header.engine()?;
        let mut log = MultiLog::create(path, &header)?;
        for (seconds, warmup) in [(0, true), (60, true), (120, false)] {
            let candles = batch(seconds)?;
            let step = if warmup {
                engine.warm_up(&candles)?
            } else {
                engine.process_batch(&candles)?
            };
            log.append(&candles, warmup, &step)?;
        }
        let events = engine.shutdown(at(181)?)?;
        let snapshot = engine.snapshot();
        log.finish(at(181)?, "duration_elapsed", &snapshot, &events)?;
        Ok(snapshot)
    }

    fn records(path: &Path) -> Result<Vec<Value>, BoxError> {
        fs::read_to_string(path)?.lines().map(|line| Ok(serde_json::from_str(line)?)).collect()
    }

    fn store_records(path: &Path, records: &[Value]) -> Result<(), BoxError> {
        let mut bytes = Vec::new();
        for record in records {
            serde_json::to_writer(&mut bytes, record)?;
            bytes.push(b'\n');
        }
        fs::write(path, bytes)?;
        Ok(())
    }

    fn trade_prices() -> Vec<[i64; 4]> {
        // Real five-candle fractals and default 20-body displacement confirm
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
        // Wednesday 07:00 New York daylight time, inside the default killzone.
        DateTime::parse_from_rfc3339("2026-10-28T11:00:00Z")?
            .with_timezone(&Utc)
            .checked_add_signed(chrono::Duration::minutes(minute))
            .ok_or_else(|| invalid("test timestamp overflow"))
    }

    fn trade_batch(minute: i64, btc: [i64; 4], eth: [i64; 4]) -> Result<Vec<Candle>, BoxError> {
        instruments()?.into_iter().zip([btc, eth]).map(|(instrument, prices)| {
            Ok(Candle::new(CandleInput {
                instrument: instrument.spec.instrument().clone(),
                timeframe: Timeframe::M1,
                open_time: trade_at(minute)?,
                close_time: trade_at(minute + 1)?,
                open: Decimal::from(prices[0]),
                high: Decimal::from(prices[1]),
                low: Decimal::from(prices[2]),
                close: Decimal::from(prices[3]),
                volume: Decimal::ONE,
                volume_unit: VolumeUnit::BaseAsset,
                trade_count: None,
                is_closed: true,
            })?)
        }).collect()
    }

    fn append_trade_batch(
        log: &mut MultiLog,
        engine: &mut MultiPortfolio,
        candles: &[Candle],
        warmup: bool,
    ) -> Result<Vec<PortfolioEvent>, BoxError> {
        let step = if warmup { engine.warm_up(candles)? } else { engine.process_batch(candles)? };
        log.append(candles, warmup, &step)?;
        Ok(step.events)
    }

    fn complete_trade_session(path: &Path) -> Result<(MultiPortfolioSnapshot, Vec<PortfolioEvent>), BoxError> {
        let header = header()?;
        let mut engine = header.engine()?;
        let mut log = MultiLog::create(path, &header)?;
        let mut events = Vec::new();
        for (minute, prices) in trade_prices().into_iter().enumerate() {
            events.extend(append_trade_batch(
                &mut log,
                &mut engine,
                &trade_batch(i64::try_from(minute)?, prices, prices)?,
                minute < 24,
            )?);
        }
        // Both symbols fill together; BTC reaches TP while ETH stays open,
        // then ETH stops out on its own candle. No synthetic approval shortcut.
        for (minute, btc, eth) in [
            (25, [109, 110, 103, 109], [109, 110, 103, 109]),
            (26, [109, 151, 109, 150], [109, 110, 103, 109]),
            (27, [150, 151, 149, 150], [109, 110, 80, 100]),
        ] {
            events.extend(append_trade_batch(
                &mut log,
                &mut engine,
                &trade_batch(minute, btc, eth)?,
                false,
            )?);
        }
        let shutdown = engine.shutdown(trade_at(28)?)?;
        let snapshot = engine.snapshot();
        log.finish(trade_at(28)?, "duration_elapsed", &snapshot, &shutdown)?;
        events.extend(shutdown);
        Ok((snapshot, events))
    }

    #[test]
    fn simultaneous_orders_and_independent_exits_replay_exactly() -> Result<(), BoxError> {
        use ict_trading_bot::paper::{PaperEvent, PaperExitReason};

        let temp = Temp::new()?;
        let input = temp.path("multi-trades.jsonl");
        let output = temp.path("multi-trades-verified.jsonl");
        let (expected, events) = complete_trade_session(&input)?;
        let placed: Vec<_> = events.iter().filter_map(|event| match event {
            PortfolioEvent::EntryPlaced { symbol, at, order } => Some((symbol, at, order)),
            _ => None,
        }).collect();
        assert_eq!(placed.len(), 2);
        assert_eq!(placed[0].0, "BTCUSDT");
        assert_eq!(placed[1].0, "ETHUSDT");
        assert_eq!(placed[0].1, placed[1].1);
        assert_ne!(placed[0].2.intent_id, placed[1].2.intent_id);
        let fills: Vec<_> = events.iter().filter_map(|event| match event {
            PortfolioEvent::Execution { symbol, event: PaperEvent::EntryFilled { at, .. }, order } => {
                Some((symbol, at, order))
            }
            _ => None,
        }).collect();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].0, placed[0].0);
        assert_eq!(fills[1].0, placed[1].0);
        assert_eq!(fills[0].1, fills[1].1);
        assert_eq!(fills[0].2, placed[0].2);
        assert_eq!(fills[1].2, placed[1].2);
        let exits: Vec<_> = events.iter().filter_map(|event| match event {
            PortfolioEvent::Execution {
                symbol, event: PaperEvent::PositionClosed { at, reason, .. }, order,
            } => Some((symbol, at, reason, order)),
            _ => None,
        }).collect();
        assert_eq!(exits.len(), 2);
        assert_eq!(exits[0].0, "BTCUSDT");
        assert_eq!(*exits[0].2, PaperExitReason::TakeProfit);
        assert_eq!(exits[1].0, "ETHUSDT");
        assert_eq!(*exits[1].2, PaperExitReason::StopLoss);
        assert!(exits[0].1 < exits[1].1);
        assert_eq!(exits[0].3, placed[0].2);
        assert_eq!(exits[1].3, placed[1].2);
        assert_eq!(expected.account.completed_trades, 2);
        assert_eq!(expected.account.wins, 1);
        assert_eq!(expected.account.losses, 1);
        assert_eq!(expected.account.initial_balance, Decimal::from(100));
        assert_eq!(expected.account.leverage, 10);
        assert!(expected.account.fees_paid > Decimal::ZERO);
        assert!(expected.account.orders.is_empty());
        assert_eq!(expected.account.reserved_risk, Decimal::ZERO);
        assert_eq!(expected.account.reserved_capital, Decimal::ZERO);
        let net_profit = events.iter().try_fold(Decimal::ZERO, |total, event| {
            if let PortfolioEvent::Execution {
                event: PaperEvent::PositionClosed { net_pnl, .. }, ..
            } = event {
                total.checked_add(*net_pnl).ok_or("fixture profit overflow")
            } else {
                Ok(total)
            }
        })?;
        assert_eq!(expected.account.balance,
            Decimal::from(100).checked_add(net_profit).ok_or("fixture balance overflow")?);

        let rows = records(&input)?;
        let placed_index = rows.iter().position(|record| {
            record["step"]["events"][0]["kind"] == "entry_placed"
                && record["step"]["events"][1]["kind"] == "entry_placed"
        }).ok_or("fixture missing simultaneous placements")?;
        let pending = &rows[placed_index]["step"]["snapshot"]["account"];
        let orders = pending["orders"].as_array().ok_or("fixture orders missing")?;
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0]["symbol"], "BTCUSDT");
        assert_eq!(orders[1]["symbol"], "ETHUSDT");
        let mut risk = Decimal::ZERO;
        let mut capital = Decimal::ZERO;
        for order in orders {
            assert_eq!(order["state"], "pending");
            let reserved_risk = decimal(order["reserved_risk"].as_str().ok_or("risk missing")?)?;
            let reserved_capital = decimal(order["reserved_capital"].as_str().ok_or("capital missing")?)?;
            assert!(reserved_risk > Decimal::ZERO && reserved_risk <= Decimal::ONE);
            assert!(reserved_capital > Decimal::ZERO);
            risk = risk.checked_add(reserved_risk).ok_or("fixture risk overflow")?;
            capital = capital.checked_add(reserved_capital).ok_or("fixture capital overflow")?;
        }
        assert!(risk <= Decimal::from(3));
        assert_eq!(risk, decimal(pending["reserved_risk"].as_str().ok_or("total risk missing")?)?);
        assert_eq!(capital, decimal(pending["reserved_capital"].as_str().ok_or("total capital missing")?)?);
        let both_open = &rows[placed_index + 1]["step"]["snapshot"]["account"]["orders"];
        assert_eq!(both_open.as_array().ok_or("open orders missing")?.len(), 2);
        assert_eq!(both_open[0]["state"], "open");
        assert_eq!(both_open[1]["state"], "open");
        let remaining = &rows[placed_index + 2]["step"]["snapshot"]["account"]["orders"];
        assert_eq!(remaining.as_array().ok_or("remaining orders missing")?.len(), 1);
        assert_eq!(remaining[0]["symbol"], "ETHUSDT");
        assert_eq!(remaining[0]["state"], "open");
        assert_eq!(remaining[0]["order"], orders[1]["order"]);
        let original = fs::read(&input)?;
        assert_eq!(serde_json::to_value(replay(&input, &output)?)?, serde_json::to_value(&expected)?);
        assert_eq!(serde_json::to_value(replay(&output, &temp.path("reverified.jsonl"))?)?, serde_json::to_value(&expected)?);
        assert_eq!(original, fs::read(&input)?);
        assert_eq!(original, fs::read(&output)?);
        Ok(())
    }

    #[test]
    fn single_order_geometry_identity_and_shared_cash_tampering_reject_before_output() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let source = temp.path("multi-trades.jsonl");
        complete_trade_session(&source)?;
        let original = records(&source)?;
        let placed_index = original.iter().position(|record| {
            record["step"]["events"][0]["kind"] == "entry_placed"
                && record["step"]["events"][1]["kind"] == "entry_placed"
        }).ok_or("fixture missing simultaneous placements")?;
        for index in 0..9 {
            let mut changed = original.clone();
            match index {
                0 => changed[placed_index]["step"]["events"][1]["order"]["intent_id"] = Value::from(Uuid::nil().to_string()),
                1 => changed[placed_index]["step"]["events"][1]["order"]["take_profit_price"] = Value::from("151"),
                2 => {
                    changed[placed_index]["step"]["events"].as_array_mut()
                        .ok_or("fixture events missing")?.pop();
                }
                3 => changed[placed_index]["step"]["snapshot"]["account"]["balance"] = Value::from("200"),
                4 => changed[placed_index]["step"]["snapshot"]["halted"] = Value::from(true),
                5 => changed[placed_index]["step"]["snapshot"]["account"]["orders"][1]["state"] = Value::from("open"),
                6 => {
                    changed[placed_index]["step"]["snapshot"]["account"]["orders"].as_array_mut()
                        .ok_or("fixture orders missing")?.pop();
                }
                7 => changed[placed_index]["step"]["snapshot"]["account"]["orders"][1]["reserved_risk"] = Value::from("0"),
                _ => changed[placed_index]["step"]["snapshot"]["account"]["orders"][1]["order"]["take_profit_price"] = Value::from("151"),
            }
            let input = temp.path(&format!("tampered-order-{index}.jsonl"));
            let output = temp.path(&format!("rejected-order-{index}.jsonl"));
            store_records(&input, &changed)?;
            assert!(replay(&input, &output).is_err(), "accepted order mutation {index}");
            assert!(!output.exists());
        }
        Ok(())
    }

    #[test]
    fn complete_v3_replay_is_exact_and_does_not_overwrite() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("source.jsonl");
        let output = temp.path("verified.jsonl");
        let expected = complete(&input)?;
        assert!(is_multi(&input)?);
        let actual = replay(&input, &output)?;
        assert_eq!(serde_json::to_value(actual)?, serde_json::to_value(expected)?);
        let source = fs::read(&input)?;
        assert_eq!(source, fs::read(&output)?);
        assert!(replay(&input, &output).is_err());
        assert!(replay(&input, &input).is_err());
        assert_eq!(source, fs::read(&input)?);
        assert_eq!(source, fs::read(&output)?);
        Ok(())
    }

    #[test]
    fn rejects_changed_version_algorithm_and_every_fixed_risk_limit() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let source = temp.path("source.jsonl");
        complete(&source)?;
        let original = records(&source)?;
        for (index, (field, value)) in [
            ("version", Value::from(2)),
            ("algorithm", Value::from("ict-paper-portfolio-v2")),
            ("session_id", Value::from(Uuid::nil().to_string())),
            ("max_active_total", Value::from(5)),
            ("max_active_per_symbol", Value::from(3)),
            ("risk_per_trade", Value::from("0.02")),
            ("max_total_risk", Value::from("0.04")),
            ("daily_drawdown_limit", Value::from("0.06")),
            ("risk_per_trade", Value::from("1e-2")),
            ("max_total_risk", Value::from(3)),
            ("daily_drawdown_limit", Value::Null),
            ("leverage", Value::from(11)),
            ("unknown_policy", Value::from(true)),
        ].into_iter().enumerate() {
            let mut changed = original.clone();
            changed[0]["header"][field] = value;
            let input = temp.path(&format!("tampered-{index}.jsonl"));
            let output = temp.path(&format!("unexpected-{index}.jsonl"));
            store_records(&input, &changed)?;
            assert!(replay(&input, &output).is_err(), "accepted {field}");
            assert!(!output.exists());
        }
        let encoded = serde_json::to_value(header()?)?;
        assert_eq!(encoded["max_active_total"], 4);
        assert_eq!(encoded["max_active_per_symbol"], 2);
        assert_eq!(encoded["risk_per_trade"], "0.01");
        assert_eq!(encoded["max_total_risk"], "0.03");
        assert_eq!(encoded["daily_drawdown_limit"], "0.05");
        Ok(())
    }

    #[test]
    fn rejects_missing_limits_duplicate_keys_and_float_decimals() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let source = temp.path("source.jsonl");
        complete(&source)?;
        let original = records(&source)?;
        let mut missing = original.clone();
        missing[0]["header"].as_object_mut().ok_or("fixture header is not an object")?
            .remove("max_active_total");
        let input = temp.path("missing-limit.jsonl");
        let output = temp.path("missing-output.jsonl");
        store_records(&input, &missing)?;
        assert!(replay(&input, &output).is_err());
        assert!(!output.exists());

        let raw = fs::read_to_string(&source)?;
        for (index, (from, to)) in [
            ("\"max_active_total\":4", "\"max_active_total\":4,\"max_active_total\":4"),
            ("\"halted\":false", "\"halted\":false,\"halted\":false"),
            ("\"risk_per_trade\":\"0.01\"", "\"risk_per_trade\":0.01"),
        ].into_iter().enumerate() {
            let changed = raw.replacen(from, to, 1);
            assert_ne!(raw, changed, "fixture did not contain {from}");
            let input = temp.path(&format!("duplicate-or-float-{index}.jsonl"));
            let output = temp.path(&format!("duplicate-output-{index}.jsonl"));
            fs::write(&input, changed)?;
            assert!(replay(&input, &output).is_err());
            assert!(!output.exists());
        }
        Ok(())
    }

    #[test]
    fn rejects_tampered_batch_summary_and_terminal_structure() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let source = temp.path("source.jsonl");
        complete(&source)?;
        let original = records(&source)?;
        for index in 0..8 {
            let mut changed = original.clone();
            match index {
                0 => changed[1]["step"]["snapshot"]["halted"] = Value::from(true),
                1 => changed[4]["snapshot"]["halted"] = Value::from(false),
                2 => changed[4]["events"] = Value::Array(Vec::new()),
                3 => { changed.pop(); }
                4 => changed.push(original[4].clone()),
                5 => changed.insert(1, original[0].clone()),
                6 => changed[1]["candles"] = Value::Array(Vec::new()),
                _ => {
                    let candle = original[1]["candles"][0].clone();
                    changed[1]["candles"] = Value::Array(vec![candle.clone(), candle.clone(), candle]);
                }
            }
            let input = temp.path(&format!("tampered-outcome-{index}.jsonl"));
            let output = temp.path(&format!("outcome-output-{index}.jsonl"));
            store_records(&input, &changed)?;
            assert!(replay(&input, &output).is_err(), "accepted mutation {index}");
            assert!(!output.exists());
        }
        let input = temp.path("truncated.jsonl");
        let output = temp.path("truncated-output.jsonl");
        let mut bytes = fs::read(&source)?;
        bytes.pop();
        fs::write(&input, bytes)?;
        assert!(replay(&input, &output).is_err());
        assert!(!output.exists());
        Ok(())
    }

    #[test]
    fn aborted_session_is_diagnostic_only_and_closes_writer() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("aborted.jsonl");
        let output = temp.path("unexpected.jsonl");
        let header = header()?;
        let mut engine = header.engine()?;
        let mut log = MultiLog::create(&input, &header)?;
        let candles = batch(0)?;
        let step = engine.warm_up(&candles)?;
        log.append(&candles, true, &step)?;
        let events = engine.shutdown(at(61)?)?;
        let snapshot = engine.snapshot();
        log.abort(at(61)?, "engine_failure", &snapshot, &events)?;
        assert!(log.append(&candles, true, &step).is_err());
        assert!(log.finish(at(61)?, "duration_elapsed", &snapshot, &events).is_err());
        assert!(is_multi(&input)?);
        let error = replay(&input, &output).err().ok_or("aborted replay succeeded")?;
        assert!(error.to_string().contains("diagnostic-only"));
        assert!(!output.exists());
        Ok(())
    }

    #[test]
    fn writer_enforces_line_file_batch_and_poison_bounds() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let header = header()?;
        let mut engine = header.engine()?;
        let mut log = MultiLog::create(&temp.path("bounded.jsonl"), &header)?;
        let candles = batch(0)?;
        let step = engine.warm_up(&candles)?;
        assert!(log.append(&[], true, &step).is_err());
        let too_many = vec![candles[0].clone(), candles[1].clone(), candles[0].clone()];
        assert!(log.append(&too_many, true, &step).is_err());
        log.batches = MAX_BATCHES;
        assert!(log.append(&candles, true, &step).is_err());
        log.batches = 0;
        log.poisoned = true;
        assert!(log.append(&candles, true, &step).is_err());
        log.poisoned = false;
        let previous_bytes = log.bytes;
        log.bytes = MAX_BYTES;
        assert!(log.append(&candles, true, &step).is_err());
        log.bytes = previous_bytes;
        assert!(log.write(&vec![b'\n'; MAX_LINE + 1]).is_err());
        assert!(log.write(b"{}").is_err());
        let oversized = Record::Batch {
            candles: vec![Value::String("x".repeat(MAX_LINE))],
            warmup: true,
            step: Value::Null,
        };
        assert!(encode(&oversized).is_err());
        // Rejections before I/O do not poison a usable journal.
        log.append(&candles, true, &step)?;
        assert_eq!(log.batches, 1);
        Ok(())
    }

    #[test]
    fn reader_enforces_file_and_line_bounds_before_output() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("too-large.jsonl");
        let output = temp.path("unexpected.jsonl");
        File::create(&input)?.set_len(MAX_BYTES + 1)?;
        assert!(replay(&input, &output).is_err());
        assert!(!output.exists());
        let mut bytes = encode(&Record::Header { header: Box::new(header()?) })?;
        bytes.extend(vec![b' '; MAX_LINE]);
        bytes.push(b'\n');
        let input = temp.path("long-line.jsonl");
        fs::write(&input, bytes)?;
        assert!(replay(&input, &output).is_err());
        assert!(!output.exists());
        Ok(())
    }
}
