//! Bounded JSONL paper-session audit and deterministic offline replay.
//!
//! Replay checks consistency, not authenticity: the file is not a signed market
//! record. Original input is never changed, and output is created only after the
//! complete input (including shutdown) has reproduced exactly.

use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    path::Path,
};

use chrono::{DateTime, Utc};
use ict_trading_bot::{
    paper::{PaperConfig, PaperEngine, PaperEvent, PaperInstrument, PaperSnapshot, PaperStep},
    Candle, Exchange, InstrumentId, InstrumentRiskSpec, MarketKind, Timeframe,
};
use rust_decimal::Decimal;
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// Shared error boundary for the binary's synchronous file operations.
pub type BoxError = Box<dyn Error + Send + Sync>;
const SCHEMA_VERSION: u16 = 1;
const ALGORITHM: &str = "ict-paper-v1";
const MAX_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_LINE_BYTES: usize = 64 * 1024;
const MAX_CANDLES: usize = 100_000;

/// Complete immutable assumptions needed to reconstruct this session.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    version: u16,
    algorithm: String,
    started_at: DateTime<Utc>,
    mode: String,
    timeframe: Timeframe,
    instrument: InstrumentDto,
    config: ConfigDto,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentDto {
    exchange: Exchange,
    market: MarketKind,
    venue_symbol: String,
    canonical_symbol: String,
    price_tick: String,
    quantity_step: String,
    minimum_quantity: String,
    #[serde(deserialize_with = "required_option")]
    maximum_quantity: Option<String>,
    quote_value_per_price_unit: String,
    min_notional: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigDto {
    initial_balance: String,
    fee_rate: String,
    stop_slippage_bps: String,
    max_notional_fraction: String,
    min_net_rr: String,
}

impl Header {
    /// Captures validated public metadata and explicit simulation assumptions.
    #[must_use]
    pub fn new(
        instrument: &PaperInstrument,
        config: PaperConfig,
        timeframe: Timeframe,
        started_at: DateTime<Utc>,
        mode: &str,
    ) -> Self {
        let spec = &instrument.spec;
        Self {
            version: SCHEMA_VERSION,
            algorithm: ALGORITHM.to_owned(),
            started_at,
            mode: mode.to_owned(),
            timeframe,
            instrument: InstrumentDto {
                exchange: spec.instrument().exchange,
                market: spec.instrument().market,
                venue_symbol: spec.instrument().venue_symbol.to_string(),
                canonical_symbol: spec.instrument().canonical_symbol.to_string(),
                price_tick: spec.price_tick().to_string(),
                quantity_step: spec.quantity_step().to_string(),
                minimum_quantity: spec.minimum_quantity().to_string(),
                maximum_quantity: spec.maximum_quantity().map(|value| value.to_string()),
                quote_value_per_price_unit: spec.quote_value_per_price_unit().to_string(),
                min_notional: instrument.min_notional.to_string(),
            },
            config: ConfigDto {
                initial_balance: config.initial_balance().to_string(),
                fee_rate: config.fee_rate().to_string(),
                stop_slippage_bps: config.stop_slippage_bps().to_string(),
                max_notional_fraction: config.max_notional_fraction().to_string(),
                min_net_rr: config.min_net_rr().to_string(),
            },
        }
    }

    /// Reconstructs validated assumptions; no network or credentials are used.
    ///
    /// # Errors
    /// Rejects unknown revisions, invalid identity/configuration, or inexact decimals.
    pub fn engine(&self) -> Result<PaperEngine, BoxError> {
        if self.version != SCHEMA_VERSION
            || self.algorithm != ALGORITHM
            || !valid_label(&self.mode, 32)
        {
            return Err(invalid("unsupported paper session header"));
        }
        let row = &self.instrument;
        let identity = InstrumentId::new(
            row.exchange,
            row.market,
            &row.venue_symbol,
            &row.canonical_symbol,
        )?;
        let spec = InstrumentRiskSpec::new(
            identity,
            decimal(&row.price_tick)?,
            decimal(&row.quantity_step)?,
            decimal(&row.minimum_quantity)?,
            row.maximum_quantity.as_deref().map(decimal).transpose()?,
            decimal(&row.quote_value_per_price_unit)?,
        )?;
        let config = &self.config;
        let config = PaperConfig::new(
            decimal(&config.initial_balance)?,
            decimal(&config.fee_rate)?,
            decimal(&config.stop_slippage_bps)?,
            decimal(&config.max_notional_fraction)?,
            decimal(&config.min_net_rr)?,
        )?;
        Ok(PaperEngine::new(
            spec,
            decimal(&row.min_notional)?,
            config,
            self.timeframe,
        )?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header {
        header: Box<Header>,
    },
    Candle {
        candle: Value,
        warmup: bool,
        step: Value,
    },
    Summary {
        at: DateTime<Utc>,
        reason: String,
        snapshot: Value,
        events: Value,
    },
}

/// Append-only, create-new output with each complete record flushed and synced.
pub struct SessionLog {
    file: File,
    bytes: u64,
    candles: usize,
    last_close: Option<DateTime<Utc>>,
    finished: bool,
    poisoned: bool,
}

impl SessionLog {
    /// Creates parent directories and a new file; never truncates an existing file.
    ///
    /// # Errors
    /// Returns validation, existing-path, serialization, capacity, or filesystem errors.
    pub fn create(path: &Path, header: &Header) -> Result<Self, BoxError> {
        header.engine()?;
        let bytes = encode(&Record::Header {
            header: Box::new(header.clone()),
        })?;
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut log = Self {
            file,
            bytes: 0,
            candles: 0,
            last_close: None,
            finished: false,
            poisoned: false,
        };
        log.write(&bytes)?;
        Ok(log)
    }

    /// Appends one observed candle and its complete deterministic engine result.
    ///
    /// # Errors
    /// Rejects finished/poisoned sessions, record limits, or failed disk writes.
    pub fn append_candle(
        &mut self,
        candle: &Candle,
        warmup: bool,
        step: &PaperStep,
    ) -> Result<(), BoxError> {
        if self.candles >= MAX_CANDLES {
            return Err(invalid("paper session candle limit reached"));
        }
        let record = Record::Candle {
            candle: serde_json::to_value(candle)?,
            warmup,
            step: serde_json::to_value(step)?,
        };
        self.write(&encode(&record)?)?;
        self.candles += 1;
        self.last_close = Some(self.last_close.map_or(candle.close_time(), |previous| {
            previous.max(candle.close_time())
        }));
        Ok(())
    }

    /// Writes the mandatory final accounting and actual pending-cancellation events.
    ///
    /// # Errors
    /// Rejects invalid shutdown metadata, a repeated finish, or failed disk writes.
    pub fn finish(
        &mut self,
        at: DateTime<Utc>,
        reason: &str,
        snapshot: &PaperSnapshot,
        events: &[PaperEvent],
    ) -> Result<(), BoxError> {
        validate_summary(at, reason, self.last_close)?;
        let record = Record::Summary {
            at,
            reason: reason.to_owned(),
            snapshot: serde_json::to_value(snapshot)?,
            events: serde_json::to_value(events)?,
        };
        self.write(&encode(&record)?)?;
        self.finished = true;
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), BoxError> {
        if self.finished || self.poisoned {
            return Err(invalid("paper session is finished or poisoned"));
        }
        let length = u64::try_from(bytes.len())?;
        if bytes.len() > MAX_LINE_BYTES
            || bytes.last() != Some(&b'\n')
            || length > MAX_FILE_BYTES.saturating_sub(self.bytes)
        {
            return Err(invalid("paper session record or file limit exceeded"));
        }
        // A partial write must never be followed by another record.
        self.poisoned = true;
        self.file.write_all(bytes)?;
        self.file.flush()?;
        self.file.sync_data()?;
        self.bytes += length;
        self.poisoned = false;
        Ok(())
    }
}

/// Replays a complete session offline and writes a new verified derived session.
///
/// No output path is created until every result and final accounting value has
/// reproduced exactly. Header metadata is not authenticated venue evidence.
///
/// # Errors
/// Rejects malformed/truncated/oversized input, unknown fields/revisions, changed
/// results, missing/extra summaries, discontinuous candles, or an existing output.
pub fn replay(input: &Path, output: &Path) -> Result<PaperSnapshot, BoxError> {
    let file = File::open(input)?;
    let length = file.metadata()?.len();
    if length > MAX_FILE_BYTES {
        return Err(invalid("paper replay input exceeds file limit"));
    }
    let mut reader = BufReader::new(file);
    let mut total = 0;
    let first =
        next_line(&mut reader, &mut total)?.ok_or_else(|| invalid("empty paper session"))?;
    let Record::Header { mut header } = decode(&first)? else {
        return Err(invalid("paper session must begin with a header"));
    };
    let mut engine = header.engine()?;
    let mut tail = Vec::new();
    let mut candles = 0;
    let mut last_close = None;
    let mut result = None;
    while let Some(line) = next_line(&mut reader, &mut total)? {
        if result.is_some() {
            return Err(invalid("records after paper summary"));
        }
        match decode(&line)? {
            Record::Candle {
                candle,
                warmup,
                step,
            } => {
                candles += 1;
                if candles > MAX_CANDLES {
                    return Err(invalid("paper replay candle limit reached"));
                }
                let candle = parse_candle(&candle)?;
                let actual = if warmup {
                    engine.warm_up(&candle)?
                } else {
                    engine.process_candle(&candle)?
                };
                if serde_json::to_value(actual)? != step {
                    return Err(invalid("paper candle result does not reproduce"));
                }
                last_close = Some(
                    last_close.map_or(candle.close_time(), |previous: DateTime<Utc>| {
                        previous.max(candle.close_time())
                    }),
                );
            }
            Record::Summary {
                at,
                reason,
                snapshot,
                events,
            } => {
                validate_summary(at, &reason, last_close)?;
                let actual_events = engine.shutdown(at)?;
                let actual_snapshot = engine.snapshot();
                if serde_json::to_value(actual_events)? != events
                    || serde_json::to_value(&actual_snapshot)? != snapshot
                {
                    return Err(invalid("paper final accounting does not reproduce"));
                }
                result = Some(actual_snapshot);
            }
            Record::Header { .. } => return Err(invalid("repeated paper header")),
        }
        tail.extend_from_slice(&line);
    }
    let snapshot = result.ok_or_else(|| invalid("paper session is missing its final summary"))?;
    "replay".clone_into(&mut header.mode);
    let header_length = u64::try_from(
        encode(&Record::Header {
            header: header.clone(),
        })?
        .len(),
    )?;
    if header_length + u64::try_from(tail.len())? > MAX_FILE_BYTES {
        return Err(invalid("derived paper session exceeds file limit"));
    }
    let mut log = SessionLog::create(output, &header)?;
    for line in tail.split_inclusive(|byte| *byte == b'\n') {
        log.write(line)?;
    }
    Ok(snapshot)
}

fn next_line(reader: &mut impl BufRead, total: &mut u64) -> Result<Option<Vec<u8>>, BoxError> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(invalid("truncated paper JSONL line"))
            };
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if count > MAX_LINE_BYTES.saturating_sub(line.len())
            || u64::try_from(count)? > MAX_FILE_BYTES.saturating_sub(*total)
        {
            return Err(invalid("paper replay line or file limit exceeded"));
        }
        line.extend_from_slice(&available[..count]);
        reader.consume(count);
        *total += u64::try_from(count)?;
        if line.last() == Some(&b'\n') {
            return Ok(Some(line));
        }
    }
}

fn encode(record: &Record) -> Result<Vec<u8>, BoxError> {
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_LINE_BYTES {
        return Err(invalid("paper record exceeds line limit"));
    }
    Ok(bytes)
}

fn decode(line: &[u8]) -> Result<Record, BoxError> {
    // Reject duplicate keys even inside generic step/event objects, where a
    // normal Value deserializer would otherwise silently keep the last value.
    let value: UniqueValue = serde_json::from_slice(line)?;
    Ok(serde_json::from_value(value.0)?)
}

fn parse_candle(value: &Value) -> Result<Candle, BoxError> {
    exact_keys(
        value,
        &[
            "instrument",
            "timeframe",
            "open_time",
            "close_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "volume_unit",
            "trade_count",
            "is_closed",
        ],
    )?;
    exact_keys(
        &value["instrument"],
        &["exchange", "market", "venue_symbol", "canonical_symbol"],
    )?;
    for field in ["open", "high", "low", "close", "volume"] {
        decimal(
            value[field]
                .as_str()
                .ok_or_else(|| invalid("candle decimals must be strings"))?,
        )?;
    }
    Ok(serde_json::from_value(value.clone())?)
}

fn exact_keys(value: &Value, fields: &[&str]) -> Result<(), BoxError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("expected JSON object"))?;
    if object.len() != fields.len() || fields.iter().any(|field| !object.contains_key(*field)) {
        return Err(invalid("unknown or missing paper candle field"));
    }
    Ok(())
}

fn decimal(value: &str) -> Result<Decimal, BoxError> {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let (whole, fraction) = unsigned
        .split_once('.')
        .map_or((unsigned, None), |(whole, fraction)| {
            (whole, Some(fraction))
        });
    if value.len() > 64
        || whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.is_some_and(|fraction| {
            fraction.is_empty()
                || fraction.len() > 28
                || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(invalid("expected an exact decimal string"));
    }
    Decimal::from_str_exact(value).map_err(|_| invalid("decimal cannot be represented exactly"))
}

fn required_option<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    Option::deserialize(deserializer)
}
fn invalid(message: &'static str) -> BoxError {
    io::Error::new(io::ErrorKind::InvalidData, message).into()
}
fn valid_label(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
}
fn validate_summary(
    at: DateTime<Utc>,
    reason: &str,
    last_close: Option<DateTime<Utc>>,
) -> Result<(), BoxError> {
    if !valid_label(reason, 128) || last_close.is_some_and(|last| at < last) {
        return Err(invalid("invalid paper shutdown metadata"));
    }
    Ok(())
}

struct UniqueValue(Value);
impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = UniqueValue;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("JSON without duplicate keys or floating-point numbers")
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_seq<A: de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<UniqueValue>()? {
                    values.push(value.0);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut object = serde_json::Map::new();
                while let Some((key, value)) = map.next_entry::<String, UniqueValue>()? {
                    if object.insert(key, value.0).is_some() {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                }
                Ok(UniqueValue(Value::Object(object)))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ict_trading_bot::{CandleInput, VolumeUnit};
    use std::path::PathBuf;
    use uuid::Uuid;

    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Result<Self, BoxError> {
            let path = std::env::temp_dir().join(format!("ict-paper-test-{}", Uuid::new_v4()));
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
    fn at(seconds: i64) -> Result<DateTime<Utc>, BoxError> {
        DateTime::from_timestamp(seconds, 0).ok_or_else(|| invalid("invalid fixture timestamp"))
    }
    fn header() -> Result<Header, BoxError> {
        let instrument = PaperInstrument {
            spec: InstrumentRiskSpec::new(
                InstrumentId::new(
                    Exchange::Bybit,
                    MarketKind::LinearPerpetual,
                    "ETHUSDT",
                    "ETH-USDT-PERP",
                )?,
                Decimal::new(1, 2),
                Decimal::new(1, 3),
                Decimal::new(1, 3),
                None,
                Decimal::ONE,
            )?,
            min_notional: Decimal::new(5, 0),
        };
        Ok(Header::new(
            &instrument,
            PaperConfig::default(),
            Timeframe::M1,
            at(300)?,
            "live",
        ))
    }
    fn candle(seconds: i64) -> Result<Candle, BoxError> {
        Ok(Candle::new(CandleInput {
            instrument: InstrumentId::new(
                Exchange::Bybit,
                MarketKind::LinearPerpetual,
                "ETHUSDT",
                "ETH-USDT-PERP",
            )?,
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
    fn complete(path: &Path) -> Result<PaperSnapshot, BoxError> {
        let header = header()?;
        let mut engine = header.engine()?;
        let mut log = SessionLog::create(path, &header)?;
        for (index, warmup) in [(0, true), (60, true), (120, false)] {
            let candle = candle(index)?;
            let step = if warmup {
                engine.warm_up(&candle)?
            } else {
                engine.process_candle(&candle)?
            };
            log.append_candle(&candle, warmup, &step)?;
        }
        let events = engine.shutdown(at(300)?)?;
        let snapshot = engine.snapshot();
        log.finish(at(300)?, "test complete", &snapshot, &events)?;
        assert!(log.finish(at(300)?, "again", &snapshot, &[]).is_err());
        Ok(snapshot)
    }

    fn trade_bars() -> Result<Vec<Candle>, BoxError> {
        // Default five-candle fractals and twenty-body displacement history:
        // bearish BOS -> SSL sweep -> bullish MSS/FVG -> CE -> future fill -> TP.
        let mut prices = vec![[100, 102, 98, 101]; 9];
        prices[2] = [100, 150, 98, 101];
        prices.extend([
            [100, 102, 95, 101],
            [100, 102, 97, 101],
            [100, 102, 98, 101],
            [100, 105, 98, 101],
            [100, 103, 97, 100],
            [99, 102, 96, 98],
            [98, 99, 93, 94],
            [94, 97, 92, 94],
            [94, 97, 91, 93],
            [93, 96, 90, 93],
            [94, 98, 92, 96],
            [96, 99, 93, 97],
            [97, 100, 89, 94],
            [94, 112, 94, 111],
            [110, 113, 108, 111],
            [110, 112, 103, 109],
            [109, 110, 103, 109],
            [109, 151, 109, 150],
        ]);
        let start = DateTime::parse_from_rfc3339("2026-10-28T11:00:00Z")?.timestamp();
        prices
            .into_iter()
            .enumerate()
            .map(|(index, values)| {
                let seconds = start + i64::try_from(index)? * 60;
                Ok(Candle::new(CandleInput {
                    instrument: InstrumentId::new(
                        Exchange::Bybit,
                        MarketKind::LinearPerpetual,
                        "ETHUSDT",
                        "ETH-USDT-PERP",
                    )?,
                    timeframe: Timeframe::M1,
                    open_time: at(seconds)?,
                    close_time: at(seconds + 60)?,
                    open: Decimal::from(values[0]),
                    high: Decimal::from(values[1]),
                    low: Decimal::from(values[2]),
                    close: Decimal::from(values[3]),
                    volume: Decimal::ONE,
                    volume_unit: VolumeUnit::BaseAsset,
                    trade_count: None,
                    is_closed: true,
                })?)
            })
            .collect()
    }

    #[test]
    fn completed_trade_replay_reproduces_identifiers_fees_and_profit() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("trade.jsonl");
        let bars = trade_bars()?;
        let mut header = header()?;
        let final_at = bars
            .last()
            .ok_or_else(|| invalid("empty trade fixture"))?
            .close_time();
        header.started_at = bars[24].open_time();
        let mut engine = header.engine()?;
        let mut log = SessionLog::create(&input, &header)?;
        let mut filled = None;
        let mut closed = None;
        for (index, candle) in bars.iter().enumerate() {
            let warmup = index < 24;
            let step = if warmup {
                engine.warm_up(candle)?
            } else {
                engine.process_candle(candle)?
            };
            for event in &step.events {
                match event {
                    PaperEvent::EntryFilled { intent_id, .. } => filled = Some(*intent_id),
                    PaperEvent::PositionClosed {
                        intent_id, net_pnl, ..
                    } => {
                        assert!(*net_pnl > Decimal::ZERO);
                        closed = Some(*intent_id);
                    }
                    PaperEvent::EntryCancelled { .. } => {}
                }
            }
            log.append_candle(candle, warmup, &step)?;
        }
        assert!(filled.is_some());
        assert_eq!(filled, closed);
        let events = engine.shutdown(final_at)?;
        let expected = engine.snapshot();
        assert_eq!(expected.completed_trades, 1);
        assert_eq!(expected.wins, 1);
        assert!(expected.balance > Decimal::new(100, 0));
        assert!(expected.fees_paid > Decimal::ZERO);
        log.finish(final_at, "trade fixture complete", &expected, &events)?;
        drop(log);
        let original = fs::read(&input)?;
        let derived = temp.path("trade-replayed.jsonl");
        assert_eq!(replay(&input, &derived)?, expected);
        assert_eq!(
            replay(&derived, &temp.path("trade-replayed-again.jsonl"))?,
            expected
        );
        assert_eq!(fs::read(input)?, original);
        Ok(())
    }

    #[test]
    fn actual_replay_reproduces_warmup_steps_and_preserves_original() -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("original.jsonl");
        let expected = complete(&input)?;
        let original = fs::read(&input)?;
        let output = temp.path("derived.jsonl");
        assert_eq!(replay(&input, &output)?, expected);
        assert_eq!(fs::read(&input)?, original);
        assert_eq!(replay(&output, &temp.path("second.jsonl"))?, expected);
        assert!(SessionLog::create(&input, &header()?).is_err());
        assert!(replay(&input, &output).is_err());
        assert_eq!(fs::read(input)?, original);
        Ok(())
    }

    #[test]
    fn exact_decimal_and_header_codecs_reject_implicit_or_lossy_values() -> Result<(), BoxError> {
        for text in [
            "1e3",
            "+1",
            " 1",
            "1.",
            ".1",
            "0.12345678901234567890123456789",
        ] {
            assert!(decimal(text).is_err());
        }
        assert_eq!(decimal("0.0006")?, Decimal::new(6, 4));
        let original = serde_json::to_value(header()?)?;
        let mut missing = original.clone();
        missing["instrument"]
            .as_object_mut()
            .ok_or_else(|| invalid("fixture object"))?
            .remove("maximum_quantity");
        assert!(serde_json::from_value::<Header>(missing).is_err());
        let mut unknown = original.clone();
        unknown["extra"] = true.into();
        assert!(serde_json::from_value::<Header>(unknown).is_err());
        let mut numeric = original.clone();
        numeric["config"]["fee_rate"] = 1.into();
        assert!(serde_json::from_value::<Header>(numeric).is_err());
        let mut version = original;
        version["version"] = 2.into();
        assert!(serde_json::from_value::<Header>(version)?.engine().is_err());
        assert!(decode(br#"{"record":"header","record":"summary"}"#).is_err());
        Ok(())
    }

    #[test]
    fn mismatched_results_and_truncated_or_missing_summary_never_create_output(
    ) -> Result<(), BoxError> {
        let temp = Temp::new()?;
        let input = temp.path("valid.jsonl");
        complete(&input)?;
        let source = fs::read_to_string(input)?;
        let mut lines = source.lines().map(str::to_owned).collect::<Vec<_>>();
        let mut changed: Value = serde_json::from_str(&lines[1])?;
        changed["step"]["snapshot"]["balance"] = "1000".into();
        lines[1] = serde_json::to_string(&changed)?;
        let mismatch = format!("{}\n", lines.join("\n"));
        let missing = source.lines().take(4).collect::<Vec<_>>().join("\n") + "\n";
        for (index, content) in [
            mismatch.as_bytes(),
            &source.as_bytes()[..source.len() - 1],
            missing.as_bytes(),
            format!("{source}{{}}\n").as_bytes(),
        ]
        .into_iter()
        .enumerate()
        {
            let path = temp.path(&format!("bad-{index}.jsonl"));
            fs::write(&path, content)?;
            let output = temp.path(&format!("output-{index}.jsonl"));
            assert!(replay(&path, &output).is_err());
            assert!(!output.exists());
        }
        Ok(())
    }

    #[test]
    fn candle_parser_requires_all_fields_and_exact_strings() -> Result<(), BoxError> {
        let original = serde_json::to_value(candle(0)?)?;
        let mut numeric = original.clone();
        numeric["open"] = 100.into();
        assert!(parse_candle(&numeric).is_err());
        let mut unknown = original.clone();
        unknown["extra"] = true.into();
        assert!(parse_candle(&unknown).is_err());
        let mut missing = original.clone();
        missing
            .as_object_mut()
            .ok_or_else(|| invalid("fixture object"))?
            .remove("trade_count");
        assert!(parse_candle(&missing).is_err());
        let mut nested = original;
        nested["instrument"]["extra"] = true.into();
        assert!(parse_candle(&nested).is_err());
        let mut reader = io::Cursor::new(vec![b'x'; MAX_LINE_BYTES + 1]);
        assert!(next_line(&mut reader, &mut 0).is_err());
        assert!(decode(b"{\"counter\":0.1}\n").is_err());
        Ok(())
    }
}
