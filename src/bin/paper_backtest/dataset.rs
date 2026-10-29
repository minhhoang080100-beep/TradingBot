//! Strict, bounded, local-only input for reproducible offline experiments.

use std::{fs::File, io::Read, path::{Component, Path}};

use chrono::{DateTime, Duration, Utc};
use ict_trading_bot::{Candle, CandleInput, Exchange, InstrumentId, MarketKind, Timeframe, VolumeUnit,
    paper::PaperInstrument, risk::InstrumentRiskSpec};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::BoxError;

const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MAX_SERIES_BYTES: u64 = 128 * 1024 * 1024;
const MAX_CANDLES: usize = 100_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    pub version: u16,
    pub source: String,
    pub captured_at: DateTime<Utc>,
    pub metadata_as_of: DateTime<Utc>,
    pub instruments: Vec<InstrumentRow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InstrumentRow {
    pub symbol: String,
    pub price_tick: String,
    pub quantity_step: String,
    pub minimum_quantity: String,
    pub maximum_quantity: Option<String>,
    pub min_notional: String,
    pub candles_file: String,
}

impl InstrumentRow {
    fn instrument(&self) -> Result<PaperInstrument, BoxError> {
        let identity = identity(&self.symbol)?;
        let min_notional = exact_decimal(&self.min_notional)?;
        if min_notional < Decimal::ZERO { return Err("minimum notional must not be negative".into()); }
        Ok(PaperInstrument {
            spec: InstrumentRiskSpec::new(identity, exact_decimal(&self.price_tick)?,
                exact_decimal(&self.quantity_step)?, exact_decimal(&self.minimum_quantity)?,
                self.maximum_quantity.as_deref().map(exact_decimal).transpose()?, Decimal::ONE)?,
            min_notional,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityRow {
    exchange: Exchange,
    market: MarketKind,
    venue_symbol: String,
    canonical_symbol: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CandleRow {
    instrument: IdentityRow,
    timeframe: Timeframe,
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    volume_unit: VolumeUnit,
    trade_count: Option<u64>,
    is_closed: bool,
}

impl CandleRow {
    fn candle(self, expected: &InstrumentId) -> Result<Candle, BoxError> {
        if self.instrument.exchange != expected.exchange || self.instrument.market != expected.market
            || self.instrument.venue_symbol != expected.venue_symbol.as_str()
            || self.instrument.canonical_symbol != expected.canonical_symbol.as_str()
            || self.timeframe != Timeframe::M1 || self.volume_unit != VolumeUnit::BaseAsset
            || !self.is_closed || self.open_time.timestamp_subsec_nanos() != 0
            || self.open_time.timestamp().rem_euclid(60) != 0
            || self.close_time.signed_duration_since(self.open_time) != Duration::minutes(1) {
            return Err("expected final canonical Bybit M1 base-volume candle".into());
        }
        Ok(Candle::new(CandleInput {
            instrument: expected.clone(), timeframe: self.timeframe,
            open_time: self.open_time, close_time: self.close_time,
            open: exact_decimal(&self.open)?, high: exact_decimal(&self.high)?,
            low: exact_decimal(&self.low)?, close: exact_decimal(&self.close)?,
            volume: exact_decimal(&self.volume)?, volume_unit: self.volume_unit,
            trade_count: self.trade_count, is_closed: self.is_closed,
        })?)
    }
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct FileEvidence {
    pub file: String,
    pub bytes: usize,
    pub sha256: String,
    pub candles: usize,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct Provenance {
    pub manifest_sha256: String,
    pub files: Vec<FileEvidence>,
    pub combined_sha256: String,
}

#[derive(Debug)]
pub(super) struct Dataset {
    pub manifest: Manifest,
    pub provenance: Provenance,
    pub instruments: Vec<PaperInstrument>,
    pub series: Vec<Vec<Candle>>,
}

pub(super) fn hash(bytes: &[u8]) -> String { hex::encode(Sha256::digest(bytes)) }

fn identity(symbol: &str) -> Result<InstrumentId, BoxError> {
    let canonical = match symbol {
        "BTCUSDT" => "BTC-USDT-PERP",
        "ETHUSDT" => "ETH-USDT-PERP",
        _ => return Err("only BTCUSDT and ETHUSDT are supported".into()),
    };
    Ok(InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual, symbol, canonical)?)
}

fn exact_decimal(value: &str) -> Result<Decimal, BoxError> {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let (whole, fraction) = unsigned.split_once('.').map_or((unsigned, None), |(a, b)| (a, Some(b)));
    if value.len() > 64 || whole.is_empty() || !whole.bytes().all(|b| b.is_ascii_digit())
        || fraction.is_some_and(|f| f.is_empty() || f.len() > 28 || !f.bytes().all(|b| b.is_ascii_digit())) {
        return Err("expected a plain exact Decimal string".into());
    }
    Ok(Decimal::from_str_exact(value)?)
}

fn bounded_read(path: &Path, limit: u64) -> Result<Vec<u8>, BoxError> {
    let file = File::open(path)?;
    if file.metadata()?.len() > limit { return Err("backtest input exceeds byte limit".into()); }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len())? > limit { return Err("backtest input exceeds byte limit".into()); }
    Ok(bytes)
}

pub(super) fn load(path: &Path) -> Result<Dataset, BoxError> {
    let bytes = bounded_read(path, MAX_MANIFEST_BYTES)?;
    let mut manifest: Manifest = serde_json::from_slice(&bytes)?;
    if manifest.version != 1 || manifest.source.trim().is_empty() || manifest.source.len() > 256
        || manifest.metadata_as_of > manifest.captured_at || manifest.instruments.len() != 2 {
        return Err("invalid or unsupported offline dataset manifest".into());
    }
    manifest.instruments.sort_unstable_by(|a, b| a.symbol.cmp(&b.symbol));
    if manifest.instruments.iter().map(|row| row.symbol.as_str()).collect::<Vec<_>>() != ["BTCUSDT", "ETHUSDT"] {
        return Err("dataset requires exactly one BTCUSDT and one ETHUSDT series".into());
    }
    let root = path.canonicalize()?.parent().ok_or("manifest has no parent directory")?.to_path_buf();
    let mut instruments = Vec::new();
    let mut series = Vec::new();
    let mut files = Vec::new();
    for row in &manifest.instruments {
        let name = Path::new(&row.candles_file);
        if name.components().count() != 1 || !matches!(name.components().next(), Some(Component::Normal(_))) {
            return Err("candles_file must be a filename within the dataset directory".into());
        }
        let source = root.join(name).canonicalize()?;
        if !source.starts_with(&root) { return Err("candle file escapes the dataset directory".into()); }
        let bytes = bounded_read(&source, MAX_SERIES_BYTES)?;
        let rows: Vec<CandleRow> = serde_json::from_slice(&bytes)?;
        if rows.is_empty() || rows.len() > MAX_CANDLES { return Err("candle count is outside 1..=100000".into()); }
        let instrument = row.instrument()?;
        let candles = rows.into_iter().map(|bar| bar.candle(instrument.spec.instrument())).collect::<Result<Vec<_>, _>>()?;
        for pair in candles.windows(2) {
            if pair[0].close_time() != pair[1].open_time() {
                return Err("candle series contains a gap, duplicate or unsorted row".into());
            }
        }
        if candles.last().is_some_and(|bar| bar.close_time() > manifest.captured_at) {
            return Err("dataset contains candles later than its capture time".into());
        }
        files.push(FileEvidence { file: row.candles_file.clone(), bytes: bytes.len(), sha256: hash(&bytes), candles: candles.len() });
        series.push(candles);
        instruments.push(instrument);
    }
    validate_alignment(&series)?;
    let manifest_sha256 = hash(&bytes);
    let combined_sha256 = hash(&serde_json::to_vec(&(&manifest_sha256, &files))?);
    Ok(Dataset { manifest, provenance: Provenance { manifest_sha256, files, combined_sha256 }, instruments, series })
}

fn validate_alignment(series: &[Vec<Candle>]) -> Result<(), BoxError> {
    let [btc, eth] = series else { return Err("expected two candle series".into()); };
    if btc.len() != eth.len() || btc.iter().zip(eth).any(|(a, b)| a.open_time() != b.open_time()) {
        return Err("BTC and ETH histories must have identical coverage".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};
    use uuid::Uuid;

    struct FixtureDir(PathBuf);

    impl Drop for FixtureDir {
        fn drop(&mut self) {
            // Only files created by this fixture; no recursive deletion.
            for name in ["manifest.json", "BTCUSDT.json", "ETHUSDT.json"] {
                let _ = fs::remove_file(self.0.join(name));
            }
            let _ = fs::remove_dir(&self.0);
        }
    }

    fn fixture() -> Result<(FixtureDir, Manifest), BoxError> {
        let directory = FixtureDir(std::env::temp_dir().join(format!("ict-backtest-{}", Uuid::new_v4())));
        fs::create_dir(&directory.0)?;
        let start = DateTime::parse_from_rfc3339("2026-10-28T00:00:00Z")?.with_timezone(&Utc);
        let mut instruments = Vec::new();
        for symbol in ["BTCUSDT", "ETHUSDT"] {
            let row = InstrumentRow { symbol: symbol.to_owned(), price_tick: "0.1".to_owned(),
                quantity_step: "0.001".to_owned(), minimum_quantity: "0.001".to_owned(),
                maximum_quantity: Some("100".to_owned()), min_notional: "1".to_owned(),
                candles_file: format!("{symbol}.json") };
            let instrument = identity(symbol)?;
            let candles = (0..3).map(|minute| Candle::new(CandleInput {
                instrument: instrument.clone(),
                timeframe: Timeframe::M1, open_time: start + Duration::minutes(minute),
                close_time: start + Duration::minutes(minute + 1), open: Decimal::from(100),
                high: Decimal::from(102), low: Decimal::from(98), close: Decimal::from(101),
                volume: Decimal::ONE, volume_unit: VolumeUnit::BaseAsset, trade_count: None, is_closed: true,
            })).collect::<Result<Vec<_>, _>>()?;
            fs::write(directory.0.join(&row.candles_file), serde_json::to_vec(&candles)?)?;
            instruments.push(row);
        }
        let manifest = Manifest { version: 1, source: "fixture".to_owned(), captured_at: start + Duration::minutes(3),
            metadata_as_of: start, instruments };
        fs::write(directory.0.join("manifest.json"), serde_json::to_vec(&manifest)?)?;
        Ok((directory, manifest))
    }

    #[test]
    fn financial_inputs_are_string_only_and_never_rounded() -> Result<(), BoxError> {
        for value in ["1e-3", "NaN", "1.12345678901234567890123456789", "", "+1", "1."] {
            assert!(exact_decimal(value).is_err());
        }
        let mut row = serde_json::json!({"symbol":"BTCUSDT","price_tick":"0.1","quantity_step":"0.001",
            "minimum_quantity":"0.001","maximum_quantity":null,"min_notional":"5","candles_file":"BTC.json"});
        assert!(serde_json::from_value::<InstrumentRow>(row.clone())?.instrument().is_ok());
        row["price_tick"] = serde_json::json!(1);
        assert!(serde_json::from_value::<InstrumentRow>(row).is_err());
        let duplicate = r#"{"version":1,"version":1,"source":"mock","captured_at":"2026-10-29T00:00:00Z","metadata_as_of":"2026-10-29T00:00:00Z","instruments":[]}"#;
        assert!(serde_json::from_str::<Manifest>(duplicate).is_err());
        Ok(())
    }

    #[test]
    fn source_hashes_are_repeatable_and_change_when_candle_evidence_changes() -> Result<(), BoxError> {
        let (directory, _) = fixture()?;
        let path = directory.0.join("manifest.json");
        let original = load(&path)?.provenance;
        assert_eq!(original.combined_sha256, load(&path)?.provenance.combined_sha256);
        let source = directory.0.join("BTCUSDT.json");
        let mut rows: serde_json::Value = serde_json::from_slice(&fs::read(&source)?)?;
        rows[0]["close"] = serde_json::json!("100.5");
        fs::write(&source, serde_json::to_vec(&rows)?)?;
        let changed = load(&path)?.provenance;
        assert_eq!(original.manifest_sha256, changed.manifest_sha256);
        assert_ne!(original.combined_sha256, changed.combined_sha256);
        assert_ne!(original.files[0].sha256, changed.files[0].sha256);
        Ok(())
    }

    #[test]
    fn gaps_duplicate_rows_future_bars_and_escaping_paths_are_rejected() -> Result<(), BoxError> {
        let (directory, mut manifest) = fixture()?;
        let path = directory.0.join("manifest.json");
        let source = directory.0.join("BTCUSDT.json");
        let original = fs::read(&source)?;
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&original)?;
        for invalid in [vec![rows[0].clone(), rows[2].clone()], vec![rows[0].clone(), rows[0].clone(), rows[2].clone()]] {
            fs::write(&source, serde_json::to_vec(&invalid)?)?;
            assert!(load(&path).is_err());
        }
        fs::write(&source, &original)?;
        manifest.captured_at -= Duration::minutes(1);
        fs::write(&path, serde_json::to_vec(&manifest)?)?;
        assert!(load(&path).is_err());
        manifest.captured_at += Duration::minutes(1);
        manifest.instruments[0].candles_file = "../BTCUSDT.json".to_owned();
        fs::write(&path, serde_json::to_vec(&manifest)?)?;
        assert!(load(&path).is_err());
        Ok(())
    }

    #[test]
    fn candle_decoder_rejects_numeric_prices_unknown_fields_and_ohlc_corruption() -> Result<(), BoxError> {
        let (directory, _) = fixture()?;
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&fs::read(directory.0.join("BTCUSDT.json"))?)?;
        let original = rows.first().ok_or("fixture row")?.clone();
        for replacement in [serde_json::json!(100), serde_json::json!("1.12345678901234567890123456789")] {
            let mut row = original.clone();
            row["open"] = replacement;
            let decoded = serde_json::from_value::<CandleRow>(row);
            assert!(decoded.map_err(BoxError::from).and_then(|row| row.candle(&identity("BTCUSDT")?)).is_err());
        }
        let mut row = original.clone();
        row["unrecognized"] = serde_json::json!(true);
        assert!(serde_json::from_value::<CandleRow>(row).is_err());
        let mut row = original;
        row["high"] = serde_json::json!("99");
        assert!(serde_json::from_value::<CandleRow>(row)?.candle(&identity("BTCUSDT")?).is_err());
        Ok(())
    }
}
