//! Keyless, bounded public Bybit market data for the local paper simulator.
//!
//! Only `/v5/market/instruments-info` and `/v5/market/kline` are requested. There
//! is no private endpoint, credential input, signing, order submission, redirect
//! following, automatic retry, or fallback instrument metadata in this module.
//! REST bars close only after their exclusive interval boundary according to
//! both local and venue response time; excessive clock skew fails closed.
//! See <https://bybit-exchange.github.io/docs/v5/market/kline> and
//! <https://bybit-exchange.github.io/docs/v5/market/instrument>.

use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    models::{Candle, CandleInput, Exchange, InstrumentId, MarketKind, Timeframe, VolumeUnit},
    risk::InstrumentRiskSpec,
};

const PUBLIC_ORIGIN: &str = "https://api.bybit.com";
const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_CANDLES: usize = 1000;
const MAX_CLOCK_SKEW_MS: u64 = 10_000;

/// Current venue constraints required by paper position sizing.
///
/// This metadata is public and is not a live-order permission or margin model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperInstrument {
    /// Validated linear-USDT contract filters, with base-asset quantity units.
    pub spec: InstrumentRiskSpec,
    /// Explicit venue minimum notional in USDT; never guessed on failure.
    pub min_notional: Decimal,
}

/// Public-data HTTP client with a fixed Bybit origin and no credentials.
#[derive(Clone, Debug)]
pub struct PublicPaperMarket {
    client: reqwest::Client,
    origin: String,
}

impl PublicPaperMarket {
    /// Builds a keyless client with a ten-second request timeout and no retries.
    ///
    /// # Errors
    /// Returns [`PaperMarketError::Configuration`] if HTTP setup fails.
    pub fn new() -> Result<Self, PaperMarketError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|_| PaperMarketError::Configuration)?;
        Ok(Self {
            client,
            origin: PUBLIC_ORIGIN.to_owned(),
        })
    }

    /// Reads current filters for one explicitly requested linear-USDT perpetual.
    ///
    /// # Errors
    /// Rejects invalid symbols, transport/venue errors, incomplete metadata,
    /// wrong product/currency, and any instrument not currently `Trading`.
    pub async fn instrument(&self, symbol: &str) -> Result<PaperInstrument, PaperMarketError> {
        validate_symbol(symbol)?;
        let body = self
            .get(
                "/v5/market/instruments-info",
                &[
                    ("category", "linear".to_owned()),
                    ("symbol", symbol.to_owned()),
                ],
            )
            .await?;
        parse_instrument(symbol, 200, &body)
    }

    /// Reads up to `limit` fully closed bars, normalized in ascending time order.
    ///
    /// The public query ends one millisecond before the current interval, so a
    /// warmup request for 500 bars need not spend a slot on an unfinished bar.
    /// Newly listed symbols may return fewer bars. The caller must check its
    /// required warmup length and maintain continuity across polling responses.
    ///
    /// # Errors
    /// Rejects limits outside 1–1000, invalid identity/time, HTTP/venue errors,
    /// inexact numbers, malformed bars, duplicates, gaps or local/server time
    /// disagreement greater than ten seconds.
    pub async fn recent_closed_candles(
        &self,
        instrument: &InstrumentId,
        timeframe: Timeframe,
        limit: u16,
        now: DateTime<Utc>,
    ) -> Result<Vec<Candle>, PaperMarketError> {
        validate_instrument_identity(instrument)?;
        if !(1..=1000).contains(&limit) {
            return Err(PaperMarketError::InvalidLimit);
        }
        let interval = timeframe.duration().num_milliseconds();
        let now_ms = now.timestamp_millis();
        if now_ms < 0 {
            return Err(PaperMarketError::InvalidTime);
        }
        let end = (now_ms - now_ms.rem_euclid(interval))
            .checked_sub(1)
            .filter(|value| *value >= 0)
            .ok_or(PaperMarketError::InvalidTime)?;
        let body = self
            .get(
                "/v5/market/kline",
                &[
                    ("category", "linear".to_owned()),
                    ("symbol", instrument.venue_symbol.as_str().to_owned()),
                    ("interval", wire_interval(timeframe).to_owned()),
                    ("limit", limit.to_string()),
                    ("end", end.to_string()),
                ],
            )
            .await?;
        let candles = parse_closed_candles(instrument, timeframe, now, 200, &body)?;
        if candles.len() > usize::from(limit) {
            return Err(PaperMarketError::InvalidCandles);
        }
        Ok(candles)
    }

    async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Vec<u8>, PaperMarketError> {
        let mut response = self
            .client
            .get(format!("{}{path}", self.origin))
            .query(query)
            .send()
            .await
            .map_err(|_| PaperMarketError::Transport)?;
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(PaperMarketError::HttpStatus(status));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_BODY_BYTES as u64)
        {
            return Err(PaperMarketError::ResponseTooLarge);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| PaperMarketError::Transport)?
        {
            if chunk.len() > MAX_BODY_BYTES.saturating_sub(body.len()) {
                return Err(PaperMarketError::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    #[cfg(test)]
    fn local(origin: String) -> Result<Self, PaperMarketError> {
        let mut market = Self::new()?;
        market.origin = origin;
        Ok(market)
    }
}

/// Parses current Bybit linear-USDT perpetual metadata without any network I/O.
///
/// Only the documented linear `maxOrderQty` field is used. The similarly named
/// `maxLimitOrderQty` belongs to the Spot response and is not a silent fallback.
///
/// # Errors
/// Rejects non-success HTTP/venue status, missing or inexact fields, multiple
/// instruments, residual pagination, non-trading products and mismatched symbols.
pub fn parse_instrument(
    symbol: &str,
    status: u16,
    body: &[u8],
) -> Result<PaperInstrument, PaperMarketError> {
    validate_symbol(symbol)?;
    let result = decode::<InstrumentResponse>(status, body)?.result;
    if result.category != "linear" || !result.next_page_cursor.is_empty() {
        return Err(PaperMarketError::UnsupportedInstrument);
    }
    let [row] = result.list.as_slice() else {
        return Err(PaperMarketError::InvalidMetadata);
    };
    if row.symbol != symbol
        || row.contract_type != "LinearPerpetual"
        || row.status != "Trading"
        || row.quote_coin != "USDT"
        || row.settle_coin != "USDT"
        || row.base_coin.is_empty()
        || !row
            .base_coin
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        || format!("{}USDT", row.base_coin) != symbol
    {
        return Err(PaperMarketError::UnsupportedInstrument);
    }
    if row.lot_size_filter.min_notional_value < Decimal::ZERO {
        return Err(PaperMarketError::InvalidMetadata);
    }
    let instrument = InstrumentId::new(
        Exchange::Bybit,
        MarketKind::LinearPerpetual,
        symbol,
        format!("{}-USDT-PERP", row.base_coin),
    )
    .map_err(|_| PaperMarketError::InvalidMetadata)?;
    let spec = InstrumentRiskSpec::new(
        instrument,
        row.price_filter.tick_size,
        row.lot_size_filter.qty_step,
        row.lot_size_filter.min_order_qty,
        Some(row.lot_size_filter.max_order_qty),
        Decimal::ONE,
    )
    .map_err(|_| PaperMarketError::InvalidMetadata)?;
    Ok(PaperInstrument {
        spec,
        min_notional: row.lot_size_filter.min_notional_value,
    })
}

/// Parses, validates and orders public REST candles without network I/O.
///
/// Every row must have seven string fields: timestamp, OHLC, volume and turnover.
/// Numbers are parsed exactly; even unused turnover and unfinished rows are
/// validated before filtering. Closure means `open + timeframe` is no later
/// than both `now` and Bybit's response time. A ten-second skew bound rejects
/// materially stale responses or mismatched clocks. This is not private-stream
/// confirmation or proof that the response itself is atomic.
///
/// # Errors
/// Rejects incorrect scope, HTTP/venue errors, malformed numbers/OHLC/time,
/// oversized lists, duplicate opens, misaligned timestamps, missing intervals,
/// missing/invalid server time or local/server skew exceeding ten seconds.
pub fn parse_closed_candles(
    instrument: &InstrumentId,
    timeframe: Timeframe,
    now: DateTime<Utc>,
    status: u16,
    body: &[u8],
) -> Result<Vec<Candle>, PaperMarketError> {
    validate_instrument_identity(instrument)?;
    if now.timestamp_millis() < 0 {
        return Err(PaperMarketError::InvalidTime);
    }
    let envelope = decode::<KlineResponse>(status, body)?;
    let server_ms = envelope.time.ok_or(PaperMarketError::InvalidTime)?;
    if server_ms <= 0 {
        return Err(PaperMarketError::InvalidTime);
    }
    let server_time =
        DateTime::from_timestamp_millis(server_ms).ok_or(PaperMarketError::InvalidTime)?;
    if now.timestamp_millis().abs_diff(server_ms) > MAX_CLOCK_SKEW_MS {
        return Err(PaperMarketError::ClockSkew);
    }
    let closed_through = now.min(server_time);
    let response = envelope.result;
    if response.category != "linear" || response.symbol != instrument.venue_symbol.as_str() {
        return Err(PaperMarketError::UnsupportedInstrument);
    }
    if response.list.len() > MAX_CANDLES {
        return Err(PaperMarketError::InvalidCandles);
    }
    let mut candles = response
        .list
        .iter()
        .map(|row| parse_candle(row, instrument, timeframe))
        .collect::<Result<Vec<_>, _>>()?;
    candles.sort_unstable_by_key(Candle::open_time);
    for pair in candles.windows(2) {
        if pair[0].open_time() == pair[1].open_time() {
            return Err(PaperMarketError::DuplicateCandle);
        }
        if pair[0].close_time() != pair[1].open_time() {
            return Err(PaperMarketError::CandleGap);
        }
    }
    candles.retain(|candle| candle.close_time() <= closed_through);
    Ok(candles)
}

fn parse_candle(
    row: &[String; 7],
    instrument: &InstrumentId,
    timeframe: Timeframe,
) -> Result<Candle, PaperMarketError> {
    if row[0].is_empty() || row[0].len() > 19 || !row[0].bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(PaperMarketError::InvalidTime);
    }
    let open_ms: i64 = row[0].parse().map_err(|_| PaperMarketError::InvalidTime)?;
    if open_ms < 0 || open_ms.rem_euclid(timeframe.duration().num_milliseconds()) != 0 {
        return Err(PaperMarketError::InvalidTime);
    }
    let open_time =
        DateTime::from_timestamp_millis(open_ms).ok_or(PaperMarketError::InvalidTime)?;
    let close_time = open_time
        .checked_add_signed(timeframe.duration())
        .ok_or(PaperMarketError::InvalidTime)?;
    let value = |index: usize| {
        crate::decimal_codec::parse_exact(&row[index]).map_err(|_| PaperMarketError::InvalidCandles)
    };
    let turnover = value(6)?;
    if turnover < Decimal::ZERO {
        return Err(PaperMarketError::InvalidCandles);
    }
    Candle::new(CandleInput {
        instrument: instrument.clone(),
        timeframe,
        open_time,
        close_time,
        open: value(1)?,
        high: value(2)?,
        low: value(3)?,
        close: value(4)?,
        volume: value(5)?,
        volume_unit: VolumeUnit::BaseAsset,
        trade_count: None,
        // Only retained closed rows escape parse_closed_candles.
        is_closed: true,
    })
    .map_err(|_| PaperMarketError::InvalidCandles)
}

fn validate_symbol(symbol: &str) -> Result<(), PaperMarketError> {
    if symbol.len() <= 4
        || symbol.len() > 32
        || !symbol.ends_with("USDT")
        || !symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(PaperMarketError::InvalidSymbol);
    }
    Ok(())
}

fn validate_instrument_identity(instrument: &InstrumentId) -> Result<(), PaperMarketError> {
    validate_symbol(instrument.venue_symbol.as_str())?;
    if instrument.exchange != Exchange::Bybit || instrument.market != MarketKind::LinearPerpetual {
        return Err(PaperMarketError::UnsupportedInstrument);
    }
    Ok(())
}

const fn wire_interval(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::M1 => "1",
        Timeframe::M3 => "3",
        Timeframe::M5 => "5",
        Timeframe::M15 => "15",
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<T> {
    ret_code: i64,
    result: T,
    time: Option<i64>,
}

fn decode<T: DeserializeOwned>(status: u16, body: &[u8]) -> Result<Envelope<T>, PaperMarketError> {
    if !(200..300).contains(&status) {
        return Err(PaperMarketError::HttpStatus(status));
    }
    if body.len() > MAX_BODY_BYTES {
        return Err(PaperMarketError::ResponseTooLarge);
    }
    let envelope: Envelope<T> =
        serde_json::from_slice(body).map_err(|_| PaperMarketError::MalformedResponse)?;
    if envelope.ret_code != 0 {
        return Err(PaperMarketError::Venue(envelope.ret_code));
    }
    Ok(envelope)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstrumentResponse {
    category: String,
    next_page_cursor: String,
    list: Vec<InstrumentRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstrumentRow {
    symbol: String,
    contract_type: String,
    status: String,
    base_coin: String,
    quote_coin: String,
    settle_coin: String,
    price_filter: PriceFilter,
    lot_size_filter: LotSizeFilter,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PriceFilter {
    #[serde(deserialize_with = "crate::decimal_codec::required::deserialize")]
    tick_size: Decimal,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LotSizeFilter {
    #[serde(deserialize_with = "crate::decimal_codec::required::deserialize")]
    qty_step: Decimal,
    #[serde(deserialize_with = "crate::decimal_codec::required::deserialize")]
    min_order_qty: Decimal,
    #[serde(deserialize_with = "crate::decimal_codec::required::deserialize")]
    max_order_qty: Decimal,
    #[serde(deserialize_with = "crate::decimal_codec::required::deserialize")]
    min_notional_value: Decimal,
}

#[derive(Deserialize)]
struct KlineResponse {
    category: String,
    symbol: String,
    list: Vec<[String; 7]>,
}

/// Sanitized public-market failures without response payloads or network URLs.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PaperMarketError {
    /// HTTP client construction failed.
    #[error("paper public-market client setup failed")]
    Configuration,
    /// Network request/body read failed or timed out; there is no automatic retry.
    #[error("paper public-market transport failed")]
    Transport,
    /// HTTP response was not successful, including redirects.
    #[error("paper public-market HTTP status {0}")]
    HttpStatus(u16),
    /// Public response exceeded one MiB.
    #[error("paper public-market response too large")]
    ResponseTooLarge,
    /// A required JSON field/type was absent or malformed.
    #[error("malformed paper public-market response")]
    MalformedResponse,
    /// Venue explicitly reported failure; its free-form message is discarded.
    #[error("paper public-market venue code {0}")]
    Venue(i64),
    /// Symbols are restricted to uppercase ASCII USDT identifiers.
    #[error("paper symbol must be an uppercase ASCII USDT symbol")]
    InvalidSymbol,
    /// Response/request is not the requested trading linear-USDT perpetual.
    #[error("unsupported or mismatched paper instrument")]
    UnsupportedInstrument,
    /// Required contract filters are inconsistent or invalid.
    #[error("invalid paper instrument metadata")]
    InvalidMetadata,
    /// Invalid OHLCV, turnover, numeric precision, row count or response shape.
    #[error("invalid paper market candles")]
    InvalidCandles,
    /// Missing/invalid server time or negative, overflowing, misaligned timestamps.
    #[error("invalid paper market time")]
    InvalidTime,
    /// Local/venue times differ by more than the ten-second freshness allowance.
    #[error("paper local and server clocks differ by more than ten seconds")]
    ClockSkew,
    /// Same candle open appeared more than once.
    #[error("duplicate candle in paper market response")]
    DuplicateCandle,
    /// At least one full interval is absent between returned rows.
    #[error("gap in paper market candle response")]
    CandleGap,
    /// Requested bar count is outside the public endpoint's range.
    #[error("paper candle limit must be between 1 and 1000")]
    InvalidLimit,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::error::Error;
    use wiremock::{
        matchers::{method, path, query_param},
        Mock, MockServer, ResponseTemplate,
    };

    type TestResult = Result<(), Box<dyn Error>>;

    fn instrument() -> Result<InstrumentId, Box<dyn Error>> {
        Ok(InstrumentId::new(
            Exchange::Bybit,
            MarketKind::LinearPerpetual,
            "BTCUSDT",
            "BTC-USDT-PERP",
        )?)
    }

    fn at(minute: i64) -> Result<DateTime<Utc>, Box<dyn Error>> {
        DateTime::from_timestamp(1_767_225_600 + minute * 60, 0)
            .ok_or_else(|| "test timestamp".into())
    }

    fn row(minute: i64) -> Result<Value, Box<dyn Error>> {
        Ok(json!([
            at(minute)?.timestamp_millis().to_string(),
            "100",
            "102",
            "99",
            "101",
            "1.25",
            "126.25"
        ]))
    }

    fn klines(rows: Vec<Value>, server_time: DateTime<Utc>) -> Value {
        let mut response =
            json!({"retCode":0,"retMsg":"OK","result":{"category":"linear","symbol":"BTCUSDT"}});
        response["result"]["list"] = Value::Array(rows);
        response["time"] = json!(server_time.timestamp_millis());
        response
    }

    fn metadata() -> Value {
        json!({"retCode":0,"retMsg":"OK","result":{"category":"linear","nextPageCursor":"","list":[{
            "symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading",
            "baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT",
            "priceFilter":{"tickSize":"0.1"},
            "lotSizeFilter":{"qtyStep":"0.001","minOrderQty":"0.001","maxOrderQty":"100","minNotionalValue":"5"}
        }]}})
    }

    #[test]
    fn instrument_filters_are_exact_and_have_explicit_units() -> TestResult {
        let parsed = parse_instrument("BTCUSDT", 200, &serde_json::to_vec(&metadata())?)?;
        assert_eq!(parsed.spec.instrument(), &instrument()?);
        assert_eq!(parsed.spec.price_tick(), Decimal::new(1, 1));
        assert_eq!(parsed.spec.quantity_step(), Decimal::new(1, 3));
        assert_eq!(parsed.spec.minimum_quantity(), Decimal::new(1, 3));
        assert_eq!(parsed.spec.maximum_quantity(), Some(Decimal::new(100, 0)));
        assert_eq!(parsed.spec.quote_value_per_price_unit(), Decimal::ONE);
        assert_eq!(parsed.min_notional, Decimal::new(5, 0));
        Ok(())
    }

    #[test]
    fn metadata_never_falls_back_on_missing_limits_or_spot_fields() -> TestResult {
        for field in ["qtyStep", "minOrderQty", "maxOrderQty", "minNotionalValue"] {
            let mut value = metadata();
            value["result"]["list"][0]["lotSizeFilter"]
                .as_object_mut()
                .ok_or("test object")?
                .remove(field);
            assert!(parse_instrument("BTCUSDT", 200, &serde_json::to_vec(&value)?).is_err());
        }
        let mut spot_limit = metadata();
        let filter = spot_limit["result"]["list"][0]["lotSizeFilter"]
            .as_object_mut()
            .ok_or("test object")?;
        filter.remove("maxOrderQty");
        filter.insert("maxLimitOrderQty".to_owned(), json!("100"));
        assert!(parse_instrument("BTCUSDT", 200, &serde_json::to_vec(&spot_limit)?).is_err());
        for bad in [
            json!(0),
            json!(null),
            json!("0"),
            json!("0.12345678901234567890123456789"),
        ] {
            let mut value = metadata();
            value["result"]["list"][0]["priceFilter"]["tickSize"] = bad;
            assert!(parse_instrument("BTCUSDT", 200, &serde_json::to_vec(&value)?).is_err());
        }
        Ok(())
    }

    #[test]
    fn metadata_rejects_nontrading_products_symbols_and_partial_results() -> TestResult {
        for (field, value) in [
            ("symbol", "ETHUSDT"),
            ("contractType", "LinearFutures"),
            ("status", "PendingOpen"),
            ("quoteCoin", "USDC"),
            ("settleCoin", "USDC"),
            ("baseCoin", "ETH"),
        ] {
            let mut response = metadata();
            response["result"]["list"][0][field] = json!(value);
            assert!(parse_instrument("BTCUSDT", 200, &serde_json::to_vec(&response)?).is_err());
        }
        let mut response = metadata();
        response["result"]["nextPageCursor"] = json!("another-page");
        assert!(parse_instrument("BTCUSDT", 200, &serde_json::to_vec(&response)?).is_err());
        let mut response = metadata();
        response["result"]["category"] = json!("spot");
        assert!(parse_instrument("BTCUSDT", 200, &serde_json::to_vec(&response)?).is_err());
        let mut response = metadata();
        response["result"]["list"] = json!([]);
        assert!(parse_instrument("BTCUSDT", 200, &serde_json::to_vec(&response)?).is_err());
        for symbol in [
            "btcusdt",
            "BTCUSDC",
            "USDT",
            "BTCUSDT&category=spot",
            "BTC/USDT",
            "BTC USDT",
        ] {
            assert!(parse_instrument(symbol, 200, &serde_json::to_vec(&metadata())?).is_err());
        }
        Ok(())
    }

    #[test]
    fn reverse_rows_sort_and_boundary_excludes_unfinished_bar() -> TestResult {
        let body = serde_json::to_vec(&klines(vec![row(2)?, row(1)?, row(0)?], at(2)?))?;
        let candles = parse_closed_candles(&instrument()?, Timeframe::M1, at(2)?, 200, &body)?;
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].open_time(), at(0)?);
        assert_eq!(candles[1].close_time(), at(2)?);
        assert!(candles.iter().all(Candle::is_closed));
        assert_eq!(candles[0].volume_unit(), VolumeUnit::BaseAsset);
        let just_before = at(2)? - chrono::Duration::milliseconds(1);
        assert_eq!(
            parse_closed_candles(&instrument()?, Timeframe::M1, just_before, 200, &body)?.len(),
            1
        );
        let at_start = serde_json::to_vec(&klines(vec![row(0)?], at(0)?))?;
        assert!(
            parse_closed_candles(&instrument()?, Timeframe::M1, at(0)?, 200, &at_start)?.is_empty()
        );
        Ok(())
    }

    #[test]
    fn server_clock_prevents_fast_local_clock_from_closing_a_forming_bar() -> TestResult {
        let server_time = at(2)? - chrono::Duration::seconds(1);
        let body = serde_json::to_vec(&klines(vec![row(1)?, row(0)?], server_time))?;
        let bars = parse_closed_candles(&instrument()?, Timeframe::M1, at(2)?, 200, &body)?;
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].close_time(), at(1)?);
        // A whole-minute-ahead clock fails closed rather than marking the
        // venue's current candle immutable and handing it to the strategy.
        assert_eq!(
            parse_closed_candles(&instrument()?, Timeframe::M1, at(3)?, 200, &body),
            Err(PaperMarketError::ClockSkew)
        );
        let boundary = server_time + chrono::Duration::seconds(10);
        assert_eq!(
            parse_closed_candles(&instrument()?, Timeframe::M1, boundary, 200, &body)?.len(),
            1
        );
        Ok(())
    }

    #[test]
    fn stale_or_future_response_time_outside_ten_seconds_fails_closed() -> TestResult {
        for offset in [-10_001, 10_001] {
            let server_time = at(2)? + chrono::Duration::milliseconds(offset);
            let body = serde_json::to_vec(&klines(vec![row(0)?], server_time))?;
            assert_eq!(
                parse_closed_candles(&instrument()?, Timeframe::M1, at(2)?, 200, &body),
                Err(PaperMarketError::ClockSkew)
            );
        }
        Ok(())
    }

    #[test]
    fn missing_malformed_or_out_of_range_server_time_is_rejected() -> TestResult {
        let mut missing = klines(vec![row(0)?], at(2)?);
        missing.as_object_mut().ok_or("test object")?.remove("time");
        assert_eq!(
            parse_closed_candles(
                &instrument()?,
                Timeframe::M1,
                at(2)?,
                200,
                &serde_json::to_vec(&missing)?
            ),
            Err(PaperMarketError::InvalidTime)
        );
        for invalid in [
            json!(null),
            json!("1767225720000"),
            json!(true),
            json!(0),
            json!(-1),
            json!(i64::MAX),
        ] {
            let mut response = klines(vec![row(0)?], at(2)?);
            response["time"] = invalid;
            assert!(parse_closed_candles(
                &instrument()?,
                Timeframe::M1,
                at(2)?,
                200,
                &serde_json::to_vec(&response)?
            )
            .is_err());
        }
        Ok(())
    }

    #[test]
    fn exact_decimals_and_turnover_are_validated_even_for_unclosed_rows() -> TestResult {
        let mut tiny = row(0)?;
        tiny[5] = json!("0.0000000000000000000000000001");
        let parsed = parse_closed_candles(
            &instrument()?,
            Timeframe::M1,
            at(1)?,
            200,
            &serde_json::to_vec(&klines(vec![tiny], at(1)?))?,
        )?;
        assert_eq!(parsed[0].volume(), Decimal::new(1, 28));
        for (field, value) in [
            (1, json!(100)),
            (4, json!("0.12345678901234567890123456789")),
            (5, json!("-1")),
            (6, json!("NaN")),
            (6, json!("-1")),
        ] {
            let mut invalid = row(2)?;
            invalid[field] = value;
            assert!(parse_closed_candles(
                &instrument()?,
                Timeframe::M1,
                at(2)?,
                200,
                &serde_json::to_vec(&klines(vec![invalid], at(2)?))?
            )
            .is_err());
        }
        Ok(())
    }

    #[test]
    fn duplicate_gap_misalignment_and_wrong_scope_fail_closed() -> TestResult {
        let duplicate = serde_json::to_vec(&klines(vec![row(1)?, row(1)?], at(5)?))?;
        assert_eq!(
            parse_closed_candles(&instrument()?, Timeframe::M1, at(5)?, 200, &duplicate),
            Err(PaperMarketError::DuplicateCandle)
        );
        let gap = serde_json::to_vec(&klines(vec![row(2)?, row(0)?], at(5)?))?;
        assert_eq!(
            parse_closed_candles(&instrument()?, Timeframe::M1, at(5)?, 200, &gap),
            Err(PaperMarketError::CandleGap)
        );
        let mut invalid = row(1)?;
        invalid[0] = json!((at(1)?.timestamp_millis() + 1).to_string());
        assert!(parse_closed_candles(
            &instrument()?,
            Timeframe::M1,
            at(5)?,
            200,
            &serde_json::to_vec(&klines(vec![invalid], at(5)?))?
        )
        .is_err());
        let mut response = klines(vec![row(0)?], at(5)?);
        response["result"]["symbol"] = json!("ETHUSDT");
        assert!(parse_closed_candles(
            &instrument()?,
            Timeframe::M1,
            at(5)?,
            200,
            &serde_json::to_vec(&response)?
        )
        .is_err());
        response["result"]["symbol"] = json!("BTCUSDT");
        response["result"]["category"] = json!("inverse");
        assert!(parse_closed_candles(
            &instrument()?,
            Timeframe::M1,
            at(5)?,
            200,
            &serde_json::to_vec(&response)?
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn missing_retcode_http_errors_and_oversized_bodies_are_not_success() -> TestResult {
        let mut missing = metadata();
        missing
            .as_object_mut()
            .ok_or("test object")?
            .remove("retCode");
        assert!(parse_instrument("BTCUSDT", 200, &serde_json::to_vec(&missing)?).is_err());
        let mut rejected = metadata();
        rejected["retCode"] = json!(10006);
        rejected["retMsg"] = json!("sensitive detail");
        let error = parse_instrument("BTCUSDT", 200, &serde_json::to_vec(&rejected)?);
        assert_eq!(error, Err(PaperMarketError::Venue(10006)));
        assert!(!format!("{error:?}").contains("sensitive detail"));
        assert_eq!(
            parse_instrument("BTCUSDT", 503, b"{}"),
            Err(PaperMarketError::HttpStatus(503))
        );
        assert_eq!(
            parse_instrument("BTCUSDT", 200, &vec![b' '; MAX_BODY_BYTES + 1]),
            Err(PaperMarketError::ResponseTooLarge)
        );
        Ok(())
    }

    #[tokio::test]
    async fn public_requests_have_no_auth_and_warmup_preserves_closed_bar_limit() -> TestResult {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v5/market/instruments-info"))
            .and(query_param("category", "linear"))
            .and(query_param("symbol", "BTCUSDT"))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata()))
            .expect(1)
            .mount(&server)
            .await;
        let end = (at(500)?.timestamp_millis() - 1).to_string();
        let rows = (0..500).rev().map(row).collect::<Result<Vec<_>, _>>()?;
        Mock::given(method("GET"))
            .and(path("/v5/market/kline"))
            .and(query_param("category", "linear"))
            .and(query_param("symbol", "BTCUSDT"))
            .and(query_param("interval", "1"))
            .and(query_param("limit", "500"))
            .and(query_param("end", end))
            .respond_with(ResponseTemplate::new(200).set_body_json(klines(rows, at(500)?)))
            .expect(1)
            .mount(&server)
            .await;
        let market = PublicPaperMarket::local(server.uri())?;
        let current = market.instrument("BTCUSDT").await?;
        let bars = market
            .recent_closed_candles(current.spec.instrument(), Timeframe::M1, 500, at(500)?)
            .await?;
        assert_eq!(bars.len(), 500);
        for request in server.received_requests().await.ok_or("request capture")? {
            assert!(request.body.is_empty());
            assert!(!request.headers.contains_key("authorization"));
            assert!(!request.headers.contains_key("x-bapi-api-key"));
            assert!(!request.headers.contains_key("x-bapi-sign"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn public_client_does_not_follow_redirects_or_retry_http_errors() -> TestResult {
        let target = MockServer::start().await;
        let source = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v5/market/instruments-info"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", target.uri()))
            .expect(1)
            .mount(&source)
            .await;
        let market = PublicPaperMarket::local(source.uri())?;
        assert_eq!(
            market.instrument("BTCUSDT").await,
            Err(PaperMarketError::HttpStatus(302))
        );
        assert!(target
            .received_requests()
            .await
            .ok_or("request capture")?
            .is_empty());
        source.reset().await;
        Mock::given(method("GET"))
            .and(path("/v5/market/instruments-info"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&source)
            .await;
        assert_eq!(
            market.instrument("BTCUSDT").await,
            Err(PaperMarketError::HttpStatus(503))
        );
        Ok(())
    }

    #[tokio::test]
    async fn client_bounds_body_and_rejects_bad_inputs_before_network() -> TestResult {
        let server = MockServer::start().await;
        let market = PublicPaperMarket::local(server.uri())?;
        assert_eq!(
            market.instrument("BTCUSDT&x=1").await,
            Err(PaperMarketError::InvalidSymbol)
        );
        assert_eq!(
            market
                .recent_closed_candles(&instrument()?, Timeframe::M1, 0, at(5)?)
                .await,
            Err(PaperMarketError::InvalidLimit)
        );
        assert_eq!(
            market
                .recent_closed_candles(&instrument()?, Timeframe::M1, 1001, at(5)?)
                .await,
            Err(PaperMarketError::InvalidLimit)
        );
        assert!(server
            .received_requests()
            .await
            .ok_or("request capture")?
            .is_empty());
        Mock::given(method("GET"))
            .and(path("/v5/market/instruments-info"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b' '; MAX_BODY_BYTES + 1]))
            .expect(1)
            .mount(&server)
            .await;
        assert_eq!(
            market.instrument("BTCUSDT").await,
            Err(PaperMarketError::ResponseTooLarge)
        );
        Ok(())
    }
}
