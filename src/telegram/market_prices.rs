//! Display-only public quotes. Never participates in trading or candle state.

use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Asia::Ho_Chi_Minh;
use rust_decimal::Decimal;
use serde::Deserialize;

const ORIGIN: &str = "https://api.bybit.com";
const MAX_BODY_BYTES: usize = 65_536;
const DEADLINE: Duration = Duration::from_secs(3);
const MAX_CLOCK_SKEW_MS: u64 = 10_000;
pub(super) const MAX_BLOCK_CHARS: usize = 256;

pub(super) struct MarketPriceClient {
    client: reqwest::Client,
    origin: String,
    deadline: Duration,
}

impl MarketPriceClient {
    pub(super) fn new() -> Result<Self, PriceError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(DEADLINE)
            .connect_timeout(DEADLINE)
            .build()
            .map_err(|_| PriceError::Transport)?;
        Ok(Self { client, origin: ORIGIN.into(), deadline: DEADLINE })
    }

    pub(super) async fn summary(&self) -> String {
        // Capture time when the worker begins the request, not when the
        // trading loop enqueues a possibly delayed summary.
        let requested_at = Utc::now();
        match tokio::time::timeout(self.deadline, self.pair(requested_at)).await {
            Ok(Ok(pair)) => pair.block().unwrap_or_else(|_| unavailable(requested_at)),
            Ok(Err(_)) | Err(_) => unavailable(requested_at),
        }
    }

    async fn pair(&self, requested_at: DateTime<Utc>) -> Result<PricePair, PriceError> {
        let (btc, eth) = tokio::try_join!(
            self.quote("BTCUSDT", requested_at),
            self.quote("ETHUSDT", requested_at),
        )?;
        PricePair::new(btc, eth, requested_at, Utc::now())
    }

    async fn quote(&self, symbol: &'static str, requested_at: DateTime<Utc>) -> Result<Quote, PriceError> {
        // The separate public client has no token, chat ID, auth header or
        // configurable production origin. Redirects and automatic retries are off.
        let mut response = self.client.get(format!("{}/v5/market/tickers", self.origin))
            .query(&[("category", "linear"), ("symbol", symbol)])
            .send().await.map_err(|_| PriceError::Transport)?;
        if !response.status().is_success() { return Err(PriceError::Http); }
        if response.content_length().is_some_and(|length| length > MAX_BODY_BYTES as u64) {
            return Err(PriceError::Oversized);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| PriceError::Transport)? {
            if chunk.len() > MAX_BODY_BYTES.saturating_sub(body.len()) { return Err(PriceError::Oversized); }
            body.extend_from_slice(&chunk);
        }
        decode(&body, symbol, requested_at, Utc::now())
    }

    #[cfg(test)]
    pub(super) fn local(origin: String, deadline: Duration) -> Result<Self, PriceError> {
        let mut client = Self::new()?;
        client.origin = origin;
        client.deadline = deadline.min(DEADLINE);
        Ok(client)
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum PriceError {
    #[error("public quote transport unavailable")]
    Transport,
    #[error("public quote HTTP response unavailable")]
    Http,
    #[error("public quote response exceeds bound")]
    Oversized,
    #[error("invalid public quote response")]
    Invalid,
    #[error("public quote timestamp is stale or inconsistent")]
    Stale,
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "retCode")]
    code: i64,
    result: Tickers,
    time: i64,
}

#[derive(Deserialize)]
struct Tickers {
    category: String,
    list: Vec<Ticker>,
}

#[derive(Deserialize)]
struct Ticker {
    symbol: String,
    #[serde(rename = "lastPrice")]
    last_price: String,
}

#[derive(Clone, Copy)]
struct Quote {
    price: Decimal,
    server_at: DateTime<Utc>,
}

fn within_clock_bound(left: DateTime<Utc>, right: DateTime<Utc>) -> bool {
    left.signed_duration_since(right).num_milliseconds().unsigned_abs() <= MAX_CLOCK_SKEW_MS
}

fn decode(body: &[u8], expected_symbol: &str, requested_at: DateTime<Utc>, received_at: DateTime<Utc>)
    -> Result<Quote, PriceError>
{
    if body.len() > MAX_BODY_BYTES { return Err(PriceError::Oversized); }
    let envelope: Envelope = serde_json::from_slice(body).map_err(|_| PriceError::Invalid)?;
    if envelope.code != 0 || envelope.result.category != "linear" || envelope.result.list.len() != 1 {
        return Err(PriceError::Invalid);
    }
    let ticker = envelope.result.list.first().ok_or(PriceError::Invalid)?;
    if ticker.symbol != expected_symbol { return Err(PriceError::Invalid); }
    let price = crate::decimal_codec::parse_exact(&ticker.last_price).map_err(|_| PriceError::Invalid)?;
    if price <= Decimal::ZERO { return Err(PriceError::Invalid); }
    let server_at = DateTime::from_timestamp_millis(envelope.time).ok_or(PriceError::Stale)?;
    if received_at < requested_at || !within_clock_bound(server_at, requested_at)
        || !within_clock_bound(server_at, received_at) {
        return Err(PriceError::Stale);
    }
    Ok(Quote { price, server_at })
}

struct PricePair {
    btc: Decimal,
    eth: Decimal,
    observed_at: DateTime<Utc>,
}

impl PricePair {
    fn new(btc: Quote, eth: Quote, requested_at: DateTime<Utc>, observed_at: DateTime<Utc>) -> Result<Self, PriceError> {
        if observed_at < requested_at || !within_clock_bound(btc.server_at, observed_at)
            || !within_clock_bound(eth.server_at, observed_at) || !within_clock_bound(btc.server_at, eth.server_at) {
            return Err(PriceError::Stale);
        }
        Ok(Self { btc: btc.price, eth: eth.price, observed_at })
    }

    fn block(&self) -> Result<String, PriceError> {
        let block = format!("\n\nGiá tham khảo Bybit Futures (USDT)\nBTCUSDT: {}\nETHUSDT: {}\nGiá giao dịch gần nhất · Nhận lúc {} VN\nPnL vẫn theo nến M1.",
            self.btc, self.eth, local(self.observed_at));
        if block.chars().count() > MAX_BLOCK_CHARS { return Err(PriceError::Oversized); }
        Ok(block)
    }
}

fn local(at: DateTime<Utc>) -> String {
    at.with_timezone(&Ho_Chi_Minh).format("%d/%m %H:%M:%S").to_string()
}

pub(super) fn unavailable(at: DateTime<Utc>) -> String {
    format!("\n\nGiá tham khảo Bybit Futures: BTC/ETH chưa lấy được.\nKiểm tra {} VN; PnL vẫn theo nến M1.", local(at))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use wiremock::{matchers::{method, path, query_param}, Mock, MockServer, ResponseTemplate};

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn at() -> Result<DateTime<Utc>, chrono::ParseError> {
        Ok(DateTime::parse_from_rfc3339("2026-10-30T01:02:03Z")?.with_timezone(&Utc))
    }

    fn body(symbol: &str, price: &str, time: DateTime<Utc>) -> Value {
        json!({"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":symbol,"lastPrice":price}]},"time":time.timestamp_millis()})
    }

    #[test]
    fn exact_decimal_and_observation_labels_are_bounded() -> TestResult {
        let time = at()?;
        let btc = decode(&serde_json::to_vec(&body("BTCUSDT", "79228162514264337593543950335", time))?, "BTCUSDT", time, time)?;
        let eth = decode(&serde_json::to_vec(&body("ETHUSDT", "0.0000000000000000000000000001", time))?, "ETHUSDT", time, time)?;
        let text = PricePair::new(btc, eth, time, time)?.block()?;
        assert!(text.contains("BTCUSDT: 79228162514264337593543950335"));
        assert!(text.contains("ETHUSDT: 0.0000000000000000000000000001"));
        assert!(text.contains("Giá giao dịch gần nhất"));
        assert!(text.contains("Nhận lúc 30/10 08:02:03 VN"));
        assert!(text.contains("PnL vẫn theo nến M1."));
        assert!(text.starts_with("\n\n"));
        assert!(text.chars().count() <= MAX_BLOCK_CHARS);
        assert!(unavailable(time).chars().count() <= MAX_BLOCK_CHARS);
        Ok(())
    }

    #[test]
    fn invalid_prices_identity_category_code_and_missing_fields_are_rejected() -> TestResult {
        let time = at()?;
        let valid = body("BTCUSDT", "123.4500", time);
        let decoded = decode(&serde_json::to_vec(&valid)?, "BTCUSDT", time, time)?;
        assert_eq!(decoded.price.to_string(), "123.4500");
        for price in ["0", "-1", "NaN", "1e5", "", "0.00000000000000000000000000001", "79228162514264337593543950336"] {
            assert!(decode(&serde_json::to_vec(&body("BTCUSDT", price, time))?, "BTCUSDT", time, time).is_err());
        }
        for (pointer, replacement) in [
            ("/retCode", json!(10001)), ("/result/category", json!("spot")),
            ("/result/list/0/symbol", json!("ETHUSDT")), ("/result/list/0/lastPrice", json!(123)),
            ("/result/list", json!([])), ("/result/list", json!([{"symbol":"BTCUSDT","lastPrice":"1"},{"symbol":"BTCUSDT","lastPrice":"1"}])),
            ("/time", Value::Null), ("/result/list/0/lastPrice", Value::Null),
        ] {
            let mut changed = valid.clone();
            *changed.pointer_mut(pointer).ok_or("missing fixture pointer")? = replacement;
            assert!(decode(&serde_json::to_vec(&changed)?, "BTCUSDT", time, time).is_err());
        }
        let mut missing = valid;
        missing["result"]["list"][0].as_object_mut().ok_or("missing fixture row")?.remove("lastPrice");
        assert!(decode(&serde_json::to_vec(&missing)?, "BTCUSDT", time, time).is_err());
        assert!(decode(br#"{"retCode":0,"retCode":0}"#, "BTCUSDT", time, time).is_err());
        assert!(decode(&vec![b'x'; MAX_BODY_BYTES + 1], "BTCUSDT", time, time).is_err());
        Ok(())
    }

    #[test]
    fn stale_future_clock_and_pair_skew_are_rejected() -> TestResult {
        let time = at()?;
        for offset in [-10_001, 10_001] {
            let server = time + chrono::Duration::milliseconds(offset);
            assert!(decode(&serde_json::to_vec(&body("BTCUSDT", "100", server))?, "BTCUSDT", time, time).is_err());
        }
        assert!(decode(&serde_json::to_vec(&body("BTCUSDT", "100", time))?, "BTCUSDT", time,
            time - chrono::Duration::milliseconds(1)).is_err());
        let btc = Quote { price: Decimal::ONE, server_at: time - chrono::Duration::seconds(9) };
        let eth = Quote { price: Decimal::ONE, server_at: time + chrono::Duration::seconds(9) };
        assert!(PricePair::new(btc, eth, time, time).is_err());
        let btc = Quote { price: Decimal::ONE, server_at: time - chrono::Duration::seconds(9) };
        let eth = Quote { price: Decimal::ONE, server_at: time };
        assert!(PricePair::new(btc, eth, time, time + chrono::Duration::seconds(2)).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_gets_are_public_and_timeouts_forbidden_and_redirects_never_retry() -> TestResult {
        let server = MockServer::start().await;
        for (symbol, price) in [("BTCUSDT", "12345.6700"), ("ETHUSDT", "123.4567")] {
            Mock::given(method("GET")).and(path("/v5/market/tickers"))
                .and(query_param("category", "linear")).and(query_param("symbol", symbol))
                .respond_with(move |_: &wiremock::Request| ResponseTemplate::new(200)
                    .set_body_json(body(symbol, price, Utc::now())).set_delay(Duration::from_millis(100)))
                .expect(1).mount(&server).await;
        }
        let client = MarketPriceClient::local(server.uri(), Duration::from_secs(1))?;
        let summary = client.summary().await;
        assert!(summary.contains("BTCUSDT: 12345.6700"));
        assert!(summary.contains("ETHUSDT: 123.4567"));
        for request in server.received_requests().await.ok_or("missing mock requests")? {
            assert_eq!(request.body.len(), 0);
            assert!(!request.headers.contains_key("authorization"));
            assert!(!request.headers.contains_key("x-bapi-api-key"));
        }
        for status in [403, 307] {
            let server = MockServer::start().await;
            Mock::given(path("/v5/market/tickers"))
                .respond_with(ResponseTemplate::new(status).insert_header("Location", format!("{}/unexpected", server.uri())))
                .mount(&server).await;
            Mock::given(path("/unexpected")).respond_with(ResponseTemplate::new(200)).expect(0).mount(&server).await;
            let summary = MarketPriceClient::local(server.uri(), Duration::from_secs(1))?.summary().await;
            assert!(summary.contains("BTC/ETH chưa lấy được"));
            let requests = server.received_requests().await.ok_or("missing mock requests")?;
            assert!(requests.len() <= 2);
            for symbol in ["BTCUSDT", "ETHUSDT"] {
                assert!(requests.iter().filter(|r| r.url.query_pairs().any(|(key, value)| key == "symbol" && value == symbol)).count() <= 1);
            }
        }
        let server = MockServer::start().await;
        Mock::given(path("/v5/market/tickers")).respond_with(ResponseTemplate::new(200)
            .set_body_string("delayed").set_delay(Duration::from_millis(300))).mount(&server).await;
        let summary = MarketPriceClient::local(server.uri(), Duration::from_millis(30))?.summary().await;
        assert!(summary.contains("BTC/ETH chưa lấy được"));
        assert!(server.received_requests().await.ok_or("missing mock requests")?.len() <= 2);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Manual compatibility check: sends only two credential-free public Bybit GETs"]
    async fn manual_public_bybit_quote_schema_smoke() -> TestResult {
        let summary = MarketPriceClient::new()?.summary().await;
        assert!(summary.contains("BTCUSDT:"));
        assert!(summary.contains("ETHUSDT:"));
        assert!(summary.contains("Nhận lúc "));
        assert!(summary.contains(" VN\n"));
        assert!(summary.chars().count() <= MAX_BLOCK_CHARS);
        Ok(())
    }
}
