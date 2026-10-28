//! Binance USD-M Futures market-data protocol adapter.
//!
//! Binance routes candlestick streams through the `/market` WebSocket path.
//! The adapter connects in combined-stream mode, but accepts both the combined
//! envelope and the raw event shape so callers can also use it with captured
//! fixtures or a custom raw-stream endpoint.

use std::{borrow::Cow, collections::HashSet, time::Duration};

use serde::{de::IgnoredAny, Deserialize, Serialize};
use url::Url;

use crate::models::{Exchange, InstrumentId, Timeframe, VolumeUnit};

use super::{
    normalize_candle, AdapterError, ExchangeMarketData, HeartbeatSpec, InboundMarketEvent,
    MarketSubscription, OutboundMessage, SubscriptionIndex, WireCandle,
};

/// Current production endpoint for Binance USD-M regular market streams.
///
/// Kline streams belong to the `/market` route. The legacy unrouted endpoint
/// only carries streams assigned to Binance's `/public` route.
pub const BINANCE_USD_M_MARKET_STREAM_ENDPOINT: &str = "wss://fstream.binance.com/market/stream";

/// Proactive rotation age for one Binance market-stream connection.
///
/// Binance enforces a 24-hour hard lifetime. Rotating ten minutes earlier gives
/// the data engine time to reconnect and resubscribe before that deadline.
pub const BINANCE_CONNECTION_MAX_AGE: Duration = Duration::from_secs((23 * 60 * 60) + (50 * 60));

const SUBSCRIPTION_REQUEST_ID: u64 = 1;
const MAX_STREAMS_PER_CONNECTION: usize = 1_024;
const PROACTIVE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(150);

/// Binance USD-M Futures candlestick adapter.
#[derive(Clone, Debug)]
pub struct BinanceMarketData {
    endpoint: Url,
    index: SubscriptionIndex,
}

impl BinanceMarketData {
    /// Creates an adapter for Binance's production USD-M market endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when subscriptions are empty, duplicated,
    /// assigned to another exchange, or exceed Binance's per-connection limit.
    pub fn new(subscriptions: Vec<MarketSubscription>) -> Result<Self, AdapterError> {
        Self::with_endpoint(subscriptions, BINANCE_USD_M_MARKET_STREAM_ENDPOINT)
    }

    /// Creates an adapter with an explicit WebSocket endpoint.
    ///
    /// This is useful for a proxy, a recorder, or a Binance-compatible test
    /// environment. The caller remains responsible for choosing the `/market`
    /// route when subscribing to production kline streams.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when the endpoint is not a valid URL or the
    /// subscription set violates adapter constraints.
    pub fn with_endpoint(
        subscriptions: Vec<MarketSubscription>,
        endpoint: &str,
    ) -> Result<Self, AdapterError> {
        let endpoint_url =
            Url::parse(endpoint).map_err(|source| AdapterError::InvalidEndpoint {
                exchange: Exchange::Binance,
                endpoint: endpoint.to_owned(),
                source,
            })?;
        let index = SubscriptionIndex::new(Exchange::Binance, subscriptions)?;
        validate_subscriptions(index.subscriptions())?;

        Ok(Self {
            endpoint: endpoint_url,
            index,
        })
    }

    fn parse_kline(
        &self,
        stream: Option<&str>,
        event: &BinanceKlineEvent<'_>,
    ) -> Result<InboundMarketEvent, AdapterError> {
        if event.event_type != "kline" {
            return Err(invalid_message("unexpected Binance event type"));
        }

        let timeframe = parse_timeframe(event.kline.interval.as_ref())?;
        if !event
            .symbol
            .eq_ignore_ascii_case(event.kline.symbol.as_ref())
        {
            return Err(invalid_message(
                "Binance outer and nested kline symbols disagree",
            ));
        }

        if let Some(stream_name) = stream {
            validate_stream_name(
                stream_name,
                event.symbol.as_ref(),
                event.kline.interval.as_ref(),
            )?;
        }

        validate_kline_times(
            event.kline.open_time_millis,
            event.kline.close_time_millis,
            timeframe,
        )?;

        let instrument = self.instrument(event.symbol.as_ref(), timeframe)?;
        let candle = normalize_candle(WireCandle {
            exchange: Exchange::Binance,
            instrument,
            timeframe,
            open_time_millis: event.kline.open_time_millis,
            open: event.kline.open.as_ref(),
            high: event.kline.high.as_ref(),
            low: event.kline.low.as_ref(),
            close: event.kline.close.as_ref(),
            volume: event.kline.volume.as_ref(),
            volume_unit: VolumeUnit::BaseAsset,
            trade_count: Some(event.kline.trade_count),
            is_closed: event.kline.is_closed,
        })?;

        Ok(InboundMarketEvent::Candle(candle))
    }

    fn instrument(
        &self,
        wire_symbol: &str,
        timeframe: Timeframe,
    ) -> Result<InstrumentId, AdapterError> {
        if let Ok(instrument) = self.index.instrument(wire_symbol, timeframe) {
            return Ok(instrument);
        }

        self.index
            .subscriptions()
            .iter()
            .find(|subscription| {
                subscription.timeframe == timeframe
                    && subscription
                        .instrument
                        .venue_symbol
                        .as_str()
                        .eq_ignore_ascii_case(wire_symbol)
            })
            .map(|subscription| subscription.instrument.clone())
            .ok_or_else(|| AdapterError::UnknownSubscription {
                exchange: Exchange::Binance,
                symbol: wire_symbol.to_owned(),
                timeframe,
            })
    }
}

impl ExchangeMarketData for BinanceMarketData {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }

    fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    fn subscriptions(&self) -> &[MarketSubscription] {
        self.index.subscriptions()
    }

    fn subscription_messages(&self) -> Result<Vec<OutboundMessage>, AdapterError> {
        let params = self
            .index
            .subscriptions()
            .iter()
            .map(|subscription| {
                format!(
                    "{}@kline_{}",
                    subscription
                        .instrument
                        .venue_symbol
                        .as_str()
                        .to_ascii_lowercase(),
                    subscription.timeframe.as_str()
                )
            })
            .collect();
        let request = SubscriptionRequest {
            method: "SUBSCRIBE",
            params,
            id: SUBSCRIPTION_REQUEST_ID,
        };
        let payload = serde_json::to_string(&request).map_err(|source| AdapterError::Json {
            exchange: Exchange::Binance,
            source,
        })?;

        Ok(vec![OutboundMessage::Text(payload)])
    }

    fn heartbeat(&self) -> HeartbeatSpec {
        // Binance sends its own protocol ping every three minutes. This empty,
        // proactive client ping is a liveness probe; the transport must still
        // echo every server Ping payload in its corresponding Pong frame.
        HeartbeatSpec {
            interval: PROACTIVE_HEARTBEAT_INTERVAL,
            message: OutboundMessage::Ping(Vec::new()),
        }
    }

    fn max_connection_age(&self) -> Option<Duration> {
        Some(BINANCE_CONNECTION_MAX_AGE)
    }

    fn parse_message(&self, payload: &[u8]) -> Result<Vec<InboundMarketEvent>, AdapterError> {
        let message = match serde_json::from_slice::<BinanceInbound<'_>>(payload) {
            Ok(message) => message,
            Err(source) => {
                if serde_json::from_slice::<IgnoredAny>(payload).is_ok() {
                    return Err(invalid_message("unsupported Binance message shape"));
                }
                return Err(AdapterError::Json {
                    exchange: Exchange::Binance,
                    source,
                });
            }
        };

        let event = match message {
            BinanceInbound::Combined(envelope) => {
                self.parse_kline(Some(&envelope.stream), &envelope.data)?
            }
            BinanceInbound::Kline(event) => self.parse_kline(None, &event)?,
            BinanceInbound::Error(error) => {
                return Err(protocol_error(&error));
            }
            BinanceInbound::NestedError(error) => {
                return Err(protocol_error(&error.error));
            }
            BinanceInbound::Acknowledgement(acknowledgement) => {
                if !acknowledgement.id.is_valid() {
                    return Err(invalid_message(
                        "Binance acknowledgement has an empty request id",
                    ));
                }
                InboundMarketEvent::SubscriptionAcknowledged
            }
        };

        Ok(vec![event])
    }
}

fn validate_subscriptions(subscriptions: &[MarketSubscription]) -> Result<(), AdapterError> {
    if subscriptions.len() > MAX_STREAMS_PER_CONNECTION {
        return Err(AdapterError::Protocol {
            exchange: Exchange::Binance,
            message: format!(
                "{} streams exceed Binance's {MAX_STREAMS_PER_CONNECTION}-stream connection limit",
                subscriptions.len()
            ),
        });
    }

    let mut normalized = HashSet::with_capacity(subscriptions.len());
    for subscription in subscriptions {
        let key = (
            subscription
                .instrument
                .venue_symbol
                .as_str()
                .to_ascii_lowercase(),
            subscription.timeframe,
        );
        if !normalized.insert(key.clone()) {
            return Err(AdapterError::DuplicateSubscription {
                exchange: Exchange::Binance,
                symbol: key.0,
                timeframe: key.1,
            });
        }
    }

    Ok(())
}

fn parse_timeframe(value: &str) -> Result<Timeframe, AdapterError> {
    match value {
        "1m" => Ok(Timeframe::M1),
        "3m" => Ok(Timeframe::M3),
        "5m" => Ok(Timeframe::M5),
        "15m" => Ok(Timeframe::M15),
        _ => Err(AdapterError::UnsupportedTimeframe {
            exchange: Exchange::Binance,
            value: value.to_owned(),
        }),
    }
}

fn validate_stream_name(
    stream: &str,
    event_symbol: &str,
    event_interval: &str,
) -> Result<(), AdapterError> {
    let Some((stream_symbol, stream_interval)) = stream.split_once("@kline_") else {
        return Err(invalid_message("invalid Binance combined stream name"));
    };

    if stream_symbol.is_empty()
        || stream_interval.is_empty()
        || stream_interval.contains('@')
        || !stream_symbol.eq_ignore_ascii_case(event_symbol)
        || stream_interval != event_interval
    {
        return Err(invalid_message(
            "Binance combined stream name disagrees with its kline payload",
        ));
    }

    Ok(())
}

fn validate_kline_times(
    open_time_millis: i64,
    inclusive_close_time_millis: i64,
    timeframe: Timeframe,
) -> Result<(), AdapterError> {
    let duration_millis = timeframe.duration().num_milliseconds();
    if open_time_millis.rem_euclid(duration_millis) != 0 {
        return Err(invalid_message(
            "Binance kline open time is not interval-aligned",
        ));
    }

    let expected_close = open_time_millis
        .checked_add(duration_millis)
        .and_then(|value| value.checked_sub(1))
        .ok_or(AdapterError::InvalidTimestamp {
            exchange: Exchange::Binance,
            millis: open_time_millis,
        })?;
    if inclusive_close_time_millis != expected_close {
        return Err(invalid_message(
            "Binance kline close time does not match its interval",
        ));
    }

    Ok(())
}

fn protocol_error(error: &BinanceProtocolError<'_>) -> AdapterError {
    AdapterError::Protocol {
        exchange: Exchange::Binance,
        message: format!("code {}: {}", error.code, error.message),
    }
}

const fn invalid_message(message: &'static str) -> AdapterError {
    AdapterError::InvalidMessage {
        exchange: Exchange::Binance,
        message,
    }
}

#[derive(Serialize)]
struct SubscriptionRequest {
    method: &'static str,
    params: Vec<String>,
    id: u64,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BinanceInbound<'a> {
    Combined(#[serde(borrow)] CombinedEnvelope<'a>),
    Kline(#[serde(borrow)] BinanceKlineEvent<'a>),
    Error(#[serde(borrow)] BinanceProtocolError<'a>),
    NestedError(#[serde(borrow)] NestedProtocolError<'a>),
    Acknowledgement(#[serde(borrow)] SubscriptionAcknowledgement<'a>),
}

#[derive(Deserialize)]
struct CombinedEnvelope<'a> {
    #[serde(borrow)]
    stream: Cow<'a, str>,
    #[serde(borrow)]
    data: BinanceKlineEvent<'a>,
}

#[derive(Deserialize)]
struct BinanceKlineEvent<'a> {
    #[serde(rename = "e", borrow)]
    event_type: Cow<'a, str>,
    #[serde(rename = "E")]
    _event_time_millis: i64,
    #[serde(rename = "s", borrow)]
    symbol: Cow<'a, str>,
    #[serde(rename = "k", borrow)]
    kline: BinanceKline<'a>,
}

#[derive(Deserialize)]
struct BinanceKline<'a> {
    #[serde(rename = "t")]
    open_time_millis: i64,
    #[serde(rename = "T")]
    close_time_millis: i64,
    #[serde(rename = "s", borrow)]
    symbol: Cow<'a, str>,
    #[serde(rename = "i", borrow)]
    interval: Cow<'a, str>,
    #[serde(rename = "o", borrow)]
    open: Cow<'a, str>,
    #[serde(rename = "c", borrow)]
    close: Cow<'a, str>,
    #[serde(rename = "h", borrow)]
    high: Cow<'a, str>,
    #[serde(rename = "l", borrow)]
    low: Cow<'a, str>,
    #[serde(rename = "v", borrow)]
    volume: Cow<'a, str>,
    #[serde(rename = "n")]
    trade_count: u64,
    #[serde(rename = "x")]
    is_closed: bool,
}

#[derive(Deserialize)]
struct BinanceProtocolError<'a> {
    code: i64,
    #[serde(rename = "msg", borrow)]
    message: Cow<'a, str>,
}

#[derive(Deserialize)]
struct NestedProtocolError<'a> {
    #[serde(borrow)]
    error: BinanceProtocolError<'a>,
}

#[derive(Deserialize)]
struct SubscriptionAcknowledgement<'a> {
    #[serde(rename = "result")]
    _result: (),
    #[serde(borrow)]
    id: RequestId<'a>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RequestId<'a> {
    Number(u64),
    String(#[serde(borrow)] Cow<'a, str>),
}

impl RequestId<'_> {
    fn is_valid(&self) -> bool {
        match self {
            Self::Number(_value) => true,
            Self::String(value) => !value.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use rust_decimal::Decimal;
    use serde_json::{json, Value};

    use crate::models::{Candle, MarketKind, ModelError};

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    const RAW_M1: &str = r#"{
        "e":"kline",
        "E":1638747720001,
        "s":"BTCUSDT",
        "k":{
            "t":1638747660000,
            "T":1638747719999,
            "s":"BTCUSDT",
            "i":"1m",
            "f":100,
            "L":200,
            "o":"40000.1200",
            "c":"40010.3400",
            "h":"40020.5000",
            "l":"39990.0000",
            "v":"12.345600",
            "n":100,
            "x":false,
            "q":"493912.3000",
            "V":"7.000000",
            "Q":"280043.0000",
            "B":"0"
        }
    }"#;

    const COMBINED_M5: &str = r#"{
        "stream":"btcusdt@kline_5m",
        "data":{
            "e":"kline",
            "E":1638747900001,
            "s":"BTCUSDT",
            "k":{
                "t":1638747600000,
                "T":1638747899999,
                "s":"BTCUSDT",
                "i":"5m",
                "f":201,
                "L":250,
                "o":"40000.00000000",
                "c":"40100.25000000",
                "h":"40125.50000000",
                "l":"39950.00000000",
                "v":"25.12500000",
                "n":50,
                "x":true,
                "q":"1005000.00000000",
                "V":"15.00000000",
                "Q":"600000.00000000",
                "B":"0"
            }
        }
    }"#;

    fn instrument(symbol: &str) -> Result<InstrumentId, ModelError> {
        InstrumentId::new(
            Exchange::Binance,
            MarketKind::LinearPerpetual,
            symbol,
            "BTC-USDT-PERP",
        )
    }

    fn subscriptions() -> Result<Vec<MarketSubscription>, ModelError> {
        Ok(vec![
            MarketSubscription::new(instrument("BTCUSDT")?, Timeframe::M1),
            MarketSubscription::new(instrument("BTCUSDT")?, Timeframe::M5),
        ])
    }

    fn adapter() -> Result<BinanceMarketData, Box<dyn Error>> {
        Ok(BinanceMarketData::new(subscriptions()?)?)
    }

    fn only_candle(events: &[InboundMarketEvent]) -> Result<Candle, io::Error> {
        match events {
            [InboundMarketEvent::Candle(candle)] => Ok(candle.clone()),
            _ => Err(io::Error::other("expected exactly one candle event")),
        }
    }

    #[test]
    fn uses_current_market_route() -> TestResult {
        let adapter = adapter()?;
        assert_eq!(
            adapter.endpoint().as_str(),
            BINANCE_USD_M_MARKET_STREAM_ENDPOINT
        );
        assert_eq!(adapter.exchange(), Exchange::Binance);
        Ok(())
    }

    #[test]
    fn builds_one_lowercase_subscription_request() -> TestResult {
        let messages = adapter()?.subscription_messages()?;
        let [OutboundMessage::Text(payload)] = messages.as_slice() else {
            return Err(io::Error::other("expected one text subscription message").into());
        };
        let value: Value = serde_json::from_str(payload)?;

        assert_eq!(
            value,
            json!({
                "method": "SUBSCRIBE",
                "params": ["btcusdt@kline_1m", "btcusdt@kline_5m"],
                "id": 1
            })
        );
        Ok(())
    }

    #[test]
    fn exposes_proactive_control_frame_heartbeat() -> TestResult {
        let heartbeat = adapter()?.heartbeat();
        assert_eq!(heartbeat.interval, Duration::from_secs(150));
        assert_eq!(heartbeat.message, OutboundMessage::Ping(Vec::new()));
        assert_eq!(BINANCE_CONNECTION_MAX_AGE, Duration::from_secs(85_800));
        assert_eq!(
            adapter()?.max_connection_age(),
            Some(Duration::from_secs(85_800))
        );
        Ok(())
    }

    #[test]
    fn parses_raw_partial_kline_without_float_rounding() -> TestResult {
        let candle = only_candle(&adapter()?.parse_message(RAW_M1.as_bytes())?)?;

        assert_eq!(candle.timeframe(), Timeframe::M1);
        assert_eq!(candle.open_time().timestamp_millis(), 1_638_747_660_000);
        assert_eq!(candle.close_time().timestamp_millis(), 1_638_747_720_000);
        assert_eq!(candle.open(), Decimal::new(400_001_200, 4));
        assert_eq!(candle.open().scale(), 4);
        assert_eq!(candle.close(), Decimal::new(400_103_400, 4));
        assert_eq!(candle.volume(), Decimal::new(123_456, 4));
        assert_eq!(candle.volume().scale(), 6);
        assert_eq!(candle.volume_unit(), VolumeUnit::BaseAsset);
        assert_eq!(candle.trade_count(), Some(100));
        assert!(!candle.is_closed());
        Ok(())
    }

    #[test]
    fn parses_combined_closed_kline() -> TestResult {
        let candle = only_candle(&adapter()?.parse_message(COMBINED_M5.as_bytes())?)?;

        assert_eq!(candle.timeframe(), Timeframe::M5);
        assert_eq!(candle.open_time().timestamp_millis(), 1_638_747_600_000);
        assert_eq!(candle.close_time().timestamp_millis(), 1_638_747_900_000);
        assert_eq!(candle.trade_count(), Some(50));
        assert!(candle.is_closed());
        Ok(())
    }

    #[test]
    fn accepts_numeric_and_string_subscription_acknowledgements() -> TestResult {
        for payload in [
            br#"{"result":null,"id":1}"#.as_slice(),
            br#"{"result":null,"id":"request-1"}"#.as_slice(),
        ] {
            assert_eq!(
                adapter()?.parse_message(payload)?,
                vec![InboundMarketEvent::SubscriptionAcknowledged]
            );
        }
        Ok(())
    }

    #[test]
    fn converts_direct_and_nested_protocol_errors() -> TestResult {
        for payload in [
            br#"{"code":2,"msg":"Invalid request"}"#.as_slice(),
            br#"{"error":{"code":3,"msg":"Invalid JSON"},"id":1}"#.as_slice(),
        ] {
            let error = adapter()?
                .parse_message(payload)
                .err()
                .ok_or_else(|| io::Error::other("expected Binance protocol error"))?;
            assert!(matches!(error, AdapterError::Protocol { .. }));
        }
        Ok(())
    }

    #[test]
    fn rejects_unknown_or_malformed_payloads_distinctly() -> TestResult {
        let unknown = adapter()?
            .parse_message(br#"{"event":"unknown"}"#)
            .err()
            .ok_or_else(|| io::Error::other("expected invalid message error"))?;
        assert!(matches!(unknown, AdapterError::InvalidMessage { .. }));

        let malformed = adapter()?
            .parse_message(br#"{"e":"kline""#)
            .err()
            .ok_or_else(|| io::Error::other("expected JSON error"))?;
        assert!(matches!(malformed, AdapterError::Json { .. }));
        Ok(())
    }

    #[test]
    fn rejects_invalid_decimal_before_domain_normalization() -> TestResult {
        let payload = RAW_M1.replace("40000.1200", "not-a-decimal");
        let error = adapter()?
            .parse_message(payload.as_bytes())
            .err()
            .ok_or_else(|| io::Error::other("expected decimal error"))?;
        assert!(matches!(
            error,
            AdapterError::InvalidDecimal { field: "open", .. }
        ));
        Ok(())
    }

    #[test]
    fn rejects_json_number_for_price() -> TestResult {
        let payload = RAW_M1.replace("\"40000.1200\"", "40000.1200");
        let error = adapter()?
            .parse_message(payload.as_bytes())
            .err()
            .ok_or_else(|| io::Error::other("expected message-shape error"))?;
        assert!(matches!(error, AdapterError::InvalidMessage { .. }));
        Ok(())
    }

    #[test]
    fn rejects_stream_payload_disagreement() -> TestResult {
        let payload = COMBINED_M5.replace("btcusdt@kline_5m", "ethusdt@kline_5m");
        let error = adapter()?
            .parse_message(payload.as_bytes())
            .err()
            .ok_or_else(|| io::Error::other("expected stream mismatch error"))?;
        assert!(matches!(error, AdapterError::InvalidMessage { .. }));
        Ok(())
    }

    #[test]
    fn rejects_outer_and_nested_symbol_disagreement() -> TestResult {
        let payload = RAW_M1.replacen("BTCUSDT", "ETHUSDT", 1);
        let error = adapter()?
            .parse_message(payload.as_bytes())
            .err()
            .ok_or_else(|| io::Error::other("expected symbol mismatch error"))?;
        assert!(matches!(error, AdapterError::InvalidMessage { .. }));
        Ok(())
    }

    #[test]
    fn rejects_inclusive_close_time_that_disagrees_with_interval() -> TestResult {
        let payload = RAW_M1.replace("1638747719999", "1638747720000");
        let error = adapter()?
            .parse_message(payload.as_bytes())
            .err()
            .ok_or_else(|| io::Error::other("expected close-time error"))?;
        assert!(matches!(error, AdapterError::InvalidMessage { .. }));
        Ok(())
    }

    #[test]
    fn rejects_unconfigured_timeframe_and_unsupported_interval() -> TestResult {
        let unconfigured = RAW_M1
            .replace("\"i\":\"1m\"", "\"i\":\"3m\"")
            .replace("1638747660000", "1638747540000");
        let error = adapter()?
            .parse_message(unconfigured.as_bytes())
            .err()
            .ok_or_else(|| io::Error::other("expected unknown subscription error"))?;
        assert!(matches!(
            error,
            AdapterError::UnknownSubscription {
                timeframe: Timeframe::M3,
                ..
            }
        ));

        let unsupported = RAW_M1.replace("\"i\":\"1m\"", "\"i\":\"1h\"");
        let error = adapter()?
            .parse_message(unsupported.as_bytes())
            .err()
            .ok_or_else(|| io::Error::other("expected unsupported timeframe error"))?;
        assert!(matches!(error, AdapterError::UnsupportedTimeframe { .. }));
        Ok(())
    }

    #[test]
    fn rejects_case_insensitive_duplicate_subscriptions() -> TestResult {
        let subscriptions = vec![
            MarketSubscription::new(instrument("BTCUSDT")?, Timeframe::M1),
            MarketSubscription::new(instrument("btcusdt")?, Timeframe::M1),
        ];
        let error = BinanceMarketData::new(subscriptions)
            .err()
            .ok_or_else(|| io::Error::other("expected duplicate subscription error"))?;

        assert!(matches!(error, AdapterError::DuplicateSubscription { .. }));
        Ok(())
    }

    #[test]
    fn rejects_invalid_endpoint() -> TestResult {
        let error = BinanceMarketData::with_endpoint(subscriptions()?, "://not-a-url")
            .err()
            .ok_or_else(|| io::Error::other("expected endpoint error"))?;
        assert!(matches!(error, AdapterError::InvalidEndpoint { .. }));
        Ok(())
    }
}
