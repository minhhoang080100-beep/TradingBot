//! OKX public candlestick adapter.
//!
//! OKX classifies candlesticks as public market data, but serves the channel
//! from the unauthenticated `business` WebSocket endpoint. The adapter uses
//! the port-443 endpoint because OKX is retiring explicit port 8443.

use std::{borrow::Cow, time::Duration};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::models::{Exchange, Timeframe, VolumeUnit};

use super::{
    normalize_candle, AdapterError, ExchangeMarketData, HeartbeatSpec, InboundMarketEvent,
    MarketSubscription, OutboundMessage, SubscriptionIndex, WireCandle,
};

/// Production OKX candlestick WebSocket endpoint over the default TLS port.
pub const PRODUCTION_ENDPOINT: &str = "wss://ws.okx.com/ws/v5/business";

/// OKX demo-trading candlestick WebSocket endpoint over the default TLS port.
pub const DEMO_ENDPOINT: &str = "wss://wspap.okx.com/ws/v5/business";

const SUBSCRIPTION_REQUEST_ID: &str = "ictmd1";
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

/// Public OKX candlestick protocol adapter.
#[derive(Clone, Debug)]
pub struct OkxMarketData {
    endpoint: Url,
    index: SubscriptionIndex,
}

impl OkxMarketData {
    /// Creates an adapter connected to the production business endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when subscriptions are empty, duplicated, or
    /// contain an instrument belonging to another exchange.
    pub fn new(subscriptions: Vec<MarketSubscription>) -> Result<Self, AdapterError> {
        Self::with_endpoint(subscriptions, PRODUCTION_ENDPOINT)
    }

    /// Creates an adapter with an explicit OKX-compatible endpoint.
    ///
    /// This is useful for demo trading and deterministic integration tests.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when `endpoint` is not a valid URL or when the
    /// subscriptions fail common validation.
    pub fn with_endpoint(
        subscriptions: Vec<MarketSubscription>,
        endpoint: &str,
    ) -> Result<Self, AdapterError> {
        let endpoint = Url::parse(endpoint).map_err(|source| AdapterError::InvalidEndpoint {
            exchange: Exchange::Okx,
            endpoint: endpoint.to_owned(),
            source,
        })?;
        let index = SubscriptionIndex::new(Exchange::Okx, subscriptions)?;

        Ok(Self { endpoint, index })
    }

    fn parse_subscription_acknowledgement(
        &self,
        message: &OkxInboundMessage<'_>,
    ) -> Result<Vec<InboundMarketEvent>, AdapterError> {
        if message.code.as_deref().is_some_and(|code| code != "0") {
            return Err(protocol_error(message));
        }

        let argument = message.arg.as_ref().ok_or(AdapterError::InvalidMessage {
            exchange: Exchange::Okx,
            message: "OKX subscription acknowledgement omitted arg",
        })?;
        let timeframe = timeframe_from_channel(argument.channel.as_ref())?;
        self.index
            .instrument(argument.inst_id.as_ref(), timeframe)?;

        Ok(vec![InboundMarketEvent::SubscriptionAcknowledged])
    }

    fn parse_candle_data(
        &self,
        message: OkxInboundMessage<'_>,
    ) -> Result<Vec<InboundMarketEvent>, AdapterError> {
        let argument = message.arg.ok_or(AdapterError::InvalidMessage {
            exchange: Exchange::Okx,
            message: "OKX candle message omitted arg",
        })?;
        let rows = message.data.ok_or(AdapterError::InvalidMessage {
            exchange: Exchange::Okx,
            message: "OKX candle message omitted data",
        })?;
        let timeframe = timeframe_from_channel(argument.channel.as_ref())?;
        let instrument = self
            .index
            .instrument(argument.inst_id.as_ref(), timeframe)?;
        let mut events = Vec::with_capacity(rows.len());

        for row in rows {
            let [timestamp, open, high, low, close, volume, _volume_currency, _volume_quote, confirm] =
                row.as_slice()
            else {
                return Err(AdapterError::InvalidMessage {
                    exchange: Exchange::Okx,
                    message: "OKX candle row must contain exactly nine string fields",
                });
            };

            let open_time_millis =
                timestamp
                    .parse::<i64>()
                    .map_err(|_| AdapterError::InvalidMessage {
                        exchange: Exchange::Okx,
                        message: "OKX candle timestamp must be a signed millisecond integer",
                    })?;
            let is_closed = match confirm.as_ref() {
                "0" => false,
                "1" => true,
                _ => {
                    return Err(AdapterError::InvalidMessage {
                        exchange: Exchange::Okx,
                        message: "OKX candle confirm flag must be 0 or 1",
                    });
                }
            };

            let candle = normalize_candle(WireCandle {
                exchange: Exchange::Okx,
                instrument: instrument.clone(),
                timeframe,
                open_time_millis,
                open: open.as_ref(),
                high: high.as_ref(),
                low: low.as_ref(),
                close: close.as_ref(),
                volume: volume.as_ref(),
                volume_unit: VolumeUnit::Contracts,
                trade_count: None,
                is_closed,
            })?;
            events.push(InboundMarketEvent::Candle(candle));
        }

        Ok(events)
    }
}

impl ExchangeMarketData for OkxMarketData {
    fn exchange(&self) -> Exchange {
        Exchange::Okx
    }

    fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    fn subscriptions(&self) -> &[MarketSubscription] {
        self.index.subscriptions()
    }

    fn subscription_messages(&self) -> Result<Vec<OutboundMessage>, AdapterError> {
        let args = self
            .index
            .subscriptions()
            .iter()
            .map(|subscription| OkxSubscriptionArgument {
                channel: channel_for_timeframe(subscription.timeframe),
                inst_id: subscription.instrument.venue_symbol.as_str(),
            })
            .collect();
        let request = OkxSubscriptionRequest {
            id: SUBSCRIPTION_REQUEST_ID,
            operation: "subscribe",
            args,
        };
        let payload = serde_json::to_string(&request).map_err(|source| AdapterError::Json {
            exchange: Exchange::Okx,
            source,
        })?;

        Ok(vec![OutboundMessage::Text(payload)])
    }

    fn heartbeat(&self) -> HeartbeatSpec {
        HeartbeatSpec {
            interval: HEARTBEAT_INTERVAL,
            message: OutboundMessage::Text("ping".to_owned()),
        }
    }

    fn parse_message(&self, payload: &[u8]) -> Result<Vec<InboundMarketEvent>, AdapterError> {
        if payload == b"pong" {
            return Ok(vec![InboundMarketEvent::Heartbeat]);
        }

        let message: OkxInboundMessage<'_> =
            serde_json::from_slice(payload).map_err(|source| AdapterError::Json {
                exchange: Exchange::Okx,
                source,
            })?;

        match message.event.as_deref() {
            Some("subscribe") => self.parse_subscription_acknowledgement(&message),
            Some("error" | "notice") => Err(protocol_error(&message)),
            Some(_) => Err(AdapterError::InvalidMessage {
                exchange: Exchange::Okx,
                message: "unsupported OKX event message",
            }),
            None => self.parse_candle_data(message),
        }
    }
}

#[derive(Serialize)]
struct OkxSubscriptionRequest<'a> {
    id: &'static str,
    #[serde(rename = "op")]
    operation: &'static str,
    args: Vec<OkxSubscriptionArgument<'a>>,
}

#[derive(Serialize)]
struct OkxSubscriptionArgument<'a> {
    channel: &'static str,
    #[serde(rename = "instId")]
    inst_id: &'a str,
}

#[derive(Deserialize)]
struct OkxInboundMessage<'a> {
    #[serde(default, borrow)]
    event: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    code: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    msg: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    arg: Option<OkxInboundArgument<'a>>,
    #[serde(default, borrow)]
    data: Option<Vec<Vec<Cow<'a, str>>>>,
}

#[derive(Deserialize)]
struct OkxInboundArgument<'a> {
    #[serde(borrow)]
    channel: Cow<'a, str>,
    #[serde(rename = "instId", borrow)]
    inst_id: Cow<'a, str>,
}

fn channel_for_timeframe(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::M1 => "candle1m",
        Timeframe::M3 => "candle3m",
        Timeframe::M5 => "candle5m",
        Timeframe::M15 => "candle15m",
    }
}

fn timeframe_from_channel(channel: &str) -> Result<Timeframe, AdapterError> {
    match channel {
        "candle1m" => Ok(Timeframe::M1),
        "candle3m" => Ok(Timeframe::M3),
        "candle5m" => Ok(Timeframe::M5),
        "candle15m" => Ok(Timeframe::M15),
        value => Err(AdapterError::UnsupportedTimeframe {
            exchange: Exchange::Okx,
            value: value.to_owned(),
        }),
    }
}

fn protocol_error(message: &OkxInboundMessage<'_>) -> AdapterError {
    let code = message.code.as_deref().unwrap_or("unknown");
    let detail = message.msg.as_deref().unwrap_or("no message supplied");
    AdapterError::Protocol {
        exchange: Exchange::Okx,
        message: format!("code {code}: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use chrono::{DateTime, Utc};
    use rust_decimal::Decimal;
    use serde_json::Value;

    use crate::models::{InstrumentId, MarketKind};

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    const INCOMPLETE_M1: &[u8] = br#"{
        "arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},
        "data":[["1735689600000","93500.10","93520.00","93480.20","93505.60","1234.500","12.34","1153790.00","0"]]
    }"#;

    const CLOSED_M5: &[u8] = br#"{
        "arg":{"channel":"candle5m","instId":"BTC-USDT-SWAP"},
        "data":[["1735689600000","93500.10","93610.00","93450.00","93580.40","6420","64.20","6004932.00","1"]]
    }"#;

    fn instrument() -> Result<InstrumentId, crate::models::ModelError> {
        InstrumentId::new(
            Exchange::Okx,
            MarketKind::LinearPerpetual,
            "BTC-USDT-SWAP",
            "BTC-USDT-PERP",
        )
    }

    fn adapter() -> Result<OkxMarketData, AdapterError> {
        let instrument = instrument()?;
        OkxMarketData::new(vec![
            MarketSubscription::new(instrument.clone(), Timeframe::M1),
            MarketSubscription::new(instrument, Timeframe::M5),
        ])
    }

    fn one_event(payload: &[u8]) -> Result<InboundMarketEvent, Box<dyn Error>> {
        let mut events = adapter()?.parse_message(payload)?;
        if events.len() != 1 {
            return Err(format!("expected one event, received {}", events.len()).into());
        }
        events
            .pop()
            .ok_or_else(|| "event vector unexpectedly became empty".into())
    }

    #[test]
    fn production_endpoint_uses_business_path_on_port_443() -> TestResult {
        let adapter = adapter()?;

        assert_eq!(adapter.endpoint().as_str(), PRODUCTION_ENDPOINT);
        assert_eq!(adapter.endpoint().scheme(), "wss");
        assert_eq!(adapter.endpoint().host_str(), Some("ws.okx.com"));
        assert_eq!(adapter.endpoint().port_or_known_default(), Some(443));
        assert_eq!(adapter.endpoint().path(), "/ws/v5/business");
        assert!(!adapter.endpoint().as_str().contains(":8443"));
        Ok(())
    }

    #[test]
    fn subscription_contains_all_configured_candle_channels() -> TestResult {
        let messages = adapter()?.subscription_messages()?;
        let Some(OutboundMessage::Text(payload)) = messages.first() else {
            return Err("expected one text subscription message".into());
        };
        let value: Value = serde_json::from_str(payload)?;

        assert_eq!(messages.len(), 1);
        assert_eq!(value.get("id").and_then(Value::as_str), Some("ictmd1"));
        assert_eq!(value.get("op").and_then(Value::as_str), Some("subscribe"));
        assert_eq!(
            value.get("args").and_then(Value::as_array).map(Vec::len),
            Some(2)
        );
        assert_eq!(
            value.pointer("/args/0/channel").and_then(Value::as_str),
            Some("candle1m")
        );
        assert_eq!(
            value.pointer("/args/1/channel").and_then(Value::as_str),
            Some("candle5m")
        );
        assert_eq!(
            value.pointer("/args/0/instId").and_then(Value::as_str),
            Some("BTC-USDT-SWAP")
        );
        Ok(())
    }

    #[test]
    fn heartbeat_is_okx_text_ping_below_thirty_seconds() -> TestResult {
        let heartbeat = adapter()?.heartbeat();

        assert_eq!(heartbeat.interval, Duration::from_secs(20));
        assert!(heartbeat.interval < Duration::from_secs(30));
        assert_eq!(heartbeat.message, OutboundMessage::Text("ping".to_owned()));
        assert_eq!(
            adapter()?.parse_message(b"pong")?,
            vec![InboundMarketEvent::Heartbeat]
        );
        Ok(())
    }

    #[test]
    fn parses_subscription_acknowledgement() -> TestResult {
        let payload = br#"{
            "id":"ictmd1","event":"subscribe",
            "arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},
            "connId":"a4d3ae55"
        }"#;

        assert_eq!(
            adapter()?.parse_message(payload)?,
            vec![InboundMarketEvent::SubscriptionAcknowledged]
        );
        Ok(())
    }

    #[test]
    fn parses_incomplete_candle_without_losing_decimal_scale() -> TestResult {
        let InboundMarketEvent::Candle(candle) = one_event(INCOMPLETE_M1)? else {
            return Err("expected candle event".into());
        };

        assert_eq!(candle.timeframe(), Timeframe::M1);
        assert!(!candle.is_closed());
        assert_eq!(candle.open().to_string(), "93500.10");
        assert_eq!(candle.close().to_string(), "93505.60");
        assert_eq!(candle.volume().to_string(), "1234.500");
        assert_eq!(candle.volume_unit(), VolumeUnit::Contracts);
        assert_eq!(candle.trade_count(), None);
        assert_eq!(
            candle.open_time(),
            DateTime::<Utc>::from_timestamp_millis(1_735_689_600_000)
                .ok_or("fixture timestamp was invalid")?
        );
        assert_eq!(
            candle.close_time() - candle.open_time(),
            Timeframe::M1.duration()
        );
        Ok(())
    }

    #[test]
    fn parses_closed_five_minute_candle() -> TestResult {
        let InboundMarketEvent::Candle(candle) = one_event(CLOSED_M5)? else {
            return Err("expected candle event".into());
        };

        assert_eq!(candle.timeframe(), Timeframe::M5);
        assert!(candle.is_closed());
        assert_eq!(candle.high(), Decimal::new(9_361_000, 2));
        assert_eq!(candle.low(), Decimal::new(9_345_000, 2));
        assert_eq!(
            candle.close_time() - candle.open_time(),
            Timeframe::M5.duration()
        );
        Ok(())
    }

    #[test]
    fn parses_every_row_in_a_batch() -> TestResult {
        let payload = br#"{
            "arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},
            "data":[
                ["1735689600000","100","102","99","101","5","0.01","500","1"],
                ["1735689660000","101","103","100","102","6","0.02","600","0"]
            ]
        }"#;
        let events = adapter()?.parse_message(payload)?;

        assert_eq!(events.len(), 2);
        assert!(matches!(
            events.first(),
            Some(InboundMarketEvent::Candle(candle)) if candle.is_closed()
        ));
        assert!(matches!(
            events.get(1),
            Some(InboundMarketEvent::Candle(candle)) if !candle.is_closed()
        ));
        Ok(())
    }

    #[test]
    fn exchange_errors_and_notices_request_reconnect() -> TestResult {
        for payload in [
            br#"{"event":"error","code":"60012","msg":"Invalid request","connId":"abc"}"#
                .as_slice(),
            br#"{"event":"notice","code":"64008","msg":"The connection will soon be closed for a service upgrade.","connId":"abc"}"#
                .as_slice(),
            br#"{"event":"notice","code":"64010","msg":"WebSocket port 8443 will be retired soon.","connId":"abc"}"#
                .as_slice(),
        ] {
            let result = adapter()?.parse_message(payload);
            assert!(matches!(result, Err(AdapterError::Protocol { exchange: Exchange::Okx, .. })));
        }
        Ok(())
    }

    #[test]
    fn rejects_malformed_rows_flags_timestamps_and_decimals() -> TestResult {
        let malformed = [
            br#"{"arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},"data":[["1735689600000","100"]]}"#
                .as_slice(),
            br#"{"arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},"data":[["not-time","100","101","99","100","1","1","1","1"]]}"#
                .as_slice(),
            br#"{"arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},"data":[["1735689600000","100","101","99","100","1","1","1","true"]]}"#
                .as_slice(),
        ];
        for payload in malformed {
            assert!(matches!(
                adapter()?.parse_message(payload),
                Err(AdapterError::InvalidMessage {
                    exchange: Exchange::Okx,
                    ..
                })
            ));
        }

        let bad_decimal = br#"{
            "arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},
            "data":[["1735689600000","100","101","99","NaN","1","1","1","1"]]
        }"#;
        assert!(matches!(
            adapter()?.parse_message(bad_decimal),
            Err(AdapterError::InvalidDecimal {
                exchange: Exchange::Okx,
                field: "close",
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn rejects_unknown_or_malformed_protocol_messages() -> TestResult {
        let unknown_stream = br#"{
            "arg":{"channel":"candle1m","instId":"ETH-USDT-SWAP"},
            "data":[["1735689600000","100","101","99","100","1","1","1","1"]]
        }"#;
        assert!(matches!(
            adapter()?.parse_message(unknown_stream),
            Err(AdapterError::UnknownSubscription {
                exchange: Exchange::Okx,
                ..
            })
        ));
        assert!(matches!(
            adapter()?.parse_message(br#"{"event":"subscribe"}"#),
            Err(AdapterError::InvalidMessage {
                exchange: Exchange::Okx,
                ..
            })
        ));
        assert!(matches!(
            adapter()?.parse_message(b"not-json"),
            Err(AdapterError::Json {
                exchange: Exchange::Okx,
                ..
            })
        ));
        Ok(())
    }
}
