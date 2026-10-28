//! Bybit V5 public Linear kline WebSocket adapter.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    exchange::{
        normalize_candle, parse_decimal, timestamp_millis, AdapterError, ExchangeMarketData,
        HeartbeatSpec, InboundMarketEvent, MarketSubscription, OutboundMessage, SubscriptionIndex,
        WireCandle,
    },
    models::{Exchange, Timeframe, VolumeUnit},
};

/// Bybit V5 public Linear market-data endpoint.
pub const MAINNET_ENDPOINT: &str = "wss://stream.bybit.com/v5/public/linear";

/// Bybit V5 public Linear testnet market-data endpoint.
pub const TESTNET_ENDPOINT: &str = "wss://stream-testnet.bybit.com/v5/public/linear";

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const HEARTBEAT_PAYLOAD: &str = r#"{"op":"ping"}"#;
const MAX_TOPICS_PER_REQUEST: usize = 100;
const MAX_ARGS_CHARACTERS: usize = 21_000;

/// Bybit V5 public Linear kline protocol adapter.
#[derive(Clone, Debug)]
pub struct BybitMarketData {
    endpoint: Url,
    subscriptions: SubscriptionIndex,
}

impl BybitMarketData {
    /// Creates an adapter connected to Bybit's mainnet Linear public stream.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when subscriptions are empty, duplicated, or
    /// belong to another exchange.
    pub fn new(subscriptions: Vec<MarketSubscription>) -> Result<Self, AdapterError> {
        Self::with_endpoint(subscriptions, MAINNET_ENDPOINT)
    }

    /// Creates an adapter connected to Bybit's testnet Linear public stream.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when subscriptions are empty, duplicated, or
    /// belong to another exchange.
    pub fn testnet(subscriptions: Vec<MarketSubscription>) -> Result<Self, AdapterError> {
        Self::with_endpoint(subscriptions, TESTNET_ENDPOINT)
    }

    /// Creates an adapter with an explicit endpoint.
    ///
    /// This is useful for regional Bybit hosts and local integration tests.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when the endpoint is invalid or subscriptions
    /// fail validation.
    pub fn with_endpoint(
        subscriptions: Vec<MarketSubscription>,
        endpoint: &str,
    ) -> Result<Self, AdapterError> {
        let endpoint = Url::parse(endpoint).map_err(|source| AdapterError::InvalidEndpoint {
            exchange: Exchange::Bybit,
            endpoint: endpoint.to_owned(),
            source,
        })?;
        let subscriptions = SubscriptionIndex::new(Exchange::Bybit, subscriptions)?;

        Ok(Self {
            endpoint,
            subscriptions,
        })
    }

    fn parse_kline(
        &self,
        message: &BybitMessage<'_>,
    ) -> Result<Vec<InboundMarketEvent>, AdapterError> {
        if message.message_type != Some("snapshot") {
            return Err(invalid_message("Bybit kline type must be snapshot"));
        }

        let topic = message
            .topic
            .ok_or_else(|| invalid_message("Bybit kline message omitted topic"))?;
        let (topic_timeframe, symbol) = parse_topic(topic)?;
        let rows = message
            .data
            .as_deref()
            .ok_or_else(|| invalid_message("Bybit kline message omitted data"))?;
        if rows.is_empty() {
            return Err(invalid_message("Bybit kline data array was empty"));
        }

        let server_timestamp = message
            .timestamp
            .ok_or_else(|| invalid_message("Bybit kline message omitted ts"))?;
        timestamp_millis(Exchange::Bybit, server_timestamp)?;

        let instrument = self.subscriptions.instrument(symbol, topic_timeframe)?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let row_timeframe = parse_timeframe(row.interval)?;
            if row_timeframe != topic_timeframe {
                return Err(invalid_message(
                    "Bybit kline row interval disagreed with its topic",
                ));
            }

            validate_end_timestamp(row.start, row.end, row_timeframe)?;
            timestamp_millis(Exchange::Bybit, row.last_trade_timestamp)?;

            // Turnover is not part of the normalized candle, but validating it
            // prevents an invalid numeric wire value from silently crossing the
            // adapter boundary.
            parse_decimal(Exchange::Bybit, "turnover", row.turnover)?;

            let candle = normalize_candle(WireCandle {
                exchange: Exchange::Bybit,
                instrument: instrument.clone(),
                timeframe: row_timeframe,
                open_time_millis: row.start,
                open: row.open,
                high: row.high,
                low: row.low,
                close: row.close,
                volume: row.volume,
                volume_unit: VolumeUnit::BaseAsset,
                trade_count: None,
                is_closed: row.confirm,
            })?;
            events.push(InboundMarketEvent::Candle(candle));
        }

        Ok(events)
    }

    fn parse_command(message: &BybitMessage<'_>) -> Result<Vec<InboundMarketEvent>, AdapterError> {
        if message.success == Some(false) {
            return Err(AdapterError::Protocol {
                exchange: Exchange::Bybit,
                message: protocol_error_message(message),
            });
        }

        match message.operation {
            Some("subscribe") if message.success == Some(true) => {
                Ok(vec![InboundMarketEvent::SubscriptionAcknowledged])
            }
            Some("ping")
                if message.success == Some(true) && message.return_message == Some("pong") =>
            {
                Ok(vec![InboundMarketEvent::Heartbeat])
            }
            // Other Bybit public channel families use `op: "pong"`; accepting
            // it costs nothing and keeps the heartbeat parser forward-compatible.
            Some("pong") => Ok(vec![InboundMarketEvent::Heartbeat]),
            Some("subscribe") => Err(invalid_message(
                "Bybit subscription response omitted a successful status",
            )),
            Some("ping") => Err(invalid_message(
                "Bybit heartbeat response omitted a successful pong status",
            )),
            Some(operation) => Err(AdapterError::Protocol {
                exchange: Exchange::Bybit,
                message: format!("unsupported operation response {operation}"),
            }),
            None => Err(invalid_message("unrecognized Bybit message")),
        }
    }
}

impl ExchangeMarketData for BybitMarketData {
    fn exchange(&self) -> Exchange {
        Exchange::Bybit
    }

    fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    fn subscriptions(&self) -> &[MarketSubscription] {
        self.subscriptions.subscriptions()
    }

    fn subscription_messages(&self) -> Result<Vec<OutboundMessage>, AdapterError> {
        let topics = self
            .subscriptions()
            .iter()
            .map(subscription_topic)
            .collect::<Vec<_>>();
        let args_characters = serde_json::to_string(&topics)
            .map_err(|source| AdapterError::Json {
                exchange: Exchange::Bybit,
                source,
            })?
            .len();
        if args_characters > MAX_ARGS_CHARACTERS {
            return Err(AdapterError::Protocol {
                exchange: Exchange::Bybit,
                message: format!(
                    "subscription args contain {args_characters} characters; maximum is \
                     {MAX_ARGS_CHARACTERS}"
                ),
            });
        }

        topics
            .chunks(MAX_TOPICS_PER_REQUEST)
            .map(|args| {
                let request = SubscribeRequest {
                    operation: "subscribe",
                    args,
                };
                serde_json::to_string(&request)
                    .map(OutboundMessage::Text)
                    .map_err(|source| AdapterError::Json {
                        exchange: Exchange::Bybit,
                        source,
                    })
            })
            .collect()
    }

    fn heartbeat(&self) -> HeartbeatSpec {
        HeartbeatSpec {
            interval: HEARTBEAT_INTERVAL,
            message: OutboundMessage::Text(HEARTBEAT_PAYLOAD.to_owned()),
        }
    }

    fn parse_message(&self, payload: &[u8]) -> Result<Vec<InboundMarketEvent>, AdapterError> {
        let message = serde_json::from_slice::<BybitMessage<'_>>(payload).map_err(|source| {
            AdapterError::Json {
                exchange: Exchange::Bybit,
                source,
            }
        })?;

        if message.success == Some(false) {
            return Self::parse_command(&message);
        }
        if message.topic.is_some() || message.data.is_some() {
            return self.parse_kline(&message);
        }
        Self::parse_command(&message)
    }
}

#[derive(Serialize)]
struct SubscribeRequest<'a> {
    #[serde(rename = "op")]
    operation: &'static str,
    args: &'a [String],
}

#[derive(Deserialize)]
struct BybitMessage<'a> {
    #[serde(default, borrow)]
    topic: Option<&'a str>,
    #[serde(rename = "type", default, borrow)]
    message_type: Option<&'a str>,
    #[serde(rename = "ts", default)]
    timestamp: Option<i64>,
    #[serde(default, borrow)]
    data: Option<Vec<BybitKline<'a>>>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(rename = "ret_msg", alias = "retMsg", default, borrow)]
    return_message: Option<&'a str>,
    #[serde(rename = "ret_code", alias = "retCode", default)]
    return_code: Option<i64>,
    #[serde(rename = "op", default, borrow)]
    operation: Option<&'a str>,
}

#[derive(Deserialize)]
struct BybitKline<'a> {
    start: i64,
    end: i64,
    #[serde(borrow)]
    interval: &'a str,
    #[serde(borrow)]
    open: &'a str,
    #[serde(borrow)]
    close: &'a str,
    #[serde(borrow)]
    high: &'a str,
    #[serde(borrow)]
    low: &'a str,
    #[serde(borrow)]
    volume: &'a str,
    #[serde(borrow)]
    turnover: &'a str,
    confirm: bool,
    #[serde(rename = "timestamp")]
    last_trade_timestamp: i64,
}

fn subscription_topic(subscription: &MarketSubscription) -> String {
    format!(
        "kline.{}.{}",
        bybit_interval(subscription.timeframe),
        subscription.instrument.venue_symbol
    )
}

const fn bybit_interval(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::M1 => "1",
        Timeframe::M3 => "3",
        Timeframe::M5 => "5",
        Timeframe::M15 => "15",
    }
}

fn parse_topic(topic: &str) -> Result<(Timeframe, &str), AdapterError> {
    let topic = topic
        .strip_prefix("kline.")
        .ok_or_else(|| invalid_message("Bybit topic did not start with kline"))?;
    let (interval, symbol) = topic
        .split_once('.')
        .ok_or_else(|| invalid_message("Bybit kline topic omitted its symbol"))?;
    if symbol.is_empty() || symbol.contains('.') {
        return Err(invalid_message("Bybit kline topic had an invalid symbol"));
    }

    Ok((parse_timeframe(interval)?, symbol))
}

fn parse_timeframe(interval: &str) -> Result<Timeframe, AdapterError> {
    match interval {
        "1" => Ok(Timeframe::M1),
        "3" => Ok(Timeframe::M3),
        "5" => Ok(Timeframe::M5),
        "15" => Ok(Timeframe::M15),
        value => Err(AdapterError::UnsupportedTimeframe {
            exchange: Exchange::Bybit,
            value: value.to_owned(),
        }),
    }
}

fn validate_end_timestamp(start: i64, end: i64, timeframe: Timeframe) -> Result<(), AdapterError> {
    let interval_millis = timeframe.duration().num_milliseconds();
    let expected = start
        .checked_add(interval_millis)
        .and_then(|exclusive_end| exclusive_end.checked_sub(1))
        .ok_or(AdapterError::InvalidTimestamp {
            exchange: Exchange::Bybit,
            millis: start,
        })?;
    if end != expected {
        return Err(invalid_message(
            "Bybit kline end timestamp disagreed with its start and interval",
        ));
    }

    Ok(())
}

fn protocol_error_message(message: &BybitMessage<'_>) -> String {
    let operation = message.operation.unwrap_or("unknown operation");
    let description = message.return_message.unwrap_or("request rejected");
    match message.return_code {
        Some(code) => format!("{operation} failed with code {code}: {description}"),
        None => format!("{operation} failed: {description}"),
    }
}

const fn invalid_message(message: &'static str) -> AdapterError {
    AdapterError::InvalidMessage {
        exchange: Exchange::Bybit,
        message,
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io, str::FromStr};

    use rust_decimal::Decimal;
    use serde_json::{json, Value};

    use super::*;
    use crate::models::{InstrumentId, MarketKind, ModelError};

    type TestResult = Result<(), Box<dyn Error>>;

    fn subscription(symbol: &str, timeframe: Timeframe) -> Result<MarketSubscription, ModelError> {
        let canonical = format!("{symbol}-PERP");
        Ok(MarketSubscription::new(
            InstrumentId::new(
                Exchange::Bybit,
                MarketKind::LinearPerpetual,
                symbol,
                canonical,
            )?,
            timeframe,
        ))
    }

    fn adapter() -> Result<BybitMarketData, AdapterError> {
        BybitMarketData::new(vec![
            subscription("BTCUSDT", Timeframe::M1).map_err(AdapterError::from)?,
            subscription("BTCUSDT", Timeframe::M5).map_err(AdapterError::from)?,
        ])
    }

    fn outbound_text(message: &OutboundMessage) -> Result<&str, io::Error> {
        match message {
            OutboundMessage::Text(text) => Ok(text),
            OutboundMessage::Ping(_) => Err(io::Error::other("expected outbound text")),
        }
    }

    fn first_candle(events: &[InboundMarketEvent]) -> Result<&crate::models::Candle, io::Error> {
        match events.first() {
            Some(InboundMarketEvent::Candle(candle)) => Ok(candle),
            _ => Err(io::Error::other("expected first event to be a candle")),
        }
    }

    #[test]
    fn constructs_mainnet_and_testnet_adapters() -> TestResult {
        let subscriptions = vec![subscription("BTCUSDT", Timeframe::M1)?];
        let mainnet = BybitMarketData::new(subscriptions.clone())?;
        let testnet = BybitMarketData::testnet(subscriptions)?;

        assert_eq!(mainnet.exchange(), Exchange::Bybit);
        assert_eq!(mainnet.endpoint().as_str(), MAINNET_ENDPOINT);
        assert_eq!(testnet.endpoint().as_str(), TESTNET_ENDPOINT);
        assert_eq!(mainnet.subscriptions().len(), 1);
        Ok(())
    }

    #[test]
    fn serializes_m1_and_m5_subscriptions() -> TestResult {
        let messages = adapter()?.subscription_messages()?;
        assert_eq!(messages.len(), 1);

        let actual: Value = serde_json::from_str(outbound_text(
            messages
                .first()
                .ok_or_else(|| io::Error::other("missing subscription message"))?,
        )?)?;
        assert_eq!(
            actual,
            json!({
                "op": "subscribe",
                "args": ["kline.1.BTCUSDT", "kline.5.BTCUSDT"]
            })
        );
        Ok(())
    }

    #[test]
    fn batches_subscription_requests_at_one_hundred_topics() -> TestResult {
        let mut subscriptions = Vec::new();
        for index in 0..101 {
            subscriptions.push(subscription(&format!("ASSET{index}USDT"), Timeframe::M1)?);
        }
        let messages = BybitMarketData::new(subscriptions)?.subscription_messages()?;

        assert_eq!(messages.len(), 2);
        let first: Value = serde_json::from_str(outbound_text(&messages[0])?)?;
        let second: Value = serde_json::from_str(outbound_text(&messages[1])?)?;
        assert_eq!(first["args"].as_array().map(Vec::len), Some(100));
        assert_eq!(second["args"].as_array().map(Vec::len), Some(1));
        Ok(())
    }

    #[test]
    fn exposes_bybit_application_heartbeat() -> TestResult {
        let heartbeat = adapter()?.heartbeat();

        assert_eq!(heartbeat.interval, Duration::from_secs(20));
        assert_eq!(
            heartbeat.message,
            OutboundMessage::Text(r#"{"op":"ping"}"#.to_owned())
        );
        Ok(())
    }

    #[test]
    fn parses_every_row_and_preserves_decimal_precision() -> TestResult {
        let payload = br#"{
            "topic":"kline.1.BTCUSDT",
            "type":"snapshot",
            "ts":1735689720012,
            "data":[
                {
                    "start":1735689600000,
                    "end":1735689659999,
                    "interval":"1",
                    "open":"93420.100000000000000001",
                    "close":"93455.700000000000000009",
                    "high":"93480.000000000000000010",
                    "low":"93390.200000000000000001",
                    "volume":"12.345000000000000001",
                    "turnover":"1153092.123500000000000001",
                    "confirm":true,
                    "timestamp":1735689659876
                },
                {
                    "start":1735689660000,
                    "end":1735689719999,
                    "interval":"1",
                    "open":"93455.700000000000000009",
                    "close":"93460.800000000000000001",
                    "high":"93490.000000000000000001",
                    "low":"93440.100000000000000001",
                    "volume":"3.000000000000000001",
                    "turnover":"280382.400000000000000001",
                    "confirm":false,
                    "timestamp":1735689719000
                }
            ]
        }"#;

        let events = adapter()?.parse_message(payload)?;
        assert_eq!(events.len(), 2);
        let first = first_candle(&events)?;
        assert_eq!(first.open(), Decimal::from_str("93420.100000000000000001")?);
        assert_eq!(
            first.close(),
            Decimal::from_str("93455.700000000000000009")?
        );
        assert_eq!(first.volume(), Decimal::from_str("12.345000000000000001")?);
        assert_eq!(first.volume_unit(), VolumeUnit::BaseAsset);
        assert_eq!(first.trade_count(), None);
        assert!(first.is_closed());

        let Some(InboundMarketEvent::Candle(second)) = events.get(1) else {
            return Err(io::Error::other("expected second candle").into());
        };
        assert!(!second.is_closed());
        assert_eq!(
            second.close(),
            Decimal::from_str("93460.800000000000000001")?
        );
        Ok(())
    }

    #[test]
    fn parses_subscription_acknowledgement_and_pong() -> TestResult {
        let adapter = adapter()?;
        let acknowledgement = adapter.parse_message(
            br#"{"success":true,"ret_msg":"","conn_id":"connection","op":"subscribe"}"#,
        )?;
        let pong = adapter.parse_message(
            br#"{"success":true,"ret_msg":"pong","conn_id":"connection","op":"ping"}"#,
        )?;

        assert_eq!(
            acknowledgement,
            vec![InboundMarketEvent::SubscriptionAcknowledged]
        );
        assert_eq!(pong, vec![InboundMarketEvent::Heartbeat]);
        Ok(())
    }

    #[test]
    fn turns_rejected_subscription_into_protocol_error() -> TestResult {
        let error = adapter()?
            .parse_message(
                br#"{"success":false,"ret_msg":"invalid topic","ret_code":10001,"op":"subscribe"}"#,
            )
            .err()
            .ok_or_else(|| io::Error::other("expected a protocol error"))?;

        assert!(matches!(
            error,
            AdapterError::Protocol {
                exchange: Exchange::Bybit,
                message
            } if message == "subscribe failed with code 10001: invalid topic"
        ));
        Ok(())
    }

    #[test]
    fn rejects_unknown_stream_and_unsupported_interval() -> TestResult {
        let unknown = br#"{
            "topic":"kline.1.ETHUSDT","type":"snapshot","ts":1735689660012,
            "data":[{"start":1735689600000,"end":1735689659999,"interval":"1",
            "open":"100","close":"101","high":"102","low":"99","volume":"1",
            "turnover":"100","confirm":true,"timestamp":1735689659000}]
        }"#;
        let unsupported = br#"{
            "topic":"kline.30.BTCUSDT","type":"snapshot","ts":1735689660012,
            "data":[{"start":1735689600000,"end":1735691399999,"interval":"30",
            "open":"100","close":"101","high":"102","low":"99","volume":"1",
            "turnover":"100","confirm":true,"timestamp":1735689659000}]
        }"#;

        assert!(matches!(
            adapter()?.parse_message(unknown),
            Err(AdapterError::UnknownSubscription {
                exchange: Exchange::Bybit,
                ..
            })
        ));
        assert!(matches!(
            adapter()?.parse_message(unsupported),
            Err(AdapterError::UnsupportedTimeframe {
                exchange: Exchange::Bybit,
                value
            }) if value == "30"
        ));
        Ok(())
    }

    #[test]
    fn rejects_invalid_decimal_and_json_number_prices() -> TestResult {
        let invalid_decimal = br#"{
            "topic":"kline.1.BTCUSDT","type":"snapshot","ts":1735689660012,
            "data":[{"start":1735689600000,"end":1735689659999,"interval":"1",
            "open":"not-a-decimal","close":"101","high":"102","low":"99","volume":"1",
            "turnover":"100","confirm":true,"timestamp":1735689659000}]
        }"#;
        let number_price = br#"{
            "topic":"kline.1.BTCUSDT","type":"snapshot","ts":1735689660012,
            "data":[{"start":1735689600000,"end":1735689659999,"interval":"1",
            "open":100,"close":"101","high":"102","low":"99","volume":"1",
            "turnover":"100","confirm":true,"timestamp":1735689659000}]
        }"#;

        assert!(matches!(
            adapter()?.parse_message(invalid_decimal),
            Err(AdapterError::InvalidDecimal {
                exchange: Exchange::Bybit,
                field: "open",
                ..
            })
        ));
        assert!(matches!(
            adapter()?.parse_message(number_price),
            Err(AdapterError::Json {
                exchange: Exchange::Bybit,
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn rejects_interval_disagreement_invalid_end_and_empty_data() -> TestResult {
        let interval_disagreement = br#"{
            "topic":"kline.1.BTCUSDT","type":"snapshot","ts":1735689660012,
            "data":[{"start":1735689600000,"end":1735689899999,"interval":"5",
            "open":"100","close":"101","high":"102","low":"99","volume":"1",
            "turnover":"100","confirm":true,"timestamp":1735689659000}]
        }"#;
        let invalid_end = br#"{
            "topic":"kline.1.BTCUSDT","type":"snapshot","ts":1735689660012,
            "data":[{"start":1735689600000,"end":1735689660000,"interval":"1",
            "open":"100","close":"101","high":"102","low":"99","volume":"1",
            "turnover":"100","confirm":true,"timestamp":1735689659000}]
        }"#;
        let empty_data =
            br#"{"topic":"kline.1.BTCUSDT","type":"snapshot","ts":1735689660012,"data":[]}"#;

        assert!(matches!(
            adapter()?.parse_message(interval_disagreement),
            Err(AdapterError::InvalidMessage {
                exchange: Exchange::Bybit,
                ..
            })
        ));
        assert!(matches!(
            adapter()?.parse_message(invalid_end),
            Err(AdapterError::InvalidMessage {
                exchange: Exchange::Bybit,
                ..
            })
        ));
        assert!(matches!(
            adapter()?.parse_message(empty_data),
            Err(AdapterError::InvalidMessage {
                exchange: Exchange::Bybit,
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn rejects_malformed_and_unrecognized_messages() -> TestResult {
        assert!(matches!(
            adapter()?.parse_message(br#"{"topic":]"#),
            Err(AdapterError::Json {
                exchange: Exchange::Bybit,
                ..
            })
        ));
        assert!(matches!(
            adapter()?.parse_message(br#"{"success":true}"#),
            Err(AdapterError::InvalidMessage {
                exchange: Exchange::Bybit,
                ..
            })
        ));
        Ok(())
    }
}
