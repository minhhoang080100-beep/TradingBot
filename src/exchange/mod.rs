//! Exchange-specific market-data adapters.
//!
//! Wire DTOs stay inside this module. Only validated domain [`Candle`] values
//! are allowed to cross into the data engine.

#![cfg_attr(
    not(any(feature = "binance", feature = "bybit", feature = "okx")),
    allow(dead_code)
)]

use std::{collections::HashMap, str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use url::Url;

use crate::models::{Candle, Exchange, InstrumentId, ModelError, Timeframe, VolumeUnit};

#[cfg(feature = "binance")]
pub mod binance;
#[cfg(feature = "bybit")]
pub mod bybit;
#[cfg(feature = "okx")]
pub mod okx;

/// One instrument/timeframe subscription on a venue connection.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MarketSubscription {
    /// Instrument identity and venue-native symbol.
    pub instrument: InstrumentId,
    /// Requested candle interval.
    pub timeframe: Timeframe,
}

impl MarketSubscription {
    /// Creates a market subscription.
    #[must_use]
    pub const fn new(instrument: InstrumentId, timeframe: Timeframe) -> Self {
        Self {
            instrument,
            timeframe,
        }
    }
}

/// Transport-independent outbound WebSocket payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboundMessage {
    /// UTF-8 application message.
    Text(String),
    /// WebSocket control-frame ping payload.
    Ping(Vec<u8>),
}

/// Exchange heartbeat cadence and payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeartbeatSpec {
    /// Time between client heartbeats.
    pub interval: Duration,
    /// Payload sent on every interval.
    pub message: OutboundMessage,
}

/// Normalized event emitted by an exchange parser.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InboundMarketEvent {
    /// Validated partial or final candle.
    Candle(Candle),
    /// Positive subscription acknowledgement.
    SubscriptionAcknowledged,
    /// Application-level heartbeat response.
    Heartbeat,
}

/// Synchronous protocol adapter used by the asynchronous data engine.
pub trait ExchangeMarketData: Send + Sync {
    /// Venue represented by this adapter.
    fn exchange(&self) -> Exchange;

    /// WebSocket endpoint used for this connection.
    fn endpoint(&self) -> &Url;

    /// Configured subscriptions.
    fn subscriptions(&self) -> &[MarketSubscription];

    /// Builds messages that subscribe or resubscribe the connection.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when a subscription cannot be serialized.
    fn subscription_messages(&self) -> Result<Vec<OutboundMessage>, AdapterError>;

    /// Returns the venue-specific heartbeat behavior.
    fn heartbeat(&self) -> HeartbeatSpec;

    /// Optional proactive connection rotation age.
    ///
    /// Binance, for example, enforces a finite connection lifetime. The default
    /// leaves a connection open until the venue or liveness monitor closes it.
    fn max_connection_age(&self) -> Option<Duration> {
        None
    }

    /// Parses one text or binary WebSocket application payload.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] for malformed data, a rejected subscription, or
    /// a payload that violates the normalized candle model.
    fn parse_message(&self, payload: &[u8]) -> Result<Vec<InboundMarketEvent>, AdapterError>;
}

#[derive(Clone, Debug)]
pub(crate) struct SubscriptionIndex {
    exchange: Exchange,
    subscriptions: Vec<MarketSubscription>,
    instruments: HashMap<(String, Timeframe), InstrumentId>,
}

impl SubscriptionIndex {
    pub(crate) fn new(
        exchange: Exchange,
        subscriptions: Vec<MarketSubscription>,
    ) -> Result<Self, AdapterError> {
        if subscriptions.is_empty() {
            return Err(AdapterError::EmptySubscriptions { exchange });
        }

        let mut instruments = HashMap::with_capacity(subscriptions.len());
        for subscription in &subscriptions {
            if subscription.instrument.exchange != exchange {
                return Err(AdapterError::ExchangeMismatch {
                    expected: exchange,
                    actual: subscription.instrument.exchange,
                });
            }

            let key = (
                subscription.instrument.venue_symbol.as_str().to_owned(),
                subscription.timeframe,
            );
            if instruments
                .insert(key.clone(), subscription.instrument.clone())
                .is_some()
            {
                return Err(AdapterError::DuplicateSubscription {
                    exchange,
                    symbol: key.0,
                    timeframe: key.1,
                });
            }
        }

        Ok(Self {
            exchange,
            subscriptions,
            instruments,
        })
    }

    pub(crate) fn subscriptions(&self) -> &[MarketSubscription] {
        &self.subscriptions
    }

    pub(crate) fn instrument(
        &self,
        symbol: &str,
        timeframe: Timeframe,
    ) -> Result<InstrumentId, AdapterError> {
        self.instruments
            .get(&(symbol.to_owned(), timeframe))
            .cloned()
            .ok_or_else(|| AdapterError::UnknownSubscription {
                exchange: self.exchange,
                symbol: symbol.to_owned(),
                timeframe,
            })
    }
}

pub(crate) struct WireCandle<'a> {
    pub(crate) exchange: Exchange,
    pub(crate) instrument: InstrumentId,
    pub(crate) timeframe: Timeframe,
    pub(crate) open_time_millis: i64,
    pub(crate) open: &'a str,
    pub(crate) high: &'a str,
    pub(crate) low: &'a str,
    pub(crate) close: &'a str,
    pub(crate) volume: &'a str,
    pub(crate) volume_unit: VolumeUnit,
    pub(crate) trade_count: Option<u64>,
    pub(crate) is_closed: bool,
}

pub(crate) fn normalize_candle(input: WireCandle<'_>) -> Result<Candle, AdapterError> {
    let open_time = timestamp_millis(input.exchange, input.open_time_millis)?;
    let close_time = open_time
        .checked_add_signed(input.timeframe.duration())
        .ok_or(AdapterError::InvalidTimestamp {
            exchange: input.exchange,
            millis: input.open_time_millis,
        })?;

    Candle::new(crate::models::CandleInput {
        instrument: input.instrument,
        timeframe: input.timeframe,
        open_time,
        close_time,
        open: parse_decimal(input.exchange, "open", input.open)?,
        high: parse_decimal(input.exchange, "high", input.high)?,
        low: parse_decimal(input.exchange, "low", input.low)?,
        close: parse_decimal(input.exchange, "close", input.close)?,
        volume: parse_decimal(input.exchange, "volume", input.volume)?,
        volume_unit: input.volume_unit,
        trade_count: input.trade_count,
        is_closed: input.is_closed,
    })
    .map_err(AdapterError::from)
}

pub(crate) fn parse_decimal(
    exchange: Exchange,
    field: &'static str,
    value: &str,
) -> Result<Decimal, AdapterError> {
    Decimal::from_str(value).map_err(|source| AdapterError::InvalidDecimal {
        exchange,
        field,
        value: value.to_owned(),
        source,
    })
}

pub(crate) fn timestamp_millis(
    exchange: Exchange,
    millis: i64,
) -> Result<DateTime<Utc>, AdapterError> {
    DateTime::from_timestamp_millis(millis)
        .ok_or(AdapterError::InvalidTimestamp { exchange, millis })
}

/// Exchange parsing or subscription failure.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// Adapter was configured without subscriptions.
    #[error("{exchange:?} adapter requires at least one subscription")]
    EmptySubscriptions {
        /// Affected venue.
        exchange: Exchange,
    },
    /// A subscription belonged to another venue.
    #[error("subscription exchange mismatch: expected {expected:?}, received {actual:?}")]
    ExchangeMismatch {
        /// Adapter venue.
        expected: Exchange,
        /// Instrument venue.
        actual: Exchange,
    },
    /// The same symbol/timeframe was configured more than once.
    #[error("duplicate {exchange:?} subscription for {symbol} {timeframe}")]
    DuplicateSubscription {
        /// Adapter venue.
        exchange: Exchange,
        /// Venue-native symbol.
        symbol: String,
        /// Candle timeframe.
        timeframe: Timeframe,
    },
    /// A message referred to a stream not configured on this adapter.
    #[error("unconfigured {exchange:?} stream {symbol} {timeframe}")]
    UnknownSubscription {
        /// Adapter venue.
        exchange: Exchange,
        /// Venue-native symbol.
        symbol: String,
        /// Candle timeframe.
        timeframe: Timeframe,
    },
    /// Endpoint URL was invalid.
    #[error("invalid {exchange:?} WebSocket endpoint {endpoint}: {source}")]
    InvalidEndpoint {
        /// Adapter venue.
        exchange: Exchange,
        /// Invalid URL.
        endpoint: String,
        /// URL parser error.
        source: url::ParseError,
    },
    /// JSON parsing or serialization failed.
    #[error("invalid {exchange:?} JSON payload: {source}")]
    Json {
        /// Adapter venue.
        exchange: Exchange,
        /// JSON error.
        source: serde_json::Error,
    },
    /// Exact decimal string could not be parsed.
    #[error("invalid {exchange:?} decimal in {field}: {value}: {source}")]
    InvalidDecimal {
        /// Adapter venue.
        exchange: Exchange,
        /// Wire field name.
        field: &'static str,
        /// Rejected wire value.
        value: String,
        /// Decimal parser error.
        source: rust_decimal::Error,
    },
    /// Millisecond timestamp was outside Chrono's supported range.
    #[error("invalid {exchange:?} millisecond timestamp {millis}")]
    InvalidTimestamp {
        /// Adapter venue.
        exchange: Exchange,
        /// Rejected epoch timestamp.
        millis: i64,
    },
    /// Venue rejected a subscription or requested reconnect.
    #[error("{exchange:?} protocol error: {message}")]
    Protocol {
        /// Adapter venue.
        exchange: Exchange,
        /// Venue error or notice.
        message: String,
    },
    /// Topic or channel could not be mapped to a supported timeframe.
    #[error("unsupported {exchange:?} timeframe {value}")]
    UnsupportedTimeframe {
        /// Adapter venue.
        exchange: Exchange,
        /// Venue interval value.
        value: String,
    },
    /// Payload omitted a required field or used an unexpected shape.
    #[error("invalid {exchange:?} message shape: {message}")]
    InvalidMessage {
        /// Adapter venue.
        exchange: Exchange,
        /// Shape violation.
        message: &'static str,
    },
    /// Normalized candle invariants failed.
    #[error(transparent)]
    Model(#[from] ModelError),
}
