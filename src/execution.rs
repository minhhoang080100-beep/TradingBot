//! Authenticated execution protocols and a conservative order coordinator.
//!
//! REST acceptance is never a fill. Private order events are authoritative for
//! cumulative fills; transport uncertainty requires reconciliation before any
//! new entry. Each engine owns one venue account in one-way position mode.

use std::fmt;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use reqwest::Method;
use rust_decimal::Decimal;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::models::{Exchange, InstrumentId, MarketKind};

#[cfg(feature = "binance")]
pub mod binance;
#[cfg(feature = "bybit")]
pub mod bybit;
pub mod coordinator;
#[cfg(feature = "okx")]
pub mod okx;
pub mod transport;

/// Account credentials, redacted in debug output and zeroized on drop.
pub struct ApiCredentials {
    api_key: SecretString,
    secret: SecretString,
    passphrase: Option<SecretString>,
}

impl ApiCredentials {
    /// Creates credentials without exposing them in errors or logs.
    ///
    /// # Errors
    /// Rejects empty credentials and header control characters.
    pub fn new(
        api_key: String,
        secret: String,
        passphrase: Option<String>,
    ) -> Result<Self, ExecutionError> {
        if api_key.is_empty()
            || secret.is_empty()
            || api_key.chars().any(char::is_control)
            || passphrase
                .as_ref()
                .is_some_and(|p| p.is_empty() || p.chars().any(char::is_control))
        {
            return Err(ExecutionError::InvalidRequest("invalid credentials"));
        }
        Ok(Self {
            api_key: api_key.into(),
            secret: secret.into(),
            passphrase: passphrase.map(Into::into),
        })
    }

    /// Exposes the key only for authentication construction.
    #[must_use]
    pub fn api_key(&self) -> &str {
        self.api_key.expose_secret()
    }
    /// Exposes the signing secret only for authentication construction.
    #[must_use]
    pub fn secret(&self) -> &str {
        self.secret.expose_secret()
    }
    /// Exposes the optional OKX passphrase only for authentication construction.
    #[must_use]
    pub fn passphrase(&self) -> Option<&str> {
        self.passphrase.as_ref().map(ExposeSecret::expose_secret)
    }
}

impl fmt::Debug for ApiCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiCredentials([REDACTED])")
    }
}

/// Exact bytes to transmit after signing. Debug deliberately excludes secrets.
pub struct SignedRequest {
    method: Method,
    path: String,
    query: String,
    body: Bytes,
    headers: Vec<(String, String)>,
}

impl SignedRequest {
    /// Keeps signing input and transmitted bytes identical.
    ///
    /// # Errors
    /// Rejects absolute URLs, path traversal, and invalid header syntax.
    pub fn new(
        method: Method,
        path: String,
        query: String,
        body: Bytes,
        headers: Vec<(String, String)>,
    ) -> Result<Self, ExecutionError> {
        if !path.starts_with('/')
            || path.starts_with("//")
            || path.contains(['?', '#', '\\'])
            || path.contains("..")
            || path.chars().any(char::is_control)
            || query.contains('#')
        {
            return Err(ExecutionError::InvalidRequest(
                "invalid signed request path",
            ));
        }
        for (key, value) in &headers {
            reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| ExecutionError::InvalidRequest("invalid header name"))?;
            reqwest::header::HeaderValue::from_str(value)
                .map_err(|_| ExecutionError::InvalidRequest("invalid header value"))?;
        }
        Ok(Self {
            method,
            path,
            query,
            body,
            headers,
        })
    }
    /// HTTP method covered by the signature when required by the venue.
    #[must_use]
    pub const fn method(&self) -> &Method {
        &self.method
    }
    /// Absolute path relative to the trusted venue host.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
    /// Already encoded query string without a question mark.
    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }
    /// Exact serialized body bytes.
    #[must_use]
    pub const fn body(&self) -> &Bytes {
        &self.body
    }
    /// Authentication headers; callers must not log these values.
    #[must_use]
    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }
}

impl fmt::Debug for SignedRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignedRequest")
            .field("method", &self.method)
            .field("path", &self.path)
            .field("authentication", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

/// Side of an order in one-way position mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrderSide {
    /// Buy base asset or increase signed position quantity.
    Buy,
    /// Sell base asset or decrease signed position quantity.
    Sell,
}

impl OrderSide {
    /// Opposite side, used for reduce-only protective exits.
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

/// Normalized order type; adapters choose venue-specific endpoints.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrderKind {
    /// Resting limit entry.
    Limit,
    /// Market order, permitted only as a reduce-only exit here.
    Market,
    /// Stop-loss trigger followed by a market exit.
    StopMarket,
    /// Take-profit trigger followed by a market exit.
    TakeProfitMarket,
}

/// Explicit order economics, validated again by every adapter before signing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderRequest {
    /// Venue and instrument identity.
    pub instrument: InstrumentId,
    /// Stable, alphanumeric ID, at most 32 characters for cross-venue use.
    pub client_order_id: String,
    /// Buy or sell.
    pub side: OrderSide,
    /// Normalized order type.
    pub kind: OrderKind,
    /// Venue quantity (base units or contracts according to risk spec).
    #[serde(with = "crate::decimal_codec::required")]
    pub quantity: Decimal,
    /// Required only for a limit order.
    #[serde(with = "crate::decimal_codec::optional")]
    pub price: Option<Decimal>,
    /// Required only for a conditional order.
    #[serde(with = "crate::decimal_codec::optional")]
    pub trigger_price: Option<Decimal>,
    /// Mandatory for all protective and market exits.
    pub reduce_only: bool,
}

impl OrderRequest {
    /// Checks order geometry and supported market scope before signing.
    ///
    /// # Errors
    /// Returns an error for invalid identity, quantity, prices, or exit flags.
    pub fn validate(&self) -> Result<(), ExecutionError> {
        validate_client_id(&self.client_order_id)?;
        if self.instrument.market != MarketKind::LinearPerpetual || self.quantity <= Decimal::ZERO {
            return Err(ExecutionError::InvalidRequest(
                "linear positive-quantity orders required",
            ));
        }
        let valid = match self.kind {
            OrderKind::Limit => {
                self.price.is_some_and(|p| p > Decimal::ZERO) && self.trigger_price.is_none()
            }
            OrderKind::Market => {
                self.price.is_none() && self.trigger_price.is_none() && self.reduce_only
            }
            OrderKind::StopMarket | OrderKind::TakeProfitMarket => {
                self.price.is_none()
                    && self.trigger_price.is_some_and(|p| p > Decimal::ZERO)
                    && self.reduce_only
            }
        };
        if !valid {
            return Err(ExecutionError::InvalidRequest(
                "invalid order prices or reduce-only flag",
            ));
        }
        Ok(())
    }

    /// Produces a query/cancel identity after an acknowledgement supplies its ID.
    #[must_use]
    pub fn reference(&self, venue_order_id: Option<String>) -> OrderRef {
        OrderRef {
            instrument: self.instrument.clone(),
            client_order_id: self.client_order_id.clone(),
            kind: self.kind,
            venue_order_id,
        }
    }
}

fn validate_client_id(value: &str) -> Result<(), ExecutionError> {
    if value.is_empty() || value.len() > 32 || !value.bytes().all(|c| c.is_ascii_alphanumeric()) {
        return Err(ExecutionError::InvalidRequest(
            "invalid client order identifier",
        ));
    }
    Ok(())
}

/// An existing entry or conditional order to cancel or reconcile.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderRef {
    /// Instrument identity.
    pub instrument: InstrumentId,
    /// Stable client ID.
    pub client_order_id: String,
    /// Selects regular versus algo endpoint.
    pub kind: OrderKind,
    /// Exchange ID; required by OKX algo cancellation.
    #[serde(deserialize_with = "deserialize_present_option")]
    pub venue_order_id: Option<String>,
}

/// REST operation; query is reconciliation and never resubmits an order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrderCommand {
    /// Submit an order once.
    Place(OrderRequest),
    /// Request cancellation; final cancellation needs stream confirmation.
    Cancel(OrderRef),
    /// Look up an existing order after uncertainty or reconnect.
    Query(OrderRef),
}

impl OrderCommand {
    /// Stable client ID used to correlate the response.
    #[must_use]
    pub fn client_order_id(&self) -> &str {
        match self {
            Self::Place(r) => &r.client_order_id,
            Self::Cancel(r) | Self::Query(r) => &r.client_order_id,
        }
    }
    /// Target instrument.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId {
        match self {
            Self::Place(r) => &r.instrument,
            Self::Cancel(r) | Self::Query(r) => &r.instrument,
        }
    }
}

/// Venue lifecycle status; `Triggered` does not imply any fill.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// Accepted and still open.
    New,
    /// Some quantity has filled.
    PartiallyFilled,
    /// All order quantity filled.
    Filled,
    /// Remainder cancelled; cumulative fill can be positive.
    Cancelled,
    /// Rejected by the venue.
    Rejected,
    /// Expired, potentially after a partial fill.
    Expired,
    /// Conditional order activated; await matching-engine execution.
    Triggered,
}

impl OrderStatus {
    /// Whether no further fills are expected, subject to delayed event delivery.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Expired
        )
    }
}

/// REST acknowledgement, never sufficient to release risk or infer a fill.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderAck {
    /// Correlated client ID.
    pub client_order_id: String,
    /// Exchange ID when supplied by the operation.
    #[serde(deserialize_with = "deserialize_present_option")]
    pub venue_order_id: Option<String>,
    /// Informational status only; no cumulative execution evidence is included.
    #[serde(deserialize_with = "deserialize_present_option")]
    pub status: Option<OrderStatus>,
}

fn deserialize_present_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Private order-stream update using cumulative, not incremental, fill quantity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderUpdate {
    /// Source venue.
    pub exchange: Exchange,
    /// Exact venue symbol.
    pub symbol: String,
    /// Client ID, including parent algo client ID for triggered children.
    pub client_order_id: String,
    /// Exchange matching-engine/algo order ID.
    pub venue_order_id: String,
    /// Current lifecycle state.
    pub status: OrderStatus,
    /// Original order quantity.
    pub quantity: Decimal,
    /// Total filled quantity for this order so far.
    pub cumulative_filled: Decimal,
    /// Average execution price, if a fill occurred.
    pub average_price: Option<Decimal>,
    /// Exchange event timestamp.
    pub at: DateTime<Utc>,
}

impl OrderUpdate {
    /// Checks cumulative execution invariants before state mutation.
    ///
    /// # Errors
    /// Rejects impossible quantities, identity, prices, and status combinations.
    pub fn validate(&self) -> Result<(), ExecutionError> {
        if self.symbol.is_empty()
            || self.client_order_id.is_empty()
            || self.venue_order_id.is_empty()
            || self.quantity <= Decimal::ZERO
            || self.cumulative_filled < Decimal::ZERO
            || self.cumulative_filled > self.quantity
            || (self.status == OrderStatus::New && self.cumulative_filled != Decimal::ZERO)
            || self.average_price.is_some_and(|p| p <= Decimal::ZERO)
            || (self.status == OrderStatus::Filled && self.cumulative_filled != self.quantity)
            || (self.status == OrderStatus::PartiallyFilled
                && (self.cumulative_filled <= Decimal::ZERO
                    || self.cumulative_filled >= self.quantity))
        {
            return Err(ExecutionError::Protocol("invalid cumulative order update"));
        }
        Ok(())
    }
}

/// Signed account position quantity; positive long, negative short, zero flat.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PositionUpdate {
    /// Source venue.
    pub exchange: Exchange,
    /// Venue symbol.
    pub symbol: String,
    /// Net signed quantity in venue units.
    pub signed_quantity: Decimal,
    /// Exchange timestamp.
    pub at: DateTime<Utc>,
}

/// Exchange-neutral private-stream payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UserEvent {
    /// Order lifecycle and execution evidence.
    Order(OrderUpdate),
    /// Account position observation.
    Position(PositionUpdate),
    /// Authentication confirmed by server.
    Authenticated,
    /// Requested topics confirmed by server.
    Subscribed,
    /// Heartbeat response.
    Heartbeat,
    /// Stream cannot prove continuity or complete order state.
    ReconciliationRequired,
    /// Explicitly irrelevant message.
    Ignored,
}

/// Venue protocol independent of HTTP transport and account state.
pub trait ExecutionAdapter: Send + Sync {
    /// Venue supported by this implementation.
    fn exchange(&self) -> Exchange;
    /// Serializes and signs the exact request once.
    ///
    /// # Errors
    /// Returns validation or signing failures without exposing credentials.
    fn prepare(
        &self,
        command: &OrderCommand,
        credentials: &ApiCredentials,
        at: DateTime<Utc>,
    ) -> Result<SignedRequest, ExecutionError>;
    /// Parses a response; acceptance never implies a fill.
    ///
    /// # Errors
    /// Returns venue rejection or ambiguity requiring order reconciliation.
    fn parse_response(
        &self,
        command: &OrderCommand,
        status: u16,
        body: &[u8],
    ) -> Result<OrderAck, ExecutionError>;
    /// Parses every event in a private-stream frame with exact decimals.
    ///
    /// # Errors
    /// Returns malformed or unsupported event errors.
    fn parse_user_message(&self, payload: &[u8]) -> Result<Vec<UserEvent>, ExecutionError>;
}

/// Generates lowercase hex HMAC-SHA256 for Binance and Bybit.
///
/// # Errors
/// Returns a signing error if the key cannot initialize the MAC.
pub fn hmac_sha256_hex(secret: &str, payload: &[u8]) -> Result<String, ExecutionError> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_| ExecutionError::Signing)?;
    mac.update(payload);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

/// Generates Base64 HMAC-SHA256 for OKX.
///
/// # Errors
/// Returns a signing error if the key cannot initialize the MAC.
pub fn hmac_sha256_base64(secret: &str, payload: &[u8]) -> Result<String, ExecutionError> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_| ExecutionError::Signing)?;
    mac.update(payload);
    Ok(STANDARD.encode(mac.finalize().into_bytes()))
}

/// Sanitized execution error. Bodies, URLs and credentials are not retained.
#[derive(thiserror::Error)]
pub enum ExecutionError {
    /// Locally invalid command.
    #[error("invalid execution request: {0}")]
    InvalidRequest(&'static str),
    /// Malformed or unsupported protocol payload.
    #[error("execution protocol error: {0}")]
    Protocol(&'static str),
    /// Definitive venue rejection, carrying only its machine code.
    #[error("venue rejected operation with code {code}")]
    VenueRejected {
        /// Sanitized exchange code.
        code: String,
    },
    /// Server may have accepted the command; never retry a placement blindly.
    #[error("execution result unknown; reconcile by client order ID")]
    AmbiguousResponse,
    /// Malformed JSON. The source is discarded to prevent value leakage.
    #[error("invalid execution JSON")]
    Json,
    /// MAC initialization failed.
    #[error("request signing failed")]
    Signing,
    /// Invalid exchange timestamp.
    #[error("invalid execution timestamp")]
    Timestamp,
    /// HTTP rejection without a recognized venue body.
    #[error("execution HTTP status {0}")]
    HttpStatus(u16),
    /// Decimal operation exceeded its range.
    #[error("execution arithmetic overflow")]
    Arithmetic,
    /// Account must be reconciled before admitting new orders.
    #[error("account reconciliation required")]
    ReconciliationRequired,
    /// Actor downstream closed.
    #[error("execution output channel closed")]
    OutputClosed,
}

impl fmt::Debug for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // serde's Debug may include a rejected JSON value. Never expose it.
        fmt::Display::fmt(self, f)
    }
}

impl From<serde_json::Error> for ExecutionError {
    fn from(_: serde_json::Error) -> Self {
        Self::Json
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_error_source_does_not_disclose_rejected_values(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let raw = serde_json::from_slice::<u64>(br#""do-not-log-this-account-value""#)
            .err()
            .ok_or("expected a JSON type error")?;
        let sanitized = ExecutionError::from(raw);
        assert!(!format!("{sanitized:?} {sanitized}").contains("do-not-log"));
        assert!(std::error::Error::source(&sanitized).is_none());
        Ok(())
    }

    #[test]
    fn signing_known_vector_and_redacted_debug() -> Result<(), ExecutionError> {
        assert_eq!(
            hmac_sha256_hex("key", b"The quick brown fox jumps over the lazy dog")?,
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
        let credentials =
            ApiCredentials::new("private-key".to_owned(), "private-secret".to_owned(), None)?;
        let request = SignedRequest::new(
            Method::POST,
            "/order".to_owned(),
            "signature=secret".to_owned(),
            Bytes::from_static(b"private-body"),
            vec![("authorization".to_owned(), "private-token".to_owned())],
        )?;
        assert!(!format!("{credentials:?}{request:?}").contains("private-"));
        assert!(!format!("{request:?}").contains("signature=secret"));
        Ok(())
    }
}
