//! Binance USD-M one-way execution protocol.
//!
//! Conditional orders use `/fapi/v1/algoOrder`, not the legacy regular-order
//! endpoint. See the official [trade API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)
//! and [user stream](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams).
//!
//! Triggered matching-engine children can have different client IDs from their
//! parent algo. Parent `ALGO_UPDATE` cumulative quantities preserve correlation
//! with our protective order; missing evidence requires REST reconciliation.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use reqwest::Method;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use crate::models::{Exchange, InstrumentId, MarketKind};

use super::{
    hmac_sha256_hex, ApiCredentials, ExecutionAdapter, ExecutionError, OrderAck, OrderCommand,
    OrderKind, OrderRef, OrderRequest, OrderSide, OrderStatus, OrderUpdate, PositionUpdate,
    SignedRequest, UserEvent,
};

/// Protocol adapter for Binance USD-M linear perpetuals in one-way mode.
#[derive(Clone, Debug)]
pub struct BinanceExecution {
    recv_window_millis: u64,
}

impl Default for BinanceExecution {
    fn default() -> Self {
        Self {
            recv_window_millis: 5_000,
        }
    }
}

// Instance helpers keep the transport's per-venue adapter API consistent.
#[allow(clippy::unused_self)]
impl BinanceExecution {
    /// Creates an adapter with a signed-request validity window.
    ///
    /// # Errors
    /// Returns an error unless the window is between 1 and 60,000 milliseconds.
    pub fn new(recv_window_millis: u64) -> Result<Self, ExecutionError> {
        if !(1..=60_000).contains(&recv_window_millis) {
            return Err(ExecutionError::InvalidRequest("invalid Binance recvWindow"));
        }
        Ok(Self { recv_window_millis })
    }

    /// Builds an API-key-only create, keepalive, or close user-stream request.
    ///
    /// The endpoint addresses the account's active stream; Binance does not
    /// require a `listenKey` parameter. A supplied token is validated but omitted.
    /// Refresh the key before its 60-minute lifetime elapses.
    ///
    /// # Errors
    /// Returns an error for an unsupported method, invalid token, or invalid header.
    pub fn listen_key_request(
        &self,
        credentials: &ApiCredentials,
        method: Method,
        listen_key: Option<&str>,
    ) -> Result<SignedRequest, ExecutionError> {
        if method != Method::POST && method != Method::PUT && method != Method::DELETE {
            return Err(ExecutionError::InvalidRequest("invalid listen-key method"));
        }
        if let Some(key) = listen_key {
            validate_token(key)?;
        }
        SignedRequest::new(
            method,
            "/fapi/v1/listenKey".to_owned(),
            String::new(),
            Bytes::new(),
            api_key_header(credentials),
        )
    }

    /// Extracts a listen key from a successful create or keepalive response.
    ///
    /// # Errors
    /// Returns an error for a venue rejection, malformed JSON, or invalid token.
    pub fn parse_listen_key(&self, body: &[u8]) -> Result<String, ExecutionError> {
        let value: Value = serde_json::from_slice(body)?;
        reject_code(&value)?;
        let token = required_text(&value, "listenKey")?;
        validate_token(token)?;
        Ok(token.to_owned())
    }

    /// Builds the private stream URL for production or demo trading.
    ///
    /// This URL contains a bearer credential and must never be logged. Rotate
    /// each connection before Binance's 24-hour connection lifetime.
    ///
    /// # Errors
    /// Returns an error for a token containing URL delimiters or invalid bytes.
    pub fn websocket_url(&self, listen_key: &str, demo: bool) -> Result<String, ExecutionError> {
        validate_token(listen_key)?;
        let base = if demo {
            "wss://demo-fstream.binance.com/ws"
        } else {
            "wss://fstream.binance.com/private/ws"
        };
        Ok(format!("{base}/{listen_key}"))
    }

    /// Parses an ordinary-order REST snapshot with cumulative execution evidence.
    ///
    /// Algo snapshots whose matching-engine terminal state remains ambiguous
    /// require reconciliation with their child order and account position.
    ///
    /// # Errors
    /// Returns an error for a venue rejection, missing execution fields, an
    /// identity mismatch, an unsupported position mode, or ambiguous algo status.
    pub fn parse_query_order(
        &self,
        reference: &OrderRef,
        status: u16,
        body: &[u8],
    ) -> Result<OrderUpdate, ExecutionError> {
        let ack = self.parse_response(&OrderCommand::Query(reference.clone()), status, body)?;
        let value: Value = serde_json::from_slice(body)?;
        let venue_order_id = ack
            .venue_order_id
            .ok_or(ExecutionError::AmbiguousResponse)?;
        if reference
            .venue_order_id
            .as_ref()
            .is_some_and(|id| *id != venue_order_id)
            || required_text(&value, "symbol")? != reference.instrument.venue_symbol.as_str()
            || required_text(&value, "positionSide")? != "BOTH"
        {
            return Err(ExecutionError::Protocol(
                "Binance query identity or account mode mismatch",
            ));
        }
        let conditional = is_conditional(reference.kind);
        let state = required_text(&value, if conditional { "algoStatus" } else { "status" })?;
        let quantity = decimal(required_text(
            &value,
            if conditional { "quantity" } else { "origQty" },
        )?)?;
        let cumulative_filled = if conditional {
            match value.get("actualQty") {
                Some(Value::String(text)) => decimal(text)?,
                None if matches!(state, "NEW" | "CANCELED" | "REJECTED" | "EXPIRED")
                    && value.get("actualOrderId").and_then(Value::as_str) == Some("") =>
                {
                    Decimal::ZERO
                }
                _ => return Err(ExecutionError::AmbiguousResponse),
            }
        } else {
            decimal(required_text(&value, "executedQty")?)?
        };
        let average_price = average_price(required_text(
            &value,
            if conditional {
                "actualPrice"
            } else {
                "avgPrice"
            },
        )?)?;
        let status = if conditional && state == "FINISHED" {
            finished_status(quantity, cumulative_filled, average_price)?
        } else {
            ack.status.ok_or(ExecutionError::AmbiguousResponse)?
        };
        let at = value
            .get("updateTime")
            .and_then(Value::as_i64)
            .ok_or(ExecutionError::Timestamp)?;
        let update = OrderUpdate {
            exchange: Exchange::Binance,
            symbol: reference.instrument.venue_symbol.to_string(),
            client_order_id: reference.client_order_id.clone(),
            venue_order_id,
            status,
            quantity,
            cumulative_filled,
            average_price,
            at: event_time(at)?,
        };
        update.validate()?;
        Ok(update)
    }
}

impl ExecutionAdapter for BinanceExecution {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }

    fn prepare(
        &self,
        command: &OrderCommand,
        credentials: &ApiCredentials,
        at: DateTime<Utc>,
    ) -> Result<SignedRequest, ExecutionError> {
        let (method, path, mut params) = command_parameters(command)?;
        if at.timestamp_millis() < 0 {
            return Err(ExecutionError::Timestamp);
        }
        params.push(("recvWindow", self.recv_window_millis.to_string()));
        params.push(("timestamp", at.timestamp_millis().to_string()));
        let mut query = serde_urlencoded::to_string(params)
            .map_err(|_| ExecutionError::InvalidRequest("cannot encode Binance parameters"))?;
        let signature = hmac_sha256_hex(credentials.secret(), query.as_bytes())?;
        query.push_str("&signature=");
        query.push_str(&signature);
        SignedRequest::new(
            method,
            path.to_owned(),
            query,
            Bytes::new(),
            api_key_header(credentials),
        )
    }

    fn parse_response(
        &self,
        command: &OrderCommand,
        status: u16,
        body: &[u8],
    ) -> Result<OrderAck, ExecutionError> {
        // A backend timeout may follow a successful mutation. Never translate it
        // into an ordinary rejection that could release risk or cause a retry.
        if status == 408 || status >= 500 {
            return Err(ExecutionError::AmbiguousResponse);
        }
        let value: Value = serde_json::from_slice(body)?;
        reject_code(&value)?;
        if !(200..300).contains(&status) {
            return Err(ExecutionError::HttpStatus(status));
        }
        let (instrument, client_id, kind) = command_identity(command);
        validate_instrument(instrument)?;
        let conditional = is_conditional(kind);
        let id_field = if conditional {
            "clientAlgoId"
        } else {
            "clientOrderId"
        };
        let venue_field = if conditional { "algoId" } else { "orderId" };
        if required_text(&value, id_field)? != client_id {
            return Err(ExecutionError::Protocol(
                "Binance acknowledgement ID mismatch",
            ));
        }
        if let Some(symbol) = value.get("symbol") {
            if symbol.as_str() != Some(instrument.venue_symbol.as_str()) {
                return Err(ExecutionError::Protocol(
                    "Binance acknowledgement symbol mismatch",
                ));
            }
        }
        let venue_order_id = numeric_id(&value, venue_field)?;
        let status = if conditional && matches!(command, OrderCommand::Cancel(_)) {
            Some(OrderStatus::Cancelled)
        } else if let Some(state) = value.get(if conditional { "algoStatus" } else { "status" }) {
            let state = state
                .as_str()
                .ok_or(ExecutionError::Protocol("invalid Binance status"))?;
            if conditional && state == "FINISHED" {
                // FINISHED includes canceled matching-engine children.
                None
            } else {
                Some(parse_status(state)?)
            }
        } else {
            None
        };
        Ok(OrderAck {
            client_order_id: client_id.to_owned(),
            venue_order_id: Some(venue_order_id),
            status,
        })
    }

    fn parse_user_message(&self, payload: &[u8]) -> Result<Vec<UserEvent>, ExecutionError> {
        let event: EventType<'_> = serde_json::from_slice(payload)?;
        match event.event_type {
            "ORDER_TRADE_UPDATE" => parse_order_event(payload),
            "ALGO_UPDATE" => parse_algo_event(payload),
            "ACCOUNT_UPDATE" => parse_account_event(payload),
            "listenKeyExpired"
            | "MARGIN_CALL"
            | "ACCOUNT_CONFIG_UPDATE"
            | "CONDITIONAL_ORDER_TRIGGER_REJECT" => Ok(vec![UserEvent::ReconciliationRequired]),
            // TRADE_LITE has no cumulative quantity. Applying it alongside the
            // full order stream would double-count fills.
            _ => Ok(vec![UserEvent::Ignored]),
        }
    }
}

type Parameters = Vec<(&'static str, String)>;

fn command_parameters(
    command: &OrderCommand,
) -> Result<(Method, &'static str, Parameters), ExecutionError> {
    let (instrument, client_id, kind) = command_identity(command);
    validate_instrument(instrument)?;
    validate_client_id(client_id)?;
    let conditional = is_conditional(kind);
    let path = if conditional {
        "/fapi/v1/algoOrder"
    } else {
        "/fapi/v1/order"
    };
    let mut params = if conditional && !matches!(command, OrderCommand::Place(_)) {
        // Algo query/cancel endpoints accept clientAlgoId or algoId only.
        Vec::new()
    } else {
        vec![("symbol", instrument.venue_symbol.to_string())]
    };
    let method = match command {
        OrderCommand::Place(order) => {
            order.validate()?;
            place_parameters(order, conditional, &mut params)?;
            Method::POST
        }
        OrderCommand::Cancel(_) | OrderCommand::Query(_) => {
            params.push((
                if conditional {
                    "clientAlgoId"
                } else {
                    "origClientOrderId"
                },
                client_id.to_owned(),
            ));
            if matches!(command, OrderCommand::Cancel(_)) {
                Method::DELETE
            } else {
                Method::GET
            }
        }
    };
    Ok((method, path, params))
}

fn place_parameters(
    order: &OrderRequest,
    conditional: bool,
    params: &mut Parameters,
) -> Result<(), ExecutionError> {
    params.push((
        "side",
        match order.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
        .to_owned(),
    ));
    params.push(("positionSide", "BOTH".to_owned()));
    params.push((
        "type",
        match order.kind {
            OrderKind::Limit => "LIMIT",
            OrderKind::Market => "MARKET",
            OrderKind::StopMarket => "STOP_MARKET",
            OrderKind::TakeProfitMarket => "TAKE_PROFIT_MARKET",
        }
        .to_owned(),
    ));
    params.push(("quantity", order.quantity.normalize().to_string()));
    params.push(("reduceOnly", order.reduce_only.to_string()));
    if conditional {
        if !order.reduce_only {
            return Err(ExecutionError::InvalidRequest(
                "protective orders must be reduce-only",
            ));
        }
        params.push(("algoType", "CONDITIONAL".to_owned()));
        params.push(("clientAlgoId", order.client_order_id.clone()));
        let trigger = order
            .trigger_price
            .ok_or(ExecutionError::InvalidRequest("missing trigger price"))?;
        params.push(("triggerPrice", trigger.normalize().to_string()));
        params.push(("workingType", "MARK_PRICE".to_owned()));
        // Enabling priceProtect can delay a protective stop during dislocation.
        params.push(("priceProtect", "false".to_owned()));
    } else {
        params.push(("newClientOrderId", order.client_order_id.clone()));
        params.push(("newOrderRespType", "ACK".to_owned()));
        if order.kind == OrderKind::Limit {
            let price = order
                .price
                .ok_or(ExecutionError::InvalidRequest("missing limit price"))?;
            params.push(("price", price.normalize().to_string()));
            params.push(("timeInForce", "GTC".to_owned()));
        }
    }
    Ok(())
}

fn command_identity(command: &OrderCommand) -> (&InstrumentId, &str, OrderKind) {
    match command {
        OrderCommand::Place(order) => (&order.instrument, &order.client_order_id, order.kind),
        OrderCommand::Cancel(order) | OrderCommand::Query(order) => {
            (&order.instrument, &order.client_order_id, order.kind)
        }
    }
}

fn is_conditional(kind: OrderKind) -> bool {
    matches!(kind, OrderKind::StopMarket | OrderKind::TakeProfitMarket)
}

fn validate_instrument(instrument: &InstrumentId) -> Result<(), ExecutionError> {
    if instrument.exchange != Exchange::Binance || instrument.market != MarketKind::LinearPerpetual
    {
        return Err(ExecutionError::InvalidRequest(
            "Binance requires a USD-M linear perpetual",
        ));
    }
    let symbol = instrument.venue_symbol.as_str();
    if symbol.len() > 64
        || !symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(ExecutionError::InvalidRequest("invalid Binance symbol"));
    }
    Ok(())
}

fn validate_client_id(value: &str) -> Result<(), ExecutionError> {
    if value.is_empty()
        || value.len() > 36
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".-_:/".contains(&byte))
    {
        return Err(ExecutionError::InvalidRequest(
            "invalid Binance client order ID",
        ));
    }
    Ok(())
}

fn validate_token(value: &str) -> Result<(), ExecutionError> {
    if value.is_empty()
        || value.len() > 512
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
    {
        return Err(ExecutionError::Protocol("invalid Binance listen key"));
    }
    Ok(())
}

fn api_key_header(credentials: &ApiCredentials) -> Vec<(String, String)> {
    vec![("X-MBX-APIKEY".to_owned(), credentials.api_key().to_owned())]
}

fn reject_code(value: &Value) -> Result<(), ExecutionError> {
    if let Some(code) = value.get("code") {
        let parsed = match code {
            Value::Number(number) => number.as_i64(),
            Value::String(text) => text.parse::<i64>().ok(),
            _ => None,
        }
        .ok_or(ExecutionError::Protocol("invalid Binance error code"))?;
        if matches!(parsed, -1000 | -1006 | -1007) {
            return Err(ExecutionError::AmbiguousResponse);
        }
        if parsed != 0 && parsed != 200 {
            return Err(ExecutionError::VenueRejected {
                code: parsed.to_string(),
            });
        }
    }
    Ok(())
}

fn required_text<'a>(value: &'a Value, key: &str) -> Result<&'a str, ExecutionError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .ok_or(ExecutionError::Protocol("missing Binance text field"))
}

fn numeric_id(value: &Value, key: &str) -> Result<String, ExecutionError> {
    let id = match value.get(key) {
        Some(Value::Number(number)) => number.as_u64(),
        Some(Value::String(text)) => text.parse::<u64>().ok(),
        _ => None,
    }
    .filter(|id| *id > 0)
    .ok_or(ExecutionError::Protocol("invalid Binance order ID"))?;
    Ok(id.to_string())
}

fn parse_status(value: &str) -> Result<OrderStatus, ExecutionError> {
    match value {
        "NEW" => Ok(OrderStatus::New),
        "PARTIALLY_FILLED" => Ok(OrderStatus::PartiallyFilled),
        "FILLED" => Ok(OrderStatus::Filled),
        "CANCELED" => Ok(OrderStatus::Cancelled),
        "REJECTED" => Ok(OrderStatus::Rejected),
        "EXPIRED" | "EXPIRED_IN_MATCH" => Ok(OrderStatus::Expired),
        "TRIGGERING" | "TRIGGERED" => Ok(OrderStatus::Triggered),
        _ => Err(ExecutionError::Protocol("unknown Binance order status")),
    }
}

fn decimal(value: &str) -> Result<Decimal, ExecutionError> {
    Decimal::from_str_exact(value).map_err(|_| ExecutionError::Protocol("invalid Binance decimal"))
}

fn event_time(value: i64) -> Result<DateTime<Utc>, ExecutionError> {
    if value < 0 {
        return Err(ExecutionError::Timestamp);
    }
    DateTime::from_timestamp_millis(value).ok_or(ExecutionError::Timestamp)
}

fn average_price(value: &str) -> Result<Option<Decimal>, ExecutionError> {
    let price = decimal(value)?;
    if price < Decimal::ZERO {
        return Err(ExecutionError::Protocol("negative Binance average price"));
    }
    Ok((price > Decimal::ZERO).then_some(price))
}

fn finished_status(
    quantity: Decimal,
    cumulative: Decimal,
    average: Option<Decimal>,
) -> Result<OrderStatus, ExecutionError> {
    if quantity <= Decimal::ZERO || cumulative < Decimal::ZERO || cumulative > quantity {
        return Err(ExecutionError::Protocol(
            "invalid Binance algo execution quantity",
        ));
    }
    if cumulative > Decimal::ZERO && average.is_none() {
        return Err(ExecutionError::AmbiguousResponse);
    }
    // FINISHED guarantees that the child has filled or been canceled. Exact
    // cumulative evidence, not this status alone, distinguishes the two cases.
    Ok(if cumulative == quantity {
        OrderStatus::Filled
    } else {
        OrderStatus::Cancelled
    })
}

#[derive(Deserialize)]
struct EventType<'a> {
    #[serde(rename = "e")]
    event_type: &'a str,
}

#[derive(Deserialize)]
struct OrderEvent<'a> {
    #[serde(rename = "E")]
    at: i64,
    #[serde(rename = "o", borrow)]
    order: WireOrder<'a>,
}

#[derive(Deserialize)]
struct WireOrder<'a> {
    #[serde(rename = "s")]
    symbol: &'a str,
    #[serde(rename = "c")]
    client_id: &'a str,
    #[serde(rename = "i")]
    id: u64,
    #[serde(rename = "X")]
    status: &'a str,
    #[serde(rename = "q")]
    quantity: &'a str,
    #[serde(rename = "z")]
    cumulative: &'a str,
    #[serde(rename = "ap")]
    average: &'a str,
    #[serde(rename = "ps")]
    position_side: &'a str,
}

fn parse_order_event(payload: &[u8]) -> Result<Vec<UserEvent>, ExecutionError> {
    let event: OrderEvent<'_> = serde_json::from_slice(payload)?;
    let order = event.order;
    if order.position_side != "BOTH" {
        return Ok(vec![UserEvent::ReconciliationRequired]);
    }
    let update = OrderUpdate {
        exchange: Exchange::Binance,
        symbol: order.symbol.to_owned(),
        client_order_id: order.client_id.to_owned(),
        venue_order_id: order.id.to_string(),
        status: parse_status(order.status)?,
        quantity: decimal(order.quantity)?,
        cumulative_filled: decimal(order.cumulative)?,
        average_price: average_price(order.average)?,
        at: event_time(event.at)?,
    };
    if order.id == 0 {
        return Err(ExecutionError::Protocol("invalid Binance order ID"));
    }
    update.validate()?;
    Ok(vec![UserEvent::Order(update)])
}

#[derive(Deserialize)]
struct AlgoEvent<'a> {
    #[serde(rename = "E")]
    at: i64,
    #[serde(rename = "o", borrow)]
    order: WireAlgo<'a>,
}

#[derive(Deserialize)]
struct WireAlgo<'a> {
    #[serde(rename = "s")]
    symbol: &'a str,
    #[serde(rename = "caid")]
    client_id: &'a str,
    #[serde(rename = "aid")]
    id: u64,
    #[serde(rename = "X")]
    status: &'a str,
    #[serde(rename = "q")]
    quantity: &'a str,
    #[serde(rename = "aq", default)]
    cumulative: Option<&'a str>,
    #[serde(rename = "ap", default)]
    average: Option<&'a str>,
    #[serde(rename = "ai", default)]
    child_id: Option<&'a str>,
    #[serde(rename = "ps")]
    position_side: &'a str,
}

fn parse_algo_event(payload: &[u8]) -> Result<Vec<UserEvent>, ExecutionError> {
    let event: AlgoEvent<'_> = serde_json::from_slice(payload)?;
    let order = event.order;
    if order.position_side != "BOTH"
        || (order.status == "FINISHED" && (order.cumulative.is_none() || order.average.is_none()))
    {
        return Ok(vec![UserEvent::ReconciliationRequired]);
    }
    let cumulative_filled = match order.cumulative {
        Some(value) => decimal(value)?,
        None if order.status == "NEW" || order.child_id == Some("") => Decimal::ZERO,
        None => return Ok(vec![UserEvent::ReconciliationRequired]),
    };
    let average = match order.average {
        Some(value) => average_price(value)?,
        None if cumulative_filled == Decimal::ZERO => None,
        None => return Ok(vec![UserEvent::ReconciliationRequired]),
    };
    let quantity = decimal(order.quantity)?;
    let status = if order.status == "FINISHED" {
        match finished_status(quantity, cumulative_filled, average) {
            Ok(status) => status,
            Err(ExecutionError::AmbiguousResponse) => {
                return Ok(vec![UserEvent::ReconciliationRequired])
            }
            Err(error) => return Err(error),
        }
    } else {
        parse_status(order.status)?
    };
    let update = OrderUpdate {
        exchange: Exchange::Binance,
        symbol: order.symbol.to_owned(),
        client_order_id: order.client_id.to_owned(),
        venue_order_id: order.id.to_string(),
        status,
        quantity,
        cumulative_filled,
        average_price: average,
        at: event_time(event.at)?,
    };
    if order.id == 0 {
        return Err(ExecutionError::Protocol("invalid Binance algo ID"));
    }
    update.validate()?;
    Ok(vec![UserEvent::Order(update)])
}

#[derive(Deserialize)]
struct AccountEvent<'a> {
    #[serde(rename = "E")]
    at: i64,
    #[serde(rename = "a", borrow)]
    account: WireAccount<'a>,
}

#[derive(Deserialize)]
struct WireAccount<'a> {
    #[serde(rename = "P", borrow, default)]
    positions: Vec<WirePosition<'a>>,
}

#[derive(Deserialize)]
struct WirePosition<'a> {
    #[serde(rename = "s")]
    symbol: &'a str,
    #[serde(rename = "pa")]
    quantity: &'a str,
    #[serde(rename = "ps")]
    position_side: &'a str,
}

fn parse_account_event(payload: &[u8]) -> Result<Vec<UserEvent>, ExecutionError> {
    let event: AccountEvent<'_> = serde_json::from_slice(payload)?;
    let at = event_time(event.at)?;
    let mut updates = Vec::with_capacity(event.account.positions.len());
    for position in event.account.positions {
        if position.position_side != "BOTH" {
            return Ok(vec![UserEvent::ReconciliationRequired]);
        }
        if position.symbol.is_empty() {
            return Err(ExecutionError::Protocol("empty Binance position symbol"));
        }
        updates.push(UserEvent::Position(PositionUpdate {
            exchange: Exchange::Binance,
            symbol: position.symbol.to_owned(),
            signed_quantity: decimal(position.quantity)?,
            at,
        }));
    }
    Ok(updates)
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn order(kind: OrderKind) -> Result<OrderRequest, Box<dyn std::error::Error>> {
        Ok(OrderRequest {
            instrument: InstrumentId::new(
                Exchange::Binance,
                MarketKind::LinearPerpetual,
                "BTCUSDT",
                "BTC-USDT-PERP",
            )?,
            client_order_id: "ictEntry01".to_owned(),
            side: OrderSide::Buy,
            kind,
            quantity: Decimal::new(123, 3),
            price: (kind == OrderKind::Limit).then_some(Decimal::new(6_512_345, 2)),
            trigger_price: is_conditional(kind).then_some(Decimal::new(640_001, 1)),
            reduce_only: is_conditional(kind),
        })
    }

    #[test]
    fn routes_protection_to_algo_service_with_decimal_parameters() -> TestResult {
        let stop = OrderCommand::Place(order(OrderKind::StopMarket)?);
        let (method, path, params) = command_parameters(&stop)?;
        assert_eq!(method, Method::POST);
        assert_eq!(path, "/fapi/v1/algoOrder");
        assert!(params.contains(&("triggerPrice", "64000.1".to_owned())));
        assert!(params.contains(&("quantity", "0.123".to_owned())));
        assert!(params.contains(&("clientAlgoId", "ictEntry01".to_owned())));
        assert!(params.contains(&("reduceOnly", "true".to_owned())));
        assert!(!params
            .iter()
            .any(|(key, _)| *key == "stopPrice" || *key == "newClientOrderId"));
        Ok(())
    }

    #[test]
    fn limit_entry_and_conditional_cancel_use_separate_identifiers() -> TestResult {
        let entry = order(OrderKind::Limit)?;
        let (_, path, params) = command_parameters(&OrderCommand::Place(entry.clone()))?;
        assert_eq!(path, "/fapi/v1/order");
        assert!(params.contains(&("timeInForce", "GTC".to_owned())));
        assert!(params.contains(&("price", "65123.45".to_owned())));
        let cancel = OrderCommand::Cancel(OrderRef {
            instrument: entry.instrument,
            client_order_id: "ictStop01".to_owned(),
            venue_order_id: None,
            kind: OrderKind::StopMarket,
        });
        let (method, path, params) = command_parameters(&cancel)?;
        assert_eq!(method, Method::DELETE);
        assert_eq!(path, "/fapi/v1/algoOrder");
        assert!(params.contains(&("clientAlgoId", "ictStop01".to_owned())));
        assert!(!params.iter().any(|(key, _)| *key == "symbol"));
        Ok(())
    }

    #[test]
    fn conditional_cannot_open_exposure_or_inject_parameters() -> TestResult {
        let mut stop = order(OrderKind::StopMarket)?;
        stop.reduce_only = false;
        assert!(command_parameters(&OrderCommand::Place(stop)).is_err());
        let mut entry = order(OrderKind::Limit)?;
        entry.client_order_id = "bad&quantity=9".to_owned();
        assert!(command_parameters(&OrderCommand::Place(entry)).is_err());
        assert!(BinanceExecution::new(0).is_err());
        assert!(BinanceExecution::new(60_001).is_err());
        Ok(())
    }

    #[test]
    fn acknowledgement_must_match_and_timeout_is_ambiguous() -> TestResult {
        let adapter = BinanceExecution::default();
        let command = OrderCommand::Place(order(OrderKind::Limit)?);
        let body =
            br#"{"clientOrderId":"ictEntry01","orderId":123,"symbol":"BTCUSDT","status":"NEW"}"#;
        let ack = adapter.parse_response(&command, 200, body)?;
        assert_eq!(ack.venue_order_id.as_deref(), Some("123"));
        assert!(adapter
            .parse_response(&command, 200, br#"{"clientOrderId":"other","orderId":123}"#)
            .is_err());
        assert!(matches!(
            adapter.parse_response(&command, 503, b"upstream timeout"),
            Err(ExecutionError::AmbiguousResponse)
        ));
        assert!(matches!(
            adapter.parse_response(&command, 400, br#"{"code":-1007,"msg":"timeout"}"#),
            Err(ExecutionError::AmbiguousResponse)
        ));
        assert!(
            matches!(adapter.parse_response(&command, 400, br#"{"code":-2019,"msg":"secret-like raw text"}"#), Err(ExecutionError::VenueRejected { code }) if code == "-2019")
        );
        Ok(())
    }

    #[test]
    fn signature_covers_the_exact_query_and_listen_key_is_unsigned() -> TestResult {
        let adapter = BinanceExecution::default();
        let credentials = ApiCredentials::new("testkey".to_owned(), "testsecret".to_owned(), None)?;
        let request = adapter.prepare(
            &OrderCommand::Place(order(OrderKind::Limit)?),
            &credentials,
            event_time(1_750_515_742_303)?,
        )?;
        let (prehash, signature) = request
            .query()
            .rsplit_once("&signature=")
            .ok_or("missing signature")?;
        assert_eq!(
            signature,
            hmac_sha256_hex("testsecret", prehash.as_bytes())?
        );
        assert!(prehash.ends_with("recvWindow=5000&timestamp=1750515742303"));
        assert!(request.body().is_empty());
        let keepalive = adapter.listen_key_request(&credentials, Method::PUT, Some("token123"))?;
        assert!(keepalive.query().is_empty());
        assert_eq!(keepalive.path(), "/fapi/v1/listenKey");
        Ok(())
    }

    #[test]
    fn query_snapshot_preserves_partial_fill_before_cancel() -> TestResult {
        let request = order(OrderKind::Limit)?;
        let reference = request.reference(Some("123".to_owned()));
        let adapter = BinanceExecution::default();
        let update = adapter.parse_query_order(&reference, 200, br#"{"clientOrderId":"ictEntry01","orderId":123,"symbol":"BTCUSDT","positionSide":"BOTH","status":"CANCELED","origQty":"0.123","executedQty":"0.001","avgPrice":"65123.45","updateTime":1750515742303}"#)?;
        assert_eq!(update.status, OrderStatus::Cancelled);
        assert_eq!(update.cumulative_filled, Decimal::new(1, 3));
        assert!(adapter.parse_query_order(&reference, 200, br#"{"clientOrderId":"ictEntry01","orderId":999,"symbol":"BTCUSDT","positionSide":"BOTH","status":"CANCELED"}"#).is_err());
        Ok(())
    }

    #[test]
    fn algo_query_cannot_infer_zero_execution_from_missing_fields() -> TestResult {
        let reference = order(OrderKind::StopMarket)?.reference(Some("100".to_owned()));
        let adapter = BinanceExecution::default();
        let completed = br#"{"clientAlgoId":"ictEntry01","algoId":100,"symbol":"BTCUSDT","positionSide":"BOTH","algoStatus":"FINISHED","quantity":"0.123","actualQty":"0.123","actualPrice":"64000","updateTime":1750515742303}"#;
        assert_eq!(
            adapter
                .parse_query_order(&reference, 200, completed)?
                .status,
            OrderStatus::Filled
        );
        let missing = br#"{"clientAlgoId":"ictEntry01","algoId":100,"symbol":"BTCUSDT","positionSide":"BOTH","algoStatus":"FINISHED","quantity":"0.123","actualPrice":"64000","updateTime":1750515742303}"#;
        assert!(matches!(
            adapter.parse_query_order(&reference, 200, missing),
            Err(ExecutionError::AmbiguousResponse)
        ));
        Ok(())
    }

    #[test]
    fn fill_preserves_exact_cumulative_quantity_and_exchange_event_time() -> TestResult {
        let events = BinanceExecution::default().parse_user_message(br#"{"e":"ORDER_TRADE_UPDATE","E":1750515742303,"T":1750515742297,"o":{"s":"BTCUSDT","c":"ictEntry01","i":123,"X":"PARTIALLY_FILLED","q":"0.123","z":"0.001","ap":"65123.45000001","ps":"BOTH"}}"#)?;
        assert!(
            matches!(events.as_slice(), [UserEvent::Order(update)] if update.cumulative_filled == Decimal::new(1, 3) && update.at.timestamp_millis() == 1_750_515_742_303 && update.average_price == Some(Decimal::new(6_512_345_000_001, 8)))
        );
        Ok(())
    }

    #[test]
    fn rejects_numeric_decimal_and_overfilled_order() {
        let adapter = BinanceExecution::default();
        assert!(adapter.parse_user_message(br#"{"e":"ORDER_TRADE_UPDATE","E":1750515742303,"o":{"s":"BTCUSDT","c":"ictEntry01","i":123,"X":"FILLED","q":0.123,"z":"0.123","ap":"65123","ps":"BOTH"}}"#).is_err());
        assert!(adapter.parse_user_message(br#"{"e":"ORDER_TRADE_UPDATE","E":1750515742303,"o":{"s":"BTCUSDT","c":"ictEntry01","i":123,"X":"FILLED","q":"0.123","z":"0.124","ap":"65123","ps":"BOTH"}}"#).is_err());
        assert!(decimal("0.00000000000000000000000000001").is_err());
    }

    #[test]
    fn finished_algo_retains_partial_fill_and_cancels_the_remainder() -> TestResult {
        let events = BinanceExecution::default().parse_user_message(br#"{"e":"ALGO_UPDATE","E":1750515742303,"o":{"s":"BTCUSDT","caid":"ictStop01","aid":100,"X":"FINISHED","q":"0.123","aq":"0.001","ap":"64000","ps":"BOTH"}}"#)?;
        assert!(
            matches!(events.as_slice(), [UserEvent::Order(update)] if update.status == OrderStatus::Cancelled && update.cumulative_filled == Decimal::new(1, 3))
        );
        Ok(())
    }

    #[test]
    fn finished_algo_requires_explicit_execution_evidence() -> TestResult {
        let adapter = BinanceExecution::default();
        let missing = adapter.parse_user_message(br#"{"e":"ALGO_UPDATE","E":1750515742303,"o":{"s":"BTCUSDT","caid":"ictStop01","aid":100,"X":"FINISHED","q":"0.123","ap":"64000","ps":"BOTH"}}"#)?;
        assert!(matches!(
            missing.as_slice(),
            [UserEvent::ReconciliationRequired]
        ));
        let zero_average = adapter.parse_user_message(br#"{"e":"ALGO_UPDATE","E":1750515742303,"o":{"s":"BTCUSDT","caid":"ictStop01","aid":100,"X":"FINISHED","q":"0.123","aq":"0.123","ap":"0","ps":"BOTH"}}"#)?;
        assert!(matches!(
            zero_average.as_slice(),
            [UserEvent::ReconciliationRequired]
        ));
        let full = adapter.parse_user_message(br#"{"e":"ALGO_UPDATE","E":1750515742303,"o":{"s":"BTCUSDT","caid":"ictStop01","aid":100,"X":"FINISHED","q":"0.123","aq":"0.123","ap":"64000","ps":"BOTH"}}"#)?;
        assert!(
            matches!(full.as_slice(), [UserEvent::Order(update)] if update.status == OrderStatus::Filled && update.cumulative_filled == Decimal::new(123, 3))
        );
        Ok(())
    }

    #[test]
    fn pending_algo_accepts_absent_execution_fields() -> TestResult {
        let events = BinanceExecution::default().parse_user_message(br#"{"e":"ALGO_UPDATE","E":1750515742303,"o":{"s":"BTCUSDT","caid":"ictStop01","aid":100,"X":"NEW","q":"0.123","ps":"BOTH"}}"#)?;
        assert!(
            matches!(events.as_slice(), [UserEvent::Order(update)] if update.status == OrderStatus::New && update.cumulative_filled == Decimal::ZERO)
        );
        Ok(())
    }

    #[test]
    fn positions_are_deltas_and_signed_short_quantity_is_preserved() -> TestResult {
        let adapter = BinanceExecution::default();
        let events = adapter.parse_user_message(br#"{"e":"ACCOUNT_UPDATE","E":1750515742303,"a":{"P":[{"s":"BTCUSDT","pa":"-0.123","ps":"BOTH"}]}}"#)?;
        assert!(
            matches!(events.as_slice(), [UserEvent::Position(position)] if position.signed_quantity == Decimal::new(-123, 3))
        );
        let expired =
            adapter.parse_user_message(br#"{"e":"listenKeyExpired","E":1750515742303}"#)?;
        assert!(matches!(
            expired.as_slice(),
            [UserEvent::ReconciliationRequired]
        ));
        Ok(())
    }

    #[test]
    fn listen_key_cannot_modify_url_and_demo_uses_current_host() -> TestResult {
        let adapter = BinanceExecution::default();
        assert_eq!(
            adapter.websocket_url("abc_123", false)?,
            "wss://fstream.binance.com/private/ws/abc_123"
        );
        assert_eq!(
            adapter.websocket_url("abc_123", true)?,
            "wss://demo-fstream.binance.com/ws/abc_123"
        );
        assert!(adapter
            .websocket_url("abc?redirect=example", false)
            .is_err());
        assert_eq!(
            adapter.parse_listen_key(br#"{"listenKey":"abc_123"}"#)?,
            "abc_123"
        );
        assert!(adapter
            .parse_listen_key(br#"{"code":-1125,"msg":"expired"}"#)
            .is_err());
        Ok(())
    }
}
