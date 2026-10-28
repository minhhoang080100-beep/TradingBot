//! Bybit V5 linear-perpetual, one-way execution protocol.
//!
//! REST acceptance is asynchronous: only private order updates or an explicit
//! reconciliation establish a fill or cancellation. Protective orders are
//! separate reduce-only conditional market orders, installed after entry fills.

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use reqwest::Method;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{
    hmac_sha256_hex, ApiCredentials, ExecutionAdapter, ExecutionError, OrderAck, OrderCommand,
    OrderKind, OrderRef, OrderRequest, OrderSide, OrderStatus, OrderUpdate, PositionUpdate,
    SignedRequest, UserEvent,
};
use crate::models::{Exchange, InstrumentId, MarketKind};

const RECEIVE_WINDOW: &str = "5000";

/// Stateless Bybit V5 adapter for linear perpetuals in one-way position mode.
#[derive(Clone, Copy, Debug, Default)]
pub struct BybitExecution;

// Keep the instance API consistent with other execution adapters.
#[allow(clippy::unused_self)]
impl BybitExecution {
    /// Private WebSocket endpoint for a production or isolated demo account.
    #[must_use]
    pub const fn websocket_url(demo: bool) -> &'static str {
        if demo {
            "wss://stream-demo.bybit.com/v5/private"
        } else {
            "wss://stream.bybit.com/v5/private"
        }
    }

    /// Creates the private-stream authentication frame, valid for five seconds.
    ///
    /// This string contains credentials and must never be logged.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid timestamp or failed signature.
    pub fn login_message(
        &self,
        credentials: &ApiCredentials,
        at: DateTime<Utc>,
    ) -> Result<String, ExecutionError> {
        let expires = at
            .checked_add_signed(Duration::seconds(5))
            .ok_or(ExecutionError::Timestamp)?
            .timestamp_millis();
        if at.timestamp_millis() < 0 {
            return Err(ExecutionError::Timestamp);
        }
        let signature = hmac_sha256_hex(
            credentials.secret(),
            format!("GET/realtime{expires}").as_bytes(),
        )?;
        Ok(json!({"op": "auth", "args": [credentials.api_key(), expires, signature]}).to_string())
    }

    /// Creates the private order and position subscription frame.
    ///
    /// Send this only after receiving a successful authentication event.
    #[must_use]
    pub fn subscription_message(&self) -> String {
        json!({"op": "subscribe", "args": ["order.linear", "position.linear"]}).to_string()
    }

    /// Parses a reconciliation response, retaining cumulative fills.
    ///
    /// # Errors
    ///
    /// Returns an error for a venue rejection, absent order, mismatching identity,
    /// unsupported account mode, or malformed data.
    pub fn parse_query_order(
        &self,
        reference: &OrderRef,
        status: u16,
        body: &[u8],
    ) -> Result<OrderUpdate, ExecutionError> {
        validate_reference(reference)?;
        let response = successful_response(status, body)?;
        if required_string(&response.result, "category")? != "linear" {
            return Err(ExecutionError::Protocol("Bybit query category mismatch"));
        }
        let rows = response
            .result
            .get("list")
            .and_then(Value::as_array)
            .ok_or(ExecutionError::Protocol("Bybit query omitted order list"))?;
        if rows.len() != 1 {
            return Err(ExecutionError::AmbiguousResponse);
        }
        let row = rows.first().ok_or(ExecutionError::AmbiguousResponse)?;
        let update = parse_order_fields(row)?;
        if update.client_order_id != reference.client_order_id
            || update.symbol != reference.instrument.venue_symbol.as_str()
            || reference
                .venue_order_id
                .as_ref()
                .is_some_and(|id| id != &update.venue_order_id)
        {
            return Err(ExecutionError::Protocol("Bybit query identity mismatch"));
        }
        Ok(update)
    }
}

impl ExecutionAdapter for BybitExecution {
    fn exchange(&self) -> Exchange {
        Exchange::Bybit
    }

    fn prepare(
        &self,
        command: &OrderCommand,
        credentials: &ApiCredentials,
        at: DateTime<Utc>,
    ) -> Result<SignedRequest, ExecutionError> {
        let (method, path, query, body) = match command {
            OrderCommand::Place(order) => (
                Method::POST,
                "/v5/order/create",
                String::new(),
                serde_json::to_vec(&order_body(order)?)?,
            ),
            OrderCommand::Cancel(reference) => {
                validate_reference(reference)?;
                (
                    Method::POST,
                    "/v5/order/cancel",
                    String::new(),
                    serde_json::to_vec(&json!({
                        "category": "linear",
                        "symbol": reference.instrument.venue_symbol.as_str(),
                        "orderLinkId": reference.client_order_id,
                    }))?,
                )
            }
            OrderCommand::Query(reference) => {
                validate_reference(reference)?;
                let query = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("category", "linear")
                    .append_pair("symbol", reference.instrument.venue_symbol.as_str())
                    .append_pair("orderLinkId", &reference.client_order_id)
                    .finish();
                (Method::GET, "/v5/order/realtime", query, Vec::new())
            }
        };
        let timestamp = at.timestamp_millis();
        if timestamp < 0 {
            return Err(ExecutionError::Timestamp);
        }
        let mut preimage =
            format!("{timestamp}{}{RECEIVE_WINDOW}", credentials.api_key()).into_bytes();
        preimage.extend_from_slice(if method == Method::GET {
            query.as_bytes()
        } else {
            &body
        });
        let signature = hmac_sha256_hex(credentials.secret(), &preimage)?;
        SignedRequest::new(
            method,
            path.to_owned(),
            query,
            Bytes::from(body),
            vec![
                (
                    "X-BAPI-API-KEY".to_owned(),
                    credentials.api_key().to_owned(),
                ),
                ("X-BAPI-TIMESTAMP".to_owned(), timestamp.to_string()),
                ("X-BAPI-RECV-WINDOW".to_owned(), RECEIVE_WINDOW.to_owned()),
                ("X-BAPI-SIGN".to_owned(), signature),
                ("Content-Type".to_owned(), "application/json".to_owned()),
            ],
        )
    }

    fn parse_response(
        &self,
        command: &OrderCommand,
        status: u16,
        body: &[u8],
    ) -> Result<OrderAck, ExecutionError> {
        if let OrderCommand::Query(reference) = command {
            let order = self.parse_query_order(reference, status, body)?;
            return Ok(OrderAck {
                client_order_id: order.client_order_id,
                venue_order_id: Some(order.venue_order_id),
                status: Some(order.status),
            });
        }
        let expected_id = match command {
            OrderCommand::Place(order) => &order.client_order_id,
            OrderCommand::Cancel(reference) => &reference.client_order_id,
            OrderCommand::Query(_) => return Err(ExecutionError::AmbiguousResponse),
        };
        let response = successful_response(status, body)?;
        let client_order_id = required_string(&response.result, "orderLinkId")?;
        let venue_order_id = required_string(&response.result, "orderId")?;
        let mismatched_venue_id = matches!(
            command,
            OrderCommand::Cancel(reference)
                if reference.venue_order_id.as_ref().is_some_and(|id| id != venue_order_id)
        );
        if client_order_id != expected_id || venue_order_id.is_empty() || mismatched_venue_id {
            return Err(ExecutionError::Protocol(
                "Bybit acknowledgement identity mismatch",
            ));
        }
        Ok(OrderAck {
            client_order_id: client_order_id.to_owned(),
            venue_order_id: Some(venue_order_id.to_owned()),
            // The REST acknowledgement does not establish an order state.
            status: None,
        })
    }

    fn parse_user_message(&self, payload: &[u8]) -> Result<Vec<UserEvent>, ExecutionError> {
        let message: Value = serde_json::from_slice(payload)?;
        if let Some(operation) = message.get("op").and_then(Value::as_str) {
            return parse_control(&message, operation).map(|event| vec![event]);
        }
        let topic = required_string(&message, "topic")?;
        if !matches!(
            topic,
            "order" | "order.linear" | "position" | "position.linear"
        ) {
            return Ok(vec![UserEvent::Ignored]);
        }
        let rows = message
            .get("data")
            .and_then(Value::as_array)
            .ok_or(ExecutionError::Protocol("Bybit private event omitted data"))?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            if topic.starts_with("order") {
                events.push(UserEvent::Order(parse_order(row)?));
            } else {
                events.push(UserEvent::Position(parse_position(row)?));
            }
        }
        Ok(events)
    }
}

fn order_body(order: &OrderRequest) -> Result<Value, ExecutionError> {
    order.validate()?;
    validate_instrument(&order.instrument)?;
    validate_client_id(&order.client_order_id)?;
    let side = match order.side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    };
    let mut body = json!({
        "category": "linear",
        "symbol": order.instrument.venue_symbol.as_str(),
        "side": side,
        "orderType": if order.kind == OrderKind::Limit { "Limit" } else { "Market" },
        "qty": order.quantity.normalize().to_string(),
        "orderLinkId": order.client_order_id,
        "positionIdx": 0,
        "reduceOnly": order.reduce_only,
    });
    if let Some(price) = order.price {
        body["price"] = json!(price.normalize().to_string());
    }
    if order.kind == OrderKind::Limit {
        body["timeInForce"] = json!("GTC");
    }
    if matches!(
        order.kind,
        OrderKind::StopMarket | OrderKind::TakeProfitMarket
    ) {
        if !order.reduce_only {
            return Err(ExecutionError::InvalidRequest(
                "Bybit protective order must reduce position",
            ));
        }
        let trigger = order.trigger_price.ok_or(ExecutionError::InvalidRequest(
            "Bybit conditional order requires trigger",
        ))?;
        let rising = matches!(
            (order.kind, order.side),
            (OrderKind::StopMarket, OrderSide::Buy)
                | (OrderKind::TakeProfitMarket, OrderSide::Sell)
        );
        body["triggerPrice"] = json!(trigger.normalize().to_string());
        body["triggerDirection"] = json!(if rising { 1 } else { 2 });
        body["triggerBy"] = json!("MarkPrice");
        body["closeOnTrigger"] = json!(true);
    }
    Ok(body)
}

fn validate_instrument(instrument: &InstrumentId) -> Result<(), ExecutionError> {
    if instrument.exchange != Exchange::Bybit || instrument.market != MarketKind::LinearPerpetual {
        return Err(ExecutionError::InvalidRequest(
            "Bybit supports linear perpetuals only",
        ));
    }
    let symbol = instrument.venue_symbol.as_str();
    if symbol.is_empty()
        || !symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(ExecutionError::InvalidRequest(
            "Bybit symbol must be uppercase alphanumeric",
        ));
    }
    Ok(())
}

fn validate_client_id(id: &str) -> Result<(), ExecutionError> {
    if id.is_empty()
        || id.len() > 36
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ExecutionError::InvalidRequest(
            "Bybit client order identifier is invalid",
        ));
    }
    Ok(())
}

fn validate_reference(reference: &OrderRef) -> Result<(), ExecutionError> {
    validate_instrument(&reference.instrument)?;
    validate_client_id(&reference.client_order_id)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    ret_code: i64,
    #[serde(default)]
    result: Value,
}

fn successful_response(status: u16, body: &[u8]) -> Result<Response, ExecutionError> {
    if !(200..300).contains(&status) {
        return Err(ExecutionError::HttpStatus(status));
    }
    let response: Response = serde_json::from_slice(body)?;
    if matches!(response.ret_code, 10_000 | 10_014 | 10_016 | 110_072) {
        return Err(ExecutionError::AmbiguousResponse);
    }
    if response.ret_code != 0 {
        return Err(ExecutionError::VenueRejected {
            code: response.ret_code.to_string(),
        });
    }
    Ok(response)
}

fn parse_control(message: &Value, operation: &str) -> Result<UserEvent, ExecutionError> {
    if operation == "pong" {
        return Ok(UserEvent::Heartbeat);
    }
    if matches!(operation, "auth" | "subscribe" | "ping") {
        if message.get("success").and_then(Value::as_bool) != Some(true) {
            return Err(ExecutionError::Protocol("Bybit private operation failed"));
        }
        return Ok(match operation {
            "auth" => UserEvent::Authenticated,
            "subscribe" => UserEvent::Subscribed,
            _ => UserEvent::Heartbeat,
        });
    }
    Ok(UserEvent::Ignored)
}

fn validate_row(row: &Value) -> Result<(), ExecutionError> {
    if required_string(row, "category")? != "linear" {
        return Err(ExecutionError::Protocol(
            "Bybit private data requires linear one-way mode",
        ));
    }
    validate_position_mode(row)
}

fn validate_position_mode(row: &Value) -> Result<(), ExecutionError> {
    if row.get("positionIdx").and_then(Value::as_u64) != Some(0) {
        return Err(ExecutionError::Protocol(
            "Bybit private data requires linear one-way mode",
        ));
    }
    if required_string(row, "symbol")?.is_empty() {
        return Err(ExecutionError::Protocol(
            "Bybit private data omitted symbol",
        ));
    }
    Ok(())
}

fn parse_order(row: &Value) -> Result<OrderUpdate, ExecutionError> {
    validate_row(row)?;
    parse_order_fields(row)
}

fn parse_order_fields(row: &Value) -> Result<OrderUpdate, ExecutionError> {
    validate_position_mode(row)?;
    let status = match required_string(row, "orderStatus")? {
        "New" | "Untriggered" => OrderStatus::New,
        "PartiallyFilled" => OrderStatus::PartiallyFilled,
        "Filled" => OrderStatus::Filled,
        "Cancelled" | "PartiallyFilledCanceled" | "Deactivated" => OrderStatus::Cancelled,
        "Rejected" => OrderStatus::Rejected,
        "Triggered" => OrderStatus::Triggered,
        _ => {
            return Err(ExecutionError::Protocol(
                "Bybit order status is unsupported",
            ))
        }
    };
    let average_price = match required_string(row, "avgPrice")? {
        "" => None,
        value => {
            let price = decimal(value)?;
            if price.is_zero() {
                None
            } else {
                Some(price)
            }
        }
    };
    let order = OrderUpdate {
        exchange: Exchange::Bybit,
        symbol: required_string(row, "symbol")?.to_owned(),
        client_order_id: required_string(row, "orderLinkId")?.to_owned(),
        venue_order_id: required_string(row, "orderId")?.to_owned(),
        status,
        quantity: decimal(required_string(row, "qty")?)?,
        cumulative_filled: decimal(required_string(row, "cumExecQty")?)?,
        average_price,
        at: timestamp(required_string(row, "updatedTime")?)?,
    };
    order.validate()?;
    Ok(order)
}

fn parse_position(row: &Value) -> Result<PositionUpdate, ExecutionError> {
    validate_row(row)?;
    let size = decimal(required_string(row, "size")?)?;
    if size < Decimal::ZERO {
        return Err(ExecutionError::Protocol("Bybit position size is negative"));
    }
    let signed_quantity = match required_string(row, "side")? {
        "Buy" => size,
        "Sell" => -size,
        "" if size.is_zero() => Decimal::ZERO,
        _ => return Err(ExecutionError::Protocol("Bybit position side is invalid")),
    };
    Ok(PositionUpdate {
        exchange: Exchange::Bybit,
        symbol: required_string(row, "symbol")?.to_owned(),
        signed_quantity,
        at: timestamp(required_string(row, "updatedTime")?)?,
    })
}

fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ExecutionError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(ExecutionError::Protocol("Bybit required string is absent"))
}

fn decimal(value: &str) -> Result<Decimal, ExecutionError> {
    Decimal::from_str_exact(value)
        .map_err(|_| ExecutionError::Protocol("Bybit decimal string is invalid"))
}

fn timestamp(value: &str) -> Result<DateTime<Utc>, ExecutionError> {
    let millis = value
        .parse::<i64>()
        .map_err(|_| ExecutionError::Timestamp)?;
    if millis < 0 {
        return Err(ExecutionError::Timestamp);
    }
    DateTime::from_timestamp_millis(millis).ok_or(ExecutionError::Timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument() -> Result<InstrumentId, Box<dyn std::error::Error>> {
        Ok(InstrumentId::new(
            Exchange::Bybit,
            MarketKind::LinearPerpetual,
            "BTCUSDT",
            "BTC-USDT-PERP",
        )?)
    }

    fn order() -> Result<OrderRequest, Box<dyn std::error::Error>> {
        Ok(OrderRequest {
            instrument: instrument()?,
            client_order_id: "ictentry001".to_owned(),
            side: OrderSide::Buy,
            kind: OrderKind::Limit,
            quantity: Decimal::new(1, 3),
            price: Some(Decimal::new(60_001, 1)),
            trigger_price: None,
            reduce_only: false,
        })
    }

    fn order_row() -> Value {
        json!({
            "category": "linear", "symbol": "BTCUSDT", "positionIdx": 0,
            "orderId": "venue-001", "orderLinkId": "ictentry001", "orderStatus": "Filled",
            "qty": "0.001", "cumExecQty": "0.001", "avgPrice": "6000.1",
            "updatedTime": "1730000000000"
        })
    }

    #[test]
    fn authentication_uses_exact_rest_bytes_and_websocket_preimage(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let credentials =
            ApiCredentials::new("test-key".to_owned(), "test-secret".to_owned(), None)?;
        let at = timestamp("1730000000000")?;
        let request = order()?;
        let signed =
            BybitExecution.prepare(&OrderCommand::Place(request.clone()), &credentials, at)?;
        assert_eq!(signed.method(), Method::POST);
        assert_eq!(signed.path(), "/v5/order/create");
        assert_eq!(signed.query(), "");
        assert_eq!(
            std::str::from_utf8(signed.body())?,
            r#"{"category":"linear","orderLinkId":"ictentry001","orderType":"Limit","positionIdx":0,"price":"6000.1","qty":"0.001","reduceOnly":false,"side":"Buy","symbol":"BTCUSDT","timeInForce":"GTC"}"#
        );
        let signature = signed
            .headers()
            .iter()
            .find(|(name, _)| name == "X-BAPI-SIGN");
        assert_eq!(
            signature.map(|(_, value)| value.as_str()),
            Some("d9dc013d5b68cd7bbd0dc67905cdc48ffc25c7ec3b8570c886d8a02deaac26b0")
        );
        let query = BybitExecution.prepare(
            &OrderCommand::Query(request.reference(None)),
            &credentials,
            at,
        )?;
        assert_eq!(query.method(), Method::GET);
        assert!(query.body().is_empty());
        assert_eq!(
            query.query(),
            "category=linear&symbol=BTCUSDT&orderLinkId=ictentry001"
        );
        let signature = query
            .headers()
            .iter()
            .find(|(name, _)| name == "X-BAPI-SIGN");
        assert_eq!(
            signature.map(|(_, value)| value.as_str()),
            Some("9a6c9e89b65920100c409c90e3daafed42a5904171c14e757eeb5631656b411f")
        );
        let login: Value = serde_json::from_str(&BybitExecution.login_message(&credentials, at)?)?;
        assert_eq!(login["args"][1], 1_730_000_005_000_i64);
        assert_eq!(
            login["args"][2],
            "05ec0a37ce143b6160c1069bcaa1a2165cd532b16eaa56ff1de1207a149dcdd7"
        );
        Ok(())
    }

    #[test]
    fn limit_order_preserves_decimal_strings() -> Result<(), Box<dyn std::error::Error>> {
        let body = order_body(&order()?)?;
        assert_eq!(body["qty"], "0.001");
        assert_eq!(body["price"], "6000.1");
        assert_eq!(body["orderType"], "Limit");
        assert_eq!(body["positionIdx"], 0);
        assert_eq!(body["timeInForce"], "GTC");
        assert_eq!(body["reduceOnly"], false);
        Ok(())
    }

    #[test]
    fn protective_triggers_cover_both_sides() -> Result<(), Box<dyn std::error::Error>> {
        for (kind, side, trigger_direction) in [
            (OrderKind::StopMarket, OrderSide::Sell, 2),
            (OrderKind::StopMarket, OrderSide::Buy, 1),
            (OrderKind::TakeProfitMarket, OrderSide::Sell, 1),
            (OrderKind::TakeProfitMarket, OrderSide::Buy, 2),
        ] {
            let mut request = order()?;
            request.kind = kind;
            request.side = side;
            request.price = None;
            request.trigger_price = Some(Decimal::from(6000));
            request.reduce_only = true;
            let body = order_body(&request)?;
            assert_eq!(body["triggerDirection"], trigger_direction);
            assert_eq!(body["orderType"], "Market");
            assert_eq!(body["reduceOnly"], true);
            assert_eq!(body["closeOnTrigger"], true);
            assert_eq!(body["triggerBy"], "MarkPrice");
            assert!(body.get("takeProfit").is_none());
            assert!(body.get("stopLoss").is_none());
            assert!(body.get("price").is_none());
            request.reduce_only = false;
            assert!(order_body(&request).is_err());
        }
        Ok(())
    }

    #[test]
    fn acknowledgement_does_not_claim_fill_or_cancellation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let request = order()?;
        let response =
            br#"{"retCode":0,"result":{"orderId":"venue-001","orderLinkId":"ictentry001"}}"#;
        let ack =
            BybitExecution.parse_response(&OrderCommand::Place(request.clone()), 200, response)?;
        assert_eq!(ack.status, None);
        let reference = OrderRef {
            instrument: request.instrument,
            client_order_id: request.client_order_id,
            kind: request.kind,
            venue_order_id: None,
        };
        let ack = BybitExecution.parse_response(&OrderCommand::Cancel(reference), 200, response)?;
        assert_eq!(ack.status, None);
        Ok(())
    }

    #[test]
    fn reject_and_mismatched_ack_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let command = OrderCommand::Place(order()?);
        for code in [10_000, 10_014, 10_016, 110_072] {
            let body = serde_json::to_vec(&json!({"retCode": code}))?;
            assert!(matches!(
                BybitExecution.parse_response(&command, 200, &body),
                Err(ExecutionError::AmbiguousResponse)
            ));
        }
        assert!(matches!(
            BybitExecution.parse_response(&command, 200, br#"{"retCode":10006,"retMsg":"rate limited"}"#),
            Err(ExecutionError::VenueRejected { code }) if code == "10006"
        ));
        assert!(BybitExecution
            .parse_response(
                &command,
                200,
                br#"{"retCode":0,"result":{"orderId":"venue-001","orderLinkId":"different"}}"#
            )
            .is_err());
        assert!(matches!(
            BybitExecution.parse_response(&command, 503, b"unavailable"),
            Err(ExecutionError::HttpStatus(503))
        ));
        Ok(())
    }

    #[test]
    fn cumulative_fills_survive_cancel_race_and_duplicates(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let row = order_row();
        let frame = json!({"topic": "order.linear", "data": [row.clone(), row.clone()]});
        let events = BybitExecution.parse_user_message(&serde_json::to_vec(&frame)?)?;
        assert_eq!(events.len(), 2);
        for event in events {
            let UserEvent::Order(update) = event else {
                return Err("expected order event".into());
            };
            assert_eq!(update.status, OrderStatus::Filled);
            assert_eq!(update.cumulative_filled, Decimal::new(1, 3));
        }
        let mut cancelled = row;
        cancelled["orderStatus"] = json!("Cancelled");
        cancelled["cumExecQty"] = json!("0.0005");
        let update = parse_order(&cancelled)?;
        assert_eq!(update.status, OrderStatus::Cancelled);
        assert_eq!(update.cumulative_filled, Decimal::new(5, 4));
        Ok(())
    }

    #[test]
    fn position_direction_and_flat_are_normalized() -> Result<(), Box<dyn std::error::Error>> {
        for (side, quantity, expected) in [
            ("Buy", "0.125", Decimal::new(125, 3)),
            ("Sell", "0.125", Decimal::new(-125, 3)),
            ("", "0", Decimal::ZERO),
        ] {
            let row = json!({
                "category": "linear", "symbol": "BTCUSDT", "positionIdx": 0,
                "side": side, "size": quantity, "updatedTime": "1730000000000"
            });
            assert_eq!(parse_position(&row)?.signed_quantity, expected);
        }
        Ok(())
    }

    #[test]
    fn invalid_wire_data_and_hedge_mode_fail_closed() {
        let mut row = order_row();
        row["positionIdx"] = json!(1);
        assert!(parse_order(&row).is_err());
        row["positionIdx"] = json!(0);
        row["qty"] = json!(1);
        assert!(parse_order(&row).is_err());
        row["qty"] = json!("-1");
        assert!(parse_order(&row).is_err());
        row["qty"] = json!("0.001");
        row["cumExecQty"] = json!("0.002");
        assert!(parse_order(&row).is_err());
        row["cumExecQty"] = json!("0.001");
        row["orderStatus"] = json!("UnknownFutureStatus");
        assert!(parse_order(&row).is_err());
    }

    #[test]
    fn auth_and_subscription_acknowledgements_require_success(
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            BybitExecution
                .parse_user_message(br#"{"op":"auth","success":true}"#)?
                .as_slice(),
            [UserEvent::Authenticated]
        ));
        assert!(BybitExecution
            .parse_user_message(br#"{"op":"auth","success":false}"#)
            .is_err());
        assert!(BybitExecution
            .parse_user_message(br#"{"op":"subscribe"}"#)
            .is_err());
        assert!(matches!(
            BybitExecution
                .parse_user_message(br#"{"op":"pong","args":["1730000000000"]}"#)?
                .as_slice(),
            [UserEvent::Heartbeat]
        ));
        Ok(())
    }

    #[test]
    fn query_identity_is_checked_before_reconciliation() -> Result<(), Box<dyn std::error::Error>> {
        let reference = OrderRef {
            instrument: instrument()?,
            client_order_id: "ictentry001".to_owned(),
            venue_order_id: Some("venue-001".to_owned()),
            kind: OrderKind::Limit,
        };
        let mut row = order_row();
        row.as_object_mut()
            .ok_or("expected object")?
            .remove("category");
        let body =
            serde_json::to_vec(&json!({"retCode":0,"result":{"category":"linear","list":[row]}}))?;
        let update = BybitExecution.parse_query_order(&reference, 200, &body)?;
        assert_eq!(update.cumulative_filled, Decimal::new(1, 3));
        let empty = br#"{"retCode":0,"result":{"category":"linear","list":[]}}"#;
        assert!(matches!(
            BybitExecution.parse_query_order(&reference, 200, empty),
            Err(ExecutionError::AmbiguousResponse)
        ));
        Ok(())
    }
}
