//! OKX V5 execution protocol for cross-margin, net-mode linear swaps.
//!
//! The adapter builds signed requests but never submits them. Quantities are
//! contracts, not base-asset amounts; the risk instrument multiplier must match
//! OKX `ctVal`. Conditional exits are separate reduce-only orders: combining TP
//! and SL in one `conditional` request would cause OKX to ignore the TP.
//!
//! Ordinary orders and positions use the private WebSocket. Algo lifecycle
//! updates require a second authenticated connection to the business WebSocket.
//! See <https://www.okx.com/docs-v5/en/> for the protocol specification.

use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use reqwest::Method;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::models::{Exchange, InstrumentId, MarketKind};

use super::{
    hmac_sha256_base64, ApiCredentials, ExecutionAdapter, ExecutionError, OrderAck, OrderCommand,
    OrderKind, OrderRef, OrderRequest, OrderSide, OrderStatus, OrderUpdate, PositionUpdate,
    SignedRequest, UserEvent,
};

/// Global REST origin. Regional accounts must use their documented regional origin.
pub const REST_ENDPOINT: &str = "https://openapi.okx.com";
/// Production authenticated ordinary-order and position stream.
pub const PRIVATE_ENDPOINT: &str = "wss://ws.okx.com/ws/v5/private";
/// Demo authenticated ordinary-order and position stream.
pub const DEMO_PRIVATE_ENDPOINT: &str = "wss://wspap.okx.com/ws/v5/private";
/// Production authenticated pending-algo lifecycle stream.
pub const BUSINESS_ENDPOINT: &str = "wss://ws.okx.com/ws/v5/business";
/// Demo authenticated pending-algo lifecycle stream.
pub const DEMO_BUSINESS_ENDPOINT: &str = "wss://wspap.okx.com/ws/v5/business";

/// Stateless OKX execution adapter for an already-configured cross/net account.
#[derive(Clone, Copy, Debug)]
pub struct OkxExecution {
    demo: bool,
}

// Instance helpers keep authentication and parsing uniform across adapters.
#[allow(clippy::unused_self)]
impl OkxExecution {
    /// Selects whether signed REST requests carry the demo-trading header.
    #[must_use]
    pub const fn new(demo: bool) -> Self {
        Self { demo }
    }

    /// Builds authentication for either the private or business WebSocket.
    ///
    /// The result contains credentials and must never be logged. Send a fresh
    /// login after each reconnect; the exchange expires it after 30 seconds.
    ///
    /// # Errors
    /// Returns an error for a missing passphrase, pre-epoch time or signing failure.
    pub fn login_message(
        &self,
        credentials: &ApiCredentials,
        at: DateTime<Utc>,
    ) -> Result<String, ExecutionError> {
        let timestamp = timestamp_seconds(at)?;
        let passphrase = passphrase(credentials)?;
        let signature = hmac_sha256_base64(
            credentials.secret(),
            format!("{timestamp}GET/users/self/verify").as_bytes(),
        )?;
        Ok(json!({
            "op": "login",
            "args": [{
                "apiKey": credentials.api_key(),
                "passphrase": passphrase,
                "timestamp": timestamp,
                "sign": signature,
            }],
        })
        .to_string())
    }

    /// Subscribes to ordinary orders and positions on the private connection.
    ///
    /// Send only after a successful login. Orders have no initial snapshot;
    /// reconnecting must therefore be followed by REST reconciliation.
    #[must_use]
    pub fn subscription_message(&self) -> String {
        json!({"op":"subscribe","args":[
            {"channel":"orders","instType":"SWAP"},
            {"channel":"positions","instType":"SWAP"},
        ]})
        .to_string()
    }

    /// Subscribes to pending conditional orders on the business connection.
    ///
    /// This connection requires its own successful login. An `effective` algo
    /// update means triggered, not filled; ordinary child fills arrive separately.
    #[must_use]
    pub fn algo_subscription_message(&self) -> String {
        json!({"op":"subscribe","args":[
            {"channel":"orders-algo","instType":"SWAP"},
        ]})
        .to_string()
    }

    /// Parses a REST reconciliation snapshot with cumulative execution evidence.
    ///
    /// Conditional snapshots retain their algo lifecycle only. A triggered algo
    /// must additionally be reconciled with its matching-engine child and net
    /// position; `actualSz` is never treated as proven executed quantity.
    ///
    /// # Errors
    /// Returns an error for an uncorrelated snapshot, invalid account mode,
    /// unknown venue state, inexact financial number or ambiguous response.
    pub fn parse_query_order(
        &self,
        reference: &OrderRef,
        status: u16,
        body: &[u8],
    ) -> Result<OrderUpdate, ExecutionError> {
        validate_reference(reference)?;
        self.parse_response(&OrderCommand::Query(reference.clone()), status, body)?;
        let response: RestResponse = serde_json::from_slice(body)?;
        let mut rows = response.data.into_iter();
        let row = rows.next().ok_or(ExecutionError::AmbiguousResponse)?;
        let update = parse_order_update(row, is_algo(reference.kind))?;
        if update.client_order_id != reference.client_order_id
            || update.symbol != reference.instrument.venue_symbol.as_str()
            || reference
                .venue_order_id
                .as_ref()
                .is_some_and(|id| id != &update.venue_order_id)
        {
            return Err(ExecutionError::AmbiguousResponse);
        }
        Ok(update)
    }

    fn sign(
        self,
        method: Method,
        path: &str,
        query: String,
        body: Bytes,
        credentials: &ApiCredentials,
        at: DateTime<Utc>,
    ) -> Result<SignedRequest, ExecutionError> {
        timestamp_seconds(at)?;
        let timestamp = at.to_rfc3339_opts(SecondsFormat::Millis, true);
        let request_path = if query.is_empty() {
            path.to_owned()
        } else {
            format!("{path}?{query}")
        };
        let mut prehash = format!("{timestamp}{}{request_path}", method.as_str()).into_bytes();
        prehash.extend_from_slice(&body);
        let signature = hmac_sha256_base64(credentials.secret(), &prehash)?;
        let mut headers = vec![
            ("Content-Type".to_owned(), "application/json".to_owned()),
            ("OK-ACCESS-KEY".to_owned(), credentials.api_key().to_owned()),
            ("OK-ACCESS-SIGN".to_owned(), signature),
            ("OK-ACCESS-TIMESTAMP".to_owned(), timestamp),
            (
                "OK-ACCESS-PASSPHRASE".to_owned(),
                passphrase(credentials)?.to_owned(),
            ),
        ];
        if self.demo {
            headers.push(("x-simulated-trading".to_owned(), "1".to_owned()));
        }
        SignedRequest::new(method, path.to_owned(), query, body, headers)
    }
}

impl ExecutionAdapter for OkxExecution {
    fn exchange(&self) -> Exchange {
        Exchange::Okx
    }

    fn prepare(
        &self,
        command: &OrderCommand,
        credentials: &ApiCredentials,
        at: DateTime<Utc>,
    ) -> Result<SignedRequest, ExecutionError> {
        let (method, path, query, body) = match command {
            OrderCommand::Place(order) => {
                let (path, value) = place_order(order)?;
                (Method::POST, path, String::new(), json_bytes(&value)?)
            }
            OrderCommand::Cancel(reference) => {
                validate_reference(reference)?;
                let (path, value) = cancel_order(reference)?;
                (Method::POST, path, String::new(), json_bytes(&value)?)
            }
            OrderCommand::Query(reference) => {
                validate_reference(reference)?;
                let (path, query) = query_order(reference);
                (Method::GET, path, query, Bytes::new())
            }
        };
        self.sign(method, path, query, body, credentials, at)
    }

    fn parse_response(
        &self,
        command: &OrderCommand,
        status: u16,
        body: &[u8],
    ) -> Result<OrderAck, ExecutionError> {
        if status == 408 || status >= 500 {
            return Err(ExecutionError::AmbiguousResponse);
        }
        let response: RestResponse = serde_json::from_slice(body)?;
        if response.code != "0" {
            return Err(venue_error(&response.code));
        }
        if !(200..300).contains(&status) {
            return Err(ExecutionError::HttpStatus(status));
        }
        let [row] = response.data.as_slice() else {
            return Err(ExecutionError::AmbiguousResponse);
        };
        if let Some(code) = row.s_code.as_deref().filter(|code| *code != "0") {
            return Err(venue_error(code));
        }
        let (client_id, kind, venue_id, symbol) = command_identity(command);
        let algo = is_algo(kind);
        let returned_id = if algo { &row.algo_id } else { &row.ord_id };
        let returned_client = if algo {
            &row.algo_cl_ord_id
        } else {
            &row.cl_ord_id
        };
        if !is_venue_id(returned_id)
            || (returned_client.is_empty() && venue_id.is_none())
            || (!returned_client.is_empty() && returned_client != client_id)
            || venue_id.is_some_and(|id| id != returned_id)
            || (!row.inst_id.is_empty() && row.inst_id != symbol)
        {
            return Err(ExecutionError::AmbiguousResponse);
        }
        let order_status = if matches!(command, OrderCommand::Query(_)) {
            Some(parse_status(&row.state, algo)?)
        } else {
            match row.s_code.as_deref() {
                Some("0") => None,
                Some(code) => return Err(venue_error(code)),
                None => return Err(ExecutionError::AmbiguousResponse),
            }
        };
        Ok(OrderAck {
            client_order_id: client_id.to_owned(),
            venue_order_id: Some(returned_id.clone()),
            status: order_status,
        })
    }

    fn parse_user_message(&self, payload: &[u8]) -> Result<Vec<UserEvent>, ExecutionError> {
        if payload == b"pong" {
            return Ok(vec![UserEvent::Heartbeat]);
        }
        let message: WsMessage = serde_json::from_slice(payload)?;
        if let Some(event) = message.event.as_deref() {
            return parse_control(event, &message);
        }
        let argument = message
            .arg
            .ok_or(ExecutionError::Protocol("OKX message missing channel"))?;
        let rows = message
            .data
            .ok_or(ExecutionError::Protocol("OKX message missing data"))?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            match argument.channel.as_str() {
                "orders" | "orders-algo" => {
                    let algo = argument.channel == "orders-algo";
                    let update = parse_order_update(row, algo)?;
                    let triggered = update.status == OrderStatus::Triggered;
                    events.push(UserEvent::Order(update));
                    if triggered {
                        events.push(UserEvent::ReconciliationRequired);
                    }
                }
                "positions" => events.push(UserEvent::Position(parse_position_update(row)?)),
                _ => return Ok(vec![UserEvent::Ignored]),
            }
        }
        Ok(events)
    }
}

fn place_order(order: &OrderRequest) -> Result<(&'static str, Value), ExecutionError> {
    order.validate()?;
    validate_instrument(&order.instrument)?;
    validate_client_id(&order.client_order_id)?;
    let mut value = json!({
        "instId": order.instrument.venue_symbol.as_str(),
        "tdMode": "cross",
        "posSide": "net",
        "side": match order.side { OrderSide::Buy => "buy", OrderSide::Sell => "sell" },
        "sz": order.quantity.normalize().to_string(),
        "reduceOnly": order.reduce_only,
    });
    match order.kind {
        OrderKind::Limit => {
            value["ordType"] = json!("limit");
            value["clOrdId"] = json!(order.client_order_id);
            value["px"] = json!(required_price(order.price)?.normalize().to_string());
        }
        OrderKind::Market => {
            value["ordType"] = json!("market");
            value["clOrdId"] = json!(order.client_order_id);
        }
        OrderKind::StopMarket | OrderKind::TakeProfitMarket => {
            if !order.reduce_only {
                return Err(ExecutionError::InvalidRequest(
                    "OKX conditional exits must reduce only",
                ));
            }
            value["ordType"] = json!("conditional");
            value["algoClOrdId"] = json!(order.client_order_id);
            let trigger = required_price(order.trigger_price)?.normalize().to_string();
            if order.kind == OrderKind::StopMarket {
                value["slTriggerPx"] = json!(trigger);
                value["slOrdPx"] = json!("-1");
                value["slTriggerPxType"] = json!("mark");
            } else {
                value["tpTriggerPx"] = json!(trigger);
                value["tpOrdPx"] = json!("-1");
                value["tpTriggerPxType"] = json!("mark");
            }
        }
    }
    let path = if is_algo(order.kind) {
        "/api/v5/trade/order-algo"
    } else {
        "/api/v5/trade/order"
    };
    Ok((path, value))
}

fn cancel_order(reference: &OrderRef) -> Result<(&'static str, Value), ExecutionError> {
    let symbol = reference.instrument.venue_symbol.as_str();
    if is_algo(reference.kind) {
        let algo_id = reference
            .venue_order_id
            .as_deref()
            .ok_or(ExecutionError::InvalidRequest(
                "OKX algo cancellation requires the reconciled venue order ID",
            ))?;
        Ok((
            "/api/v5/trade/cancel-algos",
            json!([{"instId":symbol,"algoId":algo_id}]),
        ))
    } else {
        let mut value = json!({"instId":symbol});
        if let Some(id) = reference.venue_order_id.as_ref() {
            value["ordId"] = json!(id);
        } else {
            value["clOrdId"] = json!(reference.client_order_id);
        }
        Ok(("/api/v5/trade/cancel-order", value))
    }
}

fn query_order(reference: &OrderRef) -> (&'static str, String) {
    let algo = is_algo(reference.kind);
    let mut pairs = url::form_urlencoded::Serializer::new(String::new());
    if !algo {
        pairs.append_pair("instId", reference.instrument.venue_symbol.as_str());
    }
    match reference.venue_order_id.as_deref() {
        Some(id) => {
            pairs.append_pair(if algo { "algoId" } else { "ordId" }, id);
        }
        None => {
            pairs.append_pair(
                if algo { "algoClOrdId" } else { "clOrdId" },
                &reference.client_order_id,
            );
        }
    }
    (
        if algo {
            "/api/v5/trade/order-algo"
        } else {
            "/api/v5/trade/order"
        },
        pairs.finish(),
    )
}

fn command_identity(command: &OrderCommand) -> (&str, OrderKind, Option<&str>, &str) {
    match command {
        OrderCommand::Place(order) => (
            &order.client_order_id,
            order.kind,
            None,
            order.instrument.venue_symbol.as_str(),
        ),
        OrderCommand::Cancel(reference) | OrderCommand::Query(reference) => (
            &reference.client_order_id,
            reference.kind,
            reference.venue_order_id.as_deref(),
            reference.instrument.venue_symbol.as_str(),
        ),
    }
}

fn validate_instrument(instrument: &InstrumentId) -> Result<(), ExecutionError> {
    if instrument.exchange != Exchange::Okx || instrument.market != MarketKind::LinearPerpetual {
        return Err(ExecutionError::InvalidRequest(
            "OKX adapter only accepts linear swaps",
        ));
    }
    validate_symbol(instrument.venue_symbol.as_str())
}

fn validate_symbol(symbol: &str) -> Result<(), ExecutionError> {
    if !symbol.ends_with("-SWAP")
        || !symbol
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(ExecutionError::InvalidRequest(
            "OKX swap instrument identifier is invalid",
        ));
    }
    Ok(())
}

fn validate_reference(reference: &OrderRef) -> Result<(), ExecutionError> {
    validate_instrument(&reference.instrument)?;
    validate_client_id(&reference.client_order_id)?;
    if reference
        .venue_order_id
        .as_ref()
        .is_some_and(|id| !is_venue_id(id))
    {
        return Err(ExecutionError::InvalidRequest(
            "OKX venue order ID must be numeric",
        ));
    }
    Ok(())
}

fn is_venue_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 32
        && id.bytes().all(|byte| byte.is_ascii_digit())
        && id.bytes().any(|byte| byte != b'0')
}

fn validate_client_id(id: &str) -> Result<(), ExecutionError> {
    if id.is_empty() || id.len() > 32 || !id.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(ExecutionError::InvalidRequest(
            "OKX client order ID must be 1-32 alphanumerics",
        ));
    }
    Ok(())
}

fn passphrase(credentials: &ApiCredentials) -> Result<&str, ExecutionError> {
    credentials
        .passphrase()
        .filter(|value| !value.is_empty())
        .ok_or(ExecutionError::InvalidRequest(
            "OKX requires an API passphrase",
        ))
}

fn required_price(price: Option<Decimal>) -> Result<Decimal, ExecutionError> {
    price
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(ExecutionError::InvalidRequest(
            "OKX order requires a positive price",
        ))
}

fn timestamp_seconds(at: DateTime<Utc>) -> Result<String, ExecutionError> {
    if at.timestamp() < 0 {
        return Err(ExecutionError::Timestamp);
    }
    Ok(at.timestamp().to_string())
}

fn json_bytes(value: &Value) -> Result<Bytes, ExecutionError> {
    Ok(Bytes::from(serde_json::to_vec(value)?))
}

const fn is_algo(kind: OrderKind) -> bool {
    matches!(kind, OrderKind::StopMarket | OrderKind::TakeProfitMarket)
}

fn venue_error(code: &str) -> ExecutionError {
    if matches!(code, "50001" | "50004" | "50013" | "50026") {
        // A backend failure cannot prove that an order was not accepted.
        ExecutionError::AmbiguousResponse
    } else if code.is_empty() || code.len() > 16 || !code.bytes().all(|byte| byte.is_ascii_digit())
    {
        ExecutionError::Protocol("OKX returned an invalid error code")
    } else {
        ExecutionError::VenueRejected {
            code: code.to_owned(),
        }
    }
}

fn parse_status(state: &str, algo: bool) -> Result<OrderStatus, ExecutionError> {
    match (state, algo) {
        ("live", _) => Ok(OrderStatus::New),
        ("partially_filled", false) => Ok(OrderStatus::PartiallyFilled),
        ("filled", false) => Ok(OrderStatus::Filled),
        ("canceled", _) | ("mmp_canceled", false) => Ok(OrderStatus::Cancelled),
        ("effective", true) => Ok(OrderStatus::Triggered),
        ("order_failed", true) => Ok(OrderStatus::Rejected),
        _ => Err(ExecutionError::Protocol(
            "OKX returned an unsupported order state",
        )),
    }
}

fn parse_control(event: &str, message: &WsMessage) -> Result<Vec<UserEvent>, ExecutionError> {
    if let Some(code) = message.code.as_deref().filter(|code| *code != "0") {
        return Err(venue_error(code));
    }
    match event {
        "login" if message.code.as_deref() == Some("0") => Ok(vec![UserEvent::Authenticated]),
        "subscribe" => match message.arg.as_ref().map(|arg| arg.channel.as_str()) {
            Some("orders" | "positions" | "orders-algo") => Ok(vec![UserEvent::Subscribed]),
            _ => Err(ExecutionError::Protocol(
                "OKX acknowledged an unexpected subscription",
            )),
        },
        "error" | "channel-conn-count-error" | "login" => Err(ExecutionError::Protocol(
            "OKX private stream control failed",
        )),
        _ => Ok(vec![UserEvent::Ignored]),
    }
}

fn parse_order_update(row: WireRow, algo: bool) -> Result<OrderUpdate, ExecutionError> {
    validate_wire_instrument(&row)?;
    let status = parse_status(&row.state, algo)?;
    let at = parse_timestamp(&row.u_time)?;
    let quantity = parse_decimal(&row.sz)?;
    let cumulative_filled = if algo {
        Decimal::ZERO
    } else {
        parse_decimal(&row.acc_fill_sz)?
    };
    let average_price = if algo || row.avg_px.is_empty() {
        None
    } else {
        Some(parse_decimal(&row.avg_px)?)
    };
    // A triggered child can have its own nonempty clOrdId. Correlate it to the
    // managed protective leg using the original algoClOrdId whenever present.
    let client_order_id = if algo || !row.algo_cl_ord_id.is_empty() {
        row.algo_cl_ord_id
    } else {
        row.cl_ord_id
    };
    let venue_order_id = if algo { row.algo_id } else { row.ord_id };
    validate_client_id(&client_order_id)?;
    if !is_venue_id(&venue_order_id) {
        return Err(ExecutionError::Protocol(
            "OKX update has an invalid order ID",
        ));
    }
    let update = OrderUpdate {
        exchange: Exchange::Okx,
        symbol: row.inst_id,
        client_order_id,
        venue_order_id,
        status,
        quantity,
        cumulative_filled,
        average_price,
        at,
    };
    update.validate()?;
    Ok(update)
}

fn parse_position_update(row: WireRow) -> Result<PositionUpdate, ExecutionError> {
    validate_wire_instrument(&row)?;
    Ok(PositionUpdate {
        exchange: Exchange::Okx,
        symbol: row.inst_id,
        signed_quantity: parse_decimal(&row.pos)?,
        at: parse_timestamp(&row.u_time)?,
    })
}

fn validate_wire_instrument(row: &WireRow) -> Result<(), ExecutionError> {
    validate_symbol(&row.inst_id)?;
    if row.inst_type.as_deref().is_some_and(|kind| kind != "SWAP") || row.pos_side != "net" {
        return Err(ExecutionError::Protocol(
            "OKX execution stream requires net-mode swaps",
        ));
    }
    Ok(())
}

fn parse_decimal(value: &str) -> Result<Decimal, ExecutionError> {
    Decimal::from_str_exact(value)
        .map_err(|_| ExecutionError::Protocol("OKX numeric field is not an exact decimal"))
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, ExecutionError> {
    let millis = value
        .parse::<i64>()
        .map_err(|_| ExecutionError::Timestamp)?;
    if millis < 0 {
        return Err(ExecutionError::Timestamp);
    }
    DateTime::from_timestamp_millis(millis).ok_or(ExecutionError::Timestamp)
}

#[derive(Deserialize)]
struct RestResponse {
    code: String,
    #[serde(default)]
    data: Vec<WireRow>,
}

#[derive(Deserialize)]
struct WsMessage {
    event: Option<String>,
    code: Option<String>,
    arg: Option<WireArgument>,
    data: Option<Vec<WireRow>>,
}

#[derive(Deserialize)]
struct WireArgument {
    channel: String,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireRow {
    #[serde(default)]
    inst_id: String,
    inst_type: Option<String>,
    #[serde(default)]
    pos_side: String,
    #[serde(default)]
    cl_ord_id: String,
    #[serde(default)]
    algo_cl_ord_id: String,
    #[serde(default)]
    ord_id: String,
    #[serde(default)]
    algo_id: String,
    s_code: Option<String>,
    #[serde(default)]
    state: String,
    #[serde(default)]
    sz: String,
    #[serde(default)]
    acc_fill_sz: String,
    #[serde(default)]
    avg_px: String,
    #[serde(default)]
    pos: String,
    #[serde(default)]
    u_time: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn at() -> Result<DateTime<Utc>, ExecutionError> {
        DateTime::from_timestamp_millis(1_700_000_000_000).ok_or(ExecutionError::Timestamp)
    }

    fn credentials() -> Result<ApiCredentials, ExecutionError> {
        ApiCredentials::new(
            "key".to_owned(),
            "secret".to_owned(),
            Some("pass".to_owned()),
        )
    }

    fn order(kind: OrderKind) -> Result<OrderRequest, Box<dyn std::error::Error>> {
        Ok(OrderRequest {
            instrument: InstrumentId::new(
                Exchange::Okx,
                MarketKind::LinearPerpetual,
                "BTC-USDT-SWAP",
                "BTC-USDT-PERP",
            )?,
            client_order_id: "entry1".to_owned(),
            side: if kind == OrderKind::Limit {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            kind,
            quantity: Decimal::new(25, 1),
            price: (kind == OrderKind::Limit).then_some(Decimal::new(30_000_125, 3)),
            trigger_price: is_algo(kind).then_some(Decimal::from(29_000)),
            reduce_only: kind != OrderKind::Limit,
        })
    }

    fn header<'a>(request: &'a SignedRequest, key: &str) -> Option<&'a str> {
        request
            .headers()
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn query_signature_covers_path_and_encoded_query_using_iso_milliseconds() -> TestResult {
        let reference = order(OrderKind::Limit)?.reference(None);
        let request = OkxExecution::new(true).prepare(
            &OrderCommand::Query(reference),
            &credentials()?,
            at()?,
        )?;
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.path(), "/api/v5/trade/order");
        assert_eq!(request.query(), "instId=BTC-USDT-SWAP&clOrdId=entry1");
        assert!(request.body().is_empty());
        // Independently computed HMAC fixture, not a second call to the signer.
        assert_eq!(
            header(&request, "OK-ACCESS-SIGN"),
            Some("7huUDjjc+cd6GAVqKG09UQ8EVdmMa/o5LHeWJFuf7ZM=")
        );
        assert_eq!(
            header(&request, "OK-ACCESS-TIMESTAMP"),
            Some("2023-11-14T22:13:20.000Z")
        );
        assert_eq!(header(&request, "x-simulated-trading"), Some("1"));
        Ok(())
    }

    #[test]
    fn limit_request_preserves_decimal_contract_quantity_and_exact_body_signature() -> TestResult {
        let request = OkxExecution::new(false).prepare(
            &OrderCommand::Place(order(OrderKind::Limit)?),
            &credentials()?,
            at()?,
        )?;
        let body: Value = serde_json::from_slice(request.body())?;
        assert_eq!(body["sz"], "2.5");
        assert_eq!(body["px"], "30000.125");
        assert_eq!(body["ordType"], "limit");
        assert_eq!(body["posSide"], "net");
        assert_eq!(body["tdMode"], "cross");
        assert_eq!(body["reduceOnly"], false);
        assert!(header(&request, "x-simulated-trading").is_none());
        let mut expected_payload = b"2023-11-14T22:13:20.000ZPOST/api/v5/trade/order".to_vec();
        expected_payload.extend_from_slice(request.body());
        assert_eq!(
            header(&request, "OK-ACCESS-SIGN"),
            Some(hmac_sha256_base64("secret", &expected_payload)?.as_str())
        );
        Ok(())
    }

    #[test]
    fn protective_legs_are_separate_reduce_only_market_conditional_orders() -> TestResult {
        let adapter = OkxExecution::new(true);
        for kind in [OrderKind::StopMarket, OrderKind::TakeProfitMarket] {
            let request =
                adapter.prepare(&OrderCommand::Place(order(kind)?), &credentials()?, at()?)?;
            let body: Value = serde_json::from_slice(request.body())?;
            assert_eq!(request.path(), "/api/v5/trade/order-algo");
            assert_eq!(body["ordType"], "conditional");
            assert_eq!(body["reduceOnly"], true);
            assert_eq!(body["side"], "sell");
            assert_eq!(body["algoClOrdId"], "entry1");
            let (present, absent, market) = if kind == OrderKind::StopMarket {
                ("slTriggerPx", "tpTriggerPx", "slOrdPx")
            } else {
                ("tpTriggerPx", "slTriggerPx", "tpOrdPx")
            };
            assert_eq!(body[present], "29000");
            assert!(body.get(absent).is_none());
            assert_eq!(body[market], "-1");
        }
        Ok(())
    }

    #[test]
    fn algo_cancel_requires_exchange_id_and_is_an_array() -> TestResult {
        let adapter = OkxExecution::new(true);
        let order = order(OrderKind::StopMarket)?;
        assert!(adapter
            .prepare(
                &OrderCommand::Cancel(order.reference(None)),
                &credentials()?,
                at()?
            )
            .is_err());
        let request = adapter.prepare(
            &OrderCommand::Cancel(order.reference(Some("12345".to_owned()))),
            &credentials()?,
            at()?,
        )?;
        assert_eq!(request.path(), "/api/v5/trade/cancel-algos");
        assert_eq!(
            serde_json::from_slice::<Value>(request.body())?,
            json!([{"instId":"BTC-USDT-SWAP","algoId":"12345"}])
        );
        Ok(())
    }

    #[test]
    fn rest_acceptance_does_not_prove_cancellation_or_execution() -> TestResult {
        let adapter = OkxExecution::new(true);
        let command = OrderCommand::Cancel(order(OrderKind::Limit)?.reference(None));
        let response = br#"{"code":"0","data":[{"clOrdId":"entry1","ordId":"123","sCode":"0"}]}"#;
        let ack = adapter.parse_response(&command, 200, response)?;
        assert_eq!(ack.status, None);
        assert_eq!(ack.venue_order_id.as_deref(), Some("123"));
        let rejection = br#"{"code":"0","data":[{"clOrdId":"entry1","ordId":"","sCode":"51008"}]}"#;
        assert!(
            matches!(adapter.parse_response(&command, 200, rejection), Err(ExecutionError::VenueRejected { code }) if code == "51008")
        );
        let unrelated = br#"{"code":"0","data":[{"clOrdId":"other","ordId":"123","sCode":"0"}]}"#;
        assert!(adapter.parse_response(&command, 200, unrelated).is_err());
        let missing_client = br#"{"code":"0","data":[{"ordId":"123","sCode":"0"}]}"#;
        assert!(adapter
            .parse_response(&command, 200, missing_client)
            .is_err());
        let missing_algo_client = br#"{"code":"0","data":[{"algoId":"123","sCode":"0"}]}"#;
        let algo_cancel =
            OrderCommand::Cancel(order(OrderKind::StopMarket)?.reference(Some("123".to_owned())));
        assert!(adapter
            .parse_response(&algo_cancel, 200, missing_algo_client)
            .is_ok());
        Ok(())
    }

    #[test]
    fn query_algo_triggered_is_distinct_from_matching_engine_fill() -> TestResult {
        let command = OrderCommand::Query(order(OrderKind::StopMarket)?.reference(None));
        let response =
            br#"{"code":"0","data":[{"algoClOrdId":"entry1","algoId":"123","state":"effective"}]}"#;
        let ack = OkxExecution::new(true).parse_response(&command, 200, response)?;
        assert_eq!(ack.status, Some(OrderStatus::Triggered));
        Ok(())
    }

    #[test]
    fn websocket_login_uses_epoch_seconds_and_its_fixed_verification_path() -> TestResult {
        let adapter = OkxExecution::new(true);
        let login: Value = serde_json::from_str(&adapter.login_message(&credentials()?, at()?)?)?;
        assert_eq!(login["args"][0]["timestamp"], "1700000000");
        assert_eq!(
            login["args"][0]["sign"],
            "lhmJXK08fk9SI1ZwFXKFRrPtzfbNOwC+D1xMJJ/1KZg="
        );
        assert_eq!(
            adapter.parse_user_message(br#"{"event":"login","code":"0"}"#)?,
            vec![UserEvent::Authenticated]
        );
        assert!(adapter
            .parse_user_message(
                br#"{"event":"login","code":"60009","msg":"secret echoed by venue"}"#
            )
            .is_err());
        assert_eq!(
            adapter.parse_user_message(b"pong")?,
            vec![UserEvent::Heartbeat]
        );
        Ok(())
    }

    #[test]
    fn order_stream_uses_cumulative_fills_and_preserves_cancelled_partial_fill() -> TestResult {
        let payload = br#"{"arg":{"channel":"orders"},"data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","posSide":"net","clOrdId":"entry1","ordId":"123","state":"canceled","sz":"2.5","accFillSz":"1.25","fillSz":"0.25","avgPx":"30000.125","uTime":"1700000000000"}]}"#;
        let events = OkxExecution::new(true).parse_user_message(payload)?;
        let [UserEvent::Order(update)] = events.as_slice() else {
            return Err("expected one order update".into());
        };
        assert_eq!(update.status, OrderStatus::Cancelled);
        assert_eq!(update.cumulative_filled, Decimal::new(125, 2));
        assert_eq!(update.average_price, Some(Decimal::new(30_000_125, 3)));
        Ok(())
    }

    #[test]
    fn triggered_child_correlates_algo_client_id_and_net_short_position_is_signed() -> TestResult {
        let adapter = OkxExecution::new(true);
        let child = br#"{"arg":{"channel":"orders"},"data":[{"instId":"BTC-USDT-SWAP","posSide":"net","clOrdId":"venueGeneratedChild","algoClOrdId":"exit1","ordId":"456","state":"filled","sz":"2.5","accFillSz":"2.5","avgPx":"29000","uTime":"1700000000000"}]}"#;
        let events = adapter.parse_user_message(child)?;
        let [UserEvent::Order(update)] = events.as_slice() else {
            return Err("expected child update".into());
        };
        assert_eq!(update.client_order_id, "exit1");
        assert_eq!(update.venue_order_id, "456");
        let positions = br#"{"arg":{"channel":"positions"},"data":[{"instId":"BTC-USDT-SWAP","posSide":"net","pos":"-2.5","uTime":"1700000000000"}]}"#;
        let positions = adapter.parse_user_message(positions)?;
        let [UserEvent::Position(position)] = positions.as_slice() else {
            return Err("expected position update".into());
        };
        assert_eq!(position.signed_quantity, Decimal::new(-25, 1));
        Ok(())
    }

    #[test]
    fn algo_effective_keeps_fill_zero_and_requires_reconciliation() -> TestResult {
        let payload = br#"{"arg":{"channel":"orders-algo"},"data":[{"instId":"BTC-USDT-SWAP","posSide":"net","algoClOrdId":"stop1","algoId":"123","state":"effective","sz":"2.5","actualSz":"2.5","uTime":"1700000000000"}]}"#;
        let events = OkxExecution::new(true).parse_user_message(payload)?;
        let [UserEvent::Order(update), UserEvent::ReconciliationRequired] = events.as_slice()
        else {
            return Err("expected triggered reconciliation".into());
        };
        assert_eq!(update.status, OrderStatus::Triggered);
        assert_eq!(update.cumulative_filled, Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn malformed_scope_credentials_and_protocol_states_fail_closed() -> TestResult {
        let adapter = OkxExecution::new(true);
        let mut request = order(OrderKind::Limit)?;
        request.instrument.market = MarketKind::InversePerpetual;
        assert!(adapter
            .prepare(&OrderCommand::Place(request), &credentials()?, at()?)
            .is_err());
        let no_passphrase = ApiCredentials::new("key".to_owned(), "secret".to_owned(), None)?;
        assert!(adapter.login_message(&no_passphrase, at()?).is_err());
        assert!(adapter.parse_user_message(br#"{"arg":{"channel":"positions"},"data":[{"instId":"BTC-USDT-SWAP","posSide":"short","pos":"2.5","uTime":"1700000000000"}]}"#).is_err());
        assert!(adapter
            .parse_user_message(br#"{"event":"channel-conn-count-error","channel":"orders"}"#)
            .is_err());
        assert!(adapter.parse_user_message(br#"{"arg":{"channel":"orders"},"data":[{"instId":"BTC-USDT-SWAP","posSide":"net","state":"unknown"}]}"#).is_err());
        Ok(())
    }

    #[test]
    fn server_timeouts_never_become_definitive_order_rejections() -> TestResult {
        let adapter = OkxExecution::new(true);
        let command = OrderCommand::Place(order(OrderKind::Limit)?);
        let response = br#"{"code":"50004","msg":"timeout","data":[]}"#;
        assert!(matches!(
            adapter.parse_response(&command, 200, response),
            Err(ExecutionError::AmbiguousResponse)
        ));
        assert!(matches!(
            adapter.parse_response(&command, 400, response),
            Err(ExecutionError::AmbiguousResponse)
        ));
        assert!(matches!(
            adapter.parse_response(&command, 503, b"unavailable"),
            Err(ExecutionError::AmbiguousResponse)
        ));
        let item_response = br#"{"code":"0","data":[{"sCode":"50004"}]}"#;
        assert!(matches!(
            adapter.parse_response(&command, 200, item_response),
            Err(ExecutionError::AmbiguousResponse)
        ));
        Ok(())
    }

    #[test]
    fn reconciliation_preserves_partial_fill_and_rejects_rounded_decimal() -> TestResult {
        let adapter = OkxExecution::new(true);
        let reference = order(OrderKind::Limit)?.reference(Some("123".to_owned()));
        let response = br#"{"code":"0","data":[{"instId":"BTC-USDT-SWAP","posSide":"net","clOrdId":"entry1","ordId":"123","state":"canceled","sz":"2.5","accFillSz":"1.25","avgPx":"30000.125","uTime":"1700000000000"}]}"#;
        let update = adapter.parse_query_order(&reference, 200, response)?;
        assert_eq!(update.status, OrderStatus::Cancelled);
        assert_eq!(update.cumulative_filled, Decimal::new(125, 2));
        assert!(parse_decimal("0.12345678901234567890123456789").is_err());
        Ok(())
    }
}
