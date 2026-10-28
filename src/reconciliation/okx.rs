//! Read-only OKX trading-account inventory, not an account-flat authorization.
//!
//! The scan covers account configuration, unfiltered current positions, regular
//! pending orders and all eight documented pending-algo families. It deliberately
//! retains orders/positions outside the bot's SWAP instruments. It does not scan
//! separate grid, recurring-buy, signal/copy-trading, funding or other ecosystem
//! endpoints, and does not prove that no other actor can create new exposure.
//! See <https://www.okx.com/docs-v5/en/> for the endpoint specifications.

use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use reqwest::Method;
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Deserializer};

use crate::{
    execution::{hmac_sha256_base64, ApiCredentials, ExecutionError, SignedRequest},
    models::Exchange,
};

use super::{
    InventoryAdapter, InventoryKind, InventoryPage, InventoryPartition, InventoryRecord,
    InventoryScope,
};

const PAGE_SIZE: usize = 100;
const MAX_POSITIONS: usize = 20_000;
const ALGO_FAMILIES: [(&str, &str); 8] = [
    ("algo-conditional", "conditional"),
    ("algo-oco", "oco"),
    ("algo-chase", "chase"),
    ("algo-trigger", "trigger"),
    ("algo-trailing", "move_order_stop"),
    ("algo-iceberg", "iceberg"),
    ("algo-twap", "twap"),
    ("algo-smart-iceberg", "smart_iceberg"),
];

/// Signed read-only inventory adapter for one OKX trading account.
///
/// Demo selection only controls the simulated-trading header. The caller must
/// separately bind credentials and transport origin to the intended account.
#[derive(Clone, Copy, Debug)]
pub struct OkxInventory {
    demo: bool,
}

impl OkxInventory {
    /// Selects whether requests carry the demo-trading header.
    #[must_use]
    pub const fn new(demo: bool) -> Self {
        Self { demo }
    }
}

impl InventoryAdapter for OkxInventory {
    fn exchange(&self) -> Exchange {
        Exchange::Okx
    }

    fn scope(&self) -> InventoryScope {
        InventoryScope::OkxTrading
    }

    fn partitions(&self) -> Vec<InventoryPartition> {
        let mut partitions = vec![
            InventoryPartition {
                key: "mode",
                kind: InventoryKind::Mode,
            },
            InventoryPartition {
                key: "positions",
                kind: InventoryKind::Positions,
            },
            InventoryPartition {
                key: "orders",
                kind: InventoryKind::Orders,
            },
        ];
        partitions.extend(ALGO_FAMILIES.iter().map(|(key, _)| InventoryPartition {
            key,
            kind: InventoryKind::AlgoOrders,
        }));
        partitions
    }

    fn prepare(
        &self,
        partition: &InventoryPartition,
        cursor: Option<&str>,
        credentials: &ApiCredentials,
        at: DateTime<Utc>,
    ) -> Result<SignedRequest, ExecutionError> {
        let route = route(partition)?;
        let query = query(route, cursor)?;
        let path = route.path();
        if at.timestamp() < 0 {
            return Err(ExecutionError::Timestamp);
        }
        let timestamp = at.to_rfc3339_opts(SecondsFormat::Millis, true);
        let target = if query.is_empty() {
            path.to_owned()
        } else {
            format!("{path}?{query}")
        };
        let signature = hmac_sha256_base64(
            credentials.secret(),
            format!("{timestamp}GET{target}").as_bytes(),
        )?;
        let passphrase = credentials
            .passphrase()
            .ok_or(ExecutionError::InvalidRequest("OKX passphrase required"))?;
        let mut headers = vec![
            ("OK-ACCESS-KEY".to_owned(), credentials.api_key().to_owned()),
            ("OK-ACCESS-SIGN".to_owned(), signature),
            ("OK-ACCESS-TIMESTAMP".to_owned(), timestamp),
            ("OK-ACCESS-PASSPHRASE".to_owned(), passphrase.to_owned()),
        ];
        if self.demo {
            headers.push(("x-simulated-trading".to_owned(), "1".to_owned()));
        }
        SignedRequest::new(Method::GET, path.to_owned(), query, Bytes::new(), headers)
    }

    fn parse(
        &self,
        partition: &InventoryPartition,
        status: u16,
        body: &[u8],
    ) -> Result<InventoryPage, ExecutionError> {
        match route(partition)? {
            Route::Mode => parse_mode(&decode(status, body)?),
            Route::Positions => parse_positions(decode(status, body)?),
            Route::Orders => parse_orders(decode(status, body)?),
            Route::Algo(family) => parse_algos(decode(status, body)?, family),
        }
    }
}

#[derive(Clone, Copy)]
enum Route {
    Mode,
    Positions,
    Orders,
    Algo(&'static str),
}

impl Route {
    const fn path(self) -> &'static str {
        match self {
            Self::Mode => "/api/v5/account/config",
            Self::Positions => "/api/v5/account/positions",
            Self::Orders => "/api/v5/trade/orders-pending",
            Self::Algo(_) => "/api/v5/trade/orders-algo-pending",
        }
    }
}

fn route(partition: &InventoryPartition) -> Result<Route, ExecutionError> {
    match (partition.key, partition.kind) {
        ("mode", InventoryKind::Mode) => Ok(Route::Mode),
        ("positions", InventoryKind::Positions) => Ok(Route::Positions),
        ("orders", InventoryKind::Orders) => Ok(Route::Orders),
        (key, InventoryKind::AlgoOrders) => ALGO_FAMILIES
            .iter()
            .find_map(|(expected, family)| (*expected == key).then_some(Route::Algo(family)))
            .ok_or(ExecutionError::InvalidRequest(
                "unknown OKX inventory partition",
            )),
        _ => Err(ExecutionError::InvalidRequest(
            "unknown OKX inventory partition",
        )),
    }
}

fn query(route: Route, cursor: Option<&str>) -> Result<String, ExecutionError> {
    if matches!(route, Route::Mode | Route::Positions) {
        return if cursor.is_none() {
            Ok(String::new())
        } else {
            Err(ExecutionError::InvalidRequest(
                "OKX endpoint does not paginate",
            ))
        };
    }
    if cursor.is_some_and(|value| !valid_numeric_id(value)) {
        return Err(ExecutionError::InvalidRequest(
            "invalid OKX inventory cursor",
        ));
    }
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    if let Route::Algo(family) = route {
        query.append_pair("ordType", family);
    }
    query.append_pair("limit", "100");
    if let Some(cursor) = cursor {
        query.append_pair("after", cursor);
    }
    Ok(query.finish())
}

#[derive(Deserialize)]
struct Envelope<T> {
    code: String,
    data: Vec<T>,
}

fn decode<T: DeserializeOwned>(status: u16, body: &[u8]) -> Result<Vec<T>, ExecutionError> {
    if status == 408 || status >= 500 {
        return Err(ExecutionError::AmbiguousResponse);
    }
    if !(200..300).contains(&status) {
        return Err(ExecutionError::HttpStatus(status));
    }
    let response: Envelope<T> = serde_json::from_slice(body)
        .map_err(|_| ExecutionError::Protocol("malformed OKX inventory response"))?;
    check_code(&response.code)?;
    Ok(response.data)
}

fn check_code(code: &str) -> Result<(), ExecutionError> {
    if code == "0" {
        return Ok(());
    }
    if code.is_empty() || code.len() > 16 || !code.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ExecutionError::Protocol(
            "malformed OKX inventory result code",
        ));
    }
    Err(ExecutionError::VenueRejected {
        code: code.to_owned(),
    })
}

fn row_code(code: Option<&str>) -> Result<(), ExecutionError> {
    code.map_or(Ok(()), check_code)
}

// Missing sCode is normal for read endpoints, but present null/numeric fields
// must not silently become a successful row.
fn present_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModeRow {
    pos_mode: String,
    acct_lv: String,
    #[serde(default, deserialize_with = "present_string")]
    s_code: Option<String>,
}

fn parse_mode(rows: &[ModeRow]) -> Result<InventoryPage, ExecutionError> {
    let [row] = rows else {
        return Err(ExecutionError::Protocol(
            "OKX account configuration cardinality",
        ));
    };
    row_code(row.s_code.as_deref())?;
    if !matches!(row.acct_lv.as_str(), "1" | "2" | "3" | "4") {
        return Err(ExecutionError::Protocol("unknown OKX account mode"));
    }
    let one_way = match row.pos_mode.as_str() {
        "net_mode" => row.acct_lv != "1",
        "long_short_mode" => false,
        _ => return Err(ExecutionError::Protocol("unknown OKX position mode")),
    };
    Ok(InventoryPage {
        records: vec![InventoryRecord::Mode { one_way }],
        next_cursor: None,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionRow {
    inst_id: String,
    inst_type: String,
    pos_id: String,
    pos_side: String,
    #[serde(deserialize_with = "crate::decimal_codec::required::deserialize")]
    pos: Decimal,
    #[serde(default, deserialize_with = "present_string")]
    s_code: Option<String>,
}

fn parse_positions(rows: Vec<PositionRow>) -> Result<InventoryPage, ExecutionError> {
    if rows.len() > MAX_POSITIONS {
        return Err(ExecutionError::Protocol(
            "OKX inventory position limit exceeded",
        ));
    }
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        row_code(row.s_code.as_deref())?;
        validate_symbol(&row.inst_id)?;
        validate_symbol(&row.inst_type)?;
        if !valid_numeric_id(&row.pos_id) {
            return Err(ExecutionError::Protocol("invalid OKX position identity"));
        }
        let quantity = match row.pos_side.as_str() {
            "net" => row.pos,
            "long" if row.pos >= Decimal::ZERO => row.pos,
            "short" if row.pos >= Decimal::ZERO => Decimal::ZERO
                .checked_sub(row.pos)
                .ok_or(ExecutionError::Arithmetic)?,
            _ => {
                return Err(ExecutionError::Protocol(
                    "invalid OKX position side or sign",
                ))
            }
        };
        if row.pos_side != "net" {
            records.push(InventoryRecord::Mode { one_way: false });
        }
        // MARGIN/FUTURES/OPTION/EVENTS and unfamiliar products are retained.
        // The collector must treat every nonzero row as exposure, not only SWAP.
        records.push(InventoryRecord::Position {
            symbol: row.inst_id,
            position_id: row.pos_id,
            quantity,
        });
    }
    Ok(InventoryPage {
        records,
        next_cursor: None,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderRow {
    inst_id: String,
    inst_type: String,
    ord_id: String,
    cl_ord_id: String,
    ord_type: String,
    state: String,
    #[serde(default, deserialize_with = "present_string")]
    s_code: Option<String>,
}

fn parse_orders(rows: Vec<OrderRow>) -> Result<InventoryPage, ExecutionError> {
    validate_page_length(rows.len())?;
    let next_cursor = (rows.len() == PAGE_SIZE)
        .then(|| rows.last().map(|row| row.ord_id.clone()))
        .flatten();
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        row_code(row.s_code.as_deref())?;
        validate_symbol(&row.inst_id)?;
        validate_symbol(&row.inst_type)?;
        validate_symbol(&row.ord_type)?;
        if !valid_numeric_id(&row.ord_id)
            || !matches!(row.state.as_str(), "live" | "partially_filled")
        {
            return Err(ExecutionError::Protocol("invalid OKX pending order"));
        }
        records.push(InventoryRecord::Order {
            symbol: row.inst_id,
            venue_order_id: row.ord_id,
            client_order_id: client_id(row.cl_ord_id)?,
        });
    }
    Ok(InventoryPage {
        records,
        next_cursor,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlgoRow {
    inst_id: String,
    inst_type: String,
    algo_id: String,
    algo_cl_ord_id: String,
    ord_type: String,
    state: String,
    #[serde(default, deserialize_with = "present_string")]
    s_code: Option<String>,
}

fn parse_algos(rows: Vec<AlgoRow>, family: &str) -> Result<InventoryPage, ExecutionError> {
    validate_page_length(rows.len())?;
    let next_cursor = (rows.len() == PAGE_SIZE)
        .then(|| rows.last().map(|row| row.algo_id.clone()))
        .flatten();
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        row_code(row.s_code.as_deref())?;
        validate_symbol(&row.inst_id)?;
        validate_symbol(&row.inst_type)?;
        if !valid_numeric_id(&row.algo_id)
            || row.ord_type != family
            || !matches!(row.state.as_str(), "live" | "pause")
        {
            return Err(ExecutionError::Protocol("invalid OKX pending algo order"));
        }
        records.push(InventoryRecord::Order {
            symbol: row.inst_id,
            venue_order_id: row.algo_id,
            client_order_id: client_id(row.algo_cl_ord_id)?,
        });
    }
    Ok(InventoryPage {
        records,
        next_cursor,
    })
}

fn validate_page_length(length: usize) -> Result<(), ExecutionError> {
    if length > PAGE_SIZE {
        return Err(ExecutionError::Protocol("oversized OKX inventory page"));
    }
    Ok(())
}

fn valid_numeric_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.bytes().any(|byte| byte != b'0')
}

fn validate_symbol(value: &str) -> Result<(), ExecutionError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':'))
    {
        return Err(ExecutionError::Protocol("invalid OKX inventory symbol"));
    }
    Ok(())
}

fn client_id(value: String) -> Result<Option<String>, ExecutionError> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > 32 || !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(ExecutionError::Protocol(
            "invalid OKX inventory client identity",
        ));
    }
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::error::Error;

    type TestResult = Result<(), Box<dyn Error>>;

    fn partition(key: &'static str, kind: InventoryKind) -> InventoryPartition {
        InventoryPartition { key, kind }
    }

    fn credentials() -> Result<ApiCredentials, ExecutionError> {
        ApiCredentials::new(
            "key".to_owned(),
            "secret".to_owned(),
            Some("pass".to_owned()),
        )
    }

    fn at() -> Result<DateTime<Utc>, Box<dyn Error>> {
        Ok(DateTime::parse_from_rfc3339("2026-10-28T01:02:03.456Z")?.with_timezone(&Utc))
    }

    fn body(rows: Value) -> Result<Vec<u8>, serde_json::Error> {
        let mut envelope = json!({"code":"0", "msg":""});
        envelope["data"] = rows;
        serde_json::to_vec(&envelope)
    }

    fn ordinary(id: &str) -> Value {
        json!({"instId":"BTC-USDT", "instType":"SPOT", "ordId":id,
            "clOrdId":"", "ordType":"limit", "state":"live"})
    }

    fn algo(id: &str, family: &str) -> Value {
        json!({"instId":"BTC-USD-SWAP", "instType":"SWAP", "algoId":id,
            "algoClOrdId":"external01", "ordType":family, "state":"pause"})
    }

    fn position(id: &str, quantity: Value, side: &str) -> Value {
        let mut position = json!({"instId":"BTC-USDT", "instType":"MARGIN", "posId":id,
            "posSide":side});
        position["pos"] = quantity;
        position
    }

    #[test]
    fn signs_exact_get_path_and_query_with_demo_header() -> TestResult {
        let adapter = OkxInventory::new(true);
        let request = adapter.prepare(
            &partition("algo-chase", InventoryKind::AlgoOrders),
            Some("12345"),
            &credentials()?,
            at()?,
        )?;
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.path(), "/api/v5/trade/orders-algo-pending");
        assert_eq!(request.query(), "ordType=chase&limit=100&after=12345");
        assert!(request.body().is_empty());
        let expected = hmac_sha256_base64("secret", b"2026-10-28T01:02:03.456ZGET/api/v5/trade/orders-algo-pending?ordType=chase&limit=100&after=12345")?;
        assert!(request
            .headers()
            .contains(&("OK-ACCESS-SIGN".to_owned(), expected)));
        assert!(request
            .headers()
            .contains(&("x-simulated-trading".to_owned(), "1".to_owned())));
        assert!(!format!("{request:?}").contains("secret"));
        let live = OkxInventory::new(false).prepare(
            &partition("mode", InventoryKind::Mode),
            None,
            &credentials()?,
            at()?,
        )?;
        assert_eq!(live.query(), "");
        assert!(!live
            .headers()
            .iter()
            .any(|(name, _)| name == "x-simulated-trading"));
        Ok(())
    }

    #[test]
    fn partitions_include_all_eight_algo_families_without_instrument_filters() -> TestResult {
        let adapter = OkxInventory::new(false);
        assert_eq!(adapter.exchange(), Exchange::Okx);
        assert_eq!(adapter.scope(), InventoryScope::OkxTrading);
        let partitions = adapter.partitions();
        assert_eq!(partitions.len(), 11);
        for partition in partitions {
            let request = adapter.prepare(&partition, None, &credentials()?, at()?)?;
            assert_eq!(request.method(), Method::GET);
            assert!(!request.query().contains("instType"));
            assert!(!request.query().contains("instId"));
            if partition.kind == InventoryKind::AlgoOrders {
                assert!(request.query().contains("ordType="));
            }
        }
        Ok(())
    }

    #[test]
    fn rejects_unknown_partitions_cursor_injection_and_missing_passphrase() -> TestResult {
        let adapter = OkxInventory::new(false);
        let positions = partition("positions", InventoryKind::Positions);
        assert!(adapter
            .prepare(&positions, Some("1"), &credentials()?, at()?)
            .is_err());
        let orders = partition("orders", InventoryKind::Orders);
        for cursor in ["", "0", "12&instId=BTC-USDT", "-1", "123\n"] {
            assert!(adapter
                .prepare(&orders, Some(cursor), &credentials()?, at()?)
                .is_err());
        }
        assert!(adapter
            .prepare(
                &partition("mode", InventoryKind::Orders),
                None,
                &credentials()?,
                at()?
            )
            .is_err());
        let missing = ApiCredentials::new("key".to_owned(), "secret".to_owned(), None)?;
        assert!(adapter.prepare(&orders, None, &missing, at()?).is_err());
        Ok(())
    }

    #[test]
    fn empty_inventory_is_valid_but_missing_or_unknown_mode_is_not() -> TestResult {
        let adapter = OkxInventory::new(false);
        for partition in adapter.partitions() {
            let result = adapter.parse(&partition, 200, &body(json!([]))?);
            if partition.kind == InventoryKind::Mode {
                assert!(result.is_err());
            } else {
                let page = result?;
                assert!(page.records.is_empty());
                assert!(page.next_cursor.is_none());
            }
        }
        let mode = partition("mode", InventoryKind::Mode);
        for row in [
            json!({"posMode":"unknown", "acctLv":"2"}),
            json!({"acctLv":"2"}),
            json!({"posMode":"net_mode"}),
        ] {
            assert!(adapter.parse(&mode, 200, &body(json!([row]))?).is_err());
        }
        for (pos_mode, level, one_way) in [
            ("net_mode", "2", true),
            ("long_short_mode", "2", false),
            ("net_mode", "1", false),
        ] {
            let page = adapter.parse(
                &mode,
                200,
                &body(json!([{"posMode":pos_mode, "acctLv":level}]))?,
            )?;
            assert_eq!(page.records, vec![InventoryRecord::Mode { one_way }]);
        }
        Ok(())
    }

    #[test]
    fn full_pages_continue_after_last_id_and_short_pages_terminate() -> TestResult {
        let adapter = OkxInventory::new(false);
        let rows: Vec<_> = (1..=100)
            .rev()
            .map(|id| ordinary(&id.to_string()))
            .collect();
        let orders = partition("orders", InventoryKind::Orders);
        let page = adapter.parse(&orders, 200, &body(json!(rows))?)?;
        assert_eq!(page.records.len(), 100);
        assert_eq!(page.next_cursor.as_deref(), Some("1"));
        assert!(adapter
            .parse(&orders, 200, &body(json!([ordinary("5")]))?)?
            .next_cursor
            .is_none());
        let rows: Vec<_> = (101..=200)
            .rev()
            .map(|id| algo(&id.to_string(), "smart_iceberg"))
            .collect();
        let page = adapter.parse(
            &partition("algo-smart-iceberg", InventoryKind::AlgoOrders),
            200,
            &body(json!(rows))?,
        )?;
        assert_eq!(page.next_cursor.as_deref(), Some("101"));
        assert!(adapter
            .parse(&orders, 200, &body(json!(vec![ordinary("1"); 101]))?)
            .is_err());
        Ok(())
    }

    #[test]
    fn external_products_manual_orders_and_paused_algos_are_retained() -> TestResult {
        let adapter = OkxInventory::new(false);
        let page = adapter.parse(
            &partition("orders", InventoryKind::Orders),
            200,
            &body(json!([ordinary("123")]))?,
        )?;
        assert_eq!(
            page.records,
            vec![InventoryRecord::Order {
                symbol: "BTC-USDT".to_owned(),
                venue_order_id: "123".to_owned(),
                client_order_id: None,
            }]
        );
        for (key, family) in ALGO_FAMILIES {
            let page = adapter.parse(
                &partition(key, InventoryKind::AlgoOrders),
                200,
                &body(json!([algo("456", family)]))?,
            )?;
            assert_eq!(page.records.len(), 1);
        }
        Ok(())
    }

    #[test]
    fn parses_exact_signed_positions_and_preserves_non_swap_exposure() -> TestResult {
        let page = OkxInventory::new(false).parse(
            &partition("positions", InventoryKind::Positions),
            200,
            &body(json!([
                position("1", json!("-0.0000000000000000000000000001"), "net"),
                position("2", json!("2"), "short"),
                position("3", json!("0"), "net")
            ]))?,
        )?;
        assert_eq!(page.records.len(), 4);
        assert!(page.next_cursor.is_none());
        assert!(page
            .records
            .contains(&InventoryRecord::Mode { one_way: false }));
        assert!(page.records.contains(&InventoryRecord::Position {
            symbol: "BTC-USDT".to_owned(),
            position_id: "1".to_owned(),
            quantity: Decimal::new(-1, 28),
        }));
        assert!(page.records.contains(&InventoryRecord::Position {
            symbol: "BTC-USDT".to_owned(),
            position_id: "2".to_owned(),
            quantity: Decimal::new(-2, 0),
        }));
        Ok(())
    }

    #[test]
    fn rejects_missing_numeric_or_inexact_positions_and_invalid_sides() -> TestResult {
        let adapter = OkxInventory::new(false);
        let positions = partition("positions", InventoryKind::Positions);
        for quantity in [
            json!(1),
            json!(null),
            json!(""),
            json!("1e-2"),
            json!("0.12345678901234567890123456789"),
        ] {
            assert!(adapter
                .parse(
                    &positions,
                    200,
                    &body(json!([position("1", quantity, "net")]))?
                )
                .is_err());
        }
        assert!(adapter
            .parse(
                &positions,
                200,
                &body(json!([position("1", json!("-1"), "short")]))?
            )
            .is_err());
        assert!(adapter
            .parse(
                &positions,
                200,
                &body(json!([position("1", json!("0"), "unknown")]))?
            )
            .is_err());
        assert!(adapter.parse(&positions, 200, &body(json!([{"instId":"BTC-USDT", "instType":"MARGIN", "posId":"1", "posSide":"net"}]))?).is_err());
        Ok(())
    }

    #[test]
    fn malformed_envelopes_result_codes_and_rows_never_look_empty() -> TestResult {
        let adapter = OkxInventory::new(false);
        let orders = partition("orders", InventoryKind::Orders);
        for malformed in [
            b"{}".as_slice(),
            br#"{"code":"0"}"#,
            br#"{"code":"0","data":null}"#,
            br#"{"code":0,"data":[]}"#,
        ] {
            assert!(adapter.parse(&orders, 200, malformed).is_err());
        }
        let error = adapter.parse(
            &orders,
            200,
            br#"{"code":"50101","data":[],"msg":"sensitive venue detail"}"#,
        );
        assert!(matches!(error, Err(ExecutionError::VenueRejected { .. })));
        assert!(!format!("{error:?}").contains("sensitive venue detail"));
        for code in [json!("51000"), json!(null), json!(0), json!("secret-code")] {
            let mut row = ordinary("1");
            row["sCode"] = code;
            assert!(adapter.parse(&orders, 200, &body(json!([row]))?).is_err());
        }
        assert!(adapter
            .parse(&orders, 200, &body(json!([ordinary("not-id")]))?)
            .is_err());
        assert!(adapter
            .parse(
                &partition("algo-oco", InventoryKind::AlgoOrders),
                200,
                &body(json!([algo("1", "conditional")]))?
            )
            .is_err());
        assert!(adapter.parse(&orders, 503, &body(json!([]))?).is_err());
        Ok(())
    }
}
