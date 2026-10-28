//! Read-only Bybit V5 inventory for USDT- and USDC-settled linear contracts.
//!
//! Each settlement coin is scanned separately for regular orders, conditional
//! orders, and positions. This is not a complete UTA collateral/exposure audit:
//! spot, inverse, options, lending, and other products are outside this scope.
//! Position mode is configurable per symbol, so observed `positionIdx = 0`
//! never proves account-wide one-way mode. Hedge evidence is retained instead.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use reqwest::Method;
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Deserializer};

use super::{
    InventoryAdapter, InventoryKind, InventoryPage, InventoryPartition, InventoryRecord,
    InventoryScope,
};
use crate::{
    execution::{hmac_sha256_hex, ApiCredentials, ExecutionError, SignedRequest},
    Exchange,
};

const RECEIVE_WINDOW: &str = "5000";
const MAX_CURSOR_BYTES: usize = 2048;

/// Signed GET-only inventory adapter; it never changes account mode or orders.
#[derive(Clone, Copy, Debug, Default)]
pub struct BybitInventory;

impl InventoryAdapter for BybitInventory {
    fn exchange(&self) -> Exchange {
        Exchange::Bybit
    }

    fn scope(&self) -> InventoryScope {
        InventoryScope::BybitLinearUsdtUsdc
    }

    fn partitions(&self) -> Vec<InventoryPartition> {
        vec![
            InventoryPartition {
                key: "usdt_orders",
                kind: InventoryKind::Orders,
            },
            InventoryPartition {
                key: "usdt_algo_orders",
                kind: InventoryKind::AlgoOrders,
            },
            InventoryPartition {
                key: "usdt_positions",
                kind: InventoryKind::Positions,
            },
            InventoryPartition {
                key: "usdc_orders",
                kind: InventoryKind::Orders,
            },
            InventoryPartition {
                key: "usdc_algo_orders",
                kind: InventoryKind::AlgoOrders,
            },
            InventoryPartition {
                key: "usdc_positions",
                kind: InventoryKind::Positions,
            },
        ]
    }

    fn prepare(
        &self,
        partition: &InventoryPartition,
        cursor: Option<&str>,
        credentials: &ApiCredentials,
        at: DateTime<Utc>,
    ) -> Result<SignedRequest, ExecutionError> {
        let spec = partition_spec(partition)?;
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query
            .append_pair("category", "linear")
            .append_pair("settleCoin", spec.settle_coin);
        if let Some(filter) = spec.order_filter {
            query
                .append_pair("orderFilter", filter)
                .append_pair("openOnly", "0");
        }
        query.append_pair("limit", &spec.limit.to_string());
        if let Some(cursor) = cursor {
            validate_cursor(cursor)?;
            query.append_pair("cursor", cursor);
        }
        let query = query.finish();
        let timestamp = at.timestamp_millis();
        if timestamp < 0 {
            return Err(ExecutionError::Timestamp);
        }
        let signature = hmac_sha256_hex(
            credentials.secret(),
            format!(
                "{timestamp}{}{RECEIVE_WINDOW}{query}",
                credentials.api_key()
            )
            .as_bytes(),
        )?;
        SignedRequest::new(
            Method::GET,
            spec.path.to_owned(),
            query,
            Bytes::new(),
            vec![
                (
                    "X-BAPI-API-KEY".to_owned(),
                    credentials.api_key().to_owned(),
                ),
                ("X-BAPI-TIMESTAMP".to_owned(), timestamp.to_string()),
                ("X-BAPI-RECV-WINDOW".to_owned(), RECEIVE_WINDOW.to_owned()),
                ("X-BAPI-SIGN".to_owned(), signature),
            ],
        )
    }

    fn parse(
        &self,
        partition: &InventoryPartition,
        status: u16,
        body: &[u8],
    ) -> Result<InventoryPage, ExecutionError> {
        let spec = partition_spec(partition)?;
        let (records, next_cursor) = if spec.order_filter.is_some() {
            let page = parse_page::<OrderRow>(&spec, status, body)?;
            let mut records = Vec::with_capacity(page.list.len());
            for row in page.list {
                append_order(row, &spec, &mut records)?;
            }
            (records, page.next_page_cursor)
        } else {
            let page = parse_page::<PositionRow>(&spec, status, body)?;
            let mut records = Vec::with_capacity(page.list.len());
            for row in page.list {
                append_position(row, &spec, &mut records)?;
            }
            (records, page.next_page_cursor)
        };
        Ok(InventoryPage {
            records,
            next_cursor: (!next_cursor.is_empty()).then_some(next_cursor),
        })
    }
}

struct PartitionSpec {
    settle_coin: &'static str,
    order_filter: Option<&'static str>,
    path: &'static str,
    limit: usize,
}

fn partition_spec(partition: &InventoryPartition) -> Result<PartitionSpec, ExecutionError> {
    let (settle_coin, order_filter) = match (partition.key, partition.kind) {
        ("usdt_orders", InventoryKind::Orders) => ("USDT", Some("Order")),
        ("usdt_algo_orders", InventoryKind::AlgoOrders) => ("USDT", Some("StopOrder")),
        ("usdt_positions", InventoryKind::Positions) => ("USDT", None),
        ("usdc_orders", InventoryKind::Orders) => ("USDC", Some("Order")),
        ("usdc_algo_orders", InventoryKind::AlgoOrders) => ("USDC", Some("StopOrder")),
        ("usdc_positions", InventoryKind::Positions) => ("USDC", None),
        _ => {
            return Err(ExecutionError::InvalidRequest(
                "unknown Bybit inventory partition",
            ))
        }
    };
    Ok(PartitionSpec {
        settle_coin,
        order_filter,
        path: if order_filter.is_some() {
            "/v5/order/realtime"
        } else {
            "/v5/position/list"
        },
        limit: if order_filter.is_some() { 50 } else { 200 },
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResponseCode {
    ret_code: i64,
}

#[derive(Deserialize)]
struct Response<T> {
    result: Page<T>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page<T> {
    category: String,
    // Missing pagination metadata must never be interpreted as end-of-data.
    next_page_cursor: String,
    list: Vec<T>,
    #[serde(default, deserialize_with = "present_string")]
    settle_coin: Option<String>,
    #[serde(default, deserialize_with = "present_string")]
    order_filter: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderRow {
    symbol: String,
    order_id: String,
    order_status: String,
    position_idx: u8,
    // External/manual orders need not carry this bot's custom identifier.
    #[serde(default, deserialize_with = "present_string")]
    order_link_id: Option<String>,
    #[serde(default, deserialize_with = "present_string")]
    settle_coin: Option<String>,
    #[serde(default, deserialize_with = "present_string")]
    category: Option<String>,
    #[serde(default, deserialize_with = "present_string")]
    order_filter: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionRow {
    symbol: String,
    position_idx: u8,
    side: String,
    #[serde(with = "crate::decimal_codec::required")]
    size: Decimal,
    #[serde(default, deserialize_with = "present_string")]
    settle_coin: Option<String>,
    #[serde(default, deserialize_with = "present_string")]
    category: Option<String>,
}

fn present_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some)
}

fn parse_page<T: DeserializeOwned>(
    spec: &PartitionSpec,
    status: u16,
    body: &[u8],
) -> Result<Page<T>, ExecutionError> {
    if status != 200 {
        return Err(ExecutionError::HttpStatus(status));
    }
    // Parse the code before success-only fields so an error response is never
    // mistaken for an empty inventory. Typed decoding also rejects duplicate
    // fields in the code, page metadata, and critical row identity fields.
    let code: ResponseCode = serde_json::from_slice(body)?;
    if code.ret_code != 0 {
        return Err(ExecutionError::VenueRejected {
            code: code.ret_code.to_string(),
        });
    }
    let response: Response<T> = serde_json::from_slice(body)?;
    let page = response.result;
    validate_scope(spec, page.settle_coin.as_deref(), Some(&page.category))?;
    validate_filter(spec, page.order_filter.as_deref())?;
    if page.list.len() > spec.limit {
        return Err(ExecutionError::Protocol(
            "Bybit inventory page exceeds requested limit",
        ));
    }
    if !page.next_page_cursor.is_empty() {
        validate_cursor(&page.next_page_cursor)?;
    }
    Ok(page)
}

fn append_order(
    row: OrderRow,
    spec: &PartitionSpec,
    records: &mut Vec<InventoryRecord>,
) -> Result<(), ExecutionError> {
    validate_scope(spec, row.settle_coin.as_deref(), row.category.as_deref())?;
    validate_filter(spec, row.order_filter.as_deref())?;
    validate_identifier(&row.symbol)?;
    validate_identifier(&row.order_id)?;
    validate_position_index(row.position_idx)?;
    if !matches!(
        row.order_status.as_str(),
        "New" | "PartiallyFilled" | "Untriggered"
    ) {
        return Err(ExecutionError::Protocol(
            "Bybit open-order inventory returned non-open status",
        ));
    }
    let client_order_id = row.order_link_id.filter(|id| !id.is_empty());
    if let Some(id) = &client_order_id {
        validate_identifier(id)?;
    }
    records.push(InventoryRecord::Order {
        symbol: row.symbol,
        venue_order_id: row.order_id,
        client_order_id,
    });
    append_hedge_evidence(row.position_idx, records);
    Ok(())
}

fn append_position(
    row: PositionRow,
    spec: &PartitionSpec,
    records: &mut Vec<InventoryRecord>,
) -> Result<(), ExecutionError> {
    validate_scope(spec, row.settle_coin.as_deref(), row.category.as_deref())?;
    validate_identifier(&row.symbol)?;
    validate_position_index(row.position_idx)?;
    if row.size < Decimal::ZERO
        || (row.position_idx == 1 && row.size > Decimal::ZERO && row.side != "Buy")
        || (row.position_idx == 2 && row.size > Decimal::ZERO && row.side != "Sell")
    {
        return Err(ExecutionError::Protocol(
            "Bybit position side or size is inconsistent",
        ));
    }
    let quantity = match row.side.as_str() {
        "Buy" => row.size,
        "Sell" => Decimal::ZERO
            .checked_sub(row.size)
            .ok_or(ExecutionError::Arithmetic)?,
        "" if row.size == Decimal::ZERO => Decimal::ZERO,
        _ => return Err(ExecutionError::Protocol("Bybit position side is invalid")),
    };
    records.push(InventoryRecord::Position {
        position_id: format!("{}:{}", row.symbol, row.position_idx),
        symbol: row.symbol,
        quantity,
    });
    append_hedge_evidence(row.position_idx, records);
    Ok(())
}

fn append_hedge_evidence(position_idx: u8, records: &mut Vec<InventoryRecord>) {
    if position_idx != 0 {
        records.push(InventoryRecord::Mode { one_way: false });
    }
}

fn validate_position_index(position_idx: u8) -> Result<(), ExecutionError> {
    if position_idx > 2 {
        return Err(ExecutionError::Protocol(
            "Bybit inventory position index is invalid",
        ));
    }
    Ok(())
}

fn validate_scope(
    spec: &PartitionSpec,
    settle_coin: Option<&str>,
    category: Option<&str>,
) -> Result<(), ExecutionError> {
    // Bybit does not normally echo settleCoin. Validate it whenever supplied,
    // without inventing symbol-suffix rules for USDC perpetuals and futures.
    if settle_coin.is_some_and(|coin| coin != spec.settle_coin)
        || category.is_some_and(|category| category != "linear")
    {
        return Err(ExecutionError::Protocol("Bybit inventory scope mismatch"));
    }
    Ok(())
}

fn validate_filter(spec: &PartitionSpec, filter: Option<&str>) -> Result<(), ExecutionError> {
    if filter.is_some_and(|filter| Some(filter) != spec.order_filter) {
        return Err(ExecutionError::Protocol(
            "Bybit inventory order filter mismatch",
        ));
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), ExecutionError> {
    if value.is_empty() || value.len() > 128 || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(ExecutionError::Protocol(
            "Bybit inventory identifier is invalid",
        ));
    }
    Ok(())
}

fn validate_cursor(cursor: &str) -> Result<(), ExecutionError> {
    if cursor.is_empty()
        || cursor.len() > MAX_CURSOR_BYTES
        || !cursor.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(ExecutionError::Protocol(
            "Bybit inventory cursor is invalid",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use serde_json::{json, Value};

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    fn partition(key: &'static str, kind: InventoryKind) -> InventoryPartition {
        InventoryPartition { key, kind }
    }

    fn response(rows: Value) -> Value {
        let mut response = json!({"retCode":0,"retMsg":"OK","result":{"category":"linear","nextPageCursor":"","list":[]}});
        response["result"]["list"] = rows;
        response
    }

    fn order() -> Value {
        json!({"symbol":"BTCUSDT","orderId":"external-1","positionIdx":0,"orderStatus":"New"})
    }

    fn position() -> Value {
        json!({"symbol":"BTCUSDT","positionIdx":0,"side":"Buy","size":"0.1234567890123456789012345678"})
    }

    fn parse(
        partition: &InventoryPartition,
        body: &Value,
    ) -> Result<InventoryPage, ExecutionError> {
        BybitInventory.parse(partition, 200, &serde_json::to_vec(body)?)
    }

    #[test]
    fn partitions_cover_both_settlement_coins_and_both_order_filters() -> TestResult {
        let credentials = ApiCredentials::new("key".to_owned(), "secret".to_owned(), None)?;
        let at =
            DateTime::from_timestamp(1_700_000_000, 0).ok_or_else(|| io::Error::other("time"))?;
        let partitions = BybitInventory.partitions();
        assert_eq!(partitions.len(), 6);
        for partition in partitions {
            let request = BybitInventory.prepare(&partition, None, &credentials, at)?;
            assert_eq!(request.method(), &Method::GET);
            assert!(request.body().is_empty());
            assert!(request
                .query()
                .contains(if partition.key.starts_with("usdt") {
                    "settleCoin=USDT"
                } else {
                    "settleCoin=USDC"
                }));
            match partition.kind {
                InventoryKind::Orders => assert!(request
                    .query()
                    .contains("orderFilter=Order&openOnly=0&limit=50")),
                InventoryKind::AlgoOrders => assert!(request
                    .query()
                    .contains("orderFilter=StopOrder&openOnly=0&limit=50")),
                InventoryKind::Positions => assert_eq!(request.path(), "/v5/position/list"),
                InventoryKind::Mode => {
                    return Err(io::Error::other("unexpected global mode proof").into())
                }
            }
        }
        Ok(())
    }

    #[test]
    fn signed_get_covers_exact_encoded_cursor_and_has_no_body() -> TestResult {
        let credentials = ApiCredentials::new("key".to_owned(), "secret".to_owned(), None)?;
        let at =
            DateTime::from_timestamp(1_700_000_000, 0).ok_or_else(|| io::Error::other("time"))?;
        let selected = partition("usdc_algo_orders", InventoryKind::AlgoOrders);
        let request =
            BybitInventory.prepare(&selected, Some("page_args%3Dx&n=2"), &credentials, at)?;
        let query = "category=linear&settleCoin=USDC&orderFilter=StopOrder&openOnly=0&limit=50&cursor=page_args%253Dx%26n%3D2";
        assert_eq!(request.query(), query);
        let expected =
            hmac_sha256_hex("secret", format!("1700000000000key5000{query}").as_bytes())?;
        assert!(request
            .headers()
            .iter()
            .any(|(name, value)| name == "X-BAPI-SIGN" && *value == expected));
        assert_eq!(request.path(), "/v5/order/realtime");
        assert!(BybitInventory
            .prepare(&selected, Some("bad\ncursor"), &credentials, at)
            .is_err());
        assert!(BybitInventory
            .prepare(
                &partition("wrong", InventoryKind::Orders),
                None,
                &credentials,
                at
            )
            .is_err());
        Ok(())
    }

    #[test]
    fn absent_or_empty_client_ids_preserve_external_orders() -> TestResult {
        let mut second = order();
        second["orderId"] = json!("external-2");
        second["orderLinkId"] = json!("");
        let page = parse(
            &partition("usdt_orders", InventoryKind::Orders),
            &response(json!([order(), second])),
        )?;
        assert_eq!(page.records.len(), 2);
        assert!(page.records.iter().all(|record| matches!(
            record,
            InventoryRecord::Order {
                client_order_id: None,
                ..
            }
        )));
        assert!(page.next_cursor.is_none());
        Ok(())
    }

    #[test]
    fn missing_pagination_and_wrong_scope_never_mean_empty_inventory() -> TestResult {
        let partition = partition("usdt_orders", InventoryKind::Orders);
        for pointer in ["/result/category", "/result/nextPageCursor", "/result/list"] {
            let mut body = response(json!([]));
            *body
                .pointer_mut(pointer)
                .ok_or_else(|| io::Error::other("fixture"))? = Value::Null;
            assert!(parse(&partition, &body).is_err());
        }
        let mut missing = response(json!([]));
        missing["result"]
            .as_object_mut()
            .ok_or_else(|| io::Error::other("fixture"))?
            .remove("nextPageCursor");
        assert!(parse(&partition, &missing).is_err());
        for (pointer, invalid) in [
            ("/result/category", "inverse"),
            ("/result/settleCoin", "USDC"),
            ("/result/orderFilter", "StopOrder"),
        ] {
            let mut body = response(json!([]));
            let field = pointer
                .strip_prefix("/result/")
                .ok_or_else(|| io::Error::other("fixture"))?;
            body["result"][field] = json!(invalid);
            assert!(parse(&partition, &body).is_err());
        }
        let mut row = order();
        row["settleCoin"] = json!("USDC");
        assert!(parse(&partition, &response(json!([row]))).is_err());
        Ok(())
    }

    #[test]
    fn cursor_is_retained_and_terminal_order_rows_fail_closed() -> TestResult {
        let partition = partition("usdt_algo_orders", InventoryKind::AlgoOrders);
        let mut row = order();
        row["orderStatus"] = json!("Untriggered");
        let mut body = response(json!([row]));
        body["result"]["nextPageCursor"] = json!("page_args%3Dabc");
        assert_eq!(
            parse(&partition, &body)?.next_cursor.as_deref(),
            Some("page_args%3Dabc")
        );
        for status in ["Filled", "Cancelled", "Triggered", "Unknown"] {
            body["result"]["list"][0]["orderStatus"] = json!(status);
            assert!(parse(&partition, &body).is_err());
        }
        Ok(())
    }

    #[test]
    fn positions_preserve_exact_signed_size_and_hedge_evidence() -> TestResult {
        let partition = partition("usdt_positions", InventoryKind::Positions);
        let mut row = position();
        row["positionIdx"] = json!(2);
        row["side"] = json!("Sell");
        let page = parse(&partition, &response(json!([row])))?;
        assert!(page.records.iter().any(|record| matches!(record, InventoryRecord::Position { position_id, quantity, .. }
            if position_id == "BTCUSDT:2" && quantity.to_string() == "-0.1234567890123456789012345678")));
        assert!(page
            .records
            .iter()
            .any(|record| matches!(record, InventoryRecord::Mode { one_way: false })));
        let empty = parse(&partition, &response(json!([])))?;
        assert!(empty.records.is_empty());
        let one_way_row = parse(&partition, &response(json!([position()])))?;
        assert!(!one_way_row
            .records
            .iter()
            .any(|record| matches!(record, InventoryRecord::Mode { one_way: true })));
        Ok(())
    }

    #[test]
    fn position_rows_reject_binary_numbers_precision_loss_and_bad_sides() {
        let partition = partition("usdc_positions", InventoryKind::Positions);
        for size in [
            json!(1),
            json!("0.12345678901234567890123456789"),
            json!("1e-3"),
            json!("-1"),
        ] {
            let mut row = position();
            row["size"] = size;
            assert!(parse(&partition, &response(json!([row]))).is_err());
        }
        for (index, side) in [(3, "Buy"), (1, "Sell"), (2, "Buy"), (0, "")] {
            let mut row = position();
            row["positionIdx"] = json!(index);
            row["side"] = json!(side);
            assert!(parse(&partition, &response(json!([row]))).is_err());
        }
    }

    #[test]
    fn errors_and_duplicate_critical_fields_are_rejected() {
        let partition = partition("usdt_orders", InventoryKind::Orders);
        assert!(matches!(
            BybitInventory.parse(&partition, 429, b"{}"),
            Err(ExecutionError::HttpStatus(429))
        ));
        assert!(matches!(
            BybitInventory.parse(
                &partition,
                200,
                br#"{"retCode":10001,"retMsg":"do not expose raw errors"}"#
            ),
            Err(ExecutionError::VenueRejected { .. })
        ));
        assert!(BybitInventory.parse(&partition, 200, br#"{"retCode":0,"retCode":1,"result":{"category":"linear","nextPageCursor":"","list":[]}}"#).is_err());
        assert!(BybitInventory.parse(&partition, 200, br#"{"retCode":0,"result":{"category":"linear","nextPageCursor":"","nextPageCursor":"more","list":[]}}"#).is_err());
    }
}
