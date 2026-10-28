//! Read-only, unfiltered Binance USD-M inventory protocol.
//!
//! Official endpoints: [trade inventory](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)
//! and [position mode](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account).
//! Open regular/algo orders and V3 positions are unpaginated when the symbol is
//! omitted. The four reads are not an atomic account snapshot and never grant
//! entry admission. This scope does not include Spot, COIN-M, Portfolio Margin,
//! SAPI advanced-algo bots, other subaccounts, or exchange-managed strategies.

use std::collections::HashSet;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use reqwest::Method;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::{
    InventoryAdapter, InventoryKind, InventoryPage, InventoryPartition, InventoryRecord,
    InventoryScope,
};
use crate::execution::{hmac_sha256_hex, ApiCredentials, ExecutionError, SignedRequest};
use crate::Exchange;

const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_ROWS: usize = 20_000;

/// Read-only Binance USD-M inventory request signing and response parsing.
///
/// All requests omit symbol/order filters. Transport scheduling, rate limits,
/// account authentication, and continuity checks remain the caller's concern.
#[derive(Clone, Debug)]
pub struct BinanceInventory {
    recv_window_millis: u64,
}

impl Default for BinanceInventory {
    fn default() -> Self {
        Self {
            recv_window_millis: 5_000,
        }
    }
}

impl BinanceInventory {
    /// Configures the signed-request validity window in milliseconds.
    ///
    /// # Errors
    /// Rejects zero or a window above the venue maximum of 60,000 milliseconds.
    pub fn new(recv_window_millis: u64) -> Result<Self, ExecutionError> {
        if !(1..=60_000).contains(&recv_window_millis) {
            return Err(ExecutionError::InvalidRequest(
                "invalid Binance inventory recvWindow",
            ));
        }
        Ok(Self { recv_window_millis })
    }
}

impl InventoryAdapter for BinanceInventory {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }

    fn scope(&self) -> InventoryScope {
        InventoryScope::BinanceUsdM
    }

    fn partitions(&self) -> Vec<InventoryPartition> {
        vec![
            InventoryPartition {
                key: "mode",
                kind: InventoryKind::Mode,
            },
            InventoryPartition {
                key: "orders",
                kind: InventoryKind::Orders,
            },
            InventoryPartition {
                key: "algo_orders",
                kind: InventoryKind::AlgoOrders,
            },
            InventoryPartition {
                key: "positions",
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
        let path = endpoint(partition)?;
        if cursor.is_some() {
            return Err(ExecutionError::InvalidRequest(
                "Binance inventory does not accept a cursor",
            ));
        }
        let timestamp = at.timestamp_millis();
        if timestamp < 0 {
            return Err(ExecutionError::Timestamp);
        }
        let unsigned = format!(
            "timestamp={timestamp}&recvWindow={}",
            self.recv_window_millis
        );
        let signature = hmac_sha256_hex(credentials.secret(), unsigned.as_bytes())?;
        SignedRequest::new(
            Method::GET,
            path.to_owned(),
            format!("{unsigned}&signature={signature}"),
            Bytes::new(),
            vec![("X-MBX-APIKEY".to_owned(), credentials.api_key().to_owned())],
        )
    }

    fn parse(
        &self,
        partition: &InventoryPartition,
        status: u16,
        body: &[u8],
    ) -> Result<InventoryPage, ExecutionError> {
        endpoint(partition)?;
        if status != 200 {
            return Err(ExecutionError::HttpStatus(status));
        }
        if body.len() > MAX_BODY_BYTES {
            return Err(ExecutionError::Protocol(
                "Binance inventory response exceeds size limit",
            ));
        }
        // Error messages may contain arbitrary text. Retain only the integral
        // error code; an error envelope is never interpreted as an empty page.
        if let Ok(error) = serde_json::from_slice::<ErrorResponse>(body) {
            return Err(ExecutionError::VenueRejected {
                code: error.code.to_string(),
            });
        }
        let records = match partition.kind {
            InventoryKind::Mode => {
                let mode: ModeResponse = serde_json::from_slice(body)?;
                vec![InventoryRecord::Mode {
                    one_way: !mode.dual_side_position,
                }]
            }
            InventoryKind::Orders => regular_orders(body)?,
            InventoryKind::AlgoOrders => algo_orders(body)?,
            InventoryKind::Positions => positions(body)?,
        };
        Ok(InventoryPage {
            records,
            next_cursor: None,
        })
    }
}

fn endpoint(partition: &InventoryPartition) -> Result<&'static str, ExecutionError> {
    match (partition.key, partition.kind) {
        ("mode", InventoryKind::Mode) => Ok("/fapi/v1/positionSide/dual"),
        ("orders", InventoryKind::Orders) => Ok("/fapi/v1/openOrders"),
        ("algo_orders", InventoryKind::AlgoOrders) => Ok("/fapi/v1/openAlgoOrders"),
        ("positions", InventoryKind::Positions) => Ok("/fapi/v3/positionRisk"),
        _ => Err(ExecutionError::InvalidRequest(
            "unknown Binance inventory partition",
        )),
    }
}

#[derive(Deserialize)]
struct ErrorResponse {
    code: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModeResponse {
    dual_side_position: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegularOrder {
    symbol: String,
    order_id: u64,
    // External/manual orders may have no client-provided identity. They still
    // remain visible as orders; absence must never cause a row to be skipped.
    client_order_id: Option<String>,
    status: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlgoOrder {
    symbol: String,
    algo_id: u64,
    client_algo_id: Option<String>,
    algo_type: String,
    algo_status: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionRow {
    symbol: String,
    position_side: String,
    #[serde(with = "crate::decimal_codec::required")]
    position_amt: Decimal,
}

fn regular_orders(body: &[u8]) -> Result<Vec<InventoryRecord>, ExecutionError> {
    let rows: Vec<RegularOrder> = serde_json::from_slice(body)?;
    check_count(rows.len())?;
    let mut identities = HashSet::new();
    rows.into_iter()
        .map(|row| {
            if !matches!(row.status.as_str(), "NEW" | "PARTIALLY_FILLED") {
                return Err(ExecutionError::Protocol(
                    "unknown or terminal Binance open-order status",
                ));
            }
            order_record(
                row.symbol,
                row.order_id,
                row.client_order_id,
                &mut identities,
            )
        })
        .collect()
}

fn algo_orders(body: &[u8]) -> Result<Vec<InventoryRecord>, ExecutionError> {
    let rows: Vec<AlgoOrder> = serde_json::from_slice(body)?;
    check_count(rows.len())?;
    let mut identities = HashSet::new();
    rows.into_iter()
        .map(|row| {
            if row.algo_type != "CONDITIONAL"
                || !matches!(row.algo_status.as_str(), "NEW" | "TRIGGERING" | "TRIGGERED")
            {
                return Err(ExecutionError::Protocol(
                    "unknown Binance open-algo type or status",
                ));
            }
            order_record(row.symbol, row.algo_id, row.client_algo_id, &mut identities)
        })
        .collect()
}

fn order_record(
    symbol: String,
    id: u64,
    client_id: Option<String>,
    identities: &mut HashSet<(String, u64)>,
) -> Result<InventoryRecord, ExecutionError> {
    validate_text(&symbol)?;
    if id == 0 || !identities.insert((symbol.clone(), id)) {
        return Err(ExecutionError::Protocol(
            "invalid or duplicate Binance inventory order identity",
        ));
    }
    let client_order_id = client_id.filter(|id| !id.is_empty());
    if let Some(id) = &client_order_id {
        validate_text(id)?;
    }
    Ok(InventoryRecord::Order {
        symbol,
        venue_order_id: id.to_string(),
        client_order_id,
    })
}

fn positions(body: &[u8]) -> Result<Vec<InventoryRecord>, ExecutionError> {
    let rows: Vec<PositionRow> = serde_json::from_slice(body)?;
    check_count(rows.len())?;
    let mut identities = HashSet::new();
    rows.into_iter()
        .map(|row| {
            validate_text(&row.symbol)?;
            let valid_side = match row.position_side.as_str() {
                "BOTH" => true,
                "LONG" => row.position_amt >= Decimal::ZERO,
                "SHORT" => row.position_amt <= Decimal::ZERO,
                _ => false,
            };
            let position_id = format!("{}:{}", row.symbol, row.position_side);
            if !valid_side || !identities.insert(position_id.clone()) {
                return Err(ExecutionError::Protocol(
                    "invalid or duplicate Binance inventory position identity",
                ));
            }
            // Do not net LONG and SHORT rows or discard zero records. Opposite
            // hedged positions must never appear flat because their sum is zero.
            Ok(InventoryRecord::Position {
                symbol: row.symbol,
                position_id,
                quantity: row.position_amt,
            })
        })
        .collect()
}

fn validate_text(value: &str) -> Result<(), ExecutionError> {
    if value.is_empty() || value.len() > 128 || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(ExecutionError::Protocol(
            "invalid Binance inventory identifier",
        ));
    }
    Ok(())
}

fn check_count(count: usize) -> Result<(), ExecutionError> {
    if count > MAX_ROWS {
        return Err(ExecutionError::Protocol(
            "Binance inventory record limit exceeded",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    fn partition(key: &'static str, kind: InventoryKind) -> InventoryPartition {
        InventoryPartition { key, kind }
    }

    fn credentials() -> Result<ApiCredentials, ExecutionError> {
        ApiCredentials::new("test-api-key".to_owned(), "test-secret".to_owned(), None)
    }

    fn at() -> Result<DateTime<Utc>, Box<dyn Error>> {
        DateTime::from_timestamp_millis(1_700_000_000_000)
            .ok_or_else(|| "invalid fixture time".into())
    }

    #[test]
    fn inventory_signs_get_only_without_symbol_or_order_filters() -> TestResult {
        let adapter = BinanceInventory::default();
        let credentials = credentials()?;
        let unsigned = "timestamp=1700000000000&recvWindow=5000";
        let signature = hmac_sha256_hex("test-secret", unsigned.as_bytes())?;
        assert_eq!(adapter.exchange(), Exchange::Binance);
        assert_eq!(adapter.scope(), InventoryScope::BinanceUsdM);
        let partitions = adapter.partitions();
        assert_eq!(partitions.len(), 4);
        for partition in partitions {
            let request = adapter.prepare(&partition, None, &credentials, at()?)?;
            assert_eq!(request.method(), &Method::GET);
            assert_eq!(request.path(), endpoint(&partition)?);
            assert_eq!(request.query(), format!("{unsigned}&signature={signature}"));
            assert!(request.body().is_empty());
            assert_eq!(
                request.headers(),
                &[("X-MBX-APIKEY".to_owned(), "test-api-key".to_owned())]
            );
        }
        Ok(())
    }

    #[test]
    fn invalid_partition_cursor_timestamp_or_window_is_rejected() -> TestResult {
        let adapter = BinanceInventory::default();
        let credentials = credentials()?;
        let orders = partition("orders", InventoryKind::Orders);
        assert!(adapter
            .prepare(&orders, Some(""), &credentials, at()?)
            .is_err());
        assert!(adapter
            .prepare(&orders, Some("next"), &credentials, at()?)
            .is_err());
        assert!(adapter
            .prepare(
                &partition("orders", InventoryKind::Positions),
                None,
                &credentials,
                at()?
            )
            .is_err());
        assert!(adapter
            .parse(&partition("unknown", InventoryKind::Mode), 200, b"{}")
            .is_err());
        let before_epoch = DateTime::from_timestamp(-1, 0).ok_or("invalid fixture time")?;
        assert!(adapter
            .prepare(&orders, None, &credentials, before_epoch)
            .is_err());
        assert!(BinanceInventory::new(0).is_err());
        assert!(BinanceInventory::new(60_001).is_err());
        assert!(BinanceInventory::new(60_000).is_ok());
        Ok(())
    }

    #[test]
    fn mode_requires_an_explicit_boolean_and_keeps_hedge_evidence() -> TestResult {
        let adapter = BinanceInventory::default();
        let mode = partition("mode", InventoryKind::Mode);
        let one_way = adapter.parse(&mode, 200, br#"{"dualSidePosition":false}"#)?;
        assert!(matches!(
            one_way.records.as_slice(),
            [InventoryRecord::Mode { one_way: true }]
        ));
        let hedge = adapter.parse(&mode, 200, br#"{"dualSidePosition":true}"#)?;
        assert!(matches!(
            hedge.records.as_slice(),
            [InventoryRecord::Mode { one_way: false }]
        ));
        for invalid in [
            "{}",
            "[]",
            r#"{"dualSidePosition":null}"#,
            r#"{"dualSidePosition":"false"}"#,
            r#"{"dualSidePosition":false,"dualSidePosition":true}"#,
            r#"{"unknownMode":false}"#,
        ] {
            assert!(adapter.parse(&mode, 200, invalid.as_bytes()).is_err());
        }
        Ok(())
    }

    #[test]
    fn empty_arrays_are_complete_pages_not_admission_proofs() -> TestResult {
        let adapter = BinanceInventory::default();
        for partition in adapter
            .partitions()
            .into_iter()
            .filter(|p| p.kind != InventoryKind::Mode)
        {
            let page = adapter.parse(&partition, 200, b"[]")?;
            assert!(page.records.is_empty());
            assert!(page.next_cursor.is_none());
            for invalid in [b"{}".as_slice(), b"null", b"{\"data\":[]}", b"["] {
                assert!(adapter.parse(&partition, 200, invalid).is_err());
            }
        }
        Ok(())
    }

    #[test]
    fn external_orders_without_client_ids_remain_visible_and_ids_are_exact() -> TestResult {
        let adapter = BinanceInventory::default();
        let orders = adapter.parse(
            &partition("orders", InventoryKind::Orders),
            200,
            br#"[{"symbol":"BTCUSDT","orderId":9223372036854775807,"status":"NEW"}]"#,
        )?;
        assert!(
            matches!(orders.records.as_slice(), [InventoryRecord::Order {
            symbol, venue_order_id, client_order_id: None,
        }] if symbol == "BTCUSDT" && venue_order_id == "9223372036854775807")
        );
        let algos = adapter.parse(&partition("algo_orders", InventoryKind::AlgoOrders), 200,
            br#"[{"symbol":"ETHUSDT","algoId":42,"clientAlgoId":"","algoType":"CONDITIONAL","algoStatus":"NEW"}]"#)?;
        assert!(matches!(
            algos.records.as_slice(),
            [InventoryRecord::Order {
                client_order_id: None,
                ..
            }]
        ));
        Ok(())
    }

    #[test]
    fn positions_preserve_signed_quantities_and_hedge_legs_without_netting() -> TestResult {
        let page = BinanceInventory::default().parse(&partition("positions", InventoryKind::Positions), 200,
            br#"[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0.1234567890123456789012345678"},{"symbol":"BTCUSDT","positionSide":"SHORT","positionAmt":"-0.1234567890123456789012345678"},{"symbol":"ETHUSDT","positionSide":"BOTH","positionAmt":"0"}]"#)?;
        assert_eq!(page.records.len(), 3);
        assert!(
            matches!(&page.records[0], InventoryRecord::Position { position_id, quantity, .. }
            if position_id == "BTCUSDT:LONG" && *quantity > Decimal::ZERO)
        );
        assert!(
            matches!(&page.records[1], InventoryRecord::Position { position_id, quantity, .. }
            if position_id == "BTCUSDT:SHORT" && *quantity < Decimal::ZERO)
        );
        assert!(
            matches!(&page.records[2], InventoryRecord::Position { quantity, .. } if *quantity == Decimal::ZERO)
        );
        Ok(())
    }

    #[test]
    fn malformed_or_rounded_position_evidence_is_rejected() {
        let adapter = BinanceInventory::default();
        let positions = partition("positions", InventoryKind::Positions);
        for amount in [
            "0.1",
            "null",
            r#""NaN""#,
            r#""0.12345678901234567890123456789""#,
            r#""1e-2""#,
        ] {
            let body =
                format!(r#"[{{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":{amount}}}]"#);
            assert!(adapter.parse(&positions, 200, body.as_bytes()).is_err());
        }
        for invalid in [
            r#"[{"symbol":"BTCUSDT","positionSide":"BOTH"}]"#,
            r#"[{"symbol":"BTCUSDT","positionSide":"UNKNOWN","positionAmt":"0"}]"#,
            r#"[{"symbol":"BTCUSDT","positionSide":"SHORT","positionAmt":"1"}]"#,
            r#"[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"1"},{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"1"}]"#,
        ] {
            assert!(adapter.parse(&positions, 200, invalid.as_bytes()).is_err());
        }
    }

    #[test]
    fn rejects_http_errors_error_envelopes_and_unknown_order_shapes() {
        let adapter = BinanceInventory::default();
        let orders = partition("orders", InventoryKind::Orders);
        for status in [201, 400, 429, 500] {
            assert!(adapter.parse(&orders, status, b"[]").is_err());
        }
        assert!(
            matches!(adapter.parse(&orders, 200, br#"{"code":-1021,"msg":"untrusted diagnostic"}"#),
            Err(ExecutionError::VenueRejected { code }) if code == "-1021")
        );
        for invalid in [
            r#"[{"symbol":"BTCUSDT","status":"NEW"}]"#,
            r#"[{"symbol":"BTCUSDT","orderId":1,"status":"MYSTERY"}]"#,
            r#"[{"symbol":"BTCUSDT","orderId":1.5,"status":"NEW"}]"#,
            r#"[{"symbol":"BTCUSDT","orderId":1,"status":"NEW"},{"symbol":"BTCUSDT","orderId":1,"status":"NEW"}]"#,
        ] {
            assert!(adapter.parse(&orders, 200, invalid.as_bytes()).is_err());
        }
    }
}
