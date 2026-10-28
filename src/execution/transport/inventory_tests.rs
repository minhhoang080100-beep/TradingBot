#![cfg(any(feature = "binance", feature = "bybit", feature = "okx"))]

use std::{error::Error, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use wiremock::{matchers::method, Mock, MockServer, Request, ResponseTemplate};

use super::{ApiCredentials, ExecutionAdapter, ExecutionEnvironment, ExecutionError, RestExecutor};
use crate::InventoryLimits;

#[cfg(feature = "binance")]
use crate::{execution::binance::BinanceExecution, reconciliation::binance::BinanceInventory};
#[cfg(feature = "bybit")]
use crate::{execution::bybit::BybitExecution, reconciliation::bybit::BybitInventory};
#[cfg(feature = "okx")]
use crate::{execution::okx::OkxExecution, reconciliation::okx::OkxInventory};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn executor(
    server: &MockServer,
    adapter: Arc<dyn ExecutionAdapter>,
    environment: ExecutionEnvironment,
) -> Result<RestExecutor, ExecutionError> {
    let mut executor = RestExecutor::new(
        adapter,
        Arc::new(ApiCredentials::new(
            "inventory-test-key".to_owned(),
            "inventory-test-secret".to_owned(),
            Some("inventory-test-passphrase".to_owned()),
        )?),
        environment,
        Duration::from_secs(2),
    )?;
    // The production constructor pins official origins. Only this nested test
    // module may replace the private origin with its own local mock server.
    executor.origin = server.uri();
    Ok(executor)
}

fn header<'a>(request: &'a Request, name: &str) -> Result<&'a str, Box<dyn Error + Send + Sync>> {
    Ok(request
        .headers
        .get(name)
        .ok_or("missing signed header")?
        .to_str()?)
}

#[cfg(feature = "binance")]
#[tokio::test]
async fn binance_inventory_fetches_all_unfiltered_signed_get_partitions() -> TestResult {
    let server = MockServer::start().await;
    for endpoint in [
        "/fapi/v1/positionSide/dual",
        "/fapi/v1/openOrders",
        "/fapi/v1/openAlgoOrders",
        "/fapi/v3/positionRisk",
    ] {
        let body = if endpoint.ends_with("dual") {
            r#"{"dualSidePosition":false}"#
        } else {
            "[]"
        };
        Mock::given(method("GET"))
            .and(wiremock::matchers::path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .expect(1)
            .mount(&server)
            .await;
    }
    let executor = executor(
        &server,
        Arc::new(BinanceExecution::default()),
        ExecutionEnvironment::Demo,
    )?;
    let report = executor
        .inventory(
            &BinanceInventory::default(),
            InventoryLimits::default(),
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(report.pages(), 4);
    assert_eq!(report.scope(), crate::InventoryScope::BinanceUsdM);
    assert_eq!(report.one_way_mode(), Some(true));
    let requests = server
        .received_requests()
        .await
        .ok_or("request recording disabled")?;
    assert_eq!(requests.len(), 4);
    for request in requests {
        assert_eq!(request.method.as_str(), "GET");
        assert!(request.body.is_empty());
        assert_eq!(header(&request, "X-MBX-APIKEY")?, "inventory-test-key");
        let query = request.url.query().ok_or("missing query")?;
        let (unsigned, signature) = query
            .rsplit_once("&signature=")
            .ok_or("missing signature")?;
        assert_eq!(
            signature,
            crate::execution::hmac_sha256_hex("inventory-test-secret", unsigned.as_bytes())?
        );
        let names: Vec<_> = request
            .url
            .query_pairs()
            .map(|(key, _)| key.into_owned())
            .collect();
        assert_eq!(names, ["timestamp", "recvWindow", "signature"]);
    }
    Ok(())
}

#[cfg(feature = "bybit")]
fn bybit_page(request: &Request) -> ResponseTemplate {
    let query: std::collections::HashMap<_, _> = request.url.query_pairs().into_owned().collect();
    let coin = query.get("settleCoin").map(String::as_str);
    let filter = query.get("orderFilter").map(String::as_str);
    let cursor = query.get("cursor").map(String::as_str);
    let mut list = Vec::new();
    let mut next = "";
    if query.get("category").map(String::as_str) != Some("linear")
        || !matches!(coin, Some("USDT" | "USDC"))
    {
        return ResponseTemplate::new(400);
    }
    match request.url.path() {
        "/v5/order/realtime" if matches!(filter, Some("Order" | "StopOrder")) => {
            if coin == Some("USDT") && filter == Some("Order") {
                let id = match cursor {
                    None => {
                        next = "opaque+/=cursor";
                        "venue-1"
                    }
                    Some("opaque+/=cursor") => "venue-2",
                    _ => return ResponseTemplate::new(400),
                };
                list.push(serde_json::json!({"symbol":"BTCUSDT","orderId":id,"orderStatus":"New","positionIdx":0}));
            }
        }
        "/v5/position/list" if filter.is_none() => {}
        _ => return ResponseTemplate::new(400),
    }
    ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "retCode":0,"result":{"category":"linear","nextPageCursor":next,"list":list}
    }))
}

#[cfg(feature = "bybit")]
#[tokio::test]
async fn bybit_inventory_exhausts_six_filters_and_signs_encoded_cursor() -> TestResult {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(bybit_page)
        .expect(7)
        .mount(&server)
        .await;
    let executor = executor(
        &server,
        Arc::new(BybitExecution),
        ExecutionEnvironment::Demo,
    )?;
    let report = executor
        .inventory(
            &BybitInventory,
            InventoryLimits::default(),
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(report.pages(), 7);
    assert_eq!(report.records().len(), 2);
    assert_eq!(report.one_way_mode(), None);
    let requests = server
        .received_requests()
        .await
        .ok_or("request recording disabled")?;
    let mut partitions = std::collections::HashSet::new();
    let mut cursor_pages = 0;
    for request in requests {
        assert!(request.body.is_empty());
        let query = request.url.query().ok_or("missing query")?;
        let payload = format!(
            "{}{}{}{}",
            header(&request, "X-BAPI-TIMESTAMP")?,
            header(&request, "X-BAPI-API-KEY")?,
            header(&request, "X-BAPI-RECV-WINDOW")?,
            query
        );
        assert_eq!(
            header(&request, "X-BAPI-SIGN")?,
            crate::execution::hmac_sha256_hex("inventory-test-secret", payload.as_bytes())?
        );
        let fields: std::collections::HashMap<_, _> =
            request.url.query_pairs().into_owned().collect();
        let coin = fields
            .get("settleCoin")
            .ok_or("missing settlement filter")?;
        let filter = fields
            .get("orderFilter")
            .map_or("positions", String::as_str);
        partitions.insert((coin.clone(), filter.to_owned()));
        assert!(!fields.contains_key("symbol"));
        if filter == "positions" {
            assert_eq!(fields.get("limit").map(String::as_str), Some("200"));
        } else {
            assert_eq!(fields.get("limit").map(String::as_str), Some("50"));
            assert_eq!(fields.get("openOnly").map(String::as_str), Some("0"));
        }
        if let Some(cursor) = fields.get("cursor") {
            assert_eq!(cursor, "opaque+/=cursor");
            cursor_pages += 1;
        }
    }
    assert_eq!(partitions.len(), 6);
    assert_eq!(cursor_pages, 1);
    Ok(())
}

#[cfg(feature = "okx")]
#[tokio::test]
async fn okx_inventory_fetches_all_algo_families_with_exact_demo_signatures() -> TestResult {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(|request: &Request| {
            let data = if request.url.path() == "/api/v5/account/config" {
                serde_json::json!([{"posMode":"net_mode","acctLv":"2"}])
            } else {
                serde_json::json!([])
            };
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"code":"0","data":data}))
        })
        .expect(11)
        .mount(&server)
        .await;
    let executor = executor(
        &server,
        Arc::new(OkxExecution::new(true)),
        ExecutionEnvironment::Demo,
    )?;
    let report = executor
        .inventory(
            &OkxInventory::new(true),
            InventoryLimits::default(),
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(report.pages(), 11);
    assert_eq!(report.one_way_mode(), Some(true));
    let requests = server
        .received_requests()
        .await
        .ok_or("request recording disabled")?;
    let mut families = std::collections::HashSet::new();
    for request in requests {
        assert!(request.body.is_empty());
        assert_eq!(header(&request, "x-simulated-trading")?, "1");
        assert_eq!(header(&request, "OK-ACCESS-KEY")?, "inventory-test-key");
        assert_eq!(
            header(&request, "OK-ACCESS-PASSPHRASE")?,
            "inventory-test-passphrase"
        );
        let mut target = request.url.path().to_owned();
        if let Some(query) = request.url.query() {
            target.push('?');
            target.push_str(query);
        }
        let payload = format!("{}GET{target}", header(&request, "OK-ACCESS-TIMESTAMP")?);
        assert_eq!(
            header(&request, "OK-ACCESS-SIGN")?,
            crate::execution::hmac_sha256_base64("inventory-test-secret", payload.as_bytes())?
        );
        let query: std::collections::HashMap<_, _> =
            request.url.query_pairs().into_owned().collect();
        assert!(!query.contains_key("instId"));
        assert!(!query.contains_key("instType"));
        if request.url.path() == "/api/v5/trade/orders-algo-pending" {
            families.insert(query.get("ordType").ok_or("missing algo family")?.clone());
            assert_eq!(query.get("limit").map(String::as_str), Some("100"));
        }
    }
    assert_eq!(families.len(), 8);
    Ok(())
}

#[cfg(feature = "binance")]
#[tokio::test]
async fn inventory_http_failure_returns_no_partial_report_and_is_not_retried() -> TestResult {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::path("/fapi/v1/positionSide/dual"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"dualSidePosition":false}"#))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::path("/fapi/v1/openOrders"))
        .respond_with(ResponseTemplate::new(500).set_body_string("private diagnostic"))
        .expect(1)
        .mount(&server)
        .await;
    let executor = executor(
        &server,
        Arc::new(BinanceExecution::default()),
        ExecutionEnvironment::Demo,
    )?;
    assert!(matches!(
        executor
            .inventory(
                &BinanceInventory::default(),
                InventoryLimits::default(),
                &CancellationToken::new()
            )
            .await,
        Err(crate::InventoryError::Execution(
            ExecutionError::HttpStatus(500)
        ))
    ));
    assert_eq!(
        server
            .received_requests()
            .await
            .ok_or("request recording disabled")?
            .len(),
        2
    );
    Ok(())
}

#[cfg(all(feature = "binance", feature = "bybit"))]
#[tokio::test]
async fn inventory_venue_mismatch_fails_before_http_or_credentials_leave_client() -> TestResult {
    let server = MockServer::start().await;
    let executor = executor(
        &server,
        Arc::new(BinanceExecution::default()),
        ExecutionEnvironment::Demo,
    )?;
    assert!(matches!(
        executor
            .inventory(
                &BybitInventory,
                InventoryLimits::default(),
                &CancellationToken::new()
            )
            .await,
        Err(crate::InventoryError::WrongVenue)
    ));
    assert!(server
        .received_requests()
        .await
        .ok_or("request recording disabled")?
        .is_empty());
    Ok(())
}

#[cfg(feature = "okx")]
#[tokio::test]
async fn inventory_okx_demo_mismatch_fails_before_any_http_request() -> TestResult {
    let server = MockServer::start().await;
    for environment in [ExecutionEnvironment::Demo, ExecutionEnvironment::Production] {
        let demo = matches!(environment, ExecutionEnvironment::Demo);
        let executor = executor(&server, Arc::new(OkxExecution::new(demo)), environment)?;
        assert!(matches!(
            executor
                .inventory(
                    &OkxInventory::new(!demo),
                    InventoryLimits::default(),
                    &CancellationToken::new()
                )
                .await,
            Err(crate::InventoryError::Execution(
                ExecutionError::InvalidRequest(_)
            ))
        ));
    }
    assert!(server
        .received_requests()
        .await
        .ok_or("request recording disabled")?
        .is_empty());
    Ok(())
}

#[cfg(feature = "binance")]
#[tokio::test]
async fn pre_cancelled_inventory_scan_never_starts_http() -> TestResult {
    let server = MockServer::start().await;
    let executor = executor(
        &server,
        Arc::new(BinanceExecution::default()),
        ExecutionEnvironment::Demo,
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        executor
            .inventory(
                &BinanceInventory::default(),
                InventoryLimits::default(),
                &cancellation
            )
            .await,
        Err(crate::InventoryError::Cancelled)
    ));
    assert!(server
        .received_requests()
        .await
        .ok_or("request recording disabled")?
        .is_empty());
    Ok(())
}
