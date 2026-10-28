use std::{
    collections::VecDeque,
    error::Error,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use super::*;
use crate::execution::coordinator::tests::{at, flat, instrument, intent, update};
use crate::{
    ExecutionAction, InstrumentRiskSpec, LiquiditySide, LiquidityTarget, RiskConfig, RiskManager,
    TradeAssessment,
};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error>>;

struct Adapter {
    pages: Mutex<VecDeque<InventoryPage>>,
    method: Method,
}

impl InventoryAdapter for Adapter {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }
    fn scope(&self) -> InventoryScope {
        InventoryScope::BinanceUsdM
    }
    fn partitions(&self) -> Vec<InventoryPartition> {
        partitions()
    }
    fn prepare(
        &self,
        partition: &InventoryPartition,
        cursor: Option<&str>,
        _: &ApiCredentials,
        _: DateTime<Utc>,
    ) -> Result<SignedRequest, ExecutionError> {
        SignedRequest::new(
            self.method.clone(),
            format!("/{}", partition.key),
            cursor.unwrap_or_default().to_owned(),
            Bytes::new(),
            Vec::new(),
        )
    }
    fn parse(
        &self,
        _: &InventoryPartition,
        _: u16,
        _: &[u8],
    ) -> Result<InventoryPage, ExecutionError> {
        self.pages
            .lock()
            .map_err(|_| ExecutionError::Protocol("test lock"))?
            .pop_front()
            .ok_or(ExecutionError::Protocol("missing test page"))
    }
}

struct Transport {
    calls: AtomicUsize,
    delay: Duration,
    status: u16,
    bytes: usize,
}

impl Default for Transport {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            delay: Duration::ZERO,
            status: 200,
            bytes: 0,
        }
    }
}

#[async_trait]
impl InventoryTransport for Transport {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }
    async fn fetch(&self, _: &SignedRequest) -> Result<InventoryResponse, ExecutionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(InventoryResponse {
            status: self.status,
            body: Bytes::from(vec![b' '; self.bytes]),
        })
    }
}

fn partitions() -> Vec<InventoryPartition> {
    vec![
        InventoryPartition {
            key: "orders",
            kind: InventoryKind::Orders,
        },
        InventoryPartition {
            key: "algos",
            kind: InventoryKind::AlgoOrders,
        },
        InventoryPartition {
            key: "positions",
            kind: InventoryKind::Positions,
        },
    ]
}

fn empty() -> InventoryPage {
    InventoryPage {
        records: Vec::new(),
        next_cursor: None,
    }
}
fn adapter(pages: Vec<InventoryPage>) -> Adapter {
    Adapter {
        pages: Mutex::new(pages.into()),
        method: Method::GET,
    }
}
fn credentials() -> Result<ApiCredentials, ExecutionError> {
    ApiCredentials::new("mock-key".to_owned(), "mock-secret".to_owned(), None)
}
fn order(id: &str) -> InventoryRecord {
    InventoryRecord::Order {
        symbol: "BTCUSDT".to_owned(),
        venue_order_id: id.to_owned(),
        client_order_id: None,
    }
}

#[tokio::test]
async fn paginated_empty_inventory_is_not_a_flat_account_certificate() -> TestResult {
    let adapter = adapter(vec![
        InventoryPage {
            records: vec![order("1")],
            next_cursor: Some("next".to_owned()),
        },
        empty(),
        empty(),
        empty(),
    ]);
    let transport = Transport::default();
    let report = scan_inventory(
        &adapter,
        &credentials()?,
        &transport,
        InventoryLimits::default(),
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(report.pages(), 4);
    assert_eq!(transport.calls.load(Ordering::SeqCst), 4);
    let execution = ExecutionCoordinator::new(Exchange::Binance);
    let review = report.review(&execution, Utc::now(), Duration::from_secs(5))?;
    assert_eq!(review.open_orders(), 1);
    assert_eq!(review.unmatched_orders().len(), 1);
    assert_eq!(review.one_way_mode(), None);
    assert!(!execution.is_ready());
    assert!(!format!("{report:?}").contains("BTCUSDT"));
    assert!(!format!(
        "{:?}",
        InventoryResponse {
            status: 200,
            body: Bytes::from_static(b"private-body")
        }
    )
    .contains("private-body"));
    Ok(())
}

#[tokio::test]
async fn repeated_cursor_and_duplicate_rows_fail_without_partial_report() -> TestResult {
    for duplicate in [false, true] {
        let first = InventoryPage {
            records: vec![order("1")],
            next_cursor: Some("again".to_owned()),
        };
        let second = InventoryPage {
            records: if duplicate {
                vec![order("1")]
            } else {
                Vec::new()
            },
            next_cursor: Some("again".to_owned()),
        };
        let result = scan_inventory(
            &adapter(vec![first, second]),
            &credentials()?,
            &Transport::default(),
            InventoryLimits::default(),
            &CancellationToken::new(),
        )
        .await;
        assert!(matches!(
            result,
            Err(InventoryError::InvalidPagination | InventoryError::DuplicateRecord)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn caps_and_page_errors_never_fall_back_to_empty_inventory() -> TestResult {
    let limits = InventoryLimits::new(1, 1, Duration::from_secs(1))?;
    let capped = InventoryPage {
        records: Vec::new(),
        next_cursor: Some("next".to_owned()),
    };
    assert!(matches!(
        scan_inventory(
            &adapter(vec![capped]),
            &credentials()?,
            &Transport::default(),
            limits,
            &CancellationToken::new()
        )
        .await,
        Err(InventoryError::Capacity)
    ));
    let rows = InventoryPage {
        records: vec![order("1"), order("2")],
        next_cursor: None,
    };
    assert!(matches!(
        scan_inventory(
            &adapter(vec![rows]),
            &credentials()?,
            &Transport::default(),
            limits,
            &CancellationToken::new()
        )
        .await,
        Err(InventoryError::Capacity)
    ));
    for status in [302, 429, 500] {
        let transport = Transport {
            status,
            ..Transport::default()
        };
        assert!(matches!(
            scan_inventory(
                &adapter(vec![empty()]),
                &credentials()?,
                &transport,
                limits,
                &CancellationToken::new()
            )
            .await,
            Err(InventoryError::Execution(ExecutionError::HttpStatus(_)))
        ));
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
    }
    let transport = Transport {
        bytes: 1_048_577,
        ..Transport::default()
    };
    assert!(matches!(
        scan_inventory(
            &adapter(vec![empty()]),
            &credentials()?,
            &transport,
            limits,
            &CancellationToken::new()
        )
        .await,
        Err(InventoryError::Capacity)
    ));
    assert!(matches!(
        scan_inventory(
            &adapter(vec![]),
            &credentials()?,
            &Transport::default(),
            limits,
            &CancellationToken::new()
        )
        .await,
        Err(InventoryError::Execution(_))
    ));
    Ok(())
}

#[tokio::test]
async fn only_get_is_allowed_and_cancellation_never_returns_a_partial_report() -> TestResult {
    let mut adapter = adapter(vec![empty(), empty(), empty()]);
    adapter.method = Method::POST;
    let transport = Transport::default();
    assert!(matches!(
        scan_inventory(
            &adapter,
            &credentials()?,
            &transport,
            InventoryLimits::default(),
            &CancellationToken::new()
        )
        .await,
        Err(InventoryError::NotReadOnly)
    ));
    assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
    adapter.method = Method::GET;
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        scan_inventory(
            &adapter,
            &credentials()?,
            &transport,
            InventoryLimits::default(),
            &cancel
        )
        .await,
        Err(InventoryError::Cancelled)
    ));
    assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
    let transport = Transport {
        delay: Duration::from_secs(1),
        ..Transport::default()
    };
    let credentials = credentials()?;
    let cancel = CancellationToken::new();
    let scan = scan_inventory(
        &adapter,
        &credentials,
        &transport,
        InventoryLimits::default(),
        &cancel,
    );
    let interrupt = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(scan, interrupt);
    assert!(matches!(result, Err(InventoryError::Cancelled)));
    let limits = InventoryLimits::new(1, 5, Duration::from_millis(10))?;
    assert!(matches!(
        scan_inventory(
            &adapter,
            &credentials,
            &transport,
            limits,
            &CancellationToken::new()
        )
        .await,
        Err(InventoryError::Timeout)
    ));
    Ok(())
}

#[test]
fn plan_records_and_limits_validate_without_default_success() {
    assert!(InventoryLimits::new(0, 1, Duration::from_secs(1)).is_err());
    assert!(InventoryLimits::new(1, 0, Duration::from_secs(1)).is_err());
    assert!(InventoryLimits::new(1, 1, Duration::ZERO).is_err());
    assert!(InventoryLimits::new(1, 1, Duration::from_secs(121)).is_err());
    assert!(validate_partitions(&[]).is_err());
    let mut plan = partitions();
    plan.push(plan[0]);
    assert!(validate_partitions(&plan).is_err());
    let mut seen = HashSet::new();
    let mut mode = None;
    assert!(validate_record(&partitions()[2], &order("1"), &mut seen, &mut mode).is_err());
    let mut first = order("1");
    if let InventoryRecord::Order {
        client_order_id, ..
    } = &mut first
    {
        *client_order_id = Some("shared".to_owned());
    }
    assert!(validate_record(&partitions()[0], &first, &mut seen, &mut mode).is_ok());
    if let InventoryRecord::Order { venue_order_id, .. } = &mut first {
        *venue_order_id = "2".to_owned();
    }
    assert!(matches!(
        validate_record(&partitions()[0], &first, &mut seen, &mut mode),
        Err(InventoryError::DuplicateRecord)
    ));
    assert!(validate_record(
        &partitions()[2],
        &InventoryRecord::Mode { one_way: true },
        &mut seen,
        &mut mode
    )
    .is_err());
    let dedicated_mode = InventoryPartition {
        key: "mode",
        kind: InventoryKind::Mode,
    };
    assert!(validate_record(
        &dedicated_mode,
        &InventoryRecord::Mode { one_way: true },
        &mut seen,
        &mut mode
    )
    .is_ok());
    assert!(validate_record(
        &partitions()[2],
        &InventoryRecord::Mode { one_way: false },
        &mut seen,
        &mut mode
    )
    .is_err());
}

fn local_execution(
) -> Result<(RiskManager, ExecutionCoordinator, crate::OrderRequest), Box<dyn Error>> {
    let mut risk = RiskManager::new(RiskConfig::default());
    risk.register_instrument_spec(InstrumentRiskSpec::new(
        instrument(Exchange::Binance)?,
        Decimal::new(1, 1),
        Decimal::new(1, 3),
        Decimal::new(1, 3),
        None,
        Decimal::ONE,
    )?)?;
    risk.start_trading_day(
        Decimal::new(10_000, 0),
        Decimal::new(10_000, 0),
        at(299)?,
        false,
    )?;
    let target = LiquidityTarget::new(
        Uuid::from_u128(14),
        instrument(Exchange::Binance)?,
        LiquiditySide::BuySide,
        Decimal::new(112, 0),
        at(240)?,
    )?;
    let TradeAssessment::Approved(plan) = risk.assess_trade(
        &intent(Uuid::from_u128(13), Exchange::Binance)?,
        &target,
        at(300)?,
    )?
    else {
        return Err("expected approval".into());
    };
    let mut execution = ExecutionCoordinator::new(Exchange::Binance);
    execution.reconcile_flat(flat()?, at(300)?)?;
    let request = execution
        .approve(*plan, at(300)?)?
        .into_iter()
        .find_map(|action| match action {
            ExecutionAction::Dispatch(crate::OrderCommand::Place(request)) => Some(request),
            _ => None,
        })
        .ok_or("expected entry")?;
    execution.halt();
    Ok((risk, execution, request))
}

#[test]
fn missing_local_orders_require_queries_and_do_not_release_risk() -> TestResult {
    let (risk, execution, request) = local_execution()?;
    let reserved = risk.active_reserved_risk()?;
    let mut report = InventoryReport {
        exchange: Exchange::Binance,
        scope: InventoryScope::BinanceUsdM,
        started_at: at(300)?,
        finished_at: at(301)?,
        pages: 3,
        records: Vec::new(),
    };
    let review = report.review(&execution, at(302)?, Duration::from_secs(5))?;
    assert_eq!(review.query_orders().len(), 1);
    assert_eq!(review.absent_from_open_lists().len(), 1);
    assert_eq!(risk.active_reserved_risk()?, reserved);
    assert!(!execution.is_ready());
    report.records.push(ObservedInventoryRecord {
        partition: partitions()[0],
        record: InventoryRecord::Order {
            symbol: request.instrument.venue_symbol.to_string(),
            venue_order_id: "entry-venue-id".to_owned(),
            client_order_id: Some(request.client_order_id),
        },
    });
    let review = report.review(&execution, at(302)?, Duration::from_secs(5))?;
    assert_eq!(review.query_orders().len(), 1);
    assert!(review.absent_from_open_lists().is_empty());
    assert!(review.unmatched_orders().is_empty());
    assert!(report
        .review(&execution, at(306)?, Duration::from_secs(5))
        .is_err());
    assert!(report
        .review(&execution, at(299)?, Duration::from_secs(5))
        .is_err());
    assert!(report
        .review(
            &ExecutionCoordinator::new(Exchange::Okx),
            at(302)?,
            Duration::from_secs(5)
        )
        .is_err());
    Ok(())
}

#[test]
fn observed_released_order_is_known_and_requires_fresh_cumulative_query() -> TestResult {
    let (mut risk, mut execution, request) = local_execution()?;
    let actions = execution.on_user_event(&update(
        &request,
        crate::OrderStatus::Cancelled,
        Decimal::ZERO,
        301,
    )?)?;
    for action in actions {
        if let ExecutionAction::Lifecycle {
            intent_id,
            event,
            at,
            ..
        } = action
        {
            risk.apply_reservation_event(intent_id, event, at)?;
        }
    }
    assert!(execution.reconciliation_orders().is_empty());
    assert_eq!(risk.active_reserved_risk()?, Decimal::ZERO);
    let risk_before = risk.checkpoint();
    let execution_before = execution.checkpoint();
    let mut report = InventoryReport {
        exchange: Exchange::Binance,
        scope: InventoryScope::BinanceUsdM,
        started_at: at(302)?,
        finished_at: at(303)?,
        pages: 3,
        records: Vec::new(),
    };
    assert!(report
        .review(&execution, at(304)?, Duration::from_secs(5))?
        .query_orders()
        .is_empty());
    report.records.push(ObservedInventoryRecord {
        partition: partitions()[0],
        record: InventoryRecord::Order {
            symbol: request.instrument.venue_symbol.to_string(),
            venue_order_id: format!("venue{}", request.client_order_id),
            client_order_id: Some(request.client_order_id.clone()),
        },
    });
    let review = report.review(&execution, at(304)?, Duration::from_secs(5))?;
    assert_eq!(review.query_orders().len(), 1);
    assert_eq!(
        review.query_orders()[0].client_order_id,
        request.client_order_id
    );
    assert!(review.unmatched_orders().is_empty());
    assert!(review.absent_from_open_lists().is_empty());
    assert_eq!(review.open_orders(), 1);
    // An open-list sighting alone cannot fabricate a fill or reopen/release risk.
    assert_eq!(risk.checkpoint(), risk_before);
    assert_eq!(execution.checkpoint(), execution_before);
    assert!(!execution.is_ready());
    Ok(())
}

#[test]
fn conflicting_client_or_venue_identity_is_not_claimed_as_owned() -> TestResult {
    let (risk, mut execution, request) = local_execution()?;
    execution.on_user_event(&update(
        &request,
        crate::OrderStatus::New,
        Decimal::ZERO,
        301,
    )?)?;
    let known_venue = format!("venue{}", request.client_order_id);
    let before = risk.checkpoint();
    for (venue_id, client_id) in [
        (known_venue.clone(), Some("foreign-client".to_owned())),
        (
            "foreign-venue".to_owned(),
            Some(request.client_order_id.clone()),
        ),
    ] {
        let report = InventoryReport {
            exchange: Exchange::Binance,
            scope: InventoryScope::BinanceUsdM,
            started_at: at(302)?,
            finished_at: at(303)?,
            pages: 3,
            records: vec![ObservedInventoryRecord {
                partition: partitions()[0],
                record: InventoryRecord::Order {
                    symbol: request.instrument.venue_symbol.to_string(),
                    venue_order_id: venue_id,
                    client_order_id: client_id,
                },
            }],
        };
        let review = report.review(&execution, at(304)?, Duration::from_secs(5))?;
        assert_eq!(review.unmatched_orders().len(), 1);
        assert_eq!(review.absent_from_open_lists().len(), 1);
        assert_eq!(review.query_orders().len(), 1);
        assert_eq!(risk.checkpoint(), before);
    }
    Ok(())
}

struct TempJournal {
    directory: PathBuf,
    path: PathBuf,
}

impl TempJournal {
    fn new() -> Result<Self, std::io::Error> {
        let directory = std::env::temp_dir().join(format!("ict-inventory-test-{}", Uuid::new_v4()));
        fs::create_dir(&directory)?;
        let path = directory.join("account.journal");
        Ok(Self { directory, path })
    }
}

impl Drop for TempJournal {
    fn drop(&mut self) {
        // Only the exact file/directory created by this fixture; no recursion.
        let _removed_file = fs::remove_file(&self.path);
        let _removed_directory = fs::remove_dir(&self.directory);
    }
}

fn claim_durable_entry(
    account: &mut crate::DurableAccount,
) -> Result<crate::OrderRequest, Box<dyn Error>> {
    account.apply(crate::DurableInput::RegisterSpec(InstrumentRiskSpec::new(
        instrument(Exchange::Binance)?,
        Decimal::new(1, 1),
        Decimal::new(1, 3),
        Decimal::new(1, 3),
        None,
        Decimal::ONE,
    )?))?;
    account.apply(crate::DurableInput::StartDay {
        opening_equity: Decimal::new(10_000, 0),
        current_equity: Decimal::new(10_000, 0),
        at: at(299)?,
        already_tripped: false,
    })?;
    account.apply(crate::DurableInput::ReconcileFlat(flat()?, at(300)?))?;
    account.apply(crate::DurableInput::Assess {
        intent: Box::new(intent(Uuid::from_u128(13), Exchange::Binance)?),
        target: LiquidityTarget::new(
            Uuid::from_u128(14),
            instrument(Exchange::Binance)?,
            LiquiditySide::BuySide,
            Decimal::new(112, 0),
            at(240)?,
        )?,
        at: at(300)?,
    })?;
    let delivery = account.claim_next(at(300)?)?.ok_or("missing delivery")?;
    let ExecutionAction::Dispatch(crate::OrderCommand::Place(request)) = delivery.action else {
        return Err("expected entry placement".into());
    };
    Ok(request)
}

#[test]
fn durable_review_retains_unacknowledged_outbox_query_without_writing() -> TestResult {
    let temporary = TempJournal::new()?;
    let identity =
        crate::JournalIdentity::new(Exchange::Binance, "inventory-test".to_owned(), true)?;
    let mut account =
        crate::DurableAccount::create(&temporary.path, identity, RiskConfig::default())?;
    let report = InventoryReport {
        exchange: Exchange::Binance,
        scope: InventoryScope::BinanceUsdM,
        started_at: at(302)?,
        finished_at: at(303)?,
        pages: 3,
        records: Vec::new(),
    };
    let initial_sequence = account.sequence();
    assert!(report
        .review_account(&account, at(304)?, Duration::from_secs(5))?
        .query_orders()
        .is_empty());
    assert_eq!(account.sequence(), initial_sequence);
    assert!(!account.execution().is_ready());
    let request = claim_durable_entry(&mut account)?;
    // Private cancellation can precede REST ACK. The bracket is released but
    // the attempted outbox operation still requires independent reconciliation.
    account.apply(crate::DurableInput::User(update(
        &request,
        crate::OrderStatus::Cancelled,
        Decimal::ZERO,
        301,
    )?))?;
    account.apply(crate::DurableInput::Halt)?;
    assert!(account.execution().reconciliation_orders().is_empty());
    let before_sequence = account.sequence();
    let before_risk = account.risk().checkpoint();
    let before_execution = account.execution().checkpoint();
    let before_outbox = account.outbox().to_vec();
    let review = report.review_account(&account, at(304)?, Duration::from_secs(5))?;
    assert_eq!(review.query_orders().len(), 1);
    assert_eq!(
        review.query_orders()[0].client_order_id,
        request.client_order_id
    );
    assert_eq!(review.absent_from_open_lists().len(), 1);
    assert_eq!(account.sequence(), before_sequence);
    assert_eq!(account.risk().checkpoint(), before_risk);
    assert_eq!(account.execution().checkpoint(), before_execution);
    assert_eq!(account.outbox(), before_outbox.as_slice());
    assert!(!account.execution().is_ready());
    Ok(())
}

#[test]
fn opposing_hedge_positions_do_not_net_to_flat() -> TestResult {
    let report = InventoryReport {
        exchange: Exchange::Binance,
        scope: InventoryScope::BinanceUsdM,
        started_at: at(300)?,
        finished_at: at(300)?,
        pages: 3,
        records: [Decimal::ONE, -Decimal::ONE]
            .into_iter()
            .enumerate()
            .map(|(index, quantity)| ObservedInventoryRecord {
                partition: partitions()[2],
                record: InventoryRecord::Position {
                    symbol: "BTCUSDT".to_owned(),
                    position_id: index.to_string(),
                    quantity,
                },
            })
            .collect(),
    };
    assert_eq!(
        report
            .review(
                &ExecutionCoordinator::new(Exchange::Binance),
                at(301)?,
                Duration::from_secs(5)
            )?
            .nonzero_positions(),
        2
    );
    Ok(())
}
