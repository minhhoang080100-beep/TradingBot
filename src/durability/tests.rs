use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use super::*;
use crate::execution::coordinator::tests::{at, flat, instrument, intent, update};
use crate::execution::{OrderKind, OrderRequest, OrderStatus};
use crate::{Exchange, LiquiditySide, ReservationStatus};

type TestResult = Result<(), Box<dyn Error>>;

struct TempJournal {
    directory: PathBuf,
    path: PathBuf,
}

impl TempJournal {
    fn new() -> Result<Self, std::io::Error> {
        let directory = std::env::temp_dir().join(format!("ict-durable-test-{}", Uuid::new_v4()));
        fs::create_dir(&directory)?;
        let path = directory.join("account.journal");
        Ok(Self { directory, path })
    }
}

impl Drop for TempJournal {
    fn drop(&mut self) {
        // Only exact paths created by this fixture; never recursively remove a
        // shared temp root or any files another test/user may have supplied.
        let _removed_file = fs::remove_file(&self.path);
        let _removed_directory = fs::remove_dir(&self.directory);
    }
}

fn identity() -> Result<JournalIdentity, JournalError> {
    JournalIdentity::new(Exchange::Binance, "durability-test".to_owned(), true)
}

fn configured(path: &Path) -> Result<DurableAccount, Box<dyn Error>> {
    let mut account = DurableAccount::create(path, identity()?, RiskConfig::default())?;
    account.apply(DurableInput::RegisterSpec(InstrumentRiskSpec::new(
        instrument(Exchange::Binance)?,
        Decimal::new(1, 1),
        Decimal::new(1, 3),
        Decimal::new(1, 3),
        None,
        Decimal::ONE,
    )?))?;
    account.apply(DurableInput::StartDay {
        opening_equity: Decimal::new(10_000, 0),
        current_equity: Decimal::new(10_000, 0),
        at: at(299)?,
        already_tripped: false,
    })?;
    account.apply(DurableInput::ReconcileFlat(flat()?, at(300)?))?;
    Ok(account)
}

fn assess(account: &mut DurableAccount) -> TestResult {
    account.apply(DurableInput::Assess {
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
    Ok(())
}

fn queued_entry(account: &DurableAccount) -> Result<OrderRequest, Box<dyn Error>> {
    account
        .outbox()
        .iter()
        .find_map(|entry| match entry.action() {
            ExecutionAction::Dispatch(OrderCommand::Place(request)) if !request.reduce_only => {
                Some(request.clone())
            }
            _ => None,
        })
        .ok_or_else(|| "missing entry".into())
}

fn ack(request: &OrderRequest) -> OrderAck {
    OrderAck {
        client_order_id: request.client_order_id.clone(),
        venue_order_id: Some(format!("venue{}", request.client_order_id)),
        status: Some(OrderStatus::New),
    }
}

fn claim_entry(account: &mut DurableAccount) -> Result<(Delivery, OrderRequest), Box<dyn Error>> {
    let delivery = account.claim_next(at(300)?)?.ok_or("missing delivery")?;
    let ExecutionAction::Dispatch(OrderCommand::Place(request)) = &delivery.action else {
        return Err("expected entry delivery".into());
    };
    let request = request.clone();
    Ok((delivery, request))
}

fn place_count(account: &DurableAccount) -> usize {
    account
        .outbox()
        .iter()
        .filter(|entry| {
            matches!(
                entry.action(),
                ExecutionAction::Dispatch(OrderCommand::Place(_))
            )
        })
        .count()
}

fn corrupt_latest(path: &Path, edit: impl FnOnce(&mut serde_json::Value)) -> TestResult {
    let mut journal = Journal::open(path, &identity()?)?;
    let mut value: serde_json::Value =
        serde_json::from_slice(journal.latest().ok_or("missing checkpoint")?)?;
    edit(&mut value);
    // Journal computes a valid new checksum; rejection must come from domain
    // cross-validation, not merely detection of changed bytes on disk.
    journal.append(&serde_json::to_vec(&value)?)?;
    Ok(())
}

#[test]
fn new_and_existing_journals_fail_closed_on_scope_path_and_lock() -> TestResult {
    let files = TempJournal::new()?;
    assert!(DurableAccount::open(&files.path, &identity()?).is_err());
    assert!(!files.path.exists());
    let account = configured(&files.path)?;
    assert!(DurableAccount::create(&files.path, identity()?, RiskConfig::default()).is_err());
    assert!(DurableAccount::open(&files.path, &identity()?).is_err());
    let sequence = account.sequence();
    drop(account);
    let wrong = JournalIdentity::new(Exchange::Binance, "other-account".to_owned(), true)?;
    assert!(DurableAccount::open(&files.path, &wrong).is_err());
    let wrong_environment =
        JournalIdentity::new(Exchange::Binance, "durability-test".to_owned(), false)?;
    assert!(DurableAccount::open(&files.path, &wrong_environment).is_err());
    let reopened = DurableAccount::open(&files.path, &identity()?)?;
    assert!(reopened.sequence() > sequence);
    assert!(!reopened.execution().is_ready());
    assert!(!reopened.is_poisoned());
    Ok(())
}

#[test]
fn entry_is_committed_then_claimed_once_and_ack_does_not_fill() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    assert_eq!(account.outbox().len(), 1);
    assert_eq!(account.outbox()[0].state(), DeliveryState::Queued);
    let before_claim = account.sequence();
    let (delivery, request) = claim_entry(&mut account)?;
    assert!(account.sequence() > before_claim);
    assert_eq!(account.outbox()[0].state(), DeliveryState::Attempted);
    assert!(account.claim_next(at(300)?)?.is_none());
    let mut acceptance = ack(&request);
    acceptance.status = Some(OrderStatus::Filled);
    account.acknowledge(delivery.id, acceptance)?;
    assert_eq!(account.outbox()[0].state(), DeliveryState::Completed);
    assert_eq!(
        account.risk().reservation_status(Uuid::from_u128(13)),
        Some(ReservationStatus::Pending)
    );
    assert!(account.risk().active_reserved_risk()? > Decimal::ZERO);
    assert_eq!(account.execution().reconciliation_orders().len(), 1);
    Ok(())
}

#[test]
fn queued_and_attempted_network_items_are_recovery_only_after_restart() -> TestResult {
    for attempted in [false, true] {
        let files = TempJournal::new()?;
        let mut account = configured(&files.path)?;
        assess(&mut account)?;
        let order = queued_entry(&account)?;
        let held = account.risk().active_reserved_risk()?;
        if attempted {
            claim_entry(&mut account)?;
        }
        let operation_id = account.outbox()[0].id();
        drop(account);
        let mut restored = DurableAccount::open(&files.path, &identity()?)?;
        assert!(!restored.execution().is_ready());
        assert_eq!(
            restored.outbox()[0].state(),
            DeliveryState::RecoveryRequired
        );
        assert_eq!(restored.outbox()[0].id(), operation_id);
        assert!(restored.claim_next(at(301)?)?.is_none());
        assert_eq!(restored.risk().active_reserved_risk()?, held);
        let queries = restored.recovery_queries();
        assert_eq!(queries.len(), 1);
        assert!(matches!(&queries[0], OrderCommand::Query(reference)
            if reference.client_order_id == order.client_order_id));
    }
    Ok(())
}

#[test]
fn partial_fill_replay_after_restart_does_not_duplicate_protection() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    let (delivery, request) = claim_entry(&mut account)?;
    account.acknowledge(delivery.id, ack(&request))?;
    let partial = update(&request, OrderStatus::PartiallyFilled, Decimal::ONE, 301)?;
    account.apply(DurableInput::User(partial.clone()))?;
    assert_eq!(place_count(&account), 3);
    assert_eq!(
        account.risk().reservation_status(Uuid::from_u128(13)),
        Some(ReservationStatus::PositionOpen)
    );
    let held = account.risk().active_reserved_risk()?;
    drop(account);
    let mut restored = DurableAccount::open(&files.path, &identity()?)?;
    restored.apply(DurableInput::User(partial))?;
    assert_eq!(place_count(&restored), 3);
    restored.apply(DurableInput::User(update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::TWO,
        302,
    )?))?;
    assert_eq!(place_count(&restored), 5);
    assert_eq!(restored.risk().active_reserved_risk()?, held);
    assert!(!restored.execution().is_ready());
    Ok(())
}

#[test]
fn drawdown_latch_survives_restart_and_equity_recovery() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    account.apply(DurableInput::Equity(Decimal::new(9500, 0), at(301)?))?;
    assert!(matches!(
        account.risk().circuit_breaker_state(),
        CircuitBreakerState::Tripped { .. }
    ));
    assert!(!account.execution().is_ready());
    drop(account);
    let mut restored = DurableAccount::open(&files.path, &identity()?)?;
    restored.apply(DurableInput::Equity(Decimal::new(10_000, 0), at(302)?))?;
    assert!(matches!(
        restored.risk().circuit_breaker_state(),
        CircuitBreakerState::Tripped { .. }
    ));
    assert!(assess(&mut restored).is_err());
    assert_eq!(place_count(&restored), 0);
    Ok(())
}

#[test]
fn expired_or_halted_queued_entry_cannot_be_claimed() -> TestResult {
    for halt in [false, true] {
        let files = TempJournal::new()?;
        let mut account = configured(&files.path)?;
        assess(&mut account)?;
        if halt {
            account.apply(DurableInput::Halt)?;
        }
        let now = at(if halt { 300 } else { 601 })?;
        while let Some(delivery) = account.claim_next(now)? {
            assert!(!matches!(
                delivery.action,
                ExecutionAction::Dispatch(OrderCommand::Place(_))
            ));
        }
        assert_eq!(account.outbox()[0].state(), DeliveryState::RecoveryRequired);
        assert!(account.risk().active_reserved_risk()? > Decimal::ZERO);
    }
    Ok(())
}

#[test]
fn failed_query_does_not_generate_an_unbounded_retry_chain() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    let (delivery, request) = claim_entry(&mut account)?;
    account.failed(delivery.id)?;
    let mut query_count = 0;
    for _ in 0..16 {
        let Some(delivery) = account.claim_next(at(301)?)? else {
            break;
        };
        match delivery.action {
            ExecutionAction::Dispatch(OrderCommand::Query(_)) => {
                query_count += 1;
                let sequence = account.failed(delivery.id)?;
                assert_eq!(account.failed(delivery.id)?, sequence);
            }
            ExecutionAction::Dispatch(OrderCommand::Cancel(_)) => {
                account.acknowledge(delivery.id, ack(&request))?;
            }
            ExecutionAction::Dispatch(OrderCommand::Place(_)) => {
                return Err("placement retried".into())
            }
            ExecutionAction::Lifecycle { .. } | ExecutionAction::ReconciliationRequired => {
                account.handled(delivery.id)?;
            }
        }
    }
    assert_eq!(query_count, 1);
    assert!(account.claim_next(at(301)?)?.is_none());
    assert!(account.risk().active_reserved_risk()? > Decimal::ZERO);
    Ok(())
}

#[test]
fn notification_redelivery_reuses_id_and_completion_is_idempotent() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    account.apply(DurableInput::Halt)?;
    let first = account
        .claim_next(at(301)?)?
        .ok_or("missing notification")?;
    assert_eq!(first.action, ExecutionAction::ReconciliationRequired);
    drop(account);
    let mut restored = DurableAccount::open(&files.path, &identity()?)?;
    let second = restored
        .claim_next(at(302)?)?
        .ok_or("notification was lost")?;
    assert_eq!(first, second);
    let sequence = restored.handled(second.id)?;
    assert_eq!(restored.handled(second.id)?, sequence);
    assert!(restored.claim_next(at(302)?)?.is_none());
    Ok(())
}

#[test]
fn receipts_require_claim_and_conflicting_replays_are_rejected() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    let request = queued_entry(&account)?;
    let id = account.outbox()[0].id();
    assert!(matches!(
        account.acknowledge(id, ack(&request)),
        Err(DurabilityError::UnclaimedDelivery)
    ));
    assert!(matches!(
        account.failed(id),
        Err(DurabilityError::UnclaimedDelivery)
    ));
    let (delivery, _) = claim_entry(&mut account)?;
    assert!(account.handled(delivery.id).is_err());
    let sequence = account.acknowledge(id, ack(&request))?;
    assert_eq!(account.acknowledge(id, ack(&request))?, sequence);
    let mut conflicting = ack(&request);
    conflicting.venue_order_id = Some("other-order".to_owned());
    assert!(matches!(
        account.acknowledge(id, conflicting),
        Err(DurabilityError::ConflictingReceipt)
    ));
    assert_eq!(account.sequence(), sequence);
    Ok(())
}

#[test]
fn restored_receipts_require_explicit_fields_and_matching_venue_identity() -> TestResult {
    for corruption in 0..3 {
        let files = TempJournal::new()?;
        let mut account = configured(&files.path)?;
        assess(&mut account)?;
        let (delivery, request) = claim_entry(&mut account)?;
        account.acknowledge(delivery.id, ack(&request))?;
        drop(account);
        corrupt_latest(&files.path, |value| {
            let receipt = &mut value["outbox"][0]["receipt"];
            if corruption == 0 {
                receipt["venue_order_id"] = serde_json::json!("wrong-parent");
            } else if let Some(object) = receipt.as_object_mut() {
                object.remove(if corruption == 1 {
                    "venue_order_id"
                } else {
                    "status"
                });
            }
        })?;
        assert!(
            DurableAccount::open(&files.path, &identity()?).is_err(),
            "receipt corruption case {corruption}"
        );
    }
    Ok(())
}

#[test]
fn authoritative_order_evidence_prevents_unsent_entry_replay() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    let request = queued_entry(&account)?;
    account.apply(DurableInput::User(update(
        &request,
        OrderStatus::New,
        Decimal::ZERO,
        301,
    )?))?;
    assert!(account.claim_next(at(301)?)?.is_none());
    assert_eq!(account.outbox()[0].state(), DeliveryState::RecoveryRequired);
    assert!(account.risk().active_reserved_risk()? > Decimal::ZERO);
    Ok(())
}

#[test]
fn stale_protection_queue_cannot_place_after_position_has_closed() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    let (delivery, request) = claim_entry(&mut account)?;
    account.acknowledge(delivery.id, ack(&request))?;
    account.apply(DurableInput::User(update(
        &request,
        OrderStatus::Filled,
        request.quantity,
        301,
    )?))?;
    let stop = account
        .outbox()
        .iter()
        .find_map(|entry| match entry.action() {
            ExecutionAction::Dispatch(OrderCommand::Place(order))
                if order.kind == OrderKind::StopMarket =>
            {
                Some(order.clone())
            }
            _ => None,
        })
        .ok_or("missing stop")?;
    account.apply(DurableInput::User(update(
        &stop,
        OrderStatus::Filled,
        stop.quantity,
        302,
    )?))?;
    while let Some(delivery) = account.claim_next(at(302)?)? {
        assert!(!matches!(
            delivery.action,
            ExecutionAction::Dispatch(OrderCommand::Place(_))
        ));
    }
    assert!(account
        .outbox()
        .iter()
        .filter(|entry| matches!(entry.action(),
        ExecutionAction::Dispatch(OrderCommand::Place(order)) if order.reduce_only))
        .all(|entry| entry.state() == DeliveryState::RecoveryRequired));
    Ok(())
}

#[test]
fn invalid_user_evidence_commits_halt_without_losing_reservation() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    let request = queued_entry(&account)?;
    let held = account.risk().active_reserved_risk()?;
    let mut event = update(&request, OrderStatus::PartiallyFilled, Decimal::ONE, 301)?;
    if let UserEvent::Order(order) = &mut event {
        order.quantity = Decimal::new(999, 0);
    }
    assert!(account.apply(DurableInput::User(event)).is_err());
    assert!(!account.execution().is_ready());
    assert!(!account.is_poisoned());
    assert_eq!(account.risk().active_reserved_risk()?, held);
    assert_eq!(place_count(&account), 1);
    drop(account);
    let restored = DurableAccount::open(&files.path, &identity()?)?;
    assert_eq!(restored.risk().active_reserved_risk()?, held);
    Ok(())
}

#[test]
fn valid_checksum_cannot_hide_mismatched_risk_execution_or_outbox() -> TestResult {
    for corruption in 0..5 {
        let files = TempJournal::new()?;
        let mut account = configured(&files.path)?;
        assess(&mut account)?;
        drop(account);
        corrupt_latest(&files.path, |value| match corruption {
            0 => value["risk"]["reservations"] = serde_json::json!([]),
            1 => value["execution"]["brackets"][0]["released"] = serde_json::json!(true),
            2 => {
                value["outbox"][0]["action"]["Dispatch"]["Place"]["quantity"] =
                    serde_json::json!("999");
            }
            3 => value["outbox"][0]["id"] = serde_json::json!(Uuid::nil()),
            _ => {
                if let Some(object) = value["outbox"][0].as_object_mut() {
                    object.remove("receipt");
                }
            }
        })?;
        assert!(
            DurableAccount::open(&files.path, &identity()?).is_err(),
            "corruption case {corruption}"
        );
    }
    Ok(())
}

#[test]
fn cancelled_entry_tombstone_cannot_be_changed_to_closed_position() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    let request = queued_entry(&account)?;
    account.apply(DurableInput::User(update(
        &request,
        OrderStatus::Cancelled,
        Decimal::ZERO,
        301,
    )?))?;
    assert_eq!(
        account.risk().reservation_status(Uuid::from_u128(13)),
        Some(ReservationStatus::Released)
    );
    drop(account);
    corrupt_latest(&files.path, |value| {
        value["risk"]["reservations"][0]["lifecycle"]["state"] = serde_json::json!("closed");
    })?;
    assert!(DurableAccount::open(&files.path, &identity()?).is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_worker_serializes_concurrent_commits_and_drains_on_close() -> TestResult {
    let files = TempJournal::new()?;
    let account = configured(&files.path)?;
    let before = account.sequence();
    let handle = account.spawn(2)?;
    let second_handle = handle.clone();
    let (first, second) = tokio::join!(
        handle.request(DurableOperation::Apply(DurableInput::Tick(at(301)?))),
        second_handle.request(DurableOperation::Apply(DurableInput::Tick(at(301)?))),
    );
    let (DurableResult::Committed(first), DurableResult::Committed(second)) = (first?, second?)
    else {
        return Err("expected commit results".into());
    };
    assert_eq!(first.min(second), before + 1);
    assert_eq!(first.max(second), before + 2);
    assert_eq!(
        handle.request(DurableOperation::Claim(at(301)?)).await?,
        DurableResult::Delivery(None)
    );
    drop(handle);
    drop(second_handle);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(restored) = DurableAccount::open(&files.path, &identity()?) {
                assert!(restored.sequence() > before + 2);
                return Ok::<(), Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

#[test]
fn capacity_failure_while_committing_safety_halt_poisons_handle() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    let request = queued_entry(&account)?;
    let sequence = account.sequence();
    let held = account.risk().active_reserved_risk()?;
    while account.state.outbox.len() < MAX_OUTBOX {
        account.state.outbox.push(OutboxEntry {
            id: Uuid::new_v4(),
            action: ExecutionAction::ReconciliationRequired,
            state: DeliveryState::Completed,
            receipt: None,
        });
    }
    let mut invalid = update(&request, OrderStatus::PartiallyFilled, Decimal::ONE, 301)?;
    if let UserEvent::Order(order) = &mut invalid {
        order.quantity = Decimal::new(999, 0);
    }
    assert!(matches!(
        account.apply(DurableInput::User(invalid)),
        Err(DurabilityError::Capacity)
    ));
    assert!(account.is_poisoned());
    assert!(matches!(
        account.claim_next(at(301)?),
        Err(DurabilityError::Poisoned)
    ));
    assert_eq!(account.sequence(), sequence);
    assert_eq!(account.risk().active_reserved_risk()?, held);
    Ok(())
}

#[test]
fn rejected_changed_filter_remains_stale_after_restart() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    let changed = InstrumentRiskSpec::new(
        instrument(Exchange::Binance)?,
        Decimal::new(5, 1),
        Decimal::new(1, 3),
        Decimal::new(1, 3),
        None,
        Decimal::ONE,
    )?;
    assert!(matches!(
        account.apply(DurableInput::RegisterSpec(changed)),
        Err(DurabilityError::Risk(RiskError::InstrumentSpecInUse))
    ));
    let checkpoint = serde_json::to_value(account.risk().checkpoint())?;
    assert_eq!(
        checkpoint["stale_instrument_specs"]
            .as_array()
            .ok_or("missing stale specs")?
            .len(),
        1
    );
    assert!(!account.execution().is_ready());
    drop(account);
    let mut restored = DurableAccount::open(&files.path, &identity()?)?;
    assert_eq!(
        serde_json::to_value(restored.risk().checkpoint())?,
        checkpoint
    );
    let original = InstrumentRiskSpec::new(
        instrument(Exchange::Binance)?,
        Decimal::new(1, 1),
        Decimal::new(1, 3),
        Decimal::new(1, 3),
        None,
        Decimal::ONE,
    )?;
    assert!(matches!(
        restored.apply(DurableInput::RegisterSpec(original)),
        Err(DurabilityError::Risk(RiskError::InstrumentSpecStale))
    ));
    assert!(!restored.is_poisoned());
    Ok(())
}

#[test]
fn stale_equity_before_order_expiry_prevents_entry_claim() -> TestResult {
    let files = TempJournal::new()?;
    let mut account = configured(&files.path)?;
    assess(&mut account)?;
    // Equity was observed at second 299, while the entry expires at 600.
    assert!(account.claim_next(at(305)?)?.is_none());
    assert_eq!(account.outbox()[0].state(), DeliveryState::RecoveryRequired);
    assert!(account.risk().active_reserved_risk()? > Decimal::ZERO);
    account.apply(DurableInput::Equity(Decimal::new(10_000, 0), at(305)?))?;
    assert!(account.claim_next(at(305)?)?.is_none());
    Ok(())
}
