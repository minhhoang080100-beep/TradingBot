use std::{error::Error, io};

use super::*;
use crate::{
    BreakBasis, Candle, CandleInput, FairValueGap, FairValueGapInput, FractalModel, FvgEntryMode,
    InstrumentId, InstrumentRiskSpec, LiquiditySide, LiquiditySweep, LiquiditySweepInput,
    LiquidityTarget, MarketKind, MarketStructureShift, MarketStructureShiftInput, PriceZone,
    RiskConfig, RiskManager, SwingKind, SwingPoint, Timeframe, TradeAssessment, TradeIntent,
    TradeIntentInput, VolumeUnit,
};

type TestResult = Result<(), Box<dyn Error>>;

pub(crate) fn at(seconds: i64) -> Result<DateTime<Utc>, io::Error> {
    DateTime::from_timestamp(seconds, 0).ok_or_else(|| io::Error::other("test timestamp"))
}

pub(crate) fn instrument(exchange: Exchange) -> Result<InstrumentId, Box<dyn Error>> {
    Ok(InstrumentId::new(
        exchange,
        MarketKind::LinearPerpetual,
        if exchange == Exchange::Okx {
            "BTC-USDT-SWAP"
        } else {
            "BTCUSDT"
        },
        "BTC-USDT-PERP",
    )?)
}

pub(crate) fn intent(id: Uuid, exchange: Exchange) -> Result<TradeIntent, Box<dyn Error>> {
    let series = instrument(exchange)?;
    let swing = SwingPoint::new(
        series.clone(),
        Timeframe::M1,
        SwingKind::High,
        Decimal::new(101, 0),
        at(-60)?,
        at(60)?,
        FractalModel::ThreeCandle,
    )?;
    let shift = MarketStructureShift::new(MarketStructureShiftInput {
        id: Uuid::from_u128(12),
        instrument: series.clone(),
        timeframe: Timeframe::M1,
        direction: Direction::Bullish,
        broken_swing: swing,
        break_price: Decimal::new(103, 0),
        break_basis: BreakBasis::Close,
        confirming_candle_open_time: at(120)?,
        confirmed_at: at(180)?,
    })?;
    let sweep = LiquiditySweep::new(LiquiditySweepInput {
        id: Uuid::from_u128(11),
        instrument: series.clone(),
        timeframe: Timeframe::M1,
        side: LiquiditySide::SellSide,
        reference_price: Decimal::new(99, 0),
        sweep_price: Decimal::new(98, 0),
        sweep_candle_open_time: at(0)?,
        detected_at: at(60)?,
    })?;
    let gap = FairValueGap::new(FairValueGapInput {
        id: Uuid::from_u128(10),
        instrument: series.clone(),
        timeframe: Timeframe::M1,
        direction: Direction::Bullish,
        zone: PriceZone::new(Decimal::new(100, 0), Decimal::new(102, 0))?,
        formed_from: [at(60)?, at(120)?, at(180)?],
        formed_at: at(240)?,
    })?;
    let trigger = Candle::new(CandleInput {
        instrument: series.clone(),
        timeframe: Timeframe::M1,
        open_time: at(240)?,
        close_time: at(300)?,
        open: Decimal::new(103, 0),
        high: Decimal::new(104, 0),
        low: Decimal::new(101, 0),
        close: Decimal::new(102, 0),
        volume: Decimal::ONE,
        volume_unit: VolumeUnit::Contracts,
        trade_count: Some(1),
        is_closed: true,
    })?;
    Ok(TradeIntent::new(TradeIntentInput {
        id,
        instrument: series,
        timeframe: Timeframe::M1,
        direction: Direction::Bullish,
        liquidity_sweep: sweep,
        market_structure_shift: shift,
        fair_value_gap: gap,
        entry_mode: FvgEntryMode::NearBoundary,
        trigger_candle: trigger,
        expires_at: at(600)?,
    })?)
}

fn approval() -> Result<(RiskManager, RiskPlan), Box<dyn Error>> {
    approval_for(Exchange::Binance)
}

pub(super) fn approval_for(exchange: Exchange) -> Result<(RiskManager, RiskPlan), Box<dyn Error>> {
    let mut manager = RiskManager::new(RiskConfig::default());
    manager.register_instrument_spec(InstrumentRiskSpec::new(
        instrument(exchange)?,
        Decimal::new(1, 1),
        Decimal::new(1, 3),
        Decimal::new(1, 3),
        None,
        Decimal::ONE,
    )?)?;
    let equity = Decimal::new(10_000, 0);
    manager.start_trading_day(equity, equity, at(299)?, false)?;
    let plan = assess(&mut manager, Uuid::from_u128(13), exchange, 300)?;
    Ok((manager, plan))
}

fn assess(
    manager: &mut RiskManager,
    id: Uuid,
    exchange: Exchange,
    seconds: i64,
) -> Result<RiskPlan, Box<dyn Error>> {
    let target = LiquidityTarget::new(
        Uuid::from_u128(14),
        instrument(exchange)?,
        LiquiditySide::BuySide,
        Decimal::new(112, 0),
        at(240)?,
    )?;
    match manager.assess_trade(&intent(id, exchange)?, &target, at(seconds)?)? {
        TradeAssessment::Approved(plan) => Ok(*plan),
        TradeAssessment::Replay { .. } => {
            Err(io::Error::other("expected fresh risk approval").into())
        }
    }
}

pub(crate) fn flat() -> Result<FlatAccountSnapshot, io::Error> {
    Ok(FlatAccountSnapshot {
        exchange: Exchange::Binance,
        observed_at: at(300)?,
        open_orders: 0,
        open_algo_orders: 0,
        nonzero_positions: 0,
        one_way_mode: true,
    })
}

pub(super) fn ready() -> Result<ExecutionCoordinator, Box<dyn Error>> {
    let mut coordinator = ExecutionCoordinator::new(Exchange::Binance);
    coordinator.reconcile_flat(flat()?, at(300)?)?;
    Ok(coordinator)
}

pub(super) fn placed(actions: &[ExecutionAction]) -> Vec<OrderRequest> {
    actions
        .iter()
        .filter_map(|action| match action {
            ExecutionAction::Dispatch(OrderCommand::Place(request)) => Some(request.clone()),
            _ => None,
        })
        .collect()
}

fn single_placed(actions: &[ExecutionAction]) -> Result<OrderRequest, io::Error> {
    let mut orders = placed(actions);
    if orders.len() != 1 {
        return Err(io::Error::other("expected one place request"));
    }
    orders
        .pop()
        .ok_or_else(|| io::Error::other("missing place request"))
}

pub(super) fn entry(
    coordinator: &mut ExecutionCoordinator,
    plan: &RiskPlan,
) -> Result<OrderRequest, Box<dyn Error>> {
    Ok(single_placed(
        &coordinator.approve(plan.clone(), at(300)?)?,
    )?)
}

pub(crate) fn update(
    request: &OrderRequest,
    status: OrderStatus,
    cumulative_filled: Decimal,
    seconds: i64,
) -> Result<UserEvent, io::Error> {
    Ok(UserEvent::Order(OrderUpdate {
        exchange: request.instrument.exchange,
        symbol: request.instrument.venue_symbol.to_string(),
        client_order_id: request.client_order_id.clone(),
        venue_order_id: format!("venue{}", request.client_order_id),
        status,
        quantity: request.quantity,
        cumulative_filled,
        average_price: (cumulative_filled > Decimal::ZERO).then_some(Decimal::new(102, 0)),
        at: at(seconds)?,
    }))
}

fn has_lifecycle(actions: &[ExecutionAction], expected: ReservationEvent) -> bool {
    actions.iter().any(|action| {
        matches!(action,
        ExecutionAction::Lifecycle { event, .. } if *event == expected)
    })
}

fn apply_risk(manager: &mut RiskManager, actions: &[ExecutionAction]) -> TestResult {
    for action in actions {
        if let ExecutionAction::Lifecycle {
            intent_id,
            event,
            at,
            ..
        } = action
        {
            manager.apply_reservation_event(*intent_id, *event, *at)?;
        }
    }
    Ok(())
}

fn cancel_ids(actions: &[ExecutionAction]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            ExecutionAction::Dispatch(OrderCommand::Cancel(reference)) => {
                Some(reference.client_order_id.clone())
            }
            _ => None,
        })
        .collect()
}

#[test]
fn startup_requires_account_wide_flat_reconciliation() -> TestResult {
    let (_, plan) = approval()?;
    let mut coordinator = ExecutionCoordinator::new(Exchange::Binance);
    assert!(!coordinator.is_ready());
    assert!(coordinator
        .on_user_event(&UserEvent::Authenticated)?
        .is_empty());
    assert!(coordinator
        .on_user_event(&UserEvent::Subscribed)?
        .is_empty());
    assert!(coordinator.approve(plan.clone(), at(300)?).is_err());
    coordinator.reconcile_flat(flat()?, at(300)?)?;
    assert!(coordinator.is_ready());
    let request = entry(&mut coordinator, &plan)?;
    assert_eq!(request.kind, OrderKind::Limit);
    assert_eq!(request.quantity, plan.quantity());
    assert_eq!(request.price, Some(plan.entry_price()));
    assert!(!request.reduce_only);
    assert!(coordinator.reconcile_flat(flat()?, at(300)?).is_err());
    Ok(())
}

#[test]
fn stale_future_wrong_mode_and_nonflat_snapshots_block_admission() -> TestResult {
    let base = flat()?;
    let snapshots = [
        FlatAccountSnapshot {
            observed_at: at(294)?,
            ..base
        },
        FlatAccountSnapshot {
            observed_at: at(301)?,
            ..base
        },
        FlatAccountSnapshot {
            exchange: Exchange::Bybit,
            ..base
        },
        FlatAccountSnapshot {
            one_way_mode: false,
            ..base
        },
        FlatAccountSnapshot {
            open_orders: 1,
            ..base
        },
        FlatAccountSnapshot {
            open_algo_orders: 1,
            ..base
        },
        FlatAccountSnapshot {
            nonzero_positions: 1,
            ..base
        },
    ];
    for snapshot in snapshots {
        let mut coordinator = ready()?;
        assert!(coordinator.reconcile_flat(snapshot, at(300)?).is_err());
        assert!(!coordinator.is_ready());
    }
    Ok(())
}

#[test]
fn approval_replay_does_not_dispatch_twice_or_reserve_more() -> TestResult {
    let (manager, plan) = approval()?;
    let reserved = manager.active_reserved_risk()?;
    let mut coordinator = ready()?;
    entry(&mut coordinator, &plan)?;
    assert!(coordinator.approve(plan.clone(), at(300)?)?.is_empty());
    coordinator.halt();
    assert!(coordinator.approve(plan, at(301)?)?.is_empty());
    assert_eq!(manager.active_reserved_risk()?, reserved);
    Ok(())
}

#[test]
fn acknowledgement_status_never_creates_fills_or_releases_risk() -> TestResult {
    let (mut manager, plan) = approval()?;
    let reserved = manager.active_reserved_risk()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let command = OrderCommand::Place(request.clone());
    let ack = OrderAck {
        client_order_id: request.client_order_id,
        venue_order_id: Some("123".to_owned()),
        status: Some(OrderStatus::Filled),
    };
    let actions = coordinator.acknowledge(&command, &ack)?;
    assert!(actions.is_empty());
    apply_risk(&mut manager, &actions)?;
    assert_eq!(manager.active_reserved_risk()?, reserved);
    Ok(())
}

#[test]
fn partial_fills_create_exactly_two_reduce_only_orders_per_new_slice() -> TestResult {
    let (mut manager, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let first = update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        301,
    )?;
    let actions = coordinator.on_user_event(&first)?;
    assert!(has_lifecycle(&actions, ReservationEvent::EntryFilled));
    apply_risk(&mut manager, &actions)?;
    let protection = placed(&actions);
    assert_eq!(protection.len(), 2);
    for exit in &protection {
        assert!(exit.reduce_only);
        assert_eq!(exit.side, OrderSide::Sell);
        assert_eq!(exit.quantity, Decimal::new(1, 3));
        assert_eq!(exit.price, None);
        assert_eq!(
            exit.trigger_price,
            Some(match exit.kind {
                OrderKind::StopMarket => plan.stop_loss_price(),
                OrderKind::TakeProfitMarket => plan.take_profit_price(),
                _ => return Err(io::Error::other("unexpected protective order kind").into()),
            })
        );
    }
    assert_ne!(protection[0].client_order_id, protection[1].client_order_id);
    assert!(coordinator.on_user_event(&first)?.is_empty());
    let second = coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(3, 3),
        302,
    )?)?;
    let next = placed(&second);
    assert_eq!(next.len(), 2);
    assert!(next.iter().all(|exit| exit.quantity == Decimal::new(2, 3)));
    assert!(next.iter().all(|exit| protection
        .iter()
        .all(|old| old.client_order_id != exit.client_order_id)));
    assert!(!has_lifecycle(&second, ReservationEvent::EntryFilled));
    assert!(manager.active_reserved_risk()? > Decimal::ZERO);
    Ok(())
}

#[test]
fn zero_fill_cancel_releases_but_partial_cancel_keeps_risk_and_protection() -> TestResult {
    let (mut manager, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let cancelled = coordinator.on_user_event(&update(
        &request,
        OrderStatus::Cancelled,
        Decimal::ZERO,
        301,
    )?)?;
    assert!(has_lifecycle(&cancelled, ReservationEvent::EntryCancelled));
    assert!(placed(&cancelled).is_empty());
    apply_risk(&mut manager, &cancelled)?;
    assert_eq!(manager.active_reserved_risk()?, Decimal::ZERO);

    let (mut partial_manager, partial_plan) = approval()?;
    let mut partial = ready()?;
    let partial_entry = entry(&mut partial, &partial_plan)?;
    let partial_actions = partial.on_user_event(&update(
        &partial_entry,
        OrderStatus::Cancelled,
        Decimal::new(1, 3),
        301,
    )?)?;
    assert!(has_lifecycle(
        &partial_actions,
        ReservationEvent::EntryFilled
    ));
    assert!(!has_lifecycle(
        &partial_actions,
        ReservationEvent::EntryCancelled
    ));
    assert_eq!(placed(&partial_actions).len(), 2);
    apply_risk(&mut partial_manager, &partial_actions)?;
    assert!(partial_manager.active_reserved_risk()? > Decimal::ZERO);
    Ok(())
}

#[test]
fn halt_and_late_fills_keep_protection_enabled() -> TestResult {
    let (_, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let halted = coordinator.halt();
    assert!(halted.contains(&ExecutionAction::ReconciliationRequired));
    assert_eq!(cancel_ids(&halted), vec![request.client_order_id.clone()]);
    assert!(!coordinator.is_ready());
    let late = coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        301,
    )?)?;
    assert_eq!(placed(&late).len(), 2);
    assert!(has_lifecycle(&late, ReservationEvent::EntryFilled));
    assert!(!coordinator.is_ready());
    Ok(())
}

#[test]
fn fill_delivered_after_cancel_release_reopens_risk_and_requires_reconciliation() -> TestResult {
    let (mut manager, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let cancelled = coordinator.on_user_event(&update(
        &request,
        OrderStatus::Cancelled,
        Decimal::ZERO,
        302,
    )?)?;
    apply_risk(&mut manager, &cancelled)?;
    assert_eq!(manager.active_reserved_risk()?, Decimal::ZERO);
    let late = coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        303,
    )?)?;
    assert!(late.contains(&ExecutionAction::ReconciliationRequired));
    assert_eq!(placed(&late).len(), 2);
    apply_risk(&mut manager, &late)?;
    assert!(manager.active_reserved_risk()? > Decimal::ZERO);
    assert!(!coordinator.is_ready());
    Ok(())
}

#[test]
fn older_cumulative_snapshot_is_ignored_but_newer_regression_halts_without_mutation() -> TestResult
{
    let (_, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(2, 3),
        302,
    )?)?;
    assert!(coordinator
        .on_user_event(&update(
            &request,
            OrderStatus::PartiallyFilled,
            Decimal::new(1, 3),
            301,
        )?)?
        .is_empty());
    assert!(coordinator.is_ready());
    assert!(coordinator
        .on_user_event(&update(
            &request,
            OrderStatus::PartiallyFilled,
            Decimal::new(1, 3),
            303,
        )?)
        .is_err());
    assert!(!coordinator.is_ready());
    let recovered = coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(3, 3),
        304,
    )?)?;
    let exits = placed(&recovered);
    assert_eq!(exits.len(), 2);
    assert!(exits.iter().all(|exit| exit.quantity == Decimal::new(1, 3)));
    Ok(())
}

#[test]
fn completed_exit_cancels_entry_and_sibling_then_waits_for_all_terminal_evidence() -> TestResult {
    let (mut manager, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let filled = coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        301,
    )?)?;
    apply_risk(&mut manager, &filled)?;
    let exits = placed(&filled);
    let stop = exits
        .iter()
        .find(|exit| exit.kind == OrderKind::StopMarket)
        .ok_or_else(|| io::Error::other("missing stop"))?;
    let target = exits
        .iter()
        .find(|exit| exit.kind == OrderKind::TakeProfitMarket)
        .ok_or_else(|| io::Error::other("missing target"))?;
    let stopped =
        coordinator.on_user_event(&update(stop, OrderStatus::Filled, stop.quantity, 302)?)?;
    let cancellations = cancel_ids(&stopped);
    assert_eq!(cancellations.len(), 2);
    assert!(cancellations.contains(&request.client_order_id));
    assert!(cancellations.contains(&target.client_order_id));
    assert!(!has_lifecycle(&stopped, ReservationEvent::PositionClosed));
    apply_risk(&mut manager, &stopped)?;
    assert!(manager.active_reserved_risk()? > Decimal::ZERO);
    let entry_cancelled = coordinator.on_user_event(&update(
        &request,
        OrderStatus::Cancelled,
        Decimal::new(1, 3),
        303,
    )?)?;
    assert!(!has_lifecycle(
        &entry_cancelled,
        ReservationEvent::PositionClosed
    ));
    let cleaned =
        coordinator.on_user_event(&update(target, OrderStatus::Cancelled, Decimal::ZERO, 304)?)?;
    assert!(has_lifecycle(&cleaned, ReservationEvent::PositionClosed));
    apply_risk(&mut manager, &cleaned)?;
    assert_eq!(manager.active_reserved_risk()?, Decimal::ZERO);
    assert!(coordinator
        .on_user_event(&update(target, OrderStatus::Cancelled, Decimal::ZERO, 304)?)?
        .is_empty());
    Ok(())
}

#[test]
fn completed_slice_cancels_only_its_sibling_and_retains_other_protection() -> TestResult {
    let (_, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let first = placed(&coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        301,
    )?)?);
    let second = placed(&coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(3, 3),
        302,
    )?)?);
    let stop = first
        .iter()
        .find(|exit| exit.kind == OrderKind::StopMarket)
        .ok_or_else(|| io::Error::other("missing first stop"))?;
    let sibling = first
        .iter()
        .find(|exit| exit.kind == OrderKind::TakeProfitMarket)
        .ok_or_else(|| io::Error::other("missing first target"))?;
    let stopped =
        coordinator.on_user_event(&update(stop, OrderStatus::Filled, stop.quantity, 303)?)?;
    let ids = cancel_ids(&stopped);
    assert!(ids.contains(&request.client_order_id));
    assert!(ids.contains(&sibling.client_order_id));
    assert!(second
        .iter()
        .all(|exit| !ids.contains(&exit.client_order_id)));
    assert!(!has_lifecycle(&stopped, ReservationEvent::PositionClosed));
    let cancelled = coordinator.on_user_event(&update(
        sibling,
        OrderStatus::Cancelled,
        Decimal::ZERO,
        304,
    )?)?;
    assert!(!cancelled.contains(&ExecutionAction::ReconciliationRequired));
    assert!(!has_lifecycle(&cancelled, ReservationEvent::PositionClosed));
    Ok(())
}

#[test]
fn unexpectedly_rejected_stop_halts_entries_and_keeps_risk_reserved() -> TestResult {
    let (mut manager, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let filled = coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        301,
    )?)?;
    apply_risk(&mut manager, &filled)?;
    let protection = placed(&filled);
    let stop = protection
        .iter()
        .find(|exit| exit.kind == OrderKind::StopMarket)
        .ok_or_else(|| io::Error::other("missing stop"))?;
    let rejected =
        coordinator.on_user_event(&update(stop, OrderStatus::Rejected, Decimal::ZERO, 302)?)?;
    assert!(rejected.contains(&ExecutionAction::ReconciliationRequired));
    assert!(cancel_ids(&rejected).contains(&request.client_order_id));
    assert!(!has_lifecycle(&rejected, ReservationEvent::PositionClosed));
    assert!(!coordinator.is_ready());
    apply_risk(&mut manager, &rejected)?;
    assert!(manager.active_reserved_risk()? > Decimal::ZERO);
    Ok(())
}

#[test]
fn expiry_cancel_ack_does_not_release_the_reservation() -> TestResult {
    let (mut manager, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    assert!(coordinator.tick(at(599)?).is_empty());
    let actions = coordinator.tick(at(600)?);
    assert_eq!(cancel_ids(&actions), vec![request.client_order_id.clone()]);
    assert!(coordinator.tick(at(601)?).is_empty());
    let command = OrderCommand::Cancel(request.reference(Some("123".to_owned())));
    let ack = OrderAck {
        client_order_id: request.client_order_id,
        venue_order_id: Some("123".to_owned()),
        status: Some(OrderStatus::Cancelled),
    };
    let accepted = coordinator.acknowledge(&command, &ack)?;
    assert!(!has_lifecycle(&accepted, ReservationEvent::EntryCancelled));
    apply_risk(&mut manager, &accepted)?;
    assert!(manager.active_reserved_risk()? > Decimal::ZERO);
    assert!(coordinator
        .reconcile_flat(
            FlatAccountSnapshot {
                observed_at: at(601)?,
                ..flat()?
            },
            at(601)?
        )
        .is_err());
    Ok(())
}

#[tokio::test]
async fn actor_shutdown_emits_reconciliation_without_network_io() -> TestResult {
    let coordinator = ready()?;
    let (_sender, receiver) = mpsc::channel(1);
    let (output, mut actions) = mpsc::channel(2);
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    coordinator.run(receiver, output, shutdown).await?;
    assert_eq!(
        actions.recv().await,
        Some(ExecutionAction::ReconciliationRequired)
    );
    assert_eq!(actions.recv().await, None);
    Ok(())
}

#[test]
fn late_fill_of_released_approval_protects_it_and_cancels_newer_entry() -> TestResult {
    let (mut manager, plan_a) = approval()?;
    let mut coordinator = ready()?;
    let request_a = entry(&mut coordinator, &plan_a)?;
    let released = coordinator.on_user_event(&update(
        &request_a,
        OrderStatus::Cancelled,
        Decimal::ZERO,
        301,
    )?)?;
    apply_risk(&mut manager, &released)?;
    let plan_b = assess(&mut manager, Uuid::from_u128(99), Exchange::Binance, 302)?;
    let request_b = single_placed(&coordinator.approve(plan_b, at(302)?)?)?;
    assert_ne!(request_a.client_order_id, request_b.client_order_id);
    let late = coordinator.on_user_event(&update(
        &request_a,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        303,
    )?)?;
    let protection = placed(&late);
    assert_eq!(protection.len(), 2);
    assert!(protection
        .iter()
        .all(|exit| exit.reduce_only && exit.quantity == Decimal::new(1, 3)));
    assert!(cancel_ids(&late).contains(&request_b.client_order_id));
    assert!(late.contains(&ExecutionAction::ReconciliationRequired));
    assert!(!coordinator.is_ready());
    apply_risk(&mut manager, &late)?;
    assert!(manager.active_reserved_risk()? > plan_a.planned_loss());
    Ok(())
}

#[test]
fn failed_query_does_not_create_an_unbounded_requery_loop() -> TestResult {
    let (_, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let failed_place =
        coordinator.handle(ExecutionInput::Failed(OrderCommand::Place(request.clone())))?;
    assert_eq!(
        failed_place
            .iter()
            .filter(|action| matches!(action, ExecutionAction::Dispatch(OrderCommand::Query(_))))
            .count(),
        1
    );
    for _ in 0..2 {
        let failed_query = coordinator.handle(ExecutionInput::Failed(OrderCommand::Query(
            request.reference(None),
        )))?;
        assert!(failed_query.contains(&ExecutionAction::ReconciliationRequired));
        assert!(!failed_query.iter().any(|action| matches!(
            action,
            ExecutionAction::Dispatch(OrderCommand::Query(_) | OrderCommand::Place(_))
        )));
    }
    Ok(())
}

#[test]
fn failed_cancel_requires_fresh_explicit_reconciliation_before_a_single_retry() -> TestResult {
    let (manager, plan) = approval()?;
    let reserved = manager.active_reserved_risk()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    assert_eq!(cancel_ids(&coordinator.halt()).len(), 1);
    let reference = request.reference(None);
    let failed_cancel = coordinator.handle(ExecutionInput::Failed(OrderCommand::Cancel(
        reference.clone(),
    )))?;
    assert_eq!(
        failed_cancel
            .iter()
            .filter(|action| matches!(action, ExecutionAction::Dispatch(OrderCommand::Query(_))))
            .count(),
        1
    );
    assert!(cancel_ids(&failed_cancel).is_empty());
    let failed_query =
        coordinator.handle(ExecutionInput::Failed(OrderCommand::Query(reference)))?;
    assert!(!failed_query
        .iter()
        .any(|action| matches!(action, ExecutionAction::Dispatch(OrderCommand::Query(_)))));
    let UserEvent::Order(resting) = update(&request, OrderStatus::New, Decimal::ZERO, 301)? else {
        return Err(io::Error::other("expected order snapshot").into());
    };
    assert!(coordinator
        .retry_cancellation(&resting, at(304)?, at(310)?)
        .is_err());
    // The order's own update time may be old while the REST observation is fresh.
    let retried = coordinator.retry_cancellation(&resting, at(310)?, at(310)?)?;
    assert_eq!(cancel_ids(&retried), vec![request.client_order_id]);
    assert!(placed(&retried).is_empty());
    assert!(!has_lifecycle(&retried, ReservationEvent::EntryCancelled));
    assert!(coordinator
        .on_user_event(&UserEvent::Order(resting))?
        .is_empty());
    assert_eq!(manager.active_reserved_risk()?, reserved);
    Ok(())
}

#[test]
fn triggered_child_before_parent_ack_cannot_supply_the_algo_cancel_id() -> TestResult {
    let (_, plan) = approval_for(Exchange::Okx)?;
    let mut coordinator = ExecutionCoordinator::new(Exchange::Okx);
    coordinator.reconcile_flat(
        FlatAccountSnapshot {
            exchange: Exchange::Okx,
            ..flat()?
        },
        at(300)?,
    )?;
    let request = entry(&mut coordinator, &plan)?;
    let protection = placed(&coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(2, 3),
        301,
    )?)?);
    let stop = protection
        .iter()
        .find(|exit| exit.kind == OrderKind::StopMarket)
        .ok_or_else(|| io::Error::other("missing stop"))?;
    let target = protection
        .iter()
        .find(|exit| exit.kind == OrderKind::TakeProfitMarket)
        .ok_or_else(|| io::Error::other("missing target"))?;
    coordinator.on_user_event(&update(
        stop,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        302,
    )?)?;
    let pair_closed = coordinator.on_user_event(&update(
        target,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        303,
    )?)?;
    assert!(!cancel_ids(&pair_closed).contains(&stop.client_order_id));
    assert!(!cancel_ids(&pair_closed).contains(&target.client_order_id));
    let ack = OrderAck {
        client_order_id: stop.client_order_id.clone(),
        venue_order_id: Some("parentAlgo777".to_owned()),
        status: Some(OrderStatus::New),
    };
    let actions = coordinator.acknowledge(&OrderCommand::Place(stop.clone()), &ack)?;
    assert!(actions.iter().any(|action| matches!(action,
        ExecutionAction::Dispatch(OrderCommand::Cancel(reference))
            if reference.client_order_id == stop.client_order_id
                && reference.venue_order_id.as_deref() == Some("parentAlgo777"))));
    assert!(!has_lifecycle(&actions, ReservationEvent::PositionClosed));
    Ok(())
}

#[test]
fn wrong_regular_order_id_is_rejected_before_any_fill_mutation() -> TestResult {
    let (_, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let ack = OrderAck {
        client_order_id: request.client_order_id.clone(),
        venue_order_id: Some("entry123".to_owned()),
        status: Some(OrderStatus::New),
    };
    coordinator.acknowledge(&OrderCommand::Place(request.clone()), &ack)?;
    let mut event = update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        301,
    )?;
    assert!(coordinator.on_user_event(&event).is_err());
    assert!(!coordinator.is_ready());
    if let UserEvent::Order(order) = &mut event {
        order.venue_order_id = "entry123".to_owned();
    }
    let corrected = coordinator.on_user_event(&event)?;
    assert_eq!(placed(&corrected).len(), 2);
    assert!(placed(&corrected)
        .iter()
        .all(|exit| exit.quantity == Decimal::new(1, 3)));
    Ok(())
}

#[test]
fn late_entry_after_position_close_restores_reservation_once_and_protects_delta() -> TestResult {
    let (mut manager, plan) = approval()?;
    let full_reserved = manager.active_reserved_risk()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let initial = coordinator.on_user_event(&update(
        &request,
        OrderStatus::Cancelled,
        Decimal::new(1, 3),
        301,
    )?)?;
    apply_risk(&mut manager, &initial)?;
    let protection = placed(&initial);
    let stop = protection
        .iter()
        .find(|exit| exit.kind == OrderKind::StopMarket)
        .ok_or_else(|| io::Error::other("missing stop"))?;
    let target = protection
        .iter()
        .find(|exit| exit.kind == OrderKind::TakeProfitMarket)
        .ok_or_else(|| io::Error::other("missing target"))?;
    let stopped =
        coordinator.on_user_event(&update(stop, OrderStatus::Filled, stop.quantity, 302)?)?;
    apply_risk(&mut manager, &stopped)?;
    let closed =
        coordinator.on_user_event(&update(target, OrderStatus::Cancelled, Decimal::ZERO, 303)?)?;
    assert!(has_lifecycle(&closed, ReservationEvent::PositionClosed));
    apply_risk(&mut manager, &closed)?;
    assert_eq!(manager.active_reserved_risk()?, Decimal::ZERO);
    let late_event = update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(3, 3),
        304,
    )?;
    let late = coordinator.on_user_event(&late_event)?;
    assert!(has_lifecycle(
        &late,
        ReservationEvent::EntryExposureReopened
    ));
    assert!(!has_lifecycle(&late, ReservationEvent::EntryFilled));
    let additional = placed(&late);
    assert_eq!(additional.len(), 2);
    assert!(additional
        .iter()
        .all(|exit| exit.quantity == Decimal::new(2, 3)));
    apply_risk(&mut manager, &late)?;
    assert_eq!(manager.active_reserved_risk()?, full_reserved);
    let duplicate = coordinator.on_user_event(&late_event)?;
    assert!(!has_lifecycle(
        &duplicate,
        ReservationEvent::EntryExposureReopened
    ));
    assert!(placed(&duplicate).is_empty());
    apply_risk(&mut manager, &duplicate)?;
    assert_eq!(manager.active_reserved_risk()?, full_reserved);
    Ok(())
}

#[test]
fn partial_tp_and_sl_fills_sum_to_slice_and_cancel_both_remainders() -> TestResult {
    let (_, plan) = approval()?;
    let mut coordinator = ready()?;
    let request = entry(&mut coordinator, &plan)?;
    let protection = placed(&coordinator.on_user_event(&update(
        &request,
        OrderStatus::PartiallyFilled,
        Decimal::new(3, 3),
        301,
    )?)?);
    let stop = protection
        .iter()
        .find(|exit| exit.kind == OrderKind::StopMarket)
        .ok_or_else(|| io::Error::other("missing stop"))?;
    let target = protection
        .iter()
        .find(|exit| exit.kind == OrderKind::TakeProfitMarket)
        .ok_or_else(|| io::Error::other("missing target"))?;
    let partial_target = coordinator.on_user_event(&update(
        target,
        OrderStatus::PartiallyFilled,
        Decimal::new(1, 3),
        302,
    )?)?;
    assert!(cancel_ids(&partial_target).contains(&request.client_order_id));
    let completed_pair = coordinator.on_user_event(&update(
        stop,
        OrderStatus::PartiallyFilled,
        Decimal::new(2, 3),
        303,
    )?)?;
    let ids = cancel_ids(&completed_pair);
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&stop.client_order_id));
    assert!(ids.contains(&target.client_order_id));
    assert!(!has_lifecycle(
        &completed_pair,
        ReservationEvent::PositionClosed
    ));
    assert!(!completed_pair.contains(&ExecutionAction::ReconciliationRequired));
    Ok(())
}
