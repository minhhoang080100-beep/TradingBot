//! Core library for the multi-exchange ICT trading bot.
//!
//! Exchange-specific JSON belongs in adapter DTOs. The types exported here are
//! normalized, validated domain models shared by Binance, Bybit, and OKX.

#![forbid(unsafe_code)]

pub mod candle_store;
pub mod config;
pub mod data_engine;
mod decimal_codec;
pub mod durability;
pub mod exchange;
pub mod execution;
pub mod ict_engine;
pub mod journal;
pub mod models;
pub mod paper;
pub mod reconciliation;
pub mod risk;
pub mod strategy;
pub mod telegram;

pub use candle_store::{
    CandleGap, CandleKey, CandleStore, CandleStoreError, SharedCandleStore, StoreDisposition,
    StoreOutcome,
};
pub use config::{ConfigError, DataEngineConfig, ReconnectPolicy};
pub use data_engine::{DataEngine, DataEngineError};
pub use durability::{
    Delivery, DeliveryState, DurabilityError, DurableAccount, DurableHandle, DurableInput,
    DurableOperation, DurableResult, OutboxEntry,
};
#[cfg(feature = "binance")]
pub use exchange::binance::BinanceMarketData;
#[cfg(feature = "bybit")]
pub use exchange::bybit::BybitMarketData;
#[cfg(feature = "okx")]
pub use exchange::okx::OkxMarketData;
pub use exchange::{
    AdapterError, ExchangeMarketData, HeartbeatSpec, InboundMarketEvent, MarketSubscription,
    OutboundMessage,
};
#[cfg(feature = "binance")]
pub use execution::binance::BinanceExecution;
#[cfg(feature = "bybit")]
pub use execution::bybit::BybitExecution;
pub use execution::coordinator::{
    ExecutionAction, ExecutionCheckpoint, ExecutionCoordinator, ExecutionInput, FlatAccountSnapshot,
};
#[cfg(feature = "okx")]
pub use execution::okx::OkxExecution;
pub use execution::transport::{
    run_private_session, ExecutionEnvironment, PrivateSessionConfig, RestExecutor, RestResponse,
};
pub use execution::{
    ApiCredentials, ExecutionAdapter, ExecutionError, OrderAck, OrderCommand, OrderKind, OrderRef,
    OrderRequest, OrderSide, OrderStatus, OrderUpdate, PositionUpdate, SignedRequest, UserEvent,
};
pub use ict_engine::{
    detect_fair_value_gap, detect_swing_points, DisplacementConfig, FvgFormationContext,
    FvgMitigationRule, IctDetectionBatch, IctEngine, IctEngineConfig, IctEngineEvent, IctError,
    IctTrackingLimits, Killzone, KillzoneFilter, SweepConfirmation, TradingDays,
};
pub use journal::{Journal, JournalError, JournalIdentity};
#[cfg(feature = "binance")]
pub use reconciliation::binance::BinanceInventory;
#[cfg(feature = "bybit")]
pub use reconciliation::bybit::BybitInventory;
#[cfg(feature = "okx")]
pub use reconciliation::okx::OkxInventory;
pub use reconciliation::{
    scan_inventory, InventoryAdapter, InventoryError, InventoryKind, InventoryLimits,
    InventoryPage, InventoryPartition, InventoryRecord, InventoryReport, InventoryResponse,
    InventoryReview, InventoryScope, InventoryTransport, ObservedInventoryRecord,
};

pub use models::{
    Bos, BreakBasis, BreakOfStructure, BreakOfStructureInput, Candle, CandleInput, Direction,
    DomainEvent, Exchange, FairValueGap, FairValueGapInput, FractalModel, Fvg, FvgEntryMode,
    InstrumentId, LiquiditySide, LiquiditySweep, LiquiditySweepInput, MarketKind,
    MarketStructureShift, MarketStructureShiftInput, ModelError, Mss, OrderBlock, OrderBlockInput,
    OrderBlockZoneBasis, PriceRange, PriceZone, SignalEnvelope, SignalEvent, SwingKind, SwingPoint,
    SymbolCode, Timeframe, TradeIntent, TradeIntentInput, VolumeUnit, ZoneState,
};
pub use risk::{
    CircuitBreakerState, InstrumentRiskSpec, LiquidityTarget, ReservationEvent, ReservationStatus,
    ReservationTransition, RiskAction, RiskActorError, RiskCommand, RiskConfig, RiskError,
    RiskManager, RiskPlan, TradeAssessment,
};
pub use risk::{RiskCheckpoint, RiskPlanCheckpoint};
pub use strategy::{
    CancellationReason, DataDegradationReason, KillzonePolicy, SetupDiscardReason, StrategyAction,
    StrategyCommand, StrategyConfig, StrategyEngine, StrategyError, StrategyInputRejection,
    StrategyState,
};
