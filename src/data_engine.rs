//! Resilient asynchronous WebSocket market-data engine.

use std::{future::pending, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::mpsc,
    task::{JoinError, JoinSet},
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::{
    candle_store::{CandleStore, SharedCandleStore, StoreDisposition},
    config::DataEngineConfig,
    exchange::{AdapterError, ExchangeMarketData, InboundMarketEvent, OutboundMessage},
    models::{Exchange, SignalEvent},
};

type VenueSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Runs one reconnecting WebSocket session for every configured adapter.
pub struct DataEngine {
    adapters: Vec<Arc<dyn ExchangeMarketData>>,
    store: SharedCandleStore,
    signal_tx: mpsc::Sender<SignalEvent>,
    shutdown: CancellationToken,
    config: DataEngineConfig,
}

impl DataEngine {
    /// Creates a data engine and its bounded candle store.
    ///
    /// # Errors
    ///
    /// Returns [`DataEngineError::NoAdapters`] when no exchange connection was
    /// supplied.
    pub fn new(
        adapters: Vec<Arc<dyn ExchangeMarketData>>,
        signal_tx: mpsc::Sender<SignalEvent>,
        shutdown: CancellationToken,
        config: DataEngineConfig,
    ) -> Result<Self, DataEngineError> {
        if adapters.is_empty() {
            return Err(DataEngineError::NoAdapters);
        }

        let store = CandleStore::shared(config.candle_capacity());
        Ok(Self {
            adapters,
            store,
            signal_tx,
            shutdown,
            config,
        })
    }

    /// Returns a clone of the concurrent candle store handle.
    #[must_use]
    pub fn store(&self) -> SharedCandleStore {
        Arc::clone(&self.store)
    }

    /// Returns the cancellation token used by all exchange tasks.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Runs all exchange sessions until cancellation or a fatal pipeline error.
    ///
    /// # Errors
    ///
    /// Returns [`DataEngineError`] when a task fails, the downstream signal
    /// channel closes or fills, or an adapter cannot build its subscription.
    pub async fn run(self) -> Result<(), DataEngineError> {
        let mut tasks = JoinSet::new();
        for adapter in self.adapters {
            let task_store = Arc::clone(&self.store);
            let task_tx = self.signal_tx.clone();
            let task_shutdown = self.shutdown.clone();
            let task_config = self.config.clone();
            tasks.spawn(async move {
                run_adapter(adapter, task_store, task_tx, task_shutdown, task_config).await
            });
        }
        drop(self.signal_tx);

        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Ok(());
                }
                result = tasks.join_next() => {
                    match result {
                        Some(Ok(Ok(()))) if self.shutdown.is_cancelled() => {}
                        Some(Ok(Ok(()))) => {
                            self.shutdown.cancel();
                            tasks.abort_all();
                            return Err(DataEngineError::UnexpectedSessionExit);
                        }
                        Some(Ok(Err(source))) => {
                            self.shutdown.cancel();
                            tasks.abort_all();
                            return Err(source);
                        }
                        Some(Err(source)) => {
                            self.shutdown.cancel();
                            tasks.abort_all();
                            return Err(DataEngineError::TaskJoin(source));
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }
}

async fn run_adapter(
    adapter: Arc<dyn ExchangeMarketData>,
    store: SharedCandleStore,
    signal_tx: mpsc::Sender<SignalEvent>,
    shutdown: CancellationToken,
    config: DataEngineConfig,
) -> Result<(), DataEngineError> {
    let exchange = adapter.exchange();
    let subscription_messages = adapter
        .subscription_messages()
        .map_err(|source| DataEngineError::Adapter { exchange, source })?;
    let mut attempt = 0_u32;

    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        let endpoint = adapter.endpoint().as_str().to_owned();
        info!(?exchange, %endpoint, "connecting market-data WebSocket");
        let connection = tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            result = tokio::time::timeout(
                config.connect_timeout(),
                connect_async(endpoint.as_str()),
            ) => result,
        };

        let socket = match connection {
            Ok(Ok((socket, _response))) => socket,
            Ok(Err(source)) => {
                warn!(?exchange, error = %source, "WebSocket connection failed");
                if !reconnect_delay(&config, &shutdown, &mut attempt).await {
                    return Ok(());
                }
                continue;
            }
            Err(_elapsed) => {
                warn!(?exchange, "WebSocket connection timed out");
                if !reconnect_delay(&config, &shutdown, &mut attempt).await {
                    return Ok(());
                }
                continue;
            }
        };

        info!(?exchange, "market-data WebSocket connected");
        let connected_at = Instant::now();
        let session_result = run_session(
            Arc::clone(&adapter),
            socket,
            &subscription_messages,
            Arc::clone(&store),
            &signal_tx,
            &shutdown,
            config.silence_timeout(),
        )
        .await;

        if shutdown.is_cancelled() {
            return Ok(());
        }
        if let Err(failure) = session_result {
            match failure {
                SessionFailure::SignalChannelFull => {
                    return Err(DataEngineError::SignalChannelFull);
                }
                SessionFailure::SignalChannelClosed => {
                    return Err(DataEngineError::SignalChannelClosed);
                }
                SessionFailure::InvalidHeartbeat => {
                    return Err(DataEngineError::InvalidHeartbeat { exchange });
                }
                recoverable => {
                    warn!(
                        ?exchange,
                        error = %recoverable,
                        "market-data session ended; reconnecting"
                    );
                }
            }
        }

        if connected_at.elapsed() >= config.reconnect().stable_after() {
            attempt = 0;
        }
        if !reconnect_delay(&config, &shutdown, &mut attempt).await {
            return Ok(());
        }
    }
}

async fn reconnect_delay(
    config: &DataEngineConfig,
    shutdown: &CancellationToken,
    attempt: &mut u32,
) -> bool {
    let delay = config.reconnect().delay_for_attempt(*attempt);
    *attempt = attempt.saturating_add(1);
    debug!(?delay, attempt = *attempt, "waiting before reconnect");
    tokio::select! {
        () = shutdown.cancelled() => false,
        () = tokio::time::sleep(delay) => true,
    }
}

async fn run_session(
    adapter: Arc<dyn ExchangeMarketData>,
    mut socket: VenueSocket,
    subscription_messages: &[OutboundMessage],
    store: SharedCandleStore,
    signal_tx: &mpsc::Sender<SignalEvent>,
    shutdown: &CancellationToken,
    silence_timeout: Duration,
) -> Result<(), SessionFailure> {
    for message in subscription_messages {
        socket
            .send(to_websocket_message(message.clone()))
            .await
            .map_err(SessionFailure::WebSocket)?;
    }

    let heartbeat = adapter.heartbeat();
    if heartbeat.interval.is_zero() {
        return Err(SessionFailure::InvalidHeartbeat);
    }

    let (mut sink, mut stream) = socket.split();
    let start = Instant::now() + heartbeat.interval;
    let mut heartbeat_timer = tokio::time::interval_at(start, heartbeat.interval);
    heartbeat_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let rotation_deadline = adapter.max_connection_age().map(|age| Instant::now() + age);
    let mut last_inbound = Instant::now();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                let _close_result = sink.send(Message::Close(None)).await;
                return Ok(());
            }
            () = wait_for_rotation(rotation_deadline) => {
                info!(exchange = ?adapter.exchange(), "rotating WebSocket connection");
                let _close_result = sink.send(Message::Close(None)).await;
                return Err(SessionFailure::Rotation);
            }
            _ = heartbeat_timer.tick() => {
                if last_inbound.elapsed() > silence_timeout {
                    return Err(SessionFailure::SilenceTimeout(silence_timeout));
                }
                sink.send(to_websocket_message(heartbeat.message.clone()))
                    .await
                    .map_err(SessionFailure::WebSocket)?;
            }
            incoming = stream.next() => {
                let message = incoming
                    .ok_or(SessionFailure::StreamEnded)?
                    .map_err(SessionFailure::WebSocket)?;
                last_inbound = Instant::now();

                match message {
                    Message::Text(text) => {
                        process_payload(
                            adapter.as_ref(),
                            text.as_bytes(),
                            &store,
                            signal_tx,
                        ).await?;
                    }
                    Message::Binary(bytes) => {
                        process_payload(
                            adapter.as_ref(),
                            bytes.as_ref(),
                            &store,
                            signal_tx,
                        ).await?;
                    }
                    Message::Ping(payload) => {
                        sink.send(Message::Pong(payload))
                            .await
                            .map_err(SessionFailure::WebSocket)?;
                    }
                    Message::Pong(_) => {
                        trace!(exchange = ?adapter.exchange(), "WebSocket pong received");
                    }
                    Message::Close(frame) => return Err(SessionFailure::Closed(format!("{frame:?}"))),
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

async fn wait_for_rotation(deadline: Option<Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        pending::<()>().await;
    }
}

async fn process_payload(
    adapter: &dyn ExchangeMarketData,
    payload: &[u8],
    store: &SharedCandleStore,
    signal_tx: &mpsc::Sender<SignalEvent>,
) -> Result<(), SessionFailure> {
    let events = adapter
        .parse_message(payload)
        .map_err(SessionFailure::Adapter)?;

    for event in events {
        match event {
            InboundMarketEvent::Candle(candle) => {
                let outcome = {
                    let mut writable = store.write().await;
                    writable.upsert(candle)
                };

                match outcome {
                    Ok(outcome) => {
                        if let Some(gap) = outcome.gap {
                            warn!(
                                exchange = ?adapter.exchange(),
                                expected = %gap.expected_open_time,
                                received = %gap.received_open_time,
                                "candle gap detected; REST backfill required"
                            );
                        }
                        if let StoreDisposition::Closed(candle) = outcome.disposition {
                            signal_tx
                                .try_send(SignalEvent::CandleClosed(candle))
                                .map_err(|source| match source {
                                    mpsc::error::TrySendError::Full(_) => {
                                        SessionFailure::SignalChannelFull
                                    }
                                    mpsc::error::TrySendError::Closed(_) => {
                                        SessionFailure::SignalChannelClosed
                                    }
                                })?;
                        }
                    }
                    Err(source) => {
                        warn!(
                            exchange = ?adapter.exchange(),
                            error = %source,
                            "rejected stale or conflicting candle"
                        );
                    }
                }
            }
            InboundMarketEvent::SubscriptionAcknowledged => {
                info!(exchange = ?adapter.exchange(), "subscription acknowledged");
            }
            InboundMarketEvent::Heartbeat => {
                trace!(exchange = ?adapter.exchange(), "application heartbeat received");
            }
        }
    }
    Ok(())
}

fn to_websocket_message(message: OutboundMessage) -> Message {
    match message {
        OutboundMessage::Text(text) => Message::Text(text.into()),
        OutboundMessage::Ping(payload) => Message::Ping(payload.into()),
    }
}

#[derive(Debug, thiserror::Error)]
enum SessionFailure {
    #[error("WebSocket transport error: {0}")]
    WebSocket(#[from] tungstenite::Error),
    #[error("adapter rejected payload: {0}")]
    Adapter(#[from] AdapterError),
    #[error("server closed connection: {0}")]
    Closed(String),
    #[error("WebSocket stream ended without a close frame")]
    StreamEnded,
    #[error("no inbound message for {0:?}")]
    SilenceTimeout(Duration),
    #[error("connection reached proactive rotation age")]
    Rotation,
    #[error("adapter heartbeat interval must be non-zero")]
    InvalidHeartbeat,
    #[error("downstream signal channel is full")]
    SignalChannelFull,
    #[error("downstream signal channel is closed")]
    SignalChannelClosed,
}

/// Fatal data-engine failure.
#[derive(Debug, thiserror::Error)]
pub enum DataEngineError {
    /// No exchange adapter was configured.
    #[error("data engine requires at least one exchange adapter")]
    NoAdapters,
    /// Adapter configuration or payload processing failed.
    #[error("{exchange:?} adapter failure: {source}")]
    Adapter {
        /// Affected venue.
        exchange: Exchange,
        /// Adapter error.
        source: AdapterError,
    },
    /// Adapter returned a zero heartbeat interval.
    #[error("{exchange:?} adapter heartbeat interval must be non-zero")]
    InvalidHeartbeat {
        /// Affected venue.
        exchange: Exchange,
    },
    /// Downstream processing cannot keep pace without losing a final candle.
    #[error("downstream signal channel is full; refusing to drop a closed candle")]
    SignalChannelFull,
    /// Strategy side dropped the signal receiver.
    #[error("downstream signal channel is closed")]
    SignalChannelClosed,
    /// Exchange task ended while the service was still active.
    #[error("exchange session exited unexpectedly")]
    UnexpectedSessionExit,
    /// Tokio task panicked or was cancelled unexpectedly.
    #[error("exchange task join failed: {0}")]
    TaskJoin(#[from] JoinError),
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io, num::NonZeroUsize};

    use futures_util::{SinkExt as _, StreamExt as _};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    use url::Url;

    use super::*;
    use crate::config::ReconnectPolicy;
    use crate::{
        exchange::{
            normalize_candle, HeartbeatSpec, MarketSubscription, SubscriptionIndex, WireCandle,
        },
        models::{InstrumentId, MarketKind, Timeframe, VolumeUnit},
    };

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    #[derive(Debug)]
    struct MockAdapter {
        endpoint: Url,
        index: SubscriptionIndex,
    }

    impl ExchangeMarketData for MockAdapter {
        fn exchange(&self) -> Exchange {
            Exchange::Binance
        }

        fn endpoint(&self) -> &Url {
            &self.endpoint
        }

        fn subscriptions(&self) -> &[MarketSubscription] {
            self.index.subscriptions()
        }

        fn subscription_messages(&self) -> Result<Vec<OutboundMessage>, AdapterError> {
            Ok(vec![OutboundMessage::Text("subscribe".to_owned())])
        }

        fn heartbeat(&self) -> HeartbeatSpec {
            HeartbeatSpec {
                interval: Duration::from_secs(5),
                message: OutboundMessage::Ping(Vec::new()),
            }
        }

        fn parse_message(&self, payload: &[u8]) -> Result<Vec<InboundMarketEvent>, AdapterError> {
            let is_closed = match payload {
                b"open" => false,
                b"closed" => true,
                _ => {
                    return Err(AdapterError::Protocol {
                        exchange: self.exchange(),
                        message: "unexpected mock payload".to_owned(),
                    });
                }
            };
            let subscription =
                self.subscriptions()
                    .first()
                    .ok_or(AdapterError::EmptySubscriptions {
                        exchange: self.exchange(),
                    })?;
            let candle = normalize_candle(WireCandle {
                exchange: self.exchange(),
                instrument: subscription.instrument.clone(),
                timeframe: subscription.timeframe,
                open_time_millis: 1_700_000_040_000,
                open: "100",
                high: "110",
                low: "90",
                close: if is_closed { "105" } else { "101" },
                volume: "10",
                volume_unit: VolumeUnit::BaseAsset,
                trade_count: Some(3),
                is_closed,
            })?;
            Ok(vec![InboundMarketEvent::Candle(candle)])
        }
    }

    fn mock_adapter(endpoint: Url) -> Result<MockAdapter, AdapterError> {
        let instrument = InstrumentId::new(
            Exchange::Binance,
            MarketKind::LinearPerpetual,
            "BTCUSDT",
            "BTC-USDT-PERP",
        )?;
        let index = SubscriptionIndex::new(
            Exchange::Binance,
            vec![MarketSubscription::new(instrument, Timeframe::M1)],
        )?;
        Ok(MockAdapter { endpoint, index })
    }

    #[test]
    fn engine_rejects_empty_adapter_set() {
        let (signal_tx, _signal_rx) = mpsc::channel(4);
        let config = DataEngineConfig::new(
            NonZeroUsize::MIN,
            Duration::from_secs(1),
            Duration::from_secs(2),
            ReconnectPolicy::default(),
        );
        assert!(config.is_ok());
        if let Ok(config) = config {
            let result = DataEngine::new(Vec::new(), signal_tx, CancellationToken::new(), config);
            assert!(matches!(result, Err(DataEngineError::NoAdapters)));
        }
    }

    #[tokio::test]
    async fn reconnects_and_emits_final_candle_exactly_once() -> TestResult {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let endpoint = Url::parse(&format!("ws://{address}"))?;
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();

        let server = tokio::spawn(async move {
            for connection_index in 0_u8..2 {
                let (stream, _peer) = listener.accept().await?;
                let mut socket = accept_async(stream).await?;
                let subscription = socket.next().await.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "missing subscription")
                })??;
                if !matches!(subscription, Message::Text(_)) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected text subscription",
                    )
                    .into());
                }

                if connection_index == 0 {
                    socket.send(Message::Text("open".into())).await?;
                    socket.close(None).await?;
                } else {
                    socket.send(Message::Text("closed".into())).await?;
                    socket.send(Message::Text("closed".into())).await?;
                    server_shutdown.cancelled().await;
                    let _close_result = socket.close(None).await;
                }
            }
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        });

        let reconnect = ReconnectPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(5),
            1,
            Duration::ZERO,
            Duration::from_secs(1),
        )?;
        let config = DataEngineConfig::new(
            NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
            Duration::from_secs(1),
            Duration::from_secs(2),
            reconnect,
        )?;
        let (signal_tx, mut signal_rx) = mpsc::channel(4);
        let adapter: Arc<dyn ExchangeMarketData> = Arc::new(mock_adapter(endpoint)?);
        let engine = DataEngine::new(vec![adapter], signal_tx, shutdown.clone(), config)?;
        let engine_task = tokio::spawn(engine.run());

        let event = tokio::time::timeout(Duration::from_secs(2), signal_rx.recv())
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "signal channel closed"))?;
        assert!(matches!(event, SignalEvent::CandleClosed(_)));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), signal_rx.recv())
                .await
                .is_err(),
            "duplicate final candle must not be emitted"
        );

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), engine_task).await???;
        tokio::time::timeout(Duration::from_secs(2), server).await???;
        Ok(())
    }
}
