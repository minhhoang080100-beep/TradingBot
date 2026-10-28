//! Bounded REST transport and a single authenticated private-stream session.
//!
//! Placement requests are never retried. Reconnect supervisors must create fresh
//! signatures/listen keys and reconcile orders and account positions before
//! enabling entries again. Authentication and subscription acknowledgements do
//! not establish account continuity.

use std::{fmt, sync::Arc, time::Duration};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::mpsc,
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;
use url::Url;

use super::{
    ApiCredentials, ExecutionAdapter, ExecutionError, OrderAck, OrderCommand, SignedRequest,
    UserEvent,
};
use crate::models::Exchange;

const MAX_PAYLOAD_BYTES: usize = 1_048_576;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);
type PrivateSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Explicit REST account environment; demo uses separate exchange credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionEnvironment {
    /// Exchange demo-trading account.
    Demo,
    /// Exchange production account.
    Production,
}

impl ExecutionEnvironment {
    fn origin(self, exchange: Exchange) -> &'static str {
        match (exchange, self) {
            (Exchange::Binance, Self::Demo) => "https://demo-fapi.binance.com",
            (Exchange::Binance, Self::Production) => "https://fapi.binance.com",
            (Exchange::Bybit, Self::Demo) => "https://api-demo.bybit.com",
            (Exchange::Bybit, Self::Production) => "https://api.bybit.com",
            (Exchange::Okx, _) => "https://openapi.okx.com",
        }
    }
}

/// Bounded HTTP response whose debug output excludes account data and tokens.
pub struct RestResponse {
    status: u16,
    body: Bytes,
}

impl RestResponse {
    /// HTTP status code returned by the exchange.
    #[must_use]
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Exact response bytes, for a venue parser or listen-key extraction.
    #[must_use]
    pub const fn body(&self) -> &Bytes {
        &self.body
    }
}

impl fmt::Debug for RestResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestResponse")
            .field("status", &self.status)
            .field("body", &"[REDACTED]")
            .finish()
    }
}

/// Reusable REST client pinned to a trusted exchange origin.
pub struct RestExecutor {
    adapter: Arc<dyn ExecutionAdapter>,
    credentials: Arc<ApiCredentials>,
    client: reqwest::Client,
    origin: String,
    environment: ExecutionEnvironment,
}

impl RestExecutor {
    /// Venue pinned to this HTTP client; does not verify the remote account identity.
    #[must_use]
    pub fn exchange(&self) -> Exchange {
        self.adapter.exchange()
    }

    /// Reads a bounded inventory with this executor's credential source and origin.
    /// The result never authorizes entry or replaces cumulative order recovery.
    ///
    /// # Errors
    /// Fails closed on protocol, pagination, environment, deadline or cancellation errors.
    pub async fn inventory(
        &self,
        adapter: &dyn crate::InventoryAdapter,
        limits: crate::InventoryLimits,
        cancel: &CancellationToken,
    ) -> Result<crate::InventoryReport, crate::InventoryError> {
        crate::scan_inventory(adapter, &self.credentials, self, limits, cancel).await
    }

    /// Builds a client with explicit timeout, no redirects, and no retries.
    ///
    /// OKX requires its adapter's demo setting to agree with `environment`;
    /// every request is checked before transmission.
    ///
    /// # Errors
    /// Returns an error for a zero timeout or failed HTTP client configuration.
    pub fn new(
        adapter: Arc<dyn ExecutionAdapter>,
        credentials: Arc<ApiCredentials>,
        environment: ExecutionEnvironment,
        timeout: Duration,
    ) -> Result<Self, ExecutionError> {
        if timeout.is_zero() {
            return Err(ExecutionError::InvalidRequest(
                "HTTP timeout must be positive",
            ));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(timeout)
            .connect_timeout(timeout.min(IO_TIMEOUT))
            .build()
            .map_err(|_| ExecutionError::InvalidRequest("HTTP client configuration failed"))?;
        let origin = environment.origin(adapter.exchange()).to_owned();
        Ok(Self {
            adapter,
            credentials,
            client,
            origin,
            environment,
        })
    }

    /// Signs and sends an operation exactly once, then parses the REST response.
    ///
    /// # Errors
    /// Validation errors occur before sending. Transport errors are ambiguous:
    /// callers must reconcile by client ID before any placement retry.
    pub async fn execute(
        &self,
        command: &OrderCommand,
        at: DateTime<Utc>,
    ) -> Result<OrderAck, ExecutionError> {
        let request = self.adapter.prepare(command, &self.credentials, at)?;
        let response = self.send_signed(&request).await?;
        self.adapter
            .parse_response(command, response.status, &response.body)
    }

    /// Sends exact signed bytes to this executor's fixed venue origin once.
    ///
    /// Supports API-key-only Binance listen-key management as well as ordinary
    /// signed requests. The caller must obtain the request from this venue's
    /// adapter; never log authentication headers or the returned response body.
    ///
    /// # Errors
    /// Returns an error for an environment mismatch, transport failure, or a
    /// response exceeding one MiB. A transport failure does not prove rejection.
    pub async fn send_signed(
        &self,
        request: &SignedRequest,
    ) -> Result<RestResponse, ExecutionError> {
        self.validate_environment(request)?;
        let mut endpoint = format!("{}{}", self.origin, request.path());
        if !request.query().is_empty() {
            endpoint.push('?');
            endpoint.push_str(request.query());
        }
        let mut builder = self.client.request(request.method().clone(), endpoint);
        for (name, value) in request.headers() {
            builder = builder.header(name.as_str(), value.as_str());
        }
        if !request.body().is_empty() {
            builder = builder.body(request.body().clone());
        }
        let mut response = builder
            .send()
            .await
            .map_err(|_| ExecutionError::AmbiguousResponse)?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_PAYLOAD_BYTES as u64)
        {
            return Err(ExecutionError::AmbiguousResponse);
        }
        let status = response.status().as_u16();
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ExecutionError::AmbiguousResponse)?
        {
            let length = body
                .len()
                .checked_add(chunk.len())
                .ok_or(ExecutionError::AmbiguousResponse)?;
            if length > MAX_PAYLOAD_BYTES {
                return Err(ExecutionError::AmbiguousResponse);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(RestResponse {
            status,
            body: Bytes::from(body),
        })
    }

    fn validate_environment(&self, request: &SignedRequest) -> Result<(), ExecutionError> {
        if self.adapter.exchange() == Exchange::Okx {
            let mut demo_headers = request
                .headers()
                .iter()
                .filter(|(name, _)| name.eq_ignore_ascii_case("x-simulated-trading"));
            let demo_header = demo_headers.next().map(|(_, value)| value.as_str());
            if demo_headers.next().is_some() {
                return Err(ExecutionError::InvalidRequest(
                    "duplicate OKX environment header",
                ));
            }
            let valid = match self.environment {
                ExecutionEnvironment::Demo => demo_header == Some("1"),
                ExecutionEnvironment::Production => {
                    demo_header.is_none() || demo_header == Some("0")
                }
            };
            if !valid {
                return Err(ExecutionError::InvalidRequest(
                    "OKX adapter and account environment differ",
                ));
            }
        }
        Ok(())
    }
}

impl fmt::Debug for RestExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestExecutor")
            .field("exchange", &self.adapter.exchange())
            .field("environment", &self.environment)
            .finish_non_exhaustive()
    }
}

/// One private-stream connection, with secret-bearing fields redacted in debug.
pub struct PrivateSessionConfig {
    url: String,
    authentication: Option<String>,
    subscriptions: Vec<String>,
    heartbeat: Option<String>,
}

impl PrivateSessionConfig {
    /// Configures one session on a trusted global exchange WebSocket origin.
    ///
    /// Binance authenticates in its listen-key URL and uses `None` for auth and
    /// heartbeat, with no subscriptions. Bybit uses a JSON ping; OKX uses `ping`.
    /// An explicit authentication frame delays all subscriptions until the
    /// exchange confirms login. Multiple subscription ACKs are forwarded.
    ///
    /// # Errors
    /// Rejects untrusted/insecure URLs, URL credentials, oversized messages, or
    /// empty authentication/subscription/heartbeat frames.
    pub fn new(
        url: String,
        authentication: Option<String>,
        subscriptions: Vec<String>,
        heartbeat: Option<String>,
    ) -> Result<Self, ExecutionError> {
        validate_private_url(&url, None)?;
        for message in authentication
            .iter()
            .chain(subscriptions.iter())
            .chain(heartbeat.iter())
        {
            if message.is_empty() || message.len() > MAX_PAYLOAD_BYTES {
                return Err(ExecutionError::InvalidRequest(
                    "invalid private stream outbound frame",
                ));
            }
        }
        if subscriptions.len() > 16 {
            return Err(ExecutionError::InvalidRequest(
                "too many private stream subscriptions",
            ));
        }
        Ok(Self {
            url,
            authentication,
            subscriptions,
            heartbeat,
        })
    }
}

impl fmt::Debug for PrivateSessionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrivateSessionConfig")
            .field("url", &"[REDACTED]")
            .field("authentication", &"[REDACTED]")
            .field("subscriptions", &self.subscriptions.len())
            .finish_non_exhaustive()
    }
}

fn validate_private_url(value: &str, exchange: Option<Exchange>) -> Result<(), ExecutionError> {
    let url = Url::parse(value)
        .map_err(|_| ExecutionError::InvalidRequest("invalid private stream URL"))?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(ExecutionError::InvalidRequest("invalid private stream URL"));
    }
    #[cfg(test)]
    if url.scheme() == "ws" && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")) {
        return Ok(());
    }
    if url.scheme() != "wss" || url.port().is_some_and(|port| port != 443) {
        return Err(ExecutionError::InvalidRequest(
            "private stream requires trusted TLS endpoint",
        ));
    }
    let venue = match url.host_str() {
        Some("fstream.binance.com" | "demo-fstream.binance.com") => Exchange::Binance,
        Some("stream.bybit.com" | "stream-demo.bybit.com") => Exchange::Bybit,
        Some("ws.okx.com" | "wspap.okx.com") => Exchange::Okx,
        _ => {
            return Err(ExecutionError::InvalidRequest(
                "private stream origin is not trusted",
            ))
        }
    };
    if exchange.is_some_and(|expected| expected != venue) {
        return Err(ExecutionError::InvalidRequest(
            "private stream venue mismatch",
        ));
    }
    let valid_path = match venue {
        Exchange::Binance => {
            url.path().starts_with("/ws/") || url.path().starts_with("/private/ws/")
        }
        Exchange::Bybit => url.path() == "/v5/private",
        Exchange::Okx => matches!(url.path(), "/ws/v5/private" | "/ws/v5/business"),
    };
    if !valid_path {
        return Err(ExecutionError::InvalidRequest(
            "invalid private stream path",
        ));
    }
    Ok(())
}

/// Runs one bounded private WebSocket session without automatic reconnect/replay.
///
/// Emits `ReconciliationRequired` before connecting and again after every exit,
/// including cancellation or connection failure. The caller must treat a closed
/// or full output channel as lost continuity. Fresh authentication and complete
/// REST reconciliation are the reconnect supervisor's responsibility.
///
/// # Errors
/// Returns sanitized connection, protocol, timeout, or downstream errors. Each
/// output send has a ten-second bound; errors never contain the URL or auth frame.
pub async fn run_private_session(
    adapter: Arc<dyn ExecutionAdapter>,
    config: PrivateSessionConfig,
    output: mpsc::Sender<UserEvent>,
    shutdown: CancellationToken,
) -> Result<(), ExecutionError> {
    let result = async {
        emit(&output, UserEvent::ReconciliationRequired).await?;
        validate_private_url(&config.url, Some(adapter.exchange()))?;
        if adapter.exchange() != Exchange::Binance && config.authentication.is_none() {
            return Err(ExecutionError::InvalidRequest(
                "private stream authentication is required",
            ));
        }
        session_loop(adapter.as_ref(), &config, &output, &shutdown).await
    }
    .await;
    emit(&output, UserEvent::ReconciliationRequired).await?;
    result
}

async fn session_loop(
    adapter: &dyn ExecutionAdapter,
    config: &PrivateSessionConfig,
    output: &mpsc::Sender<UserEvent>,
    shutdown: &CancellationToken,
) -> Result<(), ExecutionError> {
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(MAX_PAYLOAD_BYTES))
        .max_frame_size(Some(MAX_PAYLOAD_BYTES))
        .max_write_buffer_size(MAX_PAYLOAD_BYTES * 2);
    let connection = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Ok(()),
        result = tokio::time::timeout(IO_TIMEOUT, connect_async_with_config(config.url.as_str(), Some(ws_config), true)) => {
            result.map_err(|_| ExecutionError::AmbiguousResponse)?
                .map_err(|_| ExecutionError::AmbiguousResponse)?
        }
    };
    let (mut socket, _) = connection;
    let mut authenticated = config.authentication.is_none();
    if let Some(authentication) = &config.authentication {
        write_message(&mut socket, Message::Text(authentication.clone().into())).await?;
    } else {
        subscribe(&mut socket, &config.subscriptions).await?;
    }
    let authentication_deadline = Instant::now() + IO_TIMEOUT;
    let mut last_received = Instant::now();
    let mut heartbeat =
        tokio::time::interval_at(Instant::now() + HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                let _close = tokio::time::timeout(IO_TIMEOUT, socket.close(None)).await;
                return Ok(());
            }
            () = tokio::time::sleep_until(authentication_deadline), if !authenticated => {
                return Err(ExecutionError::Protocol("private stream authentication timed out"));
            }
            () = tokio::time::sleep_until(last_received + INACTIVITY_TIMEOUT) => {
                return Err(ExecutionError::Protocol("private stream heartbeat timed out"));
            }
            _tick = heartbeat.tick() => {
                if let Some(payload) = &config.heartbeat {
                    write_message(&mut socket, Message::Text(payload.clone().into())).await?;
                } else {
                    write_message(&mut socket, Message::Ping(Bytes::new())).await?;
                }
            }
            incoming = socket.next() => {
                let incoming = incoming.ok_or(ExecutionError::ReconciliationRequired)?
                    .map_err(|_| ExecutionError::AmbiguousResponse)?;
                last_received = Instant::now();
                let events = match incoming {
                    Message::Text(text) => adapter.parse_user_message(text.as_bytes())?,
                    Message::Binary(bytes) => adapter.parse_user_message(&bytes)?,
                    Message::Ping(payload) => {
                        write_message(&mut socket, Message::Pong(payload)).await?;
                        continue;
                    }
                    Message::Pong(_) => vec![UserEvent::Heartbeat],
                    Message::Close(_) => return Err(ExecutionError::ReconciliationRequired),
                    Message::Frame(_) => return Err(ExecutionError::Protocol("unexpected raw private frame")),
                };
                let delivery_deadline = Instant::now() + IO_TIMEOUT;
                for event in events {
                    if !authenticated {
                        match event {
                            UserEvent::Authenticated => {
                                authenticated = true;
                                subscribe(&mut socket, &config.subscriptions).await?;
                            }
                            UserEvent::Heartbeat | UserEvent::Ignored => {}
                            _ => return Err(ExecutionError::Protocol("private data arrived before authentication")),
                        }
                    }
                    tokio::time::timeout_at(delivery_deadline, emit(output, event)).await
                        .map_err(|_| ExecutionError::OutputClosed)??;
                }
            }
        }
    }
}

async fn subscribe(
    socket: &mut PrivateSocket,
    subscriptions: &[String],
) -> Result<(), ExecutionError> {
    let deadline = Instant::now() + IO_TIMEOUT;
    for subscription in subscriptions {
        tokio::time::timeout_at(
            deadline,
            write_message(socket, Message::Text(subscription.clone().into())),
        )
        .await
        .map_err(|_| ExecutionError::AmbiguousResponse)??;
    }
    Ok(())
}

async fn write_message(socket: &mut PrivateSocket, message: Message) -> Result<(), ExecutionError> {
    tokio::time::timeout(IO_TIMEOUT, socket.send(message))
        .await
        .map_err(|_| ExecutionError::AmbiguousResponse)?
        .map_err(|_| ExecutionError::AmbiguousResponse)
}

async fn emit(output: &mpsc::Sender<UserEvent>, event: UserEvent) -> Result<(), ExecutionError> {
    tokio::time::timeout(IO_TIMEOUT, output.send(event))
        .await
        .map_err(|_| ExecutionError::OutputClosed)?
        .map_err(|_| ExecutionError::OutputClosed)
}

#[cfg(test)]
mod inventory_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::{OrderKind, OrderRequest, OrderSide},
        models::{InstrumentId, MarketKind},
    };
    use reqwest::Method;
    use rust_decimal::Decimal;
    use std::error::Error;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;
    use wiremock::{
        matchers::{body_bytes, header, method, path, query_param},
        Mock, MockServer, ResponseTemplate,
    };

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    struct TestAdapter;

    impl ExecutionAdapter for TestAdapter {
        fn exchange(&self) -> Exchange {
            Exchange::Bybit
        }
        fn prepare(
            &self,
            _command: &OrderCommand,
            _credentials: &ApiCredentials,
            _at: DateTime<Utc>,
        ) -> Result<SignedRequest, ExecutionError> {
            test_request("/order")
        }
        fn parse_response(
            &self,
            command: &OrderCommand,
            status: u16,
            _body: &[u8],
        ) -> Result<OrderAck, ExecutionError> {
            if status != 200 {
                return Err(ExecutionError::HttpStatus(status));
            }
            Ok(OrderAck {
                client_order_id: command.client_order_id().to_owned(),
                venue_order_id: None,
                status: None,
            })
        }
        fn parse_user_message(&self, payload: &[u8]) -> Result<Vec<UserEvent>, ExecutionError> {
            Ok(vec![match payload {
                b"authenticated" => UserEvent::Authenticated,
                b"subscribed" => UserEvent::Subscribed,
                b"pong" => UserEvent::Heartbeat,
                _ => return Err(ExecutionError::Protocol("unrecognized test frame")),
            }])
        }
    }

    fn test_request(path: &str) -> Result<SignedRequest, ExecutionError> {
        SignedRequest::new(
            Method::POST,
            path.to_owned(),
            "signed=a%2Bb".to_owned(),
            Bytes::from_static(b"{\"qty\":\"0.001\"}"),
            vec![("X-Test-Key".to_owned(), "secret-token".to_owned())],
        )
    }

    fn executor(server: &MockServer, timeout: Duration) -> Result<RestExecutor, ExecutionError> {
        let mut executor = RestExecutor::new(
            Arc::new(TestAdapter),
            Arc::new(ApiCredentials::new(
                "test-key".to_owned(),
                "test-secret".to_owned(),
                None,
            )?),
            ExecutionEnvironment::Demo,
            timeout,
        )?;
        // Only this private test helper can replace the fixed production origin.
        executor.origin = server.uri();
        Ok(executor)
    }

    #[tokio::test]
    async fn rest_transmits_exact_signed_payload_once_and_ack_is_not_fill() -> TestResult {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/order"))
            .and(query_param("signed", "a+b"))
            .and(header("X-Test-Key", "secret-token"))
            .and(body_bytes(b"{\"qty\":\"0.001\"}".to_vec()))
            .respond_with(ResponseTemplate::new(200).set_body_string("accepted"))
            .expect(1)
            .mount(&server)
            .await;
        let executor = executor(&server, Duration::from_secs(2))?;
        let request = OrderRequest {
            instrument: InstrumentId::new(
                Exchange::Bybit,
                MarketKind::LinearPerpetual,
                "BTCUSDT",
                "BTC-USDT-PERP",
            )?,
            client_order_id: "ictentry001".to_owned(),
            side: OrderSide::Buy,
            kind: OrderKind::Limit,
            quantity: Decimal::ONE,
            price: Some(Decimal::from(100)),
            trigger_price: None,
            reduce_only: false,
        };
        let ack = executor
            .execute(&OrderCommand::Place(request), Utc::now())
            .await?;
        assert_eq!(ack.status, None);
        Ok(())
    }

    #[tokio::test]
    async fn rest_redirects_are_not_followed_and_timeout_is_not_retried() -> TestResult {
        let server = MockServer::start().await;
        Mock::given(path("/redirect"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("Location", format!("{}/destination", server.uri())),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(path("/destination"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(path("/slow"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(250)))
            .expect(1)
            .mount(&server)
            .await;
        let executor = executor(&server, Duration::from_millis(50))?;
        assert_eq!(
            executor
                .send_signed(&test_request("/redirect")?)
                .await?
                .status(),
            307
        );
        assert!(matches!(
            executor.send_signed(&test_request("/slow")?).await,
            Err(ExecutionError::AmbiguousResponse)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn rest_rejects_oversized_response_without_exposing_body() -> TestResult {
        let server = MockServer::start().await;
        Mock::given(path("/large"))
            .respond_with(
                ResponseTemplate::new(200).set_body_bytes(vec![b'x'; MAX_PAYLOAD_BYTES + 1]),
            )
            .expect(1)
            .mount(&server)
            .await;
        let executor = executor(&server, Duration::from_secs(2))?;
        assert!(matches!(
            executor.send_signed(&test_request("/large")?).await,
            Err(ExecutionError::AmbiguousResponse)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn private_stream_waits_for_auth_forwards_topic_acks_and_marks_disconnect() -> TestResult
    {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let auth = socket.next().await.ok_or("missing login")??;
            assert_eq!(auth.into_text()?, "secret-login");
            assert!(
                tokio::time::timeout(Duration::from_millis(30), socket.next())
                    .await
                    .is_err()
            );
            socket.send(Message::Text("authenticated".into())).await?;
            for expected in ["orders-subscription", "positions-subscription"] {
                let subscription = socket.next().await.ok_or("missing subscription")??;
                assert_eq!(subscription.into_text()?, expected);
                socket.send(Message::Text("subscribed".into())).await?;
            }
            socket
                .send(Message::Ping(Bytes::from_static(b"heartbeat-token")))
                .await?;
            let pong = socket.next().await.ok_or("missing pong")??;
            assert_eq!(pong, Message::Pong(Bytes::from_static(b"heartbeat-token")));
            socket.close(None).await?;
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        });
        let config = PrivateSessionConfig::new(
            format!("ws://{address}"),
            Some("secret-login".to_owned()),
            vec![
                "orders-subscription".to_owned(),
                "positions-subscription".to_owned(),
            ],
            Some("ping".to_owned()),
        )?;
        let (tx, mut rx) = mpsc::channel(16);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            run_private_session(Arc::new(TestAdapter), config, tx, CancellationToken::new()),
        )
        .await?;
        assert!(matches!(
            result,
            Err(ExecutionError::ReconciliationRequired)
        ));
        tokio::time::timeout(Duration::from_secs(3), server).await???;
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        assert_eq!(
            events,
            vec![
                UserEvent::ReconciliationRequired,
                UserEvent::Authenticated,
                UserEvent::Subscribed,
                UserEvent::Subscribed,
                UserEvent::ReconciliationRequired
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_session_still_requires_reconciliation() -> TestResult {
        let config = PrivateSessionConfig::new(
            "wss://stream-demo.bybit.com/v5/private".to_owned(),
            Some("secret-login".to_owned()),
            Vec::new(),
            None,
        )?;
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let (tx, mut rx) = mpsc::channel(4);
        run_private_session(Arc::new(TestAdapter), config, tx, shutdown).await?;
        assert_eq!(rx.recv().await, Some(UserEvent::ReconciliationRequired));
        assert_eq!(rx.recv().await, Some(UserEvent::ReconciliationRequired));
        Ok(())
    }

    #[tokio::test]
    async fn private_stream_rejects_data_before_authentication() -> TestResult {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let _auth = socket.next().await.ok_or("missing login")??;
            socket.send(Message::Text("subscribed".into())).await?;
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        });
        let config = PrivateSessionConfig::new(
            format!("ws://{address}"),
            Some("secret-login".to_owned()),
            vec!["subscription".to_owned()],
            None,
        )?;
        let (tx, mut rx) = mpsc::channel(8);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            run_private_session(Arc::new(TestAdapter), config, tx, CancellationToken::new()),
        )
        .await?;
        assert!(matches!(
            result,
            Err(ExecutionError::Protocol(
                "private data arrived before authentication"
            ))
        ));
        tokio::time::timeout(Duration::from_secs(3), server).await???;
        assert_eq!(rx.recv().await, Some(UserEvent::ReconciliationRequired));
        assert_eq!(rx.recv().await, Some(UserEvent::ReconciliationRequired));
        assert_eq!(rx.recv().await, None);
        Ok(())
    }

    #[tokio::test]
    async fn private_stream_rejects_oversized_frames() -> TestResult {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let _auth = socket.next().await.ok_or("missing login")??;
            let _send = socket
                .send(Message::Text("x".repeat(MAX_PAYLOAD_BYTES + 1).into()))
                .await;
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        });
        let config = PrivateSessionConfig::new(
            format!("ws://{address}"),
            Some("secret-login".to_owned()),
            Vec::new(),
            None,
        )?;
        let (tx, mut rx) = mpsc::channel(8);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            run_private_session(Arc::new(TestAdapter), config, tx, CancellationToken::new()),
        )
        .await?;
        assert!(matches!(result, Err(ExecutionError::AmbiguousResponse)));
        tokio::time::timeout(Duration::from_secs(3), server).await???;
        assert_eq!(rx.recv().await, Some(UserEvent::ReconciliationRequired));
        assert_eq!(rx.recv().await, Some(UserEvent::ReconciliationRequired));
        Ok(())
    }

    #[test]
    fn private_configuration_redacts_and_rejects_untrusted_origins() -> TestResult {
        let config = PrivateSessionConfig::new(
            "wss://fstream.binance.com/private/ws/secret-token".to_owned(),
            None,
            Vec::new(),
            None,
        )?;
        assert!(!format!("{config:?}").contains("secret-token"));
        assert!(PrivateSessionConfig::new(
            "wss://attacker.invalid/v5/private".to_owned(),
            Some("secret-login".to_owned()),
            Vec::new(),
            None
        )
        .is_err());
        assert!(
            validate_private_url("wss://ws.okx.com/ws/v5/private", Some(Exchange::Bybit)).is_err()
        );
        Ok(())
    }
}
