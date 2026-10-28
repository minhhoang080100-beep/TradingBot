//! Optional, bounded Telegram notifications for the PAPER runner.
//!
//! This module sends plain text only. It never receives trading commands. An
//! accepted queue item is not a delivery acknowledgement; timeouts and transport
//! errors have an unknown delivery outcome and are never automatically retried.
//! Only an explicit HTTP 429 with Telegram's bounded `retry_after` is retried,
//! once. Tokens, request URLs, message text and response bodies are never logged.

use std::{
    env, fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const ORIGIN: &str = "https://api.telegram.org";
const QUEUE_CAPACITY: usize = 64;
const MAX_TEXT_CHARS: usize = 4096;
const MAX_RESPONSE_BYTES: usize = 65_536;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Explicitly enabled Telegram destination, with a secret bot token.
pub struct TelegramConfig {
    token: SecretString,
    chat_id: i64,
}

impl fmt::Debug for TelegramConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramConfig")
            .field("token", &"[REDACTED]")
            .field("chat_id", &"[REDACTED]")
            .finish()
    }
}

impl TelegramConfig {
    /// Reads opt-in configuration without making a network request.
    ///
    /// Missing `TELEGRAM_ENABLED` means disabled. Only `true` and `false` are
    /// accepted (case-insensitive). Token and chat ID are read only when enabled.
    /// Numeric private/group chat IDs are supported; zero and usernames are not.
    ///
    /// # Errors
    /// Returns sanitized errors for invalid opt-in, absent credentials, an unsafe
    /// token path segment, or a non-numeric/zero chat ID. Values are never echoed.
    pub fn from_env() -> Result<Option<Self>, TelegramError> {
        let enabled = match env::var("TELEGRAM_ENABLED") {
            Ok(value) => enabled_value(Some(&value))?,
            Err(env::VarError::NotPresent) => false,
            Err(env::VarError::NotUnicode(_)) => return Err(TelegramError::InvalidEnabled),
        };
        if !enabled {
            return Ok(None);
        }
        let token = env::var("TELEGRAM_BOT_TOKEN").map_err(|_| TelegramError::MissingToken)?;
        let chat_id = env::var("TELEGRAM_CHAT_ID").map_err(|_| TelegramError::InvalidChatId)?;
        Self::from_values(token, &chat_id).map(Some)
    }

    fn from_values(token: String, chat_id: &str) -> Result<Self, TelegramError> {
        if token.is_empty() {
            return Err(TelegramError::MissingToken);
        }
        // A token is a URL path component. Refuse '/', '?', '#', whitespace,
        // userinfo and encoded separators rather than constructing an unsafe URL.
        let valid_token = token.split_once(':').is_some_and(|(id, secret)| {
            !id.is_empty()
                && id.bytes().all(|byte| byte.is_ascii_digit())
                && !secret.is_empty()
                && secret
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        });
        if !valid_token {
            return Err(TelegramError::InvalidToken);
        }
        let chat_id = chat_id
            .parse::<i64>()
            .ok()
            .filter(|value| *value != 0)
            .ok_or(TelegramError::InvalidChatId)?;
        Ok(Self {
            token: SecretString::from(token),
            chat_id,
        })
    }
}

fn enabled_value(value: Option<&str>) -> Result<bool, TelegramError> {
    match value {
        None => Ok(false),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(_) => Err(TelegramError::InvalidEnabled),
    }
}

/// Configuration and nonblocking enqueue failures; contains no secret values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TelegramError {
    /// Opt-in must be explicitly true or false.
    #[error("TELEGRAM_ENABLED must be true or false")]
    InvalidEnabled,
    /// Enabled notifications require a nonempty token.
    #[error("TELEGRAM_BOT_TOKEN is required when Telegram is enabled")]
    MissingToken,
    /// Token cannot safely be used in the fixed Telegram URL path.
    #[error("TELEGRAM_BOT_TOKEN has an invalid format")]
    InvalidToken,
    /// Destination must be a nonzero signed integer.
    #[error("TELEGRAM_CHAT_ID must be a nonzero numeric chat ID")]
    InvalidChatId,
    /// HTTP client setup failed; underlying errors are intentionally discarded.
    #[error("Telegram HTTP client setup failed")]
    ClientSetup,
    /// Spawning requires a Tokio runtime.
    #[error("Telegram worker requires an active Tokio runtime")]
    RuntimeUnavailable,
    /// Empty messages are not accepted by sendMessage.
    #[error("Telegram message is empty")]
    EmptyMessage,
    /// Plain text is limited to 4096 Unicode characters.
    #[error("Telegram message exceeds 4096 characters")]
    MessageTooLong,
    /// The bounded queue cannot accept another item immediately.
    #[error("Telegram notification queue is full; message was not queued")]
    QueueFull,
    /// The worker has already stopped or shutdown has started.
    #[error("Telegram notification worker is closed; message was not queued")]
    WorkerClosed,
}

/// Delivery counters; success means Telegram acknowledged the requested chat.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TelegramStats {
    /// Number of messages accepted into the bounded queue.
    pub accepted: u64,
    /// Messages acknowledged by Telegram for the configured destination.
    pub delivered: u64,
    /// Attempts without a valid success acknowledgement. Timeout/transport
    /// failures may still have reached Telegram and are not retried.
    pub failed: u64,
    /// Messages rejected before enqueueing (invalid text, full/closed queue).
    pub rejected: u64,
    /// Accepted items not yet resolved, including work abandoned at shutdown.
    pub unconfirmed: u64,
}

#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    delivered: AtomicU64,
    failed: AtomicU64,
    rejected: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> TelegramStats {
        let accepted = self.accepted.load(Ordering::Relaxed);
        let delivered = self.delivered.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        TelegramStats {
            accepted,
            delivered,
            failed,
            rejected: self.rejected.load(Ordering::Relaxed),
            unconfirmed: accepted.saturating_sub(delivered).saturating_sub(failed),
        }
    }
}

/// Owner of a single background sender with a 64-item queue.
///
/// Call [`Self::shutdown`] to drain for at most five seconds. Dropping the owner
/// cancels pending work immediately and warns about unconfirmed notifications.
pub struct TelegramNotifier {
    sender: Option<mpsc::Sender<String>>,
    task: Option<JoinHandle<()>>,
    cancel: CancellationToken,
    counters: Arc<Counters>,
}

impl fmt::Debug for TelegramNotifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramNotifier")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl TelegramNotifier {
    /// Starts a background worker; no request is sent until a message is queued.
    ///
    /// # Errors
    /// Returns sanitized setup errors if the HTTP client/runtime is unavailable.
    pub fn spawn(config: TelegramConfig) -> Result<Self, TelegramError> {
        Self::start(TelegramClient::new(config)?)
    }

    fn start(client: TelegramClient) -> Result<Self, TelegramError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| TelegramError::RuntimeUnavailable)?;
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let cancel = CancellationToken::new();
        let counters = Arc::new(Counters::default());
        let task = runtime.spawn(run_worker(
            client,
            receiver,
            cancel.clone(),
            Arc::clone(&counters),
        ));
        eprintln!("Telegram notification worker enabled; queue capacity=64; delivery is best-effort");
        Ok(Self {
            sender: Some(sender),
            task: Some(task),
            cancel,
            counters,
        })
    }

    /// Enqueues plain UTF-8 text without waiting for network or queue capacity.
    /// Text is never truncated, logged, or parsed as Markdown/HTML.
    ///
    /// # Errors
    /// Rejects empty/oversized text and full/closed queues, increments `rejected`
    /// and emits a sanitized warning. Success only acknowledges enqueueing.
    pub fn try_send(&self, text: String) -> Result<(), TelegramError> {
        let result = validate_text(&text).and_then(|()| {
            self.sender
                .as_ref()
                .ok_or(TelegramError::WorkerClosed)?
                .try_send(text)
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => TelegramError::QueueFull,
                    mpsc::error::TrySendError::Closed(_) => TelegramError::WorkerClosed,
                })
        });
        match result {
            Ok(()) => {
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                self.counters.rejected.fetch_add(1, Ordering::Relaxed);
                eprintln!("Telegram notification rejected: {error}");
                Err(error)
            }
        }
    }

    /// Returns aggregate counters without disclosing a message or destination.
    #[must_use]
    pub fn stats(&self) -> TelegramStats {
        self.counters.snapshot()
    }

    /// Stops accepting messages, drains for at most five seconds, then aborts
    /// outstanding work. An interrupted request has an unknown delivery outcome.
    /// Returns and logs all aggregate counters, including unconfirmed items.
    pub async fn shutdown(mut self) -> TelegramStats {
        self.shutdown_with_timeout(SHUTDOWN_TIMEOUT).await
    }

    async fn shutdown_with_timeout(&mut self, timeout: Duration) -> TelegramStats {
        self.sender.take();
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(timeout, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    eprintln!("Telegram worker terminated without confirming all queued messages");
                }
                Err(_) => {
                    self.cancel.cancel();
                    task.abort();
                    // Join cancellation is bounded as well; do not hold up the
                    // trading process while a remote notification is uncertain.
                    let _ = tokio::time::timeout(Duration::from_millis(100), task).await;
                    eprintln!("Telegram shutdown deadline reached; outstanding delivery is unconfirmed");
                }
            }
        }
        let stats = self.stats();
        eprintln!("Telegram notification summary: {stats:?}");
        stats
    }
}

impl Drop for TelegramNotifier {
    fn drop(&mut self) {
        self.sender.take();
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
            eprintln!(
                "Telegram worker dropped without a drain; notification summary: {:?}",
                self.stats()
            );
        }
    }
}

fn validate_text(text: &str) -> Result<(), TelegramError> {
    if text.is_empty() {
        return Err(TelegramError::EmptyMessage);
    }
    if text.chars().take(MAX_TEXT_CHARS + 1).count() > MAX_TEXT_CHARS {
        return Err(TelegramError::MessageTooLong);
    }
    Ok(())
}

async fn run_worker(
    client: TelegramClient,
    mut receiver: mpsc::Receiver<String>,
    cancel: CancellationToken,
    counters: Arc<Counters>,
) {
    loop {
        let text = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            text = receiver.recv() => match text {
                Some(text) => text,
                None => return,
            },
        };
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            result = client.send(&text) => result,
        };
        match result {
            Ok(()) => {
                counters.delivered.fetch_add(1, Ordering::Relaxed);
                eprintln!("Telegram notification delivered (chat acknowledgement validated)");
            }
            Err(error) => {
                counters.failed.fetch_add(1, Ordering::Relaxed);
                eprintln!("Telegram notification not confirmed: {error}; no further retry");
            }
        }
    }
}

struct TelegramClient {
    config: TelegramConfig,
    client: reqwest::Client,
    origin: String,
}

impl TelegramClient {
    fn new(config: TelegramConfig) -> Result<Self, TelegramError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| TelegramError::ClientSetup)?;
        Ok(Self {
            config,
            client,
            origin: ORIGIN.to_owned(),
        })
    }

    async fn send(&self, text: &str) -> Result<(), DeliveryError> {
        let first = self.send_once(text).await;
        if let Err(DeliveryError::RateLimited(Some(seconds @ 0..=30))) = first {
            eprintln!("Telegram rate limit acknowledged; retrying once after {seconds} seconds");
            tokio::time::sleep(Duration::from_secs(seconds)).await;
            return self.send_once(text).await;
        }
        first
    }

    async fn send_once(&self, text: &str) -> Result<(), DeliveryError> {
        let mut response = self
            .client
            .post(format!(
                "{}/bot{}/sendMessage",
                self.origin,
                self.config.token.expose_secret()
            ))
            .json(&SendMessage {
                chat_id: self.config.chat_id,
                text,
            })
            .send()
            .await
            .map_err(|error| sanitize_transport(&error))?;
        let status = response.status().as_u16();
        if !(200..300).contains(&status) && status != 429 {
            return Err(DeliveryError::HttpStatus(status));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            return Err(DeliveryError::ResponseTooLarge);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| sanitize_transport(&error))?
        {
            if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(body.len()) {
                return Err(DeliveryError::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        let envelope: Envelope =
            serde_json::from_slice(&body).map_err(|_| DeliveryError::InvalidResponse)?;
        if status == 429 {
            if !envelope.ok && envelope.error_code == Some(429) {
                return Err(DeliveryError::RateLimited(
                    envelope.parameters.and_then(|value| value.retry_after),
                ));
            }
            return Err(DeliveryError::HttpStatus(429));
        }
        if !envelope.ok {
            return Err(DeliveryError::Rejected(envelope.error_code));
        }
        let message = envelope.result.ok_or(DeliveryError::InvalidResponse)?;
        if message.chat.id != self.config.chat_id {
            return Err(DeliveryError::ChatMismatch);
        }
        Ok(())
    }

    #[cfg(test)]
    fn local(origin: String, timeout: Duration) -> Result<Self, TelegramError> {
        let config = TelegramConfig::from_values("123:test-secret".to_owned(), "42")?;
        let mut client = Self::new(config)?;
        client.origin = origin;
        client.client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(timeout)
            .build()
            .map_err(|_| TelegramError::ClientSetup)?;
        Ok(client)
    }
}

#[derive(Serialize)]
struct SendMessage<'a> {
    chat_id: i64,
    text: &'a str,
}

#[derive(Deserialize)]
struct Envelope {
    ok: bool,
    result: Option<SentMessage>,
    error_code: Option<u16>,
    parameters: Option<ResponseParameters>,
}

#[derive(Deserialize)]
struct SentMessage {
    chat: SentChat,
}

#[derive(Deserialize)]
struct SentChat {
    id: i64,
}

#[derive(Deserialize)]
struct ResponseParameters {
    retry_after: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum DeliveryError {
    #[error("request timed out (delivery outcome unknown)")]
    Timeout,
    #[error("transport failed (delivery outcome unknown)")]
    Transport,
    #[error("HTTP status {0}")]
    HttpStatus(u16),
    #[error("response exceeds 64 KiB")]
    ResponseTooLarge,
    #[error("invalid Telegram response")]
    InvalidResponse,
    #[error("Telegram rejected the request (code {0:?})")]
    Rejected(Option<u16>),
    #[error("rate limited (retry_after {0:?})")]
    RateLimited(Option<u64>),
    #[error("Telegram acknowledgement names a different chat")]
    ChatMismatch,
}

fn sanitize_transport(error: &reqwest::Error) -> DeliveryError {
    // reqwest errors can contain the complete URL, including the bot token.
    if error.is_timeout() {
        DeliveryError::Timeout
    } else {
        DeliveryError::Transport
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::{
        matchers::{body_json, method, path},
        Mock, MockServer, ResponseTemplate,
    };

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
    const SEND_PATH: &str = "/bot123:test-secret/sendMessage";

    fn acknowledgement() -> serde_json::Value {
        json!({"ok": true, "result": {"message_id": 1, "chat": {"id": 42}}})
    }

    #[test]
    fn opt_in_config_and_debug_never_expose_credentials() -> TestResult {
        assert!(!enabled_value(None)?);
        assert!(!enabled_value(Some("false"))?);
        assert!(enabled_value(Some("TRUE"))?);
        assert!(enabled_value(Some("1")).is_err());
        let config = TelegramConfig::from_values("123:test-secret".to_owned(), "-42")?;
        assert!(!format!("{config:?}").contains("test-secret"));
        for token in ["123:x/y", "123:x?y", "123:x%2Fy", "123:x#y", "", "123:"] {
            assert!(TelegramConfig::from_values(token.to_owned(), "42").is_err());
        }
        for chat in ["0", "@username", "", "9223372036854775808"] {
            assert!(TelegramConfig::from_values("123:test-secret".to_owned(), chat).is_err());
        }
        assert!(validate_text("").is_err());
        assert!(validate_text(&"a".repeat(4097)).is_err());
        assert!(validate_text(&"ế".repeat(4096)).is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn plain_utf8_success_checks_chat_and_drains_queue() -> TestResult {
        let server = MockServer::start().await;
        let text = "PAPER BTCUSDT: đã chạm TP <không phải HTML>";
        Mock::given(method("POST"))
            .and(path(SEND_PATH))
            .and(body_json(json!({"chat_id":42,"text":text})))
            .respond_with(ResponseTemplate::new(200).set_body_json(acknowledgement()))
            .expect(1)
            .mount(&server)
            .await;
        let notifier = TelegramNotifier::start(TelegramClient::local(
            server.uri(),
            Duration::from_secs(1),
        )?)?;
        notifier.try_send(text.to_owned())?;
        let stats = notifier.shutdown().await;
        assert_eq!((stats.accepted, stats.delivered, stats.failed), (1, 1, 0));
        assert_eq!(stats.unconfirmed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn authentication_forbidden_and_server_errors_are_not_retried() -> TestResult {
        for status in [401, 403, 500] {
            let server = MockServer::start().await;
            Mock::given(path(SEND_PATH))
                .respond_with(ResponseTemplate::new(status).set_body_string("test-secret"))
                .expect(1)
                .mount(&server)
                .await;
            let client = TelegramClient::local(server.uri(), Duration::from_secs(1))?;
            let result = client.send("secret text").await;
            assert_eq!(result, Err(DeliveryError::HttpStatus(status)));
            assert!(!format!("{result:?}").contains("test-secret"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn trusted_429_retries_at_most_once() -> TestResult {
        for eventually_succeeds in [true, false] {
            let server = MockServer::start().await;
            let calls = AtomicU64::new(0);
            Mock::given(path(SEND_PATH))
                .respond_with(move |_: &wiremock::Request| {
                    if calls.fetch_add(1, Ordering::Relaxed) > 0 && eventually_succeeds {
                        ResponseTemplate::new(200).set_body_json(acknowledgement())
                    } else {
                        ResponseTemplate::new(429).set_body_json(json!({
                            "ok":false,"error_code":429,"parameters":{"retry_after":0}
                        }))
                    }
                })
                .expect(2)
                .mount(&server)
                .await;
            let client = TelegramClient::local(server.uri(), Duration::from_secs(1))?;
            let expected = if eventually_succeeds {
                Ok(())
            } else {
                Err(DeliveryError::RateLimited(Some(0)))
            };
            assert_eq!(client.send("PAPER").await, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn missing_excessive_or_untrusted_retry_advice_is_not_retried() -> TestResult {
        for body in [
            json!({"ok":false,"error_code":429}),
            json!({"ok":false,"error_code":429,"parameters":{"retry_after":31}}),
            json!({"ok":false,"error_code":403,"parameters":{"retry_after":0}}),
            json!({"ok":true,"parameters":{"retry_after":0}}),
        ] {
            let server = MockServer::start().await;
            Mock::given(path(SEND_PATH))
                .respond_with(ResponseTemplate::new(429).set_body_json(body))
                .expect(1)
                .mount(&server)
                .await;
            let client = TelegramClient::local(server.uri(), Duration::from_secs(1))?;
            assert!(client.send("PAPER").await.is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn timeout_transport_and_redirect_errors_never_leak_or_retry() -> TestResult {
        let server = MockServer::start().await;
        Mock::given(path(SEND_PATH))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(acknowledgement())
                    .set_delay(Duration::from_millis(200)),
            )
            .expect(1)
            .mount(&server)
            .await;
        let client = TelegramClient::local(server.uri(), Duration::from_millis(25))?;
        assert_eq!(client.send("PAPER").await, Err(DeliveryError::Timeout));
        let server = MockServer::start().await;
        Mock::given(path(SEND_PATH))
            .respond_with(ResponseTemplate::new(307).insert_header(
                "Location",
                format!("{}/unexpected", server.uri()),
            ))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(path("/unexpected"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let client = TelegramClient::local(server.uri(), Duration::from_secs(1))?;
        assert_eq!(client.send("PAPER").await, Err(DeliveryError::HttpStatus(307)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        drop(listener);
        let client = TelegramClient::local(format!("http://{address}"), Duration::from_secs(1))?;
        let error = client.send("PAPER").await.err().ok_or("expected failure")?;
        // Closed-port refusal is platform-dependent: Windows may leave the
        // connection pending until our timeout, while Unix usually refuses it
        // immediately. Both are ambiguous delivery failures and neither is
        // retried; the mock expectations above enforce the no-retry behavior.
        assert!(matches!(error, DeliveryError::Transport | DeliveryError::Timeout));
        assert!(!format!("{error} {error:?}").contains("test-secret"));
        Ok(())
    }

    #[tokio::test]
    async fn oversized_malformed_or_wrong_chat_acknowledgements_fail_closed() -> TestResult {
        for (body, expected) in [
            ("x".repeat(MAX_RESPONSE_BYTES + 1), DeliveryError::ResponseTooLarge),
            ("test-secret".to_owned(), DeliveryError::InvalidResponse),
            (
                json!({"ok":true,"result":{"chat":{"id":43}}}).to_string(),
                DeliveryError::ChatMismatch,
            ),
            (
                json!({"ok":false,"error_code":400,"description":"test-secret"}).to_string(),
                DeliveryError::Rejected(Some(400)),
            ),
        ] {
            let server = MockServer::start().await;
            Mock::given(path(SEND_PATH))
                .respond_with(ResponseTemplate::new(200).set_body_string(body))
                .expect(1)
                .mount(&server)
                .await;
            let client = TelegramClient::local(server.uri(), Duration::from_secs(1))?;
            assert_eq!(client.send("PAPER").await, Err(expected));
            assert!(!format!("{expected} {expected:?}").contains("test-secret"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn full_queue_and_shutdown_deadline_are_explicit_and_bounded() -> TestResult {
        let server = MockServer::start().await;
        Mock::given(path(SEND_PATH))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(acknowledgement())
                    .set_delay(Duration::from_secs(1)),
            )
            .mount(&server)
            .await;
        let mut notifier = TelegramNotifier::start(TelegramClient::local(
            server.uri(),
            Duration::from_secs(2),
        )?)?;
        // This test uses the current-thread runtime: no await yields to the
        // worker until all 64 slots have been filled deterministically.
        for _ in 0..QUEUE_CAPACITY {
            notifier.try_send("PAPER".to_owned())?;
        }
        assert_eq!(notifier.try_send("PAPER".to_owned()), Err(TelegramError::QueueFull));
        assert_eq!(notifier.try_send(String::new()), Err(TelegramError::EmptyMessage));
        assert_eq!(
            notifier.try_send("x".repeat(MAX_TEXT_CHARS + 1)),
            Err(TelegramError::MessageTooLong)
        );
        let stats = notifier.shutdown_with_timeout(Duration::from_millis(25)).await;
        assert_eq!(stats.accepted, 64);
        assert_eq!(stats.unconfirmed, 64);
        assert_eq!(stats.rejected, 3);
        assert_eq!(notifier.try_send("PAPER".to_owned()), Err(TelegramError::WorkerClosed));
        Ok(())
    }
}
