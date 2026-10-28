//! Bounded recovery for idempotent PUBLIC GETs, never order/account mutations.
//!
//! The caller owns shutdown, session deadlines and its last-candle freshness
//! deadline. Dropping this future cancels the current request or backoff; there
//! is no spawned worker or detached retry. Construct a fresh request timestamp
//! inside the request closure on EVERY attempt, not before calling this helper.

use std::{future::Future, time::Duration};

use ict_trading_bot::paper::PaperMarketError;
use rand::RngExt;
use tokio::time::Instant;

const MAX_ATTEMPTS: u8 = 4;

#[derive(Clone, Copy)]
struct RetryPolicy {
    budget: Duration,
    backoffs: [Duration; 3],
    max_jitter_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            budget: Duration::from_secs(60),
            backoffs: [Duration::from_secs(1), Duration::from_secs(2), Duration::from_secs(4)],
            max_jitter_ms: 250,
        }
    }
}

/// Sanitized observations; notices contain no URL, body or client error text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RecoveryNotice {
    Retrying {
        operation: &'static str,
        attempt: u8,
        next_attempt: u8,
        max_attempts: u8,
        delay: Duration,
        error: PaperMarketError,
    },
    Recovered {
        operation: &'static str,
        attempts: u8,
    },
}

/// Terminal, sanitized outcomes distinguish invalid data from temporary outages.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum RecoveryError {
    #[error("public GET {operation} failed permanently after {attempts} attempt(s): {error}")]
    Permanent { operation: &'static str, attempts: u8, error: PaperMarketError },
    #[error("public GET {operation} exhausted {attempts} attempts: {error}")]
    Exhausted { operation: &'static str, attempts: u8, error: PaperMarketError },
    #[error("public GET {operation} recovery deadline reached after {attempts} attempt(s)")]
    Deadline { operation: &'static str, attempts: u8 },
    #[error("public GET {operation} retry delay {delay:?} exceeds remaining recovery budget after {attempts} attempt(s)")]
    DelayExceedsBudget { operation: &'static str, attempts: u8, delay: Duration },
    #[error("invalid public GET operation label")]
    InvalidOperation,
}

/// Performs at most four attempts within one sixty-second wall-clock budget.
///
/// The request closure must construct fresh request-time validation context on
/// every call and must return a nonblocking, cancellable future. It may represent
/// an entire synchronized public-data round; partial results are never retained
/// here. Delays are 1/2/4 seconds plus 1..=250 ms jitter, raised to at least any
/// server Retry-After hint. An unfit hint stops recovery instead of sending early.
/// Notice callbacks must also remain nonblocking observers, not account actions.
pub(super) async fn public_get<T, F, Fut, N>(operation: &'static str, request: F, on_notice: N)
    -> Result<T, RecoveryError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, PaperMarketError>>,
    N: FnMut(RecoveryNotice),
{
    public_get_with_policy(operation, request, on_notice, RetryPolicy::default()).await
}

async fn public_get_with_policy<T, F, Fut, N>(operation: &'static str, mut request: F,
    mut on_notice: N, policy: RetryPolicy) -> Result<T, RecoveryError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, PaperMarketError>>,
    N: FnMut(RecoveryNotice),
{
    if !valid_label(operation) { return Err(RecoveryError::InvalidOperation); }
    let mut attempts = 0;
    let deadline = Instant::now().checked_add(policy.budget)
        .ok_or(RecoveryError::Deadline { operation, attempts })?;
    loop {
        if Instant::now() >= deadline { return Err(RecoveryError::Deadline { operation, attempts }); }
        attempts += 1;
        let outcome = tokio::time::timeout_at(deadline, request()).await
            .map_err(|_| RecoveryError::Deadline { operation, attempts })?;
        // A synchronous/ready request must not escape the wall-clock budget.
        if Instant::now() >= deadline { return Err(RecoveryError::Deadline { operation, attempts }); }
        let error = match outcome {
            Ok(value) => {
                if attempts > 1 { on_notice(RecoveryNotice::Recovered { operation, attempts }); }
                return Ok(value);
            }
            Err(error) if !error.is_retryable() => return Err(RecoveryError::Permanent { operation, attempts, error }),
            Err(error) => error,
        };
        if attempts == MAX_ATTEMPTS { return Err(RecoveryError::Exhausted { operation, attempts, error }); }
        let backoff = policy.backoffs.get(usize::from(attempts - 1)).copied()
            .ok_or(RecoveryError::Deadline { operation, attempts })?;
        let minimum = backoff.max(error.suggested_retry_delay().unwrap_or(Duration::ZERO));
        let jitter = Duration::from_millis(rand::rng().random_range(1..=policy.max_jitter_ms.max(1)));
        let delay = minimum.checked_add(jitter)
            .ok_or(RecoveryError::DelayExceedsBudget { operation, attempts, delay: minimum })?;
        if delay >= deadline.saturating_duration_since(Instant::now()) {
            return Err(RecoveryError::DelayExceedsBudget { operation, attempts, delay });
        }
        on_notice(RecoveryNotice::Retrying {
            operation, attempt: attempts, next_attempt: attempts + 1,
            max_attempts: MAX_ATTEMPTS, delay, error,
        });
        // Notice handlers are synchronous observers. Account for their elapsed
        // time too, and never truncate a server-mandated wait to fit the budget.
        if delay >= deadline.saturating_duration_since(Instant::now()) {
            return Err(RecoveryError::DelayExceedsBudget { operation, attempts, delay });
        }
        tokio::time::timeout_at(deadline, tokio::time::sleep(delay)).await
            .map_err(|_| RecoveryError::Deadline { operation, attempts })?;
    }
}

/// Logs only validated labels, counters, elapsed delay and typed market errors.
pub(super) fn log_notice(notice: &RecoveryNotice) {
    match notice {
        RecoveryNotice::Retrying { operation, attempt, next_attempt, max_attempts, delay, error } => {
            eprintln!("Public GET recovery: operation={} attempt={attempt} next_attempt={next_attempt}/{max_attempts} delay_ms={} error={error}",
                safe_label(operation), delay.as_millis());
        }
        RecoveryNotice::Recovered { operation, attempts } => {
            eprintln!("Public GET recovered: operation={} attempts={attempts}", safe_label(operation));
        }
    }
}

fn valid_label(label: &str) -> bool {
    !label.is_empty() && label.len() <= 80
        && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b' ' | b'_' | b'-'))
}

fn safe_label(label: &str) -> &str {
    if valid_label(label) { label } else { "redacted" }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, error::Error, future::ready, sync::{Arc, atomic::{AtomicBool, Ordering}}};

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    fn fast_policy() -> RetryPolicy {
        RetryPolicy {
            budget: Duration::from_secs(2),
            backoffs: [Duration::from_millis(2), Duration::from_millis(4), Duration::from_millis(8)],
            max_jitter_ms: 1,
        }
    }

    #[test]
    fn production_policy_is_bounded_and_uses_positive_integer_jitter() {
        let policy = RetryPolicy::default();
        assert_eq!(MAX_ATTEMPTS, 4);
        assert_eq!(policy.budget, Duration::from_secs(60));
        assert_eq!(policy.backoffs, [Duration::from_secs(1), Duration::from_secs(2), Duration::from_secs(4)]);
        assert_eq!(policy.max_jitter_ms, 250);
    }

    #[tokio::test]
    async fn successful_first_request_emits_no_outage_or_recovery_notice() -> TestResult {
        let calls = Cell::new(0);
        let mut notices = Vec::new();
        let value = public_get_with_policy("metadata", || {
            calls.set(calls.get() + 1);
            ready(Ok(17))
        }, |notice| notices.push(notice), fast_policy()).await?;
        assert_eq!(value, 17);
        assert_eq!(calls.get(), 1);
        assert!(notices.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn temporary_failures_recreate_requests_and_emit_one_recovered_notice() -> TestResult {
        let calls = Cell::new(0);
        let mut notices = Vec::new();
        let value = public_get_with_policy("synchronized candles", || {
            let attempt = calls.get() + 1;
            calls.set(attempt);
            ready(if attempt < 3 { Err(PaperMarketError::Transport) } else { Ok((attempt, "fresh round")) })
        }, |notice| notices.push(notice), fast_policy()).await?;
        assert_eq!(value, (3, "fresh round"));
        assert_eq!(calls.get(), 3);
        assert!(matches!(notices.as_slice(), [
            RecoveryNotice::Retrying { attempt: 1, next_attempt: 2, max_attempts: 4, delay: first, .. },
            RecoveryNotice::Retrying { attempt: 2, next_attempt: 3, max_attempts: 4, delay: second, .. },
            RecoveryNotice::Recovered { attempts: 3, .. }
        ] if *first == Duration::from_millis(3) && *second == Duration::from_millis(5)));
        Ok(())
    }

    #[tokio::test]
    async fn invariant_schema_auth_and_retry_header_errors_do_not_retry() {
        for error in [PaperMarketError::InvalidCandles, PaperMarketError::ClockSkew,
            PaperMarketError::HttpStatus(401), PaperMarketError::InvalidRetryAfter(429),
            PaperMarketError::UnsupportedInstrument, PaperMarketError::ResponseTooLarge] {
            let calls = Cell::new(0);
            let mut notices = Vec::new();
            let result = public_get_with_policy("candles", || {
                calls.set(calls.get() + 1);
                ready(Err::<(), _>(error.clone()))
            }, |notice| notices.push(notice), fast_policy()).await;
            assert_eq!(result, Err(RecoveryError::Permanent { operation: "candles", attempts: 1, error }));
            assert_eq!(calls.get(), 1);
            assert!(notices.is_empty());
        }
    }

    #[tokio::test]
    async fn exhausted_outage_has_four_attempts_three_real_waits_and_no_busy_loop() {
        let mut starts = Vec::new();
        let mut notices = Vec::new();
        let policy = fast_policy();
        let result = public_get_with_policy("candles", || {
            starts.push(Instant::now());
            ready(Err::<(), _>(PaperMarketError::HttpStatus(503)))
        }, |notice| notices.push(notice), policy).await;
        assert_eq!(result, Err(RecoveryError::Exhausted {
            operation: "candles", attempts: 4, error: PaperMarketError::HttpStatus(503),
        }));
        assert_eq!(starts.len(), 4);
        assert_eq!(notices.len(), 3);
        for (index, pair) in starts.windows(2).enumerate() {
            assert!(pair[1].duration_since(pair[0]) >= policy.backoffs[index] + Duration::from_millis(1));
        }
        assert!(notices.iter().all(|notice| matches!(notice, RecoveryNotice::Retrying { .. })));
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); }
    }

    #[tokio::test]
    async fn wall_clock_deadline_cancels_an_unfinished_request_and_is_not_exhaustion() {
        let dropped = Arc::new(AtomicBool::new(false));
        let calls = Cell::new(0);
        let mut policy = fast_policy();
        policy.budget = Duration::from_millis(20);
        let result = public_get_with_policy("candles", || {
            calls.set(calls.get() + 1);
            let dropped = Arc::clone(&dropped);
            async move {
                let _guard = DropFlag(dropped);
                std::future::pending::<Result<(), PaperMarketError>>().await
            }
        }, |_| {}, policy).await;
        assert_eq!(result, Err(RecoveryError::Deadline { operation: "candles", attempts: 1 }));
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn zero_budget_never_invokes_the_request_closure() {
        let calls = Cell::new(0);
        let mut policy = fast_policy();
        policy.budget = Duration::ZERO;
        let result = public_get_with_policy("candles", || {
            calls.set(calls.get() + 1);
            ready(Ok(()))
        }, |_| {}, policy).await;
        assert_eq!(result, Err(RecoveryError::Deadline { operation: "candles", attempts: 0 }));
        assert_eq!(calls.get(), 0);
    }

    #[tokio::test]
    async fn elapsed_request_time_reduces_the_same_total_delay_budget() {
        let calls = Cell::new(0);
        let mut policy = fast_policy();
        policy.budget = Duration::from_millis(200);
        policy.backoffs = [Duration::from_millis(180); 3];
        let result = public_get_with_policy("candles", || {
            calls.set(calls.get() + 1);
            async {
                tokio::time::sleep(Duration::from_millis(40)).await;
                Err::<(), _>(PaperMarketError::Transport)
            }
        }, |_| {}, policy).await;
        assert!(matches!(result, Err(RecoveryError::DelayExceedsBudget { attempts: 1, .. }
            | RecoveryError::Deadline { attempts: 1, .. })));
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn server_retry_after_is_honored_with_positive_jitter() -> TestResult {
        let mut starts = Vec::new();
        let mut notices = Vec::new();
        public_get_with_policy("metadata", || {
            starts.push(Instant::now());
            ready(if starts.len() == 1 {
                Err(PaperMarketError::HttpRetryAfter { status: 429, delay: Duration::from_millis(40) })
            } else { Ok(()) })
        }, |notice| notices.push(notice), fast_policy()).await?;
        assert_eq!(starts.len(), 2);
        assert!(starts[1].duration_since(starts[0]) >= Duration::from_millis(41));
        assert!(matches!(notices.as_slice(), [RecoveryNotice::Retrying { delay, .. },
            RecoveryNotice::Recovered { attempts: 2, .. }] if *delay == Duration::from_millis(41)));
        Ok(())
    }

    #[tokio::test]
    async fn oversized_server_wait_never_sends_an_early_retry_or_overflows() {
        for delay in [Duration::from_secs(60), Duration::MAX] {
            let calls = Cell::new(0);
            let mut notices = Vec::new();
            let result = public_get_with_policy("metadata", || {
                calls.set(calls.get() + 1);
                ready(Err::<(), _>(PaperMarketError::HttpRetryAfter { status: 503, delay }))
            }, |notice| notices.push(notice), fast_policy()).await;
            assert!(matches!(result, Err(RecoveryError::DelayExceedsBudget { attempts: 1, .. })));
            assert_eq!(calls.get(), 1);
            assert!(notices.is_empty());
        }
    }

    #[tokio::test]
    async fn cancellation_during_backoff_drops_retry_work_without_another_attempt() {
        let calls = Cell::new(0);
        let cancellation = tokio::sync::Notify::new();
        let mut policy = fast_policy();
        policy.budget = Duration::from_secs(10);
        policy.backoffs = [Duration::from_secs(2); 3];
        let cancelled = {
            let request = public_get_with_policy("candles", || {
                calls.set(calls.get() + 1);
                ready(Err::<(), _>(PaperMarketError::Transport))
            }, |_| cancellation.notify_one(), policy);
            tokio::pin!(request);
            tokio::select! {
                () = cancellation.notified() => true,
                _ = &mut request => false,
            }
        };
        assert!(cancelled);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn unsafe_operation_labels_are_not_logged_or_requested() {
        let calls = Cell::new(0);
        let result = public_get_with_policy("https://private.example?secret=value", || {
            calls.set(calls.get() + 1);
            ready(Ok(()))
        }, |_| {}, fast_policy()).await;
        assert_eq!(result, Err(RecoveryError::InvalidOperation));
        assert_eq!(calls.get(), 0);
        assert_eq!(safe_label("candles\nforged log"), "redacted");
        assert_eq!(safe_label("candles BTC_ETH"), "candles BTC_ETH");
        assert!(!RecoveryError::InvalidOperation.to_string().contains("secret"));
    }

}
