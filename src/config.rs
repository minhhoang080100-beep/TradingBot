//! Runtime configuration for resilient market-data connections.

use std::{num::NonZeroUsize, time::Duration};

use rand::RngExt;

/// Exponential reconnect policy with bounded random jitter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconnectPolicy {
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: u32,
    max_jitter: Duration,
    stable_after: Duration,
}

impl ReconnectPolicy {
    /// Creates a reconnect policy.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if a delay is zero, the multiplier is below one,
    /// or the maximum delay is below the initial delay.
    pub fn new(
        initial_delay: Duration,
        max_delay: Duration,
        multiplier: u32,
        max_jitter: Duration,
        stable_after: Duration,
    ) -> Result<Self, ConfigError> {
        if initial_delay.is_zero() {
            return Err(ConfigError::ZeroDuration("reconnect.initial_delay"));
        }
        if max_delay < initial_delay {
            return Err(ConfigError::InvalidReconnectRange);
        }
        if multiplier == 0 {
            return Err(ConfigError::ZeroMultiplier);
        }
        if stable_after.is_zero() {
            return Err(ConfigError::ZeroDuration("reconnect.stable_after"));
        }

        Ok(Self {
            initial_delay,
            max_delay,
            multiplier,
            max_jitter,
            stable_after,
        })
    }

    /// Returns the reset threshold for a stable WebSocket session.
    #[must_use]
    pub const fn stable_after(&self) -> Duration {
        self.stable_after
    }

    /// Returns a capped exponential delay with additive jitter.
    #[must_use]
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let exponent = attempt.min(31);
        let growth = self.multiplier.saturating_pow(exponent);
        let base = self
            .initial_delay
            .saturating_mul(growth)
            .min(self.max_delay);
        let remaining = self.max_delay.saturating_sub(base);
        let jitter_cap = self.max_jitter.min(remaining);
        let max_millis = u64::try_from(jitter_cap.as_millis()).unwrap_or(u64::MAX);

        if max_millis == 0 {
            return base;
        }

        let jitter_millis = rand::rng().random_range(0..=max_millis);
        base.saturating_add(Duration::from_millis(jitter_millis))
    }
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(30),
            multiplier: 2,
            max_jitter: Duration::from_millis(250),
            stable_after: Duration::from_secs(60),
        }
    }
}

/// Shared behavior of all market-data sessions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataEngineConfig {
    candle_capacity: NonZeroUsize,
    connect_timeout: Duration,
    silence_timeout: Duration,
    reconnect: ReconnectPolicy,
}

impl DataEngineConfig {
    /// Creates and validates data-engine configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a timeout is zero.
    pub fn new(
        candle_capacity: NonZeroUsize,
        connect_timeout: Duration,
        silence_timeout: Duration,
        reconnect: ReconnectPolicy,
    ) -> Result<Self, ConfigError> {
        if connect_timeout.is_zero() {
            return Err(ConfigError::ZeroDuration("connect_timeout"));
        }
        if silence_timeout.is_zero() {
            return Err(ConfigError::ZeroDuration("silence_timeout"));
        }

        Ok(Self {
            candle_capacity,
            connect_timeout,
            silence_timeout,
            reconnect,
        })
    }

    /// Maximum candles retained for each instrument/timeframe key.
    #[must_use]
    pub const fn candle_capacity(&self) -> NonZeroUsize {
        self.candle_capacity
    }

    /// Maximum time allowed for a WebSocket handshake.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Maximum time without any inbound data or heartbeat response.
    #[must_use]
    pub const fn silence_timeout(&self) -> Duration {
        self.silence_timeout
    }

    /// Reconnect policy shared by all exchange sessions.
    #[must_use]
    pub const fn reconnect(&self) -> &ReconnectPolicy {
        &self.reconnect
    }
}

impl Default for DataEngineConfig {
    fn default() -> Self {
        Self {
            candle_capacity: NonZeroUsize::new(1_500).unwrap_or(NonZeroUsize::MIN),
            connect_timeout: Duration::from_secs(10),
            silence_timeout: Duration::from_secs(45),
            reconnect: ReconnectPolicy::default(),
        }
    }
}

/// Invalid runtime configuration.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// A required duration was zero.
    #[error("{0} must be greater than zero")]
    ZeroDuration(&'static str),
    /// Maximum reconnect delay was below the initial delay.
    #[error("reconnect.max_delay must be greater than or equal to initial_delay")]
    InvalidReconnectRange,
    /// Exponential multiplier was zero.
    #[error("reconnect.multiplier must be at least one")]
    ZeroMultiplier,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backoff_is_exponential_and_capped() -> Result<(), ConfigError> {
        let policy = ReconnectPolicy::new(
            Duration::from_millis(100),
            Duration::from_millis(500),
            2,
            Duration::ZERO,
            Duration::from_secs(5),
        )?;

        assert_eq!(policy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(400));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(500));
        assert_eq!(policy.delay_for_attempt(30), Duration::from_millis(500));
        Ok(())
    }
}
