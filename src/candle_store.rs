//! Bounded, deduplicating in-memory candle storage.

use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use crate::models::{Candle, InstrumentId, Timeframe};

/// Key for one independently ordered candle series.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct CandleKey {
    /// Exchange and instrument identity.
    pub instrument: InstrumentId,
    /// Candle interval.
    pub timeframe: Timeframe,
}

impl CandleKey {
    /// Derives a store key from a candle.
    #[must_use]
    pub fn from_candle(candle: &Candle) -> Self {
        Self {
            instrument: candle.instrument().clone(),
            timeframe: candle.timeframe(),
        }
    }
}

/// Missing interval detected between two newly inserted candles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandleGap {
    /// First candle open time expected after the previous stored candle.
    pub expected_open_time: DateTime<Utc>,
    /// Open time actually received from the exchange.
    pub received_open_time: DateTime<Utc>,
}

/// Effect of one successful candle upsert.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoreDisposition {
    /// New live candle appended to the series.
    InsertedOpen,
    /// Existing live candle replaced by a fresher update.
    UpdatedOpen,
    /// First final update for this candle; safe to emit downstream.
    Closed(Candle),
    /// Byte-equivalent or semantically identical repeat.
    Duplicate,
}

/// Upsert result with optional gap metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreOutcome {
    /// How the store handled the update.
    pub disposition: StoreDisposition,
    /// Gap preceding a newly inserted candle, if one exists.
    pub gap: Option<CandleGap>,
}

/// Per-series bounded ring buffers.
#[derive(Debug)]
pub struct CandleStore {
    capacity: NonZeroUsize,
    series: HashMap<CandleKey, VecDeque<Candle>>,
}

/// Concurrent store shared by exchange session tasks and readers.
pub type SharedCandleStore = Arc<RwLock<CandleStore>>;

impl CandleStore {
    /// Creates an empty store with a fixed per-series capacity.
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            series: HashMap::new(),
        }
    }

    /// Wraps a new store in an asynchronous reader/writer lock.
    #[must_use]
    pub fn shared(capacity: NonZeroUsize) -> SharedCandleStore {
        Arc::new(RwLock::new(Self::new(capacity)))
    }

    /// Inserts or updates a candle while preserving chronological order.
    ///
    /// A final candle is returned exactly once through
    /// [`StoreDisposition::Closed`]. Repeated final snapshots are deduplicated.
    ///
    /// # Errors
    ///
    /// Returns [`CandleStoreError`] for a previously evicted/stale candle or if
    /// an exchange mutates a candle after it was declared final.
    pub fn upsert(&mut self, candle: Candle) -> Result<StoreOutcome, CandleStoreError> {
        let key = CandleKey::from_candle(&candle);
        let candles = self.series.entry(key.clone()).or_default();

        if let Some(position) = candles
            .iter()
            .position(|stored| stored.open_time() == candle.open_time())
        {
            return Self::update_existing(candles, position, candle, key);
        }

        let gap = if let Some(latest) = candles.back() {
            if candle.open_time() < latest.open_time() {
                return Err(CandleStoreError::OutOfOrder {
                    key,
                    received_open_time: candle.open_time(),
                    latest_open_time: latest.open_time(),
                });
            }

            let expected = latest
                .open_time()
                .checked_add_signed(candle.timeframe().duration())
                .ok_or_else(|| CandleStoreError::TimeOverflow { key: key.clone() })?;
            (candle.open_time() > expected).then_some(CandleGap {
                expected_open_time: expected,
                received_open_time: candle.open_time(),
            })
        } else {
            None
        };

        let disposition = if candle.is_closed() {
            StoreDisposition::Closed(candle.clone())
        } else {
            StoreDisposition::InsertedOpen
        };
        candles.push_back(candle);
        while candles.len() > self.capacity.get() {
            let _evicted = candles.pop_front();
        }

        Ok(StoreOutcome { disposition, gap })
    }

    fn update_existing(
        candles: &mut VecDeque<Candle>,
        position: usize,
        candle: Candle,
        key: CandleKey,
    ) -> Result<StoreOutcome, CandleStoreError> {
        let stored = candles
            .get(position)
            .ok_or_else(|| CandleStoreError::MissingPosition { key: key.clone() })?;

        if stored == &candle {
            return Ok(StoreOutcome {
                disposition: StoreDisposition::Duplicate,
                gap: None,
            });
        }
        if stored.is_closed() {
            return Err(CandleStoreError::ConflictingClosedCandle {
                key,
                open_time: candle.open_time(),
            });
        }

        let disposition = if candle.is_closed() {
            StoreDisposition::Closed(candle.clone())
        } else {
            StoreDisposition::UpdatedOpen
        };
        let slot = candles
            .get_mut(position)
            .ok_or(CandleStoreError::MissingPosition { key })?;
        *slot = candle;

        Ok(StoreOutcome {
            disposition,
            gap: None,
        })
    }

    /// Returns a chronological clone of one series.
    #[must_use]
    pub fn snapshot(&self, key: &CandleKey) -> Vec<Candle> {
        self.series
            .get(key)
            .map(|candles| candles.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns the newest candle for a series.
    #[must_use]
    pub fn latest(&self, key: &CandleKey) -> Option<Candle> {
        self.series.get(key).and_then(VecDeque::back).cloned()
    }

    /// Number of independent instrument/timeframe series.
    #[must_use]
    pub fn series_count(&self) -> usize {
        self.series.len()
    }
}

/// Rejected store update.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CandleStoreError {
    /// Candle was older than the latest value and no matching slot remained.
    #[error(
        "out-of-order candle for {key:?}: received {received_open_time}, latest {latest_open_time}"
    )]
    OutOfOrder {
        /// Affected series.
        key: CandleKey,
        /// Stale candle timestamp.
        received_open_time: DateTime<Utc>,
        /// Newest stored timestamp.
        latest_open_time: DateTime<Utc>,
    },
    /// Venue changed OHLCV after declaring the candle final.
    #[error("conflicting update for final candle {key:?} at {open_time}")]
    ConflictingClosedCandle {
        /// Affected series.
        key: CandleKey,
        /// Final candle timestamp.
        open_time: DateTime<Utc>,
    },
    /// Timeframe addition overflowed Chrono's range.
    #[error("candle time overflow for {key:?}")]
    TimeOverflow {
        /// Affected series.
        key: CandleKey,
    },
    /// Internal ring-buffer position disappeared unexpectedly.
    #[error("candle position disappeared for {key:?}")]
    MissingPosition {
        /// Affected series.
        key: CandleKey,
    },
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use chrono::DateTime;
    use rust_decimal::Decimal;

    use crate::models::{CandleInput, Exchange, MarketKind, VolumeUnit};

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    fn at_minute(minute: i64) -> Result<DateTime<Utc>, io::Error> {
        DateTime::from_timestamp(minute * 60, 0)
            .ok_or_else(|| io::Error::other("invalid test timestamp"))
    }

    fn candle(minute: i64, close: i64, is_closed: bool) -> Result<Candle, Box<dyn Error>> {
        Ok(Candle::new(CandleInput {
            instrument: InstrumentId::new(
                Exchange::Binance,
                MarketKind::LinearPerpetual,
                "BTCUSDT",
                "BTC-USDT-PERP",
            )?,
            timeframe: Timeframe::M1,
            open_time: at_minute(minute)?,
            close_time: at_minute(minute + 1)?,
            open: Decimal::new(100, 0),
            high: Decimal::new(105, 0),
            low: Decimal::new(95, 0),
            close: Decimal::new(close, 0),
            volume: Decimal::new(10, 0),
            volume_unit: VolumeUnit::BaseAsset,
            trade_count: Some(5),
            is_closed,
        })?)
    }

    #[test]
    fn live_update_then_close_emits_once() -> TestResult {
        let mut store = CandleStore::new(NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN));

        assert_eq!(
            store.upsert(candle(1, 101, false)?)?.disposition,
            StoreDisposition::InsertedOpen
        );
        assert_eq!(
            store.upsert(candle(1, 102, false)?)?.disposition,
            StoreDisposition::UpdatedOpen
        );
        assert!(matches!(
            store.upsert(candle(1, 103, true)?)?.disposition,
            StoreDisposition::Closed(_)
        ));
        assert_eq!(
            store.upsert(candle(1, 103, true)?)?.disposition,
            StoreDisposition::Duplicate
        );
        Ok(())
    }

    #[test]
    fn capacity_and_gap_detection_are_per_series() -> TestResult {
        let mut store = CandleStore::new(NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN));
        let first = candle(1, 101, true)?;
        let key = CandleKey::from_candle(&first);
        let _first_outcome = store.upsert(first)?;
        let second = store.upsert(candle(2, 102, true)?)?;
        assert!(second.gap.is_none());
        let fourth = store.upsert(candle(4, 103, true)?)?;
        assert_eq!(
            fourth.gap,
            Some(CandleGap {
                expected_open_time: at_minute(3)?,
                received_open_time: at_minute(4)?,
            })
        );

        let snapshot = store.snapshot(&key);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].open_time(), at_minute(2)?);
        assert_eq!(snapshot[1].open_time(), at_minute(4)?);
        Ok(())
    }
}
