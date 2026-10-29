//! Causal higher-timeframe context for offline paper experiments.
//!
//! A partial five-minute block never reaches the ICT detector. The resulting
//! bias is the latest confirmed M5 BOS/MSS direction, not a forecast or an
//! instruction to trade. This module contains no account or execution state.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    ict_engine::{IctEngine, IctEngineConfig, IctError},
    models::{Candle, CandleInput, Direction, InstrumentId, ModelError, SignalEvent, Timeframe, VolumeUnit},
};

/// Failure to preserve a complete, chronological M1-to-M5 aggregation.
#[derive(Debug, Error)]
pub enum ResearchContextError {
    /// The input was not a final, minute-aligned M1 candle.
    #[error("M5 research context requires final UTC-aligned M1 candles")]
    InvalidCandle,
    /// A previous candle interval was skipped, duplicated, or reordered.
    #[error("M5 research context requires contiguous nonduplicate candles")]
    NonContiguous,
    /// One context instance may track only one instrument and volume unit.
    #[error("M5 research context changed its instrument or volume unit")]
    SeriesMismatch,
    /// A prior failure invalidated this context until it is rebuilt.
    #[error("M5 research context is poisoned by an earlier failure")]
    Poisoned,
    /// Lossless aggregation exceeded a numeric bound.
    #[error("M5 research candle aggregation overflow")]
    Overflow,
    /// Constructed aggregate failed the shared candle validation.
    #[error(transparent)]
    Model(#[from] ModelError),
    /// The real ICT detector rejected the completed M5 candle.
    #[error(transparent)]
    Ict(#[from] IctError),
    /// Detector chronology contradicted the validated aggregate sequence.
    #[error("M5 detector chronology disagreed with aggregated candles")]
    DetectorChronology,
}

#[derive(Debug)]
struct Aggregate {
    first: Candle,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    trade_count: Option<u64>,
    close_time: DateTime<Utc>,
    count: u8,
}

impl Aggregate {
    fn new(candle: &Candle) -> Self {
        Self { first: candle.clone(), high: candle.high(), low: candle.low(),
            close: candle.close(), volume: candle.volume(), trade_count: candle.trade_count(),
            close_time: candle.close_time(), count: 1 }
    }

    fn append(&mut self, candle: &Candle) -> Result<(), ResearchContextError> {
        self.high = self.high.max(candle.high());
        self.low = self.low.min(candle.low());
        self.close = candle.close();
        self.volume = self.volume.checked_add(candle.volume()).ok_or(ResearchContextError::Overflow)?;
        self.trade_count = match (self.trade_count, candle.trade_count()) {
            (Some(previous), Some(current)) => Some(previous.checked_add(current).ok_or(ResearchContextError::Overflow)?),
            _ => None,
        };
        self.close_time = candle.close_time();
        self.count = self.count.checked_add(1).ok_or(ResearchContextError::Overflow)?;
        Ok(())
    }

    fn finish(self) -> Result<Candle, ResearchContextError> {
        if self.count != 5 || self.close_time.signed_duration_since(self.first.open_time()) != Timeframe::M5.duration() {
            return Err(ResearchContextError::NonContiguous);
        }
        Ok(Candle::new(CandleInput {
            instrument: self.first.instrument().clone(), timeframe: Timeframe::M5,
            open_time: self.first.open_time(), close_time: self.close_time,
            open: self.first.open(), high: self.high, low: self.low, close: self.close,
            volume: self.volume, volume_unit: self.first.volume_unit(), trade_count: self.trade_count,
            is_closed: true,
        })?)
    }
}

/// Latest genuine M5 structure direction, built only from complete M1 blocks.
///
/// Initial candles inside an incomplete UTC five-minute block are discarded.
/// Once started, gaps and changed inputs poison this instance and clear its
/// bias, rather than silently carrying a stale trend across missing data.
#[derive(Debug)]
pub struct ConfirmedM5Bias {
    detector: IctEngine,
    instrument: Option<InstrumentId>,
    volume_unit: Option<VolumeUnit>,
    last_close: Option<DateTime<Utc>>,
    aggregate: Option<Aggregate>,
    last_completed: Option<Candle>,
    bias: Option<Direction>,
    poisoned: bool,
}

impl Default for ConfirmedM5Bias {
    fn default() -> Self {
        Self { detector: IctEngine::new(IctEngineConfig::default()), instrument: None,
            volume_unit: None, last_close: None, aggregate: None, last_completed: None,
            bias: None, poisoned: false }
    }
}

impl ConfirmedM5Bias {
    /// Returns the direction of the last confirmed M5 BOS or MSS.
    #[must_use]
    pub const fn bias(&self) -> Option<Direction> { self.bias }

    /// Close of the most recent complete M5 observation, for causal audits.
    #[must_use]
    pub fn last_completed_at(&self) -> Option<DateTime<Utc>> {
        self.last_completed.as_ref().map(Candle::close_time)
    }

    /// Consumes one final M1 candle and returns the latest confirmed M5 bias.
    ///
    /// Between fifth-bar closes, the returned direction stays frozen. A new
    /// direction is observable only when the real M5 detector confirms it.
    ///
    /// # Errors
    /// Invalid timing, gaps, duplicates, changed series, numeric overflow or a
    /// detector failure clear the bias and poison all later processing.
    pub fn process(&mut self, candle: &Candle) -> Result<Option<Direction>, ResearchContextError> {
        if self.poisoned { return Err(ResearchContextError::Poisoned); }
        let result = self.process_inner(candle);
        if result.is_err() {
            self.aggregate = None;
            self.last_completed = None;
            self.bias = None;
            self.poisoned = true;
        }
        result
    }

    fn process_inner(&mut self, candle: &Candle) -> Result<Option<Direction>, ResearchContextError> {
        if candle.timeframe() != Timeframe::M1 || !candle.is_closed()
            || candle.open_time().timestamp_subsec_nanos() != 0
            || candle.open_time().timestamp().rem_euclid(60) != 0
            || candle.close_time().signed_duration_since(candle.open_time()) != Timeframe::M1.duration() {
            return Err(ResearchContextError::InvalidCandle);
        }
        if self.last_close.is_some_and(|at| at != candle.open_time()) {
            return Err(ResearchContextError::NonContiguous);
        }
        if self.instrument.as_ref().is_some_and(|instrument| instrument != candle.instrument())
            || self.volume_unit.is_some_and(|unit| unit != candle.volume_unit()) {
            return Err(ResearchContextError::SeriesMismatch);
        }
        self.instrument = Some(candle.instrument().clone());
        self.volume_unit = Some(candle.volume_unit());
        self.last_close = Some(candle.close_time());
        if let Some(aggregate) = &mut self.aggregate {
            aggregate.append(candle)?;
        } else if candle.open_time().timestamp().rem_euclid(300) == 0 {
            self.aggregate = Some(Aggregate::new(candle));
        } else {
            return Ok(self.bias);
        }
        if self.aggregate.as_ref().is_none_or(|aggregate| aggregate.count != 5) {
            return Ok(self.bias);
        }
        let complete = self.aggregate.take().ok_or(ResearchContextError::NonContiguous)?.finish()?;
        let batch = self.detector.process_candle(&complete)?;
        if batch.series_reset() || batch.is_duplicate() { return Err(ResearchContextError::DetectorChronology); }
        if batch.structure_ambiguous() {
            self.bias = None;
        } else {
            for signal in batch.signals() {
                match signal {
                    SignalEvent::MarketStructureShiftDetected(mss) => self.bias = Some(mss.direction()),
                    SignalEvent::BreakOfStructureDetected(bos) => self.bias = Some(bos.direction()),
                    _ => {}
                }
            }
        }
        self.last_completed = Some(complete);
        Ok(self.bias)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Exchange, MarketKind};
    use std::{error::Error, io};

    type TestResult = Result<(), Box<dyn Error>>;

    fn at(minute: i64) -> Result<DateTime<Utc>, io::Error> {
        DateTime::from_timestamp(minute * 60, 0).ok_or_else(|| io::Error::other("fixture time overflow"))
    }

    fn input(minute: i64, row: [i64; 4]) -> Result<CandleInput, Box<dyn Error>> {
        Ok(CandleInput {
            instrument: InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual, "BTCUSDT", "BTC-USDT-PERP")?,
            timeframe: Timeframe::M1, open_time: at(minute)?, close_time: at(minute + 1)?,
            open: Decimal::from(row[0]), high: Decimal::from(row[1]), low: Decimal::from(row[2]), close: Decimal::from(row[3]),
            volume: Decimal::ONE, volume_unit: VolumeUnit::BaseAsset, trade_count: Some(1), is_closed: true,
        })
    }

    fn bar(minute: i64, row: [i64; 4]) -> Result<Candle, Box<dyn Error>> { Ok(Candle::new(input(minute, row)?)?) }

    #[test]
    fn aggregates_five_final_minutes_exactly_and_discards_partial_initial_block() -> TestResult {
        let mut context = ConfirmedM5Bias::default();
        for minute in 2..5 {
            context.process(&bar(minute, [100, 200, 50, 101])?)?;
            assert!(context.last_completed_at().is_none());
        }
        let rows = [[100, 105, 99, 102], [102, 108, 100, 106], [106, 107, 97, 100],
            [100, 109, 98, 108], [108, 111, 106, 110]];
        for (offset, row) in rows.into_iter().enumerate() {
            context.process(&bar(5 + i64::try_from(offset)?, row)?)?;
            if offset < 4 { assert!(context.last_completed_at().is_none()); }
        }
        let complete = context.last_completed.as_ref().ok_or("missing completed aggregate")?;
        assert_eq!(complete.timeframe(), Timeframe::M5);
        assert_eq!((complete.open_time(), complete.close_time()), (at(5)?, at(10)?));
        assert_eq!((complete.open(), complete.high(), complete.low(), complete.close()),
            (Decimal::from(100), Decimal::from(111), Decimal::from(97), Decimal::from(110)));
        assert_eq!(complete.volume(), Decimal::from(5));
        assert_eq!(complete.trade_count(), Some(5));
        Ok(())
    }

    #[test]
    fn partial_future_block_cannot_change_real_confirmed_structure_bias() -> TestResult {
        let mut context = ConfirmedM5Bias::default();
        let mut rows = vec![[100, 102, 98, 101]; 9];
        rows[2] = [100, 110, 98, 101];
        rows[5] = [100, 102, 95, 101];
        rows[8] = [101, 114, 100, 112];
        for (block, row) in rows.into_iter().enumerate() {
            for offset in 0..5 {
                context.process(&bar(i64::try_from(block)? * 5 + offset, row)?)?;
            }
        }
        assert_eq!(context.bias(), Some(Direction::Bullish));
        assert_eq!(context.last_completed_at(), Some(at(45)?));
        for minute in 45..49 {
            assert_eq!(context.process(&bar(minute, [100, 103, 90, 91])?)?, Some(Direction::Bullish));
            assert_eq!(context.last_completed_at(), Some(at(45)?));
        }
        assert_eq!(context.process(&bar(49, [100, 103, 90, 91])?)?, Some(Direction::Bearish));
        assert_eq!(context.last_completed_at(), Some(at(50)?));
        Ok(())
    }

    #[test]
    fn gaps_duplicates_wrong_timeframes_and_open_candles_fail_closed() -> TestResult {
        for next in [-1, 0, 2] {
            let mut context = ConfirmedM5Bias::default();
            context.process(&bar(0, [100, 102, 98, 101])?)?;
            assert!(matches!(context.process(&bar(next, [100, 102, 98, 101])?), Err(ResearchContextError::NonContiguous)));
            assert!(context.bias().is_none() && context.last_completed_at().is_none());
            assert!(matches!(context.process(&bar(1, [100, 102, 98, 101])?), Err(ResearchContextError::Poisoned)));
        }
        let mut open = input(0, [100, 102, 98, 101])?;
        open.is_closed = false;
        let mut wrong_timeframe = input(0, [100, 102, 98, 101])?;
        wrong_timeframe.timeframe = Timeframe::M5;
        wrong_timeframe.close_time = at(5)?;
        for value in [open, wrong_timeframe] {
            assert!(matches!(ConfirmedM5Bias::default().process(&Candle::new(value)?), Err(ResearchContextError::InvalidCandle)));
        }
        Ok(())
    }

    #[test]
    fn changing_symbols_or_volume_units_never_mixes_aggregates() -> TestResult {
        for change_symbol in [false, true] {
            let mut context = ConfirmedM5Bias::default();
            context.process(&bar(0, [100, 102, 98, 101])?)?;
            let mut changed = input(1, [100, 102, 98, 101])?;
            if change_symbol {
                changed.instrument = InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual, "ETHUSDT", "ETH-USDT-PERP")?;
            } else { changed.volume_unit = VolumeUnit::Contracts; }
            assert!(matches!(context.process(&Candle::new(changed)?), Err(ResearchContextError::SeriesMismatch)));
            assert!(context.bias().is_none());
        }
        Ok(())
    }
}
