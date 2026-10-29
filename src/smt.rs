//! Causal, paired BTC/ETH liquidity divergence for offline strategy research.
//!
//! Both final M1 observations are inspected together. A signal compares an
//! actual close-back sweep with an already confirmed, unconsumed peer pivot;
//! pivots confirmed by the current observation become eligible only later.

use std::collections::VecDeque;

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    ict_engine::IctDetectionBatch,
    models::{Candle, Direction, Exchange, InstrumentId, LiquiditySide, LiquiditySweep,
        MarketKind, SignalEvent, SwingKind, SwingPoint, Timeframe, VolumeUnit},
};

const PIVOT_CAPACITY: usize = 64;
const PIVOT_MAX_AGE_MINUTES: i64 = 60;
const PAIR_TOLERANCE_MINUTES: i64 = 3;
const EVIDENCE_TTL_MINUTES: i64 = 12;
const EVIDENCE_CAPACITY: usize = 64;

/// Immutable observations establishing one BTC/ETH divergence.
///
/// This is deliberately Serialize-only: callers cannot deserialize or freely
/// construct a purported confirmation and bypass the paired detector.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SmtEvidence {
    id: Uuid,
    focus_sweep: LiquiditySweep,
    focus_anchor: SwingPoint,
    peer_anchor: SwingPoint,
    focus_candle: Candle,
    peer_candle: Candle,
    direction: Direction,
    detected_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

impl SmtEvidence {
    /// Stable identifier binding both assets' actual confirmation observations.
    #[must_use]
    pub const fn id(&self) -> Uuid { self.id }
    /// Identity of the actual ICT liquidity sweep being confirmed.
    #[must_use]
    pub const fn sweep_id(&self) -> Uuid { self.focus_sweep.id() }
    /// Instrument which swept and closed back through its anchor.
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentId { self.focus_sweep.instrument() }
    /// Reversal direction implied by the swept liquidity side.
    #[must_use]
    pub const fn direction(&self) -> Direction { self.direction }
    /// Time at which both closed candles made this evidence observable.
    #[must_use]
    pub const fn detected_at(&self) -> DateTime<Utc> { self.detected_at }
    /// Exclusive expiry; evidence is invalid at or after this close.
    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> { self.expires_at }
    /// Original close-back sweep, never a synthetic replacement.
    #[must_use]
    pub const fn focus_sweep(&self) -> &LiquiditySweep { &self.focus_sweep }
    /// Previously confirmed pivot swept by the focus asset.
    #[must_use]
    pub const fn focus_anchor(&self) -> &SwingPoint { &self.focus_anchor }
    /// Temporally corresponding pivot which the peer did not sweep.
    #[must_use]
    pub const fn peer_anchor(&self) -> &SwingPoint { &self.peer_anchor }
    /// Focus candle proving the sweep extreme and close-back.
    #[must_use]
    pub const fn focus_candle(&self) -> &Candle { &self.focus_candle }
    /// Simultaneous peer candle proving non-confirmation of the sweep.
    #[must_use]
    pub const fn peer_candle(&self) -> &Candle { &self.peer_candle }
}

/// Complete paired result; all evidence reflects the same closed interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SmtUpdate {
    /// Evidence newly established at this close, in deterministic order.
    pub newly_confirmed: Vec<SmtEvidence>,
    /// All still-valid evidence after expiry and extreme-break invalidation.
    pub active: Vec<SmtEvidence>,
}

/// Invalid paired inputs fail closed instead of accepting one stale asset.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum SmtError {
    /// Both instruments must be canonical Bybit BTC/ETH USDT perpetuals.
    #[error("SMT requires exactly canonical Bybit BTCUSDT and ETHUSDT perpetuals")]
    Instruments,
    /// Source observations must be simultaneous, final, UTC-aligned M1 bars.
    #[error("SMT requires synchronized closed UTC-aligned M1 base-volume candles")]
    CandleAlignment,
    /// Neither replay nor a missing observation can establish new evidence.
    #[error("SMT requires contiguous paired observations without duplicates or resets")]
    Chronology,
    /// Relevant derived metadata must belong to its source candle.
    #[error("SMT batch contains inconsistent or future sweep/pivot metadata")]
    Metadata,
    /// Chrono arithmetic must remain representable.
    #[error("SMT evidence timestamp overflow")]
    TimeOverflow,
    /// Stable evidence serialization failed.
    #[error("SMT evidence encoding failed")]
    EvidenceEncoding,
    /// Never discard still-valid evidence merely to make capacity available.
    #[error("SMT active evidence capacity exhausted")]
    Capacity,
    /// A failed pair requires the enclosing session to restart safely.
    #[error("SMT detector is poisoned by a previous invalid observation")]
    Poisoned,
}

/// Bounded paired detector; errors poison subsequent processing.
#[derive(Debug, Default)]
pub struct SmtDetector {
    anchors: [VecDeque<TrackedAnchor>; 2],
    active: Vec<SmtEvidence>,
    last_close: Option<DateTime<Utc>>,
    poisoned: bool,
}

#[derive(Debug)]
struct TrackedAnchor {
    point: SwingPoint,
    consumed: bool,
}

impl From<SwingPoint> for TrackedAnchor {
    fn from(point: SwingPoint) -> Self { Self { point, consumed: false } }
}

impl SmtDetector {
    /// Still-valid evidence after the last paired close; empty after any error.
    #[must_use]
    pub fn active(&self) -> &[SmtEvidence] {
        if self.poisoned { &[] } else { &self.active }
    }

    /// Looks up an active witness for the strategy's exact original sweep.
    #[must_use]
    pub fn evidence_for_sweep(&self, sweep_id: Uuid) -> Option<&SmtEvidence> {
        self.active().iter().find(|evidence| evidence.sweep_id() == sweep_id)
    }

    /// Processes two frozen, simultaneous ICT observations in either order.
    ///
    /// Confirmation at the sweep candle's open is eligible because it was
    /// observable at the preceding candle's close. Current-bar confirmations
    /// are added only after divergence detection and are not used retroactively.
    ///
    /// # Errors
    /// Returns [`SmtError`] for mismatched instruments, invalid timestamps,
    /// inconsistent metadata, gaps/replays, overflow or exhausted capacity.
    /// Any failure poisons this detector; no later call can emit evidence.
    pub fn process_pair(&mut self, first: &Candle, first_batch: &IctDetectionBatch,
        second: &Candle, second_batch: &IctDetectionBatch) -> Result<SmtUpdate, SmtError> {
        if self.poisoned { return Err(SmtError::Poisoned); }
        let result = self.process_inner(first, first_batch, second, second_batch);
        if result.is_err() { self.poisoned = true; }
        result
    }

    fn process_inner(&mut self, first: &Candle, first_batch: &IctDetectionBatch,
        second: &Candle, second_batch: &IctDetectionBatch) -> Result<SmtUpdate, SmtError> {
        let pair = canonical_pair(first, first_batch, second, second_batch)?;
        validate_pair(&pair, self.last_close)?;
        let at = pair[0].0.close_time();
        let expires_at = at.checked_add_signed(Duration::minutes(EVIDENCE_TTL_MINUTES))
            .ok_or(SmtError::TimeOverflow)?;
        let mut active: Vec<_> = self.active.iter().filter(|evidence| {
            let (focus, peer) = if evidence.instrument() == pair[0].0.instrument() {
                (pair[0].0, pair[1].0)
            } else { (pair[1].0, pair[0].0) };
            at < evidence.expires_at() && !pivot_broken(evidence.peer_anchor(), peer)
                && match evidence.direction() {
                    Direction::Bullish => focus.low() >= evidence.focus_sweep().sweep_price(),
                    Direction::Bearish => focus.high() <= evidence.focus_sweep().sweep_price(),
                }
        }).cloned().collect();
        let mut newly_confirmed = Vec::new();
        for (focus_index, peer_index) in [(0, 1), (1, 0)] {
            let (focus, batch) = pair[focus_index];
            let peer = pair[peer_index].0;
            for signal in batch.signals() {
                if let SignalEvent::LiquiditySwept(sweep) = signal {
                    if let Some(evidence) = detect(sweep, focus, peer,
                        &self.anchors[focus_index], &self.anchors[peer_index], expires_at)? {
                        newly_confirmed.push(evidence);
                    }
                }
            }
        }
        suppress_opposing_evidence(&mut newly_confirmed);
        newly_confirmed.sort_by_key(SmtEvidence::id);
        active.extend(newly_confirmed.iter().cloned());
        active.sort_by_key(SmtEvidence::id);
        if active.len() > EVIDENCE_CAPACITY { return Err(SmtError::Capacity); }
        for (index, (candle, batch)) in pair.iter().enumerate() {
            self.anchors[index].retain(|anchor| eligible_age(&anchor.point, candle.open_time()));
            for anchor in &mut self.anchors[index] {
                anchor.consumed |= pivot_broken(&anchor.point, candle);
            }
            for anchor in batch.confirmed_swings() {
                self.anchors[index].push_back(anchor.clone().into());
                while self.anchors[index].len() > PIVOT_CAPACITY { self.anchors[index].pop_front(); }
            }
        }
        self.active = active;
        self.last_close = Some(at);
        Ok(SmtUpdate { newly_confirmed, active: self.active.clone() })
    }
}

fn suppress_opposing_evidence(evidence: &mut Vec<SmtEvidence>) {
    let bullish = evidence.iter().any(|row| row.direction() == Direction::Bullish);
    let bearish = evidence.iter().any(|row| row.direction() == Direction::Bearish);
    if bullish && bearish { evidence.clear(); }
}

type Observation<'a> = (&'a Candle, &'a IctDetectionBatch);

fn canonical_pair<'a>(first: &'a Candle, first_batch: &'a IctDetectionBatch,
    second: &'a Candle, second_batch: &'a IctDetectionBatch) -> Result<[Observation<'a>; 2], SmtError> {
    let identity = |candle: &Candle, symbol: &str, canonical: &str| {
        let instrument = candle.instrument();
        instrument.exchange == Exchange::Bybit && instrument.market == MarketKind::LinearPerpetual
            && instrument.venue_symbol.as_str() == symbol && instrument.canonical_symbol.as_str() == canonical
    };
    if identity(first, "BTCUSDT", "BTC-USDT-PERP") && identity(second, "ETHUSDT", "ETH-USDT-PERP") {
        Ok([(first, first_batch), (second, second_batch)])
    } else if identity(second, "BTCUSDT", "BTC-USDT-PERP") && identity(first, "ETHUSDT", "ETH-USDT-PERP") {
        Ok([(second, second_batch), (first, first_batch)])
    } else { Err(SmtError::Instruments) }
}

fn validate_pair(pair: &[Observation<'_>; 2], last_close: Option<DateTime<Utc>>) -> Result<(), SmtError> {
    let first = pair[0].0;
    for (candle, batch) in pair {
        if !candle.is_closed() || candle.timeframe() != Timeframe::M1 || candle.volume_unit() != VolumeUnit::BaseAsset
            || candle.close_time().signed_duration_since(candle.open_time()) != Duration::minutes(1)
            || candle.open_time().timestamp_subsec_nanos() != 0 || candle.open_time().timestamp().rem_euclid(60) != 0
            || candle.open_time() != first.open_time() || candle.close_time() != first.close_time() {
            return Err(SmtError::CandleAlignment);
        }
        if last_close.is_some_and(|at| at != candle.open_time()) || batch.series_reset() || batch.is_duplicate() {
            return Err(SmtError::Chronology);
        }
        if batch.confirmed_swings().iter().any(|anchor| anchor.instrument() != candle.instrument()
            || anchor.timeframe() != Timeframe::M1 || anchor.confirmed_at() != candle.close_time()) {
            return Err(SmtError::Metadata);
        }
        let mut sweep_ids = Vec::new();
        for signal in batch.signals() {
            if let SignalEvent::LiquiditySwept(sweep) = signal {
                let matching_extreme = match sweep.side() {
                    LiquiditySide::BuySide => sweep.sweep_price() == candle.high() && candle.close() <= sweep.reference_price(),
                    LiquiditySide::SellSide => sweep.sweep_price() == candle.low() && candle.close() >= sweep.reference_price(),
                };
                if sweep.instrument() != candle.instrument() || sweep.timeframe() != Timeframe::M1
                    || sweep.sweep_candle_open_time() != candle.open_time() || sweep.detected_at() != candle.close_time()
                    || !matching_extreme || sweep_ids.contains(&sweep.id()) {
                    return Err(SmtError::Metadata);
                }
                sweep_ids.push(sweep.id());
            }
        }
    }
    Ok(())
}

fn eligible_age(anchor: &SwingPoint, at: DateTime<Utc>) -> bool {
    anchor.confirmed_at() <= at
        && at.signed_duration_since(anchor.candle_open_time()) <= Duration::minutes(PIVOT_MAX_AGE_MINUTES)
}

fn pivot_broken(anchor: &SwingPoint, candle: &Candle) -> bool {
    match anchor.kind() {
        SwingKind::High => candle.high() > anchor.price(),
        SwingKind::Low => candle.low() < anchor.price(),
    }
}

fn detect(sweep: &LiquiditySweep, focus: &Candle, peer: &Candle,
    focus_anchors: &VecDeque<TrackedAnchor>, peer_anchors: &VecDeque<TrackedAnchor>,
    expires_at: DateTime<Utc>) -> Result<Option<SmtEvidence>, SmtError> {
    let kind = match sweep.side() { LiquiditySide::BuySide => SwingKind::High, LiquiditySide::SellSide => SwingKind::Low };
    let mut matching = focus_anchors.iter().filter(|anchor| !anchor.consumed).map(|anchor| &anchor.point)
        .filter(|anchor| anchor.kind() == kind && anchor.price() == sweep.reference_price()
            && eligible_age(anchor, focus.open_time()));
    let Some(focus_anchor) = matching.next() else { return Ok(None); };
    // Sweep metadata has no pivot identity: ambiguous equal-price anchors must
    // not silently select whichever peer relationship happens to diverge.
    if matching.next().is_some() { return Ok(None); }
    let peer_anchor = peer_anchors.iter().filter(|anchor| anchor.point.kind() == kind
        && eligible_age(&anchor.point, peer.open_time())
        && anchor.point.candle_open_time().signed_duration_since(focus_anchor.candle_open_time()).abs()
            <= Duration::minutes(PAIR_TOLERANCE_MINUTES))
        .min_by_key(|anchor| (anchor.point.candle_open_time().signed_duration_since(focus_anchor.candle_open_time()).abs(),
            std::cmp::Reverse(anchor.point.candle_open_time())));
    let Some(peer_tracked) = peer_anchor else { return Ok(None); };
    let peer_anchor = &peer_tracked.point;
    // Choose the peer by time first; do not search farther pivots until one
    // produces a convenient divergence or matching fractal model. Any strict
    // peer break rejects SMT.
    if peer_anchor.model() != focus_anchor.model() || peer_tracked.consumed || pivot_broken(peer_anchor, peer) {
        return Ok(None);
    }
    let bytes = serde_json::to_vec(&("ict-smt-paired-v1", sweep, focus_anchor, peer_anchor, focus, peer, expires_at))
        .map_err(|_| SmtError::EvidenceEncoding)?;
    Ok(Some(SmtEvidence {
        id: Uuid::new_v5(&Uuid::NAMESPACE_OID, &bytes), focus_sweep: sweep.clone(),
        focus_anchor: focus_anchor.clone(), peer_anchor: peer_anchor.clone(),
        focus_candle: focus.clone(), peer_candle: peer.clone(),
        direction: match sweep.side() { LiquiditySide::SellSide => Direction::Bullish, LiquiditySide::BuySide => Direction::Bearish },
        detected_at: focus.close_time(), expires_at,
    }))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rust_decimal::Decimal;

    use super::*;
    use crate::{ict_engine::{IctEngine, IctEngineConfig},
        models::{CandleInput, FractalModel, LiquiditySweepInput}};

    type TestResult = Result<(), Box<dyn Error>>;

    fn at(minute: i64) -> Result<DateTime<Utc>, Box<dyn Error>> {
        Ok(DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")?.with_timezone(&Utc)
            .checked_add_signed(Duration::minutes(minute)).ok_or("test timestamp overflow")?)
    }

    fn instrument(symbol: &str) -> Result<InstrumentId, Box<dyn Error>> {
        let canonical = match symbol { "BTCUSDT" => "BTC-USDT-PERP", "ETHUSDT" => "ETH-USDT-PERP", _ => return Err("fixture symbol".into()) };
        Ok(InstrumentId::new(Exchange::Bybit, MarketKind::LinearPerpetual, symbol, canonical)?)
    }

    fn candle(symbol: &str, minute: i64, prices: [i64; 4]) -> Result<Candle, Box<dyn Error>> {
        Ok(Candle::new(CandleInput { instrument: instrument(symbol)?, timeframe: Timeframe::M1,
            open_time: at(minute)?, close_time: at(minute+1)?, open: Decimal::from(prices[0]),
            high: Decimal::from(prices[1]), low: Decimal::from(prices[2]), close: Decimal::from(prices[3]),
            volume: Decimal::ONE, volume_unit: VolumeUnit::BaseAsset, trade_count: None, is_closed: true })?)
    }

    fn ict() -> Result<IctEngine, Box<dyn Error>> {
        let default = IctEngineConfig::default();
        Ok(IctEngine::new(IctEngineConfig::new(FractalModel::ThreeCandle, default.break_basis(),
            default.fvg_mitigation(), default.sweep_confirmation(), default.limits(),
            default.displacement().clone(), default.killzones())?))
    }

    fn mirror(row: [i64; 4]) -> [i64; 4] { [1000-row[0], 1000-row[2], 1000-row[1], 1000-row[3]] }

    fn formation(bullish: bool, both_sweep: bool, reverse_order: bool)
        -> Result<(SmtDetector, IctEngine, SmtUpdate), Box<dyn Error>> {
        let mut detector = SmtDetector::default();
        let mut engine = ict()?;
        let btc = [[100,102,98,100], [100,110,98,101], [100,105,98,101], [105,111,99,109]];
        let eth = [[200,202,198,200], [200,220,198,201], [200,210,198,201],
            if both_sweep { [210,221,199,219] } else { [210,219,199,218] }];
        let mut final_update = None;
        for (index, (btc_row, eth_row)) in btc.into_iter().zip(eth).enumerate() {
            let minute = i64::try_from(index)?;
            let btc_candle = candle("BTCUSDT", minute, if bullish { mirror(btc_row) } else { btc_row })?;
            let eth_candle = candle("ETHUSDT", minute, if bullish { mirror(eth_row) } else { eth_row })?;
            let btc_batch = engine.process_candle(&btc_candle)?;
            let eth_batch = engine.process_candle(&eth_candle)?;
            let update = if reverse_order { detector.process_pair(&eth_candle, &eth_batch, &btc_candle, &btc_batch)? }
                else { detector.process_pair(&btc_candle, &btc_batch, &eth_candle, &eth_batch)? };
            if index < 3 { assert!(update.newly_confirmed.is_empty()); }
            final_update = Some(update);
        }
        Ok((detector, engine, final_update.ok_or("missing final update")?))
    }

    #[test]
    fn real_sweeps_confirm_bullish_and_bearish_smt_with_order_independent_ids() -> TestResult {
        for bullish in [false, true] {
            let (detector, _, result) = formation(bullish, false, false)?;
            let (_, _, reversed) = formation(bullish, false, true)?;
            assert_eq!(result, reversed);
            assert_eq!(result.newly_confirmed.len(), 1);
            let evidence = &result.newly_confirmed[0];
            assert_eq!(evidence.instrument(), &instrument("BTCUSDT")?);
            assert_eq!(evidence.direction(), if bullish { Direction::Bullish } else { Direction::Bearish });
            assert_eq!(evidence.detected_at(), at(4)?);
            assert_eq!(evidence.expires_at(), at(16)?);
            assert_eq!(evidence.focus_anchor().confirmed_at(), at(3)?);
            assert_eq!(evidence.peer_anchor().confirmed_at(), at(3)?);
            assert_eq!(evidence.focus_candle().open_time(), evidence.peer_candle().open_time());
            assert_eq!(detector.evidence_for_sweep(evidence.sweep_id()), Some(evidence));
            let json = serde_json::to_value(evidence)?;
            assert_eq!(json["focus_sweep"]["reference_price"], if bullish { "890" } else { "110" });
            assert!(json.get("focus_anchor").is_some() && json.get("peer_anchor").is_some());
        }
        Ok(())
    }

    #[test]
    fn simultaneous_sweeps_of_the_paired_levels_do_not_confirm_smt() -> TestResult {
        for bullish in [false, true] {
            let (detector, _, result) = formation(bullish, true, false)?;
            assert!(result.newly_confirmed.is_empty() && detector.active().is_empty());
        }
        Ok(())
    }

    #[test]
    fn evidence_expires_and_strict_extreme_breaks_invalidate_in_both_directions() -> TestResult {
        for bullish in [false, true] {
            for break_focus in [false, true] {
                let (mut detector, mut engine, _) = formation(bullish, false, false)?;
                let btc = if break_focus { [109,112,100,110] } else { [109,111,100,110] };
                let eth = if break_focus { [218,220,200,219] } else { [218,221,200,219] };
                let btc = candle("BTCUSDT", 4, if bullish { mirror(btc) } else { btc })?;
                let eth = candle("ETHUSDT", 4, if bullish { mirror(eth) } else { eth })?;
                detector.process_pair(&btc, &engine.process_candle(&btc)?, &eth, &engine.process_candle(&eth)?)?;
                assert!(detector.active().is_empty());
            }
            let (mut detector, mut engine, _) = formation(bullish, false, false)?;
            for minute in 4..16 {
                let btc = [109,111,100,110];
                let eth = [218,220,200,219];
                let btc = candle("BTCUSDT", minute, if bullish { mirror(btc) } else { btc })?;
                let eth = candle("ETHUSDT", minute, if bullish { mirror(eth) } else { eth })?;
                detector.process_pair(&btc, &engine.process_candle(&btc)?, &eth, &engine.process_candle(&eth)?)?;
                assert_eq!(detector.active().len(), usize::from(minute < 15));
            }
        }
        Ok(())
    }

    #[test]
    fn missing_peer_observation_or_replayed_pair_poison_processing() -> TestResult {
        for minute in [3, 5] {
            let (mut detector, mut engine, _) = formation(false, false, false)?;
            let btc = candle("BTCUSDT", minute, [109,111,100,110])?;
            let eth = candle("ETHUSDT", minute, [218,220,200,219])?;
            // A separate fresh engine supplies well-formed metadata even for
            // timestamps that violate the paired detector's retained history.
            let mut fresh = ict()?;
            let result = detector.process_pair(&btc, &fresh.process_candle(&btc)?, &eth, &fresh.process_candle(&eth)?);
            assert_eq!(result, Err(SmtError::Chronology));
            assert!(detector.active().is_empty());
            let next_btc = candle("BTCUSDT", 4, [109,111,100,110])?;
            let next_eth = candle("ETHUSDT", 4, [218,220,200,219])?;
            assert_eq!(detector.process_pair(&next_btc, &engine.process_candle(&next_btc)?,
                &next_eth, &engine.process_candle(&next_eth)?), Err(SmtError::Poisoned));
        }
        let (mut detector, _, _) = formation(false, false, false)?;
        let mut fresh = ict()?;
        let btc = candle("BTCUSDT", 4, [109,111,100,110])?;
        let eth = candle("ETHUSDT", 5, [218,220,200,219])?;
        assert_eq!(detector.process_pair(&btc, &fresh.process_candle(&btc)?, &eth, &fresh.process_candle(&eth)?),
            Err(SmtError::CandleAlignment));
        Ok(())
    }

    fn anchor(symbol: &str, kind: SwingKind, price: i64, minute: i64) -> Result<TrackedAnchor, Box<dyn Error>> {
        Ok(SwingPoint::new(instrument(symbol)?, Timeframe::M1, kind, Decimal::from(price),
            at(minute)?, at(minute+2)?, FractalModel::ThreeCandle)?.into())
    }

    fn sweep(focus: &Candle, reference: i64) -> Result<LiquiditySweep, Box<dyn Error>> {
        Ok(LiquiditySweep::new(LiquiditySweepInput { id: Uuid::from_u128(1), instrument: focus.instrument().clone(),
            timeframe: Timeframe::M1, side: LiquiditySide::BuySide, reference_price: Decimal::from(reference),
            sweep_price: focus.high(), sweep_candle_open_time: focus.open_time(), detected_at: focus.close_time() })?)
    }

    #[test]
    fn stale_unconfirmed_misaligned_or_ambiguous_anchors_cannot_establish_evidence() -> TestResult {
        let focus = candle("BTCUSDT", 65, [105,111,99,109])?;
        let peer = candle("ETHUSDT", 65, [210,219,199,218])?;
        let sweep = sweep(&focus, 110)?;
        for (focus_minutes, peer_minute) in [
            (vec![1], 1), // too old
            (vec![64], 64), // not yet confirmed when the sweep started
            (vec![60], 56), // corresponding pivot more than three bars away
            (vec![59, 60], 60), // ambiguous equal-price focus references
        ] {
            let focus_anchors = focus_minutes.into_iter().map(|minute| anchor("BTCUSDT", SwingKind::High, 110, minute))
                .collect::<Result<VecDeque<_>, _>>()?;
            let peer_anchors = VecDeque::from([anchor("ETHUSDT", SwingKind::High, 220, peer_minute)?]);
            assert!(detect(&sweep, &focus, &peer, &focus_anchors, &peer_anchors, at(78)?)?.is_none());
        }
        let focus_anchors = VecDeque::from([anchor("BTCUSDT", SwingKind::High, 110, 60)?]);
        let peer_anchors = VecDeque::from([anchor("ETHUSDT", SwingKind::High, 220, 60)?]);
        assert!(detect(&sweep, &focus, &peer, &focus_anchors, &peer_anchors, at(78)?)?.is_some());
        Ok(())
    }

    #[test]
    fn closest_peer_is_chosen_before_testing_divergence() -> TestResult {
        let focus = candle("BTCUSDT", 10, [105,111,99,109])?;
        let peer = candle("ETHUSDT", 10, [210,219,199,218])?;
        let focus_anchors = VecDeque::from([anchor("BTCUSDT", SwingKind::High, 110, 6)?]);
        let peer_anchors = VecDeque::from([
            anchor("ETHUSDT", SwingKind::High, 220, 5)?,
            anchor("ETHUSDT", SwingKind::High, 215, 6)?,
        ]);
        assert!(detect(&sweep(&focus, 110)?, &focus, &peer, &focus_anchors, &peer_anchors, at(23)?)?.is_none());
        Ok(())
    }

    #[test]
    fn consumed_closest_peer_cannot_fall_back_to_an_unswept_farther_pivot() -> TestResult {
        let focus = candle("BTCUSDT", 10, [105,111,99,109])?;
        let peer = candle("ETHUSDT", 10, [210,219,199,218])?;
        let focus_anchors = VecDeque::from([anchor("BTCUSDT", SwingKind::High, 110, 6)?]);
        let mut closest = anchor("ETHUSDT", SwingKind::High, 220, 6)?;
        closest.consumed = true;
        let peer_anchors = VecDeque::from([anchor("ETHUSDT", SwingKind::High, 225, 5)?, closest]);
        assert!(detect(&sweep(&focus, 110)?, &focus, &peer, &focus_anchors, &peer_anchors, at(23)?)?.is_none());
        Ok(())
    }

    #[test]
    fn mixed_fractal_pair_cannot_qualify_or_fall_back_to_a_farther_matching_model() -> TestResult {
        let focus = candle("BTCUSDT", 10, [105,111,99,109])?;
        let peer = candle("ETHUSDT", 10, [210,219,199,218])?;
        let five = |symbol, minute| -> Result<TrackedAnchor, Box<dyn Error>> {
            Ok(SwingPoint::new(instrument(symbol)?, Timeframe::M1, SwingKind::High,
                Decimal::from(if symbol == "BTCUSDT" { 110 } else { 220 }),
                at(minute)?, at(minute+3)?, FractalModel::FiveCandle)?.into())
        };
        for (focus_anchor, closest, farther) in [
            (anchor("BTCUSDT", SwingKind::High, 110, 6)?, five("ETHUSDT", 6)?,
                anchor("ETHUSDT", SwingKind::High, 220, 5)?),
            (five("BTCUSDT", 6)?, anchor("ETHUSDT", SwingKind::High, 220, 6)?, five("ETHUSDT", 5)?),
        ] {
            let focus_anchors = VecDeque::from([focus_anchor]);
            let peer_anchors = VecDeque::from([farther, closest]);
            assert!(detect(&sweep(&focus, 110)?, &focus, &peer, &focus_anchors, &peer_anchors, at(23)?)?.is_none());
        }
        Ok(())
    }

    #[test]
    fn simultaneous_opposing_divergence_is_suppressed_without_order_preference() -> TestResult {
        let (_, _, bullish) = formation(true, false, false)?;
        let (_, _, bearish) = formation(false, false, false)?;
        let mut forward = bullish.newly_confirmed.clone();
        forward.extend(bearish.newly_confirmed.clone());
        let mut reversed = bearish.newly_confirmed;
        reversed.extend(bullish.newly_confirmed);
        suppress_opposing_evidence(&mut forward);
        suppress_opposing_evidence(&mut reversed);
        assert!(forward.is_empty() && reversed.is_empty());
        Ok(())
    }

    #[test]
    fn peer_wick_break_is_consumed_even_without_a_close_back_sweep() -> TestResult {
        let mut detector = SmtDetector::default();
        let mut engine = ict()?;
        let btc = [[100,102,98,100], [100,110,98,101], [100,105,98,101], [105,109,99,108], [105,111,99,109]];
        let eth = [[200,202,198,200], [200,220,198,201], [200,210,198,201], [210,222,199,221], [210,219,199,218]];
        for (index, (btc, eth)) in btc.into_iter().zip(eth).enumerate() {
            let btc = candle("BTCUSDT", i64::try_from(index)?, btc)?;
            let eth = candle("ETHUSDT", i64::try_from(index)?, eth)?;
            let result = detector.process_pair(&btc, &engine.process_candle(&btc)?, &eth, &engine.process_candle(&eth)?)?;
            assert!(result.newly_confirmed.is_empty());
        }
        assert!(detector.active().is_empty());
        Ok(())
    }

    #[test]
    fn wrong_instruments_and_foreign_pivot_metadata_fail_closed() -> TestResult {
        let btc = candle("BTCUSDT", 0, [100,102,98,100])?;
        let mut engine = ict()?;
        let batch = engine.process_candle(&btc)?;
        let mut detector = SmtDetector::default();
        assert_eq!(detector.process_pair(&btc, &batch, &btc, &batch), Err(SmtError::Instruments));

        let mut engine = ict()?;
        for minute in 0..2 {
            engine.process_candle(&candle("BTCUSDT", minute, if minute == 1 { [100,110,98,101] } else { [100,102,98,100] })?)?;
        }
        let btc = candle("BTCUSDT", 2, [100,105,98,101])?;
        let eth = candle("ETHUSDT", 2, [200,210,198,201])?;
        let foreign_batch = engine.process_candle(&btc)?;
        let mut fresh = ict()?;
        let btc_empty = fresh.process_candle(&btc)?;
        let mut detector = SmtDetector::default();
        assert_eq!(detector.process_pair(&btc, &btc_empty, &eth, &foreign_batch), Err(SmtError::Metadata));
        Ok(())
    }
}
