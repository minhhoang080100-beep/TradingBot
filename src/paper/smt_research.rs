//! Mandatory paired-SMT research gate; deliberately unavailable to live journals.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::{ict_engine::IctDetectionBatch, models::{Candle, Direction, FractalModel, TradeIntent},
    smt::{SmtDetector, SmtError, SmtEvidence}};
use super::{broker::{PaperFillTiming, PaperOrderSnapshot},
    research_context::{ConfirmedM5Bias, ResearchContextError}};

/// Fixed research choices; account limits and minimum net RR are not parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct SmtResearchConfig {
    /// M1 pivot confirmation model shared by both assets.
    pub fractal_model: FractalModel,
    /// Require the entry direction to match the last completed M5 structure.
    pub require_m5_alignment: bool,
    /// Fixed next-bar versus one-bar-delay sensitivity assumption.
    pub fill_timing: PaperFillTiming,
}

/// Immutable proof captured only after a real paper admission succeeds.
#[derive(Clone, Debug, Serialize)]
pub struct SmtAdmissionEvidence {
    /// Actual generated MSS/FVG intent, including its original stop anchor.
    pub intent: TradeIntent,
    /// Frozen paired anchors and sweep-time candles; never replaced by newer pivots.
    pub smt: SmtEvidence,
    /// Synchronized peer candle proving its paired level still held at admission.
    pub peer_at_admission: Candle,
    /// Last fully closed M5 structure direction at admission.
    pub m5_bias: Option<Direction>,
    /// Last complete M5 close known when the decision was taken.
    pub m5_completed_at: Option<DateTime<Utc>>,
    /// Closed M1 decision timestamp; no prior fill is inferred.
    pub approved_at: DateTime<Utc>,
    /// The exact rounded/sized order accepted by the shared risk and paper book.
    pub order: PaperOrderSnapshot,
}

/// Research-only audit output, outside legacy V1-V4 serialized steps.
#[derive(Clone, Debug, Default, Serialize)]
pub struct SmtResearchReport {
    /// Newly confirmed paired divergences on live evaluation bars, excluding warmup.
    pub confirmed_smt: u64,
    /// Still-pending entries cancelled at a close after their SMT expired or broke.
    pub pending_cancelled_after_smt_lost: u64,
    /// Causal evidence for every admitted order, whether later filled or cancelled.
    pub admissions: Vec<SmtAdmissionEvidence>,
}

/// Fatal research data or audit inconsistency; the portfolio fails closed.
#[derive(Debug, Error)]
pub enum SmtResearchError {
    /// Paired detector rejected causal or synchronized input.
    #[error(transparent)]
    Detector(#[from] SmtError),
    /// Higher-timeframe aggregation rejected incomplete or inconsistent input.
    #[error(transparent)]
    Context(#[from] ResearchContextError),
    /// A required instrument, witness or audit binding was absent.
    #[error("SMT research invariant: {0}")]
    Invariant(&'static str),
    /// Hard audit retention capacity was reached.
    #[error("SMT research admission audit capacity reached")]
    Capacity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SmtGateFailure { Missing, M5Alignment }

#[derive(Debug)]
pub(crate) struct SmtResearchState {
    pub config: SmtResearchConfig,
    pub report: SmtResearchReport,
    detector: SmtDetector,
    contexts: BTreeMap<String, ConfirmedM5Bias>,
    active: BTreeMap<Uuid, SmtEvidence>,
    admitted_sweeps: BTreeMap<Uuid, Uuid>,
    last_candles: BTreeMap<String, Candle>,
}

impl SmtResearchState {
    pub fn new(config: SmtResearchConfig) -> Self {
        Self { config, report: SmtResearchReport::default(), detector: SmtDetector::default(),
            contexts: ["BTCUSDT", "ETHUSDT"].into_iter().map(|symbol|
                (symbol.to_owned(), ConfirmedM5Bias::default())).collect(), active: BTreeMap::new(),
            admitted_sweeps: BTreeMap::new(), last_candles: BTreeMap::new() }
    }

    pub fn analyze(&mut self, pairs: &[(&Candle, IctDetectionBatch)], warmup: bool) -> Result<(), SmtResearchError> {
        let [btc, eth] = pairs else { return Err(SmtResearchError::Invariant("expected exactly BTC and ETH")); };
        let update = self.detector.process_pair(btc.0, &btc.1, eth.0, &eth.1)?;
        for (candle, _) in pairs {
            self.contexts.get_mut(candle.instrument().venue_symbol.as_str())
                .ok_or(SmtResearchError::Invariant("unknown M5 context"))?.process(candle)?;
        }
        if !warmup {
            self.report.confirmed_smt = self.report.confirmed_smt.saturating_add(
                u64::try_from(update.newly_confirmed.len()).unwrap_or(u64::MAX));
        }
        self.active = update.active.into_iter().map(|evidence| (evidence.sweep_id(), evidence)).collect();
        self.last_candles = pairs.iter().map(|(candle, _)|
            (candle.instrument().venue_symbol.as_str().to_owned(), (*candle).clone())).collect();
        Ok(())
    }

    pub fn allowed_sweeps(&self) -> BTreeSet<Uuid> { self.active.keys().copied().collect() }

    pub fn gate(&self, intent: &TradeIntent, at: DateTime<Utc>) -> Result<Option<SmtGateFailure>, SmtResearchError> {
        if self.report.admissions.len() >= 100_000 { return Err(SmtResearchError::Capacity); }
        let Some(evidence) = self.active.get(&intent.liquidity_sweep().id()) else { return Ok(Some(SmtGateFailure::Missing)); };
        if evidence.instrument() != intent.instrument() || evidence.direction() != intent.direction()
            || evidence.focus_sweep() != intent.liquidity_sweep()
            || evidence.detected_at() > at || evidence.expires_at() <= at {
            return Ok(Some(SmtGateFailure::Missing));
        }
        let context = self.contexts.get(intent.instrument().venue_symbol.as_str())
            .ok_or(SmtResearchError::Invariant("missing entry M5 context"))?;
        if self.config.require_m5_alignment && context.bias() != Some(intent.direction()) {
            return Ok(Some(SmtGateFailure::M5Alignment));
        }
        Ok(None)
    }

    pub fn record_admission(&mut self, intent: &TradeIntent, at: DateTime<Utc>, order: PaperOrderSnapshot) -> Result<(), SmtResearchError> {
        if self.gate(intent, at)?.is_some() || order.intent_id != intent.id() {
            return Err(SmtResearchError::Invariant("admitted order lacks its exact active SMT proof"));
        }
        let context = self.contexts.get(intent.instrument().venue_symbol.as_str())
            .ok_or(SmtResearchError::Invariant("missing admission M5 context"))?;
        let smt = self.active.get(&intent.liquidity_sweep().id()).cloned()
            .ok_or(SmtResearchError::Invariant("missing admission SMT witness"))?;
        let peer_at_admission = self.last_candles.get(smt.peer_anchor().instrument().venue_symbol.as_str()).cloned()
            .ok_or(SmtResearchError::Invariant("missing admission peer candle"))?;
        if peer_at_admission.close_time() != at { return Err(SmtResearchError::Invariant("stale admission peer candle")); }
        self.report.admissions.push(SmtAdmissionEvidence { intent: intent.clone(), smt, peer_at_admission,
            m5_bias: context.bias(), m5_completed_at: context.last_completed_at(), approved_at: at, order });
        self.admitted_sweeps.insert(intent.id(), intent.liquidity_sweep().id());
        Ok(())
    }

    pub fn lost_pending(&self, pending_ids: &BTreeSet<Uuid>) -> Vec<Uuid> {
        pending_ids.iter().filter(|id| self.admitted_sweeps.get(id)
            .is_some_and(|sweep| !self.active.contains_key(sweep))).copied().collect()
    }
}
