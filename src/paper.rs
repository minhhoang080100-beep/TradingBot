//! Credential-free paper trading, separated from authenticated execution.
//!
//! The broker is a conservative closed-candle simulation, not an exchange fill
//! model. No type in this module accepts credentials or submits private orders.
//! Funding, queue priority, partial fills and liquidation are not simulated.

pub mod broker;
pub mod engine;
pub mod market;
pub mod portfolio;
pub mod book;
pub mod multi_portfolio;
pub mod research_context;
pub mod smt_research;

pub use broker::*;
pub use engine::*;
pub use market::*;
pub use portfolio::*;
pub use book::*;
pub use multi_portfolio::*;
pub use smt_research::{SmtAdmissionEvidence, SmtResearchConfig, SmtResearchReport};
