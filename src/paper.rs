//! Credential-free paper trading, separated from authenticated execution.
//!
//! The broker is a conservative closed-candle simulation, not an exchange fill
//! model. No type in this module accepts credentials or submits private orders.
//! Funding, queue priority, partial fills and liquidation are not simulated.

pub mod broker;
pub mod engine;
pub mod market;

pub use broker::*;
pub use engine::*;
pub use market::*;
