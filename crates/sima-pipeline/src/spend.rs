//! `spend`: the read-only rental-ledger query.
//!
//! Like the other read-only queries, it opens the store and reads what a run
//! left behind — here the durable spend ledger, plus the rentals still open,
//! charged from their stamp to now. It touches no store object and mutates
//! nothing.

use std::time::{SystemTime, UNIX_EPOCH};

use sima_core::Result;
use sima_provider::{SpendReport, spend_report};
use sima_store::Store;

use crate::config::LoadedConfig;

/// The run's rental spend as of now: the closed entries, the rentals still
/// accruing, and their total. The store need not hold a finalized run — a
/// ledger outlives both the machines and the process that rented them.
pub fn spend(config: &LoadedConfig) -> Result<SpendReport> {
    let store = Store::open(&config.store)?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    spend_report(&store, &config.run.id(), now_ms)
}
