//! `machines`: the read-only machine-reputation query.
//!
//! Like the spend view, it opens the store and reads durable operational
//! state — here the incident ledger, grouped per machine — rather than a search's
//! journal. It is store-scoped, not search-scoped: every search using the store
//! shares one reputation record, so the query names no search.

use sima_core::Result;
use sima_provider::{MachineReport, machine_report};
use sima_store::Store;

use crate::config::LoadedConfig;

/// The store's recorded machine incidents, grouped per machine with the
/// blacklist verdict. The store need not hold a finalized search — reputation
/// outlives any single search.
pub fn machines(config: &LoadedConfig) -> Result<MachineReport> {
    let store = Store::open(&config.store)?;
    machine_report(&store)
}
