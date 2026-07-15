//! [`remove`]: delete the run a config describes, under its run lock.

use sima_core::Result;
use sima_store::{RemovalReport, Store};

use crate::config::LoadedConfig;

/// Removes the run a loaded config describes: acquires the run's orchestrator
/// lock so a live orchestrator on that run is excluded, then deletes the run's
/// exclusive closure through [`Store::remove_run`]. The run id comes from the
/// config's identity section, as [`status`](crate::status) derives it.
///
/// The lock releases when the returned report drops out of scope at the call
/// site — retention runs offline, so the exclusion covers the removal itself.
pub fn remove(config: &LoadedConfig) -> Result<RemovalReport> {
    let store = Store::open(&config.store)?;
    let run = config.run.id();
    let _lock = store.acquire_run_lock(&run)?;
    store.remove_run(&run)
}
