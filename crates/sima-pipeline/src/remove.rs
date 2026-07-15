//! [`remove`]: delete the run a config describes, under its run lock.

use sima_core::{Error, Result};
use sima_store::{RemovalReport, Store};

use crate::config::LoadedConfig;

/// Removes the run a loaded config describes: validates the store and the run
/// exist, acquires the run's orchestrator lock so a live orchestrator on that
/// run is excluded, then deletes the run's exclusive closure through
/// [`Store::remove_run`]. The run id comes from the config's identity section,
/// as [`status`](crate::status) derives it.
///
/// Validation precedes any mutation, matching [`status`](crate::status) and
/// [`report`](crate::report): a store root that does not exist is
/// [`Error::Validation`] before the store skeleton is created, and a run absent
/// from the store is [`Error::Validation`] ("run not found") before the run's
/// lock directory is created. Acquiring the lock creates nothing new, since the
/// run directory already exists.
///
/// The lock is held for the whole removal and releases when this call returns.
/// Retention runs offline, so the exclusion covers the removal itself.
pub fn remove(config: &LoadedConfig) -> Result<RemovalReport> {
    if !config.store.is_dir() {
        return Err(Error::Validation(format!(
            "store {} does not exist: no run was ever driven there",
            config.store.display()
        )));
    }
    let store = Store::open(&config.store)?;
    let run = config.run.id();
    if !store.runs()?.contains(&run) {
        return Err(Error::Validation(format!(
            "cannot remove run {run}: run not found"
        )));
    }
    let _lock = store.acquire_run_lock(&run)?;
    store.remove_run(&run)
}
