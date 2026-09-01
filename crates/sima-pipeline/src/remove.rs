//! [`remove`]: delete the run a config describes, under its run lock, and
//! [`remove_matching`]: delete a run of that store the config no longer names.

use sima_core::{Error, Result};
use sima_model::SearchId;
use sima_store::{RemovalReport, Store};

use crate::config::LoadedConfig;
use crate::runs::resolve_run;

/// Removes the run a loaded config describes: validates the store and the run
/// exist, acquires the run's orchestrator lock so a live orchestrator on that
/// run is excluded, then deletes the run and everything no surviving run
/// references through [`Store::remove_search`]. The run id comes from the
/// config's identity section, as [`status`](crate::status) derives it.
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
    let store = opened(config)?;
    let run = config.run.id();
    if !store.searches()?.contains(&run) {
        return Err(Error::Validation(format!(
            "cannot remove run {run}: run not found"
        )));
    }
    removed(&store, run)
}

/// Removes the run in the config's store whose id begins with `prefix`.
///
/// A store outlives the identity that filled it — an edited seed or a changed
/// parameter is a different run against the same store — so a run the config no
/// longer names is reachable only this way. Everything past resolving the run
/// is [`remove`]'s: the same lock, the same deletion, the same report. An
/// ambiguous prefix is refused naming what it matched, before anything is
/// locked or deleted.
pub fn remove_matching(config: &LoadedConfig, prefix: &str) -> Result<RemovalReport> {
    let store = opened(config)?;
    let run = resolve_run(&store, prefix)?;
    removed(&store, run)
}

/// The config's store, opened for a removal: a root that does not exist is
/// [`Error::Validation`] before the store skeleton is created.
fn opened(config: &LoadedConfig) -> Result<Store> {
    if !config.store.is_dir() {
        return Err(Error::Validation(format!(
            "store {} does not exist: no run was ever driven there",
            config.store.display()
        )));
    }
    Store::open(&config.store)
}

/// Deletes `run` under its own orchestrator lock, which is what excludes a
/// live orchestrator on it.
fn removed(store: &Store, run: SearchId) -> Result<RemovalReport> {
    let _lock = store.acquire_search_lock(&run)?;
    store.remove_search(&run)
}
