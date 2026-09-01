//! [`remove`]: delete the search a config describes, under its search lock, and
//! [`remove_matching`]: delete a search of that store the config no longer names.

use sima_core::{Error, Result};
use sima_model::SearchId;
use sima_store::{RemovalReport, Store};

use crate::config::LoadedConfig;
use crate::searches::resolve_search;

/// Removes the search a loaded config describes: validates the store and the search
/// exist, acquires the search's orchestrator lock so a live orchestrator on that
/// search is excluded, then deletes the search and everything no surviving search
/// references through [`Store::remove_search`]. The search id comes from the
/// config's identity section, as [`status`](crate::status) derives it.
///
/// Validation precedes any mutation, matching [`status`](crate::status) and
/// [`report`](crate::report): a store root that does not exist is
/// [`Error::Validation`] before the store skeleton is created, and a search absent
/// from the store is [`Error::Validation`] ("search not found") before the search's
/// lock directory is created. Acquiring the lock creates nothing new, since the
/// search directory already exists.
///
/// The lock is held for the whole removal and releases when this call returns.
/// Retention searches offline, so the exclusion covers the removal itself.
pub fn remove(config: &LoadedConfig) -> Result<RemovalReport> {
    let store = opened(config)?;
    let search = config.search.id();
    if !store.searches()?.contains(&search) {
        return Err(Error::Validation(format!(
            "cannot remove search {search}: search not found"
        )));
    }
    removed(&store, search)
}

/// Removes the search in the config's store whose id begins with `prefix`.
///
/// A store outlives the identity that filled it — an edited seed or a changed
/// parameter is a different search against the same store — so a search the config no
/// longer names is reachable only this way. Everything past resolving the search
/// is [`remove`]'s: the same lock, the same deletion, the same report. An
/// ambiguous prefix is refused naming what it matched, before anything is
/// locked or deleted.
pub fn remove_matching(config: &LoadedConfig, prefix: &str) -> Result<RemovalReport> {
    let store = opened(config)?;
    let search = resolve_search(&store, prefix)?;
    removed(&store, search)
}

/// The config's store, opened for a removal: a root that does not exist is
/// [`Error::Validation`] before the store skeleton is created.
fn opened(config: &LoadedConfig) -> Result<Store> {
    if !config.store.is_dir() {
        return Err(Error::Validation(format!(
            "store {} does not exist: no search was ever driven there",
            config.store.display()
        )));
    }
    Store::open(&config.store)
}

/// Deletes `search` under its own orchestrator lock, which is what excludes a
/// live orchestrator on it.
fn removed(store: &Store, search: SearchId) -> Result<RemovalReport> {
    let _lock = store.acquire_search_lock(&search)?;
    store.remove_search(&search)
}
