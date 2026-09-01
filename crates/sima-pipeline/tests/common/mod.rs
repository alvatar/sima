//! Shared fixtures for the pipeline integration suites.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use sima_core::Result;
use sima_pipeline::{Event, LoadedConfig, Record, load};
use sima_store::Store;

/// Writes a `sima.toml` named `name` under `dir` and loads it. `behaviors`
/// is the TOML list literal's inner content; `store` is the store path as
/// written into the file, so it resolves relative to `dir`.
pub fn loaded_with(
    dir: &Path,
    name: &str,
    behaviors: &str,
    workers: u32,
    store: &str,
) -> Result<LoadedConfig> {
    let text = format!(
        r#"
        [search]
        root_seed = 7
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = [{behaviors}]

        [config]
        store = "{store}"
        max_attempts = 3
        attempt_timeout_ms = 5000

        [orchestrator]
        workers = {workers}
    "#
    );
    let path: PathBuf = dir.join(name);
    std::fs::write(&path, text).expect("write config");
    load(&path)
}

/// [`loaded_with`] defaults: a file named `sima.toml` whose store lives
/// beside it at `./store`.
pub fn loaded(dir: &Path, behaviors: &str, workers: u32) -> Result<LoadedConfig> {
    loaded_with(dir, "sima.toml", behaviors, workers, "./store")
}

/// Writes `text` as a config file named `name` under `dir` and loads it —
/// for suites whose configs need keys beyond [`loaded_with`]'s shape.
pub fn loaded_text(dir: &Path, name: &str, text: &str) -> Result<LoadedConfig> {
    let path: PathBuf = dir.join(name);
    std::fs::write(&path, text).expect("write config");
    load(&path)
}

/// The typed journal of `config`'s search in its store.
pub fn journal_events(config: &LoadedConfig) -> Vec<Event> {
    let store = Store::open(&config.store).expect("open store");
    store
        .journal(&config.search.id())
        .expect("read journal")
        .iter()
        .map(|line| Record::from_line(line).expect("parse journal line").event)
        .collect()
}
