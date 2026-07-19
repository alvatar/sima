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
        [run]
        root_seed = 7
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = [{behaviors}]

        [execution]
        store = "{store}"
        workers = {workers}
        max_attempts = 3
        attempt_timeout_ms = 5000
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

/// The typed journal of `config`'s run in its store.
pub fn journal_events(config: &LoadedConfig) -> Vec<Event> {
    let store = Store::open(&config.store).expect("open store");
    store
        .journal(&config.run.id())
        .expect("read journal")
        .iter()
        .map(|line| Record::from_line(line).expect("parse journal line").event)
        .collect()
}
