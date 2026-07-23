//! Shared fixtures for the real-domain end-to-end suites.

use std::path::{Path, PathBuf};
use std::sync::Once;

use sima_core::Result;
use sima_pipeline::{Event, LoadedConfig, Record, load};
use sima_store::Store;

/// Writes `text` as a config file named `name` under `dir` and loads it.
/// Also ensures the worker binary exists: these tests drive `orchestrate`,
/// whose worker discovery finds `sima-worker` in the parent directory of
/// this test executable's own directory once it is built.
pub fn loaded_text(dir: &Path, name: &str, text: &str) -> Result<LoadedConfig> {
    build_worker_binary();
    let path: PathBuf = dir.join(name);
    std::fs::write(&path, text).expect("write config");
    load(&path)
}

/// The journal events of the run `config` describes, in append order. Each
/// top-level file under `tests/` compiles as its own crate, so a helper only
/// some suites use reads as dead code in the others.
#[allow(dead_code)]
pub fn journal_events(config: &LoadedConfig) -> Vec<Event> {
    let store = Store::open(&config.store).expect("open store");
    store
        .journal(&config.run.id())
        .expect("read journal")
        .iter()
        .map(|line| Record::from_line(line).expect("parse journal line").event)
        .collect()
}

/// Builds the `sima-worker` binary once per test process. Cargo builds
/// another crate's binary only when it is in the build graph, so the suites
/// that spawn workers build it explicitly.
fn build_worker_binary() {
    static BUILD: Once = Once::new();
    BUILD.call_once(|| {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = std::process::Command::new(cargo)
            .args(["build", "-p", "sima-worker"])
            .status()
            .expect("run cargo build for sima-worker");
        assert!(status.success(), "building sima-worker failed");
    });
}
