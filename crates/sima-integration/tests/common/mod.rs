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
    BUILD.call_once(|| build_binary("sima-worker"));
}

/// Builds `package`'s binary and returns its path, for the suites that drive a
/// run through a program of its own.
#[allow(dead_code)]
pub fn built_binary(package: &str) -> PathBuf {
    build_binary(package);
    // Beside the test executable's directory: `target/<profile>/deps` holds the
    // test binary and `target/<profile>` the built program.
    let exe = std::env::current_exe().expect("the test executable's path");
    let binary = exe
        .parent()
        .and_then(Path::parent)
        .expect("target/<profile> above the test executable")
        .join(package);
    assert!(binary.is_file(), "{} is built", binary.display());
    binary
}

/// Asks cargo for `package`'s binary.
fn build_binary(package: &str) {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let status = std::process::Command::new(cargo)
        .args(["build", "-p", package])
        .status()
        .unwrap_or_else(|e| panic!("run cargo build for {package}: {e}"));
    assert!(status.success(), "building {package} failed");
}
