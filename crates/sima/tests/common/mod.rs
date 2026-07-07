//! Shared fixtures for the sima CLI test suites: the config-file writer,
//! the spawn helper over the built binary, and manifest lookup through
//! the pipeline surface.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

use sima_pipeline::load;
use sima_store::{Manifest, Store};

/// Writes a `sima.toml` named `name` under `dir`: the given behaviors
/// list content and store path (resolved relative to `dir`), two workers,
/// three attempts.
pub fn write_config(dir: &Path, name: &str, behaviors: &str, store: &str) -> PathBuf {
    let text = format!(
        r#"
        [run]
        root_seed = 11
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = [{behaviors}]

        [execution]
        store = "{store}"
        workers = 2
        max_attempts = 3
    "#
    );
    let path = dir.join(name);
    std::fs::write(&path, text).expect("write config");
    path
}

/// A command over the built sima binary, its environment cleared of any
/// crashpoint arming so only an explicit test arms one.
pub fn sima_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sima"));
    command.env_remove("SIMA_CRASHPOINT");
    command
}

/// The manifest of the run `config_path` describes, from its store.
pub fn manifest_of(config_path: &Path) -> Option<Manifest> {
    let config = load(config_path).expect("load config");
    let store = Store::open(&config.store).expect("open store");
    store.manifest(&config.run.id()).expect("read manifest")
}
