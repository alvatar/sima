//! Test fixtures shared by the crate's unit tests: the stub run every
//! synthetic journal is written under, and the store it lives in.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use sima_core::Result;
use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig};
use sima_provider::Budget;
use sima_scheduler::{ExecutionConfig, Record};
use sima_store::Store;

use crate::config::{Fleet, LoadedConfig, Orchestrator, Pool};

/// A minimal stub run config; its id addresses the test's run.
pub(crate) fn stub_config() -> Result<RunConfig> {
    Ok(RunConfig {
        root_seed: 1,
        segments: None,
        format: FormatId::new("stub.v1")?,
        generator: GeneratorConfig {
            id: GeneratorId::new("stub.v1")?,
            params: Vec::new(),
        },
        params: Params { bytes: Vec::new() },
    })
}

/// A loaded config over `store` for the stub run: one orchestrator worker and
/// no other machine.
pub(crate) fn loaded(store: PathBuf) -> Result<LoadedConfig> {
    Ok(LoadedConfig {
        run: stub_config()?,
        execution: ExecutionConfig::new(1, 1, Duration::MAX, Duration::MAX, None)?,
        orchestrator: Orchestrator {
            migrate: None,
            container: None,
            pool: Some(Pool::Workers(1)),
        },
        hosts: BTreeMap::new(),
        host_classes: BTreeMap::new(),
        fleet: Fleet::default(),
        budget: Budget::default(),
        store,
    })
}

/// Loads `text` as a config file in a fresh temporary directory, for the unit
/// tests that exercise the loaded shape rather than the file's location. The
/// directory is removed at once: nothing here opens the store the config names.
pub(crate) fn load_str(text: &str) -> LoadedConfig {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("sima.toml");
    std::fs::write(&path, text).expect("write the config file");
    crate::config::load(&path).expect("the config loads")
}

/// The config text a served run is written from: a stub run over a store
/// beside the config file, as a far-side host would hold it.
const SERVED_CONFIG: &str = r#"
    [run]
    root_seed = 7
    format = "stub.v1"

    [run.generator]
    id = "stub.v1"
    behaviors = ["succeed", "succeed"]

    [config]
    store = "./store"
    max_attempts = 3

    [orchestrator]
    workers = 2
"#;

/// Writes a config file under `dir` and returns its path, without touching
/// the store it names — the state of a host where no run was ever driven.
pub(crate) fn served_config(dir: &std::path::Path) -> PathBuf {
    let path = dir.join("sima.toml");
    std::fs::write(&path, SERVED_CONFIG).expect("write the config file");
    path
}

/// Writes a config file under `dir`, creates its run in the store the config
/// names, and journals `records`: the far-side state a follow stream reads.
/// Returns the config path and the config it loads to.
pub(crate) fn served_run(
    dir: &std::path::Path,
    records: &[Record],
) -> Result<(PathBuf, LoadedConfig)> {
    let path = served_config(dir);
    let loaded = crate::config::load(&path)?;
    let store = Store::open(&loaded.store)?;
    store.create_run(&loaded.run)?;
    let mut writer = store.journal_writer(&loaded.run.id())?;
    for record in records {
        writer.append(&record.to_line()?)?;
    }
    Ok((path, loaded))
}

/// Writes `records` to the stub run's journal in a fresh store, returning the
/// temp dir (kept alive by the caller) and the loaded config over it.
pub(crate) fn journal_with(records: &[Record]) -> Result<(tempfile::TempDir, LoadedConfig)> {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path())?;
    let config = stub_config()?;
    store.create_run(&config)?;
    let mut writer = store.journal_writer(&config.id())?;
    for record in records {
        writer.append(&record.to_line()?)?;
    }
    let config = loaded(dir.path().to_path_buf())?;
    Ok((dir, config))
}
