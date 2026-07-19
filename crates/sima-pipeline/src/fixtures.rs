//! Test fixtures shared by the crate's unit tests: the stub run every
//! synthetic journal is written under, and the store it lives in.

use std::path::PathBuf;
use std::time::Duration;

use sima_core::Result;
use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig};
use sima_scheduler::{ExecutionConfig, Record};
use sima_store::Store;

use crate::config::LoadedConfig;

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

/// A loaded config over `store` for the stub run.
pub(crate) fn loaded(store: PathBuf) -> Result<LoadedConfig> {
    Ok(LoadedConfig {
        run: stub_config()?,
        devices: Vec::new(),
        remotes: Vec::new(),
        execution: ExecutionConfig::new(1, 1, Duration::MAX, Duration::MAX, None)?,
        store,
    })
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
