//! Shared fixtures for the real-domain end-to-end suites.

use std::path::{Path, PathBuf};

use sima_core::Result;
use sima_pipeline::{LoadedConfig, load};

/// Writes `text` as a config file named `name` under `dir` and loads it.
pub fn loaded_text(dir: &Path, name: &str, text: &str) -> Result<LoadedConfig> {
    let path: PathBuf = dir.join(name);
    std::fs::write(&path, text).expect("write config");
    load(&path)
}
