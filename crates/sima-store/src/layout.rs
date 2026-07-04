//! Path authority: every store path is derived here, and only here.
//!
//! The layout is the store-format contract — a change mints a new layout,
//! never mutates this one:
//!
//! ```text
//! <root>/objects/<aa>/<64-hex>     object bytes; aa = first two hex chars
//! <root>/tmp/<pid>-<seq>           in-flight writes
//! <root>/tasks/<task-key-hex>      index entry: record-hash hex + newline
//! <root>/runs/<run-id-hex>/manifest.json
//! <root>/runs/<run-id-hex>/journal
//! ```

use std::path::{Path, PathBuf};

use sima_core::Hash;
use sima_model::{RunId, TaskKey};

/// The `objects/` CAS directory.
pub(crate) fn objects_dir(root: &Path) -> PathBuf {
    root.join("objects")
}

/// The `tmp/` directory holding in-flight atomic writes.
pub(crate) fn tmp_dir(root: &Path) -> PathBuf {
    root.join("tmp")
}

/// The `tasks/` index directory.
pub(crate) fn tasks_dir(root: &Path) -> PathBuf {
    root.join("tasks")
}

/// The `runs/` directory.
pub(crate) fn runs_dir(root: &Path) -> PathBuf {
    root.join("runs")
}

/// An object's CAS path: `objects/<aa>/<64-hex>`, fanned out by the first
/// two hex characters of its address.
pub(crate) fn object_path(root: &Path, hash: &Hash) -> PathBuf {
    let hex = hash.to_string();
    objects_dir(root).join(&hex[..2]).join(hex)
}

/// A task's index-entry path: `tasks/<task-key-hex>`.
pub(crate) fn task_path(root: &Path, key: &TaskKey) -> PathBuf {
    tasks_dir(root).join(key.to_string())
}

/// A run's directory: `runs/<run-id-hex>/`.
pub(crate) fn run_dir(root: &Path, run: &RunId) -> PathBuf {
    runs_dir(root).join(run.to_string())
}

/// A run's manifest path: `runs/<run-id-hex>/manifest.json`.
pub(crate) fn manifest_path(root: &Path, run: &RunId) -> PathBuf {
    run_dir(root, run).join("manifest.json")
}

/// A run's journal path: `runs/<run-id-hex>/journal`.
pub(crate) fn journal_path(root: &Path, run: &RunId) -> PathBuf {
    run_dir(root, run).join("journal")
}
