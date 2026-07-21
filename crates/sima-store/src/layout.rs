//! Path authority: every store path is derived here, and only here.
//!
//! The layout is the store-format contract — a change mints a new layout,
//! never mutates this one:
//!
//! ```text
//! <root>/objects/<aa>/<64-hex>     object bytes; aa = first two hex chars
//! <root>/tmp/<pid>-<seq>           in-flight writes
//! <root>/tasks/<task-key-hex>      index entry: record-hash hex + newline
//! <root>/instances/<tag>           one rented instance's ledger record
//! <root>/runs/<run-id-hex>/manifest.json
//! <root>/runs/<run-id-hex>/journal
//! <root>/runs/<run-id-hex>/orchestrator.lock
//! <root>/runs/<run-id-hex>/checkpoint/<slot>   mutable resume scratch
//! <root>/runs/<run-id-hex>/placement/<chain>   mutable chain device binding
//! <root>/runs/<run-id-hex>/remove-intent       resumable removal plan
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

/// An in-flight write's path: `tmp/<pid>-<seq>`.
pub(crate) fn tmp_file(root: &Path, pid: u32, seq: u64) -> PathBuf {
    tmp_dir(root).join(format!("{pid}-{seq}"))
}

/// The `tasks/` index directory.
pub(crate) fn tasks_dir(root: &Path) -> PathBuf {
    root.join("tasks")
}

/// The `instances/` ledger directory.
pub(crate) fn instances_dir(root: &Path) -> PathBuf {
    root.join("instances")
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

/// One acquisition attempt's ledger path: `instances/<tag>`. The tag is
/// validated against its charset before it reaches this function.
pub(crate) fn instance_path(root: &Path, tag: &str) -> PathBuf {
    instances_dir(root).join(tag)
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

/// A run's orchestrator-lock path: `runs/<run-id-hex>/orchestrator.lock`.
pub(crate) fn lock_path(root: &Path, run: &RunId) -> PathBuf {
    run_dir(root, run).join("orchestrator.lock")
}

/// A run's checkpoint directory: `runs/<run-id-hex>/checkpoint/`.
pub(crate) fn checkpoint_dir(root: &Path, run: &RunId) -> PathBuf {
    run_dir(root, run).join("checkpoint")
}

/// A chain's checkpoint-slot path: `runs/<run-id-hex>/checkpoint/<slot>`.
pub(crate) fn checkpoint_path(root: &Path, run: &RunId, slot: u64) -> PathBuf {
    checkpoint_dir(root, run).join(slot.to_string())
}

/// A run's placement directory: `runs/<run-id-hex>/placement/`.
pub(crate) fn placement_dir(root: &Path, run: &RunId) -> PathBuf {
    run_dir(root, run).join("placement")
}

/// A chain's placement-slot path: `runs/<run-id-hex>/placement/<chain>`.
pub(crate) fn placement_path(root: &Path, run: &RunId, chain: u64) -> PathBuf {
    placement_dir(root, run).join(chain.to_string())
}

/// A run's removal-intent path: `runs/<run-id-hex>/remove-intent`.
pub(crate) fn remove_intent_path(root: &Path, run: &RunId) -> PathBuf {
    run_dir(root, run).join("remove-intent")
}
