//! Path authority: every store path is derived here, and only here.
//!
//! The layout is the store-format contract — a change mints a new layout,
//! never mutates this one:
//!
//! ```text
//! <root>/format                    store-format marker: the one line "1"
//! <root>/objects/<aa>/<64-hex>     object bytes; aa = first two hex chars
//! <root>/packs/<64-hex>.pack       immutable pack: many objects and an index
//! <root>/packs/maintenance.lock    serializes packing, gc, and pack rewrites
//! <root>/tmp/<pid>-<seq>           in-flight writes
//! <root>/tasks/<task-key-hex>      index entry: record-hash hex + newline
//! <root>/instances/<tag>           one rented instance's ledger record
//! <root>/spend/<owner-hex>/<tag>-<started-ms>   one closed rental's cost
//! <root>/machines/<provider>-<machine>/<tag>-<occurred-ms>   one incident
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

/// The `packs/` directory holding immutable pack files. Packs live outside
/// `objects/` because the retention walk reads every file under `objects/`
/// as one object.
pub(crate) fn packs_dir(root: &Path) -> PathBuf {
    root.join("packs")
}

/// A pack's path: `packs/<64-hex>.pack`, named by the blake3 digest of the
/// whole file.
pub(crate) fn pack_path(root: &Path, name: &Hash) -> PathBuf {
    packs_dir(root).join(format!("{name}.pack"))
}

/// The maintenance lock: `packs/maintenance.lock`.
pub(crate) fn maintenance_lock_path(root: &Path) -> PathBuf {
    packs_dir(root).join("maintenance.lock")
}

/// The store-format marker: `<root>/format`.
pub(crate) fn format_marker_path(root: &Path) -> PathBuf {
    root.join("format")
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

/// The `spend/` ledger directory.
pub(crate) fn spend_ledger_dir(root: &Path) -> PathBuf {
    root.join("spend")
}

/// The `machines/` reputation directory.
pub(crate) fn machines_ledger_dir(root: &Path) -> PathBuf {
    root.join("machines")
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

/// One owner's spend directory: `spend/<owner-hex>/`. The owner is
/// validated against its hex form before it reaches this function.
pub(crate) fn spend_dir(root: &Path, owner: &str) -> PathBuf {
    spend_ledger_dir(root).join(owner)
}

/// One closed rental's spend path: `spend/<owner-hex>/<tag>-<started-ms>`.
/// The tag is validated against its charset before it reaches this
/// function.
pub(crate) fn spend_path(root: &Path, owner: &str, tag: &str, started_ms: u64) -> PathBuf {
    spend_dir(root, owner).join(crate::spend::key(tag, started_ms))
}

/// One machine's incident directory: `machines/<provider>-<machine>/`. The
/// provider and machine are validated against their charset before they reach
/// this function.
pub(crate) fn machine_dir(root: &Path, provider: &str, machine: &str) -> PathBuf {
    machines_ledger_dir(root).join(crate::machines::machine_key(provider, machine))
}

/// One incident's path: `machines/<provider>-<machine>/<tag>-<occurred-ms>`.
/// The provider, machine, and tag are validated against their charset before
/// they reach this function.
pub(crate) fn machine_incident_path(
    root: &Path,
    provider: &str,
    machine: &str,
    tag: &str,
    occurred_ms: u64,
) -> PathBuf {
    machine_dir(root, provider, machine).join(crate::machines::incident_key(tag, occurred_ms))
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
