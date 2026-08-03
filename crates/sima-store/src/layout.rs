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

/// The suffix a pack file's name carries, which is what tells a pack from
/// the maintenance lock beside it.
pub(crate) const PACK_SUFFIX: &str = ".pack";

/// The maintenance lock's file name, inside `packs/`.
pub(crate) const MAINTENANCE_LOCK: &str = "maintenance.lock";

/// A pack's path: `packs/<64-hex>.pack`, named by the blake3 digest of the
/// whole file.
pub(crate) fn pack_path(root: &Path, name: &Hash) -> PathBuf {
    packs_dir(root).join(format!("{name}{PACK_SUFFIX}"))
}

/// The maintenance lock: `packs/maintenance.lock`.
pub(crate) fn maintenance_lock_path(root: &Path) -> PathBuf {
    packs_dir(root).join(MAINTENANCE_LOCK)
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

/// One fan-out subdirectory: `objects/<aa>/`, holding every object whose
/// address starts with those two hex characters.
pub(crate) fn fanout_dir(root: &Path, prefix: &str) -> PathBuf {
    objects_dir(root).join(prefix)
}

/// An object's CAS path: `objects/<aa>/<64-hex>`, fanned out by the first
/// two hex characters of its address.
pub(crate) fn object_path(root: &Path, hash: &Hash) -> PathBuf {
    let hex = hash.to_string();
    fanout_dir(root, &hex[..2]).join(hex)
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use sima_core::{Result, hash_bytes};

    use super::*;

    /// The root every pinned path below is derived against.
    fn root() -> &'static Path {
        Path::new("/store")
    }

    /// A digest whose hex form is fixed, for the fanned-out paths.
    fn digest() -> Hash {
        hash_bytes(b"a pinned object")
    }

    #[test]
    fn every_top_level_directory_sits_where_the_format_says() {
        // The store format is a contract with whatever else reads the
        // directory — an operator, a backup, a future version — so the names
        // are pinned here rather than only implied by the tests that happen to
        // walk them.
        assert_eq!(objects_dir(root()), Path::new("/store/objects"));
        assert_eq!(packs_dir(root()), Path::new("/store/packs"));
        assert_eq!(tasks_dir(root()), Path::new("/store/tasks"));
        assert_eq!(runs_dir(root()), Path::new("/store/runs"));
        assert_eq!(instances_dir(root()), Path::new("/store/instances"));
        assert_eq!(spend_ledger_dir(root()), Path::new("/store/spend"));
        assert_eq!(machines_ledger_dir(root()), Path::new("/store/machines"));
        assert_eq!(tmp_dir(root()), Path::new("/store/tmp"));
        assert_eq!(format_marker_path(root()), Path::new("/store/format"));
        assert_eq!(
            maintenance_lock_path(root()),
            Path::new("/store/packs/maintenance.lock")
        );
    }

    #[test]
    fn an_object_fans_out_by_the_first_two_characters_of_its_address() {
        // The fan-out is what keeps one directory from holding every object,
        // and it is derived rather than stored — so a change of width or of
        // which characters are taken would strand every object already written.
        let hash = digest();
        let hex = hash.to_string();
        assert_eq!(
            object_path(root(), &hash),
            Path::new("/store/objects").join(&hex[..2]).join(&hex)
        );
        assert_eq!(
            fanout_dir(root(), &hex[..2]),
            Path::new("/store/objects").join(&hex[..2])
        );
    }

    #[test]
    fn a_pack_and_a_task_are_named_by_their_own_identity() {
        let hash = digest();
        assert_eq!(
            pack_path(root(), &hash),
            Path::new("/store/packs").join(format!("{hash}.pack"))
        );
        let key = TaskKey::from_hash(hash);
        assert_eq!(
            task_path(root(), &key),
            Path::new("/store/tasks").join(key.to_string())
        );
    }

    #[test]
    fn a_run_holds_its_files_under_its_own_directory() -> Result<()> {
        // Everything a run owns is one subtree, which is what makes removing a
        // run a directory removal rather than a scan.
        let run = RunId::from_hash(hash_bytes(b"a pinned run"));
        let dir = Path::new("/store/runs").join(run.to_string());
        assert_eq!(run_dir(root(), &run), dir);
        assert_eq!(manifest_path(root(), &run), dir.join("manifest.json"));
        assert_eq!(journal_path(root(), &run), dir.join("journal"));
        assert_eq!(lock_path(root(), &run), dir.join("orchestrator.lock"));
        assert_eq!(remove_intent_path(root(), &run), dir.join("remove-intent"));
        assert_eq!(checkpoint_dir(root(), &run), dir.join("checkpoint"));
        assert_eq!(checkpoint_path(root(), &run, 7), dir.join("checkpoint/7"));
        assert_eq!(placement_dir(root(), &run), dir.join("placement"));
        assert_eq!(placement_path(root(), &run, 3), dir.join("placement/3"));
        Ok(())
    }

    #[test]
    fn a_ledger_entry_is_named_by_what_it_records() {
        assert_eq!(
            instance_path(root(), "sima-tag-1"),
            Path::new("/store/instances/sima-tag-1")
        );
        assert_eq!(spend_dir(root(), "abcd"), Path::new("/store/spend/abcd"));
        assert_eq!(
            spend_path(root(), "abcd", "sima-tag-1", 42),
            Path::new("/store/spend/abcd").join(crate::spend::key("sima-tag-1", 42))
        );
        assert_eq!(
            machine_dir(root(), "vastai", "machine-7"),
            Path::new("/store/machines").join(crate::machines::machine_key("vastai", "machine-7"))
        );
        assert_eq!(
            machine_incident_path(root(), "vastai", "machine-7", "sima-tag-1", 42),
            machine_dir(root(), "vastai", "machine-7")
                .join(crate::machines::incident_key("sima-tag-1", 42))
        );
    }

    #[test]
    fn a_temporary_file_names_the_writer_that_owns_it() {
        // Placement is write-to-tmp then rename, and two writers must not pick
        // one name: the pid and the sequence are what keep them apart.
        assert_eq!(tmp_file(root(), 91, 3), Path::new("/store/tmp/91-3"));
        assert_ne!(tmp_file(root(), 91, 3), tmp_file(root(), 92, 3));
        assert_ne!(tmp_file(root(), 91, 3), tmp_file(root(), 91, 4));
    }
}
