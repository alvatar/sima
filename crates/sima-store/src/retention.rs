//! Reference-guarded run removal: delete a run and everything no surviving
//! run references, guarded so no object another live run references is ever
//! removed.
//!
//! The removal unit is the run. A single task's artifacts are never removed
//! alone, because the run's manifest would then reference missing objects. No
//! public single-object delete exists; the guard is the primitive.
//!
//! The plan is computed from the survivors, never from the target: the
//! removal set is every CAS object outside the union of every other finalized
//! run's [`run_closure`](Store::run_closure), and every task-index entry
//! naming a record in that set. The target's own manifest is not consulted,
//! so an unfinalized run — interrupted or abandoned — is removable, and
//! orphaned objects from crashed pre-commit writes are collected in the same
//! sweep. Removal deletes references strictly before their referents —
//! task-index entries before the record objects they name, and the run
//! directory last — so at no point does a surviving reference point at a
//! missing object.
//!
//! Removal is durable and resumable. The plan is written to a `remove-intent`
//! file before any deletion; a crash leaves either a manifest that can still be
//! enumerated (before the intent) or a resumable intent (after it), and
//! re-running `remove_run` converges on the same end state. Deleting a file that
//! is already gone is success.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::str;

use sima_core::{Error, Hash, Result};
use sima_model::{RunId, TaskKey};

use crate::atomic::{self, io_error};
use crate::layout;
use crate::store::Store;

/// The first line of a removal-intent file, identifying its format.
const INTENT_TAG: &str = "sima.remove-intent.v1";

/// What a [`Store::remove_run`] call deleted.
#[derive(Debug, PartialEq, Eq)]
pub struct RemovalReport {
    /// Objects deleted from the CAS.
    pub objects_removed: usize,
    /// Task-index entries deleted.
    pub index_entries_removed: usize,
}

/// A run's removal plan: the objects to delete and the task-index entries to
/// drop, in deletion order (index entries before their record objects). It is
/// machine-read recovery state written to the intent file, never
/// identity-bearing.
struct RemovalPlan {
    objects: Vec<Hash>,
    tasks: Vec<TaskKey>,
}

impl Store {
    /// Every run registered in the store, sorted by id. A `runs/` entry whose
    /// name is not a run-id hex string is [`Error::Corruption`]. Retention and
    /// store sync both enumerate runs this way.
    pub fn runs(&self) -> Result<Vec<RunId>> {
        let dir = layout::runs_dir(self.root());
        let mut runs = Vec::new();
        for entry in fs::read_dir(&dir).map_err(|e| io_error(&dir, e))? {
            let entry = entry.map_err(|e| io_error(&dir, e))?;
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| {
                Error::Corruption(format!(
                    "runs/ holds a non-UTF-8 entry {:?}",
                    entry.file_name()
                ))
            })?;
            let run = RunId::from_hex(name)
                .map_err(|_| Error::Corruption(format!("runs/ holds a non-run entry {name:?}")))?;
            runs.push(run);
        }
        runs.sort();
        Ok(runs)
    }

    /// Removes a run and every object no other finalized run references.
    /// Returns what was removed.
    ///
    /// The target itself need not be finalized — the plan never consults its
    /// manifest, so an interrupted or abandoned run is removable. Every other
    /// run in the store must be finalized, since an unfinalized survivor's
    /// committed work is reachable through no manifest and the sweep would
    /// collect it; that violation is [`Error::Validation`] naming the run. A
    /// missing run directory is [`Error::Validation`] ("run not found").
    ///
    /// The plan is written to `runs/<run>/remove-intent` before any deletion,
    /// then index entries, then objects, then the run directory are removed. A
    /// crash at any point leaves the removal resumable: re-running converges on
    /// the same end state.
    pub fn remove_run(&self, run: &RunId) -> Result<RemovalReport> {
        let run_dir = layout::run_dir(self.root(), run);
        if !run_dir.is_dir() {
            return Err(Error::Validation(format!(
                "cannot remove run {run}: run not found"
            )));
        }
        // A present intent is an interrupted removal: resume it without
        // recomputing, so the plan is fixed across the crash. Otherwise compute
        // the plan under the preconditions and record it durably first.
        let plan = match self.read_remove_intent(run)? {
            Some(plan) => plan,
            None => {
                let plan = self.compute_removal(run)?;
                self.write_remove_intent(run, &plan)?;
                plan
            }
        };
        sima_core::crashpoint("remove.after-intent");
        // Delete references before referents: the task-index entries that name
        // the record objects come first.
        for task in &plan.tasks {
            remove_file_idempotent(&layout::task_path(self.root(), task))?;
        }
        for object in &plan.objects {
            remove_file_idempotent(&layout::object_path(self.root(), object))?;
            sima_core::crashpoint("remove.mid-objects");
        }
        // The run directory last: manifest, journal, checkpoints, and the intent
        // itself. Empty object fan-out directories are left in place — removing
        // them would race concurrent puts.
        fs::remove_dir_all(&run_dir).map_err(|e| io_error(&run_dir, e))?;
        Ok(RemovalReport {
            objects_removed: plan.objects.len(),
            index_entries_removed: plan.tasks.len(),
        })
    }

    /// Computes the removal plan from the survivors: every CAS object outside
    /// the union of every other run's closure, and every task-index entry
    /// naming a record in that set. Every other run must be finalized; the
    /// target's manifest is never consulted.
    fn compute_removal(&self, run: &RunId) -> Result<RemovalPlan> {
        let others: Vec<RunId> = self.runs()?.into_iter().filter(|r| r != run).collect();
        for other in &others {
            if self.manifest(other)?.is_none() {
                return Err(Error::Validation(format!(
                    "cannot remove run {run}: run {other} is not finalized, so its objects are not enumerable"
                )));
            }
        }
        let mut kept: BTreeSet<Hash> = BTreeSet::new();
        for other in &others {
            kept.extend(self.run_closure(other)?);
        }
        // Both walks return sorted entries and the filters keep their order,
        // so the plan is deterministic.
        let objects: Vec<Hash> = self
            .cas_objects()?
            .into_iter()
            .filter(|object| !kept.contains(object))
            .collect();
        let removed: BTreeSet<Hash> = objects.iter().copied().collect();
        let tasks: Vec<TaskKey> = self
            .task_index()?
            .into_iter()
            .filter(|(_, record)| removed.contains(record))
            .map(|(key, _)| key)
            .collect();
        Ok(RemovalPlan { objects, tasks })
    }

    /// Every object hash in the CAS, sorted, from walking the fan-out
    /// directories. A file whose name is not an object-hash hex string is
    /// [`Error::Corruption`].
    fn cas_objects(&self) -> Result<Vec<Hash>> {
        let dir = layout::objects_dir(self.root());
        let mut objects = Vec::new();
        for fanout in fs::read_dir(&dir).map_err(|e| io_error(&dir, e))? {
            let fanout = fanout.map_err(|e| io_error(&dir, e))?.path();
            for entry in fs::read_dir(&fanout).map_err(|e| io_error(&fanout, e))? {
                let name = entry.map_err(|e| io_error(&fanout, e))?.file_name();
                let hash = name
                    .to_str()
                    .and_then(|name| Hash::from_hex(name).ok())
                    .ok_or_else(|| {
                        Error::Corruption(format!("objects/ holds a non-object entry {name:?}"))
                    })?;
                objects.push(hash);
            }
        }
        objects.sort();
        Ok(objects)
    }

    /// Every task-index entry as (key, record hash), sorted by key, from
    /// walking `tasks/`. An entry whose name is not a task-key hex string is
    /// [`Error::Corruption`]; the content parses through the catalog's one
    /// entry reader.
    fn task_index(&self) -> Result<Vec<(TaskKey, Hash)>> {
        let dir = layout::tasks_dir(self.root());
        let mut entries = Vec::new();
        for entry in fs::read_dir(&dir).map_err(|e| io_error(&dir, e))? {
            let name = entry.map_err(|e| io_error(&dir, e))?.file_name();
            let key = name
                .to_str()
                .and_then(|name| TaskKey::from_hex(name).ok())
                .ok_or_else(|| {
                    Error::Corruption(format!("tasks/ holds a non-task entry {name:?}"))
                })?;
            if let Some(record) = self.index_entry(&key)? {
                entries.push((key, record));
            }
        }
        entries.sort();
        Ok(entries)
    }

    /// Writes the removal plan to the run's intent file through the store's
    /// atomic-write primitive.
    fn write_remove_intent(&self, run: &RunId, plan: &RemovalPlan) -> Result<()> {
        let path = layout::remove_intent_path(self.root(), run);
        atomic::write_atomic(self.root(), &path, &intent_bytes(plan))
    }

    /// Reads the run's intent file, `None` when absent. A malformed intent is
    /// [`Error::Corruption`].
    fn read_remove_intent(&self, run: &RunId) -> Result<Option<RemovalPlan>> {
        let path = layout::remove_intent_path(self.root(), run);
        match fs::read(&path) {
            Ok(bytes) => parse_intent(&bytes).map(Some),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_error(&path, e)),
        }
    }
}

/// Serializes a plan: the tag line, then one `object <hex>` line per object and
/// one `task <hex>` line per index entry.
fn intent_bytes(plan: &RemovalPlan) -> Vec<u8> {
    let mut text = String::new();
    text.push_str(INTENT_TAG);
    text.push('\n');
    for object in &plan.objects {
        text.push_str("object ");
        text.push_str(&object.to_string());
        text.push('\n');
    }
    for task in &plan.tasks {
        text.push_str("task ");
        text.push_str(&task.to_string());
        text.push('\n');
    }
    text.into_bytes()
}

/// Parses the bytes [`intent_bytes`] wrote back into a plan. A missing or wrong
/// tag, a non-UTF-8 body, or an unrecognized line is [`Error::Corruption`].
fn parse_intent(bytes: &[u8]) -> Result<RemovalPlan> {
    let malformed = || Error::Corruption("remove-intent is malformed".to_string());
    let text = str::from_utf8(bytes).map_err(|_| malformed())?;
    let mut lines = text.lines();
    if lines.next() != Some(INTENT_TAG) {
        return Err(malformed());
    }
    let mut objects = Vec::new();
    let mut tasks = Vec::new();
    for line in lines {
        if let Some(hex) = line.strip_prefix("object ") {
            objects.push(Hash::from_hex(hex).map_err(|_| malformed())?);
        } else if let Some(hex) = line.strip_prefix("task ") {
            tasks.push(TaskKey::from_hex(hex).map_err(|_| malformed())?);
        } else {
            return Err(malformed());
        }
    }
    Ok(RemovalPlan { objects, tasks })
}

/// Removes `path`, treating an already-absent file as success, so a resumed
/// removal converges.
fn remove_file_idempotent(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_error(path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{
        record_with_stored_artifact, sample_identity, sample_run_config, store_identity_components,
        temp_store,
    };
    use sima_core::hash_bytes;

    /// Commits `seeds` and finalizes a run over them under `root_seed`,
    /// returning its id. Committing a seed shared with another run is idempotent
    /// — the record is identical — so shared objects arise naturally.
    fn finalized_run(store: &Store, root_seed: u64, seeds: &[u64]) -> Result<RunId> {
        store_identity_components(store);
        let mut keys = Vec::new();
        for &seed in seeds {
            let record = record_with_stored_artifact(store, sample_identity(seed));
            store.commit_record(&record)?;
            keys.push(record.identity.key());
        }
        let run = store.create_run(&sample_run_config(root_seed))?;
        store.finalize_run(&run, &keys)?;
        Ok(run)
    }

    /// The number of object files under `objects/`, recursively — the fan-out
    /// directories may remain, but a fully removed store holds no object files.
    fn object_file_count(root: &Path) -> usize {
        let objects = root.join("objects");
        let mut count = 0;
        for fanout in fs::read_dir(&objects).expect("read objects dir") {
            let fanout = fanout.expect("fan-out entry");
            if fanout.path().is_dir() {
                count += fs::read_dir(fanout.path()).expect("read fan-out").count();
            }
        }
        count
    }

    #[test]
    fn runs_lists_every_registered_run_sorted() -> Result<()> {
        let (_dir, store) = temp_store();
        let a = finalized_run(&store, 42, &[1])?;
        let b = finalized_run(&store, 43, &[2])?;
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(store.runs()?, expected);
        Ok(())
    }

    #[test]
    fn runs_rejects_a_non_run_entry_as_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        finalized_run(&store, 42, &[1])?;
        fs::write(dir.path().join("runs").join("not-a-run-id"), b"").expect("write stray entry");
        assert!(matches!(store.runs(), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn removing_a_run_keeps_objects_shared_with_another_finalized_run() -> Result<()> {
        let (dir, store) = temp_store();
        // Run A over seeds {1, 2}, run B over {2, 3}: seed 2's record, artifact,
        // and index entry are shared; each run's config and its own seed's
        // objects are exclusive.
        let a = finalized_run(&store, 42, &[1, 2])?;
        let b = finalized_run(&store, 43, &[2, 3])?;
        let b_closure = store.run_closure(&b)?;

        // A-exclusive: config(42), seed-1 record, seed-1 artifact — three
        // objects; and one index entry, tasks/<key 1>.
        let report = store.remove_run(&a)?;
        assert_eq!(
            report,
            RemovalReport {
                objects_removed: 3,
                index_entries_removed: 1,
            }
        );

        // B is untouched: its closure still enumerates whole.
        assert_eq!(store.run_closure(&b)?, b_closure);
        assert!(!dir.path().join("runs").join(a.to_string()).exists());

        // The shared seed-2 objects and index entry survive.
        assert!(store.has_record(&sample_identity(2).key())?);
        assert!(store.has(&hash_bytes(&2u64.to_le_bytes()))?);
        // A's exclusive objects and index entry are gone.
        assert!(!store.has(a.as_hash())?, "A's config object is removed");
        assert!(!store.has(&hash_bytes(&1u64.to_le_bytes()))?);
        assert!(!store.has_record(&sample_identity(1).key())?);
        Ok(())
    }

    #[test]
    fn a_second_removal_is_run_not_found() -> Result<()> {
        let (_dir, store) = temp_store();
        let a = finalized_run(&store, 42, &[1])?;
        finalized_run(&store, 43, &[2])?;
        store.remove_run(&a)?;
        match store.remove_run(&a) {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains("run not found"), "{msg}")
            }
            other => panic!("expected Validation, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn removing_an_unfinalized_target_deletes_its_committed_work() -> Result<()> {
        // An interrupted or abandoned run: records committed, no manifest. The
        // plan comes from the surviving manifests alone, so the target's
        // missing manifest is no obstacle — its committed work, identity
        // components, and config are all swept.
        let (dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        store.commit_record(&record)?;
        let a = store.create_run(&sample_run_config(42))?;
        let report = store.remove_run(&a)?;
        // Spec, params, environment, record, artifact, config — six objects
        // and the one index entry.
        assert_eq!(
            report,
            RemovalReport {
                objects_removed: 6,
                index_entries_removed: 1,
            }
        );
        assert_eq!(object_file_count(dir.path()), 0);
        assert!(!dir.path().join("runs").join(a.to_string()).exists());
        Ok(())
    }

    #[test]
    fn removing_an_unfinalized_target_keeps_objects_a_finalized_run_references() -> Result<()> {
        let (_dir, store) = temp_store();
        let b = finalized_run(&store, 43, &[2, 3])?;
        // The unfinalized target committed seeds {1, 2}: seed 2's objects and
        // index entry are shared with B, seed 1's and the config are its own.
        for seed in [1, 2] {
            let record = record_with_stored_artifact(&store, sample_identity(seed));
            store.commit_record(&record)?;
        }
        let a = store.create_run(&sample_run_config(42))?;
        let b_closure = store.run_closure(&b)?;

        // A-exclusive: config(42), seed-1 record, seed-1 artifact — three
        // objects and one index entry.
        let report = store.remove_run(&a)?;
        assert_eq!(
            report,
            RemovalReport {
                objects_removed: 3,
                index_entries_removed: 1,
            }
        );
        assert_eq!(store.run_closure(&b)?, b_closure);
        assert!(store.has_record(&sample_identity(2).key())?);
        assert!(!store.has_record(&sample_identity(1).key())?);
        Ok(())
    }

    #[test]
    fn removal_sweeps_objects_no_surviving_manifest_reaches() -> Result<()> {
        let (_dir, store) = temp_store();
        let a = finalized_run(&store, 42, &[1])?;
        finalized_run(&store, 43, &[2])?;
        // An orphan from a crashed pre-commit write: present in the CAS,
        // referenced by nothing. Removing any run collects it alongside the
        // run's own objects.
        let stray = store.put(b"orphaned bytes")?;
        let report = store.remove_run(&a)?;
        // Config(42), seed-1 record, seed-1 artifact, and the stray.
        assert_eq!(report.objects_removed, 4);
        assert!(!store.has(&stray)?);
        Ok(())
    }

    #[test]
    fn removing_with_another_run_unfinalized_is_validation() -> Result<()> {
        let (_dir, store) = temp_store();
        let a = finalized_run(&store, 42, &[1])?;
        // A second run committed but never finalized.
        let record = record_with_stored_artifact(&store, sample_identity(2));
        store.commit_record(&record)?;
        store.create_run(&sample_run_config(43))?;
        match store.remove_run(&a) {
            Err(Error::Validation(msg)) => assert!(msg.contains("not finalized"), "{msg}"),
            other => panic!("expected Validation, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn removing_the_only_run_empties_to_the_skeleton() -> Result<()> {
        let (dir, store) = temp_store();
        let a = finalized_run(&store, 42, &[1, 2])?;
        // Every object is exclusive: config, spec, params, environment, two
        // records, two artifacts — eight objects and two index entries.
        let report = store.remove_run(&a)?;
        assert_eq!(
            report,
            RemovalReport {
                objects_removed: 8,
                index_entries_removed: 2,
            }
        );
        assert_eq!(object_file_count(dir.path()), 0, "no object files remain");
        assert_eq!(
            fs::read_dir(dir.path().join("tasks"))
                .expect("read tasks")
                .count(),
            0,
            "the task index is empty"
        );
        assert_eq!(
            fs::read_dir(dir.path().join("runs"))
                .expect("read runs")
                .count(),
            0,
            "no run directories remain"
        );
        Ok(())
    }

    #[test]
    fn an_interrupted_removal_resumes_from_its_intent() -> Result<()> {
        // A store with the target A and a reference store removed uninterrupted,
        // to compare the end state against.
        let (_ref_dir, ref_store) = temp_store();
        let ref_a = finalized_run(&ref_store, 42, &[1, 2])?;
        let reference = ref_store.remove_run(&ref_a)?;

        let (dir, store) = temp_store();
        let a = finalized_run(&store, 42, &[1, 2])?;
        // Reconstruct a mid-removal state by hand: the intent naming the full
        // plan is present, and one planned object plus one index entry are
        // already deleted, as a crash between the two deletion phases would
        // leave them.
        let plan = store.compute_removal(&a)?;
        store.write_remove_intent(&a, &plan)?;
        remove_file_idempotent(&layout::task_path(store.root(), &plan.tasks[0]))?;
        remove_file_idempotent(&layout::object_path(store.root(), &plan.objects[0]))?;

        // Resuming reads the intent, re-applies the deletions idempotently, and
        // converges on the reference end state.
        let report = store.remove_run(&a)?;
        assert_eq!(report, reference);
        assert_eq!(object_file_count(dir.path()), 0);
        assert!(!dir.path().join("runs").join(a.to_string()).exists());
        Ok(())
    }

    #[test]
    fn a_malformed_intent_is_corruption() -> Result<()> {
        let (_dir, store) = temp_store();
        let a = finalized_run(&store, 42, &[1])?;
        let path = layout::remove_intent_path(store.root(), &a);
        fs::write(&path, b"not a remove intent").expect("write bad intent");
        assert!(matches!(store.remove_run(&a), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn the_intent_round_trips_through_its_bytes() -> Result<()> {
        let plan = RemovalPlan {
            objects: vec![hash_bytes(b"one"), hash_bytes(b"two")],
            tasks: vec![TaskKey::from_hash(hash_bytes(b"task"))],
        };
        let parsed = parse_intent(&intent_bytes(&plan))?;
        assert_eq!(parsed.objects, plan.objects);
        assert_eq!(parsed.tasks, plan.tasks);
        Ok(())
    }
}
