//! Reference-guarded deletion: [`Store::remove_search`] deletes one search and
//! everything no surviving search references; [`Store::gc`] deletes everything
//! outside the finalized searches' closures. Both are guarded the same way — no
//! object a live search references is ever removed — and both delete through
//! one primitive, so an object goes whichever representation holds it.
//!
//! The removal unit is the search. A single task's artifacts are never removed
//! alone, because the search's manifest would then reference missing objects. No
//! public single-object delete exists; the guard is the primitive.
//!
//! The plan is computed from the survivors, never from the target: the
//! removal set is every object the store holds — loose and packed alike —
//! outside the union of every other finalized search's
//! [`search_closure`](Store::search_closure), and every task-index entry
//! naming a record in that set. The target's own manifest is not consulted,
//! so an unfinalized search — interrupted or abandoned — is removable, and
//! orphaned objects from crashed pre-commit writes are collected in the same
//! sweep. Removal deletes references strictly before their referents —
//! task-index entries before the record objects they name, and the search
//! directory last — so at no point does a surviving reference point at a
//! missing object.
//!
//! Removal is durable and resumable. The plan is written to a `remove-intent`
//! file before any deletion; a crash leaves either a manifest that can still be
//! enumerated (before the intent) or a resumable intent (after it), and
//! re-running `remove_search` converges on the same end state. Deleting a file that
//! is already gone is success.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::str;

use sima_core::{Error, Hash, Result};
use sima_model::{SearchId, TaskKey};

use crate::atomic::{self, io_error};
use crate::layout;
use crate::store::Store;

/// The first line of a removal-intent file, identifying its format.
const INTENT_TAG: &str = "sima.remove-intent.v1";

/// What a [`Store::remove_search`] call deleted.
#[derive(Debug, PartialEq, Eq)]
pub struct RemovalReport {
    /// Objects deleted from the CAS.
    pub objects_removed: usize,
    /// Task-index entries deleted.
    pub index_entries_removed: usize,
}

/// What a [`Store::gc`] call deleted.
#[derive(Debug, PartialEq, Eq)]
pub struct GcReport {
    /// Objects deleted, loose and packed together.
    pub objects_removed: usize,
    /// Task-index entries deleted.
    pub index_entries_removed: usize,
    /// Packs whose contents changed: replaced by the pack of their
    /// survivors, or deleted outright when every object in them was doomed.
    pub packs_rewritten: usize,
    /// Unfinalized search directories deleted, whole.
    pub searches_removed: usize,
    /// Leftover `tmp/` files swept.
    pub tmp_files_removed: usize,
}

/// A search's removal plan: the objects to delete and the task-index entries to
/// drop, in deletion order (index entries before their record objects). It is
/// machine-read recovery state written to the intent file, never
/// identity-bearing.
struct RemovalPlan {
    objects: Vec<Hash>,
    tasks: Vec<TaskKey>,
}

impl Store {
    /// Every search registered in the store, sorted by id. A `searches/` entry whose
    /// name is not a search-id hex string is [`Error::Corruption`]. Retention and
    /// the CLI's search removal both enumerate searches this way.
    pub fn searches(&self) -> Result<Vec<SearchId>> {
        let dir = layout::searches_dir(self.root());
        let mut searches = Vec::new();
        for entry in fs::read_dir(&dir).map_err(|e| io_error(&dir, e))? {
            let entry = entry.map_err(|e| io_error(&dir, e))?;
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| {
                Error::Corruption(format!(
                    "searches/ holds a non-UTF-8 entry {:?}",
                    entry.file_name()
                ))
            })?;
            let search = SearchId::from_hex(name).map_err(|_| {
                Error::Corruption(format!("searches/ holds a non-search entry {name:?}"))
            })?;
            searches.push(search);
        }
        searches.sort();
        Ok(searches)
    }

    /// Removes a search and every object no other finalized search references.
    /// Returns what was removed.
    ///
    /// The target itself need not be finalized — the plan never consults its
    /// manifest, so an interrupted or abandoned search is removable. Every other
    /// search in the store must be finalized, since an unfinalized survivor's
    /// committed work is reachable through no manifest and the sweep would
    /// collect it; that violation is [`Error::Validation`] naming the search. A
    /// missing search directory is [`Error::Validation`] ("search not found").
    ///
    /// The plan is written to `searches/<search>/remove-intent` before any deletion,
    /// then index entries, then objects, then the search directory are removed. A
    /// crash at any point leaves the removal resumable: re-running converges on
    /// the same end state.
    pub fn remove_search(&self, search: &SearchId) -> Result<RemovalReport> {
        let search_dir = layout::search_dir(self.root(), search);
        if !search_dir.is_dir() {
            return Err(Error::Validation(format!(
                "cannot remove search {search}: search not found"
            )));
        }
        // Deleting an object reshapes the packs that hold it, so the
        // removal is serialized against every other maintenance operation
        // for its whole length — the plan included, which is then computed
        // over a store no packing is moving beneath it.
        let lock = self.acquire_maintenance_lock()?;
        // A present intent is an interrupted removal: resume it without
        // recomputing, so the plan is fixed across the crash. Otherwise compute
        // the plan under the preconditions and record it durably first.
        let plan = match self.read_remove_intent(search)? {
            Some(plan) => plan,
            None => {
                let plan = self.compute_removal(search)?;
                self.write_remove_intent(search, &plan)?;
                plan
            }
        };
        sima_core::crashpoint("remove.after-intent");
        // Delete references before referents: the task-index entries that name
        // the record objects come first.
        for task in &plan.tasks {
            atomic::remove_file_idempotent(&layout::task_path(self.root(), task))?;
        }
        self.delete_objects(&plan.objects, &lock)?;
        // The search directory last: manifest, journal, checkpoints, and the intent
        // itself. Empty object fan-out directories are left in place — removing
        // them would race concurrent puts.
        fs::remove_dir_all(&search_dir).map_err(|e| io_error(&search_dir, e))?;
        Ok(RemovalReport {
            objects_removed: plan.objects.len(),
            index_entries_removed: plan.tasks.len(),
        })
    }

    /// Deletes everything the finalized searches do not reference: objects in
    /// either representation, the task-index entries naming them, every
    /// unfinalized search directory, and the leftovers in `tmp/`.
    ///
    /// The store remembers finalized searches only afterwards. An unfinalized
    /// search's committed work is reachable through no manifest, so it is
    /// swept with its search directory, and its in-flight `tmp/` writes are
    /// swept from under it — an active search's work included. That is what
    /// asking for this operation means, and the operator owns the decision.
    pub fn gc(&self) -> Result<GcReport> {
        let lock = self.acquire_maintenance_lock()?;
        // The live set: the closure of every finalized search. A search without a
        // manifest enumerates nothing, so it is doomed rather than live.
        let mut live: BTreeSet<Hash> = BTreeSet::new();
        let mut unfinalized = Vec::new();
        for search in self.searches()? {
            match self.manifest(&search)? {
                Some(_) => live.extend(self.search_closure(&search)?),
                None => unfinalized.push(search),
            }
        }

        // Everything the store holds, minus what survives.
        let doomed: Vec<Hash> = self.held_objects()?.difference(&live).copied().collect();
        let tasks: Vec<TaskKey> = self
            .task_index()?
            .into_iter()
            .filter(|(_, record)| doomed.binary_search(record).is_ok())
            .map(|(key, _)| key)
            .collect();

        // References strictly before referents: the index entries naming a
        // doomed record go before the record objects themselves.
        for task in &tasks {
            atomic::remove_file_idempotent(&layout::task_path(self.root(), task))?;
        }
        let packs_rewritten = self.delete_objects(&doomed, &lock)?;
        for search in &unfinalized {
            let dir = layout::search_dir(self.root(), search);
            fs::remove_dir_all(&dir).map_err(|e| io_error(&dir, e))?;
        }
        Ok(GcReport {
            objects_removed: doomed.len(),
            index_entries_removed: tasks.len(),
            packs_rewritten,
            searches_removed: unfinalized.len(),
            tmp_files_removed: self.sweep_tmp()?,
        })
    }

    /// Deletes every file left in `tmp/` and returns how many went. They
    /// are the inert remains of writes that died before their rename, so
    /// nothing durable references them.
    fn sweep_tmp(&self) -> Result<usize> {
        let dir = layout::tmp_dir(self.root());
        let mut swept = 0;
        for entry in fs::read_dir(&dir).map_err(|e| io_error(&dir, e))? {
            let path = entry.map_err(|e| io_error(&dir, e))?.path();
            atomic::remove_file_idempotent(&path)?;
            swept += 1;
        }
        Ok(swept)
    }

    /// Computes the removal plan from the survivors: every CAS object outside
    /// the union of every other search's closure, and every task-index entry
    /// naming a record in that set. Every other search must be finalized; the
    /// target's manifest is never consulted.
    fn compute_removal(&self, search: &SearchId) -> Result<RemovalPlan> {
        let others: Vec<SearchId> = self
            .searches()?
            .into_iter()
            .filter(|r| r != search)
            .collect();
        for other in &others {
            if self.manifest(other)?.is_none() {
                return Err(Error::Validation(format!(
                    "cannot remove search {search}: search {other} is not finalized, so its objects are not enumerable"
                )));
            }
        }
        let mut kept: BTreeSet<Hash> = BTreeSet::new();
        for other in &others {
            kept.extend(self.search_closure(other)?);
        }
        // Both walks return sorted entries and the filters keep their order,
        // so the plan is deterministic.
        let objects: Vec<Hash> = self
            .held_objects()?
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

    /// Every object the store holds, in either representation: the loose
    /// files and everything the packs hold. Both reference-guarded
    /// deletions compute their plan against this, since which
    /// representation an object sits in says nothing about whether it is
    /// referenced.
    fn held_objects(&self) -> Result<BTreeSet<Hash>> {
        self.packs_mut().rescan(self.root())?;
        let mut held: BTreeSet<Hash> = self.cas_objects()?.into_iter().collect();
        held.extend(self.packs().objects());
        Ok(held)
    }

    /// Every loose object hash, sorted, from walking the fan-out
    /// directories. A file whose name is not an object-hash hex string is
    /// [`Error::Corruption`]. The packing operation partitions this walk,
    /// and [`Store::held_objects`] is its half of what the store holds.
    pub(crate) fn cas_objects(&self) -> Result<Vec<Hash>> {
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

    /// Writes the removal plan to the search's intent file through the store's
    /// atomic-write primitive.
    fn write_remove_intent(&self, search: &SearchId, plan: &RemovalPlan) -> Result<()> {
        let path = layout::remove_intent_path(self.root(), search);
        atomic::write_atomic(self.root(), &path, &intent_bytes(plan))
    }

    /// Reads the search's intent file, `None` when absent. A malformed intent is
    /// [`Error::Corruption`].
    fn read_remove_intent(&self, search: &SearchId) -> Result<Option<RemovalPlan>> {
        let path = layout::remove_intent_path(self.root(), search);
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::testutil::{
        pack_names, record_with_stored_artifact, sample_identity, sample_search_config,
        store_identity_components, temp_store,
    };
    use sima_core::hash_bytes;

    /// Commits `seeds` and finalizes a search over them under `root_seed`,
    /// returning its id. Committing a seed shared with another search is idempotent
    /// — the record is identical — so shared objects arise naturally.
    fn finalized_search(store: &Store, root_seed: u64, seeds: &[u64]) -> Result<SearchId> {
        store_identity_components(store);
        let mut keys = Vec::new();
        for &seed in seeds {
            let record = record_with_stored_artifact(store, sample_identity(seed));
            store.commit(&record)?;
            keys.push(record.identity.key());
        }
        let search = store.create_search(&sample_search_config(root_seed))?;
        store.finalize_search(&search, &keys)?;
        Ok(search)
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
        let a = finalized_search(&store, 42, &[1])?;
        let b = finalized_search(&store, 43, &[2])?;
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(store.searches()?, expected);
        Ok(())
    }

    #[test]
    fn runs_rejects_a_non_run_entry_as_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        finalized_search(&store, 42, &[1])?;
        fs::write(dir.path().join("searches").join("not-a-search-id"), b"")
            .expect("write stray entry");
        assert!(matches!(store.searches(), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn removing_a_run_keeps_objects_shared_with_another_finalized_search() -> Result<()> {
        let (dir, store) = temp_store();
        // Run A over seeds {1, 2}, search B over {2, 3}: seed 2's record, artifact,
        // and index entry are shared; each search's config and its own seed's
        // objects are exclusive.
        let a = finalized_search(&store, 42, &[1, 2])?;
        let b = finalized_search(&store, 43, &[2, 3])?;
        let b_closure = store.search_closure(&b)?;

        // A-exclusive: config(42), seed-1 record, seed-1 artifact — three
        // objects; and one index entry, tasks/<key 1>.
        let report = store.remove_search(&a)?;
        assert_eq!(
            report,
            RemovalReport {
                objects_removed: 3,
                index_entries_removed: 1,
            }
        );

        // B is untouched: its closure still enumerates whole.
        assert_eq!(store.search_closure(&b)?, b_closure);
        assert!(!dir.path().join("searches").join(a.to_string()).exists());

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
        let a = finalized_search(&store, 42, &[1])?;
        finalized_search(&store, 43, &[2])?;
        store.remove_search(&a)?;
        match store.remove_search(&a) {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains("search not found"), "{msg}")
            }
            other => panic!("expected Validation, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn removing_an_unfinalized_target_deletes_its_committed_work() -> Result<()> {
        // An interrupted or abandoned search: records committed, no manifest. The
        // plan comes from the surviving manifests alone, so the target's
        // missing manifest is no obstacle — its committed work, identity
        // components, and config are all swept.
        let (dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        store.commit(&record)?;
        let a = store.create_search(&sample_search_config(42))?;
        let report = store.remove_search(&a)?;
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
        assert!(!dir.path().join("searches").join(a.to_string()).exists());
        Ok(())
    }

    #[test]
    fn removing_an_unfinalized_target_keeps_objects_a_finalized_search_references() -> Result<()> {
        let (_dir, store) = temp_store();
        let b = finalized_search(&store, 43, &[2, 3])?;
        // The unfinalized target committed seeds {1, 2}: seed 2's objects and
        // index entry are shared with B, seed 1's and the config are its own.
        for seed in [1, 2] {
            let record = record_with_stored_artifact(&store, sample_identity(seed));
            store.commit(&record)?;
        }
        let a = store.create_search(&sample_search_config(42))?;
        let b_closure = store.search_closure(&b)?;

        // A-exclusive: config(42), seed-1 record, seed-1 artifact — three
        // objects and one index entry.
        let report = store.remove_search(&a)?;
        assert_eq!(
            report,
            RemovalReport {
                objects_removed: 3,
                index_entries_removed: 1,
            }
        );
        assert_eq!(store.search_closure(&b)?, b_closure);
        assert!(store.has_record(&sample_identity(2).key())?);
        assert!(!store.has_record(&sample_identity(1).key())?);
        Ok(())
    }

    #[test]
    fn removal_sweeps_objects_no_surviving_manifest_reaches() -> Result<()> {
        let (_dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1])?;
        finalized_search(&store, 43, &[2])?;
        // An orphan from a crashed pre-commit write: present in the CAS,
        // referenced by nothing. Removing any search collects it alongside the
        // search's own objects.
        let stray = store.put(b"orphaned bytes")?;
        let report = store.remove_search(&a)?;
        // Config(42), seed-1 record, seed-1 artifact, and the stray.
        assert_eq!(report.objects_removed, 4);
        assert!(!store.has(&stray)?);
        Ok(())
    }

    #[test]
    fn removing_with_another_run_unfinalized_is_validation() -> Result<()> {
        let (_dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1])?;
        // A second search committed but never finalized.
        let record = record_with_stored_artifact(&store, sample_identity(2));
        store.commit(&record)?;
        store.create_search(&sample_search_config(43))?;
        match store.remove_search(&a) {
            Err(Error::Validation(msg)) => assert!(msg.contains("not finalized"), "{msg}"),
            other => panic!("expected Validation, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn removing_the_only_run_empties_to_the_skeleton() -> Result<()> {
        let (dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1, 2])?;
        // Every object is exclusive: config, spec, params, environment, two
        // records, two artifacts — eight objects and two index entries.
        let report = store.remove_search(&a)?;
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
            fs::read_dir(dir.path().join("searches"))
                .expect("read searches")
                .count(),
            0,
            "no search directories remain"
        );
        Ok(())
    }

    #[test]
    fn an_interrupted_removal_resumes_from_its_intent() -> Result<()> {
        // A store with the target A and a reference store removed uninterrupted,
        // to compare the end state against.
        let (_ref_dir, ref_store) = temp_store();
        let ref_a = finalized_search(&ref_store, 42, &[1, 2])?;
        let reference = ref_store.remove_search(&ref_a)?;

        let (dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1, 2])?;
        // Reconstruct a mid-removal state by hand: the intent naming the full
        // plan is present, and one planned object plus one index entry are
        // already deleted, as a crash between the two deletion phases would
        // leave them.
        let plan = store.compute_removal(&a)?;
        store.write_remove_intent(&a, &plan)?;
        atomic::remove_file_idempotent(&layout::task_path(store.root(), &plan.tasks[0]))?;
        atomic::remove_file_idempotent(&layout::object_path(store.root(), &plan.objects[0]))?;

        // Resuming reads the intent, re-applies the deletions idempotently, and
        // converges on the reference end state.
        let report = store.remove_search(&a)?;
        assert_eq!(report, reference);
        assert_eq!(object_file_count(dir.path()), 0);
        assert!(!dir.path().join("searches").join(a.to_string()).exists());
        Ok(())
    }

    #[test]
    fn removing_a_run_rewrites_the_pack_holding_its_objects() -> Result<()> {
        let (dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1, 2])?;
        let b = finalized_search(&store, 43, &[2, 3])?;
        store.pack()?;
        let before = pack_names(dir.path());
        let b_closure = store.search_closure(&b)?;

        let report = store.remove_search(&a)?;
        assert_eq!(
            report,
            RemovalReport {
                objects_removed: 3,
                index_entries_removed: 1,
            }
        );
        // The pack that held both searches' objects is replaced by one holding
        // the survivors, so B's closure still enumerates whole.
        assert_ne!(pack_names(dir.path()), before);
        assert_eq!(store.search_closure(&b)?, b_closure);
        assert!(!store.has(a.as_hash())?);
        assert!(store.has(&hash_bytes(&2u64.to_le_bytes()))?);
        Ok(())
    }

    #[test]
    fn removing_the_only_run_leaves_neither_objects_nor_packs() -> Result<()> {
        let (dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1, 2])?;
        store.pack()?;
        store.remove_search(&a)?;
        assert_eq!(object_file_count(dir.path()), 0, "no loose objects remain");
        assert!(pack_names(dir.path()).is_empty(), "no packs remain");
        Ok(())
    }

    #[test]
    fn an_interrupted_removal_over_packs_resumes_from_its_intent() -> Result<()> {
        let (_ref_dir, ref_store) = temp_store();
        let ref_a = finalized_search(&ref_store, 42, &[1, 2])?;
        finalized_search(&ref_store, 43, &[2, 3])?;
        ref_store.pack()?;
        let reference = ref_store.remove_search(&ref_a)?;

        let (dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1, 2])?;
        let b = finalized_search(&store, 43, &[2, 3])?;
        store.pack()?;
        // A crash after the intent was written and after the replacement
        // pack was placed, before the doomed one was deleted: both packs
        // are on disk and every object is readable twice.
        let plan = store.compute_removal(&a)?;
        store.write_remove_intent(&a, &plan)?;
        let survivors: Vec<Hash> = store
            .search_closure(&b)?
            .into_iter()
            .filter(|hash| !plan.objects.contains(hash))
            .collect();
        crate::pack::format::write_pack(store.root(), &survivors, &|hash| store.get(hash))?;

        let report = store.remove_search(&a)?;
        assert_eq!(report, reference);
        assert_eq!(store.search_closure(&b)?, ref_store.search_closure(&b)?);
        assert!(!store.has(a.as_hash())?);
        assert_eq!(object_file_count(dir.path()), 0);
        Ok(())
    }

    #[test]
    fn gc_keeps_every_finalized_search_and_sweeps_the_rest() -> Result<()> {
        let (dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1])?;
        let closure = store.search_closure(&a)?;
        let stray = store.put(b"orphaned bytes")?;
        store.pack()?;
        // A second stray, left loose after the packing, so the sweep is
        // exercised in both representations at once.
        let loose_stray = store.put(b"loose orphaned bytes")?;

        let report = store.gc()?;
        assert_eq!(report.objects_removed, 2);
        assert_eq!(report.packs_rewritten, 1);
        assert_eq!(report.searches_removed, 0);
        assert!(!store.has(&stray)?);
        assert!(!store.has(&loose_stray)?);
        assert_eq!(store.search_closure(&a)?, closure);
        assert!(dir.path().join("searches").join(a.to_string()).is_dir());
        Ok(())
    }

    #[test]
    fn gc_deletes_a_pack_whose_every_object_is_doomed() -> Result<()> {
        let (dir, store) = temp_store();
        // No search at all: every object in the store is an orphan.
        let stray = store.put(b"orphaned bytes")?;
        store.pack()?;
        let report = store.gc()?;
        assert_eq!(report.objects_removed, 1);
        assert_eq!(report.packs_rewritten, 1);
        assert!(pack_names(dir.path()).is_empty());
        assert!(!store.has(&stray)?);
        Ok(())
    }

    #[test]
    fn gc_deletes_the_index_entries_of_the_records_it_removes() -> Result<()> {
        let (_dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1])?;
        // A record committed outside any finalized search: its object and the
        // index entry naming it both go.
        let orphan = record_with_stored_artifact(&store, sample_identity(9));
        store.commit(&orphan)?;
        let report = store.gc()?;
        assert_eq!(report.index_entries_removed, 1);
        assert!(!store.has_record(&sample_identity(9).key())?);
        assert!(store.has_record(&sample_identity(1).key())?);
        assert_eq!(store.searches()?, vec![a]);
        Ok(())
    }

    #[test]
    fn gc_deletes_unfinalized_searchs_whole() -> Result<()> {
        let (dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1])?;
        let closure = store.search_closure(&a)?;
        // An unfinalized search: a directory, a committed record, a config.
        let record = record_with_stored_artifact(&store, sample_identity(2));
        store.commit(&record)?;
        let b = store.create_search(&sample_search_config(43))?;

        let report = store.gc()?;
        assert_eq!(report.searches_removed, 1);
        assert!(!dir.path().join("searches").join(b.to_string()).exists());
        assert!(dir.path().join("searches").join(a.to_string()).is_dir());
        assert!(!store.has(b.as_hash())?, "its config object goes too");
        assert!(!store.has_record(&sample_identity(2).key())?);
        assert_eq!(store.search_closure(&a)?, closure);
        Ok(())
    }

    #[test]
    fn gc_sweeps_the_leftovers_of_crashed_writes() -> Result<()> {
        let (dir, store) = temp_store();
        finalized_search(&store, 42, &[1])?;
        fs::write(dir.path().join("tmp").join("1234-0"), b"a torn write").expect("write leftover");
        let report = store.gc()?;
        assert_eq!(report.tmp_files_removed, 1);
        assert_eq!(
            fs::read_dir(dir.path().join("tmp"))
                .expect("read tmp")
                .count(),
            0
        );
        Ok(())
    }

    #[test]
    fn gc_on_a_live_store_removes_nothing() -> Result<()> {
        let (_dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1, 2])?;
        store.pack()?;
        let closure = store.search_closure(&a)?;
        let report = store.gc()?;
        assert_eq!(
            report,
            GcReport {
                objects_removed: 0,
                index_entries_removed: 0,
                packs_rewritten: 0,
                searches_removed: 0,
                tmp_files_removed: 0,
            }
        );
        assert_eq!(store.search_closure(&a)?, closure);
        // And a second call says the same, over a store it just left.
        assert_eq!(store.gc()?.objects_removed, 0);
        Ok(())
    }

    #[test]
    fn an_interrupted_gc_converges_on_re_run() -> Result<()> {
        // A reference store, swept uninterrupted, to compare against.
        let (ref_dir, ref_store) = temp_store();
        let ref_a = finalized_search(&ref_store, 42, &[1])?;
        let orphan = record_with_stored_artifact(&ref_store, sample_identity(9));
        ref_store.commit(&orphan)?;
        ref_store.pack()?;
        ref_store.gc()?;

        let (dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1])?;
        let orphan = record_with_stored_artifact(&store, sample_identity(9));
        store.commit(&orphan)?;
        store.pack()?;
        // A death after the index entry went and before the pack was
        // rewritten: the sweep recomputes its plan from the survivors, so
        // the interrupted state is just a store with fewer references.
        atomic::remove_file_idempotent(&layout::task_path(
            store.root(),
            &sample_identity(9).key(),
        ))?;

        store.gc()?;
        assert_eq!(store.search_closure(&a)?, ref_store.search_closure(&ref_a)?);
        assert_eq!(pack_names(dir.path()), pack_names(ref_dir.path()));
        assert_eq!(object_file_count(dir.path()), 0);
        Ok(())
    }

    #[test]
    fn a_malformed_intent_is_corruption() -> Result<()> {
        let (_dir, store) = temp_store();
        let a = finalized_search(&store, 42, &[1])?;
        let path = layout::remove_intent_path(store.root(), &a);
        fs::write(&path, b"not a remove intent").expect("write bad intent");
        assert!(matches!(store.remove_search(&a), Err(Error::Corruption(_))));
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
