//! The catalog: the task index and the run registry.
//!
//! Workers commit results here, and task sources derive the runnable
//! frontier from what is committed. The write-ordering discipline makes
//! every committed task's closure complete by construction: references
//! are verified durable, then the record object is written, then the
//! index entry — so an index entry proves everything beneath it exists.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::str;

use sima_core::{Codec, Error, Hash, Result, hash_bytes};
use sima_model::{RunConfig, RunId, TaskKey, TaskRecord};

use crate::atomic::{self, io_error};
use crate::layout;
use crate::manifest::{self, Manifest, ManifestEntry};
use crate::store::Store;

impl Store {
    /// Commits a task result: verifies every referenced object — all
    /// identity components and all artifacts — is already durable
    /// ([`Error::MissingObject`] otherwise, with nothing written), then
    /// puts the record's canonical bytes, then atomically writes the
    /// index entry. Recommitting an equal record is a no-op — the retry
    /// path; a conflicting record for the same key is
    /// [`Error::Corruption`] — one result per task key, ever.
    pub fn commit(&self, record: &TaskRecord) -> Result<Hash> {
        for referenced in referenced_objects(record) {
            if !self.has(&referenced)? {
                return Err(Error::MissingObject(referenced));
            }
        }
        self.write_record(record)
    }

    /// Writes a record a peer sent, for the receiving half of a sync.
    ///
    /// Only the identity components — spec, params, environment — must be
    /// durable; an absent input state and absent artifacts are accepted. A
    /// store that took a partial object set therefore holds records whose
    /// artifacts it does not have, which is a shape retention already produces:
    /// retention drops objects under disk pressure while the records stay. It
    /// costs that store the ability to answer `run_closure` or to serve those
    /// objects onward, and nothing else — a chain is located from the records
    /// alone.
    ///
    /// [`Store::commit`] keeps its rule that every referenced object must be
    /// durable, so a record a run produced is never written this way.
    pub fn replicate(&self, record: &TaskRecord) -> Result<Hash> {
        let identity = &record.identity;
        for component in [
            *identity.spec.as_hash(),
            *identity.params.as_hash(),
            *identity.environment.as_hash(),
        ] {
            if !self.has(&component)? {
                return Err(Error::MissingObject(component));
            }
        }
        self.write_record(record)
    }

    /// The write both commit paths share: the record's canonical bytes, then
    /// the index entry, atomically.
    fn write_record(&self, record: &TaskRecord) -> Result<Hash> {
        let key = record.identity.key();
        let bytes = record.to_bytes();
        let record_hash = hash_bytes(&bytes);
        // An existing entry decides the outcome before anything is
        // written: equal hash → the commit already happened.
        if let Some(existing) = self.index_entry(&key)? {
            return if existing == record_hash {
                Ok(record_hash)
            } else {
                Err(Error::Corruption(format!(
                    "task {key} is committed as record {existing}, refusing conflicting record {record_hash}"
                )))
            };
        }
        self.put(&bytes)?;
        // A death here leaves the record object durable but unindexed —
        // unreferenced content a resumed run rewrites identically.
        sima_core::crashpoint("commit.after-object");
        // The index-entry pre-check above can go stale: another writer may
        // commit this key in the gap between that read and this write.
        // write_exclusive is the authority that closes the gap — the hard link
        // fails if the entry now exists, and it then compares bytes, so a
        // conflicting record surfaces as Corruption instead of overwriting the
        // first result.
        let entry = format!("{record_hash}\n");
        atomic::write_exclusive(
            self.root(),
            &layout::task_path(self.root(), &key),
            entry.as_bytes(),
        )?;
        Ok(record_hash)
    }

    /// Reads the committed record for `key`: `None` when the task has no
    /// index entry. A malformed entry, a dangling record object, or a
    /// record whose identity key differs from the index path is
    /// [`Error::Corruption`].
    pub fn record(&self, key: &TaskKey) -> Result<Option<TaskRecord>> {
        let Some(record_hash) = self.index_entry(key)? else {
            return Ok(None);
        };
        let bytes = match self.get(&record_hash) {
            Err(Error::MissingObject(hash)) => {
                return Err(Error::Corruption(format!(
                    "index entry for task {key} dangles: record object {hash} is absent"
                )));
            }
            other => other?,
        };
        decode_record(&bytes, &record_hash, key).map(Some)
    }

    /// Reports whether a committed record answers `key`, without reading it:
    /// the index entry's existence is the answer, matching the write ordering
    /// (the entry is written last). A malformed entry is not distinguished
    /// here; it surfaces when a reader (finalize, [`Self::record`]) reads it.
    pub fn has_record(&self, key: &TaskKey) -> Result<bool> {
        let path = layout::task_path(self.root(), key);
        path.try_exists().map_err(|e| io_error(&path, e))
    }

    /// Enumerates the closed object set of a finalized run: the config
    /// object (equal to the run id), every record object, and per record
    /// its spec, params, environment, input state, and artifacts.
    /// Deduplicated and sorted, so the result is deterministic — the
    /// have/want basis of store sync and the unit of run portability.
    /// Record objects load through verified reads; leaf objects are
    /// existence-checked, and a hole is [`Error::MissingObject`].
    pub fn run_closure(&self, run: &RunId) -> Result<Vec<Hash>> {
        let Some(manifest) = self.manifest(run)? else {
            return Err(Error::Validation(format!(
                "cannot enumerate the closure of run {run}: the run is not finalized"
            )));
        };
        let mut objects = BTreeSet::from([*run.as_hash()]);
        for entry in &manifest.entries {
            objects.insert(entry.record);
            let record = decode_record(&self.get(&entry.record)?, &entry.record, &entry.task)?;
            for leaf in referenced_objects(&record) {
                if !self.has(&leaf)? {
                    return Err(Error::MissingObject(leaf));
                }
                objects.insert(leaf);
            }
        }
        Ok(objects.into_iter().collect())
    }

    /// Registers a run: puts the config's canonical bytes — the object
    /// address equals the run id by construction, both being blake3 of
    /// the same bytes — and creates `runs/<run-id>/`. Idempotent: an
    /// existing run directory is a reopen, the resume path.
    pub fn create_run(&self, config: &RunConfig) -> Result<RunId> {
        let run = RunId::from_hash(self.put(&config.to_bytes())?);
        atomic::create_dir_durable(&layout::run_dir(self.root(), &run))?;
        Ok(run)
    }

    /// Finalizing seals which tasks a run comprises. Before it a run is
    /// *open*: its task set is whatever has accrued in the index. Finalizing
    /// writes the manifest — the fixed, sorted `(task, record)` list — which
    /// marks the run answered and is what acceptance and [`Self::run_closure`]
    /// read from. After finalization the set does not change.
    ///
    /// Finalizes a run over exactly `keys`: every key must be committed
    /// ([`Error::Validation`] naming the first that is not), and the
    /// manifest is written atomically with entries sorted by task key, so
    /// its bytes are independent of the order workers completed in.
    /// Idempotent: re-finalizing with the same keys leaves the byte-equal
    /// file in place — the resume-through-finalization path; different
    /// keys are [`Error::Corruption`].
    pub fn finalize_run(&self, run: &RunId, keys: &[TaskKey]) -> Result<()> {
        if !layout::run_dir(self.root(), run).is_dir() {
            return Err(Error::Validation(format!(
                "cannot finalize run {run}: the run was never created"
            )));
        }
        let mut sorted = keys.to_vec();
        sorted.sort();
        if let Some(dup) = sorted.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(Error::Validation(format!(
                "duplicate task key {} in finalization",
                dup[0]
            )));
        }
        let mut entries = Vec::with_capacity(sorted.len());
        for key in &sorted {
            let Some(record) = self.index_entry(key)? else {
                return Err(Error::Validation(format!(
                    "cannot finalize run {run}: task {key} is not committed"
                )));
            };
            entries.push(ManifestEntry { task: *key, record });
        }
        let bytes = manifest::to_json_bytes(&Manifest { run: *run, entries });
        // A death here — every task committed, manifest assembled but not
        // yet written — leaves the run unfinalized; a resumed run re-derives
        // an empty frontier and finalizes to the identical bytes.
        sima_core::crashpoint("finalize.pre-write");
        let path = layout::manifest_path(self.root(), run);
        // The race window is between this read returning NotFound and the
        // write_exclusive below: two finalizers can both see no manifest and both
        // proceed. This read is not the guard — it only lets the common case
        // carry a message that names the run. The guard is write_exclusive's hard
        // link, which fails when the manifest already exists: the loser then
        // reads the existing manifest and compares — equal is an idempotent Ok,
        // different is Corruption.
        match fs::read(&path) {
            Ok(existing) if existing == bytes => Ok(()),
            Ok(_) => Err(Error::Corruption(format!(
                "run {run} is already finalized with a different manifest"
            ))),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                atomic::write_exclusive(self.root(), &path, &bytes)
            }
            Err(e) => Err(io_error(&path, e)),
        }
    }

    /// Reads a run's manifest: `None` while the run is unfinalized; a
    /// file that fails parsing or validation is [`Error::Corruption`].
    pub fn manifest(&self, run: &RunId) -> Result<Option<Manifest>> {
        let path = layout::manifest_path(self.root(), run);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error(&path, e)),
        };
        manifest::from_json_bytes(&bytes, run).map(Some)
    }

    /// Reads the index entry for `key`: the record hash it names, `None`
    /// when absent, [`Error::Corruption`] when the entry does not parse
    /// as record-hash hex + newline. Crate-visible so retention's index
    /// walk parses entries through the one reader.
    pub(crate) fn index_entry(&self, key: &TaskKey) -> Result<Option<Hash>> {
        let path = layout::task_path(self.root(), key);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error(&path, e)),
        };
        let malformed = || Error::Corruption(format!("index entry for task {key} is malformed"));
        let text = str::from_utf8(&bytes).map_err(|_| malformed())?;
        let hex = text.strip_suffix('\n').ok_or_else(malformed)?;
        let hash = Hash::from_hex(hex).map_err(|_| malformed())?;
        Ok(Some(hash))
    }
}

/// Decodes verified record-object bytes and requires the embedded
/// identity to answer for `key`; either failure means the store contradicts
/// itself, so both are [`Error::Corruption`].
fn decode_record(bytes: &[u8], record_hash: &Hash, key: &TaskKey) -> Result<TaskRecord> {
    let record = TaskRecord::from_bytes(bytes).map_err(|e| {
        Error::Corruption(format!(
            "record object {record_hash} for task {key} does not decode: {e}"
        ))
    })?;
    if record.identity.key() != *key {
        return Err(Error::Corruption(format!(
            "record under task {key} answers for task {}",
            record.identity.key()
        )));
    }
    Ok(record)
}

/// Every object a record references: the identity components (spec,
/// params, environment, input state when present) and the artifacts.
pub(crate) fn referenced_objects(record: &TaskRecord) -> impl Iterator<Item = Hash> + '_ {
    let identity = &record.identity;
    [
        *identity.spec.as_hash(),
        *identity.params.as_hash(),
        *identity.environment.as_hash(),
    ]
    .into_iter()
    .chain(identity.input_state)
    .chain(record.artifacts().iter().map(|a| *a.object()))
}

#[cfg(test)]
mod tests {
    use crate::testutil::{
        record_with_stored_artifact, sample_identity, store_identity_components, temp_store,
    };
    use sima_core::{Codec, Error, Result, hash_bytes};
    use sima_model::{SpecId, TaskIdentity, TaskRecord};
    use std::fs;

    #[test]
    fn commit_writes_the_record_hash_hex_entry() -> Result<()> {
        let (dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        let record_hash = store.commit(&record)?;
        assert_eq!(record_hash, hash_bytes(&record.to_bytes()));
        // The index entry is the record hash as lowercase hex + newline,
        // at tasks/<task-key-hex> — both pinned layout contract.
        let entry_path = dir
            .path()
            .join("tasks")
            .join(record.identity.key().to_string());
        let entry = fs::read_to_string(entry_path).expect("read index entry");
        assert_eq!(entry, format!("{record_hash}\n"));
        Ok(())
    }

    /// Asserts that committing `record` fails with `MissingObject` and
    /// writes nothing: no index entry, no record object.
    fn assert_commit_missing(store: &crate::Store, record: &TaskRecord) {
        assert!(matches!(store.commit(record), Err(Error::MissingObject(_))));
        let record_hash = hash_bytes(&record.to_bytes());
        assert!(!store.has(&record_hash).expect("has record object"));
        assert!(
            store
                .record(&record.identity.key())
                .expect("read index")
                .is_none()
        );
    }

    #[test]
    fn commit_with_a_missing_artifact_writes_nothing() -> Result<()> {
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        // The artifact object is referenced but never stored.
        let unstored = hash_bytes(b"unstored artifact");
        let artifact = sima_model::ArtifactRef::new("state-final", unstored)?;
        let record = TaskRecord::new(sample_identity(1), vec![artifact])?;
        assert_commit_missing(&store, &record);
        Ok(())
    }

    #[test]
    fn commit_with_a_missing_identity_component_writes_nothing() -> Result<()> {
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        // Each variant knocks out one identity component by pointing it at
        // bytes that were never stored.
        let base = sample_identity(1);
        let variants = [
            TaskIdentity {
                spec: SpecId::from_hash(hash_bytes(b"unstored spec")),
                ..base
            },
            TaskIdentity {
                params: sima_model::ParamsId::from_hash(hash_bytes(b"unstored params")),
                ..base
            },
            TaskIdentity {
                environment: sima_model::EnvironmentId::from_hash(hash_bytes(b"unstored env")),
                ..base
            },
            TaskIdentity {
                input_state: Some(hash_bytes(b"unstored input state")),
                ..base
            },
        ];
        for identity in variants {
            let record = record_with_stored_artifact(&store, identity);
            assert_commit_missing(&store, &record);
        }
        Ok(())
    }

    #[test]
    fn recommit_of_the_equal_record_is_a_no_op() -> Result<()> {
        let (dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        let first = store.commit(&record)?;
        let entry_path = dir
            .path()
            .join("tasks")
            .join(record.identity.key().to_string());
        let before = fs::read(&entry_path).expect("read entry");
        let second = store.commit(&record)?;
        assert_eq!(first, second);
        assert_eq!(fs::read(&entry_path).expect("read entry"), before);
        Ok(())
    }

    #[test]
    fn commit_of_a_conflicting_record_is_corruption() -> Result<()> {
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let identity = sample_identity(1);
        let record = record_with_stored_artifact(&store, identity);
        store.commit(&record)?;
        // Same identity, different artifact set: a second result for one
        // task key is a determinism violation.
        let other_object = store.put(b"a different artifact")?;
        let conflicting = TaskRecord::new(
            identity,
            vec![sima_model::ArtifactRef::new("state-final", other_object)?],
        )?;
        assert!(matches!(
            store.commit(&conflicting),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn record_round_trips_and_matches_its_key() -> Result<()> {
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        store.commit(&record)?;
        let key = record.identity.key();
        let read = store.record(&key)?.expect("committed record present");
        assert_eq!(read, record);
        assert_eq!(read.identity.key(), key);
        Ok(())
    }

    #[test]
    fn record_of_an_unknown_key_is_none() -> Result<()> {
        let (_dir, store) = temp_store();
        assert!(store.record(&sample_identity(9).key())?.is_none());
        Ok(())
    }

    #[test]
    fn has_record_is_false_before_commit_and_true_after() -> Result<()> {
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        let key = record.identity.key();
        assert!(!store.has_record(&key)?);
        store.commit(&record)?;
        assert!(store.has_record(&key)?);
        Ok(())
    }

    #[test]
    fn a_hand_corrupted_index_entry_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        store.commit(&record)?;
        let key = record.identity.key();
        let entry_path = dir.path().join("tasks").join(key.to_string());
        for garbage in ["not hex at all\n", "abc123\n", ""] {
            fs::write(&entry_path, garbage).expect("corrupt entry");
            assert!(matches!(store.record(&key), Err(Error::Corruption(_))));
        }
        Ok(())
    }

    #[test]
    fn an_entry_pointing_at_an_absent_object_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let key = sample_identity(1).key();
        let entry_path = dir.path().join("tasks").join(key.to_string());
        // A well-formed entry whose record object was never written: the
        // write ordering was violated.
        let dangling = hash_bytes(b"never stored record");
        fs::write(&entry_path, format!("{dangling}\n")).expect("write entry");
        assert!(matches!(store.record(&key), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn an_entry_whose_record_has_a_different_key_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        store.commit(&record)?;
        // Copy task 1's entry under task 2's key: the decoded record's
        // identity contradicts the index path it was found under.
        let entry_of =
            |identity: &TaskIdentity| dir.path().join("tasks").join(identity.key().to_string());
        fs::copy(entry_of(&record.identity), entry_of(&sample_identity(2))).expect("copy entry");
        assert!(matches!(
            store.record(&sample_identity(2).key()),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    use crate::testutil::sample_run_config;
    use sima_model::RunId;

    /// The manifest `finalize_run` must produce for `sample_run_config(42)`
    /// over the seed-1 and seed-2 sample tasks, hand-written. Every digest
    /// is derived independently with Python blake3 (pip package `blake3`)
    /// over the hand-assembled canonical bytes of the fixture objects:
    /// run id = blake3(config bytes) — the value pinned in `sima-model`'s
    /// run-config tests; task keys = blake3(identity bytes); records =
    /// blake3(record bytes). Key 42.. sorts before key 7a.., so the seed-1
    /// entry comes first regardless of finalization order.
    const PINNED_MANIFEST: &str = r#"{
  "run": "18ad1dd30bc36b634e749b10755626411a367ba066c579e3c299a3eda98d4c7b",
  "entries": [
    {
      "task": "420b9fda1a25806fa8be45d75081d6b26f6f7bbe2925d17d44c4c0aada6f2836",
      "record": "73ef3f6ef715958ffbce539fd45dc98ed8fff55e6c988c0d987fd1a7e8ea52d0"
    },
    {
      "task": "7a119187fbc5bc53c8deaa9a8ab5d524cdf97ec99fd110bbc61a98923d1c493e",
      "record": "b412b3e8bf02226de2ab75a8764463d47825c71246716d53749ecfa1fdeb0ee4"
    }
  ]
}
"#;

    /// Commits the seed-1 and seed-2 sample tasks and creates the sample
    /// run, returning the run id and the two task keys.
    fn committed_run(store: &crate::Store) -> Result<(RunId, Vec<sima_model::TaskKey>)> {
        store_identity_components(store);
        let mut keys = Vec::new();
        for seed in [1, 2] {
            let record = record_with_stored_artifact(store, sample_identity(seed));
            store.commit(&record)?;
            keys.push(record.identity.key());
        }
        let run = store.create_run(&sample_run_config(42))?;
        Ok((run, keys))
    }

    #[test]
    fn create_run_stores_the_config_at_the_run_id_address() -> Result<()> {
        let (dir, store) = temp_store();
        let config = sample_run_config(42);
        let run = store.create_run(&config)?;
        // RunId = config object address by construction: both are blake3
        // of the same canonical bytes.
        assert_eq!(run, config.id());
        assert_eq!(store.get(run.as_hash())?, config.to_bytes());
        assert!(dir.path().join("runs").join(run.to_string()).is_dir());
        Ok(())
    }

    #[test]
    fn create_run_twice_is_a_reopen() -> Result<()> {
        let (_dir, store) = temp_store();
        let config = sample_run_config(42);
        assert_eq!(store.create_run(&config)?, store.create_run(&config)?);
        Ok(())
    }

    #[test]
    fn finalize_writes_the_pinned_manifest_regardless_of_key_order() -> Result<()> {
        // Two fresh stores, permuted key order: byte-identical manifests,
        // both equal to the hand-written pin.
        for order in [[0, 1], [1, 0]] {
            let (dir, store) = temp_store();
            let (run, keys) = committed_run(&store)?;
            store.finalize_run(&run, &[keys[order[0]], keys[order[1]]])?;
            let path = dir
                .path()
                .join("runs")
                .join(run.to_string())
                .join("manifest.json");
            let written = fs::read_to_string(path).expect("read manifest");
            assert_eq!(written, PINNED_MANIFEST);
        }
        Ok(())
    }

    #[test]
    fn finalize_before_create_run_is_validation_error() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = sample_run_config(42).id();
        assert!(matches!(
            store.finalize_run(&run, &[]),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn finalize_with_an_uncommitted_key_is_validation_error_naming_it() -> Result<()> {
        let (_dir, store) = temp_store();
        let (run, mut keys) = committed_run(&store)?;
        let uncommitted = sample_identity(3).key();
        keys.push(uncommitted);
        match store.finalize_run(&run, &keys) {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains(&uncommitted.to_string()), "{msg}")
            }
            other => panic!("expected Validation, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn finalize_with_duplicate_keys_is_validation_error() -> Result<()> {
        let (_dir, store) = temp_store();
        let (run, keys) = committed_run(&store)?;
        assert!(matches!(
            store.finalize_run(&run, &[keys[0], keys[1], keys[0]]),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn refinalize_with_the_same_keys_is_a_no_op() -> Result<()> {
        let (dir, store) = temp_store();
        let (run, keys) = committed_run(&store)?;
        store.finalize_run(&run, &keys)?;
        let path = dir
            .path()
            .join("runs")
            .join(run.to_string())
            .join("manifest.json");
        let before = fs::read(&path).expect("read manifest");
        // Resume through finalization: the shuffled recall converges on
        // the same bytes and leaves the file untouched.
        store.finalize_run(&run, &[keys[1], keys[0]])?;
        assert_eq!(fs::read(&path).expect("read manifest"), before);
        Ok(())
    }

    #[test]
    fn refinalize_with_different_keys_is_corruption() -> Result<()> {
        let (_dir, store) = temp_store();
        let (run, keys) = committed_run(&store)?;
        store.finalize_run(&run, &keys)?;
        assert!(matches!(
            store.finalize_run(&run, &keys[..1]),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn manifest_round_trips_typed_data() -> Result<()> {
        let (_dir, store) = temp_store();
        let (run, keys) = committed_run(&store)?;
        store.finalize_run(&run, &keys)?;
        let manifest = store.manifest(&run)?.expect("finalized manifest");
        assert_eq!(manifest.run, run);
        let tasks: Vec<_> = manifest.entries.iter().map(|e| e.task).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(tasks, sorted);
        Ok(())
    }

    #[test]
    fn manifest_of_an_unfinalized_run_is_none() -> Result<()> {
        let (_dir, store) = temp_store();
        let (run, _keys) = committed_run(&store)?;
        assert!(store.manifest(&run)?.is_none());
        Ok(())
    }

    #[test]
    fn a_manifest_copied_into_another_run_directory_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let (run, keys) = committed_run(&store)?;
        store.finalize_run(&run, &keys)?;
        let other = store.create_run(&sample_run_config(43))?;
        let manifest_of = |run: &RunId| {
            dir.path()
                .join("runs")
                .join(run.to_string())
                .join("manifest.json")
        };
        fs::copy(manifest_of(&run), manifest_of(&other)).expect("copy manifest");
        assert!(matches!(store.manifest(&other), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn a_hand_corrupted_manifest_file_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let (run, keys) = committed_run(&store)?;
        store.finalize_run(&run, &keys)?;
        let path = dir
            .path()
            .join("runs")
            .join(run.to_string())
            .join("manifest.json");
        fs::write(&path, b"{ definitely not a manifest").expect("corrupt manifest");
        assert!(matches!(store.manifest(&run), Err(Error::Corruption(_))));
        Ok(())
    }

    use crate::testutil::{sample_environment, sample_params, sample_spec};

    #[test]
    fn closure_equals_the_hand_assembled_object_set() -> Result<()> {
        let (_dir, store) = temp_store();
        let (run, keys) = committed_run(&store)?;
        store.finalize_run(&run, &keys)?;
        // The closure, assembled by hand: the config object (= run id),
        // both record objects, the shared spec/params/environment (once
        // each), and both artifacts. Sorted, deduplicated.
        let mut expected = vec![
            *run.as_hash(),
            *sample_spec().id().as_hash(),
            *sample_params().id().as_hash(),
            *sample_environment().id().as_hash(),
        ];
        for seed in [1u64, 2] {
            let record = record_with_stored_artifact(&store, sample_identity(seed));
            expected.push(hash_bytes(&record.to_bytes()));
            expected.push(hash_bytes(&seed.to_le_bytes()));
        }
        expected.sort();
        expected.dedup();
        assert_eq!(store.run_closure(&run)?, expected);
        Ok(())
    }

    #[test]
    fn closure_includes_the_input_state_object() -> Result<()> {
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let input_state = store.put(b"state snapshot")?;
        let identity = TaskIdentity {
            input_state: Some(input_state),
            ..sample_identity(3)
        };
        let record = record_with_stored_artifact(&store, identity);
        store.commit(&record)?;
        let run = store.create_run(&sample_run_config(44))?;
        store.finalize_run(&run, &[identity.key()])?;
        assert!(store.run_closure(&run)?.contains(&input_state));
        Ok(())
    }

    #[test]
    fn closure_of_an_unfinalized_run_is_validation_error() -> Result<()> {
        let (_dir, store) = temp_store();
        let (run, _keys) = committed_run(&store)?;
        assert!(matches!(store.run_closure(&run), Err(Error::Validation(_))));
        Ok(())
    }

    #[test]
    fn closure_over_a_deleted_artifact_is_missing_object_naming_it() -> Result<()> {
        let (dir, store) = temp_store();
        let (run, keys) = committed_run(&store)?;
        store.finalize_run(&run, &keys)?;
        let artifact = hash_bytes(&1u64.to_le_bytes());
        let hex = artifact.to_string();
        fs::remove_file(dir.path().join("objects").join(&hex[..2]).join(&hex))
            .expect("delete artifact object");
        match store.run_closure(&run) {
            Err(Error::MissingObject(h)) => assert_eq!(h, artifact),
            other => panic!("expected MissingObject, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn closure_order_is_deterministic_across_calls() -> Result<()> {
        let (_dir, store) = temp_store();
        let (run, keys) = committed_run(&store)?;
        store.finalize_run(&run, &keys)?;
        assert_eq!(store.run_closure(&run)?, store.run_closure(&run)?);
        Ok(())
    }

    #[test]
    fn concurrent_commits_of_the_equal_record_all_succeed() -> Result<()> {
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        let store = &store;
        let record = &record;
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(move || store.commit(record)))
                .collect();
            for handle in handles {
                handle.join().expect("committer thread panicked")?;
            }
            Ok::<(), Error>(())
        })?;
        assert_eq!(store.record(&record.identity.key())?.as_ref(), Some(record));
        Ok(())
    }

    /// A record whose artifact object was never stored: what a sync's receiving
    /// half is handed under a named object scope.
    fn record_with_absent_artifact(seed: u64) -> TaskRecord {
        let object = hash_bytes(&seed.to_le_bytes());
        let artifact = sima_model::ArtifactRef::new("state", object).expect("artifact ref");
        TaskRecord::new(sample_identity(seed), vec![artifact]).expect("task record")
    }

    #[test]
    fn replicate_accepts_a_record_whose_artifacts_never_travelled() -> Result<()> {
        // The shape a named push produces: the record locates the chain, and
        // the bytes behind it were deliberately not sent.
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_absent_artifact(11);
        store.replicate(&record)?;
        let key = record.identity.key();
        assert_eq!(store.record(&key)?, Some(record));
        Ok(())
    }

    #[test]
    fn replicate_rejects_a_record_whose_identity_components_are_absent() {
        // Identity is what a record *is*; without it the record answers for a
        // task the store cannot name.
        let (_dir, store) = temp_store();
        let record = record_with_absent_artifact(12);
        assert!(matches!(
            store.replicate(&record),
            Err(Error::MissingObject(_))
        ));
    }

    #[test]
    fn commit_still_requires_every_referenced_object() {
        // The rule a run's own commits keep: a record this store produced
        // references bytes this store holds.
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_absent_artifact(13);
        assert!(matches!(
            store.commit(&record),
            Err(Error::MissingObject(_))
        ));
    }

    #[test]
    fn replicate_and_commit_write_the_same_record() -> Result<()> {
        // The two differ in what they require durable, never in what they
        // write, so a store that took a record by sync answers for it exactly
        // as the store that ran it does.
        let (_da, a) = temp_store();
        let (_db, b) = temp_store();
        for store in [&a, &b] {
            store_identity_components(store);
        }
        let record = record_with_stored_artifact(&a, sample_identity(14));
        b.put(&14u64.to_le_bytes())?;
        assert_eq!(a.commit(&record)?, b.replicate(&record)?);
        assert_eq!(
            a.record(&record.identity.key())?,
            b.record(&record.identity.key())?
        );
        Ok(())
    }
}
