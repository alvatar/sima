//! The catalog: the task index and the run registry.
//!
//! Workers commit results here, and task sources derive the runnable
//! frontier from what is committed. The write-ordering discipline makes
//! every committed task's closure complete by construction: references
//! are verified durable, then the record object is written, then the
//! index entry — so an index entry proves everything beneath it exists.

use std::fs;
use std::io::ErrorKind;
use std::str;

use sima_core::{Error, Hash, Result, hash_bytes};
use sima_model::{TaskKey, TaskRecord};

use crate::atomic::{self, io_error};
use crate::layout;
use crate::store::Store;

impl Store {
    /// Commits a task result: verifies every referenced object — all
    /// identity components and all artifacts — is already durable
    /// ([`Error::MissingObject`] otherwise, with nothing written), then
    /// puts the record's canonical bytes, then atomically writes the
    /// index entry. Recommitting an equal record is a no-op — the retry
    /// path; a conflicting record for the same key is
    /// [`Error::Corruption`] — one result per task key, ever.
    pub fn commit_record(&self, record: &TaskRecord) -> Result<Hash> {
        for referenced in referenced_objects(record) {
            if !self.has(&referenced)? {
                return Err(Error::MissingObject(referenced));
            }
        }
        let key = record.identity.key();
        let record_hash = hash_bytes(&record.to_bytes());
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
        self.put(&record.to_bytes())?;
        let entry = format!("{record_hash}\n");
        atomic::write_atomic(
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
        let record = TaskRecord::from_bytes(&bytes).map_err(|e| {
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
        Ok(Some(record))
    }

    /// Reads the index entry for `key`: the record hash it names, `None`
    /// when absent, [`Error::Corruption`] when the entry does not parse
    /// as record-hash hex + newline.
    fn index_entry(&self, key: &TaskKey) -> Result<Option<Hash>> {
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

/// Every object a record references: the identity components (spec,
/// params, environment, input state when present) and the artifacts.
fn referenced_objects(record: &TaskRecord) -> impl Iterator<Item = Hash> + '_ {
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
    use sima_core::{Error, Result, hash_bytes};
    use sima_model::{SpecId, TaskIdentity, TaskRecord};
    use std::fs;

    #[test]
    fn commit_writes_the_record_hash_hex_entry() -> Result<()> {
        let (dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        let record_hash = store.commit_record(&record)?;
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
        assert!(matches!(
            store.commit_record(record),
            Err(Error::MissingObject(_))
        ));
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
        let first = store.commit_record(&record)?;
        let entry_path = dir
            .path()
            .join("tasks")
            .join(record.identity.key().to_string());
        let before = fs::read(&entry_path).expect("read entry");
        let second = store.commit_record(&record)?;
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
        store.commit_record(&record)?;
        // Same identity, different artifact set: a second result for one
        // task key is a determinism violation.
        let other_object = store.put(b"a different artifact")?;
        let conflicting = TaskRecord::new(
            identity,
            vec![sima_model::ArtifactRef::new("state-final", other_object)?],
        )?;
        assert!(matches!(
            store.commit_record(&conflicting),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn record_round_trips_and_matches_its_key() -> Result<()> {
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        store.commit_record(&record)?;
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
    fn a_hand_corrupted_index_entry_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        store.commit_record(&record)?;
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
        store.commit_record(&record)?;
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

    #[test]
    fn concurrent_commits_of_the_equal_record_all_succeed() -> Result<()> {
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        let store = &store;
        let record = &record;
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(move || store.commit_record(record)))
                .collect();
            for handle in handles {
                handle.join().expect("committer thread panicked")?;
            }
            Ok::<(), Error>(())
        })?;
        assert_eq!(store.record(&record.identity.key())?.as_ref(), Some(record));
        Ok(())
    }
}
