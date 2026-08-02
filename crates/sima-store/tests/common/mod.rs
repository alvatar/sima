//! Shared fixtures for the store-sync integration tests: building stores that
//! hold committed records, and running a full sync between two of them over a
//! duplex pipe.

use std::io::pipe;
use std::thread;

use sima_core::{Codec, Result};
use sima_model::{
    ArtifactRef, Environment, EnvironmentComponent, EnvironmentValue, FormatId, Params, Spec,
    TaskIdentity, TaskKey, TaskRecord,
};
use sima_store::{ObjectScope, Store, SyncReport, SyncRole};
use tempfile::TempDir;

/// The spec shared by every fixture task.
fn sample_spec() -> Spec {
    Spec {
        format: FormatId::new("stub.v1").expect("format id"),
        bytes: vec![0xAA, 0xBB],
    }
}

/// The params shared by every fixture task.
fn sample_params() -> Params {
    Params {
        bytes: vec![1, 2, 3],
    }
}

/// The environment shared by every fixture task.
fn sample_environment() -> Environment {
    let component =
        EnvironmentComponent::new("engine", EnvironmentValue::Version("cpu-1.0.0".to_string()))
            .expect("environment component");
    Environment::new(vec![component]).expect("environment")
}

/// A fresh store holding the identity components every fixture task
/// references, and nothing else.
pub fn empty_store() -> (TempDir, Store) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path()).expect("open store");
    store_identity_components(&store);
    (dir, store)
}

/// Puts the spec, params, and environment objects every fixture task's identity
/// names, which `replicate` requires durable and `commit` requires along with
/// the artifacts.
pub fn store_identity_components(store: &Store) {
    for bytes in [
        sample_spec().to_bytes(),
        sample_params().to_bytes(),
        sample_environment().to_bytes(),
    ] {
        store.put(&bytes).expect("put identity component");
    }
}

/// A stateless task identity over the shared components, varying by seed.
pub fn sample_identity(seed: u64) -> TaskIdentity {
    TaskIdentity {
        spec: sample_spec().id(),
        params: sample_params().id(),
        seed,
        environment: sample_environment().id(),
        input_state: None,
    }
}

/// Opens a fresh store and commits one record per seed, each carrying an
/// artifact whose bytes derive from the seed, with every referenced object
/// stored. Returns the temp dir (kept alive), the store, and the task keys.
pub fn store_with(seeds: &[u64]) -> (TempDir, Store, Vec<TaskKey>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path()).expect("open store");
    store_identity_components(&store);
    let mut keys = Vec::new();
    for &seed in seeds {
        keys.push(commit_record(&store, seed, &seed.to_le_bytes()));
    }
    (dir, store, keys)
}

/// Commits a record for `seed` carrying one artifact with the given bytes,
/// storing the artifact object first. Returns the task key. The identity
/// components are assumed already stored.
pub fn commit_record(store: &Store, seed: u64, artifact_bytes: &[u8]) -> TaskKey {
    let object = store.put(artifact_bytes).expect("put artifact object");
    let artifact = ArtifactRef::new("state-final", object).expect("artifact ref");
    let record = TaskRecord::new(sample_identity(seed), vec![artifact]).expect("task record");
    store.commit(&record).expect("commit record");
    record.identity.key()
}

/// Runs a full sync between two stores over a duplex pipe on two threads, each
/// advertising every object its records reference, returning `(initiator
/// report, responder report)`.
pub fn run_sync(
    a: &Store,
    a_keys: &[TaskKey],
    b: &Store,
    b_keys: &[TaskKey],
) -> (Result<SyncReport>, Result<SyncReport>) {
    run_sync_scoped(a, a_keys, ObjectScope::Referenced, b, b_keys)
}

/// Runs a full sync in which the initiator advertises under `scope` and the
/// responder advertises everything its records reference, returning
/// `(initiator report, responder report)`.
pub fn run_sync_scoped(
    a: &Store,
    a_keys: &[TaskKey],
    scope: ObjectScope<'_>,
    b: &Store,
    b_keys: &[TaskKey],
) -> (Result<SyncReport>, Result<SyncReport>) {
    let (a_read, b_write) = pipe().expect("pipe");
    let (b_read, a_write) = pipe().expect("pipe");
    thread::scope(|threads| {
        let responder = threads.spawn(|| {
            let (mut r, mut w) = (b_read, b_write);
            b.sync(
                b_keys,
                ObjectScope::Referenced,
                &mut r,
                &mut w,
                SyncRole::Responder,
            )
        });
        let (mut r, mut w) = (a_read, a_write);
        let initiator = a.sync(a_keys, scope, &mut r, &mut w, SyncRole::Initiator);
        (initiator, responder.join().expect("responder thread"))
    })
}
