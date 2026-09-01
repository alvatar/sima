//! End-to-end store behavior over the public surface: manifest determinism
//! across commit orders, portability of a copied store directory, and
//! convergence of concurrent workers on one manifest.

use std::fs;
use std::path::Path;

use sima_core::{Codec, Result};
use sima_model::{
    ArtifactRef, Environment, EnvironmentComponent, EnvironmentValue, FormatId, GeneratorConfig,
    GeneratorId, Params, SearchConfig, SearchId, Spec, TaskIdentity, TaskKey, TaskRecord,
};
use sima_store::Store;
use tempfile::TempDir;

/// The search fixture: one spec/params/environment shared by every task.
fn spec() -> Spec {
    Spec {
        format: FormatId::new("stub.v1").expect("format id"),
        bytes: vec![0xAA, 0xBB],
    }
}

fn params() -> Params {
    Params {
        bytes: vec![1, 2, 3],
    }
}

fn environment() -> Environment {
    let component =
        EnvironmentComponent::new("engine", EnvironmentValue::Version("cpu-1.0.0".to_string()))
            .expect("environment component");
    Environment::new(vec![component]).expect("environment")
}

fn config() -> SearchConfig {
    SearchConfig {
        root_seed: 42,
        segments: None,
        format: FormatId::new("stub.v1").expect("format id"),
        generator: GeneratorConfig {
            id: GeneratorId::new("gen.v1").expect("generator id"),
            params: vec![0xDE, 0xAD],
        },
        params: params(),
    }
}

/// Stores the shared identity components, then commits the task for
/// `seed`: an identity over the shared components and one artifact
/// derived from the seed. Returns the task key.
fn commit_task(store: &Store, seed: u64) -> Result<TaskKey> {
    store.put(&spec().to_bytes())?;
    store.put(&params().to_bytes())?;
    store.put(&environment().to_bytes())?;
    let identity = TaskIdentity {
        spec: spec().id(),
        params: params().id(),
        seed,
        environment: environment().id(),
        input_state: None,
    };
    let artifact_object = store.put(&seed.to_le_bytes())?;
    let artifact = ArtifactRef::new("state-final", artifact_object)?;
    let record = TaskRecord::new(identity, vec![artifact])?;
    store.commit(&record)?;
    Ok(identity.key())
}

/// Runs the whole lifecycle in a fresh store — commit `seeds` in the
/// order given, create the search, journal, finalize — and returns the
/// store's guard, handle, and search id.
fn search_lifecycle(seeds: &[u64], journal_lines: &[&str]) -> Result<(TempDir, Store, SearchId)> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path())?;
    let mut keys = Vec::new();
    for &seed in seeds {
        keys.push(commit_task(&store, seed)?);
    }
    let search = store.create_search(&config())?;
    let mut journal = store.journal_writer(&search)?;
    for line in journal_lines {
        journal.append(line)?;
    }
    store.finalize_search(&search, &keys)?;
    Ok((dir, store, search))
}

/// The manifest bytes of `search` under `root`.
fn manifest_bytes(root: &Path, search: &SearchId) -> Vec<u8> {
    fs::read(
        root.join("searches")
            .join(search.to_string())
            .join("manifest.json"),
    )
    .expect("read manifest file")
}

#[test]
fn permuted_commit_orders_produce_byte_identical_manifests() -> Result<()> {
    // Two fresh stores, permuted commit order and different journals:
    // the manifests must still agree byte for byte — search identity is
    // independent of worker completion order, and journals are
    // observational.
    let (dir_a, _store_a, run_a) = search_lifecycle(&[1, 2, 3], &["started", "finished"])?;
    let (dir_b, _store_b, run_b) = search_lifecycle(&[3, 1, 2], &["resumed after a crash"])?;
    assert_eq!(run_a, run_b);
    assert_eq!(
        manifest_bytes(dir_a.path(), &run_a),
        manifest_bytes(dir_b.path(), &run_b)
    );
    Ok(())
}

/// Copies a directory tree, preserving the layout.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("create destination dir");
    for entry in fs::read_dir(from).expect("read source dir") {
        let entry = entry.expect("dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

#[test]
fn a_copied_store_is_fully_portable() -> Result<()> {
    let (dir, store, search) = search_lifecycle(&[1, 2], &["one line"])?;
    let copy = tempfile::tempdir().expect("create copy dir");
    let copy_root = copy.path().join("store");
    copy_dir(dir.path(), &copy_root);
    // The copy opens as-is, reads the equal manifest, holds the full
    // closure, and every object in it get-verifies.
    let copied = Store::open(&copy_root)?;
    assert_eq!(copied.manifest(&search)?, store.manifest(&search)?);
    let closure = copied.search_closure(&search)?;
    assert_eq!(closure, store.search_closure(&search)?);
    for object in &closure {
        copied.get(object)?;
    }
    assert_eq!(copied.journal(&search)?, ["one line"]);
    Ok(())
}

#[test]
fn concurrent_workers_converge_on_the_single_threaded_manifest() -> Result<()> {
    let seeds: Vec<u64> = (0..16).collect();
    let (reference_dir, _store, reference_run) = search_lifecycle(&seeds, &[])?;
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path())?;
    // Four workers commit disjoint task ranges concurrently.
    let store_ref = &store;
    let keys = std::thread::scope(|scope| {
        let handles: Vec<_> = seeds
            .chunks(4)
            .map(|chunk| {
                scope.spawn(move || -> Result<Vec<TaskKey>> {
                    chunk
                        .iter()
                        .map(|&seed| commit_task(store_ref, seed))
                        .collect()
                })
            })
            .collect();
        let mut keys = Vec::new();
        for handle in handles {
            keys.extend(handle.join().expect("worker thread panicked")?);
        }
        Ok::<Vec<TaskKey>, sima_core::Error>(keys)
    })?;
    let search = store.create_search(&config())?;
    store.finalize_search(&search, &keys)?;
    assert_eq!(search, reference_run);
    assert_eq!(
        manifest_bytes(dir.path(), &search),
        manifest_bytes(reference_dir.path(), &reference_run)
    );
    Ok(())
}
