//! End-to-end store behavior over the public surface: manifest determinism
//! across commit orders, portability of a copied store directory, and
//! convergence of concurrent workers on one manifest.

use std::fs;
use std::path::Path;

use sima_core::Result;
use sima_model::{
    ArtifactRef, Environment, EnvironmentComponent, EnvironmentValue, FormatId, GeneratorConfig,
    GeneratorId, Params, RunConfig, RunId, Spec, TaskIdentity, TaskKey, TaskRecord,
};
use sima_store::Store;
use tempfile::TempDir;

/// The run fixture: one spec/params/environment shared by every task.
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

fn config() -> RunConfig {
    RunConfig {
        root_seed: 42,
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
    store.commit_record(&record)?;
    Ok(identity.key())
}

/// Runs the whole lifecycle in a fresh store — commit `seeds` in the
/// order given, create the run, journal, finalize — and returns the
/// store's guard, handle, and run id.
fn run_lifecycle(seeds: &[u64], journal_lines: &[&str]) -> Result<(TempDir, Store, RunId)> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path())?;
    let mut keys = Vec::new();
    for &seed in seeds {
        keys.push(commit_task(&store, seed)?);
    }
    let run = store.create_run(&config())?;
    let mut journal = store.journal_writer(&run)?;
    for line in journal_lines {
        journal.append(line)?;
    }
    store.finalize_run(&run, &keys)?;
    Ok((dir, store, run))
}

/// The manifest bytes of `run` under `root`.
fn manifest_bytes(root: &Path, run: &RunId) -> Vec<u8> {
    fs::read(
        root.join("runs")
            .join(run.to_string())
            .join("manifest.json"),
    )
    .expect("read manifest file")
}

#[test]
fn permuted_commit_orders_produce_byte_identical_manifests() -> Result<()> {
    // Two fresh stores, permuted commit order and different journals:
    // the manifests must still agree byte for byte — run identity is
    // independent of worker completion order, and journals are
    // observational.
    let (dir_a, _store_a, run_a) = run_lifecycle(&[1, 2, 3], &["started", "finished"])?;
    let (dir_b, _store_b, run_b) = run_lifecycle(&[3, 1, 2], &["resumed after a crash"])?;
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
    let (dir, store, run) = run_lifecycle(&[1, 2], &["one line"])?;
    let copy = tempfile::tempdir().expect("create copy dir");
    let copy_root = copy.path().join("store");
    copy_dir(dir.path(), &copy_root);
    // The copy opens as-is, reads the equal manifest, holds the full
    // closure, and every object in it get-verifies.
    let copied = Store::open(&copy_root)?;
    assert_eq!(copied.manifest(&run)?, store.manifest(&run)?);
    let closure = copied.run_closure(&run)?;
    assert_eq!(closure, store.run_closure(&run)?);
    for object in &closure {
        copied.get(object)?;
    }
    assert_eq!(copied.journal(&run)?, ["one line"]);
    Ok(())
}

#[test]
fn concurrent_workers_converge_on_the_single_threaded_manifest() -> Result<()> {
    let seeds: Vec<u64> = (0..16).collect();
    let (reference_dir, _store, reference_run) = run_lifecycle(&seeds, &[])?;
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
    let run = store.create_run(&config())?;
    store.finalize_run(&run, &keys)?;
    assert_eq!(run, reference_run);
    assert_eq!(
        manifest_bytes(dir.path(), &run),
        manifest_bytes(reference_dir.path(), &reference_run)
    );
    Ok(())
}
