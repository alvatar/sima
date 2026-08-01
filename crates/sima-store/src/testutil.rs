//! Helpers shared by the store test modules.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use tempfile::TempDir;

use sima_core::Hash;
use sima_model::{
    ArtifactRef, Environment, EnvironmentComponent, EnvironmentValue, FormatId, GeneratorConfig,
    GeneratorId, Params, RunConfig, Spec, TaskIdentity, TaskRecord,
};

use crate::Store;
use crate::layout;
use crate::pack::format;

/// Opens a store over a fresh temporary directory, keeping the directory
/// guard alive for the test's duration.
pub(crate) fn temp_store() -> (TempDir, Store) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path()).expect("open temp store");
    (dir, store)
}

/// Packs `objects` into one pack and deletes their loose files: the state
/// [`Store::pack`] leaves behind, built directly so the read path is tested
/// against it without the maintenance operation. Returns the pack's name.
pub(crate) fn pack_objects(store: &Store, objects: &[Hash]) -> Hash {
    let name =
        format::write_pack(store.root(), objects, &|hash| store.get(hash)).expect("write pack");
    for hash in objects {
        fs::remove_file(layout::object_path(store.root(), hash)).expect("delete loose object");
    }
    name
}

/// The packs a store holds, by name. The maintenance lock shares the
/// directory and carries no pack suffix, so the suffix is what selects.
pub(crate) fn pack_names(root: &Path) -> BTreeSet<Hash> {
    fs::read_dir(layout::packs_dir(root))
        .expect("read packs dir")
        .filter_map(|entry| {
            let name = entry.expect("pack entry").file_name();
            let name = name.to_str().expect("utf-8 name").to_string();
            name.strip_suffix(layout::PACK_SUFFIX)
                .map(|hex| Hash::from_hex(hex).expect("pack name"))
        })
        .collect()
}

/// The spec fixture shared by task tests.
pub(crate) fn sample_spec() -> Spec {
    Spec {
        format: FormatId::new("stub.v1").expect("format id"),
        bytes: vec![0xAA, 0xBB],
    }
}

/// The params fixture shared by task tests.
pub(crate) fn sample_params() -> Params {
    Params {
        bytes: vec![1, 2, 3],
    }
}

/// The environment fixture shared by task tests.
pub(crate) fn sample_environment() -> Environment {
    let component =
        EnvironmentComponent::new("engine", EnvironmentValue::Version("cpu-1.0.0".to_string()))
            .expect("environment component");
    Environment::new(vec![component]).expect("environment")
}

/// The run-config fixture shared by run tests, varying by root seed.
pub(crate) fn sample_run_config(root_seed: u64) -> RunConfig {
    RunConfig {
        root_seed,
        segments: None,
        format: FormatId::new("stub.v1").expect("format id"),
        generator: GeneratorConfig {
            id: GeneratorId::new("gen.v1").expect("generator id"),
            params: vec![0xDE, 0xAD],
        },
        params: sample_params(),
    }
}

/// A stateless task identity over the sample components, varying by seed.
pub(crate) fn sample_identity(seed: u64) -> TaskIdentity {
    TaskIdentity {
        spec: sample_spec().id(),
        params: sample_params().id(),
        seed,
        environment: sample_environment().id(),
        input_state: None,
    }
}

/// Puts the sample spec, params, and environment objects, making every
/// reference of a [`sample_identity`] durable.
pub(crate) fn store_identity_components(store: &Store) {
    for bytes in [
        sample_spec().to_bytes(),
        sample_params().to_bytes(),
        sample_environment().to_bytes(),
    ] {
        store.put(&bytes).expect("put identity component");
    }
}

/// A record for `identity` carrying one stored artifact whose bytes derive
/// from the identity's seed.
pub(crate) fn record_with_stored_artifact(store: &Store, identity: TaskIdentity) -> TaskRecord {
    let object = store
        .put(&identity.seed.to_le_bytes())
        .expect("put artifact object");
    let artifact = ArtifactRef::new("state-final", object).expect("artifact ref");
    TaskRecord::new(identity, vec![artifact]).expect("task record")
}
