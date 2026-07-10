//! Helpers shared by the store test modules.

use tempfile::TempDir;

use sima_model::{
    ArtifactRef, Environment, EnvironmentComponent, EnvironmentValue, FormatId, GeneratorConfig,
    GeneratorId, Params, RunConfig, Spec, TaskIdentity, TaskRecord,
};

use crate::Store;

/// Opens a store over a fresh temporary directory, keeping the directory
/// guard alive for the test's duration.
pub(crate) fn temp_store() -> (TempDir, Store) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path()).expect("open temp store");
    (dir, store)
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
