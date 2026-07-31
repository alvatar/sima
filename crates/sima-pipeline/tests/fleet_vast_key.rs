//! A fleet drawing on a rented entry that names the vast provider fails before
//! any store mutation when the API key is absent — and only when the invocation
//! asked for the fleet at all.
//!
//! This is the sole test in its own binary so its environment edits — removing
//! the key, pinning a worker-binary path — never race another test in the same
//! process. It removes `VAST_API_KEY` and never reads its value.

use std::path::Path;

use sima_core::{Error, Result};
use sima_pipeline::{BinaryChange, Engagement, RunControl, load, orchestrate};

#[test]
fn a_vast_rental_reads_the_key_only_when_the_fleet_is_engaged() -> Result<()> {
    // Remove the key so the vast backend cannot construct, and pin a
    // worker-binary path so binary discovery (which precedes the fleet) does
    // not fail first. Both are safe: this test is alone in its process.
    unsafe {
        std::env::remove_var("VAST_API_KEY");
        std::env::set_var("SIMA_WORKER", "/bin/true");
    }

    let dir = tempfile::tempdir().expect("temp dir");
    let config_path = dir.path().join("sima.toml");
    // The rented machines carry the run, so the orchestrator declares no
    // workers. The store path is under the temp dir and must not be created.
    let text = r#"
        [run]
        root_seed = 1
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed"]

        [config]
        store = "./store"
        max_attempts = 3

        [host.slingshot]
        provider = "vast"

        [fleet]
        members = ["slingshot"]
    "#;
    std::fs::write(&config_path, text).expect("write config");
    let config = load(&config_path)?;

    // Without the flag the fleet is never resolved, so no provider is built and
    // the key is never looked for. The run then has nothing to execute on,
    // which is a validation error naming the flag — the shape that proves the
    // marketplace was not touched, since an absent key would have surfaced as a
    // provider error instead.
    match orchestrate(
        &config,
        &RunControl::detached(),
        Engagement::Orchestrator,
        BinaryChange::Refuse,
    ) {
        Err(Error::Validation(message)) => {
            assert!(
                message.contains("--fleet"),
                "the error names the flag that would engage the rental: {message}"
            );
        }
        other => panic!("expected a validation error naming the flag, got {other:?}"),
    }

    // With the flag the provider is constructed, and the absent key surfaces.
    match orchestrate(
        &config,
        &RunControl::detached(),
        Engagement::Fleet,
        BinaryChange::Refuse,
    ) {
        Err(Error::Provider(message)) => {
            assert!(
                message.contains("VAST_API_KEY"),
                "the error names the variable: {message}"
            );
        }
        other => panic!("expected a provider error naming the key, got {other:?}"),
    }

    // Neither failure reached Store::open, so no store directory was created.
    assert!(
        !Path::new(&config.store).exists(),
        "no store is created for a rental that cannot construct its provider"
    );
    Ok(())
}
