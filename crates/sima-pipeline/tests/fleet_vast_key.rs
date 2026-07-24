//! A `[fleet]` naming the vast provider fails before any store mutation when
//! the API key is absent.
//!
//! This is the sole test in its own binary so its environment edits — removing
//! the key, pinning a worker-binary path — never race another test in the same
//! process. It removes `VAST_API_KEY` and never reads its value.

use std::path::Path;

use sima_core::{Error, Result};
use sima_pipeline::{RunControl, load, orchestrate};

#[test]
fn a_vast_fleet_without_the_key_fails_before_touching_the_store() -> Result<()> {
    // Remove the key so the vast backend cannot construct, and pin a
    // worker-binary path so binary discovery (which precedes the fleet) does
    // not fail first. Both are safe: this test is alone in its process.
    unsafe {
        std::env::remove_var("VAST_API_KEY");
        std::env::set_var("SIMA_WORKER", "/bin/true");
    }

    let dir = tempfile::tempdir().expect("temp dir");
    let config_path = dir.path().join("sima.toml");
    // A fleet carries the run, so no local workers are configured. The store
    // path is under the temp dir and must not be created.
    let text = r#"
        [run]
        root_seed = 1
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed"]

        [execution]
        store = "./store"
        max_attempts = 3

        [fleet]
        provider = "vast"
        count = 1
    "#;
    std::fs::write(&config_path, text).expect("write config");
    let config = load(&config_path)?;

    match orchestrate(&config, &RunControl::detached()) {
        Err(Error::Provider(message)) => {
            assert!(
                message.contains("VAST_API_KEY"),
                "the error names the variable: {message}"
            );
        }
        other => panic!("expected a provider error naming the key, got {other:?}"),
    }

    // The failure preceded Store::open, so no store directory was created.
    assert!(
        !Path::new(&config.store).exists(),
        "no store is created for a fleet that cannot construct its provider"
    );
    Ok(())
}
