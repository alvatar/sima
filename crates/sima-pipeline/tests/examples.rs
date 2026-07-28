//! The shipped examples load, and each yields the run id it always has.
//!
//! An example is the surface a reader copies from, so it has to parse under the
//! current schema. The pinned ids are the second half: `[run]` is the only
//! hashed section, so an edit anywhere else must leave a run's identity
//! untouched, and an edit to `[run]` must be a deliberate one that shows up
//! here rather than a store that quietly stops matching.

use std::path::PathBuf;

use sima_core::Result;
use sima_pipeline::load;

/// The repository's `examples/` directory.
fn examples() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples")
}

/// Loads the example named `file` and returns its run id, rendered.
fn run_id(file: &str) -> Result<String> {
    Ok(load(&examples().join(file))?.run.id().to_string())
}

#[test]
fn the_gray_scott_example_loads_with_its_run_id() -> Result<()> {
    assert_eq!(
        run_id("gray-scott-search.toml")?,
        "2d3d58eca2ec0a3dd9ab493b875ca03d6269295b65b6a9d1bd53036490fcff43"
    );
    Ok(())
}

#[test]
fn the_gray_scott_cuda_example_loads_with_its_run_id() -> Result<()> {
    assert_eq!(
        run_id("gray-scott-cuda-search.toml")?,
        "47d714271ce0aa23f51fcc65ce0c85693572b1b120aed06e1671b598d94758fd"
    );
    Ok(())
}

#[test]
fn the_two_examples_are_different_runs() -> Result<()> {
    // The same rule through two backends is two programs with two identities,
    // so neither reuses the other's stored results.
    assert_ne!(
        run_id("gray-scott-search.toml")?,
        run_id("gray-scott-cuda-search.toml")?
    );
    Ok(())
}

#[test]
fn every_example_carries_a_worker_layout_and_declares_no_machine_it_does_not_use() -> Result<()> {
    // A reader who copies an example and runs it must get a run that executes:
    // the orchestrator states a layout, so `sima run` needs no flag. Every
    // machine beyond this one is commented out, so nothing is declared that the
    // example does not use.
    for file in ["gray-scott-search.toml", "gray-scott-cuda-search.toml"] {
        let config = load(&examples().join(file))?;
        assert!(
            config.orchestrator.pool.is_some(),
            "{file}: the orchestrator states a worker layout"
        );
        assert!(config.hosts.is_empty(), "{file}: no host is declared");
        assert!(
            config.host_classes.is_empty(),
            "{file}: no host class is declared"
        );
        assert!(
            config.fleet.members.is_empty(),
            "{file}: the fleet names no member"
        );
    }
    Ok(())
}
