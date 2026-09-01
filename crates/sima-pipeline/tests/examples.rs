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
fn search_id(file: &str) -> Result<String> {
    Ok(load(&examples().join(file))?.run.id().to_string())
}

#[test]
fn the_gray_scott_example_loads_with_its_search_id() -> Result<()> {
    assert_eq!(
        search_id("gray-scott-search.toml")?,
        "2d3d58eca2ec0a3dd9ab493b875ca03d6269295b65b6a9d1bd53036490fcff43"
    );
    Ok(())
}

#[test]
fn the_gray_scott_cuda_example_loads_with_its_search_id() -> Result<()> {
    assert_eq!(
        search_id("gray-scott-cuda-search.toml")?,
        "47d714271ce0aa23f51fcc65ce0c85693572b1b120aed06e1671b598d94758fd"
    );
    Ok(())
}

#[test]
fn the_two_examples_are_different_runs() -> Result<()> {
    // The same rule through two backends is two programs with two identities,
    // so neither reuses the other's stored results.
    assert_ne!(
        search_id("gray-scott-search.toml")?,
        search_id("gray-scott-cuda-search.toml")?
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

#[test]
fn the_stepper_example_declares_the_machines_the_tutorial_drives() -> Result<()> {
    // The stepper example ships with its machines active: the tutorial's
    // machine chapter migrates onto `cloudbox` and draws a fleet from `cheap`,
    // both rented, neither engaged by a plain `sima run` — which is what keeps
    // the example working out of the box with no key in the environment.
    let loaded = load(&examples().join("stepper-py/search.toml"))?;
    assert_eq!(loaded.orchestrator.migrate.as_deref(), Some("cloudbox"));
    assert!(loaded.hosts.contains_key("cloudbox"));
    assert!(loaded.host_classes.contains_key("cheap"));
    assert_eq!(loaded.fleet.members, ["cheap"]);
    Ok(())
}

#[test]
fn the_stepper_example_s_commented_machine_block_loads_when_uncommented() -> Result<()> {
    // A commented declaration is a declaration a reader uncomments, so it has
    // to be one the loader takes: the owned-machine block (`gpubox`) ships
    // commented because it names hardware only its author has.
    let shipped = std::fs::read_to_string(examples().join("stepper-py/search.toml"))
        .expect("the example is there");
    let uncommented: String = shipped
        .lines()
        .map(|line| match line.strip_prefix("# ") {
            // Only the declarations, which are the lines that parse as TOML.
            // Prose stays commented, so a stripped line that is not a
            // declaration is left as it was.
            Some(rest) if rest.starts_with('[') || rest.contains(" = ") => rest,
            _ => line,
        })
        .collect::<Vec<_>>()
        .join("\n");
    // The example's directory, so `binary` and `payload` resolve as shipped.
    let path = examples().join("stepper-py/uncommented.toml");
    std::fs::write(&path, &uncommented).expect("write the uncommented example");
    let loaded = load(&path);
    std::fs::remove_file(&path).expect("remove the uncommented example");
    let loaded = loaded?;

    assert!(loaded.hosts.contains_key("gpubox"));
    assert_eq!(
        loaded.run.id().to_string(),
        search_id("stepper-py/search.toml")?,
        "declaring a machine decides where, never what"
    );
    Ok(())
}

#[test]
fn the_stepper_example_loads_with_its_search_id() -> Result<()> {
    // The Python example routes its format to a program, so loading it spawns
    // that program to translate the two sections it owns. `import sima`
    // resolves in the child because the example declares `sdk = "python"`, and
    // the load vends the package the entry names before it spawns anything.
    //
    // The spawn is what makes this a load test and a path test at once: a
    // program runs in a scratch working directory of its own, so a binary named
    // relative to this process would resolve against that directory and fail to
    // spawn at all.
    assert_eq!(
        search_id("stepper-py/search.toml")?,
        "7c19fe97eaf2a8870f110f3df80840b3785bf2bd57f6633006baac3e73b48b13"
    );
    Ok(())
}
