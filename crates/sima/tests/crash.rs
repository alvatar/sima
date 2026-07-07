//! Crash-injection harness — acceptance criterion (b): a run SIGKILLed at
//! any crashpoint and resumed yields a manifest identical to a
//! never-interrupted run's.
//!
//! Each case spawns the real binary with `SIMA_CRASHPOINT` arming one
//! point, asserts the child died by SIGKILL — an unmaskable death, no
//! destructors, no unwinding — then re-runs unarmed over the same store
//! and compares the manifest against an uninterrupted reference run's.
//! The kernel-released orchestrator lock is asserted along the way.

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};

use sima_pipeline::load;
use sima_store::{Manifest, Store};

/// Enough tasks that mid-run hit counts land while work is still ahead:
/// commits and leases number six, object writes a multiple of that.
const BEHAVIORS: &str = r#""succeed", "succeed", "succeed", "sleep:20", "sleep:20", "succeed""#;

/// Writes a `sima.toml` named `name` under `dir`, its store at `store`.
fn write_config(dir: &Path, name: &str, store: &str) -> PathBuf {
    let text = format!(
        r#"
        [run]
        root_seed = 23
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = [{BEHAVIORS}]

        [execution]
        store = "{store}"
        workers = 2
        max_attempts = 3
    "#
    );
    let path = dir.join(name);
    std::fs::write(&path, text).expect("write config");
    path
}

/// Runs `sima run <config>`, armed with `crashpoint` when given, and
/// returns the exit status. Output is discarded — the store carries the
/// assertions.
fn sima_run(config: &Path, crashpoint: Option<&str>) -> ExitStatus {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sima"));
    command
        .args(["run", config.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env_remove("SIMA_CRASHPOINT");
    if let Some(arming) = crashpoint {
        command.env("SIMA_CRASHPOINT", arming);
    }
    command.status().expect("spawn sima")
}

/// The manifest of the run `config_path` describes, from its store.
fn manifest_of(config_path: &Path) -> Option<Manifest> {
    let config = load(config_path).expect("load config");
    let store = Store::open(&config.store).expect("open store");
    store.manifest(&config.run.id()).expect("read manifest")
}

/// Harness soundness: the SIGKILL assertion is falsifiable. An unarmed
/// child sails past every planted point and exits 0, so a passing armed
/// case below genuinely proves the injected death.
#[test]
fn an_unarmed_run_is_not_sigkilled() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), "unarmed.toml", "./store");
    let status = sima_run(&config, None);
    assert_ne!(status.signal(), Some(9), "unarmed child died by SIGKILL");
    assert_eq!(status.code(), Some(0), "unarmed child finalizes");
}

/// The matrix over the four planted points, each at the first hit and —
/// where a run hits the point more than once — at a mid-run count.
/// `finalize.pre-write` fires once per run, so only `:1` can land.
#[test]
fn every_crashpoint_death_resumes_to_the_reference_manifest() {
    let dir = tempfile::tempdir().expect("temp dir");

    // The reference: the same behaviors run uninterrupted.
    let reference_config = write_config(dir.path(), "reference.toml", "./store-reference");
    assert_eq!(sima_run(&reference_config, None).code(), Some(0));
    let reference = manifest_of(&reference_config).expect("reference manifest");

    for arming in [
        "object.mid-write:1",
        "object.mid-write:5",
        "commit.after-object:1",
        "commit.after-object:3",
        "lease.held:1",
        "lease.held:3",
        "finalize.pre-write:1",
    ] {
        let slug = arming.replace([':', '.'], "-");
        let config = write_config(
            dir.path(),
            &format!("{slug}.toml"),
            &format!("./store-{slug}"),
        );

        let status = sima_run(&config, Some(arming));
        assert_eq!(
            status.signal(),
            Some(9),
            "{arming}: the armed child dies by SIGKILL, got {status:?}"
        );
        assert!(
            manifest_of(&config).is_none(),
            "{arming}: no manifest survives the death"
        );

        // The kernel released the orchestrator lock with the process:
        // it is acquirable immediately, no staleness protocol.
        let loaded = load(&config).expect("load config");
        let store = Store::open(&loaded.store).expect("open store");
        drop(
            store
                .acquire_run_lock(&loaded.run.id())
                .unwrap_or_else(|e| panic!("{arming}: the lock must be free after death: {e}")),
        );

        // Resume unarmed: the frontier re-derives and the run finalizes
        // to the byte-identical manifest.
        let resumed = sima_run(&config, None);
        assert_eq!(
            resumed.code(),
            Some(0),
            "{arming}: the resumed run finalizes"
        );
        assert_eq!(
            manifest_of(&config).as_ref(),
            Some(&reference),
            "{arming}: the resumed manifest equals the reference"
        );
    }
}
