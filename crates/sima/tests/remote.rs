//! Remote-execution acceptance over the real binaries: a run spread across a
//! local pool and an `[[execution.remote]]` container pool, single-class
//! manifest invariance across the transport boundary, a remote worker killed
//! mid-lease converging through retry, and a resume without the remote
//! rebinding loudly.
//!
//! Every test needs a container runtime the orchestrator can reach and the
//! built worker image, so all are `#[ignore]` and additionally gated on
//! `SIMA_TEST_REMOTE` — an ssh destination (the test docs name `localhost` as
//! the expected value, which also guarantees driver parity for the manifest
//! comparison). Absent that variable, each test skips with a message, so a
//! blanket `--ignored` run passes clean on a machine with no remote.
//!
//! ```text
//! SIMA_TEST_REMOTE=localhost \
//! SIMA_TEST_IMAGE=localhost/sima-worker:latest \
//!   cargo test -p sima --test remote -- --ignored
//! ```
//!
//! Configuration comes from the environment so one suite runs against a
//! provisioned localhost or a real remote unchanged:
//!
//! - `SIMA_TEST_REMOTE` — the ssh destination; unset skips every test.
//! - `SIMA_TEST_IMAGE` — the worker image; default
//!   `localhost/sima-worker:latest`.
//! - `SIMA_TEST_RUNTIME` — `docker` or `podman`; default `docker`.
//! - `SIMA_TEST_RUN_ARGS` — space-separated container-run flags for GPU
//!   access; default `--gpus all`.

mod common;

use std::collections::HashSet;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use common::{
    journal_events, manifest_bytes, manifest_of, poll_until, sima_command, task_devices,
    write_config_text,
};
use sima_pipeline::LifecycleEvent;

/// The candidates and segments every remote test runs. Sized like the device
/// suite so several chains outnumber the workers and both pools pull work.
const CANDIDATES: u32 = 12;
const SEGMENTS: u64 = 3;

/// The remote target the environment names, or `None` when the suite should
/// skip. Read once per test; a test with no target returns early.
struct RemoteEnv {
    host: String,
    image: String,
    runtime: String,
    run_args: Vec<String>,
}

impl RemoteEnv {
    /// The environment's remote target, or `None` — the skip signal.
    fn from_env() -> Option<RemoteEnv> {
        let host = std::env::var("SIMA_TEST_REMOTE").ok()?;
        let image = std::env::var("SIMA_TEST_IMAGE")
            .unwrap_or_else(|_| "localhost/sima-worker:latest".to_string());
        let runtime = std::env::var("SIMA_TEST_RUNTIME").unwrap_or_else(|_| "docker".to_string());
        let run_args = std::env::var("SIMA_TEST_RUN_ARGS")
            .unwrap_or_else(|_| "--gpus all".to_string())
            .split_whitespace()
            .map(str::to_string)
            .collect();
        Some(RemoteEnv {
            host,
            image,
            runtime,
            run_args,
        })
    }

    /// The `[[execution.remote]]` table for this target, its device tables
    /// resolving `select` on the remote's own hardware.
    fn remote_table(&self, select: &str, workers: u32) -> String {
        let args = self
            .run_args
            .iter()
            .map(|a| format!("{a:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            r#"
        [[execution.remote]]
        host = "{host}"
        image = "{image}"
        runtime = "{runtime}"
        run_args = [{args}]

        [[execution.remote.device]]
        select = "{select}"
        workers = {workers}
    "#,
            host = self.host,
            image = self.image,
            runtime = self.runtime,
        )
    }
}

/// The skip guard: the remote target, or an early return with a message.
macro_rules! remote_or_skip {
    () => {
        match RemoteEnv::from_env() {
            Some(env) => env,
            None => {
                eprintln!("SIMA_TEST_REMOTE unset; skipping the remote acceptance test");
                return;
            }
        }
    };
}

/// A Gray-Scott config with `count` candidates over `segments`, its execution
/// section filled by `execution`.
fn config_text(store: &str, count: u32, segments: u64, execution: &str) -> String {
    format!(
        r#"
        [run]
        root_seed = 42
        format = "ca_evolution.gray_scott.v1"
        segments = {segments}

        [run.generator]
        id = "ca_evolution.gray_scott.v1"
        count = {count}
        feed = [0.050, 0.058]
        kill = [0.062, 0.062]
        diffusion_u = [0.16, 0.16]
        diffusion_v = [0.08, 0.08]

        [run.params]
        width = 128
        height = 128
        steps = 600
        dt = 1.0
        base_u = 0.5
        base_v = 0.25
        side_divisor = 8
        noise_width = 0.02

        [execution]
        store = "{store}"
        max_attempts = 3
        {execution}
    "#
    )
}

/// Runs `config` to completion, asserting it finalized.
fn run_to_completion(config: &Path) {
    let status = sima_command()
        .args(["run", config.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sima")
        .wait()
        .expect("wait for the run");
    assert_eq!(status.code(), Some(0), "the run finalized");
}

/// The set of hosts the journal's `WorkerBound` events name.
fn bound_hosts(events: &[LifecycleEvent]) -> HashSet<String> {
    events
        .iter()
        .filter_map(|event| match event {
            LifecycleEvent::WorkerBound { host, .. } => Some(host.clone()),
            _ => None,
        })
        .collect()
}

/// A mixed run over a local pool and a remote container pool commits from both,
/// and no chain splits across the transport boundary.
#[test]
#[ignore = "requires a container runtime and the worker image on SIMA_TEST_REMOTE"]
fn a_mixed_local_and_remote_run_commits_from_both_pools() {
    let env = remote_or_skip!();
    let dir = tempfile::tempdir().expect("temp dir");
    let execution = format!(
        "[[execution.device]]\n        select = \"nvidia\"\n        workers = 2\n{}",
        env.remote_table("nvidia", 2)
    );
    let config = write_config_text(
        dir.path(),
        "mixed.toml",
        &config_text("./store", CANDIDATES, SEGMENTS, &execution),
    );
    run_to_completion(&config);

    let events = journal_events(&config);
    let hosts = bound_hosts(&events);
    assert!(
        hosts.contains("") && hosts.contains(&env.host),
        "both the local pool and {} bound workers: {hosts:?}",
        env.host
    );
    // Every chain ran its segments on one class, across the pool boundary.
    let ran_on = task_devices(&events);
    for (task, classes) in &ran_on {
        let distinct: HashSet<&String> = classes.iter().collect();
        assert!(distinct.len() <= 1, "task {task} split across classes");
    }
    assert!(manifest_of(&config).is_some(), "the mixed run finalized");
}

/// The same single-class run on a local pool and on a remote pool commits a
/// byte-identical manifest: `ssh localhost` guarantees driver parity, so the
/// transport carrying an attempt never reaches run identity.
#[test]
#[ignore = "requires a container runtime and the worker image on SIMA_TEST_REMOTE"]
fn single_class_manifests_are_identical_local_and_remote() {
    let env = remote_or_skip!();
    let dir = tempfile::tempdir().expect("temp dir");

    let local = write_config_text(
        dir.path(),
        "local.toml",
        &config_text(
            "./local-store",
            CANDIDATES,
            SEGMENTS,
            "[[execution.device]]\n        select = \"nvidia\"\n        workers = 2",
        ),
    );
    let remote = write_config_text(
        dir.path(),
        "remote.toml",
        &config_text(
            "./remote-store",
            CANDIDATES,
            SEGMENTS,
            &env.remote_table("nvidia", 2),
        ),
    );
    run_to_completion(&local);
    run_to_completion(&remote);

    let local_manifest = manifest_bytes(&local).expect("the local run finalized");
    let remote_manifest = manifest_bytes(&remote).expect("the remote run finalized");
    assert_eq!(
        local_manifest, remote_manifest,
        "one class, one manifest, whatever transport carried the attempts"
    );
}

/// A remote worker's container killed mid-lease is a transient failure: the run
/// converges through retry to a valid manifest, and the journal records it.
#[test]
#[ignore = "requires a container runtime and the worker image on SIMA_TEST_REMOTE"]
fn a_killed_remote_container_converges_through_retry() {
    let env = remote_or_skip!();
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config_text(
        dir.path(),
        "kill.toml",
        &config_text(
            "./store",
            CANDIDATES,
            SEGMENTS,
            &env.remote_table("nvidia", 2),
        ),
    );
    let mut child = sima_command()
        .args(["run", config.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sima");

    // Wait until a worker is running remote work, then kill one of its
    // containers on the remote — the second-channel kill the remote path uses.
    let killed = poll_until(Duration::from_secs(120), || kill_one_container(&env));
    assert!(killed, "a worker container was running to kill");

    let status = child.wait().expect("wait for the run");
    assert_eq!(status.code(), Some(0), "the run converged after the kill");

    let events = journal_events(&config);
    let retried = events
        .iter()
        .any(|event| matches!(event, LifecycleEvent::Retried { .. }));
    assert!(retried, "the killed attempt was retried");
    assert!(manifest_of(&config).is_some(), "the run finalized");
}

/// A run resumed without its remote pool rebinds the chains bound to the
/// remote-only class and converges — the M4.2 rebind machinery composing with
/// remote pools.
#[test]
#[ignore = "requires a container runtime and the worker image on SIMA_TEST_REMOTE"]
fn a_resume_without_the_remote_rebinds_loudly() {
    let env = remote_or_skip!();
    let dir = tempfile::tempdir().expect("temp dir");
    // First session: a local Intel pool and a remote NVIDIA pool. Chains bind
    // across both classes.
    let with_remote = format!(
        "[[execution.device]]\n        select = \"intel\"\n        workers = 2\n{}",
        env.remote_table("nvidia", 2)
    );
    let full = write_config_text(
        dir.path(),
        "full.toml",
        &config_text("./store", CANDIDATES, SEGMENTS, &with_remote),
    );
    let mut child = sima_command()
        .args(["run", full.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sima");
    // Let some chains bind to the NVIDIA class, then kill the orchestrator.
    let bound = poll_until(Duration::from_secs(120), || {
        journal_events(&full)
            .iter()
            .filter(|e| matches!(e, LifecycleEvent::WorkerBound { .. }))
            .count()
            >= 4
    });
    assert!(bound, "workers bound before the kill");
    child.kill().expect("kill the orchestrator");
    let _ = child.wait();

    // Resume over the Intel class alone: the same store, no remote. Chains
    // bound to the absent NVIDIA class must rebind.
    let local_only = write_config_text(
        dir.path(),
        "local-only.toml",
        &config_text(
            "./store",
            CANDIDATES,
            SEGMENTS,
            "[[execution.device]]\n        select = \"intel\"\n        workers = 2",
        ),
    );
    run_to_completion(&local_only);
    let events = journal_events(&local_only);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, LifecycleEvent::ChainRebound { .. })),
        "a chain whose class was gone rebound loudly"
    );
    assert!(
        manifest_of(&local_only).is_some(),
        "the resumed run finalized"
    );
}

/// Kills one worker container on the remote whose name carries the run's pool
/// stem, best-effort. Returns whether a container was killed.
fn kill_one_container(env: &RemoteEnv) -> bool {
    // List the worker containers, take the first, and kill it over ssh.
    let listed = std::process::Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            &env.host,
            "--",
            &env.runtime,
            "ps",
            "--filter",
            "name=sima-w-",
            "--format",
            "{{.Names}}",
        ])
        .output();
    let Ok(output) = listed else {
        return false;
    };
    let Some(name) = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
    else {
        return false;
    };
    std::process::Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            &env.host,
            "--",
            &env.runtime,
            "kill",
            &name,
        ])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}
