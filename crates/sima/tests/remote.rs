//! Remote-execution acceptance over the real binaries and the built worker
//! image, in two tiers by carrier.
//!
//! **Tier A — a container pool on this machine, no ssh.** The four acceptance
//! scenarios run against the real Task-10 image over a local container runtime:
//! a `[[execution.remote]]` entry with no `host`, so the worker pool's transport
//! is `podman run --rm -i --name <name> <run_args> <image> sima-worker` with no
//! ssh prefix — every layer of the remote-worker mechanism except the ssh hop,
//! which the transport cannot distinguish from any other pipe carrier. These are
//! `#[ignore]` and skip clean when the image is absent (`SIMA_TEST_IMAGE`
//! unset), exactly as the device suite skips without a GPU; they run in this
//! stage's final gate set on any machine with a container runtime and the image.
//!
//! **Tier B — a container pool across ssh.** The ssh-specific variants: the same
//! mixed run with a real ssh hop, the two-stage kill through a second ssh
//! connection, and the BatchMode refusal on an unreachable host. Double-gated —
//! `#[ignore]` plus a `SIMA_TEST_REMOTE` ssh destination — so a blanket
//! `--ignored` run passes clean where no remote is provisioned. They run
//! unchanged the first time an SSH-reachable host exists.
//!
//! ```text
//! # Tier A, on a machine with podman and the image:
//! SIMA_TEST_IMAGE=localhost/sima-worker:latest \
//!   cargo test -p sima --test remote -- --ignored
//!
//! # Tier B additionally, with an ssh destination:
//! SIMA_TEST_REMOTE=gpubox SIMA_TEST_IMAGE=localhost/sima-worker:latest \
//!   cargo test -p sima --test remote -- --ignored
//! ```
//!
//! The environment configures the container pool so one suite runs against a
//! local runtime, a provisioned localhost, or a real remote unchanged:
//!
//! - `SIMA_TEST_IMAGE` — the worker image; unset skips Tier A, and defaults to
//!   `localhost/sima-worker:latest` for Tier B.
//! - `SIMA_TEST_REMOTE` — the ssh destination; unset skips Tier B.
//! - `SIMA_TEST_RUNTIME` — `docker` or `podman`; defaults to `podman` locally,
//!   `docker` across ssh.
//! - `SIMA_TEST_RUN_ARGS` — space-separated container-run flags for GPU access;
//!   defaults to `--device nvidia.com/gpu=all` locally, `--gpus all` across ssh.

mod common;

use std::collections::HashSet;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use common::{
    devices_reported, journal_events, manifest_bytes, manifest_of, poll_until, sima_command,
    task_devices, write_config_text,
};
use sima_pipeline::Event;

/// The candidates and segments every remote test runs. Sized like the device
/// suite so several chains outnumber the workers and both pools pull work.
const CANDIDATES: u32 = 12;
const SEGMENTS: u64 = 3;

/// The container pool the environment names: where its container runs, the
/// image, the runtime, and its GPU-access run flags. `host` is `None` for a
/// local runtime (Tier A) and the ssh destination for a remote (Tier B).
struct ContainerEnv {
    host: Option<String>,
    image: String,
    runtime: String,
    run_args: Vec<String>,
}

impl ContainerEnv {
    /// Tier A: a container runtime on this machine. `SIMA_TEST_IMAGE` unset is
    /// the skip signal — the built worker image is the artifact under test.
    fn local() -> Option<ContainerEnv> {
        let image = std::env::var("SIMA_TEST_IMAGE").ok()?;
        Some(ContainerEnv {
            host: None,
            image,
            runtime: std::env::var("SIMA_TEST_RUNTIME").unwrap_or_else(|_| "podman".to_string()),
            run_args: run_args_or("--device nvidia.com/gpu=all"),
        })
    }

    /// Tier B: a container runtime across ssh. `SIMA_TEST_REMOTE` unset is the
    /// skip signal.
    fn remote() -> Option<ContainerEnv> {
        let host = std::env::var("SIMA_TEST_REMOTE").ok()?;
        let image = std::env::var("SIMA_TEST_IMAGE")
            .unwrap_or_else(|_| "localhost/sima-worker:latest".to_string());
        Some(ContainerEnv {
            host: Some(host),
            image,
            runtime: std::env::var("SIMA_TEST_RUNTIME").unwrap_or_else(|_| "docker".to_string()),
            run_args: run_args_or("--gpus all"),
        })
    }

    /// The `[[execution.remote]]` table for this pool, its device tables
    /// resolving `select` on the hardware its container sees. A local pool omits
    /// the `host` key, so the container runs here with no ssh hop.
    fn remote_table(&self, select: &str, workers: u32) -> String {
        let args = self
            .run_args
            .iter()
            .map(|a| format!("{a:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        let host_line = match &self.host {
            Some(host) => format!("host = {host:?}\n"),
            None => String::new(),
        };
        format!(
            r#"
        [[execution.remote]]
        {host_line}image = "{image}"
        runtime = "{runtime}"
        run_args = [{args}]

        [[execution.remote.device]]
        select = "{select}"
        workers = {workers}
    "#,
            image = self.image,
            runtime = self.runtime,
        )
    }

    /// A command over the pool's runtime with `args`, ssh-wrapped when the pool
    /// runs across a host — the same carrier the transport uses, so the test's
    /// second-channel kill reaches the same containers.
    fn runtime_command(&self, args: &[&str]) -> Command {
        match &self.host {
            Some(host) => {
                let mut command = Command::new("ssh");
                command.args(["-o", "BatchMode=yes", host, "--", &self.runtime]);
                command.args(args);
                command
            }
            None => {
                let mut command = Command::new(&self.runtime);
                command.args(args);
                command
            }
        }
    }
}

/// The `SIMA_TEST_RUN_ARGS` flags, or `default` split on whitespace.
fn run_args_or(default: &str) -> Vec<String> {
    std::env::var("SIMA_TEST_RUN_ARGS")
        .unwrap_or_else(|_| default.to_string())
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

/// The Tier A skip guard: a local container pool, or an early return.
macro_rules! local_or_skip {
    () => {
        match ContainerEnv::local() {
            Some(env) => env,
            None => {
                eprintln!("SIMA_TEST_IMAGE unset; skipping the local container acceptance test");
                return;
            }
        }
    };
}

/// The Tier B skip guard: a container pool across ssh, or an early return.
macro_rules! remote_or_skip {
    () => {
        match ContainerEnv::remote() {
            Some(env) => env,
            None => {
                eprintln!("SIMA_TEST_REMOTE unset; skipping the ssh acceptance test");
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

/// The hosts the journal's `WorkerBound` events name.
fn bound_hosts(events: &[Event]) -> HashSet<String> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::WorkerBound { host, .. } => Some(host.clone()),
            _ => None,
        })
        .collect()
}

/// Asserts no chain's segments ran across two device classes.
fn assert_no_chain_split(events: &[Event]) {
    for (task, classes) in task_devices(events) {
        let distinct: HashSet<&String> = classes.iter().collect();
        assert!(distinct.len() <= 1, "task {task} split across classes");
    }
}

// ---------------------------------------------------------------------------
// Tier A — a container pool on this machine, no ssh.
// ---------------------------------------------------------------------------

/// A mixed run over a local bare pool and a local container pool commits from
/// both, and no chain splits across the transport boundary. Both pools run on
/// this machine, so the journal cannot tell them apart by host; the two pools
/// carry distinct device classes, and commits on both prove both pulled work.
#[test]
#[ignore = "requires a container runtime and the worker image (SIMA_TEST_IMAGE)"]
fn a_mixed_bare_and_container_run_commits_from_both_pools() {
    let env = local_or_skip!();
    let dir = tempfile::tempdir().expect("temp dir");
    // The bare pool takes the Intel class; the container pool the NVIDIA class.
    let execution = format!(
        "[[execution.device]]\n        select = \"intel\"\n        workers = 2\n{}",
        env.remote_table("nvidia", 2)
    );
    let config = write_config_text(
        dir.path(),
        "mixed.toml",
        &config_text("./store", CANDIDATES, SEGMENTS, &execution),
    );
    run_to_completion(&config);

    let events = journal_events(&config);
    // Commits on two distinct device classes: the bare pool and the container
    // pool each pulled work.
    assert!(
        devices_reported(&events).len() >= 2,
        "both the bare pool and the container pool bound workers: {:?}",
        devices_reported(&events)
    );
    assert_no_chain_split(&events);
    assert!(manifest_of(&config).is_some(), "the mixed run finalized");
}

/// The same single-class run on a bare pool and on a container pool commits a
/// byte-identical manifest: both resolve to the NVIDIA class on this machine, so
/// driver parity is exact and the transport carrying an attempt never reaches
/// run identity.
#[test]
#[ignore = "requires a container runtime and the worker image (SIMA_TEST_IMAGE)"]
fn single_class_manifests_are_identical_bare_and_container() {
    let env = local_or_skip!();
    let dir = tempfile::tempdir().expect("temp dir");

    let bare = write_config_text(
        dir.path(),
        "bare.toml",
        &config_text(
            "./bare-store",
            CANDIDATES,
            SEGMENTS,
            "[[execution.device]]\n        select = \"nvidia\"\n        workers = 2",
        ),
    );
    let container = write_config_text(
        dir.path(),
        "container.toml",
        &config_text(
            "./container-store",
            CANDIDATES,
            SEGMENTS,
            &env.remote_table("nvidia", 2),
        ),
    );
    run_to_completion(&bare);
    run_to_completion(&container);

    let bare_manifest = manifest_bytes(&bare).expect("the bare run finalized");
    let container_manifest = manifest_bytes(&container).expect("the container run finalized");
    assert_eq!(
        bare_manifest, container_manifest,
        "one class, one manifest, whatever transport carried the attempts"
    );
}

/// A container killed mid-lease is a transient failure: the run converges
/// through retry to a valid manifest, and the journal records it.
#[test]
#[ignore = "requires a container runtime and the worker image (SIMA_TEST_IMAGE)"]
fn a_killed_container_converges_through_retry() {
    let env = local_or_skip!();
    converges_after_a_mid_lease_kill(&env, "kill-local.toml");
}

/// A run resumed without its container pool rebinds the chains bound to the
/// container-only class and converges — the M4.2 rebind machinery composing with
/// container pools.
#[test]
#[ignore = "requires a container runtime and the worker image (SIMA_TEST_IMAGE)"]
fn a_resume_without_the_container_pool_rebinds_loudly() {
    let env = local_or_skip!();
    let dir = tempfile::tempdir().expect("temp dir");
    // First session: a bare Intel pool and a container NVIDIA pool. Chains bind
    // across both classes.
    let with_container = format!(
        "[[execution.device]]\n        select = \"intel\"\n        workers = 2\n{}",
        env.remote_table("nvidia", 2)
    );
    let full = write_config_text(
        dir.path(),
        "full.toml",
        &config_text("./store", CANDIDATES, SEGMENTS, &with_container),
    );
    let mut child = sima_command()
        .args(["run", full.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sima");
    // Let some chains bind, then kill the orchestrator.
    let bound = poll_until(Duration::from_secs(120), || {
        journal_events(&full)
            .iter()
            .filter(|e| matches!(e, Event::WorkerBound { .. }))
            .count()
            >= 4
    });
    assert!(bound, "workers bound before the kill");
    child.kill().expect("kill the orchestrator");
    let _ = child.wait();

    // Resume over the Intel class alone: the same store, no container pool.
    // Chains bound to the absent NVIDIA class must rebind.
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
            .any(|e| matches!(e, Event::ChainRebound { .. })),
        "a chain whose class was gone rebound loudly"
    );
    assert!(
        manifest_of(&local_only).is_some(),
        "the resumed run finalized"
    );
}

// ---------------------------------------------------------------------------
// Tier B — a container pool across ssh.
// ---------------------------------------------------------------------------

/// A mixed run over a local pool and a remote container pool across ssh commits
/// from both, and no chain splits. Here the pools sit on different machines, so
/// the journal names the local pool and the ssh host separately.
#[test]
#[ignore = "requires a container runtime and the worker image on SIMA_TEST_REMOTE"]
fn a_mixed_local_and_ssh_run_commits_from_both_pools() {
    let env = remote_or_skip!();
    let host = env.host.clone().expect("Tier B names an ssh host");
    let dir = tempfile::tempdir().expect("temp dir");
    let execution = format!(
        "[[execution.device]]\n        select = \"nvidia\"\n        workers = 2\n{}",
        env.remote_table("nvidia", 2)
    );
    let config = write_config_text(
        dir.path(),
        "mixed-ssh.toml",
        &config_text("./store", CANDIDATES, SEGMENTS, &execution),
    );
    run_to_completion(&config);

    let events = journal_events(&config);
    let hosts = bound_hosts(&events);
    assert!(
        hosts.contains("") && hosts.contains(&host),
        "both the local pool and {host} bound workers: {hosts:?}",
    );
    assert_no_chain_split(&events);
    assert!(manifest_of(&config).is_some(), "the mixed run finalized");
}

/// A remote container killed mid-lease over ssh converges through retry — the
/// two-stage kill's second channel is a second ssh connection to the host.
#[test]
#[ignore = "requires a container runtime and the worker image on SIMA_TEST_REMOTE"]
fn a_killed_ssh_container_converges_through_retry() {
    let env = remote_or_skip!();
    converges_after_a_mid_lease_kill(&env, "kill-ssh.toml");
}

/// An `[[execution.remote]]` naming an unreachable host fails cleanly at run
/// start rather than hanging: `BatchMode=yes` turns an unauthenticated or
/// unreachable destination into a spawn error the image bootstrap surfaces.
#[test]
#[ignore = "requires ssh present on the machine (SIMA_TEST_REMOTE gates the tier)"]
fn an_unreachable_host_fails_cleanly() {
    let _ = remote_or_skip!();
    let dir = tempfile::tempdir().expect("temp dir");
    // A host that resolves nowhere: the bootstrap image-inspect over ssh fails
    // under BatchMode, a clean non-zero exit rather than a prompt or a hang.
    let unreachable = ContainerEnv {
        host: Some("sima-nonexistent.invalid".to_string()),
        image: "localhost/sima-worker:latest".to_string(),
        runtime: "docker".to_string(),
        run_args: vec!["--gpus".to_string(), "all".to_string()],
    };
    let config = write_config_text(
        dir.path(),
        "unreachable.toml",
        &config_text(
            "./store",
            CANDIDATES,
            SEGMENTS,
            &unreachable.remote_table("nvidia", 2),
        ),
    );
    let output = sima_command()
        .args(["run", config.to_str().expect("utf-8 path")])
        .output()
        .expect("spawn sima");
    assert!(
        !output.status.success(),
        "an unreachable remote is a clean failure, not a finalized run"
    );
    assert!(
        manifest_of(&config).is_none(),
        "no manifest for a run that never spawned a worker"
    );
}

// ---------------------------------------------------------------------------
// Shared scenario bodies.
// ---------------------------------------------------------------------------

/// Runs a container-only NVIDIA pool, kills one of its containers mid-lease
/// through the pool's own carrier, and asserts the run converges through retry
/// to a finalized manifest with the retry recorded.
fn converges_after_a_mid_lease_kill(env: &ContainerEnv, config_name: &str) {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config_text(
        dir.path(),
        config_name,
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

    // Wait until a worker container is running, then kill it — the
    // second-channel kill the remote path uses.
    let killed = poll_until(Duration::from_secs(120), || kill_one_container(env));
    assert!(killed, "a worker container was running to kill");

    let status = child.wait().expect("wait for the run");
    assert_eq!(status.code(), Some(0), "the run converged after the kill");

    let events = journal_events(&config);
    let retried = events
        .iter()
        .any(|event| matches!(event, Event::Retried { .. }));
    assert!(retried, "the killed attempt was retried");
    assert!(manifest_of(&config).is_some(), "the run finalized");
}

/// Kills one worker container whose name carries the pool's stem, best-effort,
/// through the pool's own carrier (local runtime or ssh). Returns whether a
/// container was killed.
fn kill_one_container(env: &ContainerEnv) -> bool {
    let listed = env
        .runtime_command(&["ps", "--filter", "name=sima-w-", "--format", "{{.Names}}"])
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
    env.runtime_command(&["kill", &name])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}
