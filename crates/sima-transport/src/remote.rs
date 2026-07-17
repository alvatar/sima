//! [`RemoteTransport`]: a worker inside a container runtime, optionally across
//! an ssh hop.
//!
//! The worker runs inside a container the transport launches with
//! `<runtime> run --rm -i --name <container> <run_args> <image> sima-worker`.
//! When a host is set, the whole invocation is wrapped in
//! `ssh -o BatchMode=yes <host> --`; the framed stdio protocol flows through
//! ssh, the runtime, and into the worker unchanged, so the spawn, handshake,
//! and reader machinery is the subprocess transport's, reused verbatim.
//!
//! Preemption needs a second channel. A SIGKILL of the local client — the ssh
//! process, or the runtime client when local — frees the slot, but the
//! container the client is no longer attached to keeps computing until its
//! next write breaks. So [`WorkerLink::kill`] first fires
//! `<runtime> kill <container>` (itself ssh-wrapped when remote) to stop the
//! container, then kills the local client.

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use sima_contracts::DeviceBinding;
use sima_core::Result;
use sima_model::FormatId;

use crate::link::{LinkEvent, WorkerLink, WorkerTransport};
use crate::protocol::{Assignment, Hello};
use crate::subprocess::{hello, spawn_worker};

/// The container command the worker runs as; the runtime execs it as the
/// container's entrypoint argument.
const WORKER_ENTRYPOINT: &str = "sima-worker";

/// Spawns workers inside a container runtime for one run. Each spawn launches a
/// fresh container with a unique name; when [`host`](RemoteTransport::host) is
/// set the launch and the kill both cross ssh to that destination.
pub struct RemoteTransport {
    /// The ssh destination, or `None` for a container runtime on this machine
    /// (no ssh hop).
    host: Option<String>,
    /// The container runtime client: `docker` or `podman`.
    runtime: String,
    /// The worker image to run.
    image: String,
    /// Verbatim flags for the container-run command — GPU access and the like,
    /// stated by config rather than guessed by the transport.
    run_args: Vec<String>,
    /// The stem every spawn's container name derives from; the pipeline makes
    /// it unique to the run and pool.
    container_prefix: String,
    hello: Hello,
    /// The next container-name suffix. Monotonic per transport, so a name is
    /// unique across every slot's spawns and respawns without a clock.
    counter: AtomicU64,
}

impl RemoteTransport {
    /// A transport launching `image` under `runtime` for a run over `format`
    /// with the given checkpoint cadence ([`Duration::MAX`] and `None` disable
    /// an axis). `host` is the ssh destination, or `None` for a local runtime.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: Option<String>,
        runtime: String,
        image: String,
        run_args: Vec<String>,
        container_prefix: String,
        format: FormatId,
        checkpoint_interval: Duration,
        checkpoint_interval_steps: Option<std::num::NonZeroU64>,
    ) -> RemoteTransport {
        RemoteTransport {
            host,
            runtime,
            image,
            run_args,
            container_prefix,
            hello: hello(format, checkpoint_interval, checkpoint_interval_steps),
            counter: AtomicU64::new(0),
        }
    }
}

impl WorkerTransport for RemoteTransport {
    fn spawn(&self, device: Option<&DeviceBinding>) -> Result<Box<dyn WorkerLink>> {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let container = container_name(&self.container_prefix, n);
        let run = run_argv(
            self.host.as_deref(),
            &self.runtime,
            &self.image,
            &self.run_args,
            &container,
        );
        // `run_argv` never yields an empty vector: the runtime or `ssh` is
        // always the first element.
        let (program, args) = run.split_first().expect("a non-empty command vector");
        let inner = spawn_worker(Path::new(program), args, &self.hello, device)?;
        let kill_command = kill_argv(self.host.as_deref(), &self.runtime, &container);
        Ok(Box::new(RemoteLink {
            inner,
            kill_command,
        }))
    }
}

/// A live worker whose real body is a container: the subprocess link to the
/// runtime client, plus the second-channel kill that stops the container the
/// client's death would otherwise leave running.
struct RemoteLink {
    inner: Box<dyn WorkerLink>,
    /// The container-kill argv, fired before the local kill.
    kill_command: Vec<String>,
}

impl WorkerLink for RemoteLink {
    fn device_name(&self) -> &str {
        self.inner.device_name()
    }

    fn driver(&self) -> &str {
        self.inner.driver()
    }

    fn assign(&mut self, assignment: &Assignment) -> Result<()> {
        self.inner.assign(assignment)
    }

    fn next(&mut self, deadline: Option<Instant>) -> Result<LinkEvent> {
        self.inner.next(deadline)
    }

    fn kill(&mut self) {
        // The second channel first: the container outlives its detached client,
        // so stop it before freeing the local slot. Best-effort and never
        // awaited — a dead ssh connection must not block the scheduler; the
        // local kill and the container's `--rm` are the fallback.
        fire_and_forget(&self.kill_command);
        self.inner.kill();
    }
}

/// The container name for the `n`-th spawn of a pool.
pub(crate) fn container_name(prefix: &str, n: u64) -> String {
    format!("{prefix}-{n}")
}

/// The argv that launches a worker container, ssh-wrapped when `host` is set:
/// `[ssh -o BatchMode=yes <host> --] <runtime> run --rm -i --name <container>
/// <run_args...> <image> sima-worker`.
pub(crate) fn run_argv(
    host: Option<&str>,
    runtime: &str,
    image: &str,
    run_args: &[String],
    container: &str,
) -> Vec<String> {
    let mut argv = ssh_prefix(host);
    argv.extend([
        runtime.to_string(),
        "run".to_string(),
        "--rm".to_string(),
        "-i".to_string(),
        "--name".to_string(),
        container.to_string(),
    ]);
    argv.extend(run_args.iter().cloned());
    argv.push(image.to_string());
    argv.push(WORKER_ENTRYPOINT.to_string());
    argv
}

/// The argv that force-stops a worker container, ssh-wrapped when `host` is
/// set: `[ssh -o BatchMode=yes <host> --] <runtime> kill <container>`.
pub(crate) fn kill_argv(host: Option<&str>, runtime: &str, container: &str) -> Vec<String> {
    let mut argv = ssh_prefix(host);
    argv.extend([
        runtime.to_string(),
        "kill".to_string(),
        container.to_string(),
    ]);
    argv
}

/// The argv that runs the one-shot enumeration probe in a throwaway container,
/// ssh-wrapped when `host` is set: `[ssh …] <runtime> run --rm -i <run_args>
/// <image> sima-worker --enumerate`. It carries the pool's `run_args` so the
/// probe sees the same devices the workers will. The orchestrator runs it at
/// run start to resolve a remote's device selectors.
pub fn probe_argv(
    host: Option<&str>,
    runtime: &str,
    image: &str,
    run_args: &[String],
) -> Vec<String> {
    let mut argv = ssh_prefix(host);
    argv.extend([
        runtime.to_string(),
        "run".to_string(),
        "--rm".to_string(),
        "-i".to_string(),
    ]);
    argv.extend(run_args.iter().cloned());
    argv.push(image.to_string());
    argv.push(WORKER_ENTRYPOINT.to_string());
    argv.push("--enumerate".to_string());
    argv
}

/// The argv that checks a worker image is present, ssh-wrapped when `host` is
/// set: `[ssh …] <runtime> image inspect <image>`. The orchestrator runs it
/// before spawning a remote pool, so a missing image is a clean error rather
/// than a hanging handshake.
pub fn image_inspect_argv(host: Option<&str>, runtime: &str, image: &str) -> Vec<String> {
    let mut argv = ssh_prefix(host);
    argv.extend([
        runtime.to_string(),
        "image".to_string(),
        "inspect".to_string(),
        image.to_string(),
    ]);
    argv
}

/// The ssh wrapper prefixing a remote command, or an empty vector for a local
/// runtime. `BatchMode=yes` never prompts, so an unauthenticated host is a
/// clean spawn error rather than a hang; `--` ends ssh's own options.
fn ssh_prefix(host: Option<&str>) -> Vec<String> {
    match host {
        Some(host) => vec![
            "ssh".to_string(),
            "-o".to_string(),
            "BatchMode=yes".to_string(),
            host.to_string(),
            "--".to_string(),
        ],
        None => Vec::new(),
    }
}

/// Spawns a best-effort fire-and-forget command: its streams are discarded and
/// it is never waited on. A spawn failure is ignored — the local kill and the
/// container's `--rm` are the fallback if the second channel cannot run.
fn fire_and_forget(argv: &[String]) {
    let Some((program, args)) = argv.split_first() else {
        return;
    };
    let _ = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_local_run_command_omits_the_ssh_prefix() {
        let argv = run_argv(
            None,
            "podman",
            "localhost/sima-worker:latest",
            &["--device".to_string(), "/dev/dri".to_string()],
            "sima-w-run-0",
        );
        assert_eq!(
            argv,
            [
                "podman",
                "run",
                "--rm",
                "-i",
                "--name",
                "sima-w-run-0",
                "--device",
                "/dev/dri",
                "localhost/sima-worker:latest",
                "sima-worker",
            ]
        );
    }

    #[test]
    fn a_remote_run_command_wraps_the_runtime_in_ssh() {
        let argv = run_argv(
            Some("gpubox"),
            "docker",
            "sima-worker:latest",
            &["--gpus".to_string(), "all".to_string()],
            "sima-w-run-3",
        );
        assert_eq!(
            argv,
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "gpubox",
                "--",
                "docker",
                "run",
                "--rm",
                "-i",
                "--name",
                "sima-w-run-3",
                "--gpus",
                "all",
                "sima-worker:latest",
                "sima-worker",
            ]
        );
    }

    #[test]
    fn empty_run_args_leave_name_adjacent_to_image() {
        let argv = run_argv(None, "docker", "img", &[], "c");
        // With no run flags, the image follows the container name directly.
        let name_at = argv.iter().position(|a| a == "c").expect("the name");
        assert_eq!(argv[name_at + 1], "img");
        assert_eq!(argv.last().expect("entrypoint"), "sima-worker");
    }

    #[test]
    fn a_local_kill_command_omits_the_ssh_prefix() {
        assert_eq!(
            kill_argv(None, "podman", "sima-w-run-0"),
            ["podman", "kill", "sima-w-run-0"]
        );
    }

    #[test]
    fn a_remote_kill_command_wraps_the_runtime_in_ssh() {
        // The second-channel kill the remote path fires before the local kill;
        // its argv is the observable half of the two-stage order.
        assert_eq!(
            kill_argv(Some("gpubox"), "docker", "sima-w-run-3"),
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "gpubox",
                "--",
                "docker",
                "kill",
                "sima-w-run-3",
            ]
        );
    }

    #[test]
    fn the_probe_command_appends_enumerate_after_the_run_args() {
        let argv = probe_argv(
            Some("gpubox"),
            "docker",
            "sima-worker:latest",
            &["--gpus".to_string(), "all".to_string()],
        );
        assert_eq!(
            argv,
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "gpubox",
                "--",
                "docker",
                "run",
                "--rm",
                "-i",
                "--gpus",
                "all",
                "sima-worker:latest",
                "sima-worker",
                "--enumerate",
            ]
        );
    }

    #[test]
    fn the_probe_command_omits_the_ssh_prefix_when_local() {
        let argv = probe_argv(None, "podman", "img", &[]);
        assert_eq!(
            argv,
            [
                "podman",
                "run",
                "--rm",
                "-i",
                "img",
                "sima-worker",
                "--enumerate"
            ]
        );
    }

    #[test]
    fn the_image_inspect_command_wraps_in_ssh_when_remote() {
        assert_eq!(
            image_inspect_argv(Some("gpubox"), "docker", "img:tag"),
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "gpubox",
                "--",
                "docker",
                "image",
                "inspect",
                "img:tag",
            ]
        );
        assert_eq!(
            image_inspect_argv(None, "podman", "img:tag"),
            ["podman", "image", "inspect", "img:tag"]
        );
    }

    #[test]
    fn container_names_are_unique_across_spawns() {
        // A monotonic suffix per spawn: a respawn never collides with a prior
        // container that may still be shutting down.
        let names: Vec<String> = (0..3).map(|n| container_name("sima-w-run", n)).collect();
        assert_eq!(names, ["sima-w-run-0", "sima-w-run-1", "sima-w-run-2"]);
    }
}
