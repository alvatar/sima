//! [`ContainerTransport`]: a worker inside a container the transport launches,
//! here or across an ssh hop.
//!
//! The worker runs inside a container the transport launches with
//! `<runtime> run --rm -i --name <container> <run_args> <image> <command>`,
//! where the command is the image's own `sima-worker` or a program delivered
//! to the machine — see [`ContainerRun`].
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
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use sima_contracts::DeviceBinding;
use sima_core::{Result, own_process_group};
use sima_trace::Emitter;

use crate::device_probe::DeviceProbe;
use crate::link::{LinkEvent, SpawnOutcome, WORKER_ENTRYPOINT, WorkerLink, WorkerTransport};
use crate::protocol::Assignment;
use crate::spawn_settings::SpawnSettings;
use crate::ssh::SshDestination;
use crate::subprocess::{EventContext, spawn_worker};

/// Spawns workers inside a container runtime for one run. Each spawn launches a
/// fresh container with a unique name; when [`host`](ContainerTransport::host) is
/// set the launch and the kill both cross ssh to that destination.
pub struct ContainerTransport {
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
    /// What each spawn's container runs: the image's own worker, or the program
    /// delivered to this machine, with whatever that needs mounted and
    /// forwarded.
    run: ContainerRun,
    settings: SpawnSettings,
    /// The next container-name suffix. Monotonic per transport, so a name is
    /// unique across every slot's spawns and respawns without a clock.
    counter: AtomicU64,
}

impl ContainerTransport {
    /// A transport launching `image` under `runtime` to perform `run`, spawning
    /// its clients under `settings`. `host` is the ssh destination, or `None`
    /// for a local runtime.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: Option<String>,
        runtime: String,
        image: String,
        run_args: Vec<String>,
        container_prefix: String,
        run: ContainerRun,
        settings: SpawnSettings,
    ) -> ContainerTransport {
        ContainerTransport {
            host,
            runtime,
            image,
            run_args,
            container_prefix,
            run,
            settings,
            counter: AtomicU64::new(0),
        }
    }
}

impl WorkerTransport for ContainerTransport {
    fn spawn(
        &self,
        worker: u64,
        device: Option<&DeviceBinding>,
        events: Emitter,
    ) -> Result<SpawnOutcome> {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let container = container_name(&self.container_prefix, n);
        let launch = run_argv(
            self.host.as_deref(),
            &self.runtime,
            &self.image,
            &self.run_args,
            &container,
            &self.run,
        );
        // `run_argv` never yields an empty vector: the runtime or `ssh` is
        // always the first element.
        let (program, args) = launch.split_first().expect("a non-empty command vector");
        // The pool's host label: the ssh destination, or empty for a container
        // on this machine — the same value the pool journals in WorkerBound.
        // ssh carries the container's stderr to the client's stderr pipe, so
        // the capture and Event forwarding are the subprocess machinery's.
        let context = EventContext {
            events,
            worker,
            host: self.host.clone().unwrap_or_default(),
        };
        let inner = spawn_worker(
            Path::new(program),
            args,
            &self.settings,
            worker,
            device,
            context,
        )?;
        let kill_command = kill_argv(self.host.as_deref(), &self.runtime, &container);
        Ok(SpawnOutcome::Link(Box::new(ContainerLink {
            inner,
            kill_command,
            killers: Vec::new(),
        })))
    }
}

/// A live worker whose real body is a container: the subprocess link to the
/// runtime client, plus the second-channel kill that stops the container the
/// client's death would otherwise leave running.
struct ContainerLink {
    inner: Box<dyn WorkerLink>,
    /// The container-kill argv, fired before the local kill.
    kill_command: Vec<String>,
    /// The kill clients this link has fired, held so they are reaped rather
    /// than left as zombies. One preemption is one client; a run that preempts
    /// often would otherwise accumulate one entry in the process table per
    /// preemption for as long as the orchestrator lives.
    killers: Vec<Child>,
}

impl WorkerLink for ContainerLink {
    fn device_name(&self) -> &str {
        self.inner.device_name()
    }

    fn driver(&self) -> &str {
        self.inner.driver()
    }

    fn program(&self) -> &str {
        self.inner.program()
    }

    fn assign(&mut self, assignment: &Assignment) -> Result<()> {
        self.inner.assign(assignment)
    }

    fn next(&mut self, deadline: Option<Instant>) -> Result<LinkEvent> {
        self.inner.next(deadline)
    }

    fn kill(&mut self) {
        // The second channel first: the container outlives its detached client,
        // so stop it before freeing the local slot. Never awaited — a dead ssh
        // connection must not block the scheduler; the local kill and the
        // container's `--rm` are the fallback. The client is kept rather than
        // dropped, so it is reaped instead of becoming a zombie.
        self.reap_finished_killers();
        if let Some(killer) = spawn_detached(&self.kill_command) {
            self.killers.push(killer);
        }
        self.inner.kill();
    }
}

impl ContainerLink {
    /// Reaps every kill client that has already exited, leaving the ones still
    /// running. Called where a new one is spawned, so the held set stays the
    /// size of what is actually in flight rather than the count of preemptions.
    fn reap_finished_killers(&mut self) {
        self.killers
            .retain_mut(|killer| !matches!(killer.try_wait(), Ok(Some(_)) | Err(_)));
    }
}

impl Drop for ContainerLink {
    /// Settles whatever kill clients are still running. Each is given a short
    /// bound to finish its own work — an ssh hop to stop a container — and then
    /// killed, because a link drop happens where a worker slot is being freed
    /// and must not wait on a hop that will not answer.
    fn drop(&mut self) {
        let deadline = Instant::now() + KILLER_REAP_BOUND;
        for killer in &mut self.killers {
            while Instant::now() < deadline {
                match killer.try_wait() {
                    Ok(Some(_)) | Err(_) => break,
                    Ok(None) => std::thread::sleep(KILLER_REAP_POLL),
                }
            }
            let _ = killer.kill();
            let _ = killer.wait();
        }
    }
}

/// How long a link drop waits for its kill clients, in total.
const KILLER_REAP_BOUND: Duration = Duration::from_secs(2);

/// How often that wait looks at a client.
const KILLER_REAP_POLL: Duration = Duration::from_millis(20);

/// The container name for the `n`-th spawn of a pool.
pub(crate) fn container_name(prefix: &str, n: u64) -> String {
    format!("{prefix}-{n}")
}

/// The argv that launches a worker container, ssh-wrapped when `host` is set:
/// `[ssh -o BatchMode=yes <host> --] <runtime> run --rm -i --name <container>
/// [-v <mount>…] [--env <name>…] <run_args…> <image> <command…>`.
///
/// The name is what the kill channel addresses; everything after it is
/// [`ContainerRun`]'s.
pub(crate) fn run_argv(
    host: Option<&str>,
    runtime: &str,
    image: &str,
    run_args: &[String],
    container: &str,
    run: &ContainerRun,
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
    argv.extend(run.flags());
    argv.extend(run_args.iter().cloned());
    argv.push(image.to_string());
    argv.extend(run.command.iter().cloned());
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

/// What a container runs, and what it must see to run it.
///
/// A run whose format this build carries runs the image's own worker and needs
/// nothing mounted. A run whose format is a program outside this build runs
/// that program out of the machine's own filesystem, which the container has to
/// be given: the mount is stated as `<path>:<path>`, the identical path on both
/// sides, so a path naming a file outside names the same file inside — which is
/// what lets a stamp written by one container be read by the next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerRun {
    /// Bind mounts, each already in the runtime's `<host>:<container>` form.
    mounts: Vec<String>,
    /// Variable names forwarded from the machine's own environment. Only the
    /// names travel: the runtime reads each value where the container runs, so
    /// no value ever appears on a command line or crosses the wire.
    env: Vec<String>,
    /// The command the container runs, from its program onward.
    command: Vec<String>,
}

impl ContainerRun {
    /// The image's own worker with `args`, mounting nothing and forwarding
    /// nothing — everything it needs is in the image.
    pub fn worker(args: Vec<String>) -> ContainerRun {
        let mut command = vec![WORKER_ENTRYPOINT.to_string()];
        command.extend(args);
        ContainerRun {
            mounts: Vec::new(),
            env: Vec::new(),
            command,
        }
    }

    /// A command of the caller's, over the machine paths `mounts` names, with
    /// `env` forwarded by name.
    pub fn program(mounts: Vec<String>, env: Vec<String>, command: Vec<String>) -> ContainerRun {
        ContainerRun {
            mounts,
            env,
            command,
        }
    }

    /// The runtime flags this run needs before the pool's own arguments.
    fn flags(&self) -> Vec<String> {
        let mut flags = Vec::new();
        for mount in &self.mounts {
            flags.extend(["-v".to_string(), mount.clone()]);
        }
        for name in &self.env {
            flags.extend(["--env".to_string(), name.clone()]);
        }
        flags
    }
}

/// The argv that runs one command in a throwaway container, ssh-wrapped when
/// `host` is set: `[ssh …] <runtime> run --rm -i [-v <mount>…] <run_args>
/// <image> <command…>`.
///
/// The pool's `run_args` come last before the image, so a machine's own
/// configuration is what the runtime reads last, and the container sees the
/// same devices the pool's workers will.
pub fn once_argv(
    host: Option<&str>,
    runtime: &str,
    image: &str,
    run_args: &[String],
    run: &ContainerRun,
) -> Vec<String> {
    let mut argv = ssh_prefix(host);
    argv.extend([
        runtime.to_string(),
        "run".to_string(),
        "--rm".to_string(),
        "-i".to_string(),
    ]);
    argv.extend(run.flags());
    argv.extend(run_args.iter().cloned());
    argv.push(image.to_string());
    argv.extend(run.command.iter().cloned());
    argv
}

/// The argv that runs the one-shot enumeration probe in a throwaway container:
/// [`once_argv`] over the image's own worker and whatever [`DeviceProbe`] asks.
/// The orchestrator runs it at run start to resolve a remote's device
/// selectors.
pub fn probe_argv(
    host: Option<&str>,
    runtime: &str,
    image: &str,
    run_args: &[String],
    probe: DeviceProbe,
) -> Vec<String> {
    once_argv(
        host,
        runtime,
        image,
        run_args,
        &ContainerRun::worker(probe.args()),
    )
}

/// The argv that checks a worker image is present, ssh-wrapped when `host` is
/// set: `[ssh …] <runtime> image inspect <image>`. The orchestrator runs it
/// before spawning a container pool, so a missing image is a clean error rather
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

/// The ssh wrapper prefixing a command on another machine, or an empty vector
/// for a container runtime here. The destination builds its own invocation, so
/// this is the one place the container transport decides whether there is a hop
/// at all.
fn ssh_prefix(host: Option<&str>) -> Vec<String> {
    match host {
        Some(host) => SshDestination::known(host).prefix(),
        None => Vec::new(),
    }
}

/// Spawns a best-effort command with its streams discarded, handing the child
/// back so the caller can reap it rather than orphaning it.
///
/// A spawn failure is `None` and is ignored by the caller — the local kill and
/// the container's `--rm` are the fallback if the second channel cannot run.
fn spawn_detached(argv: &[String]) -> Option<Child> {
    let (program, args) = argv.split_first()?;
    own_process_group(&mut Command::new(program))
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

#[cfg(test)]
mod tests {
    use sima_model::FormatId;

    use super::*;

    #[test]
    fn a_local_run_command_omits_the_ssh_prefix() {
        let argv = run_argv(
            None,
            "podman",
            "localhost/sima:latest",
            &["--device".to_string(), "/dev/dri".to_string()],
            "sima-w-run-0",
            &ContainerRun::worker(Vec::new()),
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
                "localhost/sima:latest",
                "sima-worker",
            ]
        );
    }

    #[test]
    fn a_remote_run_command_wraps_the_runtime_in_ssh() {
        let argv = run_argv(
            Some("gpubox"),
            "docker",
            "sima:latest",
            &["--gpus".to_string(), "all".to_string()],
            "sima-w-run-3",
            &ContainerRun::worker(Vec::new()),
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
                "sima:latest",
                "sima-worker",
            ]
        );
    }

    #[test]
    fn a_worker_run_of_a_delivered_program_mounts_it_and_forwards_its_variables() {
        // A machine that received a program runs it out of its own filesystem,
        // so the tree is mounted; the entry's variables are forwarded by name,
        // so each value is that machine's own and none reaches a command line.
        let argv = run_argv(
            Some("gpubox"),
            "docker",
            "sima:latest",
            &["--gpus".to_string(), "all".to_string()],
            "sima-w-run-1",
            &ContainerRun::program(
                vec!["/srv/programs:/srv/programs".to_string()],
                vec!["HF_TOKEN".to_string(), "CACHE".to_string()],
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "exec ./program".to_string(),
                ],
            ),
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
                "sima-w-run-1",
                "-v",
                "/srv/programs:/srv/programs",
                "--env",
                "HF_TOKEN",
                "--env",
                "CACHE",
                "--gpus",
                "all",
                "sima:latest",
                "sh",
                "-c",
                "exec ./program",
            ]
        );
    }

    #[test]
    fn empty_run_args_leave_name_adjacent_to_image() {
        let argv = run_argv(
            None,
            "docker",
            "img",
            &[],
            "c",
            &ContainerRun::worker(Vec::new()),
        );
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
            "sima:latest",
            &["--gpus".to_string(), "all".to_string()],
            DeviceProbe::Format(&FormatId::new("ca_evolution.gray_scott.v1").expect("format id")),
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
                "sima:latest",
                "sima-worker",
                "--enumerate-devices",
                "ca_evolution.gray_scott.v1",
            ]
        );
    }

    #[test]
    fn the_probe_command_omits_the_ssh_prefix_when_local() {
        let argv = probe_argv(
            None,
            "podman",
            "img",
            &[],
            DeviceProbe::Format(&FormatId::new("stub.v1").expect("format id")),
        );
        assert_eq!(
            argv,
            [
                "podman",
                "run",
                "--rm",
                "-i",
                "img",
                "sima-worker",
                "--enumerate-devices",
                "stub.v1"
            ]
        );
    }

    #[test]
    fn a_command_run_carries_its_mounts_before_the_pool_s_own_arguments() {
        // The delivery and the registered-format probe both run a command of
        // sima's own in a throwaway container. The mount is the transport's,
        // the run args are the pool's, and the pool's come last so a machine's
        // configuration is what the runtime sees last.
        let argv = once_argv(
            Some("gpubox"),
            "docker",
            "sima:latest",
            &["--gpus".to_string(), "all".to_string()],
            &ContainerRun::program(
                vec!["/srv/programs:/srv/programs".to_string()],
                Vec::new(),
                vec![
                    "sima".to_string(),
                    "sync-serve".to_string(),
                    "/srv/programs".to_string(),
                ],
            ),
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
                "-v",
                "/srv/programs:/srv/programs",
                "--gpus",
                "all",
                "sima:latest",
                "sima",
                "sync-serve",
                "/srv/programs",
            ]
        );
    }

    #[test]
    fn a_worker_run_names_the_image_s_own_worker_and_mounts_nothing() {
        // The builtin path, byte-identical: no mount, and the command is the
        // entry point the image carries.
        assert_eq!(
            once_argv(
                None,
                "podman",
                "img",
                &[],
                &ContainerRun::worker(Vec::new())
            ),
            ["podman", "run", "--rm", "-i", "img", "sima-worker"]
        );
    }

    #[test]
    fn a_format_free_probe_command_ends_at_the_flag() {
        // A registered format's readiness probe: the image's worker cannot
        // resolve that format, so the probe names none.
        let argv = probe_argv(
            Some("gpubox"),
            "docker",
            "sima:latest",
            &["--gpus".to_string()],
            DeviceProbe::EveryBackend,
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
                "sima:latest",
                "sima-worker",
                "--enumerate-devices",
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
