//! [`FleetTransport`]: a worker on a rented instance, reached over ssh, whose
//! target the orchestrator can swap under a running pool.
//!
//! ssh lands inside the instance's container, so the worker runs as the ssh
//! command directly — no container-run wrapper, unlike [`RemoteTransport`].
//! The framed stdio protocol flows through ssh into the worker unchanged, so
//! the spawn, handshake, and reader machinery is the subprocess transport's,
//! reused verbatim through [`spawn_worker`].
//!
//! The transport's target is a small state machine the supervisor drives:
//!
//! - `Live(SshTarget)` — spawn builds the ssh argv and proceeds.
//! - `Replacing` — spawn blocks on a condvar until the target changes, so a
//!   worker thread waits out an instance replacement instead of spawning
//!   against a dead host.
//! - `Retired { fatal }` — spawn reports retirement rather than a worker.
//!   `fatal` distinguishes strict fill, where the run must fault, from
//!   best-effort degradation, where the worker thread exits cleanly.
//!
//! Killing a worker closes the connection: a SIGKILL of the local ssh client
//! ends the session, sshd tears the remote process down, and the worker's
//! `PR_SET_PDEATHSIG` is the backstop. There is no per-worker remote kill
//! channel; a wedged remote process is ultimately bounded by destroying the
//! instance at teardown. So the subprocess link's own `kill` — which kills the
//! local ssh client — is the whole kill, and no wrapper is needed.
//!
//! The stub-provider testing path is the same transport in [`FleetMode::Local`]:
//! it spawns a `sima-worker` binary directly with no ssh hop, so every layer
//! above the transport exercises identically without a network.

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use sima_contracts::DeviceBinding;
use sima_core::Result;
use sima_model::FormatId;
use sima_trace::Emitter;

use crate::link::{SpawnOutcome, WorkerTransport};
use crate::protocol::Hello;
use crate::subprocess::{EventContext, hello, spawn_worker};

/// The command a fleet worker runs as: `sima-worker`, over ssh the remote
/// command, in local mode the binary's own name is its path instead.
const WORKER_ENTRYPOINT: &str = "sima-worker";

/// An ssh destination on a rented instance: the host, port, and login user.
/// A plain value defined here so the transport never depends on the provider
/// crate; the pipeline maps a provider `SshEndpoint` into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshTarget {
    /// The instance's host or address.
    pub host: String,
    /// The ssh port.
    pub port: u16,
    /// The login user; `root` for a Vast instance, where ssh lands inside the
    /// container.
    pub user: String,
}

/// How a fleet transport reaches its worker.
#[derive(Debug, Clone)]
pub enum FleetMode {
    /// ssh to a rented instance; the worker runs as the ssh command.
    Ssh,
    /// Spawn the `sima-worker` binary at this path directly, no ssh hop — the
    /// stub-provider testing path.
    Local(PathBuf),
}

/// Where a fleet transport currently sends its workers, and the lifecycle of
/// that target. `spawn` reads it; the supervisor swaps it.
enum TargetState {
    /// The instance to spawn on. In [`FleetMode::Local`] the endpoint is
    /// carried for uniformity but the local spawn ignores it.
    Live(SshTarget),
    /// An instance is being replaced: `spawn` blocks until the target settles.
    Replacing,
    /// The transport has retired: `spawn` reports it rather than a worker.
    Retired {
        /// Whether the retirement must fault the run.
        fatal: bool,
    },
}

/// What awaiting a spawnable target resolved to: an instance to spawn on, or a
/// retirement to report.
enum Spawnable {
    Live(SshTarget),
    Retired { fatal: bool },
}

/// Spawns workers on a rented instance whose ssh target the supervisor can
/// swap under the running pool. One transport serves one instance's pool.
pub struct FleetTransport {
    mode: FleetMode,
    /// The current target and its lifecycle, guarded so the supervisor's swaps
    /// and the worker threads' spawns serialize.
    state: Mutex<TargetState>,
    /// Signals a target change to a `spawn` blocked in `Replacing`.
    settled: Condvar,
    hello: Hello,
}

impl FleetTransport {
    /// A transport spawning workers on `initial` under `mode`, for a run over
    /// `format` with the given checkpoint cadence ([`Duration::MAX`] and `None`
    /// disable an axis).
    pub fn new(
        mode: FleetMode,
        initial: SshTarget,
        format: FormatId,
        checkpoint_interval: Duration,
        checkpoint_interval_steps: Option<NonZeroU64>,
    ) -> FleetTransport {
        FleetTransport {
            mode,
            state: Mutex::new(TargetState::Live(initial)),
            settled: Condvar::new(),
            hello: hello(format, checkpoint_interval, checkpoint_interval_steps),
        }
    }

    /// Marks the current instance as being replaced: spawns block until the
    /// next `swap_to_live` or `retire`. A no-op once retired — a retirement is
    /// terminal.
    pub fn mark_replacing(&self) {
        let mut state = self.lock();
        if !matches!(*state, TargetState::Retired { .. }) {
            *state = TargetState::Replacing;
        }
        // No notify: nothing a blocked spawn should proceed on yet.
    }

    /// Swaps the target to `target` and releases any spawn blocked while
    /// replacing. A no-op once retired.
    pub fn swap_to_live(&self, target: SshTarget) {
        let mut state = self.lock();
        if !matches!(*state, TargetState::Retired { .. }) {
            *state = TargetState::Live(target);
            self.settled.notify_all();
        }
    }

    /// Retires the transport, releasing every blocked spawn with the
    /// retirement. Terminal: no later swap revives it.
    pub fn retire(&self, fatal: bool) {
        let mut state = self.lock();
        *state = TargetState::Retired { fatal };
        self.settled.notify_all();
    }

    /// Blocks while the target is `Replacing`, then resolves to the live
    /// instance to spawn on or the retirement to report.
    fn await_spawnable(&self) -> Spawnable {
        let mut state = self.lock();
        loop {
            match &*state {
                TargetState::Live(target) => return Spawnable::Live(target.clone()),
                TargetState::Retired { fatal } => return Spawnable::Retired { fatal: *fatal },
                TargetState::Replacing => {
                    state = self
                        .settled
                        .wait(state)
                        .expect("the fleet target lock is never poisoned");
                }
            }
        }
    }

    /// The lock over the target state, panicking on poisoning — a poisoned
    /// lock means a prior holder panicked, which is a bug, not a runtime fault.
    fn lock(&self) -> std::sync::MutexGuard<'_, TargetState> {
        self.state
            .lock()
            .expect("the fleet target lock is never poisoned")
    }
}

impl WorkerTransport for FleetTransport {
    fn spawn(
        &self,
        worker: u64,
        device: Option<&DeviceBinding>,
        events: Emitter,
    ) -> Result<SpawnOutcome> {
        let target = match self.await_spawnable() {
            Spawnable::Live(target) => target,
            Spawnable::Retired { fatal } => return Ok(SpawnOutcome::Retired { fatal }),
        };
        let argv = self.mode.spawn_argv(&target);
        // `spawn_argv` never yields an empty vector: `ssh` or the binary path is
        // always the first element.
        let (program, args) = argv.split_first().expect("a non-empty command vector");
        // The host label attributes the child's forwarded events: the instance
        // host over ssh, empty for a local spawn on this machine.
        let context = EventContext {
            events,
            worker,
            host: self.mode.host_label(&target),
        };
        let link = spawn_worker(
            Path::new(program),
            args,
            &self.hello,
            worker,
            device,
            context,
        )?;
        Ok(SpawnOutcome::Link(link))
    }
}

impl FleetMode {
    /// The argv that spawns a worker on `target`: the ssh invocation, or the
    /// local binary directly.
    fn spawn_argv(&self, target: &SshTarget) -> Vec<String> {
        match self {
            FleetMode::Ssh => ssh_argv(target, false),
            FleetMode::Local(program) => vec![program.to_string_lossy().into_owned()],
        }
    }

    /// The host label the child's events are attributed under: the instance
    /// host over ssh, empty for a local spawn.
    fn host_label(&self, target: &SshTarget) -> String {
        match self {
            FleetMode::Ssh => target.host.clone(),
            FleetMode::Local(_) => String::new(),
        }
    }
}

/// The argv that runs `sima-worker` on a fleet instance over ssh:
/// `ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -p <port>
/// <user>@<host> -- sima-worker`, with `--enumerate` appended when `probe` is
/// set.
///
/// `StrictHostKeyChecking=accept-new` accepts a freshly provisioned host's key
/// on first contact and pins it afterwards — the trust model for disposable
/// machines never present in `known_hosts`. `BatchMode=yes` never prompts, so
/// an unreachable host is a clean spawn error rather than a hang.
pub fn ssh_argv(target: &SshTarget, probe: bool) -> Vec<String> {
    let mut argv = vec![
        "ssh".to_string(),
        "-o".to_string(),
        "BatchMode=yes".to_string(),
        "-o".to_string(),
        "StrictHostKeyChecking=accept-new".to_string(),
        "-p".to_string(),
        target.port.to_string(),
        format!("{}@{}", target.user, target.host),
        "--".to_string(),
        WORKER_ENTRYPOINT.to_string(),
    ];
    if probe {
        argv.push("--enumerate".to_string());
    }
    argv
}

/// The argv that enumerates devices for a fleet instance, so the orchestrator
/// derives one worker slot per GPU: the ssh spawn argv with `--enumerate`, or
/// the local binary with `--enumerate` in [`FleetMode::Local`].
pub fn probe_argv(mode: &FleetMode, target: &SshTarget) -> Vec<String> {
    match mode {
        FleetMode::Ssh => ssh_argv(target, true),
        FleetMode::Local(program) => vec![
            program.to_string_lossy().into_owned(),
            "--enumerate".to_string(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    fn a_target() -> SshTarget {
        SshTarget {
            host: "203.0.113.7".to_string(),
            port: 41022,
            user: "root".to_string(),
        }
    }

    fn a_transport(mode: FleetMode) -> FleetTransport {
        FleetTransport::new(
            mode,
            a_target(),
            FormatId::new("stub.v1").expect("format id"),
            Duration::MAX,
            None,
        )
    }

    #[test]
    fn the_ssh_spawn_argv_carries_the_disposable_host_key_policy() {
        assert_eq!(
            ssh_argv(&a_target(), false),
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-p",
                "41022",
                "root@203.0.113.7",
                "--",
                "sima-worker",
            ]
        );
    }

    #[test]
    fn the_ssh_probe_argv_appends_enumerate() {
        let argv = ssh_argv(&a_target(), true);
        assert_eq!(argv.last().expect("a last element"), "--enumerate");
        // Otherwise identical to the spawn argv.
        assert_eq!(&argv[..argv.len() - 1], ssh_argv(&a_target(), false));
    }

    #[test]
    fn a_local_mode_spawn_argv_is_the_bare_binary() {
        let mode = FleetMode::Local(PathBuf::from("/opt/sima/sima-worker"));
        assert_eq!(mode.spawn_argv(&a_target()), ["/opt/sima/sima-worker"]);
    }

    #[test]
    fn a_local_mode_probe_argv_appends_enumerate_to_the_bare_binary() {
        let mode = FleetMode::Local(PathBuf::from("/opt/sima/sima-worker"));
        assert_eq!(
            probe_argv(&mode, &a_target()),
            ["/opt/sima/sima-worker", "--enumerate"]
        );
    }

    #[test]
    fn a_spawn_blocks_while_replacing_and_releases_on_a_swap() {
        let transport = Arc::new(a_transport(FleetMode::Ssh));
        transport.mark_replacing();
        let (tx, rx) = mpsc::channel();
        let waiter = {
            let transport = Arc::clone(&transport);
            std::thread::spawn(move || {
                // The spawnable wait is the blocking half of `spawn`, isolated
                // so the test never spawns a real ssh process.
                let spawnable = transport.await_spawnable();
                tx.send(matches!(spawnable, Spawnable::Live(_)))
                    .expect("send the outcome");
            })
        };
        // While replacing, the waiter makes no progress.
        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "the spawn is blocked while replacing"
        );
        let replacement = SshTarget {
            host: "198.51.100.9".to_string(),
            port: 50022,
            user: "root".to_string(),
        };
        transport.swap_to_live(replacement.clone());
        assert!(
            rx.recv_timeout(Duration::from_secs(5))
                .expect("the swap releases the spawn"),
            "the released spawn saw a live target"
        );
        waiter.join().expect("the waiter thread joins");
        // The swapped target is what a subsequent spawn would build against.
        match transport.await_spawnable() {
            Spawnable::Live(target) => assert_eq!(target, replacement),
            Spawnable::Retired { .. } => panic!("expected a live target after the swap"),
        }
    }

    #[test]
    fn a_retire_releases_a_blocked_spawn_with_the_retirement() {
        let transport = Arc::new(a_transport(FleetMode::Ssh));
        transport.mark_replacing();
        let (tx, rx) = mpsc::channel();
        {
            let transport = Arc::clone(&transport);
            std::thread::spawn(move || {
                let spawnable = transport.await_spawnable();
                let outcome = match spawnable {
                    Spawnable::Retired { fatal } => Some(fatal),
                    Spawnable::Live(_) => None,
                };
                tx.send(outcome).expect("send the outcome");
            });
        }
        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "the spawn is blocked while replacing"
        );
        transport.retire(true);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5))
                .expect("the retirement releases the spawn"),
            Some(true),
            "the released spawn saw a fatal retirement"
        );
    }

    #[test]
    fn a_swap_after_retirement_does_not_revive_the_transport() {
        let transport = a_transport(FleetMode::Ssh);
        transport.retire(false);
        transport.swap_to_live(a_target());
        // Retirement is terminal: a later swap is ignored.
        match transport.await_spawnable() {
            Spawnable::Retired { fatal } => assert!(!fatal),
            Spawnable::Live(_) => panic!("a swap must not revive a retired transport"),
        }
    }
}
