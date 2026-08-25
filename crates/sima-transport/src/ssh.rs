//! [`SshTransport`]: a worker launched as the ssh command itself, whose
//! destination the orchestrator can swap under a running pool.
//!
//! The worker is the command ssh executes, with no container-run wrapper: on a
//! rented machine ssh already lands inside the machine's own container, so
//! nesting a second one would have nothing to add. The framed stdio protocol
//! flows through ssh into the worker unchanged, so the spawn, handshake, and
//! reader machinery is the subprocess transport's, reused verbatim through
//! [`spawn_worker`].
//!
//! The transport's destination is a small state machine the supervisor drives:
//!
//! - `Live(SshDestination)` — spawn builds the ssh argv and proceeds.
//! - `Replacing` — spawn blocks on a condvar until the target changes, so a
//!   worker thread waits out an instance replacement instead of spawning
//!   against a dead host.
//! - `Retired { fatal }` — spawn reports retirement rather than a worker.
//!   `fatal` distinguishes strict fill, where the run must fault, from
//!   best-effort degradation, where the worker thread exits cleanly.
//!
//! A spawn resolves to one of three outcomes, not two: a live [`WorkerLink`],
//! a retirement, or — as an ssh-mode spawn failure — a wait-and-retry that
//! bridges the window between an instance dying and the supervisor swapping a
//! replacement in. A worker's child dies with its instance and the worker loop
//! respawns at once, up to a heartbeat before the supervisor notices; without
//! the retry the respawn's ssh to the dead host would fail and fault the run.
//! An ssh spawn failure therefore waits — bounded by the readiness timeout the
//! machine was acquired under, paced by its poll — retrying the same target until
//! the supervisor swaps a replacement in (which restarts the attempt on the new
//! host with a fresh bound) or retires the transport. A target that stays dead
//! past the bound faults the run, the same outcome as failing fast, delayed by
//! the bound. Local mode never retries: a local spawn failure is the worker's
//! own, with no supervisor swapping a replacement behind it.
//!
//! Killing a worker closes the connection: a SIGKILL of the local ssh client
//! ends the session, sshd tears the remote process down, and the worker's
//! `PR_SET_PDEATHSIG` is the backstop. There is no per-worker remote kill
//! channel; a wedged remote process is ultimately bounded by destroying the
//! instance at teardown. So the subprocess link's own `kill` — which kills the
//! local ssh client — is the whole kill, and no wrapper is needed.
//!
//! The stub-provider testing path is the same transport in [`SpawnMode::Local`]:
//! it spawns a `sima-worker` binary directly with no ssh hop, so every layer
//! above the transport exercises identically without a network.

use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sima_contracts::DeviceBinding;
use sima_core::Result;
use sima_model::FormatId;
use sima_trace::Emitter;

use crate::link::{SpawnOutcome, WORKER_ENTRYPOINT, WorkerLink, WorkerTransport};
use crate::spawn_settings::SpawnSettings;
use crate::subprocess::{EventContext, spawn_worker};

/// Where an ssh command lands, and the trust policy for getting there. Named
/// for the TOML key it answers to. A plain value defined here so the transport
/// never depends on the provider crate; the pipeline maps a provider
/// `SshEndpoint` into it.
///
/// Two policies, one per constructor, because the two kinds of destination
/// differ in what ssh already knows about them:
///
/// - [`SshDestination::known`] — a destination the operator configured. The
///   local ssh configuration supplies the port, the user, and the key, and the
///   host is already in `known_hosts`, so the invocation adds nothing but
///   `BatchMode=yes`.
/// - [`SshDestination::rented`] — a machine that did not exist a minute ago.
///   Its port and user are stated explicitly, its key is accepted on first
///   contact and pinned afterwards, and the connection wait is bounded.
///
/// The fields are private so a caller cannot assemble a third policy by hand:
/// every ssh command line in the workspace comes from
/// [`SshDestination::prefix`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshDestination {
    /// The destination as ssh resolves it: an alias, a `user@host`, or an
    /// address.
    host: String,
    /// The port, login user, and first-contact policy of a machine ssh knows
    /// nothing about yet; `None` for a destination the operator configured.
    fresh: Option<Fresh>,
}

/// What a destination ssh has never seen needs stated on the command line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fresh {
    /// The ssh port.
    port: u16,
    /// The login user; `root` for a Vast instance, where ssh lands inside the
    /// container.
    user: String,
}

impl SshDestination {
    /// A destination the operator already configured: an alias or a `user@host`
    /// the local ssh configuration resolves, whose key is already trusted.
    pub fn known(host: impl Into<String>) -> SshDestination {
        SshDestination {
            host: host.into(),
            fresh: None,
        }
    }

    /// A machine rented for the run, reached at an explicit port as an explicit
    /// user, whose key is accepted on first contact.
    pub fn rented(host: impl Into<String>, port: u16, user: impl Into<String>) -> SshDestination {
        SshDestination {
            host: host.into(),
            fresh: Some(Fresh {
                port,
                user: user.into(),
            }),
        }
    }

    /// The destination as ssh resolves it — the label a pool's workers are
    /// journaled under.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The ssh invocation that reaches this destination, up to and including
    /// the `--` that ends ssh's own options. Every ssh command line in the
    /// workspace starts here; the caller appends the remote command.
    ///
    /// `BatchMode=yes` never prompts, so an unauthenticated or unreachable
    /// destination is a clean spawn error rather than a hang.
    ///
    /// A fresh destination — a machine rented for this run — adds three more.
    /// `StrictHostKeyChecking=accept-new` accepts its key on first contact
    /// without prompting, the trust model for a machine never present in
    /// `known_hosts`. `UserKnownHostsFile=/dev/null` keeps that key out of the
    /// operator's file: a rental's key lives as long as the rental, so
    /// remembering it accumulates entries nothing ever removes, and a later
    /// rental at a reused address with a key of its own would then be refused
    /// and fail the run. What is given up is detecting a key change within one
    /// rental, which `accept-new` against an empty file could never have caught
    /// on first contact either. `ConnectTimeout` bounds a host that drops
    /// packets, which would otherwise stall for the kernel's TCP timeout instead
    /// of failing inside the caller's own bounds. `LogLevel=ERROR` drops ssh's
    /// own first-contact notice, which every connection to a rental would
    /// otherwise print; a far side's diagnostics come from the remote command
    /// and are unaffected.
    pub fn prefix(&self) -> Vec<String> {
        let mut argv = vec![
            "ssh".to_string(),
            "-o".to_string(),
            "BatchMode=yes".to_string(),
        ];
        match &self.fresh {
            None => argv.push(self.host.clone()),
            Some(fresh) => {
                argv.extend([
                    "-o".to_string(),
                    "StrictHostKeyChecking=accept-new".to_string(),
                    "-o".to_string(),
                    "UserKnownHostsFile=/dev/null".to_string(),
                    "-o".to_string(),
                    "LogLevel=ERROR".to_string(),
                    "-o".to_string(),
                    format!("ConnectTimeout={SSH_CONNECT_TIMEOUT_SECS}"),
                    "-p".to_string(),
                    fresh.port.to_string(),
                    format!("{}@{}", fresh.user, self.host),
                ]);
            }
        }
        argv.push("--".to_string());
        argv
    }
}

/// Whether a spawn crosses ssh or runs here.
#[derive(Debug, Clone)]
pub enum SpawnMode {
    /// ssh to the destination; the worker runs as the ssh command.
    Ssh,
    /// Spawn the `sima-worker` binary at this path directly, no ssh hop — the
    /// stub-provider testing path.
    Local(PathBuf),
}

/// Where the transport currently sends its workers, and the lifecycle of that
/// destination. `spawn` reads it; the supervisor swaps it.
enum TargetState {
    /// The instance to spawn on. In [`SpawnMode::Local`] the endpoint is
    /// carried for uniformity but the local spawn ignores it.
    Live(SshDestination),
    /// An instance is being replaced: `spawn` blocks until the target settles.
    Replacing,
    /// The transport has retired: `spawn` reports it rather than a worker.
    Retired {
        /// Whether the retirement must fault the run.
        fatal: bool,
    },
}

/// The target and a generation counter every transition bumps. A spawn that
/// failed against one live target reads the generation to tell a swap — a new
/// host it should start over on, with a fresh readiness bound — from the same
/// dead host it must keep retrying against the running bound.
struct TargetSlot {
    state: TargetState,
    generation: u64,
}

/// What awaiting a spawnable target resolved to: an instance to spawn on and
/// the generation it belongs to, or a retirement to report.
enum Spawnable {
    Live {
        target: SshDestination,
        generation: u64,
    },
    Retired {
        fatal: bool,
    },
}

/// Spawns workers over ssh at a destination the supervisor can swap under the
/// running pool. One transport serves one machine's pool.
pub struct SshTransport {
    mode: SpawnMode,
    /// The current target and its lifecycle, guarded so the supervisor's swaps
    /// and the worker threads' spawns serialize.
    state: Mutex<TargetSlot>,
    /// Signals a target change to a `spawn` blocked in `Replacing` or waiting
    /// out a failed ssh spawn.
    settled: Condvar,
    settings: SpawnSettings,
    /// How long an ssh spawn keeps retrying a failing target before it gives
    /// up and faults the run — the readiness bound the machine was acquired
    /// under, so a broken host faults only after the same wait a fresh one is
    /// given to come up. A swap restarts this bound on the new host.
    ready_timeout: Duration,
    /// How long a failed ssh spawn waits between retries; a target change wakes
    /// it early.
    ready_poll: Duration,
}

impl SshTransport {
    /// A transport spawning workers on `initial` under `mode`, for a run over
    /// `format` with the given checkpoint cadence ([`Duration::MAX`] and `None`
    /// disable an axis). `ready_timeout` and `ready_poll` bound and pace an
    /// ssh spawn's wait for a replacement, matching the readiness bounds the
    /// machine was acquired under.
    pub fn new(
        mode: SpawnMode,
        initial: SshDestination,
        settings: SpawnSettings,
        ready_timeout: Duration,
        ready_poll: Duration,
    ) -> SshTransport {
        SshTransport {
            mode,
            state: Mutex::new(TargetSlot {
                state: TargetState::Live(initial),
                generation: 0,
            }),
            settled: Condvar::new(),
            settings,
            ready_timeout,
            ready_poll,
        }
    }

    /// Marks the current instance as being replaced: spawns block until the
    /// next `swap_to_live` or `retire`. A no-op once retired — a retirement is
    /// terminal.
    pub fn mark_replacing(&self) {
        let mut slot = self.lock();
        if !matches!(slot.state, TargetState::Retired { .. }) {
            slot.state = TargetState::Replacing;
            slot.generation += 1;
            // A spawn waiting out a failed attempt wakes, sees the bumped
            // generation, and re-enters the blocking wait for the settled
            // target rather than retrying the dead host.
            self.settled.notify_all();
        }
    }

    /// Swaps the target to `target` and releases any spawn blocked while
    /// replacing or waiting out a failed attempt. A no-op once retired.
    pub fn swap_to_live(&self, target: SshDestination) {
        let mut slot = self.lock();
        if !matches!(slot.state, TargetState::Retired { .. }) {
            slot.state = TargetState::Live(target);
            slot.generation += 1;
            self.settled.notify_all();
        }
    }

    /// Retires the transport, releasing every blocked spawn with the
    /// retirement. Terminal: no later swap revives it.
    pub fn retire(&self, fatal: bool) {
        let mut slot = self.lock();
        slot.state = TargetState::Retired { fatal };
        slot.generation += 1;
        self.settled.notify_all();
    }

    /// The host of the current live target, or `None` while replacing or once
    /// retired. The parent's account of where this pool's workers currently
    /// spawn, for diagnostics and for observing a replacement's target swap.
    pub fn live_host(&self) -> Option<String> {
        match &self.lock().state {
            TargetState::Live(target) => Some(target.host().to_string()),
            TargetState::Replacing | TargetState::Retired { .. } => None,
        }
    }

    /// Blocks while the target is `Replacing`, then resolves to the live
    /// instance to spawn on, tagged with its generation, or the retirement to
    /// report.
    fn await_spawnable(&self) -> Spawnable {
        let mut slot = self.lock();
        loop {
            match &slot.state {
                TargetState::Live(target) => {
                    return Spawnable::Live {
                        target: target.clone(),
                        generation: slot.generation,
                    };
                }
                TargetState::Retired { fatal } => return Spawnable::Retired { fatal: *fatal },
                TargetState::Replacing => {
                    slot = self
                        .settled
                        .wait(slot)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        }
    }

    /// Waits up to `poll` for the target to move past `generation` — a swap, a
    /// replacement beginning, or a retirement — returning whether it moved. A
    /// `false` return means the poll elapsed with the same target still live,
    /// so the caller retries it against the running readiness bound.
    fn await_target_change(&self, generation: u64, poll: Duration) -> bool {
        let slot = self.lock();
        if slot.generation != generation {
            return true;
        }
        let (slot, _) = self
            .settled
            .wait_timeout(slot, poll)
            .unwrap_or_else(PoisonError::into_inner);
        slot.generation != generation
    }

    /// The lock over the target slot, recovering a poisoned lock: a slot holds
    /// a whole target or none, so a panicking holder leaves nothing torn.
    fn lock(&self) -> std::sync::MutexGuard<'_, TargetSlot> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl WorkerTransport for SshTransport {
    fn spawn(
        &self,
        worker: u64,
        device: Option<&DeviceBinding>,
        events: Emitter,
    ) -> Result<SpawnOutcome> {
        // Each attempt takes a fresh emitter clone: a failed spawn drops its
        // clone, so a retry never reuses a spent one.
        self.spawn_retrying(matches!(self.mode, SpawnMode::Ssh), |target| {
            self.attempt_spawn(target, worker, device, events.clone())
        })
    }
}

impl SshTransport {
    /// Resolves a spawnable target and runs `attempt` against it, retrying a
    /// failure in `retry_mode` until the attempt spawns, the target moves on,
    /// or the readiness bound elapses. Factored from
    /// [`spawn`](SshTransport::spawn) so the wait-and-retry control flow is
    /// testable with a scripted attempt in place of a real process spawn.
    fn spawn_retrying(
        &self,
        retry_mode: bool,
        mut attempt: impl FnMut(&SshDestination) -> Result<Box<dyn WorkerLink>>,
    ) -> Result<SpawnOutcome> {
        loop {
            let (target, generation) = match self.await_spawnable() {
                Spawnable::Live { target, generation } => (target, generation),
                Spawnable::Retired { fatal } => return Ok(SpawnOutcome::Retired { fatal }),
            };
            // A fresh bound per generation: a swap restarts the wait on the new
            // host rather than inheriting the dead host's remaining time.
            let deadline = Instant::now() + self.ready_timeout;
            loop {
                match attempt(&target) {
                    Ok(link) => return Ok(SpawnOutcome::Link(link)),
                    // Only an ssh spawn waits for a replacement; a local spawn
                    // failure is the worker's own and propagates at once.
                    Err(error) if !retry_mode => return Err(error),
                    Err(error) => {
                        // A target change — the supervisor swapping a
                        // replacement in, or retiring — breaks out to re-read
                        // the new target. Otherwise the same host is retried
                        // until the bound, then the failure faults the run.
                        if self.await_target_change(generation, self.ready_poll) {
                            break;
                        }
                        if Instant::now() >= deadline {
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    /// One spawn attempt against `target`: builds the argv and spawns the
    /// worker process over it.
    fn attempt_spawn(
        &self,
        target: &SshDestination,
        worker: u64,
        device: Option<&DeviceBinding>,
        events: Emitter,
    ) -> Result<Box<dyn WorkerLink>> {
        let argv = self.mode.spawn_argv(target);
        // `spawn_argv` never yields an empty vector: `ssh` or the binary path is
        // always the first element.
        let (program, args) = argv.split_first().expect("a non-empty command vector");
        // The host label attributes the child's forwarded events: the instance
        // host over ssh, empty for a local spawn on this machine.
        let context = EventContext {
            events,
            worker,
            host: self.mode.host_label(target),
        };
        spawn_worker(
            Path::new(program),
            args,
            &self.settings,
            worker,
            device,
            context,
        )
    }
}

impl SpawnMode {
    /// The argv that spawns a worker on `target`: the ssh invocation, or the
    /// local binary directly.
    fn spawn_argv(&self, target: &SshDestination) -> Vec<String> {
        match self {
            SpawnMode::Ssh => ssh_argv(target, None),
            SpawnMode::Local(program) => vec![program.to_string_lossy().into_owned()],
        }
    }

    /// The host label the child's events are attributed under: the instance
    /// host over ssh, empty for a local spawn.
    fn host_label(&self, target: &SshDestination) -> String {
        match self {
            SpawnMode::Ssh => target.host().to_string(),
            SpawnMode::Local(_) => String::new(),
        }
    }
}

/// How long ssh waits to establish a TCP connection before failing, in
/// seconds. Without it a host that drops packets stalls a spawn for the
/// kernel's TCP timeout — minutes — instead of failing within the transport's
/// own bounds, where the wait-and-retry loop can act on it.
const SSH_CONNECT_TIMEOUT_SECS: u64 = 10;

/// The argv that runs `sima-worker` at `destination` over ssh: the
/// destination's own [`prefix`](SshDestination::prefix), then the worker, with
/// `--enumerate-devices <format>` appended when `probe` names the run's format.
pub(crate) fn ssh_argv(destination: &SshDestination, probe: Option<&FormatId>) -> Vec<String> {
    let mut argv = destination.prefix();
    argv.push(WORKER_ENTRYPOINT.to_string());
    if let Some(format) = probe {
        argv.push("--enumerate-devices".to_string());
        argv.push(format.as_str().to_string());
    }
    argv
}

/// The argv that enumerates a machine's devices for `format`, so the
/// orchestrator derives one worker slot per usable GPU: the ssh spawn argv with
/// `--enumerate-devices <format>`, or the local binary with the same in
/// [`SpawnMode::Local`].
///
/// The format travels with the probe because the answer depends on it: the
/// machine enumerates the backend the run's program executes through, and a
/// device another backend reaches is not a place this run can put a worker.
pub fn probe_argv(mode: &SpawnMode, target: &SshDestination, format: &FormatId) -> Vec<String> {
    match mode {
        SpawnMode::Ssh => ssh_argv(target, Some(format)),
        SpawnMode::Local(program) => vec![
            program.to_string_lossy().into_owned(),
            "--enumerate-devices".to_string(),
            format.as_str().to_string(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use sima_core::Error;

    use super::*;
    use crate::link::LinkEvent;
    use crate::spawn_policy::SpawnPolicy;

    fn a_target() -> SshDestination {
        SshDestination::rented("203.0.113.7", 41022, "root")
    }

    /// The format a probe argv is built for.
    fn a_format() -> FormatId {
        FormatId::new("stub.v1").expect("format id")
    }

    /// A transport whose readiness bound is generous and whose poll is short,
    /// so a retry test that drives a swap in never sleeps out a long wait.
    fn a_transport(mode: SpawnMode) -> SshTransport {
        bounded_transport(mode, Duration::from_secs(5), Duration::from_millis(2))
    }

    /// A transport under explicit readiness bounds, for the retry-and-wait
    /// tests that pin how long a failing ssh spawn persists.
    fn bounded_transport(
        mode: SpawnMode,
        ready_timeout: Duration,
        ready_poll: Duration,
    ) -> SshTransport {
        SshTransport::new(
            mode,
            a_target(),
            SpawnSettings::new(
                SpawnPolicy::Inherit,
                Duration::MAX,
                FormatId::new("stub.v1").expect("format id"),
                Duration::MAX,
                None,
            ),
            ready_timeout,
            ready_poll,
        )
    }

    /// A worker link double for the retry tests: every method is inert, since
    /// a scripted attempt never converses with it.
    struct StubLink;

    impl WorkerLink for StubLink {
        fn device_name(&self) -> &str {
            ""
        }

        fn driver(&self) -> &str {
            ""
        }

        fn program(&self) -> &str {
            ""
        }

        fn assign(&mut self, _assignment: &crate::protocol::Assignment) -> Result<()> {
            Ok(())
        }

        fn next(&mut self, _deadline: Option<Instant>) -> Result<LinkEvent> {
            Ok(LinkEvent::DeadlineExpired)
        }

        fn kill(&mut self) {}
    }

    /// A boxed [`StubLink`] as an `Ok` attempt result.
    fn stub_link() -> Result<Box<dyn WorkerLink>> {
        Ok(Box::new(StubLink))
    }

    #[test]
    fn a_known_destination_adds_nothing_but_batch_mode() {
        // The operator configured it: the local ssh configuration supplies the
        // port, the user, and the key, so stating any of them here would
        // override what the operator wrote.
        assert_eq!(
            SshDestination::known("gpubox").prefix(),
            ["ssh", "-o", "BatchMode=yes", "gpubox", "--"]
        );
    }

    #[test]
    fn a_rented_destination_states_its_port_user_and_first_contact_policy() {
        assert_eq!(
            SshDestination::rented("203.0.113.7", 41022, "root").prefix(),
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                "-o",
                "ConnectTimeout=10",
                "-p",
                "41022",
                "root@203.0.113.7",
                "--",
            ]
        );
    }

    #[test]
    fn every_prefix_ends_at_the_option_terminator() {
        // The caller appends the remote command, so a prefix that did not end
        // ssh's own options would let a command argument be read as one.
        for destination in [
            SshDestination::known("gpubox"),
            SshDestination::rented("203.0.113.7", 41022, "root"),
        ] {
            let prefix = destination.prefix();
            assert_eq!(prefix.last().map(String::as_str), Some("--"));
            assert_eq!(prefix.first().map(String::as_str), Some("ssh"));
            assert!(
                prefix.contains(&"BatchMode=yes".to_string()),
                "an unauthenticated destination must fail rather than prompt"
            );
        }
    }

    #[test]
    fn a_destination_names_the_host_its_workers_are_journaled_under() {
        assert_eq!(SshDestination::known("gpubox").host(), "gpubox");
        assert_eq!(
            SshDestination::rented("203.0.113.7", 41022, "root").host(),
            "203.0.113.7"
        );
    }

    #[test]
    fn the_ssh_spawn_argv_carries_the_disposable_host_key_policy() {
        assert_eq!(
            ssh_argv(&a_target(), None),
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                "-o",
                "ConnectTimeout=10",
                "-p",
                "41022",
                "root@203.0.113.7",
                "--",
                "sima-worker",
            ]
        );
    }

    #[test]
    fn the_ssh_argv_bounds_the_connection_wait() {
        // A packet-dropping host must fail within the transport's own bounds,
        // not stall for the kernel TCP timeout: both spawn and probe argv carry
        // the connect timeout.
        assert!(ssh_argv(&a_target(), None).contains(&"ConnectTimeout=10".to_string()));
        assert!(
            ssh_argv(&a_target(), Some(&a_format())).contains(&"ConnectTimeout=10".to_string())
        );
    }

    #[test]
    fn the_ssh_probe_argv_appends_enumerate_and_the_format() {
        let argv = ssh_argv(&a_target(), Some(&a_format()));
        assert_eq!(&argv[argv.len() - 2..], ["--enumerate-devices", "stub.v1"]);
        // Otherwise identical to the spawn argv.
        assert_eq!(&argv[..argv.len() - 2], ssh_argv(&a_target(), None));
    }

    #[test]
    fn a_local_mode_spawn_argv_is_the_bare_binary() {
        let mode = SpawnMode::Local(PathBuf::from("/opt/sima/sima-worker"));
        assert_eq!(mode.spawn_argv(&a_target()), ["/opt/sima/sima-worker"]);
    }

    #[test]
    fn a_local_mode_probe_argv_appends_enumerate_to_the_bare_binary() {
        let mode = SpawnMode::Local(PathBuf::from("/opt/sima/sima-worker"));
        assert_eq!(
            probe_argv(&mode, &a_target(), &a_format()),
            ["/opt/sima/sima-worker", "--enumerate-devices", "stub.v1"]
        );
    }

    #[test]
    fn the_probe_argv_names_the_format_it_is_asked_about() {
        // The instance answers for one backend, so the probe carries which
        // program is asking rather than assuming every device on the host is a
        // place this run can put a worker.
        let format = FormatId::new("ca_evolution.gray_scott.v1").expect("format id");
        for mode in [
            SpawnMode::Ssh,
            SpawnMode::Local(PathBuf::from("/opt/sima/sima-worker")),
        ] {
            let argv = probe_argv(&mode, &a_target(), &format);
            assert_eq!(
                &argv[argv.len() - 2..],
                ["--enumerate-devices", "ca_evolution.gray_scott.v1"]
            );
        }
    }

    #[test]
    fn a_spawn_blocks_while_replacing_and_releases_on_a_swap() {
        let transport = Arc::new(a_transport(SpawnMode::Ssh));
        transport.mark_replacing();
        let (tx, rx) = mpsc::channel();
        let waiter = {
            let transport = Arc::clone(&transport);
            std::thread::spawn(move || {
                // The spawnable wait is the blocking half of `spawn`, isolated
                // so the test never spawns a real ssh process.
                let spawnable = transport.await_spawnable();
                tx.send(matches!(spawnable, Spawnable::Live { .. }))
                    .expect("send the outcome");
            })
        };
        // While replacing, the waiter makes no progress.
        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "the spawn is blocked while replacing"
        );
        let replacement = SshDestination::rented("198.51.100.9", 50022, "root");
        transport.swap_to_live(replacement.clone());
        assert!(
            rx.recv_timeout(Duration::from_secs(5))
                .expect("the swap releases the spawn"),
            "the released spawn saw a live target"
        );
        waiter.join().expect("the waiter thread joins");
        // The swapped target is what a subsequent spawn would build against.
        match transport.await_spawnable() {
            Spawnable::Live { target, .. } => assert_eq!(target, replacement),
            Spawnable::Retired { .. } => panic!("expected a live target after the swap"),
        }
    }

    #[test]
    fn a_retire_releases_a_blocked_spawn_with_the_retirement() {
        let transport = Arc::new(a_transport(SpawnMode::Ssh));
        transport.mark_replacing();
        let (tx, rx) = mpsc::channel();
        {
            let transport = Arc::clone(&transport);
            std::thread::spawn(move || {
                let spawnable = transport.await_spawnable();
                let outcome = match spawnable {
                    Spawnable::Retired { fatal } => Some(fatal),
                    Spawnable::Live { .. } => None,
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
        let transport = a_transport(SpawnMode::Ssh);
        transport.retire(false);
        transport.swap_to_live(a_target());
        // Retirement is terminal: a later swap is ignored.
        match transport.await_spawnable() {
            Spawnable::Retired { fatal } => assert!(!fatal),
            Spawnable::Live { .. } => panic!("a swap must not revive a retired transport"),
        }
    }

    #[test]
    fn a_first_spawn_against_a_live_target_proceeds_at_once() {
        // The healthy path: a live target and an attempt that succeeds first
        // try returns a link with no wait.
        let transport = a_transport(SpawnMode::Ssh);
        let attempts = AtomicUsize::new(0);
        let outcome = transport.spawn_retrying(true, |_| {
            attempts.fetch_add(1, Ordering::Relaxed);
            stub_link()
        });
        assert!(matches!(outcome, Ok(SpawnOutcome::Link(_))));
        assert_eq!(attempts.load(Ordering::Relaxed), 1, "no retry on success");
    }

    #[test]
    fn a_local_spawn_failure_propagates_without_retrying() {
        // Local mode has no supervisor swapping a replacement behind it, so a
        // failure is the first and only attempt.
        let transport = a_transport(SpawnMode::Ssh);
        let attempts = AtomicUsize::new(0);
        let outcome = transport.spawn_retrying(false, |_| {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err(Error::Transport("local spawn failed".to_string()))
        });
        assert!(matches!(outcome, Err(Error::Transport(_))));
        assert_eq!(
            attempts.load(Ordering::Relaxed),
            1,
            "no retry in local mode"
        );
    }

    #[test]
    fn a_retired_transport_spawn_reports_the_retirement_without_attempting() {
        let transport = a_transport(SpawnMode::Ssh);
        transport.retire(true);
        let outcome =
            transport.spawn_retrying(true, |_| panic!("a retired transport never attempts"));
        assert!(matches!(outcome, Ok(SpawnOutcome::Retired { fatal: true })));
    }

    #[test]
    fn a_failed_ssh_spawn_waits_for_a_swap_and_lands_on_the_new_target() {
        // The respawn race: the first attempt fails against the dead host, the
        // supervisor swaps a replacement in, and the retry lands on the new
        // target instead of faulting the run.
        let transport = a_transport(SpawnMode::Ssh);
        let replacement = SshDestination::rented("198.51.100.9", 50022, "root");
        let attempts = AtomicUsize::new(0);
        let seen: Mutex<Vec<SshDestination>> = Mutex::new(Vec::new());
        let outcome = transport.spawn_retrying(true, |target| {
            seen.lock().expect("seen lock").push(target.clone());
            if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                // The first attempt fails; the supervisor swaps a replacement
                // in before the retry re-reads the target.
                transport.mark_replacing();
                transport.swap_to_live(replacement.clone());
                Err(Error::Transport("ssh to the dead host failed".to_string()))
            } else {
                stub_link()
            }
        });
        assert!(matches!(outcome, Ok(SpawnOutcome::Link(_))));
        let seen = seen.lock().expect("seen lock");
        assert_eq!(seen.len(), 2, "one failed attempt, then the retry");
        assert_eq!(
            seen.last(),
            Some(&replacement),
            "the retry lands on the swapped-in host"
        );
    }

    #[test]
    fn a_retirement_during_the_wait_reports_the_retirement() {
        // A best-effort pool whose replacement cannot be made retires the
        // transport while a spawn is waiting: the spawn reports it rather than
        // erroring or spinning to the bound.
        let transport = a_transport(SpawnMode::Ssh);
        let outcome = transport.spawn_retrying(true, |_| {
            transport.retire(false);
            Err(Error::Transport("ssh to the dead host failed".to_string()))
        });
        assert!(matches!(
            outcome,
            Ok(SpawnOutcome::Retired { fatal: false })
        ));
    }

    #[test]
    fn a_persistently_failing_ssh_spawn_faults_after_the_bound() {
        // A genuinely broken host the supervisor never replaces: the retry loop
        // gives up after ready_timeout and the failure propagates, faulting the
        // run — the same outcome as failing fast, delayed by the bound.
        let transport = bounded_transport(
            SpawnMode::Ssh,
            Duration::from_millis(60),
            Duration::from_millis(5),
        );
        let started = Instant::now();
        let attempts = AtomicUsize::new(0);
        let outcome = transport.spawn_retrying(true, |_| {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err(Error::Transport("the host stays unreachable".to_string()))
        });
        assert!(matches!(outcome, Err(Error::Transport(_))));
        assert!(
            started.elapsed() >= Duration::from_millis(60),
            "the spawn persisted for the readiness bound"
        );
        assert!(
            attempts.load(Ordering::Relaxed) >= 2,
            "the spawn retried rather than failing on the first attempt"
        );
    }
}
