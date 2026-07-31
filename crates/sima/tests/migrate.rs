//! End-to-end acceptance of a migrated run: a run interrupted partway here,
//! moved onto another machine, finished there, and brought home — with the
//! manifest byte-identical to a run that was never interrupted.
//!
//! The far side is the real `sima` binary, reached through the stub provider,
//! whose machines are local subprocesses. Nothing here needs a network, a GPU,
//! an ssh hop, or a container, so it runs in the ordinary gate.
//!
//! The local halves are driven in-process, so the interrupt is raised from the
//! run observer rather than by signalling a subprocess: a fixed number of
//! commits, not a wall-clock guess.
//!
//! The last test moves a run over a real ssh hop, against a throwaway server
//! the test stands up and tears down. It needs no root, changes nothing outside
//! its temporary directory, and runs in the ordinary gate, because an ssh path
//! nobody exercises is an ssh path nobody knows works.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::{manifest_bytes, sima_command, worker_binary};
use sima_core::Result;
use sima_model::{TaskKey, TaskRecord};
use sima_pipeline::{
    BinaryChange, Engagement, Event, MigrateOutcome, Record, RunControl, RunOutcome, load, migrate,
    orchestrate, task_keys,
};
use sima_store::Store;

/// Two candidates over `segments` accumulating segments, so a chain is left
/// partway by an early interrupt and has a frontier to hand over.
///
/// `[run]` is the only hashed section, so two configs written from the same
/// `segments` describe the same run whatever machine drives them.
fn run_section(segments: u64) -> String {
    format!(
        r#"
        [run]
        root_seed = 21
        segments = {segments}
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["accumulate:2", "accumulate:2"]

        [config]
        store = "./store"
        max_attempts = 3
    "#
    )
}

/// Writes a config under `dir` naming a store beside it, plus `machines`.
fn config(dir: &Path, name: &str, segments: u64, machines: &str) -> PathBuf {
    let text = format!(
        "{}\n[orchestrator]\nworkers = 2\n{machines}\n",
        run_section(segments)
    );
    common::write_config_text(dir, name, &text)
}

/// A config whose orchestrator migrates onto a rented stub machine, rooted at
/// `root` and driving the `sima` binary this build produced.
fn migrating(dir: &Path, root: &Path, segments: u64) -> PathBuf {
    // The stub provider's machines are reached on this machine, so the far side
    // is a local subprocess with its own store; the bounds are the readiness
    // bounds a wind-down also waits on, kept short so no test sleeps.
    config(
        dir,
        "migrating.toml",
        segments,
        &format!(
            r#"
            migrate = "far"

            [host.far]
            provider = "stub"
            root = {root:?}
            binary = {binary:?}
            ready_timeout_ms = 30000
            ready_poll_ms = 20
            "#,
            root = root.to_string_lossy(),
            binary = far_binary(),
        ),
    )
}

/// Drives the run `config` describes, interrupting once `stop_after` tasks have
/// committed; `None` runs it to its end.
fn drive(config: &Path, stop_after: Option<usize>) -> Result<RunOutcome> {
    let loaded = load(config)?;
    let interrupt = AtomicBool::new(false);
    let committed = AtomicUsize::new(0);
    let control = RunControl {
        observer: &|record: &Record| {
            if let Some(stop_after) = stop_after
                && matches!(record.event, Event::Committed { .. })
                && committed.fetch_add(1, Ordering::Relaxed) + 1 >= stop_after
            {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
        on_start: None,
    };
    orchestrate(
        &loaded,
        &control,
        Engagement::Orchestrator,
        BinaryChange::Refuse,
    )
}

/// Moves the run `config` describes onto its destination, discarding the
/// records it forwards.
fn move_run(config: &Path) -> Result<MigrateOutcome> {
    migrate(config, &|_: &Record| {}, &AtomicBool::new(false))
}

/// Every record the store of the run `config` describes currently holds, keyed
/// by task. The frontier key of an unfinished chain has no record and is
/// absent.
fn committed_records(config: &Path) -> Result<BTreeMap<TaskKey, TaskRecord>> {
    let loaded = load(config)?;
    let store = Store::open(&loaded.store)?;
    let mut records = BTreeMap::new();
    for key in task_keys(&loaded, &store)? {
        if let Some(record) = store.record(&key)? {
            records.insert(key, record);
        }
    }
    Ok(records)
}

/// The far side's own store, under the run's directory beneath `root`.
fn far_store(config: &Path, root: &Path) -> Result<Store> {
    let run = load(config)?.run.id();
    Store::open(root.join(run.to_string()).join("store"))
}

/// Every record the far side's store holds for the run `config` describes,
/// keyed by task.
fn far_committed(config: &Path, far: &Store) -> Result<BTreeMap<TaskKey, TaskRecord>> {
    let loaded = load(config)?;
    let mut records = BTreeMap::new();
    for key in task_keys(&loaded, far)? {
        if let Some(record) = far.record(&key)? {
            records.insert(key, record);
        }
    }
    Ok(records)
}

/// The tasks the run `config` describes has journaled as committed.
fn journaled_commits(config: &Path) -> Vec<String> {
    common::journal_events(config)
        .into_iter()
        .filter_map(|event| match event {
            Event::Committed { task, .. } => Some(task),
            _ => None,
        })
        .collect()
}

/// The segment count a run finishes in: short enough that a whole run is a
/// fraction of a second.
const SEGMENTS: u64 = 6;

/// The segment count a run cannot finish while a migration is watching its
/// first record arrive. It makes the interrupt test decide on the ordering of
/// events rather than on how fast this machine is.
const UNFINISHABLE: u64 = 400;

/// Builds the worker binary once, so both the enumeration probe here and the
/// far side's own `sima run` find it beside the test's executable.
fn workers_built() {
    let _ = worker_binary();
}

/// Where the far side's `sima` is, for a config that names it.
fn far_binary() -> &'static str {
    env!("CARGO_BIN_EXE_sima")
}

#[test]
fn a_migrated_run_finalizes_to_the_manifest_an_uninterrupted_run_writes() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");

    // The reference: the same run, never interrupted, driven here throughout.
    let reference_dir = dir.path().join("reference");
    std::fs::create_dir_all(&reference_dir).expect("reference dir");
    let reference = config(&reference_dir, "reference.toml", SEGMENTS, "");
    assert!(matches!(
        drive(&reference, None)?,
        RunOutcome::Finalized { .. }
    ));

    // The migrated run: interrupted here after two commits, so its chains are
    // partway and the rest is the far side's to finish.
    let migrated_dir = dir.path().join("migrated");
    std::fs::create_dir_all(&migrated_dir).expect("migrated dir");
    let migrated = migrating(&migrated_dir, &far_root, SEGMENTS);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));
    let before = committed_records(&migrated)?;
    assert!(!before.is_empty(), "the local run committed something");
    assert!(
        manifest_bytes(&migrated).is_none(),
        "an interrupted run writes no manifest"
    );
    let total = load(&reference)?
        .run
        .segments
        .expect("a segmented run")
        .get() as usize
        * 2;
    assert!(
        before.len() < total,
        "the local run stopped short of the {total} tasks: {} committed",
        before.len()
    );

    let outcome = move_run(&migrated)?;
    assert!(
        matches!(outcome, MigrateOutcome::Finalized { .. }),
        "the migration came home complete: {outcome:?}"
    );

    // The criterion the milestone carries: byte equality with a run that was
    // never interrupted and never moved.
    assert_eq!(
        manifest_bytes(&migrated),
        manifest_bytes(&reference),
        "the migrated run's manifest is the uninterrupted run's manifest"
    );

    // Nothing committed here was recomputed there: every record that existed
    // before the move is the record that is there after it.
    let after = committed_records(&migrated)?;
    for (key, record) in &before {
        assert_eq!(
            after.get(key),
            Some(record),
            "task {key} was recomputed rather than carried"
        );
    }
    assert!(
        after.len() > before.len(),
        "the migration brought new records home"
    );

    // The rest ran on the far side: its own store holds them, and the local
    // journal gained them only because the follow forwarded them.
    let far = far_store(&migrated, &far_root)?;
    let commits = journaled_commits(&migrated);
    for key in after.keys().filter(|key| !before.contains_key(key)) {
        assert!(
            far.record(key)?.is_some(),
            "task {key} was committed on the far side"
        );
        assert!(
            commits.contains(&key.to_string()),
            "task {key}'s commit reached the local journal"
        );
    }

    // The rental is gone: the ledger holds nothing to reconcile.
    let store = Store::open(&load(&migrated)?.store)?;
    assert!(
        store.instances()?.is_empty(),
        "the machine that hosted the run was torn down"
    );
    Ok(())
}

#[test]
fn a_second_migration_over_a_finished_run_finalizes_to_the_same_manifest() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated = migrating(dir.path(), &far_root, SEGMENTS);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));
    assert!(matches!(
        move_run(&migrated)?,
        MigrateOutcome::Finalized { .. }
    ));
    let manifest = manifest_bytes(&migrated).expect("a finalized manifest");

    // Re-running is the resume path: the frontier re-derives empty, the far
    // side has nothing to do, and the run re-finalizes to the same bytes.
    assert!(matches!(
        move_run(&migrated)?,
        MigrateOutcome::Finalized { .. }
    ));
    assert_eq!(manifest_bytes(&migrated), Some(manifest));
    let store = Store::open(&load(&migrated)?.store)?;
    assert!(store.instances()?.is_empty(), "nothing was left rented");
    Ok(())
}

#[test]
fn a_migration_interrupted_during_the_follow_still_pulls_and_tears_down() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    // A chain the far side cannot reach the end of while this migration is
    // still reading its first record, so the wind-down decides the outcome
    // rather than a race with how fast this machine runs.
    let migrated = migrating(dir.path(), &far_root, UNFINISHABLE);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));
    let before = committed_records(&migrated)?;

    // Wound down as soon as the far run's first record arrives: the far side is
    // signalled, whatever it committed is pulled, and the rental is destroyed.
    let interrupt = AtomicBool::new(false);
    let outcome = migrate(
        &migrated,
        &|_: &Record| interrupt.store(true, Ordering::Relaxed),
        &interrupt,
    )?;
    assert!(
        matches!(outcome, MigrateOutcome::Interrupted { .. }),
        "a wound-down migration is resumable, not finalized: {outcome:?}"
    );
    assert!(
        manifest_bytes(&migrated).is_none(),
        "an interrupted migration seals nothing"
    );

    // The results that existed still do.
    let after = committed_records(&migrated)?;
    for (key, record) in &before {
        assert_eq!(after.get(key), Some(record), "task {key} came home intact");
    }
    // And the pull ran to completion: nothing the far side committed was left
    // behind, however far it got before the signal.
    let far = far_store(&migrated, &far_root)?;
    let far_keys = far_committed(&migrated, &far)?;
    assert!(
        !far_keys.is_empty(),
        "the far side held the chain it was sent"
    );
    for (key, record) in &far_keys {
        assert_eq!(
            Store::open(&load(&migrated)?.store)?.record(key)?.as_ref(),
            Some(record),
            "task {key} was left on the far side"
        );
    }

    let store = Store::open(&load(&migrated)?.store)?;
    assert!(
        store.instances()?.is_empty(),
        "the machine was torn down on the interrupt path"
    );
    Ok(())
}

// ---- The same acceptance, over a real ssh hop ----

/// A throwaway sshd for the duration of a test: its own host key, its own
/// authorized-keys file, a free high port, and a log of what it accepted.
///
/// It needs no root and writes nothing outside `dir`, so it changes no system
/// state and leaves nothing behind. The `Drop` kills it on every path, including
/// a panicking assertion.
struct Sshd {
    port: u16,
    /// The private key a client authenticates with.
    key: PathBuf,
    /// What the server itself recorded, which is the only evidence a hop
    /// happened that a local spawn cannot produce.
    log: PathBuf,
    pid: u32,
    /// The agent holding the key the server authorizes, and its process. The
    /// migration builds its own ssh invocations and names no identity —
    /// correctly, since a rented machine is reached with the operator's own ssh
    /// configuration — so an agent is how a test supplies one.
    agent_sock: PathBuf,
    agent_pid: u32,
}

impl Sshd {
    /// Stands a server up under `dir`, with `path_prefix` prepended to the PATH
    /// of every session it serves — which is how the far side's `sima-worker`
    /// is found without touching anything outside the test.
    fn start(dir: &Path, path_prefix: &Path) -> Sshd {
        let host_key = dir.join("hostkey");
        let key = dir.join("clientkey");
        for path in [&host_key, &key] {
            let generated = Command::new("ssh-keygen")
                .args(["-q", "-t", "ed25519", "-N", "", "-f"])
                .arg(path)
                .status()
                .expect("run ssh-keygen");
            assert!(generated.success(), "ssh-keygen failed for {path:?}");
        }
        let authorized = dir.join("authorized_keys");
        std::fs::copy(dir.join("clientkey.pub"), &authorized).expect("authorize the client key");

        // Bound and released, so sshd takes a port nothing else is on.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("bind a free port")
            .local_addr()
            .expect("the bound address")
            .port();
        let log = dir.join("sshd.log");
        let pid_file = dir.join("sshd.pid");
        // `ForceCommand` runs through the login shell, whose word splitting is
        // its own; routing the requested command through `/bin/sh -c` makes the
        // split POSIX whatever that shell is.
        let started = Command::new("/usr/sbin/sshd")
            .args(["-f", "/dev/null", "-h"])
            .arg(&host_key)
            .arg("-p")
            .arg(port.to_string())
            .arg("-E")
            .arg(&log)
            .arg("-o")
            .arg(format!("AuthorizedKeysFile={}", authorized.display()))
            .args([
                "-o",
                "StrictModes=no",
                "-o",
                "UsePAM=no",
                "-o",
                "PasswordAuthentication=no",
                "-o",
            ])
            .arg(format!("PidFile={}", pid_file.display()))
            .arg("-o")
            .arg(format!(
                "ForceCommand=PATH={}:$PATH exec /bin/sh -c \"$SSH_ORIGINAL_COMMAND\"",
                path_prefix.display()
            ))
            .status()
            .expect("run sshd");
        assert!(started.success(), "sshd refused to start");

        let pid = poll_for(Duration::from_secs(10), || {
            std::fs::read_to_string(&pid_file)
                .ok()
                .and_then(|text| text.trim().parse::<u32>().ok())
        })
        .expect("sshd wrote its pid");

        // An agent of this test's own, on a socket inside `dir`, holding the one
        // key the server authorizes.
        let agent_sock = dir.join("agent.sock");
        let agent = Command::new("ssh-agent")
            .arg("-a")
            .arg(&agent_sock)
            .output()
            .expect("run ssh-agent");
        assert!(agent.status.success(), "ssh-agent refused to start");
        let agent_pid = String::from_utf8_lossy(&agent.stdout)
            .split("SSH_AGENT_PID=")
            .nth(1)
            .and_then(|rest| rest.split(';').next())
            .and_then(|pid| pid.trim().parse::<u32>().ok())
            .expect("ssh-agent reported its pid");
        let added = Command::new("ssh-add")
            .arg(&key)
            .env("SSH_AUTH_SOCK", &agent_sock)
            .output()
            .expect("run ssh-add");
        assert!(added.status.success(), "the agent refused the key");

        let server = Sshd {
            port,
            key,
            log,
            pid,
            agent_sock,
            agent_pid,
        };
        // The server is up when it answers, not when it forked.
        assert!(
            poll_for(Duration::from_secs(10), || server.answers().then_some(())).is_some(),
            "the server never accepted a session"
        );
        server
    }

    /// Whether a client can reach the server and run a command. The options
    /// are the harness's own, naming the key explicitly and remembering no host
    /// key, so the probe touches nothing outside the test either.
    fn answers(&self) -> bool {
        Command::new("ssh")
            .args(["-p", &self.port.to_string(), "-i"])
            .arg(&self.key)
            .args([
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
            ])
            .arg(format!("{}@127.0.0.1", whoami()))
            .args(["--", "true"])
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    /// The agent socket a client authenticates through.
    fn agent(&self) -> &Path {
        &self.agent_sock
    }

    /// The endpoint the stub backend is pointed at.
    fn endpoint(&self) -> String {
        format!("{}@127.0.0.1:{}", whoami(), self.port)
    }

    /// How many sessions the server itself recorded accepting.
    fn accepted(&self) -> usize {
        std::fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains("Accepted publickey"))
            .count()
    }

    /// Whether the server's process is still there.
    fn alive(&self) -> bool {
        process_alive(self.pid)
    }
}

/// Whether a process is still there. Signal zero is the existence probe: it
/// delivers nothing and reports whether it could have.
fn process_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

impl Drop for Sshd {
    fn drop(&mut self) {
        // Both processes, on every path out — including a panicking assertion.
        for pid in [self.pid, self.agent_pid] {
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
        }
    }
}

/// The user the test runs as, which is the user its own server authenticates.
/// The environment names it on an interactive machine and `id` names it
/// everywhere else.
fn whoami() -> String {
    if let Ok(user) = std::env::var("USER") {
        return user;
    }
    let named = Command::new("id").arg("-un").output().expect("run id");
    assert!(
        named.status.success(),
        "id could not name the invoking user"
    );
    String::from_utf8(named.stdout)
        .expect("the user name is UTF-8")
        .trim()
        .to_string()
}

/// Polls `probe` every 20 ms until it yields a value or `deadline` elapses.
fn poll_for<T>(deadline: Duration, probe: impl Fn() -> Option<T>) -> Option<T> {
    let end = Instant::now() + deadline;
    loop {
        if let Some(value) = probe() {
            return Some(value);
        }
        if Instant::now() >= end {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Runs `sima migrate <config>` with the stub backend pointed at `endpoint` and
/// authenticating through `agent`, so every far-side operation crosses a real
/// ssh hop and nothing outside the test's directory is read or written.
fn migrate_over(config: &Path, endpoint: &str, agent: &Path) -> Output {
    sima_command()
        .args(["migrate", config.to_str().expect("utf-8 path")])
        .env("SIMA_STUB_SSH", endpoint)
        .env("SSH_AUTH_SOCK", agent)
        .output()
        .expect("spawn sima migrate")
}

#[test]
fn a_run_migrated_over_a_real_ssh_hop_finalizes_and_the_server_saw_it() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    // The far side's `sima-worker` is found through the PATH the server sets
    // for its sessions; the binary sits beside the `sima` the config names.
    let binaries = Path::new(far_binary())
        .parent()
        .expect("a binary directory");
    let sshd = Sshd::start(dir.path(), binaries);

    let far_root = dir.path().join("far");
    let migrated = migrating(dir.path(), &far_root, SEGMENTS);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));
    let before = committed_records(&migrated)?;
    assert!(!before.is_empty(), "the local run committed something");

    let output = migrate_over(&migrated, &sshd.endpoint(), sshd.agent());
    assert_eq!(
        output.status.code(),
        Some(0),
        "the migration finalized: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // It really crossed the hop: the server recorded every session, and a
    // migration reached in process would have produced none.
    assert!(
        sshd.accepted() > 1,
        "the server accepted the far-side sessions: {} in {:?}",
        sshd.accepted(),
        sshd.log
    );

    // And it is the same run, finished: every record carried, the rest
    // committed on the far side, nothing left rented.
    assert!(
        manifest_bytes(&migrated).is_some(),
        "the manifest is sealed"
    );
    let after = committed_records(&migrated)?;
    for (key, record) in &before {
        assert_eq!(after.get(key), Some(record), "task {key} was recomputed");
    }
    assert!(after.len() > before.len(), "the far side did the rest");
    let store = Store::open(&load(&migrated)?.store)?;
    assert!(store.instances()?.is_empty(), "nothing was left rented");
    Ok(())
}

#[test]
fn a_destination_that_cannot_be_reached_fails_rather_than_hanging() -> Result<()> {
    // `BatchMode=yes` is what makes this prompt rather than block: a server that
    // is not there refuses at once instead of waiting on a password.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated = migrating(dir.path(), &far_root, SEGMENTS);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));

    // A port nothing listens on, taken and released.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address")
        .port();
    let started = Instant::now();
    let output = migrate_over(
        &migrated,
        &format!("{}@127.0.0.1:{port}", whoami()),
        &dir.path().join("no-such-agent"),
    );
    assert_ne!(
        output.status.code(),
        Some(0),
        "an unreachable far side fails"
    );
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "it failed rather than hanging: {:?}",
        started.elapsed()
    );
    Ok(())
}

#[test]
fn a_malformed_stub_endpoint_is_refused_by_name() -> Result<()> {
    // Set but unparseable means the caller meant to cross a hop, so it fails
    // instead of quietly falling back to the in-process path.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let migrated = migrating(dir.path(), &dir.path().join("far"), SEGMENTS);
    let output = migrate_over(
        &migrated,
        "not-an-endpoint",
        &dir.path().join("no-such-agent"),
    );
    assert_ne!(output.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("SIMA_STUB_SSH"),
        "names the variable: {stderr}"
    );
    Ok(())
}

#[test]
fn the_harness_leaves_no_server_behind() {
    // The guard is what makes the tier safe to run anywhere: a failing
    // assertion must not leave a listening server on the machine.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let binaries = Path::new(far_binary())
        .parent()
        .expect("a binary directory");
    let pid = {
        let sshd = Sshd::start(dir.path(), binaries);
        assert!(sshd.alive(), "the server runs while the test holds it");
        sshd.pid
    };
    assert!(
        poll_for(Duration::from_secs(10), || (!process_alive(pid))
            .then_some(()))
        .is_some(),
        "the server outlived its guard"
    );
}
