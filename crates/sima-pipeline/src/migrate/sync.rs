//! Store synchronization over a destination: the two halves of a `Store::sync`
//! session joined by a spawned process.
//!
//! The sync engine is symmetric — both sides advertise what they hold within a
//! task key set, compute `want = theirs − mine`, and stream the difference — so
//! a push and a pull are the same call at two moments:
//!
//! ```text
//!    push                                   pull
//!    local:  segments 0,1                   local:  segments 0,1
//!    far:    (empty)                        far:    segments 0..5
//!    ──────────────────────────             ──────────────────────────
//!    local advertises 0,1                   local advertises 0,1
//!    far advertises nothing                 far advertises 0..5
//!    far wants 0,1 → receives               local wants 2..5 → receives
//!    local wants nothing                    far wants nothing
//! ```
//!
//! **The key set is derived independently on each side** from (config, store
//! state), the same rule the scheduler's frontier follows. No key list crosses
//! the wire and the sync protocol is unchanged. It converges because whichever
//! side holds more also derives more keys, and it advertises the records the
//! other lacks.
//!
//! What differs between the two directions is the object scope, which is the
//! caller's to choose: a push names the frontier states, a pull advertises
//! everything its records reference. This module does not know which it is
//! serving.

use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::process::{Child, Command, Stdio};

use sima_core::{Error, Result};
use sima_store::{ObjectScope, Store, SyncReport, SyncRole};
use sima_transport::{SpawnMode, SshDestination};

use crate::config::load;
use crate::task_keys::task_keys;

/// The far half: serves one sync session over `input` and `output`.
///
/// It loads the config on this host, so the run id, the store path, and the
/// lock are all this host's, then takes the run lock for the session's
/// duration — a sync writes records and objects, and the store admits one
/// writer. It derives the key set from (config, store state) the same way the
/// initiator does over its own store.
///
/// Nothing but protocol frames may reach `output`: the caller wires it to
/// stdout, and every diagnostic goes to stderr, which ssh keeps on its own
/// channel.
///
/// The scope is [`ObjectScope::Referenced`] in both directions, because this
/// side advertises what it holds and holds only what it was sent.
pub fn sync_serve(
    config: &Path,
    input: &mut dyn std::io::Read,
    output: &mut dyn std::io::Write,
) -> Result<SyncReport> {
    let loaded = load(config)?;
    let store = Store::open(&loaded.store)?;
    let run = loaded.run.id();
    // The lock is taken before the keys are derived: deriving them writes the
    // run's spec objects, which is a store write like any other.
    let _lock = store.acquire_run_lock(&run)?;
    let keys = task_keys(&loaded, &store)?;
    store.sync(
        &keys,
        ObjectScope::Referenced,
        input,
        output,
        SyncRole::Responder,
    )
}

/// The near half: spawns `sima sync-serve` on the destination and runs one
/// session against it.
///
/// One function serves both directions. The caller chooses the scope — a push
/// names the frontier states, a pull advertises everything its records
/// reference — and the key set is this side's own derivation, never the far
/// side's.
///
/// Stderr is inherited rather than captured, so a far-side diagnostic — a
/// missing binary, a store that will not open, a lock another process holds —
/// reaches the operator's terminal while the session is still running.
pub(crate) fn sync_over(
    store: &Store,
    keys: &[sima_model::TaskKey],
    scope: ObjectScope<'_>,
    reach: &Reach,
    far_config: &str,
) -> Result<SyncReport> {
    let argv = reach.sync_serve_argv(far_config);
    let (program, args) = argv.split_first().expect("the argv names a program");
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| Error::Validation(format!("cannot run {program:?} to sync: {e}")))?;
    // The pipes exist iff the spawn configured them; taking them cannot fail
    // past a successful spawn.
    let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
        return Err(kill(child, "the sync process has no piped stdio"));
    };
    let mut writer = BufWriter::new(stdin);
    let mut reader = BufReader::new(stdout);
    // A session error still reaps the child: a far half left holding the run
    // lock would fail the next session on this run.
    let report = store.sync(keys, scope, &mut reader, &mut writer, SyncRole::Initiator);
    drop(writer);
    let status = child
        .wait()
        .map_err(|e| Error::Validation(format!("cannot reap {program:?}: {e}")))?;
    match (report, status.success()) {
        (Ok(report), true) => Ok(report),
        // A far half that exited non-zero is the cause, and this side's own
        // session error is the symptom of its stream ending, so the exit is
        // what the operator is told about. Its diagnostics already reached
        // stderr, which is inherited.
        (_, false) => Err(Error::Validation(format!(
            "the far half of the sync failed: {program:?} exited with {status}"
        ))),
        (Err(error), true) => Err(error),
    }
}

/// Kills a child whose stdio could not be taken, and names why.
fn kill(mut child: Child, reason: &str) -> Error {
    let _ = child.kill();
    let _ = child.wait();
    Error::Validation(reason.to_string())
}

/// How a far-side command is reached: over ssh, or as a program on this
/// machine.
///
/// The distinction is [`SpawnMode`]'s, for a different program: a worker is
/// launched the same two ways, and the stub provider's testing path is a local
/// spawn in both cases, so every layer above exercises identically with no
/// network.
#[derive(Debug, Clone)]
pub(crate) enum Reach {
    /// Over ssh at this destination; the far side's own `sima` runs the verb.
    Ssh {
        /// Where the command lands.
        destination: SshDestination,
        /// The `sima` binary on that machine.
        binary: String,
    },
    /// A `sima` binary on this machine, for a destination reached without a
    /// hop.
    Here(std::path::PathBuf),
}

impl Reach {
    /// How a destination's `binary` is reached under `mode`.
    pub(crate) fn new(mode: &SpawnMode, destination: &SshDestination, binary: &str) -> Reach {
        match mode {
            SpawnMode::Ssh => Reach::Ssh {
                destination: destination.clone(),
                binary: binary.to_string(),
            },
            SpawnMode::Local(_) => Reach::Here(std::path::PathBuf::from(binary)),
        }
    }

    /// The argv that serves one sync session over `far_config`, a path on the
    /// far side that travels unresolved.
    pub(crate) fn sync_serve_argv(&self, far_config: &str) -> Vec<String> {
        self.verb_argv(&["sync-serve", far_config])
    }

    /// The argv that serves the live follow stream of the run `far_config`
    /// names.
    pub(crate) fn follow_serve_argv(&self, far_config: &str) -> Vec<String> {
        self.verb_argv(&["follow-serve", far_config])
    }

    /// The argv that runs a shell on the far side, reading its script from
    /// stdin.
    ///
    /// Feeding the script over stdin is what keeps the far-side operations free
    /// of quoting: an ssh command line is re-parsed by the far shell, so a
    /// script passed as an argument would have to survive two rounds of word
    /// splitting, while one arriving on stdin is read verbatim.
    pub(crate) fn shell_argv(&self) -> Vec<String> {
        match self {
            Reach::Ssh { destination, .. } => {
                let mut argv = destination.prefix();
                argv.push("sh".to_string());
                argv
            }
            Reach::Here(_) => vec!["/bin/sh".to_string()],
        }
    }

    /// The `sima` binary that drives the run on the far side.
    pub(crate) fn binary(&self) -> String {
        match self {
            Reach::Ssh { binary, .. } => binary.clone(),
            Reach::Here(binary) => binary.to_string_lossy().into_owned(),
        }
    }

    /// How the destination is named in an error: the ssh destination, or the
    /// binary standing in for a far side reached without a hop.
    pub(crate) fn label(&self) -> String {
        match self {
            Reach::Ssh { destination, .. } => destination.host().to_string(),
            Reach::Here(binary) => binary.to_string_lossy().into_owned(),
        }
    }

    /// The argv that runs `sima <args…>` on the destination.
    pub(crate) fn verb_argv(&self, args: &[&str]) -> Vec<String> {
        let mut argv = match self {
            Reach::Ssh {
                destination,
                binary,
            } => {
                let mut argv = destination.prefix();
                argv.push(binary.clone());
                argv
            }
            Reach::Here(binary) => vec![binary.to_string_lossy().into_owned()],
        };
        argv.extend(args.iter().map(|arg| (*arg).to_string()));
        argv
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_local_reach_runs_the_binary_directly() {
        let reach = Reach::new(
            &SpawnMode::Local("/tmp/sima".into()),
            &SshDestination::known("unused"),
            "/build/sima",
        );
        assert_eq!(
            reach.sync_serve_argv("far/sima.toml"),
            ["/build/sima", "sync-serve", "far/sima.toml"]
        );
    }

    #[test]
    fn an_ssh_reach_wraps_the_far_binary_in_the_destination_s_invocation() {
        let reach = Reach::new(
            &SpawnMode::Ssh,
            &SshDestination::rented("203.0.113.7", 41022, "root"),
            "sima",
        );
        assert_eq!(
            reach.sync_serve_argv("~/sima-runs/abc/sima.toml"),
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "ConnectTimeout=10",
                "-p",
                "41022",
                "root@203.0.113.7",
                "--",
                "sima",
                "sync-serve",
                "~/sima-runs/abc/sima.toml",
            ]
        );
    }

    #[test]
    fn the_far_config_path_travels_unresolved() {
        // It names a path on the far side, and the far side is what interprets
        // it: a tilde is the far shell's to expand.
        let reach = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        let argv = reach.sync_serve_argv("~/sima-runs/abc/sima.toml");
        assert_eq!(
            argv.last().map(String::as_str),
            Some("~/sima-runs/abc/sima.toml")
        );
    }

    #[test]
    fn a_shell_argv_reaches_a_far_shell_that_reads_its_script_from_stdin() {
        // No script on the command line, in either form: the far-side
        // operations write theirs to the shell's stdin, so nothing is quoted
        // for the hop.
        let over_ssh = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        assert_eq!(
            over_ssh.shell_argv(),
            ["ssh", "-o", "BatchMode=yes", "gpubox", "--", "sh"]
        );
        let here = Reach::new(
            &SpawnMode::Local("/tmp/sima-worker".into()),
            &SshDestination::known("unused"),
            "/build/sima",
        );
        assert_eq!(here.shell_argv(), ["/bin/sh"]);
    }

    #[test]
    fn the_binary_and_the_label_read_the_same_two_forms() {
        let over_ssh = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        assert_eq!(over_ssh.binary(), "sima");
        assert_eq!(over_ssh.label(), "gpubox");
        let here = Reach::new(
            &SpawnMode::Local("/tmp/sima-worker".into()),
            &SshDestination::known("unused"),
            "/build/sima",
        );
        assert_eq!(here.binary(), "/build/sima");
        // A far side reached without a hop has no destination to name, so its
        // binary is what an error identifies it by.
        assert_eq!(here.label(), "/build/sima");
    }

    #[test]
    fn a_follow_argv_serves_the_run_the_far_config_names() {
        let reach = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        let argv = reach.follow_serve_argv("~/sima-runs/abc/sima.toml");
        let binary = argv.iter().position(|a| a == "sima").expect("the binary");
        assert_eq!(
            &argv[binary..],
            ["sima", "follow-serve", "~/sima-runs/abc/sima.toml"]
        );
    }

    #[test]
    fn a_verb_argv_carries_every_argument_after_the_binary() {
        let reach = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        let argv = reach.verb_argv(&["run", "sima.toml"]);
        let binary = argv.iter().position(|a| a == "sima").expect("the binary");
        assert_eq!(&argv[binary..], ["sima", "run", "sima.toml"]);
    }

    /// A far half that dies at once, and one that never speaks: the two ways a
    /// spawned command can fail this side of the protocol.
    mod against_a_failing_far_half {
        use super::*;

        /// A fresh store and the empty key set a sync over nothing uses.
        fn store() -> (tempfile::TempDir, Store) {
            let dir = tempfile::tempdir().expect("temp dir");
            let store = Store::open(dir.path()).expect("open store");
            (dir, store)
        }

        #[test]
        fn a_far_half_that_exits_non_zero_is_named_by_its_command() {
            // The local session fails too — its stream ended — but the exit is
            // the cause, so that is what the operator is told.
            let (_dir, store) = store();
            let reach = Reach::Here("/bin/false".into());
            match sync_over(&store, &[], ObjectScope::Referenced, &reach, "far.toml") {
                Err(Error::Validation(message)) => {
                    assert!(
                        message.contains("/bin/false"),
                        "names the command: {message}"
                    );
                    assert!(message.contains("exited"), "names the exit: {message}");
                }
                other => panic!("expected a validation error, got {other:?}"),
            }
        }

        #[test]
        fn a_command_that_does_not_exist_fails_at_the_spawn() {
            let (_dir, store) = store();
            let reach = Reach::Here("/nonexistent/sima".into());
            match sync_over(&store, &[], ObjectScope::Referenced, &reach, "far.toml") {
                Err(Error::Validation(message)) => {
                    assert!(message.contains("/nonexistent/sima"), "{message}");
                }
                other => panic!("expected a validation error, got {other:?}"),
            }
        }
    }
}
