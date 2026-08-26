//! One store-sync session against a far half a command spawns.
//!
//! The engine below is symmetric and knows nothing about processes: it reads
//! and writes one pipe. This module is where the pipe comes from — a spawned
//! command whose stdin and stdout are the two halves — and where the far side's
//! exit is reconciled with this side's session outcome.
//!
//! Two callers reach it. A migration syncs a run's records and objects with the
//! store on its destination; a fleet delivery sends a program's objects to a
//! machine that is about to run it. Both spawn a far `sima`, both drive
//! [`Store::sync`] as the initiator, and both need the same reaping.

use std::io::{BufReader, BufWriter};
use std::process::{Child, Command, Stdio};

use sima_core::{Error, Result, own_process_group};
use sima_model::TaskKey;
use sima_store::{ObjectScope, Store, SyncReport, SyncRole};

/// Spawns `argv` and runs one sync session against it as the initiator,
/// advertising `keys` under `scope`.
///
/// Stderr is inherited rather than captured, so a far-side diagnostic — a
/// missing binary, a store that will not open, a lock another process holds —
/// reaches the operator's terminal while the session is still running.
pub(crate) fn sync_against(
    store: &Store,
    keys: &[TaskKey],
    scope: ObjectScope<'_>,
    argv: &[String],
) -> Result<SyncReport> {
    let (program, args) = argv.split_first().expect("the argv names a program");
    let mut child = own_process_group(&mut Command::new(program))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| Error::Transport(format!("cannot run {program:?} to sync: {e}")))?;
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
        .map_err(|e| Error::Transport(format!("cannot reap {program:?}: {e}")))?;
    match (report, status.success()) {
        (Ok(report), true) => Ok(report),
        // A far half that exited non-zero is the cause, and this side's own
        // session error is the symptom of its stream ending, so the exit is
        // what the operator is told about. Its diagnostics already reached
        // stderr, which is inherited.
        (_, false) => Err(Error::Transport(format!(
            "the far half of the sync failed: {program:?} exited with {status}"
        ))),
        (Err(error), true) => Err(error),
    }
}

/// Kills a child whose stdio could not be taken, and names why.
fn kill(mut child: Child, reason: &str) -> Error {
    let _ = child.kill();
    let _ = child.wait();
    Error::Transport(reason.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh store and the empty key set a sync over nothing uses.
    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open store");
        (dir, store)
    }

    #[test]
    fn a_far_half_that_exits_non_zero_is_named_by_its_command() {
        // The local session fails too — its stream ended — but the exit is the
        // cause, so that is what the operator is told.
        let (_dir, store) = store();
        match sync_against(
            &store,
            &[],
            ObjectScope::Referenced,
            &["/bin/false".to_string()],
        ) {
            Err(Error::Transport(message)) => {
                assert!(
                    message.contains("/bin/false"),
                    "names the command: {message}"
                );
                assert!(message.contains("exited"), "names the exit: {message}");
            }
            other => panic!("expected a transport error, got {other:?}"),
        }
    }

    #[test]
    fn a_command_that_does_not_exist_fails_at_the_spawn() {
        let (_dir, store) = store();
        match sync_against(
            &store,
            &[],
            ObjectScope::Referenced,
            &["/nonexistent/sima".to_string()],
        ) {
            Err(Error::Transport(message)) => {
                assert!(message.contains("/nonexistent/sima"), "{message}");
            }
            other => panic!("expected a transport error, got {other:?}"),
        }
    }
}
