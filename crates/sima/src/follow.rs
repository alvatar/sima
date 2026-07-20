//! `sima follow`: a run's events on stdout, line by line, as the journal
//! gains them.
//!
//! It is the pipeable counterpart of the tui: no terminal, no raw mode, no
//! keys — one line per event, then an exit carrying the run's outcome. The
//! lines are the ones the tui's log shows, formatted through the same
//! renderer, so the two views of a running run agree.

use std::io::Write;
use std::process::ExitCode;
use std::thread::sleep;
use std::time::Duration;

use sima_core::Result;
use sima_pipeline::{RunState, RunStatus};

use crate::Target;
use crate::render;

/// How long the loop waits before polling again when nothing has arrived.
const TICK: Duration = Duration::from_millis(100);

/// `sima follow <config>`: streams the run's events and exits when the run
/// ends, with the outcome's own exit code.
pub fn follow_command(target: &Target) -> ExitCode {
    match follow(target) {
        Ok(code) => ExitCode::from(code),
        Err(e) => crate::report(e),
    }
}

/// Follows the target's run to its end, printing each event, and returns the
/// exit code its final state carries.
///
/// The first poll replays the run's history, so a finished run prints what it
/// recorded and leaves immediately. A run still in flight streams until a
/// terminal event arrives; one whose journal ends mid-run with nobody driving
/// it — a crashed orchestrator — prints its history and leaves successfully,
/// since such a run is resumable rather than failed, and `sima status` is
/// where that state is read.
fn follow(target: &Target) -> Result<u8> {
    let mut feed = crate::feed(target)?;
    let mut status = RunStatus::new(feed.info().run);
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    loop {
        let records = feed.poll()?;
        for record in &records {
            status.apply(record);
            let Some(line) = render::describe(&record.event, status.committed, status.tasks) else {
                continue;
            };
            // A reader that closed the pipe (`sima follow ... | head`) has
            // stopped listening; there is nothing left to stream to it.
            if !crate::line_written(writeln!(out, "{line}"))? {
                return Ok(exit_code(&status.state));
            }
        }
        if let Some(code) = ended(&status.state) {
            return Ok(code);
        }
        if records.is_empty() {
            // The stream has drained. A run nobody drives will gain nothing
            // more, whatever state its journal ended in.
            if feed.holder()?.is_none() {
                return Ok(exit_code(&status.state));
            }
            sleep(TICK);
        }
    }
}

/// The exit code of a run that reached a terminal state, or `None` while it
/// is still in progress.
fn ended(state: &RunState) -> Option<u8> {
    match state {
        RunState::InProgress => None,
        terminal => Some(exit_code(terminal)),
    }
}

/// The exit code a run's state carries — the mapping `run` and `tui` share,
/// over the state a journal projects rather than the outcome an orchestrator
/// returns. A run still in progress when its stream drains is resumable, not
/// failed, so it leaves successfully.
fn exit_code(state: &RunState) -> u8 {
    match state {
        RunState::Finalized | RunState::InProgress => 0,
        RunState::Failed { .. } => crate::EXIT_FAILED,
        RunState::Interrupted => crate::EXIT_INTERRUPTED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_run_state_maps_to_its_exit_code() {
        assert_eq!(exit_code(&RunState::Finalized), 0);
        assert_eq!(
            exit_code(&RunState::Failed {
                task: "aa".to_string(),
                reason: "rejected".to_string(),
            }),
            crate::EXIT_FAILED
        );
        assert_eq!(exit_code(&RunState::Interrupted), crate::EXIT_INTERRUPTED);
        // A drained stream over a run nobody drives: resumable, not failed.
        assert_eq!(exit_code(&RunState::InProgress), 0);
    }

    #[test]
    fn only_a_terminal_state_ends_the_follow() {
        assert_eq!(ended(&RunState::InProgress), None);
        assert_eq!(ended(&RunState::Finalized), Some(0));
        assert_eq!(ended(&RunState::Interrupted), Some(crate::EXIT_INTERRUPTED));
    }
}
