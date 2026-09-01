//! `sima follow`: a search's events on stdout, line by line, as the journal
//! gains them.
//!
//! It is the pipeable counterpart of the tui: no terminal, no raw mode, no
//! keys — one line per event, then an exit carrying the search's outcome. The
//! lines are the ones the tui's log shows, formatted through the same
//! renderer, so the two views of a running search agree.

use std::io::Write;
use std::process::ExitCode;
use std::thread::sleep;
use std::time::Duration;

use sima_core::Result;
use sima_pipeline::{SearchState, SearchStatus};

use crate::Target;
use crate::render;

/// How long the loop waits before polling again when nothing has arrived.
const TICK: Duration = Duration::from_millis(100);

/// `sima follow <config>`: streams the search's events and exits when the search
/// ends, with the outcome's own exit code.
pub fn follow_command(target: &Target) -> ExitCode {
    match follow(target) {
        Ok(code) => ExitCode::from(code),
        Err(e) => crate::report(e),
    }
}

/// Follows the target's search to its end, printing each event, and returns the
/// exit code its final state carries.
///
/// The first poll replays the search's history, so a finished search prints what it
/// recorded and leaves immediately. A search still in flight streams until a
/// terminal event arrives; one whose journal ends mid-search with nobody driving
/// it — a crashed orchestrator — prints its history and leaves successfully,
/// since such a search is resumable rather than failed, and `sima status` is
/// where that state is read.
fn follow(target: &Target) -> Result<u8> {
    let mut feed = crate::feed(target)?;
    let mut status = SearchStatus::new(feed.info().search);
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
                return Ok(crate::state_exit_code(&status.state));
            }
        }
        if let Some(code) = ended(&status.state) {
            return Ok(code);
        }
        if records.is_empty() {
            // The stream has drained. A search nobody drives will gain nothing
            // more, whatever state its journal ended in.
            if feed.holder()?.is_none() {
                return Ok(crate::state_exit_code(&status.state));
            }
            sleep(TICK);
        }
    }
}

/// The exit code of a search that reached a terminal state, or `None` while it
/// is still in progress.
fn ended(state: &SearchState) -> Option<u8> {
    match state {
        SearchState::InProgress => None,
        terminal => Some(crate::state_exit_code(terminal)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_search_state_maps_to_its_exit_code() {
        assert_eq!(crate::state_exit_code(&SearchState::Finalized), 0);
        assert_eq!(
            crate::state_exit_code(&SearchState::Failed {
                task: "aa".to_string(),
                reason: "rejected".to_string(),
            }),
            crate::EXIT_FAILED
        );
        assert_eq!(
            crate::state_exit_code(&SearchState::Interrupted),
            crate::EXIT_INTERRUPTED
        );
        // A drained stream over a search nobody drives: resumable, not failed.
        assert_eq!(crate::state_exit_code(&SearchState::InProgress), 0);
    }

    #[test]
    fn only_a_terminal_state_ends_the_follow() {
        assert_eq!(ended(&SearchState::InProgress), None);
        assert_eq!(ended(&SearchState::Finalized), Some(0));
        assert_eq!(
            ended(&SearchState::Interrupted),
            Some(crate::EXIT_INTERRUPTED)
        );
    }
}
