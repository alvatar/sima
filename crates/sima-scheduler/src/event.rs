//! [`LifecycleEvent`]: the typed run-journal vocabulary.
//!
//! Each event serializes to one JSON line. Events are observational — the
//! journal records what happened, never run identity — so ids render as
//! lowercase hex strings and stats render as hex, and the event stream is
//! excluded from every equality criterion. The events a task emits trace its
//! lifecycle: queued, leased, then committed, or failed and retried, or
//! rejected, or faulted on an infrastructure error; a lease expiry and the
//! run-level start/finalize/fail frame the whole.

use serde::{Deserialize, Serialize};
use sima_core::{Error, Result};

/// One entry in the run journal. The `event` tag names the variant; every id
/// and stats payload is a lowercase-hex string, since the journal is text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum LifecycleEvent {
    /// The run began, over `tasks` keys: every task key of the run, those
    /// already committed and those still to run.
    RunStarted { run: String, tasks: usize },
    /// A task entered the ready queue.
    Queued { task: String },
    /// A worker leased a task for one attempt.
    Leased {
        task: String,
        worker: u64,
        attempt: u32,
    },
    /// A task's result was committed, referencing `record`.
    Committed {
        task: String,
        record: String,
        stats_hex: String,
    },
    /// An attempt failed transiently.
    Failed {
        task: String,
        attempt: u32,
        reason: String,
        stats_hex: String,
    },
    /// A failed task was re-enqueued for another attempt.
    Retried { task: String, next_attempt: u32 },
    /// A task failed definitively and will not be retried.
    Rejected {
        task: String,
        attempt: u32,
        reason: String,
        stats_hex: String,
    },
    /// An infrastructure fault hit this task's attempt: an executor error, a
    /// commit failure, or an input-state load failure. The run terminates with
    /// an error.
    Faulted {
        task: String,
        attempt: u32,
        error: String,
    },
    /// A lease outlived `attempt_timeout`; detection only, no preemption.
    LeaseExpired {
        task: String,
        worker: u64,
        elapsed_ms: u64,
    },
    /// Every task committed and the manifest was written.
    RunFinalized { run: String, committed: usize },
    /// A definitive failure terminated the run; no manifest was written.
    RunFailed {
        run: String,
        task: String,
        reason: String,
    },
    /// The caller interrupted the run: in-flight attempts drained and
    /// committed, and no manifest was written, so the store is resumable.
    RunInterrupted { run: String },
}

impl LifecycleEvent {
    /// Renders the event as one journal line. `serde_json::to_string` emits a
    /// single line with no embedded newline — string fields are JSON-escaped —
    /// so it satisfies the journal's one-event-per-line rule directly. A
    /// serialization failure maps to [`Error::Encoding`].
    pub fn to_line(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| Error::Encoding(format!("lifecycle event does not serialize: {e}")))
    }

    /// Parses a journal line written by [`to_line`](Self::to_line) back into an
    /// event. A line that does not parse is [`Error::Encoding`].
    pub fn from_line(line: &str) -> Result<LifecycleEvent> {
        serde_json::from_str(line)
            .map_err(|e| Error::Encoding(format!("lifecycle event does not parse: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_line_is_a_single_line() -> Result<()> {
        let event = LifecycleEvent::Leased {
            task: "ab".repeat(32),
            worker: 3,
            attempt: 1,
        };
        let line = event.to_line()?;
        assert!(!line.contains('\n'));
        assert!(!line.contains('\r'));
        assert!(line.contains("\"event\":\"leased\""));
        Ok(())
    }

    #[test]
    fn a_reason_with_a_newline_stays_one_line() -> Result<()> {
        // A panic reason may carry a newline; JSON escaping keeps the physical
        // line intact so the journal framing holds.
        let event = LifecycleEvent::Rejected {
            task: "cd".repeat(32),
            attempt: 0,
            reason: "panic: line one\nline two".to_string(),
            stats_hex: String::new(),
        };
        let line = event.to_line()?;
        assert!(!line.contains('\n'));
        Ok(())
    }

    #[test]
    fn line_round_trips_through_serde() -> Result<()> {
        let event = LifecycleEvent::Committed {
            task: "11".repeat(32),
            record: "22".repeat(32),
            stats_hex: "00000000".to_string(),
        };
        let line = event.to_line()?;
        assert_eq!(LifecycleEvent::from_line(&line)?, event);
        Ok(())
    }

    /// One instance of every variant, for exhaustiveness over the vocabulary.
    fn every_variant() -> Vec<LifecycleEvent> {
        let task = "ab".repeat(32);
        let run = "cd".repeat(32);
        vec![
            LifecycleEvent::RunStarted {
                run: run.clone(),
                tasks: 3,
            },
            LifecycleEvent::Queued { task: task.clone() },
            LifecycleEvent::Leased {
                task: task.clone(),
                worker: 1,
                attempt: 0,
            },
            LifecycleEvent::Committed {
                task: task.clone(),
                record: "ef".repeat(32),
                stats_hex: "0011".to_string(),
            },
            LifecycleEvent::Failed {
                task: task.clone(),
                attempt: 0,
                reason: "flaky".to_string(),
                stats_hex: String::new(),
            },
            LifecycleEvent::Retried {
                task: task.clone(),
                next_attempt: 1,
            },
            LifecycleEvent::Rejected {
                task: task.clone(),
                attempt: 1,
                reason: "rejected".to_string(),
                stats_hex: String::new(),
            },
            LifecycleEvent::Faulted {
                task: task.clone(),
                attempt: 1,
                error: "io error".to_string(),
            },
            LifecycleEvent::LeaseExpired {
                task: task.clone(),
                worker: 1,
                elapsed_ms: 100,
            },
            LifecycleEvent::RunFinalized {
                run: run.clone(),
                committed: 3,
            },
            LifecycleEvent::RunFailed {
                run: run.clone(),
                task,
                reason: "rejected".to_string(),
            },
            LifecycleEvent::RunInterrupted { run },
        ]
    }

    #[test]
    fn every_variant_round_trips_through_from_line() -> Result<()> {
        for event in every_variant() {
            let line = event.to_line()?;
            assert_eq!(LifecycleEvent::from_line(&line)?, event, "{line}");
        }
        Ok(())
    }

    #[test]
    fn from_line_rejects_lines_that_are_not_events() {
        for line in [
            "",
            "not json",
            "{}",
            "{\"event\":\"no_such_event\"}",
            "{\"event\":\"queued\"}",
        ] {
            assert!(
                matches!(LifecycleEvent::from_line(line), Err(Error::Encoding(_))),
                "{line:?} must be rejected"
            );
        }
    }
}
