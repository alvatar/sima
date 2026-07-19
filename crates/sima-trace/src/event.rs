//! [`Event`]: the typed structured-event vocabulary.
//!
//! Each event serializes to one JSON line. Events are observational — the
//! journal records what happened, never run identity — so ids render as
//! lowercase hex strings and stats render as hex, and the event stream is
//! excluded from every equality criterion. The events a task emits trace its
//! lifecycle: queued, leased, then committed, or failed and retried, or
//! rejected, or faulted on an infrastructure error; a lease expiry and the
//! run-level start/finalize/fail frame the whole. Alongside the lifecycle,
//! a [`Diagnostic`](Event::Diagnostic) carries observational text attributed
//! to the component and work unit it came from.

use serde::{Deserialize, Serialize};

/// Severity of a [`Diagnostic`](Event::Diagnostic) line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Info,
    Warn,
    Error,
}

/// One entry in the run journal. The `event` tag names the variant; every id
/// and stats payload is a lowercase-hex string, since the journal is text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// The run began, over `tasks` keys: every task key of the run, those
    /// already committed and those still to run. `committed` is how many of
    /// them the store already answered when the session started; it comes
    /// from the records, so it holds even against this journal, which a crash
    /// can leave short of the commits it describes. A line lacking the field
    /// reads as zero.
    RunStarted {
        run: String,
        tasks: usize,
        #[serde(default)]
        committed: usize,
    },
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
    /// A lease outlived `attempt_timeout`: the attempt's worker process is
    /// killed and the attempt fails transiently.
    LeaseExpired {
        task: String,
        worker: u64,
        elapsed_ms: u64,
    },
    /// A checkpoint save or load failed. Checkpointing is an optimization,
    /// never a task outcome: execution continues and the attempt's result is
    /// unaffected, so this event is the only trace.
    CheckpointDegraded { task: String, error: String },
    /// A worker's child reported the device it computes on, at every spawn and
    /// respawn. The device name and driver version are the child's own,
    /// verbatim; a domain that uses no device reports both empty. The host is
    /// the parent's account of where the worker's pool runs — empty for a local
    /// slot, the configured destination for a remote one. A line lacking driver
    /// or host reads each absent field as empty.
    WorkerBound {
        worker: u64,
        device: String,
        #[serde(default)]
        driver: String,
        #[serde(default)]
        host: String,
    },
    /// A chain's device class was absent from the run's devices, so its work
    /// moved to a class that is present. Classes render `vendor:device`.
    ChainRebound {
        chain: u64,
        from: String,
        to: String,
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
    /// A correlated diagnostic line: observational text attributed to the
    /// component and work unit it came from. Context keys are optional
    /// because a diagnostic may precede any lease (worker startup) or
    /// follow the run's end.
    Diagnostic {
        level: Level,
        source: String,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        worker: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_serializes_snake_case() {
        for (level, text) in [
            (Level::Info, "\"info\""),
            (Level::Warn, "\"warn\""),
            (Level::Error, "\"error\""),
        ] {
            let json = serde_json::to_string(&level).expect("serialize level");
            assert_eq!(json, text);
        }
    }

    #[test]
    fn a_diagnostic_round_trips_with_context_keys() {
        let event = Event::Diagnostic {
            level: Level::Error,
            source: "panic".to_string(),
            message: "thread panicked".to_string(),
            worker: Some(3),
            host: Some("gpubox".to_string()),
            task: Some("ab".repeat(32)),
        };
        let json = serde_json::to_string(&event).expect("serialize diagnostic");
        let back: Event = serde_json::from_str(&json).expect("parse diagnostic");
        assert_eq!(back, event);
    }

    #[test]
    fn a_diagnostic_round_trips_without_context_keys() {
        let event = Event::Diagnostic {
            level: Level::Info,
            source: "worker stderr".to_string(),
            message: "starting up".to_string(),
            worker: None,
            host: None,
            task: None,
        };
        let json = serde_json::to_string(&event).expect("serialize diagnostic");
        let back: Event = serde_json::from_str(&json).expect("parse diagnostic");
        assert_eq!(back, event);
    }

    #[test]
    fn a_diagnostic_line_lacking_context_keys_parses() {
        // The keys are optional in the line itself, so a producer that knows
        // no context writes none.
        let line = r#"{"event":"diagnostic","level":"warn","source":"transport","message":"m"}"#;
        let event: Event = serde_json::from_str(line).expect("parse diagnostic");
        assert_eq!(
            event,
            Event::Diagnostic {
                level: Level::Warn,
                source: "transport".to_string(),
                message: "m".to_string(),
                worker: None,
                host: None,
                task: None,
            }
        );
    }

    #[test]
    fn absent_context_keys_are_omitted_from_the_line() {
        let event = Event::Diagnostic {
            level: Level::Info,
            source: "worker stderr".to_string(),
            message: "starting up".to_string(),
            worker: Some(1),
            host: None,
            task: None,
        };
        let json = serde_json::to_string(&event).expect("serialize diagnostic");
        assert!(json.contains("\"worker\":1"));
        assert!(!json.contains("host"));
        assert!(!json.contains("task"));
    }
}
