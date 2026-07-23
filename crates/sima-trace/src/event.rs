//! [`Event`]: the typed structured-event vocabulary.
//!
//! Each event serializes to one JSON line. Events are observational — the
//! journal records what happened, never run identity — so ids and the stats
//! family blob render as lowercase hex strings, stats render as named
//! scalars, and the event stream is excluded from every equality criterion.
//! The events a task emits trace its
//! lifecycle: queued, leased, then committed, or failed and retried, or
//! rejected, or faulted on an infrastructure error; a lease expiry and the
//! run-level start/finalize/fail frame the whole. Alongside the lifecycle,
//! a [`Diagnostic`](Event::Diagnostic) carries observational text attributed
//! to the component and work unit it came from.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Severity of a [`Diagnostic`](Event::Diagnostic) line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Info,
    Warn,
    Error,
}

/// One observational scalar in a task-outcome event: a name and its value.
/// The trace facade owns this representation so it carries no dependency on
/// the contracts crate; the scheduler maps the executor's stats into it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatScalar {
    /// The scalar's name, as the executor emitted it.
    pub name: String,
    /// The scalar's value. A non-finite value (a diverged candidate) writes
    /// `null` and reads back as `NaN`: `serde_json` cannot serialize `NaN` or
    /// an infinity, so a journal append never fails on one.
    #[serde(
        serialize_with = "serialize_value",
        deserialize_with = "deserialize_value"
    )]
    pub value: f64,
}

/// Serializes a finite value as a JSON number; a non-finite value as `null`.
fn serialize_value<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
    if value.is_finite() {
        serializer.serialize_f64(*value)
    } else {
        serializer.serialize_none()
    }
}

/// Reads a JSON number as the value; `null` (a non-finite value written by
/// [`serialize_value`]) as `NaN`.
fn deserialize_value<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    Ok(Option::<f64>::deserialize(deserializer)?.unwrap_or(f64::NAN))
}

/// One entry in the run journal. The `event` tag names the variant; every id
/// renders as a lowercase-hex string, since the journal is text. `PartialEq`
/// only: an outcome event's [`StatScalar`] values are `f64`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// The run began, over `tasks` keys: every task key of the run, those
    /// already committed and those still to run. `committed` is how many of
    /// them the store already answered when the session started; it comes
    /// from the records, so it holds even against this journal, which a crash
    /// can leave short of the commits it describes.
    RunStarted {
        run: String,
        tasks: usize,
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
    /// A task's result was committed, referencing `record`. `stats` are the
    /// executor's named scalars; `stats_blob_hex` is the family blob as hex.
    Committed {
        task: String,
        record: String,
        stats: Vec<StatScalar>,
        stats_blob_hex: String,
    },
    /// An attempt failed transiently. Stats cover the failed evaluation the
    /// same way they cover a success.
    Failed {
        task: String,
        attempt: u32,
        reason: String,
        stats: Vec<StatScalar>,
        stats_blob_hex: String,
    },
    /// A failed task was re-enqueued for another attempt.
    Retried { task: String, next_attempt: u32 },
    /// A task failed definitively and will not be retried.
    Rejected {
        task: String,
        attempt: u32,
        reason: String,
        stats: Vec<StatScalar>,
        stats_blob_hex: String,
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
    /// slot, the configured destination for a remote one.
    WorkerBound {
        worker: u64,
        device: String,
        driver: String,
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
    fn a_finite_scalar_round_trips_as_a_json_number() {
        let scalar = StatScalar {
            name: "population".to_string(),
            value: 0.5,
        };
        let json = serde_json::to_string(&scalar).expect("serialize scalar");
        assert!(json.contains("\"value\":0.5"));
        let back: StatScalar = serde_json::from_str(&json).expect("parse scalar");
        assert_eq!(back, scalar);
    }

    #[test]
    fn a_non_finite_scalar_serializes_to_null_and_reads_back_nan() {
        // serde_json cannot serialize NaN or an infinity, so the value field
        // writes null for a non-finite value and reads null back as NaN — a
        // diverged candidate can never fail a journal append.
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let scalar = StatScalar {
                name: "activity".to_string(),
                value,
            };
            let json = serde_json::to_string(&scalar).expect("serialize scalar");
            assert!(json.contains("\"value\":null"), "{json}");
            let back: StatScalar = serde_json::from_str(&json).expect("parse scalar");
            assert!(back.value.is_nan());
        }
    }

    #[test]
    fn a_committed_event_carries_scalars_and_a_blob_hex() {
        let event = Event::Committed {
            task: "ab".repeat(32),
            record: "cd".repeat(32),
            stats: vec![
                StatScalar {
                    name: "population".to_string(),
                    value: 0.5,
                },
                StatScalar {
                    name: "activity".to_string(),
                    value: 1.0e-4,
                },
            ],
            stats_blob_hex: "aabb".to_string(),
        };
        let json = serde_json::to_string(&event).expect("serialize committed");
        let back: Event = serde_json::from_str(&json).expect("parse committed");
        assert_eq!(back, event);
    }

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
