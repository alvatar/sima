//! [`Event`]: the typed structured-event vocabulary.
//!
//! Each event serializes to one JSON line. Events are observational — the
//! journal records what happened, never search identity — so ids and the stats
//! family blob render as lowercase hex strings, stats render as named
//! scalars, and the event stream is excluded from every equality criterion.
//! The events a task emits trace its
//! lifecycle: queued, leased, then committed, or failed and retried, or
//! rejected, or faulted on an infrastructure error; a lease expiry and the
//! search-level start/finalize/fail frame the whole. Alongside the lifecycle,
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

/// One entry in the search journal. The `event` tag names the variant; every id
/// renders as a lowercase-hex string, since the journal is text. `PartialEq`
/// only: an outcome event's [`StatScalar`] values are `f64`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// The search began, over `tasks` keys: every task key of the search, those
    /// already committed and those still to search. `committed` is how many of
    /// them the store already answered when the session started; it comes
    /// from the records, so it holds even against this journal, which a crash
    /// can leave short of the commits it describes.
    SearchStarted {
        search: String,
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
    /// commit failure, or an input-state load failure. The search terminates with
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
    /// A task's checkpoint was persisted, which is the one sign a long attempt
    /// gives that it is computing rather than wedged. Rate-limited where it is
    /// emitted — at most one per attempt per interval, however often the task
    /// saves — so a task saving every second neither floods the journal nor
    /// the terminal.
    Checkpointed { task: String, worker: u64 },
    /// A worker's child reported the device it computes on, at every spawn and
    /// respawn. The device name and driver version are the child's own,
    /// verbatim; a domain that uses no device reports both empty. The host is
    /// the parent's account of where the worker's pool searches — empty for a local
    /// slot, the configured destination for a remote one. The program is the
    /// digest the child answered for the program it searches, verbatim like the
    /// device and driver beside it; absent for a format this build answers, to
    /// which no program travelled.
    WorkerBound {
        worker: u64,
        device: String,
        driver: String,
        host: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        program: Option<String>,
    },
    /// The program that served a config-routed format for one session: the
    /// format it answered for, the file the config named, and the blake3
    /// digest of that file's bytes as lowercase hex. Provenance, exactly as
    /// [`WorkerBound`](Event::WorkerBound) records device and driver: the
    /// digest identifies the build that produced the session's results and
    /// enters no hash, so a search's identity stays what the program declares.
    ProgramBound {
        format: String,
        binary: String,
        digest: String,
    },
    /// A worker's child reported a driver other than the one this search's
    /// journal last recorded for the same host and device. The driver never
    /// enters identity, so results and checkpoints from the previous driver
    /// stay valid to the store; this event is the visible record that they
    /// and the new spawns' results come from different driver builds.
    DriverChanged {
        host: String,
        device: String,
        from: String,
        to: String,
    },
    /// A chain's device class was absent from the search's devices, so its work
    /// moved to a class that is present. Classes render `vendor:device`.
    ChainRebound {
        chain: u64,
        from: String,
        to: String,
    },
    /// Every task committed and the manifest was written.
    SearchFinalized { search: String, committed: usize },
    /// A definitive failure terminated the search; no manifest was written.
    SearchFailed {
        search: String,
        task: String,
        reason: String,
    },
    /// The caller interrupted the search: the attempts in flight were abandoned
    /// and no manifest was written, so the store is resumable — each abandoned
    /// attempt re-derives in the frontier, resuming from its checkpoint.
    SearchInterrupted { search: String },
    /// An offer was taken and a machine is being paid for, before it is up.
    /// `member` names the fleet member it was rented for, and is empty for a
    /// migration, which rents the one machine its destination names. A walk
    /// that takes an offer whose machine never comes up reports each one it
    /// takes.
    Renting {
        member: String,
        machine: String,
        gpu_model: String,
        gpu_count: u32,
        rate_microusd_hour: u64,
    },
    /// The wait for a rented machine to become usable began: it is paid for
    /// and is coming up, which on a fresh one includes pulling the worker
    /// image. Reported once per machine taken, by the acquisition that took
    /// it, however many times that machine is then polled. `member` names the
    /// fleet member it is waiting for — the members of one rental come up at
    /// once, so their lines interleave and each has to say whose it is — and
    /// is empty for a migration. `timeout_ms` is what the entry describing it
    /// states the wait may take.
    AwaitingMachine { member: String, timeout_ms: u64 },
    /// The search's objects are being sent to the machine that will drive it:
    /// the identity components, the frontier states, and the program when one
    /// travels. `member` names the fleet member receiving them, and is empty
    /// for a migration.
    SendingSearch { member: String, objects: usize },
    /// The program the search's format is served by is being installed on the
    /// machine. `member` names the machine installing it as the search addresses
    /// it — a fleet member by its entry and index, a machine of yours by its
    /// ssh destination — and is empty for a migration, whose destination
    /// installs it as its search loads.
    InstallingProgram { member: String },
    /// The far `sima search` is being started on the destination, which is what
    /// the migration waits on until its first journal line arrives.
    StartingSearch,
    /// The search was interrupted while its machines were still being acquired,
    /// so no further member was rented and every machine the acquisition
    /// already held was released. `released` is how many that was.
    AcquisitionAbandoned { released: usize },
    /// A rented machine came online: reported at supervisor start for
    /// each instance, and again for each replacement. `tag` is the rental's
    /// ledger key, `instance` the provider's id, `rate_microusd_hour` its
    /// hourly rate in micro-USD.
    InstanceOnline {
        tag: String,
        instance: String,
        gpu_model: String,
        gpu_count: u32,
        rate_microusd_hour: u64,
        host: String,
    },
    /// A rented machine was polled `Gone`: the provider no longer holds it.
    InstanceLost { tag: String, instance: String },
    /// A lost instance's replacement succeeded: the pool's target moved from
    /// the `from` instance to the `to` instance.
    InstanceReplaced {
        tag: String,
        from: String,
        to: String,
    },
    /// The search's rental spend reached its cap; no further rental is made and
    /// the search winds down.
    BudgetSpendExhausted {
        accrued_microusd: u64,
        cap_microusd: u64,
    },
    /// The search's rental phase reached its wall-clock deadline; no further
    /// rental is made and the search winds down.
    BudgetWallClockExhausted { deadline_ms: u64 },
    /// A correlated diagnostic line: observational text attributed to the
    /// component and work unit it came from. Context keys are optional
    /// because a diagnostic may precede any lease (worker startup) or
    /// follow the search's end.
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
    fn each_fleet_event_round_trips_through_a_journal_line() {
        let events = [
            Event::InstanceOnline {
                tag: "sima-abc-1".to_string(),
                instance: "i-42".to_string(),
                gpu_model: "RTX 4090".to_string(),
                gpu_count: 2,
                rate_microusd_hour: 412_000,
                host: "203.0.113.7".to_string(),
            },
            Event::InstanceLost {
                tag: "sima-abc-1".to_string(),
                instance: "i-42".to_string(),
            },
            Event::InstanceReplaced {
                tag: "sima-abc-1".to_string(),
                from: "i-42".to_string(),
                to: "i-43".to_string(),
            },
            Event::BudgetSpendExhausted {
                accrued_microusd: 5_100_000,
                cap_microusd: 5_000_000,
            },
            Event::BudgetWallClockExhausted {
                deadline_ms: 1_700_000_000_000,
            },
            Event::AcquisitionAbandoned { released: 2 },
            Event::AwaitingMachine {
                member: "cheap[1]".to_string(),
                timeout_ms: 600_000,
            },
        ];
        for event in events {
            let json = serde_json::to_string(&event).expect("serialize a rental event");
            // One JSON line, tagged by the variant.
            assert!(json.contains("\"event\":"), "{json}");
            let back: Event = serde_json::from_str(&json).expect("parse a rental event");
            assert_eq!(back, event);
        }
    }

    #[test]
    fn a_program_bound_event_names_the_format_the_file_and_its_digest() {
        let event = Event::ProgramBound {
            format: "acme.thing.v1".to_string(),
            binary: "/opt/acme/worker".to_string(),
            digest: "ab".repeat(32),
        };
        let json = serde_json::to_string(&event).expect("serialize program bound");
        assert!(json.contains("\"event\":\"program_bound\""), "{json}");
        let back: Event = serde_json::from_str(&json).expect("parse program bound");
        assert_eq!(back, event);
    }

    #[test]
    fn a_worker_bound_event_carries_the_program_the_worker_answered() {
        let event = Event::WorkerBound {
            worker: 3,
            device: "NVIDIA RTX PRO 2000".to_string(),
            driver: "580.65.6".to_string(),
            host: "gpubox".to_string(),
            program: Some("cd".repeat(32)),
        };
        let json = serde_json::to_string(&event).expect("serialize worker bound");
        assert!(json.contains("\"program\":"), "{json}");
        let back: Event = serde_json::from_str(&json).expect("parse worker bound");
        assert_eq!(back, event);
    }

    #[test]
    fn a_worker_bound_event_naming_no_program_writes_no_key() {
        // The shape every search of a format this build answers writes, and the
        // shape a journal written before the field existed already holds: the
        // key is absent both ways, so one reader serves both.
        let event = Event::WorkerBound {
            worker: 3,
            device: String::new(),
            driver: String::new(),
            host: String::new(),
            program: None,
        };
        let json = serde_json::to_string(&event).expect("serialize worker bound");
        assert!(!json.contains("program"), "{json}");
        let back: Event = serde_json::from_str(&json).expect("parse worker bound");
        assert_eq!(back, event);
    }

    #[test]
    fn a_worker_bound_line_without_the_program_key_parses() {
        // A journal line a build without the field wrote, read by this one.
        let line =
            r#"{"event":"worker_bound","worker":3,"device":"a device","driver":"1.0","host":""}"#;
        let back: Event = serde_json::from_str(line).expect("parse an older worker bound");
        assert_eq!(
            back,
            Event::WorkerBound {
                worker: 3,
                device: "a device".to_string(),
                driver: "1.0".to_string(),
                host: String::new(),
                program: None,
            }
        );
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
