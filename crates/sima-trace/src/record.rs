//! [`Record`]: the journal line type — an event plus its append timestamp.

use serde::{Deserialize, Serialize};
use sima_core::{Error, Result};

use crate::event::Event;

/// One journal line: the event plus the wall-clock stamp the collector
/// thread applied when the line was appended. The event's fields flatten
/// into the line, so a lifecycle line keeps the exact shape it always had,
/// with `ts_ms` as one more top-level key:
/// `{"ts_ms":1234,"event":"leased","task":"ab…","worker":3,"attempt":1}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// Wall-clock milliseconds since the Unix epoch, stamped by the
    /// collector thread when the line is appended. Absent on lines
    /// written before the field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts_ms: Option<u64>,
    #[serde(flatten)]
    pub event: Event,
}

impl Record {
    /// Renders the record as one journal line. `serde_json::to_string` emits a
    /// single line with no embedded newline — string fields are JSON-escaped —
    /// so it satisfies the journal's one-event-per-line rule directly. A
    /// serialization failure maps to [`Error::Encoding`].
    pub fn to_line(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| Error::Encoding(format!("journal record does not serialize: {e}")))
    }

    /// Parses a journal line written by [`to_line`](Self::to_line) back into a
    /// record. A line predating `ts_ms` parses with `ts_ms: None`. A line that
    /// does not parse is [`Error::Encoding`].
    pub fn from_line(line: &str) -> Result<Record> {
        serde_json::from_str(line)
            .map_err(|e| Error::Encoding(format!("journal record does not parse: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Level;

    /// A record as the collector writes it: stamped.
    fn stamped(event: Event) -> Record {
        Record {
            ts_ms: Some(1_234),
            event,
        }
    }

    #[test]
    fn to_line_is_a_single_line() -> Result<()> {
        let record = stamped(Event::Leased {
            task: "ab".repeat(32),
            worker: 3,
            attempt: 1,
        });
        let line = record.to_line()?;
        assert!(!line.contains('\n'));
        assert!(!line.contains('\r'));
        assert!(line.contains("\"event\":\"leased\""));
        assert!(line.contains("\"ts_ms\":1234"));
        Ok(())
    }

    #[test]
    fn a_reason_with_a_newline_stays_one_line() -> Result<()> {
        // A panic reason may carry a newline; JSON escaping keeps the physical
        // line intact so the journal framing holds.
        let record = stamped(Event::Rejected {
            task: "cd".repeat(32),
            attempt: 0,
            reason: "panic: line one\nline two".to_string(),
            stats_hex: String::new(),
        });
        let line = record.to_line()?;
        assert!(!line.contains('\n'));
        Ok(())
    }

    #[test]
    fn a_line_without_a_timestamp_parses_with_none() -> Result<()> {
        // Every journal written before `ts_ms` existed. The journal is
        // observational, so its old lines stay readable.
        let task = "ab".repeat(32);
        let line = format!(r#"{{"event":"queued","task":"{task}"}}"#);
        assert_eq!(
            Record::from_line(&line)?,
            Record {
                ts_ms: None,
                event: Event::Queued { task },
            }
        );
        Ok(())
    }

    #[test]
    fn an_unstamped_record_serializes_as_the_bare_event() -> Result<()> {
        // With no timestamp the record's line is byte-identical to the
        // event's own serialization: `ts_ms` is skipped, the event flattens.
        let event = Event::Queued {
            task: "ab".repeat(32),
        };
        let record = Record {
            ts_ms: None,
            event: event.clone(),
        };
        let event_json = serde_json::to_string(&event)
            .map_err(|e| Error::Encoding(format!("event does not serialize: {e}")))?;
        assert_eq!(record.to_line()?, event_json);
        Ok(())
    }

    #[test]
    fn a_run_started_line_without_a_commit_count_reads_as_none_committed() -> Result<()> {
        // A journal written before the field existed. The journal is
        // observational, so its old lines stay readable: the absent count
        // reads as no prior commits.
        let run = "cd".repeat(32);
        let line = format!(r#"{{"event":"run_started","run":"{run}","tasks":3}}"#);
        assert_eq!(
            Record::from_line(&line)?.event,
            Event::RunStarted {
                run,
                tasks: 3,
                committed: 0,
            }
        );
        Ok(())
    }

    #[test]
    fn a_worker_bound_line_without_driver_or_host_reads_them_as_empty() -> Result<()> {
        // A journal written before the driver and host fields existed. The
        // absent fields read as empty, so an old journal's device attribution
        // stays intact.
        let line = r#"{"event":"worker_bound","worker":2,"device":"NVIDIA RTX PRO 2000"}"#;
        assert_eq!(
            Record::from_line(line)?.event,
            Event::WorkerBound {
                worker: 2,
                device: "NVIDIA RTX PRO 2000".to_string(),
                driver: String::new(),
                host: String::new(),
            }
        );
        Ok(())
    }

    #[test]
    fn a_worker_bound_line_round_trips_driver_and_host() -> Result<()> {
        let record = stamped(Event::WorkerBound {
            worker: 4,
            device: "NVIDIA RTX PRO 2000".to_string(),
            driver: "580.65.6".to_string(),
            host: "gpubox".to_string(),
        });
        assert_eq!(Record::from_line(&record.to_line()?)?, record);
        Ok(())
    }

    #[test]
    fn line_round_trips_through_serde() -> Result<()> {
        let record = stamped(Event::Committed {
            task: "11".repeat(32),
            record: "22".repeat(32),
            stats_hex: "00000000".to_string(),
        });
        let line = record.to_line()?;
        assert_eq!(Record::from_line(&line)?, record);
        Ok(())
    }

    /// One instance of every variant, for exhaustiveness over the vocabulary.
    fn every_variant() -> Vec<Event> {
        let task = "ab".repeat(32);
        let run = "cd".repeat(32);
        vec![
            Event::RunStarted {
                run: run.clone(),
                tasks: 3,
                committed: 1,
            },
            Event::Queued { task: task.clone() },
            Event::Leased {
                task: task.clone(),
                worker: 1,
                attempt: 0,
            },
            Event::Committed {
                task: task.clone(),
                record: "ef".repeat(32),
                stats_hex: "0011".to_string(),
            },
            Event::Failed {
                task: task.clone(),
                attempt: 0,
                reason: "flaky".to_string(),
                stats_hex: String::new(),
            },
            Event::Retried {
                task: task.clone(),
                next_attempt: 1,
            },
            Event::Rejected {
                task: task.clone(),
                attempt: 1,
                reason: "rejected".to_string(),
                stats_hex: String::new(),
            },
            Event::Faulted {
                task: task.clone(),
                attempt: 1,
                error: "io error".to_string(),
            },
            Event::LeaseExpired {
                task: task.clone(),
                worker: 1,
                elapsed_ms: 100,
            },
            Event::CheckpointDegraded {
                task: task.clone(),
                error: "checkpoint dir is unwritable".to_string(),
            },
            Event::WorkerBound {
                worker: 4,
                device: "NVIDIA RTX PRO 2000".to_string(),
                driver: "580.65.6".to_string(),
                host: "gpubox".to_string(),
            },
            Event::ChainRebound {
                chain: 1,
                from: "10de:2c02".to_string(),
                to: "10de:2f04".to_string(),
            },
            Event::RunFinalized {
                run: run.clone(),
                committed: 3,
            },
            Event::RunFailed {
                run: run.clone(),
                task: task.clone(),
                reason: "rejected".to_string(),
            },
            Event::RunInterrupted { run },
            Event::Diagnostic {
                level: Level::Error,
                source: "panic".to_string(),
                message: "thread panicked".to_string(),
                worker: Some(1),
                host: Some("gpubox".to_string()),
                task: Some(task),
            },
            Event::Diagnostic {
                level: Level::Info,
                source: "worker stderr".to_string(),
                message: "starting up".to_string(),
                worker: None,
                host: None,
                task: None,
            },
        ]
    }

    #[test]
    fn every_variant_round_trips_through_from_line() -> Result<()> {
        for event in every_variant() {
            let record = stamped(event);
            let line = record.to_line()?;
            assert_eq!(Record::from_line(&line)?, record, "{line}");
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
                matches!(Record::from_line(line), Err(Error::Encoding(_))),
                "{line:?} must be rejected"
            );
        }
    }
}
