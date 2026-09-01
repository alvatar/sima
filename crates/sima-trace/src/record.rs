//! [`Record`]: the journal line type — an event plus its append timestamp.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sima_core::{Error, Result};

use crate::event::Event;

/// One journal line: the event plus the wall-clock stamp the collector
/// thread applied when the line was appended. The event's fields flatten
/// into the line, so the line is flat: the event's own keys sit beside
/// `ts_ms` at the top level:
/// `{"ts_ms":1234,"event":"leased","task":"ab…","worker":3,"attempt":1}`.
/// `PartialEq` only, following [`Event`], whose outcome variants carry `f64`
/// scalars.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    /// Wall-clock milliseconds since the Unix epoch, stamped by the
    /// collector thread when the line is appended.
    pub ts_ms: u64,
    // The event's fields sit at the top level of the line beside `ts_ms`.
    #[serde(flatten)]
    pub event: Event,
}

impl Record {
    /// The record for `event`, stamped with the wall clock read on the calling
    /// thread. The collector stamps every event it appends this way; a caller
    /// appending a line of its own — outside the collector's lifetime — reads
    /// the same clock through here. A clock before the epoch stamps zero.
    pub fn stamped(event: Event) -> Record {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since_epoch| since_epoch.as_millis() as u64);
        Record { ts_ms, event }
    }

    /// Renders the record as one journal line. `serde_json::to_string` emits a
    /// single line with no embedded newline — string fields are JSON-escaped —
    /// so it satisfies the journal's one-event-per-line rule directly. A
    /// serialization failure maps to [`Error::Encoding`].
    pub fn to_line(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| Error::Encoding(format!("journal record does not serialize: {e}")))
    }

    /// Parses a journal line written by [`to_line`](Self::to_line) back into a
    /// record. Every line carries `ts_ms`; a line lacking it, like any line
    /// that does not parse, is [`Error::Encoding`].
    pub fn from_line(line: &str) -> Result<Record> {
        serde_json::from_str(line)
            .map_err(|e| Error::Encoding(format!("journal record does not parse: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Level, StatScalar};

    /// A record at a fixed stamp, so a line's text is the test's to predict.
    fn fixed_stamp(event: Event) -> Record {
        Record {
            ts_ms: 1_234,
            event,
        }
    }

    #[test]
    fn to_line_is_a_single_line() -> Result<()> {
        let record = fixed_stamp(Event::Leased {
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
        let record = fixed_stamp(Event::Rejected {
            task: "cd".repeat(32),
            attempt: 0,
            reason: "panic: line one\nline two".to_string(),
            stats: Vec::new(),
            stats_blob_hex: String::new(),
        });
        let line = record.to_line()?;
        assert!(!line.contains('\n'));
        Ok(())
    }

    #[test]
    fn a_line_without_a_timestamp_is_rejected() {
        // Every line the collector writes is stamped, so a line missing the
        // stamp is a malformed line, not a shape to accommodate.
        let task = "ab".repeat(32);
        let line = format!(r#"{{"event":"queued","task":"{task}"}}"#);
        assert!(matches!(Record::from_line(&line), Err(Error::Encoding(_))));
    }

    #[test]
    fn a_driver_changed_line_round_trips() -> Result<()> {
        let record = fixed_stamp(Event::DriverChanged {
            host: "gpubox".to_string(),
            device: "NVIDIA RTX PRO 2000".to_string(),
            from: "570.86.15".to_string(),
            to: "580.65.6".to_string(),
        });
        assert_eq!(Record::from_line(&record.to_line()?)?, record);
        Ok(())
    }

    #[test]
    fn a_driver_changed_line_without_both_versions_is_rejected() {
        let line = r#"{"ts_ms":1234,"event":"driver_changed","host":"","device":"gpu","from":"570.86.15"}"#;
        assert!(matches!(Record::from_line(line), Err(Error::Encoding(_))));
    }

    #[test]
    fn a_search_started_line_without_a_commit_count_is_rejected() {
        let search = "cd".repeat(32);
        let line =
            format!(r#"{{"ts_ms":1234,"event":"search_started","search":"{search}","tasks":3}}"#);
        assert!(matches!(Record::from_line(&line), Err(Error::Encoding(_))));
    }

    #[test]
    fn a_worker_bound_line_without_driver_or_host_is_rejected() {
        let line =
            r#"{"ts_ms":1234,"event":"worker_bound","worker":2,"device":"NVIDIA RTX PRO 2000"}"#;
        assert!(matches!(Record::from_line(line), Err(Error::Encoding(_))));
    }

    #[test]
    fn a_worker_bound_line_round_trips_driver_and_host() -> Result<()> {
        let record = fixed_stamp(Event::WorkerBound {
            worker: 4,
            device: "NVIDIA RTX PRO 2000".to_string(),
            driver: "580.65.6".to_string(),
            host: "gpubox".to_string(),
            program: None,
        });
        assert_eq!(Record::from_line(&record.to_line()?)?, record);
        Ok(())
    }

    #[test]
    fn a_program_bound_line_without_a_digest_is_rejected() {
        let line = r#"{"ts_ms":1234,"event":"program_bound","format":"acme.thing.v1","binary":"/opt/acme/worker"}"#;
        assert!(matches!(Record::from_line(line), Err(Error::Encoding(_))));
    }

    #[test]
    fn a_stamped_record_carries_its_event_through_a_line() -> Result<()> {
        // What a caller appending outside the collector gets: the same clock
        // the collector reads, on a record whose line parses back whole.
        let event = Event::ProgramBound {
            format: "acme.thing.v1".to_string(),
            binary: "/opt/acme/worker".to_string(),
            digest: "ab".repeat(32),
        };
        let record = Record::stamped(event.clone());
        assert!(record.ts_ms > 0, "{record:?}");
        assert_eq!(Record::from_line(&record.to_line()?)?, record);
        assert_eq!(record.event, event);
        Ok(())
    }

    #[test]
    fn line_round_trips_through_serde() -> Result<()> {
        let record = fixed_stamp(Event::Committed {
            task: "11".repeat(32),
            record: "22".repeat(32),
            stats: vec![StatScalar {
                name: "population".to_string(),
                value: 0.25,
            }],
            stats_blob_hex: "00000000".to_string(),
        });
        let line = record.to_line()?;
        assert_eq!(Record::from_line(&line)?, record);
        Ok(())
    }

    /// One instance of every variant, for exhaustiveness over the vocabulary.
    fn every_variant() -> Vec<Event> {
        let task = "ab".repeat(32);
        let search = "cd".repeat(32);
        vec![
            Event::SearchStarted {
                search: search.clone(),
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
                stats: vec![StatScalar {
                    name: "population".to_string(),
                    value: 0.5,
                }],
                stats_blob_hex: "0011".to_string(),
            },
            Event::Failed {
                task: task.clone(),
                attempt: 0,
                reason: "flaky".to_string(),
                stats: Vec::new(),
                stats_blob_hex: String::new(),
            },
            Event::Retried {
                task: task.clone(),
                next_attempt: 1,
            },
            Event::Rejected {
                task: task.clone(),
                attempt: 1,
                reason: "rejected".to_string(),
                stats: Vec::new(),
                stats_blob_hex: String::new(),
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
                program: Some("cd".repeat(32)),
            },
            Event::ChainRebound {
                chain: 1,
                from: "10de:2c02".to_string(),
                to: "10de:2f04".to_string(),
            },
            Event::ProgramBound {
                format: "acme.thing.v1".to_string(),
                binary: "/opt/acme/worker".to_string(),
                digest: "ef".repeat(32),
            },
            Event::SearchFinalized {
                search: search.clone(),
                committed: 3,
            },
            Event::SearchFailed {
                search: search.clone(),
                task: task.clone(),
                reason: "rejected".to_string(),
            },
            Event::SearchInterrupted { search },
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
            let record = fixed_stamp(event);
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
