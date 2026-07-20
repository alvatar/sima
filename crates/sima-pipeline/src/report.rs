//! [`report`]: each committed task's per-candidate stats, rendered from a run's
//! journal.

use std::collections::BTreeMap;

use sima_core::{Error, Result, from_hex};
use sima_domains::{Domain, domain_for};
use sima_model::FormatId;
use sima_scheduler::{Event, Record};

use crate::config::LoadedConfig;
use crate::journal;
use crate::task_history::resolve_task_key;

/// One reported task: its journaled key and the domain-rendered stats line.
#[derive(Debug, PartialEq, Eq)]
pub struct ReportRow {
    /// The committed task's key, as journaled — the lowercase-hex string.
    pub task: String,
    /// The task's stats rendered into one line by its domain.
    pub stats: String,
}

/// Renders each committed task's per-candidate stats for the run a loaded
/// config describes, from its journal alone — the read-only reporting
/// counterpart of [`orchestrate`](crate::orchestrate). Rows are sorted by task
/// key.
///
/// The format's domain renders the observational stats bytes each `Committed`
/// event carries; a task commits at most once, so each contributes one row.
/// Stats bytes the domain does not recognize are [`Error::Validation`] from
/// the renderer; a missing store, a run never started there, and an
/// unparseable line carry the errors every journal query reports.
pub fn report(config: &LoadedConfig) -> Result<Vec<ReportRow>> {
    report_records(&config.run.format, &journal::records(config)?)
}

/// Renders each committed task's stats from `records` — a run's lifecycle
/// events in append order — through `format`'s domain. The fold half of
/// [`report`], over records from any source.
pub fn report_records(format: &FormatId, records: &[Record]) -> Result<Vec<ReportRow>> {
    let domain = domain_for(format)?;
    committed_stats(records)
        .into_iter()
        .map(|(task, stats_hex)| row(task, &stats_hex, &domain))
        .collect()
}

/// Renders one committed task's per-candidate stats, addressed by a prefix of
/// its key. A task the run journaled but never committed is
/// [`Error::Validation`]: this is the view of what the run produced, and a
/// task's execution history is what [`task_history`](crate::task_history)
/// answers.
pub fn report_task(config: &LoadedConfig, prefix: &str) -> Result<ReportRow> {
    report_task_records(&config.run.format, &journal::records(config)?, prefix)
}

/// Renders one committed task's stats from `records`, addressed by a prefix
/// of its key. The fold half of [`report_task`], over records from any
/// source.
pub fn report_task_records(
    format: &FormatId,
    records: &[Record],
    prefix: &str,
) -> Result<ReportRow> {
    let domain = domain_for(format)?;
    let task = resolve_task_key(records, prefix)?;
    let stats_hex = committed_stats(records)
        .remove(&task)
        .ok_or_else(|| Error::Validation(format!("task {task} has no committed result")))?;
    row(task, &stats_hex, &domain)
}

/// The latest committed stats per task, ordered by task key. A task commits
/// once, but a resume segment re-journals prior commits, so the last
/// `Committed` line wins.
fn committed_stats(records: &[Record]) -> BTreeMap<String, String> {
    let mut latest = BTreeMap::new();
    for record in records {
        if let Event::Committed {
            task, stats_hex, ..
        } = &record.event
        {
            latest.insert(task.clone(), stats_hex.clone());
        }
    }
    latest
}

/// One row for `task`, its stats bytes rendered through the format's domain.
fn row(task: String, stats_hex: &str, domain: &Domain) -> Result<ReportRow> {
    let stats = (domain.stats)(&from_hex(stats_hex)?)?;
    Ok(ReportRow { task, stats })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_store::Store;

    use crate::fixtures::{journal_with, loaded, stub_config};

    /// Wraps an event as a record the tests journal. The stamp is irrelevant
    /// here, so every record carries the same one.
    fn rec(event: Event) -> Record {
        Record { ts_ms: 0, event }
    }

    /// A `RunStarted` line for the run.
    fn started(run: &sima_model::RunId, tasks: usize) -> Record {
        rec(Event::RunStarted {
            run: run.to_string(),
            tasks,
            committed: 0,
        })
    }

    /// A `Committed` line for `task` carrying `stats_hex`.
    fn committed(task: &str, stats_hex: &str) -> Record {
        rec(Event::Committed {
            task: task.to_string(),
            record: "11".repeat(32),
            stats_hex: stats_hex.to_string(),
        })
    }

    #[test]
    fn rows_are_one_per_task_sorted_by_key() -> Result<()> {
        let (_dir, config) = journal_with(&[
            started(&stub_config()?.id(), 2),
            // Out of key order and with a duplicate: the map sorts and the last
            // commit of a task wins.
            committed("bb", "01000000"),
            committed("aa", "00000000"),
            committed("aa", "02000000"),
        ])?;
        let rows = report(&config)?;
        assert_eq!(
            rows,
            vec![
                ReportRow {
                    task: "aa".to_string(),
                    stats: "attempt 2".to_string(),
                },
                ReportRow {
                    task: "bb".to_string(),
                    stats: "attempt 1".to_string(),
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn an_accumulate_payload_renders_attempt_and_steps() -> Result<()> {
        // attempt 0 (`00000000`) then steps 5 (`0500000000000000`).
        let (_dir, config) = journal_with(&[
            started(&stub_config()?.id(), 1),
            committed("aa", "000000000500000000000000"),
        ])?;
        assert_eq!(report(&config)?[0].stats, "attempt 0 steps 5");
        Ok(())
    }

    #[test]
    fn malformed_stats_bytes_are_a_validation_error() -> Result<()> {
        // Five bytes: a u32 attempt then one dangling byte, too short for the
        // steps u64. The renderer rejects it.
        let (_dir, config) = journal_with(&[
            started(&stub_config()?.id(), 1),
            committed("aa", "0000000099"),
        ])?;
        assert!(matches!(report(&config), Err(Error::Validation(_))));
        Ok(())
    }

    #[test]
    fn a_task_report_resolves_a_prefix_and_renders_that_task_alone() -> Result<()> {
        let (_dir, config) = journal_with(&[
            started(&stub_config()?.id(), 2),
            committed("abcd", "00000000"),
            committed("bcde", "01000000"),
        ])?;
        assert_eq!(
            report_task(&config, "ab")?,
            ReportRow {
                task: "abcd".to_string(),
                stats: "attempt 0".to_string(),
            }
        );
        Ok(())
    }

    #[test]
    fn a_task_that_never_committed_has_no_report() -> Result<()> {
        // The task has a history — it was leased and rejected — so the prefix
        // resolves; it produced no result to report.
        let (_dir, config) = journal_with(&[
            started(&stub_config()?.id(), 1),
            rec(Event::Leased {
                task: "abcd".to_string(),
                worker: 0,
                attempt: 0,
            }),
            rec(Event::Rejected {
                task: "abcd".to_string(),
                attempt: 0,
                reason: "programmed rejection".to_string(),
                stats_hex: String::new(),
            }),
        ])?;
        let reported = report_task(&config, "ab");
        assert!(
            matches!(reported, Err(Error::Validation(_))),
            "{reported:?}"
        );
        // The fold runs over records from any source, local or streamed from
        // another host, so its message states the fact and suggests no
        // command: a command naming a config path resolves on the machine that
        // reads it, which is where the records came from only half the time.
        let message = format!("{}", reported.expect_err("no committed result"));
        assert!(message.contains("has no committed result"), "{message}");
        assert!(!message.contains("sima "), "{message}");
        Ok(())
    }

    #[test]
    fn the_record_folds_equal_the_reports_read_from_the_journal() -> Result<()> {
        let format = sima_model::FormatId::new("stub.v1")?;
        let records = vec![
            started(&stub_config()?.id(), 2),
            committed("abcd", "00000000"),
            committed("bcde", "01000000"),
        ];
        let (_dir, config) = journal_with(&records)?;
        assert_eq!(report_records(&format, &records)?, report(&config)?);
        assert_eq!(
            report_task_records(&format, &records, "ab")?,
            report_task(&config, "ab")?
        );
        Ok(())
    }

    #[test]
    fn a_run_never_started_is_a_validation_error() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        Store::open(dir.path())?;
        let config = loaded(dir.path().to_path_buf())?;
        assert!(matches!(report(&config), Err(Error::Validation(_))));
        Ok(())
    }

    #[test]
    fn a_missing_store_is_a_validation_error() -> Result<()> {
        let config = loaded(std::path::PathBuf::from("/no/such/store/here"))?;
        assert!(matches!(report(&config), Err(Error::Validation(_))));
        Ok(())
    }
}
