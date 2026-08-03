//! [`report`]: each committed task's per-candidate stats, rendered from a run's
//! journal.

use std::collections::BTreeMap;

use sima_core::{Error, Result};
use sima_scheduler::{Event, Record, StatScalar};

use crate::config::LoadedConfig;
use crate::journal;
use crate::stats::render_stats;
use crate::task_history::resolve_task_key;

/// One reported task: its journaled key and its rendered stats line.
#[derive(Debug, PartialEq, Eq)]
pub struct ReportRow {
    /// The committed task's key, as journaled — the lowercase-hex string.
    pub task: String,
    /// The task's stats rendered into one line.
    pub stats: String,
}

/// Renders each committed task's per-candidate stats for the run a loaded
/// config describes, from its journal alone — the read-only reporting
/// counterpart of [`orchestrate`](crate::orchestrate). Rows are sorted by task
/// key.
///
/// Each `Committed` event carries the executor's named scalars and the family
/// blob's hex, rendered generically; a task commits at most once, so each
/// contributes one row. A missing store, a run never started there, and an
/// unparseable line carry the errors every journal query reports.
pub fn report(config: &LoadedConfig) -> Result<Vec<ReportRow>> {
    report_records(&journal::records(config)?)
}

/// Renders each committed task's stats from `records` — a run's lifecycle
/// events in append order. The fold half of [`report`], over records from any
/// source.
pub fn report_records(records: &[Record]) -> Result<Vec<ReportRow>> {
    Ok(committed_stats(records)
        .into_iter()
        .map(|(task, (scalars, blob_hex))| row(task, &scalars, &blob_hex))
        .collect())
}

/// Renders one committed task's stats from `records`, addressed by a prefix
/// of its key. The fold half of [`report_task`], over records from any
/// source.
pub fn report_task_records(records: &[Record], prefix: &str) -> Result<ReportRow> {
    let task = resolve_task_key(records, prefix)?;
    let (scalars, blob_hex) = committed_stats(records)
        .remove(&task)
        .ok_or_else(|| Error::Validation(format!("task {task} has no committed result")))?;
    Ok(row(task, &scalars, &blob_hex))
}

/// The latest committed stats per task, ordered by task key. A task commits
/// once, but a resume segment re-journals prior commits, so the last
/// `Committed` line wins. Each entry is the event's scalars and its family
/// blob hex.
fn committed_stats(records: &[Record]) -> BTreeMap<String, (Vec<StatScalar>, String)> {
    let mut latest = BTreeMap::new();
    for record in records {
        if let Event::Committed {
            task,
            stats,
            stats_blob_hex,
            ..
        } = &record.event
        {
            latest.insert(task.clone(), (stats.clone(), stats_blob_hex.clone()));
        }
    }
    latest
}

/// One row for `task`, its scalars and family blob rendered generically.
fn row(task: String, scalars: &[StatScalar], blob_hex: &str) -> ReportRow {
    ReportRow {
        task,
        stats: render_stats(scalars, blob_hex),
    }
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

    /// A `StatScalar` from a name and value.
    fn scalar(name: &str, value: f64) -> StatScalar {
        StatScalar {
            name: name.to_string(),
            value,
        }
    }

    /// A `Committed` line for `task` carrying `scalars` and no blob.
    fn committed(task: &str, scalars: Vec<StatScalar>) -> Record {
        rec(Event::Committed {
            task: task.to_string(),
            record: "11".repeat(32),
            stats: scalars,
            stats_blob_hex: String::new(),
        })
    }

    #[test]
    fn rows_are_one_per_task_sorted_by_key() -> Result<()> {
        let (_dir, config) = journal_with(&[
            started(&stub_config()?.id(), 2),
            // Out of key order and with a duplicate: the map sorts and the last
            // commit of a task wins.
            committed("bb", vec![scalar("attempt", 1.0)]),
            committed("aa", vec![scalar("attempt", 0.0)]),
            committed("aa", vec![scalar("attempt", 2.0)]),
        ])?;
        let rows = report(&config)?;
        assert_eq!(
            rows,
            vec![
                ReportRow {
                    task: "aa".to_string(),
                    stats: "attempt=2".to_string(),
                },
                ReportRow {
                    task: "bb".to_string(),
                    stats: "attempt=1".to_string(),
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn scalars_render_space_joined() -> Result<()> {
        let (_dir, config) = journal_with(&[
            started(&stub_config()?.id(), 1),
            committed("aa", vec![scalar("attempt", 0.0), scalar("steps", 5.0)]),
        ])?;
        assert_eq!(report(&config)?[0].stats, "attempt=0 steps=5");
        Ok(())
    }

    #[test]
    fn a_non_empty_blob_reports_its_byte_length() -> Result<()> {
        let (_dir, config) = journal_with(&[
            started(&stub_config()?.id(), 1),
            rec(Event::Committed {
                task: "aa".to_string(),
                record: "11".repeat(32),
                stats: vec![scalar("attempt", 0.0)],
                stats_blob_hex: "aabbcc".to_string(),
            }),
        ])?;
        assert_eq!(report(&config)?[0].stats, "attempt=0 blob=3B");
        Ok(())
    }

    #[test]
    fn a_task_report_resolves_a_prefix_and_renders_that_task_alone() -> Result<()> {
        let (_dir, config) = journal_with(&[
            started(&stub_config()?.id(), 2),
            committed("abcd", vec![scalar("attempt", 0.0)]),
            committed("bcde", vec![scalar("attempt", 1.0)]),
        ])?;
        assert_eq!(
            report_task_records(&crate::journal::records(&config)?, "ab")?,
            ReportRow {
                task: "abcd".to_string(),
                stats: "attempt=0".to_string(),
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
                stats: Vec::new(),
                stats_blob_hex: String::new(),
            }),
        ])?;
        let reported = report_task_records(&crate::journal::records(&config)?, "ab");
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
        let records = vec![
            started(&stub_config()?.id(), 2),
            committed("abcd", vec![scalar("attempt", 0.0)]),
            committed("bcde", vec![scalar("attempt", 1.0)]),
        ];
        let (_dir, config) = journal_with(&records)?;
        assert_eq!(report_records(&records)?, report(&config)?);
        assert_eq!(
            report_task_records(&records, "ab")?,
            report_task_records(&crate::journal::records(&config)?, "ab")?
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
