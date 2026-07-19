//! [`report`]: each committed task's per-candidate stats, rendered from a run's
//! journal.

use std::collections::BTreeMap;

use sima_core::{Error, Result, from_hex};
use sima_domains::domain_for;
use sima_scheduler::{Event, Record};
use sima_store::Store;

use crate::config::LoadedConfig;

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
/// event carries; a task commits at most once, so each contributes one row. A
/// store root that does not exist, or a run never started there, is
/// [`Error::Validation`], matching [`status`](crate::status). A journal line
/// that fails to parse is [`Error::Corruption`]; stats bytes the domain does
/// not recognize are [`Error::Validation`] from the renderer.
pub fn report(config: &LoadedConfig) -> Result<Vec<ReportRow>> {
    if !config.store.is_dir() {
        return Err(Error::Validation(format!(
            "store {} does not exist: no run was ever driven there",
            config.store.display()
        )));
    }
    let domain = domain_for(&config.run.format)?;
    let store = Store::open(&config.store)?;
    let run = config.run.id();
    let lines = store.journal(&run)?;
    if lines.is_empty() {
        return Err(Error::Validation(format!(
            "run {run} was never started in this store"
        )));
    }
    // The latest committed stats per task. A task commits once, but a resume
    // segment re-journals prior commits, so the last `Committed` line wins. The
    // `BTreeMap` orders the rows by task key.
    let mut latest: BTreeMap<String, String> = BTreeMap::new();
    for line in &lines {
        let record = Record::from_line(line)
            .map_err(|e| Error::Corruption(format!("journal of run {run}: {e}")))?;
        if let Event::Committed {
            task, stats_hex, ..
        } = record.event
        {
            latest.insert(task, stats_hex);
        }
    }
    latest
        .into_iter()
        .map(|(task, stats_hex)| {
            let bytes = from_hex(&stats_hex)?;
            let stats = (domain.stats)(&bytes)?;
            Ok(ReportRow { task, stats })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig};
    use sima_store::Store;

    /// A minimal stub run config; its id addresses the test's run.
    fn stub_config() -> Result<RunConfig> {
        Ok(RunConfig {
            root_seed: 1,
            segments: None,
            format: FormatId::new("stub.v1")?,
            generator: GeneratorConfig {
                id: GeneratorId::new("stub.v1")?,
                params: Vec::new(),
            },
            params: Params { bytes: Vec::new() },
        })
    }

    /// A loaded config over `store` for the stub run.
    fn loaded(store: std::path::PathBuf) -> Result<LoadedConfig> {
        Ok(LoadedConfig {
            run: stub_config()?,
            devices: Vec::new(),
            remotes: Vec::new(),
            execution: sima_scheduler::ExecutionConfig::new(
                1,
                1,
                std::time::Duration::MAX,
                std::time::Duration::MAX,
                None,
            )?,
            store,
        })
    }

    /// Wraps an event as the unstamped record the tests journal.
    fn rec(event: Event) -> Record {
        Record { ts_ms: None, event }
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

    /// Writes `records` to the run's journal in a fresh store, returning the
    /// temp dir (kept alive by the caller) and the loaded config over it.
    fn journal_with(records: &[Record]) -> Result<(tempfile::TempDir, LoadedConfig)> {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let config = stub_config()?;
        store.create_run(&config)?;
        let run = config.id();
        let mut writer = store.journal_writer(&run)?;
        for record in records {
            writer.append(&record.to_line()?)?;
        }
        let loaded = loaded(dir.path().to_path_buf())?;
        Ok((dir, loaded))
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
