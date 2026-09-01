//! [`SearchObserver`]: follow a search's journal and lock while another process
//! drives it.

use sima_core::{Error, Result};
use sima_model::SearchId;
use sima_scheduler::Record;
use sima_store::Store;

use crate::config::LoadedConfig;

/// Follows the search a loaded config describes while a separate orchestrator
/// drives it: [`poll`](SearchObserver::poll) returns the lifecycle events
/// appended since the previous call — the first call returns the search's full
/// history — and [`holder`](SearchObserver::holder) reports who holds the search's
/// orchestrator lock.
///
/// Observation is read-only: the observer never takes the search lock and never
/// writes the store. Polling is the contract — the caller decides the
/// cadence, and the observer runs no thread of its own.
pub struct SearchObserver {
    store: Store,
    search: SearchId,
    /// Journal bytes consumed so far; [`Store::journal_from`] resumes here.
    offset: u64,
}

impl SearchObserver {
    /// Opens an observer over the search the loaded config describes. A store
    /// root that does not exist is [`Error::Validation`], matching
    /// [`status`](crate::status): a query must not create the store skeleton.
    pub fn new(config: &LoadedConfig) -> Result<SearchObserver> {
        if !config.store.is_dir() {
            return Err(Error::Validation(format!(
                "store {} does not exist: no search was ever driven there",
                config.store.display()
            )));
        }
        Ok(SearchObserver {
            store: Store::open(&config.store)?,
            search: config.search.id(),
            offset: 0,
        })
    }

    /// The journal records appended since the previous poll, in append
    /// order; the first poll returns the search's full history. A line that
    /// fails to parse is [`Error::Corruption`] naming the search, matching
    /// [`report`](crate::report); the failed region stays unconsumed, so the
    /// next poll reports it again.
    pub fn poll(&mut self) -> Result<Vec<Record>> {
        let (lines, offset) = self.store.journal_from(&self.search, self.offset)?;
        let records = crate::journal::parse(&self.search, &lines)?;
        // Consume the region only once every line in it parsed.
        self.offset = offset;
        Ok(records)
    }

    /// The raw journal lines appended since the previous poll, in append
    /// order; the first poll returns the search's full history. The unparsed
    /// counterpart of [`poll`](SearchObserver::poll), for the follow stream,
    /// which forwards lines verbatim so the far side stays the only place
    /// that parses them. Nothing can fail past the read, so the region is
    /// consumed unconditionally.
    pub fn poll_lines(&mut self) -> Result<Vec<String>> {
        let (lines, offset) = self.store.journal_from(&self.search, self.offset)?;
        self.offset = offset;
        Ok(lines)
    }

    /// Who holds the search's orchestrator lock: `Some` with the recorded
    /// holder line (pid, hostname) while another process drives the search,
    /// `None` while the lock is free.
    pub fn holder(&self) -> Result<Option<String>> {
        self.store.lock_holder(&self.search)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{loaded, stub_config};

    /// A fresh store with the stub search created, and the loaded config over
    /// it. The temp dir keeps the store alive for the caller.
    fn created_store() -> Result<(tempfile::TempDir, Store, SearchId, LoadedConfig)> {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let config = stub_config()?;
        let search = config.id();
        store.create_search(&config)?;
        let loaded = loaded(dir.path().to_path_buf())?;
        Ok((dir, store, search, loaded))
    }

    /// Wraps an event as a record the tests journal. The stamp is irrelevant
    /// here, so every record carries the same one.
    fn rec(event: sima_scheduler::Event) -> Record {
        Record { ts_ms: 0, event }
    }

    /// A `SearchStarted` record for `search`.
    fn started(search: &SearchId, tasks: usize) -> Record {
        rec(sima_scheduler::Event::SearchStarted {
            search: search.to_string(),
            tasks,
            committed: 0,
        })
    }

    /// A `Committed` record for `task`.
    fn committed(task: &str) -> Record {
        rec(sima_scheduler::Event::Committed {
            task: task.to_string(),
            record: "11".repeat(32),
            stats: Vec::new(),
            stats_blob_hex: String::new(),
        })
    }

    /// Appends `records` to the search's journal, as the driving orchestrator
    /// would.
    fn append(store: &Store, search: &SearchId, records: &[Record]) -> Result<()> {
        let mut writer = store.journal_writer(search)?;
        for record in records {
            writer.append(&record.to_line()?)?;
        }
        Ok(())
    }

    #[test]
    fn the_first_poll_replays_history_and_later_polls_return_increments() -> Result<()> {
        let (_dir, store, search, loaded) = created_store()?;
        append(&store, &search, &[started(&search, 2), committed("aa")])?;

        let mut observer = SearchObserver::new(&loaded)?;
        // The first poll is the seed: the full history, in append order.
        assert_eq!(observer.poll()?, [started(&search, 2), committed("aa")]);
        // Later polls deliver only what the orchestrator appended since.
        append(&store, &search, &[committed("bb")])?;
        assert_eq!(observer.poll()?, [committed("bb")]);
        // Nothing appended: the poll is empty.
        assert_eq!(observer.poll()?, Vec::<Record>::new());
        Ok(())
    }

    #[test]
    fn a_malformed_journal_line_is_corruption_naming_the_search() -> Result<()> {
        let (_dir, store, search, loaded) = created_store()?;
        let mut writer = store.journal_writer(&search)?;
        writer.append("not a lifecycle event")?;
        let mut observer = SearchObserver::new(&loaded)?;
        match observer.poll() {
            Err(Error::Corruption(message)) => {
                assert!(
                    message.contains(&search.to_string()),
                    "the error names the search: {message}"
                );
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn a_missing_store_is_validation_and_creates_nothing() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("no-store");
        let config = loaded(missing.clone())?;
        assert!(matches!(
            SearchObserver::new(&config),
            Err(Error::Validation(_))
        ));
        // The probe of a nonexistent store must not create its skeleton.
        assert!(!missing.exists(), "the observer created the store");
        Ok(())
    }

    #[test]
    fn holder_reflects_the_search_lock_across_acquire_and_release() -> Result<()> {
        let (_dir, store, search, loaded) = created_store()?;
        let observer = SearchObserver::new(&loaded)?;
        assert_eq!(observer.holder()?, None);
        let lock = store.acquire_search_lock(&search)?;
        let holder = observer.holder()?.expect("a holder while locked");
        let pid = std::process::id().to_string();
        assert_eq!(holder.split_whitespace().next(), Some(pid.as_str()));
        drop(lock);
        assert_eq!(observer.holder()?, None);
        Ok(())
    }
}
