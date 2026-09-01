//! [`LocalFeed`]: a run followed on the machine that drives it.

use sima_core::Result;
use sima_scheduler::Record;

use crate::config::LoadedConfig;
use crate::feed::{FeedInfo, RunFeed};
use crate::journal;
use crate::run_observer::RunObserver;

/// Follows a run through the store on this machine: a [`RunObserver`] paired
/// with the metadata the loaded config carries.
pub struct LocalFeed {
    info: FeedInfo,
    observer: RunObserver,
}

impl LocalFeed {
    /// Opens a feed over the run the loaded config describes. A store root
    /// that does not exist and a run never started there are both
    /// [`Error::Validation`](sima_core::Error::Validation), as they are for
    /// every read-only query — a run with no journal has nothing to follow,
    /// and the remote feed reports the same.
    pub fn open(config: &LoadedConfig) -> Result<LocalFeed> {
        journal::followable(config)?;
        Ok(LocalFeed {
            info: info(config),
            observer: RunObserver::new(config)?,
        })
    }

    /// The feed for a run that has one, or `None` when there is nothing to
    /// follow yet: no store at that root, or a run never driven in it.
    ///
    /// Absence is the ordinary case on a first run; every other failure on the
    /// read path is still an error, rather than being read off a variant and
    /// bucketed with it.
    pub fn opened(config: &LoadedConfig) -> Result<Option<LocalFeed>> {
        if journal::journaled(config)?.is_none() {
            return Ok(None);
        }
        LocalFeed::open(config).map(Some)
    }
}

/// Reads the whole journal of the run a loaded config describes, with the
/// metadata a view renders through — the local counterpart of
/// [`remote_snapshot`](crate::remote_snapshot), for the one-shot views that
/// render once and exit.
pub fn local_snapshot(config: &LoadedConfig) -> Result<(FeedInfo, Vec<Record>)> {
    Ok((info(config), journal::records(config)?))
}

/// The metadata a loaded config carries about its run.
fn info(config: &LoadedConfig) -> FeedInfo {
    FeedInfo {
        run: config.run.id(),
        format: config.run.format.clone(),
        workers: config.execution.workers,
    }
}

impl RunFeed for LocalFeed {
    fn info(&self) -> &FeedInfo {
        &self.info
    }

    fn poll(&mut self) -> Result<Vec<Record>> {
        self.observer.poll()
    }

    fn holder(&self) -> Result<Option<String>> {
        self.observer.holder()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::Result;
    use sima_scheduler::{Event, Record};
    use sima_store::Store;

    use crate::fixtures::{loaded, stub_config};

    /// A `Committed` record for `task`.
    fn committed(task: &str) -> Record {
        Record {
            ts_ms: 0,
            event: Event::Committed {
                task: task.to_string(),
                record: "11".repeat(32),
                stats: Vec::new(),
                stats_blob_hex: String::new(),
            },
        }
    }

    #[test]
    fn a_local_feed_yields_the_journal_and_the_config_metadata() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let run = stub_config()?;
        store.create_search(&run)?;
        let mut writer = store.journal_writer(&run.id())?;
        writer.append(&committed("aa").to_line()?)?;
        let config = loaded(dir.path().to_path_buf())?;

        let mut feed = LocalFeed::open(&config)?;
        assert_eq!(feed.info().run, run.id());
        assert_eq!(feed.info().format, run.format);
        assert_eq!(feed.info().workers, config.execution.workers);
        // The feed follows exactly as the observer it wraps: history first,
        // then only what was appended since.
        assert_eq!(feed.poll()?, [committed("aa")]);
        assert_eq!(feed.poll()?, Vec::<Record>::new());
        assert_eq!(feed.holder()?, None);
        let lock = store.acquire_search_lock(&run.id())?;
        assert!(feed.holder()?.is_some(), "the taken lock has a holder");
        drop(lock);
        Ok(())
    }
}
