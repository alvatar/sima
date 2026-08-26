//! Reading a run's journal, under the guards every read-only query applies,
//! and the boundary a verb writes to it through.

use std::thread;

use sima_core::{Error, Result};
use sima_model::RunId;
use sima_scheduler::Record;
use sima_store::Store;
use sima_trace::{Collector, Emitter, Observer};

use crate::config::LoadedConfig;

/// Runs `body` under the run's collector, so everything a verb emits reaches
/// the local journal and the operator's view through one boundary.
///
/// It spans the whole of what a verb does rather than the part that drives
/// tasks: the phases of putting a run on machines are what the operator
/// watches while there is nothing yet driving, and they cross the same
/// boundary the run's own records cross later. For a migration it is also the
/// only way a far run's records land here at all, since journals do not sync.
pub(crate) fn under_collector<T>(
    store: &Store,
    run: &RunId,
    observer: Observer<'_>,
    body: impl FnOnce(&Emitter) -> Result<T>,
) -> Result<T> {
    let writer = store.journal_writer(run)?;
    thread::scope(|scope| -> Result<T> {
        let collector = Collector::spawn(scope, writer, observer);
        let events = collector.emitter();
        let out = body(&events);
        // The collector joins only once every emitter is dropped.
        drop(events);
        let journal = collector.shutdown();
        // A journal that could not be appended is a store fault worth
        // reporting, but only when the body itself did not already fail.
        out.and_then(|value| journal.map(|()| value))
    })
}

/// Reads the journal of the run a loaded config describes, parsed into
/// records. A store root that does not exist is [`Error::Validation`] before
/// anything touches the disk, since opening a store creates its skeleton and
/// a query must not; a run never started there is [`Error::Validation`]; a
/// line that fails to parse is [`Error::Corruption`].
pub(crate) fn records(config: &LoadedConfig) -> Result<Vec<Record>> {
    let (run, lines) = lines(config)?;
    parse(&run, &lines)
}

/// Parses journal lines into records, naming the run a malformed one belongs
/// to.
///
/// Every reader of a journal goes through here — the one-shot queries, the
/// live observer, and the feed that forwards a far side's lines — so a line
/// that does not parse reads as the same corruption whichever of them met it.
pub(crate) fn parse(run: &RunId, lines: &[String]) -> Result<Vec<Record>> {
    lines
        .iter()
        .map(|line| {
            Record::from_line(line)
                .map_err(|e| Error::Corruption(format!("journal of run {run}: {e}")))
        })
        .collect()
}

/// Fails unless the run a loaded config describes has a journal to follow,
/// applying the guards [`records`] applies. The live views read the journal
/// incrementally rather than at once, so they check at open what a one-shot
/// query learns from its single read; the journal is read once more here,
/// which happens once per session.
pub(crate) fn followable(config: &LoadedConfig) -> Result<()> {
    lines(config).map(|_| ())
}

/// The records of the run a loaded config describes, or `None` when there is
/// no such run to read: no store at that root, or a run never driven in it.
///
/// The distinction is what a caller that seeds a display from prior progress
/// needs. Reading it off the error variant instead would put every future
/// `Validation` on this path — a malformed store marker, a bad run id — into
/// the same bucket as "nothing here yet", and the caller would open on zeroed
/// counts rather than report the fault. Absence is answered here as absence;
/// everything else is still an error.
pub(crate) fn journaled(config: &LoadedConfig) -> Result<Option<Vec<Record>>> {
    if !config.store.is_dir() {
        return Ok(None);
    }
    let store = Store::open(&config.store)?;
    let run = config.run.id();
    let lines = store.journal(&run)?;
    if lines.is_empty() {
        return Ok(None);
    }
    parse(&run, &lines).map(Some)
}

/// The journal lines of the run a loaded config describes, with the run they
/// belong to, under the guards every read-only query applies.
fn lines(config: &LoadedConfig) -> Result<(RunId, Vec<String>)> {
    if !config.store.is_dir() {
        return Err(Error::Validation(format!(
            "store {} does not exist: no run was ever driven there",
            config.store.display()
        )));
    }
    let store = Store::open(&config.store)?;
    let run = config.run.id();
    let lines = store.journal(&run)?;
    if lines.is_empty() {
        return Err(Error::Validation(format!(
            "run {run} was never started in this store"
        )));
    }
    Ok((run, lines))
}

#[cfg(test)]
mod tests {
    use sima_store::Store;

    use super::*;
    use crate::fixtures::loaded;

    #[test]
    fn a_read_over_a_missing_store_is_refused_before_it_touches_the_disk() -> Result<()> {
        // Opening a store creates its skeleton, and a query must not, so the
        // absence is observed rather than probed. Every read-only query goes
        // through here, so the guard is stated once for all of them.
        let config = loaded(std::path::PathBuf::from("/no/such/store/here"))?;
        assert!(matches!(records(&config), Err(Error::Validation(_))));
        assert!(matches!(followable(&config), Err(Error::Validation(_))));
        assert!(journaled(&config)?.is_none());
        Ok(())
    }

    #[test]
    fn a_read_of_a_run_never_started_in_an_existing_store_is_refused() -> Result<()> {
        // The store is there because another run used it; this run has no
        // journal in it, which a query reports rather than answering with an
        // empty history.
        let dir = tempfile::tempdir().expect("temp dir");
        Store::open(dir.path())?;
        let config = loaded(dir.path().to_path_buf())?;
        assert!(matches!(records(&config), Err(Error::Validation(_))));
        assert!(matches!(followable(&config), Err(Error::Validation(_))));
        assert!(journaled(&config)?.is_none());
        Ok(())
    }
}
