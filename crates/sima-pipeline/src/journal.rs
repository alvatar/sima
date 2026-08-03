//! Reading a run's journal, under the guards every read-only query applies.

use sima_core::{Error, Result};
use sima_model::RunId;
use sima_scheduler::Record;
use sima_store::Store;

use crate::config::LoadedConfig;

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
