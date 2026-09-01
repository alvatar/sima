//! The feed: a search's records and lock state, from the host that drives it.
//!
//! Every live view of a search — the tui and `follow` — consumes one
//! [`SearchFeed`]. [`LocalFeed`] follows a search on this machine; the remote
//! implementation follows one on the host its orchestrator runs on, over the
//! stream [`protocol`] defines. The view loop is the same either way.

mod local;
mod protocol;
mod remote;
mod serve;

pub use local::{LocalFeed, local_snapshot};
pub use protocol::{FOLLOW_PROTOCOL_VERSION, FollowFrame};
pub(crate) use remote::snapshot_over_argv;
pub use remote::{RemoteFeed, remote_snapshot};
pub use serve::follow_serve;

use sima_core::Result;
use sima_model::{FormatId, SearchId};
use sima_scheduler::Record;

/// The search metadata a view renders through and cannot derive from records
/// alone: it lives in the config, which is read on the host that drives the
/// search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedInfo {
    /// The search the feed follows.
    pub search: SearchId,
    /// The search's format id; the domain that renders stats resolves from it.
    pub format: FormatId,
    /// The configured worker count, for the occupancy view.
    pub workers: usize,
}

/// A live source of one search's observations: the records its journal gains and
/// the state of its orchestrator lock. Polling is the contract — the caller
/// decides the cadence — matching the observer a local feed wraps.
pub trait SearchFeed {
    /// The search metadata the view renders through, fixed for the feed's life.
    fn info(&self) -> &FeedInfo;

    /// The records appended since the previous poll, in append order; the
    /// first poll returns the search's history.
    fn poll(&mut self) -> Result<Vec<Record>>;

    /// Who holds the search's orchestrator lock, or `None` while it is free.
    ///
    /// How fresh the answer is follows from where the lock is. A local feed
    /// probes it directly, so the answer holds as of the call. A remote feed
    /// reports what the far side last observed, whose age is bounded by the
    /// far side's probe interval plus the stream's latency — a caller that
    /// acts on a free lock acts on an observation within that bound.
    fn holder(&self) -> Result<Option<String>>;
}
