//! [`WorkerPool`]: one transport's worker slots within a search.

use sima_contracts::DeviceBinding;
use sima_transport::WorkerTransport;

/// One worker pool of a search: a transport, the host its workers search on, and the
/// device slots to spawn against it.
///
/// A search's pools are this machine's first, then one per other machine in
/// order; worker ids stay global and sequential across them. Placement does not
/// know hosts — a class is global (present on any pool means present in the
/// search) — so a chain bound to a class searches on whichever pool holds it.
pub struct WorkerPool<'a> {
    /// The transport this pool's workers spawn through.
    pub transport: &'a dyn WorkerTransport,
    /// Where this pool's workers search, journaled with each `WorkerBound`: empty
    /// for the local pool, the configured destination for a remote one.
    pub host: String,
    /// The device each of this pool's slots computes on; `None` leaves the
    /// choice to the backend's default selection.
    pub slots: Vec<Option<DeviceBinding>>,
}
