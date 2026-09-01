//! `sima migrate` and `sima recall`: moving a search's orchestrator onto another
//! machine, and ending it there.
//!
//! Everything built below this module distributes **workers** — the store and
//! the orchestrator stay here, and task inputs and results cross the wire
//! inline. Migration moves the **orchestrator**: the search's durable state
//! travels to the destination, a `sima search` process drives it there, and the
//! results come back.
//!
//! - [`destination`] — which machine, read from `[orchestrator].migrate`.
//! - [`far_config`] — where the search lives there, and what it reads when it
//!   arrives.
//! - [`objects`] — which objects a push carries, of those its records
//!   reference.
//! - [`sync`] — the two halves of a store sync, joined by a spawned process.
//! - [`far_side`] — every operation either verb performs on the destination,
//!   behind one boundary.
//! - [`far_search`] — the search on the destination: how it is ended, pulled from,
//!   settled, and let go of, which is what both verbs share.
//! - [`session`] — `sima migrate`: the choreography that joins them.
//! - [`recall`] — `sima recall`: the inverse, which starts nothing.

pub(crate) mod destination;
pub(crate) mod far_config;
pub(crate) mod far_search;
pub(crate) mod far_side;
#[cfg(test)]
pub(crate) mod fixtures;
pub(crate) mod objects;
pub(crate) mod recall;
pub(crate) mod session;
pub(crate) mod sync;

pub use far_search::MigrateOutcome;
pub use recall::recall;
pub use session::migrate;
pub use sync::sync_serve;
