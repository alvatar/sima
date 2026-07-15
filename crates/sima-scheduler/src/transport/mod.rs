//! The execution transport: how the orchestrator converses with the
//! subprocess workers that host domain executors.
//!
//! The transport is operational machinery, never identity-bearing: nothing
//! that crosses it is hashed, and a run's manifests are byte-identical
//! whatever transport carried its attempts.
//!
//! - [`protocol`] — the wire protocol: frame IO and the message vocabulary
//!   both endpoints share.
//! - [`host`] — the child side: [`host::serve`] hosts a resolved executor
//!   over the pipe for the life of the worker process.
//! - [`link`] — the parent-side seam: the [`link::WorkerTransport`] and
//!   [`link::WorkerLink`] traits the scheduler is written against.
//! - [`subprocess`] — the production transport: one process per worker,
//!   SIGKILL preemption.

pub mod host;
pub mod link;
pub mod protocol;
pub mod subprocess;

pub(crate) mod checkpoint_cadence;
