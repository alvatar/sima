//! The execution transport: how the orchestrator converses with the
//! subprocess workers that host domain executors.
//!
//! The transport is operational machinery, never identity-bearing: nothing
//! that crosses it is hashed, and a run's manifests are byte-identical
//! whatever transport carried its attempts.
//!
//! - [`protocol`] — the wire protocol: the message vocabulary both endpoints
//!   share, framed with [`sima_core::frame`].
//! - [`host`] — the child side: [`host::serve`] hosts a resolved executor
//!   over the pipe for the life of the worker process.
//! - [`link`] — the parent-side seam: the [`WorkerTransport`] and
//!   [`WorkerLink`] traits the scheduler is written against.
//! - [`subprocess`] — the production transport: one process per worker,
//!   SIGKILL preemption.
//! - [`remote`] — a worker inside a container runtime, optionally across an
//!   ssh hop, over the same spawn and handshake machinery.
//! - [`loopback`] — the test transport: the real host loop and wire protocol
//!   over in-memory pipes, for tests that need workers without processes.

pub mod host;
pub mod link;
pub mod loopback;
pub mod protocol;
pub mod remote;
pub mod subprocess;

mod checkpoint_cadence;

pub use link::{LinkEvent, SpawnOutcome, WorkerLink, WorkerTransport};
pub use remote::RemoteTransport;
pub use subprocess::SubprocessTransport;
