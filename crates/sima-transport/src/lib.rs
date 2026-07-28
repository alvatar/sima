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
//! - [`container`] — a worker inside a container runtime, optionally across an
//!   ssh hop, over the same spawn and handshake machinery.
//! - [`ssh`] — a worker launched as the ssh command itself, with no container
//!   wrapper, whose destination the orchestrator can swap under the running
//!   pool as machines are replaced.
//! - [`loopback`] — the test transport: the real host loop and wire protocol
//!   over in-memory pipes, for tests that need workers without processes.
//!
//! The two remote transports differ by **how a worker is launched**, which is
//! what their names state: one nests the worker in a container the transport
//! runs, the other hands the worker to ssh as the command to execute.

pub mod container;
pub mod host;
pub mod link;
pub mod loopback;
pub mod protocol;
pub mod ssh;
pub mod subprocess;

mod checkpoint_cadence;

pub use container::ContainerTransport;
pub use link::{LinkEvent, SpawnOutcome, WorkerLink, WorkerTransport};
pub use ssh::{SpawnMode, SshDestination, SshTransport};
pub use subprocess::SubprocessTransport;
