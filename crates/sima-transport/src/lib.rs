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
//! - [`domain_service`] — the second conversation: what a format binds, asked
//!   of the program that owns it and answered from its two components.
//! - [`serve`] — the process entry point of a program that hosts a domain:
//!   both roles behind one call, chosen by the arguments it was spawned with.
//! - [`link`] — the parent-side boundary: the [`WorkerTransport`] and
//!   [`WorkerLink`] traits the scheduler is written against.
//! - [`device_probe`] — what a machine's enumeration probe is asked: one
//!   format's backend, or every backend the worker there compiles in.
//! - [`spawn_policy`] — what environment and working directory a spawned
//!   child receives: an inherited surface for a sima-owned process, an
//!   explicit one for a configured program.
//! - [`spawn_settings`] — what every worker spawn of one pool shares: that
//!   policy, the deadline on the handshake answer, and the handshake frame.
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
pub mod device_probe;
pub mod domain_service;
pub mod host;
pub mod link;
pub mod loopback;
pub mod protocol;
pub mod serve;
pub mod spawn_policy;
pub mod spawn_settings;
pub mod ssh;
pub mod subprocess;

mod answer_deadline;
mod checkpoint_cadence;

pub use container::ContainerTransport;
pub use device_probe::DeviceProbe;
pub use link::{LinkEvent, SpawnOutcome, WorkerLink, WorkerTransport};
pub use spawn_policy::SpawnPolicy;
pub use spawn_settings::SpawnSettings;
pub use ssh::{RemoteCommand, SpawnMode, SshDestination, SshTransport};
pub use subprocess::SubprocessTransport;
