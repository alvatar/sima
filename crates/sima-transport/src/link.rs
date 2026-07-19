//! The parent-side transport seam: [`WorkerTransport`] spawns workers,
//! [`WorkerLink`] converses with one.
//!
//! The seam exists so the scheduler's worker loop is written against traits
//! the tests can implement without processes: the production transport spawns
//! `sima-worker` subprocesses, the test loopback runs the same host loop and
//! wire protocol over in-memory pipes. Everything a child does reaches the
//! caller as a [`LinkEvent`] — including its death and the caller's deadline
//! expiring — so outcome classification stays entirely with the caller; an
//! `Err` from the link is a frame-protocol violation or a broken pipe, the
//! faults the caller answers by killing the child.

use std::time::Instant;

use sima_contracts::{DeviceBinding, Outcome};
use sima_core::Result;

use crate::protocol::Assignment;

/// Spawns workers. One transport serves a whole run; each worker slot holds
/// one [`WorkerLink`] at a time and replaces it when the child dies.
pub trait WorkerTransport: Sync {
    /// Spawns one worker as slot `worker`, bound to `device` — or, for
    /// `None`, to the execution backend's default selection — and performs
    /// the handshake; the worker id travels in the `Hello` so the child can
    /// attribute events. An `Err` is a spawn failure — an infrastructure
    /// error, never a task outcome.
    fn spawn(&self, worker: u64, device: Option<&DeviceBinding>) -> Result<Box<dyn WorkerLink>>;
}

/// The parent's conversation with one live worker.
pub trait WorkerLink: Send {
    /// The device the worker reported at the handshake; empty for a domain
    /// that uses no device. Provenance for the journal: what the child
    /// resolved, never what the parent assumed.
    fn device_name(&self) -> &str;

    /// The driver version the worker reported at the handshake; empty for a
    /// domain that uses no device. Journaled beside the device name so a
    /// cross-machine divergence within one class is diagnosable.
    fn driver(&self) -> &str;

    /// Hands the worker one task. An `Err` is a broken pipe — the child is
    /// dead or dying; the caller classifies and replaces it.
    fn assign(&mut self, assignment: &Assignment) -> Result<()>;

    /// Waits for the worker's next event, up to `deadline` when given.
    /// Death and deadline expiry arrive as events, not errors, so the caller
    /// owns their classification; an `Err` is a frame violation, answered by
    /// killing the child.
    fn next(&mut self, deadline: Option<Instant>) -> Result<LinkEvent>;

    /// Kills the worker immediately and reaps it. Best effort: a child
    /// already dead is fine.
    fn kill(&mut self);
}

/// What a wait on a worker link yielded.
#[derive(Debug)]
pub enum LinkEvent {
    /// A due checkpoint save to persist.
    Save(Vec<u8>),
    /// The attempt's outcome, verbatim from the executor.
    Done(Outcome),
    /// The executor panicked; the rendered reason.
    Panicked(String),
    /// The executor returned `Err` — an infrastructure fault; the message.
    Fault(String),
    /// The child died without an outcome: its event stream ended. Carries a
    /// description of the death — the exit status or signal where the
    /// transport can observe it.
    Died(String),
    /// The caller's deadline expired with no event; nothing was consumed.
    DeadlineExpired,
}
