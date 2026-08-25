//! The parent-side transport boundary: [`WorkerTransport`] spawns workers,
//! [`WorkerLink`] converses with one.
//!
//! A spawn resolves to one of three outcomes: a live [`WorkerLink`] to
//! converse with, a [`SpawnOutcome::Retired`] when an ssh transport's
//! instances are gone with no replacement, or an `Err` — an infrastructure
//! spawn failure the caller faults on. The retirement is a spawn-time channel
//! distinct from the conversation's [`LinkEvent`] outcomes below.
//!
//! The boundary exists so the scheduler's worker loop is written against traits
//! the tests can implement without processes: the production transport spawns
//! `sima-worker` subprocesses, the test loopback runs the same host loop and
//! wire protocol over in-memory pipes. Everything a child does reaches the
//! caller as a [`LinkEvent`] — including its death and the caller's deadline
//! expiring — so outcome classification stays entirely with the caller; an
//! `Err` from the link is a frame-protocol violation or a broken pipe, the
//! faults the caller answers by killing the child.

use std::time::Instant;

/// The command a worker runs as inside a container or at the far end of an ssh
/// hop: the binary's name on the `PATH` there. A local spawn names a path
/// instead, since the binary it runs is the one this build found.
pub(crate) const WORKER_ENTRYPOINT: &str = "sima-worker";

use sima_contracts::{DeviceBinding, Outcome};
use sima_core::Result;
use sima_trace::Emitter;

use crate::protocol::Assignment;

/// Spawns workers. One transport serves a whole run; each worker slot holds
/// one [`WorkerLink`] at a time and replaces it when the child dies.
pub trait WorkerTransport: Sync {
    /// Spawns one worker as slot `worker`, bound to `device` — or, for
    /// `None`, to the execution backend's default selection — and performs
    /// the handshake; the worker id travels in the `Hello` so the child can
    /// attribute events. `events` is the run's emitter: the spawn's reader
    /// threads emit the child's structured events and captured stderr
    /// through it, and drop their clones when the child dies, so the
    /// collector's channel closes when the run's last worker does.
    ///
    /// A successful spawn yields [`SpawnOutcome::Link`]; an ssh transport
    /// whose instances are gone yields [`SpawnOutcome::Retired`] instead of a
    /// link. An `Err` is a spawn failure — an infrastructure error, never a
    /// task outcome.
    fn spawn(
        &self,
        worker: u64,
        device: Option<&DeviceBinding>,
        events: Emitter,
    ) -> Result<SpawnOutcome>;
}

/// What spawning a worker slot produced.
pub enum SpawnOutcome {
    /// A live worker to converse with.
    Link(Box<dyn WorkerLink>),
    /// The slot's transport retired: no worker was spawned, and none will be.
    /// `fatal` marks a retirement the run must fault on — a strict-fill rental
    /// that lost the instances it depends on; a non-fatal retirement lets the
    /// worker thread exit cleanly, the best-effort degradation of a rental that
    /// runs on whatever instances remain.
    Retired {
        /// Whether the retirement must fault the run.
        fatal: bool,
    },
}

impl SpawnOutcome {
    /// The link, for call sites where a link is guaranteed: tests, and the
    /// transports that never retire.
    ///
    /// # Panics
    ///
    /// Panics on a [`SpawnOutcome::Retired`] — the caller has asserted the
    /// transport cannot retire, so a retirement here is a bug, not a runtime
    /// fault.
    pub fn into_link(self) -> Box<dyn WorkerLink> {
        match self {
            SpawnOutcome::Link(link) => link,
            SpawnOutcome::Retired { .. } => {
                panic!("spawn retired where a link was expected")
            }
        }
    }
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

    /// The digest of the program the worker reported running at the handshake;
    /// empty when no program travelled to its machine. Agreed with what the run
    /// sent before the spawn returned, so what this carries is a fact about the
    /// machine the journal can record.
    fn program(&self) -> &str;

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
