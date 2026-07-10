//! The resume-checkpoint side of the executor contract.
//!
//! A checkpoint is the disposable crash-resume mechanism: during a long
//! evaluation the executor periodically offers its full continuation state,
//! and on a restarted attempt it may pick up saved bytes instead of starting
//! from the segment's beginning. The contract splits the responsibilities:
//!
//! - The **executor** decides *what* bytes capture its continuation state and
//!   *when* it is safe to offer them (a step boundary).
//! - The **handle** decides *whether* an offer is written (cadence, storage)
//!   and performs all I/O — the executor never touches the store.
//!
//! Checkpoint bytes never enter a task key, a record, or a manifest. Using or
//! ignoring [`Checkpoint::resume`] must yield byte-identical committed
//! artifacts; a checkpoint changes recovery time only.

/// The executor-facing checkpoint channel of one attempt.
pub trait Checkpoint {
    /// Bytes saved by a previous attempt of this task, if any survive. The
    /// executor validates them itself and falls back to a fresh start when
    /// they do not apply; a resumed and a fresh evaluation must commit
    /// byte-identical artifacts.
    fn resume(&self) -> Option<&[u8]>;

    /// Offers continuation state at a point where resuming from it is safe.
    /// The executor calls this from inside
    /// [`execute`](crate::Executor::execute), at its own safe step
    /// boundaries; the handle may decline. It calls `produce` only when it
    /// decides to perform a save, so serialization costs nothing when no
    /// save is due.
    fn offer(&self, produce: &dyn Fn() -> Vec<u8>);
}

/// The inert handle for stateless tasks and tests: nothing to resume, offers
/// ignored.
pub struct NoCheckpoint;

impl Checkpoint for NoCheckpoint {
    fn resume(&self) -> Option<&[u8]> {
        None
    }

    fn offer(&self, _produce: &dyn Fn() -> Vec<u8>) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_checkpoint_is_inert() {
        let handle = NoCheckpoint;
        assert_eq!(handle.resume(), None);
        // The no-op handle must never invoke the producer.
        handle.offer(&|| panic!("NoCheckpoint must not call produce"));
    }

    #[test]
    fn checkpoint_is_dyn_compatible() {
        fn _object_safe(_: &dyn Checkpoint) {}
    }
}
