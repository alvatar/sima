//! [`CheckpointCadence`]: when a checkpoint offer becomes a save.

use std::cell::Cell;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

/// The two-axis save cadence of one attempt: a wall-clock interval and a
/// step-count interval, unioned — a save is due when either axis fires, and
/// both axes reset on save. [`Duration::MAX`] disables the wall-clock axis;
/// `None` disables the step axis. The cadence decides *whether* an offer is
/// written; the storage side of the checkpoint contract lives with whoever
/// owns the slot I/O.
pub(crate) struct CheckpointCadence {
    interval: Duration,
    /// Step-count cadence: a save is due every `n`th offer since the last
    /// save. `None` leaves the wall-clock `interval` as the only cadence.
    step_interval: Option<NonZeroU64>,
    /// When the last save happened — initialized to the attempt's start, so
    /// the first save becomes due one full interval in.
    last_saved: Cell<Instant>,
    /// Offers seen since the last save, driving the step-count cadence.
    offers_since_save: Cell<u64>,
}

impl CheckpointCadence {
    /// A cadence at the start of an attempt: no offers seen, the wall-clock
    /// axis anchored to now.
    pub(crate) fn new(interval: Duration, step_interval: Option<NonZeroU64>) -> CheckpointCadence {
        CheckpointCadence {
            interval,
            step_interval,
            last_saved: Cell::new(Instant::now()),
            offers_since_save: Cell::new(0),
        }
    }

    /// Advances the offer counter and reports whether this offer triggers a
    /// save, under either cadence axis.
    ///
    /// The name states the mutation: the step-count axis advances its counter
    /// here, so every offer is counted exactly once and a caller asking twice
    /// about one offer would count it twice. The wall-clock axis reads the
    /// elapsed time since the last save. A save is due when either axis fires.
    pub(crate) fn advance_due(&self) -> bool {
        let step_due = match self.step_interval {
            Some(n) => {
                let count = self.offers_since_save.get() + 1;
                self.offers_since_save.set(count);
                count >= n.get()
            }
            None => false,
        };
        let clock_due =
            self.interval != Duration::MAX && self.last_saved.get().elapsed() >= self.interval;
        step_due || clock_due
    }

    /// Resets both axes: the offer counter to zero, the wall-clock anchor to
    /// now. The caller resets before attempting its save, so a persistently
    /// failing save degrades once per cadence period instead of once per
    /// offer.
    pub(crate) fn reset(&self) {
        self.offers_since_save.set(0);
        self.last_saved.set(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_axes_disabled_is_never_due() {
        let cadence = CheckpointCadence::new(Duration::MAX, None);
        for _ in 0..100 {
            assert!(!cadence.advance_due());
        }
    }

    #[test]
    fn a_zero_interval_is_due_at_every_offer() {
        let cadence = CheckpointCadence::new(Duration::ZERO, None);
        assert!(cadence.advance_due());
        cadence.reset();
        assert!(cadence.advance_due());
    }

    #[test]
    fn the_step_axis_fires_every_nth_offer() {
        let cadence = CheckpointCadence::new(Duration::MAX, NonZeroU64::new(3));
        // Offers 1 and 2 are below the cadence; offer 3 fires.
        assert!(!cadence.advance_due());
        assert!(!cadence.advance_due());
        assert!(cadence.advance_due());
    }

    #[test]
    fn reset_restarts_the_step_counter() {
        let cadence = CheckpointCadence::new(Duration::MAX, NonZeroU64::new(2));
        assert!(!cadence.advance_due());
        assert!(cadence.advance_due());
        cadence.reset();
        // The counter restarts: one offer below cadence, the second fires.
        assert!(!cadence.advance_due());
        assert!(cadence.advance_due());
    }

    #[test]
    fn an_unreset_step_axis_stays_due() {
        // The step axis never resets by itself: past the cadence it keeps
        // firing until the owner performs a save and resets.
        let cadence = CheckpointCadence::new(Duration::MAX, NonZeroU64::new(2));
        assert!(!cadence.advance_due());
        assert!(cadence.advance_due());
        assert!(cadence.advance_due());
    }

    #[test]
    fn the_axes_union_step_fires_under_a_far_off_clock() {
        let cadence = CheckpointCadence::new(Duration::from_secs(3600), NonZeroU64::new(1));
        assert!(cadence.advance_due());
    }

    #[test]
    fn the_axes_union_clock_fires_under_a_far_off_step_cadence() {
        let cadence = CheckpointCadence::new(Duration::ZERO, NonZeroU64::new(1000));
        assert!(cadence.advance_due());
    }
}
