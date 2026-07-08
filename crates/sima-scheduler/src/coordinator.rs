//! [`Coordinator`]: the shared run coordination.
//!
//! One `Coordinator` per run holds everything the scheduler threads share — the
//! ready queue, the lease table, and the wind-down state — behind a single
//! mutex, plus the condition variable every thread waits on. Its methods are
//! the only mutations: leasing the next task and the settlement family that
//! releases a lease and applies the outcome atomically.

use std::collections::{HashMap, VecDeque};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Instant;

use sima_contracts::WorkerId;
use sima_core::Error;
use sima_model::TaskKey;

use crate::lease::Lease;
use crate::task_source::RunnableTask;

/// A task waiting in the ready queue: the runnable task and the attempt it is
/// queued for.
pub(crate) struct Pending {
    pub(crate) task: RunnableTask,
    /// The task key, equal to `task.identity.key()`, computed once where the
    /// `Pending` is built so no lifecycle step recomputes it under the lock.
    pub(crate) key: TaskKey,
    pub(crate) attempt: u32,
}

/// A definitive candidate failure: the task that could not produce a result,
/// and why.
pub(crate) struct Failure {
    pub(crate) task: TaskKey,
    pub(crate) reason: String,
}

/// Why the run is winding down. `Running` is the steady state; every other
/// variant is terminal and makes each worker stop pulling new work. The
/// variants form a precedence lattice — `Running < Interrupted < Failed <
/// Fault` — and each setter only upgrades: an interrupt never displaces a
/// failure, a failure never displaces a fault, and among faults the first
/// wins. `Finished` sits outside the lattice as the drained sentinel the
/// driver installs when it takes the terminal state.
pub(crate) enum Stop {
    /// Work is proceeding.
    Running,
    /// The caller requested a graceful wind-down; the run returns
    /// [`RunOutcome::Interrupted`](crate::RunOutcome::Interrupted).
    Interrupted,
    /// A definitive candidate failure; the run returns
    /// [`RunOutcome::Failed`](crate::RunOutcome::Failed).
    Failed(Failure),
    /// An infrastructure fault; the run returns `Err`.
    Fault(Error),
    /// The driver saw the work through and asked the pool to exit.
    Finished,
}

/// The mutable state every scheduler thread shares.
pub(crate) struct Shared {
    /// FIFO of tasks ready to lease.
    pub(crate) queue: VecDeque<Pending>,
    /// The in-memory lease table, keyed by task. Its size is the count of
    /// tasks in flight: every lease insertion and removal pairs with the task
    /// being leased or resolved under this lock.
    pub(crate) leases: HashMap<TaskKey, Lease>,
    /// The run's wind-down state.
    pub(crate) stop: Stop,
}

/// The shared state plus the condition every thread waits on.
pub(crate) struct Coordinator {
    pub(crate) state: Mutex<Shared>,
    pub(crate) idle: Condvar,
}

impl Coordinator {
    /// A fresh coordinator in the steady `Running` state: empty queue, no
    /// leases.
    pub(crate) fn new() -> Coordinator {
        Coordinator {
            state: Mutex::new(Shared {
                queue: VecDeque::new(),
                leases: HashMap::new(),
                stop: Stop::Running,
            }),
            idle: Condvar::new(),
        }
    }

    /// Locks the shared state, recovering a poisoned lock. Poisoning would mean
    /// a thread panicked holding the lock; the scheduler panics only inside the
    /// worker's `catch_unwind`, which holds no lock, so the lock is not
    /// poisoned in practice.
    pub(crate) fn lock(&self) -> MutexGuard<'_, Shared> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Leases the next ready task, inserting its lease and counting it in
    /// flight; returns `None` once the run is winding down and the calling
    /// worker should exit.
    pub(crate) fn next_task(&self, worker: WorkerId) -> Option<Pending> {
        let mut state = self.lock();
        loop {
            if !matches!(state.stop, Stop::Running) {
                // Winding down: pull no more work. Wake peers and the driver so
                // they observe the state and drain.
                self.idle.notify_all();
                return None;
            }
            if let Some(pending) = state.queue.pop_front() {
                let key = pending.key;
                // The lease records when the attempt started; the watchdog
                // derives expiry from its age, so there is no deadline
                // arithmetic that could overflow.
                state.leases.insert(
                    key,
                    Lease {
                        worker,
                        attempt: pending.attempt,
                        leased_at: Instant::now(),
                    },
                );
                return Some(pending);
            }
            // The queue is empty: this worker may be the one making the pool
            // quiescent, so wake the driver before parking for new work.
            self.idle.notify_all();
            state = self.idle.wait(state).unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Releases `key`'s lease, applies the outcome-specific update to the
    /// shared state, and wakes the pool. Every settle path — resolve, requeue,
    /// terminate, fault — shares this lock, lease removal, and notification;
    /// the four wrappers carry only their own middle step.
    fn settle<R>(&self, key: TaskKey, apply: impl FnOnce(&mut Shared) -> R) -> R {
        let mut state = self.lock();
        state.leases.remove(&key);
        let result = apply(&mut state);
        self.idle.notify_all();
        result
    }

    /// Clears a resolved lease, which removes its task from the in-flight set.
    pub(crate) fn resolve(&self, key: TaskKey) {
        self.settle(key, |_state| {});
    }

    /// Clears the lease and, while the run is still healthy, re-enqueues the
    /// task at the next attempt. Returns whether it was re-enqueued: a run
    /// already winding down abandons the task rather than queueing work no
    /// worker will take.
    pub(crate) fn requeue(&self, key: TaskKey, task: RunnableTask, next_attempt: u32) -> bool {
        self.settle(key, |state| {
            let running = matches!(state.stop, Stop::Running);
            if running {
                // The key is already in hand as a parameter — the requeued
                // attempt reuses it rather than recomputing it under the lock.
                state.queue.push_back(Pending {
                    key,
                    task,
                    attempt: next_attempt,
                });
            }
            running
        })
    }

    /// Records a definitive candidate failure. It upgrades the healthy and
    /// interrupted states — a candidate that failed definitively during an
    /// interrupt wind-down still decides the run — and never downgrades an
    /// infrastructure fault; the first candidate failure among candidate
    /// failures wins.
    pub(crate) fn terminate(&self, key: TaskKey, reason: String) {
        self.settle(key, |state| {
            if matches!(state.stop, Stop::Running | Stop::Interrupted) {
                state.stop = Stop::Failed(Failure { task: key, reason });
            }
        });
    }

    /// Requests a graceful wind-down: it upgrades `Running` to `Interrupted`
    /// and nothing else, since every other state already outranks an
    /// interrupt on the stop lattice.
    pub(crate) fn interrupt(&self) {
        let mut state = self.lock();
        if matches!(state.stop, Stop::Running) {
            state.stop = Stop::Interrupted;
        }
        self.idle.notify_all();
    }

    /// Records an infrastructure fault: it outranks a definitive candidate
    /// failure, because the result path itself broke and the run must surface
    /// the error. The first fault wins; a later candidate failure never
    /// displaces it.
    pub(crate) fn fault(&self, key: TaskKey, err: Error) {
        self.settle(key, |state| {
            if !matches!(state.stop, Stop::Fault(_)) {
                state.stop = Stop::Fault(err);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use sima_core::hash_bytes;

    use super::*;

    /// A coordinator in a given stop state, with an empty queue and lease table.
    fn coordinator_with(stop: Stop) -> Coordinator {
        let coordinator = Coordinator::new();
        coordinator.lock().stop = stop;
        coordinator
    }

    /// A throwaway task key.
    fn a_key() -> TaskKey {
        TaskKey::from_hash(hash_bytes(b"fault-upgrade task"))
    }

    #[test]
    fn a_fault_upgrades_a_definitive_candidate_failure() {
        let coordinator = coordinator_with(Stop::Failed(Failure {
            task: a_key(),
            reason: "candidate rejected".to_string(),
        }));
        coordinator.fault(a_key(), Error::Corruption("store broke".to_string()));
        assert!(matches!(coordinator.lock().stop, Stop::Fault(_)));
    }

    #[test]
    fn the_first_fault_is_kept_over_a_later_one() {
        let coordinator = coordinator_with(Stop::Fault(Error::Corruption("first fault".to_string())));
        coordinator.fault(a_key(), Error::Validation("second fault".to_string()));
        match &coordinator.lock().stop {
            Stop::Fault(e) => assert_eq!(e.to_string(), "store corruption: first fault"),
            _ => panic!("expected a fault stop state"),
        }
    }

    #[test]
    fn an_interrupt_upgrades_a_running_coordinator() {
        let coordinator = Coordinator::new();
        coordinator.interrupt();
        assert!(matches!(coordinator.lock().stop, Stop::Interrupted));
    }

    #[test]
    fn an_interrupt_never_displaces_a_definitive_failure() {
        let coordinator = coordinator_with(Stop::Failed(Failure {
            task: a_key(),
            reason: "candidate rejected".to_string(),
        }));
        coordinator.interrupt();
        assert!(matches!(coordinator.lock().stop, Stop::Failed(_)));
    }

    #[test]
    fn an_interrupt_never_displaces_a_fault() {
        let coordinator = coordinator_with(Stop::Fault(Error::Corruption("store broke".to_string())));
        coordinator.interrupt();
        assert!(matches!(coordinator.lock().stop, Stop::Fault(_)));
    }

    #[test]
    fn a_definitive_failure_upgrades_an_interrupt() {
        let coordinator = coordinator_with(Stop::Interrupted);
        coordinator.terminate(a_key(), "candidate rejected".to_string());
        assert!(matches!(coordinator.lock().stop, Stop::Failed(_)));
    }

    #[test]
    fn a_fault_upgrades_an_interrupt() {
        let coordinator = coordinator_with(Stop::Interrupted);
        coordinator.fault(a_key(), Error::Corruption("store broke".to_string()));
        assert!(matches!(coordinator.lock().stop, Stop::Fault(_)));
    }
}
