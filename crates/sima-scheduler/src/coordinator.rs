//! [`Coordinator`]: the shared run coordination.
//!
//! One `Coordinator` per run holds everything the scheduler threads share — the
//! ready queue, the lease table, and the wind-down state — behind a single
//! mutex, plus the condition variable every thread waits on. Its methods are
//! the only access: leasing the next task, the settlement methods that release
//! a lease and apply the outcome atomically, and the gate the driver reads to
//! decide what to do next. Nothing outside this module holds the lock, so the
//! invariant is enforced by visibility rather than asserted in prose.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Duration;

use sima_contracts::DeviceClass;
use sima_core::Error;
use sima_model::TaskKey;

use crate::placement::{ChainPlacement, Eligibility, eligibility};
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

/// A leased task and what the pull decided about its chain's placement. The
/// decision leaves the lock so its slot write and journal event happen off it.
pub(crate) struct Leased {
    pub(crate) pending: Pending,
    pub(crate) placement: ChainPlacement,
}

/// What the driver's next step is, as the shared state decides it.
pub(crate) enum Gate {
    /// The run wound down and every lease is released; the payload is the
    /// reason, taken by value.
    Terminal(RunState),
    /// The queue is empty and work has settled since the last poll, so the
    /// source is asked for more. `idle` is whether any lease is outstanding —
    /// an empty poll finalizes only when none is.
    Poll { idle: bool, settled: u64 },
}

/// A definitive candidate failure: the task that could not produce a result,
/// and why.
pub(crate) struct Failure {
    pub(crate) task: TaskKey,
    pub(crate) reason: String,
}

/// Why the run is winding down. `Running` is the steady state; every other
/// variant is terminal and makes each worker stop pulling new work. The
/// variants form a precedence order — `Running < Interrupted < Failed <
/// Fault` — and each setter only upgrades: an interrupt never displaces a
/// failure, a failure never displaces a fault, and among faults the first
/// wins. `Finished` sits outside the order as the drained sentinel the
/// driver installs when it takes the terminal state.
pub(crate) enum RunState {
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

/// The mutable state every scheduler thread shares. Every field is private:
/// the methods of [`Coordinator`] are the only way in.
struct Shared {
    /// FIFO of tasks ready to lease.
    queue: VecDeque<Pending>,
    /// The in-memory lease table: the set of tasks in flight. Every insertion
    /// and removal pairs with the task being leased or settled under this
    /// lock; the holding worker and the attempt travel in the journal events,
    /// and leases live in memory only — a process death drops them all, and
    /// resume re-derives the frontier from the store.
    leases: HashSet<TaskKey>,
    /// The run's wind-down state.
    state: RunState,
    /// Monotonic count of lease releases: incremented once per settled
    /// attempt, whatever the outcome. The driver records this count at each
    /// poll of the task source and polls again only after it has moved.
    /// The count is the right poll trigger because a source derives its
    /// frontier from committed records, only workers commit them, and a
    /// worker commits before releasing its lease — so an unchanged count
    /// means a re-poll would derive the same frontier.
    settled: u64,
    /// The device class each chain's work runs on, seeded at run start from
    /// the store's placement slots and extended as chains bind. Empty and
    /// unread when the run has one implicit class.
    chains: HashMap<u64, DeviceClass>,
    /// The class each chain-less task's attempts run on. A stateless task is a
    /// chain of length one: its retries stick within the run, and once it
    /// commits there is nothing left to place — so this stays in memory and
    /// no slot is ever written for it.
    stateless: HashMap<TaskKey, DeviceClass>,
    /// Workers still alive, seeded to the pool's slot total before the workers
    /// spawn and decremented as each exits. When the last one exits while the
    /// run is still `Running`, no worker remains to drain the queue, so the run
    /// faults instead of the driver waiting forever.
    live_workers: usize,
}

impl Shared {
    /// The class a pending task's work is bound to, if any. A chained task
    /// reads the run's durable placement; a chain-less one reads the in-memory
    /// retry stickiness.
    ///
    /// Borrowed rather than owned: the queue scan calls this once per queued
    /// task on every pull, and a class is a string.
    fn binding(&self, pending: &Pending) -> Option<&DeviceClass> {
        match pending.task.chain {
            Some(chain) => self.chains.get(&chain),
            None => self.stateless.get(&pending.key),
        }
    }

    /// Applies what a pull of `pending` by a worker of `class` decides, and
    /// reports it for the caller to persist and journal.
    ///
    /// The class the task was bound to is read here rather than passed in, so
    /// the one clone this needs happens only on the task that was taken.
    fn bind(&mut self, pending: &Pending, class: &DeviceClass) -> ChainPlacement {
        match pending.task.chain {
            // A chain-less task places in memory alone: it is a chain of
            // length one, so nothing outlives the run to be coherent with.
            None => {
                self.stateless.insert(pending.key, class.clone());
                ChainPlacement::Settled
            }
            // The common pull is of a chain already bound to the pulling
            // class, so that case reads and returns without writing the map or
            // cloning the class.
            Some(chain) if self.chains.get(&chain) == Some(class) => ChainPlacement::Settled,
            Some(chain) => match self.chains.insert(chain, class.clone()) {
                Some(from) => ChainPlacement::Rebound {
                    chain,
                    from,
                    to: class.clone(),
                },
                None => ChainPlacement::Bound {
                    chain,
                    to: class.clone(),
                },
            },
        }
    }
}

/// The shared state plus the condition every thread waits on.
pub(crate) struct Coordinator {
    state: Mutex<Shared>,
    state_changed: Condvar,
    /// The classes the run has. Empty for the single implicit class, which is
    /// what makes [`Coordinator::next_task`] short-circuit placement entirely.
    classes: Vec<DeviceClass>,
}

impl Coordinator {
    /// A fresh coordinator in the steady `Running` state: empty queue, no
    /// leases, one implicit device class — the shape every test that does not
    /// exercise placement wants. The driver builds its own through
    /// [`Coordinator::with_placement`].
    #[cfg(test)]
    pub(crate) fn new() -> Coordinator {
        Coordinator::with_placement(Vec::new(), HashMap::new())
    }

    /// A fresh coordinator for a run over `classes`, its chain placements
    /// seeded from `chains` — the bindings the store already holds, so a
    /// resumed chain returns to the class it ran on.
    pub(crate) fn with_placement(
        classes: Vec<DeviceClass>,
        chains: HashMap<u64, DeviceClass>,
    ) -> Coordinator {
        Coordinator {
            state: Mutex::new(Shared {
                queue: VecDeque::new(),
                leases: HashSet::new(),
                state: RunState::Running,
                settled: 0,
                chains,
                stateless: HashMap::new(),
                live_workers: 0,
            }),
            state_changed: Condvar::new(),
            classes,
        }
    }

    /// Locks the shared state, recovering a poisoned lock. Poisoning would mean
    /// a thread panicked holding the lock; the scheduler panics only inside the
    /// worker's `catch_unwind`, which holds no lock, so the lock is not
    /// poisoned in practice.
    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Leases the next task the calling worker's `class` may run, inserting
    /// its lease and counting it in flight; returns `None` once the run is
    /// winding down and the calling worker should exit.
    ///
    /// A worker of a class takes the first queued task its class is eligible
    /// for, so an unbound chain goes to whichever class reaches it first and a
    /// bound one waits for its own. A run with one implicit class passes
    /// `None` and takes the head of the queue, exactly as a run with no
    /// placement does.
    ///
    /// The park is unconditional: a worker whose class has nothing eligible
    /// waits for the next state change rather than spinning, and settles,
    /// polls, and wind-down all wake it.
    pub(crate) fn next_task(&self, class: Option<&DeviceClass>) -> Option<Leased> {
        let mut shared = self.lock();
        loop {
            if !matches!(shared.state, RunState::Running) {
                // Winding down: pull no more work. Wake peers and the driver so
                // they observe the state and drain.
                self.state_changed.notify_all();
                return None;
            }
            if let Some(leased) = self.take_eligible(&mut shared, class) {
                shared.leases.insert(leased.pending.key);
                return Some(leased);
            }
            // Nothing to take: wake the driver — it may have a poll or a
            // wind-down decision pending on this — before parking.
            self.state_changed.notify_all();
            shared = self
                .state_changed
                .wait(shared)
                .unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Removes the first queued task `class` may run and records what that
    /// pull decided about its placement, or `None` when the queue holds
    /// nothing for this class.
    fn take_eligible(&self, shared: &mut Shared, class: Option<&DeviceClass>) -> Option<Leased> {
        // The single implicit class: every task is eligible, so the head of
        // the queue wins and no placement state is touched at all.
        let Some(class) = class else {
            return shared.queue.pop_front().map(|pending| Leased {
                pending,
                placement: ChainPlacement::Settled,
            });
        };
        // The scan runs over every pending task on every pull, so it borrows
        // each task's binding rather than cloning a class per entry.
        let position = shared.queue.iter().position(|pending| {
            !matches!(
                eligibility(shared.binding(pending), class, &self.classes),
                Eligibility::Skip
            )
        })?;
        let pending = shared.queue.remove(position).expect("position is in range");
        let placement = shared.bind(&pending, class);
        Some(Leased { pending, placement })
    }

    /// Releases `key`'s lease, applies the outcome-specific update to the
    /// shared state, and wakes the pool. Every settle path — resolve, requeue,
    /// terminate, fault — shares this lock, lease removal, and notification;
    /// the four wrappers carry only their own middle step.
    fn settle<R>(&self, key: TaskKey, apply: impl FnOnce(&mut Shared) -> R) -> R {
        let mut shared = self.lock();
        shared.leases.remove(&key);
        shared.settled += 1;
        let result = apply(&mut shared);
        self.state_changed.notify_all();
        result
    }

    /// Clears a lease with no further state change: the task committed, or a
    /// run winding down abandoned its in-flight attempt.
    pub(crate) fn resolve(&self, key: TaskKey) {
        self.settle(key, |_shared| {});
    }

    /// Clears the lease and, while the run is still healthy, re-enqueues the
    /// task at the next attempt. Returns whether it was re-enqueued: a run
    /// already winding down abandons the task rather than queueing work no
    /// worker will take.
    pub(crate) fn requeue(&self, key: TaskKey, task: RunnableTask, next_attempt: u32) -> bool {
        self.settle(key, |shared| {
            let running = matches!(shared.state, RunState::Running);
            if running {
                // The key is already in hand as a parameter — the requeued
                // attempt reuses it rather than recomputing it under the lock.
                shared.queue.push_back(Pending {
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
        self.settle(key, |shared| {
            if matches!(shared.state, RunState::Running | RunState::Interrupted) {
                shared.state = RunState::Failed(Failure { task: key, reason });
            }
        });
    }

    /// Requests a graceful wind-down: it upgrades `Running` to `Interrupted`
    /// and nothing else, since every other state already outranks an
    /// interrupt in the precedence order.
    pub(crate) fn interrupt(&self) {
        let mut shared = self.lock();
        if matches!(shared.state, RunState::Running) {
            shared.state = RunState::Interrupted;
        }
        self.state_changed.notify_all();
    }

    /// Records an infrastructure fault: it outranks a definitive candidate
    /// failure, because the result path itself broke and the run must surface
    /// the error. The first fault wins; a later candidate failure never
    /// displaces it.
    pub(crate) fn fault(&self, key: TaskKey, err: Error) {
        self.settle(key, |shared| {
            if !matches!(shared.state, RunState::Fault(_)) {
                shared.state = RunState::Fault(err);
            }
        });
    }

    /// Records an infrastructure fault that holds no lease — a worker that
    /// cannot be spawned or respawned. Same precedence as [`fault`]: the
    /// first fault wins.
    ///
    /// [`fault`]: Coordinator::fault
    pub(crate) fn fault_run(&self, err: Error) {
        let mut shared = self.lock();
        if !matches!(shared.state, RunState::Fault(_)) {
            shared.state = RunState::Fault(err);
        }
        self.state_changed.notify_all();
    }

    /// Whether the run is still in its steady state, for a caller deciding
    /// whether to keep working rather than to wind down.
    pub(crate) fn is_running(&self) -> bool {
        matches!(self.lock().state, RunState::Running)
    }

    /// What the driver does next, given the settle count it last polled at.
    ///
    /// Returns `None` after parking for up to `park`: there was nothing to do,
    /// and the caller loops around to re-check whatever it watches outside the
    /// lock. The park happens under the same lock acquisition as the decision,
    /// so a notification arriving between the two is not missed.
    pub(crate) fn next_gate(&self, polled_at: Option<u64>, park: Duration) -> Option<Gate> {
        let mut shared = self.lock();
        if !matches!(shared.state, RunState::Running) {
            // Drained means every lease is released; the queue may still hold
            // tasks no worker will take, which is what a wind-down leaves.
            if shared.leases.is_empty() {
                // Take the terminal reason by value: the payload moves out to
                // be returned, and the `Finished` left in its place is the
                // signal that makes every worker's next_task exit.
                let terminal = std::mem::replace(&mut shared.state, RunState::Finished);
                self.state_changed.notify_all();
                return Some(Gate::Terminal(terminal));
            }
        } else if shared.queue.is_empty() && polled_at != Some(shared.settled) {
            // The gate ignores outstanding leases, so a chain task's successor
            // is handed out the moment its own predecessor commits, while other
            // tasks still run.
            return Some(Gate::Poll {
                idle: shared.leases.is_empty(),
                settled: shared.settled,
            });
        }
        // Nothing to do: park under the same lock acquisition, so a
        // notification arriving between the decision and the park is not
        // missed.
        drop(
            self.state_changed
                .wait_timeout(shared, park)
                .unwrap_or_else(|p| p.into_inner()),
        );
        None
    }

    /// Installs the drained sentinel: the driver saw the work through and the
    /// pool may exit.
    pub(crate) fn finish(&self) {
        self.lock().state = RunState::Finished;
        self.state_changed.notify_all();
    }

    /// Puts freshly polled tasks at the back of the ready queue and wakes the
    /// workers waiting for them.
    pub(crate) fn push_ready(&self, pending: Vec<Pending>) {
        self.lock().queue.extend(pending);
        self.state_changed.notify_all();
    }

    /// The run's wind-down state, for an assertion about which one it reached.
    /// Reading it costs the lock, so it is handed to a closure rather than
    /// cloned: `RunState` carries an `Error` and a `Failure`, neither of which
    /// is cloneable.
    #[cfg(test)]
    pub(crate) fn with_state<T>(&self, read: impl FnOnce(&RunState) -> T) -> T {
        read(&self.lock().state)
    }

    /// The count of lease releases so far, the figure the poll gate turns on.
    #[cfg(test)]
    pub(crate) fn releases(&self) -> u64 {
        self.lock().settled
    }

    /// The keys waiting in the ready queue, in order.
    #[cfg(test)]
    pub(crate) fn queued(&self) -> Vec<TaskKey> {
        self.lock()
            .queue
            .iter()
            .map(|pending| pending.key)
            .collect()
    }

    /// Records `key` as in flight without a worker having leased it, for a
    /// test that exercises what a settlement does to a standing lease.
    #[cfg(test)]
    pub(crate) fn hold_lease(&self, key: TaskKey) {
        self.lock().leases.insert(key);
    }

    /// Whether `key` is in flight.
    #[cfg(test)]
    pub(crate) fn holds_lease(&self, key: &TaskKey) -> bool {
        self.lock().leases.contains(key)
    }

    /// Seeds the live-worker count to `count`, the pool's slot total, before
    /// the workers spawn. Set once at run start.
    pub(crate) fn set_live_workers(&self, count: usize) {
        self.lock().live_workers = count;
    }

    /// Records a worker's exit. The last worker leaving while the run is still
    /// `Running` faults it: no worker remains to drain the queue, so the driver
    /// would otherwise wait forever. A worker that exits after the run has
    /// already wound down decrements the count and nothing more — the terminal
    /// state stands.
    pub(crate) fn worker_exited(&self) {
        let mut shared = self.lock();
        shared.live_workers = shared.live_workers.saturating_sub(1);
        if shared.live_workers == 0 && matches!(shared.state, RunState::Running) {
            shared.state = RunState::Fault(Error::Transport(
                "every worker retired with work still pending".to_string(),
            ));
        }
        self.state_changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use sima_core::hash_bytes;
    use sima_model::{EnvironmentId, ParamsId, SpecId, TaskIdentity};

    use super::*;

    /// A coordinator in a given run state, with an empty queue and lease table.
    fn coordinator_with(state: RunState) -> Coordinator {
        let coordinator = Coordinator::new();
        coordinator.lock().state = state;
        coordinator
    }

    /// A throwaway task key.
    fn a_key() -> TaskKey {
        TaskKey::from_hash(hash_bytes(b"fault-upgrade task"))
    }

    /// A class that must be valid.
    fn class(id: &str) -> DeviceClass {
        DeviceClass::new(id).expect("a valid class id")
    }

    /// The Intel iGPU class.
    fn intel() -> DeviceClass {
        class("8086:7d51")
    }

    /// The NVIDIA dGPU class.
    fn nvidia() -> DeviceClass {
        class("10de:2d39")
    }

    /// A queued task of `chain`, distinguishable by `seed`.
    fn pending(chain: Option<u64>, seed: u64) -> Pending {
        let identity = TaskIdentity {
            spec: SpecId::from_hash(hash_bytes(b"spec")),
            params: ParamsId::from_hash(hash_bytes(b"params")),
            seed,
            environment: EnvironmentId::from_hash(hash_bytes(b"env")),
            input_state: None,
        };
        Pending {
            key: identity.key(),
            task: RunnableTask {
                spec: sima_model::Spec {
                    format: sima_model::FormatId::new("stub.v1").expect("format id"),
                    bytes: Vec::new(),
                },
                identity,
                chain,
            },
            attempt: 0,
        }
    }

    /// A park short enough that a test which reaches it is not held up, and
    /// long enough that nothing races it: every gate test here expects a
    /// decision rather than the park.
    const PARK: Duration = Duration::from_millis(1);

    /// A coordinator over both classes, holding `queue`.
    fn placed(queue: Vec<Pending>, chains: HashMap<u64, DeviceClass>) -> Coordinator {
        let coordinator = Coordinator::with_placement(vec![nvidia(), intel()], chains);
        coordinator.lock().queue = queue.into();
        coordinator
    }

    /// What `class` would take from `coordinator`'s queue right now.
    fn take(coordinator: &Coordinator, class: &DeviceClass) -> Option<Leased> {
        let mut shared = coordinator.lock();
        coordinator.take_eligible(&mut shared, Some(class))
    }

    #[test]
    fn a_worker_runs_a_chain_bound_to_its_own_class() {
        let task = pending(Some(0), 1);
        let key = task.key;
        let coordinator = placed(vec![task], HashMap::from([(0, nvidia())]));
        let leased = take(&coordinator, &nvidia()).expect("its own chain");
        assert_eq!(leased.pending.key, key);
        // Already bound: nothing to persist or journal.
        assert_eq!(leased.placement, ChainPlacement::Settled);
    }

    #[test]
    fn a_worker_never_takes_a_chain_bound_to_another_present_class() {
        // The invariant stickiness rests on: an idle worker of the wrong class
        // leaves the work for the class that owns it, however long that waits.
        let coordinator = placed(vec![pending(Some(0), 1)], HashMap::from([(0, nvidia())]));
        assert!(take(&coordinator, &intel()).is_none());
        // The task is still queued for the class that owns it.
        assert_eq!(coordinator.lock().queue.len(), 1);
        assert!(take(&coordinator, &nvidia()).is_some());
    }

    #[test]
    fn a_worker_passes_over_an_ineligible_task_to_a_later_eligible_one() {
        // The queue is FIFO, but a worker takes the first task it may run, so
        // one class's bound chain never blocks another class's work.
        let theirs = pending(Some(0), 1);
        let mine = pending(Some(1), 2);
        let mine_key = mine.key;
        let coordinator = placed(
            vec![theirs, mine],
            HashMap::from([(0, nvidia()), (1, intel())]),
        );
        let leased = take(&coordinator, &intel()).expect("its own chain, behind another's");
        assert_eq!(leased.pending.key, mine_key);
        assert_eq!(coordinator.lock().queue.len(), 1, "the other's chain stays");
    }

    #[test]
    fn an_unbound_chain_binds_to_whichever_class_pulls_it() {
        let coordinator = placed(vec![pending(Some(3), 1)], HashMap::new());
        let leased = take(&coordinator, &intel()).expect("an unbound chain is anyone's");
        assert_eq!(
            leased.placement,
            ChainPlacement::Bound {
                chain: 3,
                to: intel()
            }
        );
        // The binding is in the map before the task leaves the pull.
        assert_eq!(coordinator.lock().chains.get(&3), Some(&intel()));
    }

    #[test]
    fn a_chain_bound_to_an_absent_class_moves_to_a_class_that_is_here() {
        // A run whose devices do not include the chain's class: the work
        // moves rather than stranding, and the decision comes back to be
        // journaled.
        let gone = class("1002:1234");
        let coordinator = placed(
            vec![pending(Some(0), 1)],
            HashMap::from([(0, gone.clone())]),
        );
        let leased = take(&coordinator, &nvidia()).expect("the run continues");
        assert_eq!(
            leased.placement,
            ChainPlacement::Rebound {
                chain: 0,
                from: gone,
                to: nvidia()
            }
        );
        assert_eq!(coordinator.lock().chains.get(&0), Some(&nvidia()));
    }

    #[test]
    fn a_chain_less_tasks_retry_stays_on_the_class_that_first_ran_it() {
        // A stateless task is a chain of length one: its attempts stick within
        // the run, and nothing about it is persisted.
        let task = pending(None, 1);
        let key = task.key;
        let coordinator = placed(vec![task], HashMap::new());
        let leased = take(&coordinator, &intel()).expect("unbound");
        assert_eq!(
            leased.placement,
            ChainPlacement::Settled,
            "no slot to write"
        );

        // The attempt failed and requeued; only Intel may retry it.
        coordinator.lock().queue.push_back(pending(None, 1));
        assert!(take(&coordinator, &nvidia()).is_none());
        assert_eq!(
            take(&coordinator, &intel())
                .expect("its own retry")
                .pending
                .key,
            key
        );
        assert!(coordinator.lock().chains.is_empty(), "no chain, no binding");
    }

    #[test]
    fn one_implicit_class_takes_the_head_of_the_queue_untouched() {
        // The single-device run: no class, no placement state read or written,
        // and the queue stays strictly FIFO.
        let first = pending(Some(0), 1);
        let first_key = first.key;
        let coordinator = Coordinator::new();
        coordinator.lock().queue = vec![first, pending(Some(1), 2)].into();
        let mut shared = coordinator.lock();
        let leased = coordinator
            .take_eligible(&mut shared, None)
            .expect("the head");
        assert_eq!(leased.pending.key, first_key);
        assert_eq!(leased.placement, ChainPlacement::Settled);
        assert!(shared.chains.is_empty());
    }

    #[test]
    fn every_settled_attempt_moves_the_release_count_once() {
        // The driver polls the source only when this count has moved, so a
        // settlement that failed to move it would leave a chain's successor
        // unqueued until some other task settled. Every outcome counts: a
        // resolution, a requeue, and a termination alike.
        let coordinator = Coordinator::new();
        let task = pending(Some(0), 1);
        let key = task.key;
        assert_eq!(coordinator.releases(), 0);

        coordinator.hold_lease(key);
        coordinator.resolve(key);
        assert_eq!(coordinator.releases(), 1);

        coordinator.hold_lease(key);
        coordinator.requeue(key, task.task, 1);
        assert_eq!(coordinator.releases(), 2);

        coordinator.hold_lease(key);
        coordinator.terminate(key, "candidate rejected".to_string());
        assert_eq!(coordinator.releases(), 3);
    }

    #[test]
    fn the_poll_gate_opens_once_per_settlement() {
        // The gate is what turns a settlement into a poll: it opens while the
        // queue is empty and the count has moved past the caller's last poll,
        // and stays shut until the next settlement moves it again.
        let coordinator = Coordinator::new();
        let task = pending(Some(0), 1);
        let key = task.key;
        coordinator.hold_lease(key);
        coordinator.resolve(key);

        let Some(Gate::Poll { idle, settled }) = coordinator.next_gate(None, PARK) else {
            panic!("a settlement with an empty queue opens the poll gate");
        };
        assert!(idle, "no lease is outstanding");
        assert_eq!(settled, 1);

        // Polled at that count, the gate parks rather than polling again.
        assert!(coordinator.next_gate(Some(settled), PARK).is_none());
    }

    #[test]
    fn a_wound_down_run_yields_its_terminal_reason_once() {
        // The driver takes the reason by value and leaves `Finished` behind,
        // which is the signal every worker's next_task exits on — so a second
        // look yields the sentinel rather than the reason again.
        let coordinator = Coordinator::new();
        coordinator.interrupt();
        assert!(matches!(
            coordinator.next_gate(None, PARK),
            Some(Gate::Terminal(RunState::Interrupted))
        ));
        assert!(matches!(
            coordinator.next_gate(None, PARK),
            Some(Gate::Terminal(RunState::Finished))
        ));
    }

    #[test]
    fn a_wound_down_run_holding_a_lease_is_not_terminal_yet() {
        // The in-flight attempt has to settle first, or the driver would
        // return while a worker still holds a task.
        let coordinator = Coordinator::new();
        let key = a_key();
        coordinator.hold_lease(key);
        coordinator.interrupt();
        assert!(coordinator.next_gate(None, PARK).is_none());
        coordinator.resolve(key);
        assert!(matches!(
            coordinator.next_gate(None, PARK),
            Some(Gate::Terminal(RunState::Interrupted))
        ));
    }

    #[test]
    fn queued_work_shuts_the_poll_gate() {
        // A non-empty queue means the workers have something to take, so the
        // source is not asked for more however much has settled.
        let coordinator = Coordinator::new();
        coordinator.push_ready(vec![pending(Some(0), 1)]);
        assert!(coordinator.next_gate(None, PARK).is_none());
    }

    #[test]
    fn a_fault_upgrades_a_definitive_candidate_failure() {
        let coordinator = coordinator_with(RunState::Failed(Failure {
            task: a_key(),
            reason: "candidate rejected".to_string(),
        }));
        coordinator.fault(a_key(), Error::Corruption("store broke".to_string()));
        assert!(matches!(coordinator.lock().state, RunState::Fault(_)));
    }

    #[test]
    fn the_first_fault_is_kept_over_a_later_one() {
        let coordinator = coordinator_with(RunState::Fault(Error::Corruption(
            "first fault".to_string(),
        )));
        coordinator.fault(a_key(), Error::Validation("second fault".to_string()));
        match &coordinator.lock().state {
            RunState::Fault(e) => assert_eq!(e.to_string(), "store corruption: first fault"),
            _ => panic!("expected a fault run state"),
        }
    }

    #[test]
    fn an_interrupt_upgrades_a_running_coordinator() {
        let coordinator = Coordinator::new();
        coordinator.interrupt();
        assert!(matches!(coordinator.lock().state, RunState::Interrupted));
    }

    #[test]
    fn an_interrupt_never_displaces_a_definitive_failure() {
        let coordinator = coordinator_with(RunState::Failed(Failure {
            task: a_key(),
            reason: "candidate rejected".to_string(),
        }));
        coordinator.interrupt();
        assert!(matches!(coordinator.lock().state, RunState::Failed(_)));
    }

    #[test]
    fn an_interrupt_never_displaces_a_fault() {
        let coordinator = coordinator_with(RunState::Fault(Error::Corruption(
            "store broke".to_string(),
        )));
        coordinator.interrupt();
        assert!(matches!(coordinator.lock().state, RunState::Fault(_)));
    }

    #[test]
    fn a_definitive_failure_upgrades_an_interrupt() {
        let coordinator = coordinator_with(RunState::Interrupted);
        coordinator.terminate(a_key(), "candidate rejected".to_string());
        assert!(matches!(coordinator.lock().state, RunState::Failed(_)));
    }

    #[test]
    fn a_fault_upgrades_an_interrupt() {
        let coordinator = coordinator_with(RunState::Interrupted);
        coordinator.fault(a_key(), Error::Corruption("store broke".to_string()));
        assert!(matches!(coordinator.lock().state, RunState::Fault(_)));
    }

    #[test]
    fn a_run_fault_upgrades_without_touching_leases() {
        let coordinator = Coordinator::new();
        coordinator.lock().leases.insert(a_key());
        coordinator.fault_run(Error::Transport("spawn failed".to_string()));
        let shared = coordinator.lock();
        assert!(matches!(shared.state, RunState::Fault(_)));
        // No lease settles: the fault holds none.
        assert!(shared.leases.contains(&a_key()));
        assert_eq!(shared.settled, 0);
    }

    #[test]
    fn a_run_fault_never_displaces_an_earlier_fault() {
        let coordinator = coordinator_with(RunState::Fault(Error::Corruption(
            "first fault".to_string(),
        )));
        coordinator.fault_run(Error::Transport("respawn failed".to_string()));
        match &coordinator.lock().state {
            RunState::Fault(e) => assert_eq!(e.to_string(), "store corruption: first fault"),
            _ => panic!("expected a fault run state"),
        }
    }
}
