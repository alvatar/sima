//! The tui state machine: a [`Msg`] folds into [`TuiState`], which projects a
//! [`ViewModel`] the view draws.
//!
//! `TuiState` owns a [`RunStatus`] and delegates every lifecycle event to its
//! fold, so the run's counters and worker occupancy come from the one
//! accumulator `sima status` also uses. Only UI concerns fold here: which run
//! the session is driving, whether a start or stop is pending for the runtime
//! to act on, whether an exit is armed, and the session outcome that decides
//! the exit code. No terminal types, no channels, no I/O.

use std::collections::VecDeque;

use sima_core::Result;
use sima_pipeline::{LifecycleEvent, Occupancy, RunOutcome, RunStatus};

use crate::render::describe;

/// How many rendered event lines the log keeps; older lines scroll off.
const LOG_CAPACITY: usize = 100;

/// A key press mapped to its meaning, decoupled from the terminal backend so
/// the state machine never names a crossterm type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyAction {
    /// Start the run, or restart it after a terminal state.
    Start,
    /// Wind the run down gracefully.
    Stop,
    /// Leave: immediately when idle or ended, else stop and leave once the
    /// run returns.
    Quit,
    /// Leave at once without draining.
    ForceQuit,
}

/// A message folded into the UI state: a lifecycle event from the run, a key
/// press, or the run thread's terminal result.
#[derive(Debug)]
pub enum Msg {
    /// One lifecycle event from the observer stream.
    Event(LifecycleEvent),
    /// A key press, already mapped to its action.
    Key(KeyAction),
    /// The orchestrate thread returned this outcome.
    Finished(Result<RunOutcome>),
}

/// What the session is doing, as the header reads it. The terminal run states
/// (finalized, failed, interrupted) are projected from the session outcome
/// once the run thread has returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Activity {
    /// No run driven yet, or a run finished and none is active.
    Idle,
    /// The orchestrate thread is running.
    Running,
    /// An interrupt was requested; the run is draining.
    WindingDown,
    /// The run thread returned; the outcome holds the terminal state.
    Ended,
}

/// The session's outcome, which decides the exit code. A tui session may
/// drive several runs; the last terminal state decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionOutcome {
    /// No run reached a terminal state — fresh, or quit while idle.
    Clean,
    /// The last run finalized.
    Finalized,
    /// The last run failed definitively.
    Failed,
    /// A run was interrupted, or force-quit while in flight.
    Interrupted,
    /// An infrastructure fault surfaced from a run.
    Faulted,
}

impl SessionOutcome {
    /// The exit code this outcome maps to, matching `sima run`: success for a
    /// clean or finalized session, the failure code for a definitive failure,
    /// the interrupt code for a wound-down or force-quit run, and the generic
    /// error code for a fault.
    fn exit_code(self) -> u8 {
        match self {
            SessionOutcome::Clean | SessionOutcome::Finalized => 0,
            SessionOutcome::Failed => crate::EXIT_FAILED,
            SessionOutcome::Interrupted => crate::EXIT_INTERRUPTED,
            SessionOutcome::Faulted => crate::EXIT_ERROR,
        }
    }

    /// The outcome a returned run result carries.
    fn of(result: &Result<RunOutcome>) -> SessionOutcome {
        match result {
            Ok(RunOutcome::Finalized { .. }) => SessionOutcome::Finalized,
            Ok(RunOutcome::Failed { .. }) => SessionOutcome::Failed,
            Ok(RunOutcome::Interrupted { .. }) => SessionOutcome::Interrupted,
            Err(_) => SessionOutcome::Faulted,
        }
    }
}

/// The UI state: the folded run status plus the session's own concerns.
pub struct TuiState {
    /// The run's observable state, folded from the observer stream through
    /// the shared accumulator.
    status: RunStatus,
    /// The configured worker count, for one panel row per worker.
    workers: usize,
    /// What the session is doing.
    activity: Activity,
    /// The session outcome deciding the exit code.
    outcome: SessionOutcome,
    /// A start is pending: the runtime should spawn the orchestrate thread.
    start_pending: bool,
    /// A stop is pending: the runtime should set the interrupt flag.
    stop_pending: bool,
    /// Once the active run returns, leave the loop.
    exit_on_finish: bool,
    /// Leave the loop now.
    exit: bool,
    /// The most recent rendered event lines, oldest first.
    log: VecDeque<String>,
}

/// One worker's row in the panel: the worker id and, if it holds a lease, the
/// leased task and attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRow {
    /// The worker id, `0..workers`.
    pub worker: u64,
    /// The lease the worker holds, or `None` when idle.
    pub lease: Option<Occupancy>,
}

/// The display projection the view draws: everything on screen, already
/// resolved, so the view only lays it out.
#[derive(Debug)]
pub struct ViewModel {
    /// The run the session drives, as its id string.
    pub run: String,
    /// The header state label: idle, running, winding down, or a terminal
    /// state.
    pub state: &'static str,
    /// One row per configured worker.
    pub workers: Vec<WorkerRow>,
    /// Committed task count.
    pub committed: usize,
    /// The run's task count.
    pub tasks: usize,
    /// Retry count.
    pub retried: usize,
    /// Rejection count.
    pub rejected: usize,
    /// Fault count.
    pub faulted: usize,
    /// Lease-expiry count.
    pub lease_expired: usize,
    /// The recent event lines, oldest first.
    pub log: Vec<String>,
}

impl TuiState {
    /// An idle session over `status` with `workers` configured workers. The
    /// status seeds the display — zeroed for a fresh store, or replayed from
    /// an existing journal — and the live stream folds into it from here.
    pub fn new(status: RunStatus, workers: usize) -> TuiState {
        TuiState {
            status,
            workers,
            activity: Activity::Idle,
            outcome: SessionOutcome::Clean,
            start_pending: false,
            stop_pending: false,
            exit_on_finish: false,
            exit: false,
            log: VecDeque::new(),
        }
    }

    /// Folds one message into the state.
    pub fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Event(event) => self.fold_event(event),
            Msg::Key(action) => self.fold_key(action),
            Msg::Finished(result) => self.fold_finished(&result),
        }
    }

    /// Folds one lifecycle event into the status and appends its line to the
    /// log, sharing the wording of `sima run` through [`describe`]. The
    /// commit count is read after the fold so a commit line shows the count
    /// that includes it.
    fn fold_event(&mut self, event: LifecycleEvent) {
        self.status.apply(&event);
        if let Some(line) = describe(&event, self.status.committed, self.status.tasks) {
            if self.log.len() == LOG_CAPACITY {
                self.log.pop_front();
            }
            self.log.push_back(line);
        }
    }

    /// Folds a key action into the session's activity and pending requests.
    fn fold_key(&mut self, action: KeyAction) {
        match action {
            KeyAction::Start => {
                // A run starts from idle or after a terminal state; while one
                // runs the key does nothing.
                if matches!(self.activity, Activity::Idle | Activity::Ended) {
                    self.activity = Activity::Running;
                    self.start_pending = true;
                    self.exit_on_finish = false;
                }
            }
            KeyAction::Stop => {
                if matches!(self.activity, Activity::Running) {
                    self.activity = Activity::WindingDown;
                    self.stop_pending = true;
                }
            }
            KeyAction::Quit => match self.activity {
                // Running: wind down and leave once the run returns.
                Activity::Running => {
                    self.activity = Activity::WindingDown;
                    self.stop_pending = true;
                    self.exit_on_finish = true;
                }
                // Already winding down: just arm the exit for the return.
                Activity::WindingDown => self.exit_on_finish = true,
                // Nothing in flight: leave now.
                Activity::Idle | Activity::Ended => self.exit = true,
            },
            KeyAction::ForceQuit => {
                // A run in flight is abandoned mid-run, which the shell reads
                // as an interrupt; the process exit releases its lock.
                if matches!(self.activity, Activity::Running | Activity::WindingDown) {
                    self.outcome = SessionOutcome::Interrupted;
                }
                self.exit = true;
            }
        }
    }

    /// Folds the run thread's return: the session moves to its terminal
    /// state, records the outcome, and leaves if an exit was armed.
    fn fold_finished(&mut self, result: &Result<RunOutcome>) {
        self.activity = Activity::Ended;
        self.outcome = SessionOutcome::of(result);
        if self.exit_on_finish {
            self.exit = true;
        }
    }

    /// Takes the pending start request, clearing it: the runtime spawns the
    /// orchestrate thread when this returns true.
    pub fn take_start(&mut self) -> bool {
        std::mem::take(&mut self.start_pending)
    }

    /// Takes the pending stop request, clearing it: the runtime sets the
    /// interrupt flag when this returns true.
    pub fn take_stop(&mut self) -> bool {
        std::mem::take(&mut self.stop_pending)
    }

    /// Whether the loop should leave.
    pub fn should_exit(&self) -> bool {
        self.exit
    }

    /// The session's exit code.
    pub fn exit_code(&self) -> u8 {
        self.outcome.exit_code()
    }

    /// The header state label for the current activity.
    fn state_label(&self) -> &'static str {
        match self.activity {
            Activity::Idle => "idle",
            Activity::Running => "running",
            Activity::WindingDown => "winding down",
            Activity::Ended => match self.outcome {
                SessionOutcome::Finalized => "finalized",
                SessionOutcome::Failed => "failed",
                SessionOutcome::Interrupted => "interrupted",
                SessionOutcome::Faulted => "faulted",
                SessionOutcome::Clean => "idle",
            },
        }
    }

    /// Projects the current state into a [`ViewModel`] for the view.
    pub fn view(&self) -> ViewModel {
        let workers = (0..self.workers as u64)
            .map(|worker| WorkerRow {
                worker,
                lease: self.status.occupancy.get(&worker).cloned(),
            })
            .collect();
        ViewModel {
            run: self.status.run.to_string(),
            state: self.state_label(),
            workers,
            committed: self.status.committed,
            tasks: self.status.tasks,
            retried: self.status.retried,
            rejected: self.status.rejected,
            faulted: self.status.faulted,
            lease_expired: self.status.lease_expired,
            log: self.log.iter().cloned().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::{Error, hash_bytes};
    use sima_model::{RunId, TaskKey};

    fn run_id() -> RunId {
        RunId::from_hash(hash_bytes(b"tui state run"))
    }

    fn idle(workers: usize) -> TuiState {
        TuiState::new(RunStatus::new(run_id()), workers)
    }

    fn started(tasks: usize) -> LifecycleEvent {
        LifecycleEvent::RunStarted {
            run: "00".repeat(32),
            tasks,
        }
    }

    fn leased(task: &str, worker: u64, attempt: u32) -> LifecycleEvent {
        LifecycleEvent::Leased {
            task: task.to_string(),
            worker,
            attempt,
        }
    }

    fn committed(task: &str) -> LifecycleEvent {
        LifecycleEvent::Committed {
            task: task.to_string(),
            record: "11".repeat(32),
            stats_hex: String::new(),
        }
    }

    fn finalized() -> Result<RunOutcome> {
        Ok(RunOutcome::Finalized { run: run_id() })
    }

    fn failed_outcome() -> Result<RunOutcome> {
        Ok(RunOutcome::Failed {
            task: TaskKey::from_hash(hash_bytes(b"a task")),
            reason: "rejected".to_string(),
        })
    }

    fn interrupted() -> Result<RunOutcome> {
        Ok(RunOutcome::Interrupted { run: run_id() })
    }

    #[test]
    fn an_idle_session_shows_idle_workers_and_start_requests_a_run() {
        let mut state = idle(2);
        let view = state.view();
        assert_eq!(view.state, "idle");
        assert_eq!(view.workers.len(), 2);
        assert!(view.workers.iter().all(|row| row.lease.is_none()));

        state.handle(Msg::Key(KeyAction::Start));
        assert!(state.take_start(), "start is requested");
        assert!(!state.take_start(), "the request is taken only once");
        assert_eq!(state.view().state, "running");
    }

    #[test]
    fn stop_is_a_no_op_while_idle() {
        let mut state = idle(1);
        state.handle(Msg::Key(KeyAction::Stop));
        assert!(!state.take_stop(), "nothing to stop while idle");
        assert_eq!(state.view().state, "idle");
    }

    #[test]
    fn events_reach_the_owned_status_and_show_through_the_view() {
        let mut state = idle(2);
        state.handle(Msg::Key(KeyAction::Start));
        let _ = state.take_start();
        state.handle(Msg::Event(started(2)));
        state.handle(Msg::Event(leased("aa", 0, 0)));
        state.handle(Msg::Event(committed("aa")));

        let view = state.view();
        assert_eq!(view.tasks, 2);
        assert_eq!(view.committed, 1);
        assert!(view.workers[0].lease.is_none(), "the commit freed worker 0");
        assert!(
            view.log.iter().any(|line| line.contains("committed 1/2")),
            "the commit line reaches the log: {:?}",
            view.log
        );
    }

    #[test]
    fn stop_winds_down_and_a_later_finish_reports_interrupted() {
        let mut state = idle(1);
        state.handle(Msg::Key(KeyAction::Start));
        let _ = state.take_start();
        state.handle(Msg::Key(KeyAction::Stop));
        assert!(state.take_stop(), "stop is requested");
        assert_eq!(state.view().state, "winding down");

        state.handle(Msg::Finished(interrupted()));
        assert_eq!(state.view().state, "interrupted");
    }

    #[test]
    fn start_restarts_after_a_terminal_state_but_is_a_no_op_while_running() {
        let mut state = idle(1);
        state.handle(Msg::Key(KeyAction::Start));
        let _ = state.take_start();
        state.handle(Msg::Key(KeyAction::Start));
        assert!(!state.take_start(), "start while running does nothing");

        state.handle(Msg::Finished(finalized()));
        assert_eq!(state.view().state, "finalized");
        state.handle(Msg::Key(KeyAction::Start));
        assert!(state.take_start(), "start restarts a finished run");
        assert_eq!(state.view().state, "running");
    }

    #[test]
    fn quit_leaves_immediately_when_idle_and_the_exit_code_is_clean() {
        let mut state = idle(1);
        state.handle(Msg::Key(KeyAction::Quit));
        assert!(state.should_exit());
        assert_eq!(state.exit_code(), 0);
    }

    #[test]
    fn quit_while_running_stops_and_leaves_once_the_run_returns() {
        let mut state = idle(1);
        state.handle(Msg::Key(KeyAction::Start));
        let _ = state.take_start();
        state.handle(Msg::Key(KeyAction::Quit));
        assert!(state.take_stop(), "quit while running stops");
        assert_eq!(state.view().state, "winding down");
        assert!(!state.should_exit(), "it waits for the run to return");

        state.handle(Msg::Finished(interrupted()));
        assert!(state.should_exit(), "the return arms the exit");
        assert_eq!(state.exit_code(), 130);
    }

    #[test]
    fn force_quit_leaves_at_once_and_reports_interrupted_mid_run() {
        let mut state = idle(1);
        state.handle(Msg::Key(KeyAction::Start));
        let _ = state.take_start();
        state.handle(Msg::Key(KeyAction::ForceQuit));
        assert!(state.should_exit());
        assert_eq!(state.exit_code(), 130, "a force quit mid-run exits 130");
    }

    #[test]
    fn the_exit_code_follows_the_last_terminal_outcome() {
        let mut failed = idle(1);
        failed.handle(Msg::Key(KeyAction::Start));
        let _ = failed.take_start();
        failed.handle(Msg::Finished(failed_outcome()));
        assert_eq!(failed.exit_code(), 2);

        let mut faulted = idle(1);
        faulted.handle(Msg::Key(KeyAction::Start));
        let _ = faulted.take_start();
        faulted.handle(Msg::Finished(Err(Error::Corruption(
            "store broke".to_string(),
        ))));
        assert_eq!(faulted.exit_code(), 1);

        let mut done = idle(1);
        done.handle(Msg::Key(KeyAction::Start));
        let _ = done.take_start();
        done.handle(Msg::Finished(finalized()));
        assert_eq!(done.exit_code(), 0);
    }
}
