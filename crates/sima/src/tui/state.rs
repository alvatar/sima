//! The tui state machine: [`TuiState`] handles a [`Msg`] and projects a
//! [`ViewModel`] the view draws.
//!
//! `TuiState` owns a [`RunStatus`] and applies every journal record to it,
//! so `sima status` and the tui update the same `RunStatus` type through the
//! same `apply` method and derive identical state from the same events. Only
//! UI concerns live here: which run the session is driving, whether a start or
//! stop is pending for the runtime to act on, whether an exit is armed, and
//! the session outcome that decides the exit code. No terminal types, no
//! channels, no I/O.

use std::collections::VecDeque;

use sima_core::Result;
use sima_pipeline::{Occupancy, Record, RunOutcome, RunState, RunStatus};

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
    /// Open the help overlay listing the key bindings.
    Help,
}

/// The observer session's view of the run's lock, set by the app loop after
/// each probe. Liveness comes from the lock: a held lock is a live foreign
/// orchestrator, a free one — while the journal still says in progress —
/// is a dead or finished orchestrator whose run is resumable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockView {
    /// Another process holds the run lock; the string is the holder line
    /// its locker recorded (pid, hostname).
    Held(String),
    /// The lock is free: no orchestrator drives the run.
    Free,
}

/// A message handled by the UI state: a journal record from the run, a key
/// press, or the run thread's terminal result.
#[derive(Debug)]
pub enum Msg {
    /// One journal record from the observer stream.
    Event(Record),
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
    /// The outcome a returned run result carries, for the header label.
    fn of(result: &Result<RunOutcome>) -> SessionOutcome {
        match result {
            Ok(RunOutcome::Finalized { .. }) => SessionOutcome::Finalized,
            Ok(RunOutcome::Failed { .. }) => SessionOutcome::Failed,
            Ok(RunOutcome::Interrupted { .. }) => SessionOutcome::Interrupted,
            Err(_) => SessionOutcome::Faulted,
        }
    }
}

/// The UI state: the run status plus the session's own concerns.
pub struct TuiState {
    /// The run's observable state, built from the observer stream through the
    /// same `apply` method `sima status` uses.
    status: RunStatus,
    /// The configured worker count, for one panel row per worker.
    workers: usize,
    /// What the session is doing.
    activity: Activity,
    /// The session outcome, for the header's terminal-state label.
    outcome: SessionOutcome,
    /// The exit code the session leaves with; 0 until a run — or a force
    /// quit mid-run — decides otherwise.
    exit_code: u8,
    /// A start is pending: the runtime should spawn the orchestrate thread.
    start_pending: bool,
    /// A stop is pending: the runtime should set the interrupt flag.
    stop_pending: bool,
    /// Once the active run returns, leave the loop.
    exit_on_finish: bool,
    /// Leave the loop now.
    exit: bool,
    /// Whether the help overlay is open, drawn over the frame.
    help_open: bool,
    /// The lock state of a run another process drives, while this session
    /// observes it; `None` in the drive session. Governs the header label,
    /// the meaning of `s`, and the refusal of `x`.
    observation: Option<LockView>,

    /// Whether this session may take a freed run over. A run is driven where
    /// its hardware is, so a session watching another host's run never can.
    takeover: bool,

    /// A transient status message — a refused key names the holder here.
    /// Cleared when the observed lock state changes.
    notice: Option<String>,
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
    /// The header state label: idle, running, winding down, a terminal
    /// state, or the observation line naming the run's holder.
    pub state: String,
    /// A transient status message shown beside the state, or `None`.
    pub notice: Option<String>,
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
    /// Whether the help overlay is drawn over the frame.
    pub help: bool,
}

impl TuiState {
    /// An idle session over `status` with `workers` configured workers. The
    /// status seeds the display — zeroed for a fresh store, or replayed from
    /// an existing journal — and the live stream applies to it from here.
    pub fn new(status: RunStatus, workers: usize) -> TuiState {
        TuiState {
            status,
            workers,
            activity: Activity::Idle,
            outcome: SessionOutcome::Clean,
            exit_code: 0,
            start_pending: false,
            stop_pending: false,
            exit_on_finish: false,
            exit: false,
            help_open: false,
            observation: None,
            takeover: true,
            notice: None,
            log: VecDeque::new(),
        }
    }

    /// Sets the observed lock state, entering or updating observer mode. The
    /// app loop calls this with the startup probe's holder and again on every
    /// probed change. A change clears any refusal notice, since the notice
    /// names the lock state it was issued under.
    pub fn observe(&mut self, lock: LockView) {
        self.notice = None;
        self.observation = Some(lock);
    }

    /// Marks the session unable to take the run over, for an observation of a
    /// run on another host: the take-over affordance is absent from the
    /// header, and `s` over a freed lock reports why instead of leaving.
    pub fn observe_only(&mut self) {
        self.takeover = false;
    }

    /// Handles one message.
    pub fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Event(record) => self.on_event(record),
            Msg::Key(action) => self.on_key(action),
            Msg::Finished(result) => self.on_finished(&result),
        }
    }

    /// Applies one journal record to the status and appends its line to the
    /// log, sharing the wording of `sima run` through [`describe`]. The
    /// commit count is read after the record is applied so a commit line
    /// shows the count that includes it.
    fn on_event(&mut self, record: Record) {
        self.status.apply(&record);
        if let Some(line) = describe(&record.event, self.status.committed, self.status.tasks) {
            if self.log.len() == LOG_CAPACITY {
                self.log.pop_front();
            }
            self.log.push_back(line);
        }
    }

    /// Applies a key action to the session's activity and pending requests.
    ///
    /// The help overlay is modal: while it is open the next key press closes
    /// it and does nothing else, so a bound key read behind the overlay neither
    /// starts, stops, nor leaves.
    fn on_key(&mut self, action: KeyAction) {
        if self.help_open {
            self.help_open = false;
            return;
        }
        match action {
            KeyAction::Start => match &self.observation {
                // There is no channel to control another process: while the
                // foreign orchestrator lives, `s` is refused with a notice.
                Some(LockView::Held(holder)) => {
                    self.notice = Some(format!("run held by {holder}"));
                }
                // The lock is free: the observer loop reads this request as
                // the take-over and leaves for the drive session — unless the
                // run is on another host, where this session cannot drive.
                Some(LockView::Free) if self.takeover => self.start_pending = true,
                Some(LockView::Free) => {
                    self.notice = Some("cannot take over a run on another host".to_string());
                }
                None => {
                    // A run starts from idle or after a terminal state; while
                    // one runs the key does nothing.
                    if matches!(self.activity, Activity::Idle | Activity::Ended) {
                        self.activity = Activity::Running;
                        self.start_pending = true;
                        self.exit_on_finish = false;
                    }
                }
            },
            KeyAction::Stop => match &self.observation {
                Some(LockView::Held(holder)) => {
                    self.notice = Some(format!("run held by {holder}"));
                }
                // No run of this session's is in flight: nothing to stop.
                Some(LockView::Free) => {}
                None => {
                    if matches!(self.activity, Activity::Running) {
                        self.activity = Activity::WindingDown;
                        self.stop_pending = true;
                    }
                }
            },
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
                    self.exit_code = crate::EXIT_INTERRUPTED;
                }
                self.exit = true;
            }
            KeyAction::Help => self.help_open = true,
        }
    }

    /// Handles the run thread's return: the session moves to its terminal
    /// state, records the outcome and the exit code it maps to — through the
    /// mapping `sima run` shares — and leaves if an exit was armed.
    fn on_finished(&mut self, result: &Result<RunOutcome>) {
        self.activity = Activity::Ended;
        self.outcome = SessionOutcome::of(result);
        self.exit_code = match result {
            Ok(outcome) => crate::outcome_exit_code(outcome),
            Err(_) => crate::EXIT_ERROR,
        };
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

    /// Closes the help overlay if it is open, reporting whether it did. The
    /// runtime calls this for a key with no binding: while the overlay is open
    /// any key press dismisses it, and a bound key already dismisses it in
    /// [`on_key`](TuiState::on_key).
    pub fn dismiss_help_if_open(&mut self) -> bool {
        std::mem::take(&mut self.help_open)
    }

    /// Whether the loop should leave.
    pub fn should_exit(&self) -> bool {
        self.exit
    }

    /// The session's exit code. An observer session derives it from the
    /// journal's run state — the drive-session mapping over what was
    /// observed, with a run still in progress leaving clean — so quitting
    /// after a watched run ended reports that ending, exactly as the drive
    /// session that produced it would.
    pub fn exit_code(&self) -> u8 {
        if self.observation.is_some() {
            return match self.status.state {
                RunState::InProgress => 0,
                RunState::Finalized => 0,
                RunState::Failed { .. } => crate::EXIT_FAILED,
                RunState::Interrupted => crate::EXIT_INTERRUPTED,
            };
        }
        self.exit_code
    }

    /// The header state label. The drive session labels its own activity; an
    /// observer session follows the journal's run state — a terminal event
    /// ends the observation in the drive session's presentation — and, while
    /// the run is in progress, the probed lock: held names the holder, free
    /// means the orchestrator died without a terminal line and the run is
    /// resumable.
    fn state_label(&self) -> String {
        if let Some(lock) = &self.observation {
            return match (&self.status.state, lock) {
                (RunState::Finalized, _) => "finalized".to_string(),
                (RunState::Failed { .. }, _) => "failed".to_string(),
                (RunState::Interrupted, _) => "interrupted".to_string(),
                (RunState::InProgress, LockView::Held(holder)) => {
                    format!("observing — run held by {holder}")
                }
                (RunState::InProgress, LockView::Free) if self.takeover => {
                    "orchestrator gone — run resumable; press s to continue it".to_string()
                }
                (RunState::InProgress, LockView::Free) => {
                    "orchestrator gone — run resumable".to_string()
                }
            };
        }
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
        .to_string()
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
            notice: self.notice.clone(),
            workers,
            committed: self.status.committed,
            tasks: self.status.tasks,
            retried: self.status.retried,
            rejected: self.status.rejected,
            faulted: self.status.faulted,
            lease_expired: self.status.lease_expired,
            log: self.log.iter().cloned().collect(),
            help: self.help_open,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::{Error, hash_bytes};
    use sima_model::{RunId, TaskKey};
    use sima_pipeline::Event;

    fn run_id() -> RunId {
        RunId::from_hash(hash_bytes(b"tui state run"))
    }

    fn idle(workers: usize) -> TuiState {
        TuiState::new(RunStatus::new(run_id()), workers)
    }

    /// Wraps an event as a record the tests feed the state. The stamp is
    /// irrelevant here, so every record carries the same one.
    fn rec(event: Event) -> Record {
        Record { ts_ms: 0, event }
    }

    fn started(tasks: usize) -> Record {
        rec(Event::RunStarted {
            run: "00".repeat(32),
            tasks,
            committed: 0,
        })
    }

    fn leased(task: &str, worker: u64, attempt: u32) -> Record {
        rec(Event::Leased {
            task: task.to_string(),
            worker,
            attempt,
        })
    }

    fn committed(task: &str) -> Record {
        rec(Event::Committed {
            task: task.to_string(),
            record: "11".repeat(32),
            stats_hex: String::new(),
        })
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
    fn force_quit_while_idle_leaves_clean() {
        let mut state = idle(1);
        state.handle(Msg::Key(KeyAction::ForceQuit));
        assert!(state.should_exit());
        assert_eq!(
            state.exit_code(),
            0,
            "a force quit with no run in flight is clean"
        );
    }

    #[test]
    fn quit_while_winding_down_arms_the_exit_without_a_second_stop() {
        let mut state = idle(1);
        state.handle(Msg::Key(KeyAction::Start));
        let _ = state.take_start();
        state.handle(Msg::Key(KeyAction::Stop));
        assert!(state.take_stop(), "the graceful stop is requested once");
        state.handle(Msg::Key(KeyAction::Quit));
        assert!(
            !state.take_stop(),
            "quit while winding down does not request a second stop"
        );
        assert!(!state.should_exit(), "it waits for the run to return");

        state.handle(Msg::Finished(interrupted()));
        assert!(state.should_exit(), "the return arms the exit");
        assert_eq!(state.exit_code(), 130);
    }

    #[test]
    fn help_opens_and_a_key_closes_it_without_acting_then_reopens() {
        let mut state = idle(1);
        assert!(!state.view().help, "help starts closed");

        state.handle(Msg::Key(KeyAction::Help));
        assert!(state.view().help, "'?' opens the help overlay");

        // A bound key while the overlay is open closes it and does nothing.
        state.handle(Msg::Key(KeyAction::Start));
        assert!(!state.view().help, "a key press closes the overlay");
        assert!(!state.take_start(), "the dismissing key starts no run");
        assert_eq!(state.view().state, "idle");

        state.handle(Msg::Key(KeyAction::Help));
        assert!(state.view().help, "'?' reopens the overlay");
    }

    #[test]
    fn a_key_read_behind_help_neither_stops_nor_quits() {
        let mut state = idle(1);
        state.handle(Msg::Key(KeyAction::Start));
        let _ = state.take_start();
        state.handle(Msg::Key(KeyAction::Help));
        assert!(state.view().help);

        // Quit behind the overlay closes it, requesting neither a stop nor exit.
        state.handle(Msg::Key(KeyAction::Quit));
        assert!(!state.view().help, "the press closed the overlay");
        assert!(!state.take_stop(), "quit behind help requests no stop");
        assert!(!state.should_exit(), "quit behind help does not leave");
        assert_eq!(state.view().state, "running");
    }

    #[test]
    fn an_unbound_key_dismisses_an_open_overlay_only() {
        let mut state = idle(1);
        assert!(
            !state.dismiss_help_if_open(),
            "nothing to dismiss when closed"
        );

        state.handle(Msg::Key(KeyAction::Help));
        assert!(
            state.dismiss_help_if_open(),
            "the open overlay is dismissed"
        );
        assert!(!state.view().help, "the overlay is now closed");
        assert!(!state.dismiss_help_if_open(), "a second dismiss is a no-op");
    }

    /// A state observing a run another orchestrator holds, seeded zeroed as
    /// the observer session starts.
    fn observing(workers: usize) -> TuiState {
        let mut state = idle(workers);
        state.observe(LockView::Held("4242 elsewhere".to_string()));
        state
    }

    fn run_finalized() -> Record {
        rec(Event::RunFinalized {
            run: "00".repeat(32),
            committed: 1,
        })
    }

    fn run_interrupted() -> Record {
        rec(Event::RunInterrupted {
            run: "00".repeat(32),
        })
    }

    #[test]
    fn observer_counters_match_a_drive_session_over_the_same_events() {
        // One display path: the observer feeds the same apply the drive
        // session uses, so identical events yield identical counters.
        let mut observer = observing(2);
        let mut driver = idle(2);
        let events = [started(2), leased("aa", 0, 0), committed("aa")];
        for event in &events {
            observer.handle(Msg::Event(event.clone()));
            driver.handle(Msg::Event(event.clone()));
        }
        let (o, d) = (observer.view(), driver.view());
        assert_eq!(o.tasks, d.tasks);
        assert_eq!(o.committed, d.committed);
        assert_eq!(o.workers, d.workers);
        assert_eq!(o.log, d.log);
    }

    #[test]
    fn the_observer_header_names_the_holder() {
        let state = observing(1);
        assert_eq!(state.view().state, "observing — run held by 4242 elsewhere");
    }

    #[test]
    fn start_and_stop_while_held_set_the_notice_and_request_nothing() {
        let mut state = observing(1);
        state.handle(Msg::Key(KeyAction::Start));
        assert!(!state.take_start(), "another process holds the run");
        let notice = state.view().notice.expect("a refusal notice");
        assert!(
            notice.contains("4242 elsewhere"),
            "the notice names the holder: {notice}"
        );
        state.handle(Msg::Key(KeyAction::Stop));
        assert!(!state.take_stop(), "there is no run of ours to stop");
        assert!(state.view().notice.is_some());
    }

    #[test]
    fn a_free_lock_without_a_terminal_event_reads_resumable() {
        let mut state = observing(1);
        state.handle(Msg::Event(started(2)));
        state.observe(LockView::Free);
        assert_eq!(
            state.view().state,
            "orchestrator gone — run resumable; press s to continue it"
        );
    }

    #[test]
    fn the_lock_freeing_clears_a_stale_refusal_notice() {
        let mut state = observing(1);
        state.handle(Msg::Key(KeyAction::Start));
        assert!(state.view().notice.is_some());
        state.observe(LockView::Free);
        assert_eq!(state.view().notice, None);
    }

    #[test]
    fn start_once_the_lock_is_free_requests_the_take_over() {
        let mut state = observing(1);
        state.observe(LockView::Free);
        state.handle(Msg::Key(KeyAction::Start));
        assert!(state.take_start(), "s on a free lock leaves for the drive");
    }

    #[test]
    fn an_observe_only_session_neither_offers_nor_performs_a_take_over() {
        // A run is driven where its hardware is, so a session watching
        // another host's run never takes it over: the affordance is absent
        // from the header and `s` over a freed lock says why.
        let mut state = observing(1);
        state.observe_only();
        state.handle(Msg::Event(started(2)));
        state.observe(LockView::Free);
        assert_eq!(state.view().state, "orchestrator gone — run resumable");

        state.handle(Msg::Key(KeyAction::Start));
        assert!(!state.take_start(), "s takes over nothing from here");
        assert!(
            state
                .view()
                .notice
                .is_some_and(|notice| notice.contains("another host")),
            "the refusal names why: {:?}",
            state.view().notice
        );
    }

    #[test]
    fn a_terminal_event_ends_observation_in_the_drive_presentation() {
        let mut finalized = observing(1);
        finalized.handle(Msg::Event(started(1)));
        finalized.handle(Msg::Event(run_finalized()));
        assert_eq!(finalized.view().state, "finalized");
        assert_eq!(finalized.exit_code(), 0);

        let mut failed = observing(1);
        failed.handle(Msg::Event(started(1)));
        failed.handle(Msg::Event(rec(Event::RunFailed {
            run: "00".repeat(32),
            task: "aa".to_string(),
            reason: "rejected".to_string(),
        })));
        assert_eq!(failed.view().state, "failed");
        assert_eq!(failed.exit_code(), 2);

        let mut interrupted = observing(1);
        interrupted.handle(Msg::Event(started(1)));
        interrupted.handle(Msg::Event(run_interrupted()));
        assert_eq!(interrupted.view().state, "interrupted");
        assert_eq!(interrupted.exit_code(), 130);
    }

    #[test]
    fn a_resume_segment_in_the_replay_returns_the_header_to_observing() {
        // A replayed history may end an old segment and start a new one; the
        // header follows the journal's last run-level event, so the seed of a
        // resumed run reads as in progress, never as its old interruption.
        let mut state = observing(1);
        state.handle(Msg::Event(started(2)));
        state.handle(Msg::Event(run_interrupted()));
        state.handle(Msg::Event(started(2)));
        assert_eq!(state.view().state, "observing — run held by 4242 elsewhere");
        assert_eq!(state.exit_code(), 0, "an in-progress observation is clean");
    }

    #[test]
    fn quitting_a_live_observation_is_clean() {
        let mut state = observing(1);
        state.handle(Msg::Event(started(2)));
        state.handle(Msg::Key(KeyAction::Quit));
        assert!(state.should_exit(), "quit leaves the observer at once");
        assert_eq!(state.exit_code(), 0);
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
