//! [`FarRun`]: the run on the destination, and what a verb does to it once it
//! is done watching.
//!
//! Two verbs reach a machine that is already hosting a run. `sima migrate`
//! reaches it after the follow it drove; `sima recall` reaches it over a far
//! run it never followed. What either does from there is the same four steps —
//! end the far run, pull what it produced, settle the run over the store that
//! came home, and dispose of the machine — so they live here rather than in
//! either verb.

use std::thread::sleep;
use std::time::{Duration, Instant};

use sima_core::{Error, Result};
use sima_model::RunId;
use sima_provider::{InstanceGuard, Provider};
use sima_scheduler::{Event, Level};
use sima_store::{ObjectScope, Store};
use sima_trace::Emitter;

#[cfg(not(test))]
use crate::config::{DEFAULT_READY_POLL_MS, DEFAULT_READY_TIMEOUT_MS};
use crate::config::{HostForm, LoadedConfig};
use crate::migrate::destination::Destination;
use crate::migrate::far_side::FarSide;
use crate::status::{RunState, status_records};
use crate::task_keys::task_keys;

/// What a migration came home with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrateOutcome {
    /// Every task committed and the local manifest was written.
    Finalized { run: RunId },
    /// The far run ended and the results came home, with tasks still to run.
    /// The run is resumable, here or on another migration.
    Outstanding { run: RunId, remaining: usize },
    /// A task failed definitively on the far side; no manifest was written.
    Failed { task: String, reason: String },
    /// The migration was wound down — out of budget, or asked to end by a
    /// recall. The results were pulled and any rental destroyed, so the run is
    /// resumable.
    Interrupted { run: RunId, remaining: usize },
    /// This side let go: the far run keeps computing on `machine`, nothing was
    /// pulled, and a rental is left standing. The run comes home on the next
    /// migration that sees it end, or on a recall.
    Detached { run: RunId, machine: String },
    /// The operator let go while the run was still being placed, so no far run
    /// was started: nothing computes on `machine`, a rental taken for it was
    /// released, and the run is exactly as it was.
    Abandoned { run: RunId, machine: String },
}

/// What ended the follow, which decides what happens to the far run after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FollowEnd {
    /// The far run ended on its own: a terminal event, or a process that is
    /// gone. Nothing is signalled; the results come home.
    FarRun,
    /// This side ended it, because the run's budget ran out. It is signalled,
    /// waited for, and its results come home.
    WoundDown,
    /// This side let go, on the operator's interrupt. The far run is left
    /// alone.
    Detached,
}

/// The test-only overrides a far run reads instead of the production bounds,
/// in one struct rather than scattered across its fields.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct Overrides {
    /// What a destination stating no readiness bounds falls back on. The suite
    /// takes the same tolerance production takes — the attempts and what each
    /// records are what its tests fix — at a poll that costs no wall clock.
    pub(crate) stated_nowhere: (Duration, Duration),
    /// How long the follow waits for the far run's first journal line. The
    /// production bound covers a process start; what the suite fixes is which
    /// failure a wait that ran out reports, so it runs the same path in a
    /// fraction of the time.
    pub(crate) attach_bound: Duration,
    /// How long the follow waits before polling again. What the suite fixes is
    /// the order of what it asks for, not the rate.
    pub(crate) tick: Duration,
}

#[cfg(test)]
impl Default for Overrides {
    fn default() -> Overrides {
        Overrides {
            stated_nowhere: (Duration::from_millis(200), Duration::from_millis(1)),
            attach_bound: Duration::from_millis(500),
            tick: Duration::from_millis(1),
        }
    }
}

/// The run on the destination, and everything this side does to end it and
/// bring it home: steps 9 through 12 over a machine already in hand.
///
/// Both verbs work through it. [`migrate`] reaches it after the follow it
/// drove; [`recall`](crate::migrate::recall::recall) reaches it over a far run
/// it never followed. The four steps are the same either way, which is why they
/// live here rather than in either verb.
pub(crate) struct FarRun<'a> {
    pub(crate) far: &'a dyn FarSide,
    pub(crate) store: &'a Store,
    pub(crate) config: &'a LoadedConfig,
    pub(crate) destination: &'a Destination<'a>,
    /// The run's journal boundary, opened by the verb around everything it
    /// does, so a phase this side narrates and a record the far side produced
    /// cross the same one.
    pub(crate) events: &'a Emitter,
    /// The rented machine hosting the run, disposed of on every path out;
    /// `None` for a machine of yours, which is nothing to tear down.
    pub(crate) rental: Option<InstanceGuard<'a, dyn Provider + Sync + 'a>>,
    #[cfg(test)]
    pub(crate) overrides: Overrides,
}

/// The one answer a verb comes home with, from what it did and what became of
/// the machine it did it on.
///
/// The verb's own failure is the cause; the ledger record a failed teardown
/// leaves is what the next reconciliation pass acts on.
pub(crate) fn merged(
    outcome: Result<MigrateOutcome>,
    teardown: Result<()>,
) -> Result<MigrateOutcome> {
    match (outcome, teardown) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

impl<'a> FarRun<'a> {
    /// Runs `body` over this far run and disposes of its machine afterwards, on
    /// every path out — including the ones no call site can reach.
    pub(crate) fn under_teardown(
        mut self,
        body: impl FnOnce(&FarRun<'a>) -> Result<MigrateOutcome>,
    ) -> Result<MigrateOutcome> {
        let outcome = body(&self);
        let teardown = self.dispose(&outcome);
        merged(outcome, teardown)
    }

    /// Step 12: what becomes of the rented machine, decided by what the verb
    /// came home with.
    ///
    /// A guard left alive is a machine still being paid for, so every path that
    /// ended the far run destroys it. A detached migration is the one path that
    /// does not: the run is still computing there, so the machine is kept and
    /// its ledger record left standing for the next invocation to adopt. An
    /// abandoned placement started nothing, so its machine is released like any
    /// other that computes nothing. Nothing here applies to a machine of yours,
    /// which was never rented.
    fn dispose(&mut self, outcome: &Result<MigrateOutcome>) -> Result<()> {
        let Some(guard) = self.rental.take() else {
            return Ok(());
        };
        if matches!(outcome, Ok(MigrateOutcome::Detached { .. })) {
            guard.keep();
            return Ok(());
        }
        guard.release()
    }

    /// Steps 9 through 11 over a far run this side never followed: end it if it
    /// is still going, read what it ended as, bring its results home, and
    /// settle the run over the store they extended.
    ///
    /// Nothing here starts anything, and the journal read is a read. A
    /// destination that was never migrated to is refused before any of it,
    /// since there is no run there to end and no store to pull from.
    pub(crate) fn wind_back(&self) -> Result<MigrateOutcome> {
        if !self.far.placed()? {
            return Err(Error::Validation(format!(
                "{:?} holds no directory for run {}: nothing was ever migrated there, so there \
                 is nothing to recall. `sima migrate` is what puts a run on a machine.",
                self.destination.name,
                self.config.run.id()
            )));
        }
        let end = match self.far.driving()? {
            // A run still going is ended the way an attached migration ends
            // one: signalled on every poll, waited for, terminated past the
            // bound.
            Some(pid) => {
                self.wind_down(pid, true, self.events)?;
                FollowEnd::WoundDown
            }
            // One that already ended is only collected from.
            None => FollowEnd::FarRun,
        };
        // What the far run ended as, read once the far side is quiet, so what
        // it holds is final. Nothing was followed, so this read is the only
        // way a definitive failure — written in the far journal, which does
        // not sync — reaches this side at all.
        let state = self.far_state()?;
        self.pull()?;
        self.settle(state, end)
    }

    /// The state the far run's own journal projects, over the records the far
    /// side serves in one read.
    ///
    /// A far store holding no journal projects what an empty one does — a run
    /// still in progress — so what the store holds after the pull is then the
    /// whole of what decides the outcome. A read that faulted is not that: the
    /// far side said nothing about how its run ended, and the failure names the
    /// machine and the read and carries the far side's own words.
    fn far_state(&self) -> Result<RunState> {
        let records = self.far.snapshot().map_err(|error| {
            Error::Validation(format!(
                "the journal of the run on {:?} could not be read: {error}",
                self.destination.name
            ))
        })?;
        Ok(status_records(self.config.run.id(), &records.unwrap_or_default()).state)
    }

    /// Step 10: everything the far side's records reference, which is what
    /// makes the store that comes home complete.
    ///
    /// The far run has ended by now on every path that reaches here — of its
    /// own accord, on the wind-down's signal, or on the termination the
    /// wind-down escalates to — so the far side's run lock is free for `sima
    /// sync-serve` to take.
    pub(crate) fn pull(&self) -> Result<()> {
        let keys = task_keys(self.config, self.store)?;
        self.far.sync(self.store, &keys, ObjectScope::Referenced)?;
        Ok(())
    }

    /// What a far run that is gone reports: the machine it was on, and the last
    /// words its log holds.
    ///
    /// Every far-side load failure looks the same from here — a program that
    /// cannot answer for its format, an install script that exited non-zero, a
    /// store that will not open — so the far run's own words are what tell them
    /// apart. `what` states what this side observed of the death.
    pub(crate) fn died(&self, what: &str) -> Error {
        let tail = match self.far.log_tail() {
            Ok(tail) if tail.trim().is_empty() => "(its log is empty)".to_string(),
            Ok(tail) => tail,
            Err(error) => format!("(its log could not be read: {error})"),
        };
        Error::Validation(format!(
            "the run on {:?} {what}. Its last words were:\n{tail}",
            self.destination.name
        ))
    }

    /// Step 9: asks the far run to wind down when this side ended the follow,
    /// then waits for it to exit.
    ///
    /// `sima sync-serve` takes the far side's run lock and the far `sima run`
    /// holds it while running, so the pull cannot proceed until the far run is
    /// gone. A wait that runs out is recorded and then escalated: the run is
    /// ended outright, and one that survives even that fails the migration by
    /// name. Either way the far run is gone before the pull in front of it.
    ///
    /// The signal is re-sent on every poll rather than once, because a far run
    /// is not signallable from the instant it starts. A shell starts an
    /// asynchronous command with `SIGINT` ignored and the disposition survives
    /// the exec, so the far run becomes signallable only once its own handler
    /// replaces the inherited ignore — which is after it has loaded its config.
    /// A wind-down that begins inside that window would otherwise signal into
    /// nothing and wait out the whole bound. Re-sending is idempotent against a
    /// run already winding down and costs one signal per poll interval.
    pub(crate) fn wind_down(&self, pid: u32, signal: bool, events: &Emitter) -> Result<()> {
        let (bound, poll) = self.ready_bounds();
        let deadline = Instant::now() + bound;
        loop {
            if signal {
                self.far.interrupt(pid)?;
            }
            if self.far.driving()?.is_none() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                events.emit(Event::Diagnostic {
                    level: Level::Warn,
                    source: "migrate".to_string(),
                    message: format!(
                        "the run on {:?} did not exit within {}ms of the wind-down; \
                         terminating it",
                        self.destination.name,
                        bound.as_millis()
                    ),
                    worker: None,
                    host: Some(self.destination.name.to_string()),
                    task: None,
                });
                // Ending it outright is safe by the same invariant crash
                // recovery rests on: a run that dies without winding down leaves
                // a resumable store. Abandoning it is what is not safe — an
                // owned destination has no rental to destroy, so the run would
                // keep computing, and the pull it is left in front of cannot
                // take a lock the survivor still holds.
                self.far.terminate(pid)?;
                // One interval for the far side to reap it, then the answer is
                // final: nothing here can end a process that survived that.
                sleep(poll);
                if self.far.driving()?.is_some() {
                    return Err(Error::Validation(format!(
                        "the run on {:?} is still there as pid {pid} after being terminated",
                        self.destination.name
                    )));
                }
                return Ok(());
            }
            sleep(poll);
        }
    }

    /// Step 11 over this far run's store and config.
    pub(crate) fn settle(&self, state: RunState, end: FollowEnd) -> Result<MigrateOutcome> {
        settle(self.store, self.config, state, end)
    }

    /// The destination's readiness bounds: how long to wait, and how often to
    /// look. A rented machine states its own; a machine of yours states none,
    /// so it takes the same defaults a rental would. The wind-down waits for
    /// the far run to exit under them, and the first contact spaces its
    /// attempts by the poll.
    pub(crate) fn ready_bounds(&self) -> (Duration, Duration) {
        match self.destination.form {
            HostForm::Rented(spec) => (spec.ready_timeout, spec.ready_poll),
            HostForm::Owned(_) => self.stated_nowhere_bounds(),
        }
    }

    /// The bounds a destination that states none falls back on. A machine of
    /// yours is the only such destination: the config admits the readiness keys
    /// on a rented entry alone.
    ///
    /// The suite reads them from a test override instead, so it spends no wall clock
    /// waiting out a tolerance whose attempts are what its tests fix. The
    /// values below are therefore exercised by the config's own tests rather
    /// than by anything here.
    #[cfg(not(test))]
    fn stated_nowhere_bounds(&self) -> (Duration, Duration) {
        (
            Duration::from_millis(DEFAULT_READY_TIMEOUT_MS),
            Duration::from_millis(DEFAULT_READY_POLL_MS),
        )
    }

    #[cfg(test)]
    fn stated_nowhere_bounds(&self) -> (Duration, Duration) {
        self.overrides.stated_nowhere
    }
}

/// Step 11: what the run comes home as, over the store a pull extended.
///
/// A definitive far-side failure is the outcome whatever the store holds — the
/// run cannot complete. Otherwise the key set is re-derived and every key
/// checked: a complete run finalizes, a wound-down one stays resumable, and one
/// whose far side ended early reports what is left.
///
/// It reads the local store and nothing else, so a verb with no machine left to
/// contact settles through it all the same.
pub(crate) fn settle(
    store: &Store,
    config: &LoadedConfig,
    state: RunState,
    end: FollowEnd,
) -> Result<MigrateOutcome> {
    if let RunState::Failed { task, reason } = state {
        return Ok(MigrateOutcome::Failed { task, reason });
    }
    let run = config.run.id();
    let keys = task_keys(config, store)?;
    let mut remaining = 0;
    for key in &keys {
        if !store.has_record(key)? {
            remaining += 1;
        }
    }
    if end == FollowEnd::WoundDown || matches!(state, RunState::Interrupted) {
        return Ok(MigrateOutcome::Interrupted { run, remaining });
    }
    if remaining > 0 {
        return Ok(MigrateOutcome::Outstanding { run, remaining });
    }
    store.finalize_run(&run, &keys)?;
    Ok(MigrateOutcome::Finalized { run })
}
