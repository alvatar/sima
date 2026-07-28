//! [`migrate`]: the choreography that moves a run's orchestrator onto another
//! machine and brings the results home.
//!
//! ```text
//!  ┌────────────────────────────────────────────────────────────────────────┐
//!  │  0  load config; require [orchestrator].migrate, a declared host       │
//!  │  1  open the local store; acquire the run lock, held to the end        │
//!  │  2  destination, by the form the named host takes:                     │
//!  │       yours    ──▶ that machine; no rental, no teardown                │
//!  │       rented   ──▶ adopt the rental already hosting this run, or       │
//!  │                      acquire one per the host entry                    │
//!  │  3  reach the destination: a machine of yours answers an image check,  │
//!  │       a rented one answers its enumeration probe, which also gives     │
//!  │       its device layout                                                │
//!  │  4  create the far-side directory; write the synthesized config        │
//!  │  5  is the far side already driving this run?                          │
//!  │       ├─ yes ──▶ skip to 7; this invocation is a reattach              │
//!  │       └─ no  ──▶ PUSH the run's closure, then                          │
//!  │  6  START: setsid the far `sima run`, capture its pid into run.pid     │
//!  │  7  FOLLOW: render each record and forward it into the local journal;  │
//!  │       poll the budget verdict when this is a rental                    │
//!  │  8  end on: a terminal run event | local interrupt | budget exhaustion │
//!  │  9  WIND DOWN: signal the far run, wait for it to exit (bounded)       │
//!  │ 10  PULL: everything the far side's records reference                  │
//!  │ 11  re-derive the key set; finalize when every key is committed,       │
//!  │       otherwise report the rest                                        │
//!  │ 12  TEARDOWN: release the guard (rental only)                          │
//!  └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! **The far run is detached.** It is started with `setsid` and its pid
//! recorded, so a laptop that sleeps, a network that drops, or a `sima migrate`
//! that is killed leaves the destination computing. Re-running reattaches: a
//! rented machine is found through the instance ledger, a machine of yours
//! through `run.pid`, and either way the push and the start are skipped.
//!
//! **Journals do not sync**, so each record the follow delivers is forwarded
//! into the local journal through the collector every other event crosses —
//! without it the local journal would hold a gap for every segment executed
//! remotely. A reattaching migration discards the history its first poll
//! replays and forwards only what arrives after it; the records it therefore
//! loses are diagnostic detail, since journals are observational and excluded
//! from every identity criterion.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::RunId;
use sima_provider::{
    AcquireLimits, Budget, InstanceGuard, Objective, Provider, Verdict, acquire, adopt, assess,
    now_ms,
};
use sima_store::{ObjectScope, Rental as RentalRole, RunLock, Store};
use sima_trace::{Collector, Emitter, Event, Level, Observer};

use crate::config::{FillPolicy, HostForm, LoadedConfig, Rented, load};
// The readiness defaults are what a destination stating none falls back on,
// which under test comes from the session's seams instead.
#[cfg(not(test))]
use crate::config::{DEFAULT_READY_POLL_MS, DEFAULT_READY_TIMEOUT_MS};
use crate::feed::RunFeed;
use crate::fleet::Rental;
use crate::migrate::destination::{Destination, destination_for};
use crate::migrate::far_config::{FarWorkers, far_config};
use crate::migrate::far_side::{Contact, FarSide, Remote};
use crate::migrate::objects::push_objects;
use crate::rental::{budget_exhausted, provider_for};
use crate::status::{RunState, RunStatus};
use crate::task_keys::task_keys;

/// How long the follow waits before polling again when nothing has arrived.
const TICK: Duration = Duration::from_millis(100);

/// How often a rental's budget is assessed while the follow runs. The ceiling
/// is the run's and moves only with the clock and the rental's rate, so
/// assessing it on every record poll would read the spend ledger ten times a
/// second for an answer that changes on the scale of minutes.
const BUDGET_INTERVAL: Duration = Duration::from_secs(10);

/// How long the follow waits for the far run to journal its first line before
/// reporting why it could not attach.
///
/// `sima follow-serve` refuses a run that has journaled nothing — the right
/// answer for a view of a run nobody drove — and the far `sima run` journals
/// only once it has loaded its config, opened its store, and taken its lock. The
/// bound therefore covers a process start rather than a machine coming up, and
/// a far run that has exited ends the wait at once whatever is left of it.
const ATTACH_BOUND: Duration = Duration::from_secs(30);

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
    /// The migration was wound down — locally interrupted, or out of budget.
    /// The results were pulled and any rental destroyed, so the run is
    /// resumable.
    Interrupted { run: RunId, remaining: usize },
}

/// Moves the run `config` describes onto the machine its `[orchestrator]`
/// names, follows it there, and brings the results home.
///
/// `observer` receives every record the far run produced, in journal order,
/// through the same collector that appends them locally. `interrupt` is the
/// level-triggered wind-down request `SIGINT` sets: once raised, the far run is
/// signalled, its results are pulled, and a rental is destroyed, leaving the run
/// resumable.
///
/// The local run lock is held for the whole call, so nothing else drives or
/// reconciles this run while it is away.
pub fn migrate(
    config: &Path,
    observer: Observer<'_>,
    interrupt: &AtomicBool,
) -> Result<MigrateOutcome> {
    // The file's own text is what travels: `[run]` is carried across as a
    // parsed value rather than re-derived, so no translation here can perturb
    // the run id.
    let local_text = std::fs::read_to_string(config)
        .map_err(|e| Error::Validation(format!("cannot read config {}: {e}", config.display())))?;
    let loaded = load(config)?;
    let destination = destination_for(&loaded)?;
    let store = Store::open(&loaded.store)?;
    // Registering the run is what gives it a journal to forward into, and it is
    // the same idempotent registration a local `sima run` performs.
    let run = store.create_run(&loaded.run)?;
    let lock = store.acquire_run_lock(&run)?;

    match destination.form {
        // A machine of yours is reached as it stands: nothing is rented, so no
        // provider is constructed and no credential is read.
        HostForm::Owned(owned) => {
            let far = Remote::owned(&destination, owned, &run);
            Session {
                far: &far,
                store: &store,
                config: &loaded,
                destination: &destination,
                local_text: &local_text,
                rental: None,
                observer,
                interrupt,
                #[cfg(test)]
                seams: Seams::default(),
            }
            .drive()
        }
        HostForm::Rented(spec) => {
            // One machine under strict fill: `migrate` names exactly one, so
            // there is no count and no shortfall to consider.
            let rental = Rental {
                name: destination.name,
                spec,
                count: 1,
                fill: FillPolicy::Strict,
            };
            let provider = provider_for(&rental)?;
            let guard = hold(
                provider.as_ref(),
                &store,
                &lock,
                spec,
                &loaded.budget,
                interrupt,
            )?;
            let far = Remote::rented(
                &destination,
                provider.as_ref(),
                guard.endpoint(),
                &run,
                &loaded.run.format,
            )?;
            Session {
                far: &far,
                store: &store,
                config: &loaded,
                destination: &destination,
                local_text: &local_text,
                rental: Some(guard),
                observer,
                interrupt,
                #[cfg(test)]
                seams: Seams::default(),
            }
            .drive()
        }
    }
}

/// The rented machine hosting this run: the one already hosting it, or a fresh
/// one under the host entry's specification.
///
/// Adoption comes first because a migration detaches the far side deliberately,
/// so a machine already working and already being paid for is the common case
/// on a second invocation. `interrupt` aborts an offer walk in flight, so a
/// `SIGINT` during acquisition is not waited out.
fn hold<'a>(
    provider: &'a (dyn Provider + Sync),
    store: &'a Store,
    lock: &RunLock,
    spec: &Rented,
    budget: &Budget,
    interrupt: &AtomicBool,
) -> Result<InstanceGuard<'a, dyn Provider + Sync + 'a>> {
    let limits = AcquireLimits {
        ready_timeout: spec.ready_timeout,
        ready_poll: spec.ready_poll,
    };
    if let Some(guard) = adopt(provider, store, lock, &limits)? {
        return Ok(guard);
    }
    acquire(
        provider,
        store,
        lock,
        RentalRole::Orchestrator,
        &spec.constraints,
        Objective::CheapestPerHour,
        &limits,
        budget,
        interrupt,
    )
}

/// One migration, past the destination's resolution: steps 3 through 12 over a
/// machine already in hand.
///
/// Split from [`migrate`] so the choreography is driven against a recording
/// [`FarSide`] with no machine at all, and so every path out of it passes back
/// through the teardown.
/// The session's test-only overrides, in one struct on the type rather than
/// scattered across its fields.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct Seams {
    /// What a destination stating no readiness bounds falls back on. The suite
    /// takes the same tolerance production takes — the attempts and what each
    /// records are what its tests fix — at a poll that costs no wall clock.
    stated_nowhere: (Duration, Duration),
}

#[cfg(test)]
impl Default for Seams {
    fn default() -> Seams {
        Seams {
            stated_nowhere: (Duration::from_millis(200), Duration::from_millis(1)),
        }
    }
}

struct Session<'a> {
    far: &'a dyn FarSide,
    store: &'a Store,
    config: &'a LoadedConfig,
    destination: &'a Destination<'a>,
    /// The local config's own file text, which is what travels.
    local_text: &'a str,
    /// The rented machine hosting the run, released on every path out; `None`
    /// for a machine of yours, which is nothing to tear down.
    rental: Option<InstanceGuard<'a, dyn Provider + Sync + 'a>>,
    observer: Observer<'a>,
    interrupt: &'a AtomicBool,
    #[cfg(test)]
    seams: Seams,
}

impl Session<'_> {
    /// The whole migration, with the teardown of step 12 on every path out.
    fn drive(mut self) -> Result<MigrateOutcome> {
        let rental = self.rental.take();
        let outcome = self.run_to_end();
        // A guard left alive is a machine still being paid for, so the teardown
        // runs whatever the migration did.
        let teardown = match rental {
            Some(guard) => guard.release(),
            None => Ok(()),
        };
        match (outcome, teardown) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            // The migration's own failure is the cause; the ledger record a
            // failed teardown leaves is what the next reconciliation pass acts
            // on.
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    /// Steps 3 through 11: reach the machine, place the run on it, push, start,
    /// follow, wind down, pull, and settle.
    fn run_to_end(&self) -> Result<MigrateOutcome> {
        let probed = self.reach()?;
        let far_text = far_config(
            self.local_text,
            FarWorkers::for_form(self.destination.form, &probed),
        )?;
        self.far.place(&far_text)?;

        // A far side already driving this run is a reattach: it holds the
        // closure it was sent and its own progress since, so pushing would send
        // what it already has, and starting would run a second orchestrator
        // against a store whose lock the first one holds.
        let reattached = self.far.driving()?;
        let pid = match reattached {
            Some(pid) => pid,
            None => {
                let keys = task_keys(self.config, self.store)?;
                let objects = push_objects(self.store, &keys)?;
                self.far
                    .sync(self.store, &keys, ObjectScope::Named(&objects))?;
                self.far.start()?
            }
        };

        let (state, wound_down) = self.watch(pid, reattached.is_some())?;

        // The pull takes everything the far side's records reference, so the
        // store that comes home is complete. The far run has ended by now on
        // every path that reaches here — of its own accord, on the wind-down's
        // signal, or on the termination the wind-down escalates to — so the far
        // side's run lock is free for `sima sync-serve` to take.
        let keys = task_keys(self.config, self.store)?;
        self.far.sync(self.store, &keys, ObjectScope::Referenced)?;
        self.settle(state, wound_down)
    }

    /// Steps 7 through 9: follow the far run to its end, then wind it down and
    /// wait for it to exit.
    ///
    /// Returns the state the far run's journal projects and whether this side
    /// wound it down. Both happen under the run's collector, so every record the
    /// follow delivers reaches the local journal and the operator's view through
    /// one seam, and the budget and timeout reports land in that same journal.
    fn watch(&self, pid: u32, reattached: bool) -> Result<(RunState, bool)> {
        let run = self.config.run.id();
        let writer = self.store.journal_writer(&run)?;
        thread::scope(|scope| -> Result<(RunState, bool)> {
            let collector = Collector::spawn(scope, writer, self.observer);
            let events = collector.emitter();
            let followed =
                self.follow(&run, reattached, &events)
                    .and_then(|(state, wound_down)| {
                        self.wind_down(pid, wound_down, &events)?;
                        Ok((state, wound_down))
                    });
            // The collector joins only once every emitter is dropped.
            drop(events);
            let journal = collector.shutdown();
            // A journal that could not be appended is a store fault worth
            // reporting, but only when the follow itself did not already fail.
            followed.and_then(|out| journal.map(|()| out))
        })
    }

    /// Follows the far run until it ends, this side is interrupted, or a
    /// rental's budget runs out, forwarding each record into the run's journal.
    fn follow(&self, run: &RunId, reattached: bool, events: &Emitter) -> Result<(RunState, bool)> {
        let budget = self.budget();
        let mut feed = self.attach()?;
        let mut status = RunStatus::new(*run);
        let mut replay = reattached;
        // Unset, so the first tick assesses: a migration re-run under a ceiling
        // already spent must not first watch for an interval.
        let mut assessed: Option<Instant> = None;
        loop {
            let records = feed.poll()?;
            for record in &records {
                status.apply(record);
                // The first poll of a reattached follow is the far run's whole
                // history, produced while nothing was attached to journal it.
                if !replay {
                    events.emit(record.event.clone());
                }
            }
            replay = false;
            if !matches!(status.state, RunState::InProgress) {
                return Ok((status.state, false));
            }
            if self.interrupt.load(Ordering::Relaxed) {
                return Ok((status.state, true));
            }
            if let Some(budget) = budget
                && assessed.is_none_or(|at| at.elapsed() >= BUDGET_INTERVAL)
            {
                assessed = Some(Instant::now());
                if let Verdict::Exhausted(exhaustion) = assess(self.store, run, budget, now_ms())? {
                    events.emit(budget_exhausted(exhaustion));
                    return Ok((status.state, true));
                }
            }
            if records.is_empty() {
                // A free lock is not yet an ended run: the far `sima run` takes
                // it only once it has loaded its config and opened its store,
                // and the follow can connect before that. The pid it was started
                // under answers without a race, so it is what decides — and it
                // is asked only on the rare tick where the lock reads free.
                if feed.holder()?.is_none() && self.far.driving()?.is_none() {
                    return Ok((status.state, false));
                }
                sleep(TICK);
            }
        }
    }

    /// Opens the follow, waiting out the window between the far run being
    /// started and its first journal line.
    ///
    /// A migration knows the run is coming up, because it started it, so it
    /// waits for that rather than reporting the refusal a view of an unjournaled
    /// run gets. The wait ends the moment the far run is gone: one that has
    /// exited will never journal, and its refusal is then the answer.
    fn attach(&self) -> Result<Box<dyn RunFeed>> {
        let deadline = Instant::now() + ATTACH_BOUND;
        loop {
            match self.far.follow() {
                Ok(feed) => return Ok(feed),
                Err(error) => {
                    if Instant::now() >= deadline || self.far.driving()?.is_none() {
                        return Err(error);
                    }
                    sleep(TICK);
                }
            }
        }
    }

    /// Step 3: the destination answers that it can drive this run, and a rented
    /// one answers with the devices its far-side workers will run on.
    ///
    /// This is the first contact with the machine, and it is retried under the
    /// destination's own readiness bounds: a provider reports an instance ready
    /// when its container is running, which is before the route to it carries
    /// an ssh, so a freshly rented host refuses the first connections, and a
    /// machine of yours can be rebooting. `ready_timeout` is what the entry
    /// describing the machine says about how long it may take to become usable,
    /// so it is what bounds the wait; a machine that answers at once costs
    /// nothing, since the loop ends on the first connection that lands.
    ///
    /// Only this step retries. Every later operation runs against a machine
    /// that has already answered, so a failure there states something real —
    /// the run's directory could not be written, the far side could not be
    /// started, a sync broke mid-session — and repeating it would hide that.
    fn reach(&self) -> Result<Vec<DeviceInfo>> {
        let (bound, poll) = self.ready_bounds();
        let deadline = Instant::now() + bound;
        loop {
            // A machine that answered has answered: what it said is the result,
            // whether that is its devices or a reason the run cannot proceed.
            // Only a machine that could not be reached is worth asking again.
            match self.far.devices()? {
                Contact::Answered(devices) => return Ok(devices),
                Contact::Unreachable(error) => {
                    if Instant::now() >= deadline {
                        return Err(error);
                    }
                }
            }
            sleep(poll);
        }
    }

    /// Step 9: asks the far run to wind down when this side ended the follow,
    /// then waits for it to exit.
    ///
    /// `sima sync-serve` takes the far side's run lock and the far `sima run`
    /// holds it while running, so the pull cannot proceed until the far run is
    /// gone. A wait that runs out is recorded and then escalated: the run is
    /// ended outright, and one that survives even that fails the migration by
    /// name. Either way the far run does not outlive the migration that started
    /// it.
    ///
    /// The signal is re-sent on every poll rather than once, because a far run
    /// is not signallable from the instant it starts. A shell starts an
    /// asynchronous command with `SIGINT` ignored and the disposition survives
    /// the exec, so the far run becomes signallable only once its own handler
    /// replaces the inherited ignore — which is after it has loaded its config.
    /// A wind-down that begins inside that window would otherwise signal into
    /// nothing and wait out the whole bound. Re-sending is idempotent against a
    /// run already winding down and costs one signal per poll interval.
    fn wind_down(&self, pid: u32, signal: bool, events: &Emitter) -> Result<()> {
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

    /// Step 11: what the run comes home as, over the store the pull extended.
    ///
    /// A definitive far-side failure is the outcome whatever the store holds —
    /// the run cannot complete. Otherwise the key set is re-derived and every
    /// key checked: a complete run finalizes, a wound-down one stays resumable,
    /// and one whose far side ended early reports what is left.
    fn settle(&self, state: RunState, wound_down: bool) -> Result<MigrateOutcome> {
        if let RunState::Failed { task, reason } = state {
            return Ok(MigrateOutcome::Failed { task, reason });
        }
        let run = self.config.run.id();
        let keys = task_keys(self.config, self.store)?;
        let mut remaining = 0;
        for key in &keys {
            if !self.store.has_record(key)? {
                remaining += 1;
            }
        }
        if wound_down || matches!(state, RunState::Interrupted) {
            return Ok(MigrateOutcome::Interrupted { run, remaining });
        }
        if remaining > 0 {
            return Ok(MigrateOutcome::Outstanding { run, remaining });
        }
        self.store.finalize_run(&run, &keys)?;
        Ok(MigrateOutcome::Finalized { run })
    }

    /// The ceiling this migration runs under, or `None` when nothing is being
    /// paid for. `[budget]` is the run's own, the same ceiling a fleet spends
    /// under; a machine of yours has no rate and so no ceiling to keep.
    fn budget(&self) -> Option<&Budget> {
        matches!(self.destination.form, HostForm::Rented(_)).then_some(&self.config.budget)
    }

    /// The destination's readiness bounds: how long to wait, and how often to
    /// look. A rented machine states its own; a machine of yours states none,
    /// so it takes the same defaults a rental would. The wind-down waits for
    /// the far run to exit under them, and the first contact spaces its
    /// attempts by the poll.
    fn ready_bounds(&self) -> (Duration, Duration) {
        match self.destination.form {
            HostForm::Rented(spec) => (spec.ready_timeout, spec.ready_poll),
            HostForm::Owned(_) => self.stated_nowhere_bounds(),
        }
    }

    /// The bounds a destination that states none falls back on. A machine of
    /// yours is the only such destination: the config admits the readiness keys
    /// on a rented entry alone.
    ///
    /// The suite reads them from a seam instead, so it spends no wall clock
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
        self.seams.stated_nowhere
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use sima_core::Result;
    use sima_domains::devices::{DeviceInfo, DeviceType};
    use sima_model::TaskKey;
    use sima_provider::stub::StubProvider;
    use sima_provider::{Constraints, InstanceStatus, Provision};
    use sima_scheduler::{Record, RunOutcome};
    use sima_store::{
        InstanceRecord, InstanceRecordState, Rental as RentalRole, SpendEntry, SyncReport,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::feed::{FeedInfo, RunFeed};
    use crate::fixtures::{drive_run, sync_between};

    /// The pid the scripted far side reports for a run it started.
    const PID: u32 = 4242;

    // ---- The recording far side ----

    /// One far-side operation, in the order it was asked for.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Step {
        Devices,
        Place,
        Driving,
        Start,
        /// A sync, under the object scope the session chose for it — which is
        /// the whole of what makes one direction a push and the other a pull.
        Sync(Scope),
        Follow,
        Interrupt(u32),
        Terminate(u32),
    }

    /// The object scope a sync was handed.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Scope {
        /// The identity components and each chain's frontier state: a push.
        Named,
        /// Everything the records reference: a pull.
        Referenced,
    }

    /// What ends a scripted far run, which is what the wind-down's escalation
    /// is measured against.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Ending {
        /// Winds down on the first signal it hears, as `sima run` does.
        OnInterrupt,
        /// Outlasts the wind-down and ends when it is terminated.
        OnTermination,
        /// Never exits, however it is asked to.
        Never,
    }

    /// The push, named where a step sequence reads it.
    const PUSH: Step = Step::Sync(Scope::Named);
    /// The pull.
    const PULL: Step = Step::Sync(Scope::Referenced);

    /// A far side that records what it was asked to do and answers from a
    /// script, so the choreography is driven with no machine at all.
    ///
    /// Its far run is alive from the start (or from a preset, for a reattach)
    /// until the feed delivers a terminal record or the wind-down signals it —
    /// which is what a real `sima run` does — unless it is stubborn, in which
    /// case it never exits and the wind-down's wait runs out on it.
    struct Scripted<'a> {
        devices: Vec<DeviceInfo>,
        /// The far-side run's pid while it is alive.
        alive: Arc<Mutex<Option<u32>>>,
        /// What ends this far run.
        ending: Ending,
        /// How many times the first contact is refused before the machine
        /// answers: a freshly rented host's sshd can lag its provider's
        /// `Ready`, and the connection is refused until it is up.
        refusals: Mutex<usize>,
        /// A machine that answers, without the worker image its pool needs —
        /// a failure no amount of waiting resolves.
        image_absent: bool,
        /// How many signals the far run discards before it winds down: a run
        /// that has not yet installed its own handler is not signallable, and
        /// the disposition it inherited discards what it is sent.
        deaf: Mutex<usize>,
        /// The records the follow feed delivers, one batch per poll.
        polls: Arc<Mutex<VecDeque<Vec<Record>>>>,
        /// The far side's own store and the run it holds, when a sync is to be
        /// performed for real rather than recorded and skipped.
        far: Option<(&'a Store, &'a LoadedConfig)>,
        steps: Mutex<Vec<Step>>,
        placed: Mutex<Option<String>>,
    }

    impl<'a> Scripted<'a> {
        /// A far side driving nothing, offering one card, with nothing to
        /// deliver.
        fn new() -> Scripted<'a> {
            Scripted {
                devices: vec![DeviceInfo {
                    vendor_id: 0x10de,
                    device_id: 0x2684,
                    name: "NVIDIA GeForce RTX 4090".to_string(),
                    device_type: DeviceType::Discrete,
                    member: 0,
                }],
                alive: Arc::new(Mutex::new(None)),
                ending: Ending::OnInterrupt,
                refusals: Mutex::new(0),
                image_absent: false,
                deaf: Mutex::new(0),
                polls: Arc::new(Mutex::new(VecDeque::new())),
                far: None,
                steps: Mutex::new(Vec::new()),
                placed: Mutex::new(None),
            }
        }

        /// A far side already driving this run, which is what a reattach finds.
        fn already_driving(self) -> Scripted<'a> {
            *self.alive.lock().expect("the pid lock") = Some(PID);
            self
        }

        /// A far run that never exits, however it is asked to.
        fn stubborn(mut self) -> Scripted<'a> {
            self.ending = Ending::Never;
            self
        }

        /// A far run that keeps going through the whole wind-down and ends only
        /// when it is terminated.
        fn outlasting_the_wind_down(mut self) -> Scripted<'a> {
            self.ending = Ending::OnTermination;
            self
        }

        /// A machine that answers but does not hold the worker image.
        fn without_the_image(mut self) -> Scripted<'a> {
            self.image_absent = true;
            self
        }

        /// A machine that refuses its first `contacts` connections before it
        /// answers at all.
        fn refusing(self, contacts: usize) -> Scripted<'a> {
            *self.refusals.lock().expect("the refusal lock") = contacts;
            self
        }

        /// A far run that discards its first `signals` before it becomes
        /// signallable.
        fn deaf_for(self, signals: usize) -> Scripted<'a> {
            *self.deaf.lock().expect("the deafness lock") = signals;
            self
        }

        /// The records the follow delivers, one batch per poll.
        fn delivering(self, batches: Vec<Vec<Record>>) -> Scripted<'a> {
            *self.polls.lock().expect("the poll lock") = batches.into();
            self
        }

        /// The store a sync actually exchanges with, and the config whose run it
        /// holds. Without it a sync is recorded and nothing moves.
        fn syncing_with(mut self, store: &'a Store, config: &'a LoadedConfig) -> Scripted<'a> {
            self.far = Some((store, config));
            self
        }

        fn record(&self, step: Step) {
            self.steps.lock().expect("the step lock").push(step);
        }

        fn steps(&self) -> Vec<Step> {
            self.steps.lock().expect("the step lock").clone()
        }
    }

    impl FarSide for Scripted<'_> {
        fn devices(&self) -> Result<Contact> {
            self.record(Step::Devices);
            let mut refusals = self.refusals.lock().expect("the refusal lock");
            if *refusals > 0 {
                *refusals -= 1;
                return Ok(Contact::Unreachable(Error::Validation(
                    "ssh: connect to host: Connection refused".to_string(),
                )));
            }
            if self.image_absent {
                return Err(Error::Validation(
                    "worker image \"sima:latest\" is not present on \"gpubox\"".to_string(),
                ));
            }
            Ok(Contact::Answered(self.devices.clone()))
        }

        fn place(&self, config: &str) -> Result<()> {
            self.record(Step::Place);
            *self.placed.lock().expect("the placement lock") = Some(config.to_string());
            Ok(())
        }

        fn driving(&self) -> Result<Option<u32>> {
            self.record(Step::Driving);
            Ok(*self.alive.lock().expect("the pid lock"))
        }

        fn start(&self) -> Result<u32> {
            self.record(Step::Start);
            *self.alive.lock().expect("the pid lock") = Some(PID);
            Ok(PID)
        }

        fn interrupt(&self, pid: u32) -> Result<()> {
            self.record(Step::Interrupt(pid));
            let mut deaf = self.deaf.lock().expect("the deafness lock");
            if *deaf > 0 {
                // Sent into the window before the run's own handler replaced
                // the disposition it inherited: discarded, with no trace.
                *deaf -= 1;
                return Ok(());
            }
            if self.ending == Ending::OnInterrupt {
                *self.alive.lock().expect("the pid lock") = None;
            }
            Ok(())
        }

        fn terminate(&self, pid: u32) -> Result<()> {
            self.record(Step::Terminate(pid));
            // A termination is not declinable, so only a far run scripted to
            // outlast everything survives it.
            if self.ending != Ending::Never {
                *self.alive.lock().expect("the pid lock") = None;
            }
            Ok(())
        }

        fn sync(
            &self,
            store: &Store,
            keys: &[TaskKey],
            scope: ObjectScope<'_>,
        ) -> Result<SyncReport> {
            self.record(match scope {
                ObjectScope::Named(_) => PUSH,
                ObjectScope::Referenced => PULL,
            });
            match self.far {
                // The far side derives its own key set over its own store, as
                // `sima sync-serve` does; no key list crosses the wire.
                Some((far, config)) => {
                    let far_keys = task_keys(config, far)?;
                    sync_between(store, keys, scope, far, &far_keys)
                }
                None => Ok(SyncReport::default()),
            }
        }

        fn follow(&self) -> Result<Box<dyn RunFeed>> {
            self.record(Step::Follow);
            Ok(Box::new(ScriptedFeed {
                info: FeedInfo {
                    run: RunId::from_hash(sima_core::hash_bytes(b"scripted")),
                    format: sima_model::FormatId::new("stub.v1").expect("format id"),
                    workers: 1,
                },
                polls: Arc::clone(&self.polls),
                alive: Arc::clone(&self.alive),
                ending: self.ending,
            }))
        }
    }

    /// The feed a scripted far side hands out: one batch per poll, and the far
    /// run ends when a terminal record is delivered.
    struct ScriptedFeed {
        info: FeedInfo,
        polls: Arc<Mutex<VecDeque<Vec<Record>>>>,
        alive: Arc<Mutex<Option<u32>>>,
        ending: Ending,
    }

    impl RunFeed for ScriptedFeed {
        fn info(&self) -> &FeedInfo {
            &self.info
        }

        fn poll(&mut self) -> Result<Vec<Record>> {
            let batch = self
                .polls
                .lock()
                .expect("the poll lock")
                .pop_front()
                .unwrap_or_default();
            let terminal = batch.iter().any(|record| {
                matches!(
                    record.event,
                    Event::RunFinalized { .. }
                        | Event::RunFailed { .. }
                        | Event::RunInterrupted { .. }
                )
            });
            if terminal && self.ending != Ending::Never {
                // A `sima run` that wrote its terminal event exits.
                *self.alive.lock().expect("the pid lock") = None;
            }
            Ok(batch)
        }

        fn holder(&self) -> Result<Option<String>> {
            // The far side holds its run lock for as long as its run does.
            Ok(self
                .alive
                .lock()
                .expect("the pid lock")
                .map(|pid| format!("{pid} far")))
        }
    }

    // ---- The local side ----

    /// The local side of a migration: the config file's text, its loaded form,
    /// and the store the run lives in.
    struct Local {
        _dir: TempDir,
        text: String,
        config: LoadedConfig,
        store: Store,
    }

    /// The run every session test moves: one candidate over twenty accumulating
    /// segments, so the chain has a frontier at every stage.
    fn config_text(machine: &str, root: &str, bounds: &str) -> String {
        format!(
            r#"
            [run]
            root_seed = 5
            segments = 20
            format = "stub.v1"

            [run.generator]
            id = "stub.v1"
            behaviors = ["accumulate:2"]

            [run.params]
            hex = "01"

            [config]
            store = "./store"
            max_attempts = 1

            [orchestrator]
            workers = 1
            migrate = "slingshot"

            [host.slingshot]
            root = {root:?}
            binary = "/bin/true"
            {machine}
            {bounds}

            [budget]
            max_spend_usd = 1.0
            "#
        )
    }

    /// A local side whose store holds `committed` segments of the run, over a
    /// destination of the given form.
    ///
    /// The run-global ceiling is a dollar, which nothing here reaches unless the
    /// test seeds spend against it.
    fn local(machine: &str, bounds: &str, committed: Option<usize>) -> Local {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("far");
        let text = config_text(machine, &root.to_string_lossy(), bounds);
        let path = dir.path().join("sima.toml");
        std::fs::write(&path, &text).expect("write the config");
        let config = crate::config::load(&path).expect("the config loads");
        let store = Store::open(&config.store).expect("open the store");
        if let Some(committed) = committed {
            assert!(matches!(
                drive_run(&store, &config.run, Some(committed)),
                RunOutcome::Interrupted { .. }
            ));
        }
        Local {
            _dir: dir,
            text,
            config,
            store,
        }
    }

    /// The declaration of a rented machine, with bounds that never wait.
    const RENTED: &str = "provider = \"stub\"";
    /// The declaration of a machine of yours.
    const OWNED: &str = "workers = 1";
    /// Readiness bounds a wind-down runs through without sleeping.
    const PROMPT: &str = "ready_timeout_ms = 200\nready_poll_ms = 1";

    /// A second store holding the same run, driven `committed` segments in —
    /// the far side of a migration, as a real sync finds it.
    fn far_store(config: &LoadedConfig, committed: Option<usize>) -> (TempDir, Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open the store");
        drive_run(&store, &config.run, committed);
        (dir, store)
    }

    /// Drives one session over `far`, capturing every record the follow
    /// forwarded.
    fn session_over(
        local: &Local,
        far: &dyn FarSide,
        rental: Option<InstanceGuard<'_, dyn Provider + Sync + '_>>,
        interrupt: &AtomicBool,
    ) -> (Result<MigrateOutcome>, Vec<Record>) {
        let captured: Mutex<Vec<Record>> = Mutex::new(Vec::new());
        let observer = |record: &Record| {
            captured
                .lock()
                .expect("the capture lock")
                .push(record.clone());
        };
        let destination = destination_for(&local.config).expect("the host is declared");
        let outcome = Session {
            far,
            store: &local.store,
            config: &local.config,
            destination: &destination,
            local_text: &local.text,
            rental,
            observer: &observer,
            interrupt,
            seams: Seams::default(),
        }
        .drive();
        let records = std::mem::take(&mut *captured.lock().expect("the capture lock"));
        (outcome, records)
    }

    // ---- Journal records the scripted far side delivers ----

    fn rec(event: Event) -> Record {
        Record { ts_ms: 0, event }
    }

    fn started(run: &RunId) -> Record {
        rec(Event::RunStarted {
            run: run.to_string(),
            tasks: 20,
            committed: 0,
        })
    }

    fn committed(task: &str) -> Record {
        rec(Event::Committed {
            task: task.to_string(),
            record: "11".repeat(32),
            stats: Vec::new(),
            stats_blob_hex: String::new(),
        })
    }

    fn finalized(run: &RunId) -> Record {
        rec(Event::RunFinalized {
            run: run.to_string(),
            committed: 20,
        })
    }

    fn failed(run: &RunId, task: &str, reason: &str) -> Record {
        rec(Event::RunFailed {
            run: run.to_string(),
            task: task.to_string(),
            reason: reason.to_string(),
        })
    }

    // ---- A rented machine, provisioned and adopted ----

    /// A stub marketplace of one generous offer.
    fn marketplace() -> StubProvider {
        StubProvider::new(vec![sima_provider::Offer {
            id: sima_provider::OfferId("offer-0".to_string()),
            machine: "machine-0".to_string(),
            gpu_model: "stub-gpu".to_string(),
            gpu_count: 1,
            vram_mb: 24_000,
            price: sima_provider::Price(100_000),
            reliability: 1.0,
            verified: true,
            disk_gb: 1_000,
            bandwidth_mbps: 10_000,
            location: String::new(),
        }])
    }

    /// Bounds that never wait.
    fn limits() -> AcquireLimits {
        AcquireLimits {
            ready_timeout: Duration::from_millis(500),
            ready_poll: Duration::ZERO,
        }
    }

    /// Rents one machine to host the run, as `hold` does when there is nothing
    /// to adopt.
    fn hosting<'a>(
        provider: &'a (dyn Provider + Sync),
        store: &'a Store,
        lock: &RunLock,
    ) -> Result<InstanceGuard<'a, dyn Provider + Sync + 'a>> {
        acquire(
            provider,
            store,
            lock,
            RentalRole::Orchestrator,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &limits(),
            &Budget::default(),
            &AtomicBool::new(false),
        )
    }

    // ---- The choreography ----

    #[test]
    fn the_far_side_is_asked_for_the_steps_of_the_choreography_in_order() -> Result<()> {
        // Reach the machine, place the run on it, ask whether it is already
        // going, push, start, follow, wait it out, pull.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new().delivering(vec![vec![started(&run), finalized(&run)]]);
        let interrupt = AtomicBool::new(false);
        let (outcome, _) = session_over(&local, &far, None, &interrupt);
        outcome?;
        assert_eq!(
            far.steps(),
            [
                Step::Devices,
                Step::Place,
                Step::Driving,
                PUSH,
                Step::Start,
                Step::Follow,
                // The wind-down finds the run already gone: it wrote its
                // terminal event and exited.
                Step::Driving,
                PULL,
            ]
        );
        Ok(())
    }

    #[test]
    fn a_machine_that_refuses_the_first_connection_is_reached_on_the_next() -> Result<()> {
        // A freshly rented machine reports ready before its sshd accepts, so
        // the first contact is refused. The migration is what was paid for; it
        // waits for the machine rather than failing in front of it.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new()
            .refusing(1)
            .delivering(vec![vec![started(&run), finalized(&run)]]);

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        outcome?;
        let steps = far.steps();
        assert_eq!(
            steps.iter().filter(|step| **step == Step::Devices).count(),
            2,
            "the refused contact was tried again: {steps:?}"
        );
        assert_eq!(
            steps.last(),
            Some(&PULL),
            "the whole choreography ran past it: {steps:?}"
        );
        assert_eq!(
            steps.iter().filter(|step| **step == Step::Place).count(),
            1,
            "nothing past the contact was repeated: {steps:?}"
        );
        Ok(())
    }

    #[test]
    fn a_machine_that_never_answers_fails_the_migration_within_the_bound() -> Result<()> {
        // The tolerance is for a machine coming up, not for one that is not
        // there: the wait ends at the destination's stated bound and the
        // refusal itself is the error. How many attempts fit in that bound is
        // the machine's business, so the count is not what this fixes.
        let local = local(RENTED, PROMPT, Some(3));
        let far = Scripted::new().refusing(usize::MAX);

        let started = Instant::now();
        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        let error = outcome.expect_err("a machine that never answers fails");
        assert!(
            error.to_string().contains("Connection refused"),
            "the refusal itself is reported: {error}"
        );
        let steps = far.steps();
        assert!(steps.len() > 1, "it was asked more than once: {steps:?}");
        assert!(
            steps.iter().all(|step| *step == Step::Devices),
            "nothing past the contact ran: {steps:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "it gave up at the stated bound: {:?}",
            started.elapsed()
        );
        Ok(())
    }

    #[test]
    fn a_machine_of_yours_that_never_answers_is_given_the_same_tolerance() -> Result<()> {
        // A destination of yours states no readiness bounds — the config
        // rejects rental keys on it — so it falls back on what states none,
        // and is waited for exactly as a destination that states its own is.
        // The suite spends none of that wall clock.
        let local = local(OWNED, "", Some(3));
        let far = Scripted::new().refusing(usize::MAX);

        let started = Instant::now();
        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        outcome.expect_err("a machine that never answers fails");
        let steps = far.steps();
        assert!(steps.len() > 1, "it was asked more than once: {steps:?}");
        assert!(
            steps.iter().all(|step| *step == Step::Devices),
            "nothing past the contact ran: {steps:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the tolerance costs the suite nothing: {:?}",
            started.elapsed()
        );
        Ok(())
    }

    #[test]
    fn a_machine_that_answers_late_within_its_stated_bound_is_still_reached() -> Result<()> {
        // The live failure in miniature: the machine does answer, later than a
        // fixed count of attempts allows. Its entry states how long it may take
        // to become usable, and the contact is bounded by that rather than by a
        // count — here a bound wide enough for far more attempts than the six a
        // count would have given.
        let local = local(
            RENTED,
            "ready_timeout_ms = 2000\nready_poll_ms = 1",
            Some(3),
        );
        let run = local.config.run.id();
        let far = Scripted::new()
            .refusing(40)
            .delivering(vec![vec![started(&run), finalized(&run)]]);

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        outcome?;
        let steps = far.steps();
        assert_eq!(
            steps.iter().filter(|step| **step == Step::Devices).count(),
            41,
            "every refusal inside the bound was tried again: {steps:?}"
        );
        assert_eq!(
            steps.last(),
            Some(&PULL),
            "the whole choreography ran past it: {steps:?}"
        );
        Ok(())
    }

    #[test]
    fn a_contact_that_fails_for_a_reason_waiting_cannot_fix_is_attempted_once() -> Result<()> {
        // The machine answered; what it said is that it does not hold the
        // worker image, and it will not come to hold one by being asked again.
        let local = local(OWNED, "", Some(3));
        let far = Scripted::new().without_the_image();

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        let error = outcome.expect_err("a machine without the image cannot drive the run");
        assert!(
            error.to_string().contains("image"),
            "the reason reaches the caller unchanged: {error}"
        );
        assert_eq!(
            far.steps(),
            vec![Step::Devices],
            "the tolerance is for a machine coming up, not for this"
        );
        Ok(())
    }

    #[test]
    fn the_config_the_far_side_is_given_is_the_same_run() -> Result<()> {
        // The whole move rests on it: a far side driving another run would
        // start the chain again from segment zero.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new().delivering(vec![vec![started(&run), finalized(&run)]]);
        session_over(&local, &far, None, &AtomicBool::new(false)).0?;
        let placed = far
            .placed
            .lock()
            .expect("the placement lock")
            .clone()
            .expect("a config was placed");
        assert_eq!(crate::fixtures::load_str(&placed).run.id(), run);
        Ok(())
    }

    #[test]
    fn a_raised_interrupt_signals_the_far_run_pulls_and_tears_the_rental_down() -> Result<()> {
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let lock = local.store.acquire_run_lock(&run)?;
        let provider = marketplace();
        let guard = hosting(&provider, &local.store, &lock)?;
        // Raised before the follow starts, which is what a `sima migrate`
        // interrupted early looks like.
        let interrupt = AtomicBool::new(true);
        let far = Scripted::new().delivering(vec![vec![started(&run), committed("aa")]]);

        let (outcome, _) = session_over(&local, &far, Some(guard), &interrupt);
        assert!(matches!(outcome?, MigrateOutcome::Interrupted { .. }));
        assert_eq!(
            far.steps(),
            [
                Step::Devices,
                Step::Place,
                Step::Driving,
                PUSH,
                Step::Start,
                Step::Follow,
                Step::Interrupt(PID),
                Step::Driving,
                PULL,
            ],
            "signal, wait, pull — in that order"
        );
        assert_eq!(provider.destroyed().len(), 1, "the rental is torn down");
        Ok(())
    }

    #[test]
    fn an_exhausted_budget_winds_the_far_run_down_and_reports_the_exhaustion() -> Result<()> {
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let lock = local.store.acquire_run_lock(&run)?;
        let provider = marketplace();
        let guard = hosting(&provider, &local.store, &lock)?;
        // A closed rental this run already paid for, well past the ceiling the
        // config declares.
        local.store.put_spend(&SpendEntry {
            tag: "sima-prior-0".to_string(),
            provider: "stub".to_string(),
            owner: run.to_string(),
            price_micro_usd_hour: 100_000,
            started_ms: 1_700_000_000_000,
            ended_ms: 1_700_000_003_600_000,
            cost_micro_usd: 2_000_000,
        })?;
        // The far run would go on to finalize; the ceiling is what stops it
        // first, so a migration that never assessed would come home finalized
        // instead of wound down.
        let far = Scripted::new().delivering(vec![
            vec![started(&run), committed("aa")],
            vec![finalized(&run)],
        ]);

        let (outcome, records) = session_over(&local, &far, Some(guard), &AtomicBool::new(false));
        assert!(matches!(outcome?, MigrateOutcome::Interrupted { .. }));
        assert!(
            far.steps().contains(&Step::Interrupt(PID)),
            "the far run is asked to wind down: {:?}",
            far.steps()
        );
        assert_eq!(far.steps().last(), Some(&PULL), "the results come home");
        assert!(
            records
                .iter()
                .any(|record| matches!(record.event, Event::BudgetSpendExhausted { .. })),
            "the exhaustion is reported: {records:?}"
        );
        assert_eq!(provider.destroyed().len(), 1);
        Ok(())
    }

    #[test]
    fn a_budget_within_its_ceiling_lets_the_follow_run_on() -> Result<()> {
        // The counterpart: the same rental under a ceiling it has not reached
        // is not wound down, so the assessment decides rather than its presence.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let lock = local.store.acquire_run_lock(&run)?;
        let provider = marketplace();
        let guard = hosting(&provider, &local.store, &lock)?;
        let far = Scripted::new().delivering(vec![vec![started(&run), finalized(&run)]]);

        let (outcome, records) = session_over(&local, &far, Some(guard), &AtomicBool::new(false));
        assert!(matches!(outcome?, MigrateOutcome::Outstanding { .. }));
        assert!(
            !far.steps().contains(&Step::Interrupt(PID)),
            "nothing wound the far run down"
        );
        assert!(
            !records.iter().any(|record| matches!(
                record.event,
                Event::BudgetSpendExhausted { .. } | Event::BudgetWallClockExhausted { .. }
            )),
            "no exhaustion is reported"
        );
        Ok(())
    }

    /// The one diagnostic a session reported, of which the wind-down's expiry
    /// is the only one these tests produce.
    fn reported(records: &[Record]) -> String {
        records
            .iter()
            .find_map(|record| match &record.event {
                Event::Diagnostic { message, .. } => Some(message.clone()),
                _ => None,
            })
            .expect("the timeout is reported")
    }

    #[test]
    fn a_far_run_that_outlasts_the_wind_down_is_terminated_and_the_pull_follows() -> Result<()> {
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new()
            .outlasting_the_wind_down()
            .delivering(vec![vec![started(&run), committed("aa")]]);

        let (outcome, records) = session_over(&local, &far, None, &AtomicBool::new(true));
        assert!(matches!(outcome?, MigrateOutcome::Interrupted { .. }));
        // The graceful path was tried and reported before anything harder was
        // reached for.
        let report = reported(&records);
        assert!(report.contains("slingshot"), "names the machine: {report}");
        assert!(report.contains("did not exit"), "{report}");

        let steps = far.steps();
        let terminated = steps
            .iter()
            .position(|step| *step == Step::Terminate(PID))
            .expect("the far run was terminated");
        let signalled = steps
            .iter()
            .position(|step| *step == Step::Interrupt(PID))
            .expect("the far run was signalled first");
        assert!(signalled < terminated, "signalled before terminated");
        assert_eq!(
            steps.last(),
            Some(&PULL),
            "the pull follows a far run that is really gone: {steps:?}"
        );
        Ok(())
    }

    #[test]
    fn a_far_run_that_survives_termination_fails_the_migration_naming_it() -> Result<()> {
        // Nothing ended it, so the far side's run lock is still held and the
        // pull cannot succeed. Failing here names the cause; reaching the sync
        // would report the lock instead.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new()
            .stubborn()
            .delivering(vec![vec![started(&run), committed("aa")]]);

        let (outcome, records) = session_over(&local, &far, None, &AtomicBool::new(true));
        let error = outcome.expect_err("a far run that will not end fails the migration");
        let text = error.to_string();
        assert!(text.contains("slingshot"), "names the machine: {text}");
        assert!(text.contains(&PID.to_string()), "names the pid: {text}");
        assert!(reported(&records).contains("did not exit"));
        assert!(
            !far.steps().contains(&PULL),
            "the pull is not attempted: {:?}",
            far.steps()
        );
        Ok(())
    }

    #[test]
    fn a_far_run_that_discards_its_first_signals_is_signalled_again() -> Result<()> {
        // A shell starts an asynchronous command with `SIGINT` ignored and the
        // disposition survives the exec, so a wind-down beginning before the far
        // run installed its own handler signals into nothing. Re-sending on
        // every poll is what closes that window.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new()
            .deaf_for(2)
            .delivering(vec![vec![started(&run), committed("aa")]]);

        let (outcome, records) = session_over(&local, &far, None, &AtomicBool::new(true));
        assert!(matches!(outcome?, MigrateOutcome::Interrupted { .. }));
        assert_eq!(
            far.steps()
                .iter()
                .filter(|step| **step == Step::Interrupt(PID))
                .count(),
            3,
            "two discarded, the third heard: {:?}",
            far.steps()
        );
        // The run went away well inside the bound, so nothing was reported.
        assert!(
            !records
                .iter()
                .any(|record| matches!(record.event, Event::Diagnostic { .. })),
            "no timeout was reported: {records:?}"
        );
        Ok(())
    }

    // ---- What the migration comes home as ----

    #[test]
    fn a_migration_that_brought_every_task_home_finalizes() -> Result<()> {
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        assert!(
            local.store.manifest(&run)?.is_none(),
            "the local run is unfinished before the migration"
        );
        // The far side ran the chain out.
        let (_far_dir, far) = far_store(&local.config, None);
        let scripted = Scripted::new()
            .syncing_with(&far, &local.config)
            .delivering(vec![vec![started(&run), finalized(&run)]]);

        let (outcome, _) = session_over(&local, &scripted, None, &AtomicBool::new(false));
        assert_eq!(outcome?, MigrateOutcome::Finalized { run });
        assert!(
            local.store.manifest(&run)?.is_some(),
            "the manifest is written here, over the store the pull completed"
        );
        Ok(())
    }

    #[test]
    fn a_migration_with_tasks_outstanding_reports_the_count_and_writes_no_manifest() -> Result<()> {
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        // The far side got further, but not to the end.
        let (_far_dir, far) = far_store(&local.config, Some(8));
        let scripted = Scripted::new()
            .syncing_with(&far, &local.config)
            .delivering(vec![vec![started(&run), finalized(&run)]]);

        let (outcome, _) = session_over(&local, &scripted, None, &AtomicBool::new(false));
        // The chain is traversable forward only, so an unfinished chain always
        // leaves exactly its next segment underived-from: one key uncommitted.
        assert_eq!(outcome?, MigrateOutcome::Outstanding { run, remaining: 1 });
        assert!(local.store.manifest(&run)?.is_none(), "nothing was sealed");
        // The pull did move the far side's progress home.
        assert!(
            task_keys(&local.config, &local.store)?.len() > 4,
            "the local store gained the far side's segments"
        );
        Ok(())
    }

    #[test]
    fn a_definitive_far_side_failure_is_the_outcome() -> Result<()> {
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new().delivering(vec![vec![
            started(&run),
            failed(&run, "aa", "the candidate diverged"),
        ]]);

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        assert_eq!(
            outcome?,
            MigrateOutcome::Failed {
                task: "aa".to_string(),
                reason: "the candidate diverged".to_string(),
            }
        );
        assert!(local.store.manifest(&run)?.is_none());
        assert_eq!(
            far.steps().last(),
            Some(&PULL),
            "a failed run's results still come home"
        );
        Ok(())
    }

    // ---- Teardown ----

    #[test]
    fn every_path_out_of_a_migration_tears_the_rental_down() -> Result<()> {
        // The end state that matters: no path leaves a machine running and
        // being paid for. The session releases the guard explicitly so a
        // teardown failure is reported rather than swallowed; the guard's own
        // drop is the second line of the same guarantee.
        let run_to = |far: &dyn FarSide, interrupt: bool| -> Result<usize> {
            let local = local(RENTED, PROMPT, Some(3));
            let lock = local.store.acquire_run_lock(&local.config.run.id())?;
            let provider = marketplace();
            let guard = hosting(&provider, &local.store, &lock)?;
            let _ = session_over(&local, far, Some(guard), &AtomicBool::new(interrupt));
            Ok(provider.destroyed().len())
        };

        // The success path: the far run finalized.
        let done = local(RENTED, PROMPT, None);
        let run = done.config.run.id();
        let ok = Scripted::new().delivering(vec![vec![started(&run), finalized(&run)]]);
        assert_eq!(run_to(&ok, false)?, 1, "the success path tears down");

        // The failure path: the machine could not answer that it can drive.
        struct Unreachable;
        impl FarSide for Unreachable {
            fn devices(&self) -> Result<Contact> {
                Err(Error::Validation("the machine is unreachable".to_string()))
            }
            fn place(&self, _: &str) -> Result<()> {
                unreachable!("the migration never got past the reach check")
            }
            fn driving(&self) -> Result<Option<u32>> {
                unreachable!("the migration never got past the reach check")
            }
            fn start(&self) -> Result<u32> {
                unreachable!("the migration never got past the reach check")
            }
            fn interrupt(&self, _: u32) -> Result<()> {
                unreachable!("the migration never got past the reach check")
            }
            fn terminate(&self, _: u32) -> Result<()> {
                unreachable!("the migration never got past the reach check")
            }
            fn sync(&self, _: &Store, _: &[TaskKey], _: ObjectScope<'_>) -> Result<SyncReport> {
                unreachable!("the migration never got past the reach check")
            }
            fn follow(&self) -> Result<Box<dyn RunFeed>> {
                unreachable!("the migration never got past the reach check")
            }
        }
        assert_eq!(
            run_to(&Unreachable, false)?,
            1,
            "the failure path tears down"
        );

        // The interrupt path.
        let interrupted = Scripted::new().delivering(vec![vec![started(&run), committed("aa")]]);
        assert_eq!(
            run_to(&interrupted, true)?,
            1,
            "the interrupt path tears down"
        );

        // A far run that survived even termination fails the migration, and the
        // teardown that destroys the machine it is on runs on that path too.
        let unkillable = Scripted::new()
            .stubborn()
            .delivering(vec![vec![started(&run), committed("aa")]]);
        assert_eq!(
            run_to(&unkillable, true)?,
            1,
            "a far run that will not end still tears down"
        );
        Ok(())
    }

    #[test]
    fn a_migration_onto_a_machine_of_yours_rents_nothing() -> Result<()> {
        // Reached through `migrate` itself, which is where the rental decision
        // is: a machine of yours constructs no provider, so nothing is rented
        // even when the machine turns out to be unreachable.
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("sima.toml");
        std::fs::write(
            &path,
            config_text(
                "ssh = \"sima.invalid.test\"\nworkers = 1",
                &dir.path().join("far").to_string_lossy(),
                "",
            ),
        )
        .expect("write the config");
        let observer = |_: &Record| {};
        let error = migrate(&path, &observer, &AtomicBool::new(false))
            .expect_err("the machine cannot be reached");
        assert!(
            error.to_string().contains("sima.invalid.test"),
            "the reach check is what failed, naming what it could not reach: {error}"
        );
        let loaded = crate::config::load(&path)?;
        let store = Store::open(&loaded.store)?;
        assert!(
            store.instances()?.is_empty(),
            "a machine of yours rents nothing"
        );
        Ok(())
    }

    // ---- Reattaching ----

    #[test]
    fn a_rented_far_side_already_driving_is_adopted_and_neither_pushed_to_nor_started() -> Result<()>
    {
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let lock = local.store.acquire_run_lock(&run)?;
        let provider = marketplace();
        // The ledger as an earlier invocation left it: a live rental of this
        // run's orchestrator.
        let offer = provider.offers()?.into_iter().next().expect("an offer");
        let Provision::Provisioned(instance) = provider.provision(&offer.id, "sima-tag-0")? else {
            panic!("the stub provisions its offer");
        };
        let InstanceStatus::Ready(_) = provider.instance(&instance.id)? else {
            panic!("the stub instance is ready at once");
        };
        local.store.put_instance(&InstanceRecord {
            role: RentalRole::Orchestrator,
            tag: "sima-tag-0".to_string(),
            provider: "stub".to_string(),
            machine: "machine-0".to_string(),
            owner: run.to_string(),
            state: InstanceRecordState::Live {
                instance: instance.id.0.clone(),
            },
            price_micro_usd_hour: 100_000,
            created_ms: 1_700_000_000_000,
        })?;

        let HostForm::Rented(spec) = &local.config.hosts["slingshot"].form else {
            panic!("the fixture declares a rented machine");
        };
        let guard = hold(
            &provider,
            &local.store,
            &lock,
            spec,
            &Budget::default(),
            &AtomicBool::new(false),
        )?;
        assert_eq!(
            guard.id(),
            &instance.id,
            "the running machine is taken back"
        );
        assert_eq!(
            provider.live().len(),
            1,
            "no second machine was rented for it"
        );

        let far = Scripted::new()
            .already_driving()
            .delivering(vec![vec![started(&run), finalized(&run)]]);
        let (outcome, _) = session_over(&local, &far, Some(guard), &AtomicBool::new(false));
        outcome?;
        let steps = far.steps();
        assert!(!steps.contains(&PUSH), "nothing is pushed: {steps:?}");
        assert!(
            !steps.contains(&Step::Start),
            "nothing is started: {steps:?}"
        );
        assert!(
            steps.contains(&Step::Follow),
            "the follow attaches: {steps:?}"
        );
        Ok(())
    }

    #[test]
    fn a_machine_of_yours_already_driving_is_neither_pushed_to_nor_started() -> Result<()> {
        // It has no ledger record to adopt, since nothing was rented; `run.pid`
        // is the whole of the evidence.
        let local = local(OWNED, "", Some(3));
        let run = local.config.run.id();
        let far = Scripted::new()
            .already_driving()
            .delivering(vec![vec![started(&run), finalized(&run)]]);

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        outcome?;
        let steps = far.steps();
        assert!(!steps.contains(&PUSH), "nothing is pushed: {steps:?}");
        assert!(
            !steps.contains(&Step::Start),
            "nothing is started: {steps:?}"
        );
        Ok(())
    }

    #[test]
    fn a_reattaching_migration_forwards_what_arrives_after_it_and_not_the_replay() -> Result<()> {
        // The feed's first poll is the far run's whole history, produced while
        // nothing was attached to journal it.
        let local = local(OWNED, "", Some(3));
        let run = local.config.run.id();
        let far = Scripted::new().already_driving().delivering(vec![
            vec![started(&run), committed("aa")],
            vec![committed("bb"), finalized(&run)],
        ]);

        let (outcome, records) = session_over(&local, &far, None, &AtomicBool::new(false));
        outcome?;
        let tasks: Vec<&str> = records
            .iter()
            .filter_map(|record| match &record.event {
                Event::Committed { task, .. } => Some(task.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(tasks, ["bb"], "the replayed history is discarded");
        Ok(())
    }

    #[test]
    fn a_migration_that_started_the_far_run_forwards_its_whole_history() -> Result<()> {
        // The counterpart: nothing was attached because nothing was running, so
        // the first poll is this migration's own to journal.
        let local = local(OWNED, "", Some(3));
        let run = local.config.run.id();
        let far = Scripted::new().delivering(vec![
            vec![started(&run), committed("aa")],
            vec![committed("bb"), finalized(&run)],
        ]);

        let (outcome, records) = session_over(&local, &far, None, &AtomicBool::new(false));
        outcome?;
        let tasks: Vec<&str> = records
            .iter()
            .filter_map(|record| match &record.event {
                Event::Committed { task, .. } => Some(task.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(tasks, ["aa", "bb"]);
        // And the local journal holds them, which is the point of forwarding.
        let (lines, _) = local.store.journal_from(&run, 0)?;
        assert!(
            lines.iter().filter(|line| line.contains("\"aa\"")).count() > 0,
            "the far side's records reached the local journal"
        );
        Ok(())
    }
}
