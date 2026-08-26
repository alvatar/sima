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
//!  │       └─ no  ──▶ PUSH the run's closure; open the FOLLOW, whose first  │
//!  │                    poll is then the journal as it already stood, then  │
//!  │  6  START: setsid the far `sima run`, capture its pid into run.pid     │
//!  │  7  FOLLOW: render each record and forward it into the local journal;  │
//!  │       poll the budget verdict when this is a rental                    │
//!  │  8  end on: a terminal run event | local interrupt | budget exhaustion │
//!  │       ├─ interrupt ──▶ DETACH: leave the far run and its machine as    │
//!  │       │                  they are; the migration is over here          │
//!  │       └─ otherwise ──▶                                                 │
//!  │  9  WIND DOWN: signal the far run, wait for it to exit (bounded)       │
//!  │ 10  PULL: everything the far side's records reference                  │
//!  │ 11  re-derive the key set; finalize when every key is committed,       │
//!  │       otherwise report the rest                                        │
//!  │ 12  TEARDOWN: release the guard (rental only)                          │
//!  └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! **The far run is detached.** It is started with `setsid` and its pid
//! recorded, so a laptop that sleeps, a network that drops, a `sima migrate`
//! that is killed, and a Ctrl-C all leave the destination computing. Re-running
//! reattaches: a rented machine is found through the instance ledger, a machine
//! of yours through `run.pid`, and either way the push and the start are
//! skipped. The far run outlives everything except a terminal event and its
//! own ceiling.
//!
//! **Journals do not sync**, so each record the follow delivers is forwarded
//! into the local journal through the collector every other event crosses —
//! without it the local journal would hold a gap for every segment executed
//! remotely. The records a migration does not forward are diagnostic detail,
//! since journals are observational and excluded from every identity criterion.
//!
//! **The follow's first poll is the journal as it already stood**, and what
//! that is depends on when the follow opened:
//!
//! - A migration that starts the far run opens the follow **before** the start,
//!   so the first poll is an earlier session's journal — a run that once
//!   finished on this destination leaves one ending in its finalization. Those
//!   records are neither forwarded nor allowed to decide this run's outcome,
//!   and a far process that dies before writing any of its own is reported as
//!   the death it is rather than as that stale ending.
//! - A reattach opens the follow on a run already going, so the first poll is
//!   that run's own history: it decides the state, and is not re-emitted,
//!   having been produced while nothing was attached to journal it.
//! - A destination whose journal is empty cannot be followed at all until the
//!   run writes its first line, so the follow opens after the start and its
//!   first poll is this run's.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::RunId;
use sima_provider::{
    AcquireLimits, Budget, InstanceGuard, Objective, Provider, Verdict, acquire, adopt, assess,
    now_ms,
};
use sima_store::{ObjectScope, Rental as RentalRole, RunLock, Store};
use sima_trace::{Emitter, Observer};

use crate::config::{FillPolicy, HostForm, LoadedConfig, Rented};
// The readiness defaults are what a destination stating none falls back on,
// which under test comes from the session's test overrides instead.
use crate::feed::RunFeed;
use crate::fleet::Rental;
use crate::migrate::destination::destination_for;
use crate::migrate::far_config::{Registration, far_config};
#[cfg(test)]
use crate::migrate::far_run::Overrides;
use crate::migrate::far_run::{FarRun, FollowEnd, MigrateOutcome};
use crate::migrate::far_side::{Contact, Remote};
use crate::migrate::objects::push_objects;
use crate::payload::{PayloadSpec, closure, ingest};
use crate::program_binding::BinaryChange;
use crate::rental::{budget_exhausted, provider_for_rental};
use crate::sdk::Sdk;
use crate::status::{RunState, RunStatus};
use crate::task_keys::task_keys;

/// How long the follow waits before polling again when nothing has arrived.
///
/// The suite reads it from a test override instead, so what it fixes is the
/// order of what a follow asks for rather than the rate it asks at.
#[cfg(not(test))]
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
///
/// The suite reads it from a test override instead, so it fixes which failure a
/// wait that ran out reports without spending the wait.
#[cfg(not(test))]
const ATTACH_BOUND: Duration = Duration::from_secs(30);

/// Moves the run `config` describes onto the machine its `[orchestrator]`
/// names, follows it there, and brings the results home.
///
/// `observer` receives every record the far run produced, in journal order,
/// through the same collector that appends them locally. `interrupt` is the
/// level-triggered request `SIGINT` sets, and it means two different things by
/// when it arrives: during the acquisition it abandons the offer walk, since
/// there is no far run yet to leave behind; during the follow it detaches,
/// leaving the far run computing and its machine standing.
///
/// The local run lock is held for the whole call, so nothing else drives or
/// reconciles this run while it is away.
pub fn migrate(
    config: &Path,
    loaded: &LoadedConfig,
    observer: Observer<'_>,
    interrupt: &AtomicBool,
    accept: BinaryChange,
) -> Result<MigrateOutcome> {
    // The file's own text is what travels: `[run]` is carried across as a
    // parsed value rather than re-derived, so no translation here can perturb
    // the run id. The caller hands over what it already loaded, since a second
    // translation of one file is a second chance for the two to disagree.
    let local_text = std::fs::read_to_string(config).map_err(|source| Error::Io {
        path: config.to_path_buf(),
        source,
    })?;
    // The refusal a routed format with nothing to carry gets precedes the
    // destination, the store, the lock, and any provider, so it is stated
    // before anything is opened, moved, or rented.
    let carried = carried(loaded)?;
    let destination = destination_for(loaded)?;
    let store = Store::open(&loaded.store)?;
    // The program's bytes become objects before the lock is taken: the ingest
    // writes content-addressed objects and nothing else, which is the one store
    // write that needs no exclusion — two writers of one object write the same
    // bytes to the same address.
    let registration = match carried {
        None => None,
        Some(carried) => Some(Registration {
            format: loaded.run.format.as_str().to_string(),
            payload_digest: ingest(&store, &carried.payload)?,
            env: carried.env,
            sdk: carried.sdk,
        }),
    };
    // Registering the run is what gives it a journal to forward into, and it is
    // the same idempotent registration a local `sima run` performs.
    let run = store.create_run(&loaded.run)?;
    let lock = store.acquire_run_lock(&run)?;

    match destination.form {
        // A machine of yours is reached as it stands: nothing is rented, so no
        // provider is constructed and no credential is read.
        HostForm::Owned(owned) => {
            let far = Remote::owned(&destination, owned, &run);
            FarRun {
                far: &far,
                store: &store,
                config: loaded,
                destination: &destination,
                observer,
                rental: None,
                #[cfg(test)]
                overrides: Overrides::default(),
            }
            .under_teardown(|far_run| {
                Session {
                    far_run,
                    local_text: &local_text,
                    registration: registration.as_ref(),
                    accept,
                    interrupt,
                    // Nothing asked for this machine before now, so the contact
                    // is what starts its clock.
                    usable_by: None,
                }
                .run_to_end()
            })
        }
        HostForm::Rented(spec) => {
            // One machine under strict fill: `migrate` names exactly one, so
            // there is no count and no shortfall to consider.
            let rental = Rental {
                name: destination.name,
                spec,
                count: 1,
                fill: FillPolicy::Strict,
                root: destination.root,
                binary: destination.binary,
            };
            let provider = provider_for_rental(&rental)?;
            // The clock on this machine starts here, where it is first asked
            // for, and every stage that waits for it runs under the one
            // deadline: its readiness and its reachability are stages of the
            // single wait the entry states a budget for.
            let usable_by = Instant::now() + spec.ready_timeout;
            let guard = hold(
                provider.as_ref(),
                &store,
                &lock,
                spec,
                usable_by,
                &loaded.budget,
                interrupt,
            )?;
            // A run whose format is a program asks the machine about no
            // format: nothing there can resolve one it has not been given, and
            // the program's own enumeration is what the far run derives its
            // layout from once the load has installed it.
            let far = Remote::rented(
                &destination,
                provider.as_ref(),
                guard.endpoint(),
                &run,
                registration.is_none().then_some(&loaded.run.format),
            )?;
            FarRun {
                far: &far,
                store: &store,
                config: loaded,
                destination: &destination,
                observer,
                rental: Some(guard),
                #[cfg(test)]
                overrides: Overrides::default(),
            }
            .under_teardown(|far_run| {
                Session {
                    far_run,
                    local_text: &local_text,
                    registration: registration.as_ref(),
                    accept,
                    interrupt,
                    usable_by: Some(usable_by),
                }
                .run_to_end()
            })
        }
    }
}

/// What a migration of this run has to carry to the destination beyond the
/// run's own closure: the program its format is served by, as the `[domain.*]`
/// entry declares it travels.
struct Carried {
    payload: PayloadSpec,
    /// The variable names the entry declared, which reach the far entry as
    /// names; each value is the destination's own.
    env: Vec<String>,
    /// The SDK the entry declared, which reaches the far entry as the same
    /// declaration; the package is the destination binary's to vend.
    sdk: Option<Sdk>,
}

/// What `loaded` must carry, and `None` for a run whose format this build
/// answers — there is no program, so nothing travels.
///
/// A routed format whose entry states no payload is a program this machine
/// holds and no other, and that is refused here: naming the format, the
/// program, and the key that would make it travel.
fn carried(loaded: &LoadedConfig) -> Result<Option<Carried>> {
    let Some(routed) = loaded.domains.routed(&loaded.run.format) else {
        return Ok(None);
    };
    let Some(payload) = routed.payload else {
        return Err(Error::Validation(format!(
            "the run's format {format:?} is served by the program {}, and the \
             [domain.{format:?}] entry states no payload: a migration carries the program to \
             the destination as objects, so the entry names what travels. Add a payload key \
             naming the file or directory that is the program, or drive this run on the \
             machine the program is installed on.",
            routed.binary.display(),
            format = loaded.run.format.as_str(),
        )));
    };
    Ok(Some(Carried {
        payload: payload.clone(),
        env: routed.env.to_vec(),
        sdk: routed.sdk,
    }))
}

/// The rented machine hosting this run: the one already hosting it, or a fresh
/// one under the host entry's specification.
///
/// Adoption comes first because a migration detaches the far side deliberately,
/// so a machine already working and already being paid for is the common case
/// on a second invocation. `interrupt` aborts an offer walk in flight, so a
/// `SIGINT` during acquisition is not waited out — and that is all it can mean
/// here: detaching exists only once a far run is driving, and nothing is yet.
fn hold<'a>(
    provider: &'a (dyn Provider + Sync),
    store: &'a Store,
    lock: &RunLock,
    spec: &Rented,
    usable_by: Instant,
    budget: &Budget,
    interrupt: &AtomicBool,
) -> Result<InstanceGuard<'a, dyn Provider + Sync + 'a>> {
    let limits = AcquireLimits {
        usable_by,
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

/// One migration, past the destination's resolution: steps 3 through 8 over the
/// far run it starts or attaches to.
///
/// Split from [`migrate`] so the choreography is driven against a recording
/// [`FarSide`] with no machine at all.
struct Session<'a> {
    far_run: &'a FarRun<'a>,
    /// The local config's own file text, which is what travels.
    local_text: &'a str,
    /// How the far side answers for this run's format, when a `[domain.*]`
    /// entry routes it to a program of this machine's. `None` for a format
    /// this build answers, whose far config declares no `[domain.*]` table.
    registration: Option<&'a Registration>,
    /// What the invocation stated about a program whose build changed under
    /// the run — carried to the far `sima run`, whose own binding guard is
    /// what compares the two.
    accept: BinaryChange,
    interrupt: &'a AtomicBool,
    /// When the destination must be usable by, established when the machine was
    /// first asked for. `None` for a destination with no acquisition phase — a
    /// machine of yours — whose contact is the first thing to ask for it and so
    /// starts the deadline itself.
    usable_by: Option<Instant>,
}

impl Session<'_> {
    /// Steps 3 through 11: reach the machine, place the run on it, push, start,
    /// follow, wind down, pull, and settle.
    fn run_to_end(&self) -> Result<MigrateOutcome> {
        let far_run = self.far_run;
        let probed = self.reach()?;
        let far_text = far_config(
            self.local_text,
            far_run.destination.form,
            &probed,
            self.registration,
        )?;
        far_run.far.place(&far_text)?;

        // A far side already driving this run is a reattach: it holds the
        // closure it was sent and its own progress since, so pushing would send
        // what it already has, and starting would run a second orchestrator
        // against a store whose lock the first one holds.
        let reattached = far_run.far.driving()?;
        let (pid, opened) = match reattached {
            Some(pid) => (pid, None),
            None => {
                let keys = task_keys(far_run.config, far_run.store)?;
                let mut objects = push_objects(far_run.store, &keys)?;
                // The program travels with the run it serves: the manifest and
                // its files ride the same push, and the sync's negotiation is
                // what skips the bytes the destination already holds.
                if let Some(registration) = self.registration {
                    objects.extend(closure(far_run.store, &registration.payload_digest)?);
                }
                far_run
                    .far
                    .sync(far_run.store, &keys, ObjectScope::Named(&objects))?;
                // The follow opens before the far run starts, so its first poll
                // is the journal as an earlier session left it and everything
                // after that is this run's. It is what tells a run that once
                // finished on this destination from the one starting now.
                //
                // A journal that is not there yet cannot be followed, and there
                // is then nothing earlier to tell apart: the follow opens after
                // the start instead, waiting for the run's first line.
                let opened = far_run.far.follow().ok();
                (far_run.far.start(self.accept)?, opened)
            }
        };

        let (state, end) = self.watch(pid, reattached.is_some(), opened)?;
        // Detaching leaves the far run computing, so there is nothing to pull:
        // its results come home on the next migration that sees it end, or on
        // a recall. Skipping the pull is what makes letting go immediate.
        if end == FollowEnd::Detached {
            return Ok(MigrateOutcome::Detached {
                run: far_run.config.run.id(),
                machine: far_run.destination.name.to_string(),
            });
        }

        far_run.pull()?;
        far_run.settle(state, end)
    }

    /// Steps 7 through 9: follow the far run to its end, then wind it down and
    /// wait for it to exit.
    ///
    /// Returns the state the far run's journal projects and what ended the
    /// follow, both under the run's journal boundary.
    fn watch(
        &self,
        pid: u32,
        reattached: bool,
        opened: Option<Box<dyn RunFeed>>,
    ) -> Result<(RunState, FollowEnd)> {
        let run = self.far_run.config.run.id();
        self.far_run.journaling(|events| {
            let (state, end) = self.follow(&run, reattached, opened, events)?;
            match end {
                // Letting go signals nothing and waits for nothing: the far run
                // is left exactly as it was.
                FollowEnd::Detached => {}
                FollowEnd::FarRun => self.far_run.wind_down(pid, false, events)?,
                FollowEnd::WoundDown => self.far_run.wind_down(pid, true, events)?,
            }
            Ok((state, end))
        })
    }

    /// Follows the far run until it ends, this side is interrupted, or a
    /// rental's budget runs out, forwarding each record into the run's journal.
    fn follow(
        &self,
        run: &RunId,
        reattached: bool,
        opened: Option<Box<dyn RunFeed>>,
        events: &Emitter,
    ) -> Result<(RunState, FollowEnd)> {
        let budget = self.budget();
        // A follow opened before the far run started is the one to use; every
        // other case waits for the run's first journal line and opens then.
        // What the first poll holds follows from which it was:
        //
        // - opened before the start: an earlier session's journal. This
        //   migration is not watching that run, so its records neither decide
        //   this outcome nor cross into the local journal a second time.
        // - a reattach: the far run's own history, produced while nothing was
        //   attached to journal it. It decides the state, and is not re-emitted.
        // - opened after the start: the journal was empty before it, so the
        //   first poll is this run's like any other.
        let earlier_session = opened.is_some();
        let mut feed = match opened {
            Some(feed) => feed,
            None => self.attach()?,
        };
        let mut status = RunStatus::new(*run);
        let mut first = true;
        // Whether this run has journaled a line of its own.
        let mut journaled = false;
        // Unset, so the first tick assesses: a migration re-run under a ceiling
        // already spent must not first watch for an interval.
        let mut assessed: Option<Instant> = None;
        loop {
            let records = feed.poll()?;
            if !(first && earlier_session) {
                let replayed = first && reattached;
                for record in &records {
                    status.apply(record);
                    if !replayed {
                        events.emit(record.event.clone());
                    }
                }
                journaled |= !records.is_empty();
            }
            first = false;
            if !matches!(status.state, RunState::InProgress) {
                return Ok((status.state, FollowEnd::FarRun));
            }
            // The operator letting go, which is the whole of what a `SIGINT`
            // asks for: the far run is none of this side's business from here.
            if self.interrupt.load(Ordering::Relaxed) {
                return Ok((status.state, FollowEnd::Detached));
            }
            // Money is the one thing that cannot wait for an operator to come
            // back, so an exhausted ceiling ends the far run rather than
            // letting go of it.
            if let Some(budget) = budget
                && assessed.is_none_or(|at| at.elapsed() >= BUDGET_INTERVAL)
            {
                assessed = Some(Instant::now());
                if let Verdict::Exhausted(exhaustion) =
                    assess(self.far_run.store, run, budget, now_ms())?
                {
                    events.emit(budget_exhausted(exhaustion));
                    return Ok((status.state, FollowEnd::WoundDown));
                }
            }
            if records.is_empty() {
                // A free lock is not yet an ended run: the far `sima run` takes
                // it only once it has loaded its config and opened its store,
                // and the follow can connect before that. The pid it was started
                // under answers without a race, so it is what decides — and it
                // is asked only on the rare tick where the lock reads free.
                if feed.holder()?.is_none() && self.far_run.far.driving()?.is_none() {
                    // The far run this migration started is gone and journaled
                    // nothing of its own, so everything the state rests on was
                    // written by an earlier session. It died while loading —
                    // an install that could not build, a binding guard that
                    // refused — and its own words are in the log it wrote.
                    if !reattached && !journaled {
                        return Err(self.far_run.died("ended before it journaled anything"));
                    }
                    return Ok((status.state, FollowEnd::FarRun));
                }
                sleep(self.tick());
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
        let deadline = Instant::now() + self.attach_bound();
        loop {
            match self.far_run.far.follow() {
                Ok(feed) => return Ok(feed),
                Err(error) => {
                    let gone = self.far_run.far.driving()?.is_none();
                    if gone || Instant::now() >= deadline {
                        return Err(self.unattachable(error, gone));
                    }
                    sleep(self.tick());
                }
            }
        }
    }

    /// What a follow that could not attach reports.
    ///
    /// A far run that is gone died before it journaled anything, which is what
    /// every far-side load failure looks like from here — a program that cannot
    /// answer for its format, an install script that exited non-zero, a store
    /// that will not open. The follow's own refusal says only that there is no
    /// run to follow, so the far run's own words are fetched from its log and
    /// the machine is named.
    ///
    /// A far run still alive that simply has not journaled inside the bound is
    /// a different thing, and the follow's refusal is the whole of it.
    fn unattachable(&self, refusal: Error, gone: bool) -> Error {
        if !gone {
            return refusal;
        }
        self.far_run.died(&format!(
            "ended before it journaled anything, so there is nothing to follow: {refusal}"
        ))
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
        // A rented destination was asked for before this, and that is when its
        // clock started; a machine of yours is first asked for here.
        let deadline = self.usable_by.unwrap_or_else(|| Instant::now() + bound);
        loop {
            // A machine that answered has answered: what it said is the result,
            // whether that is its devices or a reason the run cannot proceed.
            // Only a machine that could not be reached is worth asking again.
            match self.far_run.far.devices()? {
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

    /// The ceiling this migration runs under, or `None` when nothing is being
    /// paid for. `[budget]` is the run's own, the same ceiling a fleet spends
    /// under; a machine of yours has no rate and so no ceiling to keep.
    fn budget(&self) -> Option<&Budget> {
        matches!(self.far_run.destination.form, HostForm::Rented(_))
            .then_some(&self.far_run.config.budget)
    }

    /// The readiness bounds this session waits under, which are the far run's.
    fn ready_bounds(&self) -> (Duration, Duration) {
        self.far_run.ready_bounds()
    }

    /// How long the follow waits for the far run's first journal line.
    #[cfg(not(test))]
    fn attach_bound(&self) -> Duration {
        ATTACH_BOUND
    }

    #[cfg(test)]
    fn attach_bound(&self) -> Duration {
        self.far_run.overrides.attach_bound
    }

    /// How long the follow waits before polling again.
    #[cfg(not(test))]
    fn tick(&self) -> Duration {
        TICK
    }

    #[cfg(test)]
    fn tick(&self) -> Duration {
        self.far_run.overrides.tick
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use sima_core::Result;
    use sima_model::TaskKey;
    use sima_provider::{InstanceStatus, Provision};
    use sima_scheduler::{Event, Record};
    use sima_store::{InstanceRecord, InstanceRecordState, Rental as RentalRole, SyncReport};

    use super::*;
    use crate::migrate::far_side::FarSide;
    use crate::migrate::fixtures::{
        Local, OWNED, PID, PROMPT, PULL, PUSH, RENTED, Scripted, Step, committed, config_text,
        failed, far_store, finalized, hosting, local, marketplace, over_budget, started,
    };

    /// Drives one session over `far`, capturing every record the follow
    /// forwarded.
    fn session_over(
        local: &Local,
        far: &dyn FarSide,
        rental: Option<InstanceGuard<'_, dyn Provider + Sync + '_>>,
        interrupt: &AtomicBool,
    ) -> (Result<MigrateOutcome>, Vec<Record>) {
        session_over_by(local, far, rental, interrupt, None)
    }

    /// Drives one session whose destination must be usable by `usable_by`, as
    /// a rented one is once its acquisition has started the clock.
    fn session_over_by(
        local: &Local,
        far: &dyn FarSide,
        rental: Option<InstanceGuard<'_, dyn Provider + Sync + '_>>,
        interrupt: &AtomicBool,
        usable_by: Option<Instant>,
    ) -> (Result<MigrateOutcome>, Vec<Record>) {
        let captured: Mutex<Vec<Record>> = Mutex::new(Vec::new());
        let observer = |record: &Record| {
            captured
                .lock()
                .expect("the capture lock")
                .push(record.clone());
        };
        let destination = destination_for(&local.config).expect("the host is declared");
        let outcome = FarRun {
            far,
            store: &local.store,
            config: &local.config,
            destination: &destination,
            observer: &observer,
            rental,
            overrides: Overrides::default(),
        }
        .under_teardown(|far_run| {
            Session {
                far_run,
                local_text: &local.text,
                registration: None,
                accept: BinaryChange::Refuse,
                interrupt,
                usable_by,
            }
            .run_to_end()
        });
        let records = std::mem::take(&mut *captured.lock().expect("the capture lock"));
        (outcome, records)
    }

    // ---- The program the run carries with it ----

    /// Drives one session under `registration` and `accept`, so what the
    /// far side is handed about the program is what the test fixes.
    fn session_carrying(
        local: &Local,
        far: &dyn FarSide,
        registration: Option<&Registration>,
        accept: BinaryChange,
    ) -> Result<MigrateOutcome> {
        let observer = |_: &Record| {};
        let destination = destination_for(&local.config).expect("the host is declared");
        FarRun {
            far,
            store: &local.store,
            config: &local.config,
            destination: &destination,
            observer: &observer,
            rental: None,
            overrides: Overrides::default(),
        }
        .under_teardown(|far_run| {
            Session {
                far_run,
                local_text: &local.text,
                registration,
                accept,
                interrupt: &AtomicBool::new(false),
                usable_by: None,
            }
            .run_to_end()
        })
    }

    #[test]
    fn a_carried_program_reaches_the_far_config_and_the_push() -> Result<()> {
        // The two halves of delivering a program: the far config states the
        // digest to install, and the push carries the objects to install it
        // from.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let (_payload, spec) = crate::fixtures::file_payload();
        let digest = crate::payload::ingest(&local.store, &spec)?;
        let registration = Registration {
            format: "stub.v1".to_string(),
            payload_digest: digest,
            env: vec!["PATH".to_string()],
            sdk: None,
        };
        let far = Scripted::new().delivering(vec![vec![started(&run), finalized(&run)]]);

        session_carrying(&local, &far, Some(&registration), BinaryChange::Refuse)?;

        // The far config installs it.
        let placed = far
            .placed
            .lock()
            .expect("the placement lock")
            .clone()
            .expect("a config was placed");
        let table: toml::Table = placed.parse().expect("the far config parses");
        assert_eq!(
            table["domain"]["stub.v1"]["payload_digest"].as_str(),
            Some(digest.to_string().as_str())
        );

        // The push carries every object it needs to.
        let pushed = far.pushed.lock().expect("the push lock").clone();
        for object in crate::payload::closure(&local.store, &digest)? {
            assert!(
                pushed.contains(&object),
                "{object} must travel with the run"
            );
        }
        Ok(())
    }

    #[test]
    fn a_run_this_build_answers_pushes_no_program_and_declares_none() -> Result<()> {
        // The counterpart: nothing about a program is written down, and the
        // push is the identity components and the frontier states alone.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new().delivering(vec![vec![started(&run), finalized(&run)]]);

        session_carrying(&local, &far, None, BinaryChange::Refuse)?;
        let placed = far
            .placed
            .lock()
            .expect("the placement lock")
            .clone()
            .expect("a config was placed");
        assert!(
            !placed
                .parse::<toml::Table>()
                .expect("the far config parses")
                .contains_key("domain"),
            "{placed}"
        );
        let keys = task_keys(&local.config, &local.store)?;
        assert_eq!(
            far.pushed.lock().expect("the push lock").clone(),
            push_objects(&local.store, &keys)?
        );
        Ok(())
    }

    #[test]
    fn the_acceptance_of_a_changed_program_reaches_the_far_start() -> Result<()> {
        // The comparison is the far run's — it journals what it installed —
        // so the operator's acceptance has to travel to it.
        for accept in [BinaryChange::Refuse, BinaryChange::Accept] {
            let local = local(RENTED, PROMPT, Some(3));
            let run = local.config.run.id();
            let far = Scripted::new().delivering(vec![vec![started(&run), finalized(&run)]]);
            session_carrying(&local, &far, None, accept)?;
            assert_eq!(
                *far.started_with.lock().expect("the acceptance lock"),
                Some(accept),
                "the far run is started with what the invocation stated"
            );
        }
        Ok(())
    }

    #[test]
    fn a_reattach_carries_no_program_because_it_starts_nothing() -> Result<()> {
        // A far run already going installed its program when it loaded; the
        // push and the start are skipped, so nothing is sent or accepted.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new()
            .already_driving()
            .delivering(vec![vec![started(&run), finalized(&run)]]);
        session_carrying(&local, &far, None, BinaryChange::Accept)?;
        assert!(!far.steps().contains(&PUSH), "{:?}", far.steps());
        assert!(!far.steps().contains(&Step::Start), "{:?}", far.steps());
        assert_eq!(*far.started_with.lock().expect("the acceptance lock"), None);
        Ok(())
    }

    #[test]
    fn a_far_run_that_died_before_journaling_reports_its_own_last_words() -> Result<()> {
        // Every far-side load failure looks the same from here — the follow
        // finds a run that never started — so the far run's log is what says
        // which one it was. An install that could not build the program is the
        // case this exists for.
        let local = local(RENTED, PROMPT, Some(3));
        let far = Scripted::new().dying_while_loading(
            "sima: validation error: the install script install.sh exited with exit status: 3",
        );

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        let error = outcome.expect_err("a run that never journaled cannot be followed");
        let text = error.to_string();
        assert!(text.contains("slingshot"), "names the machine: {text}");
        assert!(
            text.contains("install script install.sh exited"),
            "carries the far run's own words: {text}"
        );
        assert!(
            far.steps().contains(&Step::LogTail),
            "the log was asked for: {:?}",
            far.steps()
        );
        Ok(())
    }

    #[test]
    fn a_far_run_that_dies_over_an_earlier_session_s_journal_reports_its_death() -> Result<()> {
        // A second migration onto a run that once finished on the destination:
        // the far journal already ends in that finalization, and this
        // invocation's process dies while loading. The follow attaches to the
        // journal that is there, so the stale ending is all it can replay —
        // and it is not this migration's outcome.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new()
            .dying_while_loading(
                "sima: validation error: the install script install.sh exited with exit status: 3",
            )
            .over_an_existing_journal(vec![started(&run), finalized(&run)]);

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        let text = outcome
            .expect_err("a stale finalization is not this migration's outcome")
            .to_string();
        assert!(text.contains("slingshot"), "names the machine: {text}");
        assert!(
            text.contains("install script install.sh exited"),
            "carries the far run's own words: {text}"
        );
        assert!(
            !far.steps().contains(&PULL),
            "nothing was pulled: {:?}",
            far.steps()
        );
        assert!(
            local.store.manifest(&run)?.is_none(),
            "and nothing was sealed"
        );
        Ok(())
    }

    #[test]
    fn a_far_run_that_dies_after_journaling_still_comes_home_outstanding() -> Result<()> {
        // The counterpart, and the behavior that must not drift: a far run
        // that journaled its own records and then vanished mid-run is a run
        // whose results come home, not a death to report.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new()
            .vanishing_when_drained()
            .delivering(vec![vec![started(&run), committed("aa")]]);

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        assert!(matches!(outcome?, MigrateOutcome::Outstanding { .. }));
        assert_eq!(
            far.steps().last(),
            Some(&PULL),
            "its results came home: {:?}",
            far.steps()
        );
        Ok(())
    }

    #[test]
    fn a_far_run_that_died_leaving_no_log_still_names_the_machine() -> Result<()> {
        // A run that never wrote a line leaves the absence to report, which is
        // still more than the follow's own refusal states.
        let local = local(RENTED, PROMPT, Some(3));
        let far = Scripted::new().dying_while_loading("");

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        let text = outcome
            .expect_err("a run that never journaled cannot be followed")
            .to_string();
        assert!(text.contains("slingshot"), "{text}");
        assert!(text.contains("log is empty"), "{text}");
        Ok(())
    }

    #[test]
    fn a_far_run_that_never_journals_but_stays_up_reports_the_follow_s_refusal() -> Result<()> {
        // A run that is up owes no explanation: the follow's own refusal is
        // the whole of what happened, and no log is fetched to explain a death
        // that did not occur. The bound the wait runs out on is production's;
        // the suite overrides only how long it is.
        let local = local(RENTED, PROMPT, Some(3));
        let far = Scripted::new()
            .already_driving()
            .refusing_the_follow(usize::MAX);

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(false));
        let text = outcome
            .expect_err("a follow that never opens fails the migration")
            .to_string();
        assert!(text.contains("never started in this store"), "{text}");
        assert!(
            !text.contains("ended before it journaled"),
            "a run that is up did not die: {text}"
        );
        assert!(
            !far.steps().contains(&Step::LogTail),
            "and no log was asked for: {:?}",
            far.steps()
        );
        Ok(())
    }

    #[test]
    fn a_far_run_still_alive_that_has_not_journaled_is_waited_for_and_asked_nothing() -> Result<()>
    {
        // The other side of the distinction: a run that is up and simply has
        // not journaled yet is not a run that died. The follow waits it out,
        // and no log is fetched to explain a failure that did not happen.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new()
            .refusing_the_follow(3)
            .delivering(vec![vec![started(&run), finalized(&run)]]);

        session_over(&local, &far, None, &AtomicBool::new(false)).0?;
        assert_eq!(
            far.steps().iter().filter(|s| **s == Step::Follow).count(),
            4,
            "every refusal inside the bound was tried again: {:?}",
            far.steps()
        );
        assert!(
            !far.steps().contains(&Step::LogTail),
            "a run that is up owes no explanation: {:?}",
            far.steps()
        );
        Ok(())
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
                // The follow opens before the start, so its first poll is the
                // journal as it stood before this run wrote to it.
                Step::Follow,
                Step::Start,
                // The far side holds nothing yet, so that follow was refused
                // and the one that reads this run's records opens after it.
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
    fn a_contact_runs_under_the_deadline_the_acquisition_started() -> Result<()> {
        // The entry states one budget for how long from asking for a machine
        // until it is usable, and the readiness wait and this contact are
        // stages of that one wait. An acquisition that spent it leaves the
        // contact nothing, rather than starting a second wait of its own.
        let local = local(RENTED, PROMPT, Some(3));
        let far = Scripted::new().refusing(usize::MAX);

        let spent = Instant::now();
        let (outcome, _) =
            session_over_by(&local, &far, None, &AtomicBool::new(false), Some(spent));
        outcome.expect_err("a machine that never answers fails");
        assert_eq!(
            far.steps(),
            vec![Step::Devices],
            "one attempt under a budget already spent"
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
    fn a_raised_interrupt_detaches_and_leaves_the_far_run_computing() -> Result<()> {
        // Ctrl-C is the operator letting go, not the operator stopping the
        // run: the far side is neither signalled nor pulled from, and the
        // machine it computes on is kept.
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
        assert_eq!(
            outcome?,
            MigrateOutcome::Detached {
                run,
                machine: "slingshot".to_string(),
            },
            "letting go is its own outcome, and it names where the run is"
        );
        assert_eq!(
            far.steps(),
            [
                Step::Devices,
                Step::Place,
                Step::Driving,
                PUSH,
                Step::Follow,
                Step::Start,
                Step::Follow,
            ],
            "nothing was signalled and nothing was pulled"
        );
        assert_eq!(
            *far.alive.lock().expect("the pid lock"),
            Some(PID),
            "the far run keeps computing"
        );
        assert!(
            provider.destroyed().is_empty(),
            "the machine it computes on is kept"
        );
        assert_eq!(
            local.store.instance_records()?.len(),
            1,
            "its ledger record stands, so the next migration adopts it"
        );
        Ok(())
    }

    #[test]
    fn an_interrupt_during_the_acquisition_abandons_it_and_rents_nothing() -> Result<()> {
        // Detaching exists only once there is a far run to detach from. Before
        // one, the interrupt keeps the meaning it has always had: abandon the
        // offer walk and leave nothing rented.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let lock = local.store.acquire_run_lock(&run)?;
        let provider = marketplace();
        let HostForm::Rented(spec) = &local.config.hosts["slingshot"].form else {
            panic!("the fixture declares a rented machine");
        };

        let held = hold(
            &provider,
            &local.store,
            &lock,
            spec,
            Instant::now() + spec.ready_timeout,
            &Budget::default(),
            &AtomicBool::new(true),
        );
        let Err(error) = held else {
            panic!("an interrupted acquisition rents nothing");
        };
        assert!(error.to_string().contains("cancelled"), "{error}");
        assert!(provider.live().is_empty(), "no machine was left running");
        assert!(
            local.store.instance_records()?.is_empty(),
            "and none is left in the ledger"
        );
        Ok(())
    }

    #[test]
    fn a_detached_migration_of_a_machine_of_yours_reports_where_the_run_is() -> Result<()> {
        // The outcome carries what the operator needs to come back: the run,
        // and the machine it is still computing on.
        let local = local(OWNED, "", Some(3));
        let run = local.config.run.id();
        let far = Scripted::new().delivering(vec![vec![started(&run), committed("aa")]]);

        let (outcome, _) = session_over(&local, &far, None, &AtomicBool::new(true));
        assert_eq!(
            outcome?,
            MigrateOutcome::Detached {
                run,
                machine: "slingshot".to_string(),
            }
        );
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
        over_budget(&local)?;
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
        over_budget(&local)?;
        let far = Scripted::new()
            .outlasting_the_wind_down()
            .delivering(vec![vec![started(&run), committed("aa")]]);

        let (outcome, records) = session_over(&local, &far, None, &AtomicBool::new(false));
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
        over_budget(&local)?;
        let far = Scripted::new()
            .stubborn()
            .delivering(vec![vec![started(&run), committed("aa")]]);

        let (outcome, records) = session_over(&local, &far, None, &AtomicBool::new(false));
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
        over_budget(&local)?;
        let far = Scripted::new()
            .deaf_for(2)
            .delivering(vec![vec![started(&run), committed("aa")]]);

        let (outcome, records) = session_over(&local, &far, None, &AtomicBool::new(false));
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
        // The end state that matters: no path that ends the far run leaves a
        // machine running and being paid for. The session releases the guard
        // explicitly so a teardown failure is reported rather than swallowed;
        // the guard's own drop is the second line of the same guarantee.
        //
        // Detaching is the one path that keeps the machine, since the run it
        // hosts is still computing; it has its own test.
        let run_to = |far: &dyn FarSide, exhausted: bool| -> Result<usize> {
            let local = local(RENTED, PROMPT, Some(3));
            if exhausted {
                over_budget(&local)?;
            }
            let lock = local.store.acquire_run_lock(&local.config.run.id())?;
            let provider = marketplace();
            let guard = hosting(&provider, &local.store, &lock)?;
            let _ = session_over(&local, far, Some(guard), &AtomicBool::new(false));
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
            fn placed(&self) -> Result<bool> {
                unreachable!("the migration never got past the reach check")
            }
            fn driving(&self) -> Result<Option<u32>> {
                unreachable!("the migration never got past the reach check")
            }
            fn start(&self, _: BinaryChange) -> Result<u32> {
                unreachable!("the migration never got past the reach check")
            }
            fn interrupt(&self, _: u32) -> Result<()> {
                unreachable!("the migration never got past the reach check")
            }
            fn terminate(&self, _: u32) -> Result<()> {
                unreachable!("the migration never got past the reach check")
            }
            fn log_tail(&self) -> Result<String> {
                unreachable!("the migration never got past the reach check")
            }
            fn sync(&self, _: &Store, _: &[TaskKey], _: ObjectScope<'_>) -> Result<SyncReport> {
                unreachable!("the migration never got past the reach check")
            }
            fn follow(&self) -> Result<Box<dyn RunFeed>> {
                unreachable!("the migration never got past the reach check")
            }
            fn snapshot(&self) -> Result<Option<Vec<Record>>> {
                unreachable!("the migration never got past the reach check")
            }
        }
        assert_eq!(
            run_to(&Unreachable, false)?,
            1,
            "the failure path tears down"
        );

        // The wind-down path: the ceiling ran out and this side ended the far
        // run.
        let wound_down = Scripted::new().delivering(vec![vec![started(&run), committed("aa")]]);
        assert_eq!(
            run_to(&wound_down, true)?,
            1,
            "the wind-down path tears down"
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
        let loaded = crate::config::load(&path)?;
        let error = migrate(
            &path,
            &loaded,
            &observer,
            &AtomicBool::new(false),
            BinaryChange::Refuse,
        )
        .expect_err("the machine cannot be reached");
        assert!(
            error.to_string().contains("sima.invalid.test"),
            "the reach check is what failed, naming what it could not reach: {error}"
        );
        let store = Store::open(&loaded.store)?;
        assert!(
            store.instance_records()?.is_empty(),
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
            Instant::now() + spec.ready_timeout,
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
        let far = Scripted::new()
            .already_driving()
            .over_an_existing_journal(vec![started(&run), committed("aa")])
            .delivering(vec![vec![committed("bb"), finalized(&run)]]);

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
