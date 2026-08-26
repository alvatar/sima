//! The values a migration test starts from: a far side that answers from a
//! script and records what it was asked, and the local side whose store and
//! config a verb is driven over.
//!
//! Both verbs are exercised against the same machine that is not there, so the
//! doubles are built once here rather than twice. Compiled only for the tests,
//! which are the whole of what reaches them.

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sima_contracts::DeviceClass;
use sima_core::{Error, Result};
use sima_domains::devices::{DeviceInfo, DeviceType};
use sima_model::{RunId, TaskKey};
use sima_provider::stub::StubProvider;
use sima_provider::{
    AcquireLimits, Budget, Constraints, InstanceGuard, Objective, Provider, acquire,
};
use sima_scheduler::{Event, Record, RunOutcome};
use sima_store::{ObjectScope, Rental as RentalRole, RunLock, SpendEntry, Store, SyncReport};
use tempfile::TempDir;

use crate::config::LoadedConfig;
use crate::feed::{FeedInfo, RunFeed};
use crate::fixtures::{drive_run, sync_between};
use crate::migrate::far_side::{Contact, FarSide};
use crate::program_binding::BinaryChange;
use crate::task_keys::task_keys;

pub(crate) const PID: u32 = 4242;

// ---- The recording far side ----

/// One far-side operation, in the order it was asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Devices,
    Place,
    Placed,
    Driving,
    Start,
    /// A sync, under the object scope the session chose for it — which is
    /// the whole of what makes one direction a push and the other a pull.
    Sync(Scope),
    Follow,
    /// A one-shot read of the far run's journal, which is how a recall learns
    /// what it ended as.
    Snapshot,
    LogTail,
    Interrupt(u32),
    Terminate(u32),
}

/// The object scope a sync was handed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    /// The identity components and each chain's frontier state: a push.
    Named,
    /// Everything the records reference: a pull.
    Referenced,
}

/// What the far store's journal is, which the probe and the read of it
/// answer between them.
#[derive(Debug, Clone)]
pub(crate) enum FarJournal {
    /// No journal file at all: nothing has ever journaled on that
    /// destination.
    Absent,
    /// A journal holding these records, served in full.
    Holding(Vec<Record>),
    /// A journal that is there and cannot be read: the far side answers for
    /// itself with these words. Never an absence.
    Faulting(String),
}

/// What ends a scripted far run, which is what the wind-down's escalation
/// is measured against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ending {
    /// Winds down on the first signal it hears, as `sima run` does.
    OnInterrupt,
    /// Outlasts the wind-down and ends when it is terminated.
    OnTermination,
    /// Never exits, however it is asked to.
    Never,
}

/// The push, named where a step sequence reads it.
pub(crate) const PUSH: Step = Step::Sync(Scope::Named);
/// The pull.
pub(crate) const PULL: Step = Step::Sync(Scope::Referenced);

/// A far side that records what it was asked to do and answers from a
/// script, so the choreography is driven with no machine at all.
///
/// Its far run is alive from the start (or from a preset, for a reattach)
/// until the feed delivers a terminal record or the wind-down signals it —
/// which is what a real `sima run` does — unless it is stubborn, in which
/// case it never exits and the wind-down's wait runs out on it.
pub(crate) struct Scripted<'a> {
    devices: Vec<DeviceInfo>,
    /// The far-side run's pid while it is alive.
    pub(crate) alive: Arc<Mutex<Option<u32>>>,
    /// What ends this far run.
    ending: Ending,
    /// How many times the first contact is refused before the machine
    /// answers: a freshly rented host's sshd can lag its provider's
    /// `Ready`, and the connection is refused until it is up.
    refusals: Mutex<usize>,
    /// A machine that answers, without the worker image its pool needs —
    /// a failure no amount of waiting resolves.
    image_absent: bool,
    /// A machine holding no directory for this run, which is what one that
    /// was never migrated to looks like.
    unplaced: bool,
    /// How many signals the far run discards before it winds down: a run
    /// that has not yet installed its own handler is not signallable, and
    /// the disposition it inherited discards what it is sent.
    deaf: Mutex<usize>,
    /// The records the follow feed delivers, one batch per poll.
    pub(crate) polls: Arc<Mutex<VecDeque<Vec<Record>>>>,
    /// The journal the far store already holds: what a follow opened before
    /// this run starts replays, and what a one-shot read of the far side
    /// answers with. Absent for a destination no run has ever journaled on.
    journal: Mutex<FarJournal>,
    /// The far side's own store and the run it holds, when a sync is to be
    /// performed for real rather than recorded and skipped.
    far: Option<(&'a Store, &'a LoadedConfig)>,
    pub(crate) steps: Mutex<Vec<Step>>,
    pub(crate) placed: Mutex<Option<String>>,
    /// What the far `sima run` was started with about a changed program.
    pub(crate) started_with: Mutex<Option<BinaryChange>>,
    /// The objects the push named, which is what a program has to be in.
    pub(crate) pushed: Mutex<Vec<sima_core::Hash>>,
    /// What the far run's log holds, when it wrote one.
    log: Option<String>,
    /// Whether the far run exits the moment it is started, which is what a
    /// far-side load failure looks like from here.
    dies_at_start: bool,
    /// Whether the far run goes away once its feed has delivered everything
    /// it was scripted with: a run that died mid-flight, journaling nothing
    /// terminal.
    vanishing: bool,
    /// How many times the follow is refused before it opens: a far run is
    /// up before it journals, and `sima follow-serve` refuses a run that
    /// journaled nothing.
    follow_refusals: Mutex<usize>,
}

impl<'a> Scripted<'a> {
    /// A far side driving nothing, offering one card, with nothing to
    /// deliver.
    pub(crate) fn new() -> Scripted<'a> {
        Scripted {
            devices: vec![DeviceInfo {
                class: DeviceClass::new("10de:2684").expect("class id"),
                name: "NVIDIA GeForce RTX 4090".to_string(),
                device_type: DeviceType::Discrete,
                member: 0,
            }],
            alive: Arc::new(Mutex::new(None)),
            ending: Ending::OnInterrupt,
            refusals: Mutex::new(0),
            image_absent: false,
            unplaced: false,
            deaf: Mutex::new(0),
            polls: Arc::new(Mutex::new(VecDeque::new())),
            journal: Mutex::new(FarJournal::Absent),
            far: None,
            steps: Mutex::new(Vec::new()),
            placed: Mutex::new(None),
            started_with: Mutex::new(None),
            pushed: Mutex::new(Vec::new()),
            log: None,
            dies_at_start: false,
            vanishing: false,
            follow_refusals: Mutex::new(0),
        }
    }

    /// A far side already driving this run, which is what a reattach finds.
    pub(crate) fn already_driving(self) -> Scripted<'a> {
        *self.alive.lock().expect("the pid lock") = Some(PID);
        self
    }

    /// A far run that never exits, however it is asked to.
    pub(crate) fn stubborn(mut self) -> Scripted<'a> {
        self.ending = Ending::Never;
        self
    }

    /// A far run that keeps going through the whole wind-down and ends only
    /// when it is terminated.
    pub(crate) fn outlasting_the_wind_down(mut self) -> Scripted<'a> {
        self.ending = Ending::OnTermination;
        self
    }

    /// A machine nothing was ever migrated to: it holds no directory for
    /// this run.
    pub(crate) fn never_migrated_to(mut self) -> Scripted<'a> {
        self.unplaced = true;
        self
    }

    /// A machine that answers but does not hold the worker image.
    pub(crate) fn without_the_image(mut self) -> Scripted<'a> {
        self.image_absent = true;
        self
    }

    /// A machine that refuses its first `contacts` connections before it
    /// answers at all.
    pub(crate) fn refusing(self, contacts: usize) -> Scripted<'a> {
        *self.refusals.lock().expect("the refusal lock") = contacts;
        self
    }

    /// A far run that discards its first `signals` before it becomes
    /// signallable.
    pub(crate) fn deaf_for(self, signals: usize) -> Scripted<'a> {
        *self.deaf.lock().expect("the deafness lock") = signals;
        self
    }

    /// A far run that is up but has not journaled yet, so the follow is
    /// refused its first `attempts` times before it opens.
    pub(crate) fn refusing_the_follow(self, attempts: usize) -> Scripted<'a> {
        *self.follow_refusals.lock().expect("the follow lock") = attempts;
        self
    }

    /// A far run that exits before it journals anything, over a store that
    /// holds no journal at all, leaving `log` behind — what a first migration
    /// finds when the far side fails to load.
    pub(crate) fn dying_before_journaling(mut self, log: &str) -> Scripted<'a> {
        self.log = Some(log.to_string());
        self.dies_at_start = true;
        self
    }

    /// A far run that exits while loading, leaving `log` behind, over a store
    /// whose journal an earlier session already filled — what a second
    /// migration onto a run that once finished there finds.
    pub(crate) fn dying_while_loading(mut self, log: &str) -> Scripted<'a> {
        self.log = Some(log.to_string());
        self.dies_at_start = true;
        self
    }

    /// A far run that goes away once its feed has delivered everything it was
    /// scripted with, journaling nothing terminal: a death mid-run.
    pub(crate) fn vanishing_when_drained(mut self) -> Scripted<'a> {
        self.vanishing = true;
        self
    }

    /// The journal already in the far store: what a follow opened before this
    /// run starts replays, what a run that ended on this destination left
    /// behind, and what a recall reads to learn how it ended.
    pub(crate) fn over_an_existing_journal(self, records: Vec<Record>) -> Scripted<'a> {
        *self.journal.lock().expect("the journal lock") = FarJournal::Holding(records);
        self
    }

    /// A far side holding a journal it cannot serve, answering the read with
    /// `words` of its own.
    pub(crate) fn faulting_on_the_journal_read(self, words: &str) -> Scripted<'a> {
        *self.journal.lock().expect("the journal lock") = FarJournal::Faulting(words.to_string());
        self
    }

    /// The records the far journal holds, which a follow replays before
    /// anything live. A journal that is absent or unreadable replays nothing.
    fn journaled_records(&self) -> Vec<Record> {
        match &*self.journal.lock().expect("the journal lock") {
            FarJournal::Holding(records) => records.clone(),
            FarJournal::Absent | FarJournal::Faulting(_) => Vec::new(),
        }
    }

    /// The records the follow delivers, one batch per poll, from the second
    /// poll on.
    pub(crate) fn delivering(self, batches: Vec<Vec<Record>>) -> Scripted<'a> {
        *self.polls.lock().expect("the poll lock") = batches.into();
        self
    }

    /// The store a sync actually exchanges with, and the config whose run it
    /// holds. Without it a sync is recorded and nothing moves.
    pub(crate) fn syncing_with(
        mut self,
        store: &'a Store,
        config: &'a LoadedConfig,
    ) -> Scripted<'a> {
        self.far = Some((store, config));
        self
    }

    pub(crate) fn record(&self, step: Step) {
        self.steps.lock().expect("the step lock").push(step);
    }

    pub(crate) fn steps(&self) -> Vec<Step> {
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

    fn placed(&self) -> Result<bool> {
        self.record(Step::Placed);
        Ok(!self.unplaced)
    }

    fn driving(&self) -> Result<Option<u32>> {
        self.record(Step::Driving);
        Ok(*self.alive.lock().expect("the pid lock"))
    }

    fn start(&self, accept: BinaryChange) -> Result<u32> {
        self.record(Step::Start);
        *self.started_with.lock().expect("the acceptance lock") = Some(accept);
        // A run that dies while loading its config is gone by the time
        // anything asks, which is what leaves the pid naming nothing.
        if !self.dies_at_start {
            *self.alive.lock().expect("the pid lock") = Some(PID);
        }
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

    fn sync(&self, store: &Store, keys: &[TaskKey], scope: ObjectScope<'_>) -> Result<SyncReport> {
        self.record(match scope {
            ObjectScope::Named(named) => {
                *self.pushed.lock().expect("the push lock") = named.to_vec();
                PUSH
            }
            ObjectScope::Referenced => PULL,
        });
        match self.far {
            // The far side derives its own key set where it sits, as
            // `sima sync-serve` does over the run's journal; no key list
            // crosses the wire. This double holds the config, so it
            // derives the same set the far side's journal would name.
            Some((far, config)) => {
                let far_keys = task_keys(config, far)?;
                sync_between(store, keys, scope, far, &far_keys)
            }
            None => Ok(SyncReport::default()),
        }
    }

    fn snapshot(&self) -> Result<Option<Vec<Record>>> {
        self.record(Step::Snapshot);
        // The far store's journal as it stands, which is what `follow-serve
        // --once` writes out — and, for a journal the far side holds and
        // cannot serve, the words it answers with instead.
        match &*self.journal.lock().expect("the journal lock") {
            FarJournal::Absent => Ok(None),
            FarJournal::Holding(records) => Ok(Some(records.clone())),
            FarJournal::Faulting(words) => Err(Error::Reported(words.to_string())),
        }
    }

    fn log_tail(&self) -> Result<String> {
        self.record(Step::LogTail);
        Ok(self.log.clone().unwrap_or_default())
    }

    fn follow(&self) -> Result<Box<dyn RunFeed>> {
        self.record(Step::Follow);
        // `sima follow-serve` refuses a run that journaled nothing, which is
        // the whole of what this side can learn from it. The far journal holds
        // something when an earlier session left records or a run is there
        // writing its own; a run that died while loading left neither.
        let existing = self.journaled_records();
        let unjournaled = existing.is_empty() && self.alive.lock().expect("the pid lock").is_none();
        let mut refusals = self.follow_refusals.lock().expect("the follow lock");
        if unjournaled || *refusals > 0 {
            *refusals = refusals.saturating_sub(1);
            return Err(Error::Validation(
                "run 00 was never started in this store".to_string(),
            ));
        }
        Ok(Box::new(ScriptedFeed {
            info: FeedInfo {
                run: RunId::from_hash(sima_core::hash_bytes(b"scripted")),
                format: sima_model::FormatId::new("stub.v1").expect("format id"),
                workers: 1,
            },
            polls: Arc::clone(&self.polls),
            history: Some(existing),
            alive: Arc::clone(&self.alive),
            ending: self.ending,
            vanishing: self.vanishing,
        }))
    }
}

/// The feed a scripted far side hands out: one batch per poll, and the far
/// run ends when a terminal record is delivered.
pub(crate) struct ScriptedFeed {
    info: FeedInfo,
    polls: Arc<Mutex<VecDeque<Vec<Record>>>>,
    /// The journal as it already stood when the feed opened, delivered by its
    /// first poll — as [`RemoteFeed`](crate::feed::RemoteFeed) drains its own
    /// history before anything live. `None` once that poll has happened.
    history: Option<Vec<Record>>,
    alive: Arc<Mutex<Option<u32>>>,
    ending: Ending,
    /// Whether the far run goes away once every batch has been delivered.
    vanishing: bool,
}

impl RunFeed for ScriptedFeed {
    fn info(&self) -> &FeedInfo {
        &self.info
    }

    fn poll(&mut self) -> Result<Vec<Record>> {
        // The journal as it stood when the feed opened. An empty one is still
        // a poll of its own, as the real feed's first frame is.
        if let Some(history) = self.history.take()
            && !history.is_empty()
        {
            return Ok(history);
        }
        let batch = self
            .polls
            .lock()
            .expect("the poll lock")
            .pop_front()
            .unwrap_or_default();
        if self.vanishing && batch.is_empty() {
            // Everything it was scripted with has been delivered, and the far
            // run is gone without journaling anything terminal.
            *self.alive.lock().expect("the pid lock") = None;
        }
        let terminal = batch.iter().any(|record| {
            matches!(
                record.event,
                Event::RunFinalized { .. } | Event::RunFailed { .. } | Event::RunInterrupted { .. }
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
pub(crate) struct Local {
    _dir: TempDir,
    pub(crate) text: String,
    pub(crate) config: LoadedConfig,
    pub(crate) store: Store,
}

/// The run every session test moves: one candidate over twenty accumulating
/// segments, so the chain has a frontier at every stage.
pub(crate) fn config_text(machine: &str, root: &str, bounds: &str) -> String {
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
pub(crate) fn local(machine: &str, bounds: &str, committed: Option<usize>) -> Local {
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
pub(crate) const RENTED: &str = "provider = \"stub\"";
/// The declaration of a machine of yours.
pub(crate) const OWNED: &str = "workers = 1";
/// Readiness bounds a wind-down runs through without sleeping.
pub(crate) const PROMPT: &str = "ready_timeout_ms = 200\nready_poll_ms = 1";

/// A second store holding the same run, driven `committed` segments in —
/// the far side of a migration, as a real sync finds it.
pub(crate) fn far_store(config: &LoadedConfig, committed: Option<usize>) -> (TempDir, Store) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path()).expect("open the store");
    drive_run(&store, &config.run, committed);
    (dir, store)
}

// ---- Journal records the scripted far side delivers ----

pub(crate) fn rec(event: Event) -> Record {
    Record { ts_ms: 0, event }
}

pub(crate) fn started(run: &RunId) -> Record {
    rec(Event::RunStarted {
        run: run.to_string(),
        tasks: 20,
        committed: 0,
    })
}

pub(crate) fn committed(task: &str) -> Record {
    rec(Event::Committed {
        task: task.to_string(),
        record: "11".repeat(32),
        stats: Vec::new(),
        stats_blob_hex: String::new(),
    })
}

pub(crate) fn finalized(run: &RunId) -> Record {
    rec(Event::RunFinalized {
        run: run.to_string(),
        committed: 20,
    })
}

pub(crate) fn failed(run: &RunId, task: &str, reason: &str) -> Record {
    rec(Event::RunFailed {
        run: run.to_string(),
        task: task.to_string(),
        reason: reason.to_string(),
    })
}

// ---- A rented machine, provisioned and adopted ----

/// A stub marketplace of one generous offer.
pub(crate) fn marketplace() -> StubProvider {
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
pub(crate) fn limits() -> AcquireLimits {
    AcquireLimits {
        usable_by: Instant::now() + Duration::from_millis(500),
        ready_poll: Duration::ZERO,
    }
}

/// Spends this run past the ceiling its config declares, by booking a
/// closed rental it already paid for.
///
/// Budget exhaustion is what winds a migration down from this side, so it
/// is what every wind-down test drives: an interrupt lets go instead.
pub(crate) fn over_budget(local: &Local) -> Result<()> {
    local.store.put_spend(&SpendEntry {
        tag: "sima-prior-0".to_string(),
        provider: "stub".to_string(),
        owner: local.config.run.id().to_string(),
        price_micro_usd_hour: 100_000,
        started_ms: 1_700_000_000_000,
        ended_ms: 1_700_000_003_600_000,
        cost_micro_usd: 2_000_000,
    })
}

/// Rents one machine to host the run, as `hold` does when there is nothing
/// to adopt.
pub(crate) fn hosting<'a>(
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
