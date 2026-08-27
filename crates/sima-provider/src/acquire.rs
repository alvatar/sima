//! [`acquire`]: renting one machine, from the marketplace to a guard.
//!
//! The loop walks the ranked offers and treats a lost offer, and a machine
//! that never comes up, as reasons to try the next one. Only an API failure
//! aborts it: that failure would repeat against every remaining offer.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use sima_core::{Error, Result};
use sima_model::RunId;
use sima_store::{InstanceRecord, InstanceRecordState, Rental, RunLock, Store};

use crate::budget::{Budget, Exhaustion, Verdict, assess, now_ms};
use crate::guard::{InstanceGuard, teardown};
use crate::offer::{Constraints, Objective, Offer, select};
use crate::provider::{Instance, InstanceStatus, Provider, Provision, SshEndpoint};
use crate::reconcile::{ReconcileScope, reconcile};
use crate::reputation::{IncidentKind, excluded_machines, record_incident};

/// An acquisition nobody is narrating: what a caller with nothing to say
/// about the offer it took passes as `taken`.
pub static UNREPORTED: &(dyn Fn(&Offer) + Sync) = &|_: &Offer| ();

/// An acquisition nobody can call off: what a caller with no wind-down to
/// observe passes as `cancel`. The flag is never set, so the walk runs to its
/// own conclusion, and it is behind a call because nothing outside may reach
/// the flag to set it.
pub fn never_cancelled() -> &'static AtomicBool {
    static NEVER: AtomicBool = AtomicBool::new(false);
    &NEVER
}

/// The gate concurrent acquisitions over one store take their offers through.
///
/// Taking an offer is all the ledger writing an acquisition does — the orphan
/// reap that opens a walk, the budget read against the ledger, and the intent
/// and live records for the machine — and acquisitions that run at once share
/// the store those land in. Only the take is under the gate: the wait for the
/// machine to come up is where the minutes go, and it runs outside, which is
/// the whole point of holding one.
///
/// A caller acquiring alone builds its own and contends with nobody.
#[derive(Default)]
pub struct Admission(Mutex<()>);

impl Admission {
    /// A gate the acquisitions sharing one store take their offers through.
    pub fn new() -> Admission {
        Admission::default()
    }

    /// Enters the gate, waiting out whichever acquisition holds it.
    ///
    /// A gate a panicking take poisoned is entered all the same: what such a
    /// take left in the ledger is reconciliation's to clear, and refusing
    /// every acquisition after it would strand the run instead.
    fn enter(&self) -> MutexGuard<'_, ()> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Bounds on waiting for a provisioned instance to become ready.
#[derive(Debug, Clone, Copy)]
pub struct AcquireLimits {
    /// When the machine must be usable by. A deadline rather than a duration,
    /// because the readiness wait is one stage of a longer wait for the same
    /// machine — a caller that goes on to reach it runs that under this same
    /// deadline, so the machine gets one budget rather than one per stage.
    pub usable_by: Instant,
    /// How long to wait between status calls.
    pub ready_poll: Duration,
}

/// Distinguishes acquisition attempts made by one process.
static SEQ: AtomicU64 = AtomicU64::new(0);

/// Distinguishes this process's attempts from those of every other process,
/// including one the operating system gave the same pid to later.
///
/// Drawn once, from the operating system: a fresh `RandomState` takes its
/// keys from OS entropy, and the initial state of the hasher it builds is
/// those keys. The lower 32 bits are what the tag carries, as 8 hex
/// characters.
static NONCE: LazyLock<String> = LazyLock::new(|| {
    let seed = RandomState::new().build_hasher().finish();
    format!("{:08x}", seed as u32)
});

/// Rents one machine satisfying `constraints`, best by `objective`: list
/// the marketplace, rank it, then walk the ranked offers — write the intent
/// record, provision, upgrade the record, wait for readiness — falling
/// through to the next offer on a lost offer or a machine that never comes
/// up.
///
/// The returned guard owns the instance. An empty ranked list, and a list
/// walked to its end, are both [`Error::Provider`].
///
/// `lock` is the acquiring run's orchestrator lock, and taking it by
/// reference is how the caller's obligation is met: reconciliation reads a
/// held run lock as the owner still running, for this run and for every
/// other live run, so a run that rents a machine holds its lock for as long
/// as it holds the machine. The record's owner is stamped from the lock.
///
/// `budget` is what the run may still spend and how long its rental phase
/// may last. A budget already reached refuses the acquisition with
/// [`Error::Provider`], before any offer is asked for and again before each
/// attempt, so no money is committed past it. A caller that must tell
/// exhaustion from every other failure reads [`assess`] itself.
///
/// `cancel` aborts a walk in progress: it is read between ranked offers and
/// inside the readiness poll, so a caller winding down — the fleet supervisor
/// on interrupt or run teardown — abandons the acquisition promptly rather than
/// sitting out the deadline. Every offer in the walk shares the one deadline
/// `limits` carries, so a walk costs the budget for a usable machine once
/// however many candidates it tries. A cancellation tears down any machine
/// already provisioned and returns [`Error::Provider`] naming it. A caller with
/// nothing to cancel passes a never-set flag.
///
/// `taken` is called with each offer a machine has been provisioned against,
/// before the wait for that machine to come up: it is where a caller says what
/// is now being paid for, which is the one thing an operator cannot see while
/// the wait runs. A walk whose first machine never comes up calls it again for
/// the next offer it takes. A caller with nothing to say passes
/// [`UNREPORTED`].
///
/// `admission` serializes the ledger-writing half of this against every other
/// acquisition holding the same gate, so acquisitions run concurrently over one
/// store without racing on it. The readiness wait is deliberately outside it.
#[allow(clippy::too_many_arguments)]
pub fn acquire<'a, P: Provider + ?Sized>(
    provider: &'a P,
    store: &'a Store,
    lock: &RunLock,
    role: Rental,
    constraints: &Constraints,
    objective: Objective,
    limits: &AcquireLimits,
    budget: &Budget,
    admission: &Admission,
    cancel: &AtomicBool,
    taken: &dyn Fn(&Offer),
) -> Result<InstanceGuard<'a, P>> {
    let owner = lock.run();
    let mut constraints = constraints.clone();
    let ranked = {
        // Reaping, reading the budget, and reading the incident ledger all
        // touch the store the concurrent acquisitions share, so the walk is
        // ranked under the gate and taken under it offer by offer.
        let _taking = admission.enter();
        // Orphans of an earlier crash are destroyed before a new machine is
        // paid for. This comes before the budget check: destroying orphans
        // stops spending, which matters most when the budget is exhausted.
        reconcile(provider, store, ReconcileScope::Workers)?;
        // An exhausted budget refuses before the marketplace is even listed.
        admit(store, owner, budget)?;
        // Every offer selection, initial and every supervisor replacement,
        // flows through here, so deriving the excluded set once before
        // `select` covers both. The set is computed from the incident ledger,
        // never stored.
        constraints
            .excluded_machines
            .extend(excluded_machines(store, provider.id())?);
        select(provider.offers()?, &constraints, objective)
    };
    if ranked.is_empty() {
        return Err(Error::Provider(format!(
            "no offer satisfies the constraints {constraints:?}"
        )));
    }
    for offer in ranked {
        // Cancellation between offers abandons the walk before another machine
        // is paid for.
        if cancel.load(Ordering::Relaxed) {
            return Err(cancelled());
        }
        let (tag, instance) = match take(
            provider, store, owner, role, budget, &offer, admission, taken,
        )? {
            Took::Machine { tag, instance } => (tag, instance),
            // The offer went to another renter: the next-ranked one is the
            // acquisition's answer.
            Took::Gone => continue,
        };
        if let Some(endpoint) = wait_ready(provider, &instance, limits, cancel)? {
            return Ok(InstanceGuard::new(
                provider,
                store,
                tag,
                instance.id,
                endpoint,
                offer.machine.clone(),
                offer.gpu_model.clone(),
                offer.gpu_count,
                instance.price,
            ));
        }
        // A machine that never came up is a bad offer, not a fatal error;
        // a cancelled wait leaves the same pending machine. Either way it is
        // torn down. The record already carries the rate the provider named.
        teardown(provider, store, &tag, &instance.id, None)?;
        // A cancellation during the wait abandons the walk once the pending
        // machine is down, rather than moving on to the next offer.
        if cancel.load(Ordering::Relaxed) {
            return Err(cancelled());
        }
        // The wait ran out on a machine that never reported an endpoint (or one
        // that went gone while provisioning): an incident against the machine,
        // recorded now that a cancellation has been ruled out. A machine with
        // no identity records nothing.
        record_incident(
            store,
            provider.id(),
            &offer.machine,
            &tag,
            IncidentKind::NeverReady,
            now_ms(),
        )?;
    }
    Err(Error::Provider(
        "every qualifying offer was lost or failed to become ready".to_string(),
    ))
}

/// The error a cancelled acquisition returns, naming the cancellation so the
/// caller tells it from a market that simply had nothing.
fn cancelled() -> Error {
    Error::Provider("the acquisition was cancelled".to_string())
}

/// What taking one offer answered.
enum Took {
    /// The offer is this run's: a machine carrying `tag` exists and is coming
    /// up.
    Machine { tag: String, instance: Instance },
    /// Another renter has it, and nothing was written that outlives the
    /// attempt.
    Gone,
}

/// Takes `offer` under the admission gate: admits the spend, writes the intent
/// record, provisions the machine, and upgrades the record to the live one.
///
/// Everything the acquisition writes to the store for this machine happens
/// here, which is what makes the gate enough to keep concurrent acquisitions
/// off each other. The wait for the machine to come up is the caller's, outside
/// the gate.
#[allow(clippy::too_many_arguments)]
fn take<P: Provider + ?Sized>(
    provider: &P,
    store: &Store,
    owner: &RunId,
    role: Rental,
    budget: &Budget,
    offer: &Offer,
    admission: &Admission,
    taken: &dyn Fn(&Offer),
) -> Result<Took> {
    let _taking = admission.enter();
    // A machine that consumed the budget during a failed readiness wait
    // must not be followed by another rental.
    admit(store, owner, budget)?;
    let tag = attempt_tag(owner);
    // One stamp per attempt: the live write carries the intent's, which
    // is what the record's field states.
    let created_ms = now_ms();
    // Durable before the provider is asked for anything: the provider
    // attaches this tag to whatever it creates, so a death anywhere
    // after this line leaves a record naming the machine that may
    // exist. Without it, a crash between the call and its answer would
    // leak an instance nothing knows about.
    store.put_instance(&record(
        &tag,
        provider.id(),
        owner,
        offer,
        None,
        created_ms,
        role,
    ))?;
    // A death here leaves an intent record naming a tag no machine yet
    // carries: reconcile clears it, and nothing leaks.
    sima_core::crashpoint("provider.intent-written");
    let instance = match provider.provision(&offer.id, &tag) {
        Ok(Provision::Provisioned(instance)) => instance,
        Ok(Provision::OfferGone) => {
            store.remove_instance(&tag)?;
            return Ok(Took::Gone);
        }
        // An API failure repeats against every remaining offer, so it
        // aborts the walk. Its intent record stays: an error answer does
        // not say whether the request landed — a timeout can mean a
        // machine carrying the tag exists — and the record is the only
        // thing reconciliation acts on, so clearing it here would make
        // that machine unreachable.
        Err(e) => return Err(e),
    };
    // A death here leaves an intent record while a machine carrying its tag
    // exists: reconcile matches the tag and destroys the untracked
    // instance, so nothing leaks.
    sima_core::crashpoint("provider.provisioned");
    // The money starts here, whatever the readiness wait goes on to find.
    taken(offer);
    store.put_instance(&record(
        &tag,
        provider.id(),
        owner,
        offer,
        Some(&instance),
        created_ms,
        role,
    ))?;
    Ok(Took::Machine { tag, instance })
}

/// Admits one rental attempt, or refuses it naming the limit `owner`
/// reached and the numbers behind it.
///
/// The comparison is against what stands now: how long the rental being
/// admitted will run is unknowable here, so nothing is projected. Bounding
/// how far a running fleet may overshoot is the work of the caller that
/// polls [`assess`] while the fleet runs.
fn admit(store: &Store, owner: &RunId, budget: &Budget) -> Result<()> {
    match assess(store, owner, budget, now_ms())? {
        Verdict::Within { .. } => Ok(()),
        Verdict::Exhausted(Exhaustion::Spend { accrued, cap }) => Err(Error::Provider(format!(
            "the run's rental budget is exhausted: spent {} of {} micro-USD",
            accrued.0, cap.0
        ))),
        Verdict::Exhausted(Exhaustion::WallClock { deadline_ms }) => Err(Error::Provider(format!(
            "the run's rental budget is exhausted: the rental deadline (epoch ms {deadline_ms}) has passed"
        ))),
    }
}

/// Polls `instance` until it reports an endpoint, `None` when it is gone, the
/// deadline passes, or `cancel` is set. The deadline is an [`Instant`], so a
/// wall-clock adjustment cannot extend or cut the wait, and every offer in one
/// walk shares it: the budget is for getting a usable machine, not for each
/// candidate in turn. A cancellation returns `None` — the same "no endpoint"
/// the caller tears the pending machine down for — so the caller closes it out
/// on one path.
fn wait_ready<P: Provider + ?Sized>(
    provider: &P,
    instance: &Instance,
    limits: &AcquireLimits,
    cancel: &AtomicBool,
) -> Result<Option<SshEndpoint>> {
    loop {
        // Checked before each status call, so a cancellation set while the
        // machine is still provisioning abandons the wait promptly.
        if cancel.load(Ordering::Relaxed) {
            return Ok(None);
        }
        match provider.instance(&instance.id)? {
            InstanceStatus::Ready(endpoint) => return Ok(Some(endpoint)),
            InstanceStatus::Gone => return Ok(None),
            InstanceStatus::Provisioning => {
                if Instant::now() >= limits.usable_by {
                    return Ok(None);
                }
                std::thread::sleep(limits.ready_poll);
            }
        }
    }
}

/// The ledger record for one attempt: the intent record while `instance` is
/// `None`, the live record once the provider named the machine. Both writes
/// carry the attempt's single `created_ms` stamp.
#[allow(clippy::too_many_arguments)]
fn record(
    tag: &str,
    provider: &str,
    owner: &RunId,
    offer: &Offer,
    instance: Option<&Instance>,
    created_ms: u64,
    role: Rental,
) -> InstanceRecord {
    InstanceRecord {
        tag: tag.to_string(),
        provider: provider.to_string(),
        // The offer's machine at intent, carried unchanged by the live write.
        machine: offer.machine.clone(),
        owner: owner.to_string(),
        // Written at intent, so no window exists in which a hosting rental is
        // recorded as an ordinary one and reconciliation reaps it.
        role,
        state: match instance {
            Some(instance) => InstanceRecordState::Live {
                instance: instance.id.0.clone(),
            },
            None => InstanceRecordState::Intent,
        },
        // The offer's rate until the provider states the instance's own.
        price_micro_usd_hour: instance.map_or(offer.price, |instance| instance.price).0,
        created_ms,
    }
}

/// The tag one acquisition attempt runs under:
/// `sima-<owner16>-<pid>-<rand8hex>-<seq>`. It is both the ledger key and the
/// provider-side label, so the machine and its record carry one name. The
/// owner's first 16 hex characters keep it short enough for provider label
/// limits while staying attributable; the full owner lives in the record.
///
/// A tag is an operational identifier and nothing hashes it. The random
/// component is what makes it unrepeatable across restarts: a pid the
/// operating system recycles, together with a counter that starts at zero in
/// every process, would otherwise reproduce a tag an earlier process used.
/// A spend entry is keyed by the pair (tag, start stamp), so two rentals
/// share a key only where a reproduced tag meets a coinciding stamp — the
/// tag alone keeps that pair apart whatever the clock reads.
fn attempt_tag(owner: &RunId) -> String {
    let owner = owner.to_string();
    format!(
        "sima-{}-{}-{}-{}",
        &owner[..16],
        std::process::id(),
        *NONCE,
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::{Duration, Instant};

    use sima_core::{Error, Result};
    use sima_model::RunId;
    use sima_store::{
        IncidentKind, InstanceRecord, InstanceRecordState, MachineIncident, Rental, SpendEntry,
        Store,
    };

    use super::{
        AcquireLimits, Admission, Ordering, UNREPORTED, acquire, attempt_tag, never_cancelled,
    };
    use crate::budget::{Budget, Cost};
    use crate::guard::InstanceGuard;
    use crate::offer::{Constraints, Objective, Offer, OfferId, Price};
    use crate::provider::{InstanceId, InstanceStatus, Provider, Provision, TaggedInstance};
    use crate::reconcile::{ReconcileScope, reconcile};
    use crate::stub::StubProvider;
    use crate::testutil::{
        acquire_any, instance_record, live_state, prompt_limits, sample_run, spend_entries,
        stub_offer, temp_store,
    };

    /// A provider that watches what the acquisition loop does before and
    /// while it calls through: every `provision` asserts the attempt's
    /// intent record is already durable, and keeps the records it saw in
    /// call order.
    struct WatchingProvider {
        inner: StubProvider,
        root: PathBuf,
        observed_intents: Mutex<Vec<InstanceRecord>>,
        calls: AtomicUsize,
    }

    impl WatchingProvider {
        fn new(inner: StubProvider, root: PathBuf) -> WatchingProvider {
            WatchingProvider {
                inner,
                root,
                observed_intents: Mutex::new(Vec::new()),
                calls: AtomicUsize::new(0),
            }
        }

        /// How many provider calls the loop has made, of any kind.
        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        /// Counts one call through to the provider.
        fn counted<T>(&self, call: impl FnOnce() -> Result<T>) -> Result<T> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            call()
        }

        /// The intent records the loop wrote before calling through, in call
        /// order.
        fn observed_intents(&self) -> Vec<InstanceRecord> {
            self.observed_intents.lock().expect("intent lock").clone()
        }

        /// The tags the loop provisioned under, in call order.
        fn provisioned_tags(&self) -> Vec<String> {
            self.observed_intents()
                .into_iter()
                .map(|record| record.tag)
                .collect()
        }
    }

    impl Provider for WatchingProvider {
        fn id(&self) -> &'static str {
            self.inner.id()
        }

        fn offers(&self) -> Result<Vec<Offer>> {
            self.counted(|| self.inner.offers())
        }

        fn provision(&self, offer: &OfferId, tag: &str) -> Result<Provision> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let store = Store::open(&self.root).expect("open the store the loop writes to");
            let records = store.instance_records().expect("list the ledger");
            let intent = records
                .iter()
                .find(|record| record.tag == tag)
                .unwrap_or_else(|| panic!("the intent record for {tag} must precede this call"));
            assert_eq!(intent.state, InstanceRecordState::Intent);
            self.observed_intents
                .lock()
                .expect("intent lock")
                .push(intent.clone());
            // Pushes the live write into a later millisecond, so a record
            // stamped twice is visible as two different stamps.
            std::thread::sleep(Duration::from_millis(2));
            self.inner.provision(offer, tag)
        }

        fn instance(&self, id: &InstanceId) -> Result<InstanceStatus> {
            self.counted(|| self.inner.instance(id))
        }

        fn instances(&self) -> Result<Vec<TaggedInstance>> {
            self.counted(|| self.inner.instances())
        }

        fn destroy(&self, id: &InstanceId) -> Result<()> {
            self.counted(|| self.inner.destroy(id))
        }
    }

    #[test]
    fn a_rented_machine_comes_back_as_a_guard_over_one_live_record() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)]);
        let guard = acquire_any(&stub, &store)?;
        assert_eq!(guard.endpoint().port, 22);
        assert_eq!(guard.endpoint().user, "root");
        let records = store.instance_records()?;
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.provider, "stub");
        assert_eq!(record.owner, sample_run(7).to_string());
        assert_eq!(record.price_micro_usd_hour, 100_000);
        // The record and the guard both carry the offer's machine, so a later
        // incident can be attributed to it.
        assert_eq!(record.machine, "m-cheap");
        assert_eq!(guard.machine(), "m-cheap");
        // The live state names the machine the guard holds.
        assert_eq!(record.instance(), Some(guard.id().0.as_str()));
        assert_eq!(record.tag, guard.tag());
        // The tag names the owner, the acquiring process, that process's
        // random component, and the attempt.
        assert_parts(&record.tag);
        Ok(())
    }

    #[test]
    fn acquisitions_sharing_a_gate_each_come_back_with_their_own_machine() -> Result<()> {
        // Members of one rental acquire at once, over one store and one run
        // lock. Their takes pass through the gate one at a time, so each ends
        // up on an offer of its own and the ledger holds one live record per
        // machine — no two attempts under one tag, and no offer taken twice.
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(vec![
            stub_offer("cheap", 100_000),
            stub_offer("dear", 200_000),
        ]);
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let admission = Admission::new();
        let guards: Vec<InstanceGuard<'_, StubProvider>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    scope.spawn(|| {
                        acquire(
                            &stub,
                            &store,
                            &lock,
                            Rental::Worker,
                            &Constraints::default(),
                            Objective::CheapestPerHour,
                            &prompt_limits(),
                            &Budget::default(),
                            &admission,
                            never_cancelled(),
                            UNREPORTED,
                        )
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("an acquiring thread joins"))
                .collect::<Result<Vec<_>>>()
        })?;
        let mut machines: Vec<&str> = guards.iter().map(InstanceGuard::machine).collect();
        machines.sort_unstable();
        assert_eq!(machines, vec!["m-cheap", "m-dear"]);
        let mut tags: Vec<String> = store
            .instance_records()?
            .into_iter()
            .map(|record| record.tag)
            .collect();
        tags.sort();
        tags.dedup();
        assert_eq!(tags.len(), 2, "one record per machine, under its own tag");
        Ok(())
    }

    /// Asserts that `tag` has the documented shape —
    /// `sima-<owner16>-<pid>-<rand8hex>-<seq>` over the owner these tests
    /// acquire under — and returns its parts.
    fn assert_parts(tag: &str) -> Vec<&str> {
        // Providers label instances with it, so it stays within the
        // conservative alphanumeric-and-hyphen charset.
        assert!(
            tag.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
            "a tag is a provider label: {tag}"
        );
        let parts: Vec<&str> = tag.split('-').collect();
        assert_eq!(parts.len(), 5, "{tag}");
        assert_eq!(parts[0], "sima");
        assert_eq!(parts[1], &sample_run(7).to_string()[..16]);
        assert_eq!(parts[2], std::process::id().to_string());
        assert_eq!(parts[3].len(), 8, "the random component: {tag}");
        assert!(
            parts[3]
                .chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
            "the random component is lowercase hex: {tag}"
        );
        assert!(parts[4].parse::<u64>().is_ok(), "the attempt counter");
        parts
    }

    #[test]
    fn two_tags_of_one_process_share_its_random_component_and_differ_by_attempt() {
        let owner = sample_run(7);
        let first = attempt_tag(&owner);
        let second = attempt_tag(&owner);
        let first = assert_parts(&first);
        let second = assert_parts(&second);
        // The random component is drawn once per process, so it is the
        // attempt counter alone that separates two tags of one process.
        assert_eq!(first[3], second[3]);
        assert_ne!(first[4], second[4]);
    }

    #[test]
    fn a_lost_offer_falls_through_to_the_next_ranked_one() -> Result<()> {
        let (_dir, store) = temp_store();
        let cheap = stub_offer("cheap", 100_000);
        let dearer = stub_offer("dearer", 200_000);
        let stub = StubProvider::new(vec![cheap.clone(), dearer])
            .gone_at_provision(OfferId("cheap".to_string()));
        let guard = acquire_any(&stub, &store)?;
        let records = store.instance_records()?;
        // The lost attempt left nothing behind; the taken one is the only
        // record.
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].price_micro_usd_hour, 200_000);
        assert_eq!(records[0].tag, guard.tag());
        assert!(stub.destroyed().is_empty());
        Ok(())
    }

    #[test]
    fn a_machine_that_never_comes_up_is_destroyed_and_the_next_offer_taken() -> Result<()> {
        let (_dir, store) = temp_store();
        let stalling = stub_offer("cheap", 100_000);
        let stub = StubProvider::new(vec![stalling.clone(), stub_offer("dearer", 200_000)])
            .never_ready(stalling.id.clone());
        let limits = AcquireLimits {
            usable_by: Instant::now(),
            ready_poll: Duration::ZERO,
        };
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let guard = acquire(
            &stub,
            &store,
            &lock,
            Rental::Worker,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &limits,
            &Budget::default(),
            &Admission::new(),
            never_cancelled(),
            UNREPORTED,
        )?;
        let records = store.instance_records()?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].price_micro_usd_hour, 200_000);
        assert_eq!(records[0].tag, guard.tag());
        // The abandoned machine was taken down, not left running.
        assert_eq!(stub.destroyed().len(), 1);
        assert_ne!(stub.destroyed()[0], *guard.id());
        Ok(())
    }

    #[test]
    fn a_machine_that_never_comes_up_is_closed_out_before_the_next_offer() -> Result<()> {
        let (_dir, store) = temp_store();
        let stalling = stub_offer("cheap", 100_000);
        let stub = StubProvider::new(vec![stalling.clone(), stub_offer("dearer", 200_000)])
            .never_ready(stalling.id.clone());
        let limits = AcquireLimits {
            usable_by: Instant::now(),
            ready_poll: Duration::ZERO,
        };
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let guard = acquire(
            &stub,
            &store,
            &lock,
            Rental::Worker,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &limits,
            &Budget::default(),
            &Admission::new(),
            never_cancelled(),
            UNREPORTED,
        )?;
        // The abandoned machine ran and was billed for, so the walk that
        // moved past it left its cost behind.
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        assert_ne!(entries[0].tag, guard.tag());
        assert_eq!(entries[0].price_micro_usd_hour, 100_000);
        Ok(())
    }

    #[test]
    fn a_machine_that_never_comes_up_records_one_never_ready_incident() -> Result<()> {
        let (_dir, store) = temp_store();
        let stalling = stub_offer("cheap", 100_000);
        let stub = StubProvider::new(vec![stalling.clone(), stub_offer("dearer", 200_000)])
            .never_ready(stalling.id.clone());
        let limits = AcquireLimits {
            usable_by: Instant::now(),
            ready_poll: Duration::ZERO,
        };
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let guard = acquire(
            &stub,
            &store,
            &lock,
            Rental::Worker,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &limits,
            &Budget::default(),
            &Admission::new(),
            never_cancelled(),
            UNREPORTED,
        )?;
        // The abandoned machine left one incident naming its own machine and
        // the attempt that observed it; the taken machine left none.
        let incidents = store.machine_incidents()?;
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].kind, sima_store::IncidentKind::NeverReady);
        assert_eq!(incidents[0].machine, "m-cheap");
        assert_ne!(incidents[0].tag, guard.tag());
        Ok(())
    }

    #[test]
    fn a_never_ready_machine_with_no_identity_records_nothing() -> Result<()> {
        let (_dir, store) = temp_store();
        // A provider reporting no machine identifier normalizes it to empty;
        // an empty machine is never blacklisted, so it records no incident.
        let stalling = Offer {
            machine: String::new(),
            ..stub_offer("cheap", 100_000)
        };
        let stub = StubProvider::new(vec![stalling.clone(), stub_offer("dearer", 200_000)])
            .never_ready(stalling.id.clone());
        let limits = AcquireLimits {
            usable_by: Instant::now(),
            ready_poll: Duration::ZERO,
        };
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let _guard = acquire(
            &stub,
            &store,
            &lock,
            Rental::Worker,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &limits,
            &Budget::default(),
            &Admission::new(),
            never_cancelled(),
            UNREPORTED,
        )?;
        assert!(store.machine_incidents()?.is_empty());
        Ok(())
    }

    /// Records `count` `Lost` incidents against `machine` under `provider`.
    fn strike(store: &Store, provider: &str, machine: &str, count: usize) {
        for n in 0..count {
            store
                .put_machine_incident(&MachineIncident {
                    provider: provider.to_string(),
                    machine: machine.to_string(),
                    kind: IncidentKind::Lost,
                    tag: format!("sima-strike-{n}"),
                    occurred_ms: n as u64,
                })
                .expect("record a strike");
        }
    }

    #[test]
    fn a_machine_with_two_incidents_is_excluded_and_the_next_offer_taken() -> Result<()> {
        let (_dir, store) = temp_store();
        // The cheapest offer's machine already holds two strikes.
        strike(&store, "stub", "m-cheap", 2);
        let stub = StubProvider::new(vec![
            stub_offer("cheap", 100_000),
            stub_offer("dearer", 200_000),
        ]);
        let guard = acquire_any(&stub, &store)?;
        let records = store.instance_records()?;
        assert_eq!(records.len(), 1);
        // The blacklisted machine was skipped for the dearer, clean one; the
        // cheapest offer was never even provisioned.
        assert_eq!(records[0].machine, "m-dearer");
        assert_eq!(records[0].price_micro_usd_hour, 200_000);
        assert_eq!(records[0].tag, guard.tag());
        assert!(stub.destroyed().is_empty());
        Ok(())
    }

    #[test]
    fn a_machine_with_one_incident_is_still_rented() -> Result<()> {
        let (_dir, store) = temp_store();
        // One strike is below the threshold: the cheapest machine is tolerated.
        strike(&store, "stub", "m-cheap", 1);
        let stub = StubProvider::new(vec![
            stub_offer("cheap", 100_000),
            stub_offer("dearer", 200_000),
        ]);
        let guard = acquire_any(&stub, &store)?;
        assert_eq!(store.instance_records()?[0].machine, "m-cheap");
        assert_eq!(guard.machine(), "m-cheap");
        Ok(())
    }

    #[test]
    fn two_incidents_under_a_different_provider_exclude_nothing() -> Result<()> {
        let (_dir, store) = temp_store();
        // The strikes are another provider's; the stub's cheapest is untouched.
        strike(&store, "vastai", "m-cheap", 2);
        let stub = StubProvider::new(vec![
            stub_offer("cheap", 100_000),
            stub_offer("dearer", 200_000),
        ]);
        let guard = acquire_any(&stub, &store)?;
        assert_eq!(store.instance_records()?[0].machine, "m-cheap");
        assert_eq!(guard.machine(), "m-cheap");
        Ok(())
    }

    #[test]
    fn a_machine_that_keeps_failing_is_refused_once_it_has_two_strikes() -> Result<()> {
        // The end-to-end shape: a physical machine relisted across three
        // acquisitions records a strike each time it never comes up, and is
        // refused the third time — recorded, derived, and excluded through
        // `acquire` alone.
        let (_dir, store) = temp_store();
        let limits = AcquireLimits {
            usable_by: Instant::now(),
            ready_poll: Duration::ZERO,
        };
        // One flaky offer sharing the machine `m-flaky` and a distinct clean
        // fallback per round, as a marketplace relisting one machine looks.
        let round = |flaky_id: &str, clean_id: &str| -> StubProvider {
            let flaky = Offer {
                machine: "m-flaky".to_string(),
                ..stub_offer(flaky_id, 100_000)
            };
            StubProvider::new(vec![flaky.clone(), stub_offer(clean_id, 200_000)])
                .never_ready(flaky.id.clone())
        };
        let rent = |stub: &StubProvider| -> Result<InstanceRecord> {
            let lock = store.acquire_run_lock(&sample_run(7))?;
            let guard = acquire(
                stub,
                &store,
                &lock,
                Rental::Worker,
                &Constraints::default(),
                Objective::CheapestPerHour,
                &limits,
                &Budget::default(),
                &Admission::new(),
                never_cancelled(),
                UNREPORTED,
            )?;
            Ok(store
                .instance_record(guard.tag())?
                .expect("the rented record's tag"))
        };
        let stub1 = round("f1", "c1");
        let stub2 = round("f2", "c2");
        let stub3 = round("f3", "c3");
        // The first two rounds each rent the clean fallback and leave a strike.
        assert_eq!(rent(&stub1)?.machine, "m-c1");
        assert_eq!(rent(&stub2)?.machine, "m-c2");
        assert_eq!(store.machine_incidents()?.len(), 2);
        // The third round finds `m-flaky` blacklisted: its offer is never
        // provisioned, so the clean fallback is taken and no strike is added.
        assert_eq!(rent(&stub3)?.machine, "m-c3");
        assert_eq!(store.machine_incidents()?.len(), 2);
        Ok(())
    }

    #[test]
    fn a_successful_acquisition_records_no_incident() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)]);
        let _guard = acquire_any(&stub, &store)?;
        assert!(store.machine_incidents()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_lost_offer_records_no_incident() -> Result<()> {
        let (_dir, store) = temp_store();
        let cheap = stub_offer("cheap", 100_000);
        let stub = StubProvider::new(vec![cheap.clone(), stub_offer("dearer", 200_000)])
            .gone_at_provision(OfferId("cheap".to_string()));
        let _guard = acquire_any(&stub, &store)?;
        // An offer another renter took is not the machine's failure: the
        // provider answered that no machine of ours exists.
        assert!(store.machine_incidents()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_lost_offer_leaves_no_spend_entry() -> Result<()> {
        let (_dir, store) = temp_store();
        let cheap = stub_offer("cheap", 100_000);
        let stub = StubProvider::new(vec![cheap.clone(), stub_offer("dearer", 200_000)])
            .gone_at_provision(OfferId("cheap".to_string()));
        let guard = acquire_any(&stub, &store)?;
        // The provider itself answered that no machine exists, which is the
        // one clear that owes nothing.
        assert!(spend_entries(&store, &sample_run(7))?.is_empty());
        drop(guard);
        Ok(())
    }

    /// Rents over `provider` under `budget`, with limits that poll without
    /// waiting.
    fn acquire_within<'a, P: Provider>(
        provider: &'a P,
        store: &'a Store,
        budget: &Budget,
    ) -> Result<InstanceGuard<'a, P>> {
        let lock = store.acquire_run_lock(&sample_run(7))?;
        acquire(
            provider,
            store,
            &lock,
            Rental::Worker,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &prompt_limits(),
            budget,
            &Admission::new(),
            never_cancelled(),
            UNREPORTED,
        )
    }

    /// A closed rental of `owner` costing `cost`, started at `started_ms`.
    fn spent(owner: &RunId, started_ms: u64, cost: u64) -> SpendEntry {
        SpendEntry {
            tag: "sima-spent-0".to_string(),
            provider: "stub".to_string(),
            owner: owner.to_string(),
            price_micro_usd_hour: 100_000,
            started_ms,
            ended_ms: started_ms + 3_600_000,
            cost_micro_usd: cost,
        }
    }

    #[test]
    fn a_budget_out_of_money_refuses_without_calling_the_provider() -> Result<()> {
        let (dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)]);
        let watching = WatchingProvider::new(stub, dir.path().to_path_buf());
        // The instance ledger is empty, so reconciliation reaches no
        // provider API either: the refusal costs nothing at all.
        store.put_spend(&spent(&sample_run(7), 1_700_000_000_000, 120_000))?;
        let budget = Budget {
            max_spend: Some(Cost(100_000)),
            ..Budget::default()
        };
        assert!(matches!(
            acquire_within(&watching, &store, &budget),
            Err(Error::Provider(message))
                if message == "the run's rental budget is exhausted: spent 120000 of 100000 micro-USD"
        ));
        assert_eq!(watching.calls(), 0);
        assert!(store.instance_records()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_budget_out_of_time_refuses_without_calling_the_provider() -> Result<()> {
        let (dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)]);
        let watching = WatchingProvider::new(stub, dir.path().to_path_buf());
        // The first rental anchored the phase far enough back that its
        // deadline is behind any clock this test could read.
        store.put_spend(&spent(&sample_run(7), 0, 0))?;
        let budget = Budget {
            max_wall_clock: Some(Duration::from_millis(1)),
            ..Budget::default()
        };
        assert!(matches!(
            acquire_within(&watching, &store, &budget),
            Err(Error::Provider(message))
                if message == "the run's rental budget is exhausted: the rental deadline (epoch ms 1) has passed"
        ));
        assert_eq!(watching.calls(), 0);
        assert!(store.instance_records()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_budget_the_first_attempt_consumes_refuses_the_second() -> Result<()> {
        let (dir, store) = temp_store();
        let stalling = stub_offer("cheap", 100_000);
        let stub = StubProvider::new(vec![stalling.clone(), stub_offer("dearer", 200_000)])
            .never_ready(stalling.id.clone())
            // A rate that consumes the cap within a millisecond of running.
            .charging_instances_at(Price(u64::MAX / 2));
        let watching = WatchingProvider::new(stub, dir.path().to_path_buf());
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let budget = Budget {
            max_spend: Some(Cost(1)),
            ..Budget::default()
        };
        let outcome = acquire(
            &watching,
            &store,
            &lock,
            Rental::Worker,
            &Constraints::default(),
            Objective::CheapestPerHour,
            // At least one poll sleeps, so the abandoned machine's charged
            // window is never empty.
            &AcquireLimits {
                usable_by: Instant::now() + Duration::from_millis(1),
                ready_poll: Duration::from_millis(1),
            },
            &budget,
            &Admission::new(),
            never_cancelled(),
            UNREPORTED,
        );
        assert!(matches!(
            outcome,
            Err(Error::Provider(message))
                if message.starts_with("the run's rental budget is exhausted: spent ")
        ));
        // The second offer was never provisioned: what the first machine
        // cost is already past the cap.
        assert_eq!(watching.provisioned_tags().len(), 1);
        assert!(store.instance_records()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_machine_ready_only_after_several_polls_is_waited_out_and_rented() -> Result<()> {
        let (_dir, store) = temp_store();
        // Two `Provisioning` answers before readiness, so the wait repolls
        // instead of settling on the first status call.
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)]).ready_after(2);
        let guard = acquire_any(&stub, &store)?;
        assert_eq!(guard.endpoint().port, 22);
        // The machine was waited for, not abandoned.
        assert!(stub.destroyed().is_empty());
        let records = store.instance_records()?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].instance(), Some(guard.id().0.as_str()));
        Ok(())
    }

    #[test]
    fn a_machine_still_provisioning_when_the_wait_elapses_is_abandoned() -> Result<()> {
        let (_dir, store) = temp_store();
        let stalling = stub_offer("cheap", 100_000);
        let stub = StubProvider::new(vec![stalling.clone(), stub_offer("dearer", 200_000)])
            .never_ready(stalling.id.clone());
        // A window the wait reaches by elapsed time, over several polls, and
        // short enough to keep the suite quick.
        let limits = AcquireLimits {
            usable_by: Instant::now() + Duration::from_millis(10),
            ready_poll: Duration::from_millis(1),
        };
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let guard = acquire(
            &stub,
            &store,
            &lock,
            Rental::Worker,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &limits,
            &Budget::default(),
            &Admission::new(),
            never_cancelled(),
            UNREPORTED,
        )?;
        let records = store.instance_records()?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].price_micro_usd_hour, 200_000);
        assert_eq!(records[0].tag, guard.tag());
        // The stalling machine was taken down before the next offer was
        // rented.
        assert_eq!(stub.destroyed().len(), 1);
        assert_ne!(stub.destroyed()[0], *guard.id());
        Ok(())
    }

    #[test]
    fn losing_every_offer_is_a_provider_error_over_a_clean_ledger() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("a", 100_000), stub_offer("b", 200_000)])
            .gone_at_provision(OfferId("a".to_string()))
            .gone_at_provision(OfferId("b".to_string()));
        assert!(matches!(
            acquire_any(&stub, &store),
            Err(Error::Provider(_))
        ));
        assert!(store.instance_records()?.is_empty());
        assert!(stub.live().is_empty());
        Ok(())
    }

    #[test]
    fn constraints_no_offer_meets_are_a_provider_error_before_any_rental() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("modest", 100_000)]);
        let constraints = Constraints {
            min_vram_mb: Some(1_000_000),
            ..Constraints::default()
        };
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let outcome = acquire(
            &stub,
            &store,
            &lock,
            Rental::Worker,
            &constraints,
            Objective::CheapestPerHour,
            &prompt_limits(),
            &Budget::default(),
            &Admission::new(),
            never_cancelled(),
            UNREPORTED,
        );
        assert!(matches!(outcome, Err(Error::Provider(_))));
        // Nothing was rented, so nothing is owed.
        assert!(stub.instances()?.is_empty());
        assert!(store.instance_records()?.is_empty());
        Ok(())
    }

    /// Aborts one acquisition on a failing provision call, returning the
    /// tag of the intent record it left in the ledger.
    fn aborted_attempt_tag(store: &Store) -> Result<String> {
        let stub = StubProvider::new(vec![stub_offer("a", 100_000)])
            .failing_provision("create instance: 500");
        assert!(acquire_any(&stub, store).is_err());
        let records = store.instance_records()?;
        assert_eq!(records.len(), 1);
        Ok(records[0].tag.clone())
    }

    #[test]
    fn an_api_failure_aborts_the_loop_after_one_attempt() -> Result<()> {
        let (dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("a", 100_000), stub_offer("b", 200_000)])
            .failing_provision("create instance: 500");
        let watching = WatchingProvider::new(stub, dir.path().to_path_buf());
        let outcome = acquire_any(&watching, &store);
        assert!(matches!(
            outcome,
            Err(Error::Provider(message)) if message == "create instance: 500"
        ));
        // The failure would repeat against the second offer, so the loop
        // never reaches it.
        assert_eq!(watching.provisioned_tags().len(), 1);
        // The attempt's intent record survives: the error says nothing about
        // whether the request landed, so the machine that may carry the tag
        // stays discoverable.
        let records = store.instance_records()?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].state, InstanceRecordState::Intent);
        assert_eq!(records[0].tag, watching.provisioned_tags()[0]);
        Ok(())
    }

    #[test]
    fn the_record_an_api_failure_left_is_cleared_by_reconciliation() -> Result<()> {
        let (_dir, store) = temp_store();
        let tag = aborted_attempt_tag(&store)?;
        // The owner holds no lock, and the provider created nothing under
        // the tag, so the record is all there was to clean up.
        let provider = StubProvider::new(Vec::new());
        let report = reconcile(&provider, &store, ReconcileScope::Workers)?;
        assert!(report.destroyed.is_empty());
        assert_eq!(report.cleared, vec![tag.clone()]);
        assert!(store.instance_records()?.is_empty());
        // The attempt is charged: the failure says nothing about whether a
        // machine was created, and an overcounted phantom is the safe
        // direction to be wrong in.
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tag, tag);
        Ok(())
    }

    #[test]
    fn a_machine_an_api_failure_may_have_created_is_reconciled_away() -> Result<()> {
        let (_dir, store) = temp_store();
        let tag = aborted_attempt_tag(&store)?;
        // The request had landed after all: the provider holds a machine
        // under the attempt's tag, which the intent record leads to.
        let landed = InstanceId("stub-landed".to_string());
        let provider = StubProvider::new(Vec::new()).with_instance(landed.clone(), &tag);
        let report = reconcile(&provider, &store, ReconcileScope::Workers)?;
        assert_eq!(report.destroyed, vec![landed]);
        assert_eq!(report.cleared, vec![tag]);
        assert!(provider.live().is_empty());
        assert!(store.instance_records()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_record_of_the_acquiring_run_survives_the_acquire_time_reconcile() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)])
            .with_instance(InstanceId("held".to_string()), "sima-tag-0");
        store.put_instance(&instance_record(
            "sima-tag-0",
            live_state("held"),
            sample_run(7),
        ))?;
        // The lock the acquisition runs under is the acquiring run's, so its
        // own earlier record reads as owned by a running orchestrator.
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let guard = acquire(
            &stub,
            &store,
            &lock,
            Rental::Worker,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &prompt_limits(),
            &Budget::default(),
            &Admission::new(),
            never_cancelled(),
            UNREPORTED,
        )?;
        assert!(stub.destroyed().is_empty());
        let tags: Vec<String> = store
            .instance_records()?
            .into_iter()
            .map(|record| record.tag)
            .collect();
        assert!(tags.contains(&"sima-tag-0".to_string()));
        assert!(tags.contains(&guard.tag().to_string()));
        Ok(())
    }

    #[test]
    fn the_intent_record_is_durable_before_the_provider_is_called() -> Result<()> {
        let (dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)]);
        let watching = WatchingProvider::new(stub, dir.path().to_path_buf());
        // The provider asserts the record's presence from a separately
        // opened store, which is what a later process would find.
        let guard = acquire_any(&watching, &store)?;
        assert_eq!(watching.provisioned_tags(), vec![guard.tag().to_string()]);
        Ok(())
    }

    #[test]
    fn an_attempt_is_stamped_once_and_the_live_write_keeps_that_stamp() -> Result<()> {
        let (dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)]);
        let watching = WatchingProvider::new(stub, dir.path().to_path_buf());
        let _guard = acquire_any(&watching, &store)?;
        let intent = watching.observed_intents();
        assert_eq!(intent.len(), 1);
        let records = store.instance_records()?;
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0].state, InstanceRecordState::Live { .. }));
        // The record is stamped at intent, which is what its field says.
        assert_eq!(records[0].created_ms, intent[0].created_ms);
        Ok(())
    }

    /// A provider that sets a cancellation flag the moment a readiness poll
    /// reports `Provisioning`, so a wait entered against a not-yet-ready
    /// machine is cancelled from within the poll loop rather than by racing a
    /// background thread.
    struct CancellingProvider<'a> {
        inner: StubProvider,
        cancel: &'a AtomicBool,
    }

    impl Provider for CancellingProvider<'_> {
        fn id(&self) -> &'static str {
            self.inner.id()
        }

        fn offers(&self) -> Result<Vec<Offer>> {
            self.inner.offers()
        }

        fn provision(&self, offer: &OfferId, tag: &str) -> Result<Provision> {
            self.inner.provision(offer, tag)
        }

        fn instance(&self, id: &InstanceId) -> Result<InstanceStatus> {
            let status = self.inner.instance(id)?;
            if matches!(status, InstanceStatus::Provisioning) {
                self.cancel.store(true, Ordering::Relaxed);
            }
            Ok(status)
        }

        fn instances(&self) -> Result<Vec<TaggedInstance>> {
            self.inner.instances()
        }

        fn destroy(&self, id: &InstanceId) -> Result<()> {
            self.inner.destroy(id)
        }
    }

    #[test]
    fn a_cancellation_set_before_the_walk_provisions_nothing() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)]);
        let cancel = AtomicBool::new(true);
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let outcome = acquire(
            &stub,
            &store,
            &lock,
            Rental::Worker,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &prompt_limits(),
            &Budget::default(),
            &Admission::new(),
            &cancel,
            UNREPORTED,
        );
        assert!(matches!(
            outcome,
            Err(Error::Provider(message)) if message.contains("cancelled")
        ));
        // Nothing was rented: the walk stopped before the first provision.
        assert!(store.instance_records()?.is_empty());
        assert!(stub.live().is_empty());
        Ok(())
    }

    #[test]
    fn a_cancellation_during_the_readiness_wait_tears_the_pending_machine_down() -> Result<()> {
        let (_dir, store) = temp_store();
        let stalling = stub_offer("cheap", 100_000);
        let inner = StubProvider::new(vec![stalling.clone()]).never_ready(stalling.id.clone());
        let cancel = AtomicBool::new(false);
        let provider = CancellingProvider {
            inner,
            cancel: &cancel,
        };
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let outcome = acquire(
            &provider,
            &store,
            &lock,
            Rental::Worker,
            &Constraints::default(),
            Objective::CheapestPerHour,
            // A generous window the wait would sit through if it were not
            // cancelled from within the poll.
            &AcquireLimits {
                usable_by: Instant::now() + Duration::from_secs(5),
                ready_poll: Duration::from_millis(1),
            },
            &Budget::default(),
            &Admission::new(),
            &cancel,
            UNREPORTED,
        );
        assert!(matches!(
            outcome,
            Err(Error::Provider(message)) if message.contains("cancelled")
        ));
        // The machine that was provisioned before the cancellation is torn
        // down, and its ledger record is cleared — nothing leaks.
        assert_eq!(
            provider.inner.destroyed().len(),
            1,
            "the pending machine is torn down"
        );
        assert!(provider.inner.live().is_empty());
        assert!(store.instance_records()?.is_empty());
        // A cancelled wait is our wind-down, not the machine's fault, so it
        // records no incident.
        assert!(store.machine_incidents()?.is_empty());
        Ok(())
    }
}
