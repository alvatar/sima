//! [`acquire`]: renting one machine, from the marketplace to a guard.
//!
//! The loop walks the ranked offers and treats a lost offer, and a machine
//! that never comes up, as reasons to try the next one. Only an API failure
//! aborts it: that failure would repeat against every remaining offer.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sima_core::{Error, Result};
use sima_model::RunId;
use sima_store::{InstanceRecord, InstanceRecordState, RunLock, Store};

use crate::guard::{InstanceGuard, teardown};
use crate::offer::{Constraints, Objective, Offer, select};
use crate::provider::{Instance, InstanceStatus, Provider, Provision, SshEndpoint};
use crate::reconcile::reconcile;

/// Bounds on waiting for a provisioned instance to become ready.
#[derive(Debug, Clone, Copy)]
pub struct AcquireLimits {
    /// How long a machine may take to report itself ready before the offer
    /// is abandoned.
    pub ready_timeout: Duration,
    /// How long to wait between status calls.
    pub ready_poll: Duration,
}

/// Distinguishes acquisition attempts made by one process.
static SEQ: AtomicU64 = AtomicU64::new(0);

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
pub fn acquire<'a, P: Provider>(
    provider: &'a P,
    store: &'a Store,
    lock: &RunLock,
    constraints: &Constraints,
    objective: Objective,
    limits: &AcquireLimits,
) -> Result<InstanceGuard<'a, P>> {
    let owner = lock.run();
    // Orphans of an earlier crash are destroyed before a new machine is
    // paid for.
    reconcile(provider, store)?;
    let ranked = select(provider.offers()?, constraints, objective);
    if ranked.is_empty() {
        return Err(Error::Provider(format!(
            "no offer satisfies the constraints {constraints:?}"
        )));
    }
    for offer in ranked {
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
            &offer,
            None,
            created_ms,
        ))?;
        let instance = match provider.provision(&offer.id, &tag) {
            Ok(Provision::Provisioned(instance)) => instance,
            // The offer went to another renter: the next-ranked one is the
            // acquisition's answer.
            Ok(Provision::OfferGone) => {
                store.remove_instance(&tag)?;
                continue;
            }
            // An API failure repeats against every remaining offer, so it
            // aborts the loop. Its intent record stays: an error answer does
            // not say whether the request landed — a timeout can mean a
            // machine carrying the tag exists — and the record is the only
            // thing reconciliation acts on, so clearing it here would make
            // that machine unreachable.
            Err(e) => return Err(e),
        };
        store.put_instance(&record(
            &tag,
            provider.id(),
            owner,
            &offer,
            Some(&instance),
            created_ms,
        ))?;
        if let Some(endpoint) = wait_ready(provider, &instance, limits)? {
            return Ok(InstanceGuard::new(
                provider,
                store,
                tag,
                instance.id,
                endpoint,
            ));
        }
        // A machine that never came up is a bad offer, not a fatal error.
        teardown(provider, store, &tag, &instance.id)?;
    }
    Err(Error::Provider(
        "every qualifying offer was lost or failed to become ready".to_string(),
    ))
}

/// Polls `instance` until it reports an endpoint, `None` when it is gone or
/// the wait runs out. The deadline is measured with [`Instant`], so a
/// wall-clock adjustment cannot extend or cut the wait.
fn wait_ready<P: Provider>(
    provider: &P,
    instance: &Instance,
    limits: &AcquireLimits,
) -> Result<Option<SshEndpoint>> {
    let started = Instant::now();
    loop {
        match provider.instance(&instance.id)? {
            InstanceStatus::Ready(endpoint) => return Ok(Some(endpoint)),
            InstanceStatus::Gone => return Ok(None),
            InstanceStatus::Provisioning => {
                if started.elapsed() >= limits.ready_timeout {
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
fn record(
    tag: &str,
    provider: &str,
    owner: &RunId,
    offer: &Offer,
    instance: Option<&Instance>,
    created_ms: u64,
) -> InstanceRecord {
    InstanceRecord {
        tag: tag.to_string(),
        provider: provider.to_string(),
        owner: owner.to_string(),
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

/// The tag one acquisition attempt runs under: `sima-<owner16>-<pid>-<seq>`.
/// It is both the ledger key and the provider-side label, so the machine and
/// its record carry one name. The owner's first 16 hex characters keep it
/// short enough for provider label limits while staying attributable; the
/// full owner lives in the record.
fn attempt_tag(owner: &RunId) -> String {
    let owner = owner.to_string();
    format!(
        "sima-{}-{}-{}",
        &owner[..16],
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Wall-clock milliseconds since the epoch, the stamp the journal carries.
/// A clock behind the epoch stamps zero.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::Duration;

    use sima_core::{Error, Result};
    use sima_store::{InstanceRecord, InstanceRecordState, Store};

    use super::{AcquireLimits, acquire};
    use crate::offer::{Constraints, Objective, Offer, OfferId};
    use crate::provider::{InstanceId, InstanceStatus, Provider, Provision, TaggedInstance};
    use crate::reconcile::reconcile;
    use crate::stub::StubProvider;
    use crate::testutil::{
        acquire_any, instance_record, live_state, prompt_limits, sample_run, stub_offer, temp_store,
    };

    /// A provider that watches what the acquisition loop does before and
    /// while it calls through: every `provision` asserts the attempt's
    /// intent record is already durable, and keeps the records it saw in
    /// call order.
    struct WatchingProvider {
        inner: StubProvider,
        root: PathBuf,
        observed_intents: Mutex<Vec<InstanceRecord>>,
    }

    impl WatchingProvider {
        fn new(inner: StubProvider, root: PathBuf) -> WatchingProvider {
            WatchingProvider {
                inner,
                root,
                observed_intents: Mutex::new(Vec::new()),
            }
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
            self.inner.offers()
        }

        fn provision(&self, offer: &OfferId, tag: &str) -> Result<Provision> {
            let store = Store::open(&self.root).expect("open the store the loop writes to");
            let records = store.instances().expect("list the ledger");
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
            self.inner.instance(id)
        }

        fn instances(&self) -> Result<Vec<TaggedInstance>> {
            self.inner.instances()
        }

        fn destroy(&self, id: &InstanceId) -> Result<()> {
            self.inner.destroy(id)
        }
    }

    #[test]
    fn a_rented_machine_comes_back_as_a_guard_over_one_live_record() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)]);
        let guard = acquire_any(&stub, &store)?;
        assert_eq!(guard.endpoint().port, 22);
        assert_eq!(guard.endpoint().user, "root");
        let records = store.instances()?;
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.provider, "stub");
        assert_eq!(record.owner, sample_run(7).to_string());
        assert_eq!(record.price_micro_usd_hour, 100_000);
        // The live state names the machine the guard holds.
        assert_eq!(record.instance(), Some(guard.id().0.as_str()));
        assert_eq!(record.tag, guard.tag());
        // The tag names the owner, the acquiring process, and the attempt.
        let parts: Vec<&str> = record.tag.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "sima");
        assert_eq!(parts[1], &sample_run(7).to_string()[..16]);
        assert_eq!(parts[2], std::process::id().to_string());
        assert!(parts[3].parse::<u64>().is_ok(), "the attempt counter");
        Ok(())
    }

    #[test]
    fn a_lost_offer_falls_through_to_the_next_ranked_one() -> Result<()> {
        let (_dir, store) = temp_store();
        let cheap = stub_offer("cheap", 100_000);
        let dearer = stub_offer("dearer", 200_000);
        let stub = StubProvider::new(vec![cheap.clone(), dearer])
            .gone_at_provision(OfferId("cheap".to_string()));
        let guard = acquire_any(&stub, &store)?;
        let records = store.instances()?;
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
            ready_timeout: Duration::ZERO,
            ready_poll: Duration::ZERO,
        };
        let lock = store.acquire_run_lock(&sample_run(7))?;
        let guard = acquire(
            &stub,
            &store,
            &lock,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &limits,
        )?;
        let records = store.instances()?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].price_micro_usd_hour, 200_000);
        assert_eq!(records[0].tag, guard.tag());
        // The abandoned machine was taken down, not left running.
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
        assert!(store.instances()?.is_empty());
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
            &constraints,
            Objective::CheapestPerHour,
            &prompt_limits(),
        );
        assert!(matches!(outcome, Err(Error::Provider(_))));
        // Nothing was rented, so nothing is owed.
        assert!(stub.instances()?.is_empty());
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    /// Aborts one acquisition on a failing provision call, returning the
    /// tag of the intent record it left in the ledger.
    fn aborted_attempt_tag(store: &Store) -> Result<String> {
        let stub = StubProvider::new(vec![stub_offer("a", 100_000)])
            .failing_provision("create instance: 500");
        assert!(acquire_any(&stub, store).is_err());
        let records = store.instances()?;
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
        let records = store.instances()?;
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
        let report = reconcile(&provider, &store)?;
        assert!(report.destroyed.is_empty());
        assert_eq!(report.cleared, vec![tag]);
        assert!(store.instances()?.is_empty());
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
        let report = reconcile(&provider, &store)?;
        assert_eq!(report.destroyed, vec![landed]);
        assert_eq!(report.cleared, vec![tag]);
        assert!(provider.live().is_empty());
        assert!(store.instances()?.is_empty());
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
            &Constraints::default(),
            Objective::CheapestPerHour,
            &prompt_limits(),
        )?;
        assert!(stub.destroyed().is_empty());
        let tags: Vec<String> = store
            .instances()?
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
        let records = store.instances()?;
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0].state, InstanceRecordState::Live { .. }));
        // The record is stamped at intent, which is what its field says.
        assert_eq!(records[0].created_ms, intent[0].created_ms);
        Ok(())
    }
}
