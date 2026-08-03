//! [`reconcile`]: destroying the machines a crashed process left running.
//!
//! A guard tears its instance down on every exit path that runs code. What
//! remains is the process that ran none — killed outright, or the machine
//! it ran on lost power. The ledger record it wrote before calling the
//! provider is the trace, and this pass acts on it:
//!
//! | Record state | Provider says                        | Owner run lock | Action                              |
//! |--------------|--------------------------------------|----------------|-------------------------------------|
//! | live         | the listing carries the instance     | held           | keep: a running orchestrator owns it |
//! | live         | the listing carries the instance     | free           | destroy instance, close the rental out |
//! | live         | the listing omits it, the id probe finds it | free    | destroy instance, close the rental out |
//! | live         | the id probe answers gone            | free           | close the rental out                |
//! | intent       | —                                    | held           | keep: an acquisition is in flight   |
//! | intent       | tag scan finds instance              | free           | destroy it, then close the rental out |
//! | intent       | tag scan finds nothing               | free           | close the rental out                |
//!
//! A live record the scan missed is probed by id before anything is
//! concluded: a listing may omit a machine that is running — the vast
//! backend drops rows carrying no label — and only the provider's own answer
//! for the instance id is read as absence. The scan alone would clear the
//! record of a machine that keeps billing, leaving nothing for a later pass
//! to act on.
//!
//! Closing a rental out writes what it cost and then clears its record, so
//! every orphan this pass reaps leaves its spend behind. The last row
//! charges a machine it never found: a close-out lost to a crash and a
//! provision that never landed leave the same state, and only one of the
//! two answers is safe to be wrong about.
//!
//! What a reaped rental is charged is the rate its listing states, which is
//! what the marketplace bills. The record's own rate stands for a machine
//! the scan did not find, and for a listing stating no rate.
//!
//! The owner run lock column rests on one contract: every live run holds
//! its orchestrator lock for as long as it holds a machine.
//! [`acquire`](crate::acquire) enforces it for the acquiring run through its
//! signature, which takes the lock itself.
//!
//! A record is judged by its owner's lock alone, so a run holding its lock
//! keeps every record naming it — including one an earlier process of that
//! same run left behind. Such a leftover is indistinguishable from a machine
//! the live process is using, and it is reaped like any orphan once the
//! run's lock is free.
//!
//! Only records naming the given provider participate; another provider's
//! records are left untouched and unreported.

use sima_core::{Error, Result};
use sima_model::RunId;
use sima_store::{InstanceRecord, InstanceRecordState, Rental, Store};

use crate::guard::{close_out, teardown};
use crate::offer::Price;
use crate::provider::{InstanceId, InstanceStatus, Provider, TaggedInstance};

/// Which rentals a reconciliation pass considers.
///
/// A run's orchestrator holds its lock while it drives, so a record whose owner
/// holds no lock is normally an orphan of a crash and is destroyed. A migration
/// breaks that inference: it detaches the far side deliberately, and the local
/// process may be gone while the rental is working and paid for, so a
/// [`Rental::Orchestrator`] record has exactly the shape reconciliation reaps.
/// It is therefore spared unless a caller says otherwise; every other rental is
/// an orphan when its owner's lock is free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileScope {
    /// Rentals carrying workers alone. The default, and what every acquisition
    /// runs: it must not destroy a working detached migration.
    Workers,
    /// Every rental, a hosting one included. What `sima reconcile --hosted`
    /// asks for, when the operator knows no migration is running.
    Hosted,
}

impl ReconcileScope {
    /// Whether a rental in this role is this pass's to reap.
    fn covers(self, role: Rental) -> bool {
        match self {
            ReconcileScope::Workers => role == Rental::Worker,
            ReconcileScope::Hosted => true,
        }
    }
}

/// What one reconciliation pass did.
#[derive(Debug, Default)]
pub struct ReconcileReport {
    /// The instances it destroyed.
    pub destroyed: Vec<InstanceId>,
    /// The ledger tags it cleared.
    pub cleared: Vec<String>,
}

/// Destroys the instances orphaned by a process that died without tearing them
/// down, and clears their ledger records, within `scope`.
///
/// Runs at the start of every acquisition under [`ReconcileScope::Workers`], so
/// orphans stop costing money before a new machine is rented — and so a
/// detached migration, whose rental is working and paid for while nothing local
/// holds a lock, is not destroyed by an unrelated command that happens to rent
/// against the same store. A ledger holding no record for this provider reaches
/// no provider API at all.
pub fn reconcile<P: Provider + ?Sized>(
    provider: &P,
    store: &Store,
    scope: ReconcileScope,
) -> Result<ReconcileReport> {
    let records: Vec<InstanceRecord> = store
        .instances()?
        .into_iter()
        .filter(|record| record.provider == provider.id() && scope.covers(record.role))
        .collect();
    if records.is_empty() {
        return Ok(ReconcileReport::default());
    }
    let held = provider.instances()?;
    let mut report = ReconcileReport::default();
    for record in records {
        if owner_alive(store, &record)? {
            continue;
        }
        match reaping(provider, &held, &record)? {
            Reap::Destroy { id, listed } => {
                teardown(provider, store, &record.tag, &id, listed)?;
                report.destroyed.push(id);
            }
            // The provider no longer holds the machine: the record is all
            // that is left of it, and it is charged for at the rate it
            // carries. A machine that died provider-side ran until it did;
            // an intent record naming no machine is indistinguishable from a
            // close-out a crash lost, so it is charged too.
            Reap::Close => close_out(store, &record, None)?,
        }
        report.cleared.push(record.tag);
    }
    Ok(report)
}

/// What the pass does with one orphan.
enum Reap {
    /// Destroy the machine, then close the rental out at `listed` where a
    /// listing row states a rate for it.
    Destroy {
        /// The machine to destroy.
        id: InstanceId,
        /// The rate its listing row states, if the scan carried one.
        listed: Option<Price>,
    },
    /// Close the rental out at the record's own rate, with no machine to
    /// destroy.
    Close,
}

/// Decides what becomes of `record`, from the scan `held` and, for a live
/// record the scan missed, the provider's own answer for the instance id.
///
/// The scan is a listing, and a listing may omit a machine that is running:
/// the vast backend drops rows carrying no label, which is what an operator
/// edit in the console produces. Closing such a record out would clear the
/// only trace of a machine that keeps billing, so absence is concluded from
/// the provider's answer for the id alone. A probe that fails propagates,
/// because reading a fault as absence has that same effect.
fn reaping<P: Provider + ?Sized>(
    provider: &P,
    held: &[TaggedInstance],
    record: &InstanceRecord,
) -> Result<Reap> {
    match &record.state {
        InstanceRecordState::Live { instance } => {
            let id = InstanceId(instance.clone());
            if let Some(listed) = held.iter().find(|held| held.id == id) {
                return Ok(Reap::Destroy {
                    id,
                    listed: listed.price,
                });
            }
            match provider.instance(&id)? {
                InstanceStatus::Gone => Ok(Reap::Close),
                // The machine exists — up, or still coming up — and no
                // listing row is in hand, so the record's own rate, which
                // was rewritten from the instance's at provision, is what
                // its close-out books.
                InstanceStatus::Ready(_) | InstanceStatus::Provisioning => {
                    Ok(Reap::Destroy { id, listed: None })
                }
            }
        }
        // An intent record names no instance — its writer died before
        // learning of one — so the tag it was written under is what
        // identifies the machine the provider may have created.
        InstanceRecordState::Intent => Ok(match held.iter().find(|held| held.tag == record.tag) {
            Some(listed) => Reap::Destroy {
                id: listed.id.clone(),
                listed: listed.price,
            },
            None => Reap::Close,
        }),
    }
}

/// Whether the run that wrote `record` is still running.
///
/// The probe is the run's orchestrator lock, which the kernel releases the
/// moment its holder exits: a free lock means the owner is gone. The lock
/// answers on the machine holding it, which is the machine acquisition and
/// reconciliation both run on.
fn owner_alive(store: &Store, record: &InstanceRecord) -> Result<bool> {
    let owner = RunId::from_hex(&record.owner).map_err(|_| {
        Error::Corruption(format!(
            "instance record {} names a malformed owner {:?}",
            record.tag, record.owner
        ))
    })?;
    Ok(store.lock_holder(&owner)?.is_some())
}

#[cfg(test)]
mod tests {
    use sima_core::{Error, Result};
    use sima_store::{InstanceRecord, InstanceRecordState, Rental, Store};

    use super::{ReconcileScope, reconcile};
    use crate::guard::teardown;
    use crate::offer::Price;
    use crate::provider::{InstanceId, Provider};
    use crate::stub::StubProvider;
    use crate::testutil::{
        acquire_any, instance_record, instance_record_as, live_state, sample_run, spend_entries,
        stub_offer, temp_store,
    };

    /// A record for `tag` in `state`, owned by the run for seed 7.
    fn record(tag: &str, state: InstanceRecordState) -> InstanceRecord {
        instance_record(tag, state, sample_run(7))
    }

    #[test]
    fn a_live_record_whose_owner_died_takes_its_instance_down() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-1".to_string()), "sima-tag-0");
        store.put_instance(&record("sima-tag-0", live_state("i-1")))?;
        let report = reconcile(&stub, &store, ReconcileScope::Workers)?;
        assert_eq!(report.destroyed, vec![InstanceId("i-1".to_string())]);
        assert_eq!(report.cleared, vec!["sima-tag-0".to_string()]);
        assert!(stub.live().is_empty());
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_destroyed_live_orphan_has_its_rental_closed_out() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-1".to_string()), "sima-tag-0");
        let orphan = record("sima-tag-0", live_state("i-1"));
        store.put_instance(&orphan)?;
        reconcile(&stub, &store, ReconcileScope::Workers)?;
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tag, "sima-tag-0");
        assert_eq!(entries[0].started_ms, orphan.created_ms);
        assert_eq!(entries[0].price_micro_usd_hour, orphan.price_micro_usd_hour);
        Ok(())
    }

    #[test]
    fn a_live_orphan_whose_machine_is_gone_is_closed_out_and_cleared() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new());
        // The machine died on the provider's side, or a crash landed
        // between its destroy and its close-out: either way it ran, and
        // what it ran for is charged.
        let orphan = record("sima-tag-0", live_state("expired"));
        store.put_instance(&orphan)?;
        reconcile(&stub, &store, ReconcileScope::Workers)?;
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        // Nothing states a rate for a machine that no longer exists, so the
        // record's own is what the entry books.
        assert_eq!(entries[0].price_micro_usd_hour, orphan.price_micro_usd_hour);
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_live_orphan_the_listing_omits_but_the_probe_finds_is_destroyed() -> Result<()> {
        let (_dir, store) = temp_store();
        // A machine the account holds and the scan does not carry, which is
        // what a provider row reporting no tag looks like.
        let stub = StubProvider::new(Vec::new())
            .with_unlisted_instance(InstanceId("i-1".to_string()), "sima-tag-0");
        let orphan = record("sima-tag-0", live_state("i-1"));
        store.put_instance(&orphan)?;
        let report = reconcile(&stub, &store, ReconcileScope::Workers)?;
        assert_eq!(report.destroyed, vec![InstanceId("i-1".to_string())]);
        assert_eq!(report.cleared, vec!["sima-tag-0".to_string()]);
        assert!(stub.live().is_empty());
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        // No listing row is in hand, so the record's rate is what it costs.
        assert_eq!(entries[0].price_micro_usd_hour, orphan.price_micro_usd_hour);
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_live_orphan_whose_probe_fails_keeps_its_record() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new()).failing_instance("show instance: 503");
        let orphan = record("sima-tag-0", live_state("i-1"));
        store.put_instance(&orphan)?;
        // Reading the fault as absence would clear the record of a machine
        // that may still be running and billed.
        assert!(matches!(
            reconcile(&stub, &store, ReconcileScope::Workers),
            Err(Error::Provider(message)) if message == "show instance: 503"
        ));
        assert_eq!(store.instances()?, vec![orphan]);
        assert!(spend_entries(&store, &sample_run(7))?.is_empty());
        Ok(())
    }

    #[test]
    fn an_intent_orphan_whose_tag_names_a_machine_is_closed_out() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-2".to_string()), "sima-tag-0");
        store.put_instance(&record("sima-tag-0", InstanceRecordState::Intent))?;
        reconcile(&stub, &store, ReconcileScope::Workers)?;
        assert_eq!(spend_entries(&store, &sample_run(7))?.len(), 1);
        Ok(())
    }

    #[test]
    fn an_orphan_the_listing_prices_is_charged_that_rate_rather_than_the_records() -> Result<()> {
        let (_dir, store) = temp_store();
        // The record of an intent orphan carries the offer's rate, which its
        // writer never got to replace with the machine's own. The listing
        // states what the marketplace bills.
        let stub = StubProvider::new(Vec::new())
            .charging_instances_at(Price(250_000))
            .with_instance(InstanceId("i-2".to_string()), "sima-tag-0");
        store.put_instance(&record("sima-tag-0", InstanceRecordState::Intent))?;
        reconcile(&stub, &store, ReconcileScope::Workers)?;
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].price_micro_usd_hour, 250_000);
        Ok(())
    }

    #[test]
    fn a_listing_cheaper_than_the_record_is_booked_at_the_cheaper_rate() -> Result<()> {
        let (_dir, store) = temp_store();
        // The entry follows the bill in both directions: a listing under the
        // record's rate is booked as it stands, not floored at the record's.
        let stub = StubProvider::new(Vec::new())
            .charging_instances_at(Price(40_000))
            .with_instance(InstanceId("i-1".to_string()), "sima-tag-0");
        store.put_instance(&record("sima-tag-0", live_state("i-1")))?;
        reconcile(&stub, &store, ReconcileScope::Workers)?;
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].price_micro_usd_hour, 40_000);
        Ok(())
    }

    #[test]
    fn a_listing_stating_no_rate_leaves_the_records_rate_standing() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-1".to_string()), "sima-tag-0");
        let orphan = record("sima-tag-0", live_state("i-1"));
        store.put_instance(&orphan)?;
        reconcile(&stub, &store, ReconcileScope::Workers)?;
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].price_micro_usd_hour, orphan.price_micro_usd_hour);
        Ok(())
    }

    #[test]
    fn an_intent_orphan_naming_no_machine_is_charged_all_the_same() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new());
        // A close-out lost to a crash and a provision that never landed
        // leave the same state, so the pass charges both: a phantom attempt
        // counted is an overcount, and a real machine missed is money the
        // ledger never accounts for.
        store.put_instance(&record("sima-tag-0", InstanceRecordState::Intent))?;
        reconcile(&stub, &store, ReconcileScope::Workers)?;
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tag, "sima-tag-0");
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn closing_one_rental_twice_leaves_one_entry() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new());
        let orphan = record("sima-tag-0", live_state("expired"));
        store.put_instance(&orphan)?;
        reconcile(&stub, &store, ReconcileScope::Workers)?;
        let first = spend_entries(&store, &sample_run(7))?;
        assert_eq!(first.len(), 1);
        // The state a crash between the entry write and the record clear
        // leaves: the record is back, and its close-out reproduces the same
        // key, so the entry is rewritten rather than added to.
        store.put_instance(&orphan)?;
        reconcile(&stub, &store, ReconcileScope::Workers)?;
        let second = spend_entries(&store, &sample_run(7))?;
        assert_eq!(second.len(), 1);
        assert!(second[0].ended_ms >= first[0].ended_ms);
        Ok(())
    }

    #[test]
    fn a_rental_two_paths_close_is_charged_once() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new());
        let orphan = record("sima-tag-0", InstanceRecordState::Intent);
        store.put_instance(&orphan)?;
        reconcile(&stub, &store, ReconcileScope::Workers)?;
        let charged = spend_entries(&store, &sample_run(7))?;
        assert_eq!(charged.len(), 1);
        // The acquiring process comes back to a tag reconciliation already
        // charged and cleared. Its teardown finds no record, so it writes
        // nothing: the entry the pass left stands alone.
        teardown(
            &stub,
            &store,
            "sima-tag-0",
            &InstanceId("i-1".to_string()),
            None,
        )?;
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].started_ms, orphan.created_ms);
        assert_eq!(entries[0].ended_ms, charged[0].ended_ms);
        Ok(())
    }

    #[test]
    fn a_live_record_whose_owner_still_runs_is_left_alone() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-1".to_string()), "sima-tag-0");
        store.put_instance(&record("sima-tag-0", live_state("i-1")))?;
        // The owner holds its orchestrator lock for the length of the pass,
        // which is what a running run looks like.
        let _lock = store.acquire_run_lock(&sample_run(7))?;
        let report = reconcile(&stub, &store, ReconcileScope::Workers)?;
        assert!(report.destroyed.is_empty());
        assert!(report.cleared.is_empty());
        assert_eq!(stub.live(), vec![InstanceId("i-1".to_string())]);
        assert_eq!(store.instances()?.len(), 1);
        Ok(())
    }

    #[test]
    fn a_live_record_the_provider_no_longer_holds_is_only_cleared() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new());
        store.put_instance(&record("sima-tag-0", live_state("expired")))?;
        let report = reconcile(&stub, &store, ReconcileScope::Workers)?;
        assert!(report.destroyed.is_empty());
        assert_eq!(report.cleared, vec!["sima-tag-0".to_string()]);
        assert!(stub.destroyed().is_empty());
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn an_intent_record_whose_tag_names_a_machine_takes_it_down() -> Result<()> {
        let (_dir, store) = temp_store();
        // The process died between writing the intent record and learning
        // what the provider had created for it.
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-2".to_string()), "sima-tag-0");
        store.put_instance(&record("sima-tag-0", InstanceRecordState::Intent))?;
        let report = reconcile(&stub, &store, ReconcileScope::Workers)?;
        assert_eq!(report.destroyed, vec![InstanceId("i-2".to_string())]);
        assert_eq!(report.cleared, vec!["sima-tag-0".to_string()]);
        assert!(stub.live().is_empty());
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn an_intent_record_naming_no_machine_is_only_cleared() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub =
            StubProvider::new(Vec::new()).with_instance(InstanceId("i-3".to_string()), "other-tag");
        store.put_instance(&record("sima-tag-0", InstanceRecordState::Intent))?;
        let report = reconcile(&stub, &store, ReconcileScope::Workers)?;
        assert!(report.destroyed.is_empty());
        assert_eq!(report.cleared, vec!["sima-tag-0".to_string()]);
        // The instance under another tag belongs to another attempt.
        assert_eq!(stub.live(), vec![InstanceId("i-3".to_string())]);
        Ok(())
    }

    #[test]
    fn an_intent_record_whose_owner_still_runs_is_left_alone() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-4".to_string()), "sima-tag-0");
        store.put_instance(&record("sima-tag-0", InstanceRecordState::Intent))?;
        let _lock = store.acquire_run_lock(&sample_run(7))?;
        let report = reconcile(&stub, &store, ReconcileScope::Workers)?;
        assert!(report.destroyed.is_empty());
        assert!(report.cleared.is_empty());
        assert_eq!(stub.live(), vec![InstanceId("i-4".to_string())]);
        Ok(())
    }

    #[test]
    fn another_providers_record_is_untouched() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-5".to_string()), "sima-tag-0");
        let mut foreign = record("sima-tag-0", live_state("i-5"));
        foreign.provider = "vastai".to_string();
        store.put_instance(&foreign)?;
        let report = reconcile(&stub, &store, ReconcileScope::Workers)?;
        assert!(report.destroyed.is_empty());
        assert!(report.cleared.is_empty());
        assert_eq!(store.instances()?, vec![foreign]);
        assert_eq!(stub.live(), vec![InstanceId("i-5".to_string())]);
        Ok(())
    }

    #[test]
    fn a_record_with_a_malformed_owner_is_corruption() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(Vec::new());
        let mut malformed = record("sima-tag-0", live_state("i-6"));
        malformed.owner = "not-a-run-id".to_string();
        store.put_instance(&malformed)?;
        assert!(matches!(
            reconcile(&stub, &store, ReconcileScope::Workers),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn acquisition_destroys_an_orphan_before_renting_anything_new() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)])
            .with_instance(InstanceId("orphan".to_string()), "sima-tag-0");
        // The orphan belongs to another run, whose lock is free: the
        // acquiring run holds only its own.
        store.put_instance(&instance_record(
            "sima-tag-0",
            live_state("orphan"),
            sample_run(8),
        ))?;
        let guard = acquire_any(&stub, &store)?;
        // The orphan went down before the new machine came up, and the
        // ledger holds the new attempt alone.
        assert_eq!(stub.destroyed(), vec![InstanceId("orphan".to_string())]);
        assert_eq!(stub.live(), vec![guard.id().clone()]);
        let records = store.instances()?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tag, guard.tag());
        Ok(())
    }

    /// Provisions the stub's `nth` offer under `tag`, returning its instance
    /// id. A rented offer never returns to the stub's marketplace, so two
    /// rentals name two offers.
    fn provisioned(stub: &StubProvider, nth: usize, tag: &str) -> String {
        let offer = stub
            .offers()
            .expect("offers")
            .into_iter()
            .nth(nth)
            .expect("an offer");
        match stub.provision(&offer.id, tag).expect("provision") {
            crate::provider::Provision::Provisioned(instance) => instance.id.0,
            crate::provider::Provision::OfferGone => panic!("the stub offer is available"),
        }
    }

    /// A stub holding one worker rental and one hosting rental, both owned by a
    /// run holding no lock — the shape a pass reaps — with their records in the
    /// ledger.
    fn one_of_each(store: &Store) -> StubProvider {
        let stub = StubProvider::new(vec![stub_offer("a", 100_000), stub_offer("b", 200_000)]);
        for (nth, tag, role) in [
            (0, "sima-worker-0", Rental::Worker),
            (1, "sima-hosting-0", Rental::Orchestrator),
        ] {
            let instance = provisioned(&stub, nth, tag);
            store
                .put_instance(&instance_record_as(
                    tag,
                    live_state(&instance),
                    sample_run(7),
                    role,
                ))
                .expect("put the ledger record");
        }
        stub
    }

    #[test]
    fn a_default_pass_spares_a_hosting_rental_and_reaps_a_worker_one() -> Result<()> {
        // Both records have the shape a pass reaps, and only one of them is an
        // orphan. The other is a detached migration, working and paid for.
        let (_dir, store) = temp_store();
        let stub = one_of_each(&store);

        let report = reconcile(&stub, &store, ReconcileScope::Workers)?;
        assert_eq!(report.cleared, vec!["sima-worker-0".to_string()]);
        let left: Vec<String> = store.instances()?.into_iter().map(|r| r.tag).collect();
        assert_eq!(
            left,
            vec!["sima-hosting-0".to_string()],
            "the hosting rental keeps its record"
        );
        assert_eq!(stub.live().len(), 1, "and keeps its machine");
        Ok(())
    }

    #[test]
    fn the_hosted_scope_reaps_both() -> Result<()> {
        // What `sima reconcile --hosted` asks for, when the operator knows no
        // migration is running.
        let (_dir, store) = temp_store();
        let stub = one_of_each(&store);

        let report = reconcile(&stub, &store, ReconcileScope::Hosted)?;
        assert_eq!(report.cleared.len(), 2);
        assert!(store.instances()?.is_empty());
        assert!(stub.live().is_empty());
        Ok(())
    }
}
