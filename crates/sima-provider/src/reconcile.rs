//! [`reconcile`]: destroying the machines a crashed process left running.
//!
//! A guard tears its instance down on every exit path that runs code. What
//! remains is the process that ran none — killed outright, or the machine
//! it ran on lost power. The ledger record it wrote before calling the
//! provider is the trace, and this pass acts on it:
//!
//! | Record state | Provider says           | Owner run lock | Action                          |
//! |--------------|-------------------------|----------------|---------------------------------|
//! | live         | instance exists         | held           | keep: a running orchestrator owns it |
//! | live         | instance exists         | free           | destroy instance, clear record  |
//! | live         | instance gone           | free           | clear record                    |
//! | intent       | —                       | held           | keep: an acquisition is in flight |
//! | intent       | tag scan finds instance | free           | destroy it, then clear record   |
//! | intent       | tag scan finds nothing  | free           | clear record                    |
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
use sima_store::{InstanceRecord, InstanceRecordState, Store};

use crate::guard::teardown;
use crate::provider::{InstanceId, Provider, TaggedInstance};

/// What one reconciliation pass did.
#[derive(Debug, Default)]
pub struct ReconcileReport {
    /// The instances it destroyed.
    pub destroyed: Vec<InstanceId>,
    /// The ledger tags it cleared.
    pub cleared: Vec<String>,
}

/// Destroys the instances orphaned by a process that died without tearing
/// them down, and clears their ledger records.
///
/// Runs at the start of every acquisition, so orphans stop costing money
/// before a new machine is rented. A ledger holding no record for this
/// provider reaches no provider API at all.
pub fn reconcile<P: Provider>(provider: &P, store: &Store) -> Result<ReconcileReport> {
    let records: Vec<InstanceRecord> = store
        .instances()?
        .into_iter()
        .filter(|record| record.provider == provider.id())
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
        let orphan = match &record.state {
            InstanceRecordState::Live { instance } => Some(InstanceId(instance.clone())),
            // An intent record names no instance — its writer died before
            // learning of one — so the tag it was written under is what
            // identifies the machine the provider may have created.
            InstanceRecordState::Intent => tagged(&held, &record.tag),
        };
        match orphan {
            Some(id) if held.iter().any(|instance| instance.id == id) => {
                teardown(provider, store, &record.tag, &id)?;
                report.destroyed.push(id);
            }
            // The provider no longer holds the machine: the record is all
            // that is left of it.
            _ => store.remove_instance(&record.tag)?,
        }
        report.cleared.push(record.tag);
    }
    Ok(report)
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

/// The instance the provider holds under `tag`, if any.
fn tagged(held: &[TaggedInstance], tag: &str) -> Option<InstanceId> {
    held.iter()
        .find(|instance| instance.tag == tag)
        .map(|instance| instance.id.clone())
}

#[cfg(test)]
mod tests {
    use sima_core::{Error, Result};
    use sima_store::{InstanceRecord, InstanceRecordState};

    use super::reconcile;
    use crate::provider::InstanceId;
    use crate::stub::StubProvider;
    use crate::testutil::{
        acquire_any, instance_record, live_state, sample_run, stub_offer, temp_store,
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
        let report = reconcile(&stub, &store)?;
        assert_eq!(report.destroyed, vec![InstanceId("i-1".to_string())]);
        assert_eq!(report.cleared, vec!["sima-tag-0".to_string()]);
        assert!(stub.live().is_empty());
        assert!(store.instances()?.is_empty());
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
        let report = reconcile(&stub, &store)?;
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
        let report = reconcile(&stub, &store)?;
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
        let report = reconcile(&stub, &store)?;
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
        let report = reconcile(&stub, &store)?;
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
        let report = reconcile(&stub, &store)?;
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
        let report = reconcile(&stub, &store)?;
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
            reconcile(&stub, &store),
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
}
