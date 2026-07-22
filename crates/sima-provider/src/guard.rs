//! [`InstanceGuard`]: ownership of one rented instance.

use sima_core::Result;
use sima_store::{InstanceRecord, SpendEntry, Store};

use crate::budget::{Cost, now_ms};
use crate::offer::Price;
use crate::provider::{InstanceId, Provider, SshEndpoint};

/// Ownership of one rented instance: destroys it, writes what it cost, and
/// clears its ledger record on the way out of scope.
///
/// [`release`](InstanceGuard::release) is the deliberate teardown path and
/// reports what failed. Dropping tears down too, covering the exits no call
/// site can reach — a `?` returning an error, a panic unwinding — and
/// discards the outcome, because a destructor has nowhere to report it. A
/// teardown lost that way is caught by the ledger record, which the next
/// reconciliation pass acts on.
pub struct InstanceGuard<'a, P: Provider> {
    /// The provider holding the instance.
    provider: &'a P,
    /// The store holding the ledger record.
    store: &'a Store,
    /// The ledger key, which is also the instance's provider-side tag.
    tag: String,
    /// The instance this guard owns.
    id: InstanceId,
    /// Where the instance answers SSH.
    endpoint: SshEndpoint,
    /// Whether teardown already ran, so drop leaves it alone.
    released: bool,
}

impl<'a, P: Provider> InstanceGuard<'a, P> {
    /// Takes ownership of the instance `tag` names, reachable at
    /// `endpoint`.
    pub(crate) fn new(
        provider: &'a P,
        store: &'a Store,
        tag: String,
        id: InstanceId,
        endpoint: SshEndpoint,
    ) -> InstanceGuard<'a, P> {
        InstanceGuard {
            provider,
            store,
            tag,
            id,
            endpoint,
            released: false,
        }
    }

    /// Where the instance answers SSH.
    pub fn endpoint(&self) -> &SshEndpoint {
        &self.endpoint
    }

    /// The instance this guard owns.
    pub fn id(&self) -> &InstanceId {
        &self.id
    }

    /// The ledger key the instance was rented under.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// Destroys the instance, closes its rental out, and reports the first
    /// failure. A silently failed teardown is a machine still being paid
    /// for, so the caller learns of it here.
    pub fn release(mut self) -> Result<()> {
        let outcome = teardown(self.provider, self.store, &self.tag, &self.id, None);
        self.released = true;
        outcome
    }
}

impl<P: Provider> Drop for InstanceGuard<'_, P> {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        // Nothing here can report: the outcome is dropped, and the ledger
        // record left behind by a failed teardown is what reconciliation
        // acts on.
        let _ = teardown(self.provider, self.store, &self.tag, &self.id, None);
    }
}

/// Destroys the instance, then closes its rental out at `listed`. The order
/// is what keeps a failed teardown recoverable: a provider that refused the
/// destroy leaves the record standing, so the next reconciliation pass finds
/// the machine and takes it down.
///
/// A record already cleared leaves nothing to reconstruct the rental from,
/// so the destroy still runs — it is idempotent — and no entry is written.
pub(crate) fn teardown<P: Provider>(
    provider: &P,
    store: &Store,
    tag: &str,
    id: &InstanceId,
    listed: Option<Price>,
) -> Result<()> {
    provider.destroy(id)?;
    match store.instance(tag)? {
        Some(record) => close_out(store, &record, listed),
        None => Ok(()),
    }
}

/// Writes what the rental cost, then clears its ledger record.
///
/// `listed` is the rate the provider's listing states for the machine, which
/// only a caller holding that listing has; it is what the marketplace bills,
/// so it is what the entry books. Every other caller passes `None` and the
/// record's own rate stands.
///
/// The entry comes first, so a failure between the two steps leaves the
/// record standing and the next reconciliation pass closes the rental out
/// again. That repeat is safe: the entry's key is the record's tag and
/// stamp, so a second close overwrites the first with a later end rather
/// than adding a second charge.
pub(crate) fn close_out(
    store: &Store,
    record: &InstanceRecord,
    listed: Option<Price>,
) -> Result<()> {
    let ended_ms = now_ms();
    // A clock that stepped backwards yields a window of no time rather than
    // an underflow.
    let elapsed_ms = ended_ms.saturating_sub(record.created_ms);
    let rate = listed.unwrap_or(Price(record.price_micro_usd_hour));
    store.put_spend(&SpendEntry {
        tag: record.tag.clone(),
        provider: record.provider.clone(),
        owner: record.owner.clone(),
        price_micro_usd_hour: rate.0,
        started_ms: record.created_ms,
        ended_ms,
        cost_micro_usd: Cost::accrued(rate, elapsed_ms).0,
    })?;
    store.remove_instance(&record.tag)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use sima_core::{Error, Result};
    use sima_store::SpendEntry;

    use super::teardown;
    use crate::budget::Cost;
    use crate::offer::{Offer, OfferId, Price};
    use crate::provider::{InstanceId, InstanceStatus, Provider, Provision, TaggedInstance};
    use crate::stub::StubProvider;
    use crate::testutil::{
        acquire_any, instance_record, live_state, sample_run, spend_entries, stub_offer, temp_store,
    };

    /// A stub listing the one offer these tests rent.
    fn one_offer() -> StubProvider {
        StubProvider::new(vec![stub_offer("only", 100_000)])
    }

    /// Asserts that `entry` charges `rate` over a window opening at
    /// `started_ms` and closing no earlier, for the cost that pairing
    /// implies.
    fn assert_charges(entry: &SpendEntry, rate: u64, started_ms: u64) {
        assert_eq!(entry.price_micro_usd_hour, rate);
        assert_eq!(entry.started_ms, started_ms);
        assert!(
            entry.ended_ms >= entry.started_ms,
            "the window closes no earlier than it opens: {entry:?}"
        );
        assert_eq!(
            entry.cost_micro_usd,
            Cost::accrued(Price(rate), entry.ended_ms - entry.started_ms).0
        );
    }

    #[test]
    fn releasing_a_guard_closes_the_rental_out() -> Result<()> {
        let (_dir, store) = temp_store();
        // The instance is charged at a rate the offer never listed, so the
        // entry is pinned to the rate the record carried, not the offer's.
        let stub = one_offer().charging_instances_at(Price(250_000));
        let guard = acquire_any(&stub, &store)?;
        let tag = guard.tag().to_string();
        let record = store.instance(&tag)?.expect("the rental's record");
        guard.release()?;
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tag, tag);
        assert_eq!(entries[0].provider, "stub");
        assert_eq!(entries[0].owner, sample_run(7).to_string());
        assert_charges(&entries[0], 250_000, record.created_ms);
        Ok(())
    }

    #[test]
    fn dropping_a_guard_closes_the_rental_out() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = one_offer();
        let tag = {
            let guard = acquire_any(&stub, &store)?;
            guard.tag().to_string()
        };
        let entries = spend_entries(&store, &sample_run(7))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tag, tag);
        assert_charges(&entries[0], 100_000, entries[0].started_ms);
        Ok(())
    }

    #[test]
    fn tearing_down_a_rental_whose_record_is_gone_writes_no_entry() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = one_offer();
        let guard = acquire_any(&stub, &store)?;
        let id = guard.id().clone();
        // The record is all a close-out could be built from, and it is
        // already cleared: the machine still goes down.
        store.remove_instance(guard.tag())?;
        guard.release()?;
        assert_eq!(stub.destroyed(), vec![id]);
        assert!(spend_entries(&store, &sample_run(7))?.is_empty());
        Ok(())
    }

    #[test]
    fn a_rental_whose_entry_cannot_be_written_keeps_its_record() -> Result<()> {
        let (dir, store) = temp_store();
        let stub = one_offer();
        let guard = acquire_any(&stub, &store)?;
        let tag = guard.tag().to_string();
        let id = guard.id().clone();
        let spend = dir.path().join("spend");
        fs::set_permissions(&spend, fs::Permissions::from_mode(0o500))
            .expect("make the spend ledger unwritable");
        let outcome = guard.release();
        fs::set_permissions(&spend, fs::Permissions::from_mode(0o700))
            .expect("restore the spend ledger");
        assert!(outcome.is_err(), "a lost entry must reach the caller");
        // The machine is down, and the record survives the failure, so the
        // next reconciliation pass closes the rental out.
        assert_eq!(stub.destroyed(), vec![id]);
        assert!(store.instance(&tag)?.is_some());
        Ok(())
    }

    #[test]
    fn a_teardown_a_provider_refuses_keeps_the_record_and_writes_no_entry() -> Result<()> {
        let (_dir, store) = temp_store();
        let record = instance_record("sima-tag-0", live_state("i-1"), sample_run(7));
        store.put_instance(&record)?;
        // The destroy is the first fallible step: a machine still running
        // keeps its record, so it stays discoverable, and its rental is not
        // closed out while it is still being paid for.
        let outcome = teardown(
            &RefusingProvider,
            &store,
            "sima-tag-0",
            &InstanceId("i-1".to_string()),
            None,
        );
        assert!(matches!(outcome, Err(Error::Provider(_))));
        assert_eq!(store.instance("sima-tag-0")?, Some(record));
        assert!(spend_entries(&store, &sample_run(7))?.is_empty());
        Ok(())
    }

    /// A provider that refuses every destroy, standing in for a machine the
    /// API will not take down.
    struct RefusingProvider;

    impl Provider for RefusingProvider {
        fn id(&self) -> &'static str {
            "stub"
        }

        fn offers(&self) -> Result<Vec<Offer>> {
            Ok(Vec::new())
        }

        fn provision(&self, _offer: &OfferId, _tag: &str) -> Result<Provision> {
            Ok(Provision::OfferGone)
        }

        fn instance(&self, _id: &InstanceId) -> Result<InstanceStatus> {
            Ok(InstanceStatus::Gone)
        }

        fn instances(&self) -> Result<Vec<TaggedInstance>> {
            Ok(Vec::new())
        }

        fn destroy(&self, _id: &InstanceId) -> Result<()> {
            Err(Error::Provider("destroy instance: 500".to_string()))
        }
    }

    #[test]
    fn releasing_destroys_the_instance_and_clears_its_record() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = one_offer();
        let guard = acquire_any(&stub, &store)?;
        let id = guard.id().clone();
        guard.release()?;
        assert_eq!(stub.destroyed(), vec![id]);
        assert!(stub.live().is_empty());
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn dropping_a_guard_tears_the_instance_down_too() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = one_offer();
        let id = {
            let guard = acquire_any(&stub, &store)?;
            guard.id().clone()
        };
        assert_eq!(stub.destroyed(), vec![id]);
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_panic_between_acquisition_and_release_still_tears_down() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = one_offer();
        let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
            let _guard = acquire_any(&stub, &store)?;
            panic!("the work the guard was held for failed");
        }));
        assert!(outcome.is_err(), "the panic must reach the caller");
        // The unwind ran the guard's drop, so nothing is still rented.
        assert_eq!(stub.destroyed().len(), 1);
        assert!(stub.live().is_empty());
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_released_guard_does_not_tear_down_again_when_it_drops() -> Result<()> {
        let (_dir, store) = temp_store();
        let stub = one_offer();
        let guard = acquire_any(&stub, &store)?;
        guard.release()?;
        assert_eq!(stub.destroyed().len(), 1);
        Ok(())
    }
}
