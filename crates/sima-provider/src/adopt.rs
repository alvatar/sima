//! [`adopt`]: taking ownership of a rental that is already running.
//!
//! A migration detaches the far side deliberately, so the process that rented
//! the machine is routinely gone while the machine is still working and still
//! being paid for. Re-invoking the migration must take that machine back rather
//! than rent a second one, and the ledger is what makes it findable: a live
//! record of role [`Rental::Orchestrator`] owned by this run names it.
//!
//! Adoption never rewrites the record. The rental's charged window opens at the
//! record's `created_ms` and closes when its spend entry is written, so a
//! rewrite here would move the window and mis-bill the rental.

use std::thread::sleep;
use std::time::Instant;

use sima_core::{Error, Result};
use sima_store::{InstanceRecord, Rental, RunLock, Store};

use crate::acquire::AcquireLimits;
use crate::guard::InstanceGuard;
use crate::offer::Price;
use crate::provider::{InstanceId, InstanceStatus, Provider};

/// Rebuilds a guard over the rental hosting this run's orchestrator, or `None`
/// when there is none to take back.
///
/// The ledger is searched for a live record of role [`Rental::Orchestrator`]
/// owned by `lock`'s run and belonging to `provider`. What happens then follows
/// the provider's answer:
///
/// - **Ready** — the guard is rebuilt from the record's tag, machine, and rate
///   plus the endpoint the provider now reports. The record is left byte for
///   byte as it was.
/// - **Provisioning** — polled to `limits.ready_timeout`, then an error naming
///   the instance and its tag. Destroying a machine that may be coming up is a
///   guess about money, so the caller is told rather than charged for a second
///   one.
/// - **Gone** — the record is removed and `None` returned; the caller rents
///   fresh.
///
/// A record owned by a different run, or in any other role, is not this
/// caller's to take and is ignored.
pub fn adopt<'a, P: Provider + ?Sized>(
    provider: &'a P,
    store: &'a Store,
    lock: &RunLock,
    limits: &AcquireLimits,
) -> Result<Option<InstanceGuard<'a, P>>> {
    let owner = lock.run().to_string();
    let Some(record) = store.instances()?.into_iter().find(|record| {
        record.provider == provider.id()
            && record.owner == owner
            && record.role == Rental::Orchestrator
            && record.instance().is_some()
    }) else {
        return Ok(None);
    };
    let id = InstanceId(
        record
            .instance()
            .expect("the search kept only records naming an instance")
            .to_string(),
    );
    let started = Instant::now();
    loop {
        match provider.instance(&id)? {
            InstanceStatus::Ready(endpoint) => {
                // The offer's hardware is not in the ledger — a record carries
                // what reconciliation and billing need, not what an offer said —
                // so the rebuilt guard reports none. It is journal detail, and a
                // reattaching migration announces nothing about hardware it did
                // not choose.
                return Ok(Some(InstanceGuard::new(
                    provider,
                    store,
                    record.tag.clone(),
                    id,
                    endpoint,
                    record.machine.clone(),
                    String::new(),
                    0,
                    Price(record.price_micro_usd_hour),
                )));
            }
            InstanceStatus::Gone => {
                // Nothing to take back: the machine died while nothing was
                // watching, and the record is all that is left of it.
                store.remove_instance(&record.tag)?;
                return Ok(None);
            }
            InstanceStatus::Provisioning => {
                if started.elapsed() >= limits.ready_timeout {
                    return Err(provisioning_past_the_bound(&record));
                }
                sleep(limits.ready_poll);
            }
        }
    }
}

/// The error a rental still provisioning past the readiness bound raises,
/// naming the instance and its tag so an operator can find it on the provider's
/// side.
fn provisioning_past_the_bound(record: &InstanceRecord) -> Error {
    Error::Provider(format!(
        "the rental hosting this run is still provisioning past the readiness bound: \
         instance {:?} under tag {:?}",
        record.instance().unwrap_or_default(),
        record.tag
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sima_store::InstanceRecordState;

    use super::*;
    use crate::stub::StubProvider;
    use crate::testutil::{instance_record_as, live_state, sample_run, stub_offer, temp_store};

    /// Bounds that never wait: a provisioning rental fails at once.
    fn limits() -> AcquireLimits {
        AcquireLimits {
            ready_timeout: Duration::ZERO,
            ready_poll: Duration::ZERO,
        }
    }

    /// Provisions the stub's `nth` offer under `tag` and returns the instance
    /// it created. A rented offer never returns to the stub's marketplace, so
    /// two rentals name two offers.
    fn provisioned(stub: &StubProvider, nth: usize, tag: &str) -> InstanceId {
        let offer = stub
            .offers()
            .expect("offers")
            .into_iter()
            .nth(nth)
            .expect("an offer");
        match stub.provision(&offer.id, tag).expect("provision") {
            crate::provider::Provision::Provisioned(instance) => instance.id,
            crate::provider::Provision::OfferGone => panic!("the stub offer is available"),
        }
    }

    #[test]
    fn a_live_hosting_rental_comes_back_as_a_guard() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = sample_run(3);
        let lock = store.acquire_run_lock(&run)?;
        let stub = StubProvider::new(vec![stub_offer("a", 100_000)]);
        let id = provisioned(&stub, 0, "sima-tag-0");
        let record = instance_record_as("sima-tag-0", live_state(&id.0), run, Rental::Orchestrator);
        store.put_instance(&record)?;

        let guard = adopt(&stub, &store, &lock, &limits())?.expect("the rental is adopted");
        assert_eq!(guard.id(), &id);
        assert_eq!(guard.tag(), "sima-tag-0");
        // The record is untouched: the charged window stays anchored where the
        // rental began.
        assert_eq!(store.instances()?, vec![record]);
        // Releasing it is an ordinary teardown, so a reattached migration tears
        // its machine down exactly as the first one would have.
        guard.release()?;
        assert_eq!(stub.destroyed().len(), 1);
        Ok(())
    }

    #[test]
    fn a_rental_the_provider_no_longer_holds_clears_its_record() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = sample_run(3);
        let lock = store.acquire_run_lock(&run)?;
        let stub = StubProvider::new(vec![stub_offer("a", 100_000)]);
        let id = provisioned(&stub, 0, "sima-tag-0");
        store.put_instance(&instance_record_as(
            "sima-tag-0",
            live_state(&id.0),
            run,
            Rental::Orchestrator,
        ))?;
        stub.destroy(&id)?;

        assert!(adopt(&stub, &store, &lock, &limits())?.is_none());
        assert!(
            store.instances()?.is_empty(),
            "the record is all that was left of it"
        );
        Ok(())
    }

    #[test]
    fn a_rental_still_provisioning_past_the_bound_is_named_not_destroyed() -> Result<()> {
        // Destroying a machine that may be coming up is a guess about money.
        let (_dir, store) = temp_store();
        let run = sample_run(3);
        let lock = store.acquire_run_lock(&run)?;
        let stub = StubProvider::new(vec![stub_offer("a", 100_000)]).ready_after(5);
        let id = provisioned(&stub, 0, "sima-tag-0");
        store.put_instance(&instance_record_as(
            "sima-tag-0",
            live_state(&id.0),
            run,
            Rental::Orchestrator,
        ))?;

        // A guard has no Debug — it owns a machine, and rendering one is not
        // something any path needs — so the outcome is matched rather than
        // printed.
        match adopt(&stub, &store, &lock, &limits()) {
            Err(Error::Provider(message)) => {
                assert!(message.contains("sima-tag-0"), "names the tag: {message}");
                assert!(message.contains(&id.0), "names the instance: {message}");
            }
            Err(other) => panic!("expected a provider error, got {other:?}"),
            Ok(_) => panic!("a rental past its readiness bound must not be adopted"),
        }
        assert!(stub.destroyed().is_empty(), "nothing was destroyed");
        assert_eq!(store.instances()?.len(), 1, "the record stands");
        Ok(())
    }

    #[test]
    fn a_worker_rental_and_another_run_s_rental_are_not_this_caller_s() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = sample_run(3);
        let lock = store.acquire_run_lock(&run)?;
        let stub = StubProvider::new(vec![stub_offer("a", 100_000), stub_offer("b", 200_000)]);
        let mine = provisioned(&stub, 0, "sima-tag-0");
        let theirs = provisioned(&stub, 1, "sima-tag-1");
        // Mine, but carrying workers: the local orchestrator drives it, so
        // there is nothing detached to take back.
        store.put_instance(&instance_record_as(
            "sima-tag-0",
            live_state(&mine.0),
            run,
            Rental::Worker,
        ))?;
        // Hosting, but another run's.
        store.put_instance(&instance_record_as(
            "sima-tag-1",
            live_state(&theirs.0),
            sample_run(4),
            Rental::Orchestrator,
        ))?;

        assert!(adopt(&stub, &store, &lock, &limits())?.is_none());
        assert_eq!(store.instances()?.len(), 2, "neither record is touched");
        Ok(())
    }

    #[test]
    fn a_record_still_at_intent_names_no_machine_to_adopt() -> Result<()> {
        // An attempt that never reached a machine has nothing to take back;
        // reconciliation is what clears it.
        let (_dir, store) = temp_store();
        let run = sample_run(3);
        let lock = store.acquire_run_lock(&run)?;
        let stub = StubProvider::new(vec![stub_offer("a", 100_000)]);
        store.put_instance(&instance_record_as(
            "sima-tag-0",
            InstanceRecordState::Intent,
            run,
            Rental::Orchestrator,
        ))?;
        assert!(adopt(&stub, &store, &lock, &limits())?.is_none());
        Ok(())
    }
}
