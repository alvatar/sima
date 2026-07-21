//! [`InstanceGuard`]: ownership of one rented instance.

use sima_core::Result;
use sima_store::Store;

use crate::provider::{InstanceId, Provider, SshEndpoint};

/// Ownership of one rented instance: destroys it and clears its ledger
/// record on the way out of scope.
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

    /// Destroys the instance and clears its ledger record, reporting the
    /// first failure. A silently failed teardown is a machine still being
    /// paid for, so the caller learns of it here.
    pub fn release(mut self) -> Result<()> {
        let outcome = teardown(self.provider, self.store, &self.tag, &self.id);
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
        let _ = teardown(self.provider, self.store, &self.tag, &self.id);
    }
}

/// Destroys the instance, then clears its ledger record. The order is what
/// keeps a failed teardown recoverable: a provider that refused the destroy
/// leaves the record standing, so the next reconciliation pass finds the
/// machine and takes it down.
pub(crate) fn teardown<P: Provider>(
    provider: &P,
    store: &Store,
    tag: &str,
    id: &InstanceId,
) -> Result<()> {
    provider.destroy(id)?;
    store.remove_instance(tag)
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use sima_core::Result;

    use crate::stub::StubProvider;
    use crate::testutil::{acquire_any, stub_offer, temp_store};

    /// A stub listing the one offer these tests rent.
    fn one_offer() -> StubProvider {
        StubProvider::new(vec![stub_offer("only", 100_000)])
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
