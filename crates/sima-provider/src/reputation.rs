//! Machine reputation: recording operational incidents against marketplace
//! machines.
//!
//! A rented machine's whole influence is operational — a worker never touches
//! the store — so a bad machine is judged by observable behavior alone: it
//! vanished mid-run, never became ready, or failed the worker probe. Each such
//! behavior is recorded durably against the provider's stable machine
//! identifier, so a machine with a pattern of failures is disqualified at
//! selection. The recording sites call through the store handle they already
//! hold; the derivation that turns these records into an excluded set lives at
//! acquisition.

use sima_core::Result;
use sima_store::{MachineIncident, Store};

pub use sima_store::IncidentKind;

/// Records one operational incident of `kind` against `machine` — the
/// provider's stable machine identifier — observed by rental `tag` at
/// `occurred_ms`, through the store the caller holds.
///
/// A machine with no identity (an empty string, which a provider reporting no
/// machine identifier normalizes to) is never blacklisted, so it records
/// nothing. A recording failure is a store I/O failure and reaches the caller.
pub fn record_incident(
    store: &Store,
    provider: &str,
    machine: &str,
    tag: &str,
    kind: IncidentKind,
    occurred_ms: u64,
) -> Result<()> {
    if machine.is_empty() {
        return Ok(());
    }
    store.put_machine_incident(&MachineIncident {
        provider: provider.to_string(),
        machine: machine.to_string(),
        kind,
        tag: tag.to_string(),
        occurred_ms,
    })
}

#[cfg(test)]
mod tests {
    use sima_core::Result;

    use super::{IncidentKind, record_incident};
    use crate::testutil::temp_store;

    #[test]
    fn recording_an_incident_leaves_one_record_naming_the_machine_and_tag() -> Result<()> {
        let (_dir, store) = temp_store();
        record_incident(
            &store,
            "stub",
            "81234",
            "sima-tag-0",
            IncidentKind::NeverReady,
            1_700_000_000_000,
        )?;
        let incidents = store.machine_incidents()?;
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].machine, "81234");
        assert_eq!(incidents[0].tag, "sima-tag-0");
        assert_eq!(incidents[0].kind, IncidentKind::NeverReady);
        Ok(())
    }

    #[test]
    fn a_machine_with_no_identity_records_nothing() -> Result<()> {
        let (_dir, store) = temp_store();
        // A provider reporting no machine identifier normalizes it to an empty
        // string, and an empty machine is never blacklisted.
        record_incident(&store, "stub", "", "sima-tag-0", IncidentKind::Lost, 1)?;
        assert!(store.machine_incidents()?.is_empty());
        Ok(())
    }
}
