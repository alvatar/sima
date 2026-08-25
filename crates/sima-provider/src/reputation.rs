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

use std::collections::{BTreeMap, HashMap};

use sima_core::Result;
use sima_store::{MachineIncident, Store};

pub use sima_store::IncidentKind;

/// The number of recorded incidents at which a machine is disqualified from
/// every later offer selection.
///
/// Two: the market is deep and one fluke is tolerated, but a second failure of
/// any kind is a pattern worth avoiding the machine for. The threshold is
/// deliberately fixed, without a config knob.
pub(crate) const BLACKLIST_STRIKES: usize = 2;

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

/// The machines disqualified for `provider` in `store`: every machine with at
/// least [`BLACKLIST_STRIKES`] recorded incidents under that provider id.
///
/// The set is derived from the incident records at each acquisition, never
/// materialized, so the records are the one source of truth. Incidents under
/// another provider's id count toward that provider alone.
pub(crate) fn excluded_machines(store: &Store, provider: &str) -> Result<Vec<String>> {
    let mut strikes: HashMap<String, usize> = HashMap::new();
    for incident in store.machine_incidents()? {
        if incident.provider == provider {
            *strikes.entry(incident.machine).or_default() += 1;
        }
    }
    Ok(strikes
        .into_iter()
        .filter(|(_, count)| *count >= BLACKLIST_STRIKES)
        .map(|(machine, _)| machine)
        .collect())
}

/// The reputation ledger grouped for reporting: one summary per machine,
/// sorted by provider then machine so the render is deterministic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineReport {
    /// Every machine that has recorded at least one incident.
    pub machines: Vec<MachineSummary>,
}

/// One machine's recorded incidents, counted by kind, with the window they
/// span and whether the machine is blacklisted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineSummary {
    /// The provider the machine was rented from.
    pub provider: String,
    /// The provider's stable machine identifier.
    pub machine: String,
    /// Incidents of every kind against the machine.
    pub incidents: usize,
    /// Incidents where a live instance was polled gone mid-run.
    pub lost: usize,
    /// Incidents where a machine never became ready.
    pub never_ready: usize,
    /// Incidents where a ready machine failed the worker probe.
    pub probe_failed: usize,
    /// Incidents where a ready machine could not be given the run's program.
    pub install_failed: usize,
    /// The earliest incident's stamp, in epoch milliseconds.
    pub first_occurred_ms: u64,
    /// The latest incident's stamp, in epoch milliseconds.
    pub last_occurred_ms: u64,
    /// Whether the machine is disqualified at selection: at least
    /// [`BLACKLIST_STRIKES`] incidents.
    pub blacklisted: bool,
}

/// Groups every incident in `store` per (provider, machine) for the
/// `report --machines` view: counts by kind, the window they span, and the
/// blacklist verdict. Reads the same ledger the excluded set is derived from,
/// so the view and the enforcement never disagree.
pub fn machine_report(store: &Store) -> Result<MachineReport> {
    let mut grouped: BTreeMap<(String, String), MachineSummary> = BTreeMap::new();
    for incident in store.machine_incidents()? {
        let summary = grouped
            .entry((incident.provider.clone(), incident.machine.clone()))
            .or_insert_with(|| MachineSummary {
                provider: incident.provider.clone(),
                machine: incident.machine.clone(),
                incidents: 0,
                lost: 0,
                never_ready: 0,
                probe_failed: 0,
                install_failed: 0,
                first_occurred_ms: incident.occurred_ms,
                last_occurred_ms: incident.occurred_ms,
                blacklisted: false,
            });
        summary.incidents += 1;
        match incident.kind {
            IncidentKind::Lost => summary.lost += 1,
            IncidentKind::NeverReady => summary.never_ready += 1,
            IncidentKind::ProbeFailed => summary.probe_failed += 1,
            IncidentKind::InstallFailed => summary.install_failed += 1,
        }
        summary.first_occurred_ms = summary.first_occurred_ms.min(incident.occurred_ms);
        summary.last_occurred_ms = summary.last_occurred_ms.max(incident.occurred_ms);
    }
    let machines = grouped
        .into_values()
        .map(|mut summary| {
            summary.blacklisted = summary.incidents >= BLACKLIST_STRIKES;
            summary
        })
        .collect();
    Ok(MachineReport { machines })
}

#[cfg(test)]
mod tests {
    use sima_core::Result;

    use super::{
        BLACKLIST_STRIKES, IncidentKind, excluded_machines, machine_report, record_incident,
    };
    use crate::testutil::temp_store;

    /// Records `count` incidents against `machine` under the stub provider.
    fn strike(store: &sima_store::Store, machine: &str, count: usize) -> Result<()> {
        for n in 0..count {
            record_incident(
                store,
                "stub",
                machine,
                &format!("sima-tag-{n}"),
                IncidentKind::Lost,
                n as u64,
            )?;
        }
        Ok(())
    }

    #[test]
    fn a_machine_reaches_the_excluded_set_at_the_strike_threshold() -> Result<()> {
        let (_dir, store) = temp_store();
        strike(&store, "one-strike", BLACKLIST_STRIKES - 1)?;
        strike(&store, "blacklisted", BLACKLIST_STRIKES)?;
        let excluded = excluded_machines(&store, "stub")?;
        // Below the threshold is tolerated; at it, disqualified.
        assert!(!excluded.contains(&"one-strike".to_string()));
        assert!(excluded.contains(&"blacklisted".to_string()));
        Ok(())
    }

    #[test]
    fn incidents_under_another_provider_exclude_nothing_here() -> Result<()> {
        let (_dir, store) = temp_store();
        strike(&store, "blacklisted", BLACKLIST_STRIKES)?;
        // The strikes are the stub's; another provider's excluded set is empty.
        assert!(excluded_machines(&store, "vastai")?.is_empty());
        Ok(())
    }

    #[test]
    fn the_machine_report_groups_by_machine_counts_by_kind_and_marks_blacklisted() -> Result<()> {
        let (_dir, store) = temp_store();
        // One machine with two incidents of different kinds, spanning a window,
        // and a second with a single incident.
        record_incident(&store, "stub", "flaky", "sima-a", IncidentKind::Lost, 10)?;
        record_incident(
            &store,
            "stub",
            "flaky",
            "sima-b",
            IncidentKind::ProbeFailed,
            30,
        )?;
        record_incident(
            &store,
            "stub",
            "fluke",
            "sima-c",
            IncidentKind::NeverReady,
            20,
        )?;
        let report = machine_report(&store)?;
        assert_eq!(report.machines.len(), 2);
        // Sorted by provider then machine: `flaky` before `fluke`.
        let flaky = &report.machines[0];
        assert_eq!(flaky.machine, "flaky");
        assert_eq!(flaky.incidents, 2);
        assert_eq!(flaky.lost, 1);
        assert_eq!(flaky.probe_failed, 1);
        assert_eq!(flaky.first_occurred_ms, 10);
        assert_eq!(flaky.last_occurred_ms, 30);
        assert!(flaky.blacklisted, "two incidents blacklist");
        let fluke = &report.machines[1];
        assert_eq!(fluke.machine, "fluke");
        assert_eq!(fluke.never_ready, 1);
        assert!(!fluke.blacklisted, "one incident is tolerated");
        Ok(())
    }

    #[test]
    fn the_machine_report_over_a_clean_store_is_empty() -> Result<()> {
        let (_dir, store) = temp_store();
        assert!(machine_report(&store)?.machines.is_empty());
        Ok(())
    }

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
