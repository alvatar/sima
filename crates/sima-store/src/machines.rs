//! The machine-reputation ledger: one record per operational incident, keyed
//! by the marketplace machine that produced it.
//!
//! A rented machine that vanished mid-search, never came up, or came up without a
//! usable GPU has already cost money once. Each such behavior is recorded here
//! durably, scoped to the provider's stable machine identifier and shared by
//! every search using the store, so a machine with a pattern of failures is
//! disqualified at offer selection. The blacklist itself is never stored: it
//! is derived from these records at each acquisition, so there is one source
//! of truth and nothing to reconcile.
//!
//! Records are operational and serde-serialized, like instance and spend
//! records, and never identity-bearing. Each incident is placed under
//! `machines/<provider>-<machine>/<tag>-<occurred_ms>`: a directory per
//! machine, so listing one machine's incidents is a single directory read.
//! The provider, machine, and tag become path components, so each is validated
//! against its charset before it reaches the filesystem.

use serde::{Deserialize, Serialize};
use sima_core::{Error, Result};

use crate::atomic;
use crate::instances::{validate_charset, validate_tag};
use crate::layout;
use crate::ledger;
use crate::store::Store;

/// One operational incident against a marketplace machine: the durable trace
/// that a rented machine failed in a way attributable to the machine itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineIncident {
    /// The provider the machine was rented from, e.g. `vastai`.
    pub provider: String,
    /// The provider's stable identifier for the physical machine.
    pub machine: String,
    /// What the machine did.
    pub kind: IncidentKind,
    /// The rental attempt that observed the incident, for attribution.
    pub tag: String,
    /// Wall-clock milliseconds since the epoch when the incident was observed,
    /// like the journal's stamps.
    pub occurred_ms: u64,
}

/// The operational behaviors a machine is judged by, in decreasing
/// attributability. There is no output verification: a worker never touches
/// the store, so a bad machine's whole influence is operational.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentKind {
    /// A live instance the supervisor polled `Gone` mid-search.
    Lost,
    /// A provisioned machine that never reported a usable endpoint within the
    /// readiness timeout, including one that went `Gone` while provisioning.
    NeverReady,
    /// A machine that reported ready but failed the ssh worker probe, so it
    /// cannot search work.
    ProbeFailed,
    /// A machine that answered but could not be given the program the search's
    /// format is routed to — the delivery or the install it searches failed — so it
    /// can serve no worker for this search.
    InstallFailed,
}

impl Store {
    /// Records `incident` under its machine, replacing any incident already at
    /// the same key — a repeated record of one attempt's incident is
    /// idempotent. The machine's directory is created on first write. The
    /// provider, machine, and tag are validated before any path is touched.
    pub fn put_machine_incident(&self, incident: &MachineIncident) -> Result<()> {
        validate_charset("incident provider", &incident.provider)?;
        validate_charset("incident machine", &incident.machine)?;
        validate_tag(&incident.tag)?;
        let dir = layout::machine_dir(self.root(), &incident.provider, &incident.machine);
        atomic::create_dir_durable(&dir)?;
        let path = layout::machine_incident_path(
            self.root(),
            &incident.provider,
            &incident.machine,
            &incident.tag,
            incident.occurred_ms,
        );
        atomic::write_atomic(self.root(), &path, &incident_bytes(incident))
    }

    /// Every incident the ledger holds, across every machine store-wide.
    ///
    /// A file that does not parse, or whose incident names a different machine
    /// or attempt than its path, is [`Error::Corruption`] naming the file: the
    /// ledger is store state, so a read either verifies or fails.
    pub fn machine_incidents(&self) -> Result<Vec<MachineIncident>> {
        let mut incidents = Vec::new();
        // Keyed one level deeper than the other ledgers: a directory per
        // machine, an incident file per attempt within it.
        for machine in ledger::groups(&layout::machines_ledger_dir(self.root()))? {
            for (path, incident) in
                ledger::entries::<MachineIncident>(&machine, "machine incident")?
            {
                verify_placement(&path, &incident)?;
                incidents.push(incident);
            }
        }
        Ok(incidents)
    }
}

/// Fails when `incident` sits at a path its own fields do not name — a machine
/// directory or an incident file it was moved off of.
fn verify_placement(path: &std::path::Path, incident: &MachineIncident) -> Result<()> {
    let file = path.file_name().and_then(|name| name.to_str());
    let parent = path
        .parent()
        .and_then(|dir| dir.file_name())
        .and_then(|name| name.to_str());
    let key = incident_key(&incident.tag, incident.occurred_ms);
    let machine = machine_key(&incident.provider, &incident.machine);
    if file != Some(key.as_str()) || parent != Some(machine.as_str()) {
        return Err(Error::Corruption(format!(
            "machine incident {} names {}/{}",
            path.display(),
            machine,
            key,
        )));
    }
    Ok(())
}

/// One machine's directory name: `<provider>-<machine>`.
pub(crate) fn machine_key(provider: &str, machine: &str) -> String {
    format!("{provider}-{machine}")
}

/// One incident's file name: `<tag>-<occurred_ms>`.
pub(crate) fn incident_key(tag: &str, occurred_ms: u64) -> String {
    format!("{tag}-{occurred_ms}")
}

/// Renders an incident: pretty-printed JSON with a trailing newline, so the
/// ledger reads on a terminal.
fn incident_bytes(incident: &MachineIncident) -> Vec<u8> {
    // The incident is plain strings, an enum, and an integer; serialization
    // cannot fail.
    let mut text = serde_json::to_string_pretty(incident).expect("machine incident serializes");
    text.push('\n');
    text.into_bytes()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sima_core::{Error, Result};

    use super::{IncidentKind, MachineIncident};
    use crate::Store;
    use crate::testutil::temp_store;

    /// An incident of `kind` against `machine`, observed by `tag` at
    /// `occurred_ms`, rented from the stub provider.
    fn incident(machine: &str, kind: IncidentKind, tag: &str, occurred_ms: u64) -> MachineIncident {
        MachineIncident {
            provider: "stub".to_string(),
            machine: machine.to_string(),
            kind,
            tag: tag.to_string(),
            occurred_ms,
        }
    }

    #[test]
    fn an_incident_round_trips_through_the_ledger() -> Result<()> {
        let (dir, store) = temp_store();
        let incident = incident("81234", IncidentKind::Lost, "sima-tag-0", 1_700_000_000_000);
        store.put_machine_incident(&incident)?;
        assert_eq!(store.machine_incidents()?, vec![incident]);
        // The incident path is part of the fixed layout contract.
        assert!(
            dir.path()
                .join("machines")
                .join("stub-81234")
                .join("sima-tag-0-1700000000000")
                .is_file()
        );
        Ok(())
    }

    #[test]
    fn the_incident_kind_serializes_snake_case() -> Result<()> {
        let (dir, store) = temp_store();
        store.put_machine_incident(&incident(
            "81234",
            IncidentKind::NeverReady,
            "sima-tag-0",
            1,
        ))?;
        let text = fs::read_to_string(
            dir.path()
                .join("machines")
                .join("stub-81234")
                .join("sima-tag-0-1"),
        )
        .expect("read the incident");
        assert!(
            text.contains(r#""kind": "never_ready""#),
            "the kind is snake_case: {text}"
        );
        Ok(())
    }

    #[test]
    fn two_incidents_for_one_machine_list_as_two() -> Result<()> {
        let (_dir, store) = temp_store();
        store.put_machine_incident(&incident("81234", IncidentKind::Lost, "sima-tag-0", 1))?;
        store.put_machine_incident(&incident(
            "81234",
            IncidentKind::ProbeFailed,
            "sima-tag-1",
            2,
        ))?;
        assert_eq!(store.machine_incidents()?.len(), 2);
        Ok(())
    }

    #[test]
    fn incidents_across_machines_and_providers_coexist() -> Result<()> {
        let (_dir, store) = temp_store();
        let mut foreign = incident("90000", IncidentKind::Lost, "sima-tag-1", 1);
        foreign.provider = "vastai".to_string();
        store.put_machine_incident(&incident("81234", IncidentKind::Lost, "sima-tag-0", 1))?;
        store.put_machine_incident(&foreign)?;
        let listed = store.machine_incidents()?;
        assert_eq!(listed.len(), 2);
        assert!(listed.contains(&foreign));
        Ok(())
    }

    #[test]
    fn recording_one_incident_twice_leaves_one() -> Result<()> {
        let (_dir, store) = temp_store();
        // The same attempt's incident recorded again lands on the same
        // (machine, tag, stamp) key and replaces rather than appends.
        let incident = incident("81234", IncidentKind::Lost, "sima-tag-0", 1);
        store.put_machine_incident(&incident)?;
        store.put_machine_incident(&incident)?;
        assert_eq!(store.machine_incidents()?, vec![incident]);
        Ok(())
    }

    #[test]
    fn an_empty_store_lists_no_incidents() -> Result<()> {
        let (_dir, store) = temp_store();
        assert!(store.machine_incidents()?.is_empty());
        Ok(())
    }

    #[test]
    fn an_unparseable_incident_is_corruption_naming_the_file() -> Result<()> {
        let (dir, store) = temp_store();
        store.put_machine_incident(&incident("81234", IncidentKind::Lost, "sima-tag-0", 1))?;
        let path = dir
            .path()
            .join("machines")
            .join("stub-81234")
            .join("sima-bad-9");
        fs::write(&path, b"not json").expect("write a garbage incident");
        let listed = store.machine_incidents();
        let Err(Error::Corruption(msg)) = listed else {
            panic!("a malformed incident must be corruption, got {listed:?}");
        };
        assert!(
            msg.contains("sima-bad-9"),
            "corruption names the file: {msg}"
        );
        Ok(())
    }

    #[test]
    fn an_incident_moved_off_its_key_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let incident = incident("81234", IncidentKind::Lost, "sima-tag-0", 1);
        let machine_dir = dir.path().join("machines").join("stub-81234");
        // Both halves of the file key are checked: the tag and the stamp.
        for moved in ["sima-other-1", "sima-tag-0-2"] {
            store.put_machine_incident(&incident)?;
            fs::rename(machine_dir.join("sima-tag-0-1"), machine_dir.join(moved))
                .expect("move the incident off its key");
            assert!(
                matches!(store.machine_incidents(), Err(Error::Corruption(_))),
                "the incident at {moved} was accepted"
            );
            fs::remove_file(machine_dir.join(moved)).expect("clear the moved incident");
        }
        Ok(())
    }

    #[test]
    fn an_incident_under_the_wrong_machine_directory_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        store.put_machine_incident(&incident("81234", IncidentKind::Lost, "sima-tag-0", 1))?;
        let machines = dir.path().join("machines");
        // Its fields name stub-81234; moving the whole directory contradicts
        // the path, and a read must refuse it.
        fs::rename(machines.join("stub-81234"), machines.join("stub-99999"))
            .expect("move the machine directory");
        assert!(matches!(
            store.machine_incidents(),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn a_component_outside_the_charset_is_refused_before_any_write() -> Result<()> {
        let (dir, store) = temp_store();
        let refused = [
            incident("../escape", IncidentKind::Lost, "sima-tag-0", 1),
            incident("", IncidentKind::Lost, "sima-tag-0", 1),
            incident("ABC", IncidentKind::Lost, "sima-tag-0", 1),
            MachineIncident {
                provider: "../escape".to_string(),
                ..incident("81234", IncidentKind::Lost, "sima-tag-0", 1)
            },
            MachineIncident {
                tag: "sima  0".to_string(),
                ..incident("81234", IncidentKind::Lost, "sima-tag-0", 1)
            },
        ];
        for incident in refused {
            assert!(
                matches!(
                    store.put_machine_incident(&incident),
                    Err(Error::Validation(_))
                ),
                "put accepted {incident:?}"
            );
        }
        // Nothing reached the filesystem.
        assert!(!dir.path().join("machines").join("stub-").exists());
        assert!(Store::open(dir.path())?.machine_incidents()?.is_empty());
        Ok(())
    }

    #[test]
    fn the_reputation_ledger_leaves_the_instance_and_spend_listings_untouched() -> Result<()> {
        let (_dir, store) = temp_store();
        store.put_machine_incident(&incident("81234", IncidentKind::Lost, "sima-tag-0", 1))?;
        // The reputation ledger is a separate directory: the existing record
        // listings read exactly what they did before it held anything.
        assert!(store.instance_records()?.is_empty());
        assert_eq!(store.machine_incidents()?.len(), 1);
        Ok(())
    }

    #[test]
    fn opening_a_store_without_a_machines_directory_creates_it() -> Result<()> {
        let (dir, store) = temp_store();
        drop(store);
        fs::remove_dir_all(dir.path().join("machines")).expect("remove the reputation directory");
        let store = Store::open(dir.path())?;
        assert!(dir.path().join("machines").is_dir());
        assert!(store.machine_incidents()?.is_empty());
        Ok(())
    }
}
