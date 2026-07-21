//! The instance ledger: one record per rented-machine acquisition attempt.
//!
//! A rented machine outlives the process that rented it whenever that
//! process dies without running its destructors. The ledger is the durable
//! trace that makes such an instance discoverable afterwards: the record is
//! placed under the tag the provider attaches to the machine, before the
//! provider is asked to create it, so every crash point leaves either
//! nothing or a record naming what may exist.
//!
//! Records are operational and serde-serialized, like the journal, and
//! never identity-bearing. The tag is both the ledger key and the file
//! name, so its charset is validated before it reaches the filesystem.

use std::fs;
use std::io::ErrorKind;

use serde::{Deserialize, Serialize};
use sima_core::{Error, Result};

use crate::atomic::{self, io_error};
use crate::layout;
use crate::store::Store;

/// One acquisition attempt's durable trace: what a later invocation needs
/// to destroy an instance its creator did not live to tear down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceRecord {
    /// The ledger key, which is also the tag the provider carries on the
    /// instance created under this attempt.
    pub tag: String,
    /// The provider's stable identifier; reconciliation matches on it.
    pub provider: String,
    /// The owning run, full 64-character hex. Its orchestrator lock answers
    /// whether the owner is still alive.
    pub owner: String,
    /// How far the attempt got, and the instance once there is one.
    pub state: InstanceRecordState,
    /// The offer's rate at intent, the instance's rate once live.
    pub price_micro_usd_hour: u64,
    /// Wall-clock milliseconds since the epoch at intent, like the journal's
    /// stamps. The live write keeps the stamp the attempt began under. The
    /// stamp serves human diagnosis of the ledger: ordering, identity, and
    /// reconciliation all decide from other fields, which is what leaves the
    /// clock free to move backwards.
    pub created_ms: u64,
}

impl InstanceRecord {
    /// The provider-side instance this record names, for a caller that asks
    /// what machine the record leads to without deciding on the state. An
    /// attempt still at intent names none.
    pub fn instance(&self) -> Option<&str> {
        match &self.state {
            InstanceRecordState::Intent => None,
            InstanceRecordState::Live { instance } => Some(instance),
        }
    }
}

/// How far one acquisition attempt got. The instance id lives in the live
/// variant, so a record names a machine exactly when the attempt reached
/// one, and the type carries that pairing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceRecordState {
    /// The provider has been asked, or is about to be asked, for a machine:
    /// an instance carrying the tag may exist.
    Intent,
    /// The provider created this instance.
    Live {
        /// The provider-side instance id.
        instance: String,
    },
}

impl Store {
    /// Places `record` under its tag, replacing any record already there —
    /// that replacement is the intent to live upgrade. The write goes
    /// through the store's atomic-write path, so a reader sees a complete
    /// record or none.
    pub fn put_instance(&self, record: &InstanceRecord) -> Result<()> {
        validate_tag(&record.tag)?;
        let path = layout::instance_path(self.root(), &record.tag);
        atomic::write_atomic(self.root(), &path, &record_bytes(record))
    }

    /// Every record the ledger holds. A file that does not parse, or whose
    /// record names a different tag than its file name, is
    /// [`Error::Corruption`] naming the file: the ledger is store state, so
    /// a read either verifies or fails.
    pub fn instances(&self) -> Result<Vec<InstanceRecord>> {
        let dir = layout::instances_dir(self.root());
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io_error(&dir, e)),
        };
        let mut records = Vec::new();
        for entry in entries {
            let path = entry.map_err(|e| io_error(&dir, e))?.path();
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                // The record was cleared between the scan and the read: a
                // teardown finished, which is exactly the state the reader
                // would have found one moment later.
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(io_error(&path, e)),
            };
            let record: InstanceRecord = serde_json::from_slice(&bytes).map_err(|e| {
                Error::Corruption(format!(
                    "instance record {} does not parse: {e}",
                    path.display()
                ))
            })?;
            if Some(record.tag.as_str()) != path.file_name().and_then(|name| name.to_str()) {
                return Err(Error::Corruption(format!(
                    "instance record {} names the tag {:?}",
                    path.display(),
                    record.tag
                )));
            }
            records.push(record);
        }
        Ok(records)
    }

    /// Clears the record under `tag`. An absent record is `Ok`: a guard's
    /// teardown and a reconciliation pass may clear the same record.
    pub fn remove_instance(&self, tag: &str) -> Result<()> {
        validate_tag(tag)?;
        atomic::remove_file_durable(&layout::instance_path(self.root(), tag))
    }
}

/// Renders a record: pretty-printed JSON with a trailing newline, so the
/// ledger reads on a terminal.
fn record_bytes(record: &InstanceRecord) -> Vec<u8> {
    // The record is plain strings and integers; serialization cannot fail.
    let mut text = serde_json::to_string_pretty(record).expect("instance record serializes");
    text.push('\n');
    text.into_bytes()
}

/// Accepts a tag of one or more `[a-z0-9-]` characters, which is what may
/// become a file name directly under the ledger directory.
fn validate_tag(tag: &str) -> Result<()> {
    if !tag.is_empty()
        && tag
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Ok(());
    }
    Err(Error::Validation(format!(
        "instance tag {tag:?} must be one or more of [a-z0-9-]"
    )))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sima_core::{Error, Result};

    use crate::testutil::{sample_run_config, temp_store};
    use crate::{InstanceRecord, InstanceRecordState, Store};

    /// An intent record under `tag`, owned by the run for `root_seed`.
    fn intent(tag: &str, root_seed: u64) -> InstanceRecord {
        InstanceRecord {
            tag: tag.to_string(),
            provider: "stub".to_string(),
            owner: sample_run_config(root_seed).id().to_string(),
            state: InstanceRecordState::Intent,
            price_micro_usd_hour: 82_400,
            created_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn a_record_round_trips_through_the_ledger() -> Result<()> {
        let (dir, store) = temp_store();
        let record = intent("sima-0123456789abcdef-42-0", 7);
        store.put_instance(&record)?;
        assert_eq!(store.instances()?, vec![record]);
        // The record path is part of the fixed layout contract.
        assert!(
            dir.path()
                .join("instances")
                .join("sima-0123456789abcdef-42-0")
                .is_file()
        );
        Ok(())
    }

    #[test]
    fn an_intent_record_names_its_state_alone_and_holds_no_instance() -> Result<()> {
        let (dir, store) = temp_store();
        let tag = "sima-0123456789abcdef-42-0";
        let record = intent(tag, 7);
        assert_eq!(record.instance(), None);
        store.put_instance(&record)?;
        let text = fs::read_to_string(dir.path().join("instances").join(tag))
            .expect("read the intent record");
        assert!(
            text.contains(r#""state": "intent""#),
            "the intent state is a bare name: {text}"
        );
        Ok(())
    }

    #[test]
    fn a_live_record_carries_its_instance_inside_the_state() -> Result<()> {
        let (dir, store) = temp_store();
        let tag = "sima-0123456789abcdef-42-0";
        let record = InstanceRecord {
            state: InstanceRecordState::Live {
                instance: "i-9".to_string(),
            },
            ..intent(tag, 7)
        };
        // The id is reachable without matching on the state, for callers
        // that only ask what machine the record names.
        assert_eq!(record.instance(), Some("i-9"));
        store.put_instance(&record)?;
        let text =
            fs::read_to_string(dir.path().join("instances").join(tag)).expect("read the record");
        assert!(
            text.contains(r#""instance": "i-9""#),
            "the live state carries the instance: {text}"
        );
        assert_eq!(store.instances()?, vec![record]);
        Ok(())
    }

    #[test]
    fn rewriting_a_tag_upgrades_the_record_in_place() -> Result<()> {
        let (_dir, store) = temp_store();
        let tag = "sima-0123456789abcdef-42-0";
        store.put_instance(&intent(tag, 7))?;
        let live = InstanceRecord {
            state: InstanceRecordState::Live {
                instance: "i-9".to_string(),
            },
            ..intent(tag, 7)
        };
        store.put_instance(&live)?;
        // One tag is one acquisition attempt: the upgrade replaces, never
        // appends.
        assert_eq!(store.instances()?, vec![live]);
        Ok(())
    }

    #[test]
    fn removing_a_record_clears_it_and_an_absent_tag_is_ok() -> Result<()> {
        let (_dir, store) = temp_store();
        let tag = "sima-0123456789abcdef-42-0";
        store.put_instance(&intent(tag, 7))?;
        store.remove_instance(tag)?;
        assert!(store.instances()?.is_empty());
        // Idempotent: a reconciliation pass may race the guard that already
        // cleared the record.
        store.remove_instance(tag)?;
        Ok(())
    }

    #[test]
    fn a_ledger_with_no_records_lists_empty() -> Result<()> {
        let (_dir, store) = temp_store();
        assert!(store.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn an_unparseable_record_is_corruption_naming_the_file() -> Result<()> {
        let (dir, store) = temp_store();
        fs::write(dir.path().join("instances").join("sima-bad"), b"not json")
            .expect("write a garbage record");
        let listed = store.instances();
        let Err(Error::Corruption(msg)) = listed else {
            panic!("a malformed record must be corruption, got {listed:?}");
        };
        assert!(msg.contains("sima-bad"), "corruption names the file: {msg}");
        Ok(())
    }

    #[test]
    fn a_record_whose_tag_disagrees_with_its_file_name_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        store.put_instance(&intent("sima-0123456789abcdef-42-0", 7))?;
        fs::rename(
            dir.path()
                .join("instances")
                .join("sima-0123456789abcdef-42-0"),
            dir.path().join("instances").join("sima-other"),
        )
        .expect("move the record off its key");
        assert!(matches!(store.instances(), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn a_tag_outside_the_charset_is_rejected_by_put_and_remove() -> Result<()> {
        let (_dir, store) = temp_store();
        // The tag becomes a file name, so the charset is enforced before it
        // reaches the filesystem.
        for tag in ["../escape", "", "sima-ABC", "sima_0", "sima 0"] {
            assert!(
                matches!(
                    store.put_instance(&intent(tag, 7)),
                    Err(Error::Validation(_))
                ),
                "put accepted the tag {tag:?}"
            );
            assert!(
                matches!(store.remove_instance(tag), Err(Error::Validation(_))),
                "remove accepted the tag {tag:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn records_of_several_runs_and_providers_coexist() -> Result<()> {
        let (_dir, store) = temp_store();
        let mut written = Vec::new();
        for (index, provider) in ["stub", "vastai"].iter().enumerate() {
            let mut record = intent(&format!("sima-tag-{index}"), index as u64);
            record.provider = (*provider).to_string();
            store.put_instance(&record)?;
            written.push(record);
        }
        let listed = store.instances()?;
        assert_eq!(listed.len(), written.len());
        for record in &written {
            assert!(listed.contains(record), "missing record {record:?}");
        }
        Ok(())
    }

    #[test]
    fn opening_a_store_without_a_ledger_directory_creates_it() -> Result<()> {
        let (dir, store) = temp_store();
        drop(store);
        // A root laid out before the ledger existed opens identically: the
        // skeleton is created where absent.
        fs::remove_dir_all(dir.path().join("instances")).expect("remove the ledger directory");
        let store = Store::open(dir.path())?;
        assert!(dir.path().join("instances").is_dir());
        assert!(store.instances()?.is_empty());
        Ok(())
    }
}
