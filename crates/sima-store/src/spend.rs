//! The spend ledger: one entry per rental that has been closed out.
//!
//! An instance record lives only as long as its machine: clearing it is the
//! last step of teardown, and with it the rate and lifetime of that rental
//! would be gone. The spend ledger is where the rental's cost survives that
//! removal, so a search's total spend stays readable across every machine it
//! ever rented, and across the process boundary a crash draws.
//!
//! Entries are operational and serde-serialized, like instance records, and
//! never identity-bearing. An entry is keyed by its rental's tag and the
//! stamp its instance record was written under: a repeated close of one
//! rental reproduces that key and overwrites, while two rentals that reused
//! a tag across process restarts carry distinct stamps and coexist.

use serde::{Deserialize, Serialize};
use sima_core::{Error, Result};

use crate::atomic;
use crate::instances::validate_tag;
use crate::layout;
use crate::ledger;
use crate::store::Store;

/// Characters an owner directory name is made of: the search id's hex form.
const OWNER_HEX_LEN: usize = 64;

/// One closed rental: what a machine cost from its acquisition attempt's
/// record stamp to its confirmed destruction.
///
/// The provider layer computes the cost and writes the entry; the store
/// holds it verbatim and does no money arithmetic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpendEntry {
    /// The rental's tag, which is the instance record's key.
    pub tag: String,
    /// The provider the machine was rented from.
    pub provider: String,
    /// The owning search, full 64-character hex.
    pub owner: String,
    /// The rate the record carried at close.
    pub price_micro_usd_hour: u64,
    /// The instance record's `created_ms`: where the charged window opens.
    pub started_ms: u64,
    /// Wall-clock milliseconds since the epoch at close-out.
    pub ended_ms: u64,
    /// What the window cost at the rate, as the writer computed it.
    pub cost_micro_usd: u64,
}

impl Store {
    /// Places `entry` under its owner, tag, and start stamp, replacing any
    /// entry already there — a repeated close of one rental lands on the
    /// same key, which is what makes close-out idempotent. The owner's
    /// directory is created on first write.
    pub fn put_spend(&self, entry: &SpendEntry) -> Result<()> {
        validate_tag(&entry.tag)?;
        validate_owner(&entry.owner)?;
        let dir = layout::spend_dir(self.root(), &entry.owner);
        atomic::create_dir_durable(&dir)?;
        let path = layout::spend_path(self.root(), &entry.owner, &entry.tag, entry.started_ms);
        atomic::write_atomic(self.root(), &path, &entry_bytes(entry))
    }

    /// Every entry `owner` has closed out. An owner that has closed none
    /// holds no directory and lists empty.
    ///
    /// A file that does not parse, or whose entry names a different tag or
    /// start stamp than its file name, is [`Error::Corruption`] naming the
    /// file: the ledger is store state, so a read either verifies or fails.
    pub fn spend_entries(&self, owner: &str) -> Result<Vec<SpendEntry>> {
        validate_owner(owner)?;
        let mut spend = Vec::new();
        for (path, entry) in
            ledger::entries::<SpendEntry>(&layout::spend_dir(self.root(), owner), "spend entry")?
        {
            if Some(key(&entry.tag, entry.started_ms).as_str())
                != path.file_name().and_then(|name| name.to_str())
            {
                return Err(Error::Corruption(format!(
                    "spend entry {} names the rental {:?} started at {}",
                    path.display(),
                    entry.tag,
                    entry.started_ms
                )));
            }
            spend.push(entry);
        }
        Ok(spend)
    }
}

/// One entry's file name: `<tag>-<started_ms>`.
pub(crate) fn key(tag: &str, started_ms: u64) -> String {
    format!("{tag}-{started_ms}")
}

/// Renders an entry: pretty-printed JSON with a trailing newline, so the
/// ledger reads on a terminal.
fn entry_bytes(entry: &SpendEntry) -> Vec<u8> {
    // The entry is plain strings and integers; serialization cannot fail.
    let mut text = serde_json::to_string_pretty(entry).expect("spend entry serializes");
    text.push('\n');
    text.into_bytes()
}

/// Accepts an owner in the search id's hex form, which becomes a directory
/// name under the spend ledger.
fn validate_owner(owner: &str) -> Result<()> {
    if owner.len() == OWNER_HEX_LEN && owner.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(());
    }
    Err(Error::Validation(format!(
        "spend owner {owner:?} must be {OWNER_HEX_LEN} hex characters"
    )))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sima_core::{Error, Result};

    use crate::spend::SpendEntry;
    use crate::testutil::{sample_search_config, temp_store};

    /// The owner for `root_seed`, in the hex form the ledger keys on.
    fn owner(root_seed: u64) -> String {
        sample_search_config(root_seed).id().to_string()
    }

    /// An entry for `tag` started at `started_ms`, owned by the search for
    /// `root_seed`.
    fn entry(tag: &str, started_ms: u64, root_seed: u64) -> SpendEntry {
        SpendEntry {
            tag: tag.to_string(),
            provider: "stub".to_string(),
            owner: owner(root_seed),
            price_micro_usd_hour: 82_400,
            started_ms,
            ended_ms: started_ms + 3_600_000,
            cost_micro_usd: 82_400,
        }
    }

    #[test]
    fn an_entry_round_trips_through_the_spend_ledger() -> Result<()> {
        let (dir, store) = temp_store();
        let entry = entry("sima-0123456789abcdef-42-0", 1_700_000_000_000, 7);
        store.put_spend(&entry)?;
        assert_eq!(store.spend_entries(&owner(7))?, vec![entry]);
        // The entry path is part of the fixed layout contract.
        assert!(
            dir.path()
                .join("spend")
                .join(owner(7))
                .join("sima-0123456789abcdef-42-0-1700000000000")
                .is_file()
        );
        Ok(())
    }

    #[test]
    fn closing_one_rental_twice_leaves_one_entry() -> Result<()> {
        let (_dir, store) = temp_store();
        let first = entry("sima-tag-0", 1_700_000_000_000, 7);
        store.put_spend(&first)?;
        // The same rental closed again: same tag and start stamp, a later
        // end. The key is reproduced from the record, so the second close
        // replaces the first rather than adding to it.
        let second = SpendEntry {
            ended_ms: first.ended_ms + 60_000,
            cost_micro_usd: first.cost_micro_usd + 1_374,
            ..first.clone()
        };
        store.put_spend(&second)?;
        assert_eq!(store.spend_entries(&owner(7))?, vec![second]);
        Ok(())
    }

    #[test]
    fn two_rentals_under_one_tag_started_apart_both_survive() -> Result<()> {
        let (_dir, store) = temp_store();
        // Tags repeat across process restarts; the start stamp is what keeps
        // an older rental's cost from being replaced by a newer one's.
        let older = entry("sima-tag-0", 1_700_000_000_000, 7);
        let newer = entry("sima-tag-0", 1_700_000_900_000, 7);
        store.put_spend(&older)?;
        store.put_spend(&newer)?;
        let listed = store.spend_entries(&owner(7))?;
        assert_eq!(listed.len(), 2);
        assert!(listed.contains(&older));
        assert!(listed.contains(&newer));
        Ok(())
    }

    #[test]
    fn an_owners_entries_are_the_only_ones_it_lists() -> Result<()> {
        let (_dir, store) = temp_store();
        let mine = entry("sima-tag-0", 1_700_000_000_000, 7);
        let theirs = entry("sima-tag-1", 1_700_000_000_000, 8);
        store.put_spend(&mine)?;
        store.put_spend(&theirs)?;
        assert_eq!(store.spend_entries(&owner(7))?, vec![mine]);
        assert_eq!(store.spend_entries(&owner(8))?, vec![theirs]);
        Ok(())
    }

    #[test]
    fn an_owner_that_closed_nothing_lists_empty() -> Result<()> {
        let (_dir, store) = temp_store();
        assert!(store.spend_entries(&owner(7))?.is_empty());
        Ok(())
    }

    #[test]
    fn an_unparseable_entry_is_corruption_naming_the_file() -> Result<()> {
        let (dir, store) = temp_store();
        store.put_spend(&entry("sima-tag-0", 1_700_000_000_000, 7))?;
        let path = dir.path().join("spend").join(owner(7)).join("sima-bad-1");
        fs::write(&path, b"not json").expect("write a garbage entry");
        let listed = store.spend_entries(&owner(7));
        let Err(Error::Corruption(msg)) = listed else {
            panic!("a malformed entry must be corruption, got {listed:?}");
        };
        assert!(
            msg.contains("sima-bad-1"),
            "corruption names the file: {msg}"
        );
        Ok(())
    }

    #[test]
    fn an_entry_moved_off_its_key_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let entry = entry("sima-tag-0", 1_700_000_000_000, 7);
        let owner_dir = dir.path().join("spend").join(owner(7));
        // Both halves of the key are checked: the tag and the start stamp.
        for moved in ["sima-other-1700000000000", "sima-tag-0-1700000000001"] {
            store.put_spend(&entry)?;
            fs::rename(
                owner_dir.join("sima-tag-0-1700000000000"),
                owner_dir.join(moved),
            )
            .expect("move the entry off its key");
            assert!(
                matches!(store.spend_entries(&owner(7)), Err(Error::Corruption(_))),
                "the entry at {moved} was accepted"
            );
            fs::remove_file(owner_dir.join(moved)).expect("clear the moved entry");
        }
        Ok(())
    }

    #[test]
    fn a_tag_outside_the_charset_is_rejected() -> Result<()> {
        let (_dir, store) = temp_store();
        // The tag becomes part of a file name, so the charset is enforced
        // before it reaches the filesystem.
        for tag in ["../escape", "", "sima-ABC", "sima_0", "sima 0"] {
            assert!(
                matches!(
                    store.put_spend(&entry(tag, 1_700_000_000_000, 7)),
                    Err(Error::Validation(_))
                ),
                "put accepted the tag {tag:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn an_owner_outside_the_hex_form_is_rejected_by_put_and_list() -> Result<()> {
        let (_dir, store) = temp_store();
        // The owner becomes a directory name, so its form is enforced too.
        for name in [
            "..",
            "",
            "not-a-search-id",
            &"a".repeat(63),
            &"g".repeat(64),
        ] {
            let mut entry = entry("sima-tag-0", 1_700_000_000_000, 7);
            entry.owner = name.to_string();
            assert!(
                matches!(store.put_spend(&entry), Err(Error::Validation(_))),
                "put accepted the owner {name:?}"
            );
            assert!(
                matches!(store.spend_entries(name), Err(Error::Validation(_))),
                "list accepted the owner {name:?}"
            );
        }
        Ok(())
    }
}
