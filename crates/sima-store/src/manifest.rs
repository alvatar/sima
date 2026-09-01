//! The search manifest: typed data plus its serde JSON mirror.
//!
//! The manifest is the object every equality-based acceptance criterion
//! compares, so its bytes are canonicalized: pretty-printed 2-space JSON,
//! trailing newline, entries strictly ascending by task key. Serde runs
//! on a private hex-string mirror; conversion to the typed form is where
//! validation lives. The manifest is human-readable index data,
//! serialized with serde, and is never identity-bearing.

use serde::{Deserialize, Serialize};
use sima_core::{Error, Hash, Result};
use sima_model::{SearchId, TaskKey};

/// A finalized search's manifest: the search it belongs to and one entry per
/// committed task, strictly ascending by task key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// The search this manifest finalizes. Because a search id is the hash of
    /// the search's canonical config bytes, this field simultaneously
    /// addresses the stored config object.
    pub search: SearchId,
    /// The search's committed tasks, strictly ascending by task key.
    pub entries: Vec<ManifestEntry>,
}

/// One manifest entry: a task and the record committed for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestEntry {
    /// The task's key.
    pub task: TaskKey,
    /// The address of the record object answering the task.
    pub record: Hash,
}

/// The serde mirror of [`Manifest`]: digests as lowercase hex strings.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestJson {
    search: String,
    entries: Vec<EntryJson>,
}

/// The serde mirror of [`ManifestEntry`].
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryJson {
    task: String,
    record: String,
}

/// Renders the canonical manifest bytes: pretty-printed 2-space JSON with
/// a trailing newline.
pub(crate) fn to_json_bytes(manifest: &Manifest) -> Vec<u8> {
    let mirror = ManifestJson {
        search: manifest.search.to_string(),
        entries: manifest
            .entries
            .iter()
            .map(|entry| EntryJson {
                task: entry.task.to_string(),
                record: entry.record.to_string(),
            })
            .collect(),
    };
    // The mirror is plain strings and vecs; serialization cannot fail.
    let mut text = serde_json::to_string_pretty(&mirror).expect("manifest mirror serializes");
    text.push('\n');
    text.into_bytes()
}

/// Parses and validates manifest bytes read from `dir_search`'s directory.
/// Malformed JSON, bad hex, unsorted or duplicate entries, and a `search`
/// field disagreeing with the directory are all [`Error::Corruption`] —
/// the file contradicts what the store wrote.
pub(crate) fn from_json_bytes(bytes: &[u8], dir_search: &SearchId) -> Result<Manifest> {
    let mirror: ManifestJson = serde_json::from_slice(bytes).map_err(|e| {
        Error::Corruption(format!(
            "manifest for search {dir_search} does not parse: {e}"
        ))
    })?;
    let search = SearchId::from_hex(&mirror.search).map_err(|_| {
        Error::Corruption(format!(
            "manifest for search {dir_search} names a malformed search id {:?}",
            mirror.search
        ))
    })?;
    if search != *dir_search {
        return Err(Error::Corruption(format!(
            "manifest under search {dir_search} names search {search}"
        )));
    }
    let mut entries = Vec::with_capacity(mirror.entries.len());
    for entry in &mirror.entries {
        let task = TaskKey::from_hex(&entry.task).map_err(|_| {
            Error::Corruption(format!(
                "manifest for search {dir_search} holds a malformed task key {:?}",
                entry.task
            ))
        })?;
        let record = Hash::from_hex(&entry.record).map_err(|_| {
            Error::Corruption(format!(
                "manifest for search {dir_search} holds a malformed record hash {:?}",
                entry.record
            ))
        })?;
        if let Some(prev) = entries.last().map(|e: &ManifestEntry| e.task)
            && prev >= task
        {
            return Err(Error::Corruption(format!(
                "manifest for search {dir_search} entries out of order: {prev} then {task}"
            )));
        }
        entries.push(ManifestEntry { task, record });
    }
    Ok(Manifest { search, entries })
}

#[cfg(test)]
mod tests {
    use super::{Manifest, ManifestEntry, from_json_bytes, to_json_bytes};
    use sima_core::{Error, Result, hash_bytes};
    use sima_model::{SearchId, TaskKey};

    /// A manifest over placeholder digests: search 32x0a, entries under keys
    /// 32x11 < 32x22, records 32xaa and 32xbb.
    fn sample_manifest() -> Result<Manifest> {
        Ok(Manifest {
            search: SearchId::from_hex(&"0a".repeat(32))?,
            entries: vec![
                ManifestEntry {
                    task: TaskKey::from_hex(&"11".repeat(32))?,
                    record: sima_core::Hash::from_hex(&"aa".repeat(32))?,
                },
                ManifestEntry {
                    task: TaskKey::from_hex(&"22".repeat(32))?,
                    record: sima_core::Hash::from_hex(&"bb".repeat(32))?,
                },
            ],
        })
    }

    #[test]
    fn json_bytes_round_trip() -> Result<()> {
        let manifest = sample_manifest()?;
        let bytes = to_json_bytes(&manifest);
        assert_eq!(from_json_bytes(&bytes, &manifest.search)?, manifest);
        Ok(())
    }

    #[test]
    fn json_ends_with_a_trailing_newline() -> Result<()> {
        let bytes = to_json_bytes(&sample_manifest()?);
        assert_eq!(bytes.last(), Some(&b'\n'));
        Ok(())
    }

    #[test]
    fn a_search_field_disagreeing_with_the_directory_is_corruption() -> Result<()> {
        let manifest = sample_manifest()?;
        let bytes = to_json_bytes(&manifest);
        let other = SearchId::from_hash(hash_bytes(b"a different search"));
        assert!(matches!(
            from_json_bytes(&bytes, &other),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    /// Asserts that `json` is rejected as corruption when read for the
    /// sample search.
    fn assert_corrupt(json: &str) {
        let search = SearchId::from_hex(&"0a".repeat(32)).expect("search id");
        assert!(
            matches!(
                from_json_bytes(json.as_bytes(), &search),
                Err(Error::Corruption(_))
            ),
            "must reject: {json}"
        );
    }

    #[test]
    fn malformed_json_is_corruption() {
        assert_corrupt("not json at all");
        assert_corrupt("{}");
        assert_corrupt("{\"search\": 7, \"entries\": []}");
    }

    #[test]
    fn bad_hex_is_corruption() {
        let search = "0a".repeat(32);
        assert_corrupt(&format!(
            "{{\"search\": \"{search}\", \"entries\": [{{\"task\": \"zz\", \"record\": \"{}\"}}]}}",
            "aa".repeat(32)
        ));
        assert_corrupt("{\"search\": \"UPPER\", \"entries\": []}");
    }

    #[test]
    fn unsorted_entries_are_corruption() {
        let search = "0a".repeat(32);
        let (k1, k2) = ("11".repeat(32), "22".repeat(32));
        let rec = "aa".repeat(32);
        assert_corrupt(&format!(
            "{{\"search\": \"{search}\", \"entries\": [\
             {{\"task\": \"{k2}\", \"record\": \"{rec}\"}}, \
             {{\"task\": \"{k1}\", \"record\": \"{rec}\"}}]}}"
        ));
    }

    #[test]
    fn duplicate_task_keys_are_corruption() {
        let search = "0a".repeat(32);
        let k1 = "11".repeat(32);
        let rec = "aa".repeat(32);
        assert_corrupt(&format!(
            "{{\"search\": \"{search}\", \"entries\": [\
             {{\"task\": \"{k1}\", \"record\": \"{rec}\"}}, \
             {{\"task\": \"{k1}\", \"record\": \"{rec}\"}}]}}"
        ));
    }

    #[test]
    fn unknown_fields_are_corruption() {
        let search = "0a".repeat(32);
        assert_corrupt(&format!(
            "{{\"search\": \"{search}\", \"entries\": [], \"extra\": 1}}"
        ));
    }
}
