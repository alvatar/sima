//! The read walk every operational ledger shares.
//!
//! Three ledgers sit beside the search data — the instances a rental holds, the
//! spend those rentals accrued, and the incidents machines were blamed for —
//! and each is a directory of one JSON file per entry. They differ in what an
//! entry is and in what its file name must agree with; they do not differ in
//! how the directory is walked, when an absence is an absence, or what a file
//! that does not parse means.
//!
//! What that leaves each ledger is its own verification, which is the part that
//! knows what the file name says.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use sima_core::{Error, Result};

use crate::atomic::io_error;

/// Every JSON file directly under `dir`, parsed as `T`, with the path each came
/// from.
///
/// A directory that does not exist lists empty: a store that never rented holds
/// no ledger, which is an absence rather than a fault. A file removed between
/// the scan and the read is skipped for the same reason — a teardown that
/// finished is the state the reader would have found one moment later.
///
/// `noun` names what an entry is, so a file that does not parse fails saying
/// which ledger it came from. The ledger is store state, so a read either
/// verifies or fails.
pub(crate) fn entries<T: DeserializeOwned>(dir: &Path, noun: &str) -> Result<Vec<(PathBuf, T)>> {
    let listing = match fs::read_dir(dir) {
        Ok(listing) => listing,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_error(dir, e)),
    };
    let mut parsed = Vec::new();
    for entry in listing {
        let path = entry.map_err(|e| io_error(dir, e))?.path();
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(io_error(&path, e)),
        };
        let value = serde_json::from_slice(&bytes).map_err(|e| {
            Error::Corruption(format!("{noun} {} does not parse: {e}", path.display()))
        })?;
        parsed.push((path, value));
    }
    Ok(parsed)
}

/// Every subdirectory of `dir`, for a ledger keyed one level deeper. Absent
/// lists empty, exactly as [`entries`] does.
pub(crate) fn groups(dir: &Path) -> Result<Vec<PathBuf>> {
    let listing = match fs::read_dir(dir) {
        Ok(listing) => listing,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_error(dir, e)),
    };
    listing
        .map(|entry| Ok(entry.map_err(|e| io_error(dir, e))?.path()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize, PartialEq, Debug)]
    struct Entry {
        name: String,
    }

    #[test]
    fn a_ledger_that_was_never_written_lists_empty() {
        // A store that never rented holds no ledger directory, which is an
        // absence rather than a fault: the reader answers "nothing" and the
        // caller has nothing to reconcile.
        let dir = tempfile::tempdir().expect("temp dir");
        let absent = dir.path().join("never-written");
        assert!(
            entries::<Entry>(&absent, "entry")
                .expect("absence")
                .is_empty()
        );
        assert!(groups(&absent).expect("absence").is_empty());
    }

    #[test]
    fn every_entry_comes_back_with_the_path_it_was_read_from() {
        // The path is what each ledger checks its own entry against, so the
        // walk hands it back rather than keeping it.
        let dir = tempfile::tempdir().expect("temp dir");
        fs::write(dir.path().join("a"), br#"{"name": "a"}"#).expect("write");
        let read = entries::<Entry>(dir.path(), "entry").expect("one entry");
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].0, dir.path().join("a"));
        assert_eq!(read[0].1.name, "a");
    }

    #[test]
    fn a_file_that_does_not_parse_names_its_ledger_and_its_path() {
        let dir = tempfile::tempdir().expect("temp dir");
        fs::write(dir.path().join("bad"), b"not json").expect("write");
        let Err(Error::Corruption(message)) = entries::<Entry>(dir.path(), "spend entry") else {
            panic!("expected a corrupt file to be refused");
        };
        assert!(message.contains("spend entry"), "{message}");
        assert!(message.contains("bad"), "{message}");
    }
}
