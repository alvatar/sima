//! Helpers shared by the store test modules.

use tempfile::TempDir;

use crate::Store;

/// Opens a store over a fresh temporary directory, keeping the directory
/// guard alive for the test's duration.
pub(crate) fn temp_store() -> (TempDir, Store) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path()).expect("open temp store");
    (dir, store)
}
