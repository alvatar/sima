//! The [`Store`] handle and its open path.

use std::path::{Path, PathBuf};

use sima_core::Result;

use crate::atomic;
use crate::layout;

/// Handle over a store root directory — the only durable state in sima.
///
/// Every method takes `&self` and is safe under concurrent use from
/// multiple threads: durable files are placed by atomic rename, and
/// writers racing on one path either converge on identical bytes or fail
/// with `Corruption`.
pub struct Store {
    root: PathBuf,
}

impl Store {
    /// Creates or opens the store at `root`, building the directory
    /// skeleton (`objects/`, `tmp/`, `tasks/`, `runs/`, `instances/`,
    /// `spend/`, `machines/`) durably where
    /// absent. A fresh root and an existing store open identically —
    /// resume is a reopen.
    pub fn open(root: impl Into<PathBuf>) -> Result<Store> {
        let root = root.into();
        for dir in [
            layout::objects_dir(&root),
            layout::tmp_dir(&root),
            layout::tasks_dir(&root),
            layout::runs_dir(&root),
            layout::instances_dir(&root),
            layout::spend_ledger_dir(&root),
            layout::machines_ledger_dir(&root),
        ] {
            atomic::create_dir_durable(&dir)?;
        }
        Ok(Store { root })
    }

    /// The root directory this store was opened on. All store paths are
    /// derived from it.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use crate::Store;
    use sima_core::{Error, Result};
    use std::fs;

    #[test]
    fn open_creates_the_skeleton_on_a_fresh_root() -> Result<()> {
        let dir = tempfile::tempdir().expect("create temp dir");
        Store::open(dir.path())?;
        // The skeleton directories, pinned by name — the disk layout is a
        // fixed contract.
        for sub in [
            "objects",
            "packs",
            "tmp",
            "tasks",
            "runs",
            "instances",
            "spend",
            "machines",
        ] {
            assert!(dir.path().join(sub).is_dir(), "missing skeleton dir {sub}");
        }
        Ok(())
    }

    #[test]
    fn open_writes_the_format_marker_on_a_fresh_store() -> Result<()> {
        let dir = tempfile::tempdir().expect("create temp dir");
        Store::open(dir.path())?;
        // The marker is the store-format contract in one line, pinned by
        // its bytes.
        assert_eq!(
            fs::read(dir.path().join("format")).expect("read marker"),
            b"1\n"
        );
        Ok(())
    }

    #[test]
    fn open_settles_the_marker_on_a_store_that_lacks_it() -> Result<()> {
        let dir = tempfile::tempdir().expect("create temp dir");
        // A store laid out before the marker existed: the skeleton alone.
        for sub in ["objects", "tmp", "tasks", "runs"] {
            fs::create_dir_all(dir.path().join(sub)).expect("create skeleton dir");
        }
        Store::open(dir.path())?;
        assert_eq!(
            fs::read(dir.path().join("format")).expect("read marker"),
            b"1\n"
        );
        Ok(())
    }

    #[test]
    fn a_foreign_format_version_refuses_to_open() -> Result<()> {
        let dir = tempfile::tempdir().expect("create temp dir");
        Store::open(dir.path())?;
        fs::write(dir.path().join("format"), b"2\n").expect("write marker");
        match Store::open(dir.path()) {
            Err(Error::Validation(msg)) => {
                // The sentence names both versions, so the mismatch is
                // actionable without reading the file.
                assert!(msg.contains('2'), "the store's version: {msg}");
                assert!(msg.contains('1'), "this binary's version: {msg}");
            }
            Err(other) => panic!("expected Validation, got {other}"),
            Ok(_) => panic!("a foreign format version must not open"),
        }
        Ok(())
    }

    #[test]
    fn an_unparseable_marker_is_corruption() -> Result<()> {
        let dir = tempfile::tempdir().expect("create temp dir");
        Store::open(dir.path())?;
        fs::write(dir.path().join("format"), b"not a version\n").expect("write marker");
        assert!(matches!(Store::open(dir.path()), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn concurrent_opens_of_a_fresh_root_converge() -> Result<()> {
        let dir = tempfile::tempdir().expect("create temp dir");
        // Racing opens write the marker through the atomic path, so they
        // converge on identical bytes rather than tearing one.
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| Store::open(dir.path())))
                .collect();
            for handle in handles {
                handle.join().expect("open thread panicked")?;
            }
            Ok::<(), Error>(())
        })?;
        assert_eq!(
            fs::read(dir.path().join("format")).expect("read marker"),
            b"1\n"
        );
        Ok(())
    }

    #[test]
    fn reopen_of_an_existing_store_succeeds() -> Result<()> {
        let dir = tempfile::tempdir().expect("create temp dir");
        Store::open(dir.path())?;
        // Resume is a reopen: an existing root opens identically to a
        // fresh one.
        Store::open(dir.path())?;
        Ok(())
    }

    #[test]
    fn open_on_a_root_occupied_by_a_file_fails_with_io() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let file_root = dir.path().join("occupied");
        fs::write(&file_root, b"a file, not a store").expect("write blocker file");
        assert!(matches!(Store::open(&file_root), Err(Error::Io { .. })));
    }
}
