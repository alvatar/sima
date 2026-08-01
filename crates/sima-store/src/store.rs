//! The [`Store`] handle and its open path.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use sima_core::{Error, Result};

use crate::atomic::{self, io_error};
use crate::layout;
use crate::pack::cache::PackCache;

/// The store format this binary writes and reads. The number moves only on
/// a layout change; a store marked otherwise is refused at open, which is
/// what turns a version mismatch into one sentence instead of missing
/// objects mid-operation.
const FORMAT_VERSION: u32 = 1;

/// Handle over a store root directory — the only durable state in sima.
///
/// Every method takes `&self` and is safe under concurrent use from
/// multiple threads: durable files are placed by atomic rename, and
/// writers racing on one path either converge on identical bytes or fail
/// with `Corruption`.
pub struct Store {
    root: PathBuf,
    /// Where this handle's packed objects live. Derived state, rebuilt
    /// from `packs/` whenever a lookup misses, so it is behind a lock
    /// rather than in the handle's type: reads stay `&self`.
    packs: RwLock<PackCache>,
}

impl Store {
    /// Creates or opens the store at `root`, building the directory
    /// skeleton (`objects/`, `packs/`, `tmp/`, `tasks/`, `runs/`,
    /// `instances/`, `spend/`, `machines/`) durably where
    /// absent. A fresh root and an existing store open identically —
    /// resume is a reopen.
    ///
    /// The format marker settles in the same pass: a store already marked
    /// with another version is [`Error::Validation`] naming both versions,
    /// and nothing is created under it; a store without a marker gains one,
    /// so a store laid out before the marker existed upgrades on first
    /// touch.
    pub fn open(root: impl Into<PathBuf>) -> Result<Store> {
        let root = root.into();
        // The version check comes first: a store this binary cannot read is
        // refused before anything is created inside it.
        let marked = read_format_marker(&root)?;
        if let Some(version) = marked
            && version != FORMAT_VERSION
        {
            return Err(Error::Validation(format!(
                "store at {} is format version {version}; this binary reads version {FORMAT_VERSION}",
                root.display()
            )));
        }
        for dir in [
            layout::objects_dir(&root),
            layout::packs_dir(&root),
            layout::tmp_dir(&root),
            layout::tasks_dir(&root),
            layout::runs_dir(&root),
            layout::instances_dir(&root),
            layout::spend_ledger_dir(&root),
            layout::machines_ledger_dir(&root),
        ] {
            atomic::create_dir_durable(&dir)?;
        }
        if marked.is_none() {
            // Written through the atomic path, so racing opens converge on
            // identical bytes instead of tearing the marker.
            atomic::write_atomic(
                &root,
                &layout::format_marker_path(&root),
                format!("{FORMAT_VERSION}\n").as_bytes(),
            )?;
        }
        Ok(Store {
            root,
            packs: RwLock::new(PackCache::new()),
        })
    }

    /// The root directory this store was opened on. All store paths are
    /// derived from it.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The pack cache for reading, recovering a poisoned lock. Poisoning
    /// would mean a thread panicked holding the lock; the cache's own
    /// operations do not panic, and a stale or smaller view is corrected by
    /// the next rescan, so the recovered state is safe to read.
    pub(crate) fn packs(&self) -> RwLockReadGuard<'_, PackCache> {
        self.packs
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// The pack cache for updating, recovering a poisoned lock for the
    /// reason [`Store::packs`] does.
    pub(crate) fn packs_mut(&self) -> RwLockWriteGuard<'_, PackCache> {
        self.packs
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

/// The format version `root` is marked with: `None` when the marker is
/// absent. Content that is not a version number is [`Error::Corruption`] —
/// the marker is the one file that says what the layout beneath it means.
fn read_format_marker(root: &Path) -> Result<Option<u32>> {
    let path = layout::format_marker_path(root);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_error(&path, e)),
    };
    text.trim()
        .parse::<u32>()
        .map(Some)
        .map_err(|_| Error::Corruption(format!("{} holds no format version", path.display())))
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
