//! Atomic-write mechanics shared by objects, index entries, and manifests.
//!
//! Every durable file is placed the same way: full content to a unique
//! `tmp/<pid>-<seq>` file, fsync, rename into place, fsync of the
//! destination's parent directory. Directories are created through
//! [`create_dir_durable`], which fsyncs the parent so the new entry
//! itself survives a crash. A reader — including a process resuming
//! after SIGKILL — therefore observes a complete file or none. Leftover
//! `tmp/` files after a crash are inert; sweeping them is retention
//! work, deferred to P6.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sima_core::{Error, Result};

use crate::layout;

/// Process-global sequence keeping concurrent in-flight writes on
/// distinct `tmp/` paths.
static SEQ: AtomicU64 = AtomicU64::new(0);

/// Wraps an OS failure at `path` into the project error.
pub(crate) fn io_error(path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Writes `bytes` to `dest` atomically through the store's `tmp/`
/// directory, replacing any existing file. Concurrent writers to one
/// destination race benignly when their content is identical: rename is
/// last-write-wins.
pub(crate) fn write_atomic(root: &Path, dest: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = write_tmp(root, bytes)?;
    fs::rename(&tmp, dest).map_err(|e| io_error(dest, e))?;
    sync_parent_dir(dest)
}

/// Creates `dir` (and any missing ancestors) and fsyncs its parent, so
/// the new directory entry itself survives a crash — a file fsynced
/// inside a directory whose entry was never synced can vanish with the
/// directory. Idempotent over an existing directory.
pub(crate) fn create_dir_durable(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir).map_err(|e| io_error(dir, e))?;
    sync_parent_dir(dir)
}

/// Writes `bytes` to a fresh `tmp/<pid>-<seq>` file and fsyncs it,
/// returning the path ready to enter its destination.
fn write_tmp(root: &Path, bytes: &[u8]) -> Result<PathBuf> {
    let tmp = layout::tmp_file(
        root,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
    );
    let mut file = File::create(&tmp).map_err(|e| io_error(&tmp, e))?;
    file.write_all(bytes).map_err(|e| io_error(&tmp, e))?;
    file.sync_all().map_err(|e| io_error(&tmp, e))?;
    Ok(tmp)
}

/// Fsyncs the directory containing `path`, making a rename or directory
/// creation under it durable. Every destination lies inside the store
/// skeleton, so a parent always exists.
#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let dir = File::open(parent).map_err(|e| io_error(parent, e))?;
    dir.sync_all().map_err(|e| io_error(parent, e))
}

/// Directory fsync is unavailable off unix; renames and links still land
/// atomically, and durability of the directory entry is best-effort. The
/// project targets linux.
#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{create_dir_durable, write_atomic};
    use crate::testutil::temp_store;
    use sima_core::Result;
    use std::fs;

    #[test]
    fn create_dir_durable_creates_and_accepts_an_existing_dir() -> Result<()> {
        let (dir, _store) = temp_store();
        let created = dir.path().join("objects").join("aa");
        create_dir_durable(&created)?;
        assert!(created.is_dir());
        // Idempotent: creating an existing directory succeeds.
        create_dir_durable(&created)?;
        Ok(())
    }

    #[test]
    fn write_atomic_places_exact_content_and_leaves_tmp_empty() -> Result<()> {
        let (dir, store) = temp_store();
        let dest = dir.path().join("tasks").join("entry");
        write_atomic(store.root(), &dest, b"payload bytes\n")?;
        assert_eq!(
            fs::read(&dest).expect("read destination"),
            b"payload bytes\n"
        );
        // A completed write leaves nothing in flight.
        let leftovers = fs::read_dir(dir.path().join("tmp"))
            .expect("read tmp dir")
            .count();
        assert_eq!(leftovers, 0);
        Ok(())
    }

    #[test]
    fn concurrent_identical_writes_to_one_destination_both_succeed() -> Result<()> {
        let (dir, store) = temp_store();
        let dest = dir.path().join("tasks").join("entry");
        // Racing writers of identical content converge: rename is
        // last-write-wins over the same bytes.
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| write_atomic(store.root(), &dest, b"identical content")))
                .collect();
            for handle in handles {
                handle.join().expect("writer thread panicked")?;
            }
            Ok::<(), sima_core::Error>(())
        })?;
        assert_eq!(
            fs::read(&dest).expect("read destination"),
            b"identical content"
        );
        let leftovers = fs::read_dir(dir.path().join("tmp"))
            .expect("read tmp dir")
            .count();
        assert_eq!(leftovers, 0);
        Ok(())
    }
}
