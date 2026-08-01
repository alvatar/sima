//! Atomic-write mechanics shared by objects, index entries, and manifests.
//!
//! Every durable file starts as full content in a unique `tmp/<pid>-<seq>`
//! file, fsynced, then enters its destination in one of two ways, each
//! followed by an fsync of the destination's parent directory:
//!
//! - [`write_atomic`] renames into place, replacing any existing file. It
//!   serves content-addressed objects: one path means one content, so a
//!   racing writer carries identical bytes and the last writer wins
//!   harmlessly.
//! - [`write_exclusive`] enters through a hard link that fails when the
//!   destination already exists. It serves index entries and manifests,
//!   where two different results must never silently overwrite each other:
//!   a collision compares bytes, and byte-equal content is an idempotent
//!   `Ok` while different content fails loudly with `Corruption`.
//!
//! Directories are created through [`create_dir_durable`], which fsyncs
//! the parent so the new entry itself survives a crash. A reader —
//! including a process resuming after SIGKILL — therefore observes a
//! complete file or none. Leftover `tmp/` files after a crash are inert;
//! sweeping them is retention work.

use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sima_core::{Error, Result};

use crate::layout;

// Durable directory fsync (below) has no portable equivalent off unix.
// Refuse the build there rather than silently downgrade crash safety.
#[cfg(not(unix))]
compile_error!("sima-store supports unix targets only: durable directory fsync is unix-specific");

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
/// directory, replacing any existing file. This serves content-addressed
/// objects, where one path means one content: concurrent writers carry
/// identical bytes, so the last writer wins harmlessly.
pub(crate) fn write_atomic(root: &Path, dest: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = write_tmp(root, bytes)?;
    place_atomic(&tmp, dest)
}

/// A fresh in-flight write path, `tmp/<pid>-<seq>`, distinct from every
/// other this process hands out. Content written there is durable once
/// fsynced, and enters its destination through [`place_atomic`]. A writer
/// whose content is too large to hold in memory builds it here directly
/// rather than through [`write_atomic`].
pub(crate) fn tmp_path(root: &Path) -> PathBuf {
    layout::tmp_file(
        root,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
    )
}

/// Renames a completed, fsynced `tmp/` file into `dest` and fsyncs the
/// destination's parent, so the placement itself survives a crash.
pub(crate) fn place_atomic(tmp: &Path, dest: &Path) -> Result<()> {
    fs::rename(tmp, dest).map_err(|e| io_error(dest, e))?;
    sync_parent_dir(dest)
}

/// Writes `bytes` to `dest` for index entries and manifests, where two
/// different results must never silently overwrite each other. The fsynced
/// `tmp/` file enters through a hard link that fails when `dest` already
/// exists; on collision the existing content decides — byte-equal is `Ok`
/// (an idempotent re-placement), different content is [`Error::Corruption`]
/// and the existing file stays intact. Racing writers therefore converge on
/// identical bytes or fail loudly. The trailing `remove_file` drops the temp
/// name once the content is linked into place; it is cleanup, not part of
/// the atomicity.
pub(crate) fn write_exclusive(root: &Path, dest: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = write_tmp(root, bytes)?;
    match fs::hard_link(&tmp, dest) {
        Ok(()) => {
            fs::remove_file(&tmp).map_err(|e| io_error(&tmp, e))?;
            sync_parent_dir(dest)
        }
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {
            fs::remove_file(&tmp).map_err(|e| io_error(&tmp, e))?;
            let existing = fs::read(dest).map_err(|e| io_error(dest, e))?;
            if existing == bytes {
                Ok(())
            } else {
                Err(Error::Corruption(format!(
                    "refusing to replace {} with different content",
                    dest.display()
                )))
            }
        }
        Err(e) => Err(io_error(dest, e)),
    }
}

/// Removes `path`, treating an already-absent file as success, so a
/// resumed removal converges.
pub(crate) fn remove_file_idempotent(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_error(path, e)),
    }
}

/// Removes `path` idempotently and fsyncs its parent, so the removal
/// itself survives a crash.
pub(crate) fn remove_file_durable(path: &Path) -> Result<()> {
    remove_file_idempotent(path)?;
    sync_parent_dir(path)
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
    let tmp = tmp_path(root);
    let mut file = File::create(&tmp).map_err(|e| io_error(&tmp, e))?;
    // The payload is written in two parts around the crashpoint, so an
    // armed death lands after bytes have reached the temp file but before
    // the write completes: it leaves a torn temp file — inert by layout —
    // and never a torn final file, since nothing has entered the
    // destination yet.
    let split = bytes.len().min(1);
    file.write_all(&bytes[..split])
        .map_err(|e| io_error(&tmp, e))?;
    sima_core::crashpoint("object.mid-write");
    file.write_all(&bytes[split..])
        .map_err(|e| io_error(&tmp, e))?;
    file.sync_all().map_err(|e| io_error(&tmp, e))?;
    Ok(tmp)
}

/// Fsyncs the directory containing `path`, making a rename, link, or
/// directory creation under it durable. Every destination lies inside
/// the store skeleton, so a parent always exists.
fn sync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let dir = File::open(parent).map_err(|e| io_error(parent, e))?;
    dir.sync_all().map_err(|e| io_error(parent, e))
}

#[cfg(test)]
mod tests {
    use super::{create_dir_durable, write_atomic, write_exclusive};
    use crate::testutil::temp_store;
    use sima_core::{Error, Result};
    use std::fs;

    #[test]
    fn write_exclusive_places_exact_content_on_a_fresh_destination() -> Result<()> {
        let (dir, store) = temp_store();
        let dest = dir.path().join("tasks").join("entry");
        write_exclusive(store.root(), &dest, b"placed bytes\n")?;
        assert_eq!(
            fs::read(&dest).expect("read destination"),
            b"placed bytes\n"
        );
        let leftovers = fs::read_dir(dir.path().join("tmp"))
            .expect("read tmp dir")
            .count();
        assert_eq!(leftovers, 0);
        Ok(())
    }

    #[test]
    fn write_exclusive_over_equal_bytes_is_a_no_op() -> Result<()> {
        let (dir, store) = temp_store();
        let dest = dir.path().join("tasks").join("entry");
        write_exclusive(store.root(), &dest, b"same bytes")?;
        write_exclusive(store.root(), &dest, b"same bytes")?;
        assert_eq!(fs::read(&dest).expect("read destination"), b"same bytes");
        let leftovers = fs::read_dir(dir.path().join("tmp"))
            .expect("read tmp dir")
            .count();
        assert_eq!(leftovers, 0);
        Ok(())
    }

    #[test]
    fn write_exclusive_over_different_bytes_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let dest = dir.path().join("tasks").join("entry");
        write_exclusive(store.root(), &dest, b"original bytes")?;
        // The loser of a conflicting race fails loudly, and the placed
        // content stays intact.
        assert!(matches!(
            write_exclusive(store.root(), &dest, b"conflicting bytes"),
            Err(Error::Corruption(_))
        ));
        assert_eq!(
            fs::read(&dest).expect("read destination"),
            b"original bytes"
        );
        let leftovers = fs::read_dir(dir.path().join("tmp"))
            .expect("read tmp dir")
            .count();
        assert_eq!(leftovers, 0);
        Ok(())
    }

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
        // Racing writers of identical content converge: rename replaces
        // over the same bytes, so the last writer wins.
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

    #[test]
    fn concurrent_identical_write_exclusive_all_succeed() -> Result<()> {
        let (dir, store) = temp_store();
        let dest = dir.path().join("tasks").join("entry");
        let store = &store;
        let dest = &dest;
        // The first thread's hard link creates the entry; every other reads
        // the identical bytes back and returns an idempotent Ok.
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(move || write_exclusive(store.root(), dest, b"identical entry"))
                })
                .collect();
            for handle in handles {
                handle.join().expect("place thread panicked")?;
            }
            Ok::<(), Error>(())
        })?;
        assert_eq!(
            fs::read(dest).expect("read destination"),
            b"identical entry"
        );
        let leftovers = fs::read_dir(dir.path().join("tmp"))
            .expect("read tmp dir")
            .count();
        assert_eq!(leftovers, 0);
        Ok(())
    }

    #[test]
    fn concurrent_conflicting_write_exclusive_has_exactly_one_winner() -> Result<()> {
        let (dir, store) = temp_store();
        let dest = dir.path().join("tasks").join("entry");
        let store = &store;
        let dest = &dest;
        // Each thread offers distinct content: exactly one hard link lands,
        // and every other fails loudly instead of overwriting it.
        let outcomes: Vec<Result<()>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|i| {
                    scope.spawn(move || {
                        write_exclusive(store.root(), dest, format!("payload {i}").as_bytes())
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("place thread panicked"))
                .collect()
        });
        let wins = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
        assert_eq!(wins, 1);
        let conflicts = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(Error::Corruption(_))))
            .count();
        assert_eq!(conflicts, 7);
        // The surviving file is exactly one of the offered payloads, intact.
        let survivor = fs::read(dest).expect("read destination");
        let offered: Vec<Vec<u8>> = (0..8)
            .map(|i| format!("payload {i}").into_bytes())
            .collect();
        assert!(offered.contains(&survivor));
        let leftovers = fs::read_dir(dir.path().join("tmp"))
            .expect("read tmp dir")
            .count();
        assert_eq!(leftovers, 0);
        Ok(())
    }
}
