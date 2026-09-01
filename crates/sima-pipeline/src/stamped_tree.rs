//! A directory this machine builds from something content-addressed, and the
//! stamp that says which content it was built from.
//!
//! Two trees are built this way: the program a `[domain.*]` entry's payload
//! installs into, and the SDK the binary vends beside it. Both answer the same
//! question at every load — is what is on disk already the thing this digest
//! names — and both have to answer it cheaply, because a status query, a
//! follow attach, and a reattaching migration all load a config and build
//! nothing.
//!
//! ```text
//!        build of digest D under root
//!                    │
//!                    ▼
//!        installed.digest == D and ──yes──► nothing to do
//!        the tree is complete?
//!                    │ no
//!                    ▼
//!           take the root's lock  ◄── concurrent loaders wait here; the
//!                    │                kernel frees a crashed builder's lock
//!                    ▼
//!           re-check the stamp ──hit──► release, nothing to do
//!                    │ miss
//!                    ▼
//!        remove the stamp, build, write the stamp last
//! ```
//!
//! The ordering is the whole contract: the stamp is removed first, so a build
//! that dies part-way leaves a tree nothing claims and the next loader rebuilds
//! it; it is written last and atomically, so a stamp is only ever read beside
//! the tree it names. The lock is blocking and lives on the open file
//! description, so a crashed builder's is released by the kernel and no
//! staleness protocol exists — the rule the store's search lock follows.
//!
//! What "complete" means is the caller's: a program tree is complete when its
//! entry point is executable, and the SDK's when the package is importable. The
//! stamp alone would claim a tree someone deleted the contents of.

use std::fs::OpenOptions;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use sima_core::{Error, Hash, Result};

/// The digest the tree was built from, written last.
pub(crate) const STAMP_FILE: &str = "installed.digest";
/// Held while building, so concurrent loaders build one tree between them.
const LOCK_FILE: &str = ".lock";

/// The mode a file that runs is written under.
pub(crate) const EXECUTABLE_MODE: u32 = 0o755;
/// The mode every other written file gets.
pub(crate) const REGULAR_MODE: u32 = 0o644;

/// Builds the tree at `root` for `digest`, unless it is already there.
///
/// `complete` answers whether the tree the stamp claims is actually on disk;
/// `build` fills `root` and is called with the tree's lock held and the stamp
/// removed. See the module documentation for the ordering both rest on.
pub(crate) fn build_once(
    root: &Path,
    digest: &Hash,
    complete: &dyn Fn() -> Result<bool>,
    build: &dyn Fn() -> Result<()>,
) -> Result<()> {
    // The cost the stamp exists to save, and nothing more: the decision is
    // taken again under the lock, which is where it is actually made. What this
    // buys is that a loader with nothing to do reads one file instead of
    // queueing behind a build running for someone else.
    if built(root, digest, complete)? {
        return Ok(());
    }
    create_dir(root)?;
    let path = root.join(LOCK_FILE);
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
    lock.lock().map_err(|source| Error::Io {
        path: path.clone(),
        source,
    })?;
    let outcome = under_lock(root, digest, complete, build);
    // Named rather than left to scope end: the release is what the next loader
    // is waiting on, and it happens whether the build succeeded or not.
    let _ = lock.unlock();
    outcome
}

/// The decision and the build, with the lock held.
fn under_lock(
    root: &Path,
    digest: &Hash,
    complete: &dyn Fn() -> Result<bool>,
    build: &dyn Fn() -> Result<()>,
) -> Result<()> {
    // Another loader may have built it while this one waited for the lock.
    if built(root, digest, complete)? {
        return Ok(());
    }
    // The stamp goes first, so a build that dies part-way leaves a tree nothing
    // claims.
    remove_file(&root.join(STAMP_FILE))?;
    build()?;
    // Last, and atomically, so a stamp is only ever read beside the tree it
    // names: a reader sees the whole file or none.
    let stamp = root.join(STAMP_FILE);
    let pending = root.join(format!("{STAMP_FILE}.pending"));
    write_file(&pending, digest.to_string().as_bytes(), REGULAR_MODE)?;
    std::fs::rename(&pending, &stamp).map_err(|source| Error::Io {
        path: stamp,
        source,
    })
}

/// Whether `root` already holds what `digest` names: the stamp says so and the
/// tree it claims is complete.
fn built(root: &Path, digest: &Hash, complete: &dyn Fn() -> Result<bool>) -> Result<bool> {
    let stamp = root.join(STAMP_FILE);
    let recorded = match std::fs::read_to_string(&stamp) {
        Ok(recorded) => recorded,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(Error::Io {
                path: stamp,
                source,
            });
        }
    };
    Ok(recorded.trim() == digest.to_string() && complete()?)
}

/// Refuses a path that would not mean the same thing on the machine the tree
/// is built on.
///
/// Both manifests a tree is built from — a payload's and the SDK's — name
/// their files this way, and both are read off a wire, so the rule lives here
/// with the writing it guards rather than in either one.
pub(crate) fn validate_path(path: &str) -> Result<()> {
    let refuse = |why: &str| {
        Err(Error::Validation(format!(
            "payload path {path:?} {why}; a manifest names relative, \
             `/`-separated paths inside the payload"
        )))
    };
    if path.is_empty() {
        return refuse("is empty");
    }
    if path.starts_with('/') {
        return refuse("is absolute");
    }
    if path.contains('\\') {
        return refuse("holds a backslash");
    }
    for component in path.split('/') {
        match component {
            "" => return refuse("holds an empty component"),
            "." | ".." => return refuse("holds a `.` or `..` component"),
            _ => {}
        }
    }
    Ok(())
}

/// Whether `path` is a file this machine can run.
pub(crate) fn executable(path: &Path) -> Result<bool> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.is_file() && metadata.permissions().mode() & 0o111 != 0),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(Error::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Removes a directory and everything under it; one that is not there is
/// already removed.
pub(crate) fn remove_dir(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Removes a file; one that is not there is already removed.
pub(crate) fn remove_file(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Reads a file, naming it on failure.
pub(crate) fn read_file(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Writes `bytes` at `path` under `mode`, naming the path on failure.
pub(crate) fn write_file(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let io = |source| Error::Io {
        path: path.to_path_buf(),
        source,
    };
    std::fs::write(path, bytes).map_err(io)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(io)
}

/// Creates a directory and its parents, naming it on failure.
pub(crate) fn create_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sima_core::hash_bytes;

    use super::*;

    /// A root under a fresh temporary directory, and the digest the tests build
    /// it for.
    fn root() -> (tempfile::TempDir, PathBuf, Hash) {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("tree");
        (dir, root, hash_bytes(b"one build"))
    }

    /// A build that writes one file into `root` and counts its own calls.
    fn counting<'a>(root: &'a Path, calls: &'a AtomicUsize) -> impl Fn() -> Result<()> + 'a {
        move || {
            calls.fetch_add(1, Ordering::Relaxed);
            create_dir(root)?;
            write_file(&root.join("built"), b"contents", REGULAR_MODE)
        }
    }

    /// Whether the file a [`counting`] build writes is there.
    fn present(root: &Path) -> impl Fn() -> Result<bool> + '_ {
        move || Ok(root.join("built").is_file())
    }

    #[test]
    fn a_tree_is_built_once_and_the_stamp_answers_every_later_build() -> Result<()> {
        let (_dir, root, digest) = root();
        let calls = AtomicUsize::new(0);
        build_once(&root, &digest, &present(&root), &counting(&root, &calls))?;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            std::fs::read_to_string(root.join(STAMP_FILE)).expect("the stamp"),
            digest.to_string()
        );

        build_once(&root, &digest, &present(&root), &counting(&root, &calls))?;
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "the stamp is what the second load reads"
        );
        Ok(())
    }

    #[test]
    fn a_different_digest_rebuilds_exactly_once() -> Result<()> {
        let (_dir, root, digest) = root();
        let calls = AtomicUsize::new(0);
        build_once(&root, &digest, &present(&root), &counting(&root, &calls))?;

        let changed = hash_bytes(b"another build");
        build_once(&root, &changed, &present(&root), &counting(&root, &calls))?;
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(
            std::fs::read_to_string(root.join(STAMP_FILE)).expect("the stamp"),
            changed.to_string(),
            "the stamp names the build that is there"
        );

        build_once(&root, &changed, &present(&root), &counting(&root, &calls))?;
        assert_eq!(calls.load(Ordering::Relaxed), 2, "and not a third time");
        Ok(())
    }

    #[test]
    fn a_stamped_tree_whose_contents_went_missing_is_rebuilt() -> Result<()> {
        // The stamp alone would claim a tree someone emptied, so what the
        // caller calls complete is checked beside it.
        let (_dir, root, digest) = root();
        let calls = AtomicUsize::new(0);
        build_once(&root, &digest, &present(&root), &counting(&root, &calls))?;
        std::fs::remove_file(root.join("built")).expect("empty the tree");

        build_once(&root, &digest, &present(&root), &counting(&root, &calls))?;
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert!(root.join("built").is_file());
        Ok(())
    }

    #[test]
    fn a_build_that_fails_leaves_no_stamp_and_the_next_one_tries_again() -> Result<()> {
        // The ordering the module rests on: the stamp is removed before the
        // build, so a tree half a failed build left behind claims nothing.
        let (_dir, root, digest) = root();
        let calls = AtomicUsize::new(0);
        build_once(&root, &digest, &present(&root), &counting(&root, &calls))?;

        let changed = hash_bytes(b"a build that fails");
        let failing = || -> Result<()> { Err(Error::Validation("the build failed".to_string())) };
        assert!(build_once(&root, &changed, &present(&root), &failing).is_err());
        assert!(
            !root.join(STAMP_FILE).exists(),
            "a tree nothing claims is what a failed build leaves"
        );

        build_once(&root, &changed, &present(&root), &counting(&root, &calls))?;
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[test]
    fn concurrent_builders_build_one_tree_between_them() -> Result<()> {
        // The lock, exercised: every thread loads the same root at once, and
        // exactly one of them builds.
        let (_dir, root, digest) = root();
        let calls = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    build_once(&root, &digest, &present(&root), &counting(&root, &calls))
                        .expect("build the tree");
                });
            }
        });
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        Ok(())
    }
}
