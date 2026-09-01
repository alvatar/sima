//! The store's advisory file locks: [`SearchLock`], the per-search orchestrator
//! lock, and [`take_file_lock`], the primitive every store lock is built
//! from — the maintenance lock over `packs/` included.
//!
//! One orchestrator drives a search at a time. The lock is the OS's advisory
//! file lock on the search's `orchestrator.lock` file, so the kernel releases
//! it the instant the holder exits — cleanly, by SIGKILL, or by power loss
//! on the machine's next boot — and no staleness protocol exists. The
//! file's content (pid and hostname) is diagnostic only: it names the
//! holder in error messages and is never consulted for liveness.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Write};
use std::path::Path;

use sima_core::{Error, Result};
use sima_model::SearchId;

use crate::atomic::{self, io_error};
use crate::layout;
use crate::store::Store;

/// Takes the OS lock on the file at `path`, creating it when absent, and
/// records this process as the holder for diagnostics. A lock already held
/// is the error `contended` builds from the holder line the owner recorded.
///
/// The returned file owns the lock: it is held until the file is unlocked
/// or the holder exits, however it exits.
pub(crate) fn take_file_lock(path: &Path, contended: impl Fn(&str) -> Error) -> Result<File> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|e| io_error(path, e))?;
    match file.try_lock() {
        Ok(()) => {
            // Locked. Record the holder for diagnostics: a hostname from
            // the environment is enough for a string nothing consults for
            // liveness. Any previous holder's content is overwritten.
            let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
            let holder = format!("{} {hostname}\n", std::process::id());
            file.set_len(0).map_err(|e| io_error(path, e))?;
            file.write_all(holder.as_bytes())
                .map_err(|e| io_error(path, e))?;
            Ok(file)
        }
        Err(TryLockError::WouldBlock) => Err(contended(&recorded_holder(&mut file))),
        Err(TryLockError::Error(e)) => Err(io_error(path, e)),
    }
}

/// Exclusive right to drive a search: holds the OS lock on the search's
/// `orchestrator.lock` file. The kernel releases it when the holder
/// exits, however it exits. Unlocks on drop.
///
/// The lock names the search it was taken for, so a reference to it is a
/// capability: holding one proves that search's lock is held, which is what
/// every liveness probe against that search reads.
pub struct SearchLock {
    /// The search this lock covers.
    search: SearchId,
    /// The locked file, unlocked on drop.
    file: File,
}

impl SearchLock {
    /// The search this lock covers.
    pub fn search(&self) -> &SearchId {
        &self.search
    }
}

impl Drop for SearchLock {
    /// Frees the lock the moment the guard goes, by unlocking it.
    ///
    /// The OS lock lives on the open file description, and closing frees it
    /// only once the last descriptor onto that description is gone. Spawning
    /// a process copies the whole descriptor table into the child, where
    /// close-on-exec clears the copies at exec, so a worker spawned while
    /// this lock is held shares its description for as long as the fork takes
    /// to reach exec. Unlocking names the lock itself, so the release is
    /// immediate and a search resumed in that window finds the lock free.
    fn drop(&mut self) {
        // Nothing actionable remains if the release fails: the descriptor
        // closes next, and process exit releases the lock regardless.
        let _ = self.file.unlock();
    }
}

impl Store {
    /// Takes the search's orchestrator lock, creating the search directory if
    /// missing. A lock already held is [`Error::Validation`] naming the
    /// holder recorded in the file (pid, hostname).
    pub fn acquire_search_lock(&self, search: &SearchId) -> Result<SearchLock> {
        atomic::create_dir_durable(&layout::search_dir(self.root(), search))?;
        let file = take_file_lock(&layout::lock_path(self.root(), search), |holder| {
            // Contended: name the holder the lock owner recorded.
            Error::Validation(format!(
                "search {search} is already locked by another orchestrator: {holder}"
            ))
        })?;
        Ok(SearchLock {
            search: *search,
            file,
        })
    }

    /// Reports who holds `search`'s orchestrator lock: `Some` with the holder
    /// line the locker recorded (pid, hostname) while another process holds
    /// it, `None` while it is free. A missing search directory or lock file is
    /// a free lock. The probe never creates a file or directory — the lock
    /// file is opened without create — and a lock the probe itself acquires
    /// (proving it free) is released immediately when the handle drops.
    pub fn lock_holder(&self, search: &SearchId) -> Result<Option<String>> {
        let path = layout::lock_path(self.root(), search);
        let mut file = match OpenOptions::new().read(true).open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error(&path, e)),
        };
        match file.try_lock() {
            Ok(()) => {
                // Free, which taking it just proved. Release it explicitly,
                // for the reason [`SearchLock`]'s drop does: closing would leave
                // the probe's own lock standing for as long as a concurrent
                // spawn's copy of this description lives, so a probe could
                // lock out the orchestrator it was only asking about.
                let _ = file.unlock();
                Ok(None)
            }
            Err(TryLockError::WouldBlock) => Ok(Some(recorded_holder(&mut file))),
            Err(TryLockError::Error(e)) => Err(io_error(&path, e)),
        }
    }
}

/// The holder line recorded in an open lock file, for diagnostics:
/// `unknown` when the content is blank or unreadable.
fn recorded_holder(file: &mut File) -> String {
    let mut holder = String::new();
    let _ = file.read_to_string(&mut holder);
    let holder = holder.trim();
    if holder.is_empty() {
        "unknown".to_string()
    } else {
        holder.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sima_core::hash_bytes;

    use super::*;

    /// A fresh store and a search id to lock; the search's directory does not
    /// exist yet, so acquisition also covers its creation.
    fn store_and_search() -> (tempfile::TempDir, Store, SearchId) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open store");
        let search = SearchId::from_hash(hash_bytes(b"a search to lock"));
        (dir, store, search)
    }

    #[test]
    fn acquire_records_the_holder_in_the_lock_file() -> Result<()> {
        let (dir, store, search) = store_and_search();
        let _lock = store.acquire_search_lock(&search)?;
        let content = fs::read_to_string(
            dir.path()
                .join("searches")
                .join(search.to_string())
                .join("orchestrator.lock"),
        )
        .expect("read lock file");
        // The content is diagnostic: this process's pid, then a hostname.
        let pid = std::process::id().to_string();
        assert_eq!(content.split_whitespace().next(), Some(pid.as_str()));
        Ok(())
    }

    #[test]
    fn a_lock_names_the_search_it_covers() -> Result<()> {
        let (_dir, store, search) = store_and_search();
        let lock = store.acquire_search_lock(&search)?;
        // A reference to the lock stands for that search's liveness, so the
        // search it covers is readable from it.
        assert_eq!(lock.search(), &search);
        Ok(())
    }

    #[test]
    fn a_held_lock_is_validation_naming_the_holder() -> Result<()> {
        let (_dir, store, search) = store_and_search();
        let _lock = store.acquire_search_lock(&search)?;
        match store.acquire_search_lock(&search) {
            Err(Error::Validation(msg)) => {
                let pid = std::process::id().to_string();
                assert!(msg.contains(&pid), "the error names the holder: {msg}");
            }
            Err(other) => panic!("expected Validation, got {other}"),
            Ok(_) => panic!("a held lock must not be acquired again"),
        }
        Ok(())
    }

    #[test]
    fn dropping_the_lock_releases_it() -> Result<()> {
        let (_dir, store, search) = store_and_search();
        drop(store.acquire_search_lock(&search)?);
        // Released on drop: the second acquisition succeeds.
        store.acquire_search_lock(&search)?;
        Ok(())
    }

    #[test]
    fn dropping_the_lock_releases_it_while_a_copy_of_its_descriptor_lives() -> Result<()> {
        let (_dir, store, search) = store_and_search();
        let lock = store.acquire_search_lock(&search)?;
        // What spawning a worker does to this descriptor: the fork hands the
        // child a copy of every one of the parent's, and close-on-exec closes
        // them at exec rather than at fork, so any concurrent spawn shares
        // this lock's open file description for that window. `try_clone` is
        // that copy, deterministically. Releasing must act on the lock
        // itself, so it cannot wait on an unrelated child reaching exec.
        let copy = lock.file.try_clone().expect("copy the lock's descriptor");
        drop(lock);
        store.acquire_search_lock(&search)?;
        drop(copy);
        Ok(())
    }

    #[test]
    fn lock_holder_names_the_holder_while_held_and_clears_on_release() -> Result<()> {
        let (_dir, store, search) = store_and_search();
        let lock = store.acquire_search_lock(&search)?;
        let holder = store.lock_holder(&search)?.expect("a holder while locked");
        // The probe returns the recorded diagnostic line: pid, then hostname.
        let pid = std::process::id().to_string();
        assert_eq!(holder.split_whitespace().next(), Some(pid.as_str()));
        drop(lock);
        assert_eq!(store.lock_holder(&search)?, None);
        Ok(())
    }

    #[test]
    fn lock_holder_probes_a_missing_search_without_creating_anything() -> Result<()> {
        let (dir, store, search) = store_and_search();
        assert_eq!(store.lock_holder(&search)?, None);
        // The probe is read-only: no search directory appeared.
        assert!(
            !dir.path()
                .join("searches")
                .join(search.to_string())
                .exists()
        );
        Ok(())
    }

    #[test]
    fn lock_holder_on_a_search_without_a_lock_file_creates_none() -> Result<()> {
        let (dir, store, search) = store_and_search();
        let search_dir = dir.path().join("searches").join(search.to_string());
        fs::create_dir_all(&search_dir).expect("create search dir");
        assert_eq!(store.lock_holder(&search)?, None);
        // The probe opened without create: still no lock file.
        assert!(!search_dir.join("orchestrator.lock").exists());
        Ok(())
    }

    #[test]
    fn lock_holder_is_none_on_a_released_lock_file() -> Result<()> {
        let (dir, store, search) = store_and_search();
        // A lock file left by an exited holder: the OS released the lock
        // with the process, so the stale content names nobody alive.
        let search_dir = dir.path().join("searches").join(search.to_string());
        fs::create_dir_all(&search_dir).expect("create search dir");
        fs::write(search_dir.join("orchestrator.lock"), b"999999 elsewhere\n")
            .expect("pre-create lock file");
        assert_eq!(store.lock_holder(&search)?, None);
        Ok(())
    }

    #[test]
    fn a_leftover_lock_file_without_a_holder_acquires_fine() -> Result<()> {
        let (dir, store, search) = store_and_search();
        // A plain file left by a dead holder: the OS released its lock
        // with the process, so the content is stale and irrelevant.
        let search_dir = dir.path().join("searches").join(search.to_string());
        fs::create_dir_all(&search_dir).expect("create search dir");
        fs::write(search_dir.join("orchestrator.lock"), b"999999 elsewhere\n")
            .expect("pre-create lock file");
        let _lock = store.acquire_search_lock(&search)?;
        Ok(())
    }
}
