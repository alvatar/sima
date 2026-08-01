//! The store's advisory file locks: [`RunLock`], the per-run orchestrator
//! lock, and [`take_file_lock`], the primitive every store lock is built
//! from — the maintenance lock over `packs/` included.
//!
//! One orchestrator drives a run at a time. The lock is the OS's advisory
//! file lock on the run's `orchestrator.lock` file, so the kernel releases
//! it the instant the holder exits — cleanly, by SIGKILL, or by power loss
//! on the machine's next boot — and no staleness protocol exists. The
//! file's content (pid and hostname) is diagnostic only: it names the
//! holder in error messages and is never consulted for liveness.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Write};
use std::path::Path;

use sima_core::{Error, Result};
use sima_model::RunId;

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

/// Exclusive right to drive a run: holds the OS lock on the run's
/// `orchestrator.lock` file. The kernel releases it when the holder
/// exits, however it exits. Unlocks on drop.
///
/// The lock names the run it was taken for, so a reference to it is a
/// capability: holding one proves that run's lock is held, which is what
/// every liveness probe against that run reads.
pub struct RunLock {
    /// The run this lock covers.
    run: RunId,
    /// The locked file, unlocked on drop.
    file: File,
}

impl RunLock {
    /// The run this lock covers.
    pub fn run(&self) -> &RunId {
        &self.run
    }
}

impl Drop for RunLock {
    /// Frees the lock the moment the guard goes, by unlocking it.
    ///
    /// The OS lock lives on the open file description, and closing frees it
    /// only once the last descriptor onto that description is gone. Spawning
    /// a process copies the whole descriptor table into the child, where
    /// close-on-exec clears the copies at exec, so a worker spawned while
    /// this lock is held shares its description for as long as the fork takes
    /// to reach exec. Unlocking names the lock itself, so the release is
    /// immediate and a run resumed in that window finds the lock free.
    fn drop(&mut self) {
        // Nothing actionable remains if the release fails: the descriptor
        // closes next, and process exit releases the lock regardless.
        let _ = self.file.unlock();
    }
}

impl Store {
    /// Takes the run's orchestrator lock, creating the run directory if
    /// missing. A lock already held is [`Error::Validation`] naming the
    /// holder recorded in the file (pid, hostname).
    pub fn acquire_run_lock(&self, run: &RunId) -> Result<RunLock> {
        atomic::create_dir_durable(&layout::run_dir(self.root(), run))?;
        let file = take_file_lock(&layout::lock_path(self.root(), run), |holder| {
            // Contended: name the holder the lock owner recorded.
            Error::Validation(format!(
                "run {run} is already locked by another orchestrator: {holder}"
            ))
        })?;
        Ok(RunLock { run: *run, file })
    }

    /// Reports who holds `run`'s orchestrator lock: `Some` with the holder
    /// line the locker recorded (pid, hostname) while another process holds
    /// it, `None` while it is free. A missing run directory or lock file is
    /// a free lock. The probe never creates a file or directory — the lock
    /// file is opened without create — and a lock the probe itself acquires
    /// (proving it free) is released immediately when the handle drops.
    pub fn lock_holder(&self, run: &RunId) -> Result<Option<String>> {
        let path = layout::lock_path(self.root(), run);
        let mut file = match OpenOptions::new().read(true).open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error(&path, e)),
        };
        match file.try_lock() {
            Ok(()) => {
                // Free, which taking it just proved. Release it explicitly,
                // for the reason [`RunLock`]'s drop does: closing would leave
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

    /// A fresh store and a run id to lock; the run's directory does not
    /// exist yet, so acquisition also covers its creation.
    fn store_and_run() -> (tempfile::TempDir, Store, RunId) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open store");
        let run = RunId::from_hash(hash_bytes(b"a run to lock"));
        (dir, store, run)
    }

    #[test]
    fn acquire_records_the_holder_in_the_lock_file() -> Result<()> {
        let (dir, store, run) = store_and_run();
        let _lock = store.acquire_run_lock(&run)?;
        let content = fs::read_to_string(
            dir.path()
                .join("runs")
                .join(run.to_string())
                .join("orchestrator.lock"),
        )
        .expect("read lock file");
        // The content is diagnostic: this process's pid, then a hostname.
        let pid = std::process::id().to_string();
        assert_eq!(content.split_whitespace().next(), Some(pid.as_str()));
        Ok(())
    }

    #[test]
    fn a_lock_names_the_run_it_covers() -> Result<()> {
        let (_dir, store, run) = store_and_run();
        let lock = store.acquire_run_lock(&run)?;
        // A reference to the lock stands for that run's liveness, so the
        // run it covers is readable from it.
        assert_eq!(lock.run(), &run);
        Ok(())
    }

    #[test]
    fn a_held_lock_is_validation_naming_the_holder() -> Result<()> {
        let (_dir, store, run) = store_and_run();
        let _lock = store.acquire_run_lock(&run)?;
        match store.acquire_run_lock(&run) {
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
        let (_dir, store, run) = store_and_run();
        drop(store.acquire_run_lock(&run)?);
        // Released on drop: the second acquisition succeeds.
        store.acquire_run_lock(&run)?;
        Ok(())
    }

    #[test]
    fn dropping_the_lock_releases_it_while_a_copy_of_its_descriptor_lives() -> Result<()> {
        let (_dir, store, run) = store_and_run();
        let lock = store.acquire_run_lock(&run)?;
        // What spawning a worker does to this descriptor: the fork hands the
        // child a copy of every one of the parent's, and close-on-exec closes
        // them at exec rather than at fork, so any concurrent spawn shares
        // this lock's open file description for that window. `try_clone` is
        // that copy, deterministically. Releasing must act on the lock
        // itself, so it cannot wait on an unrelated child reaching exec.
        let copy = lock.file.try_clone().expect("copy the lock's descriptor");
        drop(lock);
        store.acquire_run_lock(&run)?;
        drop(copy);
        Ok(())
    }

    #[test]
    fn lock_holder_names_the_holder_while_held_and_clears_on_release() -> Result<()> {
        let (_dir, store, run) = store_and_run();
        let lock = store.acquire_run_lock(&run)?;
        let holder = store.lock_holder(&run)?.expect("a holder while locked");
        // The probe returns the recorded diagnostic line: pid, then hostname.
        let pid = std::process::id().to_string();
        assert_eq!(holder.split_whitespace().next(), Some(pid.as_str()));
        drop(lock);
        assert_eq!(store.lock_holder(&run)?, None);
        Ok(())
    }

    #[test]
    fn lock_holder_probes_a_missing_run_without_creating_anything() -> Result<()> {
        let (dir, store, run) = store_and_run();
        assert_eq!(store.lock_holder(&run)?, None);
        // The probe is read-only: no run directory appeared.
        assert!(!dir.path().join("runs").join(run.to_string()).exists());
        Ok(())
    }

    #[test]
    fn lock_holder_on_a_run_without_a_lock_file_creates_none() -> Result<()> {
        let (dir, store, run) = store_and_run();
        let run_dir = dir.path().join("runs").join(run.to_string());
        fs::create_dir_all(&run_dir).expect("create run dir");
        assert_eq!(store.lock_holder(&run)?, None);
        // The probe opened without create: still no lock file.
        assert!(!run_dir.join("orchestrator.lock").exists());
        Ok(())
    }

    #[test]
    fn lock_holder_is_none_on_a_released_lock_file() -> Result<()> {
        let (dir, store, run) = store_and_run();
        // A lock file left by an exited holder: the OS released the lock
        // with the process, so the stale content names nobody alive.
        let run_dir = dir.path().join("runs").join(run.to_string());
        fs::create_dir_all(&run_dir).expect("create run dir");
        fs::write(run_dir.join("orchestrator.lock"), b"999999 elsewhere\n")
            .expect("pre-create lock file");
        assert_eq!(store.lock_holder(&run)?, None);
        Ok(())
    }

    #[test]
    fn a_leftover_lock_file_without_a_holder_acquires_fine() -> Result<()> {
        let (dir, store, run) = store_and_run();
        // A plain file left by a dead holder: the OS released its lock
        // with the process, so the content is stale and irrelevant.
        let run_dir = dir.path().join("runs").join(run.to_string());
        fs::create_dir_all(&run_dir).expect("create run dir");
        fs::write(run_dir.join("orchestrator.lock"), b"999999 elsewhere\n")
            .expect("pre-create lock file");
        let _lock = store.acquire_run_lock(&run)?;
        Ok(())
    }
}
