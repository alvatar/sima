//! Content-addressed object storage: `put`, `get`, `has`.
//!
//! Every identity-bearing byte string — specs, params, environments,
//! records, configs, state snapshots, artifacts — lands here as an object
//! addressed by its blake3 digest. Deletion is retention work, deferred
//! to P6; the CAS surface is exactly these three methods.

use std::fs;
use std::io::ErrorKind;

use sima_core::{Error, Hash, Result, hash_bytes};

use crate::atomic::{self, io_error};
use crate::layout;
use crate::store::Store;

impl Store {
    /// Stores `bytes` under their blake3 address and returns it.
    /// Idempotent: an existing object is left in place, unrewritten.
    /// Concurrent puts of the same bytes race benignly — rename is
    /// last-write-wins over identical content.
    pub fn put(&self, bytes: &[u8]) -> Result<Hash> {
        let hash = hash_bytes(bytes);
        if self.has(&hash)? {
            return Ok(hash);
        }
        let path = layout::object_path(self.root(), &hash);
        // The fan-out subdirectory is created durably on first use.
        if let Some(parent) = path.parent() {
            atomic::create_dir_durable(parent)?;
        }
        atomic::write_atomic(self.root(), &path, bytes)?;
        Ok(hash)
    }

    /// Reads the object at `hash`, verifying every read: the bytes are
    /// re-hashed, and a digest mismatch is [`Error::Corruption`]. An
    /// absent object is [`Error::MissingObject`].
    pub fn get(&self, hash: &Hash) -> Result<Vec<u8>> {
        let path = layout::object_path(self.root(), hash);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return Err(Error::MissingObject(*hash)),
            Err(e) => return Err(io_error(&path, e)),
        };
        let actual = hash_bytes(&bytes);
        if actual != *hash {
            return Err(Error::Corruption(format!(
                "object {hash} holds bytes hashing to {actual}"
            )));
        }
        Ok(bytes)
    }

    /// Reports whether the object at `hash` exists, without reading it.
    pub fn has(&self, hash: &Hash) -> Result<bool> {
        let path = layout::object_path(self.root(), hash);
        path.try_exists().map_err(|e| io_error(&path, e))
    }
}

#[cfg(test)]
mod tests {
    use crate::testutil::temp_store;
    use sima_core::{Error, Hash, Result, hash_bytes};
    use std::fs;
    use std::time::{Duration, SystemTime};

    /// blake3 of `b"sima cas object"`, computed independently with Python
    /// blake3 (pip package `blake3`):
    /// `blake3.blake3(b"sima cas object").hexdigest()`.
    const PINNED_OBJECT_HEX: &str =
        "6c118a68de72dfce9ca7a3939f085a58c730b98dbb23cbdc58869477b8384806";

    /// Official blake3 test vector for empty input (test_vectors.json,
    /// input_len 0, first 32 bytes).
    const EMPTY_OBJECT_HEX: &str =
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

    #[test]
    fn put_returns_the_independently_derived_hash_at_the_pinned_path() -> Result<()> {
        let (dir, store) = temp_store();
        let hash = store.put(b"sima cas object")?;
        assert_eq!(hash, Hash::from_hex(PINNED_OBJECT_HEX)?);
        // The fan-out path is part of the fixed layout contract: the first
        // two hex characters name the subdirectory.
        let expected = dir
            .path()
            .join("objects")
            .join(&PINNED_OBJECT_HEX[..2])
            .join(PINNED_OBJECT_HEX);
        assert_eq!(fs::read(expected).expect("read object"), b"sima cas object");
        Ok(())
    }

    #[test]
    fn put_is_idempotent_and_leaves_the_existing_file_in_place() -> Result<()> {
        let (dir, store) = temp_store();
        let hash = store.put(b"sima cas object")?;
        let path = dir
            .path()
            .join("objects")
            .join(&PINNED_OBJECT_HEX[..2])
            .join(PINNED_OBJECT_HEX);
        // Stamp the object into the past; an idempotent re-put must not
        // rewrite the file, so the stamp survives.
        let past = SystemTime::now() - Duration::from_secs(3600);
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open object for stamping");
        file.set_modified(past).expect("stamp mtime");
        drop(file);
        let again = store.put(b"sima cas object")?;
        assert_eq!(again, hash);
        let mtime = fs::metadata(&path)
            .expect("stat object")
            .modified()
            .expect("read mtime");
        assert_eq!(mtime, past);
        Ok(())
    }

    #[test]
    fn get_round_trips_put_bytes() -> Result<()> {
        let (_dir, store) = temp_store();
        let hash = store.put(b"round trip payload")?;
        assert_eq!(store.get(&hash)?, b"round trip payload");
        Ok(())
    }

    #[test]
    fn get_of_an_absent_hash_is_missing_object() -> Result<()> {
        let (_dir, store) = temp_store();
        let absent = hash_bytes(b"never stored");
        match store.get(&absent) {
            Err(Error::MissingObject(h)) => assert_eq!(h, absent),
            other => panic!("expected MissingObject, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn get_of_a_tampered_object_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let hash = store.put(b"sima cas object")?;
        let path = dir
            .path()
            .join("objects")
            .join(&PINNED_OBJECT_HEX[..2])
            .join(PINNED_OBJECT_HEX);
        fs::write(&path, b"tampered bytes").expect("tamper object");
        assert!(matches!(store.get(&hash), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn has_reflects_existence() -> Result<()> {
        let (_dir, store) = temp_store();
        let absent = hash_bytes(b"never stored");
        assert!(!store.has(&absent)?);
        let hash = store.put(b"present")?;
        assert!(store.has(&hash)?);
        Ok(())
    }

    #[test]
    fn empty_bytes_are_a_legal_object() -> Result<()> {
        let (_dir, store) = temp_store();
        let hash = store.put(b"")?;
        assert_eq!(hash, Hash::from_hex(EMPTY_OBJECT_HEX)?);
        assert!(store.has(&hash)?);
        assert_eq!(store.get(&hash)?, Vec::<u8>::new());
        Ok(())
    }

    /// Counts the object files under `objects/`, across fan-out
    /// subdirectories.
    fn object_count(root: &std::path::Path) -> usize {
        fs::read_dir(root.join("objects"))
            .expect("read objects dir")
            .map(|entry| {
                fs::read_dir(entry.expect("fan-out dir").path())
                    .expect("read fan-out dir")
                    .count()
            })
            .sum()
    }

    #[test]
    fn concurrent_puts_of_identical_bytes_yield_one_object() -> Result<()> {
        let (dir, store) = temp_store();
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| store.put(b"raced bytes")))
                .collect();
            for handle in handles {
                handle.join().expect("writer thread panicked")?;
            }
            Ok::<(), Error>(())
        })?;
        assert_eq!(object_count(dir.path()), 1);
        Ok(())
    }

    #[test]
    fn concurrent_puts_of_distinct_bytes_yield_distinct_objects() -> Result<()> {
        let (dir, store) = temp_store();
        let store = &store;
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|i: u8| scope.spawn(move || store.put(&[i])))
                .collect();
            for handle in handles {
                handle.join().expect("writer thread panicked")?;
            }
            Ok::<(), Error>(())
        })?;
        assert_eq!(object_count(dir.path()), 8);
        Ok(())
    }
}
