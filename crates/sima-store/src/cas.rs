//! Content-addressed object storage: `put`, `get`, `has`.
//!
//! Every identity-bearing byte string — specs, params, environments,
//! records, configs, state snapshots, artifacts — lands here as an object
//! addressed by its blake3 digest. Deletion is retention work; the CAS
//! surface is exactly these three methods.
//!
//! An object lives loose under `objects/` or inside a pack under `packs/`,
//! and which one is a fact about the store's shape, never about the object:
//! reads look loose first, then in the packs, and both paths verify the
//! bytes against the address before returning them. Writes always land
//! loose — consolidating them into packs is maintenance work.

use std::fs;
use std::io::ErrorKind;

use sima_core::{Error, Hash, Result, hash_bytes};

use crate::atomic::{self, io_error};
use crate::layout;
use crate::pack::format::{self, PackEntry};
use crate::store::Store;

/// Fan-out subdirectory the loose-object estimate samples. blake3 spreads
/// addresses uniformly, so any one of the 256 stands for all of them.
const ESTIMATE_SAMPLE: &str = "00";

impl Store {
    /// Stores `bytes` under their blake3 address and returns it.
    /// Idempotent: an existing object is left in place, unrewritten.
    /// Concurrent puts of the same bytes race benignly — rename replaces
    /// over identical content, so the last writer wins harmlessly.
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
    ///
    /// A loose file answers first; failing that, the packs do.
    pub fn get(&self, hash: &Hash) -> Result<Vec<u8>> {
        let path = layout::object_path(self.root(), hash);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return self.get_packed(hash),
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

    /// Reports whether the object at `hash` exists, without reading it,
    /// in either representation.
    pub fn has(&self, hash: &Hash) -> Result<bool> {
        let path = layout::object_path(self.root(), hash);
        if path.try_exists().map_err(|e| io_error(&path, e))? {
            return Ok(true);
        }
        Ok(self.locate_packed(hash)?.is_some())
    }

    /// An estimate of how many loose objects the store holds, from one
    /// directory read: the addresses spread uniformly over the 256 fan-out
    /// subdirectories, so one subdirectory's count times 256 is the whole.
    /// A store without that subdirectory estimates zero, which is coarse
    /// exactly where the count is too small for the answer to matter.
    pub fn loose_object_estimate(&self) -> Result<u64> {
        let dir = layout::fanout_dir(self.root(), ESTIMATE_SAMPLE);
        let sampled = match fs::read_dir(&dir) {
            Ok(entries) => entries.count() as u64,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(io_error(&dir, e)),
        };
        Ok(sampled * 256)
    }

    /// Reads an object out of the pack that holds it.
    ///
    /// A pack that vanished between the lookup and the read is a rewrite
    /// running beside this read: the replacement is durable before the
    /// original goes, so forgetting the vanished pack and searching once
    /// more finds the object wherever it moved to.
    fn get_packed(&self, hash: &Hash) -> Result<Vec<u8>> {
        for _ in 0..2 {
            let Some((pack, entry)) = self.locate_packed(hash)? else {
                break;
            };
            match format::read_entry(&layout::pack_path(self.root(), &pack), hash, &entry) {
                Err(Error::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {
                    self.packs_mut().forget(&pack);
                }
                other => return other,
            }
        }
        Err(Error::MissingObject(*hash))
    }

    /// The pack holding `hash` and its entry, `None` when no pack does.
    /// A miss rescans `packs/` once, which is what makes a pack another
    /// process wrote visible to this handle.
    fn locate_packed(&self, hash: &Hash) -> Result<Option<(Hash, PackEntry)>> {
        if let Some(found) = self.packs().lookup(hash) {
            return Ok(Some(found));
        }
        let mut cache = self.packs_mut();
        cache.rescan(self.root())?;
        Ok(cache.lookup(hash))
    }
}

#[cfg(test)]
mod tests {
    use crate::layout;
    use crate::pack::format;
    use crate::testutil::{pack_objects, temp_store};
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
    fn a_packed_object_answers_get_and_has_after_its_loose_file_is_gone() -> Result<()> {
        let (dir, store) = temp_store();
        let hash = store.put(b"packed payload")?;
        pack_objects(&store, &[hash]);
        // The loose file is gone; the pack is the only copy left.
        assert!(!layout::object_path(dir.path(), &hash).exists());
        assert!(store.has(&hash)?);
        assert_eq!(store.get(&hash)?, b"packed payload");
        Ok(())
    }

    #[test]
    fn put_of_packed_content_writes_no_loose_file() -> Result<()> {
        let (dir, store) = temp_store();
        let hash = store.put(b"packed payload")?;
        pack_objects(&store, &[hash]);
        // `put` skips what the store already holds, in whichever
        // representation it holds it.
        assert_eq!(store.put(b"packed payload")?, hash);
        assert_eq!(object_count(dir.path()), 0);
        Ok(())
    }

    #[test]
    fn get_of_an_absent_hash_over_a_packed_store_is_missing_object() -> Result<()> {
        let (_dir, store) = temp_store();
        let hash = store.put(b"packed payload")?;
        pack_objects(&store, &[hash]);
        let absent = hash_bytes(b"never stored");
        assert!(!store.has(&absent)?);
        match store.get(&absent) {
            Err(Error::MissingObject(h)) => assert_eq!(h, absent),
            other => panic!("expected MissingObject, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn get_of_a_tampered_pack_entry_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let hash = store.put(b"packed payload")?;
        let pack = pack_objects(&store, &[hash]);
        // Flip a byte of the pack's data region, past the header: the read
        // re-hashes what it decoded, so the tampering surfaces here exactly
        // as it does on a loose object.
        let path = layout::pack_path(dir.path(), &pack);
        let mut bytes = fs::read(&path).expect("read pack");
        bytes[12] ^= 0xff;
        fs::write(&path, &bytes).expect("tamper pack");
        assert!(matches!(store.get(&hash), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn a_pack_written_after_the_first_lookup_is_found() -> Result<()> {
        let (_dir, store) = temp_store();
        // A miss loads the store's view of packs/, so a pack that appeared
        // after this handle last looked is still found.
        let absent = hash_bytes(b"packed later");
        assert!(!store.has(&absent)?);
        let hash = store.put(b"packed later")?;
        pack_objects(&store, &[hash]);
        assert!(store.has(&hash)?);
        assert_eq!(store.get(&hash)?, b"packed later");
        Ok(())
    }

    #[test]
    fn a_pack_replaced_between_the_lookup_and_the_read_still_answers() -> Result<()> {
        let (_dir, store) = temp_store();
        let kept = store.put(b"kept payload")?;
        let dropped = store.put(b"dropped payload")?;
        let first = pack_objects(&store, &[kept, dropped]);
        // Load the cache against the first pack, then rewrite it the way a
        // removal does: the replacement lands durably before the original
        // goes, so the reader that meets the vanished file finds the object
        // on its retry.
        assert_eq!(store.get(&kept)?, b"kept payload");
        let replacement = format::write_pack(store.root(), &[kept], &|hash| store.get(hash))?;
        assert_ne!(replacement, first);
        fs::remove_file(layout::pack_path(store.root(), &first)).expect("delete the doomed pack");
        assert_eq!(store.get(&kept)?, b"kept payload");
        assert!(matches!(store.get(&dropped), Err(Error::MissingObject(_))));
        Ok(())
    }

    #[test]
    fn the_loose_estimate_scales_one_fan_out_directory() -> Result<()> {
        let (dir, store) = temp_store();
        // The fan-out is uniform, so one subdirectory's count stands for
        // all 256 of them. Built by hand, the arithmetic is exact.
        let fanout = dir.path().join("objects").join("00");
        fs::create_dir_all(&fanout).expect("create fan-out dir");
        for i in 0..3u8 {
            fs::write(fanout.join(format!("{:02x}{}", i, "0".repeat(62))), b"")
                .expect("write object");
        }
        assert_eq!(store.loose_object_estimate()?, 3 * 256);
        Ok(())
    }

    #[test]
    fn the_loose_estimate_of_a_store_without_that_fan_out_is_zero() -> Result<()> {
        let (_dir, store) = temp_store();
        // An empty store, and one whose objects all landed elsewhere, both
        // estimate zero — coarse exactly where the count is small enough
        // for the answer not to matter.
        assert_eq!(store.loose_object_estimate()?, 0);
        Ok(())
    }

    #[test]
    fn the_loose_estimate_lands_in_the_order_of_the_true_count() -> Result<()> {
        let (_dir, store) = temp_store();
        for i in 0..1000u64 {
            store.put(&i.to_le_bytes())?;
        }
        // blake3 spreads 1000 objects over 256 subdirectories, so the
        // sampled directory holds about four of them. A bound of 25 is far
        // beyond anything that spread produces.
        assert!(
            store.loose_object_estimate()? <= 25 * 256,
            "the estimate stays in the order of the true count"
        );
        Ok(())
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
