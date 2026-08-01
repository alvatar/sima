//! The maintenance operations: consolidating loose objects into packs,
//! under the lock that serializes every operation reshaping `packs/`.
//!
//! Objects are born loose, because the write path — full content to `tmp/`,
//! fsync, rename — is the store's crash-safety spine. Packing is what an
//! operator runs to collapse those files into a few, and it never changes
//! what the store holds, only how it holds it.
//!
//! The operation is safe to interrupt at any point, because it never
//! removes the last copy of anything: a loose file goes only once a pack
//! holding that object is durable. A killed run therefore leaves a store
//! where some objects are readable twice, and re-running the operation
//! converges — the completed packs are seen as packs, and the loose files
//! they absorbed are deleted without writing anything.

use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::Path;

use sima_core::{Error, Hash, Result};

use crate::atomic::{self, io_error};
use crate::layout;
use crate::lock::take_file_lock;
use crate::pack::format::{self, MAX_PACK_RAW_BYTES};
use crate::store::Store;

/// What a [`Store::pack`] call did.
#[derive(Debug, PartialEq, Eq)]
pub struct PackReport {
    /// Objects written into new packs.
    pub objects_packed: usize,
    /// Packs written.
    pub packs_written: usize,
    /// Loose files deleted, including duplicates of already-packed objects.
    pub loose_removed: usize,
    /// Raw bytes of the objects packed.
    pub raw_bytes: u64,
    /// Bytes the new pack files occupy, indices and footers included.
    pub stored_bytes: u64,
}

/// Exclusive right to reshape the store's packs: holds the OS lock on
/// `packs/maintenance.lock`, released by the kernel when the holder exits,
/// however it exits. Unlocks on drop.
pub(crate) struct MaintenanceLock {
    /// The locked file, unlocked on drop.
    file: File,
}

impl Drop for MaintenanceLock {
    /// Frees the lock the moment the guard goes, for the reason
    /// [`crate::RunLock`]'s drop does.
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl Store {
    /// Consolidates every loose object into packs and deletes the loose
    /// files the packs absorbed.
    ///
    /// Serialized against every other maintenance operation by
    /// `packs/maintenance.lock`; contention is [`sima_core::Error::Validation`]
    /// naming the holder.
    pub fn pack(&self) -> Result<PackReport> {
        let _lock = self.acquire_maintenance_lock()?;
        self.packs_mut().rescan(self.root())?;

        // What is loose, split by whether a pack already holds it. A loose
        // duplicate is what a death between the pack write and the deletion
        // leaves: it is deleted, never packed again.
        let mut unpacked = Vec::new();
        let mut duplicates = Vec::new();
        {
            let cache = self.packs();
            for hash in self.cas_objects()? {
                if cache.lookup(&hash).is_some() {
                    duplicates.push(hash);
                } else {
                    unpacked.push(hash);
                }
            }
        }
        let sizes = loose_sizes(self.root(), &unpacked)?;

        let mut report = PackReport {
            objects_packed: 0,
            packs_written: 0,
            loose_removed: 0,
            raw_bytes: 0,
            stored_bytes: 0,
        };
        for group in format::split_by_cap(&sizes, MAX_PACK_RAW_BYTES) {
            let hashes: Vec<Hash> = group.iter().map(|(hash, _)| *hash).collect();
            let name = format::write_pack(self.root(), &hashes, &|hash| self.get(hash))?;
            let path = layout::pack_path(self.root(), &name);
            report.stored_bytes += fs::metadata(&path).map_err(|e| io_error(&path, e))?.len();
            report.raw_bytes += group.iter().map(|(_, raw_len)| raw_len).sum::<u64>();
            report.objects_packed += group.len();
            report.packs_written += 1;
            sima_core::crashpoint("pack.after-pack-write");
        }

        // The packs are durable and loadable before a single loose file
        // goes: the rescan is what proves the second half, since a pack
        // whose index does not load fails here, with every object still
        // readable where it has always been.
        self.packs_mut().rescan(self.root())?;
        for hash in sizes.iter().map(|(hash, _)| hash).chain(&duplicates) {
            atomic::remove_file_idempotent(&layout::object_path(self.root(), hash))?;
            report.loose_removed += 1;
            sima_core::crashpoint("pack.mid-loose-delete");
        }
        Ok(report)
    }

    /// Takes the store's maintenance lock. A lock already held is
    /// [`sima_core::Error::Validation`] naming the holder recorded in the
    /// file (pid, hostname).
    pub(crate) fn acquire_maintenance_lock(&self) -> Result<MaintenanceLock> {
        let file = take_file_lock(&layout::maintenance_lock_path(self.root()), |holder| {
            Error::Validation(format!(
                "store maintenance is already running in another process: {holder}"
            ))
        })?;
        Ok(MaintenanceLock { file })
    }
}

/// The raw size of each loose object, in the walk's order — what the cap
/// split partitions on. An object whose file went between the walk and the
/// stat is dropped: it is no longer in the store, so it is nothing to pack.
fn loose_sizes(root: &Path, objects: &[Hash]) -> Result<Vec<(Hash, u64)>> {
    let mut sizes = Vec::with_capacity(objects.len());
    for hash in objects {
        let path = layout::object_path(root, hash);
        match fs::metadata(&path) {
            Ok(meta) => sizes.push((*hash, meta.len())),
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(io_error(&path, e)),
        }
    }
    Ok(sizes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::temp_store;
    use sima_core::hash_bytes;
    use std::collections::BTreeSet;

    /// Puts `count` objects of distinct content, returning their addresses.
    fn put_objects(store: &Store, count: u64) -> Vec<Hash> {
        (0..count)
            .map(|i| store.put(&payload(i)).expect("put object"))
            .collect()
    }

    /// The content of test object `i`: long enough to compress, distinct
    /// per index.
    fn payload(i: u64) -> Vec<u8> {
        format!("object {i} ").repeat(64).into_bytes()
    }

    /// The loose object files a store holds, by address.
    fn loose_objects(root: &Path) -> BTreeSet<Hash> {
        let mut found = BTreeSet::new();
        for fanout in fs::read_dir(root.join("objects")).expect("read objects dir") {
            let fanout = fanout.expect("fan-out entry").path();
            for entry in fs::read_dir(&fanout).expect("read fan-out") {
                let name = entry.expect("object entry").file_name();
                found.insert(
                    Hash::from_hex(name.to_str().expect("utf-8 name")).expect("object name"),
                );
            }
        }
        found
    }

    /// The packs a store holds, by name.
    fn packs(root: &Path) -> BTreeSet<Hash> {
        fs::read_dir(root.join("packs"))
            .expect("read packs dir")
            .filter_map(|entry| {
                let name = entry.expect("pack entry").file_name();
                let name = name.to_str().expect("utf-8 name").to_string();
                name.strip_suffix(".pack")
                    .map(|hex| Hash::from_hex(hex).expect("pack name"))
            })
            .collect()
    }

    /// Every object the store answers for, read back and verified.
    fn readable(store: &Store, objects: &[Hash]) {
        for hash in objects {
            assert!(store.has(hash).expect("has"), "object {hash} is present");
            let bytes = store.get(hash).expect("get");
            assert_eq!(hash_bytes(&bytes), *hash, "object {hash} reads back whole");
        }
    }

    #[test]
    fn packing_moves_every_loose_object_into_packs() -> Result<()> {
        let (dir, store) = temp_store();
        let objects = put_objects(&store, 8);
        let raw: u64 = (0..8).map(|i| payload(i).len() as u64).sum();

        let report = store.pack()?;
        assert_eq!(report.objects_packed, 8);
        assert_eq!(report.packs_written, 1);
        assert_eq!(report.loose_removed, 8);
        assert_eq!(report.raw_bytes, raw);
        assert!(report.stored_bytes > 0);

        // The objects are readable, and the files that held them are gone.
        readable(&store, &objects);
        assert!(loose_objects(dir.path()).is_empty());
        assert_eq!(packs(dir.path()).len(), 1);
        // A completed operation leaves nothing in flight.
        assert_eq!(
            fs::read_dir(dir.path().join("tmp"))
                .expect("read tmp")
                .count(),
            0
        );
        Ok(())
    }

    #[test]
    fn a_second_pack_writes_and_deletes_nothing() -> Result<()> {
        let (dir, store) = temp_store();
        let objects = put_objects(&store, 4);
        store.pack()?;
        let before = packs(dir.path());

        let report = store.pack()?;
        assert_eq!(
            report,
            PackReport {
                objects_packed: 0,
                packs_written: 0,
                loose_removed: 0,
                raw_bytes: 0,
                stored_bytes: 0,
            }
        );
        assert_eq!(packs(dir.path()), before, "the packs are untouched");
        readable(&store, &objects);
        Ok(())
    }

    #[test]
    fn a_loose_duplicate_of_a_packed_object_is_deleted_without_repacking() -> Result<()> {
        let (dir, store) = temp_store();
        let objects = put_objects(&store, 2);
        store.pack()?;
        let before = packs(dir.path());
        // A loose copy of an object the packs already hold: what a crash
        // between the pack write and the loose deletion leaves.
        let path = layout::object_path(dir.path(), &objects[0]);
        fs::create_dir_all(path.parent().expect("fan-out")).expect("create fan-out");
        fs::write(&path, store.get(&objects[0])?).expect("restore loose duplicate");

        let report = store.pack()?;
        assert_eq!(report.packs_written, 0);
        assert_eq!(report.objects_packed, 0);
        assert_eq!(report.loose_removed, 1);
        assert_eq!(packs(dir.path()), before, "nothing was repacked");
        assert!(loose_objects(dir.path()).is_empty());
        readable(&store, &objects);
        Ok(())
    }

    #[test]
    fn a_death_between_pack_writes_converges_on_re_run() -> Result<()> {
        let (dir, store) = temp_store();
        let objects = put_objects(&store, 6);
        // The state a death inside the write phase leaves: one pack durable,
        // every loose file still in place.
        let packed: Vec<Hash> = objects[..3].to_vec();
        format::write_pack(store.root(), &packed, &|hash| store.get(hash))?;
        assert_eq!(loose_objects(dir.path()).len(), 6);

        let report = store.pack()?;
        // The completed pack is seen as one: only the remainder is packed,
        // and every loose file goes.
        assert_eq!(report.objects_packed, 3);
        assert_eq!(report.loose_removed, 6);
        readable(&store, &objects);
        assert!(loose_objects(dir.path()).is_empty());
        // The partition differs from an uninterrupted run's, which is a fact
        // about the store's shape and not about what it holds.
        assert_eq!(packs(dir.path()).len(), 2);
        Ok(())
    }

    #[test]
    fn a_death_mid_loose_delete_converges_on_re_run() -> Result<()> {
        let (dir, store) = temp_store();
        let objects = put_objects(&store, 4);
        // The state a death inside the deletion phase leaves: the pack is
        // durable and some of the loose files it absorbed are still there.
        let name = format::write_pack(store.root(), &objects, &|hash| store.get(hash))?;
        for hash in &objects[..2] {
            fs::remove_file(layout::object_path(dir.path(), hash)).expect("delete loose object");
        }

        let report = store.pack()?;
        assert_eq!(report.packs_written, 0, "nothing is written twice");
        assert_eq!(report.loose_removed, 2);
        assert_eq!(packs(dir.path()), BTreeSet::from([name]));
        assert!(loose_objects(dir.path()).is_empty());
        readable(&store, &objects);
        Ok(())
    }

    #[test]
    fn every_object_is_readable_throughout_a_pack() -> Result<()> {
        let (_dir, store) = temp_store();
        let objects = put_objects(&store, 32);
        // A reader hammering the store while the operation moves every
        // object beneath it must never miss one: the loose file goes only
        // after a pack holding it is durable.
        std::thread::scope(|scope| {
            let store = &store;
            let objects = &objects;
            let reader = scope.spawn(move || {
                for _ in 0..20 {
                    readable(store, objects);
                }
            });
            store.pack()?;
            reader.join().expect("reader thread panicked");
            Ok::<(), Error>(())
        })?;
        readable(&store, &objects);
        Ok(())
    }

    #[test]
    fn a_put_racing_a_pack_lands_in_the_store() -> Result<()> {
        let (_dir, store) = temp_store();
        let objects = put_objects(&store, 16);
        let late = std::thread::scope(|scope| {
            let store = &store;
            let writer = scope.spawn(move || store.put(b"a late object"));
            store.pack()?;
            writer.join().expect("writer thread panicked")
        })?;
        // Whether it landed before the walk or after it, the object is in
        // the store: as a loose file the next pack absorbs, or already in
        // one.
        readable(&store, &objects);
        readable(&store, &[late]);
        Ok(())
    }

    #[test]
    fn a_second_maintenance_operation_names_the_holder() -> Result<()> {
        let (_dir, store) = temp_store();
        let _held = store.acquire_maintenance_lock()?;
        match store.acquire_maintenance_lock() {
            Err(Error::Validation(msg)) => {
                let pid = std::process::id().to_string();
                assert!(msg.contains(&pid), "the error names the holder: {msg}");
            }
            Err(other) => panic!("expected Validation, got {other}"),
            Ok(_) => panic!("a held maintenance lock must not be taken again"),
        }
        Ok(())
    }

    #[test]
    fn the_maintenance_lock_is_released_on_drop() -> Result<()> {
        let (_dir, store) = temp_store();
        drop(store.acquire_maintenance_lock()?);
        store.acquire_maintenance_lock()?;
        Ok(())
    }
}
