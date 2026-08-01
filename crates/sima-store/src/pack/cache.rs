//! [`PackCache`]: where each packed object lives, held in memory.
//!
//! A pack file is immutable, so its index is loaded once and stays true for
//! as long as the file exists. The only mutations `packs/` ever sees are a
//! whole file appearing and a whole file disappearing, which is what makes
//! this cache cheap to keep honest: a rescan lists the directory — a
//! handful of names — and loads indices only for the packs it has not seen.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use sima_core::{Error, Hash, Result};

use crate::atomic::io_error;
use crate::layout;
use crate::pack::format::{self, PackEntry};

/// The pack indices this handle has loaded, and where each packed object
/// sits.
///
/// Every rescan builds its state fully before installing it, so a failure
/// part-way leaves the previous state intact rather than a half-loaded one.
pub(crate) struct PackCache {
    /// Names of the packs whose indices are loaded.
    loaded: HashSet<Hash>,
    /// Every packed object: the pack that holds it, and where inside it.
    objects: HashMap<Hash, (Hash, PackEntry)>,
}

impl PackCache {
    /// An empty cache, which one rescan fills.
    pub(crate) fn new() -> PackCache {
        unimplemented!("PackCache::new")
    }

    /// Where `hash` lives: the pack holding it, and its entry.
    pub(crate) fn lookup(&self, _hash: &Hash) -> Option<(Hash, PackEntry)> {
        unimplemented!("PackCache::lookup")
    }

    /// Brings the cache level with `packs/`: loads the indices of packs it
    /// has not seen, and forgets everything when a pack it holds has
    /// vanished, since the objects that pack held now live elsewhere.
    pub(crate) fn rescan(&mut self, _root: &Path) -> Result<()> {
        unimplemented!("PackCache::rescan")
    }

    /// Drops one pack's entries, for a reader that met the file already
    /// gone — a concurrent rewrite replaced it, and the next lookup must
    /// find the replacement.
    pub(crate) fn forget(&mut self, _pack: &Hash) {
        unimplemented!("PackCache::forget")
    }
}

/// The packs present in `packs/`, by name. `maintenance.lock` is the one
/// other file that belongs there; anything else is [`Error::Corruption`],
/// as a foreign entry is under `objects/`.
fn present_packs(_root: &Path) -> Result<HashSet<Hash>> {
    unimplemented!("present_packs")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{pack_objects, temp_store};
    use sima_core::hash_bytes;

    #[test]
    fn a_rescan_finds_the_objects_of_every_pack() -> Result<()> {
        let (_dir, store) = temp_store();
        let first: Vec<Hash> = [b"alpha".as_slice(), b"beta".as_slice()]
            .iter()
            .map(|bytes| store.put(bytes).expect("put"))
            .collect();
        let second = vec![store.put(b"gamma").expect("put")];
        pack_objects(&store, &first);
        pack_objects(&store, &second);

        let mut cache = PackCache::new();
        cache.rescan(store.root())?;
        for hash in first.iter().chain(&second) {
            assert!(cache.lookup(hash).is_some(), "packed object {hash} is known");
        }
        assert!(cache.lookup(&hash_bytes(b"never packed")).is_none());
        Ok(())
    }

    #[test]
    fn a_rescan_loads_only_the_packs_it_has_not_seen() -> Result<()> {
        let (_dir, store) = temp_store();
        let first = vec![store.put(b"alpha").expect("put")];
        let first_pack = pack_objects(&store, &first);

        let mut cache = PackCache::new();
        cache.rescan(store.root())?;
        assert_eq!(cache.loaded, HashSet::from([first_pack]));

        let second = vec![store.put(b"beta").expect("put")];
        let second_pack = pack_objects(&store, &second);
        cache.rescan(store.root())?;
        assert_eq!(cache.loaded, HashSet::from([first_pack, second_pack]));
        // A rescan over an unchanged directory changes nothing.
        cache.rescan(store.root())?;
        assert_eq!(cache.loaded, HashSet::from([first_pack, second_pack]));
        Ok(())
    }

    #[test]
    fn a_rescan_drops_the_entries_of_a_vanished_pack() -> Result<()> {
        let (_dir, store) = temp_store();
        let doomed = vec![store.put(b"alpha").expect("put")];
        let kept = vec![store.put(b"beta").expect("put")];
        let doomed_pack = pack_objects(&store, &doomed);
        pack_objects(&store, &kept);

        let mut cache = PackCache::new();
        cache.rescan(store.root())?;
        fs::remove_file(layout::pack_path(store.root(), &doomed_pack)).expect("delete pack");
        cache.rescan(store.root())?;
        assert!(cache.lookup(&doomed[0]).is_none(), "its objects are dropped");
        assert!(cache.lookup(&kept[0]).is_some(), "the other pack survives");
        Ok(())
    }

    #[test]
    fn forgetting_a_pack_drops_its_entries_alone() -> Result<()> {
        let (_dir, store) = temp_store();
        let first = vec![store.put(b"alpha").expect("put")];
        let second = vec![store.put(b"beta").expect("put")];
        let first_pack = pack_objects(&store, &first);
        pack_objects(&store, &second);

        let mut cache = PackCache::new();
        cache.rescan(store.root())?;
        cache.forget(&first_pack);
        assert!(cache.lookup(&first[0]).is_none());
        assert!(cache.lookup(&second[0]).is_some());
        // The forgotten pack is loadable again, since the file is still
        // there: forgetting is about this handle's view, not the store's.
        cache.rescan(store.root())?;
        assert!(cache.lookup(&first[0]).is_some());
        Ok(())
    }

    #[test]
    fn a_foreign_entry_under_packs_is_corruption() -> Result<()> {
        let (_dir, store) = temp_store();
        fs::write(layout::packs_dir(store.root()).join("stray"), b"").expect("write stray entry");
        let mut cache = PackCache::new();
        assert!(matches!(cache.rescan(store.root()), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn the_maintenance_lock_is_not_a_pack() -> Result<()> {
        let (_dir, store) = temp_store();
        // The lock lives beside the packs it serializes access to, so the
        // scan must pass over it.
        fs::write(layout::maintenance_lock_path(store.root()), b"1 host\n").expect("write lock");
        let mut cache = PackCache::new();
        cache.rescan(store.root())?;
        assert!(cache.loaded.is_empty());
        Ok(())
    }
}
