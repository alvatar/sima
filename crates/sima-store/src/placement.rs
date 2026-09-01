//! Placement slots: mutable per-chain device bindings under a search.
//!
//! A slot records the device class a chain's work searches on, so every segment,
//! retry, and resumed attempt of one chain reaches the same class. The binding
//! is advisory coherence state: it enters no hash, no record, and no manifest,
//! and losing one costs coherence for that chain, never correctness — an
//! unbound chain simply binds again on the next pull.
//!
//! The surface is latest-only — [`Store::bind_chain`] writes the slot,
//! [`Store::chain_bindings`] reads a search's slots back for resume — and there is
//! no deletion; a rebind overwrites.
//!
//! The payload is opaque here, exactly as checkpoint bytes are: the store
//! depends on core and model alone, so the layer that owns the binding's
//! meaning owns its encoding. The file frames the payload with the canonical
//! codec under a tag, so a read can tell the frame is a placement slot; a slot
//! that is missing or malformed is skipped, never an error.

use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;

use sima_core::{Dec, Enc, Result};
use sima_model::SearchId;

use crate::atomic::{self, io_error};
use crate::layout;
use crate::store::Store;

/// Frame tag identifying a placement-slot file.
const TAG_PLACEMENT: &str = "sima.placement.v1";

impl Store {
    /// Binds chain `chain` to `payload`, replacing any previous binding —
    /// that replacement is the rebind. The write goes through the store's one
    /// atomic-write path, so a crash leaves the previous binding or the new
    /// one, never a torn file.
    pub fn bind_chain(&self, search: &SearchId, chain: u64, payload: &[u8]) -> Result<()> {
        atomic::create_dir_durable(&layout::placement_dir(self.root(), search))?;
        let mut enc = Enc::new();
        enc.str(TAG_PLACEMENT).bytes(payload);
        let path = layout::placement_path(self.root(), search, chain);
        atomic::write_atomic(self.root(), &path, &enc.finish())
    }

    /// Every chain binding the search holds, for seeding placement on resume. A
    /// search with no slots — and a search whose placement directory was never
    /// created — reads as an empty map. Entries that do not name a chain, or
    /// whose frame is unusable, are skipped: an unbound chain binds again.
    /// Only a genuine I/O failure is `Err`.
    pub fn chain_bindings(&self, search: &SearchId) -> Result<HashMap<u64, Vec<u8>>> {
        let dir = layout::placement_dir(self.root(), search);
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(e) => return Err(io_error(&dir, e)),
        };
        let mut bindings = HashMap::new();
        for entry in entries {
            let path = entry.map_err(|e| io_error(&dir, e))?.path();
            // The file name is the chain; anything else in the directory is
            // not a slot this search wrote.
            let Some(chain) = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<u64>().ok())
            else {
                continue;
            };
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(io_error(&path, e)),
            };
            if let Some(payload) = decode_payload(&bytes) {
                bindings.insert(chain, payload);
            }
        }
        Ok(bindings)
    }
}

/// Decodes a slot frame, returning the payload only when the frame is whole
/// and carries the placement tag.
fn decode_payload(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut dec = Dec::new(bytes);
    if dec.str().ok()? != TAG_PLACEMENT {
        return None;
    }
    let payload = dec.bytes().ok()?.to_vec();
    dec.finish().ok()?;
    Some(payload)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sima_core::Result;

    use crate::testutil::{sample_search_config, temp_store};

    #[test]
    fn a_binding_round_trips_at_the_pinned_path() -> Result<()> {
        let (dir, store) = temp_store();
        let search = sample_search_config(42).id();
        store.bind_chain(&search, 3, br#"{"vendor_id":32902,"device_id":32081}"#)?;
        assert_eq!(
            store.chain_bindings(&search)?.get(&3).map(Vec::as_slice),
            Some(br#"{"vendor_id":32902,"device_id":32081}"#.as_slice())
        );
        // The slot path is part of the fixed layout contract.
        let expected = dir
            .path()
            .join("searches")
            .join(search.to_string())
            .join("placement")
            .join("3");
        assert!(expected.is_file());
        Ok(())
    }

    #[test]
    fn a_run_with_no_bindings_reads_empty() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = sample_search_config(42).id();
        assert!(store.chain_bindings(&search)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_rebind_overwrites_the_previous_binding() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = sample_search_config(42).id();
        store.bind_chain(&search, 0, b"first class")?;
        store.bind_chain(&search, 0, b"second class")?;
        let bindings = store.chain_bindings(&search)?;
        assert_eq!(bindings.len(), 1, "the rebind replaces, never appends");
        assert_eq!(
            bindings.get(&0).map(Vec::as_slice),
            Some(b"second class".as_slice())
        );
        Ok(())
    }

    #[test]
    fn every_chain_of_a_run_reads_back() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = sample_search_config(42).id();
        for chain in 0..4u64 {
            store.bind_chain(&search, chain, format!("class {chain}").as_bytes())?;
        }
        let bindings = store.chain_bindings(&search)?;
        assert_eq!(bindings.len(), 4);
        for chain in 0..4u64 {
            assert_eq!(
                bindings.get(&chain).map(Vec::as_slice),
                Some(format!("class {chain}").as_bytes())
            );
        }
        Ok(())
    }

    #[test]
    fn bindings_are_scoped_to_their_run() -> Result<()> {
        let (_dir, store) = temp_store();
        let one = sample_search_config(1).id();
        let two = sample_search_config(2).id();
        store.bind_chain(&one, 0, b"one's class")?;
        assert!(store.chain_bindings(&two)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_torn_write_is_invisible_to_readers() -> Result<()> {
        // The write that never reached its rename: the payload sits in tmp/,
        // so the slot the reader looks for does not exist. The binding is
        // advisory, so the chain simply binds again.
        let (dir, store) = temp_store();
        let search = sample_search_config(42).id();
        fs::create_dir_all(
            dir.path()
                .join("searches")
                .join(search.to_string())
                .join("placement"),
        )
        .expect("create the placement directory");
        fs::write(dir.path().join("tmp").join("999-0"), b"never renamed")
            .expect("write the in-flight file");
        assert!(store.chain_bindings(&search)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_malformed_slot_is_skipped() -> Result<()> {
        // A slot whose frame does not decode carries no usable binding; the
        // chain binds again rather than the search failing.
        let (dir, store) = temp_store();
        let search = sample_search_config(42).id();
        store.bind_chain(&search, 0, b"intact")?;
        let placement = dir
            .path()
            .join("searches")
            .join(search.to_string())
            .join("placement");
        fs::write(placement.join("1"), b"not a frame").expect("write a malformed slot");
        let bindings = store.chain_bindings(&search)?;
        assert_eq!(bindings.len(), 1, "the intact slot still reads");
        assert_eq!(
            bindings.get(&0).map(Vec::as_slice),
            Some(b"intact".as_slice())
        );
        Ok(())
    }

    #[test]
    fn a_file_that_does_not_name_a_chain_is_skipped() -> Result<()> {
        let (dir, store) = temp_store();
        let search = sample_search_config(42).id();
        store.bind_chain(&search, 0, b"intact")?;
        let placement = dir
            .path()
            .join("searches")
            .join(search.to_string())
            .join("placement");
        fs::write(placement.join("not-a-chain"), b"stray").expect("write a stray file");
        assert_eq!(store.chain_bindings(&search)?.len(), 1);
        Ok(())
    }
}
