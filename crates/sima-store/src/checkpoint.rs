//! Resume-checkpoint slots: mutable per-chain scratch state under a search.
//!
//! A slot holds the latest continuation state a running segment offered —
//! nothing more. Checkpoints are disposable by contract: they enter no hash,
//! no record, and no manifest, and losing one costs recovery time only. The
//! surface is latest-only — [`Store::save_checkpoint`] overwrites the slot,
//! [`Store::checkpoint`] reads it back — and there is no deletion; the next
//! segment's saves overwrite the previous segment's slot.
//!
//! The file frames its payload with the canonical codec — tag, the owning
//! task's key digest, payload bytes — so a read can tell whose state it
//! holds. A slot that is missing, malformed, or keyed to a different task
//! loads as `None`: a stale or torn checkpoint is skipped, never an error.

use std::fs;
use std::io::ErrorKind;

use sima_core::{Dec, Enc, Hash, Result};
use sima_model::{SearchId, TaskKey};

use crate::atomic::{self, io_error};
use crate::layout;
use crate::store::Store;

/// Frame tag identifying a checkpoint-slot file.
const TAG_CHECKPOINT: &str = "sima.checkpoint.v1";

impl Store {
    /// Writes `payload` as the latest checkpoint of chain `slot`, owned by
    /// the task at `key`, replacing any previous content. The write goes
    /// through the store's one atomic-write path, so a crash leaves the
    /// previous slot content or the new one, never a torn file the reader
    /// could mistake for state.
    pub fn save_checkpoint(
        &self,
        search: &SearchId,
        slot: u64,
        key: &TaskKey,
        payload: &[u8],
    ) -> Result<()> {
        atomic::create_dir_durable(&layout::checkpoint_dir(self.root(), search))?;
        let mut enc = Enc::new();
        enc.str(TAG_CHECKPOINT).hash(key.as_hash()).bytes(payload);
        let path = layout::checkpoint_path(self.root(), search, slot);
        atomic::write_atomic(self.root(), &path, &enc.finish())
    }

    /// Reads back the checkpoint of chain `slot` if it belongs to the task
    /// at `key`. A missing slot, a malformed frame, a wrong tag, or a frame
    /// keyed to another task all load as `None` — checkpoints are disposable,
    /// so anything unusable is skipped, never an error. Only a genuine I/O
    /// failure is `Err`.
    pub fn checkpoint(
        &self,
        search: &SearchId,
        slot: u64,
        key: &TaskKey,
    ) -> Result<Option<Vec<u8>>> {
        let path = layout::checkpoint_path(self.root(), search, slot);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error(&path, e)),
        };
        Ok(decode_owned_payload(&bytes, key.as_hash()))
    }
}

/// Decodes a slot frame, returning the payload only when the frame is whole
/// and its key digest matches `owner`.
fn decode_owned_payload(bytes: &[u8], owner: &Hash) -> Option<Vec<u8>> {
    let mut dec = Dec::new(bytes);
    if dec.str().ok()? != TAG_CHECKPOINT {
        return None;
    }
    if dec.hash().ok()? != *owner {
        return None;
    }
    let payload = dec.bytes().ok()?.to_vec();
    dec.finish().ok()?;
    Some(payload)
}

#[cfg(test)]
mod tests {
    use crate::testutil::{sample_identity, sample_search_config, temp_store};
    use sima_core::Result;
    use std::fs;

    #[test]
    fn save_then_load_round_trips_at_the_pinned_path() -> Result<()> {
        let (dir, store) = temp_store();
        let search = sample_search_config(42).id();
        let key = sample_identity(1).key();
        store.save_checkpoint(&search, 3, &key, b"continuation bytes")?;
        assert_eq!(
            store.checkpoint(&search, 3, &key)?.as_deref(),
            Some(b"continuation bytes".as_slice())
        );
        // The slot path is part of the fixed layout contract.
        let expected = dir
            .path()
            .join("searches")
            .join(search.to_string())
            .join("checkpoint")
            .join("3");
        assert!(expected.is_file());
        Ok(())
    }

    #[test]
    fn a_missing_slot_loads_as_none() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = sample_search_config(42).id();
        let key = sample_identity(1).key();
        assert_eq!(store.checkpoint(&search, 0, &key)?, None);
        Ok(())
    }

    #[test]
    fn a_second_save_overwrites_the_first() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = sample_search_config(42).id();
        let key = sample_identity(1).key();
        store.save_checkpoint(&search, 0, &key, b"first")?;
        store.save_checkpoint(&search, 0, &key, b"second")?;
        assert_eq!(
            store.checkpoint(&search, 0, &key)?.as_deref(),
            Some(b"second".as_slice())
        );
        Ok(())
    }

    #[test]
    fn a_slot_keyed_to_another_task_loads_as_none() -> Result<()> {
        // The stale previous-segment case: the slot survives with the old
        // segment's key, and the new segment must not adopt it.
        let (_dir, store) = temp_store();
        let search = sample_search_config(42).id();
        let previous = sample_identity(1).key();
        let current = sample_identity(2).key();
        store.save_checkpoint(&search, 0, &previous, b"old state")?;
        assert_eq!(store.checkpoint(&search, 0, &current)?, None);
        // The owning key still reads it.
        assert_eq!(
            store.checkpoint(&search, 0, &previous)?.as_deref(),
            Some(b"old state".as_slice())
        );
        Ok(())
    }

    #[test]
    fn a_corrupted_slot_loads_as_none() -> Result<()> {
        let (dir, store) = temp_store();
        let search = sample_search_config(42).id();
        let key = sample_identity(1).key();
        store.save_checkpoint(&search, 0, &key, b"payload")?;
        let path = dir
            .path()
            .join("searches")
            .join(search.to_string())
            .join("checkpoint")
            .join("0");
        let full = fs::read(&path).expect("read slot");
        // A torn prefix, garbage, and an empty file all load as None.
        for bytes in [&full[..full.len() / 2], b"garbage".as_slice(), b""] {
            fs::write(&path, bytes).expect("corrupt slot");
            assert_eq!(store.checkpoint(&search, 0, &key)?, None);
        }
        Ok(())
    }

    #[test]
    fn slots_are_independent_per_chain() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = sample_search_config(42).id();
        let key_a = sample_identity(1).key();
        let key_b = sample_identity(2).key();
        store.save_checkpoint(&search, 0, &key_a, b"chain zero")?;
        store.save_checkpoint(&search, 1, &key_b, b"chain one")?;
        assert_eq!(
            store.checkpoint(&search, 0, &key_a)?.as_deref(),
            Some(b"chain zero".as_slice())
        );
        assert_eq!(
            store.checkpoint(&search, 1, &key_b)?.as_deref(),
            Some(b"chain one".as_slice())
        );
        Ok(())
    }
}
