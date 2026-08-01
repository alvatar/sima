//! Packs: many objects in one immutable file, so the store's inode count
//! follows the pack count rather than the object count.
//!
//! Objects are born loose — the write path is the crash-safety spine and
//! stays as it is. Consolidation is maintenance work an operator asks for.
//! The namespace divides into the concerns a pack has:
//!
//! - [`format`] — the pack file's bytes: writing one, loading and validating
//!   its index, reading one entry back decoded and verified.
//!
//! Object identity is untouched by packing. A packed object is addressed by
//! the blake3 digest of its uncompressed bytes, exactly as a loose one is,
//! and every read re-hashes what it decoded — the verified-read contract
//! holds through packs.

pub(crate) mod format;
