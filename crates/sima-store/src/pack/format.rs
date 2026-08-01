//! The pack file: a header, the objects' stored bytes, an index over them,
//! and a footer locating the index.
//!
//! ```text
//! offset 0        magic       8 bytes   b"simapack"
//! offset 8        version     u32       1
//! offset 12       data region           each entry's stored bytes, in index order
//!                 index region          count × 57 bytes, ascending by hash
//!                     hash        32 bytes  the object's address
//!                     offset      u64       absolute file offset of its stored bytes
//!                     stored_len  u64       bytes as stored here
//!                     raw_len     u64       bytes after decoding
//!                     encoding    u8        0 = raw, 1 = zstd
//! file end − 24   footer      24 bytes
//!                     index_offset  u64       absolute file offset of the index
//!                     count         u64       index entry count
//!                     magic         8 bytes   b"simapack"
//! ```
//!
//! The index is sorted strictly ascending by hash, which gives binary search
//! and makes the file a pure function of its object set: a fixed set writes
//! byte-identical bytes, hence an identical name, which is what makes an
//! interrupted maintenance run converge by re-running it.
//!
//! Every object is compressed on its own, so reading one object decompresses
//! one object. Identity is the address of the *uncompressed* bytes, and a
//! read re-hashes what it decoded before returning it.

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use sima_core::{Dec, Enc, Error, Hash, Hasher, Result, hash_bytes};

use crate::atomic::{self, io_error};
use crate::layout;

/// Raw bytes one pack holds at most. The cap bounds the cost of every later
/// rewrite: deleting one object rewrites the pack that holds it, never more
/// than this much data.
pub(crate) const MAX_PACK_RAW_BYTES: u64 = 1 << 30;

/// Magic opening and closing every pack file.
const MAGIC: &[u8; 8] = b"simapack";

/// The pack format this binary writes and reads.
const VERSION: u32 = 1;

/// Header length: the magic and the version.
const HEADER_LEN: u64 = 12;

/// One index entry: hash, offset, stored length, raw length, encoding.
const ENTRY_LEN: u64 = 57;

/// Footer length: the index offset, the entry count, and the magic.
const FOOTER_LEN: u64 = 24;

/// zstd level every entry is compressed at. Level 3 is zstd's default
/// balance of ratio against speed, and the level enters the file's bytes,
/// so it is fixed rather than tuned per call.
const ZSTD_LEVEL: i32 = 3;

/// How one entry's bytes are stored in the pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Encoding {
    /// Stored as they are: compression did not shrink them.
    Raw,
    /// Stored as a zstd frame.
    Zstd,
}

/// Where one object sits inside a pack, and how to decode it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PackEntry {
    /// Absolute file offset of the stored bytes.
    pub(crate) offset: u64,
    /// Length of the stored bytes.
    pub(crate) stored_len: u64,
    /// Length of the object after decoding — its identity-bearing bytes.
    pub(crate) raw_len: u64,
    /// How the stored bytes decode.
    pub(crate) encoding: Encoding,
}

impl Encoding {
    /// The byte the index records this encoding as.
    fn wire(self) -> u8 {
        match self {
            Encoding::Raw => 0,
            Encoding::Zstd => 1,
        }
    }

    /// The encoding an index byte names.
    fn from_wire(byte: u8) -> Option<Encoding> {
        match byte {
            0 => Some(Encoding::Raw),
            1 => Some(Encoding::Zstd),
            _ => None,
        }
    }
}

/// Writes a pack holding `objects` and returns its name, the blake3 digest
/// of the whole file. `source` yields each object's raw bytes.
///
/// The input is sorted and deduplicated here, so the file is a pure
/// function of the object set whatever order the caller offers it in. The
/// file is built in the store's `tmp/` directory and fsynced before it
/// enters `packs/` by rename, so a reader meets a complete pack or none.
/// An empty object set is [`Error::Validation`]: a pack with no objects is
/// a file that says nothing.
pub(crate) fn write_pack(
    root: &Path,
    objects: &[Hash],
    source: &dyn Fn(&Hash) -> Result<Vec<u8>>,
) -> Result<Hash> {
    let mut sorted = objects.to_vec();
    sorted.sort();
    sorted.dedup();
    if sorted.is_empty() {
        return Err(Error::Validation(
            "a pack holds at least one object".to_string(),
        ));
    }

    let mut writer = PackWriter::create(atomic::tmp_path(root))?;
    writer.write(MAGIC)?;
    let mut header = Enc::new();
    header.u32(VERSION);
    writer.write(&header.finish())?;

    let mut entries = Vec::with_capacity(sorted.len());
    for hash in &sorted {
        let raw = source(hash)?;
        let compressed =
            zstd::bulk::compress(&raw, ZSTD_LEVEL).map_err(|e| io_error(writer.path(), e))?;
        // zstd is kept only where it shrinks the object: an equal or larger
        // frame would cost a decompression on every read for nothing.
        let (encoding, stored) = if compressed.len() < raw.len() {
            (Encoding::Zstd, compressed.as_slice())
        } else {
            (Encoding::Raw, raw.as_slice())
        };
        let entry = PackEntry {
            offset: writer.offset(),
            stored_len: stored.len() as u64,
            raw_len: raw.len() as u64,
            encoding,
        };
        writer.write(stored)?;
        entries.push((*hash, entry));
    }

    // The index follows the data it points into, and the footer says where
    // it starts — so the reader finds the index from the file's end without
    // scanning the objects.
    let index_offset = writer.offset();
    for (hash, entry) in &entries {
        writer.write(&entry_bytes(hash, entry))?;
    }
    writer.write(&footer_bytes(index_offset, entries.len() as u64))?;

    let (tmp, name) = writer.finish()?;
    atomic::place_atomic(&tmp, &layout::pack_path(root, &name))?;
    Ok(name)
}

/// Loads and validates a pack's index, returning its entries ascending by
/// hash. Every violation of the format is [`Error::Corruption`] naming the
/// pack file.
pub(crate) fn load_index(path: &Path) -> Result<Vec<(Hash, PackEntry)>> {
    let mut file = File::open(path).map_err(|e| io_error(path, e))?;
    let len = file.metadata().map_err(|e| io_error(path, e))?.len();
    if len < HEADER_LEN + FOOTER_LEN {
        return Err(corrupt(path, "file is shorter than a header and a footer"));
    }

    let mut header = [0u8; HEADER_LEN as usize];
    file.read_exact(&mut header)
        .map_err(|e| io_error(path, e))?;
    if &header[..MAGIC.len()] != MAGIC {
        return Err(corrupt(path, "header magic is not simapack"));
    }
    let version = Dec::new(&header[MAGIC.len()..])
        .u32()
        .map_err(|_| corrupt(path, "header carries no version"))?;
    if version != VERSION {
        return Err(corrupt(
            path,
            &format!("pack format version is {version}, not {VERSION}"),
        ));
    }

    let mut footer = [0u8; FOOTER_LEN as usize];
    file.seek(SeekFrom::End(-(FOOTER_LEN as i64)))
        .map_err(|e| io_error(path, e))?;
    file.read_exact(&mut footer)
        .map_err(|e| io_error(path, e))?;
    if &footer[FOOTER_LEN as usize - MAGIC.len()..] != MAGIC {
        return Err(corrupt(path, "footer magic is not simapack"));
    }
    let mut dec = Dec::new(&footer);
    let malformed = || corrupt(path, "footer is malformed");
    let index_offset = dec.u64().map_err(|_| malformed())?;
    let count = dec.u64().map_err(|_| malformed())?;

    // The index must occupy exactly the span between where the footer says
    // it starts and the footer itself, which bounds `count` before a byte is
    // allocated for it.
    let index_len = count
        .checked_mul(ENTRY_LEN)
        .ok_or_else(|| corrupt(path, "index entry count overflows the file"))?;
    let expected = index_offset
        .checked_add(index_len)
        .and_then(|end| end.checked_add(FOOTER_LEN));
    if index_offset < HEADER_LEN || expected != Some(len) {
        return Err(corrupt(
            path,
            &format!("index of {count} entries at {index_offset} does not fill a {len}-byte file"),
        ));
    }

    let mut region = vec![0u8; index_len as usize];
    file.seek(SeekFrom::Start(index_offset))
        .map_err(|e| io_error(path, e))?;
    file.read_exact(&mut region)
        .map_err(|e| io_error(path, e))?;
    let mut dec = Dec::new(&region);
    let mut entries: Vec<(Hash, PackEntry)> = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let malformed = || corrupt(path, "index entry is malformed");
        let hash = dec.hash().map_err(|_| malformed())?;
        let offset = dec.u64().map_err(|_| malformed())?;
        let stored_len = dec.u64().map_err(|_| malformed())?;
        let raw_len = dec.u64().map_err(|_| malformed())?;
        let encoding = Encoding::from_wire(dec.u8().map_err(|_| malformed())?)
            .ok_or_else(|| corrupt(path, &format!("object {hash} has an unknown encoding")))?;
        // Every entry addresses bytes inside the data region, so a read
        // never reaches into the index or past the file.
        let end = offset.checked_add(stored_len);
        if offset < HEADER_LEN || end.is_none_or(|end| end > index_offset) {
            return Err(corrupt(
                path,
                &format!("object {hash} is stored outside the data region"),
            ));
        }
        // Ascending order is what makes the index searchable and the file a
        // pure function of its object set.
        if let Some((previous, _)) = entries.last()
            && *previous >= hash
        {
            return Err(corrupt(
                path,
                &format!("index is not ascending at object {hash}"),
            ));
        }
        entries.push((
            hash,
            PackEntry {
                offset,
                stored_len,
                raw_len,
                encoding,
            },
        ));
    }
    Ok(entries)
}

/// Reads one object out of a pack, decoded and verified: the decoded length
/// must match the entry's `raw_len` and the bytes must hash to `hash`.
pub(crate) fn read_entry(path: &Path, hash: &Hash, entry: &PackEntry) -> Result<Vec<u8>> {
    let mut file = File::open(path).map_err(|e| io_error(path, e))?;
    file.seek(SeekFrom::Start(entry.offset))
        .map_err(|e| io_error(path, e))?;
    let mut stored = vec![0u8; size(entry.stored_len, path)?];
    file.read_exact(&mut stored)
        .map_err(|e| io_error(path, e))?;

    let raw_len = size(entry.raw_len, path)?;
    let raw = match entry.encoding {
        Encoding::Raw => stored,
        // The declared length caps the output, so a frame claiming to
        // expand beyond what the index promised fails here rather than
        // allocating what it asked for.
        Encoding::Zstd => zstd::bulk::decompress(&stored, raw_len)
            .map_err(|e| corrupt(path, &format!("object {hash} does not decompress: {e}")))?,
    };
    if raw.len() != raw_len {
        return Err(corrupt(
            path,
            &format!(
                "object {hash} decodes to {} bytes, not the {raw_len} the index states",
                raw.len()
            ),
        ));
    }
    // The verified read, as it is performed on a loose object: identity is
    // the digest of these bytes, so packing cannot silently alter content.
    let actual = hash_bytes(&raw);
    if actual != *hash {
        return Err(corrupt(
            path,
            &format!("object {hash} decodes to bytes hashing to {actual}"),
        ));
    }
    Ok(raw)
}

/// Splits objects into the groups each pack will hold, greedily under `cap`
/// raw bytes. A pack closes when the next object would cross the cap and it
/// already holds one, so a single object above the cap gets a pack of its
/// own.
pub(crate) fn split_by_cap(sizes: &[(Hash, u64)], cap: u64) -> Vec<&[(Hash, u64)]> {
    let mut groups = Vec::new();
    let mut start = 0;
    let mut held: u64 = 0;
    for (index, (_, raw_len)) in sizes.iter().enumerate() {
        if index > start && held.saturating_add(*raw_len) > cap {
            groups.push(&sizes[start..index]);
            start = index;
            held = 0;
        }
        held = held.saturating_add(*raw_len);
    }
    if start < sizes.len() {
        groups.push(&sizes[start..]);
    }
    groups
}

/// The pack file under construction: a buffered writer that hashes every
/// byte on its way to disk, so the pack's name — the digest of the whole
/// file — is known the moment the last byte lands, and counts the bytes
/// written, which is the offset the next one goes to.
struct PackWriter {
    path: PathBuf,
    file: BufWriter<File>,
    hasher: Hasher,
    offset: u64,
}

impl PackWriter {
    /// Creates the file at `path` and starts an empty hash state over it.
    fn create(path: PathBuf) -> Result<PackWriter> {
        let file = File::create(&path).map_err(|e| io_error(&path, e))?;
        Ok(PackWriter {
            path,
            file: BufWriter::new(file),
            hasher: Hasher::new(),
            offset: 0,
        })
    }

    /// The file being written, for error context.
    fn path(&self) -> &Path {
        &self.path
    }

    /// The offset the next write lands at.
    fn offset(&self) -> u64 {
        self.offset
    }

    /// Appends `bytes` to the file and to the digest.
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.file
            .write_all(bytes)
            .map_err(|e| io_error(&self.path, e))?;
        self.hasher.update(bytes);
        self.offset += bytes.len() as u64;
        Ok(())
    }

    /// Flushes and fsyncs the file, returning its path and its name.
    fn finish(self) -> Result<(PathBuf, Hash)> {
        let file = self
            .file
            .into_inner()
            .map_err(|e| io_error(&self.path, e.into_error()))?;
        file.sync_all().map_err(|e| io_error(&self.path, e))?;
        Ok((self.path, self.hasher.finish()))
    }
}

/// One index entry's 57 bytes: hash, offset, stored length, raw length,
/// encoding.
fn entry_bytes(hash: &Hash, entry: &PackEntry) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.hash(hash)
        .u64(entry.offset)
        .u64(entry.stored_len)
        .u64(entry.raw_len)
        .u8(entry.encoding.wire());
    enc.finish()
}

/// The footer's 24 bytes: where the index starts, how many entries it
/// holds, and the magic that closes the file.
fn footer_bytes(index_offset: u64, count: u64) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.u64(index_offset).u64(count);
    let mut bytes = enc.finish();
    bytes.extend_from_slice(MAGIC);
    bytes
}

/// A pack that contradicts the format, named by its file.
fn corrupt(path: &Path, what: &str) -> Error {
    Error::Corruption(format!("pack {}: {what}", path.display()))
}

/// A length from the index as an address-space size. A pack written on a
/// wider machine can carry a length this one cannot address.
fn size(len: u64, path: &Path) -> Result<usize> {
    usize::try_from(len).map_err(|_| corrupt(path, &format!("length {len} exceeds address space")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::temp_store;
    use sima_core::hash_bytes;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;

    /// Bytes zstd shrinks: one byte repeated.
    fn compressible() -> Vec<u8> {
        vec![b'z'; 4096]
    }

    /// Bytes zstd cannot shrink: a blake3 chain, which is high-entropy by
    /// construction.
    fn incompressible(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len + Hash::LEN);
        let mut block = hash_bytes(b"pack entropy");
        while out.len() < len {
            out.extend_from_slice(block.as_bytes());
            block = hash_bytes(block.as_bytes());
        }
        out.truncate(len);
        out
    }

    /// An object set keyed by address, and a source closure over it.
    fn object_set(payloads: Vec<Vec<u8>>) -> BTreeMap<Hash, Vec<u8>> {
        payloads
            .into_iter()
            .map(|bytes| (hash_bytes(&bytes), bytes))
            .collect()
    }

    /// The source [`write_pack`] reads objects through.
    fn source(objects: &BTreeMap<Hash, Vec<u8>>) -> impl Fn(&Hash) -> Result<Vec<u8>> + '_ {
        move |hash| {
            objects
                .get(hash)
                .cloned()
                .ok_or(Error::MissingObject(*hash))
        }
    }

    /// Writes a pack of `objects` into a fresh store and returns the store
    /// directory guard, the pack's path, and its name.
    fn packed(objects: &BTreeMap<Hash, Vec<u8>>) -> (tempfile::TempDir, PathBuf, Hash) {
        let (dir, store) = temp_store();
        let hashes: Vec<Hash> = objects.keys().copied().collect();
        let name = write_pack(store.root(), &hashes, &source(objects)).expect("write pack");
        let path = dir.path().join("packs").join(format!("{name}.pack"));
        (dir, path, name)
    }

    /// The three payloads every round-trip test packs: compressible,
    /// incompressible, and empty.
    fn mixed_payloads() -> Vec<Vec<u8>> {
        vec![compressible(), incompressible(2048), Vec::new()]
    }

    /// Writes `bytes` as a pack file of its own and loads its index, for the
    /// tampering cases.
    fn load_tampered(dir: &Path, bytes: &[u8]) -> Result<Vec<(Hash, PackEntry)>> {
        let path = dir.join("tampered.pack");
        fs::write(&path, bytes).expect("write tampered pack");
        load_index(&path)
    }

    /// The index region's file offset, read from a pack's footer.
    fn index_offset(bytes: &[u8]) -> usize {
        let at = bytes.len() - FOOTER_LEN as usize;
        u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes")) as usize
    }

    #[test]
    fn a_pack_round_trips_every_object_it_holds() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (_dir, path, _name) = packed(&objects);
        let index = load_index(&path)?;
        assert_eq!(index.len(), objects.len());
        for (hash, entry) in &index {
            assert_eq!(&read_entry(&path, hash, entry)?, &objects[hash]);
        }
        Ok(())
    }

    #[test]
    fn the_pack_is_named_by_the_digest_of_its_own_bytes() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (_dir, path, name) = packed(&objects);
        let bytes = fs::read(&path).expect("read pack");
        assert_eq!(name, hash_bytes(&bytes));
        Ok(())
    }

    #[test]
    fn the_header_and_footer_sit_at_their_pinned_offsets() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (_dir, path, _name) = packed(&objects);
        let bytes = fs::read(&path).expect("read pack");
        // The layout is a fixed contract: magic, version, and the closing
        // magic are pinned by offset.
        assert_eq!(&bytes[..8], MAGIC);
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes")),
            VERSION
        );
        assert_eq!(&bytes[bytes.len() - 8..], MAGIC);
        let footer = bytes.len() - FOOTER_LEN as usize;
        let count = u64::from_le_bytes(bytes[footer + 8..footer + 16].try_into().expect("8 bytes"));
        assert_eq!(count, objects.len() as u64);
        assert_eq!(
            index_offset(&bytes) as u64 + count * ENTRY_LEN + FOOTER_LEN,
            bytes.len() as u64
        );
        Ok(())
    }

    #[test]
    fn one_object_set_writes_byte_identical_packs() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (_first_dir, first, first_name) = packed(&objects);
        // A second store, and the hashes offered in the reverse order: the
        // writer sorts, so the bytes are a pure function of the object set.
        let (second_dir, store) = temp_store();
        let mut reversed: Vec<Hash> = objects.keys().copied().collect();
        reversed.reverse();
        let second_name = write_pack(store.root(), &reversed, &source(&objects))?;
        assert_eq!(second_name, first_name);
        let second = second_dir
            .path()
            .join("packs")
            .join(format!("{second_name}.pack"));
        assert_eq!(
            fs::read(&first).expect("read first pack"),
            fs::read(&second).expect("read second pack")
        );
        Ok(())
    }

    #[test]
    fn compression_shrinks_what_it_can_and_stores_the_rest_raw() -> Result<()> {
        let payloads = mixed_payloads();
        let objects = object_set(payloads.clone());
        let (_dir, path, _name) = packed(&objects);
        let index: BTreeMap<Hash, PackEntry> = load_index(&path)?.into_iter().collect();

        let shrunk = index[&hash_bytes(&payloads[0])];
        assert_eq!(shrunk.encoding, Encoding::Zstd);
        assert!(
            shrunk.stored_len < shrunk.raw_len,
            "a zstd entry stores fewer bytes than it holds: {shrunk:?}"
        );
        // High-entropy bytes and the empty object both grow under
        // compression, so both are stored as they are.
        let kept = index[&hash_bytes(&payloads[1])];
        assert_eq!(kept.encoding, Encoding::Raw);
        assert_eq!(kept.stored_len, kept.raw_len);
        let empty = index[&hash_bytes(&payloads[2])];
        assert_eq!(empty.encoding, Encoding::Raw);
        assert_eq!(empty.stored_len, 0);
        Ok(())
    }

    #[test]
    fn the_cap_closes_a_pack_before_it_is_crossed() {
        let sizes: Vec<(Hash, u64)> = (0u8..5).map(|i| (hash_bytes(&[i]), 40)).collect();
        // A cap of 100 raw bytes holds two 40-byte objects; the third would
        // cross it, so it opens the next pack.
        let groups = split_by_cap(&sizes, 100);
        assert_eq!(
            groups.iter().map(|group| group.len()).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
    }

    #[test]
    fn an_object_above_the_cap_gets_a_pack_of_its_own() {
        let sizes = vec![
            (hash_bytes(&[0]), 10),
            (hash_bytes(&[1]), 500),
            (hash_bytes(&[2]), 10),
        ];
        // The oversized object cannot share a pack with anything: it closes
        // the pack before it and opens the pack after it.
        let groups = split_by_cap(&sizes, 100);
        assert_eq!(
            groups.iter().map(|group| group.len()).collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
    }

    #[test]
    fn an_empty_object_set_is_refused() {
        let (_dir, store) = temp_store();
        let objects = BTreeMap::new();
        assert!(matches!(
            write_pack(store.root(), &[], &source(&objects)),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn a_wrong_header_magic_is_corruption() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (dir, path, _name) = packed(&objects);
        let mut bytes = fs::read(&path).expect("read pack");
        bytes[0] = b'x';
        assert!(matches!(
            load_tampered(dir.path(), &bytes),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn a_wrong_version_is_corruption() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (dir, path, _name) = packed(&objects);
        let mut bytes = fs::read(&path).expect("read pack");
        bytes[8..12].copy_from_slice(&2u32.to_le_bytes());
        assert!(matches!(
            load_tampered(dir.path(), &bytes),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn a_wrong_footer_magic_is_corruption() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (dir, path, _name) = packed(&objects);
        let mut bytes = fs::read(&path).expect("read pack");
        let last = bytes.len() - 1;
        bytes[last] = b'x';
        assert!(matches!(
            load_tampered(dir.path(), &bytes),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn a_file_too_short_to_hold_a_footer_is_corruption() -> Result<()> {
        let (dir, _store) = temp_store();
        assert!(matches!(
            load_tampered(dir.path(), MAGIC),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn an_index_length_disagreeing_with_the_file_is_corruption() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (dir, path, _name) = packed(&objects);
        let mut bytes = fs::read(&path).expect("read pack");
        // One entry more than the file can hold.
        let count_at = bytes.len() - FOOTER_LEN as usize + 8;
        let count = u64::from_le_bytes(bytes[count_at..count_at + 8].try_into().expect("8 bytes"));
        bytes[count_at..count_at + 8].copy_from_slice(&(count + 1).to_le_bytes());
        assert!(matches!(
            load_tampered(dir.path(), &bytes),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn an_entry_reaching_past_the_data_region_is_corruption() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (dir, path, _name) = packed(&objects);
        let mut bytes = fs::read(&path).expect("read pack");
        // The first entry's stored length, stretched past the index.
        let at = index_offset(&bytes) + Hash::LEN + 8;
        bytes[at..at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            load_tampered(dir.path(), &bytes),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn an_index_out_of_hash_order_is_corruption() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (dir, path, _name) = packed(&objects);
        let mut bytes = fs::read(&path).expect("read pack");
        // Swap the first two entries, so the index descends where it must
        // ascend.
        let at = index_offset(&bytes);
        let entry = ENTRY_LEN as usize;
        let first: Vec<u8> = bytes[at..at + entry].to_vec();
        let second: Vec<u8> = bytes[at + entry..at + 2 * entry].to_vec();
        bytes[at..at + entry].copy_from_slice(&second);
        bytes[at + entry..at + 2 * entry].copy_from_slice(&first);
        assert!(matches!(
            load_tampered(dir.path(), &bytes),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn tampered_stored_bytes_fail_the_read_as_corruption() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (dir, path, _name) = packed(&objects);
        let mut bytes = fs::read(&path).expect("read pack");
        let index = load_index(&path)?;
        // Flip a byte inside the entry that holds the most data, so the
        // decode either fails or produces bytes hashing elsewhere.
        let (hash, entry) = index
            .iter()
            .max_by_key(|(_, entry)| entry.stored_len)
            .expect("a non-empty pack");
        bytes[entry.offset as usize] ^= 0xff;
        let tampered = dir.path().join("tampered.pack");
        fs::write(&tampered, &bytes).expect("write tampered pack");
        assert!(matches!(
            read_entry(&tampered, hash, entry),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn a_raw_length_disagreeing_with_the_decoded_object_is_corruption() -> Result<()> {
        let objects = object_set(mixed_payloads());
        let (_dir, path, _name) = packed(&objects);
        let index = load_index(&path)?;
        let (hash, entry) = index.first().expect("a non-empty pack");
        // The stored bytes decode to their own length, whatever the index
        // claims, so a wrong claim is caught before the object is returned.
        let lying = PackEntry {
            raw_len: entry.raw_len + 1,
            ..*entry
        };
        assert!(matches!(
            read_entry(&path, hash, &lying),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }
}
