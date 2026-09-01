//! Manual CAS cost measurements at the search workload shape: 1000 objects
//! of 128 KiB and 100 of 2 MiB.
//!
//! Two `#[ignore]` benchmarks that print timings and always pass, search on the
//! dev machine with:
//!
//! ```text
//! cargo test -p sima-store --test measurement -- --ignored --nocapture
//! ```
//!
//! They quantify two store costs: whether `put`'s per-object fsync needs
//! group-commit batching, and whether `get`'s verified read (a re-hash of
//! every object) needs a bulk unverified path for large artifacts. The
//! measured numbers and the decision gate they feed are recorded in the
//! roadmap, not here.

use std::time::Instant;

use sima_core::{hash_bytes, prng};
use sima_store::Store;

/// One binary kibibyte.
const KIB: usize = 1024;
/// One binary mebibyte.
const MIB: usize = 1024 * KIB;

/// The two object sizes measured: the search's snapshot size, and a larger
/// artifact for the size-scaling signal. `(label, count, size)`.
const SHAPES: [(&str, usize, usize); 2] = [("128 KiB", 1000, 128 * KIB), ("2 MiB", 100, 2 * MIB)];

/// Fills a `size`-byte payload with counter-based PRNG words seeded by `seed`,
/// so every object is distinct and generation stays cheap — no `rand`, no I/O.
fn payload(seed: u64, size: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(size);
    let mut counter = 0u64;
    while bytes.len() < size {
        let word = prng::next(seed, counter).to_le_bytes();
        let take = word.len().min(size - bytes.len());
        bytes.extend_from_slice(&word[..take]);
        counter += 1;
    }
    bytes
}

/// The CAS object path for `hash`, per the fixed store layout:
/// `objects/<aa>/<64-hex>`.
fn object_path(root: &std::path::Path, hash: &sima_core::Hash) -> std::path::PathBuf {
    let hex = hash.to_string();
    root.join("objects").join(&hex[..2]).join(hex)
}

#[test]
#[ignore = "measurement, search manually on the dev machine"]
fn write_cost() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path()).expect("open store");
    for (label, count, size) in SHAPES {
        let payloads: Vec<Vec<u8>> = (0..count as u64).map(|i| payload(i, size)).collect();

        let start = Instant::now();
        let hashes: Vec<_> = payloads
            .iter()
            .map(|p| store.put(p).expect("put"))
            .collect();
        let elapsed = start.elapsed();

        // The harness exercises the real put path — temp file, fsync, rename —
        // and every put returned the object's content address.
        assert_eq!(hashes.len(), count);
        for (p, h) in payloads.iter().zip(&hashes) {
            assert_eq!(*h, hash_bytes(p));
        }

        let megabytes = (count * size) as f64 / MIB as f64;
        let secs = elapsed.as_secs_f64();
        println!(
            "write {label}: {count} objects in {elapsed:?} — {:.0} objects/s, {:.1} MB/s",
            count as f64 / secs,
            megabytes / secs,
        );
    }
}

#[test]
#[ignore = "measurement, search manually on the dev machine"]
fn read_cost() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path()).expect("open store");
    for (label, count, size) in SHAPES {
        let hashes: Vec<_> = (0..count as u64)
            .map(|i| store.put(&payload(i, size)).expect("put"))
            .collect();

        // Verified reads: `get` re-hashes every object.
        let start = Instant::now();
        let verified_bytes: usize = hashes
            .iter()
            .map(|h| store.get(h).expect("get").len())
            .sum();
        let verified = start.elapsed();

        // The raw baseline: `fs::read` of the same files, skipping verification,
        // so the re-hash delta is explicit.
        let start = Instant::now();
        let raw_bytes: usize = hashes
            .iter()
            .map(|h| {
                std::fs::read(object_path(dir.path(), h))
                    .expect("raw read")
                    .len()
            })
            .sum();
        let raw = start.elapsed();

        // The harness exercises the real get path — read then re-hash — and the
        // bytes match what was stored.
        assert_eq!(verified_bytes, count * size);
        assert_eq!(raw_bytes, count * size);
        assert_eq!(store.get(&hashes[0]).expect("get"), payload(0, size));

        let megabytes = (count * size) as f64 / MIB as f64;
        println!(
            "read {label}: verified {verified:?} — {:.1} MB/s | raw {raw:?} — {:.1} MB/s",
            megabytes / verified.as_secs_f64(),
            megabytes / raw.as_secs_f64(),
        );
    }
}
