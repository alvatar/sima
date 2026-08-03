//! Helpers shared by the crate's test modules: hex text for comparing
//! pinned byte layouts, and fill-pattern digests for synthetic ids.

pub(crate) use sima_core::to_hex;
use sima_core::{Hash, Result};

/// Parses a pinned lowercase-hex layout back into bytes, through the crate that
/// wrote it: a second parser here could disagree with `to_hex` about what a
/// pinned layout says.
pub(crate) fn from_hex(hex: &str) -> Vec<u8> {
    sima_core::from_hex(hex).expect("pinned hex is valid")
}

/// A synthetic digest with every byte equal to the two-digit hex `fill`.
pub(crate) fn fill_hash(fill: &str) -> Result<Hash> {
    Hash::from_hex(&fill.repeat(Hash::LEN))
}
