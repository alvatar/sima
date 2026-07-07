//! Helpers shared by the crate's test modules: hex text for comparing
//! pinned byte layouts, and fill-pattern digests for synthetic ids.

use sima_core::{Hash, Result};
pub(crate) use sima_core::to_hex;

/// Parses a pinned lowercase-hex layout back into bytes.
pub(crate) fn from_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("pinned hex is valid"))
        .collect()
}

/// A synthetic digest with every byte equal to the two-digit hex `fill`.
pub(crate) fn fill_hash(fill: &str) -> Result<Hash> {
    Hash::from_hex(&fill.repeat(Hash::LEN))
}
