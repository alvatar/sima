//! Content identity: a newtype over the 32-byte blake3 digest.
//!
//! Everything identity-bearing in sima is addressed by a [`Hash`]; raw
//! `[u8; 32]` digests never cross public API boundaries. The canonical text
//! form is lowercase hex ([`fmt::Display`]), and [`Hash::from_hex`] accepts
//! exactly that form back — one spelling per identity.

use std::fmt;

use crate::error::{Error, Result};

/// A 32-byte blake3 digest identifying a piece of content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash([u8; Hash::LEN]);

impl Hash {
    /// Digest length in bytes.
    pub const LEN: usize = 32;

    /// Wraps raw digest bytes. Crate-internal for the canonical decoder; the
    /// public ways to obtain a `Hash` are [`hash_bytes`] and [`Hash::from_hex`].
    pub(crate) const fn from_bytes(bytes: [u8; Hash::LEN]) -> Self {
        Hash(bytes)
    }

    /// Digest bytes, for the canonical encoder.
    pub(crate) const fn as_bytes(&self) -> &[u8; Hash::LEN] {
        &self.0
    }

    /// Parses the canonical lowercase-hex form. Rejects any other length,
    /// uppercase digits, and non-hex characters with [`Error::Encoding`].
    pub fn from_hex(s: &str) -> Result<Hash> {
        let hex = s.as_bytes();
        if hex.len() != 2 * Hash::LEN {
            return Err(Error::Encoding(format!(
                "hash hex must be {} characters, got {}",
                2 * Hash::LEN,
                hex.len()
            )));
        }
        let mut out = [0u8; Hash::LEN];
        for (byte, pair) in out.iter_mut().zip(hex.chunks_exact(2)) {
            *byte = (hex_val(pair[0])? << 4) | hex_val(pair[1])?;
        }
        Ok(Hash(out))
    }
}

/// Decodes one canonical (lowercase) hex digit.
fn hex_val(c: u8) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        _ => Err(Error::Encoding(format!(
            "invalid hash hex character {:?}",
            c as char
        ))),
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Hashes `data` with blake3.
pub fn hash_bytes(data: &[u8]) -> Hash {
    Hash(*blake3::hash(data).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Official blake3 test vector: empty input (test_vectors.json,
    /// input_len 0, first 32 bytes).
    const EMPTY_HEX: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
    /// Official blake3 test vector: input_len 1, i.e. the single byte 0x00.
    const BYTE0_HEX: &str = "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213";

    #[test]
    fn hash_bytes_matches_official_empty_vector() -> Result<()> {
        assert_eq!(hash_bytes(b""), Hash::from_hex(EMPTY_HEX)?);
        Ok(())
    }

    #[test]
    fn hash_bytes_matches_official_single_byte_vector() -> Result<()> {
        assert_eq!(hash_bytes(&[0u8]), Hash::from_hex(BYTE0_HEX)?);
        Ok(())
    }

    #[test]
    fn display_then_from_hex_is_identity() -> Result<()> {
        let h = hash_bytes(b"round trip");
        let hex = h.to_string();
        assert_eq!(hex.len(), 2 * Hash::LEN);
        assert_eq!(Hash::from_hex(&hex)?, h);
        Ok(())
    }

    #[test]
    fn display_is_lowercase_hex() {
        let hex = hash_bytes(b"case").to_string();
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn from_hex_rejects_wrong_length() {
        for s in [&EMPTY_HEX[..63], &format!("{EMPTY_HEX}0")[..], ""] {
            assert!(matches!(Hash::from_hex(s), Err(Error::Encoding(_))));
        }
    }

    #[test]
    fn from_hex_rejects_non_hex_and_uppercase() {
        let bad_char = format!("g{}", &EMPTY_HEX[1..]);
        let upper = EMPTY_HEX.to_uppercase();
        assert!(matches!(Hash::from_hex(&bad_char), Err(Error::Encoding(_))));
        assert!(matches!(Hash::from_hex(&upper), Err(Error::Encoding(_))));
    }
}
