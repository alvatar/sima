//! Candidate spec: opaque candidate bytes plus the format id governing
//! their interpretation.

use sima_core::{Codec, Dec, Enc, Result, hash_bytes};

use crate::canonical::{self, TAG_SPEC};

/// Name of the format a domain registers to interpret specs — and the search
/// params paired with them. Validated by the shared name rule: 1..=64
/// bytes in `[a-z0-9._-]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatId(String);

impl FormatId {
    /// Validates `name` against the name rule and wraps it.
    pub fn new(name: impl Into<String>) -> Result<FormatId> {
        let name = name.into();
        canonical::validate_name(&name)?;
        Ok(FormatId(name))
    }

    /// The format name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A candidate: opaque bytes interpreted by the domain registered under
/// `format`. The bytes carry the candidate exclusively — the genome,
/// program, or whatever the domain evolves; search parameters travel in the
/// separate [`crate::Params`] blob, whose interpretation this spec's format
/// id also governs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    /// Format governing the interpretation of the candidate bytes and of
    /// the params paired with this spec.
    pub format: FormatId,
    /// The candidate itself. Opaque to the infrastructure; empty is legal.
    pub bytes: Vec<u8>,
}

impl Spec {
    /// Appends the tagged canonical form: tag, format str, candidate bytes.
    pub fn encode(&self, enc: &mut Enc) {
        enc.str(TAG_SPEC)
            .str(self.format.as_str())
            .bytes(&self.bytes);
    }

    /// Reads and validates a canonical form written by [`Spec::encode`].
    pub fn decode(dec: &mut Dec<'_>) -> Result<Spec> {
        canonical::expect_tag(dec, TAG_SPEC)?;
        let format = FormatId::new(dec.str()?)?;
        let bytes = dec.bytes()?.to_vec();
        Ok(Spec { format, bytes })
    }

    /// The spec's content id: the blake3 digest of its standalone bytes.
    pub fn id(&self) -> SpecId {
        SpecId::from_hash(hash_bytes(&self.to_bytes()))
    }
}

canonical::standalone_codec!(Spec);

canonical::id_newtype! {
    /// Content id of a [`Spec`]: the digest of its standalone canonical
    /// bytes, doubling as its store object address.
    SpecId
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::TAG_PARAMS;
    use crate::testutil::{from_hex, to_hex};
    use sima_core::{Enc, Error, Result};

    fn sample_spec() -> Result<Spec> {
        Ok(Spec {
            format: FormatId::new("stub.v1")?,
            bytes: vec![0xAA, 0xBB],
        })
    }

    /// Hand-derived canonical bytes of `sample_spec`, field by field in
    /// encoding order per the `sima-core` encode format:
    ///   str tag    "sima.spec.v1" -> u64 len 12 LE ‖ UTF-8 bytes
    ///   str format "stub.v1"      -> u64 len 7 LE  ‖ UTF-8 bytes
    ///   bytes      [aa, bb]       -> u64 len 2 LE  ‖ payload
    const PINNED_HEX: &str = "0c0000000000000073696d612e737065632e7631\
                              0700000000000000737475622e7631\
                              0200000000000000aabb";

    /// blake3 of the `PINNED_HEX` bytes, computed independently with Python
    /// blake3 (pip package `blake3`):
    /// `blake3.blake3(bytes.fromhex(PINNED_HEX)).hexdigest()`.
    const PINNED_ID_HEX: &str = "d68a9bbc155bf73715576c680a36d77c550425cc5a0d32369e70bec98f481f2b";

    fn pinned_bytes() -> Vec<u8> {
        let hex: String = PINNED_HEX.split_whitespace().collect();
        from_hex(&hex)
    }

    #[test]
    fn format_id_accepts_names_in_the_rule() -> Result<()> {
        for name in ["a", "stub.v1", "state-final"] {
            assert_eq!(FormatId::new(name)?.as_str(), name);
        }
        Ok(())
    }

    #[test]
    fn format_id_rejects_names_outside_the_rule() {
        for name in ["", "Stub.v1", "has space", "a/b"] {
            assert!(matches!(FormatId::new(name), Err(Error::Validation(_))));
        }
    }

    #[test]
    fn encoding_matches_the_hand_derived_layout() -> Result<()> {
        let expected: String = PINNED_HEX.split_whitespace().collect();
        assert_eq!(to_hex(&sample_spec()?.to_bytes()), expected);
        Ok(())
    }

    #[test]
    fn id_matches_the_independently_computed_digest() -> Result<()> {
        assert_eq!(sample_spec()?.id(), SpecId::from_hex(PINNED_ID_HEX)?);
        Ok(())
    }

    #[test]
    fn to_bytes_from_bytes_round_trips() -> Result<()> {
        let spec = sample_spec()?;
        assert_eq!(Spec::from_bytes(&spec.to_bytes())?, spec);
        let empty = Spec {
            format: FormatId::new("stub.v1")?,
            bytes: Vec::new(),
        };
        assert_eq!(Spec::from_bytes(&empty.to_bytes())?, empty);
        Ok(())
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = pinned_bytes();
        for cut in 0..full.len() {
            assert!(
                matches!(Spec::from_bytes(&full[..cut]), Err(Error::Encoding(_))),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut buf = pinned_bytes();
        buf.push(0);
        assert!(matches!(Spec::from_bytes(&buf), Err(Error::Encoding(_))));
    }

    #[test]
    fn decode_rejects_a_wrong_domain_tag() {
        // A params-tagged buffer routed to the spec decoder must fail on the
        // tag, before any payload is interpreted.
        let mut enc = Enc::new();
        enc.str(TAG_PARAMS).str("stub.v1").bytes(&[0xAA]);
        assert!(matches!(
            Spec::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn decode_rejects_an_invalid_format_name() {
        let mut enc = Enc::new();
        enc.str(crate::canonical::TAG_SPEC)
            .str("Bad Name")
            .bytes(&[]);
        assert!(matches!(
            Spec::from_bytes(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn empty_and_one_byte_candidates_have_distinct_ids() -> Result<()> {
        let format = FormatId::new("stub.v1")?;
        let empty = Spec {
            format: format.clone(),
            bytes: Vec::new(),
        };
        let one = Spec {
            format,
            bytes: vec![0x00],
        };
        assert_ne!(empty.id(), one.id());
        Ok(())
    }

    #[test]
    fn same_candidate_bytes_under_two_formats_have_distinct_ids() -> Result<()> {
        let a = Spec {
            format: FormatId::new("domain-a.v1")?,
            bytes: vec![0xAA, 0xBB],
        };
        let b = Spec {
            format: FormatId::new("domain-b.v1")?,
            bytes: vec![0xAA, 0xBB],
        };
        assert_ne!(a.id(), b.id());
        Ok(())
    }
}
