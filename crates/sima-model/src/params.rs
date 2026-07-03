//! Run parameters: the opaque evaluation-axis blob paired with a spec.

use sima_core::{Dec, Enc, Result, hash_bytes};

use crate::canon::{self, TAG_PARAMS};

/// Run parameters for evaluating a candidate: extent, step count, budgets —
/// whatever the family's evaluation needs beyond the candidate itself.
/// Opaque to the infrastructure and interpreted under the paired spec's
/// format id; params carries no format id of its own. Config is the
/// producer: generators produce specs (the search axis), config produces
/// params (the evaluation axis). A mandatory task-key slot with
/// possibly-empty bytes — one spelling, no `Option`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Params {
    /// The parameter blob. Opaque to the infrastructure; empty is legal.
    pub bytes: Vec<u8>,
}

impl Params {
    /// Wraps a parameter blob.
    pub fn new(bytes: Vec<u8>) -> Params {
        Params { bytes }
    }

    /// Appends the tagged canonical form: tag, parameter bytes.
    pub fn encode(&self, enc: &mut Enc) {
        enc.str(TAG_PARAMS).bytes(&self.bytes);
    }

    /// Reads and validates a canonical form written by [`Params::encode`].
    pub fn decode(dec: &mut Dec<'_>) -> Result<Params> {
        canon::expect_tag(dec, TAG_PARAMS)?;
        Ok(Params {
            bytes: dec.bytes()?.to_vec(),
        })
    }

    /// The params' content id: the blake3 digest of its standalone bytes.
    pub fn id(&self) -> ParamsId {
        ParamsId::from_hash(hash_bytes(&self.to_bytes()))
    }
}

canon::standalone_codec!(Params);

canon::id_newtype! {
    /// Content id of a [`Params`] blob: the digest of its standalone
    /// canonical bytes, doubling as its store object address.
    ParamsId
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canon::TAG_SPEC;
    use crate::spec::{FormatId, Spec};
    use crate::testutil::{from_hex, to_hex};
    use sima_core::{Enc, Error, Result};

    /// Hand-derived canonical bytes of empty params, field by field in
    /// encoding order per the `sima-core` encode format:
    ///   str tag "sima.params.v1" -> u64 len 14 LE ‖ UTF-8 bytes
    ///   bytes   []               -> u64 len 0 LE
    const PINNED_EMPTY_HEX: &str = "0e0000000000000073696d612e706172616d732e76310000000000000000";

    /// Hand-derived canonical bytes of params [01, 02, 03]: the same tag,
    /// then u64 len 3 LE ‖ payload.
    const PINNED_HEX: &str = "0e0000000000000073696d612e706172616d732e76310300000000000000010203";

    /// blake3 of the `PINNED_EMPTY_HEX` bytes, computed independently with
    /// Python blake3 (pip package `blake3`):
    /// `blake3.blake3(bytes.fromhex(PINNED_EMPTY_HEX)).hexdigest()`.
    /// Pinned on its own because the empty-params id recurs wherever a
    /// family needs no run parameters.
    const PINNED_EMPTY_ID_HEX: &str =
        "f4dceb2cab41bf105e41382f26f55d3d053b6141d75509bbd16b3d24913e11c6";

    /// blake3 of the `PINNED_HEX` bytes, same tool and derivation.
    const PINNED_ID_HEX: &str = "9df4499c85574df917272715e8eadb20afa0a30e7375e9e0be98d6e66eb3fba2";

    #[test]
    fn encoding_matches_the_hand_derived_layouts() {
        assert_eq!(
            to_hex(&Params::new(Vec::new()).to_bytes()),
            PINNED_EMPTY_HEX
        );
        assert_eq!(to_hex(&Params::new(vec![1, 2, 3]).to_bytes()), PINNED_HEX);
    }

    #[test]
    fn ids_match_the_independently_computed_digests() -> Result<()> {
        assert_eq!(
            Params::new(Vec::new()).id(),
            ParamsId::from_hex(PINNED_EMPTY_ID_HEX)?
        );
        assert_eq!(
            Params::new(vec![1, 2, 3]).id(),
            ParamsId::from_hex(PINNED_ID_HEX)?
        );
        Ok(())
    }

    #[test]
    fn to_bytes_from_bytes_round_trips() -> Result<()> {
        for params in [Params::new(Vec::new()), Params::new(vec![1, 2, 3])] {
            assert_eq!(Params::from_bytes(&params.to_bytes())?, params);
        }
        Ok(())
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = from_hex(PINNED_HEX);
        for cut in 0..full.len() {
            assert!(
                matches!(Params::from_bytes(&full[..cut]), Err(Error::Encoding(_))),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut buf = from_hex(PINNED_HEX);
        buf.push(0);
        assert!(matches!(Params::from_bytes(&buf), Err(Error::Encoding(_))));
    }

    #[test]
    fn decode_rejects_a_wrong_domain_tag() {
        // A spec-tagged buffer routed to the params decoder must fail on the
        // tag, before any payload is interpreted.
        let mut enc = Enc::new();
        enc.str(TAG_SPEC).bytes(&[1, 2, 3]);
        assert!(matches!(
            Params::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn params_and_spec_over_the_same_raw_bytes_hash_differently() -> Result<()> {
        // Domain tags keep the identity worlds disjoint: the same raw blob
        // committed as params and as a spec candidate never shares an id.
        let raw = vec![0xAA, 0xBB];
        let params = Params::new(raw.clone());
        let spec = Spec {
            format: FormatId::new("stub.v1")?,
            bytes: raw,
        };
        assert_ne!(params.id().as_hash(), spec.id().as_hash());
        Ok(())
    }
}
