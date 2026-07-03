//! Task identity: the exact inputs that determine a task's results, and
//! the task key derived from them.

use sima_core::{Dec, Enc, Hash, Result, hash_bytes};

use crate::canon::{self, TAG_TASK};
use crate::env::EnvId;
use crate::params::ParamsId;
use crate::spec::SpecId;

/// The identity-bearing inputs of a task: spec ‖ params ‖ seed ‖ env ‖
/// input-state-ref. Two tasks with equal identities are the same task —
/// the store indexes results by the key derived from this preimage.
/// Execution context (worker, attempt, timing) never enters; it is journal
/// material in higher layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskIdentity {
    /// The candidate under evaluation.
    pub spec: SpecId,
    /// The run parameters the evaluation runs under.
    pub params: ParamsId,
    /// The task's deterministic seed.
    pub seed: u64,
    /// The environment the results depend on.
    pub env: EnvId,
    /// For segments: the digest of the opaque, family-serialized state
    /// object this task continues from; `None` for stateless tasks. The
    /// state object is store-addressed and never a model type, so the
    /// reference is a plain digest.
    pub input_state: Option<Hash>,
}

impl TaskIdentity {
    /// Appends the tagged canonical form: tag, spec digest, params digest,
    /// seed u64, env digest, optional input-state digest.
    pub fn encode(&self, enc: &mut Enc) {
        enc.str(TAG_TASK)
            .hash(self.spec.as_hash())
            .hash(self.params.as_hash())
            .u64(self.seed)
            .hash(self.env.as_hash())
            .opt_hash(self.input_state.as_ref());
    }

    /// Reads a canonical form written by [`TaskIdentity::encode`].
    pub fn decode(dec: &mut Dec<'_>) -> Result<TaskIdentity> {
        canon::expect_tag(dec, TAG_TASK)?;
        Ok(TaskIdentity {
            spec: SpecId::from_hash(dec.hash()?),
            params: ParamsId::from_hash(dec.hash()?),
            seed: dec.u64()?,
            env: EnvId::from_hash(dec.hash()?),
            input_state: dec.opt_hash()?,
        })
    }

    /// The task key: the blake3 digest of the identity's standalone bytes,
    /// under which the store indexes the task's results.
    pub fn key(&self) -> TaskKey {
        TaskKey::from_hash(hash_bytes(&self.to_bytes()))
    }
}

canon::standalone_codec!(TaskIdentity);

canon::id_newtype! {
    /// Key of a task: the digest of its [`TaskIdentity`] bytes. Manifests
    /// sort by it, and the store indexes committed results under it.
    TaskKey
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canon::TAG_SPEC;
    use crate::env::EnvId;
    use crate::params::ParamsId;
    use crate::spec::SpecId;
    use sima_core::{Enc, Error, Hash, Result};

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn from_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("pinned hex is valid"))
            .collect()
    }

    fn fill_hash(fill: &str) -> Result<Hash> {
        Hash::from_hex(&fill.repeat(32))
    }

    /// The stateless-arm sample: spec 32x11, params 32x22, seed
    /// 0x0102030405060708, env 32x33, no input state.
    fn sample_identity() -> Result<TaskIdentity> {
        Ok(TaskIdentity {
            spec: SpecId::from_hash(fill_hash("11")?),
            params: ParamsId::from_hash(fill_hash("22")?),
            seed: 0x0102_0304_0506_0708,
            env: EnvId::from_hash(fill_hash("33")?),
            input_state: None,
        })
    }

    /// The segment-arm sample: `sample_identity` with input state 32x44.
    fn sample_segment_identity() -> Result<TaskIdentity> {
        Ok(TaskIdentity {
            input_state: Some(fill_hash("44")?),
            ..sample_identity()?
        })
    }

    /// Hand-derived canonical bytes of `sample_identity`, field by field in
    /// encoding order per the `sima-core` encode format:
    ///   str tag "sima.task.v1" -> u64 len 12 LE ‖ UTF-8 bytes
    ///   spec digest    -> 32 raw bytes (0x11 repeated)
    ///   params digest  -> 32 raw bytes (0x22 repeated)
    ///   seed           -> u64 LE: 0807060504030201
    ///   env digest     -> 32 raw bytes (0x33 repeated)
    ///   input state    -> Option<Hash> absent flag byte 00
    const PINNED_NONE_HEX: &str = "0c0000000000000073696d612e7461736b2e7631\
                                   1111111111111111111111111111111111111111111111111111111111111111\
                                   2222222222222222222222222222222222222222222222222222222222222222\
                                   0807060504030201\
                                   3333333333333333333333333333333333333333333333333333333333333333\
                                   00";

    /// `sample_segment_identity`: the same layout with the input-state arm
    /// present — flag byte 01 followed by 32 raw digest bytes (0x44
    /// repeated).
    const PINNED_SOME_HEX: &str = "0c0000000000000073696d612e7461736b2e7631\
                                   1111111111111111111111111111111111111111111111111111111111111111\
                                   2222222222222222222222222222222222222222222222222222222222222222\
                                   0807060504030201\
                                   3333333333333333333333333333333333333333333333333333333333333333\
                                   014444444444444444444444444444444444444444444444444444444444444444";

    /// blake3 of the `PINNED_NONE_HEX` bytes, computed independently with
    /// Python blake3 (pip package `blake3`):
    /// `blake3.blake3(bytes.fromhex(PINNED_NONE_HEX)).hexdigest()`.
    const PINNED_NONE_KEY_HEX: &str =
        "86797525d22e2f02300eaaa444c54ade3243f383382223e322201dcc8bf24deb";

    /// blake3 of the `PINNED_SOME_HEX` bytes, same tool and derivation.
    const PINNED_SOME_KEY_HEX: &str =
        "85488efdf18beac9943b3d3d15086267aa5127be2d34d25cf4421cc0860371e8";

    fn pinned_none() -> String {
        PINNED_NONE_HEX.split_whitespace().collect()
    }

    fn pinned_some() -> String {
        PINNED_SOME_HEX.split_whitespace().collect()
    }

    #[test]
    fn encoding_matches_the_hand_derived_layouts() -> Result<()> {
        assert_eq!(to_hex(&sample_identity()?.to_bytes()), pinned_none());
        assert_eq!(
            to_hex(&sample_segment_identity()?.to_bytes()),
            pinned_some()
        );
        Ok(())
    }

    #[test]
    fn keys_match_the_independently_computed_digests() -> Result<()> {
        assert_eq!(
            sample_identity()?.key(),
            TaskKey::from_hex(PINNED_NONE_KEY_HEX)?
        );
        assert_eq!(
            sample_segment_identity()?.key(),
            TaskKey::from_hex(PINNED_SOME_KEY_HEX)?
        );
        Ok(())
    }

    #[test]
    fn to_bytes_from_bytes_round_trips_both_arms() -> Result<()> {
        for identity in [sample_identity()?, sample_segment_identity()?] {
            assert_eq!(TaskIdentity::from_bytes(&identity.to_bytes())?, identity);
        }
        Ok(())
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = from_hex(&pinned_some());
        for cut in 0..full.len() {
            assert!(
                matches!(
                    TaskIdentity::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut buf = from_hex(&pinned_none());
        buf.push(0);
        assert!(matches!(
            TaskIdentity::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn decode_rejects_a_wrong_domain_tag() {
        let mut enc = Enc::new();
        enc.str(TAG_SPEC);
        assert!(matches!(
            TaskIdentity::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn varying_any_single_slot_changes_the_key() -> Result<()> {
        let base = sample_identity()?;
        // One variant per key slot, plus the two input-state arms: `None`
        // vs `Some(a)` and `Some(a)` vs `Some(b)`.
        let variants = [
            base,
            TaskIdentity {
                spec: SpecId::from_hash(fill_hash("aa")?),
                ..base
            },
            TaskIdentity {
                params: ParamsId::from_hash(fill_hash("aa")?),
                ..base
            },
            TaskIdentity {
                seed: base.seed + 1,
                ..base
            },
            TaskIdentity {
                env: EnvId::from_hash(fill_hash("aa")?),
                ..base
            },
            TaskIdentity {
                input_state: Some(fill_hash("aa")?),
                ..base
            },
            TaskIdentity {
                input_state: Some(fill_hash("bb")?),
                ..base
            },
        ];
        let keys: Vec<TaskKey> = variants.iter().map(TaskIdentity::key).collect();
        for (i, a) in keys.iter().enumerate() {
            for (j, b) in keys.iter().enumerate().skip(i + 1) {
                assert_ne!(a, b, "variants {i} and {j} must have distinct keys");
            }
        }
        Ok(())
    }

    #[test]
    fn identity_and_spec_encodings_hash_differently() -> Result<()> {
        // Domain tags keep the identity worlds disjoint: a spec whose
        // candidate bytes are an identity's entire payload still hashes
        // into a different id space.
        let identity = sample_identity()?;
        let spec = crate::spec::Spec {
            format: crate::spec::FormatId::new("stub.v1")?,
            bytes: identity.to_bytes(),
        };
        assert_ne!(identity.key().as_hash(), spec.id().as_hash());
        Ok(())
    }

    #[test]
    fn task_key_display_and_from_hex_round_trip() -> Result<()> {
        let key = sample_identity()?.key();
        assert_eq!(TaskKey::from_hex(&key.to_string())?, key);
        Ok(())
    }

    #[test]
    fn task_keys_sort_deterministically_in_digest_byte_order() -> Result<()> {
        // The manifest order M1.3 uses: sorting keys must agree with the
        // lexicographic order of their digest bytes — equivalently their
        // lowercase-hex forms, which fixed-width hex preserves.
        let mut keys: Vec<TaskKey> = [
            sample_segment_identity()?.key(),
            TaskIdentity {
                seed: 7,
                ..sample_identity()?
            }
            .key(),
            sample_identity()?.key(),
            TaskIdentity {
                seed: 9,
                ..sample_identity()?
            }
            .key(),
        ]
        .to_vec();
        let mut resorted = keys.clone();
        resorted.reverse();
        keys.sort();
        resorted.sort();
        assert_eq!(keys, resorted);
        let hexes: Vec<String> = keys.iter().map(TaskKey::to_string).collect();
        let mut by_hex = hexes.clone();
        by_hex.sort();
        assert_eq!(hexes, by_hex);
        Ok(())
    }
}
