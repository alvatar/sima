//! Run configuration and run identity: the identity-bearing portion of a
//! run's configuration, whose canonical bytes define the run id.
//!
//! Run identity is the hash of canonicalized config. Execution knobs —
//! worker count, store path, backends — live in a separate, non-identity
//! configuration section in higher layers: a run resumed with different
//! parallelism or on different hardware keeps its run id. The environment
//! id is also absent: config records intent, and the environment travels
//! in every task key.

use sima_core::{Dec, Enc, Result, hash_bytes};

use crate::canonical::{self, TAG_RUN_CONFIG};
use crate::params::Params;
use crate::spec::FormatId;

/// Name of the generator a run draws candidates from. Validated by the
/// shared name rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratorId(String);

impl GeneratorId {
    /// Validates `name` against the name rule and wraps it.
    pub fn new(name: impl Into<String>) -> Result<GeneratorId> {
        let name = name.into();
        canonical::validate_name(&name)?;
        Ok(GeneratorId(name))
    }

    /// The generator name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The generator's own settings: which generator, and the opaque parameter
/// blob its implementation (M1.4+) defines the encoding of. Scoped under
/// the generator and distinct from the run-level [`Params`] that feeds
/// every task's params slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratorConfig {
    /// The generator to draw candidates from.
    pub id: GeneratorId,
    /// The generator's own settings blob. Opaque to the infrastructure;
    /// empty is legal.
    pub params: Vec<u8>,
}

/// The identity-bearing portion of a run's configuration. Its canonical
/// bytes are the run-id preimage: run identity is the hash of
/// canonicalized config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunConfig {
    /// The run's root seed, from which task seeds derive.
    pub root_seed: u64,
    /// Format governing the interpretation of generated specs and of the
    /// run params.
    pub format: FormatId,
    /// The generator and its settings — the search axis.
    pub generator: GeneratorConfig,
    /// The run params fed to every task's params slot — the evaluation
    /// axis, produced by config.
    pub params: Params,
}

impl RunConfig {
    /// Appends the tagged canonical form: tag, root_seed u64, format str,
    /// generator id str, generator params bytes, then the embedded run
    /// params encoding (its own tag included).
    pub fn encode(&self, enc: &mut Enc) {
        enc.str(TAG_RUN_CONFIG)
            .u64(self.root_seed)
            .str(self.format.as_str())
            .str(self.generator.id.as_str())
            .bytes(&self.generator.params);
        self.params.encode(enc);
    }

    /// Reads and validates a canonical form written by
    /// [`RunConfig::encode`].
    pub fn decode(dec: &mut Dec<'_>) -> Result<RunConfig> {
        canonical::expect_tag(dec, TAG_RUN_CONFIG)?;
        let root_seed = dec.u64()?;
        let format = FormatId::new(dec.str()?)?;
        let generator = GeneratorConfig {
            id: GeneratorId::new(dec.str()?)?,
            params: dec.bytes()?.to_vec(),
        };
        let params = Params::decode(dec)?;
        Ok(RunConfig {
            root_seed,
            format,
            generator,
            params,
        })
    }

    /// The run id: the blake3 digest of the config's standalone bytes.
    pub fn id(&self) -> RunId {
        RunId::from_hash(hash_bytes(&self.to_bytes()))
    }
}

canonical::standalone_codec!(RunConfig);

canonical::id_newtype! {
    /// Identity of a run: the digest of its canonicalized [`RunConfig`]
    /// bytes. Stable across resume, parallelism changes, and hardware.
    RunId
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::TAG_SPEC;
    use crate::params::Params;
    use crate::spec::FormatId;
    use crate::testutil::{from_hex, to_hex};
    use sima_core::{Enc, Error, Result};

    fn sample_config() -> Result<RunConfig> {
        Ok(RunConfig {
            root_seed: 42,
            format: FormatId::new("stub.v1")?,
            generator: GeneratorConfig {
                id: GeneratorId::new("gen.v1")?,
                params: vec![0xDE, 0xAD],
            },
            params: Params {
                bytes: vec![1, 2, 3],
            },
        })
    }

    /// Hand-derived canonical bytes of `sample_config`, field by field in
    /// encoding order per the `sima-core` encode format:
    ///   str tag "sima.run-config.v1" -> u64 len 18 LE ‖ UTF-8 bytes
    ///   root_seed        -> u64 LE: 2a00000000000000
    ///   str format       "stub.v1" -> u64 len 7 LE ‖ UTF-8 bytes
    ///   str generator id "gen.v1"  -> u64 len 6 LE ‖ UTF-8 bytes
    ///   generator params [de, ad]  -> u64 len 2 LE ‖ payload
    ///   embedded run-params encoding, its own tag included: str tag
    ///     "sima.params.v1", bytes [01, 02, 03] (u64 len 3 LE ‖ payload)
    const PINNED_HEX: &str = "120000000000000073696d612e72756e2d636f6e6669672e7631\
                              2a00000000000000\
                              0700000000000000737475622e7631\
                              060000000000000067656e2e7631\
                              0200000000000000dead\
                              0e0000000000000073696d612e706172616d732e76310300000000000000010203";

    /// blake3 of the `PINNED_HEX` bytes, computed independently with Python
    /// blake3 (pip package `blake3`):
    /// `blake3.blake3(bytes.fromhex(PINNED_HEX)).hexdigest()`.
    const PINNED_ID_HEX: &str = "0aaaca9861f35e442cf23ec57b1c0d8258d0912d74e8ccd175d959731e5ca65f";

    fn pinned() -> String {
        PINNED_HEX.split_whitespace().collect()
    }

    #[test]
    fn generator_id_accepts_names_in_the_rule() -> Result<()> {
        for name in ["a", "gen.v1", "random-walk"] {
            assert_eq!(GeneratorId::new(name)?.as_str(), name);
        }
        Ok(())
    }

    #[test]
    fn generator_id_rejects_names_outside_the_rule() {
        for name in ["", "Gen.v1", "has space", "a/b"] {
            assert!(matches!(GeneratorId::new(name), Err(Error::Validation(_))));
        }
    }

    #[test]
    fn encoding_matches_the_hand_derived_layout() -> Result<()> {
        let hex = to_hex(&sample_config()?.to_bytes());
        assert_eq!(hex, pinned());
        // The embedded run params' own domain tag is visible in the bytes.
        assert!(hex.contains(&to_hex(b"sima.params.v1")));
        Ok(())
    }

    #[test]
    fn id_matches_the_independently_computed_digest() -> Result<()> {
        assert_eq!(sample_config()?.id(), RunId::from_hex(PINNED_ID_HEX)?);
        Ok(())
    }

    #[test]
    fn to_bytes_from_bytes_round_trips() -> Result<()> {
        let full = sample_config()?;
        let empty_blobs = RunConfig {
            generator: GeneratorConfig {
                id: GeneratorId::new("gen.v1")?,
                params: Vec::new(),
            },
            params: Params { bytes: Vec::new() },
            ..sample_config()?
        };
        for config in [full, empty_blobs] {
            assert_eq!(RunConfig::from_bytes(&config.to_bytes())?, config);
        }
        Ok(())
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = from_hex(&pinned());
        for cut in 0..full.len() {
            assert!(
                matches!(RunConfig::from_bytes(&full[..cut]), Err(Error::Encoding(_))),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut buf = from_hex(&pinned());
        buf.push(0);
        assert!(matches!(
            RunConfig::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn decode_rejects_a_wrong_domain_tag() {
        let mut enc = Enc::new();
        enc.str(TAG_SPEC);
        assert!(matches!(
            RunConfig::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn decode_rejects_an_invalid_format_name() {
        // Decode revalidates the name rules: a format id violating the
        // name rule is rejected even in well-framed bytes.
        let mut enc = Enc::new();
        enc.str(TAG_RUN_CONFIG).u64(42).str("Stub");
        assert!(matches!(
            RunConfig::from_bytes(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn decode_rejects_an_invalid_generator_name() {
        // The same revalidation for the generator id, past a valid format.
        let mut enc = Enc::new();
        enc.str(TAG_RUN_CONFIG)
            .u64(42)
            .str("stub.v1")
            .str("Bad Gen");
        assert!(matches!(
            RunConfig::from_bytes(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn varying_any_single_field_changes_the_run_id() -> Result<()> {
        let base = sample_config()?;
        let variants = [
            base.clone(),
            RunConfig {
                root_seed: 43,
                ..base.clone()
            },
            RunConfig {
                format: FormatId::new("other.v1")?,
                ..base.clone()
            },
            RunConfig {
                generator: GeneratorConfig {
                    id: GeneratorId::new("other-gen.v1")?,
                    params: base.generator.params.clone(),
                },
                ..base.clone()
            },
            RunConfig {
                generator: GeneratorConfig {
                    id: base.generator.id.clone(),
                    params: vec![0xBE, 0xEF],
                },
                ..base.clone()
            },
            RunConfig {
                params: Params { bytes: vec![9] },
                ..base.clone()
            },
        ];
        let ids: Vec<RunId> = variants.iter().map(RunConfig::id).collect();
        for (i, a) in ids.iter().enumerate() {
            for (j, b) in ids.iter().enumerate().skip(i + 1) {
                assert_ne!(a, b, "variants {i} and {j} must have distinct run ids");
            }
        }
        Ok(())
    }

    #[test]
    fn run_id_display_and_from_hex_round_trip() -> Result<()> {
        let id = sample_config()?.id();
        assert_eq!(RunId::from_hex(&id.to_string())?, id);
        Ok(())
    }
}
