//! Search configuration and search identity: the identity-bearing portion of a
//! search's configuration, whose canonical bytes define the search id.
//!
//! Search identity is the hash of canonicalized config. Execution knobs —
//! worker count, store path, backends — live in a separate, non-identity
//! configuration section in higher layers: a search resumed with different
//! parallelism or on different hardware keeps its search id. The environment
//! id is also absent: config records intent, and the environment travels
//! in every task key.

use std::num::NonZeroU64;

use sima_core::{Codec, Dec, Enc, Error, Result, hash_bytes};

use crate::canonical::{self, TAG_SEARCH_CONFIG};
use crate::params::Params;
use crate::spec::FormatId;

/// Name of the generator a search draws candidates from. Validated by the
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
/// blob its implementation defines the encoding of. Scoped under
/// the generator and distinct from the search-level [`Params`] that feeds
/// every task's params slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratorConfig {
    /// The generator to draw candidates from.
    pub id: GeneratorId,
    /// The generator's own settings blob. Opaque to the infrastructure;
    /// empty is legal.
    pub params: Vec<u8>,
}

/// The identity-bearing portion of a search's configuration. Its canonical
/// bytes are the search-id preimage: search identity is the hash of
/// canonicalized config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchConfig {
    /// The search's root seed, from which task seeds derive.
    pub root_seed: u64,
    /// The number of tasks each candidate's chain comprises: the search's
    /// work-division quantity, walked segment by segment through committed
    /// state. Absent means one stateless task per candidate.
    pub segments: Option<NonZeroU64>,
    /// Format governing the interpretation of generated specs and of the
    /// search params.
    pub format: FormatId,
    /// The generator and its settings — the search axis.
    pub generator: GeneratorConfig,
    /// The search params fed to every task's params slot — the evaluation
    /// axis, produced by config.
    pub params: Params,
}

impl SearchConfig {
    /// Appends the tagged canonical form: tag, root_seed u64, segments
    /// optional u64, format str, generator id str, generator params bytes,
    /// then the embedded search params encoding (its own tag included).
    pub fn encode(&self, enc: &mut Enc) {
        enc.str(TAG_SEARCH_CONFIG)
            .u64(self.root_seed)
            .opt_u64(self.segments.map(NonZeroU64::get))
            .str(self.format.as_str())
            .str(self.generator.id.as_str())
            .bytes(&self.generator.params);
        self.params.encode(enc);
    }

    /// Reads and validates a canonical form written by
    /// [`SearchConfig::encode`].
    pub fn decode(dec: &mut Dec<'_>) -> Result<SearchConfig> {
        canonical::expect_tag(dec, TAG_SEARCH_CONFIG)?;
        let root_seed = dec.u64()?;
        let segments = dec
            .opt_u64()?
            .map(|n| {
                NonZeroU64::new(n)
                    .ok_or_else(|| Error::Encoding("segments must be non-zero".into()))
            })
            .transpose()?;
        let format = FormatId::new(dec.str()?)?;
        let generator = GeneratorConfig {
            id: GeneratorId::new(dec.str()?)?,
            params: dec.bytes()?.to_vec(),
        };
        let params = Params::decode(dec)?;
        Ok(SearchConfig {
            root_seed,
            segments,
            format,
            generator,
            params,
        })
    }

    /// The search id: the blake3 digest of the config's standalone bytes.
    pub fn id(&self) -> SearchId {
        SearchId::from_hash(hash_bytes(&self.to_bytes()))
    }
}

canonical::standalone_codec!(SearchConfig);

canonical::id_newtype! {
    /// Identity of a search: the digest of its canonicalized [`SearchConfig`]
    /// bytes. Stable across resume, parallelism changes, and hardware.
    SearchId
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::TAG_SPEC;
    use crate::params::Params;
    use crate::spec::FormatId;
    use crate::testutil::{from_hex, to_hex};
    use sima_core::{Enc, Error, Result};

    fn sample_config() -> Result<SearchConfig> {
        Ok(SearchConfig {
            root_seed: 42,
            segments: None,
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
    ///   segments absent  -> Option<u64> flag byte 00
    ///   str format       "stub.v1" -> u64 len 7 LE ‖ UTF-8 bytes
    ///   str generator id "gen.v1"  -> u64 len 6 LE ‖ UTF-8 bytes
    ///   generator params [de, ad]  -> u64 len 2 LE ‖ payload
    ///   embedded search-params encoding, its own tag included: str tag
    ///     "sima.params.v1", bytes [01, 02, 03] (u64 len 3 LE ‖ payload)
    const PINNED_HEX: &str = "120000000000000073696d612e72756e2d636f6e6669672e7631\
                              2a00000000000000\
                              00\
                              0700000000000000737475622e7631\
                              060000000000000067656e2e7631\
                              0200000000000000dead\
                              0e0000000000000073696d612e706172616d732e76310300000000000000010203";

    /// blake3 of the `PINNED_HEX` bytes, computed independently with Python
    /// blake3 (pip package `blake3`):
    /// `blake3.blake3(bytes.fromhex(PINNED_HEX)).hexdigest()`.
    const PINNED_ID_HEX: &str = "18ad1dd30bc36b634e749b10755626411a367ba066c579e3c299a3eda98d4c7b";

    /// `sample_config` with `segments = 7`: the absent flag byte 00 becomes
    /// flag byte 01 followed by u64 LE 7. Id computed independently as above.
    const PINNED_SEGMENTS_HEX: &str = "120000000000000073696d612e72756e2d636f6e6669672e7631\
                                       2a00000000000000\
                                       010700000000000000\
                                       0700000000000000737475622e7631\
                                       060000000000000067656e2e7631\
                                       0200000000000000dead\
                                       0e0000000000000073696d612e706172616d732e76310300000000000000010203";
    const PINNED_SEGMENTS_ID_HEX: &str =
        "2fcb67605146f0a53395b9aa009fc90b0e90a3c3ed71cae93311cc32f22b902a";

    fn pinned() -> String {
        PINNED_HEX.split_whitespace().collect()
    }

    fn sample_segmented() -> Result<SearchConfig> {
        Ok(SearchConfig {
            segments: NonZeroU64::new(7),
            ..sample_config()?
        })
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
        // The embedded search params' own domain tag is visible in the bytes.
        assert!(hex.contains(&to_hex(b"sima.params.v1")));
        Ok(())
    }

    #[test]
    fn id_matches_the_independently_computed_digest() -> Result<()> {
        assert_eq!(sample_config()?.id(), SearchId::from_hex(PINNED_ID_HEX)?);
        Ok(())
    }

    #[test]
    fn segmented_encoding_matches_the_hand_derived_layout() -> Result<()> {
        let expected: String = PINNED_SEGMENTS_HEX.split_whitespace().collect();
        assert_eq!(to_hex(&sample_segmented()?.to_bytes()), expected);
        Ok(())
    }

    #[test]
    fn segmented_id_matches_the_independently_computed_digest() -> Result<()> {
        assert_eq!(
            sample_segmented()?.id(),
            SearchId::from_hex(PINNED_SEGMENTS_ID_HEX)?
        );
        Ok(())
    }

    #[test]
    fn decode_rejects_zero_segments() {
        // A present flag with value zero is malformed: the type is
        // Option<NonZeroU64>, and zero has no representation.
        let mut enc = Enc::new();
        enc.str(TAG_SEARCH_CONFIG).u64(42).opt_u64(Some(0));
        assert!(matches!(
            SearchConfig::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn to_bytes_from_bytes_round_trips() -> Result<()> {
        let full = sample_config()?;
        let empty_blobs = SearchConfig {
            generator: GeneratorConfig {
                id: GeneratorId::new("gen.v1")?,
                params: Vec::new(),
            },
            params: Params { bytes: Vec::new() },
            ..sample_config()?
        };
        for config in [full, empty_blobs, sample_segmented()?] {
            assert_eq!(SearchConfig::from_bytes(&config.to_bytes())?, config);
        }
        Ok(())
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = from_hex(&pinned());
        for cut in 0..full.len() {
            assert!(
                matches!(
                    SearchConfig::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut buf = from_hex(&pinned());
        buf.push(0);
        assert!(matches!(
            SearchConfig::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn decode_rejects_a_wrong_domain_tag() {
        let mut enc = Enc::new();
        enc.str(TAG_SPEC);
        assert!(matches!(
            SearchConfig::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn decode_rejects_an_invalid_format_name() {
        // Decode revalidates the name rules: a format id violating the
        // name rule is rejected even in well-framed bytes.
        let mut enc = Enc::new();
        enc.str(TAG_SEARCH_CONFIG).u64(42).opt_u64(None).str("Stub");
        assert!(matches!(
            SearchConfig::from_bytes(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn decode_rejects_an_invalid_generator_name() {
        // The same revalidation for the generator id, past a valid format.
        let mut enc = Enc::new();
        enc.str(TAG_SEARCH_CONFIG)
            .u64(42)
            .opt_u64(None)
            .str("stub.v1")
            .str("Bad Gen");
        assert!(matches!(
            SearchConfig::from_bytes(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn varying_any_single_field_changes_the_search_id() -> Result<()> {
        let base = sample_config()?;
        let variants = [
            base.clone(),
            SearchConfig {
                root_seed: 43,
                ..base.clone()
            },
            SearchConfig {
                segments: NonZeroU64::new(10),
                ..base.clone()
            },
            SearchConfig {
                format: FormatId::new("other.v1")?,
                ..base.clone()
            },
            SearchConfig {
                generator: GeneratorConfig {
                    id: GeneratorId::new("other-gen.v1")?,
                    params: base.generator.params.clone(),
                },
                ..base.clone()
            },
            SearchConfig {
                generator: GeneratorConfig {
                    id: base.generator.id.clone(),
                    params: vec![0xBE, 0xEF],
                },
                ..base.clone()
            },
            SearchConfig {
                params: Params { bytes: vec![9] },
                ..base.clone()
            },
        ];
        let ids: Vec<SearchId> = variants.iter().map(SearchConfig::id).collect();
        for (i, a) in ids.iter().enumerate() {
            for (j, b) in ids.iter().enumerate().skip(i + 1) {
                assert_ne!(a, b, "variants {i} and {j} must have distinct search ids");
            }
        }
        Ok(())
    }

    #[test]
    fn search_id_display_and_from_hex_round_trip() -> Result<()> {
        let id = sample_config()?.id();
        assert_eq!(SearchId::from_hex(&id.to_string())?, id);
        Ok(())
    }
}
