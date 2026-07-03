//! Canonical-form conventions shared by every identity-bearing type:
//! domain-tag constants, the domain-tag reader, the name validator, and the
//! id-newtype macro.
//!
//! Every canonical encoding opens with a str-framed domain tag from the
//! table below. Tags make stored blobs self-describing, turn a hash routed
//! to the wrong decoder into an immediate clean failure, and the `.v1`
//! suffix anchors format versioning: a layout change mints a `.v2` tag; a
//! published `.v1` layout is fixed forever.

use sima_core::{Dec, Error, Result};

/// Domain tag opening a canonical [`crate::Spec`] encoding.
pub(crate) const TAG_SPEC: &str = "sima.spec.v1";
/// Domain tag opening a canonical [`crate::Params`] encoding.
pub(crate) const TAG_PARAMS: &str = "sima.params.v1";
/// Domain tag opening a canonical [`crate::Environment`] encoding.
pub(crate) const TAG_ENV: &str = "sima.env.v1";
/// Domain tag opening a canonical [`crate::TaskIdentity`] encoding — the
/// task-key preimage.
#[allow(dead_code)]
pub(crate) const TAG_TASK: &str = "sima.task.v1";
/// Domain tag opening a canonical [`crate::TaskRecord`] encoding.
#[allow(dead_code)]
pub(crate) const TAG_TASK_RECORD: &str = "sima.task-record.v1";
/// Domain tag opening a canonical [`crate::RunConfig`] encoding — the
/// run-id preimage.
#[allow(dead_code)]
pub(crate) const TAG_RUN_CONFIG: &str = "sima.run-config.v1";

/// Reads the domain tag opening a canonical encoding and requires it to be
/// `tag`, so a hash routed to the wrong decoder fails immediately.
pub(crate) fn expect_tag(dec: &mut Dec<'_>, tag: &str) -> Result<()> {
    let found = dec.str()?;
    if found == tag {
        Ok(())
    } else {
        Err(Error::Encoding(format!(
            "domain tag mismatch: expected {tag:?}, found {found:?}"
        )))
    }
}

/// Validates a store-adjacent name — format ids, generator ids, environment
/// component names, artifact names: 1..=64 bytes, every byte in
/// `[a-z0-9._-]`. Lowercase-only keeps one spelling per identity.
pub(crate) fn validate_name(name: &str) -> Result<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 {
        return Err(Error::Validation(format!(
            "name must be 1..=64 bytes, got {} bytes",
            bytes.len()
        )));
    }
    for &b in bytes {
        match b {
            b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => {}
            _ => {
                return Err(Error::Validation(format!(
                    "name {name:?} contains byte {:?}; allowed bytes are [a-z0-9._-]",
                    b as char
                )));
            }
        }
    }
    Ok(())
}

/// Sorts `items` by name and rejects duplicate names, so a canonicalized
/// sequence has exactly one byte spelling per value.
pub(crate) fn sort_by_unique_name<T>(items: &mut [T], name_of: fn(&T) -> &str) -> Result<()> {
    items.sort_by(|a, b| name_of(a).cmp(name_of(b)));
    for pair in items.windows(2) {
        if name_of(&pair[0]) == name_of(&pair[1]) {
            return Err(Error::Validation(format!(
                "duplicate name {:?} in a uniquely-named sequence",
                name_of(&pair[0])
            )));
        }
    }
    Ok(())
}

/// Requires strictly ascending names while decoding a canonicalized
/// sequence, rejecting out-of-order and duplicate names.
pub(crate) fn require_ascending_names(prev: Option<&str>, next: &str) -> Result<()> {
    match prev {
        Some(p) if p >= next => Err(Error::Validation(format!(
            "names out of canonical order: {p:?} then {next:?}"
        ))),
        _ => Ok(()),
    }
}

/// Implements the standalone framing of the uniform type surface:
/// `to_bytes` runs `encode` on a fresh encoder — the result is exactly the
/// value's store object bytes; `from_bytes` runs `decode` and rejects
/// trailing input.
macro_rules! standalone_codec {
    ($ty:ident) => {
        impl $ty {
            /// The standalone canonical bytes — exactly the value's store
            /// object bytes.
            pub fn to_bytes(&self) -> Vec<u8> {
                let mut enc = ::sima_core::Enc::new();
                self.encode(&mut enc);
                enc.finish()
            }

            /// Parses standalone canonical bytes, rejecting trailing input.
            pub fn from_bytes(bytes: &[u8]) -> ::sima_core::Result<Self> {
                let mut dec = ::sima_core::Dec::new(bytes);
                let value = Self::decode(&mut dec)?;
                dec.finish()?;
                Ok(value)
            }
        }
    };
}
pub(crate) use standalone_codec;

/// Defines an id newtype over [`sima_core::Hash`]: the blake3 digest of a
/// value's standalone canonical bytes, wrapped in a role label that prevents
/// cross-wiring between id kinds. `from_hash` is public because stores
/// rebuild ids from digests decoded outside this crate; `Display` and
/// `from_hex` carry the canonical lowercase-hex text form used in store
/// paths and CLI arguments.
macro_rules! id_newtype {
    ($(#[$attr:meta])* $name:ident) => {
        $(#[$attr])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(::sima_core::Hash);

        impl $name {
            /// Wraps a digest as this id kind.
            pub const fn from_hash(hash: ::sima_core::Hash) -> Self {
                Self(hash)
            }

            /// The wrapped digest.
            pub const fn as_hash(&self) -> &::sima_core::Hash {
                &self.0
            }

            /// Parses the canonical lowercase-hex form.
            pub fn from_hex(s: &str) -> ::sima_core::Result<Self> {
                Ok(Self(::sima_core::Hash::from_hex(s)?))
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Display::fmt(&self.0, f)
            }
        }
    };
}
pub(crate) use id_newtype;

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::{Enc, hash_bytes};

    const TAGS: [&str; 6] = [
        TAG_SPEC,
        TAG_PARAMS,
        TAG_ENV,
        TAG_TASK,
        TAG_TASK_RECORD,
        TAG_RUN_CONFIG,
    ];

    #[test]
    fn tags_are_pairwise_distinct() {
        for (i, a) in TAGS.iter().enumerate() {
            for b in &TAGS[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn tags_match_the_settled_shape() {
        // Every tag is `sima.<name>.v1` with `<name>` in the name alphabet.
        for tag in TAGS {
            let name = tag
                .strip_prefix("sima.")
                .and_then(|rest| rest.strip_suffix(".v1"))
                .expect("tag must be sima.<name>.v1");
            assert!(!name.is_empty(), "tag {tag:?} has an empty name");
            assert!(
                name.bytes().all(|b| matches!(b, b'a'..=b'z' | b'-')),
                "tag {tag:?} name part must be lowercase words"
            );
        }
    }

    /// Encodes `tag` as the opening of a canonical form, as every `encode`
    /// in this crate does.
    fn tagged(tag: &str) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.str(tag);
        enc.finish()
    }

    #[test]
    fn expect_tag_accepts_the_expected_tag() -> Result<()> {
        let buf = tagged(TAG_SPEC);
        let mut dec = Dec::new(&buf);
        expect_tag(&mut dec, TAG_SPEC)?;
        dec.finish()
    }

    #[test]
    fn expect_tag_rejects_a_mismatched_tag() {
        let buf = tagged(TAG_PARAMS);
        let mut dec = Dec::new(&buf);
        assert!(matches!(
            expect_tag(&mut dec, TAG_SPEC),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn expect_tag_rejects_truncated_input() {
        let buf = tagged(TAG_SPEC);
        let mut dec = Dec::new(&buf[..buf.len() - 1]);
        assert!(matches!(
            expect_tag(&mut dec, TAG_SPEC),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn validate_name_accepts_names_in_the_rule() -> Result<()> {
        for name in ["a", "stub.v1", "state-final", "under_score", "0"] {
            validate_name(name)?;
        }
        validate_name(&"a".repeat(64))
    }

    #[test]
    fn validate_name_rejects_names_outside_the_rule() {
        let long = "a".repeat(65);
        for name in ["", &long, "Upper", "has space", "a/b", "café"] {
            assert!(
                matches!(validate_name(name), Err(Error::Validation(_))),
                "name {name:?} must be rejected with Error::Validation"
            );
        }
    }

    // The macro's output is exercised on a locally defined id kind; the
    // same assertions hold for every id the crate defines with it.
    id_newtype! {
        /// Id kind defined for the macro tests.
        TestId
    }

    #[test]
    fn id_newtype_display_and_from_hex_round_trip() -> Result<()> {
        let id = TestId::from_hash(hash_bytes(b"macro round trip"));
        let hex = id.to_string();
        assert_eq!(hex.len(), 64);
        assert_eq!(TestId::from_hex(&hex)?, id);
        Ok(())
    }

    #[test]
    fn id_newtype_display_matches_the_digest_hex() {
        let hash = hash_bytes(b"digest text form");
        let id = TestId::from_hash(hash);
        assert_eq!(id.to_string(), hash.to_string());
        assert_eq!(id.as_hash(), &hash);
    }

    #[test]
    fn id_newtype_from_hex_rejects_malformed_hex() {
        for s in ["", "abc", &"g".repeat(64), &"A".repeat(64)] {
            assert!(matches!(TestId::from_hex(s), Err(Error::Encoding(_))));
        }
    }

    #[test]
    fn id_newtype_orders_by_digest_bytes() -> Result<()> {
        let lo = TestId::from_hex(&"00".repeat(32))?;
        let hi = TestId::from_hex(&"ff".repeat(32))?;
        assert!(lo < hi);
        Ok(())
    }
}
