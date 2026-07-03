//! Execution environment: the content-derived components that participate
//! in task identity.
//!
//! Components are content-derived only — an engine version constant, a
//! digest of a build input. Anything machine-derived (hostname, device,
//! driver, paths, time) is journal metadata in higher layers, never a
//! component: two machines with equal environments must produce equal
//! results, so only result-relevant identity may enter.

use sima_core::{Dec, Enc, Error, Hash, Result, hash_bytes};

use crate::canonical::{self, TAG_ENVIRONMENT};

/// Arm byte marking an [`EnvironmentValue::Version`] payload.
const ARM_VERSION: u8 = 0;
/// Arm byte marking an [`EnvironmentValue::Digest`] payload.
const ARM_DIGEST: u8 = 1;

/// Value of an environment component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvironmentValue {
    /// Engine or executor identity as a versioned constant; non-empty.
    Version(String),
    /// Content hash of a build input the executor's results depend on
    /// (compiled SPIR-V joins here in P2).
    Digest(Hash),
}

/// A named environment component. Fields are private so every constructed
/// component satisfies the name rule and the non-empty-version rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentComponent {
    name: String,
    value: EnvironmentValue,
}

impl EnvironmentComponent {
    /// Validates the name against the shared name rule — and, for
    /// [`EnvironmentValue::Version`], the version string against the non-empty
    /// rule — then wraps them.
    pub fn new(name: impl Into<String>, value: EnvironmentValue) -> Result<EnvironmentComponent> {
        let name = name.into();
        canonical::validate_name(&name)?;
        if let EnvironmentValue::Version(version) = &value
            && version.is_empty()
        {
            return Err(Error::Validation(format!(
                "environment component {name:?} has an empty version string"
            )));
        }
        Ok(EnvironmentComponent { name, value })
    }

    /// The component's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The component's value.
    pub fn value(&self) -> &EnvironmentValue {
        &self.value
    }

    /// Appends the component's canonical form: name str, arm byte, payload.
    fn encode(&self, enc: &mut Enc) {
        enc.str(&self.name);
        match &self.value {
            EnvironmentValue::Version(version) => {
                enc.u8(ARM_VERSION).str(version);
            }
            EnvironmentValue::Digest(digest) => {
                enc.u8(ARM_DIGEST).hash(digest);
            }
        }
    }

    /// Reads a canonical form written by [`EnvironmentComponent::encode`],
    /// revalidating the constructor rules.
    fn decode(dec: &mut Dec<'_>) -> Result<EnvironmentComponent> {
        let name = dec.str()?.to_string();
        let value = match dec.u8()? {
            ARM_VERSION => EnvironmentValue::Version(dec.str()?.to_string()),
            ARM_DIGEST => EnvironmentValue::Digest(dec.hash()?),
            arm => {
                return Err(Error::Encoding(format!(
                    "invalid environment value arm byte {arm}, expected {ARM_VERSION} or {ARM_DIGEST}"
                )));
            }
        };
        EnvironmentComponent::new(name, value)
    }
}

/// The environment a task's results depend on: a non-empty list of
/// components, held sorted by unique name so equal environments have equal
/// bytes regardless of construction order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment {
    components: Vec<EnvironmentComponent>,
}

impl Environment {
    /// Sorts `components` by name and wraps them; rejects an empty list and
    /// duplicate names.
    pub fn new(mut components: Vec<EnvironmentComponent>) -> Result<Environment> {
        if components.is_empty() {
            return Err(Error::Validation(
                "environment must have at least one component".to_string(),
            ));
        }
        canonical::sort_by_unique_name(&mut components, EnvironmentComponent::name)?;
        Ok(Environment { components })
    }

    /// The components, sorted by name.
    pub fn components(&self) -> &[EnvironmentComponent] {
        &self.components
    }

    /// Appends the tagged canonical form: tag, u64 component count, then
    /// each component in name order.
    pub fn encode(&self, enc: &mut Enc) {
        enc.str(TAG_ENVIRONMENT).u64(self.components.len() as u64);
        for component in &self.components {
            component.encode(enc);
        }
    }

    /// Reads and validates a canonical form written by
    /// [`Environment::encode`]: the count is untrusted, so components are
    /// accumulated without preallocation, and names must arrive strictly
    /// ascending.
    pub fn decode(dec: &mut Dec<'_>) -> Result<Environment> {
        canonical::expect_tag(dec, TAG_ENVIRONMENT)?;
        let count = dec.u64()?;
        if count == 0 {
            return Err(Error::Validation(
                "environment must have at least one component".to_string(),
            ));
        }
        let mut components: Vec<EnvironmentComponent> = Vec::new();
        for _ in 0..count {
            let component = EnvironmentComponent::decode(dec)?;
            canonical::require_ascending_names(
                components.last().map(EnvironmentComponent::name),
                component.name(),
            )?;
            components.push(component);
        }
        Ok(Environment { components })
    }

    /// The environment's content id: the blake3 digest of its standalone
    /// bytes.
    pub fn id(&self) -> EnvironmentId {
        EnvironmentId::from_hash(hash_bytes(&self.to_bytes()))
    }
}

canonical::standalone_codec!(Environment);

canonical::id_newtype! {
    /// Content id of an [`Environment`]: the digest of its standalone
    /// canonical bytes, carried in every task key.
    EnvironmentId
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::{TAG_ENVIRONMENT, TAG_SPEC};
    use crate::testutil::{fill_hash, from_hex, to_hex};
    use sima_core::{Enc, Error, Hash, Result};

    fn sample_digest() -> Result<Hash> {
        fill_hash("22")
    }

    /// The two components of the pinned environment, in sorted name order.
    fn sample_components() -> Result<[EnvironmentComponent; 2]> {
        Ok([
            EnvironmentComponent::new("engine", EnvironmentValue::Version("0.1.0".to_string()))?,
            EnvironmentComponent::new("shader", EnvironmentValue::Digest(sample_digest()?))?,
        ])
    }

    fn sample_environment() -> Result<Environment> {
        Environment::new(sample_components()?.to_vec())
    }

    /// Hand-derived canonical bytes of `sample_env`, field by field in
    /// encoding order per the `sima-core` encode format:
    ///   str tag "sima.env.v1"  -> u64 len 11 LE ‖ UTF-8 bytes
    ///   u64 component count 2
    ///   component "engine": str name (len 6 LE ‖ UTF-8), arm byte 00
    ///     (Version), str payload "0.1.0" (len 5 LE ‖ UTF-8)
    ///   component "shader": str name (len 6 LE ‖ UTF-8), arm byte 01
    ///     (Digest), payload 32 raw digest bytes (0x22 repeated)
    const PINNED_HEX: &str = "0b0000000000000073696d612e656e762e7631\
                              0200000000000000\
                              0600000000000000656e67696e65000500000000000000302e312e30\
                              0600000000000000736861646572012222222222222222222222222222222222222222222222222222222222222222";

    /// blake3 of the `PINNED_HEX` bytes, computed independently with Python
    /// blake3 (pip package `blake3`):
    /// `blake3.blake3(bytes.fromhex(PINNED_HEX)).hexdigest()`.
    const PINNED_ID_HEX: &str = "65d1e7d99c1fd16782d8bf23d9d00f948095daa388134efa0c5a4cb7a11ba14f";

    fn pinned() -> String {
        PINNED_HEX.split_whitespace().collect()
    }

    #[test]
    fn constructor_sorts_components_by_name() -> Result<()> {
        let [engine, shader] = sample_components()?;
        let sorted = Environment::new(vec![engine.clone(), shader.clone()])?;
        let shuffled = Environment::new(vec![shader, engine])?;
        assert_eq!(sorted, shuffled);
        assert_eq!(sorted.id(), shuffled.id());
        assert_eq!(
            sorted
                .components()
                .iter()
                .map(EnvironmentComponent::name)
                .collect::<Vec<_>>(),
            ["engine", "shader"]
        );
        Ok(())
    }

    #[test]
    fn constructor_rejects_duplicate_names() -> Result<()> {
        let a = EnvironmentComponent::new("engine", EnvironmentValue::Version("1".to_string()))?;
        let b = EnvironmentComponent::new("engine", EnvironmentValue::Version("2".to_string()))?;
        assert!(matches!(
            Environment::new(vec![a, b]),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn constructor_rejects_an_empty_component_list() {
        assert!(matches!(
            Environment::new(Vec::new()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn component_rejects_an_invalid_name() {
        assert!(matches!(
            EnvironmentComponent::new("Engine", EnvironmentValue::Version("1".to_string())),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn component_rejects_an_empty_version() {
        assert!(matches!(
            EnvironmentComponent::new("engine", EnvironmentValue::Version(String::new())),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn encoding_matches_the_hand_derived_layout() -> Result<()> {
        assert_eq!(to_hex(&sample_environment()?.to_bytes()), pinned());
        Ok(())
    }

    #[test]
    fn id_matches_the_independently_computed_digest() -> Result<()> {
        assert_eq!(
            sample_environment()?.id(),
            EnvironmentId::from_hex(PINNED_ID_HEX)?
        );
        Ok(())
    }

    #[test]
    fn to_bytes_from_bytes_round_trips() -> Result<()> {
        let env = sample_environment()?;
        assert_eq!(Environment::from_bytes(&env.to_bytes())?, env);
        Ok(())
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = from_hex(&pinned());
        for cut in 0..full.len() {
            assert!(
                matches!(
                    Environment::from_bytes(&full[..cut]),
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
            Environment::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn decode_rejects_a_wrong_domain_tag() {
        let mut enc = Enc::new();
        enc.str(TAG_SPEC).u64(1);
        assert!(matches!(
            Environment::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    /// Encodes a two-component environment body by hand, with the
    /// components in the order given.
    fn encode_components(names_and_versions: &[(&str, &str)]) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.str(TAG_ENVIRONMENT)
            .u64(names_and_versions.len() as u64);
        for (name, version) in names_and_versions {
            enc.str(name).u8(0).str(version);
        }
        enc.finish()
    }

    #[test]
    fn decode_rejects_out_of_order_components() {
        let buf = encode_components(&[("shader", "1"), ("engine", "1")]);
        assert!(matches!(
            Environment::from_bytes(&buf),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn decode_rejects_duplicate_names() {
        let buf = encode_components(&[("engine", "1"), ("engine", "2")]);
        assert!(matches!(
            Environment::from_bytes(&buf),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn decode_rejects_an_empty_component_list() {
        let buf = encode_components(&[]);
        assert!(matches!(
            Environment::from_bytes(&buf),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn decode_rejects_an_unknown_arm_byte() {
        let mut enc = Enc::new();
        enc.str(TAG_ENVIRONMENT).u64(1).str("engine").u8(2).str("1");
        assert!(matches!(
            Environment::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn version_and_digest_under_the_same_name_have_distinct_ids() -> Result<()> {
        let version = Environment::new(vec![EnvironmentComponent::new(
            "engine",
            EnvironmentValue::Version("x".to_string()),
        )?])?;
        let digest = Environment::new(vec![EnvironmentComponent::new(
            "engine",
            EnvironmentValue::Digest(sample_digest()?),
        )?])?;
        assert_ne!(version.id(), digest.id());
        Ok(())
    }
}
