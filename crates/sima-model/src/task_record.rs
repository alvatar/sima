//! Task result record: the identity a committed result answers for and the
//! artifacts it produced.
//!
//! Records exist only for committed successes. Attempt numbers, worker
//! ids, timings, and failure histories are journal material in higher
//! layers, never record fields — the record carries identity inputs, the
//! journal carries execution context.

use sima_core::{Dec, Enc, Hash, Result};

use crate::canon::{self, TAG_TASK_RECORD};
use crate::task::TaskIdentity;

/// A named reference to a committed artifact object in the store. The name
/// is private so every constructed ref satisfies the name rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRef {
    name: String,
    object: Hash,
}

impl ArtifactRef {
    /// Validates the name against the shared name rule and wraps the
    /// reference.
    pub fn new(name: impl Into<String>, object: Hash) -> Result<ArtifactRef> {
        let name = name.into();
        canon::validate_name(&name)?;
        Ok(ArtifactRef { name, object })
    }

    /// The artifact's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The digest of the artifact's object bytes.
    pub fn object(&self) -> &Hash {
        &self.object
    }

    /// Appends the reference's canonical form: name str, object digest.
    fn encode(&self, enc: &mut Enc) {
        enc.str(&self.name).hash(&self.object);
    }

    /// Reads a canonical form written by [`ArtifactRef::encode`],
    /// revalidating the name rule.
    fn decode(dec: &mut Dec<'_>) -> Result<ArtifactRef> {
        let name = dec.str()?.to_string();
        let object = dec.hash()?;
        ArtifactRef::new(name, object)
    }
}

/// The committed result of a task: the full identity it answers for and
/// the artifacts it produced, sorted by unique name. Embedding the
/// identity makes records self-describing — the store verifies
/// `record.identity.key()` against the index path a record was found
/// under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    /// The identity whose evaluation this record commits.
    pub identity: TaskIdentity,
    artifacts: Vec<ArtifactRef>,
}

impl TaskRecord {
    /// Sorts `artifacts` by name and wraps them with the identity; rejects
    /// duplicate names. An empty artifact list is legal.
    pub fn new(identity: TaskIdentity, mut artifacts: Vec<ArtifactRef>) -> Result<TaskRecord> {
        canon::sort_by_unique_name(&mut artifacts, ArtifactRef::name)?;
        Ok(TaskRecord {
            identity,
            artifacts,
        })
    }

    /// The artifact references, sorted by name.
    pub fn artifacts(&self) -> &[ArtifactRef] {
        &self.artifacts
    }

    /// Appends the tagged canonical form: tag, the embedded identity
    /// encoding (its own tag included), u64 artifact count, then each
    /// reference in name order.
    pub fn encode(&self, enc: &mut Enc) {
        enc.str(TAG_TASK_RECORD);
        self.identity.encode(enc);
        enc.u64(self.artifacts.len() as u64);
        for artifact in &self.artifacts {
            artifact.encode(enc);
        }
    }

    /// Reads and validates a canonical form written by
    /// [`TaskRecord::encode`]: the count is untrusted, so references are
    /// accumulated without preallocation, and names must arrive strictly
    /// ascending.
    pub fn decode(dec: &mut Dec<'_>) -> Result<TaskRecord> {
        canon::expect_tag(dec, TAG_TASK_RECORD)?;
        let identity = TaskIdentity::decode(dec)?;
        let count = dec.u64()?;
        let mut artifacts: Vec<ArtifactRef> = Vec::new();
        for _ in 0..count {
            let artifact = ArtifactRef::decode(dec)?;
            canon::require_ascending_names(
                artifacts.last().map(ArtifactRef::name),
                artifact.name(),
            )?;
            artifacts.push(artifact);
        }
        Ok(TaskRecord {
            identity,
            artifacts,
        })
    }
}

canon::standalone_codec!(TaskRecord);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canon::{TAG_SPEC, TAG_TASK_RECORD};
    use crate::env::EnvId;
    use crate::params::ParamsId;
    use crate::spec::SpecId;
    use crate::task::TaskIdentity;
    use sima_core::{Enc, Error, Hash, Result, hash_bytes};

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

    /// The stateless-arm identity pinned in the `task` module tests.
    fn sample_identity() -> Result<TaskIdentity> {
        Ok(TaskIdentity {
            spec: SpecId::from_hash(fill_hash("11")?),
            params: ParamsId::from_hash(fill_hash("22")?),
            seed: 0x0102_0304_0506_0708,
            env: EnvId::from_hash(fill_hash("33")?),
            input_state: None,
        })
    }

    fn sample_record() -> Result<TaskRecord> {
        TaskRecord::new(
            sample_identity()?,
            vec![ArtifactRef::new("state-final", fill_hash("55")?)?],
        )
    }

    /// Hand-derived canonical bytes of `sample_record`, field by field in
    /// encoding order per the `sima-core` encode format:
    ///   str tag "sima.task-record.v1" -> u64 len 19 LE ‖ UTF-8 bytes
    ///   embedded identity encoding, its own tag included: str tag
    ///     "sima.task.v1", spec digest 32x11, params digest 32x22, seed
    ///     u64 LE 0807060504030201, env digest 32x33, absent flag byte 00
    ///   u64 artifact count 1
    ///   ref: str name "state-final" (len 11 LE ‖ UTF-8), 32 raw digest
    ///     bytes (0x55 repeated)
    const PINNED_HEX: &str = "130000000000000073696d612e7461736b2d7265636f72642e7631\
                              0c0000000000000073696d612e7461736b2e7631\
                              1111111111111111111111111111111111111111111111111111111111111111\
                              2222222222222222222222222222222222222222222222222222222222222222\
                              0807060504030201\
                              3333333333333333333333333333333333333333333333333333333333333333\
                              00\
                              0100000000000000\
                              0b0000000000000073746174652d66696e616c\
                              5555555555555555555555555555555555555555555555555555555555555555";

    /// blake3 of the `PINNED_HEX` bytes, computed independently with Python
    /// blake3 (pip package `blake3`):
    /// `blake3.blake3(bytes.fromhex(PINNED_HEX)).hexdigest()`.
    const PINNED_DIGEST_HEX: &str =
        "86e602d6a611fe1fb392680e94b263bf771d8359a6cd62ea15ee061dcfae4942";

    fn pinned() -> String {
        PINNED_HEX.split_whitespace().collect()
    }

    #[test]
    fn encoding_matches_the_hand_derived_layout() -> Result<()> {
        let hex = to_hex(&sample_record()?.to_bytes());
        assert_eq!(hex, pinned());
        // Records are self-describing: the embedded identity's own domain
        // tag is visible in the bytes.
        assert!(hex.contains(&to_hex(b"sima.task.v1")));
        Ok(())
    }

    #[test]
    fn record_bytes_match_the_independently_computed_digest() -> Result<()> {
        assert_eq!(
            hash_bytes(&sample_record()?.to_bytes()),
            Hash::from_hex(PINNED_DIGEST_HEX)?
        );
        Ok(())
    }

    #[test]
    fn to_bytes_from_bytes_round_trips_zero_one_and_three_artifacts() -> Result<()> {
        let artifacts = [
            Vec::new(),
            vec![ArtifactRef::new("state-final", fill_hash("55")?)?],
            vec![
                ArtifactRef::new("snapshot.0", fill_hash("66")?)?,
                ArtifactRef::new("snapshot.1", fill_hash("77")?)?,
                ArtifactRef::new("state-final", fill_hash("55")?)?,
            ],
        ];
        for refs in artifacts {
            let record = TaskRecord::new(sample_identity()?, refs)?;
            assert_eq!(TaskRecord::from_bytes(&record.to_bytes())?, record);
        }
        Ok(())
    }

    #[test]
    fn constructor_sorts_artifacts_by_name() -> Result<()> {
        let a = ArtifactRef::new("snapshot.0", fill_hash("66")?)?;
        let b = ArtifactRef::new("state-final", fill_hash("55")?)?;
        let sorted = TaskRecord::new(sample_identity()?, vec![a.clone(), b.clone()])?;
        let shuffled = TaskRecord::new(sample_identity()?, vec![b, a])?;
        assert_eq!(sorted, shuffled);
        assert_eq!(
            sorted
                .artifacts()
                .iter()
                .map(ArtifactRef::name)
                .collect::<Vec<_>>(),
            ["snapshot.0", "state-final"]
        );
        Ok(())
    }

    #[test]
    fn constructor_rejects_duplicate_artifact_names() -> Result<()> {
        let a = ArtifactRef::new("state-final", fill_hash("55")?)?;
        let b = ArtifactRef::new("state-final", fill_hash("66")?)?;
        assert!(matches!(
            TaskRecord::new(sample_identity()?, vec![a, b]),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn artifact_ref_rejects_an_invalid_name() -> Result<()> {
        assert!(matches!(
            ArtifactRef::new("State Final", fill_hash("55")?),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    /// Encodes a record body by hand with the given artifact names, each
    /// referencing the 0x55 digest, in the order given.
    fn encode_record(names: &[&str]) -> Result<Vec<u8>> {
        let mut enc = Enc::new();
        enc.str(TAG_TASK_RECORD);
        sample_identity()?.encode(&mut enc);
        enc.u64(names.len() as u64);
        for name in names {
            enc.str(name).hash(&fill_hash("55")?);
        }
        Ok(enc.finish())
    }

    #[test]
    fn decode_rejects_out_of_order_artifact_names() -> Result<()> {
        let buf = encode_record(&["state-final", "snapshot.0"])?;
        assert!(matches!(
            TaskRecord::from_bytes(&buf),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_duplicate_artifact_names() -> Result<()> {
        let buf = encode_record(&["state-final", "state-final"])?;
        assert!(matches!(
            TaskRecord::from_bytes(&buf),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn decoded_identity_keys_match_the_source() -> Result<()> {
        let record = sample_record()?;
        let decoded = TaskRecord::from_bytes(&record.to_bytes())?;
        assert_eq!(decoded.identity.key(), sample_identity()?.key());
        Ok(())
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = from_hex(&pinned());
        for cut in 0..full.len() {
            assert!(
                matches!(
                    TaskRecord::from_bytes(&full[..cut]),
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
            TaskRecord::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn decode_rejects_a_wrong_domain_tag() {
        let mut enc = Enc::new();
        enc.str(TAG_SPEC);
        assert!(matches!(
            TaskRecord::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }
}
