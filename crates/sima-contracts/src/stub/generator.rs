//! The stub generator: turns a seeded config into a run's candidate specs.
//!
//! [`StubGeneratorConfig`] is the generator's params blob — a list of the
//! behaviors to program into the run's candidates. [`StubGenerator`] reads it,
//! stamps each candidate with a nonce derived from the run seed so the specs
//! stay distinct and depend on the seed, and returns one [`Spec`] per behavior.

use sima_core::{Dec, Enc, Result, prng};
use sima_model::{FormatId, GeneratorId, Spec};

use super::program::{StubBehavior, StubProgram};
use crate::generator::Generator;

/// The stub generator's params: the behaviors to program into a run's
/// candidates, in order. Its canonical form carries no domain tag — it lives
/// inside a params blob, which frames it. One candidate is produced per
/// behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StubGeneratorConfig {
    /// The behavior to program into each candidate, in production order.
    pub behaviors: Vec<StubBehavior>,
}

impl StubGeneratorConfig {
    /// Appends the canonical form: a `u64` count, then each behavior in order.
    pub fn encode(&self, enc: &mut Enc) {
        enc.u64(self.behaviors.len() as u64);
        for behavior in &self.behaviors {
            behavior.encode(enc);
        }
    }

    /// Reads a canonical form written by [`StubGeneratorConfig::encode`]. The
    /// count is untrusted, so behaviors accumulate without preallocation and
    /// a truncated tail fails cleanly through the behavior decoder.
    pub fn decode(dec: &mut Dec<'_>) -> Result<StubGeneratorConfig> {
        let count = dec.u64()?;
        let mut behaviors = Vec::new();
        for _ in 0..count {
            behaviors.push(StubBehavior::decode(dec)?);
        }
        Ok(StubGeneratorConfig { behaviors })
    }

    /// The standalone canonical bytes of this config.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        self.encode(&mut enc);
        enc.finish()
    }

    /// Parses standalone canonical bytes, rejecting trailing input.
    pub fn from_bytes(bytes: &[u8]) -> Result<StubGeneratorConfig> {
        let mut dec = Dec::new(bytes);
        let config = StubGeneratorConfig::decode(&mut dec)?;
        dec.finish()?;
        Ok(config)
    }
}

/// Produces a run's candidate specs from a [`StubGeneratorConfig`]. Each spec
/// carries a [`StubProgram`] whose nonce is derived from the run seed, so the
/// generator is genuinely seeded: a different root seed yields different specs.
#[derive(Debug, Clone)]
pub struct StubGenerator {
    id: GeneratorId,
}

impl StubGenerator {
    /// Constructs the generator, registered under id `stub.v1`.
    pub fn new() -> Result<StubGenerator> {
        Ok(StubGenerator {
            id: GeneratorId::new("stub.v1")?,
        })
    }
}

impl Generator for StubGenerator {
    fn id(&self) -> &GeneratorId {
        &self.id
    }

    fn generate(&self, root_seed: u64, params: &[u8], format: &FormatId) -> Result<Vec<Spec>> {
        let config = StubGeneratorConfig::from_bytes(params)?;
        let mut specs = Vec::with_capacity(config.behaviors.len());
        for (i, behavior) in config.behaviors.iter().enumerate() {
            // Per-candidate nonce from the project PRNG (the `rand` crate is
            // barred from result-affecting paths): distinct candidates, and
            // specs that change with the seed.
            let program = StubProgram {
                behavior: *behavior,
                nonce: prng::derive(root_seed, i as u64),
            };
            specs.push(Spec {
                format: format.clone(),
                bytes: program.to_bytes(),
            });
        }
        Ok(specs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::{Error, to_hex};

    fn config(behaviors: Vec<StubBehavior>) -> Vec<u8> {
        StubGeneratorConfig { behaviors }.to_bytes()
    }

    #[test]
    fn generator_config_round_trips() -> Result<()> {
        // Pinned per §6: u64 count 2 LE, then Succeed (00), then Panic (02).
        let pinned = StubGeneratorConfig {
            behaviors: vec![StubBehavior::Succeed, StubBehavior::Panic],
        };
        assert_eq!(to_hex(&pinned.to_bytes()), "02000000000000000002");
        for behaviors in [
            Vec::new(),
            vec![StubBehavior::Succeed],
            vec![
                StubBehavior::Flaky(2),
                StubBehavior::Sleep(5),
                StubBehavior::Panic,
            ],
        ] {
            let cfg = StubGeneratorConfig { behaviors };
            assert_eq!(StubGeneratorConfig::from_bytes(&cfg.to_bytes())?, cfg);
        }
        Ok(())
    }

    #[test]
    fn generator_config_decode_rejects_truncation_and_trailing() {
        let full = config(vec![StubBehavior::Succeed, StubBehavior::Panic]);
        for cut in 0..full.len() {
            assert!(
                matches!(
                    StubGeneratorConfig::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
        let mut trailing = full;
        trailing.push(0);
        assert!(matches!(
            StubGeneratorConfig::from_bytes(&trailing),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn generate_is_deterministic() -> Result<()> {
        let generator = StubGenerator::new()?;
        let format = FormatId::new("stub.v1")?;
        let params = config(vec![StubBehavior::Succeed, StubBehavior::Panic]);
        assert_eq!(
            generator.generate(42, &params, &format)?,
            generator.generate(42, &params, &format)?
        );
        Ok(())
    }

    #[test]
    fn generated_specs_are_distinct() -> Result<()> {
        let generator = StubGenerator::new()?;
        let format = FormatId::new("stub.v1")?;
        let params = config(vec![
            StubBehavior::Succeed,
            StubBehavior::Succeed,
            StubBehavior::Succeed,
        ]);
        let specs = generator.generate(7, &params, &format)?;
        assert_eq!(specs.len(), 3);
        let ids: Vec<_> = specs.iter().map(Spec::id).collect();
        assert_ne!(ids[0], ids[1]);
        assert_ne!(ids[0], ids[2]);
        assert_ne!(ids[1], ids[2]);
        Ok(())
    }

    #[test]
    fn generate_stamps_the_requested_format() -> Result<()> {
        let generator = StubGenerator::new()?;
        let format = FormatId::new("family-a.v1")?;
        let params = config(vec![StubBehavior::Succeed, StubBehavior::Sleep(0)]);
        for spec in generator.generate(1, &params, &format)? {
            assert_eq!(spec.format, format);
        }
        Ok(())
    }

    #[test]
    fn different_root_seed_changes_the_specs() -> Result<()> {
        let generator = StubGenerator::new()?;
        let format = FormatId::new("stub.v1")?;
        let params = config(vec![StubBehavior::Succeed]);
        let a = generator.generate(1, &params, &format)?;
        let b = generator.generate(2, &params, &format)?;
        assert_ne!(a[0].id(), b[0].id());
        Ok(())
    }

    #[test]
    fn empty_config_yields_no_specs() -> Result<()> {
        let generator = StubGenerator::new()?;
        let format = FormatId::new("stub.v1")?;
        assert!(
            generator
                .generate(1, &config(Vec::new()), &format)?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn generate_rejects_malformed_params() -> Result<()> {
        let generator = StubGenerator::new()?;
        let format = FormatId::new("stub.v1")?;
        // A single byte cannot even hold the u64 count prefix.
        assert!(matches!(
            generator.generate(1, &[0xFF], &format),
            Err(Error::Encoding(_))
        ));
        Ok(())
    }
}
