//! A minimal [`CaModel`] used only by tests to prove the generic CA machinery
//! runs with no dependency on any concrete model.

use sima_core::{Codec, Dec, Enc, Error, Result, TomlConfig, prng};

use super::ignition::{PatchSpec, seeded_patch};
use super::model::CaModel;
use super::params::CaParams;
use crate::cellular::Grid;

/// A one-channel toy model: the genome is a single rate, the ignition a single
/// base value, the generator config a single sampling range.
pub(crate) struct Toy;

/// The toy genome: one rate scalar.
#[derive(Debug, Clone, Copy, PartialEq, Codec)]
#[codec(validate = new)]
pub(crate) struct ToyGenome {
    rate: f32,
}

/// The toy ignition: one base value.
#[derive(Debug, Clone, Copy, PartialEq, Codec, TomlConfig)]
#[codec(validate = new)]
#[toml(validate = new)]
pub(crate) struct ToyIgnition {
    base: f32,
}

/// The toy generator config: one `[lo, hi]` sampling range.
#[derive(Debug, Clone, Copy, PartialEq, Codec, TomlConfig)]
#[codec(validate = new)]
#[toml(validate = new)]
pub(crate) struct ToyGenConfig {
    rate: [f32; 2],
}

impl ToyGenome {
    /// Builds a toy genome. The toy rate carries no validation rule; `new`
    /// exists so decode routes through it, like every model genome.
    pub(crate) fn new(rate: f32) -> Result<ToyGenome> {
        Ok(ToyGenome { rate })
    }
}

impl ToyIgnition {
    /// Builds a toy ignition. The toy base carries no validation rule.
    pub(crate) fn new(base: f32) -> Result<ToyIgnition> {
        Ok(ToyIgnition { base })
    }
}

impl ToyGenConfig {
    /// Builds a toy generator config, validating `lo <= hi`.
    pub(crate) fn new(rate: [f32; 2]) -> Result<ToyGenConfig> {
        if rate[0] > rate[1] {
            return Err(Error::Validation(format!(
                "toy generator rate range must satisfy lo <= hi, got {rate:?}"
            )));
        }
        Ok(ToyGenConfig { rate })
    }
}

impl Toy {
    /// A toy genome with the given rate.
    pub(crate) fn genome(rate: f32) -> ToyGenome {
        ToyGenome { rate }
    }

    /// A toy ignition with the given base value.
    pub(crate) fn ignition(base: f32) -> ToyIgnition {
        ToyIgnition { base }
    }

    /// A toy generator config with the given sampling range.
    pub(crate) fn gen_config(rate: [f32; 2]) -> ToyGenConfig {
        ToyGenConfig { rate }
    }
}

impl CaModel for Toy {
    type Genome = ToyGenome;
    type Ignition = ToyIgnition;
    type GenConfig = ToyGenConfig;

    const FORMAT_ID: &'static str = "toy.v1";
    const NAME: &'static str = "toy";
    const VERSION: &'static str = "v1";
    const CHANNELS: u32 = 1;
    const KERNEL_WGSL: &'static str = "// toy kernel";

    fn decode_genome(bytes: &[u8]) -> Result<ToyGenome> {
        ToyGenome::from_bytes(bytes)
    }

    fn encode_genome(genome: &ToyGenome) -> Vec<u8> {
        genome.to_bytes()
    }

    fn uniforms(genome: &ToyGenome, shared: &CaParams) -> Vec<f32> {
        vec![genome.rate, shared.dt()]
    }

    fn ignite(shared: &CaParams, ignition: &ToyIgnition, seed: u64) -> Result<Grid> {
        seeded_patch(
            shared.width(),
            shared.height(),
            Self::CHANNELS,
            PatchSpec {
                background: &[0.0],
                patch: &[ignition.base],
                side_divisor: 4,
                noise: 0.0,
            },
            seed,
        )
    }

    fn parse_ignition(table: &toml::Table) -> Result<ToyIgnition> {
        ToyIgnition::parse(table, Self::FORMAT_ID, "params")
    }

    fn encode_ignition(ignition: &ToyIgnition, enc: &mut Enc) {
        ignition.encode(enc);
    }

    fn decode_ignition(dec: &mut Dec) -> Result<ToyIgnition> {
        ToyIgnition::decode(dec)
    }

    fn parse_gen_config(table: &toml::Table) -> Result<ToyGenConfig> {
        ToyGenConfig::parse(table, Self::FORMAT_ID, "generator")
    }

    fn encode_gen_config(cfg: &ToyGenConfig) -> Vec<u8> {
        cfg.to_bytes()
    }

    fn decode_gen_config(bytes: &[u8]) -> Result<ToyGenConfig> {
        ToyGenConfig::from_bytes(bytes)
    }

    fn sample(cfg: &ToyGenConfig, seed: u64, index: u64) -> ToyGenome {
        let s = prng::derive(seed, index);
        let t = prng::unit_f64(prng::next(s, 0)) as f32;
        let [lo, hi] = cfg.rate;
        ToyGenome {
            rate: lo + t * (hi - lo),
        }
    }
}

/// The genericity lock: the generic CA machinery runs end to end over the toy
/// model, with no dependency on Gray-Scott. Together with the toy-driven tests
/// in `params`, `generator`, and `executor`, this proves the domain is
/// model-agnostic — a second model plugs in by implementing [`CaModel`] alone.
#[cfg(test)]
mod tests {
    use sima_contracts::Generator;
    use sima_model::FormatId;

    use super::super::domain::build_domain;
    use super::super::generator::{CaGenerator, translate as translate_generator};
    use super::super::params::{decode_params, translate as translate_params};
    use super::*;

    #[test]
    fn the_environment_names_derive_from_the_model() -> Result<()> {
        // build_domain forms the component names from M::NAME, so a different
        // model yields different names with no change to the builder.
        let domain = build_domain::<Toy>()?;
        assert_eq!(domain.format.as_str(), "toy.v1");
        let names: Vec<&str> = domain
            .environment
            .components()
            .iter()
            .map(|c| c.name())
            .collect();
        assert_eq!(names, ["toy.executor", "toy.kernel", "wgsl.compiler"]);
        Ok(())
    }

    #[test]
    fn the_genome_codec_round_trips() -> Result<()> {
        let genome = Toy::genome(0.5);
        assert_eq!(Toy::decode_genome(&Toy::encode_genome(&genome))?, genome);
        // Trailing bytes are rejected.
        let mut bytes = Toy::encode_genome(&genome);
        bytes.push(0);
        assert!(matches!(
            Toy::decode_genome(&bytes),
            Err(Error::Encoding(_))
        ));
        Ok(())
    }

    #[test]
    fn the_spine_runs_over_a_non_gray_scott_model() -> Result<()> {
        // Params translation: a full toy `[run.params]` table becomes a blob that
        // decodes back to the shared fields and the toy ignition.
        let params_table: toml::Table = "width = 8\nheight = 8\nsteps = 4\ndt = 1.0\nbase = 0.5"
            .parse()
            .expect("parse params table");
        let blob = translate_params::<Toy>(&params_table)?.bytes;
        let (shared, ignition) = decode_params::<Toy>(&blob)?;
        assert_eq!((shared.width(), shared.height(), shared.steps()), (8, 8, 4));
        assert_eq!(ignition, Toy::ignition(0.5));

        // Generator translation and sampling: three distinct candidates, each a
        // toy spec stamped with the requested format.
        let gen_table: toml::Table = "count = 3\nrate = [0.01, 0.08]"
            .parse()
            .expect("parse generator table");
        let gen_blob = translate_generator::<Toy>(&gen_table)?;
        let format = FormatId::new(Toy::FORMAT_ID)?;
        let specs = CaGenerator::<Toy>::new()?.generate(42, &gen_blob, &format)?;
        assert_eq!(specs.len(), 3);
        for spec in &specs {
            assert_eq!(spec.format, format);
            Toy::decode_genome(&spec.bytes)?;
        }
        // Distinct candidates: the three specs are pairwise different.
        assert_ne!(specs[0].bytes, specs[1].bytes);
        assert_ne!(specs[1].bytes, specs[2].bytes);
        Ok(())
    }
}
