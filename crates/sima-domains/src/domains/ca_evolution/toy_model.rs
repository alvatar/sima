//! A minimal [`CaModel`] used only by tests to prove the generic CA machinery
//! runs with no dependency on any concrete model.

use sima_core::{Dec, Enc, Error, Result, prng};

use super::ignition::seeded_patch;
use super::model::CaModel;
use super::params::CaParams;
use crate::cellular::Grid;
use crate::domains::translate;

/// A one-channel toy model: the genome is a single rate, the ignition a single
/// base value, the generator config a single sampling range.
pub(crate) struct Toy;

/// The toy genome: one rate scalar.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ToyGenome {
    rate: f32,
}

/// The toy ignition: one base value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ToyIgnition {
    base: f64,
}

/// The toy generator config: one `[lo, hi]` sampling range.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ToyGenConfig {
    rate: [f32; 2],
}

impl Toy {
    /// A toy genome with the given rate.
    pub(crate) fn genome(rate: f32) -> ToyGenome {
        ToyGenome { rate }
    }

    /// A toy ignition with the given base value.
    pub(crate) fn ignition(base: f64) -> ToyIgnition {
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
        let mut dec = Dec::new(bytes);
        let rate = dec.f32()?;
        dec.finish()?;
        Ok(ToyGenome { rate })
    }

    fn encode_genome(genome: &ToyGenome) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.f32(genome.rate);
        enc.finish()
    }

    fn uniforms(genome: &ToyGenome, shared: &CaParams) -> Vec<f32> {
        vec![genome.rate, shared.dt()]
    }

    fn ignite(shared: &CaParams, ignition: &ToyIgnition, seed: u64) -> Result<Grid> {
        seeded_patch(
            shared.width(),
            shared.height(),
            Self::CHANNELS,
            &[0.0],
            &[ignition.base],
            4,
            0.0,
            seed,
        )
    }

    fn parse_ignition(table: &toml::Table) -> Result<ToyIgnition> {
        translate::reject_unknown_keys(Self::FORMAT_ID, table, &["base"], "params")?;
        let base = translate::float(table, Self::FORMAT_ID, "params", "base")?;
        Ok(ToyIgnition { base })
    }

    fn encode_ignition(ignition: &ToyIgnition, enc: &mut Enc) {
        enc.f64(ignition.base);
    }

    fn decode_ignition(dec: &mut Dec) -> Result<ToyIgnition> {
        Ok(ToyIgnition { base: dec.f64()? })
    }

    fn parse_gen_config(table: &toml::Table) -> Result<ToyGenConfig> {
        translate::reject_unknown_keys(Self::FORMAT_ID, table, &["rate"], "generator")?;
        let rate = translate::range(table, Self::FORMAT_ID, "rate")?;
        if rate[0] > rate[1] {
            return Err(Error::Validation(format!(
                "toy generator rate range must satisfy lo <= hi, got {rate:?}"
            )));
        }
        Ok(ToyGenConfig { rate })
    }

    fn encode_gen_config(cfg: &ToyGenConfig) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.f32(cfg.rate[0]).f32(cfg.rate[1]);
        enc.finish()
    }

    fn decode_gen_config(bytes: &[u8]) -> Result<ToyGenConfig> {
        let mut dec = Dec::new(bytes);
        let rate = [dec.f32()?, dec.f32()?];
        dec.finish()?;
        Ok(ToyGenConfig { rate })
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
