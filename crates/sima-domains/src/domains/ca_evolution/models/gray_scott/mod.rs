//! The Gray-Scott reaction-diffusion model: a two-chemical CA on a 2D grid whose
//! candidates are the four evolvable scalars of its update rule.
//!
//! [`GrayScottGenome`] is the spec payload (feed, kill, and the two diffusion
//! rates), [`GrayScottIgnition`] the model's slice of `[run.params]` (the former
//! patch), and [`GrayScottGenConfig`] the sampling box. [`GrayScott`] binds them
//! to the generic machinery through [`CaModel`], with the reaction-diffusion
//! kernel co-located in `gray_scott.wgsl`.

mod gen_config;
mod genome;
mod ignition;

use sima_core::{Dec, Enc, Result};

use super::super::ignition::seeded_patch;
use super::super::model::CaModel;
use super::super::params::CaParams;
use crate::cellular::Grid;

use gen_config::GrayScottGenConfig;
use genome::GrayScottGenome;
use ignition::GrayScottIgnition;

/// The Gray-Scott reaction-diffusion model. Zero-sized: the generic machinery is
/// monomorphized over it, and every rule-specific value is a genome, ignition,
/// or config the methods below produce.
pub(crate) struct GrayScott;

impl CaModel for GrayScott {
    type Genome = GrayScottGenome;
    type Ignition = GrayScottIgnition;
    type GenConfig = GrayScottGenConfig;

    const FORMAT_ID: &'static str = "ca_evolution.gray_scott.v1";
    const NAME: &'static str = "ca_evolution.gray_scott";
    const VERSION: &'static str = "v1";
    const CHANNELS: u32 = 2;
    const KERNEL_WGSL: &'static str = include_str!("gray_scott.wgsl");

    fn decode_genome(bytes: &[u8]) -> Result<GrayScottGenome> {
        GrayScottGenome::from_bytes(bytes)
    }

    fn encode_genome(genome: &GrayScottGenome) -> Vec<u8> {
        genome.to_bytes()
    }

    fn uniforms(genome: &GrayScottGenome, shared: &CaParams) -> Vec<f32> {
        // Binding 3 of the cellular convention: [f, k, du, dv, dt].
        vec![
            genome.feed(),
            genome.kill(),
            genome.diffusion_u(),
            genome.diffusion_v(),
            shared.dt(),
        ]
    }

    fn ignite(shared: &CaParams, ignition: &GrayScottIgnition, seed: u64) -> Result<Grid> {
        // Gray-Scott ignites from the fixed point (u, v) = (1, 0) with a centered
        // noisy patch of the ignition's base values.
        seeded_patch(
            shared.width(),
            shared.height(),
            Self::CHANNELS,
            &[1.0, 0.0],
            &[ignition.base_u(), ignition.base_v()],
            ignition.side_divisor(),
            ignition.noise_width(),
            seed,
        )
    }

    fn parse_ignition(table: &toml::Table) -> Result<GrayScottIgnition> {
        GrayScottIgnition::parse(table, Self::FORMAT_ID)
    }

    fn encode_ignition(ignition: &GrayScottIgnition, enc: &mut Enc) {
        ignition.encode(enc);
    }

    fn decode_ignition(dec: &mut Dec) -> Result<GrayScottIgnition> {
        GrayScottIgnition::decode(dec)
    }

    fn parse_gen_config(table: &toml::Table) -> Result<GrayScottGenConfig> {
        GrayScottGenConfig::parse(table, Self::FORMAT_ID)
    }

    fn encode_gen_config(cfg: &GrayScottGenConfig) -> Vec<u8> {
        cfg.to_bytes()
    }

    fn decode_gen_config(bytes: &[u8]) -> Result<GrayScottGenConfig> {
        GrayScottGenConfig::from_bytes(bytes)
    }

    fn sample(cfg: &GrayScottGenConfig, seed: u64, index: u64) -> GrayScottGenome {
        cfg.sample(seed, index)
    }
}

#[cfg(test)]
mod tests {
    use sima_model::EnvironmentValue;
    use sima_toolkit_wgsl::source_digest;

    use super::super::super::domain::build_domain;
    use super::*;

    #[test]
    fn the_kernel_compiles_device_free() {
        // Hosted CI catches a kernel that fails to compile without a device.
        sima_toolkit_wgsl::check(GrayScott::KERNEL_WGSL, "main").expect("kernel compiles");
    }

    #[test]
    fn the_environment_pins_the_kernel_digest() -> Result<()> {
        // build_domain derives the model's environment device-free, hashing the
        // kernel source rather than compiling it. The kernel component carries
        // that digest, so editing the shader changes every task key.
        let domain = build_domain::<GrayScott>()?;
        assert_eq!(domain.format.as_str(), GrayScott::FORMAT_ID);
        let components = domain.environment.components();
        assert_eq!(components.len(), 3);
        assert_eq!(components[0].name(), "ca_evolution.gray_scott.executor");
        assert_eq!(components[1].name(), "ca_evolution.gray_scott.kernel");
        assert_eq!(
            *components[1].value(),
            EnvironmentValue::Digest(source_digest(GrayScott::KERNEL_WGSL))
        );
        assert_eq!(components[2].name(), "wgsl.compiler");
        Ok(())
    }

    #[test]
    fn uniforms_pack_the_rates_then_dt() -> Result<()> {
        let genome = GrayScottGenome::new(0.055, 0.062, 0.16, 0.08)?;
        let shared = CaParams::new(64, 64, 100, 1.0)?;
        assert_eq!(
            GrayScott::uniforms(&genome, &shared),
            vec![0.055, 0.062, 0.16, 0.08, 1.0]
        );
        Ok(())
    }
}
