//! [`GrayScott`]: the reaction-diffusion rule, bound to the generic CA
//! machinery through [`CaModel`].

use super::{GrayScottGenConfig, GrayScottGenome, GrayScottIgnition};
use crate::domains::ca_evolution::ignition::{PatchSpec, seeded_patch};
use crate::domains::ca_evolution::model::CaModel;
use crate::domains::ca_evolution::params::CaParams;
use crate::substrates::cellular::Grid;
use sima_core::Result;

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
    // The V field (channel 1) carries the pattern; a cell is alive where V rises
    // meaningfully above the near-zero background.
    const ALIVE_CHANNEL: u32 = 1;
    const ALIVE_MIN: f32 = 0.1;
    const KERNEL_SOURCE: &'static str = include_str!("gray_scott.wgsl");

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
            PatchSpec {
                background: &[1.0, 0.0],
                patch: &[ignition.base_u(), ignition.base_v()],
                side_divisor: ignition.side_divisor(),
                noise: ignition.noise_width(),
            },
            seed,
        )
    }

    fn sample(cfg: &GrayScottGenConfig, seed: u64, index: u64) -> GrayScottGenome {
        cfg.sample(seed, index)
    }
}

#[cfg(test)]
mod tests {
    use sima_model::EnvironmentValue;
    use sima_toolkit_wgsl::source_digest;

    use sima_contracts::Domain;

    use super::*;
    use crate::domains::ca_evolution::domain::CaDomain;
    use crate::substrates::cellular::WgslEngine;

    #[test]
    fn the_kernel_compiles_device_free() {
        // Hosted CI catches a kernel that fails to compile without a device.
        sima_toolkit_wgsl::check(GrayScott::KERNEL_SOURCE, "main").expect("kernel compiles");
    }

    #[test]
    fn the_environment_pins_the_kernel_digest() -> Result<()> {
        // build_binding derives the model's environment device-free, hashing the
        // kernel source rather than compiling it. The kernel component carries
        // that digest, so editing the shader changes every task key.
        let domain = CaDomain::<GrayScott, WgslEngine>::new()?;
        assert_eq!(domain.format().as_str(), GrayScott::FORMAT_ID);
        let components = domain.environment().components();
        assert_eq!(components.len(), 4);
        assert_eq!(components[0].name(), "ca_evolution.gray_scott.executor");
        assert_eq!(components[1].name(), "ca_evolution.gray_scott.kernel");
        assert_eq!(
            *components[1].value(),
            EnvironmentValue::Digest(source_digest(GrayScott::KERNEL_SOURCE))
        );
        assert_eq!(components[2].name(), "ca_evolution.gray_scott.reduce");
        assert_eq!(components[3].name(), "wgsl.compiler");
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
