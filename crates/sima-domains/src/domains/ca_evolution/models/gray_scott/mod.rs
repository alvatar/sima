//! The Gray-Scott reaction-diffusion model: a two-chemical CA on a 2D grid whose
//! candidates are the four evolvable scalars of its update rule.
//!
//! [`GrayScottGenome`] is the spec payload (feed, kill, and the two diffusion
//! rates), [`GrayScottIgnition`] the model's slice of `[run.params]` (the
//! centered seeded patch over the fixed point), and [`GrayScottGenConfig`] the
//! sampling box. [`GrayScott`] binds them to the generic machinery through
//! [`CaModel`], with the reaction-diffusion kernel co-located in
//! `gray_scott.wgsl`.

mod gen_config;
mod genome;
mod ignition;

use sima_core::Result;

use super::super::ignition::{PatchSpec, seeded_patch};
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

    /// Executor fixtures driving the real [`CaExecutor<GrayScott>`] on the GPU:
    /// the bare-grid model commits the grid alone, and its committed stats
    /// summarize that grid. Touches no store.
    #[cfg(test)]
    mod executor {
        use sima_contracts::{
            ExecutionContext, Executor, NoCheckpoint, Outcome, STATE_ARTIFACT, TaskInput, WorkerId,
        };
        use sima_core::{Codec, hash_bytes};
        use sima_model::{EnvironmentId, FormatId, Params, Spec};

        use super::super::super::super::executor::CaExecutor;
        use super::super::super::super::params::encode_params;
        use super::super::super::super::stats::grid_stats;
        use super::*;

        /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
        #[test]
        #[ignore = "requires a Vulkan device"]
        fn stats_summarize_the_committed_grid() {
            // A bare-grid model commits the grid alone, so its stats are
            // `grid_stats` of the grid the committed bytes decode to. This pins the
            // executor wiring for a non-stepped model.
            let exec = CaExecutor::<GrayScott>::new().expect("executor");
            let spec = Spec {
                format: FormatId::new(GrayScott::FORMAT_ID).expect("format id"),
                bytes: GrayScottGenome::new(0.055, 0.062, 0.16, 0.08)
                    .expect("genome")
                    .to_bytes(),
            };
            let params = Params {
                bytes: encode_params::<GrayScott>(
                    &CaParams::new(32, 32, 16, 1.0).expect("params"),
                    &GrayScottIgnition::new(0.5, 0.25, 8, 0.02).expect("ignition"),
                ),
            };
            let input = TaskInput {
                spec: &spec,
                params: &params,
                seed: 42,
                environment: EnvironmentId::from_hash(hash_bytes(b"env")),
                input_state: None,
            };
            let ctx = ExecutionContext {
                attempt: 0,
                worker: WorkerId(0),
            };
            match exec.execute(&input, &ctx, &NoCheckpoint).expect("execute") {
                Outcome::Completed { artifacts, stats } => {
                    let state = artifacts
                        .iter()
                        .find(|a| a.name == STATE_ARTIFACT)
                        .expect("a state artifact");
                    let grid = Grid::from_bytes(&state.bytes).expect("grid");
                    assert_eq!(stats.bytes, grid_stats(&grid));
                    assert!(!stats.bytes.is_empty(), "the stats channel is filled");
                }
                other => panic!("expected Completed, got {other:?}"),
            }
        }
    }
}
