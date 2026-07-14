//! The asynchronous Neural Cellular Automaton: a grid of cells that update
//! their state channels by a small learned network, stochastically and out of
//! phase, with the update mask keyed on a clock channel carried inside the grid.
//!
//! [`NcaGenome`] is the spec payload (the flat network weight vector),
//! [`NcaIgnition`] the model's slice of `[run.params]` (the centered seeded
//! patch), and [`NcaGenConfig`] the sampling box. The model binds them to the
//! generic machinery through [`CaModel`](super::super::model::CaModel), with the
//! asynchronous update kernel co-located in `nca.wgsl` and composed on top of the
//! shared WGSL PRNG.

mod gen_config;
mod genome;
mod ignition;

use sima_core::Result;

use super::super::model::CaModel;
use super::super::params::CaParams;
use crate::cellular::Grid;

use gen_config::NcaGenConfig;
use genome::NcaGenome;
use ignition::NcaIgnition;

/// State channels the network reads and writes. Channels `0..C_STATE` of the
/// grid are network state; the channel after them is the clock.
pub(crate) const C_STATE: usize = 8;
/// Learned 3×3 depthwise perception filters, each applied to every state
/// channel.
pub(crate) const P: usize = 3;
/// Hidden units in the update network's single hidden layer.
pub(crate) const H: usize = 32;
/// Grid channels per cell: the `C_STATE` network state channels plus one clock
/// channel (index `C_STATE`) that carries the asynchronous update's phase, so a
/// committed grid is a complete continuation.
pub(crate) const CHANNELS: u32 = C_STATE as u32 + 1;

/// The asynchronous Neural CA model. Zero-sized: the generic machinery is
/// monomorphized over it, and every rule-specific value is a genome, ignition,
/// or config the methods below produce.
pub(crate) struct Nca;

impl CaModel for Nca {
    type Genome = NcaGenome;
    type Ignition = NcaIgnition;
    type GenConfig = NcaGenConfig;

    const FORMAT_ID: &'static str = "ca_evolution.nca.v1";
    const NAME: &'static str = "ca_evolution.nca";
    const VERSION: &'static str = "v1";
    const CHANNELS: u32 = CHANNELS;
    // The kernel is the shared WGSL PRNG snippet composed with the async update
    // kernel. `concat!` needs string literals, and `include_str!` expands to one
    // only within this crate — the reason both files live in `sima-domains`. The
    // kernel's source digest covers both, which is correct: both determine the
    // compiled SPIR-V.
    const KERNEL_WGSL: &'static str = concat!(
        include_str!("../../../../cellular/shaders/prng.wgsl"),
        include_str!("nca.wgsl"),
    );
    // The kernel reads the candidate seed at runtime for the async mask, so the
    // executor binds it as the binding-4 seed buffer.
    const SEED_BUFFER: bool = true;

    fn uniforms(genome: &NcaGenome, shared: &CaParams) -> Vec<f32> {
        // Binding 3 of the cellular convention: [dt, then the N genome weights].
        let weights = genome.weights();
        let mut buffer = Vec::with_capacity(1 + weights.len());
        buffer.push(shared.dt());
        buffer.extend_from_slice(weights);
        buffer
    }

    fn ignite(shared: &CaParams, ignition: &NcaIgnition, seed: u64) -> Result<Grid> {
        ignition.ignite(shared.width(), shared.height(), seed)
    }

    fn sample(cfg: &NcaGenConfig, seed: u64, index: u64) -> NcaGenome {
        cfg.sample(seed, index)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::{Codec, Result, hash_bytes};
    use sima_model::EnvironmentValue;
    use sima_toolkit_wgsl::source_digest;

    use super::super::super::domain::build_domain;
    use super::super::super::params::CaParams;
    use super::*;

    #[test]
    fn the_kernel_compiles_device_free() {
        // Hosted CI catches a kernel that fails to compile without a device. This
        // also proves the shared PRNG snippet and the update kernel compose into
        // a valid module.
        sima_toolkit_wgsl::check(Nca::KERNEL_WGSL, "main").expect("kernel compiles");
    }

    #[test]
    fn the_environment_pins_the_kernel_digest() -> Result<()> {
        // build_domain derives the model's environment device-free, hashing the
        // composed kernel source. The kernel component carries that digest, so
        // editing either shader file changes every task key.
        let domain = build_domain::<Nca>()?;
        assert_eq!(domain.format.as_str(), Nca::FORMAT_ID);
        let components = domain.environment.components();
        assert_eq!(components.len(), 3);
        assert_eq!(components[0].name(), "ca_evolution.nca.executor");
        assert_eq!(components[1].name(), "ca_evolution.nca.kernel");
        assert_eq!(
            *components[1].value(),
            EnvironmentValue::Digest(source_digest(Nca::KERNEL_WGSL))
        );
        assert_eq!(components[2].name(), "wgsl.compiler");
        Ok(())
    }

    #[test]
    fn uniforms_pack_dt_then_weights() -> Result<()> {
        let genome = Nca::sample(&NcaGenConfig::new(0.5)?, 1, 0);
        let shared = CaParams::new(32, 32, 100, 1.0)?;
        let uniforms = Nca::uniforms(&genome, &shared);
        // dt then the N=1091 weights: length 1092.
        assert_eq!(uniforms.len(), 1 + genome.weights().len());
        assert_eq!(uniforms.len(), 1092);
        assert_eq!(uniforms[0], shared.dt());
        assert_eq!(&uniforms[1..], genome.weights());
        Ok(())
    }

    /// GPU executor fixtures: they drive the real [`CaExecutor<Nca>`] end to end
    /// so the seed-buffer binding and the async kernel run exactly as in a live
    /// run, without touching any store.
    #[cfg(test)]
    mod gpu {
        use sima_contracts::{
            Checkpoint, ExecutionContext, Executor, NoCheckpoint, Outcome, STATE_ARTIFACT,
            TaskInput, WorkerId,
        };
        use sima_model::{EnvironmentId, FormatId, Params, Spec};

        use super::super::super::super::executor::CaExecutor;
        use super::super::super::super::params::encode_params;
        use super::*;

        /// A spec whose genome is candidate 0 sampled at the given scale.
        fn spec(weight_scale: f32) -> Spec {
            let genome = Nca::sample(
                &NcaGenConfig::new(weight_scale).expect("valid scale"),
                42,
                0,
            );
            Spec {
                format: FormatId::new(Nca::FORMAT_ID).expect("valid format id"),
                bytes: genome.to_bytes(),
            }
        }

        /// Well-formed run params on a 32x32 grid over `steps`, noiseless ignition.
        fn params(steps: u32) -> Params {
            Params {
                bytes: encode_params::<Nca>(
                    &CaParams::new(32, 32, steps, 1.0).expect("valid params"),
                    &NcaIgnition::new(1.0, 8, 0.0).expect("valid ignition"),
                ),
            }
        }

        fn ctx() -> ExecutionContext {
            ExecutionContext {
                attempt: 0,
                worker: WorkerId(0),
            }
        }

        /// Runs the executor and returns the committed `state` artifact bytes.
        fn run_state(
            exec: &CaExecutor<Nca>,
            spec: &Spec,
            params: &Params,
            input_state: Option<&[u8]>,
        ) -> Vec<u8> {
            let input = TaskInput {
                spec,
                params,
                seed: 42,
                environment: EnvironmentId::from_hash(hash_bytes(b"env")),
                input_state,
            };
            let checkpoint: &dyn Checkpoint = &NoCheckpoint;
            match exec.execute(&input, &ctx(), checkpoint).expect("execute") {
                Outcome::Completed { artifacts, .. } => {
                    artifacts
                        .into_iter()
                        .find(|a| a.name == STATE_ARTIFACT)
                        .expect("a state artifact")
                        .bytes
                }
                other => panic!("expected Completed, got {other:?}"),
            }
        }

        /// Asserts every cell's clock channel (the channel after the state
        /// channels) equals `expected`.
        fn assert_clock(grid: &Grid, expected: f32) {
            let data = grid.data();
            let stride = CHANNELS as usize;
            let cells = (grid.width() * grid.height()) as usize;
            for cell in 0..cells {
                assert_eq!(
                    data[cell * stride + C_STATE],
                    expected,
                    "clock at cell {cell} must be {expected}"
                );
            }
        }

        /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
        #[test]
        #[ignore = "requires a Vulkan device"]
        fn repeated_runs_are_byte_identical() {
            // The async mask is deterministic in (seed, cell, clock), so two runs
            // of the same task commit byte-identical grids.
            let exec = CaExecutor::<Nca>::new().expect("executor");
            let (spec, params) = (spec(0.5), params(50));
            let first = run_state(&exec, &spec, &params, None);
            let second = run_state(&exec, &spec, &params, None);
            assert_eq!(first, second);
        }

        /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
        #[test]
        #[ignore = "requires a Vulkan device"]
        fn segment_continuation_is_cadence_invariant() {
            // Ignite + 50 -> A; continue A + 50 -> B; ignite + 100 -> C. The clock
            // channel makes the grid a complete continuation, so B is byte-identical
            // to C: splitting the trajectory at a segment boundary changes nothing.
            let exec = CaExecutor::<Nca>::new().expect("executor");
            let spec = spec(0.5);
            let a = run_state(&exec, &spec, &params(50), None);
            let b = run_state(&exec, &spec, &params(50), Some(&a));
            let c = run_state(&exec, &spec, &params(100), None);
            assert_eq!(b, c, "segmented 50+50 must equal unsegmented 100");
            // A carries clock 50 everywhere; C carries clock 100.
            assert_clock(&Grid::from_bytes(&a).expect("grid A"), 50.0);
            assert_clock(&Grid::from_bytes(&c).expect("grid C"), 100.0);
        }

        /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
        #[test]
        #[ignore = "requires a Vulkan device"]
        fn a_smoke_run_yields_a_finite_clocked_grid() {
            // A small scale keeps the residual dynamics bounded over a few steps,
            // so every committed value is finite; the clock equals the step count.
            let exec = CaExecutor::<Nca>::new().expect("executor");
            let steps = 8;
            let bytes = run_state(&exec, &spec(0.02), &params(steps), None);
            let grid = Grid::from_bytes(&bytes).expect("grid");
            assert_eq!((grid.width(), grid.height(), grid.channels()), (32, 32, 9));
            for &value in grid.data() {
                assert!(value.is_finite(), "committed value {value} must be finite");
            }
            assert_clock(&grid, steps as f32);
        }
    }
}
