//! The asynchronous Neural Cellular Automaton: a grid of cells that update
//! their state channels by a small learned network, stochastically and out of
//! phase, with the update mask keyed on the absolute step the harness supplies.
//!
//! [`NcaGenome`] is the spec payload (the flat network weight vector),
//! [`NcaIgnition`] the model's slice of `[run.params]` (the centered seeded
//! patch), and [`NcaGenConfig`] the sampling box. The model binds them to the
//! generic machinery through [`CaModel`](super::super::model::CaModel), with the
//! asynchronous update kernel co-located in `nca.wgsl` and composed on top of the
//! shared WGSL PRNG. The model is stepped: its kernel reads the per-step index
//! from the harness and its committed state frames that step ahead of the grid.

mod gen_config;
mod genome;
mod ignition;

use sima_core::Result;

use super::super::model::CaModel;
use super::super::params::CaParams;
use crate::substrates::cellular::Grid;

use gen_config::NcaGenConfig;
use genome::NcaGenome;
use ignition::NcaIgnition;

/// State channels the network reads and writes. Every grid channel is network
/// state; the asynchronous update's phase travels as the harness step index, not
/// a grid channel.
pub(crate) const C_STATE: usize = 8;
/// Learned 3×3 depthwise perception filters, each applied to every state
/// channel.
pub(crate) const P: usize = 3;
/// Hidden units in the update network's single hidden layer.
pub(crate) const H: usize = 32;
/// Grid channels per cell: exactly the `C_STATE` network state channels.
pub(crate) const CHANNELS: u32 = C_STATE as u32;

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
    // Channel 0 is the visible state channel; a cell is alive where it rises
    // meaningfully above zero.
    const ALIVE_CHANNEL: u32 = 0;
    const ALIVE_MIN: f32 = 0.1;
    // The kernel is the shared WGSL PRNG snippet composed with the async update
    // kernel. `concat!` needs string literals, and `include_str!` expands to one
    // only within this crate — the reason both files live in `sima-domains`. The
    // kernel's source digest covers both, which is correct: both determine the
    // compiled SPIR-V.
    const KERNEL_SOURCE: &'static str = concat!(
        include_str!("../../../../substrates/cellular/wgsl/shaders/prng.wgsl"),
        include_str!("nca.wgsl"),
    );
    // The kernel reads the candidate seed at runtime for the async mask, so the
    // executor binds it as the binding-4 seed buffer.
    const SEED_BUFFER: bool = true;
    // The mask is keyed on the absolute step, so the kernel reads the per-step
    // index from the binding-5 step buffer and the committed state frames that
    // step ahead of the grid.
    const STEPPED: bool = true;

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

    use sima_contracts::Domain;

    use super::super::super::domain::CaDomain;
    use super::super::super::params::CaParams;
    use super::*;
    use crate::substrates::cellular::WgslEngine;

    #[test]
    fn the_kernel_compiles_device_free() {
        // Hosted CI catches a kernel that fails to compile without a device. This
        // also proves the shared PRNG snippet and the update kernel compose into
        // a valid module.
        sima_toolkit_wgsl::check(Nca::KERNEL_SOURCE, "main").expect("kernel compiles");
    }

    #[test]
    fn the_environment_pins_the_kernel_digest() -> Result<()> {
        // build_binding derives the model's environment device-free, hashing the
        // composed kernel source. The kernel component carries that digest, so
        // editing either shader file changes every task key.
        let domain = CaDomain::<Nca, WgslEngine>::new()?;
        assert_eq!(domain.format().as_str(), Nca::FORMAT_ID);
        let components = domain.environment().components();
        assert_eq!(components.len(), 4);
        assert_eq!(components[0].name(), "ca_evolution.nca.executor");
        assert_eq!(components[1].name(), "ca_evolution.nca.kernel");
        assert_eq!(
            *components[1].value(),
            EnvironmentValue::Digest(source_digest(Nca::KERNEL_SOURCE))
        );
        assert_eq!(components[2].name(), "ca_evolution.nca.reduce");
        assert_eq!(components[3].name(), "wgsl.compiler");
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

    /// Executor fixtures driving the real [`CaExecutor<Nca, WgslEngine>`]: the
    /// `on_device` tests run the async kernel through the seed and step buffers
    /// exactly as a live run does, and the device-free tests beside them
    /// exercise the executor's validation of a stepped input state before any
    /// GPU work. Neither touches a store.
    #[cfg(test)]
    mod executor {
        use sima_contracts::{
            Checkpoint, ExecutionContext, Executor, NoCheckpoint, Outcome, STATE_ARTIFACT,
            TaskInput, WorkerId,
        };
        use sima_core::Error;
        use sima_model::{EnvironmentId, FormatId, Params, Spec};

        use super::super::super::super::continuation::{decode_continuation, encode_continuation};
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

        /// A task input with a fixed seed and environment.
        fn input<'a>(
            spec: &'a Spec,
            params: &'a Params,
            input_state: Option<&'a [u8]>,
        ) -> TaskInput<'a> {
            TaskInput {
                spec,
                params,
                seed: 42,
                environment: EnvironmentId::from_hash(hash_bytes(b"env")),
                input_state,
            }
        }

        /// Runs the executor and returns the committed `state` artifact bytes.
        fn run_state(
            exec: &CaExecutor<Nca, WgslEngine>,
            spec: &Spec,
            params: &Params,
            input_state: Option<&[u8]>,
        ) -> Vec<u8> {
            let checkpoint: &dyn Checkpoint = &NoCheckpoint;
            match exec
                .execute(&input(spec, params, input_state), &ctx(), checkpoint)
                .expect("execute")
            {
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

        /// The step and grid the committed `state` bytes frame.
        fn framed(bytes: &[u8]) -> (u64, Grid) {
            decode_continuation(bytes).expect("framed continuation state")
        }

        #[test]
        fn a_malformed_stepped_input_state_is_an_error() {
            // A stepped model decodes its input state as (step, grid); a buffer
            // too short for even the eight-byte step header is Validation before
            // any GPU work.
            let exec = CaExecutor::<Nca, WgslEngine>::new(None).expect("executor");
            let (spec, params) = (spec(0.5), params(50));
            match exec.execute(
                &input(&spec, &params, Some(&[0u8; 4])),
                &ctx(),
                &NoCheckpoint,
            ) {
                Err(Error::Validation(message)) => assert!(
                    message.contains("input state"),
                    "the error names the input state: {message}"
                ),
                other => panic!("expected Validation, got {other:?}"),
            }
        }

        #[test]
        fn a_mismatched_stepped_state_is_an_error() {
            // A well-framed state whose grid is 8x8x8 against 32x32 run params:
            // the header decodes, the grid dimensions do not match, and the error
            // names both triples before any GPU work.
            let exec = CaExecutor::<Nca, WgslEngine>::new(None).expect("executor");
            let (spec, params) = (spec(0.5), params(50));
            let wrong =
                encode_continuation(0, &Grid::new(8, 8, 8, vec![0.0; 8 * 8 * 8]).expect("grid"));
            match exec.execute(&input(&spec, &params, Some(&wrong)), &ctx(), &NoCheckpoint) {
                Err(Error::Validation(message)) => assert!(
                    message.contains("(8, 8, 8)") && message.contains("(32, 32, 8)"),
                    "the error names both dimension triples: {message}"
                ),
                other => panic!("expected Validation, got {other:?}"),
            }
        }

        /// Running the model's kernel needs a real Vulkan device.
        mod on_device {
            use super::*;

            #[test]
            fn repeated_runs_are_byte_identical() {
                // The async mask is deterministic in (seed, cell, step), so two runs
                // of the same task commit byte-identical framed states.
                let exec = CaExecutor::<Nca, WgslEngine>::new(None).expect("executor");
                let (spec, params) = (spec(0.5), params(50));
                let first = run_state(&exec, &spec, &params, None);
                let second = run_state(&exec, &spec, &params, None);
                assert_eq!(first, second);
            }

            #[test]
            fn segment_continuation_is_cadence_invariant() {
                // Ignite + 50 -> A; continue A + 50 -> B; ignite + 100 -> C. The framed
                // step makes the committed state a complete continuation, so B is
                // byte-identical to C: splitting the trajectory at a segment boundary
                // changes nothing.
                let exec = CaExecutor::<Nca, WgslEngine>::new(None).expect("executor");
                let spec = spec(0.5);
                let a = run_state(&exec, &spec, &params(50), None);
                let b = run_state(&exec, &spec, &params(50), Some(&a));
                let c = run_state(&exec, &spec, &params(100), None);
                assert_eq!(b, c, "segmented 50+50 must equal unsegmented 100");
                // A reached step 50, C step 100, each over an 8-channel grid.
                let (a_step, a_grid) = framed(&a);
                let (c_step, c_grid) = framed(&c);
                assert_eq!(a_step, 50, "segment A reached step 50");
                assert_eq!(c_step, 100, "the whole run reached step 100");
                assert_eq!(a_grid.channels(), 8);
                assert_eq!(c_grid.channels(), 8);
            }

            #[test]
            fn a_smoke_run_yields_a_finite_grid() {
                // A small scale keeps the residual dynamics bounded over a few steps,
                // so every committed value is finite; the framed step equals the step
                // count over an 8-channel grid.
                let exec = CaExecutor::<Nca, WgslEngine>::new(None).expect("executor");
                let steps = 8;
                let bytes = run_state(&exec, &spec(0.02), &params(steps), None);
                let (step, grid) = framed(&bytes);
                assert_eq!(
                    step,
                    u64::from(steps),
                    "the framed step equals the step count"
                );
                assert_eq!((grid.width(), grid.height(), grid.channels()), (32, 32, 8));
                for &value in grid.data() {
                    assert!(value.is_finite(), "committed value {value} must be finite");
                }
            }
        }
    }
}
