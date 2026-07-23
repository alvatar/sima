//! [`CaExecutor<M>`]: evaluates a CA candidate on the GPU for the model `M`.

use std::marker::PhantomData;
use std::sync::Mutex;

use sima_contracts::{
    Artifact, Checkpoint, DeviceBinding, ExecutionContext, Executor, Outcome, STATE_ARTIFACT,
    Stats, TaskInput,
};
use sima_core::{Codec, Error, Result};
use sima_model::FormatId;
use sima_toolkit_wgsl::{Buffer, Context, Kernel};

use super::continuation::{decode_continuation, encode_continuation};
use super::model::CaModel;
use super::params::decode_params;
use crate::cellular::{Grid, GridPair, ReduceKernels, reduce, run};

/// Evaluates a candidate of the model `M` on the GPU, under format
/// `M::FORMAT_ID`: the spec's genome and the run params frame one task — ignite
/// (or continue) a grid, advance it `steps` kernel dispatches, reduce the final
/// grid pair into the observational stat scalars, and commit the final state as
/// the `state` artifact. A bare-grid model commits the grid's canonical bytes; a
/// stepped model commits framed continuation state, the reached step ahead of
/// the grid.
///
/// The GPU engine is created lazily on the first execute, never at construction,
/// so [`build_domain`](super::domain::build_domain) stays device-free —
/// orchestrate calls it before any store mutation, and unit tests run with no
/// GPU. A `Mutex` serializes the GPU section: the scheduler runs `workers`
/// threads calling `execute` on one shared executor, and a single GPU serializes
/// the work anyway. The span the lock covers and why it is required are
/// documented inline at the lock site.
///
/// The checkpoint channel goes unused: the harness performs all `steps`
/// dispatches in one call and downloads once at the end, so there is no mid-run
/// state to offer, and a killed attempt restarts its segment from the segment's
/// input — bounded by `steps`, which `segments` controls. A config that sets a
/// checkpoint interval simply never gets a save for this model. Ignoring the
/// channel cannot change committed bytes.
pub(crate) struct CaExecutor<M: CaModel> {
    format: FormatId,
    /// The device the engine opens, or `None` for the toolkit's default
    /// selection. Read once, at engine initialization.
    device: Option<DeviceBinding>,
    /// The lazily initialized engine: `None` until the first execute, then a
    /// fully constructed engine for the process's lifetime. A failed
    /// initialization leaves `None`, so a later attempt retries.
    gpu: Mutex<Option<GpuEngine>>,
    /// `M` is used only through its associated items in the methods below, never
    /// stored; `fn() -> M` keeps the executor `Send + Sync` regardless of `M`.
    model: PhantomData<fn() -> M>,
}

/// The device context and the compiled kernels, created together once: the
/// update kernel that advances the grid and the reduction that summarizes it.
struct GpuEngine {
    /// Declared before `context` so they drop first: struct fields drop in
    /// declaration order, and the kernels' pipeline handles belong to the
    /// context's device, so a kernel must be destroyed before the context. A
    /// reorder would drop the device first and segfault at engine drop.
    kernel: Kernel,
    reduce: ReduceKernels,
    context: Context,
}

impl<M: CaModel> CaExecutor<M> {
    /// Constructs the executor for `M::FORMAT_ID` on `device` — or, for `None`,
    /// on the toolkit's default selection — performing no GPU work.
    pub(crate) fn new(device: Option<&DeviceBinding>) -> Result<CaExecutor<M>> {
        Ok(CaExecutor {
            format: FormatId::new(M::FORMAT_ID)?,
            device: device.copied(),
            gpu: Mutex::new(None),
            model: PhantomData,
        })
    }
}

impl<M: CaModel> Executor for CaExecutor<M> {
    fn format(&self) -> &FormatId {
        &self.format
    }

    fn execute(
        &self,
        input: &TaskInput<'_>,
        _ctx: &ExecutionContext,
        _checkpoint: &dyn Checkpoint,
    ) -> Result<Outcome> {
        // Structural validation strictly before any GPU touch: a malformed spec,
        // params, or input state is an identity fault (`Err`), never a candidate
        // failure — and the error paths stay device-free, like the stub's.
        let genome = M::Genome::from_bytes(&input.spec.bytes).map_err(|e| {
            Error::Validation(format!("{} spec is not a valid genome: {e}", M::NAME))
        })?;
        let (shared, ignition) = decode_params::<M>(&input.params.bytes)
            .map_err(|e| Error::Validation(format!("{} params are malformed: {e}", M::NAME)))?;
        // The initial grid and, for a stepped model, the step the trajectory has
        // already reached. A stepped model frames (step, grid) in its committed
        // state; a bare-grid model stores the grid alone and carries no step.
        let (initial, step_base) = match input.input_state {
            // The first segment ignites from the seeded grid at step 0.
            None => (
                M::ignite(&shared, &ignition, input.seed)?,
                M::STEPPED.then_some(0u64),
            ),
            // A successor continues from its predecessor's committed state, which
            // must match the run's dimensions exactly.
            Some(bytes) => {
                let (step, grid) = if M::STEPPED {
                    let (step, grid) = decode_continuation(bytes).map_err(|e| {
                        Error::Validation(format!("{} input state is malformed: {e}", M::NAME))
                    })?;
                    (Some(step), grid)
                } else {
                    let grid = Grid::from_bytes(bytes).map_err(|e| {
                        Error::Validation(format!("{} input state is malformed: {e}", M::NAME))
                    })?;
                    (None, grid)
                };
                let got = (grid.width(), grid.height(), grid.channels());
                let want = (shared.width(), shared.height(), M::CHANNELS);
                if got != want {
                    return Err(Error::Validation(format!(
                        "{} input state dimensions {got:?} do not match the run params {want:?}",
                        M::NAME
                    )));
                }
                (grid, step)
            }
        };
        // The lock spans the whole GPU section — engine init here, then the
        // uniform upload, dispatch loop, and download below — because Vulkan
        // queues and command pools require external synchronization and the
        // worker threads share this one executor. Initializing the engine inside
        // the lock is why `domain_for` needs no device: nothing touches the GPU
        // until the first execute. A poisoned lock is safe to enter: the slot
        // only ever holds None or a fully constructed engine, assigned after
        // construction completes.
        let mut gpu = self
            .gpu
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if gpu.is_none() {
            // The binding names the device to open; without one, the toolkit's
            // default selection applies.
            let context = match self.device {
                Some(device) => {
                    Context::for_device(device.vendor_id, device.device_id, device.member)?
                }
                None => Context::new()?,
            };
            let kernel = context.kernel(M::KERNEL_WGSL, "main")?;
            let reduce = ReduceKernels::build(&context)?;
            *gpu = Some(GpuEngine {
                context,
                kernel,
                reduce,
            });
        }
        let engine = gpu.as_ref().expect("gpu engine initialized above");
        // The model's uniform buffer — binding 3 of the cellular convention,
        // bound after dims.
        let uniforms = M::uniforms(&genome, &shared);
        let uniform_bytes: &[u8] = bytemuck::cast_slice(&uniforms);
        let uniforms_buffer = engine.context.buffer(uniform_bytes.len())?;
        engine.context.upload(&uniforms_buffer, uniform_bytes)?;
        // Binding 4, opted into per model via `M::SEED_BUFFER`: the candidate's
        // u64 seed as two u32 words (low, high). A kernel consuming the seed at
        // runtime — an asynchronous update mask — reads it here; integers must
        // travel as integers, since a driver may rewrite a raw bit pattern
        // parked in an f32 slot. Held in this scope so it outlives the dispatch.
        let seed_buffer = if M::SEED_BUFFER {
            let words = [input.seed as u32, (input.seed >> 32) as u32];
            let seed_bytes: &[u8] = bytemuck::cast_slice(&words);
            let buffer = engine.context.buffer(seed_bytes.len())?;
            engine.context.upload(&buffer, seed_bytes)?;
            Some(buffer)
        } else {
            None
        };
        let mut params: Vec<&Buffer> = vec![&uniforms_buffer];
        if let Some(seed_buffer) = seed_buffer.as_ref() {
            params.push(seed_buffer);
        }
        let trajectory = run(
            &engine.context,
            &engine.kernel,
            &initial,
            shared.steps(),
            &params,
            step_base,
        )?;
        // Observational per-candidate stats, reduced on the GPU over the two
        // resident grids ($G_N$ and $G_{N-1}$) before any readback. They travel
        // the `Stats` channel to the journal and enter no record, manifest, or
        // identity criterion, so a reduction fault degrades to empty stats
        // rather than failing the evaluation. The alive rule is the model's own.
        let scalars = reduce(
            &engine.context,
            &engine.reduce,
            &GridPair {
                current: trajectory.current(),
                previous: trajectory.previous(),
                channels: trajectory.channels(),
                cell_count: trajectory.cell_count(),
                alive_channel: M::ALIVE_CHANNEL,
                alive_min: M::ALIVE_MIN,
            },
        )
        .unwrap_or_default();
        let last = trajectory.grid()?;
        // The one committed artifact is the segment's final state. A stepped
        // model frames it as (step reached, grid) so a successor resumes the
        // absolute step; a bare-grid model commits the grid alone, since its
        // update is the same map at every step and the grid is a complete
        // continuation on its own.
        let bytes = match step_base {
            Some(base) => encode_continuation(base + u64::from(shared.steps()), &last),
            None => last.to_bytes(),
        };
        Ok(Outcome::Completed {
            artifacts: vec![Artifact {
                name: STATE_ARTIFACT.to_string(),
                bytes,
            }],
            stats: Stats {
                scalars,
                blob: Vec::new(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use sima_contracts::NoCheckpoint;
    use sima_contracts::WorkerId;
    use sima_core::hash_bytes;
    use sima_model::{EnvironmentId, Params, Spec};

    use super::super::models::gray_scott::GrayScott;
    use super::super::models::nca::Nca;
    use super::super::params::{CaParams, encode_params};
    use super::super::toy_model::Toy;
    use super::*;

    /// The models' constructor-bearing types. The model submodules are private,
    /// so the genome, ignition, and sampling-config types are reachable here
    /// only through the trait's associated types.
    type Genome<M> = <M as CaModel>::Genome;
    type Ignition<M> = <M as CaModel>::Ignition;
    type GenConfig<M> = <M as CaModel>::GenConfig;

    fn env() -> EnvironmentId {
        EnvironmentId::from_hash(hash_bytes(b"env"))
    }

    fn ctx() -> ExecutionContext {
        ExecutionContext {
            attempt: 0,
            worker: WorkerId(0),
        }
    }

    /// A well-formed toy spec at the given value.
    fn spec() -> Spec {
        Spec {
            format: FormatId::new(Toy::FORMAT_ID).expect("valid format id"),
            bytes: Toy::genome(0.5).to_bytes(),
        }
    }

    /// Well-formed toy run params on a 64x64 grid.
    fn params() -> Params {
        Params {
            bytes: encode_params::<Toy>(
                &CaParams::new(64, 64, 100, 1.0).expect("valid params"),
                &Toy::ignition(0.5),
            ),
        }
    }

    fn input<'a>(
        spec: &'a Spec,
        params: &'a Params,
        input_state: Option<&'a [u8]>,
    ) -> TaskInput<'a> {
        TaskInput {
            spec,
            params,
            seed: 42,
            environment: env(),
            input_state,
        }
    }

    #[test]
    fn format_answers_the_model_id() -> Result<()> {
        assert_eq!(
            CaExecutor::<Toy>::new(None)?.format().as_str(),
            Toy::FORMAT_ID
        );
        Ok(())
    }

    #[test]
    fn a_malformed_spec_is_an_error() -> Result<()> {
        // The error paths stay device-free: they precede any GPU touch.
        let exec = CaExecutor::<Toy>::new(None)?;
        let params = params();
        for bytes in [vec![0xFF], Vec::new()] {
            let spec = Spec {
                format: FormatId::new(Toy::FORMAT_ID)?,
                bytes,
            };
            match exec.execute(&input(&spec, &params, None), &ctx(), &NoCheckpoint) {
                Err(Error::Validation(message)) => {
                    assert!(
                        message.contains("genome"),
                        "the error names the genome: {message}"
                    );
                }
                other => panic!("expected Validation, got {other:?}"),
            }
        }
        Ok(())
    }

    #[test]
    fn malformed_params_are_an_error() -> Result<()> {
        let exec = CaExecutor::<Toy>::new(None)?;
        let spec = spec();
        let params = Params {
            bytes: vec![1, 2, 3],
        };
        assert!(matches!(
            exec.execute(&input(&spec, &params, None), &ctx(), &NoCheckpoint),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn a_mismatched_input_state_is_an_error() -> Result<()> {
        // An 8x8 predecessor grid against 64x64 run params: the error names both
        // dimension triples. The toy model has one channel.
        let exec = CaExecutor::<Toy>::new(None)?;
        let spec = spec();
        let params = params();
        let state = Grid::new(8, 8, 1, vec![0.0; 64])?.to_bytes();
        match exec.execute(&input(&spec, &params, Some(&state)), &ctx(), &NoCheckpoint) {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("(8, 8, 1)") && message.contains("(64, 64, 1)"),
                    "the error names both dimension triples: {message}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn a_non_grid_input_state_is_an_error() -> Result<()> {
        let exec = CaExecutor::<Toy>::new(None)?;
        let spec = spec();
        let params = params();
        assert!(matches!(
            exec.execute(
                &input(&spec, &params, Some(b"not a grid")),
                &ctx(),
                &NoCheckpoint
            ),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    /// The scalar names the reduction emits for a `channels`-channel model:
    /// four metrics per channel, then the two grid-level scalars.
    fn expected_scalar_names(channels: u32) -> Vec<String> {
        let mut names = Vec::new();
        for c in 0..channels {
            for metric in ["mean", "var", "min", "max"] {
                names.push(format!("c{c}.{metric}"));
            }
        }
        names.push("population".to_string());
        names.push("activity".to_string());
        names
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn a_bare_grid_evaluation_reduces_to_named_scalars() {
        // A bare-grid model reduces its final grid pair into the named scalars;
        // the family blob stays empty. Gray-Scott, two channels, is the vehicle.
        let exec = CaExecutor::<GrayScott>::new(None).expect("executor");
        let spec = Spec {
            format: FormatId::new(GrayScott::FORMAT_ID).expect("format id"),
            bytes: Genome::<GrayScott>::new(0.055, 0.062, 0.16, 0.08)
                .expect("genome")
                .to_bytes(),
        };
        let params = Params {
            bytes: encode_params::<GrayScott>(
                &CaParams::new(32, 32, 16, 1.0).expect("params"),
                &Ignition::<GrayScott>::new(0.5, 0.25, 8, 0.02).expect("ignition"),
            ),
        };
        match exec
            .execute(&input(&spec, &params, None), &ctx(), &NoCheckpoint)
            .expect("execute")
        {
            Outcome::Completed { artifacts, stats } => {
                assert!(
                    artifacts.iter().any(|a| a.name == STATE_ARTIFACT),
                    "a state artifact"
                );
                assert!(stats.blob.is_empty(), "ca_evolution carries no blob");
                let names: Vec<String> =
                    stats.scalars.iter().map(|(n, _)| n.clone()).collect();
                assert_eq!(names, expected_scalar_names(GrayScott::CHANNELS));
                let population = stats
                    .scalars
                    .iter()
                    .find(|(n, _)| n == "population")
                    .expect("a population scalar")
                    .1;
                assert!(
                    (0.0..=1.0).contains(&population),
                    "population is a fraction: {population}"
                );
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn a_stepped_evaluation_reduces_the_decoded_grid() {
        // A stepped model frames its committed state, but the reduction runs over
        // the resident grid pair, so it names the same scalars. NCA, eight
        // channels, is the vehicle.
        let exec = CaExecutor::<Nca>::new(None).expect("executor");
        let genome = Nca::sample(&GenConfig::<Nca>::new(0.5).expect("config"), 42, 0);
        let spec = Spec {
            format: FormatId::new(Nca::FORMAT_ID).expect("format id"),
            bytes: genome.to_bytes(),
        };
        let params = Params {
            bytes: encode_params::<Nca>(
                &CaParams::new(32, 32, 50, 1.0).expect("params"),
                &Ignition::<Nca>::new(1.0, 8, 0.0).expect("ignition"),
            ),
        };
        match exec
            .execute(&input(&spec, &params, None), &ctx(), &NoCheckpoint)
            .expect("execute")
        {
            Outcome::Completed { artifacts, stats } => {
                let state = artifacts
                    .iter()
                    .find(|a| a.name == STATE_ARTIFACT)
                    .expect("a state artifact");
                // The committed state is a continuation frame; the reduction ran
                // over the grid, not these framed bytes.
                decode_continuation(&state.bytes).expect("framed");
                assert!(stats.blob.is_empty(), "ca_evolution carries no blob");
                let names: Vec<String> =
                    stats.scalars.iter().map(|(n, _)| n.clone()).collect();
                assert_eq!(names, expected_scalar_names(Nca::CHANNELS));
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }
}
