//! [`CaExecutor<M>`]: evaluates a CA candidate on the GPU for the model `M`.

use std::marker::PhantomData;
use std::sync::Mutex;

use sima_contracts::{
    Artifact, Checkpoint, ExecutionContext, Executor, Outcome, STATE_ARTIFACT, Stats, TaskInput,
};
use sima_core::{Error, Result};
use sima_model::FormatId;
use sima_toolkit_wgsl::{Context, Kernel};

use super::model::CaModel;
use super::params::decode_params;
use crate::cellular::{Grid, run};

/// Evaluates a candidate of the model `M` on the GPU, under format
/// `M::FORMAT_ID`: the spec's genome and the run params frame one task — ignite
/// (or continue) a grid, advance it `steps` kernel dispatches, commit the final
/// grid's canonical bytes as the `state` artifact with empty stats.
///
/// The GPU engine is created lazily on the first execute, never at construction,
/// so [`build_domain`](super::domain::build_domain) stays device-free —
/// orchestrate calls it before any store mutation, and unit tests run with no
/// GPU. A `Mutex` serializes the whole GPU section (upload → dispatch loop →
/// download): Vulkan queues and command pools require external synchronization,
/// and the scheduler runs `workers` threads calling `execute` concurrently on
/// one shared executor. A single GPU serializes the work anyway.
///
/// The checkpoint channel goes unused: the harness performs all `steps`
/// dispatches in one call and downloads once at the end, so there is no mid-run
/// state to offer, and a killed attempt restarts its segment from the segment's
/// input — bounded by `steps`, which `segments` controls. A config that sets a
/// checkpoint interval simply never gets a save for this model. Ignoring the
/// channel cannot change committed bytes.
pub(crate) struct CaExecutor<M: CaModel> {
    format: FormatId,
    /// The lazily initialized engine: `None` until the first execute, then a
    /// fully constructed engine for the process's lifetime. A failed
    /// initialization leaves `None`, so a later attempt retries.
    gpu: Mutex<Option<GpuEngine>>,
    /// `M` is used only through its associated items in the methods below, never
    /// stored; `fn() -> M` keeps the executor `Send + Sync` regardless of `M`.
    model: PhantomData<fn() -> M>,
}

/// The device context and the compiled kernel, created together once.
struct GpuEngine {
    /// Declared before `context` so it drops first: struct fields drop in
    /// declaration order, and the kernel's pipeline handles belong to the
    /// context's device, so the kernel must be destroyed before the context.
    kernel: Kernel,
    context: Context,
}

impl<M: CaModel> CaExecutor<M> {
    /// Constructs the executor for `M::FORMAT_ID`, performing no GPU work.
    pub(crate) fn new() -> Result<CaExecutor<M>> {
        Ok(CaExecutor {
            format: FormatId::new(M::FORMAT_ID)?,
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
        let genome = M::decode_genome(&input.spec.bytes).map_err(|e| {
            Error::Validation(format!("{} spec is not a valid genome: {e}", M::NAME))
        })?;
        let (shared, ignition) = decode_params::<M>(&input.params.bytes)
            .map_err(|e| Error::Validation(format!("{} params are malformed: {e}", M::NAME)))?;
        let initial = match input.input_state {
            // The first segment ignites from the seeded grid.
            None => M::ignite(&shared, &ignition, input.seed)?,
            // A successor continues from its predecessor's committed state, which
            // must match the run's dimensions exactly.
            Some(bytes) => {
                let grid = Grid::from_bytes(bytes).map_err(|e| {
                    Error::Validation(format!("{} input state is malformed: {e}", M::NAME))
                })?;
                let got = (grid.width(), grid.height(), grid.channels());
                let want = (shared.width(), shared.height(), M::CHANNELS);
                if got != want {
                    return Err(Error::Validation(format!(
                        "{} input state dimensions {got:?} do not match the run params {want:?}",
                        M::NAME
                    )));
                }
                grid
            }
        };
        // Serialize all device access. A poisoned lock is safe to enter: the slot
        // only ever holds None or a fully constructed engine, assigned after
        // construction completes.
        let mut gpu = self
            .gpu
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if gpu.is_none() {
            let context = Context::new()?;
            let kernel = context.kernel(M::KERNEL_WGSL, "main")?;
            *gpu = Some(GpuEngine { context, kernel });
        }
        let engine = gpu.as_ref().expect("gpu engine initialized above");
        // The model's uniform buffer — binding 3 of the cellular convention,
        // bound after dims.
        let uniforms = M::uniforms(&genome, &shared);
        let uniform_bytes: &[u8] = bytemuck::cast_slice(&uniforms);
        let uniforms_buffer = engine.context.buffer(uniform_bytes.len())?;
        engine.context.upload(&uniforms_buffer, uniform_bytes)?;
        let last = run(
            &engine.context,
            &engine.kernel,
            &initial,
            shared.steps(),
            &[&uniforms_buffer],
        )?;
        // The final grid's canonical bytes are the one committed artifact: the
        // update is the same map at every step, so grid bytes alone are the
        // complete continuation state a successor segment needs.
        Ok(Outcome::Completed {
            artifacts: vec![Artifact {
                name: STATE_ARTIFACT.to_string(),
                bytes: last.to_bytes(),
            }],
            stats: Stats { bytes: Vec::new() },
        })
    }
}

#[cfg(test)]
mod tests {
    use sima_contracts::NoCheckpoint;
    use sima_contracts::WorkerId;
    use sima_core::hash_bytes;
    use sima_model::{EnvironmentId, Params, Spec};

    use super::super::params::{CaParams, encode_params};
    use super::super::toy_model::Toy;
    use super::*;

    fn env() -> EnvironmentId {
        EnvironmentId::from_hash(hash_bytes(b"env"))
    }

    fn ctx() -> ExecutionContext {
        ExecutionContext {
            attempt: 0,
            worker: WorkerId(0),
        }
    }

    /// A well-formed toy spec at the given rate.
    fn spec() -> Spec {
        Spec {
            format: FormatId::new(Toy::FORMAT_ID).expect("valid format id"),
            bytes: Toy::encode_genome(&Toy::genome(0.5)),
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
        assert_eq!(CaExecutor::<Toy>::new()?.format().as_str(), Toy::FORMAT_ID);
        Ok(())
    }

    #[test]
    fn a_malformed_spec_is_an_error() -> Result<()> {
        // The error paths stay device-free: they precede any GPU touch.
        let exec = CaExecutor::<Toy>::new()?;
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
        let exec = CaExecutor::<Toy>::new()?;
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
        let exec = CaExecutor::<Toy>::new()?;
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
        let exec = CaExecutor::<Toy>::new()?;
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
}
