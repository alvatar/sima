//! [`CaEvolutionExecutor`]: evaluates a `ca_evolution` candidate on the GPU.

use std::sync::Mutex;

use sima_core::{Error, Result};
use sima_model::FormatId;
use sima_toolkit_wgsl::{Context, Kernel};

use super::{CaEvolutionGenome, CaEvolutionParams};
use crate::cellular::{Grid, run};
use sima_contracts::{
    Artifact, Checkpoint, ExecutionContext, Executor, Outcome, STATE_ARTIFACT, Stats, TaskInput,
};

/// The Gray-Scott WGSL kernel source. Compiled on the first execute;
/// checked device-free by this module's tests, so hosted CI catches a
/// source that fails to compile without needing a device.
pub(crate) const KERNEL_WGSL: &str = include_str!("../../../shaders/gray_scott.wgsl");

/// Evaluates a `ca_evolution` candidate on the GPU, under format
/// `ca_evolution.v1`: the spec's genome and the run params frame one task —
/// ignite (or continue) a grid, advance it `steps` kernel dispatches, commit
/// the final grid's canonical bytes as the `state` artifact with empty
/// stats.
///
/// The GPU engine is created lazily on the first execute, never at
/// construction, so [`domain_for`](crate::domain_for) stays device-free —
/// orchestrate calls it before any store mutation, and unit tests run with
/// no GPU. A `Mutex` serializes the whole GPU section (upload → dispatch
/// loop → download): Vulkan queues and command pools require external
/// synchronization, and the scheduler runs `workers` threads calling
/// `execute` concurrently on one shared executor. A single GPU serializes
/// the work anyway.
///
/// The checkpoint channel goes unused: the harness performs all `steps`
/// dispatches in one call and downloads once at the end, so there is no
/// mid-run state to offer, and a killed attempt restarts its segment from
/// the segment's input — bounded by `steps`, which `segments` controls. A
/// config that sets a checkpoint interval simply never gets a save for this
/// domain. Ignoring the channel cannot change committed bytes.
pub struct CaEvolutionExecutor {
    format: FormatId,
    /// The lazily initialized engine: `None` until the first execute, then
    /// a fully constructed engine for the process's lifetime. A failed
    /// initialization leaves `None`, so a later attempt retries.
    gpu: Mutex<Option<GpuEngine>>,
}

/// The device context and the compiled kernel, created together once.
///
/// Field order is a constraint: struct fields drop in declaration order, and
/// the kernel's pipeline handles belong to the context's device, so the
/// kernel must be declared — and therefore destroyed — before the context.
struct GpuEngine {
    kernel: Kernel,
    context: Context,
}

impl CaEvolutionExecutor {
    /// Constructs the executor for the `ca_evolution.v1` format, performing
    /// no GPU work.
    pub fn new() -> Result<CaEvolutionExecutor> {
        Ok(CaEvolutionExecutor {
            format: FormatId::new("ca_evolution.v1")?,
            gpu: Mutex::new(None),
        })
    }
}

impl Executor for CaEvolutionExecutor {
    fn format(&self) -> &FormatId {
        &self.format
    }

    fn execute(
        &self,
        input: &TaskInput<'_>,
        _ctx: &ExecutionContext,
        _checkpoint: &dyn Checkpoint,
    ) -> Result<Outcome> {
        // Structural validation strictly before any GPU touch: a malformed
        // spec, params, or input state is an identity fault (`Err`), never a
        // candidate failure — and the error paths stay device-free, like the
        // stub's treatment.
        let genome = CaEvolutionGenome::from_bytes(&input.spec.bytes).map_err(|e| {
            Error::Validation(format!("ca_evolution spec is not a valid genome: {e}"))
        })?;
        let params = CaEvolutionParams::from_bytes(&input.params.bytes)
            .map_err(|e| Error::Validation(format!("ca_evolution params are malformed: {e}")))?;
        let initial = match input.input_state {
            // The first segment ignites from the seeded patch.
            None => params
                .patch()
                .seeded_initial(params.width(), params.height(), input.seed)?,
            // A successor continues from its predecessor's committed state,
            // which must match the run's dimensions exactly.
            Some(bytes) => {
                let grid = Grid::from_bytes(bytes).map_err(|e| {
                    Error::Validation(format!("ca_evolution input state is malformed: {e}"))
                })?;
                let got = (grid.width(), grid.height(), grid.channels());
                let want = (params.width(), params.height(), 2);
                if got != want {
                    return Err(Error::Validation(format!(
                        "ca_evolution input state dimensions {got:?} do not match the run \
                         params {want:?}"
                    )));
                }
                grid
            }
        };
        // Serialize all device access. A poisoned lock is safe to enter:
        // the slot only ever holds None or a fully constructed engine,
        // assigned after construction completes.
        let mut gpu = self
            .gpu
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if gpu.is_none() {
            let context = Context::new()?;
            let kernel = context.kernel(KERNEL_WGSL, "main")?;
            *gpu = Some(GpuEngine { context, kernel });
        }
        let engine = gpu.as_ref().expect("gpu engine initialized above");
        // The rates buffer: [f, k, du, dv, dt], the one params buffer the
        // harness binds after dims — binding 3 of the cellular convention.
        let rates = [
            genome.feed(),
            genome.kill(),
            genome.diffusion_u(),
            genome.diffusion_v(),
            params.dt(),
        ];
        let rates_buffer = engine.context.buffer(std::mem::size_of_val(&rates))?;
        engine
            .context
            .upload(&rates_buffer, bytemuck::cast_slice(&rates))?;
        let last = run(
            &engine.context,
            &engine.kernel,
            &initial,
            params.steps(),
            &[&rates_buffer],
        )?;
        // The final grid's canonical bytes are the one committed artifact:
        // the update is the same map at every step, so grid bytes alone are
        // the complete continuation state a successor segment needs.
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
    use sima_contracts::{NoCheckpoint, WorkerId};
    use sima_core::hash_bytes;
    use sima_model::{EnvironmentId, Params, Spec};

    use super::super::CaEvolutionPatch;
    use super::*;

    /// The pattern-forming sample point with the classical diffusion pair.
    fn sample_genome() -> CaEvolutionGenome {
        CaEvolutionGenome::new(0.055, 0.062, 0.16, 0.08).expect("valid sample genome")
    }

    /// Pearson's classical ignition configuration.
    fn pearson_patch() -> CaEvolutionPatch {
        CaEvolutionPatch::new(0.5, 0.25, 8, 0.02).expect("valid pearson patch")
    }

    fn spec_for(genome: &CaEvolutionGenome) -> Spec {
        Spec {
            format: FormatId::new("ca_evolution.v1").expect("valid format id"),
            bytes: genome.to_bytes(),
        }
    }

    /// The D12 evaluation frame: a 64x64 grid at dt = 1.0 with the Pearson
    /// patch, advancing `steps` per task.
    fn params_for_steps(steps: u32) -> Params {
        Params {
            bytes: CaEvolutionParams::new(64, 64, steps, 1.0, pearson_patch())
                .expect("valid params")
                .to_bytes(),
        }
    }

    fn env() -> EnvironmentId {
        EnvironmentId::from_hash(hash_bytes(b"env"))
    }

    fn ctx() -> ExecutionContext {
        ExecutionContext {
            attempt: 0,
            worker: WorkerId(0),
        }
    }

    /// Executes one task over `genome` and returns the committed state
    /// bytes, asserting the outcome shape: one `state` artifact, empty
    /// stats.
    fn run_state(
        exec: &CaEvolutionExecutor,
        genome: &CaEvolutionGenome,
        steps: u32,
        input_state: Option<&[u8]>,
    ) -> Vec<u8> {
        let spec = spec_for(genome);
        let params = params_for_steps(steps);
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 42,
            environment: env(),
            input_state,
        };
        match exec
            .execute(&input, &ctx(), &NoCheckpoint)
            .expect("execute")
        {
            Outcome::Completed { artifacts, stats } => {
                assert_eq!(artifacts.len(), 1, "one committed artifact");
                assert_eq!(artifacts[0].name, STATE_ARTIFACT);
                assert!(stats.bytes.is_empty(), "ca_evolution stats are empty");
                artifacts[0].bytes.clone()
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[test]
    fn the_kernel_compiles_device_free() {
        sima_toolkit_wgsl::check(KERNEL_WGSL, "main").expect("kernel compiles");
    }

    #[test]
    fn format_answers_ca_evolution() -> Result<()> {
        assert_eq!(
            CaEvolutionExecutor::new()?.format().as_str(),
            "ca_evolution.v1"
        );
        Ok(())
    }

    #[test]
    fn a_malformed_spec_is_an_error() -> Result<()> {
        let exec = CaEvolutionExecutor::new()?;
        let params = params_for_steps(100);
        for bytes in [vec![0xFF], Vec::new()] {
            let spec = Spec {
                format: FormatId::new("ca_evolution.v1")?,
                bytes,
            };
            let input = TaskInput {
                spec: &spec,
                params: &params,
                seed: 42,
                environment: env(),
                input_state: None,
            };
            match exec.execute(&input, &ctx(), &NoCheckpoint) {
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
        let exec = CaEvolutionExecutor::new()?;
        let spec = spec_for(&sample_genome());
        let params = Params {
            bytes: vec![1, 2, 3],
        };
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 42,
            environment: env(),
            input_state: None,
        };
        assert!(matches!(
            exec.execute(&input, &ctx(), &NoCheckpoint),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn a_mismatched_input_state_is_an_error() -> Result<()> {
        // An 8x8 predecessor grid against 64x64 run params: the error names
        // both dimension triples.
        let exec = CaEvolutionExecutor::new()?;
        let spec = spec_for(&sample_genome());
        let params = params_for_steps(100);
        let state = Grid::new(8, 8, 2, vec![0.0; 128])?.to_bytes();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 42,
            environment: env(),
            input_state: Some(&state),
        };
        match exec.execute(&input, &ctx(), &NoCheckpoint) {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("(8, 8, 2)") && message.contains("(64, 64, 2)"),
                    "the error names both dimension triples: {message}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn a_non_grid_input_state_is_an_error() -> Result<()> {
        let exec = CaEvolutionExecutor::new()?;
        let spec = spec_for(&sample_genome());
        let params = params_for_steps(100);
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 42,
            environment: env(),
            input_state: Some(b"not a grid"),
        };
        assert!(matches!(
            exec.execute(&input, &ctx(), &NoCheckpoint),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn a_pattern_forming_point_forms_structure() -> Result<()> {
        // 64x64, seed 42, the sample point, 3000 steps at dt = 1. Three
        // assertions, each catching a distinct failure mode:
        //   - finiteness catches divergence;
        //   - the population stddev of v catches death (a decayed grid is
        //     uniform, stddev 0) — but the initial 64-cell patch alone
        //     already clears 0.02, so stddev cannot prove growth;
        //   - the count of cells with v > 0.1 catches a patch that survived
        //     without growing: more than four times the initial patch means
        //     the structure demonstrably grew beyond its seed.
        let exec = CaEvolutionExecutor::new()?;
        let state = run_state(&exec, &sample_genome(), 3000, None);
        let evolved = Grid::from_bytes(&state)?;
        assert!(evolved.data().iter().all(|value| value.is_finite()));
        // Population standard deviation of the v channel, accumulated in
        // f64 — test-side math, free to choose its own precision.
        let v: Vec<f64> = evolved
            .data()
            .chunks(2)
            .map(|cell| cell[1] as f64)
            .collect();
        let mean = v.iter().sum::<f64>() / v.len() as f64;
        let variance = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / v.len() as f64;
        let stddev = variance.sqrt();
        let spread = v.iter().filter(|&&x| x > 0.1).count();
        println!("pattern point: v stddev {stddev}, spread {spread} cells");
        assert!(stddev > 0.02, "v stddev {stddev} must exceed 0.02");
        assert!(spread > 256, "spread {spread} must exceed 256 cells");
        Ok(())
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn a_dead_point_decays_to_the_trivial_state() -> Result<()> {
        // Same frame at (f, k) = (0.05, 0.075): k sits above the pattern
        // band of the (f, k) map, so the patch reacts transiently, fails to
        // self-sustain, and dies; the feed then pulls u back toward 1.
        let genome = CaEvolutionGenome::new(0.05, 0.075, 0.16, 0.08)?;
        let exec = CaEvolutionExecutor::new()?;
        let state = run_state(&exec, &genome, 3000, None);
        let evolved = Grid::from_bytes(&state)?;
        let max_v = evolved
            .data()
            .chunks(2)
            .map(|cell| cell[1])
            .fold(f32::MIN, f32::max);
        let min_u = evolved
            .data()
            .chunks(2)
            .map(|cell| cell[0])
            .fold(f32::MAX, f32::min);
        println!("dead point: max v {max_v}, min u {min_u}");
        assert!(max_v < 1e-3, "max v {max_v} must be under 1e-3");
        assert!(min_u > 0.9, "min u {min_u} must exceed 0.9");
        Ok(())
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn repeated_evaluation_commits_identical_bytes() -> Result<()> {
        // Two fresh execute calls over the same TaskInput: per-backend
        // determinism at the executor level — results reproduce run to run
        // on one machine.
        let exec = CaEvolutionExecutor::new()?;
        let first = run_state(&exec, &sample_genome(), 3000, None);
        let second = run_state(&exec, &sample_genome(), 3000, None);
        assert_eq!(first, second);
        Ok(())
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn segment_continuation_matches_one_run() -> Result<()> {
        // 200 steps, then the committed state fed back for another 200,
        // equals one 400-step evaluation byte for byte: the input_state
        // decode path restores the exact grid, and grid bytes alone are the
        // complete continuation state. Same machine, same backend, so
        // equality is exact.
        let exec = CaEvolutionExecutor::new()?;
        let first = run_state(&exec, &sample_genome(), 200, None);
        let second = run_state(&exec, &sample_genome(), 200, Some(&first));
        let whole = run_state(&exec, &sample_genome(), 400, None);
        assert_eq!(second, whole);
        Ok(())
    }
}
