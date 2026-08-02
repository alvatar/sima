//! [`CaExecutor<M, E>`]: evaluates a CA candidate of the model `M` on the
//! backend `E`.

use std::marker::PhantomData;
use std::sync::Mutex;

use sima_contracts::{
    Artifact, Checkpoint, DeviceBinding, ExecutionContext, Executor, Outcome, STATE_ARTIFACT,
    Stats, TaskInput,
};
use sima_core::{Codec, Error, Result};
use sima_model::FormatId;

use super::continuation::{decode_continuation, encode_continuation};
use super::model::CaModel;
use super::params::decode_params;
use crate::substrates::cellular::{CellularEngine, EvaluationInput, Grid};

/// Evaluates a candidate of the model `M` on the backend `E`, under format
/// `M::FORMAT_ID`: the spec's genome and the run params frame one task — ignite
/// (or continue) a grid, advance it `steps` kernel dispatches, reduce the final
/// grid pair into the observational stat scalars, and commit the final state as
/// the `state` artifact. A bare-grid model commits the grid's canonical bytes; a
/// stepped model commits framed continuation state, the reached step ahead of
/// the grid.
///
/// Everything above is written once for every backend. What differs between
/// backends — opening a device, compiling kernels, dispatching, reducing —
/// sits behind [`CellularEngine`], so a model runs on a second backend by being
/// registered against a second engine and nothing here changes.
///
/// The engine is created lazily on the first execute, never at construction,
/// so [`build_binding`](super::binding::build_binding) stays device-free —
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
pub(crate) struct CaExecutor<M: CaModel, E: CellularEngine> {
    format: FormatId,
    /// The device the engine opens, or `None` for the toolkit's default
    /// selection. Read once, at engine initialization.
    device: Option<DeviceBinding>,
    /// The lazily initialized engine: `None` until the first execute, then a
    /// fully constructed engine for the process's lifetime. A failed
    /// initialization leaves `None`, so a later attempt retries.
    engine: Mutex<Option<E>>,
    /// `M` is used only through its associated items in the methods below, never
    /// stored; `fn() -> M` keeps the executor `Send + Sync` regardless of `M`.
    model: PhantomData<fn() -> M>,
}

impl<M: CaModel, E: CellularEngine> CaExecutor<M, E> {
    /// Constructs the executor for `M::FORMAT_ID` on `device` — or, for `None`,
    /// on the toolkit's default selection — performing no GPU work.
    pub(crate) fn new(device: Option<&DeviceBinding>) -> Result<CaExecutor<M, E>> {
        Ok(CaExecutor {
            format: FormatId::new(M::FORMAT_ID)?,
            device: device.cloned(),
            engine: Mutex::new(None),
            model: PhantomData,
        })
    }
}

impl<M: CaModel, E: CellularEngine> Executor for CaExecutor<M, E> {
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
        // evaluation below — because a device's queues and command pools
        // require external synchronization and the worker threads share this
        // one executor. Initializing the engine inside the lock is why
        // `binding_for` needs no device: nothing touches the GPU until the first
        // execute. A poisoned lock is safe to enter: the slot only ever holds
        // None or a fully constructed engine, assigned after construction
        // completes.
        let mut slot = self
            .engine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(E::build(self.device.as_ref(), M::KERNEL_SOURCE)?);
        }
        let engine = slot.as_ref().expect("engine initialized above");
        let evaluation = engine.evaluate(&EvaluationInput {
            initial: &initial,
            steps: shared.steps(),
            uniforms: &M::uniforms(&genome, &shared),
            // The candidate seed reaches the kernel only for a model that
            // declares it consumes one.
            seed: M::SEED_BUFFER.then_some(input.seed),
            step_base,
            alive_channel: M::ALIVE_CHANNEL,
            alive_min: M::ALIVE_MIN,
        })?;
        let scalars = stats_or_propagate(evaluation.scalars(), shared.snapshot_when())?;
        let keep = keep_snapshot(shared.snapshot_when(), &scalars, M::NAME)?;
        // The committed artifact is the segment's final state. A stepped model
        // frames it as (step reached, grid) so a successor resumes the absolute
        // step; a bare-grid model commits the grid alone, since its update is the
        // same map at every step and the grid is a complete continuation on its
        // own. A dropped snapshot commits an empty artifact list — and skips the
        // full-grid readback entirely, which is the bandwidth the predicate saves.
        let artifacts = if keep {
            let last = evaluation.grid()?;
            let bytes = match step_base {
                // The base came out of the predecessor's committed artifact and
                // is identity-bearing: the successor's task key hashes it, so a
                // wrapped sum would mint a key for a step this chain never
                // reached and quietly resolve to another segment's result.
                Some(base) => encode_continuation(reached_step(base, shared.steps())?, &last),
                None => last.to_bytes(),
            };
            vec![Artifact {
                name: STATE_ARTIFACT.to_string(),
                bytes,
            }]
        } else {
            Vec::new()
        };
        Ok(Outcome::Completed {
            artifacts,
            stats: Stats {
                scalars,
                blob: Vec::new(),
            },
        })
    }
}

/// The stat scalars to carry forward given the reduction result and the run's
/// predicate. The fault handling discriminates on the error variant:
///
/// - A definitive fault (`Error::Validation`, a misdeclared model constant such
///   as `M::CHANNELS` or `M::ALIVE_CHANNEL`) can never succeed for this model,
///   so it surfaces whether or not a predicate needs the scalars — never
///   silently blanking stats for every task of the misdeclared model.
/// - A transient device fault (`Error::Backend`, and any other variant) fails
///   the evaluation only when a predicate is present, since the scalars then
///   decide whether the snapshot commits and the verdict is uncomputable
///   without them. Absent a predicate the scalars are purely observational —
///   they travel the `Stats` channel to the journal and enter no record,
///   manifest, or identity criterion — so the fault degrades to empty stats
///   rather than failing.
fn stats_or_propagate(
    reduced: Result<Vec<(String, f64)>>,
    predicate: Option<&(String, f64)>,
) -> Result<Vec<(String, f64)>> {
    match reduced {
        Ok(scalars) => Ok(scalars),
        Err(error @ Error::Validation(_)) => Err(error),
        Err(error) => match predicate {
            Some(_) => Err(error),
            None => Ok(Vec::new()),
        },
    }
}

/// Whether to commit the snapshot given the run's predicate and the computed
/// stats. An absent predicate always commits. A present predicate commits
/// exactly when the named scalar is at least its minimum AND every scalar in
/// the list is finite: a non-finite value anywhere marks the candidate diverged
/// and drops the snapshot. A predicate naming a scalar absent from the stats is
/// an infrastructure fault, unreachable after translation-time validation.
///
/// The all-finite conjunct lives here because IEEE semantics are reliable at the
/// Rust layer. In a reduction kernel they are not: a shader's `min` and `max`
/// may skip a NaN operand and the population test counts a NaN cell as dead, so
/// a predicate on `population`, `c<i>.min`, or `c<i>.max` could otherwise commit
/// a partially diverged grid. The finite check catches divergence a sum-derived
/// scalar (mean, variance, activity) would surface but those scalars would not.
fn keep_snapshot(
    predicate: Option<&(String, f64)>,
    scalars: &[(String, f64)],
    model: &str,
) -> Result<bool> {
    match predicate {
        None => Ok(true),
        Some((scalar, min)) => {
            let value = scalars
                .iter()
                .find(|(name, _)| name == scalar)
                .map(|(_, value)| *value)
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "{model} snapshot_when names scalar {scalar:?}, absent from the \
                         computed stats"
                    ))
                })?;
            let all_finite = scalars.iter().all(|(_, value)| value.is_finite());
            Ok(all_finite && value >= *min)
        }
    }
}

/// The absolute step a segment resuming at `base` reaches after `steps`.
///
/// The base comes out of the predecessor's committed artifact and is
/// identity-bearing: the successor's task key hashes it, so a wrapped sum would
/// mint a key for a step this chain never reached and quietly resolve to
/// another segment's result. Decoded input is not trusted to be small.
fn reached_step(base: u64, steps: u32) -> Result<u64> {
    base.checked_add(u64::from(steps)).ok_or_else(|| {
        Error::Validation(format!(
            "a segment resuming at step {base} would reach {base} + {steps}, past what a step \
             index holds"
        ))
    })
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
    use crate::substrates::cellular::WgslEngine;

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
    fn a_segment_that_would_step_past_the_index_is_refused() {
        // The base is decoded from the predecessor's committed artifact and
        // enters the successor's task key, so a wrapped sum would mint a key
        // for a step the chain never reached.
        let error = reached_step(u64::MAX - 3, 4).expect_err("past the index");
        let Error::Validation(message) = error else {
            panic!("expected a validation error");
        };
        assert!(message.contains(&(u64::MAX - 3).to_string()), "{message}");
    }

    #[test]
    fn a_segment_reaching_the_last_representable_step_is_allowed() {
        // The bound is the wrap, not a margin below it.
        assert_eq!(
            reached_step(u64::MAX - 4, 4).expect("the last step"),
            u64::MAX
        );
        assert_eq!(reached_step(100, 50).expect("an ordinary chain"), 150);
    }

    #[test]
    fn format_answers_the_model_id() -> Result<()> {
        assert_eq!(
            CaExecutor::<Toy, WgslEngine>::new(None)?.format().as_str(),
            Toy::FORMAT_ID
        );
        Ok(())
    }

    #[test]
    fn a_malformed_spec_is_an_error() -> Result<()> {
        // The error paths stay device-free: they precede any GPU touch.
        let exec = CaExecutor::<Toy, WgslEngine>::new(None)?;
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
        let exec = CaExecutor::<Toy, WgslEngine>::new(None)?;
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
        let exec = CaExecutor::<Toy, WgslEngine>::new(None)?;
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
        let exec = CaExecutor::<Toy, WgslEngine>::new(None)?;
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

    /// The scalar names the reduction emits for a `channels`-channel model.
    fn expected_scalar_names(channels: u32) -> Vec<String> {
        crate::substrates::cellular::scalar_names(channels)
    }

    /// A stat list carrying one scalar at `value`.
    fn one_scalar(name: &str, value: f64) -> Vec<(String, f64)> {
        vec![(name.to_string(), value)]
    }

    #[test]
    fn an_absent_predicate_keeps_the_snapshot() -> Result<()> {
        // No predicate commits regardless of the scalar values, a diverged
        // candidate included: the divergence guard is a predicate-only conjunct.
        assert!(keep_snapshot(None, &one_scalar("population", 0.0), "m")?);
        assert!(keep_snapshot(None, &one_scalar("c0.min", f64::NAN), "m")?);
        Ok(())
    }

    #[test]
    fn a_predicate_keeps_at_and_above_the_threshold() -> Result<()> {
        let predicate = ("population".to_string(), 0.5);
        // Exactly at the minimum keeps, and above it keeps.
        assert!(keep_snapshot(
            Some(&predicate),
            &one_scalar("population", 0.5),
            "m"
        )?);
        assert!(keep_snapshot(
            Some(&predicate),
            &one_scalar("population", 0.9),
            "m"
        )?);
        Ok(())
    }

    #[test]
    fn a_predicate_drops_below_the_threshold() -> Result<()> {
        let predicate = ("population".to_string(), 0.5);
        assert!(!keep_snapshot(
            Some(&predicate),
            &one_scalar("population", 0.49),
            "m"
        )?);
        Ok(())
    }

    #[test]
    fn a_predicate_drops_a_non_finite_value() -> Result<()> {
        // A diverged candidate: NaN and the infinities all fail the gate, so the
        // snapshot is dropped rather than committed on a spurious comparison.
        let predicate = ("activity".to_string(), 1.0e-4);
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(!keep_snapshot(
                Some(&predicate),
                &one_scalar("activity", value),
                "m"
            )?);
        }
        Ok(())
    }

    #[test]
    fn a_predicate_drops_when_another_scalar_diverges() -> Result<()> {
        // The named scalar clears its threshold, but a different scalar is
        // non-finite: the candidate diverged, so the snapshot drops. WGSL
        // min/max skip NaN operands and the population test counts a NaN cell as
        // dead, so a predicate on a finite scalar must still catch divergence
        // reported through another scalar.
        let predicate = ("population".to_string(), 0.5);
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let scalars = vec![("population".to_string(), 0.9), ("c0.min".to_string(), bad)];
            assert!(!keep_snapshot(Some(&predicate), &scalars, "m")?);
        }
        Ok(())
    }

    #[test]
    fn a_predicate_keeps_when_every_scalar_is_finite() -> Result<()> {
        // The named scalar clears its threshold and every scalar in the list is
        // finite, so the snapshot commits: the divergence guard admits a
        // converged candidate.
        let predicate = ("population".to_string(), 0.5);
        let scalars = vec![
            ("c0.min".to_string(), -1.0),
            ("population".to_string(), 0.9),
            ("activity".to_string(), 0.001),
        ];
        assert!(keep_snapshot(Some(&predicate), &scalars, "m")?);
        Ok(())
    }

    #[test]
    fn a_predicate_naming_a_missing_scalar_is_a_fault() {
        let predicate = ("nonesuch".to_string(), 0.5);
        assert!(matches!(
            keep_snapshot(Some(&predicate), &one_scalar("population", 1.0), "m"),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn a_successful_reduction_carries_its_scalars_either_way() -> Result<()> {
        // Whether or not a predicate is present, a successful reduction's scalars
        // pass through unchanged.
        let predicate = ("population".to_string(), 0.5);
        for guard in [None, Some(&predicate)] {
            let scalars = stats_or_propagate(Ok(one_scalar("population", 1.0)), guard)?;
            assert_eq!(scalars, one_scalar("population", 1.0));
        }
        Ok(())
    }

    #[test]
    fn a_device_fault_propagates_under_a_predicate() {
        // A transient device fault (`Error::Backend`) with a predicate present:
        // the predicate needs the scalars to decide the snapshot, so the fault
        // surfaces rather than a spurious missing-scalar fault.
        let predicate = ("population".to_string(), 0.5);
        let fault: Result<Vec<(String, f64)>> = Err(Error::Backend("device lost".to_string()));
        match stats_or_propagate(fault, Some(&predicate)) {
            Err(Error::Backend(message)) => assert_eq!(message, "device lost"),
            other => panic!("expected the device fault, got {other:?}"),
        }
    }

    #[test]
    fn a_device_fault_degrades_to_empty_without_a_predicate() -> Result<()> {
        // A transient device fault (`Error::Backend`) with no predicate: the
        // scalars are observational, so the fault degrades to an empty list and
        // the evaluation still completes.
        let fault: Result<Vec<(String, f64)>> = Err(Error::Backend("device lost".to_string()));
        assert!(stats_or_propagate(fault, None)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_validation_fault_propagates_either_way() {
        // `Error::Validation` from the reduction is a misdeclared model constant:
        // the reduction can never succeed for this model, so the fault surfaces
        // whether or not a predicate needs the scalars, never silently blanking
        // stats.
        let predicate = ("population".to_string(), 0.5);
        for guard in [None, Some(&predicate)] {
            let fault: Result<Vec<(String, f64)>> =
                Err(Error::Validation("alive_channel out of range".to_string()));
            match stats_or_propagate(fault, guard) {
                Err(Error::Validation(message)) => {
                    assert_eq!(message, "alive_channel out of range");
                }
                other => panic!("expected the validation fault, got {other:?}"),
            }
        }
    }

    /// Executing the domain dispatches to a real device.
    mod on_device {
        use super::*;

        #[test]
        fn a_bare_grid_evaluation_reduces_to_named_scalars() {
            // A bare-grid model reduces its final grid pair into the named scalars;
            // the family blob stays empty. Gray-Scott, two channels, is the vehicle.
            let exec = CaExecutor::<GrayScott, WgslEngine>::new(None).expect("executor");
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
                    let names: Vec<String> = stats.scalars.iter().map(|(n, _)| n.clone()).collect();
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

        #[test]
        fn a_stepped_evaluation_reduces_the_decoded_grid() {
            // A stepped model frames its committed state, but the reduction runs over
            // the resident grid pair, so it names the same scalars. NCA, eight
            // channels, is the vehicle.
            let exec = CaExecutor::<Nca, WgslEngine>::new(None).expect("executor");
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
                    let names: Vec<String> = stats.scalars.iter().map(|(n, _)| n.clone()).collect();
                    assert_eq!(names, expected_scalar_names(Nca::CHANNELS));
                }
                other => panic!("expected Completed, got {other:?}"),
            }
        }

        #[test]
        fn a_failed_predicate_drops_the_snapshot_but_keeps_the_stats() {
            // `population` is a fraction, so a minimum of 2.0 can never be met: the
            // state artifact is dropped, the outcome still completes, and the stats
            // are journaled regardless.
            let exec = CaExecutor::<GrayScott, WgslEngine>::new(None).expect("executor");
            let spec = Spec {
                format: FormatId::new(GrayScott::FORMAT_ID).expect("format id"),
                bytes: Genome::<GrayScott>::new(0.055, 0.062, 0.16, 0.08)
                    .expect("genome")
                    .to_bytes(),
            };
            let params = Params {
                bytes: encode_params::<GrayScott>(
                    &CaParams::new(32, 32, 16, 1.0)
                        .expect("params")
                        .with_snapshot_when(Some(("population".to_string(), 2.0))),
                    &Ignition::<GrayScott>::new(0.5, 0.25, 8, 0.02).expect("ignition"),
                ),
            };
            match exec
                .execute(&input(&spec, &params, None), &ctx(), &NoCheckpoint)
                .expect("execute")
            {
                Outcome::Completed { artifacts, stats } => {
                    assert!(artifacts.is_empty(), "the dropped snapshot commits nothing");
                    assert!(!stats.scalars.is_empty(), "stats are journaled regardless");
                }
                other => panic!("expected Completed, got {other:?}"),
            }
        }
    }
}
