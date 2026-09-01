//! The stub executor: evaluates a candidate by reading its programmed
//! behavior.
//!
//! [`StubExecutor`] decodes the [`StubProgram`] a spec carries and acts on it:
//! succeed, fail while below a threshold attempt, reject definitively, panic,
//! sleep, or accumulate — the stateful behavior that folds a seed through a
//! deterministic step function and commits the resulting [`StubState`]. Every
//! committed [`Artifact`] is a pure function of [`TaskInput`] alone, so it
//! never varies with the execution context. The attempt number folds only
//! into [`Stats`], the observational output, and into the `Flaky` retry gate.
//! This split is the boundary the whole contract exists to enforce.

use std::time::Duration;

use sima_core::{Enc, Error, Hash, Result, hash_bytes};
use sima_model::FormatId;

use super::program::{StubBehavior, StubProgram};
use super::state::StubState;
use sima_contracts::{Artifact, Checkpoint, ExecutionContext, Executor, Outcome, Stats, TaskInput};

/// Evaluates a candidate carrying a [`StubProgram`], under format `stub.v1`.
#[derive(Debug, Clone)]
pub struct StubExecutor {
    format: FormatId,
}

impl StubExecutor {
    /// Constructs the executor for the `stub.v1` format.
    pub fn new() -> Result<StubExecutor> {
        Ok(StubExecutor {
            format: FormatId::new("stub.v1")?,
        })
    }
}

impl Executor for StubExecutor {
    fn format(&self) -> &FormatId {
        &self.format
    }

    fn execute(
        &self,
        input: &TaskInput<'_>,
        ctx: &ExecutionContext,
        checkpoint: &dyn Checkpoint,
    ) -> Result<Outcome> {
        // A spec whose bytes are not a valid program is a structural input
        // fault, not a candidate failure: it is `Err`, not `Outcome::Failed`.
        let program = StubProgram::from_bytes(&input.spec.bytes)
            .map_err(|e| Error::Validation(format!("stub spec is not a valid program: {e}")))?;
        match program.behavior {
            StubBehavior::Succeed => Ok(completed(input, ctx)),
            StubBehavior::Flaky(n) => {
                // The one place the stub reads `ctx.attempt` to affect control
                // flow; the eventual artifact stays attempt-independent.
                if (ctx.attempt as u64) < n {
                    Ok(Outcome::Failed {
                        reason: format!("programmed failure: attempt {} of {}", ctx.attempt, n),
                        stats: stats(ctx),
                    })
                } else {
                    Ok(completed(input, ctx))
                }
            }
            StubBehavior::Panic => panic!("stub executor: programmed panic"),
            StubBehavior::Sleep(millis) => {
                std::thread::sleep(Duration::from_millis(millis));
                Ok(completed(input, ctx))
            }
            StubBehavior::Reject => Ok(Outcome::Rejected {
                reason: "programmed rejection".to_string(),
                stats: stats(ctx),
            }),
            StubBehavior::Accumulate(k) => accumulate(input, ctx, checkpoint, k, Duration::ZERO),
            StubBehavior::PacedAccumulate { steps, step_ms } => accumulate(
                input,
                ctx,
                checkpoint,
                steps,
                Duration::from_millis(step_ms),
            ),
        }
    }
}

/// The `Accumulate` semantics: fold the accumulator through k steps keyed by
/// the absolute step index — so the trajectory is invariant under where the
/// segmentation cuts fall — offering a checkpoint at every step boundary, and
/// commit the resulting state as the `state` artifact. The stats carry the
/// steps this attempt actually executed, so a test can prove a resume
/// checkpoint shortened re-execution. `pace` is `PacedAccumulate`'s sleep per
/// step; zero for the unpaced behavior.
fn accumulate(
    input: &TaskInput<'_>,
    ctx: &ExecutionContext,
    checkpoint: &dyn Checkpoint,
    k: u64,
    pace: Duration,
) -> Result<Outcome> {
    // Malformed input state is a structural input fault, like a malformed
    // spec: the identity referenced bytes that are not a stub state.
    let mut state = match input.input_state {
        None => StubState {
            step: 0,
            acc: input.seed,
        },
        Some(bytes) => StubState::from_bytes(bytes)
            .map_err(|e| Error::Validation(format!("stub input state is malformed: {e}")))?,
    };
    let start = state.step;
    let end = start
        .checked_add(k)
        .ok_or_else(|| Error::Validation(format!("stub step range {start} + {k} overflows u64")))?;
    // A saved checkpoint is adopted only when it decodes and its step lies
    // inside this task's range; anything else is stale and ignored — resuming
    // never changes the committed bytes, only how many steps reach them.
    if let Some(bytes) = checkpoint.resume()
        && let Ok(saved) = StubState::from_bytes(bytes)
        && saved.step >= start
        && saved.step < end
    {
        state = saved;
    }
    let mut steps_executed: u64 = 0;
    while state.step < end {
        // The pace is wall clock alone: it precedes the step so an interrupt
        // lands before work, and it never touches the state trajectory.
        if !pace.is_zero() {
            std::thread::sleep(pace);
        }
        state.acc = sima_core::prng::derive(state.acc, state.step);
        state.step += 1;
        steps_executed += 1;
        sima_core::crashpoint("stub.accumulate.step");
        checkpoint.offer(&|| state.to_bytes());
    }
    Ok(Outcome::Completed {
        artifacts: vec![Artifact {
            name: sima_contracts::STATE_ARTIFACT.to_string(),
            bytes: state.to_bytes(),
        }],
        // The accumulate stats carry the attempt and the steps this attempt
        // actually executed, so a test can prove a resume checkpoint shortened
        // re-execution.
        stats: Stats {
            scalars: vec![
                ("attempt".to_string(), f64::from(ctx.attempt)),
                ("steps".to_string(), steps_executed as f64),
            ],
            blob: STUB_STATS_BLOB.to_vec(),
        },
    })
}

/// The successful outcome: the identity artifact plus the stats carrying the
/// attempt number.
fn completed(input: &TaskInput<'_>, ctx: &ExecutionContext) -> Outcome {
    Outcome::Completed {
        artifacts: vec![identity_artifact(input)],
        stats: stats(ctx),
    }
}

/// The one committed artifact, carrying the 32 raw bytes of the identity
/// digest under the name `output`.
fn identity_artifact(input: &TaskInput<'_>) -> Artifact {
    Artifact {
        name: "output".to_string(),
        bytes: identity_digest(input).as_bytes().to_vec(),
    }
}

/// A digest over the identity inputs only — spec, params, seed, environment,
/// and the input-state bytes folded in as their digest, matching how the task
/// key treats input state. No execution-context field participates, so the
/// artifact is a pure function of identity.
fn identity_digest(input: &TaskInput<'_>) -> Hash {
    let mut enc = Enc::new();
    enc.hash(input.spec.id().as_hash())
        .hash(input.params.id().as_hash())
        .u64(input.seed)
        .hash(input.environment.as_hash())
        .opt_hash(input.input_state.map(hash_bytes).as_ref());
    hash_bytes(&enc.finish())
}

/// A fixed marker the stub places in the stats family blob, so the blob
/// channel is exercised across the wire and journal alongside the scalars.
const STUB_STATS_BLOB: &[u8] = b"stub";

/// Observational stats: the attempt number as a named scalar, plus the fixed
/// blob marker. Journal-bound, never identity-bearing, so it legitimately
/// varies with the execution context.
fn stats(ctx: &ExecutionContext) -> Stats {
    Stats {
        scalars: vec![("attempt".to_string(), f64::from(ctx.attempt))],
        blob: STUB_STATS_BLOB.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_contracts::{NoCheckpoint, WorkerId};
    use sima_model::{EnvironmentId, Params, Spec};

    fn spec_for(behavior: StubBehavior, nonce: u64) -> Result<Spec> {
        Ok(Spec {
            format: FormatId::new("stub.v1")?,
            bytes: StubProgram { behavior, nonce }.to_bytes(),
        })
    }

    fn env() -> EnvironmentId {
        EnvironmentId::from_hash(hash_bytes(b"env"))
    }

    fn params() -> Params {
        Params {
            bytes: vec![1, 2, 3],
        }
    }

    fn ctx(attempt: u32, worker: u64) -> ExecutionContext {
        ExecutionContext {
            attempt,
            worker: WorkerId(worker),
        }
    }

    /// The value of the named scalar in `stats`; panics when it is absent.
    fn scalar(stats: &Stats, name: &str) -> f64 {
        stats
            .scalars
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| *v)
            .unwrap_or_else(|| panic!("scalar {name} present"))
    }

    /// The single artifact of a `Completed` outcome; panics otherwise.
    fn artifact(outcome: &Outcome) -> &Artifact {
        match outcome {
            Outcome::Completed { artifacts, .. } => {
                assert_eq!(artifacts.len(), 1, "stub emits exactly one artifact");
                &artifacts[0]
            }
            Outcome::Failed { reason, .. } => panic!("expected Completed, got Failed: {reason}"),
            Outcome::Rejected { reason, .. } => {
                panic!("expected Completed, got Rejected: {reason}")
            }
        }
    }

    #[test]
    fn succeed_produces_the_identity_artifact() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Succeed, 0)?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 5,
            environment: env(),
            input_state: None,
        };
        let outcome = exec.execute(&input, &ctx(0, 0), &NoCheckpoint)?;
        let artifact = artifact(&outcome);
        assert_eq!(artifact.name, "output");
        assert_eq!(artifact.bytes.len(), Hash::LEN);
        Ok(())
    }

    #[test]
    fn artifact_is_independent_of_execution_context() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Succeed, 0)?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 5,
            environment: env(),
            input_state: None,
        };
        let mut artifacts = Vec::new();
        let mut stats_by_attempt = Vec::new();
        for (attempt, worker) in [(0u32, 0u64), (1, 1), (5, 99)] {
            let outcome = exec.execute(&input, &ctx(attempt, worker), &NoCheckpoint)?;
            match outcome {
                Outcome::Completed {
                    artifacts: a,
                    stats,
                } => {
                    artifacts.push(a);
                    stats_by_attempt.push(stats);
                }
                Outcome::Failed { reason, .. } => {
                    panic!("expected Completed, got Failed: {reason}")
                }
                Outcome::Rejected { reason, .. } => {
                    panic!("expected Completed, got Rejected: {reason}")
                }
            }
        }
        // Artifacts identical across every (attempt, worker) pairing.
        assert_eq!(artifacts[0], artifacts[1]);
        assert_eq!(artifacts[0], artifacts[2]);
        // Stats carry the attempt, so they differ where the attempt differs.
        assert_ne!(stats_by_attempt[0], stats_by_attempt[1]);
        assert_ne!(stats_by_attempt[1], stats_by_attempt[2]);
        Ok(())
    }

    #[test]
    fn flaky_fails_before_the_count() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Flaky(3), 0)?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 5,
            environment: env(),
            input_state: None,
        };
        for attempt in [0u32, 1, 2] {
            assert!(matches!(
                exec.execute(&input, &ctx(attempt, 0), &NoCheckpoint)?,
                Outcome::Failed { .. }
            ));
        }
        Ok(())
    }

    #[test]
    fn flaky_completes_at_and_after_the_count() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Flaky(3), 0)?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 5,
            environment: env(),
            input_state: None,
        };
        let at_three = exec.execute(&input, &ctx(3, 0), &NoCheckpoint)?;
        let at_four = exec.execute(&input, &ctx(4, 7), &NoCheckpoint)?;
        // The eventual artifact does not depend on which attempt reached it.
        assert_eq!(artifact(&at_three), artifact(&at_four));
        Ok(())
    }

    #[test]
    fn panic_behavior_panics() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Panic, 0)?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 5,
            environment: env(),
            input_state: None,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            exec.execute(&input, &ctx(0, 0), &NoCheckpoint)
        }));
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn sleep_behavior_completes() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Sleep(0), 0)?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 5,
            environment: env(),
            input_state: None,
        };
        assert!(matches!(
            exec.execute(&input, &ctx(0, 0), &NoCheckpoint)?,
            Outcome::Completed { .. }
        ));
        Ok(())
    }

    #[test]
    fn reject_returns_a_rejected_outcome_with_stats() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Reject, 0)?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 5,
            environment: env(),
            input_state: None,
        };
        match exec.execute(&input, &ctx(2, 0), &NoCheckpoint)? {
            Outcome::Rejected { reason, stats } => {
                assert_eq!(reason, "programmed rejection");
                // The stub folds the attempt into the observational scalars.
                assert_eq!(scalar(&stats, "attempt"), 2.0);
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn flaky_failure_carries_observational_stats() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Flaky(2), 0)?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 5,
            environment: env(),
            input_state: None,
        };
        // A transient failure still reports stats, and they carry the attempt,
        // so successive attempts differ.
        let (first, second) = match (
            exec.execute(&input, &ctx(0, 0), &NoCheckpoint)?,
            exec.execute(&input, &ctx(1, 0), &NoCheckpoint)?,
        ) {
            (Outcome::Failed { stats: a, .. }, Outcome::Failed { stats: b, .. }) => (a, b),
            other => panic!("expected two Failed outcomes, got {other:?}"),
        };
        assert_eq!(scalar(&first, "attempt"), 0.0);
        assert_ne!(first, second);
        Ok(())
    }

    #[test]
    fn input_state_participates_in_identity() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Succeed, 0)?;
        let params = params();
        let make = |state: Option<&'static [u8]>| TaskInput {
            spec: &spec,
            params: &params,
            seed: 5,
            environment: env(),
            input_state: state,
        };
        let none = exec.execute(&make(None), &ctx(0, 0), &NoCheckpoint)?;
        let a = exec.execute(&make(Some(&[1, 2, 3])), &ctx(0, 0), &NoCheckpoint)?;
        let b = exec.execute(&make(Some(&[4, 5, 6])), &ctx(0, 0), &NoCheckpoint)?;
        // Three distinct input states yield three distinct artifacts.
        assert_ne!(artifact(&none), artifact(&a));
        assert_ne!(artifact(&none), artifact(&b));
        assert_ne!(artifact(&a), artifact(&b));
        // The same input state reproduces the same artifact.
        let a_again = exec.execute(&make(Some(&[1, 2, 3])), &ctx(9, 3), &NoCheckpoint)?;
        assert_eq!(artifact(&a), artifact(&a_again));
        Ok(())
    }

    #[test]
    fn distinct_identity_inputs_yield_distinct_artifacts() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Succeed, 0)?;
        let params = params();
        let base = exec.execute(
            &TaskInput {
                spec: &spec,
                params: &params,
                seed: 5,
                environment: env(),
                input_state: None,
            },
            &ctx(0, 0),
            &NoCheckpoint,
        )?;

        // Varying the spec (a different nonce mints a different spec id).
        let spec2 = spec_for(StubBehavior::Succeed, 1)?;
        let vary_spec = exec.execute(
            &TaskInput {
                spec: &spec2,
                params: &params,
                seed: 5,
                environment: env(),
                input_state: None,
            },
            &ctx(0, 0),
            &NoCheckpoint,
        )?;
        assert_ne!(artifact(&base), artifact(&vary_spec));

        // Varying the params.
        let params2 = Params { bytes: vec![9] };
        let vary_params = exec.execute(
            &TaskInput {
                spec: &spec,
                params: &params2,
                seed: 5,
                environment: env(),
                input_state: None,
            },
            &ctx(0, 0),
            &NoCheckpoint,
        )?;
        assert_ne!(artifact(&base), artifact(&vary_params));

        // Varying the seed.
        let vary_seed = exec.execute(
            &TaskInput {
                spec: &spec,
                params: &params,
                seed: 6,
                environment: env(),
                input_state: None,
            },
            &ctx(0, 0),
            &NoCheckpoint,
        )?;
        assert_ne!(artifact(&base), artifact(&vary_seed));

        // Varying the environment.
        let vary_env = exec.execute(
            &TaskInput {
                spec: &spec,
                params: &params,
                seed: 5,
                environment: EnvironmentId::from_hash(hash_bytes(b"env2")),
                input_state: None,
            },
            &ctx(0, 0),
            &NoCheckpoint,
        )?;
        assert_ne!(artifact(&base), artifact(&vary_env));
        Ok(())
    }

    /// A scripted checkpoint double: serves preset resume bytes and records
    /// every offered payload by invoking the producer.
    struct ScriptedCheckpoint {
        resume: Option<Vec<u8>>,
        offers: std::cell::RefCell<Vec<Vec<u8>>>,
    }

    impl ScriptedCheckpoint {
        fn new(resume: Option<Vec<u8>>) -> ScriptedCheckpoint {
            ScriptedCheckpoint {
                resume,
                offers: std::cell::RefCell::new(Vec::new()),
            }
        }
    }

    impl Checkpoint for ScriptedCheckpoint {
        fn resume(&self) -> Option<&[u8]> {
            self.resume.as_deref()
        }

        fn offer(&self, produce: &dyn Fn() -> Vec<u8>) {
            self.offers.borrow_mut().push(produce());
        }
    }

    /// The reference trajectory: `acc` folded from `state.acc` through the
    /// absolute steps `[state.step, state.step + steps)`.
    fn fold(mut state: StubState, steps: u64) -> StubState {
        for _ in 0..steps {
            state.acc = sima_core::prng::derive(state.acc, state.step);
            state.step += 1;
        }
        state
    }

    /// Runs an `Accumulate(k)` task and returns its outcome.
    fn run_accumulate(
        k: u64,
        seed: u64,
        input_state: Option<&[u8]>,
        attempt: u32,
        checkpoint: &dyn Checkpoint,
    ) -> Result<Outcome> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::Accumulate(k), 0)?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed,
            environment: env(),
            input_state,
        };
        exec.execute(&input, &ctx(attempt, 0), checkpoint)
    }

    /// The `state` artifact bytes and decoded stats of a `Completed`
    /// accumulate outcome; panics otherwise.
    fn state_and_stats(outcome: &Outcome) -> (Vec<u8>, u32, u64) {
        let Outcome::Completed { artifacts, stats } = outcome else {
            panic!("expected Completed, got {outcome:?}");
        };
        assert_eq!(artifacts.len(), 1, "accumulate commits one artifact");
        assert_eq!(artifacts[0].name, sima_contracts::STATE_ARTIFACT);
        let attempt = scalar(stats, "attempt") as u32;
        let steps = scalar(stats, "steps") as u64;
        (artifacts[0].bytes.clone(), attempt, steps)
    }

    #[test]
    fn paced_accumulate_commits_the_bytes_accumulate_commits() -> Result<()> {
        // The pace is operational alone: the paced behavior's trajectory,
        // committed state, and step stats equal the unpaced behavior's.
        let exec = StubExecutor::new()?;
        let spec = spec_for(
            StubBehavior::PacedAccumulate {
                steps: 3,
                step_ms: 1,
            },
            0,
        )?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 42,
            environment: env(),
            input_state: None,
        };
        let paced = exec.execute(&input, &ctx(0, 0), &NoCheckpoint)?;
        let (state_bytes, _, steps) = state_and_stats(&paced);
        let unpaced = run_accumulate(3, 42, None, 0, &NoCheckpoint)?;
        assert_eq!(state_bytes, state_and_stats(&unpaced).0);
        assert_eq!(steps, 3);
        Ok(())
    }

    #[test]
    fn accumulate_initializes_from_the_seed() -> Result<()> {
        let outcome = run_accumulate(3, 42, None, 0, &NoCheckpoint)?;
        let (state_bytes, attempt, steps) = state_and_stats(&outcome);
        let expected = fold(StubState { step: 0, acc: 42 }, 3);
        assert_eq!(state_bytes, expected.to_bytes());
        assert_eq!(attempt, 0);
        assert_eq!(steps, 3);
        Ok(())
    }

    #[test]
    fn accumulate_continues_from_the_input_state() -> Result<()> {
        let mid = fold(StubState { step: 0, acc: 42 }, 2);
        let outcome = run_accumulate(2, 42, Some(&mid.to_bytes()), 1, &NoCheckpoint)?;
        let (state_bytes, attempt, steps) = state_and_stats(&outcome);
        assert_eq!(state_bytes, fold(mid, 2).to_bytes());
        assert_eq!(attempt, 1);
        assert_eq!(steps, 2);
        Ok(())
    }

    #[test]
    fn two_segments_equal_one_task_of_double_length() -> Result<()> {
        // The trajectory is keyed by the absolute step index, so where the
        // segmentation cut falls cannot change the final state.
        let first = run_accumulate(2, 7, None, 0, &NoCheckpoint)?;
        let (first_state, _, _) = state_and_stats(&first);
        let second = run_accumulate(2, 7, Some(&first_state), 0, &NoCheckpoint)?;
        let (segmented, _, _) = state_and_stats(&second);
        let whole = run_accumulate(4, 7, None, 0, &NoCheckpoint)?;
        let (unsegmented, _, _) = state_and_stats(&whole);
        assert_eq!(segmented, unsegmented);
        Ok(())
    }

    #[test]
    fn accumulate_offers_a_checkpoint_at_every_step() -> Result<()> {
        let handle = ScriptedCheckpoint::new(None);
        run_accumulate(3, 42, None, 0, &handle)?;
        let offers = handle.offers.borrow();
        // One offer per step, each carrying the state after that step.
        assert_eq!(offers.len(), 3);
        for (i, offered) in offers.iter().enumerate() {
            let expected = fold(StubState { step: 0, acc: 42 }, i as u64 + 1);
            assert_eq!(offered, &expected.to_bytes(), "offer {i}");
        }
        Ok(())
    }

    #[test]
    fn a_resume_checkpoint_in_range_is_adopted() -> Result<()> {
        // A saved state two steps in: the resumed attempt executes only the
        // remaining step, and the committed state is byte-identical to an
        // uninterrupted search.
        let saved = fold(StubState { step: 0, acc: 42 }, 2);
        let handle = ScriptedCheckpoint::new(Some(saved.to_bytes()));
        let outcome = run_accumulate(3, 42, None, 1, &handle)?;
        let (state_bytes, _, steps) = state_and_stats(&outcome);
        assert_eq!(steps, 1);
        let reference = run_accumulate(3, 42, None, 0, &NoCheckpoint)?;
        let (reference_bytes, _, reference_steps) = state_and_stats(&reference);
        assert_eq!(reference_steps, 3);
        assert_eq!(state_bytes, reference_bytes);
        Ok(())
    }

    #[test]
    fn a_stale_resume_checkpoint_is_ignored() -> Result<()> {
        // Out-of-range saved steps — before this segment's start and at or
        // past its end — and undecodable bytes are all ignored: the task
        // runs fully and commits the reference state.
        let start = fold(StubState { step: 0, acc: 42 }, 2);
        let reference = run_accumulate(2, 42, Some(&start.to_bytes()), 0, &NoCheckpoint)?;
        let (reference_bytes, _, _) = state_and_stats(&reference);
        let stale = [
            fold(StubState { step: 0, acc: 42 }, 1).to_bytes(), // before start
            fold(StubState { step: 0, acc: 42 }, 4).to_bytes(), // at the end
            b"garbage".to_vec(),
        ];
        for bytes in stale {
            let handle = ScriptedCheckpoint::new(Some(bytes));
            let outcome = run_accumulate(2, 42, Some(&start.to_bytes()), 1, &handle)?;
            let (state_bytes, _, steps) = state_and_stats(&outcome);
            assert_eq!(steps, 2, "a stale checkpoint must not shorten the search");
            assert_eq!(state_bytes, reference_bytes);
        }
        Ok(())
    }

    #[test]
    fn accumulate_rejects_malformed_input_state() -> Result<()> {
        // Malformed input state is an infrastructure fault, like a malformed
        // spec: the identity referenced bytes that are not a stub state.
        assert!(matches!(
            run_accumulate(2, 42, Some(b"not a state"), 0, &NoCheckpoint),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn malformed_spec_is_an_error() -> Result<()> {
        let exec = StubExecutor::new()?;
        let params = params();
        for bytes in [vec![0xFF], Vec::new()] {
            let spec = Spec {
                format: FormatId::new("stub.v1")?,
                bytes,
            };
            let input = TaskInput {
                spec: &spec,
                params: &params,
                seed: 5,
                environment: env(),
                input_state: None,
            };
            assert!(matches!(
                exec.execute(&input, &ctx(0, 0), &NoCheckpoint),
                Err(Error::Validation(_))
            ));
        }
        Ok(())
    }
}
