//! The stub executor: evaluates a candidate by reading its programmed
//! behavior.
//!
//! [`StubExecutor`] decodes the [`StubProgram`] a spec carries and acts on it:
//! succeed, fail while below a threshold attempt, panic, or sleep. The one
//! committed [`Artifact`] is the digest of the identity inputs alone — a pure
//! function of [`TaskInput`] — so it never varies with the execution context.
//! The attempt number folds only into [`Stats`], the observational output, and
//! into the `FailThenSucceed` retry gate. This split is the boundary the whole
//! contract exists to enforce.

use std::time::Duration;

use sima_core::{Enc, Error, Hash, Result, hash_bytes};
use sima_model::FormatId;

use super::program::{StubBehavior, StubProgram};
use crate::executor::{Artifact, ExecutionContext, Executor, Outcome, Stats, TaskInput};

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

    fn execute(&self, input: &TaskInput<'_>, ctx: &ExecutionContext) -> Result<Outcome> {
        // A spec whose bytes are not a valid program is a structural input
        // fault, not a candidate failure: it is `Err`, not `Outcome::Failed`.
        let program = StubProgram::from_bytes(&input.spec.bytes)
            .map_err(|e| Error::Validation(format!("stub spec is not a valid program: {e}")))?;
        match program.behavior {
            StubBehavior::Succeed => Ok(completed(input, ctx)),
            StubBehavior::FailThenSucceed(n) => {
                // The one place the stub reads `ctx.attempt` to affect control
                // flow; the eventual artifact stays attempt-independent.
                if (ctx.attempt as u64) < n {
                    Ok(Outcome::Failed {
                        reason: format!("programmed failure: attempt {} of {}", ctx.attempt, n),
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
        }
    }
}

/// The successful outcome: the identity artifact plus attempt-bearing stats.
fn completed(input: &TaskInput<'_>, ctx: &ExecutionContext) -> Outcome {
    Outcome::Completed {
        artifacts: vec![result_artifact(input)],
        stats: stats(ctx),
    }
}

/// The one committed artifact, named `result`, carrying the identity digest's
/// 32 raw bytes (written through the canonical encoder, the public route to a
/// digest's bytes).
fn result_artifact(input: &TaskInput<'_>) -> Artifact {
    let mut enc = Enc::new();
    enc.hash(&identity_digest(input));
    Artifact {
        name: "result".to_string(),
        bytes: enc.finish(),
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

/// Observational stats: the attempt number, encoded. Journal-bound, never
/// identity-bearing, so it legitimately varies with the execution context.
fn stats(ctx: &ExecutionContext) -> Stats {
    let mut enc = Enc::new();
    enc.u32(ctx.attempt);
    Stats {
        bytes: enc.finish(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::WorkerId;
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

    /// The single artifact of a `Completed` outcome; panics otherwise.
    fn artifact(outcome: &Outcome) -> &Artifact {
        match outcome {
            Outcome::Completed { artifacts, .. } => {
                assert_eq!(artifacts.len(), 1, "stub emits exactly one artifact");
                &artifacts[0]
            }
            Outcome::Failed { reason } => panic!("expected Completed, got Failed: {reason}"),
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
        let outcome = exec.execute(&input, &ctx(0, 0))?;
        let artifact = artifact(&outcome);
        assert_eq!(artifact.name, "result");
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
            let outcome = exec.execute(&input, &ctx(attempt, worker))?;
            match outcome {
                Outcome::Completed { artifacts: a, stats } => {
                    artifacts.push(a);
                    stats_by_attempt.push(stats);
                }
                Outcome::Failed { reason } => panic!("expected Completed, got {reason}"),
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
    fn fail_then_succeed_fails_before_the_count() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::FailThenSucceed(3), 0)?;
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
                exec.execute(&input, &ctx(attempt, 0))?,
                Outcome::Failed { .. }
            ));
        }
        Ok(())
    }

    #[test]
    fn fail_then_succeed_completes_at_and_after_the_count() -> Result<()> {
        let exec = StubExecutor::new()?;
        let spec = spec_for(StubBehavior::FailThenSucceed(3), 0)?;
        let params = params();
        let input = TaskInput {
            spec: &spec,
            params: &params,
            seed: 5,
            environment: env(),
            input_state: None,
        };
        let at_three = exec.execute(&input, &ctx(3, 0))?;
        let at_four = exec.execute(&input, &ctx(4, 7))?;
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
            exec.execute(&input, &ctx(0, 0))
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
            exec.execute(&input, &ctx(0, 0))?,
            Outcome::Completed { .. }
        ));
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
        let none = exec.execute(&make(None), &ctx(0, 0))?;
        let a = exec.execute(&make(Some(&[1, 2, 3])), &ctx(0, 0))?;
        let b = exec.execute(&make(Some(&[4, 5, 6])), &ctx(0, 0))?;
        // Three distinct input states yield three distinct artifacts.
        assert_ne!(artifact(&none), artifact(&a));
        assert_ne!(artifact(&none), artifact(&b));
        assert_ne!(artifact(&a), artifact(&b));
        // The same input state reproduces the same artifact.
        let a_again = exec.execute(&make(Some(&[1, 2, 3])), &ctx(9, 3))?;
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
        )?;
        assert_ne!(artifact(&base), artifact(&vary_env));
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
                exec.execute(&input, &ctx(0, 0)),
                Err(Error::Validation(_))
            ));
        }
        Ok(())
    }
}
