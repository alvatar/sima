//! The executor contract and its input/output surface.
//!
//! An executor is pure compute over one candidate. It receives two disjoint
//! input groups — [`TaskInput`], the identity-bearing inputs that determine
//! the task key and the committed artifacts, and [`ExecutionContext`], the
//! per-attempt facts it may read but must never fold into an artifact — plus
//! the attempt's [`Checkpoint`] resume channel, and returns an [`Outcome`]:
//! committed [`Artifact`]s plus observational [`Stats`], or a domain
//! [`Outcome::Failed`].

use sima_core::Result;
use sima_model::{EnvironmentId, FormatId, Params, Spec};

use crate::checkpoint::Checkpoint;

/// Artifact name under which a segmented executor commits its continuation
/// state. The next segment's task carries this artifact's object hash as
/// `input_state`, so the chain walks committed state hop by hop. A segmented
/// run over an executor that never commits it is a misconfiguration the
/// scheduler reports as a validation error.
pub const STATE_ARTIFACT: &str = "state";

/// Opaque worker label. An executor may read it (e.g. for logging) but must
/// never let it influence a committed artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId(pub u64);

/// The identity-bearing inputs of one evaluation: the resolved candidate and
/// its evaluation settings, the seed, the environment, and — for a segment —
/// the loaded bytes of the state object this task continues from. Every field
/// here determines the task key and the committed artifacts.
#[derive(Debug)]
pub struct TaskInput<'a> {
    /// The candidate under evaluation (resolved bytes, not just its id).
    pub spec: &'a Spec,
    /// The run parameters the evaluation runs under.
    pub params: &'a Params,
    /// The task's deterministic seed.
    pub seed: u64,
    /// The environment the results depend on.
    pub environment: EnvironmentId,
    /// Loaded bytes of the input-state object; `None` for a stateless task.
    /// The key carries this state's digest (`TaskIdentity.input_state`); the
    /// executor receives the bytes.
    pub input_state: Option<&'a [u8]>,
}

/// Execution context: visible to the executor, forbidden from influencing any
/// committed artifact. It may legitimately flow into [`Stats`], gate retryable
/// failure (the sanctioned `attempt` read), or drive logging — never into an
/// [`Artifact`].
#[derive(Debug, Clone, Copy)]
pub struct ExecutionContext {
    /// Zero-based attempt number: 0 is the first try.
    pub attempt: u32,
    /// The worker running this attempt.
    pub worker: WorkerId,
}

/// A produced artifact: a named blob the worker will store in the CAS and
/// reference from the `TaskRecord`. Its bytes must be a pure function of the
/// [`TaskInput`] identity — never of [`ExecutionContext`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    /// Artifact name; validated when the worker builds the `ArtifactRef`.
    /// Must satisfy the name rule (1..=64 bytes of `[a-z0-9._-]`).
    pub name: String,
    /// The produced bytes.
    pub bytes: Vec<u8>,
}

/// Observational statistics destined for the run journal: named scalars plus
/// an opaque family payload. Observational only — never enters a record, a
/// manifest, or any identity criterion — and may reflect execution context.
///
/// The `f64` scalars make `Stats` (and every type embedding it) `PartialEq`
/// only: a non-finite scalar never equals itself, matching IEEE-754.
#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    /// Named observational scalars, ordered as the executor emitted them. A
    /// value may be non-finite when a candidate diverged.
    pub scalars: Vec<(String, f64)>,
    /// Opaque family payload for anything richer than a scalar. Empty when the
    /// scalars carry everything.
    pub blob: Vec<u8>,
}

impl Stats {
    /// Stats carrying nothing: no scalars, an empty blob. The degraded result
    /// when even the reduction over a failed attempt could not run.
    pub fn empty() -> Stats {
        Stats {
            scalars: Vec::new(),
            blob: Vec::new(),
        }
    }
}

/// The result of one evaluation attempt. `PartialEq` only, since [`Stats`]
/// carries `f64` scalars.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// The candidate evaluated successfully: committed artifacts plus
    /// observational stats.
    Completed {
        artifacts: Vec<Artifact>,
        stats: Stats,
    },
    /// A transient failure: the attempt failed for a reason that may not
    /// recur. Retryable at the scheduler's discretion. `stats` is
    /// observational. The reason is observational as well: journal and
    /// reporting material, never identity-bearing.
    Failed { reason: String, stats: Stats },
    /// A definitive failure: the candidate cannot produce a result, so it is
    /// never retried. `stats` is observational. The reason is observational as
    /// well: journal and reporting material, never identity-bearing.
    Rejected { reason: String, stats: Stats },
}

/// Pure compute over one candidate. Receives identity inputs and execution
/// context as disjoint groups; returns produced artifacts (the phenotype the
/// spec's genotype expresses into) and stats, or a failure outcome. Never
/// touches the store.
pub trait Executor {
    /// The format id this executor interprets. The pipeline dispatches
    /// a run to the executor whose format matches the run config's format.
    fn format(&self) -> &FormatId;

    /// Evaluate one candidate. `Ok` carries the domain result — see
    /// [`Outcome`] for the three arms and their retry semantics. `Err` is
    /// reserved for an infrastructure fault — a structurally invalid spec, a
    /// store fault — never a candidate that merely evaluated badly.
    ///
    /// `checkpoint` is the attempt's resume channel — see [`Checkpoint`] for
    /// the contract. Stateless executors ignore it.
    fn execute(
        &self,
        input: &TaskInput<'_>,
        ctx: &ExecutionContext,
        checkpoint: &dyn Checkpoint,
    ) -> Result<Outcome>;
}

/// `Executor` is dyn-compatible: it carries no auto-trait supertraits, and
/// use sites add `Send`/`Sync` where they store it as a trait object.
const _: fn() = || {
    fn _object_safe(_: &dyn Executor) {}
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_id_is_a_transparent_u64_label() {
        let worker = WorkerId(7);
        assert_eq!(worker.0, 7);
        assert_eq!(worker, WorkerId(7));
        assert_ne!(worker, WorkerId(8));
    }

    #[test]
    fn stats_carry_named_scalars_and_a_blob() {
        let stats = Stats {
            scalars: vec![
                ("population".to_string(), 0.5),
                ("activity".to_string(), 1e-4),
            ],
            blob: vec![0xAA, 0xBB],
        };
        assert_eq!(stats.scalars.len(), 2);
        assert_eq!(stats.scalars[0], ("population".to_string(), 0.5));
        assert_eq!(stats.blob, vec![0xAA, 0xBB]);
    }

    #[test]
    fn empty_stats_carry_nothing() {
        let stats = Stats::empty();
        assert!(stats.scalars.is_empty());
        assert!(stats.blob.is_empty());
    }

    #[test]
    fn every_outcome_arm_carries_stats() {
        // The contract keeps stats symmetric across the three arms; a
        // non-finite scalar is representable, so a diverged candidate still
        // reports what it computed.
        let stats = || Stats {
            scalars: vec![("c0.max".to_string(), f64::NAN)],
            blob: Vec::new(),
        };
        let arms = [
            Outcome::Completed {
                artifacts: Vec::new(),
                stats: stats(),
            },
            Outcome::Failed {
                reason: "transient".to_string(),
                stats: stats(),
            },
            Outcome::Rejected {
                reason: "definitive".to_string(),
                stats: stats(),
            },
        ];
        for arm in arms {
            let carried = match arm {
                Outcome::Completed { stats, .. }
                | Outcome::Failed { stats, .. }
                | Outcome::Rejected { stats, .. } => stats,
            };
            assert_eq!(carried.scalars.len(), 1);
            assert!(carried.scalars[0].1.is_nan());
        }
    }
}
