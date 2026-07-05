//! The executor contract and its input/output surface.
//!
//! An executor is pure compute over one candidate. It receives two disjoint
//! input groups — [`TaskInput`], the identity-bearing inputs that determine
//! the task key and the committed artifacts, and [`ExecutionContext`], the
//! per-attempt facts it may read but must never fold into an artifact — and
//! returns an [`Outcome`]: committed [`Artifact`]s plus observational
//! [`Stats`], or a domain [`Outcome::Failed`].

use sima_core::Result;
use sima_model::{EnvironmentId, FormatId, Params, Spec};

/// Opaque worker label. An executor may read it (e.g. for logging) but must
/// never let it influence a committed artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId(pub u64);

/// The identity-bearing inputs of one evaluation: the resolved candidate and
/// its evaluation settings, the seed, the environment, and — for a segment —
/// the loaded bytes of the state object this task continues from. Every field
/// here determines the task key and the committed artifacts.
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
    /// executor receives the bytes. Unused by the stub except as identity.
    pub input_state: Option<&'a [u8]>,
}

/// Execution context: visible to the executor, forbidden from influencing any
/// committed artifact. It may legitimately flow into [`Stats`], gate retryable
/// failure (the sanctioned `attempt` read), or drive logging — never into an
/// [`Artifact`].
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
    /// Artifact name; validated when the worker builds the `ArtifactRef`
    /// (M1.5). Must satisfy the name rule (1..=64 bytes of `[a-z0-9._-]`).
    pub name: String,
    /// The produced bytes.
    pub bytes: Vec<u8>,
}

/// Observational statistics: opaque, non-identity-bearing bytes destined for
/// the run journal. May reflect execution context. Never enters a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    /// Executor-defined observational payload.
    pub bytes: Vec<u8>,
}

/// The result of one evaluation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The candidate evaluated successfully: committed artifacts plus
    /// observational stats.
    Completed {
        artifacts: Vec<Artifact>,
        stats: Stats,
    },
    /// The candidate evaluation failed. Retryable at the scheduler's
    /// discretion; the reason is observational.
    Failed { reason: String },
}

/// Pure compute over one candidate. Receives identity inputs and execution
/// context as disjoint groups; returns produced artifacts and stats, or a
/// failure outcome. Never touches the store.
pub trait Executor {
    /// The format id this executor interprets. The pipeline (M1.6) dispatches
    /// a run to the executor whose format matches the run config's format.
    fn format(&self) -> &FormatId;

    /// Evaluate one candidate. `Err` signals an infrastructure fault (e.g. a
    /// structurally invalid spec); a failed-but-well-formed evaluation is
    /// `Ok(Outcome::Failed { .. })`.
    fn execute(&self, input: &TaskInput<'_>, ctx: &ExecutionContext) -> Result<Outcome>;
}

/// `Executor` is dyn-compatible: it carries no auto-trait supertraits, and
/// use sites add `Send`/`Sync` where they store it as a trait object (D7).
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
}
