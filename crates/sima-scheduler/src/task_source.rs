//! The task-source interface: it derives the runnable frontier from
//! `(config, store state)`.

use sima_core::Result;
use sima_model::{Spec, TaskIdentity, TaskKey};

/// A runnable task: the resolved candidate and its identity. The spec bytes
/// travel with the task so the worker builds a [`sima_contracts::TaskInput`]
/// without a store read.
#[derive(Debug, Clone)]
pub struct RunnableTask {
    /// The candidate under evaluation, resolved to its bytes.
    pub spec: Spec,
    /// The identity whose evaluation this task commits.
    pub identity: TaskIdentity,
}

/// Derives the currently-runnable tasks of a run from `(config, store state)`.
///
/// One interface covers both a static batch and, in a later phase, a segment
/// chain that derives successors as predecessors commit — which is why
/// frontier derivation belongs to this layer rather than to whatever produced
/// the candidates.
pub trait TaskSource {
    /// Return the tasks runnable now and not yet handed out. The driver calls
    /// this repeatedly: the static batch returns the full unanswered set on
    /// the first call and an empty vec thereafter; a chain source returns
    /// successors as their predecessors commit.
    fn poll(&mut self) -> Result<Vec<RunnableTask>>;

    /// The full set of task keys the run comprises, materialized when the
    /// frontier is. The driver finalizes over exactly this set.
    fn all_keys(&self) -> &[TaskKey];
}
