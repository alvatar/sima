//! The task-source interface: it derives the runnable frontier from
//! `(config, store state)`.

use sima_contracts::Generator;
use sima_core::Result;
use sima_model::{RunConfig, Spec, SpecId, TaskIdentity, TaskKey};
use sima_store::Store;

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
    /// this repeatedly, leases outstanding or not, and the source returns each
    /// runnable task exactly once across the run: it tracks what it has handed
    /// out and watches the store for the commit. The static batch returns the
    /// full unanswered set on the first call and an empty vec thereafter; a
    /// chain source returns successors as their predecessors commit.
    fn poll(&mut self) -> Result<Vec<RunnableTask>>;

    /// The task keys the run comprises, as materialized so far. The set is
    /// complete once a poll has returned empty at an idle pool — the point at
    /// which the driver finalizes over exactly this set.
    fn all_keys(&self) -> &[TaskKey];

    /// The planned task count of the whole run, known at construction. Feeds
    /// the run-started report; unlike [`TaskSource::all_keys`], it never
    /// grows.
    fn task_total(&self) -> usize;
}

/// The construction prologue every task source shares: runs `generator` under
/// `config` and stores each spec object, returning each spec paired with its
/// id. The spec object is durable before any task referencing it can commit;
/// its address is the spec id (both are the blake3 of the spec's canonical
/// bytes).
pub(crate) fn generate_specs(
    generator: &dyn Generator,
    config: &RunConfig,
    store: &Store,
) -> Result<Vec<(Spec, SpecId)>> {
    let specs = generator.generate(config.root_seed, &config.generator.params, &config.format)?;
    specs
        .into_iter()
        .map(|spec| {
            let id = SpecId::from_hash(store.put(&spec.to_bytes())?);
            Ok((spec, id))
        })
        .collect()
}
