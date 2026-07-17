//! [`StaticBatch`]: the task source that drives a generator once.

use sima_contracts::Generator;
use sima_core::{Result, prng};
use sima_model::{Environment, RunConfig, TaskIdentity, TaskKey};
use sima_store::Store;

use crate::task_source::{RunnableTask, TaskSource, generate_specs};

/// A task source over a fixed batch of candidates. On construction it
/// materializes the frontier: it generates the run's specs, stores each spec
/// object, derives every task identity, and separates the tasks the store
/// already answers from those still to run. The frontier is a pure function of
/// `(config, environment)`, so it is identical across fresh stores. Resume is
/// this same construction — every start re-derives the full task set from
/// `(config, environment)` and skips the keys the store already answers, with
/// no checkpoint and no separate resume mode.
pub struct StaticBatch {
    /// The tasks not yet committed, handed out on the first poll.
    runnable: Vec<RunnableTask>,
    /// Every task key the batch comprises, committed or not.
    all_keys: Vec<TaskKey>,
    /// The keys the store already answered at construction.
    prior_committed: usize,
    /// Whether the runnable set has been handed out.
    polled: bool,
}

impl StaticBatch {
    /// Materializes the batch: runs `generator` under `config`, stores each
    /// spec object, builds each task identity against `environment`, and
    /// filters out the keys `store` already answers so a resume runs only the
    /// unfinished work.
    pub fn new(
        generator: &dyn Generator,
        config: &RunConfig,
        environment: &Environment,
        store: &Store,
    ) -> Result<StaticBatch> {
        let specs = generate_specs(generator, config, store)?;
        let params = config.params.id();
        let environment_id = environment.id();
        let mut all_keys = Vec::with_capacity(specs.len());
        let mut runnable = Vec::new();
        for (i, (spec, spec_id)) in specs.into_iter().enumerate() {
            let identity = TaskIdentity {
                spec: spec_id,
                params,
                // The per-task seed is a deterministic substream of the run
                // seed, keyed by the candidate's index in the generator output.
                seed: prng::derive(config.root_seed, i as u64),
                environment: environment_id,
                input_state: None,
            };
            let key = identity.key();
            all_keys.push(key);
            // An existence check, not a read: resuming a mostly-complete run
            // must not decode every committed record to answer a boolean.
            if !store.has_record(&key)? {
                runnable.push(RunnableTask {
                    spec,
                    identity,
                    chain: None,
                });
            }
        }
        Ok(StaticBatch {
            prior_committed: all_keys.len() - runnable.len(),
            runnable,
            all_keys,
            polled: false,
        })
    }
}

impl TaskSource for StaticBatch {
    fn poll(&mut self) -> Result<Vec<RunnableTask>> {
        if self.polled {
            return Ok(Vec::new());
        }
        self.polled = true;
        Ok(std::mem::take(&mut self.runnable))
    }

    fn all_keys(&self) -> &[TaskKey] {
        &self.all_keys
    }

    fn task_total(&self) -> usize {
        self.all_keys.len()
    }

    fn prior_committed(&self) -> usize {
        self.prior_committed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_domains::{StubBehavior, StubGenerator, StubGeneratorConfig};
    use sima_model::{GeneratorConfig, Params};

    /// A run config whose generator programs the given behaviors.
    fn config(behaviors: Vec<StubBehavior>) -> Result<RunConfig> {
        Ok(RunConfig {
            root_seed: 7,
            segments: None,
            format: sima_model::FormatId::new("stub.v1")?,
            generator: GeneratorConfig {
                id: sima_model::GeneratorId::new("stub.v1")?,
                params: StubGeneratorConfig { behaviors }.to_bytes(),
            },
            params: Params {
                bytes: vec![1, 2, 3],
            },
        })
    }

    /// A one-component stub environment.
    fn environment() -> Result<Environment> {
        Environment::new(vec![sima_model::EnvironmentComponent::new(
            "executor",
            sima_model::EnvironmentValue::Version("stub.v1".to_string()),
        )?])
    }

    fn temp_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open store");
        (dir, store)
    }

    #[test]
    fn first_poll_returns_all_tasks_then_empties() -> Result<()> {
        let (_dir, store) = temp_store();
        let generator = StubGenerator::new()?;
        let config = config(vec![StubBehavior::Succeed, StubBehavior::Succeed])?;
        let mut batch = StaticBatch::new(&generator, &config, &environment()?, &store)?;
        assert_eq!(batch.all_keys().len(), 2);
        assert_eq!(batch.poll()?.len(), 2);
        assert!(batch.poll()?.is_empty());
        Ok(())
    }

    #[test]
    fn frontier_is_identical_across_fresh_stores() -> Result<()> {
        let generator = StubGenerator::new()?;
        let config = config(vec![StubBehavior::Succeed, StubBehavior::Flaky(1)])?;
        let env = environment()?;
        let (_a, store_a) = temp_store();
        let (_b, store_b) = temp_store();
        let batch_a = StaticBatch::new(&generator, &config, &env, &store_a)?;
        let batch_b = StaticBatch::new(&generator, &config, &env, &store_b)?;
        assert_eq!(batch_a.all_keys(), batch_b.all_keys());
        Ok(())
    }

    #[test]
    fn a_committed_key_is_excluded_from_the_runnable_set() -> Result<()> {
        let (_dir, store) = temp_store();
        let generator = StubGenerator::new()?;
        let config = config(vec![StubBehavior::Succeed])?;
        let env = environment()?;
        // Learn the sole task's key, then commit a record for it directly.
        let key = StaticBatch::new(&generator, &config, &env, &store)?.all_keys()[0];
        let batch = StaticBatch::new(&generator, &config, &env, &store)?;
        // Nothing committed yet: the one task is runnable.
        assert_eq!(batch.all_keys(), &[key]);

        let mut batch = batch;
        let task = batch.poll()?.pop().expect("one runnable task");
        store.put(&config.params.to_bytes())?;
        store.put(&env.to_bytes())?;
        let record = sima_model::TaskRecord::new(task.identity, Vec::new())?;
        store.commit_record(&record)?;

        // A fresh batch over the same config now skips the committed key.
        let resumed = StaticBatch::new(&generator, &config, &env, &store)?;
        assert_eq!(resumed.all_keys(), &[key]);
        let mut resumed = resumed;
        assert!(resumed.poll()?.is_empty());
        Ok(())
    }

    #[test]
    fn prior_commits_are_counted_from_the_records_the_store_holds() -> Result<()> {
        let (_dir, store) = temp_store();
        let generator = StubGenerator::new()?;
        let config = config(vec![StubBehavior::Succeed, StubBehavior::Succeed])?;
        let env = environment()?;
        // A fresh store answers nothing.
        assert_eq!(
            StaticBatch::new(&generator, &config, &env, &store)?.prior_committed(),
            0
        );

        // Commit one of the two records straight to the store, which writes
        // no journal line: the state a crash between a record write and its
        // journal append leaves behind.
        let mut batch = StaticBatch::new(&generator, &config, &env, &store)?;
        let task = batch.poll()?.remove(0);
        store.put(&config.params.to_bytes())?;
        store.put(&env.to_bytes())?;
        store.commit_record(&sima_model::TaskRecord::new(task.identity, Vec::new())?)?;

        // The count follows the records, so it is exact where a journal
        // replay would come up short.
        let resumed = StaticBatch::new(&generator, &config, &env, &store)?;
        assert_eq!(resumed.prior_committed(), 1);
        assert_eq!(resumed.task_total(), 2);
        Ok(())
    }
}
