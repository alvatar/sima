//! The task-source interface: it derives the runnable frontier from
//! `(config, store state)`.

use sima_contracts::Generator;
use sima_core::{Error, Result};
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
    /// The chain this task belongs to, when a chain source derived it; the
    /// worker keys the run's checkpoint slot by it. Stateless tasks carry
    /// `None` and get no slot.
    pub chain: Option<u64>,
}

/// Derives the currently-runnable tasks of a run from `(config, store state)`.
///
/// One interface covers both a static batch and a segment chain that derives
/// successors as predecessors commit — which is why frontier derivation
/// belongs to this layer rather than to whatever produced the candidates.
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

    /// How many of the run's tasks the store already answered when this
    /// source derived its frontier: what earlier sessions committed. Feeds
    /// the run-started report, whose display counts on from it.
    fn prior_committed(&self) -> usize;
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
    let specs = generator.generate(config.root_seed, &config.generator.params)?;
    specs
        .into_iter()
        .map(|spec| {
            // A generator stamps its own format, so a run pairing one with a
            // domain that reads another format is caught here rather than at
            // the first task, where the bytes would be read as the wrong thing.
            if spec.format != config.format {
                return Err(Error::Validation(format!(
                    "generator {:?} produced a spec of format {:?}, and the run is over {:?}",
                    config.generator.id.as_str(),
                    spec.format.as_str(),
                    config.format.as_str()
                )));
            }
            let id = SpecId::from_hash(store.put(&spec.to_bytes())?);
            Ok((spec, id))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params};

    use super::*;

    /// A generator producing one spec of the format it is told to stamp, so a
    /// test can present a run with a candidate of a format it never asked for
    /// — what a program answering the generate question is free to return.
    struct StampingGenerator {
        id: GeneratorId,
        stamped: FormatId,
    }

    impl Generator for StampingGenerator {
        fn id(&self) -> &GeneratorId {
            &self.id
        }

        fn format(&self) -> &FormatId {
            &self.stamped
        }

        fn translate_config(&self, _toml: &str) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        fn generate(&self, _root_seed: u64, _params: &[u8]) -> Result<Vec<Spec>> {
            Ok(vec![Spec {
                format: self.stamped.clone(),
                bytes: vec![7],
            }])
        }
    }

    /// A run over `format`, its generator named and its params empty.
    fn config(format: &str) -> RunConfig {
        RunConfig {
            root_seed: 42,
            segments: None::<NonZeroU64>,
            format: FormatId::new(format).expect("format id"),
            generator: GeneratorConfig {
                id: GeneratorId::new("acme.gen.v1").expect("generator id"),
                params: Vec::new(),
            },
            params: Params { bytes: Vec::new() },
        }
    }

    /// The generator stamping `format`, under the id the config names.
    fn generator(format: &str) -> StampingGenerator {
        StampingGenerator {
            id: GeneratorId::new("acme.gen.v1").expect("generator id"),
            stamped: FormatId::new(format).expect("format id"),
        }
    }

    #[test]
    fn a_spec_of_the_run_s_format_is_stored_and_addressed_by_its_bytes() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let specs = generate_specs(&generator("stub.v1"), &config("stub.v1"), &store)?;
        let [(spec, id)] = specs.as_slice() else {
            panic!("one candidate, got {specs:?}");
        };
        assert_eq!(spec.format.as_str(), "stub.v1");
        assert_eq!(
            *id,
            SpecId::from_hash(sima_core::hash_bytes(&spec.to_bytes()))
        );
        Ok(())
    }

    #[test]
    fn a_spec_of_another_format_is_refused_naming_both_formats() -> Result<()> {
        // The candidate a generator returns is a value that crossed a wire
        // when a program produced it, so what the run executes is checked
        // against what the run is over — before the bytes are read as the
        // wrong thing, and before the spec object is stored.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let Err(error) = generate_specs(&generator("acme.thing.v1"), &config("stub.v1"), &store)
        else {
            panic!("expected a spec of another format to be refused");
        };
        let message = error.to_string();
        assert!(
            message.contains("acme.thing.v1"),
            "names the format produced: {message}"
        );
        assert!(
            message.contains("stub.v1"),
            "names the run's format: {message}"
        );
        assert!(
            message.contains("acme.gen.v1"),
            "names the generator: {message}"
        );
        Ok(())
    }
}
