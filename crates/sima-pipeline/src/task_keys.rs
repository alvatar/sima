//! `task_keys`: the task keys a loaded config's run comprises over a store.
//!
//! The pipeline half of the scheduler's own derivation: it reads the run's
//! environment and generator from the source that answers for its format, and
//! hands them to [`sima_scheduler::run_keys`]. Both halves of a store sync need
//! this set, and each derives it independently from `(config, store state)` —
//! no key list crosses the wire, so the sync protocol stays as it is.

use sima_core::Result;
use sima_model::TaskKey;
use sima_store::Store;

use crate::config::LoadedConfig;

/// The task keys `config`'s run comprises, as `store`'s current state
/// materializes them.
///
/// Deriving them **writes the run's spec objects to `store`**, since the
/// derivation constructs the run's task source; the write is idempotent, and
/// nothing else about the store changes — no run is registered, no record is
/// committed, and no journal line is appended. `store` is the caller's, so a
/// caller deriving over a far side's store passes that one.
pub fn task_keys(config: &LoadedConfig, store: &Store) -> Result<Vec<TaskKey>> {
    let source = config.domains.source(&config.run.format);
    let environment = source.environment(&config.run.format)?;
    let generator = source.generator(&config.run.generator.id, &config.run.format)?;
    sima_scheduler::run_keys(store, &config.run, &environment, generator.as_ref())
}

#[cfg(test)]
mod tests {
    use sima_domains::StubGenerator;

    use super::*;
    use crate::fixtures::load_str;

    /// A stub config over `segments` chained tasks per candidate, storing into
    /// `store`.
    fn config(segments: Option<u64>) -> String {
        let segments = segments.map_or(String::new(), |n| format!("segments = {n}\n"));
        format!(
            r#"
            [run]
            root_seed = 4
            format = "stub.v1"
            {segments}
            [run.generator]
            id = "stub.v1"
            behaviors = ["succeed", "succeed", "succeed"]

            [config]
            store = "./store"
            max_attempts = 1

            [orchestrator]
            workers = 1
            "#
        )
    }

    #[test]
    fn the_keys_agree_with_the_scheduler_s_own_derivation() -> Result<()> {
        // One derivation, reached two ways: the pipeline reads the environment
        // and generator the config names, and the scheduler does the rest.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let loaded = load_str(&config(None));
        let generator = StubGenerator::new()?;
        assert_eq!(
            task_keys(&loaded, &store)?,
            sima_scheduler::run_keys(
                &store,
                &loaded.run,
                &crate::fixtures::stub_environment(),
                &generator
            )?
        );
        Ok(())
    }

    #[test]
    fn a_batch_over_an_empty_store_names_one_key_per_candidate() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        assert_eq!(task_keys(&load_str(&config(None)), &store)?.len(), 3);
        Ok(())
    }

    #[test]
    fn a_chain_over_an_empty_store_names_its_first_segments() -> Result<()> {
        // Forward-only traversal: without a committed predecessor there is no
        // successor key to derive, so a six-segment chain still starts at one
        // key per candidate.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        assert_eq!(task_keys(&load_str(&config(Some(6))), &store)?.len(), 3);
        Ok(())
    }

    #[test]
    fn an_unknown_format_is_a_dispatch_error_rather_than_an_empty_set() -> Result<()> {
        // The dispatch is the pipeline's half, so a config naming a format no
        // build carries fails here rather than answering nothing.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let mut loaded = load_str(&config(None));
        loaded.run.format = sima_model::FormatId::new("no-such-domain.v1")?;
        assert!(task_keys(&loaded, &store).is_err());
        Ok(())
    }
}
