//! The task keys a search comprises over a store, derived two ways.
//!
//! [`task_keys`] is the pipeline half of the scheduler's own derivation: it
//! reads the search's environment and generator from the source that answers for
//! its format, and hands them to [`sima_scheduler::search_keys`]. It is what a
//! side of a sync that holds the config derives — no key list crosses the
//! wire, so the sync protocol stays as it is.
//!
//! [`journaled_keys`] derives the same kind of set from the search's journal
//! alone, for the side of a sync that must not load a config. A far side
//! serving a sync is delivering the program its config would spawn, so
//! loading that config is the one thing it cannot do; its journal names every
//! task it has state for, which is what a sync has to advertise.

use std::collections::BTreeSet;

use sima_core::Result;
use sima_model::{SearchId, TaskKey};
use sima_scheduler::Record;
use sima_store::Store;

use crate::config::LoadedConfig;
use crate::task_history::lifecycle_task;

/// The task keys `config`'s search comprises, as `store`'s current state
/// materializes them.
///
/// Deriving them **writes the search's spec objects to `store`**, since the
/// derivation constructs the search's task source; the write is idempotent, and
/// nothing else about the store changes — no search is registered, no record is
/// committed, and no journal line is appended. `store` is the caller's, so a
/// caller deriving over a far side's store passes that one.
pub fn task_keys(config: &LoadedConfig, store: &Store) -> Result<Vec<TaskKey>> {
    let source = config.domains.source(&config.search.format);
    let environment = source.environment(&config.search.format)?;
    let generator = source.generator(&config.search.generator.id, &config.search.format)?;
    sima_scheduler::search_keys(store, &config.search, &environment, generator.as_ref())
}

/// The task keys `search`'s journal names in `store`: every key a lifecycle event
/// belongs to, ordered and deduplicated.
///
/// This is exactly the set with state on this side. A record or a checkpoint
/// exists only for a task the search queued, and queueing is journaled, so a key
/// the journal never named references nothing here and has nothing to
/// advertise. A search that journaled nothing yields the empty set, which is what
/// a store about to receive its first push holds.
///
/// **A line that does not parse is skipped**, the rule
/// [`crate::program_binding`] reads the journal under: it is observational, a
/// crash can tear its final write, and a torn line states nothing about which
/// tasks ran. A task field that is not a key is skipped for the same reason —
/// what it cost is one key not advertised, which leaves the search resumable,
/// while refusing would fail a whole transfer over one damaged line.
pub(crate) fn journaled_keys(store: &Store, search: &SearchId) -> Result<Vec<TaskKey>> {
    let mut keys = BTreeSet::new();
    for line in store.journal(search)? {
        let Ok(record) = Record::from_line(&line) else {
            continue;
        };
        let Some(task) = lifecycle_task(&record.event) else {
            continue;
        };
        if let Ok(key) = TaskKey::from_hex(task) {
            keys.insert(key);
        }
    }
    Ok(keys.into_iter().collect())
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
            [search]
            root_seed = 4
            format = "stub.v1"
            {segments}
            [search.generator]
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
            sima_scheduler::search_keys(
                &store,
                &loaded.search,
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
        loaded.search.format = sima_model::FormatId::new("no-such-domain.v1")?;
        assert!(task_keys(&loaded, &store).is_err());
        Ok(())
    }

    // ---- The journal's own account of what a store holds ----

    /// A stub config whose candidates accumulate, so a segmented search of them
    /// commits the continuation state each next segment starts from.
    fn chained(segments: u64) -> String {
        format!(
            r#"
            [search]
            root_seed = 4
            format = "stub.v1"
            segments = {segments}

            [search.generator]
            id = "stub.v1"
            behaviors = ["accumulate:2", "accumulate:2", "accumulate:2"]

            [config]
            store = "./store"
            max_attempts = 1

            [orchestrator]
            workers = 1
            "#
        )
    }

    #[test]
    fn a_driven_run_journals_every_key_it_comprises() -> Result<()> {
        // The claim the store-addressed sync rests on: what a side that cannot
        // load a config derives from the journal is what a side that can
        // derives from the config.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let loaded = load_str(&chained(4));
        assert!(matches!(
            crate::fixtures::drive_search(&store, &loaded.search, None),
            sima_scheduler::SearchOutcome::Finalized { .. }
        ));
        let keys = task_keys(&loaded, &store)?;
        assert_eq!(keys.len(), 12, "three candidates over four segments");
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(journaled_keys(&store, &loaded.search.id())?, expected);
        Ok(())
    }

    #[test]
    fn a_run_stopped_partway_journals_the_keys_it_reached() -> Result<()> {
        // The shape a far search that was wound down leaves: the journal names
        // what it worked on, which is exactly what it has state for.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let loaded = load_str(&chained(4));
        crate::fixtures::drive_search(&store, &loaded.search, Some(2));
        let journaled = journaled_keys(&store, &loaded.search.id())?;
        assert!(!journaled.is_empty(), "it reached some");
        for key in &journaled {
            assert!(
                store.has_record(key)? || store.record(key)?.is_none(),
                "every journaled key is one this store knows about"
            );
        }
        // And every committed record's key is named, which is what the pull
        // has to advertise.
        for key in task_keys(&loaded, &store)? {
            if store.has_record(&key)? {
                assert!(journaled.contains(&key), "{key} committed but unnamed");
            }
        }
        Ok(())
    }

    #[test]
    fn a_run_that_journaled_nothing_names_no_key() -> Result<()> {
        // What a store about to take its first push holds.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let loaded = load_str(&config(None));
        assert!(journaled_keys(&store, &loaded.search.id())?.is_empty());
        Ok(())
    }

    #[test]
    fn a_torn_line_is_skipped_and_the_rest_of_the_journal_stands() -> Result<()> {
        // A crash can tear the journal's final write, and the journal is
        // observational: one damaged line must not cost a whole transfer.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let loaded = load_str(&chained(2));
        crate::fixtures::drive_search(&store, &loaded.search, None);
        let intact = journaled_keys(&store, &loaded.search.id())?;
        assert!(!intact.is_empty());

        store
            .journal_writer(&loaded.search.id())?
            .append("{\"ts_ms\":1,\"event\":\"no_such_event\"}")?;
        assert_eq!(journaled_keys(&store, &loaded.search.id())?, intact);
        Ok(())
    }

    #[test]
    fn a_task_field_that_is_not_a_key_is_skipped() -> Result<()> {
        // Same rule, one level in: a line that parses but names something
        // other than a task key states nothing about what this store holds.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let loaded = load_str(&config(None));
        store.create_search(&loaded.search)?;
        store.journal_writer(&loaded.search.id())?.append(
            &sima_scheduler::Record::stamped(sima_scheduler::Event::Queued {
                task: "not a key".to_string(),
            })
            .to_line()?,
        )?;
        assert!(journaled_keys(&store, &loaded.search.id())?.is_empty());
        Ok(())
    }

    #[test]
    fn a_run_level_event_names_no_key() -> Result<()> {
        // The events that frame the search rather than a task carry no key to
        // advertise, so nothing derives one from them.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let loaded = load_str(&config(None));
        store.create_search(&loaded.search)?;
        let mut writer = store.journal_writer(&loaded.search.id())?;
        for event in [
            sima_scheduler::Event::SearchStarted {
                search: loaded.search.id().to_string(),
                tasks: 3,
                committed: 0,
            },
            sima_scheduler::Event::SearchInterrupted {
                search: loaded.search.id().to_string(),
            },
        ] {
            writer.append(&sima_scheduler::Record::stamped(event).to_line()?)?;
        }
        assert!(journaled_keys(&store, &loaded.search.id())?.is_empty());
        Ok(())
    }
}
