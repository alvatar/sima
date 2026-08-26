//! The runs a store holds: what is in it, and addressing one of them by id.
//!
//! A config names one run — the run its identity section hashes to — so every
//! other verb reaches a store through a config and sees exactly that run. A
//! store accumulates the runs of every identity ever driven against it: an
//! edited seed, changed params, a different generator. These two answer the
//! questions a config cannot: what is in here, and delete that one.

use std::path::Path;

use sima_core::{Error, Result};
use sima_model::RunId;
use sima_store::Store;

use crate::journal::parse;
use crate::status::{RunState, status_records};

/// One run as a store holds it: its identity, the state its journal projects,
/// and its task ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub run: RunId,
    /// What the run's journal projects, by the same fold `sima status` uses.
    pub state: RunState,
    /// The run's task count, as its latest session stated it.
    pub tasks: usize,
    /// How many of those tasks the journal shows committed.
    pub committed: usize,
}

/// Every run in the store at `root`, by id.
///
/// A store root that does not exist is [`Error::Validation`] before anything
/// touches the disk, since opening a store creates its skeleton and a query
/// must not. A run registered but never driven has no journal and no records,
/// so it summarizes as in progress with an empty ledger — which is what it is.
pub fn runs(root: &Path) -> Result<Vec<RunSummary>> {
    if !root.is_dir() {
        return Err(Error::Validation(format!(
            "store {} does not exist: no run was ever driven there",
            root.display()
        )));
    }
    let store = Store::open(root)?;
    store
        .runs()?
        .into_iter()
        .map(|run| {
            let records = parse(&run, &store.journal(&run)?)?;
            let status = status_records(run, &records);
            Ok(RunSummary {
                run,
                state: status.state,
                tasks: status.tasks,
                committed: status.committed,
            })
        })
        .collect()
}

/// The one run in `store` whose id begins with `prefix`.
///
/// Any unambiguous prefix addresses a run, as one does a task. An ambiguous
/// one is refused naming every run it matches, since the answer to it is to
/// type more of one of them. The empty prefix is refused before that: it
/// begins every run, so in a store holding one it would address that run
/// while naming nothing.
pub(crate) fn resolve_run(store: &Store, prefix: &str) -> Result<RunId> {
    if prefix.is_empty() {
        return Err(Error::Validation(
            "--run takes a run id or a leading part of one, and was given nothing. Every run \
             begins with the empty prefix, so it addresses no run in particular; `sima runs \
             <store-dir>` lists what the store holds."
                .to_string(),
        ));
    }
    let matched: Vec<RunId> = store
        .runs()?
        .into_iter()
        .filter(|run| run.to_string().starts_with(prefix))
        .collect();
    match matched.as_slice() {
        [run] => Ok(*run),
        [] => Err(Error::Validation(format!(
            "no run in this store matches prefix {prefix}"
        ))),
        many => Err(Error::Validation(format!(
            "prefix {prefix} is ambiguous: it matches {}",
            many.iter()
                .map(RunId::to_string)
                .collect::<Vec<String>>()
                .join(", ")
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{drive_run, load_str};

    /// A run of two succeeding candidates under `root_seed`, which is what
    /// makes two configs over one store two runs.
    fn seeded(root_seed: u64, store: &Path) -> crate::config::LoadedConfig {
        load_str(&format!(
            r#"
            [run]
            root_seed = {root_seed}
            format = "stub.v1"

            [run.generator]
            id = "stub.v1"
            behaviors = ["succeed", "succeed"]

            [config]
            store = "{}"
            max_attempts = 3

            [orchestrator]
            workers = 1
        "#,
            store.display()
        ))
    }

    /// Two identities over `root` whose run ids begin with the same character,
    /// so the store they are driven into holds a prefix that names both.
    ///
    /// Run ids are hashes, so which seeds collide is fixed by the configs
    /// rather than chosen: the search walks them in order and takes the first
    /// pair that does, which makes the same two every time.
    fn sharing_a_leading_character(root: &Path) -> (crate::config::LoadedConfig, u64) {
        let mut seen: Vec<(char, u64)> = Vec::new();
        for seed in 1..u64::MAX {
            let leading = seeded(seed, root)
                .run
                .id()
                .to_string()
                .chars()
                .next()
                .expect("a run id has characters");
            if let Some((_, earlier)) = seen.iter().find(|(char, _)| *char == leading) {
                return (seeded(*earlier, root), seed);
            }
            seen.push((leading, seed));
        }
        unreachable!("sixteen leading characters cannot hold seventeen run ids apart");
    }

    /// A store holding two runs of different identities, and their ids.
    fn two_runs(dir: &Path) -> Result<(Store, RunId, RunId)> {
        let root = dir.join("store");
        let store = Store::open(&root)?;
        let (first, second) = sharing_a_leading_character(&root);
        let second = seeded(second, &root);
        drive_run(&store, &first.run, None);
        drive_run(&store, &second.run, None);
        Ok((store, first.run.id(), second.run.id()))
    }

    #[test]
    fn a_store_lists_every_run_it_holds_with_its_state_and_ledger() -> Result<()> {
        // Two identities driven against one store: the listing is what says
        // both are in there, since a config names only one of them.
        let dir = tempfile::tempdir().expect("temp dir");
        let (store, first, second) = two_runs(dir.path())?;

        let listed = runs(store.root())?;
        assert_eq!(listed.len(), 2, "{listed:?}");
        for summary in &listed {
            assert_eq!(summary.state, RunState::Finalized);
            assert!(summary.tasks > 0, "{summary:?}");
            assert_eq!(
                summary.committed, summary.tasks,
                "a finalized run committed all of them: {summary:?}"
            );
        }
        let ids: Vec<RunId> = listed.iter().map(|summary| summary.run).collect();
        assert!(ids.contains(&first) && ids.contains(&second), "{ids:?}");
        Ok(())
    }

    #[test]
    fn a_store_that_was_never_driven_in_is_refused_rather_than_created() {
        let dir = tempfile::tempdir().expect("temp dir");
        let absent = dir.path().join("nothing-here");
        let error = runs(&absent).expect_err("a store that is not there holds no run");
        assert!(error.to_string().contains("does not exist"), "{error}");
        assert!(!absent.exists(), "and the query created nothing");
    }

    #[test]
    fn an_unambiguous_prefix_addresses_a_run_and_an_ambiguous_one_names_them() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let (store, first, second) = two_runs(dir.path())?;

        let id = first.to_string();
        assert_eq!(resolve_run(&store, &id[..12])?, first);
        assert_eq!(resolve_run(&store, &id)?, first);

        // A prefix both runs begin with is refused naming them, which is what
        // makes typing more of one an answer.
        let shared = &first.to_string()[..1];
        let error = resolve_run(&store, shared).expect_err("both runs begin that way");
        let text = error.to_string();
        assert!(text.contains("ambiguous"), "{text}");
        assert!(text.contains(&first.to_string()), "{text}");
        assert!(text.contains(&second.to_string()), "{text}");

        let error = resolve_run(&store, "ffffffffffff").expect_err("no run matches");
        assert!(error.to_string().contains("no run"), "{error}");
        Ok(())
    }

    #[test]
    fn an_empty_prefix_is_refused_rather_than_read_as_any_run() -> Result<()> {
        // It is a prefix of every run, so a store holding one would have that
        // one deleted by an argument that named nothing. The flag is what the
        // refusal names, since the fix is to type a run into it.
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("store");
        let store = Store::open(&root)?;
        let only = seeded(1, &root);
        drive_run(&store, &only.run, None);

        let error = resolve_run(&store, "").expect_err("an empty prefix names no run");
        let text = error.to_string();
        assert!(text.contains("--run"), "names the flag: {text}");
        assert_eq!(
            store.runs()?,
            vec![only.run.id()],
            "and nothing was touched"
        );
        Ok(())
    }
}
