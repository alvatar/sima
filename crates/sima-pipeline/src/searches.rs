//! The searches a store holds: what is in it, and addressing one of them by id.
//!
//! A config names one search — the search its identity section hashes to — so every
//! other verb reaches a store through a config and sees exactly that search. A
//! store accumulates the searches of every identity ever driven against it: an
//! edited seed, changed params, a different generator. These two answer the
//! questions a config cannot: what is in here, and delete that one.

use std::path::Path;

use sima_core::{Error, Result};
use sima_model::SearchId;
use sima_store::Store;

use crate::journal::parse;
use crate::status::{SearchState, status_records};

/// One search as a store holds it: its identity, the state its journal projects,
/// and its task ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSummary {
    pub search: SearchId,
    /// What the search's journal projects, by the same fold `sima status` uses.
    pub state: SearchState,
    /// The search's task count, as its latest session stated it.
    pub tasks: usize,
    /// How many of those tasks the journal shows committed.
    pub committed: usize,
}

/// Every search in the store at `root`, by id.
///
/// A store root that is not there at all is [`Error::Validation`] before
/// anything touches the disk, since opening a store creates its skeleton and a
/// query for a store nobody drove in must not conjure one. A directory that is
/// there and holds no store is opened like any other, and the skeleton that
/// writes is the same one every verb reaching that path writes.
///
/// A search registered but never driven has no journal and no records,
/// so it summarizes as in progress with an empty ledger — which is what it is.
pub fn searches(root: &Path) -> Result<Vec<SearchSummary>> {
    if !root.is_dir() {
        return Err(Error::Validation(format!(
            "store {} does not exist: no search was ever driven there",
            root.display()
        )));
    }
    let store = Store::open(root)?;
    store
        .searches()?
        .into_iter()
        .map(|search| {
            let records = parse(&search, &store.journal(&search)?)?;
            let status = status_records(search, &records);
            Ok(SearchSummary {
                search,
                state: status.state,
                tasks: status.tasks,
                committed: status.committed,
            })
        })
        .collect()
}

/// The one search in `store` whose id begins with `prefix`.
///
/// Any unambiguous prefix addresses a search, as one does a task. An ambiguous
/// one is refused naming every search it matches, since the answer to it is to
/// type more of one of them. The empty prefix is refused before that: it
/// begins every search, so in a store holding one it would address that search
/// while naming nothing.
pub(crate) fn resolve_search(store: &Store, prefix: &str) -> Result<SearchId> {
    if prefix.is_empty() {
        return Err(Error::Validation(
            "--search takes a search id or a leading part of one, and was given nothing. Every search \
             begins with the empty prefix, so it addresses no search in particular; `sima searches \
             <store-dir>` lists what the store holds."
                .to_string(),
        ));
    }
    let matched: Vec<SearchId> = store
        .searches()?
        .into_iter()
        .filter(|search| search.to_string().starts_with(prefix))
        .collect();
    match matched.as_slice() {
        [search] => Ok(*search),
        [] => Err(Error::Validation(format!(
            "no search in this store matches prefix {prefix}"
        ))),
        many => Err(Error::Validation(format!(
            "prefix {prefix} is ambiguous: it matches {}",
            many.iter()
                .map(SearchId::to_string)
                .collect::<Vec<String>>()
                .join(", ")
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{drive_search, load_str};

    /// A search of two succeeding candidates under `root_seed`, which is what
    /// makes two configs over one store two searches.
    fn seeded(root_seed: u64, store: &Path) -> crate::config::LoadedConfig {
        load_str(&format!(
            r#"
            [search]
            root_seed = {root_seed}
            format = "stub.v1"

            [search.generator]
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

    /// Two identities over `root` whose search ids begin with the same character,
    /// so the store they are driven into holds a prefix that names both.
    ///
    /// Search ids are hashes, so which seeds collide is fixed by the configs
    /// rather than chosen: the search walks them in order and takes the first
    /// pair that does, which makes the same two every time.
    fn sharing_a_leading_character(root: &Path) -> (crate::config::LoadedConfig, u64) {
        let mut seen: Vec<(char, u64)> = Vec::new();
        for seed in 1..u64::MAX {
            let leading = seeded(seed, root)
                .search
                .id()
                .to_string()
                .chars()
                .next()
                .expect("a search id has characters");
            if let Some((_, earlier)) = seen.iter().find(|(char, _)| *char == leading) {
                return (seeded(*earlier, root), seed);
            }
            seen.push((leading, seed));
        }
        unreachable!("sixteen leading characters cannot hold seventeen search ids apart");
    }

    /// A store holding two searches of different identities, and their ids.
    fn two_searches(dir: &Path) -> Result<(Store, SearchId, SearchId)> {
        let root = dir.join("store");
        let store = Store::open(&root)?;
        let (first, second) = sharing_a_leading_character(&root);
        let second = seeded(second, &root);
        drive_search(&store, &first.search, None);
        drive_search(&store, &second.search, None);
        Ok((store, first.search.id(), second.search.id()))
    }

    #[test]
    fn a_store_lists_every_search_it_holds_with_its_state_and_ledger() -> Result<()> {
        // Two identities driven against one store: the listing is what says
        // both are in there, since a config names only one of them.
        let dir = tempfile::tempdir().expect("temp dir");
        let (store, first, second) = two_searches(dir.path())?;

        let listed = searches(store.root())?;
        assert_eq!(listed.len(), 2, "{listed:?}");
        for summary in &listed {
            assert_eq!(summary.state, SearchState::Finalized);
            assert!(summary.tasks > 0, "{summary:?}");
            assert_eq!(
                summary.committed, summary.tasks,
                "a finalized search committed all of them: {summary:?}"
            );
        }
        let ids: Vec<SearchId> = listed.iter().map(|summary| summary.search).collect();
        assert!(ids.contains(&first) && ids.contains(&second), "{ids:?}");
        Ok(())
    }

    #[test]
    fn a_store_that_was_never_driven_in_is_refused_rather_than_created() {
        let dir = tempfile::tempdir().expect("temp dir");
        let absent = dir.path().join("nothing-here");
        let error = searches(&absent).expect_err("a store that is not there holds no search");
        assert!(error.to_string().contains("does not exist"), "{error}");
        assert!(!absent.exists(), "and the query created nothing");
    }

    #[test]
    fn an_unambiguous_prefix_addresses_a_search_and_an_ambiguous_one_names_them() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let (store, first, second) = two_searches(dir.path())?;

        let id = first.to_string();
        assert_eq!(resolve_search(&store, &id[..12])?, first);
        assert_eq!(resolve_search(&store, &id)?, first);

        // A prefix both searches begin with is refused naming them, which is what
        // makes typing more of one an answer.
        let shared = &first.to_string()[..1];
        let error = resolve_search(&store, shared).expect_err("both searches begin that way");
        let text = error.to_string();
        assert!(text.contains("ambiguous"), "{text}");
        assert!(text.contains(&first.to_string()), "{text}");
        assert!(text.contains(&second.to_string()), "{text}");

        let error = resolve_search(&store, "ffffffffffff").expect_err("no search matches");
        assert!(error.to_string().contains("no search"), "{error}");
        Ok(())
    }

    #[test]
    fn an_empty_prefix_is_refused_rather_than_read_as_any_search() -> Result<()> {
        // It is a prefix of every search, so a store holding one would have that
        // one deleted by an argument that named nothing. The flag is what the
        // refusal names, since the fix is to type a search into it.
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("store");
        let store = Store::open(&root)?;
        let only = seeded(1, &root);
        drive_search(&store, &only.search, None);

        let error = resolve_search(&store, "").expect_err("an empty prefix names no search");
        let text = error.to_string();
        assert!(text.contains("--search"), "names the flag: {text}");
        assert_eq!(
            store.searches()?,
            vec![only.search.id()],
            "and nothing was touched"
        );
        Ok(())
    }
}
