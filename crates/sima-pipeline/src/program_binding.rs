//! Binding a session to the build that serves it: the resume gate over a
//! config-routed program.
//!
//! A program's identity is what it declares through its environment
//! components, so a rebuilt binary that keeps its declarations keeps its
//! `EnvironmentId` — and with it every task key, every stored result, and
//! every checkpoint the previous build produced. The hash records none of
//! this.
//!
//! This module records it: each session journals the digest of the file that
//! served it, and a session resuming a search compares that record against the
//! file it is about to search. A difference is the user's decision to make, so
//! the search stops until the invocation states an answer.

use sima_core::{Error, Result};
use sima_model::{SearchConfig, SearchId};
use sima_scheduler::{Event, Record};
use sima_store::Store;

use crate::domain_registry::RoutedProgram;

/// What an invocation does when the program serving the search's format is a
/// different build from the one that drove the search before.
///
/// A search whose results and checkpoints came from another build is a search whose
/// stored work may no longer mean what it meant, and sima cannot tell whether
/// the change was material. The refusal is therefore the default and the
/// acceptance is explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryChange {
    /// A changed program stops the search.
    Refuse,
    /// A changed program drives the search, and becomes what the next session
    /// compares against.
    Accept,
}

/// Compares the program now serving `search`'s format against the build the search
/// was last driven by, and records the current one.
///
/// Called under the search lock, so the read and the append race no other
/// orchestrator. A session that proceeds appends its record, so the next
/// session compares against the build that actually ran; a refusal returns
/// ahead of the append, leaving the digest history naming the builds that drove
/// sessions.
pub(crate) fn bind(
    store: &Store,
    search: &SearchConfig,
    routed: &RoutedProgram<'_>,
    accept: BinaryChange,
) -> Result<()> {
    let id = search.id();
    let digest = routed.digest.to_string();
    let previous = last_digest(store, &id)?;
    if let Some(previous) = previous
        && previous != digest
        && accept == BinaryChange::Refuse
    {
        return Err(Error::Validation(format!(
            "the program serving format {:?} changed since this search last ran: {} is now \
             {digest}, the search was driven by {previous}; stored results and checkpoints were \
             produced by the previous build. Pass --accept-binary to continue with the changed \
             program.",
            search.format.as_str(),
            routed.binary.display()
        )));
    }
    // The search directory must exist for the journal writer, and this append
    // precedes the collector, so the registration the scheduler would perform
    // later happens here. It is idempotent, so a resume re-registers nothing.
    store.create_search(search)?;
    let mut writer = store.journal_writer(&id)?;
    writer.append(
        &Record::stamped(Event::ProgramBound {
            format: search.format.as_str().to_string(),
            binary: routed.binary.display().to_string(),
            digest,
        })
        .to_line()?,
    )
}

/// The digest the search's last recorded program binding carries, or `None` for a
/// search that recorded none — a fresh search, or one whose format no config routed
/// when it ran.
///
/// A line that does not parse is skipped: the journal is observational, and a
/// crash can tear its final write, so a torn or foreign line states nothing
/// about which build drove the search.
fn last_digest(store: &Store, search: &SearchId) -> Result<Option<String>> {
    Ok(store
        .journal(search)?
        .iter()
        .filter_map(|line| Record::from_line(line).ok())
        .filter_map(|record| match record.event {
            Event::ProgramBound { digest, .. } => Some(digest),
            _ => None,
        })
        .next_back())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use sima_core::hash_bytes;

    use super::*;
    use crate::fixtures::stub_config;

    /// A routed program over a path and the digest of `bytes`.
    fn routed<'a>(binary: &'a Path, digest: &'a sima_core::Hash) -> RoutedProgram<'a> {
        RoutedProgram {
            binary,
            digest,
            payload: None,
            env: &[],
            sdk: None,
            payload_digest: None,
        }
    }

    /// A store in a fresh temporary directory.
    fn temp_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open store");
        (dir, store)
    }

    /// The program-binding digests the search's journal carries, in append order.
    fn bound_digests(store: &Store, search: &SearchId) -> Result<Vec<String>> {
        Ok(store
            .journal(search)?
            .iter()
            .filter_map(|line| Record::from_line(line).ok())
            .filter_map(|record| match record.event {
                Event::ProgramBound { digest, .. } => Some(digest),
                _ => None,
            })
            .collect())
    }

    #[test]
    fn a_first_binding_records_the_build_and_compares_against_nothing() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = stub_config()?;
        let digest = hash_bytes(b"the first build");
        bind(
            &store,
            &search,
            &routed(Path::new("/opt/acme/worker"), &digest),
            BinaryChange::Refuse,
        )?;
        assert_eq!(bound_digests(&store, &search.id())?, [digest.to_string()]);
        Ok(())
    }

    #[test]
    fn an_unchanged_build_binds_again_under_a_refusal() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = stub_config()?;
        let digest = hash_bytes(b"the same build twice");
        for _ in 0..2 {
            bind(
                &store,
                &search,
                &routed(Path::new("/opt/acme/worker"), &digest),
                BinaryChange::Refuse,
            )?;
        }
        // Every session records its binding, so the journal holds the search's
        // whole build history rather than only its first.
        assert_eq!(
            bound_digests(&store, &search.id())?,
            [digest.to_string(), digest.to_string()]
        );
        Ok(())
    }

    #[test]
    fn a_changed_build_under_a_refusal_names_both_digests_and_the_flag() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = stub_config()?;
        let first = hash_bytes(b"the build that ran");
        let second = hash_bytes(b"the build that would search now");
        bind(
            &store,
            &search,
            &routed(Path::new("/opt/acme/worker"), &first),
            BinaryChange::Refuse,
        )?;
        let Err(error) = bind(
            &store,
            &search,
            &routed(Path::new("/opt/acme/worker"), &second),
            BinaryChange::Refuse,
        ) else {
            panic!("expected a changed build to be refused");
        };
        let text = error.to_string();
        for named in [
            "stub.v1",
            "/opt/acme/worker",
            &first.to_string(),
            &second.to_string(),
            "--accept-binary",
        ] {
            assert!(text.contains(named), "{named} is missing from {text}");
        }
        // The refused session recorded nothing, so the search still names the
        // build that drove it.
        assert_eq!(bound_digests(&store, &search.id())?, [first.to_string()]);
        Ok(())
    }

    #[test]
    fn an_accepted_change_binds_the_new_build_for_the_next_session() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = stub_config()?;
        let first = hash_bytes(b"the build that ran");
        let second = hash_bytes(b"the build the user accepted");
        let binary = Path::new("/opt/acme/worker");
        bind(
            &store,
            &search,
            &routed(binary, &first),
            BinaryChange::Refuse,
        )?;
        bind(
            &store,
            &search,
            &routed(binary, &second),
            BinaryChange::Accept,
        )?;
        // The accepted build is what the next session compares against, so
        // running it again needs no flag.
        bind(
            &store,
            &search,
            &routed(binary, &second),
            BinaryChange::Refuse,
        )?;
        assert_eq!(
            bound_digests(&store, &search.id())?,
            [first.to_string(), second.to_string(), second.to_string()]
        );
        Ok(())
    }

    #[test]
    fn a_torn_line_before_the_binding_leaves_the_comparison_intact() -> Result<()> {
        // The journal is observational and a crash can tear its final write, so
        // a line that does not parse states nothing about which build ran.
        let (_dir, store) = temp_store();
        let search = stub_config()?;
        let digest = hash_bytes(b"the build that ran");
        bind(
            &store,
            &search,
            &routed(Path::new("/opt/acme/worker"), &digest),
            BinaryChange::Refuse,
        )?;
        store
            .journal_writer(&search.id())?
            .append("{\"ts_ms\":1,\"event\":\"no_such_event\"}")?;
        assert_eq!(last_digest(&store, &search.id())?, Some(digest.to_string()));
        Ok(())
    }
}
