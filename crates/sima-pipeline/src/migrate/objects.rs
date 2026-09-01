//! The objects a push sends: identity components, plus each chain's frontier
//! state.
//!
//! A push must send every **record** in the key set — a chain is traversable
//! forward only, since segment k's key derives from segment k−1's produced
//! state, so without the prefix records the far side cannot locate the frontier
//! and would start from segment 0. The records are small: an identity and a few
//! hashes.
//!
//! The **state bytes behind them** are what is worth not sending. The far side
//! reads the last state of each chain and nothing else, so every earlier one is
//! bandwidth and rental time spent on bytes nobody opens, and the waste grows
//! with how far the local search got before the interrupt.
//!
//! ```text
//!    chain interrupted after segment 2 of 6
//!
//!    record 0  ──state H₀──┐
//!    record 1  ──state H₁──┼── all three sets of bytes would travel
//!    record 2  ──state H₂──┘
//!                     │
//!                     └─ only H₂ is read: the frontier segment's input
//! ```
//!
//! The named set is computed here rather than in the store because it needs
//! [`STATE_ARTIFACT`] from `sima-contracts`, which `sima-store` does not depend
//! on — and because it is a property of how a search continues, which is this
//! layer's subject.

use std::collections::BTreeSet;

use sima_contracts::STATE_ARTIFACT;
use sima_core::{Hash, Result};
use sima_model::TaskKey;
use sima_store::Store;

/// The objects a push advertises: every record's identity components, plus each
/// `state` artifact that no record in the key set names as its input state.
///
/// That last set is exactly the frontier states, derived from the records alone
/// with no knowledge of chain structure: a state some successor consumes is not
/// a frontier, and one nothing consumes is. A search with no segments names the
/// identity components alone, since a completed stateless task's output is
/// never an input to anything.
///
/// The result is ordered and deduplicated, so one store state always names one
/// set.
pub(crate) fn push_objects(store: &Store, keys: &[TaskKey]) -> Result<Vec<Hash>> {
    let mut named = BTreeSet::new();
    // The states some record in the set continues from. A state here is a
    // predecessor's output that a successor still needs to be handed, so it is
    // behind the frontier and the far side never reads it.
    let mut consumed = BTreeSet::new();
    let mut states = BTreeSet::new();
    for key in keys {
        let Some(record) = store.record(key)? else {
            continue;
        };
        let identity = &record.identity;
        named.extend([
            *identity.spec.as_hash(),
            *identity.params.as_hash(),
            *identity.environment.as_hash(),
        ]);
        if let Some(input) = identity.input_state {
            consumed.insert(input);
        }
        for artifact in record.artifacts() {
            if artifact.name() == STATE_ARTIFACT {
                states.insert(*artifact.object());
            }
        }
    }
    named.extend(states.difference(&consumed).copied());
    Ok(named.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use sima_core::Codec;
    use sima_core::hash_bytes;
    use sima_model::{
        ArtifactRef, Environment, EnvironmentComponent, EnvironmentValue, FormatId, Params, Spec,
        TaskIdentity, TaskRecord,
    };

    use super::*;

    /// The spec every fixture task evaluates.
    fn spec() -> Spec {
        Spec {
            format: FormatId::new("stub.v1").expect("format id"),
            bytes: vec![0xAA],
        }
    }

    /// The params every fixture task searches under.
    fn params() -> Params {
        Params { bytes: vec![1] }
    }

    /// The environment every fixture task depends on.
    fn environment() -> Environment {
        Environment::new(vec![
            EnvironmentComponent::new("engine", EnvironmentValue::Version("stub".to_string()))
                .expect("component"),
        ])
        .expect("environment")
    }

    /// The three objects every record's identity references.
    fn identity_components() -> Vec<Hash> {
        let mut components = vec![
            *spec().id().as_hash(),
            *params().id().as_hash(),
            *environment().id().as_hash(),
        ];
        components.sort();
        components
    }

    /// A store holding the identity components and nothing else.
    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open store");
        for bytes in [
            spec().to_bytes(),
            params().to_bytes(),
            environment().to_bytes(),
        ] {
            store.put(&bytes).expect("put identity component");
        }
        (dir, store)
    }

    /// Commits one segment of the chain seeded `seed`, continuing from
    /// `input_state` and producing a state derived from `output`; returns its
    /// key and produced state digest. The seed is the chain's, constant across
    /// its segments and distinct between chains, as the scheduler derives it.
    fn segment(
        store: &Store,
        seed: u64,
        input_state: Option<Hash>,
        output: &[u8],
    ) -> (TaskKey, Hash) {
        let produced = store.put(output).expect("put the state object");
        let identity = TaskIdentity {
            spec: spec().id(),
            params: params().id(),
            seed,
            environment: environment().id(),
            input_state,
        };
        let artifact = ArtifactRef::new(STATE_ARTIFACT, produced).expect("artifact ref");
        let record = TaskRecord::new(identity, vec![artifact]).expect("task record");
        store.commit(&record).expect("commit the segment");
        (record.identity.key(), produced)
    }

    #[test]
    fn a_committed_chain_names_its_identity_components_and_one_state() -> Result<()> {
        // Three committed segments: two of the three produced states are some
        // successor's input, so only the last is a frontier.
        let (_dir, store) = store();
        let (first, state0) = segment(&store, 7, None, b"state 0");
        let (second, state1) = segment(&store, 7, Some(state0), b"state 1");
        let (third, state2) = segment(&store, 7, Some(state1), b"state 2");

        let named = push_objects(&store, &[first, second, third])?;
        let mut expected = identity_components();
        expected.push(state2);
        expected.sort();
        assert_eq!(named, expected, "the identity components plus one state");
        assert!(!named.contains(&state0));
        assert!(!named.contains(&state1));
        Ok(())
    }

    #[test]
    fn a_run_with_no_segments_names_the_identity_components_alone() -> Result<()> {
        // A completed stateless task's output is never an input to anything, so
        // there is no continuation state to hand anyone.
        let (_dir, store) = store();
        let object = store.put(b"an ordinary artifact")?;
        let identity = TaskIdentity {
            spec: spec().id(),
            params: params().id(),
            seed: 1,
            environment: environment().id(),
            input_state: None,
        };
        let artifact = ArtifactRef::new("snapshot", object).expect("artifact ref");
        let record = TaskRecord::new(identity, vec![artifact]).expect("task record");
        store.commit(&record)?;

        assert_eq!(
            push_objects(&store, &[record.identity.key()])?,
            identity_components()
        );
        Ok(())
    }

    #[test]
    fn a_key_the_store_does_not_answer_contributes_nothing() -> Result<()> {
        // The frontier key itself is in the set and uncommitted; it names no
        // objects because there is no record to name them.
        let (_dir, store) = store();
        let (committed, state) = segment(&store, 7, None, b"state 0");
        let uncommitted = TaskKey::from_hash(hash_bytes(b"never committed"));

        let mut expected = identity_components();
        expected.push(state);
        expected.sort();
        assert_eq!(push_objects(&store, &[committed, uncommitted])?, expected);
        Ok(())
    }

    #[test]
    fn several_chains_each_name_their_own_frontier() -> Result<()> {
        // Two independent chains: each contributes the one state nothing
        // consumes, and the identity components are named once between them.
        let (_dir, store) = store();
        let (a0, a_state0) = segment(&store, 1, None, b"a0");
        let (a1, a_state1) = segment(&store, 1, Some(a_state0), b"a1");
        let (b0, b_state0) = segment(&store, 2, None, b"b0");

        let named = push_objects(&store, &[a0, a1, b0])?;
        let mut expected = identity_components();
        expected.extend([a_state1, b_state0]);
        expected.sort();
        expected.dedup();
        assert_eq!(named, expected);
        Ok(())
    }

    #[test]
    fn an_empty_key_set_names_nothing() -> Result<()> {
        let (_dir, store) = store();
        assert!(push_objects(&store, &[])?.is_empty());
        Ok(())
    }

    /// A generator-derived chain, search partway, then pushed under the named
    /// scope into a fresh store — the shape a migration produces.
    mod over_a_real_chain {
        use sima_domains::{StubBehavior, StubGenerator, StubGeneratorConfig};
        use sima_model::{GeneratorConfig, GeneratorId, SearchConfig};
        use sima_scheduler::{SearchOutcome, search_keys};
        use sima_store::ObjectScope;

        use super::*;
        use crate::fixtures::{drive_search, stub_environment, sync_between};

        /// A search of one candidate over twenty accumulating segments.
        fn chained_run() -> SearchConfig {
            SearchConfig {
                root_seed: 5,
                segments: std::num::NonZeroU64::new(20),
                format: FormatId::new("stub.v1").expect("format id"),
                generator: GeneratorConfig {
                    id: GeneratorId::new("stub.v1").expect("generator id"),
                    params: StubGeneratorConfig {
                        behaviors: vec![StubBehavior::Accumulate(2)],
                    }
                    .to_bytes(),
                },
                params: Params { bytes: vec![1] },
            }
        }

        #[test]
        fn a_store_that_took_a_named_push_derives_the_same_frontier() -> Result<()> {
            // The claim the whole partial transfer rests on. The far side holds
            // every record and only the frontier's state bytes, and derives
            // exactly the frontier the complete store derives — a chain is
            // located from its records, and the state bytes are what the
            // frontier segment *searches on*, not what finds it.
            let here = tempfile::tempdir().expect("temp dir");
            let there = tempfile::tempdir().expect("temp dir");
            let local = Store::open(here.path())?;
            let far = Store::open(there.path())?;
            let config = chained_run();
            // Stopped shortly after it starts committing, so the chain is
            // partway and has a frontier to hand over.
            assert!(matches!(
                drive_search(&local, &config, Some(3)),
                SearchOutcome::Interrupted { .. }
            ));

            let generator = StubGenerator::new()?;
            let keys = search_keys(&local, &config, &stub_environment(), &generator)?;
            let named = push_objects(&local, &keys)?;
            assert!(keys.len() > 2, "the chain got past its first segment");

            // The far side holds nothing yet, so it derives no key of its own.
            sync_between(&local, &keys, ObjectScope::Named(&named), &far, &[])?;

            // Every record travelled.
            for key in &keys[..keys.len() - 1] {
                assert!(far.record(key)?.is_some(), "record {key} must travel");
            }
            // The frontier the far side derives is the one this side derives.
            assert_eq!(
                search_keys(&far, &config, &stub_environment(), &generator)?,
                keys,
                "the gapped store derives the frontier the complete one does"
            );
            // And it is genuinely gapped: an earlier segment's state never came.
            let earlier = *local.record(&keys[0])?.expect("committed").artifacts()[0].object();
            assert!(
                !far.has(&earlier)?,
                "an earlier segment's state is bytes nobody opens"
            );
            Ok(())
        }
    }
}
