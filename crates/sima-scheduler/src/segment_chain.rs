//! [`SegmentChain`]: the task source that walks each candidate's chain of
//! segments through committed state.
//!
//! One chain per generated spec. Chain i's seed is the same substream a
//! static batch would give candidate i, constant across the chain's
//! segments; segments are distinguished by `input_state` alone. Segment 0
//! carries no input state; segment j+1's input state is the object hash of
//! segment j's committed `state` artifact, so the chain walks committed
//! state hop by hop and the frontier is derived from `(config, store
//! state)` — resume is the same construction.

use std::collections::HashSet;

use sima_contracts::{Generator, STATE_ARTIFACT};
use sima_core::{Error, Result, prng};
use sima_model::{
    Environment, EnvironmentId, ParamsId, RunConfig, Spec, SpecId, TaskIdentity, TaskKey,
};
use sima_store::Store;

use crate::task_source::{RunnableTask, TaskSource, generate_specs};

/// The run's task keys as materialized so far: insertion-ordered and
/// deduplicated, so convergent chains never hand `finalize_run` a duplicate.
struct KeySet {
    ordered: Vec<TaskKey>,
    seen: HashSet<TaskKey>,
}

impl KeySet {
    fn new() -> KeySet {
        KeySet {
            ordered: Vec::new(),
            seen: HashSet::new(),
        }
    }

    fn push(&mut self, key: TaskKey) {
        if self.seen.insert(key) {
            self.ordered.push(key);
        }
    }
}

/// One candidate's chain: the candidate's evaluation divided into a fixed
/// number of segments, each a task taking the previous segment's committed
/// state as input (segment 0 takes none). At most one segment is
/// runnable at a time, the frontier, so the struct is a cursor: the
/// identity parts constant across the chain plus the walk position.
struct Chain {
    /// The candidate every segment of this chain evaluates.
    spec: Spec,
    /// The spec's id, folded into every segment's identity.
    spec_id: SpecId,
    /// The chain's seed, constant across its segments.
    seed: u64,
    /// The next uncommitted segment's identity; `None` once the chain has
    /// walked all its segments.
    frontier: Option<TaskIdentity>,
    /// Segments walked past (committed) so far.
    committed: u64,
    /// Whether the frontier was handed out and awaits its commit.
    handed_out: bool,
}

impl Chain {
    /// The fast-forward step: while the store answers the frontier key and
    /// segments remain, read the committed record's `state` artifact and
    /// derive the successor identity, collecting each new key. Runs once at
    /// construction (resume) and again whenever a handed-out segment may
    /// have committed.
    fn advance(
        &mut self,
        store: &Store,
        params: ParamsId,
        environment: EnvironmentId,
        segments: u64,
        keys: &mut KeySet,
    ) -> Result<()> {
        // Identities are content-addressed, so a state fixed point (or
        // cycle) makes a successor reuse an earlier key. The loop needs no
        // special case for that: the store answers the repeated key
        // immediately, `committed` still counts every hop up to `segments`,
        // and `keys` deduplicates.
        while let Some(identity) = &self.frontier {
            let key = identity.key();
            let Some(record) = store.record(&key)? else {
                break;
            };
            // A committed segment without the state artifact means the run's
            // domain carries no continuation state: a segmented run over a
            // stateless domain is a misconfiguration, reported as a
            // validation error.
            let state = record
                .artifacts()
                .iter()
                .find(|artifact| artifact.name() == STATE_ARTIFACT)
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "segmented task {key} committed no {STATE_ARTIFACT:?} artifact: \
                         the run's domain carries no continuation state"
                    ))
                })?;
            self.committed += 1;
            self.handed_out = false;
            if self.committed == segments {
                self.frontier = None;
            } else {
                let successor = TaskIdentity {
                    spec: self.spec_id,
                    params,
                    seed: self.seed,
                    environment,
                    input_state: Some(*state.object()),
                };
                keys.push(successor.key());
                self.frontier = Some(successor);
            }
        }
        Ok(())
    }
}

/// The task source over all of a run's chains, selected by the driver when
/// the run config carries a segment count. Where a `Chain` is one
/// candidate's cursor through its segments, `SegmentChain` holds every
/// chain and derives the run's frontier from their positions. It borrows
/// the store because advancing a chain means reading its committed records:
/// the frontier depends on store state for the run's whole life, where
/// [`StaticBatch`](crate::StaticBatch) reads the store at construction
/// only.
pub struct SegmentChain<'a> {
    store: &'a Store,
    params: ParamsId,
    environment: EnvironmentId,
    /// Tasks per chain, from `RunConfig.segments`.
    segments: u64,
    chains: Vec<Chain>,
    keys: KeySet,
    /// The segments the store already answered at construction, across every
    /// chain: what the fast-forward walked past.
    prior_committed: usize,
}

impl<'a> SegmentChain<'a> {
    /// Materializes the chains: generates and stores the run's specs, then
    /// fast-forwards each chain against the store, walking past every
    /// already-committed segment so a resume starts at each chain's true
    /// frontier.
    pub fn new(
        generator: &dyn Generator,
        config: &RunConfig,
        environment: &Environment,
        store: &'a Store,
    ) -> Result<SegmentChain<'a>> {
        let segments = config
            .segments
            .ok_or_else(|| {
                Error::Validation("a segment chain requires config.segments".to_string())
            })?
            .get();
        let specs = generate_specs(generator, config, store)?;
        let params = config.params.id();
        let environment_id = environment.id();
        let mut keys = KeySet::new();
        let mut chains = Vec::with_capacity(specs.len());
        for (i, (spec, spec_id)) in specs.into_iter().enumerate() {
            let identity = TaskIdentity {
                spec: spec_id,
                params,
                // The same substream a static batch would give candidate i,
                // constant across the chain's segments.
                seed: prng::derive(config.root_seed, i as u64),
                environment: environment_id,
                input_state: None,
            };
            keys.push(identity.key());
            let mut chain = Chain {
                spec,
                spec_id,
                seed: identity.seed,
                frontier: Some(identity),
                committed: 0,
                handed_out: false,
            };
            chain.advance(store, params, environment_id, segments, &mut keys)?;
            chains.push(chain);
        }
        Ok(SegmentChain {
            store,
            params,
            environment: environment_id,
            segments,
            // Counted here, before any poll advances a chain past a segment
            // this session ran.
            prior_committed: chains.iter().map(|c| c.committed as usize).sum(),
            chains,
            keys,
        })
    }
}

impl TaskSource for SegmentChain<'_> {
    fn poll(&mut self) -> Result<Vec<RunnableTask>> {
        let mut out = Vec::new();
        for (i, chain) in self.chains.iter_mut().enumerate() {
            // A handed-out frontier may have committed since the last poll:
            // re-run the fast-forward step, which also skips any successors
            // the store already answers (a fixed point or cross-run reuse).
            if chain.handed_out {
                chain.advance(
                    self.store,
                    self.params,
                    self.environment,
                    self.segments,
                    &mut self.keys,
                )?;
            }
            // At most one runnable task per chain: its frontier, exactly once.
            if !chain.handed_out
                && let Some(identity) = &chain.frontier
            {
                out.push(RunnableTask {
                    spec: chain.spec.clone(),
                    identity: *identity,
                    chain: Some(i as u64),
                });
                chain.handed_out = true;
            }
        }
        Ok(out)
    }

    fn all_keys(&self) -> &[TaskKey] {
        &self.keys.ordered
    }

    fn task_total(&self) -> usize {
        self.chains.len() * self.segments as usize
    }

    fn prior_committed(&self) -> usize {
        self.prior_committed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::Codec;
    use sima_domains::{StubBehavior, StubGenerator, StubGeneratorConfig};
    use sima_model::{ArtifactRef, GeneratorConfig, Params, TaskRecord};

    /// A segmented run config over the given behaviors.
    fn config(behaviors: Vec<StubBehavior>, segments: u64) -> Result<RunConfig> {
        Ok(RunConfig {
            root_seed: 7,
            segments: std::num::NonZeroU64::new(segments),
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

    /// Commits a record answering `task`, carrying `state` as the committed
    /// `state` artifact; returns the state object's hash.
    fn commit_with_state(
        store: &Store,
        task: &RunnableTask,
        state: &[u8],
    ) -> Result<sima_core::Hash> {
        let state_hash = store.put(state)?;
        let record = TaskRecord::new(
            task.identity,
            vec![ArtifactRef::new(STATE_ARTIFACT, state_hash)?],
        )?;
        store.commit(&record)?;
        Ok(state_hash)
    }

    /// Commits a record answering `task` with no artifacts — the stateless
    /// misconfiguration case.
    fn commit_stateless(store: &Store, task: &RunnableTask) -> Result<()> {
        let record = TaskRecord::new(task.identity, Vec::new())?;
        store.commit(&record)?;
        Ok(())
    }

    /// Stores the referenced objects a committed record needs and builds a
    /// chain source over one `accumulate:1` candidate.
    fn one_chain(store: &Store, segments: u64) -> Result<(RunConfig, SegmentChain<'_>)> {
        let generator = StubGenerator::new()?;
        let config = config(vec![StubBehavior::Accumulate(1)], segments)?;
        let env = environment()?;
        store.put(&config.params.to_bytes())?;
        store.put(&env.to_bytes())?;
        let source = SegmentChain::new(&generator, &config, &env, store)?;
        Ok((config, source))
    }

    #[test]
    fn a_fresh_store_yields_segment_zero_per_chain() -> Result<()> {
        let (_dir, store) = temp_store();
        let generator = StubGenerator::new()?;
        let config = config(
            vec![StubBehavior::Accumulate(1), StubBehavior::Accumulate(1)],
            3,
        )?;
        let env = environment()?;
        let mut source = SegmentChain::new(&generator, &config, &env, &store)?;
        assert_eq!(source.task_total(), 6, "2 chains x 3 segments");
        let tasks = source.poll()?;
        assert_eq!(tasks.len(), 2, "one segment-0 task per chain");
        for (i, task) in tasks.iter().enumerate() {
            assert_eq!(task.identity.input_state, None);
            assert_eq!(task.identity.seed, prng::derive(config.root_seed, i as u64));
            assert_eq!(task.chain, Some(i as u64));
        }
        // The two chains differ by spec (nonce) and seed, so keys differ.
        assert_ne!(tasks[0].identity.key(), tasks[1].identity.key());
        Ok(())
    }

    #[test]
    fn a_commit_makes_the_next_poll_yield_the_successor() -> Result<()> {
        let (_dir, store) = temp_store();
        let (_config, mut source) = one_chain(&store, 3)?;
        let first = source.poll()?.pop().expect("segment 0");
        let state_hash = commit_with_state(&store, &first, b"state after segment 0")?;
        let successor = source.poll()?.pop().expect("segment 1");
        assert_eq!(successor.identity.input_state, Some(state_hash));
        assert_eq!(successor.identity.seed, first.identity.seed);
        assert_eq!(successor.identity.spec, first.identity.spec);
        assert_eq!(successor.chain, first.chain);
        Ok(())
    }

    #[test]
    fn a_handed_out_task_is_not_re_yielded_while_uncommitted() -> Result<()> {
        let (_dir, store) = temp_store();
        let (_config, mut source) = one_chain(&store, 3)?;
        assert_eq!(source.poll()?.len(), 1);
        // Nothing committed: repeated polls yield nothing new.
        assert!(source.poll()?.is_empty());
        assert!(source.poll()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_record_without_the_state_artifact_faults_naming_it() -> Result<()> {
        let (_dir, store) = temp_store();
        let (_config, mut source) = one_chain(&store, 3)?;
        let first = source.poll()?.pop().expect("segment 0");
        commit_stateless(&store, &first)?;
        match source.poll() {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains(STATE_ARTIFACT), "names the artifact: {msg}");
                assert!(
                    msg.contains(&first.identity.key().to_string()),
                    "names the task key: {msg}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn construction_fast_forwards_past_committed_segments() -> Result<()> {
        let (_dir, store) = temp_store();
        // Commit segments 0 and 1 through a first source.
        let (_config, mut source) = one_chain(&store, 3)?;
        let first = source.poll()?.pop().expect("segment 0");
        commit_with_state(&store, &first, b"state 0")?;
        let second = source.poll()?.pop().expect("segment 1");
        let state_hash = commit_with_state(&store, &second, b"state 1")?;
        // A fresh source over the same store resumes at segment 2.
        let (_config, mut resumed) = one_chain(&store, 3)?;
        let third = resumed.poll()?.pop().expect("segment 2");
        assert_eq!(third.identity.input_state, Some(state_hash));
        assert_eq!(
            resumed.all_keys(),
            &[
                first.identity.key(),
                second.identity.key(),
                third.identity.key()
            ]
        );
        // The fast-forward is what a resumed session's display counts on
        // from: two segments' records, committed here without any journal
        // line, are two prior commits.
        assert_eq!(resumed.prior_committed(), 2);
        Ok(())
    }

    #[test]
    fn a_fully_committed_chain_yields_nothing_and_lists_every_key() -> Result<()> {
        let (_dir, store) = temp_store();
        let (_config, mut source) = one_chain(&store, 2)?;
        let mut keys = Vec::new();
        for state in [b"state 0".as_slice(), b"state 1"] {
            let task = source.poll()?.pop().expect("a segment");
            keys.push(task.identity.key());
            commit_with_state(&store, &task, state)?;
        }
        assert!(source.poll()?.is_empty());
        assert_eq!(source.all_keys(), keys.as_slice());
        // Resume over the fully committed store: nothing runnable, same keys.
        let (_config, mut resumed) = one_chain(&store, 2)?;
        assert!(resumed.poll()?.is_empty());
        assert_eq!(resumed.all_keys(), keys.as_slice());
        Ok(())
    }

    #[test]
    fn a_state_fixed_point_deduplicates_keys_and_terminates() -> Result<()> {
        let (_dir, store) = temp_store();
        let (_config, mut source) = one_chain(&store, 4)?;
        // Segment 0 commits state S; segment 1 receives hash(S) and commits
        // S again, so segment 2's identity equals segment 1's — a fixed
        // point. The walk terminates at the segment count with the key set
        // deduplicated to two entries.
        let first = source.poll()?.pop().expect("segment 0");
        commit_with_state(&store, &first, b"fixed point")?;
        let second = source.poll()?.pop().expect("segment 1");
        commit_with_state(&store, &second, b"fixed point")?;
        assert!(source.poll()?.is_empty(), "the chain is exhausted");
        assert_eq!(
            source.all_keys(),
            &[first.identity.key(), second.identity.key()]
        );
        // task_total reports the planned count, not the deduplicated one.
        assert_eq!(source.task_total(), 4);
        Ok(())
    }

    #[test]
    fn two_fresh_stores_derive_identical_frontiers() -> Result<()> {
        let (_a, store_a) = temp_store();
        let (_b, store_b) = temp_store();
        let (_config, mut source_a) = one_chain(&store_a, 3)?;
        let (_config, mut source_b) = one_chain(&store_b, 3)?;
        assert_eq!(source_a.all_keys(), source_b.all_keys());
        let task_a = source_a.poll()?.pop().expect("segment 0");
        let task_b = source_b.poll()?.pop().expect("segment 0");
        assert_eq!(task_a.identity.key(), task_b.identity.key());
        Ok(())
    }
}
