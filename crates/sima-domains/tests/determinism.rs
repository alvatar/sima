//! End-to-end determinism of the stub domain: generating a search's specs and
//! executing them yields a byte-identical committed result twice over, and
//! that result does not depend on the execution context (attempt, worker).
//!
//! This proves the acceptance clause ("search-twice → identical hashes") at
//! the domain boundary, without a store: it compares committed records only —
//! the equality criterion the phases use — and never the observational stats.

use sima_contracts::{
    ExecutionContext, Executor, Generator, NoCheckpoint, Outcome, TaskInput, WorkerId,
};
use sima_core::{Codec, Hash, Result, hash_bytes, prng};
use sima_domains::{StubBehavior, StubExecutor, StubGenerator, StubGeneratorConfig};
use sima_model::{ArtifactRef, EnvironmentId, Params, TaskIdentity, TaskRecord};

/// Runs generate → execute → record for a fixed config and folds the committed
/// records into one digest, executing every task at `(attempt, worker)`.
///
/// The per-task `seed` here derives from the root seed as a stand-in for the
/// scheduler's real derivation; the point is only that it is fixed
/// across searches, so any search-to-search difference would come from the domain.
fn run_digest(attempt: u32, worker: WorkerId) -> Result<Hash> {
    let generator = StubGenerator::new()?;
    let executor = StubExecutor::new()?;
    let root_seed = 0x0ABC_D123_4567_89AB_u64;
    let params = Params {
        bytes: vec![7, 7, 7],
    };
    let environment = EnvironmentId::from_hash(hash_bytes(b"determinism-env"));
    // Flaky(0) completes on attempt 0, so every candidate reaches
    // Completed at any attempt.
    let config = StubGeneratorConfig {
        behaviors: vec![
            StubBehavior::Succeed,
            StubBehavior::Succeed,
            StubBehavior::Flaky(0),
            StubBehavior::Succeed,
        ],
    };

    let specs = generator.generate(root_seed, &config.to_bytes())?;

    let mut records = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        let seed = prng::derive(root_seed, i as u64);
        let input = TaskInput {
            spec,
            params: &params,
            seed,
            environment,
            input_state: None,
        };
        let outcome =
            executor.execute(&input, &ExecutionContext { attempt, worker }, &NoCheckpoint)?;
        let Outcome::Completed { artifacts, .. } = outcome else {
            panic!("expected every candidate to complete");
        };
        let identity = TaskIdentity {
            spec: spec.id(),
            params: params.id(),
            seed,
            environment,
            input_state: None,
        };
        let refs = artifacts
            .into_iter()
            .map(|artifact| {
                let object = hash_bytes(&artifact.bytes);
                ArtifactRef::new(artifact.name, object)
            })
            .collect::<Result<Vec<_>>>()?;
        records.push(TaskRecord::new(identity, refs)?);
    }

    // A search's committed result is order-independent: sort by task key, then
    // fold the record bytes into one digest.
    records.sort_by_key(|record| record.identity.key());
    let mut bytes = Vec::new();
    for record in &records {
        bytes.extend_from_slice(&record.to_bytes());
    }
    Ok(hash_bytes(&bytes))
}

#[test]
fn run_is_deterministic() -> Result<()> {
    assert_eq!(run_digest(0, WorkerId(0))?, run_digest(0, WorkerId(0))?);
    Ok(())
}

#[test]
fn run_digest_is_independent_of_execution_context() -> Result<()> {
    // Same committed digest whether every task ran on its first attempt and
    // worker 0, or a later attempt on a different worker.
    assert_eq!(run_digest(0, WorkerId(0))?, run_digest(7, WorkerId(3))?);
    Ok(())
}
