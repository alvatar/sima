//! Shared fixtures for the scheduler integration tests: a stub run built from
//! programmed behaviors, run into a temporary store.
//!
//! This module compiles into every test binary; each uses only some helpers,
//! so the unused-in-one-binary warnings are expected and silenced here.
#![allow(dead_code)]

use std::time::Duration;

use sima_contracts::{Executor, StubBehavior, StubExecutor, StubGenerator, StubGeneratorConfig};
use sima_core::Result;
use sima_model::{
    Environment, EnvironmentComponent, EnvironmentValue, FormatId, GeneratorConfig, GeneratorId,
    Params, RunConfig, RunId, TaskKey,
};
use sima_scheduler::{ExecutionConfig, LifecycleEvent, RunOutcome, StaticBatch, TaskSource, run};
use sima_store::Store;

/// A one-component stub environment, standing in for real execution identity.
pub fn environment() -> Environment {
    Environment::new(vec![
        EnvironmentComponent::new("executor", EnvironmentValue::Version("stub.v1".to_string()))
            .expect("environment component"),
    ])
    .expect("environment")
}

/// A run config whose stub generator programs `behaviors`, under `root_seed`.
pub fn config(root_seed: u64, behaviors: Vec<StubBehavior>) -> RunConfig {
    RunConfig {
        root_seed,
        format: FormatId::new("stub.v1").expect("format id"),
        generator: GeneratorConfig {
            id: GeneratorId::new("stub.v1").expect("generator id"),
            params: StubGeneratorConfig { behaviors }.to_bytes(),
        },
        params: Params {
            bytes: vec![1, 2, 3],
        },
    }
}

/// A validated execution config.
pub fn exec(workers: usize, max_attempts: u32, timeout_ms: u64) -> ExecutionConfig {
    exec_with_timeout(workers, max_attempts, Duration::from_millis(timeout_ms))
}

/// A validated execution config with an explicit attempt timeout, for tests
/// that need a duration outside the millisecond range such as `Duration::MAX`.
pub fn exec_with_timeout(
    workers: usize,
    max_attempts: u32,
    timeout: Duration,
) -> ExecutionConfig {
    ExecutionConfig::new(workers, max_attempts, timeout).expect("execution config")
}

/// A fresh store under a temporary directory; the directory is returned so it
/// outlives the store.
pub fn temp_store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path()).expect("open store");
    (dir, store)
}

/// Runs `cfg` into `store` with the stub generator and executor.
pub fn run_into(store: &Store, cfg: &RunConfig, exec: &ExecutionConfig) -> Result<RunOutcome> {
    run_with(store, cfg, exec, &StubExecutor::new()?)
}

/// Runs `cfg` into `store` with the stub generator and a caller-supplied
/// executor, so a test can inject faulting behavior the stub does not model.
pub fn run_with(
    store: &Store,
    cfg: &RunConfig,
    exec: &ExecutionConfig,
    executor: &(dyn Executor + Sync),
) -> Result<RunOutcome> {
    let generator = StubGenerator::new()?;
    run(store, cfg, &environment(), &generator, executor, exec)
}

/// The run id of `cfg` — the address of its config object, and the directory
/// its journal and manifest live under.
pub fn run_id(cfg: &RunConfig) -> RunId {
    cfg.id()
}

/// The task keys `cfg` comprises, in generator order, derived on a throwaway
/// store so a test can name a task without running it.
pub fn task_keys(cfg: &RunConfig) -> Vec<TaskKey> {
    let (_dir, store) = temp_store();
    let generator = StubGenerator::new().expect("stub generator");
    StaticBatch::new(&generator, cfg, &environment(), &store)
        .expect("materialize frontier")
        .all_keys()
        .to_vec()
}

/// The run's journal, parsed into typed events.
pub fn journal_events(store: &Store, run: &RunId) -> Vec<LifecycleEvent> {
    store
        .journal(run)
        .expect("read journal")
        .iter()
        .map(|line| LifecycleEvent::from_line(line).expect("parse journal line"))
        .collect()
}

/// Counts the events whose task field, extracted by `task_of`, names `task`.
pub fn count_events(
    events: &[LifecycleEvent],
    task: &TaskKey,
    task_of: impl Fn(&LifecycleEvent) -> Option<&str>,
) -> usize {
    let task = task.to_string();
    events
        .iter()
        .filter(|e| task_of(e) == Some(task.as_str()))
        .count()
}

/// How many `Leased` events name `task`.
pub fn leased_count(events: &[LifecycleEvent], task: &TaskKey) -> usize {
    count_events(events, task, |e| match e {
        LifecycleEvent::Leased { task, .. } => Some(task.as_str()),
        _ => None,
    })
}

/// How many `Failed` events name `task`.
pub fn failed_count(events: &[LifecycleEvent], task: &TaskKey) -> usize {
    count_events(events, task, |e| match e {
        LifecycleEvent::Failed { task, .. } => Some(task.as_str()),
        _ => None,
    })
}

/// How many `Retried` events name `task`.
pub fn retried_count(events: &[LifecycleEvent], task: &TaskKey) -> usize {
    count_events(events, task, |e| match e {
        LifecycleEvent::Retried { task, .. } => Some(task.as_str()),
        _ => None,
    })
}

/// How many `Rejected` events name `task`.
pub fn rejected_count(events: &[LifecycleEvent], task: &TaskKey) -> usize {
    count_events(events, task, |e| match e {
        LifecycleEvent::Rejected { task, .. } => Some(task.as_str()),
        _ => None,
    })
}

/// How many `Committed` events name `task`.
pub fn committed_count(events: &[LifecycleEvent], task: &TaskKey) -> usize {
    count_events(events, task, |e| match e {
        LifecycleEvent::Committed { task, .. } => Some(task.as_str()),
        _ => None,
    })
}

/// How many `Faulted` events name `task`.
pub fn faulted_count(events: &[LifecycleEvent], task: &TaskKey) -> usize {
    count_events(events, task, |e| match e {
        LifecycleEvent::Faulted { task, .. } => Some(task.as_str()),
        _ => None,
    })
}

/// How many `TaskOverran` events name `task`.
pub fn overran_count(events: &[LifecycleEvent], task: &TaskKey) -> usize {
    count_events(events, task, |e| match e {
        LifecycleEvent::TaskOverran { task, .. } => Some(task.as_str()),
        _ => None,
    })
}
