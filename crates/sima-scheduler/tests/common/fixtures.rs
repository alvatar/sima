//! The fixture implementations behind the `common` namespace.
//!
//! This module compiles into every test binary; each uses only some helpers,
//! so the unused-in-one-binary warnings are expected and silenced here.
#![allow(dead_code)]

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use sima_contracts::{DeviceClass, Executor};
use sima_core::Result;
use sima_domains::{StubBehavior, StubExecutor, StubGenerator, StubGeneratorConfig};
use sima_model::{
    Environment, EnvironmentComponent, EnvironmentValue, FormatId, GeneratorConfig, GeneratorId,
    Params, RunConfig, RunId, TaskKey,
};
use sima_scheduler::{
    DeviceEntry, ExecutionConfig, LifecycleEvent, RunControl, RunOutcome, StaticBatch, TaskSource,
    WorkerPool, run, worker_slots,
};
use sima_store::Store;
use sima_transport::loopback::{LoopbackTransport, SharedResolver};

/// A one-component stub environment, standing in for real execution identity.
pub fn environment() -> Environment {
    Environment::new(vec![
        EnvironmentComponent::new("executor", EnvironmentValue::Version("stub.v1".to_string()))
            .expect("environment component"),
    ])
    .expect("environment")
}

/// A run config whose stub generator programs `behaviors`, under `root_seed`,
/// dividing each candidate's evaluation into `segments` chained tasks.
pub fn chained_config(root_seed: u64, behaviors: Vec<StubBehavior>, segments: u64) -> RunConfig {
    RunConfig {
        segments: NonZeroU64::new(segments),
        ..config(root_seed, behaviors)
    }
}

/// A run config whose stub generator programs `behaviors`, under `root_seed`.
pub fn config(root_seed: u64, behaviors: Vec<StubBehavior>) -> RunConfig {
    RunConfig {
        root_seed,
        segments: None,
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

/// A device class named by its vendor id; the device id is fixed, so a test
/// names a class with one number.
pub fn class(vendor_id: u32) -> DeviceClass {
    DeviceClass {
        vendor_id,
        device_id: 0x0001,
    }
}

/// A resolved device entry: `workers` workers on one single-card class.
pub fn device(vendor_id: u32, workers: usize) -> DeviceEntry {
    DeviceEntry {
        class: class(vendor_id),
        name: class(vendor_id).to_string(),
        workers,
        members: 1,
    }
}

/// A validated execution config whose pool is spread over `devices`.
pub fn exec_over(devices: Vec<DeviceEntry>, max_attempts: u32) -> ExecutionConfig {
    ExecutionConfig::with_devices(devices, max_attempts, Duration::MAX, Duration::MAX, None)
        .expect("execution config")
}

/// A validated execution config with an explicit attempt timeout, for tests
/// that need a duration outside the millisecond range such as `Duration::MAX`.
pub fn exec_with_timeout(workers: usize, max_attempts: u32, timeout: Duration) -> ExecutionConfig {
    ExecutionConfig::new(workers, max_attempts, timeout, Duration::MAX, None)
        .expect("execution config")
}

/// A fresh store under a temporary directory; the directory is returned so it
/// outlives the store.
pub fn temp_store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path()).expect("open store");
    (dir, store)
}

/// The stub-executor resolver for the loopback transport. The stub uses no
/// device, so it ignores the binding and names none.
pub fn stub_resolver() -> SharedResolver {
    Arc::new(|_, _| {
        let executor: Box<dyn Executor> = Box::new(StubExecutor::new()?);
        Ok((executor, String::new(), String::new()))
    })
}

/// A stub-executor resolver that names the device it was handed, so a test can
/// read back which class ran a task through the journal's `WorkerBound`
/// events. The executor itself is the plain stub: the binding says where to
/// compute, and the stub computes the same wherever it is.
pub fn device_naming_resolver() -> SharedResolver {
    Arc::new(|_, device| {
        let executor: Box<dyn Executor> = Box::new(StubExecutor::new()?);
        let name = match device {
            Some(device) => format!("{} #{}", device.class(), device.member),
            None => String::new(),
        };
        Ok((executor, name, String::new()))
    })
}

/// The class part of a name [`device_naming_resolver`] produced.
pub fn named_class(device: &str) -> &str {
    device
        .split(" #")
        .next()
        .expect("split yields a first part")
}

/// The device each worker last reported, from the run's `WorkerBound` events.
pub fn worker_devices(events: &[LifecycleEvent]) -> HashMap<u64, String> {
    let mut devices = HashMap::new();
    for event in events {
        if let LifecycleEvent::WorkerBound { worker, device, .. } = event {
            devices.insert(*worker, device.clone());
        }
    }
    devices
}

/// The classes that leased each task, in lease order: the join of `Leased`
/// with the leasing worker's device.
pub fn task_classes(events: &[LifecycleEvent]) -> HashMap<String, Vec<String>> {
    let devices = worker_devices(events);
    let mut classes: HashMap<String, Vec<String>> = HashMap::new();
    for event in events {
        if let LifecycleEvent::Leased { task, worker, .. } = event {
            let device = devices
                .get(worker)
                .expect("a worker reports before it leases");
            classes
                .entry(task.clone())
                .or_default()
                .push(named_class(device).to_string());
        }
    }
    classes
}

/// A loopback transport hosting `resolver`'s executor for `cfg`'s format
/// under `exec`'s checkpoint cadence: the real wire protocol and host loop
/// over in-memory pipes, so these tests run the full scheduler without
/// processes.
fn loopback(
    cfg: &RunConfig,
    exec: &ExecutionConfig,
    resolver: SharedResolver,
) -> LoopbackTransport {
    LoopbackTransport::new(
        cfg.format.clone(),
        exec.checkpoint_interval,
        exec.checkpoint_interval_steps,
        resolver,
    )
}

/// Runs `cfg` into `store` with the stub generator and executor.
pub fn run_into(store: &Store, cfg: &RunConfig, exec: &ExecutionConfig) -> Result<RunOutcome> {
    run_with(store, cfg, exec, stub_resolver())
}

/// Runs `cfg` into `store` with the stub generator and a caller-supplied
/// executor resolver, so a test can inject faulting behavior the stub does
/// not model.
pub fn run_with(
    store: &Store,
    cfg: &RunConfig,
    exec: &ExecutionConfig,
    resolver: SharedResolver,
) -> Result<RunOutcome> {
    let generator = StubGenerator::new()?;
    let transport = loopback(cfg, exec, resolver);
    let pools = [WorkerPool {
        transport: &transport,
        host: String::new(),
        slots: worker_slots(exec),
    }];
    run(
        store,
        cfg,
        &environment(),
        &generator,
        &pools,
        exec,
        &RunControl::detached(),
    )
}

/// Runs `cfg` into `store` under a caller-supplied [`RunControl`], with the
/// stub generator and executor, so a test can observe events or interrupt
/// the run.
pub fn run_controlled(
    store: &Store,
    cfg: &RunConfig,
    exec: &ExecutionConfig,
    control: &RunControl,
) -> Result<RunOutcome> {
    let generator = StubGenerator::new()?;
    let transport = loopback(cfg, exec, stub_resolver());
    let pools = [WorkerPool {
        transport: &transport,
        host: String::new(),
        slots: worker_slots(exec),
    }];
    run(
        store,
        cfg,
        &environment(),
        &generator,
        &pools,
        exec,
        control,
    )
}

/// Runs `cfg` into `store` over caller-built pools, with the stub generator, so
/// a test can spread one run across several transports on distinct hosts.
pub fn run_pools(
    store: &Store,
    cfg: &RunConfig,
    exec: &ExecutionConfig,
    pools: &[WorkerPool<'_>],
) -> Result<RunOutcome> {
    let generator = StubGenerator::new()?;
    run(
        store,
        cfg,
        &environment(),
        &generator,
        pools,
        exec,
        &RunControl::detached(),
    )
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

/// How many `LeaseExpired` events name `task`.
pub fn lease_expired_count(events: &[LifecycleEvent], task: &TaskKey) -> usize {
    count_events(events, task, |e| match e {
        LifecycleEvent::LeaseExpired { task, .. } => Some(task.as_str()),
        _ => None,
    })
}
