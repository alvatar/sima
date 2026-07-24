//! Worker retirement: a fleet transport that yields no worker winds the run
//! down by faulting under strict fill, degrading to the survivors under
//! best-effort, and faulting rather than hanging when the last worker leaves.

mod common;

use std::time::Duration;

use common::{config, exec, run_id, run_pools, stub_resolver, temp_store};
use sima_contracts::DeviceBinding;
use sima_core::{Error, Result};
use sima_domains::StubBehavior;
use sima_scheduler::{RunOutcome, WorkerPool};
use sima_trace::Emitter;
use sima_transport::loopback::LoopbackTransport;
use sima_transport::{SpawnOutcome, WorkerTransport};

/// A transport that spawns no worker: every spawn reports retirement. Models a
/// fleet instance that is gone, so the scheduler's retirement handling can be
/// driven without a provider.
struct RetiringTransport {
    fatal: bool,
}

impl WorkerTransport for RetiringTransport {
    fn spawn(
        &self,
        _worker: u64,
        _device: Option<&DeviceBinding>,
        _events: Emitter,
    ) -> Result<SpawnOutcome> {
        Ok(SpawnOutcome::Retired { fatal: self.fatal })
    }
}

/// A transport whose spawn fails outright — an infrastructure error, not a
/// retirement. The regression guard that a spawn `Err` still faults the run.
struct FailingSpawnTransport;

impl WorkerTransport for FailingSpawnTransport {
    fn spawn(
        &self,
        _worker: u64,
        _device: Option<&DeviceBinding>,
        _events: Emitter,
    ) -> Result<SpawnOutcome> {
        Err(Error::Transport(
            "the worker could not be spawned".to_string(),
        ))
    }
}

/// One deviceless worker slot.
fn one_slot() -> Vec<Option<DeviceBinding>> {
    vec![None]
}

#[test]
fn a_fatal_retirement_faults_the_run() -> Result<()> {
    let cfg = config(70, vec![StubBehavior::Succeed]);
    let (_dir, store) = temp_store();
    let transport = RetiringTransport { fatal: true };
    let pools = [WorkerPool {
        transport: &transport,
        host: String::new(),
        slots: one_slot(),
    }];
    match run_pools(&store, &cfg, &exec(1, 3, 1_000), &pools) {
        Err(Error::Transport(msg)) => {
            assert!(msg.contains("strict fill"), "names the cause: {msg}");
        }
        other => panic!("expected a strict-fill fault, got {other:?}"),
    }
    // A faulted run writes no manifest.
    assert!(store.manifest(&run_id(&cfg))?.is_none());
    Ok(())
}

#[test]
fn a_non_fatal_retirement_lets_a_survivor_finish_the_run() -> Result<()> {
    // Two distinct candidates, and two pools: a retiring best-effort pool and a
    // healthy loopback pool. The survivor drains the queue and the run
    // finalizes.
    let cfg = config(71, vec![StubBehavior::Succeed, StubBehavior::Sleep(0)]);
    let (_dir, store) = temp_store();
    let exec = exec(1, 3, 1_000);
    let retiring = RetiringTransport { fatal: false };
    let survivor = LoopbackTransport::new(cfg.format.clone(), Duration::MAX, None, stub_resolver());
    let pools = [
        WorkerPool {
            transport: &retiring,
            host: String::new(),
            slots: one_slot(),
        },
        WorkerPool {
            transport: &survivor,
            host: String::new(),
            slots: one_slot(),
        },
    ];
    assert!(matches!(
        run_pools(&store, &cfg, &exec, &pools)?,
        RunOutcome::Finalized { .. }
    ));
    // Every candidate committed through the survivor.
    let manifest = store
        .manifest(&run_id(&cfg))?
        .expect("a finalized manifest");
    assert_eq!(manifest.entries.len(), 2);
    Ok(())
}

#[test]
fn the_last_worker_retiring_faults_rather_than_hangs() -> Result<()> {
    // A best-effort retirement that leaves no worker behind: with work still
    // pending and no one to drain it, the run must fault instead of the driver
    // waiting forever.
    let cfg = config(72, vec![StubBehavior::Succeed]);
    let (_dir, store) = temp_store();
    let transport = RetiringTransport { fatal: false };
    let pools = [WorkerPool {
        transport: &transport,
        host: String::new(),
        slots: one_slot(),
    }];
    match run_pools(&store, &cfg, &exec(1, 3, 1_000), &pools) {
        Err(Error::Transport(msg)) => {
            assert!(
                msg.contains("every worker retired"),
                "names the cause: {msg}"
            );
        }
        other => panic!("expected an every-worker-retired fault, got {other:?}"),
    }
    assert!(store.manifest(&run_id(&cfg))?.is_none());
    Ok(())
}

#[test]
fn a_spawn_failure_still_faults_the_run() -> Result<()> {
    // The regression guard: a spawn that returns `Err` — an infrastructure
    // failure, not a retirement — faults the run, unchanged by the
    // SpawnOutcome refactor.
    let cfg = config(73, vec![StubBehavior::Succeed]);
    let (_dir, store) = temp_store();
    let transport = FailingSpawnTransport;
    let pools = [WorkerPool {
        transport: &transport,
        host: String::new(),
        slots: one_slot(),
    }];
    match run_pools(&store, &cfg, &exec(1, 3, 1_000), &pools) {
        Err(Error::Transport(msg)) => {
            assert!(
                msg.contains("could not be spawned"),
                "carries the cause: {msg}"
            );
        }
        other => panic!("expected a transport fault, got {other:?}"),
    }
    assert!(store.manifest(&run_id(&cfg))?.is_none());
    Ok(())
}
