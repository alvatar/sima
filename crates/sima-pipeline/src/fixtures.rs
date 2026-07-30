//! Test fixtures shared by the crate's unit tests: the stub run every
//! synthetic journal is written under, the store it lives in, and the two
//! things a test needs a real store for — a run actually driven, and a sync
//! actually performed between two of them.

use std::collections::BTreeMap;
use std::io::pipe;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use sima_contracts::Executor;
use sima_core::Result;
use sima_domains::{StubExecutor, StubGenerator};
use sima_model::{Environment, FormatId, GeneratorConfig, GeneratorId, Params, RunConfig, TaskKey};
use sima_provider::Budget;
use sima_scheduler::{
    Event, ExecutionConfig, Record, RunControl, RunOutcome, WorkerPool, run, worker_slots,
};
use sima_store::{ObjectScope, Store, SyncReport, SyncRole};
use sima_transport::loopback::LoopbackTransport;

use crate::config::{Fleet, LoadedConfig, Orchestrator, Pool};
use crate::domain_registry::DomainRegistry;

/// A minimal stub run config; its id addresses the test's run.
pub(crate) fn stub_config() -> Result<RunConfig> {
    Ok(RunConfig {
        root_seed: 1,
        segments: None,
        format: FormatId::new("stub.v1")?,
        generator: GeneratorConfig {
            id: GeneratorId::new("stub.v1")?,
            params: Vec::new(),
        },
        params: Params { bytes: Vec::new() },
    })
}

/// A loaded config over `store` for the stub run: one orchestrator worker and
/// no other machine.
pub(crate) fn loaded(store: PathBuf) -> Result<LoadedConfig> {
    Ok(LoadedConfig {
        run: stub_config()?,
        execution: ExecutionConfig::new(1, 1, Duration::MAX, Duration::MAX, None)?,
        orchestrator: Orchestrator {
            migrate: None,
            container: None,
            pool: Some(Pool::Workers(1)),
        },
        hosts: BTreeMap::new(),
        host_classes: BTreeMap::new(),
        fleet: Fleet::default(),
        budget: Budget::default(),
        store,
        domains: DomainRegistry::builtin(),
    })
}

/// Builds the `sima-worker` binary once per test process and returns its path.
///
/// The tests that route a format to a program need a program that answers, and
/// sima's own worker is one: it serves the in-tree formats over exactly the
/// protocol a binary outside the workspace does. Cargo builds another crate's
/// binary only when it is in the build graph, so the build is asked for here.
pub(crate) fn built_worker() -> PathBuf {
    static BUILD: std::sync::Once = std::sync::Once::new();
    BUILD.call_once(|| {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = std::process::Command::new(cargo)
            .args(["build", "-p", "sima-worker"])
            .status()
            .expect("run cargo build for sima-worker");
        assert!(status.success(), "building sima-worker failed");
    });
    // Beside the test executable's directory: `target/<profile>/deps` holds the
    // test binary and `target/<profile>` the built worker.
    let exe = std::env::current_exe().expect("the test executable's path");
    let dir = exe
        .parent()
        .and_then(std::path::Path::parent)
        .expect("target/<profile> above the test executable");
    let worker = dir.join("sima-worker");
    assert!(worker.is_file(), "{} is built", worker.display());
    worker
}

/// Loads `text` as a config file in a fresh temporary directory, for the unit
/// tests that exercise the loaded shape rather than the file's location. The
/// directory is removed at once: nothing here opens the store the config names.
pub(crate) fn load_str(text: &str) -> LoadedConfig {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("sima.toml");
    std::fs::write(&path, text).expect("write the config file");
    crate::config::load(&path).expect("the config loads")
}

/// The config text a served run is written from: a stub run over a store
/// beside the config file, as a far-side host would hold it.
const SERVED_CONFIG: &str = r#"
    [run]
    root_seed = 7
    format = "stub.v1"

    [run.generator]
    id = "stub.v1"
    behaviors = ["succeed", "succeed"]

    [config]
    store = "./store"
    max_attempts = 3

    [orchestrator]
    workers = 2
"#;

/// Writes a config file under `dir` and returns its path, without touching
/// the store it names — the state of a host where no run was ever driven.
pub(crate) fn served_config(dir: &std::path::Path) -> PathBuf {
    let path = dir.join("sima.toml");
    std::fs::write(&path, SERVED_CONFIG).expect("write the config file");
    path
}

/// Writes a config file under `dir`, creates its run in the store the config
/// names, and journals `records`: the far-side state a follow stream reads.
/// Returns the config path and the config it loads to.
pub(crate) fn served_run(
    dir: &std::path::Path,
    records: &[Record],
) -> Result<(PathBuf, LoadedConfig)> {
    let path = served_config(dir);
    let loaded = crate::config::load(&path)?;
    let store = Store::open(&loaded.store)?;
    store.create_run(&loaded.run)?;
    let mut writer = store.journal_writer(&loaded.run.id())?;
    for record in records {
        writer.append(&record.to_line()?)?;
    }
    Ok((path, loaded))
}

/// The environment the stub domain's tasks depend on, taken from the domain
/// itself: a fixture that minted its own would produce task keys no derivation
/// through [`crate::task_keys`] could answer for.
pub(crate) fn stub_environment() -> Environment {
    sima_domains::binding_for(&FormatId::new("stub.v1").expect("format id"))
        .expect("the stub domain")
        .environment
}

/// Drives `config` into `store` over in-memory workers, so a test has a store
/// of real records without a worker binary or a device.
///
/// `stop_after` interrupts the run once that many tasks have committed, which
/// is how a test leaves a chain partway; `None` runs it to its end.
pub(crate) fn drive_run(
    store: &Store,
    config: &RunConfig,
    stop_after: Option<usize>,
) -> RunOutcome {
    let exec =
        ExecutionConfig::new(1, 1, Duration::MAX, Duration::MAX, None).expect("execution config");
    let transport = LoopbackTransport::new(
        config.format.clone(),
        exec.checkpoint_interval,
        exec.checkpoint_interval_steps,
        Arc::new(|_, _| {
            let executor: Box<dyn Executor> = Box::new(StubExecutor::new()?);
            Ok((executor, String::new(), String::new()))
        }),
    );
    let pools = [WorkerPool {
        transport: &transport,
        host: String::new(),
        slots: worker_slots(&exec),
    }];
    let interrupt = AtomicBool::new(false);
    let committed = AtomicUsize::new(0);
    let control = RunControl {
        observer: &|record: &Record| {
            if let Some(stop_after) = stop_after
                && matches!(record.event, Event::Committed { .. })
                && committed.fetch_add(1, Ordering::Relaxed) + 1 >= stop_after
            {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
        on_start: None,
    };
    run(
        store,
        config,
        &stub_environment(),
        &StubGenerator::new().expect("stub generator"),
        &pools,
        &exec,
        &control,
    )
    .expect("the run drives")
}

/// Runs one sync session between two stores over a duplex pipe, `near` as the
/// initiator under `scope` and `far` as the responder over everything its own
/// records reference — the shape `sima sync-serve` gives the far half.
///
/// Each side brings its own key set, as each derives it independently from
/// (config, store state); no key list crosses the wire. Returns the initiator's
/// report.
pub(crate) fn sync_between(
    near: &Store,
    near_keys: &[TaskKey],
    scope: ObjectScope<'_>,
    far: &Store,
    far_keys: &[TaskKey],
) -> Result<SyncReport> {
    let (near_read, far_write) = pipe().expect("pipe");
    let (far_read, near_write) = pipe().expect("pipe");
    thread::scope(|threads| {
        let responder = threads.spawn(|| {
            let (mut r, mut w) = (far_read, far_write);
            far.sync(
                far_keys,
                ObjectScope::Referenced,
                &mut r,
                &mut w,
                SyncRole::Responder,
            )
        });
        let (mut r, mut w) = (near_read, near_write);
        let report = near.sync(near_keys, scope, &mut r, &mut w, SyncRole::Initiator);
        responder
            .join()
            .expect("the responder thread")
            .expect("the far half succeeds");
        report
    })
}

/// Writes `records` to the stub run's journal in a fresh store, returning the
/// temp dir (kept alive by the caller) and the loaded config over it.
pub(crate) fn journal_with(records: &[Record]) -> Result<(tempfile::TempDir, LoadedConfig)> {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path())?;
    let config = stub_config()?;
    store.create_run(&config)?;
    let mut writer = store.journal_writer(&config.id())?;
    for record in records {
        writer.append(&record.to_line()?)?;
    }
    let config = loaded(dir.path().to_path_buf())?;
    Ok((dir, config))
}
