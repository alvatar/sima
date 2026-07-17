//! Shared fixtures for the sima CLI test suites: the config-file writer,
//! the spawn helper over the built binary, the worker-binary build, the
//! process-table scan, and manifest lookup through the pipeline surface.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;
use std::time::{Duration, Instant};

use sima_domains::{domain_for, generator_for};
use sima_model::TaskIdentity;
use sima_pipeline::{LifecycleEvent, load};
use sima_store::{Manifest, Store};

/// Writes a `sima.toml` named `name` under `dir`: the given behaviors
/// list content and store path (resolved relative to `dir`), two workers,
/// three attempts.
pub fn write_config(dir: &Path, name: &str, behaviors: &str, store: &str) -> PathBuf {
    let text = format!(
        r#"
        [run]
        root_seed = 11
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = [{behaviors}]

        [execution]
        store = "{store}"
        workers = 2
        max_attempts = 3
    "#
    );
    let path = dir.join(name);
    std::fs::write(&path, text).expect("write config");
    path
}

/// Writes `text` as a config file named `name` under `dir` — for suites
/// whose configs need keys beyond [`write_config`]'s shape.
pub fn write_config_text(dir: &Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).expect("write config");
    path
}

/// A command over the built sima binary, its environment cleared of any
/// crashpoint arming so only an explicit test arms one, and its worker
/// binary pinned through `SIMA_WORKER` so the run path never depends on
/// what happens to sit beside the test executable.
pub fn sima_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sima"));
    command.env_remove("SIMA_CRASHPOINT");
    command.env("SIMA_WORKER", worker_binary());
    command
}

/// Builds the `sima-worker` binary once per test process and returns its
/// path. Cargo builds another crate's binary only when it is in the build
/// graph, so the suites that spawn workers build it explicitly. A
/// crash-injection build of this suite builds the worker with the same
/// feature, so executor-side crashpoints fire inside the worker process.
pub fn worker_binary() -> PathBuf {
    static BUILD: Once = Once::new();
    BUILD.call_once(|| {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let mut command = Command::new(cargo);
        command.args(["build", "-p", "sima-worker"]);
        if cfg!(feature = "crash-injection") {
            command.args(["--features", "crash-injection"]);
        }
        let status = command.status().expect("run cargo build for sima-worker");
        assert!(status.success(), "building sima-worker failed");
    });
    target_dir().join("debug").join("sima-worker")
}

/// The workspace target directory: `CARGO_TARGET_DIR` when set, else
/// `target/` at the workspace root derived from this crate's manifest dir.
fn target_dir() -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"),
    }
}

/// Pids of live `sima-worker` processes whose parent is `parent`, from the
/// process table. `/proc/<pid>/stat` frames the command name in parentheses;
/// the fields after the closing parenthesis start with the state and the
/// parent pid.
pub fn worker_processes(parent: u32) -> Vec<u32> {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let pid: u32 = entry.file_name().to_str()?.parse().ok()?;
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
            let close = stat.rfind(')')?;
            let ppid: u32 = stat[close + 1..].split_whitespace().nth(1)?.parse().ok()?;
            (comm_of(&stat) == Some("sima-worker") && ppid == parent).then_some(pid)
        })
        .collect()
}

/// The manifest of the run `config_path` describes, from its store.
pub fn manifest_of(config_path: &Path) -> Option<Manifest> {
    let config = load(config_path).expect("load config");
    let store = Store::open(&config.store).expect("open store");
    store.manifest(&config.run.id()).expect("read manifest")
}

/// The journal of the run `config_path` describes, parsed into typed events.
pub fn journal_events(config_path: &Path) -> Vec<LifecycleEvent> {
    let config = load(config_path).expect("load config");
    let store = Store::open(&config.store).expect("open store");
    store
        .journal(&config.run.id())
        .expect("read journal")
        .iter()
        .map(|line| LifecycleEvent::from_line(line).expect("parse journal line"))
        .collect()
}

/// Every device the run's workers reported, across the whole journal.
pub fn devices_reported(events: &[LifecycleEvent]) -> HashSet<String> {
    events
        .iter()
        .filter_map(|event| match event {
            LifecycleEvent::WorkerBound { device, .. } if !device.is_empty() => {
                Some(device.clone())
            }
            _ => None,
        })
        .collect()
}

/// The devices each task's attempts ran on, in lease order.
///
/// A worker id names a different device from one session to the next — the
/// pool's shape is a config's business, not a run's — so the walk tracks what
/// each worker's device is *at that point* in the journal and attributes each
/// lease to it. Reading the whole journal first and taking each worker's last
/// device would credit a resumed session's work to the wrong hardware.
pub fn task_devices(events: &[LifecycleEvent]) -> HashMap<String, Vec<String>> {
    let mut current: HashMap<u64, String> = HashMap::new();
    let mut ran_on: HashMap<String, Vec<String>> = HashMap::new();
    for event in events {
        match event {
            LifecycleEvent::WorkerBound { worker, device } => {
                current.insert(*worker, device.clone());
            }
            LifecycleEvent::Leased { task, worker, .. } => {
                let device = current
                    .get(worker)
                    .expect("a worker reports its device before it leases");
                ran_on.entry(task.clone()).or_default().push(device.clone());
            }
            _ => {}
        }
    }
    ran_on
}

/// One candidate's chain as the store holds it.
pub struct ChainTrail {
    /// The task keys of the segments walked, in order: every committed one,
    /// and the first uncommitted one where the walk stopped.
    pub keys: Vec<String>,
    /// How many of the chain's segments are committed.
    pub committed: usize,
    /// The chain's segment count, from the run config.
    pub segments: usize,
}

impl ChainTrail {
    /// Whether the chain has segments still to run.
    pub fn has_work_left(&self) -> bool {
        self.committed < self.segments
    }
}

/// Each chain of the run, walked through the state its segments committed:
/// chain `i` is candidate `i`'s trajectory.
///
/// The journal names tasks, never chains, so this is what joins a chain to the
/// devices its work ran on. It derives the same identities the scheduler's own
/// chain source does — candidate `i`'s seed substream, then each successor's
/// input state from its predecessor's committed `state` artifact — and stops
/// at the first segment the store has yet to answer.
pub fn chain_trails(config_path: &Path) -> Vec<ChainTrail> {
    let config = load(config_path).expect("load config");
    let store = Store::open(&config.store).expect("open store");
    let generator = generator_for(&config.run.generator.id).expect("dispatch the generator");
    let environment = domain_for(&config.run.format)
        .expect("dispatch the domain")
        .environment
        .id();
    let specs = generator
        .generate(
            config.run.root_seed,
            &config.run.generator.params,
            &config.run.format,
        )
        .expect("generate the run's candidates");
    let params = config.run.params.id();
    let segments = config.run.segments.map_or(1, NonZeroU64::get);

    let mut chains = Vec::with_capacity(specs.len());
    for (i, spec) in specs.iter().enumerate() {
        let mut identity = TaskIdentity {
            spec: spec.id(),
            params,
            seed: sima_core::prng::derive(config.run.root_seed, i as u64),
            environment,
            input_state: None,
        };
        let mut keys = Vec::new();
        let mut committed = 0;
        for _ in 0..segments {
            let key = identity.key();
            keys.push(key.to_string());
            // The next segment continues from this one's committed state; an
            // uncommitted segment ends the chain's known trajectory.
            let Some(record) = store.record(&key).expect("read the record") else {
                break;
            };
            committed += 1;
            let Some(state) = record
                .artifacts()
                .iter()
                .find(|artifact| artifact.name() == "state")
            else {
                break;
            };
            identity.input_state = Some(*state.object());
        }
        chains.push(ChainTrail {
            keys,
            committed,
            segments: segments as usize,
        });
    }
    chains
}

/// The raw bytes of the manifest the run `config_path` describes wrote, or
/// `None` where it has yet to finalize. The file itself, for the comparisons
/// that are about bytes rather than about a parsed value.
pub fn manifest_bytes(config_path: &Path) -> Option<Vec<u8>> {
    let config = load(config_path).expect("load config");
    let path = config
        .store
        .join("runs")
        .join(config.run.id().to_string())
        .join("manifest.json");
    std::fs::read(path).ok()
}

/// Polls `probe` every 20 ms until it holds or `deadline` elapses; returns
/// whether it held. Every wait in the suites goes through a deadline poll —
/// no fixed sleep carries a correctness assumption.
pub fn poll_until(deadline: Duration, probe: impl Fn() -> bool) -> bool {
    let end = Instant::now() + deadline;
    loop {
        if probe() {
            return true;
        }
        if Instant::now() >= end {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Whether `pid` is a live `sima-worker` process. A recycled pid under
/// another command name reads as dead, so the check never latches onto an
/// unrelated process.
pub fn worker_alive(pid: u32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    comm_of(&stat) == Some("sima-worker")
}

/// The command name framed in parentheses in a `/proc/<pid>/stat` line.
fn comm_of(stat: &str) -> Option<&str> {
    let open = stat.find('(')?;
    let close = stat.rfind(')')?;
    Some(&stat[open + 1..close])
}
