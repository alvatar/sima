//! Shared fixtures for the sima CLI test suites: the config-file writer,
//! the spawn helper over the built binary, the worker-binary build, the
//! process-table scan, and manifest lookup through the pipeline surface.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;
use std::time::{Duration, Instant};

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
