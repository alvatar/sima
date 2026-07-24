//! [`LoadedConfig`]: a `sima.toml`, loaded and translated.
//!
//! The file schema (this comment is the reference):
//!
//! ```toml
//! [run]                     # identity section — canonicalized into
//! root_seed = 42            # RunConfig, so these fields define the RunId
//! format = "stub.v1"
//! segments = 10             # optional; absent = static batch; must be >= 1
//!
//! [run.generator]
//! id = "stub.v1"
//! # remaining keys are generator-specific; stub.v1 takes:
//! behaviors = ["succeed", "flaky:2", "sleep:50", "reject", "panic"]
//!
//! [run.params]              # domain-specific; stub.v1 takes:
//! hex = ""                  # optional hex string, default empty
//!
//! [execution]               # operational — never hashed
//! store = "./store"         # resolved relative to this file's directory
//! workers = 4               # required, unless [[execution.device]] is used
//! max_attempts = 3
//! attempt_timeout_ms = 5000 # optional; absent disables the attempt deadline
//! checkpoint_interval_ms = 30000 # optional; wall-clock checkpoint cadence
//! checkpoint_interval_steps = 100 # optional; step-count cadence, >= 1
//!
//! [[execution.device]]      # optional; absent = the backend's own choice
//! select = "nvidia"         # case-insensitive substring of the device name,
//! workers = 3               # or its exact "vendor:device" hex pair
//!
//! [[execution.device]]
//! select = "8086:7d67"
//! workers = 1
//!
//! [[execution.remote]]      # optional; a worker pool running inside a
//! host     = "gpubox"       # container. host is optional: an ssh destination
//! workers  = 4              # runs the container there; absent runs it on this
//! image    = "localhost/sima-worker:latest"   # machine with no ssh hop.
//! runtime  = "docker"       # optional; docker | podman
//! run_args = ["--gpus", "all"]                # optional; verbatim run flags
//!                           # workers XOR [[execution.remote.device]] tables
//! [[execution.remote.device]]  # optional; same semantics as local
//! select  = "nvidia"
//! workers = 4
//!
//! [fleet]                   # optional; a pool of rented instances the run
//! provider = "vast"         # acquires. "vast" | "stub" (stub: in-process,
//! count = 2                 # for tests). count is the instances to acquire,
//! fill = "strict"           # >= 1. fill is "strict" | "best-effort", default
//! image = "ghcr.io/alvatar/sima-worker:latest"  # "strict"; default image as
//! disk_gb = 32              # shown. disk_gb, ready_timeout_ms (600000), and
//! ready_timeout_ms = 600000 # ready_poll_ms (5000) all default as shown.
//! ready_poll_ms = 5000
//!
//! [fleet.constraints]       # optional; every key optional, maps onto the
//! gpu_models = ["RTX 4090"] # provider's offer constraints. max_price_usd_hour
//! min_gpu_count = 1         # is f64 dollars, converted to a micro-USD rate.
//! min_vram_mb = 16000
//! max_price_usd_hour = 0.5
//! min_reliability = 0.95
//! verified_only = true
//! min_disk_gb = 32
//! min_bandwidth_mbps = 100
//!
//! [fleet.budget]            # optional; both keys optional. max_spend_usd is
//! max_spend_usd = 5.0       # f64 dollars, converted to a micro-USD cost cap,
//! max_wall_clock_ms = 3600000  # rounded up.
//! ```
//!
//! The two checkpoint cadences are unioned: a save is due when either fires,
//! and either present enables checkpointing. With both absent, no checkpoint
//! is ever written.
//!
//! With `[[execution.device]]` entries the local pool is their sum, so the
//! top-level `workers` key must be absent; without either, there is no local
//! pool — valid only when an `[[execution.remote]]` pool carries the run. Each
//! `select` must name exactly one device, resolved against the machine's
//! hardware when a run starts — never at load, so reading a config needs no
//! GPU.
//!
//! Each `[[execution.remote]]` pool sets `workers` or its own device tables but
//! never both nor neither, its `runtime` is `docker` or `podman`, and no two
//! entries name one machine — each a validation error naming the machine. A
//! pool's device selectors resolve at run start, over the machine its container
//! runs on (the `host`, or this one when `host` is absent).
//!
//! The `[run]` section is canonicalized into [`RunConfig`], so its fields
//! define the run id; `[execution]` is operational and never hashed — a run
//! resumed with different parallelism or from a different store path keeps
//! its id. The structural keys are strict: an unknown key anywhere is
//! rejected. The `[run.generator]` table (minus `id`) and the `[run.params]`
//! table pass opaquely to the generator and domain translations, which own
//! and validate their keys.
//!
//! The `[fleet]` section is operational too: it decides where tasks run,
//! never what they produce, so it never enters the run id. Its constraints
//! and budget map onto the provider control plane's own types.

use std::fs;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use sima_core::{Error, Result};
use sima_domains::{generator_params_for, params_for};
use sima_model::{FormatId, GeneratorConfig, GeneratorId, RunConfig};
use sima_provider::{Budget, Constraints, Cost, Price};
use sima_scheduler::ExecutionConfig;

use crate::devices::DeviceSelector;

/// The image a remote pool runs when its config names none.
const DEFAULT_IMAGE: &str = "localhost/sima-worker:latest";
/// The container runtime a remote pool uses when its config names none.
const DEFAULT_RUNTIME: &str = "docker";
/// The worker image a fleet rents when its config names none.
const DEFAULT_FLEET_IMAGE: &str = "ghcr.io/alvatar/sima-worker:latest";
/// The disk a fleet instance is provisioned with when its config names none.
const DEFAULT_FLEET_DISK_GB: u64 = 32;
/// How long a fleet waits for an instance to become reachable when its config
/// names no timeout: the provider host pulls the image before the container
/// exists, which takes minutes.
const DEFAULT_READY_TIMEOUT_MS: u64 = 600_000;
/// How often a fleet polls an instance for readiness when its config names no
/// interval.
const DEFAULT_READY_POLL_MS: u64 = 5_000;

/// A `sima.toml`, loaded and translated: the identity-bearing
/// [`RunConfig`], the operational [`ExecutionConfig`], and the store path
/// resolved relative to the config file.
#[derive(Debug)]
pub struct LoadedConfig {
    /// The identity section, canonicalized; its id is the run id.
    pub run: RunConfig,
    /// The execution section; never hashed. Its `workers` is the local pool
    /// size — `0` for a run with no local pool — and its device entries are
    /// empty here: a selector names real hardware, so it resolves where the
    /// run starts.
    pub execution: ExecutionConfig,
    /// The local pool's devices, unresolved. Empty means the local pool takes
    /// the backend's default selection, or that there is no local pool.
    pub devices: Vec<DeviceSelector>,
    /// The remote pools, in config order. Empty means a local-only run.
    pub remotes: Vec<RemoteConfig>,
    /// The rented-instance fleet, or `None` for a run that rents nothing.
    pub fleet: Option<FleetConfig>,
    /// The store path, resolved against the config file's directory.
    pub store: PathBuf,
}

/// Which control-plane backend a fleet acquires instances through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetProvider {
    /// The Vast.ai marketplace backend.
    Vast,
    /// The in-process stub backend: scripted offers, instant readiness, and a
    /// local-spawn transport, so the fleet spine is exercised without a network
    /// or real hardware. The testing path.
    Stub,
}

/// What a fleet does when it cannot acquire its full declared count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillPolicy {
    /// The full count or the run fails before any task runs, tearing down
    /// whatever was acquired.
    Strict,
    /// Run with what was acquired, at least one instance.
    BestEffort,
}

/// The resolved `[fleet]` section: a pool of rented instances acquired beside
/// the local and manual remote pools. Operational only — it decides where
/// tasks run, never what they produce, so it never enters the run id.
///
/// The constraints and budget are the provider control plane's own types, so
/// the section maps onto them without an intermediate mirror.
#[derive(Debug, Clone)]
pub struct FleetConfig {
    /// The control-plane backend to acquire through.
    pub provider: FleetProvider,
    /// How many instances to acquire; at least one.
    pub count: usize,
    /// What to do on a shortfall.
    pub fill: FillPolicy,
    /// The worker image each instance runs.
    pub image: String,
    /// The disk each instance is provisioned with, in gigabytes.
    pub disk_gb: u64,
    /// How long to wait for an instance to become reachable before giving up
    /// on it.
    pub ready_timeout: Duration,
    /// How often to poll an instance for readiness.
    pub ready_poll: Duration,
    /// The hard offer constraints that qualify a rentable machine.
    pub constraints: Constraints,
    /// The spend and wall-clock ceilings the rental phase must stay under.
    pub budget: Budget,
}

/// One resolved `[[execution.remote]]` pool: where its container runs and the
/// container settings, with the device selectors left unresolved until the run
/// starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteConfig {
    /// The ssh destination (an alias or `user@host`) the container runs on, or
    /// `None` for a container on this machine with no ssh hop.
    pub host: Option<String>,
    /// The worker image to run.
    pub image: String,
    /// The container runtime: `docker` or `podman`.
    pub runtime: String,
    /// Verbatim flags for the container-run command — GPU access and the like.
    pub run_args: Vec<String>,
    /// The plain worker count, when the pool uses no device tables.
    pub workers: Option<usize>,
    /// The device selectors, unresolved; empty when `workers` is set.
    pub devices: Vec<DeviceSelector>,
}

/// The raw file structure `toml` parses into. Strict on the structural
/// keys; the generator and params tables stay opaque here.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    run: RunSection,
    execution: ExecutionSection,
    /// The `[fleet]` section; absent means a run that rents nothing.
    fleet: Option<FleetSection>,
}

/// The `[run]` section: every field enters run identity.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunSection {
    /// TOML integers are i64; the load rejects negatives. Seeds above
    /// `i64::MAX` are not expressible in the file format.
    root_seed: i64,
    format: String,
    /// The number of tasks each candidate's chain comprises; validated to be
    /// at least 1. Absent means one stateless task per candidate.
    segments: Option<i64>,
    generator: GeneratorSection,
    /// Domain-owned; absent means an empty table, and the domain decides
    /// the defaults.
    #[serde(default)]
    params: toml::Table,
}

/// The `[run.generator]` section: the id names the generator, every other
/// key belongs to it and is validated by its translation.
#[derive(Deserialize)]
struct GeneratorSection {
    id: String,
    #[serde(flatten)]
    rest: toml::Table,
}

/// The `[execution]` section: operational settings, never hashed.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionSection {
    store: String,
    /// The pool size, required unless `[[execution.device]]` entries carry it.
    workers: Option<usize>,
    max_attempts: u32,
    attempt_timeout_ms: Option<u64>,
    checkpoint_interval_ms: Option<u64>,
    checkpoint_interval_steps: Option<u64>,
    /// The `[[execution.device]]` entries; absent means the run takes the
    /// backend's default device selection.
    #[serde(default)]
    device: Vec<DeviceSection>,
    /// The `[[execution.remote]]` pools; absent means a local-only run.
    #[serde(default)]
    remote: Vec<RemoteSection>,
}

/// One `[[execution.device]]` entry: which device, and how many workers on it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceSection {
    select: String,
    workers: usize,
}

/// One `[[execution.remote]]` pool: workers inside a container, on an ssh
/// destination or — when `host` is absent — on this machine. `workers` and
/// `[[execution.remote.device]]` are exclusive, as they are locally.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteSection {
    host: Option<String>,
    workers: Option<usize>,
    image: Option<String>,
    runtime: Option<String>,
    #[serde(default)]
    run_args: Vec<String>,
    #[serde(default)]
    device: Vec<DeviceSection>,
}

/// The `[fleet]` section: a pool of rented instances. `provider` and `fill`
/// are read as strings and matched, so an unknown value is a validation error
/// naming it; `count` is validated to be at least 1.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FleetSection {
    provider: String,
    /// TOML integers are i64; the load rejects values below 1.
    count: i64,
    fill: Option<String>,
    image: Option<String>,
    disk_gb: Option<u64>,
    ready_timeout_ms: Option<u64>,
    ready_poll_ms: Option<u64>,
    #[serde(default)]
    constraints: FleetConstraintsSection,
    budget: Option<FleetBudgetSection>,
}

/// The `[fleet.constraints]` table: every key optional, each mapping onto one
/// field of the provider's offer constraints. `max_price_usd_hour` is dollars,
/// converted to a micro-USD rate.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FleetConstraintsSection {
    #[serde(default)]
    gpu_models: Vec<String>,
    min_gpu_count: Option<u32>,
    min_vram_mb: Option<u64>,
    max_price_usd_hour: Option<f64>,
    min_reliability: Option<f64>,
    #[serde(default)]
    verified_only: bool,
    min_disk_gb: Option<u64>,
    min_bandwidth_mbps: Option<u64>,
}

/// The `[fleet.budget]` table: both keys optional. `max_spend_usd` is dollars,
/// converted to a micro-USD cost cap rounded up.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FleetBudgetSection {
    max_spend_usd: Option<f64>,
    max_wall_clock_ms: Option<u64>,
}

/// Loads and translates the `sima.toml` at `path`. Parse errors, unknown
/// or missing keys, and invalid values are [`Error::Validation`] naming
/// the file; the generator and params tables are validated by the code
/// the config names.
pub fn load(path: &Path) -> Result<LoadedConfig> {
    let text = fs::read_to_string(path).map_err(|e| Error::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let file: FileConfig =
        toml::from_str(&text).map_err(|e| Error::Validation(format!("{}: {e}", path.display())))?;

    let root_seed = u64::try_from(file.run.root_seed).map_err(|_| {
        Error::Validation(format!(
            "{}: root_seed must be non-negative, got {}",
            path.display(),
            file.run.root_seed
        ))
    })?;
    let segments = file
        .run
        .segments
        .map(|value| {
            u64::try_from(value)
                .ok()
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "{}: segments must be at least 1, got {value}",
                        path.display()
                    ))
                })
        })
        .transpose()?;
    let format = FormatId::new(file.run.format)?;
    let generator_id = GeneratorId::new(file.run.generator.id)?;
    // Identity flows through the dispatched-to code: the generator and the
    // domain turn their tables into the canonical bytes the model hashes.
    let generator_params = generator_params_for(&generator_id, &file.run.generator.rest)?;
    let params = params_for(&format, &file.run.params, segments.is_some())?;
    let run = RunConfig {
        root_seed,
        segments,
        format,
        generator: GeneratorConfig {
            id: generator_id,
            params: generator_params,
        },
        params,
    };

    let attempt_timeout = file
        .execution
        .attempt_timeout_ms
        .map_or(Duration::MAX, Duration::from_millis);
    let checkpoint_interval = file
        .execution
        .checkpoint_interval_ms
        .map_or(Duration::MAX, Duration::from_millis);
    // The step cadence is optional and, when present, at least 1: a zero
    // cadence has no meaning (every offer, and no offer, at once), so it is a
    // validation fault naming the key.
    let checkpoint_interval_steps = file
        .execution
        .checkpoint_interval_steps
        .map(|n| {
            NonZeroU64::new(n).ok_or_else(|| {
                Error::Validation(format!(
                    "{}: checkpoint_interval_steps must be at least 1, got 0",
                    path.display()
                ))
            })
        })
        .transpose()?;
    let remotes = resolve_remotes(path, file.execution.remote)?;
    let fleet = resolve_fleet(path, file.fleet)?;
    // The local pool size comes from one place or the other, never both: with
    // device entries the pool is their sum, so a top-level count could only
    // disagree with it. With neither, there is no local pool — valid only when
    // a remote pool or a fleet carries the work.
    let workers = match (file.execution.workers, file.execution.device.is_empty()) {
        (Some(_), false) => {
            return Err(Error::Validation(format!(
                "{}: execution.workers and [[execution.device]] cannot both be set; \
                 the device entries carry the workers",
                path.display()
            )));
        }
        (None, true) if remotes.is_empty() && fleet.is_none() => {
            return Err(Error::Validation(format!(
                "{}: execution.workers is required without [[execution.device]] entries, \
                 an [[execution.remote]] pool, or a [fleet]",
                path.display()
            )));
        }
        (Some(workers), true) => workers,
        (None, false) => file.execution.device.iter().map(|d| d.workers).sum(),
        // No local pool: the remotes carry the run.
        (None, true) => 0,
    };
    let execution = ExecutionConfig::new(
        workers,
        file.execution.max_attempts,
        attempt_timeout,
        checkpoint_interval,
        checkpoint_interval_steps,
    )?;
    // The selectors stay unresolved: they name real hardware, and loading a
    // config must work where none is present.
    let devices: Vec<DeviceSelector> = file
        .execution
        .device
        .into_iter()
        .map(|entry| DeviceSelector {
            select: entry.select,
            workers: entry.workers,
        })
        .collect();

    // Relative to the config file's directory, never the working directory;
    // join leaves an absolute path as written.
    let base = path.parent().unwrap_or(Path::new(""));
    let store = base.join(&file.execution.store);

    Ok(LoadedConfig {
        run,
        execution,
        devices,
        remotes,
        fleet,
        store,
    })
}

/// Converts a dollar amount to micro-USD, rounding up so a cap or rate is
/// never rendered stricter than the figure written. The value must be
/// validated finite and non-negative first.
fn dollars_to_micro_ceil(dollars: f64) -> u64 {
    (dollars * 1_000_000.0).ceil() as u64
}

/// Validates that a dollar figure is finite and non-negative, naming `key` on
/// failure.
fn finite_dollars(path: &Path, key: &str, value: f64) -> Result<f64> {
    if !value.is_finite() || value < 0.0 {
        return Err(Error::Validation(format!(
            "{}: fleet {key} must be finite and non-negative, got {value}",
            path.display()
        )));
    }
    Ok(value)
}

/// Validates the `[fleet]` section and resolves it into a [`FleetConfig`], its
/// constraints and budget mapped onto the provider control plane's types.
/// `provider` and `fill` must name a known variant, `count` must be at least
/// one, and every money value must be finite and non-negative — each a
/// [`Error::Validation`] naming what was wrong.
fn resolve_fleet(path: &Path, section: Option<FleetSection>) -> Result<Option<FleetConfig>> {
    let Some(section) = section else {
        return Ok(None);
    };
    let provider = match section.provider.as_str() {
        "vast" => FleetProvider::Vast,
        "stub" => FleetProvider::Stub,
        other => {
            return Err(Error::Validation(format!(
                "{}: fleet provider {other:?} is not one of vast, stub",
                path.display()
            )));
        }
    };
    let count = usize::try_from(section.count)
        .ok()
        .filter(|&count| count >= 1)
        .ok_or_else(|| {
            Error::Validation(format!(
                "{}: fleet count must be at least 1, got {}",
                path.display(),
                section.count
            ))
        })?;
    let fill = match section.fill.as_deref() {
        None | Some("strict") => FillPolicy::Strict,
        Some("best-effort") => FillPolicy::BestEffort,
        Some(other) => {
            return Err(Error::Validation(format!(
                "{}: fleet fill {other:?} is not one of strict, best-effort",
                path.display()
            )));
        }
    };

    let constraints_section = section.constraints;
    let max_price = constraints_section
        .max_price_usd_hour
        .map(|dollars| {
            finite_dollars(path, "max_price_usd_hour", dollars)
                .map(|dollars| Price(dollars_to_micro_ceil(dollars)))
        })
        .transpose()?;
    let constraints = Constraints {
        gpu_models: constraints_section.gpu_models,
        min_gpu_count: constraints_section.min_gpu_count,
        min_vram_mb: constraints_section.min_vram_mb,
        max_price,
        min_reliability: constraints_section.min_reliability,
        verified_only: constraints_section.verified_only,
        min_disk_gb: constraints_section.min_disk_gb,
        min_bandwidth_mbps: constraints_section.min_bandwidth_mbps,
        // The excluded set is not configured: acquisition derives it from the
        // reputation ledger at each attempt.
        excluded_machines: Vec::new(),
    };

    let budget = match section.budget {
        None => Budget::default(),
        Some(budget) => {
            let max_spend = budget
                .max_spend_usd
                .map(|dollars| {
                    finite_dollars(path, "max_spend_usd", dollars)
                        .map(|dollars| Cost(dollars_to_micro_ceil(dollars)))
                })
                .transpose()?;
            Budget {
                max_spend,
                max_wall_clock: budget.max_wall_clock_ms.map(Duration::from_millis),
            }
        }
    };

    Ok(Some(FleetConfig {
        provider,
        count,
        fill,
        image: section
            .image
            .unwrap_or_else(|| DEFAULT_FLEET_IMAGE.to_string()),
        disk_gb: section.disk_gb.unwrap_or(DEFAULT_FLEET_DISK_GB),
        ready_timeout: Duration::from_millis(
            section.ready_timeout_ms.unwrap_or(DEFAULT_READY_TIMEOUT_MS),
        ),
        ready_poll: Duration::from_millis(section.ready_poll_ms.unwrap_or(DEFAULT_READY_POLL_MS)),
        constraints,
        budget,
    }))
}

/// Validates the `[[execution.remote]]` entries and resolves each into a
/// [`RemoteConfig`], its device selectors left unresolved. Each entry sets
/// `workers` or `[[execution.remote.device]]` but never both nor neither, its
/// `runtime` is `docker` or `podman`, and no two entries name one machine — each
/// a [`Error::Validation`] naming the machine, so the fix is one line. A missing
/// `host` names this machine, so at most one entry may omit it.
fn resolve_remotes(path: &Path, sections: Vec<RemoteSection>) -> Result<Vec<RemoteConfig>> {
    let mut remotes: Vec<RemoteConfig> = Vec::with_capacity(sections.len());
    for section in sections {
        // The machine an entry names, for error messages: the ssh destination,
        // or this machine when the container runs locally.
        let machine = match &section.host {
            Some(host) => format!("host {host:?}"),
            None => "the local machine".to_string(),
        };
        if remotes.iter().any(|r| r.host == section.host) {
            return Err(Error::Validation(format!(
                "{}: two [[execution.remote]] entries name {machine}; one entry per machine",
                path.display(),
            )));
        }
        // Workers XOR device tables, exactly as locally.
        match (section.workers, section.device.is_empty()) {
            (Some(_), false) => {
                return Err(Error::Validation(format!(
                    "{}: remote {machine} sets both workers and [[execution.remote.device]]; \
                     the device entries carry the workers",
                    path.display(),
                )));
            }
            (None, true) => {
                return Err(Error::Validation(format!(
                    "{}: remote {machine} sets neither workers nor [[execution.remote.device]]; \
                     one is required",
                    path.display(),
                )));
            }
            _ => {}
        }
        let runtime = section
            .runtime
            .unwrap_or_else(|| DEFAULT_RUNTIME.to_string());
        if runtime != "docker" && runtime != "podman" {
            return Err(Error::Validation(format!(
                "{}: remote {machine} runtime {runtime:?} is not one of docker, podman",
                path.display(),
            )));
        }
        let devices = section
            .device
            .into_iter()
            .map(|entry| DeviceSelector {
                select: entry.select,
                workers: entry.workers,
            })
            .collect();
        remotes.push(RemoteConfig {
            host: section.host,
            image: section.image.unwrap_or_else(|| DEFAULT_IMAGE.to_string()),
            runtime,
            run_args: section.run_args,
            workers: section.workers,
            devices,
        });
    }
    Ok(remotes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_domains::{StubBehavior, StubGeneratorConfig};
    use sima_model::RunId;

    /// Writes `text` as a config file named `name` under `dir`.
    fn write_config(dir: &Path, name: &str, text: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, text).expect("write config file");
        path
    }

    /// The reference schema instance from the module doc.
    const BASE: &str = r#"
        [run]
        root_seed = 42
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed", "flaky:2", "sleep:50", "reject", "panic"]

        [run.params]
        hex = "00ff"

        [execution]
        store = "./store"
        workers = 4
        max_attempts = 3
        attempt_timeout_ms = 5000
    "#;

    /// Loads `text` from a fresh tempdir.
    fn load_text(text: &str) -> Result<LoadedConfig> {
        let dir = tempfile::tempdir().expect("temp dir");
        load(&write_config(dir.path(), "sima.toml", text))
    }

    /// The run id `text` loads to.
    fn id_of(text: &str) -> RunId {
        load_text(text).expect("config loads").run.id()
    }

    /// The reference schema without a top-level `workers`, for the configs that
    /// carry `[[execution.device]]` entries instead.
    const DEVICE_BASE: &str = r#"
        [run]
        root_seed = 42
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed"]

        [execution]
        store = "./store"
        max_attempts = 3
    "#;

    #[test]
    fn device_entries_load_as_unresolved_selectors() -> Result<()> {
        let loaded = load_text(&format!(
            r#"{DEVICE_BASE}
            [[execution.device]]
            select = "nvidia"
            workers = 3

            [[execution.device]]
            select = "8086:7d67"
            workers = 1
            "#
        ))?;
        assert_eq!(
            loaded.devices,
            vec![
                DeviceSelector {
                    select: "nvidia".to_string(),
                    workers: 3,
                },
                DeviceSelector {
                    select: "8086:7d67".to_string(),
                    workers: 1,
                },
            ]
        );
        // The pool is the entries' sum; the classes resolve at run start, so
        // the loaded settings name no device yet.
        assert_eq!(loaded.execution.workers, 4);
        assert!(loaded.execution.devices.is_empty());
        Ok(())
    }

    #[test]
    fn a_config_without_device_entries_asks_for_no_device() -> Result<()> {
        let loaded = load_text(BASE)?;
        assert!(loaded.devices.is_empty());
        assert_eq!(loaded.execution.workers, 4);
        Ok(())
    }

    #[test]
    fn workers_and_device_entries_may_not_both_be_set() {
        let error = load_text(&format!(
            r#"{BASE}
            [[execution.device]]
            select = "nvidia"
            workers = 3
            "#
        ))
        .expect_err("the pool would have two sizes");
        let Err(Error::Validation(message)) = Err::<(), _>(error) else {
            panic!("expected a validation error");
        };
        assert!(message.contains("execution.workers"), "{message}");
        assert!(message.contains("execution.device"), "{message}");
    }

    #[test]
    fn workers_is_required_without_device_entries() {
        let error = load_text(DEVICE_BASE).expect_err("no pool size anywhere");
        let Err(Error::Validation(message)) = Err::<(), _>(error) else {
            panic!("expected a validation error");
        };
        assert!(message.contains("execution.workers"), "{message}");
    }

    #[test]
    fn a_device_entry_rejects_an_unknown_key() {
        let error = load_text(&format!(
            r#"{DEVICE_BASE}
            [[execution.device]]
            select = "nvidia"
            workers = 3
            member = 1
            "#
        ))
        .expect_err("device entries are strict");
        assert!(matches!(error, Error::Validation(_)));
    }

    #[test]
    fn device_entries_never_enter_run_identity() {
        // `[execution]` is operational: one `[run]` section is one run,
        // whether its workers sit on one device or spread over two.
        let one_device = id_of(&format!(
            r#"{DEVICE_BASE}
            [[execution.device]]
            select = "nvidia"
            workers = 4
            "#
        ));
        let two_devices = id_of(&format!(
            r#"{DEVICE_BASE}
            [[execution.device]]
            select = "nvidia"
            workers = 3

            [[execution.device]]
            select = "intel"
            workers = 1
            "#
        ));
        assert_eq!(one_device, two_devices);
        // And the same run with a plain worker count is still that run.
        let no_devices = id_of(&format!("{DEVICE_BASE}\nworkers = 4\n"));
        assert_eq!(one_device, no_devices);
    }

    #[test]
    fn a_remote_pool_loads_with_its_defaults() -> Result<()> {
        let loaded = load_text(&format!(
            "{BASE}\n[[execution.remote]]\nhost = \"gpubox\"\nworkers = 4\n"
        ))?;
        assert_eq!(loaded.remotes.len(), 1);
        let remote = &loaded.remotes[0];
        assert_eq!(remote.host.as_deref(), Some("gpubox"));
        assert_eq!(remote.image, "localhost/sima-worker:latest");
        assert_eq!(remote.runtime, "docker");
        assert!(remote.run_args.is_empty());
        assert_eq!(remote.workers, Some(4));
        assert!(remote.devices.is_empty());
        Ok(())
    }

    #[test]
    fn a_remote_pool_takes_explicit_image_runtime_and_run_args() -> Result<()> {
        let loaded = load_text(&format!(
            r#"{BASE}
            [[execution.remote]]
            host = "gpubox"
            workers = 2
            image = "sima-worker:pinned"
            runtime = "podman"
            run_args = ["--gpus", "all"]
            "#
        ))?;
        let remote = &loaded.remotes[0];
        assert_eq!(remote.image, "sima-worker:pinned");
        assert_eq!(remote.runtime, "podman");
        assert_eq!(remote.run_args, vec!["--gpus", "all"]);
        Ok(())
    }

    #[test]
    fn a_remote_pool_with_device_tables_loads_unresolved() -> Result<()> {
        let loaded = load_text(&format!(
            r#"{BASE}
            [[execution.remote]]
            host = "gpubox"

            [[execution.remote.device]]
            select = "nvidia"
            workers = 4
            "#
        ))?;
        let remote = &loaded.remotes[0];
        assert_eq!(remote.workers, None);
        assert_eq!(remote.devices.len(), 1);
        assert_eq!(remote.devices[0].select, "nvidia");
        assert_eq!(remote.devices[0].workers, 4);
        Ok(())
    }

    #[test]
    fn a_remote_with_both_workers_and_devices_is_rejected() {
        let error = load_text(&format!(
            r#"{BASE}
            [[execution.remote]]
            host = "gpubox"
            workers = 2

            [[execution.remote.device]]
            select = "nvidia"
            workers = 4
            "#
        ))
        .expect_err("the remote pool would have two sizes");
        let Err(Error::Validation(message)) = Err::<(), _>(error) else {
            panic!("expected a validation error");
        };
        assert!(message.contains("gpubox"), "names the host: {message}");
    }

    #[test]
    fn a_remote_with_neither_workers_nor_devices_is_rejected() {
        let error = load_text(&format!(
            "{BASE}\n[[execution.remote]]\nhost = \"gpubox\"\n"
        ))
        .expect_err("the remote pool has no size");
        let Err(Error::Validation(message)) = Err::<(), _>(error) else {
            panic!("expected a validation error");
        };
        assert!(message.contains("gpubox"), "names the host: {message}");
    }

    #[test]
    fn duplicate_remote_hosts_are_rejected() {
        let error = load_text(&format!(
            r#"{BASE}
            [[execution.remote]]
            host = "gpubox"
            workers = 2

            [[execution.remote]]
            host = "gpubox"
            workers = 3
            "#
        ))
        .expect_err("one entry per machine");
        let Err(Error::Validation(message)) = Err::<(), _>(error) else {
            panic!("expected a validation error");
        };
        assert!(message.contains("gpubox"), "names the host: {message}");
    }

    #[test]
    fn a_host_less_remote_loads_as_a_local_container_pool() -> Result<()> {
        // No `host`: the container runs on this machine, no ssh hop. The rest of
        // the pool's settings are unchanged.
        let loaded = load_text(&format!(
            r#"{BASE}
            [[execution.remote]]
            runtime = "podman"
            run_args = ["--device", "/dev/dri"]

            [[execution.remote.device]]
            select = "nvidia"
            workers = 2
            "#
        ))?;
        let remote = &loaded.remotes[0];
        assert_eq!(remote.host, None, "a container pool on this machine");
        assert_eq!(remote.runtime, "podman");
        assert_eq!(remote.run_args, vec!["--device", "/dev/dri"]);
        assert_eq!(remote.devices.len(), 1);
        Ok(())
    }

    #[test]
    fn two_host_less_remotes_are_rejected() {
        // A missing host names this machine, so two such entries collide the
        // same way two entries naming one ssh destination do.
        let error = load_text(&format!(
            r#"{BASE}
            [[execution.remote]]
            workers = 2

            [[execution.remote]]
            workers = 3
            "#
        ))
        .expect_err("one entry per machine, and the local machine is one");
        let Err(Error::Validation(message)) = Err::<(), _>(error) else {
            panic!("expected a validation error");
        };
        assert!(
            message.contains("the local machine"),
            "names the machine: {message}"
        );
    }

    #[test]
    fn a_local_container_pool_coexists_with_a_local_bare_pool() -> Result<()> {
        // The bare local pool and a host-less container pool are two distinct
        // pools on one machine; both load.
        let loaded = load_text(&format!(
            r#"{BASE}
            [[execution.remote]]
            workers = 2
            "#
        ))?;
        assert_eq!(loaded.execution.workers, 4, "the bare local pool");
        assert_eq!(loaded.remotes.len(), 1);
        assert_eq!(loaded.remotes[0].host, None);
        Ok(())
    }

    #[test]
    fn an_unknown_remote_runtime_is_rejected() {
        let error = load_text(&format!(
            r#"{BASE}
            [[execution.remote]]
            host = "gpubox"
            workers = 2
            runtime = "containerd"
            "#
        ))
        .expect_err("the runtime must be docker or podman");
        let Err(Error::Validation(message)) = Err::<(), _>(error) else {
            panic!("expected a validation error");
        };
        assert!(
            message.contains("containerd"),
            "names the runtime: {message}"
        );
    }

    #[test]
    fn a_run_may_have_no_local_pool_when_a_remote_carries_it() -> Result<()> {
        // No top-level workers and no local device tables: the remote pool is
        // the whole run, so the local pool size is zero.
        let loaded = load_text(&format!(
            "{DEVICE_BASE}\n[[execution.remote]]\nhost = \"gpubox\"\nworkers = 4\n"
        ))?;
        assert_eq!(loaded.execution.workers, 0, "no local pool");
        assert_eq!(loaded.remotes.len(), 1);
        Ok(())
    }

    #[test]
    fn a_remote_section_rejects_an_unknown_key() {
        let error = load_text(&format!(
            r#"{BASE}
            [[execution.remote]]
            host = "gpubox"
            workers = 2
            user = "root"
            "#
        ))
        .expect_err("remote entries are strict");
        assert!(matches!(error, Error::Validation(_)));
    }

    #[test]
    fn remotes_never_enter_run_identity() {
        // `[execution]` is operational: adding a remote pool does not change
        // which run the `[run]` section names.
        let local_only = id_of(BASE);
        let with_remote = id_of(&format!(
            "{BASE}\n[[execution.remote]]\nhost = \"gpubox\"\nworkers = 4\n"
        ));
        assert_eq!(local_only, with_remote);
    }

    /// A full `[fleet]` section, every key set, appended after a base config.
    const FULL_FLEET: &str = r#"
        [fleet]
        provider = "vast"
        count = 2
        fill = "strict"
        image = "ghcr.io/example/worker:pinned"
        disk_gb = 64
        ready_timeout_ms = 120000
        ready_poll_ms = 2000

        [fleet.constraints]
        gpu_models = ["RTX 4090"]
        min_gpu_count = 1
        min_vram_mb = 16000
        max_price_usd_hour = 0.5
        min_reliability = 0.95
        verified_only = true
        min_disk_gb = 32
        min_bandwidth_mbps = 100

        [fleet.budget]
        max_spend_usd = 5.0
        max_wall_clock_ms = 3600000
    "#;

    #[test]
    fn a_full_fleet_section_resolves_to_the_expected_types() -> Result<()> {
        let loaded = load_text(&format!("{BASE}{FULL_FLEET}"))?;
        let fleet = loaded.fleet.expect("a fleet section");
        assert_eq!(fleet.provider, FleetProvider::Vast);
        assert_eq!(fleet.count, 2);
        assert_eq!(fleet.fill, FillPolicy::Strict);
        assert_eq!(fleet.image, "ghcr.io/example/worker:pinned");
        assert_eq!(fleet.disk_gb, 64);
        assert_eq!(fleet.ready_timeout, Duration::from_millis(120000));
        assert_eq!(fleet.ready_poll, Duration::from_millis(2000));
        // The constraints map 1:1, with the dollar rate converted to micro-USD.
        assert_eq!(fleet.constraints.gpu_models, vec!["RTX 4090".to_string()]);
        assert_eq!(fleet.constraints.min_gpu_count, Some(1));
        assert_eq!(fleet.constraints.min_vram_mb, Some(16000));
        assert_eq!(fleet.constraints.max_price, Some(Price(500_000)));
        assert_eq!(fleet.constraints.min_reliability, Some(0.95));
        assert!(fleet.constraints.verified_only);
        assert_eq!(fleet.constraints.min_disk_gb, Some(32));
        assert_eq!(fleet.constraints.min_bandwidth_mbps, Some(100));
        // The budget's dollar cap converts to a micro-USD cost.
        assert_eq!(fleet.budget.max_spend, Some(Cost(5_000_000)));
        assert_eq!(
            fleet.budget.max_wall_clock,
            Some(Duration::from_millis(3600000))
        );
        Ok(())
    }

    #[test]
    fn a_minimal_fleet_section_takes_every_default() -> Result<()> {
        // Only the required keys; everything else falls to its default.
        let loaded = load_text(&format!(
            "{BASE}\n[fleet]\nprovider = \"stub\"\ncount = 1\n"
        ))?;
        let fleet = loaded.fleet.expect("a fleet section");
        assert_eq!(fleet.provider, FleetProvider::Stub);
        assert_eq!(fleet.count, 1);
        assert_eq!(fleet.fill, FillPolicy::Strict);
        assert_eq!(fleet.image, "ghcr.io/alvatar/sima-worker:latest");
        assert_eq!(fleet.disk_gb, 32);
        assert_eq!(fleet.ready_timeout, Duration::from_millis(600_000));
        assert_eq!(fleet.ready_poll, Duration::from_millis(5_000));
        // Absent constraints and budget are the permissive defaults.
        assert!(fleet.constraints.gpu_models.is_empty());
        assert_eq!(fleet.constraints.max_price, None);
        assert!(!fleet.constraints.verified_only);
        assert_eq!(fleet.budget.max_spend, None);
        assert_eq!(fleet.budget.max_wall_clock, None);
        Ok(())
    }

    #[test]
    fn best_effort_fill_resolves() -> Result<()> {
        let loaded = load_text(&format!(
            "{BASE}\n[fleet]\nprovider = \"stub\"\ncount = 2\nfill = \"best-effort\"\n"
        ))?;
        assert_eq!(loaded.fleet.expect("a fleet").fill, FillPolicy::BestEffort);
        Ok(())
    }

    #[test]
    fn a_cost_cap_rounds_up() -> Result<()> {
        // A fractional-micro dollar cap rounds up so the cap is never rendered
        // stricter than written.
        let loaded = load_text(&format!(
            "{BASE}\n[fleet]\nprovider = \"stub\"\ncount = 1\n\
             [fleet.budget]\nmax_spend_usd = 1.2345678\n"
        ))?;
        let fleet = loaded.fleet.expect("a fleet");
        assert_eq!(fleet.budget.max_spend, Some(Cost(1_234_568)));
        Ok(())
    }

    #[test]
    fn a_zero_fleet_count_is_rejected_naming_the_key() {
        for value in ["count = 0", "count = -1"] {
            let text = format!("{BASE}\n[fleet]\nprovider = \"stub\"\n{value}\n");
            match load_text(&text) {
                Err(Error::Validation(msg)) => {
                    assert!(msg.contains("count"), "the error names count: {msg}");
                }
                other => panic!("expected Validation for {value:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn an_unknown_fleet_fill_is_rejected_naming_it() {
        let text = format!("{BASE}\n[fleet]\nprovider = \"stub\"\ncount = 1\nfill = \"eager\"\n");
        match load_text(&text) {
            Err(Error::Validation(msg)) => assert!(msg.contains("eager"), "names the value: {msg}"),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_fleet_provider_is_rejected_naming_it() {
        let text = format!("{BASE}\n[fleet]\nprovider = \"aws\"\ncount = 1\n");
        match load_text(&text) {
            Err(Error::Validation(msg)) => assert!(msg.contains("aws"), "names the value: {msg}"),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn non_finite_or_negative_fleet_money_is_rejected_naming_the_key() {
        let cases = [
            ("max_price_usd_hour", "[fleet.constraints]", "-0.5"),
            ("max_price_usd_hour", "[fleet.constraints]", "nan"),
            ("max_price_usd_hour", "[fleet.constraints]", "inf"),
            ("max_spend_usd", "[fleet.budget]", "-1.0"),
            ("max_spend_usd", "[fleet.budget]", "nan"),
        ];
        for (key, table, value) in cases {
            let text = format!(
                "{BASE}\n[fleet]\nprovider = \"stub\"\ncount = 1\n{table}\n{key} = {value}\n"
            );
            match load_text(&text) {
                Err(Error::Validation(msg)) => {
                    assert!(msg.contains(key), "names {key}: {msg}");
                }
                other => panic!("expected Validation for {key}={value}, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_fleet_lets_the_local_pool_be_absent() -> Result<()> {
        // No top-level workers, no local device tables, but a fleet carries the
        // run: the local pool size is zero, and the fleet is resolved.
        let loaded = load_text(&format!(
            "{DEVICE_BASE}\n[fleet]\nprovider = \"stub\"\ncount = 2\n"
        ))?;
        assert_eq!(loaded.execution.workers, 0, "no local pool");
        assert!(loaded.fleet.is_some());
        Ok(())
    }

    #[test]
    fn a_fleet_coexists_with_a_local_pool() -> Result<()> {
        // Pools are additive: a fleet and local workers both stand.
        let loaded = load_text(&format!(
            "{BASE}\n[fleet]\nprovider = \"stub\"\ncount = 2\n"
        ))?;
        assert_eq!(loaded.execution.workers, 4, "the local pool");
        assert!(loaded.fleet.is_some());
        Ok(())
    }

    #[test]
    fn a_config_without_a_fleet_loads_without_one() -> Result<()> {
        // The regression guard: a config that names no fleet keeps loading
        // exactly as before, with no fleet resolved.
        assert!(load_text(BASE)?.fleet.is_none());
        Ok(())
    }

    #[test]
    fn a_fleet_section_rejects_an_unknown_key() {
        let text = format!("{BASE}\n[fleet]\nprovider = \"stub\"\ncount = 1\nregion = \"eu\"\n");
        assert!(matches!(load_text(&text), Err(Error::Validation(_))));
    }

    #[test]
    fn a_fleet_never_enters_run_identity() {
        // `[fleet]` is operational: it decides where tasks run, never what they
        // produce, so it does not change which run the `[run]` section names.
        let local_only = id_of(BASE);
        let with_fleet = id_of(&format!("{BASE}{FULL_FLEET}"));
        assert_eq!(local_only, with_fleet);
        // And varying an operational fleet field leaves the id untouched.
        let more_instances = id_of(&format!(
            "{BASE}{}",
            FULL_FLEET.replace("count = 2", "count = 8")
        ));
        assert_eq!(local_only, more_instances);
    }

    #[test]
    fn the_reference_config_loads_into_the_expected_run_config() -> Result<()> {
        let loaded = load_text(BASE)?;
        assert_eq!(loaded.run.root_seed, 42);
        assert_eq!(loaded.run.format.as_str(), "stub.v1");
        assert_eq!(loaded.run.generator.id.as_str(), "stub.v1");
        // The behaviors list encodes through the stub generator's own codec.
        let expected = StubGeneratorConfig {
            behaviors: vec![
                StubBehavior::Succeed,
                StubBehavior::Flaky(2),
                StubBehavior::Sleep(50),
                StubBehavior::Reject,
                StubBehavior::Panic,
            ],
        };
        assert_eq!(loaded.run.generator.params, expected.to_bytes());
        assert_eq!(loaded.run.params.bytes, vec![0x00, 0xff]);
        assert_eq!(loaded.execution.workers, 4);
        assert_eq!(loaded.execution.max_attempts, 3);
        assert_eq!(
            loaded.execution.attempt_timeout,
            Duration::from_millis(5000)
        );
        Ok(())
    }

    #[test]
    fn loading_the_same_file_twice_yields_the_same_run_id() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(dir.path(), "sima.toml", BASE);
        assert_eq!(load(&path)?.run.id(), load(&path)?.run.id());
        Ok(())
    }

    #[test]
    fn every_identity_field_changes_the_run_id() {
        // Every [run] field whose variation still names dispatchable ids:
        // the format and generator ids admit one value in this build, and
        // the model's own tests pin that they enter the id. The remaining
        // fields flow through translation, which is what this pins.
        let base = id_of(BASE);
        for (from, to) in [
            ("root_seed = 42", "root_seed = 43"),
            ("\"succeed\", \"flaky:2\"", "\"succeed\", \"flaky:3\""),
            ("hex = \"00ff\"", "hex = \"00fe\""),
        ] {
            let varied = BASE.replace(from, to);
            assert_ne!(base, id_of(&varied), "{to} must change the run id");
        }
    }

    #[test]
    fn execution_values_never_touch_the_run_id() {
        let base = id_of(BASE);
        for (from, to) in [
            ("store = \"./store\"", "store = \"./elsewhere\""),
            ("workers = 4", "workers = 1"),
            ("max_attempts = 3", "max_attempts = 9"),
            ("attempt_timeout_ms = 5000", "attempt_timeout_ms = 1"),
        ] {
            let varied = BASE.replace(from, to);
            assert_eq!(base, id_of(&varied), "{to} must not change the run id");
        }
    }

    #[test]
    fn unknown_keys_are_rejected_at_every_level() {
        for (section, addition) in [
            ("top level", "surprise = 1\n"),
            ("[run]", "[run]\nsurprise = 1\n"),
            ("[execution]", "[execution]\nsurprise = 1\n"),
            ("[run.params]", "[run.params]\nsurprise = 1\n"),
            ("[run.generator]", "[run.generator]\nsurprise = 1\n"),
        ] {
            // Appending re-opens the named table; TOML allows adding keys to
            // a table from a later header only when they do not collide.
            let text = format!("{BASE}\n{addition}");
            assert!(
                matches!(load_text(&text), Err(Error::Validation(_))),
                "an unknown key at {section} must be rejected"
            );
        }
    }

    #[test]
    fn missing_required_keys_are_rejected() {
        for required in [
            "root_seed = 42",
            "format = \"stub.v1\"",
            "id = \"stub.v1\"",
            "store = \"./store\"",
            "workers = 4",
            "max_attempts = 3",
        ] {
            let text = BASE.replace(required, "");
            assert!(
                matches!(load_text(&text), Err(Error::Validation(_))),
                "a config missing {required:?} must be rejected"
            );
        }
    }

    #[test]
    fn segments_loads_into_the_run_config() -> Result<()> {
        let text = BASE.replace("root_seed = 42", "root_seed = 42\nsegments = 10");
        let loaded = load_text(&text)?;
        assert_eq!(loaded.run.segments, std::num::NonZeroU64::new(10));
        Ok(())
    }

    #[test]
    fn absent_segments_loads_none() -> Result<()> {
        assert_eq!(load_text(BASE)?.run.segments, None);
        Ok(())
    }

    #[test]
    fn zero_or_negative_segments_are_rejected_naming_the_field() {
        for value in ["segments = 0", "segments = -1"] {
            let text = BASE.replace("root_seed = 42", &format!("root_seed = 42\n{value}"));
            match load_text(&text) {
                Err(Error::Validation(msg)) => {
                    assert!(msg.contains("segments"), "the error names the field: {msg}");
                }
                other => panic!("expected Validation for {value:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn segments_changes_the_run_id() {
        let base = id_of(BASE);
        let segmented = BASE.replace("root_seed = 42", "root_seed = 42\nsegments = 10");
        assert_ne!(base, id_of(&segmented));
        // Different segment counts also differ from each other.
        let five = BASE.replace("root_seed = 42", "root_seed = 42\nsegments = 5");
        assert_ne!(id_of(&segmented), id_of(&five));
    }

    #[test]
    fn checkpoint_interval_loads_and_defaults_to_disabled() -> Result<()> {
        assert_eq!(
            load_text(BASE)?.execution.checkpoint_interval,
            Duration::MAX
        );
        let text = BASE.replace(
            "attempt_timeout_ms = 5000",
            "attempt_timeout_ms = 5000\ncheckpoint_interval_ms = 30000",
        );
        assert_eq!(
            load_text(&text)?.execution.checkpoint_interval,
            Duration::from_millis(30000)
        );
        Ok(())
    }

    #[test]
    fn checkpoint_interval_steps_loads_and_defaults_to_disabled() -> Result<()> {
        assert_eq!(load_text(BASE)?.execution.checkpoint_interval_steps, None);
        let text = BASE.replace(
            "attempt_timeout_ms = 5000",
            "attempt_timeout_ms = 5000\ncheckpoint_interval_steps = 100",
        );
        assert_eq!(
            load_text(&text)?.execution.checkpoint_interval_steps,
            NonZeroU64::new(100)
        );
        Ok(())
    }

    #[test]
    fn a_zero_checkpoint_interval_steps_is_rejected_naming_the_key() {
        let text = BASE.replace(
            "attempt_timeout_ms = 5000",
            "attempt_timeout_ms = 5000\ncheckpoint_interval_steps = 0",
        );
        match load_text(&text) {
            Err(Error::Validation(msg)) => assert!(
                msg.contains("checkpoint_interval_steps"),
                "the error names the key: {msg}"
            ),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn checkpoint_interval_steps_never_touches_the_run_id() {
        let base = id_of(BASE);
        let text = BASE.replace(
            "attempt_timeout_ms = 5000",
            "attempt_timeout_ms = 5000\ncheckpoint_interval_steps = 7",
        );
        assert_eq!(base, id_of(&text));
    }

    #[test]
    fn checkpoint_interval_never_touches_the_run_id() {
        let base = id_of(BASE);
        let text = BASE.replace(
            "attempt_timeout_ms = 5000",
            "attempt_timeout_ms = 5000\ncheckpoint_interval_ms = 1",
        );
        assert_eq!(base, id_of(&text));
    }

    #[test]
    fn a_negative_root_seed_is_rejected() {
        let text = BASE.replace("root_seed = 42", "root_seed = -1");
        match load_text(&text) {
            Err(Error::Validation(msg)) => {
                assert!(
                    msg.contains("root_seed"),
                    "the error names the field: {msg}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn the_store_path_resolves_against_the_config_directory() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let nested = dir.path().join("configs");
        fs::create_dir(&nested).expect("create nested dir");
        let loaded = load(&write_config(&nested, "sima.toml", BASE))?;
        // Relative to the file's directory, never the working directory.
        assert_eq!(loaded.store, nested.join("./store"));

        // An absolute store path stays as written.
        let absolute = dir.path().join("elsewhere");
        let text = BASE.replace(
            "store = \"./store\"",
            &format!("store = {:?}", absolute.display()),
        );
        let loaded = load(&write_config(&nested, "absolute.toml", &text))?;
        assert_eq!(loaded.store, absolute);
        Ok(())
    }

    #[test]
    fn an_absent_attempt_timeout_disables_the_deadline() -> Result<()> {
        let text = BASE.replace("attempt_timeout_ms = 5000", "");
        let loaded = load_text(&text)?;
        assert_eq!(loaded.execution.attempt_timeout, Duration::MAX);
        Ok(())
    }

    #[test]
    fn a_syntax_error_is_validation_naming_the_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(dir.path(), "broken.toml", "run = [not toml");
        match load(&path) {
            Err(Error::Validation(msg)) => {
                assert!(
                    msg.contains("broken.toml"),
                    "the error names the file: {msg}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn a_missing_file_is_an_io_error() {
        assert!(matches!(
            load(Path::new("/nonexistent/sima.toml")),
            Err(Error::Io { .. })
        ));
    }
}
