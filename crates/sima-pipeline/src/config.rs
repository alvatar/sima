//! [`LoadedConfig`]: a `sima.toml`, loaded and translated.
//!
//! A run declares the machines it can use by naming them once, and refers to
//! them by name everywhere else. A host is one machine; a host class is several
//! identical machines declared in one entry; `[fleet]` lists the members a run
//! may draw on; `[orchestrator]` is this machine.
//!
//! The file schema (this comment is the reference):
//!
//! ```toml
//! [run]                                   # the only hashed section
//! root_seed = 42
//! format    = "stub.v1"
//! segments  = 10                          # optional; absent = static batch, >= 1
//!
//! [run.generator]
//! id    = "stub.v1"
//! # remaining keys are generator-specific; stub.v1 takes:
//! behaviors = ["succeed", "flaky:2", "sleep:50", "reject", "panic"]
//!
//! [run.params]                            # domain-specific; stub.v1 takes:
//! hex = ""                                # optional hex string, default empty
//!
//! [config]                                # global settings, fully qualified
//! store                     = "./store"   # resolved against this file's directory
//! max_attempts              = 3
//! attempt_timeout_ms        = 300000      # optional; absent disables the deadline
//! checkpoint_interval_ms    = 30000       # optional; wall-clock cadence
//! checkpoint_interval_steps = 500         # optional; step cadence, >= 1
//!
//! [host.gpubox]                           # reached at "gpubox"; image and runtime default
//! # ssh      = "user@10.0.0.5"            # override the address
//! # image    = "localhost/sima:latest"    # default as shown
//! # runtime  = "podman"                   # docker | podman; default docker
//! # run_args = ["--gpus", "all"]          # verbatim container-run flags
//! # workers  = 4                          # exclusive with the device tables below
//! # root     = "~/sima-runs"              # where a migrated run lives here; default as shown
//! # binary   = "sima"                     # the sima binary here; default as shown
//! [[host.gpubox.device]]
//! select  = "nvidia"
//! workers = 2
//!
//! [host.bigbox]                           # named one thing, reached at another
//! ssh     = "bigbox.dept.internal"
//! workers = 8
//!
//! [host.slingshot]                        # one rented machine: a host, not a class
//! provider = "vast"
//! disk_gb  = 64
//! [host.slingshot.constraints]
//! gpu_models  = ["RTX 4090"]
//! min_vram_mb = 16000
//!
//! [host_class.lab]                        # lab1 … lab6; raise count to grow
//! count   = 6
//! workers = 8
//!
//! [host_class.oldlab]                     # addresses that follow no pattern
//! ssh     = ["fermi", "pauli", "dirac"]
//! workers = 4
//!
//! [host_class.rtx4090]                    # four rented to one specification
//! provider = "vast"
//! count    = 4
//! fill     = "best-effort"                # strict | best-effort; default strict
//! disk_gb  = 32
//! [host_class.rtx4090.constraints]        # every key optional
//! gpu_models         = ["RTX 4090"]
//! min_gpu_count      = 1
//! min_vram_mb        = 16000
//! max_price_usd_hour = 0.45
//! min_reliability    = 0.95
//! verified_only      = true
//! min_disk_gb        = 32
//! min_bandwidth_mbps = 100
//!
//! [fleet]
//! members = ["gpubox", "lab", "rtx4090"]
//!
//! [budget]                                # ceilings over every rental in the run
//! max_spend_usd     = 20.0
//! max_wall_clock_ms = 21600000
//!
//! [orchestrator]                          # this machine
//! migrate = "slingshot"                   # the host `sima migrate` moves the run onto
//! # image    = "localhost/sima:latest"    # run this machine's workers in a container
//! # runtime  = "podman"                   # docker | podman; default docker
//! # run_args = ["--gpus", "all"]          # verbatim container-run flags
//! # workers  = 4                          # exclusive with the device tables below
//! [[orchestrator.device]]
//! select  = "nvidia"
//! workers = 1
//! ```
//!
//! ## Addressing
//!
//! The entry's name is its ssh destination unless `ssh` says otherwise, so a
//! class scales by changing one number. A class appends the index to the name
//! with no separator and no padding, so a class of six is `lab1 … lab6`.
//!
//! | Entry | Addresses |
//! |---|---|
//! | `[host.<name>]` | `<name>` |
//! | `[host.<name>]` with `ssh = "…"` | as written |
//! | `[host_class.<name>]` with `count = N` | `<name>1` … `<name>N` |
//! | `[host_class.<name>]` with `ssh = […]` | as written; the list is the count, so `count` is rejected |
//! | any entry with `provider` | from the provider; `ssh` is rejected |
//!
//! ## Keys, by form
//!
//! An entry is **yours** when it names no `provider`, and **rented** when it
//! does. Keys of the other form are rejected, naming the key and the form.
//!
//! | Key | Yours | Rented | Meaning |
//! |---|---|---|---|
//! | `ssh` | yes | no | destination, or a list of them on a class |
//! | `count` | class only | class only | how many machines |
//! | `image` | yes | yes | the worker image |
//! | `runtime` | yes | no | `docker` or `podman` |
//! | `run_args` | yes | no | verbatim container-run flags |
//! | `workers` | yes | no | plain worker count, exclusive with device tables |
//! | `[[….device]]` | yes | no | device tables, exclusive with `workers` |
//! | `provider` | no | yes | `vast` or `stub` |
//! | `fill` | no | class only | `strict` or `best-effort`, default `strict` |
//! | `disk_gb` | no | yes | provisioned disk |
//! | `ready_timeout_ms`, `ready_poll_ms` | no | yes | readiness bounds |
//! | `[….constraints]` | no | yes | offer constraints |
//! | `root` | yes | yes | where a migrated run lives on that machine |
//! | `binary` | yes | yes | the `sima` binary on that machine |
//!
//! A rented machine states no worker layout: it did not exist when the config
//! was written, so its devices come from the `sima-worker --enumerate` probe.
//!
//! `[orchestrator]` is a machine of yours, implicitly this one, so it takes the
//! same worker-side keys an owned host does — `image`, `runtime`, `run_args`,
//! and either `workers` or device tables — plus `migrate`, which names a
//! `[host.*]` entry. It takes no `ssh` and no `provider`, being where the
//! command was typed, and no `root` or `binary`, the run already being here. Its
//! `runtime` and `run_args` describe the container `image` names, so both are
//! rejected without one.
//!
//! `[budget]` is run-global: a run may draw on several rented classes under one
//! ceiling, so the ceiling is a property of the run.
//!
//! ## What a run uses
//!
//! ```text
//! sima run           the orchestrator alone
//! sima run --fleet   the orchestrator plus every member of [fleet]
//! ```
//!
//! Declaring a machine says a run *may* use it; the invocation says it *does*.
//! Without `--fleet` no provider is constructed and no credential is read. A
//! declared host or class that no `[fleet] members` list names is valid and
//! unused.
//!
//! ## Identity and cadence
//!
//! The `[run]` section is canonicalized into [`RunConfig`], so its fields define
//! the run id; every other section is operational and never hashed — a run
//! resumed with different parallelism, a different store path, or a different
//! set of machines keeps its id. The structural keys are strict: an unknown key
//! anywhere is rejected. The `[run.generator]` table (minus `id`) and the
//! `[run.params]` table pass opaquely to the generator and domain translations,
//! which own and validate their keys.
//!
//! The two checkpoint cadences are unioned: a save is due when either fires, and
//! either present enables checkpointing. With both absent, no checkpoint is ever
//! written.
//!
//! A device `select` names real hardware, so it resolves when a run starts and
//! never at load — reading a config needs no GPU.

use std::collections::BTreeMap;
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

/// The image a machine of yours runs its workers from when its entry names
/// none.
const DEFAULT_IMAGE: &str = "localhost/sima:latest";
/// The container runtime a machine of yours uses when its entry names none.
const DEFAULT_RUNTIME: &str = "docker";
/// The worker image a rented machine runs when its entry names none.
const DEFAULT_RENTED_IMAGE: &str = "ghcr.io/alvatar/sima-worker:latest";
/// The disk a rented machine is provisioned with when its entry names none.
const DEFAULT_DISK_GB: u64 = 32;
/// How long a rental waits for an instance to become reachable when its entry
/// names no timeout: the provider host pulls the image before the container
/// exists, which takes minutes.
const DEFAULT_READY_TIMEOUT_MS: u64 = 600_000;
/// How often a rental polls an instance for readiness when its entry names no
/// interval.
const DEFAULT_READY_POLL_MS: u64 = 5_000;
/// Where a migrated run's directory goes on a machine whose entry names no
/// root.
const DEFAULT_ROOT: &str = "~/sima-runs";
/// The `sima` binary a migrated run is driven by on a machine whose entry names
/// none.
const DEFAULT_BINARY: &str = "sima";

/// A `sima.toml`, loaded and translated: the identity-bearing [`RunConfig`], the
/// operational [`ExecutionConfig`], the machines the run may draw on, and the
/// store path resolved relative to the config file.
#[derive(Debug)]
pub struct LoadedConfig {
    /// The identity section, canonicalized; its id is the run id.
    pub run: RunConfig,
    /// The parameters the run executes under, assembled from `[config]` and the
    /// orchestrator's worker layout; never hashed. Its `workers` is the
    /// orchestrator's pool size — `0` for an orchestrator that declares none —
    /// and its device entries are empty here: a selector names real hardware, so
    /// it resolves where the run starts.
    pub execution: ExecutionConfig,
    /// This machine.
    pub orchestrator: Orchestrator,
    /// The declared hosts, by name.
    pub hosts: BTreeMap<String, Host>,
    /// The declared host classes, by name.
    pub host_classes: BTreeMap<String, HostClass>,
    /// The members a run may draw on, in the order they were listed.
    pub fleet: Fleet,
    /// The spend and wall-clock ceilings over every rental in the run.
    pub budget: Budget,
    /// The store path, resolved against the config file's directory.
    pub store: PathBuf,
}

/// This machine: the worker layout a run executes on by default, and the host a
/// migration moves the run onto.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Orchestrator {
    /// The `[host.*]` entry `sima migrate` moves the run onto, or `None` for a
    /// config that names no destination.
    pub migrate: Option<String>,
    /// The container this machine's workers run in, or `None` for workers as
    /// plain subprocesses.
    pub container: Option<Container>,
    /// This machine's worker layout, or `None` for an orchestrator that
    /// executes nothing itself.
    pub pool: Option<Pool>,
}

/// One declared machine.
#[derive(Debug, Clone, PartialEq)]
pub struct Host {
    /// How the machine is obtained and what it runs.
    pub form: HostForm,
    /// Where a migrated run's directory goes on this machine.
    pub root: String,
    /// The `sima` binary that drives a migrated run on this machine.
    pub binary: String,
}

/// One declared group of identical machines.
#[derive(Debug, Clone, PartialEq)]
pub struct HostClass {
    /// How the machines are obtained and what they run.
    pub form: HostClassForm,
    /// Where a migrated run's directory goes on these machines.
    pub root: String,
    /// The `sima` binary that drives a migrated run on these machines.
    pub binary: String,
}

/// A host is a machine you have or one rented for the run. The two are
/// exclusive by construction, so nothing downstream asks which keys were given.
#[derive(Debug, Clone, PartialEq)]
pub enum HostForm {
    /// A machine of yours, reached over ssh.
    Owned(OwnedHost),
    /// A machine rented for the run.
    Rented(Rented),
}

/// A host class is a group of machines you have or a rental of several to one
/// specification.
#[derive(Debug, Clone, PartialEq)]
pub enum HostClassForm {
    /// Machines of yours, one per address.
    Owned(OwnedClass),
    /// Several machines rented to one specification.
    Rented(RentedClass),
}

/// A machine of yours: where it is reached, the container its workers run in,
/// and how many of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedHost {
    /// The ssh destination — the entry's own name unless `ssh` overrode it.
    pub ssh: String,
    /// The container this machine's workers run in.
    pub container: Container,
    /// This machine's worker layout.
    pub pool: Pool,
}

/// Machines of yours declared in one entry: one ssh destination each, sharing a
/// container and a worker layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedClass {
    /// One ssh destination per machine, derived from the entry's name and
    /// `count` unless an `ssh` list gave them.
    pub ssh: Vec<String>,
    /// The container each machine's workers run in.
    pub container: Container,
    /// Each machine's worker layout.
    pub pool: Pool,
}

/// A machine to rent: which control plane, what to ask it for, and how long to
/// wait for the result. It states no worker layout — the machine does not exist
/// until the run asks for it, so its devices come from the enumeration probe.
#[derive(Debug, Clone, PartialEq)]
pub struct Rented {
    /// The control-plane backend to acquire through.
    pub provider: ProviderId,
    /// The worker image each instance runs.
    pub image: String,
    /// The disk each instance is provisioned with, in gigabytes.
    pub disk_gb: u64,
    /// How long to wait for an instance to become reachable before giving up on
    /// it.
    pub ready_timeout: Duration,
    /// How often to poll an instance for readiness.
    pub ready_poll: Duration,
    /// The hard offer constraints that qualify a rentable machine.
    pub constraints: Constraints,
}

/// Several machines rented to one specification, and what a shortfall does.
#[derive(Debug, Clone, PartialEq)]
pub struct RentedClass {
    /// What each machine is rented as.
    pub spec: Rented,
    /// How many to acquire; at least one.
    pub count: usize,
    /// What to do when the market cannot fill the count.
    pub fill: FillPolicy,
}

/// The container a machine's workers run in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Container {
    /// The worker image to run.
    pub image: String,
    /// The container runtime: `docker` or `podman`.
    pub runtime: String,
    /// Verbatim flags for the container-run command — GPU access and the like.
    pub run_args: Vec<String>,
}

/// A machine's worker layout: a plain count, or one entry per device class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pool {
    /// A plain worker count, naming no device.
    Workers(usize),
    /// One selector per device class; the pool is their sum. The selectors stay
    /// unresolved until the run starts, over the hardware they name.
    Devices(Vec<DeviceSelector>),
}

impl Pool {
    /// How many workers the layout declares.
    pub fn workers(&self) -> usize {
        match self {
            Pool::Workers(workers) => *workers,
            Pool::Devices(devices) => devices.iter().map(|device| device.workers).sum(),
        }
    }

    /// The device selectors the layout names; empty for a plain count.
    pub fn devices(&self) -> &[DeviceSelector] {
        match self {
            Pool::Workers(_) => &[],
            Pool::Devices(devices) => devices,
        }
    }
}

/// The set of machines a run may draw on, listed by name. A collective, so it
/// never declares an element.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fleet {
    /// The hosts and host classes the run may use, in the order listed.
    pub members: Vec<String>,
}

/// Which control plane a rented machine is acquired through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderId {
    /// The Vast.ai marketplace backend.
    Vast,
    /// The in-process stub backend: scripted offers, instant readiness, and a
    /// local-spawn transport, so the rental spine is exercised without a network
    /// or real hardware. The testing path.
    Stub,
}

/// What a rented class does when it cannot acquire its full declared count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillPolicy {
    /// The full count or the run fails before any task runs, tearing down
    /// whatever was acquired.
    Strict,
    /// Run with what was acquired, at least one machine.
    BestEffort,
}

/// The raw file structure `toml` parses into. Strict on the structural keys;
/// the generator and params tables stay opaque here.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    run: RunSection,
    config: ConfigSection,
    /// The `[host.*]` entries, by name; absent means none declared.
    #[serde(default)]
    host: BTreeMap<String, MachineSection>,
    /// The `[host_class.*]` entries, by name; absent means none declared.
    #[serde(default)]
    host_class: BTreeMap<String, MachineSection>,
    /// The `[fleet]` section; absent means an empty member list.
    fleet: Option<FleetSection>,
    /// The `[budget]` section; absent means no ceiling.
    budget: Option<BudgetSection>,
    /// The `[orchestrator]` section; absent means this machine executes nothing
    /// and declares no migration destination.
    orchestrator: Option<OrchestratorSection>,
}

/// The `[run]` section: every field enters run identity.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunSection {
    /// TOML integers are i64; the load rejects negatives. Seeds above
    /// `i64::MAX` are not expressible in the file format.
    root_seed: i64,
    format: String,
    /// The number of tasks each candidate's chain comprises; validated to be at
    /// least 1. Absent means one stateless task per candidate.
    segments: Option<i64>,
    generator: GeneratorSection,
    /// Domain-owned; absent means an empty table, and the domain decides the
    /// defaults.
    #[serde(default)]
    params: toml::Table,
}

/// The `[run.generator]` section: the id names the generator, every other key
/// belongs to it and is validated by its translation.
#[derive(Deserialize)]
struct GeneratorSection {
    id: String,
    #[serde(flatten)]
    rest: toml::Table,
}

/// The `[config]` section: global operational settings, never hashed.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigSection {
    store: String,
    max_attempts: u32,
    attempt_timeout_ms: Option<u64>,
    checkpoint_interval_ms: Option<u64>,
    checkpoint_interval_steps: Option<u64>,
}

/// One `[[….device]]` entry: which device, and how many workers on it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceSection {
    select: String,
    workers: usize,
}

/// One `[host.*]` or `[host_class.*]` entry as written. Both forms' keys parse
/// here so a key belonging to the other form is rejected naming the key and the
/// form, rather than falling to the parser's unknown-key message.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MachineSection {
    ssh: Option<SshSection>,
    /// TOML integers are i64; the load rejects values below 1.
    count: Option<i64>,
    image: Option<String>,
    runtime: Option<String>,
    run_args: Option<Vec<String>>,
    workers: Option<usize>,
    #[serde(default)]
    device: Vec<DeviceSection>,
    provider: Option<String>,
    fill: Option<String>,
    disk_gb: Option<u64>,
    ready_timeout_ms: Option<u64>,
    ready_poll_ms: Option<u64>,
    constraints: Option<ConstraintsSection>,
    root: Option<String>,
    binary: Option<String>,
}

/// An `ssh` value: one destination on a host, a list of them on a class. Both
/// parse, so naming the wrong one is a validation error against the entry
/// rather than a type error against the file.
#[derive(Deserialize)]
#[serde(untagged)]
enum SshSection {
    One(String),
    Many(Vec<String>),
}

/// The `[….constraints]` table: every key optional, each mapping onto one field
/// of the provider's offer constraints. `max_price_usd_hour` is dollars,
/// converted to a micro-USD rate.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ConstraintsSection {
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

/// The `[budget]` table: both keys optional. `max_spend_usd` is dollars,
/// converted to a micro-USD cost cap rounded up.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetSection {
    max_spend_usd: Option<f64>,
    max_wall_clock_ms: Option<u64>,
}

/// The `[fleet]` section: the members a run may draw on.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FleetSection {
    #[serde(default)]
    members: Vec<String>,
}

/// The `[orchestrator]` section: this machine's worker layout and the host a
/// migration moves onto. The keys it does not take parse here so each is
/// rejected naming why, rather than falling to the parser's unknown-key
/// message.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrchestratorSection {
    migrate: Option<String>,
    image: Option<String>,
    runtime: Option<String>,
    run_args: Option<Vec<String>>,
    workers: Option<usize>,
    #[serde(default)]
    device: Vec<DeviceSection>,
    ssh: Option<toml::Value>,
    provider: Option<toml::Value>,
    root: Option<toml::Value>,
    binary: Option<toml::Value>,
}

/// Which entry a machine declaration came from: one machine, or several.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Entry {
    Host,
    Class,
}

impl Entry {
    /// The section name the entry is written under, for error messages.
    fn section(self) -> &'static str {
        match self {
            Entry::Host => "host",
            Entry::Class => "host_class",
        }
    }
}

/// Loads and translates the `sima.toml` at `path`. Parse errors, unknown or
/// missing keys, and invalid values are [`Error::Validation`] naming the file;
/// the generator and params tables are validated by the code the config names.
pub fn load(path: &Path) -> Result<LoadedConfig> {
    let text = fs_read(path)?;
    let file: FileConfig =
        toml::from_str(&text).map_err(|e| Error::Validation(format!("{}: {e}", path.display())))?;

    let run = resolve_run(path, file.run)?;
    let orchestrator = resolve_orchestrator(path, file.orchestrator)?;

    let mut hosts = BTreeMap::new();
    for (name, section) in file.host {
        let host = resolve_host(path, &name, section)?;
        hosts.insert(name, host);
    }
    let mut host_classes = BTreeMap::new();
    for (name, section) in file.host_class {
        // One name cannot mean two machines: a member naming it would have no
        // single answer, and a migration destination even less.
        if hosts.contains_key(&name) {
            return Err(Error::Validation(format!(
                "{}: {name:?} is declared as both a host and a host class; \
                 a name names one machine or one class",
                path.display()
            )));
        }
        let class = resolve_host_class(path, &name, section)?;
        host_classes.insert(name, class);
    }

    let fleet = Fleet {
        members: file.fleet.map(|fleet| fleet.members).unwrap_or_default(),
    };
    for member in &fleet.members {
        if !hosts.contains_key(member) && !host_classes.contains_key(member) {
            return Err(Error::Validation(format!(
                "{}: fleet member {member:?} names no [host.*] or [host_class.*] entry",
                path.display()
            )));
        }
    }
    // A migration moves the orchestrator onto exactly one machine, so its
    // destination is a host and never a class.
    if let Some(destination) = &orchestrator.migrate {
        if host_classes.contains_key(destination) {
            return Err(Error::Validation(format!(
                "{}: orchestrator migrate names the host class {destination:?}; \
                 a migration moves onto one machine, so it names a [host.*] entry",
                path.display()
            )));
        }
        if !hosts.contains_key(destination) {
            return Err(Error::Validation(format!(
                "{}: orchestrator migrate names {destination:?}, which no [host.*] entry declares",
                path.display()
            )));
        }
    }

    let budget = resolve_budget(path, file.budget)?;
    let execution = resolve_execution(path, &file.config, &orchestrator)?;
    // Relative to the config file's directory, never the working directory;
    // join leaves an absolute path as written.
    let base = path.parent().unwrap_or(Path::new(""));
    let store = base.join(&file.config.store);

    Ok(LoadedConfig {
        run,
        execution,
        orchestrator,
        hosts,
        host_classes,
        fleet,
        budget,
        store,
    })
}

/// Reads the config file, mapping an I/O failure onto the path that caused it.
fn fs_read(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).map_err(|e| Error::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

/// Translates the `[run]` section into the canonical [`RunConfig`] whose hash is
/// the run id, dispatching the generator and domain translations that own the
/// opaque tables.
fn resolve_run(path: &Path, section: RunSection) -> Result<RunConfig> {
    let root_seed = u64::try_from(section.root_seed).map_err(|_| {
        Error::Validation(format!(
            "{}: root_seed must be non-negative, got {}",
            path.display(),
            section.root_seed
        ))
    })?;
    let segments = section
        .segments
        .map(|value| {
            u64::try_from(value)
                .ok()
                .and_then(NonZeroU64::new)
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "{}: segments must be at least 1, got {value}",
                        path.display()
                    ))
                })
        })
        .transpose()?;
    let format = FormatId::new(section.format)?;
    let generator_id = GeneratorId::new(section.generator.id)?;
    // Identity flows through the dispatched-to code: the generator and the
    // domain turn their tables into the canonical bytes the model hashes.
    let generator_params = generator_params_for(&generator_id, &section.generator.rest)?;
    let params = params_for(&format, &section.params, segments.is_some())?;
    Ok(RunConfig {
        root_seed,
        segments,
        format,
        generator: GeneratorConfig {
            id: generator_id,
            params: generator_params,
        },
        params,
    })
}

/// Assembles the parameters the run executes under from `[config]` and the
/// orchestrator's worker layout. The orchestrator's device selectors stay
/// unresolved: they name real hardware, and loading a config must work where
/// none is present.
fn resolve_execution(
    path: &Path,
    config: &ConfigSection,
    orchestrator: &Orchestrator,
) -> Result<ExecutionConfig> {
    let attempt_timeout = config
        .attempt_timeout_ms
        .map_or(Duration::MAX, Duration::from_millis);
    let checkpoint_interval = config
        .checkpoint_interval_ms
        .map_or(Duration::MAX, Duration::from_millis);
    // The step cadence is optional and, when present, at least 1: a zero cadence
    // has no meaning (every offer, and no offer, at once), so it is a validation
    // fault naming the key.
    let checkpoint_interval_steps = config
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
    let workers = orchestrator.pool.as_ref().map_or(0, Pool::workers);
    ExecutionConfig::new(
        workers,
        config.max_attempts,
        attempt_timeout,
        checkpoint_interval,
        checkpoint_interval_steps,
    )
}

/// Validates the `[orchestrator]` section and resolves it. This machine takes an
/// owned machine's worker-side keys plus `migrate`; the keys that would describe
/// somewhere else are rejected naming why.
fn resolve_orchestrator(path: &Path, section: Option<OrchestratorSection>) -> Result<Orchestrator> {
    let Some(section) = section else {
        return Ok(Orchestrator::default());
    };
    for (key, present, reason) in [
        (
            "ssh",
            section.ssh.is_some(),
            "the orchestrator is this machine, where the command was typed",
        ),
        (
            "provider",
            section.provider.is_some(),
            "the orchestrator is this machine, which is not rented",
        ),
        ("root", section.root.is_some(), "the run already lives here"),
        (
            "binary",
            section.binary.is_some(),
            "the run is already driven by this binary",
        ),
    ] {
        if present {
            return Err(Error::Validation(format!(
                "{}: [orchestrator] sets {key:?}, which it does not take: {reason}",
                path.display()
            )));
        }
    }
    let container = orchestrator_container(
        path,
        "[orchestrator]",
        section.image,
        section.runtime,
        section.run_args,
    )?;
    let pool = resolve_pool(path, "[orchestrator]", section.workers, section.device)?;
    Ok(Orchestrator {
        migrate: section.migrate,
        container,
        pool,
    })
}

/// Validates one `[host.*]` entry and resolves it into a [`Host`], its form
/// decided by the presence of `provider`.
fn resolve_host(path: &Path, name: &str, mut section: MachineSection) -> Result<Host> {
    let subject = subject(Entry::Host, name);
    reject_cross_form(path, &subject, Entry::Host, &section)?;
    let (root, binary) = migration_paths(&mut section);
    let form = match &section.provider {
        Some(_) => HostForm::Rented(resolve_rented(path, &subject, section)?),
        None => {
            // The entry's name is its address unless `ssh` says otherwise, so a
            // machine reached at its own name needs no address at all.
            let ssh = match section.ssh {
                None => name.to_string(),
                Some(SshSection::One(ref destination)) => destination.clone(),
                Some(SshSection::Many(_)) => {
                    return Err(Error::Validation(format!(
                        "{}: {subject} sets ssh to a list; a host is one machine, \
                         so its ssh is one destination — declare a [host_class.*] for several",
                        path.display()
                    )));
                }
            };
            let container = machine_container(
                path,
                &subject,
                section.image,
                section.runtime,
                section.run_args,
            )?;
            let pool = resolve_pool(path, &subject, section.workers, section.device)?
                .ok_or_else(|| missing_pool(path, &subject))?;
            HostForm::Owned(OwnedHost {
                ssh,
                container,
                pool,
            })
        }
    };
    Ok(Host { form, root, binary })
}

/// Validates one `[host_class.*]` entry and resolves it into a [`HostClass`],
/// its form decided by the presence of `provider` and its size by `count` or the
/// length of its `ssh` list.
fn resolve_host_class(path: &Path, name: &str, mut section: MachineSection) -> Result<HostClass> {
    let subject = subject(Entry::Class, name);
    reject_cross_form(path, &subject, Entry::Class, &section)?;
    let (root, binary) = migration_paths(&mut section);
    let form = match &section.provider {
        Some(_) => {
            let count = class_count(path, &subject, section.count)?.ok_or_else(|| {
                Error::Validation(format!(
                    "{}: {subject} sets no count; a rented host class states how many machines \
                     to acquire",
                    path.display()
                ))
            })?;
            let fill = match section.fill.as_deref() {
                None | Some("strict") => FillPolicy::Strict,
                Some("best-effort") => FillPolicy::BestEffort,
                Some(other) => {
                    return Err(Error::Validation(format!(
                        "{}: {subject} fill {other:?} is not one of strict, best-effort",
                        path.display()
                    )));
                }
            };
            let spec = resolve_rented(path, &subject, section)?;
            HostClassForm::Rented(RentedClass { spec, count, fill })
        }
        None => {
            // Whichever of `count` and an `ssh` list is present *is* the count,
            // so there is never a length to keep in step.
            let ssh = match (&section.ssh, class_count(path, &subject, section.count)?) {
                (Some(SshSection::Many(_)), Some(_)) => {
                    return Err(Error::Validation(format!(
                        "{}: {subject} sets both count and an ssh list; \
                         the list is the count",
                        path.display()
                    )));
                }
                (Some(SshSection::Many(list)), None) => {
                    if list.is_empty() {
                        return Err(Error::Validation(format!(
                            "{}: {subject} sets an empty ssh list; a class is at least one machine",
                            path.display()
                        )));
                    }
                    list.clone()
                }
                (Some(SshSection::One(_)), _) => {
                    return Err(Error::Validation(format!(
                        "{}: {subject} sets ssh to one destination; a class is several machines, \
                         so its ssh is a list — declare a [host.*] for one",
                        path.display()
                    )));
                }
                // The class derives its addresses from its own name, appending
                // the index with no separator and no padding: `lab1 … lab6`.
                (None, Some(count)) => (1..=count).map(|n| format!("{name}{n}")).collect(),
                (None, None) => {
                    return Err(Error::Validation(format!(
                        "{}: {subject} sets neither count nor an ssh list; \
                         a class states how many machines it is",
                        path.display()
                    )));
                }
            };
            let container = machine_container(
                path,
                &subject,
                section.image,
                section.runtime,
                section.run_args,
            )?;
            let pool = resolve_pool(path, &subject, section.workers, section.device)?
                .ok_or_else(|| missing_pool(path, &subject))?;
            HostClassForm::Owned(OwnedClass {
                ssh,
                container,
                pool,
            })
        }
    };
    Ok(HostClass { form, root, binary })
}

/// Where a migrated run's directory goes on a machine and which `sima` drives it
/// there, defaulted. Both are host keys on either form, since any host may
/// become a migration destination, so both are read before the form is decided.
fn migration_paths(section: &mut MachineSection) -> (String, String) {
    (
        section
            .root
            .take()
            .unwrap_or_else(|| DEFAULT_ROOT.to_string()),
        section
            .binary
            .take()
            .unwrap_or_else(|| DEFAULT_BINARY.to_string()),
    )
}

/// How a machine entry is named in an error: the section it is written under and
/// its own name.
fn subject(entry: Entry, name: &str) -> String {
    format!("{} {name:?}", entry.section())
}

/// The error a machine of yours that states no worker layout raises.
fn missing_pool(path: &Path, subject: &str) -> Error {
    Error::Validation(format!(
        "{}: {subject} sets neither workers nor device tables; \
         a machine of yours states its worker layout",
        path.display()
    ))
}

/// Rejects every key belonging to the form the entry is not, naming the key and
/// the form, and every key only a class takes.
///
/// An entry is rented when it names a `provider` and yours when it does not, so
/// the presence of that one key decides which half of the schema applies.
fn reject_cross_form(
    path: &Path,
    subject: &str,
    entry: Entry,
    section: &MachineSection,
) -> Result<()> {
    let rented = section.provider.is_some();
    let owned_keys = [
        ("ssh", section.ssh.is_some()),
        ("runtime", section.runtime.is_some()),
        ("run_args", section.run_args.is_some()),
        ("workers", section.workers.is_some()),
        ("device", !section.device.is_empty()),
    ];
    let rented_keys = [
        ("fill", section.fill.is_some()),
        ("disk_gb", section.disk_gb.is_some()),
        ("ready_timeout_ms", section.ready_timeout_ms.is_some()),
        ("ready_poll_ms", section.ready_poll_ms.is_some()),
        ("constraints", section.constraints.is_some()),
    ];
    if rented {
        for (key, present) in owned_keys {
            if present {
                return Err(Error::Validation(format!(
                    "{}: {subject} names a provider, so it is rented, but sets {key:?}, \
                     which belongs to a machine of yours",
                    path.display()
                )));
            }
        }
        // `fill` decides what a shortfall does, and only a count can fall short.
        if entry == Entry::Host && section.fill.is_some() {
            return Err(Error::Validation(format!(
                "{}: {subject} sets \"fill\", which only a rented host class takes; \
                 a host is one machine, so there is no count to fall short of",
                path.display()
            )));
        }
    } else {
        for (key, present) in rented_keys {
            if present {
                return Err(Error::Validation(format!(
                    "{}: {subject} names no provider, so it is a machine of yours, \
                     but sets {key:?}, which belongs to a rented machine",
                    path.display()
                )));
            }
        }
    }
    if entry == Entry::Host && section.count.is_some() {
        return Err(Error::Validation(format!(
            "{}: {subject} sets \"count\", which only a host class takes; \
             a host is one machine",
            path.display()
        )));
    }
    Ok(())
}

/// Validates a class's `count`: absent stays absent, present must be at least
/// one.
fn class_count(path: &Path, subject: &str, count: Option<i64>) -> Result<Option<usize>> {
    count
        .map(|count| {
            usize::try_from(count)
                .ok()
                .filter(|&count| count >= 1)
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "{}: {subject} count must be at least 1, got {count}",
                        path.display()
                    ))
                })
        })
        .transpose()
}

/// The container a machine of yours runs its workers in. Its image defaults, so
/// every machine has one and the runtime and the run flags are always
/// meaningful — an entry naming none of the three still gets the default
/// container.
fn machine_container(
    path: &Path,
    subject: &str,
    image: Option<String>,
    runtime: Option<String>,
    run_args: Option<Vec<String>>,
) -> Result<Container> {
    Ok(Container {
        image: image.unwrap_or_else(|| DEFAULT_IMAGE.to_string()),
        runtime: checked_runtime(path, subject, runtime)?,
        run_args: run_args.unwrap_or_default(),
    })
}

/// The container the orchestrator runs its workers in, or `None` for workers as
/// plain subprocesses.
///
/// This machine's image does not default — the orchestrator runs bare unless it
/// is asked for a container — so the runtime and the run flags would describe a
/// container that does not exist, and each is rejected naming the key.
fn orchestrator_container(
    path: &Path,
    subject: &str,
    image: Option<String>,
    runtime: Option<String>,
    run_args: Option<Vec<String>>,
) -> Result<Option<Container>> {
    let Some(image) = image else {
        for (key, present) in [
            ("runtime", runtime.is_some()),
            ("run_args", run_args.is_some()),
        ] {
            if present {
                return Err(Error::Validation(format!(
                    "{}: {subject} sets {key:?} but no image, so it runs its workers as plain \
                     subprocesses and there is no container for {key:?} to describe",
                    path.display()
                )));
            }
        }
        return Ok(None);
    };
    Ok(Some(Container {
        image,
        runtime: checked_runtime(path, subject, runtime)?,
        run_args: run_args.unwrap_or_default(),
    }))
}

/// The container runtime an entry named, defaulted, and checked against the two
/// this build drives.
fn checked_runtime(path: &Path, subject: &str, runtime: Option<String>) -> Result<String> {
    let runtime = runtime.unwrap_or_else(|| DEFAULT_RUNTIME.to_string());
    if runtime != "docker" && runtime != "podman" {
        return Err(Error::Validation(format!(
            "{}: {subject} runtime {runtime:?} is not one of docker, podman",
            path.display()
        )));
    }
    Ok(runtime)
}

/// Resolves a worker layout from `workers` or the device tables, which are
/// exclusive: with device entries the pool is their sum, so a plain count could
/// only disagree with it. `None` means the entry stated no layout.
fn resolve_pool(
    path: &Path,
    subject: &str,
    workers: Option<usize>,
    device: Vec<DeviceSection>,
) -> Result<Option<Pool>> {
    match (workers, device.is_empty()) {
        (Some(_), false) => Err(Error::Validation(format!(
            "{}: {subject} sets both workers and device tables; \
             the device entries carry the workers",
            path.display()
        ))),
        (Some(workers), true) => Ok(Some(Pool::Workers(workers))),
        (None, false) => Ok(Some(Pool::Devices(
            device
                .into_iter()
                .map(|entry| DeviceSelector {
                    select: entry.select,
                    workers: entry.workers,
                })
                .collect(),
        ))),
        (None, true) => Ok(None),
    }
}

/// Resolves the rented keys into the specification a machine is acquired under,
/// its constraints mapped onto the provider control plane's own type.
fn resolve_rented(path: &Path, subject: &str, section: MachineSection) -> Result<Rented> {
    let name = section
        .provider
        .as_deref()
        .expect("a rented entry names a provider");
    let provider = match name {
        "vast" => ProviderId::Vast,
        "stub" => ProviderId::Stub,
        other => {
            return Err(Error::Validation(format!(
                "{}: {subject} provider {other:?} is not one of vast, stub",
                path.display()
            )));
        }
    };
    let constraints_section = section.constraints.unwrap_or_default();
    let max_price = constraints_section
        .max_price_usd_hour
        .map(|dollars| {
            finite_dollars(path, subject, "max_price_usd_hour", dollars)
                .map(|dollars| Price(dollars_to_micro_ceil(dollars)))
        })
        .transpose()?;
    Ok(Rented {
        provider,
        image: section
            .image
            .unwrap_or_else(|| DEFAULT_RENTED_IMAGE.to_string()),
        disk_gb: section.disk_gb.unwrap_or(DEFAULT_DISK_GB),
        ready_timeout: Duration::from_millis(
            section.ready_timeout_ms.unwrap_or(DEFAULT_READY_TIMEOUT_MS),
        ),
        ready_poll: Duration::from_millis(section.ready_poll_ms.unwrap_or(DEFAULT_READY_POLL_MS)),
        constraints: Constraints {
            gpu_models: constraints_section.gpu_models,
            min_gpu_count: constraints_section.min_gpu_count,
            min_vram_mb: constraints_section.min_vram_mb,
            max_price,
            min_reliability: constraints_section.min_reliability,
            verified_only: constraints_section.verified_only,
            min_disk_gb: constraints_section.min_disk_gb,
            min_bandwidth_mbps: constraints_section.min_bandwidth_mbps,
            // The excluded set is not configured: acquisition derives it from
            // the reputation ledger at each attempt.
            excluded_machines: Vec::new(),
        },
    })
}

/// Resolves the `[budget]` section into the provider control plane's own type.
/// An absent section is the permissive default.
fn resolve_budget(path: &Path, section: Option<BudgetSection>) -> Result<Budget> {
    let Some(section) = section else {
        return Ok(Budget::default());
    };
    let max_spend = section
        .max_spend_usd
        .map(|dollars| {
            finite_dollars(path, "[budget]", "max_spend_usd", dollars)
                .map(|dollars| Cost(dollars_to_micro_ceil(dollars)))
        })
        .transpose()?;
    Ok(Budget {
        max_spend,
        max_wall_clock: section.max_wall_clock_ms.map(Duration::from_millis),
    })
}

/// Converts a dollar amount to micro-USD, rounding up so a cap or rate is never
/// rendered stricter than the figure written. The value must be validated finite
/// and non-negative first.
fn dollars_to_micro_ceil(dollars: f64) -> u64 {
    (dollars * 1_000_000.0).ceil() as u64
}

/// Validates that a dollar figure is finite and non-negative, naming the entry
/// and `key` on failure.
fn finite_dollars(path: &Path, subject: &str, key: &str, value: f64) -> Result<f64> {
    if !value.is_finite() || value < 0.0 {
        return Err(Error::Validation(format!(
            "{}: {subject} {key} must be finite and non-negative, got {value}",
            path.display()
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sima_domains::{StubBehavior, StubGeneratorConfig};
    use sima_model::RunId;

    use super::*;

    /// Writes `text` as a config file named `name` under `dir`.
    fn write_config(dir: &Path, name: &str, text: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, text).expect("write config file");
        path
    }

    /// The reference schema instance from the module doc: a run driven on this
    /// machine alone.
    const BASE: &str = r#"
        [run]
        root_seed = 42
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed", "flaky:2", "sleep:50", "reject", "panic"]

        [run.params]
        hex = "00ff"

        [config]
        store = "./store"
        max_attempts = 3
        attempt_timeout_ms = 5000

        [orchestrator]
        workers = 4
    "#;

    /// The reference schema with an orchestrator that executes nothing, for the
    /// configs whose machines carry the run.
    const NO_POOL: &str = r#"
        [run]
        root_seed = 42
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed"]

        [config]
        store = "./store"
        max_attempts = 3
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

    /// The validation message `text` is rejected with.
    fn rejection(text: &str) -> String {
        match load_text(text) {
            Err(Error::Validation(message)) => message,
            other => panic!("expected a validation error, got {other:?}"),
        }
    }

    /// The host `name` loads to.
    fn host_of(text: &str, name: &str) -> Host {
        load_text(text)
            .expect("config loads")
            .hosts
            .remove(name)
            .expect("the host is declared")
    }

    /// The host class `name` loads to.
    fn class_of(text: &str, name: &str) -> HostClass {
        load_text(text)
            .expect("config loads")
            .host_classes
            .remove(name)
            .expect("the class is declared")
    }

    /// The owned form of `host`, or a panic naming what it was instead.
    fn owned(host: &Host) -> &OwnedHost {
        match &host.form {
            HostForm::Owned(owned) => owned,
            HostForm::Rented(_) => panic!("expected a machine of yours"),
        }
    }

    /// The owned form of `class`, or a panic naming what it was instead.
    fn owned_class(class: &HostClass) -> &OwnedClass {
        match &class.form {
            HostClassForm::Owned(owned) => owned,
            HostClassForm::Rented(_) => panic!("expected machines of yours"),
        }
    }

    // ---- The identity and global sections, unchanged by the machine model ----

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
        // Every [run] field whose variation still names dispatchable ids: the
        // format and generator ids admit one value in this build, and the
        // model's own tests pin that they enter the id. The remaining fields
        // flow through translation, which is what this pins.
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
    fn operational_values_never_touch_the_run_id() {
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
    fn segments_loads_into_the_run_config() -> Result<()> {
        let text = BASE.replace("root_seed = 42", "root_seed = 42\nsegments = 10");
        assert_eq!(load_text(&text)?.run.segments, NonZeroU64::new(10));
        assert_eq!(load_text(BASE)?.run.segments, None);
        Ok(())
    }

    #[test]
    fn zero_or_negative_segments_are_rejected_naming_the_field() {
        for value in ["segments = 0", "segments = -1"] {
            let text = BASE.replace("root_seed = 42", &format!("root_seed = 42\n{value}"));
            assert!(rejection(&text).contains("segments"), "{value}");
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
    fn the_two_checkpoint_cadences_load_and_default_to_disabled() -> Result<()> {
        let loaded = load_text(BASE)?;
        assert_eq!(loaded.execution.checkpoint_interval, Duration::MAX);
        assert_eq!(loaded.execution.checkpoint_interval_steps, None);
        let text = BASE.replace(
            "attempt_timeout_ms = 5000",
            "attempt_timeout_ms = 5000\n\
             checkpoint_interval_ms = 30000\n\
             checkpoint_interval_steps = 100",
        );
        let loaded = load_text(&text)?;
        assert_eq!(
            loaded.execution.checkpoint_interval,
            Duration::from_millis(30000)
        );
        assert_eq!(
            loaded.execution.checkpoint_interval_steps,
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
        assert!(rejection(&text).contains("checkpoint_interval_steps"));
    }

    #[test]
    fn neither_cadence_touches_the_run_id() {
        let base = id_of(BASE);
        for addition in [
            "checkpoint_interval_ms = 1",
            "checkpoint_interval_steps = 7",
        ] {
            let text = BASE.replace(
                "attempt_timeout_ms = 5000",
                &format!("attempt_timeout_ms = 5000\n{addition}"),
            );
            assert_eq!(base, id_of(&text), "{addition}");
        }
    }

    #[test]
    fn an_absent_attempt_timeout_disables_the_deadline() -> Result<()> {
        let text = BASE.replace("attempt_timeout_ms = 5000", "");
        assert_eq!(load_text(&text)?.execution.attempt_timeout, Duration::MAX);
        Ok(())
    }

    #[test]
    fn a_negative_root_seed_is_rejected() {
        let text = BASE.replace("root_seed = 42", "root_seed = -1");
        assert!(rejection(&text).contains("root_seed"));
    }

    #[test]
    fn unknown_keys_are_rejected_at_every_level() {
        for (section, addition) in [
            ("top level", "surprise = 1\n"),
            ("[run]", "[run]\nsurprise = 1\n"),
            ("[config]", "[config]\nsurprise = 1\n"),
            ("[run.params]", "[run.params]\nsurprise = 1\n"),
            ("[run.generator]", "[run.generator]\nsurprise = 1\n"),
            ("[orchestrator]", "[orchestrator]\nsurprise = 1\n"),
            ("[fleet]", "[fleet]\nsurprise = 1\n"),
            ("[budget]", "[budget]\nsurprise = 1\n"),
            ("[host.*]", "[host.gpubox]\nworkers = 1\nsurprise = 1\n"),
            (
                "[host_class.*]",
                "[host_class.lab]\ncount = 2\nworkers = 1\nsurprise = 1\n",
            ),
            (
                "a device table",
                "[[orchestrator.device]]\nselect = \"nvidia\"\nworkers = 1\nmember = 1\n",
            ),
            (
                "a constraints table",
                "[host.x]\nprovider = \"stub\"\n[host.x.constraints]\nregion = \"eu\"\n",
            ),
        ] {
            // Appending re-opens the named table; TOML allows adding keys to a
            // table from a later header only when they do not collide.
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
    fn a_syntax_error_is_validation_naming_the_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(dir.path(), "broken.toml", "run = [not toml");
        match load(&path) {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains("broken.toml"), "names the file: {msg}");
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

    // ---- The orchestrator ----

    #[test]
    fn an_orchestrator_with_a_plain_count_declares_no_container() -> Result<()> {
        let loaded = load_text(BASE)?;
        assert_eq!(loaded.orchestrator.pool, Some(Pool::Workers(4)));
        assert_eq!(loaded.orchestrator.container, None, "plain subprocesses");
        assert_eq!(loaded.orchestrator.migrate, None);
        Ok(())
    }

    #[test]
    fn an_absent_orchestrator_executes_nothing() -> Result<()> {
        let loaded = load_text(NO_POOL)?;
        assert_eq!(loaded.orchestrator, Orchestrator::default());
        assert_eq!(loaded.execution.workers, 0);
        Ok(())
    }

    #[test]
    fn orchestrator_device_tables_load_as_unresolved_selectors() -> Result<()> {
        let loaded = load_text(&format!(
            r#"{NO_POOL}
            [orchestrator]

            [[orchestrator.device]]
            select = "nvidia"
            workers = 3

            [[orchestrator.device]]
            select = "8086:7d67"
            workers = 1
            "#
        ))?;
        assert_eq!(
            loaded.orchestrator.pool,
            Some(Pool::Devices(vec![
                DeviceSelector {
                    select: "nvidia".to_string(),
                    workers: 3,
                },
                DeviceSelector {
                    select: "8086:7d67".to_string(),
                    workers: 1,
                },
            ]))
        );
        // The pool is the entries' sum; the classes resolve at run start, so the
        // loaded settings name no device yet.
        assert_eq!(loaded.execution.workers, 4);
        assert!(loaded.execution.devices.is_empty());
        Ok(())
    }

    #[test]
    fn an_orchestrator_naming_an_image_runs_its_workers_in_a_container() -> Result<()> {
        let loaded = load_text(&format!(
            r#"{NO_POOL}
            [orchestrator]
            workers = 2
            image = "localhost/sima:pinned"
            runtime = "podman"
            run_args = ["--device", "/dev/dri"]
            "#
        ))?;
        assert_eq!(
            loaded.orchestrator.container,
            Some(Container {
                image: "localhost/sima:pinned".to_string(),
                runtime: "podman".to_string(),
                run_args: vec!["--device".to_string(), "/dev/dri".to_string()],
            })
        );
        Ok(())
    }

    #[test]
    fn an_orchestrator_image_defaults_its_runtime_and_run_flags() -> Result<()> {
        let loaded = load_text(&format!(
            "{NO_POOL}\n[orchestrator]\nworkers = 2\nimage = \"img\"\n"
        ))?;
        let container = loaded.orchestrator.container.expect("a container");
        assert_eq!(container.runtime, "docker");
        assert!(container.run_args.is_empty());
        Ok(())
    }

    #[test]
    fn orchestrator_container_keys_without_an_image_are_rejected_naming_the_key() {
        // This machine runs bare unless it is asked for a container, so a
        // runtime or a run flag here describes a container that does not exist.
        for key in ["runtime = \"podman\"", "run_args = [\"--gpus\", \"all\"]"] {
            let text = format!("{NO_POOL}\n[orchestrator]\nworkers = 2\n{key}\n");
            let message = rejection(&text);
            let name = key.split(' ').next().expect("the key name");
            assert!(message.contains(name), "names the key: {message}");
            assert!(message.contains("image"), "names the image: {message}");
        }
    }

    #[test]
    fn a_machines_container_keys_stand_without_an_image() -> Result<()> {
        // The other side of the asymmetry: a machine of yours always runs a
        // container, its image defaulting, so the runtime and the run flags are
        // meaningful whether or not the entry names one.
        let text = format!(
            "{BASE}\n[host.gpubox]\nworkers = 4\nruntime = \"podman\"\n\
             run_args = [\"--gpus\", \"all\"]\n"
        );
        let host = host_of(&text, "gpubox");
        let owned = owned(&host);
        assert_eq!(owned.container.image, "localhost/sima:latest");
        assert_eq!(owned.container.runtime, "podman");
        assert_eq!(owned.container.run_args, vec!["--gpus", "all"]);
        Ok(())
    }

    #[test]
    fn an_unknown_container_runtime_is_rejected_naming_it() {
        let text = format!(
            "{NO_POOL}\n[orchestrator]\nworkers = 2\nimage = \"img\"\nruntime = \"containerd\"\n"
        );
        assert!(rejection(&text).contains("containerd"));
    }

    #[test]
    fn workers_and_device_tables_may_not_both_be_set() {
        let text = format!(
            r#"{BASE}
            [[orchestrator.device]]
            select = "nvidia"
            workers = 3
            "#
        );
        let message = rejection(&text);
        assert!(message.contains("workers"), "{message}");
        assert!(message.contains("device"), "{message}");
    }

    #[test]
    fn the_orchestrator_takes_no_key_that_names_somewhere_else() {
        for key in [
            "ssh = \"gpubox\"",
            "provider = \"stub\"",
            "root = \"~/elsewhere\"",
            "binary = \"/usr/bin/sima\"",
        ] {
            let text = format!("{NO_POOL}\n[orchestrator]\nworkers = 1\n{key}\n");
            let message = rejection(&text);
            let name = key.split(' ').next().expect("the key name");
            assert!(message.contains(name), "names the key: {message}");
            assert!(
                message.contains("orchestrator"),
                "names the section: {message}"
            );
        }
    }

    // ---- Addressing ----

    #[test]
    fn a_host_is_reached_at_its_own_name() {
        let text = format!("{BASE}\n[host.gpubox]\nworkers = 4\n");
        assert_eq!(owned(&host_of(&text, "gpubox")).ssh, "gpubox");
    }

    #[test]
    fn an_ssh_key_overrides_a_hosts_address() {
        let text = format!("{BASE}\n[host.bigbox]\nssh = \"bigbox.dept.internal\"\nworkers = 8\n");
        assert_eq!(owned(&host_of(&text, "bigbox")).ssh, "bigbox.dept.internal");
    }

    #[test]
    fn a_class_derives_its_addresses_from_its_name_and_count() {
        let text = format!("{BASE}\n[host_class.lab]\ncount = 6\nworkers = 8\n");
        // Unseparated and unpadded, so a class of six and one of two hundred
        // read the same way and nothing breaks at a power of ten.
        assert_eq!(
            owned_class(&class_of(&text, "lab")).ssh,
            ["lab1", "lab2", "lab3", "lab4", "lab5", "lab6"]
        );
    }

    #[test]
    fn a_class_of_ten_pads_nothing() {
        let text = format!("{BASE}\n[host_class.lab]\ncount = 10\nworkers = 1\n");
        let class = class_of(&text, "lab");
        let ssh = &owned_class(&class).ssh;
        assert_eq!(ssh.len(), 10);
        assert_eq!(ssh[8], "lab9");
        assert_eq!(ssh[9], "lab10");
    }

    #[test]
    fn a_class_takes_addresses_that_follow_no_pattern() {
        let text = format!(
            "{BASE}\n[host_class.oldlab]\nssh = [\"fermi\", \"pauli\", \"dirac\"]\nworkers = 4\n"
        );
        assert_eq!(
            owned_class(&class_of(&text, "oldlab")).ssh,
            ["fermi", "pauli", "dirac"]
        );
    }

    #[test]
    fn a_class_with_an_ssh_list_rejects_a_count() {
        let text = format!(
            "{BASE}\n[host_class.oldlab]\nssh = [\"fermi\", \"pauli\"]\ncount = 2\nworkers = 4\n"
        );
        let message = rejection(&text);
        assert!(message.contains("count"), "{message}");
        assert!(message.contains("the list is the count"), "{message}");
    }

    #[test]
    fn an_empty_ssh_list_is_rejected() {
        let text = format!("{BASE}\n[host_class.oldlab]\nssh = []\nworkers = 4\n");
        assert!(rejection(&text).contains("empty ssh list"));
    }

    #[test]
    fn a_class_with_neither_count_nor_an_ssh_list_is_rejected() {
        let text = format!("{BASE}\n[host_class.lab]\nworkers = 4\n");
        let message = rejection(&text);
        assert!(message.contains("count"), "{message}");
    }

    #[test]
    fn a_host_rejects_an_ssh_list_and_a_class_rejects_a_lone_destination() {
        let host = rejection(&format!(
            "{BASE}\n[host.gpubox]\nssh = [\"a\", \"b\"]\nworkers = 1\n"
        ));
        assert!(host.contains("host_class"), "points at a class: {host}");
        let class = rejection(&format!(
            "{BASE}\n[host_class.lab]\nssh = \"a\"\nworkers = 1\n"
        ));
        assert!(class.contains("list"), "asks for a list: {class}");
    }

    #[test]
    fn a_count_below_one_is_rejected() {
        for value in ["count = 0", "count = -1"] {
            let text = format!("{BASE}\n[host_class.lab]\n{value}\nworkers = 4\n");
            assert!(rejection(&text).contains("count"), "{value}");
        }
    }

    #[test]
    fn a_count_on_a_host_is_rejected_naming_the_entry_it_belongs_to() {
        let text = format!("{BASE}\n[host.gpubox]\ncount = 2\nworkers = 4\n");
        let message = rejection(&text);
        assert!(message.contains("count"), "{message}");
        assert!(message.contains("host class"), "{message}");
    }

    // ---- The two forms ----

    #[test]
    fn a_host_of_yours_defaults_its_image_and_runtime() {
        let text = format!("{BASE}\n[host.gpubox]\nworkers = 4\n");
        let host = host_of(&text, "gpubox");
        let owned = owned(&host);
        assert_eq!(owned.container.image, "localhost/sima:latest");
        assert_eq!(owned.container.runtime, "docker");
        assert!(owned.container.run_args.is_empty());
        assert_eq!(owned.pool, Pool::Workers(4));
        assert_eq!(host.root, "~/sima-runs");
        assert_eq!(host.binary, "sima");
    }

    #[test]
    fn a_host_of_yours_takes_device_tables() {
        let text = format!(
            r#"{BASE}
            [host.gpubox]
            [[host.gpubox.device]]
            select = "nvidia"
            workers = 2
            "#
        );
        assert_eq!(
            owned(&host_of(&text, "gpubox")).pool,
            Pool::Devices(vec![DeviceSelector {
                select: "nvidia".to_string(),
                workers: 2,
            }])
        );
    }

    #[test]
    fn a_rented_host_resolves_its_specification_with_defaults() -> Result<()> {
        let text = format!("{BASE}\n[host.slingshot]\nprovider = \"vast\"\n");
        let host = host_of(&text, "slingshot");
        let HostForm::Rented(rented) = &host.form else {
            panic!("expected a rented machine");
        };
        assert_eq!(rented.provider, ProviderId::Vast);
        assert_eq!(rented.image, "ghcr.io/alvatar/sima-worker:latest");
        assert_eq!(rented.disk_gb, 32);
        assert_eq!(rented.ready_timeout, Duration::from_millis(600_000));
        assert_eq!(rented.ready_poll, Duration::from_millis(5_000));
        assert!(rented.constraints.gpu_models.is_empty());
        assert_eq!(rented.constraints.max_price, None);
        assert!(!rented.constraints.verified_only);
        Ok(())
    }

    #[test]
    fn a_rented_host_resolves_every_constraint_it_names() -> Result<()> {
        let loaded = load_text(&format!(
            r#"{BASE}
            [host.slingshot]
            provider = "vast"
            disk_gb = 64
            image = "ghcr.io/example/worker:pinned"
            ready_timeout_ms = 120000
            ready_poll_ms = 2000

            [host.slingshot.constraints]
            gpu_models = ["RTX 4090"]
            min_gpu_count = 1
            min_vram_mb = 16000
            max_price_usd_hour = 0.5
            min_reliability = 0.95
            verified_only = true
            min_disk_gb = 32
            min_bandwidth_mbps = 100
            "#
        ))?;
        let HostForm::Rented(rented) = &loaded.hosts["slingshot"].form else {
            panic!("expected a rented machine");
        };
        assert_eq!(rented.image, "ghcr.io/example/worker:pinned");
        assert_eq!(rented.disk_gb, 64);
        assert_eq!(rented.ready_timeout, Duration::from_millis(120_000));
        assert_eq!(rented.ready_poll, Duration::from_millis(2_000));
        assert_eq!(rented.constraints.gpu_models, vec!["RTX 4090".to_string()]);
        assert_eq!(rented.constraints.min_gpu_count, Some(1));
        assert_eq!(rented.constraints.min_vram_mb, Some(16000));
        // The dollar rate converts to a micro-USD price.
        assert_eq!(rented.constraints.max_price, Some(Price(500_000)));
        assert_eq!(rented.constraints.min_reliability, Some(0.95));
        assert!(rented.constraints.verified_only);
        assert_eq!(rented.constraints.min_disk_gb, Some(32));
        assert_eq!(rented.constraints.min_bandwidth_mbps, Some(100));
        Ok(())
    }

    #[test]
    fn a_rented_class_carries_its_count_and_fill() -> Result<()> {
        let text = format!(
            "{BASE}\n[host_class.rtx4090]\nprovider = \"vast\"\ncount = 4\nfill = \"best-effort\"\n"
        );
        let HostClassForm::Rented(rented) = &class_of(&text, "rtx4090").form else {
            panic!("expected rented machines");
        };
        assert_eq!(rented.count, 4);
        assert_eq!(rented.fill, FillPolicy::BestEffort);
        assert_eq!(rented.spec.provider, ProviderId::Vast);
        // An absent fill is strict: the declared count or nothing.
        let strict = format!("{BASE}\n[host_class.rtx4090]\nprovider = \"stub\"\ncount = 2\n");
        let HostClassForm::Rented(rented) = &class_of(&strict, "rtx4090").form else {
            panic!("expected rented machines");
        };
        assert_eq!(rented.fill, FillPolicy::Strict);
        Ok(())
    }

    #[test]
    fn a_rented_class_without_a_count_is_rejected() {
        let text = format!("{BASE}\n[host_class.rtx4090]\nprovider = \"stub\"\n");
        assert!(rejection(&text).contains("count"));
    }

    #[test]
    fn an_unknown_provider_is_rejected_naming_it() {
        let text = format!("{BASE}\n[host.slingshot]\nprovider = \"aws\"\n");
        assert!(rejection(&text).contains("aws"));
    }

    #[test]
    fn an_unknown_fill_is_rejected_naming_it() {
        let text =
            format!("{BASE}\n[host_class.r]\nprovider = \"stub\"\ncount = 2\nfill = \"eager\"\n");
        assert!(rejection(&text).contains("eager"));
    }

    #[test]
    fn a_rented_entry_rejects_every_key_belonging_to_a_machine_of_yours() {
        for key in [
            "ssh = \"gpubox\"",
            "runtime = \"podman\"",
            "run_args = [\"--gpus\", \"all\"]",
            "workers = 4",
        ] {
            let text = format!("{BASE}\n[host.slingshot]\nprovider = \"stub\"\n{key}\n");
            let message = rejection(&text);
            let name = key.split(' ').next().expect("the key name");
            assert!(message.contains(name), "names the key: {message}");
            assert!(message.contains("rented"), "names the form: {message}");
        }
        // A device table is the same rejection, written as its own table.
        let text = format!(
            "{BASE}\n[host.slingshot]\nprovider = \"stub\"\n\
             [[host.slingshot.device]]\nselect = \"nvidia\"\nworkers = 1\n"
        );
        let message = rejection(&text);
        assert!(message.contains("device"), "names the key: {message}");
        assert!(message.contains("rented"), "names the form: {message}");
    }

    #[test]
    fn a_machine_of_yours_rejects_every_key_belonging_to_a_rented_one() {
        for key in [
            "fill = \"strict\"",
            "disk_gb = 64",
            "ready_timeout_ms = 1000",
            "ready_poll_ms = 100",
        ] {
            let text = format!("{BASE}\n[host.gpubox]\nworkers = 4\n{key}\n");
            let message = rejection(&text);
            let name = key.split(' ').next().expect("the key name");
            assert!(message.contains(name), "names the key: {message}");
            assert!(
                message.contains("machine of yours"),
                "names the form: {message}"
            );
        }
        // A constraints table is the same rejection, written as its own table.
        let text = format!(
            "{BASE}\n[host.gpubox]\nworkers = 4\n\
             [host.gpubox.constraints]\nmin_vram_mb = 16000\n"
        );
        let message = rejection(&text);
        assert!(message.contains("constraints"), "names the key: {message}");
        assert!(
            message.contains("machine of yours"),
            "names the form: {message}"
        );
    }

    #[test]
    fn fill_on_a_rented_host_is_rejected_as_a_class_key() {
        let text = format!("{BASE}\n[host.slingshot]\nprovider = \"stub\"\nfill = \"strict\"\n");
        let message = rejection(&text);
        assert!(message.contains("fill"), "{message}");
        assert!(message.contains("class"), "{message}");
    }

    #[test]
    fn a_machine_of_yours_stating_no_worker_layout_is_rejected() {
        for entry in ["[host.gpubox]", "[host_class.lab]\ncount = 2"] {
            let text = format!("{BASE}\n{entry}\n");
            let message = rejection(&text);
            assert!(message.contains("workers"), "{message}");
            assert!(message.contains("device"), "{message}");
        }
    }

    #[test]
    fn non_finite_or_negative_money_is_rejected_naming_the_key() {
        for value in ["-0.5", "nan", "inf"] {
            let text = format!(
                "{BASE}\n[host.slingshot]\nprovider = \"stub\"\n\
                 [host.slingshot.constraints]\nmax_price_usd_hour = {value}\n"
            );
            assert!(rejection(&text).contains("max_price_usd_hour"), "{value}");
        }
        for value in ["-1.0", "nan"] {
            let text = format!("{BASE}\n[budget]\nmax_spend_usd = {value}\n");
            assert!(rejection(&text).contains("max_spend_usd"), "{value}");
        }
    }

    // ---- The fleet, the budget, and cross-entry rules ----

    #[test]
    fn the_fleet_lists_the_members_a_run_may_draw_on() -> Result<()> {
        let loaded = load_text(&format!(
            r#"{BASE}
            [host.gpubox]
            workers = 4

            [host_class.lab]
            count = 2
            workers = 1

            [fleet]
            members = ["lab", "gpubox"]
            "#
        ))?;
        // In the order listed, which is the order the run engages them in.
        assert_eq!(loaded.fleet.members, ["lab", "gpubox"]);
        Ok(())
    }

    #[test]
    fn a_member_naming_nothing_declared_is_rejected() {
        let text = format!("{BASE}\n[fleet]\nmembers = [\"gpubox\"]\n");
        let message = rejection(&text);
        assert!(message.contains("gpubox"), "{message}");
        assert!(message.contains("host"), "{message}");
    }

    #[test]
    fn a_declared_machine_no_fleet_names_loads_and_is_unused() -> Result<()> {
        // A machine you have written down, which is the point of naming them.
        let loaded = load_text(&format!("{BASE}\n[host.gpubox]\nworkers = 4\n"))?;
        assert!(loaded.hosts.contains_key("gpubox"));
        assert!(loaded.fleet.members.is_empty());
        Ok(())
    }

    #[test]
    fn one_name_declared_as_both_a_host_and_a_class_is_rejected() {
        let text =
            format!("{BASE}\n[host.lab]\nworkers = 1\n[host_class.lab]\ncount = 2\nworkers = 1\n");
        let message = rejection(&text);
        assert!(message.contains("lab"), "{message}");
        assert!(message.contains("both"), "{message}");
    }

    #[test]
    fn the_budget_resolves_to_the_provider_ceiling_types() -> Result<()> {
        let loaded = load_text(&format!(
            "{BASE}\n[budget]\nmax_spend_usd = 20.0\nmax_wall_clock_ms = 21600000\n"
        ))?;
        assert_eq!(loaded.budget.max_spend, Some(Cost(20_000_000)));
        assert_eq!(
            loaded.budget.max_wall_clock,
            Some(Duration::from_millis(21_600_000))
        );
        // Absent, the ceiling is permissive.
        assert_eq!(load_text(BASE)?.budget, Budget::default());
        Ok(())
    }

    #[test]
    fn a_cost_cap_rounds_up() -> Result<()> {
        // A fractional-micro dollar cap rounds up so the cap is never rendered
        // stricter than written.
        let loaded = load_text(&format!("{BASE}\n[budget]\nmax_spend_usd = 1.2345678\n"))?;
        assert_eq!(loaded.budget.max_spend, Some(Cost(1_234_568)));
        Ok(())
    }

    // ---- Migration destinations ----

    /// The reference config whose orchestrator migrates onto `destination`,
    /// with `machines` declared after it.
    fn migrating(destination: &str, machines: &str) -> String {
        format!(
            "{}\n{machines}",
            BASE.replace(
                "workers = 4",
                &format!("workers = 4\n        migrate = {destination:?}"),
            )
        )
    }

    #[test]
    fn migrate_names_a_declared_host() -> Result<()> {
        let loaded = load_text(&migrating(
            "slingshot",
            "[host.slingshot]\nprovider = \"stub\"\n",
        ))?;
        assert_eq!(loaded.orchestrator.migrate.as_deref(), Some("slingshot"));
        Ok(())
    }

    #[test]
    fn migrate_naming_a_class_is_rejected() {
        let message = rejection(&migrating(
            "lab",
            "[host_class.lab]\ncount = 2\nworkers = 1\n",
        ));
        assert!(message.contains("lab"), "{message}");
        assert!(message.contains("one machine"), "{message}");
    }

    #[test]
    fn migrate_naming_nothing_declared_is_rejected() {
        let message = rejection(&migrating("slingshot", ""));
        assert!(message.contains("slingshot"), "{message}");
        assert!(message.contains("host"), "{message}");
    }

    // ---- The machine model never enters run identity ----

    #[test]
    fn declaring_machines_never_changes_the_run_id() {
        let base = id_of(BASE);
        let declared = id_of(&format!(
            r#"{BASE}
            [host.gpubox]
            workers = 4

            [host_class.rtx4090]
            provider = "stub"
            count = 4

            [fleet]
            members = ["gpubox", "rtx4090"]

            [budget]
            max_spend_usd = 20.0
            "#
        ));
        assert_eq!(base, declared, "machines decide where, never what");
    }
}
