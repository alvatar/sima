//! The `sima.toml` schema as serde reads it, and the read that produces it.
//!
//! One struct per section, each `deny_unknown_fields`, so a key the schema does
//! not declare is refused where it is written rather than ignored. These types
//! are the file's shape and nothing more — every default, every cross-key rule,
//! and every resolution against the file's own directory belongs to the modules
//! that translate them.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;
use sima_core::{Error, Result};

/// The raw file structure `toml` parses into. Strict on the structural keys;
/// the generator and params tables stay opaque here.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileConfig {
    pub(super) run: RunSection,
    pub(super) config: ConfigSection,
    /// The `[host.*]` entries, by name; absent means none declared.
    #[serde(default)]
    pub(super) host: BTreeMap<String, MachineSection>,
    /// The `[host_class.*]` entries, by name; absent means none declared.
    #[serde(default)]
    pub(super) host_class: BTreeMap<String, MachineSection>,
    /// The `[fleet]` section; absent means an empty member list.
    pub(super) fleet: Option<FleetSection>,
    /// The `[budget]` section; absent means no ceiling.
    pub(super) budget: Option<BudgetSection>,
    /// The `[orchestrator]` section; absent means this machine executes nothing
    /// and declares no migration destination.
    pub(super) orchestrator: Option<OrchestratorSection>,
    /// The `[domain.*]` entries, by format id; absent means every format this
    /// run names is answered by this build.
    #[serde(default)]
    pub(super) domain: BTreeMap<String, DomainSection>,
}

/// One `[domain.*]` entry: the program that answers for the format the entry is
/// named after.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DomainSection {
    /// The binary sima spawns, resolved against the config file's directory;
    /// an absolute path is taken as written.
    pub(super) binary: String,
    /// Environment variable names the program receives beyond the baseline
    /// every spawned program gets. Absent means the baseline alone.
    pub(super) env: Option<Vec<String>>,
    /// What travels when this run migrates: one file or one directory,
    /// resolved against the config file's directory. Absent means the program
    /// is this machine's alone.
    pub(super) payload: Option<String>,
    /// The shell script the destination runs to turn the payload into the
    /// program it spawns; optional for a single-file payload, required for a
    /// directory.
    pub(super) install: Option<String>,
    /// The payload manifest this config's store already holds, written by a
    /// migration when it synthesizes the far config. The destination
    /// materializes and installs it at load.
    pub(super) payload_digest: Option<String>,
}

/// The `[run]` section: every field enters run identity.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RunSection {
    /// TOML integers are i64; the load rejects negatives. Seeds above
    /// `i64::MAX` are not expressible in the file format.
    pub(super) root_seed: i64,
    pub(super) format: String,
    /// The number of tasks each candidate's chain comprises; validated to be at
    /// least 1. Absent means one stateless task per candidate.
    pub(super) segments: Option<i64>,
    pub(super) generator: GeneratorSection,
    /// Domain-owned; absent means an empty table, and the domain decides the
    /// defaults.
    #[serde(default)]
    pub(super) params: toml::Table,
}

/// The `[run.generator]` section: the id names the generator, every other key
/// belongs to it and is validated by its translation.
#[derive(Deserialize)]
pub(super) struct GeneratorSection {
    pub(super) id: String,
    #[serde(flatten)]
    pub(super) rest: toml::Table,
}

/// The `[config]` section: global operational settings, never hashed.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConfigSection {
    pub(super) store: String,
    pub(super) max_attempts: u32,
    pub(super) attempt_timeout_ms: Option<u64>,
    pub(super) answer_timeout_ms: Option<u64>,
    pub(super) checkpoint_interval_ms: Option<u64>,
    pub(super) checkpoint_interval_steps: Option<u64>,
}

/// One `[[….device]]` entry: which device, and how many workers on it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeviceSection {
    pub(super) select: String,
    pub(super) workers: usize,
}

/// One `[host.*]` or `[host_class.*]` entry as written. Both forms' keys parse
/// here so a key belonging to the other form is rejected naming the key and the
/// form, rather than falling to the parser's unknown-key message.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MachineSection {
    pub(super) ssh: Option<SshSection>,
    /// TOML integers are i64; the load rejects values below 1.
    pub(super) count: Option<i64>,
    pub(super) image: Option<String>,
    pub(super) runtime: Option<String>,
    pub(super) run_args: Option<Vec<String>>,
    pub(super) workers: Option<usize>,
    #[serde(default)]
    pub(super) device: Vec<DeviceSection>,
    pub(super) provider: Option<String>,
    pub(super) fill: Option<String>,
    pub(super) disk_gb: Option<u64>,
    pub(super) ready_timeout_ms: Option<u64>,
    pub(super) ready_poll_ms: Option<u64>,
    pub(super) constraints: Option<ConstraintsSection>,
    pub(super) root: Option<String>,
    pub(super) binary: Option<String>,
}

/// An `ssh` value: one destination on a host, a list of them on a class. Both
/// parse, so naming the wrong one is a validation error against the entry
/// rather than a type error against the file.
#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum SshSection {
    One(String),
    Many(Vec<String>),
}

/// The `[….constraints]` table: every key optional, each mapping onto one field
/// of the provider's offer constraints. `max_price_usd_hour` is dollars,
/// converted to a micro-USD rate.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct ConstraintsSection {
    #[serde(default)]
    pub(super) gpu_models: Vec<String>,
    pub(super) min_gpu_count: Option<u32>,
    pub(super) min_vram_mb: Option<u64>,
    pub(super) max_price_usd_hour: Option<f64>,
    pub(super) min_reliability: Option<f64>,
    #[serde(default)]
    pub(super) verified_only: bool,
    pub(super) min_disk_gb: Option<u64>,
    pub(super) min_bandwidth_mbps: Option<u64>,
}

/// The `[budget]` table: both keys optional. `max_spend_usd` is dollars,
/// converted to a micro-USD cost cap rounded up.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BudgetSection {
    pub(super) max_spend_usd: Option<f64>,
    pub(super) max_wall_clock_ms: Option<u64>,
}

/// The `[fleet]` section: the members a run may draw on.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FleetSection {
    #[serde(default)]
    pub(super) members: Vec<String>,
}

/// The `[orchestrator]` section: this machine's worker layout and the host a
/// migration moves onto. The keys it does not take parse here so each is
/// rejected naming why, rather than falling to the parser's unknown-key
/// message.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OrchestratorSection {
    pub(super) migrate: Option<String>,
    pub(super) image: Option<String>,
    pub(super) runtime: Option<String>,
    pub(super) run_args: Option<Vec<String>>,
    pub(super) workers: Option<usize>,
    #[serde(default)]
    pub(super) device: Vec<DeviceSection>,
    pub(super) ssh: Option<toml::Value>,
    pub(super) provider: Option<toml::Value>,
    pub(super) root: Option<toml::Value>,
    pub(super) binary: Option<toml::Value>,
}

/// Which entry a machine declaration came from: one machine, or several.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Entry {
    Host,
    Class,
}

impl Entry {
    /// The section name the entry is written under, for error messages.
    pub(super) fn section(self) -> &'static str {
        match self {
            Entry::Host => "host",
            Entry::Class => "host_class",
        }
    }
}

/// Reads the config file, mapping an I/O failure onto the path that caused it.
pub(super) fn fs_read(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).map_err(|e| Error::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

/// Every key the `[config]` section admits, read off the section's own schema.
///
/// `deny_unknown_fields` makes serde's rejection of an unknown key name every
/// key it would have accepted, so the list comes from the struct rather than
/// from a copy of it that a new field would leave behind.
#[cfg(test)]
pub(crate) fn config_section_keys() -> Vec<String> {
    let refusal = toml::Value::Table(
        [(
            "a key no section admits".to_string(),
            toml::Value::Integer(0),
        )]
        .into_iter()
        .collect(),
    )
    .try_into::<ConfigSection>()
    .err()
    .expect("an unknown key is refused")
    .to_string();
    let (_, expected) = refusal
        .split_once("expected one of ")
        .expect("the refusal names the keys it would have taken");
    expected
        .split(", ")
        .map(|key| key.trim().trim_matches('`').to_string())
        .take_while(|key| !key.is_empty())
        .collect()
}
