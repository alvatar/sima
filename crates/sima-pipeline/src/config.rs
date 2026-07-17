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
//! [[execution.remote]]      # optional; a worker pool on an ssh-reachable
//! host     = "gpubox"       # machine, running workers inside a container
//! workers  = 4              # workers XOR [[execution.remote.device]] tables
//! image    = "localhost/sima-worker:latest"   # optional; this default
//! runtime  = "docker"       # optional; docker | podman
//! run_args = ["--gpus", "all"]                # optional; verbatim run flags
//!
//! [[execution.remote.device]]  # optional; same semantics as local
//! select  = "nvidia"
//! workers = 4
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
//! entries name one host — each a validation error naming the host. Remote
//! device selectors resolve at run start, over the remote's own hardware.
//!
//! The `[run]` section is canonicalized into [`RunConfig`], so its fields
//! define the run id; `[execution]` is operational and never hashed — a run
//! resumed with different parallelism or from a different store path keeps
//! its id. The structural keys are strict: an unknown key anywhere is
//! rejected. The `[run.generator]` table (minus `id`) and the `[run.params]`
//! table pass opaquely to the generator and domain translations, which own
//! and validate their keys.

use std::fs;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use sima_core::{Error, Result};
use sima_domains::{generator_params_for, params_for};
use sima_model::{FormatId, GeneratorConfig, GeneratorId, RunConfig};
use sima_scheduler::ExecutionConfig;

use crate::devices::DeviceSelector;

/// The image a remote pool runs when its config names none.
const DEFAULT_IMAGE: &str = "localhost/sima-worker:latest";
/// The container runtime a remote pool uses when its config names none.
const DEFAULT_RUNTIME: &str = "docker";

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
    /// The store path, resolved against the config file's directory.
    pub store: PathBuf,
}

/// One resolved `[[execution.remote]]` pool: its ssh destination and container
/// settings, with the device selectors left unresolved until the run starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteConfig {
    /// The ssh destination: an alias or `user@host`.
    pub host: String,
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

/// One `[[execution.remote]]` pool: an ssh destination running workers inside a
/// container. `workers` and `[[execution.remote.device]]` are exclusive, as
/// they are locally.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteSection {
    host: String,
    workers: Option<usize>,
    image: Option<String>,
    runtime: Option<String>,
    #[serde(default)]
    run_args: Vec<String>,
    #[serde(default)]
    device: Vec<DeviceSection>,
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
    let params = params_for(&format, &file.run.params)?;
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
    // The local pool size comes from one place or the other, never both: with
    // device entries the pool is their sum, so a top-level count could only
    // disagree with it. With neither, there is no local pool — valid only when
    // a remote pool carries the work.
    let workers = match (file.execution.workers, file.execution.device.is_empty()) {
        (Some(_), false) => {
            return Err(Error::Validation(format!(
                "{}: execution.workers and [[execution.device]] cannot both be set; \
                 the device entries carry the workers",
                path.display()
            )));
        }
        (None, true) if remotes.is_empty() => {
            return Err(Error::Validation(format!(
                "{}: execution.workers is required without [[execution.device]] entries \
                 or an [[execution.remote]] pool",
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
        store,
    })
}

/// Validates the `[[execution.remote]]` entries and resolves each into a
/// [`RemoteConfig`], its device selectors left unresolved. Each entry sets
/// `workers` or `[[execution.remote.device]]` but never both nor neither, its
/// `runtime` is `docker` or `podman`, and no two entries name one host — each a
/// [`Error::Validation`] naming the host, so the fix is one line.
fn resolve_remotes(path: &Path, sections: Vec<RemoteSection>) -> Result<Vec<RemoteConfig>> {
    let mut remotes: Vec<RemoteConfig> = Vec::with_capacity(sections.len());
    for section in sections {
        if remotes.iter().any(|r| r.host == section.host) {
            return Err(Error::Validation(format!(
                "{}: two [[execution.remote]] entries name host {:?}; one entry per machine",
                path.display(),
                section.host
            )));
        }
        // Workers XOR device tables, exactly as locally.
        match (section.workers, section.device.is_empty()) {
            (Some(_), false) => {
                return Err(Error::Validation(format!(
                    "{}: remote {:?} sets both workers and [[execution.remote.device]]; \
                     the device entries carry the workers",
                    path.display(),
                    section.host
                )));
            }
            (None, true) => {
                return Err(Error::Validation(format!(
                    "{}: remote {:?} sets neither workers nor [[execution.remote.device]]; \
                     one is required",
                    path.display(),
                    section.host
                )));
            }
            _ => {}
        }
        let runtime = section.runtime.unwrap_or_else(|| DEFAULT_RUNTIME.to_string());
        if runtime != "docker" && runtime != "podman" {
            return Err(Error::Validation(format!(
                "{}: remote {:?} runtime {runtime:?} is not one of docker, podman",
                path.display(),
                section.host
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
        assert_eq!(remote.host, "gpubox");
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
        assert!(message.contains("containerd"), "names the runtime: {message}");
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
