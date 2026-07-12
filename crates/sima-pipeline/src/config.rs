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
//! # gray-scott.v1 takes eight required keys: width, height, steps, dt,
//! # base_u, base_v, side_divisor, noise_width
//!
//! [execution]               # operational — never hashed
//! store = "./store"         # resolved relative to this file's directory
//! workers = 4
//! max_attempts = 3
//! attempt_timeout_ms = 5000 # optional; absent disables expiry reporting
//! checkpoint_interval_ms = 30000 # optional; absent disables checkpointing
//! ```
//!
//! The `[run]` section is canonicalized into [`RunConfig`], so its fields
//! define the run id; `[execution]` is operational and never hashed — a run
//! resumed with different parallelism or from a different store path keeps
//! its id. The structural keys are strict: an unknown key anywhere is
//! rejected. The `[run.generator]` table (minus `id`) and the `[run.params]`
//! table pass opaquely to the generator and domain translations, which own
//! and validate their keys.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use sima_core::{Error, Result};
use sima_domains::{generator_params_for, params_for};
use sima_model::{FormatId, GeneratorConfig, GeneratorId, RunConfig};
use sima_scheduler::ExecutionConfig;

/// A `sima.toml`, loaded and translated: the identity-bearing
/// [`RunConfig`], the operational [`ExecutionConfig`], and the store path
/// resolved relative to the config file.
#[derive(Debug)]
pub struct LoadedConfig {
    /// The identity section, canonicalized; its id is the run id.
    pub run: RunConfig,
    /// The execution section; never hashed.
    pub execution: ExecutionConfig,
    /// The store path, resolved against the config file's directory.
    pub store: PathBuf,
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
    workers: usize,
    max_attempts: u32,
    attempt_timeout_ms: Option<u64>,
    checkpoint_interval_ms: Option<u64>,
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
    let execution = ExecutionConfig::new(
        file.execution.workers,
        file.execution.max_attempts,
        attempt_timeout,
        checkpoint_interval,
    )?;

    // Relative to the config file's directory, never the working directory;
    // join leaves an absolute path as written.
    let base = path.parent().unwrap_or(Path::new(""));
    let store = base.join(&file.execution.store);

    Ok(LoadedConfig {
        run,
        execution,
        store,
    })
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
    fn an_absent_attempt_timeout_disables_expiry_reporting() -> Result<()> {
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
