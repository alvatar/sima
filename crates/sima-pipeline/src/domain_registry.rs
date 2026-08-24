//! [`DomainRegistry`]: where a format's domain is answered from.
//!
//! A run asks one boundary — [`DomainSource`] — for everything the orchestrator
//! reads of a format: its environment, its devices, its configuration
//! translations, its generator, and the binary its workers are spawned from.
//! Two things answer it:
//!
//! - [`BuiltinSource`], for the formats this build carries. It calls
//!   `sima-domains` directly, so the common path pays no process and no pipe.
//! - [`BinarySource`], for a format a config routes to a program of its own. It
//!   holds one session with that program for the life of the config.
//!
//! The registry is what a config resolves into, so a program that cannot answer
//! for the format it is declared under fails there — before a run reaches a
//! store.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use sima_contracts::{DeviceInfo, Generator};
use sima_core::{Error, Hash, Result, hash_bytes};
use sima_model::{Environment, FormatId, GeneratorId, Params, Spec};
use sima_transport::SpawnPolicy;
use sima_transport::domain_service::DomainService;

use crate::payload::PayloadSpec;

/// One `[domain.*]` entry, resolved: the format it answers for, the program
/// that answers, and the environment variables that program receives beyond
/// the baseline every spawned program gets.
pub(crate) struct DomainEntry {
    pub(crate) format: FormatId,
    /// The program, resolved against the config file's directory.
    pub(crate) binary: PathBuf,
    /// Exact variable names, forwarded from the orchestrator's environment
    /// where it holds them.
    pub(crate) env: Vec<String>,
    /// What travels when this run migrates, resolved against the config file's
    /// directory. `None` for an entry whose program stays on this machine.
    pub(crate) payload: Option<PayloadSpec>,
    /// The payload manifest the store already holds, which is what a
    /// synthesized far config states. The tree it names is materialized and
    /// installed where the config resolves, so the binary the entry spawns is
    /// there by the time it is spawned.
    pub(crate) payload_digest: Option<Hash>,
}

/// Where a format's domain is answered from.
pub(crate) trait DomainSource: Send + Sync {
    /// The environment the format's results depend on.
    fn environment(&self, format: &FormatId) -> Result<Environment>;

    /// The devices the format's work can run on.
    fn enumerate_devices(&self, format: &FormatId) -> Result<Vec<DeviceInfo>>;

    /// The `[run.params]` section, as text, translated into the canonical
    /// params bytes that enter the run id.
    fn translate_config(&self, format: &FormatId, toml: &str, segmented: bool) -> Result<Params>;

    /// The generator the run produces its candidates from, which also owns the
    /// translation of its own `[run.generator]` section.
    ///
    /// Built here rather than at the first batch, so a run naming a generator
    /// its source cannot answer for fails before its store exists.
    fn generator(
        &self,
        generator: &GeneratorId,
        format: &FormatId,
    ) -> Result<Box<dyn Generator + '_>>;

    /// The binary a worker for this format is spawned from.
    fn worker_binary(&self) -> Result<PathBuf>;

    /// The environment and working directory this format's workers are spawned
    /// under. Selecting it is the pipeline's business — it knows whether the
    /// binary is sima's own or a program a config named — while applying it is
    /// the transport's.
    fn spawn_policy(&self) -> SpawnPolicy;
}

/// The formats this build carries, answered in process.
#[derive(Debug)]
pub(crate) struct BuiltinSource;

impl DomainSource for BuiltinSource {
    fn environment(&self, format: &FormatId) -> Result<Environment> {
        Ok(sima_domains::domain_for(format)?.environment().clone())
    }

    fn enumerate_devices(&self, format: &FormatId) -> Result<Vec<DeviceInfo>> {
        sima_domains::domain_for(format)?.enumerate_devices()
    }

    fn translate_config(&self, format: &FormatId, toml: &str, segmented: bool) -> Result<Params> {
        sima_domains::domain_for(format)?.translate_config(toml, segmented)
    }

    fn generator(
        &self,
        generator: &GeneratorId,
        format: &FormatId,
    ) -> Result<Box<dyn Generator + '_>> {
        // The format is what the generator is checked against: a generator
        // drawing for another format would produce specs this run's executor
        // cannot read.
        sima_domains::generator_for(format, generator)
    }

    fn worker_binary(&self) -> Result<PathBuf> {
        crate::process::worker_binary()
    }

    fn spawn_policy(&self) -> SpawnPolicy {
        // sima's own worker, in the orchestrator's own trust domain.
        SpawnPolicy::Inherit
    }
}

/// One format, answered by the program a config routes it to.
#[derive(Debug)]
pub(crate) struct BinarySource {
    binary: PathBuf,
    /// The blake3 digest of the program file, read where the config resolved
    /// into this registry. Provenance a run journals and a resume compares
    /// against; it enters no hash, so the run's identity is what the program
    /// declares and nothing else.
    digest: Hash,
    /// The policy this program's processes are spawned under — its domain
    /// service and every worker of the run alike, so the two halves of one
    /// program see one environment.
    policy: SpawnPolicy,
    /// What travels when this run migrates, as the entry declared it. A
    /// migration reads it through [`DomainRegistry::routed`], which is the one
    /// boundary a caller sees the program itself through.
    payload: Option<PayloadSpec>,
    /// The variable names the entry declared, kept beside the policy they are
    /// in: a migration writes them into the far entry, so the program sees the
    /// same names there — with that machine's own values.
    env: Vec<String>,
    /// The open session. One conversation serves the whole config, so the
    /// program pays its startup cost once; the lock is what makes that one
    /// conversation reachable from the threads a run drives.
    session: Mutex<DomainService>,
}

impl BinarySource {
    /// Digests the entry's program, spawns it for its format, and confirms it
    /// answers — so a program that cannot be read or run, or does not serve the
    /// format, fails here. Every question this session asks is bounded by
    /// `answer_timeout`.
    fn spawn(entry: DomainEntry, answer_timeout: Duration) -> Result<BinarySource> {
        let DomainEntry {
            format,
            binary,
            env,
            payload,
            // Consumed where the config resolved: the tree it names is already
            // materialized and installed by the time the binary is spawned.
            payload_digest: _,
        } = entry;
        // The build about to serve this config, digested before it runs. The
        // digest is provenance every session journals, so an unreadable
        // program fails registration here, naming the path.
        let digest = hash_bytes(&std::fs::read(&binary).map_err(|source| Error::Io {
            path: binary.clone(),
            source,
        })?);
        // Both failures read as one thing to whoever wrote the entry: the
        // program declared for this format does not answer for it. The
        // program's own words follow.
        let declared = |e: Error| {
            Error::Validation(format!(
                "the program {} declared for format {:?} cannot answer for it: {e}",
                binary.display(),
                format.as_str()
            ))
        };
        let policy = SpawnPolicy::Explicit {
            passthrough: env.clone(),
        };
        let mut service =
            DomainService::spawn(&binary, &format, &policy, answer_timeout).map_err(declared)?;
        service.environment(&format).map_err(declared)?;
        Ok(BinarySource {
            binary,
            digest,
            policy,
            payload,
            env,
            session: Mutex::new(service),
        })
    }

    /// The open session, past a lock a panicking thread may have poisoned: the
    /// program is unaffected by a panic on this side, so the conversation
    /// continues.
    fn session(&self) -> std::sync::MutexGuard<'_, DomainService> {
        self.session.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl DomainSource for BinarySource {
    fn environment(&self, format: &FormatId) -> Result<Environment> {
        self.session().environment(format)
    }

    fn enumerate_devices(&self, format: &FormatId) -> Result<Vec<DeviceInfo>> {
        self.session().enumerate_devices(format)
    }

    fn translate_config(&self, format: &FormatId, toml: &str, segmented: bool) -> Result<Params> {
        self.session().translate_config(format, toml, segmented)
    }

    fn generator(
        &self,
        generator: &GeneratorId,
        format: &FormatId,
    ) -> Result<Box<dyn Generator + '_>> {
        Ok(Box::new(SessionGenerator {
            source: self,
            id: generator.clone(),
            format: format.clone(),
        }))
    }

    fn worker_binary(&self) -> Result<PathBuf> {
        Ok(self.binary.clone())
    }

    fn spawn_policy(&self) -> SpawnPolicy {
        self.policy.clone()
    }
}

/// A generator that answers over a program's session: both its translation and
/// its draw cross the pipe.
struct SessionGenerator<'a> {
    source: &'a BinarySource,
    id: GeneratorId,
    format: FormatId,
}

impl Generator for SessionGenerator<'_> {
    fn id(&self) -> &GeneratorId {
        &self.id
    }

    fn format(&self) -> &FormatId {
        &self.format
    }

    fn translate_config(&self, toml: &str) -> Result<Vec<u8>> {
        self.source
            .session()
            .translate_generator_config(&self.id, toml)
    }

    fn generate(&self, root_seed: u64, params: &[u8]) -> Result<Vec<Spec>> {
        self.source
            .session()
            .generate(&self.id, &self.format, root_seed, params)
    }
}

/// Which source answers for each format of a run.
///
/// Opaque to a caller: a loaded config carries one, and what it holds is the
/// pipeline's own business.
#[derive(Debug)]
pub struct DomainRegistry {
    builtin: BuiltinSource,
    /// The formats a config routes to a program of their own, by format id.
    configured: BTreeMap<String, BinarySource>,
}

impl DomainRegistry {
    /// The registry `entries` declare: one program per format, each spawned and
    /// asked to answer for the format it is declared under, under the run's
    /// answer deadline.
    pub(crate) fn new(
        entries: Vec<DomainEntry>,
        answer_timeout: Duration,
    ) -> Result<DomainRegistry> {
        let mut configured = BTreeMap::new();
        for entry in entries {
            let format = entry.format.as_str().to_string();
            configured.insert(format, BinarySource::spawn(entry, answer_timeout)?);
        }
        Ok(DomainRegistry {
            builtin: BuiltinSource,
            configured,
        })
    }

    /// The registry of a config that routes no format to a program.
    pub fn builtin() -> DomainRegistry {
        DomainRegistry {
            builtin: BuiltinSource,
            configured: BTreeMap::new(),
        }
    }

    /// The source answering for `format`: the program a config routed it to,
    /// or this build itself.
    pub(crate) fn source(&self, format: &FormatId) -> &dyn DomainSource {
        match self.configured.get(format.as_str()) {
            Some(source) => source,
            None => &self.builtin,
        }
    }

    /// The program a config routes `format` to, or `None` for a format this
    /// build answers itself. What separates the two cases for a caller that
    /// needs the program rather than the answers — the run's provenance and
    /// the refusals a program's presence implies.
    pub(crate) fn routed(&self, format: &FormatId) -> Option<RoutedProgram<'_>> {
        self.configured
            .get(format.as_str())
            .map(|source| RoutedProgram {
                binary: &source.binary,
                digest: &source.digest,
                payload: source.payload.as_ref(),
                env: &source.env,
            })
    }
}

/// The program answering for one format: the file a config named, the digest
/// of the bytes that file held when the config resolved, and what the entry
/// declared travels when the run moves.
pub(crate) struct RoutedProgram<'a> {
    pub(crate) binary: &'a Path,
    pub(crate) digest: &'a Hash,
    /// `None` for an entry whose program stays on the machine it is installed
    /// on, which is what a migration refuses to move.
    pub(crate) payload: Option<&'a PayloadSpec>,
    /// The variable names the entry declared. They travel to a far entry by
    /// name alone, as they are written here: each value comes from the machine
    /// the program runs on.
    pub(crate) env: &'a [String],
}

/// The text of a configuration section, as the source that owns its keys
/// receives it.
///
/// A section crosses as TOML text rather than as a parsed table, so a
/// program is free of sima's own TOML. The text is written from the table the
/// file declared, so it parses back to that table.
pub(crate) fn section_text(table: &toml::Table) -> Result<String> {
    toml::to_string(table).map_err(|e| {
        sima_core::Error::Validation(format!("the configuration section cannot be written: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use sima_core::Result;
    use sima_model::{FormatId, GeneratorId};

    use super::*;
    use crate::fixtures::built_worker;

    /// A validated format id.
    fn format(name: &str) -> FormatId {
        FormatId::new(name).expect("format id")
    }

    /// A validated generator id.
    fn generator(name: &str) -> GeneratorId {
        GeneratorId::new(name).expect("generator id")
    }

    /// An entry routing `name` to `binary` with `env`, declaring nothing that
    /// travels: what a program installed on this machine alone looks like.
    fn entry(name: &str, binary: PathBuf, env: Vec<String>) -> DomainEntry {
        DomainEntry {
            format: format(name),
            binary,
            env,
            payload: None,
            payload_digest: None,
        }
    }

    /// A registry whose `stub.v1` is answered by the built worker binary,
    /// which serves the in-tree formats over the same protocol a program outside
    /// the workspace does.
    fn served_by_binary() -> Result<DomainRegistry> {
        DomainRegistry::new(
            vec![entry("stub.v1", built_worker(), Vec::new())],
            Duration::MAX,
        )
    }

    #[test]
    fn a_format_with_no_entry_is_answered_in_process() -> Result<()> {
        // Every format a config does not route stays a direct call, so the
        // common path pays no process and no pipe.
        let registry = DomainRegistry::builtin();
        let source = registry.source(&format("stub.v1"));
        assert_eq!(
            source.environment(&format("stub.v1"))?,
            *sima_domains::domain_for(&format("stub.v1"))?.environment()
        );
        Ok(())
    }

    #[test]
    fn an_entry_routes_its_format_to_the_binary_it_names() -> Result<()> {
        // The declared format is answered by the program, and its worker
        // spawns from that same binary.
        let registry = served_by_binary()?;
        let source = registry.source(&format("stub.v1"));
        assert_eq!(source.worker_binary()?, built_worker());
        assert_eq!(
            source.environment(&format("stub.v1"))?,
            *sima_domains::domain_for(&format("stub.v1"))?.environment()
        );
        Ok(())
    }

    #[test]
    fn a_format_beside_a_declared_one_stays_in_process() -> Result<()> {
        // One entry routes one format; every other format of the same build is
        // still a direct call. The session the entry opened serves `stub.v1`
        // alone, so an answer about another format is one it never saw.
        let registry = served_by_binary()?;
        let nca = format("ca_evolution.nca.v1");
        assert_eq!(
            registry.source(&nca).environment(&nca)?,
            *sima_domains::domain_for(&nca)?.environment()
        );
        Ok(())
    }

    #[test]
    fn a_generator_that_does_not_belong_to_the_format_is_refused() {
        // A run declares a format and a generator separately, and the two must
        // agree: a generator that produces specs of another format would mint a
        // run id over the mismatch and fail only when the first spec is stored,
        // after the store exists. The refusal names both ids, since either one
        // could be the typo.
        let registry = DomainRegistry::builtin();
        let error = registry
            .source(&format("stub.v1"))
            .generator(&generator("ca_evolution.nca.v1"), &format("stub.v1"))
            .err()
            .expect("a generator belonging to another format");
        let message = error.to_string();
        assert!(
            message.contains("ca_evolution.nca.v1"),
            "names the generator: {message}"
        );
        assert!(message.contains("stub.v1"), "names the format: {message}");
    }

    #[test]
    fn a_binary_that_cannot_answer_for_its_format_fails_to_register() {
        // The registry is what a config resolves into, so a program that
        // cannot answer for the format it is declared under fails there —
        // before a run reaches a store.
        let Err(error) = DomainRegistry::new(
            vec![entry("acme.thing.v1", built_worker(), Vec::new())],
            Duration::MAX,
        ) else {
            panic!("expected a program that serves no such format to fail");
        };
        assert!(error.to_string().contains("acme.thing.v1"), "{error}");
    }

    #[test]
    fn a_binary_that_cannot_be_run_names_itself() {
        let Err(error) = DomainRegistry::new(
            vec![entry(
                "acme.thing.v1",
                PathBuf::from("/no/such/domain/binary"),
                Vec::new(),
            )],
            Duration::MAX,
        ) else {
            panic!("expected a binary that cannot be run to fail");
        };
        assert!(
            error.to_string().contains("/no/such/domain/binary"),
            "{error}"
        );
    }

    #[test]
    fn an_unknown_format_with_no_entry_names_itself() {
        let registry = DomainRegistry::builtin();
        let Err(error) = registry
            .source(&format("no-such-domain.v1"))
            .environment(&format("no-such-domain.v1"))
        else {
            panic!("expected a format this build does not carry to fail");
        };
        assert!(error.to_string().contains("no-such-domain.v1"), "{error}");
    }

    #[test]
    fn both_sources_translate_a_section_to_the_same_bytes() -> Result<()> {
        // What proves the protocol carries the configuration: the bytes that enter
        // the run id are the same whichever side answered.
        let registry = served_by_binary()?;
        let builtin = DomainRegistry::builtin();
        let text = "hex = \"00ff\"\n";
        assert_eq!(
            registry.source(&format("stub.v1")).translate_config(
                &format("stub.v1"),
                text,
                false
            )?,
            builtin
                .source(&format("stub.v1"))
                .translate_config(&format("stub.v1"), text, false)?
        );
        let text = "behaviors = [\"succeed\", \"reject\"]\n";
        assert_eq!(
            registry
                .source(&format("stub.v1"))
                .generator(&generator("stub.v1"), &format("stub.v1"))?
                .translate_config(text)?,
            builtin
                .source(&format("stub.v1"))
                .generator(&generator("stub.v1"), &format("stub.v1"))?
                .translate_config(text)?
        );
        Ok(())
    }

    #[test]
    fn both_sources_generate_the_same_specs() -> Result<()> {
        let registry = served_by_binary()?;
        let builtin = DomainRegistry::builtin();
        let params = builtin
            .source(&format("stub.v1"))
            .generator(&generator("stub.v1"), &format("stub.v1"))?
            .translate_config("behaviors = [\"succeed\"]\n")?;
        assert_eq!(
            registry
                .source(&format("stub.v1"))
                .generator(&generator("stub.v1"), &format("stub.v1"))?
                .generate(42, &params)?,
            builtin
                .source(&format("stub.v1"))
                .generator(&generator("stub.v1"), &format("stub.v1"))?
                .generate(42, &params)?
        );
        Ok(())
    }

    #[test]
    fn both_sources_enumerate_the_same_devices() -> Result<()> {
        let registry = served_by_binary()?;
        assert_eq!(
            registry
                .source(&format("stub.v1"))
                .enumerate_devices(&format("stub.v1"))?,
            sima_domains::devices::enumerate_devices(&format("stub.v1"))?
        );
        Ok(())
    }

    #[test]
    fn an_unknown_generator_binds_nothing_in_process() {
        // The in-process source builds the generator where the run is set up,
        // so a run naming a generator this build does not carry fails before
        // its store exists.
        let registry = DomainRegistry::builtin();
        let Err(error) = registry
            .source(&format("stub.v1"))
            .generator(&generator("no-such-generator.v1"), &format("stub.v1"))
        else {
            panic!("expected a generator this build does not carry to bind nothing");
        };
        assert!(
            error.to_string().contains("no-such-generator.v1"),
            "{error}"
        );
    }

    #[test]
    fn a_format_answered_in_process_spawns_the_way_sima_spawns_its_own() -> Result<()> {
        // The builtin worker is sima's own binary in sima's own trust domain,
        // so it keeps the orchestrator's environment and working directory.
        let registry = DomainRegistry::builtin();
        assert_eq!(
            registry.source(&format("stub.v1")).spawn_policy(),
            SpawnPolicy::Inherit
        );
        Ok(())
    }

    #[test]
    fn an_entry_s_declared_names_reach_the_policy_its_program_is_spawned_under() -> Result<()> {
        // One policy answers for the whole program: the session already open
        // and every worker the run will spawn from the same binary.
        let registry = DomainRegistry::new(
            vec![entry(
                "stub.v1",
                built_worker(),
                vec!["ACME_ASSETS".to_string()],
            )],
            Duration::MAX,
        )?;
        assert_eq!(
            registry.source(&format("stub.v1")).spawn_policy(),
            SpawnPolicy::Explicit {
                passthrough: vec!["ACME_ASSETS".to_string()],
            }
        );
        Ok(())
    }

    #[test]
    fn a_format_with_no_entry_is_routed_to_no_program() {
        // A format this build answers has no program and no digest, so the
        // provenance the routed program carries is asked for only where a
        // config named one.
        let registry = DomainRegistry::builtin();
        assert!(registry.routed(&format("stub.v1")).is_none());
    }

    #[test]
    fn an_entry_routes_its_format_with_the_digest_of_the_file_it_names() -> Result<()> {
        // What the run journals as provenance: the bytes of the build that
        // answered, digested where the config resolved into a registry.
        let registry = served_by_binary()?;
        let routed = registry
            .routed(&format("stub.v1"))
            .expect("the declared format is routed to its program");
        assert_eq!(routed.binary, built_worker());
        let bytes = std::fs::read(built_worker()).expect("read the program");
        assert_eq!(*routed.digest, sima_core::hash_bytes(&bytes));
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn a_binary_whose_bytes_cannot_be_read_names_itself() {
        // A program sima cannot digest is a program whose provenance a run
        // could not record, so the config fails to resolve rather than running
        // with a build nothing identifies. Execute permission alone runs a
        // binary but does not read it.
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("temp dir");
        let unreadable = dir.path().join("worker");
        std::fs::copy(built_worker(), &unreadable).expect("copy the program");
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o111))
            .expect("make the program execute-only");
        let outcome = DomainRegistry::new(
            vec![entry("stub.v1", unreadable.clone(), Vec::new())],
            Duration::MAX,
        );
        // Restore before asserting, so a failure still leaves a removable dir.
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o755))
            .expect("restore the program");
        let Err(error) = outcome else {
            panic!("expected a program whose bytes cannot be read to fail");
        };
        assert!(
            error
                .to_string()
                .contains(&unreadable.display().to_string()),
            "{error}"
        );
    }

    #[test]
    fn a_section_parses_back_to_the_table_it_was_written_from() -> Result<()> {
        // Configuration crosses as text, so the text a source is
        // given must mean what the file declared — values, nested tables, and
        // floats alike.
        let declared: toml::Table = r#"
            width = 128
            dt = 1.0
            noise_width = 0.02
            names = ["a", "b"]
            snapshot_when = { scalar = "activity", min = 1e-4 }
        "#
        .parse()
        .expect("a table");
        let text = section_text(&declared)?;
        assert_eq!(text.parse::<toml::Table>().expect("a table"), declared);
        Ok(())
    }

    #[test]
    fn an_absent_section_is_empty_text() -> Result<()> {
        assert_eq!(section_text(&toml::Table::new())?, "");
        Ok(())
    }
}
