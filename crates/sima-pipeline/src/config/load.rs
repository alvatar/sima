//! [`LoadedConfig`] and the load that produces it: the file read, parsed, and
//! translated section by section.
//!
//! The order is what makes a bad config fail early: the domains a config routes
//! to a program are resolved first, so a program that cannot answer for its
//! format fails before a store path is even computed; the identity section
//! follows, then the operational settings, then the machines.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use sima_core::{Error, Hash, Result};
use sima_model::{FormatId, SearchConfig};
use sima_provider::Budget;
use sima_scheduler::ExecutionConfig;
use sima_store::Store;

use super::GENERATED_DIR;
use super::file::{DomainSection, FileConfig, fs_read};
use super::machines::{
    Fleet, Host, HostClass, HostClassForm, HostForm, Orchestrator, resolve_host,
    resolve_host_class, resolve_orchestrator,
};
use super::search::resolve_search;
use super::settings::{optional_bound, resolve_budget, resolve_execution};
use crate::domain_registry::{DomainEntry, DomainRegistry};
use crate::payload::{PayloadSpec, ProgramTree, install};
use crate::sdk::{Sdk, materialize};

/// A `sima.toml`, loaded and translated: the identity-bearing [`SearchConfig`], the
/// operational [`ExecutionConfig`], the machines the search may draw on, and the
/// store path resolved relative to the config file.
#[derive(Debug)]
pub struct LoadedConfig {
    /// The identity section, canonicalized; its id is the search id.
    pub search: SearchConfig,
    /// The parameters the search executes under, assembled from `[config]` and the
    /// orchestrator's worker layout; never hashed. Its `workers` is the
    /// orchestrator's pool size — `0` for an orchestrator that declares none —
    /// and its device entries are empty here: a selector names real hardware, so
    /// it resolves where the search starts.
    pub execution: ExecutionConfig,
    /// This machine.
    pub orchestrator: Orchestrator,
    /// The declared hosts, by name.
    pub hosts: BTreeMap<String, Host>,
    /// The declared host classes, by name.
    pub host_classes: BTreeMap<String, HostClass>,
    /// The members a search may draw on, in the order they were listed.
    pub fleet: Fleet,
    /// The spend and wall-clock ceilings over every rental in the search.
    pub budget: Budget,
    /// The store path, resolved against the config file's directory.
    pub store: PathBuf,
    /// Where each of the search's format questions is answered from: this build,
    /// or the program a `[domain.*]` entry routes the format to.
    pub domains: DomainRegistry,
}

/// One `[exec]` job, resolved against its config file and restricted to one
/// rented host.
#[derive(Debug)]
pub struct ExecConfig {
    /// The `[host.*]` entry name, which also defines the ledger owner.
    pub host_name: String,
    /// The resolved rented host and its remote paths.
    pub host: Host,
    /// One opaque command interpreted by the remote shell.
    pub command: String,
    /// The payload shipped to the machine.
    pub payload: PayloadSpec,
    /// Remote shell globs anchored at the payload root.
    pub outputs: Vec<String>,
    /// The local directory fetched files are unpacked into.
    pub fetch_to: PathBuf,
    /// The spend and wall-clock ceilings for the exec rental.
    pub budget: Budget,
    /// The ledger and payload object store.
    pub store: PathBuf,
}

/// The store's directory name under [`GENERATED_DIR`], for a config that
/// names no path of its own.
const STORE_DIR: &str = "store";
/// Where exec outputs land when `[exec]` names no directory.
const EXEC_OUTPUT_DIR: &str = "exec-outputs";

/// Loads and translates the `sima.toml` at `path`. Parse errors, unknown or
/// missing keys, and invalid values are [`Error::Validation`] naming the file;
/// the generator and params tables are validated by the code the config names.
pub fn load(path: &Path) -> Result<LoadedConfig> {
    let text = fs_read(path)?;
    let file: FileConfig =
        toml::from_str(&text).map_err(|e| Error::Validation(format!("{}: {e}", path.display())))?;

    let search_section = required_section(path, "search", file.search)?;
    let config_section = required_section(path, "config", file.config)?;

    // The answer deadline precedes the registry, which spawns programs and
    // asks them questions: a session opened before the deadline was read
    // would wait on its first answer without one.
    let answer_timeout = optional_bound(config_section.answer_timeout_ms);
    // Relative to the config file's directory, never the working directory;
    // join leaves an absolute path as written. Computed here rather than at
    // the end, because an entry naming a payload digest reads that store to
    // install the program the same entry's binary names. Naming a path opens
    // nothing: a config that routes no payload never touches it.
    let store = resolve_store(path, Some(&config_section));
    // The registry precedes the search's translation, which is answered through
    // it: a program declared for a format is spawned and asked here, so an
    // entry naming one that cannot answer fails before the search has a store.
    let domains = resolve_domains(path, file.domain, answer_timeout, &store)?;
    let search = resolve_search(path, search_section, &domains)?;
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

    reject_repeated_machines(path, &hosts, &host_classes, &fleet)?;

    let budget = resolve_budget(path, file.budget)?;
    let execution = resolve_execution(path, &config_section, &orchestrator)?;

    Ok(LoadedConfig {
        search,
        execution,
        orchestrator,
        hosts,
        host_classes,
        fleet,
        budget,
        store,
        domains,
    })
}

/// Loads the `[exec]` contract without resolving search identity, domains, or
/// worker layout.
pub fn load_exec(path: &Path) -> Result<ExecConfig> {
    let text = fs_read(path)?;
    let mut file: FileConfig =
        toml::from_str(&text).map_err(|e| Error::Validation(format!("{}: {e}", path.display())))?;
    let exec = required_section(path, "exec", file.exec)?;
    let store = resolve_store(path, file.config.as_ref());
    let budget = resolve_budget(path, file.budget)?;
    let host_name = exec.host;
    let Some(section) = file.host.remove(&host_name) else {
        let detail = if file.host_class.contains_key(&host_name) {
            "names a [host_class.*] entry; an exec uses one rented [host.*] entry"
        } else {
            "names no [host.*] entry"
        };
        return Err(Error::Validation(format!(
            "{}: [exec] host {host_name:?} {detail}",
            path.display()
        )));
    };
    let host = resolve_host(path, &host_name, section)?;
    if !matches!(host.form, HostForm::Rented(_)) {
        return Err(Error::Validation(format!(
            "{}: [exec] host {host_name:?} is a machine of yours; an exec names a rented \
             [host.*] entry",
            path.display()
        )));
    }
    let base = path.parent().unwrap_or(Path::new(""));
    let payload =
        resolve_payload_paths(path, base, "[exec]", &exec.payload, exec.install.as_deref())?;
    for output in &exec.outputs {
        if Path::new(output)
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
        {
            return Err(Error::Validation(format!(
                "{}: [exec] output glob {output:?} escapes the payload root; output globs are relative to that root",
                path.display()
            )));
        }
    }
    let fetch_to = base.join(exec.fetch_to.as_deref().unwrap_or(EXEC_OUTPUT_DIR));
    Ok(ExecConfig {
        host_name,
        host,
        command: exec.command,
        payload,
        outputs: exec.outputs,
        fetch_to,
        budget,
        store,
    })
}

/// Resolves the shared store setting from any command config at `path`.
/// Verb-specific sections are parsed but are not required or translated.
pub fn load_store(path: &Path) -> Result<PathBuf> {
    let text = fs_read(path)?;
    let file: FileConfig =
        toml::from_str(&text).map_err(|e| Error::Validation(format!("{}: {e}", path.display())))?;
    Ok(resolve_store(path, file.config.as_ref()))
}

/// Requires one verb-specific top-level section with a direct message instead
/// of exposing serde's structural representation.
fn required_section<T>(path: &Path, name: &str, section: Option<T>) -> Result<T> {
    section.ok_or_else(|| {
        Error::Validation(format!(
            "{}: [{name}] section is required for this command",
            path.display()
        ))
    })
}

/// Resolves the shared store setting, defaulting beside the config when its
/// section or key is absent.
fn resolve_store(path: &Path, config: Option<&super::file::ConfigSection>) -> PathBuf {
    let base = path.parent().unwrap_or(Path::new(""));
    match config.and_then(|config| config.store.as_deref()) {
        Some(stated) => base.join(stated),
        None => base.join(GENERATED_DIR).join(STORE_DIR),
    }
}

/// Builds the registry the `[domain.*]` entries declare: each entry's format id
/// paired with the binary that answers for it, resolved against the config
/// file's directory, and the variable names that binary is given.
fn resolve_domains(
    path: &Path,
    entries: BTreeMap<String, DomainSection>,
    answer_timeout: Duration,
    store: &Path,
) -> Result<DomainRegistry> {
    // Absolute, because a spawned program runs in a scratch working directory
    // of its own: a path relative to this process would resolve against that
    // directory rather than against the config that named it.
    // A bare file name has an empty parent, which is this directory.
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let base = std::path::absolute(parent).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut declared = entries
        .into_iter()
        .map(|(format, section)| {
            let payload = resolve_payload(path, &base, &format, &section)?;
            let payload_digest = resolve_payload_digest(path, &format, section.payload_digest)?;
            let env = resolve_domain_env(path, &format, section.env)?;
            let sdk = resolve_sdk(path, &format, section.sdk)?;
            let format = FormatId::new(format).map_err(|e| {
                Error::Validation(format!("{}: [domain.*] entry: {e}", path.display()))
            })?;
            Ok(DomainEntry {
                format,
                binary: base.join(section.binary),
                env,
                payload,
                payload_digest,
                sdk,
                sdk_path: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    vend_sdks(path, &base, &mut declared)?;
    install_payloads(path, &base, &declared, store)?;
    DomainRegistry::new(declared, answer_timeout)
        .map_err(|e| Error::Validation(format!("{}: {e}", path.display())))
}

/// Vends the SDK every entry declaring one needs, and records where, before any
/// of their programs is spawned — so a program's `import` resolves the first
/// time it runs.
///
/// One tree per SDK, however many entries declare it: what it holds is a
/// property of this binary, not of any one program. An entry declaring none
/// leaves the disk alone.
fn vend_sdks(path: &Path, base: &Path, declared: &mut [DomainEntry]) -> Result<()> {
    let mut vended: BTreeMap<Sdk, PathBuf> = BTreeMap::new();
    for entry in declared {
        let Some(sdk) = entry.sdk else {
            continue;
        };
        let directory = match vended.get(&sdk) {
            Some(directory) => directory.clone(),
            None => {
                let format = entry.format.as_str();
                let directory = materialize(base, sdk).map_err(|e| {
                    Error::Validation(format!(
                        "{}: [domain.{format:?}] the {} SDK could not be vended: {e}",
                        path.display(),
                        sdk.as_str(),
                    ))
                })?;
                vended.insert(sdk, directory.clone());
                directory
            }
        };
        entry.sdk_path = Some(directory);
    }
    Ok(())
}

/// Installs every payload the entries name, before any of their programs is
/// spawned — so the file an entry's `binary` points at is on this machine by
/// the time the registry reads it.
///
/// The store is opened only when an entry names a digest. Opening one creates
/// it, and a config that routes no payload must leave the disk as it found it.
fn install_payloads(
    path: &Path,
    base: &Path,
    declared: &[DomainEntry],
    store: &Path,
) -> Result<()> {
    if declared.iter().all(|entry| entry.payload_digest.is_none()) {
        return Ok(());
    }
    let store = Store::open(store)?;
    for entry in declared {
        let Some(digest) = &entry.payload_digest else {
            continue;
        };
        let format = entry.format.as_str();
        let tree = ProgramTree::new(base, format)?;
        install(&store, digest, &tree).map_err(|e| {
            Error::Validation(format!(
                "{}: [domain.{format:?}] payload {digest} could not be installed: {e}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

/// Validates what an entry declares travels, and resolves both paths against
/// `base`, the config file's own directory.
///
/// The rules say the same thing four ways: an entry names exactly one source
/// for the program that will run on the destination, and a source that needs a
/// script says so.
fn resolve_payload(
    path: &Path,
    base: &Path,
    format: &str,
    section: &DomainSection,
) -> Result<Option<PayloadSpec>> {
    let refuse = |message: String| -> Error {
        Error::Validation(format!("{}: [domain.{format:?}] {message}", path.display()))
    };
    // A digest names a manifest the store already holds, so it is the whole
    // description of the program: anything stating a second source is asking
    // for two programs under one entry.
    if section.payload_digest.is_some() {
        if section.payload.is_some() {
            return Err(refuse(
                "states both payload and payload_digest; a payload is ingested here \
                 and a digest names a manifest the store already holds, so an entry \
                 names one or the other"
                    .to_string(),
            ));
        }
        if section.install.is_some() {
            return Err(refuse(
                "states both install and payload_digest; the manifest the digest names \
                 carries its own install script"
                    .to_string(),
            ));
        }
        return Ok(None);
    }
    let Some(declared) = &section.payload else {
        if section.install.is_some() {
            return Err(refuse(
                "states install and no payload; the script installs the payload, \
                 so there is nothing for it to install"
                    .to_string(),
            ));
        }
        return Ok(None);
    };
    resolve_payload_paths(
        path,
        base,
        &format!("[domain.{format:?}]"),
        declared,
        section.install.as_deref(),
    )
    .map(Some)
}

/// Resolves and validates one declared payload and its optional install script.
fn resolve_payload_paths(
    path: &Path,
    base: &Path,
    subject: &str,
    declared: &str,
    declared_install: Option<&str>,
) -> Result<PayloadSpec> {
    let refuse = |message: String| -> Error {
        Error::Validation(format!("{}: {subject} {message}", path.display()))
    };
    let payload = base.join(declared);
    // `symlink_metadata` is deliberate: what travels is the entry as written,
    // and a link to a directory is not a directory to walk.
    let metadata = std::fs::symlink_metadata(&payload).map_err(|source| {
        refuse(format!(
            "payload {} cannot be read: {source}",
            payload.display()
        ))
    })?;
    let install = match declared_install {
        Some(declared) => {
            let install = base.join(declared);
            let metadata = std::fs::symlink_metadata(&install).map_err(|source| {
                refuse(format!(
                    "install {} cannot be read: {source}",
                    install.display()
                ))
            })?;
            if !metadata.is_file() {
                return Err(refuse(format!(
                    "install {} is not a regular file; it is search as a shell script",
                    install.display()
                )));
            }
            Some(install)
        }
        // A directory has no entry point by convention — which of its files
        // runs is what the script decides — while a single file is the
        // program, so the script would only put it where the convention
        // already puts it.
        None if metadata.is_dir() => {
            return Err(refuse(format!(
                "payload {} is a directory and the entry states no install; \
                 a directory payload names the script that turns it into a program",
                payload.display()
            )));
        }
        None => None,
    };
    Ok(PayloadSpec { payload, install })
}

/// Parses the manifest digest an entry states, which is a content address like
/// any other in this system.
fn resolve_payload_digest(
    path: &Path,
    format: &str,
    digest: Option<String>,
) -> Result<Option<Hash>> {
    digest
        .map(|digest| {
            Hash::from_hex(&digest).map_err(|e| {
                Error::Validation(format!(
                    "{}: [domain.{format:?}] payload_digest {digest:?} is not a content \
                     address: {e}",
                    path.display()
                ))
            })
        })
        .transpose()
}

/// Validates the SDK an entry declares, which is a language this binary vends a
/// package for.
fn resolve_sdk(path: &Path, format: &str, sdk: Option<String>) -> Result<Option<Sdk>> {
    sdk.map(|sdk| {
        Sdk::parse(&sdk).ok_or_else(|| {
            Error::Validation(format!(
                "{}: [domain.{format:?}] sdk {sdk:?} names no SDK this binary vends; \
                 the key takes {}",
                path.display(),
                Sdk::accepted(),
            ))
        })
    })
    .transpose()
}

/// Validates the variable names an entry forwards to its program.
///
/// Each is a name alone: its value comes from the orchestrator's own
/// environment, so an entry that wrote one would be a second source of it.
fn resolve_domain_env(path: &Path, format: &str, env: Option<Vec<String>>) -> Result<Vec<String>> {
    let env = env.unwrap_or_default();
    for name in &env {
        if name.is_empty() || name.contains('=') {
            return Err(Error::Validation(format!(
                "{}: [domain.{format:?}] env takes environment variable names, \
                 each one non-empty and free of '=', and got {name:?}",
                path.display()
            )));
        }
    }
    Ok(env)
}

/// Rejects a fleet that would engage one machine twice.
///
/// Two entries may name one ssh destination — alternative worker profiles for a
/// box, picked by which one `members` names — but engaging both in one search puts
/// two pools on one machine: it over-subscribes it, and both pools journal
/// under the same host label, so the search's per-host attribution stops meaning
/// anything. A member listed twice is the same fault said differently.
///
/// Only machines of yours are checked. A rented entry carries no destination
/// until it is acquired, and two rentals are two machines by construction.
fn reject_repeated_machines(
    path: &Path,
    hosts: &BTreeMap<String, Host>,
    host_classes: &BTreeMap<String, HostClass>,
    fleet: &Fleet,
) -> Result<()> {
    // The member that first engaged each ssh destination, so a collision can
    // name both entries — the two lines the reader has to reconcile.
    let mut engaged: BTreeMap<&str, &str> = BTreeMap::new();
    let mut listed: BTreeSet<&str> = BTreeSet::new();
    for member in &fleet.members {
        if !listed.insert(member.as_str()) {
            return Err(Error::Validation(format!(
                "{}: fleet member {member:?} is listed twice; a search engages a machine once",
                path.display()
            )));
        }
        for destination in destinations(member, hosts, host_classes) {
            let Some(first) = engaged.insert(destination, member) else {
                continue;
            };
            return Err(Error::Validation(if first == member {
                format!(
                    "{}: host_class {member:?} lists the ssh destination {destination:?} twice; \
                     a search engages a machine once",
                    path.display()
                )
            } else {
                format!(
                    "{}: fleet members {first:?} and {member:?} both engage the ssh destination \
                     {destination:?}; a search puts one worker pool on a machine — name one of them",
                    path.display()
                )
            }));
        }
    }
    Ok(())
}

/// The ssh destinations a member is reached at: one for a host of yours, one
/// per machine for a class of yours, and none for a rented entry, whose
/// address the provider answers with at acquisition.
fn destinations<'a>(
    member: &str,
    hosts: &'a BTreeMap<String, Host>,
    host_classes: &'a BTreeMap<String, HostClass>,
) -> Vec<&'a str> {
    if let Some(host) = hosts.get(member) {
        return match &host.form {
            HostForm::Owned(owned) => vec![owned.ssh.as_str()],
            HostForm::Rented(_) => Vec::new(),
        };
    }
    match host_classes.get(member).map(|class| &class.form) {
        Some(HostClassForm::Owned(owned)) => owned.ssh.iter().map(String::as_str).collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::super::machines::{Container, FillPolicy, OwnedClass, OwnedHost, Pool, ProviderId};
    use sima_provider::{Cost, Price};

    use crate::devices::DeviceSelector;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use sima_domains::{StubBehavior, StubGeneratorConfig};
    use sima_model::SearchId;
    use sima_transport::SpawnPolicy;

    use super::*;

    /// Writes `text` as a config file named `name` under `dir`.
    fn write_config(dir: &Path, name: &str, text: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, text).expect("write config file");
        path
    }

    /// The reference schema instance from the module doc: a search driven on this
    /// machine alone.
    const BASE: &str = r#"
        [search]
        root_seed = 42
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = ["succeed", "flaky:2", "sleep:50", "reject", "panic"]

        [search.params]
        hex = "00ff"

        [config]
        store = "./store"
        max_attempts = 3
        attempt_timeout_ms = 5000

        [orchestrator]
        workers = 4
    "#;

    /// The reference schema with an orchestrator that executes nothing, for the
    /// configs whose machines carry the search.
    const NO_POOL: &str = r#"
        [search]
        root_seed = 42
        format = "stub.v1"

        [search.generator]
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

    /// The search id `text` loads to.
    fn id_of(text: &str) -> SearchId {
        load_text(text).expect("config loads").search.id()
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
    fn the_reference_config_loads_into_the_expected_search_config() -> Result<()> {
        let loaded = load_text(BASE)?;
        assert_eq!(loaded.search.root_seed, 42);
        assert_eq!(loaded.search.format.as_str(), "stub.v1");
        assert_eq!(loaded.search.generator.id.as_str(), "stub.v1");
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
        assert_eq!(loaded.search.generator.params, expected.to_bytes());
        assert_eq!(loaded.search.params.bytes, vec![0x00, 0xff]);
        assert_eq!(loaded.execution.workers, 4);
        assert_eq!(loaded.execution.max_attempts, 3);
        assert_eq!(
            loaded.execution.attempt_timeout,
            Duration::from_millis(5000)
        );
        Ok(())
    }

    #[test]
    fn loading_the_same_file_twice_yields_the_same_search_id() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(dir.path(), "sima.toml", BASE);
        assert_eq!(load(&path)?.search.id(), load(&path)?.search.id());
        Ok(())
    }

    #[test]
    fn a_legacy_run_section_is_rejected_as_unknown() {
        let legacy = BASE.replacen("[search]", "[run]", 1);
        let message = rejection(&legacy);
        assert!(message.contains("unknown field `run`"), "{message}");
    }

    #[test]
    fn a_generator_of_another_format_is_refused_at_load() {
        // Format and generator are separate keys, so a config can name a
        // generator that draws for a different format. Minting a search id over
        // that pairing would defer the failure to the first stored spec, on a
        // search whose id is already fixed; the load refuses it instead, naming
        // both ids because either could be the typo.
        let dir = tempfile::tempdir().expect("temp dir");
        let mismatched = BASE.replace(
            "[search.generator]\n        id = \"stub.v1\"",
            "[search.generator]\n        id = \"ca_evolution.nca.v1\"",
        );
        let path = write_config(dir.path(), "sima.toml", &mismatched);
        let Err(Error::Validation(message)) = load(&path) else {
            panic!("expected a mismatched generator to be refused");
        };
        assert!(message.contains("ca_evolution.nca.v1"), "{message}");
        assert!(message.contains("stub.v1"), "{message}");
    }

    #[test]
    fn every_identity_field_changes_the_search_id() {
        // Every [search] field whose variation still names dispatchable ids: the
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
            assert_ne!(base, id_of(&varied), "{to} must change the search id");
        }
    }

    #[test]
    fn operational_values_never_touch_the_search_id() {
        let base = id_of(BASE);
        for (from, to) in [
            ("store = \"./store\"", "store = \"./elsewhere\""),
            ("workers = 4", "workers = 1"),
            ("max_attempts = 3", "max_attempts = 9"),
            ("attempt_timeout_ms = 5000", "attempt_timeout_ms = 1"),
        ] {
            let varied = BASE.replace(from, to);
            assert_eq!(base, id_of(&varied), "{to} must not change the search id");
        }
        // And dropping the store key entirely, which is the edit the shipped
        // examples made when the default arrived: a search identity cannot turn
        // on where its results are kept.
        let unstated = BASE.replace("        store = \"./store\"\n", "");
        assert_ne!(
            unstated, BASE,
            "the store key was not removed, so this pins nothing"
        );
        assert_eq!(
            base,
            id_of(&unstated),
            "an unstated store must not change the search id"
        );
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
    fn a_config_stating_no_store_lands_in_the_dot_directory_beside_it() -> Result<()> {
        // Everything a driven config generates goes under one directory beside
        // it, and the store is the largest of them. Stating the key is for the
        // stores that live somewhere else.
        let dir = tempfile::tempdir().expect("temp dir");
        let text = BASE.replace("        store = \"./store\"\n", "");
        let loaded = load(&write_config(dir.path(), "sima.toml", &text))?;
        assert_eq!(loaded.store, dir.path().join(".sima").join("store"));
        // Naming a path is what overrides it, verbatim.
        let loaded = load(&write_config(dir.path(), "stated.toml", BASE))?;
        assert_eq!(loaded.store, dir.path().join("./store"));
        Ok(())
    }

    #[test]
    fn segments_loads_into_the_search_config() -> Result<()> {
        let text = BASE.replace("root_seed = 42", "root_seed = 42\nsegments = 10");
        assert_eq!(load_text(&text)?.search.segments, NonZeroU64::new(10));
        assert_eq!(load_text(BASE)?.search.segments, None);
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
    fn segments_changes_the_search_id() {
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
    fn neither_cadence_touches_the_search_id() {
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
            ("[search]", "[search]\nsurprise = 1\n"),
            ("[config]", "[config]\nsurprise = 1\n"),
            ("[search.params]", "[search.params]\nsurprise = 1\n"),
            ("[search.generator]", "[search.generator]\nsurprise = 1\n"),
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
    fn a_search_config_missing_max_attempts_names_the_file_and_key() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(
            dir.path(),
            "missing-attempts.toml",
            &BASE.replace("        max_attempts = 3\n", ""),
        );
        let Err(Error::Validation(message)) = load(&path) else {
            panic!("missing max_attempts must be a validation error");
        };
        assert_eq!(
            message,
            format!("{}: [config] max_attempts is required", path.display())
        );
    }

    #[test]
    fn a_syntax_error_is_validation_naming_the_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(dir.path(), "broken.toml", "search = [not toml");
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
        // The pool is the entries' sum; the classes resolve at search start, so the
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
        // runtime or a search flag here describes a container that does not exist.
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
        // container, its image defaulting, so the runtime and the search flags are
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
        assert_eq!(host.root, "~/sima");
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
        let text = format!("{BASE}\n[host.cloudbox]\nprovider = \"vast\"\n");
        let host = host_of(&text, "cloudbox");
        let HostForm::Rented(rented) = &host.form else {
            panic!("expected a rented machine");
        };
        assert_eq!(rented.provider, ProviderId::Vast);
        assert_eq!(rented.image, "ghcr.io/alvatar/sima:latest");
        assert_eq!(rented.disk_gb, 32);
        assert_eq!(rented.ready_timeout, Duration::from_millis(1_200_000));
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
            [host.cloudbox]
            provider = "vast"
            disk_gb = 64
            image = "ghcr.io/example/worker:pinned"
            ready_timeout_ms = 120000
            ready_poll_ms = 2000

            [host.cloudbox.constraints]
            gpu_models = ["RTX 4090"]
            min_gpu_count = 1
            min_vram_mb = 16000
            min_cuda = 12.0
            max_price_usd_hour = 0.5
            min_reliability = 0.95
            verified_only = true
            min_disk_gb = 32
            min_bandwidth_mbps = 100
            "#
        ))?;
        let HostForm::Rented(rented) = &loaded.hosts["cloudbox"].form else {
            panic!("expected a rented machine");
        };
        assert_eq!(rented.image, "ghcr.io/example/worker:pinned");
        assert_eq!(rented.disk_gb, 64);
        assert_eq!(rented.ready_timeout, Duration::from_millis(120_000));
        assert_eq!(rented.ready_poll, Duration::from_millis(2_000));
        assert_eq!(rented.constraints.gpu_models, vec!["RTX 4090".to_string()]);
        assert_eq!(rented.constraints.min_gpu_count, Some(1));
        assert_eq!(rented.constraints.min_vram_mb, Some(16000));
        assert_eq!(rented.constraints.min_cuda, Some(12.0));
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
        let text = format!("{BASE}\n[host.cloudbox]\nprovider = \"aws\"\n");
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
            let text = format!("{BASE}\n[host.cloudbox]\nprovider = \"stub\"\n{key}\n");
            let message = rejection(&text);
            let name = key.split(' ').next().expect("the key name");
            assert!(message.contains(name), "names the key: {message}");
            assert!(message.contains("rented"), "names the form: {message}");
        }
        // A device table is the same rejection, written as its own table.
        let text = format!(
            "{BASE}\n[host.cloudbox]\nprovider = \"stub\"\n\
             [[host.cloudbox.device]]\nselect = \"nvidia\"\nworkers = 1\n"
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
        let text = format!("{BASE}\n[host.cloudbox]\nprovider = \"stub\"\nfill = \"strict\"\n");
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
                "{BASE}\n[host.cloudbox]\nprovider = \"stub\"\n\
                 [host.cloudbox.constraints]\nmax_price_usd_hour = {value}\n"
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
    fn the_fleet_lists_the_members_a_search_may_draw_on() -> Result<()> {
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
        // In the order listed, which is the order the search engages them in.
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
    fn a_member_listed_twice_is_rejected() {
        let text = format!(
            "{BASE}\n[host.gpubox]\nworkers = 4\n[fleet]\nmembers = [\"gpubox\", \"gpubox\"]\n"
        );
        let message = rejection(&text);
        assert!(message.contains("gpubox"), "{message}");
        assert!(message.contains("twice"), "{message}");
    }

    #[test]
    fn two_engaged_entries_on_one_destination_are_rejected_naming_both() {
        // Two pools on one machine over-subscribe it and journal under one
        // host label, so the search's per-host attribution stops meaning anything.
        let text = format!(
            r#"{BASE}
            [host.gpubox]
            workers = 4

            [host_class.spare]
            ssh = ["gpubox", "sparebox"]
            workers = 9

            [fleet]
            members = ["gpubox", "spare"]
            "#
        );
        let message = rejection(&text);
        assert!(
            message.contains("gpubox"),
            "names the destination: {message}"
        );
        assert!(
            message.contains("spare"),
            "names the other entry: {message}"
        );
    }

    #[test]
    fn a_class_repeating_a_destination_in_its_own_list_is_rejected() {
        let text = format!(
            "{BASE}\n[host_class.lab]\nssh = [\"fermi\", \"fermi\"]\nworkers = 1\n\
             [fleet]\nmembers = [\"lab\"]\n"
        );
        let message = rejection(&text);
        assert!(message.contains("fermi"), "{message}");
        assert!(message.contains("twice"), "{message}");
    }

    #[test]
    fn two_profiles_for_one_machine_load_when_the_fleet_names_one() -> Result<()> {
        // Two entries may describe one box under different worker layouts and
        // be picked between by membership; only engaging both at once is a
        // fault.
        let text = format!(
            r#"{BASE}
            [host.gpubox]
            workers = 4

            [host.gpubox_full]
            ssh     = "gpubox"
            workers = 16

            [fleet]
            members = ["gpubox_full"]
            "#
        );
        let loaded = load_text(&text)?;
        assert_eq!(loaded.hosts.len(), 2, "both profiles are declared");
        assert_eq!(loaded.fleet.members, ["gpubox_full"], "one is engaged");
        Ok(())
    }

    #[test]
    fn two_rented_entries_are_two_machines_however_they_are_named() -> Result<()> {
        // A rented entry carries no destination until it is acquired, so two
        // rentals are two machines by construction and neither collides.
        let loaded = load_text(&format!(
            r#"{BASE}
            [host.first]
            provider = "stub"

            [host.second]
            provider = "stub"

            [fleet]
            members = ["first", "second"]
            "#
        ))?;
        assert_eq!(loaded.fleet.members, ["first", "second"]);
        Ok(())
    }

    #[test]
    fn a_declared_collision_the_fleet_does_not_engage_loads() -> Result<()> {
        // The check is on the engaged set, not the declared one: an unnamed
        // entry is a machine written down, whatever it points at.
        let loaded = load_text(&format!(
            "{BASE}\n[host.gpubox]\nworkers = 4\n[host.alias]\nssh = \"gpubox\"\nworkers = 9\n"
        ))?;
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
    fn a_zero_wall_clock_ceiling_is_no_ceiling_at_all() -> Result<()> {
        // Zero states the absence rather than a deadline that has already
        // passed: a search wound down before it computed anything is nothing to
        // ask for, so the value that would express it says "no limit" instead.
        let loaded = load_text(&format!("{BASE}\n[budget]\nmax_wall_clock_ms = 0\n"))?;
        assert_eq!(loaded.budget.max_wall_clock, None);
        assert_eq!(
            loaded.budget,
            load_text(BASE)?.budget,
            "stating zero and stating nothing are the same search"
        );
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
            "cloudbox",
            "[host.cloudbox]\nprovider = \"stub\"\n",
        ))?;
        assert_eq!(loaded.orchestrator.migrate.as_deref(), Some("cloudbox"));
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
        let message = rejection(&migrating("cloudbox", ""));
        assert!(message.contains("cloudbox"), "{message}");
        assert!(message.contains("host"), "{message}");
    }

    // ---- The machine model never enters search identity ----

    #[test]
    fn declaring_machines_never_changes_the_search_id() {
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

    // ---- Where a format is answered from ----

    #[test]
    fn a_format_routed_to_a_program_keeps_its_search_id() {
        // The protocol carries the configuration, so the identity a search has is the
        // one it has by direct call: the same file with an entry and without it
        // is the same search.
        let entry = format!(
            r#"{BASE}
            [domain."stub.v1"]
            binary = "{}"
            "#,
            crate::fixtures::built_worker().display()
        );
        assert_eq!(
            id_of(BASE),
            id_of(&entry),
            "where it is answered from is operational"
        );
    }

    #[test]
    fn a_program_that_cannot_answer_for_its_format_fails_the_load() {
        // The entry is read where the config resolves, so a program that cannot
        // answer fails there — and nothing of the search exists yet.
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(
            dir.path(),
            "sima.toml",
            &format!(
                r#"{BASE}
                [domain."acme.thing.v1"]
                binary = "{}"
                "#,
                crate::fixtures::built_worker().display()
            ),
        );
        let Err(error) = load(&path) else {
            panic!("expected a program that serves no such format to fail the load");
        };
        assert!(error.to_string().contains("acme.thing.v1"), "{error}");
        assert!(
            !dir.path().join("store").exists(),
            "a config that does not resolve leaves no store"
        );
    }

    #[test]
    fn a_program_path_resolves_against_the_config_file() {
        // Paths in a config are the file's, never the working directory's, so a
        // search and its program travel together.
        let dir = tempfile::tempdir().expect("temp dir");
        std::os::unix::fs::symlink(crate::fixtures::built_worker(), dir.path().join("program"))
            .expect("link the program beside the config");
        let path = write_config(
            dir.path(),
            "sima.toml",
            &format!(
                r#"{BASE}
                [domain."stub.v1"]
                binary = "program"
                "#
            ),
        );
        assert_eq!(
            load(&path).expect("the config loads").search.id(),
            id_of(BASE)
        );
    }

    #[test]
    fn a_domain_entry_takes_the_binary_alone() {
        let message = rejection(&format!(
            r#"{BASE}
            [domain."stub.v1"]
            binary = "/opt/acme/worker"
            workers = 4
            "#
        ));
        assert!(message.contains("workers"), "{message}");
    }

    #[test]
    fn a_domain_entry_named_outside_the_format_rule_is_rejected() {
        let message = rejection(&format!(
            r#"{BASE}
            [domain."Bad Name"]
            binary = "/opt/acme/worker"
            "#
        ));
        assert!(message.contains("Bad Name"), "{message}");
    }

    /// The spawn policy the loaded config gives `stub.v1`, over an entry
    /// routing it to the built worker with `env` written as `entry` states.
    fn stub_entry_policy(entry: &str) -> SpawnPolicy {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(
            dir.path(),
            "sima.toml",
            &format!(
                r#"{BASE}
                [domain."stub.v1"]
                binary = "{}"
                {entry}
                "#,
                crate::fixtures::built_worker().display()
            ),
        );
        let config = load(&path).expect("the config loads");
        config
            .domains
            .source(&FormatId::new("stub.v1").expect("format id"))
            .spawn_policy()
    }

    #[test]
    fn an_entry_without_env_forwards_the_baseline_alone() {
        // The key is optional, and an entry that omits it declares nothing
        // beyond what every program receives.
        assert_eq!(
            stub_entry_policy(""),
            SpawnPolicy::Explicit {
                passthrough: Vec::new(),
                prepend: Vec::new(),
                assign: Vec::new(),
            }
        );
    }

    #[test]
    fn the_names_an_entry_declares_reach_its_program_s_spawn_policy() {
        assert_eq!(
            stub_entry_policy(r#"env = ["ACME_ASSETS", "ACME_LICENSE_PATH"]"#),
            SpawnPolicy::Explicit {
                passthrough: vec!["ACME_ASSETS".to_string(), "ACME_LICENSE_PATH".to_string()],
                prepend: Vec::new(),
                assign: Vec::new(),
            }
        );
    }

    #[test]
    fn an_entry_declaring_the_sdk_reads_it_from_the_tree_the_binary_vended() {
        // The whole plumbing of the key in one assertion: the load materialized
        // the package under the config's own directory, and the program is
        // spawned reading it ahead of anything the machine has under that name.
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(
            dir.path(),
            "sima.toml",
            &format!(
                r#"{BASE}
                [domain."stub.v1"]
                binary = "{}"
                sdk = "python"
                "#,
                crate::fixtures::built_worker().display()
            ),
        );
        let config = load(&path).expect("the config loads");
        let installed = dir.path().join(".sima/sdk/python/installed");
        assert!(
            installed.join("sima/__init__.py").is_file(),
            "the package is on this machine"
        );
        assert_eq!(
            config
                .domains
                .source(&FormatId::new("stub.v1").expect("format id"))
                .spawn_policy(),
            SpawnPolicy::Explicit {
                passthrough: Vec::new(),
                prepend: vec![(
                    "PYTHONPATH".to_string(),
                    std::ffi::OsString::from(installed),
                )],
                assign: Vec::new(),
            }
        );
    }

    #[test]
    fn a_config_declaring_no_sdk_vends_none() {
        // Nothing is written for a config that asked for nothing: the tree is
        // a directory an entry declaring the key is what creates.
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(dir.path(), "sima.toml", BASE);
        load(&path).expect("the config loads");
        assert!(!dir.path().join(".sima").exists());
    }

    #[test]
    fn an_sdk_this_binary_does_not_vend_is_refused_naming_it() {
        let message = rejection(&format!(
            r#"{BASE}
            [domain."stub.v1"]
            binary = "/opt/acme/worker"
            sdk = "rust"
            "#
        ));
        assert!(message.contains("sima.toml"), "names the config: {message}");
        assert!(message.contains("stub.v1"), "names the format: {message}");
        assert!(message.contains("sdk"), "names the key: {message}");
        assert!(message.contains("rust"), "names the value: {message}");
        assert!(
            message.contains("python"),
            "names what it does vend: {message}"
        );
    }

    #[test]
    fn an_env_entry_is_a_variable_name_and_an_empty_one_is_refused() {
        let message = rejection(&format!(
            r#"{BASE}
            [domain."stub.v1"]
            binary = "/opt/acme/worker"
            env = ["ACME_ASSETS", ""]
            "#
        ));
        assert!(message.contains("env"), "{message}");
        assert!(message.contains(r#""""#), "names the value: {message}");
    }

    #[test]
    fn an_absent_answer_deadline_leaves_every_protocol_wait_unbounded() {
        // The key is optional and absent is the state every search had before
        // one existed: a wait for as long as the peer lives.
        assert_eq!(
            load_text(BASE)
                .expect("the config loads")
                .execution
                .answer_timeout,
            Duration::MAX
        );
    }

    #[test]
    fn the_answer_deadline_reaches_the_execution_settings() {
        let text = BASE.replace(
            "max_attempts = 3",
            "max_attempts = 3\nanswer_timeout_ms = 120000",
        );
        assert_eq!(
            load_text(&text)
                .expect("the config loads")
                .execution
                .answer_timeout,
            Duration::from_millis(120_000)
        );
    }

    #[test]
    fn a_zero_answer_deadline_is_taken_as_written() {
        // The attempt deadline takes any millisecond value the file states and
        // enforces it; this one follows the same rule rather than a second one.
        let text = BASE.replace(
            "max_attempts = 3",
            "max_attempts = 3\nanswer_timeout_ms = 0",
        );
        assert_eq!(
            load_text(&text)
                .expect("the config loads")
                .execution
                .answer_timeout,
            Duration::ZERO
        );
    }

    #[test]
    fn a_negative_answer_deadline_is_refused_like_a_negative_attempt_deadline() {
        for key in ["answer_timeout_ms", "attempt_timeout_ms"] {
            let text = BASE.replace("max_attempts = 3", &format!("max_attempts = 3\n{key} = -1"));
            assert!(rejection(&text).contains(key), "{key}");
        }
    }

    #[test]
    fn a_program_silent_past_the_answer_deadline_fails_the_load_naming_it() {
        // The registry's sessions open while the config resolves, so the
        // deadline is read before them: a program that never answers is a
        // config failure naming what was awaited.
        let dir = tempfile::tempdir().expect("temp dir");
        let program = dir.path().join("wedged.sh");
        // `exec` puts the sleep in the shell's place, so the process holding
        // the pipes is the one the expiry's kill reaches.
        fs::write(&program, "#!/bin/sh\nexec sleep 300\n").expect("write the program");
        fs::set_permissions(
            &program,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .expect("make it executable");
        let path = write_config(
            dir.path(),
            "sima.toml",
            &format!(
                r#"{BASE}
                [domain."stub.v1"]
                binary = "{}"
                "#,
                program.display()
            )
            .replace(
                "max_attempts = 3",
                "max_attempts = 3\nanswer_timeout_ms = 300",
            ),
        );
        let started = std::time::Instant::now();
        let Err(error) = load(&path) else {
            panic!("expected a program that never answers to fail the load");
        };
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "{:?}",
            started.elapsed()
        );
        let message = error.to_string();
        assert!(message.contains("wedged.sh"), "{message}");
        assert!(message.contains("Ready"), "names the answer: {message}");
    }

    // ---- What travels when the search moves: payload, install, payload_digest ----

    /// A `[domain."stub.v1"]` entry over the built worker, carrying `keys`.
    /// The directory the config sits in is handed to `keys` so a test can
    /// place a payload beside the file it is declared in.
    fn payload_entry(keys: impl Fn(&Path) -> String) -> Result<LoadedConfig> {
        let dir = tempfile::tempdir().expect("temp dir");
        let text = format!(
            r#"{BASE}
            [domain."stub.v1"]
            binary = "{}"
            {}
            "#,
            crate::fixtures::built_worker().display(),
            keys(dir.path()),
        );
        load(&write_config(dir.path(), "sima.toml", &text))
    }

    /// The validation message `payload_entry` is rejected with.
    fn payload_rejection(keys: impl Fn(&Path) -> String) -> String {
        match payload_entry(keys) {
            Err(Error::Validation(message)) => message,
            other => panic!("expected a validation error, got {other:?}"),
        }
    }

    /// Writes an executable single-file payload under `dir` and answers its
    /// path.
    fn file_payload(dir: &Path) -> PathBuf {
        let path = dir.join("program.sh");
        fs::write(&path, "#!/bin/sh\nexec true\n").expect("write the payload");
        path
    }

    /// Writes a directory payload of one file under `dir` and answers its path.
    fn dir_payload(dir: &Path) -> PathBuf {
        let path = dir.join("tree");
        fs::create_dir_all(path.join("assets")).expect("create the payload tree");
        fs::write(path.join("assets/weights.bin"), b"w").expect("write a payload file");
        path
    }

    /// Writes an install script under `dir` and answers its path.
    fn install_script(dir: &Path) -> PathBuf {
        let path = dir.join("install.sh");
        fs::write(&path, "#!/bin/sh\nexit 0\n").expect("write the script");
        path
    }

    #[test]
    fn a_single_file_payload_needs_no_install_script() -> Result<()> {
        // The file itself is the entry point, so a script that would only copy
        // it into place states nothing the convention does not.
        let loaded = payload_entry(|dir| format!("payload = {:?}", file_payload(dir).display()))?;
        let routed = loaded
            .domains
            .routed(&FormatId::new("stub.v1").expect("format id"))
            .expect("the declared format is routed to its program");
        let payload = routed.payload.expect("the entry states what travels");
        assert!(payload.payload.ends_with("program.sh"));
        assert_eq!(payload.install, None);
        Ok(())
    }

    #[test]
    fn a_payload_and_an_install_script_reach_the_routed_program() -> Result<()> {
        let loaded = payload_entry(|dir| {
            format!(
                "payload = {:?}\ninstall = {:?}",
                dir_payload(dir).display(),
                install_script(dir).display(),
            )
        })?;
        let routed = loaded
            .domains
            .routed(&FormatId::new("stub.v1").expect("format id"))
            .expect("the declared format is routed to its program");
        let payload = routed.payload.expect("the entry states what travels");
        assert!(payload.payload.ends_with("tree"));
        assert_eq!(
            payload.install.as_deref().and_then(Path::file_name),
            Some(std::ffi::OsStr::new("install.sh"))
        );
        Ok(())
    }

    #[test]
    fn a_directory_payload_without_an_install_script_is_refused() {
        // A tree has no entry point by convention: which of its files runs is
        // what the script decides.
        let message =
            payload_rejection(|dir| format!("payload = {:?}", dir_payload(dir).display()));
        assert!(message.contains("install"), "names the key: {message}");
        assert!(message.contains("stub.v1"), "names the format: {message}");
        assert!(message.contains("sima.toml"), "names the config: {message}");
    }

    #[test]
    fn an_install_script_without_a_payload_is_refused() {
        // The script installs the payload; with none declared it installs
        // nothing.
        let message =
            payload_rejection(|dir| format!("install = {:?}", install_script(dir).display()));
        assert!(message.contains("install"), "names the key: {message}");
        assert!(message.contains("payload"), "names the key it needs");
    }

    #[test]
    fn a_payload_beside_a_payload_digest_is_refused() {
        // Two sources for one program: one to ingest here, one already in the
        // store.
        let message = payload_rejection(|dir| {
            format!(
                "payload = {:?}\npayload_digest = \"{}\"",
                file_payload(dir).display(),
                "ab".repeat(32),
            )
        });
        assert!(message.contains("payload"), "{message}");
        assert!(message.contains("payload_digest"), "{message}");
    }

    #[test]
    fn an_install_script_beside_a_payload_digest_is_refused() {
        // The manifest the digest names carries the script, so a second one
        // here would be a second answer to the same question.
        let message = payload_rejection(|dir| {
            format!(
                "install = {:?}\npayload_digest = \"{}\"",
                install_script(dir).display(),
                "ab".repeat(32),
            )
        });
        assert!(message.contains("install"), "{message}");
        assert!(message.contains("payload_digest"), "{message}");
    }

    #[test]
    fn a_payload_that_is_not_there_is_refused_naming_the_path() {
        let message =
            payload_rejection(|dir| format!("payload = {:?}", dir.join("absent.py").display()));
        assert!(message.contains("payload"), "{message}");
        assert!(message.contains("absent.py"), "names the path: {message}");
    }

    #[test]
    fn an_install_script_that_is_not_there_is_refused_naming_the_path() {
        let message = payload_rejection(|dir| {
            format!(
                "payload = {:?}\ninstall = {:?}",
                dir_payload(dir).display(),
                dir.join("absent.sh").display(),
            )
        });
        assert!(message.contains("install"), "{message}");
        assert!(message.contains("absent.sh"), "names the path: {message}");
    }

    #[test]
    fn an_install_that_names_a_directory_is_refused() {
        // It is search as a shell script, so it is one file.
        let message = payload_rejection(|dir| {
            format!(
                "payload = {:?}\ninstall = {:?}",
                dir_payload(dir).display(),
                dir_payload(dir).display(),
            )
        });
        assert!(message.contains("install"), "{message}");
        assert!(message.contains("regular file"), "{message}");
    }

    #[test]
    fn a_payload_digest_that_is_not_a_hash_is_refused_naming_the_key() {
        for digest in ["", "ab", &"zz".repeat(32), &"ab".repeat(33)] {
            let message = payload_rejection(|_| format!("payload_digest = {digest:?}"));
            assert!(message.contains("payload_digest"), "{digest:?}: {message}");
        }
    }

    #[test]
    fn the_two_paths_resolve_against_the_config_file_s_directory() -> Result<()> {
        // A relative path in a config is the file's, never the working
        // directory's — the rule `binary` and `store` already follow.
        let dir = tempfile::tempdir().expect("temp dir");
        let nested = dir.path().join("configs");
        fs::create_dir(&nested).expect("create nested dir");
        dir_payload(&nested);
        install_script(&nested);
        let text = format!(
            r#"{BASE}
            [domain."stub.v1"]
            binary = "{}"
            payload = "./tree"
            install = "./install.sh"
            "#,
            crate::fixtures::built_worker().display(),
        );
        let loaded = load(&write_config(&nested, "sima.toml", &text))?;
        let routed = loaded
            .domains
            .routed(&FormatId::new("stub.v1").expect("format id"))
            .expect("the declared format is routed to its program");
        let payload = routed.payload.expect("the entry states what travels");
        assert_eq!(payload.payload, nested.join("tree"));
        assert_eq!(
            payload.install.as_deref(),
            Some(nested.join("install.sh").as_path())
        );
        Ok(())
    }

    #[test]
    fn neither_payload_key_touches_the_search_id() {
        // Both are operational: they decide how the program reaches another
        // machine, never what the search computes.
        let dir = tempfile::tempdir().expect("temp dir");
        let entry = format!(
            r#"{BASE}
            [domain."stub.v1"]
            binary = "{}"
            "#,
            crate::fixtures::built_worker().display(),
        );
        let with_payload = format!(
            "{entry}payload = {:?}\ninstall = {:?}\n",
            dir_payload(dir.path()).display(),
            install_script(dir.path()).display(),
        );
        let plain = load(&write_config(dir.path(), "plain.toml", &entry))
            .expect("the config loads")
            .search
            .id();
        let carrying = load(&write_config(dir.path(), "carrying.toml", &with_payload))
            .expect("the config loads")
            .search
            .id();
        assert_eq!(plain, carrying, "what travels is operational");
        assert_eq!(plain, id_of(BASE), "and so is the entry itself");
    }

    // ---- A payload digest is installed where the config resolves ----

    /// A far side: a directory holding the store a payload was ingested into,
    /// and the digest that names it. `build` writes the payload beside the
    /// config and answers what travels.
    struct FarSide {
        dir: tempfile::TempDir,
        digest: Hash,
    }

    impl FarSide {
        /// A far side whose store holds the payload `build` describes.
        fn new(build: impl Fn(&Path) -> PayloadSpec) -> FarSide {
            let dir = tempfile::tempdir().expect("temp dir");
            let store = Store::open(dir.path().join("store")).expect("open the store");
            let digest =
                crate::payload::ingest(&store, &build(dir.path())).expect("ingest the payload");
            FarSide { dir, digest }
        }

        /// Loads the config that installs this payload, as the far `sima search`
        /// does.
        fn load(&self) -> Result<LoadedConfig> {
            self.load_digest(&self.digest)
        }

        /// Loads a config naming `digest`, for the tests that change what the
        /// entry points at.
        fn load_digest(&self, digest: &Hash) -> Result<LoadedConfig> {
            load(&self.place(digest))
        }

        /// Writes the config naming `digest` and answers its path. Separate
        /// from the load, so several loaders can read one file rather than
        /// racing to write it.
        fn place(&self, digest: &Hash) -> PathBuf {
            write_config(
                self.dir.path(),
                "sima.toml",
                &format!(
                    r#"{BASE}
                    [domain."stub.v1"]
                    binary = "{}"
                    payload_digest = "{digest}"
                    "#,
                    crate::payload::relative_entry_point("stub.v1"),
                ),
            )
        }

        /// The entry point the install is contracted to leave.
        fn entry_point(&self) -> PathBuf {
            self.dir
                .path()
                .join(".sima/program/stub.v1/installed/program")
        }

        /// How many times an install script that counts itself has run.
        fn installs(&self) -> usize {
            std::fs::read_to_string(self.dir.path().join("installs"))
                .map(|text| text.lines().count())
                .unwrap_or(0)
        }
    }

    /// Writes a program that answers for `stub.v1` at `path`: a wrapper around
    /// the built worker, which is small enough to ingest and travel.
    fn wrapper(path: &Path) -> PathBuf {
        fs::create_dir_all(path.parent().expect("a parent")).expect("create the parent");
        fs::write(
            path,
            format!(
                "#!/bin/sh\nexec {} \"$@\"\n",
                crate::fixtures::built_worker().display()
            ),
        )
        .expect("write the wrapper");
        fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .expect("make it executable");
        path.to_path_buf()
    }

    /// Writes `script` at `path`, as an install script travels.
    fn script(path: &Path, script: &str) -> PathBuf {
        fs::write(path, script).expect("write the script");
        path.to_path_buf()
    }

    /// A directory payload of one wrapper, installed by a script that records
    /// each execution in `<dir>/installs` and puts the wrapper where the convention
    /// says. `extra` is appended to the tree so two payloads can differ.
    fn counted_payload(dir: &Path, extra: &str) -> PayloadSpec {
        wrapper(&dir.join("src/wrapper.sh"));
        fs::write(dir.join("src/note"), extra).expect("write the note");
        PayloadSpec {
            payload: dir.join("src"),
            install: Some(script(
                &dir.join("install.sh"),
                &format!(
                    "#!/bin/sh\n\
                     set -e\n\
                     echo ran >> {installs:?}\n\
                     cp \"$SIMA_PAYLOAD_DIR/wrapper.sh\" \"$SIMA_INSTALL_DIR/program\"\n\
                     chmod 755 \"$SIMA_INSTALL_DIR/program\"\n",
                    installs = dir.join("installs").display(),
                ),
            )),
        }
    }

    #[test]
    fn a_config_routing_no_payload_opens_no_store_and_writes_nothing() -> Result<()> {
        // The load of an ordinary config touches the disk only to read the
        // file: opening a store creates it, and a program tree is a directory
        // nothing asked for.
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(dir.path(), "sima.toml", BASE);
        load(&path)?;
        assert!(!dir.path().join("store").exists(), "no store was opened");
        assert!(!dir.path().join(".sima").exists(), "nothing was installed");
        Ok(())
    }

    #[test]
    fn a_payload_digest_installs_the_program_the_binary_names() -> Result<()> {
        // A single-file payload is its own entry point, so it lands at the
        // convention's path, executable, with no script involved.
        let far = FarSide::new(|dir| PayloadSpec {
            payload: wrapper(&dir.join("src/wrapper.sh")),
            install: None,
        });
        let loaded = far.load()?;
        let entry = far.entry_point();
        let mode = fs::metadata(&entry)
            .expect("the entry point")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o755, "the entry point runs");
        // The load spawned it and it answered, which is what routing it proves.
        assert_eq!(
            loaded
                .domains
                .routed(&FormatId::new("stub.v1").expect("format id"))
                .expect("the format is routed to the installed program")
                .binary,
            entry,
            "the entry the config names is the one that answered"
        );
        Ok(())
    }

    #[test]
    fn a_payload_digest_reaches_every_spawn_the_loaded_config_makes() -> Result<()> {
        // The whole path from the key: the load installs the tree the digest
        // names, then states that digest to every process it spawns from the
        // entry point, so each worker answers it back at its handshake.
        let far = FarSide::new(|dir| PayloadSpec {
            payload: wrapper(&dir.join("src/wrapper.sh")),
            install: None,
        });
        let loaded = far.load()?;
        let format = FormatId::new("stub.v1").expect("format id");
        let SpawnPolicy::Explicit { assign, .. } = loaded.domains.source(&format).spawn_policy()
        else {
            panic!("a routed format spawns on an explicit surface");
        };
        assert_eq!(
            assign,
            vec![(
                "SIMA_PROGRAM_DIGEST".to_string(),
                std::ffi::OsString::from(far.digest.to_string()),
            )]
        );
        Ok(())
    }

    #[test]
    fn an_install_script_builds_the_entry_point_from_the_payload() -> Result<()> {
        let far = FarSide::new(|dir| counted_payload(dir, "first"));
        far.load()?;
        assert!(far.entry_point().is_file());
        assert_eq!(far.installs(), 1);
        // The materialized payload stays where it was put, so an installed
        // wrapper may point into it.
        assert!(
            far.dir
                .path()
                .join(".sima/program/stub.v1/payload/wrapper.sh")
                .is_file()
        );
        Ok(())
    }

    #[test]
    fn the_stamp_makes_a_second_load_install_nothing() -> Result<()> {
        // What a reattach, a status query, and a follow attach all rest on.
        let far = FarSide::new(|dir| counted_payload(dir, "first"));
        far.load()?;
        far.load()?;
        far.load()?;
        assert_eq!(far.installs(), 1, "the stamp answered the later loads");
        Ok(())
    }

    #[test]
    fn a_changed_payload_reinstalls_exactly_once() -> Result<()> {
        let far = FarSide::new(|dir| counted_payload(dir, "first"));
        far.load()?;
        // The same tree with one file's content changed: a second digest over
        // the same store.
        let store = Store::open(far.dir.path().join("store"))?;
        let changed = crate::payload::ingest(&store, &counted_payload(far.dir.path(), "second"))?;
        assert_ne!(changed, far.digest);
        far.load_digest(&changed)?;
        far.load_digest(&changed)?;
        assert_eq!(far.installs(), 2, "once for the change, and no more");
        Ok(())
    }

    #[test]
    fn a_reinstall_leaves_nothing_of_the_tree_it_replaced() -> Result<()> {
        // The previous payload's files are not the new program's, and a
        // wrapper that still found them would run a build nobody asked for.
        let leaves = |dir: &Path, extra: &str| -> PayloadSpec {
            wrapper(&dir.join("src/wrapper.sh"));
            fs::write(dir.join("src/note"), extra).expect("write the note");
            PayloadSpec {
                payload: dir.join("src"),
                install: Some(script(
                    &dir.join("install.sh"),
                    &format!(
                        "#!/bin/sh\n\
                         set -e\n\
                         cp \"$SIMA_PAYLOAD_DIR/wrapper.sh\" \"$SIMA_INSTALL_DIR/program\"\n\
                         chmod 755 \"$SIMA_INSTALL_DIR/program\"\n\
                         {extra}\n",
                    ),
                )),
            }
        };
        let far = FarSide::new(|dir| leaves(dir, "touch \"$SIMA_INSTALL_DIR/leftover\""));
        far.load()?;
        let leftover = far
            .dir
            .path()
            .join(".sima/program/stub.v1/installed/leftover");
        let stale_payload = far.dir.path().join(".sima/program/stub.v1/payload/note");
        assert!(leftover.is_file(), "the first install left it");
        assert_eq!(
            fs::read_to_string(&stale_payload).expect("the note"),
            "touch \"$SIMA_INSTALL_DIR/leftover\""
        );

        let store = Store::open(far.dir.path().join("store"))?;
        let changed = crate::payload::ingest(&store, &leaves(far.dir.path(), "true"))?;
        far.load_digest(&changed)?;
        assert!(!leftover.exists(), "the replaced tree left nothing behind");
        assert_eq!(
            fs::read_to_string(&stale_payload).expect("the note"),
            "true",
            "and the materialized payload is the new one"
        );
        Ok(())
    }

    #[test]
    fn an_install_that_exits_non_zero_fails_the_load_naming_what_it_said() {
        let far = FarSide::new(|dir| PayloadSpec {
            payload: wrapper(&dir.join("src/wrapper.sh")),
            install: Some(script(
                &dir.join("install.sh"),
                "#!/bin/sh\necho 'no compiler on this machine' >&2\nexit 3\n",
            )),
        });
        let Err(Error::Validation(message)) = far.load() else {
            panic!("expected a failing install to fail the load");
        };
        for named in [
            "install.sh",
            "exit status: 3",
            "install.log",
            "no compiler on this machine",
            "stub.v1",
        ] {
            assert!(message.contains(named), "{named} is missing from {message}");
        }
    }

    #[test]
    fn an_install_that_leaves_no_entry_point_fails_naming_the_contract() {
        let far = FarSide::new(|dir| PayloadSpec {
            payload: wrapper(&dir.join("src/wrapper.sh")),
            install: Some(script(
                &dir.join("install.sh"),
                "#!/bin/sh\nmkdir -p \"$SIMA_INSTALL_DIR/lib\"\nexit 0\n",
            )),
        });
        let Err(Error::Validation(message)) = far.load() else {
            panic!("expected an install leaving no program to fail the load");
        };
        assert!(
            message.contains("SIMA_INSTALL_DIR/program"),
            "names the contract: {message}"
        );
    }

    #[test]
    fn a_payload_digest_the_store_lacks_fails_the_load_naming_it() {
        let far = FarSide::new(|dir| PayloadSpec {
            payload: wrapper(&dir.join("src/wrapper.sh")),
            install: None,
        });
        let absent = sima_core::hash_bytes(b"a payload nobody pushed");
        let Err(Error::Validation(message)) = far.load_digest(&absent) else {
            panic!("expected a digest the store lacks to fail the load");
        };
        assert!(message.contains(&absent.to_string()), "{message}");
    }

    #[test]
    fn two_concurrent_loads_build_one_tree_and_both_succeed() -> Result<()> {
        // A `sima search` and a `sima status` can load one config at once. The
        // lock is what makes the second wait for the tree rather than read a
        // half-built one.
        let far = FarSide::new(|dir| counted_payload(dir, "first"));
        let path = far.place(&far.digest);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4).map(|_| scope.spawn(|| load(&path))).collect();
            for handle in handles {
                handle.join().expect("the load thread")?;
            }
            Ok::<(), Error>(())
        })?;
        assert_eq!(far.installs(), 1, "one tree between them");
        assert!(far.entry_point().is_file());
        Ok(())
    }

    #[test]
    fn a_format_id_that_would_name_another_directory_is_refused() {
        // A format id admits `.` and `..`, and the tree is keyed by it, so the
        // id is held to the rule a manifest path is held to: a tree under a
        // config's directory, and never one beside it.
        //
        // The digest is one the store holds, so the refusal cannot be the
        // materialization failing for want of the payload.
        let far = FarSide::new(|dir| PayloadSpec {
            payload: wrapper(&dir.join("src/wrapper.sh")),
            install: None,
        });
        for name in [".", ".."] {
            let text = format!(
                r#"{BASE}
                [domain."{name}"]
                binary = "/bin/true"
                payload_digest = "{}"
                "#,
                far.digest,
            );
            let path = write_config(far.dir.path(), "sima.toml", &text);
            let Err(Error::Validation(message)) = load(&path) else {
                panic!("the format id {name:?} must be refused");
            };
            assert!(
                message.contains("payload path"),
                "{name:?} is refused as a path: {message}"
            );
        }
    }

    #[test]
    fn an_env_entry_carrying_a_value_is_refused() {
        // A name, never an assignment: the value comes from the
        // orchestrator's own environment, so writing one here would be a
        // second, silent source of it.
        let message = rejection(&format!(
            r#"{BASE}
            [domain."stub.v1"]
            binary = "/opt/acme/worker"
            env = ["ACME_ASSETS=/opt/acme"]
            "#
        ));
        assert!(message.contains("env"), "{message}");
        assert!(
            message.contains("ACME_ASSETS=/opt/acme"),
            "names the value: {message}"
        );
    }

    /// Writes an exec-only config and its directory payload, then returns the
    /// path. The install path is resolved against the config directory.
    fn exec_config(dir: &Path, extra: &str) -> PathBuf {
        let payload = dir.join("payload");
        fs::create_dir(&payload).expect("create payload");
        fs::write(payload.join("main.rs"), "fn main() {}\n").expect("write payload");
        fs::write(dir.join("install.sh"), "#!/bin/sh\nexit 0\n").expect("write install");
        write_config(
            dir,
            "exec.toml",
            &format!(
                r#"
                [exec]
                host = "bench"
                command = "RUST_LOG=info cargo test --release"
                payload = "payload"
                install = "install.sh"
                outputs = ["reports/*.html", "*.pfm"]
                fetch_to = "results"

                [budget]
                max_spend_usd = 2.5

                [host.bench]
                provider = "stub"
                image = "specialized:latest"
                bootstrap_sima = true
                disk_gb = 64
                env = {{ NVIDIA_DRIVER_CAPABILITIES = "all" }}
                {extra}
                "#
            ),
        )
    }

    #[test]
    fn exec_load_resolves_its_job_host_payload_budget_and_output_paths() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = exec_config(dir.path(), "");
        let loaded = load_exec(&path)?;
        assert_eq!(loaded.host_name, "bench");
        assert_eq!(loaded.command, "RUST_LOG=info cargo test --release");
        assert_eq!(loaded.payload.payload, dir.path().join("payload"));
        assert_eq!(loaded.payload.install, Some(dir.path().join("install.sh")));
        assert_eq!(loaded.outputs, ["reports/*.html", "*.pfm"]);
        assert_eq!(loaded.fetch_to, dir.path().join("results"));
        assert_eq!(loaded.store, dir.path().join(".sima/store"));
        assert_eq!(loaded.budget.max_spend, Some(Cost(2_500_000)));
        assert_eq!(loaded.host.root, "~/sima");
        assert_eq!(loaded.host.binary, "sima");
        let HostForm::Rented(rented) = &loaded.host.form else {
            panic!("exec resolves a rented host");
        };
        assert_eq!(rented.image, "specialized:latest");
        assert_eq!(rented.disk_gb, 64);
        assert!(rented.bootstrap_sima);
        assert_eq!(
            rented
                .env
                .get("NVIDIA_DRIVER_CAPABILITIES")
                .map(String::as_str),
            Some("all")
        );
        Ok(())
    }

    #[test]
    fn exec_fetch_directory_defaults_beside_the_config() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = exec_config(dir.path(), "");
        let text = fs::read_to_string(&path)
            .expect("read config")
            .replace("                fetch_to = \"results\"\n", "");
        fs::write(&path, text).expect("rewrite config");
        assert_eq!(load_exec(&path)?.fetch_to, dir.path().join("exec-outputs"));
        Ok(())
    }

    #[test]
    fn exec_output_globs_cannot_escape_the_payload_root() {
        for output in ["../secret", "/etc/passwd", "reports/../../secret"] {
            let dir = tempfile::tempdir().expect("temp dir");
            let path = exec_config(dir.path(), "");
            let text = fs::read_to_string(&path).expect("read config").replace(
                "outputs = [\"reports/*.html\", \"*.pfm\"]",
                &format!("outputs = [{output:?}]"),
            );
            fs::write(&path, text).expect("rewrite config");
            let message = load_exec(&path)
                .expect_err("escaping output glob")
                .to_string();
            assert!(message.contains(output), "{message}");
            assert!(message.contains("payload root"), "{message}");
        }
    }

    #[test]
    fn exec_refuses_keys_carried_by_the_command_string() {
        for key in ["workdir = \"src\"", "env = { RUST_LOG = \"info\" }"] {
            let dir = tempfile::tempdir().expect("temp dir");
            let path = exec_config(dir.path(), "");
            let text = fs::read_to_string(&path).expect("read config").replace(
                "                host = \"bench\"",
                &format!("                host = \"bench\"\n                {key}"),
            );
            fs::write(&path, text).expect("rewrite config");
            let message = load_exec(&path).expect_err("unknown exec key").to_string();
            assert!(
                message.contains(key.split_whitespace().next().expect("the key name")),
                "{message}"
            );
        }
    }

    #[test]
    fn exec_host_must_name_one_rented_host() {
        for (replacement, expected) in [
            ("host = \"missing\"", "missing"),
            ("host = \"bench\"", "rented"),
        ] {
            let dir = tempfile::tempdir().expect("temp dir");
            let path = exec_config(dir.path(), "");
            let mut text = fs::read_to_string(&path)
                .expect("read config")
                .replace("host = \"bench\"", replacement);
            if expected == "rented" {
                text = text.replace("provider = \"stub\"", "workers = 1");
                text = text.replace("                image = \"specialized:latest\"\n", "");
                text = text.replace("                bootstrap_sima = true\n", "");
                text = text.replace("                disk_gb = 64\n", "");
                text = text.replace(
                    "                env = { NVIDIA_DRIVER_CAPABILITIES = \"all\" }\n",
                    "",
                );
            }
            fs::write(&path, text).expect("rewrite config");
            let message = load_exec(&path).expect_err("invalid exec host").to_string();
            assert!(message.contains(expected), "{message}");
        }

        let dir = tempfile::tempdir().expect("temp dir");
        let path = exec_config(dir.path(), "");
        let text = fs::read_to_string(&path)
            .expect("read config")
            .replace("                host = \"bench\"\n", "");
        fs::write(&path, text).expect("rewrite config");
        let message = load_exec(&path).expect_err("missing host").to_string();
        assert!(message.contains("host"), "{message}");
    }

    #[test]
    fn exec_directory_payload_requires_an_install_script() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = exec_config(dir.path(), "");
        let text = fs::read_to_string(&path)
            .expect("read config")
            .replace("                install = \"install.sh\"\n", "");
        fs::write(&path, text).expect("rewrite config");
        let message = load_exec(&path).expect_err("missing install").to_string();
        assert!(message.contains("install"), "{message}");
        assert!(message.contains("directory"), "{message}");
    }

    #[test]
    fn exec_only_config_loads_for_exec_and_search_names_its_missing_section() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = exec_config(dir.path(), "");
        load_exec(&path)?;
        let message = load(&path)
            .expect_err("search section is required")
            .to_string();
        assert!(message.contains("[search]"), "{message}");
        Ok(())
    }

    #[test]
    fn store_load_accepts_each_command_config_and_its_shared_override() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let exec = exec_config(dir.path(), "");
        assert_eq!(load_store(&exec)?, dir.path().join(".sima/store"));

        let text = fs::read_to_string(&exec).expect("read exec config");
        fs::write(&exec, format!("{text}\n[config]\nstore = \"s\"\n"))
            .expect("add the shared store setting");
        assert_eq!(load_store(&exec)?, dir.path().join("s"));
        assert_eq!(load_exec(&exec)?.store, dir.path().join("s"));

        let search = write_config(dir.path(), "search.toml", BASE);
        assert_eq!(load_store(&search)?, load(&search)?.store);
        Ok(())
    }

    #[test]
    fn store_load_names_an_unparsable_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_config(dir.path(), "broken.toml", "not = [toml");
        let message = load_store(&path)
            .expect_err("invalid TOML must fail")
            .to_string();
        assert!(message.contains(&path.display().to_string()), "{message}");
    }

    #[test]
    fn search_load_names_each_required_search_section() {
        for (removed, expected) in [("search", "[search]"), ("config", "[config]")] {
            let text = match removed {
                "search" => BASE
                    .split("        [config]")
                    .nth(1)
                    .map(|tail| format!("[config]{tail}"))
                    .expect("the base config section"),
                "config" => BASE
                    .split("        [config]")
                    .next()
                    .expect("the base search section")
                    .to_string(),
                _ => unreachable!(),
            };
            let message = load_text(&text).expect_err("required section").to_string();
            assert!(message.contains(expected), "{message}");
        }
    }

    #[test]
    fn rented_exec_keys_default_and_owned_entries_reject_them() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = exec_config(dir.path(), "");
        let text = fs::read_to_string(&path)
            .expect("read config")
            .replace("                bootstrap_sima = true\n", "")
            .replace(
                "                env = { NVIDIA_DRIVER_CAPABILITIES = \"all\" }\n",
                "",
            );
        fs::write(&path, text).expect("rewrite config");
        let HostForm::Rented(rented) = load_exec(&path)?.host.form else {
            panic!("rented host");
        };
        assert!(!rented.bootstrap_sima);
        assert!(rented.env.is_empty());

        for key in [
            "env = { NVIDIA_DRIVER_CAPABILITIES = \"all\" }",
            "bootstrap_sima = true",
        ] {
            let text = format!("{BASE}\n[host.owned]\nworkers = 1\n{key}\n");
            let message = rejection(&text);
            assert!(
                message.contains(key.split_whitespace().next().expect("the key name")),
                "{message}"
            );
            assert!(message.contains("machine of yours"), "{message}");
        }
        Ok(())
    }
}
