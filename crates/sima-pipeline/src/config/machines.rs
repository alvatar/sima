//! The machines a search may draw on: what a declaration resolves into, and the
//! translation that gets it there.
//!
//! A machine is declared once by name and referred to by name everywhere else.
//! What separates the two kinds is who owns it: a machine of yours is reached
//! over ssh and states its own worker layout, while a rented one names a
//! provider and states a specification, because it does not exist until the search
//! asks for it. Each kind rejects the other's keys by name, so a declaration
//! that mixes them fails saying which key belongs where.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use sima_core::{Error, Result};
use sima_provider::{Constraints, Price};

use super::file::{DeviceSection, Entry, MachineSection, OrchestratorSection, SshSection};
use super::settings::{dollars_to_micro_ceil, finite_dollars};
use crate::devices::DeviceSelector;

/// The image a machine of yours runs its workers from when its entry names
/// none.
const DEFAULT_IMAGE: &str = "localhost/sima:latest";
/// The container runtime a machine of yours uses when its entry names none.
const DEFAULT_RUNTIME: &str = "docker";
/// The image a rented machine runs when its entry names none. It carries both
/// binaries: `sima-worker` for the machine's workers, and `sima` for the
/// orchestrator of a search migrated onto it.
const DEFAULT_RENTED_IMAGE: &str = "ghcr.io/alvatar/sima:latest";
/// The disk a rented machine is provisioned with when its entry names none.
const DEFAULT_DISK_GB: u64 = 32;
/// How long a rental waits for an instance to become reachable when its entry
/// names no timeout: the provider host pulls the image before the container
/// exists, which takes minutes.
pub(crate) const DEFAULT_READY_TIMEOUT_MS: u64 = 600_000;
/// How often a rental polls an instance for readiness when its entry names no
/// interval.
pub(crate) const DEFAULT_READY_POLL_MS: u64 = 5_000;
/// Where a migrated search's directory goes on a machine whose entry names no
/// root.
const DEFAULT_ROOT: &str = "~/sima";
/// The `sima` binary a migrated search is driven by on a machine whose entry names
/// none.
const DEFAULT_BINARY: &str = "sima";

/// This machine: the worker layout a search executes on by default, and the host a
/// migration moves the search onto.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Orchestrator {
    /// The `[host.*]` entry `sima migrate` moves the search onto, or `None` for a
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
    /// Where a migrated search's directory goes on this machine.
    pub root: String,
    /// The `sima` binary that drives a migrated search on this machine.
    pub binary: String,
}

/// One declared group of identical machines.
#[derive(Debug, Clone, PartialEq)]
pub struct HostClass {
    /// How the machines are obtained and what they run.
    pub form: HostClassForm,
    /// Where a migrated search's directory goes on these machines.
    pub root: String,
    /// The `sima` binary that drives a migrated search on these machines.
    pub binary: String,
}

/// A host is a machine you have or one rented for the search. The two are
/// exclusive by construction, so nothing downstream asks which keys were given.
#[derive(Debug, Clone, PartialEq)]
pub enum HostForm {
    /// A machine of yours, reached over ssh.
    Owned(OwnedHost),
    /// A machine rented for the search.
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
/// until the search asks for it, so its devices come from the enumeration probe.
#[derive(Debug, Clone, PartialEq)]
pub struct Rented {
    /// The control-plane backend to acquire through.
    pub provider: ProviderId,
    /// The image each instance runs: `sima-worker` for its workers, and the
    /// `sima` a search migrated onto it is driven by.
    pub image: String,
    /// Environment assigned when the provider creates the instance.
    pub env: BTreeMap<String, String>,
    /// Whether exec may install the static sima binary on this machine.
    pub bootstrap_sima: bool,
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
    /// Verbatim flags for the container-search command — GPU access and the like.
    pub run_args: Vec<String>,
}

/// A machine's worker layout: a plain count, or one entry per device class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pool {
    /// A plain worker count, naming no device.
    Workers(usize),
    /// One selector per device class; the pool is their sum. The selectors stay
    /// unresolved until the search starts, over the hardware they name.
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

/// The set of machines a search may draw on, listed by name. A collective, so it
/// never declares an element.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fleet {
    /// The hosts and host classes the search may use, in the order listed.
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
    ///
    /// Pointed at a machine that is really there by the `SIMA_STUB_SSH`
    /// environment variable — `user@host:port` — its instances report that
    /// endpoint and are reached over ssh instead, which is how the ssh path is
    /// exercised against a throwaway server without renting anything. The
    /// channel is an environment variable rather than a key here because a key
    /// valid only under one provider would be an exception carved into a schema
    /// that has none.
    Stub,
}

impl ProviderId {
    /// The id the backend answers to, and the one a ledger record carries. It
    /// is what the provider registry dispatches on, so config and ledger name
    /// one backend the same way.
    pub fn as_str(self) -> &'static str {
        match self {
            ProviderId::Vast => sima_provider_vast::PROVIDER_ID,
            ProviderId::Stub => sima_provider::STUB_PROVIDER_ID,
        }
    }
}

/// What a rented class does when it cannot acquire its full declared count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillPolicy {
    /// The full count or the search fails before any task runs, tearing down
    /// whatever was acquired.
    Strict,
    /// Run with what was acquired, at least one machine.
    BestEffort,
}

/// Validates the `[orchestrator]` section and resolves it. This machine takes an
/// owned machine's worker-side keys plus `migrate`; the keys that would describe
/// somewhere else are rejected naming why.
pub(super) fn resolve_orchestrator(
    path: &Path,
    section: Option<OrchestratorSection>,
) -> Result<Orchestrator> {
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
        (
            "root",
            section.root.is_some(),
            "the search already lives here",
        ),
        (
            "binary",
            section.binary.is_some(),
            "the search is already driven by this binary",
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
pub(super) fn resolve_host(path: &Path, name: &str, mut section: MachineSection) -> Result<Host> {
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
pub(super) fn resolve_host_class(
    path: &Path,
    name: &str,
    mut section: MachineSection,
) -> Result<HostClass> {
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

/// Where a migrated search's directory goes on a machine and which `sima` drives it
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
        ("env", section.env.is_some()),
        ("bootstrap_sima", section.bootstrap_sima.is_some()),
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
/// every machine has one and the runtime and the search flags are always
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
/// is asked for a container — so the runtime and the search flags would describe a
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
        env: section.env.unwrap_or_default(),
        bootstrap_sima: section.bootstrap_sima.unwrap_or(false),
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
