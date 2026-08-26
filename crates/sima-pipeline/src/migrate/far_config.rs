//! The far side's directory and the config written into it.
//!
//! Two things a migration needs before it can start anything on the
//! destination: where the run lives there, and what the run reads when it does.
//!
//! **The directory is derived from the run id**, so a reattaching migration
//! finds it without remembering anything and two runs on one machine never
//! collide.
//!
//! **The config is the local one with everything about here removed.** `[run]`
//! travels verbatim, so the run id is preserved by construction — identity is
//! derived from the translated `RunConfig`, not from the file text, and a
//! round trip through the parser and serializer preserves every value.
//! `[config]` travels with its store path rewritten to a relative one, which
//! the load resolves against the config file's own directory. Everything
//! naming a machine is dropped: this machine's own worker layout names hardware
//! the destination does not have, and the declared hosts, classes, and fleet
//! name machines reachable from here, which says nothing about what the
//! destination can reach.
//!
//! `[budget]` travels in one key and only to a machine of yours:
//! `max_wall_clock_ms` bounds the run's own computing, and is worth keeping
//! only where no bill runs against the time it bounds. `max_spend_usd` needs
//! the provider key that never travels.
//!
//! **The destination's form is what the synthesis takes**, so every decision
//! the form settles is made here rather than by the caller handing in a shape
//! already derived from it: the form answers both what the far
//! `[orchestrator]` states and whether the wall-clock ceiling is carried.
//!
//! The far side therefore declares no machine beyond itself. It rents nothing,
//! whatever the local config declares — renting needs the provider key, and the
//! key never leaves this machine — so a run drawing on four rented machines
//! while driven from here executes on the destination alone once moved.

use std::collections::BTreeMap;
use std::path::Path;

use sima_core::{Error, Hash, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::RunId;

use crate::config::{HostForm, OwnedHost, Pool};
use crate::devices::usable;
use crate::payload::relative_entry_point;
use crate::sdk::Sdk;

/// The default `sima.toml` name the far side's `sima run` is pointed at.
const CONFIG_FILE: &str = "sima.toml";
/// The store path the synthesized config names, resolved by the load against
/// the config file's own directory.
const FAR_STORE: &str = "./store";

/// Where a migrated run lives on its destination.
///
/// ```text
/// <root>/<64-hex run id>/
///     sima.toml       the synthesized config
///     store/          the far side's store
///     run.log         the far-side `sima run` stdout and stderr
///     run.pid         the far-side `sima run` process id
/// ```
///
/// The paths are the destination's, not this machine's, so they are strings
/// rather than `PathBuf`s: `root` defaults to `~/sima-runs`, whose tilde the
/// far side's own shell expands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FarLayout {
    /// The run's directory on the destination.
    dir: String,
    /// The run living there, which the far store is addressed by.
    run: RunId,
}

impl FarLayout {
    /// The layout for `run` under a destination's `root`.
    pub(crate) fn new(root: &str, run: &RunId) -> FarLayout {
        FarLayout {
            dir: format!("{}/{run}", root.trim_end_matches('/')),
            run: *run,
        }
    }

    /// The run's directory.
    pub(crate) fn dir(&self) -> &str {
        &self.dir
    }

    /// The synthesized config.
    pub(crate) fn config(&self) -> String {
        format!("{}/{CONFIG_FILE}", self.dir)
    }

    /// The far side's store, which `sima sync-serve` is pointed at directly:
    /// the synthesized config names it relative to its own directory, so the
    /// path is derivable here without reading that config back.
    pub(crate) fn store(&self) -> String {
        format!("{}/{}", self.dir, FAR_STORE.trim_start_matches("./"))
    }

    /// The run the far side is driving, which addresses its store.
    pub(crate) fn run(&self) -> &RunId {
        &self.run
    }

    /// The far run's journal, inside the far store.
    ///
    /// Where a journal sits under a store root is the store's layout, so the
    /// path is derived through [`sima_store::journal_path`] rather than
    /// restated here. The join is textual: the far root is a path on the
    /// destination, which only the far shell resolves.
    pub(crate) fn journal(&self) -> String {
        sima_store::journal_path(Path::new(&self.store()), &self.run)
            .to_string_lossy()
            .into_owned()
    }

    /// The far-side `sima run` process id, what a second invocation reads to
    /// tell a run still going from one that ended.
    pub(crate) fn pid(&self) -> String {
        format!("{}/run.pid", self.dir)
    }

    /// The far-side `sima run` stdout and stderr.
    pub(crate) fn log(&self) -> String {
        format!("{}/run.log", self.dir)
    }
}

/// What the far side's `[orchestrator]` is built from, decided by the
/// destination's form.
enum FarWorkers<'a> {
    /// A machine of yours: its own container and worker layout, moved onto the
    /// far side's `[orchestrator]`, which makes them a container pool on the
    /// machine the config now sits on. Nothing is probed — the operator wrote
    /// the layout down.
    Declared(&'a OwnedHost),
    /// A rented machine: plain local workers derived from what the enumeration
    /// probe reported. There is no container to nest inside, since ssh lands
    /// within the instance's own container.
    Probed(&'a [DeviceInfo]),
}

impl<'a> FarWorkers<'a> {
    /// The layout a destination's form calls for. A machine of yours states
    /// one; a rented one has the probe answer for it.
    fn for_form(form: &'a HostForm, probed: &'a [DeviceInfo]) -> FarWorkers<'a> {
        match form {
            HostForm::Owned(owned) => FarWorkers::Declared(owned),
            HostForm::Rented(_) => FarWorkers::Probed(probed),
        }
    }
}

/// How the far side answers for a format this machine routes to a program of
/// its own: the payload already ingested here, which the destination
/// materializes and installs at load.
///
/// The far entry states a digest rather than a payload, so the destination has
/// nothing to ingest and no path of this machine's to resolve.
pub(crate) struct Registration {
    /// The format the entry answers for.
    pub(crate) format: String,
    /// The manifest object the push carries over.
    pub(crate) payload_digest: Hash,
    /// The variable names the local entry declared. They travel by name alone:
    /// each value comes from the machine the program ends up running on.
    pub(crate) env: Vec<String>,
    /// The SDK the local entry declared, which travels as the declaration it
    /// is: the destination's own binary vends the package, so nothing of it
    /// crosses the wire.
    pub(crate) sdk: Option<Sdk>,
}

/// The config the far side runs, synthesized from the local config's own text.
///
/// Working from the file text rather than the loaded value is what preserves
/// `[run]` exactly: the section is carried across as a parsed value and never
/// re-derived, so no translation this crate performs can perturb the run id.
///
/// `form` is the destination's, and decides both what the far
/// `[orchestrator]` states and whether `[budget]` carries the wall-clock
/// ceiling; `probed` is what the enumeration reported there, which only a
/// rented destination's layout is built from.
///
/// `registration` is present exactly when the run's format is served by a
/// program this machine routes it to; a format this build answers carries
/// none, and the far config then declares no `[domain.*]` table at all.
pub(crate) fn far_config(
    local_text: &str,
    form: &HostForm,
    probed: &[DeviceInfo],
    registration: Option<&Registration>,
) -> Result<String> {
    let local: toml::Table = toml::from_str(local_text)
        .map_err(|e| Error::Validation(format!("the local config no longer parses: {e}")))?;

    let mut far = toml::Table::new();
    // `[run]` verbatim: the only hashed section, carried as a value so the far
    // side's run is this run.
    let run = local
        .get("run")
        .cloned()
        .ok_or_else(|| Error::Validation("the local config names no [run] section".to_string()))?;
    far.insert("run".to_string(), run);
    far.insert(
        "config".to_string(),
        toml::Value::Table(far_settings(&local)?),
    );
    far.insert(
        "orchestrator".to_string(),
        toml::Value::Table(far_orchestrator(
            FarWorkers::for_form(form, probed),
            registration.is_some(),
        )),
    );
    if let Some(budget) = far_budget(&local, form) {
        far.insert("budget".to_string(), toml::Value::Table(budget));
    }
    if let Some(registration) = registration {
        let mut domains = toml::Table::new();
        domains.insert(
            registration.format.clone(),
            toml::Value::Table(far_domain(registration)),
        );
        far.insert("domain".to_string(), toml::Value::Table(domains));
    }

    toml::to_string_pretty(&far)
        .map_err(|e| Error::Encoding(format!("the far-side config cannot be written: {e}")))
}

/// The far side's `[domain.<format>]`: the entry point the install leaves, the
/// manifest to install it from, the variable names the program receives, and
/// the SDK it is written against.
///
/// The local entry's own `binary` and `payload` do not travel — both name
/// paths on this machine — and the destination's `binary` is the convention
/// the install fills instead.
fn far_domain(registration: &Registration) -> toml::Table {
    let mut entry = toml::Table::new();
    entry.insert(
        "binary".to_string(),
        toml::Value::String(relative_entry_point(&registration.format)),
    );
    entry.insert(
        "payload_digest".to_string(),
        toml::Value::String(registration.payload_digest.to_string()),
    );
    if !registration.env.is_empty() {
        entry.insert(
            "env".to_string(),
            toml::Value::Array(
                registration
                    .env
                    .iter()
                    .map(|name| toml::Value::String(name.clone()))
                    .collect(),
            ),
        );
    }
    // The declaration travels, not the package: the destination's own binary
    // vends the copy that matches the protocol it speaks.
    if let Some(sdk) = registration.sdk {
        entry.insert(
            "sdk".to_string(),
            toml::Value::String(sdk.as_str().to_string()),
        );
    }
    entry
}

/// The far side's `[budget]`: the wall-clock ceiling alone, and nothing at all
/// for a rented destination or a local config that states no ceiling.
///
/// A ceiling on spend never travels, because keeping it means destroying the
/// machine the run is on, and that needs the provider key — which never leaves
/// this machine. The spend ceiling stays here, assessed by an attached
/// migration.
///
/// A ceiling on time travels to a machine of yours and stops at a rented one.
/// **A rental bills by the hour rather than by use**, so a run that ends early
/// on rented hardware saves nothing: the bill is identical whether the machine
/// computes or idles, so a machine stopped and still billing costs what a
/// computing one costs and returns nothing. The ceiling is worth keeping only
/// where no bill runs against the time — a plain local run, and a machine of
/// yours. A
/// detached run on rented hardware therefore computes until `sima recall` ends
/// it, which is also what takes the rental down.
fn far_budget(local: &toml::Table, form: &HostForm) -> Option<toml::Table> {
    if matches!(form, HostForm::Rented(_)) {
        return None;
    }
    let limit = local
        .get("budget")
        .and_then(toml::Value::as_table)?
        .get("max_wall_clock_ms")?;
    let mut far = toml::Table::new();
    far.insert("max_wall_clock_ms".to_string(), limit.clone());
    Some(far)
}

/// The far side's `[config]`: the store beside its own file, and every setting
/// that describes the run rather than this machine.
///
/// `store` is the one key that does not travel — the far side names its own —
/// and every other key of the section is carried through verbatim rather than
/// picked from a list. A hand-mirrored list is a second place to edit: a key
/// added to the section and forgotten here would be silently dropped from
/// every migrated run, and nothing would say so. The local section has already
/// been through the loader, which rejects a key the section does not declare,
/// so what is copied here is exactly what the section admits.
fn far_settings(local: &toml::Table) -> Result<toml::Table> {
    let settings = local
        .get("config")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| {
            Error::Validation("the local config names no [config] section".to_string())
        })?;
    let mut far = toml::Table::new();
    far.insert(
        "store".to_string(),
        toml::Value::String(FAR_STORE.to_string()),
    );
    for (key, value) in settings {
        if key != "store" {
            far.insert(key.clone(), value.clone());
        }
    }
    Ok(far)
}

/// The far side's `[orchestrator]`: the destination's own worker layout, in
/// whichever of the two shapes its form calls for — or none at all.
///
/// `registered` says the run's format is served by a program rather than by
/// the destination's own build. A rented destination then states no layout:
/// its probe named no format, because nothing there can resolve one that is not
/// installed yet, so what it answered says the machine is up and nothing about
/// where this run's work can go. Only the program knows, and only once the far
/// load has installed it — so the far run derives its workers from the
/// program's own enumeration at start.
///
/// A machine of yours states its layout either way: the operator wrote it down.
fn far_orchestrator(workers: FarWorkers<'_>, registered: bool) -> toml::Table {
    let mut table = toml::Table::new();
    match workers {
        FarWorkers::Probed(_) if registered => {}
        FarWorkers::Declared(owned) => {
            table.insert(
                "image".to_string(),
                toml::Value::String(owned.container.image.clone()),
            );
            table.insert(
                "runtime".to_string(),
                toml::Value::String(owned.container.runtime.clone()),
            );
            if !owned.container.run_args.is_empty() {
                table.insert(
                    "run_args".to_string(),
                    toml::Value::Array(
                        owned
                            .container
                            .run_args
                            .iter()
                            .map(|arg| toml::Value::String(arg.clone()))
                            .collect(),
                    ),
                );
            }
            match &owned.pool {
                Pool::Workers(count) => {
                    table.insert("workers".to_string(), toml::Value::Integer(*count as i64));
                }
                Pool::Devices(selectors) => {
                    table.insert(
                        "device".to_string(),
                        toml::Value::Array(
                            selectors
                                .iter()
                                .map(|selector| {
                                    device_entry(&selector.select, selector.workers as i64)
                                })
                                .collect(),
                        ),
                    );
                }
            }
        }
        FarWorkers::Probed(devices) => {
            let classes = probed_classes(devices);
            if classes.is_empty() {
                // A machine that reports no device this run can open still gets
                // a worker, bound to nothing.
                table.insert("workers".to_string(), toml::Value::Integer(1));
            } else {
                table.insert(
                    "device".to_string(),
                    toml::Value::Array(
                        classes
                            .into_iter()
                            .map(|(class, members)| device_entry(&class, members))
                            .collect(),
                    ),
                );
            }
        }
    }
    table
}

/// One device table: the selector and the workers on it.
fn device_entry(select: &str, workers: i64) -> toml::Value {
    let mut entry = toml::Table::new();
    entry.insert(
        "select".to_string(),
        toml::Value::String(select.to_string()),
    );
    entry.insert("workers".to_string(), toml::Value::Integer(workers));
    toml::Value::Table(entry)
}

/// The usable devices the probe reported, grouped into classes: one entry per
/// `vendor:device` pair, carrying how many cards of that class the machine has.
///
/// The `BTreeMap` orders by the rendered class, so the same enumeration always
/// synthesizes the same file whatever order the probe listed its devices in.
fn probed_classes(devices: &[DeviceInfo]) -> Vec<(String, i64)> {
    let mut classes: BTreeMap<String, i64> = BTreeMap::new();
    for device in usable(devices) {
        *classes.entry(device.class.to_string()).or_default() += 1;
    }
    classes.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use sima_contracts::DeviceClass;
    use sima_domains::devices::DeviceType;
    use sima_model::RunId;

    use std::collections::BTreeSet;

    use super::*;
    use crate::config::LoadedConfig;
    use crate::devices::DeviceSelector;
    use crate::fixtures::load_str;

    /// The local config text every synthesis test starts from.
    const LOCAL: &str = r#"
        [run]
        root_seed = 9
        format = "stub.v1"
        segments = 6

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed", "succeed"]

        [run.params]
        hex = "0a0b"

        [config]
        store = "/somewhere/else/store"
        max_attempts = 3
        attempt_timeout_ms = 300000
        checkpoint_interval_steps = 500

        [orchestrator]
        migrate = "gpubox"

        [[orchestrator.device]]
        select = "intel"
        workers = 2

        [host.gpubox]
        workers = 4
        image = "localhost/sima:pinned"
        runtime = "podman"
        run_args = ["--gpus", "all"]

        [host_class.lab]
        count = 3
        workers = 1

        [host.rented]
        provider = "stub"

        [fleet]
        members = ["lab", "rented"]

        [budget]
        max_spend_usd = 5.0
    "#;

    /// The entry `LOCAL` declares for a machine of yours.
    const OWNED: &str = "gpubox";
    /// The entry `LOCAL` declares for a machine to rent.
    const RENTED: &str = "rented";

    /// The config `LOCAL` synthesizes into for the destination `host`, whose
    /// enumeration reported `probed`, as text.
    fn far_text(host: &str, probed: &[DeviceInfo], registration: Option<&Registration>) -> String {
        let local = declared();
        far_config(LOCAL, &local.hosts[host].form, probed, registration).expect("the synthesis")
    }

    /// The synthesized config, loaded back.
    fn synthesized(host: &str, probed: &[DeviceInfo]) -> LoadedConfig {
        load_str(&far_text(host, probed, None))
    }

    /// `LOCAL` with a wall-clock ceiling of `ms` beside the spend ceiling it
    /// already states, so the budget tests vary one key and nothing else.
    fn stating_a_ceiling(ms: u64) -> String {
        LOCAL.replace(
            "max_spend_usd = 5.0",
            &format!("max_spend_usd = 5.0\n        max_wall_clock_ms = {ms}"),
        )
    }

    /// A local config whose `[config]` section sets every key the section
    /// admits, so the completeness check below has every one to carry.
    const EVERY_SETTING: &str = r#"
        [run]
        root_seed = 9
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed"]

        [run.params]
        hex = "0a"

        [config]
        store = "/somewhere/else/store"
        max_attempts = 3
        attempt_timeout_ms = 300000
        answer_timeout_ms = 120000
        checkpoint_interval_ms = 30000
        checkpoint_interval_steps = 500

        [orchestrator]
        migrate = "gpubox"
        workers = 1

        [host.gpubox]
        workers = 4
    "#;

    #[test]
    fn every_setting_the_section_admits_travels() {
        // A key added to `[config]` and forgotten in the synthesis would be
        // silently dropped from every migrated run. The fixture is held to the
        // section's schema and the far section to the fixture, so a new key
        // breaks this test rather than the migration.
        let local: toml::Table = EVERY_SETTING.parse().expect("the local config parses");
        let far = far_settings(&local).expect("the synthesis succeeds");
        let local_settings = local["config"].as_table().expect("a [config] table");
        // The fixture is checked against the section's own schema first:
        // comparing the far section to a local one that itself omits a key
        // would pass while the key was dropped.
        // Set comparison: the fixture is a TOML table, so its keys arrive
        // sorted while the schema's arrive in declaration order.
        assert_eq!(
            local_settings.keys().cloned().collect::<BTreeSet<String>>(),
            crate::config::config_section_keys()
                .into_iter()
                .collect::<BTreeSet<String>>(),
            "the fixture sets every key the section admits"
        );
        assert_eq!(
            far.keys().collect::<Vec<_>>(),
            local_settings.keys().collect::<Vec<_>>(),
            "the far section carries every key of the local one"
        );
        // Every value travels verbatim but the store, which the far side names
        // for itself beside its own config file.
        for (key, value) in &far {
            if key == "store" {
                assert_eq!(value.as_str(), Some(FAR_STORE));
            } else {
                assert_eq!(value, &local_settings[key], "{key} travels verbatim");
            }
        }
    }

    #[test]
    fn the_far_side_names_its_own_store() {
        // The one key that does not travel: a far side pointed at this
        // machine's store path would write nothing this machine can read.
        let local: toml::Table = EVERY_SETTING.parse().expect("the local config parses");
        let far = far_settings(&local).expect("the synthesis succeeds");
        assert_eq!(far["store"].as_str(), Some(FAR_STORE));
        assert_ne!(far["store"], local["config"]["store"]);
    }

    /// The `gpubox` entry `LOCAL` declares.
    fn declared() -> LoadedConfig {
        load_str(LOCAL)
    }

    /// One enumerated device of the given class and category.
    fn device(vendor_id: u32, device_id: u32, member: u32, device_type: DeviceType) -> DeviceInfo {
        DeviceInfo {
            class: DeviceClass::new(format!("{vendor_id:04x}:{device_id:04x}")).expect("class id"),
            name: format!("device {vendor_id:04x}:{device_id:04x}"),
            device_type,
            member,
        }
    }

    // ---- The directory ----

    #[test]
    fn the_layout_is_derived_from_the_run_id() {
        let run = RunId::from_hash(sima_core::hash_bytes(b"a run"));
        let layout = FarLayout::new("~/sima-runs", &run);
        assert_eq!(layout.dir(), format!("~/sima-runs/{run}"));
        assert_eq!(layout.config(), format!("~/sima-runs/{run}/sima.toml"));
        assert_eq!(layout.pid(), format!("~/sima-runs/{run}/run.pid"));
        assert_eq!(layout.log(), format!("~/sima-runs/{run}/run.log"));
    }

    #[test]
    fn the_far_journal_sits_where_the_store_s_own_layout_puts_it() {
        // The probe that tells an absent journal from a fault reading one has
        // to look where the far store actually keeps it, so the path is the
        // store's layout applied to the far root — never a second spelling of
        // it here.
        let run = RunId::from_hash(sima_core::hash_bytes(b"a run"));
        let layout = FarLayout::new("~/sima-runs", &run);
        assert_eq!(
            layout.journal(),
            format!("{}/runs/{run}/journal", layout.store())
        );
        assert_eq!(
            layout.journal(),
            format!("~/sima-runs/{run}/store/runs/{run}/journal")
        );
    }

    #[test]
    fn a_trailing_separator_on_the_root_yields_no_double_separator() {
        let run = RunId::from_hash(sima_core::hash_bytes(b"a run"));
        assert_eq!(
            FarLayout::new("/scratch/", &run).dir(),
            FarLayout::new("/scratch", &run).dir()
        );
    }

    #[test]
    fn two_runs_under_one_root_never_collide() {
        let first = RunId::from_hash(sima_core::hash_bytes(b"first"));
        let second = RunId::from_hash(sima_core::hash_bytes(b"second"));
        assert_ne!(
            FarLayout::new("~/sima-runs", &first).dir(),
            FarLayout::new("~/sima-runs", &second).dir()
        );
    }

    // ---- Identity, which the whole move rests on ----

    #[test]
    fn the_synthesized_config_is_the_same_run() {
        let local = declared();
        for (host, probed) in [
            (OWNED, &[][..]),
            (
                RENTED,
                &[device(0x10de, 0x2684, 0, DeviceType::Discrete)][..],
            ),
        ] {
            assert_eq!(
                synthesized(host, probed).run.id(),
                local.run.id(),
                "the far side drives this run, not another"
            );
        }
    }

    #[test]
    fn the_store_resolves_beside_the_synthesized_file() {
        // A relative store path is resolved against the config file's own
        // directory, so the far side needs no absolute path and the local
        // store path — which names a directory on this machine — never travels.
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("sima.toml");
        std::fs::write(&path, far_text(OWNED, &[], None)).expect("write");
        assert_eq!(
            crate::config::load(&path).expect("loads").store,
            dir.path().join("./store")
        );
    }

    // ---- What travels and what does not ----

    #[test]
    fn nothing_naming_another_machine_survives() {
        let far = synthesized(OWNED, &[]);
        assert!(far.hosts.is_empty(), "no host travels");
        assert!(far.host_classes.is_empty(), "no class travels");
        assert!(far.fleet.members.is_empty(), "no fleet travels");
        assert_eq!(
            far.budget,
            sima_provider::Budget::default(),
            "the spend ceiling does not travel: enforcing it needs the key that stays here"
        );
        assert_eq!(
            far.orchestrator.migrate, None,
            "a run that has arrived does not migrate onward"
        );
    }

    /// The far config a local one stating a ceiling of `ms` synthesizes into
    /// for the destination `host`, as text and loaded back.
    fn under_a_ceiling(host: &str, ms: u64) -> (String, LoadedConfig) {
        let local = declared();
        let text = far_config(&stating_a_ceiling(ms), &local.hosts[host].form, &[], None)
            .expect("the synthesis");
        let far = load_str(&text);
        (text, far)
    }

    #[test]
    fn the_wall_clock_ceiling_travels_to_a_machine_of_yours() {
        // Nothing is billed for the time a machine of yours spends computing,
        // so ending its run early is worth something and the ceiling is worth
        // carrying. The spend ceiling still does not travel: keeping it means
        // destroying a machine, which needs the key that stays here.
        let (text, far) = under_a_ceiling(OWNED, 3_600_000);
        assert!(
            !text.contains("max_spend_usd"),
            "the spend ceiling is not written down: {text}"
        );
        assert_eq!(
            far.budget.max_wall_clock,
            Some(std::time::Duration::from_millis(3_600_000))
        );
        assert_eq!(far.budget.max_spend, None);
    }

    #[test]
    fn neither_ceiling_travels_to_a_rented_destination() {
        // A rental bills by the hour rather than by use, so a run that stops
        // early there saves nothing and leaves the worst state of all: a
        // machine still billing and no longer computing. What ends a rented
        // run is `sima recall`, which takes the machine down with it.
        let (text, far) = under_a_ceiling(RENTED, 3_600_000);
        assert!(
            !text.contains("max_wall_clock_ms"),
            "no ceiling on time is written down: {text}"
        );
        assert!(!text.contains("max_spend_usd"), "and none on spend: {text}");
        assert_eq!(far.budget, sima_provider::Budget::default());
    }

    #[test]
    fn the_run_settings_travel_and_the_local_ones_do_not() {
        let local = declared();
        let far = synthesized(OWNED, &[]);
        assert_eq!(far.execution.max_attempts, 3);
        assert_eq!(
            far.execution.attempt_timeout,
            std::time::Duration::from_millis(300_000)
        );
        assert_eq!(
            far.execution.checkpoint_interval_steps,
            std::num::NonZeroU64::new(500)
        );
        // This machine's own worker layout named its hardware, and does not.
        assert_ne!(far.orchestrator.pool, local.orchestrator.pool);
    }

    // ---- What answers for the run's format on the far side ----

    /// The registration a `[domain.*]` entry with `env` synthesizes into.
    fn registration(env: &[&str]) -> Registration {
        Registration {
            format: "acme.thing.v1".to_string(),
            payload_digest: sima_core::hash_bytes(b"a payload manifest"),
            env: env.iter().map(|name| (*name).to_string()).collect(),
            sdk: None,
        }
    }

    #[test]
    fn a_rented_destination_serving_a_program_states_no_worker_layout() {
        // The probe named no format, so what it answered is that the machine is
        // up and nothing about where this run's work can go: only the program
        // knows, and it is not installed there until the far run loads. The
        // layout is left out, and the far run derives it at start.
        let registration = registration(&[]);
        let far = far_text(
            RENTED,
            &[device(0x10de, 0x2684, 0, DeviceType::Discrete)],
            Some(&registration),
        );
        let table: toml::Table = far.parse().expect("the far config parses");
        let orchestrator = table["orchestrator"].as_table().expect("a table");
        assert!(!orchestrator.contains_key("workers"), "{orchestrator:?}");
        assert!(!orchestrator.contains_key("device"), "{orchestrator:?}");
    }

    #[test]
    fn a_rented_destination_serving_a_builtin_format_carries_the_probed_tables() {
        // The image there answers for the format itself, so the probe's answer
        // is where the work can go and the layout follows from it.
        let far = far_text(
            RENTED,
            &[device(0x10de, 0x2684, 0, DeviceType::Discrete)],
            None,
        );
        let table: toml::Table = far.parse().expect("the far config parses");
        let orchestrator = table["orchestrator"].as_table().expect("a table");
        assert!(orchestrator.contains_key("device"), "{orchestrator:?}");
    }

    #[test]
    fn an_owned_destination_serving_a_program_keeps_its_declared_layout() {
        // A machine of yours states its layout whatever answers for the format,
        // because the operator wrote it down.
        let registration = registration(&[]);
        let table: toml::Table = far_text(OWNED, &[], Some(&registration))
            .parse()
            .expect("the far config parses");
        let orchestrator = table["orchestrator"].as_table().expect("a table");
        assert!(
            orchestrator.contains_key("workers") || orchestrator.contains_key("device"),
            "{orchestrator:?}"
        );
    }

    #[test]
    fn a_builtin_format_synthesizes_no_domain_table() {
        // Every format this build answers is answered the same way there, so
        // nothing about a program is written down.
        let table: toml::Table = far_text(OWNED, &[], None)
            .parse()
            .expect("the far config parses");
        assert!(!table.contains_key("domain"), "{table:?}");
    }

    #[test]
    fn a_registered_format_synthesizes_the_entry_that_installs_its_program() {
        let registration = registration(&["PATH", "PYTHONPATH"]);
        let table: toml::Table = far_text(OWNED, &[], Some(&registration))
            .parse()
            .expect("the far config parses");
        let entry = table["domain"]["acme.thing.v1"]
            .as_table()
            .expect("the entry");
        assert_eq!(
            entry["binary"].as_str(),
            Some("./program/acme.thing.v1/installed/program"),
            "the binary names what the install leaves"
        );
        assert_eq!(
            entry["payload_digest"].as_str(),
            Some(registration.payload_digest.to_string().as_str())
        );
        assert_eq!(
            entry["env"].as_array().expect("the names"),
            &[
                toml::Value::String("PATH".to_string()),
                toml::Value::String("PYTHONPATH".to_string()),
            ],
            "the names travel; the values are that machine's"
        );
        assert_eq!(entry.len(), 3, "and nothing else: {entry:?}");
    }

    #[test]
    fn a_declared_sdk_travels_as_the_declaration_it_is() {
        // What the destination needs is the statement that the program wants
        // the SDK; the package itself is its own binary's to vend, so nothing
        // of it is written into the far config or carried over the wire.
        let table: toml::Table = far_text(
            OWNED,
            &[],
            Some(&Registration {
                sdk: Some(Sdk::Python),
                ..registration(&["PATH"])
            }),
        )
        .parse()
        .expect("the far config parses");
        let entry = table["domain"]["acme.thing.v1"]
            .as_table()
            .expect("the entry");
        assert_eq!(entry["sdk"].as_str(), Some("python"));
        assert_eq!(entry.len(), 4, "beside what an entry already states");
    }

    #[test]
    fn an_entry_declaring_no_sdk_writes_no_sdk_key() {
        // The key is optional, and a program needing none of it declares none.
        let table: toml::Table = far_text(OWNED, &[], Some(&registration(&["PATH"])))
            .parse()
            .expect("the far config parses");
        assert!(
            !table["domain"]["acme.thing.v1"]
                .as_table()
                .expect("the entry")
                .contains_key("sdk")
        );
    }

    #[test]
    fn an_entry_declaring_no_names_writes_no_env_key() {
        // The key is optional, and an entry that omits it declares nothing
        // beyond the baseline every spawned program receives.
        let table: toml::Table = far_text(OWNED, &[], Some(&registration(&[])))
            .parse()
            .expect("the far config parses");
        let entry = table["domain"]["acme.thing.v1"]
            .as_table()
            .expect("the entry");
        assert!(!entry.contains_key("env"), "{entry:?}");
        assert_eq!(entry.len(), 2);
    }

    #[test]
    fn the_registration_leaves_the_run_section_byte_for_byte() {
        // The one hashed section: a run that carries its program is the same
        // run as one that does not.
        let with = far_text(OWNED, &[], Some(&registration(&["PATH"])))
            .parse::<toml::Table>()
            .expect("parses")["run"]
            .clone();
        let without = far_text(OWNED, &[], None)
            .parse::<toml::Table>()
            .expect("parses")["run"]
            .clone();
        assert_eq!(with, without);
    }

    // ---- A machine of yours ----

    #[test]
    fn a_machine_of_yours_carries_its_container_and_layout_onto_the_orchestrator() {
        let far = synthesized(OWNED, &[]);
        let container = far.orchestrator.container.expect("a container pool");
        assert_eq!(container.image, "localhost/sima:pinned");
        assert_eq!(container.runtime, "podman");
        assert_eq!(container.run_args, vec!["--gpus", "all"]);
        assert_eq!(far.orchestrator.pool, Some(Pool::Workers(4)));
    }

    #[test]
    fn a_machine_of_yours_carries_its_device_tables() {
        let mut local = declared();
        let HostForm::Owned(owned) = &mut local.hosts.get_mut(OWNED).expect("declared").form else {
            panic!("{OWNED} is a machine of yours");
        };
        owned.pool = Pool::Devices(vec![
            DeviceSelector {
                select: "nvidia".to_string(),
                workers: 2,
            },
            DeviceSelector {
                select: "8086:7d67".to_string(),
                workers: 1,
            },
        ]);
        let far = load_str(
            &far_config(LOCAL, &local.hosts[OWNED].form, &[], None).expect("the synthesis"),
        );
        assert_eq!(
            far.orchestrator.pool,
            Some(Pool::Devices(vec![
                DeviceSelector {
                    select: "nvidia".to_string(),
                    workers: 2,
                },
                DeviceSelector {
                    select: "8086:7d67".to_string(),
                    workers: 1,
                },
            ]))
        );
    }

    // ---- A rented machine ----

    #[test]
    fn a_rented_machine_runs_plain_workers_with_no_container() {
        let far = synthesized(RENTED, &[device(0x10de, 0x2684, 0, DeviceType::Discrete)]);
        assert_eq!(
            far.orchestrator.container, None,
            "ssh already lands inside the instance's own container"
        );
    }

    #[test]
    fn two_identical_cards_are_one_class_carrying_two_workers() {
        let far = synthesized(
            RENTED,
            &[
                device(0x10de, 0x2684, 0, DeviceType::Discrete),
                device(0x10de, 0x2684, 1, DeviceType::Discrete),
            ],
        );
        assert_eq!(
            far.orchestrator.pool,
            Some(Pool::Devices(vec![DeviceSelector {
                select: "10de:2684".to_string(),
                workers: 2,
            }]))
        );
    }

    #[test]
    fn two_different_cards_are_two_classes_carrying_one_worker_each() {
        let far = synthesized(
            RENTED,
            &[
                device(0x10de, 0x2684, 0, DeviceType::Discrete),
                device(0x8086, 0x7d67, 0, DeviceType::Integrated),
            ],
        );
        assert_eq!(
            far.orchestrator.pool,
            Some(Pool::Devices(vec![
                DeviceSelector {
                    select: "10de:2684".to_string(),
                    workers: 1,
                },
                DeviceSelector {
                    select: "8086:7d67".to_string(),
                    workers: 1,
                },
            ])),
            "ordered by class, so one enumeration always writes one file"
        );
    }

    #[test]
    fn a_rasterizer_beside_a_card_gets_no_entry() {
        // The machine was rented for the card; a worker on the rasterizer would
        // spend the rental running the slowest device on it.
        let far = synthesized(
            RENTED,
            &[
                device(0x10005, 0x0000, 0, DeviceType::Cpu),
                device(0x10de, 0x2684, 0, DeviceType::Discrete),
            ],
        );
        assert_eq!(
            far.orchestrator.pool,
            Some(Pool::Devices(vec![DeviceSelector {
                select: "10de:2684".to_string(),
                workers: 1,
            }]))
        );
    }

    #[test]
    fn a_probe_reporting_nothing_still_gets_a_worker() {
        let far = synthesized(RENTED, &[]);
        assert_eq!(far.orchestrator.pool, Some(Pool::Workers(1)));
        assert!(far.orchestrator.container.is_none());
    }

    #[test]
    fn a_machine_offering_only_a_rasterizer_still_gets_it() {
        // With no card to prefer, the rasterizer is the only device this run's
        // program can open and takes the entry.
        let far = synthesized(RENTED, &[device(0x10005, 0x0000, 0, DeviceType::Cpu)]);
        assert_eq!(
            far.orchestrator.pool,
            Some(Pool::Devices(vec![DeviceSelector {
                select: "10005:0000".to_string(),
                workers: 1,
            }]))
        );
    }
}
