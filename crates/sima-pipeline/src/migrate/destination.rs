//! The machine a migration moves onto.
//!
//! `[orchestrator].migrate` names a declared `[host.*]` entry, so a migration
//! adds no configuration of its own: a host declaration already says everything
//! about a machine, in either of its forms, and naming one is the whole of
//! naming a destination.
//!
//! The load already rejected a `migrate` naming a class or naming nothing
//! declared, so the only fault left here is a config that names no destination
//! at all — which `sima run` is entitled to and `sima migrate` is not.

use sima_core::{Error, Result};

use crate::config::{HostForm, LoadedConfig};

/// The machine a migration moves onto, borrowed from the loaded config.
///
/// The `form` carries the whole of what the destination is: a machine of yours,
/// with the ssh destination, container, and worker layout its entry states, or
/// a rented one, with the specification to acquire it under and no worker
/// layout — a machine that did not exist when the config was written carries
/// none, so its devices come from the enumeration probe.
#[derive(Debug)]
pub(crate) struct Destination<'a> {
    /// The entry that declared it, for errors and for the far-side directory's
    /// diagnostics.
    pub(crate) name: &'a str,
    /// How the machine is obtained and what it runs.
    pub(crate) form: &'a HostForm,
    /// Where the run's directory goes on that machine.
    pub(crate) root: &'a str,
    /// The `sima` binary that drives the run there.
    pub(crate) binary: &'a str,
}

/// The destination `config`'s orchestrator names.
///
/// A config whose `[orchestrator]` names no `migrate` names no destination,
/// which is a validation error naming the key: `sima run` ignores the key, so
/// its absence is only a fault for the command that needs it. Every other way
/// the key could be wrong — naming a class, naming nothing declared — the load
/// already rejected, so the lookup here cannot miss.
pub(crate) fn destination_for(config: &LoadedConfig) -> Result<Destination<'_>> {
    let name = config.orchestrator.migrate.as_deref().ok_or_else(|| {
        Error::Validation(
            "this config names no migration destination; give [orchestrator] a migrate key \
             naming the [host.*] entry to move the run onto"
                .to_string(),
        )
    })?;
    let host = config
        .hosts
        .get(name)
        .expect("the load rejects a migrate naming no declared host");
    Ok(Destination {
        name,
        form: &host.form,
        root: &host.root,
        binary: &host.binary,
    })
}

#[cfg(test)]
mod tests {
    use sima_core::Error;

    use super::*;
    use crate::config::{HostForm, Pool};
    use crate::fixtures::load_str;

    /// The reference config, with `machines` declared and `migrate` naming
    /// `destination` when it is given.
    fn config(destination: Option<&str>, machines: &str) -> String {
        let migrate = destination.map_or(String::new(), |name| format!("migrate = {name:?}\n"));
        format!(
            r#"
            [run]
            root_seed = 1
            format = "stub.v1"

            [run.generator]
            id = "stub.v1"
            behaviors = ["succeed"]

            [config]
            store = "./store"
            max_attempts = 1

            [orchestrator]
            workers = 2
            {migrate}
            {machines}
            "#
        )
    }

    #[test]
    fn an_orchestrator_naming_no_destination_is_an_error_naming_the_key() {
        let loaded = load_str(&config(None, ""));
        match destination_for(&loaded) {
            Err(Error::Validation(message)) => {
                assert!(message.contains("migrate"), "names the key: {message}");
            }
            other => panic!("expected a validation error, got {other:?}"),
        }
    }

    #[test]
    fn a_machine_of_yours_resolves_to_its_own_declaration() {
        let loaded = load_str(&config(
            Some("gpubox"),
            "[host.gpubox]\nworkers = 4\nrun_args = [\"--gpus\", \"all\"]\n",
        ));
        let destination = destination_for(&loaded).expect("the host is declared");
        assert_eq!(destination.name, "gpubox");
        // The defaults a migration relies on, unstated by this entry.
        assert_eq!(destination.root, "~/sima-runs");
        assert_eq!(destination.binary, "sima");
        let HostForm::Owned(owned) = destination.form else {
            panic!("expected a machine of yours");
        };
        assert_eq!(owned.ssh, "gpubox");
        assert_eq!(owned.container.image, "localhost/sima:latest");
        assert_eq!(owned.container.run_args, vec!["--gpus", "all"]);
        assert_eq!(owned.pool, Pool::Workers(4));
    }

    #[test]
    fn a_rented_machine_resolves_to_its_specification_and_states_no_layout() {
        let loaded = load_str(&config(
            Some("cloudbox"),
            "[host.cloudbox]\nprovider = \"stub\"\ndisk_gb = 64\nroot = \"/scratch\"\n\
             binary = \"/opt/sima\"\n[host.cloudbox.constraints]\nmin_vram_mb = 16000\n",
        ));
        let destination = destination_for(&loaded).expect("the host is declared");
        assert_eq!(destination.root, "/scratch");
        assert_eq!(destination.binary, "/opt/sima");
        let HostForm::Rented(rented) = destination.form else {
            panic!("expected a rented machine");
        };
        assert_eq!(rented.disk_gb, 64);
        assert_eq!(rented.constraints.min_vram_mb, Some(16000));
        // A rented machine states no worker layout: it did not exist when the
        // config was written, so its devices come from the probe.
    }

    #[test]
    fn a_host_serving_as_both_a_member_and_the_destination_resolves_the_same() {
        // The declaration says what a machine is; the role it plays says
        // nothing about it.
        let alone = load_str(&config(Some("gpubox"), "[host.gpubox]\nworkers = 4\n"));
        let engaged = load_str(&config(
            Some("gpubox"),
            "[host.gpubox]\nworkers = 4\n[fleet]\nmembers = [\"gpubox\"]\n",
        ));
        assert_eq!(
            destination_for(&alone).expect("declared").form,
            destination_for(&engaged).expect("declared").form
        );
    }
}
