//! Fleet membership: `[fleet] members` resolved to the machines it names.
//!
//! A member names a `[host.*]` or `[host_class.*]` entry. A host is one
//! machine; a class expands into as many as it declares. Resolution splits them
//! by form, because the two are engaged differently: a machine of yours is
//! reached where it stands, and a rented one is acquired from a control plane
//! first.
//!
//! Whether a search consults this at all is the invocation's answer, not the
//! config's — see [`Engagement`].

use crate::config::{
    Container, FillPolicy, Host, HostClass, HostClassForm, HostForm, LoadedConfig, Pool, Rented,
};

/// Which machines a search executes on. A config declares what a search *may* use;
/// the invocation decides what it *does* use, so renting is never a
/// consequence of a config sitting on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engagement {
    /// The orchestrator alone.
    Orchestrator,
    /// The orchestrator plus every member of `[fleet]`.
    Fleet,
}

/// One machine of yours the fleet draws on.
pub(crate) struct OwnedMachine<'a> {
    /// The ssh destination it is reached at, which is also the label its
    /// workers are journaled under.
    pub(crate) ssh: &'a str,
    /// The container its workers search in.
    pub(crate) container: &'a Container,
    /// Its worker layout.
    pub(crate) pool: &'a Pool,
    /// Where this machine keeps what a search puts there — a migrated search's
    /// directory, and the programs a fleet search delivers.
    pub(crate) root: &'a str,
}

/// One rental request: what to acquire, how many, and what a shortfall does.
pub(crate) struct Rental<'a> {
    /// The entry that declared it, for errors and container naming.
    pub(crate) name: &'a str,
    /// What each machine is rented as.
    pub(crate) spec: &'a Rented,
    /// How many machines to acquire.
    pub(crate) count: usize,
    /// What to do when the market cannot fill the count.
    pub(crate) fill: FillPolicy,
    /// Where each machine keeps what a search puts there.
    pub(crate) root: &'a str,
    /// The `sima` binary on each machine, which searches the far half of whatever
    /// this search delivers to it.
    pub(crate) binary: &'a str,
}

/// The machines `[fleet] members` names, split by form.
#[derive(Default)]
pub(crate) struct Members<'a> {
    /// Machines of yours, in member order, a class expanded into its own.
    pub(crate) owned: Vec<OwnedMachine<'a>>,
    /// Rental requests, in member order.
    pub(crate) rentals: Vec<Rental<'a>>,
}

impl Members<'_> {
    /// Whether the fleet names no machine at all.
    pub(crate) fn is_empty(&self) -> bool {
        self.owned.is_empty() && self.rentals.is_empty()
    }
}

/// Resolves `[fleet] members` to the machines they name, in the order listed.
///
/// Every member names a declared host or class — the load rejects one that does
/// not — so resolution cannot fail here.
pub(crate) fn members(config: &LoadedConfig) -> Members<'_> {
    let mut resolved = Members::default();
    for member in &config.fleet.members {
        if let Some(host) = config.hosts.get(member) {
            push_host(&mut resolved, member, host);
        } else if let Some(class) = config.host_classes.get(member) {
            push_class(&mut resolved, member, class);
        }
    }
    resolved
}

/// Adds one declared host to the resolved members.
fn push_host<'a>(resolved: &mut Members<'a>, name: &'a str, host: &'a Host) {
    match &host.form {
        HostForm::Owned(owned) => resolved.owned.push(OwnedMachine {
            ssh: &owned.ssh,
            container: &owned.container,
            pool: &owned.pool,
            root: &host.root,
        }),
        // One machine is a rental of exactly one, which cannot fall short of a
        // count without failing outright.
        HostForm::Rented(spec) => resolved.rentals.push(Rental {
            name,
            spec,
            count: 1,
            fill: FillPolicy::Strict,
            root: &host.root,
            binary: &host.binary,
        }),
    }
}

/// Adds one declared class to the resolved members, expanded into its machines.
fn push_class<'a>(resolved: &mut Members<'a>, name: &'a str, class: &'a HostClass) {
    match &class.form {
        HostClassForm::Owned(owned) => {
            resolved
                .owned
                .extend(owned.ssh.iter().map(|ssh| OwnedMachine {
                    ssh,
                    container: &owned.container,
                    pool: &owned.pool,
                    root: &class.root,
                }));
        }
        HostClassForm::Rented(rented) => resolved.rentals.push(Rental {
            name,
            spec: &rented.spec,
            count: rented.count,
            fill: rented.fill,
            root: &class.root,
            binary: &class.binary,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::load_str;

    /// A config whose orchestrator searches two workers, plus whatever `machines`
    /// declares.
    fn config(machines: &str) -> LoadedConfig {
        load_str(&format!(
            r#"
            [search]
            root_seed = 1
            format = "stub.v1"

            [search.generator]
            id = "stub.v1"
            behaviors = ["succeed"]

            [config]
            store = "./store"
            max_attempts = 1

            [orchestrator]
            workers = 2

            {machines}
            "#
        ))
    }

    #[test]
    fn a_fleet_naming_nothing_resolves_to_no_machine() {
        let config = config("[host.gpubox]\nworkers = 1\n");
        // A declared machine no member names is never reached.
        assert!(members(&config).is_empty());
    }

    #[test]
    fn a_class_expands_into_its_machines_in_order() {
        let config =
            config("[host_class.lab]\ncount = 6\nworkers = 8\n[fleet]\nmembers = [\"lab\"]\n");
        let members = members(&config);
        let addresses: Vec<&str> = members.owned.iter().map(|machine| machine.ssh).collect();
        assert_eq!(addresses, ["lab1", "lab2", "lab3", "lab4", "lab5", "lab6"]);
        assert!(members.rentals.is_empty());
    }

    #[test]
    fn members_resolve_in_the_order_they_are_listed() {
        let config = config(
            r#"[host.gpubox]
            workers = 4

            [host_class.oldlab]
            ssh = ["fermi", "pauli"]
            workers = 1

            [fleet]
            members = ["oldlab", "gpubox"]
            "#,
        );
        let members = members(&config);
        let addresses: Vec<&str> = members.owned.iter().map(|machine| machine.ssh).collect();
        assert_eq!(addresses, ["fermi", "pauli", "gpubox"]);
    }

    #[test]
    fn a_rented_host_is_a_rental_of_one() {
        let config =
            config("[host.cloudbox]\nprovider = \"stub\"\n[fleet]\nmembers = [\"cloudbox\"]\n");
        let members = members(&config);
        assert!(members.owned.is_empty());
        assert_eq!(members.rentals.len(), 1);
        assert_eq!(members.rentals[0].name, "cloudbox");
        assert_eq!(members.rentals[0].count, 1);
        assert_eq!(members.rentals[0].fill, FillPolicy::Strict);
    }

    #[test]
    fn a_rented_class_is_one_rental_of_its_count() {
        let config = config(
            "[host_class.rtx4090]\nprovider = \"stub\"\ncount = 4\nfill = \"best-effort\"\n\
             [fleet]\nmembers = [\"rtx4090\"]\n",
        );
        let members = members(&config);
        assert_eq!(members.rentals.len(), 1, "a class is one request, not four");
        assert_eq!(members.rentals[0].count, 4);
        assert_eq!(members.rentals[0].fill, FillPolicy::BestEffort);
    }

    #[test]
    fn the_two_forms_resolve_side_by_side() {
        let config = config(
            r#"[host.gpubox]
            workers = 4

            [host_class.rtx4090]
            provider = "stub"
            count = 2

            [fleet]
            members = ["gpubox", "rtx4090"]
            "#,
        );
        let members = members(&config);
        assert_eq!(members.owned.len(), 1);
        assert_eq!(members.rentals.len(), 1);
    }
}
