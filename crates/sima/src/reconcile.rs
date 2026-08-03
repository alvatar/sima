//! `sima reconcile <config>`: the reconciliation pass, invoked on its own.
//!
//! Acquisition reconciles before it rents, so a machine left running by a
//! process that died without tearing it down stops costing money when the
//! next run starts. After a hard crash nothing starts on its own, and this
//! command is that same pass reached directly — from a shell, or from
//! whatever schedules periodic maintenance.
//!
//! The ledger decides which providers it touches: the records name them, so
//! a store holding no record reaches no provider API and needs no
//! credentials.
//!
//! A rental hosting a migrated run's orchestrator is spared by default. It has
//! exactly the shape a pass reaps — nothing local holds its owner's lock,
//! because a migration detaches the far side deliberately — and destroying one
//! would kill a run that is working and paid for. `--hosted` includes them, for
//! an operator who knows no migration is running.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::ExitCode;

use sima_core::Result;
use sima_pipeline::load;
use sima_provider::{Provider, ReconcileReport, ReconcileScope, reconcile};
use sima_store::Store;

use crate::report;

/// `sima reconcile <config.toml> [--hosted]`: destroys the machines the
/// config's store still holds records for, and prints what the pass did. The
/// store comes from the config's `[config]` section, as the query commands
/// derive it. Without `--hosted` a rental hosting a migrated run is spared.
pub(crate) fn reconcile_command(config: &Path, scope: ReconcileScope) -> ExitCode {
    match clean_store(config, scope) {
        Ok(report) => {
            print_report(&report);
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Loads the config, opens its store, and runs the pass over every provider
/// the instance ledger names.
fn clean_store(config: &Path, scope: ReconcileScope) -> Result<ReconcileReport> {
    let store = Store::open(load(config)?.store)?;
    clean(&store, backend, scope)
}

/// Runs one reconciliation pass per provider the ledger names, over the
/// backends `resolve` constructs, and reports what all of them did.
///
/// A ledger holding no record resolves no backend at all: reconciliation is
/// driven by what the store says was rented, so a store that rented nothing
/// costs no provider call and no credential.
fn clean<R>(store: &Store, resolve: R, scope: ReconcileScope) -> Result<ReconcileReport>
where
    R: Fn(&str) -> Result<Box<dyn Provider>>,
{
    let providers: BTreeSet<String> = store
        .instance_records()?
        .into_iter()
        .map(|record| record.provider)
        .collect();
    let mut report = ReconcileReport::default();
    for id in providers {
        let pass = reconcile(resolve(&id)?.as_ref(), store, scope)?;
        report.destroyed.extend(pass.destroyed);
        report.cleared.extend(pass.cleared);
    }
    Ok(report)
}

/// The backend a ledger record's provider id names, through the one registry a
/// run resolves its own providers with — so a build that carries a backend for
/// a run carries it here too.
///
/// The settings are the read-only ones: the image and disk a rental would boot
/// enter a request only when an instance is created, which no path from here
/// does.
fn backend(id: &str) -> Result<Box<dyn Provider>> {
    Ok(sima_pipeline::provider_for(
        id,
        &sima_pipeline::ProviderSettings::read_only(),
    )?)
}

/// Prints what the pass did, or that the store held nothing to act on.
fn print_report(report: &ReconcileReport) {
    if report.destroyed.is_empty() && report.cleared.is_empty() {
        println!("nothing to reconcile");
        return;
    }
    println!(
        "reconciled: {} machines destroyed, {} records cleared",
        report.destroyed.len(),
        report.cleared.len()
    );
}

#[cfg(test)]
mod tests {
    use sima_core::Error;
    use std::cell::Cell;

    use sima_core::Result;
    use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig, RunId};
    use sima_provider::ReconcileScope;
    use sima_provider::stub::StubProvider;
    use sima_provider::{InstanceId, Provider};
    use sima_store::{InstanceRecord, InstanceRecordState, Rental, Store};

    use super::{backend, clean};

    /// A store over a fresh temporary directory, with its directory guard.
    fn temp_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open the store");
        (dir, store)
    }

    /// The run a seeded record is owned by. It holds no lock, which is what
    /// a run whose process died looks like.
    fn owner() -> RunId {
        RunConfig {
            root_seed: 7,
            segments: None,
            format: FormatId::new("stub.v1").expect("format id"),
            generator: GeneratorConfig {
                id: GeneratorId::new("stub.v1").expect("generator id"),
                params: Vec::new(),
            },
            params: Params { bytes: Vec::new() },
        }
        .id()
    }

    /// A live record naming `instance`, rented from `provider` under `tag`.
    fn record(tag: &str, provider: &str, instance: &str) -> InstanceRecord {
        InstanceRecord {
            tag: tag.to_string(),
            provider: provider.to_string(),
            machine: "m-0".to_string(),
            owner: owner().to_string(),
            role: Rental::Worker,
            state: InstanceRecordState::Live {
                instance: instance.to_string(),
            },
            price_micro_usd_hour: 100_000,
            created_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn an_empty_ledger_resolves_no_backend_and_reports_nothing_done() -> Result<()> {
        let (_dir, store) = temp_store();
        let resolutions = Cell::new(0);
        let report = clean(
            &store,
            |_| {
                resolutions.set(resolutions.get() + 1);
                Err(Error::Provider("no backend may be built".to_string()))
            },
            ReconcileScope::Workers,
        )?;
        assert_eq!(resolutions.get(), 0);
        assert!(report.destroyed.is_empty());
        assert!(report.cleared.is_empty());
        Ok(())
    }

    #[test]
    fn an_orphan_of_a_dead_run_is_destroyed_and_its_record_cleared() -> Result<()> {
        let (_dir, store) = temp_store();
        store.put_instance(&record("sima-tag-0", "stub", "i-1"))?;
        let report = clean(
            &store,
            |id| {
                assert_eq!(id, "stub");
                Ok(Box::new(
                    StubProvider::new(Vec::new())
                        .with_instance(InstanceId("i-1".to_string()), "sima-tag-0"),
                ) as Box<dyn Provider>)
            },
            ReconcileScope::Workers,
        )?;
        assert_eq!(report.destroyed, vec![InstanceId("i-1".to_string())]);
        assert_eq!(report.cleared, vec!["sima-tag-0".to_string()]);
        assert!(store.instance_records()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_record_of_a_live_run_is_left_to_the_run_that_owns_it() -> Result<()> {
        let (_dir, store) = temp_store();
        store.put_instance(&record("sima-tag-0", "stub", "i-1"))?;
        // The owning run holds its orchestrator lock, which is what a live
        // run looks like to the pass.
        let _lock = store.acquire_run_lock(&owner())?;
        let report = clean(
            &store,
            |_| {
                Ok(Box::new(
                    StubProvider::new(Vec::new())
                        .with_instance(InstanceId("i-1".to_string()), "sima-tag-0"),
                ) as Box<dyn Provider>)
            },
            ReconcileScope::Workers,
        )?;
        assert!(report.destroyed.is_empty());
        assert_eq!(store.instance_records()?.len(), 1);
        Ok(())
    }

    #[test]
    fn each_provider_the_ledger_names_gets_its_own_pass() -> Result<()> {
        let (_dir, store) = temp_store();
        store.put_instance(&record("sima-tag-0", "stub", "i-1"))?;
        store.put_instance(&record("sima-tag-1", "other", "i-2"))?;
        store.put_instance(&record("sima-tag-2", "stub", "i-3"))?;
        let resolved: std::cell::RefCell<Vec<String>> = std::cell::RefCell::new(Vec::new());
        clean(
            &store,
            |id| {
                resolved.borrow_mut().push(id.to_string());
                Ok(Box::new(StubProvider::new(Vec::new())) as Box<dyn Provider>)
            },
            ReconcileScope::Workers,
        )?;
        // One backend per distinct provider id, and no second pass over a
        // provider two records name.
        assert_eq!(resolved.into_inner(), vec!["other", "stub"]);
        Ok(())
    }

    #[test]
    fn a_record_naming_a_provider_with_no_backend_is_an_error_naming_it() -> Result<()> {
        let (_dir, store) = temp_store();
        store.put_instance(&record("sima-tag-0", "nowhere", "i-1"))?;
        assert!(matches!(
            clean(&store, backend, ReconcileScope::Workers),
            Err(Error::Provider(message)) if message.contains("nowhere")
        ));
        // The record survives: nothing could judge the machine it names.
        assert_eq!(store.instance_records()?.len(), 1);
        Ok(())
    }
}
