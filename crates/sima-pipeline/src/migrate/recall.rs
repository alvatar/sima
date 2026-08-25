//! [`recall`]: ending a migrated run and bringing its results home.
//!
//! The inverse of [`migrate`](crate::migrate): it places nothing, pushes
//! nothing, and starts nothing. What it does is what a migration does after
//! its follow — wind the far run down, pull, settle, and take the rented
//! machine away — over a run this side may never have watched.
//!
//! ```text
//!  ┌────────────────────────────────────────────────────────────────────────┐
//!  │  0  load config; require [orchestrator].migrate                        │
//!  │  1  open the local store; acquire the run lock, held to the end        │
//!  │  2  the machine, by the form the named host takes:                     │
//!  │       yours    ──▶ that machine; no rental, no teardown                │
//!  │       rented   ──▶ the rental hosting this run, adopted from the       │
//!  │                      ledger — never a fresh one                        │
//!  │  3  is the run's directory there? no ──▶ refuse, naming what is missing│
//!  │  4  is the far side driving? yes ──▶ WIND DOWN, bounded and escalating │
//!  │  5  PULL: everything the far side's records reference                  │
//!  │  6  settle over the store that came home                               │
//!  │  7  TEARDOWN: destroy the rental                                       │
//!  └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! **A rented machine that is already gone leaves nothing to contact.** Its
//! ledger record was cleared when it was destroyed, so there is no endpoint to
//! reach and no far store to pull from; the run settles over what the local
//! store already holds, which is the plain report that there is nothing to do.
//!
//! **No interrupt is registered.** A recall is short and every step of it is
//! resumable — a signalled far run stays resumable, a sync is content-addressed
//! and picks up where it stopped — so a Ctrl-C during one takes the default
//! death and a second recall carries on.

use std::time::Instant;

use sima_core::Result;
use sima_provider::{AcquireLimits, adopt};
use sima_store::Store;
use sima_trace::Observer;

use crate::config::{FillPolicy, HostForm, LoadedConfig};
use crate::fleet::Rental;
use crate::migrate::destination::destination_for;
#[cfg(test)]
use crate::migrate::far_run::Overrides;
use crate::migrate::far_run::{FarRun, FollowEnd, MigrateOutcome, settle};
use crate::migrate::far_side::Remote;
use crate::rental::provider_for_rental;
use crate::status::RunState;

/// Ends the run `loaded` describes on the machine its `[orchestrator]` names,
/// brings its results home, and takes that machine away.
///
/// `observer` receives what this side journals while it waits, which is the
/// wind-down's own report and nothing else: the far run's records do not travel
/// without a follow, and a recall drives none.
///
/// The local run lock is held for the whole call, so nothing else drives or
/// reconciles this run while it is being wound back.
pub fn recall(loaded: &LoadedConfig, observer: Observer<'_>) -> Result<MigrateOutcome> {
    let destination = destination_for(loaded)?;
    let store = Store::open(&loaded.store)?;
    // The same idempotent registration `sima run` and `sima migrate` perform:
    // it is what gives the run a journal for the wind-down to report into.
    let run = store.create_run(&loaded.run)?;
    let lock = store.acquire_run_lock(&run)?;

    match destination.form {
        HostForm::Owned(owned) => {
            let far = Remote::owned(&destination, owned, &run);
            FarRun {
                far: &far,
                store: &store,
                config: loaded,
                destination: &destination,
                observer,
                rental: None,
                #[cfg(test)]
                overrides: Overrides::default(),
            }
            .under_teardown(FarRun::wind_back)
        }
        HostForm::Rented(spec) => {
            let rental = Rental {
                name: destination.name,
                spec,
                count: 1,
                fill: FillPolicy::Strict,
                root: destination.root,
                binary: destination.binary,
            };
            let provider = provider_for_rental(&rental)?;
            let limits = AcquireLimits {
                usable_by: Instant::now() + spec.ready_timeout,
                ready_poll: spec.ready_poll,
            };
            // Adoption only: a recall never rents. A machine that is not there
            // to take back is one this run is already off, and what is left of
            // the run is whatever the local store holds.
            let Some(guard) = adopt(provider.as_ref(), &store, &lock, &limits)? else {
                return settle(&store, loaded, RunState::InProgress, FollowEnd::FarRun);
            };
            let far = Remote::rented(
                &destination,
                provider.as_ref(),
                guard.endpoint(),
                &run,
                None,
            )?;
            FarRun {
                far: &far,
                store: &store,
                config: loaded,
                destination: &destination,
                observer,
                rental: Some(guard),
                #[cfg(test)]
                overrides: Overrides::default(),
            }
            .under_teardown(FarRun::wind_back)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use sima_provider::{InstanceGuard, Provider};
    use sima_scheduler::Record;

    use super::*;
    use crate::migrate::destination::destination_for;
    use crate::migrate::far_side::FarSide;
    use crate::migrate::fixtures::{
        Local, OWNED, PID, PROMPT, PULL, PUSH, RENTED, Scripted, Step, committed, far_store,
        finalized, hosting, local, marketplace, started,
    };

    /// Winds one far run back, capturing what this side journaled while it did.
    fn recall_over(
        local: &Local,
        far: &dyn FarSide,
        rental: Option<InstanceGuard<'_, dyn Provider + Sync + '_>>,
    ) -> (Result<MigrateOutcome>, Vec<Record>) {
        let captured: Mutex<Vec<Record>> = Mutex::new(Vec::new());
        let observer = |record: &Record| {
            captured
                .lock()
                .expect("the capture lock")
                .push(record.clone());
        };
        let destination = destination_for(&local.config).expect("the host is declared");
        let outcome = FarRun {
            far,
            store: &local.store,
            config: &local.config,
            destination: &destination,
            observer: &observer,
            rental,
            overrides: Overrides::default(),
        }
        .under_teardown(FarRun::wind_back);
        let records = std::mem::take(&mut *captured.lock().expect("the capture lock"));
        (outcome, records)
    }

    #[test]
    fn a_recall_of_a_driving_run_winds_it_down_pulls_and_tears_the_rental_down() -> Result<()> {
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let lock = local.store.acquire_run_lock(&run)?;
        let provider = marketplace();
        let guard = hosting(&provider, &local.store, &lock)?;
        let far = Scripted::new()
            .already_driving()
            .delivering(vec![vec![started(&run), committed("aa")]]);

        let (outcome, _) = recall_over(&local, &far, Some(guard));
        assert!(matches!(outcome?, MigrateOutcome::Interrupted { .. }));
        assert_eq!(
            far.steps(),
            [
                Step::Placed,
                Step::Driving,
                Step::Interrupt(PID),
                Step::Driving,
                PULL,
            ],
            "signal, wait, pull — and nothing before them"
        );
        assert!(
            !far.steps().contains(&Step::Place) && !far.steps().contains(&Step::Start),
            "a recall places nothing and starts nothing: {:?}",
            far.steps()
        );
        assert!(!far.steps().contains(&PUSH), "and pushes nothing");
        assert_eq!(provider.destroyed().len(), 1, "the rental is torn down");
        Ok(())
    }

    #[test]
    fn a_recall_of_an_ended_run_collects_it_without_restarting_anything() -> Result<()> {
        // The other way a run comes home: it finished while nothing was
        // attached, so there is nothing to end and only results to fetch.
        let local = local(RENTED, PROMPT, Some(3));
        let (_far_dir, far_store) = far_store(&local.config, None);
        let far = Scripted::new().syncing_with(&far_store, &local.config);

        let (outcome, _) = recall_over(&local, &far, None);
        assert_eq!(
            outcome?,
            MigrateOutcome::Finalized {
                run: local.config.run.id()
            }
        );
        assert_eq!(
            far.steps(),
            [Step::Placed, Step::Driving, PULL],
            "the pull and the settlement alone"
        );
        assert!(
            local.store.manifest(&local.config.run.id())?.is_some(),
            "the manifest is written here, over the store the pull completed"
        );
        Ok(())
    }

    #[test]
    fn a_recall_of_an_ended_run_short_of_its_end_reports_what_is_left() -> Result<()> {
        let local = local(RENTED, PROMPT, Some(3));
        let (_far_dir, far_store) = far_store(&local.config, Some(8));
        let far = Scripted::new().syncing_with(&far_store, &local.config);

        let (outcome, _) = recall_over(&local, &far, None);
        assert_eq!(
            outcome?,
            MigrateOutcome::Outstanding {
                run: local.config.run.id(),
                remaining: 1,
            }
        );
        Ok(())
    }

    #[test]
    fn a_recall_of_a_machine_never_migrated_to_names_what_is_missing() -> Result<()> {
        let local = local(OWNED, "", Some(3));
        let far = Scripted::new().never_migrated_to();

        let (outcome, _) = recall_over(&local, &far, None);
        let text = outcome
            .expect_err("there is nothing there to recall")
            .to_string();
        assert!(text.contains("slingshot"), "names the machine: {text}");
        assert!(
            text.contains(&local.config.run.id().to_string()),
            "names the run: {text}"
        );
        assert!(text.contains("nothing to recall"), "{text}");
        assert_eq!(
            far.steps(),
            [Step::Placed],
            "nothing was asked of it past that: {:?}",
            far.steps()
        );
        Ok(())
    }

    #[test]
    fn a_recall_of_a_far_run_that_outlasts_the_wind_down_terminates_it_and_pulls() -> Result<()> {
        // The escalation is the migration's, reached over a run this side
        // never followed.
        let local = local(RENTED, PROMPT, Some(3));
        let run = local.config.run.id();
        let far = Scripted::new()
            .already_driving()
            .outlasting_the_wind_down()
            .delivering(vec![vec![started(&run), finalized(&run)]]);

        let (outcome, records) = recall_over(&local, &far, None);
        assert!(matches!(outcome?, MigrateOutcome::Interrupted { .. }));
        let report = records
            .iter()
            .find_map(|record| match &record.event {
                sima_scheduler::Event::Diagnostic { message, .. } => Some(message.clone()),
                _ => None,
            })
            .expect("the wait that ran out is reported");
        assert!(report.contains("did not exit"), "{report}");
        let steps = far.steps();
        assert!(
            steps.contains(&Step::Terminate(PID)),
            "the far run was terminated: {steps:?}"
        );
        assert_eq!(
            steps.last(),
            Some(&PULL),
            "and the pull followed: {steps:?}"
        );
        Ok(())
    }
}
