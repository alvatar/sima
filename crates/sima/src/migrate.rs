//! `sima migrate <config>`: the run's orchestrator moved onto another machine.
//!
//! The destination is `[orchestrator].migrate`, so the command takes no
//! argument beyond the config: where a run executes belongs in the file that
//! describes it. What happens there is the pipeline's; this module parses,
//! renders, and maps the outcome to an exit code.
//!
//! The far run is detached, so a `sima migrate` killed here leaves the
//! destination computing and re-running reattaches to it. Ctrl-C is the
//! deliberate wind-down instead: the far run is signalled, its results are
//! pulled, and any rental is destroyed.

use std::path::Path;
use std::process::ExitCode;

use sima_core::Result;
use sima_pipeline::{BinaryChange, MigrateOutcome, load, migrate};

use crate::{EXIT_ERROR, EXIT_FAILED, EXIT_INTERRUPTED, render, report};

/// `sima migrate <config.toml> [--accept-binary]`: moves the run onto its
/// destination, renders the far run's events as they arrive, and exits on the
/// outcome. `accept` is what the invocation asked for about a program whose
/// build changed under the run; it travels to the far `sima run`, whose own
/// binding guard is what compares the two.
pub(crate) fn migrate_command(config: &Path, accept: BinaryChange) -> ExitCode {
    match moved(config, accept) {
        Ok(outcome) => {
            println!("{}", describe(&outcome));
            ExitCode::from(exit_code(&outcome))
        }
        Err(e) => report(e),
    }
}

/// Registers the interrupt flag before any output — so Ctrl-C winds the far run
/// down from the first line on — and moves the run.
fn moved(config: &Path, accept: BinaryChange) -> Result<MigrateOutcome> {
    let interrupt = crate::register_interrupt()?;

    // Named before the move, as `sima run` names it: the far side's directory
    // is derived from the run id, so an operator looking at the destination's
    // `run.log` needs it while the migration is still going. The load is the
    // migration's own: one translation of one file, handed on rather than
    // repeated.
    let loaded = load(config)?;
    println!("run {}", loaded.run.id());
    // The far run's records reach the same renderer a local run's do, so one
    // run reads the same whichever machine drove it.
    let progress = render::Progress::new();
    migrate(
        config,
        &loaded,
        &|record| progress.event(record),
        &interrupt,
        accept,
    )
}

/// The migration's own closing line: what the local store holds now that the
/// results are back, which the far run's journal does not state.
fn describe(outcome: &MigrateOutcome) -> String {
    match outcome {
        MigrateOutcome::Finalized { run } => format!("migrated: run {run} finalized here"),
        MigrateOutcome::Outstanding { run, remaining } => {
            format!("migrated: run {run} came home with {remaining} tasks outstanding")
        }
        MigrateOutcome::Interrupted { run, remaining } => {
            format!("migration wound down: run {run} has {remaining} tasks outstanding")
        }
        MigrateOutcome::Failed { task, reason } => {
            format!("migration ended on a definitive failure of task {task}: {reason}")
        }
    }
}

/// The exit code an outcome carries, on the binary's own mapping: a migration
/// that came home with tasks outstanding is neither a success nor a candidate
/// failure, so it takes the general error code.
fn exit_code(outcome: &MigrateOutcome) -> u8 {
    match outcome {
        MigrateOutcome::Finalized { .. } => 0,
        MigrateOutcome::Failed { .. } => EXIT_FAILED,
        MigrateOutcome::Interrupted { .. } => EXIT_INTERRUPTED,
        MigrateOutcome::Outstanding { .. } => EXIT_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use sima_core::hash_bytes;
    use sima_pipeline::RunId;

    use super::*;

    fn run() -> RunId {
        RunId::from_hash(hash_bytes(b"a migrated run"))
    }

    #[test]
    fn each_outcome_maps_to_its_exit_code() {
        assert_eq!(exit_code(&MigrateOutcome::Finalized { run: run() }), 0);
        assert_eq!(
            exit_code(&MigrateOutcome::Failed {
                task: "aa".to_string(),
                reason: "diverged".to_string(),
            }),
            EXIT_FAILED
        );
        assert_eq!(
            exit_code(&MigrateOutcome::Interrupted {
                run: run(),
                remaining: 3,
            }),
            EXIT_INTERRUPTED
        );
        // Resumable, but not what was asked for: the general error code.
        assert_eq!(
            exit_code(&MigrateOutcome::Outstanding {
                run: run(),
                remaining: 3,
            }),
            EXIT_ERROR
        );
    }

    #[test]
    fn every_outcome_states_what_the_local_store_holds() {
        let run = run();
        assert!(
            describe(&MigrateOutcome::Finalized { run }).contains(&run.to_string()),
            "the finalized line names the run"
        );
        for (outcome, expected) in [
            (
                MigrateOutcome::Outstanding { run, remaining: 3 },
                "3 tasks outstanding",
            ),
            (
                MigrateOutcome::Interrupted { run, remaining: 2 },
                "2 tasks outstanding",
            ),
            (
                MigrateOutcome::Failed {
                    task: "aa".to_string(),
                    reason: "diverged".to_string(),
                },
                "diverged",
            ),
        ] {
            let line = describe(&outcome);
            assert!(line.contains(expected), "{line}");
        }
    }
}
