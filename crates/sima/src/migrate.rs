//! `sima migrate <config>` and `sima recall <config>`: the run's orchestrator
//! moved onto another machine, and taken off it again.
//!
//! The destination is `[orchestrator].migrate` for both, so neither takes an
//! argument beyond the config: where a run executes belongs in the file that
//! describes it. What happens there is the pipeline's; this module parses,
//! renders, and maps the outcome to an exit code — one table, since both verbs
//! come home with the same answers.
//!
//! The far run is detached, so nothing a migration does ends it: a killed
//! `sima migrate`, a closed terminal, and a Ctrl-C all leave the destination
//! computing, and re-running attaches to it again. `sima recall` is what ends
//! it, which is why it is a verb rather than a signal.

use std::path::Path;
use std::process::ExitCode;

use sima_core::Result;
use sima_pipeline::{BinaryChange, MigrateOutcome, load, migrate, recall};

use crate::render::Narration;
use crate::{EXIT_ERROR, EXIT_FAILED, EXIT_INTERRUPTED, render, report};

/// `sima migrate <config.toml> [--accept-binary]`: moves the run onto its
/// destination, renders the far run's events as they arrive, and exits on the
/// outcome. `accept` is what the invocation asked for about a program whose
/// build changed under the run; it travels to the far `sima run`, whose own
/// binding guard is what compares the two.
pub(crate) fn migrate_command(
    config: &Path,
    accept: BinaryChange,
    narration: Narration,
) -> ExitCode {
    match moved(config, accept, narration) {
        Ok(outcome) => {
            println!("{}", describe(&outcome, config));
            ExitCode::from(exit_code(&outcome))
        }
        Err(e) => report(e),
    }
}

/// Registers the interrupt flag before any output — so Ctrl-C detaches from the
/// first line on — and moves the run.
fn moved(config: &Path, accept: BinaryChange, narration: Narration) -> Result<MigrateOutcome> {
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
    let progress = render::Progress::new(narration);
    migrate(
        config,
        &loaded,
        &|record| progress.event(record),
        &interrupt,
        accept,
    )
}

/// `sima recall <config.toml>`: winds the run down on its destination, brings
/// the results home, and takes the machine away.
///
/// No interrupt flag is registered: a recall is short and every step of it is
/// resumable, so a Ctrl-C during one takes the default death.
pub(crate) fn recall_command(config: &Path, narration: Narration) -> ExitCode {
    match load(config).and_then(|loaded| {
        println!("run {}", loaded.run.id());
        // The far run's records do not reach a recall — it follows nothing —
        // so the renderer sees only what this side journals while it waits.
        let progress = render::Progress::new(narration);
        recall(&loaded, &|record| progress.event(record))
    }) {
        Ok(outcome) => {
            println!("{}", describe(&outcome, config));
            ExitCode::from(exit_code(&outcome))
        }
        Err(e) => report(e),
    }
}

/// The command's own closing line: what the local store holds now that the
/// results are back, which the far run's journal does not state.
///
/// A detached run is the one outcome that leaves work where it is, so its line
/// states the machine and both ways back — `config` is named in them, since a
/// second invocation needs the same file this one was given.
fn describe(outcome: &MigrateOutcome, config: &Path) -> String {
    match outcome {
        MigrateOutcome::Finalized { run } => format!("migrated: run {run} finalized here"),
        MigrateOutcome::Outstanding { run, remaining } => {
            format!("migrated: run {run} came home with {remaining} tasks outstanding")
        }
        MigrateOutcome::Interrupted { run, remaining } => {
            format!("migration wound down: run {run} has {remaining} tasks outstanding")
        }
        MigrateOutcome::Detached { run, machine } => {
            let config = config.display();
            format!(
                "detached: run {run} is still computing on {machine:?}\n\
                 \x20 sima migrate {config}  attach to it again\n\
                 \x20 sima recall {config}   wind it down and bring the results home"
            )
        }
        MigrateOutcome::Abandoned { run, machine } => {
            let config = config.display();
            format!(
                "abandoned: nothing was started on {machine:?}, so run {run} is where it was\n\
                 \x20 sima migrate {config}  place it there again"
            )
        }
        // Either verb reaches it: a migration watched the failure arrive, a
        // recall read it in the journal the far run left.
        MigrateOutcome::Failed { task, reason } => {
            format!("the run ended on a definitive failure of task {task}: {reason}")
        }
    }
}

/// The exit code an outcome carries, on the binary's own mapping: a migration
/// that came home with tasks outstanding is neither a success nor a candidate
/// failure, so it takes the general error code. Detaching did what was asked,
/// so it is a success; a placement the operator stopped is an interrupt, and
/// takes the code every interrupted run takes.
fn exit_code(outcome: &MigrateOutcome) -> u8 {
    match outcome {
        MigrateOutcome::Finalized { .. } | MigrateOutcome::Detached { .. } => 0,
        MigrateOutcome::Failed { .. } => EXIT_FAILED,
        MigrateOutcome::Interrupted { .. } | MigrateOutcome::Abandoned { .. } => EXIT_INTERRUPTED,
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
    fn a_detached_migration_exits_zero_and_names_both_ways_back() {
        // Detaching did what was asked, so it is a success; the line has to
        // carry the two commands, since nothing else states them.
        let outcome = MigrateOutcome::Detached {
            run: run(),
            machine: "gpubox".to_string(),
        };
        assert_eq!(exit_code(&outcome), 0);
        let line = describe(&outcome, Path::new("exp.toml"));
        assert!(line.contains("gpubox"), "names the machine: {line}");
        assert!(
            line.contains("sima migrate exp.toml"),
            "names the way back: {line}"
        );
        assert!(
            line.contains("sima recall exp.toml"),
            "names the way to end it: {line}"
        );
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
        let config = Path::new("exp.toml");
        assert!(
            describe(&MigrateOutcome::Finalized { run }, config).contains(&run.to_string()),
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
            (
                MigrateOutcome::Detached {
                    run,
                    machine: "gpubox".to_string(),
                },
                "still computing",
            ),
        ] {
            let line = describe(&outcome, config);
            assert!(line.contains(expected), "{line}");
        }
    }
}
