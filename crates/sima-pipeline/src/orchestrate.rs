//! [`orchestrate`]: one loaded config driven to its outcome.

use sima_core::Result;
use sima_scheduler::{RunControl, RunOutcome};
use sima_store::Store;

use crate::config::LoadedConfig;
use crate::family;

/// Drives the run a loaded config describes: opens the store (creating it
/// where missing), takes the run's orchestrator lock, dispatches the family
/// and the generator, and runs the scheduler. Resume and re-evaluation are
/// this same call — the frontier re-derives from store state, so an
/// interrupted or failed run continues and a finalized one re-finalizes
/// without touching an executor. The lock is held for the whole call and
/// releases on return.
pub fn orchestrate(config: &LoadedConfig, control: &RunControl) -> Result<RunOutcome> {
    let store = Store::open(&config.store)?;
    let run = config.run.id();
    let _lock = store.acquire_run_lock(&run)?;
    let family = family::family_for(&config.run.format)?;
    let generator = family::generator_for(&config.run.generator.id)?;
    sima_scheduler::run(
        &store,
        &config.run,
        &family.environment,
        generator.as_ref(),
        family.executor.as_ref(),
        &config.execution,
        control,
    )
}
