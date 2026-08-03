//! The operational settings: what a run executes under, and what it may spend.
//!
//! None of it enters the run id. A deadline, a cadence, an attempt count, and a
//! spend ceiling are all properties of this session rather than of the work, so
//! two runs differing only here are the same run and share their results.

use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;

use sima_core::{Error, Result};
use sima_provider::{Budget, Cost};
use sima_scheduler::ExecutionConfig;

use super::file::{BudgetSection, ConfigSection};
use super::machines::{Orchestrator, Pool};

/// Assembles the parameters the run executes under from `[config]` and the
/// orchestrator's worker layout. The orchestrator's device selectors stay
/// unresolved: they name real hardware, and loading a config must work where
/// none is present.
pub(super) fn resolve_execution(
    path: &Path,
    config: &ConfigSection,
    orchestrator: &Orchestrator,
) -> Result<ExecutionConfig> {
    let attempt_timeout = optional_bound(config.attempt_timeout_ms);
    let checkpoint_interval = optional_bound(config.checkpoint_interval_ms);
    // The step cadence is optional and, when present, at least 1: a zero cadence
    // has no meaning (every offer, and no offer, at once), so it is a validation
    // fault naming the key.
    let checkpoint_interval_steps = config
        .checkpoint_interval_steps
        .map(|n| {
            NonZeroU64::new(n).ok_or_else(|| {
                Error::Validation(format!(
                    "{}: checkpoint_interval_steps must be at least 1, got 0",
                    path.display()
                ))
            })
        })
        .transpose()?;
    let workers = orchestrator.pool.as_ref().map_or(0, Pool::workers);
    ExecutionConfig::new(
        workers,
        config.max_attempts,
        attempt_timeout,
        optional_bound(config.answer_timeout_ms),
        checkpoint_interval,
        checkpoint_interval_steps,
    )
}

/// An optional millisecond setting as a duration. Absent disables the bound,
/// which [`Duration::MAX`] expresses: a wait longer than the address space of
/// milliseconds is no bound in effect.
pub(super) fn optional_bound(ms: Option<u64>) -> Duration {
    ms.map_or(Duration::MAX, Duration::from_millis)
}

/// Resolves the `[budget]` section into the provider control plane's own type.
/// An absent section is the permissive default.
pub(super) fn resolve_budget(path: &Path, section: Option<BudgetSection>) -> Result<Budget> {
    let Some(section) = section else {
        return Ok(Budget::default());
    };
    let max_spend = section
        .max_spend_usd
        .map(|dollars| {
            finite_dollars(path, "[budget]", "max_spend_usd", dollars)
                .map(|dollars| Cost(dollars_to_micro_ceil(dollars)))
        })
        .transpose()?;
    Ok(Budget {
        max_spend,
        max_wall_clock: section.max_wall_clock_ms.map(Duration::from_millis),
    })
}

/// Converts a dollar amount to micro-USD, rounding up so a cap or rate is never
/// rendered stricter than the figure written. The value must be validated finite
/// and non-negative first.
pub(super) fn dollars_to_micro_ceil(dollars: f64) -> u64 {
    (dollars * 1_000_000.0).ceil() as u64
}

/// Validates that a dollar figure is finite and non-negative, naming the entry
/// and `key` on failure.
pub(super) fn finite_dollars(path: &Path, subject: &str, key: &str, value: f64) -> Result<f64> {
    if !value.is_finite() || value < 0.0 {
        return Err(Error::Validation(format!(
            "{}: {subject} {key} must be finite and non-negative, got {value}",
            path.display()
        )));
    }
    Ok(value)
}
