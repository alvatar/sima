//! [`ExecutionConfig`]: the operational settings a run executes under.

use std::num::NonZeroU64;
use std::time::Duration;

use sima_core::{Error, Result};

/// Operational run settings. Never hashed; not part of run identity — a run
/// resumed with different parallelism or a different timeout keeps its run id.
/// The file form is the execution section of the run configuration in higher
/// layers.
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Number of worker threads; at least 1.
    pub workers: usize,
    /// Total attempts per task before a transient failure becomes definitive;
    /// at least 1.
    pub max_attempts: u32,
    /// Soft per-attempt deadline the watchdog uses for lease-expiry detection.
    /// In process, execution cannot be preempted, so this drives reporting,
    /// not termination. A value larger than any attempt could take (for
    /// example [`Duration::MAX`]) disables expiry reporting: no lease's age
    /// ever exceeds it.
    pub attempt_timeout: Duration,
    /// Wall-clock cadence between checkpoint saves during one attempt: the
    /// first save becomes due one full interval after execution starts.
    /// [`Duration::MAX`] disables this axis.
    pub checkpoint_interval: Duration,
    /// Step-count cadence between checkpoint saves during one attempt: a save
    /// becomes due every `n`th offer since the last save. `None` disables this
    /// axis. The two axes are unioned — a save is due when either fires, and
    /// either axis present enables checkpointing. With both disabled no offer
    /// is ever due, so no slot is written or read.
    pub checkpoint_interval_steps: Option<NonZeroU64>,
}

impl ExecutionConfig {
    /// Validates the settings and wraps them: `workers` and `max_attempts`
    /// must each be at least 1 ([`Error::Validation`] otherwise).
    pub fn new(
        workers: usize,
        max_attempts: u32,
        attempt_timeout: Duration,
        checkpoint_interval: Duration,
        checkpoint_interval_steps: Option<NonZeroU64>,
    ) -> Result<Self> {
        if workers == 0 {
            return Err(Error::Validation(
                "execution config requires at least one worker".to_string(),
            ));
        }
        if max_attempts == 0 {
            return Err(Error::Validation(
                "execution config requires at least one attempt per task".to_string(),
            ));
        }
        Ok(ExecutionConfig {
            workers,
            max_attempts,
            attempt_timeout,
            checkpoint_interval,
            checkpoint_interval_steps,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::Error;

    #[test]
    fn new_accepts_valid_settings() -> Result<()> {
        let config = ExecutionConfig::new(
            4,
            3,
            Duration::from_millis(50),
            Duration::MAX,
            NonZeroU64::new(64),
        )?;
        assert_eq!(config.workers, 4);
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.attempt_timeout, Duration::from_millis(50));
        assert_eq!(config.checkpoint_interval, Duration::MAX);
        assert_eq!(config.checkpoint_interval_steps, NonZeroU64::new(64));
        Ok(())
    }

    #[test]
    fn new_rejects_zero_workers() {
        assert!(matches!(
            ExecutionConfig::new(0, 1, Duration::from_millis(1), Duration::MAX, None),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn new_rejects_zero_attempts() {
        assert!(matches!(
            ExecutionConfig::new(1, 0, Duration::from_millis(1), Duration::MAX, None),
            Err(Error::Validation(_))
        ));
    }
}
