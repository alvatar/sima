//! [`ExecutionConfig`]: the operational settings a run executes under.

use std::num::NonZeroU64;
use std::time::Duration;

use sima_contracts::DeviceClass;
use sima_core::{Error, Result};

/// One device class a run spreads its workers over, resolved: the class, the
/// name the backend reports for it, how many workers it carries, and how many
/// physical cards it has.
///
/// The resolved form of one configured device selector. Selectors name devices
/// by substring or id and resolve against real hardware, so resolution happens
/// where a run starts, never where a config is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceEntry {
    /// The class these workers compute on.
    pub class: DeviceClass,
    /// The device name the backend reports, for events and diagnostics.
    pub name: String,
    /// Worker processes on this class; at least 1.
    pub workers: usize,
    /// Physical cards in the class; the slots round-robin over them.
    pub members: u32,
}

/// Operational run settings. Never hashed; not part of run identity — a run
/// resumed with different parallelism or a different timeout keeps its run id.
/// The file form is the execution section of the run configuration in higher
/// layers.
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Number of worker processes; at least 1. With device entries present it
    /// is their sum.
    pub workers: usize,
    /// Total attempts per task before a transient failure becomes definitive;
    /// at least 1.
    pub max_attempts: u32,
    /// Enforced per-attempt deadline: on expiry the attempt's worker process
    /// is killed, the journal records the lease expiry, and the attempt
    /// fails transiently — retried up to `max_attempts`. A value larger than
    /// any attempt could take (for example [`Duration::MAX`]) disables
    /// enforcement.
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
    /// The device classes the pool spreads over. Empty is the single implicit
    /// class: every worker takes the backend's default device, and placement
    /// costs nothing.
    pub devices: Vec<DeviceEntry>,
}

impl ExecutionConfig {
    /// Validates the settings and wraps them, over the backend's default
    /// device selection. `workers` is the local pool size; `0` is a run with
    /// no local pool, valid only when a remote pool carries the work — the
    /// "at least one worker" requirement is a whole-run property the pool
    /// assembly enforces, not a per-config one. `max_attempts` must be at
    /// least 1 ([`Error::Validation`] otherwise).
    pub fn new(
        workers: usize,
        max_attempts: u32,
        attempt_timeout: Duration,
        checkpoint_interval: Duration,
        checkpoint_interval_steps: Option<NonZeroU64>,
    ) -> Result<Self> {
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
            devices: Vec::new(),
        })
    }

    /// Validates the settings and wraps them with the pool spread over
    /// `devices`: the entries carry the workers, so the pool size is their sum
    /// and each entry must carry at least one worker.
    pub fn with_devices(
        devices: Vec<DeviceEntry>,
        max_attempts: u32,
        attempt_timeout: Duration,
        checkpoint_interval: Duration,
        checkpoint_interval_steps: Option<NonZeroU64>,
    ) -> Result<Self> {
        for entry in &devices {
            if entry.workers == 0 {
                return Err(Error::Validation(format!(
                    "device {} ({}) requires at least one worker",
                    entry.name, entry.class
                )));
            }
            // The slots of an entry round-robin over its cards, so a class
            // with no card has nothing to run on.
            if entry.members == 0 {
                return Err(Error::Validation(format!(
                    "device {} ({}) requires at least one card",
                    entry.name, entry.class
                )));
            }
        }
        let workers = devices.iter().map(|entry| entry.workers).sum();
        let mut config = ExecutionConfig::new(
            workers,
            max_attempts,
            attempt_timeout,
            checkpoint_interval,
            checkpoint_interval_steps,
        )?;
        config.devices = devices;
        Ok(config)
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
    fn new_accepts_zero_local_workers() -> Result<()> {
        // A run with no local pool: the workers come from a remote pool, so the
        // "at least one worker" requirement is enforced across pools, not here.
        let config = ExecutionConfig::new(0, 1, Duration::from_millis(1), Duration::MAX, None)?;
        assert_eq!(config.workers, 0);
        Ok(())
    }

    #[test]
    fn new_rejects_zero_attempts() {
        assert!(matches!(
            ExecutionConfig::new(1, 0, Duration::from_millis(1), Duration::MAX, None),
            Err(Error::Validation(_))
        ));
    }

    /// A resolved entry for a class carrying `workers` workers.
    fn entry(vendor_id: u32, workers: usize) -> DeviceEntry {
        DeviceEntry {
            class: DeviceClass {
                vendor_id,
                device_id: 1,
            },
            name: format!("device {vendor_id:04x}"),
            workers,
            members: 1,
        }
    }

    #[test]
    fn a_config_over_devices_pools_the_entries_workers() -> Result<()> {
        let config = ExecutionConfig::with_devices(
            vec![entry(0x10de, 3), entry(0x8086, 1)],
            3,
            Duration::MAX,
            Duration::MAX,
            None,
        )?;
        assert_eq!(config.workers, 4, "the pool is the entries' sum");
        assert_eq!(config.devices.len(), 2, "one entry per class");
        Ok(())
    }

    #[test]
    fn a_device_entry_with_no_workers_is_rejected() {
        let error = ExecutionConfig::with_devices(
            vec![entry(0x10de, 0)],
            1,
            Duration::MAX,
            Duration::MAX,
            None,
        );
        let Err(Error::Validation(message)) = error else {
            panic!("expected a validation error");
        };
        assert!(message.contains("10de:0001"), "names the device: {message}");
    }

    #[test]
    fn a_device_entry_with_no_cards_is_rejected() {
        // The slots round-robin over an entry's cards, so a class of none has
        // nothing to run on.
        let error = ExecutionConfig::with_devices(
            vec![DeviceEntry {
                members: 0,
                ..entry(0x10de, 1)
            }],
            1,
            Duration::MAX,
            Duration::MAX,
            None,
        );
        let Err(Error::Validation(message)) = error else {
            panic!("expected a validation error");
        };
        assert!(message.contains("10de:0001"), "names the device: {message}");
        assert!(message.contains("card"), "names what is missing: {message}");
    }

    #[test]
    fn no_device_entries_is_the_single_implicit_class() -> Result<()> {
        let config = ExecutionConfig::new(4, 1, Duration::MAX, Duration::MAX, None)?;
        assert!(config.devices.is_empty(), "the single implicit class");
        Ok(())
    }
}
