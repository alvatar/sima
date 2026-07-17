//! Chain placement: which device class a chain's work runs on.
//!
//! Placement is greedy and sticky. An unbound chain goes to whichever class
//! pulls it first, so a faster device naturally takes more chains — no shares
//! to tune, and a device that throttles simply pulls less. Once bound, every
//! segment, retry, and resumed attempt of that chain runs on the same class,
//! so a candidate's whole trajectory is internally coherent and a retried
//! attempt reproduces what the failed attempt would have committed.
//!
//! A binding moves only when its class is absent from the current device set —
//! the hardware changed between sessions — and the journal records it. Run
//! continuity outranks placement: a chain never strands because the card it
//! ran on is gone.
//!
//! Placement is derived operational state, never identity: the binding enters
//! no task key, no record, and no manifest. The slot it persists to lives
//! beside the run's checkpoints, and losing it costs coherence for one chain,
//! never correctness.

use serde::{Deserialize, Serialize};
use sima_contracts::DeviceClass;
use sima_core::{Error, Result};

/// What a worker of a given class may do with a queued task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Eligibility {
    /// The chain is already bound to this worker's class: run it.
    Run,
    /// The chain is unbound: this worker takes it and binds it.
    Bind,
    /// The chain is bound to a class the run no longer has: this worker takes
    /// it and the binding moves.
    Rebind,
    /// The chain belongs to another class that is present: leave it be.
    Skip,
}

/// What a worker of `class` may do with a task whose chain is bound to
/// `bound`, given the classes `present` in the run.
///
/// Pure over its inputs, so the rule is verifiable without threads, workers,
/// or a device.
pub(crate) fn eligibility(
    bound: Option<DeviceClass>,
    class: DeviceClass,
    present: &[DeviceClass],
) -> Eligibility {
    match bound {
        None => Eligibility::Bind,
        Some(bound) if bound == class => Eligibility::Run,
        // Another class holds it, and that class is still here to do the work.
        Some(bound) if present.contains(&bound) => Eligibility::Skip,
        // Its class is gone; continuity outranks stickiness.
        Some(_) => Eligibility::Rebind,
    }
}

/// The placement decision a pull made, for the caller to persist and journal
/// before the assignment goes out.
///
/// The decision travels out of the coordinator rather than acting inside it:
/// persisting is I/O, and no worker should wait on another's disk write to
/// pull its next task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChainPlacement {
    /// Nothing to record: the chain was already bound to the pulling class,
    /// the task has no chain, or the run has one implicit class.
    Settled,
    /// The pull bound an unbound chain to the pulling worker's class.
    Bound { chain: u64, to: DeviceClass },
    /// The pull moved a chain off a class the run no longer has.
    Rebound {
        chain: u64,
        from: DeviceClass,
        to: DeviceClass,
    },
}

/// A chain's binding as it is persisted: the class, in the human-readable
/// operational world the journal and manifest also live in — never the
/// canonical identity encoding, which a binding has no business carrying.
#[derive(Debug, Serialize, Deserialize)]
struct ClassSlot {
    vendor_id: u32,
    device_id: u32,
}

/// The slot payload binding a chain to `class`.
pub(crate) fn encode_class(class: DeviceClass) -> Result<Vec<u8>> {
    let slot = ClassSlot {
        vendor_id: class.vendor_id,
        device_id: class.device_id,
    };
    serde_json::to_vec(&slot)
        .map_err(|e| Error::Encoding(format!("encode a chain's device class: {e}")))
}

/// The class a slot payload binds its chain to.
pub(crate) fn decode_class(payload: &[u8]) -> Result<DeviceClass> {
    let slot: ClassSlot = serde_json::from_slice(payload)
        .map_err(|e| Error::Encoding(format!("decode a chain's device class: {e}")))?;
    Ok(DeviceClass {
        vendor_id: slot.vendor_id,
        device_id: slot.device_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const INTEL: DeviceClass = DeviceClass {
        vendor_id: 0x8086,
        device_id: 0x7d51,
    };
    const NVIDIA: DeviceClass = DeviceClass {
        vendor_id: 0x10de,
        device_id: 0x2d39,
    };

    #[test]
    fn a_chain_bound_to_the_pulling_class_runs() {
        assert_eq!(
            eligibility(Some(NVIDIA), NVIDIA, &[INTEL, NVIDIA]),
            Eligibility::Run
        );
    }

    #[test]
    fn an_unbound_chain_is_taken_by_whoever_pulls_it() {
        assert_eq!(
            eligibility(None, INTEL, &[INTEL, NVIDIA]),
            Eligibility::Bind
        );
        assert_eq!(
            eligibility(None, NVIDIA, &[INTEL, NVIDIA]),
            Eligibility::Bind
        );
    }

    #[test]
    fn a_chain_bound_to_another_present_class_is_left_alone() {
        assert_eq!(
            eligibility(Some(NVIDIA), INTEL, &[INTEL, NVIDIA]),
            Eligibility::Skip
        );
    }

    #[test]
    fn a_chain_bound_to_an_absent_class_rebinds() {
        // The card it ran on is gone: the run continues on what is here.
        assert_eq!(
            eligibility(Some(NVIDIA), INTEL, &[INTEL]),
            Eligibility::Rebind
        );
    }

    #[test]
    fn a_class_slot_round_trips() -> Result<()> {
        assert_eq!(decode_class(&encode_class(NVIDIA)?)?, NVIDIA);
        Ok(())
    }

    #[test]
    fn a_slot_payload_is_the_readable_operational_form() -> Result<()> {
        let payload = encode_class(INTEL)?;
        assert_eq!(
            String::from_utf8(payload).expect("utf-8"),
            r#"{"vendor_id":32902,"device_id":32081}"#
        );
        Ok(())
    }

    #[test]
    fn a_malformed_slot_payload_is_an_encoding_error() {
        assert!(matches!(decode_class(b"not json"), Err(Error::Encoding(_))));
    }
}
