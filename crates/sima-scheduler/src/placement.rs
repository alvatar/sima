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
    /// The chain is bound to a class absent from the run's devices: this
    /// worker takes it and the binding moves.
    Rebind,
    /// The chain belongs to another class that is present: leave it be.
    Skip,
}

/// What a worker of `class` may do with a task whose chain is bound to
/// `bound`, given `present_classes`, the classes the run has.
///
/// Pure over its inputs, so the rule is verifiable without threads, workers,
/// or a device.
/// Both classes arrive by reference: this runs once per queued task on every
/// task pull, so the scan compares in place rather than cloning a class.
pub(crate) fn eligibility(
    bound: Option<&DeviceClass>,
    class: &DeviceClass,
    present_classes: &[DeviceClass],
) -> Eligibility {
    match bound {
        None => Eligibility::Bind,
        Some(bound) if bound == class => Eligibility::Run,
        // Another class holds it, and that class is still here to do the work.
        Some(bound) if present_classes.contains(bound) => Eligibility::Skip,
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
    /// The pull moved a chain off a class absent from the run's devices.
    Rebound {
        chain: u64,
        from: DeviceClass,
        to: DeviceClass,
    },
}

/// A chain's binding as it is persisted: the class, in the human-readable
/// operational world the journal and manifest also live in. The canonical
/// identity encoding is reserved for values that enter a hash, which a
/// binding never does.
#[derive(Debug, Serialize, Deserialize)]
struct ClassSlot {
    class: DeviceClass,
}

/// The slot payload binding a chain to `class`.
pub(crate) fn encode_class(class: &DeviceClass) -> Result<Vec<u8>> {
    let slot = ClassSlot {
        class: class.clone(),
    };
    serde_json::to_vec(&slot)
        .map_err(|e| Error::Encoding(format!("encode a chain's device class: {e}")))
}

/// The class a slot payload binds its chain to. A payload that is not a slot,
/// or that names a class no backend could have minted, is an error the caller
/// reads as an absent binding.
pub(crate) fn decode_class(payload: &[u8]) -> Result<DeviceClass> {
    let slot: ClassSlot = serde_json::from_slice(payload)
        .map_err(|e| Error::Encoding(format!("decode a chain's device class: {e}")))?;
    Ok(slot.class)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A class that must be valid.
    fn class(id: &str) -> DeviceClass {
        DeviceClass::new(id).expect("a valid class id")
    }

    /// The Intel iGPU class.
    fn intel() -> DeviceClass {
        class("8086:7d51")
    }

    /// The NVIDIA dGPU class.
    fn nvidia() -> DeviceClass {
        class("10de:2d39")
    }

    #[test]
    fn a_chain_bound_to_the_pulling_class_runs() {
        assert_eq!(
            eligibility(Some(&nvidia()), &nvidia(), &[intel(), nvidia()]),
            Eligibility::Run
        );
    }

    #[test]
    fn an_unbound_chain_is_taken_by_whoever_pulls_it() {
        assert_eq!(
            eligibility(None, &intel(), &[intel(), nvidia()]),
            Eligibility::Bind
        );
        assert_eq!(
            eligibility(None, &nvidia(), &[intel(), nvidia()]),
            Eligibility::Bind
        );
    }

    #[test]
    fn a_chain_bound_to_another_present_class_is_left_alone() {
        assert_eq!(
            eligibility(Some(&nvidia()), &intel(), &[intel(), nvidia()]),
            Eligibility::Skip
        );
    }

    #[test]
    fn a_chain_bound_to_an_absent_class_rebinds() {
        // The card it ran on is gone: the run continues on what is here.
        assert_eq!(
            eligibility(Some(&nvidia()), &intel(), &[intel()]),
            Eligibility::Rebind
        );
    }

    #[test]
    fn a_partitioned_cards_profiles_are_separate_classes() {
        // Two slices of one card report the same pair, so only the profile
        // tells them apart. Work bound to the larger slice may not run on the
        // smaller one, and eligibility follows the class it was given.
        let small = class("10de:2330:1g.10gb");
        let large = class("10de:2330:4g.40gb");
        assert_eq!(
            eligibility(Some(&large), &small, &[small.clone(), large.clone()]),
            Eligibility::Skip
        );
        assert_eq!(
            eligibility(Some(&large), &large, &[small, large.clone()]),
            Eligibility::Run
        );
    }

    #[test]
    fn a_class_slot_round_trips() -> Result<()> {
        let class = nvidia();
        assert_eq!(decode_class(&encode_class(&class)?)?, class);
        Ok(())
    }

    #[test]
    fn a_slot_payload_is_the_readable_operational_form() -> Result<()> {
        let payload = encode_class(&intel())?;
        assert_eq!(
            String::from_utf8(payload).expect("utf-8"),
            r#"{"class":"8086:7d51"}"#
        );
        Ok(())
    }

    #[test]
    fn a_slot_naming_an_invalid_class_is_an_encoding_error() {
        // A slot is read back into a validated class, so a payload carrying a
        // name no backend could have minted fails here rather than travelling
        // on as a class nothing matches.
        assert!(matches!(
            decode_class(br#"{"class":"8086 7D51"}"#),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn a_malformed_slot_payload_is_an_encoding_error() {
        assert!(matches!(decode_class(b"not json"), Err(Error::Encoding(_))));
    }
}
