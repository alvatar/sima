//! What compute devices a program can run on.
//!
//! The domains layer is where the set of compiled-in execution backends is
//! known, so it is where "what devices can be used" is answered for the layers
//! above: they ask this crate rather than depending on a toolkit directly.
//!
//! Placement asks the question about a format id. A backend reaches only the
//! devices its own driver stack exposes, and the two stacks disagree on real
//! hosts: a rented instance whose Vulkan loader cannot initialize the NVIDIA
//! driver offers a WGSL program the CPU rasterizer alone while CUDA opens the
//! card there, and a laptop's Intel integrated GPU is a Vulkan device that CUDA
//! cannot open at all. A list of everything present would therefore hand a
//! program devices its backend faults on, so each domain carries the
//! enumeration of its own backend and the answer follows the program.
//!
//! [`enumerate_all_devices`] asks it about the machine instead, across every
//! compiled backend. That answer states reachability and hardware, and is used
//! where no format this build carries is in play: a fleet machine's readiness
//! probe for a search whose format is a program outside this build. Placement for
//! such a search comes from the program's own enumeration, never from this list.

use sima_core::Result;
use sima_model::FormatId;

pub use sima_contracts::{DeviceInfo, DeviceType};

/// Every device the program bound to `format` can run on.
///
/// Resolving the format is what selects the enumeration: a domain carries the
/// one its own execution backend supplies, so nothing above this crate has to
/// know which backends the build compiles in.
pub fn enumerate_devices(format: &FormatId) -> Result<Vec<DeviceInfo>> {
    crate::domain_for(format)?.enumerate_devices()
}

/// Every device every compiled backend reaches, asked about a machine rather
/// than about a program.
///
/// This is the answer for a search whose format is a program outside this build:
/// nothing here can resolve that format, and only the program itself knows
/// which of these devices its backend opens. The list is therefore a statement
/// about the machine — it is reachable, and this is its hardware — and never a
/// resolution of a program's device selectors.
///
/// Class minting is shared across backends, so one card is the same class under
/// Vulkan and under CUDA; a device both reach is listed once, keyed by class and
/// member, so the count reads as the hardware rather than as the drivers.
pub fn enumerate_all_devices() -> Result<Vec<DeviceInfo>> {
    let mut all: Vec<DeviceInfo> = Vec::new();
    for backend in [
        sima_toolkit_wgsl::enumerate_devices,
        sima_toolkit_cuda::enumerate_devices,
    ] {
        for device in backend()? {
            if !all
                .iter()
                .any(|listed| listed.class == device.class && listed.member == device.member)
            {
                all.push(device);
            }
        }
    }
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A validated format id.
    fn format(name: &str) -> FormatId {
        FormatId::new(name).expect("format id")
    }

    #[test]
    fn every_backend_together_answers_what_the_machine_reaches() {
        // The machine-facing question: a device here is one some backend
        // opened, so the answer covers both backends' lists.
        let all = enumerate_all_devices().expect("enumeration answers on any machine");
        for backend in [
            sima_toolkit_wgsl::enumerate_devices().expect("the WGSL backend answers"),
            sima_toolkit_cuda::enumerate_devices().expect("the CUDA backend answers"),
        ] {
            for device in backend {
                assert!(
                    all.iter().any(
                        |listed| listed.class == device.class && listed.member == device.member
                    ),
                    "{:?} member {} is reached by a backend and listed",
                    device.class,
                    device.member
                );
            }
        }
    }

    #[test]
    fn a_card_two_backends_both_reach_is_listed_once() {
        // Class minting is shared across backends, so an NVIDIA card is the
        // same class under Vulkan and under CUDA. Listing it twice would read
        // as two places to put a worker where there is one.
        let all = enumerate_all_devices().expect("enumeration answers on any machine");
        let mut seen: Vec<(String, u32)> = all
            .iter()
            .map(|device| (device.class.as_str().to_string(), device.member))
            .collect();
        let listed = seen.len();
        seen.sort();
        seen.dedup();
        assert_eq!(listed, seen.len(), "each class and member appears once");
    }

    #[test]
    fn a_program_that_opens_no_device_enumerates_none() {
        // The stub computes in the worker process, so it has no device to be
        // placed on and the layers above derive a deviceless worker.
        assert!(
            enumerate_devices(&format("stub.v1"))
                .expect("the stub always answers")
                .is_empty()
        );
    }

    #[test]
    fn enumeration_answers_on_a_machine_with_no_device_at_all() {
        // No backend faults for want of a driver, so the probe the worker runs
        // answers rather than failing, whichever program it is asked about.
        for name in [
            "stub.v1",
            "ca_evolution.gray_scott.v1",
            "ca_evolution.nca.v1",
            "ca_evolution.gray_scott_cuda.v1",
        ] {
            enumerate_devices(&format(name)).expect("enumeration answers on any machine");
        }
    }

    #[test]
    fn a_domain_answers_with_the_devices_its_own_backend_reports() {
        // The rented-host case and its mirror: Vulkan there reaches the CPU
        // rasterizer alone while CUDA opens the card, and a laptop's Intel
        // integrated GPU is a Vulkan device CUDA cannot open. Each domain's
        // answer is its backend's own list, so no worker is ever bound to a
        // device its backend faults on.
        for (name, backend_devices) in [
            (
                "ca_evolution.gray_scott.v1",
                sima_toolkit_wgsl::enumerate_devices()
                    .expect("the WGSL backend answers")
                    .len(),
            ),
            (
                "ca_evolution.gray_scott_cuda.v1",
                sima_toolkit_cuda::enumerate_devices()
                    .expect("the CUDA backend answers")
                    .len(),
            ),
        ] {
            assert_eq!(
                enumerate_devices(&format(name))
                    .expect("a registered format")
                    .len(),
                backend_devices,
                "{name} answers with its own backend's devices"
            );
        }
    }

    #[test]
    fn an_unknown_format_has_no_devices_to_enumerate() {
        // The probe resolves the format before it touches a backend, so a
        // format this build does not know is a validation error rather than an
        // empty list that would read as a machine with no hardware.
        assert!(enumerate_devices(&format("no-such-domain.v1")).is_err());
    }
}
