//! What a machine's enumeration probe is asked.

use sima_model::FormatId;

/// The question a device probe puts to a machine.
///
/// A search whose format this build carries asks about that format: the worker
/// resolves it and answers with that backend's own devices, which are the
/// places the search's work can go.
///
/// A search whose format is a program outside this build asks about no format at
/// all. The worker on that machine cannot resolve the format, and only the
/// installed program knows which devices its backend opens, so the answer is a
/// statement about the machine — it is reachable, and this is its hardware —
/// which the caller reads as readiness and never as a resolution of the
/// program's device selectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceProbe<'a> {
    /// The devices the named format's backend reaches.
    Format(&'a FormatId),
    /// Every device every compiled backend reaches.
    EveryBackend,
}

impl DeviceProbe<'_> {
    /// The arguments this probe appends to a worker argv. Both transports build
    /// their probe command from these, so the two cannot spell the flag apart.
    pub(crate) fn args(self) -> Vec<String> {
        let mut args = vec!["--enumerate-devices".to_string()];
        if let DeviceProbe::Format(format) = self {
            args.push(format.as_str().to_string());
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_format_probe_names_the_format_after_the_flag() {
        let format = FormatId::new("ca_evolution.gray_scott.v1").expect("format id");
        assert_eq!(
            DeviceProbe::Format(&format).args(),
            ["--enumerate-devices", "ca_evolution.gray_scott.v1"]
        );
    }

    #[test]
    fn a_format_free_probe_is_the_flag_alone() {
        assert_eq!(DeviceProbe::EveryBackend.args(), ["--enumerate-devices"]);
    }
}
