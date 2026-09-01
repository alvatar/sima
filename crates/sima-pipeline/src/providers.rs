//! [`provider_for`]: the one place a provider id resolves to the backend that
//! answers for it.
//!
//! Two callers need that resolution, and they need it for different reasons: a
//! search acquiring machines for a rented class, and `sima reconcile` reaching the
//! backend a ledger record names so it can destroy what a crashed search left
//! behind. Both go through here, so adding a backend is one arm in one match
//! rather than one arm in each.
//!
//! What separates the two callers is settings, not dispatch: an acquisition
//! knows the image and disk an instance boots with, while a reconciliation only
//! reads and destroys and supplies neither.

use sima_core::{Error, Result};
use sima_provider::stub::StubProvider;
use sima_provider::{Offer, OfferId, Price, Provider, SshEndpoint};
use sima_provider_vast::{VastConfig, VastProvider};

/// The environment channel that points the stub backend at a machine that is
/// really there, as `user@host:port`.
///
/// It exists so a test can exercise the ssh path against a throwaway server of
/// its own, without a key in the configuration schema that would be valid for
/// one provider and rejected for every other. Unset, the stub fabricates an
/// endpoint naming no machine and is reached in process.
const STUB_SSH: &str = "SIMA_STUB_SSH";

/// What a backend needs beyond the id that names it.
pub struct ProviderSettings<'a> {
    /// The image an instance boots and the disk it is given. Both enter a
    /// request only when an instance is created.
    pub image: &'a str,
    pub disk_gb: u64,
    /// How many machines the caller intends to acquire, which the stub's
    /// scripted marketplace sizes itself to.
    pub count: usize,
}

impl ProviderSettings<'_> {
    /// The settings of a caller that only reads and destroys: no instance is
    /// created from here, so no image and no disk enter a request.
    pub fn read_only() -> ProviderSettings<'static> {
        ProviderSettings {
            image: "",
            disk_gb: 0,
            count: 0,
        }
    }
}

/// The control-plane backend `id` names, configured by `settings`.
///
/// The `vastai` backend reads its key from `VAST_API_KEY`; an absent key is an
/// [`Error::Provider`] naming the variable, raised here before any store
/// mutation. The `stub` backend is in-process, listing a generous
/// always-available marketplace so a stub rental fills its declared count.
///
/// [`STUB_SSH`] is read here and nowhere else, so a caller naming any other
/// provider never looks at it.
pub fn provider_for(id: &str, settings: &ProviderSettings<'_>) -> Result<Box<dyn Provider + Sync>> {
    match id {
        sima_provider_vast::PROVIDER_ID => {
            let config = VastConfig::from_env(settings.image, settings.disk_gb)?;
            Ok(Box::new(VastProvider::new(config)))
        }
        sima_provider::STUB_PROVIDER_ID => {
            let stub = StubProvider::new(stub_offers(settings.count));
            Ok(Box::new(match std::env::var_os(STUB_SSH) {
                Some(value) => {
                    let endpoint = stub_endpoint(&value.to_string_lossy())?;
                    stub.endpoint(&endpoint.host, endpoint.port, &endpoint.user)
                }
                None => stub,
            }))
        }
        unknown => Err(Error::Provider(format!(
            "the provider {unknown:?} is one this build has no backend for"
        ))),
    }
}

/// A machine the stub backend offers, one per requested slot.
///
/// The marketplace is generous — cheap, plentiful, and well past every
/// constraint a config can state — so a stub rental fills its declared count
/// and the acquisition path is exercised rather than the filtering.
fn stub_offers(count: usize) -> Vec<Offer> {
    (0..count.max(1))
        .map(|n| Offer {
            id: OfferId(format!("stub-offer-{n}")),
            machine: format!("stub-machine-{n}"),
            gpu_model: "stub-gpu".to_string(),
            gpu_count: 1,
            vram_mb: 24_000,
            // Distinct rates keep the cheapest-per-hour ranking a total order;
            // $0.10/hr and up, low enough to sit under an ordinary price cap.
            price: Price(100_000 + n as u64),
            reliability: 1.0,
            verified: true,
            disk_gb: 1_000,
            bandwidth_mbps: 10_000,
            location: String::new(),
        })
        .collect()
}

/// The endpoint a `user@host:port` value names.
///
/// A value that does not parse is an error naming the variable rather than a
/// fall back to the in-process path. A caller that set it meant to cross a hop,
/// and one that quietly did not would report a success that tested nothing.
fn stub_endpoint(value: &str) -> Result<SshEndpoint> {
    let malformed = || {
        Error::Validation(format!(
            "{STUB_SSH} is {value:?}, which is not a user@host:port endpoint"
        ))
    };
    let (user, rest) = value.split_once('@').ok_or_else(malformed)?;
    // From the right: an IPv6 literal in brackets holds colons of its own.
    let (host, port) = rest.rsplit_once(':').ok_or_else(malformed)?;
    let port: u16 = port.parse().map_err(|_| malformed())?;
    if user.is_empty() || host.is_empty() || port == 0 {
        return Err(malformed());
    }
    Ok(SshEndpoint {
        host: host.to_string(),
        port,
        user: user.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_provider::SshEndpoint;

    #[test]
    fn a_provider_this_build_has_no_backend_for_is_rejected() {
        // The ledger holds whatever id the search that wrote it used, so a build
        // that dropped a backend must say so rather than act on the record.
        let error = provider_for("no-such-cloud", &ProviderSettings::read_only())
            .err()
            .expect("an id with no backend");
        let Error::Provider(message) = error else {
            panic!("expected a provider error");
        };
        assert!(message.contains("no-such-cloud"), "{message}");
    }

    #[test]
    fn the_stub_backend_answers_for_its_own_id() -> Result<()> {
        let provider = provider_for("stub", &ProviderSettings::read_only())?;
        assert_eq!(provider.id(), sima_provider::STUB_PROVIDER_ID);
        Ok(())
    }

    #[test]
    fn the_stub_marketplace_is_sized_to_the_requested_count() {
        // A rental fills its declared count only if the marketplace lists at
        // least that many machines, and a read-only caller asks for none —
        // which still lists one, since an empty marketplace would read as an
        // exhausted one.
        assert_eq!(stub_offers(0).len(), 1);
        assert_eq!(stub_offers(4).len(), 4);
    }

    #[test]
    fn a_stub_endpoint_reads_as_a_user_a_host_and_a_port() -> Result<()> {
        assert_eq!(
            stub_endpoint("tester@127.0.0.1:41022")?,
            SshEndpoint {
                host: "127.0.0.1".to_string(),
                port: 41022,
                user: "tester".to_string(),
            }
        );
        // Taken from the right, so a bracketed IPv6 literal keeps its own
        // colons.
        assert_eq!(stub_endpoint("root@[::1]:22")?.host, "[::1]");
        Ok(())
    }

    #[test]
    fn a_malformed_stub_endpoint_names_the_variable_and_falls_back_to_nothing() {
        // Falling back to the in-process path would report a success that
        // tested nothing, which is the failure this whole boundary exists to avoid.
        for value in [
            "127.0.0.1:41022",
            "tester@127.0.0.1",
            "tester@127.0.0.1:0",
            "tester@127.0.0.1:not-a-port",
            "@127.0.0.1:22",
            "tester@:22",
            "",
        ] {
            match stub_endpoint(value) {
                Err(Error::Validation(message)) => assert!(
                    message.contains(STUB_SSH) && message.contains(value),
                    "names the variable and the value: {message}"
                ),
                other => panic!("expected {value:?} to be refused, got {other:?}"),
            }
        }
    }
}
