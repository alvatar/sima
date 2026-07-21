//! The provider contract: the obligations every rented-hardware backend
//! meets.
//!
//! The trait is synchronous, matching both the workload — low-frequency
//! control-plane calls — and a codebase whose concurrency is threads.
//! Implementations are API clients; the contract each method states is what
//! acquisition, the guard, and reconciliation rely on.
//!
//! Any service supplying the following fits, whether it is a peer-to-peer
//! marketplace or a first-party cloud with a fixed catalog:
//!
//! - **Rental against offers**, where an offer can be gone by the time
//!   provisioning reaches the service. A fixed type catalog degenerates into
//!   one offer per type at the type's list price, and a type that is out of
//!   stock is [`Provision::OfferGone`].
//! - **A client-chosen tag** attached to the created instance and reported
//!   back verbatim by the instance scan, under the terms
//!   [`Provider::provision`] and [`Provider::instances`] state.
//! - **SSH reachability**, so a ready instance is a user, host, and port.
//! - **Hourly pricing**, normalized to micro-USD ([`Price`]).
//! - **Idempotent destroy**, so tearing down a machine already gone
//!   succeeds.

use sima_core::Result;

use crate::offer::{Offer, OfferId, Price};

/// A provider-scoped instance identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceId(pub String);

/// How to reach a running instance over SSH.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshEndpoint {
    /// The host to connect to.
    pub host: String,
    /// The SSH port.
    pub port: u16,
    /// The user to log in as.
    pub user: String,
}

/// A created instance: its identity and the rate the provider charges for
/// it.
#[derive(Debug, Clone)]
pub struct Instance {
    /// The provider's identifier for the instance.
    pub id: InstanceId,
    /// The hourly rate the provider is charging.
    pub price: Price,
}

/// What [`Provider::provision`] produced. On a marketplace another renter
/// may take an offer first, which is normal operation and therefore an
/// outcome.
#[derive(Debug)]
pub enum Provision {
    /// The provider created the instance.
    Provisioned(Instance),
    /// The offer was taken before this request reached the provider.
    OfferGone,
}

/// Provider-reported instance state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstanceStatus {
    /// The machine is coming up and has no endpoint yet.
    Provisioning,
    /// The machine is up and reachable at this endpoint.
    Ready(SshEndpoint),
    /// The provider holds no such instance: it was destroyed, expired, or
    /// never existed.
    Gone,
}

/// An instance the account holds, with the tag it was created under — the
/// unit a reconciliation scan works over.
#[derive(Debug, Clone)]
pub struct TaggedInstance {
    /// The provider's identifier for the instance.
    pub id: InstanceId,
    /// The tag the instance was created under.
    pub tag: String,
}

/// The rented-hardware control plane: list the market, rent a machine,
/// query it, destroy it.
pub trait Provider {
    /// The provider's stable identifier, such as `stub` or `vastai`.
    /// Ledger records carry it and reconciliation matches on it, so it
    /// never changes for a given backend.
    fn id(&self) -> &'static str;

    /// The current marketplace, normalized. Order carries no meaning;
    /// [`select`](crate::select) imposes the order that does.
    fn offers(&self) -> Result<Vec<Offer>>;

    /// Rents `offer`, attaching `tag` to the created instance verbatim.
    /// An offer another renter took first is [`Provision::OfferGone`];
    /// only an API or transport failure is `Err`.
    ///
    /// The tag is the ledger key, and it is the whole of what recovers an
    /// attempt that died before learning the instance id: the record that
    /// attempt left names the tag and nothing else, so a backend that drops,
    /// rewrites, or omits it leaves the machine running and billed with
    /// nothing in the process able to detect that.
    fn provision(&self, offer: &OfferId, tag: &str) -> Result<Provision>;

    /// The provider-reported state of one instance. An identifier the
    /// provider does not hold is [`InstanceStatus::Gone`].
    fn instance(&self, id: &InstanceId) -> Result<InstanceStatus>;

    /// Every instance this account currently holds, each with the tag it
    /// was created under, verbatim. This scan is what reconciliation matches
    /// an intent record against, and the tag is the only key it has: the
    /// contract offers no fallback.
    fn instances(&self) -> Result<Vec<TaggedInstance>>;

    /// Destroys an instance. Destroying one already gone is `Ok`: guards
    /// and reconciliation may race each other and provider-side expiry.
    fn destroy(&self, id: &InstanceId) -> Result<()>;
}
