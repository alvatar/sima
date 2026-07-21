//! The rented-hardware control plane: the provider-agnostic seam between a
//! run and the machines it rents.
//!
//! A provider lists a marketplace of concrete offers, rents one, reports
//! its state, and destroys it. Offers are normalized across providers, so
//! a fixed type catalog is one offer per type and a live marketplace is the
//! general case of the same shape.
//!
//! Selection splits in two: hard [`Constraints`] disqualify offers, and one
//! scalar [`Objective`] ranks whatever qualifies.
//!
//! A rented machine is money spent for as long as it runs, so teardown is
//! guaranteed on three levels. [`InstanceGuard`] destroys the instance
//! whenever it goes out of scope, covering success, failure, panic unwind,
//! and the graceful wind-down an interrupt triggers. Behind it, every
//! attempt writes a ledger record in the store before the provider is
//! called, so a process killed outright still leaves the machine
//! discoverable — and `reconcile`, which runs at the start of every
//! acquisition, destroys what an earlier process left behind.

mod acquire;
mod guard;
mod offer;
mod provider;
mod reconcile;
pub mod stub;
#[cfg(test)]
mod testutil;

pub use acquire::{AcquireLimits, acquire};
pub use guard::InstanceGuard;
pub use offer::{Constraints, Objective, Offer, OfferId, Price, select};
pub use provider::{
    Instance, InstanceId, InstanceStatus, Provider, Provision, SshEndpoint, TaggedInstance,
};
pub use reconcile::{ReconcileReport, reconcile};
