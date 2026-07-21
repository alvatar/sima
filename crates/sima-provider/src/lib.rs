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

mod offer;
mod provider;
pub mod stub;
#[cfg(test)]
mod testutil;

pub use offer::{Constraints, Objective, Offer, OfferId, Price, select};
pub use provider::{
    Instance, InstanceId, InstanceStatus, Provider, Provision, SshEndpoint, TaggedInstance,
};
