//! The rented-hardware control plane: the provider-agnostic boundary between a
//! search and the machines it rents.
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
//!
//! What those machines cost is counted the same way: every rental that ends
//! leaves a durable spend entry behind, so a search's total spend outlives both
//! its machines and the process that rented them. A [`Budget`] states the
//! ceilings that total and the rental phase's wall-clock must stay under,
//! [`assess`] answers where a search stands against them, and acquisition
//! refuses to rent once they are reached.

mod acquire;
mod adopt;
mod budget;
mod guard;
mod offer;
mod provider;
mod reconcile;
mod reputation;
pub mod stub;

pub use stub::STUB_PROVIDER_ID;
#[cfg(test)]
mod testutil;

pub use acquire::{
    AcquireLimits, Admission, UNREPORTED, acquire, is_acquisition_cancelled, never_cancelled,
};
pub use adopt::adopt;
pub use budget::{
    Budget, Cost, Exhaustion, OpenSpend, SpendReport, Verdict, assess, now_ms, spend_report,
};
pub use guard::InstanceGuard;
pub use offer::{Constraints, Objective, Offer, OfferId, Price, select};
pub use provider::{
    Instance, InstanceId, InstanceStatus, Provider, Provision, Reachability, SshEndpoint,
    TaggedInstance,
};
pub use reconcile::{ReconcileReport, ReconcileScope, reconcile};
pub use reputation::{
    IncidentKind, MachineReport, MachineSummary, machine_report, record_incident,
};
