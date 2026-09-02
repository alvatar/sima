//! Renting: a rented entry's `provider` id resolved to a control-plane backend,
//! the machines acquired behind teardown guards, and the supervisor that keeps
//! them within budget and replaces the ones that vanish.
//!
//! The pipeline is where provider choice becomes concrete, so this is the one
//! edge from configuration to a boxed [`Provider`]. A search whose fleet is not
//! engaged never reaches here, so it constructs no provider and reads no
//! `VAST_API_KEY`.

mod acquire;
mod rented_program;
mod supervisor;

#[cfg(test)]
pub(crate) mod fixtures;

pub(crate) use acquire::{
    RentalGroup, acquire_hosts, budget_exhausted, endpoint_target, provider_for_rental,
    release_all, taken, transport_mode,
};
pub(crate) use supervisor::{StopOnSpawnFailure, StopSignal, Supervisor};
