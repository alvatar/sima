//! The generator contract.
//!
//! A generator decides which candidates a run tries. It owns the
//! `[run.generator]` keys it reads, and turns the run's root seed into the
//! run's specs deterministically: the same `(root_seed, params)` yields the
//! same specs in the same order.

use sima_core::Result;
use sima_model::{FormatId, GeneratorId, Spec};

/// One way of choosing candidates for a format.
///
/// A format has one [`crate::Domain`] and many generators — random sampling, a
/// sweep, mutation of a previous run's winners — so a run names the one it
/// wants by id.
pub trait Generator: Send + Sync {
    /// The id a run config names this generator by.
    fn id(&self) -> &GeneratorId;

    /// The format the specs this generator produces are of.
    fn format(&self) -> &FormatId;

    /// The `[run.generator]` section as TOML text, minus its `id` key,
    /// translated to the opaque params blob [`Generator::generate`] reads.
    ///
    /// The section crosses as text, so a generator parses it with a TOML of
    /// its own choosing. The bytes enter the run id.
    fn translate_params(&self, toml: &str) -> Result<Vec<u8>>;

    /// Produces the run's candidate specs from its root seed and the blob
    /// [`Generator::translate_params`] produced, stamping each with
    /// [`Generator::format`]. Deterministic in both inputs.
    fn generate(&self, root_seed: u64, params: &[u8]) -> Result<Vec<Spec>>;
}

/// `Generator` is dyn-compatible: a host holds one behind a trait object for
/// the life of a session. The auto-trait supertraits are part of the contract —
/// a generator is reached from the threads a run drives.
const _: fn() = || {
    fn _object_safe(_: &dyn Generator) {}
};
