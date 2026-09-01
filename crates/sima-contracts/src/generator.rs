//! The generator contract.
//!
//! A generator decides which candidates a search tries. It owns the
//! `[search.generator]` keys it reads, and turns the search's root seed into the
//! search's specs deterministically: the same `(root_seed, params)` yields the
//! same specs in the same order.

use sima_core::Result;
use sima_model::{FormatId, GeneratorId, Spec};

/// One way of choosing candidates for a format.
///
/// A format has one [`crate::Domain`] and many generators — random sampling, a
/// sweep, mutation of a previous search's winners — so a search names the one it
/// wants by id.
pub trait Generator: Send + Sync {
    /// The id a search config names this generator by.
    fn id(&self) -> &GeneratorId;

    /// The format the specs this generator produces are of.
    fn format(&self) -> &FormatId;

    /// The `[search.generator]` section as TOML text, minus its `id` key,
    /// translated to the opaque params blob [`Generator::generate`] reads.
    ///
    /// The section crosses as text, so a generator parses it with a TOML of
    /// its own choosing. The bytes enter the search id.
    fn translate_config(&self, toml: &str) -> Result<Vec<u8>>;

    /// Produces the search's candidate specs from its root seed and the blob
    /// [`Generator::translate_config`] produced, stamping each with
    /// [`Generator::format`]. Deterministic in both inputs.
    fn generate(&self, root_seed: u64, params: &[u8]) -> Result<Vec<Spec>>;
}

/// `Generator` is dyn-compatible: a host holds one behind a trait object for
/// the life of a session. The auto-trait supertraits are part of the contract —
/// a generator is reached from the threads a search drives.
const _: fn() = || {
    fn _object_safe(_: &dyn Generator) {}
};
