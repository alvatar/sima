//! The generator contract.
//!
//! A generator turns a run's root seed and generator params into the run's
//! candidate specs, deterministically: the same `(root_seed, params, format)`
//! yields the same specs in the same order. It produces the search axis;
//! evaluation settings travel separately as params.

use sima_core::Result;
use sima_model::{FormatId, GeneratorId, Spec};

/// Produces a run's candidate specs deterministically. Same
/// `(root_seed, params, format)` → the same specs, in the same order.
pub trait Generator {
    /// The generator id this implementation registers under. The pipeline
    /// (M1.6) dispatches a run to the generator whose id matches the run
    /// config's `generator.id`.
    fn id(&self) -> &GeneratorId;

    /// Produce the run's candidate specs. `root_seed` is the run's root seed;
    /// `params` is the generator's own settings blob (opaque, from
    /// `RunConfig.generator.params`); `format` is stamped into every produced
    /// spec. Deterministic in all three.
    fn generate(&self, root_seed: u64, params: &[u8], format: &FormatId) -> Result<Vec<Spec>>;
}

/// `Generator` is dyn-compatible: it carries no auto-trait supertraits, and
/// use sites add `Send`/`Sync` where they store it as a trait object (D7).
const _: fn() = || {
    fn _object_safe(_: &dyn Generator) {}
};
