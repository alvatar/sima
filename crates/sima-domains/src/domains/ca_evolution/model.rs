//! [`CaModel`]: the seam between the generic CA domain and a concrete model.

use sima_core::{Codec, Result};

use super::params::CaParams;
use crate::cellular::Grid;
use crate::domains::translate::TomlConfig;

/// The rule-specific pieces the generic CA machinery needs. The domain owns the
/// substrate (grid, harness, executor/generator skeleton, environment, shared
/// params) and the codecs — the byte codec derived through [`Codec`], the TOML
/// parsing through [`TomlConfig`] — so a model declares only its fields, their
/// validating constructor, and the three rule functions below.
///
/// The machinery is monomorphized ([`CaExecutor<M>`](super::executor::CaExecutor),
/// not `dyn`), so the trait carries associated types and need not be
/// object-safe. The `'static` bound reflects that every model is a zero-sized
/// marker type; it lets the boxed executor be a `'static` trait object.
pub(crate) trait CaModel: 'static {
    /// The model's evolvable parameters (the spec payload). Sampled by the
    /// generator, never authored, so it needs a byte codec but no TOML parser.
    type Genome: Codec;
    /// The model's ignition configuration (its slice of `[run.params]`).
    type Ignition: Codec + TomlConfig;
    /// The model's generator sampling configuration (its `[run.generator]` keys
    /// beyond the shared `count`).
    type GenConfig: Codec + TomlConfig;

    /// Registered id, e.g. `"ca_evolution.gray_scott.v1"`. Used as both the
    /// format id and the generator id.
    const FORMAT_ID: &'static str;
    /// Environment component stem, e.g. `"ca_evolution.gray_scott"`; the builder
    /// forms `"{NAME}.executor"` and `"{NAME}.kernel"`.
    const NAME: &'static str;
    /// Executor version, the value of the `"{NAME}.executor"` component.
    const VERSION: &'static str;
    /// Channels per cell.
    const CHANNELS: u32;
    /// The WGSL update kernel source (`include_str!` of the co-located file).
    const KERNEL_WGSL: &'static str;

    /// Packs the kernel's binding-3 uniform buffer from the genome and the
    /// shared params.
    fn uniforms(genome: &Self::Genome, shared: &CaParams) -> Vec<f32>;

    /// Builds the initial grid, specializing
    /// [`seeded_patch`](super::ignition::seeded_patch).
    fn ignite(shared: &CaParams, ignition: &Self::Ignition, seed: u64) -> Result<Grid>;

    /// Draws one genome for candidate `index` from the config and chain seed.
    fn sample(cfg: &Self::GenConfig, seed: u64, index: u64) -> Self::Genome;
}
