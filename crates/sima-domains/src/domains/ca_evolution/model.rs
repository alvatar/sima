//! [`CaModel`]: the seam between the generic CA domain and a concrete model.

use sima_core::{Dec, Enc, Result};

use super::params::CaParams;
use crate::cellular::Grid;

/// The rule-specific pieces the generic CA machinery needs. The domain owns the
/// substrate (grid, harness, executor/generator skeleton, environment, shared
/// params); a model owns its genome, kernel, channels, uniform packing, and
/// ignition. Implement this to add a cellular-automaton model.
///
/// The machinery is monomorphized ([`CaExecutor<M>`](super::executor::CaExecutor),
/// not `dyn`), so the trait carries associated types and need not be
/// object-safe. The `'static` bound reflects that every model is a zero-sized
/// marker type; it lets the boxed executor be a `'static` trait object.
pub(crate) trait CaModel: 'static {
    /// The model's evolvable parameters (the spec payload).
    type Genome;
    /// The model's ignition configuration (its slice of `[run.params]`).
    type Ignition;
    /// The model's generator sampling configuration (its `[run.generator]` keys
    /// beyond the shared `count`).
    type GenConfig;

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

    // --- genome codec (identity-bearing) ---

    /// Parses a genome from its canonical spec-payload bytes.
    fn decode_genome(bytes: &[u8]) -> Result<Self::Genome>;
    /// The genome's canonical spec-payload bytes.
    fn encode_genome(genome: &Self::Genome) -> Vec<u8>;

    // --- execution ---

    /// Packs the kernel's binding-3 uniform buffer from the genome and the
    /// shared params (Gray-Scott: `[f, k, du, dv, dt]`).
    fn uniforms(genome: &Self::Genome, shared: &CaParams) -> Vec<f32>;
    /// Builds the initial grid, specializing
    /// [`seeded_patch`](super::ignition::seeded_patch).
    fn ignite(shared: &CaParams, ignition: &Self::Ignition, seed: u64) -> Result<Grid>;

    // --- params translation (the model's own `[run.params]` keys) ---

    /// Reads the model's ignition keys from the `[run.params]` table, with the
    /// shared keys already stripped, rejecting any key it does not define.
    fn parse_ignition(table: &toml::Table) -> Result<Self::Ignition>;
    /// Appends the ignition's canonical form after the shared params fields.
    fn encode_ignition(ignition: &Self::Ignition, enc: &mut Enc);
    /// Reads the ignition's canonical form from the params blob remainder.
    fn decode_ignition(dec: &mut Dec) -> Result<Self::Ignition>;

    // --- generator ---

    /// Reads the model's sampling keys from the `[run.generator]` table, with the
    /// shared `count` already stripped, rejecting any key it does not define.
    fn parse_gen_config(table: &toml::Table) -> Result<Self::GenConfig>;
    /// The generator config's standalone canonical bytes.
    fn encode_gen_config(cfg: &Self::GenConfig) -> Vec<u8>;
    /// Parses the generator config from its standalone canonical bytes.
    fn decode_gen_config(bytes: &[u8]) -> Result<Self::GenConfig>;
    /// Draws one genome for candidate `index` from the config and chain seed.
    fn sample(cfg: &Self::GenConfig, seed: u64, index: u64) -> Self::Genome;
    /// Genome identity for dedup (the generator rejects duplicate draws).
    fn genome_key(genome: &Self::Genome) -> Vec<u8> {
        Self::encode_genome(genome)
    }
}
