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
    /// Whether the kernel declares the binding-4 seed buffer: the candidate's
    /// u64 seed as two u32 words, low then high. A model consuming the seed at
    /// runtime (an asynchronous update mask) opts in; the default keeps the
    /// binding-3 f32 buffer as the kernel's only parameter buffer.
    ///
    /// The kernel's storage bindings are ascending and positional: 0 input grid,
    /// 1 output grid, 2 dimensions, 3 the model uniforms, then the seed buffer if
    /// [`SEED_BUFFER`](CaModel::SEED_BUFFER) is set, then the step buffer if
    /// [`STEPPED`](CaModel::STEPPED) is set. For a model opting into both, seed is
    /// binding 4 and step is binding 5.
    const SEED_BUFFER: bool = false;
    /// Whether the model advances against an absolute step index. A stepped model
    /// receives the per-step index buffer (the harness uploads `step_base + i`
    /// before dispatch `i`) and commits framed continuation state — a `u64` step
    /// ahead of the grid ([`encode_continuation`](super::continuation)) — because
    /// a kernel that consumes the step makes the bare grid an incomplete
    /// continuation. The two are inseparable, so one const drives both. The
    /// default keeps the bare-grid state a model's kernel needs no step to
    /// continue.
    const STEPPED: bool = false;

    /// Packs the kernel's binding-3 uniform buffer from the genome and the
    /// shared params.
    fn uniforms(genome: &Self::Genome, shared: &CaParams) -> Vec<f32>;

    /// Builds the initial grid, specializing
    /// [`seeded_patch`](super::ignition::seeded_patch).
    fn ignite(shared: &CaParams, ignition: &Self::Ignition, seed: u64) -> Result<Grid>;

    /// Draws one genome for candidate `index` from the config and chain seed.
    fn sample(cfg: &Self::GenConfig, seed: u64, index: u64) -> Self::Genome;
}
