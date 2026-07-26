//! [`CellularEngine`]: the seam between a cellular evaluation and the compute
//! substrate it runs on.

use sima_contracts::DeviceBinding;
use sima_core::{Hash, Result};

use crate::cellular::Grid;

/// A compute substrate an evaluation runs on: it holds a device and the kernels
/// compiled for it, it advances a grid, and it reduces the result.
///
/// The trait is one operation wide on purpose. Everything a candidate needs
/// around that operation — decoding its spec, igniting or resuming its grid,
/// deciding whether to keep a snapshot — belongs to the executor and is written
/// once for every substrate.
///
/// A substrate also answers for its own identity: the digest of the reduction
/// kernel it runs and the component that pins its compiler both enter the
/// environment of every domain built on it, so two substrates give a domain two
/// distinct environments and neither invalidates the other's stored results.
///
/// The trait is defined here rather than in a toolkit because the toolkits know
/// nothing of grids or scalars: they are compute libraries, and this is the
/// vocabulary of the cellular kind. Implementing it for a foreign substrate
/// violates no orphan rule, since the trait itself is local.
pub(crate) trait CellularEngine: Send + Sized + 'static {
    /// The name of the environment component that pins this substrate's
    /// compiler. Each substrate names its own, so a domain's environment says
    /// which compiler it depends on rather than only what that compiler is.
    const COMPILER_COMPONENT: &'static str;

    /// Canonical identity of the compiler and its output-affecting options,
    /// recorded in a domain's environment beside its kernel digest.
    const COMPILER_ID: &'static str;

    /// Opens `device` — or, for `None`, the toolkit's default selection — and
    /// compiles `kernel` along with this substrate's stats reduction.
    fn build(device: Option<&DeviceBinding>, kernel: &'static str) -> Result<Self>;

    /// The device's name and driver version, resolved without opening a device,
    /// for the worker handshake.
    fn device_desc(device: Option<&DeviceBinding>) -> Result<(String, String)>;

    /// The digest of this substrate's reduction kernel, an environment
    /// component of every domain that runs on it.
    fn reduce_digest() -> Hash;

    /// Advances `input.initial` by `input.steps`, leaving the final grid and
    /// the step before it resident on the device.
    fn evaluate(&self, input: &EvaluationInput<'_>) -> Result<Box<dyn CellularEvaluation + '_>>;
}

/// One evaluation's inputs: the grid to advance, how far to advance it, and the
/// per-candidate values its kernel reads.
///
/// The optional fields are the two kernel parameters a model opts into. A
/// kernel that consumes the candidate seed receives it as `seed`, and one that
/// advances against an absolute step index receives its first step as
/// `step_base`; a `None` binds no buffer at all, so a kernel declaring neither
/// is dispatched exactly as it would be if the options did not exist.
pub(crate) struct EvaluationInput<'a> {
    /// The grid the first step reads.
    pub initial: &'a Grid,
    /// Steps to advance.
    pub steps: u32,
    /// The model's uniform values, the kernel's first parameter after the grids
    /// and their dimensions.
    pub uniforms: &'a [f32],
    /// The candidate's seed, for a kernel that consumes it.
    pub seed: Option<u64>,
    /// The absolute index of the first step, for a kernel that advances against
    /// one. A resumed segment continues from the step its predecessor reached.
    pub step_base: Option<u64>,
    /// The channel a cell's liveness reads, and the minimum value on it a live
    /// cell holds — the model's own rule.
    pub alive_channel: u32,
    pub alive_min: f32,
}

/// One completed evaluation: the scalars its final grid pair reduces to, and
/// that final grid for a caller that asks.
///
/// The two are separate calls because the snapshot predicate decides from the
/// scalars whether the grid is worth committing, and a dropped snapshot must
/// skip the readback entirely — the bandwidth the predicate exists to save.
pub(crate) trait CellularEvaluation {
    /// Reduces the resident grid pair into the named scalars, in emission
    /// order.
    ///
    /// The reduction runs here rather than inside
    /// [`evaluate`](CellularEngine::evaluate) so that its faults stay
    /// separable from the advance's: the executor propagates a failed advance
    /// unconditionally, while a failed reduction is observational unless a
    /// snapshot predicate needs its values. One evaluation asks once.
    fn scalars(&self) -> Result<Vec<(String, f64)>>;

    /// Downloads the final grid. Called only when the snapshot predicate keeps
    /// it.
    fn grid(&self) -> Result<Grid>;
}
