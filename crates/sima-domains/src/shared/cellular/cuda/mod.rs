//! The CUDA substrate's dispatch loop and reduction, behind
//! [`CudaEngine`](crate::shared::cellular::CudaEngine).
//!
//! Both files are transcriptions of their WGSL counterparts one level up:
//! [`step`] mirrors [`cellular::step`](crate::shared::cellular::step) and [`reduce`]
//! mirrors [`cellular::reduce`](crate::shared::cellular::reduce). Reading a pair side
//! by side is the point — where the CUDA version departs from the WGSL one, the
//! departure carries an inline comment saying why.
//!
//! What the two substrates genuinely share stays shared: the scalar naming, the
//! channel bound, and the partition count all come from
//! [`cellular::reduce`](crate::shared::cellular::reduce), so the two reductions cannot
//! drift into folding differently.

pub(crate) mod reduce;
pub(crate) mod step;
