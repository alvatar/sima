//! [`CellularBackend`]: the one [`CellularEngine`] implementation, over any
//! [`CellularOps`].
//!
//! One engine parameterized by the adapter, so the device is opened, the
//! kernels are built, the uniform and seed buffers are packed, and the
//! reduction is run in exactly one place whatever backend answers.
//! `WgslEngine` and `CudaEngine` name two instantiations of it.

use sima_contracts::{DeviceBinding, DeviceInfo};
use sima_core::{Hash, Result, hash_bytes};

use crate::substrates::cellular::harness::{Trajectory, run};
use crate::substrates::cellular::ops::CellularOps;
use crate::substrates::cellular::reduce::{GridPair, ReduceKernels, reduce};
use crate::substrates::cellular::{CellularEngine, CellularEvaluation, EvaluationInput, Grid};

/// A cellular engine on the backend `O`: the device, the model's update kernel
/// built for it, and the four reduction passes.
pub(crate) struct CellularBackend<O: CellularOps> {
    /// Declared before `ops` so they drop first: struct fields drop in
    /// declaration order, and a kernel's handles belong to the device the
    /// adapter owns, so a kernel must be destroyed before it. A reorder would
    /// drop the device first and segfault at engine drop.
    kernel: O::Kernel,
    reduce: ReduceKernels<O>,
    ops: O,
}

impl<O: CellularOps> CellularEngine for CellularBackend<O> {
    const COMPILER_COMPONENT: &'static str = O::COMPILER_COMPONENT;
    const COMPILER_ID: &'static str = O::COMPILER_ID;

    /// `kernel` is whatever this backend loads: shader source on one, committed
    /// PTX on the other.
    fn build(device: Option<&DeviceBinding>, kernel: &'static str) -> Result<CellularBackend<O>> {
        // The binding names the device to open; without one, the backend's
        // default selection applies.
        let ops = O::open(device)?;
        let kernel = ops.kernel(kernel, O::ENTRY, crate::substrates::cellular::BLOCK_WIDTH)?;
        let reduce = ReduceKernels::build(&ops)?;
        Ok(CellularBackend {
            kernel,
            reduce,
            ops,
        })
    }

    fn enumerate_devices() -> Result<Vec<DeviceInfo>> {
        O::enumerate_devices()
    }

    fn device_desc(device: Option<&DeviceBinding>) -> Result<(String, String)> {
        O::device_desc(device)
    }

    fn reduce_digest() -> Hash {
        // The artifact this backend loads, whichever form it takes: editing a
        // shader and regenerating a PTX both move this digest, and with it every
        // task key of every domain on the backend.
        hash_bytes(O::REDUCE_SOURCE.as_bytes())
    }

    fn evaluate(&self, input: &EvaluationInput<'_>) -> Result<Box<dyn CellularEvaluation + '_>> {
        // The model's uniform buffer — binding 3 of the cellular convention,
        // bound after dims. A model with no uniform values declares no such
        // binding, so none is built: the bound list must match the bindings the
        // kernel declares.
        let uniforms = match input.uniforms {
            [] => None,
            values => Some(self.packed(bytemuck::cast_slice(values))?),
        };
        // Binding 4, present only for a kernel that consumes the candidate
        // seed: the u64 as two u32 words (low, high). Integers must travel as
        // integers, since a driver may rewrite a raw bit pattern parked in an
        // f32 slot, and the two backends must decode one genome the same way.
        // Held in this scope so it outlives the dispatch.
        let seed = match input.seed {
            Some(seed) => {
                let words = [seed as u32, (seed >> 32) as u32];
                Some(self.packed(bytemuck::cast_slice(&words))?)
            }
            None => None,
        };
        let params: Vec<&O::Buffer> = uniforms.iter().chain(seed.iter()).collect();
        let trajectory = run(
            &self.ops,
            &self.kernel,
            input.initial,
            input.steps,
            &params,
            input.step_base,
        )?;
        Ok(Box::new(Evaluation {
            ops: &self.ops,
            reduce: &self.reduce,
            trajectory,
            alive_channel: input.alive_channel,
            alive_min: input.alive_min,
        }))
    }
}

impl<O: CellularOps> CellularBackend<O> {
    /// A device buffer holding exactly `bytes`.
    fn packed(&self, bytes: &[u8]) -> Result<O::Buffer> {
        let mut buffer = self.ops.buffer(bytes.len())?;
        self.ops.upload(&mut buffer, bytes)?;
        Ok(buffer)
    }
}

/// One evaluation: the two ping-pong buffers left resident on the device, over
/// the engine that produced them.
struct Evaluation<'a, O: CellularOps> {
    ops: &'a O,
    reduce: &'a ReduceKernels<O>,
    trajectory: Trajectory<'a, O>,
    alive_channel: u32,
    alive_min: f32,
}

impl<O: CellularOps> CellularEvaluation for Evaluation<'_, O> {
    fn scalars(&self) -> Result<Vec<(String, f64)>> {
        // Reduced on the GPU over the two resident grids ($G_N$ and $G_{N-1}$)
        // before any readback. The alive rule is the model's own.
        reduce(
            self.ops,
            self.reduce,
            &GridPair {
                current: self.trajectory.current(),
                previous: self.trajectory.previous(),
                channels: self.trajectory.channels(),
                cell_count: self.trajectory.cell_count(),
                alive_channel: self.alive_channel,
                alive_min: self.alive_min,
            },
        )
    }

    fn grid(&self) -> Result<Grid> {
        self.trajectory.grid()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::substrates::cellular::reference::{SMOKE_PTX, SMOKE_WGSL};
    use crate::substrates::cellular::{CudaEngine, WgslEngine};

    /// A grid of exactly representable values that vary cell to cell, so a
    /// backend reading the wrong cell shows up as a different maximum. The
    /// values repeat every 13 cells; the maximum reaches 12 wherever the
    /// reduction covers a full period.
    fn a_grid() -> Grid {
        Grid::new(8, 8, 2, (0..128).map(|i| (i % 13) as f32).collect()).expect("grid")
    }

    /// The evaluation inputs both engines are given: no uniforms the smoke
    /// kernel reads, no seed, no step index.
    fn an_input(initial: &Grid, steps: u32) -> EvaluationInput<'_> {
        EvaluationInput {
            initial,
            steps,
            uniforms: &[],
            seed: None,
            step_base: None,
            alive_channel: 0,
            alive_min: 6.0,
        }
    }

    /// Building the engine opens a device, so every test here needs one.
    mod on_device {
        use super::*;

        /// The scalars an engine of `E` reduces `a_grid()` to after `steps`.
        fn scalars<E: CellularEngine>(source: &'static str, steps: u32) -> HashMap<String, f64> {
            let engine = E::build(None, source).expect("build the engine");
            engine
                .evaluate(&an_input(&a_grid(), steps))
                .expect("evaluate")
                .scalars()
                .expect("reduce")
                .into_iter()
                .collect()
        }

        #[test]
        fn the_engine_advances_and_reduces_a_grid() {
            // The engine is the dispatch harness and the reduction behind one
            // call. The smoke kernel is a neighborhood max, so advancing a grid
            // can only raise a cell: the mean after three steps is above the
            // mean it started from, and every scalar is finite.
            let start: f64 = a_grid()
                .data()
                .iter()
                .step_by(2)
                .map(|&v| f64::from(v))
                .sum::<f64>()
                / 64.0;
            for map in [
                scalars::<WgslEngine>(SMOKE_WGSL, 3),
                scalars::<CudaEngine>(SMOKE_PTX, 3),
            ] {
                assert!(
                    map.values().all(|v| v.is_finite()),
                    "every scalar is finite: {map:?}"
                );
                assert!(
                    map["c0.mean"] > start,
                    "a neighborhood max raises the mean: {} from {start}",
                    map["c0.mean"]
                );
            }
        }

        #[test]
        fn a_zero_step_evaluation_reports_no_activity() {
            // Both buffers hold the initial grid, so the pair is equal and the
            // reduction sees no change — the resumption case where a segment
            // advances nothing.
            assert_eq!(scalars::<WgslEngine>(SMOKE_WGSL, 0)["activity"], 0.0);
            assert_eq!(scalars::<CudaEngine>(SMOKE_PTX, 0)["activity"], 0.0);
        }

        #[test]
        fn both_backends_agree_on_the_same_grid() {
            // The port's exit criterion: the same kernel transcribed for the two
            // backends, advanced over the same grid, reduces to the same
            // scalars. A transcription error in either the step kernel or the
            // reduction — a swapped neighbor, a wrong partition boundary, a
            // misplaced output slot — moves a scalar far past the tolerance.
            //
            // The tolerance is relative and loose enough to survive a fused
            // multiply-add and a reassociation the two compilers make
            // differently, tight enough to catch a transcription error.
            const TOLERANCE: f64 = 1e-3;
            let wgsl = scalars::<WgslEngine>(SMOKE_WGSL, 4);
            let cuda = scalars::<CudaEngine>(SMOKE_PTX, 4);
            assert_eq!(
                wgsl.keys().collect::<std::collections::BTreeSet<_>>(),
                cuda.keys().collect::<std::collections::BTreeSet<_>>(),
                "both backends emit the same scalars"
            );
            for (name, wgsl_value) in &wgsl {
                let cuda_value = cuda[name];
                let scale = wgsl_value.abs().max(1.0);
                assert!(
                    (cuda_value - wgsl_value).abs() <= TOLERANCE * scale,
                    "{name}: CUDA {cuda_value} against WGSL {wgsl_value}"
                );
            }
        }

        #[test]
        fn the_engine_reads_the_uniforms_the_model_declares() {
            // A model with uniform values gets them at binding 3, and a kernel
            // with no uniform block gets no such binding: the bound list must
            // match what the kernel declares, and a zero-sized buffer is what
            // Vulkan rejects outright. The probe adds its single uniform to
            // every cell, so the value is read back exactly.
            const ADD_UNIFORM_WGSL: &str = r#"
@group(0) @binding(0) var<storage, read> in_grid: array<f32>;
@group(0) @binding(1) var<storage, read_write> out_grid: array<f32>;
@group(0) @binding(2) var<storage, read> dims: array<u32>;
@group(0) @binding(3) var<storage, read> uniforms: array<f32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let cell = gid.x;
    if (cell >= dims[0] * dims[1]) { return; }
    out_grid[cell] = in_grid[cell] + uniforms[0];
}
"#;
            let engine = WgslEngine::build(None, ADD_UNIFORM_WGSL).expect("build the engine");
            let initial = Grid::new(4, 4, 1, vec![0.0; 16]).expect("grid");
            let grid = engine
                .evaluate(&EvaluationInput {
                    initial: &initial,
                    steps: 3,
                    uniforms: &[0.25],
                    seed: None,
                    step_base: None,
                    alive_channel: 0,
                    alive_min: 1.0,
                })
                .expect("evaluate")
                .grid()
                .expect("download the grid");
            assert!(
                grid.data().iter().all(|&v| v == 0.75),
                "three steps of +0.25: {:?}",
                grid.data()
            );
        }

        #[test]
        fn a_device_the_backend_cannot_open_fails_naming_it() {
            // The binding names a class this machine's CUDA driver does not have
            // — an Intel integrated GPU is the live case, since the WGSL backend
            // reaches it and this one cannot. The failure names the device.
            let binding = DeviceBinding {
                class: sima_contracts::DeviceClass::new("8086:7d51").expect("class id"),
                member: 0,
            };
            let message = match CudaEngine::build(Some(&binding), SMOKE_PTX) {
                Err(error) => error.to_string(),
                Ok(_) => panic!("no CUDA device is an Intel iGPU"),
            };
            assert!(
                message.contains("8086"),
                "the error names the device: {message}"
            );
        }
    }
}
