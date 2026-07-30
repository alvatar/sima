//! [`CudaEngine`]: the CUDA backend behind the [`CellularEngine`] boundary.

use sima_contracts::{DeviceBinding, DeviceClass, DeviceInfo, DeviceType};
use sima_core::{Hash, Result, hash_bytes};
use sima_toolkit_cuda::{Buffer, Context, Kernel, selected_device_desc};

use crate::substrates::cellular::cuda::reduce::{
    GridPair, REDUCE_PTX, ReduceKernels, reduce as reduce_pair,
};
use crate::substrates::cellular::cuda::step::{BLOCK_WIDTH, Trajectory, run};
use crate::substrates::cellular::{CellularEngine, CellularEvaluation, EvaluationInput, Grid};

/// The entry point every cellular CUDA kernel declares. `main` is spoken for in
/// C++, so the convention's single entry point takes the name the toolkit's own
/// kernels use.
const ENTRY: &str = "main_kernel";

/// The CUDA backend: an NVIDIA device, the model's update kernel loaded onto
/// it, and the four reduction passes.
pub(crate) struct CudaEngine {
    /// Declared before `context` so they drop first: struct fields drop in
    /// declaration order, and the kernels' function handles belong to modules
    /// loaded into the context, so a kernel must be destroyed before the
    /// context.
    kernel: Kernel,
    reduce: ReduceKernels,
    context: Context,
}

impl CellularEngine for CudaEngine {
    const COMPILER_COMPONENT: &'static str = "cuda.compiler";
    const COMPILER_ID: &'static str = sima_toolkit_cuda::COMPILER_ID;

    /// `kernel` is committed PTX rather than source: a CUDA kernel is compiled
    /// on a developer machine and the artifact travels with the build, so
    /// nothing compiles CUDA C while a run executes.
    fn build(device: Option<&DeviceBinding>, kernel: &'static str) -> Result<CudaEngine> {
        // The binding names the device to open; without one, the toolkit's
        // default selection applies.
        let context = match device {
            Some(device) => Context::for_class(device.class().as_str(), device.member)?,
            None => Context::new()?,
        };
        let kernel = context.kernel(kernel, ENTRY, BLOCK_WIDTH)?;
        let reduce = ReduceKernels::build(&context)?;
        Ok(CudaEngine {
            kernel,
            reduce,
            context,
        })
    }

    fn enumerate_devices() -> Result<Vec<DeviceInfo>> {
        sima_toolkit_cuda::enumerate_devices()?
            .into_iter()
            .map(from_toolkit)
            .collect()
    }

    fn device_desc(device: Option<&DeviceBinding>) -> Result<(String, String)> {
        // The toolkit reads back the class names it minted; this is where the
        // binding maps onto them.
        selected_device_desc(device.map(|d| (d.class().as_str(), d.member)))
    }

    fn reduce_digest() -> Hash {
        // The PTX, not the CUDA C: the committed artifact is what the device
        // executes, and it is what a regenerated kernel changes.
        hash_bytes(REDUCE_PTX.as_bytes())
    }

    fn evaluate(&self, input: &EvaluationInput<'_>) -> Result<Box<dyn CellularEvaluation + '_>> {
        // The model's uniform buffer — parameter 3 of the cellular convention,
        // passed after dims. A model with no uniform values declares no such
        // parameter, so none is built: the argument list must match the
        // parameters the kernel declares.
        let uniforms = match input.uniforms {
            [] => None,
            values => {
                let bytes: &[u8] = bytemuck::cast_slice(values);
                let mut buffer = self.context.buffer(bytes.len())?;
                self.context.upload(&mut buffer, bytes)?;
                Some(buffer)
            }
        };
        // Parameter 4, present only for a kernel that consumes the candidate
        // seed: the u64 as two u32 words (low, high). Integers travel as
        // integers, matching the WGSL side, so one model's genome decodes the
        // same on either backend. Held in this scope so it outlives the
        // dispatch.
        let seed = match input.seed {
            Some(seed) => {
                let words = [seed as u32, (seed >> 32) as u32];
                let seed_bytes: &[u8] = bytemuck::cast_slice(&words);
                let mut buffer = self.context.buffer(seed_bytes.len())?;
                self.context.upload(&mut buffer, seed_bytes)?;
                Some(buffer)
            }
            None => None,
        };
        let mut params: Vec<&Buffer> = Vec::with_capacity(2);
        if let Some(uniforms) = uniforms.as_ref() {
            params.push(uniforms);
        }
        if let Some(seed) = seed.as_ref() {
            params.push(seed);
        }
        let trajectory = run(
            &self.context,
            &self.kernel,
            input.initial,
            input.steps,
            &params,
            input.step_base,
        )?;
        Ok(Box::new(CudaEvaluation {
            context: &self.context,
            reduce: &self.reduce,
            trajectory,
            alive_channel: input.alive_channel,
            alive_min: input.alive_min,
        }))
    }
}

/// One CUDA evaluation: the two ping-pong buffers left resident on the device,
/// over the engine that produced them.
struct CudaEvaluation<'a> {
    context: &'a Context,
    reduce: &'a ReduceKernels,
    trajectory: Trajectory<'a>,
    alive_channel: u32,
    alive_min: f32,
}

impl CellularEvaluation for CudaEvaluation<'_> {
    fn scalars(&self) -> Result<Vec<(String, f64)>> {
        // Reduced on the GPU over the two resident grids ($G_N$ and $G_{N-1}$)
        // before any readback. The alive rule is the model's own.
        reduce_pair(
            self.context,
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

/// The CUDA toolkit's enumerated device in the domains layer's vocabulary. CUDA
/// discovers GPUs alone, so its two categories map straight across and the
/// remaining ones never arise from it.
fn from_toolkit(device: sima_toolkit_cuda::DeviceInfo) -> Result<DeviceInfo> {
    Ok(DeviceInfo {
        class: DeviceClass::new(device.class)?,
        name: device.name,
        device_type: match device.device_type {
            sima_toolkit_cuda::DeviceType::Discrete => DeviceType::Discrete,
            sima_toolkit_cuda::DeviceType::Integrated => DeviceType::Integrated,
        },
        member: device.member,
    })
}

#[cfg(test)]
mod tests {
    use sima_contracts::DeviceClass;

    use super::*;

    #[test]
    fn every_toolkit_device_category_has_a_counterpart() {
        for (toolkit, neutral) in [
            (
                sima_toolkit_cuda::DeviceType::Discrete,
                DeviceType::Discrete,
            ),
            (
                sima_toolkit_cuda::DeviceType::Integrated,
                DeviceType::Integrated,
            ),
        ] {
            let device = sima_toolkit_cuda::DeviceInfo {
                class: sima_toolkit_cuda::class_of(0x10de, 0x2d39),
                name: "NVIDIA RTX PRO 2000".to_string(),
                device_type: toolkit,
                member: 0,
            };
            assert_eq!(from_toolkit(device).expect("convert").device_type, neutral);
        }
    }

    #[test]
    fn a_device_carries_through_the_conversion_verbatim() {
        let device = sima_toolkit_cuda::DeviceInfo {
            class: sima_toolkit_cuda::class_of(0x10de, 0x2684),
            name: "NVIDIA GeForce RTX 4090".to_string(),
            device_type: sima_toolkit_cuda::DeviceType::Discrete,
            member: 1,
        };
        assert_eq!(
            from_toolkit(device).expect("convert"),
            DeviceInfo {
                class: DeviceClass::new("10de:2684").expect("class id"),
                name: "NVIDIA GeForce RTX 4090".to_string(),
                device_type: DeviceType::Discrete,
                member: 1,
            }
        );
    }

    #[test]
    fn enumeration_answers_on_a_machine_with_no_cuda_device() {
        // The probe the worker runs must answer rather than fail on a host with
        // no NVIDIA driver, so the orchestrator reads "no device" instead of a
        // failed probe.
        CudaEngine::enumerate_devices().expect("enumeration answers on any machine");
    }

    /// The smoke kernel both backends ship: a toroidal neighborhood max over
    /// the cellular convention's first three parameters.
    const SMOKE_PTX: &str = include_str!("../../../kernels/smoke.ptx");
    const SMOKE_WGSL: &str = include_str!("../../../shaders/smoke.wgsl");

    /// The relative tolerance the two backends' scalars are held to. Loose
    /// enough to survive a fused multiply-add and a reassociation the two
    /// compilers make differently, tight enough to catch a transcription error
    /// in the port.
    const TOLERANCE: f64 = 1e-3;

    #[test]
    fn the_reduction_digest_hashes_the_committed_ptx() {
        // The environment component this feeds is what makes a regenerated
        // reduction invalidate every task key of every domain on this
        // backend, so it must cover the artifact that executes.
        assert_eq!(
            CudaEngine::reduce_digest(),
            hash_bytes(REDUCE_PTX.as_bytes())
        );
    }

    #[test]
    fn the_compiler_component_is_distinct_from_the_other_backends() {
        // The component name enters the environment, so two backends must
        // name different ones: that is what keeps one backend's stored
        // results out of the other's task keys.
        assert_eq!(CudaEngine::COMPILER_COMPONENT, "cuda.compiler");
        assert_ne!(
            CudaEngine::COMPILER_COMPONENT,
            crate::substrates::cellular::WgslEngine::COMPILER_COMPONENT
        );
        assert_eq!(CudaEngine::COMPILER_ID, sima_toolkit_cuda::COMPILER_ID);
    }

    /// A grid whose values are distinct and exactly representable, so a
    /// backend that read the wrong cell shows up as a different maximum.
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

    /// Requires a CUDA device.
    #[test]
    fn the_engine_advances_and_reduces_a_grid() {
        // The engine is the dispatch harness and the reduction behind one call.
        // The smoke kernel is a neighborhood max, so advancing a grid can only
        // raise a cell: the mean after three steps is above the mean of the
        // grid it started from, and every scalar is finite.
        let engine = CudaEngine::build(None, SMOKE_PTX).expect("build the engine");
        let initial = a_grid();
        let start: f64 = initial
            .data()
            .iter()
            .step_by(2)
            .map(|&v| f64::from(v))
            .sum::<f64>()
            / 64.0;
        let evaluation = engine
            .evaluate(&an_input(&initial, 3))
            .expect("evaluate the grid");
        let scalars: std::collections::HashMap<String, f64> =
            evaluation.scalars().expect("reduce").into_iter().collect();
        assert!(
            scalars.values().all(|v| v.is_finite()),
            "every scalar is finite: {scalars:?}"
        );
        assert!(
            scalars["c0.mean"] > start,
            "a neighborhood max raises the mean: {} from {start}",
            scalars["c0.mean"]
        );
        let grid = evaluation.grid().expect("download the grid");
        assert_eq!(grid.width(), initial.width());
        assert_eq!(grid.channels(), initial.channels());
    }

    /// Requires both a CUDA device and a Vulkan device.
    #[test]
    fn both_backends_agree_on_the_same_grid() {
        // The port's exit criterion: the same kernel transcribed for the two
        // backends, advanced over the same grid, reduces to the same scalars.
        // A transcription error in either the step kernel or the reduction —
        // a swapped neighbor, a wrong partition boundary, a misplaced output
        // slot — moves a scalar far past the tolerance.
        let initial = a_grid();
        let cuda = CudaEngine::build(None, SMOKE_PTX).expect("build the CUDA engine");
        let wgsl = crate::substrates::cellular::WgslEngine::build(None, SMOKE_WGSL)
            .expect("build the WGSL engine");
        let cuda_scalars = cuda
            .evaluate(&an_input(&initial, 4))
            .expect("CUDA evaluation")
            .scalars()
            .expect("CUDA reduction");
        let wgsl_scalars = wgsl
            .evaluate(&an_input(&initial, 4))
            .expect("WGSL evaluation")
            .scalars()
            .expect("WGSL reduction");
        assert_eq!(
            cuda_scalars.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            wgsl_scalars.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            "both backends emit the same scalars in the same order"
        );
        for ((name, cuda_value), (_, wgsl_value)) in cuda_scalars.iter().zip(&wgsl_scalars) {
            let scale = wgsl_value.abs().max(1.0);
            assert!(
                (cuda_value - wgsl_value).abs() <= TOLERANCE * scale,
                "{name}: CUDA {cuda_value} against WGSL {wgsl_value}"
            );
        }
    }

    /// Requires a CUDA device.
    #[test]
    fn a_zero_step_evaluation_reports_no_activity() {
        // Both buffers hold the initial grid, so the pair is equal and the
        // reduction sees no change — the resumption case where a segment
        // advances nothing.
        let engine = CudaEngine::build(None, SMOKE_PTX).expect("build the engine");
        let initial = a_grid();
        let scalars: std::collections::HashMap<String, f64> = engine
            .evaluate(&an_input(&initial, 0))
            .expect("evaluate")
            .scalars()
            .expect("reduce")
            .into_iter()
            .collect();
        assert_eq!(scalars["activity"], 0.0);
    }

    /// Requires a CUDA device.
    #[test]
    fn a_device_the_backend_cannot_open_fails_naming_it() {
        // The binding names a class this machine's CUDA driver does not have —
        // an Intel integrated GPU is the live case, since the WGSL backend
        // reaches it and this one cannot. The failure names the device.
        let binding = DeviceBinding {
            class: DeviceClass::new("8086:7d51").expect("class id"),
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
