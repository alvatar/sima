//! [`WgslEngine`]: the WGSL substrate behind the [`CellularEngine`] seam.

use sima_contracts::DeviceBinding;
use sima_core::{Hash, Result, hash_bytes};
use sima_toolkit_wgsl::{Buffer, Context, Kernel, selected_device_desc};

use crate::cellular::{
    CellularEngine, CellularEvaluation, EvaluationInput, Grid, GridPair, REDUCE_WGSL,
    ReduceKernels, Trajectory, reduce as reduce_pair, run,
};

/// The WGSL substrate: a Vulkan device, the model's update kernel compiled for
/// it, and the four reduction passes.
pub(crate) struct WgslEngine {
    /// Declared before `context` so they drop first: struct fields drop in
    /// declaration order, and the kernels' pipeline handles belong to the
    /// context's device, so a kernel must be destroyed before the context. A
    /// reorder would drop the device first and segfault at engine drop.
    kernel: Kernel,
    reduce: ReduceKernels,
    context: Context,
}

impl CellularEngine for WgslEngine {
    const COMPILER_COMPONENT: &'static str = "wgsl.compiler";
    const COMPILER_ID: &'static str = sima_toolkit_wgsl::COMPILER_ID;

    fn build(device: Option<&DeviceBinding>, kernel: &'static str) -> Result<WgslEngine> {
        // The binding names the device to open; without one, the toolkit's
        // default selection applies.
        let context = match device {
            Some(device) => Context::for_device(device.vendor_id, device.device_id, device.member)?,
            None => Context::new()?,
        };
        let kernel = context.kernel(kernel, "main")?;
        let reduce = ReduceKernels::build(&context)?;
        Ok(WgslEngine {
            kernel,
            reduce,
            context,
        })
    }

    fn device_desc(device: Option<&DeviceBinding>) -> Result<(String, String)> {
        // The toolkit speaks plain device ids; this is where the binding maps
        // to them.
        selected_device_desc(device.map(|d| (d.vendor_id, d.device_id, d.member)))
    }

    fn reduce_digest() -> Hash {
        hash_bytes(REDUCE_WGSL.as_bytes())
    }

    fn evaluate(&self, input: &EvaluationInput<'_>) -> Result<Box<dyn CellularEvaluation + '_>> {
        // The model's uniform buffer — binding 3 of the cellular convention,
        // bound after dims.
        let uniform_bytes: &[u8] = bytemuck::cast_slice(input.uniforms);
        let uniforms = self.context.buffer(uniform_bytes.len())?;
        self.context.upload(&uniforms, uniform_bytes)?;
        // Binding 4, present only for a kernel that consumes the candidate
        // seed: the u64 as two u32 words (low, high). Integers must travel as
        // integers, since a driver may rewrite a raw bit pattern parked in an
        // f32 slot. Held in this scope so it outlives the dispatch.
        let seed = match input.seed {
            Some(seed) => {
                let words = [seed as u32, (seed >> 32) as u32];
                let seed_bytes: &[u8] = bytemuck::cast_slice(&words);
                let buffer = self.context.buffer(seed_bytes.len())?;
                self.context.upload(&buffer, seed_bytes)?;
                Some(buffer)
            }
            None => None,
        };
        let mut params: Vec<&Buffer> = vec![&uniforms];
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
        Ok(Box::new(WgslEvaluation {
            context: &self.context,
            reduce: &self.reduce,
            trajectory,
            alive_channel: input.alive_channel,
            alive_min: input.alive_min,
        }))
    }
}

/// One WGSL evaluation: the two ping-pong buffers left resident on the device,
/// over the engine that produced them.
struct WgslEvaluation<'a> {
    context: &'a Context,
    reduce: &'a ReduceKernels,
    trajectory: Trajectory<'a>,
    alive_channel: u32,
    alive_min: f32,
}

impl CellularEvaluation for WgslEvaluation<'_> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reduction_digest_hashes_the_shader_source() {
        // The environment component this feeds is what makes an edit to the
        // reduction invalidate every task key of every domain on this
        // substrate, so it must cover the shader text exactly.
        assert_eq!(
            WgslEngine::reduce_digest(),
            hash_bytes(REDUCE_WGSL.as_bytes())
        );
    }

    #[test]
    fn the_compiler_component_pins_the_toolkit_identity() {
        // The value is the toolkit's own pinned constant, so a naga upgrade
        // that changes emitted SPIR-V moves every task key on this substrate.
        assert_eq!(WgslEngine::COMPILER_COMPONENT, "wgsl.compiler");
        assert_eq!(WgslEngine::COMPILER_ID, sima_toolkit_wgsl::COMPILER_ID);
    }

    /// A one-channel kernel that adds its single uniform to every cell,
    /// declaring the four bindings the cellular convention gives a model with
    /// no seed and no step: input grid, output grid, dimensions, uniforms.
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

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn the_engine_evaluates_exactly_as_the_harness_it_wraps() {
        // The engine is the dispatch harness and the reduction behind one
        // call, so its scalars and its final grid must be bit for bit what
        // calling the two directly produces. The fixed reduction topology makes
        // that an exact comparison, not an approximate one.
        let uniforms = [0.25_f32];
        let initial =
            Grid::new(4, 4, 1, (0..16).map(|i| i as f32).collect()).expect("initial grid");
        let steps = 3;

        let engine = WgslEngine::build(None, ADD_UNIFORM_WGSL).expect("build the engine");
        let evaluation = engine
            .evaluate(&EvaluationInput {
                initial: &initial,
                steps,
                uniforms: &uniforms,
                seed: None,
                step_base: None,
                alive_channel: 0,
                alive_min: 1.0,
            })
            .expect("evaluate");
        let engine_scalars = evaluation.scalars().expect("engine scalars");
        let engine_grid = evaluation.grid().expect("engine grid");

        // The same work through the toolkit directly, on a context of its own.
        let context = Context::new().expect("context");
        let kernel = context
            .kernel(ADD_UNIFORM_WGSL, "main")
            .expect("build kernel");
        let reduce = ReduceKernels::build(&context).expect("reduction kernels");
        let uniform_bytes: &[u8] = bytemuck::cast_slice(&uniforms);
        let uniform_buffer = context.buffer(uniform_bytes.len()).expect("uniform buffer");
        context
            .upload(&uniform_buffer, uniform_bytes)
            .expect("upload uniforms");
        let trajectory =
            run(&context, &kernel, &initial, steps, &[&uniform_buffer], None).expect("direct run");
        let direct_scalars = reduce_pair(
            &context,
            &reduce,
            &GridPair {
                current: trajectory.current(),
                previous: trajectory.previous(),
                channels: trajectory.channels(),
                cell_count: trajectory.cell_count(),
                alive_channel: 0,
                alive_min: 1.0,
            },
        )
        .expect("direct reduction");

        assert_eq!(
            engine_grid.to_bytes(),
            trajectory.grid().expect("direct grid").to_bytes()
        );
        let bits = |scalars: &[(String, f64)]| -> Vec<(String, u64)> {
            scalars
                .iter()
                .map(|(name, value)| (name.clone(), value.to_bits()))
                .collect()
        };
        assert_eq!(bits(&engine_scalars), bits(&direct_scalars));
    }
}
