//! CUDA-C-to-PTX compilation with NVRTC, and the identity a domain records for
//! a kernel.
//!
//! Compilation happens on a developer machine, once per kernel edit, and its
//! output is committed beside the source. Nothing on the execution path calls
//! into this module: a worker loads the committed PTX and the driver's
//! just-in-time compiler takes it from there, so no worker image ships a
//! compiler and no run depends on the CUDA toolkit being installed.
//!
//! What a domain records for a CUDA kernel is therefore the digest of the PTX
//! itself, the artifact that executes, rather than a source digest paired with
//! a compiler version the run has to trust. [`COMPILER_ID`] states only what
//! that PTX targets.

use cudarc::nvrtc::{CompileOptions, compile_ptx_with_opts};

use sima_core::{Error, Result};

/// Canonical identity of the form a CUDA kernel executes in.
///
/// A domain records this next to its kernel's PTX digest. The digest already
/// covers the compiler's exact output, so this names only the architecture the
/// PTX targets — the one property of the artifact that decides which cards can
/// load it.
pub const COMPILER_ID: &str = "ptx; arch=compute_75";

/// The NVRTC options every committed PTX is produced under, in order.
///
/// `compute_75` is old enough to be broadly supported and, because PTX is
/// forward compatible, the driver's just-in-time compiler targets any newer
/// architecture from it. Fused multiply-add stays on, the compiler's own
/// default: it is the arithmetic a CUDA program would normally have, and the
/// tolerance a cross-substrate comparison holds to is set with that in mind.
///
/// There is no optimization level here. NVRTC optimizes device code fully by
/// default and rejects `-O3` outright; its `--dopt` switch is meaningful only
/// beside the debug flag `-G`, which no committed PTX is built with.
///
/// The regeneration test recompiles each committed kernel under exactly these
/// options and asserts the result matches what is committed, so the pair cannot
/// drift apart silently.
pub const PTX_OPTIONS: [&str; 2] = ["--gpu-architecture=compute_75", "--fmad=true"];

/// Compiles CUDA C to PTX under [`PTX_OPTIONS`].
///
/// Needs `libnvrtc`, which comes with the CUDA toolkit or, without one, from the
/// `nvidia-cuda-nvrtc-cu12` wheel — it is a userspace compiler that needs
/// neither a driver nor a device. Compilation failures carry NVRTC's own
/// diagnostics, which name the offending source line.
pub fn compile(source: &str) -> Result<String> {
    let options = CompileOptions {
        options: PTX_OPTIONS
            .iter()
            .map(|option| option.to_string())
            .collect(),
        ..Default::default()
    };
    let ptx = compile_ptx_with_opts(source, options)
        .map_err(|e| Error::Gpu(format!("compile CUDA C to PTX: {e}")))?;
    Ok(ptx.to_src())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed CUDA C sample for the compilation tests: one entry point over two
    /// buffers, matching the shape every kernel in this project has.
    const SAMPLE: &str = "\
extern \"C\" __global__ void __launch_bounds__(64) main_kernel(
    const unsigned int* in_buf, unsigned int* out_buf, unsigned int count) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) { return; }
    out_buf[i] = in_buf[i] * 2u + 1u;
}
";

    #[test]
    fn compiler_id_is_pinned() {
        // The identity a domain records must change deliberately, in the same
        // edit that changes what the committed PTX targets.
        assert_eq!(COMPILER_ID, "ptx; arch=compute_75");
    }

    #[test]
    fn the_options_name_the_architecture_the_compiler_id_states() {
        // The two are one decision written twice; a target change must move
        // both, so the constants are checked against each other.
        assert!(
            PTX_OPTIONS.contains(&"--gpu-architecture=compute_75"),
            "the options target the architecture COMPILER_ID names"
        );
    }

    /// Requires `libnvrtc`. Run with `cargo test -- --ignored` on a machine
    /// that has it.
    #[test]
    #[ignore = "requires libnvrtc"]
    fn compile_produces_ptx_for_the_declared_architecture() {
        let ptx = compile(SAMPLE).expect("compile the sample");
        assert!(
            ptx.contains(".target sm_75"),
            "the PTX targets the declared architecture: {ptx}"
        );
        assert!(
            ptx.contains("main_kernel"),
            "the PTX carries the entry point: {ptx}"
        );
    }

    /// Requires `libnvrtc`. Run with `cargo test -- --ignored` on a machine
    /// that has it.
    #[test]
    #[ignore = "requires libnvrtc"]
    fn compile_rejects_malformed_cuda_c() {
        let result = compile("__global__ void broken() { let x = ; }");
        match result {
            Err(Error::Gpu(message)) => {
                assert!(message.contains("compile CUDA C to PTX"), "{message}");
            }
            other => panic!("expected a Gpu compile error, got {other:?}"),
        }
    }

    /// Requires `libnvrtc`. Run with `cargo test -- --ignored` on a machine
    /// that has it.
    #[test]
    #[ignore = "requires libnvrtc"]
    fn compilation_is_reproducible() {
        // Committing generated PTX rests on this: the same source under the
        // same options yields the same text, so the regeneration test compares
        // like with like.
        assert_eq!(
            compile(SAMPLE).expect("first compile"),
            compile(SAMPLE).expect("second compile")
        );
    }
}
