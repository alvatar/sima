//! WGSL-to-SPIR-V compilation with `naga`, and the identity inputs a domain
//! records for a kernel: the source digest and the compiler id.

use naga::back::spv;
use naga::valid::{Capabilities, ValidationFlags, Validator};

use sima_core::{Error, Hash, Result, hash_bytes};

/// Canonical identity of the compiler and the output-affecting options.
///
/// A domain records this next to a kernel's source digest so a run's engine
/// identity covers how the WGSL was lowered to SPIR-V, not only its source.
/// The value is a pinned constant; the known-answer test in this module fails
/// if a `naga` upgrade changes emitted SPIR-V, forcing a deliberate update
/// here in the same change that bumps the dependency.
pub const COMPILER_ID: &str = "naga 26.0.0; spirv=1.5; opt=none";

/// Target SPIR-V version. `1.5` is accepted by Vulkan 1.3.
const SPIRV_VERSION: (u8, u8) = (1, 5);

/// A compiled kernel: the parsed naga module for binding reflection, and the
/// SPIR-V words handed to Vulkan as the shader module.
pub(crate) struct Compiled {
    pub module: naga::Module,
    pub spirv: Vec<u32>,
}

/// blake3 digest of the WGSL source bytes — an identity input for a domain.
pub fn source_digest(wgsl: &str) -> Hash {
    hash_bytes(wgsl.as_bytes())
}

/// Compiles WGSL to SPIR-V for the named compute entry point.
///
/// The three naga stages — parse, validate, emit — each map their failure to
/// [`Error::Gpu`] with a context string. The emitter's writer flags are fixed
/// so the output depends only on the source and the pinned compiler version,
/// keeping [`COMPILER_ID`] and the known-answer test meaningful.
pub(crate) fn compile(wgsl: &str, entry: &str) -> Result<Compiled> {
    let module =
        naga::front::wgsl::parse_str(wgsl).map_err(|e| Error::Gpu(format!("parse WGSL: {e}")))?;
    let info = Validator::new(ValidationFlags::all(), Capabilities::default())
        .validate(&module)
        .map_err(|e| Error::Gpu(format!("validate WGSL: {e}")))?;
    let options = spv::Options {
        lang_version: SPIRV_VERSION,
        flags: spv::WriterFlags::empty(),
        ..Default::default()
    };
    let pipeline = spv::PipelineOptions {
        shader_stage: naga::ShaderStage::Compute,
        entry_point: entry.to_string(),
    };
    let spirv = spv::write_vec(&module, &info, &options, Some(&pipeline))
        .map_err(|e| Error::Gpu(format!("emit SPIR-V for entry point '{entry}': {e}")))?;
    Ok(Compiled { module, spirv })
}

/// The group-0 storage-buffer binding numbers a kernel exposes, ascending.
///
/// Reflected from the naga module: every global variable in the storage
/// address space that carries a group-0 resource binding contributes its
/// binding number. These map one-to-one to the `STORAGE_BUFFER` descriptors of
/// the kernel's single descriptor set, and the ascending order fixes both the
/// set-layout binding order and the order buffers bind at dispatch.
pub(crate) fn storage_bindings(module: &naga::Module) -> Vec<u32> {
    let mut bindings: Vec<u32> = module
        .global_variables
        .iter()
        .filter_map(|(_, var)| match (&var.space, &var.binding) {
            (naga::AddressSpace::Storage { .. }, Some(binding)) if binding.group == 0 => {
                Some(binding.binding)
            }
            _ => None,
        })
        .collect();
    bindings.sort_unstable();
    bindings
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::hash_bytes;

    /// Fixed WGSL sample for the known-answer tests: two group-0 storage
    /// buffers, one compute entry point. Pinning its compiled output guards
    /// against a naga upgrade silently changing emitted SPIR-V.
    const SAMPLE: &str = "\
@group(0) @binding(0) var<storage, read> in_buf: array<u32>;
@group(0) @binding(1) var<storage, read_write> out_buf: array<u32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= arrayLength(&out_buf)) { return; }
    out_buf[i] = in_buf[i] * 2u + 1u;
}
";

    /// Little-endian byte serialization of the SPIR-V words, for hashing.
    fn spirv_bytes(words: &[u32]) -> Vec<u8> {
        words.iter().flat_map(|w| w.to_le_bytes()).collect()
    }

    #[test]
    fn compile_produces_non_empty_spirv() {
        let compiled = compile(SAMPLE, "main").expect("compile sample");
        assert!(!compiled.spirv.is_empty());
    }

    #[test]
    fn spirv_output_is_stable() {
        let compiled = compile(SAMPLE, "main").expect("compile sample");
        let digest = hash_bytes(&spirv_bytes(&compiled.spirv));
        assert_eq!(digest.to_string(), SPIRV_DIGEST);
    }

    #[test]
    fn source_digest_is_stable() {
        assert_eq!(source_digest(SAMPLE).to_string(), SOURCE_DIGEST);
    }

    #[test]
    fn reflects_group0_storage_bindings_sorted() {
        let compiled = compile(SAMPLE, "main").expect("compile sample");
        let bindings = storage_bindings(&compiled.module);
        assert_eq!(bindings, vec![0, 1]);
    }

    #[test]
    fn compiler_id_is_pinned() {
        assert_eq!(COMPILER_ID, "naga 26.0.0; spirv=1.5; opt=none");
    }

    /// Pinned blake3 of the sample's SPIR-V (little-endian words).
    const SPIRV_DIGEST: &str = "371649a4a9eb9519680f198ddffb78b60cbc4a32f9073186067a1f962a931e71";
    /// Pinned blake3 of the sample's WGSL source bytes.
    const SOURCE_DIGEST: &str = "0a87dee74bf40f3e867a062c7c3bc440f8ec90501775efe0601b9126d68576fe";
}
