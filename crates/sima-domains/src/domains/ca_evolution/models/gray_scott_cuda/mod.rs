//! The Gray-Scott reaction-diffusion model on the CUDA backend: the same rule
//! as [`GrayScott`](super::gray_scott::GrayScott), evaluated through committed
//! PTX instead of a compiled shader.
//!
//! Everything that defines the rule is shared with the WGSL program — the
//! genome, the ignition, the sampling box, the uniform packing, and the liveness
//! rule — so the two cannot drift apart as models. What differs is the artifact
//! the device executes and the identity that artifact gives the program: a
//! format id of its own, and an environment naming the CUDA compiler and the
//! PTX digest.
//!
//! It is a distinct program rather than a second backend for one program.
//! Numerical agreement between the two is a tolerance, not an equality, so
//! sharing an identity would let a task key resolve to a result the other
//! backend produced.

use sima_core::Result;

use super::super::model::CaModel;
use super::super::params::CaParams;
use super::gray_scott::{GrayScott, GrayScottGenConfig, GrayScottGenome, GrayScottIgnition};
use crate::substrates::cellular::Grid;

/// The Gray-Scott model bound to the CUDA backend. Zero-sized, like every
/// model: the generic machinery is monomorphized over it.
pub(crate) struct GrayScottCuda;

impl CaModel for GrayScottCuda {
    type Genome = GrayScottGenome;
    type Ignition = GrayScottIgnition;
    type GenConfig = GrayScottGenConfig;

    const FORMAT_ID: &'static str = "ca_evolution.gray_scott_cuda.v1";
    const NAME: &'static str = "ca_evolution.gray_scott_cuda";
    const VERSION: &'static str = "v1";
    const CHANNELS: u32 = GrayScott::CHANNELS;
    const ALIVE_CHANNEL: u32 = GrayScott::ALIVE_CHANNEL;
    const ALIVE_MIN: f32 = GrayScott::ALIVE_MIN;
    /// The committed PTX, not the CUDA C beside it: the engine loads the
    /// artifact the device executes, and the environment hashes what it loads.
    const KERNEL_SOURCE: &'static str = include_str!("gray_scott.ptx");

    fn uniforms(genome: &GrayScottGenome, shared: &CaParams) -> Vec<f32> {
        GrayScott::uniforms(genome, shared)
    }

    fn ignite(shared: &CaParams, ignition: &GrayScottIgnition, seed: u64) -> Result<Grid> {
        GrayScott::ignite(shared, ignition, seed)
    }

    fn sample(cfg: &GrayScottGenConfig, seed: u64, index: u64) -> GrayScottGenome {
        GrayScott::sample(cfg, seed, index)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::hash_bytes;
    use sima_model::EnvironmentValue;

    use sima_contracts::Domain;

    use super::super::super::domain::CaDomain;
    use super::*;
    use crate::substrates::cellular::{CudaEngine, WgslEngine};

    /// The CUDA C the committed PTX is generated from.
    const KERNEL_CU: &str = include_str!("gray_scott.cu");

    #[test]
    fn the_committed_ptx_declares_the_convention_entry_point() {
        assert!(
            GrayScottCuda::KERNEL_SOURCE.contains(".entry main_kernel("),
            "the committed PTX declares the cellular entry point"
        );
        assert!(
            GrayScottCuda::KERNEL_SOURCE.contains(".target sm_75"),
            "the committed PTX targets the declared architecture"
        );
    }

    /// Requires `libnvrtc`.
    #[test]
    fn the_committed_ptx_reproduces_from_its_source() {
        assert_eq!(
            sima_toolkit_cuda::compile(KERNEL_CU).expect("compile the kernel"),
            GrayScottCuda::KERNEL_SOURCE,
            "the committed PTX is not what the committed source compiles to"
        );
    }

    #[test]
    fn the_environment_pins_the_ptx_digest_and_the_cuda_compiler() -> Result<()> {
        // The environment is derived device-free, hashing the artifact the
        // engine loads. Regenerating the PTX changes every task key.
        let domain = CaDomain::<GrayScottCuda, CudaEngine>::new()?;
        assert_eq!(domain.format().as_str(), GrayScottCuda::FORMAT_ID);
        let components = domain.environment().components();
        let names: Vec<&str> = components.iter().map(|c| c.name()).collect();
        assert_eq!(
            names,
            [
                "ca_evolution.gray_scott_cuda.executor",
                "ca_evolution.gray_scott_cuda.kernel",
                "ca_evolution.gray_scott_cuda.reduce",
                "cuda.compiler",
            ]
        );
        assert_eq!(
            *components[1].value(),
            EnvironmentValue::Digest(hash_bytes(GrayScottCuda::KERNEL_SOURCE.as_bytes()))
        );
        Ok(())
    }

    #[test]
    fn the_two_programs_are_separate_identities() -> Result<()> {
        // The rule is shared, the identity is not: a task key from one program
        // must never resolve to a result the other backend produced, since
        // the two agree only to a tolerance.
        let cuda = CaDomain::<GrayScottCuda, CudaEngine>::new()?;
        let wgsl = CaDomain::<GrayScott, WgslEngine>::new()?;
        assert_ne!(cuda.format(), wgsl.format());
        assert_ne!(cuda.environment().id(), wgsl.environment().id());
        Ok(())
    }

    #[test]
    fn the_rule_itself_is_shared_with_the_wgsl_program() -> Result<()> {
        // Both programs must evolve the same rule, so everything but the
        // artifact comes from one place. A genome sampled for one is the genome
        // sampled for the other, and it packs into the same uniforms.
        let shared = CaParams::new(64, 64, 100, 1.0)?;
        let config =
            GrayScottGenConfig::new([0.03, 0.06], [0.055, 0.07], [0.16, 0.16], [0.08, 0.08])?;
        let genome = GrayScottCuda::sample(&config, 42, 7);
        assert_eq!(
            GrayScottCuda::uniforms(&genome, &shared),
            GrayScott::uniforms(&GrayScott::sample(&config, 42, 7), &shared)
        );
        assert_eq!(GrayScottCuda::CHANNELS, GrayScott::CHANNELS);
        assert_eq!(GrayScottCuda::ALIVE_CHANNEL, GrayScott::ALIVE_CHANNEL);
        assert_eq!(GrayScottCuda::ALIVE_MIN, GrayScott::ALIVE_MIN);
        Ok(())
    }

    #[test]
    fn both_programs_ignite_identically() -> Result<()> {
        // Ignition is the candidate's starting state; if it differed, the two
        // programs would be evolving different things and their cross-program
        // comparison would mean nothing.
        let shared = CaParams::new(32, 32, 10, 1.0)?;
        let ignition = GrayScottIgnition::new(0.5, 0.25, 8, 0.01)?;
        assert_eq!(
            GrayScottCuda::ignite(&shared, &ignition, 9)?.to_bytes(),
            GrayScott::ignite(&shared, &ignition, 9)?.to_bytes()
        );
        Ok(())
    }
}
