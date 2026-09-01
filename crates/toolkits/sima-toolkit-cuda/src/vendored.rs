//! Finding the pinned NVRTC that the build vendored beside the binaries.
//!
//! The build script places the pinned release in a directory next to the
//! binaries, and `.cargo/config.toml` puts that directory on their `RUNPATH`,
//! which is how `libnvrtc.so` itself is reached ahead of any CUDA toolkit
//! installed on the machine.
//!
//! One library needs more than the `RUNPATH`. NVRTC opens its own helper,
//! `libnvrtc-builtins.so.12.0`, by bare name at compile time, and the loader
//! answers a nested open from the `RUNPATH` of the library that asked —
//! `libnvrtc.so`, which NVIDIA ships without one. The helper is therefore
//! opened here by absolute path first. Its file name is also its soname, so the
//! open NVRTC makes afterwards resolves to this already-loaded copy rather than
//! searching the loader cache and finding another release.

use std::path::PathBuf;
use std::sync::OnceLock;

use libloading::os::unix::{Library, RTLD_GLOBAL, RTLD_NOW};

/// The helper NVRTC opens while it compiles. The name is the soname the
/// already-loaded copy answers to.
const BUILTINS: &str = "libnvrtc-builtins.so.12.0";

/// The directory names the build script vendors into, relative to the running
/// binary. A command sits in the build's binary directory and a test one level
/// below it, mirroring the two `RUNPATH` entries.
const RELATIVE_DIRS: [&str; 2] = ["nvrtc", "../nvrtc"];

/// Holds the helper open for the life of the process.
static BUILTINS_LIBRARY: OnceLock<Option<Library>> = OnceLock::new();

/// Opens the vendored NVRTC helper, once, so NVRTC's own open of it resolves to
/// the pinned copy.
///
/// A machine building against a supplied NVRTC has the helper on the loader
/// path already, so an absent vendored copy leaves the open to NVRTC.
pub(crate) fn preload_builtins() {
    BUILTINS_LIBRARY.get_or_init(|| {
        let path = directory()?.join(BUILTINS);
        // SAFETY: opening a shared library searches its initializers. This one is
        // NVIDIA's compiler helper, which the process is about to load anyway
        // through NVRTC.
        unsafe { Library::open(Some(&path), RTLD_NOW | RTLD_GLOBAL) }.ok()
    });
}

/// The directory holding the vendored release, if the build produced one.
fn directory() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let base = exe.parent()?;
    RELATIVE_DIRS
        .iter()
        .map(|relative| base.join(relative))
        .find(|candidate| candidate.join(BUILTINS).is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_vendored_helper_sits_beside_the_running_binary() {
        // The build vendors it, so the lookup that NVRTC's own open depends on
        // resolves. A failure here means kernel compilation would fall back to
        // whichever release the machine has installed.
        let directory = directory().expect("the build vendored the pinned NVRTC");
        assert!(directory.join(BUILTINS).is_file());
    }
}
