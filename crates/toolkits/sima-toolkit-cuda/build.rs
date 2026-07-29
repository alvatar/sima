//! Vendors the pinned NVRTC beside the build's binaries.
//!
//! The committed PTX is regenerated and compared byte for byte by the
//! known-answer tests, so the comparison holds only when the NVRTC that
//! compiles it is the pinned one. `cudarc` opens NVRTC at run time by trying
//! sonames, the first of which is the bare `libnvrtc.so`, and a bare name
//! resolves through `LD_LIBRARY_PATH`, then the binary's `RUNPATH`, then
//! `ld.so.cache`. A CUDA toolkit installed on the machine registers itself in
//! that cache, so the pinned library reaches the loader through the `RUNPATH`
//! that `.cargo/config.toml` points at this directory.
//!
//! The archive is fetched once per profile and verified against the digest
//! below. `SIMA_NVRTC_DIR` names an existing copy instead, for a machine
//! building offline.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};

/// The pinned NVRTC release. It emits PTX ISA 8.0, which keeps the committed
/// artifacts loadable on r525 and newer drivers.
const VERSION: &str = "12.0.76";

/// NVIDIA's redistributable archive for the pinned release.
const URL: &str = "https://developer.download.nvidia.com/compute/cuda/redist/cuda_nvrtc/linux-x86_64/cuda_nvrtc-linux-x86_64-12.0.76-archive.tar.xz";

/// The archive's SHA-256, checked before anything is extracted.
const SHA256: &str = "0a4ebc9a1516a5e00f14c69365ba782dcfab545d2abb15740569970f89855bff";

/// The directory name the `RUNPATH` in `.cargo/config.toml` names, relative to
/// where the build places its binaries.
const VENDOR_DIR: &str = "nvrtc";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=SIMA_NVRTC_DIR");

    // The vendored copy is a build product, so it lives beside the binaries
    // rather than in the source tree.
    let Some(target) = profile_dir() else {
        println!(
            "cargo:warning=OUT_DIR has an unexpected shape, so the pinned NVRTC was not vendored"
        );
        return;
    };
    let vendor = target.join(VENDOR_DIR);

    if let Some(supplied) = env::var_os("SIMA_NVRTC_DIR") {
        link(&vendor, Path::new(&supplied));
        return;
    }
    // The marker records which release the directory holds, so a change to the
    // pin replaces it rather than loading whatever landed there first.
    let marker = vendor.join(".version");
    if fs::read_to_string(&marker).is_ok_and(|held| held.trim() == VERSION) {
        return;
    }
    match vendor_release(&vendor) {
        Ok(()) => {
            let _ = fs::write(&marker, VERSION);
        }
        Err(reason) => {
            // A machine without the archive still builds: the workspace opens
            // CUDA at run time, so only the tests that compile kernels need it.
            println!(
                "cargo:warning=the pinned NVRTC {VERSION} is absent ({reason}). \
                 Kernel compilation will use whichever NVRTC the machine offers, \
                 which changes the emitted PTX. Set SIMA_NVRTC_DIR to a directory \
                 holding libnvrtc.so for an offline build."
            );
        }
    }
}

/// The directory the build places its binaries in, derived from `OUT_DIR`
/// (`<profile>/build/<package>-<hash>/out`).
fn profile_dir() -> Option<PathBuf> {
    let out = PathBuf::from(env::var_os("OUT_DIR")?);
    Some(out.parent()?.parent()?.parent()?.to_path_buf())
}

/// Points `vendor` at the libraries in `supplied`, so a directory the caller
/// already holds is reachable through the same `RUNPATH`.
fn link(vendor: &Path, supplied: &Path) {
    let _ = fs::remove_dir_all(vendor);
    if let Err(e) = fs::create_dir_all(vendor) {
        println!("cargo:warning=create {}: {e}", vendor.display());
        return;
    }
    copy_libraries(supplied, vendor);
}

/// Fetches, verifies, and extracts the pinned release into `vendor`.
fn vendor_release(vendor: &Path) -> Result<(), String> {
    let staging = vendor.with_extension("staging");
    let _ = fs::remove_dir_all(&staging);
    fs::create_dir_all(&staging).map_err(|e| e.to_string())?;
    let archive = staging.join("nvrtc.tar.xz");

    run(Command::new("curl").args([
        "--silent",
        "--show-error",
        "--location",
        "--fail",
        "--max-time",
        "600",
        "--output",
        &archive.to_string_lossy(),
        URL,
    ]))?;
    verify(&archive)?;
    run(Command::new("tar").args([
        "--extract",
        "--xz",
        "--strip-components=2",
        "--file",
        &archive.to_string_lossy(),
        "--directory",
        &staging.to_string_lossy(),
        // The stubs directory holds link-time placeholders that resolve no
        // symbol at run time, so only the real libraries are taken.
        "--exclude=*/stubs/*",
        "--wildcards",
        "*/lib/*",
    ]))?;

    let _ = fs::remove_dir_all(vendor);
    fs::create_dir_all(vendor).map_err(|e| e.to_string())?;
    copy_libraries(&staging, vendor);
    let _ = fs::remove_dir_all(&staging);
    Ok(())
}

/// Copies every shared object from `from` into `to`, following symlinks so the
/// bare `libnvrtc.so` name `cudarc` asks for is a real file.
fn copy_libraries(from: &Path, to: &Path) {
    let Ok(entries) = fs::read_dir(from) else {
        println!("cargo:warning=read {}", from.display());
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        if !name.to_string_lossy().contains(".so") {
            continue;
        }
        if path.is_file() {
            let _ = fs::copy(&path, to.join(&name));
        }
    }
}

/// Confirms the archive is the pinned one before it is extracted.
fn verify(archive: &Path) -> Result<(), String> {
    let output = Command::new("sha256sum")
        .arg(archive)
        .output()
        .map_err(|e| format!("run sha256sum: {e}"))?;
    let text = String::from_utf8_lossy(&output.stdout);
    let digest = text.split_whitespace().next().unwrap_or_default();
    if digest == SHA256 {
        return Ok(());
    }
    Err(format!(
        "the archive hashes to {digest}, and the pinned release hashes to {SHA256}"
    ))
}

/// Runs `command`, turning a nonzero exit into its stderr.
fn run(command: &mut Command) -> Result<(), String> {
    let output = command
        .output()
        .map_err(|e| format!("run {:?}: {e}", command.get_program()))?;
    if output.status.success() {
        return Ok(());
    }
    Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
}
