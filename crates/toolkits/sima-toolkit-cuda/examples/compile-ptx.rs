//! Compiles a CUDA C kernel to PTX under the toolkit's pinned options and
//! writes the result to stdout. The regeneration step for every committed
//! `.ptx` in the workspace.
//!
//! Needs `libnvrtc`, which opens no device and needs no driver. The build
//! vendors the pinned release beside this binary, which reaches it through its
//! `RUNPATH`:
//!
//! ```text
//! cargo run -p sima-toolkit-cuda --example compile-ptx -- kernel.cu > kernel.ptx
//! ```
//!
//! Each kernel's regeneration test then asserts the committed artifact is
//! exactly what its committed source compiles to.

use std::io::Write;

use sima_core::{Error, Result};

fn main() -> Result<()> {
    let Some(path) = std::env::args().nth(1) else {
        return Err(Error::Validation(
            "compile-ptx takes the path of a CUDA C source file".to_string(),
        ));
    };
    let source = std::fs::read_to_string(&path)
        .map_err(|e| Error::Validation(format!("reading {path:?} failed: {e}")))?;
    let ptx = sima_toolkit_cuda::compile(&source)?;
    std::io::stdout()
        .write_all(ptx.as_bytes())
        .map_err(|e| Error::Validation(format!("writing the PTX failed: {e}")))
}
