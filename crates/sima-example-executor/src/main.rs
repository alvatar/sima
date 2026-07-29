//! Step 5 of the five described in the library. Steps 1 to 4 are in `lib.rs`.

use sima_example_executor::{DoublerDomain, SamplerPlug};

fn main() {
    if let Err(e) = host() {
        eprintln!("sima-example-executor: {e}");
        std::process::exit(1);
    }
}

// 5. Hand it all over.
//
// `serve` reads the process arguments to learn which role sima wants — answer
// what the format binds, or execute the tasks it sends — and then owns the
// process: it reads requests off stdin, calls into the plugs, and writes
// answers to stdout until sima closes the pipe.
//
// This is the whole of hosting. A program that gets here has plugged in.
/// Builds the plugs and hosts them for the life of the process.
fn host() -> sima_api::Result<()> {
    let domain = DoublerDomain::new()?;
    let generator = SamplerPlug::new()?;
    sima_api::serve(&domain, &[&generator])
}
