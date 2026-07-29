//! Step 6 of the six described in the library: hand both components over.

use sima_example_executor::{DoublerDomain, Sampler};

fn main() {
    if let Err(e) = host() {
        eprintln!("sima-example-executor: {e}");
        std::process::exit(1);
    }
}

// 6. Hand component A and component B to sima.
//
// `serve` reads the process arguments to learn which role sima wants — answer
// what the format binds, or execute the tasks it sends — and then owns the
// process: it reads requests off stdin, calls into the two components, and
// writes answers to stdout until sima closes the pipe.
//
// This is the whole of hosting. A program that gets here has plugged in.
/// Builds both components and hosts them for the life of the process.
fn host() -> sima_api::Result<()> {
    let domain = DoublerDomain::new()?;
    let generator = Sampler::new()?;
    sima_api::serve(&domain, &[&generator])
}
