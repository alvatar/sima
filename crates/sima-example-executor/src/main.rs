//! A program outside the workspace, in the shape a third party writes one:
//! implement the two plugs, hand them to [`sima_api::serve`].
//!
//! The role is read from the arguments, so this one binary is both what a run
//! asks about the format and what its workers execute in.

use sima_example_executor::{DoublerDomain, SamplerPlug};

fn main() {
    if let Err(e) = host() {
        eprintln!("sima-example-executor: {e}");
        std::process::exit(1);
    }
}

/// Builds the plugs and hosts them for the life of the process.
fn host() -> sima_api::Result<()> {
    let domain = DoublerDomain::new()?;
    let generator = SamplerPlug::new()?;
    sima_api::serve(&domain, &[&generator])
}
