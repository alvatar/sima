//! `sima` command-line binary. Prints the version and exits; commands arrive
//! with the pipeline milestones.

fn main() {
    println!("sima {}", env!("CARGO_PKG_VERSION"));
}
