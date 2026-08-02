//! Shared fixtures for the real-domain end-to-end suites.

use std::path::{Path, PathBuf};
use std::sync::Once;

use sima_core::Result;
use sima_pipeline::{Event, LoadedConfig, Record, load};
use sima_store::Store;

/// Writes `text` as a config file named `name` under `dir` and loads it.
/// Also ensures the worker binary exists: these tests drive `orchestrate`,
/// whose worker discovery finds `sima-worker` in the parent directory of
/// this test executable's own directory once it is built.
pub fn loaded_text(dir: &Path, name: &str, text: &str) -> Result<LoadedConfig> {
    build_worker_binary();
    let path: PathBuf = dir.join(name);
    std::fs::write(&path, text).expect("write config");
    load(&path)
}

/// The journal events of the run `config` describes, in append order. Each
/// top-level file under `tests/` compiles as its own crate, so a helper only
/// some suites use reads as dead code in the others.
#[allow(dead_code)]
pub fn journal_events(config: &LoadedConfig) -> Vec<Event> {
    let store = Store::open(&config.store).expect("open store");
    store
        .journal(&config.run.id())
        .expect("read journal")
        .iter()
        .map(|line| Record::from_line(line).expect("parse journal line").event)
        .collect()
}

/// The text of the shipped example `name`, from `examples/` at the workspace
/// root — two levels above this integration crate.
#[allow(dead_code)]
pub fn shipped_example(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Loads `text` as the example `name` would be loaded, from a directory of
/// its own. Loading resolves the store path against the file's directory and
/// creates nothing, so a variant costs a parse and no store.
#[allow(dead_code)]
pub fn load_example_variant(name: &str, text: &str) -> Result<LoadedConfig> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join(name);
    std::fs::write(&path, text).expect("write the variant");
    load(&path)
}

/// Uncomments the commented key lines `lines` names, each by a literal that
/// starts the line after its `# ` marker.
///
/// A shipped example ships its optional knobs commented, so a variant test
/// enables the ones it exercises and loads the result. Each literal must match
/// exactly one commented line, and a miss panics naming it: an example that
/// drifts away from what a variant enables fails here rather than silently
/// testing a config the file no longer holds.
#[allow(dead_code)]
pub fn uncomment(text: &str, lines: &[&str]) -> String {
    let mut out = text.to_string();
    for literal in lines {
        out = rewrite_lines(
            &out,
            &format!("the commented line `{literal}`"),
            |line| {
                line.strip_prefix("# ")
                    .is_some_and(|k| k.starts_with(literal))
            },
            |line| line.replacen("# ", "", 1),
        );
    }
    out
}

/// Uncomments the commented block `header` heads: the header line itself and
/// the commented lines under it, up to the first blank line. `header` is
/// written as it appears, brackets included — `[host.gpubox]`,
/// `[[orchestrator.device]]`.
///
/// A block's key lines repeat across blocks — several machines declare
/// `workers`, several name an image — so a block is enabled whole, by the
/// table it heads. The blank line is the boundary the examples are written to:
/// a block's explanatory prose sits above its header, and a nested table that
/// is exclusive with the block's own keys sits below a blank line of its own.
#[allow(dead_code)]
pub fn uncomment_block(text: &str, header: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut inside = false;
    let mut found = 0;
    for line in text.lines() {
        let commented = line.strip_prefix("# ");
        if commented.is_some_and(|rest| rest.starts_with(header)) {
            inside = true;
            found += 1;
        } else if line.trim().is_empty() {
            inside = false;
        }
        match (inside, commented) {
            (true, Some(rest)) => out.push_str(rest),
            _ => out.push_str(line),
        }
        out.push('\n');
    }
    assert_eq!(found, 1, "the example heads `{header}` exactly once");
    out
}

/// Comments out the active lines `lines` names, each by its exact content —
/// the inverse of [`uncomment`], for a variant that replaces a knob with the
/// one it is exclusive with.
#[allow(dead_code)]
pub fn comment_out(text: &str, lines: &[&str]) -> String {
    let mut out = text.to_string();
    for literal in lines {
        out = rewrite_lines(
            &out,
            &format!("the active line `{literal}`"),
            |line| line.trim() == *literal,
            |line| format!("# {line}"),
        );
    }
    out
}

/// Applies `rewrite` to the one line of `text` that `matches` selects,
/// panicking with `what` unless exactly one line does.
fn rewrite_lines(
    text: &str,
    what: &str,
    matches: impl Fn(&str) -> bool,
    rewrite: impl Fn(&str) -> String,
) -> String {
    let mut out = String::with_capacity(text.len());
    let mut hits = 0;
    for line in text.lines() {
        if matches(line) {
            hits += 1;
            out.push_str(&rewrite(line));
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    assert_eq!(hits, 1, "the example holds {what} exactly once");
    out
}

/// Builds the `sima-worker` binary once per test process. Cargo builds
/// another crate's binary only when it is in the build graph, so the suites
/// that spawn workers build it explicitly.
fn build_worker_binary() {
    static BUILD: Once = Once::new();
    BUILD.call_once(|| build_binary("sima-worker"));
}

/// Builds `package`'s binary and returns its path, for the suites that drive a
/// run through a program of its own.
#[allow(dead_code)]
pub fn built_binary(package: &str) -> PathBuf {
    build_binary(package);
    // Beside the test executable's directory: `target/<profile>/deps` holds the
    // test binary and `target/<profile>` the built program.
    let exe = std::env::current_exe().expect("the test executable's path");
    let binary = exe
        .parent()
        .and_then(Path::parent)
        .expect("target/<profile> above the test executable")
        .join(package);
    assert!(binary.is_file(), "{} is built", binary.display());
    binary
}

/// Asks cargo for `package`'s binary.
fn build_binary(package: &str) {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let status = std::process::Command::new(cargo)
        .args(["build", "-p", package])
        .status()
        .unwrap_or_else(|e| panic!("run cargo build for {package}: {e}"));
    assert!(status.success(), "building {package} failed");
}
