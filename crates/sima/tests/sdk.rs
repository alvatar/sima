//! CLI acceptance of `sima sdk`: the vend verb writes the package this binary
//! carries, so a program can be developed against it outside a run.
//!
//! The verb opens no store and reads no config — it writes what the binary
//! already holds — so what is asserted here is the files and their contents.

mod common;

use std::path::{Path, PathBuf};
use std::process::Output;

use common::sima_command;

/// The `python/sima/` directory the binary embeds its package from: this crate
/// sits two levels below the repository root.
fn package_source() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the repository root above crates/sima")
        .join("python/sima")
}

/// Runs the sima binary with `args`, capturing output.
fn sima(args: &[&str]) -> Output {
    sima_command().args(args).output().expect("spawn sima")
}

#[test]
fn sdk_python_vends_the_package_this_binary_carries() {
    let dir = tempfile::tempdir().expect("temp dir");
    let out = dir.path().join("vendor");
    let output = sima(&["sdk", "python", "--out", out.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");

    // Contents, not just presence: the verb and the embedded package cannot
    // drift apart, and neither can either from the source they are written
    // from.
    let mut vended = 0;
    for entry in std::fs::read_dir(package_source()).expect("read the package directory") {
        let source = entry.expect("a directory entry").path();
        if source.extension().is_none_or(|extension| extension != "py") {
            continue;
        }
        let name = source.file_name().expect("a file name");
        assert_eq!(
            std::fs::read_to_string(out.join("sima").join(name)).expect("the vended file"),
            std::fs::read_to_string(&source).expect("the source file"),
            "{}",
            name.display()
        );
        vended += 1;
    }
    assert!(vended > 0, "the package holds files");

    // Idempotent: the verb is a write, so vending twice into one directory
    // leaves what vending once left.
    assert_eq!(
        sima(&["sdk", "python", "--out", out.to_str().expect("utf-8 path")])
            .status
            .code(),
        Some(0)
    );
    assert!(out.join("sima/__init__.py").is_file());
}

#[test]
fn sdk_refuses_a_language_this_binary_does_not_vend() {
    let dir = tempfile::tempdir().expect("temp dir");
    let out = dir.path().join("vendor");
    let output = sima(&["sdk", "cobol", "--out", out.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let message = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(message.contains("cobol"), "names the language: {message}");
    assert!(message.contains("python"), "names what it vends: {message}");
    assert!(!out.exists(), "and wrote nothing");
}
