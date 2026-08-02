//! Running the commands a run's setup depends on, and finding the binary its
//! workers are spawned from.
//!
//! Three parts of the pipeline reach outside the process before a run starts —
//! orchestration, rental, and migration — and each needs the same things: run a
//! command and read its exit status, run one and read its stdout, and confirm a
//! machine holds the worker image. Holding them here is what keeps rental and
//! migration from importing orchestration for helpers that have nothing to do
//! with driving a run.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};

use sima_core::{Error, Result};
use sima_transport::container::image_inspect_argv;

use crate::config::Container;

/// The code ssh exits with when it could not make the connection at all, as
/// distinct from a command on the far side exiting non-zero.
const SSH_UNREACHABLE: i32 = 255;

/// What asking a machine for a pool's worker image found.
pub(crate) enum ImageCheck {
    /// The runtime answered and holds the image.
    Present,
    /// The machine could not be reached to ask, which is what a fresh or
    /// rebooting one answers until it is up. The error names the destination
    /// and is what to report if it never comes up.
    Unreachable(Error),
}

/// Verifies a pool's worker image is present, failing with the command that
/// puts it there. A missing image must be a clean error, not a hanging
/// handshake. The fix differs by where the container runs: build it locally, or
/// ship the local build to the machine.
///
/// A machine that could not be reached at all is [`ImageCheck::Unreachable`]
/// rather than an error, because the two failures are worth different
/// responses: an image the runtime does not hold will not appear by being asked
/// again, while a machine that is still coming up will answer shortly. The
/// caller decides — a pool fails on either, a migration's first contact waits
/// out the second.
pub(crate) fn bootstrap_image(host: Option<&str>, container: &Container) -> Result<ImageCheck> {
    let argv = image_inspect_argv(host, &container.runtime, &container.image);
    let status = command_status(&argv)?;
    if status.success() {
        return Ok(ImageCheck::Present);
    }
    // 255 is ssh's own code for a connection it could not make, and it can only
    // mean that when there is an ssh in the command at all: a local runtime
    // exiting 255 is the runtime speaking, about the image.
    if let Some(host) = host
        && status.code() == Some(SSH_UNREACHABLE)
    {
        return Ok(ImageCheck::Unreachable(Error::Validation(format!(
            "cannot reach {host:?}: ssh exited with {status}"
        ))));
    }
    let (place, fix) = match host {
        Some(host) => (
            format!("on {host:?}"),
            format!(
                "podman save {} | ssh {host} {} load",
                container.image, container.runtime
            ),
        ),
        None => (
            "locally".to_string(),
            format!(
                "podman build -t {} -f containers/sima/Containerfile .",
                container.image
            ),
        ),
    };
    Err(Error::Validation(format!(
        "worker image {:?} is not present {place}; put it there with: {fix}",
        container.image
    )))
}

/// Runs `argv`, discarding its streams, and reports whether it exited zero.
fn command_status(argv: &[String]) -> Result<ExitStatus> {
    let (program, args) = argv.split_first().expect("a non-empty command vector");
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| Error::Validation(format!("running {program:?} failed: {e}")))
}

/// Runs `argv` and returns its stdout, or an error if it fails or its output
/// is not UTF-8. Stderr is inherited for diagnostics.
pub(crate) fn command_stdout(argv: &[String]) -> Result<String> {
    let (program, args) = argv.split_first().expect("a non-empty command vector");
    let output = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stderr(Stdio::inherit())
        .output()
        .map_err(|e| Error::Validation(format!("running {program:?} failed: {e}")))?;
    if !output.status.success() {
        return Err(Error::Validation(format!(
            "{program:?} exited with {}",
            output.status
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|e| Error::Validation(format!("{program:?} output is not UTF-8: {e}")))
}

/// Locates the `sima-worker` binary, in order:
///
/// - the `SIMA_WORKER` environment variable (an absolute path), for tests
///   and later remote layouts;
/// - `sima-worker` beside the current executable;
/// - `sima-worker` in the parent directory of the current executable's
///   directory, which covers test executables under `target/debug/deps`
///   finding the binary in `target/debug`.
///
/// A missing binary is a validation error naming the searched locations.
///
/// This is one of the two environment channels the pipeline reads. The other is
/// `SIMA_STUB_SSH`, which points the stub backend at a machine that is really
/// there so the ssh path runs against a throwaway server; it is read in
/// `providers` and nowhere else.
pub(crate) fn worker_binary() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("SIMA_WORKER") {
        return Ok(PathBuf::from(path));
    }
    let exe = std::env::current_exe().map_err(|e| {
        Error::Validation(format!(
            "cannot locate sima-worker: the current executable's path is unknown: {e}"
        ))
    })?;
    let mut searched = Vec::new();
    for dir in [exe.parent(), exe.parent().and_then(Path::parent)] {
        let Some(dir) = dir else { continue };
        let candidate = dir.join("sima-worker");
        if candidate.is_file() {
            return Ok(candidate);
        }
        searched.push(candidate);
    }
    Err(Error::Validation(format!(
        "sima-worker binary not found; set SIMA_WORKER or place it at one of: {}",
        searched
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}
