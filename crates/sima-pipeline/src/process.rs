//! Running the commands a search's setup depends on, and finding the binary its
//! workers are spawned from.
//!
//! Three parts of the pipeline reach outside the process before a search starts —
//! orchestration, rental, and migration — and each needs the same things: search a
//! command and read its exit status, search one and read its stdout, and confirm a
//! machine holds the worker image. Holding them here is what keeps rental and
//! migration from importing orchestration for helpers that have nothing to do
//! with driving a search.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};

use sima_core::{Error, Result, own_process_group};
use sima_transport::container::image_inspect_argv;

use crate::config::Container;

/// The code ssh exits with when it could not make the connection at all, as
/// distinct from a command on the far side exiting non-zero.
const SSH_UNREACHABLE: i32 = 255;

/// What a machine answered at the transport boundary.
#[derive(Debug)]
pub(crate) enum Probe {
    /// The route carried the probe successfully.
    Answered,
    /// SSH could not establish a connection to the machine.
    Unreachable {
        /// The diagnostic from SSH.
        error: Error,
        /// The class of connection refusal.
        refusal: Refusal,
    },
}

/// Why SSH could not establish a connection to a machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The route does not carry SSH yet.
    Route,
    /// SSH answered but rejected the account credentials.
    Auth,
}

/// Probes an SSH route, separating a connection failure from a command
/// failure after the route answered. Exit 255 is ssh's own status and means
/// unreachable only when ssh is in `argv`, so callers pass ssh argv only.
pub(crate) fn ssh_contact(argv: &[String], label: &str) -> Result<Probe> {
    let (status, said) = command_report(argv)?;
    if status.success() {
        return Ok(Probe::Answered);
    }
    if status.code() == Some(SSH_UNREACHABLE) {
        let refusal = match said.contains("Permission denied") {
            true => Refusal::Auth,
            false => Refusal::Route,
        };
        return Ok(Probe::Unreachable {
            error: unreachable(label, status, &said),
            refusal,
        });
    }
    Err(Error::Transport(format!(
        "command on {label} exited with {status}"
    )))
}

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
    bootstrap_image_with_argv(host, container, &argv)
}

/// Runs an image-presence probe using the supplied command arguments.
fn bootstrap_image_with_argv(
    host: Option<&str>,
    container: &Container,
    argv: &[String],
) -> Result<ImageCheck> {
    let (status, said) = command_report(argv)?;
    if status.success() {
        return Ok(ImageCheck::Present);
    }
    // 255 is ssh's own code for a connection it could not make, and it can only
    // mean that when there is an ssh in the command at all: a local runtime
    // exiting 255 is the runtime speaking, about the image.
    if let Some(host) = host
        && status.code() == Some(SSH_UNREACHABLE)
    {
        return Ok(ImageCheck::Unreachable(unreachable(
            &format!("{host:?}"),
            status,
            &said,
        )));
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

/// Reports an unreachable SSH endpoint using its last diagnostic line.
fn unreachable(label: &str, status: ExitStatus, said: &str) -> Error {
    let last = said
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map(str::trim)
        .map_or_else(|| format!("ssh exited with {status}"), str::to_string);
    Error::Transport(format!("cannot reach {label}: {last}"))
}

/// Runs a silent status probe and returns its exit status and trimmed stderr.
///
/// Stderr is captured rather than inherited because the caller polls the
/// probe. This keeps intermediate failures silent while retaining what the far
/// side said for the error returned when the probe stops.
fn command_report(argv: &[String]) -> Result<(ExitStatus, String)> {
    let (program, args) = argv.split_first().expect("a non-empty command vector");
    let output = own_process_group(&mut Command::new(program))
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| Error::Transport(format!("running {program:?} failed: {e}")))?;
    Ok((
        output.status,
        String::from_utf8_lossy(&output.stderr).trim().to_string(),
    ))
}

/// Runs `argv` and returns its stdout, or an error if it fails or its output
/// is not UTF-8.
///
/// Stderr is captured and folded into the error rather than inherited. Every
/// caller here is a device-enumeration probe, and a probe is polled until the
/// machine answers: an inherited stderr writes ssh's own `Connection refused`
/// to the operator's terminal once per attempt, which reads as a fault while
/// the wait is doing exactly what it is meant to. Captured, it says nothing
/// until something actually fails — and then it says what the far side said,
/// which the exit status alone does not.
pub(crate) fn command_stdout(argv: &[String]) -> Result<String> {
    let (program, args) = argv.split_first().expect("a non-empty command vector");
    let output = own_process_group(&mut Command::new(program))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| Error::Transport(format!("running {program:?} failed: {e}")))?;
    if !output.status.success() {
        let said = String::from_utf8_lossy(&output.stderr);
        let said = said.trim();
        let status = output.status;
        return Err(Error::Transport(match said.is_empty() {
            true => format!("{program:?} exited with {status}"),
            false => format!("{program:?} exited with {status}: {said}"),
        }));
    }
    String::from_utf8(output.stdout)
        .map_err(|e| Error::Transport(format!("{program:?} output is not UTF-8: {e}")))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn container(runtime: &str) -> Container {
        Container {
            image: "worker-image".to_string(),
            runtime: runtime.to_string(),
            run_args: Vec::new(),
        }
    }

    #[test]
    fn ssh_contact_distinguishes_an_unreachable_route() {
        let probe = ssh_contact(
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "exit 255".to_string(),
            ],
            "test host",
        )
        .expect("the unreachable route is a probe outcome");
        let Probe::Unreachable { error, refusal } = probe else {
            panic!("exit 255 must be unreachable");
        };
        assert_eq!(refusal, Refusal::Route);
        assert_eq!(
            error.to_string(),
            "worker transport error: cannot reach test host: ssh exited with exit status: 255"
        );
    }

    #[test]
    fn ssh_contact_reports_the_last_diagnostic_from_an_unreachable_route() {
        let probe = ssh_contact(
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "echo 'banner line' >&2; echo 'Permission denied (publickey).' >&2; exit 255"
                    .to_string(),
            ],
            "test host",
        )
        .expect("the unreachable route is a probe outcome");
        let Probe::Unreachable { error, refusal } = probe else {
            panic!("exit 255 must be unreachable");
        };
        assert_eq!(refusal, Refusal::Auth);
        assert_eq!(
            error.to_string(),
            "worker transport error: cannot reach test host: Permission denied (publickey)."
        );
    }

    #[test]
    fn ssh_contact_ignores_blank_lines_after_the_last_diagnostic() {
        let probe = ssh_contact(
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "printf 'Connection refused\\n\\n' >&2; exit 255".to_string(),
            ],
            "test host",
        )
        .expect("the unreachable route is a probe outcome");
        let Probe::Unreachable { error, refusal } = probe else {
            panic!("exit 255 must be unreachable");
        };
        assert_eq!(refusal, Refusal::Route);
        assert_eq!(
            error.to_string(),
            "worker transport error: cannot reach test host: Connection refused"
        );
    }

    #[test]
    fn an_unreachable_route_uses_the_last_non_empty_line() {
        let status = Command::new("/bin/sh")
            .args(["-c", "exit 255"])
            .status()
            .expect("run the status fixture");
        assert_eq!(
            unreachable("h", status, "a\nb\n").to_string(),
            "worker transport error: cannot reach h: b"
        );
    }

    #[test]
    fn ssh_contact_accepts_an_answering_route() {
        assert!(matches!(
            ssh_contact(&["/bin/true".to_string()], "test host"),
            Ok(Probe::Answered)
        ));
    }

    #[test]
    fn ssh_contact_preserves_a_remote_command_failure() {
        let error = ssh_contact(
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "exit 3".to_string(),
            ],
            "test host",
        )
        .expect_err("a remote failure is not an unreachable route");
        assert_eq!(
            error.to_string(),
            "worker transport error: command on test host exited with exit status: 3"
        );
    }

    #[test]
    fn a_local_runtime_exiting_255_still_reports_the_missing_image() {
        let error = match bootstrap_image_with_argv(
            None,
            &container("podman"),
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "exit 255".to_string(),
            ],
        ) {
            Err(error) => error,
            Ok(_) => panic!("a local runtime failure must mean the image is absent"),
        };
        assert_eq!(
            error.to_string(),
            "validation error: worker image \"worker-image\" is not present locally; put it there with: podman build -t worker-image -f containers/sima/Containerfile ."
        );
    }

    #[test]
    fn a_remote_image_probe_reports_ssh_stderr() {
        let check = bootstrap_image_with_argv(
            Some("host"),
            &container("runtime"),
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "echo 'Connection refused' >&2; exit 255".to_string(),
            ],
        )
        .expect("an unreachable host is a probe outcome");
        let ImageCheck::Unreachable(error) = check else {
            panic!("exit 255 with a host must be unreachable");
        };
        assert_eq!(
            error.to_string(),
            "worker transport error: cannot reach \"host\": Connection refused"
        );
    }

    #[test]
    fn a_command_that_fails_carries_what_it_said_rather_than_printing_it() {
        // The probe loop's ssh writes its refusal to stderr on every attempt.
        // Captured, it reaches the one error that reports the wait ran out;
        // inherited, it would reach the terminal once per attempt while the
        // wait was still doing its job.
        let error = command_stdout(&[
            "/bin/sh".to_string(),
            "-c".to_string(),
            "echo 'connection refused' >&2; exit 255".to_string(),
        ])
        .expect_err("a command that exits non-zero fails");
        let text = error.to_string();
        assert!(text.contains("connection refused"), "{text}");
        assert!(text.contains("255"), "names the status: {text}");
    }

    #[test]
    fn a_command_that_succeeds_answers_with_its_stdout_alone() {
        let stdout = command_stdout(&[
            "/bin/sh".to_string(),
            "-c".to_string(),
            "echo answer; echo noise >&2".to_string(),
        ])
        .expect("the command succeeded");
        assert_eq!(stdout, "answer\n");
    }
}
