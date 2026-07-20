//! Follow and remote observation, in two tiers by carrier.
//!
//! **Tier A — the whole mechanism, no ssh.** The near side spawns an `ssh`
//! found on `PATH`; these tests put one there that drops the ssh arguments and
//! runs the built binary on this machine. Every part of remote observation is
//! exercised: the invocation, the far side loading the config and serving its
//! journal, the frame stream, the near side folding records and rendering
//! them. What is absent is the network hop, which the stream cannot
//! distinguish from any other pipe carrier. These run everywhere.
//!
//! **Tier B — a real ssh hop.** The same views across a real connection.
//! `#[ignore]` and gated on `SIMA_TEST_FOLLOW_HOST`, so a blanket `--ignored`
//! run passes clean where no destination is configured.
//!
//! ```text
//! SIMA_TEST_FOLLOW_HOST=localhost cargo test -p sima --test follow -- --ignored
//! ```
//!
//! The destination must satisfy two conditions, which `localhost` satisfies
//! by construction:
//!
//! - It reaches this machine's filesystem at the same paths, since the config
//!   path travels unresolved and the far side reads the store beside it.
//! - Its `PATH` holds a `sima` built from this source. A build mismatch is
//!   what the stream's version handshake refuses, and the refusal names both
//!   versions.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::time::Duration;

use common::{manifest_of, sima_command};

/// The ssh destination Tier B runs against, or `None` to skip it.
fn follow_host() -> Option<String> {
    std::env::var("SIMA_TEST_FOLLOW_HOST")
        .ok()
        .filter(|host| !host.is_empty())
}

/// Writes a `sima.toml` under `dir` whose store lives beside it.
fn write_config(dir: &Path, behaviors: &str) -> PathBuf {
    common::write_config(dir, "sima.toml", behaviors, "./store")
}

/// Runs the sima binary with `args` over a real `ssh`, capturing output.
fn sima(args: &[&str]) -> Output {
    sima_command().args(args).output().expect("spawn sima")
}

/// The stdout of `output`, as UTF-8.
fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is UTF-8")
}

/// Writes an `ssh` into `dir` that runs the far side on this machine: it
/// drops everything up to the `--` that ends ssh's own options, drops the
/// `sima` program name the near side asks for, and runs the built binary with
/// the rest. The result is the follow transport over a plain pipe.
fn ssh_shim(dir: &Path) -> PathBuf {
    let path = dir.join("ssh");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\n\
             while [ \"$1\" != \"--\" ]; do shift; done\n\
             shift\n\
             shift\n\
             exec {} \"$@\"\n",
            env!("CARGO_BIN_EXE_sima")
        ),
    )
    .expect("write the ssh shim");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("make the ssh shim executable");
    }
    dir.to_path_buf()
}

/// Runs the sima binary with `args` against the shimmed `ssh` in `bin`, so a
/// `--on` command reaches the far side over a pipe instead of a connection.
fn sima_shimmed(bin: &Path, args: &[&str]) -> Output {
    spawn_shimmed(bin, args, Stdio::piped())
        .wait_with_output()
        .expect("collect sima")
}

/// Spawns the sima binary with `args` against the shimmed `ssh` in `bin`,
/// leaving the caller to decide when to collect it — for the tests that assert
/// a command ends on its own rather than what it printed.
fn spawn_shimmed(bin: &Path, args: &[&str], stdout: Stdio) -> std::process::Child {
    let path = std::env::var("PATH").unwrap_or_default();
    sima_command()
        .env("PATH", format!("{}:{path}", bin.display()))
        .args(args)
        .stdout(stdout)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sima")
}

/// The read-only views a remote target must render identically to a local
/// one, as argument lists over a config path.
fn views(path: &str) -> Vec<Vec<&str>> {
    vec![
        vec!["status", path],
        vec!["status", path, "--failed"],
        vec!["report", path],
        vec!["report", path, "--all"],
    ]
}

#[test]
fn a_remote_view_renders_exactly_what_the_local_one_renders() {
    let dir = tempfile::tempdir().expect("temp dir");
    let bin = ssh_shim(dir.path());
    let config = write_config(dir.path(), r#""succeed", "flaky:1", "succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // The remote path streams records and folds them here; the local path
    // reads the same journal directly. One renderer, so one rendering.
    for view in views(path) {
        let local = sima(&view);
        let remote = sima_shimmed(&bin, &[view.clone(), vec!["--on", "gpubox"]].concat());
        assert_eq!(local.status.code(), Some(0), "{view:?}: {local:?}");
        assert_eq!(remote.status.code(), Some(0), "{view:?}: {remote:?}");
        assert_eq!(stdout(&remote), stdout(&local), "{view:?}");
    }
}

#[test]
fn a_remote_task_view_renders_exactly_what_the_local_one_renders() {
    let dir = tempfile::tempdir().expect("temp dir");
    let bin = ssh_shim(dir.path());
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // A task-addressed view resolves the prefix on this side, over records
    // that arrived from the other: the resolution is part of the fold.
    let task = common::journal_events(&config)
        .iter()
        .find_map(|event| match event {
            sima_pipeline::Event::Committed { task, .. } => Some(task.clone()),
            _ => None,
        })
        .expect("a committed task");
    let short = &task[..8];
    for view in [
        vec!["status", path, "--task", short],
        vec!["report", path, "--task", short],
    ] {
        let local = sima(&view);
        let remote = sima_shimmed(&bin, &[view.clone(), vec!["--on", "gpubox"]].concat());
        assert_eq!(remote.status.code(), Some(0), "{view:?}: {remote:?}");
        assert_eq!(stdout(&remote), stdout(&local), "{view:?}");
    }
}

#[test]
fn a_remote_follow_streams_a_live_run_to_its_end() {
    let dir = tempfile::tempdir().expect("temp dir");
    let bin = ssh_shim(dir.path());
    let config = write_config(dir.path(), r#""sleep:800", "sleep:800", "sleep:800""#);
    let path = config.to_str().expect("utf-8 path");
    let mut run = common::driving(&config);

    let output = sima_shimmed(&bin, &["follow", path, "--on", "gpubox"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(stdout(&output).contains("finalized"), "{output:?}");
    assert_eq!(run.wait().expect("wait for sima run").code(), Some(0));
}

#[test]
fn a_remote_follow_carries_the_run_s_outcome_code() {
    let dir = tempfile::tempdir().expect("temp dir");
    let bin = ssh_shim(dir.path());
    let config = write_config(dir.path(), r#""succeed", "reject""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(2));

    let output = sima_shimmed(&bin, &["follow", path, "--on", "gpubox"]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(stdout(&output).contains("rejected"), "{output:?}");
}

#[test]
fn a_far_side_fault_reaches_the_near_side_as_its_own_error() {
    let dir = tempfile::tempdir().expect("temp dir");
    let bin = ssh_shim(dir.path());
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");

    // Nothing was ever driven there: the far side serves the fault, and this
    // side reports the error the local query reports.
    let remote = sima_shimmed(&bin, &["status", path, "--on", "gpubox"]);
    let local = sima(&["status", path]);
    assert_eq!(remote.status.code(), Some(1), "{remote:?}");
    assert_eq!(
        String::from_utf8(remote.stderr).expect("stderr is UTF-8"),
        String::from_utf8(local.stderr).expect("stderr is UTF-8"),
    );
}

#[test]
fn a_remote_follow_over_an_abandoned_run_exits_0() {
    let dir = tempfile::tempdir().expect("temp dir");
    let bin = ssh_shim(dir.path());
    let config = write_config(dir.path(), r#""sleep:4000", "sleep:4000""#);
    let path = config.to_str().expect("utf-8 path");
    common::abandon_run(&config);

    // The far side reports a free lock over a journal that stopped mid-run,
    // and the near side ends on it the way the local follow does: a resumable
    // run, rendered and left successfully.
    let output = sima_shimmed(&bin, &["follow", path, "--on", "gpubox"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(text.contains("started: 2 tasks"), "{text}");
    assert!(!text.contains("finalized"), "{text}");
}

/// Writes an `ssh` into its own directory under `dir` that greets the near
/// side at `protocol` and then stays alive without serving anything. It is the
/// far side a refusal has to cope with: still running, still holding the pipe
/// open, at the moment the near side decides it cannot be spoken to.
fn stalling_ssh_shim(dir: &Path, protocol: u32) -> PathBuf {
    let bin = dir.join("stalling");
    std::fs::create_dir_all(&bin).expect("the shim directory");
    let greeting = bin.join("hello.frame");
    let mut bytes = Vec::new();
    sima_core::write_frame(
        &mut bytes,
        &sima_pipeline::FollowFrame::Hello {
            protocol,
            run: sima_model::RunId::from_hash(sima_core::hash_bytes(b"a stalling far side")),
            format: sima_model::FormatId::new("stub.v1").expect("format id"),
            workers: 1,
            holder: None,
        }
        .encode(),
    )
    .expect("frame the greeting");
    std::fs::write(&greeting, bytes).expect("write the greeting");
    let path = bin.join("ssh");
    std::fs::write(
        &path,
        format!("#!/bin/sh\ncat {}\nsleep 600\n", greeting.display()),
    )
    .expect("write the ssh shim");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("make the ssh shim executable");
    }
    bin
}

#[test]
fn a_far_side_at_another_protocol_version_is_refused_while_it_still_runs() {
    let dir = tempfile::tempdir().expect("temp dir");
    let bin = stalling_ssh_shim(dir.path(), sima_pipeline::FOLLOW_PROTOCOL_VERSION + 1);
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");

    // Reporting the refusal collects what the far side said, which means
    // reaping it — so the refusal has to end that process rather than wait on
    // one that will outlive the follow.
    let output = common::wait_within(
        spawn_shimmed(&bin, &["follow", path, "--on", "gpubox"], Stdio::null()),
        Duration::from_secs(30),
    );
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains(&sima_pipeline::FOLLOW_PROTOCOL_VERSION.to_string())
            && stderr.contains(&(sima_pipeline::FOLLOW_PROTOCOL_VERSION + 1).to_string()),
        "{stderr}"
    );
}

#[test]
fn a_followed_run_finalizes_to_the_manifest_an_unobserved_run_produces() {
    // Observation is read-only by construction — the far side takes no lock
    // and writes nothing — so a followed run and an unobserved one must reach
    // byte-identical manifests.
    let behaviors = r#""succeed", "flaky:1", "sleep:400", "succeed""#;
    let dir = tempfile::tempdir().expect("temp dir");
    let bin = ssh_shim(dir.path());
    let config = write_config(dir.path(), behaviors);
    let path = config.to_str().expect("utf-8 path");
    let mut run = common::driving(&config);
    let followed = sima_shimmed(&bin, &["follow", path, "--on", "gpubox"]);
    assert_eq!(followed.status.code(), Some(0), "{followed:?}");
    assert_eq!(run.wait().expect("wait for sima run").code(), Some(0));

    let reference_dir = tempfile::tempdir().expect("reference temp dir");
    let reference = write_config(reference_dir.path(), behaviors);
    assert_eq!(
        sima(&["run", reference.to_str().expect("utf-8 path")])
            .status
            .code(),
        Some(0)
    );
    assert_eq!(
        manifest_of(&config).expect("the followed run's manifest"),
        manifest_of(&reference).expect("the unobserved run's manifest"),
    );
}

#[test]
#[ignore = "requires an ssh destination in SIMA_TEST_FOLLOW_HOST"]
fn a_remote_view_over_ssh_renders_exactly_what_the_local_one_renders() {
    let Some(host) = follow_host() else {
        return;
    };
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "flaky:1", "succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    for view in views(path) {
        let local = sima(&view);
        let remote = sima(&[view.clone(), vec!["--on", &host]].concat());
        assert_eq!(remote.status.code(), Some(0), "{view:?}: {remote:?}");
        assert_eq!(stdout(&remote), stdout(&local), "{view:?}");
    }
}

#[test]
#[ignore = "requires an ssh destination in SIMA_TEST_FOLLOW_HOST"]
fn a_remote_follow_over_ssh_streams_a_live_run_to_its_end() {
    let Some(host) = follow_host() else {
        return;
    };
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""sleep:800", "sleep:800", "sleep:800""#);
    let path = config.to_str().expect("utf-8 path");
    let mut run = common::driving(&config);

    let output = sima(&["follow", path, "--on", &host]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(stdout(&output).contains("finalized"), "{output:?}");
    assert_eq!(run.wait().expect("wait for sima run").code(), Some(0));
}

#[test]
#[ignore = "requires an ssh destination in SIMA_TEST_FOLLOW_HOST"]
fn a_followed_run_over_ssh_finalizes_to_the_unobserved_manifest() {
    let Some(host) = follow_host() else {
        return;
    };
    let behaviors = r#""succeed", "flaky:1", "sleep:400", "succeed""#;
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), behaviors);
    let path = config.to_str().expect("utf-8 path");
    let mut run = common::driving(&config);
    let followed = sima(&["follow", path, "--on", &host]);
    assert_eq!(followed.status.code(), Some(0), "{followed:?}");
    assert_eq!(run.wait().expect("wait for sima run").code(), Some(0));

    let reference_dir = tempfile::tempdir().expect("reference temp dir");
    let reference = write_config(reference_dir.path(), behaviors);
    assert_eq!(
        sima(&["run", reference.to_str().expect("utf-8 path")])
            .status
            .code(),
        Some(0)
    );
    assert_eq!(
        manifest_of(&config).expect("the followed run's manifest"),
        manifest_of(&reference).expect("the unobserved run's manifest"),
    );
}

#[test]
#[ignore = "requires an ssh destination in SIMA_TEST_FOLLOW_HOST"]
fn an_unreachable_host_fails_promptly_and_names_it() {
    if follow_host().is_none() {
        return;
    }
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // BatchMode refuses rather than prompting, so an unresolvable destination
    // is a prompt refusal instead of a hang on a password prompt.
    let output = common::wait_within(
        sima_command()
            .args(["status", path, "--on", "sima.invalid.test"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn sima"),
        Duration::from_secs(60),
    );
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("sima.invalid.test"), "{stderr}");
}
