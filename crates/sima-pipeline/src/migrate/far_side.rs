//! [`FarSide`]: everything a migration does to the destination machine.
//!
//! One seam carries the whole of it — confirm the machine can drive the run,
//! place the run's directory and config, tell a run already going from one that
//! ended, start it detached, sync against it, follow it, and ask it to wind
//! down. Keeping the operations behind a trait is what lets the choreography be
//! driven against a recording double, the same seam `Provider` and
//! `WorkerTransport` already establish.
//!
//! The production implementation reaches the machine through [`Reach`], so the
//! two destination forms differ only in how they were obtained: a machine of
//! yours at the ssh destination its entry names, a rented one at the endpoint
//! its provider reported — or, for the stub provider, at a `sima` on this
//! machine, so every layer above runs with no network.
//!
//! **Far-side paths travel unresolved.** `~/sima-runs` is the far shell's to
//! expand, so a destination's `root` reaches the shell unquoted and must be a
//! path, not an expression — which is what a host declaration states.

use std::io::Write;
use std::process::{Command, Stdio};

use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::{FormatId, RunId, TaskKey};
use sima_provider::{Provider, SshEndpoint};
use sima_store::{ObjectScope, Store, SyncReport};
use sima_transport::{SpawnMode, SshDestination};

use crate::config::{Container, OwnedHost};
use crate::devices::parse_enumeration;
use crate::feed::{RemoteFeed, RunFeed};
use crate::migrate::destination::Destination;
use crate::migrate::far_config::FarLayout;
use crate::migrate::sync::{Reach, sync_over};
use crate::orchestrate::{bootstrap_image, command_stdout};
use crate::rental::{endpoint_target, transport_mode};

/// The far side of a migration: the machine the run moves onto, and every
/// operation the choreography performs on it.
///
/// The directory the run lives in is the implementation's own — it derives from
/// the run id under the destination's `root` — so no caller passes a path.
pub(crate) trait FarSide {
    /// Confirms the machine can drive this run, and reports the devices it
    /// offers.
    ///
    /// The two forms answer differently. A machine of yours answers with the
    /// worker image its entry names being present, and reports no device: its
    /// worker layout is declared, not probed. A rented machine answers with its
    /// enumeration probe, which is also where its layout comes from.
    fn devices(&self) -> Result<Vec<DeviceInfo>>;

    /// Creates the run's directory and writes `config` into it.
    fn place(&self, config: &str) -> Result<()>;

    /// The far-side `sima run` process id, when `run.pid` names one that is
    /// still alive. A machine that was never started, one whose run has exited,
    /// and one with no directory at all all answer `None`.
    fn driving(&self) -> Result<Option<u32>>;

    /// Starts the far-side `sima run` detached, records its pid in `run.pid`,
    /// and returns it.
    fn start(&self) -> Result<u32>;

    /// Asks the far run to wind down. `sima run` drains its in-flight attempts
    /// on `SIGINT` and leaves the far store resumable.
    fn interrupt(&self, pid: u32) -> Result<()>;

    /// Ends the far run outright, for a run that outlasted the wind-down it was
    /// asked for. The store it leaves is resumable, which is what a run that
    /// dies without winding down always leaves.
    fn terminate(&self, pid: u32) -> Result<()>;

    /// Runs one store sync session against the far side, over this side's own
    /// key set and the object scope the direction calls for.
    fn sync(&self, store: &Store, keys: &[TaskKey], scope: ObjectScope<'_>) -> Result<SyncReport>;

    /// Opens a live follow of the far run.
    fn follow(&self) -> Result<Box<dyn RunFeed>>;
}

/// How a destination answers that it can drive the run, which is the one thing
/// the two forms do differently.
enum Readiness {
    /// A machine of yours: the worker image its entry names must be present on
    /// it, since that is where its far-side workers run.
    Image {
        /// The ssh destination the image is inspected over.
        host: String,
        container: Container,
    },
    /// A rented machine: `sima-worker --enumerate` inside the instance's own
    /// container, which is also the machine's device layout.
    Enumerate {
        mode: SpawnMode,
        target: SshDestination,
        format: FormatId,
    },
}

/// The far side of a real destination, reached over ssh or over a `sima` on
/// this machine.
pub(crate) struct Remote {
    reach: Reach,
    layout: FarLayout,
    readiness: Readiness,
}

impl Remote {
    /// The far side of a machine of yours, reached at the ssh destination its
    /// entry names.
    pub(crate) fn owned(destination: &Destination<'_>, owned: &OwnedHost, run: &RunId) -> Remote {
        let target = SshDestination::known(&owned.ssh);
        Remote {
            reach: Reach::new(&SpawnMode::Ssh, &target, destination.binary),
            layout: FarLayout::new(destination.root, run),
            readiness: Readiness::Image {
                host: owned.ssh.clone(),
                container: owned.container.clone(),
            },
        }
    }

    /// The far side of a rented machine, reached at the endpoint its provider
    /// reported and the way that provider says its machines are reached — over
    /// ssh, or on this machine for an in-process backend — so both the probe
    /// and the far-side commands land where the machine actually is.
    pub(crate) fn rented(
        destination: &Destination<'_>,
        provider: &(dyn Provider + Sync),
        endpoint: &SshEndpoint,
        run: &RunId,
        format: &FormatId,
    ) -> Result<Remote> {
        let mode = transport_mode(provider)?;
        let target = endpoint_target(endpoint.clone());
        Ok(Remote {
            reach: Reach::new(&mode, &target, destination.binary),
            layout: FarLayout::new(destination.root, run),
            readiness: Readiness::Enumerate {
                mode,
                target,
                format: format.clone(),
            },
        })
    }

    /// Runs `script` through a shell on the far side and returns its stdout.
    ///
    /// The script travels on the shell's stdin, so nothing in it is quoted for
    /// the hop. Writing it before reading the output cannot deadlock: every
    /// script here is a few hundred bytes, well inside a pipe buffer.
    fn shell(&self, script: &str) -> Result<String> {
        let argv = self.reach.shell_argv();
        let (program, args) = argv.split_first().expect("the argv names a program");
        let label = self.reach.label();
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Inherited, so a far-side diagnostic reaches the operator's
            // terminal as it happens.
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| Error::Validation(format!("cannot run {program:?} on {label}: {e}")))?;
        child
            .stdin
            .take()
            .expect("the spawn configured a piped stdin")
            .write_all(script.as_bytes())
            .map_err(|e| Error::Validation(format!("cannot send a command to {label}: {e}")))?;
        let output = child
            .wait_with_output()
            .map_err(|e| Error::Validation(format!("cannot reap the shell on {label}: {e}")))?;
        if !output.status.success() {
            return Err(Error::Validation(format!(
                "a command on {label} exited with {}",
                output.status
            )));
        }
        String::from_utf8(output.stdout)
            .map_err(|e| Error::Validation(format!("output from {label} is not UTF-8: {e}")))
    }
}

impl FarSide for Remote {
    fn devices(&self) -> Result<Vec<DeviceInfo>> {
        match &self.readiness {
            Readiness::Image { host, container } => {
                bootstrap_image(Some(host), container)?;
                Ok(Vec::new())
            }
            Readiness::Enumerate {
                mode,
                target,
                format,
            } => {
                let argv = sima_transport::ssh::probe_argv(mode, target, format);
                parse_enumeration(&command_stdout(&argv)?)
            }
        }
    }

    fn place(&self, config: &str) -> Result<()> {
        // The directory travels unquoted so the far shell expands a leading
        // tilde; the config travels in a quoted heredoc so nothing in it is
        // expanded.
        self.shell(&format!(
            "set -e\n\
             mkdir -p {dir}\n\
             cat > {config_path} <<'SIMA_CONFIG'\n\
             {config}\n\
             SIMA_CONFIG\n",
            dir = self.layout.dir(),
            config_path = self.layout.config(),
        ))?;
        Ok(())
    }

    fn driving(&self) -> Result<Option<u32>> {
        // Every way the answer is "nothing is driving it" exits zero with no
        // output: no directory, no pid file, an empty one, or a pid no process
        // answers to.
        let stdout = self.shell(&format!(
            "pid=$(cat {pid} 2>/dev/null) || exit 0\n\
             [ -n \"$pid\" ] || exit 0\n\
             kill -0 \"$pid\" 2>/dev/null && echo \"$pid\"\n\
             exit 0\n",
            pid = self.layout.pid(),
        ))?;
        parse_pid(stdout.trim(), &self.reach.label())
    }

    fn start(&self) -> Result<u32> {
        // `setsid` detaches the run from the session that started it, so the
        // far side keeps computing when this migration's connection drops. It
        // does not fork when it is not already a process-group leader, which a
        // background job of a non-interactive shell is not, so `$!` is the run's
        // own pid and not a shell's.
        //
        // No `--fleet`: the synthesized config declares no machine beyond the
        // one it sits on, so the flag would engage an empty fleet.
        //
        // A shell starts an asynchronous command with `SIGINT` ignored, and the
        // disposition survives the exec, so the wind-down's `kill -INT` reaches
        // a run that installs its own handler and no other. `sima run`
        // registers one, which replaces the inherited disposition.
        // The `cd` is the guard that the placement happened: without it a
        // missing directory would surface only as a redirection failure inside
        // the background job, which the script's own exit status never sees.
        let stdout = self.shell(&format!(
            "cd {dir} || exit 1\n\
             setsid nohup {binary} run {config} > {log} 2>&1 < /dev/null &\n\
             pid=$!\n\
             echo $pid > {pid}\n\
             echo $pid\n",
            dir = self.layout.dir(),
            binary = self.reach.binary(),
            config = self.layout.config(),
            log = self.layout.log(),
            pid = self.layout.pid(),
        ))?;
        parse_pid(stdout.trim(), &self.reach.label())?.ok_or_else(|| {
            Error::Validation(format!(
                "starting the run on {} reported no process id",
                self.reach.label()
            ))
        })
    }

    fn interrupt(&self, pid: u32) -> Result<()> {
        // A run that exited between the poll and the signal is not a failure:
        // the wind-down wanted it gone, and it is.
        self.shell(&format!("kill -INT {pid} 2>/dev/null\nexit 0\n"))?;
        Ok(())
    }

    fn terminate(&self, pid: u32) -> Result<()> {
        // `SIGKILL` rather than `SIGTERM`: the graceful request has already been
        // made and re-made for the whole wind-down bound, so what is wanted here
        // is the signal a process cannot decline. A run already gone is the
        // outcome, not a fault.
        self.shell(&format!("kill -KILL {pid} 2>/dev/null\nexit 0\n"))?;
        Ok(())
    }

    fn sync(&self, store: &Store, keys: &[TaskKey], scope: ObjectScope<'_>) -> Result<SyncReport> {
        sync_over(store, keys, scope, &self.reach, &self.layout.config())
    }

    fn follow(&self) -> Result<Box<dyn RunFeed>> {
        let argv = self.reach.follow_serve_argv(&self.layout.config());
        Ok(Box::new(RemoteFeed::open_over(&argv, &self.reach.label())?))
    }
}

/// A process id a far-side script printed, or `None` for the empty output that
/// means there was none. Anything else is the far side answering something
/// other than a pid, which is a fault rather than an absence.
fn parse_pid(stdout: &str, label: &str) -> Result<Option<u32>> {
    if stdout.is_empty() {
        return Ok(None);
    }
    stdout.parse().map(Some).map_err(|e| {
        Error::Validation(format!("{label} reported {stdout:?} instead of a pid: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::fixtures::load_str;
    use crate::migrate::destination::destination_for;

    /// The run every fixture places on its far side.
    fn run() -> RunId {
        RunId::from_hash(sima_core::hash_bytes(b"a migrated run"))
    }

    /// A far side reached without a hop, rooted at `root` and driving `binary`.
    ///
    /// The readiness is the rented form over a local spawn, which is what the
    /// stub provider's destination is; the tests here drive the directory and
    /// process operations, which are the same in both forms.
    fn here(root: &Path, binary: &Path) -> Remote {
        let loaded = load_str(&format!(
            r#"
            [run]
            root_seed = 1
            format = "stub.v1"

            [run.generator]
            id = "stub.v1"
            behaviors = ["succeed"]

            [config]
            store = "./store"
            max_attempts = 1

            [orchestrator]
            workers = 1
            migrate = "slingshot"

            [host.slingshot]
            provider = "stub"
            root = {root:?}
            binary = {binary:?}
            "#,
            root = root.to_string_lossy(),
            binary = binary.to_string_lossy(),
        ));
        let destination = destination_for(&loaded).expect("the host is declared");
        // A control plane pointed at no machine, which is what makes the far
        // side this machine.
        let provider = sima_provider::stub::StubProvider::new(Vec::new());
        Remote::rented(
            &destination,
            &provider,
            &SshEndpoint {
                host: "unreached".to_string(),
                port: 22,
                user: "root".to_string(),
            },
            &run(),
            &FormatId::new("stub.v1").expect("format id"),
        )
        .expect("the stub reaches its machine without a hop")
    }

    /// A stand-in for the far side's `sima`: it ignores its arguments and lives
    /// for `seconds`, which is what the pid operations are about.
    fn sleeping_binary(dir: &Path, seconds: &str) -> std::path::PathBuf {
        let path = dir.join("sima");
        std::fs::write(&path, format!("#!/bin/sh\nexec sleep {seconds}\n"))
            .expect("write the stand-in");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("make it executable");
        }
        path
    }

    /// Polls `far` until nothing is driving it, or the wait runs out.
    fn until_gone(far: &Remote) -> Result<Option<u32>> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let pid = far.driving()?;
            if pid.is_none() || Instant::now() > deadline {
                return Ok(pid);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn placing_creates_the_run_s_directory_and_writes_its_config() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        far.place("[run]\nroot_seed = 1\n")?;
        let placed = dir.path().join(run().to_string()).join("sima.toml");
        assert_eq!(
            std::fs::read_to_string(&placed).expect("the config was written"),
            "[run]\nroot_seed = 1\n\n",
            "the heredoc carries the text and one closing newline"
        );
        Ok(())
    }

    #[test]
    fn a_config_holding_shell_syntax_is_written_verbatim() -> Result<()> {
        // The heredoc is quoted, so nothing in the config is expanded: a params
        // blob or a run_args entry may hold any byte a TOML string may hold.
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        let config = "[run]\nhex = \"$HOME `id` \\\\ 'quoted'\"\n";
        far.place(config)?;
        let placed = dir.path().join(run().to_string()).join("sima.toml");
        assert_eq!(
            std::fs::read_to_string(&placed).expect("the config was written"),
            format!("{config}\n")
        );
        Ok(())
    }

    #[test]
    fn placing_twice_leaves_the_second_config() -> Result<()> {
        // A reattaching migration places again over the directory it made the
        // first time; the directory survives and the config is the new one.
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        far.place("[run]\nroot_seed = 1\n")?;
        far.place("[run]\nroot_seed = 2\n")?;
        let placed = dir.path().join(run().to_string()).join("sima.toml");
        assert_eq!(
            std::fs::read_to_string(&placed).expect("the config was written"),
            "[run]\nroot_seed = 2\n\n"
        );
        Ok(())
    }

    #[test]
    fn a_far_side_that_was_never_started_is_driving_nothing() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        // No directory at all.
        assert_eq!(far.driving()?, None);
        // A directory, but no run.
        far.place("[run]\nroot_seed = 1\n")?;
        assert_eq!(far.driving()?, None);
        Ok(())
    }

    #[test]
    fn a_started_run_reports_its_pid_until_it_ends() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        // Long enough that the assertions below race nothing, and detached, so
        // the test's own exit does not depend on it.
        let binary = sleeping_binary(dir.path(), "30");
        let far = here(dir.path(), &binary);
        far.place("[run]\nroot_seed = 1\n")?;

        let pid = far.start()?;
        assert_eq!(
            far.driving()?,
            Some(pid),
            "the recorded pid names the live run"
        );
        let home = dir.path().join(run().to_string());
        assert_eq!(
            std::fs::read_to_string(home.join("run.pid"))
                .expect("the pid file")
                .trim(),
            pid.to_string(),
            "a second invocation reads the pid from the file"
        );
        assert!(home.join("run.log").is_file(), "the run's output is kept");

        // The run ends of its own accord, which is what a terminal run event
        // leaves behind, and the far side stops reporting it.
        kill(pid);
        assert_eq!(until_gone(&far)?, None);
        Ok(())
    }

    #[test]
    fn a_run_that_ended_before_the_signal_is_not_a_failure() -> Result<()> {
        // The window between the wind-down's poll and its signal: the run the
        // signal wanted gone is gone, which is the outcome, not a fault.
        let dir = tempfile::tempdir().expect("temp dir");
        let binary = sleeping_binary(dir.path(), "30");
        let far = here(dir.path(), &binary);
        far.place("[run]\nroot_seed = 1\n")?;
        let pid = far.start()?;
        kill(pid);
        assert_eq!(until_gone(&far)?, None);
        far.interrupt(pid)?;
        Ok(())
    }

    /// Ends a detached far-side process. `SIGINT` is not what does it: a shell
    /// starts an asynchronous command with `SIGINT` ignored and the disposition
    /// survives the exec, so a stand-in that installs no handler of its own —
    /// unlike `sima run`, which does — never sees one.
    fn kill(pid: u32) {
        // The stand-in is this test's own descendant, so the signal is this
        // process's to send.
        let signalled = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .expect("send the signal");
        assert!(signalled.success(), "the stand-in was running");
    }

    #[test]
    fn a_pid_file_naming_a_dead_process_is_driving_nothing() -> Result<()> {
        // What a far side looks like after its run ended: the directory and the
        // pid file survive, and neither means the run is still going.
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        far.place("[run]\nroot_seed = 1\n")?;
        let home = dir.path().join(run().to_string());
        // A pid that cannot be live: the kernel's own maximum plus one is out of
        // range for any process.
        std::fs::write(home.join("run.pid"), "4194305\n").expect("write the pid file");
        assert_eq!(far.driving()?, None);
        Ok(())
    }

    #[test]
    fn an_empty_pid_file_is_driving_nothing() -> Result<()> {
        // The window between the redirection creating the file and the shell
        // writing into it.
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        far.place("[run]\nroot_seed = 1\n")?;
        std::fs::write(dir.path().join(run().to_string()).join("run.pid"), "")
            .expect("write the pid file");
        assert_eq!(far.driving()?, None);
        Ok(())
    }

    #[test]
    fn a_start_into_a_directory_that_is_not_there_fails() {
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        // Nothing was placed, so the run has nowhere to start.
        assert!(far.start().is_err());
    }

    #[test]
    fn output_that_is_not_a_pid_is_named_rather_than_taken() {
        // A far side answering something other than a pid is a fault, and the
        // error carries what it said.
        let error = parse_pid("no such command", "gpubox").expect_err("not a pid");
        assert!(error.to_string().contains("no such command"), "{error}");
        assert!(error.to_string().contains("gpubox"), "{error}");
        assert_eq!(parse_pid("", "gpubox").expect("no pid"), None);
        assert_eq!(parse_pid("4321", "gpubox").expect("a pid"), Some(4321));
    }
}
