//! [`FarSide`]: everything a migration does to the destination machine.
//!
//! One boundary carries the whole of it — confirm the machine can drive the search,
//! place the search's directory and config, tell a search already going from one that
//! ended, start it detached, sync against it, follow it, and ask it to wind
//! down. Keeping the operations behind a trait is what lets the choreography be
//! driven against a recording double, the same boundary `Provider` and
//! `WorkerTransport` already establish.
//!
//! The production implementation reaches the machine through [`Reach`], so the
//! two destination forms differ only in how they were obtained: a machine of
//! yours at the ssh destination its entry names, a rented one at the endpoint
//! its provider reported — or, for the stub provider, at a `sima` on this
//! machine, so every layer above searches with no network.
//!
//! **Far-side paths travel unresolved.** `~/sima` is the far shell's to
//! expand, so a destination's `root` reaches the shell unquoted and must be a
//! path, not an expression — which is what a host declaration states.

use std::io::Write;
use std::process::{Command, Stdio};

use sima_core::{Error, Result, own_process_group};
use sima_domains::devices::DeviceInfo;
use sima_model::{FormatId, SearchId, TaskKey};
use sima_provider::{Provider, SshEndpoint};
use sima_scheduler::Record;
use sima_store::{ObjectScope, Store, SyncReport};
use sima_transport::{SpawnMode, SshDestination};

use crate::config::{Container, OwnedHost};
use crate::devices::parse_enumeration;
use crate::feed::{RemoteFeed, SearchFeed, snapshot_over_argv};
use crate::migrate::destination::Destination;
use crate::migrate::far_config::FarLayout;
use crate::migrate::sync::{Reach, sync_over};
use crate::process::{ImageCheck, bootstrap_image, command_stdout};
use crate::program_binding::BinaryChange;
use crate::rental::{endpoint_target, transport_mode};

/// The far side of a migration: the machine the search moves onto, and every
/// operation the choreography performs on it.
///
/// The directory the search lives in is the implementation's own — it derives from
/// the search id under the destination's `root` — so no caller passes a path.
pub(crate) trait FarSide {
    /// Confirms the machine can drive this search, and reports the devices it
    /// offers.
    ///
    /// The two forms answer differently. A machine of yours answers with the
    /// worker image its entry names being present, and reports no device: its
    /// worker layout is declared, not probed. A rented machine answers with its
    /// enumeration probe, which is also where its layout comes from.
    ///
    /// A machine that could not be reached at all is [`Contact::Unreachable`]
    /// rather than an error, so the caller can wait for one that is still
    /// coming up while reporting at once what waiting cannot fix.
    fn devices(&self) -> Result<Contact>;

    /// Creates the search's directory and writes `config` into it.
    fn place(&self, config: &str) -> Result<()>;

    /// Whether the search's directory is there, which is what a migration onto
    /// this machine leaves behind and nothing else creates.
    ///
    /// A recall asks it before anything else: a destination that was never
    /// migrated to has nothing to end, nothing to pull, and no far config to
    /// read, and saying so beats every later step's own confusion.
    fn placed(&self) -> Result<bool>;

    /// The far-side `sima search` process id, when `search.pid` names one that is
    /// still alive. A machine that was never started, one whose search has exited,
    /// and one with no directory at all all answer `None`.
    fn driving(&self) -> Result<Option<u32>>;

    /// Starts the far-side `sima search` detached, records its pid in `search.pid`,
    /// and returns it.
    ///
    /// `accept` is what this invocation stated about a program whose build
    /// changed under the search. The comparison itself is the far search's — it
    /// journals what it installed and compares against what it journaled — so
    /// the acceptance travels to it rather than being decided here.
    fn start(&self, accept: BinaryChange) -> Result<u32>;

    /// Asks the far search to wind down. `sima search` drains its in-flight attempts
    /// on `SIGINT` and leaves the far store resumable.
    fn interrupt(&self, pid: u32) -> Result<()>;

    /// Ends the far search outright, for a search that outlasted the wind-down it was
    /// asked for. The store it leaves is resumable, which is what a search that
    /// dies without winding down always leaves.
    fn terminate(&self, pid: u32) -> Result<()>;

    /// Runs one store sync session against the far side, over this side's own
    /// key set and the object scope the direction calls for.
    fn sync(&self, store: &Store, keys: &[TaskKey], scope: ObjectScope<'_>) -> Result<SyncReport>;

    /// Opens a live follow of the far search.
    fn follow(&self) -> Result<Box<dyn SearchFeed>>;

    /// The far search's journal, read once and in full — or `None` when the far
    /// store holds no journal at all.
    ///
    /// A recall follows nothing, so this is the only way what the far search ended
    /// as reaches this side: a definitive failure is written there and travels
    /// no other way, since journals do not sync.
    ///
    /// **Absence is a filesystem fact, never an inference from a fault.** The
    /// journal file is probed for before it is read, and that probe alone
    /// answers `None` — a search that died before writing a line, a directory
    /// holding no store yet. Everything else fails: a far side that holds a
    /// journal and answers the read with a fault said nothing about how the search
    /// ended, and taking that for an empty journal would bring a search that
    /// cannot complete home as one with work still to do.
    fn snapshot(&self) -> Result<Option<Vec<Record>>>;

    /// The last lines of the far search's log.
    ///
    /// A far `sima search` that fails while loading its config — a program that
    /// cannot answer, an install that exits non-zero — dies before it journals
    /// anything, so the follow finds a search that never started and can say only
    /// that. Its own words are in the log it wrote, and this is how they reach
    /// the operator who asked for the migration.
    fn log_tail(&self) -> Result<String>;
}

/// How much of the far search's log an attach failure carries. Enough for a
/// config-load failure to state itself in full; the whole log is on the
/// destination, at the path the layout fixes.
const LOG_TAIL_LINES: usize = 40;

/// What a first contact with the destination found.
pub(crate) enum Contact {
    /// The machine answered, offering these devices — none for a machine of
    /// yours, whose worker layout is declared rather than probed.
    Answered(Vec<DeviceInfo>),
    /// The machine could not be reached, which a fresh or rebooting one answers
    /// until it is up. The error is what to report if it never comes up.
    Unreachable(Error),
}

/// How a destination answers that it can drive the search, which is the one thing
/// the two forms do differently.
enum Readiness {
    /// A machine of yours: the worker image its entry names must be present on
    /// it, since that is where its far-side workers search.
    Image {
        /// The ssh destination the image is inspected over.
        host: String,
        container: Container,
    },
    /// A rented machine: `sima-worker --enumerate-devices` inside the
    /// instance's own container.
    EnumerateDevices {
        mode: SpawnMode,
        target: SshDestination,
        /// The format the probe asks about, whose answer is also the machine's
        /// device layout. `None` for a search whose format is a program the
        /// machine has not been given yet, and the answer then states only that
        /// the machine is up.
        format: Option<FormatId>,
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
    pub(crate) fn owned(
        destination: &Destination<'_>,
        owned: &OwnedHost,
        search: &SearchId,
    ) -> Remote {
        let target = SshDestination::known(&owned.ssh);
        Remote {
            reach: Reach::new(&SpawnMode::Ssh, &target, destination.binary),
            layout: FarLayout::new(destination.root, search),
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
    /// `format` is what the readiness probe asks about, and `None` for a search
    /// whose format is a program: nothing on that machine can resolve a format
    /// it has not been given yet, so the probe asks about none and its answer
    /// states only that the machine is up.
    pub(crate) fn rented(
        destination: &Destination<'_>,
        provider: &(dyn Provider + Sync),
        endpoint: &SshEndpoint,
        search: &SearchId,
        format: Option<&FormatId>,
    ) -> Result<Remote> {
        let mode = transport_mode(provider)?;
        let target = endpoint_target(endpoint.clone());
        Ok(Remote {
            reach: Reach::new(&mode, &target, destination.binary),
            layout: FarLayout::new(destination.root, search),
            readiness: Readiness::EnumerateDevices {
                mode,
                target,
                format: format.cloned(),
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
        let mut child = own_process_group(&mut Command::new(program))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Inherited, so a far-side diagnostic reaches the operator's
            // terminal as it happens.
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| Error::Transport(format!("cannot search {program:?} on {label}: {e}")))?;
        child
            .stdin
            .take()
            .expect("the spawn configured a piped stdin")
            .write_all(script.as_bytes())
            .map_err(|e| Error::Transport(format!("cannot send a command to {label}: {e}")))?;
        let output = child
            .wait_with_output()
            .map_err(|e| Error::Transport(format!("cannot reap the shell on {label}: {e}")))?;
        if !output.status.success() {
            return Err(Error::Transport(format!(
                "a command on {label} exited with {}",
                output.status
            )));
        }
        String::from_utf8(output.stdout)
            .map_err(|e| Error::Transport(format!("output from {label} is not UTF-8: {e}")))
    }

    /// Whether the far store holds this search's journal, which is what the
    /// one-shot read needs there to be.
    ///
    /// The path is the store's own layout applied to the far store root, so
    /// nothing here restates where a journal sits.
    fn journaled(&self) -> Result<bool> {
        // As with the directory `placed` probes for, the file's absence is an
        // answer rather than a failure, so the script exits zero either way and
        // its output is the whole of what it said.
        let stdout = self.shell(&format!(
            "[ -f {journal} ] && echo yes\nexit 0\n",
            journal = self.layout.journal(),
        ))?;
        Ok(stdout.trim() == "yes")
    }
}

impl FarSide for Remote {
    fn devices(&self) -> Result<Contact> {
        match &self.readiness {
            Readiness::Image { host, container } => match bootstrap_image(Some(host), container)? {
                ImageCheck::Present => Ok(Contact::Answered(Vec::new())),
                ImageCheck::Unreachable(error) => Ok(Contact::Unreachable(error)),
            },
            Readiness::EnumerateDevices {
                mode,
                target,
                format,
            } => {
                let probe = format.as_ref().map_or(
                    sima_transport::DeviceProbe::EveryBackend,
                    sima_transport::DeviceProbe::Format,
                );
                let argv = sima_transport::ssh::probe_argv(mode, target, probe);
                // The probe's stdout is what carries its answer, so a failure
                // here states only that no answer came back — a connection that
                // was refused and a probe that ran and said nothing usable read
                // the same. Both are answered as unreachable, which is what
                // this arm has always done with them.
                match command_stdout(&argv).and_then(|stdout| parse_enumeration(&stdout)) {
                    Ok(devices) => Ok(Contact::Answered(devices)),
                    Err(error) => Ok(Contact::Unreachable(error)),
                }
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

    fn placed(&self) -> Result<bool> {
        // The directory's absence is an answer, not a failure, so the script
        // exits zero either way and its output is the whole of what it said.
        let stdout = self.shell(&format!(
            "[ -d {dir} ] && echo yes\nexit 0\n",
            dir = self.layout.dir(),
        ))?;
        Ok(stdout.trim() == "yes")
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

    fn start(&self, accept: BinaryChange) -> Result<u32> {
        // `setsid` detaches the search from the session that started it, so the
        // far side keeps computing when this migration's connection drops. It
        // does not fork when it is not already a process-group leader, which a
        // background job of a non-interactive shell is not, so `$!` is the search's
        // own pid and not a shell's.
        //
        // No `--fleet`: the synthesized config declares no machine beyond the
        // one it sits on, so the flag would engage an empty fleet.
        //
        // A shell starts an asynchronous command with `SIGINT` ignored, and the
        // disposition survives the exec, so the wind-down's `kill -INT` reaches
        // a search that installs its own handler and no other. `sima search`
        // registers one, which replaces the inherited disposition.
        // The `cd` is the guard that the placement happened: without it a
        // missing directory would surface only as a redirection failure inside
        // the background job, which the script's own exit status never sees.
        //
        // `--accept-binary` when this invocation stated it: the far search
        // installs the payload and compares its digest against what it
        // journaled, so the acceptance has to reach it to have any effect.
        let stdout = self.shell(&format!(
            "cd {dir} || exit 1\n\
             setsid nohup {binary} search {config}{accept} > {log} 2>&1 < /dev/null &\n\
             pid=$!\n\
             echo $pid > {pid}\n\
             echo $pid\n",
            dir = self.layout.dir(),
            binary = self.reach.binary(),
            config = self.layout.config(),
            accept = match accept {
                BinaryChange::Accept => " --accept-binary",
                BinaryChange::Refuse => "",
            },
            log = self.layout.log(),
            pid = self.layout.pid(),
        ))?;
        parse_pid(stdout.trim(), &self.reach.label())?.ok_or_else(|| {
            Error::Validation(format!(
                "starting the search on {} reported no process id",
                self.reach.label()
            ))
        })
    }

    fn interrupt(&self, pid: u32) -> Result<()> {
        // A search that exited between the poll and the signal is not a failure:
        // the wind-down wanted it gone, and it is.
        self.shell(&format!("kill -INT {pid} 2>/dev/null\nexit 0\n"))?;
        Ok(())
    }

    fn terminate(&self, pid: u32) -> Result<()> {
        // `SIGKILL` rather than `SIGTERM`: the graceful request has already been
        // made and re-made for the whole wind-down bound, so what is wanted here
        // is the signal a process cannot decline. A search already gone is the
        // outcome, not a fault.
        self.shell(&format!("kill -KILL {pid} 2>/dev/null\nexit 0\n"))?;
        Ok(())
    }

    fn sync(&self, store: &Store, keys: &[TaskKey], scope: ObjectScope<'_>) -> Result<SyncReport> {
        sync_over(
            store,
            keys,
            scope,
            &self.reach,
            &self.layout.store(),
            self.layout.search(),
        )
    }

    fn follow(&self) -> Result<Box<dyn SearchFeed>> {
        let argv = self.reach.follow_serve_argv(&self.layout.config());
        Ok(Box::new(RemoteFeed::open_over(&argv, &self.reach.label())?))
    }

    fn snapshot(&self) -> Result<Option<Vec<Record>>> {
        // The probe first: a journal file that is not there is the one absence,
        // so every way the read below can fail is a failure — including a far
        // side answering for itself, which says nothing about how the search
        // ended.
        if !self.journaled()? {
            return Ok(None);
        }
        let argv = self.reach.follow_serve_once_argv(&self.layout.config());
        let (_, records) = snapshot_over_argv(&argv, &self.reach.label())?;
        Ok(Some(records))
    }

    fn log_tail(&self) -> Result<String> {
        // A search that never started leaves no log, and that absence is itself
        // the answer to give: the script exits zero with nothing to say.
        self.shell(&format!(
            "tail -n {LOG_TAIL_LINES} {log} 2>/dev/null\nexit 0\n",
            log = self.layout.log(),
        ))
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

    /// The search every fixture places on its far side.
    fn search() -> SearchId {
        SearchId::from_hash(sima_core::hash_bytes(b"a migrated search"))
    }

    /// A far side reached without a hop, rooted at `root` and driving `binary`.
    ///
    /// The readiness is the rented form over a local spawn, which is what the
    /// stub provider's destination is; the tests here drive the directory and
    /// process operations, which are the same in both forms.
    fn here(root: &Path, binary: &Path) -> Remote {
        let loaded = load_str(&format!(
            r#"
            [search]
            root_seed = 1
            format = "stub.v1"

            [search.generator]
            id = "stub.v1"
            behaviors = ["succeed"]

            [config]
            store = "./store"
            max_attempts = 1

            [orchestrator]
            workers = 1
            migrate = "cloudbox"

            [host.cloudbox]
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
            &search(),
            Some(&FormatId::new("stub.v1").expect("format id")),
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

    /// A stand-in for the far side's `sima` that records the arguments it was
    /// started with, at `<dir>/argv`, before it lives for `seconds`.
    fn recording_binary(dir: &Path, seconds: &str) -> std::path::PathBuf {
        let path = dir.join("sima");
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\necho \"$@\" > {}\nexec sleep {seconds}\n",
                dir.join("argv").display(),
            ),
        )
        .expect("write the stand-in");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("make it executable");
        path
    }

    /// A stand-in for the far side's `sima` that answers a follow with one
    /// `Fault` frame and nothing else, which is how a far side reports for
    /// itself: a config that does not load, a store that will not open, a
    /// journal that will not parse.
    fn faulting_binary(dir: &Path, words: &str) -> std::path::PathBuf {
        answering_binary(dir, &[crate::feed::FollowFrame::Fault(words.to_string())])
    }

    /// A stand-in that answers a one-shot follow the way a far side serving a
    /// journal does: the handshake, the journal's lines — none here — and the
    /// end of the stream.
    fn serving_binary(dir: &Path) -> std::path::PathBuf {
        answering_binary(
            dir,
            &[
                crate::feed::FollowFrame::Hello {
                    protocol: crate::feed::FOLLOW_PROTOCOL_VERSION,
                    search: search(),
                    format: FormatId::new("stub.v1").expect("format id"),
                    workers: 1,
                    holder: None,
                },
                crate::feed::FollowFrame::Records(Vec::new()),
                crate::feed::FollowFrame::Complete,
            ],
        )
    }

    /// A stand-in for the far side's `sima` that writes `frames` and exits,
    /// which is the whole of what a follow reads from it.
    fn answering_binary(dir: &Path, frames: &[crate::feed::FollowFrame]) -> std::path::PathBuf {
        let path = dir.join("frames");
        let mut bytes = Vec::new();
        for frame in frames {
            sima_core::write_frame(&mut bytes, &frame.encode()).expect("frame the answer");
        }
        std::fs::write(&path, bytes).expect("write the frames");
        let binary = dir.join("sima");
        std::fs::write(&binary, format!("#!/bin/sh\nexec cat {}\n", path.display()))
            .expect("write the stand-in");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))
            .expect("make it executable");
        binary
    }

    /// Writes an empty journal where the far store's own layout puts this
    /// search's, which is what a destination that has journaled looks like to the
    /// probe.
    fn far_journal(far: &Remote) {
        let path = sima_store::journal_path(Path::new(&far.layout.store()), &search());
        std::fs::create_dir_all(path.parent().expect("the search's directory"))
            .expect("the store tree");
        std::fs::write(&path, "").expect("write the journal");
    }

    #[test]
    fn a_journal_that_is_not_there_is_an_absence_and_one_that_is_is_read() -> Result<()> {
        // The two answers the probe decides between, over the path the far
        // store's own layout fixes: no file at all is the absence a wind-back
        // settles over, and a file is a read whose records — none here — are
        // what the far search journaled.
        let dir = tempfile::tempdir().expect("temp dir");
        let binary = serving_binary(dir.path());
        let far = here(dir.path(), &binary);
        far.place("[search]\nroot_seed = 1\n")?;

        assert!(!far.journaled()?, "nothing has journaled there");
        assert_eq!(far.snapshot()?, None, "so there is nothing to read");
        far_journal(&far);
        assert!(far.journaled()?, "the journal is where the layout puts it");
        assert_eq!(far.snapshot()?, Some(Vec::new()), "and it is read");
        Ok(())
    }

    #[test]
    fn a_fault_over_an_existing_journal_is_a_failure_rather_than_no_records() -> Result<()> {
        // Absence is a filesystem fact, and a fault is not one: the far side
        // holds a journal and could not serve it, so what the search ended as is
        // exactly what it did not say. Reading that as an empty journal would
        // bring a search that failed definitively home as resumable.
        let dir = tempfile::tempdir().expect("temp dir");
        let binary = faulting_binary(
            dir.path(),
            "validation error: the store there will not open",
        );
        let far = here(dir.path(), &binary);
        far.place("[search]\nroot_seed = 1\n")?;
        far_journal(&far);

        let error = far.snapshot().expect_err("the journal could not be read");
        assert!(
            error.to_string().contains("the store there will not open"),
            "the far side's own words come home: {error}"
        );
        Ok(())
    }

    /// The arguments the recording stand-in was started with, waited for: the
    /// start returns when the shell that backgrounded it does, which is before
    /// the job itself has search.
    fn recorded_argv(dir: &Path) -> String {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(argv) = std::fs::read_to_string(dir.join("argv")) {
                return argv;
            }
            assert!(Instant::now() < deadline, "the stand-in never recorded");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn the_log_tail_is_the_far_search_s_last_lines() -> Result<()> {
        // What the operator sees when the far search died before it journaled:
        // its own output, read over the same shell channel every other far-side
        // operation uses.
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        far.place("[search]\nroot_seed = 1\n")?;
        let log: String = (0..LOG_TAIL_LINES + 10)
            .map(|line| format!("line {line}\n"))
            .collect();
        std::fs::write(
            dir.path().join(search().to_string()).join("search.log"),
            &log,
        )
        .expect("write the log");

        let tail = far.log_tail()?;
        let lines: Vec<&str> = tail.lines().collect();
        assert_eq!(lines.len(), LOG_TAIL_LINES);
        assert_eq!(lines.last(), Some(&"line 49"));
        assert_eq!(lines.first(), Some(&"line 10"));
        Ok(())
    }

    #[test]
    fn a_far_search_that_left_no_log_answers_with_nothing() -> Result<()> {
        // A search that never started wrote no log, and the absence is the
        // answer: reading it is not itself a failure to report.
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        far.place("[search]\nroot_seed = 1\n")?;
        assert!(far.log_tail()?.trim().is_empty());
        Ok(())
    }

    #[test]
    fn the_acceptance_of_a_changed_program_is_what_the_far_search_is_started_with() -> Result<()> {
        // The far search installs the payload and compares its digest against
        // what it journaled, so the flag has to reach its argv to have any
        // effect there.
        for (accept, expected) in [(BinaryChange::Accept, true), (BinaryChange::Refuse, false)] {
            let dir = tempfile::tempdir().expect("temp dir");
            let binary = recording_binary(dir.path(), "30");
            let far = here(dir.path(), &binary);
            far.place("[search]\nroot_seed = 1\n")?;
            let pid = far.start(accept)?;
            let argv = recorded_argv(dir.path());
            assert!(argv.contains("search"), "it is a search: {argv}");
            assert_eq!(
                argv.contains("--accept-binary"),
                expected,
                "{accept:?} produced {argv}"
            );
            kill(pid);
            until_gone(&far)?;
        }
        Ok(())
    }

    /// Polls `far` until nothing is driving it, or the wait searches out.
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
        far.place("[search]\nroot_seed = 1\n")?;
        let placed = dir.path().join(search().to_string()).join("sima.toml");
        assert_eq!(
            std::fs::read_to_string(&placed).expect("the config was written"),
            "[search]\nroot_seed = 1\n\n",
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
        let config = "[search]\nhex = \"$HOME `id` \\\\ 'quoted'\"\n";
        far.place(config)?;
        let placed = dir.path().join(search().to_string()).join("sima.toml");
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
        far.place("[search]\nroot_seed = 1\n")?;
        far.place("[search]\nroot_seed = 2\n")?;
        let placed = dir.path().join(search().to_string()).join("sima.toml");
        assert_eq!(
            std::fs::read_to_string(&placed).expect("the config was written"),
            "[search]\nroot_seed = 2\n\n"
        );
        Ok(())
    }

    #[test]
    fn a_far_side_that_was_never_started_is_driving_nothing() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        // No directory at all.
        assert_eq!(far.driving()?, None);
        // A directory, but no search.
        far.place("[search]\nroot_seed = 1\n")?;
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
        far.place("[search]\nroot_seed = 1\n")?;

        let pid = far.start(BinaryChange::Refuse)?;
        assert_eq!(
            far.driving()?,
            Some(pid),
            "the recorded pid names the live search"
        );
        let home = dir.path().join(search().to_string());
        assert_eq!(
            std::fs::read_to_string(home.join("search.pid"))
                .expect("the pid file")
                .trim(),
            pid.to_string(),
            "a second invocation reads the pid from the file"
        );
        assert!(
            home.join("search.log").is_file(),
            "the search's output is kept"
        );

        // The search ends of its own accord, which is what a terminal search event
        // leaves behind, and the far side stops reporting it.
        kill(pid);
        assert_eq!(until_gone(&far)?, None);
        Ok(())
    }

    #[test]
    fn a_run_that_ended_before_the_signal_is_not_a_failure() -> Result<()> {
        // The window between the wind-down's poll and its signal: the search the
        // signal wanted gone is gone, which is the outcome, not a fault.
        let dir = tempfile::tempdir().expect("temp dir");
        let binary = sleeping_binary(dir.path(), "30");
        let far = here(dir.path(), &binary);
        far.place("[search]\nroot_seed = 1\n")?;
        let pid = far.start(BinaryChange::Refuse)?;
        kill(pid);
        assert_eq!(until_gone(&far)?, None);
        far.interrupt(pid)?;
        Ok(())
    }

    /// Ends a detached far-side process. `SIGINT` is not what does it: a shell
    /// starts an asynchronous command with `SIGINT` ignored and the disposition
    /// survives the exec, so a stand-in that installs no handler of its own —
    /// unlike `sima search`, which does — never sees one.
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
        // What a far side looks like after its search ended: the directory and the
        // pid file survive, and neither means the search is still going.
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        far.place("[search]\nroot_seed = 1\n")?;
        let home = dir.path().join(search().to_string());
        // A pid that cannot be live: the kernel's own maximum plus one is out of
        // range for any process.
        std::fs::write(home.join("search.pid"), "4194305\n").expect("write the pid file");
        assert_eq!(far.driving()?, None);
        Ok(())
    }

    #[test]
    fn an_empty_pid_file_is_driving_nothing() -> Result<()> {
        // The window between the redirection creating the file and the shell
        // writing into it.
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        far.place("[search]\nroot_seed = 1\n")?;
        std::fs::write(dir.path().join(search().to_string()).join("search.pid"), "")
            .expect("write the pid file");
        assert_eq!(far.driving()?, None);
        Ok(())
    }

    #[test]
    fn a_start_into_a_directory_that_is_not_there_fails() {
        let dir = tempfile::tempdir().expect("temp dir");
        let far = here(dir.path(), Path::new("/bin/true"));
        // Nothing was placed, so the search has nowhere to start.
        assert!(far.start(BinaryChange::Refuse).is_err());
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
