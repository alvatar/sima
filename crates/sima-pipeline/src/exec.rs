//! The exec contract: one opaque command on one rented machine.
//!
//! A remote command's exit code is returned verbatim, so code 1 can mean the
//! command or this orchestration failing. The diagnostic text distinguishes
//! them.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use sima_core::{Error, Result, hash_bytes, own_process_group};
use sima_model::SearchId;
use sima_provider::{
    AcquireLimits, Admission, Exhaustion, IncidentKind, InstanceGuard, Objective, Provider,
    Verdict, acquire, adopt, assess, is_acquisition_cancelled, never_cancelled, now_ms,
    record_incident,
};
use sima_store::{Rental, Store};

use crate::config::{ExecConfig, HostForm, load_exec};
use crate::fetch::{fetch_over, shell_quote};
use crate::migrate::sync::Reach;
use crate::process::{Probe, ssh_contact};
use crate::program_delivery::{ExecDelivery, ingest_exec};
use crate::providers::{ProviderSettings, provider_for};
use crate::rental::{endpoint_target, transport_mode};

/// How this invocation acts on the job's standing machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecAction {
    /// Start one command, keeping the machine after completion by default.
    Start { one_shot: bool },
    /// Follow the command already running there.
    Attach,
    /// End the command, fetch its files, and destroy the machine.
    End,
}

/// Invocation-specific exec settings.
#[derive(Debug, Clone)]
pub struct ExecOptions {
    /// The lifecycle action.
    pub action: ExecAction,
    /// A local fetch destination overriding `[exec].fetch_to`.
    pub fetch_to: Option<PathBuf>,
}

/// How an exec invocation ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecOutcome {
    /// The command ended with this remote exit code.
    Completed(i32),
    /// The operator interrupted the stream; the command and machine remain.
    Detached,
    /// The operator interrupted before the command started.
    Abandoned {
        /// Whether the standing machine was kept for a later invocation.
        kept: bool,
    },
    /// `--end` destroyed the standing machine.
    Ended,
    /// `--end` found no standing machine.
    NoInstance,
    /// An attached command reached a budget limit and was terminated.
    BudgetExhausted(Exhaustion),
}

/// The two output channels exec exposes to its caller.
pub trait ExecObserver {
    /// One line emitted by the remote command, without its trailing newline.
    fn command(&mut self, line: &str);
    /// One orchestration status line.
    fn narration(&mut self, line: &str);
    /// The machine adopted or acquired for this invocation.
    fn instance(&mut self, id: &str, rate_microusd_hour: u64, adopted: bool) {
        self.narration(&exec_instance_line(id, rate_microusd_hour, adopted));
    }
}

/// Formats the machine line shared by exec observers.
pub fn exec_instance_line(id: &str, rate_microusd_hour: u64, adopted: bool) -> String {
    format!(
        "{} instance {id} at ${:.6}/hr",
        if adopted { "adopted" } else { "acquired" },
        rate_microusd_hour as f64 / 1_000_000.0
    )
}

/// Runs one exec invocation from `config_path`.
pub fn exec(
    config_path: &Path,
    options: &ExecOptions,
    interrupt: &AtomicBool,
    observer: &mut dyn ExecObserver,
) -> Result<ExecOutcome> {
    run(load_exec(config_path)?, options, interrupt, observer)
}

fn run(
    config: ExecConfig,
    options: &ExecOptions,
    interrupt: &AtomicBool,
    observer: &mut dyn ExecObserver,
) -> Result<ExecOutcome> {
    let owner = owner_id(&config.host_name);
    let store = Store::open(&config.store)?;
    let lock = store.acquire_search_lock(&owner).map_err(|error| {
        Error::Validation(format!(
            "exec job for host {:?} is already active: {error}",
            config.host_name
        ))
    })?;
    let HostForm::Rented(spec) = &config.host.form else {
        unreachable!("load_exec restricts the host form")
    };
    let provider = provider_for(
        spec.provider.as_str(),
        &ProviderSettings {
            image: &spec.image,
            disk_gb: spec.disk_gb,
            env: Some(&spec.env),
            count: 1,
        },
    )?;
    let limits = AcquireLimits {
        usable_by: Instant::now() + spec.ready_timeout,
        ready_poll: spec.ready_poll,
    };
    let prior_record = store.instance_records()?.into_iter().find(|record| {
        record.owner == owner.to_string()
            && record.provider == provider.id()
            && record.role == Rental::Exec
            && record.instance().is_some()
    });
    let mut held = adopt(provider.as_ref(), &store, &lock, Rental::Exec, &limits)?;
    let adopted = held.is_some();
    if held.is_none() {
        match options.action {
            ExecAction::Attach => {
                return Err(Error::Provider(if let Some(record) = prior_record {
                    format!(
                        "the live ledger record for machine {:?}, instance {:?}, is gone or was destroyed",
                        record.machine,
                        record
                            .instance()
                            .expect("the matched record names an instance")
                    )
                } else {
                    "there is no standing instance for this job".to_string()
                }));
            }
            ExecAction::End => return Ok(ExecOutcome::NoInstance),
            ExecAction::Start { one_shot } => {
                if interrupt.load(Ordering::Relaxed) {
                    return Ok(ExecOutcome::Abandoned { kept: false });
                }
                let cancel = if one_shot {
                    interrupt
                } else {
                    never_cancelled()
                };
                held = match acquire(
                    provider.as_ref(),
                    &store,
                    &lock,
                    Rental::Exec,
                    &spec.constraints,
                    Objective::CheapestPerHour,
                    &limits,
                    &config.budget,
                    &Admission::new(),
                    cancel,
                    sima_provider::UNREPORTED,
                ) {
                    Ok(guard) => Some(guard),
                    Err(error)
                        if one_shot
                            && interrupt.load(Ordering::Relaxed)
                            && is_acquisition_cancelled(&error) =>
                    {
                        return Ok(ExecOutcome::Abandoned { kept: false });
                    }
                    Err(error) => return Err(error),
                };
            }
        }
    }
    let guard = held.expect("adopted or acquired");
    observer.instance(&guard.id().0, guard.rate().0, adopted);
    let reach = Reach::new(
        &transport_mode(provider.as_ref())?,
        &endpoint_target(guard.endpoint().clone()),
        &config.host.binary,
    );
    let far = RemoteExec::new(reach, &config.host.root, &owner);
    match contact_within(
        &far,
        limits.usable_by,
        limits.ready_poll,
        interrupt,
        observer,
    ) {
        Ok(Reached::Answered) => {}
        Ok(Reached::Interrupted) => {
            let kept = !matches!(options.action, ExecAction::Start { one_shot: true });
            let outcome = ExecOutcome::Abandoned { kept };
            let session = if kept {
                SessionOutcome::Keep(outcome)
            } else {
                SessionOutcome::Release(outcome)
            };
            return finish_guard(guard, options.action, Ok(session));
        }
        Err(error) if !adopted => {
            return Err(abandon_unreached(guard, &store, provider.id(), error));
        }
        Err(error) => return finish_guard(guard, options.action, Err(error)),
    }
    let fetch_to = options.fetch_to.as_ref().unwrap_or(&config.fetch_to);
    let session = match options.action {
        ExecAction::Attach => attach(&far, &store, &config, fetch_to, interrupt, observer),
        ExecAction::End => end(&far, &config, fetch_to, observer),
        ExecAction::Start { one_shot } => start(
            &far, &store, &config, fetch_to, one_shot, interrupt, observer,
        ),
    };
    finish_guard(guard, options.action, session)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reached {
    Answered,
    Interrupted,
}

/// Waits for the first contact with a machine under its readiness bounds.
fn contact_within(
    far: &dyn FarExec,
    deadline: Instant,
    poll: Duration,
    interrupt: &AtomicBool,
    observer: &mut dyn ExecObserver,
) -> Result<Reached> {
    let mut narrated = false;
    loop {
        match far.contact()? {
            Probe::Answered => return Ok(Reached::Answered),
            Probe::Unreachable(error) => {
                if Instant::now() >= deadline {
                    return Err(error);
                }
            }
        }
        if !narrated {
            observer.narration(&format!(
                "waiting for the machine to answer (up to {}s)",
                deadline.saturating_duration_since(Instant::now()).as_secs()
            ));
            narrated = true;
        }
        if interrupt.load(Ordering::Relaxed) {
            return Ok(Reached::Interrupted);
        }
        std::thread::sleep(poll);
    }
}

/// Records and releases a fresh rental that never answered its first probe.
fn abandon_unreached(
    guard: InstanceGuard<'_, dyn Provider + Sync>,
    store: &Store,
    provider_id: &str,
    error: Error,
) -> Error {
    let recorded = record_incident(
        store,
        provider_id,
        guard.machine(),
        guard.tag(),
        IncidentKind::ProbeFailed,
        now_ms(),
    );
    let released = guard.release();
    match (recorded.err(), released.err()) {
        (None, None) => error,
        (recording, release) => {
            let mut message = error.to_string();
            if let Some(recording) = recording {
                message.push_str(&format!(
                    "; recording the machine incident also failed: {recording}"
                ));
            }
            if let Some(release) = release {
                message.push_str(&format!(
                    "; releasing the machine also failed: {release}"
                ));
            }
            Error::Provider(message)
        }
    }
}

fn finish_guard(
    guard: InstanceGuard<'_, dyn Provider + Sync>,
    action: ExecAction,
    outcome: Result<SessionOutcome>,
) -> Result<ExecOutcome> {
    match outcome {
        Ok(SessionOutcome::Keep(value))
            if matches!(action, ExecAction::Start { one_shot: true })
                && !matches!(value, ExecOutcome::Detached) =>
        {
            guard.release()?;
            Ok(value)
        }
        Ok(SessionOutcome::Keep(value)) => {
            guard.keep();
            Ok(value)
        }
        Ok(SessionOutcome::Release(value)) => {
            guard.release()?;
            Ok(value)
        }
        Ok(SessionOutcome::KeepError(error)) => {
            guard.keep();
            Err(error)
        }
        Ok(SessionOutcome::ReleaseError(error)) => {
            guard.release()?;
            Err(error)
        }
        Err(error) => {
            match action {
                ExecAction::Attach | ExecAction::Start { one_shot: false } => guard.keep(),
                ExecAction::End | ExecAction::Start { one_shot: true } => guard.release()?,
            }
            Err(error)
        }
    }
}

#[derive(Debug)]
enum SessionOutcome {
    Keep(ExecOutcome),
    Release(ExecOutcome),
    KeepError(Error),
    ReleaseError(Error),
}

fn start(
    far: &dyn FarExec,
    store: &Store,
    config: &ExecConfig,
    fetch_to: &Path,
    one_shot: bool,
    interrupt: &AtomicBool,
    observer: &mut dyn ExecObserver,
) -> Result<SessionOutcome> {
    if matches!(far.state()?, RemoteState::Running(_)) {
        return Ok(SessionOutcome::KeepError(Error::Validation(
            "an exec command is already running; use --attach to watch it or --end to stop it"
                .to_string(),
        )));
    }
    if interrupt.load(Ordering::Relaxed) {
        return Ok(if one_shot {
            SessionOutcome::Release(ExecOutcome::Abandoned { kept: false })
        } else {
            SessionOutcome::Keep(ExecOutcome::Abandoned { kept: true })
        });
    }
    let HostForm::Rented(spec) = &config.host.form else {
        unreachable!()
    };
    if !far.binary_present()? {
        if !spec.bootstrap_sima {
            return Err(Error::Validation(format!(
                "the remote image has no {:?} binary; set bootstrap_sima = true on the rented host entry",
                config.host.binary
            )));
        }
        observer.narration("bootstrapping sima");
        far.bootstrap_sima()?;
    }
    observer.narration("delivering and installing payload");
    far.deliver(store, &ingest_exec(&config.payload, store)?)?;
    if interrupt.load(Ordering::Relaxed) {
        return Ok(if one_shot {
            SessionOutcome::Release(ExecOutcome::Abandoned { kept: false })
        } else {
            SessionOutcome::Keep(ExecOutcome::Abandoned { kept: true })
        });
    }
    far.start(&config.command)?;
    follow_to_outcome(far, store, config, fetch_to, interrupt, observer)
}

fn attach(
    far: &dyn FarExec,
    store: &Store,
    config: &ExecConfig,
    fetch_to: &Path,
    interrupt: &AtomicBool,
    observer: &mut dyn ExecObserver,
) -> Result<SessionOutcome> {
    match far.state()? {
        RemoteState::Running(_) => {
            follow_to_outcome(far, store, config, fetch_to, interrupt, observer)
        }
        RemoteState::Finished(code) => Err(Error::Validation(format!(
            "the exec command already finished with exit code {code}; its outputs remain remote until --end fetches them"
        ))),
        RemoteState::Idle => Err(Error::Validation(
            "the standing instance has no exec command to attach to".to_string(),
        )),
    }
}

fn end(
    far: &dyn FarExec,
    config: &ExecConfig,
    fetch_to: &Path,
    observer: &mut dyn ExecObserver,
) -> Result<SessionOutcome> {
    match far.state()? {
        RemoteState::Running(pid) => {
            observer.narration("ending remote command");
            far.kill(pid)?;
            far.wait_gone(pid)?;
        }
        RemoteState::Finished(_) => {}
        RemoteState::Idle => return Ok(SessionOutcome::Release(ExecOutcome::Ended)),
    }
    observer.narration("fetching outputs");
    match far.fetch(&config.outputs, fetch_to, observer) {
        Ok(()) => Ok(SessionOutcome::Release(ExecOutcome::Ended)),
        Err(error) => {
            observer.narration("fetch failed; instance kept with outputs remote");
            Ok(SessionOutcome::KeepError(error))
        }
    }
}

fn follow_to_outcome(
    far: &dyn FarExec,
    store: &Store,
    config: &ExecConfig,
    fetch_to: &Path,
    interrupt: &AtomicBool,
    observer: &mut dyn ExecObserver,
) -> Result<SessionOutcome> {
    let followed = match far.follow(
        store,
        owner_id(&config.host_name),
        &config.budget,
        interrupt,
        observer,
    ) {
        Ok(followed) => followed,
        Err(error) => {
            observer
                .narration("log stream failed; instance kept and the command may still be running");
            return Ok(SessionOutcome::KeepError(error));
        }
    };
    match followed {
        Followed::Detached => Ok(SessionOutcome::Keep(ExecOutcome::Detached)),
        Followed::Exhausted(exhaustion) => {
            let teardown = (|| {
                if let RemoteState::Running(pid) = far.state()? {
                    far.kill(pid)?;
                    far.wait_gone(pid)?;
                }
                observer.narration("fetching outputs");
                far.fetch(&config.outputs, fetch_to, observer)
            })();
            match teardown {
                Ok(()) => Ok(SessionOutcome::Release(ExecOutcome::BudgetExhausted(
                    exhaustion,
                ))),
                Err(error) => {
                    observer.narration("budget exhausted; instance release remains mandatory");
                    Ok(SessionOutcome::ReleaseError(error))
                }
            }
        }
        Followed::Completed(code) => {
            observer.narration("fetching outputs");
            match far.fetch(&config.outputs, fetch_to, observer) {
                Ok(()) => Ok(SessionOutcome::Keep(ExecOutcome::Completed(code))),
                Err(error) => {
                    observer.narration("fetch failed; instance kept with outputs remote");
                    Ok(SessionOutcome::KeepError(error))
                }
            }
        }
    }
}

fn owner_id(host: &str) -> SearchId {
    SearchId::from_hash(hash_bytes(format!("sima-exec:{host}").as_bytes()))
}

/// One remote path as a shell word, preserving the current user's `~/`
/// expansion while quoting every path component supplied by config.
fn remote_path_word(path: &str) -> String {
    path.strip_prefix("~/").map_or_else(
        || shell_quote(path),
        |under_home| format!("\"$HOME\"/{}", shell_quote(under_home)),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteState {
    Idle,
    Running(u32),
    Finished(i32),
}

enum Followed {
    Completed(i32),
    Detached,
    Exhausted(Exhaustion),
}

/// The remote operations the exec choreography depends on. The production
/// implementation speaks through a [`Reach`]; tests can drive the same state
/// transitions without a process or network.
trait FarExec {
    /// Whether the transport route answers before any session operation runs.
    fn contact(&self) -> Result<Probe>;
    /// The command state recorded in the remote job tree.
    fn state(&self) -> Result<RemoteState>;
    /// Whether the configured sima binary resolves on the remote host.
    fn binary_present(&self) -> Result<bool>;
    /// Uploads the static sima artifact.
    fn bootstrap_sima(&self) -> Result<()>;
    /// Delivers and installs one payload.
    fn deliver(&self, store: &Store, delivery: &ExecDelivery) -> Result<()>;
    /// Starts the opaque command detached.
    fn start(&self, command: &str) -> Result<u32>;
    /// Replays and follows the remote log through command completion.
    fn follow(
        &self,
        store: &Store,
        owner: SearchId,
        budget: &sima_provider::Budget,
        interrupt: &AtomicBool,
        observer: &mut dyn ExecObserver,
    ) -> Result<Followed>;
    /// Kills the detached command's process group.
    fn kill(&self, pid: u32) -> Result<()>;
    /// Waits until the detached command is absent.
    fn wait_gone(&self, pid: u32) -> Result<()>;
    /// Fetches declared outputs and the command log.
    fn fetch(
        &self,
        outputs: &[String],
        local: &Path,
        observer: &mut dyn ExecObserver,
    ) -> Result<()>;
}

/// Ends and reaps the local log transport while leaving the detached remote
/// command untouched. The reader owns the transport's stdout, so closing the
/// process precedes joining it.
fn stop_log_stream(
    child: &mut std::process::Child,
    receive: mpsc::Receiver<std::io::Result<String>>,
    reader: std::thread::JoinHandle<()>,
) {
    let _ = child.kill();
    let _ = child.wait();
    drop(receive);
    let _ = reader.join();
}

struct RemoteExec {
    reach: Reach,
    root: String,
}

impl RemoteExec {
    fn new(reach: Reach, host_root: &str, owner: &SearchId) -> RemoteExec {
        RemoteExec {
            reach,
            root: format!(
                "{}/exec/{}",
                host_root.trim_end_matches('/'),
                &owner.to_string()[..16]
            ),
        }
    }

    fn shell(&self, script: &str) -> Result<String> {
        let argv = self.reach.shell_argv();
        let (program, args) = argv.split_first().expect("shell argv");
        let mut child = own_process_group(&mut Command::new(program))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| Error::Transport(format!("cannot reach {}: {e}", self.reach.label())))?;
        child
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(script.as_bytes())
            .map_err(|e| Error::Transport(format!("cannot send remote command: {e}")))?;
        let output = child
            .wait_with_output()
            .map_err(|e| Error::Transport(format!("cannot reap remote command: {e}")))?;
        if !output.status.success() {
            return Err(Error::Transport(format!(
                "command on {} exited with {}",
                self.reach.label(),
                output.status
            )));
        }
        String::from_utf8(output.stdout)
            .map_err(|e| Error::Transport(format!("remote output is not UTF-8: {e}")))
    }
}

impl FarExec for RemoteExec {
    fn contact(&self) -> Result<Probe> {
        match &self.reach {
            Reach::Ssh { destination, .. } => {
                let mut argv = destination.prefix();
                argv.push("true".to_string());
                ssh_contact(&argv, &self.reach.label())
            }
            Reach::Here(_) => Ok(Probe::Answered),
        }
    }

    fn state(&self) -> Result<RemoteState> {
        let output = self.shell(&format!(
            "job={}\npid=$(cat \"$job/exec.pid\" 2>/dev/null || true)\nif [ -f \"$job/exec.status\" ]; then echo finished:$(cat \"$job/exec.status\"); elif [ -n \"$pid\" ] && kill -0 \"$pid\" 2>/dev/null; then echo running:$pid; else echo idle; fi\n",
            self.root
        ))?;
        let state = output.trim();
        if state == "idle" {
            return Ok(RemoteState::Idle);
        }
        if let Some(pid) = state.strip_prefix("running:") {
            return pid.parse().map(RemoteState::Running).map_err(|_| {
                Error::Transport(format!(
                    "{} reported invalid exec pid {pid:?}",
                    self.reach.label()
                ))
            });
        }
        if let Some(code) = state.strip_prefix("finished:") {
            return code.parse().map(RemoteState::Finished).map_err(|_| {
                Error::Transport(format!(
                    "{} reported invalid exec status {code:?}",
                    self.reach.label()
                ))
            });
        }
        Err(Error::Transport(format!(
            "{} reported invalid exec state {state:?}",
            self.reach.label()
        )))
    }

    fn binary_present(&self) -> Result<bool> {
        Ok(self
            .shell(&format!(
                "command -v {} >/dev/null 2>&1 && echo yes\nexit 0\n",
                remote_path_word(&self.reach.binary())
            ))?
            .trim()
            == "yes")
    }

    fn bootstrap_sima(&self) -> Result<()> {
        let executable = std::env::current_exe().map_err(|source| Error::Io {
            path: PathBuf::from("/proc/self/exe"),
            source,
        })?;
        let artifact = executable
            .parent()
            .unwrap_or(Path::new(""))
            .join("sima-static");
        if !artifact.is_file() {
            return Err(Error::Validation(format!(
                "bootstrap_sima expects {}; build it with scripts/build-sima-static.sh",
                artifact.display()
            )));
        }
        let binary = self.reach.binary();
        let target = if binary.contains('/') {
            binary
        } else {
            format!("/usr/local/bin/{binary}")
        };
        let script = format!(
            "set -e\ntarget={}\nmkdir -p \"$(dirname \"$target\")\"\ncat > \"$target\"\nchmod +x \"$target\"\n",
            remote_path_word(&target)
        );
        let argv = self.reach.shell_script_argv(&script);
        let (program, args) = argv.split_first().expect("shell argv");
        let mut child = own_process_group(&mut Command::new(program))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| Error::Transport(format!("cannot start sima bootstrap: {e}")))?;
        let mut input = child.stdin.take().expect("piped stdin");
        let mut file = std::fs::File::open(&artifact).map_err(|source| Error::Io {
            path: artifact.clone(),
            source,
        })?;
        std::io::copy(&mut file, &mut input)
            .map_err(|e| Error::Transport(format!("cannot upload {}: {e}", artifact.display())))?;
        drop(input);
        let status = child
            .wait()
            .map_err(|e| Error::Transport(format!("cannot reap sima bootstrap: {e}")))?;
        if !status.success() {
            return Err(Error::Transport(format!(
                "sima bootstrap exited with {status}"
            )));
        }
        Ok(())
    }

    fn deliver(&self, store: &Store, delivery: &ExecDelivery) -> Result<()> {
        let args = delivery.args(&self.root);
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        delivery.send(store, &self.reach.verb_argv(&refs))?;
        Ok(())
    }

    fn start(&self, command: &str) -> Result<u32> {
        let wrapper = format!(
            "cd payload || exit 1\nsh -c {}\nstatus=$?\necho $status > ../exec.status\nexit $status",
            shell_quote(command)
        );
        let output = self.shell(&format!(
            "set -e\njob={}\nmkdir -p \"$job/payload\"\nrm -f \"$job/exec.status\"\ncd \"$job\"\nsetsid nohup sh -c {} > exec.log 2>&1 < /dev/null &\npid=$!\necho $pid > exec.pid\necho $pid\n",
            self.root,
            shell_quote(&wrapper)
        ))?;
        output.trim().parse().map_err(|_| {
            Error::Transport(format!(
                "{} reported invalid started pid {:?}",
                self.reach.label(),
                output.trim()
            ))
        })
    }

    fn follow(
        &self,
        store: &Store,
        owner: SearchId,
        budget: &sima_provider::Budget,
        interrupt: &AtomicBool,
        observer: &mut dyn ExecObserver,
    ) -> Result<Followed> {
        let argv = self.reach.shell_argv();
        let (program, args) = argv.split_first().expect("shell argv");
        let mut child = own_process_group(&mut Command::new(program))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| {
                Error::Transport(format!("cannot attach to {}: {e}", self.reach.label()))
            })?;
        child
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(
                format!(
                    "job={}\ncd \"$job\" || exit 1\npid=$(cat exec.pid)\ntail -n +1 -f --pid=$pid exec.log\n",
                    self.root
                )
                .as_bytes(),
            )
            .map_err(|e| Error::Transport(format!("cannot start remote log stream: {e}")))?;
        let stdout = child.stdout.take().expect("piped stdout");
        let (send, receive) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        let mut assessed = Instant::now() - Duration::from_secs(10);
        loop {
            if interrupt.load(Ordering::Relaxed) {
                stop_log_stream(&mut child, receive, reader);
                return Ok(Followed::Detached);
            }
            if assessed.elapsed() >= Duration::from_secs(10) {
                assessed = Instant::now();
                match assess(store, &owner, budget, now_ms()) {
                    Ok(Verdict::Exhausted(exhaustion)) => {
                        stop_log_stream(&mut child, receive, reader);
                        return Ok(Followed::Exhausted(exhaustion));
                    }
                    Ok(Verdict::Within { .. }) => {}
                    Err(error) => {
                        stop_log_stream(&mut child, receive, reader);
                        return Err(error);
                    }
                }
            }
            match receive.recv_timeout(Duration::from_millis(100)) {
                Ok(Ok(line)) => observer.command(&line),
                Ok(Err(error)) => {
                    stop_log_stream(&mut child, receive, reader);
                    return Err(Error::Transport(format!(
                        "remote exec log is unreadable: {error}"
                    )));
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        reader
            .join()
            .map_err(|_| Error::Transport("remote exec log reader panicked".to_string()))?;
        let status = child
            .wait()
            .map_err(|e| Error::Transport(format!("cannot reap remote log stream: {e}")))?;
        if !status.success() {
            return Err(Error::Transport(format!(
                "remote log stream exited with {status}"
            )));
        }
        match self.state()? {
            RemoteState::Finished(code) => Ok(Followed::Completed(code)),
            state => Err(Error::Transport(format!(
                "remote command stream ended in state {state:?}"
            ))),
        }
    }

    fn kill(&self, pid: u32) -> Result<()> {
        self.shell(&format!(
            "kill -KILL -- -{pid} 2>/dev/null || kill -KILL {pid} 2>/dev/null || true\n"
        ))?;
        Ok(())
    }

    fn wait_gone(&self, pid: u32) -> Result<()> {
        self.shell(&format!(
            "while kill -0 {pid} 2>/dev/null; do sleep 0.1; done\n"
        ))?;
        Ok(())
    }

    fn fetch(
        &self,
        outputs: &[String],
        local: &Path,
        observer: &mut dyn ExecObserver,
    ) -> Result<()> {
        fetch_over(
            &self.reach.shell_argv(),
            &self.root,
            outputs,
            local,
            &mut |line| observer.narration(line),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::fs;
    use std::sync::{Mutex, PoisonError};

    use super::*;
    use crate::rental::fixtures::{acquisition_env, offer};
    use sima_provider::stub::StubProvider;
    use sima_provider::{Budget, Constraints, IncidentKind};
    use sima_store::{InstanceRecord, InstanceRecordState};

    #[derive(Default)]
    struct Recording {
        command: Vec<String>,
        narration: Vec<String>,
    }

    impl ExecObserver for Recording {
        fn command(&mut self, line: &str) {
            self.command.push(line.to_string());
        }

        fn narration(&mut self, line: &str) {
            self.narration.push(line.to_string());
        }
    }

    struct ChoreographyDouble {
        calls: RefCell<Vec<&'static str>>,
        state: Cell<RemoteState>,
        binary_present: bool,
        refusals: Cell<u32>,
    }

    impl ChoreographyDouble {
        fn idle() -> ChoreographyDouble {
            ChoreographyDouble {
                calls: RefCell::new(Vec::new()),
                state: Cell::new(RemoteState::Idle),
                binary_present: true,
                refusals: Cell::new(0),
            }
        }
    }

    impl FarExec for ChoreographyDouble {
        fn contact(&self) -> Result<Probe> {
            self.calls.borrow_mut().push("contact");
            let refusals = self.refusals.get();
            if refusals == 0 {
                return Ok(Probe::Answered);
            }
            self.refusals.set(refusals - 1);
            Ok(Probe::Unreachable(Error::Transport(
                "the choreography double is unreachable".to_string(),
            )))
        }

        fn state(&self) -> Result<RemoteState> {
            self.calls.borrow_mut().push("state");
            Ok(self.state.get())
        }

        fn binary_present(&self) -> Result<bool> {
            self.calls.borrow_mut().push("binary_present");
            Ok(self.binary_present)
        }

        fn bootstrap_sima(&self) -> Result<()> {
            self.calls.borrow_mut().push("bootstrap_sima");
            Ok(())
        }

        fn deliver(&self, _store: &Store, _delivery: &ExecDelivery) -> Result<()> {
            self.calls.borrow_mut().push("deliver");
            Ok(())
        }

        fn start(&self, _command: &str) -> Result<u32> {
            self.calls.borrow_mut().push("start");
            self.state.set(RemoteState::Running(42));
            Ok(42)
        }

        fn follow(
            &self,
            _store: &Store,
            _owner: SearchId,
            _budget: &sima_provider::Budget,
            _interrupt: &AtomicBool,
            observer: &mut dyn ExecObserver,
        ) -> Result<Followed> {
            self.calls.borrow_mut().push("follow");
            observer.command("remote line");
            self.state.set(RemoteState::Finished(7));
            Ok(Followed::Completed(7))
        }

        fn kill(&self, _pid: u32) -> Result<()> {
            self.calls.borrow_mut().push("kill");
            self.state.set(RemoteState::Finished(137));
            Ok(())
        }

        fn wait_gone(&self, _pid: u32) -> Result<()> {
            self.calls.borrow_mut().push("wait_gone");
            Ok(())
        }

        fn fetch(
            &self,
            _outputs: &[String],
            _local: &Path,
            _observer: &mut dyn ExecObserver,
        ) -> Result<()> {
            self.calls.borrow_mut().push("fetch");
            Ok(())
        }
    }

    fn local_far(dir: &Path) -> RemoteExec {
        RemoteExec::new(
            Reach::Here(PathBuf::from("/bin/sh")),
            dir.to_str().expect("utf8 path"),
            &owner_id("bench"),
        )
    }

    fn job_config(dir: &Path, command: &str) -> PathBuf {
        fs::create_dir(dir.join("payload")).expect("payload dir");
        fs::write(dir.join("payload/input.txt"), "input").expect("payload file");
        fs::write(dir.join("install.sh"), "#!/bin/sh\nexit 0\n").expect("install");
        let path = dir.join("job.toml");
        fs::write(
            &path,
            format!(
                r#"
                [exec]
                host = "bench"
                command = {command:?}
                payload = "payload"
                install = "install.sh"
                outputs = ["reports/*.txt"]

                [host.bench]
                provider = "stub"
                root = {:?}
                binary = {:?}
                "#,
                dir.join("remote").display().to_string(),
                crate::fixtures::built_sima().display().to_string(),
            ),
        )
        .expect("job config");
        path
    }

    fn bootstrap_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[test]
    fn exec_owner_depends_only_on_the_host_entry_name() {
        assert_eq!(owner_id("bench"), owner_id("bench"));
        assert_ne!(owner_id("bench"), owner_id("other"));
        assert_eq!(
            owner_id("bench").to_string(),
            SearchId::from_hash(hash_bytes(b"sima-exec:bench")).to_string()
        );
    }

    #[test]
    fn shell_quoting_preserves_an_opaque_command() {
        assert_eq!(
            shell_quote("printf '%s\\n' \"a b\""),
            "'printf '\\''%s\\n'\\'' \"a b\"'"
        );
    }

    #[test]
    fn remote_path_quoting_preserves_home_expansion_and_literal_components() {
        assert_eq!(
            remote_path_word("~/bin/sima special"),
            "\"$HOME\"/'bin/sima special'"
        );
        assert_eq!(remote_path_word("/opt/sima special"), "'/opt/sima special'");
    }

    #[test]
    fn binary_probe_quotes_a_path_with_spaces() -> Result<()> {
        let dir = tempfile::tempdir().expect("binary root");
        let bin = dir.path().join("bin space");
        fs::create_dir(&bin).expect("binary directory");
        let binary = bin.join("sima");
        std::os::unix::fs::symlink("/bin/true", &binary).expect("binary link");
        let far = RemoteExec::new(Reach::Here(binary), "unused", &owner_id("bench"));
        assert!(far.binary_present()?);
        Ok(())
    }

    #[test]
    fn instance_line_has_one_shared_format() {
        assert_eq!(
            exec_instance_line("instance-1", 123_456, false),
            "acquired instance instance-1 at $0.123456/hr"
        );
        assert_eq!(
            exec_instance_line("instance-1", 123_456, true),
            "adopted instance instance-1 at $0.123456/hr"
        );
    }

    #[test]
    fn remote_layout_is_stable_under_the_host_root() {
        let owner = owner_id("bench");
        let far = RemoteExec::new(Reach::Here(PathBuf::from("sima")), "~/sima/", &owner);
        assert_eq!(
            far.root,
            format!("~/sima/exec/{}", &owner.to_string()[..16])
        );
    }

    #[test]
    fn contact_waits_until_the_machine_answers() -> Result<()> {
        let far = ChoreographyDouble {
            refusals: Cell::new(2),
            ..ChoreographyDouble::idle()
        };
        let mut recording = Recording::default();
        let reached = contact_within(
            &far,
            Instant::now() + Duration::from_secs(10),
            Duration::ZERO,
            &AtomicBool::new(false),
            &mut recording,
        )?;
        assert_eq!(reached, Reached::Answered);
        assert_eq!(*far.calls.borrow(), ["contact", "contact", "contact"]);
        assert_eq!(recording.narration.len(), 1);
        assert!(
            recording.narration[0].contains("waiting for the machine to answer"),
            "{:?}",
            recording.narration
        );
        Ok(())
    }

    #[test]
    fn contact_returns_the_last_refusal_at_the_deadline() {
        let far = ChoreographyDouble {
            refusals: Cell::new(u32::MAX),
            ..ChoreographyDouble::idle()
        };
        let mut recording = Recording::default();
        let error = contact_within(
            &far,
            Instant::now(),
            Duration::ZERO,
            &AtomicBool::new(false),
            &mut recording,
        )
        .expect_err("the elapsed deadline ends the wait");
        assert!(
            error
                .to_string()
                .contains("choreography double is unreachable"),
            "{error}"
        );
        assert_eq!(*far.calls.borrow(), ["contact"]);
    }

    #[test]
    fn contact_reads_an_interrupt_after_the_first_refusal() -> Result<()> {
        let far = ChoreographyDouble {
            refusals: Cell::new(u32::MAX),
            ..ChoreographyDouble::idle()
        };
        let mut recording = Recording::default();
        let poll = Duration::from_secs(1);
        let started = Instant::now();
        let reached = contact_within(
            &far,
            Instant::now() + Duration::from_secs(10),
            poll,
            &AtomicBool::new(true),
            &mut recording,
        )?;
        assert_eq!(reached, Reached::Interrupted);
        assert_eq!(*far.calls.borrow(), ["contact"]);
        assert!(started.elapsed() < poll);
        Ok(())
    }

    #[test]
    fn contact_that_answers_immediately_emits_no_wait_narration() -> Result<()> {
        let far = ChoreographyDouble::idle();
        let mut recording = Recording::default();
        assert_eq!(
            contact_within(
                &far,
                Instant::now() + Duration::from_secs(10),
                Duration::ZERO,
                &AtomicBool::new(false),
                &mut recording,
            )?,
            Reached::Answered
        );
        assert!(recording.narration.is_empty());
        Ok(())
    }

    #[test]
    fn an_unreached_fresh_rental_is_recorded_and_released() -> Result<()> {
        let (_dir, store, owner) = acquisition_env();
        let provider = StubProvider::new(vec![offer("unreached", 100_000)]);
        let provider: &(dyn Provider + Sync) = &provider;
        let lock = store.acquire_search_lock(&owner)?;
        let guard = acquire(
            provider,
            &store,
            &lock,
            Rental::Exec,
            &Constraints::default(),
            Objective::CheapestPerHour,
            &AcquireLimits {
                usable_by: Instant::now() + Duration::from_secs(1),
                ready_poll: Duration::ZERO,
            },
            &Budget::default(),
            &Admission::new(),
            never_cancelled(),
            sima_provider::UNREPORTED,
        )?;
        let machine = guard.machine().to_string();
        let expected = Error::Transport("fresh rental never answered".to_string());
        let returned = abandon_unreached(guard, &store, provider.id(), expected);
        assert_eq!(
            returned.to_string(),
            "worker transport error: fresh rental never answered"
        );
        let incidents = store.machine_incidents()?;
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].machine, machine);
        assert_eq!(incidents[0].kind, IncidentKind::ProbeFailed);
        assert!(store.instance_records()?.is_empty());
        Ok(())
    }

    #[test]
    fn start_choreography_runs_through_the_far_side_boundary() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(dir.path(), "opaque command");
        let config = load_exec(&path)?;
        let store = Store::open(&config.store)?;
        let far = ChoreographyDouble::idle();
        let mut recording = Recording::default();
        assert_eq!(
            contact_within(
                &far,
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
                &AtomicBool::new(false),
                &mut recording,
            )?,
            Reached::Answered
        );
        let outcome = start(
            &far,
            &store,
            &config,
            &config.fetch_to,
            false,
            &AtomicBool::new(false),
            &mut recording,
        )?;
        assert!(matches!(
            outcome,
            SessionOutcome::Keep(ExecOutcome::Completed(7))
        ));
        assert_eq!(recording.command, ["remote line"]);
        assert_eq!(
            *far.calls.borrow(),
            [
                "contact",
                "state",
                "binary_present",
                "deliver",
                "start",
                "follow",
                "fetch",
            ]
        );
        Ok(())
    }

    #[test]
    fn a_missing_remote_binary_requires_the_explicit_bootstrap_key() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(dir.path(), "opaque command");
        let config = load_exec(&path)?;
        let store = Store::open(&config.store)?;
        let far = ChoreographyDouble {
            binary_present: false,
            ..ChoreographyDouble::idle()
        };
        let mut recording = Recording::default();
        assert_eq!(
            contact_within(
                &far,
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
                &AtomicBool::new(false),
                &mut recording,
            )?,
            Reached::Answered
        );
        let error = start(
            &far,
            &store,
            &config,
            &config.fetch_to,
            false,
            &AtomicBool::new(false),
            &mut recording,
        )
        .expect_err("bootstrap is explicit");
        assert!(error.to_string().contains("bootstrap_sima"), "{error}");
        assert_eq!(*far.calls.borrow(), ["contact", "state", "binary_present"]);
        Ok(())
    }

    #[test]
    fn a_finished_status_is_authoritative_over_a_reused_pid() -> Result<()> {
        let dir = tempfile::tempdir().expect("remote root");
        let far = local_far(dir.path());
        fs::create_dir_all(&far.root).expect("job tree");
        fs::write(
            Path::new(&far.root).join("exec.pid"),
            std::process::id().to_string(),
        )
        .expect("stale pid");
        fs::write(Path::new(&far.root).join("exec.status"), "23\n").expect("status");
        assert_eq!(far.state()?, RemoteState::Finished(23));
        Ok(())
    }

    #[test]
    fn detached_start_records_log_pid_and_nonzero_status() -> Result<()> {
        let dir = tempfile::tempdir().expect("remote root");
        let far = local_far(dir.path());
        far.start("printf 'first\\nsecond\\n'; exit 7")?;
        let deadline = Instant::now() + Duration::from_secs(2);
        let code = loop {
            match far.state()? {
                RemoteState::Finished(code) => break code,
                RemoteState::Running(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                state => panic!("command did not finish, state {state:?}"),
            }
        };
        assert_eq!(code, 7);
        let store_dir = tempfile::tempdir().expect("store");
        let store = Store::open(store_dir.path())?;
        let mut recording = Recording::default();
        let followed = far.follow(
            &store,
            owner_id("bench"),
            &sima_provider::Budget::default(),
            &AtomicBool::new(false),
            &mut recording,
        )?;
        assert!(matches!(followed, Followed::Completed(7)));
        assert_eq!(recording.command, ["first", "second"]);
        Ok(())
    }

    #[test]
    fn remote_kill_ends_the_detached_process_group() -> Result<()> {
        let dir = tempfile::tempdir().expect("remote root");
        let far = local_far(dir.path());
        let pid = far.start("sleep 30")?;
        assert_eq!(far.state()?, RemoteState::Running(pid));
        far.kill(pid)?;
        far.wait_gone(pid)?;
        assert!(!matches!(far.state()?, RemoteState::Running(_)));
        Ok(())
    }

    #[test]
    fn fresh_exec_runs_fetches_nonzero_outputs_and_keeps_by_default() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let config = job_config(
            dir.path(),
            "mkdir -p reports; echo result > reports/out.txt; echo streamed; exit 7",
        );
        let mut recording = Recording::default();
        let outcome = exec(
            &config,
            &ExecOptions {
                action: ExecAction::Start { one_shot: false },
                fetch_to: None,
            },
            &AtomicBool::new(false),
            &mut recording,
        )?;
        assert_eq!(outcome, ExecOutcome::Completed(7));
        assert_eq!(recording.command, ["streamed"]);
        assert_eq!(
            fs::read_to_string(dir.path().join("exec-outputs/reports/out.txt"))
                .expect("fetched output")
                .trim(),
            "result"
        );
        assert!(dir.path().join("exec-outputs/exec.log").is_file());
        let store = Store::open(dir.path().join(".sima/store"))?;
        assert_eq!(store.instance_records()?.len(), 1, "kept rental ledger");
        Ok(())
    }

    #[test]
    fn one_shot_exec_releases_and_writes_spend_after_fetch() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let config = job_config(dir.path(), "mkdir -p reports; echo done > reports/out.txt");
        let mut recording = Recording::default();
        let outcome = exec(
            &config,
            &ExecOptions {
                action: ExecAction::Start { one_shot: true },
                fetch_to: None,
            },
            &AtomicBool::new(false),
            &mut recording,
        )?;
        assert_eq!(outcome, ExecOutcome::Completed(0));
        let store = Store::open(dir.path().join(".sima/store"))?;
        assert!(store.instance_records()?.is_empty());
        assert_eq!(
            store.spend_entries(&owner_id("bench").to_string())?.len(),
            1
        );
        assert!(dir.path().join("exec-outputs/reports/out.txt").is_file());
        Ok(())
    }

    #[test]
    fn interrupt_before_acquisition_rents_nothing_in_either_mode() -> Result<()> {
        for one_shot in [false, true] {
            let dir = tempfile::tempdir().expect("job");
            let config = job_config(dir.path(), "echo must-not-run");
            let mut recording = Recording::default();
            assert_eq!(
                exec(
                    &config,
                    &ExecOptions {
                        action: ExecAction::Start { one_shot },
                        fetch_to: None,
                    },
                    &AtomicBool::new(true),
                    &mut recording,
                )?,
                ExecOutcome::Abandoned { kept: false }
            );
            let store = Store::open(dir.path().join(".sima/store"))?;
            assert!(store.instance_records()?.is_empty());
        }
        Ok(())
    }

    #[test]
    fn one_shot_keeps_the_machine_when_fetch_fails() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let config = job_config(dir.path(), "mkdir -p reports; echo done > reports/out.txt");
        let blocked = dir.path().join("blocked-fetch");
        fs::write(&blocked, "a file cannot be an output directory").expect("block fetch");
        let mut recording = Recording::default();
        let error = exec(
            &config,
            &ExecOptions {
                action: ExecAction::Start { one_shot: true },
                fetch_to: Some(blocked),
            },
            &AtomicBool::new(false),
            &mut recording,
        )
        .expect_err("fetch failure");
        assert!(error.to_string().contains("blocked-fetch"), "{error}");
        let store = Store::open(dir.path().join(".sima/store"))?;
        assert_eq!(store.instance_records()?.len(), 1);
        assert!(
            store
                .spend_entries(&owner_id("bench").to_string())?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn a_finished_machine_runs_a_fresh_cycle_without_reinstalling_the_same_payload() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(
            dir.path(),
            "count=$(cat cycles 2>/dev/null || echo 0); expr $count + 1 > cycles; mkdir -p reports; cp cycles reports/out.txt",
        );
        fs::write(dir.path().join("install.sh"), "printf x >> install-count\n")
            .expect("count installs");
        let config = load_exec(&path)?;
        let far = RemoteExec::new(
            Reach::Here(crate::fixtures::built_sima()),
            &config.host.root,
            &owner_id("bench"),
        );
        let store = Store::open(&config.store)?;
        let mut recording = Recording::default();
        for _ in 0..2 {
            assert!(matches!(
                start(
                    &far,
                    &store,
                    &config,
                    &config.fetch_to,
                    false,
                    &AtomicBool::new(false),
                    &mut recording,
                )?,
                SessionOutcome::Keep(ExecOutcome::Completed(0))
            ));
        }
        let payload = Path::new(&far.root).join("payload");
        assert_eq!(
            fs::read_to_string(payload.join("cycles")).expect("cycle count"),
            "2\n"
        );
        assert_eq!(
            fs::read_to_string(payload.join("install-count")).expect("install count"),
            "x"
        );
        Ok(())
    }

    #[test]
    fn attach_and_end_without_a_standing_instance_are_disjoint() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let config = job_config(dir.path(), "true");
        let mut recording = Recording::default();
        let error = exec(
            &config,
            &ExecOptions {
                action: ExecAction::Attach,
                fetch_to: None,
            },
            &AtomicBool::new(false),
            &mut recording,
        )
        .expect_err("attach never rents");
        assert!(
            error.to_string().contains("no standing instance"),
            "{error}"
        );
        assert_eq!(
            exec(
                &config,
                &ExecOptions {
                    action: ExecAction::End,
                    fetch_to: None,
                },
                &AtomicBool::new(false),
                &mut recording,
            )?,
            ExecOutcome::NoInstance
        );
        Ok(())
    }

    #[test]
    fn attach_reports_a_ledger_machine_the_provider_no_longer_holds() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let config = job_config(dir.path(), "true");
        let store = Store::open(dir.path().join(".sima/store"))?;
        store.put_instance(&InstanceRecord {
            role: Rental::Exec,
            tag: "sima-exec-gone".to_string(),
            provider: "stub".to_string(),
            machine: "stub-machine-gone".to_string(),
            owner: owner_id("bench").to_string(),
            state: InstanceRecordState::Live {
                instance: "stub-instance-gone".to_string(),
            },
            price_micro_usd_hour: 100_000,
            created_ms: 1_700_000_000_000,
        })?;
        let mut recording = Recording::default();
        let error = exec(
            &config,
            &ExecOptions {
                action: ExecAction::Attach,
                fetch_to: None,
            },
            &AtomicBool::new(false),
            &mut recording,
        )
        .expect_err("gone machine");
        let message = error.to_string();
        assert!(message.contains("live ledger record"), "{message}");
        assert!(
            message.contains("gone") || message.contains("destroyed"),
            "{message}"
        );
        assert!(message.contains("stub-machine-gone"), "{message}");
        assert!(message.contains("stub-instance-gone"), "{message}");
        assert!(store.instance_records()?.is_empty());
        Ok(())
    }

    #[test]
    fn attach_to_a_finished_command_reports_its_status() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(dir.path(), "exit 23");
        let config = load_exec(&path)?;
        let far = local_far(&PathBuf::from(&config.host.root));
        far.start("exit 23")?;
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(far.state()?, RemoteState::Running(_)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let store = Store::open(&config.store)?;
        let mut recording = Recording::default();
        let error = attach(
            &far,
            &store,
            &config,
            &config.fetch_to,
            &AtomicBool::new(false),
            &mut recording,
        )
        .expect_err("finished command is not attachable");
        assert!(error.to_string().contains("exit code 23"), "{error}");
        Ok(())
    }

    #[test]
    fn end_requests_keep_when_fetch_fails() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(dir.path(), "echo output");
        let config = load_exec(&path)?;
        let far = local_far(&PathBuf::from(&config.host.root));
        far.start("echo output")?;
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(far.state()?, RemoteState::Running(_)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let blocked = dir.path().join("blocked-fetch");
        fs::write(&blocked, "file").expect("block fetch");
        let mut recording = Recording::default();
        assert!(matches!(
            end(&far, &config, &blocked, &mut recording)?,
            SessionOutcome::KeepError(_)
        ));
        Ok(())
    }

    #[test]
    fn end_kills_a_running_command_fetches_and_requests_release() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(dir.path(), "sleep 30");
        let config = load_exec(&path)?;
        let far = local_far(&PathBuf::from(&config.host.root));
        far.start("mkdir -p reports; echo partial > reports/out.txt; sleep 30")?;
        let remote_output = Path::new(&far.root).join("payload/reports/out.txt");
        let deadline = Instant::now() + Duration::from_secs(2);
        while !remote_output.is_file() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            remote_output.is_file(),
            "the command produced its partial output"
        );
        let mut recording = Recording::default();
        assert!(matches!(
            end(&far, &config, &config.fetch_to, &mut recording)?,
            SessionOutcome::Release(ExecOutcome::Ended)
        ));
        assert!(!matches!(far.state()?, RemoteState::Running(_)));
        assert_eq!(
            fs::read_to_string(config.fetch_to.join("reports/out.txt")).expect("fetched output"),
            "partial\n"
        );
        assert!(config.fetch_to.join("exec.log").is_file());
        Ok(())
    }

    #[test]
    fn every_start_refuses_a_running_command_and_keeps_the_instance() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(dir.path(), "sleep 30");
        let config = load_exec(&path)?;
        let far = local_far(&PathBuf::from(&config.host.root));
        let pid = far.start("echo begun; sleep 30")?;
        let store = Store::open(&config.store)?;
        let mut recording = Recording::default();
        for one_shot in [false, true] {
            let outcome = start(
                &far,
                &store,
                &config,
                &config.fetch_to,
                one_shot,
                &AtomicBool::new(false),
                &mut recording,
            )?;
            let SessionOutcome::KeepError(error) = outcome else {
                panic!("a running command must keep its instance");
            };
            let message = error.to_string();
            assert!(
                message.contains("--attach") && message.contains("--end"),
                "{message}"
            );
        }
        let outcome = attach(
            &far,
            &store,
            &config,
            &config.fetch_to,
            &AtomicBool::new(true),
            &mut recording,
        )?;
        assert!(matches!(
            outcome,
            SessionOutcome::Keep(ExecOutcome::Detached)
        ));
        assert_eq!(far.state()?, RemoteState::Running(pid));
        far.kill(pid)?;
        far.wait_gone(pid)?;
        Ok(())
    }

    #[test]
    fn interrupt_before_start_abandons_with_the_requested_machine_disposition() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(dir.path(), "echo must-not-run");
        let config = load_exec(&path)?;
        let far = local_far(&PathBuf::from(&config.host.root));
        let store = Store::open(&config.store)?;
        let mut recording = Recording::default();

        assert!(matches!(
            start(
                &far,
                &store,
                &config,
                &config.fetch_to,
                false,
                &AtomicBool::new(true),
                &mut recording,
            )?,
            SessionOutcome::Keep(ExecOutcome::Abandoned { kept: true })
        ));
        assert!(matches!(far.state()?, RemoteState::Idle));
        assert!(matches!(
            start(
                &far,
                &store,
                &config,
                &config.fetch_to,
                true,
                &AtomicBool::new(true),
                &mut recording,
            )?,
            SessionOutcome::Release(ExecOutcome::Abandoned { kept: false })
        ));
        assert!(matches!(far.state()?, RemoteState::Idle));
        Ok(())
    }

    #[test]
    fn budget_exhaustion_kills_fetches_and_requests_release() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(dir.path(), "sleep 30");
        let mut config = load_exec(&path)?;
        config.budget.max_spend = Some(sima_provider::Cost(0));
        let far = local_far(&PathBuf::from(&config.host.root));
        far.start("echo budget-log; sleep 30")?;
        let store = Store::open(&config.store)?;
        let mut recording = Recording::default();
        let outcome = follow_to_outcome(
            &far,
            &store,
            &config,
            &config.fetch_to,
            &AtomicBool::new(false),
            &mut recording,
        )?;
        assert!(matches!(
            outcome,
            SessionOutcome::Release(ExecOutcome::BudgetExhausted(_))
        ));
        assert!(!matches!(far.state()?, RemoteState::Running(_)));
        assert!(config.fetch_to.join("exec.log").is_file());
        Ok(())
    }

    #[test]
    fn budget_exhaustion_requests_release_when_fetch_fails() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(dir.path(), "sleep 30");
        let mut config = load_exec(&path)?;
        config.budget.max_spend = Some(sima_provider::Cost(0));
        let far = local_far(&PathBuf::from(&config.host.root));
        far.start("sleep 30")?;
        let store = Store::open(&config.store)?;
        let blocked = dir.path().join("blocked-fetch");
        fs::write(&blocked, "file").expect("block fetch");
        let mut recording = Recording::default();
        assert!(matches!(
            follow_to_outcome(
                &far,
                &store,
                &config,
                &blocked,
                &AtomicBool::new(false),
                &mut recording,
            )?,
            SessionOutcome::ReleaseError(_)
        ));
        assert!(!matches!(far.state()?, RemoteState::Running(_)));
        Ok(())
    }

    #[test]
    fn a_second_exec_is_refused_by_the_owner_lock_naming_its_holder() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let config = job_config(dir.path(), "true");
        let store = Store::open(dir.path().join(".sima/store"))?;
        let _held = store.acquire_search_lock(&owner_id("bench"))?;
        let mut recording = Recording::default();
        let error = exec(
            &config,
            &ExecOptions {
                action: ExecAction::Start { one_shot: false },
                fetch_to: None,
            },
            &AtomicBool::new(false),
            &mut recording,
        )
        .expect_err("lock contention");
        let message = error.to_string();
        assert!(
            message.contains("already active") && message.contains("locked"),
            "{message}"
        );
        Ok(())
    }

    #[test]
    fn bootstrap_uploads_once_and_a_missing_artifact_names_the_build() -> Result<()> {
        let _serial = bootstrap_guard();
        let dir = tempfile::tempdir().expect("job");
        let config = job_config(dir.path(), "mkdir -p reports; echo ok > reports/out.txt");
        let original_binary = crate::fixtures::built_sima();
        let remote_binary = dir.path().join("remote-bin/sima");
        let text = fs::read_to_string(&config).expect("config").replace(
            &format!("binary = {:?}", original_binary.display().to_string()),
            &format!(
                "binary = {:?}\n                bootstrap_sima = true",
                remote_binary.display().to_string()
            ),
        );
        fs::write(&config, text).expect("bootstrap config");
        let test_exe = std::env::current_exe().expect("test executable");
        let artifact = test_exe
            .parent()
            .expect("test binary dir")
            .join("sima-static");
        let created = !artifact.exists();
        if created {
            fs::copy(&original_binary, &artifact).expect("place static fixture");
        }
        let options = ExecOptions {
            action: ExecAction::Start { one_shot: true },
            fetch_to: None,
        };
        let mut recording = Recording::default();
        assert_eq!(
            exec(&config, &options, &AtomicBool::new(false), &mut recording)?,
            ExecOutcome::Completed(0)
        );
        assert!(remote_binary.is_file());
        assert!(
            recording
                .narration
                .iter()
                .any(|line| line.contains("bootstrapping"))
        );
        if created {
            fs::remove_file(&artifact).expect("remove static fixture");
        }

        // The installed binary is the machine's stamp: the next fresh rental
        // over this local stub path probes it and needs no local artifact.
        recording = Recording::default();
        assert_eq!(
            exec(&config, &options, &AtomicBool::new(false), &mut recording)?,
            ExecOutcome::Completed(0)
        );
        assert!(
            !recording
                .narration
                .iter()
                .any(|line| line.contains("bootstrapping"))
        );

        if created {
            let absent_binary = dir.path().join("another-bin/sima");
            let text = fs::read_to_string(&config).expect("config").replace(
                &remote_binary.display().to_string(),
                &absent_binary.display().to_string(),
            );
            fs::write(&config, text).expect("absent binary config");
            let error = exec(&config, &options, &AtomicBool::new(false), &mut recording)
                .expect_err("missing static artifact");
            let message = error.to_string();
            assert!(message.contains("sima-static"), "{message}");
            assert!(
                message.contains("scripts/build-sima-static.sh"),
                "{message}"
            );
        }
        Ok(())
    }
}
