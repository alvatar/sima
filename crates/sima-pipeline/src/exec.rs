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
    AcquireLimits, Admission, Exhaustion, InstanceGuard, Objective, Provider, Verdict, acquire,
    adopt, assess, now_ms,
};
use sima_store::{Rental, Store};

use crate::config::{ExecConfig, HostForm, load_exec};
use crate::fetch::{fetch_over, shell_quote};
use crate::migrate::sync::Reach;
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
    let had_record = store.instance_records()?.into_iter().any(|record| {
        record.owner == owner.to_string()
            && record.provider == provider.id()
            && record.role == Rental::Exec
            && record.instance().is_some()
    });
    let mut held = adopt(provider.as_ref(), &store, &lock, Rental::Exec, &limits)?;
    if held.is_none() {
        match options.action {
            ExecAction::Attach => {
                return Err(Error::Provider(
                    if had_record {
                        "the ledger machine is gone or was destroyed"
                    } else {
                        "there is no standing instance for this job"
                    }
                    .to_string(),
                ));
            }
            ExecAction::End => return Ok(ExecOutcome::NoInstance),
            ExecAction::Start { .. } => {
                held = Some(acquire(
                    provider.as_ref(),
                    &store,
                    &lock,
                    Rental::Exec,
                    &spec.constraints,
                    Objective::CheapestPerHour,
                    &limits,
                    &config.budget,
                    &Admission::new(),
                    interrupt,
                    sima_provider::UNREPORTED,
                )?);
            }
        }
    }
    let guard = held.expect("adopted or acquired");
    observer.narration(&format!(
        "instance {} at ${:.6}/hr",
        guard.id().0,
        guard.rate().0 as f64 / 1_000_000.0
    ));
    let reach = Reach::new(
        &transport_mode(provider.as_ref())?,
        &endpoint_target(guard.endpoint().clone()),
        &config.host.binary,
    );
    let far = RemoteExec::new(reach, &config.host.root, &owner);
    let fetch_to = options.fetch_to.as_ref().unwrap_or(&config.fetch_to);
    if let ExecAction::Start { one_shot } = options.action {
        match far.state() {
            Ok(RemoteState::Running(_)) => {
                guard.keep();
                return Err(Error::Validation(
                    "an exec command is already running; use --attach to watch it or --end to stop it"
                        .to_string(),
                ));
            }
            Ok(_) => {}
            Err(error) => {
                if one_shot {
                    guard.release()?;
                } else {
                    guard.keep();
                }
                return Err(error);
            }
        }
    }
    let session = match options.action {
        ExecAction::Attach => attach(&far, &store, &config, fetch_to, interrupt, observer),
        ExecAction::End => end(&far, &config, fetch_to, observer),
        ExecAction::Start { one_shot } => start(
            &far, &store, &config, fetch_to, one_shot, interrupt, observer,
        ),
    };
    finish_guard(guard, options.action, session)
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
}

fn start(
    far: &RemoteExec,
    store: &Store,
    config: &ExecConfig,
    fetch_to: &Path,
    one_shot: bool,
    interrupt: &AtomicBool,
    observer: &mut dyn ExecObserver,
) -> Result<SessionOutcome> {
    if matches!(far.state()?, RemoteState::Running(_)) {
        return Err(Error::Validation(
            "an exec command is already running; use --attach to watch it or --end to stop it"
                .to_string(),
        ));
    }
    if interrupt.load(Ordering::Relaxed) {
        return Ok(if one_shot {
            SessionOutcome::Release(ExecOutcome::Detached)
        } else {
            SessionOutcome::Keep(ExecOutcome::Detached)
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
    observer.narration("delivering payload");
    far.deliver(store, &ingest_exec(&config.payload, store)?)?;
    if interrupt.load(Ordering::Relaxed) {
        return Ok(if one_shot {
            SessionOutcome::Release(ExecOutcome::Detached)
        } else {
            SessionOutcome::Keep(ExecOutcome::Detached)
        });
    }
    far.start(&config.command)?;
    follow_to_outcome(far, store, config, fetch_to, interrupt, observer)
}

fn attach(
    far: &RemoteExec,
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
    far: &RemoteExec,
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
    far.fetch(&config.outputs, fetch_to)?;
    Ok(SessionOutcome::Release(ExecOutcome::Ended))
}

fn follow_to_outcome(
    far: &RemoteExec,
    store: &Store,
    config: &ExecConfig,
    fetch_to: &Path,
    interrupt: &AtomicBool,
    observer: &mut dyn ExecObserver,
) -> Result<SessionOutcome> {
    match far.follow(
        store,
        owner_id(&config.host_name),
        &config.budget,
        interrupt,
        observer,
    )? {
        Followed::Detached => Ok(SessionOutcome::Keep(ExecOutcome::Detached)),
        Followed::Exhausted(exhaustion) => {
            if let RemoteState::Running(pid) = far.state()? {
                far.kill(pid)?;
                far.wait_gone(pid)?;
            }
            observer.narration("fetching outputs");
            far.fetch(&config.outputs, fetch_to)?;
            Ok(SessionOutcome::Release(ExecOutcome::BudgetExhausted(
                exhaustion,
            )))
        }
        Followed::Completed(code) => {
            observer.narration("fetching outputs");
            far.fetch(&config.outputs, fetch_to)?;
            Ok(SessionOutcome::Keep(ExecOutcome::Completed(code)))
        }
    }
}

fn owner_id(host: &str) -> SearchId {
    SearchId::from_hash(hash_bytes(format!("sima-exec:{host}").as_bytes()))
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

    fn state(&self) -> Result<RemoteState> {
        let output = self.shell(&format!(
            "job={}\npid=$(cat \"$job/exec.pid\" 2>/dev/null || true)\nif [ -n \"$pid\" ] && kill -0 \"$pid\" 2>/dev/null; then echo running:$pid; elif [ -f \"$job/exec.status\" ]; then echo finished:$(cat \"$job/exec.status\"); else echo idle; fi\n",
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
                self.reach.binary()
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
                "bootstrap_sima expects {} built with: cargo build --release --target x86_64-unknown-linux-musl -p sima",
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
            target
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
                let _ = child.kill();
                let _ = child.wait();
                drop(receive);
                let _ = reader.join();
                return Ok(Followed::Detached);
            }
            if assessed.elapsed() >= Duration::from_secs(10) {
                assessed = Instant::now();
                if let Verdict::Exhausted(exhaustion) = assess(store, &owner, budget, now_ms())? {
                    let _ = child.kill();
                    let _ = child.wait();
                    drop(receive);
                    let _ = reader.join();
                    return Ok(Followed::Exhausted(exhaustion));
                }
            }
            match receive.recv_timeout(Duration::from_millis(100)) {
                Ok(Ok(line)) => observer.command(&line),
                Ok(Err(error)) => {
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

    fn fetch(&self, outputs: &[String], local: &Path) -> Result<()> {
        fetch_over(&self.reach.shell_argv(), &self.root, outputs, local)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Mutex, PoisonError};

    use super::*;

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
    fn remote_layout_is_stable_under_the_host_root() {
        let owner = owner_id("bench");
        let far = RemoteExec::new(Reach::Here(PathBuf::from("sima")), "~/sima/", &owner);
        assert_eq!(
            far.root,
            format!("~/sima/exec/{}", &owner.to_string()[..16])
        );
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
    fn plain_start_refuses_a_running_command_and_interrupt_detaches() -> Result<()> {
        let dir = tempfile::tempdir().expect("job");
        let path = job_config(dir.path(), "sleep 30");
        let config = load_exec(&path)?;
        let far = local_far(&PathBuf::from(&config.host.root));
        let pid = far.start("echo begun; sleep 30")?;
        let store = Store::open(&config.store)?;
        let mut recording = Recording::default();
        let error = start(
            &far,
            &store,
            &config,
            &config.fetch_to,
            false,
            &AtomicBool::new(false),
            &mut recording,
        )
        .expect_err("plain invocation refuses a running command");
        let message = error.to_string();
        assert!(
            message.contains("--attach") && message.contains("--end"),
            "{message}"
        );
        let outcome = follow_to_outcome(
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
            assert!(message.contains("x86_64-unknown-linux-musl"), "{message}");
        }
        Ok(())
    }
}
