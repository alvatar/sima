//! `sima` command-line binary. `search` drives a config to its outcome with
//! live progress and graceful Ctrl-C; the query commands read the search's
//! journal along two axes — what the command reports, and how much of the
//! search it covers:
//!
//! - `status` reports execution: the search's state and counters, one task's
//!   attempt timeline under `--task <key>`, or the tasks that did not commit
//!   under `--failed`.
//! - `report` reports results, efficiency, and cost: the committed stats,
//!   grouped by default, one line per task under `--all`, or one task's under
//!   `--task <key>`; the search's throughput, retry rates, and per-worker
//!   utilization over a chart of commits and worker occupancy under
//!   `--timeline`; the rental ledger under `--spend`; and machine reputation
//!   under `--machines`. The view flags are mutually exclusive.
//!
//! A `<key>` is any prefix of a task key that names one task.
//!
//! A search executes on the machines the invocation asks for, never on every
//! machine a config happens to declare: `search` and `tui` use `[orchestrator]`
//! alone, and `--fleet` adds every member of `[fleet]`. Without it no provider
//! is constructed and no rental credential is read. `migrate` moves the whole
//! search — its store and its orchestrator — onto the one machine
//! `[orchestrator].migrate` names, and brings the results back.
//! `exec` is the separate command contract: one opaque `[exec].command` on
//! one rented `[host.*]`, with its log and declared output files fetched home.
//! It uses the store only for rental accounting and payload objects.
//!
//! A search through a program a `[domain.*]` entry names stops when that
//! program's build changed since the search last ran; `--accept-binary` is the
//! invocation stating that the changed build should drive it anyway.
//!
//! `search`, `exec`, `migrate`, and `recall` render a live stream. On a search
//! command, `--quiet` narrows that to the search's own progress and errors. On
//! an exec, it leaves only the remote command's lines. The lines it drops state
//! orchestration progress for an operator watching the placement.
//!
//! `sdk` writes the SDK this binary carries into a directory, which is how a
//! program is developed against the package the searches that spawn it vend.
//!
//! All orchestration lives in `sima-pipeline` — this binary parses arguments,
//! renders output, registers the interrupt flag, and maps outcomes to exit
//! codes:
//!
//! - 0 — the search finalized, a query answered, or an exec detached;
//! - 2 — a definitive candidate failure;
//! - 130 — interrupted by Ctrl-C, store resumable;
//! - 1 — everything else: infrastructure fault, config error, usage error, and
//!   a `migrate` that came home with tasks outstanding.
//!
//! A completed exec instead returns its remote command's exit code verbatim;
//! code 1 can therefore mean either the command or sima failed, and the
//! diagnostic text distinguishes them.

mod follow;
mod migrate;
mod pack;
mod reconcile;
mod render;
mod tui;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use sima_core::{Error, Hash, Result};
use sima_pipeline::{
    BinaryChange, Engagement, ExecAction, ExecObserver, ExecOptions, ExecOutcome, FeedInfo,
    LoadedConfig, LocalFeed, Record, RemoteFeed, RemovalReport, ReportRow, Sdk, SearchControl,
    SearchFeed, SearchId, SearchOutcome, SearchState, SearchStatus, SearchTimeline, TaskHistory,
    exec, failures_records, follow_serve, load, load_exec, local_snapshot, orchestrate,
    receive_exec_payload, receive_program, remote_snapshot, report_records, report_task_records,
    seeded_status, status_records, sync_serve, task_history_records, timeline_records,
};
use sima_provider::ReconcileScope;

use crate::render::Narration;

/// Exit code for a definitive candidate failure.
pub(crate) const EXIT_FAILED: u8 = 2;
/// Exit code for a search wound down by an interrupt, matching the shell
/// convention for death by SIGINT.
pub(crate) const EXIT_INTERRUPTED: u8 = 130;
/// Exit code for everything else that is not success: infrastructure
/// fault, config error, usage error.
pub(crate) const EXIT_ERROR: u8 = 1;

/// Loose objects past which the binary recommends packing. One file per
/// object costs one inode each, and six figures of them is where that
/// starts to press on the filesystem rather than merely occupy it.
const LOOSE_OBJECT_WARN_THRESHOLD: u64 = 100_000;

/// The verbs that open a local store to read or to drive, and so are where
/// a recommendation to pack it belongs. `tui` is excluded because its
/// alternate screen swallows stderr, and `pack` because it is the answer.
const STORE_OPENING_VERBS: [&str; 7] = [
    "search", "exec", "status", "report", "migrate", "recall", "rm",
];

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (args, host) = split_target(&args);
    let (args, accept) = split_binary_change(&args);
    let (args, narration) = split_quiet(&args);
    // A store on the far side of `--on` is not this machine's to pack, so
    // only a local invocation is measured.
    if host.is_none()
        && let [verb, config, ..] = args[..]
        && STORE_OPENING_VERBS.contains(&verb)
    {
        warn_on_loose_objects(&resolve_config(config), verb);
    }
    if host.is_none()
        && let Some((config, action, fetch_to)) = exec_form(&args)
    {
        return exec_command(
            &resolve_config(config),
            action,
            fetch_to.map(Path::new),
            narration,
        );
    }
    match args[..] {
        // The write commands never observe: `search` drives a search, which happens
        // where the hardware is, and `rm` and `reconcile` mutate a store. A
        // host on any of them falls through to the usage error.
        ["search", config] if host.is_none() => search_command(
            &resolve_config(config),
            Engagement::Orchestrator,
            accept,
            narration,
        ),
        ["search", config, "--fleet"] if host.is_none() => search_command(
            &resolve_config(config),
            Engagement::Fleet,
            accept,
            narration,
        ),
        ["migrate", config] if host.is_none() => {
            migrate::migrate_command(&resolve_config(config), accept, narration)
        }
        // The inverse of `migrate`, and the only thing that ends a far search. It
        // takes no `--accept-binary`: that answers a comparison only a start
        // makes, and a recall starts nothing, so the flag stays among the
        // arguments and falls to the usage error.
        ["recall", config] if host.is_none() => {
            migrate::recall_command(&resolve_config(config), narration)
        }
        ["rm", config] if host.is_none() => rm_command(&resolve_config(config)),
        // A store outlives the identity that filled it, so a search the config no
        // longer names is addressed by its own id instead.
        ["rm", config, "--search", prefix] if host.is_none() => {
            rm_matching_command(&resolve_config(config), prefix)
        }
        // The one read command whose argument is a store directory: what a
        // store holds is every search ever driven against it, and a config names
        // one of them.
        ["searches", store] if host.is_none() => searches_command(Path::new(store)),
        // The only verb whose argument is a store directory rather than a
        // config: packing needs no search knowledge, and a store defines every
        // search it holds.
        ["pack", store] if host.is_none() => pack::pack_command(Path::new(store), false),
        ["pack", store, "--gc"] if host.is_none() => pack::pack_command(Path::new(store), true),
        ["reconcile", config] if host.is_none() => {
            reconcile::reconcile_command(&resolve_config(config), ReconcileScope::Workers)
        }
        ["reconcile", config, "--hosted"] if host.is_none() => {
            reconcile::reconcile_command(&resolve_config(config), ReconcileScope::Hosted)
        }
        // The far half of the follow transport, invoked over ssh by another
        // machine's read command. It is not a user-facing verb.
        ["follow-serve", config] if host.is_none() => serve_command(config, false),
        ["follow-serve", config, "--once"] if host.is_none() => serve_command(config, true),
        // The far half of a store sync, invoked over ssh by a migration. Not a
        // user-facing verb either.
        ["sync-serve", store, "--search", search] if host.is_none() => {
            sync_serve_command(Path::new(store), search)
        }
        // The far half of a program delivery, invoked by a search putting work on
        // this machine. The same verb, because a delivery is a store sync plus
        // an install; the arguments are what tell the two forms apart.
        ["sync-serve", dir, "--payload", payload] if host.is_none() => {
            receive_program_command(Path::new(dir), payload, None)
        }
        ["sync-serve", dir, "--payload", payload, "--sdk", sdk] if host.is_none() => {
            receive_program_command(Path::new(dir), payload, Some(sdk))
        }
        ["sync-serve", dir, "--exec-payload", payload] if host.is_none() => {
            receive_exec_payload_command(Path::new(dir), payload)
        }
        // The SDK this binary carries, written out for developing a program
        // outside a search. It opens no store and reads no config.
        ["sdk", language, "--out", out] if host.is_none() => sdk_command(language, Path::new(out)),
        ["status", config] => status_command(&Target::new(config, host)),
        ["status", config, "--failed"] => status_failed_command(&Target::new(config, host)),
        ["status", config, "--task", key] => status_task_command(&Target::new(config, host), key),
        ["report", config] => report_command(&Target::new(config, host), Report::Summary),
        ["report", config, "--all"] => report_command(&Target::new(config, host), Report::All),
        ["report", config, "--task", key] => report_task_command(&Target::new(config, host), key),
        ["report", config, "--timeline"] => timeline_command(&Target::new(config, host)),
        // The rental ledger lives in the local store the orchestrator writes,
        // so the spend view reads it here, like `rm` and `reconcile`. A host
        // falls through to the usage error: the ledger does not travel the
        // follow feed.
        ["report", config, "--spend"] if host.is_none() => spend_command(&resolve_config(config)),
        // The reputation ledger is store state too, so it reads locally like
        // `--spend`; a host falls through to the usage error.
        ["report", config, "--machines"] if host.is_none() => {
            machines_command(&resolve_config(config))
        }
        ["tui", config] => tui::tui_command(&Target::new(config, host), Engagement::Orchestrator),
        ["tui", config, "--fleet"] if host.is_none() => {
            tui::tui_command(&Target::new(config, host), Engagement::Fleet)
        }
        ["follow", config] => follow::follow_command(&Target::new(config, host)),
        _ => {
            eprint!(
                "usage: sima search <config>                   drive the search on this machine\n\
                 \x20      sima search <config> --fleet           drive it on this machine and [fleet]\n\
                 \x20      sima search <config> --accept-binary   continue through a changed program\n\
                 \x20      sima search <config> --quiet           print the search's own progress and no more\n\
                 \x20      sima exec <config>                     run [exec].command on its rented host\n\
                 \x20      sima exec <config> --attach            replay and follow its running command\n\
                 \x20      sima exec <config> --one-shot          run, fetch, and destroy the instance\n\
                 \x20      sima exec <config> --end               stop, fetch, and destroy the instance\n\
                 \x20      sima exec <config> --fetch-to <dir>    override the local output directory\n\
                 \x20      sima exec <config> --quiet             print only the remote command's output\n\
                 \x20      sima status <config>                   report the search's state\n\
                 \x20      sima status <config> --task <key>      print one task's attempt timeline\n\
                 \x20      sima status <config> --failed          digest the tasks that did not commit\n\
                 \x20      sima report <config>                   count committed tasks per distinct stats value\n\
                 \x20      sima report <config> --all             print each committed task's stats\n\
                 \x20      sima report <config> --task <key>      print one committed task's stats\n\
                 \x20      sima report <config> --timeline        report the search's metrics and its timeline\n\
                 \x20      sima report <config> --spend           report the search's rental spend\n\
                 \x20      sima report <config> --machines        report machine reputation and blacklisting\n\
                 \x20      sima migrate <config>                  move the search onto the host [orchestrator] names\n\
                 \x20      sima migrate <config> --accept-binary  … through a changed program\n\
                 \x20      sima recall <config>                   wind the migrated search down and bring it home\n\
                 \x20      sima rm <config>                       delete the search and what only it references\n\
                 \x20      sima rm <config> --search <id>         … delete that search of the same store instead\n\
                 \x20      sima searches <store-dir>              list the searches the store holds\n\
                 \x20      sima sdk <language> --out <dir>        write the SDK this binary carries into <dir>\n\
                 \x20      sima pack <store-dir>                  consolidate the store's loose objects into packs\n\
                 \x20      sima pack <store-dir> --gc             … and delete everything outside the finalized\n\
                 \x20                                             searches' closures, unfinalized searches included, which\n\
                 \x20                                             destroys the work of a search still going\n\
                 \x20      sima reconcile <config>                destroy the machines a crashed search left running\n\
                 \x20      sima reconcile <config> --hosted       destroy the machines hosting a migrated search too\n\
                 \x20      sima tui <config> [--fleet]            drive the search in a full-screen terminal UI\n\
                 \x20      sima follow <config>                   stream the search's events until it ends\n\
                 \x20      <config> is a sima.toml path; the .toml extension may be omitted\n\
                 \x20      <key> is any prefix of a task key that names one task\n\
                 \x20      --on <host> observes a search on an ssh destination: status, report,\n\
                 \x20      tui, and follow accept it (report --spend and --machines stay\n\
                 \x20      local), and <config> is then a path on that host\n"
            );
            ExitCode::from(EXIT_ERROR)
        }
    }
}

/// Splits `--on <host>` out of the arguments, wherever in them it appears,
/// returning the rest and the host it named. The commands match on the rest,
/// so every command form keeps its exact shape whether or not a host is set.
///
/// A trailing `--on` with nothing after it names no host and stays in the
/// remaining arguments, where it matches no command form and falls to the
/// usage error. A repeated `--on` takes the last host given.
fn split_target(args: &[String]) -> (Vec<&str>, Option<&str>) {
    let mut rest = Vec::new();
    let mut host = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--on" if index + 1 < args.len() => {
                host = Some(args[index + 1].as_str());
                index += 2;
            }
            arg => {
                rest.push(arg);
                index += 1;
            }
        }
    }
    (rest, host)
}

/// Splits `--accept-binary` out of a `search` or `migrate` invocation, wherever in
/// it the flag appears, returning the rest and the answer it states. Both
/// commands match on the rest, so the flag composes with `--fleet` in either
/// order rather than multiplying the command forms.
///
/// `migrate` takes it because the comparison happens in the far `sima search`,
/// which installed the program the far config names: the acceptance is the
/// operator's, stated here, and travels to the search that acts on it.
///
/// Every other command keeps the flag among its arguments, where it matches no
/// form and falls to the usage error.
fn split_binary_change<'a>(args: &[&'a str]) -> (Vec<&'a str>, BinaryChange) {
    if !matches!(args.first(), Some(&"search") | Some(&"migrate")) {
        return (args.to_vec(), BinaryChange::Refuse);
    }
    let mut accept = BinaryChange::Refuse;
    let rest = args
        .iter()
        .copied()
        .filter(|arg| {
            let flag = *arg == "--accept-binary";
            if flag {
                accept = BinaryChange::Accept;
            }
            !flag
        })
        .collect();
    (rest, accept)
}

/// Splits `--quiet` out of an invocation that renders a search's stream, wherever
/// in it the flag appears, returning the rest and how much of the stream to
/// print. It composes with the other flags in either order, as
/// [`split_binary_change`] does, rather than multiplying the command forms.
///
/// The four verbs that render a live stream take it — `search`, `exec`,
/// `migrate`, and `recall`. Every other command keeps the flag among its
/// arguments, where it matches no form and falls to the usage error.
fn split_quiet<'a>(args: &[&'a str]) -> (Vec<&'a str>, Narration) {
    if !matches!(
        args.first(),
        Some(&"search") | Some(&"exec") | Some(&"migrate") | Some(&"recall")
    ) {
        return (args.to_vec(), Narration::Full);
    }
    let mut narration = Narration::Full;
    let rest = args
        .iter()
        .copied()
        .filter(|arg| {
            let flag = *arg == "--quiet";
            if flag {
                narration = Narration::Minimal;
            }
            !flag
        })
        .collect();
    (rest, narration)
}

/// Parses one user-facing `sima exec` form. Lifecycle flags are mutually
/// exclusive, while `--fetch-to` composes with start, one-shot, and end in
/// either order. Attach takes no other exec option.
fn exec_form<'a>(args: &[&'a str]) -> Option<(&'a str, ExecAction, Option<&'a str>)> {
    let ["exec", config, rest @ ..] = args else {
        return None;
    };
    let mut attach = false;
    let mut one_shot = false;
    let mut end = false;
    let mut fetch_to = None;
    let mut index = 0;
    while index < rest.len() {
        match rest[index] {
            "--attach" if !attach => attach = true,
            "--one-shot" if !one_shot => one_shot = true,
            "--end" if !end => end = true,
            "--fetch-to"
                if fetch_to.is_none()
                    && index + 1 < rest.len()
                    && !rest[index + 1].starts_with("--") =>
            {
                fetch_to = Some(rest[index + 1]);
                index += 1;
            }
            _ => return None,
        }
        index += 1;
    }
    let action = match (attach, one_shot, end) {
        (false, false, false) => ExecAction::Start { one_shot: false },
        (false, true, false) => ExecAction::Start { one_shot: true },
        (true, false, false) if fetch_to.is_none() => ExecAction::Attach,
        (false, false, true) => ExecAction::End,
        _ => return None,
    };
    Some((config, action, fetch_to))
}

/// The search a read command addresses: one on this machine, or one on the host
/// its orchestrator runs on.
///
/// A search's identity is the hash of its config, and its store path resolves
/// relative to the config file's directory, so a remote target carries the
/// config argument unresolved: it names a path on the far side, and the far
/// side is what interprets it.
enum Target {
    /// A config file on this machine, resolved as written.
    Local(PathBuf),
    /// A config file on `host`, as typed.
    Remote {
        /// The ssh destination the local ssh client resolves.
        host: String,
        /// The config argument, passed through to that host verbatim.
        config: String,
    },
}

impl Target {
    /// The target a command's config argument and optional host name.
    fn new(config: &str, host: Option<&str>) -> Target {
        match host {
            None => Target::Local(resolve_config(config)),
            Some(host) => Target::Remote {
                host: host.to_string(),
                config: config.to_string(),
            },
        }
    }
}

/// Opens a live feed over the target's search: the journal on this machine, or
/// one follow stream from the host the orchestrator runs on. The views that
/// tail a search consume the feed and never learn which it is.
fn feed(target: &Target) -> Result<Box<dyn SearchFeed>> {
    match target {
        Target::Local(path) => Ok(Box::new(LocalFeed::open(&load(path)?)?)),
        Target::Remote { host, config } => Ok(Box::new(RemoteFeed::open(host, config)?)),
    }
}

/// Reads everything the target's search journaled, with the metadata the views
/// render through: locally through the store, remotely over one follow
/// stream. The fold that renders it is the same either way.
fn snapshot(target: &Target) -> Result<(FeedInfo, Vec<Record>)> {
    match target {
        Target::Local(path) => local_snapshot(&load(path)?),
        Target::Remote { host, config } => remote_snapshot(host, config),
    }
}

/// Resolves the config argument to a path: the argument as given when it
/// names a file, otherwise the argument with `.toml` appended when that
/// names one — so `sima search demo` finds `demo.toml`. When neither exists,
/// the argument passes through unchanged and loading reports the error
/// against what the user typed.
fn resolve_config(arg: &str) -> PathBuf {
    let path = PathBuf::from(arg);
    if !path.is_file() {
        let with_toml = PathBuf::from(format!("{arg}.toml"));
        if with_toml.is_file() {
            return with_toml;
        }
    }
    path
}

/// `sima search <config.toml> [--fleet] [--accept-binary]`: loads, prints the search
/// id, orchestrates with progress rendering and the SIGINT flag installed, and
/// maps the outcome to the exit code. `engagement` is what the invocation asked
/// for: this machine alone, or this machine and the fleet. `accept` is what it
/// asked for about a program whose build changed under the search.
fn search_command(
    config: &Path,
    engagement: Engagement,
    accept: BinaryChange,
    narration: Narration,
) -> ExitCode {
    match drive(config, engagement, accept, narration) {
        Ok(outcome) => ExitCode::from(outcome_exit_code(&outcome)),
        Err(e) => report(e),
    }
}

/// Terminal observer for an exec. Remote command lines always reach stdout;
/// orchestration lines do so only at full narration.
struct ExecProgress {
    narration: Narration,
    machine: Option<(String, u64)>,
}

impl ExecObserver for ExecProgress {
    fn command(&mut self, line: &str) {
        println!("{line}");
    }

    fn narration(&mut self, line: &str) {
        if self.narration == Narration::Full {
            println!("{line}");
        }
    }

    fn instance(&mut self, id: &str, rate_microusd_hour: u64, adopted: bool) {
        self.machine = Some((id.to_string(), rate_microusd_hour));
        self.narration(&format!(
            "{} instance {id} at ${:.6}/hr",
            if adopted { "adopted" } else { "acquired" },
            rate_microusd_hour as f64 / 1_000_000.0
        ));
    }
}

impl ExecProgress {
    /// The adopted or acquired machine, formatted for a terminal lifecycle
    /// line. Every outcome past acquisition has one.
    fn machine(&self) -> String {
        let (id, rate) = self
            .machine
            .as_ref()
            .expect("an exec outcome past acquisition names its machine");
        format!("instance {id} at ${:.6}/hr", *rate as f64 / 1_000_000.0)
    }
}

/// `sima exec <config.toml>` and its lifecycle forms: registers the interrupt
/// flag, drives one remote command, renders its terminal state, and preserves
/// the remote command's exit code.
fn exec_command(
    config: &Path,
    action: ExecAction,
    fetch_to: Option<&Path>,
    narration: Narration,
) -> ExitCode {
    let interrupt = match register_interrupt() {
        Ok(interrupt) => interrupt,
        Err(error) => return report(error),
    };
    let fetch_to = fetch_to.map(|path| config.parent().unwrap_or_else(|| Path::new("")).join(path));
    let options = ExecOptions { action, fetch_to };
    let mut progress = ExecProgress {
        narration,
        machine: None,
    };
    match exec(config, &options, &interrupt, &mut progress) {
        Ok(ExecOutcome::Completed(code)) => {
            if narration == Narration::Full {
                match action {
                    ExecAction::Start { one_shot: true } => {
                        println!(
                            "completed with exit code {code}; released {}",
                            progress.machine()
                        );
                    }
                    ExecAction::Start { one_shot: false } | ExecAction::Attach => println!(
                        "completed with exit code {code}; kept {}; end with sima exec {} --end",
                        progress.machine(),
                        config.display()
                    ),
                    ExecAction::End => unreachable!("end never completes a command"),
                }
            }
            ExitCode::from(exec_exit_code(code))
        }
        Ok(ExecOutcome::Detached) => {
            if narration == Narration::Full {
                println!(
                    "detached from {}: attach with sima exec {} --attach; end with sima exec {} --end",
                    progress.machine(),
                    config.display(),
                    config.display()
                );
            }
            ExitCode::SUCCESS
        }
        Ok(ExecOutcome::Abandoned { kept }) => {
            if narration == Narration::Full {
                if kept {
                    println!(
                        "abandoned before start; kept {}; end with sima exec {} --end",
                        progress.machine(),
                        config.display()
                    );
                } else if progress.machine.is_some() {
                    println!("abandoned before start; released {}", progress.machine());
                } else {
                    println!("abandoned before start; acquisition cancelled");
                }
            }
            ExitCode::SUCCESS
        }
        Ok(ExecOutcome::Ended) => {
            if narration == Narration::Full {
                println!("ended: outputs fetched and instance released");
            }
            ExitCode::SUCCESS
        }
        Ok(ExecOutcome::NoInstance) => {
            if narration == Narration::Full {
                println!("no standing instance for this exec");
            }
            ExitCode::SUCCESS
        }
        Ok(ExecOutcome::BudgetExhausted(exhaustion)) => {
            if narration == Narration::Full {
                println!(
                    "budget exhausted ({exhaustion:?}); outputs fetched and released {}",
                    progress.machine()
                );
            }
            ExitCode::from(EXIT_ERROR)
        }
        Err(error) => report(error),
    }
}

/// Converts the shell's command status to the process exit-code range. A
/// malformed remote status is an orchestration fault.
fn exec_exit_code(code: i32) -> u8 {
    u8::try_from(code).unwrap_or(EXIT_ERROR)
}

/// Loads the config and drives its search.
fn drive(
    config: &Path,
    engagement: Engagement,
    accept: BinaryChange,
    narration: Narration,
) -> Result<SearchOutcome> {
    let loaded = load(config)?;
    let interrupt = register_interrupt()?;

    println!("search {}", loaded.search.id());
    // The search's own `SearchStarted` carries the prior commits, counted from the
    // store, so a resumed search counts on from where it stopped.
    let progress = render::Progress::new(narration);
    let control = SearchControl {
        observer: &|record| progress.event(record),
        interrupt: &interrupt,
        on_start: None,
    };
    orchestrate(&loaded, &control, engagement, accept)
}

/// `sima report <config.toml> --spend`: the search's rental ledger — closed
/// rentals, open ones, and the total — read from the local store.
fn spend_command(config: &Path) -> ExitCode {
    match load(config).and_then(|loaded| sima_pipeline::spend(&loaded)) {
        Ok(report) => {
            println!("{}", render::spend_block(&report));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// `sima report <config.toml> --machines`: the store's machine-reputation
/// ledger — one line per machine with a recorded incident, its counts by kind,
/// and its blacklist status — read from the local store. Store-scoped, so it
/// answers whatever the store observed across every search, and exits 0 over a
/// store that recorded none.
fn machines_command(config: &Path) -> ExitCode {
    match load(config).and_then(|loaded| sima_pipeline::machines(&loaded)) {
        Ok(report) => {
            println!("{}", render::machines_block(&report));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// `sima status <config.toml>`: the config's execution section names the
/// store, its identity section derives the search id.
fn status_command(target: &Target) -> ExitCode {
    match read_status(target) {
        Ok(report) => {
            println!("{}", render::status_block(&report));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Computes the target search's status from the records it journaled.
fn read_status(target: &Target) -> Result<SearchStatus> {
    let (info, records) = snapshot(target)?;
    Ok(status_records(info.search, &records))
}

/// `sima report <config.toml> --timeline`: the search's execution metrics and the
/// temporal shape of the session behind them. The query answers whatever the
/// search's own outcome was, so a report over a failed search still exits 0.
fn timeline_command(target: &Target) -> ExitCode {
    match read_timeline(target) {
        Ok(timeline) => {
            println!("{}", render::timeline_block(&timeline));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Computes the target search's metrics from the records it journaled.
fn read_timeline(target: &Target) -> Result<SearchTimeline> {
    let (info, records) = snapshot(target)?;
    Ok(timeline_records(info.search, &records))
}

/// `sima status <config.toml> --task <key>`: one task's attempt timeline,
/// addressed by a prefix of its key. The store and search id come from the
/// config the same way the aggregate status derives them.
fn status_task_command(target: &Target, prefix: &str) -> ExitCode {
    match read_task_history(target, prefix) {
        Ok(history) => {
            println!("{}", render::task_block(&history));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Projects one task's lifecycle from the records the target search journaled.
fn read_task_history(target: &Target, prefix: &str) -> Result<TaskHistory> {
    let (_info, records) = snapshot(target)?;
    task_history_records(&records, prefix)
}

/// `sima status <config.toml> --failed`: the tasks the search did not commit,
/// one line each. The query answers whatever the search's own outcome was, so a
/// digest over a failed search still exits 0.
fn status_failed_command(target: &Target) -> ExitCode {
    match read_failures(target) {
        Ok((search, failures)) => {
            println!("{}", render::failures_block(&search, &failures));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Projects the tasks the target search did not commit, with the search the digest
/// names.
fn read_failures(target: &Target) -> Result<(SearchId, Vec<TaskHistory>)> {
    let (info, records) = snapshot(target)?;
    Ok((info.search, failures_records(&records)))
}

/// Seeds the tui's display from any existing journal for `config`'s search,
/// replaying it through the same `apply` method `sima status` uses so a
/// resumed search opens on its prior progress. This is the observational view:
/// it reports what the journal says, which is the whole of what `sima status`
/// answers too.
///
/// A store that does not exist yet, or a search never driven, seeds a zeroed
/// status; a corrupt journal or an I/O fault is a real problem `sima status`
/// reports, so it surfaces here rather than hiding behind wrong counts. The
/// two are told apart by asking whether the search is journaled at all, not by
/// reading an error variant — which would put every future failure on the read
/// path into the "nothing here yet" bucket.
pub(crate) fn seed_status(config: &LoadedConfig) -> Result<SearchStatus> {
    let mut seeded = seeded_status(config)?;
    // The counters and last state are worth seeding, but a journal ending
    // mid-search leaves leases no live worker holds; a fresh session starts with
    // every worker idle and repopulates occupancy from live `Leased` events.
    seeded.occupancy.clear();
    Ok(seeded)
}

/// How much of a search's committed stats `sima report` prints.
enum Report {
    /// A total header, then one line per distinct rendered stats value with
    /// its count.
    Summary,
    /// One `<short task key>  <rendered stats>` line per committed task.
    All,
}

/// `sima report [--all] <config.toml>`: renders the search's committed stats,
/// compactly by default. The store and search id come from the config the same
/// way `status` derives them.
fn report_command(target: &Target, scope: Report) -> ExitCode {
    match read_report(target) {
        Ok(rows) => write_rows(&rows, scope),
        Err(e) => report(e),
    }
}

/// `sima report <config.toml> --task <key>`: one committed task's stats,
/// addressed by a prefix of its key.
fn report_task_command(target: &Target, prefix: &str) -> ExitCode {
    match read_report_task(target, prefix) {
        Ok(row) => write_rows(&[row], Report::All),
        Err(e) => report(e),
    }
}

/// Writes `rows` to stdout, taken locked once, in the form `scope` names.
fn write_rows(rows: &[ReportRow], scope: Report) -> ExitCode {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let written = match scope {
        Report::Summary => write_summary(&mut out, rows),
        Report::All => write_report(&mut out, rows),
    };
    match written {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => report(e),
    }
}

/// Maps one report line's write outcome: `Ok(true)` when written, `Ok(false)`
/// when the reader closed the pipe, `Err` otherwise. Piping into a reader
/// that closes early (`sima report ... | head`) is ordinary use, so the
/// resulting `BrokenPipe` is that reader's normal exit — the caller stops
/// writing and reports success. Any other write failure is an infrastructure
/// fault against stdout.
fn line_written(result: std::io::Result<()>) -> Result<bool> {
    match result {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(false),
        Err(e) => Err(Error::Io {
            path: PathBuf::from("stdout"),
            source: e,
        }),
    }
}

/// Writes one line per reported task — `<short task key>  <rendered stats>` —
/// to `out`, taken locked once by the caller.
fn write_report(out: &mut impl std::io::Write, rows: &[ReportRow]) -> Result<()> {
    for row in rows {
        if !line_written(writeln!(out, "{}  {}", render::short(&row.task), row.stats))? {
            return Ok(());
        }
    }
    Ok(())
}

/// Writes the compact summary to `out`, taken locked once by the caller: a
/// `<total> committed tasks` header, then one `<count>  <stats>` line per
/// distinct rendered stats value.
fn write_summary(out: &mut impl std::io::Write, rows: &[ReportRow]) -> Result<()> {
    if !line_written(writeln!(out, "{} committed tasks", rows.len()))? {
        return Ok(());
    }
    for (count, stats) in group_stats(rows) {
        if !line_written(writeln!(out, "{count}  {stats}"))? {
            return Ok(());
        }
    }
    Ok(())
}

/// Groups report rows by their rendered stats value: one entry per distinct
/// value with its task count, ordered by count descending, ties by the stats
/// string ascending — so the summary is deterministic.
fn group_stats(rows: &[ReportRow]) -> Vec<(usize, &str)> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for row in rows {
        *counts.entry(&row.stats).or_default() += 1;
    }
    let mut groups: Vec<(usize, &str)> = counts
        .into_iter()
        .map(|(stats, count)| (count, stats))
        .collect();
    // The map iterates by stats ascending; the stable sort by count descending
    // keeps that as the order among equal counts.
    groups.sort_by_key(|&(count, _)| std::cmp::Reverse(count));
    groups
}

/// Renders each committed task's stats from the records the target search
/// journaled.
fn read_report(target: &Target) -> Result<Vec<ReportRow>> {
    let (_info, records) = snapshot(target)?;
    report_records(&records)
}

/// Renders one committed task's stats from the records the target search
/// journaled.
fn read_report_task(target: &Target, prefix: &str) -> Result<ReportRow> {
    let (_info, records) = snapshot(target)?;
    report_task_records(&records, prefix)
}

/// `sima follow-serve <config> [--once]`: writes the search's follow stream to
/// stdout, which carries frames and nothing else — every diagnostic goes to
/// stderr, which ssh keeps on its own channel. The near half of the transport
/// spawns this over ssh; it is not a user-facing verb and stays out of the
/// usage text.
fn serve_command(config: &str, once: bool) -> ExitCode {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    match follow_serve(&resolve_config(config), once, &mut out) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => report(e),
    }
}

/// `sima sync-serve <store-dir> --search <search-id>`: serves one store-sync session
/// over stdin and stdout, which carry protocol frames and nothing else — every
/// diagnostic goes to stderr, which ssh keeps on its own channel. A migration
/// spawns this over ssh; it is not a user-facing verb and stays out of the
/// usage text.
///
/// It addresses the store and the search rather than a config, because loading a
/// config resolves its `[domain.*]` entries — which installs and spawns the
/// program that the very session being served may be delivering. The initiator
/// knows both values: it derives the search id locally and the far store sits in
/// the search's own directory.
///
/// It takes the search lock for the session, so a `sima search` driving this search on
/// this machine makes the sync fail cleanly on the lock rather than writing
/// underneath it.
fn sync_serve_command(store: &Path, search: &str) -> ExitCode {
    let search = match SearchId::from_hex(search) {
        Ok(search) => search,
        Err(e) => return report(e),
    };
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let (mut input, mut output) = (stdin.lock(), stdout.lock());
    match sync_serve(store, &search, &mut input, &mut output) {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => report(e),
    }
}

/// `sima sync-serve <dir> --payload <digest> [--sdk <digest>]`: receives the
/// objects those digests name into `<dir>/store` and installs both trees, over
/// the same stdin and stdout the `--search` form uses. A search putting work on this
/// machine spawns it before constructing a pool there; it is not a user-facing
/// verb and stays out of the usage text, as the `--search` form does.
///
/// It addresses a directory and two digests rather than a config, for the
/// reason the `--search` form addresses a store and a search: loading a config
/// resolves its `[domain.*]` entries, which spawns the very program this
/// session is delivering.
fn receive_program_command(dir: &Path, payload: &str, sdk: Option<&str>) -> ExitCode {
    let digests = Hash::from_hex(payload)
        .and_then(|payload| Ok((payload, sdk.map(Hash::from_hex).transpose()?)));
    let (payload, sdk) = match digests {
        Ok(digests) => digests,
        Err(e) => return report(e),
    };
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let (mut input, mut output) = (stdin.lock(), stdout.lock());
    match receive_program(dir, &payload, sdk.as_ref(), &mut input, &mut output) {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => report(e),
    }
}

/// `sima sync-serve <dir> --exec-payload <digest>`: receives an exec payload
/// into its shared object cache and materializes it over the mutable job tree.
fn receive_exec_payload_command(dir: &Path, payload: &str) -> ExitCode {
    let payload = match Hash::from_hex(payload) {
        Ok(payload) => payload,
        Err(error) => return report(error),
    };
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let (mut input, mut output) = (stdin.lock(), stdout.lock());
    match receive_exec_payload(dir, &payload, &mut input, &mut output) {
        Ok(_) => ExitCode::SUCCESS,
        Err(error) => report(error),
    }
}

/// `sima sdk <language> --out <dir>`: writes the SDK this binary carries under
/// `<dir>`, so a program can be developed against the package the searches that
/// spawn it will vend.
///
/// It is the same write a config load performs beneath its stamp, addressed by
/// hand: what a search puts on the program's module path is what lands here.
fn sdk_command(language: &str, out: &Path) -> ExitCode {
    let Some(sdk) = Sdk::parse(language) else {
        return report(Error::Validation(format!(
            "{language:?} names no SDK this binary vends; the verb takes {}",
            Sdk::accepted()
        )));
    };
    match sdk.vend(out) {
        Ok(()) => {
            println!("vended the {language} SDK into {}", out.display());
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// `sima rm <config.toml>`: deletes the search — and everything no surviving search
/// references — under its search lock, and prints what was removed. The search id
/// comes from the config's identity section, as `status` derives it.
fn rm_command(config: &Path) -> ExitCode {
    match remove_search(config) {
        Ok(report) => {
            println!(
                "removed search: {} objects, {} index entries",
                report.objects_removed, report.index_entries_removed
            );
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Loads the config and removes its search.
fn remove_search(config: &Path) -> Result<RemovalReport> {
    let loaded = load(config)?;
    sima_pipeline::remove(&loaded)
}

/// `sima rm <config.toml> --search <id-prefix>`: removes the search of that config's
/// store whose id begins with the prefix, whether or not the config still
/// names it.
fn rm_matching_command(config: &Path, prefix: &str) -> ExitCode {
    match load(config).and_then(|loaded| sima_pipeline::remove_matching(&loaded, prefix)) {
        Ok(report) => {
            println!(
                "removed search: {} objects, {} index entries",
                report.objects_removed, report.index_entries_removed
            );
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// `sima searches <store-dir>`: one line per search the store holds, with its state
/// and its task ledger.
fn searches_command(store: &Path) -> ExitCode {
    match sima_pipeline::searches(store) {
        Ok(summaries) => {
            println!("{}", render::searches_block(&summaries));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Prints one stderr line when the config's store has accumulated enough
/// loose objects for packing to pay, naming the command that does it.
///
/// Best effort throughout: a config that does not load or a store that
/// cannot be measured says nothing here, because the command itself is
/// about to report whatever is wrong with far better context. A store that
/// does not exist yet is left alone rather than opened, since opening one
/// creates it and the query commands are read-only about that. `verb` selects
/// the config contract, so an exec warning resolves no search program.
fn warn_on_loose_objects(config: &Path, verb: &str) {
    let store_path = match if verb == "exec" {
        load_exec(config).map(|loaded| loaded.store)
    } else {
        load(config).map(|loaded| loaded.store)
    } {
        Ok(store) => store,
        Err(_) => return,
    };
    if !store_path.is_dir() {
        return;
    }
    let Ok(store) = sima_store::Store::open(&store_path) else {
        return;
    };
    let Ok(estimate) = store.loose_object_estimate() else {
        return;
    };
    if estimate >= LOOSE_OBJECT_WARN_THRESHOLD {
        eprintln!(
            "store holds ~{estimate} loose objects; run `sima pack {}` to consolidate",
            store_path.display()
        );
    }
}

/// Prints `error` to stderr and yields the generic error exit code.
pub(crate) fn report(error: Error) -> ExitCode {
    eprintln!("sima: {error}");
    ExitCode::from(EXIT_ERROR)
}

/// Wraps a signal-registration failure: an OS-level refusal to install
/// the handler, surfaced before the search starts.
fn register_error(e: std::io::Error) -> Error {
    Error::System(format!("cannot register the SIGINT handler: {e}"))
}

/// The exit code a finished search maps to — the mapping `search` and `tui` share:
/// success when finalized, the failure code for a definitive candidate
/// failure, and the interrupt code for a wound-down search.
pub(crate) fn outcome_exit_code(outcome: &SearchOutcome) -> u8 {
    match outcome {
        SearchOutcome::Finalized { .. } => 0,
        SearchOutcome::Failed { .. } => EXIT_FAILED,
        SearchOutcome::Interrupted { .. } => EXIT_INTERRUPTED,
    }
}

/// The exit code a search's state carries, over the state a journal projects
/// rather than the outcome an orchestrator returns.
///
/// `search` returns an outcome and every observational command projects a state,
/// so the two mappings exist; this is the one the observers share. A search still
/// in progress when its stream drains is resumable, not failed, so it leaves
/// successfully.
pub(crate) fn state_exit_code(state: &SearchState) -> u8 {
    match state {
        SearchState::Finalized | SearchState::InProgress => 0,
        SearchState::Failed { .. } => EXIT_FAILED,
        SearchState::Interrupted => EXIT_INTERRUPTED,
    }
}

/// Registers the SIGINT flag every long-running command winds down on.
///
/// Registered before any output, so Ctrl-C is graceful from the first line on;
/// a second Ctrl-C falls through to the default death, which is safe because
/// that is exactly the crash the recovery guarantees cover. Both registrations
/// are needed and in this order: the conditional default is what lets the
/// second signal kill, and it must be in place before the flag that swallows
/// the first.
pub(crate) fn register_interrupt() -> Result<Arc<AtomicBool>> {
    let interrupt = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register_conditional_default(signal_hook::consts::SIGINT, interrupt.clone())
        .map_err(register_error)?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, interrupt.clone())
        .map_err(register_error)?;
    Ok(interrupt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::hash_bytes;
    use sima_model::{SearchId, TaskKey};

    /// A writer that fails every write with a fixed error kind, to drive
    /// `write_report`'s error handling without a real pipe.
    struct FailingWriter(std::io::ErrorKind);

    impl std::io::Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from(self.0))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// One report row over the given task key and rendered stats.
    fn row(task: &str, stats: &str) -> ReportRow {
        ReportRow {
            task: task.to_string(),
            stats: stats.to_string(),
        }
    }

    #[test]
    fn a_broken_pipe_while_writing_the_report_is_success() {
        // A reader closing the pipe (`sima report ... | head`) surfaces as
        // BrokenPipe; that is its normal exit, so the write reports success.
        let mut sink = FailingWriter(std::io::ErrorKind::BrokenPipe);
        assert!(write_report(&mut sink, &[row("aa", "attempt 0")]).is_ok());
        assert!(write_summary(&mut sink, &[row("aa", "attempt 0")]).is_ok());
    }

    #[test]
    fn any_other_stdout_write_failure_is_reported() {
        let mut sink = FailingWriter(std::io::ErrorKind::PermissionDenied);
        assert!(matches!(
            write_report(&mut sink, &[row("aa", "attempt 0")]),
            Err(Error::Io { .. })
        ));
        assert!(matches!(
            write_summary(&mut sink, &[row("aa", "attempt 0")]),
            Err(Error::Io { .. })
        ));
    }

    #[test]
    fn grouping_orders_by_count_descending_then_stats_ascending() {
        // "a" and "c" tie at two rows each: the tie breaks on the stats string,
        // ascending, so equal counts render in a deterministic order.
        let rows = [
            row("1", "c"),
            row("2", "a"),
            row("3", "c"),
            row("4", "b"),
            row("5", "a"),
        ];
        assert_eq!(group_stats(&rows), vec![(2, "a"), (2, "c"), (1, "b")]);
    }

    #[test]
    fn the_summary_prints_the_header_then_grouped_lines() {
        let rows = [
            row("aa", "attempt 1"),
            row("bb", "attempt 0"),
            row("cc", "attempt 0"),
        ];
        let mut out = Vec::new();
        write_summary(&mut out, &rows).expect("write");
        assert_eq!(
            String::from_utf8(out).expect("utf-8"),
            "3 committed tasks\n2  attempt 0\n1  attempt 1\n"
        );
    }

    /// Splits an argument list given as string slices.
    fn split(args: &[&str]) -> (Vec<String>, Option<String>) {
        let args: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        let (rest, host) = split_target(&args);
        (
            rest.into_iter().map(str::to_string).collect(),
            host.map(str::to_string),
        )
    }

    #[test]
    fn a_host_leaves_every_command_form_intact() {
        // The commands match on the rest, so extracting the pair — from any
        // position — must leave exactly the argument list they already match.
        let (rest, host) = split(&["status", "exp.toml", "--task", "ab", "--on", "gpubox"]);
        assert_eq!(rest, ["status", "exp.toml", "--task", "ab"]);
        assert_eq!(host.as_deref(), Some("gpubox"));

        let (rest, host) = split(&["status", "--on", "gpubox", "exp.toml", "--failed"]);
        assert_eq!(rest, ["status", "exp.toml", "--failed"]);
        assert_eq!(host.as_deref(), Some("gpubox"));
    }

    #[test]
    fn arguments_without_a_host_pass_through_unchanged() {
        let (rest, host) = split(&["report", "exp.toml", "--all"]);
        assert_eq!(rest, ["report", "exp.toml", "--all"]);
        assert_eq!(host, None);
    }

    #[test]
    fn a_trailing_host_flag_names_no_host_and_stays_in_the_arguments() {
        // Left in place, it matches no command form and falls to the usage
        // error, rather than silently reading as a local command.
        let (rest, host) = split(&["status", "exp.toml", "--on"]);
        assert_eq!(rest, ["status", "exp.toml", "--on"]);
        assert_eq!(host, None);
    }

    #[test]
    fn a_repeated_host_flag_takes_the_last_host() {
        let (rest, host) = split(&["status", "exp.toml", "--on", "a", "--on", "b"]);
        assert_eq!(rest, ["status", "exp.toml"]);
        assert_eq!(host.as_deref(), Some("b"));
    }

    /// Splits the binary-change answer out of an argument list given as string
    /// slices.
    fn split_change(args: &[&str]) -> (Vec<String>, BinaryChange) {
        let (rest, accept) = split_binary_change(args);
        (rest.into_iter().map(str::to_string).collect(), accept)
    }

    #[test]
    fn the_binary_flag_leaves_every_search_form_intact() {
        // `search` matches on the rest, so extracting the flag — from either
        // position — must leave exactly the argument list the arms match.
        let (rest, accept) = split_change(&["search", "exp.toml", "--accept-binary"]);
        assert_eq!(rest, ["search", "exp.toml"]);
        assert_eq!(accept, BinaryChange::Accept);

        let (rest, accept) = split_change(&["search", "exp.toml", "--fleet", "--accept-binary"]);
        assert_eq!(rest, ["search", "exp.toml", "--fleet"]);
        assert_eq!(accept, BinaryChange::Accept);

        let (rest, accept) = split_change(&["search", "exp.toml", "--accept-binary", "--fleet"]);
        assert_eq!(rest, ["search", "exp.toml", "--fleet"]);
        assert_eq!(accept, BinaryChange::Accept);
    }

    #[test]
    fn the_binary_flag_leaves_the_migrate_form_intact() {
        // A migration takes it because the far `sima search` is where the
        // comparison happens: the acceptance is stated here and travels there.
        let (rest, accept) = split_change(&["migrate", "exp.toml", "--accept-binary"]);
        assert_eq!(rest, ["migrate", "exp.toml"]);
        assert_eq!(accept, BinaryChange::Accept);

        let (rest, accept) = split_change(&["migrate", "exp.toml"]);
        assert_eq!(rest, ["migrate", "exp.toml"]);
        assert_eq!(accept, BinaryChange::Refuse);
    }

    #[test]
    fn a_search_without_the_binary_flag_refuses_a_changed_program() {
        let (rest, accept) = split_change(&["search", "exp.toml", "--fleet"]);
        assert_eq!(rest, ["search", "exp.toml", "--fleet"]);
        assert_eq!(accept, BinaryChange::Refuse);
    }

    #[test]
    fn the_binary_flag_stays_in_another_command_s_arguments() {
        // Left in place, it matches no form and falls to the usage error,
        // rather than reading as a query that quietly accepted something.
        let (rest, accept) = split_change(&["status", "exp.toml", "--accept-binary"]);
        assert_eq!(rest, ["status", "exp.toml", "--accept-binary"]);
        assert_eq!(accept, BinaryChange::Refuse);
    }

    #[test]
    fn exec_forms_dispatch_to_the_declared_action() {
        assert_eq!(
            exec_form(&["exec", "job.toml"]),
            Some(("job.toml", ExecAction::Start { one_shot: false }, None))
        );
        assert_eq!(
            exec_form(&["exec", "job.toml", "--attach"]),
            Some(("job.toml", ExecAction::Attach, None))
        );
        assert_eq!(
            exec_form(&["exec", "job.toml", "--fetch-to", "results", "--one-shot",]),
            Some((
                "job.toml",
                ExecAction::Start { one_shot: true },
                Some("results"),
            ))
        );
        assert_eq!(
            exec_form(&["exec", "job.toml", "--end", "--fetch-to", "results"]),
            Some(("job.toml", ExecAction::End, Some("results")))
        );
    }

    #[test]
    fn malformed_exec_forms_fall_through_to_usage() {
        for args in [
            &["exec"][..],
            &["exec", "job.toml", "--attach", "--one-shot"],
            &["exec", "job.toml", "--attach", "--fetch-to", "results"],
            &["exec", "job.toml", "--end", "--one-shot"],
            &["exec", "job.toml", "--fetch-to"],
            &["exec", "job.toml", "--fetch-to", "--one-shot"],
            &["exec", "job.toml", "--fetch-to", "a", "--fetch-to", "b"],
            &["exec", "job.toml", "--unknown"],
        ] {
            assert_eq!(exec_form(args), None, "{args:?}");
        }
    }

    #[test]
    fn exec_preserves_every_shell_exit_code() {
        for code in 0..=u8::MAX {
            assert_eq!(exec_exit_code(i32::from(code)), code);
        }
        assert_eq!(exec_exit_code(-1), EXIT_ERROR);
        assert_eq!(exec_exit_code(256), EXIT_ERROR);
    }

    #[test]
    fn each_outcome_maps_to_its_exit_code() {
        let search = SearchId::from_hash(hash_bytes(b"exit code search"));
        assert_eq!(outcome_exit_code(&SearchOutcome::Finalized { search }), 0);
        assert_eq!(
            outcome_exit_code(&SearchOutcome::Failed {
                task: TaskKey::from_hash(hash_bytes(b"a task")),
                reason: "rejected".to_string(),
            }),
            EXIT_FAILED
        );
        assert_eq!(
            outcome_exit_code(&SearchOutcome::Interrupted { search }),
            EXIT_INTERRUPTED
        );
    }
}
