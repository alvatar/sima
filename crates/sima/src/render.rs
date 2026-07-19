//! Terminal rendering: one plain line per meaningful lifecycle event, and
//! the status block. Ids render short — the first twelve hex characters —
//! since a run's journal names them consistently.

use std::sync::atomic::{AtomicUsize, Ordering};

use sima_pipeline::{Event, Record, RunState, RunStatus};

/// How many hex characters of an id a progress line shows.
const SHORT: usize = 12;

/// The leading `SHORT` characters of a journaled id.
pub fn short(id: &str) -> &str {
    &id[..id.len().min(SHORT)]
}

/// Renders `event` to the one line it warrants, or `None` for the `Queued`,
/// `Leased`, and `WorkerBound` bookkeeping events. `committed`/`tasks` supply
/// the running `committed k/n` count a commit line shows; on `RunStarted`, a
/// nonzero `committed` is the run's prior progress and the line names it, so a
/// resumed session does not read as a restart. The single source of the
/// event wording: `sima run` prints these lines to stdout and the tui folds
/// them into its event log.
pub fn describe(event: &Event, committed: usize, tasks: usize) -> Option<String> {
    Some(match event {
        Event::RunStarted { tasks, .. } if committed > 0 => {
            format!("started: {tasks} tasks, {committed} already committed")
        }
        Event::RunStarted { tasks, .. } => format!("started: {tasks} tasks"),
        Event::Committed { task, .. } => {
            format!("committed {committed}/{tasks}  {}", short(task))
        }
        Event::Retried { task, next_attempt } => {
            format!("retrying {} (attempt {next_attempt})", short(task))
        }
        Event::Rejected { task, reason, .. } => {
            format!("rejected {}: {reason}", short(task))
        }
        Event::Failed {
            task,
            attempt,
            reason,
            ..
        } => format!("failed {} (attempt {attempt}): {reason}", short(task)),
        Event::Faulted { task, error, .. } => format!("fault {}: {error}", short(task)),
        Event::LeaseExpired {
            task, elapsed_ms, ..
        } => format!("lease expired {} ({elapsed_ms} ms)", short(task)),
        Event::CheckpointDegraded { task, error } => {
            format!("checkpoint degraded {}: {error}", short(task))
        }
        Event::RunFinalized { committed, .. } => {
            format!("finalized: {committed} tasks committed")
        }
        Event::RunFailed { task, reason, .. } => {
            format!("run failed on {}: {reason}", short(task))
        }
        Event::RunInterrupted { .. } => {
            "interrupted: store resumable, re-run to continue".to_string()
        }
        // A rebind means the hardware changed under the search: the chain's
        // device is gone and its work moved. Loud by design.
        Event::ChainRebound { chain, from, to } => {
            format!("chain {chain} rebound: {from} is absent, continuing on {to}")
        }
        // A warn or error diagnostic is worth a console line; info-level
        // diagnostics (worker stderr) are journaled, not echoed, so the
        // run's console output stays clean.
        Event::Diagnostic {
            level,
            source,
            message,
            worker,
            ..
        } => {
            let level = match level {
                sima_pipeline::Level::Warn => "warn",
                sima_pipeline::Level::Error => "error",
                sima_pipeline::Level::Info => return None,
            };
            match worker {
                Some(worker) => format!("{level} {source} worker {worker}: {message}"),
                None => format!("{level} {source}: {message}"),
            }
        }
        Event::Queued { .. } | Event::Leased { .. } | Event::WorkerBound { .. } => return None,
    })
}

/// Progress rendering over a run's event stream: prints one line per
/// meaningful event. Called from the collector thread, one record at a
/// time, in journal order; the counters give the `committed k/n` running
/// count.
pub struct Progress {
    /// The run's task count, from `RunStarted`.
    tasks: AtomicUsize,
    /// Commits accounted for: the run's prior commits plus those seen live.
    committed: AtomicUsize,
}

impl Progress {
    /// A progress renderer over a session's events. Both counters come from
    /// the run's own `RunStarted`, so nothing needs to be known before the
    /// run starts.
    pub fn new() -> Progress {
        Progress {
            tasks: AtomicUsize::new(0),
            committed: AtomicUsize::new(0),
        }
    }

    /// Prints the line the record's event warrants, if any, keeping the
    /// running commit count for the `committed k/n` line. `Queued`, `Leased`,
    /// and `WorkerBound` yield no line and stay silent.
    pub fn event(&self, record: &Record) {
        let event = &record.event;
        if let Event::RunStarted {
            tasks, committed, ..
        } = event
        {
            self.tasks.store(*tasks, Ordering::Relaxed);
            // The run's prior commits, counted from the store's records by
            // the source that derived the frontier. A resumed session counts
            // on from there.
            self.committed.store(*committed, Ordering::Relaxed);
        }
        // A commit advances the running count; every other line reads it
        // without moving it.
        let committed = match event {
            Event::Committed { .. } => self.committed.fetch_add(1, Ordering::Relaxed) + 1,
            _ => self.committed.load(Ordering::Relaxed),
        };
        if let Some(line) = describe(event, committed, self.tasks.load(Ordering::Relaxed)) {
            println!("{line}");
        }
    }

    /// The commits accounted for so far.
    #[cfg(test)]
    fn committed(&self) -> usize {
        self.committed.load(Ordering::Relaxed)
    }
}

/// Renders the status block, one aligned `name  value` line per field.
pub fn status_block(status: &RunStatus) -> String {
    let state = match &status.state {
        RunState::InProgress => "in progress".to_string(),
        RunState::Finalized => "finalized".to_string(),
        RunState::Failed { task, reason } => {
            format!("failed on {}: {reason}", short(task))
        }
        RunState::Interrupted => "interrupted".to_string(),
    };
    let block = format!(
        "run                  {}\n\
         state                {state}\n\
         tasks                {}\n\
         committed            {}\n\
         retried              {}\n\
         rejected             {}\n\
         faulted              {}\n\
         lease expired        {}\n\
         checkpoint degraded  {}",
        status.run,
        status.tasks,
        status.committed,
        status.retried,
        status.rejected,
        status.faulted,
        status.lease_expired,
        status.checkpoint_degraded,
    );
    match devices_line(status) {
        Some(devices) => format!("{block}\ndevices              {devices}"),
        None => block,
    }
}

/// The run's device composition: committed tasks per device, busiest first,
/// and the chains that moved when their device went absent.
///
/// `None` when the journal names no device, as a run whose domain uses none
/// does. Nothing is inferred: what is not in the journal is not printed.
fn devices_line(status: &RunStatus) -> Option<String> {
    if status.devices.is_empty() {
        return None;
    }
    let mut composition: Vec<(&String, &usize)> = status.devices.iter().collect();
    // Busiest first; the name breaks ties, so one journal renders one way.
    composition.sort_by(|(a_name, a_count), (b_name, b_count)| {
        b_count.cmp(a_count).then(a_name.cmp(b_name))
    });
    let mut line = composition
        .iter()
        .map(|(name, count)| format!("{name} ×{count}"))
        .collect::<Vec<String>>()
        .join(", ");
    if status.rebound_chains > 0 {
        line.push_str(&format!(", rebound chains: {}", status.rebound_chains));
    }
    Some(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_truncates_long_ids_and_keeps_short_ones() {
        assert_eq!(short(&"ab".repeat(32)), "abababababab");
        assert_eq!(short("abcd"), "abcd");
    }

    #[test]
    fn a_run_started_line_reports_prior_commits_when_resuming() {
        // The started line names the prior commits so a resumed session does
        // not read as a restart. A fresh run keeps the bare form.
        let event = Event::RunStarted {
            run: "ab".repeat(32),
            tasks: 200,
            committed: 26,
        };
        assert_eq!(
            describe(&event, 26, 200).expect("a started line"),
            "started: 200 tasks, 26 already committed"
        );
        assert_eq!(
            describe(&event, 0, 200).expect("a started line"),
            "started: 200 tasks"
        );
    }

    /// Wraps an event as the unstamped record the tests feed the renderer.
    fn rec(event: Event) -> Record {
        Record { ts_ms: None, event }
    }

    #[test]
    fn a_session_counts_commits_on_from_the_started_event() {
        let progress = Progress::new();
        progress.event(&rec(Event::RunStarted {
            run: "ab".repeat(32),
            tasks: 3,
            committed: 2,
        }));
        // The count is the event's, which the run derived from its store —
        // the journal replay this once read from can lag its own records.
        assert_eq!(progress.committed(), 2);

        progress.event(&rec(Event::Committed {
            task: "cd".repeat(32),
            record: "ef".repeat(32),
            stats_hex: String::new(),
        }));
        assert_eq!(progress.committed(), 3);
    }

    #[test]
    fn a_degraded_checkpoint_renders_a_line() {
        let event = Event::CheckpointDegraded {
            task: "ab".repeat(32),
            error: "checkpoint dir is unwritable".to_string(),
        };
        let line = describe(&event, 0, 0).expect("a degraded checkpoint warrants a line");
        assert!(line.contains("checkpoint degraded"), "{line}");
        assert!(line.contains("unwritable"), "{line}");
    }

    /// A diagnostic event over `level`, attributed to worker 3.
    fn diagnostic(level: sima_pipeline::Level, source: &str, message: &str) -> Event {
        Event::Diagnostic {
            level,
            source: source.to_string(),
            message: message.to_string(),
            worker: Some(3),
            host: None,
            task: None,
        }
    }

    #[test]
    fn a_warn_or_error_diagnostic_renders_level_source_and_worker() {
        let warn = diagnostic(
            sima_pipeline::Level::Warn,
            "transport",
            "undecodable event frame",
        );
        let line = describe(&warn, 0, 0).expect("a warn diagnostic warrants a line");
        for part in ["warn", "transport", "worker 3", "undecodable event frame"] {
            assert!(line.contains(part), "missing {part}: {line}");
        }
        let error = diagnostic(sima_pipeline::Level::Error, "panic", "thread panicked");
        let line = describe(&error, 0, 0).expect("an error diagnostic warrants a line");
        for part in ["error", "panic", "worker 3"] {
            assert!(line.contains(part), "missing {part}: {line}");
        }
    }

    #[test]
    fn an_info_diagnostic_renders_nothing() {
        // Worker stderr is journaled, not echoed: the run's console output
        // stays clean.
        let info = diagnostic(sima_pipeline::Level::Info, "worker stderr", "starting up");
        assert!(describe(&info, 0, 0).is_none());
    }

    /// A zeroed status for a throwaway run; tests set the fields they assert.
    fn a_status() -> RunStatus {
        RunStatus::new(sima_model::RunId::from_hash(sima_core::hash_bytes(
            b"a run to render",
        )))
    }

    #[test]
    fn the_status_block_names_every_field() {
        let mut status = a_status();
        status.tasks = 3;
        status.committed = 2;
        status.retried = 1;
        let block = status_block(&status);
        for field in [
            "run",
            "state",
            "tasks",
            "committed",
            "retried",
            "rejected",
            "faulted",
            "lease expired",
            "checkpoint degraded",
        ] {
            assert!(block.contains(field), "missing {field}: {block}");
        }
        assert!(block.contains("in progress"));
    }

    #[test]
    fn the_status_block_reports_the_run_s_device_composition() {
        let mut status = a_status();
        status.committed = 1000;
        status.devices = [
            ("Intel Arc 140T".to_string(), 388),
            ("NVIDIA RTX PRO 2000".to_string(), 612),
        ]
        .into_iter()
        .collect();
        status.rebound_chains = 2;
        let block = status_block(&status);
        // Busiest device first, whatever order the map holds.
        assert!(
            block.contains("devices              NVIDIA RTX PRO 2000 ×612, Intel Arc 140T ×388, rebound chains: 2"),
            "{block}"
        );
    }

    #[test]
    fn the_composition_shows_the_host_for_remote_pools() {
        // One device name on a local pool and a remote one: the remote entry
        // reads `device @ host`, the local one the plain name, busiest first.
        let mut status = a_status();
        status.committed = 1000;
        status.devices = [
            ("NVIDIA RTX PRO 2000".to_string(), 612),
            ("NVIDIA RTX PRO 2000 @ gpubox".to_string(), 388),
        ]
        .into_iter()
        .collect();
        let block = status_block(&status);
        assert!(
            block.contains(
                "devices              NVIDIA RTX PRO 2000 ×612, NVIDIA RTX PRO 2000 @ gpubox ×388"
            ),
            "{block}"
        );
    }

    #[test]
    fn a_run_that_moved_no_chain_reports_no_rebinds() {
        let mut status = a_status();
        status.devices = [("Intel Arc 140T".to_string(), 4)].into_iter().collect();
        let block = status_block(&status);
        assert!(
            block.contains("devices              Intel Arc 140T ×4"),
            "{block}"
        );
        assert!(!block.contains("rebound"), "{block}");
    }

    #[test]
    fn a_run_that_names_no_device_renders_no_device_line() {
        // A journal carrying no WorkerBound events, as a run whose domain uses
        // no device writes: there is nothing truthful to print.
        let block = status_block(&a_status());
        assert!(!block.contains("devices"), "{block}");
    }
}
