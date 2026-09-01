//! Task-lifecycle acceptance: retry, definitive failure, rejection, panic
//! isolation, resume, and attempt-deadline preemption.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{
    committed_count, config, exec, exec_with_timeout, failed_count, faulted_count, journal_events,
    lease_expired_count, leased_count, rejected_count, retried_count, run_into, run_with,
    search_id, task_keys, temp_store,
};
use sima_contracts::{Checkpoint, ExecutionContext, Executor, Outcome, TaskInput};
use sima_core::{Error, Result};
use sima_domains::{StubBehavior, StubExecutor, StubProgram};
use sima_model::{FormatId, SearchConfig};
use sima_scheduler::{Event, SearchOutcome};
use sima_store::Store;
use sima_transport::loopback::SharedResolver;

/// A flaky candidate fails, is retried, and finally commits; the committed
/// record is deterministic, so the same config into a second fresh store
/// produces the identical manifest regardless of how many attempts it took.
#[test]
fn flaky_task_retries_then_commits() -> Result<()> {
    let cfg = config(1, vec![StubBehavior::Flaky(2)]);
    let exec = exec(1, 3, 1_000);
    let key = task_keys(&cfg)[0];

    let (_dir, store) = temp_store();
    assert!(matches!(
        run_into(&store, &cfg, &exec)?,
        SearchOutcome::Finalized { .. }
    ));

    let search = search_id(&cfg);
    let events = journal_events(&store, &search);
    // Two failed attempts, each re-enqueued, then one commit; never rejected.
    assert_eq!(failed_count(&events, &key), 2);
    assert_eq!(retried_count(&events, &key), 2);
    assert_eq!(committed_count(&events, &key), 1);
    assert_eq!(rejected_count(&events, &key), 0);

    let manifest = store.manifest(&search)?.expect("finalized manifest");
    assert_eq!(manifest.entries.len(), 1);

    // Attempt-independence: a second fresh search commits the identical record.
    let (_other, other) = temp_store();
    run_into(&other, &cfg, &exec)?;
    assert_eq!(manifest, other.manifest(&search)?.expect("second manifest"));
    Ok(())
}

/// A flaky candidate that never recovers exhausts its retries and fails the
/// search definitively: no manifest is written, yet a sibling that committed
/// stays committed, so the store is resumable.
#[test]
fn exhausted_retries_fail_the_search_and_leave_the_store_resumable() -> Result<()> {
    let cfg = config(2, vec![StubBehavior::Succeed, StubBehavior::Flaky(5)]);
    let keys = task_keys(&cfg);
    let (succeed_key, flaky_key) = (keys[0], keys[1]);

    let (_dir, store) = temp_store();
    match run_into(&store, &cfg, &exec(1, 3, 1_000))? {
        SearchOutcome::Failed { task, .. } => assert_eq!(task, flaky_key),
        other => panic!("expected Failed, got {other:?}"),
    }

    let search = search_id(&cfg);
    // No manifest: the search did not finalize.
    assert!(store.manifest(&search)?.is_none());
    // The sibling that committed remains committed — resumable progress.
    assert!(store.record(&succeed_key)?.is_some());
    Ok(())
}

/// A rejected candidate terminates the search immediately, with no retry.
#[test]
fn a_rejected_candidate_terminates_without_retry() -> Result<()> {
    let cfg = config(3, vec![StubBehavior::Reject]);
    let key = task_keys(&cfg)[0];

    let (_dir, store) = temp_store();
    match run_into(&store, &cfg, &exec(1, 3, 1_000))? {
        SearchOutcome::Failed { task, .. } => assert_eq!(task, key),
        other => panic!("expected Failed, got {other:?}"),
    }

    let search = search_id(&cfg);
    let events = journal_events(&store, &search);
    // Exactly one rejection, at the first attempt, and never retried or failed.
    assert_eq!(rejected_count(&events, &key), 1);
    assert_eq!(retried_count(&events, &key), 0);
    assert_eq!(failed_count(&events, &key), 0);
    assert!(store.manifest(&search)?.is_none());
    Ok(())
}

/// A panic inside the program is caught, classified as a rejection, and
/// isolated: the search returns `Ok(Failed)` rather than unwinding, and a sibling
/// that committed is unaffected.
#[test]
fn a_panic_is_isolated_and_classified_as_a_rejection() -> Result<()> {
    let cfg = config(4, vec![StubBehavior::Succeed, StubBehavior::Panic]);
    let keys = task_keys(&cfg);
    let (succeed_key, panic_key) = (keys[0], keys[1]);

    let (_dir, store) = temp_store();
    // The call returns instead of unwinding — that return is the isolation.
    match run_into(&store, &cfg, &exec(1, 3, 1_000))? {
        SearchOutcome::Failed { task, .. } => assert_eq!(task, panic_key),
        other => panic!("expected Failed, got {other:?}"),
    }

    let search = search_id(&cfg);
    let events = journal_events(&store, &search);
    assert_eq!(rejected_count(&events, &panic_key), 1);
    // The rejection reason preserves the panic payload.
    let reason = events
        .iter()
        .find_map(|e| match e {
            Event::Rejected { task, reason, .. } if *task == panic_key.to_string() => {
                Some(reason.clone())
            }
            _ => None,
        })
        .expect("a rejected event for the panic task");
    assert!(reason.starts_with("panic:"), "{reason}");
    // The sibling that committed is unaffected.
    assert!(store.record(&succeed_key)?.is_some());
    Ok(())
}

/// Resume re-runs only the unfinished work. A batch where task Y never recovers
/// fails with task X committed. Re-running the same config with Y fixed — which
/// changes Y's spec, and so Y's key, the intended "evaluate the fixed candidate again"
/// path — finalizes, and X, already committed under its unchanged key, is
/// skipped by the frontier and never re-executed.
#[test]
fn resume_reruns_only_the_unfinished_work() -> Result<()> {
    let failing = config(5, vec![StubBehavior::Succeed, StubBehavior::Flaky(5)]);
    let fixed = config(5, vec![StubBehavior::Succeed, StubBehavior::Succeed]);

    // X is candidate 0 in both, so its key is stable; fixing Y changes its
    // spec and thus its key.
    let x_key = task_keys(&failing)[0];
    assert_eq!(x_key, task_keys(&fixed)[0], "X's key must be stable");
    assert_ne!(
        task_keys(&failing)[1],
        task_keys(&fixed)[1],
        "fixing Y changes its key"
    );
    let fixed_y_key = task_keys(&fixed)[1];

    let (_dir, store) = temp_store();
    // First search fails; X commits.
    assert!(matches!(
        run_into(&store, &failing, &exec(1, 3, 1_000))?,
        SearchOutcome::Failed { .. }
    ));
    assert!(store.record(&x_key)?.is_some());

    // Run the fixed config into the same store again: it finalizes.
    assert!(matches!(
        run_into(&store, &fixed, &exec(1, 3, 1_000))?,
        SearchOutcome::Finalized { .. }
    ));

    let fixed_search = search_id(&fixed);
    let events = journal_events(&store, &fixed_search);
    // X was already committed, so the fixed search never leases it; only the
    // fixed Y runs.
    assert_eq!(leased_count(&events, &x_key), 0);
    assert_eq!(committed_count(&events, &fixed_y_key), 1);
    // The manifest covers both tasks: X carried over, fixed Y freshly committed.
    let manifest = store
        .manifest(&fixed_search)?
        .expect("fixed search finalized");
    let tasks: Vec<_> = manifest.entries.iter().map(|e| e.task).collect();
    assert!(tasks.contains(&x_key));
    assert!(tasks.contains(&fixed_y_key));
    Ok(())
}

/// A search reports the commits its store already answers, so a resumed session
/// counts on from them instead of reading as a restart. The count comes from
/// the frontier derivation — the same store reading that decides what to run
/// — so it is exact even against a journal that a crash left short of the
/// records it describes.
#[test]
fn a_search_reports_the_commits_its_store_already_holds() -> Result<()> {
    let cfg = config(9, vec![StubBehavior::Succeed, StubBehavior::Succeed]);
    let (_dir, store) = temp_store();
    assert!(matches!(
        run_into(&store, &cfg, &exec(2, 3, 1_000))?,
        SearchOutcome::Finalized { .. }
    ));
    // The same config over the same store: every task is already answered.
    assert!(matches!(
        run_into(&store, &cfg, &exec(2, 3, 1_000))?,
        SearchOutcome::Finalized { .. }
    ));

    // Both sessions appended to one journal, in order.
    let started: Vec<(usize, usize)> = journal_events(&store, &search_id(&cfg))
        .iter()
        .filter_map(|e| match e {
            Event::SearchStarted {
                tasks, committed, ..
            } => Some((*tasks, *committed)),
            _ => None,
        })
        .collect();
    assert_eq!(
        started,
        vec![(2, 0), (2, 2)],
        "a fresh store answers nothing; the second session finds both commits"
    );
    Ok(())
}

/// `attempt_timeout` is enforced: a task outliving it is preempted — the
/// journal records the lease expiry before each retry — and once the
/// attempts are exhausted the search fails definitively, never committing.
#[test]
fn an_overrunning_task_is_preempted_and_exhausts_its_attempts() -> Result<()> {
    // Each attempt sleeps far past the timeout, so every one is preempted.
    let cfg = config(6, vec![StubBehavior::Sleep(400)]);
    let key = task_keys(&cfg)[0];

    let (_dir, store) = temp_store();
    match run_into(&store, &cfg, &exec(1, 2, 30))? {
        SearchOutcome::Failed { task, .. } => assert_eq!(task, key),
        other => panic!("expected Failed, got {other:?}"),
    }

    let search = search_id(&cfg);
    let events = journal_events(&store, &search);
    // Every attempt expired and failed transiently; the first was retried,
    // the second exhausted the cap; nothing committed.
    assert_eq!(lease_expired_count(&events, &key), 2);
    assert_eq!(failed_count(&events, &key), 2);
    assert_eq!(retried_count(&events, &key), 1);
    assert_eq!(committed_count(&events, &key), 0);
    assert!(store.manifest(&search)?.is_none());
    Ok(())
}

/// An unbounded attempt timeout (`Duration::MAX`) disables preemption
/// without breaking the search: no deadline lands on the clock, so no arithmetic
/// overflows. The task completes and the search finalizes.
#[test]
fn an_unbounded_timeout_finalizes_without_expiry_reports() -> Result<()> {
    let cfg = config(9, vec![StubBehavior::Sleep(10)]);
    let key = task_keys(&cfg)[0];

    let (_dir, store) = temp_store();
    assert!(matches!(
        run_into(&store, &cfg, &exec_with_timeout(1, 1, Duration::MAX))?,
        SearchOutcome::Finalized { .. }
    ));

    let search = search_id(&cfg);
    let events = journal_events(&store, &search);
    assert_eq!(lease_expired_count(&events, &key), 0);
    Ok(())
}

/// Each task's `Queued` event is journaled before its first `Leased` event —
/// the per-task order the event vocabulary promises. The guarantee is
/// structural: enqueue emits `Queued` before it publishes the task to the
/// queue, so no woken worker can journal a `Leased` ahead of the driver's
/// `Queued`. The search repeats ten times so that an ordering violation, were one
/// possible, would have room to surface across many worker interleavings.
#[test]
fn queued_is_journaled_before_the_first_lease() -> Result<()> {
    let cfg = config(13, vec![StubBehavior::Succeed; 16]);
    let keys = task_keys(&cfg);
    for _ in 0..10 {
        let (_dir, store) = temp_store();
        assert!(matches!(
            run_into(&store, &cfg, &exec(8, 1, 1_000))?,
            SearchOutcome::Finalized { .. }
        ));
        let events = journal_events(&store, &search_id(&cfg));
        for key in &keys {
            let task = key.to_string();
            let queued = events
                .iter()
                .position(|e| matches!(e, Event::Queued { task: t } if *t == task))
                .expect("a Queued event for each task");
            let leased = events
                .iter()
                .position(|e| matches!(e, Event::Leased { task: t, .. } if *t == task))
                .expect("a Leased event for each task");
            assert!(
                queued < leased,
                "Queued must precede Leased for task {task}"
            );
        }
    }
    Ok(())
}

/// Every journal line the search writes is a parseable record carrying the
/// collector's wall-clock stamp.
#[test]
fn every_journal_line_carries_a_timestamp() -> Result<()> {
    let cfg = config(14, vec![StubBehavior::Succeed, StubBehavior::Succeed]);
    let (_dir, store) = temp_store();
    assert!(matches!(
        run_into(&store, &cfg, &exec(2, 3, 1_000))?,
        SearchOutcome::Finalized { .. }
    ));
    for line in store.journal(&search_id(&cfg))? {
        let record = sima_trace::Record::from_line(&line)?;
        assert!(record.ts_ms > 0, "unstamped line: {line}");
    }
    Ok(())
}

/// A long attempt timeout never delays the search: the deadline bounds the wait
/// on the worker link, and an outcome arriving early settles immediately.
/// The test finishing within the suite's normal runtime is the guard.
#[test]
fn a_long_timeout_does_not_delay_the_search() -> Result<()> {
    let cfg = config(12, vec![StubBehavior::Succeed, StubBehavior::Succeed]);
    let (_dir, store) = temp_store();
    assert!(matches!(
        run_into(
            &store,
            &cfg,
            &exec_with_timeout(2, 1, Duration::from_secs(3600))
        )?,
        SearchOutcome::Finalized { .. }
    ));
    Ok(())
}

/// An executor that raises an infrastructure fault. It delegates a `Succeed`
/// program to the stub — so siblings still commit — and returns `Err` for any
/// other behavior, modelling a store fault or a structurally invalid spec the
/// stub generator cannot produce on its own.
struct FaultyExecutor {
    inner: StubExecutor,
    format: FormatId,
}

impl FaultyExecutor {
    fn new() -> FaultyExecutor {
        FaultyExecutor {
            inner: StubExecutor::new().expect("stub executor"),
            format: FormatId::new("stub.v1").expect("format id"),
        }
    }
}

/// The loopback resolver serving a fresh [`FaultyExecutor`] per worker.
fn faulty_resolver() -> SharedResolver {
    Arc::new(|_, _| {
        let executor: Box<dyn Executor> = Box::new(FaultyExecutor::new());
        Ok((executor, String::new(), String::new()))
    })
}

impl Executor for FaultyExecutor {
    fn format(&self) -> &FormatId {
        &self.format
    }

    fn execute(
        &self,
        input: &TaskInput<'_>,
        ctx: &ExecutionContext,
        checkpoint: &dyn Checkpoint,
    ) -> Result<Outcome> {
        let program = StubProgram::from_bytes(&input.spec.bytes)?;
        if matches!(program.behavior, StubBehavior::Succeed) {
            self.inner.execute(input, ctx, checkpoint)
        } else {
            Err(Error::Validation(
                "injected infrastructure fault".to_string(),
            ))
        }
    }
}

/// An infrastructure fault from the executor fails the whole search with `Err`,
/// distinct from a candidate that merely evaluated badly, and writes no
/// manifest.
#[test]
fn an_executor_fault_fails_the_search_with_an_error() -> Result<()> {
    let cfg = config(7, vec![StubBehavior::Reject]);
    let key = task_keys(&cfg)[0];
    let (_dir, store) = temp_store();
    // A fault crossing the wire surfaces as `Reported`, carrying the child's
    // own rendering verbatim: the executor's `Error::Validation` reaches the
    // parent as "validation error: ...", classification intact (f_008).
    match run_with(&store, &cfg, &exec(1, 3, 1_000), faulty_resolver()) {
        Err(Error::Reported(msg)) => {
            assert_eq!(msg, "validation error: injected infrastructure fault");
        }
        Err(other) => panic!("expected a reported wire fault, got {other}"),
        Ok(_) => panic!("expected an infrastructure fault, got a search outcome"),
    }
    // No manifest: the faulted search did not finalize.
    assert!(store.manifest(&search_id(&cfg))?.is_none());
    // The fault is journaled once, so it is not hidden behind the search error.
    let events = journal_events(&store, &search_id(&cfg));
    assert_eq!(faulted_count(&events, &key), 1);
    Ok(())
}

/// A fault leaves the store clean and resumable: a sibling that committed
/// before the fault survives, and no manifest is written.
#[test]
fn a_fault_preserves_already_committed_siblings() -> Result<()> {
    let cfg = config(8, vec![StubBehavior::Succeed, StubBehavior::Reject]);
    let keys = task_keys(&cfg);
    let (succeed_key, fault_key) = (keys[0], keys[1]);

    let (_dir, store) = temp_store();
    // One worker in FIFO order commits the Succeed sibling before reaching the
    // faulting candidate.
    match run_with(&store, &cfg, &exec(1, 3, 1_000), faulty_resolver()) {
        Err(Error::Reported(_)) => {}
        Err(other) => panic!("expected a reported wire fault, got {other}"),
        Ok(_) => panic!("expected an infrastructure fault, got a search outcome"),
    }

    assert!(store.manifest(&search_id(&cfg))?.is_none());
    // The sibling committed before the fault remains committed.
    assert!(store.record(&succeed_key)?.is_some());
    // The faulting candidate never committed.
    assert!(store.record(&fault_key)?.is_none());
    Ok(())
}

/// A fresh store whose journal for `cfg` is a symlink to `/dev/full`: the
/// writer opens it, but every append fails with `ENOSPC`, so the journal sink
/// holds an error from the first event. The search directory is created first so
/// `search()`'s own `create_search` is a reopen.
fn store_with_a_dead_journal(cfg: &SearchConfig) -> Result<(tempfile::TempDir, Store)> {
    let (dir, store) = temp_store();
    store.create_search(cfg)?;
    let journal = dir
        .path()
        .join("searches")
        .join(search_id(cfg).to_string())
        .join("journal");
    std::os::unix::fs::symlink("/dev/full", &journal).expect("symlink journal to /dev/full");
    Ok((dir, store))
}

/// A definitive candidate failure is returned even when the journal degraded:
/// the domain outcome is the truth, and the journal fault resurfaces on the
/// next search that finalizes over the store.
#[test]
fn a_journal_fault_yields_to_a_domain_failed_outcome() -> Result<()> {
    let cfg = config(10, vec![StubBehavior::Reject]);
    let (_dir, store) = store_with_a_dead_journal(&cfg)?;
    match run_into(&store, &cfg, &exec(1, 3, 1_000)) {
        Ok(SearchOutcome::Failed { .. }) => {}
        Ok(other) => panic!("expected Failed, got {other:?}"),
        Err(e) => panic!("expected Ok(Failed), got Err: {e}"),
    }
    Ok(())
}

/// A Finalized outcome, by contrast, yields to the journal fault: with the search
/// otherwise successful the journal error is the sole signal, and the manifest
/// is already written before it surfaces.
#[test]
fn a_finalized_search_surfaces_the_journal_fault() -> Result<()> {
    let cfg = config(11, vec![StubBehavior::Succeed]);
    let (_dir, store) = store_with_a_dead_journal(&cfg)?;
    match run_into(&store, &cfg, &exec(1, 3, 1_000)) {
        Err(Error::Io { .. }) => {}
        Err(other) => panic!("expected an io fault, got {other}"),
        Ok(_) => panic!("expected the journal fault to surface"),
    }
    // The finalize completed before the journal fault surfaced.
    assert!(store.manifest(&search_id(&cfg))?.is_some());
    Ok(())
}
