//! Subprocess-worker acceptance over the real binaries: preemption,
//! worker-death convergence, orphan protection, and parallelism equality.
//! Every wait polls to a deadline; no fixed sleep carries a correctness
//! assumption.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::time::Duration;

use common::{
    journal_events, manifest_of, poll_until, sima_command, worker_alive, worker_processes,
};
use sima_pipeline::{LifecycleEvent, load};
use sima_store::Store;

/// Spawns `sima run` over `config` with its output discarded — the store
/// and the process table carry the assertions.
fn spawn_run(config: &Path) -> Child {
    sima_command()
        .args(["run", config.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sima")
}

/// `attempt_timeout` is enforced end to end: a sleeping task is preempted at
/// the deadline — the journal records `LeaseExpired` before the retry — and
/// exhausting the attempts fails the run with the definitive-failure exit
/// code.
#[test]
fn preemption_kills_an_overrunning_attempt_and_exhausts_the_run() {
    let dir = tempfile::tempdir().expect("temp dir");
    let text = r#"
        [run]
        root_seed = 11
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["sleep:60000"]

        [execution]
        store = "./store"
        workers = 1
        max_attempts = 2
        attempt_timeout_ms = 150
    "#;
    let config = common::write_config_text(dir.path(), "preempt.toml", text);

    let status = spawn_run(&config).wait().expect("wait for the run");
    assert_eq!(status.code(), Some(2), "a definitive failure exits 2");

    let events = journal_events(&config);
    let count = |probe: fn(&LifecycleEvent) -> bool| events.iter().filter(|e| probe(e)).count();
    // Both attempts expired and failed transiently; the first was retried,
    // the second exhausted the cap; nothing committed and the run failed.
    assert_eq!(
        count(|e| matches!(e, LifecycleEvent::LeaseExpired { .. })),
        2,
        "each attempt journals its expiry: {events:?}"
    );
    assert_eq!(count(|e| matches!(e, LifecycleEvent::Failed { .. })), 2);
    assert_eq!(count(|e| matches!(e, LifecycleEvent::Retried { .. })), 1);
    assert_eq!(count(|e| matches!(e, LifecycleEvent::Committed { .. })), 0);
    assert!(
        matches!(events.last(), Some(LifecycleEvent::RunFailed { .. })),
        "the journal closes with run_failed: {events:?}"
    );
    // The expiry precedes the retry, per the journal's lifecycle order.
    let expired = events
        .iter()
        .position(|e| matches!(e, LifecycleEvent::LeaseExpired { .. }))
        .expect("an expiry");
    let retried = events
        .iter()
        .position(|e| matches!(e, LifecycleEvent::Retried { .. }))
        .expect("a retry");
    assert!(expired < retried, "LeaseExpired precedes Retried");
    assert!(manifest_of(&config).is_none(), "no manifest is written");
}

/// Number of `Leased` events in `config`'s journal so far. Leasing is
/// journaled as the assignment goes out, so the count is the poll signal
/// that assignments have landed on the workers.
fn leased(config: &Path) -> usize {
    journal_events(config)
        .iter()
        .filter(|e| matches!(e, LifecycleEvent::Leased { .. }))
        .count()
}

/// A `sima.toml` whose sleeps keep workers busy long enough for the process
/// table to be inspected mid-run.
fn write_sleep_config(dir: &Path, name: &str, store: &str, sleep_ms: u64) -> PathBuf {
    let text = format!(
        r#"
        [run]
        root_seed = 11
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["sleep:{sleep_ms}", "sleep:{sleep_ms}"]

        [execution]
        store = "{store}"
        workers = 2
        max_attempts = 3
    "#
    );
    common::write_config_text(dir, name, &text)
}

/// A worker SIGKILLed from outside mid-run converges: the run retries the
/// lost attempt on a replacement child, finalizes, and its manifest equals
/// an undisturbed reference run's.
#[test]
fn an_externally_killed_worker_converges_to_the_reference_manifest() {
    let dir = tempfile::tempdir().expect("temp dir");

    // The sleep duration is identity-bearing, so the reference run shares it
    // exactly; it is long enough that the kill below lands mid-attempt.
    let reference = write_sleep_config(dir.path(), "reference.toml", "./store-ref", 2000);
    assert_eq!(
        spawn_run(&reference).wait().expect("reference run").code(),
        Some(0)
    );
    let reference = manifest_of(&reference).expect("reference manifest");

    let config = write_sleep_config(dir.path(), "killed.toml", "./store-killed", 2000);
    let mut run = spawn_run(&config);
    // Wait for a worker child, then for an assignment to land — a `Leased`
    // event in the journal — so the SIGKILL cannot land inside spawn: a
    // spawn failure is an infrastructure fault, which is not this test's
    // path.
    assert!(
        poll_until(Duration::from_secs(30), || {
            !worker_processes(run.id()).is_empty()
        }),
        "no worker child appeared"
    );
    assert!(
        poll_until(Duration::from_secs(30), || leased(&config) >= 1),
        "no assignment was leased"
    );
    let victim = *worker_processes(run.id())
        .first()
        .expect("a live worker child");
    // Safety: victim is a live child of the run process just read from the
    // process table; SIGKILL to it has no memory-safety conditions.
    unsafe {
        libc::kill(victim as i32, libc::SIGKILL);
    }

    let status = run.wait().expect("wait for the run");
    assert_eq!(status.code(), Some(0), "the run survives the worker death");
    assert_eq!(
        manifest_of(&config).as_ref(),
        Some(&reference),
        "the converged manifest equals the reference"
    );
    // The death was observed and retried, so the convergence is not a kill
    // that missed its window.
    let events = journal_events(&config);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, LifecycleEvent::Failed { .. })),
        "the worker death journals a transient failure: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, LifecycleEvent::Retried { .. })),
        "the lost attempt is retried: {events:?}"
    );
}

/// No worker outlives its parent: with the orchestrator SIGKILLed mid-run,
/// every `sima-worker` child exits within a deadline, the kernel has
/// released the run lock, and a resumed run converges to the reference
/// manifest.
#[test]
fn workers_die_with_their_parent_and_the_run_resumes() {
    let dir = tempfile::tempdir().expect("temp dir");

    // Sleeps far longer than the orphan deadline below, so a child that
    // survives its parent is caught: a sleeping executor never reads stdin,
    // so the end-of-stream fallback alone would only exit the child after
    // its sleep completes — well past the deadline. The duration is
    // identity-bearing, so the reference run shares it exactly.
    const SLEEP_MS: u64 = 6_000;

    let reference = write_sleep_config(dir.path(), "reference.toml", "./store-ref", SLEEP_MS);
    assert_eq!(
        spawn_run(&reference).wait().expect("reference run").code(),
        Some(0)
    );
    let reference = manifest_of(&reference).expect("reference manifest");

    let config = write_sleep_config(dir.path(), "orphaned.toml", "./store-orphaned", SLEEP_MS);
    let mut run = spawn_run(&config);
    assert!(
        poll_until(Duration::from_secs(30), || {
            worker_processes(run.id()).len() == 2
        }),
        "both worker children appear"
    );
    // Wait for both assignments to land — two `Leased` events in the
    // journal — so the children are inside their sleeps when the parent
    // dies.
    assert!(
        poll_until(Duration::from_secs(30), || leased(&config) >= 2),
        "both assignments lease"
    );
    let workers = worker_processes(run.id());
    run.kill().expect("SIGKILL the orchestrator");
    run.wait().expect("reap the orchestrator");

    // PR_SET_PDEATHSIG delivers SIGKILL to each child with the parent's
    // death; the deadline is generous slack for the reaper, far below the
    // sleeps.
    assert!(
        poll_until(Duration::from_millis(2_500), || {
            workers.iter().all(|pid| !worker_alive(*pid))
        }),
        "a sima-worker outlived its parent: {workers:?}"
    );

    // The kernel released the orchestrator lock with the process.
    let loaded = load(&config).expect("load config");
    let store = Store::open(&loaded.store).expect("open store");
    drop(
        store
            .acquire_run_lock(&loaded.run.id())
            .expect("the lock is free after the death"),
    );

    // Resume converges. The resumed run re-executes the abandoned sleeps —
    // they are identity-bearing, so this wait is the tasks' own duration,
    // not a correctness assumption.
    let status = spawn_run(&config).wait().expect("resumed run");
    assert_eq!(status.code(), Some(0), "the resumed run finalizes");
    assert_eq!(
        manifest_of(&config).as_ref(),
        Some(&reference),
        "the resumed manifest equals the reference"
    );
}

/// The same identity section at `workers = 1` and `workers = 4` yields
/// byte-identical manifests: parallelism is operational, never identity.
#[test]
fn worker_count_never_reaches_the_manifest() {
    let dir = tempfile::tempdir().expect("temp dir");
    let write = |name: &str, store: &str, workers: u32| {
        let text = format!(
            r#"
            [run]
            root_seed = 11
            format = "stub.v1"

            [run.generator]
            id = "stub.v1"
            behaviors = ["succeed", "flaky:1", "succeed", "succeed"]

            [execution]
            store = "{store}"
            workers = {workers}
            max_attempts = 3
        "#
        );
        common::write_config_text(dir.path(), name, &text)
    };
    let single = write("single.toml", "./store-single", 1);
    let many = write("many.toml", "./store-many", 4);

    assert_eq!(spawn_run(&single).wait().expect("run").code(), Some(0));
    assert_eq!(spawn_run(&many).wait().expect("run").code(), Some(0));
    assert_eq!(
        manifest_of(&single).expect("single-worker manifest"),
        manifest_of(&many).expect("four-worker manifest"),
    );
}
