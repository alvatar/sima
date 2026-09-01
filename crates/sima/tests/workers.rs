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
use sima_pipeline::{Event, load};
use sima_store::Store;

/// Spawns `sima search` over `config` with its output discarded — the store
/// and the process table carry the assertions.
fn spawn_run(config: &Path) -> Child {
    sima_command()
        .args(["search", config.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sima")
}

/// `attempt_timeout` is enforced end to end: a sleeping task is preempted at
/// the deadline — the journal records `LeaseExpired` before the retry — and
/// exhausting the attempts fails the search with the definitive-failure exit
/// code.
#[test]
fn preemption_kills_an_overrunning_attempt_and_exhausts_the_search() {
    let dir = tempfile::tempdir().expect("temp dir");
    let text = r#"
        [search]
        root_seed = 11
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = ["sleep:60000"]

        [config]
        store = "./store"
        max_attempts = 2
        attempt_timeout_ms = 150

        [orchestrator]
        workers = 1
    "#;
    let config = common::write_config_text(dir.path(), "preempt.toml", text);

    let status = spawn_run(&config).wait().expect("wait for the search");
    assert_eq!(status.code(), Some(2), "a definitive failure exits 2");

    let events = journal_events(&config);
    let count = |probe: fn(&Event) -> bool| events.iter().filter(|e| probe(e)).count();
    // Both attempts expired and failed transiently; the first was retried,
    // the second exhausted the cap; nothing committed and the search failed.
    assert_eq!(
        count(|e| matches!(e, Event::LeaseExpired { .. })),
        2,
        "each attempt journals its expiry: {events:?}"
    );
    assert_eq!(count(|e| matches!(e, Event::Failed { .. })), 2);
    assert_eq!(count(|e| matches!(e, Event::Retried { .. })), 1);
    assert_eq!(count(|e| matches!(e, Event::Committed { .. })), 0);
    assert!(
        matches!(events.last(), Some(Event::SearchFailed { .. })),
        "the journal closes with search_failed: {events:?}"
    );
    // The expiry precedes the retry, per the journal's lifecycle order.
    let expired = events
        .iter()
        .position(|e| matches!(e, Event::LeaseExpired { .. }))
        .expect("an expiry");
    let retried = events
        .iter()
        .position(|e| matches!(e, Event::Retried { .. }))
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
        .filter(|e| matches!(e, Event::Leased { .. }))
        .count()
}

/// A `sima.toml` whose sleeps keep workers busy long enough for the process
/// table to be inspected mid-search. `workers` is operational, never identity:
/// a reference search matches whatever count its counterpart used.
fn write_sleep_config(dir: &Path, name: &str, store: &str, sleep_ms: u64, workers: u32) -> PathBuf {
    let text = format!(
        r#"
        [search]
        root_seed = 11
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = ["sleep:{sleep_ms}", "sleep:{sleep_ms}"]

        [config]
        store = "{store}"
        max_attempts = 3

        [orchestrator]
        workers = {workers}
    "#
    );
    common::write_config_text(dir, name, &text)
}

/// A worker SIGKILLed from outside mid-search converges: the search retries the
/// lost attempt on a replacement child, finalizes, and its manifest equals
/// an undisturbed reference search's.
#[test]
fn an_externally_killed_worker_converges_to_the_reference_manifest() {
    let dir = tempfile::tempdir().expect("temp dir");

    // The sleep duration is identity-bearing, so the reference search shares it
    // exactly; it is long enough that the kill below lands mid-attempt.
    let reference = write_sleep_config(dir.path(), "reference.toml", "./store-ref", 2000, 1);
    assert_eq!(
        spawn_run(&reference)
            .wait()
            .expect("reference search")
            .code(),
        Some(0)
    );
    let reference = manifest_of(&reference).expect("reference manifest");

    // One worker, so the child in the process table IS the leased child. With
    // two, the pid picked below could be the sibling still inside its
    // handshake — a child dying there is a spawn failure, which faults the
    // search: an infrastructure fault, not this test's path.
    let config = write_sleep_config(dir.path(), "killed.toml", "./store-killed", 2000, 1);
    let mut search = spawn_run(&config);
    // Wait for the worker child, then for an assignment to land — a `Leased`
    // event in the journal — so the SIGKILL lands inside the attempt.
    assert!(
        poll_until(Duration::from_secs(30), || {
            !worker_processes(search.id()).is_empty()
        }),
        "no worker child appeared"
    );
    assert!(
        poll_until(Duration::from_secs(30), || leased(&config) >= 1),
        "no assignment was leased"
    );
    let victim = *worker_processes(search.id())
        .first()
        .expect("a live worker child");
    // Safety: victim is a live child of the search process just read from the
    // process table; SIGKILL to it has no memory-safety conditions.
    unsafe {
        libc::kill(victim as i32, libc::SIGKILL);
    }

    let status = search.wait().expect("wait for the search");
    assert_eq!(
        status.code(),
        Some(0),
        "the search survives the worker death"
    );
    assert_eq!(
        manifest_of(&config).as_ref(),
        Some(&reference),
        "the converged manifest equals the reference"
    );
    // The death was observed and retried, so the convergence is not a kill
    // that missed its window.
    let events = journal_events(&config);
    assert!(
        events.iter().any(|e| matches!(e, Event::Failed { .. })),
        "the worker death journals a transient failure: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(e, Event::Retried { .. })),
        "the lost attempt is retried: {events:?}"
    );
}

/// No worker outlives its parent: with the orchestrator SIGKILLed mid-search,
/// every `sima-worker` child exits within a deadline, the kernel has
/// released the search lock, and a resumed search converges to the reference
/// manifest.
#[test]
fn workers_die_with_their_parent_and_the_search_resumes() {
    let dir = tempfile::tempdir().expect("temp dir");

    // Sleeps far longer than the orphan deadline below, so a child that
    // survives its parent is caught: a sleeping executor never reads stdin,
    // so the end-of-stream fallback alone would only exit the child after
    // its sleep completes — well past the deadline. The duration is
    // identity-bearing, so the reference search shares it exactly.
    const SLEEP_MS: u64 = 6_000;

    let reference = write_sleep_config(dir.path(), "reference.toml", "./store-ref", SLEEP_MS, 2);
    assert_eq!(
        spawn_run(&reference)
            .wait()
            .expect("reference search")
            .code(),
        Some(0)
    );
    let reference = manifest_of(&reference).expect("reference manifest");

    let config = write_sleep_config(dir.path(), "orphaned.toml", "./store-orphaned", SLEEP_MS, 2);
    let mut search = spawn_run(&config);
    assert!(
        poll_until(Duration::from_secs(30), || {
            worker_processes(search.id()).len() == 2
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
    let workers = worker_processes(search.id());
    search.kill().expect("SIGKILL the orchestrator");
    search.wait().expect("reap the orchestrator");

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
            .acquire_search_lock(&loaded.search.id())
            .expect("the lock is free after the death"),
    );

    // Resume converges. The resumed search re-executes the abandoned sleeps —
    // they are identity-bearing, so this wait is the tasks' own duration,
    // not a correctness assumption.
    let status = spawn_run(&config).wait().expect("resumed search");
    assert_eq!(status.code(), Some(0), "the resumed search finalizes");
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
            [search]
            root_seed = 11
            format = "stub.v1"

            [search.generator]
            id = "stub.v1"
            behaviors = ["succeed", "flaky:1", "succeed", "succeed"]

            [config]
            store = "{store}"
            max_attempts = 3

            [orchestrator]
            workers = {workers}
        "#
        );
        common::write_config_text(dir.path(), name, &text)
    };
    let single = write("single.toml", "./store-single", 1);
    let many = write("many.toml", "./store-many", 4);

    assert_eq!(spawn_run(&single).wait().expect("search").code(), Some(0));
    assert_eq!(spawn_run(&many).wait().expect("search").code(), Some(0));
    assert_eq!(
        manifest_of(&single).expect("single-worker manifest"),
        manifest_of(&many).expect("four-worker manifest"),
    );
}
