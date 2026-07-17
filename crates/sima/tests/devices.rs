//! Multi-GPU acceptance over the real binaries and real devices: a search
//! spread across unequal cards, chain stickiness across a resume, a rebind
//! when a card is gone, and single-device determinism unchanged.
//!
//! Every test here needs the machine's GPUs, so all are `#[ignore]` like the
//! other device suites. Run them where the hardware is:
//!
//! ```text
//! cargo test -p sima --test devices -- --ignored
//! ```
//!
//! The placement rule itself is proven device-free in the scheduler's own
//! tests; what these prove is that the whole path — config, resolution,
//! protocol, workers, store — carries it onto real hardware.

mod common;

use std::collections::HashSet;
use std::path::Path;
use std::process::{Child, Stdio};
use std::time::Duration;

use common::{
    chain_keys, journal_events, manifest_of, poll_until, sima_command, task_devices, worker_devices,
};
use sima_pipeline::LifecycleEvent;

/// A Gray-Scott config over `devices` — the rendered `[[execution.device]]`
/// entries — with `count` candidates, each divided into `segments`.
fn config_text(store: &str, count: u32, segments: u64, devices: &str) -> String {
    format!(
        r#"
        [run]
        root_seed = 42
        format = "ca_evolution.gray_scott.v1"
        segments = {segments}

        [run.generator]
        id = "ca_evolution.gray_scott.v1"
        count = {count}
        feed = [0.050, 0.058]
        kill = [0.062, 0.062]
        diffusion_u = [0.16, 0.16]
        diffusion_v = [0.08, 0.08]

        [run.params]
        width = 32
        height = 32
        steps = 40
        dt = 1.0
        base_u = 0.5
        base_v = 0.25
        side_divisor = 8
        noise_width = 0.02

        [execution]
        store = "{store}"
        max_attempts = 3
        {devices}
    "#
    )
}

/// Both of this machine's classes, two workers each.
const BOTH_DEVICES: &str = r#"
        [[execution.device]]
        select = "nvidia"
        workers = 2

        [[execution.device]]
        select = "intel"
        workers = 2
    "#;

/// The NVIDIA class alone, two workers.
const NVIDIA_ONLY: &str = r#"
        [[execution.device]]
        select = "nvidia"
        workers = 2
    "#;

/// Spawns `sima run` over `config`, output discarded.
fn spawn_run(config: &Path) -> Child {
    sima_command()
        .args(["run", config.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sima")
}

/// Runs `config` to completion, asserting it finalized.
fn run_to_completion(config: &Path) {
    let status = spawn_run(config).wait().expect("wait for the run");
    assert_eq!(status.code(), Some(0), "the run finalized");
}

/// The distinct devices the run's committed work ran on.
fn devices_used(events: &[LifecycleEvent]) -> HashSet<String> {
    worker_devices(events)
        .into_values()
        .filter(|device| !device.is_empty())
        .collect()
}

/// Asserts every chain ran its segments on one device, and returns how many
/// chains each device carried.
fn assert_chains_never_split(config: &Path) -> Vec<(String, usize)> {
    let events = journal_events(config);
    let ran_on = task_devices(&events);
    let mut per_device: Vec<(String, usize)> = Vec::new();
    for (chain, keys) in chain_keys(config).iter().enumerate() {
        let devices: HashSet<&String> = keys
            .iter()
            .filter_map(|key| ran_on.get(key))
            .flatten()
            .collect();
        // A chain whose segments were all committed by an earlier session
        // contributes no lease to this journal; one that ran must have run in
        // one place.
        assert!(
            devices.len() <= 1,
            "chain {chain} split across devices: {devices:?}"
        );
        if let Some(device) = devices.into_iter().next() {
            match per_device.iter_mut().find(|(name, _)| name == device) {
                Some((_, count)) => *count += 1,
                None => per_device.push((device.clone(), 1)),
            }
        }
    }
    per_device
}

/// A Gray-Scott search over both of this machine's GPUs completes, uses both,
/// and keeps every chain on one of them.
#[test]
#[ignore = "requires both GPUs"]
fn a_search_over_two_device_classes_uses_both_and_splits_no_chain() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = common::write_config_text(
        dir.path(),
        "both.toml",
        &config_text("./store", 8, 2, BOTH_DEVICES),
    );
    run_to_completion(&config);

    let events = journal_events(&config);
    let used = devices_used(&events);
    assert_eq!(
        used.len(),
        2,
        "both device classes carried workers: {used:?}"
    );
    let per_device = assert_chains_never_split(&config);
    assert_eq!(
        per_device.iter().map(|(_, n)| n).sum::<usize>(),
        8,
        "every chain ran"
    );
    // Greedy placement puts work on both: neither class sat idle while the
    // other did all eight chains.
    assert_eq!(
        per_device.len(),
        2,
        "both classes took chains: {per_device:?}"
    );
    assert!(manifest_of(&config).is_some(), "the run wrote a manifest");
}

/// A run killed mid-flight and resumed keeps each chain on the class it
/// started on: the binding is durable, so nothing rebinds.
#[test]
#[ignore = "requires both GPUs"]
fn chains_keep_their_class_across_a_resume() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = common::write_config_text(
        dir.path(),
        "resume.toml",
        &config_text("./store", 8, 3, BOTH_DEVICES),
    );

    // Kill the orchestrator once the run is under way, so some chains are
    // bound and partly walked while others are untouched.
    let mut child = spawn_run(&config);
    let bound = poll_until(Duration::from_secs(60), || {
        journal_events(&config)
            .iter()
            .filter(|e| matches!(e, LifecycleEvent::Committed { .. }))
            .count()
            >= 2
    });
    assert!(bound, "the run committed before the deadline");
    child.kill().expect("kill the orchestrator");
    child.wait().expect("reap the orchestrator");

    run_to_completion(&config);

    let events = journal_events(&config);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, LifecycleEvent::ChainRebound { .. })),
        "every class was still present, so nothing moved"
    );
    assert_chains_never_split(&config);
    assert!(manifest_of(&config).is_some(), "the resumed run finalized");
}

/// A chain bound to a class the config no longer names moves to one that is
/// present, loudly, and the run converges.
#[test]
#[ignore = "requires both GPUs"]
fn removing_a_device_rebinds_its_chains_and_the_run_converges() {
    let dir = tempfile::tempdir().expect("temp dir");
    let two = common::write_config_text(
        dir.path(),
        "two.toml",
        &config_text("./store", 8, 3, BOTH_DEVICES),
    );
    // Both classes bind chains, then the session ends mid-run.
    let mut child = spawn_run(&two);
    let spread = poll_until(Duration::from_secs(60), || {
        devices_used(&journal_events(&two)).len() == 2
            && journal_events(&two)
                .iter()
                .filter(|e| matches!(e, LifecycleEvent::Committed { .. }))
                .count()
                >= 2
    });
    assert!(spread, "both classes took work before the deadline");
    child.kill().expect("kill the orchestrator");
    child.wait().expect("reap the orchestrator");

    // The same run, resumed over one class: the chains bound to the other have
    // nowhere to go but here.
    let one = common::write_config_text(
        dir.path(),
        "one.toml",
        &config_text("./store", 8, 3, NVIDIA_ONLY),
    );
    run_to_completion(&one);

    let rebound = journal_events(&one)
        .iter()
        .filter(|e| matches!(e, LifecycleEvent::ChainRebound { .. }))
        .count();
    assert!(rebound > 0, "the orphaned chains moved, and said so");
    // The manifest is valid; no equality is claimed against a single-class
    // reference, because mixed provenance is a legitimate outcome here.
    assert!(manifest_of(&one).is_some(), "the run converged");
}

/// A single-device config is byte-for-byte what it was before placement
/// existed: one class, four workers, and a manifest equal to the same run's
/// on any other worker count.
#[test]
#[ignore = "requires an NVIDIA GPU"]
fn a_single_device_run_is_unchanged_by_the_placement_machinery() {
    let dir = tempfile::tempdir().expect("temp dir");
    // The reference: the pre-placement shape, a plain worker count over the
    // backend's own device choice.
    let reference = common::write_config_text(
        dir.path(),
        "reference.toml",
        &config_text("./reference-store", 4, 2, "workers = 4"),
    );
    run_to_completion(&reference);

    // The same run under the placement machinery: one named device, four
    // workers on it.
    let placed = common::write_config_text(
        dir.path(),
        "placed.toml",
        &config_text(
            "./placed-store",
            4,
            2,
            r#"
        [[execution.device]]
        select = "nvidia"
        workers = 4
    "#,
        ),
    );
    run_to_completion(&placed);

    let reference = manifest_of(&reference).expect("the reference finalized");
    let placed = manifest_of(&placed).expect("the placed run finalized");
    assert_eq!(
        reference, placed,
        "placement is operational: it never touches what a run commits"
    );
}
