//! Multi-GPU acceptance over the real binaries and real devices: a search
//! spread across unequal cards, chain stickiness across a resume, a rebind
//! when a card is gone, and single-device determinism unchanged.
//!
//! Every test here needs the machine's GPUs, which the `on_device` module
//! holding them states.
//!
//! The placement rule itself is proven device-free in the scheduler's own
//! tests; what these prove is that the whole path — config, resolution,
//! protocol, workers, store — carries it onto real hardware.

mod common;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::process::{Child, Stdio};
use std::time::Duration;

use common::{
    ChainTrail, chain_trails, devices_reported, journal_events, manifest_bytes, manifest_of,
    poll_until, require_devices, sima_command, task_devices,
};
use sima_pipeline::Event;

/// The format every test here searches: the WGSL Gray-Scott model, whose devices
/// the Vulkan loader enumerates.
const FORMAT: &str = "ca_evolution.gray_scott.v1";

/// The candidates and segments every multi-device test here searches.
///
/// Sized so both classes provably pull work: a device's first task waits on
/// its worker's handshake, which initializes its GPU backend, so a class that
/// initializes faster would take every chain of a search whose tasks are quicker
/// than that startup gap. At 128×128 over 600 steps a segment costs far more
/// than a handshake, so the slower class's workers are still handed chains
/// long after both are up — and 12 chains outnumber the 4 workers, so no
/// class can hold the whole search by taking one task each.
const CANDIDATES: u32 = 12;
const SEGMENTS: u64 = 3;

/// A Gray-Scott config over `orchestrator` — this machine's worker layout, a
/// plain count or the rendered `[[orchestrator.device]]` entries — with `count`
/// candidates, each divided into `segments`.
fn config_text(store: &str, count: u32, segments: u64, orchestrator: &str) -> String {
    format!(
        r#"
        [search]
        root_seed = 42
        format = "{FORMAT}"
        segments = {segments}

        [search.generator]
        id = "ca_evolution.gray_scott.v1"
        count = {count}
        feed = [0.050, 0.058]
        kill = [0.062, 0.062]
        diffusion_u = [0.16, 0.16]
        diffusion_v = [0.08, 0.08]

        [search.params]
        width = 128
        height = 128
        steps = 600
        dt = 1.0
        base_u = 0.5
        base_v = 0.25
        side_divisor = 8
        noise_width = 0.02

        [config]
        store = "{store}"
        max_attempts = 3
        {orchestrator}
    "#
    )
}

/// Both of this machine's classes, two workers each.
const BOTH_DEVICES: &str = r#"
        [[orchestrator.device]]
        select = "nvidia"
        workers = 2

        [[orchestrator.device]]
        select = "intel"
        workers = 2
    "#;

/// The NVIDIA class alone, two workers.
const NVIDIA_ONLY: &str = r#"
        [[orchestrator.device]]
        select = "nvidia"
        workers = 2
    "#;

/// Spawns `sima search` over `config`, output discarded.
fn spawn_run(config: &Path) -> Child {
    sima_command()
        .args(["search", config.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sima")
}

/// Runs `config` to completion, asserting it finalized.
fn run_to_completion(config: &Path) {
    let status = spawn_run(config).wait().expect("wait for the search");
    assert_eq!(status.code(), Some(0), "the search finalized");
}

/// The devices a chain's segments ran on, from the journal.
fn chain_devices(trail: &ChainTrail, ran_on: &HashMap<String, Vec<String>>) -> HashSet<String> {
    trail
        .keys
        .iter()
        .filter_map(|key| ran_on.get(key))
        .flatten()
        .cloned()
        .collect()
}

/// Asserts every chain ran its segments on one device, and returns how many
/// chains each device carried.
fn assert_chains_never_split(config: &Path) -> Vec<(String, usize)> {
    let events = journal_events(config);
    let ran_on = task_devices(&events);
    let mut per_device: Vec<(String, usize)> = Vec::new();
    for (chain, trail) in chain_trails(config).iter().enumerate() {
        let devices = chain_devices(trail, &ran_on);
        // A chain whose segments were all committed by an earlier session
        // contributes no lease to this journal; one that ran must have search in
        // one place.
        assert!(
            devices.len() <= 1,
            "chain {chain} split across devices: {devices:?}"
        );
        if let Some(device) = devices.into_iter().next() {
            match per_device.iter_mut().find(|(name, _)| *name == device) {
                Some((_, count)) => *count += 1,
                None => per_device.push((device, 1)),
            }
        }
    }
    per_device
}

/// A chain that has search on a device whose name contains `device`, and still
/// has segments left — the state a rebind needs to have anything to move.
fn chain_with_work_on(config: &Path, device: &str) -> Option<usize> {
    let events = journal_events(config);
    let ran_on = task_devices(&events);
    chain_trails(config).iter().position(|trail| {
        trail.has_work_left()
            && chain_devices(trail, &ran_on)
                .iter()
                .any(|name| name.to_lowercase().contains(device))
    })
}

/// Every test here drives the machine's real GPUs through the real binaries.
mod on_device {
    use super::*;

    /// A Gray-Scott search over both of this machine's GPUs completes, uses both,
    /// and keeps every chain on one of them.
    #[test]
    fn a_search_over_two_device_classes_uses_both_and_splits_no_chain() {
        require_devices(FORMAT, &["nvidia", "intel"]);
        let dir = tempfile::tempdir().expect("temp dir");
        let config = common::write_config_text(
            dir.path(),
            "both.toml",
            &config_text("./store", CANDIDATES, SEGMENTS, BOTH_DEVICES),
        );
        run_to_completion(&config);

        let events = journal_events(&config);
        let reported = devices_reported(&events);
        assert_eq!(
            reported.len(),
            2,
            "both device classes carried workers: {reported:?}"
        );
        let per_device = assert_chains_never_split(&config);
        assert_eq!(
            per_device.iter().map(|(_, n)| n).sum::<usize>(),
            CANDIDATES as usize,
            "every chain ran"
        );
        // Greedy placement hands an unbound chain to whichever class is free, and
        // at this workload's sizing neither class can absorb the search alone.
        assert_eq!(
            per_device.len(),
            2,
            "both classes took chains: {per_device:?}"
        );
        assert!(
            manifest_of(&config).is_some(),
            "the search wrote a manifest"
        );
    }

    /// A search killed mid-flight and resumed keeps each chain on the class it
    /// started on: the binding is durable, so nothing rebinds.
    #[test]
    fn chains_keep_their_class_across_a_resume() {
        require_devices(FORMAT, &["nvidia", "intel"]);
        let dir = tempfile::tempdir().expect("temp dir");
        let config = common::write_config_text(
            dir.path(),
            "resume.toml",
            &config_text("./store", CANDIDATES, SEGMENTS, BOTH_DEVICES),
        );

        // Kill the orchestrator once the search is under way, so some chains are
        // bound and partly walked while others are untouched.
        let mut child = spawn_run(&config);
        let bound = poll_until(Duration::from_secs(120), || {
            journal_events(&config)
                .iter()
                .filter(|e| matches!(e, Event::Committed { .. }))
                .count()
                >= 2
        });
        assert!(bound, "the search committed before the deadline");
        child.kill().expect("kill the orchestrator");
        child.wait().expect("reap the orchestrator");

        // The kill landed mid-search: work remains, so the second session is a real
        // resume rather than a re-finalization that would satisfy every assertion
        // below without running anything.
        assert!(
            manifest_of(&config).is_none(),
            "the first session was killed before it finalized"
        );
        let first_session = journal_events(&config).len();

        run_to_completion(&config);

        let events = journal_events(&config);
        let resumed_leases = events[first_session..]
            .iter()
            .filter(|e| matches!(e, Event::Leased { .. }))
            .count();
        assert!(resumed_leases > 0, "the second session ran the work left");
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, Event::ChainRebound { .. })),
            "every class was still present, so nothing moved"
        );
        assert_chains_never_split(&config);
        assert!(
            manifest_of(&config).is_some(),
            "the resumed search finalized"
        );
    }

    /// A chain bound to a class the config no longer names moves to one that is
    /// present, loudly, and the search converges.
    #[test]
    fn removing_a_device_rebinds_its_chains_and_the_run_converges() {
        require_devices(FORMAT, &["nvidia", "intel"]);
        let dir = tempfile::tempdir().expect("temp dir");
        let two = common::write_config_text(
            dir.path(),
            "two.toml",
            &config_text("./store", CANDIDATES, SEGMENTS, BOTH_DEVICES),
        );
        // The state a rebind needs something to move: a chain bound to Intel that
        // still has segments left. Workers reporting both devices would say only
        // that both classes started, not that Intel holds any unfinished chain.
        let mut child = spawn_run(&two);
        let orphan_pending = poll_until(Duration::from_secs(180), || {
            chain_with_work_on(&two, "intel").is_some()
        });
        assert!(
            orphan_pending,
            "Intel held an unfinished chain before the deadline"
        );
        let orphan = chain_with_work_on(&two, "intel").expect("the chain the poll saw");
        child.kill().expect("kill the orchestrator");
        child.wait().expect("reap the orchestrator");

        // The same search, resumed over one class: the chain bound to the other has
        // nowhere to go but here.
        let one = common::write_config_text(
            dir.path(),
            "one.toml",
            &config_text("./store", CANDIDATES, SEGMENTS, NVIDIA_ONLY),
        );
        run_to_completion(&one);

        let rebound: Vec<u64> = journal_events(&one)
            .iter()
            .filter_map(|e| match e {
                Event::ChainRebound { chain, .. } => Some(*chain),
                _ => None,
            })
            .collect();
        assert!(
            rebound.contains(&(orphan as u64)),
            "chain {orphan}, bound to the device that is gone, moved and said so: {rebound:?}"
        );
        // The manifest is valid; no equality is claimed against a single-class
        // reference, because mixed provenance is a legitimate outcome here.
        assert!(manifest_of(&one).is_some(), "the search converged");
    }

    /// A search that names one device commits byte-for-byte what the same search
    /// commits under a plain worker count: placement is operational, so it reaches
    /// nothing a search records.
    #[test]
    fn a_single_device_run_commits_the_same_manifest_as_a_plain_worker_count() {
        require_devices(FORMAT, &["nvidia"]);
        let dir = tempfile::tempdir().expect("temp dir");
        // The reference: a plain worker count over the backend's own device
        // choice, naming no device and reading no placement state.
        let reference = common::write_config_text(
            dir.path(),
            "reference.toml",
            &config_text(
                "./reference-store",
                4,
                2,
                "[orchestrator]\n        workers = 4",
            ),
        );
        run_to_completion(&reference);

        // The same search under the placement machinery: one named device, four
        // workers on it.
        let placed = common::write_config_text(
            dir.path(),
            "placed.toml",
            &config_text(
                "./placed-store",
                4,
                2,
                r#"
        [[orchestrator.device]]
        select = "nvidia"
        workers = 4
    "#,
            ),
        );
        run_to_completion(&placed);

        // The manifest file itself, byte for byte: what a search commits is the
        // claim, so the bytes on disk are the evidence.
        let reference = manifest_bytes(&reference).expect("the reference finalized");
        let placed = manifest_bytes(&placed).expect("the placed search finalized");
        assert_eq!(
            reference, placed,
            "placement is operational: it never touches what a search commits"
        );
    }
}
