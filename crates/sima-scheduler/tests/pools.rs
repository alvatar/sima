//! Worker pools over several transports, through the real wire protocol and
//! host loop: one run spread across two loopback pools on distinct hosts, each
//! holding its own device class.
//!
//! What is under test is the pool boundary — slots spawn on their own pool's
//! transport, `WorkerBound` carries each pool's host, and placement binds a
//! chain to a class across pools exactly as it does within one. The classes
//! are fictitious ids over the stub domain, so nothing touches a GPU.

mod common;

use std::collections::HashSet;
use std::time::Duration;

use common::{
    chained_config, class, class_slot, device, device_naming_resolver, exec_over, journal_events,
    named_class, run_pools, search_id, task_classes, temp_store,
};
use sima_core::Result;
use sima_domains::StubBehavior;
use sima_scheduler::{Event, RunOutcome, WorkerPool, worker_slots};
use sima_transport::loopback::LoopbackTransport;

/// Two fictitious classes, one per pool.
const INTEL: u32 = 0x8086;
const NVIDIA: u32 = 0x10de;

#[test]
fn a_run_spreads_across_two_pools_and_places_by_class() -> Result<()> {
    let (_dir, store) = temp_store();
    let config = chained_config(101, vec![StubBehavior::Accumulate(2); 4], 3);
    let run = search_id(&config);
    // Seed one chain onto each class, so both pools run real work and the
    // cross-pool binding is concrete rather than a placement race.
    store.create_search(&config)?;
    store.bind_chain(&run, 0, &class_slot(&class(INTEL)))?;
    store.bind_chain(&run, 1, &class_slot(&class(NVIDIA)))?;

    // Two pools, each a class of its own on a distinct host. The execs supply
    // each pool's two slots; only their cadence reaches the run.
    let nvidia_exec = exec_over(vec![device(NVIDIA, 2)], 1);
    let intel_exec = exec_over(vec![device(INTEL, 2)], 1);
    let alpha = LoopbackTransport::new(
        config.format.clone(),
        Duration::MAX,
        None,
        device_naming_resolver(),
    );
    let beta = LoopbackTransport::new(
        config.format.clone(),
        Duration::MAX,
        None,
        device_naming_resolver(),
    );
    let pools = [
        WorkerPool {
            transport: &alpha,
            host: "alpha".to_string(),
            slots: worker_slots(&nvidia_exec),
        },
        WorkerPool {
            transport: &beta,
            host: "beta".to_string(),
            slots: worker_slots(&intel_exec),
        },
    ];
    let outcome = run_pools(&store, &config, &nvidia_exec, &pools)?;
    assert!(matches!(outcome, RunOutcome::Finalized { .. }));

    let events = journal_events(&store, &run);
    let nvidia = format!("{NVIDIA:04x}:0001");
    let intel = format!("{INTEL:04x}:0001");

    // Every slot spawned on its own pool's transport under its pool's host:
    // four workers, alpha carrying both NVIDIA slots, beta both INTEL slots.
    let mut bound: Vec<(String, String)> = events
        .iter()
        .filter_map(|event| match event {
            Event::WorkerBound { host, device, .. } if !device.is_empty() => {
                Some((host.clone(), named_class(device).to_string()))
            }
            _ => None,
        })
        .collect();
    bound.sort();
    assert_eq!(
        bound,
        vec![
            ("alpha".to_string(), nvidia.clone()),
            ("alpha".to_string(), nvidia.clone()),
            ("beta".to_string(), intel.clone()),
            ("beta".to_string(), intel.clone()),
        ],
        "each pool's slots reported its own host and class"
    );

    // Placement across pools obeys the one-class rule: no task split across
    // classes, and both seeded classes ran, so the INTEL chain landed on beta
    // and the NVIDIA chain on alpha — the only pools that hold those classes.
    let classes = task_classes(&events);
    for (task, ran_on) in &classes {
        let distinct: HashSet<&String> = ran_on.iter().collect();
        assert_eq!(
            distinct.len(),
            1,
            "task {task} did not split across classes"
        );
    }
    let used: HashSet<String> = classes.values().flatten().cloned().collect();
    assert!(
        used.contains(&nvidia) && used.contains(&intel),
        "both pools ran real work across the class boundary: {used:?}"
    );
    Ok(())
}
