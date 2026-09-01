//! Placement across device classes, through the real wire protocol and host
//! loop: an unbound chain goes to whoever pulls it, a bound one stays put, and
//! a chain whose class is gone moves loudly.
//!
//! The classes here are fictitious ids over the stub domain, and the loopback
//! resolver reports the device it was handed. Nothing touches a GPU: what is
//! under test is the placement rule and its persistence, which are device-free
//! by construction.

mod common;

use std::collections::HashSet;

use common::{
    chained_config, class, class_slot, device, device_naming_resolver, exec_over, journal_events,
    named_class, run_with, search_id, task_classes, temp_store, worker_devices,
};
use sima_contracts::DeviceClass;
use sima_core::Result;
use sima_domains::StubBehavior;
use sima_scheduler::{Event, SearchOutcome};

/// Two fictitious classes, two workers each.
const INTEL: u32 = 0x8086;
const NVIDIA: u32 = 0x10de;

/// The class a chain's placement slot binds it to, as the scheduler wrote it.
fn slot_class(
    store: &sima_store::Store,
    search: &sima_model::SearchId,
    chain: u64,
) -> Option<String> {
    let payload = store
        .chain_bindings(search)
        .expect("read bindings")
        .remove(&chain)?;
    let value: serde_json::Value = serde_json::from_slice(&payload).expect("a slot is JSON");
    Some(
        value["class"]
            .as_str()
            .expect("a slot names its class")
            .to_string(),
    )
}

#[test]
fn a_chains_segments_agree_with_the_class_its_slot_records() -> Result<()> {
    // End to end over four chained segments and two classes: the class each
    // segment ran on is the one the chain's slot names. Which class wins the
    // first pull is a race, so the assertion joins what happened against what
    // was recorded rather than naming a class.
    //
    // That the segments cannot split across classes is the coordinator's
    // eligibility invariant, proven deterministically in its own unit tests; a
    // search this small could satisfy this assertion by luck.
    let (_dir, store) = temp_store();
    let config = chained_config(42, vec![StubBehavior::Accumulate(2)], 4);
    let exec = exec_over(vec![device(NVIDIA, 2), device(INTEL, 2)], 1);
    let outcome = run_with(&store, &config, &exec, device_naming_resolver())?;
    assert!(matches!(outcome, SearchOutcome::Finalized { .. }));

    let events = journal_events(&store, &search_id(&config));
    let classes = task_classes(&events);
    assert_eq!(classes.len(), 4, "four segments ran");
    let bound = slot_class(&store, &search_id(&config), 0).expect("the chain bound");
    for (task, ran_on) in &classes {
        assert_eq!(ran_on, &vec![bound.clone()], "segment {task}");
    }
    Ok(())
}

#[test]
fn every_chain_binds_to_a_class_the_search_has() -> Result<()> {
    // Eight single-segment chains over two classes: every chain ends up with a
    // durable binding to one of the search's classes. Which class takes which
    // chain is a race — that is what greedy placement is — so the assertion is
    // over the invariant, never a particular split.
    let (_dir, store) = temp_store();
    let config = chained_config(7, vec![StubBehavior::Accumulate(2); 8], 1);
    let exec = exec_over(vec![device(NVIDIA, 2), device(INTEL, 2)], 1);
    let outcome = run_with(&store, &config, &exec, device_naming_resolver())?;
    assert!(matches!(outcome, SearchOutcome::Finalized { .. }));

    let search = search_id(&config);
    let bindings = store.chain_bindings(&search)?;
    assert_eq!(bindings.len(), 8, "every chain bound");
    let present: HashSet<String> = [
        format!("{:04x}:0001", NVIDIA),
        format!("{:04x}:0001", INTEL),
    ]
    .into_iter()
    .collect();
    for chain in 0..8u64 {
        let bound = slot_class(&store, &search, chain).expect("the chain bound");
        assert!(present.contains(&bound), "chain {chain} bound to {bound}");
    }
    // No chain moved: every class the search started with is still here.
    assert!(!events_contain_rebind(&journal_events(&store, &search)));
    Ok(())
}

#[test]
fn a_chain_whose_class_is_gone_rebinds_and_the_search_completes() -> Result<()> {
    // The hardware changed between sessions: the store holds a binding to a
    // class this search does not have. Continuity outranks stickiness — the work
    // moves to a class that is present, and the journal says so.
    let (_dir, store) = temp_store();
    let config = chained_config(11, vec![StubBehavior::Accumulate(2)], 2);
    let search = search_id(&config);
    // The search directory must exist before a slot can be written into it.
    store.create_search(&config)?;
    let absent = class_slot(&DeviceClass::new("1002:0001").expect("class id"));
    store.bind_chain(&search, 0, &absent)?;

    let exec = exec_over(vec![device(NVIDIA, 2)], 1);
    let outcome = run_with(&store, &config, &exec, device_naming_resolver())?;
    assert!(matches!(outcome, SearchOutcome::Finalized { .. }));

    let events = journal_events(&store, &search);
    let rebinds: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e, Event::ChainRebound { .. }))
        .collect();
    assert_eq!(rebinds.len(), 1, "the orphaned chain rebound once, loudly");
    assert!(matches!(
        rebinds[0],
        Event::ChainRebound { chain: 0, from, to }
            if from == "1002:0001" && to == &format!("{NVIDIA:04x}:0001")
    ));
    // The slot now names the class the work actually moved to.
    assert_eq!(
        slot_class(&store, &search, 0).as_deref(),
        Some(format!("{NVIDIA:04x}:0001").as_str())
    );
    Ok(())
}

#[test]
fn a_chain_resumed_from_its_slot_stays_on_its_class() -> Result<()> {
    // The binding is durable: a chain seeded to a class the search still has runs
    // on that class, with no rebind, whatever else the pool is doing.
    let (_dir, store) = temp_store();
    let config = chained_config(13, vec![StubBehavior::Accumulate(2)], 3);
    let search = search_id(&config);
    store.create_search(&config)?;
    store.bind_chain(&search, 0, &class_slot(&class(INTEL)))?;

    // The Intel class carries one worker against NVIDIA's three, so a chain
    // that ignored its binding would almost certainly land on NVIDIA.
    let exec = exec_over(vec![device(NVIDIA, 3), device(INTEL, 1)], 1);
    let outcome = run_with(&store, &config, &exec, device_naming_resolver())?;
    assert!(matches!(outcome, SearchOutcome::Finalized { .. }));

    let events = journal_events(&store, &search);
    assert!(!events_contain_rebind(&events), "the class was present");
    let intel = format!("{INTEL:04x}:0001");
    for (task, classes) in task_classes(&events) {
        assert_eq!(classes, vec![intel.clone()], "task {task} kept its class");
    }
    Ok(())
}

#[test]
fn a_chain_whose_slot_cannot_be_read_binds_again() -> Result<()> {
    // A slot the scheduler cannot decode carries no usable placement, so it is
    // read as an unbound chain: the chain binds again on its first pull and the
    // search proceeds. Placement is advisory coherence state, and losing one slot
    // costs coherence for that chain rather than the whole resume.
    let (_dir, store) = temp_store();
    let config = chained_config(29, vec![StubBehavior::Accumulate(2)], 2);
    let search = search_id(&config);
    store.create_search(&config)?;
    store.bind_chain(&search, 0, b"not a placement slot")?;

    let exec = exec_over(vec![device(NVIDIA, 2)], 1);
    let outcome = run_with(&store, &config, &exec, device_naming_resolver())?;
    assert!(matches!(outcome, SearchOutcome::Finalized { .. }));

    // Binding again is what an unbound chain does, so no rebind is journaled.
    let events = journal_events(&store, &search);
    assert!(!events_contain_rebind(&events));
    assert_eq!(
        slot_class(&store, &search, 0).as_deref(),
        Some(format!("{NVIDIA:04x}:0001").as_str()),
        "the chain bound to the class that pulled it"
    );
    Ok(())
}

#[test]
fn a_stateless_tasks_retries_stay_on_one_class() -> Result<()> {
    // A task with no chain is a chain of length one: its attempts stick within
    // the search, so a retried attempt reproduces what the failed one would have
    // committed. Nothing persists — after it commits there is nothing to place.
    let (_dir, store) = temp_store();
    let config = common::config(17, vec![StubBehavior::Flaky(2)]);
    let exec = exec_over(vec![device(NVIDIA, 2), device(INTEL, 2)], 3);
    let outcome = run_with(&store, &config, &exec, device_naming_resolver())?;
    assert!(matches!(outcome, SearchOutcome::Finalized { .. }));

    let search = search_id(&config);
    let events = journal_events(&store, &search);
    let classes = task_classes(&events);
    let (task, attempts) = classes.iter().next().expect("the search had one task");
    assert_eq!(attempts.len(), 3, "two failures then a success: {task}");
    let used: HashSet<&String> = attempts.iter().collect();
    assert_eq!(used.len(), 1, "every attempt ran on one class: {used:?}");
    assert!(
        store.chain_bindings(&search)?.is_empty(),
        "a chain-less task persists no placement"
    );
    Ok(())
}

#[test]
fn every_worker_reports_the_device_it_computes_on() -> Result<()> {
    let (_dir, store) = temp_store();
    let config = common::config(23, vec![StubBehavior::Succeed; 4]);
    let exec = exec_over(vec![device(NVIDIA, 2), device(INTEL, 1)], 1);
    run_with(&store, &config, &exec, device_naming_resolver())?;

    let devices = worker_devices(&journal_events(&store, &search_id(&config)));
    assert_eq!(devices.len(), 3, "every worker of the pool reported");
    // The slots carry the pool's shape: two NVIDIA workers, one Intel.
    let mut named: Vec<&str> = devices.values().map(|d| named_class(d)).collect();
    named.sort_unstable();
    let intel = format!("{INTEL:04x}:0001");
    let nvidia = format!("{NVIDIA:04x}:0001");
    let mut expected = [intel.as_str(), nvidia.as_str(), nvidia.as_str()];
    expected.sort_unstable();
    assert_eq!(named, expected);
    Ok(())
}

/// Whether any chain moved class during the search.
fn events_contain_rebind(events: &[Event]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, Event::ChainRebound { .. }))
}
