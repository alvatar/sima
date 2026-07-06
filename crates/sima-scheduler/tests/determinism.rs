//! Determinism acceptance: the same config run into fresh stores yields
//! byte-identical manifests, whatever the worker count and whatever order the
//! journal happened to record.

mod common;

use common::{config, exec, run_id, run_into, temp_store};
use sima_contracts::StubBehavior;
use sima_core::Result;
use sima_scheduler::RunOutcome;

/// The phase-acceptance proof: one config, two fresh stores, identical manifest.
#[test]
fn same_config_in_two_fresh_stores_yields_identical_manifests() -> Result<()> {
    let cfg = config(
        42,
        vec![
            StubBehavior::Succeed,
            StubBehavior::Succeed,
            StubBehavior::Succeed,
        ],
    );
    let exec = exec(4, 3, 1_000);

    let (_a, store_a) = temp_store();
    let (_b, store_b) = temp_store();
    assert!(matches!(
        run_into(&store_a, &cfg, &exec)?,
        RunOutcome::Finalized { .. }
    ));
    assert!(matches!(
        run_into(&store_b, &cfg, &exec)?,
        RunOutcome::Finalized { .. }
    ));

    let run = run_id(&cfg);
    let manifest_a = store_a.manifest(&run)?.expect("store A finalized");
    let manifest_b = store_b.manifest(&run)?.expect("store B finalized");
    assert_eq!(manifest_a, manifest_b);
    Ok(())
}

/// The pool must not leak completion order into identity: one worker and eight
/// produce the same manifest.
#[test]
fn manifest_is_independent_of_worker_count() -> Result<()> {
    let cfg = config(
        7,
        vec![
            StubBehavior::Succeed,
            StubBehavior::Flaky(1),
            StubBehavior::Succeed,
            StubBehavior::Succeed,
        ],
    );

    let (_one, store_one) = temp_store();
    let (_many, store_many) = temp_store();
    run_into(&store_one, &cfg, &exec(1, 3, 1_000))?;
    run_into(&store_many, &cfg, &exec(8, 3, 1_000))?;

    let run = run_id(&cfg);
    assert_eq!(
        store_one.manifest(&run)?.expect("single-worker manifest"),
        store_many.manifest(&run)?.expect("many-worker manifest"),
    );
    Ok(())
}

/// The journal is observational: two identical runs may record events in
/// different orders yet still finalize to the same manifest. The test asserts
/// the manifests match without asserting anything about journal equality.
#[test]
fn manifests_match_though_journals_need_not() -> Result<()> {
    let cfg = config(
        99,
        vec![
            StubBehavior::Succeed,
            StubBehavior::Succeed,
            StubBehavior::Succeed,
            StubBehavior::Succeed,
        ],
    );
    let exec = exec(4, 3, 1_000);

    let (_a, store_a) = temp_store();
    let (_b, store_b) = temp_store();
    run_into(&store_a, &cfg, &exec)?;
    run_into(&store_b, &cfg, &exec)?;

    let run = run_id(&cfg);
    assert_eq!(
        store_a.manifest(&run)?.expect("manifest A"),
        store_b.manifest(&run)?.expect("manifest B"),
    );
    Ok(())
}
