//! The budget guard: what a search has spent on rented machines, and whether
//! it may rent another.
//!
//! Spend is counted from durable store state alone — the closed rentals in
//! the spend ledger, plus the records of rentals still open, charged from
//! the stamp they were written under to now. Nothing is held in memory
//! between calls, so a resumed search counts what the process before it spent,
//! and the provider's own billing API is never consulted.
//!
//! The guard supplies a verdict; it enforces nothing. Refusing a rental the
//! budget cannot pay for happens in [`acquire`](crate::acquire), and the
//! cadence at which a running fleet is checked against its budget belongs
//! to the caller that polls [`assess`].

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sima_core::Result;
use sima_model::SearchId;
use sima_store::{SpendEntry, Store};

use crate::offer::Price;

/// Milliseconds in one hour, the denominator turning an hourly rate into
/// what an elapsed window charges.
const MS_PER_HOUR: u128 = 3_600_000;

/// A total amount of money in micro-USD: $1.23 is `Cost(1_230_000)`.
///
/// The same unit [`Price`] states a rate in, so an hourly rate multiplied
/// by a duration lands here without a conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cost(pub u64);

impl Cost {
    /// What `rate` charges over `elapsed_ms`, rounded up: every started
    /// fraction counts, so the amount is at least what the window costs at
    /// that rate.
    pub fn accrued(rate: Price, elapsed_ms: u64) -> Cost {
        // The product of a rate and a duration leaves 64 bits at extreme
        // values, so the multiplication happens wide and the quotient comes
        // back clamped.
        let micro = (rate.0 as u128 * elapsed_ms as u128).div_ceil(MS_PER_HOUR);
        Cost(micro.min(u64::MAX as u128) as u64)
    }
}

/// A search's rental budget. Both limits are optional, and an absent one is
/// unlimited, which is what the default states.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Budget {
    /// Ceiling on the search's total spend, across every rental it made, live
    /// and past.
    pub max_spend: Option<Cost>,
    /// Ceiling on the wall-clock the rental phase may span, measured from
    /// the search's first rental. One bound per search, across providers.
    pub max_wall_clock: Option<Duration>,
}

/// What a search's spend and clock look like against its budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Both limits hold.
    Within {
        /// What the search has spent so far.
        accrued: Cost,
        /// When the rental phase searches out, once a wall-clock limit is set
        /// and a first rental anchors it.
        deadline_ms: Option<u64>,
    },
    /// A limit is reached, and no further rental may be made.
    Exhausted(Exhaustion),
}

/// The limit a search reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exhaustion {
    /// Accrued spend reached the cap.
    Spend {
        /// What the search has spent.
        accrued: Cost,
        /// The cap it reached.
        cap: Cost,
    },
    /// The rental phase's deadline passed.
    WallClock {
        /// The deadline, in wall-clock milliseconds since the epoch.
        deadline_ms: u64,
    },
}

/// A search's rentals, closed and open, and what they have cost.
#[derive(Debug, Clone)]
pub struct SpendReport {
    /// Rentals that have been closed out.
    pub entries: Vec<SpendEntry>,
    /// Rentals still accruing.
    pub open: Vec<OpenSpend>,
    /// What the two together have cost.
    pub total: Cost,
}

/// One rental still accruing: a ledger record whose rental has no spend
/// entry yet, charged from the stamp the record carries.
#[derive(Debug, Clone)]
pub struct OpenSpend {
    /// The rental's tag.
    pub tag: String,
    /// The rate it is charged at.
    pub rate: Price,
    /// When its charged window opened.
    pub started_ms: u64,
    /// What it has cost so far.
    pub accrued: Cost,
}

/// What `owner` has spent as of `now_ms`: every closed rental's cost, plus
/// every open rental charged from its stamp to `now_ms`.
///
/// A record whose rental already has an entry is the entry's own, pending
/// removal, and is left out — the entry is what that rental cost. Records
/// of every provider count: the budget is one pool of money per search.
pub fn spend_report(store: &Store, owner: &SearchId, now_ms: u64) -> Result<SpendReport> {
    let owner = owner.to_string();
    let entries = store.spend_entries(&owner)?;
    let closed: HashSet<(&str, u64)> = entries
        .iter()
        .map(|entry| (entry.tag.as_str(), entry.started_ms))
        .collect();
    let open: Vec<OpenSpend> = store
        .instance_records()?
        .into_iter()
        .filter(|record| {
            record.owner == owner && !closed.contains(&(record.tag.as_str(), record.created_ms))
        })
        .map(|record| {
            let rate = Price(record.price_micro_usd_hour);
            OpenSpend {
                rate,
                started_ms: record.created_ms,
                // A stamp ahead of the clock leaves a window of no time.
                accrued: Cost::accrued(rate, now_ms.saturating_sub(record.created_ms)),
                tag: record.tag,
            }
        })
        .collect();
    let total = entries
        .iter()
        .map(|entry| entry.cost_micro_usd)
        .chain(open.iter().map(|rental| rental.accrued.0))
        .fold(0, u64::saturating_add);
    Ok(SpendReport {
        entries,
        open,
        total: Cost(total),
    })
}

/// Whether `owner` may rent, as of `now_ms`.
///
/// A limit is reached when spend meets its cap or the clock meets the
/// deadline, so a budget exactly consumed admits nothing further. A search
/// that has rented nothing has no anchor and therefore no deadline. When
/// both limits are reached, the spend is the one reported.
pub fn assess(store: &Store, owner: &SearchId, budget: &Budget, now_ms: u64) -> Result<Verdict> {
    let report = spend_report(store, owner, now_ms)?;
    if let Some(cap) = budget.max_spend
        && report.total >= cap
    {
        return Ok(Verdict::Exhausted(Exhaustion::Spend {
            accrued: report.total,
            cap,
        }));
    }
    let deadline_ms = deadline(&report, budget);
    if let Some(deadline_ms) = deadline_ms
        && now_ms >= deadline_ms
    {
        return Ok(Verdict::Exhausted(Exhaustion::WallClock { deadline_ms }));
    }
    Ok(Verdict::Within {
        accrued: report.total,
        deadline_ms,
    })
}

/// When the rental phase searches out: the search's earliest rental plus the
/// wall-clock limit. Absent while either the limit or the first rental is.
///
/// The addition saturates, so a limit longer than the clock can express
/// reads as the furthest future the stamp holds rather than wrapping into
/// a deadline already passed.
fn deadline(report: &SpendReport, budget: &Budget) -> Option<u64> {
    let limit = budget.max_wall_clock?;
    let anchor = report
        .entries
        .iter()
        .map(|entry| entry.started_ms)
        .chain(report.open.iter().map(|rental| rental.started_ms))
        .min()?;
    let limit_ms = limit.as_millis().min(u64::MAX as u128) as u64;
    Some(anchor.saturating_add(limit_ms))
}

/// Wall-clock milliseconds since the epoch: the stamp a rental's charged
/// window opens and closes on, and the stamp the ledger record carries. A
/// clock behind the epoch stamps zero.
///
/// Public because [`assess`] takes the stamp as a parameter — so a caller can
/// drive an assessment without waiting on the clock — and every caller that is
/// not doing that needs the same reading of it.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sima_core::Result;
    use sima_model::SearchId;
    use sima_store::{InstanceRecord, InstanceRecordState, Rental, SpendEntry, Store};

    use super::{Budget, Cost, Exhaustion, Verdict, assess, spend_report};
    use crate::offer::Price;
    use crate::testutil::{live_state, sample_search, temp_store};

    /// The wall-clock stamp the fixtures are placed around.
    const NOON: u64 = 1_700_000_000_000;

    /// A live record for `tag` owned by `owner`, charged at `rate` from
    /// `created_ms`.
    fn record(tag: &str, owner: &SearchId, rate: u64, created_ms: u64) -> InstanceRecord {
        InstanceRecord {
            tag: tag.to_string(),
            provider: "stub".to_string(),
            machine: "m-0".to_string(),
            owner: owner.to_string(),
            role: Rental::Worker,
            state: live_state("i-1"),
            price_micro_usd_hour: rate,
            created_ms,
        }
    }

    /// A closed rental for `tag` owned by `owner`, started at `started_ms`
    /// and costing `cost`.
    fn entry(tag: &str, owner: &SearchId, started_ms: u64, cost: u64) -> SpendEntry {
        SpendEntry {
            tag: tag.to_string(),
            provider: "stub".to_string(),
            owner: owner.to_string(),
            price_micro_usd_hour: 100_000,
            started_ms,
            ended_ms: started_ms + 3_600_000,
            cost_micro_usd: cost,
        }
    }

    /// The total `owner` has spent as of `now_ms`.
    fn total(store: &Store, owner: &SearchId, now_ms: u64) -> Result<Cost> {
        Ok(spend_report(store, owner, now_ms)?.total)
    }

    #[test]
    fn closed_rentals_total_what_their_entries_cost() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 82_400))?;
        store.put_spend(&entry("sima-tag-1", &owner, NOON, 17_600))?;
        let report = spend_report(&store, &owner, NOON + 3_600_000)?;
        assert_eq!(report.entries.len(), 2);
        assert!(report.open.is_empty());
        assert_eq!(report.total, Cost(100_000));
        Ok(())
    }

    #[test]
    fn an_open_rental_is_charged_from_its_stamp_to_now() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        store.put_instance(&record("sima-tag-0", &owner, 100_000, NOON))?;
        let report = spend_report(&store, &owner, NOON + 1_800_000)?;
        assert!(report.entries.is_empty());
        assert_eq!(report.open.len(), 1);
        assert_eq!(report.open[0].tag, "sima-tag-0");
        assert_eq!(report.open[0].rate, Price(100_000));
        assert_eq!(report.open[0].started_ms, NOON);
        assert_eq!(report.open[0].accrued, Cost(50_000));
        assert_eq!(report.total, Cost(50_000));
        Ok(())
    }

    #[test]
    fn an_intent_record_is_charged_like_a_live_one() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        // The window opens at the stamp, which is written before the
        // provider is called, so an attempt that never reached a machine
        // still charges for the time it may have been running one.
        let mut intent = record("sima-tag-0", &owner, 100_000, NOON);
        intent.state = InstanceRecordState::Intent;
        store.put_instance(&intent)?;
        assert_eq!(total(&store, &owner, NOON + 3_600_000)?, Cost(100_000));
        Ok(())
    }

    #[test]
    fn closed_and_open_rentals_total_together() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 82_400))?;
        store.put_instance(&record("sima-tag-1", &owner, 100_000, NOON))?;
        assert_eq!(total(&store, &owner, NOON + 3_600_000)?, Cost(182_400));
        Ok(())
    }

    #[test]
    fn a_record_its_own_entry_already_closed_is_counted_once() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        // The state a crash between the entry write and the record clear
        // leaves: the entry is the closure, and the record is waiting to be
        // removed.
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 82_400))?;
        store.put_instance(&record("sima-tag-0", &owner, 100_000, NOON))?;
        let report = spend_report(&store, &owner, NOON + 3_600_000)?;
        assert!(report.open.is_empty());
        assert_eq!(report.total, Cost(82_400));
        Ok(())
    }

    #[test]
    fn a_record_sharing_only_a_tag_with_an_entry_is_a_rental_of_its_own() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        // Tags repeat across process restarts; a later rental under a name
        // an earlier one used is a second machine, and is charged as one.
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 82_400))?;
        store.put_instance(&record("sima-tag-0", &owner, 100_000, NOON + 7_200_000))?;
        let report = spend_report(&store, &owner, NOON + 10_800_000)?;
        assert_eq!(report.open.len(), 1);
        assert_eq!(report.total, Cost(182_400));
        Ok(())
    }

    #[test]
    fn another_runs_rentals_are_no_part_of_this_ones_spend() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        let other = sample_search(8);
        store.put_spend(&entry("sima-tag-0", &other, NOON, 82_400))?;
        store.put_instance(&record("sima-tag-1", &other, 100_000, NOON))?;
        assert_eq!(total(&store, &owner, NOON + 3_600_000)?, Cost(0));
        Ok(())
    }

    #[test]
    fn a_rental_from_another_provider_counts_against_the_same_budget() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        // One search, one pool of money: where the machine was rented changes
        // nothing about what it costs.
        let mut foreign = record("sima-tag-0", &owner, 100_000, NOON);
        foreign.provider = "vastai".to_string();
        store.put_instance(&foreign)?;
        assert_eq!(total(&store, &owner, NOON + 3_600_000)?, Cost(100_000));
        Ok(())
    }

    #[test]
    fn a_record_stamped_ahead_of_the_clock_accrues_nothing() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        // A clock that stepped backwards leaves a window of no time rather
        // than an underflow.
        store.put_instance(&record("sima-tag-0", &owner, 100_000, NOON + 3_600_000))?;
        assert_eq!(total(&store, &owner, NOON)?, Cost(0));
        Ok(())
    }

    #[test]
    fn a_budget_with_no_limits_holds_and_names_no_deadline() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 82_400))?;
        assert_eq!(
            assess(&store, &owner, &Budget::default(), NOON + 3_600_000)?,
            Verdict::Within {
                accrued: Cost(82_400),
                deadline_ms: None,
            }
        );
        Ok(())
    }

    #[test]
    fn spend_exhausts_the_budget_at_its_cap_and_not_before() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 99_999))?;
        let budget = Budget {
            max_spend: Some(Cost(100_000)),
            ..Budget::default()
        };
        assert!(matches!(
            assess(&store, &owner, &budget, NOON)?,
            Verdict::Within { .. }
        ));
        // Reaching the cap exhausts it: the next rental would spend money
        // the budget no longer holds.
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 100_000))?;
        assert_eq!(
            assess(&store, &owner, &budget, NOON)?,
            Verdict::Exhausted(Exhaustion::Spend {
                accrued: Cost(100_000),
                cap: Cost(100_000),
            })
        );
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 120_000))?;
        assert_eq!(
            assess(&store, &owner, &budget, NOON)?,
            Verdict::Exhausted(Exhaustion::Spend {
                accrued: Cost(120_000),
                cap: Cost(100_000),
            })
        );
        Ok(())
    }

    #[test]
    fn the_clock_exhausts_the_budget_at_the_deadline_and_not_before() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 0))?;
        let budget = Budget {
            max_wall_clock: Some(Duration::from_secs(3_600)),
            ..Budget::default()
        };
        let deadline_ms = NOON + 3_600_000;
        assert_eq!(
            assess(&store, &owner, &budget, deadline_ms - 1)?,
            Verdict::Within {
                accrued: Cost(0),
                deadline_ms: Some(deadline_ms),
            }
        );
        assert_eq!(
            assess(&store, &owner, &budget, deadline_ms)?,
            Verdict::Exhausted(Exhaustion::WallClock { deadline_ms })
        );
        assert_eq!(
            assess(&store, &owner, &budget, deadline_ms + 1)?,
            Verdict::Exhausted(Exhaustion::WallClock { deadline_ms })
        );
        Ok(())
    }

    #[test]
    fn the_deadline_is_anchored_at_the_earliest_rental_of_the_run() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        // The anchor is the first rental, whether it is closed or open, so
        // a later machine cannot push the deadline out.
        store.put_spend(&entry("sima-tag-1", &owner, NOON + 600_000, 0))?;
        store.put_instance(&record("sima-tag-2", &owner, 0, NOON))?;
        store.put_instance(&record("sima-tag-3", &owner, 0, NOON + 1_200_000))?;
        let budget = Budget {
            max_wall_clock: Some(Duration::from_secs(3_600)),
            ..Budget::default()
        };
        assert_eq!(
            assess(&store, &owner, &budget, NOON)?,
            Verdict::Within {
                accrued: Cost(0),
                deadline_ms: Some(NOON + 3_600_000),
            }
        );
        Ok(())
    }

    #[test]
    fn a_run_that_rented_nothing_has_no_deadline_to_pass() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        let budget = Budget {
            max_wall_clock: Some(Duration::from_millis(1)),
            ..Budget::default()
        };
        // Without a rental there is nothing to anchor the phase to, so the
        // clock cannot exhaust a budget however long the search has been up.
        assert_eq!(
            assess(&store, &owner, &budget, NOON)?,
            Verdict::Within {
                accrued: Cost(0),
                deadline_ms: None,
            }
        );
        Ok(())
    }

    #[test]
    fn a_budget_out_of_both_money_and_time_reports_the_money() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 100_000))?;
        let budget = Budget {
            max_spend: Some(Cost(100_000)),
            max_wall_clock: Some(Duration::from_secs(1)),
        };
        assert_eq!(
            assess(&store, &owner, &budget, NOON + 3_600_000)?,
            Verdict::Exhausted(Exhaustion::Spend {
                accrued: Cost(100_000),
                cap: Cost(100_000),
            })
        );
        Ok(())
    }

    #[test]
    fn a_wall_clock_beyond_what_the_clock_holds_saturates() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        store.put_spend(&entry("sima-tag-0", &owner, NOON, 0))?;
        let budget = Budget {
            max_wall_clock: Some(Duration::from_millis(u64::MAX)),
            ..Budget::default()
        };
        // A deadline that would wrap reads as the furthest future the stamp
        // holds, never as a deadline already passed.
        assert_eq!(
            assess(&store, &owner, &budget, NOON)?,
            Verdict::Within {
                accrued: Cost(0),
                deadline_ms: Some(u64::MAX),
            }
        );
        Ok(())
    }

    #[test]
    fn a_total_beyond_what_the_unit_holds_saturates() -> Result<()> {
        let (_dir, store) = temp_store();
        let owner = sample_search(7);
        store.put_spend(&entry("sima-tag-0", &owner, NOON, u64::MAX))?;
        store.put_spend(&entry("sima-tag-1", &owner, NOON, u64::MAX))?;
        assert_eq!(total(&store, &owner, NOON)?, Cost(u64::MAX));
        Ok(())
    }

    #[test]
    fn a_full_hour_charges_the_rate() {
        assert_eq!(Cost::accrued(Price(82_400), 3_600_000), Cost(82_400));
    }

    #[test]
    fn a_window_of_no_time_charges_nothing() {
        assert_eq!(Cost::accrued(Price(82_400), 0), Cost(0));
        // A rate of nothing charges nothing however long it searches.
        assert_eq!(Cost::accrued(Price(0), 7_200_000), Cost(0));
    }

    #[test]
    fn a_whole_fraction_of_an_hour_charges_that_fraction() {
        assert_eq!(Cost::accrued(Price(100_000), 1_800_000), Cost(50_000));
        assert_eq!(Cost::accrued(Price(100_000), 900_000), Cost(25_000));
    }

    #[test]
    fn several_hours_charge_the_rate_that_many_times() {
        assert_eq!(Cost::accrued(Price(82_400), 10_800_000), Cost(247_200));
    }

    #[test]
    fn a_started_fraction_of_a_micro_usd_counts_in_full() {
        // A millisecond at this rate is a small fraction of one micro-USD,
        // and rounding down would charge nothing for a machine that ran.
        assert_eq!(Cost::accrued(Price(1), 1), Cost(1));
        assert_eq!(Cost::accrued(Price(82_400), 3_600_001), Cost(82_401));
    }

    #[test]
    fn a_rate_and_a_duration_beyond_a_u64_product_still_compute() {
        // The product overflows 64 bits; the intermediate is wider, so the
        // quotient is exact.
        let rate = Price(u64::MAX / 2);
        assert_eq!(Cost::accrued(rate, 3_600_000), Cost(u64::MAX / 2));
    }

    #[test]
    fn a_charge_beyond_what_the_unit_holds_saturates() {
        assert_eq!(Cost::accrued(Price(u64::MAX), 7_200_000), Cost(u64::MAX));
    }
}
