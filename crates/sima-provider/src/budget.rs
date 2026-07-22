//! The money a run spends on rented machines: the unit it is counted in
//! and the arithmetic that turns a rate and a duration into an amount.

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
    /// fraction counts, so the amount is at least what the provider bills
    /// and never less.
    pub fn accrued(rate: Price, elapsed_ms: u64) -> Cost {
        // The product of a rate and a duration leaves 64 bits at extreme
        // values, so the multiplication happens wide and the quotient comes
        // back clamped.
        let micro = (rate.0 as u128 * elapsed_ms as u128).div_ceil(MS_PER_HOUR);
        Cost(micro.min(u64::MAX as u128) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::Cost;
    use crate::offer::Price;

    #[test]
    fn a_full_hour_charges_the_rate() {
        assert_eq!(Cost::accrued(Price(82_400), 3_600_000), Cost(82_400));
    }

    #[test]
    fn a_window_of_no_time_charges_nothing() {
        assert_eq!(Cost::accrued(Price(82_400), 0), Cost(0));
        // A rate of nothing charges nothing however long it runs.
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
