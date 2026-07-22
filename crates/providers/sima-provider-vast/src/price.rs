//! The conversion from the marketplace's rates to the normalized [`Price`].

use sima_provider::Price;

/// Micro-USD in one dollar, the unit [`Price`] normalizes to.
const MICRO_USD_PER_USD: f64 = 1_000_000.0;

/// `dollars_per_hour` as a normalized price, rounded to the nearest
/// micro-USD, or `None` for a rate no price can be read from.
///
/// A rate below zero, NaN, or infinite is a marketplace answer this backend
/// cannot interpret. The float-to-integer cast would saturate each of them
/// to zero, and a machine priced at zero is ranked first, rented, and
/// charged nothing — every consequence undercounts, so the conversion
/// refuses instead. A true zero converts: a machine that costs nothing
/// costs nothing.
pub(crate) fn per_hour(dollars_per_hour: f64) -> Option<Price> {
    if !dollars_per_hour.is_finite() || dollars_per_hour < 0.0 {
        return None;
    }
    Some(Price((dollars_per_hour * MICRO_USD_PER_USD).round() as u64))
}

#[cfg(test)]
mod tests {
    use super::per_hour;
    use sima_provider::Price;

    #[test]
    fn a_rate_in_dollars_becomes_micro_usd_per_hour() {
        assert_eq!(per_hour(0.412), Some(Price(412_000)));
    }

    #[test]
    fn a_rate_below_a_micro_usd_rounds_to_the_nearest() {
        assert_eq!(per_hour(0.1234565), Some(Price(123_457)));
        assert_eq!(per_hour(0.0000004), Some(Price(0)));
    }

    #[test]
    fn a_machine_that_costs_nothing_converts_to_a_price_of_nothing() {
        assert_eq!(per_hour(0.0), Some(Price(0)));
    }

    #[test]
    fn a_rate_no_price_can_be_read_from_converts_to_nothing() {
        assert_eq!(per_hour(-0.1), None);
        assert_eq!(per_hour(f64::NAN), None);
        assert_eq!(per_hour(f64::INFINITY), None);
        assert_eq!(per_hour(f64::NEG_INFINITY), None);
    }
}
