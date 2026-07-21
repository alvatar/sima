//! The conversion from the marketplace's rates to the normalized [`Price`].

use sima_provider::Price;

/// Micro-USD in one dollar, the unit [`Price`] normalizes to.
const MICRO_USD_PER_USD: f64 = 1_000_000.0;

/// `dollars_per_hour` as a normalized price, rounded to the nearest
/// micro-USD.
pub(crate) fn per_hour(dollars_per_hour: f64) -> Price {
    Price((dollars_per_hour * MICRO_USD_PER_USD).round() as u64)
}

#[cfg(test)]
mod tests {
    use super::per_hour;
    use sima_provider::Price;

    #[test]
    fn a_rate_in_dollars_becomes_micro_usd_per_hour() {
        assert_eq!(per_hour(0.412), Price(412_000));
    }

    #[test]
    fn a_rate_below_a_micro_usd_rounds_to_the_nearest() {
        assert_eq!(per_hour(0.1234565), Price(123_457));
        assert_eq!(per_hour(0.0000004), Price(0));
    }
}
