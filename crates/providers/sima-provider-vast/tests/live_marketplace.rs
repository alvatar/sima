//! The backend against the live marketplace.
//!
//! The test is ignored by default: it needs a real API key and reaches the
//! network, which the rest of the suite never does. Run it deliberately,
//! with `VAST_API_KEY` set:
//!
//! ```text
//! cargo test -p sima-provider-vast -- --ignored
//! ```
//!
//! It reads and rents nothing. Renting spends money, so a rental belongs to
//! a search that is started on purpose.

use sima_core::Result;
use sima_provider::{Constraints, Objective, Provider, select};
use sima_provider_vast::{VastConfig, VastProvider};

/// The live marketplace normalizes: every offer it lists parses into the
/// normalized model, at a price that is a real rate, and selection ranks
/// what comes back.
#[test]
#[ignore = "reaches the live marketplace and needs a real VAST_API_KEY"]
fn the_live_marketplace_lists_offers_that_normalize() -> Result<()> {
    let provider = VastProvider::new(VastConfig::from_env("ghcr.io/owner/sima-worker", 64)?);
    let offers = provider.offers()?;
    assert!(
        !offers.is_empty(),
        "the marketplace lists on-demand machines at any hour"
    );
    for offer in &offers {
        assert!(!offer.id.0.is_empty(), "an offer is identified");
        assert!(offer.price.0 > 0, "an on-demand machine is charged for");
        assert!(
            (0.0..=1.0).contains(&offer.reliability),
            "reliability is a fraction: {}",
            offer.reliability
        );
    }
    let ranked = select(offers, &Constraints::default(), Objective::CheapestPerHour);
    assert!(
        ranked.windows(2).all(|pair| pair[0].price <= pair[1].price),
        "selection ranks the marketplace by price"
    );
    Ok(())
}
