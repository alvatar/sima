//! The marketplace search and the normalization of what it answers.
//!
//! The query narrows to what defines this backend's scope — rentable
//! machines rented on demand — and nothing else: hard constraints and
//! ranking belong to [`select`](sima_provider::select), so every offer the
//! marketplace lists reaches the caller normalized.

use serde::Deserialize;
use sima_core::{Error, Result};
use sima_provider::{Offer, OfferId, Price};

use crate::client::VastClient;

/// The search endpoint.
const SEARCH_PATH: &str = "/api/v0/bundles/";

/// What the search is called in failures.
const OPERATION: &str = "list offers";

/// Micro-USD in one dollar, the unit [`Price`] normalizes to.
const MICRO_USD_PER_USD: f64 = 1_000_000.0;

/// The verification the marketplace reports for a host it vetted.
const VERIFIED: &str = "verified";

/// The search's answer.
#[derive(Deserialize)]
struct OfferPage {
    /// The offers matching the query.
    offers: Vec<OfferRow>,
}

/// One offer as the marketplace reports it. The type never leaves this
/// crate: what crosses the boundary is the normalized [`Offer`].
#[derive(Deserialize)]
struct OfferRow {
    /// The marketplace's offer identifier.
    id: i64,
    /// The GPU model, in the marketplace's own naming.
    gpu_name: String,
    /// GPUs on the machine.
    num_gpus: u32,
    /// VRAM per GPU, in megabytes.
    gpu_ram: f64,
    /// The hourly rate, in dollars.
    dph_total: f64,
    /// Host reliability, in `[0, 1]`.
    reliability: f64,
    /// Whether the marketplace vetted the host.
    verification: String,
    /// Disk available to the rental, in gigabytes.
    disk_space: f64,
    /// Downlink bandwidth, in megabits per second.
    inet_down: f64,
    /// The host's region, which the marketplace reports as null when it
    /// holds none.
    geolocation: Option<String>,
}

/// The marketplace's current on-demand offers, normalized.
pub(crate) fn search(client: &VastClient) -> Result<Vec<Offer>> {
    let query = serde_json::json!({"rentable": {"eq": true}, "type": "ondemand"});
    let body = client.post(SEARCH_PATH, &query, OPERATION)?.ok(OPERATION)?;
    // An offer missing a field is a marketplace answer this backend cannot
    // interpret, so the whole listing fails naming the field rather than
    // silently dropping a machine that might have been the cheapest.
    let page: OfferPage = serde_json::from_value(body)
        .map_err(|failure| Error::Provider(format!("{OPERATION}: {failure}")))?;
    Ok(page.offers.into_iter().map(normalize).collect())
}

/// The normalized form of one marketplace row.
fn normalize(row: OfferRow) -> Offer {
    Offer {
        id: OfferId(row.id.to_string()),
        gpu_model: row.gpu_name,
        gpu_count: row.num_gpus,
        vram_mb: row.gpu_ram.round() as u64,
        price: Price((row.dph_total * MICRO_USD_PER_USD).round() as u64),
        reliability: row.reliability,
        verified: row.verification == VERIFIED,
        disk_gb: row.disk_space.round() as u64,
        bandwidth_mbps: row.inet_down.round() as u64,
        location: row.geolocation.unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::search;
    use crate::client::VastClient;
    use crate::test_server::{ScriptedAnswer, TestServer};
    use sima_core::{Error, Result};
    use sima_provider::{OfferId, Price};

    /// A page carrying `offers` verbatim, as the search answers it.
    fn page(offers: &str) -> Vec<ScriptedAnswer> {
        vec![ScriptedAnswer {
            status: 200,
            body: format!(r#"{{"offers": [{offers}]}}"#),
        }]
    }

    /// A nominal offer: a vetted host with a region and a round rate.
    const NOMINAL: &str = r#"{
        "id": 8123456,
        "gpu_name": "RTX_4090",
        "num_gpus": 2,
        "gpu_ram": 24564.0,
        "dph_total": 0.412,
        "reliability": 0.9871,
        "verification": "verified",
        "disk_space": 205.7,
        "inet_down": 1350.4,
        "geolocation": "Warsaw, PL"
    }"#;

    /// An offer from a host the marketplace did not vet and reports no
    /// region for, at a rate whose micro-USD value needs rounding.
    const UNVETTED: &str = r#"{
        "id": 9000001,
        "gpu_name": "RTX_3090",
        "num_gpus": 1,
        "gpu_ram": 24576.0,
        "dph_total": 0.1234565,
        "reliability": 0.5,
        "verification": "unverified",
        "disk_space": 50.0,
        "inet_down": 400.0,
        "geolocation": null
    }"#;

    #[test]
    fn the_search_asks_for_rentable_on_demand_machines() -> Result<()> {
        let server = TestServer::new(page(""));
        let client = VastClient::new(&server.url(), "k-secret");
        search(&client)?;
        let request = &server.requests()[0];
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/api/v0/bundles/");
        let query = request.json();
        assert_eq!(query["rentable"]["eq"], true);
        assert_eq!(query["type"], "ondemand");
        Ok(())
    }

    #[test]
    fn a_listed_offer_normalizes_field_by_field() -> Result<()> {
        let server = TestServer::new(page(NOMINAL));
        let client = VastClient::new(&server.url(), "k-secret");
        let offers = search(&client)?;
        assert_eq!(offers.len(), 1);
        let offer = &offers[0];
        assert_eq!(offer.id, OfferId("8123456".to_string()));
        // The model keeps the marketplace's naming: constraints match it
        // case-insensitively by substring.
        assert_eq!(offer.gpu_model, "RTX_4090");
        assert_eq!(offer.gpu_count, 2);
        assert_eq!(offer.vram_mb, 24_564);
        assert_eq!(offer.price, Price(412_000));
        assert!((offer.reliability - 0.9871).abs() < f64::EPSILON);
        assert!(offer.verified);
        assert_eq!(offer.disk_gb, 206);
        assert_eq!(offer.bandwidth_mbps, 1_350);
        assert_eq!(offer.location, "Warsaw, PL");
        Ok(())
    }

    #[test]
    fn an_unvetted_host_without_a_region_normalizes_to_an_empty_location() -> Result<()> {
        let server = TestServer::new(page(UNVETTED));
        let client = VastClient::new(&server.url(), "k-secret");
        let offers = search(&client)?;
        assert!(!offers[0].verified);
        assert_eq!(offers[0].location, "");
        Ok(())
    }

    #[test]
    fn a_fractional_hourly_rate_rounds_to_the_nearest_micro_usd() -> Result<()> {
        let server = TestServer::new(page(UNVETTED));
        let client = VastClient::new(&server.url(), "k-secret");
        let offers = search(&client)?;
        // $0.1234565/hr is 123_456.5 micro-USD, which rounds up.
        assert_eq!(offers[0].price, Price(123_457));
        Ok(())
    }

    #[test]
    fn every_listed_offer_reaches_the_caller() -> Result<()> {
        let server = TestServer::new(page(&format!("{NOMINAL},{UNVETTED}")));
        let client = VastClient::new(&server.url(), "k-secret");
        assert_eq!(search(&client)?.len(), 2);
        Ok(())
    }

    #[test]
    fn an_offer_missing_a_field_fails_the_listing_naming_the_field() {
        let malformed = r#"{
            "id": 7,
            "num_gpus": 1,
            "gpu_ram": 24576.0,
            "dph_total": 0.2,
            "reliability": 0.9,
            "verification": "verified",
            "disk_space": 50.0,
            "inet_down": 400.0,
            "geolocation": null
        }"#;
        let server = TestServer::new(page(malformed));
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            search(&client),
            Err(Error::Provider(message))
                if message.starts_with("list offers: ") && message.contains("gpu_name")
        ));
    }

    #[test]
    fn a_failing_search_reaches_the_caller_as_the_api_named_it() {
        let server = TestServer::new(vec![ScriptedAnswer {
            status: 401,
            body: r#"{"success": false, "error": "invalid_api_key"}"#.to_string(),
        }]);
        let client = VastClient::new(&server.url(), "k-wrong");
        assert!(matches!(
            search(&client),
            Err(Error::Provider(message))
                if message == "list offers: HTTP 401: invalid_api_key"
        ));
    }
}
