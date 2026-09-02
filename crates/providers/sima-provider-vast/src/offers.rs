//! The marketplace search and the normalization of what it answers.
//!
//! The query combines this backend's market scope with compatible hard
//! constraints to reduce the response. [`select`](sima_provider::select)
//! applies the complete constraints and ranking locally, so the marketplace
//! remains a narrowing step rather than the authority on qualification.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sima_core::{Error, Result};
use sima_provider::{Constraints, Offer, OfferId, Price};

use crate::client::{LIST_TIMEOUT, VastClient};
use crate::price;

/// The search endpoint.
const SEARCH_PATH: &str = "/api/v0/bundles/";

/// What the search is called in failures.
const OPERATION: &str = "list offers";

/// The verification the marketplace reports for a host it vetted.
const VERIFIED: &str = "verified";

/// How many offers the search asks for. Without an explicit limit the
/// marketplace answers one small default page (64 rows), an arbitrary sliver
/// of the market that selection would then rank as if it were the whole
/// listing. The full marketplace lists on the order of 2,500 offers; this
/// bound covers it with headroom while keeping the answer a few megabytes.
const SEARCH_LIMIT: u32 = 4096;

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
    /// The marketplace's stable identifier for the physical machine behind
    /// the offer, which reputation is scoped to.
    machine_id: i64,
    /// The GPU model, in the marketplace's own naming.
    gpu_name: String,
    /// GPUs on the machine.
    num_gpus: u32,
    /// VRAM per GPU, in megabytes.
    gpu_ram: f64,
    /// Highest CUDA level the installed driver is known to support. Null and
    /// absent both mean the marketplace reports none.
    #[serde(default)]
    cuda_max_good: Option<f64>,
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

/// The marketplace query narrowed by the compatible parts of `narrowing`.
fn query(narrowing: &Constraints) -> Value {
    let mut query = Map::from_iter([
        ("rentable".to_string(), serde_json::json!({"eq": true})),
        ("type".to_string(), Value::String("ondemand".to_string())),
        (
            "order".to_string(),
            serde_json::json!([["dph_total", "asc"]]),
        ),
        ("limit".to_string(), Value::from(SEARCH_LIMIT)),
    ]);
    insert_bound(&mut query, "num_gpus", "gte", narrowing.min_gpu_count);
    insert_bound(&mut query, "gpu_ram", "gte", narrowing.min_vram_mb);
    insert_bound(&mut query, "cuda_max_good", "gte", narrowing.min_cuda);
    if let Some(price) = narrowing.max_price {
        insert_bound(
            &mut query,
            "dph_total",
            "lte",
            Some(dollars_rounded_up_to_cent(price)),
        );
    }
    insert_bound(&mut query, "reliability2", "gte", narrowing.min_reliability);
    if narrowing.verified_only {
        insert_bound(&mut query, "verified", "eq", Some(true));
    }
    insert_bound(&mut query, "disk_space", "gte", narrowing.min_disk_gb);
    insert_bound(&mut query, "inet_down", "gte", narrowing.min_bandwidth_mbps);
    Value::Object(query)
}

/// Inserts a marketplace comparison when the local constraint has a value.
fn insert_bound<T: Serialize>(
    query: &mut Map<String, Value>,
    key: &str,
    operator: &str,
    value: Option<T>,
) {
    if let Some(value) = value {
        query.insert(
            key.to_string(),
            Value::Object(Map::from_iter([(
                operator.to_string(),
                serde_json::json!(value),
            )])),
        );
    }
}

/// Converts a micro-USD rate to dollars, rounding the server-side bound up
/// to a cent so local selection remains authoritative.
fn dollars_rounded_up_to_cent(Price(micro_usd): Price) -> f64 {
    let cents = micro_usd.div_ceil(10_000);
    cents as f64 / 100.0
}

/// The marketplace's current on-demand offers, normalized.
pub(crate) fn search(client: &VastClient, narrowing: &Constraints) -> Result<Vec<Offer>> {
    let query = query(narrowing);
    let body = client
        .post_with_timeout(SEARCH_PATH, &query, OPERATION, LIST_TIMEOUT)?
        .ok(OPERATION)?;
    // An offer missing a field is a marketplace answer this backend cannot
    // interpret, so the whole listing fails naming the field rather than
    // silently dropping a machine that might have been the cheapest. A rate
    // no price can be read from is narrower — one row the marketplace
    // answered anomalously — and costs that row alone.
    let page: OfferPage = serde_json::from_value(body)
        .map_err(|failure| Error::Provider(format!("{OPERATION}: {failure}")))?;
    Ok(page.offers.into_iter().filter_map(normalize).collect())
}

/// The normalized form of one marketplace row, or `None` for a row whose
/// rate no price can be read from: ranking rests on the price, and an offer
/// priced by an anomalous rate would rank as free and be rented first.
fn normalize(row: OfferRow) -> Option<Offer> {
    Some(Offer {
        id: OfferId(row.id.to_string()),
        machine: row.machine_id.to_string(),
        gpu_model: row.gpu_name,
        gpu_count: row.num_gpus,
        vram_mb: row.gpu_ram.round() as u64,
        cuda: row.cuda_max_good.unwrap_or(0.0),
        price: price::per_hour(row.dph_total)?,
        reliability: row.reliability,
        verified: row.verification == VERIFIED,
        disk_gb: row.disk_space.round() as u64,
        bandwidth_mbps: row.inet_down.round() as u64,
        location: row.geolocation.unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::{query, search};
    use crate::client::VastClient;
    use crate::test_server::{ScriptedAnswer, TestServer};
    use sima_core::{Error, Result};
    use sima_provider::{Constraints, OfferId, Price};

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
        "machine_id": 81234,
        "gpu_name": "RTX 4090",
        "num_gpus": 2,
        "gpu_ram": 24564.0,
        "cuda_max_good": 12.4,
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
        "machine_id": 90000,
        "gpu_name": "RTX 3090",
        "num_gpus": 1,
        "gpu_ram": 24576.0,
        "cuda_max_good": null,
        "dph_total": 0.1234565,
        "reliability": 0.5,
        "verification": "unverified",
        "disk_space": 50.0,
        "inet_down": 400.0,
        "geolocation": null
    }"#;

    #[test]
    fn the_default_query_carries_only_the_market_scope_and_order() {
        assert_eq!(
            query(&Constraints::default()),
            serde_json::json!({
                "rentable": {"eq": true},
                "type": "ondemand",
                "order": [["dph_total", "asc"]],
                "limit": 4096,
            })
        );
    }

    #[test]
    fn the_query_maps_every_compatible_constraint() {
        let constraints = Constraints {
            min_gpu_count: Some(2),
            min_vram_mb: Some(24_576),
            min_cuda: Some(12.2),
            max_price: Some(Price(450_000)),
            min_reliability: Some(0.98),
            verified_only: true,
            min_disk_gb: Some(80),
            min_bandwidth_mbps: Some(200),
            ..Constraints::default()
        };
        assert_eq!(
            query(&constraints),
            serde_json::json!({
                "rentable": {"eq": true},
                "type": "ondemand",
                "order": [["dph_total", "asc"]],
                "limit": 4096,
                "num_gpus": {"gte": 2},
                "gpu_ram": {"gte": 24_576},
                "cuda_max_good": {"gte": 12.2},
                "dph_total": {"lte": 0.45},
                "reliability2": {"gte": 0.98},
                "verified": {"eq": true},
                "disk_space": {"gte": 80},
                "inet_down": {"gte": 200},
            })
        );
    }

    #[test]
    fn the_price_bound_rounds_up_to_the_next_cent() {
        let constraints = Constraints {
            max_price: Some(Price(450_001)),
            ..Constraints::default()
        };
        assert_eq!(query(&constraints)["dph_total"]["lte"], 0.46);
    }

    #[test]
    fn provider_incompatible_constraints_stay_local() {
        let constraints = Constraints {
            gpu_models: vec!["RTX 4090".to_string()],
            excluded_machines: vec!["machine-7".to_string()],
            ..Constraints::default()
        };
        assert_eq!(query(&constraints), query(&Constraints::default()));
    }

    #[test]
    fn false_verified_only_emits_no_verification_filter() {
        let constraints = Constraints {
            verified_only: false,
            ..Constraints::default()
        };
        assert!(query(&constraints).get("verified").is_none());
    }

    #[test]
    fn the_search_sends_the_narrowing_query_verbatim() -> Result<()> {
        let constraints = Constraints {
            min_gpu_count: Some(2),
            max_price: Some(Price(450_001)),
            ..Constraints::default()
        };
        let server = TestServer::new(page(""));
        let client = VastClient::new(&server.url(), "k-secret");
        search(&client, &constraints)?;
        assert_eq!(server.requests()[0].json(), query(&constraints));
        Ok(())
    }

    #[test]
    fn the_search_asks_for_rentable_on_demand_machines() -> Result<()> {
        let server = TestServer::new(page(""));
        let client = VastClient::new(&server.url(), "k-secret");
        search(&client, &Constraints::default())?;
        let request = &server.requests()[0];
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/api/v0/bundles/");
        let query = request.json();
        assert_eq!(query["rentable"]["eq"], true);
        assert_eq!(query["type"], "ondemand");
        assert_eq!(query["limit"], 4096);
        Ok(())
    }

    #[test]
    fn a_listed_offer_normalizes_field_by_field() -> Result<()> {
        let server = TestServer::new(page(NOMINAL));
        let client = VastClient::new(&server.url(), "k-secret");
        let offers = search(&client, &Constraints::default())?;
        assert_eq!(offers.len(), 1);
        let offer = &offers[0];
        assert_eq!(offer.id, OfferId("8123456".to_string()));
        // The machine id is the marketplace's stable per-machine identifier,
        // normalized to its decimal string; reputation is scoped to it.
        assert_eq!(offer.machine, "81234");
        // The model keeps the marketplace's naming: constraints match it
        // case-insensitively by substring.
        assert_eq!(offer.gpu_model, "RTX 4090");
        assert_eq!(offer.gpu_count, 2);
        assert_eq!(offer.vram_mb, 24_564);
        assert!((offer.cuda - 12.4).abs() < f64::EPSILON);
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
        let offers = search(&client, &Constraints::default())?;
        assert!(!offers[0].verified);
        assert_eq!(offers[0].cuda, 0.0);
        assert_eq!(offers[0].location, "");
        Ok(())
    }

    #[test]
    fn a_fractional_hourly_rate_rounds_to_the_nearest_micro_usd() -> Result<()> {
        let server = TestServer::new(page(UNVETTED));
        let client = VastClient::new(&server.url(), "k-secret");
        let offers = search(&client, &Constraints::default())?;
        // $0.1234565/hr is 123_456.5 micro-USD, which rounds up.
        assert_eq!(offers[0].price, Price(123_457));
        Ok(())
    }

    #[test]
    fn every_listed_offer_reaches_the_caller() -> Result<()> {
        let server = TestServer::new(page(&format!("{NOMINAL},{UNVETTED}")));
        let client = VastClient::new(&server.url(), "k-secret");
        assert_eq!(search(&client, &Constraints::default())?.len(), 2);
        Ok(())
    }

    #[test]
    fn an_offer_whose_rate_does_not_convert_to_a_price_is_omitted() -> Result<()> {
        // A rate below zero is a marketplace answer no price can be read
        // from, and an offer whose price is unknown is not rentable.
        let anomalous = NOMINAL.replace(r#""dph_total": 0.412"#, r#""dph_total": -0.5"#);
        let server = TestServer::new(page(&format!("{anomalous},{UNVETTED}")));
        let client = VastClient::new(&server.url(), "k-secret");
        let offers = search(&client, &Constraints::default())?;
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].id, OfferId("9000001".to_string()));
        Ok(())
    }

    #[test]
    fn an_offer_missing_its_machine_id_fails_the_listing_naming_the_field() {
        // A machine id is what reputation is scoped to, so a row without one
        // is a marketplace answer this backend cannot interpret: the whole
        // listing fails naming the field, consistent with every other row
        // field.
        let malformed = r#"{
            "id": 7,
            "gpu_name": "RTX 4090",
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
            search(&client, &Constraints::default()),
            Err(Error::Provider(message))
                if message.starts_with("list offers: ") && message.contains("machine_id")
        ));
    }

    #[test]
    fn an_offer_missing_a_field_fails_the_listing_naming_the_field() {
        let malformed = r#"{
            "id": 7,
            "machine_id": 81234,
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
            search(&client, &Constraints::default()),
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
            search(&client, &Constraints::default()),
            Err(Error::Provider(message))
                if message == "list offers: HTTP 401: invalid_api_key"
        ));
    }
}
