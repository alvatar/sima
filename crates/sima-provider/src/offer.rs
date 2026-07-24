//! The normalized offer model and the selection that ranks it.
//!
//! Selection is two deliberately separate steps: hard constraints
//! disqualify offers, and one scalar objective orders whatever qualifies.
//! Nothing scores an offer across criteria, so the reason an offer was
//! taken is always one comparison.

/// A price in micro-USD per hour: $0.0824/hr is `Price(82_400)`.
///
/// Integer, so ranking is a total order without float comparison. Prices
/// are ephemeral market data and never enter a hash.
///
/// Micro-USD is the unit every provider is normalized to; a backend billing
/// in another currency converts as part of its own configuration, so the
/// rates reaching selection are comparable across providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub u64);

/// A marketplace offer's provider-scoped identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OfferId(pub String);

/// One rentable machine as the marketplace lists it, normalized across
/// providers. A fixed type catalog degenerates into this: one offer per
/// type, at the type's list price. A machine carrying no GPUs states a
/// `gpu_count` of 0, a `vram_mb` of 0, and an empty `gpu_model`, which
/// default constraints admit.
#[derive(Debug, Clone)]
pub struct Offer {
    /// The provider's identifier for this offer.
    pub id: OfferId,
    /// The provider's stable identifier for the physical machine behind this
    /// offer, which reputation is scoped to. Empty when the provider reports
    /// none: an empty machine records no incidents and never matches the
    /// excluded set.
    pub machine: String,
    /// The GPU model as the provider names it, for example `RTX 4090`.
    pub gpu_model: String,
    /// GPUs on the machine.
    pub gpu_count: u32,
    /// VRAM per GPU, in megabytes.
    pub vram_mb: u64,
    /// The hourly rate.
    pub price: Price,
    /// Provider-reported host reliability, in `[0, 1]`. This describes trust
    /// in a marketplace host; a first-party datacenter backend, which reports
    /// nothing of the kind, states 1.0.
    pub reliability: f64,
    /// Whether the provider vetted the host. As with `reliability`, this is
    /// marketplace host trust, and a first-party datacenter backend states
    /// true.
    pub verified: bool,
    /// Disk available to the rental, in gigabytes.
    pub disk_gb: u64,
    /// Downlink bandwidth as the provider reports it, in megabits per second.
    pub bandwidth_mbps: u64,
    /// The provider's region label; empty when it reports none.
    pub location: String,
}

/// Hard constraints: an offer either qualifies or is disqualified. The
/// default disqualifies nothing.
#[derive(Debug, Clone, Default)]
pub struct Constraints {
    /// Acceptable GPU models, any-of, matched case-insensitively by
    /// substring. An empty list admits every model.
    pub gpu_models: Vec<String>,
    /// Fewest GPUs the machine must carry.
    pub min_gpu_count: Option<u32>,
    /// Least VRAM per GPU, in megabytes.
    pub min_vram_mb: Option<u64>,
    /// Highest hourly rate that qualifies.
    pub max_price: Option<Price>,
    /// Least provider-reported reliability.
    pub min_reliability: Option<f64>,
    /// Whether only provider-vetted hosts qualify.
    pub verified_only: bool,
    /// Least disk, in gigabytes.
    pub min_disk_gb: Option<u64>,
    /// Least downlink bandwidth, in megabits per second.
    pub min_bandwidth_mbps: Option<u64>,
    /// Provider-scoped machine identifiers to disqualify. An offer whose
    /// non-empty `machine` is listed is disqualified; the empty machine never
    /// matches, so a machine with no identity is never excluded. Acquisition
    /// fills this from the reputation ledger. An empty list disqualifies
    /// nothing, which is what the default states.
    pub excluded_machines: Vec<String>,
}

/// The single scalar ranking over qualifying offers.
#[derive(Debug, Clone, Copy)]
pub enum Objective {
    /// Ascending price, ties broken by offer id, so ranking is
    /// deterministic.
    CheapestPerHour,
}

/// Filters `offers` by the hard constraints, then ranks the qualifiers by
/// `objective`. The result is the order acquisition walks.
pub fn select(offers: Vec<Offer>, constraints: &Constraints, objective: Objective) -> Vec<Offer> {
    let mut qualifying: Vec<Offer> = offers
        .into_iter()
        .filter(|offer| qualifies(offer, constraints))
        .collect();
    match objective {
        Objective::CheapestPerHour => {
            qualifying.sort_by(|a, b| (a.price, &a.id).cmp(&(b.price, &b.id)));
        }
    }
    qualifying
}

/// Whether `offer` satisfies every constraint. Each `Option` constraint
/// judges only when set.
fn qualifies(offer: &Offer, constraints: &Constraints) -> bool {
    if !constraints.gpu_models.is_empty() && !names_model(&constraints.gpu_models, &offer.gpu_model)
    {
        return false;
    }
    if constraints.verified_only && !offer.verified {
        return false;
    }
    // A machine with a pattern of operational failures is disqualified; a
    // machine with no identity carries an empty string and never matches.
    if !offer.machine.is_empty() && constraints.excluded_machines.contains(&offer.machine) {
        return false;
    }
    let minimums = [
        at_least(offer.gpu_count, constraints.min_gpu_count),
        at_least(offer.vram_mb, constraints.min_vram_mb),
        at_least(offer.disk_gb, constraints.min_disk_gb),
        at_least(offer.bandwidth_mbps, constraints.min_bandwidth_mbps),
        at_least(offer.reliability, constraints.min_reliability),
    ];
    if minimums.contains(&false) {
        return false;
    }
    constraints.max_price.is_none_or(|max| offer.price <= max)
}

/// Whether `value` reaches `minimum`, which judges only when set. The
/// comparison is `>=`, so a provider reporting a reliability of NaN — a
/// value comparable to nothing — falls short of any threshold.
fn at_least<T: PartialOrd>(value: T, minimum: Option<T>) -> bool {
    minimum.is_none_or(|minimum| value >= minimum)
}

/// Whether any of `accepted` names `model`, matched case-insensitively by
/// substring — the rule device selectors use for hardware names.
fn names_model(accepted: &[String], model: &str) -> bool {
    let model = model.to_lowercase();
    accepted
        .iter()
        .any(|candidate| model.contains(&candidate.to_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::{Constraints, Objective, Offer, OfferId, Price, select};

    /// An offer that qualifies under every constraint the tests set, so a
    /// test varies exactly the field it is about.
    fn offer(id: &str) -> Offer {
        Offer {
            id: OfferId(id.to_string()),
            machine: format!("m-{id}"),
            gpu_model: "RTX 4090".to_string(),
            gpu_count: 2,
            vram_mb: 24_576,
            price: Price(500_000),
            reliability: 0.99,
            verified: true,
            disk_gb: 200,
            bandwidth_mbps: 1_000,
            location: "eu-west".to_string(),
        }
    }

    /// The ids `select` returns, in rank order.
    fn ranked(offers: Vec<Offer>, constraints: &Constraints) -> Vec<String> {
        select(offers, constraints, Objective::CheapestPerHour)
            .into_iter()
            .map(|offer| offer.id.0)
            .collect()
    }

    #[test]
    fn the_default_constraints_disqualify_nothing() {
        let offers = vec![
            Offer {
                gpu_model: "GTX 1080".to_string(),
                gpu_count: 1,
                vram_mb: 8_192,
                reliability: 0.1,
                verified: false,
                disk_gb: 10,
                bandwidth_mbps: 5,
                ..offer("modest")
            },
            offer("ample"),
        ];
        assert_eq!(ranked(offers, &Constraints::default()).len(), 2);
    }

    #[test]
    fn an_empty_offer_list_selects_to_empty() {
        assert!(ranked(Vec::new(), &Constraints::default()).is_empty());
    }

    #[test]
    fn min_gpu_count_disqualifies_smaller_machines() {
        let offers = vec![
            Offer {
                gpu_count: 1,
                ..offer("single")
            },
            offer("double"),
        ];
        let constraints = Constraints {
            min_gpu_count: Some(2),
            ..Constraints::default()
        };
        assert_eq!(ranked(offers, &constraints), vec!["double"]);
    }

    #[test]
    fn min_vram_disqualifies_smaller_cards() {
        let offers = vec![
            Offer {
                vram_mb: 8_192,
                ..offer("small")
            },
            offer("large"),
        ];
        let constraints = Constraints {
            min_vram_mb: Some(16_384),
            ..Constraints::default()
        };
        assert_eq!(ranked(offers, &constraints), vec!["large"]);
    }

    #[test]
    fn max_price_disqualifies_dearer_offers_and_admits_the_boundary() {
        let offers = vec![
            Offer {
                price: Price(500_001),
                ..offer("dear")
            },
            offer("boundary"),
        ];
        let constraints = Constraints {
            max_price: Some(Price(500_000)),
            ..Constraints::default()
        };
        assert_eq!(ranked(offers, &constraints), vec!["boundary"]);
    }

    #[test]
    fn min_reliability_disqualifies_weaker_hosts_and_a_nan_report() {
        let offers = vec![
            Offer {
                reliability: 0.5,
                ..offer("shaky")
            },
            Offer {
                reliability: f64::NAN,
                ..offer("unreported")
            },
            offer("steady"),
        ];
        let constraints = Constraints {
            min_reliability: Some(0.9),
            ..Constraints::default()
        };
        // Every comparison against NaN is false, so a host reporting one
        // never reaches the threshold.
        assert_eq!(ranked(offers, &constraints), vec!["steady"]);
    }

    #[test]
    fn verified_only_disqualifies_unvetted_hosts() {
        let offers = vec![
            Offer {
                verified: false,
                ..offer("unvetted")
            },
            offer("vetted"),
        ];
        let constraints = Constraints {
            verified_only: true,
            ..Constraints::default()
        };
        assert_eq!(ranked(offers, &constraints), vec!["vetted"]);
    }

    #[test]
    fn min_disk_disqualifies_smaller_volumes() {
        let offers = vec![
            Offer {
                disk_gb: 20,
                ..offer("cramped")
            },
            offer("roomy"),
        ];
        let constraints = Constraints {
            min_disk_gb: Some(100),
            ..Constraints::default()
        };
        assert_eq!(ranked(offers, &constraints), vec!["roomy"]);
    }

    #[test]
    fn min_bandwidth_disqualifies_slower_links() {
        let offers = vec![
            Offer {
                bandwidth_mbps: 100,
                ..offer("slow")
            },
            offer("fast"),
        ];
        let constraints = Constraints {
            min_bandwidth_mbps: Some(500),
            ..Constraints::default()
        };
        assert_eq!(ranked(offers, &constraints), vec!["fast"]);
    }

    #[test]
    fn a_gpu_model_matches_case_insensitively_by_substring() {
        let offers = vec![
            Offer {
                gpu_model: "NVIDIA GeForce RTX 4090".to_string(),
                ..offer("long-name")
            },
            Offer {
                gpu_model: "RTX 3090".to_string(),
                ..offer("other-model")
            },
        ];
        let constraints = Constraints {
            gpu_models: vec!["rtx 4090".to_string()],
            ..Constraints::default()
        };
        assert_eq!(ranked(offers, &constraints), vec!["long-name"]);
    }

    #[test]
    fn several_gpu_models_admit_any_of_them() {
        let offers = vec![
            Offer {
                gpu_model: "A100 SXM4".to_string(),
                ..offer("a100")
            },
            Offer {
                gpu_model: "H100 PCIe".to_string(),
                ..offer("h100")
            },
            Offer {
                gpu_model: "RTX 4090".to_string(),
                ..offer("consumer")
            },
        ];
        let constraints = Constraints {
            gpu_models: vec!["A100".to_string(), "H100".to_string()],
            ..Constraints::default()
        };
        let ids = ranked(offers, &constraints);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"a100".to_string()));
        assert!(ids.contains(&"h100".to_string()));
    }

    #[test]
    fn an_empty_gpu_model_list_admits_every_model() {
        let offers = vec![
            Offer {
                gpu_model: "Tesla T4".to_string(),
                ..offer("t4")
            },
            offer("rtx"),
        ];
        assert_eq!(ranked(offers, &Constraints::default()).len(), 2);
    }

    #[test]
    fn ranking_is_ascending_by_price_with_ties_broken_by_offer_id() {
        let offers = vec![
            Offer {
                price: Price(900_000),
                ..offer("dear")
            },
            Offer {
                price: Price(100_000),
                ..offer("cheap-b")
            },
            Offer {
                price: Price(100_000),
                ..offer("cheap-a")
            },
        ];
        assert_eq!(
            ranked(offers, &Constraints::default()),
            vec!["cheap-a", "cheap-b", "dear"]
        );
    }

    #[test]
    fn an_excluded_machine_is_disqualified_and_the_empty_machine_never_matches() {
        let offers = vec![
            offer("bad"),
            Offer {
                machine: String::new(),
                ..offer("anonymous")
            },
            offer("good"),
        ];
        let constraints = Constraints {
            // The bad machine and, pointlessly, the empty string are listed.
            excluded_machines: vec!["m-bad".to_string(), String::new()],
            ..Constraints::default()
        };
        let ids = ranked(offers, &constraints);
        // The listed machine is out; the machine with no identity is admitted
        // even though the empty string is "listed", because it never matches.
        assert_eq!(ids, vec!["anonymous", "good"]);
    }

    #[test]
    fn a_disqualified_offer_never_outranks_a_qualifying_one() {
        let offers = vec![
            Offer {
                price: Price(1),
                vram_mb: 4_096,
                ..offer("cheap-and-small")
            },
            offer("adequate"),
        ];
        let constraints = Constraints {
            min_vram_mb: Some(16_384),
            ..Constraints::default()
        };
        assert_eq!(ranked(offers, &constraints), vec!["adequate"]);
    }
}
