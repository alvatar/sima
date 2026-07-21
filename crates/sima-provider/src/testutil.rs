//! Fixtures shared by the crate's test modules.

use crate::offer::{Offer, OfferId, Price};

/// An offer at `price` micro-USD per hour, with hardware ample enough that
/// default constraints admit it.
pub(crate) fn stub_offer(id: &str, price: u64) -> Offer {
    Offer {
        id: OfferId(id.to_string()),
        gpu_model: "RTX 4090".to_string(),
        gpu_count: 1,
        vram_mb: 24_576,
        price: Price(price),
        reliability: 0.99,
        verified: true,
        disk_gb: 100,
        bandwidth_mbps: 1_000,
        location: "eu-west".to_string(),
    }
}
