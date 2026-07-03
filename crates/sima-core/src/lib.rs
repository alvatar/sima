//! Foundations shared by every sima crate: the project error type, the
//! blake3-backed content-identity hash, the canonical byte encoding for
//! identity-bearing data, and the counter-based deterministic PRNG.

pub mod encode;
pub mod error;
pub mod hash;
pub mod prng;

pub use encode::{Dec, Enc};
pub use error::{Error, Result};
pub use hash::{Hash, hash_bytes};
