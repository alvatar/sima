//! Foundations shared by every sima crate: the project error type, the
//! blake3-backed content-identity hash, the canonical byte encoding for
//! identity-bearing data (the [`Codec`] trait over [`Enc`]/[`Dec`]), the
//! counter-based deterministic PRNG, and the feature-gated crash-injection
//! points durability tests arm.

pub mod crashpoint;
pub mod encode;
pub mod error;
pub mod hash;
pub mod prng;

pub use crashpoint::crashpoint;
pub use encode::{Codec, Dec, Enc};
pub use error::{Error, Result};
pub use hash::{Hash, hash_bytes, to_hex};
