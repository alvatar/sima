//! Foundations shared by every sima crate: the project error type, the
//! blake3-backed content-identity hash, the canonical byte encoding for
//! identity-bearing data (with the [`Codec`] derive), the translation of
//! human-readable TOML config into validated structs (with the [`TomlConfig`]
//! derive), the counter-based deterministic PRNG, and the feature-gated
//! crash-injection points durability tests arm.

pub mod crashpoint;
pub mod encode;
pub mod error;
pub mod hash;
pub mod prng;
pub mod toml_config;

pub use crashpoint::crashpoint;
pub use encode::{Codec, Dec, Enc};
pub use error::{Error, Result};
pub use hash::{Hash, hash_bytes, to_hex};
pub use toml_config::TomlConfig;
// The `Codec`/`TomlConfig` derives share their trait names, so
// `use sima_core::{Codec, TomlConfig};` brings both the trait and its derive
// into scope, as `serde` does for `Serialize`.
pub use sima_core_derive::{Codec, TomlConfig};
