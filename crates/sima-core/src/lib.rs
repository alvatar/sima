//! Foundations shared by every sima crate: the project error type, the
//! blake3-backed content-identity hash, the canonical byte encoding for
//! identity-bearing data (the [`Codec`] trait over [`Enc`]/[`Dec`]),
//! length-prefixed framing for byte-stream transports, the counter-based
//! deterministic PRNG, the disposition every spawned child is given toward
//! the terminal's signals, and the feature-gated crash-injection points
//! durability tests arm.

pub mod crashpoint;
pub mod encode;
pub mod error;
pub mod frame;
pub mod hash;
pub mod prng;
pub mod process;

pub use crashpoint::crashpoint;
pub use encode::{Codec, Dec, Enc};
pub use error::{Error, Result};
pub use frame::{MAX_PAYLOAD, read_frame, write_frame};
pub use hash::{Hash, Hasher, from_hex, hash_bytes, to_hex};
pub use process::own_process_group;
