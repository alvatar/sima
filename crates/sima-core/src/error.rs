//! Project-wide error and result types.
//!
//! Every fallible sima API returns [`Result`]. The error surface is a small
//! closed enum; variants are added when a milestone introduces a new failure
//! class.

use std::fmt;
use std::path::PathBuf;

use crate::hash::Hash;

/// Error type shared by all sima crates.
///
/// This enum is the single place failure classes accrue: later milestones
/// add variants here (store I/O, corruption, lease conflicts, ...) instead
/// of defining crate-local error types. Only `Debug` is derived: variants
/// carrying non-comparable payloads such as `io::Error` are coming, so
/// nothing may rely on `Error` being cloneable or comparable.
#[derive(Debug)]
pub enum Error {
    /// Canonical encoding or decoding failed: truncated input, bad framing,
    /// malformed hex. The payload describes what was expected and found.
    Encoding(String),
    /// A value violates a model invariant, or a caller misuses a store
    /// API: malformed name, duplicate or unsorted components, empty
    /// environment, finalizing an uncreated run, journal payloads breaking
    /// the framing rules. The payload names the violated invariant.
    Validation(String),
    /// An OS-level filesystem failure while touching `path`.
    Io {
        /// The path the failing operation touched.
        path: PathBuf,
        /// The underlying OS error.
        source: std::io::Error,
    },
    /// Store content contradicts a store invariant: object bytes hashing
    /// differently from their address, a dangling index entry, a record
    /// whose identity key differs from its index path, a malformed index
    /// entry or manifest, a conflicting rewrite of either. The payload
    /// describes the contradiction.
    Corruption(String),
    /// A requested or referenced object is absent from the CAS. Distinct
    /// from [`Error::Corruption`] because absence here is recoverable —
    /// store sync negotiates exactly this class.
    MissingObject(Hash),
    /// A GPU execution backend failed: shader compilation, device or queue
    /// setup, memory allocation, or command submission. The payload names the
    /// operation and the underlying cause.
    Gpu(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Encoding(msg) => write!(f, "encoding error: {msg}"),
            Error::Validation(msg) => write!(f, "validation error: {msg}"),
            Error::Io { path, source } => write!(f, "io error: {}: {source}", path.display()),
            Error::Corruption(msg) => write!(f, "store corruption: {msg}"),
            Error::MissingObject(hash) => write!(f, "missing object: {hash}"),
            Error::Gpu(msg) => write!(f, "gpu error: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Result alias used by all fallible sima APIs.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time assertion: `Error` stays thread-safe and boxable.
    const _: fn() = || {
        fn assert_bounds<T: Send + Sync + 'static>() {}
        assert_bounds::<Error>();
    };

    #[test]
    fn display_renders_variant_context() {
        let e = Error::Encoding("truncated at byte 3".to_string());
        assert_eq!(e.to_string(), "encoding error: truncated at byte 3");
    }

    #[test]
    fn display_renders_validation_context() {
        let e = Error::Validation("name must be 1..=64 bytes".to_string());
        assert_eq!(e.to_string(), "validation error: name must be 1..=64 bytes");
    }

    #[test]
    fn display_renders_io_context() {
        let e = Error::Io {
            path: std::path::PathBuf::from("/store/objects"),
            source: std::io::Error::other("disk gone"),
        };
        assert_eq!(e.to_string(), "io error: /store/objects: disk gone");
    }

    #[test]
    fn display_renders_corruption_context() {
        let e = Error::Corruption("object bytes hash differently".to_string());
        assert_eq!(
            e.to_string(),
            "store corruption: object bytes hash differently"
        );
    }

    #[test]
    fn display_renders_gpu_context() {
        let e = Error::Gpu("compile WGSL: unexpected token".to_string());
        assert_eq!(e.to_string(), "gpu error: compile WGSL: unexpected token");
    }

    #[test]
    fn display_renders_missing_object_context() {
        let h = crate::hash::hash_bytes(b"absent");
        let e = Error::MissingObject(h);
        assert_eq!(e.to_string(), format!("missing object: {h}"));
    }
}
