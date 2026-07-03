//! Project-wide error and result types.
//!
//! Every fallible sima API returns [`Result`]. The error surface is a small
//! closed enum; variants are added when a milestone introduces a new failure
//! class.

use std::fmt;

/// Error type shared by all sima crates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Canonical encoding or decoding failed: truncated input, bad framing,
    /// malformed hex. The payload describes what was expected and found.
    Encoding(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Encoding(msg) => write!(f, "encoding error: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

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
}
