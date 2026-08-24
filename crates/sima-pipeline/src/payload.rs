//! The program a run carries with it: what a config declares travels, and the
//! content-addressed form it travels as.
//!
//! A `[domain.*]` entry routes a format to a program on this machine. Moving
//! that run onto another machine therefore has to move the program too, and
//! the payload is how: the files the entry names become ordinary store
//! objects, one manifest object names them, and the manifest's hash is the
//! digest the far config states. The sync that already carries a run's closure
//! carries these objects with it, so nothing is published and no image is
//! rebuilt.
//!
//! The module sits in the pipeline because a payload is a config-driven
//! concept; the store below it stays generic bytes.

use std::path::PathBuf;

/// What a `[domain.*]` entry declares travels: the payload itself, and the
/// script that turns it into a program on the destination.
///
/// Both paths are resolved against the config file's directory, the rule every
/// path in a config follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PayloadSpec {
    /// One file or one directory. A single file is the program; a directory is
    /// whatever `install` makes of it.
    pub(crate) payload: PathBuf,
    /// The shell script the destination runs over the materialized payload.
    /// `None` for a single-file payload, which is its own entry point.
    pub(crate) install: Option<PathBuf>,
}
