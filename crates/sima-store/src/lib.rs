//! Durability layer: content-addressed objects, the task index, per-run
//! manifests and journals, under one fixed disk layout.
//!
//! The store is the only durable state in sima. One [`Store`] handle owns a
//! root directory laid out as:
//!
//! ```text
//! <root>/objects/<aa>/<64-hex>     object bytes; aa = first two hex chars
//! <root>/tmp/<pid>-<seq>           in-flight writes
//! <root>/tasks/<task-key-hex>      index entry: record-hash hex + newline
//! <root>/runs/<run-id-hex>/manifest.json
//! <root>/runs/<run-id-hex>/journal
//! <root>/runs/<run-id-hex>/orchestrator.lock
//! <root>/runs/<run-id-hex>/checkpoint/<slot>   mutable resume scratch
//! ```
//!
//! Every durable file is placed atomically — full content to `tmp/`,
//! fsync, then into place with a parent-directory fsync: objects enter by
//! rename, index entries and manifests by a hard link that fails when the
//! destination already exists. Every directory is created with its parent
//! fsynced, so a reader, including a process resuming after SIGKILL,
//! observes a complete file or none. Store methods take `&self` and are
//! safe under concurrent use: writers racing on one path converge on
//! identical bytes, and a conflicting racer fails with `Corruption`.

mod atomic;
mod cas;
mod catalog;
mod checkpoint;
mod journal;
mod layout;
mod lock;
mod manifest;
mod store;
#[cfg(test)]
mod testutil;

pub use journal::JournalWriter;
pub use lock::RunLock;
pub use manifest::{Manifest, ManifestEntry};
pub use store::Store;
