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
//! ```
//!
//! Every durable file is placed by an atomic write — full content to
//! `tmp/`, fsync, rename, parent-directory fsync — so a reader, including
//! a process resuming after SIGKILL, observes a complete file or none.
//! Store methods take `&self` and are safe under concurrent use: writers
//! racing on one path either converge on identical bytes or fail with
//! `Corruption`.

#[cfg_attr(not(test), allow(dead_code))]
mod atomic;
mod layout;
mod store;

pub use store::Store;
