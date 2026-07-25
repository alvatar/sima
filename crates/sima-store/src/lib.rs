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
//! <root>/instances/<tag>           one rented instance's ledger record
//! <root>/spend/<owner-hex>/<tag>-<started-ms>   one closed rental's cost
//! <root>/machines/<provider>-<machine>/<tag>-<occurred-ms>   one incident
//! <root>/runs/<run-id-hex>/manifest.json
//! <root>/runs/<run-id-hex>/journal
//! <root>/runs/<run-id-hex>/orchestrator.lock
//! <root>/runs/<run-id-hex>/checkpoint/<slot>   mutable resume scratch
//! <root>/runs/<run-id-hex>/placement/<chain>   mutable chain device binding
//! <root>/runs/<run-id-hex>/remove-intent       resumable removal plan
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
mod instances;
mod journal;
mod layout;
mod lock;
mod machines;
mod manifest;
mod placement;
mod retention;
mod spend;
mod store;
mod sync;
#[cfg(test)]
mod testutil;

pub use instances::{InstanceRecord, InstanceRecordState};
pub use journal::JournalWriter;
pub use lock::RunLock;
pub use machines::{IncidentKind, MachineIncident};
pub use manifest::{Manifest, ManifestEntry};
pub use retention::RemovalReport;
pub use spend::SpendEntry;
pub use store::Store;
pub use sync::{SYNC_PROTOCOL_VERSION, SyncReport, SyncRole};
