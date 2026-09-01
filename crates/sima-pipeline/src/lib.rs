//! Pipeline layer: the human-facing configuration in, a driven run out.
//!
//! A `sima.toml` is loaded and translated into the identity-bearing
//! [`sima_model::SearchConfig`] plus the operational execution settings. The
//! format id resolves through the [`DomainRegistry`] to what answers for it —
//! [`sima_domains`] for the formats this build carries, or the program a
//! `[domain.*]` entry names — which supplies the executor that evaluates the
//! format's specs, the environment that enters task identity, the translation
//! of the domain-owned params section, and the generator the id names with its
//! own config translation. The pipeline routes configuration sections to the
//! code that owns them; it never interprets their content.
//!
//! Beside the driven run sit the read-only queries over what a run left
//! behind. Each merges the run's journal and touches no store object:
//! [`status`] and [`task_history`] project execution — the run's state, and
//! one task's attempts — [`failures`] names the tasks that did not commit,
//! and [`report`] and [`report_task`] render the results committed tasks
//! produced, while [`timeline`] measures how efficiently the run executed.
//! The queries return data; rendering it is the caller's.

mod ceiling;
mod config;
mod devices;
mod domain_registry;
mod feed;
#[cfg(test)]
mod fixtures;
mod fleet;
mod journal;
mod machines;
mod migrate;
mod orchestrate;
mod payload;
mod process;
mod program_binding;
mod program_delivery;
mod providers;
mod remove;
mod rental;
mod report;
mod run_observer;
mod runs;
mod sdk;
mod spend;
mod stamped_tree;
mod stats;
mod status;
mod sync_session;
mod task_history;
mod task_keys;
mod timeline;

pub use config::{
    Container, Fleet, Host, HostClass, HostClassForm, HostForm, LoadedConfig, Orchestrator,
    OwnedClass, OwnedHost, Pool, ProviderId, Rented, RentedClass, load,
};
pub use devices::DeviceSelector;
pub use domain_registry::DomainRegistry;
pub use feed::{
    FOLLOW_PROTOCOL_VERSION, FeedInfo, FollowFrame, LocalFeed, RemoteFeed, RunFeed, follow_serve,
    local_snapshot, remote_snapshot,
};
pub use fleet::Engagement;
pub use machines::machines;
pub use migrate::{MigrateOutcome, migrate, recall, sync_serve};
pub use orchestrate::orchestrate;
pub use program_binding::BinaryChange;
pub use program_delivery::{ProgramDelivery, ingest_program, receive_program};
pub use providers::{ProviderSettings, provider_for};
pub use remove::{remove, remove_matching};
pub use report::{ReportRow, report, report_records, report_task_records};
pub use run_observer::RunObserver;
pub use sdk::Sdk;
pub use spend::spend;
// The rental-ledger and reputation types a caller renders those reports
// through.
pub use sima_provider::{Cost, MachineReport, MachineSummary, OpenSpend, Price, SpendReport};
pub use sima_store::SpendEntry;
// The run identity a query names, re-exported with the rest of the surface a
// caller reads a run through.
pub use runs::{RunSummary, runs};
pub use sima_model::SearchId;
pub use sima_store::RemovalReport;
// The scheduler types a caller drives and observes runs through, re-exported
// so the CLI consumes one coherent surface.
pub use sima_scheduler::{Event, LIVENESS_INTERVAL, Level, Record, RunControl, RunOutcome};
pub use status::{Occupancy, RunState, RunStatus, seeded_status, status, status_records};
pub use task_history::{
    Attempt, AttemptResult, TaskHistory, TaskOutcome, failures_records, task_history_records,
};
pub use task_keys::task_keys;
pub use timeline::{RetryStats, RunTimeline, WorkerMetrics, timeline_records};
