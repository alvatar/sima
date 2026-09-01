//! Pipeline layer: the human-facing configuration in, a driven search out.
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
//! Beside the driven search sit the read-only queries over what a search left
//! behind. Each merges the search's journal and touches no store object:
//! [`status`] and [`task_history`] project execution — the search's state, and
//! one task's attempts — [`failures`] names the tasks that did not commit,
//! and [`report`] and [`report_task`] render the results committed tasks
//! produced, while [`timeline`] measures how efficiently the search executed.
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
mod sdk;
mod search_observer;
mod searches;
mod spend;
mod stamped_tree;
mod stats;
mod status;
mod sync_session;
mod task_history;
mod task_keys;
mod timeline;

pub use config::{
    Container, ExecConfig, Fleet, Host, HostClass, HostClassForm, HostForm, LoadedConfig,
    Orchestrator, OwnedClass, OwnedHost, Pool, ProviderId, Rented, RentedClass, load, load_exec,
};
pub use devices::DeviceSelector;
pub use domain_registry::DomainRegistry;
pub use feed::{
    FOLLOW_PROTOCOL_VERSION, FeedInfo, FollowFrame, LocalFeed, RemoteFeed, SearchFeed,
    follow_serve, local_snapshot, remote_snapshot,
};
pub use fleet::Engagement;
pub use machines::machines;
pub use migrate::{MigrateOutcome, migrate, recall, sync_serve};
pub use orchestrate::orchestrate;
pub use payload::PayloadSpec;
pub use program_binding::BinaryChange;
pub use program_delivery::{ProgramDelivery, ingest_program, receive_program};
pub use providers::{ProviderSettings, provider_for};
pub use remove::{remove, remove_matching};
pub use report::{ReportRow, report, report_records, report_task_records};
pub use sdk::Sdk;
pub use search_observer::SearchObserver;
pub use spend::spend;
// The rental-ledger and reputation types a caller renders those reports
// through.
pub use sima_provider::{Cost, MachineReport, MachineSummary, OpenSpend, Price, SpendReport};
pub use sima_store::SpendEntry;
// The search identity a query names, re-exported with the rest of the surface a
// caller reads a search through.
pub use searches::{SearchSummary, searches};
pub use sima_model::SearchId;
pub use sima_store::RemovalReport;
// The scheduler types through which a caller drives and observes a search, re-exported
// so the CLI consumes one coherent surface.
pub use sima_scheduler::{Event, LIVENESS_INTERVAL, Level, Record, SearchControl, SearchOutcome};
pub use status::{Occupancy, SearchState, SearchStatus, seeded_status, status, status_records};
pub use task_history::{
    Attempt, AttemptResult, TaskHistory, TaskOutcome, failures_records, task_history_records,
};
pub use task_keys::task_keys;
pub use timeline::{RetryStats, SearchTimeline, WorkerMetrics, timeline_records};
