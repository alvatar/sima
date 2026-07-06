//! The scheduler: it runs a search from `(RunConfig, store state)`.
//!
//! A task source derives the runnable frontier — the tasks the store does not
//! yet answer — and the driver hands each to an executor on a fixed pool of
//! worker threads, commits successes through the store, retries transient
//! failures, and stops on a definitive one. It is the layer that bridges pure
//! executor output into durable store state, so the executor trust boundary
//! lives on the worker seam: the executor returns values, and only the worker
//! writes to the store.
//!
//! Determinism is the correctness criterion: the same config run twice into
//! two fresh stores yields byte-identical manifests. Worker completion order
//! never reaches identity — the manifest sorts by task key at finalize, and
//! the journal, whose event order does vary between runs, is observational and
//! excluded from every equality criterion.

mod config;
mod event;
mod static_batch;
mod task_source;

pub use config::ExecutionConfig;
pub use event::LifecycleEvent;
pub use static_batch::StaticBatch;
pub use task_source::{RunnableTask, TaskSource};
