//! Path authority: every store path is derived here, and only here.
//!
//! The layout is the store-format contract — a change mints a new layout,
//! never mutates this one:
//!
//! ```text
//! <root>/objects/<aa>/<64-hex>     object bytes; aa = first two hex chars
//! <root>/tmp/<pid>-<seq>           in-flight writes
//! <root>/tasks/<task-key-hex>      index entry: record-hash hex + newline
//! <root>/runs/<run-id-hex>/manifest.json
//! <root>/runs/<run-id-hex>/journal
//! ```

use std::path::{Path, PathBuf};

/// The `objects/` CAS directory.
pub(crate) fn objects_dir(root: &Path) -> PathBuf {
    root.join("objects")
}

/// The `tmp/` directory holding in-flight atomic writes.
pub(crate) fn tmp_dir(root: &Path) -> PathBuf {
    root.join("tmp")
}

/// The `tasks/` index directory.
pub(crate) fn tasks_dir(root: &Path) -> PathBuf {
    root.join("tasks")
}

/// The `runs/` directory.
pub(crate) fn runs_dir(root: &Path) -> PathBuf {
    root.join("runs")
}
