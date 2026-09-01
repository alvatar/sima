//! `sima pack <store-dir> [--gc]`: store maintenance, invoked on its own.
//!
//! A store keeps every object it is given as one file, because that write
//! path is what survives a crash. At millions of objects those files cost an
//! inode each and press on the directory limits inside the fan-out. This
//! verb collapses them into packs — many objects per file — and leaves
//! everything above the store unchanged, since an object's address is the
//! hash of its bytes wherever those bytes are held.
//!
//! The argument is the store directory itself, and no config takes part: a
//! store carries the definition of every search it holds, so packing needs no
//! search knowledge and works on a store whose config files are long gone.
//!
//! `--gc` additionally deletes everything outside the finalized searches'
//! closures, unfinalized searches and their directories included, and it runs
//! before the packing so that an orphan is unlinked rather than packed and
//! then rewritten away. Beside an active search the sweep destroys that search's
//! work; the operator owns that decision, which is why it is a flag and not
//! the default.

use std::path::Path;
use std::process::ExitCode;

use sima_core::Result;
use sima_store::{GcReport, PackReport, Store};

use crate::report;

/// `sima pack <store-dir> [--gc]`: consolidates the store's loose objects
/// into packs, then sweeps it when `gc` was asked for, printing one line per
/// phase.
pub(crate) fn pack_command(store: &Path, gc: bool) -> ExitCode {
    match maintain(store, gc) {
        Ok((swept, packed)) => {
            if let Some(swept) = swept {
                println!("{}", gc_line(&swept));
            }
            println!("{}", pack_line(&packed));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Opens the store and runs the phases the invocation asked for, in the
/// order that does the least work: the sweep first, so an orphan is
/// unlinked where it lies instead of being packed and then rewritten out of
/// the pack it just entered. The store both orders leave behind is the
/// same, down to which objects it holds.
fn maintain(store: &Path, gc: bool) -> Result<(Option<GcReport>, PackReport)> {
    let store = Store::open(store)?;
    let swept = if gc { Some(store.gc()?) } else { None };
    Ok((swept, store.pack()?))
}

/// What the packing phase did, in one line.
fn pack_line(report: &PackReport) -> String {
    format!(
        "packed {} objects into {} packs ({} raw, {} stored); removed {} loose files",
        report.objects_packed,
        report.packs_written,
        byte_size(report.raw_bytes),
        byte_size(report.stored_bytes),
        report.loose_removed,
    )
}

/// What the sweep did, in one line.
fn gc_line(report: &GcReport) -> String {
    format!(
        "gc: removed {} objects, {} index entries, rewrote {} packs, removed {} unfinalized searches, swept {} tmp files",
        report.objects_removed,
        report.index_entries_removed,
        report.packs_rewritten,
        report.searches_removed,
        report.tmp_files_removed,
    )
}

/// A byte count in the largest binary unit that leaves it above one, with
/// one decimal above bytes: the operator is reading an order of magnitude,
/// not an exact figure.
fn byte_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_sizes_render_in_the_unit_that_fits() {
        assert_eq!(byte_size(0), "0 B");
        assert_eq!(byte_size(1023), "1023 B");
        assert_eq!(byte_size(1024), "1.0 KiB");
        assert_eq!(byte_size(15_246_562_099), "14.2 GiB");
        // The largest unit holds, however far past it the count runs.
        assert_eq!(byte_size(u64::MAX), "16777216.0 TiB");
    }

    #[test]
    fn the_report_lines_name_every_counter() {
        let packed = PackReport {
            objects_packed: 1_200_000,
            packs_written: 3,
            loose_removed: 1_200_000,
            raw_bytes: 15_246_562_099,
            stored_bytes: 6_549_123_456,
        };
        assert_eq!(
            pack_line(&packed),
            "packed 1200000 objects into 3 packs (14.2 GiB raw, 6.1 GiB stored); \
             removed 1200000 loose files"
        );
        let swept = GcReport {
            objects_removed: 34_000,
            index_entries_removed: 120,
            packs_rewritten: 2,
            searches_removed: 3,
            tmp_files_removed: 17,
        };
        assert_eq!(
            gc_line(&swept),
            "gc: removed 34000 objects, 120 index entries, rewrote 2 packs, \
             removed 3 unfinalized searches, swept 17 tmp files"
        );
    }
}
