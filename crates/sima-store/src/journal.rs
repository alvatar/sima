//! The run journal: append-only observational history, one event per line.
//!
//! The store owns the framing and its crash tolerance only: one event per
//! line, newline-terminated, fsynced per append; on read, bytes past the
//! last newline are a torn final write, detected and ignored. The meaning
//! of each line belongs to the emitting layers — scheduler lifecycle events
//! and status above. Journals legitimately differ between identical runs
//! and are excluded from every equality criterion.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::str;

use sima_core::{Error, Result};
use sima_model::RunId;

use crate::atomic::io_error;
use crate::layout;
use crate::store::Store;

/// Append handle over one run's journal. Appending takes `&mut self`:
/// one orchestrator writes a run's journal, and this handle is its
/// single-writer seam.
pub struct JournalWriter {
    path: PathBuf,
    file: File,
}

impl Store {
    /// Opens the append handle for `run`'s journal, creating the file on
    /// first use. The run must have been created ([`Error::Validation`]
    /// otherwise).
    pub fn journal_writer(&self, run: &RunId) -> Result<JournalWriter> {
        if !layout::run_dir(self.root(), run).is_dir() {
            return Err(Error::Validation(format!(
                "cannot open the journal of run {run}: the run was never created"
            )));
        }
        let path = layout::journal_path(self.root(), run);
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .map_err(|e| io_error(&path, e))?;
        Ok(JournalWriter { path, file })
    }

    /// Reads `run`'s journal lines in append order. An absent file is an
    /// empty journal. The intact region is the content up to the last
    /// newline — bytes past it are a torn final write and are ignored;
    /// invalid UTF-8 inside the intact region is [`Error::Corruption`].
    pub fn journal(&self, run: &RunId) -> Result<Vec<String>> {
        Ok(self.journal_from(run, 0)?.0)
    }

    /// Reads the complete lines of `run`'s journal whose bytes lie past
    /// `offset`, returning them with the new offset — the position
    /// immediately after the last returned line's newline. Bytes past the
    /// last newline are a torn or in-flight final write and stay
    /// unconsumed: a later call from the returned offset re-reads them once
    /// their newline lands. An absent file is `(empty, 0)`; invalid UTF-8
    /// in the returned region is [`Error::Corruption`]. Journals are
    /// append-only and never truncated, so a returned offset stays valid
    /// for the file's lifetime.
    pub fn journal_from(&self, run: &RunId, offset: u64) -> Result<(Vec<String>, u64)> {
        let path = layout::journal_path(self.root(), run);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok((Vec::new(), 0)),
            Err(e) => return Err(io_error(&path, e)),
        };
        // Read only the tail past the offset; the region before it was
        // consumed by earlier calls.
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| io_error(&path, e))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|e| io_error(&path, e))?;
        let Some(last_newline) = bytes.iter().rposition(|&b| b == b'\n') else {
            // No complete line past the offset: nothing is consumed.
            return Ok((Vec::new(), offset));
        };
        let intact = &bytes[..last_newline];
        let consumed = offset + last_newline as u64 + 1;
        if intact.is_empty() {
            return Ok((Vec::new(), consumed));
        }
        let text = str::from_utf8(intact)
            .map_err(|_| Error::Corruption(format!("journal of run {run} holds invalid UTF-8")))?;
        Ok((text.split('\n').map(String::from).collect(), consumed))
    }
}

impl JournalWriter {
    /// Appends one event line: the payload plus a terminating newline in
    /// a single write, fsynced before returning. The payload must be
    /// nonempty and free of `\n`/`\r` ([`Error::Validation`] otherwise),
    /// so one line stays one event.
    pub fn append(&mut self, line: &str) -> Result<()> {
        if line.is_empty() {
            return Err(Error::Validation(
                "journal line must be nonempty".to_string(),
            ));
        }
        if line.bytes().any(|b| b == b'\n' || b == b'\r') {
            return Err(Error::Validation(format!(
                "journal line {line:?} contains a line break"
            )));
        }
        let mut framed = Vec::with_capacity(line.len() + 1);
        framed.extend_from_slice(line.as_bytes());
        framed.push(b'\n');
        self.file
            .write_all(&framed)
            .map_err(|e| io_error(&self.path, e))?;
        self.file.sync_all().map_err(|e| io_error(&self.path, e))
    }
}

#[cfg(test)]
mod tests {
    use crate::testutil::{sample_run_config, temp_store};
    use sima_core::{Error, Result};
    use sima_model::RunId;
    use std::fs;
    use std::path::Path;

    /// Creates the sample run, returning its id.
    fn created_run(store: &crate::Store) -> Result<RunId> {
        store.create_run(&crate::testutil::sample_run_config(42))
    }

    /// The journal path of `run` under `root`.
    fn journal_path(root: &Path, run: &RunId) -> std::path::PathBuf {
        root.join("runs").join(run.to_string()).join("journal")
    }

    #[test]
    fn appended_lines_read_back_in_order() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = created_run(&store)?;
        let mut writer = store.journal_writer(&run)?;
        for line in ["first event", "second event", "third event"] {
            writer.append(line)?;
        }
        assert_eq!(
            store.journal(&run)?,
            ["first event", "second event", "third event"]
        );
        Ok(())
    }

    #[test]
    fn framing_violations_are_validation_errors() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = created_run(&store)?;
        let mut writer = store.journal_writer(&run)?;
        for payload in ["", "line\nbreak", "carriage\rreturn"] {
            assert!(matches!(writer.append(payload), Err(Error::Validation(_))));
        }
        // Nothing was written by the rejected appends.
        assert_eq!(store.journal(&run)?, Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn a_run_with_no_journal_file_reads_empty() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = created_run(&store)?;
        assert_eq!(store.journal(&run)?, Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn a_torn_final_line_is_ignored() -> Result<()> {
        let (dir, store) = temp_store();
        let run = created_run(&store)?;
        let mut writer = store.journal_writer(&run)?;
        writer.append("complete line")?;
        writer.append("torn line")?;
        // Truncate mid-final-line, as a crash mid-write would leave it.
        let path = journal_path(dir.path(), &run);
        let bytes = fs::read(&path).expect("read journal");
        fs::write(&path, &bytes[..bytes.len() - 3]).expect("truncate journal");
        assert_eq!(store.journal(&run)?, ["complete line"]);
        Ok(())
    }

    #[test]
    fn a_file_ending_at_a_newline_reads_every_line() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = created_run(&store)?;
        let mut writer = store.journal_writer(&run)?;
        writer.append("only line")?;
        assert_eq!(store.journal(&run)?, ["only line"]);
        Ok(())
    }

    #[test]
    fn invalid_utf8_in_the_intact_region_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let run = created_run(&store)?;
        let path = journal_path(dir.path(), &run);
        fs::write(&path, b"valid line\n\xff\xfe garbage\n").expect("write journal");
        assert!(matches!(store.journal(&run), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn invalid_utf8_past_the_last_newline_is_torn_not_corrupt() -> Result<()> {
        let (dir, store) = temp_store();
        let run = created_run(&store)?;
        let path = journal_path(dir.path(), &run);
        // The garbage sits past the last newline: a torn write, ignored.
        fs::write(&path, b"valid line\n\xff\xfe").expect("write journal");
        assert_eq!(store.journal(&run)?, ["valid line"]);
        Ok(())
    }

    #[test]
    fn journal_writer_before_create_run_is_validation_error() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = sample_run_config(42).id();
        assert!(matches!(
            store.journal_writer(&run),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn journal_from_returns_only_lines_past_the_offset() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = created_run(&store)?;
        let mut writer = store.journal_writer(&run)?;
        writer.append("first")?;
        writer.append("second")?;
        let (lines, offset) = store.journal_from(&run, 0)?;
        assert_eq!(lines, ["first", "second"]);
        writer.append("third")?;
        // Reading from the returned offset delivers only the new line.
        let (lines, next) = store.journal_from(&run, offset)?;
        assert_eq!(lines, ["third"]);
        // Nothing appended since: the read is empty and the offset holds.
        let (lines, still) = store.journal_from(&run, next)?;
        assert_eq!(lines, Vec::<String>::new());
        assert_eq!(still, next);
        Ok(())
    }

    #[test]
    fn journal_from_at_offset_zero_matches_journal() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = created_run(&store)?;
        let mut writer = store.journal_writer(&run)?;
        writer.append("first")?;
        writer.append("second")?;
        assert_eq!(store.journal_from(&run, 0)?.0, store.journal(&run)?);
        Ok(())
    }

    #[test]
    fn journal_from_on_an_absent_file_is_empty_at_offset_zero() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = created_run(&store)?;
        assert_eq!(store.journal_from(&run, 0)?, (Vec::new(), 0));
        Ok(())
    }

    #[test]
    fn journal_from_leaves_a_torn_final_write_unconsumed() -> Result<()> {
        let (dir, store) = temp_store();
        let run = created_run(&store)?;
        let mut writer = store.journal_writer(&run)?;
        writer.append("complete")?;
        let path = journal_path(dir.path(), &run);
        // Bytes past the last newline, as a write in flight would leave them.
        append_raw(&path, b"par");
        let (lines, offset) = store.journal_from(&run, 0)?;
        assert_eq!(lines, ["complete"]);
        assert_eq!(
            offset,
            "complete\n".len() as u64,
            "the torn tail stays unconsumed"
        );
        // Once the line's newline lands, the same offset delivers it whole.
        append_raw(&path, b"tial line\n");
        let (lines, _) = store.journal_from(&run, offset)?;
        assert_eq!(lines, ["partial line"]);
        Ok(())
    }

    #[test]
    fn invalid_utf8_past_the_offset_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let run = created_run(&store)?;
        let mut writer = store.journal_writer(&run)?;
        writer.append("valid line")?;
        let (_, offset) = store.journal_from(&run, 0)?;
        append_raw(&journal_path(dir.path(), &run), b"\xff\xfe garbage\n");
        assert!(matches!(
            store.journal_from(&run, offset),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    /// Appends raw bytes to `path`, as a foreign writer's partial or
    /// complete write would land them.
    fn append_raw(path: &Path, bytes: &[u8]) {
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open journal for raw append");
        file.write_all(bytes).expect("raw append");
    }

    #[test]
    fn two_writers_appending_alternately_produce_a_readable_journal() -> Result<()> {
        let (_dir, store) = temp_store();
        let run = created_run(&store)?;
        // The orchestrator lease enforces a single writer per run; the framing
        // must hold regardless of who writes.
        let mut a = store.journal_writer(&run)?;
        let mut b = store.journal_writer(&run)?;
        a.append("a1")?;
        b.append("b1")?;
        a.append("a2")?;
        b.append("b2")?;
        assert_eq!(store.journal(&run)?, ["a1", "b1", "a2", "b2"]);
        Ok(())
    }
}
