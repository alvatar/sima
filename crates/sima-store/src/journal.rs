//! The search journal: append-only observational history, one event per line.
//!
//! The store owns the framing and its crash tolerance only: one event per
//! line, newline-terminated, fsynced per append; on read, bytes past the
//! last newline are a torn final write, detected and ignored. The meaning
//! of each line belongs to the emitting layers — scheduler lifecycle events
//! and status above. Journals legitimately differ between identical searches
//! and are excluded from every equality criterion.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::str;

use sima_core::{Error, Result};
use sima_model::SearchId;

use crate::atomic::io_error;
use crate::layout;
use crate::store::Store;

/// Append handle over one search's journal. Appending takes `&mut self`:
/// one orchestrator writes a search's journal, and this handle is its
/// single-writer boundary.
pub struct JournalWriter {
    path: PathBuf,
    file: File,
}

impl Store {
    /// Opens the append handle for `search`'s journal, creating the file on
    /// first use. The search must have been created ([`Error::Validation`]
    /// otherwise).
    pub fn journal_writer(&self, search: &SearchId) -> Result<JournalWriter> {
        if !layout::search_dir(self.root(), search).is_dir() {
            return Err(Error::Validation(format!(
                "cannot open the journal of search {search}: the search was never created"
            )));
        }
        let path = layout::journal_path(self.root(), search);
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .map_err(|e| io_error(&path, e))?;
        Ok(JournalWriter { path, file })
    }

    /// Reads `search`'s journal lines in append order. An absent file is an
    /// empty journal. The intact region is the content up to the last
    /// newline — bytes past it are a torn final write and are ignored;
    /// invalid UTF-8 inside the intact region is [`Error::Corruption`].
    pub fn journal(&self, search: &SearchId) -> Result<Vec<String>> {
        Ok(self.journal_from(search, 0)?.0)
    }

    /// Reads the complete lines of `search`'s journal whose bytes lie past
    /// `offset`, returning them with the new offset — the position
    /// immediately after the last returned line's newline. Bytes past the
    /// last newline are a torn or in-flight final write and stay
    /// unconsumed: a later call from the returned offset re-reads them once
    /// their newline lands. An absent file is `(empty, 0)`; invalid UTF-8
    /// in the returned region is [`Error::Corruption`]. Journals are
    /// append-only and never truncated, so a returned offset stays valid
    /// for the file's lifetime.
    pub fn journal_from(&self, search: &SearchId, offset: u64) -> Result<(Vec<String>, u64)> {
        let path = layout::journal_path(self.root(), search);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok((Vec::new(), 0)),
            Err(e) => return Err(io_error(&path, e)),
        };
        // The read below runs to the file's end, so only a regular file — a
        // thing with an extent — is readable; a special file in the journal's
        // place (a device, a pipe) would be read forever and is refused
        // instead.
        let meta = file.metadata().map_err(|e| io_error(&path, e))?;
        if !meta.is_file() {
            return Err(Error::Corruption(format!(
                "journal of search {search} is not a regular file"
            )));
        }
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
        let text = str::from_utf8(intact).map_err(|_| {
            Error::Corruption(format!("journal of search {search} holds invalid UTF-8"))
        })?;
        Ok((text.split('\n').map(String::from).collect(), consumed))
    }
}

/// The trace collector's durable boundary: a journal writer is exactly an
/// append-one-line sink, so the collector funnels every record through the
/// same crash-safe append as any other journal line.
impl sima_trace::DurableSink for JournalWriter {
    fn append_line(&mut self, line: &str) -> Result<()> {
        self.append(line)
    }

    fn append_lines(&mut self, lines: &[String]) -> Result<()> {
        self.append_batch(lines)
    }
}

impl JournalWriter {
    /// Appends one event line: the payload plus a terminating newline in
    /// a single write, fsynced before returning. The payload must be
    /// nonempty and free of `\n`/`\r` ([`Error::Validation`] otherwise),
    /// so one line stays one event.
    pub fn append(&mut self, line: &str) -> Result<()> {
        self.append_batch(std::slice::from_ref(&line.to_string()))
    }

    /// Appends every line of `lines`, in order, and fsyncs once.
    ///
    /// The framing rules are per line and unchanged: each payload is nonempty,
    /// free of `\n`/`\r`, and newline-terminated, so one line stays one event
    /// however many are written together. What the batch shares is the
    /// durability barrier — a search committing tasks faster than one fsync each
    /// is otherwise capped at the disk's fsync rate however many workers it
    /// has.
    ///
    /// Every line is validated before anything is written, so a malformed one
    /// leaves the journal exactly as it was rather than partway through the
    /// batch.
    pub fn append_batch(&mut self, lines: &[String]) -> Result<()> {
        if lines.is_empty() {
            return Ok(());
        }
        let mut framed = Vec::with_capacity(lines.iter().map(|l| l.len() + 1).sum());
        for line in lines {
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
            framed.extend_from_slice(line.as_bytes());
            framed.push(b'\n');
        }
        self.file
            .write_all(&framed)
            .map_err(|e| io_error(&self.path, e))?;
        self.file.sync_all().map_err(|e| io_error(&self.path, e))
    }
}

#[cfg(test)]
mod tests {
    use crate::testutil::{sample_search_config, temp_store};
    use sima_core::{Error, Result};
    use sima_model::SearchId;
    use std::fs;
    use std::path::Path;

    /// Creates the sample search, returning its id.
    fn created_search(store: &crate::Store) -> Result<SearchId> {
        store.create_search(&crate::testutil::sample_search_config(42))
    }

    /// The journal path of `search` under `root`.
    fn journal_path(root: &Path, search: &SearchId) -> std::path::PathBuf {
        root.join("searches")
            .join(search.to_string())
            .join("journal")
    }

    #[test]
    fn appended_lines_read_back_in_order() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = created_search(&store)?;
        let mut writer = store.journal_writer(&search)?;
        for line in ["first event", "second event", "third event"] {
            writer.append(line)?;
        }
        assert_eq!(
            store.journal(&search)?,
            ["first event", "second event", "third event"]
        );
        Ok(())
    }

    #[test]
    fn framing_violations_are_validation_errors() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = created_search(&store)?;
        let mut writer = store.journal_writer(&search)?;
        for payload in ["", "line\nbreak", "carriage\rreturn"] {
            assert!(matches!(writer.append(payload), Err(Error::Validation(_))));
        }
        // Nothing was written by the rejected appends.
        assert_eq!(store.journal(&search)?, Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn the_durable_sink_boundary_appends_like_the_writer() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = created_search(&store)?;
        let mut writer = store.journal_writer(&search)?;
        let sink: &mut dyn sima_trace::DurableSink = &mut writer;
        sink.append_line("a collector record")?;
        assert_eq!(store.journal(&search)?, ["a collector record"]);
        Ok(())
    }

    #[test]
    fn a_search_with_no_journal_file_reads_empty() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = created_search(&store)?;
        assert_eq!(store.journal(&search)?, Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn a_torn_final_line_is_ignored() -> Result<()> {
        let (dir, store) = temp_store();
        let search = created_search(&store)?;
        let mut writer = store.journal_writer(&search)?;
        writer.append("complete line")?;
        writer.append("torn line")?;
        // Truncate mid-final-line, as a crash mid-write would leave it.
        let path = journal_path(dir.path(), &search);
        let bytes = fs::read(&path).expect("read journal");
        fs::write(&path, &bytes[..bytes.len() - 3]).expect("truncate journal");
        assert_eq!(store.journal(&search)?, ["complete line"]);
        Ok(())
    }

    #[test]
    fn a_file_ending_at_a_newline_reads_every_line() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = created_search(&store)?;
        let mut writer = store.journal_writer(&search)?;
        writer.append("only line")?;
        assert_eq!(store.journal(&search)?, ["only line"]);
        Ok(())
    }

    #[test]
    fn invalid_utf8_in_the_intact_region_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let search = created_search(&store)?;
        let path = journal_path(dir.path(), &search);
        fs::write(&path, b"valid line\n\xff\xfe garbage\n").expect("write journal");
        assert!(matches!(store.journal(&search), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn invalid_utf8_past_the_last_newline_is_torn_not_corrupt() -> Result<()> {
        let (dir, store) = temp_store();
        let search = created_search(&store)?;
        let path = journal_path(dir.path(), &search);
        // The garbage sits past the last newline: a torn write, ignored.
        fs::write(&path, b"valid line\n\xff\xfe").expect("write journal");
        assert_eq!(store.journal(&search)?, ["valid line"]);
        Ok(())
    }

    #[test]
    fn a_journal_that_is_not_a_regular_file_is_refused() -> Result<()> {
        let (dir, store) = temp_store();
        let search = created_search(&store)?;
        let path = journal_path(dir.path(), &search);
        // /dev/full reads as an endless stream of zeros, so a special file in
        // the journal's place must be refused up front — a read to its end
        // would allocate until the machine dies.
        std::os::unix::fs::symlink("/dev/full", &path).expect("symlink journal to /dev/full");
        assert!(matches!(store.journal(&search), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn journal_writer_before_create_search_is_validation_error() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = sample_search_config(42).id();
        assert!(matches!(
            store.journal_writer(&search),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn journal_from_returns_only_lines_past_the_offset() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = created_search(&store)?;
        let mut writer = store.journal_writer(&search)?;
        writer.append("first")?;
        writer.append("second")?;
        let (lines, offset) = store.journal_from(&search, 0)?;
        assert_eq!(lines, ["first", "second"]);
        writer.append("third")?;
        // Reading from the returned offset delivers only the new line.
        let (lines, next) = store.journal_from(&search, offset)?;
        assert_eq!(lines, ["third"]);
        // Nothing appended since: the read is empty and the offset holds.
        let (lines, still) = store.journal_from(&search, next)?;
        assert_eq!(lines, Vec::<String>::new());
        assert_eq!(still, next);
        Ok(())
    }

    #[test]
    fn journal_from_at_offset_zero_matches_journal() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = created_search(&store)?;
        let mut writer = store.journal_writer(&search)?;
        writer.append("first")?;
        writer.append("second")?;
        assert_eq!(store.journal_from(&search, 0)?.0, store.journal(&search)?);
        Ok(())
    }

    #[test]
    fn journal_from_on_an_absent_file_is_empty_at_offset_zero() -> Result<()> {
        let (_dir, store) = temp_store();
        let search = created_search(&store)?;
        assert_eq!(store.journal_from(&search, 0)?, (Vec::new(), 0));
        Ok(())
    }

    #[test]
    fn journal_from_leaves_a_torn_final_write_unconsumed() -> Result<()> {
        let (dir, store) = temp_store();
        let search = created_search(&store)?;
        let mut writer = store.journal_writer(&search)?;
        writer.append("complete")?;
        let path = journal_path(dir.path(), &search);
        // Bytes past the last newline, as a write in flight would leave them.
        append_raw(&path, b"par");
        let (lines, offset) = store.journal_from(&search, 0)?;
        assert_eq!(lines, ["complete"]);
        assert_eq!(
            offset,
            "complete\n".len() as u64,
            "the torn tail stays unconsumed"
        );
        // Once the line's newline lands, the same offset delivers it whole.
        append_raw(&path, b"tial line\n");
        let (lines, _) = store.journal_from(&search, offset)?;
        assert_eq!(lines, ["partial line"]);
        Ok(())
    }

    #[test]
    fn invalid_utf8_past_the_offset_is_corruption() -> Result<()> {
        let (dir, store) = temp_store();
        let search = created_search(&store)?;
        let mut writer = store.journal_writer(&search)?;
        writer.append("valid line")?;
        let (_, offset) = store.journal_from(&search, 0)?;
        append_raw(&journal_path(dir.path(), &search), b"\xff\xfe garbage\n");
        assert!(matches!(
            store.journal_from(&search, offset),
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
        let search = created_search(&store)?;
        // The orchestrator lease enforces a single writer per search; the framing
        // must hold regardless of who writes.
        let mut a = store.journal_writer(&search)?;
        let mut b = store.journal_writer(&search)?;
        a.append("a1")?;
        b.append("b1")?;
        a.append("a2")?;
        b.append("b2")?;
        assert_eq!(store.journal(&search)?, ["a1", "b1", "a2", "b2"]);
        Ok(())
    }
}
