//! [`RemoteFeed`] and [`remote_snapshot`]: a run followed on another host,
//! over one SSH connection.
//!
//! The near side spawns `sima follow-serve` on the host the orchestrator runs
//! on and reads the frames it writes. Nothing about the run is interpreted
//! here beyond the journal lines the far side forwards: the run id, the store
//! path, and the lock all belong to that host, and this side renders.
//!
//! Failures are named where they are detectable. `BatchMode=yes` scopes
//! interactive authentication out, so an unreachable host or one that would
//! ask for a password exits instead of prompting; a far side that speaks
//! another protocol version is refused rather than decoded; and whatever the
//! child wrote to stderr is folded into the error, so a missing `sima` on the
//! far side reads as what it is. A far side that connects and then stalls
//! before its first frame is a live connection, and the open waits on it.

use std::io::Read;
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::thread::JoinHandle;

use sima_core::{Error, Result, own_process_group, read_frame};
use sima_scheduler::Record;
use sima_transport::SshDestination;

use crate::feed::protocol::{FOLLOW_PROTOCOL_VERSION, FollowFrame};
use crate::feed::{FeedInfo, RunFeed};

/// Follows a run on another host: the frame stream its `follow-serve` writes,
/// with the metadata and lock state its frames carry.
pub struct RemoteFeed {
    info: FeedInfo,
    /// The lock holder as the far side last reported it, from the opening
    /// `Hello` and every `Holder` frame since.
    holder: Option<String>,
    /// The run's history, read at open and returned by the first poll — so a
    /// feed opens already holding what the run has done, as a local one does.
    history: Vec<Record>,
    stream: Stream,
}

impl RemoteFeed {
    /// Opens a live follow of the run `config` names on `host`. `config` is a
    /// path on that host, passed through unresolved — the far side interprets
    /// it, which is the whole reason the stream exists.
    pub fn open(host: &str, config: &str) -> Result<RemoteFeed> {
        RemoteFeed::over(Stream::spawn(
            &follow_serve_argv(host, config, false),
            host,
        )?)
    }

    /// Opens a live follow over an explicit invocation of `sima follow-serve`,
    /// for a destination the caller reaches its own way — a migration follows
    /// the run it started, whose destination may be an ssh hop at an explicit
    /// port or a `sima` on this machine. `label` names the destination in the
    /// errors that report it.
    pub(crate) fn open_over(argv: &[String], label: &str) -> Result<RemoteFeed> {
        RemoteFeed::over(Stream::spawn(argv, label)?)
    }

    /// The feed over an open stream, once its `Hello` is read and accepted.
    /// The boundary the protocol tests drive without a subprocess.
    fn over(mut stream: Stream) -> Result<RemoteFeed> {
        let (info, holder) = hello(&mut stream)?;
        // Waiting here is what makes a later empty poll mean the stream is
        // caught up rather than merely slow: without the history in hand, a
        // poll that arrives before the first frame reads as a run that did
        // nothing, and a caller that ends on a drained feed would end at once.
        let history = match stream.next()? {
            Some(FollowFrame::Records(lines)) => crate::journal::parse(&info.run, &lines)?,
            Some(FollowFrame::Fault(message)) => return Err(Error::Reported(message)),
            Some(frame) => {
                return Err(stream.failure(&format!("unexpected {frame:?} after the handshake")));
            }
            None => return Err(stream.failure("the remote stream ended before the run's history")),
        };
        Ok(RemoteFeed {
            info,
            holder,
            history,
            stream,
        })
    }
}

impl RunFeed for RemoteFeed {
    fn info(&self) -> &FeedInfo {
        &self.info
    }

    fn poll(&mut self) -> Result<Vec<Record>> {
        let mut records = std::mem::take(&mut self.history);
        loop {
            match self.stream.pending()? {
                Pending::Empty => return Ok(records),
                Pending::Frame(FollowFrame::Records(lines)) => {
                    records.extend(crate::journal::parse(&self.info.run, &lines)?);
                }
                Pending::Frame(FollowFrame::Holder(holder)) => self.holder = holder,
                Pending::Frame(FollowFrame::Fault(message)) => {
                    return Err(Error::Reported(message));
                }
                Pending::Frame(frame) => {
                    return Err(self
                        .stream
                        .failure(&format!("unexpected {frame:?} while following the run")));
                }
                // The far side went away without ending the follow: the run's
                // observation is over, and saying so beats reporting silence.
                Pending::Ended => {
                    return Err(self.stream.failure("the remote follow stream ended"));
                }
            }
        }
    }

    fn holder(&self) -> Result<Option<String>> {
        Ok(self.holder.clone())
    }
}

/// Reads the run `config` names on `host` once: its metadata and every record
/// its journal holds, for the one-shot views that render and exit.
pub fn remote_snapshot(host: &str, config: &str) -> Result<(FeedInfo, Vec<Record>)> {
    snapshot_over(Stream::spawn(&follow_serve_argv(host, config, true), host)?)
}

/// Reads a run once over an explicit invocation of `sima follow-serve --once`,
/// for a destination the caller reaches its own way — the counterpart of
/// [`RemoteFeed::open_over`], which a recall uses to read what the far run
/// ended as. `label` names the destination in the errors that report it.
pub(crate) fn snapshot_over_argv(argv: &[String], label: &str) -> Result<(FeedInfo, Vec<Record>)> {
    snapshot_over(Stream::spawn(argv, label)?)
}

/// The snapshot over an open stream, read to its `Complete`. The boundary the
/// protocol tests drive without a subprocess.
fn snapshot_over(mut stream: Stream) -> Result<(FeedInfo, Vec<Record>)> {
    let (info, _) = hello(&mut stream)?;
    let mut records = Vec::new();
    loop {
        match stream.next()? {
            Some(FollowFrame::Records(lines)) => {
                records.extend(crate::journal::parse(&info.run, &lines)?)
            }
            Some(FollowFrame::Complete) => return Ok((info, records)),
            // A holder update is meaningless to a snapshot, which reports what
            // the run produced rather than whether it is running.
            Some(FollowFrame::Holder(_)) => {}
            Some(FollowFrame::Fault(message)) => return Err(Error::Reported(message)),
            Some(frame) => {
                return Err(stream.failure(&format!("unexpected {frame:?} in a snapshot stream")));
            }
            None => {
                return Err(stream.failure("the remote stream ended before the snapshot completed"));
            }
        }
    }
}

/// Reads the stream's opening frame and accepts it: the run metadata and the
/// lock holder it carries. A version gap, a first frame that is not `Hello` —
/// an older `sima` without the verb writes nothing parseable — and a stream
/// that opens with nothing are each refused by name.
fn hello(stream: &mut Stream) -> Result<(FeedInfo, Option<String>)> {
    match stream.next()? {
        Some(FollowFrame::Hello {
            protocol,
            run,
            format,
            workers,
            holder,
        }) if protocol == FOLLOW_PROTOCOL_VERSION => Ok((
            FeedInfo {
                run,
                format,
                workers: workers as usize,
            },
            holder,
        )),
        Some(FollowFrame::Hello { protocol, .. }) => Err(stream.failure(&format!(
            "remote sima speaks follow protocol v{protocol}; this build expects \
             v{FOLLOW_PROTOCOL_VERSION}; run matching builds on both machines"
        ))),
        Some(FollowFrame::Fault(message)) => Err(Error::Reported(message)),
        Some(frame) => Err(stream.failure(&format!(
            "the remote follow stream opened with {frame:?} instead of a handshake"
        ))),
        None => Err(stream.failure("the remote follow stream produced nothing")),
    }
}

/// The argv that serves a run's follow stream from `host`: the destination's
/// own ssh invocation, then the far side's own `sima`. `config` is a path on
/// the far side and travels unresolved.
fn follow_serve_argv(host: &str, config: &str, once: bool) -> Vec<String> {
    let mut argv = SshDestination::known(host).prefix();
    argv.extend([
        "sima".to_string(),
        "follow-serve".to_string(),
        config.to_string(),
    ]);
    if once {
        argv.push("--once".to_string());
    }
    argv
}

/// What a non-blocking read of a stream found.
enum Pending {
    /// One frame, decoded.
    Frame(FollowFrame),
    /// Nothing has arrived since the last read; the stream is still open.
    Empty,
    /// The far side closed the stream.
    Ended,
}

/// One open follow stream: the frames a reader thread decodes off the far
/// side's stdout, whatever it wrote to stderr, and the process to reap.
struct Stream {
    frames: Receiver<Result<FollowFrame>>,
    /// The stderr capture thread; joined when an error needs what the far
    /// side said, so a missing binary or a refused connection is named.
    stderr: Option<JoinHandle<String>>,
    /// The ssh process, absent for an in-process stream under test.
    child: Option<Child>,
    /// The host the stream runs on, for the errors that name it.
    host: String,
}

impl Stream {
    /// Spawns the invocation that serves a run's follow stream and reads its
    /// stdout as frames. `label` is what the errors name the far side by.
    fn spawn(argv: &[String], label: &str) -> Result<Stream> {
        let (program, args) = argv.split_first().expect("the argv names a program");
        let mut child = own_process_group(&mut Command::new(program))
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                Error::Transport(format!("cannot run {program:?} to follow {label}: {e}"))
            })?;
        // The pipes exist iff the spawn configured them; taking them cannot
        // fail past a successful spawn.
        let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
            return Err(Error::Transport(format!(
                "the process following {label} has no piped output"
            )));
        };
        let mut stream = Stream::over(stdout);
        stream.stderr = Some(std::thread::spawn(move || capture(stderr)));
        stream.child = Some(child);
        stream.host = label.to_string();
        Ok(stream)
    }

    /// A stream over any byte source, with no process behind it.
    fn over(reader: impl Read + Send + 'static) -> Stream {
        let (sender, frames) = channel();
        std::thread::spawn(move || read_frames(reader, sender));
        Stream {
            frames,
            stderr: None,
            child: None,
            host: String::new(),
        }
    }

    /// The next frame, waiting for it; `None` when the stream ended.
    fn next(&mut self) -> Result<Option<FollowFrame>> {
        self.frames.recv().ok().transpose()
    }

    /// The next frame if one has arrived, without waiting.
    fn pending(&mut self) -> Result<Pending> {
        match self.frames.try_recv() {
            Ok(frame) => Ok(Pending::Frame(frame?)),
            Err(TryRecvError::Empty) => Ok(Pending::Empty),
            Err(TryRecvError::Disconnected) => Ok(Pending::Ended),
        }
    }

    /// An error reporting `message`, naming the host and folding in what the
    /// far side wrote to stderr. The child is reaped first so its output is
    /// complete: without it, the reason a remote refused would be lost to a
    /// race with the capture thread. Reaping ends it the way [`Drop`] does —
    /// a live `follow-serve` exits only when its stdout pipe closes, which a
    /// refusal taken mid-stream has not yet done, so waiting alone would wait
    /// on a process the refusal itself keeps alive. Killing an already-exited
    /// child is a no-op the reap absorbs.
    fn failure(&mut self, message: &str) -> Error {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        let said = self
            .stderr
            .take()
            .and_then(|capture| capture.join().ok())
            .unwrap_or_default();
        let said = said.trim();
        let mut error = match self.host.as_str() {
            "" => message.to_string(),
            host => format!("following the run on {host}: {message}"),
        };
        if !said.is_empty() {
            error.push_str(&format!(": {said}"));
        }
        Error::Validation(error)
    }
}

impl Drop for Stream {
    /// Ends the far side with the stream: closing the pipe is what tells a
    /// live `follow-serve` to stop, and killing the ssh process is what
    /// guarantees it, however the near side left.
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Decodes one follow stream into `frames` until it ends. Runs on the
/// stream's reader thread: end-of-stream simply ends the thread — the dropped
/// sender is the signal the reader observes — and a torn frame or an
/// undecodable payload is sent as the stream's final `Err`.
fn read_frames(mut reader: impl Read, frames: Sender<Result<FollowFrame>>) {
    loop {
        let frame = match read_frame(&mut reader) {
            Ok(Some(payload)) => FollowFrame::decode(&payload),
            Ok(None) => return,
            Err(e) => Err(e),
        };
        let failed = frame.is_err();
        // A send failure means the reader is gone; nothing is owed.
        if frames.send(frame).is_err() || failed {
            return;
        }
    }
}

/// Reads a child's stderr to its end. Invalid UTF-8 is replaced rather than
/// refused — this is capture, and capture never fails the follow.
fn capture(stderr: ChildStderr) -> String {
    let mut bytes = Vec::new();
    let mut stderr = stderr;
    let _ = stderr.read_to_end(&mut bytes);
    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::sync::mpsc::{Receiver, Sender, channel};
    use std::time::{Duration, Instant};

    use sima_core::{Error, hash_bytes, write_frame};
    use sima_model::{FormatId, RunId};
    use sima_scheduler::{Event, StatScalar};

    use crate::feed::FOLLOW_PROTOCOL_VERSION;

    /// A reader fed chunk by chunk from another thread, so a test delivers
    /// frames in stages the way a live far side does: a read with nothing
    /// queued blocks, and dropping the sender ends the stream.
    struct StagedReader {
        chunks: Receiver<Vec<u8>>,
        pending: Vec<u8>,
    }

    impl Read for StagedReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.pending.is_empty() {
                match self.chunks.recv() {
                    Ok(chunk) => self.pending = chunk,
                    // Every sender is gone: the far side closed the stream.
                    Err(_) => return Ok(0),
                }
            }
            let taken = buf.len().min(self.pending.len());
            buf[..taken].copy_from_slice(&self.pending[..taken]);
            self.pending.drain(..taken);
            Ok(taken)
        }
    }

    /// A staged reader and the sender that feeds it frames.
    fn staged() -> (Sender<Vec<u8>>, StagedReader) {
        let (sender, chunks) = channel();
        (
            sender,
            StagedReader {
                chunks,
                pending: Vec::new(),
            },
        )
    }

    /// Sends `frame` to a staged reader as its own chunk.
    fn send(sender: &Sender<Vec<u8>>, frame: &FollowFrame) {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &frame.encode()).expect("frame the input");
        sender.send(bytes).expect("the reader is alive");
    }

    /// The run every test stream describes.
    fn run() -> RunId {
        RunId::from_hash(hash_bytes(b"a remotely followed run"))
    }

    /// A `Hello` frame at `protocol` for that run.
    fn hello_at(protocol: u32) -> FollowFrame {
        FollowFrame::Hello {
            protocol,
            run: run(),
            format: FormatId::new("stub.v1").expect("format id"),
            workers: 3,
            holder: Some("11 gpubox".to_string()),
        }
    }

    /// A `Records` frame carrying one committed task's journal line.
    fn records_of(task: &str) -> FollowFrame {
        FollowFrame::Records(vec![
            Record {
                ts_ms: 0,
                event: Event::Committed {
                    task: task.to_string(),
                    record: "11".repeat(32),
                    stats: Vec::new(),
                    stats_blob_hex: String::new(),
                },
            }
            .to_line()
            .expect("a journal line"),
        ])
    }

    /// The bytes of `frames`, framed back to back — a whole stream at once.
    fn stream_of(frames: &[FollowFrame]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for frame in frames {
            write_frame(&mut bytes, &frame.encode()).expect("frame the input");
        }
        bytes
    }

    /// Polls `feed` until it yields records or the wait runs out.
    fn polled(feed: &mut RemoteFeed) -> Result<Vec<Record>> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let records = feed.poll()?;
            if !records.is_empty() || Instant::now() > deadline {
                return Ok(records);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn a_snapshot_reads_the_served_stream_into_records_and_metadata() -> Result<()> {
        let bytes = stream_of(&[
            hello_at(FOLLOW_PROTOCOL_VERSION),
            records_of("aa"),
            records_of("bb"),
            FollowFrame::Complete,
        ]);
        let (info, records) = snapshot_over(Stream::over(std::io::Cursor::new(bytes)))?;
        assert_eq!(info.run, run());
        assert_eq!(info.format, FormatId::new("stub.v1")?);
        assert_eq!(info.workers, 3);
        assert_eq!(records.len(), 2, "{records:?}");
        Ok(())
    }

    #[test]
    fn a_served_run_reads_back_as_the_records_and_metadata_of_its_journal() -> Result<()> {
        // The two halves of the transport against each other, without ssh:
        // what the far side serves is exactly what the near side renders.
        let dir = tempfile::tempdir().expect("temp dir");
        let journaled = [
            Record {
                ts_ms: 0,
                event: Event::RunStarted {
                    run: "00".repeat(32),
                    tasks: 2,
                    committed: 0,
                },
            },
            Record {
                ts_ms: 1,
                event: Event::Committed {
                    task: "aa".to_string(),
                    record: "11".repeat(32),
                    stats: Vec::new(),
                    stats_blob_hex: String::new(),
                },
            },
        ];
        let (config, loaded) = crate::fixtures::served_run(dir.path(), &journaled)?;
        let mut served = Vec::new();
        crate::feed::follow_serve(&config, true, &mut served)?;

        let (info, records) = snapshot_over(Stream::over(std::io::Cursor::new(served)))?;
        assert_eq!(info.run, loaded.run.id());
        assert_eq!(info.format, loaded.run.format);
        assert_eq!(info.workers, loaded.execution.workers);
        assert_eq!(records, journaled);
        Ok(())
    }

    #[test]
    fn a_record_with_a_non_finite_scalar_round_trips_through_the_transport() -> Result<()> {
        // The transport is the only place a journal Record is re-serialized to a
        // line and re-parsed on the near side. A diverged candidate carries a
        // non-finite scalar, which travels as JSON null and reads back as NaN,
        // so the frame path must carry it without failing. StatScalar's
        // PartialEq is value equality and NaN never equals NaN, so the recovered
        // value is checked with is_nan rather than compared.
        let committed = Record {
            ts_ms: 7,
            event: Event::Committed {
                task: "aa".to_string(),
                record: "11".repeat(32),
                stats: vec![StatScalar {
                    name: "activity".to_string(),
                    value: f64::NAN,
                }],
                stats_blob_hex: String::new(),
            },
        };
        let bytes = stream_of(&[
            hello_at(FOLLOW_PROTOCOL_VERSION),
            FollowFrame::Records(vec![committed.to_line()?]),
            FollowFrame::Complete,
        ]);
        let (_, records) = snapshot_over(Stream::over(std::io::Cursor::new(bytes)))?;
        let [record] = records.as_slice() else {
            panic!("one record, got {records:?}");
        };
        let Event::Committed { stats, .. } = &record.event else {
            panic!("a committed event, got {:?}", record.event);
        };
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].name, "activity");
        assert!(
            stats[0].value.is_nan(),
            "the non-finite scalar reads back as NaN"
        );
        Ok(())
    }

    #[test]
    fn a_snapshot_that_ends_before_complete_is_an_error() {
        let bytes = stream_of(&[hello_at(FOLLOW_PROTOCOL_VERSION), records_of("aa")]);
        assert!(snapshot_over(Stream::over(std::io::Cursor::new(bytes))).is_err());
    }

    #[test]
    fn a_live_feed_yields_records_and_tracks_the_holder_across_polls() -> Result<()> {
        let (sender, reader) = staged();
        send(&sender, &hello_at(FOLLOW_PROTOCOL_VERSION));
        send(&sender, &FollowFrame::Records(Vec::new()));
        let mut feed = RemoteFeed::over(Stream::over(reader))?;
        assert_eq!(feed.holder()?.as_deref(), Some("11 gpubox"));

        send(&sender, &records_of("aa"));
        assert_eq!(polled(&mut feed)?.len(), 1);
        send(&sender, &records_of("bb"));
        assert_eq!(polled(&mut feed)?.len(), 1, "the next poll is an increment");

        send(&sender, &FollowFrame::Holder(None));
        let deadline = Instant::now() + Duration::from_secs(5);
        while feed.holder()?.is_some() && Instant::now() < deadline {
            feed.poll()?;
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(feed.holder()?, None, "the released lock reached the feed");
        Ok(())
    }

    #[test]
    fn a_hello_at_another_version_names_both_versions() {
        let bytes = stream_of(&[hello_at(FOLLOW_PROTOCOL_VERSION + 1)]);
        let Err(Error::Validation(message)) =
            RemoteFeed::over(Stream::over(std::io::Cursor::new(bytes)))
        else {
            panic!("expected a validation error over a version gap");
        };
        assert!(
            message.contains(&FOLLOW_PROTOCOL_VERSION.to_string())
                && message.contains(&(FOLLOW_PROTOCOL_VERSION + 1).to_string()),
            "{message}"
        );
    }

    #[test]
    fn a_first_frame_that_is_not_hello_is_an_error() {
        let bytes = stream_of(&[records_of("aa")]);
        assert!(RemoteFeed::over(Stream::over(std::io::Cursor::new(bytes))).is_err());
    }

    #[test]
    fn a_feed_opens_holding_the_run_s_history_however_late_it_arrives() -> Result<()> {
        // A poll cannot tell a run that has done nothing from a first frame
        // that has not arrived, so the open waits for the history frame. Were
        // it not to, a caller that ends on a drained feed — `sima follow` over
        // a run nothing holds — would end before rendering anything.
        let (sender, reader) = staged();
        send(&sender, &hello_at(FOLLOW_PROTOCOL_VERSION));
        let staging = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            send(&sender, &records_of("aa"));
            sender
        });
        // The poll follows the open with nothing in between: only an open that
        // waited for the history can answer it with the record.
        let mut feed = RemoteFeed::over(Stream::over(reader))?;
        assert_eq!(feed.poll()?.len(), 1, "the first poll yields the history");
        drop(staging.join().expect("the staging thread"));
        Ok(())
    }

    #[test]
    fn a_stream_that_ends_after_the_handshake_is_an_error() {
        // The far side greeted and went away without serving the journal.
        let bytes = stream_of(&[hello_at(FOLLOW_PROTOCOL_VERSION)]);
        assert!(RemoteFeed::over(Stream::over(std::io::Cursor::new(bytes))).is_err());
    }

    #[test]
    fn a_stream_that_opens_with_nothing_is_an_error() {
        // An older `sima` without the verb, or a host that printed nothing.
        assert!(RemoteFeed::over(Stream::over(std::io::Cursor::new(Vec::new()))).is_err());
    }

    #[test]
    fn a_fault_frame_becomes_the_call_s_error() {
        let fault = FollowFrame::Fault("run was never started".to_string());
        let opening = stream_of(std::slice::from_ref(&fault));
        let Err(error) = RemoteFeed::over(Stream::over(std::io::Cursor::new(opening))) else {
            panic!("expected the fault to fail the open");
        };
        assert!(error.to_string().contains("never started"), "{error}");

        // And mid-stream, a fault surfaces through the poll that reads it.
        let (sender, reader) = staged();
        send(&sender, &hello_at(FOLLOW_PROTOCOL_VERSION));
        send(&sender, &FollowFrame::Records(Vec::new()));
        let mut feed = RemoteFeed::over(Stream::over(reader)).expect("the stream opens");
        send(&sender, &fault);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match feed.poll() {
                Err(e) => {
                    assert!(e.to_string().contains("never started"), "{e}");
                    return;
                }
                Ok(_) if Instant::now() > deadline => panic!("the fault never surfaced"),
                Ok(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        }
    }

    #[test]
    fn a_stream_that_drops_mid_follow_fails_the_poll() -> Result<()> {
        let (sender, reader) = staged();
        send(&sender, &hello_at(FOLLOW_PROTOCOL_VERSION));
        send(&sender, &FollowFrame::Records(Vec::new()));
        let mut feed = RemoteFeed::over(Stream::over(reader))?;
        // The far side went away without a terminal frame.
        drop(sender);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match feed.poll() {
                Err(_) => return Ok(()),
                Ok(_) if Instant::now() > deadline => panic!("the dropped stream never failed"),
                Ok(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        }
    }

    #[test]
    fn the_ssh_invocation_carries_batch_mode_the_host_and_the_config() {
        assert_eq!(
            follow_serve_argv("gpubox", "/srv/exp.toml", false),
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "gpubox",
                "--",
                "sima",
                "follow-serve",
                "/srv/exp.toml",
            ]
        );
        assert_eq!(
            follow_serve_argv("gpubox", "exp.toml", true)
                .last()
                .expect("a last argument"),
            "--once"
        );
    }

    #[test]
    fn an_unreachable_host_is_a_validation_error_naming_it() {
        // A destination no ssh config resolves: BatchMode refuses rather than
        // prompting, so the failure is prompt and clean.
        let error = remote_snapshot("sima.invalid.test", "exp.toml")
            .expect_err("an unreachable host cannot serve a stream");
        assert!(matches!(error, Error::Validation(_)), "{error:?}");
        assert!(error.to_string().contains("sima.invalid.test"), "{error}");
    }
}
