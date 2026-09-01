//! End-to-end acceptance of a program written in another language: the Python
//! example, driven through the full spine.
//!
//! What is proven here is that `docs/protocol.md` is the whole requirement. The
//! program under test shares no code with this workspace — it speaks the wire
//! from the SDK written against that document, which the configuration declares
//! and the binary vends — so every message the search needs must cross correctly
//! or a search fails here.
//!
//! One search covers the ordinary path: both roles, every domain-service
//! question, a chain of segments hopping on committed state, and the artifacts
//! it lands on. The rest arm one failure each — a worker that dies mid-segment,
//! a transient failure, a raise, a candidate the program rejects, and a
//! configuration it refuses — because recovery is what a protocol claim rests
//! on.
//!
//! `python3` is a hard requirement: the guard below fails loudly naming it
//! rather than letting a suite pass over an absent interpreter.

mod common;

use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::{journal_events, loaded_text};
use sima_core::{Result, prng};
use sima_pipeline::{
    BinaryChange, Engagement, Event, LoadedConfig, Record, Sdk, SearchControl, SearchOutcome, load,
    orchestrate, task_keys,
};
use sima_store::Store;

/// The format the example program serves.
const FORMAT: &str = "example.stepper.v1";
/// The generator it draws candidates with.
const GENERATOR: &str = "example.stepper.candidates";
/// The root seed every configuration here runs under. Candidate `i` is the
/// byte `(root_seed + i) % 256`, so the candidates are 7, 8, and upward.
const ROOT_SEED: u64 = 7;
/// The state a stepper task commits: a `u64` step and a `u64` accumulator.
const STATE_LEN: usize = 16;

/// The repository root, from which the example and the Python package are
/// reached: this crate sits two levels below it.
fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the repository root above crates/sima-integration")
        .to_path_buf()
}

/// Asserts `python3` runs, once per test process.
///
/// The interpreter is a requirement of this suite, not an option: a machine
/// without it fails here, naming what is missing, rather than reporting a green
/// suite that tested nothing.
fn require_python3() {
    static CHECK: Once = Once::new();
    CHECK.call_once(|| {
        let version = std::process::Command::new("python3")
            .arg("--version")
            .output();
        match version {
            Ok(output) if output.status.success() => {}
            other => panic!(
                "these tests drive a Python program, so python3 is required and must run: {other:?}"
            ),
        }
    });
}

/// Writes the executable wrapper the configuration routes the format to, and
/// returns it.
///
/// The wrapper exists to arm a failure path: the variables it exports are the
/// search's arming, so the configuration carries none of it and an armed search keeps
/// the identity of an unarmed one. `import sima` needs nothing from it — every
/// configuration here declares `sdk = "python"`, and the package the binary
/// vends is what the interpreter reads.
fn wrapper(dir: &Path, arming: &[(&str, &str)]) -> PathBuf {
    require_python3();
    let repo = repo();
    let exports: String = arming
        .iter()
        .map(|(name, value)| format!("export {name}={value}\n"))
        .collect();
    let path = dir.join("stepper.sh");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\n\
             {exports}\
             exec python3 {} \"$@\"\n",
            repo.join("examples/stepper-py/stepper.py").display(),
        ),
    )
    .expect("write the wrapper");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("make the wrapper executable");
    }
    path
}

/// Vends the SDK under `dir` and answers the directory an interpreter reads it
/// from.
///
/// The two tests below drive the program's session by hand, outside every spawn
/// sima performs, so what a spawn policy would put on the module path is theirs
/// to put there.
fn vended_sdk(dir: &Path) -> PathBuf {
    let path = dir.join("sdk");
    Sdk::Python.vend(&path).expect("vend the SDK");
    path
}

/// A search over the example program, in the shape each test varies from.
struct Search {
    /// Tasks per candidate; `None` is one stateless task each.
    segments: Option<u64>,
    /// Candidates the generator draws.
    count: u64,
    /// Steps one task executes.
    steps: u64,
    /// The byte every candidate carries, when the search fixes it.
    value: Option<u64>,
    /// Whether every checkpoint offer saves.
    checkpointing: bool,
    /// Attempts one task may take.
    max_attempts: u32,
    /// Whether the orchestrator places its workers on the program's device.
    by_device: bool,
}

impl Search {
    /// The base search: two candidates, three segments of five steps each, no
    /// checkpointing, workers placed on the device the program enumerates.
    fn new() -> Search {
        Search {
            segments: Some(3),
            count: 2,
            steps: 5,
            value: None,
            checkpointing: false,
            max_attempts: 2,
            by_device: true,
        }
    }

    /// The steps one candidate's whole chain executes.
    fn total_steps(&self) -> u64 {
        self.steps * self.segments.unwrap_or(1)
    }

    /// The configuration text, routing the format to `program`.
    fn text(&self, store: &str, program: &Path) -> String {
        let segments = self
            .segments
            .map_or(String::new(), |n| format!("segments = {n}\n"));
        let value = self
            .value
            .map_or(String::new(), |v| format!("value = {v}\n"));
        let checkpointing = if self.checkpointing {
            "checkpoint_interval_ms = 0\n"
        } else {
            ""
        };
        let pool = if self.by_device {
            "[[orchestrator.device]]\nselect = \"example:cpu\"\nworkers = 2\n"
        } else {
            "[orchestrator]\nworkers = 2\n"
        };
        format!(
            r#"
[search]
root_seed = {ROOT_SEED}
format = "{FORMAT}"
{segments}
[search.generator]
id = "{GENERATOR}"
count = {}
{value}
[search.params]
steps = {}

[config]
store = "{store}"
max_attempts = {}
{checkpointing}
{pool}
[domain."{FORMAT}"]
binary = "{}"
sdk = "python"
"#,
            self.count,
            self.steps,
            self.max_attempts,
            program.display(),
        )
    }

    /// The state each candidate's chain ends on: the total step count, and the
    /// accumulator that many increments past the task's seed.
    ///
    /// Computed here from the search's own inputs rather than read back from the
    /// program, so the assertion states what the arithmetic must produce.
    fn expected_states(&self) -> Vec<(u64, u64)> {
        (0..self.count)
            .map(|i| {
                let increment = self.value.unwrap_or((ROOT_SEED + i) % 256);
                let seed = prng::derive(ROOT_SEED, i);
                let total = self.total_steps();
                (total, seed.wrapping_add(increment.wrapping_mul(total)))
            })
            .collect()
    }
}

/// Drives `config` to its outcome with no observer.
fn run(config: &LoadedConfig) -> Result<SearchOutcome> {
    orchestrate(
        config,
        &SearchControl::detached(),
        Engagement::Orchestrator,
        BinaryChange::Refuse,
    )
}

/// Asserts the search finalized, naming what it did instead.
fn finalized(outcome: &SearchOutcome) {
    assert!(
        matches!(outcome, SearchOutcome::Finalized { .. }),
        "the search finalized: {outcome:?}"
    );
}

/// Every state artifact the finalized search committed, decoded into its step and
/// accumulator, in manifest order.
fn committed_states(config: &LoadedConfig) -> Result<Vec<(u64, u64)>> {
    let store = Store::open(&config.store)?;
    let manifest = store
        .manifest(&config.search.id())?
        .expect("a finalized manifest");
    manifest
        .entries
        .iter()
        .map(|entry| {
            let record = store
                .record(&entry.task)?
                .expect("a manifest entry's record");
            let artifact = record
                .artifacts()
                .iter()
                .find(|artifact| artifact.name() == "state")
                .expect("the stepper commits the state artifact");
            let bytes = store.get(artifact.object())?;
            assert_eq!(
                bytes.len(),
                STATE_LEN,
                "a stepper state is {STATE_LEN} bytes"
            );
            let step = u64::from_le_bytes(bytes[..8].try_into().expect("the step half"));
            let acc = u64::from_le_bytes(bytes[8..].try_into().expect("the accumulator half"));
            Ok((step, acc))
        })
        .collect()
}

/// The states the search's chains ended on: the committed states at the final
/// step, sorted, so the assertion does not depend on commit order.
fn final_states(config: &LoadedConfig, total_steps: u64) -> Result<Vec<(u64, u64)>> {
    let mut states: Vec<(u64, u64)> = committed_states(config)?
        .into_iter()
        .filter(|(step, _)| *step == total_steps)
        .collect();
    states.sort_unstable();
    Ok(states)
}

/// The states `search` must end on, sorted the same way.
fn expected_final(search: &Search) -> Vec<(u64, u64)> {
    let mut expected = search.expected_states();
    expected.sort_unstable();
    expected
}

#[test]
fn a_terminal_interrupt_leaves_the_interpreter_no_signal_to_print_about() -> Result<()> {
    // A terminal delivers Ctrl-C to every process in its foreground group, and
    // the program serving this format is an interpreter: signalled directly, it
    // raises `KeyboardInterrupt` and prints its traceback over whatever the
    // operator was reading. sima is the one interrupt handler, so its children
    // lead groups of their own and the program is ended by the wind-down, with
    // nothing of its own to say.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = wrapper(dir.path(), &[]);
    // Long enough that the signal lands while the program is computing, so
    // what it does with a signal is what the test observes.
    let search = Search {
        count: 2,
        steps: 400_000,
        ..Search::new()
    };
    let config = loaded_text(dir.path(), "sima.toml", &search.text("./store", &program))?;
    let path = dir.path().join("sima.toml");

    let child = std::process::Command::new(common::built_binary("sima"))
        .args(["search", path.to_str().expect("utf-8 path")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        // A group of its own, so the signal below reaches the search and its
        // children the way a terminal's does, and nothing of the suite's.
        .process_group(0)
        .spawn()
        .expect("spawn sima search");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while !journal_events(&config)
        .iter()
        .any(|event| matches!(event, Event::Leased { .. }))
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the search leased a task before the interrupt"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    // A negative pid names the group, which is what a terminal signals.
    assert_eq!(
        unsafe { libc::kill(-(child.id() as libc::pid_t), libc::SIGINT) },
        0,
        "the search's process group was signalled"
    );
    let output = child.wait_with_output().expect("wait for sima search");
    assert_eq!(
        output.status.code(),
        Some(130),
        "the search wound itself down: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Neither on the terminal nor in the journal: the program's captured
    // stderr is journaled as diagnostics, so a traceback would be recorded
    // there even when nothing reached the terminal.
    let terminal = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    let journaled: String = journal_events(&config)
        .iter()
        .filter_map(|event| match event {
            Event::Diagnostic { message, .. } => Some(message.clone()),
            _ => None,
        })
        .collect::<Vec<String>>()
        .join("\n");
    for said in ["Traceback", "KeyboardInterrupt"] {
        assert!(
            !terminal.contains(said),
            "{said} on the terminal: {terminal}"
        );
        assert!(!journaled.contains(said), "{said} journaled: {journaled}");
    }
    Ok(())
}

#[test]
fn a_python_program_answers_the_domain_service_and_runs_a_search() -> Result<()> {
    // The ordinary path end to end. Loading the configuration asks the program
    // every domain-service question — its environment, its devices, both
    // translations, and the search's specs — and driving it hands each segment an
    // Assign and takes back a Done, with the chain hopping on the state each
    // segment committed.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = wrapper(dir.path(), &[]);
    let search = Search::new();
    let config = loaded_text(dir.path(), "sima.toml", &search.text("./store", &program))?;

    finalized(&run(&config)?);

    // A chain's successor exists once its predecessor commits, so the search's
    // full key set is what the walked store answers: one task per segment of
    // every candidate.
    let store = Store::open(&config.store)?;
    let tasks = (search.count * search.segments.expect("a segmented search")) as usize;
    assert_eq!(task_keys(&config, &store)?.len(), tasks);
    assert_eq!(
        committed_states(&config)?.len(),
        tasks,
        "every segment committed"
    );

    assert_eq!(
        final_states(&config, search.total_steps())?,
        expected_final(&search),
        "each chain ends on the accumulator its increments produce"
    );
    Ok(())
}

#[test]
fn a_python_search_interrupted_between_segments_resumes_to_completion() -> Result<()> {
    // A chain is resumable at every hop: the first orchestration is stopped
    // partway, and a second over the same store walks the chains from where
    // their committed state left them to the same end.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = wrapper(dir.path(), &[]);
    let search = Search {
        // Long enough that the interrupt lands while tasks are still queued,
        // rather than racing a search that is already over.
        count: 8,
        steps: 20_000,
        ..Search::new()
    };
    let text = search.text("./store", &program);

    let config = loaded_text(dir.path(), "sima.toml", &text)?;
    let interrupt = AtomicBool::new(false);
    let committed = AtomicUsize::new(0);
    let observer = |record: &Record| {
        if matches!(record.event, Event::Committed { .. })
            && committed.fetch_add(1, Ordering::Relaxed) + 1 >= 2
        {
            interrupt.store(true, Ordering::Relaxed);
        }
    };
    let outcome = orchestrate(
        &config,
        &SearchControl {
            observer: &observer,
            interrupt: &interrupt,
            on_start: None,
        },
        Engagement::Orchestrator,
        BinaryChange::Refuse,
    )?;
    assert!(
        matches!(outcome, SearchOutcome::Interrupted { .. }),
        "the search stopped partway: {outcome:?}"
    );

    let resumed = loaded_text(dir.path(), "sima.toml", &text)?;
    finalized(&run(&resumed)?);
    assert_eq!(
        final_states(&resumed, search.total_steps())?,
        expected_final(&search),
        "the resumed search lands on the states the whole search would have"
    );
    Ok(())
}

#[test]
fn a_worker_death_mid_segment_resumes_from_the_checkpoint() -> Result<()> {
    // The checkpoint contract across the wire: the program saves at every step
    // boundary, dies without a terminal frame, and the retry inside the same
    // session picks the saved state up. The proof it was used is the steps the
    // successful attempt executed, and the proof it changed nothing is that the
    // committed bytes equal an unarmed search's.
    let dir = tempfile::tempdir().expect("temp dir");
    let search = Search {
        segments: Some(1),
        count: 1,
        steps: 200,
        checkpointing: true,
        max_attempts: 3,
        ..Search::new()
    };

    let reference = wrapper(dir.path(), &[]);
    let unarmed = loaded_text(
        dir.path(),
        "unarmed.toml",
        &search.text("./unarmed", &reference),
    )?;
    finalized(&run(&unarmed)?);

    let armed_dir = dir.path().join("armed");
    std::fs::create_dir(&armed_dir).expect("the armed search's directory");
    let program = wrapper(&armed_dir, &[("STEPPER_EXIT_AT_STEP", "2")]);
    let config = loaded_text(&armed_dir, "sima.toml", &search.text("./store", &program))?;
    finalized(&run(&config)?);

    let steps = committed_steps(&config);
    assert_eq!(steps.len(), 1, "one task committed, got {steps:?}");
    assert!(
        steps[0] < search.steps,
        "the checkpoint must shorten the retry, got {} of {} steps",
        steps[0],
        search.steps
    );
    assert_eq!(
        committed_states(&config)?,
        committed_states(&unarmed)?,
        "the resumed attempt commits what an unarmed search commits"
    );
    Ok(())
}

#[test]
fn a_transient_failure_is_retried_and_the_search_completes() -> Result<()> {
    // A `Done` carrying the failed arm: the parent journals the program's own
    // reason and retries the task, and the search reaches the same end.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = wrapper(dir.path(), &[("STEPPER_FAIL_ONCE", "1")]);
    let search = Search {
        max_attempts: 3,
        ..Search::new()
    };
    let config = loaded_text(dir.path(), "sima.toml", &search.text("./store", &program))?;

    finalized(&run(&config)?);
    let failures: Vec<String> = journal_events(&config)
        .into_iter()
        .filter_map(|event| match event {
            Event::Failed { reason, .. } => Some(reason),
            _ => None,
        })
        .collect();
    assert!(
        failures
            .iter()
            .any(|reason| reason == "armed transient failure"),
        "the program's own reason is journaled: {failures:?}"
    );
    assert_eq!(
        final_states(&config, search.total_steps())?,
        expected_final(&search),
        "the retried tasks land where they would have"
    );
    Ok(())
}

#[test]
fn a_python_exception_crosses_as_a_diagnostic_and_rejects_the_task() -> Result<()> {
    // The panic path: the program's traceback crosses as a structured event
    // before the terminal frame, and the frame is definitive — sima never
    // retries a task whose evaluation raised.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = wrapper(dir.path(), &[("STEPPER_RAISE_ONCE", "1")]);
    let search = Search {
        max_attempts: 3,
        ..Search::new()
    };
    let config = loaded_text(dir.path(), "sima.toml", &search.text("./store", &program))?;

    let outcome = run(&config)?;
    let SearchOutcome::Failed { reason, .. } = &outcome else {
        panic!("a raised evaluation ends the search definitively: {outcome:?}");
    };
    assert!(reason.contains("armed panic"), "{reason}");

    let events = journal_events(&config);
    let diagnostics: Vec<String> = events
        .iter()
        .filter_map(|event| match event {
            Event::Diagnostic {
                source, message, ..
            } if source == "panic" => Some(message.clone()),
            _ => None,
        })
        .collect();
    assert!(
        diagnostics
            .iter()
            .any(|message| message.contains("RuntimeError") && message.contains("armed panic")),
        "the traceback crosses as a panic diagnostic: {diagnostics:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            Event::Rejected { reason, .. } if reason.contains("armed panic")
        )),
        "the raise settles the attempt as a rejection"
    );
    Ok(())
}

#[test]
fn a_zero_increment_candidate_is_rejected_naming_the_reason() -> Result<()> {
    // The rejected arm of `Done`: the program judges the candidate unable to
    // produce a result, and its reason crosses verbatim into the journal and
    // into the search's outcome.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = wrapper(dir.path(), &[]);
    let search = Search {
        segments: None,
        count: 1,
        value: Some(0),
        ..Search::new()
    };
    let config = loaded_text(dir.path(), "sima.toml", &search.text("./store", &program))?;

    let outcome = run(&config)?;
    let SearchOutcome::Failed { reason, .. } = &outcome else {
        panic!("a rejected candidate ends the search definitively: {outcome:?}");
    };
    assert_eq!(reason, "zero increment");
    assert!(
        journal_events(&config).iter().any(|event| matches!(
            event,
            Event::Rejected { reason, .. } if reason == "zero increment"
        )),
        "the program's rejection reason is journaled verbatim"
    );
    Ok(())
}

#[test]
fn a_bad_params_key_fails_the_load_naming_the_programs_message() {
    // The domain service's failure path: a translation the program refuses is
    // surfaced as the program wrote it, at load, before any search exists.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = wrapper(dir.path(), &[]);
    let search = Search::new();
    let text = search
        .text("./store", &program)
        .replace("steps = 5", "stepz = 5");
    let path = dir.path().join("sima.toml");
    std::fs::write(&path, text).expect("write the config");

    let Err(error) = load(&path) else {
        panic!("expected the program to refuse the params section");
    };
    let rendered = error.to_string();
    for named in ["stepz", FORMAT, "takes steps alone"] {
        assert!(
            rendered.contains(named),
            "{named} is missing from {rendered}"
        );
    }
}

/// The steps each committed attempt executed, from the `steps` scalar of every
/// `Committed` event in the search's journal.
fn committed_steps(config: &LoadedConfig) -> Vec<u64> {
    journal_events(config)
        .into_iter()
        .filter_map(|event| match event {
            Event::Committed { stats, .. } => Some(
                stats
                    .iter()
                    .find(|scalar| scalar.name == "steps")
                    .map(|scalar| scalar.value as u64)
                    .expect("the stepper reports a steps scalar"),
            ),
            _ => None,
        })
        .collect()
}

/// One frame: the payload's u32 little-endian length, then the payload. The
/// wire's own framing, written by hand so a test can put a frame on it that no
/// encoder in this workspace would produce.
fn write_frame(stream: &mut impl Write, payload: &[u8]) {
    stream
        .write_all(&(payload.len() as u32).to_le_bytes())
        .expect("write the frame length");
    stream.write_all(payload).expect("write the frame payload");
    stream.flush().expect("flush the frame");
}

/// Reads one frame's payload, or `None` at the end of the stream.
fn read_frame(stream: &mut impl Read) -> Option<Vec<u8>> {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).ok()?;
    let mut payload = vec![0u8; u32::from_le_bytes(prefix) as usize];
    stream.read_exact(&mut payload).ok()?;
    Some(payload)
}

/// The length-prefixed strings of a payload, from `offset` onward.
fn strings(payload: &[u8], mut offset: usize) -> Vec<String> {
    let mut fields = Vec::new();
    while offset < payload.len() {
        let length = u64::from_le_bytes(
            payload[offset..offset + 8]
                .try_into()
                .expect("a u64 length prefix"),
        ) as usize;
        offset += 8;
        fields.push(
            String::from_utf8(payload[offset..offset + length].to_vec()).expect("a UTF-8 field"),
        );
        offset += length;
    }
    fields
}

/// A worker-role `Hello` payload for the example format at protocol version 1,
/// leaving the device to the program.
fn worker_hello() -> Vec<u8> {
    let mut hello = vec![0u8];
    hello.extend(1u32.to_le_bytes());
    hello.extend(0u64.to_le_bytes());
    hello.extend((FORMAT.len() as u64).to_le_bytes());
    hello.extend(FORMAT.as_bytes());
    hello.extend(u64::MAX.to_le_bytes());
    hello.extend(0u64.to_le_bytes());
    hello.push(0);
    hello
}

/// Spawns `command`, waiting out a wrapper that is momentarily unexecutable.
///
/// A file open for writing anywhere in the system cannot be executed. Every
/// test here writes its own wrapper and then runs it, and a spawn elsewhere in
/// this process that forks while one of those writes is open holds that write
/// end until its own exec — so an exec landing inside that window is refused
/// with `ETXTBSY`. The window closes on its own, so the spawn is retried
/// through it and fails loudly past a bound no such window reaches.
fn spawned(command: &mut std::process::Command) -> std::process::Child {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match command.spawn() {
            Ok(child) => return child,
            Err(error)
                if error.raw_os_error() == Some(libc::ETXTBSY)
                    && std::time::Instant::now() < deadline => {}
            Err(error) => panic!("spawn the program: {error}"),
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Drives one worker-role handshake against the example program spawned with
/// `env` on top of the module path, and answers the `Ready` payload's strings.
fn worker_ready(env: &[(&str, &str)]) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let program = wrapper(dir.path(), &[]);
    let mut command = std::process::Command::new(&program);
    command
        .env("PYTHONPATH", vended_sdk(dir.path()))
        .env_remove("SIMA_PROGRAM_DIGEST")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    for (name, value) in env {
        command.env(name, value);
    }
    let mut child = spawned(&mut command);
    let mut stdin = child.stdin.take().expect("the piped stdin");
    let mut stdout = child.stdout.take().expect("the piped stdout");
    write_frame(&mut stdin, &worker_hello());
    let ready = read_frame(&mut stdout).expect("the handshake answer");
    assert_eq!(ready.first(), Some(&0), "the answer is Ready");
    // Closing stdin is the shutdown signal; the session ends there.
    drop(stdin);
    child.wait().expect("reap the program");
    // Past the tag byte and the protocol version: device name, driver, digest.
    strings(&ready, 5)
}

#[test]
fn the_python_worker_echoes_the_program_digest_its_environment_holds() {
    // The SDK's half of the echo contract: the value the spawner stated, back
    // verbatim, with the program never asked to hash anything. Both SDKs answer
    // the same field the same way, so a program written in either language
    // takes part in the same agreement.
    let digest = "d".repeat(64);
    let answered = worker_ready(&[("SIMA_PROGRAM_DIGEST", digest.as_str())]);
    assert_eq!(
        answered.last().map(String::as_str),
        Some(digest.as_str()),
        "the trailing Ready field is the digest the spawn stated"
    );
}

#[test]
fn a_python_worker_spawned_without_a_digest_answers_none() {
    let answered = worker_ready(&[]);
    assert_eq!(
        answered.last().map(String::as_str),
        Some(""),
        "an unset variable answers an empty digest"
    );
    assert_eq!(answered.len(), 3, "device name, driver, and the digest");
}

#[test]
fn a_protocol_violation_ends_the_python_session_rather_than_being_answered() {
    // The two failures a program can meet are different things, and the SDK
    // has to tell them apart the way the Rust host does. A refusal the program
    // itself raises is answered as `Failed` and the session continues; a
    // message tag this side does not know means the stream may no longer be at
    // a frame boundary, so every later answer would be about the wrong
    // question — the session ends instead.
    //
    // What this pins against is the opposite answer: replying `Failed` to a tag
    // the program could not parse and reading on for the next one.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = wrapper(dir.path(), &[]);
    let mut command = std::process::Command::new(&program);
    command
        .args(["--serve-domain", FORMAT])
        .env("PYTHONPATH", vended_sdk(dir.path()))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    let mut child = spawned(&mut command);
    let mut stdin = child.stdin.take().expect("the piped stdin");
    let mut stdout = child.stdout.take().expect("the piped stdout");

    // Hello, tag 0, then the protocol version: the handshake the program
    // answers with Ready before any question.
    let mut hello = vec![0u8];
    hello.extend(1u32.to_le_bytes());
    write_frame(&mut stdin, &hello);
    assert!(
        read_frame(&mut stdout).is_some_and(|ready| ready.first() == Some(&0)),
        "the program answers the handshake with Ready"
    );

    // A tag no version of this protocol assigns.
    write_frame(&mut stdin, &[u8::MAX]);
    assert!(
        read_frame(&mut stdout).is_none(),
        "a tag the program cannot parse ends the session rather than being answered"
    );
    let status = child.wait().expect("reap the program");
    assert!(!status.success(), "the session ends on the violation");
}

#[test]
fn a_malformed_format_id_ends_the_python_session_rather_than_being_answered() {
    // The same distinction on a well-formed tag carrying an id that is not a
    // name. Rust decodes these ids into validated types, so a malformed one
    // fails the decode and ends the session; the SDK reads a plain string, and
    // an id it merely compared and refused would be answered `Failed` with the
    // session running on. The two sides answer a malformed frame the same way.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = wrapper(dir.path(), &[]);
    let mut command = std::process::Command::new(&program);
    command
        .args(["--serve-domain", FORMAT])
        .env("PYTHONPATH", vended_sdk(dir.path()))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    let mut child = spawned(&mut command);
    let mut stdin = child.stdin.take().expect("the piped stdin");
    let mut stdout = child.stdout.take().expect("the piped stdout");

    let mut hello = vec![0u8];
    hello.extend(1u32.to_le_bytes());
    write_frame(&mut stdin, &hello);
    assert!(
        read_frame(&mut stdout).is_some_and(|ready| ready.first() == Some(&0)),
        "the program answers the handshake with Ready"
    );

    // Describe, tag 2, carrying a format id with a byte the name rule excludes.
    let malformed = "NOT A NAME";
    let mut describe = vec![2u8];
    describe.extend((malformed.len() as u64).to_le_bytes());
    describe.extend(malformed.as_bytes());
    write_frame(&mut stdin, &describe);
    assert!(
        read_frame(&mut stdout).is_none(),
        "an id the program cannot decode ends the session rather than being answered"
    );
    let status = child.wait().expect("reap the program");
    assert!(!status.success(), "the session ends on the violation");
}
