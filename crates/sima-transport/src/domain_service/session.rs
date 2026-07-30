//! The parent's side of the domain service: [`DomainService`] holds the
//! conversation with one program for the life of a run.
//!
//! The program is spawned once, in its domain-service role, and answers every
//! question the run asks about its format: what environment its results depend
//! on, what devices its work runs on, how its configuration translates, and
//! what specs its generators produce. Holding the session open is what lets a
//! program pay its startup cost once.
//!
//! A failure the program renders crosses as [`Error::Reported`], so the
//! classification stays with the process that made it.

use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use sima_contracts::DeviceInfo;
use sima_core::{Error, Result, read_frame, write_frame};
use sima_model::{Environment, FormatId, GeneratorId, Params, Spec};

use crate::domain_service::protocol::{FromDomain, PROTOCOL_VERSION, ToDomain};

/// The flag that puts a program in its domain-service role.
const SERVE_DOMAIN: &str = "--serve-domain";

/// One program, spawned to answer for one format.
#[derive(Debug)]
pub struct DomainService {
    child: Child,
    /// The child's stdin; dropping it is the shutdown signal that follows the
    /// farewell.
    stdin: Option<ChildStdin>,
    stdout: ChildStdout,
    /// The program, for diagnostics: a failure names which binary produced it.
    binary: PathBuf,
}

impl DomainService {
    /// Spawns `binary` in its domain-service role for `format` and completes
    /// the handshake, so a program that cannot be run, cannot speak this
    /// protocol version, or does not serve the format fails here rather than at
    /// the first question.
    pub fn spawn(binary: &Path, format: &FormatId) -> Result<DomainService> {
        let mut child = Command::new(binary)
            .arg(SERVE_DOMAIN)
            .arg(format.as_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| {
                Error::Transport(format!(
                    "spawning the domain service {} failed: {e}",
                    binary.display()
                ))
            })?;
        // The pipes exist iff the spawn configured them; taking them cannot
        // fail past a successful spawn.
        let stdin = child.stdin.take().ok_or_else(|| {
            Error::Transport("the spawned domain service has no piped stdin".to_string())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            Error::Transport("the spawned domain service has no piped stdout".to_string())
        })?;
        let mut service = DomainService {
            child,
            stdin: Some(stdin),
            stdout,
            binary: binary.to_path_buf(),
        };
        // The handshake: Hello out, Ready back. Any other answer is a spawn
        // failure, and the misbehaving program is killed and reaped before the
        // error returns. A program that refuses the format it was spawned for
        // exits at once, which the parent meets as a torn write or as an
        // unanswered read depending on which side moved first; both are the
        // handshake refused, so both read as that, with the cause behind it.
        match service.handshake() {
            Ok(()) => Ok(service),
            Err(e) => {
                service.kill();
                Err(Error::Transport(format!(
                    "the domain service {} refused the handshake: {e}",
                    binary.display()
                )))
            }
        }
    }

    /// The environment the format's results depend on.
    pub fn environment(&mut self, format: &FormatId) -> Result<Environment> {
        match self.ask(&ToDomain::Describe {
            format: format.clone(),
        })? {
            FromDomain::Described { environment } => Ok(environment),
            other => Err(self.unexpected("Described", &other)),
        }
    }

    /// The devices the format's work can run on.
    pub fn enumerate_devices(&mut self, format: &FormatId) -> Result<Vec<DeviceInfo>> {
        match self.ask(&ToDomain::Enumerate {
            format: format.clone(),
        })? {
            FromDomain::Enumerated { devices } => Ok(devices),
            other => Err(self.unexpected("Enumerated", &other)),
        }
    }

    /// The `[run.params]` section, as text, translated into the format's
    /// canonical params bytes.
    pub fn translate_config(
        &mut self,
        format: &FormatId,
        toml: &str,
        segmented: bool,
    ) -> Result<Params> {
        match self.ask(&ToDomain::TranslateParams {
            format: format.clone(),
            toml: toml.to_string(),
            segmented,
        })? {
            FromDomain::Translated { bytes } => Ok(Params { bytes }),
            other => Err(self.unexpected("Translated", &other)),
        }
    }

    /// The `[run.generator]` section, as text, translated into the generator's
    /// opaque params blob.
    pub fn translate_generator_config(
        &mut self,
        generator: &GeneratorId,
        toml: &str,
    ) -> Result<Vec<u8>> {
        match self.ask(&ToDomain::TranslateGeneratorParams {
            generator: generator.clone(),
            toml: toml.to_string(),
        })? {
            FromDomain::Translated { bytes } => Ok(bytes),
            other => Err(self.unexpected("Translated", &other)),
        }
    }

    /// The run's candidate specs.
    pub fn generate(
        &mut self,
        generator: &GeneratorId,
        format: &FormatId,
        root_seed: u64,
        params: &[u8],
    ) -> Result<Vec<Spec>> {
        match self.ask(&ToDomain::Generate {
            generator: generator.clone(),
            format: format.clone(),
            root_seed,
            params: params.to_vec(),
        })? {
            FromDomain::Generated { specs } => Ok(specs),
            other => Err(self.unexpected("Generated", &other)),
        }
    }

    /// Opens the conversation, refusing a program that speaks another version.
    fn handshake(&mut self) -> Result<()> {
        match self.ask(&ToDomain::Hello {
            protocol: PROTOCOL_VERSION,
        })? {
            FromDomain::Ready { protocol } if protocol == PROTOCOL_VERSION => Ok(()),
            FromDomain::Ready { protocol } => Err(Error::Transport(format!(
                "domain service {} protocol version mismatch: parent speaks \
                 {PROTOCOL_VERSION}, it speaks {protocol}",
                self.binary.display()
            ))),
            other => Err(self.unexpected("Ready", &other)),
        }
    }

    /// Asks one question and reads its answer. A failure the program rendered
    /// crosses verbatim.
    fn ask(&mut self, question: &ToDomain) -> Result<FromDomain> {
        let stdin = self.stdin.as_mut().ok_or_else(|| {
            Error::Transport(format!(
                "the domain service {} is already closed",
                self.binary.display()
            ))
        })?;
        write_frame(stdin, &question.encode())?;
        let Some(payload) = read_frame(&mut self.stdout)? else {
            return Err(Error::Transport(format!(
                "the domain service {} ended before answering",
                self.binary.display()
            )));
        };
        match FromDomain::decode(&payload)? {
            FromDomain::Failed { message } => Err(Error::Reported(message)),
            answer => Ok(answer),
        }
    }

    /// An answer of the wrong shape: a protocol violation naming what was
    /// expected and which program sent it.
    fn unexpected(&self, expected: &str, answer: &FromDomain) -> Error {
        Error::Transport(format!(
            "expected {expected} from the domain service {}, got {answer:?}",
            self.binary.display()
        ))
    }

    /// Kills the program and reaps it. Best effort: one already dead is fine.
    fn kill(&mut self) {
        self.stdin = None;
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for DomainService {
    /// Says goodbye, then closes the pipe and reaps the program. A farewell
    /// that cannot be written means the program is already gone, so the close
    /// and the reap are what settle it.
    fn drop(&mut self) {
        if let Some(stdin) = self.stdin.as_mut() {
            let _ = write_frame(stdin, &ToDomain::Goodbye.encode());
        }
        self.stdin = None;
        let _ = self.child.wait();
    }
}
