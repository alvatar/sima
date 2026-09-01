//! A whole sima program, in the shape a third party writes one.
//!
//! # What sima does, and what a program supplies
//!
//! sima draws candidates, schedules them over workers and machines, stores
//! every result by its content, and records how each one was produced. A
//! program supplies the problem itself: what a candidate is, how to evaluate
//! one, what settings a search may state, and what hardware the work searches on.
//!
//! This program evaluates a candidate of one byte by doubling it. Everything
//! sima needs from a real program, it needs from this one too, which is why
//! the whole contract fits in one file.
//!
//! # The two components
//!
//! - [`DoublerGenerator`], the [`Generator`]: which candidates to try.
//! - [`DoublerDomain`], the [`Domain`]: what the format is, and the
//!   [`DoublerExecutor`] it builds to evaluate each candidate.
//!
//! A program registers under a **format id** — here `example.doubler.v1`, 1 to
//! 64 bytes of `[a-z0-9._-]`. It governs how candidate bytes and search params are
//! read, so a format whose meaning changes is a new id. Several methods below
//! exist solely to return it.
//!
//! # How a search reaches this program
//!
//! ```toml
//! [search]
//! root_seed = 7
//! format = "example.doubler.v1"
//!
//! [search.generator]
//! id = "example.doubler.v1"
//! count = 8                          # step 3 translates this
//!
//! [[orchestrator.device]]
//! select = "example:cpu"             # step 5 lists this
//! workers = 2
//!
//! [domain."example.doubler.v1"]
//! binary = "/path/to/sima-example-executor"
//! ```
//!
//! sima searches the binary in two roles: once per configured format to ask what
//! the format binds, and once per worker slot to execute tasks. Both are the
//! same program; [`sima_api::serve`] resolves which is being asked.
//!
//! Each role starts in a fresh scratch directory of its own, with the
//! environment reduced to what the platform needs — the loader, the locale,
//! the user's caches, the GPU stacks — plus the variable names the entry's
//! optional `env` key lists. So a program names its assets by absolute path,
//! and reads any setting of its own from a variable its entry declares.
//!
//! # The six steps
//!
//! 1. Produce the candidates.
//! 2. Evaluate one candidate.
//! 3. Translate the configuration each component owns.
//! 4. Declare what enters a result's identity.
//! 5. Declare the hardware.
//! 6. Hand both components over — `main.rs`.
//!
//! `sima-api` is this crate's only sima dependency, so anything the facade does
//! not publish is a compile error here.

use sima_api::{
    Artifact, Checkpoint, DeviceBinding, DeviceClass, DeviceInfo, DeviceType, Domain, Environment,
    EnvironmentComponent, EnvironmentValue, Error, ExecutionContext, Executor, FormatId, Generator,
    GeneratorId, Outcome, Params, Result, Spec, Stats, TaskInput, prng,
};

/// The format this program serves, and the id its generator registers under.
pub const FORMAT: &str = "example.doubler.v1";

/// The device's reported name. A search's selector matches either this, as a
/// case-insensitive substring, or the class exactly.
const DEVICE_NAME: &str = "example host processor";

/// Where a backend over real hardware reports its driver version.
const DRIVER: &str = "example.doubler v1";

// ===========================================================================
// The generator: which candidates the search tries.
// ===========================================================================

/// Draws one-byte specs from the search's root seed.
///
/// Separate from the domain because one format has one executor and many
/// generators: a search names which one it wants by id.
pub struct DoublerGenerator {
    id: GeneratorId,
    format: FormatId,
}

impl DoublerGenerator {
    /// The generator a search reaches by id.
    pub fn new() -> Result<DoublerGenerator> {
        Ok(DoublerGenerator {
            id: GeneratorId::new(FORMAT)?,
            format: FormatId::new(FORMAT)?,
        })
    }
}

impl Generator for DoublerGenerator {
    fn id(&self) -> &GeneratorId {
        &self.id
    }

    fn format(&self) -> &FormatId {
        &self.format
    }

    // 1. Produce the candidates.
    //
    // Called in the orchestrator with the search's root seed and the blob step 3
    // produced. The specs become the search's tasks and their bytes enter every
    // task key, so this must be deterministic: the same seed and params always
    // yield the same specs, or a resumed search computes different work than it
    // started.
    fn generate(&self, root_seed: u64, params: &[u8]) -> Result<Vec<Spec>> {
        // The count is the whole settings blob: one byte, so a search asks for at
        // most 255 candidates and an absent blob asks for one.
        let count = u64::from(params.first().copied().unwrap_or(1));
        let mut stream = prng::Stream::new(root_seed);
        Ok((0..count)
            .map(|_| Spec {
                format: self.format.clone(),
                bytes: vec![stream.next_u64() as u8],
            })
            .collect())
    }

    // 3. Translate `[search.generator]`, the section this component owns.
    //
    // Text in, canonical bytes out — the bytes step 1 reads and the search id
    // covers. The text is parsed with a TOML crate of this program's choosing,
    // which is what keeps sima's off the surface. This one takes `count`, so
    // the blob is one byte.
    fn translate_config(&self, toml: &str) -> Result<Vec<u8>> {
        let table = section(toml)?;
        if let Some(key) = table.keys().find(|key| key.as_str() != "count") {
            return Err(Error::Validation(format!(
                "[search.generator] carries {key:?}; {FORMAT} takes count alone"
            )));
        }
        let Some(value) = table.get("count") else {
            return Ok(vec![1]);
        };
        let count = value
            .as_integer()
            .and_then(|count| u8::try_from(count).ok())
            .filter(|count| *count > 0)
            .ok_or_else(|| Error::Validation(format!("count is 1 to 255, got {value}")))?;
        Ok(vec![count])
    }
}

// ===========================================================================
// The executor: evaluates one candidate. The domain below builds one per
// worker.
// ===========================================================================

/// Evaluates a one-byte spec: the result is that byte doubled.
///
/// One executor is built per worker and searches every task that worker takes, so
/// this is where a real program holds what is expensive to acquire: its device
/// context, its compiled kernels, its loaded assets.
pub struct DoublerExecutor {
    format: FormatId,
    /// The device this executor computes on, or `None` for the backend's own
    /// default selection.
    device: Option<DeviceBinding>,
}

impl DoublerExecutor {
    /// Binds the executor to the format its specs carry and the device it searches
    /// on.
    pub fn new(device: Option<DeviceBinding>) -> Result<DoublerExecutor> {
        Ok(DoublerExecutor {
            format: FormatId::new(FORMAT)?,
            device,
        })
    }
}

impl Executor for DoublerExecutor {
    fn format(&self) -> &FormatId {
        &self.format
    }

    // 2. Evaluate one candidate. This is the work.
    //
    // Called once per task, in a worker process, with the candidate bytes, the
    // search's translated params, and a per-task seed. What it returns is stored:
    //
    // - `Completed` — artifacts are stored by content under the task key;
    //   stats are observational numbers a report reads.
    // - `Rejected` — this candidate cannot produce a result. Final.
    // - `Failed` — this attempt failed. sima retries up to `max_attempts`.
    //
    // Returning `Err` means the machinery broke rather than the candidate.
    // `checkpoint` is for long tasks: offer state periodically and a later
    // attempt resumes from it.
    fn execute(
        &self,
        input: &TaskInput<'_>,
        _ctx: &ExecutionContext,
        _checkpoint: &dyn Checkpoint,
    ) -> Result<Outcome> {
        // Rejected rather than failed: retrying an empty spec would evaluate
        // the same bytes to the same outcome.
        let Some(&byte) = input.spec.bytes.first() else {
            return Ok(Outcome::Rejected {
                reason: "an empty spec carries no candidate byte".to_string(),
                stats: Stats::empty(),
            });
        };
        let doubled = byte.wrapping_mul(2);
        let mut scalars = vec![("doubled".to_string(), f64::from(doubled))];
        // Stats enter no key and no environment, so reporting the device is
        // free: a search's report shows which member produced each result.
        if let Some(device) = &self.device {
            scalars.push(("device.member".to_string(), f64::from(device.member)));
        }
        Ok(Outcome::Completed {
            artifacts: vec![Artifact {
                name: "doubled".to_string(),
                bytes: vec![doubled],
            }],
            stats: Stats {
                scalars,
                blob: Vec::new(),
            },
        })
    }
}

// ===========================================================================
// The domain: what the format is, and the executor it builds.
// ===========================================================================

/// What the format binds: its executor, the environment its results depend on,
/// the devices it searches on, and the translation of `[search.params]`.
pub struct DoublerDomain {
    format: FormatId,
    environment: Environment,
}

impl DoublerDomain {
    /// The domain a search reaches this program's format through.
    pub fn new() -> Result<DoublerDomain> {
        Ok(DoublerDomain {
            format: FormatId::new(FORMAT)?,
            environment: Environment::new(vec![EnvironmentComponent::new(
                "example.doubler.executor",
                EnvironmentValue::Version("v1".to_string()),
            )?])?,
        })
    }
}

impl Domain for DoublerDomain {
    fn format(&self) -> &FormatId {
        &self.format
    }

    // 3, again: `[search.params]` is this component's section, translated the
    // same way. Its bytes are what step 2 reads, and they enter the search id.
    //
    // Rejecting an unread key protects that identity: a silently ignored
    // setting gives the search an identity promising something the executor never
    // applied.
    fn translate_config(&self, toml: &str, _segmented: bool) -> Result<Params> {
        let table = section(toml)?;
        if let Some(key) = table.keys().next() {
            return Err(Error::Validation(format!(
                "[search.params] carries {key:?}; {FORMAT} takes no params"
            )));
        }
        Ok(Params { bytes: Vec::new() })
    }

    // 4. Declare what enters a result's identity.
    //
    // sima addresses a result by a key hashed from the candidate, the search's
    // params, and this environment. Two searches agree on a stored result only when
    // all three agree, so change the arithmetic in step 2 and bump this
    // version: every result stored by the old one keeps its old address.
    fn environment(&self) -> &Environment {
        &self.environment
    }

    // 5. Declare the hardware. Three methods, one lifecycle.
    //
    // `enumerate_devices` lists every device this program can compute on. A search
    // resolves its `[[orchestrator.device]]` selectors against the list and
    // spreads its workers over the members of the class each one names. A
    // **class** promises its members are interchangeable, so two identical
    // cards are two members of one class and a card beside an integrated chip
    // is two classes. The name is this program's to mint — a GPU backend mints
    // something like `10de:2330`, plus a partition profile where a card is
    // sliced. An empty list means plain workers, no device.
    fn enumerate_devices(&self) -> Result<Vec<DeviceInfo>> {
        Ok(vec![DeviceInfo {
            class: DeviceClass::new("example:cpu")?,
            name: DEVICE_NAME.to_string(),
            device_type: DeviceType::Cpu,
            member: 0,
        }])
    }

    // `executor` builds the executor on the device sima placed this worker on.
    // A constructor rather than a stored object, because it searches in the worker
    // process where the device is finally known: a real program opens its
    // context, compiles its kernels, and loads its assets here, once.
    fn executor(&self, device: Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>> {
        Ok(Box::new(DoublerExecutor::new(device.cloned())?))
    }

    // `device_desc` names that device as `(name, driver version)`, reported
    // beside every attempt so a result says which hardware produced it. It is
    // observational and enters no identity; two empty strings mean no device.
    fn device_desc(&self, device: Option<&DeviceBinding>) -> Result<(String, String)> {
        let name = match device {
            Some(device) => format!("{DEVICE_NAME} #{}", device.member),
            None => DEVICE_NAME.to_string(),
        };
        Ok((name, DRIVER.to_string()))
    }
}

/// Parses a configuration section, which crosses as the text the search declared.
/// Empty text is a section with no keys.
fn section(toml: &str) -> Result<toml::Table> {
    toml.parse()
        .map_err(|e| Error::Validation(format!("the configuration section is not valid TOML: {e}")))
}
