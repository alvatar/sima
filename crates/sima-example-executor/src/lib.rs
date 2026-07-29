//! A whole sima program, in the shape a third party writes one.
//!
//! # The division of labour
//!
//! sima draws candidates, schedules them over workers and machines, stores
//! every result by its content, and records how each one was produced. It knows
//! nothing about the problem. A program supplies exactly that: what a candidate
//! is, how to evaluate one, what settings a run may state, and what hardware
//! the work runs on.
//!
//! This program evaluates a candidate of one byte by doubling it. Everything
//! sima needs from a real renderer or simulator, it needs from this too, which
//! is why the whole contract fits in one file.
//!
//! A program registers under a **format id** — here `example.doubler.v1`, 1 to
//! 64 bytes of `[a-z0-9._-]`. It governs how candidate bytes and run params are
//! read, so a format whose meaning changes is a new id. Several methods below
//! do nothing but return it.
//!
//! # How a run reaches it
//!
//! ```toml
//! [run]
//! root_seed = 7
//! format = "example.doubler.v1"
//!
//! [run.generator]
//! id = "example.doubler.v1"
//! count = 8                          # step 3 translates this
//!
//! [[execution.device]]
//! select = "example:cpu"             # step 4 lists this
//! workers = 2
//!
//! [domain."example.doubler.v1"]
//! binary = "/path/to/sima-example-executor"
//! ```
//!
//! sima runs the binary in two roles: once per configured format to ask what
//! the format binds, and once per worker slot to execute tasks. Both are the
//! same program; [`sima_api::serve`] sorts out which is being asked.
//!
//! # The five steps
//!
//! 1. Evaluate one candidate.
//! 2. Produce the candidates.
//! 3. Declare what enters a result's identity.
//! 4. Declare the hardware.
//! 5. Hand it all over — in `main.rs`.
//!
//! `sima-api` is this crate's only sima dependency, so anything the facade does
//! not publish is a compile error here.

use sima_api::{
    Artifact, Checkpoint, DeviceBinding, DeviceClass, DeviceInfo, DeviceType, DomainPlug,
    Environment, EnvironmentComponent, EnvironmentValue, Error, ExecutionContext, Executor,
    FormatId, Generator, GeneratorId, GeneratorPlug, Outcome, Params, Result, Spec, Stats,
    TaskInput, prng,
};

/// The format this program serves, and the id its generator registers under.
pub const FORMAT: &str = "example.doubler.v1";

/// The device's reported name. A run's selector matches either this, as a
/// case-insensitive substring, or the class exactly.
const DEVICE_NAME: &str = "example host processor";

/// Where a backend over real hardware reports its driver version.
const DRIVER: &str = "example.doubler v1";

/// Evaluates a one-byte spec: the result is that byte doubled.
///
/// One executor is built per worker and runs every task that worker takes, so
/// this is where a real program holds what is expensive to acquire: its device
/// context, its compiled kernels, its loaded assets.
pub struct Doubler {
    format: FormatId,
    /// The device this executor computes on, or `None` for the backend's own
    /// default selection.
    device: Option<DeviceBinding>,
}

impl Doubler {
    /// Binds the executor to the format its specs carry and the device it runs
    /// on.
    pub fn new(device: Option<DeviceBinding>) -> Result<Doubler> {
        Ok(Doubler {
            format: FormatId::new(FORMAT)?,
            device,
        })
    }
}

impl Executor for Doubler {
    fn format(&self) -> &FormatId {
        &self.format
    }

    // 1. Evaluate one candidate. This is the work.
    //
    // Called once per task, in a worker process, with the candidate bytes, the
    // run's translated params, and a per-task seed. What it returns is what
    // gets stored:
    //
    // - `Completed` — artifacts are stored by content and addressed by the task
    //   key; stats are observational numbers a report reads.
    // - `Rejected` — this candidate cannot produce a result. Final: sima never
    //   retries it.
    // - `Failed` — this attempt failed. sima retries up to `max_attempts`.
    //
    // Returning `Err` instead means the machinery broke, rather than the
    // candidate. `checkpoint` is for long tasks: offer state periodically and a
    // later attempt resumes from it.
    fn execute(
        &self,
        input: &TaskInput<'_>,
        _ctx: &ExecutionContext,
        _checkpoint: &dyn Checkpoint,
    ) -> Result<Outcome> {
        // Rejected rather than failed: retrying an empty spec would evaluate
        // the same bytes to the same nothing.
        let Some(&byte) = input.spec.bytes.first() else {
            return Ok(Outcome::Rejected {
                reason: "an empty spec carries no candidate byte".to_string(),
                stats: Stats::empty(),
            });
        };
        let doubled = byte.wrapping_mul(2);
        let mut scalars = vec![("doubled".to_string(), f64::from(doubled))];
        // Stats enter no key and no environment, so reporting the device is
        // free: a run's report gains where each result came off.
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

/// Draws one-byte specs from the run's root seed.
pub struct Sampler {
    id: GeneratorId,
}

impl Sampler {
    /// Binds the generator to the id a run config names it by.
    pub fn new() -> Result<Sampler> {
        Ok(Sampler {
            id: GeneratorId::new(FORMAT)?,
        })
    }
}

impl Generator for Sampler {
    fn id(&self) -> &GeneratorId {
        &self.id
    }

    // 2. Produce the candidates.
    //
    // Called in the orchestrator with the run's root seed and the settings blob
    // step 3 produced. The specs returned become the run's tasks, and their
    // bytes enter every task key — so this must be deterministic: the same seed
    // and the same params always yield the same specs, or a resumed run
    // computes different work than it started.
    fn generate(&self, root_seed: u64, params: &[u8], format: &FormatId) -> Result<Vec<Spec>> {
        // The count is the whole settings blob: one byte, so a run asks for at
        // most 255 candidates and an absent blob asks for one.
        let count = u64::from(params.first().copied().unwrap_or(1));
        let mut stream = prng::Stream::new(root_seed);
        Ok((0..count)
            .map(|_| Spec {
                format: format.clone(),
                bytes: vec![stream.next_u64() as u8],
            })
            .collect())
    }
}

/// What the format binds, as this program supplies it: its executor, the
/// environment its results depend on, the devices it runs on, and the
/// translation of its own configuration.
pub struct DoublerDomain {
    format: FormatId,
    environment: Environment,
}

impl DoublerDomain {
    /// The plug a run reaches this program's format through.
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

impl DomainPlug for DoublerDomain {
    fn format(&self) -> &FormatId {
        &self.format
    }

    // 3. Declare what enters a result's identity. Three methods, one rule.
    //
    // sima addresses a result by a key hashed from the candidate, the run's
    // params, and this environment — so two runs agree on a stored result only
    // when they agree on all three. The environment is a list of named versions
    // or digests naming what the results depend on: change the arithmetic in
    // step 1 and bump the version here, after which every result stored by the
    // old one keeps its old address and is never mistaken for the new.
    //
    // The two translations turn a configuration section into the canonical
    // bytes that identity covers. The section arrives as the text the run
    // declared, parsed here with a TOML crate of this program's own choosing,
    // which is what keeps sima's off the surface. `translate_params` below
    // takes `[run.params]` and feeds step 1; `SamplerPlug::translate_params`
    // takes `[run.generator]` and feeds step 2.
    //
    // Rejecting a key this program does not read is therefore not pedantry: a
    // silently ignored setting gives the run an identity promising something
    // the executor never applied.
    fn environment(&self) -> &Environment {
        &self.environment
    }

    fn translate_params(&self, toml: &str, _segmented: bool) -> Result<Params> {
        let table = section(toml)?;
        if let Some(key) = table.keys().next() {
            return Err(Error::Validation(format!(
                "[run.params] carries {key:?}; {FORMAT} takes no params"
            )));
        }
        Ok(Params { bytes: Vec::new() })
    }

    // 4. Declare the hardware. Three methods, one lifecycle.
    //
    // `enumerate` lists every device this program can compute on. A run
    // resolves its `[[execution.device]]` selectors against that list and
    // spreads its workers over the members of the class each one names. A
    // **class** is a promise that its members are interchangeable, so two
    // devices share one only when either can stand in for the other: two
    // identical cards are two members of one class, a card and an integrated
    // chip are two classes. The name is this program's to mint — a GPU backend
    // mints something like `10de:2330`, and adds a partition profile where a
    // card is sliced. A program that opens no device answers with an empty list
    // and takes plain workers.
    //
    // `executor` then builds the executor on the device sima placed this worker
    // on. A constructor rather than a stored object, because it runs in the
    // worker process where the device is finally known — a real program opens
    // its context, compiles its kernels, and loads its assets here, once.
    //
    // `device_desc` names that device as `(name, driver version)`, reported
    // beside every attempt so a result says which hardware produced it. It is
    // observational and enters no identity; two empty strings mean no device.
    fn enumerate(&self) -> Result<Vec<DeviceInfo>> {
        Ok(vec![DeviceInfo {
            class: DeviceClass::new("example:cpu")?,
            name: DEVICE_NAME.to_string(),
            device_type: DeviceType::Cpu,
            member: 0,
        }])
    }

    fn executor(&self, device: Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>> {
        Ok(Box::new(Doubler::new(device.cloned())?))
    }

    fn device_desc(&self, device: Option<&DeviceBinding>) -> Result<(String, String)> {
        let name = match device {
            Some(device) => format!("{DEVICE_NAME} #{}", device.member),
            None => DEVICE_NAME.to_string(),
        };
        Ok((name, DRIVER.to_string()))
    }
}

/// The generator that draws this format's candidates.
///
/// Separate from the domain because one format has one executor and many
/// generators: a run names which one it wants by id.
pub struct SamplerPlug {
    id: GeneratorId,
    format: FormatId,
}

impl SamplerPlug {
    /// The plug a run reaches this program's generator through.
    pub fn new() -> Result<SamplerPlug> {
        Ok(SamplerPlug {
            id: GeneratorId::new(FORMAT)?,
            format: FormatId::new(FORMAT)?,
        })
    }
}

impl GeneratorPlug for SamplerPlug {
    fn id(&self) -> &GeneratorId {
        &self.id
    }

    fn format(&self) -> &FormatId {
        &self.format
    }

    /// The other half of step 3, for the generator's own settings: text in,
    /// opaque bytes out, and those bytes are what step 2 reads. This one takes
    /// `count`, so the blob is one byte.
    fn translate_params(&self, toml: &str) -> Result<Vec<u8>> {
        let table = section(toml)?;
        if let Some(key) = table.keys().find(|key| key.as_str() != "count") {
            return Err(Error::Validation(format!(
                "[run.generator] carries {key:?}; {FORMAT} takes count alone"
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

    fn generator(&self) -> Result<Box<dyn Generator>> {
        Ok(Box::new(Sampler::new()?))
    }
}

/// Parses a configuration section, which crosses as the text the run declared.
/// Empty text is a section with no keys.
fn section(toml: &str) -> Result<toml::Table> {
    toml.parse()
        .map_err(|e| Error::Validation(format!("the configuration section is no TOML: {e}")))
}
