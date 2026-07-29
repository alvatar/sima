//! A whole sima program, in the shape a third party writes one.
//!
//! # What a program supplies
//!
//! - [`Doubler`] — the [`Executor`]: evaluates one candidate.
//! - [`Sampler`] — the [`Generator`]: draws candidates from the run's seed.
//! - [`DoublerDomain`] — the [`DomainPlug`]: what the format id binds. It names
//!   the format's environment, enumerates the devices its work runs on, builds
//!   the executor on the device a run placed it on, and translates
//!   `[run.params]`.
//! - [`SamplerPlug`] — the [`GeneratorPlug`]: the same for one generator.
//!
//! `main` builds these and calls [`sima_api::serve`]. That is the whole
//! program: sima drives generation, scheduling, placement, the store,
//! provenance, and the fleet around it.
//!
//! # Reaching it from a run
//!
//! ```toml
//! [run]
//! root_seed = 7
//! format = "example.doubler.v1"
//!
//! [run.generator]
//! id = "example.doubler.v1"
//! count = 8
//!
//! [[execution.device]]
//! select = "example:cpu"      # the class this program mints, below
//! workers = 2
//!
//! [domain."example.doubler.v1"]
//! binary = "/path/to/sima-example-executor"
//! ```
//!
//! # Two properties of the surface
//!
//! `sima-api` is this crate's only sima dependency, so anything the facade does
//! not publish is a compile error here. Configuration sections arrive as text
//! and are parsed with this program's own TOML, which keeps sima's version of
//! it off the published surface.

use sima_api::{
    Artifact, Checkpoint, DeviceBinding, DeviceClass, DeviceInfo, DeviceType, DomainPlug,
    Environment, EnvironmentComponent, EnvironmentValue, Error, ExecutionContext, Executor,
    FormatId, Generator, GeneratorId, GeneratorPlug, Outcome, Params, Result, Spec, Stats,
    TaskInput, prng,
};

/// The format this program serves, and the id its generator registers under.
pub const FORMAT: &str = "example.doubler.v1";

/// The class this program mints for the device it computes on.
///
/// A class is a promise of substitutability: two devices share a class when
/// work bound to one may run on the other. The doubling runs on the host's
/// processor, so one class covers it. A backend over real cards mints whatever
/// separates the ones that cannot stand in for each other — the configuration
/// space identifiers, and a partition profile beside them where a card is
/// sliced.
const DEVICE_CLASS: &str = "example:cpu";

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

    fn execute(
        &self,
        input: &TaskInput<'_>,
        _ctx: &ExecutionContext,
        _checkpoint: &dyn Checkpoint,
    ) -> Result<Outcome> {
        // An empty spec is a candidate that cannot produce a result, so it is
        // rejected rather than failed: retrying it would evaluate the same
        // bytes to the same nothing.
        let Some(&byte) = input.spec.bytes.first() else {
            return Ok(Outcome::Rejected {
                reason: "an empty spec carries no candidate byte".to_string(),
                stats: Stats::empty(),
            });
        };
        let doubled = byte.wrapping_mul(2);
        let mut scalars = vec![("doubled".to_string(), f64::from(doubled))];
        // Stats are observational, so reporting the member is free: it enters
        // no key and no environment, and a run's report gains the device each
        // result came off.
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

    fn generate(&self, root_seed: u64, params: &[u8], format: &FormatId) -> Result<Vec<Spec>> {
        // The count is the generator's whole settings blob: one byte, so a run
        // asks for at most 255 candidates and an absent blob asks for one.
        let count = u64::from(params.first().copied().unwrap_or(1));
        // The run's seed drives the draw, so the same run redraws the same
        // candidates on any machine.
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
    ///
    /// The environment names what the results depend on, so a change to the
    /// executor's arithmetic is a version bump here and every stored result of
    /// the old one stays addressed by the old environment.
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

    fn environment(&self) -> &Environment {
        &self.environment
    }

    fn executor(&self, device: Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>> {
        // A constructor rather than a held executor: the device is known only
        // where execution happens, which is the worker process this call runs
        // in. A real program opens its context here.
        Ok(Box::new(Doubler::new(device.cloned())?))
    }

    fn device_desc(&self, device: Option<&DeviceBinding>) -> Result<(String, String)> {
        // Reported beside every attempt, so a result names the device that
        // produced it. It is observational — a name and a driver version, never
        // part of an identity.
        let name = match device {
            Some(device) => format!("{DEVICE_NAME} #{}", device.member),
            None => DEVICE_NAME.to_string(),
        };
        Ok((name, DRIVER.to_string()))
    }

    fn enumerate(&self) -> Result<Vec<DeviceInfo>> {
        // Every device this format's work can run on. A run resolves its
        // `[[execution.device]]` selectors against this list and spreads its
        // workers over the members of the class each one names, so a program
        // with two cards enumerates two members of one class and a program with
        // a card and an integrated chip enumerates two classes.
        Ok(vec![DeviceInfo {
            class: DeviceClass::new(DEVICE_CLASS)?,
            name: DEVICE_NAME.to_string(),
            device_type: DeviceType::Cpu,
            member: 0,
        }])
    }

    fn translate_params(&self, toml: &str, _segmented: bool) -> Result<Params> {
        // The doubling takes no settings, so the canonical bytes are empty and
        // any key in the section is a fault worth naming: a run whose params
        // were quietly ignored would carry an identity that promised something
        // the executor never read.
        let table = section(toml)?;
        if let Some(key) = table.keys().next() {
            return Err(Error::Validation(format!(
                "[run.params] carries {key:?}; {FORMAT} takes no params"
            )));
        }
        Ok(Params { bytes: Vec::new() })
    }
}

/// The generator that draws this format's candidates.
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

    fn translate_params(&self, toml: &str) -> Result<Vec<u8>> {
        // One key, `count`: how many candidates to draw, 1 to 255 — the blob
        // the generator reads is that one byte.
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

/// Parses a configuration section, which crosses the boundary as the text the
/// run declared. Empty text is a section with no keys.
fn section(toml: &str) -> Result<toml::Table> {
    toml.parse()
        .map_err(|e| Error::Validation(format!("the configuration section is no TOML: {e}")))
}
