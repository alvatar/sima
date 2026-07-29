//! The smallest program that hosts a domain against the published surface
//! alone: an executor, a generator, and the two plugs that hand them over.
//!
//! The manifest is the assertion: `sima-api` is the only sima dependency, so
//! anything needed here that the facade does not re-export is a compile error
//! rather than a discovery made out of tree. The configuration sections arrive
//! as text and are parsed here with this program's own TOML, which is why
//! sima's never enters the surface.
//!
//! The whole of a hosted program is in this file and its `main`: implement
//! [`DomainPlug`] and [`GeneratorPlug`], then call [`sima_api::serve`].

use sima_api::{
    Artifact, Checkpoint, DeviceBinding, DeviceInfo, DomainPlug, Environment, EnvironmentComponent,
    EnvironmentValue, Error, ExecutionContext, Executor, FormatId, Generator, GeneratorId,
    GeneratorPlug, Outcome, Params, Result, Spec, Stats, TaskInput, prng,
};

/// The format this program serves, and the id its generator registers under.
pub const FORMAT: &str = "example.doubler.v1";

/// Evaluates a one-byte spec: the result is that byte doubled.
pub struct Doubler {
    format: FormatId,
}

impl Doubler {
    /// Binds the executor to the format its specs carry.
    pub fn new() -> Result<Doubler> {
        Ok(Doubler {
            format: FormatId::new(FORMAT)?,
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
        Ok(Outcome::Completed {
            artifacts: vec![Artifact {
                name: "doubled".to_string(),
                bytes: vec![doubled],
            }],
            stats: Stats {
                scalars: vec![("doubled".to_string(), f64::from(doubled))],
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
/// environment its results depend on, the devices it runs on — none, it
/// computes in the worker process — and the translation of its own
/// configuration.
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

    fn executor(&self, _device: Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>> {
        Ok(Box::new(Doubler::new()?))
    }

    fn device_desc(&self, _device: Option<&DeviceBinding>) -> Result<(String, String)> {
        // The arithmetic runs in the worker process, so there is no device to
        // name and no driver to report.
        Ok((String::new(), String::new()))
    }

    fn enumerate(&self) -> Result<Vec<DeviceInfo>> {
        Ok(Vec::new())
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

/// Parses a configuration section, which crosses the seam as the text the run
/// declared. Empty text is a section with no keys.
fn section(toml: &str) -> Result<toml::Table> {
    toml.parse()
        .map_err(|e| Error::Validation(format!("the configuration section is no TOML: {e}")))
}

#[cfg(test)]
mod tests {
    use sima_api::{DeviceClass, DeviceInfo, DeviceType, DomainPlug, GeneratorPlug, serve};

    use super::*;

    #[test]
    fn the_published_surface_names_a_device_list() {
        // What a domain answers when asked which devices its work runs on.
        // These two executors compute in the worker process and open none, so
        // the vocabulary is exercised rather than used: a domain that does open
        // a device builds its answer out of exactly these types, and they reach
        // it through the facade alone.
        let device = DeviceInfo {
            class: DeviceClass::new("8086:7d51").expect("class id"),
            name: "Intel(R) Graphics (ARL)".to_string(),
            device_type: DeviceType::Integrated,
            member: 0,
        };
        assert_eq!(device.class.as_str(), "8086:7d51");
        assert_eq!(device.member, 0);
    }

    #[test]
    fn the_published_surface_names_what_a_hosted_program_is() {
        // The whole of a program outside the workspace: the two plugs it
        // implements and the one call that hosts them. Naming them here is
        // what keeps them reachable through the facade alone.
        let host: fn(&dyn DomainPlug, &[&dyn GeneratorPlug]) -> sima_api::Result<()> = serve;
        assert_eq!(std::mem::size_of_val(&host), std::mem::size_of::<fn()>());
    }

    #[test]
    fn the_generator_section_carries_the_candidate_count() {
        // The section arrives as its text and leaves as the blob the generator
        // reads, so a count declared in the file is the count drawn.
        let plug = SamplerPlug::new().expect("the plug binds");
        assert_eq!(
            plug.translate_params("count = 7").expect("a count"),
            vec![7]
        );
        // An absent section draws one candidate.
        assert_eq!(plug.translate_params("").expect("a default"), vec![1]);
    }

    #[test]
    fn a_count_outside_the_range_names_itself() {
        let plug = SamplerPlug::new().expect("the plug binds");
        for text in ["count = 0", "count = 256", "count = \"many\""] {
            assert!(plug.translate_params(text).is_err(), "{text}");
        }
    }

    #[test]
    fn a_key_the_program_does_not_take_is_a_failure() {
        // A section quietly ignored would leave a run whose identity promised
        // settings the executor never read.
        let domain = DoublerDomain::new().expect("the plug binds");
        let error = domain
            .translate_params("width = 128", false)
            .expect_err("a key this format does not take");
        assert!(error.to_string().contains("width"), "{error}");
        let plug = SamplerPlug::new().expect("the plug binds");
        assert!(plug.translate_params("width = 128").is_err());
    }

    #[test]
    fn the_domain_binds_its_executor_and_opens_no_device() {
        let domain = DoublerDomain::new().expect("the plug binds");
        assert_eq!(domain.format().as_str(), FORMAT);
        assert_eq!(
            domain
                .executor(None)
                .expect("an executor")
                .format()
                .as_str(),
            FORMAT
        );
        assert!(domain.enumerate().expect("an enumeration").is_empty());
        assert_eq!(
            domain.device_desc(None).expect("a description"),
            (String::new(), String::new())
        );
        assert_eq!(
            domain.translate_params("", false).expect("no params").bytes,
            Vec::<u8>::new()
        );
    }

    #[test]
    fn the_generator_plug_produces_specs_of_the_format() {
        let plug = SamplerPlug::new().expect("the plug binds");
        let format = FormatId::new(FORMAT).expect("format id");
        let specs = plug
            .generator()
            .expect("a generator")
            .generate(42, &[3], &format)
            .expect("three candidates");
        assert_eq!(specs.len(), 3);
        assert!(specs.iter().all(|spec| spec.format == format));
    }
}
