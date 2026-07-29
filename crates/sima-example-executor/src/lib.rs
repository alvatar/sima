//! The smallest executor and generator that compile against the published
//! surface alone.
//!
//! The manifest is the assertion: `sima-api` is the only sima dependency, so
//! anything needed here that the facade does not re-export is a compile error
//! rather than a discovery made out of tree.

use sima_api::{
    Artifact, Checkpoint, ExecutionContext, Executor, FormatId, Generator, GeneratorId, Outcome,
    Result, Spec, Stats, TaskInput, prng,
};

/// Evaluates a one-byte spec: the result is that byte doubled.
pub struct Doubler {
    format: FormatId,
}

impl Doubler {
    /// Binds the executor to the format its specs carry.
    pub fn new() -> Result<Doubler> {
        Ok(Doubler {
            format: FormatId::new("example.doubler.v1")?,
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
            id: GeneratorId::new("example.doubler.v1")?,
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

#[cfg(test)]
mod tests {
    use sima_api::{DeviceClass, DeviceInfo, DeviceType};

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
}
