//! The Gray-Scott generator's configuration: the candidate count and the
//! sampling range of each genome parameter, with its canonical byte codec.

use sima_core::{Dec, Enc, Error, Result};

use super::genome::GrayScottGenome;

/// The Gray-Scott generator's params: the candidate count and the sampled
/// box — one `[lo, hi]` range per genome parameter, in the frozen field
/// order `feed`, `kill`, `diffusion_u`, `diffusion_v`.
///
/// A degenerate range `[v, v]` fixes its parameter, so a Pearson-style run —
/// vary `feed` and `kill`, pin the classical diffusion pair — is a
/// configuration of this one generator, never a second generator.
///
/// The canonical form is the `u64` count, then the four ranges in field
/// order, each as `f32` lo then `f32` hi — 40 bytes, carrying no domain tag:
/// the config lives inside a params blob, which frames it.
// On constructed values the derived `PartialEq` is total and coincides with
// byte equality: the corner validation excludes NaN (which would make
// equality irreflexive) and -0.0 (the one value that equals another
// numerically while differing in bytes) from every bound, and `u64`
// equality is exact.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GrayScottGeneratorConfig {
    /// The number of candidates to draw.
    count: u64,
    /// Sampling range `[lo, hi]` of the feed rate `f`.
    feed: [f32; 2],
    /// Sampling range `[lo, hi]` of the kill rate `k`.
    kill: [f32; 2],
    /// Sampling range `[lo, hi]` of the `u` channel's diffusion coefficient.
    diffusion_u: [f32; 2],
    /// Sampling range `[lo, hi]` of the `v` channel's diffusion coefficient.
    diffusion_v: [f32; 2],
}

impl GrayScottGeneratorConfig {
    /// Builds a config, validating the count and the box: `count` must be at
    /// least 1, both box corners must construct as genomes, and each range
    /// must satisfy `lo ≤ hi`. Any violation is [`Error::Validation`].
    pub fn new(
        count: u64,
        feed: [f32; 2],
        kill: [f32; 2],
        diffusion_u: [f32; 2],
        diffusion_v: [f32; 2],
    ) -> Result<GrayScottGeneratorConfig> {
        // A zero-candidate run is meaningless; enforcing it here makes
        // decode, which funnels through this constructor, reject such
        // blobs too.
        if count == 0 {
            return Err(Error::Validation(format!(
                "gray-scott generator count must be at least 1, got {count}"
            )));
        }
        // Both box corners must construct as genomes — the genome's own
        // validation, reused verbatim, whose errors already name the
        // parameter and the value. Each parameter's valid set is an
        // interval, so valid corners imply every point of the box is valid.
        GrayScottGenome::new(feed[0], kill[0], diffusion_u[0], diffusion_v[0])?;
        GrayScottGenome::new(feed[1], kill[1], diffusion_u[1], diffusion_v[1])?;
        // Ordered bounds, checked after the corners so both bounds are known
        // non-NaN and the comparison is meaningful.
        for (name, [lo, hi]) in [
            ("feed", feed),
            ("kill", kill),
            ("diffusion_u", diffusion_u),
            ("diffusion_v", diffusion_v),
        ] {
            if lo > hi {
                return Err(Error::Validation(format!(
                    "gray-scott generator {name} range must satisfy lo <= hi, got [{lo}, {hi}]"
                )));
            }
        }
        Ok(GrayScottGeneratorConfig {
            count,
            feed,
            kill,
            diffusion_u,
            diffusion_v,
        })
    }

    /// The number of candidates to draw.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// The sampling range `[lo, hi]` of the feed rate `f`.
    pub fn feed(&self) -> [f32; 2] {
        self.feed
    }

    /// The sampling range `[lo, hi]` of the kill rate `k`.
    pub fn kill(&self) -> [f32; 2] {
        self.kill
    }

    /// The sampling range `[lo, hi]` of the `u` channel's diffusion
    /// coefficient.
    pub fn diffusion_u(&self) -> [f32; 2] {
        self.diffusion_u
    }

    /// The sampling range `[lo, hi]` of the `v` channel's diffusion
    /// coefficient.
    pub fn diffusion_v(&self) -> [f32; 2] {
        self.diffusion_v
    }

    /// Appends the canonical form: the `u64` count, then the four ranges in
    /// the frozen field order, each bound via [`Enc::f32`].
    pub fn encode(&self, enc: &mut Enc) {
        enc.u64(self.count);
        for [lo, hi] in [self.feed, self.kill, self.diffusion_u, self.diffusion_v] {
            enc.f32(lo).f32(hi);
        }
    }

    /// Reads a canonical form written by [`GrayScottGeneratorConfig::encode`],
    /// funneling the values through [`GrayScottGeneratorConfig::new`] so
    /// decode and construction share one validation path.
    pub fn decode(dec: &mut Dec<'_>) -> Result<GrayScottGeneratorConfig> {
        let count = dec.u64()?;
        let mut range = || -> Result<[f32; 2]> { Ok([dec.f32()?, dec.f32()?]) };
        let feed = range()?;
        let kill = range()?;
        let diffusion_u = range()?;
        let diffusion_v = range()?;
        GrayScottGeneratorConfig::new(count, feed, kill, diffusion_u, diffusion_v)
    }

    /// The standalone canonical bytes of this config.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        self.encode(&mut enc);
        enc.finish()
    }

    /// Parses standalone canonical bytes, rejecting trailing input.
    pub fn from_bytes(bytes: &[u8]) -> Result<GrayScottGeneratorConfig> {
        let mut dec = Dec::new(bytes);
        let config = GrayScottGeneratorConfig::decode(&mut dec)?;
        dec.finish()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::to_hex;

    use super::*;

    /// A config spanning the interesting region of parameter space
    /// (Pearson's map) with the classical diffusion pair pinned.
    fn sample() -> GrayScottGeneratorConfig {
        GrayScottGeneratorConfig::new(64, [0.01, 0.08], [0.03, 0.07], [0.16, 0.16], [0.08, 0.08])
            .expect("valid sample config")
    }

    /// The sample's ranges with `range` substituted at `position`, in the
    /// frozen field order `feed`, `kill`, `diffusion_u`, `diffusion_v`.
    fn with_substituted(position: usize, range: [f32; 2]) -> Result<GrayScottGeneratorConfig> {
        let mut r = [[0.01, 0.08], [0.03, 0.07], [0.16, 0.16], [0.08, 0.08]];
        r[position] = range;
        GrayScottGeneratorConfig::new(64, r[0], r[1], r[2], r[3])
    }

    /// The canonical bytes of [`sample`], derived by hand from the layout —
    /// the `u64` count little-endian, then each bound's `to_bits()` as
    /// little-endian hex in the frozen order — and independently reproduced
    /// with Python `struct`: `(64).to_bytes(8, 'little').hex() +
    /// ''.join(struct.pack('<f', v).hex() for v in (0.01, 0.08, 0.03, 0.07,
    /// 0.16, 0.16, 0.08, 0.08))`.
    const CONFIG_BYTES_HEX: &str =
        "40000000000000000ad7233c0ad7a33d8fc2f53c295c8f3d0ad7233e0ad7233e0ad7a33d0ad7a33d";

    #[test]
    fn config_is_byte_stable() {
        assert_eq!(to_hex(&sample().to_bytes()), CONFIG_BYTES_HEX);
    }

    #[test]
    fn config_round_trips_through_bytes() -> Result<()> {
        let minimal = GrayScottGeneratorConfig::new(
            1,
            [0.01, 0.01],
            [0.03, 0.03],
            [0.16, 0.16],
            [0.08, 0.08],
        )?;
        for config in [sample(), minimal] {
            assert_eq!(
                GrayScottGeneratorConfig::from_bytes(&config.to_bytes())?,
                config
            );
        }
        Ok(())
    }

    #[test]
    fn config_rejects_truncation_and_trailing() {
        let full = sample().to_bytes();
        assert_eq!(full.len(), 40);
        for cut in 0..full.len() {
            assert!(
                matches!(
                    GrayScottGeneratorConfig::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
        let mut trailing = full;
        trailing.push(0);
        assert!(matches!(
            GrayScottGeneratorConfig::from_bytes(&trailing),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn config_rejects_zero_count() {
        assert!(matches!(
            GrayScottGeneratorConfig::new(
                0,
                [0.01, 0.08],
                [0.03, 0.07],
                [0.16, 0.16],
                [0.08, 0.08]
            ),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn config_rejects_inverted_ranges() {
        let names = ["feed", "kill", "diffusion_u", "diffusion_v"];
        for (position, name) in names.iter().enumerate() {
            match with_substituted(position, [0.07, 0.03]) {
                Err(Error::Validation(message)) => {
                    assert!(
                        message.contains(name),
                        "message {message:?} must name {name}"
                    )
                }
                other => panic!("inverted {name} range must be a validation error, got {other:?}"),
            }
        }
    }

    #[test]
    fn config_rejects_invalid_bounds() {
        // Each invalid value substituted as the lo and as the hi bound of
        // every parameter; the corner construction surfaces the genome's
        // rule for each.
        let ranges = [[0.01f32, 0.08], [0.03, 0.07], [0.16, 0.16], [0.08, 0.08]];
        for (position, range) in ranges.iter().enumerate() {
            for bad in [f32::NAN, -0.0, -0.01] {
                for slot in 0..2 {
                    let mut corrupted = *range;
                    corrupted[slot] = bad;
                    assert!(
                        matches!(
                            with_substituted(position, corrupted),
                            Err(Error::Validation(_))
                        ),
                        "position {position} slot {slot} value {bad} must be rejected"
                    );
                }
            }
        }
        // Zero as a diffusion bound: the genome's strict-positivity rule.
        assert!(matches!(
            with_substituted(2, [0.0, 0.16]),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            with_substituted(3, [0.0, 0.08]),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn config_accepts_degenerate_and_zero_bounds() -> Result<()> {
        // A degenerate range fixes its parameter; every position admits one.
        with_substituted(0, [0.055, 0.055])?;
        with_substituted(1, [0.062, 0.062])?;
        with_substituted(2, [0.16, 0.16])?;
        with_substituted(3, [0.08, 0.08])?;
        // The genome admits +0.0 feed and kill, so zero bounds pass the
        // corner check.
        with_substituted(0, [0.0, 0.0])?;
        with_substituted(1, [0.0, 0.07])?;
        Ok(())
    }
}
