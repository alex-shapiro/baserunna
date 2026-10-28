//! Mergeable summaries for aggregates whose results must not depend on how
//! rows are split across parts, merges and seals: an exact float sum, a
//! HyperLogLog distinct count and a DDSketch quantile summary. Each merge
//! is commutative and associative, so a query's answer depends only on the
//! rows it aggregated.

use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use std::collections::BTreeMap;

/// A 64-bit hash with good dispersion, stable across releases because
/// aggregate states may be persisted.
pub fn hash64(data: &[u8]) -> u64 {
    let mut h: u64 = 0x9E37_79B9_7F4A_7C15 ^ (data.len() as u64).wrapping_mul(0xA076_1D64_78BD_642F);
    for c in data.chunks(8) {
        let mut w = [0u8; 8];
        w[..c.len()].copy_from_slice(c);
        h = mix(h ^ mix(u64::from_le_bytes(w)));
    }
    mix(h)
}

fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

const LIMB_BITS: u32 = 32;
const LIMBS: usize = 70;
/// The fixed point's least significant bit is 2^-1074, the smallest subnormal.
const LSB_EXP: i32 = -1074;

/// The exact sum of finite floats: a fixed-point accumulator wide enough
/// for any finite double, rounded once when read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactSum {
    limbs: Vec<i64>,
    adds: u32,
}

impl Default for ExactSum {
    fn default() -> Self {
        ExactSum { limbs: vec![0; LIMBS], adds: 0 }
    }
}

impl ExactSum {
    pub fn add(&mut self, x: f64) {
        if x == 0.0 || !x.is_finite() {
            return;
        }
        let bits = x.to_bits();
        let exp = ((bits >> 52) & 0x7FF) as i32;
        let frac = bits & ((1u64 << 52) - 1);
        let (mant, e) = if exp == 0 { (frac, 1) } else { (frac | (1 << 52), exp) };
        let shift = (e - 1075 - LSB_EXP) as u32;
        let neg = bits >> 63 == 1;
        let (limb, off) = ((shift / LIMB_BITS) as usize, shift % LIMB_BITS);
        let wide = (mant as u128) << off;
        for i in 0..3 {
            let part = ((wide >> (LIMB_BITS * i)) & 0xFFFF_FFFF) as i64;
            if part != 0 && limb + (i as usize) < LIMBS {
                self.limbs[limb + i as usize] += if neg { -part } else { part };
            }
        }
        self.adds += 1;
        if self.adds >= 1 << 30 {
            self.normalize();
        }
    }

    fn normalize(&mut self) {
        let mut carry = 0i64;
        for l in self.limbs.iter_mut() {
            let v = *l + carry;
            carry = v >> LIMB_BITS;
            *l = v & 0xFFFF_FFFF;
        }
        if let Some(top) = self.limbs.last_mut() {
            *top += carry << LIMB_BITS;
        }
        self.adds = 0;
    }

    pub fn merge(&mut self, o: &ExactSum) {
        let mut o = o.clone();
        o.normalize();
        self.normalize();
        for (a, b) in self.limbs.iter_mut().zip(&o.limbs) {
            *a += *b;
        }
        self.adds = 2;
        self.normalize();
    }

    /// The sum rounded to the nearest double, or an infinity past the range.
    pub fn value(&self) -> f64 {
        let mut s = self.clone();
        s.normalize();
        let neg = s.limbs[LIMBS - 1] < 0;
        if neg {
            let mut borrow = 0i64;
            for l in s.limbs.iter_mut() {
                let v = -*l - borrow;
                borrow = i64::from(v < 0);
                *l = v.rem_euclid(1 << LIMB_BITS);
            }
        }
        let Some(top) = s.limbs.iter().rposition(|l| *l != 0) else { return 0.0 };
        let lo = top.saturating_sub(2);
        let mut acc: u128 = 0;
        for i in (lo..=top).rev() {
            acc = (acc << LIMB_BITS) | s.limbs[i] as u128;
        }
        if s.limbs[..lo].iter().any(|l| *l != 0) {
            acc |= 1;
        }
        let exp = LSB_EXP + (lo as i32) * LIMB_BITS as i32;
        let mag = scale(acc as f64, exp);
        if neg { -mag } else { mag }
    }

    pub fn encode(&self, w: &mut Writer) {
        let mut s = self.clone();
        s.normalize();
        let used: Vec<(usize, i64)> = s.limbs.iter().copied().enumerate().filter(|(_, l)| *l != 0).collect();
        w.varint(used.len() as u64);
        for (i, l) in used {
            w.u8(i as u8);
            w.u64(l as u64);
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> DResult<ExactSum> {
        let mut s = ExactSum::default();
        let n = r.len(LIMBS)?;
        for _ in 0..n {
            let i = r.u8()? as usize;
            if i >= LIMBS {
                return Err(DecodeError("bad sum limb"));
            }
            s.limbs[i] = r.u64()? as i64;
        }
        Ok(s)
    }
}

/// `x * 2^e`, rounding at most once: every step but the last is exact
/// unless the result overflows.
fn scale(mut x: f64, mut e: i32) -> f64 {
    let pow2 = |s: i32| f64::from_bits(((s + 1023) as u64) << 52);
    while e != 0 {
        let s = e.clamp(-1000, 1000);
        x *= pow2(s);
        e -= s;
    }
    x
}

const HLL_P: u32 = 14;
const HLL_M: usize = 1 << HLL_P;
/// Distinct hashes kept exactly before switching to registers.
const HLL_EXACT_MAX: usize = 1024;

/// An approximate distinct count after ClickHouse's `uniqCombined`: an exact
/// set of hashes while small, then a HyperLogLog sketch read with Ertl's
/// improved estimator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Hll {
    Exact(std::collections::BTreeSet<u64>),
    Registers(Vec<u8>),
}

impl Default for Hll {
    fn default() -> Self {
        Hll::Exact(Default::default())
    }
}

impl Hll {
    pub fn add_hash(&mut self, h: u64) {
        match self {
            Hll::Exact(s) => {
                s.insert(h);
                if s.len() > HLL_EXACT_MAX {
                    let mut regs = vec![0u8; HLL_M];
                    for h in s.iter() {
                        set_register(&mut regs, *h);
                    }
                    *self = Hll::Registers(regs);
                }
            }
            Hll::Registers(r) => set_register(r, h),
        }
    }

    pub fn merge(&mut self, o: &Hll) {
        match o {
            Hll::Exact(s) => s.iter().for_each(|h| self.add_hash(*h)),
            Hll::Registers(or) => {
                if let Hll::Exact(s) = self {
                    let mut regs = vec![0u8; HLL_M];
                    for h in s.iter() {
                        set_register(&mut regs, *h);
                    }
                    *self = Hll::Registers(regs);
                }
                if let Hll::Registers(r) = self {
                    for (a, b) in r.iter_mut().zip(or) {
                        *a = (*a).max(*b);
                    }
                }
            }
        }
    }

    pub fn estimate(&self) -> u64 {
        match self {
            Hll::Exact(s) => s.len() as u64,
            Hll::Registers(r) => ertl(r).round() as u64,
        }
    }

    pub fn mem_size(&self) -> usize {
        match self {
            Hll::Exact(s) => s.len() * 24 + 32,
            Hll::Registers(_) => HLL_M + 32,
        }
    }

    pub fn encode(&self, w: &mut Writer) {
        match self {
            Hll::Exact(s) => {
                w.u8(0);
                w.varint(s.len() as u64);
                s.iter().for_each(|h| w.u64(*h));
            }
            Hll::Registers(r) => {
                w.u8(1);
                w.raw(r);
            }
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> DResult<Hll> {
        match r.u8()? {
            0 => {
                let n = r.len(HLL_EXACT_MAX)?;
                let mut s = std::collections::BTreeSet::new();
                for _ in 0..n {
                    s.insert(r.u64()?);
                }
                Ok(Hll::Exact(s))
            }
            1 => {
                let regs = r.take(HLL_M)?.to_vec();
                if regs.iter().any(|x| *x > 65 - HLL_P as u8) {
                    return Err(DecodeError("bad register"));
                }
                Ok(Hll::Registers(regs))
            }
            _ => Err(DecodeError("bad distinct count state")),
        }
    }
}

fn set_register(regs: &mut [u8], h: u64) {
    let idx = (h >> (64 - HLL_P)) as usize;
    let rest = h << HLL_P;
    let rank = (rest.leading_zeros().min(64 - HLL_P) + 1) as u8;
    regs[idx] = regs[idx].max(rank);
}

/// Ertl's improved raw estimator ("New cardinality estimation algorithms
/// for HyperLogLog sketches", 2017, algorithm 6), unbiased without tables.
fn ertl(regs: &[u8]) -> f64 {
    let q = 64 - HLL_P as usize;
    let m = regs.len() as f64;
    let mut c = vec![0f64; q + 2];
    for r in regs {
        c[*r as usize] += 1.0;
    }
    let mut z = m * tau(1.0 - c[q + 1] / m);
    for k in (1..=q).rev() {
        z = 0.5 * (z + c[k]);
    }
    z += m * sigma(c[0] / m);
    let alpha = 0.5 / std::f64::consts::LN_2;
    alpha * m * m / z
}

fn sigma(x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let (mut x, mut y, mut z) = (x, 1.0, x);
    loop {
        x *= x;
        let prev = z;
        z += x * y;
        y += y;
        if z == prev {
            return z;
        }
    }
}

fn tau(x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let (mut x, mut y, mut z) = (x, 1.0, 1.0 - x);
    loop {
        x = x.sqrt();
        let prev = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z == prev {
            return z / 3.0;
        }
    }
}

/// Relative accuracy of quantiles.
pub const DD_ALPHA: f64 = 0.01;

/// A DDSketch: counts of values in logarithmic buckets, so every quantile
/// is within [`DD_ALPHA`] of a value at that rank and merging adds counts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DdSketch {
    pos: BTreeMap<i32, u64>,
    neg: BTreeMap<i32, u64>,
    zeros: u64,
}

fn gamma() -> f64 {
    (1.0 + DD_ALPHA) / (1.0 - DD_ALPHA)
}

impl DdSketch {
    pub fn add(&mut self, x: f64) {
        if !x.is_finite() {
            return;
        }
        if x.abs() < 1e-300 {
            self.zeros += 1;
            return;
        }
        let i = (x.abs().ln() / gamma().ln()).ceil() as i32;
        *(if x > 0.0 { &mut self.pos } else { &mut self.neg }).entry(i).or_insert(0) += 1;
    }

    pub fn count(&self) -> u64 {
        self.zeros + self.pos.values().sum::<u64>() + self.neg.values().sum::<u64>()
    }

    pub fn merge(&mut self, o: &DdSketch) {
        for (i, n) in &o.pos {
            *self.pos.entry(*i).or_insert(0) += n;
        }
        for (i, n) in &o.neg {
            *self.neg.entry(*i).or_insert(0) += n;
        }
        self.zeros += o.zeros;
    }

    /// The value at quantile `q` in `[0, 1]`, or `None` if empty.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let n = self.count();
        if n == 0 {
            return None;
        }
        let rank = (q.clamp(0.0, 1.0) * (n - 1) as f64).floor() as u64;
        let g = gamma();
        let value = |i: i32| 2.0 * g.powi(i) / (g + 1.0);
        let mut seen = 0u64;
        for (i, c) in self.neg.iter().rev() {
            seen += c;
            if seen > rank {
                return Some(-value(*i));
            }
        }
        seen += self.zeros;
        if seen > rank {
            return Some(0.0);
        }
        for (i, c) in &self.pos {
            seen += c;
            if seen > rank {
                return Some(value(*i));
            }
        }
        None
    }

    pub fn mem_size(&self) -> usize {
        (self.pos.len() + self.neg.len()) * 32 + 48
    }

    pub fn encode(&self, w: &mut Writer) {
        w.varint(self.zeros);
        for m in [&self.pos, &self.neg] {
            w.varint(m.len() as u64);
            for (i, n) in m {
                w.varint(((i << 1) ^ (i >> 31)) as u32 as u64);
                w.varint(*n);
            }
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> DResult<DdSketch> {
        let mut s = DdSketch { zeros: r.varint()?, ..Default::default() };
        for neg in [false, true] {
            let n = r.len(r.remaining())?;
            for _ in 0..n {
                let z = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad bucket"))?;
                let i = ((z >> 1) as i32) ^ -((z & 1) as i32);
                let c = r.varint()?;
                if neg {
                    s.neg.insert(i, c)
                } else {
                    s.pos.insert(i, c)
                };
            }
        }
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::rng::Rng;

    fn exact(xs: &[f64]) -> f64 {
        let mut s = ExactSum::default();
        xs.iter().for_each(|x| s.add(*x));
        s.value()
    }

    #[test]
    fn exact_sums_round_once() {
        assert_eq!(exact(&[]), 0.0);
        assert_eq!(exact(&[0.1, 0.2]), 0.30000000000000004);
        assert_eq!(exact(&[1e100, 1.0, -1e100]), 1.0);
        assert_eq!(exact(&[f64::MAX, f64::MAX]), f64::INFINITY);
        assert_eq!(exact(&[-f64::MAX, -f64::MAX]), f64::NEG_INFINITY);
        assert_eq!(exact(&[5e-324, 5e-324]), 1e-323);
        assert_eq!(exact(&[-3.5, 1.25]), -2.25);
        assert_eq!(exact(&[1.0, 2f64.powi(-60)]), 1.0, "ties and stickies round to nearest");
        assert_eq!(exact(&[1.0, 2f64.powi(-53), 2f64.powi(-100)]), 1.0000000000000002);
        assert_eq!(exact(&[f64::MAX, -f64::MAX, 3.0]), 3.0);
    }

    #[test]
    fn exact_sums_ignore_order_and_grouping() {
        let mut r = Rng::new(4);
        for _ in 0..300 {
            let n = r.usize(40);
            let xs: Vec<f64> = (0..n)
                .map(|_| {
                    (r.unit() - 0.5) * 10f64.powi(r.usize(40) as i32 - 20) * if r.chance(0.1) { 1e200 } else { 1.0 }
                })
                .collect();
            let want = exact(&xs);
            let mut shuffled = xs.clone();
            r.shuffle(&mut shuffled);
            let cut = r.usize(n + 1);
            let (mut a, mut b) = (ExactSum::default(), ExactSum::default());
            shuffled[..cut].iter().for_each(|x| a.add(*x));
            shuffled[cut..].iter().for_each(|x| b.add(*x));
            b.merge(&a);
            assert_eq!(b.value(), want);
            let mut w = Writer::new();
            b.encode(&mut w);
            assert_eq!(ExactSum::decode(&mut Reader::new(&w.buf)).unwrap().value(), want);
            if xs.iter().all(|x| x.abs() < 1e10 && x.abs() > 1e-10) {
                let naive: f64 = xs.iter().sum();
                assert!((naive - want).abs() <= 1e-6 * xs.iter().map(|x| x.abs()).sum::<f64>().max(1e-300));
            }
        }
    }

    #[test]
    fn distinct_counts_are_exact_while_small_and_accurate_after() {
        let mut h = Hll::default();
        for i in 0..1000u64 {
            h.add_hash(hash64(&i.to_le_bytes()));
        }
        assert_eq!(h.estimate(), 1000);
        for n in [5_000u64, 50_000, 300_000] {
            let mut a = Hll::default();
            let mut b = Hll::default();
            for i in 0..n {
                let hh = hash64(&i.to_le_bytes());
                if i % 3 == 0 {
                    a.add_hash(hh)
                } else {
                    b.add_hash(hh)
                };
            }
            a.merge(&b);
            let e = a.estimate() as f64;
            assert!((e - n as f64).abs() / (n as f64) < 0.03, "{n}: estimated {e}");
            let mut w = Writer::new();
            a.encode(&mut w);
            assert_eq!(Hll::decode(&mut Reader::new(&w.buf)).unwrap(), a);
        }
    }

    #[test]
    fn distinct_count_merges_ignore_order() {
        let mut r = Rng::new(8);
        for _ in 0..30 {
            let hashes: Vec<u64> = (0..r.usize(3000)).map(|_| hash64(&(r.below(2500)).to_le_bytes())).collect();
            let mut whole = Hll::default();
            hashes.iter().for_each(|h| whole.add_hash(*h));
            let mut parts: Vec<Hll> = vec![Hll::default(); 1 + r.usize(5)];
            for h in &hashes {
                let i = r.usize(parts.len());
                parts[i].add_hash(*h);
            }
            r.shuffle(&mut parts);
            let mut acc = Hll::default();
            parts.iter().for_each(|p| acc.merge(p));
            assert_eq!(acc.estimate(), whole.estimate());
        }
    }

    #[test]
    fn quantiles_are_within_relative_accuracy() {
        let mut r = Rng::new(2);
        let mut xs: Vec<f64> = (0..20_000).map(|_| (r.unit() - 0.2) * 1000.0f64).collect();
        xs.push(0.0);
        let mut a = DdSketch::default();
        let mut b = DdSketch::default();
        for (i, x) in xs.iter().enumerate() {
            if i % 2 == 0 {
                a.add(*x)
            } else {
                b.add(*x)
            };
        }
        a.merge(&b);
        xs.sort_by(f64::total_cmp);
        for q in [0.0f64, 0.01, 0.25, 0.5, 0.9, 0.99, 1.0] {
            let want = xs[(q * (xs.len() - 1) as f64).floor() as usize];
            let got = a.quantile(q).unwrap();
            assert!((got - want).abs() <= DD_ALPHA * want.abs() + 1e-9, "q{q}: {got} vs {want}");
        }
        let mut w = Writer::new();
        a.encode(&mut w);
        assert_eq!(DdSketch::decode(&mut Reader::new(&w.buf)).unwrap(), a);
        assert_eq!(DdSketch::default().quantile(0.5), None);
    }
}
#[cfg(test)]
mod probe_tests {
    use super::*;
    #[test]
    fn quarter_sums_are_exact() {
        let mut r = crate::util::rng::Rng::new(1);
        for _ in 0..10000 {
            let xs: Vec<f64> = (0..r.usize(30)).map(|_| (r.below(400) as f64 - 100.0) / 4.0).collect();
            let mut s = ExactSum::default();
            xs.iter().for_each(|x| s.add(*x));
            let naive: f64 = xs.iter().sum();
            assert_eq!(s.value(), naive, "{xs:?}");
            let cut = r.usize(xs.len() + 1);
            let (mut a, mut b) = (ExactSum::default(), ExactSum::default());
            xs[..cut].iter().for_each(|x| a.add(*x));
            xs[cut..].iter().for_each(|x| b.add(*x));
            a.merge(&b);
            assert_eq!(a.value(), naive, "merged {xs:?} at {cut}");
        }
    }
}
