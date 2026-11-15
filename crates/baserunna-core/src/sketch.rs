//! Commutative, associative aggregate summaries: exact float sums,
//! HyperLogLog distinct counts, and DDSketch quantile summaries.

use crate::codec::{DResult, DecodeError, Reader, Writer};
use std::collections::BTreeMap;

/// Computes a deterministic 64-bit hash for persisted aggregate states.
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
/// Exponent of the least significant fixed-point bit, `2^-1074`, which is the
/// smallest positive `f64` subnormal.
const LSB_EXP: i32 = -1074;

/// Fixed-point accumulator that computes the exact sum of finite `f64` values
/// and rounds once on read.
#[derive(Clone, Debug, Default)]
pub struct ExactSum {
    limbs: Vec<i64>,
    adds: u32,
}

impl PartialEq for ExactSum {
    fn eq(&self, o: &ExactSum) -> bool {
        let limbs = |s: &ExactSum| {
            let mut s = s.clone();
            s.normalize();
            if s.limbs.is_empty() { vec![0; LIMBS] } else { s.limbs }
        };
        limbs(self) == limbs(o)
    }
}

impl Eq for ExactSum {}

impl ExactSum {
    pub fn mem_size(&self) -> usize {
        24 + self.limbs.capacity() * 8
    }

    pub fn add(&mut self, x: f64) {
        if x == 0.0 || !x.is_finite() {
            return;
        }
        if self.limbs.is_empty() {
            self.limbs = vec![0; LIMBS];
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
        if self.limbs.is_empty() {
            return;
        }
        let mut overflow = 0i64;
        for l in self.limbs.iter_mut() {
            let v = *l + overflow;
            overflow = v >> LIMB_BITS;
            *l = v & 0xFFFF_FFFF;
        }
        if let Some(top) = self.limbs.last_mut() {
            *top += overflow << LIMB_BITS;
        }
        self.adds = 0;
    }

    pub fn merge(&mut self, o: &ExactSum) {
        if o.limbs.is_empty() {
            return;
        }
        if self.limbs.is_empty() {
            *self = o.clone();
            return;
        }
        let mut o = o.clone();
        o.normalize();
        self.normalize();
        for (a, b) in self.limbs.iter_mut().zip(&o.limbs) {
            *a += *b;
        }
        self.adds = 2;
        self.normalize();
    }

    /// Returns the sum rounded to the nearest `f64`, or infinity on overflow.
    pub fn value(&self) -> f64 {
        self.quotient(1)
    }

    /// Divides the exact sum by positive `n` and rounds once to the nearest
    /// `f64` using round-ties-to-even, returning infinity on overflow.
    pub fn quotient(&self, n: u64) -> f64 {
        debug_assert!(n > 0);
        let mut s = self.clone();
        s.normalize();
        if s.limbs.is_empty() {
            return 0.0;
        }
        let neg = s.limbs[LIMBS - 1] < 0;
        if neg {
            let mut borrow = 0i64;
            for l in s.limbs.iter_mut() {
                let v = -*l - borrow;
                borrow = i64::from(v < 0);
                *l = v.rem_euclid(1 << LIMB_BITS);
            }
        }
        // Long division with a guard limb below 2^-1074 for exact rounding.
        let n = n.max(1) as u128;
        let mut q = [0u32; LIMBS + 1];
        let mut rem = 0u128;
        for i in (0..q.len()).rev() {
            let limb = if i > 0 { s.limbs[i - 1] as u128 } else { 0 };
            let cur = rem << LIMB_BITS | limb;
            q[i] = (cur / n) as u32;
            rem = cur % n;
        }
        let Some(top) = q.iter().rposition(|l| *l != 0) else { return 0.0 };
        let bits = LIMB_BITS as usize;
        let bit = |i: usize| u64::from(q[i / bits] >> (i % bits) & 1);
        let high = top * bits + bits - 1 - q[top].leading_zeros() as usize;
        // Clamp the least significant mantissa bit at 2^-1074 for subnormals.
        let lsb = high.saturating_sub(52).max(bits);
        let mut mant = (lsb..=high).rev().fold(0u64, |m, i| m << 1 | bit(i));
        let below = lsb - 1;
        let sticky =
            rem != 0 || q[..below / bits].iter().any(|l| *l != 0) || q[below / bits] & ((1 << (below % bits)) - 1) != 0;
        if bit(below) == 1 && (sticky || mant & 1 == 1) {
            mant += 1;
        }
        if mant == 0 {
            return 0.0;
        }
        let mag = scale(mant as f64, LSB_EXP + (lsb - bits) as i32);
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
        if n > 0 {
            s.limbs = vec![0; LIMBS];
        }
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

/// Multiplies `x` by `2^e`, rounding at most once at the final step.
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
const HLL_EXACT_MAX: usize = 1024;

/// Distinct-count sketch that stores an exact hash set up to `HLL_EXACT_MAX`
/// elements and transitions to a 14-bit HyperLogLog register array.
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

/// Computes Ertl's improved HyperLogLog cardinality estimate from 2017,
/// Algorithm 6.
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

/// Relative error bound for [`DdSketch`] quantiles.
pub const DD_ALPHA: f64 = 0.01;

/// Quantile sketch using logarithmic histogram buckets with relative error
/// bound [`DD_ALPHA`].
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
        if x == 0.0 {
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

    /// Returns the estimated value at quantile `q` in `[0.0, 1.0]`, or `None`
    /// if the sketch is empty.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let n = self.count();
        if n == 0 {
            return None;
        }
        let rank = (q.clamp(0.0, 1.0) * (n - 1) as f64).floor() as u64;
        let g = gamma();
        // Evaluate bucket value in log space to prevent overflow if gamma^i > f64::MAX.
        let value = |i: i32| (i as f64 * g.ln() + (2.0 / (g + 1.0)).ln()).exp().min(f64::MAX);
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

