//! Bloom filter.

use super::codec::{DResult, DecodeError, Reader, Writer};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bloom {
    bits: Vec<u8>,
    k: u8,
}

/// Hash `key` for [`Bloom::insert_hash`].
pub fn key_hash(key: &[u8]) -> u64 {
    // FNV-1a followed by a SplitMix finalizer for better bit dispersion.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

impl Bloom {
    /// A filter sized for `n` keys at `bits_per_key`.
    pub fn new(n: usize, bits_per_key: usize) -> Bloom {
        let nbits = (n * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let k = ((bits_per_key as f64) * 0.69).round().clamp(1.0, 30.0) as u8;
        Bloom { bits: vec![0; nbytes], k }
    }

    fn nbits(&self) -> u64 {
        self.bits.len() as u64 * 8
    }

    pub fn insert(&mut self, key: &[u8]) {
        self.insert_hash(key_hash(key));
    }

    /// Insert a precomputed [`key_hash`].
    pub fn insert_hash(&mut self, h: u64) {
        let (mut a, b) = (h, h.rotate_left(32) | 1);
        let nbits = self.nbits();
        for _ in 0..self.k {
            let bit = a % nbits;
            self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
            a = a.wrapping_add(b);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() {
            return true;
        }
        let h = key_hash(key);
        let (mut a, b) = (h, h.rotate_left(32) | 1);
        let nbits = self.nbits();
        for _ in 0..self.k {
            let bit = a % nbits;
            if self.bits[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
                return false;
            }
            a = a.wrapping_add(b);
        }
        true
    }

    pub fn encode(&self, w: &mut Writer) {
        w.u8(self.k);
        w.bytes(&self.bits);
    }

    pub fn decode(r: &mut Reader<'_>) -> DResult<Bloom> {
        let k = r.u8()?;
        if k == 0 || k > 30 {
            return Err(DecodeError("bad bloom k"));
        }
        let bits = r.bytes()?.to_vec();
        if bits.is_empty() {
            return Err(DecodeError("empty bloom"));
        }
        Ok(Bloom { bits, k })
    }

    pub fn size_bytes(&self) -> usize {
        self.bits.len() + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_low_fp() {
        let n = 5000;
        let mut b = Bloom::new(n, 10);
        for i in 0..n {
            b.insert(format!("key{i}").as_bytes());
        }
        for i in 0..n {
            assert!(b.may_contain(format!("key{i}").as_bytes()));
        }
        let fp = (0..n).filter(|i| b.may_contain(format!("other{i}").as_bytes())).count();
        assert!(fp < n / 30, "false positive rate too high: {fp}");
    }

    #[test]
    fn roundtrip() {
        let mut b = Bloom::new(10, 10);
        b.insert(b"a");
        let mut w = Writer::new();
        b.encode(&mut w);
        let buf = w.into_inner();
        let d = Bloom::decode(&mut Reader::new(&buf)).unwrap();
        assert_eq!(d, b);
        assert!(d.may_contain(b"a"));
    }
}
