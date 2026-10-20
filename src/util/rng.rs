//! Deterministic xoshiro256** PRNG seeded via SplitMix64.

use std::cell::RefCell;
use tokio::time::Instant;

thread_local! {
    static BRANCHES: RefCell<Vec<(Instant, u64)>> = const { RefCell::new(Vec::new()) };
}

/// Configures virtual-time branch points `(instant, salt)` on the current thread,
/// sorted by instant. Pass an empty vector to clear branch points.
pub fn branch(points: Vec<(Instant, u64)>) {
    BRANCHES.with(|b| *b.borrow_mut() = points);
}

fn branch_salt() -> u64 {
    BRANCHES.with(|b| {
        let b = b.borrow();
        if b.is_empty() {
            return 0;
        }
        let now = Instant::now();
        b.iter().take_while(|(at, _)| *at <= now).fold(0, |acc, (_, salt)| acc ^ salt)
    })
}

#[derive(Clone, Debug)]
pub struct Rng {
    s: [u64; 4],
    branched: bool,
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Rng {
    pub fn new(seed: u64) -> Rng {
        let mut st = seed;
        let s = [splitmix64(&mut st), splitmix64(&mut st), splitmix64(&mut st), splitmix64(&mut st)];
        Rng { s, branched: true }
    }

    /// Creates a generator that ignores thread-local branch points.
    pub fn unbranched(seed: u64) -> Rng {
        Rng { branched: false, ..Rng::new(seed) }
    }

    /// Derive an independent generator.
    pub fn fork(&mut self) -> Rng {
        Rng::new(self.next_u64())
    }

    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        match if self.branched { branch_salt() } else { 0 } {
            0 => result,
            salt => {
                let mut x = result ^ salt;
                splitmix64(&mut x)
            }
        }
    }

    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// Uniform in `[0, n)`; `n` must be > 0.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        // Lemire's multiply-shift with rejection for exact uniformity.
        loop {
            let x = self.next_u64();
            let m = (x as u128) * (n as u128);
            let lo = m as u64;
            if lo >= n.wrapping_neg() % n {
                return (m >> 64) as u64;
            }
        }
    }

    /// Uniform in `[lo, hi)`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(hi > lo, "empty range");
        lo + self.below(hi - lo)
    }

    pub fn usize(&mut self, n: usize) -> usize {
        self.below(n as u64) as usize
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        self.unit() < p
    }

    /// Selects an index with probability proportional to its weight, or
    /// returns `None` if the total weight is not positive.
    pub fn weighted(&mut self, weights: &[f64]) -> Option<usize> {
        let total: f64 = weights.iter().sum();
        if total <= 0.0 {
            return None;
        }
        let mut x = self.unit() * total;
        for (i, w) in weights.iter().enumerate() {
            if x < *w {
                return Some(i);
            }
            x -= w;
        }
        weights.iter().rposition(|w| *w > 0.0)
    }

    /// Uniform float in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    pub fn fill(&mut self, out: &mut [u8]) {
        for chunk in out.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }

    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        let mut v = vec![0u8; n];
        self.fill(&mut v);
        v
    }

    /// Generates `len` bytes of compressible JSON-like text.
    pub fn text(&mut self, len: usize) -> String {
        const WORDS: [&str; 16] = [
            "alpha", "bravo", "user", "active", "pending", "id", "name", "email", "created", "status", "true", "false",
            "count", "2026", "items", "note",
        ];
        let mut out = format!("{{\"n\":{},\"text\":\"", self.below(1_000_000));
        while out.len() < len {
            out.push_str(self.pick(&WORDS));
            out.push(if self.chance(0.2) { char::from(b'a' + self.below(26) as u8) } else { ' ' });
        }
        out.truncate(len);
        out
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.usize(items.len())]
    }

    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.usize(i + 1);
            items.swap(i, j);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        assert_ne!(Rng::new(1).next_u64(), Rng::new(2).next_u64());
    }

    #[test]
    fn below_in_range_and_covers() {
        let mut r = Rng::new(7);
        let mut seen = [false; 10];
        for _ in 0..10_000 {
            let x = r.below(10);
            assert!(x < 10);
            seen[x as usize] = true;
        }
        assert!(seen.iter().all(|s| *s));
    }

    #[test]
    fn unit_bounds() {
        let mut r = Rng::new(9);
        for _ in 0..10_000 {
            let u = r.unit();
            assert!((0.0..1.0).contains(&u));
        }
    }
}
