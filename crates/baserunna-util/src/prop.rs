//! Property-testing harness. Set `PROP_SEED` to reproduce a failure and
//! `PROP_CASES` to override the case count.

use super::rng::Rng;
use std::fmt::Debug;

pub fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

fn name_seed(name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

/// Run `prop` on `cases` generated inputs, shrinking any failure with `shrink`.
pub fn check<T, G, S, P>(name: &str, cases: usize, generate: G, shrink: S, prop: P)
where
    T: Debug + Clone,
    G: Fn(&mut Rng) -> T,
    S: Fn(&T) -> Vec<T>,
    P: Fn(&T) -> Result<(), String>,
{
    let cases = env_u64("PROP_CASES").map(|c| c as usize).unwrap_or(cases);
    let base = env_u64("PROP_SEED").unwrap_or_else(|| name_seed(name));
    for i in 0..cases {
        let seed = base.wrapping_add(i as u64);
        let mut rng = Rng::new(seed);
        let input = generate(&mut rng);
        if let Err(e) = run_catching(&prop, &input) {
            let (min, err) = minimize(input, e, &shrink, &prop);
            panic!("property '{name}' failed (PROP_SEED={seed} PROP_CASES=1)\nerror: {err}\nminimal input: {min:#?}");
        }
    }
}

fn run_catching<T, P>(prop: &P, input: &T) -> Result<(), String>
where
    P: Fn(&T) -> Result<(), String>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| prop(input))) {
        Ok(r) => r,
        Err(p) => Err(p
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "panic".into())),
    }
}

fn minimize<T, S, P>(mut cur: T, mut err: String, shrink: &S, prop: &P) -> (T, String)
where
    T: Clone,
    S: Fn(&T) -> Vec<T>,
    P: Fn(&T) -> Result<(), String>,
{
    let mut budget = 2000;
    'outer: while budget > 0 {
        for cand in shrink(&cur) {
            budget -= 1;
            if let Err(e) = run_catching(prop, &cand) {
                cur = cand;
                err = e;
                continue 'outer;
            }
            if budget == 0 {
                break;
            }
        }
        break;
    }
    (cur, err)
}

/// Shrink a slice by removing progressively smaller chunks.
pub fn shrink_vec<T: Clone>(v: &[T]) -> Vec<Vec<T>> {
    let mut out = Vec::new();
    let n = v.len();
    if n == 0 {
        return out;
    }
    let mut chunk = n / 2;
    while chunk >= 1 {
        let mut start = 0;
        while start < n {
            let end = (start + chunk).min(n);
            let mut c = Vec::with_capacity(n - (end - start));
            c.extend_from_slice(&v[..start]);
            c.extend_from_slice(&v[end..]);
            out.push(c);
            start = end;
        }
        if chunk == 1 {
            break;
        }
        chunk /= 2;
    }
    out
}

pub fn no_shrink<T>(_: &T) -> Vec<T> {
    Vec::new()
}

/// Random bytes biased toward small alphabets so that collisions and shared
/// prefixes are common.
pub fn small_key(rng: &mut Rng, max_len: usize) -> Vec<u8> {
    let n = 1 + rng.usize(max_len.max(1));
    (0..n)
        .map(|_| match rng.below(10) {
            0 => rng.next_u32() as u8,
            1 => 0,
            2 => 0xFF,
            _ => b'a' + rng.below(4) as u8,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shrinks_to_minimal() {
        let r = std::panic::catch_unwind(|| {
            check(
                "contains-7",
                200,
                |r| (0..r.usize(50)).map(|_| r.below(10)).collect::<Vec<_>>(),
                |v| shrink_vec(v),
                |v| {
                    if v.contains(&7) { Err("has 7".into()) } else { Ok(()) }
                },
            )
        });
        let msg = match r {
            Err(p) => p.downcast_ref::<String>().cloned().unwrap(),
            Ok(_) => panic!("expected failure"),
        };
        assert!(msg.contains("minimal input: [\n    7,\n]"), "{msg}");
    }
}
