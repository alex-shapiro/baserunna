#[cfg(test)]
mod tests {
    use baserunna_core::sketch::*;
    use baserunna_core::codec::{Reader, Writer};
    use crate::util::rng::Rng;

    fn exact(xs: &[f64]) -> f64 {
        let mut s = ExactSum::default();
        xs.iter().for_each(|x| s.add(*x));
        s.value()
    }

    #[test]
    fn exact_sum_single_rounding() {
        assert_eq!(exact(&[]), 0.0);
        assert_eq!(exact(&[0.1, 0.2]), 0.30000000000000004);
        assert_eq!(exact(&[1e100, 1.0, -1e100]), 1.0);
        assert_eq!(exact(&[f64::MAX, f64::MAX]), f64::INFINITY);
        assert_eq!(exact(&[-f64::MAX, -f64::MAX]), f64::NEG_INFINITY);
        assert_eq!(exact(&[5e-324, 5e-324]), 1e-323);
        assert_eq!(exact(&[-3.5, 1.25]), -2.25);
        assert_eq!(exact(&[1.0, 2f64.powi(-60)]), 1.0, "ties and stickies are rounded to nearest");
        assert_eq!(exact(&[1.0, 2f64.powi(-53), 2f64.powi(-100)]), 1.0000000000000002);
        assert_eq!(exact(&[f64::MAX, -f64::MAX, 3.0]), 3.0);
    }

    #[test]
    fn quotient_single_rounding() {
        let q = |xs: &[f64], n: u64| {
            let mut s = ExactSum::default();
            xs.iter().for_each(|x| s.add(*x));
            s.quotient(n)
        };
        assert_eq!(q(&[f64::MAX, f64::MAX], 2), f64::MAX, "a sum beyond f64 is divided before rounding");
        assert_eq!(q(&[-f64::MAX, -f64::MAX, -f64::MAX], 3), -f64::MAX);
        assert_eq!(q(&[1.0, 2.0], 3), 1.0);
        assert_eq!(q(&[1.0], 3), 1.0 / 3.0);
        let tiny = 5e-324;
        assert_eq!(q(&[2f64.powi(33) * tiny], 1 << 34), 0.0, "ties to even");
        assert_eq!(q(&[(2f64.powi(33) + 2.0) * tiny], (1 << 34) + 2), tiny, "just over half a unit is rounded up");
    }

    #[test]
    fn exact_sum_order_and_merge_invariance() {
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
    fn hll_exact_and_register_accuracy() {
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
    fn hll_merge_order_invariance() {
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
    fn ddsketch_relative_accuracy() {
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
    use baserunna_core::sketch::*;
    #[test]
    fn dyadic_quarter_sums_are_exact() {
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
