#[cfg(test)]
mod tests {
    use baserunna_core::codec::*;
    use crate::util::rng::Rng;

    #[test]
    fn varint_roundtrip() {
        let mut r = Rng::new(1);
        let mut vals = vec![0u64, 1, 127, 128, 255, 16383, 16384, u64::MAX, u64::MAX - 1];
        for _ in 0..2000 {
            let bits = r.below(64) as u32;
            vals.push(r.next_u64() >> bits);
        }
        let mut w = Writer::new();
        for v in &vals {
            w.varint(*v);
        }
        let buf = w.into_inner();
        let mut rd = Reader::new(&buf);
        for v in &vals {
            assert_eq!(rd.varint().unwrap(), *v);
        }
        assert!(rd.is_empty());
    }

    #[test]
    fn varint_rejects_overflow() {
        let bad = [0xFFu8; 11];
        assert!(Reader::new(&bad).varint().is_err());
        let bad2 = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02];
        assert!(Reader::new(&bad2).varint().is_err());
    }

    #[test]
    fn reader_never_panics_on_garbage() {
        let mut r = Rng::new(3);
        for _ in 0..5000 {
            let n = r.usize(20);
            let data = r.bytes(n);
            let mut rd = Reader::new(&data);
            for _ in 0..8 {
                let _ = match r.below(6) {
                    0 => rd.varint().map(|_| ()),
                    1 => rd.bytes().map(|_| ()),
                    2 => rd.u64().map(|_| ()),
                    3 => rd.str().map(|_| ()),
                    4 => rd.bool().map(|_| ()),
                    _ => rd.u128().map(|_| ()),
                };
            }
        }
    }
}
