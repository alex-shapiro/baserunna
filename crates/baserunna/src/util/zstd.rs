//! Zstd compression with thread-local contexts.

use std::cell::RefCell;
use std::io;
use zstd::bulk::{Compressor, Decompressor};

thread_local! {
    static COMPRESSORS: RefCell<Vec<(i32, Compressor<'static>)>> = const { RefCell::new(Vec::new()) };
    static DECOMPRESSOR: RefCell<Option<Decompressor<'static>>> = const { RefCell::new(None) };
}

pub fn compress(data: &[u8], level: i32) -> io::Result<Vec<u8>> {
    COMPRESSORS.with(|cs| {
        let mut cs = cs.borrow_mut();
        let i = match cs.iter().position(|(l, _)| *l == level) {
            Some(i) => i,
            None => {
                cs.push((level, Compressor::new(level)?));
                cs.len() - 1
            }
        };
        cs[i].1.compress(data)
    })
}

/// Decompresses `data`, which must decompress to at most `capacity` bytes.
pub fn decompress(data: &[u8], capacity: usize) -> io::Result<Vec<u8>> {
    DECOMPRESSOR.with(|d| {
        let mut d = d.borrow_mut();
        if d.is_none() {
            *d = Some(Decompressor::new()?);
        }
        d.as_mut().map_or_else(|| Err(io::Error::other("no decompressor")), |d| d.decompress(data, capacity))
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn roundtrips_across_levels_and_reuse() {
        let data: Vec<u8> = (0..20_000u32).flat_map(|i| (i % 97).to_le_bytes()).collect();
        for level in [1, 3, 1, 3] {
            let c = super::compress(&data, level).unwrap();
            assert!(c.len() < data.len() / 4);
            assert_eq!(super::decompress(&c, data.len()).unwrap(), data);
        }
        assert!(super::decompress(&[1, 2, 3], 100).is_err());
        let c = super::compress(&data, 1).unwrap();
        assert!(super::decompress(&c, data.len() - 1).is_err(), "output over capacity");
    }
}
