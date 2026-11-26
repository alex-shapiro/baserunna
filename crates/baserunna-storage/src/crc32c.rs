//! CRC-32C (Castagnoli) checksums.

pub fn crc32c(data: &[u8]) -> u32 {
    ::crc32c::crc32c(data)
}

/// Extend a running CRC-32C checksum with `data`.
pub fn extend(crc: u32, data: &[u8]) -> u32 {
    ::crc32c::crc32c_append(crc, data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
    }

    #[test]
    fn incremental() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let (a, b) = data.split_at(17);
        assert_eq!(extend(crc32c(a), b), crc32c(data));
    }
}
