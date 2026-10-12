//! Write, space and read amplification of the storage engine on realistic
//! workloads (SPEC §5.6), with production sizes, compressed and not.
//!
//! ```text
//! cargo run --release --example amplification [scale]
//! ```

use baserunna::engine::EngineConfig;
use baserunna::engine::segment::Compression;
use baserunna::sim::amp::{measure_all, table};

fn main() {
    let scale: f64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(1.0);
    let prod = EngineConfig::default();
    println!("Production configuration ({:?}), scale {scale}:\n", prod.compression);
    println!("{}", table(&measure_all(scale, &prod)));
    let plain = EngineConfig { compression: Compression::None, ..EngineConfig::default() };
    println!("Without compression (compaction policy alone):\n");
    println!("{}", table(&measure_all(scale, &plain)));
}
