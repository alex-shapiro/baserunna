//! Tenant compute code that runs both in the node and in its WebAssembly sandbox.
//!
//! Code here does no I/O, so it builds for the host and for
//! `wasm32-unknown-unknown`.

pub mod bloom;
pub mod codec;
pub mod crc32c;
pub mod json;
pub mod sketch;
pub mod vector;
pub mod zstd;

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher};

/// `HashMap` with a deterministic hasher.
pub type DetHashMap<K, V> = HashMap<K, V, BuildHasherDefault<DefaultHasher>>;
