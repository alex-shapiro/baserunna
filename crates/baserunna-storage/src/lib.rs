//! Storage engine and database operations for Baserunna.
//!
//! This crate implements key-value transactions, tables, streams, and event tables on object
//! storage. See `SPEC.md`.

#![forbid(unsafe_code)]
#![recursion_limit = "512"]

pub mod bloom;
pub mod catalog;
pub mod crc32c;
pub mod engine;
pub mod events;
pub mod filter;
pub mod metrics;
pub mod ops;
pub mod queries;
pub mod store;
pub mod stream;
pub mod table;
pub mod timing;
pub mod txns;
pub mod zstd;
