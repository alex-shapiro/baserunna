//! Baserunna: a horizontally scalable service hosting many small,
//! transactional key-value and table databases on object storage.
//! See `SPEC.md` for the design.

#![forbid(unsafe_code)]
#![recursion_limit = "512"]

pub mod api;
pub mod auth;
pub mod client;
pub mod cluster;
pub mod console;
pub mod engine;
pub mod env;
pub mod error;
pub mod filter;
pub mod limits;
pub mod log;
pub mod mail;
pub mod meta;
pub mod node;
pub mod ops;
pub mod sim;
pub mod store;
pub mod stream;
pub mod table;
pub mod util;

pub use error::{Error, Result};
