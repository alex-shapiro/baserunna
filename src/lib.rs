//! Object storage database host with stateless compute nodes.
//! See `SPEC.md` for full design.

#![forbid(unsafe_code)]
#![recursion_limit = "512"]

pub mod api;
pub mod auth;
pub mod buggify;
pub mod client;
pub mod cluster;
pub mod console;
pub mod engine;
pub mod env;
pub mod error;
pub mod events;
pub mod filter;
pub mod limits;
pub mod log;
pub mod mail;
pub mod meta;
pub mod metrics;
pub mod node;
pub mod ops;
pub mod probe;
pub mod sim;
pub mod store;
pub mod stream;
pub mod table;
pub mod timing;
pub mod txns;
pub mod util;

pub use error::{Error, Result};
