//! Object storage database host with stateless compute nodes.
//! See `SPEC.md` for full design.

#![forbid(unsafe_code)]
#![recursion_limit = "512"]

#[cfg(feature = "server")]
pub mod api;
#[cfg(feature = "server")]
pub mod auth;
pub mod buggify;
pub mod catalog;
#[cfg(feature = "server")]
pub mod cluster;
#[cfg(feature = "server")]
pub mod console;
pub mod engine;
pub mod env;
pub mod error;
pub mod events;
pub mod filter;
pub mod limits;
pub mod log;
#[cfg(feature = "server")]
pub mod mail;
#[cfg(feature = "server")]
pub mod meta;
pub mod metrics;
#[cfg(feature = "server")]
pub mod node;
pub mod ops;
pub mod probe;
pub mod queries;
#[cfg(feature = "server")]
pub mod sandbox;
#[cfg(feature = "server")]
pub mod sim;
pub mod store;
pub mod stream;
pub mod table;
pub mod timing;
pub mod txns;
pub mod util;

pub use error::{Error, Result};
