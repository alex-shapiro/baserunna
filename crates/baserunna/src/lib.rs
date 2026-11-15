//! Object storage database host with stateless compute nodes.
//! See `SPEC.md` for full design.

#![deny(unsafe_code)]
#![recursion_limit = "512"]

#[cfg(feature = "handlers")]
pub mod api;
#[cfg(feature = "handlers")]
pub mod auth;
pub mod buggify;
pub mod catalog;
#[cfg(feature = "handlers")]
pub mod cluster;
#[cfg(feature = "handlers")]
pub mod console;
#[cfg(feature = "handlers")]
pub mod cpu_slots;
pub mod engine;
pub mod env;
pub mod error;
pub mod events;
pub mod filter;
pub mod limits;
pub mod log;
#[cfg(feature = "handlers")]
pub mod mail;
#[cfg(feature = "handlers")]
pub mod meta;
pub mod metrics;
#[cfg(feature = "handlers")]
pub mod node;
pub mod ops;
pub mod probe;
pub mod queries;
#[cfg(feature = "server")]
pub mod sandbox;
#[cfg(feature = "handlers")]
pub mod guest_router;
#[cfg(feature = "server")]
pub mod sim;
pub mod store;
pub mod stream;
pub mod table;
pub mod timing;
pub mod txns;
pub mod util;

pub use error::{Error, Result};
