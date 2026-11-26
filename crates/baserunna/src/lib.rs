//! Object storage database host with stateless compute nodes.
//! See `SPEC.md` for full design.

#![forbid(unsafe_code)]
#![recursion_limit = "512"]

pub mod api;
pub mod auth;
pub mod cluster;
pub mod console;
pub mod limits;
pub mod mail;
pub mod meta;
pub mod node;
pub mod sim;
