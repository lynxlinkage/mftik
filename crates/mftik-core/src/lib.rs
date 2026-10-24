//! Pure Rust STS core.
//!
//! Algorithms land here and do not do IO. The Python SDK calls this crate
//! through `mftik-py`.

pub mod cloid;
pub mod cloidgen;
pub mod volume;

/// Crate name, so other workspace crates can link this shell.
pub const CRATE_NAME: &str = env!("CARGO_PKG_NAME");
