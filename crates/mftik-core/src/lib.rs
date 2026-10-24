//! Pure Rust STS core.
//!
//! Algorithms land here and do not do IO. The Python SDK calls this crate
//! through `mftik-py`.

pub mod book;

/// Crate name, so other workspace crates can link this shell.
pub const CRATE_NAME: &str = env!("CARGO_PKG_NAME");

#[allow(dead_code)]
fn proto_is_linked() -> &'static str {
    mftik_proto::CRATE_NAME
}
