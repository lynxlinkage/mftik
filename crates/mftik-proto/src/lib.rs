//! Protobuf types for the pinned mftiko contract.
//!
//! `build.rs` compiles `contract/proto/mftiko` with prost-build and the
//! `protoc` binary from `protoc-bin-vendored`. Generated Rust is written to
//! Cargo's `OUT_DIR` and is not committed.

/// Crate name, so other workspace crates can link this shell.
pub const CRATE_NAME: &str = env!("CARGO_PKG_NAME");

/// Generated modules. Layout follows the `.proto` packages.
#[allow(clippy::all)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/mftiko.rs"));
}

#[cfg(test)]
mod roundtrip;
