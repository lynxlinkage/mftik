//! Pure Rust STS core.
//!
//! Algorithms land here and do not do IO. The Python SDK calls this crate
//! through `mftik-py`. The order book is [`book`]. Local sync is [`bookpath`].
//! The session dispatcher is [`dispatch`]. The TD path is [`tdpath`].
//! The MD path is [`mdpath`].
//! Client order ids are [`cloid`] and [`cloidgen`]. Atomic volume replace is
//! [`volume`]. The worker end of C4 is [`c4`]. Instrument rows are
//! [`symbol`], and the session SYM client is [`symclient`]. The frames the
//! dispatcher writes to the strategy event log are [`eventlog`].

pub mod c4;
pub mod dispatch;
pub mod eventlog;
pub mod mdpath;
pub mod tdpath;

pub mod book;
pub mod bookpath;
pub mod cloid;
pub mod cloidgen;
pub mod symbol;
pub mod symclient;
pub mod volume;

/// Crate name, so other workspace crates can link this shell.
pub const CRATE_NAME: &str = env!("CARGO_PKG_NAME");

#[allow(dead_code)]
fn proto_is_linked() -> &'static str {
    mftik_proto::CRATE_NAME
}
