//! Local order book sync (PROTOCOL §9.5, E08-D057–E08-D061).
//!
//! One instrument, one ring from [`crate::book`]. A snapshot is `md.view`'s
//! current window. This module does not ask the exchange and does not call
//! SYM. NATS and timers stay in E03 (E02-D001).
//!
//! # Driving it
//!
//! The host injects `now_ns` on every call. The machine returns the actions
//! to perform. `view` during a [`Effect::Hook`] already contains that delta.
//! Do not feed another book event until the hook has been handed to the
//! strategy.
//!
//! ```text
//! let effects = path.on_message(now_ns, bytes);
//! for effect in effects {
//!     match effect {
//!         Effect::Hook(update) => strategy(update, path.view(update.uid)),
//!         Effect::RequestSnapshot { uid, generation, wait } => {
//!             let reply = md_view(uid, wait);
//!             handle(path.on_snapshot(now_ns, uid, generation, reply));
//!         }
//!         Effect::ArmTimer { token, deadline_ns, .. } => arm(token, deadline_ns),
//!         Effect::Log(line) => log(line),
//!     }
//! }
//! ```
//!
//! [`crate::dispatch::Dispatcher::push_book`] already skips the stale prehook.
//! Pass those bytes to [`Path::on_message`]. Every decoded delta is applied.
//! `recv_window` only decides whether it becomes a hook. A bad envelope is
//! rejected by the dispatcher before it gets here; [`Path::on_message`] still
//! reports one if the host calls it directly.
//!
//! `generation` on a snapshot is a staleness token, not a unique request id.
//! One snapshot is in flight per instrument. A retry after a failed read
//! reuses the token. Pass the reply to the instrument that was asked.
//! Each [`Effect::ArmTimer`] fires at most once; pass the token back unchanged.

mod path;

#[cfg(test)]
mod tests;

pub use path::{
    BACKOFF_MAX, BACKOFF_MIN, BUFFER_AGE, BUFFER_MAX, Config, Delta, Effect, OpenError, Path,
    Snapshot, SnapshotReply, TimerKind, TimerToken, Update,
};
