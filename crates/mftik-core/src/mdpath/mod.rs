//! Session path to market data (PROTOCOL §7.1, §9.3.2, §9.4.2–§9.4.4,
//! E08-D046–E08-D049).
//!
//! This is a pure state machine (E03-D001). It turns bindings snapshots into
//! subscription changes, routes `md.f` bytes, decodes them for the hooks, and
//! builds `md.view` / `md.fetch` requests. NATS stays in E03. Nothing here
//! builds an `md.f` subject: every subscription is `FeedBinding.subject` as
//! given (E01-D006).
//!
//! # Bindings
//!
//! `StartPayload.bindings`, `Command.bindings`, and `Live` share one snapshot
//! format (PROTOCOL §9.3.5, E01-D011). Subscribes happen before the set
//! changes, and unsubscribes after, like the Go SDK:
//!
//! ```text
//! match path.apply(&bindings)? {           // Err: no Applied for Live
//!     Apply::Unchanged => {}               // success, Applied for Live
//!     Apply::Staged(subscribes) => {
//!         if subscribe_all(&subscribes).and_then(|_| flush()).is_err() {
//!             unsubscribe(path.apply_aborted());
//!             return Err(..);
//!         }
//!         unsubscribe(path.apply_bound()?); // bindings_applied changed
//!         send_report();
//!     }
//! }
//! ```
//!
//! # Live messages
//!
//! The IO thread asks [`Path::route`] where a message goes and pushes it on
//! the dispatcher without decoding the payload. The dispatcher's envelope
//! check and stale prehook then run, and its feed callback calls
//! [`Hooks::decode`] with the same bytes. `orderbook_delta` goes through
//! [`crate::dispatch::Dispatcher::push_book`] to [`crate::bookpath`]. This path
//! does not ask for tape and does not hold live messages (E08-D047).
//!
//! ```text
//! match path.route(sub, bytes) {
//!     Route::Md => dispatcher.push_md(bytes),
//!     Route::Book => dispatcher.push_book(bytes),
//!     Route::Skip => Ok(()),
//! }
//! // dispatcher feed callback
//! match hooks.decode(bytes) {
//!     Decoded::Hook(event) => strategy(event),
//!     Decoded::Skip => {}
//!     Decoded::Log(line) => log(line),
//! }
//! ```
//!
//! # Reads
//!
//! [`Path::tape`], [`Path::book_snapshot`], [`Path::fetch_klines`], and
//! [`Path::fetch_open_interest`] return a [`Request`] that waits
//! `recv_window`. The ticker and the venue come from SYM `Info`, passed in by
//! the caller. Pass how the request ended to [`Path::on_reply`] with its
//! ticket. The [`Delivery`] goes on the TD buffer, timeouts and missing
//! responders included (E08-D048).

mod event;
mod path;

#[cfg(test)]
mod tests;

pub use event::{Data, Decoded, Event, Hooks, Md, TOPIC_BOOK, Topic};
pub use path::{
    Apply, Config, Delivery, Effect, Error, OpenError, Path, ReadError, ReadKind, ReadTicket,
    Reply, Request, RequestError, Route, SubId, TapeQuery, Via, fetch_subject, view_subject,
};
