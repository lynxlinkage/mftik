//! What a hook may do (Go `sdk.Context`, `sdk/sessctx.go`).
//!
//! A hook gets `&Context`; it lives for that hook. Every method is for the
//! hook that holds it (E08-D003). The one entry from other threads is
//! [`Poster`], which [`Context::poster`] hands out: Go's `Post`. Nothing here
//! blocks except [`Context::resolve`], which only OnInit and OnStart may
//! call.
//!
//! Instruments are UIDs everywhere (E08-D004): resolve the tickers of the
//! strategy's own config once, keep the UIDs, and read current fields with
//! [`Context::info`].
//!
//! Callbacks take the strategy and a Context, `FnOnce(&mut S, &Context<S>)`,
//! where Go's take a Context and reach the strategy through the closure.
//! They run on the dispatcher, one at a time, like any hook.
//!
//! The order calls and reads, and SYM, are wired by E03-07 and E03-08; until
//! then they return [`Error::NotWired`] or find nothing (see `wiring`). The
//! market data reads and the book are the MD path's (E03-06).

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mftik_core::mdpath::{ReadError, TapeQuery};
use mftik_core::symbol::Info;
use mftik_proto::wire::mftiko::md::v1 as md;
use mftik_proto::wire::mftiko::td::v1 as td;

use crate::md::BookView;
use crate::plane::Plane;
use crate::strategy::{Order, Strategy};

/// A mistake in a Context call. TD's answers come as events, not as errors
/// (Go's `sdk.Err*`; the texts match).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// Only OnInit and OnStart may call this.
    NotInInit,
    /// The instrument is not known to this session.
    UnknownUid,
    /// The account is not bound to this session.
    UnknownAccount,
    /// The algo_id is not declared for this account.
    AlgoNotDeclared,
    /// The session has no such feed.
    NoFeed,
    /// The session is stopping.
    Stopping,
    /// The path behind this call is not in the runtime yet: `td` (E03-07)
    /// or `sym` (E03-08).
    NotWired(&'static str),
    /// Any other failure, with its text.
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInInit => f.write_str("sdk: only OnInit and OnStart may call this"),
            Self::UnknownUid => f.write_str("sdk: the instrument is not known to this session"),
            Self::UnknownAccount => f.write_str("sdk: the account is not bound to this session"),
            Self::AlgoNotDeclared => {
                f.write_str("sdk: the algo_id is not declared for this account")
            }
            Self::NoFeed => f.write_str("sdk: the session has no such feed"),
            Self::Stopping => f.write_str("sdk: the session is stopping"),
            Self::NotWired(path) => write!(f, "sdk: the {path} path is not wired yet"),
            Self::Other(text) => f.write_str(text),
        }
    }
}

impl std::error::Error for Error {}

/// A hook's view of the session. See the module docs.
pub struct Context<S> {
    plane: Arc<Plane<S>>,
}

impl<S: Strategy> Context<S> {
    pub(crate) fn new(plane: Arc<Plane<S>>) -> Self {
        Self { plane }
    }

    pub fn session_id(&self) -> &str {
        self.plane.session().map_or("", |s| s.id.as_str())
    }

    /// The strategy's own parameters from the session spec; the platform
    /// does not read them.
    pub fn params(&self) -> &[u8] {
        self.plane.session().map_or(&[], |s| s.params.as_slice())
    }

    // ---------- instruments ----------

    /// Looks a UniversalTicker up in SYM (`GetRequest`). Only OnInit and
    /// OnStart may call it; any other hook gets [`Error::NotInInit`] without
    /// a request. It may block, until the hook's `deadline_ms` when it has
    /// one. A ticker SYM does not know is one the platform does not support;
    /// returning that error ends the session `INIT_FAILED`.
    pub fn resolve(&self, ticker: &str) -> Result<Info, Error> {
        self.plane.resolve(ticker)
    }

    /// The instrument's current row, from memory (E08-D025). A UID this
    /// session has not resolved is [`Error::UnknownUid`] (E08-D024).
    pub fn info(&self, uid: u32) -> Result<Info, Error> {
        self.plane.wiring.info(uid)
    }

    // ---------- orders ----------

    /// Places an order and returns its client_order_id. The order is in
    /// [`Context::open_orders`] as `PENDING_NEW` before this returns; what
    /// happens to it comes as OrderEvents. An error means nothing was sent.
    pub fn submit(&self, order: Order) -> Result<String, Error> {
        self.plane.wiring.submit(order)
    }

    /// Asks TD to cancel one of the strategy's orders. A local refusal comes
    /// as a RejectEvent.
    pub fn cancel(&self, api_id: i64, client_order_id: &str) -> Result<(), Error> {
        self.plane.wiring.cancel(api_id, client_order_id)
    }

    /// Cancels every open order of `algo_id` on the account, only those of
    /// `uids` when it is not empty, split by the account's cancel_batch
    /// (E06-D002, E06-D031).
    pub fn batch_cancel(&self, api_id: i64, algo_id: u32, uids: &[u32]) -> Result<(), Error> {
        self.plane.wiring.batch_cancel(api_id, algo_id, uids)
    }

    /// Asks td.view for a new snapshot of the account; it arrives as OnRecon.
    pub fn request_recon(&self, api_id: i64) -> Result<(), Error> {
        self.plane.wiring.request_recon(api_id)
    }

    // ---------- local state (current as of the event being handled) ----------

    /// One of the strategy's orders, a copy (E08-D056). Inside OnOrder a
    /// terminal order is still visible.
    pub fn order(&self, api_id: i64, client_order_id: &str) -> Option<td::Order> {
        self.plane.wiring.order(api_id, client_order_id)
    }

    /// The account's live orders of `algo_id`; 0 is every algo the session
    /// declared. Terminal orders are omitted.
    pub fn open_orders(&self, api_id: i64, algo_id: u32) -> Vec<td::Order> {
        self.plane.wiring.open_orders(api_id, algo_id)
    }

    /// The account's position for `uid`, as last written (E08-D053).
    pub fn position(&self, api_id: i64, uid: u32) -> Option<td::Position> {
        self.plane.wiring.position(api_id, uid)
    }

    /// The account's balance of `asset`, as TD sent it (E08-D053).
    pub fn balance(&self, api_id: i64, asset: &str) -> Option<td::Balance> {
        self.plane.wiring.balance(api_id, asset)
    }

    /// The local book of an instrument once md.view's snapshot is in place
    /// (E08-D057), as a view that derefs to [`OrderBook`](crate::OrderBook). It is borrowed
    /// from this hook's Context, so it cannot outlive the hook (E08-D003),
    /// and the books change only between hooks, so it does not change under
    /// you (E03-D067). `None` while it is not synced (E08-D060).
    pub fn book(&self, uid: u32) -> Option<BookView<'_>> {
        self.plane.wiring.book(uid)
    }

    // ---------- reads (the answer runs `f` on the dispatcher) ----------
    // Each waits recv_window_ms; a timeout or a missing responder is an
    // error handed to `f` (E08-D048).

    /// Pages md.view's tape (E04-D036, E08-D047).
    pub fn tape<F>(&self, uid: u32, topic: &str, query: TapeQuery, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut S, &Context<S>, Result<md::TapePage, ReadError>) + Send + 'static,
    {
        let plane = Arc::clone(&self.plane);
        self.plane.wiring.tape(
            uid,
            topic,
            query,
            Box::new(move |reply| plane.in_hook(|s, ctx| f(s, ctx, reply))),
        )
    }

    /// Asks md.view for the windowed book.
    pub fn book_snapshot<F>(&self, uid: u32, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut S, &Context<S>, Result<md::BookSnapshot, ReadError>) + Send + 'static,
    {
        let plane = Arc::clone(&self.plane);
        self.plane.wiring.book_snapshot(
            uid,
            Box::new(move |reply| plane.in_hook(|s, ctx| f(s, ctx, reply))),
        )
    }

    /// Asks md.fetch for the latest `limit` closed klines (E04-D019).
    pub fn fetch_klines<F>(&self, uid: u32, interval: &str, limit: u32, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut S, &Context<S>, Result<md::Klines, ReadError>) + Send + 'static,
    {
        let plane = Arc::clone(&self.plane);
        self.plane.wiring.fetch_klines(
            uid,
            interval,
            limit,
            Box::new(move |reply| plane.in_hook(|s, ctx| f(s, ctx, reply))),
        )
    }

    /// Asks md.fetch for the open interest the exchange does not push.
    pub fn fetch_open_interest<F>(&self, uid: u32, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut S, &Context<S>, Result<md::OpenInterest, ReadError>) + Send + 'static,
    {
        let plane = Arc::clone(&self.plane);
        self.plane.wiring.fetch_open_interest(
            uid,
            Box::new(move |reply| plane.in_hook(|s, ctx| f(s, ctx, reply))),
        )
    }

    // ---------- control ----------

    /// Runs `f` on the dispatcher `delay` from now, on the TD buffer. Each
    /// call is its own timer (E08-D003). Before Init has the dispatcher, or
    /// once the session is stopping, nothing is scheduled.
    pub fn after<F>(&self, delay: Duration, f: F)
    where
        F: FnOnce(&mut S, &Context<S>) + Send + 'static,
    {
        self.plane.after(delay, f);
    }

    /// Hands `f` to the dispatcher's TD buffer. From another thread, use
    /// [`Context::poster`].
    pub fn post<F>(&self, f: F)
    where
        F: FnOnce(&mut S, &Context<S>) + Send + 'static,
    {
        self.poster().post(f);
    }

    /// The thread-safe handle for [`Poster::post`].
    pub fn poster(&self) -> Poster<S> {
        Poster {
            plane: Arc::clone(&self.plane),
        }
    }

    /// Ends the session `DONE` after this hook (PROTOCOL §5.3).
    pub fn exit(&self) {
        self.plane.set_asked();
        if let Some(disp) = self.plane.dispatcher() {
            disp.exit();
        }
    }

    /// Ends the session `FAILED` with `reason` after this hook.
    pub fn fail(&self, reason: impl Into<String>) {
        self.plane.set_asked();
        if let Some(disp) = self.plane.dispatcher() {
            disp.fail(reason);
        }
    }

    /// The session's writable directory, kept across incarnations
    /// (E09-D004). Empty when unset. Write it with [`crate::volume::write`].
    pub fn volume_dir(&self) -> &str {
        self.plane.session().map_or("", |s| s.volume_dir.as_str())
    }

    /// Where the named input from the spec was unpacked, read-only
    /// (E09-D004). Empty when there is no input directory or `name` is not a
    /// relative path inside it, as Go.
    pub fn input_dir(&self, name: &str) -> String {
        let dir = self.plane.session().map_or("", |s| s.input_dir.as_str());
        input_path(dir, name)
    }
}

/// Go's `Post`: hands a callback to the dispatcher from any thread. It runs
/// on the TD buffer like any hook. Before Init has the dispatcher, or once
/// the TD buffer is closed, the callback is dropped. With the event log on,
/// posts are numbered in call order, dropped ones included (PROTOCOL §12.5).
pub struct Poster<S> {
    plane: Arc<Plane<S>>,
}

impl<S> Clone for Poster<S> {
    fn clone(&self) -> Self {
        Self {
            plane: Arc::clone(&self.plane),
        }
    }
}

impl<S: Strategy> Poster<S> {
    pub fn post<F>(&self, f: F)
    where
        F: FnOnce(&mut S, &Context<S>) + Send + 'static,
    {
        let Some(disp) = self.plane.dispatcher() else {
            return;
        };
        self.plane.fresh_now();
        let plane = Arc::clone(&self.plane);
        let _ = disp.post(move || plane.in_hook(f));
    }
}

/// `filepath.Join(dir, name)` for a `name` Go's `relativeName` accepts, else
/// empty.
fn input_path(dir: &str, name: &str) -> String {
    if dir.is_empty() || !relative_name(name) {
        return String::new();
    }
    Path::new(dir).join(name).to_string_lossy().into_owned()
}

/// Non-empty, relative, already clean, and no `..` element.
fn relative_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_names_must_stay_inside_the_directory() {
        assert_eq!(input_path("/in", "model"), "/in/model");
        assert_eq!(input_path("/in", "a/b.bin"), "/in/a/b.bin");
        for bad in [
            "", ".", "..", "/abs", "a/../b", "a/./b", "a//b", "a/", "../x",
        ] {
            assert_eq!(input_path("/in", bad), "", "{bad:?}");
        }
        assert_eq!(input_path("", "model"), "");
    }

    #[test]
    fn error_texts_match_go() {
        assert_eq!(
            Error::NotInInit.to_string(),
            "sdk: only OnInit and OnStart may call this"
        );
        assert_eq!(Error::Stopping.to_string(), "sdk: the session is stopping");
        assert_eq!(
            Error::NotWired("td").to_string(),
            "sdk: the td path is not wired yet"
        );
    }
}
