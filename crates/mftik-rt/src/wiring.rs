//! Where the MD, TD, and SYM paths join the plane.
//!
//! The protocol logic of each path is a sans-IO machine in `mftik-core`
//! (`mdpath` and `bookpath`, `tdpath`, `symclient`, E03-D001). Driving them
//! over NATS is E03-06 (MD, #409), E03-07 (TD, #410), and E03-08 (SYM, #411);
//! this ticket (E03-05) wires none of them. Each method below is the one call
//! the plane makes into a path, at the point Go's `sdk/plane.go` makes it, so
//! those tickets fill these bodies and add their state here without moving
//! a call site:
//!
//! - [`Wiring::apply_bindings`] is `mdpath.Apply` for `StartPayload`, `Live`,
//!   and `Command.bindings`. Unwired, a decoded snapshot is accepted and
//!   nothing is subscribed.
//! - [`Wiring::listen_td`] and [`Wiring::recon`] are `tdpath.Listen` before
//!   the gate and `tdpath.Recon` after OnStart. Unwired, they succeed and do
//!   nothing. [`Wiring::unsubscribe_td`] is the dispatcher's `unsubscribe_td`.
//! - [`Wiring::resolve`] and [`Wiring::info`] are `symclient`, driven by
//!   [`crate::sym::Sym`] (E03-08). Init opens it on the session's connection
//!   with [`Wiring::open_sym`]; the connection's reconnects reach it through
//!   [`Wiring::sym_reconnect_hook`].
//! - The order calls and reads are `tdpath`, the market data reads are
//!   `mdpath`, and the book is `bookpath`. Unwired, the calls are
//!   [`Error::NotWired`] and the reads find nothing, which is what Go
//!   returns before those paths exist.

use mftik_core::book::OrderBook;
use mftik_core::mdpath::{ReadError, TapeQuery};
use mftik_core::symbol::Info;
use mftik_core::symclient;
use mftik_proto::wire::mftiko::md::v1 as md;
use mftik_proto::wire::mftiko::stsctl::v1::Bindings;
use mftik_proto::wire::mftiko::td::v1 as td;

use crate::call::CallCtx;
use crate::clock::Clock;
use crate::context::Error;
use crate::io::Io;
use crate::nats::Reconnected;
use crate::strategy::Order;
use crate::sym::{Log, Sym};
use crate::timers::Timers;

/// Names in [`Error::NotWired`].
pub const MD: &str = "md";
pub const TD: &str = "td";

/// Hands a read's result to its callback. The path pushes it on the TD
/// buffer (E08-D048).
pub(crate) type Deliver<T> = Box<dyn FnOnce(Result<T, ReadError>) + Send>;

/// The paths of one session. See the module docs.
#[derive(Debug, Default)]
pub(crate) struct Wiring {
    pub(crate) sym: Sym,
}

impl Wiring {
    /// Init has the session's connection: the SYM path starts using it.
    pub(crate) fn open_sym(
        &self,
        io: &Io,
        clock: Clock,
        timers: &Timers,
        nats: &async_nats::Client,
        log: Log,
    ) {
        self.sym.open(io, clock, timers, nats, log);
    }

    /// What the connection calls on each reconnect (Go's
    /// `ReconnectHandler`, PROTOCOL §7.3).
    pub(crate) fn sym_reconnect_hook(&self) -> Reconnected {
        self.sym.reconnect_hook()
    }

    pub(crate) fn apply_bindings(&self, bindings: &Bindings) -> Result<(), String> {
        let _ = bindings;
        Ok(())
    }

    pub(crate) fn listen_td(&self) -> Result<(), String> {
        Ok(())
    }

    /// The first Recon after OnStart. `Err(Error::Stopping)` is not a Start
    /// failure: a hold that filled during OnStart has already ended the
    /// session `FAILED` (E08-D041).
    pub(crate) fn recon(&self) -> Result<(), Error> {
        Ok(())
    }

    pub(crate) fn unsubscribe_td(&self) {}

    /// `call` is the hook's context: the resolve waits until its deadline
    /// when it has one (E03-D041 on #411).
    pub(crate) fn resolve(&self, call: &CallCtx, ticker: &str) -> Result<Info, Error> {
        self.sym.resolve(call, ticker).map_err(sym_error)
    }

    pub(crate) fn info(&self, uid: u32) -> Result<Info, Error> {
        self.sym.info(uid).map_err(sym_error)
    }

    pub(crate) fn submit(&self, order: Order) -> Result<String, Error> {
        let _ = order;
        Err(Error::NotWired(TD))
    }

    pub(crate) fn cancel(&self, api_id: i64, client_order_id: &str) -> Result<(), Error> {
        let _ = (api_id, client_order_id);
        Err(Error::NotWired(TD))
    }

    pub(crate) fn batch_cancel(
        &self,
        api_id: i64,
        algo_id: u32,
        uids: &[u32],
    ) -> Result<(), Error> {
        let _ = (api_id, algo_id, uids);
        Err(Error::NotWired(TD))
    }

    pub(crate) fn request_recon(&self, api_id: i64) -> Result<(), Error> {
        let _ = api_id;
        Err(Error::NotWired(TD))
    }

    pub(crate) fn order(&self, api_id: i64, client_order_id: &str) -> Option<td::Order> {
        let _ = (api_id, client_order_id);
        None
    }

    pub(crate) fn open_orders(&self, api_id: i64, algo_id: u32) -> Vec<td::Order> {
        let _ = (api_id, algo_id);
        Vec::new()
    }

    pub(crate) fn position(&self, api_id: i64, uid: u32) -> Option<td::Position> {
        let _ = (api_id, uid);
        None
    }

    pub(crate) fn balance(&self, api_id: i64, asset: &str) -> Option<td::Balance> {
        let _ = (api_id, asset);
        None
    }

    pub(crate) fn tape(
        &self,
        uid: u32,
        topic: &str,
        query: TapeQuery,
        deliver: Deliver<md::TapePage>,
    ) -> Result<(), Error> {
        let _ = (uid, topic, query, deliver);
        Err(Error::NotWired(MD))
    }

    pub(crate) fn book_snapshot(
        &self,
        uid: u32,
        deliver: Deliver<md::BookSnapshot>,
    ) -> Result<(), Error> {
        let _ = (uid, deliver);
        Err(Error::NotWired(MD))
    }

    pub(crate) fn fetch_klines(
        &self,
        uid: u32,
        interval: &str,
        limit: u32,
        deliver: Deliver<md::Klines>,
    ) -> Result<(), Error> {
        let _ = (uid, interval, limit, deliver);
        Err(Error::NotWired(MD))
    }

    pub(crate) fn fetch_open_interest(
        &self,
        uid: u32,
        deliver: Deliver<md::OpenInterest>,
    ) -> Result<(), Error> {
        let _ = (uid, deliver);
        Err(Error::NotWired(MD))
    }

    pub(crate) fn book(&self, uid: u32) -> Option<&OrderBook> {
        let _ = uid;
        None
    }
}

/// Go's `asSDK` for symclient errors. `Closed` (no connection yet, or the
/// client closed) is `Stopping`, as Go's `p.sym == nil` (E03-D085). Other
/// errors keep their text.
fn sym_error(err: symclient::Error) -> Error {
    match err {
        symclient::Error::NotInInit => Error::NotInInit,
        symclient::Error::UnknownUid => Error::UnknownUid,
        symclient::Error::Closed => Error::Stopping,
        other => Error::Other(other.to_string()),
    }
}
