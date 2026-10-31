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
//!   and `Command.bindings` (E03-06, `crate::md`). Before Init opens the MD
//!   path it is Go's `bindings` error.
//! - [`Wiring::open_td`] is `tdpath.Open` in Init. [`Wiring::listen_td`] and
//!   [`Wiring::recon`] are `tdpath.Listen` before the gate and `tdpath.Recon`
//!   after OnStart. [`Wiring::unsubscribe_td`] is the dispatcher's
//!   `unsubscribe_td`. The TD path is `crate::td` (E03-07).
//! - [`Wiring::resolve`] and [`Wiring::info`] are `symclient`, driven by
//!   [`crate::sym::Sym`] (E03-08). Init opens it on the session's connection
//!   with [`Wiring::open_sym`]; the connection's reconnects reach it through
//!   [`Wiring::sym_reconnect_hook`].
//! - The order calls and reads are `tdpath`: before Init opens it they are
//!   [`Error::Stopping`] and find nothing, as Go's with a nil path.
//! - The market data reads are `mdpath` and the book is `bookpath`, in
//!   `crate::md`. Before Init they are [`Error::Stopping`] and no book, as
//!   Go's `sessCtx` with a nil path.
//! - Every row symclient stores (`Effect::Stored`) reaches the MD path's
//!   rows, which is where bookpath gets `price_tick` and the reads get the
//!   ticker and venue (E03-D065): [`Wiring::open_sym`] sets that callback.
//!   [`Wiring::watch_bindings_applied`] is what Report watches (§9.2.4,
//!   `crate::report`).

use std::sync::{Arc, OnceLock};

use mftik_core::mdpath::{ReadError, TapeQuery};
use mftik_core::symbol::Info;
use mftik_core::symclient;
use mftik_proto::wire::mftiko::common::v1::Version;
use mftik_proto::wire::mftiko::md::v1 as md;
use mftik_proto::wire::mftiko::stsctl::v1::Bindings;
use mftik_proto::wire::mftiko::td::v1 as td;
use tokio::sync::watch;

use crate::call::CallCtx;
use crate::clock::Clock;
use crate::context::Error;
use crate::io::Io;
use crate::md::{self as mdwire, BookView, Md, Rows};
use crate::nats::Reconnected;
use crate::strategy::Order;
use crate::sym::{Log, Sym};
use crate::td::{self as tdwire, TdPath};
use crate::timers::Timers;

/// Hands a read's result to its callback. The path pushes it on the TD
/// buffer (E08-D048).
pub(crate) type Deliver<T> = Box<dyn FnOnce(Result<T, ReadError>) + Send>;

/// The paths of one session. See the module docs.
#[derive(Debug, Default)]
pub(crate) struct Wiring {
    md: OnceLock<Arc<Md>>,
    rows: Arc<Rows>,
    applied: watch::Sender<Option<Version>>,
    pub(crate) sym: Sym,
    td: OnceLock<TdPath>,
}

impl Wiring {
    /// Init has the session's connection: the SYM path starts using it.
    /// Each row it stores is handed to the MD path's rows first, so bookpath
    /// has the row's `price_tick` and the reads its ticker and venue
    /// (E03-D065, E03-D069).
    pub(crate) fn open_sym(
        &self,
        io: &Io,
        clock: Clock,
        timers: &Timers,
        nats: &async_nats::Client,
        log: Log,
    ) {
        let rows = Arc::clone(&self.rows);
        self.sym
            .on_stored(Arc::new(move |_uid: u32, info: &Info| rows.stored(info)));
        self.sym.open(io, clock, timers, nats, log);
    }

    /// What the connection calls on each reconnect (Go's
    /// `ReconnectHandler`, PROTOCOL §7.3).
    pub(crate) fn sym_reconnect_hook(&self) -> Reconnected {
        self.sym.reconnect_hook()
    }

    /// Init opens the MD path once the dispatcher exists (Go `mdpath.Open`,
    /// `bookpath.Open`).
    pub(crate) fn open_md(&self, config: mdwire::Config) -> Result<(), String> {
        let md = Md::open(config, Arc::clone(&self.rows), self.applied.clone())?;
        let _ = self.md.set(md);
        Ok(())
    }

    pub(crate) fn md(&self) -> Option<&Arc<Md>> {
        self.md.get()
    }

    pub(crate) fn apply_bindings(&self, bindings: &Bindings) -> Result<(), String> {
        match self.md.get() {
            Some(md) => md.apply(bindings),
            None => Err("bindings".to_owned()),
        }
    }

    /// `Report.bindings_applied` (§9.2.4): `None` before the first snapshot
    /// is bound.
    pub(crate) fn bindings_applied(&self) -> Option<Version> {
        self.md.get().and_then(|md| md.bindings_applied())
    }

    /// Changes each time a snapshot with a newer version is bound; a
    /// snapshot that is not newer does not change it.
    pub(crate) fn watch_bindings_applied(&self) -> watch::Receiver<Option<Version>> {
        self.applied.subscribe()
    }

    /// Opens the TD path in Init. A second open is refused.
    pub(crate) fn open_td(&self, config: tdwire::Config) -> Result<(), String> {
        let path = TdPath::open(config)?;
        self.td.set(path).map_err(|_| "td".to_owned())
    }

    /// C4 never calls Start before a successful Init, so the path is open;
    /// without one (the plane's unit tests) this and [`Wiring::recon`] do
    /// nothing.
    pub(crate) fn listen_td(&self) -> Result<(), String> {
        self.td.get().map_or(Ok(()), TdPath::listen)
    }

    /// The first Recon after OnStart. `Err(Error::Stopping)` is not a Start
    /// failure: a hold that filled during OnStart has already ended the
    /// session `FAILED` (E08-D041).
    pub(crate) fn recon(&self) -> Result<(), Error> {
        self.td.get().map_or(Ok(()), TdPath::recon)
    }

    pub(crate) fn unsubscribe_td(&self) {
        if let Some(td) = self.td.get() {
            td.unsubscribe();
        }
    }

    /// `call` is the hook's context: the resolve waits until its deadline
    /// when it has one (E03-D041 on #411). The error is SYM's own, which the
    /// event log classifies; the plane maps it with [`sym_error`].
    pub(crate) fn resolve(&self, call: &CallCtx, ticker: &str) -> Result<Info, symclient::Error> {
        self.sym.resolve(call, ticker)
    }

    pub(crate) fn info(&self, uid: u32) -> Result<Info, Error> {
        self.sym.info(uid).map_err(sym_error)
    }

    pub(crate) fn submit(&self, order: Order) -> Result<String, Error> {
        self.td.get().ok_or(Error::Stopping)?.submit(order)
    }

    pub(crate) fn cancel(&self, api_id: i64, client_order_id: &str) -> Result<(), Error> {
        self.td
            .get()
            .ok_or(Error::Stopping)?
            .cancel(api_id, client_order_id)
    }

    pub(crate) fn batch_cancel(
        &self,
        api_id: i64,
        algo_id: u32,
        uids: &[u32],
    ) -> Result<(), Error> {
        self.td
            .get()
            .ok_or(Error::Stopping)?
            .batch_cancel(api_id, algo_id, uids)
    }

    pub(crate) fn request_recon(&self, api_id: i64) -> Result<(), Error> {
        self.td.get().ok_or(Error::Stopping)?.request_recon(api_id)
    }

    pub(crate) fn order(&self, api_id: i64, client_order_id: &str) -> Option<td::Order> {
        self.td.get()?.order(api_id, client_order_id)
    }

    pub(crate) fn open_orders(&self, api_id: i64, algo_id: u32) -> Vec<td::Order> {
        self.td
            .get()
            .map_or_else(Vec::new, |td| td.open_orders(api_id, algo_id))
    }

    pub(crate) fn position(&self, api_id: i64, uid: u32) -> Option<td::Position> {
        self.td.get()?.position(api_id, uid)
    }

    pub(crate) fn balance(&self, api_id: i64, asset: &str) -> Option<td::Balance> {
        self.td.get()?.balance(api_id, asset)
    }

    pub(crate) fn tape(
        &self,
        uid: u32,
        topic: &str,
        query: TapeQuery,
        deliver: Deliver<md::TapePage>,
    ) -> Result<(), Error> {
        self.md
            .get()
            .ok_or(Error::Stopping)?
            .tape(uid, topic, query, deliver)
    }

    pub(crate) fn book_snapshot(
        &self,
        uid: u32,
        deliver: Deliver<md::BookSnapshot>,
    ) -> Result<(), Error> {
        self.md
            .get()
            .ok_or(Error::Stopping)?
            .book_snapshot(uid, deliver)
    }

    pub(crate) fn fetch_klines(
        &self,
        uid: u32,
        interval: &str,
        limit: u32,
        deliver: Deliver<md::Klines>,
    ) -> Result<(), Error> {
        self.md
            .get()
            .ok_or(Error::Stopping)?
            .fetch_klines(uid, interval, limit, deliver)
    }

    pub(crate) fn fetch_open_interest(
        &self,
        uid: u32,
        deliver: Deliver<md::OpenInterest>,
    ) -> Result<(), Error> {
        self.md
            .get()
            .ok_or(Error::Stopping)?
            .fetch_open_interest(uid, deliver)
    }

    /// One synced local book for the hook that asks (see `Context::book`,
    /// E03-D067). `None` before Init opens the MD path.
    pub(crate) fn book(&self, uid: u32) -> Option<BookView<'_>> {
        self.md.get()?.book(uid)
    }
}

/// Go's `asSDK` for symclient errors. `Closed` (no connection yet, or the
/// client closed) is `Stopping`, as Go's `p.sym == nil` (E03-D085). Other
/// errors keep their text.
pub(crate) fn sym_error(err: symclient::Error) -> Error {
    match err {
        symclient::Error::NotInInit => Error::NotInInit,
        symclient::Error::UnknownUid => Error::UnknownUid,
        symclient::Error::Closed => Error::Stopping,
        other => Error::Other(other.to_string()),
    }
}
