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
//! - [`Wiring::listen_td`] and [`Wiring::recon`] are `tdpath.Listen` before
//!   the gate and `tdpath.Recon` after OnStart. Unwired, they succeed and do
//!   nothing. [`Wiring::unsubscribe_td`] is the dispatcher's `unsubscribe_td`.
//! - [`Wiring::resolve`] and [`Wiring::info`] are `symclient`. Unwired, they
//!   are [`Error::NotWired`].
//! - The order calls and reads are `tdpath`. Unwired, the calls are
//!   [`Error::NotWired`] and the reads find nothing, which is what Go
//!   returns before those paths exist.
//! - The market data reads are `mdpath` and the book is `bookpath`, in
//!   `crate::md`. Before Init they are [`Error::Stopping`] and no book, as
//!   Go's `sessCtx` with a nil path.
//! - [`Wiring::row_stored`] is where the SYM path (E03-08) reports each row
//!   symclient stores (`Effect::Stored`). [`Wiring::watch_bindings_applied`]
//!   is what Report (E03-09) watches (§9.2.4).

use std::sync::{Arc, OnceLock};

use mftik_core::mdpath::{ReadError, TapeQuery};
use mftik_core::symbol::Info;
use mftik_proto::wire::mftiko::common::v1::Version;
use mftik_proto::wire::mftiko::md::v1 as md;
use mftik_proto::wire::mftiko::stsctl::v1::Bindings;
use mftik_proto::wire::mftiko::td::v1 as td;
use tokio::sync::watch;

use crate::call::CallCtx;
use crate::context::Error;
use crate::md::{self as mdwire, BookView, Md, Rows};
use crate::strategy::Order;

/// Names in [`Error::NotWired`].
pub const TD: &str = "td";
pub const SYM: &str = "sym";

/// Hands a read's result to its callback. The path pushes it on the TD
/// buffer (E08-D048).
pub(crate) type Deliver<T> = Box<dyn FnOnce(Result<T, ReadError>) + Send>;

/// The paths of one session. See the module docs.
#[derive(Debug, Default)]
pub(crate) struct Wiring {
    md: OnceLock<Arc<Md>>,
    rows: Arc<Rows>,
    applied: watch::Sender<Option<Version>>,
}

impl Wiring {
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

    /// symclient stored `info` (E03-08 calls this for every
    /// `symclient::Effect::Stored`). Never blocks; any thread.
    #[allow(dead_code)] // E03-08 calls it.
    pub(crate) fn row_stored(&self, info: &Info) {
        self.rows.stored(info);
    }

    /// `Report.bindings_applied` (§9.2.4): `None` before the first snapshot
    /// is bound.
    #[allow(dead_code)] // E03-09 reads it.
    pub(crate) fn bindings_applied(&self) -> Option<Version> {
        self.md.get().and_then(|md| md.bindings_applied())
    }

    /// Changes each time a snapshot with a newer version is bound; a
    /// snapshot that is not newer does not change it.
    #[allow(dead_code)] // E03-09 watches it.
    pub(crate) fn watch_bindings_applied(&self) -> watch::Receiver<Option<Version>> {
        self.applied.subscribe()
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
        let _ = (call, ticker);
        Err(Error::NotWired(SYM))
    }

    pub(crate) fn info(&self, uid: u32) -> Result<Info, Error> {
        let _ = uid;
        Err(Error::NotWired(SYM))
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

    /// The local books, locked for the hook that asks (see `Context::book`).
    /// `None` before Init opens the MD path.
    pub(crate) fn book(&self, uid: u32) -> Option<BookView<'_>> {
        self.md.get()?.book(uid)
    }
}
