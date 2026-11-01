//! The MD path over NATS (PROTOCOL §7.1, §9.2.9, §9.3.2, §9.3.5,
//! §9.4.2–§9.4.4, §9.5; E03-06).
//!
//! `mdpath` and `bookpath` decide; this module does their IO (E03-D001):
//!
//! - Bindings: [`Md::apply`] stages a snapshot, subscribes each new subject,
//!   flushes, and only then binds it, unsubscribing what left the set
//!   (E03-D011). A failed subscribe or flush aborts the stage. Applies are
//!   serialized, as Go's `Apply` holds its mutex. `bindings_applied` is
//!   published on a watch when it changes (§9.2.4), for Report (E03-09).
//! - `md.f`: each subscription's task, on the IO thread, asks
//!   `mdpath::Path::route` where a message goes and copies the bytes into the
//!   dispatcher with `push_md` or `push_book`, without decoding the payload
//!   (§9.4.3, E08-D049). The dispatcher clock is moved first (E03-D052).
//! - The book: the dispatcher's book callback is [`Md::on_book_message`].
//!   bookpath's effects run on the dispatcher thread: `RequestSnapshot` is an
//!   `md.view` request that waits `recv_window`, `ArmTimer` is a
//!   [`TimerKey::Book`] timer whose fire goes on the TD buffer, and `Hook` is
//!   OnBook. Snapshot replies come back on the TD buffer too, so bookpath is
//!   driven from one thread, as Go's callbacks all run on the dispatcher.
//! - Reads: Tape, BookSnapshot, FetchKlines, and FetchOpenInterest send one
//!   NATS request each, waiting `recv_window`; the reply, a timeout, or a
//!   missing responder goes on the TD buffer (E08-D048). With the event log
//!   on, each request, the book's own snapshots too, is a `read_call`
//!   numbered from 1 when it is sent, and how it ended is the `read_reply`
//!   input of that TD entry, with the same read id (§12.4, §12.5, E07-D011).
//! - Instruments: [`Rows`] keeps, per UID, the ticker and venue the reads
//!   need and the `price_tick` bookpath needs. The SYM path (E03-08) calls
//!   [`Rows::stored`] for every `symclient::Effect::Stored`; this path never
//!   asks SYM (E08-D023, E08-D059).

use std::cell::Ref;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use mftik_core::book::{Level, OrderBook};
use mftik_core::bookpath::{self, Snapshot, SnapshotReply, TimerToken};
use mftik_core::dispatch::{Dispatcher, Input};
use mftik_core::eventlog::{
    Kind, READ_BOOK_SNAPSHOT, READ_KLINES, READ_OPEN_INTEREST, READ_TAPE, Recorder, outcome,
    put_str,
};
use mftik_core::mdpath::{
    self, Apply, Delivery, ReadKind, Reply, Request, RequestError, SubId, TapeQuery,
};
use mftik_core::symbol::Info;
use mftik_proto::wire::mftiko::common::v1::Version;
use mftik_proto::wire::mftiko::md::v1 as md;
use mftik_proto::wire::mftiko::stsctl::v1::Bindings;
use tokio::sync::{oneshot, watch};

use crate::clock::Clock;
use crate::context::Error;
use crate::dcell::DispatcherCell;
use crate::io::Io;
use crate::nats;
use crate::sym::{DEADLINE_EXCEEDED, NO_RESPONDERS};
use crate::timers::{TimerKey, Timers};
use crate::wiring::Deliver;

#[cfg(test)]
mod tests;

/// How long the flush after new subscribes may take, Go's `flushTimeout`.
pub(crate) const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// OnBook, through the plane (Go `bookHook`).
pub(crate) type BookSink = Arc<dyn Fn(bookpath::Update) + Send + Sync>;

/// One log line (Go `note`).
pub(crate) type Log = Arc<dyn Fn(&str) + Send + Sync>;

/// What [`Md::open`] needs. Init makes it once the dispatcher exists.
pub(crate) struct Config {
    pub(crate) io: Io,
    pub(crate) clock: Clock,
    pub(crate) timers: Timers,
    pub(crate) disp: Dispatcher,
    pub(crate) nats: Option<async_nats::Client>,
    pub(crate) recv_window: Duration,
    pub(crate) hooks: mdpath::Hooks,
    pub(crate) on_book: BookSink,
    pub(crate) log: Log,
    pub(crate) replay: bool,
    pub(crate) rec: Option<Arc<dyn Recorder>>,
}

/// The instruments this session has resolved, as the MD path reads them.
///
/// E03-08 calls [`Rows::stored`] each time symclient stores a row
/// (`Effect::Stored { uid }`, then `Client::info(uid)`). It never blocks and
/// may be called from any thread, before or after Init opens the MD path.
#[derive(Debug, Default)]
pub(crate) struct Rows {
    inner: Mutex<RowsInner>,
}

#[derive(Debug, Default)]
struct RowsInner {
    by_uid: HashMap<u32, Row>,
    /// Ticks stored since bookpath last took them, the latest per UID, so
    /// refetches without book traffic do not pile up.
    ticks: HashMap<u32, f64>,
}

#[derive(Clone, Debug)]
struct Row {
    ticker: String,
    venue: String,
}

impl Rows {
    /// One stored row: its ticker and venue for the reads, its `price_tick`
    /// for bookpath (`bookpath::Path::resolve`, §9.5.7). A later row for the
    /// same UID replaces it.
    pub(crate) fn stored(&self, info: &Info) {
        let mut inner = lock(&self.inner);
        inner.by_uid.insert(
            info.uid,
            Row {
                ticker: info.ticker.to_string(),
                venue: info.ticker.venue.clone(),
            },
        );
        inner.ticks.insert(info.uid, info.price_tick());
    }

    /// The ticker of `uid`, empty when it has no row (mdpath's `UnknownUid`).
    fn ticker(&self, uid: u32) -> String {
        lock(&self.inner)
            .by_uid
            .get(&uid)
            .map(|row| row.ticker.clone())
            .unwrap_or_default()
    }

    fn venue(&self, uid: u32) -> Option<String> {
        lock(&self.inner)
            .by_uid
            .get(&uid)
            .map(|row| row.venue.clone())
    }

    fn take_ticks(&self) -> HashMap<u32, f64> {
        std::mem::take(&mut lock(&self.inner).ticks)
    }
}

/// A synced local book, borrowed for the rest of a hook (`Context::book`).
///
/// Go's `Book` returns a `BookView`; this is the same idea. It derefs to
/// [`OrderBook`]. Holding it past the hook is not possible (it borrows the
/// Context), and the books do not change while it is alive: they change only
/// between hooks, on the same thread (E03-D067).
pub struct BookView<'a>(Ref<'a, OrderBook>);

impl Deref for BookView<'_> {
    type Target = OrderBook;

    fn deref(&self) -> &OrderBook {
        &self.0
    }
}

impl fmt::Debug for BookView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BookView").finish_non_exhaustive()
    }
}

/// One session's MD path. See the module docs.
pub(crate) struct Md {
    io: Io,
    clock: Clock,
    timers: Timers,
    disp: Dispatcher,
    nats: Option<async_nats::Client>,
    rows: Arc<Rows>,
    applied: watch::Sender<Option<Version>>,
    on_book: BookSink,
    log: Log,
    path: Mutex<mdpath::Path>,
    /// One apply at a time (Go's `op` mutex).
    applying: Mutex<()>,
    /// The live NATS subscriptions; sending stops one.
    subs: Mutex<HashMap<SubId, oneshot::Sender<()>>>,
    /// Touched on the dispatcher thread only, without a lock: the book
    /// callback, the TD entries of snapshot replies and timers, and a hook's
    /// Context (E03-D064, E03-D067).
    books: DispatcherCell<bookpath::Path>,
    /// The last read id. Counted with the event log on only, as Go's.
    reads: AtomicU64,
    replay: bool,
    rec: Option<Arc<dyn Recorder>>,
    waiting: Mutex<HashMap<u64, WaitingRead>>,
    me: Weak<Md>,
}

/// A replayed read waiting for its recorded `read_reply`.
type WaitingDone = Box<dyn FnOnce(&Md, Delivery) + Send>;

struct WaitingRead {
    ticket: mdpath::ReadTicket,
    done: WaitingDone,
}

impl fmt::Debug for Md {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Md").finish_non_exhaustive()
    }
}

impl Md {
    /// The path with nothing subscribed; apply the start bindings next.
    /// `rows` and `applied` outlive it: they belong to the session's wiring.
    pub(crate) fn open(
        config: Config,
        rows: Arc<Rows>,
        applied: watch::Sender<Option<Version>>,
    ) -> Result<Arc<Md>, String> {
        let path = mdpath::Path::open(mdpath::Config {
            recv_window: config.recv_window,
            hooks: config.hooks,
        })
        .map_err(|err| err.to_string())?;
        let books = bookpath::Path::open(bookpath::Config {
            recv_window: config.recv_window,
        })
        .map_err(|err| err.to_string())?;
        Ok(Arc::new_cyclic(|me| Md {
            io: config.io,
            clock: config.clock,
            timers: config.timers,
            disp: config.disp,
            nats: config.nats,
            rows,
            applied,
            on_book: config.on_book,
            log: config.log,
            path: Mutex::new(path),
            applying: Mutex::new(()),
            subs: Mutex::default(),
            books: DispatcherCell::new(books),
            reads: AtomicU64::new(0),
            replay: config.replay,
            rec: config.rec,
            waiting: Mutex::default(),
            me: me.clone(),
        }))
    }

    // ---------- bindings ----------

    /// One `StartPayload.bindings`, `Live`, or `Command.bindings` snapshot
    /// (E03-D011). `Ok` for a version that is not newer, so C4 still sends
    /// `Applied` for Live (§9.2.9). Blocks on the IO thread for the
    /// subscribes and the flush, so it must not run there.
    pub(crate) fn apply(&self, bindings: &Bindings) -> Result<(), String> {
        let _one = lock(&self.applying);
        let staged = lock(&self.path)
            .apply(bindings)
            .map_err(|err| err.to_string())?;
        let subscribes = match staged {
            Apply::Unchanged => return Ok(()),
            Apply::Staged(subscribes) => subscribes,
        };
        if self.replay {
            drop(subscribes);
        } else {
            let Some(me) = self.me.upgrade() else {
                return Err(mdpath::Error::Stopping.to_string());
            };
            let bound = self
                .io
                .block_on(async move { me.subscribe_all(subscribes).await });
            if let Err(err) = bound {
                let opened = lock(&self.path).apply_aborted();
                self.unsubscribe(opened);
                return Err(err);
            }
        }
        let (left, applied) = {
            let mut path = lock(&self.path);
            let left = path.apply_bound().map_err(|err| err.to_string())?;
            (left, path.bindings_applied())
        };
        self.unsubscribe(left);
        self.applied.send_if_modified(|held| {
            let changed = *held != applied;
            *held = applied;
            changed
        });
        Ok(())
    }

    /// `Report.bindings_applied` (§9.2.4).
    pub(crate) fn bindings_applied(&self) -> Option<Version> {
        lock(&self.path).bindings_applied()
    }

    async fn subscribe_all(self: Arc<Self>, effects: Vec<mdpath::Effect>) -> Result<(), String> {
        let nats = self
            .nats
            .as_ref()
            .ok_or_else(|| "mdpath: nil nats conn".to_owned())?;
        let mut any = false;
        for effect in effects {
            let mdpath::Effect::Subscribe { sub, subject } = effect else {
                continue;
            };
            let subscriber = nats
                .subscribe(subject.clone())
                .await
                .map_err(|err| format!("mdpath: subscribe {subject}: {err}"))?;
            let (stop, stopped) = oneshot::channel();
            lock(&self.subs).insert(sub, stop);
            let me = Arc::downgrade(&self);
            self.io.spawn(read_feed(me, sub, subscriber, stopped));
            any = true;
        }
        if any {
            flush(nats).await?;
        }
        Ok(())
    }

    fn unsubscribe(&self, effects: Vec<mdpath::Effect>) {
        let mut subs = lock(&self.subs);
        for effect in effects {
            if let mdpath::Effect::Unsubscribe { sub, .. } = effect
                && let Some(stop) = subs.remove(&sub)
            {
                let _ = stop.send(());
            }
        }
    }

    /// One `md.f` message on the IO thread (§9.4.3).
    fn on_msg(&self, sub: SubId, msg: &[u8]) {
        let route = lock(&self.path).route(sub, msg);
        match route {
            mdpath::Route::Md => {
                self.fresh_now();
                let _ = self.disp.push_md(msg);
            }
            mdpath::Route::Book => {
                self.fresh_now();
                let _ = self.disp.push_book(msg);
            }
            mdpath::Route::Skip => {}
        }
    }

    /// Moves the dispatcher clock before a push (E03-D052).
    fn fresh_now(&self) {
        self.disp.set_now_ns(self.clock.now_ns());
    }

    // ---------- the book ----------

    /// The dispatcher's book callback: one `orderbook_delta` (§9.5.4).
    pub(crate) fn on_book_message(&self, msg: &[u8]) {
        let effects = self.with_books(|books, now| books.on_message(now, msg));
        self.perform_book(effects);
    }

    /// One synced local book for a hook (E08-D057, E08-D060). On the
    /// dispatcher thread only; the view must be dropped before the books
    /// change, which is between hooks (E03-D067).
    pub(crate) fn book(&self, uid: u32) -> Option<BookView<'_>> {
        Ref::filter_map(self.books.borrow(), |books| books.view(uid))
            .ok()
            .map(BookView)
    }

    fn with_books<R>(&self, f: impl FnOnce(&mut bookpath::Path, i64) -> R) -> R {
        let mut books = self.books.borrow_mut();
        for (uid, tick) in self.rows.take_ticks() {
            books.resolve(uid, tick);
        }
        let now = if self.replay {
            self.disp.now_ns()
        } else {
            self.clock.now_ns()
        };
        f(&mut books, now)
    }

    /// bookpath's effects, in order, with the books unlocked so OnBook can
    /// read them (bookpath: "view during a Hook already contains that
    /// delta").
    fn perform_book(&self, effects: Vec<bookpath::Effect>) {
        let mut queue = VecDeque::from(effects);
        while let Some(effect) = queue.pop_front() {
            match effect {
                bookpath::Effect::Hook(update) => (self.on_book)(update),
                bookpath::Effect::Log(line) => (self.log)(&line),
                bookpath::Effect::ArmTimer {
                    token, deadline_ns, ..
                } => self.arm_book_timer(token, deadline_ns),
                bookpath::Effect::RequestSnapshot {
                    uid, generation, ..
                } => {
                    if let Err(text) = self.request_snapshot(uid, generation) {
                        // Go's `fetch`: a request that could not start fails
                        // at once and backs off.
                        let more = self.with_books(|books, now| {
                            books.on_snapshot(now, uid, generation, SnapshotReply::Failed(text))
                        });
                        for effect in more.into_iter().rev() {
                            queue.push_front(effect);
                        }
                    }
                }
            }
        }
    }

    /// `md.view` for the book; the reply goes on the TD buffer. The wait is
    /// mdpath's, which is `recv_window` as bookpath's.
    fn request_snapshot(&self, uid: u32, generation: u64) -> Result<(), String> {
        let ticker = self.rows.ticker(uid);
        let request = lock(&self.path)
            .book_snapshot(&ticker)
            .map_err(|err| err.to_string())?;
        self.send(request, move |md, delivery| {
            let reply = match delivery.reply {
                Reply::BookSnapshot(Ok(snap)) => SnapshotReply::Book(snapshot(snap)),
                Reply::BookSnapshot(Err(err)) => SnapshotReply::Failed(err.to_string()),
                _ => SnapshotReply::Empty,
            };
            let effects =
                md.with_books(|books, now| books.on_snapshot(now, uid, generation, reply));
            md.perform_book(effects);
        });
        Ok(())
    }

    /// Each arm is a new timer, numbered in the event log's sequence when it
    /// is armed, as Go's `After` makes one per call (PROTOCOL §12.5). An arm
    /// of the same key replaces the last one, which then never fires.
    fn arm_book_timer(&self, token: TimerToken, deadline_ns: i64) {
        let me = self.me.clone();
        if self.replay {
            let _ = self.disp.timer(move || {
                if let Some(md) = me.upgrade() {
                    let effects = md.with_books(|books, now| books.on_timer(now, token));
                    md.perform_book(effects);
                }
            });
            return;
        }
        let key = TimerKey::Book {
            uid: token.uid,
            kind: token.kind,
        };
        let number = self.disp.number_timer();
        self.timers.arm(key, deadline_ns, move || {
            let Some(md) = me.upgrade() else {
                return;
            };
            md.fresh_now();
            let on_td = me.clone();
            let input = Input {
                kind: Kind::TIMER,
                r#ref: number,
                ..Input::default()
            };
            // A stopping session drops it, as Go's `Timer.After` then does.
            let _ = md.disp.push_in(input, move || {
                if let Some(md) = on_td.upgrade() {
                    let effects = md.with_books(|books, now| books.on_timer(now, token));
                    md.perform_book(effects);
                }
            });
        });
    }

    // ---------- reads ----------

    /// Go `Tape` (E04-D036, E08-D047).
    pub(crate) fn tape(
        &self,
        uid: u32,
        topic: &str,
        query: TapeQuery,
        deliver: Deliver<md::TapePage>,
    ) -> Result<(), Error> {
        let ticker = self.rows.ticker(uid);
        let request = lock(&self.path)
            .tape(&ticker, topic, query)
            .map_err(sdk_error)?;
        self.send(request, move |_, delivery| {
            if let Reply::Tape(result) = delivery.reply {
                deliver(result);
            }
        });
        Ok(())
    }

    /// Go `BookSnapshot`. A `not_synced` reply is an error; no retry.
    pub(crate) fn book_snapshot(
        &self,
        uid: u32,
        deliver: Deliver<md::BookSnapshot>,
    ) -> Result<(), Error> {
        let ticker = self.rows.ticker(uid);
        let request = lock(&self.path).book_snapshot(&ticker).map_err(sdk_error)?;
        self.send(request, move |_, delivery| {
            if let Reply::BookSnapshot(result) = delivery.reply {
                deliver(result);
            }
        });
        Ok(())
    }

    /// Go `FetchKlines` (E04-D019, E04-D056).
    pub(crate) fn fetch_klines(
        &self,
        uid: u32,
        interval: &str,
        limit: u32,
        deliver: Deliver<md::Klines>,
    ) -> Result<(), Error> {
        let venue = self.rows.venue(uid).ok_or(Error::UnknownUid)?;
        let request = lock(&self.path)
            .fetch_klines(uid, &venue, interval, limit)
            .map_err(sdk_error)?;
        self.send(request, move |_, delivery| {
            if let Reply::Klines(result) = delivery.reply {
                deliver(result);
            }
        });
        Ok(())
    }

    /// Go `FetchOpenInterest`.
    pub(crate) fn fetch_open_interest(
        &self,
        uid: u32,
        deliver: Deliver<md::OpenInterest>,
    ) -> Result<(), Error> {
        let venue = self.rows.venue(uid).ok_or(Error::UnknownUid)?;
        let request = lock(&self.path)
            .fetch_open_interest(uid, &venue)
            .map_err(sdk_error)?;
        self.send(request, move |_, delivery| {
            if let Reply::OpenInterest(result) = delivery.reply {
                deliver(result);
            }
        });
        Ok(())
    }

    /// Sends `request` on the IO thread and runs `done` on the TD buffer with
    /// how it ended (E08-D048). A full or closed TD buffer drops it with Go's
    /// `md reply dropped` line.
    fn send(&self, request: Request, done: impl FnOnce(&Md, Delivery) + Send + 'static) {
        let read = self.record_read(&request);
        if self.replay {
            lock(&self.waiting).insert(
                read,
                WaitingRead {
                    ticket: request.ticket,
                    done: Box::new(done),
                },
            );
            return;
        }
        let me = self.me.clone();
        let client = self.nats.clone().expect("md nats");
        self.io.spawn(async move {
            let Request {
                ticket,
                subject,
                body,
                wait,
                ..
            } = request;
            let result = nats_request(&client, subject, body, wait).await;
            let Some(md) = me.upgrade() else {
                return;
            };
            let logged = md
                .disp
                .record_now()
                .map(|recv_ns| reply_input(read, recv_ns, &result));
            let delivery = lock(&md.path).on_reply(
                ticket,
                result.as_ref().map(|b| b.as_ref()).map_err(Clone::clone),
            );
            let Some(delivery) = delivery else {
                return;
            };
            md.fresh_now();
            let hook = move || {
                if let Some(md) = me.upgrade() {
                    done(&md, delivery);
                }
            };
            let pushed = match logged {
                Some(input) => md.disp.push_in(input, hook),
                None => md.disp.push_td(hook),
            };
            if let Err(err) = pushed {
                (md.log)(&format!("md reply dropped: {err}"));
            }
        });
    }

    /// Finish the replayed read `id` with its recorded `read_reply`.
    pub(crate) fn replay_reply(&self, id: u64, flags: u16, body: &[u8]) -> Result<(), String> {
        let waiting = lock(&self.waiting)
            .remove(&id)
            .ok_or_else(|| format!("mdpath: no replayed read waits for this reply: read {id}"))?;
        let result = match flags {
            outcome::OK => Ok(body),
            outcome::TIMEOUT => Err(mdpath::RequestError::Timeout),
            outcome::NO_RESPONDERS => Err(mdpath::RequestError::NoResponder),
            _ => Err(mdpath::RequestError::Other(
                String::from_utf8_lossy(body).into_owned(),
            )),
        };
        let delivery = lock(&self.path).on_reply(
            waiting.ticket,
            result.as_ref().copied().map_err(Clone::clone),
        );
        let Some(delivery) = delivery else {
            return Ok(());
        };
        (waiting.done)(self, delivery);
        Ok(())
    }

    fn recorder(&self) -> Option<&Arc<dyn Recorder>> {
        self.rec.as_ref().or_else(|| self.disp.recorder())
    }

    /// Writes the `read_call` of one request and returns its read id, which
    /// the reply's `read_reply` carries. 0 with the event log off (Go
    /// `readCall`).
    fn record_read(&self, request: &Request) -> u64 {
        let Some(rec) = self.recorder() else {
            return 0;
        };
        let flags = match request.kind {
            ReadKind::Tape => READ_TAPE,
            ReadKind::BookSnapshot => READ_BOOK_SNAPSHOT,
            ReadKind::Klines => READ_KLINES,
            ReadKind::OpenInterest => READ_OPEN_INTEREST,
        };
        let mut subject = Vec::new();
        put_str(&mut subject, request.subject.as_bytes());
        let read = self.reads.fetch_add(1, Ordering::Relaxed) + 1;
        rec.append(
            Kind::READ_CALL,
            flags,
            0,
            rec.now_ns(),
            read,
            &subject,
            &request.body,
        );
        read
    }
}

/// A read's `read_reply` input: the reply as it came, or how the request
/// failed, with Go's texts (Go `replyIn`).
fn reply_input(read: u64, recv_ns: i64, result: &Result<Bytes, RequestError>) -> Input {
    let (flags, body) = match result {
        Ok(reply) => (outcome::OK, reply.clone()),
        Err(RequestError::Timeout) => (
            outcome::TIMEOUT,
            Bytes::from_static(DEADLINE_EXCEEDED.as_bytes()),
        ),
        Err(RequestError::NoResponder) => (
            outcome::NO_RESPONDERS,
            Bytes::from_static(NO_RESPONDERS.as_bytes()),
        ),
        Err(RequestError::Other(text)) => (outcome::FAILED, Bytes::from(text.clone())),
    };
    Input {
        kind: Kind::READ_REPLY,
        flags,
        recv_ns,
        r#ref: read,
        body,
        ..Input::default()
    }
}

/// One subscription's messages until it is stopped (Go's `onMsg` callback).
async fn read_feed(
    md: Weak<Md>,
    sub: SubId,
    mut subscriber: async_nats::Subscriber,
    mut stop: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;
            _ = &mut stop => {
                let _ = subscriber.unsubscribe().await;
                return;
            }
            msg = subscriber.next() => {
                let Some(msg) = msg else { return };
                let Some(path) = md.upgrade() else { return };
                path.on_msg(sub, msg.payload.as_ref());
            }
        }
    }
}

/// [`nats::barrier`] within [`FLUSH_TIMEOUT`]: the new SUBs are on the
/// server (E03-D062).
async fn flush(client: &async_nats::Client) -> Result<(), String> {
    nats::barrier(client, FLUSH_TIMEOUT)
        .await
        .map_err(|err| format!("mdpath: flush: {err}"))
}

/// One `md.view` or `md.fetch` request that waits `wait`.
async fn nats_request(
    client: &async_nats::Client,
    subject: String,
    body: Vec<u8>,
    wait: Duration,
) -> Result<Bytes, RequestError> {
    use async_nats::client::RequestErrorKind;
    let request = async_nats::Request::new()
        .payload(body.into())
        .timeout(Some(wait));
    match client.send_request(subject, request).await {
        Ok(msg) => Ok(msg.payload),
        Err(err) => Err(match err.kind() {
            RequestErrorKind::TimedOut => RequestError::Timeout,
            RequestErrorKind::NoResponders => RequestError::NoResponder,
            _ => RequestError::Other(err.to_string()),
        }),
    }
}

fn snapshot(snap: md::BookSnapshot) -> Snapshot {
    Snapshot {
        uid: snap.uid,
        conn_epoch: snap.conn_epoch,
        seq: snap.seq,
        bids: levels(&snap.bids),
        asks: levels(&snap.asks),
    }
}

fn levels(levels: &[md::Level]) -> Vec<Level> {
    levels
        .iter()
        .map(|level| Level {
            price: level.price,
            size: level.qty,
        })
        .collect()
}

/// Go's `asSDK` for mdpath errors.
fn sdk_error(err: mdpath::Error) -> Error {
    match err {
        mdpath::Error::NoFeed => Error::NoFeed,
        mdpath::Error::UnknownUid => Error::UnknownUid,
        mdpath::Error::Stopping => Error::Stopping,
        other => Error::Other(other.to_string()),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
