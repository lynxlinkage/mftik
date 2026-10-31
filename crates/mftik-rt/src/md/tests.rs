//! The MD path against a real nats-server and a fake MD peer in this process:
//! the peer publishes `md.f` envelopes and answers `md.view` and `md.fetch`.
//! The dispatcher runs its loop on a thread of its own, as in a session.
//!
//! nats-server comes from `MFTIK_NATS_SERVER`, else `PATH`; a test that
//! cannot find it fails (E03-D056). These tests need no auth, so the server
//! runs without it.

use std::collections::VecDeque;
use std::sync::{Condvar, OnceLock, mpsc};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use mftik_core::dispatch;
use mftik_core::eventlog::Recorder;
use mftik_core::mdpath::{Decoded, Hooks, ReadError};
use mftik_core::symbol::{Category, Info, PRICE_TICK, Status, UniversalTicker};
use mftik_proto::wire::mftiko::md::v1::{feed_event, fetch_reply, view_reply};
use mftik_proto::wire::mftiko::stsctl::v1::{FeedBinding, SelectorBinding};
use mftik_testkit::NatsServer;
use prost::Message;

use super::*;
use crate::eventlog::{Record, Scratch};

const FUSE: Duration = Duration::from_secs(10);
const UID: u32 = 7;
const BOOK_SUBJECT: &str = "md.v1.f.bybit1.orderbook_delta.Bybit_Perp_BTCUSDT";
const BOOK_VIEW: &str = "md.v1.view.bybit1.orderbook_delta.Bybit_Perp_BTCUSDT";
const TRADE_SUBJECT: &str = "md.v1.f.bybit1.trade.Bybit_Perp_BTCUSDT";
const TRADE_VIEW: &str = "md.v1.view.bybit1.trade.Bybit_Perp_BTCUSDT";
const FETCH: &str = "md.v1.fetch.Bybit";
const DISPATCH_THREAD: &str = "md-test-dispatch";

// ---------- nats-server ----------

/// nats-server without auth, on a client port it picks.
fn server() -> NatsServer {
    NatsServer::start(["-a", "127.0.0.1", "-p", "-1"])
}

// ---------- the rig ----------

/// What the strategy side saw, in dispatcher order.
#[derive(Debug)]
enum Seen {
    Md(mdpath::Event),
    /// OnBook with the best bid and ask in the view at that moment.
    Book(bookpath::Update, Option<(f64, f64)>),
    Log(String),
    /// A read's result as text, and whether it ran on the dispatcher.
    Read(String, bool),
}

/// Holds the feed callback after it has reported a message, while `held`.
#[derive(Default)]
struct Gate {
    held: Mutex<bool>,
    cv: Condvar,
}

impl Gate {
    fn hold(&self) {
        *lock(&self.held) = true;
    }

    fn release(&self) {
        *lock(&self.held) = false;
        self.cv.notify_all();
    }

    fn wait(&self) {
        let mut held = lock(&self.held);
        while *held {
            held = self.cv.wait(held).unwrap_or_else(PoisonError::into_inner);
        }
    }
}

struct Rig {
    md: Arc<Md>,
    disp: Dispatcher,
    io: Io,
    peer: async_nats::Client,
    rows: Arc<Rows>,
    applied: watch::Receiver<Option<Version>>,
    seen: mpsc::Receiver<Seen>,
    tx: mpsc::Sender<Seen>,
    gate: Arc<Gate>,
    _server: NatsServer,
}

struct Opts {
    recv_window: Duration,
    ring_bytes: usize,
    hooks: Hooks,
    rec: Option<Arc<dyn Recorder>>,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            recv_window: Duration::from_secs(5),
            ring_bytes: 0,
            hooks: Hooks {
                trade: true,
                ..Hooks::default()
            },
            rec: None,
        }
    }
}

/// Best bid and ask of a synced book. On the dispatcher thread only (E03-D067).
fn view_of(md: &Md, uid: u32) -> Option<(f64, f64)> {
    let ob = md.book(uid)?;
    Some((ob.price(ob.best_bid_tick()?), ob.price(ob.best_ask_tick()?)))
}

/// [`view_of`] from the test thread: asked on the dispatcher thread, as a TD
/// entry, since the books belong to that thread.
fn view_from_test(md: &Arc<Md>, uid: u32) -> Option<(f64, f64)> {
    let (tx, rx) = mpsc::channel();
    let on_dispatcher = Arc::clone(md);
    md.disp
        .push_td(move || {
            let _ = tx.send(view_of(&on_dispatcher, uid));
        })
        .ok()?;
    rx.recv_timeout(Duration::from_secs(5)).ok().flatten()
}

impl Rig {
    fn new(opts: Opts) -> Rig {
        let server = server();
        let io = Io::start().expect("io thread");
        let clock = Clock::new();
        let timers = Timers::new(io.clone(), clock);
        let url = server.url().to_owned();
        let client = io
            .block_on(async move { async_nats::connect(url).await })
            .expect("session client");
        let url = server.url().to_owned();
        let peer = io
            .block_on(async move { async_nats::connect(url).await })
            .expect("peer client");
        let (tx, seen) = mpsc::channel();
        let gate = Arc::new(Gate::default());
        let slot: Arc<OnceLock<Arc<Md>>> = Arc::default();
        let hooks = opts.hooks;
        let (feed_tx, feed_gate) = (tx.clone(), Arc::clone(&gate));
        let book_slot = Arc::clone(&slot);
        let log_tx = Mutex::new(tx.clone());
        let disp = Dispatcher::new(dispatch::Config {
            recv_window: opts.recv_window,
            now_ns: clock.now_ns(),
            ring_bytes: opts.ring_bytes,
            feed: Some(Box::new(move |msg| {
                match hooks.decode(msg) {
                    Decoded::Hook(event) => {
                        let _ = feed_tx.send(Seen::Md(event));
                    }
                    Decoded::Log(line) => {
                        let _ = feed_tx.send(Seen::Log(line));
                    }
                    Decoded::Skip => {}
                }
                feed_gate.wait();
            })),
            book: Some(Box::new(move |msg| {
                if let Some(md) = book_slot.get() {
                    md.on_book_message(msg);
                }
            })),
            log: Some(Arc::new(move |line: &str| {
                let _ = lock(&log_tx).send(Seen::Log(line.to_owned()));
            })),
            rec: opts.rec,
            ..Default::default()
        });
        let rows = Arc::new(Rows::default());
        let (applied_tx, applied) = watch::channel(None);
        let (book_tx, view_slot) = (Mutex::new(tx.clone()), Arc::clone(&slot));
        let md_log = Mutex::new(tx.clone());
        let md = Md::open(
            Config {
                io: io.clone(),
                clock,
                timers,
                disp: disp.clone(),
                nats: client,
                recv_window: opts.recv_window,
                hooks,
                on_book: Arc::new(move |update| {
                    let view = view_slot.get().and_then(|md| view_of(md, update.uid));
                    let _ = lock(&book_tx).send(Seen::Book(update, view));
                }),
                log: Arc::new(move |line| {
                    let _ = lock(&md_log).send(Seen::Log(line.to_owned()));
                }),
            },
            Arc::clone(&rows),
            applied_tx,
        )
        .expect("md path");
        let _ = slot.set(Arc::clone(&md));
        let driven = disp.clone();
        thread::Builder::new()
            .name(DISPATCH_THREAD.into())
            .spawn(move || driven.drive())
            .expect("dispatcher thread");
        Rig {
            md,
            disp,
            io,
            peer,
            rows,
            applied,
            seen,
            tx,
            gate,
            _server: server,
        }
    }

    fn bind(&self, seq: u64, feeds: &[(&str, &str)]) -> Result<(), String> {
        self.md.apply(&bindings(seq, feeds))
    }

    fn publish(&self, subject: &str, msg: Vec<u8>) {
        let peer = self.peer.clone();
        let subject = subject.to_owned();
        let sent = self.io.block_on(async move {
            peer.publish(subject, msg.into()).await.is_ok() && peer.flush().await.is_ok()
        });
        assert!(sent, "publish");
    }

    /// Answers requests on `subject` with the script, in order: `None`, or
    /// an empty script, is no answer. Returns the request bodies it saw.
    fn respond(&self, subject: &str, script: Script) -> Arc<Mutex<Vec<Vec<u8>>>> {
        let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let peer = self.peer.clone();
        let subject = subject.to_owned();
        let mut sub = self.io.block_on({
            let peer = peer.clone();
            async move {
                let sub = peer.subscribe(subject).await.expect("responder");
                flush(&peer).await.expect("responder flush");
                sub
            }
        });
        let seen = Arc::clone(&bodies);
        self.io.spawn(async move {
            while let Some(msg) = sub.next().await {
                lock(&seen).push(msg.payload.to_vec());
                let answer = lock(&script).pop_front().flatten();
                if let (Some(answer), Some(reply)) = (answer, msg.reply) {
                    let _ = peer.publish(reply, answer.into()).await;
                }
            }
        });
        bodies
    }

    /// The next thing seen.
    #[track_caller]
    fn next(&self) -> Seen {
        self.seen.recv_timeout(FUSE).expect("nothing seen in time")
    }

    /// Skips until `f` takes one; returns it and what it skipped.
    #[track_caller]
    fn until<T>(&self, mut f: impl FnMut(&Seen) -> Option<T>) -> (T, Vec<Seen>) {
        let mut skipped = Vec::new();
        loop {
            let seen = self.next();
            if let Some(got) = f(&seen) {
                return (got, skipped);
            }
            skipped.push(seen);
        }
    }

    #[track_caller]
    fn wait_for(&self, what: &str, mut ok: impl FnMut() -> bool) {
        let start = Instant::now();
        while !ok() {
            assert!(start.elapsed() < FUSE, "timed out waiting for {what}");
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// A read's callback that reports on the rig.
    fn reader<T: fmt::Debug + Send + 'static>(&self) -> Deliver<T> {
        let tx = Mutex::new(self.tx.clone());
        Box::new(move |result: Result<T, ReadError>| {
            let on_disp = thread::current().name() == Some(DISPATCH_THREAD);
            let text = match result {
                Ok(value) => format!("ok {value:?}"),
                Err(err) => format!("err {err}"),
            };
            let _ = lock(&tx).send(Seen::Read(text, on_disp));
        })
    }

    #[track_caller]
    fn read(&self) -> (String, bool) {
        self.until(|seen| match seen {
            Seen::Read(text, on_disp) => Some((text.clone(), *on_disp)),
            _ => None,
        })
        .0
    }
}

type Script = Arc<Mutex<VecDeque<Option<Vec<u8>>>>>;

fn script(answers: impl IntoIterator<Item = Option<Vec<u8>>>) -> Script {
    Arc::new(Mutex::new(answers.into_iter().collect()))
}

fn bindings(seq: u64, feeds: &[(&str, &str)]) -> Bindings {
    Bindings {
        version: Some(Version { epoch: 1, seq }),
        feeds: feeds
            .iter()
            .map(|(feed, subject)| FeedBinding {
                feed: (*feed).to_owned(),
                subject: (*subject).to_owned(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn info(uid: u32, symbol: &str, tick: f64) -> Info {
    Info {
        uid,
        ticker: UniversalTicker {
            venue: "Bybit".into(),
            category: Category::Perp,
            symbol: symbol.into(),
        },
        exch_ticker: symbol.into(),
        base: "BTC".into(),
        quote: "USDT".into(),
        contract_size: 1.0,
        contract_value_ccy: "BTC".into(),
        price_ccy: "USDT".into(),
        settle: "USDT".into(),
        expiry_ns: 0,
        strike: 0.0,
        option_type: String::new(),
        status: Status::Tradable,
        filters: [(PRICE_TICK.to_owned(), tick)].into_iter().collect(),
        updated_at_ns: 1,
    }
}

fn wall_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos() as i64
}

/// A v1 `md.f` envelope (PROTOCOL §7.1).
fn frame(md_ts: i64, epoch: u64, seq: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 40 + payload.len()];
    out[0] = 1;
    out[8..16].copy_from_slice(&md_ts.to_le_bytes());
    out[16..24].copy_from_slice(&epoch.to_le_bytes());
    out[24..32].copy_from_slice(&seq.to_le_bytes());
    out[40..].copy_from_slice(payload);
    out
}

fn trade(seq: u64, md_ts: i64, id: &str) -> Vec<u8> {
    let event = md::FeedEvent {
        uid: UID,
        event: Some(feed_event::Event::Trade(md::Trade {
            trade_id: id.into(),
            price: 100.0,
            qty: 1.0,
            ..Default::default()
        })),
    };
    frame(md_ts, 1, seq, &event.encode_to_vec())
}

fn lv(levels: &[(f64, f64)]) -> Vec<md::Level> {
    levels
        .iter()
        .map(|&(price, qty)| md::Level { price, qty })
        .collect()
}

fn delta(uid: u32, seq: u64, bids: &[(f64, f64)], asks: &[(f64, f64)], reset: bool) -> Vec<u8> {
    let event = md::FeedEvent {
        uid,
        event: Some(feed_event::Event::OrderBookDelta(md::OrderBookDelta {
            meta: Some(md::Meta { exch_ts_ns: 42 }),
            bids: lv(bids),
            asks: lv(asks),
            reset,
        })),
    };
    frame(wall_ns(), 1, seq, &event.encode_to_vec())
}

fn book_reply(uid: u32, seq: u64, bids: &[(f64, f64)], asks: &[(f64, f64)]) -> Option<Vec<u8>> {
    Some(
        md::ViewReply {
            result: Some(view_reply::Result::Book(md::BookSnapshot {
                bids: lv(bids),
                asks: lv(asks),
                conn_epoch: 1,
                seq,
                uid,
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

fn view_error(error: &str) -> Option<Vec<u8>> {
    Some(
        md::ViewReply {
            error: error.into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

fn md_seq(seen: &Seen) -> Option<u64> {
    match seen {
        Seen::Md(event) => Some(event.md.seq),
        _ => None,
    }
}

fn book_seq(seen: &Seen) -> Option<(u64, Option<(f64, f64)>)> {
    match seen {
        Seen::Book(update, view) => Some((update.seq, *view)),
        _ => None,
    }
}

fn logs(seen: &[Seen]) -> Vec<&str> {
    seen.iter()
        .filter_map(|s| match s {
            Seen::Log(line) => Some(line.as_str()),
            _ => None,
        })
        .collect()
}

// ---------- bindings ----------

#[test]
fn bindings_subscribe_before_they_bind_and_unsubscribe_after() {
    let rig = Rig::new(Opts::default());
    let trade_s1 = "md.v1.f.a.trade.Bybit_Perp_BTCUSDT";
    let trade_s2 = "md.v1.f.b.trade.Bybit_Perp_BTCUSDT";
    let ticker_s3 = "md.v1.f.b.ticker.Bybit_Perp_BTCUSDT";
    let key = "trade.Bybit_Perp_BTCUSDT";
    let mut applied = rig.applied.clone();
    assert_eq!(rig.md.bindings_applied(), None);

    rig.bind(1, &[(key, trade_s1)]).expect("v1");
    assert!(applied.has_changed().expect("watch"));
    assert_eq!(
        *applied.borrow_and_update(),
        Some(Version { epoch: 1, seq: 1 })
    );
    rig.publish(trade_s1, trade(1, wall_ns(), "t1"));
    assert_eq!(rig.until(md_seq).0, 1);

    // Not newer: success, nothing subscribed, nothing to report.
    rig.bind(1, &[(key, trade_s2)]).expect("v1 again");
    assert!(!applied.has_changed().expect("watch"));

    // The feed moves to s2; s1 is unsubscribed once v2 is bound.
    rig.bind(
        2,
        &[(key, trade_s2), ("ticker.Bybit_Perp_BTCUSDT", ticker_s3)],
    )
    .expect("v2");
    assert_eq!(
        *applied.borrow_and_update(),
        Some(Version { epoch: 1, seq: 2 })
    );
    rig.publish(trade_s1, trade(2, wall_ns(), "left"));
    // ticker has no hook: skipped before decoding (E08-D005).
    rig.publish(ticker_s3, trade(3, wall_ns(), "no hook"));
    rig.publish(trade_s2, trade(4, wall_ns(), "t4"));
    let (seq, skipped) = rig.until(md_seq);
    assert_eq!(seq, 4);
    assert!(skipped.iter().all(|s| md_seq(s).is_none()), "{skipped:?}");

    // A subscribe NATS refuses aborts the stage: set and version stay.
    let err = rig
        .bind(3, &[(key, trade_s2), ("aggtrade.X", "md.v1.f.bad subject")])
        .expect_err("bad subject");
    assert!(
        err.starts_with("mdpath: subscribe md.v1.f.bad subject"),
        "{err}"
    );
    assert_eq!(
        rig.md.bindings_applied(),
        Some(Version { epoch: 1, seq: 2 })
    );
    assert!(!applied.has_changed().expect("watch"));
    // A selector is refused whatever its version (E08-D022).
    let mut selector = bindings(9, &[(key, trade_s2)]);
    selector.selectors.push(SelectorBinding::default());
    assert_eq!(
        rig.md.apply(&selector),
        Err("mdpath: selectors are not supported".into())
    );
    // The bound set still delivers, and the next apply is not blocked.
    rig.publish(trade_s2, trade(5, wall_ns(), "t5"));
    assert_eq!(rig.until(md_seq).0, 5);
    rig.bind(4, &[(key, trade_s1)]).expect("v4");
    rig.publish(trade_s2, trade(6, wall_ns(), "left"));
    rig.publish(trade_s1, trade(7, wall_ns(), "t7"));
    assert_eq!(rig.until(md_seq).0, 7);
}

// ---------- md.f into the dispatcher ----------

#[test]
fn the_prehook_drops_stale_messages_and_bad_envelopes_are_counted() {
    let rig = Rig::new(Opts {
        recv_window: Duration::from_millis(200),
        ..Opts::default()
    });
    rig.bind(1, &[("trade.Bybit_Perp_BTCUSDT", TRADE_SUBJECT)])
        .expect("bind");
    let stale = wall_ns() - 2_000_000_000;
    rig.publish(TRADE_SUBJECT, trade(1, stale, "stale"));
    rig.publish(TRADE_SUBJECT, trade(2, wall_ns(), "fresh"));
    rig.publish(TRADE_SUBJECT, vec![1, 0, 0]);
    let mut version = trade(3, wall_ns(), "v2");
    version[0] = 2;
    rig.publish(TRADE_SUBJECT, version);
    rig.publish(TRADE_SUBJECT, frame(wall_ns(), 1, 4, b"\xff\xff\xff"));
    rig.publish(TRADE_SUBJECT, trade(5, stale, "stale"));
    rig.publish(TRADE_SUBJECT, trade(6, wall_ns(), "fresh"));
    let (first, before) = rig.until(md_seq);
    let (last, between) = rig.until(md_seq);
    assert_eq!((first, last), (2, 6));
    let lines: Vec<&str> = logs(&before).into_iter().chain(logs(&between)).collect();
    assert_eq!(
        lines
            .iter()
            .filter(|l| l.starts_with("md envelope rejected"))
            .count(),
        2,
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.starts_with("md payload rejected")),
        "{lines:?}"
    );
    // Two stale and two bad envelopes; a bad payload is not a drop (E08-D049).
    assert_eq!(rig.disp.stats().md.dropped, 4);
}

#[test]
fn a_full_ring_drops_the_oldest_messages() {
    let rig = Rig::new(Opts {
        ring_bytes: 4096,
        ..Opts::default()
    });
    rig.bind(1, &[("trade.Bybit_Perp_BTCUSDT", TRADE_SUBJECT)])
        .expect("bind");
    let pad = "x".repeat(150);
    rig.gate.hold();
    rig.publish(TRADE_SUBJECT, trade(1, wall_ns(), &pad));
    assert_eq!(rig.until(md_seq).0, 1);
    // The feed callback now holds message 1; the rest pile up in the ring.
    const LAST: u64 = 60;
    for seq in 2..=LAST {
        rig.publish(TRADE_SUBJECT, trade(seq, wall_ns(), &pad));
    }
    rig.wait_for("every message pushed", || {
        let stat = rig.disp.stats().md;
        u64::from(stat.depth) + stat.dropped == LAST - 1
    });
    let dropped = rig.disp.stats().md.dropped;
    assert!(dropped > 0, "the ring held everything");
    rig.gate.release();
    let mut got = Vec::new();
    while got.last() != Some(&LAST) {
        got.push(rig.until(md_seq).0);
    }
    // The newest survive, in order, and the oldest are the ones dropped.
    let first = LAST - got.len() as u64 + 1;
    assert_eq!(got, (first..=LAST).collect::<Vec<_>>());
    assert_eq!(first - 2, dropped);
}

// ---------- the book ----------

#[test]
fn the_book_syncs_from_md_view_and_resyncs_after_a_gap_and_a_reset() {
    let rig = Rig::new(Opts::default());
    rig.rows.stored(&info(UID, "BTCUSDT", 0.5));
    let answers = script([
        view_error("not_synced"),
        book_reply(UID, 10, &[(100.0, 1.0)], &[(101.0, 2.0)]),
    ]);
    let asked = rig.respond(BOOK_VIEW, Arc::clone(&answers));
    rig.bind(1, &[("orderbook_delta.Bybit_Perp_BTCUSDT", BOOK_SUBJECT)])
        .expect("bind");
    let requests = || lock(&asked).len();

    // The first delta asks md.view; not_synced backs off 1 s; the retry's
    // snapshot (seq 10) folds the buffered delta 11, which is not a hook.
    rig.publish(BOOK_SUBJECT, delta(UID, 11, &[(100.5, 3.0)], &[], false));
    let (_, _) = rig.until(|s| match s {
        Seen::Log(line) if line == "book: uid 7: mdpath: md.view: not_synced" => Some(()),
        _ => None,
    });
    let started = Instant::now();
    rig.wait_for("synced", || {
        view_from_test(&rig.md, UID) == Some((100.5, 101.0))
    });
    assert!(started.elapsed() >= Duration::from_millis(900));
    assert_eq!(requests(), 2);
    assert_eq!(
        md::ViewRequest::decode(lock(&asked)[0].as_slice()).expect("view request"),
        md::ViewRequest::default()
    );
    rig.publish(
        BOOK_SUBJECT,
        delta(UID, 12, &[], &[(101.0, 0.0), (102.0, 1.0)], false),
    );
    let ((seq, view), skipped) = rig.until(book_seq);
    assert_eq!((seq, view), (12, Some((100.5, 102.0))));
    assert!(skipped.iter().all(|s| book_seq(s).is_none()));

    // A gap: unsynced, asked again, synced at the snapshot's cursor.
    lock(&answers).push_back(book_reply(UID, 20, &[(99.0, 1.0)], &[(103.0, 1.0)]));
    rig.publish(BOOK_SUBJECT, delta(UID, 14, &[(1.0, 1.0)], &[], false));
    rig.wait_for("resynced", || {
        requests() == 3 && view_from_test(&rig.md, UID) == Some((99.0, 103.0))
    });
    rig.publish(BOOK_SUBJECT, delta(UID, 21, &[(99.5, 1.0)], &[], false));
    let ((seq, view), skipped) = rig.until(book_seq);
    assert_eq!((seq, view), (21, Some((99.5, 103.0))));
    assert!(skipped.iter().all(|s| book_seq(s).is_none()), "{skipped:?}");

    // A reset drops the book without applying its levels (E08-D061).
    lock(&answers).push_back(book_reply(UID, 30, &[(98.0, 1.0)], &[(104.0, 1.0)]));
    rig.publish(BOOK_SUBJECT, delta(UID, 22, &[(1.0, 1.0)], &[], true));
    rig.wait_for("reset", || {
        requests() == 4 && view_from_test(&rig.md, UID) == Some((98.0, 104.0))
    });
    rig.publish(
        BOOK_SUBJECT,
        delta(UID, 31, &[], &[(104.0, 0.0), (105.0, 2.0)], false),
    );
    let ((seq, view), skipped) = rig.until(book_seq);
    assert_eq!((seq, view), (31, Some((98.0, 105.0))));
    assert!(skipped.iter().all(|s| book_seq(s).is_none()), "{skipped:?}");
}

#[test]
fn the_book_waits_for_a_stored_row_and_a_failed_view_backs_off() {
    let rig = Rig::new(Opts {
        recv_window: Duration::from_millis(300),
        ..Opts::default()
    });
    let uid = 8;
    let feed = "orderbook_delta.Bybit_Perp_ETHUSDT";
    let subject = "md.v1.f.bybit1.orderbook_delta.Bybit_Perp_ETHUSDT";
    // No answer at first: the view times out after recv_window.
    let answers = script([None]);
    let asked = rig.respond(
        "md.v1.view.bybit1.orderbook_delta.Bybit_Perp_ETHUSDT",
        Arc::clone(&answers),
    );
    rig.bind(1, &[(feed, subject)]).expect("bind");
    // Not resolved: stays unsynced, logged once, md.view not asked (§9.5.7).
    rig.publish(
        subject,
        delta(uid, 1, &[(10.0, 1.0)], &[(11.0, 1.0)], false),
    );
    rig.until(|s| match s {
        Seen::Log(line) if line == "book: uid 8 is not resolved" => Some(()),
        _ => None,
    });
    assert!(lock(&asked).is_empty());

    // The SYM path stores the row (E03-08's hook point); the next delta asks.
    rig.rows.stored(&info(uid, "ETHUSDT", 0.01));
    lock(&answers).push_back(book_reply(uid, 3, &[(10.0, 2.0)], &[(11.0, 2.0)]));
    rig.publish(subject, delta(uid, 2, &[(10.0, 1.0)], &[], false));
    rig.until(|s| match s {
        Seen::Log(line) if line == "book: uid 8: mdpath: md.view: context deadline exceeded" => {
            Some(())
        }
        _ => None,
    });
    rig.wait_for("synced after the backoff", || {
        view_from_test(&rig.md, uid) == Some((10.0, 11.0))
    });
    assert_eq!(lock(&asked).len(), 2);
    rig.publish(subject, delta(uid, 4, &[(10.01, 1.0)], &[], false));
    assert_eq!(rig.until(book_seq).0, (4, Some((10.01, 11.0))));
}

// ---------- the event log ----------

/// One line per frame: `rec_seq`, kind, `ref` and `flags` when set, and the
/// envelope seq of an MD message.
fn describe(record: &Record) -> String {
    let mut line = format!("{} {}", record.seq, record.kind.name().unwrap_or("?"));
    if record.r#ref != 0 {
        line += &format!(" ref={}", record.r#ref);
    }
    if record.flags != 0 {
        line += &format!(" flags={}", record.flags);
    }
    if matches!(record.kind, Kind::MD_FEED | Kind::MD_BOOK) {
        let seq = u64::from_le_bytes(record.body[24..32].try_into().expect("envelope"));
        line += &format!(" seq={seq}");
    }
    line
}

#[test]
fn md_messages_and_book_timers_are_written_before_their_hooks() {
    let log = Scratch::open();
    let rig = Rig::new(Opts {
        rec: Some(log.recorder()),
        ..Opts::default()
    });
    rig.rows.stored(&info(UID, "BTCUSDT", 0.5));
    let answers = script([
        view_error("not_synced"),
        book_reply(UID, 10, &[(100.0, 1.0)], &[(101.0, 2.0)]),
    ]);
    rig.respond(BOOK_VIEW, answers);
    rig.bind(
        1,
        &[
            ("orderbook_delta.Bybit_Perp_BTCUSDT", BOOK_SUBJECT),
            ("trade.Bybit_Perp_BTCUSDT", TRADE_SUBJECT),
        ],
    )
    .expect("bind");

    // Delta 11 starts a buffer, whose age timer is 1, and asks md.view.
    // not_synced arms the backoff, timer 2, and its run asks again.
    let buffered = delta(UID, 11, &[(100.5, 3.0)], &[], false);
    rig.publish(BOOK_SUBJECT, buffered.clone());
    rig.wait_for("synced", || {
        view_from_test(&rig.md, UID) == Some((100.5, 101.0))
    });
    let hooked = delta(UID, 12, &[], &[(101.0, 0.0), (102.0, 1.0)], false);
    rig.publish(BOOK_SUBJECT, hooked.clone());
    assert_eq!(rig.until(book_seq).0, (12, Some((100.5, 102.0))));
    let traded = trade(1, wall_ns(), "t1");
    rig.publish(TRADE_SUBJECT, traded.clone());
    assert_eq!(rig.until(md_seq).0, 1);

    // The md.view replies are reads: not written here.
    let frames = log.close();
    assert_eq!(
        frames.iter().map(describe).collect::<Vec<_>>(),
        [
            "1 md_book seq=11",
            "2 hook_end ref=1",
            "3 timer ref=2",
            "4 hook_end ref=3",
            "5 md_book seq=12",
            "6 hook_end ref=5",
            "7 md_feed seq=1",
            "8 hook_end ref=7",
            "9 end",
        ]
    );
    assert_eq!(frames[0].body, buffered);
    assert!(frames[2].body.is_empty());
    assert_eq!(frames[4].body, hooked);
    assert_eq!(frames[6].body, traded);
    // recv is when the message reached the ring or the timer fired; t is
    // when the hook started.
    for pair in frames[..8].chunks(2) {
        let (input, end) = (&pair[0], &pair[1]);
        assert!(0 < input.recv_ns && input.recv_ns <= input.t_ns);
        assert!(input.t_ns <= end.t_ns);
    }
    assert!(frames[2].t_ns - frames[0].t_ns >= 900_000_000);
}

// ---------- reads ----------

#[test]
fn reads_answer_on_the_td_buffer_with_timeouts_and_missing_responders() {
    let window = Duration::from_millis(300);
    let rig = Rig::new(Opts {
        recv_window: window,
        ..Opts::default()
    });
    rig.rows.stored(&info(UID, "BTCUSDT", 0.5));
    rig.bind(
        1,
        &[
            ("trade.Bybit_Perp_BTCUSDT", TRADE_SUBJECT),
            ("orderbook_delta.Bybit_Perp_BTCUSDT", BOOK_SUBJECT),
        ],
    )
    .expect("bind");

    // Refused before any request, with Go's sdk errors.
    let query = TapeQuery::default();
    assert_eq!(
        rig.md.tape(99, "trade", query, rig.reader()),
        Err(Error::UnknownUid)
    );
    assert_eq!(
        rig.md.tape(UID, "kline_1m", query, rig.reader()),
        Err(Error::NoFeed)
    );
    assert_eq!(
        rig.md.tape(UID, "", query, rig.reader()),
        Err(Error::NoFeed)
    );
    assert_eq!(
        rig.md.fetch_klines(99, "1m", 10, rig.reader()),
        Err(Error::UnknownUid)
    );
    assert_eq!(
        rig.md.fetch_open_interest(99, rig.reader()),
        Err(Error::UnknownUid)
    );

    // No responder on md.fetch.
    rig.md
        .fetch_klines(UID, "1m", 10, rig.reader())
        .expect("klines");
    assert_eq!(
        rig.read(),
        (
            "err mdpath: md.fetch: nats: no responders available for request".into(),
            true
        )
    );

    // A responder that does not answer: the wait is recv_window.
    let book = rig.respond(BOOK_VIEW, script([None]));
    let started = Instant::now();
    rig.md.book_snapshot(UID, rig.reader()).expect("book");
    assert_eq!(
        rig.read(),
        (
            "err mdpath: md.view: context deadline exceeded".into(),
            true
        )
    );
    let took = started.elapsed();
    assert!(took >= window - Duration::from_millis(20), "{took:?}");
    assert!(took < window * 4, "{took:?}");
    assert_eq!(lock(&book).len(), 1);

    // Answers, and a reply that carries an error.
    let page = md::TapePage {
        more: false,
        head: Some(md::Cursor {
            conn_epoch: 1,
            seq: 9,
        }),
        ..Default::default()
    };
    let tape = rig.respond(
        TRADE_VIEW,
        script([Some(
            md::ViewReply {
                result: Some(view_reply::Result::Tape(page.clone())),
                ..Default::default()
            }
            .encode_to_vec(),
        )]),
    );
    let query = TapeQuery {
        limit: 5,
        before: None,
        since_ns: 77,
    };
    rig.md
        .tape(UID, "trade", query, rig.reader())
        .expect("tape");
    assert_eq!(rig.read(), (format!("ok {page:?}"), true));
    assert_eq!(
        md::ViewRequest::decode(lock(&tape)[0].as_slice()).expect("view request"),
        md::ViewRequest {
            limit: 5,
            before: None,
            since_ns: Some(77),
        }
    );

    let oi = md::OpenInterest {
        open_interest: 12.5,
        ..Default::default()
    };
    let fetch = rig.respond(
        FETCH,
        script([
            Some(
                md::FetchReply {
                    uid: UID,
                    result: Some(fetch_reply::Result::OpenInterest(oi)),
                    ..Default::default()
                }
                .encode_to_vec(),
            ),
            Some(
                md::FetchReply {
                    error: "invalid".into(),
                    reason: "limit".into(),
                    uid: UID,
                    ..Default::default()
                }
                .encode_to_vec(),
            ),
        ]),
    );
    rig.md
        .fetch_open_interest(UID, rig.reader())
        .expect("open interest");
    assert_eq!(rig.read(), (format!("ok {oi:?}"), true));
    rig.md
        .fetch_klines(UID, "1m", 6000, rig.reader())
        .expect("klines");
    assert_eq!(
        rig.read(),
        ("err mdpath: md.fetch: invalid: limit".into(), true)
    );
    let bodies = lock(&fetch).clone();
    assert_eq!(bodies.len(), 2);
    assert_eq!(
        md::FetchRequest::decode(bodies[1].as_slice()).expect("fetch request"),
        md::FetchRequest {
            req: Some(md::fetch_request::Req::Klines(md::FetchKlines {
                interval: "1m".into(),
                limit: 6000,
                uid: UID,
            })),
        }
    );
    // Nothing else came back: one delivery per read.
    assert!(rig.seen.recv_timeout(Duration::from_millis(100)).is_err());
}
