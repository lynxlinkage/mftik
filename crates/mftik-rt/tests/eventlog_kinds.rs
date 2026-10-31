//! One whole session's event log, as Go `mftiko-eventlog dump` prints it
//! (PROTOCOL §12, E07-D010, E07-D011): every kind is in it, each order call
//! pairs with its publish and its reply and each read with its reply by id,
//! every input has its `hook_end`, and no NATS credential is anywhere in it.
//!
//! The worker is this binary running [`Every`]. It resolves in OnInit, reads
//! in OnStart, submits twice in OnRecon, and exits once each callback and
//! hook it waits for has run. The test is the platform: the fake shim sends
//! a Command and a Live with bindings, and a peer on a NATS connection of its
//! own is SYM, MD, and TD. TD refuses both orders, so OnOrder shows each
//! reply was taken before the exit. The book syncs from its first delta,
//! with a snapshot read of the md path's own; the strategy then asks for the
//! open interest, and the peer publishes the next delta before it answers,
//! so OnBook runs. The binary comes from `MFTIK_EVENTLOG`, else PATH.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::{Client, Subject};
use futures_util::StreamExt;
use mftik_core::symbol::PRICE_TICK;
use mftik_core::tdpath::evt_subject;
use mftik_proto::wire::mftiko::common::v1::{self as common, Outcome, Version};
use mftik_proto::wire::mftiko::md::v1::{self as md, feed_event, fetch_reply, view_reply};
use mftik_proto::wire::mftiko::session::v1::{AccountDecl, EventLog};
use mftik_proto::wire::mftiko::stsctl::v1::{Bindings, Command, FeedBinding, command};
use mftik_proto::wire::mftiko::sym::v1::{self as sym, reply, request};
use mftik_proto::wire::mftiko::td::v1 as td;
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::eventlog;
use mftik_rt::io::Io;
use mftik_rt::{
    BookHandler, BookUpdate, Context, Handlers, HookError, Order, OrderEvent, OrderHandler,
    PositionEvent, PositionHandler, ReconEvent, ReconHandler, Strategy, TapeQuery, Trade,
    TradeHandler, topic,
};
use mftik_testkit::*;
use prost::Message;
use serde_json::Value;

const TICKER: &str = "Bybit_Spot_BTCUSDT";
const MISSING: &str = "Bybit_Spot_NOPE";
const UID: u32 = 7;
const TICK: f64 = 0.5;
const API: i64 = 3;

const TRADE: &str = "trade.Bybit_Spot_BTCUSDT";
const BOOK: &str = "orderbook_delta.Bybit_Spot_BTCUSDT";
const TRADE_SUBJECT: &str = "md.v1.f.bybit1.trade.Bybit_Spot_BTCUSDT";
const TRADE_VIEW: &str = "md.v1.view.bybit1.trade.Bybit_Spot_BTCUSDT";
const BOOK_SUBJECT: &str = "md.v1.f.bybit1.orderbook_delta.Bybit_Spot_BTCUSDT";
const BOOK_VIEW: &str = "md.v1.view.bybit1.orderbook_delta.Bybit_Spot_BTCUSDT";
const FETCH: &str = "md.v1.fetch.Bybit";

/// PROTOCOL §12.4's kinds, by number.
const KINDS: [&str; 19] = [
    "init",
    "start",
    "bindings",
    "resolve",
    "md_feed",
    "md_book",
    "td_event",
    "td_view",
    "order_reply",
    "read_reply",
    "timer",
    "post",
    "stop",
    "onstop",
    "hook_end",
    "order_call",
    "order_publish",
    "read_call",
    "end",
];

/// The kinds that are a hook's input: a `hook_end` refers to each.
const INPUTS: [&str; 11] = [
    "init",
    "start",
    "md_feed",
    "md_book",
    "td_event",
    "td_view",
    "order_reply",
    "read_reply",
    "timer",
    "post",
    "onstop",
];

// ---------- the worker side ----------

/// What [`Every`] waits for before it exits.
const WAITS: [&str; 11] = [
    "post",
    "timer",
    "tape",
    "klines",
    "open_interest",
    "book_snapshot",
    "recon",
    "orders",
    "position",
    "trade",
    "book",
];

/// How often [`Every::look`] looks at the book until it is synced.
const LOOK: Duration = Duration::from_millis(10);

#[derive(Default)]
struct Every {
    left: BTreeSet<&'static str>,
    refused: usize,
}

impl Every {
    /// `what` has run; the last one ends the session.
    fn done(&mut self, ctx: &Context<Self>, what: &'static str) {
        assert!(self.left.remove(what), "{what} ran twice");
        if self.left.is_empty() {
            ctx.exit();
        }
    }

    fn look(&mut self, ctx: &Context<Self>) {
        if ctx.book(UID).is_none() {
            ctx.after(LOOK, Self::look);
            return;
        }
        ctx.fetch_open_interest(UID, |s, ctx, oi| {
            oi.expect("open interest");
            s.done(ctx, "open_interest");
        })
        .expect("fetch_open_interest");
    }
}

impl Strategy for Every {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        let info = ctx.resolve(TICKER)?;
        if info.uid != UID {
            return Err(format!("uid {}", info.uid).into());
        }
        if ctx.resolve(MISSING).is_ok() {
            return Err("resolved a ticker SYM does not know".into());
        }
        self.left = WAITS.into_iter().collect();
        Ok(())
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        ctx.post(|s, ctx| s.done(ctx, "post"));
        ctx.after(Duration::ZERO, |s, ctx| s.done(ctx, "timer"));
        ctx.tape(UID, topic::TRADE, TapeQuery::default(), |s, ctx, page| {
            page.expect("tape");
            s.done(ctx, "tape");
        })?;
        ctx.fetch_klines(UID, "1m", 1, |s, ctx, klines| {
            klines.expect("klines");
            s.done(ctx, "klines");
        })?;
        ctx.after(LOOK, Self::look);
        Ok(())
    }

    fn on_stop(&mut self, _: &Context<Self>) {}

    fn handlers(&self, h: &mut Handlers<Self>) {
        h.trade().book().order().position().recon();
    }
}

impl ReconHandler for Every {
    fn on_recon(&mut self, ctx: &Context<Self>, _: ReconEvent) {
        for _ in 0..2 {
            ctx.submit(Order {
                api_id: API,
                algo_id: 1,
                uid: UID,
                side: common::Side::Buy,
                order_type: common::OrderType::Limit,
                tif: common::TimeInForce::Gtc,
                qty: Some(1.0),
                quote_qty: None,
                price: Some(100.0),
                recv_window_ms: 0,
                reduce_only: false,
                params: Default::default(),
            })
            .expect("submit");
        }
        self.done(ctx, "recon");
    }
}

impl OrderHandler for Every {
    fn on_order(&mut self, ctx: &Context<Self>, ev: OrderEvent) {
        assert_eq!(ev.order.status, td::OrderStatus::Rejected as i32, "{ev:?}");
        self.refused += 1;
        if self.refused == 2 {
            self.done(ctx, "orders");
        }
    }
}

impl PositionHandler for Every {
    fn on_position(&mut self, ctx: &Context<Self>, _: PositionEvent) {
        self.done(ctx, "position");
    }
}

impl TradeHandler for Every {
    fn on_trade(&mut self, ctx: &Context<Self>, _: Trade) {
        self.done(ctx, "trade");
    }
}

impl BookHandler for Every {
    fn on_book(&mut self, ctx: &Context<Self>, _: BookUpdate) {
        ctx.book_snapshot(UID, |s, ctx, snapshot| {
            snapshot.expect("book snapshot");
            s.done(ctx, "book_snapshot");
        })
        .expect("book_snapshot");
        self.done(ctx, "book");
    }
}

#[test]
fn worker_entry() {
    if env::var_os(WORKER_ENV).is_some() {
        mftik_rt::run(Every::default(), Some(TEST_RELEASE));
    }
}

// ---------- the peer: SYM, MD, and TD ----------

/// What the peer sends for one request, in order.
enum Out {
    /// To the request's reply inbox.
    Reply(Vec<u8>),
    Publish(String, Vec<u8>),
}

struct Peer {
    io: Io,
    client: Client,
}

impl Peer {
    fn start(nats: &Nats) -> Peer {
        let io = Io::start().expect("peer io");
        let url = nats.url.clone();
        let mut creds = nats.creds.clone();
        creds.inbox_prefix = "_INBOX_peer".into();
        let client = io
            .block_on(async move {
                mftik_rt::nats::dial(&url, &creds, Duration::from_secs(5), None).await
            })
            .expect("peer dials");
        let peer = Peer { io, client };
        peer.serve("sym", |req| vec![Out::Reply(sym_answer(req))]);
        peer.serve(TRADE_VIEW, |_| vec![Out::Reply(tape_page())]);
        peer.serve(BOOK_VIEW, |_| vec![Out::Reply(book_body())]);
        peer.serve(FETCH, fetch_answer);
        peer.serve("td.view.*", |_| vec![Out::Reply(recon_reply())]);
        let first = AtomicBool::new(true);
        peer.serve("td.order.*", move |req| {
            let mut out = vec![Out::Reply(refused(req))];
            if first.swap(false, Ordering::SeqCst) {
                out.push(Out::Publish(evt_subject(API), position_event()));
            }
            out
        });
        peer.round_trip();
        peer
    }

    fn serve(&self, subject: &str, answer: impl Fn(&[u8]) -> Vec<Out> + Send + 'static) {
        let client = self.client.clone();
        let subject = subject.to_owned();
        let mut sub = self
            .io
            .block_on({
                let client = client.clone();
                async move { client.subscribe(subject).await }
            })
            .expect("subscribe");
        self.io.spawn(async move {
            while let Some(msg) = sub.next().await {
                for out in answer(&msg.payload) {
                    let (to, body) = match out {
                        Out::Reply(body) => (msg.reply.clone().expect("reply inbox"), body),
                        Out::Publish(subject, body) => (Subject::from(subject), body),
                    };
                    client.publish(to, body.into()).await.expect("peer publish");
                }
            }
        });
    }

    /// A round trip through the server, so the subscriptions are in place:
    /// async-nats `flush` only writes the socket.
    fn round_trip(&self) {
        let client = self.client.clone();
        self.io.block_on(async move {
            let inbox = client.new_inbox();
            let mut echo = client.subscribe(inbox.clone()).await.expect("echo");
            client
                .publish(inbox, Vec::new().into())
                .await
                .expect("ping");
            tokio::time::timeout(FUSE, echo.next())
                .await
                .expect("round trip")
                .expect("echo message");
        });
    }

    fn publish(&self, subject: &str, body: Vec<u8>) {
        let client = self.client.clone();
        let subject = subject.to_owned();
        self.io.block_on(async move {
            client.publish(subject, body.into()).await.expect("publish");
            client.flush().await.expect("flush");
        });
    }
}

fn listing() -> sym::SymbolInfo {
    let mut filters = std::collections::HashMap::new();
    filters.insert(PRICE_TICK.to_owned(), sym::Bound { value: Some(TICK) });
    sym::SymbolInfo {
        universal_ticker: TICKER.to_owned(),
        exch_ticker: "BTCUSDT".to_owned(),
        base: "BTC".into(),
        quote: "USDT".into(),
        status: sym::SymbolStatus::Tradable as i32,
        filters,
        updated_at_ns: 10,
        uid: UID,
        ..Default::default()
    }
}

/// natsx error reply, code 2 `not_found`.
fn not_found() -> Vec<u8> {
    let text = b"not_found";
    let mut body = vec![0, 1, 0, 2];
    body.extend_from_slice(&(text.len() as u16).to_be_bytes());
    body.extend_from_slice(text);
    body
}

fn sym_answer(req: &[u8]) -> Vec<u8> {
    let result = match sym::Request::decode(req).ok().and_then(|req| req.req) {
        Some(request::Req::Get(get)) if get.universal_ticker == TICKER => {
            reply::Result::Info(listing())
        }
        Some(request::Req::GetByUid(by)) if by.uid == UID => reply::Result::Info(listing()),
        Some(request::Req::Revisions(_)) => reply::Result::Revisions(sym::RevisionList {
            tables: vec![sym::TableRevision {
                venue: "Bybit".into(),
                category: "Spot".into(),
                revision: 1,
            }],
        }),
        _ => return not_found(),
    };
    sym::Reply {
        result: Some(result),
        ..Default::default()
    }
    .encode_to_vec()
}

fn tape_page() -> Vec<u8> {
    md::ViewReply {
        result: Some(view_reply::Result::Tape(md::TapePage {
            head: Some(md::Cursor {
                conn_epoch: 1,
                seq: 1,
            }),
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// The book at the first delta's cursor, (1, 1).
fn book_body() -> Vec<u8> {
    md::ViewReply {
        result: Some(view_reply::Result::Book(md::BookSnapshot {
            bids: vec![md::Level {
                price: 100.0,
                qty: 2.0,
            }],
            asks: vec![md::Level {
                price: 101.0,
                qty: 3.0,
            }],
            conn_epoch: 1,
            seq: 1,
            uid: UID,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Klines answer at once. The open interest first publishes the delta after
/// the snapshot: the strategy asks for it once the book is synced.
fn fetch_answer(req: &[u8]) -> Vec<Out> {
    let result = match md::FetchRequest::decode(req).ok().and_then(|req| req.req) {
        Some(md::fetch_request::Req::Klines(_)) => fetch_reply::Result::Klines(md::Klines {
            klines: vec![md::Kline {
                interval: "1m".into(),
                close: 5.0,
                closed: true,
                ..Default::default()
            }],
        }),
        Some(md::fetch_request::Req::OpenInterest(_)) => {
            fetch_reply::Result::OpenInterest(md::OpenInterest {
                open_interest: 4.0,
                ..Default::default()
            })
        }
        other => panic!("unexpected fetch {other:?}"),
    };
    let reply = md::FetchReply {
        uid: UID,
        result: Some(result),
        ..Default::default()
    }
    .encode_to_vec();
    match md::FetchReply::decode(reply.as_slice()).map(|r| r.result) {
        Ok(Some(fetch_reply::Result::OpenInterest(_))) => vec![
            Out::Publish(BOOK_SUBJECT.into(), delta(2)),
            Out::Reply(reply),
        ],
        _ => vec![Out::Reply(reply)],
    }
}

fn wall_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos() as i64
}

/// One md v1 envelope at `(1, seq)`, stamped now (PROTOCOL §7.1).
fn envelope(seq: u64, event: feed_event::Event) -> Vec<u8> {
    let payload = md::FeedEvent {
        uid: UID,
        event: Some(event),
    }
    .encode_to_vec();
    let mut out = vec![0u8; 40];
    out[0] = 1;
    out[8..16].copy_from_slice(&wall_ns().to_le_bytes());
    out[16..24].copy_from_slice(&1u64.to_le_bytes());
    out[24..32].copy_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

fn delta(seq: u64) -> Vec<u8> {
    envelope(
        seq,
        feed_event::Event::OrderBookDelta(md::OrderBookDelta {
            bids: vec![md::Level {
                price: 100.5,
                qty: 1.0,
            }],
            ..Default::default()
        }),
    )
}

fn trade() -> Vec<u8> {
    envelope(
        1,
        feed_event::Event::Trade(md::Trade {
            trade_id: "t1".into(),
            ..Default::default()
        }),
    )
}

/// The account's snapshot: incarnation 1, through seq 10.
fn recon_reply() -> Vec<u8> {
    td::ViewReply {
        incarnation: 1,
        seq: 10,
        cancel_batch: 1,
        reply: Some(td::view_reply::Reply::Recon(td::Recon {
            incarnation: 1,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// TD refuses the Submit locally.
fn refused(req: &[u8]) -> Vec<u8> {
    let request = td::OrderRequest::decode(req).expect("order request");
    let Some(td::order_request::Req::Submit(submit)) = request.req else {
        panic!("want a Submit, got {request:?}");
    };
    td::OrderReply {
        accepted: false,
        code: td::RejectCode::InvalidOrder as i32,
        reason: "invalid".into(),
        client_order_id: submit.client_order_id,
    }
    .encode_to_vec()
}

/// The next event after the snapshot.
fn position_event() -> Vec<u8> {
    td::AccountEvent {
        seq: 11,
        source: Some(common::WorkerRef {
            key: format!("td/{API}"),
            incarnation: 1,
        }),
        event: Some(td::account_event::Event::PositionUpdate(
            td::PositionUpdate {
                positions: vec![td::Position {
                    uid: UID,
                    size: 2.0,
                    ..Default::default()
                }],
            },
        )),
    }
    .encode_to_vec()
}

fn feeds(seq: u64) -> Bindings {
    Bindings {
        version: Some(Version { epoch: 1, seq }),
        feeds: [(TRADE, TRADE_SUBJECT), (BOOK, BOOK_SUBJECT)]
            .into_iter()
            .map(|(feed, subject)| FeedBinding {
                feed: feed.into(),
                subject: subject.into(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

// ---------- the dump ----------

fn kind(frame: &Value) -> &str {
    frame["kind"].as_str().expect("kind")
}

fn num(frame: &Value, key: &str) -> u64 {
    frame[key].as_u64().unwrap_or(0)
}

/// The frames of `kind`, as `(id, rec_seq)`: the id is the frame's `key`.
fn ids(frames: &[Value], of: &str, key: &str) -> Vec<(u64, u64)> {
    frames
        .iter()
        .filter(|frame| kind(frame) == of)
        .map(|frame| (num(frame, key), num(frame, "rec_seq")))
        .collect()
}

/// The `rec_seq` of the one frame of `kind` whose `key` is `id`.
#[track_caller]
fn only(frames: &[Value], of: &str, key: &str, id: u64) -> u64 {
    let found: Vec<u64> = ids(frames, of, key)
        .into_iter()
        .filter(|(n, _)| *n == id)
        .map(|(_, seq)| seq)
        .collect();
    assert_eq!(found.len(), 1, "{of} {key}={id}: {found:?}");
    found[0]
}

#[test]
fn a_whole_session_has_every_kind_and_each_call_and_read_pairs_up() {
    let nats = start_nats();
    let peer = Peer::start(&nats);
    let mut shim = Shim::spawn(&nats.url);
    hello(&mut shim);
    let mut payload = init_payload(&nats.creds, "");
    payload.accounts.push(AccountDecl {
        name: "main".into(),
        api_id: API,
        algo_ids: vec![1],
        ..Default::default()
    });
    payload.event_log = Some(EventLog {
        enabled: true,
        keep_mb: 1,
    });
    // Every waits for OnTrade and OnBook: a slow runner must not make the
    // trade or the delta too old to run them.
    payload.recv_window_ms = 10_000;
    init(&shim, payload, 5000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    start_with(&shim, feeds(1));
    expect_done(&mut shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }

    // A Command's bindings, then a Live's.
    shim.send(worker_down::Msg::Relay(worker::Relay {
        contract: "stsctl.v1".into(),
        corr_id: b"c1".to_vec(),
        payload: Command {
            cmd: Some(command::Cmd::Bindings(feeds(2))),
        }
        .encode_to_vec(),
    }));
    match shim.expect() {
        worker_up::Msg::Relay(echo) => assert_eq!(echo.corr_id, b"c1"),
        other => panic!("want the Relay echo, got {other:?}"),
    }
    shim.wait_report(|r| r.bindings_applied == Some(Version { epoch: 1, seq: 2 }));
    shim.send(worker_down::Msg::Live(worker::Live {
        contract: "stsctl.v1".into(),
        payload: feeds(3).encode_to_vec(),
        digest: "live-3".into(),
    }));
    expect_applied(&mut shim, "live-3");

    peer.publish(TRADE_SUBJECT, trade());
    peer.publish(BOOK_SUBJECT, delta(1));
    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);

    let dir = ended.volume.0.join(eventlog::DIR);
    let (ok, text) = eventlog_dump(&[&dir]);
    assert!(ok, "{text}");
    let mut lines = text.lines();
    let header: Value = serde_json::from_str(lines.next().expect("header")).expect("json");
    assert_eq!(header["session_id"], "c0ffee", "{header}");
    let frames: Vec<Value> = lines
        .map(|line| serde_json::from_str(line).expect("json"))
        .collect();
    for frame in &frames {
        assert!(frame.get("rec_seq").is_some(), "one file only: {frame}");
        assert!(frame.get("decode_error").is_none(), "{frame}");
    }
    let seqs: Vec<u64> = frames.iter().map(|f| num(f, "rec_seq")).collect();
    assert_eq!(seqs, (1..=frames.len() as u64).collect::<Vec<_>>());
    let kinds: BTreeSet<&str> = frames.iter().map(kind).collect();
    assert_eq!(kinds, KINDS.into_iter().collect(), "{text}");
    let last = frames.last().expect("frames");
    assert_eq!((kind(last), num(last, "dropped")), ("end", 0));

    // Every input has one hook_end after it, and every hook returned.
    let inputs: Vec<u64> = frames
        .iter()
        .filter(|f| INPUTS.contains(&kind(f)))
        .map(|f| num(f, "rec_seq"))
        .collect();
    let mut ends = Vec::new();
    for frame in frames.iter().filter(|f| kind(f) == "hook_end") {
        assert_eq!(frame["ending"], "returned", "{frame}");
        assert!(num(frame, "input") < num(frame, "rec_seq"), "{frame}");
        ends.push(num(frame, "input"));
    }
    ends.sort_unstable();
    assert_eq!(ends, inputs);

    // Call ids count from 1; each call is published once, then answered.
    let calls = ids(&frames, "order_call", "call");
    assert_eq!(calls.iter().map(|c| c.0).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(ids(&frames, "order_publish", "call").len(), calls.len());
    assert_eq!(ids(&frames, "order_reply", "call").len(), calls.len());
    for (call, at) in calls {
        let published = only(&frames, "order_publish", "call", call);
        let replied = only(&frames, "order_reply", "call", call);
        assert!(at < published && published < replied, "call {call}");
        let frame = &frames[at as usize - 1];
        assert_eq!((&frame["op"], num(frame, "api_id")), (&"submit".into(), API as u64));
        let reply = &frames[replied as usize - 1];
        assert_eq!(reply["outcome"], "ok", "{reply}");
        assert_eq!(reply["reply"]["reason"], "invalid", "{reply}");
        assert_eq!(reply["reply"]["client_order_id"], frame["cloids"][0]);
    }

    // Read ids count from 1 in call order: the strategy's tape and klines,
    // the book's own snapshot, then what the strategy asked once the book
    // was synced. Each has one reply of the same read after it.
    let reads: Vec<(u64, &str, u64)> = frames
        .iter()
        .filter(|f| kind(f) == "read_call")
        .map(|f| {
            let read = f["read_kind"].as_str().expect("read_kind");
            (num(f, "read"), read, num(f, "rec_seq"))
        })
        .collect();
    assert_eq!(
        reads.iter().map(|r| (r.0, r.1)).collect::<Vec<_>>(),
        [
            (1, "tape"),
            (2, "klines"),
            (3, "book_snapshot"),
            (4, "open_interest"),
            (5, "book_snapshot"),
        ]
    );
    assert_eq!(ids(&frames, "read_reply", "read").len(), reads.len());
    for (read, read_kind, at) in reads {
        let replied = only(&frames, "read_reply", "read", read);
        assert!(at < replied, "read {read}");
        let reply = &frames[replied as usize - 1];
        assert_eq!(reply["outcome"], "ok", "{reply}");
        assert_eq!(reply["read_kind"], read_kind, "{reply}");
    }

    let of = |want: &str| -> Vec<&Value> { frames.iter().filter(|f| kind(f) == want).collect() };
    let resolves = of("resolve");
    assert_eq!(resolves.len(), 2);
    assert_eq!(
        (&resolves[0]["ticker"], &resolves[0]["outcome"]),
        (&TICKER.into(), &"ok".into())
    );
    assert_eq!(num(&resolves[0]["info"], "uid"), u64::from(UID));
    assert_eq!(
        (&resolves[1]["ticker"], &resolves[1]["outcome"]),
        (&MISSING.into(), &"not_found".into())
    );
    // The Command's bindings, then the Live's; the Report shows the first
    // applied a moment before its frame is written.
    let mut bindings: Vec<(bool, &str)> = of("bindings")
        .iter()
        .map(|f| {
            let seq = f["bindings"]["version"]["seq"].as_str().unwrap_or("");
            (f["from_command"] == true, seq)
        })
        .collect();
    bindings.sort_unstable();
    assert_eq!(bindings, [(false, "3"), (true, "2")]);
    // Each delta is an input, the one the book syncs from too.
    let deltas: Vec<u64> = of("md_book").iter().map(|f| num(f, "seq")).collect();
    assert_eq!(deltas, [1, 2]);
    let stops = of("stop");
    assert_eq!(stops.len(), 1);
    assert_eq!(stops[0]["deliver"], true);
    for once in [
        "init", "start", "md_feed", "post", "td_view", "td_event", "onstop", "end",
    ] {
        assert_eq!(of(once).len(), 1, "{once}: {text}");
    }

    // E07-D010: the init payload is there without its NATS credentials, and
    // no credential is in the dump or in the files.
    let init = &frames[0];
    assert_eq!(kind(init), "init");
    assert_eq!(init["payload"]["session_id"], "c0ffee", "{init}");
    assert!(init["payload"].get("nats").is_none(), "{init}");
    let creds = &nats.creds;
    let mut bytes = Vec::new();
    for file in eventlog::files(&dir).expect("log files") {
        bytes.extend(fs::read(&file.path).expect("log file"));
    }
    for secret in [&creds.user_jwt, &creds.seed, &creds.inbox_prefix] {
        assert!(!text.contains(secret.as_str()), "the dump has {secret}");
        assert!(
            !bytes
                .windows(secret.len())
                .any(|w| w == secret.as_bytes()),
            "a log file has {secret}"
        );
    }
}
