//! Python MD reads, callbacks, and BookView against a fake SYM and MD peer.
//!
//! `cargo test --workspace` compiles these and does not run them (`#[ignore]`).
//! The python CI job installs the wheel, points `MFTIK_PYTHON` at that
//! interpreter, and runs this file with `--ignored`.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::Client;
use futures_util::StreamExt;
use mftik_core::symbol::PRICE_TICK;
use mftik_proto::wire::mftiko::common::v1::{Outcome, Version};
use mftik_proto::wire::mftiko::md::v1::{self as md, feed_event, fetch_reply, view_reply};
use mftik_proto::wire::mftiko::stsctl::v1::{Bindings, FeedBinding};
use mftik_proto::wire::mftiko::sym::v1::{self as sym, reply, request};
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::io::Io;
use mftik_testkit::{
    Ended, Nats, Shim, WorkerSpawn, expect_applied, expect_done, expect_ending, expect_hello, init,
    init_payload, start_nats, start_with,
};
use prost::Message;

const TICKER_NAME: &str = "Bybit_Spot_BTCUSDT";
const UID: u32 = 7;
const TICK: f64 = 0.5;

const TRADE_SUBJECT: &str = "md.v1.f.bybit1.trade.Bybit_Spot_BTCUSDT";
const TRADE_VIEW: &str = "md.v1.view.bybit1.trade.Bybit_Spot_BTCUSDT";
const BOOK_SUBJECT: &str = "md.v1.f.bybit1.orderbook_delta.Bybit_Spot_BTCUSDT";
const BOOK_VIEW: &str = "md.v1.view.bybit1.orderbook_delta.Bybit_Spot_BTCUSDT";
const LIQ_SUBJECT: &str = "md.v1.f.bybit1.liquidation.Bybit_Spot_BTCUSDT";
const TICKER_SUBJECT: &str = "md.v1.f.bybit1.ticker.Bybit_Spot_BTCUSDT";
const TICKER_VIEW: &str = "md.v1.view.bybit1.ticker.Bybit_Spot_BTCUSDT";
const FETCH: &str = "md.v1.fetch.Bybit";

fn python_bin() -> PathBuf {
    PathBuf::from(std::env::var_os("MFTIK_PYTHON").expect("MFTIK_PYTHON"))
}

fn strategies_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/pystrategies")
}

fn package_version() -> String {
    let output = Command::new(python_bin())
        .args(["-c", "import mftik; print(mftik.__version__, end='')"])
        .output()
        .expect("read mftik.__version__");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("version is utf-8")
}

fn python_module(spec: &str) -> WorkerSpawn {
    WorkerSpawn::new(python_bin())
        .args(["-m", "mftik.run", spec])
        .env("PYTHONPATH", strategies_dir())
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env_remove("MFTIK_RELEASE")
}

fn spawn(worker: WorkerSpawn, nats: &Nats) -> Shim {
    Shim::spawn_worker(&nats.url, worker)
}

fn running_with(shim: &mut Shim, nats: &Nats, bindings: Bindings, release: &str) {
    expect_hello(shim, release);
    init(shim, init_payload(&nats.creds, ""), 5000);
    expect_applied(shim, "live-1");
    expect_done(shim, Verb::Init, true, "");
    start_with(shim, bindings);
    expect_done(shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }
}

fn stop(shim: &Shim) {
    shim.send(worker_down::Msg::Stop(worker::Stop {
        grace_ms: 5000,
        reason: "operator".into(),
    }));
}

fn assert_traceback(ended: &Ended, type_name: &str) {
    assert!(
        ended.stderr.contains("Traceback"),
        "stderr missing traceback: {}",
        ended.stderr
    );
    assert!(
        ended.stderr.contains(type_name),
        "stderr missing {type_name}: {}",
        ended.stderr
    );
}

fn feeds(pairs: &[(&str, &str)]) -> Bindings {
    Bindings {
        version: Some(Version { epoch: 1, seq: 1 }),
        feeds: pairs
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

fn read_bindings() -> Bindings {
    feeds(&[
        ("trade.Bybit_Spot_BTCUSDT", TRADE_SUBJECT),
        ("orderbook_delta.Bybit_Spot_BTCUSDT", BOOK_SUBJECT),
        ("liquidation.Bybit_Spot_BTCUSDT", LIQ_SUBJECT),
        ("ticker.Bybit_Spot_BTCUSDT", TICKER_SUBJECT),
    ])
}

fn book_bindings() -> Bindings {
    feeds(&[("orderbook_delta.Bybit_Spot_BTCUSDT", BOOK_SUBJECT)])
}

fn trade_bindings() -> Bindings {
    feeds(&[("trade.Bybit_Spot_BTCUSDT", TRADE_SUBJECT)])
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counts {
    sym: usize,
    trade_view: usize,
    book_view: usize,
    ticker_view: usize,
    fetch: usize,
}

struct Peer {
    io: Io,
    client: Client,
    counts: Arc<Mutex<Counts>>,
}

impl Peer {
    fn start(nats: &Nats) -> Peer {
        let io = Io::start().expect("io");
        let url = nats.url.clone();
        let creds = nats.creds.clone();
        let client = io
            .block_on(async move {
                mftik_rt::nats::dial(&url, &creds, Duration::from_secs(5), None).await
            })
            .expect("peer dials");
        let counts = Arc::new(Mutex::new(Counts::default()));

        let seen = Arc::clone(&counts);
        serve(&io, &client, "sym", move |msg| {
            lock(&seen).sym += 1;
            Some(sym_answer(&msg.payload))
        });
        let seen = Arc::clone(&counts);
        serve(&io, &client, TRADE_VIEW, move |_| {
            lock(&seen).trade_view += 1;
            Some(tape_page())
        });
        let seen = Arc::clone(&counts);
        serve(&io, &client, BOOK_VIEW, move |_| {
            lock(&seen).book_view += 1;
            Some(book_body())
        });
        let seen = Arc::clone(&counts);
        serve(&io, &client, TICKER_VIEW, move |_| {
            lock(&seen).ticker_view += 1;
            None
        });
        let seen = Arc::clone(&counts);
        serve(&io, &client, FETCH, move |msg| {
            lock(&seen).fetch += 1;
            Some(fetch_answer(&msg.payload))
        });
        Peer { io, client, counts }
    }

    fn publish(&self, subject: &str, msg: Vec<u8>) {
        let client = self.client.clone();
        let subject = subject.to_owned();
        self.io.block_on(async move {
            client.publish(subject, msg.into()).await.expect("publish");
            client.flush().await.expect("flush");
        });
    }

    fn counts(&self) -> Counts {
        *lock(&self.counts)
    }
}

fn serve<F>(io: &Io, client: &Client, subject: &str, on_msg: F)
where
    F: Fn(async_nats::Message) -> Option<Vec<u8>> + Send + Sync + 'static,
{
    let client = client.clone();
    let subject = subject.to_owned();
    let on_msg = Arc::new(on_msg);
    let sub = io.block_on({
        let client = client.clone();
        async move {
            let sub = client.subscribe(subject).await.expect("subscribe");
            client.flush().await.expect("flush");
            sub
        }
    });
    io.spawn(async move {
        let mut sub = sub;
        while let Some(msg) = sub.next().await {
            let reply = msg.reply.clone();
            let Some(body) = on_msg(msg) else {
                continue;
            };
            let Some(reply) = reply else {
                continue;
            };
            let _ = client.publish(reply, body.into()).await;
        }
    });
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn listing() -> sym::SymbolInfo {
    let mut filters = std::collections::HashMap::new();
    filters.insert(PRICE_TICK.to_owned(), sym::Bound { value: Some(TICK) });
    sym::SymbolInfo {
        universal_ticker: TICKER_NAME.to_owned(),
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

fn info_body() -> Vec<u8> {
    sym::Reply {
        result: Some(reply::Result::Info(listing())),
        ..Default::default()
    }
    .encode_to_vec()
}

fn revisions_body() -> Vec<u8> {
    sym::Reply {
        result: Some(reply::Result::Revisions(sym::RevisionList {
            tables: vec![sym::TableRevision {
                venue: "Bybit".into(),
                category: "Spot".into(),
                revision: 1,
            }],
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// natsx error reply, code 2 `not_found`.
fn not_found() -> Vec<u8> {
    let text = b"not_found";
    let mut body = vec![0, 1, 0, 2];
    body.extend_from_slice(&(u16::try_from(text.len()).unwrap_or(u16::MAX)).to_be_bytes());
    body.extend_from_slice(text);
    body
}

fn sym_answer(payload: &[u8]) -> Vec<u8> {
    let Ok(req) = sym::Request::decode(payload) else {
        return not_found();
    };
    match req.req {
        Some(request::Req::Get(get)) if get.universal_ticker == TICKER_NAME => info_body(),
        Some(request::Req::GetByUid(by)) if by.uid == UID => info_body(),
        Some(request::Req::Revisions(_)) => revisions_body(),
        _ => not_found(),
    }
}

fn tape_page() -> Vec<u8> {
    md::ViewReply {
        result: Some(view_reply::Result::Tape(md::TapePage {
            more: false,
            head: Some(md::Cursor {
                conn_epoch: 1,
                seq: 9,
            }),
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

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

fn klines_body() -> Vec<u8> {
    md::FetchReply {
        uid: UID,
        result: Some(fetch_reply::Result::Klines(md::Klines {
            klines: vec![md::Kline {
                interval: "1m".into(),
                close: 5.0,
                closed: true,
                ..Default::default()
            }],
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

fn oi_body() -> Vec<u8> {
    md::FetchReply {
        uid: UID,
        result: Some(fetch_reply::Result::OpenInterest(md::OpenInterest {
            open_interest: 4.0,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

fn fetch_error() -> Vec<u8> {
    md::FetchReply {
        error: "invalid".into(),
        reason: "limit".into(),
        uid: UID,
        ..Default::default()
    }
    .encode_to_vec()
}

fn fetch_answer(payload: &[u8]) -> Vec<u8> {
    let Ok(req) = md::FetchRequest::decode(payload) else {
        return fetch_error();
    };
    match req.req {
        Some(md::fetch_request::Req::Klines(klines)) if klines.interval == "1m" => klines_body(),
        Some(md::fetch_request::Req::OpenInterest(_)) => oi_body(),
        _ => fetch_error(),
    }
}

/// One `orderbook_delta` at the snapshot cursor, so the fold skips it.
fn delta() -> Vec<u8> {
    let payload = md::FeedEvent {
        uid: UID,
        event: Some(feed_event::Event::OrderBookDelta(md::OrderBookDelta {
            bids: vec![md::Level {
                price: 1.0,
                qty: 1.0,
            }],
            ..Default::default()
        })),
    }
    .encode_to_vec();
    let md_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos() as i64;
    let mut out = vec![0u8; 40 + payload.len()];
    out[0] = 1;
    out[8..16].copy_from_slice(&md_ts.to_le_bytes());
    out[16..24].copy_from_slice(&1u64.to_le_bytes());
    out[24..32].copy_from_slice(&1u64.to_le_bytes());
    out[40..].copy_from_slice(&payload);
    out
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn reads_succeed_on_the_dispatcher_and_errors_match_e03_d013() {
    let nats = start_nats();
    let peer = Peer::start(&nats);
    let mut shim = spawn(python_module("reads:Reads"), &nats);
    running_with(&mut shim, &nats, read_bindings(), &package_version());
    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}\n{}", ended.stdout, ended.stderr);
    assert!(ended.stdout.contains("TAIL-reads"), "{}", ended.stdout);
    assert!(ended.stdout.contains("TAIL-stop"), "{}", ended.stdout);
    let counts = peer.counts();
    assert!(counts.sym >= 1, "{counts:?}");
    assert!(counts.trade_view >= 1, "{counts:?}");
    assert!(counts.book_view >= 1, "{counts:?}");
    assert!(counts.ticker_view >= 1, "{counts:?}");
    assert!(counts.fetch >= 3, "{counts:?}");
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn book_view_after_md_view_sync_dies_with_the_hook() {
    let nats = start_nats();
    let peer = Peer::start(&nats);
    let mut shim = spawn(python_module("reads:Book"), &nats);
    running_with(&mut shim, &nats, book_bindings(), &package_version());
    peer.publish(BOOK_SUBJECT, delta());
    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}\n{}", ended.stdout, ended.stderr);
    assert!(ended.stdout.contains("TAIL-book"), "{}", ended.stdout);
    assert!(ended.stdout.contains("TAIL-stop"), "{}", ended.stdout);
    assert!(peer.counts().book_view >= 1, "{:?}", peer.counts());
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn a_non_callable_read_callback_is_type_error() {
    let nats = start_nats();
    let mut shim = spawn(python_module("reads:NotCallable"), &nats);
    running_with(&mut shim, &nats, feeds(&[]), &package_version());
    stop(&shim);
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-callable"), "{}", ended.stdout);
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn a_read_callback_with_the_wrong_arity_is_crashed() {
    let nats = start_nats();
    let _peer = Peer::start(&nats);
    let mut shim = spawn(python_module("reads:Arity"), &nats);
    running_with(&mut shim, &nats, trade_bindings(), &package_version());
    // The mismatch is raised by the call itself, so CPython has no Python
    // frame to print. stderr still receives the exception line.
    const REASON: &str = "hook panic: TypeError: Arity.on_start.<locals>.bad() \
        takes 1 positional argument but 3 were given";
    expect_ending(&mut shim, Outcome::Crashed, REASON, Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(20), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-arity"), "{}", ended.stdout);
    assert!(
        ended
            .stderr
            .contains("takes 1 positional argument but 3 were given"),
        "{}",
        ended.stderr
    );
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn a_read_callback_exception_is_crashed() {
    let nats = start_nats();
    let _peer = Peer::start(&nats);
    let mut shim = spawn(python_module("reads:Boom"), &nats);
    running_with(&mut shim, &nats, trade_bindings(), &package_version());
    expect_ending(
        &mut shim,
        Outcome::Crashed,
        "hook panic: RuntimeError: read broke",
        Some(true),
    );
    let ended = shim.end();
    assert_eq!(ended.code, Some(20), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-boom"), "{}", ended.stdout);
    assert_traceback(&ended, "RuntimeError");
}
