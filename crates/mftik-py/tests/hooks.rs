//! Optional-hook delivery through `python -m mftik.run` (E04-03).
//!
//! Not a port of the Go tests. The fake MD peer publishes `md.f`. The fake TD
//! peer answers `td.view` and publishes `td.evt`. `cargo test --workspace`
//! compiles this file and does not run it (`#[ignore]`). The python CI job
//! runs it with `--ignored`.
//!
//! The wired-hook test leaves `on_book` and `on_reject` unfired on purpose:
//! an unresolved delta is logged and not delivered, and `td.evt` does not
//! carry a reject. `on_book` is delivered by a later test once `ctx.resolve`
//! has stored the tick and `md.view` has synced the book. `on_reject` is
//! delivered when `ctx.cancel` is refused locally because `td.order` has no
//! responder.

use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use mftik_core::cloid::{self, EPOCH_MS};
use mftik_core::tdpath::evt_subject;
use mftik_proto::wire::mftiko::common::v1::{self as common, Outcome, Version};
use mftik_proto::wire::mftiko::md::v1::{self as md, feed_event, view_reply as md_view};
use mftik_proto::wire::mftiko::session::v1::AccountDecl;
use mftik_proto::wire::mftiko::stsctl::v1::{Bindings, FeedBinding};
use mftik_proto::wire::mftiko::sym::v1::{self as sym, reply, request};
use mftik_proto::wire::mftiko::td::v1::{self as td, view_reply};
use mftik_proto::wire::mftiko::worker::v1::{Verb, worker_up};
use mftik_rt::io::Io;
use mftik_testkit::{
    Nats, Shim, WorkerSpawn, expect_applied, expect_done, expect_ending, expect_hello, init,
    init_payload, start_nats, start_with,
};
use prost::Message;

const TOPICS: &[&str] = &[
    "bestquote",
    "trade",
    "aggtrade",
    "kline_1m",
    "ticker",
    "liquidation",
    "funding_rate",
    "open_interest",
    "greeks",
    "mark_price",
    "orderbook_delta",
];

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

struct Peer {
    io: Io,
    client: async_nats::Client,
}

impl Peer {
    fn connect(nats: &Nats) -> Self {
        let io = Io::start().expect("peer io");
        let url = nats.url.clone();
        let creds = nats.creds.clone();
        let client = io
            .block_on(async move {
                mftik_rt::nats::dial(&url, &creds, Duration::from_secs(5), None).await
            })
            .expect("peer connects");
        Self { io, client }
    }

    fn serve_view(&self) {
        let client = self.client.clone();
        let sub = self
            .io
            .block_on({
                let client = client.clone();
                async move { client.subscribe("td.view.*").await }
            })
            .expect("subscribe td.view");
        self.io.spawn(async move {
            let mut sub = sub;
            while let Some(msg) = sub.next().await {
                let Some(to) = msg.reply else {
                    continue;
                };
                client
                    .publish(to, snapshot().encode_to_vec().into())
                    .await
                    .expect("view reply");
            }
        });
        self.barrier();
    }

    fn publish(&self, subject: &str, body: Vec<u8>) {
        let client = self.client.clone();
        let subject = subject.to_owned();
        self.io.block_on(async move {
            client.publish(subject, body.into()).await.expect("publish");
            client.flush().await.expect("flush");
        });
    }

    fn barrier(&self) {
        let client = self.client.clone();
        self.io
            .block_on(async move { mftik_rt::nats::barrier(&client, Duration::from_secs(5)).await })
            .expect("barrier");
    }
}

struct BookPeer {
    io: Io,
    client: async_nats::Client,
    views: Arc<AtomicUsize>,
    subjects: Arc<Mutex<Vec<String>>>,
}

impl BookPeer {
    fn start(nats: &Nats) -> Self {
        let io = Io::start().expect("book peer io");
        let mut creds = nats.creds.clone();
        creds.inbox_prefix = "_INBOX_peer".into();
        let url = nats.url.clone();
        let client = io
            .block_on(async move {
                mftik_rt::nats::dial(&url, &creds, Duration::from_secs(5), None).await
            })
            .expect("book peer connects");
        let views = Arc::new(AtomicUsize::new(0));
        let subjects = Arc::new(Mutex::new(Vec::new()));
        serve_sym(&io, client.clone());
        serve_book_view(
            &io,
            client.clone(),
            Arc::clone(&views),
            Arc::clone(&subjects),
        );
        let peer = Self {
            io,
            client,
            views,
            subjects,
        };
        peer.barrier();
        peer
    }

    fn views(&self) -> usize {
        self.views.load(Ordering::SeqCst)
    }

    fn subjects(&self) -> Vec<String> {
        self.subjects.lock().expect("subjects").clone()
    }

    fn publish(&self, subject: &str, body: Vec<u8>) {
        let client = self.client.clone();
        let subject = subject.to_owned();
        self.io.block_on(async move {
            client.publish(subject, body.into()).await.expect("publish");
            client.flush().await.expect("flush");
        });
    }

    fn barrier(&self) {
        let client = self.client.clone();
        self.io
            .block_on(async move { mftik_rt::nats::barrier(&client, Duration::from_secs(5)).await })
            .expect("barrier");
    }
}

fn serve_sym(io: &Io, client: async_nats::Client) {
    let sub = io
        .block_on({
            let client = client.clone();
            async move { client.subscribe("sym").await }
        })
        .expect("subscribe sym");
    io.spawn(async move {
        let mut sub = sub;
        while let Some(msg) = sub.next().await {
            let Some(reply_to) = msg.reply else {
                continue;
            };
            let body = sym_answer(&msg.payload);
            client
                .publish(reply_to, body.into())
                .await
                .expect("sym reply");
        }
    });
}

fn sym_answer(payload: &[u8]) -> Vec<u8> {
    let req = sym::Request::decode(payload).expect("sym request");
    match req.req.expect("sym request is set") {
        request::Req::Get(get) if get.universal_ticker == "Bybit_Spot_BTCUSDT" => sym_info(),
        request::Req::GetByUid(by) if by.uid == 7 => sym_info(),
        request::Req::Revisions(_) => sym::Reply {
            result: Some(reply::Result::Revisions(sym::RevisionList {
                tables: vec![sym::TableRevision {
                    venue: "Bybit".into(),
                    category: "Spot".into(),
                    revision: 1,
                }],
            })),
            ..Default::default()
        }
        .encode_to_vec(),
        _ => {
            let text = b"not_found";
            let length = u16::try_from(text.len()).expect("short");
            let mut body = vec![0, 1, 0, 2];
            body.extend_from_slice(&length.to_be_bytes());
            body.extend_from_slice(text);
            body
        }
    }
}

fn sym_info() -> Vec<u8> {
    let mut filters = std::collections::HashMap::new();
    filters.insert(
        mftik_core::symbol::PRICE_TICK.to_owned(),
        sym::Bound { value: Some(0.01) },
    );
    sym::Reply {
        result: Some(reply::Result::Info(sym::SymbolInfo {
            universal_ticker: "Bybit_Spot_BTCUSDT".into(),
            exch_ticker: "BTCUSDT".into(),
            base: "BTC".into(),
            quote: "USDT".into(),
            status: sym::SymbolStatus::Tradable as i32,
            filters,
            updated_at_ns: 10,
            uid: 7,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

fn serve_book_view(
    io: &Io,
    client: async_nats::Client,
    views: Arc<AtomicUsize>,
    subjects: Arc<Mutex<Vec<String>>>,
) {
    let sub = io
        .block_on({
            let client = client.clone();
            async move { client.subscribe("md.v1.view.>").await }
        })
        .expect("subscribe md.view");
    io.spawn(async move {
        let mut sub = sub;
        while let Some(msg) = sub.next().await {
            subjects
                .lock()
                .expect("subjects")
                .push(msg.subject.to_string());
            let Some(reply_to) = msg.reply else {
                continue;
            };
            let reply = md::ViewReply {
                result: Some(md_view::Result::Book(md::BookSnapshot {
                    uid: 7,
                    conn_epoch: 1,
                    seq: 1,
                    bids: vec![md::Level {
                        price: 11.0,
                        qty: 1.0,
                    }],
                    asks: vec![md::Level {
                        price: 12.0,
                        qty: 1.0,
                    }],
                    ..Default::default()
                })),
                ..Default::default()
            };
            client
                .publish(reply_to, reply.encode_to_vec().into())
                .await
                .expect("book snapshot");
            client.flush().await.expect("flush snapshot");
            views.fetch_add(1, Ordering::SeqCst);
        }
    });
}

fn subject(topic: &str) -> String {
    format!("md.v1.f.m1.{topic}.v")
}

fn bindings(topics: &[&str]) -> Bindings {
    Bindings {
        version: Some(Version { epoch: 1, seq: 1 }),
        feeds: topics
            .iter()
            .map(|topic| FeedBinding {
                feed: format!("{topic}.v"),
                subject: subject(topic),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn bindings_keyed(feeds: &[(&str, &str)]) -> Bindings {
    Bindings {
        version: Some(Version { epoch: 1, seq: 1 }),
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

fn with_account(nats: &Nats) -> mftik_proto::wire::mftiko::stsctl::v1::InitPayload {
    let mut payload = init_payload(&nats.creds, "");
    payload.accounts.push(AccountDecl {
        name: "main".into(),
        api_id: 7,
        algo_ids: vec![1],
        ..Default::default()
    });
    payload
}

fn wall_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos() as i64
}

fn envelope(seq: u64, md_ts: i64, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 40];
    out[0] = 1;
    out[8..16].copy_from_slice(&md_ts.to_le_bytes());
    out[16..24].copy_from_slice(&1u64.to_le_bytes());
    out[24..32].copy_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn book_delta(seq: u64, md_ts: i64, price: f64, qty: f64) -> Vec<u8> {
    feed(
        seq,
        md_ts,
        feed_event::Event::OrderBookDelta(md::OrderBookDelta {
            meta: Some(md::Meta { exch_ts_ns: 42 }),
            bids: vec![md::Level { price, qty }],
            ..Default::default()
        }),
    )
}

fn feed(seq: u64, md_ts: i64, event: feed_event::Event) -> Vec<u8> {
    let payload = md::FeedEvent {
        uid: 7,
        event: Some(event),
    }
    .encode_to_vec();
    envelope(seq, md_ts, &payload)
}

fn cloid(seq: u32) -> String {
    cloid::encode(1, EPOCH_MS, seq).expect("cloid")
}

fn snapshot() -> td::ViewReply {
    td::ViewReply {
        incarnation: 3,
        seq: 5,
        cancel_batch: 1,
        reply: Some(view_reply::Reply::Recon(td::Recon {
            incarnation: 3,
            positions: vec![td::Position {
                uid: 7,
                size: 2.0,
                ..Default::default()
            }],
            balances: vec![td::Balance {
                asset: "usdt".into(),
                free: 4.0,
                ..Default::default()
            }],
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn source() -> Option<common::WorkerRef> {
    Some(common::WorkerRef {
        key: "td/7".into(),
        incarnation: 3,
    })
}

fn account(seq: u64, event: td::account_event::Event) -> td::AccountEvent {
    td::AccountEvent {
        seq,
        source: source(),
        event: Some(event),
    }
}

fn order_event(seq: u64, id: String) -> td::AccountEvent {
    account(
        seq,
        td::account_event::Event::OrderUpdate(td::OrderUpdate {
            order: Some(td::Order {
                uid: 7,
                client_order_id: id,
                status: td::OrderStatus::New as i32,
                qty: 1.0,
                ..Default::default()
            }),
            cause: i32::from(td::OrderCause::Acknowledged),
            ..Default::default()
        }),
    )
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn each_wired_hook_is_delivered_and_skipped_topics_are_not() {
    let nats = start_nats();
    let peer = Peer::connect(&nats);
    peer.serve_view();
    let mut shim = spawn(python_module("hooks:Deliver"), &nats);
    expect_hello(&mut shim, &package_version());
    init(&shim, with_account(&nats), 5000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    start_with(&shim, bindings(TOPICS));
    expect_done(&mut shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }

    let now = wall_ns();
    peer.publish(
        &subject("trade"),
        feed(
            90,
            now - 5_000_000_000,
            feed_event::Event::Trade(md::Trade {
                trade_id: "old".into(),
                price: 11.0,
                ..Default::default()
            }),
        ),
    );
    peer.publish(&subject("trade"), envelope(91, now, &[0x07]));
    peer.publish(&subject("greeks"), envelope(92, now, &[0x07]));
    peer.publish(
        &subject("greeks"),
        feed(
            93,
            now,
            feed_event::Event::Trade(md::Trade {
                trade_id: "greeks-skip".into(),
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("orderbook_delta"),
        feed(
            12,
            now,
            feed_event::Event::OrderBookDelta(md::OrderBookDelta {
                bids: vec![md::Level {
                    price: 11.0,
                    qty: 1.0,
                }],
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("bestquote"),
        feed(
            1,
            now,
            feed_event::Event::BestQuote(md::BestQuote {
                bid: 11.0,
                bid_qty: 2.0,
                ask: 12.0,
                ask_qty: 4.0,
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("bestquote"),
        feed(
            2,
            now,
            feed_event::Event::BestQuote(md::BestQuote {
                bid: 12.0,
                bid_qty: 2.0,
                ask: 13.0,
                ask_qty: 4.0,
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("trade"),
        feed(
            3,
            now,
            feed_event::Event::Trade(md::Trade {
                trade_id: "t1".into(),
                price: 11.0,
                qty: 2.0,
                side: common::Side::Buy as i32,
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("trade"),
        feed(
            4,
            now,
            feed_event::Event::Trade(md::Trade {
                trade_id: "t2".into(),
                price: 12.0,
                qty: 2.0,
                side: common::Side::Sell as i32,
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("aggtrade"),
        feed(
            5,
            now,
            feed_event::Event::AggTrade(md::AggTrade {
                trade_id: "ag".into(),
                price: 11.0,
                qty: 2.0,
                side: common::Side::Buy as i32,
                match_count: 3,
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("kline_1m"),
        feed(
            6,
            now,
            feed_event::Event::Kline(md::Kline {
                interval: "1m".into(),
                close: 11.0,
                quote_volume: Some(2.5),
                closed: true,
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("ticker"),
        feed(
            7,
            now,
            feed_event::Event::Ticker(md::Ticker {
                last: Some(11.0),
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("liquidation"),
        feed(
            8,
            now,
            feed_event::Event::Liquidation(md::Liquidation {
                side: common::Side::Buy as i32,
                price: 11.0,
                qty: 2.0,
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("funding_rate"),
        feed(
            9,
            now,
            feed_event::Event::FundingRate(md::FundingRate {
                rate: 0.25,
                next_funding_ns: 5,
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("open_interest"),
        feed(
            10,
            now,
            feed_event::Event::OpenInterest(md::OpenInterest {
                open_interest: 11.0,
                ..Default::default()
            }),
        ),
    );
    peer.publish(
        &subject("mark_price"),
        feed(
            11,
            now,
            feed_event::Event::MarkPrice(md::MarkPrice {
                mark_price: 11.0,
                ..Default::default()
            }),
        ),
    );

    let first = cloid(1);
    let second = cloid(2);
    for event in [
        order_event(6, first.clone()),
        order_event(7, second.clone()),
        account(
            8,
            td::account_event::Event::Fill(td::Fill {
                uid: 7,
                fill_id: "f1".into(),
                client_order_id: first.clone(),
                qty: 1.0,
                price: 11.0,
                ..Default::default()
            }),
        ),
        account(
            9,
            td::account_event::Event::PositionUpdate(td::PositionUpdate {
                positions: vec![td::Position {
                    uid: 7,
                    size: 3.0,
                    ..Default::default()
                }],
            }),
        ),
        account(
            10,
            td::account_event::Event::BalanceUpdate(td::BalanceUpdate {
                balances: vec![td::Balance {
                    asset: "usdt".into(),
                    free: 5.0,
                    ..Default::default()
                }],
            }),
        ),
    ] {
        peer.publish(&evt_subject(7), event.encode_to_vec());
    }

    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(
        ended.code,
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        ended.stdout,
        ended.stderr
    );
    assert!(!ended.stderr.contains("Traceback"), "{}", ended.stderr);
    assert_eq!(
        ended.stderr.matches("md payload rejected").count(),
        1,
        "{}",
        ended.stderr
    );
    assert_eq!(
        ended.stderr.matches("book: uid 7 is not resolved").count(),
        1,
        "{}",
        ended.stderr
    );
    assert!(
        !ended.stdout.contains("\"hook\": \"book\""),
        "{}",
        ended.stdout
    );
    assert!(
        !ended.stdout.contains("\"hook\": \"reject\""),
        "{}",
        ended.stdout
    );
    assert!(
        !ended.stdout.contains("\"hook\": \"greeks\""),
        "{}",
        ended.stdout
    );
    assert!(
        !ended.stdout.contains("\"trade_id\": \"old\""),
        "{}",
        ended.stdout
    );
    assert!(!ended.stdout.contains("greeks-skip"), "{}", ended.stdout);
    assert!(ended.stdout.contains("\"hook\": \"init\""));
    assert!(ended.stdout.contains("\"hook\": \"start\""));
    assert!(ended.stdout.contains("\"hook\": \"stop\""));

    let quotes = hook_lines(&ended.stdout, "best_quote");
    assert_eq!(quotes.len(), 2, "{}", ended.stdout);
    let low = line_raw(&quotes, "bid", "11.0");
    let high = line_raw(&quotes, "bid", "12.0");
    assert_eq!(field(low, "uid"), "7");
    assert_eq!(field(low, "seq"), "1");
    assert_eq!(field(high, "seq"), "2");
    assert_ne!(field(low, "obj"), field(high, "obj"));
    assert_ne!(field(low, "data"), field(high, "data"));

    let trades = hook_lines(&ended.stdout, "trade");
    assert_eq!(trades.len(), 2, "{}", ended.stdout);
    let t1 = line_with(&trades, "trade_id", "t1");
    let t2 = line_with(&trades, "trade_id", "t2");
    assert_eq!(field(t1, "price"), "11.0");
    assert_eq!(field(t1, "uid"), "7");
    assert_eq!(field(t1, "seq"), "3");
    assert_eq!(field(t1, "frozen"), "frozen");
    assert_eq!(field(t2, "price"), "12.0");
    assert_eq!(field(t2, "seq"), "4");
    assert_ne!(field(t1, "obj"), field(t2, "obj"));
    assert_ne!(field(t1, "data"), field(t2, "data"));

    let agg = only(&ended.stdout, "agg_trade");
    assert_eq!(field(agg, "trade_id"), "ag");
    assert_eq!(field(agg, "count"), "3");
    assert_eq!(field(agg, "price"), "11.0");

    let kline = only(&ended.stdout, "kline");
    assert_eq!(field(kline, "interval"), "1m");
    assert_eq!(field(kline, "close"), "11.0");
    assert_eq!(field(kline, "closed"), "true");
    assert_eq!(field(kline, "quote"), "2.5");
    assert_eq!(field(only(&ended.stdout, "ticker"), "last"), "11.0");

    let liquidation = only(&ended.stdout, "liquidation");
    assert_eq!(field(liquidation, "price"), "11.0");
    assert_eq!(field(liquidation, "qty"), "2.0");
    assert_eq!(field(liquidation, "side"), "1");
    assert_eq!(field(only(&ended.stdout, "funding_rate"), "rate"), "0.25");
    assert_eq!(field(only(&ended.stdout, "open_interest"), "oi"), "11.0");
    assert_eq!(field(only(&ended.stdout, "mark_price"), "mark"), "11.0");

    let orders = hook_lines(&ended.stdout, "order");
    assert_eq!(orders.len(), 2, "{}", ended.stdout);
    let o1 = line_with(&orders, "cloid", &first);
    let o2 = line_with(&orders, "cloid", &second);
    assert_eq!(field(o1, "seq"), "6");
    assert_eq!(field(o2, "seq"), "7");
    assert_eq!(field(o1, "uid"), "7");
    assert_eq!(field(o1, "origin"), "1");
    assert_eq!(
        field(o1, "cause"),
        i32::from(td::OrderCause::Acknowledged).to_string()
    );
    assert_eq!(
        field(o1, "status"),
        (td::OrderStatus::New as i32).to_string()
    );
    assert_ne!(field(o1, "obj"), field(o2, "obj"));

    let fill = only(&ended.stdout, "fill");
    assert_eq!(field(fill, "fill_id"), "f1");
    assert_eq!(field(fill, "qty"), "1.0");
    assert_eq!(field(fill, "seq"), "8");

    let position = only(&ended.stdout, "position");
    assert_eq!(field(position, "size"), "3.0");
    assert_eq!(field(position, "uid"), "7");
    assert_eq!(field(position, "seq"), "9");

    let balance = only(&ended.stdout, "balance");
    assert_eq!(field(balance, "asset"), "usdt");
    assert_eq!(field(balance, "free"), "5.0");
    assert_eq!(field(balance, "seq"), "10");

    let recon = only(&ended.stdout, "recon");
    assert_eq!(field(recon, "size"), "2.0");
    assert_eq!(field(recon, "free"), "4.0");
    assert_eq!(field(recon, "asset"), "usdt");
    assert_eq!(field(recon, "inc"), "3");
    assert_eq!(field(recon, "seq"), "5");
    assert_eq!(
        field(recon, "reason"),
        i32::from(td::ReconReason::Requested).to_string()
    );
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn async_on_trade_is_init_failed() {
    init_failed("hooks:AsyncTrade", "TypeError: on_trade is async");
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn async_on_init_is_init_failed_and_not_called() {
    init_failed("hooks:AsyncInit", "TypeError: on_init is async");
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn async_on_stop_is_init_failed_and_registers_nothing() {
    init_failed("hooks:AsyncStop", "TypeError: on_stop is async");
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn on_trade_exception_crashes_and_on_stop_runs() {
    crash_on_trade("hooks:TradeBoom", "hook panic: RuntimeError: trade broke");
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn system_exit_in_on_trade_is_a_crash() {
    crash_on_trade("hooks:TradeExit", "hook panic: SystemExit: stop-via-sys");
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn on_reject_is_delivered_when_cancel_has_no_responder() {
    let nats = start_nats();
    let peer = Peer::connect(&nats);
    peer.serve_view();
    let mut shim = spawn(python_module("hooks:Cancel"), &nats);
    expect_hello(&mut shim, &package_version());
    init(&shim, with_account(&nats), 5000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    start_with(&shim, bindings(&["trade"]));
    expect_done(&mut shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }
    let id = cloid(1);
    peer.publish(&evt_subject(7), order_event(6, id.clone()).encode_to_vec());
    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(
        ended.code,
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        ended.stdout,
        ended.stderr
    );
    let reject = only(&ended.stdout, "reject");
    assert_eq!(field(reject, "cloid"), id);
    assert_eq!(
        field(reject, "request"),
        (mftik_core::tdpath::RejectedRequest::Cancel as i32).to_string()
    );
    assert_eq!(
        field(reject, "code"),
        i32::from(td::RejectCode::AccountOffline).to_string()
    );
    assert_eq!(field(reject, "reason"), mftik_core::tdpath::REASON_OFFLINE);
    assert!(ended.stdout.contains("\"hook\": \"stop\""));
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn on_book_is_delivered_once_the_book_is_synced() {
    let nats = start_nats();
    let peer = BookPeer::start(&nats);
    let mut shim = spawn(python_module("hooks:Book"), &nats);
    expect_hello(&mut shim, &package_version());
    init(&shim, init_payload(&nats.creds, ""), 5000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    start_with(
        &shim,
        bindings_keyed(&[(
            "orderbook_delta.Bybit_Spot_BTCUSDT",
            "md.v1.f.m1.orderbook_delta.Bybit_Spot_BTCUSDT",
        )]),
    );
    expect_done(&mut shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }
    let now = wall_ns();
    peer.publish(
        "md.v1.f.m1.orderbook_delta.Bybit_Spot_BTCUSDT",
        book_delta(2, now, 11.0, 1.0),
    );
    let started = Instant::now();
    while peer.views() == 0 {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "no md.view for the book: {:?}",
            peer.subjects()
        );
        thread::sleep(Duration::from_millis(10));
    }
    // The snapshot reply is applied on the TD buffer. The next delta has to
    // follow that apply; one that arrives while the reply is in flight is
    // folded into the snapshot and is not a hook.
    thread::sleep(Duration::from_millis(500));
    peer.publish(
        "md.v1.f.m1.orderbook_delta.Bybit_Spot_BTCUSDT",
        book_delta(3, wall_ns(), 11.0, 2.0),
    );
    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(
        ended.code,
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        ended.stdout,
        ended.stderr
    );
    let resolved = only(&ended.stdout, "resolved");
    assert_eq!(field(resolved, "uid"), "7");
    assert_eq!(field(resolved, "tick"), "0.01");
    let book = only(&ended.stdout, "book");
    assert_eq!(field(book, "uid"), "7");
    assert_eq!(field(book, "seq"), "3");
    assert_eq!(field(book, "exch"), "42");
    assert!(ended.stdout.contains("\"hook\": \"stop\""));
}

fn init_failed(spec: &str, error: &str) {
    let nats = start_nats();
    let mut shim = spawn(python_module(spec), &nats);
    expect_hello(&mut shim, &package_version());
    init(&shim, init_payload(&nats.creds, ""), 5000);
    expect_done(&mut shim, Verb::Init, false, error);
    expect_ending(&mut shim, Outcome::InitFailed, error, Some(false));
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert!(!ended.stdout.contains("TAIL-init"), "{}", ended.stdout);
    assert!(!ended.stdout.contains("TAIL-stop"), "{}", ended.stdout);
    assert!(ended.stderr.contains("Traceback"), "{}", ended.stderr);
    assert!(ended.stderr.contains(error), "{}", ended.stderr);
}

fn crash_on_trade(spec: &str, reason: &str) {
    let nats = start_nats();
    let peer = Peer::connect(&nats);
    let mut shim = spawn(python_module(spec), &nats);
    expect_hello(&mut shim, &package_version());
    init(&shim, init_payload(&nats.creds, ""), 5000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    start_with(&shim, bindings(&["trade"]));
    expect_done(&mut shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }
    peer.publish(
        &subject("trade"),
        feed(
            1,
            wall_ns(),
            feed_event::Event::Trade(md::Trade {
                trade_id: "t1".into(),
                price: 11.0,
                ..Default::default()
            }),
        ),
    );
    expect_ending(&mut shim, Outcome::Crashed, reason, Some(true));
    let ended = shim.end();
    assert_eq!(
        ended.code,
        Some(20),
        "stdout:\n{}\nstderr:\n{}",
        ended.stdout,
        ended.stderr
    );
    assert!(ended.stdout.contains("TAIL-stop"), "{}", ended.stdout);
    assert!(ended.stderr.contains("Traceback"), "{}", ended.stderr);
    let text = reason.strip_prefix("hook panic: ").expect("crash reason");
    assert!(
        ended.stderr.contains(text),
        "stderr missing {text}: {}",
        ended.stderr
    );
}

fn hook_lines<'a>(stdout: &'a str, hook: &str) -> Vec<&'a str> {
    let needle = format!("\"hook\": \"{hook}\"");
    stdout
        .lines()
        .filter(|line| line.contains(&needle))
        .collect()
}

fn only<'a>(stdout: &'a str, hook: &str) -> &'a str {
    let found = hook_lines(stdout, hook);
    assert_eq!(found.len(), 1, "{hook} in {stdout}");
    found[0]
}

fn line_with<'a>(lines: &[&'a str], key: &str, value: &str) -> &'a str {
    let needle = format!("\"{key}\": \"{value}\"");
    lines
        .iter()
        .copied()
        .find(|line| line.contains(&needle))
        .unwrap_or_else(|| panic!("no {key}={value} in {lines:?}"))
}

fn line_raw<'a>(lines: &[&'a str], key: &str, value: &str) -> &'a str {
    let needle = format!("\"{key}\": {value}");
    lines
        .iter()
        .copied()
        .find(|line| line.contains(&needle))
        .unwrap_or_else(|| panic!("no {key}={value} in {lines:?}"))
}

fn field<'a>(line: &'a str, key: &str) -> &'a str {
    let pat = format!("\"{key}\": ");
    let rest = line
        .split(&pat)
        .nth(1)
        .unwrap_or_else(|| panic!("missing {key} in {line}"));
    if let Some(text) = rest.strip_prefix('"') {
        text.split('"').next().unwrap()
    } else {
        rest.split([',', '}']).next().unwrap().trim()
    }
}
