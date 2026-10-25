//! The MD path through `mftik_rt::run` (E03-06): Start's bindings are
//! subscribed before the gate, `md.f` reaches OnTrade through the dispatcher
//! and the prehook, and Live changes the set and answers `Applied`. The fake
//! MD peer is a NATS client in the test process. See `common` for the shim.
//!
//! The book and the reads need SYM rows, which the SYM path (E03-08) stores;
//! they are tested against the MD path itself in `src/md/tests.rs`.

mod common;

use std::env;
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::*;
use mftik_proto::wire::mftiko::common::v1::{Outcome, Version};
use mftik_proto::wire::mftiko::md::v1::{self as md, feed_event};
use mftik_proto::wire::mftiko::stsctl::v1::{Bindings, FeedBinding, SelectorBinding};
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::{Context, HookError, Strategy, TapeQuery, Trade, TradeHandler};
use prost::Message;

const FEED: &str = "trade.Bybit_Perp_BTCUSDT";
const S1: &str = "md.v1.f.bybit1.trade.Bybit_Perp_BTCUSDT";
const S2: &str = "md.v1.f.bybit2.trade.Bybit_Perp_BTCUSDT";
const OTHER: &str = "trade.Bybit_Perp_ETHUSDT";

// ---------- the worker side ----------

#[derive(Default)]
struct MdWorker;

fn line(hook: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{{\"hook\":\"{hook}\"}}");
    let _ = out.flush();
}

impl Strategy for MdWorker {
    fn on_init(&mut self, _: &Context<Self>) -> Result<(), HookError> {
        line("OnInit");
        Ok(())
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        line("OnStart");
        // No SYM row for the uid: refused before any request.
        let tape = ctx.tape(1, "trade", TapeQuery::default(), |_, _, _| {});
        line(&format!("Tape {:?}", tape.err()));
        line(&format!("Book {}", ctx.book(1).is_some()));
        Ok(())
    }

    fn on_stop(&mut self, _: &Context<Self>) {
        line("OnStop");
    }

    fn handlers(&self, h: &mut mftik_rt::Handlers<Self>) {
        h.trade();
    }
}

impl TradeHandler for MdWorker {
    fn on_trade(&mut self, ctx: &Context<Self>, ev: Trade) {
        line(&format!("Trade {}", ev.data.trade_id));
        if ev.data.trade_id == "end" {
            ctx.exit();
        }
    }
}

#[test]
fn worker_entry() {
    if env::var_os(WORKER_ENV).is_some() {
        mftik_rt::run(MdWorker, Some(TEST_RELEASE));
    }
}

// ---------- the MD peer ----------

struct Peer {
    rt: tokio::runtime::Runtime,
    client: async_nats::Client,
}

impl Peer {
    fn connect(nats: &Nats) -> Peer {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let client = rt
            .block_on(mftik_rt::nats::dial(
                &nats.url,
                &nats.creds,
                Duration::from_secs(5),
                None,
            ))
            .expect("peer connects");
        Peer { rt, client }
    }

    fn publish(&self, subject: &str, msg: Vec<u8>) {
        let client = self.client.clone();
        let subject = subject.to_owned();
        self.rt.block_on(async move {
            client.publish(subject, msg.into()).await.expect("publish");
            client.flush().await.expect("flush");
        });
    }
}

fn wall_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos() as i64
}

/// One `trade` in a v1 envelope (PROTOCOL §7.1).
fn trade(seq: u64, md_ts: i64, id: &str) -> Vec<u8> {
    let payload = md::FeedEvent {
        uid: 7,
        event: Some(feed_event::Event::Trade(md::Trade {
            trade_id: id.into(),
            ..Default::default()
        })),
    }
    .encode_to_vec();
    let mut out = vec![0u8; 40];
    out[0] = 1;
    out[8..16].copy_from_slice(&md_ts.to_le_bytes());
    out[16..24].copy_from_slice(&1u64.to_le_bytes());
    out[24..32].copy_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

fn feeds(seq: u64, feeds: &[(&str, &str)]) -> Bindings {
    Bindings {
        version: Some(Version { epoch: 1, seq }),
        feeds: feeds
            .iter()
            .map(|(feed, subject)| FeedBinding {
                feed: (*feed).into(),
                subject: (*subject).into(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn live(shim: &Shim, bindings: &Bindings, digest: &str) {
    shim.send(worker_down::Msg::Live(worker::Live {
        contract: "stsctl.v1".into(),
        payload: bindings.encode_to_vec(),
        digest: digest.into(),
    }));
}

// ---------- the test ----------

#[test]
fn md_f_reaches_on_trade_through_start_and_live() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    hello(&mut shim);
    init(&shim, init_payload(&nats.creds, ""), 5000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    // Done(Start) comes after the subscribe and its flush (E03-D011).
    start_with(&shim, feeds(1, &[(FEED, S1)]));
    expect_done(&mut shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }
    let peer = Peer::connect(&nats);
    // recv_window_ms is 500: the stale one is dropped by the prehook.
    peer.publish(S1, trade(1, wall_ns() - 5_000_000_000, "stale"));
    peer.publish(S1, trade(2, wall_ns(), "a"));

    live(&shim, &feeds(2, &[(FEED, S1), (OTHER, S2)]), "live-2");
    expect_applied(&mut shim, "live-2");
    peer.publish(S2, trade(3, wall_ns(), "b"));
    // A selector is an error: no Applied (§9.2.9). The next one answers.
    let mut selector = feeds(3, &[(OTHER, S2)]);
    selector.selectors.push(SelectorBinding::default());
    live(&shim, &selector, "live-x");
    live(&shim, &feeds(3, &[(OTHER, S2)]), "live-3");
    expect_applied(&mut shim, "live-3");
    // S1 left the set with live-3.
    peer.publish(S1, trade(4, wall_ns(), "gone"));
    peer.publish(S2, trade(5, wall_ns(), "end"));

    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert_eq!(
        hooks(&ended),
        [
            "OnInit",
            "gate",
            "OnStart",
            "Tape Some(UnknownUid)",
            "Book false",
            "Trade a",
            "Trade b",
            "Trade end",
            "OnStop"
        ]
    );
    assert!(
        ended
            .stderr
            .contains("live not applied: mdpath: selectors are not supported"),
        "{}",
        ended.stderr
    );
}
