//! Example strategies through `python -m mftik.run` (E05-03, E05-D031), or as
//! a strategy binary (E08-D009).
//!
//! `examples/python/*/test_e2e.py` and `examples/rust/md_watch/test_e2e.py`
//! write a scenario and run this ignored test. The shim, nats-server, and the
//! MD, TD, and SYM peers live here. A scenario with `program` runs that binary
//! instead of Python, from `cwd` when it is set, with only the shim's
//! variables, as a shim starts a packed image (PROTOCOL §4.6). Its Hello must
//! carry [`mftik_rt::RELEASE`], which is what `mftik_rt::run(strategy, None)`
//! sends. `cargo test --workspace` compiles this file and does not run it.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use mftik_core::cloid::{self, EPOCH_MS};
use mftik_core::symbol::PRICE_TICK;
use mftik_core::tdpath::evt_subject;
use mftik_proto::wire::mftiko::common::v1::{self as common, Outcome, Version};
use mftik_proto::wire::mftiko::md::v1::{self as md, feed_event, view_reply as md_view};
use mftik_proto::wire::mftiko::session::v1::{AccountDecl, EventLog};
use mftik_proto::wire::mftiko::stsctl::v1::{Bindings, FeedBinding, InitPayload};
use mftik_proto::wire::mftiko::sym::v1::{self as sym, reply, request};
use mftik_proto::wire::mftiko::td::v1::{self as td, view_reply};
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::io::Io;
use mftik_testkit::{
    FUSE, Nats, Shim, TempDir, WorkerSpawn, expect_applied, expect_done, expect_ending,
    expect_hello, init, init_payload, start_nats, start_with,
};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};

const SCENARIO_ENV: &str = "MFTIK_EXAMPLE_SCENARIO";
const REPORT_ENV: &str = "MFTIK_EXAMPLE_REPORT";
const INCARNATION: u32 = 3;
const SNAPSHOT_SEQ: u64 = 5;

#[derive(Deserialize)]
struct Scenario {
    #[serde(default)]
    spec: String,
    #[serde(default)]
    pythonpath: String,
    #[serde(default)]
    program: Option<PathBuf>,
    #[serde(default)]
    cwd: Option<PathBuf>,
    params: Value,
    ticker: String,
    uid: u32,
    #[serde(default = "default_tick")]
    price_tick: f64,
    #[serde(default)]
    api_id: i64,
    #[serde(default)]
    algo_ids: Vec<u32>,
    #[serde(default)]
    feeds: Vec<String>,
    #[serde(default = "default_name")]
    volume_name: String,
    #[serde(default)]
    volume_data: Option<String>,
    #[serde(default = "default_input")]
    input_name: String,
    #[serde(default)]
    input_data: Option<String>,
    steps: Vec<String>,
    #[serde(default)]
    recon_open: bool,
    #[serde(default)]
    position_size: Option<f64>,
    #[serde(default = "default_batch")]
    cancel_batch: u32,
    /// With the event log on, the volume's `eventlog/` is copied here after
    /// the session ends (E07-D024).
    #[serde(default)]
    event_log_dir: Option<PathBuf>,
}

fn default_tick() -> f64 {
    0.01
}

fn default_name() -> String {
    "fills".into()
}

fn default_input() -> String {
    "state".into()
}

fn default_batch() -> u32 {
    4
}

enum Step {
    Trade,
    Book,
    Sleep(f64),
    WaitSubmit,
    Accept,
    WaitCancel,
    Fill,
    Stop,
}

fn parse_step(step: &str) -> Step {
    if let Some(rest) = step.strip_prefix("sleep:") {
        return Step::Sleep(rest.parse().unwrap_or_else(|_| panic!("sleep {step}")));
    }
    match step {
        "trade" => Step::Trade,
        "book" => Step::Book,
        "wait_submit" => Step::WaitSubmit,
        "accept" => Step::Accept,
        "wait_cancel" => Step::WaitCancel,
        "fill" => Step::Fill,
        "stop" => Step::Stop,
        other => panic!("unknown step {other}"),
    }
}

struct Peer {
    io: Io,
    client: async_nats::Client,
    requests: Arc<Mutex<Vec<td::OrderRequest>>>,
    views: Arc<AtomicUsize>,
}

impl Peer {
    fn connect(nats: &Nats) -> Self {
        let io = Io::start().expect("peer io");
        let mut creds = nats.creds.clone();
        creds.inbox_prefix = "_INBOX_peer".into();
        let url = nats.url.clone();
        let client = io
            .block_on(async move {
                mftik_rt::nats::dial(&url, &creds, Duration::from_secs(5), None).await
            })
            .expect("peer connects");
        Self {
            io,
            client,
            requests: Arc::new(Mutex::new(Vec::new())),
            views: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn serve(&self, scenario: &Scenario) {
        serve_sym(&self.io, self.client.clone(), scenario);
        serve_book(
            &self.io,
            self.client.clone(),
            Arc::clone(&self.views),
            scenario.uid,
        );
        serve_view(&self.io, self.client.clone(), scenario);
        serve_orders(&self.io, self.client.clone(), Arc::clone(&self.requests));
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

    fn requests(&self) -> Vec<td::OrderRequest> {
        self.requests.lock().expect("requests").clone()
    }

    fn views(&self) -> usize {
        self.views.load(Ordering::SeqCst)
    }
}

fn serve_sym(io: &Io, client: async_nats::Client, scenario: &Scenario) {
    let ticker = scenario.ticker.clone();
    let uid = scenario.uid;
    let tick = scenario.price_tick;
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
            let body = sym_answer(&msg.payload, &ticker, uid, tick);
            client
                .publish(reply_to, body.into())
                .await
                .expect("sym reply");
        }
    });
}

fn sym_answer(payload: &[u8], ticker: &str, uid: u32, tick: f64) -> Vec<u8> {
    let req = sym::Request::decode(payload).expect("sym request");
    let known = match req.req.expect("sym request is set") {
        request::Req::Get(get) => get.universal_ticker == ticker,
        request::Req::GetByUid(by) => by.uid == uid,
        request::Req::Revisions(_) => {
            return sym::Reply {
                result: Some(reply::Result::Revisions(sym::RevisionList {
                    tables: vec![sym::TableRevision {
                        venue: venue_of(ticker),
                        category: "Spot".into(),
                        revision: 1,
                    }],
                })),
                ..Default::default()
            }
            .encode_to_vec();
        }
        _ => false,
    };
    if known {
        return sym_info(ticker, uid, tick);
    }
    let text = b"not_found";
    let length = u16::try_from(text.len()).expect("short");
    let mut body = vec![0, 1, 0, 2];
    body.extend_from_slice(&length.to_be_bytes());
    body.extend_from_slice(text);
    body
}

fn venue_of(ticker: &str) -> String {
    ticker.split('_').next().unwrap_or("Ex").to_owned()
}

fn sym_info(ticker: &str, uid: u32, tick: f64) -> Vec<u8> {
    let mut filters = std::collections::HashMap::new();
    filters.insert(PRICE_TICK.to_owned(), sym::Bound { value: Some(tick) });
    sym::Reply {
        result: Some(reply::Result::Info(sym::SymbolInfo {
            universal_ticker: ticker.to_owned(),
            exch_ticker: "ABC".into(),
            base: "ABC".into(),
            quote: "USD".into(),
            status: sym::SymbolStatus::Tradable as i32,
            filters,
            updated_at_ns: 10,
            uid,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

fn serve_book(io: &Io, client: async_nats::Client, views: Arc<AtomicUsize>, uid: u32) {
    let sub = io
        .block_on({
            let client = client.clone();
            async move { client.subscribe("md.v1.view.>").await }
        })
        .expect("subscribe md.view");
    io.spawn(async move {
        let mut sub = sub;
        while let Some(msg) = sub.next().await {
            let Some(reply_to) = msg.reply else {
                continue;
            };
            let reply = md::ViewReply {
                result: Some(md_view::Result::Book(md::BookSnapshot {
                    uid,
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

fn serve_view(io: &Io, client: async_nats::Client, scenario: &Scenario) {
    let reply = snapshot(scenario).encode_to_vec();
    let sub = io
        .block_on({
            let client = client.clone();
            async move { client.subscribe("td.view.*").await }
        })
        .expect("subscribe td.view");
    io.spawn(async move {
        let mut sub = sub;
        while let Some(msg) = sub.next().await {
            let Some(to) = msg.reply else {
                continue;
            };
            client
                .publish(to, reply.clone().into())
                .await
                .expect("view reply");
        }
    });
}

fn snapshot(scenario: &Scenario) -> td::ViewReply {
    let algo = scenario.algo_ids.first().copied().unwrap_or(1);
    let mut recon = td::Recon {
        incarnation: INCARNATION,
        ..Default::default()
    };
    if scenario.recon_open {
        recon.open_orders.push(td::Order {
            uid: scenario.uid,
            client_order_id: cloid::encode(algo, EPOCH_MS, 1).expect("leftover cloid"),
            status: td::OrderStatus::Accepted as i32,
            qty: 1.0,
            ..Default::default()
        });
    }
    if let Some(size) = scenario.position_size {
        recon.positions.push(td::Position {
            uid: scenario.uid,
            size,
            ..Default::default()
        });
    }
    td::ViewReply {
        incarnation: INCARNATION,
        seq: SNAPSHOT_SEQ,
        cancel_batch: scenario.cancel_batch,
        reply: Some(view_reply::Reply::Recon(recon)),
        ..Default::default()
    }
}

fn serve_orders(io: &Io, client: async_nats::Client, requests: Arc<Mutex<Vec<td::OrderRequest>>>) {
    let sub = io
        .block_on({
            let client = client.clone();
            async move { client.subscribe("td.order.*").await }
        })
        .expect("subscribe td.order");
    io.spawn(async move {
        let mut sub = sub;
        while let Some(msg) = sub.next().await {
            let request = td::OrderRequest::decode(msg.payload.as_ref()).expect("order request");
            requests.lock().expect("requests").push(request);
            let Some(to) = msg.reply else {
                continue;
            };
            let reply = td::OrderReply {
                accepted: true,
                ..Default::default()
            };
            client
                .publish(to, reply.encode_to_vec().into())
                .await
                .expect("order reply");
        }
    });
}

fn python_bin() -> PathBuf {
    PathBuf::from(std::env::var_os("MFTIK_PYTHON").expect("MFTIK_PYTHON"))
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

/// The worker, and the `Hello.release` it must send.
fn worker_for(scenario: &Scenario) -> (WorkerSpawn, String) {
    if let Some(program) = &scenario.program {
        let mut worker = WorkerSpawn::new(program).env_clear();
        if let Some(dir) = &scenario.cwd {
            worker = worker.current_dir(dir);
        }
        return (worker, mftik_rt::RELEASE.to_owned());
    }
    let worker = WorkerSpawn::new(python_bin())
        .args(["-m", "mftik.run", &scenario.spec])
        .env("PYTHONPATH", &scenario.pythonpath)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env_remove("MFTIK_RELEASE");
    (worker, package_version())
}

fn bindings_for(scenario: &Scenario) -> Bindings {
    let mut feeds = Vec::new();
    if scenario.feeds.iter().any(|feed| feed == "trade") {
        feeds.push(FeedBinding {
            feed: "trade.v".into(),
            subject: "md.v1.f.m1.trade.v".into(),
            ..Default::default()
        });
    }
    if scenario.feeds.iter().any(|feed| feed == "book") {
        let feed = format!("orderbook_delta.{}", scenario.ticker);
        feeds.push(FeedBinding {
            feed: feed.clone(),
            subject: format!("md.v1.f.m1.{feed}"),
            ..Default::default()
        });
    }
    Bindings {
        version: Some(Version { epoch: 1, seq: 1 }),
        feeds,
        ..Default::default()
    }
}

fn payload_for(nats: &Nats, scenario: &Scenario) -> InitPayload {
    let mut payload = init_payload(&nats.creds, "");
    payload.params = serde_json::to_vec(&scenario.params).expect("params");
    if scenario.api_id > 0 {
        payload.accounts.push(AccountDecl {
            name: "main".into(),
            api_id: scenario.api_id,
            algo_ids: scenario.algo_ids.clone(),
            ..Default::default()
        });
    }
    if scenario.event_log_dir.is_some() {
        // A session's first incarnation; the log's file names carry it.
        payload.incarnation = 1;
        payload.event_log = Some(EventLog {
            enabled: true,
            keep_mb: 0,
        });
    }
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

fn feed(uid: u32, seq: u64, md_ts: i64, event: feed_event::Event) -> Vec<u8> {
    let payload = md::FeedEvent {
        uid,
        event: Some(event),
    }
    .encode_to_vec();
    envelope(seq, md_ts, &payload)
}

fn book_delta(uid: u32, seq: u64, qty: f64) -> Vec<u8> {
    feed(
        uid,
        seq,
        wall_ns(),
        feed_event::Event::OrderBookDelta(md::OrderBookDelta {
            meta: Some(md::Meta { exch_ts_ns: 42 }),
            bids: vec![md::Level { price: 11.0, qty }],
            ..Default::default()
        }),
    )
}

fn account_event(seq: u64, event: td::account_event::Event) -> td::AccountEvent {
    td::AccountEvent {
        seq,
        source: Some(common::WorkerRef {
            key: "td/7".into(),
            incarnation: INCARNATION,
        }),
        event: Some(event),
    }
}

struct Drive {
    submit: Option<String>,
    next_seq: u64,
    stopped: bool,
}

fn find_submit(requests: &[td::OrderRequest]) -> Option<String> {
    requests.iter().find_map(|request| match &request.req {
        Some(td::order_request::Req::Submit(submit)) => Some(submit.client_order_id.clone()),
        _ => None,
    })
}

fn has_cancel(requests: &[td::OrderRequest], cloid: &str) -> bool {
    requests.iter().any(|request| match &request.req {
        Some(td::order_request::Req::Cancel(cancel)) => cancel.client_order_id == cloid,
        _ => false,
    })
}

fn wait_until(label: &str, mut ready: impl FnMut() -> bool) {
    let started = Instant::now();
    while !ready() {
        assert!(started.elapsed() < FUSE, "timed out waiting for {label}");
        thread::sleep(Duration::from_millis(20));
    }
}

fn run_step(step: Step, peer: &Peer, shim: &mut Shim, scenario: &Scenario, drive: &mut Drive) {
    match step {
        Step::Trade => peer.publish(
            "md.v1.f.m1.trade.v",
            feed(
                scenario.uid,
                1,
                wall_ns(),
                feed_event::Event::Trade(md::Trade {
                    trade_id: "t1".into(),
                    price: 11.0,
                    qty: 2.0,
                    side: common::Side::Buy as i32,
                    ..Default::default()
                }),
            ),
        ),
        Step::Book => {
            let subject = format!("md.v1.f.m1.orderbook_delta.{}", scenario.ticker);
            peer.publish(&subject, book_delta(scenario.uid, 2, 1.0));
            wait_until("md.view", || peer.views() > 0);
            thread::sleep(Duration::from_millis(500));
            peer.publish(&subject, book_delta(scenario.uid, 3, 2.0));
        }
        Step::Sleep(secs) => {
            let delay =
                Duration::try_from_secs_f64(secs).unwrap_or_else(|_| panic!("sleep {secs}"));
            thread::sleep(delay);
        }
        Step::WaitSubmit => {
            wait_until("submit", || find_submit(&peer.requests()).is_some());
            drive.submit = find_submit(&peer.requests());
        }
        Step::Accept => {
            let cloid = drive.submit.clone().expect("submit before accept");
            let seq = drive.next_seq;
            drive.next_seq += 1;
            peer.publish(
                &evt_subject(scenario.api_id),
                account_event(
                    seq,
                    td::account_event::Event::OrderUpdate(td::OrderUpdate {
                        order: Some(td::Order {
                            uid: scenario.uid,
                            client_order_id: cloid,
                            status: td::OrderStatus::Accepted as i32,
                            qty: 1.0,
                            price: Some(10.0),
                            ..Default::default()
                        }),
                        cause: i32::from(td::OrderCause::Accepted),
                        ..Default::default()
                    }),
                )
                .encode_to_vec(),
            );
        }
        Step::WaitCancel => {
            let cloid = drive.submit.clone().expect("submit before cancel");
            wait_until("cancel", || has_cancel(&peer.requests(), &cloid));
        }
        Step::Fill => {
            let algo = scenario.algo_ids.first().copied().unwrap_or(1);
            let cloid = drive
                .submit
                .clone()
                .unwrap_or_else(|| cloid::encode(algo, EPOCH_MS, 9).expect("fill cloid"));
            let seq = drive.next_seq;
            drive.next_seq += 1;
            peer.publish(
                &evt_subject(scenario.api_id),
                account_event(
                    seq,
                    td::account_event::Event::Fill(td::Fill {
                        uid: scenario.uid,
                        fill_id: "f1".into(),
                        client_order_id: cloid,
                        qty: 1.0,
                        price: 10.0,
                        ..Default::default()
                    }),
                )
                .encode_to_vec(),
            );
        }
        Step::Stop => stop(shim, drive),
    }
}

fn stop(shim: &Shim, drive: &mut Drive) {
    if drive.stopped {
        return;
    }
    drive.stopped = true;
    shim.send(worker_down::Msg::Stop(worker::Stop {
        grace_ms: 5000,
        reason: "operator".into(),
    }));
}

fn order_value(request: &td::OrderRequest) -> Value {
    match &request.req {
        Some(td::order_request::Req::Submit(submit)) => json!({
            "op": "submit",
            "uid": submit.uid,
            "qty": submit.qty,
            "price": submit.price,
            "side": submit.side,
            "order_type": submit.order_type,
            "tif": submit.tif,
            "cloid": submit.client_order_id,
        }),
        Some(td::order_request::Req::Cancel(cancel)) => json!({
            "op": "cancel",
            "uid": cancel.uid,
            "cloid": cancel.client_order_id,
        }),
        Some(td::order_request::Req::BatchCancel(batch)) => json!({
            "op": "batch_cancel",
            "ids": batch.orders.iter().map(|order| &order.client_order_id).collect::<Vec<_>>(),
        }),
        _ => json!({"op": "empty"}),
    }
}

fn finish(
    mut shim: Shim,
    volume_name: &str,
    event_log_dir: Option<&Path>,
) -> (Option<i32>, String, String, Option<String>) {
    assert!(shim.next().is_none(), "frames after the last one");
    let started = Instant::now();
    let status = loop {
        if let Some(status) = shim.child.try_wait().expect("wait") {
            break status;
        }
        assert!(started.elapsed() < FUSE, "worker did not exit");
        thread::sleep(Duration::from_millis(10));
    };
    let volume = fs::read(shim.volume.0.join(volume_name))
        .ok()
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned());
    if let Some(out) = event_log_dir {
        copy_dir(&shim.volume.0.join(mftik_rt::eventlog::DIR), out);
    }
    let mut stdout = String::new();
    let mut stderr = String::new();
    let _ = shim
        .child
        .stdout
        .take()
        .expect("stdout")
        .read_to_string(&mut stdout);
    let _ = shim
        .child
        .stderr
        .take()
        .expect("stderr")
        .read_to_string(&mut stderr);
    (status.code(), stdout, stderr, volume)
}

/// The files of `from`, one level deep: the event log has no subdirectories.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("event log copy dir");
    for entry in fs::read_dir(from).expect("eventlog dir") {
        let entry = entry.expect("eventlog entry");
        fs::copy(entry.path(), to.join(entry.file_name())).expect("copy event log file");
    }
}

#[test]
#[ignore = "example e2e: MFTIK_EXAMPLE_SCENARIO and MFTIK_PYTHON"]
fn drive() {
    let scenario_path = std::env::var(SCENARIO_ENV).expect(SCENARIO_ENV);
    let report_path = std::env::var(REPORT_ENV).expect(REPORT_ENV);
    let scenario: Scenario =
        serde_json::from_reader(fs::File::open(&scenario_path).expect("scenario file"))
            .expect("scenario json");
    let nats = start_nats();
    let peer = Peer::connect(&nats);
    peer.serve(&scenario);

    let (mut worker, release) = worker_for(&scenario);
    let input = TempDir::new("input");
    if let Some(data) = &scenario.input_data {
        let dir = input.0.join(&scenario.input_name);
        fs::create_dir_all(&dir).expect("input dir");
        fs::write(dir.join(&scenario.volume_name), data).expect("input file");
        worker = worker.env("MFTIKO_INPUT_DIR", &input.0);
    }
    let mut shim = Shim::spawn_worker(&nats.url, worker);
    if let Some(data) = &scenario.volume_data {
        fs::write(shim.volume.0.join(&scenario.volume_name), data).expect("volume seed");
    }

    expect_hello(&mut shim, &release);
    init(&shim, payload_for(&nats, &scenario), 5000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    start_with(&shim, bindings_for(&scenario));
    expect_done(&mut shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }

    let mut state = Drive {
        submit: None,
        next_seq: SNAPSHOT_SEQ + 1,
        stopped: false,
    };
    for step in &scenario.steps {
        run_step(parse_step(step), &peer, &mut shim, &scenario, &mut state);
    }
    stop(&shim, &mut state);
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    let orders: Vec<Value> = peer.requests().iter().map(order_value).collect();
    let (code, stdout, stderr, volume) = finish(
        shim,
        &scenario.volume_name,
        scenario.event_log_dir.as_deref(),
    );
    let report = json!({
        "code": code,
        "stdout": stdout,
        "stderr": stderr,
        "orders": orders,
        "volume": volume,
    });
    fs::write(
        &report_path,
        serde_json::to_vec_pretty(&report).expect("report"),
    )
    .expect("write report");
}
