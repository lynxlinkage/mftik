//! Python Context against a fake SYM and a fake TD (E04-04).
//!
//! `cargo test --workspace` compiles this and does not run it (`#[ignore]`).
//! The python CI job installs the wheel, points `MFTIK_PYTHON` at that
//! interpreter, and runs this file with `--ignored`. These cases are not a
//! port of the Go tests.

use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::Client;
use futures_util::StreamExt;
use mftik_core::symbol::PRICE_TICK;
use mftik_core::tdpath::evt_subject;
use mftik_proto::wire::mftiko::common::v1::Outcome;
use mftik_proto::wire::mftiko::session::v1::AccountDecl;
use mftik_proto::wire::mftiko::stsctl::v1::{InitPayload, NatsCredentials};
use mftik_proto::wire::mftiko::sym::v1::{self as sym, reply, request};
use mftik_proto::wire::mftiko::td::v1::{self as td};
use mftik_proto::wire::mftiko::worker::v1::Verb;
use mftik_rt::io::Io;
use mftik_testkit::{
    Ended, Nats, Shim, WorkerSpawn, expect_applied, expect_done, expect_ending, expect_hello, init,
    init_payload, start, start_nats,
};
use prost::Message;

const TICKER: &str = "Bybit_Spot_BTCUSDT";
const MISSING: &str = "Bybit_Spot_NOPE";
const UID: u32 = 1001;
const API: i64 = 7;

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

fn spawn(spec: &str, nats: &Nats) -> Shim {
    Shim::spawn_worker(&nats.url, python_module(spec))
}

fn with_timeout(creds: &NatsCredentials, hook_timeout_s: u32) -> InitPayload {
    let mut payload = init_payload(creds, "");
    payload.hook_timeout_s = hook_timeout_s;
    payload
}

fn with_account(creds: &NatsCredentials) -> InitPayload {
    let mut payload = with_timeout(creds, 5);
    payload.accounts.push(AccountDecl {
        name: "main".into(),
        api_id: API,
        algo_ids: vec![1],
        ..Default::default()
    });
    payload
}

fn hook_line<'a>(ended: &'a Ended, hook: &str) -> &'a str {
    let needle = format!("\"hook\": \"{hook}\"");
    ended
        .lines
        .iter()
        .find(|line| line.contains(&needle))
        .unwrap_or_else(|| {
            panic!(
                "missing {hook}\nstdout: {}\nstderr: {}",
                ended.stdout, ended.stderr
            )
        })
}

struct SymBook {
    row: sym::SymbolInfo,
    gets: Vec<String>,
    delay: Duration,
}

impl SymBook {
    fn answer(&mut self, payload: &[u8]) -> (Vec<u8>, Duration) {
        let req = sym::Request::decode(payload).expect("sym request");
        match req.req.expect("sym request is set") {
            request::Req::Get(get) => {
                self.gets.push(get.universal_ticker.clone());
                let delay = std::mem::take(&mut self.delay);
                let body = if get.universal_ticker == self.row.universal_ticker {
                    info_body(&self.row)
                } else {
                    not_found()
                };
                (body, delay)
            }
            request::Req::GetByUid(by) => {
                let body = if by.uid == self.row.uid {
                    info_body(&self.row)
                } else {
                    not_found()
                };
                (body, Duration::ZERO)
            }
            request::Req::Revisions(_) => (revisions(), Duration::ZERO),
            other => panic!("unexpected sym request {other:?}"),
        }
    }
}

fn listing() -> sym::SymbolInfo {
    let mut filters = std::collections::HashMap::new();
    filters.insert(PRICE_TICK.to_owned(), sym::Bound { value: Some(0.01) });
    sym::SymbolInfo {
        universal_ticker: TICKER.to_owned(),
        exch_ticker: "BTCUSDT".into(),
        base: "BTC".into(),
        quote: "USDT".into(),
        status: sym::SymbolStatus::Tradable as i32,
        filters,
        updated_at_ns: 10,
        uid: UID,
        ..Default::default()
    }
}

fn info_body(info: &sym::SymbolInfo) -> Vec<u8> {
    sym::Reply {
        result: Some(reply::Result::Info(info.clone())),
        ..Default::default()
    }
    .encode_to_vec()
}

fn revisions() -> Vec<u8> {
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

/// natsx error reply, handler code, text `not_found`.
fn not_found() -> Vec<u8> {
    let text = b"not_found";
    let length = u16::try_from(text.len()).expect("short");
    let mut body = vec![0, 1, 0, 2];
    body.extend_from_slice(&length.to_be_bytes());
    body.extend_from_slice(text);
    body
}

struct Peer {
    _io: Io,
    book: Arc<Mutex<SymBook>>,
    orders: Arc<Mutex<Vec<td::OrderRequest>>>,
    views: Arc<AtomicUsize>,
}

impl Peer {
    fn start(nats: &Nats, first_get: Duration) -> Self {
        let io = Io::start().expect("peer io");
        let mut creds = nats.creds.clone();
        creds.inbox_prefix = "_INBOX_peer".into();
        let url = nats.url.clone();
        let client = io
            .block_on(async move {
                mftik_rt::nats::dial(&url, &creds, Duration::from_secs(3), None).await
            })
            .expect("fake SYM and TD connect");
        let book = Arc::new(Mutex::new(SymBook {
            row: listing(),
            gets: Vec::new(),
            delay: first_get,
        }));
        let orders = Arc::new(Mutex::new(Vec::new()));
        let views = Arc::new(AtomicUsize::new(0));
        serve_sym(&io, client.clone(), Arc::clone(&book));
        serve_view(&io, client.clone(), Arc::clone(&views));
        serve_orders(&io, client.clone(), Arc::clone(&orders));
        round_trip(&io, &client);
        Self {
            _io: io,
            book,
            orders,
            views,
        }
    }

    fn gets(&self) -> Vec<String> {
        self.book.lock().expect("sym").gets.clone()
    }

    fn orders(&self) -> Vec<td::OrderRequest> {
        self.orders.lock().expect("orders").clone()
    }

    fn views(&self) -> usize {
        self.views.load(Ordering::SeqCst)
    }
}

fn serve_sym(io: &Io, client: Client, book: Arc<Mutex<SymBook>>) {
    let mut sub = io
        .block_on({
            let client = client.clone();
            async move { client.subscribe("sym").await }
        })
        .expect("subscribe sym");
    io.spawn(async move {
        while let Some(msg) = sub.next().await {
            let Some(reply_to) = msg.reply else {
                continue;
            };
            let (body, delay) = book.lock().expect("sym").answer(&msg.payload);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            client
                .publish(reply_to, body.into())
                .await
                .expect("sym reply");
        }
    });
}

fn serve_view(io: &Io, client: Client, views: Arc<AtomicUsize>) {
    let mut sub = io
        .block_on({
            let client = client.clone();
            async move { client.subscribe("td.view.*").await }
        })
        .expect("subscribe td.view");
    io.spawn(async move {
        while let Some(msg) = sub.next().await {
            views.fetch_add(1, Ordering::SeqCst);
            let Some(reply_to) = msg.reply else {
                continue;
            };
            let reply = td::ViewReply {
                incarnation: 1,
                seq: 10,
                cancel_batch: 2,
                reply: Some(td::view_reply::Reply::Recon(td::Recon {
                    incarnation: 1,
                    ..Default::default()
                })),
                ..Default::default()
            };
            client
                .publish(reply_to, reply.encode_to_vec().into())
                .await
                .expect("view reply");
        }
    });
}

fn serve_orders(io: &Io, client: Client, orders: Arc<Mutex<Vec<td::OrderRequest>>>) {
    let mut sub = io
        .block_on({
            let client = client.clone();
            async move { client.subscribe("td.order.*").await }
        })
        .expect("subscribe td.order");
    io.spawn(async move {
        let mut published = false;
        while let Some(msg) = sub.next().await {
            let request = td::OrderRequest::decode(msg.payload.as_ref()).expect("order request");
            let first_submit = if published {
                None
            } else if let Some(td::order_request::Req::Submit(submit)) = &request.req {
                published = true;
                Some(submit.client_order_id.clone())
            } else {
                None
            };
            orders.lock().expect("orders").push(request);
            if let Some(reply_to) = msg.reply {
                let reply = td::OrderReply {
                    accepted: true,
                    ..Default::default()
                };
                client
                    .publish(reply_to, reply.encode_to_vec().into())
                    .await
                    .expect("order reply");
            }
            if let Some(id) = first_submit {
                for event in [order_event(&id), position_event(), balance_event()] {
                    client
                        .publish(evt_subject(API), event.encode_to_vec().into())
                        .await
                        .expect("td.evt");
                }
            }
        }
    });
}

fn order_event(client_order_id: &str) -> td::AccountEvent {
    td::AccountEvent {
        seq: 11,
        source: Some(source()),
        event: Some(td::account_event::Event::OrderUpdate(td::OrderUpdate {
            order: Some(td::Order {
                uid: UID,
                client_order_id: client_order_id.to_owned(),
                order_id: "ex-1".into(),
                status: td::OrderStatus::Accepted as i32,
                ..Default::default()
            }),
            ..Default::default()
        })),
    }
}

fn position_event() -> td::AccountEvent {
    td::AccountEvent {
        seq: 12,
        source: Some(source()),
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
}

fn balance_event() -> td::AccountEvent {
    td::AccountEvent {
        seq: 13,
        source: Some(source()),
        event: Some(td::account_event::Event::BalanceUpdate(td::BalanceUpdate {
            balances: vec![td::Balance {
                asset: "USDT".into(),
                free: 3.0,
                ..Default::default()
            }],
        })),
    }
}

fn source() -> mftik_proto::wire::mftiko::common::v1::WorkerRef {
    mftik_proto::wire::mftiko::common::v1::WorkerRef {
        key: "td/7".into(),
        incarnation: 1,
    }
}

fn round_trip(io: &Io, client: &Client) {
    let client = client.clone();
    io.block_on(async move {
        let inbox = client.new_inbox();
        let mut echo = client.subscribe(inbox.clone()).await.expect("echo");
        client
            .publish(inbox, Vec::<u8>::new().into())
            .await
            .expect("ping");
        tokio::time::timeout(Duration::from_secs(5), echo.next())
            .await
            .expect("round trip")
            .expect("echo message");
    });
}

fn running(shim: &mut Shim, payload: InitPayload, release: &str) {
    expect_hello(shim, release);
    init(shim, payload, 8000);
    expect_applied(shim, "live-1");
    expect_done(shim, Verb::Init, true, "");
    start(shim);
    expect_done(shim, Verb::Start, true, "");
    match shim.expect() {
        mftik_proto::wire::mftiko::worker::v1::worker_up::Msg::Stage(stage) => {
            assert_eq!(stage.name, "running");
        }
        other => panic!("want Stage(running), got {other:?}"),
    }
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn resolve_submit_and_local_state() {
    let nats = start_nats();
    let peer = Peer::start(&nats, Duration::ZERO);
    let release = package_version();
    let mut shim = spawn("context_cases:Trade", &nats);
    running(&mut shim, with_account(&nats.creds), &release);
    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-trade"), "{}", ended.stdout);

    let resolved = hook_line(&ended, "resolved");
    assert!(resolved.contains("\"uid\": 1001"), "{resolved}");
    assert!(resolved.contains("\"tick\": 0.01"), "{resolved}");
    assert!(resolved.contains("\"again\": 1001"), "{resolved}");
    assert!(resolved.contains("\"tradable\": true"), "{resolved}");
    assert!(
        hook_line(&ended, "unknown-uid").contains("not known to this session"),
        "{}",
        ended.stdout
    );
    assert!(hook_line(&ended, "submit-type").contains("\"name\": \"TypeError\""));
    assert!(ended.stdout.contains("\"hook\": \"submit-kwargs\""));
    assert!(hook_line(&ended, "submit-enum").contains("unknown side 99"));
    let init_state = hook_line(&ended, "init-state");
    assert!(init_state.contains("\"orders\": 0"), "{init_state}");
    assert!(init_state.contains("\"order\": true"), "{init_state}");
    assert!(init_state.contains("\"pos\": true"), "{init_state}");
    assert!(init_state.contains("\"bal\": true"), "{init_state}");

    let orders = peer.orders();
    let submits: Vec<_> = orders
        .iter()
        .filter_map(|request| match &request.req {
            Some(td::order_request::Req::Submit(submit)) => Some(submit),
            _ => None,
        })
        .collect();
    let cancels: Vec<_> = orders
        .iter()
        .filter_map(|request| match &request.req {
            Some(td::order_request::Req::Cancel(cancel)) => Some(cancel.client_order_id.as_str()),
            _ => None,
        })
        .collect();
    let batches: Vec<_> = orders
        .iter()
        .filter_map(|request| match &request.req {
            Some(td::order_request::Req::BatchCancel(batch)) => Some(
                batch
                    .orders
                    .iter()
                    .map(|order| order.client_order_id.as_str())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(submits.len(), 2, "{orders:?}");
    assert_eq!(submits[0].uid, UID);
    assert_eq!(submits[1].uid, UID + 1);
    assert_eq!(cancels, vec![submits[0].client_order_id.as_str()]);
    assert_eq!(batches, vec![vec![submits[0].client_order_id.as_str()]]);
    assert!(peer.views() >= 2, "views {}", peer.views());

    let submitted = hook_line(&ended, "submitted");
    assert!(submitted.contains("\"n\": 2"), "{submitted}");
    assert!(submitted.contains("\"status\": [1, 1]"), "{submitted}");
    assert!(submitted.contains("\"other\": 0"), "{submitted}");
    let ids_at = submitted.find("\"ids\":").expect("ids");
    let first_at = submitted[ids_at..]
        .find(submits[0].client_order_id.as_str())
        .expect("first id");
    let second_at = submitted[ids_at..]
        .find(submits[1].client_order_id.as_str())
        .expect("second id");
    assert!(first_at < second_at, "{submitted}");

    let later = hook_line(&ended, "later");
    assert!(later.contains("\"oid\": \"ex-1\""), "{later}");
    assert!(later.contains("\"status\": 3"), "{later}");
    assert!(later.contains("\"size\": 2.0"), "{later}");
    assert!(later.contains("\"free\": 3.0"), "{later}");
    assert!(later.contains("\"missing_order\": true"), "{later}");
    assert!(later.contains("\"missing_pos\": true"), "{later}");
    assert!(later.contains("\"missing_bal\": true"), "{later}");
    assert!(later.contains("\"other_algo\": 0"), "{later}");
    assert_eq!(peer.gets(), vec![TICKER.to_owned()]);
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn resolve_outside_init_raises_not_in_init_and_sends_nothing() {
    let nats = start_nats();
    let peer = Peer::start(&nats, Duration::ZERO);
    let mut shim = spawn("context_cases:ResolveLate", &nats);
    running(&mut shim, with_timeout(&nats.creds, 5), &package_version());
    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-late"), "{}", ended.stdout);
    let late = hook_line(&ended, "late");
    assert!(late.contains("\"name\": \"NotInInit\""), "{late}");
    assert!(
        late.contains("sdk: only OnInit and OnStart may call this"),
        "{late}"
    );
    assert_eq!(peer.gets(), vec![TICKER.to_owned()]);
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn an_unknown_ticker_from_on_init_is_init_failed() {
    let nats = start_nats();
    let peer = Peer::start(&nats, Duration::ZERO);
    let mut shim = spawn("context_cases:ResolveMissing", &nats);
    expect_hello(&mut shim, &package_version());
    init(&shim, with_timeout(&nats.creds, 5), 8000);
    let error = "Error: symbol: no such instrument: not_found";
    expect_done(&mut shim, Verb::Init, false, error);
    expect_ending(&mut shim, Outcome::InitFailed, error, Some(false));
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert_eq!(peer.gets(), vec![MISSING.to_owned()]);
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn resolve_releases_the_gil_while_it_waits() {
    let nats = start_nats();
    let peer = Peer::start(&nats, Duration::from_millis(400));
    let mut shim = spawn("context_cases:ResolveGil", &nats);
    running(&mut shim, with_timeout(&nats.creds, 5), &package_version());
    shim.send(
        mftik_proto::wire::mftiko::worker::v1::worker_down::Msg::Stop(
            mftik_proto::wire::mftiko::worker::v1::Stop {
                grace_ms: 5000,
                reason: "operator".into(),
            },
        ),
    );
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    let gil = hook_line(&ended, "gil");
    assert!(gil.contains("\"uid\": 1001"), "{gil}");
    assert!(!gil.contains("\"n\": 0"), "{gil}");
    assert_eq!(peer.gets(), vec![TICKER.to_owned()]);
}
