//! The TD path against a real nats-server and a fake TD peer in this
//! process: `td.view` and `td.order` responders and a `td.evt` publisher on
//! a connection of their own. The dispatcher runs its loop on a thread of
//! its own; the hooks are recorded.
//!
//! nats-server comes from `MFTIK_NATS_SERVER`, else `PATH`, as in
//! `tests/session.rs`. A test that cannot find it fails.

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant as StdInstant};

use mftik_core::cloid;
use mftik_core::dispatch::{self, RunEnd, StopRequest};
use mftik_core::tdpath::{Origin, RejectedRequest, Td};
use mftik_proto::wire::mftiko::common::v1 as common;

use super::*;

const FUSE: Duration = Duration::from_secs(20);
const SESSION: &str = "c0ffee";

// ---------- nats-server, no auth ----------

struct Server {
    child: Child,
    url: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn nats_server_bin() -> PathBuf {
    if let Some(path) = std::env::var_os("MFTIK_NATS_SERVER") {
        return PathBuf::from(path);
    }
    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|dir| dir.join("nats-server"))
                .find(|bin| bin.is_file())
        })
        .expect("nats-server not found: set MFTIK_NATS_SERVER or put it on PATH")
}

fn start_server() -> Server {
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let child = Command::new(nats_server_bin())
        .args(["-a", "127.0.0.1", "-p", &port.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start nats-server");
    let start = StdInstant::now();
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(start.elapsed() < FUSE, "nats-server did not listen");
        thread::sleep(Duration::from_millis(20));
    }
    Server {
        child,
        url: format!("nats://127.0.0.1:{port}"),
    }
}

fn connect(io: &Io, url: &str) -> Client {
    let url = url.to_owned();
    io.block_on(async move { async_nats::connect(url).await })
        .expect("connect")
}

#[track_caller]
fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let start = StdInstant::now();
    while !cond() {
        assert!(start.elapsed() < FUSE, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

// ---------- the fake TD peer ----------

/// The fixture snapshot: incarnation 3, seq 5 (PROTOCOL §9.8, conformance
/// `answerView`).
const INCARNATION: u32 = 3;
const SNAPSHOT_SEQ: u64 = 5;

#[derive(Clone, Copy, PartialEq, Eq)]
enum View {
    /// Reply with the fixture.
    Reply,
    /// As `Reply`, but each account's first reply first publishes two events
    /// on this connection, as conformance's recon check: account 7's order
    /// seq 4 and seq 6, account 8's position seq 5 and seq 6.
    Inject,
    /// Take the request and never answer.
    Hold,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Orders {
    Accept,
    /// Take the request and never answer.
    Silent,
}

struct Peer {
    io: Io,
    nats: Client,
    requests: Arc<Mutex<Vec<td::OrderRequest>>>,
    views: Arc<AtomicUsize>,
}

/// Account 7 batches two cancels, account 8 has no batch cancel.
fn cancel_batch(api_id: i64) -> u32 {
    if api_id == 7 { 2 } else { 1 }
}

impl Peer {
    fn connect(url: &str) -> Peer {
        let io = Io::start().expect("peer io");
        let nats = connect(&io, url);
        Peer {
            io,
            nats,
            requests: Arc::default(),
            views: Arc::default(),
        }
    }

    fn serve_view(&self, mode: View) {
        let nats = self.nats.clone();
        let views = Arc::clone(&self.views);
        let mut sub = self
            .io
            .block_on({
                let nats = nats.clone();
                async move { nats.subscribe("td.view.*").await }
            })
            .expect("subscribe td.view");
        self.io.spawn(async move {
            let mut injected = Vec::new();
            while let Some(msg) = sub.next().await {
                views.fetch_add(1, Ordering::SeqCst);
                if mode == View::Hold {
                    continue;
                }
                let api_id: i64 = msg.subject.as_str()["td.view.".len()..]
                    .parse()
                    .expect("api id");
                if mode == View::Inject && !injected.contains(&api_id) {
                    injected.push(api_id);
                    let events = if api_id == 7 {
                        [order_event(4, INCARNATION), order_event(6, INCARNATION)]
                    } else {
                        [
                            position_event(5, INCARNATION),
                            position_event(6, INCARNATION),
                        ]
                    };
                    for event in events {
                        nats.publish(tdpath::evt_subject(api_id), event.encode_to_vec().into())
                            .await
                            .expect("evt");
                    }
                }
                let reply = td::ViewReply {
                    incarnation: INCARNATION,
                    seq: SNAPSHOT_SEQ,
                    cancel_batch: cancel_batch(api_id),
                    reply: Some(td::view_reply::Reply::Recon(td::Recon {
                        incarnation: INCARNATION,
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                let to = msg.reply.expect("reply subject");
                nats.publish(to, reply.encode_to_vec().into())
                    .await
                    .expect("view reply");
            }
        });
        self.flush();
    }

    fn serve_orders(&self, mode: Orders) {
        let nats = self.nats.clone();
        let requests = Arc::clone(&self.requests);
        let mut sub = self
            .io
            .block_on({
                let nats = nats.clone();
                async move { nats.subscribe("td.order.*").await }
            })
            .expect("subscribe td.order");
        self.io.spawn(async move {
            while let Some(msg) = sub.next().await {
                let request = td::OrderRequest::decode(msg.payload.as_ref()).expect("request");
                requests.lock().expect("requests").push(request);
                if mode == Orders::Silent {
                    continue;
                }
                let reply = td::OrderReply {
                    accepted: true,
                    ..Default::default()
                };
                let to = msg.reply.expect("reply subject");
                nats.publish(to, reply.encode_to_vec().into())
                    .await
                    .expect("order reply");
            }
        });
        self.flush();
    }

    /// Account 7's view is the fixture snapshot. Account 8's view and every
    /// `td.order` stay parked until [`Parked`] publishes the reply.
    fn serve_live_and_parked(&self) -> Parked {
        let view_inbox: Arc<Mutex<Option<String>>> = Arc::default();
        let order_inbox: Arc<Mutex<Option<String>>> = Arc::default();
        let nats = self.nats.clone();
        let view_slot = Arc::clone(&view_inbox);
        let mut views = self
            .io
            .block_on({
                let nats = nats.clone();
                async move { nats.subscribe("td.view.*").await }
            })
            .expect("subscribe td.view");
        self.io.spawn(async move {
            while let Some(msg) = views.next().await {
                let api_id: i64 = msg.subject.as_str()["td.view.".len()..]
                    .parse()
                    .expect("api id");
                let to = msg.reply.expect("reply subject");
                if api_id == 8 {
                    *view_slot.lock().expect("view") = Some(to.to_string());
                    continue;
                }
                let reply = td::ViewReply {
                    incarnation: INCARNATION,
                    seq: SNAPSHOT_SEQ,
                    cancel_batch: cancel_batch(api_id),
                    reply: Some(td::view_reply::Reply::Recon(td::Recon {
                        incarnation: INCARNATION,
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                nats.publish(to, reply.encode_to_vec().into())
                    .await
                    .expect("view reply");
            }
        });
        let nats = self.nats.clone();
        let order_slot = Arc::clone(&order_inbox);
        let mut orders = self
            .io
            .block_on({
                let nats = nats.clone();
                async move { nats.subscribe("td.order.*").await }
            })
            .expect("subscribe td.order");
        self.io.spawn(async move {
            while let Some(msg) = orders.next().await {
                td::OrderRequest::decode(msg.payload.as_ref()).expect("request");
                let to = msg.reply.expect("reply subject");
                *order_slot.lock().expect("order") = Some(to.to_string());
            }
        });
        self.flush();
        Parked {
            nats: self.nats.clone(),
            io: self.io.clone(),
            view_inbox,
            order_inbox,
        }
    }

    fn publish(&self, api_id: i64, event: td::AccountEvent) {
        let nats = self.nats.clone();
        self.io
            .block_on(async move {
                nats.publish(tdpath::evt_subject(api_id), event.encode_to_vec().into())
                    .await
            })
            .expect("publish");
    }

    /// A round trip through the server, so its subscriptions are in place:
    /// async-nats `flush` only writes the socket.
    fn flush(&self) {
        let nats = self.nats.clone();
        self.io
            .block_on(async move {
                let inbox = nats.new_inbox();
                let mut echo = nats.subscribe(inbox.clone()).await.ok()?;
                nats.publish(inbox, Vec::new().into()).await.ok()?;
                tokio::time::timeout(FUSE, echo.next()).await.ok()?
            })
            .expect("round trip");
    }

    fn requests(&self) -> Vec<td::OrderRequest> {
        self.requests.lock().expect("requests").clone()
    }
}

/// Reply subjects captured by [`Peer::serve_live_and_parked`].
struct Parked {
    nats: Client,
    io: Io,
    view_inbox: Arc<Mutex<Option<String>>>,
    order_inbox: Arc<Mutex<Option<String>>>,
}

impl Parked {
    fn inbox(slot: &Mutex<Option<String>>, what: &'static str) -> String {
        wait_until(what, || slot.lock().expect("inbox").is_some());
        slot.lock().expect("inbox").clone().expect(what)
    }

    fn view_inbox(&self) -> String {
        Self::inbox(&self.view_inbox, "api 8 view")
    }

    fn order_inbox(&self) -> String {
        Self::inbox(&self.order_inbox, "the order")
    }

    fn publish(&self, inbox: String, body: Vec<u8>) {
        let nats = self.nats.clone();
        self.io
            .block_on(async move {
                nats.publish(inbox, body.into())
                    .await
                    .map_err(|err| err.to_string())?;
                nats.flush().await.map_err(|err| err.to_string())
            })
            .expect("reply");
    }
}

fn source(incarnation: u32) -> Option<common::WorkerRef> {
    Some(common::WorkerRef {
        key: "td/7".into(),
        incarnation,
    })
}

/// An OrderUpdate of this session's algo 1, its client id made from `seq`.
fn order_event(seq: u64, incarnation: u32) -> td::AccountEvent {
    td::AccountEvent {
        seq,
        source: source(incarnation),
        event: Some(td::account_event::Event::OrderUpdate(td::OrderUpdate {
            order: Some(td::Order {
                uid: 1001,
                client_order_id: cloid::encode(
                    1,
                    cloid::EPOCH_MS + 1 + (seq / 1024) as i64,
                    (seq % 1024) as u32,
                )
                .expect("cloid"),
                status: td::OrderStatus::New as i32,
                ..Default::default()
            }),
            ..Default::default()
        })),
    }
}

fn position_event(seq: u64, incarnation: u32) -> td::AccountEvent {
    td::AccountEvent {
        seq,
        source: source(incarnation),
        event: Some(td::account_event::Event::PositionUpdate(
            td::PositionUpdate {
                positions: vec![td::Position {
                    uid: 1001,
                    size: 2.0,
                    ..Default::default()
                }],
            },
        )),
    }
}

fn balance_event(seq: u64, incarnation: u32) -> td::AccountEvent {
    td::AccountEvent {
        seq,
        source: source(incarnation),
        event: Some(td::account_event::Event::BalanceUpdate(td::BalanceUpdate {
            balances: vec![td::Balance {
                asset: "usd".into(),
                free: 1.0,
                locked: 2.0,
                prelock: 3.0,
            }],
        })),
    }
}

/// The client id [`order_event`] stores for `seq`.
fn event_id(seq: u64) -> String {
    cloid::encode(
        1,
        cloid::EPOCH_MS + 1 + (seq / 1024) as i64,
        (seq % 1024) as u32,
    )
    .expect("cloid")
}

// ---------- the session side ----------

/// Runs inside the next delivered hook, once.
type During = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

/// Reads the OMS from the strategy `on_stop`, after the TD drain.
type StopWatch = Box<dyn FnMut(&TdPath, &dispatch::StopInfo) -> Result<(), String> + Send>;

/// A delivered hook, and whether `order` found the hook's order inside it.
#[derive(Clone, Debug)]
struct Seen {
    hook: Hook,
    visible: bool,
}

struct Rig {
    peer: Peer,
    td: TdPath,
    disp: Dispatcher,
    seen: Arc<Mutex<Vec<Seen>>>,
    logs: Arc<Mutex<Vec<String>>>,
    /// Runs inside the next delivered hook, once.
    during: During,
    _server: Server,
}

fn rig(recv_window: Duration) -> Rig {
    rig_with(recv_window, None)
}

fn rig_with(recv_window: Duration, on_stop: Option<StopWatch>) -> Rig {
    let server = start_server();
    let peer = Peer::connect(&server.url);
    let io = Io::start().expect("io");
    let clock = Clock::new();
    let timers = Timers::new(io.clone(), clock);
    let nats = connect(&io, &server.url);
    let path: Arc<OnceLock<TdPath>> = Arc::default();
    let logs: Arc<Mutex<Vec<String>>> = Arc::default();
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
    let during: During = Arc::default();
    let unsubscribe = Arc::clone(&path);
    let disp_logs = Arc::clone(&logs);
    let on_stop = on_stop.map(|mut callback| {
        let path = Arc::clone(&path);
        Box::new(move |info: &dispatch::StopInfo| {
            let td = path.get().expect("td").clone();
            callback(&td, info)
        }) as dispatch::OnStopHook
    });
    let disp = Dispatcher::new(dispatch::Config {
        recv_window,
        hook_timeout: Duration::ZERO,
        now_ns: clock.now_ns(),
        on_stop,
        unsubscribe_td: Some(Box::new(move || {
            if let Some(td) = unsubscribe.get() {
                td.unsubscribe();
            }
        })),
        log: Some(Arc::new(move |line: &str| {
            disp_logs.lock().expect("logs").push(line.to_owned());
        })),
        ..Default::default()
    });
    let (reads, record, hooks_during) = (Arc::clone(&path), Arc::clone(&seen), Arc::clone(&during));
    let td_logs = Arc::clone(&logs);
    let td = TdPath::open(Config {
        session_id: SESSION.into(),
        recv_window,
        accounts: vec![
            AccountDecl {
                api_id: 7,
                algo_ids: vec![1],
                ..Default::default()
            },
            AccountDecl {
                api_id: 8,
                algo_ids: vec![1, 2],
                ..Default::default()
            },
        ],
        nats,
        io,
        timers,
        clock,
        disp: disp.clone(),
        deliver: Arc::new(move |hook: Hook| {
            let visible = match &hook {
                Hook::Order(ev) => {
                    let id = ev.order.client_order_id.as_str();
                    reads
                        .get()
                        .and_then(|td| td.order(ev.td.api_id, id))
                        .is_some()
                }
                _ => false,
            };
            let run = hooks_during.lock().expect("during").take();
            if let Some(run) = run {
                run();
            }
            record.lock().expect("seen").push(Seen { hook, visible });
        }),
        log: Arc::new(move |line: &str| {
            td_logs.lock().expect("logs").push(line.to_owned());
        }),
    })
    .expect("open");
    path.set(td.clone()).expect("once");
    let driver = disp.clone();
    thread::spawn(move || driver.drive());
    Rig {
        peer,
        td,
        disp,
        seen,
        logs,
        during,
        _server: server,
    }
}

impl Rig {
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("seen").clone()
    }

    fn logged(&self, line: &str) -> bool {
        self.logs.lock().expect("logs").iter().any(|l| l == line)
    }

    /// The hooks of one account, by kind and stamp.
    fn stamps(&self, api_id: i64) -> Vec<(&'static str, Td)> {
        self.seen()
            .into_iter()
            .filter_map(|seen| {
                let (kind, td) = match seen.hook {
                    Hook::Order(ev) => ("order", ev.td),
                    Hook::Fill(ev) => ("fill", ev.td),
                    Hook::Position(ev) => ("position", ev.td),
                    Hook::Balance(ev) => ("balance", ev.td),
                    Hook::Recon(ev) => ("recon", ev.td),
                    Hook::Reject(ev) => ("reject", ev.td),
                };
                (td.api_id == api_id).then_some((kind, td))
            })
            .collect()
    }

    /// Listen, the first recon, and both accounts' OnRecon.
    fn reconciled(&self) {
        self.td.listen().expect("listen");
        self.td.recon().expect("recon");
        wait_until("both recons", || {
            self.stamps(7).iter().any(|(k, _)| *k == "recon")
                && self.stamps(8).iter().any(|(k, _)| *k == "recon")
        });
    }

    fn end(&self) -> RunEnd {
        let disp = self.disp.clone();
        let (tx, rx) = std_mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(disp.wait_run());
        });
        rx.recv_timeout(FUSE).expect("the session ended")
    }
}

fn buy(api_id: i64, algo_id: u32) -> Order {
    Order {
        api_id,
        algo_id,
        uid: 1001,
        side: common::Side::Buy,
        order_type: common::OrderType::Limit,
        tif: common::TimeInForce::Gtc,
        qty: Some(1.0),
        quote_qty: None,
        price: Some(1.0),
        recv_window_ms: 0,
        reduce_only: false,
        params: HashMap::new(),
    }
}

fn td_stamp(api_id: i64, incarnation: u32, seq: u64) -> Td {
    Td {
        api_id,
        incarnation,
        seq,
    }
}

fn order_hooks(seen: &[Seen]) -> Vec<(OrderEventView, bool)> {
    seen.iter()
        .filter_map(|seen| match &seen.hook {
            Hook::Order(ev) => Some((
                OrderEventView {
                    td: ev.td,
                    origin: ev.origin,
                    id: ev.order.client_order_id.clone(),
                    status: ev.order.status,
                    code: ev.code,
                    reason: ev.reason.clone(),
                },
                seen.visible,
            )),
            _ => None,
        })
        .collect()
}

#[derive(Clone, Debug, PartialEq)]
struct OrderEventView {
    td: Td,
    origin: Origin,
    id: String,
    status: i32,
    code: i32,
    reason: String,
}

// ---------- the tests ----------

#[test]
fn the_recon_watermark_releases_only_the_next_seq_and_api_id_is_the_subject() {
    let rig = rig(Duration::from_millis(500));
    rig.peer.serve_view(View::Inject);
    // Before listen the path refuses the calls.
    assert_eq!(
        rig.td.request_recon(7),
        Err(Error::Other("tdpath: td.evt is not subscribed yet".into()))
    );
    rig.td.listen().expect("listen");
    rig.td.recon().expect("recon");
    // The peer publishes two events of each account before its reply:
    // subscribed before the request, so they reach the hold. Account 7's
    // seq 4 and account 8's seq 5 are at or below the cursor, seq 6 is the
    // next one. No message says which account it is.
    wait_until("seq 6 of both accounts", || {
        rig.stamps(7)
            .iter()
            .any(|(k, td)| *k == "order" && td.seq == 6)
            && rig.stamps(8).iter().any(|(k, _)| *k == "position")
    });
    assert_eq!(
        rig.stamps(7),
        [
            ("recon", td_stamp(7, INCARNATION, SNAPSHOT_SEQ)),
            ("order", td_stamp(7, INCARNATION, 6)),
        ]
    );
    assert_eq!(
        rig.stamps(8),
        [
            ("recon", td_stamp(8, INCARNATION, SNAPSHOT_SEQ)),
            ("position", td_stamp(8, INCARNATION, 6)),
        ]
    );
    // OnOrder sees the order it is about; the ledger has the position.
    assert!(order_hooks(&rig.seen()).iter().all(|(_, visible)| *visible));
    assert_eq!(rig.td.position(8, 1001).map(|p| p.size), Some(2.0));
    assert!(rig.logged(QUEUED_LINE));

    // Live: seq 5 is behind the cursor, seq 7 is next.
    rig.peer.publish(7, order_event(5, INCARNATION));
    rig.peer.publish(7, order_event(7, INCARNATION));
    rig.peer.flush();
    wait_until("seq 7", || rig.stamps(7).len() == 3);
    thread::sleep(Duration::from_millis(50));
    assert_eq!(rig.stamps(7)[2], ("order", td_stamp(7, INCARNATION, 7)));
    assert_eq!(rig.stamps(7).len(), 3);
    assert_eq!(rig.peer.views.load(Ordering::SeqCst), 2);
}

#[test]
fn no_responder_is_a_101_local_reject() {
    let rig = rig(Duration::from_millis(500));
    rig.peer.serve_view(View::Reply);
    rig.reconciled();
    let id = rig.td.submit(buy(7, 1)).expect("submit");
    wait_until("the reject", || !order_hooks(&rig.seen()).is_empty());
    let (ev, visible) = order_hooks(&rig.seen()).remove(0);
    assert_eq!(
        ev,
        OrderEventView {
            td: td_stamp(7, 0, 0),
            origin: Origin::LocalReject,
            id: id.clone(),
            status: td::OrderStatus::Rejected as i32,
            code: td::RejectCode::AccountOffline as i32,
            reason: tdpath::REASON_OFFLINE.into(),
        }
    );
    assert!(visible, "the terminal order is visible inside OnOrder");
    wait_until("the order to leave the OMS", || {
        rig.td.order(7, &id).is_none()
    });
    assert!(rig.td.open_orders(7, 0).is_empty());

    // A cancel with no responder is a RejectEvent; nothing changes.
    let other = cloid::encode(1, cloid::EPOCH_MS + 5, 9).expect("cloid");
    rig.td.cancel(7, &other).expect("cancel");
    wait_until("the cancel reject", || {
        rig.seen().iter().any(|s| matches!(s.hook, Hook::Reject(_)))
    });
    let reject = rig
        .seen()
        .into_iter()
        .find_map(|s| match s.hook {
            Hook::Reject(ev) => Some(ev),
            _ => None,
        })
        .expect("reject");
    assert_eq!(reject.request, RejectedRequest::Cancel);
    assert_eq!(reject.client_order_ids, [other]);
    assert_eq!(reject.code, td::RejectCode::AccountOffline as i32);
    assert_eq!(reject.reason, tdpath::REASON_OFFLINE);
}

#[test]
fn a_submit_timeout_is_unknown_and_a_cancel_timeout_changes_nothing() {
    let window = Duration::from_millis(300);
    let rig = rig(window);
    rig.peer.serve_view(View::Reply);
    rig.peer.serve_orders(Orders::Silent);
    rig.reconciled();
    let sent = StdInstant::now();
    let id = rig.td.submit(buy(7, 1)).expect("submit");
    wait_until("the timeout", || !order_hooks(&rig.seen()).is_empty());
    assert!(sent.elapsed() >= window, "{:?}", sent.elapsed());
    let (ev, visible) = order_hooks(&rig.seen()).remove(0);
    assert_eq!(ev.origin, Origin::Timeout);
    assert_eq!(ev.status, td::OrderStatus::Unknown as i32);
    assert_eq!(ev.td, td_stamp(7, 0, 0));
    assert!(visible);
    // UNKNOWN is not terminal and is not asked about or sent again.
    let unknown = rig.td.order(7, &id).expect("still in the OMS");
    assert_eq!(unknown.status, td::OrderStatus::Unknown as i32);
    thread::sleep(window * 2);
    assert_eq!(rig.peer.requests().len(), 1);
    assert_eq!(rig.peer.views.load(Ordering::SeqCst), 2);

    rig.td.cancel(7, &id).expect("cancel");
    wait_until("the cancel request", || rig.peer.requests().len() == 2);
    thread::sleep(window * 2);
    assert_eq!(rig.seen().len(), 3, "two recons and the UNKNOWN, no more");
    assert_eq!(rig.td.order(7, &id), Some(unknown));
}

#[test]
fn the_hold_takes_65536_events_and_one_more_fails_the_session() {
    // The recon is never answered inside this window, so the events stay held.
    let rig = rig(Duration::from_secs(60));
    rig.peer.serve_view(View::Hold);
    rig.td.listen().expect("listen");
    rig.td.recon().expect("recon");
    wait_until("both asks", || rig.peer.views.load(Ordering::SeqCst) == 2);
    let cap = tdpath::HOLD_CAP as u64;
    let batch = 4096;
    let mut seq = 0;
    while seq < cap {
        let nats = rig.peer.nats.clone();
        let from = seq + 1;
        seq += batch;
        rig.peer
            .io
            .block_on(async move {
                for n in from..=from + batch - 1 {
                    let event = order_event(n, INCARNATION).encode_to_vec();
                    nats.publish("td.evt.7", event.into()).await?;
                }
                nats.flush().await.map_err(|err| err.to_string().into())
            })
            .map_err(|err: Box<dyn std::error::Error + Send + Sync>| err.to_string())
            .expect("publish");
        wait_until("the batch to reach the hold", || rig.td.fed() == seq);
    }
    assert_eq!(seq, cap);
    assert!(!rig.logged(tdpath::REASON_HOLD_FULL));
    assert!(rig.seen().is_empty());
    rig.peer.publish(8, order_event(1, INCARNATION));
    rig.peer.flush();
    assert_eq!(
        rig.end(),
        RunEnd::Failed {
            reason: tdpath::REASON_HOLD_FULL.into()
        }
    );
    assert!(rig.logged(tdpath::REASON_HOLD_FULL));
    assert_eq!(rig.td.submit(buy(7, 1)), Err(Error::Stopping));
}

#[test]
fn batch_cancel_splits_by_cancel_batch_and_ids_come_from_one_counter() {
    let rig = rig(Duration::from_millis(500));
    rig.peer.serve_view(View::Reply);
    rig.peer.serve_orders(Orders::Accept);
    rig.td.listen().expect("listen");
    assert_eq!(
        rig.td.batch_cancel(7, 1, &[]),
        Err(Error::Other(
            "tdpath: the account's cancel_batch is not known yet".into()
        ))
    );
    rig.td.recon().expect("recon");
    wait_until("both recons", || rig.seen().len() == 2);
    assert_eq!(rig.td.submit(buy(9, 1)), Err(Error::UnknownAccount));
    assert_eq!(rig.td.submit(buy(7, 2)), Err(Error::AlgoNotDeclared));

    let mut ids = Vec::new();
    for api_id in [7, 7, 8, 7, 8] {
        ids.push(rig.td.submit(buy(api_id, 1)).expect("submit"));
    }
    // One counter for the process: every id is algo 1 and later than the
    // one before, across both accounts.
    let decoded: Vec<_> = ids
        .iter()
        .map(|id| cloid::decode(id).expect("id"))
        .collect();
    for pair in decoded.windows(2) {
        assert!((pair[0].ts_ms, pair[0].seq) < (pair[1].ts_ms, pair[1].seq));
    }
    assert!(decoded.iter().all(|id| id.algo == 1 && id.ver == 1));
    let mut on7 = [ids[0].clone(), ids[1].clone(), ids[3].clone()];
    let mut on8 = [ids[2].clone(), ids[4].clone()];
    on7.sort();
    on8.sort();
    wait_until("the submits", || rig.peer.requests().len() == 5);
    rig.td.batch_cancel(7, 1, &[]).expect("batch 7");
    rig.td.batch_cancel(8, 1, &[1001]).expect("batch 8");
    wait_until("the cancels", || rig.peer.requests().len() == 9);
    let requests = rig.peer.requests();
    for (request, id) in requests[..5].iter().zip(&ids) {
        match &request.req {
            Some(td::order_request::Req::Submit(submit)) => {
                assert_eq!(&submit.client_order_id, id);
                assert_eq!(submit.session_id, SESSION);
            }
            other => panic!("want Submit, got {other:?}"),
        }
    }
    let batches: Vec<Vec<String>> = requests[5..7]
        .iter()
        .map(|request| match &request.req {
            Some(td::order_request::Req::BatchCancel(batch)) => batch
                .orders
                .iter()
                .map(|o| o.client_order_id.clone())
                .collect(),
            other => panic!("want BatchCancel, got {other:?}"),
        })
        .collect();
    assert_eq!(batches, [on7[..2].to_vec(), on7[2..].to_vec()]);
    let singles: Vec<String> = requests[7..]
        .iter()
        .map(|request| match &request.req {
            Some(td::order_request::Req::Cancel(cancel)) => {
                assert_eq!(cancel.session_id, SESSION);
                assert_eq!(cancel.uid, 1001);
                cancel.client_order_id.clone()
            }
            other => panic!("want Cancel, got {other:?}"),
        })
        .collect();
    assert_eq!(singles, on8);
    // Accepted: nothing changes until td.evt says so.
    thread::sleep(Duration::from_millis(50));
    assert_eq!(rig.seen().len(), 2);
    assert_eq!(rig.td.open_orders(7, 0).len(), 3);
}

#[test]
fn unsubscribe_stops_td_evt_and_the_drain_delivers_what_was_received() {
    let rig = rig(Duration::from_millis(500));
    rig.peer.serve_view(View::Inject);
    rig.reconciled();
    wait_until("seq 6", || rig.stamps(7).len() == 2);

    // A hook blocks while seq 7 and seq 8 reach the TD buffer, then ends the
    // session DONE.
    let (go, wait) = std_mpsc::channel::<()>();
    let ender = rig.disp.clone();
    rig.disp
        .push_td(move || {
            let _ = wait.recv_timeout(FUSE);
            ender.exit();
        })
        .expect("push");
    rig.peer.publish(7, order_event(7, INCARNATION));
    rig.peer.publish(7, order_event(8, INCARNATION));
    rig.peer.flush();
    wait_until("both on the TD buffer", || rig.disp.stats().td.depth >= 2);
    go.send(()).expect("go");
    assert_eq!(rig.end(), RunEnd::Done);

    // During the drain, after unsubscribe_td: seq 9 is published and is not
    // received, and order calls are refused.
    let (peer_nats, peer_io) = (rig.peer.nats.clone(), rig.peer.io.clone());
    let td = rig.td.clone();
    let refused = Arc::new(Mutex::new(None));
    let saw = Arc::clone(&refused);
    *rig.during.lock().expect("during") = Some(Box::new(move || {
        peer_io
            .block_on(async move {
                let event = order_event(9, INCARNATION).encode_to_vec();
                peer_nats.publish("td.evt.7", event.into()).await?;
                peer_nats
                    .flush()
                    .await
                    .map_err(|err| err.to_string().into())
            })
            .map_err(|err: Box<dyn std::error::Error + Send + Sync>| err.to_string())
            .expect("publish seq 9");
        thread::sleep(Duration::from_millis(100));
        *saw.lock().expect("refused") = Some(td.submit(buy(7, 1)));
    }));
    rig.disp.on_stop(&StopRequest::new(None)).expect("on_stop");
    let seqs: Vec<u64> = rig.stamps(7).iter().map(|(_, td)| td.seq).collect();
    assert_eq!(seqs, [SNAPSHOT_SEQ, 6, 7, 8]);
    assert_eq!(
        *refused.lock().expect("refused"),
        Some(Err(Error::Stopping))
    );
    assert_eq!(rig.td.request_recon(7), Err(Error::Stopping));
    // Idempotent.
    rig.td.unsubscribe();
}

#[test]
fn after_unsubscribe_a_queued_recon_is_delivered_and_its_release_stops() {
    let rig = rig(Duration::from_millis(500));
    rig.peer.serve_view(View::Reply);
    rig.reconciled();
    assert_eq!(rig.stamps(7).len(), 1);

    let (go, wait) = std_mpsc::channel::<()>();
    let ender = rig.disp.clone();
    rig.disp
        .push_td(move || {
            let _ = wait.recv_timeout(FUSE);
            ender.exit();
        })
        .expect("push");
    // Seq 6 and seq 7 queue up live; the RequestRecon makes the account hold
    // before they are fed, so the drain holds them and then feeds the reply.
    rig.peer.publish(7, order_event(6, INCARNATION));
    rig.peer.publish(7, order_event(7, INCARNATION));
    rig.peer.flush();
    wait_until("both events on the TD buffer", || {
        rig.disp.stats().td.depth >= 2
    });
    rig.td.request_recon(7).expect("request recon");
    wait_until("the reply on the TD buffer", || {
        rig.disp.stats().td.depth >= 3
    });
    go.send(()).expect("go");
    assert_eq!(rig.end(), RunEnd::Done);
    rig.disp.on_stop(&StopRequest::new(None)).expect("on_stop");
    // Go: OnRecon runs (`beginReply`), then `releaseStep` sees the closed
    // path and releases nothing.
    assert_eq!(
        rig.stamps(7),
        [
            ("recon", td_stamp(7, INCARNATION, SNAPSHOT_SEQ)),
            ("recon", td_stamp(7, INCARNATION, SNAPSHOT_SEQ)),
        ]
    );
    assert_eq!(rig.peer.views.load(Ordering::SeqCst), 3);
}

/// What account 7 and 8 hold when the strategy `on_stop` runs.
#[derive(Clone, Debug)]
struct StopLedger {
    early: Option<i32>,
    submitted: Option<i32>,
    late: Option<i32>,
    trigger: Option<i32>,
    held: Option<i32>,
    planted: Option<i32>,
    pos7: Option<f64>,
    pos8: Option<f64>,
    usd: Option<(f64, f64, f64)>,
    eur: Option<f64>,
    open7: usize,
    open8: usize,
}

#[test]
fn on_stop_keeps_events_applied_before_hold_full_and_drops_the_rest() {
    // E05-D051 (revised) through E05-D054. Account 7 is live; its order,
    // position, and balance are applied before the hold fills. Account 8
    // stays holding. The dispatcher is blocked so the 65537th event, a later
    // in-sequence event, the parked recon reply, and the order reject sit
    // behind that trigger. `on_stop` runs after the drain, which is when the
    // strategy reads the OMS.
    let snap: Arc<Mutex<Option<StopLedger>>> = Arc::default();
    let submitted_id: Arc<Mutex<Option<String>>> = Arc::default();
    let (record, submitted_for_stop) = (Arc::clone(&snap), Arc::clone(&submitted_id));
    let early_id = event_id(6);
    let late_id = event_id(9);
    let trigger_id = event_id(65_537);
    let held_id = event_id(1);
    let planted_id = event_id(70_000);
    let planted_reply = planted_id.clone();
    let rig = rig_with(
        Duration::from_secs(60),
        Some(Box::new(move |td, _| {
            let submitted = submitted_for_stop.lock().expect("id").clone();
            let usd = td.balance(7, "usd");
            *record.lock().expect("snap") = Some(StopLedger {
                early: td.order(7, &early_id).map(|order| order.status),
                submitted: submitted.and_then(|id| td.order(7, &id).map(|order| order.status)),
                late: td.order(7, &late_id).map(|order| order.status),
                trigger: td.order(8, &trigger_id).map(|order| order.status),
                held: td.order(8, &held_id).map(|order| order.status),
                planted: td.order(8, &planted_id).map(|order| order.status),
                pos7: td.position(7, 1001).map(|position| position.size),
                pos8: td.position(8, 4).map(|position| position.size),
                usd: usd.map(|balance| (balance.free, balance.locked, balance.prelock)),
                eur: td.balance(8, "eur").map(|balance| balance.free),
                open7: td.open_orders(7, 0).len(),
                open8: td.open_orders(8, 0).len(),
            });
            Ok(())
        })),
    );
    let parked = rig.peer.serve_live_and_parked();
    rig.td.listen().expect("listen");
    rig.td.request_recon(7).expect("recon 7");
    wait_until("account 7 recon", || {
        rig.stamps(7).iter().any(|(kind, _)| *kind == "recon")
    });
    assert!(rig.stamps(8).is_empty());

    rig.peer.publish(7, order_event(6, INCARNATION));
    rig.peer.publish(7, position_event(7, INCARNATION));
    rig.peer.publish(7, balance_event(8, INCARNATION));
    rig.peer.flush();
    wait_until("the three live hooks", || rig.stamps(7).len() == 4);

    let base = rig.td.fed();
    let cap = tdpath::HOLD_CAP as u64;
    let batch = 4096;
    let mut seq = 0;
    while seq < cap {
        let nats = rig.peer.nats.clone();
        let from = seq + 1;
        seq += batch;
        rig.peer
            .io
            .block_on(async move {
                for n in from..=from + batch - 1 {
                    let event = order_event(n, INCARNATION).encode_to_vec();
                    nats.publish(tdpath::evt_subject(8), event.into()).await?;
                }
                nats.flush().await.map_err(|err| err.to_string().into())
            })
            .map_err(|err: Box<dyn std::error::Error + Send + Sync>| err.to_string())
            .expect("publish");
        let seen = base + seq;
        wait_until("the batch to reach the hold", || rig.td.fed() == seen);
    }
    assert_eq!(seq, cap);
    assert!(!rig.logged(tdpath::REASON_HOLD_FULL));
    assert!(rig.stamps(8).is_empty());

    let id = rig.td.submit(buy(7, 1)).expect("submit");
    *submitted_id.lock().expect("id") = Some(id);
    let order_inbox = parked.order_inbox();
    rig.td.request_recon(8).expect("recon 8");
    let view_inbox = parked.view_inbox();

    let (go, wait) = std_mpsc::channel::<()>();
    let entered = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&entered);
    rig.disp
        .push_td(move || {
            flag.store(true, Ordering::SeqCst);
            let _ = wait.recv_timeout(FUSE);
        })
        .expect("blocker");
    wait_until("the blocker is running", || entered.load(Ordering::SeqCst));
    assert_eq!(rig.disp.stats().td.depth, 0);

    rig.peer.publish(8, order_event(65_537, INCARNATION));
    rig.peer.flush();
    wait_until("the trigger is queued", || rig.disp.stats().td.depth >= 1);
    assert_eq!(rig.disp.stats().td.depth, 1);

    rig.peer.publish(7, order_event(9, INCARNATION));
    rig.peer.flush();
    wait_until("the late event is queued", || {
        rig.disp.stats().td.depth >= 2
    });
    assert_eq!(rig.disp.stats().td.depth, 2);

    parked.publish(
        view_inbox,
        td::ViewReply {
            incarnation: INCARNATION,
            seq: SNAPSHOT_SEQ,
            cancel_batch: 1,
            reply: Some(td::view_reply::Reply::Recon(td::Recon {
                open_orders: vec![td::Order {
                    uid: 4,
                    client_order_id: planted_reply,
                    status: td::OrderStatus::New as i32,
                    ..Default::default()
                }],
                positions: vec![td::Position {
                    uid: 4,
                    size: 9.0,
                    ..Default::default()
                }],
                balances: vec![td::Balance {
                    asset: "eur".into(),
                    free: 4.0,
                    locked: 0.0,
                    prelock: 0.0,
                }],
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    wait_until("the recon reply is queued", || {
        rig.disp.stats().td.depth >= 3
    });
    assert_eq!(rig.disp.stats().td.depth, 3);

    parked.publish(
        order_inbox,
        td::OrderReply {
            accepted: false,
            code: 101,
            reason: "down".into(),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    wait_until("the order reply is queued", || {
        rig.disp.stats().td.depth >= 4
    });
    assert_eq!(rig.disp.stats().td.depth, 4);

    go.send(()).expect("go");
    assert_eq!(
        rig.end(),
        RunEnd::Failed {
            reason: tdpath::REASON_HOLD_FULL.into()
        }
    );
    assert!(rig.logged(tdpath::REASON_HOLD_FULL));
    assert_eq!(rig.td.submit(buy(7, 1)), Err(Error::Stopping));
    assert_eq!(rig.td.request_recon(7), Err(Error::Stopping));
    assert_eq!(rig.td.request_recon(8), Err(Error::Stopping));

    rig.disp.on_stop(&StopRequest::new(None)).expect("on_stop");
    let snap = snap.lock().expect("snap").clone().expect("on_stop ran");
    assert_eq!(snap.early, Some(td::OrderStatus::New as i32));
    assert_eq!(snap.submitted, Some(td::OrderStatus::PendingNew as i32));
    assert_eq!(snap.late, None);
    assert_eq!(snap.trigger, None);
    assert_eq!(snap.held, None);
    assert_eq!(snap.planted, None);
    assert_eq!(snap.pos7, Some(2.0));
    assert_eq!(snap.pos8, None);
    assert_eq!(snap.usd, Some((1.0, 2.0, 3.0)));
    assert_eq!(snap.eur, None);
    assert_eq!(snap.open7, 2);
    assert_eq!(snap.open8, 0);
    assert_eq!(
        rig.stamps(7),
        [
            ("recon", td_stamp(7, INCARNATION, SNAPSHOT_SEQ)),
            ("order", td_stamp(7, INCARNATION, 6)),
            ("position", td_stamp(7, INCARNATION, 7)),
            ("balance", td_stamp(7, INCARNATION, 8)),
        ]
    );
    assert!(rig.stamps(8).is_empty());
}
