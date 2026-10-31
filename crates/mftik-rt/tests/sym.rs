//! The SYM path over a real nats-server, against a fake SYM in this process
//! (E03-08, PROTOCOL §7.3, §9.4.8).
//!
//! Most tests drive [`mftik_rt::sym::Sym`] directly, as the plane does: the
//! session's connection comes from `mftik_rt::nats::dial`, so its reconnects
//! reach the path the way they do in a session. The last tests run a session
//! worker through `mftik_rt::run` (this test binary re-run as the worker with
//! fd 3 on a socketpair, as `tests/session.rs` does) to check that resolve
//! blocks inside OnInit and OnStart and fails at once in other hooks.
//!
//! The server runs without auth: these tests are about SYM, and
//! `tests/session.rs` covers the JWT login. nats-server comes from
//! `MFTIK_NATS_SERVER`, else `PATH`; a test that cannot find it fails.

use std::collections::BTreeMap;
use std::env;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use mftik_core::symbol::{PRICE_TICK, Status};
use mftik_core::symclient::{Config, Error};
use mftik_proto::wire::mftiko::stsctl::v1::{Bindings, InitPayload, NatsCredentials, StartPayload};
use mftik_proto::wire::mftiko::sym::v1::{self as sym, reply, request};
use mftik_proto::wire::mftiko::worker::v1::{
    self as worker, WorkerDown, WorkerUp, worker_down, worker_up,
};
use mftik_rt::call::CallCtx;
use mftik_rt::clock::Clock;
use mftik_rt::frame::{FrameReader, FrameWriter};
use mftik_rt::io::Io;
use mftik_rt::sym::Sym;
use mftik_rt::timers::Timers;
use mftik_rt::{Context, HookError, Strategy};
use mftik_testkit::NatsServer;
use nkeys::KeyPair;
use prost::Message;

const FUSE: Duration = Duration::from_secs(20);
const WORKER_ENV: &str = "MFTIK_RT_SYM_TEST_WORKER";

const BTC: u32 = 1001;
const ETH: u32 = 1002;
const BTC_T: &str = "Bybit_Spot_BTCUSDT";
const ETH_T: &str = "Bybit_Spot_ETHUSDT";
const MISSING_T: &str = "Bybit_Spot_XRPUSDT";
const SPOT_SUB: &str = "sym.v1.changed.Bybit.Spot";

// ---------- nats-server ----------

/// nats-server without auth, on a client port it picks.
fn start_server() -> NatsServer {
    NatsServer::start(["-a", "127.0.0.1", "-p", "-1"])
}

/// The session's credentials. The server has no auth, so any JWT will do;
/// the seed must be an nkey seed for `dial`.
fn creds() -> NatsCredentials {
    NatsCredentials {
        user_jwt: "test-jwt".into(),
        seed: KeyPair::new_user().seed().expect("seed"),
        inbox_prefix: "_INBOX_symtest".into(),
    }
}

/// A TCP relay between the session and the server. [`Relay::cut`] drops
/// every connection through it, so the session's client reconnects while
/// the fake SYM, connected directly, stays up.
struct Relay {
    port: u16,
    conns: Arc<Mutex<Vec<TcpStream>>>,
}

impl Relay {
    fn start(upstream: u16) -> Relay {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind relay");
        let port = listener.local_addr().expect("addr").port();
        let conns: Arc<Mutex<Vec<TcpStream>>> = Arc::default();
        let kept = Arc::clone(&conns);
        thread::spawn(move || {
            for down in listener.incoming() {
                let Ok(down) = down else { return };
                let Ok(up) = TcpStream::connect(("127.0.0.1", upstream)) else {
                    continue;
                };
                for (from, to) in [(&down, &up), (&up, &down)] {
                    let (mut from, mut to) = (
                        from.try_clone().expect("clone"),
                        to.try_clone().expect("clone"),
                    );
                    thread::spawn(move || {
                        let mut buf = [0u8; 16 * 1024];
                        while let Ok(n) = from.read(&mut buf) {
                            if n == 0 || to.write_all(&buf[..n]).is_err() {
                                break;
                            }
                        }
                        let _ = to.shutdown(Shutdown::Both);
                        let _ = from.shutdown(Shutdown::Both);
                    });
                }
                lock(&kept).extend([down, up]);
            }
        });
        Relay { port, conns }
    }

    fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    fn cut(&self) {
        for conn in lock(&self.conns).drain(..) {
            let _ = conn.shutdown(Shutdown::Both);
        }
    }
}

// ---------- the fake SYM ----------

#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Get(String),
    ByUid(u32, bool),
    Rev,
}

#[derive(Default)]
struct State {
    active: BTreeMap<u32, sym::SymbolInfo>,
    terminal: BTreeMap<u32, sym::SymbolInfo>,
    revision: i64,
    calls: Vec<Call>,
    get_delay: Duration,
    /// `get_delay` applies to the first Get only.
    first_get_only: bool,
    by_uid_delay: Duration,
}

impl State {
    fn answer(&mut self, payload: &[u8]) -> (Vec<u8>, Duration) {
        let req = sym::Request::decode(payload).expect("a sym.v1.Request");
        match req.req.expect("request is set") {
            request::Req::Get(get) => {
                self.calls.push(Call::Get(get.universal_ticker.clone()));
                let row = self
                    .active
                    .values()
                    .find(|row| row.universal_ticker == get.universal_ticker);
                let delay = self.get_delay;
                if self.first_get_only {
                    self.get_delay = Duration::ZERO;
                }
                (row.map_or_else(not_found, info_body), delay)
            }
            request::Req::GetByUid(by) => {
                self.calls.push(Call::ByUid(by.uid, by.include_inactive));
                let row = self.active.get(&by.uid).or_else(|| {
                    by.include_inactive
                        .then(|| self.terminal.get(&by.uid))
                        .flatten()
                });
                (row.map_or_else(not_found, info_body), self.by_uid_delay)
            }
            request::Req::Revisions(_) => {
                self.calls.push(Call::Rev);
                let list = sym::RevisionList {
                    tables: vec![sym::TableRevision {
                        venue: "Bybit".into(),
                        category: "Spot".into(),
                        revision: self.revision,
                    }],
                };
                let body = sym::Reply {
                    result: Some(reply::Result::Revisions(list)),
                    ..Default::default()
                };
                (body.encode_to_vec(), Duration::ZERO)
            }
            other => panic!("unexpected request {other:?}"),
        }
    }
}

fn listing(uid: u32, ticker: &str, tick: f64) -> sym::SymbolInfo {
    let mut filters = std::collections::HashMap::new();
    filters.insert(PRICE_TICK.to_owned(), sym::Bound { value: Some(tick) });
    sym::SymbolInfo {
        universal_ticker: ticker.to_owned(),
        exch_ticker: ticker.rsplit('_').next().unwrap_or_default().to_owned(),
        base: "BTC".into(),
        quote: "USDT".into(),
        status: sym::SymbolStatus::Tradable as i32,
        filters,
        updated_at_ns: 10,
        uid,
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

/// natsx error reply, code 2 `not_found`.
fn not_found() -> Vec<u8> {
    let text = b"not_found";
    let mut body = vec![0, 1, 0, 2];
    body.extend_from_slice(&(text.len() as u16).to_be_bytes());
    body.extend_from_slice(text);
    body
}

/// SYM with BTC and ETH listed in `Bybit Spot` at revision 1.
struct FakeSym {
    io: Io,
    nats: async_nats::Client,
    state: Arc<Mutex<State>>,
}

impl FakeSym {
    fn start(url: &str) -> FakeSym {
        let io = Io::start().expect("io");
        let mut state = State {
            revision: 1,
            ..Default::default()
        };
        state.active.insert(BTC, listing(BTC, BTC_T, 0.01));
        state.active.insert(ETH, listing(ETH, ETH_T, 0.05));
        let state = Arc::new(Mutex::new(state));
        let url = url.to_owned();
        let nats = io
            .block_on(async move { async_nats::connect(url).await })
            .expect("fake sym connects");
        let (client, served) = (nats.clone(), Arc::clone(&state));
        io.block_on(async move {
            let mut sub = client.subscribe("sym").await.expect("subscribe sym");
            client.flush().await.expect("flush");
            tokio::spawn(async move {
                while let Some(msg) = sub.next().await {
                    let Some(reply_to) = msg.reply.clone() else {
                        continue;
                    };
                    let (body, delay) = lock(&served).answer(&msg.payload);
                    let client = client.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = client.publish(reply_to, body.into()).await;
                        let _ = client.flush().await;
                    });
                }
            });
        });
        FakeSym { io, nats, state }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    fn calls_since(&self, mark: usize) -> Vec<Call> {
        self.state().calls[mark..].to_vec()
    }

    fn mark(&self) -> usize {
        self.state().calls.len()
    }

    /// Publishes `sym.v1.changed.Bybit.Spot` with `revision`.
    fn changed(&self, revision: i64) {
        let msg = sym::Changed {
            venue: "Bybit".into(),
            category: "Spot".into(),
            revision,
        };
        let nats = self.nats.clone();
        self.io.block_on(async move {
            nats.publish(SPOT_SUB, msg.encode_to_vec().into())
                .await
                .expect("publish");
            nats.flush().await.expect("flush");
        });
    }

    /// Sets the BTC row's price tick and moves the table to `revision`,
    /// without a notification.
    fn retick(&self, tick: f64, revision: i64) {
        let mut state = self.state();
        state.active.insert(BTC, listing(BTC, BTC_T, tick));
        state.revision = revision;
    }
}

// ---------- the session side ----------

struct Session {
    sym: Sym,
    stored: Arc<Mutex<Vec<(u32, f64)>>>,
    logs: Arc<Mutex<Vec<String>>>,
    _nats: async_nats::Client,
}

impl Session {
    fn open(url: &str, config: Config) -> Session {
        let io = Io::start().expect("io");
        let clock = Clock::new();
        let timers = Timers::new(io.clone(), clock);
        let sym = Sym::new(config).expect("config");
        let (url, creds, hook) = (url.to_owned(), creds(), sym.reconnect_hook());
        let nats = io
            .block_on(async move {
                mftik_rt::nats::dial(&url, &creds, Duration::from_secs(2), Some(hook)).await
            })
            .expect("session dials");
        let logs: Arc<Mutex<Vec<String>>> = Arc::default();
        let kept = Arc::clone(&logs);
        sym.open(
            &io,
            clock,
            &timers,
            &nats,
            Arc::new(move |line: &str| lock(&kept).push(line.to_owned())),
        );
        let stored: Arc<Mutex<Vec<(u32, f64)>>> = Arc::default();
        let kept = Arc::clone(&stored);
        sym.on_stored(Arc::new(move |uid, info| {
            lock(&kept).push((uid, info.price_tick()));
        }));
        Session {
            sym,
            stored,
            logs,
            _nats: nats,
        }
    }

    fn tick(&self, uid: u32) -> f64 {
        self.sym.info(uid).expect("resolved").price_tick()
    }

    fn stored(&self) -> Vec<(u32, f64)> {
        lock(&self.stored).clone()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[track_caller]
fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < FUSE, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn within(after: Duration) -> CallCtx {
    CallCtx::with_deadline(Some(1), Some(tokio::time::Instant::now() + after))
}

// ---------- changes, revisions, reconnect, the terminal table ----------

#[test]
fn a_changed_notification_refetches_the_table_and_reports_the_rows() {
    let server = start_server();
    let fake = FakeSym::start(server.url());
    let session = Session::open(server.url(), Config::default());

    let info = session
        .sym
        .resolve(&CallCtx::background(), BTC_T)
        .expect("resolve");
    assert_eq!((info.uid, info.price_tick()), (BTC, 0.01));
    assert_eq!(fake.calls_since(0)[0], Call::Get(BTC_T.into()));
    assert_eq!(session.stored().last(), Some(&(BTC, 0.01)));
    assert_eq!(session.sym.info(ETH), Err(Error::UnknownUid));

    // A notification at the confirmed revision does nothing; a newer one
    // refetches the table's rows. NATS keeps their order.
    let mark = fake.mark();
    fake.changed(1);
    fake.retick(0.5, 2);
    fake.changed(2);
    wait_until("the new tick", || session.tick(BTC) == 0.5);
    wait_until("the pass to end", || fake.calls_since(mark).len() >= 3);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        fake.calls_since(mark),
        [Call::Rev, Call::ByUid(BTC, false), Call::Rev]
    );
    assert_eq!(session.stored().last(), Some(&(BTC, 0.5)));

    // The revision is confirmed: the same notification again sends nothing.
    let mark = fake.mark();
    fake.changed(2);
    thread::sleep(Duration::from_millis(200));
    assert!(fake.calls_since(mark).is_empty());
    assert!(lock(&session.logs).is_empty(), "{:?}", lock(&session.logs));
}

#[test]
fn the_revision_check_finds_a_table_that_fell_behind() {
    let server = start_server();
    let fake = FakeSym::start(server.url());
    let config = Config {
        check_interval: Duration::from_millis(200),
    };
    let session = Session::open(server.url(), config);
    session
        .sym
        .resolve(&CallCtx::background(), BTC_T)
        .expect("resolve");
    // No notification: only the timer can see this.
    fake.retick(0.25, 7);
    wait_until("the check to refetch", || session.tick(BTC) == 0.25);
    assert_eq!(session.stored().last(), Some(&(BTC, 0.25)));
    // Checks go on, and find nothing more to do.
    let mark = fake.mark();
    wait_until("two more checks", || fake.calls_since(mark).len() >= 2);
    assert!(fake.calls_since(mark).iter().all(|c| *c == Call::Rev));
}

#[test]
fn a_reconnect_compares_revisions_at_once() {
    let server = start_server();
    let fake = FakeSym::start(server.url());
    let relay = Relay::start(server.port());
    // The default check interval is five minutes: only the reconnect can
    // find this in time.
    let session = Session::open(&relay.url(), Config::default());
    session
        .sym
        .resolve(&CallCtx::background(), BTC_T)
        .expect("resolve");
    fake.retick(0.125, 3);
    let mark = fake.mark();
    relay.cut();
    wait_until("the refetch after reconnect", || session.tick(BTC) == 0.125);
    // The row is stored before the pass sends its closing Revisions.
    wait_until("the pass to end", || fake.calls_since(mark).len() >= 4);
    assert_eq!(
        fake.calls_since(mark),
        [Call::Rev, Call::Rev, Call::ByUid(BTC, false), Call::Rev]
    );
    // The changed subscription came back with the connection.
    fake.retick(0.5, 4);
    fake.changed(4);
    wait_until("a notification after reconnect", || {
        session.tick(BTC) == 0.5
    });
}

#[test]
fn a_row_moved_to_the_terminal_table_keeps_its_last_copy() {
    let server = start_server();
    let fake = FakeSym::start(server.url());
    let session = Session::open(server.url(), Config::default());
    session
        .sym
        .resolve(&CallCtx::background(), BTC_T)
        .expect("resolve");
    {
        let mut state = fake.state();
        let mut row = state.active.remove(&BTC).expect("row");
        row.status = sym::SymbolStatus::Delisted as i32;
        state.terminal.insert(BTC, row);
        state.revision = 2;
    }
    let mark = fake.mark();
    fake.changed(2);
    wait_until("the delisted row", || {
        session.sym.info(BTC).expect("kept").status == Status::Delisted
    });
    assert_eq!(
        fake.calls_since(mark),
        [
            Call::Rev,
            Call::ByUid(BTC, false),
            Call::ByUid(BTC, true),
            Call::Rev
        ]
    );
    assert_eq!(session.stored().last(), Some(&(BTC, 0.01)));
    // A ticker SYM does not list is NotFound and not tracked.
    assert_eq!(
        session.sym.resolve(&CallCtx::background(), MISSING_T),
        Err(Error::NotFound("not_found".into()))
    );
}

// ---------- how long a resolve waits ----------

#[test]
fn a_resolve_waits_until_the_hook_deadline_past_five_seconds() {
    let server = start_server();
    let fake = FakeSym::start(server.url());
    let session = Session::open(server.url(), Config::default());
    fake.state().get_delay = Duration::from_millis(5_600);
    let started = Instant::now();
    let info = session
        .sym
        .resolve(&within(Duration::from_secs(9)), BTC_T)
        .expect("resolve past 5 s");
    assert_eq!(info.uid, BTC);
    assert!(started.elapsed() >= Duration::from_millis(5_600));
}

#[test]
fn without_a_deadline_a_resolve_waits_five_seconds() {
    let server = start_server();
    let fake = FakeSym::start(server.url());
    let session = Session::open(server.url(), Config::default());
    fake.state().get_delay = Duration::from_secs(8);
    let started = Instant::now();
    let got = session.sym.resolve(&CallCtx::background(), BTC_T);
    let took = started.elapsed();
    assert_eq!(
        got,
        Err(Error::Failed(
            "symclient: request sym: context deadline exceeded".into()
        ))
    );
    assert!(
        took >= Duration::from_millis(4_900) && took < Duration::from_millis(7_000),
        "{took:?}"
    );
}

#[test]
fn the_hook_deadline_or_a_cancel_ends_a_resolve() {
    let server = start_server();
    let fake = FakeSym::start(server.url());
    let session = Session::open(server.url(), Config::default());
    fake.state().get_delay = Duration::from_secs(3);

    let started = Instant::now();
    let got = session
        .sym
        .resolve(&within(Duration::from_millis(300)), BTC_T);
    assert_eq!(
        got,
        Err(Error::Failed(
            "symclient: request sym: context deadline exceeded".into()
        ))
    );
    assert!(started.elapsed() < Duration::from_secs(2));

    let call = CallCtx::background();
    let cancel = call.clone();
    let started = Instant::now();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        cancel.cancel();
    });
    let got = session.sym.resolve(&call, BTC_T);
    canceller.join().expect("canceller");
    assert_eq!(
        got,
        Err(Error::Failed(
            "symclient: request sym: context canceled".into()
        ))
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    // Nothing was stored.
    assert_eq!(session.sym.info(BTC), Err(Error::UnknownUid));
}

#[test]
fn a_resolve_that_meets_a_refetch_finishes_with_it() {
    let server = start_server();
    let fake = FakeSym::start(server.url());
    let session = Session::open(server.url(), Config::default());
    session
        .sym
        .resolve(&CallCtx::background(), BTC_T)
        .expect("resolve");
    // A slow refetch of the table is under way...
    fake.state().by_uid_delay = Duration::from_millis(700);
    fake.retick(0.5, 2);
    let mark = fake.mark();
    fake.changed(2);
    wait_until("the refetch to start", || {
        fake.calls_since(mark).contains(&Call::ByUid(BTC, false))
    });
    // ...when ETH, in the same table, is resolved. It ends with the pass,
    // which must take ETH in before it confirms a revision.
    let started = Instant::now();
    let info = session
        .sym
        .resolve(&CallCtx::background(), ETH_T)
        .expect("resolve ETH");
    assert_eq!(info.uid, ETH);
    assert!(started.elapsed() >= Duration::from_millis(400));
    assert_eq!(session.tick(BTC), 0.5);
    let calls = fake.calls_since(mark);
    assert!(calls.contains(&Call::Get(ETH_T.into())), "{calls:?}");
    assert!(calls.contains(&Call::ByUid(ETH, false)), "{calls:?}");
}

// ---------- through a session worker ----------

/// The worker's strategy. `InitPayload.params` names the scenario.
#[derive(Default)]
struct Worker {
    scenario: String,
}

fn line(text: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{{\"hook\":\"{text}\"}}");
    let _ = out.flush();
}

impl Strategy for Worker {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        self.scenario = String::from_utf8(ctx.params().to_vec())?;
        let ticker = if self.scenario == "missing" {
            MISSING_T
        } else {
            BTC_T
        };
        let info = ctx.resolve(ticker)?;
        line(&format!("init {} {}", info.uid, info.price_tick()));
        Ok(())
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        let info = ctx.resolve(ETH_T)?;
        line(&format!("start {}", info.uid));
        ctx.post(|_, ctx| {
            let got = ctx.resolve(BTC_T).map(|info| info.uid);
            line(&format!("post {got:?}"));
            let tick = ctx.info(BTC).map(|info| info.price_tick());
            line(&format!("info {tick:?}"));
            ctx.exit();
        });
        Ok(())
    }

    fn on_stop(&mut self, ctx: &Context<Self>) {
        let got = ctx.resolve(BTC_T).map(|info| info.uid);
        line(&format!("stop {got:?}"));
    }
}

#[test]
fn worker_entry() {
    if env::var_os(WORKER_ENV).is_some() {
        mftik_rt::run(Worker::default(), None);
    }
}

struct Ran {
    code: Option<i32>,
    frames: Vec<worker_up::Msg>,
    lines: Vec<String>,
    stderr: String,
}

/// Runs a worker through Init (with `deadline_ms`) and, if Init succeeds,
/// Start, then reads fd 3 to EOF.
fn run_worker(url: &str, scenario: &str, deadline_ms: u32) -> Ran {
    let (ours, theirs) = UnixStream::pair().expect("socketpair");
    ours.set_read_timeout(Some(FUSE)).expect("read timeout");
    let fd = theirs.as_raw_fd();
    let mut cmd = Command::new(env::current_exe().expect("test binary"));
    cmd.args(["worker_entry", "--exact", "--nocapture", "--test-threads=1"])
        .env(WORKER_ENV, "1")
        .env("MFTIKO_CTL_FD", "3")
        .env("MFTIKO_NATS_URL", url)
        .env_remove("MFTIKO_VOLUME_DIR")
        .env_remove("MFTIKO_INPUT_DIR")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: dup2 is async-signal-safe; the new fd 3 is inherited.
    unsafe {
        cmd.pre_exec(move || {
            if libc::dup2(fd, 3) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = cmd.spawn().expect("spawn worker");
    drop(theirs);
    let out = FrameWriter::new(ours.try_clone().expect("clone"));
    let mut frames = FrameReader::new(ours);
    let send = |msg| {
        out.send(&WorkerDown { msg: Some(msg) })
            .expect("send to worker")
    };
    let init = InitPayload {
        session_id: "c0ffee".into(),
        params: scenario.as_bytes().to_vec(),
        nats: Some(creds()),
        recv_window_ms: 500,
        hook_timeout_s: 10,
        ..Default::default()
    };
    send(worker_down::Msg::Init(worker::Init {
        contract: "stsctl.v1".into(),
        payload: init.encode_to_vec(),
        deadline_ms,
        live_digest: "live-1".into(),
    }));
    let mut seen = Vec::new();
    let mut started = false;
    while let Some(up) = frames.read_message::<WorkerUp>().expect("read from worker") {
        let msg = up.msg.expect("a WorkerUp message");
        if matches!(msg, worker_up::Msg::Beat(_) | worker_up::Msg::Report(_)) {
            continue;
        }
        let init_ok = matches!(&msg, worker_up::Msg::Done(done)
            if done.verb == worker::Verb::Init as i32 && done.ok);
        seen.push(msg);
        if init_ok && !started {
            started = true;
            send(worker_down::Msg::Start(worker::Start {
                contract: "stsctl.v1".into(),
                payload: StartPayload {
                    bindings: Some(Bindings::default()),
                }
                .encode_to_vec(),
                deadline_ms: 5000,
            }));
        }
    }
    let start = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("wait") {
            break status;
        }
        assert!(start.elapsed() < FUSE, "worker did not exit");
        thread::sleep(Duration::from_millis(10));
    };
    let (mut stdout, mut stderr) = (String::new(), String::new());
    let _ = child
        .stdout
        .take()
        .expect("stdout")
        .read_to_string(&mut stdout);
    let _ = child
        .stderr
        .take()
        .expect("stderr")
        .read_to_string(&mut stderr);
    let lines = stdout
        .lines()
        .filter_map(|l| l.find("{\"hook\":\"").map(|at| l[at + 9..].to_owned()))
        .map(|l| l.trim_end_matches("\"}").to_owned())
        .collect();
    Ran {
        code: status.code(),
        frames: seen,
        lines,
        stderr,
    }
}

fn done_error(ran: &Ran, verb: worker::Verb) -> Option<String> {
    ran.frames.iter().find_map(|msg| match msg {
        worker_up::Msg::Done(done) if done.verb == verb as i32 => Some(done.error.clone()),
        _ => None,
    })
}

#[test]
fn resolve_blocks_in_on_init_and_on_start_and_fails_in_other_hooks() {
    let server = start_server();
    let fake = FakeSym::start(server.url());
    // Past the request's 5 s, inside Init's deadline_ms: the plane passes the
    // hook's context to the resolve.
    {
        let mut state = fake.state();
        state.get_delay = Duration::from_millis(5_400);
        state.first_get_only = true;
    }
    let ran = run_worker(server.url(), "", 9_000);
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(done_error(&ran, worker::Verb::Init).as_deref(), Some(""));
    assert_eq!(done_error(&ran, worker::Verb::Start).as_deref(), Some(""));
    assert_eq!(
        ran.lines,
        [
            "init 1001 0.01",
            "gate",
            "start 1002",
            "post Err(NotInInit)",
            "info Ok(0.01)",
            "stop Err(NotInInit)",
        ],
        "{}",
        ran.stderr
    );
    // The resolves outside OnInit and OnStart sent nothing.
    let gets = fake
        .state()
        .calls
        .iter()
        .filter(|c| matches!(c, Call::Get(_)))
        .count();
    assert_eq!(gets, 2);
}

#[test]
fn an_unknown_ticker_from_on_init_is_init_failed() {
    let server = start_server();
    let _fake = FakeSym::start(server.url());
    let ran = run_worker(server.url(), "missing", 5_000);
    assert_eq!(ran.code, Some(10), "{}", ran.stderr);
    assert_eq!(
        done_error(&ran, worker::Verb::Init).as_deref(),
        Some("symbol: no such instrument: not_found")
    );
    assert!(ran.lines.is_empty(), "{:?}", ran.lines);
}

#[test]
fn the_init_deadline_ends_a_resolve_in_on_init() {
    let server = start_server();
    let fake = FakeSym::start(server.url());
    fake.state().get_delay = Duration::from_secs(10);
    let started = Instant::now();
    let ran = run_worker(server.url(), "", 1_500);
    assert_eq!(ran.code, Some(10), "{}", ran.stderr);
    assert_eq!(
        done_error(&ran, worker::Verb::Init).as_deref(),
        Some("deadline")
    );
    assert!(started.elapsed() < Duration::from_secs(5));
}
