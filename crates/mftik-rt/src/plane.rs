//! The session plane: what C4's plane calls do (Go `sdk/plane.go`).
//!
//! Init parses `InitPayload`, connects NATS, starts the event log writer
//! when the session has it on, builds the dispatcher, opens the paths, and
//! calls OnInit. Start applies the bindings, listens on TD, prints
//! the gate line, calls OnStart, and asks for the first Recon. Run is the
//! dispatcher loop. Live applies a bindings snapshot, Relay queues a
//! Command, OnStop goes through the dispatcher once Run has started and
//! straight to the strategy before that. Every strategy hook runs on the
//! thread that called [`crate::run`] (E03-D003).
//!
//! With the event log on, the plane writes the frames that do not pass the
//! dispatcher (§12.4, Go `sdk/record.go`): `init`, without the NATS
//! credentials (E07-D010), and `start`, each with its `hook_end`; a
//! `bindings` for each Live or Command snapshot that was applied; a
//! `resolve` for each [`Context::resolve`]; and an OnStop before Run as
//! `onstop`. A panic in one of those hooks is flushed to the file at once
//! (E07-D012).

use std::any::Any;
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::io::{self, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use mftik_core::bookpath;
use mftik_core::c4::CallError;
use mftik_core::cloidgen;
use mftik_core::dispatch::{self, Dispatcher, Input, Timer};
use mftik_core::eventlog::{
    END_ERROR, END_PANIC, FROM_COMMAND, Kind, Recorder, field_bytes, init_head, outcome, put_str,
    without_field,
};
use mftik_core::mdpath::{self, Data};
use mftik_core::symbol::Info;
use mftik_core::symclient;
use mftik_core::tdpath;
use mftik_proto::wire::mftiko::session::v1::{AccountDecl, EventLog};
use mftik_proto::wire::mftiko::stsctl::v1::{
    Bindings, Command, EventLogStat, InitPayload, StartPayload, command,
};
use prost::Message;

use crate::call::CallCtx;
use crate::clock::Clock;
use crate::context::{Context, Error};
use crate::eventlog::{self, Recording};
use crate::io::Io;
use crate::md;
use crate::nats;
use crate::strategy::{BookUpdate, Feed, Handlers, HookError, Strategy};
use crate::sym;
use crate::td;
use crate::timers::{TimerKey, Timers};
use crate::wiring::{Wiring, sym_error};

/// Volume directory, written by the strategy only (PROTOCOL §5.4, E09-D004).
pub const VOLUME_DIR_ENV: &str = "MFTIKO_VOLUME_DIR";
/// Unpacked inputs, read-only (E09-D004).
pub const INPUT_DIR_ENV: &str = "MFTIKO_INPUT_DIR";

/// The conformance line printed after the subscriptions and before OnStart
/// (PROTOCOL §9.8, Go `emitGate`).
pub const GATE_LINE: &str = "{\"hook\":\"gate\"}\n";

/// How often the IO thread moves the dispatcher's injected clock while
/// nothing else does (E03-D004, E03-D051). The watchdog, the hook budget, and
/// the prehook read that clock; anything that queues a hook moves it first,
/// so a hook's start is current and the tick bounds only how late the
/// watchdog sees the budget run out.
pub const CLOCK_TICK: Duration = Duration::from_millis(10);

/// `InitPayload.nats`, the session's NATS credentials: the `init` frame
/// leaves the field out (E07-D010).
const NATS_FIELD: u32 = 9;
/// `Command.bindings`, which a `bindings` frame from a Command carries.
const BINDINGS_FIELD: u32 = 1;

/// One stderr line, as the Go SDK's `note` (E06-D010).
pub(crate) fn note(line: &str) {
    crate::log::note(line);
}

/// What `InitPayload` set for the whole session.
#[derive(Debug)]
pub(crate) struct Session {
    pub(crate) id: String,
    pub(crate) params: Vec<u8>,
    pub(crate) volume_dir: String,
    pub(crate) input_dir: String,
    /// `InitPayload.event_log` when it is enabled; `None` is off (E20-D007).
    pub(crate) event_log: Option<EventLog>,
    /// `Context.Rand`'s seed, drawn once per worker. The `init` frame
    /// records it (§12.4).
    pub(crate) seed: u64,
}

/// One Command waiting for the command thread, and when it reached the
/// worker while the event log is on (Go `relayed`).
struct Relayed {
    at: Option<i64>,
    payload: Vec<u8>,
}

pub(crate) struct Plane<S> {
    strategy: Mutex<S>,
    handlers: Handlers<S>,
    io: Io,
    clock: Clock,
    timers: Timers,
    pub(crate) wiring: Wiring,
    session: OnceLock<Session>,
    disp: OnceLock<Dispatcher>,
    nats: OnceLock<async_nats::Client>,
    /// The event log writer, while the session has the log on.
    event_log: OnceLock<Arc<eventlog::Log>>,
    /// The context of the Init or Start call C4 started last.
    call: Mutex<Option<CallCtx>>,
    resolve_allowed: AtomicBool,
    /// The strategy called exit or fail, or a Command failed the session.
    asked: AtomicBool,
    /// C4 started Run: OnStop goes through the dispatcher.
    loop_started: AtomicBool,
    dispatch_timers: Arc<Mutex<HashMap<u64, Timer>>>,
    commands: Mutex<mpsc::Sender<Relayed>>,
    /// A replay answers Resolve from the log. `None` in a session.
    replay_resolve: Mutex<Option<ReplayResolve>>,
}

type ReplayResolve = Arc<dyn Fn(&str) -> Result<Info, Error> + Send + Sync>;

impl<S: Strategy> Plane<S> {
    /// The plane, with the thread that applies Commands in order (Go
    /// `cmdLoop`).
    pub(crate) fn start(
        strategy: S,
        io: Io,
        clock: Clock,
        timers: Timers,
    ) -> io::Result<Arc<Self>> {
        let mut handlers = Handlers::default();
        strategy.handlers(&mut handlers);
        let (tx, rx) = mpsc::channel::<Relayed>();
        let plane = Arc::new(Plane {
            strategy: Mutex::new(strategy),
            handlers,
            io,
            clock,
            timers,
            wiring: Wiring::default(),
            session: OnceLock::new(),
            disp: OnceLock::new(),
            nats: OnceLock::new(),
            event_log: OnceLock::new(),
            call: Mutex::new(None),
            resolve_allowed: AtomicBool::new(false),
            asked: AtomicBool::new(false),
            loop_started: AtomicBool::new(false),
            dispatch_timers: Arc::default(),
            commands: Mutex::new(tx),
            replay_resolve: Mutex::new(None),
        });
        let weak = Arc::downgrade(&plane);
        thread::Builder::new()
            .name("mftik-cmd".into())
            .spawn(move || {
                for relayed in rx {
                    match weak.upgrade() {
                        Some(plane) => plane.handle_command(&relayed),
                        None => return,
                    }
                }
            })?;
        Ok(plane)
    }

    /// Runs `f` with the strategy and a hook's [`Context`]. Once Init has
    /// the dispatcher, every hook through here is one the dispatcher runs,
    /// and [`Context::now_ns`] is its start; before that the start is now.
    pub(crate) fn in_hook<R>(self: &Arc<Self>, f: impl FnOnce(&mut S, &Context<S>) -> R) -> R {
        let start_ns = self.disp.get().is_none().then(|| self.clock.now_ns());
        self.in_hook_at(start_ns, f)
    }

    /// [`Plane::in_hook`] for a hook that started at `start_ns`. `None` is
    /// the dispatcher's running hook.
    fn in_hook_at<R>(
        self: &Arc<Self>,
        start_ns: Option<i64>,
        f: impl FnOnce(&mut S, &Context<S>) -> R,
    ) -> R {
        let mut strategy = self.strategy.lock().unwrap_or_else(PoisonError::into_inner);
        let ctx = Context::new(Arc::clone(self), start_ns);
        f(&mut strategy, &ctx)
    }

    pub(crate) fn session(&self) -> Option<&Session> {
        self.session.get()
    }

    /// The session clock: the dispatcher's injected clock and the event
    /// log's both read it (E03-D004).
    pub(crate) fn clock(&self) -> Clock {
        self.clock
    }

    /// `Report.event_log`: present only while the event log is on (Go
    /// `eventLogStat`).
    pub(crate) fn event_log_stat(&self) -> Option<EventLogStat> {
        let (records, dropped) = self.event_log.get()?.stats();
        Some(EventLogStat { records, dropped })
    }

    pub(crate) fn dispatcher(&self) -> Option<&Dispatcher> {
        self.disp.get()
    }

    /// The event log, once Init made the dispatcher with it on.
    fn recorder(&self) -> Option<&Arc<dyn Recorder>> {
        self.disp.get().and_then(Dispatcher::recorder)
    }

    /// The event log's clock. `None` while the log is off, and then the
    /// clock is not read (E07-D009).
    fn record_now(&self) -> Option<i64> {
        self.disp.get().and_then(Dispatcher::record_now)
    }

    /// The session's NATS connection, once Init has it.
    #[allow(dead_code)] // The MD, TD, and SYM paths use it (E03-06–E03-08).
    pub(crate) fn nats(&self) -> Option<&async_nats::Client> {
        self.nats.get()
    }

    pub(crate) fn resolve_allowed(&self) -> bool {
        self.resolve_allowed.load(Ordering::SeqCst)
    }

    pub(crate) fn current_call(&self) -> Option<CallCtx> {
        lock(&self.call).clone()
    }

    pub(crate) fn set_asked(&self) {
        self.asked.store(true, Ordering::SeqCst);
    }

    /// C4 starts an Init or Start call with this context.
    pub(crate) fn begin_call(&self, call: CallCtx) {
        *lock(&self.call) = Some(call);
    }

    /// C4 starts Run. Called before the loop is handed to the dispatcher
    /// thread, so a Cancel right after it reaches the dispatcher.
    pub(crate) fn begin_run(&self) {
        self.loop_started.store(true, Ordering::SeqCst);
    }

    pub(crate) fn loop_started(&self) -> bool {
        self.loop_started.load(Ordering::SeqCst)
    }

    /// C4 `Effect::Cancel`: ends the Init or Start context, or stops Run as
    /// `DONE` (Go's `Disp.Run` on `ctx.Done`).
    pub(crate) fn cancel(&self) {
        if let Some(call) = lock(&self.call).as_ref() {
            call.cancel();
        }
        if self.loop_started()
            && let Some(disp) = self.disp.get()
        {
            disp.exit();
        }
    }

    /// The plane's Init. Returns the hook budget C4 gives OnStop after a
    /// self-end (Go `HookTimeout`).
    pub(crate) fn init(
        self: &Arc<Self>,
        payload: &[u8],
        call: &CallCtx,
    ) -> Result<Duration, CallError> {
        let recv_ns = self.clock.now_ns();
        if call.is_done() {
            return Err(CallError::Cancelled);
        }
        let init = InitPayload::decode(payload).map_err(|_| failed("init payload"))?;
        if init.session_id.is_empty() {
            return Err(failed("session id"));
        }
        // Resolved by the STS agent, never 0 (PROTOCOL §9.3.1, E08-D003).
        if init.recv_window_ms == 0 {
            return Err(failed("recv window"));
        }
        if init.hook_timeout_s == 0 {
            return Err(failed("hook timeout"));
        }
        let creds = match init.nats {
            Some(creds)
                if !creds.user_jwt.is_empty()
                    && !creds.seed.is_empty()
                    && !creds.inbox_prefix.is_empty() =>
            {
                creds
            }
            _ => return Err(failed("nats credentials")),
        };
        let url = env(nats::NATS_URL_ENV);
        if url.is_empty() {
            return Err(failed("nats url"));
        }
        let recv_window = Duration::from_millis(u64::from(init.recv_window_ms));
        let hook_timeout = Duration::from_secs(u64::from(init.hook_timeout_s));
        // A second Init never reaches the plane (C4 ends INIT_FAILED first).
        // Later `note` lines carry this id once it is known (E06-D004).
        crate::log::set_note_session(&init.session_id);
        let _ = self.session.set(Session {
            id: init.session_id,
            params: init.params,
            volume_dir: env(VOLUME_DIR_ENV),
            input_dir: env(INPUT_DIR_ENV),
            event_log: init.event_log.filter(|log| log.enabled),
            seed: random_seed(),
        });

        let connecting = call.clone();
        let on_reconnect = self.wiring.sym_reconnect_hook();
        let client = self
            .io
            .block_on(async move {
                let dial = |timeout| {
                    let (url, creds) = (url.clone(), creds.clone());
                    let on_reconnect = Some(Arc::clone(&on_reconnect));
                    async move { nats::dial(&url, &creds, timeout, on_reconnect).await }
                };
                nats::connect(&connecting, dial, &note).await
            })
            .map_err(|nats::Ended| CallError::Cancelled)?;
        self.wiring
            .open_sym(&self.io, self.clock, &self.timers, &client, Arc::new(note));
        let _ = self.nats.set(client.clone());
        self.open_event_log(init.incarnation);

        let disp = self.new_dispatcher(recv_window, hook_timeout);
        let _ = self.disp.set(disp.clone());
        self.start_clock(disp.clone());
        self.open_md(disp.clone(), recv_window)?;
        self.open_td(init.accounts, recv_window, client, disp)
            .map_err(CallError::Failed)?;

        if call.is_done() {
            return Err(CallError::Cancelled);
        }
        // The payload without the NATS credentials, after the client order id
        // counter as Init found it (E07-D010).
        let input = self.recorder().map(|_| Input {
            kind: Kind::INIT,
            recv_ns,
            r#ref: self.session.get().map_or(0, |s| s.seed),
            head: Bytes::copy_from_slice(&init_head(cloidgen::slot())),
            body: without_field(payload, NATS_FIELD).into(),
            ..Input::default()
        });
        self.resolve_allowed.store(true, Ordering::SeqCst);
        let result = self.outside(input, |s, ctx| s.on_init(ctx));
        self.resolve_allowed.store(false, Ordering::SeqCst);
        result?;
        Ok(hook_timeout)
    }

    /// Runs a hook the dispatcher does not run: OnInit, OnStart, and an
    /// OnStop before Run (Go `outside`). Its start is [`Context::now_ns`].
    /// With the event log on, `input` is written first, stamped with that
    /// start, and a `hook_end` after. A panic's frames are flushed to the
    /// file at once, waiting at most five seconds: the process may end
    /// before its exit closes the log (E07-D012).
    fn outside(
        self: &Arc<Self>,
        input: Option<Input>,
        hook: impl FnOnce(&mut S, &Context<S>) -> Result<(), HookError>,
    ) -> Result<(), CallError> {
        let rec = self.recorder();
        let start_ns = match rec {
            Some(rec) => rec.now_ns(),
            None => self.clock.now_ns(),
        };
        let seq = rec.zip(input).map(|(rec, input)| {
            rec.append(
                input.kind,
                input.flags,
                input.recv_ns,
                start_ns,
                input.r#ref,
                &input.head,
                &input.body,
            )
        });
        let result = catch_unwind(AssertUnwindSafe(|| self.in_hook_at(Some(start_ns), hook)));
        if let (Some(rec), Some(seq)) = (rec, seq) {
            let (flags, text) = match &result {
                Ok(Ok(())) => (0, String::new()),
                Ok(Err(err)) => (END_ERROR, err.to_string()),
                Err(payload) => (END_PANIC, panic_message(payload.as_ref())),
            };
            rec.append(
                Kind::HOOK_END,
                flags,
                0,
                rec.now_ns(),
                seq,
                text.as_bytes(),
                &[],
            );
            if result.is_err() {
                self.flush_event_log();
            }
        }
        hook_result(result)
    }

    fn flush_event_log(&self) {
        if let Some(log) = self.event_log.get()
            && !log.flush_within(eventlog::CLOSE_WAIT)
        {
            note("event log: flush did not finish writing");
        }
    }

    /// Starts the event log writer when the session has the log on (Go
    /// `openRec`). Files go to `$MFTIKO_VOLUME_DIR/eventlog`; without a
    /// volume directory the writer counts every frame as dropped and says so
    /// once (§12.1). The writer is closed at the exit. One that cannot start
    /// is a line on stderr, and the session runs without the log.
    fn open_event_log(&self, incarnation: u32) {
        let Some(session) = self.session.get() else {
            return;
        };
        let Some(spec) = &session.event_log else {
            return;
        };
        let dir = (!session.volume_dir.is_empty())
            .then(|| Path::new(&session.volume_dir).join(eventlog::DIR));
        let opened = eventlog::Log::open(eventlog::Config {
            dir,
            header: eventlog::Header::mftik(incarnation, &session.id),
            keep_bytes: u64::from(spec.keep_mb) << 20,
            ..eventlog::Config::default()
        });
        match opened {
            Ok(log) => {
                let log = Arc::new(log);
                crate::exit::close_at_exit(&log);
                let _ = self.event_log.set(log);
            }
            Err(err) => note(&format!("event log: {err}")),
        }
    }

    /// Go's `mdpath.Open` and `bookpath.Open` (E03-06).
    fn open_md(self: &Arc<Self>, disp: Dispatcher, recv_window: Duration) -> Result<(), CallError> {
        let Some(client) = self.nats.get().cloned() else {
            return Err(failed("nats"));
        };
        let plane = Arc::downgrade(self);
        self.wiring
            .open_md(md::Config {
                io: self.io.clone(),
                clock: self.clock,
                timers: self.timers.clone(),
                disp,
                nats: Some(client),
                recv_window,
                hooks: self.handlers.md_hooks(),
                on_book: Arc::new(move |update| {
                    if let Some(plane) = plane.upgrade() {
                        plane.deliver_book(update);
                    }
                }),
                log: Arc::new(note),
                replay: false,
                rec: None,
            })
            .map_err(CallError::Failed)
    }

    /// The dispatcher's feed callback: one `md.f` message past the prehook,
    /// decoded for its hook (E03-D012). A replay runs a recorded `md_feed`
    /// through this too.
    pub(crate) fn on_feed(self: &Arc<Self>, msg: &[u8]) {
        match self.handlers.md_hooks().decode(msg) {
            mdpath::Decoded::Hook(event) => self.deliver_md(event),
            mdpath::Decoded::Skip => {}
            mdpath::Decoded::Log(line) => note(&line),
        }
    }

    /// The plane's Start.
    pub(crate) fn start_call(
        self: &Arc<Self>,
        payload: &[u8],
        call: &CallCtx,
    ) -> Result<(), CallError> {
        let recv_ns = self.record_now();
        if call.is_done() {
            return Err(CallError::Cancelled);
        }
        if self.asked.load(Ordering::SeqCst) {
            return Ok(());
        }
        let bindings = match StartPayload::decode(payload) {
            Ok(StartPayload {
                bindings: Some(bindings),
            }) => bindings,
            _ => return Err(failed("start payload")),
        };
        self.wiring
            .apply_bindings(&bindings)
            .map_err(CallError::Failed)?;
        if call.is_done() {
            return Err(CallError::Cancelled);
        }
        self.wiring.listen_td().map_err(CallError::Failed)?;
        emit_gate();
        let input = recv_ns.map(|recv_ns| Input {
            kind: Kind::START,
            recv_ns,
            body: Bytes::copy_from_slice(payload),
            ..Input::default()
        });
        self.resolve_allowed.store(true, Ordering::SeqCst);
        let result = self.outside(input, |s, ctx| s.on_start(ctx));
        self.resolve_allowed.store(false, Ordering::SeqCst);
        result?;
        if self.asked.load(Ordering::SeqCst) {
            return Ok(());
        }
        if call.is_done() {
            return Err(CallError::Cancelled);
        }
        match self.wiring.recon() {
            Ok(()) | Err(Error::Stopping) => Ok(()),
            Err(err) => Err(CallError::Failed(err.to_string())),
        }
    }

    /// The plane's Run: the dispatcher loop on this thread (E03-D003).
    /// C4 reads the ending with `wait_run` on another thread.
    pub(crate) fn run_loop(&self) {
        if let Some(disp) = self.disp.get() {
            disp.drive();
        }
    }

    /// OnStop before Run started: Start failed, so the dispatcher loop is
    /// not there to take it (Go `plane.OnStop`).
    pub(crate) fn on_stop_direct(self: &Arc<Self>) -> Result<(), CallError> {
        let input = self.record_now().map(|recv_ns| Input {
            kind: Kind::ONSTOP,
            recv_ns,
            ..Input::default()
        });
        self.outside(input, |s, ctx| {
            s.on_stop(ctx);
            Ok(())
        })
    }

    /// The plane's Live: one bindings snapshot (E01-D011, E08-D046). One
    /// that is not newer is applied too, as nothing.
    pub(crate) fn live(&self, payload: &[u8]) -> Result<(), CallError> {
        let at = self.record_now();
        let bindings = Bindings::decode(payload).map_err(|_| failed("bindings"))?;
        self.wiring
            .apply_bindings(&bindings)
            .map_err(CallError::Failed)?;
        if let Some(at) = at {
            self.record_bindings(at, 0, payload);
        }
        Ok(())
    }

    /// The plane's Relay: queue one Command (PROTOCOL §9.3.5). C4 has sent
    /// the echo.
    pub(crate) fn relay(&self, payload: Vec<u8>) {
        let at = self.record_now();
        let _ = lock(&self.commands).send(Relayed { at, payload });
    }

    fn handle_command(&self, relayed: &Relayed) {
        let Ok(cmd) = Command::decode(relayed.payload.as_slice()) else {
            note("command");
            return;
        };
        match cmd.cmd {
            Some(command::Cmd::Bindings(bindings)) => {
                if let Err(err) = self.wiring.apply_bindings(&bindings) {
                    note(&format!("command bindings: {err}"));
                } else if let Some(at) = relayed.at
                    && let Some(raw) = field_bytes(&relayed.payload, BINDINGS_FIELD)
                {
                    self.record_bindings(at, FROM_COMMAND, raw);
                }
            }
            Some(command::Cmd::Fail(fail)) => {
                self.set_asked();
                if let Some(disp) = self.disp.get() {
                    disp.fail(fail.reason);
                }
            }
            None => {}
        }
    }

    /// A `bindings` frame for a snapshot that was applied: `at` is when it
    /// reached the worker, `raw` the Bindings as received (Go
    /// `recordBindings`).
    fn record_bindings(&self, at: i64, flags: u16, raw: &[u8]) {
        if let Some(rec) = self.recorder() {
            rec.append(Kind::BINDINGS, flags, at, rec.now_ns(), 0, raw, &[]);
        }
    }

    /// [`Context::resolve`] (Go `plane.resolve`). With the event log on,
    /// each call is a `resolve` frame, a refused one too: the ticker, then
    /// the SymbolInfo or the error the strategy got.
    pub(crate) fn resolve(&self, ticker: &str) -> Result<Info, Error> {
        if let Some(resolve) = lock(&self.replay_resolve).clone() {
            return resolve(ticker);
        }
        let call_ns = self.record_now();
        let result = if self.resolve_allowed() {
            let call = self.current_call().unwrap_or_else(CallCtx::background);
            self.wiring.resolve(&call, ticker)
        } else {
            Err(symclient::Error::NotInInit)
        };
        if let Some(call_ns) = call_ns {
            self.record_resolve(call_ns, ticker, &result);
        }
        result.map_err(sym_error)
    }

    /// Go `recordResolve`: recv is when the answer came, t the call.
    fn record_resolve(&self, call_ns: i64, ticker: &str, result: &Result<Info, symclient::Error>) {
        let Some(rec) = self.recorder() else {
            return;
        };
        let mut head = Vec::new();
        put_str(&mut head, ticker.as_bytes());
        let (flags, rest) = match result {
            Ok(info) => (outcome::OK, symclient::info_to_proto(info).encode_to_vec()),
            Err(err) => (
                resolve_outcome(err),
                sym_error(err.clone()).to_string().into_bytes(),
            ),
        };
        rec.append(Kind::RESOLVE, flags, rec.now_ns(), call_ns, 0, &head, &rest);
    }

    /// One decoded market data event to its hook (Go `mdHooks`). The
    /// dispatcher's feed callback calls it. An event without a registered
    /// hook is dropped; mdpath skips those before decoding (E08-D005).
    pub(crate) fn deliver_md(self: &Arc<Self>, event: mdpath::Event) {
        let md = event.md;
        let h = &self.handlers;
        macro_rules! deliver {
            ($hook:expr, $data:expr) => {
                if let Some(hook) = $hook {
                    self.in_hook(|s, ctx| hook(s, ctx, Feed { md, data: $data }));
                }
            };
        }
        match event.data {
            Data::BestQuote(data) => deliver!(h.best_quote, data),
            Data::Trade(data) => deliver!(h.trade, data),
            Data::AggTrade(data) => deliver!(h.agg_trade, data),
            Data::Kline(data) => deliver!(h.kline, data),
            Data::Ticker(data) => deliver!(h.ticker, data),
            Data::Liquidation(data) => deliver!(h.liquidation, data),
            Data::FundingRate(data) => deliver!(h.funding_rate, data),
            Data::OpenInterest(data) => deliver!(h.open_interest, data),
            Data::Greeks(data) => deliver!(h.greeks, data),
            Data::MarkPrice(data) => deliver!(h.mark_price, data),
        }
    }

    /// bookpath's `Hook` to OnBook (Go `bookHook`). The MD path calls it;
    /// the book is kept whether or not OnBook is registered.
    pub(crate) fn deliver_book(self: &Arc<Self>, update: bookpath::Update) {
        if let Some(hook) = self.handlers.book {
            let ev = BookUpdate {
                md: mdpath::Md {
                    uid: update.uid,
                    md_ts_ns: update.md_ts_ns,
                    conn_epoch: update.conn_epoch,
                    seq: update.seq,
                },
                exch_ts_ns: update.exch_ts_ns,
            };
            self.in_hook(|s, ctx| hook(s, ctx, ev));
        }
    }

    /// tdpath's `Hook` to its handler (Go `tdHooks`). The TD path calls it
    /// after the state already includes the event (E08-D053).
    pub(crate) fn deliver_td(self: &Arc<Self>, hook: tdpath::Hook) {
        let h = &self.handlers;
        macro_rules! deliver {
            ($hook:expr, $ev:expr) => {
                if let Some(hook) = $hook {
                    self.in_hook(|s, ctx| hook(s, ctx, $ev));
                }
            };
        }
        match hook {
            tdpath::Hook::Order(ev) => deliver!(h.order, ev),
            tdpath::Hook::Fill(ev) => deliver!(h.fill, ev),
            tdpath::Hook::Position(ev) => deliver!(h.position, ev),
            tdpath::Hook::Balance(ev) => deliver!(h.balance, ev),
            tdpath::Hook::Recon(ev) => deliver!(h.recon, ev),
            tdpath::Hook::Reject(ev) => deliver!(h.reject, ev),
        }
    }

    /// Go's `tdpath.Open` in Init (E03-07).
    fn open_td(
        self: &Arc<Self>,
        accounts: Vec<AccountDecl>,
        recv_window: Duration,
        client: async_nats::Client,
        disp: Dispatcher,
    ) -> Result<(), String> {
        let plane = Arc::downgrade(self);
        self.wiring.open_td(td::Config {
            session_id: self.session.get().map(|s| s.id.clone()).unwrap_or_default(),
            recv_window,
            accounts,
            nats: Some(client),
            io: self.io.clone(),
            timers: self.timers.clone(),
            clock: self.clock,
            disp,
            deliver: Arc::new(move |hook| {
                if let Some(plane) = plane.upgrade() {
                    plane.deliver_td(hook);
                }
            }),
            log: Arc::new(note),
            replay: false,
            rec: None,
            call: None,
        })
    }

    fn new_dispatcher(
        self: &Arc<Self>,
        recv_window: Duration,
        hook_timeout: Duration,
    ) -> Dispatcher {
        let on_stop = Arc::downgrade(self);
        let unsubscribe = Arc::downgrade(self);
        let fire_plane = Arc::downgrade(self);
        let (feed_plane, book_plane) = (Arc::downgrade(self), Arc::downgrade(self));
        let (arm_ids, arm_timers) = (Arc::clone(&self.dispatch_timers), self.timers.clone());
        let (disarm_ids, disarm_timers) = (Arc::clone(&self.dispatch_timers), self.timers.clone());
        Dispatcher::new(dispatch::Config {
            recv_window,
            hook_timeout,
            now_ns: self.clock.now_ns(),
            ring_bytes: 0,
            on_stop: Some(Box::new(move |_| {
                if let Some(plane) = on_stop.upgrade() {
                    plane.in_hook(|s, ctx| s.on_stop(ctx));
                }
                Ok(())
            })),
            unsubscribe_td: Some(Box::new(move || {
                if let Some(plane) = unsubscribe.upgrade() {
                    plane.wiring.unsubscribe_td();
                }
            })),
            feed: Some(Box::new(move |msg| {
                if let Some(plane) = feed_plane.upgrade() {
                    plane.on_feed(msg);
                }
            })),
            book: Some(Box::new(move |msg| {
                if let Some(md) = book_plane.upgrade().and_then(|p| p.wiring.md().cloned()) {
                    md.on_book_message(msg);
                }
            })),
            log: Some(Arc::new(note)),
            arm_timer: Some(Arc::new(move |id, deadline_ns| {
                let Some(timer) = lock(&arm_ids).get(&id).cloned() else {
                    return;
                };
                let ids = Arc::clone(&arm_ids);
                let plane = fire_plane.clone();
                arm_timers.arm(TimerKey::Dispatch(id), deadline_ns, move || {
                    // Every dispatcher timer the plane makes is armed once.
                    lock(&ids).remove(&id);
                    if let Some(plane) = plane.upgrade() {
                        plane.fresh_now();
                    }
                    timer.fire();
                });
            })),
            disarm_timer: Some(Arc::new(move |id| {
                disarm_timers.disarm(TimerKey::Dispatch(id));
                lock(&disarm_ids).remove(&id);
            })),
            rec: self.event_log.get().map(|log| {
                Arc::new(Recording::new(Arc::clone(log), self.clock)) as Arc<dyn Recorder>
            }),
            replay: false,
        })
    }

    /// Builds the dispatcher and the paths for an offline replay: no NATS,
    /// no event log writer, Post kept for the log to run (PROTOCOL §12.6).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_replay(
        self: &Arc<Self>,
        init: &InitPayload,
        slot: u64,
        seed: u64,
        volume_dir: String,
        input_dir: String,
        out: Arc<dyn Recorder>,
        call: td::ReplayCall,
        resolve: ReplayResolve,
    ) -> Result<(), String> {
        if init.recv_window_ms == 0 {
            return Err("sdk: replay: the init payload has no recv window".into());
        }
        crate::log::set_note_session(&init.session_id);
        let _ = self.session.set(Session {
            id: init.session_id.clone(),
            params: init.params.clone(),
            volume_dir,
            input_dir,
            event_log: None,
            seed,
        });
        cloidgen::restore(slot);
        *lock(&self.replay_resolve) = Some(resolve);
        let recv_window = Duration::from_millis(u64::from(init.recv_window_ms));
        let hook_timeout = Duration::from_secs(u64::from(init.hook_timeout_s));
        let disp = self.new_replay_dispatcher(recv_window, hook_timeout);
        let _ = self.disp.set(disp.clone());
        self.open_replay_md(disp.clone(), recv_window, Arc::clone(&out))?;
        self.open_replay_td(init.accounts.clone(), recv_window, disp, out, call)
    }

    fn new_replay_dispatcher(
        self: &Arc<Self>,
        recv_window: Duration,
        hook_timeout: Duration,
    ) -> Dispatcher {
        let (feed_plane, book_plane) = (Arc::downgrade(self), Arc::downgrade(self));
        Dispatcher::new(dispatch::Config {
            recv_window,
            hook_timeout,
            now_ns: 0,
            ring_bytes: 1 << 10,
            feed: Some(Box::new(move |msg| {
                if let Some(plane) = feed_plane.upgrade() {
                    plane.on_feed(msg);
                }
            })),
            book: Some(Box::new(move |msg| {
                if let Some(md) = book_plane.upgrade().and_then(|p| p.wiring.md().cloned()) {
                    md.on_book_message(msg);
                }
            })),
            log: Some(Arc::new(note)),
            replay: true,
            ..Default::default()
        })
    }

    fn open_replay_md(
        self: &Arc<Self>,
        disp: Dispatcher,
        recv_window: Duration,
        rec: Arc<dyn Recorder>,
    ) -> Result<(), String> {
        let plane = Arc::downgrade(self);
        self.wiring.open_md(md::Config {
            io: self.io.clone(),
            clock: self.clock,
            timers: self.timers.clone(),
            disp,
            nats: None,
            recv_window,
            hooks: self.handlers.md_hooks(),
            on_book: Arc::new(move |update| {
                if let Some(plane) = plane.upgrade() {
                    plane.deliver_book(update);
                }
            }),
            log: Arc::new(note),
            replay: true,
            rec: Some(rec),
        })
    }

    fn open_replay_td(
        self: &Arc<Self>,
        accounts: Vec<AccountDecl>,
        recv_window: Duration,
        disp: Dispatcher,
        rec: Arc<dyn Recorder>,
        call: td::ReplayCall,
    ) -> Result<(), String> {
        let plane = Arc::downgrade(self);
        self.wiring.open_td(td::Config {
            session_id: self.session.get().map(|s| s.id.clone()).unwrap_or_default(),
            recv_window,
            accounts,
            nats: None,
            io: self.io.clone(),
            timers: self.timers.clone(),
            clock: self.clock,
            disp,
            deliver: Arc::new(move |hook| {
                if let Some(plane) = plane.upgrade() {
                    plane.deliver_td(hook);
                }
            }),
            log: Arc::new(note),
            replay: true,
            rec: Some(rec),
            call: Some(call),
        })
    }

    pub(crate) fn set_resolve_allowed(&self, allowed: bool) {
        self.resolve_allowed.store(allowed, Ordering::SeqCst);
    }

    /// A dispatcher timer that runs `f` as a hook once, `delay` from now.
    pub(crate) fn after(
        self: &Arc<Self>,
        delay: Duration,
        f: impl FnOnce(&mut S, &Context<S>) + Send + 'static,
    ) {
        let Some(disp) = self.disp.get() else {
            return;
        };
        let plane = Arc::clone(self);
        let mut f = Some(f);
        let timer = disp.timer(move || {
            if let Some(f) = f.take() {
                plane.in_hook(f);
            }
        });
        lock(&self.dispatch_timers).insert(timer.id(), timer.clone());
        if timer.after(delay).is_none() {
            lock(&self.dispatch_timers).remove(&timer.id());
        }
    }

    /// Wakes `wake` each time a newer bindings snapshot is bound, from the
    /// IO thread, until the receiver is gone (§9.2.4).
    pub(crate) fn on_bindings_applied(&self, wake: mpsc::Sender<()>) {
        let mut applied = self.wiring.watch_bindings_applied();
        self.io.spawn(async move {
            while applied.changed().await.is_ok() {
                if wake.send(()).is_err() {
                    return;
                }
            }
        });
    }

    /// Moves the dispatcher's clock to now, before something queues a hook,
    /// so the hook's start and an MD event's age are current (E03-D051).
    pub(crate) fn fresh_now(&self) {
        if let Some(disp) = self.disp.get() {
            disp.set_now_ns(self.clock.now_ns());
        }
    }

    /// Moves the dispatcher's clock while hooks run (E03-D004).
    fn start_clock(&self, disp: Dispatcher) {
        let clock = self.clock;
        self.io.spawn(async move {
            let mut tick = tokio::time::interval(CLOCK_TICK);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                disp.set_now_ns(clock.now_ns());
            }
        });
    }
}

fn failed(text: &str) -> CallError {
    CallError::Failed(text.to_owned())
}

/// `Context.Rand`'s seed: std draws `RandomState`'s keys from the OS.
fn random_seed() -> u64 {
    RandomState::new().hash_one(0u8)
}

/// A failed resolve's flags (Go `resolveOutcome`). NotInInit, NotFound and
/// Stopping are the SDK's own answers; the rest classify the request to
/// SYM by its text, as an `order_reply` does.
pub(crate) fn resolve_outcome(err: &symclient::Error) -> u16 {
    match err {
        symclient::Error::NotInInit => outcome::NOT_IN_INIT,
        symclient::Error::NotFound(_) => outcome::NOT_FOUND,
        symclient::Error::Closed => outcome::STOPPING,
        symclient::Error::UnknownUid
        | symclient::Error::InvalidTicker(_)
        | symclient::Error::Failed(_) => sym::outcome_of(&err.to_string()),
    }
}

/// A hook's result as C4 takes it. A panic is a failure with the dispatcher's
/// `hook panic: ...` text.
fn hook_result(result: thread::Result<Result<(), HookError>>) -> Result<(), CallError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(CallError::Failed(err.to_string())),
        Err(payload) => Err(CallError::Failed(format!(
            "hook panic: {}",
            panic_message(payload.as_ref())
        ))),
    }
}

fn panic_message(payload: &dyn Any) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_owned()
    }
}

/// Runs `body` as one hook on a plane that has not opened a session.
///
/// Binding tests use this to call a strategy hook without fd 3 or NATS.
/// [`Io::start`] leaves its thread running until the process exits; the
/// command thread exits when the plane is dropped.
#[doc(hidden)]
pub fn in_one_hook<S: Strategy>(
    strategy: S,
    body: impl FnOnce(&mut S, &Context<S>),
) -> io::Result<()> {
    let io_thread = Io::start()?;
    let clock = Clock::new();
    let timers = Timers::new(io_thread.clone(), clock);
    let plane = Plane::start(strategy, io_thread, clock, timers)?;
    plane.in_hook(body);
    Ok(())
}

fn emit_gate() {
    let mut out = io::stdout().lock();
    let _ = out.write_all(GATE_LINE.as_bytes());
    let _ = out.flush();
}

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_default()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use mftik_core::tdpath::{FillEvent, Td};
    use mftik_proto::wire::mftiko::common::v1::Version;
    use mftik_proto::wire::mftiko::md::v1 as md;
    use mftik_proto::wire::mftiko::stsctl::v1::NatsCredentials;
    use mftik_proto::wire::mftiko::td::v1 as td;

    use super::*;
    use crate::eventlog::{Record, Scratch};
    use crate::strategy::{BookHandler, FillHandler, Trade, TradeHandler};

    #[derive(Default)]
    struct Recorder {
        seen: Vec<String>,
    }

    impl Strategy for Recorder {
        fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
            self.seen.push(format!("init {}", ctx.session_id()));
            Ok(())
        }
        fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
            self.seen
                .push(format!("start {:?}", ctx.resolve("x").err()));
            Ok(())
        }
        fn on_stop(&mut self, _: &Context<Self>) {
            self.seen.push("stop".into());
        }
        fn handlers(&self, h: &mut Handlers<Self>) {
            h.trade().book().fill();
        }
    }

    impl TradeHandler for Recorder {
        fn on_trade(&mut self, ctx: &Context<Self>, ev: Trade) {
            assert_eq!(ctx.resolve("BTC").err(), Some(Error::NotInInit));
            self.seen.push(format!("trade {}", ev.md.seq));
        }
    }

    impl BookHandler for Recorder {
        fn on_book(&mut self, ctx: &Context<Self>, ev: BookUpdate) {
            assert!(ctx.book(ev.md.uid).is_none());
            self.seen
                .push(format!("book {} {}", ev.md.uid, ev.exch_ts_ns));
        }
    }

    impl FillHandler for Recorder {
        fn on_fill(&mut self, _: &Context<Self>, ev: FillEvent) {
            self.seen.push(format!("fill {}", ev.td.api_id));
        }
    }

    fn plane() -> Arc<Plane<Recorder>> {
        let io = Io::start().expect("io thread");
        let clock = Clock::at(1_000_000_000, Instant::now());
        let timers = Timers::new(io.clone(), clock);
        Plane::start(Recorder::default(), io, clock, timers).expect("plane")
    }

    fn seen(plane: &Arc<Plane<Recorder>>) -> Vec<String> {
        plane.in_hook(|s, _| s.seen.clone())
    }

    fn init_payload(edit: impl FnOnce(&mut InitPayload)) -> Vec<u8> {
        let mut init = InitPayload {
            session_id: "c0ffee".into(),
            recv_window_ms: 500,
            hook_timeout_s: 1,
            nats: Some(NatsCredentials {
                user_jwt: "jwt".into(),
                seed: "seed".into(),
                inbox_prefix: "_INBOX_c0ffee".into(),
            }),
            ..Default::default()
        };
        edit(&mut init);
        init.encode_to_vec()
    }

    fn failure(text: &str) -> Result<Duration, CallError> {
        Err(CallError::Failed(text.into()))
    }

    #[test]
    fn init_refuses_a_bad_payload_before_any_io() {
        let plane = plane();
        let call = CallCtx::background();
        assert_eq!(plane.init(b"\xff", &call), failure("init payload"));
        type Edit = fn(&mut InitPayload);
        let cases: [(&str, Edit); 6] = [
            ("session id", |i| i.session_id.clear()),
            ("recv window", |i| i.recv_window_ms = 0),
            ("hook timeout", |i| i.hook_timeout_s = 0),
            ("nats credentials", |i| i.nats = None),
            ("nats credentials", |i| {
                i.nats.as_mut().expect("creds").seed.clear();
            }),
            ("nats credentials", |i| {
                i.nats.as_mut().expect("creds").inbox_prefix.clear();
            }),
        ];
        for (want, edit) in cases {
            assert_eq!(
                plane.init(&init_payload(edit), &call),
                failure(want),
                "{want}"
            );
        }
        assert!(plane.session().is_none());
        assert!(plane.dispatcher().is_none());
        assert!(seen(&plane).is_empty());
    }

    #[test]
    fn a_done_context_ends_init_and_start_at_once() {
        let plane = plane();
        let call = CallCtx::background();
        call.cancel();
        assert_eq!(
            plane.init(&init_payload(|_| {}), &call),
            Err(CallError::Cancelled)
        );
        assert_eq!(plane.start_call(b"", &call), Err(CallError::Cancelled));
        assert!(seen(&plane).is_empty());
    }

    /// The MD path on a client that never connects: enough for snapshots
    /// without feeds, which subscribe nothing.
    fn open_offline<S: Strategy>(plane: &Arc<Plane<S>>) {
        let client = plane
            .io
            .block_on(async {
                async_nats::ConnectOptions::new()
                    .retry_on_initial_connect()
                    .connect("nats://127.0.0.1:1")
                    .await
            })
            .expect("offline client");
        let _ = plane.nats.set(client);
        let window = Duration::from_millis(500);
        let disp = plane.new_dispatcher(window, Duration::from_secs(1));
        let _ = plane.disp.set(disp.clone());
        plane.open_md(disp, window).expect("md path");
    }

    #[test]
    fn start_needs_bindings_and_runs_on_start_after_the_gate() {
        let plane = plane();
        let call = CallCtx::background();
        assert_eq!(
            plane.start_call(&StartPayload::default().encode_to_vec(), &call),
            Err(CallError::Failed("start payload".into()))
        );
        let start = StartPayload {
            bindings: Some(Bindings::default()),
        };
        // Before Init opens the MD path, as Go's nil mdpath.
        assert_eq!(
            plane.start_call(&start.encode_to_vec(), &call),
            Err(CallError::Failed("bindings".into()))
        );
        assert!(seen(&plane).is_empty());
        open_offline(&plane);
        assert_eq!(plane.start_call(&start.encode_to_vec(), &call), Ok(()));
        // resolve is allowed in OnStart; without Init there is no SYM.
        assert_eq!(
            seen(&plane),
            vec![format!("start {:?}", Some(Error::Stopping))]
        );
        assert!(!plane.resolve_allowed());
        assert_eq!(
            plane.wiring.bindings_applied(),
            Some(Default::default()),
            "a snapshot without a version is epoch 0, seq 0"
        );
        // After exit or fail, Start succeeds without OnStart.
        plane.set_asked();
        assert_eq!(plane.start_call(&start.encode_to_vec(), &call), Ok(()));
        assert_eq!(seen(&plane).len(), 1);
    }

    #[test]
    fn live_takes_a_bindings_snapshot() {
        let plane = plane();
        assert_eq!(
            plane.live(b"\xff"),
            Err(CallError::Failed("bindings".into()))
        );
        assert_eq!(
            plane.live(&Bindings::default().encode_to_vec()),
            Err(CallError::Failed("bindings".into()))
        );
        open_offline(&plane);
        let mut watch = plane.wiring.watch_bindings_applied();
        let v = |seq| Bindings {
            version: Some(Version { epoch: 1, seq }),
            ..Default::default()
        };
        assert_eq!(plane.live(&v(2).encode_to_vec()), Ok(()));
        assert!(watch.has_changed().expect("watch"));
        assert_eq!(
            *watch.borrow_and_update(),
            Some(Version { epoch: 1, seq: 2 })
        );
        // Not newer: success, so C4 sends Applied, and no change to report.
        assert_eq!(plane.live(&v(1).encode_to_vec()), Ok(()));
        assert_eq!(plane.live(&v(2).encode_to_vec()), Ok(()));
        assert!(!watch.has_changed().expect("watch"));
        let selector = Bindings {
            selectors: vec![Default::default()],
            ..v(3)
        };
        assert_eq!(
            plane.live(&selector.encode_to_vec()),
            Err(CallError::Failed(
                "mdpath: selectors are not supported".into()
            ))
        );
        assert_eq!(
            plane.wiring.bindings_applied(),
            Some(Version { epoch: 1, seq: 2 })
        );
    }

    struct Panics;

    impl Strategy for Panics {
        fn on_init(&mut self, _: &Context<Self>) -> Result<(), HookError> {
            Ok(())
        }
        fn on_start(&mut self, _: &Context<Self>) -> Result<(), HookError> {
            Err("no venue".into())
        }
        fn on_stop(&mut self, _: &Context<Self>) {
            panic!("boom");
        }
    }

    #[test]
    fn on_stop_before_run_calls_the_strategy_and_a_panic_is_a_failure() {
        let plane = plane();
        assert_eq!(plane.on_stop_direct(), Ok(()));
        assert_eq!(seen(&plane), vec!["stop".to_owned()]);

        let io = Io::start().expect("io thread");
        let clock = Clock::new();
        let plane = Plane::start(Panics, io.clone(), clock, Timers::new(io, clock)).expect("plane");
        open_offline(&plane);
        assert_eq!(
            plane.on_stop_direct(),
            Err(CallError::Failed("hook panic: boom".into()))
        );
        let start = StartPayload {
            bindings: Some(Bindings::default()),
        };
        assert_eq!(
            plane.start_call(&start.encode_to_vec(), &CallCtx::background()),
            Err(CallError::Failed("no venue".into()))
        );
    }

    #[test]
    fn events_reach_only_registered_hooks() {
        let plane = plane();
        let mdh = mdpath::Md {
            uid: 1001,
            md_ts_ns: 5,
            conn_epoch: 1,
            seq: 9,
        };
        plane.deliver_md(mdpath::Event {
            md: mdh,
            data: Data::Trade(md::Trade::default()),
        });
        // No BestQuote hook registered: dropped.
        plane.deliver_md(mdpath::Event {
            md: mdh,
            data: Data::BestQuote(md::BestQuote::default()),
        });
        plane.deliver_book(bookpath::Update {
            uid: 1001,
            conn_epoch: 1,
            seq: 10,
            md_ts_ns: 5,
            exch_ts_ns: 4,
        });
        let td = Td {
            api_id: 7,
            incarnation: 3,
            seq: 6,
        };
        plane.deliver_td(tdpath::Hook::Fill(FillEvent {
            td,
            fill: td::Fill::default(),
        }));
        plane.deliver_td(tdpath::Hook::Position(tdpath::PositionEvent {
            td,
            positions: Vec::new(),
        }));
        assert_eq!(
            seen(&plane),
            vec!["trade 9".to_owned(), "book 1001 4".into(), "fill 7".into()]
        );
    }

    #[test]
    fn context_calls_before_init_find_nothing_and_do_not_schedule() {
        let plane = plane();
        plane.in_hook(|_, ctx| {
            assert_eq!(ctx.session_id(), "");
            assert!(ctx.params().is_empty());
            assert_eq!(ctx.volume_dir(), "");
            assert_eq!(ctx.input_dir("model"), "");
            assert_eq!(ctx.resolve("BTC"), Err(Error::NotInInit));
            assert_eq!(ctx.info(1), Err(Error::UnknownUid));
            assert_eq!(ctx.cancel(7, "1"), Err(Error::Stopping));
            assert!(ctx.open_orders(7, 0).is_empty());
            assert!(ctx.position(7, 1).is_none());
            assert_eq!(
                ctx.fetch_open_interest(1, |_, _, _| {}),
                Err(Error::Stopping)
            );
            assert_eq!(ctx.seed(), None);
            // The hook started when the plane called it.
            let now = ctx.now_ns();
            thread::sleep(Duration::from_millis(1));
            assert_eq!(ctx.now_ns(), now);
            assert!(now < ctx.clock().now_ns());
            // No dispatcher yet: these are dropped, as Go.
            ctx.after(Duration::ZERO, |s, _| s.seen.push("after".into()));
            ctx.post(|s, _| s.seen.push("post".into()));
            ctx.exit();
        });
        assert!(seen(&plane).is_empty());
    }

    /// Each hook's `now_ns`, read twice 1ms apart, and its `seed`.
    #[derive(Default)]
    struct Times {
        seen: Vec<(&'static str, i64, i64, Option<u64>)>,
    }

    impl Times {
        fn note(&mut self, hook: &'static str, ctx: &Context<Self>) {
            let now = ctx.now_ns();
            thread::sleep(Duration::from_millis(1));
            self.seen.push((hook, now, ctx.now_ns(), ctx.seed()));
        }
    }

    impl Strategy for Times {
        fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
            self.note("init", ctx);
            Ok(())
        }
        fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
            self.note("start", ctx);
            ctx.post(|s, ctx| s.note("post", ctx));
            Ok(())
        }
        fn on_stop(&mut self, ctx: &Context<Self>) {
            self.note("stop", ctx);
        }
    }

    fn start_payload() -> Vec<u8> {
        StartPayload {
            bindings: Some(Bindings::default()),
        }
        .encode_to_vec()
    }

    #[test]
    fn without_the_log_now_is_when_the_plane_or_the_dispatcher_started_the_hook() {
        let io = Io::start().expect("io thread");
        let clock = Clock::new();
        let plane = Plane::start(Times::default(), io.clone(), clock, Timers::new(io, clock))
            .expect("plane");
        open_offline(&plane);
        let before = clock.now_ns();
        assert_eq!(
            plane.start_call(&start_payload(), &CallCtx::background()),
            Ok(())
        );
        let queued = clock.now_ns();
        thread::sleep(Duration::from_millis(1));
        let disp = plane.dispatcher().expect("dispatcher");
        assert!(disp.run_one());
        assert_eq!(plane.on_stop_direct(), Ok(()));
        let after = clock.now_ns();
        let seen = plane.in_hook(|s, _| s.seen.clone());
        let [start, post, stop] = [0, 1, 2].map(|i| seen[i].1);
        // OnStart and OnStop read the clock as they start. The post starts at
        // the dispatcher's clock, which the post moved when it queued.
        assert!(before <= start && start <= queued, "{seen:?}");
        assert!(start <= post && post <= queued, "{seen:?}");
        assert!(queued < stop && stop <= after, "{seen:?}");
        assert_eq!(post, disp.hook_start_ns());
        assert_eq!(
            seen,
            [
                ("start", start, start, None),
                ("post", post, post, None),
                ("stop", stop, stop, None),
            ]
        );
    }

    // ---------- the event log ----------

    /// [`open_offline`] with the event log on.
    fn logged<S: Strategy>(strategy: S, log: &Scratch) -> Arc<Plane<S>> {
        let io = Io::start().expect("io thread");
        let clock = Clock::new();
        let plane =
            Plane::start(strategy, io.clone(), clock, Timers::new(io, clock)).expect("plane");
        let _ = plane.event_log.set(log.log());
        open_offline(&plane);
        plane
    }

    /// One line per frame: `rec_seq`, kind, and `ref` and `flags` when set.
    fn lines(frames: &[Record]) -> Vec<String> {
        frames
            .iter()
            .map(|f| {
                let mut line = format!("{} {}", f.seq, f.kind.name().unwrap_or("?"));
                if f.r#ref != 0 {
                    line += &format!(" ref={}", f.r#ref);
                }
                if f.flags != 0 {
                    line += &format!(" flags={}", f.flags);
                }
                line
            })
            .collect()
    }

    fn resolved(ticker: &str, rest: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        put_str(&mut body, ticker.as_bytes());
        body.extend_from_slice(rest);
        body
    }

    #[test]
    fn the_plane_writes_start_bindings_resolves_and_an_onstop_before_run() {
        let log = Scratch::open();
        let plane = logged(Recorder::default(), &log);
        let start = StartPayload {
            bindings: Some(Bindings::default()),
        }
        .encode_to_vec();
        assert_eq!(plane.start_call(&start, &CallCtx::background()), Ok(()));
        let v = |seq| Bindings {
            version: Some(Version { epoch: 1, seq }),
            ..Default::default()
        };
        let live = v(2).encode_to_vec();
        assert_eq!(plane.live(&live), Ok(()));
        // A snapshot that is refused is not written.
        let selector = Bindings {
            selectors: vec![Default::default()],
            ..v(3)
        };
        assert!(plane.live(&selector.encode_to_vec()).is_err());
        let cmd = Command {
            cmd: Some(command::Cmd::Bindings(v(3))),
        };
        plane.handle_command(&Relayed {
            at: plane.record_now(),
            payload: cmd.encode_to_vec(),
        });
        plane.in_hook(|_, ctx| assert_eq!(ctx.resolve("BTC"), Err(Error::NotInInit)));
        assert_eq!(plane.on_stop_direct(), Ok(()));

        let frames = log.close();
        assert_eq!(
            lines(&frames),
            [
                "1 start",
                "2 resolve flags=7",
                "3 hook_end ref=1",
                "4 bindings",
                "5 bindings flags=1",
                "6 resolve flags=5",
                "7 onstop",
                "8 hook_end ref=7",
                "9 end",
            ]
        );
        assert_eq!(frames[0].body, start);
        // Without SYM the session is stopping, and only Init and Start may
        // resolve.
        assert_eq!(
            frames[1].body,
            resolved("x", b"sdk: the session is stopping")
        );
        assert_eq!(
            frames[5].body,
            resolved("BTC", b"sdk: only OnInit and OnStart may call this")
        );
        assert_eq!(frames[3].body, live);
        assert_eq!(frames[4].body, v(3).encode_to_vec());
        for f in &frames[..frames.len() - 1] {
            match f.kind {
                // recv is the answer, t the call.
                Kind::RESOLVE => assert!(0 < f.t_ns && f.t_ns <= f.recv_ns, "{f:?}"),
                Kind::HOOK_END => assert!(f.body.is_empty(), "{f:?}"),
                _ => assert!(0 < f.recv_ns && f.recv_ns <= f.t_ns, "{f:?}"),
            }
        }
    }

    #[test]
    fn a_panic_outside_the_dispatcher_is_in_the_file_when_the_call_returns() {
        let log = Scratch::open();
        let plane = logged(Panics, &log);
        let start = StartPayload {
            bindings: Some(Bindings::default()),
        };
        assert_eq!(
            plane.start_call(&start.encode_to_vec(), &CallCtx::background()),
            Err(CallError::Failed("no venue".into()))
        );
        assert_eq!(
            plane.on_stop_direct(),
            Err(CallError::Failed("hook panic: boom".into()))
        );
        assert_eq!(log.log().stats(), (4, 0));

        let frames = log.close();
        assert_eq!(
            lines(&frames),
            [
                "1 start",
                "2 hook_end ref=1 flags=2",
                "3 onstop",
                "4 hook_end ref=3 flags=1",
                "5 end",
            ]
        );
        assert_eq!(frames[1].body, b"no venue");
        assert_eq!(frames[3].body, b"boom");
    }

    #[test]
    fn with_the_log_now_is_the_t_ns_of_the_hooks_input() {
        let log = Scratch::open();
        let plane = logged(Times::default(), &log);
        assert_eq!(
            plane.start_call(&start_payload(), &CallCtx::background()),
            Ok(())
        );
        assert!(plane.dispatcher().expect("dispatcher").run_one());
        assert_eq!(plane.on_stop_direct(), Ok(()));
        let seen = plane.in_hook(|s, _| s.seen.clone());

        let frames = log.close();
        assert_eq!(
            lines(&frames),
            [
                "1 start",
                "2 hook_end ref=1",
                "3 post ref=1",
                "4 hook_end ref=3",
                "5 onstop",
                "6 hook_end ref=5",
                "7 end",
            ]
        );
        let t = |i: usize| frames[i].t_ns;
        // Without Init there is no seed.
        assert_eq!(
            seen,
            [
                ("start", t(0), t(0), None),
                ("post", t(2), t(2), None),
                ("stop", t(4), t(4), None),
            ]
        );
    }

    #[test]
    fn a_failed_resolve_is_classified_as_go_classifies_it() {
        use symclient::Error as E;
        let request = |text: &str| E::Failed(format!("symclient: request sym: {text}"));
        let cases = [
            (E::NotInInit, outcome::NOT_IN_INIT),
            (E::NotFound("delisted".into()), outcome::NOT_FOUND),
            (E::Closed, outcome::STOPPING),
            (request(sym::NO_RESPONDERS), outcome::NO_RESPONDERS),
            (request(sym::DEADLINE_EXCEEDED), outcome::TIMEOUT),
            (
                E::Failed("symclient: subscribe sym.v1.changed.Bybit.Perp: nats: timeout".into()),
                outcome::TIMEOUT,
            ),
            (request(sym::CANCELED), outcome::CANCELED),
            (E::Failed("symclient: reply: bad".into()), outcome::FAILED),
            (
                E::InvalidTicker("symbol: bad ticker".into()),
                outcome::FAILED,
            ),
        ];
        for (err, flags) in cases {
            assert_eq!(resolve_outcome(&err), flags, "{err:?}");
        }
    }
}
