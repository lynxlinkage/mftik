//! The SYM path: `symclient` over NATS (PROTOCOL §7.3, §9.4.8; E03-D041,
//! E03-D081–E03-D085).
//!
//! [`Sym`] owns the sans-IO [`symclient::Client`] and performs its effects:
//!
//! - `Request` is one NATS request on `sym`. A request that a resolve is
//!   blocked on (`resolve: Some(id)`) waits until the deadline of that
//!   resolve's hook (`deadline_ms` of Init or Start) and ends when the hook's
//!   context is cancelled; without a deadline it waits the effect's 5 s.
//!   Background refetches wait 5 s (#411, E03-D081).
//! - `Subscribe` subscribes `sym.v1.changed.{venue}.{category}`, flushes, and
//!   reports the result; its messages go to `on_changed` in arrival order.
//! - `ArmTimer` is [`TimerKey::Sym`] in the session's timer table.
//! - `Stored { uid }` calls the callback set with [`Sym::on_stored`].
//! - `Resolved { id }` wakes the hook blocked in [`Sym::resolve`]. The result
//!   may come from the reply to another request: a resolve that meets a
//!   refetch of its table waits for that refetch.
//! - `Log` is a stderr line.
//!
//! The NATS connection's reconnects reach [`Sym::on_reconnect`] through
//! [`Sym::reconnect_hook`] (Go's `ReconnectHandler`, E03-D084).
//!
//! Replies, messages, and timers are handled on the IO thread. The client is
//! behind a mutex that no one holds across IO, so the hook thread blocked in
//! `resolve` never stops the IO thread.

use std::collections::HashMap;
use std::fmt;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use futures_util::StreamExt;
use mftik_core::symbol::Info;
use mftik_core::symclient::{
    self, Client, Config, Effect, Error, Outcome, REQUEST_TIMEOUT, ResolveId, Resolving, SUBJECT,
};
use prost::Message;
use tokio::task::AbortHandle;

use crate::call::CallCtx;
use crate::clock::Clock;
use crate::io::Io;
use crate::nats::Reconnected;
use crate::timers::{TimerKey, Timers};

/// A row `symclient` stored, with the row as stored. See [`Sym::on_stored`].
pub type OnStored = Arc<dyn Fn(u32, &Info) + Send + Sync>;

/// A log line.
pub type Log = Arc<dyn Fn(&str) + Send + Sync>;

/// Go's texts for a request that did not get a reply (`nats.go`,
/// `context`), as mdpath uses them (E03-D013).
pub const DEADLINE_EXCEEDED: &str = "context deadline exceeded";
pub const CANCELED: &str = "context canceled";
pub const NO_RESPONDERS: &str = "nats: no responders available for request";

/// How long a subscribe waits for its flush, Go's `FlushTimeout`.
pub const SUBSCRIBE_FLUSH: Duration = REQUEST_TIMEOUT;

/// The session's SYM path. Clones share it.
#[derive(Clone)]
pub struct Sym {
    inner: Arc<Inner>,
}

struct Inner {
    client: Mutex<Client>,
    host: OnceLock<Host>,
    waiters: Mutex<HashMap<ResolveId, Waiter>>,
    subs: Mutex<HashMap<String, AbortHandle>>,
    on_stored: Mutex<Option<OnStored>>,
}

struct Host {
    io: Io,
    clock: Clock,
    timers: Timers,
    nats: async_nats::Client,
    log: Log,
}

/// A resolve blocked in a hook: where its result goes, and the hook's
/// context that bounds its requests.
struct Waiter {
    tx: mpsc::Sender<Result<Info, Error>>,
    call: CallCtx,
}

impl fmt::Debug for Sym {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sym")
            .field("open", &self.inner.host.get().is_some())
            .finish_non_exhaustive()
    }
}

impl Default for Sym {
    fn default() -> Self {
        Self::new(Config::default()).expect("the default config is valid")
    }
}

impl Sym {
    /// A path with nothing resolved. Until [`Sym::open`], `resolve` is
    /// [`Error::Closed`] and effects are not performed.
    pub fn new(config: Config) -> Result<Self, symclient::OpenError> {
        Ok(Self {
            inner: Arc::new(Inner {
                client: Mutex::new(Client::open(config)?),
                host: OnceLock::new(),
                waiters: Mutex::default(),
                subs: Mutex::default(),
                on_stored: Mutex::default(),
            }),
        })
    }

    /// Gives the path the session's connection, once Init has it. A second
    /// call does nothing.
    pub fn open(
        &self,
        io: &Io,
        clock: Clock,
        timers: &Timers,
        nats: &async_nats::Client,
        log: Log,
    ) {
        let _ = self.inner.host.set(Host {
            io: io.clone(),
            clock,
            timers: timers.clone(),
            nats: nats.clone(),
            log,
        });
    }

    /// Sets the callback for each `Stored { uid }`: a resolve or a refetch
    /// wrote the row, and [`Sym::info`] returns it. The MD path passes
    /// `info.price_tick()` to `bookpath::Path::resolve` here (E03-D045). In
    /// a session it runs on the IO thread, once per store, in the order the
    /// rows were stored; it must not block and must not call
    /// [`Sym::resolve`]. Setting it again replaces it (E03-D083).
    pub fn on_stored(&self, f: OnStored) {
        *lock(&self.inner.on_stored) = Some(f);
    }

    /// `symclient::Client::resolve`, blocking until its `Resolved`. The
    /// caller has checked that OnInit or OnStart is running (E08-D004).
    /// `call` is that hook's context. Before [`Sym::open`] this is
    /// [`Error::Closed`].
    pub fn resolve(&self, call: &CallCtx, ticker: &str) -> Result<Info, Error> {
        let Some(host) = self.inner.host.get() else {
            return Err(Error::Closed);
        };
        let (tx, rx) = mpsc::channel();
        let effects = {
            let mut client = lock(&self.inner.client);
            client.allow_resolve(true);
            let resolving = client.resolve(host.clock.now_ns(), ticker);
            client.allow_resolve(false);
            let Resolving { id, effects } = resolving?;
            let waiter = Waiter {
                tx,
                call: call.clone(),
            };
            lock(&self.inner.waiters).insert(id, waiter);
            effects
        };
        self.inner.perform(effects);
        rx.recv().unwrap_or(Err(Error::Closed))
    }

    /// The row in memory (E08-D025).
    pub fn info(&self, uid: u32) -> Result<Info, Error> {
        lock(&self.inner.client).info(uid).cloned()
    }

    /// The connection is back: compare revisions now (PROTOCOL §7.3). Before
    /// [`Sym::open`] it does nothing.
    pub fn on_reconnect(&self) {
        let Some(host) = self.inner.host.get() else {
            return;
        };
        let effects = lock(&self.inner.client).on_reconnect(host.clock.now_ns());
        self.inner.perform(effects);
    }

    /// [`Sym::on_reconnect`] as the callback [`crate::nats::dial`] takes.
    pub fn reconnect_hook(&self) -> Reconnected {
        let sym = self.clone();
        Arc::new(move || sym.on_reconnect())
    }
}

impl Inner {
    fn perform(self: &Arc<Self>, effects: Vec<Effect>) {
        let Some(host) = self.host.get() else {
            return;
        };
        for effect in effects {
            match effect {
                Effect::Request {
                    ticket,
                    request,
                    wait,
                    resolve,
                } => {
                    let call =
                        resolve.and_then(|id| lock(&self.waiters).get(&id).map(|w| w.call.clone()));
                    let inner = Arc::clone(self);
                    let nats = host.nats.clone();
                    let payload = request.encode_to_vec();
                    host.io.spawn(async move {
                        let reply = request_sym(&nats, payload, wait, call).await;
                        let Some(host) = inner.host.get() else {
                            return;
                        };
                        let outcome = match &reply {
                            Ok(body) => Outcome::Body(body),
                            Err(text) => Outcome::Failed(text),
                        };
                        let effects =
                            lock(&inner.client).on_reply(host.clock.now_ns(), ticket, outcome);
                        inner.perform(effects);
                    });
                }
                Effect::Subscribe { ticket, subject } => {
                    let inner = Arc::clone(self);
                    let nats = host.nats.clone();
                    let name = subject.clone();
                    let task = host.io.spawn(async move {
                        let subscribed = subscribe(&nats, &name).await;
                        let Some(host) = inner.host.get() else {
                            return;
                        };
                        let result = subscribed.as_ref().map(|_| ()).map_err(String::as_str);
                        let effects =
                            lock(&inner.client).on_subscribed(host.clock.now_ns(), ticket, result);
                        inner.perform(effects);
                        let Ok(mut sub) = subscribed else {
                            return;
                        };
                        while let Some(msg) = sub.next().await {
                            let effects = lock(&inner.client).on_changed(
                                host.clock.now_ns(),
                                msg.subject.as_str(),
                                &msg.payload,
                            );
                            inner.perform(effects);
                        }
                    });
                    lock(&self.subs).insert(subject, task.abort_handle());
                }
                Effect::Unsubscribe { subject } => {
                    // Dropping the subscriber unsubscribes it.
                    if let Some(task) = lock(&self.subs).remove(&subject) {
                        task.abort();
                    }
                }
                Effect::ArmTimer {
                    token, deadline_ns, ..
                } => {
                    let inner = Arc::clone(self);
                    host.timers.arm(TimerKey::Sym, deadline_ns, move || {
                        let Some(host) = inner.host.get() else {
                            return;
                        };
                        let effects = lock(&inner.client).on_timer(host.clock.now_ns(), token);
                        inner.perform(effects);
                    });
                }
                Effect::Stored { uid } => {
                    let f = lock(&self.on_stored).clone();
                    if let Some(f) = f {
                        let row = lock(&self.client).info(uid).cloned();
                        if let Ok(info) = row {
                            f(uid, &info);
                        }
                    }
                }
                Effect::Resolved { id, result } => {
                    if let Some(waiter) = lock(&self.waiters).remove(&id) {
                        let _ = waiter.tx.send(result);
                    }
                }
                Effect::Log(line) => (host.log)(&line),
            }
        }
    }
}

/// One request on [`SUBJECT`]. With `call`, it waits until the hook's
/// deadline, or `wait` when the hook has none, and ends when the hook's
/// context ends (Go's `natsBus.Request` with the hook's ctx). `Err` is the
/// text for [`Outcome::Failed`].
async fn request_sym(
    nats: &async_nats::Client,
    payload: Vec<u8>,
    wait: Duration,
    call: Option<CallCtx>,
) -> Result<Vec<u8>, String> {
    let wait = call.as_ref().and_then(CallCtx::remaining).unwrap_or(wait);
    if let Some(call) = &call
        && call.is_done()
    {
        return Err(done_text(call).to_owned());
    }
    let request = async_nats::Request::new()
        .payload(payload.into())
        .timeout(Some(wait));
    let reply = nats.send_request(SUBJECT, request);
    let reply = match &call {
        Some(call) => tokio::select! {
            biased;
            reply = reply => reply,
            () = call.done() => return Err(done_text(call).to_owned()),
        },
        None => reply.await,
    };
    match reply {
        Ok(msg) => Ok(msg.payload.to_vec()),
        Err(err) => Err(match err.kind() {
            async_nats::RequestErrorKind::TimedOut => DEADLINE_EXCEEDED.to_owned(),
            async_nats::RequestErrorKind::NoResponders => NO_RESPONDERS.to_owned(),
            _ => err.to_string(),
        }),
    }
}

fn done_text(call: &CallCtx) -> &'static str {
    if call.is_cancelled() {
        CANCELED
    } else {
        DEADLINE_EXCEEDED
    }
}

/// Subscribe and flush, so the subscription is on the server before
/// `on_subscribed` (Go's `Subscribe` then `FlushTimeout`).
async fn subscribe(
    nats: &async_nats::Client,
    subject: &str,
) -> Result<async_nats::Subscriber, String> {
    let sub = nats
        .subscribe(subject.to_owned())
        .await
        .map_err(|err| err.to_string())?;
    // A round trip, not a socket flush (E03-D076).
    match crate::nats::barrier(nats, SUBSCRIBE_FLUSH).await {
        Ok(()) => Ok(sub),
        Err(err) if err == "timeout" => Err("nats: timeout".to_owned()),
        Err(err) => Err(err),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
