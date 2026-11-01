//! The TD path over NATS (PROTOCOL §7.2, §9.4.5–§9.4.7, §9.6; E03-07).
//!
//! `mftik_core::tdpath` holds the OMS, the ledger, the recon watermark, and
//! the backoff; this module performs its effects (E03-D001):
//!
//! - [`TdPath::listen`] subscribes `td.evt.{api_id}` for every AccountDecl
//!   and flushes before the first `td.view` ReconRequest (§9.6.1). A reader
//!   task per account decodes each message on the IO thread and tags it with
//!   that subscription's `api_id`, so every hook carries the account the
//!   subject names (§9.6.2).
//! - Submit, Cancel, and BatchCancel go to `td.order.{api_id}` in the order
//!   tdpath returned them: one task publishes each request after the one
//!   before it and lets the replies overlap. Each waits `recv_window`. A 503
//!   status is no responder (a 101 local reject); a timeout or a body that is
//!   not an `OrderReply` is no answer: Submit becomes `ORDER_STATUS_UNKNOWN`,
//!   a cancel changes nothing (§9.4.7, E08-D001, E08-D043, E08-D045).
//! - `td.view` replies, `td.order` outcomes, `td.evt` messages, and backoff
//!   ticks are pushed on the dispatcher's TD buffer and fed to tdpath there,
//!   one hook per buffer entry. tdpath's rule that nothing is fed while a
//!   hook's release chain is open holds because inputs that arrive in the
//!   middle of a chain wait in arrival order until it ends (E03-D071).
//! - `client_order_id` comes from `cloidgen`, the process-wide counter, with
//!   `now_ms` from the injected clock (§9.4.6, E08-D014).
//! - [`TdPath::unsubscribe`] is the dispatcher's `unsubscribe_td` on `DONE`
//!   and `FAILED` (§9.2.5): the readers stop, nothing that arrives later is
//!   pushed, and the drain still feeds what the TD buffer already holds
//!   (E03-D072).
//! - With the event log on, a reader keeps each `td.evt`'s bytes and the
//!   time it came as its `td_event` input. tdpath hands the input back with
//!   the event's hook, held or live, and the buffer entry that runs that hook
//!   writes it first with `record_input` (§12.4, E07-D007). An event that is
//!   dropped, or runs no strategy hook, is not written.
//! - With the event log on, Submit, Cancel, each round of a BatchCancel, and
//!   RequestRecon write an `order_call` numbered from 1 in call order. A
//!   request's `order_publish` and `order_reply` carry its call id: the
//!   publish once the bytes are on the connection, the reply as the input of
//!   the buffer entry that feeds it, which is an entry of its own when the
//!   round runs no hook. A Recon reply from `td.view` is the `td_view` input
//!   of its OnRecon (§12.4, §12.5, E07-D011).

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use async_nats::{Client, StatusCode, Subscriber};
use bytes::Bytes;
use futures_util::StreamExt;
use mftik_core::cloidgen;
use mftik_core::dispatch::{self, Dispatcher, PushError};
use mftik_core::eventlog::{
    self, FROM_BATCH, Kind, OP_BATCH_CANCEL, OP_CANCEL, OP_RECON, OP_SUBMIT, Recorder, outcome,
};
use mftik_core::tdpath::{
    self, Bound, ClientOrderIdError, ClientOrderIds, Effect, Hook, ReconOutcome, RoundOutcome,
    RoundTicket,
};
use mftik_proto::wire::mftiko::session::v1::AccountDecl;
use mftik_proto::wire::mftiko::td::v1 as td;
use prost::Message;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::clock::Clock;
use crate::context::Error;
use crate::io::Io;
use crate::strategy::Order;
use crate::sym::{DEADLINE_EXCEEDED, NO_RESPONDERS};
use crate::timers::{TimerKey, Timers};

#[cfg(test)]
mod tests;

/// The flush after the `td.evt` subscribes, as the Go session client's.
pub(crate) const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// Logged once a `td.view` recon reply is on the TD buffer. Conformance's
/// buffer priority check waits for these words (Go `tdpath.ask`).
pub(crate) const QUEUED_LINE: &str = "td.view: queued";

/// Go's text when the connection closes under a request
/// (`nats.ErrConnectionClosed`): a reply inbox that ends without a message.
const CONNECTION_CLOSED: &str = "nats: connection closed";

/// Runs one tdpath hook as a strategy hook (the plane's `deliver_td`).
pub(crate) type DeliverTd = Arc<dyn Fn(Hook) + Send + Sync>;
/// One stderr line.
pub(crate) type Log = Arc<dyn Fn(&str) + Send + Sync>;

/// What [`TdPath::open`] needs, from `InitPayload` and the plane.
pub(crate) struct Config {
    pub(crate) session_id: String,
    pub(crate) recv_window: Duration,
    pub(crate) accounts: Vec<AccountDecl>,
    pub(crate) nats: Option<Client>,
    pub(crate) io: Io,
    pub(crate) timers: Timers,
    pub(crate) clock: Clock,
    pub(crate) disp: Dispatcher,
    pub(crate) deliver: DeliverTd,
    pub(crate) log: Log,
    /// Offline replay: no NATS, order outputs go to [`Self::rec`].
    pub(crate) replay: bool,
    pub(crate) rec: Option<Arc<dyn Recorder>>,
    /// Recorded `order_call` time and submit_gen by call id.
    pub(crate) call: Option<ReplayCall>,
}

pub(crate) type ReplayCall = Arc<dyn Fn(u64) -> Option<(i64, u64)> + Send + Sync>;

/// One session's TD path. Clones share it.
#[derive(Clone)]
pub(crate) struct TdPath {
    inner: Arc<Inner>,
}

impl fmt::Debug for TdPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TdPath")
            .field("api_ids", &self.inner.api_ids)
            .finish_non_exhaustive()
    }
}

struct Inner {
    nats: Option<Client>,
    io: Io,
    timers: Timers,
    clock: Clock,
    disp: Dispatcher,
    deliver: DeliverTd,
    log: Log,
    api_ids: Vec<i64>,
    state: Mutex<State>,
    /// `td.order` requests, in publish order.
    orders: mpsc::UnboundedSender<Outgoing>,
    /// `true` stops the `td.evt` readers.
    readers: watch::Sender<bool>,
    /// `unsubscribe` ran: nothing new is pushed or sent.
    closing: AtomicBool,
    /// tdpath failed the session, or the TD buffer was full.
    failed: AtomicBool,
    /// The last order call id. Counted with the event log on only, as Go's.
    calls: AtomicU64,
    replay: bool,
    rec: Option<Arc<dyn Recorder>>,
    call: Option<ReplayCall>,
}

struct State {
    path: tdpath::Path,
    /// Inputs not fed yet, in arrival order.
    deferred: VecDeque<Input>,
    /// Hooks tdpath returned that have not run, with their event log inputs.
    /// No hook is an `order_reply` whose round ran none: its entry writes
    /// only the input, as Go writes every round's reply.
    chain: VecDeque<(Option<Hook>, Option<dispatch::Input>)>,
    /// A `resume` entry is on the TD buffer.
    queued: bool,
    /// Replay: requests waiting for their recorded `order_reply`, by call id.
    replay_rounds: HashMap<u64, RoundTicket>,
    #[cfg(test)]
    fed: u64,
}

enum Input {
    Event {
        api_id: i64,
        event: td::AccountEvent,
        /// The `td_event` input, with the event log on.
        logged: Option<dispatch::Input>,
    },
    Order {
        ticket: RoundTicket,
        outcome: RoundOutcome,
        /// The `order_reply` input, with the event log on.
        logged: Option<dispatch::Input>,
    },
    Recon {
        api_id: i64,
        generation: u64,
        outcome: ReconOutcome,
        /// The `td_view` input of a Recon reply, with the event log on.
        logged: Option<dispatch::Input>,
    },
    Backoff {
        api_id: i64,
    },
}

struct Outgoing {
    ticket: RoundTicket,
    /// The request's order call id; 0 with the event log off.
    call: u64,
    subject: String,
    body: Bytes,
    /// `recv_window` from when tdpath returned the send (Go's round context).
    deadline: Instant,
}

/// The order call whose `td.order` requests [`Inner::perform`] sends, as
/// its `order_call` frames write it. Only made with the event log on.
#[derive(Clone, Copy)]
struct Call {
    /// The call's clock. A Submit's is the one its client order id is made
    /// from (Go `callTime`).
    at_ns: i64,
    /// The td.view generation a Submit recorded; 0 for the cancels.
    submit_gen: u64,
    /// Each round of a BatchCancel.
    from_batch: bool,
}

/// How one `td.order` round ended.
enum Answer {
    /// A message came on the inbox, with its status.
    Reply(Option<StatusCode>, Bytes),
    NoResponders,
    /// The round's `recv_window` ran out.
    Timeout,
    /// Not sent, or the inbox ended; the text says why.
    Failed(String),
}

impl TdPath {
    /// Opens tdpath and the `td.order` sender. Errors are tdpath's texts,
    /// which end Init (Go `tdpath.Open`).
    pub(crate) fn open(config: Config) -> Result<TdPath, String> {
        let decls = config
            .accounts
            .iter()
            .map(|decl| tdpath::AccountDecl {
                api_id: decl.api_id,
                algo_ids: decl.algo_ids.clone(),
            })
            .collect::<Vec<_>>();
        let api_ids = decls.iter().map(|decl| decl.api_id).collect();
        let path = tdpath::Path::open(tdpath::Config {
            session_id: config.session_id,
            recv_window: config.recv_window,
            accounts: decls,
        })
        .map_err(|err| err.to_string())?;
        let (orders, outgoing) = mpsc::unbounded_channel();
        let inner = Arc::new(Inner {
            nats: config.nats,
            io: config.io,
            timers: config.timers,
            clock: config.clock,
            disp: config.disp,
            deliver: config.deliver,
            log: config.log,
            api_ids,
            state: Mutex::new(State {
                path,
                deferred: VecDeque::new(),
                chain: VecDeque::new(),
                queued: false,
                replay_rounds: HashMap::new(),
                #[cfg(test)]
                fed: 0,
            }),
            orders,
            readers: watch::Sender::new(false),
            closing: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            calls: AtomicU64::new(0),
            replay: config.replay,
            rec: config.rec,
            call: config.call,
        });
        if !inner.replay
            && let Some(nats) = inner.nats.clone()
        {
            inner
                .io
                .spawn(send_orders(Arc::downgrade(&inner), nats, outgoing));
        }
        Ok(TdPath { inner })
    }

    /// Subscribes `td.evt` for every account and flushes, so an event
    /// published after this returns is not missed; events are held until
    /// that account's recon (Go `tdpath.Listen`). Blocks on the IO thread;
    /// call it from the plane's Start.
    pub(crate) fn listen(&self) -> Result<(), String> {
        if self.inner.replay {
            self.inner.lock().path.listen_replay();
            return Ok(());
        }
        let inner = &self.inner;
        let effects = inner.lock().path.listen().map_err(|err| err.to_string())?;
        let Some(api_ids) = effects.into_iter().find_map(|effect| match effect {
            Effect::SubscribeEvt { api_ids } => Some(api_ids),
            _ => None,
        }) else {
            return Ok(());
        };
        let nats = inner
            .nats
            .clone()
            .ok_or_else(|| "tdpath: nil nats conn".to_owned())?;
        let subscribed = inner.io.block_on(async move {
            let mut subs = Vec::with_capacity(api_ids.len());
            for api_id in api_ids {
                let sub = nats
                    .subscribe(tdpath::evt_subject(api_id))
                    .await
                    .map_err(|err| err.to_string())?;
                subs.push((api_id, sub));
            }
            // A round trip, not a socket flush: the SUBs are on the server
            // before the gate (E03-D062, E03-D076).
            match crate::nats::barrier(&nats, FLUSH_TIMEOUT).await {
                Ok(()) => Ok(subs),
                Err(err) => Err(format!("nats: flush: {err}")),
            }
        });
        let subs = match subscribed {
            Ok(subs) => subs,
            Err(text) => {
                inner.lock().path.listen_aborted();
                return Err(text);
            }
        };
        let bound = inner.lock().path.listen_bound();
        match bound {
            Ok(Bound::Listening) => {
                for (api_id, sub) in subs {
                    inner.read_evt(api_id, sub);
                }
                Ok(())
            }
            Ok(Bound::Stopping(_)) => {
                // Dropping a subscriber unsubscribes from the IO thread.
                inner.io.spawn(async move { drop(subs) });
                Err(tdpath::Error::Stopping.to_string())
            }
            Err(err) => {
                inner.io.spawn(async move { drop(subs) });
                Err(err.to_string())
            }
        }
    }

    /// The first recon of every account, after OnStart (Go `tdpath.Recon`).
    /// Not an order call.
    pub(crate) fn recon(&self) -> Result<(), Error> {
        if self.inner.replay {
            return Ok(());
        }
        self.inner.call(false, |path| path.recon_all())
    }

    /// An order call with no `td.order` request: its reply is a `td_view`
    /// (Go `RequestRecon`).
    pub(crate) fn request_recon(&self, api_id: i64) -> Result<(), Error> {
        let inner = &self.inner;
        if inner.closing() {
            return Err(Error::Stopping);
        }
        let mut state = inner.lock();
        let effects = state.path.request_recon(api_id).map_err(sdk_error)?;
        let (at_ns, _) = inner.call_time();
        if inner.recorder().is_some() {
            inner.record_call(OP_RECON, at_ns, api_id, 0, &[]);
        }
        if !inner.replay {
            inner.perform(&mut state, effects, None);
        }
        Ok(())
    }

    pub(crate) fn submit(&self, order: Order) -> Result<String, Error> {
        let inner = &self.inner;
        if inner.closing() {
            return Err(Error::Stopping);
        }
        let (now_ns, recorded_gen) = inner.call_time();
        let mut ids = ProcessIds {
            api_id: order.api_id,
        };
        let mut state = inner.lock();
        let submitted = state
            .path
            .submit(order, now_ns.div_euclid(1_000_000), &mut ids)
            .map_err(sdk_error)?;
        let call = inner.recorder().map(|_| Call {
            at_ns: now_ns,
            submit_gen: recorded_gen.unwrap_or(submitted.submit_gen),
            from_batch: false,
        });
        inner.perform(&mut state, submitted.effects, call);
        Ok(submitted.client_order_id)
    }

    pub(crate) fn cancel(&self, api_id: i64, client_order_id: &str) -> Result<(), Error> {
        self.inner
            .call(false, |path| path.cancel(api_id, client_order_id))
    }

    /// Split by the account's `cancel_batch`; 1 sends one Cancel per order
    /// (§9.4.5, E06-D031).
    pub(crate) fn batch_cancel(
        &self,
        api_id: i64,
        algo_id: u32,
        uids: &[u32],
    ) -> Result<(), Error> {
        self.inner
            .call(true, |path| path.batch_cancel(api_id, algo_id, uids))
    }

    pub(crate) fn order(&self, api_id: i64, client_order_id: &str) -> Option<td::Order> {
        self.inner.lock().path.order(api_id, client_order_id)
    }

    pub(crate) fn open_orders(&self, api_id: i64, algo_id: u32) -> Vec<td::Order> {
        self.inner.lock().path.open_orders(api_id, algo_id)
    }

    pub(crate) fn position(&self, api_id: i64, uid: u32) -> Option<td::Position> {
        self.inner.lock().path.position(api_id, uid)
    }

    pub(crate) fn balance(&self, api_id: i64, asset: &str) -> Option<td::Balance> {
        self.inner.lock().path.balance(api_id, asset)
    }

    /// Apply a recorded `td_event` on the replay's hook thread.
    pub(crate) fn replay_event(&self, api_id: i64, body: &[u8]) -> Result<(), String> {
        let event = td::AccountEvent::decode(body).map_err(|err| err.to_string())?;
        let effect = self
            .inner
            .lock()
            .path
            .replay_event(api_id, event)
            .map_err(|err| err.to_string())?;
        self.inner.replay_effect(effect)
    }

    /// Apply a recorded `td_view` Recon reply on the replay's hook thread.
    pub(crate) fn replay_view(&self, api_id: i64, body: &[u8]) -> Result<(), String> {
        let (generation, raw) = eventlog::parse_td_view(body).map_err(|err| err.to_string())?;
        let reply = td::ViewReply::decode(raw).map_err(|err| err.to_string())?;
        if !is_recon(&reply) {
            return Err("tdpath: not a recon".into());
        }
        let effects = self
            .inner
            .lock()
            .path
            .replay_view(api_id, generation, reply)
            .map_err(|err| err.to_string())?;
        self.inner.replay_effects(effects)
    }

    /// Finish the replayed request of `call` with its recorded `order_reply`.
    pub(crate) fn replay_reply(&self, call: u64, flags: u16, body: &[u8]) -> Result<(), String> {
        let ticket = self
            .inner
            .lock()
            .replay_rounds
            .remove(&call)
            .ok_or_else(|| {
                format!("tdpath: no replayed request waits for this reply: call {call}")
            })?;
        let outcome = match flags {
            outcome::OK => match td::OrderReply::decode(body) {
                Ok(reply) => RoundOutcome::Reply(reply),
                Err(_) => RoundOutcome::NoAnswer,
            },
            outcome::NO_RESPONDERS => RoundOutcome::NoResponder,
            _ => RoundOutcome::NoAnswer,
        };
        let effects = self.inner.lock().path.on_order_round(ticket, outcome);
        self.inner.replay_effects(effects)
    }

    /// The dispatcher's `unsubscribe_td`, before the TD drain of `DONE` and
    /// `FAILED` (§9.2.5, E08-D003). The `td.evt` readers stop, recon backoffs
    /// are disarmed, and replies and events that arrive after this are not
    /// pushed. What the TD buffer already holds is still fed and delivered
    /// by the drain. Order calls return [`Error::Stopping`] from here on, as
    /// Go's after `tdpath.Close`. Idempotent.
    pub(crate) fn unsubscribe(&self) {
        let inner = &self.inner;
        if inner.closing.swap(true, Ordering::SeqCst) {
            return;
        }
        inner.readers.send_replace(true);
        for &api_id in &inner.api_ids {
            inner.timers.disarm(TimerKey::TdBackoff { api_id });
        }
    }

    /// Inputs fed to tdpath so far.
    #[cfg(test)]
    fn fed(&self) -> u64 {
        self.inner.lock().fed
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn closing(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }

    fn replay_effects(self: &Arc<Self>, effects: Vec<Effect>) -> Result<(), String> {
        for effect in effects {
            if let Effect::Hook(hook) = effect {
                self.replay_effect(Some(Effect::Hook(hook)))?;
            }
        }
        Ok(())
    }

    fn replay_effect(self: &Arc<Self>, effect: Option<Effect>) -> Result<(), String> {
        let Some(Effect::Hook(hook)) = effect else {
            return Ok(());
        };
        (self.deliver)(*hook);
        let _ = self.lock().path.end_hook();
        Ok(())
    }

    /// A tdpath call a hook makes. Its effects are sends, never hooks. With
    /// the event log on, its `td.order` requests are order calls at the
    /// call's clock; `from_batch` marks a BatchCancel's rounds.
    fn call(
        self: &Arc<Self>,
        from_batch: bool,
        f: impl FnOnce(&mut tdpath::Path) -> Result<Vec<Effect>, tdpath::Error>,
    ) -> Result<(), Error> {
        if self.closing() {
            return Err(Error::Stopping);
        }
        let mut state = self.lock();
        let effects = f(&mut state.path).map_err(sdk_error)?;
        let (at_ns, _) = self.call_time();
        let call = self.recorder().map(|_| Call {
            at_ns,
            submit_gen: 0,
            from_batch,
        });
        self.perform(&mut state, effects, call);
        Ok(())
    }

    /// Writes one `order_call` and returns its call id, which the request's
    /// `order_publish` and `order_reply` carry. Under the state lock, so ids
    /// follow call order. 0 with the event log off (Go `Path.record`).
    fn recorder(&self) -> Option<&Arc<dyn Recorder>> {
        self.rec.as_ref().or_else(|| self.disp.recorder())
    }

    /// The clock of one order call. A replay reads the recorded call it is
    /// about to repeat, so the client order id is made from that time.
    fn call_time(&self) -> (i64, Option<u64>) {
        if self.replay
            && let Some(call) = &self.call
        {
            let next = self.calls.load(Ordering::Relaxed) + 1;
            if let Some((ns, submit_gen)) = call(next) {
                return (ns, Some(submit_gen));
            }
        }
        let now = if self.replay {
            self.disp.now_ns()
        } else {
            self.clock.now_ns()
        };
        (now, None)
    }

    fn record_call(
        &self,
        flags: u16,
        at_ns: i64,
        api_id: i64,
        submit_gen: u64,
        cloids: &[&str],
    ) -> u64 {
        let Some(rec) = self.recorder() else {
            return 0;
        };
        let mut body = Vec::new();
        eventlog::order_call(&mut body, api_id, submit_gen, cloids);
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        rec.append(Kind::ORDER_CALL, flags, 0, at_ns, call, &body, &[]);
        call
    }

    /// The `order_call` of one `td.order` request: the operation and the
    /// client order ids come from the request.
    fn record_order(&self, call: Call, api_id: i64, request: &td::OrderRequest) -> u64 {
        use td::order_request::Req;
        let (op, cloids) = match &request.req {
            Some(Req::Submit(submit)) => (OP_SUBMIT, vec![submit.client_order_id.as_str()]),
            Some(Req::Cancel(cancel)) => (OP_CANCEL, vec![cancel.client_order_id.as_str()]),
            Some(Req::BatchCancel(batch)) => (
                OP_BATCH_CANCEL,
                batch
                    .orders
                    .iter()
                    .map(|order| order.client_order_id.as_str())
                    .collect(),
            ),
            None => (0, Vec::new()),
        };
        let flags = if call.from_batch { op | FROM_BATCH } else { op };
        self.record_call(flags, call.at_ns, api_id, call.submit_gen, &cloids)
    }

    /// Moves the dispatcher's clock before queueing (E03-D052).
    fn fresh_now(&self) {
        self.disp.set_now_ns(self.clock.now_ns());
    }

    /// Queues `input` on the TD buffer. `false` when it was not queued.
    fn push(self: &Arc<Self>, input: Input) -> bool {
        if self.closing() {
            return false;
        }
        self.fresh_now();
        let this = Arc::clone(self);
        match self.disp.push_td(move || this.accept(input)) {
            Ok(()) => true,
            // The dispatcher ends the session FAILED with "td buffer full".
            Err(PushError::Full) => {
                self.failed.store(true, Ordering::SeqCst);
                false
            }
            Err(PushError::Closed) => false,
        }
    }

    /// A TD buffer entry with one input, on the dispatcher thread.
    fn accept(self: &Arc<Self>, input: Input) {
        {
            let mut state = self.lock();
            state.deferred.push_back(input);
            if state.queued {
                // A chain is open; `resume` reaches this input in order.
                return;
            }
        }
        self.step();
    }

    /// The TD buffer entry that continues a chain.
    fn resume(self: &Arc<Self>) {
        self.lock().queued = false;
        self.step();
    }

    /// Feeds waiting inputs until one returns a hook, runs that hook, then
    /// queues `resume` for what is left. At most one strategy hook per TD
    /// buffer entry, so each gets its own budget and TD keeps its priority
    /// over MD between them.
    fn step(self: &Arc<Self>) {
        let next = {
            let mut state = self.lock();
            loop {
                if let Some(next) = state.chain.pop_front() {
                    break Some(next);
                }
                let Some(input) = state.deferred.pop_front() else {
                    break None;
                };
                let (effects, logged) = state.feed(input);
                self.perform(&mut state, effects, None);
                if let Some(logged) = logged {
                    state.attach(logged);
                }
            }
        };
        if let Some((hook, logged)) = next {
            if let Some(logged) = &logged {
                self.disp.record_input(logged);
            }
            if let Some(hook) = hook {
                (self.deliver)(hook);
                let mut state = self.lock();
                let next = state.path.end_hook();
                // After unsubscribe Go's release stops at the next held event
                // (`releaseStep` sees the path failed); so does this one.
                if let Some(effect) = next
                    && !self.closing()
                {
                    self.perform(&mut state, vec![effect], None);
                }
            }
        }
        let more = {
            let mut state = self.lock();
            let more = !state.queued && !(state.chain.is_empty() && state.deferred.is_empty());
            if more {
                state.queued = true;
            }
            more
        };
        if more {
            self.fresh_now();
            let this = Arc::clone(self);
            if self.disp.push_td(move || this.resume()).is_err() {
                self.lock().queued = false;
            }
        }
    }

    /// Does what tdpath asked. Hooks wait in the chain for `step`. Each
    /// `td.order` request is an order call of `call`'s, with the event log on.
    fn perform(self: &Arc<Self>, state: &mut State, effects: Vec<Effect>, call: Option<Call>) {
        for effect in effects {
            match effect {
                Effect::Hook(hook) => {
                    let logged = state.path.take_input();
                    state.chain.push_back((Some(*hook), logged));
                }
                // Go cancels the path's context in Close: no new sends.
                _ if self.closing() => {}
                Effect::SendOrder {
                    ticket,
                    api_id,
                    subject,
                    request,
                    wait,
                } => {
                    let call = call.map_or(0, |call| self.record_order(call, api_id, &request));
                    if self.replay {
                        if let Some(rec) = self.recorder() {
                            rec.append(
                                Kind::ORDER_PUBLISH,
                                0,
                                0,
                                rec.now_ns(),
                                call,
                                &request.encode_to_vec(),
                                &[],
                            );
                        }
                        state.replay_rounds.insert(call, ticket);
                        continue;
                    }
                    let _ = self.orders.send(Outgoing {
                        ticket,
                        call,
                        subject,
                        body: request.encode_to_vec().into(),
                        deadline: Instant::now() + wait,
                    });
                }
                Effect::SendRecon {
                    api_id,
                    generation,
                    subject,
                    request,
                    wait,
                } => self.send_recon(api_id, generation, subject, request.encode_to_vec(), wait),
                Effect::ArmBackoff { api_id, after } => {
                    let deadline_ns = self.clock.now_ns().saturating_add(duration_ns(after));
                    let weak = Arc::downgrade(self);
                    self.timers
                        .arm(TimerKey::TdBackoff { api_id }, deadline_ns, move || {
                            if let Some(this) = weak.upgrade() {
                                this.push(Input::Backoff { api_id });
                            }
                        });
                }
                Effect::Fail { reason } => {
                    self.failed.store(true, Ordering::SeqCst);
                    (self.log)(&reason);
                    self.disp.fail(reason);
                }
                Effect::UnsubscribeEvt { .. } => {
                    self.readers.send_replace(true);
                }
                // Only `listen` returns it, and handles it there.
                Effect::SubscribeEvt { .. } => {}
            }
        }
    }

    /// One `td.view` ReconRequest. Any failure is `ReconOutcome::Failed`;
    /// tdpath arms the backoff (E08-D042).
    fn send_recon(
        self: &Arc<Self>,
        api_id: i64,
        generation: u64,
        subject: String,
        body: Vec<u8>,
        wait: Duration,
    ) {
        let weak = Arc::downgrade(self);
        let nats = self.nats.clone().expect("td nats");
        self.io.spawn(async move {
            let request = async_nats::Request::new()
                .payload(body.into())
                .timeout(Some(wait));
            let reply = nats.send_request(subject, request).await;
            let Some(this) = weak.upgrade() else {
                return;
            };
            let (outcome, logged) = match reply {
                Ok(msg) => match td::ViewReply::decode(msg.payload.as_ref()) {
                    Ok(reply) if is_recon(&reply) => {
                        let logged = this.view_input(api_id, generation, msg.payload);
                        (ReconOutcome::Reply(reply), logged)
                    }
                    Ok(reply) => (ReconOutcome::Reply(reply), None),
                    Err(_) => (ReconOutcome::Failed, None),
                },
                Err(_) => (ReconOutcome::Failed, None),
            };
            let recon = matches!(&outcome, ReconOutcome::Reply(reply) if is_recon(reply));
            let queued = this.push(Input::Recon {
                api_id,
                generation,
                outcome,
                logged,
            });
            if queued && recon && !this.failed.load(Ordering::SeqCst) {
                (this.log)(QUEUED_LINE);
            }
        });
    }

    /// A Recon reply's `td_view` input: the request's generation, then the
    /// reply as it came (Go `ask`). `None` with the event log off.
    fn view_input(&self, api_id: i64, generation: u64, reply: Bytes) -> Option<dispatch::Input> {
        let recv_ns = self.disp.record_now()?;
        Some(dispatch::Input {
            kind: Kind::TD_VIEW,
            recv_ns,
            r#ref: api_id as u64,
            head: Bytes::copy_from_slice(&eventlog::td_view_head(generation)),
            body: reply,
            ..dispatch::Input::default()
        })
    }

    /// Queues how a `td.order` round ended, with its `order_reply` input
    /// when the event log is on (Go `replyIn`). Once `unsubscribe` ran
    /// nothing is queued or written.
    fn answer(self: &Arc<Self>, ticket: RoundTicket, call: u64, answer: Answer) {
        let logged = self.disp.record_now().map(|recv_ns| dispatch::Input {
            kind: Kind::ORDER_REPLY,
            flags: answer.flags(),
            recv_ns,
            r#ref: call,
            body: answer.body(),
            ..dispatch::Input::default()
        });
        self.push(Input::Order {
            ticket,
            outcome: answer.outcome(),
            logged,
        });
    }

    /// Reads `td.evt.{api_id}` until `unsubscribe`. The account is the
    /// subscription's, not anything in the message (§9.6.2).
    fn read_evt(self: &Arc<Self>, api_id: i64, mut sub: Subscriber) {
        let weak = Arc::downgrade(self);
        let mut stop = self.readers.subscribe();
        self.io.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = stop.wait_for(|stop| *stop) => break,
                    msg = sub.next() => {
                        let Some(msg) = msg else { break };
                        let Some(this) = weak.upgrade() else { break };
                        let at = this.disp.record_now();
                        match td::AccountEvent::decode(msg.payload.as_ref()) {
                            Ok(event) => {
                                let logged = at.map(|recv_ns| dispatch::Input {
                                    kind: Kind::TD_EVENT,
                                    recv_ns,
                                    r#ref: api_id as u64,
                                    body: msg.payload,
                                    ..dispatch::Input::default()
                                });
                                this.push(Input::Event { api_id, event, logged });
                            }
                            Err(err) => (this.log)(&format!("td.evt: {err}")),
                        }
                    }
                }
            }
            let _ = sub.unsubscribe().await;
        });
    }
}

impl State {
    /// Feeds one input to tdpath. The second value is its `order_reply` or
    /// `td_view`, for [`State::attach`] once the effects are performed.
    fn feed(&mut self, input: Input) -> (Vec<Effect>, Option<dispatch::Input>) {
        #[cfg(test)]
        {
            self.fed += 1;
        }
        match input {
            Input::Event {
                api_id,
                event,
                logged,
            } => (self.path.on_account_event_in(api_id, event, logged), None),
            Input::Order {
                ticket,
                outcome,
                logged,
            } => (self.path.on_order_round(ticket, outcome), logged),
            Input::Recon {
                api_id,
                generation,
                outcome,
                logged,
            } => (
                self.path.on_recon_round(api_id, generation, outcome),
                logged,
            ),
            Input::Backoff { api_id } => (self.path.on_backoff(api_id), None),
        }
    }

    /// Gives a round's input to the hook feeding it returned, which leads
    /// the chain: `step` feeds only once the chain is empty. A round that
    /// returned no hook still writes its `order_reply`, as an entry of its
    /// own; its `td_view` is not written, as a `td.evt` that runs no hook.
    fn attach(&mut self, logged: dispatch::Input) {
        if let Some((Some(_), slot)) = self.chain.front_mut()
            && slot.is_none()
        {
            *slot = Some(logged);
        } else if logged.kind == Kind::ORDER_REPLY {
            self.chain.push_front((None, Some(logged)));
        }
    }
}

/// Publishes each `td.order` request after the previous one is on the
/// connection, then waits for its reply in its own task (Go
/// `requestInOrder`). With the event log on, the request is its call's
/// `order_publish` once it is on the connection.
async fn send_orders(
    inner: Weak<Inner>,
    nats: Client,
    mut outgoing: mpsc::UnboundedReceiver<Outgoing>,
) {
    while let Some(out) = outgoing.recv().await {
        let Some(this) = inner.upgrade() else {
            return;
        };
        if this.closing() {
            continue;
        }
        let Outgoing {
            ticket,
            call,
            subject,
            body,
            deadline,
        } = out;
        let logged = this.recorder().map(|_| body.clone());
        match tokio::time::timeout_at(deadline, publish(&nats, subject, body)).await {
            Ok(Ok(sub)) => {
                if let (Some(rec), Some(body)) = (this.recorder(), logged) {
                    rec.append(Kind::ORDER_PUBLISH, 0, 0, rec.now_ns(), call, &body, &[]);
                }
                let weak = Weak::clone(&inner);
                tokio::spawn(async move {
                    let answer = reply(sub, deadline).await;
                    if let Some(this) = weak.upgrade() {
                        this.answer(ticket, call, answer);
                    }
                });
            }
            Ok(Err(text)) => this.answer(ticket, call, Answer::Failed(text)),
            Err(_) => this.answer(ticket, call, Answer::Timeout),
        }
    }
}

/// Subscribes a reply inbox, then publishes. Both go through the
/// connection's one command queue, so the subscription is in place before
/// the request.
async fn publish(nats: &Client, subject: String, body: Bytes) -> Result<Subscriber, String> {
    let inbox = nats.new_inbox();
    let sub = nats
        .subscribe(inbox.clone())
        .await
        .map_err(|err| err.to_string())?;
    nats.publish_with_reply(subject, inbox, body)
        .await
        .map_err(|err| err.to_string())?;
    Ok(sub)
}

async fn reply(mut sub: Subscriber, deadline: Instant) -> Answer {
    match tokio::time::timeout_at(deadline, sub.next()).await {
        Ok(Some(msg)) if msg.status == Some(StatusCode::NO_RESPONDERS) => Answer::NoResponders,
        Ok(Some(msg)) => Answer::Reply(msg.status, msg.payload),
        Ok(None) => Answer::Failed(CONNECTION_CLOSED.to_owned()),
        Err(_) => Answer::Timeout,
    }
}

impl Answer {
    /// A 503 status is no responder (a 101 local reject); a timeout, a
    /// failure, or a body that is not an `OrderReply` is no answer.
    fn outcome(self) -> RoundOutcome {
        match self {
            Answer::Reply(None | Some(StatusCode::OK), body) => {
                match td::OrderReply::decode(body.as_ref()) {
                    Ok(reply) => RoundOutcome::Reply(reply),
                    Err(_) => RoundOutcome::NoAnswer,
                }
            }
            Answer::NoResponders => RoundOutcome::NoResponder,
            Answer::Reply(..) | Answer::Timeout | Answer::Failed(_) => RoundOutcome::NoAnswer,
        }
    }

    /// The `order_reply` flags, as Go's `evlog.Outcome` classifies the
    /// request's error. A message is OK whatever it holds.
    fn flags(&self) -> u16 {
        match self {
            Answer::Reply(..) => outcome::OK,
            Answer::NoResponders => outcome::NO_RESPONDERS,
            Answer::Timeout => outcome::TIMEOUT,
            Answer::Failed(_) => outcome::FAILED,
        }
    }

    /// The `order_reply` body: the message's bytes, or the error's text.
    fn body(&self) -> Bytes {
        match self {
            Answer::Reply(_, body) => body.clone(),
            Answer::NoResponders => Bytes::from_static(NO_RESPONDERS.as_bytes()),
            Answer::Timeout => Bytes::from_static(DEADLINE_EXCEEDED.as_bytes()),
            Answer::Failed(text) => Bytes::from(text.clone()),
        }
    }
}

fn is_recon(reply: &td::ViewReply) -> bool {
    matches!(reply.reply, Some(td::view_reply::Reply::Recon(_)))
}

/// `cloidgen::next`: one counter for the process, every algo and account
/// (§9.4.6, E08-D011, E08-D014).
struct ProcessIds {
    api_id: i64,
}

impl ClientOrderIds for ProcessIds {
    fn next(
        &mut self,
        algo_ids: &[u32],
        algo_id: u32,
        now_ms: i64,
    ) -> Result<String, ClientOrderIdError> {
        let decl = AccountDecl {
            api_id: self.api_id,
            algo_ids: algo_ids.to_vec(),
            ..AccountDecl::default()
        };
        cloidgen::next(&decl, algo_id, now_ms).map_err(|err| {
            if err.is_algo_not_declared() {
                ClientOrderIdError::AlgoNotDeclared
            } else {
                ClientOrderIdError::Other(err.to_string())
            }
        })
    }
}

/// tdpath's errors as Go's `asSDK` maps them. The rest keep tdpath's text;
/// a cloidgen error keeps cloidgen's, as Go returns it unwrapped.
fn sdk_error(err: tdpath::Error) -> Error {
    match err {
        tdpath::Error::UnknownAccount => Error::UnknownAccount,
        tdpath::Error::AlgoNotDeclared => Error::AlgoNotDeclared,
        tdpath::Error::Stopping => Error::Stopping,
        tdpath::Error::ClientOrderId(text) => Error::Other(text),
        other => Error::Other(other.to_string()),
    }
}

fn duration_ns(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}
