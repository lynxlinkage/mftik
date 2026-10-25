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

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use async_nats::{Client, StatusCode, Subscriber};
use futures_util::StreamExt;
use mftik_core::cloidgen;
use mftik_core::dispatch::{Dispatcher, PushError};
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
use crate::timers::{TimerKey, Timers};

#[cfg(test)]
mod tests;

/// The flush after the `td.evt` subscribes, as the Go session client's.
pub(crate) const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// Logged once a `td.view` recon reply is on the TD buffer. Conformance's
/// buffer priority check waits for these words (Go `tdpath.ask`).
pub(crate) const QUEUED_LINE: &str = "td.view: queued";

/// Runs one tdpath hook as a strategy hook (the plane's `deliver_td`).
pub(crate) type DeliverTd = Arc<dyn Fn(Hook) + Send + Sync>;
/// One stderr line.
pub(crate) type Log = Arc<dyn Fn(&str) + Send + Sync>;

/// What [`TdPath::open`] needs, from `InitPayload` and the plane.
pub(crate) struct Config {
    pub(crate) session_id: String,
    pub(crate) recv_window: Duration,
    pub(crate) accounts: Vec<AccountDecl>,
    pub(crate) nats: Client,
    pub(crate) io: Io,
    pub(crate) timers: Timers,
    pub(crate) clock: Clock,
    pub(crate) disp: Dispatcher,
    pub(crate) deliver: DeliverTd,
    pub(crate) log: Log,
}

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
    nats: Client,
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
}

struct State {
    path: tdpath::Path,
    /// Inputs not fed yet, in arrival order.
    deferred: VecDeque<Input>,
    /// Hooks tdpath returned that have not run.
    chain: VecDeque<Hook>,
    /// A `resume` entry is on the TD buffer.
    queued: bool,
    #[cfg(test)]
    fed: u64,
}

enum Input {
    Event {
        api_id: i64,
        event: td::AccountEvent,
    },
    Order {
        ticket: RoundTicket,
        outcome: RoundOutcome,
    },
    Recon {
        api_id: i64,
        generation: u64,
        outcome: ReconOutcome,
    },
    Backoff {
        api_id: i64,
    },
}

struct Outgoing {
    ticket: RoundTicket,
    subject: String,
    body: Vec<u8>,
    /// `recv_window` from when tdpath returned the send (Go's round context).
    deadline: Instant,
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
                #[cfg(test)]
                fed: 0,
            }),
            orders,
            readers: watch::Sender::new(false),
            closing: AtomicBool::new(false),
            failed: AtomicBool::new(false),
        });
        inner.io.spawn(send_orders(
            Arc::downgrade(&inner),
            inner.nats.clone(),
            outgoing,
        ));
        Ok(TdPath { inner })
    }

    /// Subscribes `td.evt` for every account and flushes, so an event
    /// published after this returns is not missed; events are held until
    /// that account's recon (Go `tdpath.Listen`). Blocks on the IO thread;
    /// call it from the plane's Start.
    pub(crate) fn listen(&self) -> Result<(), String> {
        let inner = &self.inner;
        let effects = inner.lock().path.listen().map_err(|err| err.to_string())?;
        let Some(api_ids) = effects.into_iter().find_map(|effect| match effect {
            Effect::SubscribeEvt { api_ids } => Some(api_ids),
            _ => None,
        }) else {
            return Ok(());
        };
        let nats = inner.nats.clone();
        let subscribed = inner.io.block_on(async move {
            let mut subs = Vec::with_capacity(api_ids.len());
            for api_id in api_ids {
                let sub = nats
                    .subscribe(tdpath::evt_subject(api_id))
                    .await
                    .map_err(|err| err.to_string())?;
                subs.push((api_id, sub));
            }
            match tokio::time::timeout(FLUSH_TIMEOUT, nats.flush()).await {
                Ok(Ok(())) => Ok(subs),
                Ok(Err(err)) => Err(err.to_string()),
                Err(_) => Err("nats: flush timeout".to_owned()),
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
    pub(crate) fn recon(&self) -> Result<(), Error> {
        self.inner.call(|path| path.recon_all())
    }

    pub(crate) fn request_recon(&self, api_id: i64) -> Result<(), Error> {
        self.inner.call(|path| path.request_recon(api_id))
    }

    pub(crate) fn submit(&self, order: Order) -> Result<String, Error> {
        let inner = &self.inner;
        if inner.closing() {
            return Err(Error::Stopping);
        }
        let now_ms = inner.clock.now_ns().div_euclid(1_000_000);
        let mut ids = ProcessIds {
            api_id: order.api_id,
        };
        let mut state = inner.lock();
        let submitted = state
            .path
            .submit(order, now_ms, &mut ids)
            .map_err(sdk_error)?;
        inner.perform(&mut state, submitted.effects);
        Ok(submitted.client_order_id)
    }

    pub(crate) fn cancel(&self, api_id: i64, client_order_id: &str) -> Result<(), Error> {
        self.inner.call(|path| path.cancel(api_id, client_order_id))
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
            .call(|path| path.batch_cancel(api_id, algo_id, uids))
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

    /// A tdpath call a hook makes. Its effects are sends, never hooks.
    fn call(
        self: &Arc<Self>,
        f: impl FnOnce(&mut tdpath::Path) -> Result<Vec<Effect>, tdpath::Error>,
    ) -> Result<(), Error> {
        if self.closing() {
            return Err(Error::Stopping);
        }
        let mut state = self.lock();
        let effects = f(&mut state.path).map_err(sdk_error)?;
        self.perform(&mut state, effects);
        Ok(())
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
        let hook = {
            let mut state = self.lock();
            loop {
                if let Some(hook) = state.chain.pop_front() {
                    break Some(hook);
                }
                let Some(input) = state.deferred.pop_front() else {
                    break None;
                };
                let effects = state.feed(input);
                self.perform(&mut state, effects);
            }
        };
        if let Some(hook) = hook {
            (self.deliver)(hook);
            let mut state = self.lock();
            let next = state.path.end_hook();
            // After unsubscribe Go's release stops at the next held event
            // (`releaseStep` sees the path failed); so does this one.
            if let Some(effect) = next
                && !self.closing()
            {
                self.perform(&mut state, vec![effect]);
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

    /// Does what tdpath asked. Hooks wait in the chain for `step`.
    fn perform(self: &Arc<Self>, state: &mut State, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Hook(hook) => state.chain.push_back(*hook),
                // Go cancels the path's context in Close: no new sends.
                _ if self.closing() => {}
                Effect::SendOrder {
                    ticket,
                    subject,
                    request,
                    wait,
                    ..
                } => {
                    let _ = self.orders.send(Outgoing {
                        ticket,
                        subject,
                        body: request.encode_to_vec(),
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
        let nats = self.nats.clone();
        self.io.spawn(async move {
            let request = async_nats::Request::new()
                .payload(body.into())
                .timeout(Some(wait));
            let outcome = match nats.send_request(subject, request).await {
                Ok(msg) => match td::ViewReply::decode(msg.payload.as_ref()) {
                    Ok(reply) => ReconOutcome::Reply(reply),
                    Err(_) => ReconOutcome::Failed,
                },
                Err(_) => ReconOutcome::Failed,
            };
            let recon = matches!(
                &outcome,
                ReconOutcome::Reply(td::ViewReply {
                    reply: Some(td::view_reply::Reply::Recon(_)),
                    ..
                })
            );
            let Some(this) = weak.upgrade() else {
                return;
            };
            let queued = this.push(Input::Recon {
                api_id,
                generation,
                outcome,
            });
            if queued && recon && !this.failed.load(Ordering::SeqCst) {
                (this.log)(QUEUED_LINE);
            }
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
                        match td::AccountEvent::decode(msg.payload.as_ref()) {
                            Ok(event) => {
                                this.push(Input::Event { api_id, event });
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
    fn feed(&mut self, input: Input) -> Vec<Effect> {
        #[cfg(test)]
        {
            self.fed += 1;
        }
        match input {
            Input::Event { api_id, event } => self.path.on_account_event(api_id, event),
            Input::Order { ticket, outcome } => self.path.on_order_round(ticket, outcome),
            Input::Recon {
                api_id,
                generation,
                outcome,
            } => self.path.on_recon_round(api_id, generation, outcome),
            Input::Backoff { api_id } => self.path.on_backoff(api_id),
        }
    }
}

/// Publishes each `td.order` request after the previous one is on the
/// connection, then waits for its reply in its own task (Go
/// `requestInOrder`).
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
        let ticket = out.ticket;
        let deadline = out.deadline;
        match tokio::time::timeout_at(deadline, publish(&nats, out)).await {
            Ok(Some(sub)) => {
                let weak = Weak::clone(&inner);
                tokio::spawn(async move {
                    let outcome = reply(sub, deadline).await;
                    if let Some(this) = weak.upgrade() {
                        this.push(Input::Order { ticket, outcome });
                    }
                });
            }
            // Not sent, or not before the round's deadline: no answer.
            Ok(None) | Err(_) => {
                this.push(Input::Order {
                    ticket,
                    outcome: RoundOutcome::NoAnswer,
                });
            }
        }
    }
}

/// Subscribes a reply inbox, then publishes. Both go through the
/// connection's one command queue, so the subscription is in place before
/// the request.
async fn publish(nats: &Client, out: Outgoing) -> Option<Subscriber> {
    let inbox = nats.new_inbox();
    let sub = nats.subscribe(inbox.clone()).await.ok()?;
    nats.publish_with_reply(out.subject, inbox, out.body.into())
        .await
        .ok()?;
    Some(sub)
}

async fn reply(mut sub: Subscriber, deadline: Instant) -> RoundOutcome {
    let Ok(Some(msg)) = tokio::time::timeout_at(deadline, sub.next()).await else {
        return RoundOutcome::NoAnswer;
    };
    match msg.status {
        Some(StatusCode::NO_RESPONDERS) => RoundOutcome::NoResponder,
        None | Some(StatusCode::OK) => match td::OrderReply::decode(msg.payload.as_ref()) {
            Ok(reply) => RoundOutcome::Reply(reply),
            Err(_) => RoundOutcome::NoAnswer,
        },
        Some(_) => RoundOutcome::NoAnswer,
    }
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
