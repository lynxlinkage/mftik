//! Session SYM client (PROTOCOL §7.3, §9.4 item 8; E08-D004, E08-D022–E08-D026,
//! E04-D057).
//!
//! A strategy resolves the tickers of its own config in `OnInit` and `OnStart`.
//! [`Client::resolve`] asks SYM with `GetRequest` and may block the hook.
//! After that, any hook reads the current row with [`Client::info`], which
//! reads memory only. A UID this session has not resolved is
//! [`Error::UnknownUid`]; the client does not retry and does not end the
//! session (E08-D024, E08-D025). Nothing here looks up a UID that arrived on an
//! event, and nothing holds an event back (E08-D023). Phase 1 has no selector
//! (E08-D022).
//!
//! Only resolved instruments are tracked. Each resolved `(venue, category)` is
//! subscribed on `sym.v1.changed.{venue}.{category}`. A newer notification, a
//! revision check about every five minutes that finds the table behind, and a
//! NATS reconnect each refetch that table's tracked rows with
//! `GetByUidRequest`. A row SYM has moved to the terminal table is refetched
//! with `include_inactive`; the last copy stays, its terminal
//! [`crate::symbol::Status`] is the inactive mark, and `info` still returns it
//! (E08-D026).
//!
//! # Driving it
//!
//! This is a pure state machine (E03-D001). Every call takes the injected
//! `now_ns` and returns effects. NATS, the timer, and the blocking wait of
//! `resolve` belong to the host.
//!
//! ```text
//! client.allow_resolve(true);           // around OnInit and OnStart only
//! let Resolving { id, effects } = client.resolve(now_ns, ticker)?;
//! perform(effects);                     // until Effect::Resolved { id, .. }
//! client.allow_resolve(false);
//!
//! fn perform(effects: Vec<Effect>) {
//!     for effect in effects {
//!         match effect {
//!             Effect::Request { ticket, request, wait, .. } => {
//!                 let outcome = nats.request(SUBJECT, request.encode_to_vec(), wait);
//!                 perform(client.on_reply(now_ns, ticket, outcome));
//!             }
//!             Effect::Subscribe { ticket, subject } => {
//!                 let result = nats.subscribe(subject); // messages -> on_changed
//!                 perform(client.on_subscribed(now_ns, ticket, result));
//!             }
//!             Effect::Unsubscribe { subject } => nats.unsubscribe(subject),
//!             Effect::ArmTimer { token, deadline_ns, .. } => arm(token, deadline_ns),
//!             Effect::Stored { uid } => {
//!                 bookpath.resolve(uid, client.info(uid)?.price_tick());
//!             }
//!             Effect::Resolved { id, result } => wake(id, result),
//!             Effect::Log(line) => log(line),
//!         }
//!     }
//! }
//! ```
//!
//! Perform effects in the order returned. Report [`Client::on_subscribed`]
//! once the subscription is on the connection; the `Revisions` read that
//! follows it then cannot miss a later change. Requests may overlap; match
//! each reply to its [`Ticket`]. A resolve can finish in a reply to a request
//! it did not send, when it waits on a refetch already under way.
//!
//! `Request::wait` is five seconds, the Go SDK's default. A request with
//! `resolve` set is on a hook's critical path; Go waits there until the
//! hook's deadline instead, when `OnInit` or `OnStart` has one.
//!
//! # Refetch
//!
//! One `(venue, category)` refetches at a time. A pass reads `Revisions`,
//! refetches every tracked row of the table in UID order, and reads
//! `Revisions` again. The table's revision is confirmed only when both reads
//! agree and no resolve added a row to the table during the pass. Otherwise
//! the pass repeats, up to [`REFRESH_TRIES`] times. A failed pass keeps the
//! rows already stored and leaves the confirmed revision as it was, so the
//! next check tries again. A notification, a check, or a resolve that arrives
//! during a pass is folded into one more pass after it, when it still needs
//! one.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::time::Duration;

use mftik_proto::wire::mftiko::sym::v1::{self as sym, reply, request};
use prost::Message;

use crate::symbol::{Category, Filters, Info, Status, UniversalTicker, valid_uid};

#[cfg(test)]
mod tests;

/// The SYM request subject (PROTOCOL §7, E04-D057). Exactly `sym`.
pub const SUBJECT: &str = "sym";

/// Gap between revision checks when no notification arrived (PROTOCOL §7.3).
pub const CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Wait for one SYM request, the Go SDK's default.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Passes one refetch makes before it gives up on a moving table.
pub const REFRESH_TRIES: u32 = 8;

/// natsx error reply: code of a handler error. SYM's `not_found` uses it.
const CODE_HANDLER: u16 = 2;

/// `sym.v1.changed.{venue}.{category}`. `None` when either part is empty or
/// contains `.`, `*`, `>`, a space, or a tab.
pub fn changed_subject(venue: &str, category: &str) -> Option<String> {
    let token = |value: &str| !value.is_empty() && !value.contains(['.', '*', '>', ' ', '\t']);
    if !token(venue) || !token(category) {
        return None;
    }
    Some(format!("sym.v1.changed.{venue}.{category}"))
}

/// What [`Client::open`] needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Revision check period. Zero is rejected.
    pub check_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            check_interval: CHECK_INTERVAL,
        }
    }
}

/// Why [`Client::open`] refused the config.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenError {
    /// `check_interval` is zero.
    CheckInterval,
}

impl fmt::Display for OpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CheckInterval => {
                formatter.write_str("symclient: check interval must be positive")
            }
        }
    }
}

impl std::error::Error for OpenError {}

/// A failed `resolve` or `info`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// `resolve` outside `OnInit` and `OnStart` (E08-D004). Nothing was sent.
    NotInInit,
    /// `info` of a UID this session has not resolved (E08-D024).
    UnknownUid,
    /// The ticker is not a canonical UniversalTicker. Nothing was sent.
    InvalidTicker(String),
    /// SYM has no such ticker in the active table. The platform does not
    /// support it; the strategy returning this ends the session `INIT_FAILED`.
    /// The text is SYM's reason.
    NotFound(String),
    /// Timeout, no responder, a reply that does not decode, or a failed
    /// subscribe.
    Failed(String),
    /// [`Client::close`] was called.
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInInit => {
                formatter.write_str("symclient: only OnInit and OnStart may call Resolve")
            }
            Self::UnknownUid => {
                formatter.write_str("symclient: the instrument is not known to this session")
            }
            Self::InvalidTicker(message) | Self::Failed(message) => formatter.write_str(message),
            Self::NotFound(reason) => write!(formatter, "symbol: no such instrument: {reason}"),
            Self::Closed => formatter.write_str("symclient: closed"),
        }
    }
}

impl std::error::Error for Error {}

/// One [`Effect::Request`] or [`Effect::Subscribe`]. Pass it back with the
/// outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ticket(u64);

/// One [`Client::resolve`]. [`Effect::Resolved`] carries it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResolveId(u64);

/// One [`Effect::ArmTimer`]. Only the latest one is live.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimerToken(u64);

/// How one SYM request ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome<'a> {
    /// The reply payload as received: a `sym.v1.Reply` or a natsx error reply.
    Body(&'a [u8]),
    /// Timeout, no responder, or the request could not be sent. The text is
    /// kept in the error.
    Failed(&'a str),
}

/// [`Client::resolve`] accepted the ticker. Perform `effects`; the result
/// arrives as [`Effect::Resolved`] with this `id`.
#[derive(Clone, Debug, PartialEq)]
pub struct Resolving {
    pub id: ResolveId,
    pub effects: Vec<Effect>,
}

/// Something the host must do.
#[derive(Clone, Debug, PartialEq)]
pub enum Effect {
    /// Publish `request` on [`SUBJECT`] and wait up to `wait`. Hand the reply
    /// to [`Client::on_reply`] with `ticket`. `resolve` is set while that
    /// resolve is blocked on this request.
    Request {
        ticket: Ticket,
        request: sym::Request,
        wait: Duration,
        resolve: Option<ResolveId>,
    },
    /// Subscribe `subject` and pass its messages to [`Client::on_changed`].
    /// Report the result with [`Client::on_subscribed`].
    Subscribe { ticket: Ticket, subject: String },
    /// Drop a subscription. Only [`Client::close`] returns this.
    Unsubscribe { subject: String },
    /// Call [`Client::on_timer`] at `deadline_ns` on the injected clock.
    ArmTimer {
        token: TimerToken,
        after: Duration,
        deadline_ns: i64,
    },
    /// A row was written by a resolve or a refetch. [`Client::info`] has it.
    /// The host passes its `price_tick` to [`crate::bookpath::Path::resolve`].
    Stored { uid: u32 },
    /// The resolve finished. `Ok` is the row as stored now.
    Resolved {
        id: ResolveId,
        result: Result<Info, Error>,
    },
    /// One log line. The text follows the Go SDK.
    Log(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Key {
    venue: String,
    category: Category,
}

impl Key {
    fn of(ticker: &UniversalTicker) -> Self {
        Self {
            venue: ticker.venue.clone(),
            category: ticker.category,
        }
    }

    fn subject(&self) -> Option<String> {
        changed_subject(&self.venue, self.category.as_str())
    }
}

impl fmt::Display for Key {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.venue, self.category)
    }
}

/// What an outstanding request is for.
#[derive(Debug)]
enum Wait {
    Get(ResolveId),
    Sync(Key),
    Rev { key: Key, first: bool },
    Row { key: Key, uid: u32, inactive: bool },
    Check,
}

#[derive(Debug)]
enum Stage {
    Get(UniversalTicker),
    Subscribing { key: Key, info: Box<Info> },
    Settling { uid: u32 },
}

#[derive(Debug)]
enum Sub {
    Pending,
    Active,
}

/// Why a pass started. It picks the log line of a failure, as in Go.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Why {
    Sync,
    Notify,
    Check,
}

#[derive(Debug)]
struct Pass {
    why: Why,
    attempt: u32,
    rev1: i64,
    uids: Vec<u32>,
    next: usize,
    dirty: bool,
}

#[derive(Debug)]
enum Op {
    Idle,
    /// A resolve's `Revisions` read, before deciding on a pass.
    Sync,
    Pass(Pass),
}

#[derive(Debug)]
struct Table {
    uids: BTreeSet<u32>,
    confirmed: Option<i64>,
    op: Op,
    waiters: Vec<ResolveId>,
    again: bool,
    want: Option<i64>,
}

impl Table {
    fn new() -> Self {
        Self {
            uids: BTreeSet::new(),
            confirmed: None,
            op: Op::Idle,
            waiters: Vec::new(),
            again: false,
            want: None,
        }
    }
}

/// A failed request, before it becomes a log line or an [`Error`].
#[derive(Debug, PartialEq, Eq)]
enum Fault {
    NotFound(String),
    Other(String),
}

impl Fault {
    fn into_error(self) -> Error {
        match self {
            Self::NotFound(reason) => Error::NotFound(reason),
            Self::Other(message) => Error::Failed(message),
        }
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(reason) => write!(formatter, "symbol: no such instrument: {reason}"),
            Self::Other(message) => formatter.write_str(message),
        }
    }
}

/// One session's view of the instruments it resolved.
#[derive(Debug)]
pub struct Client {
    interval: Duration,
    now_ns: i64,
    allow: bool,
    closed: bool,
    rows: HashMap<u32, Info>,
    tables: BTreeMap<Key, Table>,
    subs: BTreeMap<Key, Sub>,
    sub_tickets: HashMap<Ticket, Key>,
    waits: HashMap<Ticket, Wait>,
    resolves: BTreeMap<ResolveId, Stage>,
    checking: bool,
    check_again: bool,
    timer: Option<u64>,
    next_id: u64,
}

impl Client {
    /// Nothing tracked, nothing subscribed. `resolve` is refused until
    /// [`Client::allow_resolve`].
    pub fn open(config: Config) -> Result<Self, OpenError> {
        if config.check_interval.is_zero() {
            return Err(OpenError::CheckInterval);
        }
        Ok(Self {
            interval: config.check_interval,
            now_ns: 0,
            allow: false,
            closed: false,
            rows: HashMap::new(),
            tables: BTreeMap::new(),
            subs: BTreeMap::new(),
            sub_tickets: HashMap::new(),
            waits: HashMap::new(),
            resolves: BTreeMap::new(),
            checking: false,
            check_again: false,
            timer: None,
            next_id: 0,
        })
    }

    /// True while `OnInit` or `OnStart` is the running hook (E08-D004). The
    /// host clears it around every other hook.
    pub fn allow_resolve(&mut self, on: bool) {
        self.allow = on;
    }

    pub fn resolve_allowed(&self) -> bool {
        self.allow && !self.closed
    }

    /// Look `ticker` up with `GetRequest`. Outside the resolve window, after
    /// close, or for a ticker that is not canonical, this fails here and
    /// nothing is sent.
    ///
    /// A ticker SYM does not list ends as [`Error::NotFound`] and is not
    /// tracked. On success the table is subscribed, the row is stored, and the
    /// table's revision is checked before [`Effect::Resolved`]. A failed check
    /// is logged and does not fail the resolve.
    pub fn resolve(&mut self, now_ns: i64, ticker: &str) -> Result<Resolving, Error> {
        self.now_ns = now_ns;
        if self.closed {
            return Err(Error::Closed);
        }
        if !self.allow {
            return Err(Error::NotInInit);
        }
        let parsed =
            UniversalTicker::parse(ticker).map_err(|err| Error::InvalidTicker(err.to_string()))?;
        let id = ResolveId(self.mint());
        let request = request::Req::Get(sym::GetRequest {
            universal_ticker: parsed.to_string(),
            include_inactive: false,
        });
        self.resolves.insert(id, Stage::Get(parsed));
        let mut effects = Vec::new();
        self.send(Wait::Get(id), request, Some(id), &mut effects);
        Ok(Resolving { id, effects })
    }

    /// The current row of a resolved instrument. Memory only (E08-D025); a
    /// delisted row is still returned (E08-D026). The borrow lasts until the
    /// next `&mut self` call.
    pub fn info(&self, uid: u32) -> Result<&Info, Error> {
        self.rows.get(&uid).ok_or(Error::UnknownUid)
    }

    /// The reply to an [`Effect::Request`]. An unknown or reused ticket is
    /// ignored.
    pub fn on_reply(&mut self, now_ns: i64, ticket: Ticket, outcome: Outcome<'_>) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) {
            return out;
        }
        let Some(wait) = self.waits.remove(&ticket) else {
            return out;
        };
        let reply = decode(outcome);
        match wait {
            Wait::Get(id) => self.on_get(id, reply, &mut out),
            Wait::Sync(key) => self.on_sync(key, reply, &mut out),
            Wait::Rev { key, first } => self.on_rev(key, first, reply, &mut out),
            Wait::Row { key, uid, inactive } => self.on_row(key, uid, inactive, reply, &mut out),
            Wait::Check => self.on_check(reply, &mut out),
        }
        out
    }

    /// The result of an [`Effect::Subscribe`]. `Err` carries the host's text.
    /// A failed subscribe fails the resolves waiting on it; their rows are
    /// not stored, and a later resolve subscribes again.
    pub fn on_subscribed(
        &mut self,
        now_ns: i64,
        ticket: Ticket,
        result: Result<(), &str>,
    ) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) {
            return out;
        }
        let Some(key) = self.sub_tickets.remove(&ticket) else {
            return out;
        };
        let waiting: Vec<ResolveId> = self
            .resolves
            .iter()
            .filter(|(_, stage)| matches!(stage, Stage::Subscribing { key: k, .. } if *k == key))
            .map(|(id, _)| *id)
            .collect();
        match result {
            Ok(()) => {
                self.subs.insert(key, Sub::Active);
                for id in waiting {
                    if let Some(Stage::Subscribing { key, info }) = self.resolves.remove(&id) {
                        self.admit(id, key, *info, &mut out);
                    }
                }
            }
            Err(text) => {
                self.subs.remove(&key);
                let subject = key.subject().unwrap_or_default();
                for id in waiting {
                    self.resolves.remove(&id);
                    out.push(Effect::Resolved {
                        id,
                        result: Err(Error::Failed(format!(
                            "symclient: subscribe {subject}: {text}"
                        ))),
                    });
                }
            }
        }
        out
    }

    /// One message from a `sym.v1.changed.*` subscription.
    ///
    /// A newer revision of a tracked table refetches its rows. A revision at
    /// or below the confirmed one does nothing. A message that does not decode,
    /// does not match its subject, or carries a revision below 1 runs a full
    /// revision check instead.
    pub fn on_changed(&mut self, now_ns: i64, subject: &str, payload: &[u8]) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) {
            return out;
        }
        let Ok(msg) = sym::Changed::decode(payload) else {
            self.check(&mut out);
            return out;
        };
        let matches =
            changed_subject(&msg.venue, &msg.category).is_some_and(|want| want == subject);
        if !matches || msg.revision < 1 {
            self.check(&mut out);
            return out;
        }
        let Some(category) = Category::from_name(&msg.category) else {
            return out;
        };
        let key = Key {
            venue: msg.venue,
            category,
        };
        let Some(table) = self.tables.get_mut(&key) else {
            return out;
        };
        if table.uids.is_empty() || table.confirmed.is_some_and(|rev| msg.revision <= rev) {
            return out;
        }
        if matches!(table.op, Op::Idle) {
            self.start_pass(&key, Why::Notify, &mut out);
        } else {
            table.want = Some(
                table
                    .want
                    .map_or(msg.revision, |want| want.max(msg.revision)),
            );
        }
        out
    }

    /// The NATS connection came back (PROTOCOL §7.3). Compare revisions now.
    pub fn on_reconnect(&mut self, now_ns: i64) -> Vec<Effect> {
        let mut out = Vec::new();
        if self.begin(now_ns) {
            self.check(&mut out);
        }
        out
    }

    /// The revision-check timer fired. A stale token is ignored. The next
    /// check is armed from `now_ns`.
    pub fn on_timer(&mut self, now_ns: i64, token: TimerToken) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) || self.timer != Some(token.0) {
            return out;
        }
        self.timer = None;
        self.check(&mut out);
        self.arm(&mut out);
        out
    }

    /// Stop. Pending resolves end with [`Error::Closed`], every subject a
    /// `Subscribe` was returned for is unsubscribed, and later replies,
    /// messages, and timers are ignored. `info` keeps working.
    pub fn close(&mut self) -> Vec<Effect> {
        let mut out = Vec::new();
        if self.closed {
            return out;
        }
        self.closed = true;
        self.allow = false;
        for (id, _) in std::mem::take(&mut self.resolves) {
            out.push(Effect::Resolved {
                id,
                result: Err(Error::Closed),
            });
        }
        for (key, _) in std::mem::take(&mut self.subs) {
            if let Some(subject) = key.subject() {
                out.push(Effect::Unsubscribe { subject });
            }
        }
        self.sub_tickets.clear();
        self.waits.clear();
        self.timer = None;
        for table in self.tables.values_mut() {
            table.op = Op::Idle;
            table.waiters.clear();
        }
        out
    }

    fn begin(&mut self, now_ns: i64) -> bool {
        self.now_ns = now_ns;
        !self.closed
    }

    fn mint(&mut self) -> u64 {
        self.next_id = self.next_id.wrapping_add(1);
        self.next_id
    }

    fn send(
        &mut self,
        wait: Wait,
        req: request::Req,
        resolve: Option<ResolveId>,
        out: &mut Vec<Effect>,
    ) {
        let ticket = Ticket(self.mint());
        self.waits.insert(ticket, wait);
        out.push(Effect::Request {
            ticket,
            request: sym::Request { req: Some(req) },
            wait: REQUEST_TIMEOUT,
            resolve,
        });
    }

    /// A table request. It carries the first resolve blocked on the table.
    fn send_table(&mut self, key: &Key, wait: Wait, req: request::Req, out: &mut Vec<Effect>) {
        let resolve = self
            .tables
            .get(key)
            .and_then(|table| table.waiters.first().copied());
        self.send(wait, req, resolve, out);
    }

    fn arm(&mut self, out: &mut Vec<Effect>) {
        let id = self.mint();
        self.timer = Some(id);
        out.push(Effect::ArmTimer {
            token: TimerToken(id),
            after: self.interval,
            deadline_ns: deadline_ns(self.now_ns, self.interval),
        });
    }

    fn on_get(&mut self, id: ResolveId, reply: Result<sym::Reply, Fault>, out: &mut Vec<Effect>) {
        let Some(Stage::Get(ticker)) = self.resolves.remove(&id) else {
            return;
        };
        let info = match reply.and_then(info_of) {
            Ok(info) if info.ticker == ticker => info,
            Ok(info) => {
                let message = format!("symclient: get {ticker} returned {}", info.ticker);
                out.push(Effect::Resolved {
                    id,
                    result: Err(Error::Failed(message)),
                });
                return;
            }
            Err(fault) => {
                out.push(Effect::Resolved {
                    id,
                    result: Err(fault.into_error()),
                });
                return;
            }
        };
        let key = Key::of(&info.ticker);
        match self.subs.get(&key) {
            Some(Sub::Active) => self.admit(id, key, info, out),
            Some(Sub::Pending) => {
                self.resolves.insert(
                    id,
                    Stage::Subscribing {
                        key,
                        info: Box::new(info),
                    },
                );
            }
            None => {
                let Some(subject) = key.subject() else {
                    let message = format!("symclient: {key} is not a subject token");
                    out.push(Effect::Resolved {
                        id,
                        result: Err(Error::Failed(message)),
                    });
                    return;
                };
                let ticket = Ticket(self.mint());
                self.subs.insert(key.clone(), Sub::Pending);
                self.sub_tickets.insert(ticket, key.clone());
                self.resolves.insert(
                    id,
                    Stage::Subscribing {
                        key,
                        info: Box::new(info),
                    },
                );
                out.push(Effect::Subscribe { ticket, subject });
            }
        }
    }

    /// The table is subscribed: store the row, then settle the table.
    fn admit(&mut self, id: ResolveId, key: Key, info: Info, out: &mut Vec<Effect>) {
        let uid = info.uid;
        self.store(&key, info, out);
        if self.timer.is_none() {
            self.arm(out);
        }
        self.resolves.insert(id, Stage::Settling { uid });
        let Some(table) = self.tables.get_mut(&key) else {
            return;
        };
        table.waiters.push(id);
        let idle = match &mut table.op {
            Op::Idle => true,
            Op::Sync => false,
            Op::Pass(pass) => {
                // A row the pass has not listed: its revision is unknown, so
                // the pass must not confirm one.
                if !pass.uids.contains(&uid) {
                    pass.dirty = true;
                }
                false
            }
        };
        if idle {
            table.op = Op::Sync;
            self.send_table(
                &key,
                Wait::Sync(key.clone()),
                request::Req::Revisions(sym::RevisionsRequest {}),
                out,
            );
        }
    }

    fn store(&mut self, key: &Key, info: Info, out: &mut Vec<Effect>) {
        let uid = info.uid;
        self.rows.insert(uid, info);
        self.tables
            .entry(key.clone())
            .or_insert_with(Table::new)
            .uids
            .insert(uid);
        out.push(Effect::Stored { uid });
    }

    fn on_sync(&mut self, key: Key, reply: Result<sym::Reply, Fault>, out: &mut Vec<Effect>) {
        let Some(table) = self.tables.get(&key) else {
            return;
        };
        if !matches!(table.op, Op::Sync) {
            return;
        }
        match reply.and_then(revisions_of) {
            Ok(remote) => {
                let rev = remote.get(&key).copied().unwrap_or(0);
                if table.confirmed == Some(rev) {
                    self.settle(&key, out);
                } else {
                    self.start_pass(&key, Why::Sync, out);
                }
            }
            Err(fault) => {
                out.push(Effect::Log(format!("symclient: sync {key}: {fault}")));
                self.settle(&key, out);
            }
        }
    }

    fn start_pass(&mut self, key: &Key, why: Why, out: &mut Vec<Effect>) {
        let Some(table) = self.tables.get_mut(key) else {
            return;
        };
        table.op = Op::Pass(Pass {
            why,
            attempt: 1,
            rev1: 0,
            uids: Vec::new(),
            next: 0,
            dirty: false,
        });
        self.send_rev(key, true, out);
    }

    fn send_rev(&mut self, key: &Key, first: bool, out: &mut Vec<Effect>) {
        self.send_table(
            key,
            Wait::Rev {
                key: key.clone(),
                first,
            },
            request::Req::Revisions(sym::RevisionsRequest {}),
            out,
        );
    }

    fn on_rev(
        &mut self,
        key: Key,
        first: bool,
        reply: Result<sym::Reply, Fault>,
        out: &mut Vec<Effect>,
    ) {
        let rev = match reply.and_then(revisions_of) {
            Ok(remote) => remote.get(&key).copied().unwrap_or(0),
            Err(fault) => {
                self.fail_pass(&key, &fault.to_string(), out);
                return;
            }
        };
        let Some(table) = self.tables.get_mut(&key) else {
            return;
        };
        let Op::Pass(pass) = &mut table.op else {
            return;
        };
        if first {
            pass.rev1 = rev;
            pass.uids = table.uids.iter().copied().collect();
            pass.next = 0;
            pass.dirty = false;
            self.next_row(&key, out);
            return;
        }
        if pass.rev1 == rev && !pass.dirty {
            table.confirmed = Some(rev);
            self.settle(&key, out);
            return;
        }
        if pass.attempt < REFRESH_TRIES {
            pass.attempt += 1;
            self.send_rev(&key, true, out);
            return;
        }
        let message = if pass.rev1 == rev {
            format!("symclient: {key}: refresh did not settle")
        } else {
            format!(
                "symclient: revision moved during refresh: {key} from {} to {rev}",
                pass.rev1
            )
        };
        self.fail_pass(&key, &message, out);
    }

    fn next_row(&mut self, key: &Key, out: &mut Vec<Effect>) {
        let Some(Op::Pass(pass)) = self.tables.get(key).map(|table| &table.op) else {
            return;
        };
        let next = pass.uids.get(pass.next).copied();
        match next {
            Some(uid) => self.send_row(key, uid, false, out),
            None => self.send_rev(key, false, out),
        }
    }

    fn send_row(&mut self, key: &Key, uid: u32, inactive: bool, out: &mut Vec<Effect>) {
        self.send_table(
            key,
            Wait::Row {
                key: key.clone(),
                uid,
                inactive,
            },
            request::Req::GetByUid(sym::GetByUidRequest {
                uid,
                include_inactive: inactive,
            }),
            out,
        );
    }

    /// One refetched row. Active `not_found` means the row moved to the
    /// terminal table: ask again with `include_inactive` (E08-D026).
    fn on_row(
        &mut self,
        key: Key,
        uid: u32,
        inactive: bool,
        reply: Result<sym::Reply, Fault>,
        out: &mut Vec<Effect>,
    ) {
        let info = match reply.and_then(info_of) {
            Ok(info) => info,
            Err(Fault::NotFound(_)) if !inactive => {
                self.send_row(&key, uid, true, out);
                return;
            }
            Err(fault) => {
                self.fail_pass(&key, &fault.to_string(), out);
                return;
            }
        };
        if info.uid != uid {
            let message = format!("symclient: get uid {uid} returned {}", info.uid);
            self.fail_pass(&key, &message, out);
            return;
        }
        if Key::of(&info.ticker) != key {
            let message = format!("symclient: uid {uid} is {}, not {key}", info.ticker);
            self.fail_pass(&key, &message, out);
            return;
        }
        self.store(&key, info, out);
        if let Some(Op::Pass(pass)) = self.tables.get_mut(&key).map(|table| &mut table.op) {
            pass.next += 1;
        }
        self.next_row(&key, out);
    }

    fn fail_pass(&mut self, key: &Key, message: &str, out: &mut Vec<Effect>) {
        let Some(Op::Pass(pass)) = self.tables.get(key).map(|table| &table.op) else {
            return;
        };
        let line = match pass.why {
            Why::Sync => format!("symclient: sync {key}: {message}"),
            Why::Notify => format!("symclient: reload {key}: {message}"),
            Why::Check => format!("symclient: revision check: {message}"),
        };
        out.push(Effect::Log(line));
        self.settle(key, out);
    }

    /// The table's sync or pass is over. Finish the resolves waiting on it,
    /// then run one more pass if a notification or a check asked during it.
    fn settle(&mut self, key: &Key, out: &mut Vec<Effect>) {
        let Some(table) = self.tables.get_mut(key) else {
            return;
        };
        table.op = Op::Idle;
        let waiters = std::mem::take(&mut table.waiters);
        let again = std::mem::take(&mut table.again);
        let want = table.want.take();
        let behind = want.is_some_and(|want| table.confirmed.is_none_or(|rev| want > rev));
        for id in waiters {
            let Some(Stage::Settling { uid }) = self.resolves.remove(&id) else {
                continue;
            };
            let result = self.rows.get(&uid).cloned().ok_or(Error::UnknownUid);
            out.push(Effect::Resolved { id, result });
        }
        if behind {
            self.start_pass(key, Why::Notify, out);
        } else if again {
            self.start_pass(key, Why::Check, out);
        }
    }

    /// Compare every tracked table with SYM's revisions. One check at a
    /// time; a request during one runs one more after it.
    fn check(&mut self, out: &mut Vec<Effect>) {
        if !self.tables.values().any(|table| !table.uids.is_empty()) {
            return;
        }
        if self.checking {
            self.check_again = true;
            return;
        }
        self.checking = true;
        self.send(
            Wait::Check,
            request::Req::Revisions(sym::RevisionsRequest {}),
            None,
            out,
        );
    }

    fn on_check(&mut self, reply: Result<sym::Reply, Fault>, out: &mut Vec<Effect>) {
        self.checking = false;
        match reply.and_then(revisions_of) {
            Ok(remote) => {
                let behind: Vec<Key> = self
                    .tables
                    .iter()
                    .filter(|(key, table)| {
                        !table.uids.is_empty()
                            && table.confirmed != Some(remote.get(*key).copied().unwrap_or(0))
                    })
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in behind {
                    let Some(table) = self.tables.get_mut(&key) else {
                        continue;
                    };
                    if matches!(table.op, Op::Idle) {
                        self.start_pass(&key, Why::Check, out);
                    } else {
                        table.again = true;
                    }
                }
            }
            Err(fault) => out.push(Effect::Log(format!("symclient: revision check: {fault}"))),
        }
        if std::mem::take(&mut self.check_again) {
            self.check(out);
        }
    }
}

/// The reply payload: a natsx error reply, or a `sym.v1.Reply` whose `error`
/// and `reason` are empty.
fn decode(outcome: Outcome<'_>) -> Result<sym::Reply, Fault> {
    let body = match outcome {
        Outcome::Failed(text) => {
            return Err(Fault::Other(format!(
                "symclient: request {SUBJECT}: {text}"
            )));
        }
        Outcome::Body(body) => body,
    };
    if body.first() == Some(&0) {
        let (code, message) = reply_error(body).map_err(Fault::Other)?;
        let not_found = message == "not_found" || message.starts_with("not_found:");
        if code == CODE_HANDLER && not_found {
            return Err(Fault::NotFound(message));
        }
        return Err(Fault::Other(format!("symclient: {message}")));
    }
    let reply =
        sym::Reply::decode(body).map_err(|err| Fault::Other(format!("symclient: reply: {err}")))?;
    if !reply.error.is_empty() || !reply.reason.is_empty() {
        return Err(Fault::Other(format!(
            "symclient: reply error field {:?}",
            reply.error
        )));
    }
    Ok(reply)
}

/// natsx error reply: `0x00`, version 1, code `u16`, length `u16`, UTF-8 text,
/// big-endian, exactly `6 + length` bytes.
fn reply_error(body: &[u8]) -> Result<(u16, String), String> {
    if body.len() < 6 || body[0] != 0 {
        return Err("symclient: short error reply".to_owned());
    }
    if body[1] != 1 {
        return Err(format!("symclient: error reply version {}", body[1]));
    }
    let length = usize::from(u16::from_be_bytes([body[4], body[5]]));
    if 6 + length != body.len() {
        return Err("symclient: error reply length".to_owned());
    }
    let message = std::str::from_utf8(&body[6..])
        .map_err(|_| "symclient: error reply is not utf-8".to_owned())?;
    Ok((u16::from_be_bytes([body[2], body[3]]), message.to_owned()))
}

fn info_of(reply: sym::Reply) -> Result<Info, Fault> {
    match reply.result {
        Some(reply::Result::Info(msg)) => info_from_proto(msg).map_err(Fault::Other),
        _ => Err(Fault::Other("symclient: empty info".to_owned())),
    }
}

/// Every table's revision. A table SYM did not list is revision 0. A
/// category this client cannot track is skipped.
fn revisions_of(reply: sym::Reply) -> Result<HashMap<Key, i64>, Fault> {
    let Some(reply::Result::Revisions(list)) = reply.result else {
        return Err(Fault::Other("symclient: revisions: empty reply".to_owned()));
    };
    let mut out = HashMap::with_capacity(list.tables.len());
    for row in list.tables {
        if row.revision < 0 {
            return Err(Fault::Other(format!(
                "symclient: revision {} is negative",
                row.revision
            )));
        }
        let Some(category) = Category::from_name(&row.category) else {
            continue;
        };
        out.insert(
            Key {
                venue: row.venue,
                category,
            },
            row.revision,
        );
    }
    Ok(out)
}

/// `SymbolInfo` to [`Info`], with the Go SDK's checks: a canonical ticker, a
/// valid UID, a decodable status, and no empty filter name. An unset bound
/// is NaN (E00-D008).
fn info_from_proto(msg: sym::SymbolInfo) -> Result<Info, String> {
    let ticker = UniversalTicker::parse(&msg.universal_ticker)
        .map_err(|err| format!("symclient: ticker {:?}: {err}", msg.universal_ticker))?;
    if !valid_uid(msg.uid) {
        return Err(format!("symclient: uid {} is not a valid uid", msg.uid));
    }
    let status = status_from_proto(msg.status)?;
    let mut filters = Filters::new();
    for (name, bound) in msg.filters {
        if name.is_empty() {
            return Err("symclient: filter name is empty".to_owned());
        }
        filters.insert(name, bound.value.unwrap_or(f64::NAN));
    }
    Ok(Info {
        uid: msg.uid,
        ticker,
        exch_ticker: msg.exch_ticker,
        base: msg.base,
        quote: msg.quote,
        contract_size: msg.contract_size.unwrap_or(0.0),
        contract_value_ccy: msg.contract_value_ccy,
        price_ccy: msg.price_ccy,
        settle: msg.settle.unwrap_or_default(),
        expiry_ns: msg.expiry_ns,
        strike: msg.strike.unwrap_or(0.0),
        option_type: msg.option_type,
        status,
        filters,
        updated_at_ns: msg.updated_at_ns,
    })
}

/// [`Info`] back to `SymbolInfo`, as Go's `InfoToProto`: the event log's
/// `resolve` frame carries it. A zero or empty optional field is unset, and
/// a NaN bound is a bound without a value.
pub fn info_to_proto(info: &Info) -> sym::SymbolInfo {
    let present = |value: f64| (value != 0.0).then_some(value);
    sym::SymbolInfo {
        uid: info.uid,
        universal_ticker: info.ticker.to_string(),
        exch_ticker: info.exch_ticker.clone(),
        base: info.base.clone(),
        quote: info.quote.clone(),
        contract_size: present(info.contract_size),
        contract_value_ccy: info.contract_value_ccy.clone(),
        price_ccy: info.price_ccy.clone(),
        settle: (!info.settle.is_empty()).then(|| info.settle.clone()),
        expiry_ns: info.expiry_ns,
        strike: present(info.strike),
        option_type: info.option_type.clone(),
        status: i32::from(status_to_proto(info.status)),
        filters: info
            .filters
            .iter()
            .map(|(name, &value)| {
                let value = (!value.is_nan()).then_some(value);
                (name.clone(), sym::Bound { value })
            })
            .collect(),
        updated_at_ns: info.updated_at_ns,
    }
}

fn status_to_proto(status: Status) -> sym::SymbolStatus {
    match status {
        Status::Unspecified => sym::SymbolStatus::Unspecified,
        Status::Prelisting => sym::SymbolStatus::Prelisting,
        Status::Tradable => sym::SymbolStatus::Tradable,
        Status::Halted => sym::SymbolStatus::Halted,
        Status::Settling => sym::SymbolStatus::Settling,
        Status::Expired => sym::SymbolStatus::Expired,
        Status::Delisted => sym::SymbolStatus::Delisted,
        Status::Unknown => sym::SymbolStatus::Unknown,
    }
}

fn status_from_proto(value: i32) -> Result<Status, String> {
    let status = match sym::SymbolStatus::try_from(value) {
        Ok(sym::SymbolStatus::Prelisting) => Status::Prelisting,
        Ok(sym::SymbolStatus::Tradable) => Status::Tradable,
        Ok(sym::SymbolStatus::Halted) => Status::Halted,
        Ok(sym::SymbolStatus::Settling) => Status::Settling,
        Ok(sym::SymbolStatus::Expired) => Status::Expired,
        Ok(sym::SymbolStatus::Delisted) => Status::Delisted,
        Ok(sym::SymbolStatus::Unknown) => Status::Unknown,
        Ok(sym::SymbolStatus::Unspecified) => {
            return Err(
                "symclient: status \"SYMBOL_STATUS_UNSPECIFIED\" cannot be decoded".to_owned(),
            );
        }
        Err(_) => return Err(format!("symclient: status \"{value}\" cannot be decoded")),
    };
    Ok(status)
}

fn deadline_ns(now: i64, after: Duration) -> i64 {
    let after = i128::try_from(after.as_nanos()).unwrap_or(i128::MAX);
    let sum = i128::from(now).saturating_add(after);
    i64::try_from(sum).unwrap_or(if sum > 0 { i64::MAX } else { i64::MIN })
}
