//! Bindings, subscriptions, routing, and `md.view` / `md.fetch` reads.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::time::Duration;

use mftik_proto::wire::mftiko::common::v1 as common;
use mftik_proto::wire::mftiko::md::v1::{self as md, fetch_reply, fetch_request, view_reply};
use mftik_proto::wire::mftiko::stsctl::v1 as stsctl;
use prost::Message;

use super::event::{Hooks, TOPIC_BOOK};
use crate::dispatch::envelope;

const FEED_PREFIX: &str = "md.v1.f.";
const VIEW_PREFIX: &str = "md.v1.view.";
const FETCH_ROOT: &str = "md.v1.fetch.";

/// What [`Path::open`] needs. There is no NATS connection and no clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// `InitPayload.recv_window_ms`. Zero is rejected. Every `md.view` and
    /// `md.fetch` waits this long (E08-D048).
    pub recv_window: Duration,
    /// The strategy's market data hooks. Fixed for the session.
    pub hooks: Hooks,
}

/// Why [`Path::open`] refused the config.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenError {
    RecvWindow,
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecvWindow => f.write_str("mdpath: recv window must be positive"),
        }
    }
}

impl std::error::Error for OpenError {}

/// A refused snapshot or read. The texts match the Go SDK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The snapshot has selectors. Phase 1 does not resolve them (E08-D022, E08-D046).
    Selector,
    /// The feed key is not `topic.ticker`.
    FeedKey(String),
    EmptySubject,
    /// One feed key listed on two subjects.
    FeedOnTwoSubjects(String),
    /// One subject listed for two feed keys.
    SubjectForTwoFeeds(String),
    /// [`Path::close`] has been called.
    Stopping,
    /// [`Path::apply`] while an earlier snapshot is still staged.
    ApplyInProgress,
    /// [`Path::apply_bound`] with nothing staged.
    ApplyNotInProgress,
    /// The session has no subscribed feed for that ticker and topic.
    NoFeed,
    /// The caller passed an empty ticker.
    UnknownUid,
    /// The feed subject has no `md.v1.f.` prefix, so `md.view` cannot be named.
    BadSubject,
    /// The venue is not one subject token (E08-D019).
    BadVenue(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Selector => f.write_str("mdpath: selectors are not supported"),
            Self::FeedKey(key) => write!(f, "mdpath: feed {key:?} is not topic.ticker"),
            Self::EmptySubject => f.write_str("mdpath: empty subject"),
            Self::FeedOnTwoSubjects(key) => write!(f, "mdpath: feed {key} is on two subjects"),
            Self::SubjectForTwoFeeds(subject) => {
                write!(f, "mdpath: subject {subject} is listed for two feeds")
            }
            Self::Stopping => f.write_str("mdpath: the session is stopping"),
            Self::ApplyInProgress => f.write_str("mdpath: a bindings apply is in progress"),
            Self::ApplyNotInProgress => f.write_str("mdpath: no bindings apply is in progress"),
            Self::NoFeed => f.write_str("mdpath: the session has no such feed"),
            Self::UnknownUid => f.write_str("mdpath: the instrument is not known to this session"),
            Self::BadSubject => f.write_str("mdpath: feed subject has no md.v1.f. prefix"),
            Self::BadVenue(venue) => write!(f, "mdpath: venue {venue:?} is not a subject token"),
        }
    }
}

impl std::error::Error for Error {}

/// One subscription. Two ids can name the same subject while a feed key on it
/// is replaced. Messages from that subscription go to [`Path::route`] with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubId(pub(super) u64);

/// A subscription change for E03. The subject is `FeedBinding.subject` as
/// given (E01-D006).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Subscribe { sub: SubId, subject: String },
    Unsubscribe { sub: SubId, subject: String },
}

/// What [`Path::apply`] decided.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use = "subscribe, flush, then apply_bound or apply_aborted"]
pub enum Apply {
    /// The version is not newer than `bindings_applied`. This is success with
    /// nothing to do; C4 still sends `Applied` for `Live` (PROTOCOL §9.2.9).
    Unchanged,
    /// The version is newer. Perform each [`Effect::Subscribe`], flush when
    /// there was one, then call [`Path::apply_bound`]. On a failure call
    /// [`Path::apply_aborted`]. The list may be empty.
    Staged(Vec<Effect>),
}

/// Where one `md.f` message from a subscription goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// [`crate::dispatch::Dispatcher::push_md`]. A bad envelope also goes here so
    /// the dispatcher drops it, counts it, and logs it (E08-D049).
    Md,
    /// [`crate::dispatch::Dispatcher::push_book`], for [`crate::bookpath`].
    Book,
    /// Not delivered: the subscription left the set, or the topic has no hook (E08-D005).
    Skip,
}

/// One page of a feed's tape (E04-D036). A limit of zero is read by MD as
/// 1000. `before` of `None` asks for the newest page. `since_ns` of zero sets
/// no time bound.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TapeQuery {
    pub limit: u32,
    pub before: Option<md::Cursor>,
    pub since_ns: i64,
}

/// Identity of one [`Request`]. Pass it back to [`Path::on_reply`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReadTicket(pub(super) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReadKind {
    Tape,
    BookSnapshot,
    Klines,
    OpenInterest,
}

/// One NATS request for E03. `body` is the encoded `ViewRequest` or
/// `FetchRequest`. `wait` is `recv_window` (E08-D048).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub ticket: ReadTicket,
    pub kind: ReadKind,
    pub subject: String,
    pub body: Vec<u8>,
    pub wait: Duration,
}

/// How a request ended without a reply body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestError {
    /// `wait` ran out.
    Timeout,
    /// The subject had no responder.
    NoResponder,
    /// Any other request failure. The text is kept.
    Other(String),
}

/// Which request a [`ReadError`] belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Via {
    View,
    Fetch,
}

impl fmt::Display for Via {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::View => f.write_str("md.view"),
            Self::Fetch => f.write_str("md.fetch"),
        }
    }
}

/// A read that ended without a result. The texts match the Go SDK, including
/// the Go wording for a timeout and a missing responder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadError {
    Timeout {
        via: Via,
    },
    NoResponder {
        via: Via,
    },
    Request {
        via: Via,
        message: String,
    },
    /// The reply body did not decode. The text is prost's.
    Decode {
        via: Via,
        message: String,
    },
    /// The reply's `error`. `reason` is `FetchReply.reason`, empty on `md.view`.
    Reply {
        via: Via,
        error: String,
        reason: String,
    },
    /// No error and no result of the asked kind.
    Empty {
        via: Via,
        what: &'static str,
    },
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout { via } => write!(f, "mdpath: {via}: context deadline exceeded"),
            Self::NoResponder { via } => {
                write!(
                    f,
                    "mdpath: {via}: nats: no responders available for request"
                )
            }
            Self::Request { via, message } | Self::Decode { via, message } => {
                write!(f, "mdpath: {via}: {message}")
            }
            Self::Reply { via, error, reason } if reason.is_empty() => {
                write!(f, "mdpath: {via}: {error}")
            }
            Self::Reply { via, error, reason } => write!(f, "mdpath: {via}: {error}: {reason}"),
            Self::Empty { via, what } => write!(f, "mdpath: {via}: empty {what}"),
        }
    }
}

impl std::error::Error for ReadError {}

/// The result handed to the read's callback on the TD buffer.
#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    Tape(Result<md::TapePage, ReadError>),
    BookSnapshot(Result<md::BookSnapshot, ReadError>),
    Klines(Result<md::Klines, ReadError>),
    OpenInterest(Result<md::OpenInterest, ReadError>),
}

/// One finished read. Push it on the TD buffer and run the callback that
/// owns `ticket` there (E08-D048). It owns its data.
#[derive(Clone, Debug, PartialEq)]
pub struct Delivery {
    pub ticket: ReadTicket,
    pub reply: Reply,
}

/// The `md.view` subject of a feed subject: a leading `md.v1.f.` becomes
/// `md.v1.view.`, and the rest is copied as given (E04-D055). `None` when the
/// prefix is missing or nothing follows it.
pub fn view_subject(feed_subject: &str) -> Option<String> {
    match feed_subject.strip_prefix(FEED_PREFIX) {
        Some(rest) if !rest.is_empty() => Some(format!("{VIEW_PREFIX}{rest}")),
        _ => None,
    }
}

/// `md.v1.fetch.{venue}` (E04-D056). The venue is one subject token: not
/// empty, and no `.`, `*`, `>`, or whitespace (E08-D019).
pub fn fetch_subject(venue: &str) -> Result<String, Error> {
    if !is_token(venue) {
        return Err(Error::BadVenue(venue.to_owned()));
    }
    Ok(format!("{FETCH_ROOT}{venue}"))
}

#[derive(Clone, Debug)]
struct Feed {
    sub: SubId,
    key: String,
    topic: String,
    view: Option<String>,
}

#[derive(Debug)]
struct Spec {
    key: String,
    topic: String,
    view: Option<String>,
}

#[derive(Debug)]
struct Stage {
    version: (i64, u64),
    next: BTreeMap<String, Feed>,
    add: Vec<(String, SubId)>,
    drop: Vec<(String, SubId)>,
}

/// One session's market data path.
///
/// The subscribed set changes only in [`Path::apply_bound`]. Reads use that
/// set. A staged snapshot's new subscriptions already route.
#[derive(Debug)]
pub struct Path {
    recv: Duration,
    hooks: Hooks,
    by_subject: BTreeMap<String, Feed>,
    by_key: HashMap<String, String>,
    live: HashMap<SubId, Route>,
    stage: Option<Stage>,
    applied: Option<(i64, u64)>,
    closed: bool,
    next_sub: u64,
    next_ticket: u64,
    reads: HashMap<ReadTicket, ReadKind>,
}

impl Path {
    /// Empty path. It does not subscribe; apply the start bindings.
    pub fn open(config: Config) -> Result<Self, OpenError> {
        if config.recv_window.is_zero() {
            return Err(OpenError::RecvWindow);
        }
        Ok(Self {
            recv: config.recv_window,
            hooks: config.hooks,
            by_subject: BTreeMap::new(),
            by_key: HashMap::new(),
            live: HashMap::new(),
            stage: None,
            applied: None,
            closed: false,
            next_sub: 0,
            next_ticket: 0,
            reads: HashMap::new(),
        })
    }

    pub fn recv_window(&self) -> Duration {
        self.recv
    }

    /// The hooks from [`Config`]. Copy them into the dispatcher's feed
    /// callback and call [`Hooks::decode`] there.
    pub fn hooks(&self) -> Hooks {
        self.hooks
    }

    /// `Report.bindings_applied` (PROTOCOL §9.2.4). `None` before the first
    /// [`Path::apply_bound`]. A `Bindings` without a version is epoch 0, seq 0.
    pub fn bindings_applied(&self) -> Option<common::Version> {
        self.applied
            .map(|(epoch, seq)| common::Version { epoch, seq })
    }

    /// The subscribed subjects, sorted.
    pub fn subjects(&self) -> Vec<String> {
        self.by_subject.keys().cloned().collect()
    }

    /// Stage one `StartPayload.bindings`, `Command.bindings`, or `Live`
    /// snapshot (PROTOCOL §9.3.2, §9.3.5, E08-D046).
    ///
    /// Selectors are refused first, whatever the version. Bad feeds are
    /// refused next. Then only a version strictly newer than
    /// `bindings_applied` is staged, comparing epoch and then seq. The first
    /// snapshot is staged even at version 0. The set is the subjects listed:
    /// a subject not held is subscribed; one that left, or whose feed key
    /// changed, is unsubscribed in [`Path::apply_bound`]. `FeedBinding.state`,
    /// `md_instance`, and the accounts are not read.
    ///
    /// Do not call this again before [`Path::apply_bound`] or
    /// [`Path::apply_aborted`].
    pub fn apply(&mut self, bindings: &stsctl::Bindings) -> Result<Apply, Error> {
        if !bindings.selectors.is_empty() {
            return Err(Error::Selector);
        }
        let next = index_feeds(&bindings.feeds)?;
        if self.closed {
            return Err(Error::Stopping);
        }
        if self.stage.is_some() {
            return Err(Error::ApplyInProgress);
        }
        let version = bindings
            .version
            .as_ref()
            .map_or((0, 0), |version| (version.epoch, version.seq));
        if let Some(held) = self.applied
            && !newer(held, version)
        {
            return Ok(Apply::Unchanged);
        }
        let mut merged = BTreeMap::new();
        let mut add = Vec::new();
        let mut drop = Vec::new();
        for (subject, spec) in next {
            if let Some(old) = self.by_subject.get(&subject)
                && old.key == spec.key
                && old.topic == spec.topic
                && old.view == spec.view
            {
                merged.insert(subject, old.clone());
                continue;
            }
            self.next_sub += 1;
            let sub = SubId(self.next_sub);
            self.live.insert(sub, route_of(&self.hooks, &spec.topic));
            if let Some(old) = self.by_subject.get(&subject) {
                drop.push((subject.clone(), old.sub));
            }
            add.push((subject.clone(), sub));
            merged.insert(
                subject,
                Feed {
                    sub,
                    key: spec.key,
                    topic: spec.topic,
                    view: spec.view,
                },
            );
        }
        for (subject, old) in &self.by_subject {
            if !merged.contains_key(subject) {
                drop.push((subject.clone(), old.sub));
            }
        }
        drop.sort();
        let effects = add
            .iter()
            .map(|(subject, sub)| Effect::Subscribe {
                sub: *sub,
                subject: subject.clone(),
            })
            .collect();
        self.stage = Some(Stage {
            version,
            next: merged,
            add,
            drop,
        });
        Ok(Apply::Staged(effects))
    }

    /// The staged subscribes have flushed. The set and `bindings_applied`
    /// change now, so a `Report` is due (PROTOCOL §9.2.4). Perform the
    /// returned unsubscribes.
    #[must_use = "unsubscribe the subjects that left the set"]
    pub fn apply_bound(&mut self) -> Result<Vec<Effect>, Error> {
        if self.closed {
            return Err(Error::Stopping);
        }
        let stage = self.stage.take().ok_or(Error::ApplyNotInProgress)?;
        for (_, sub) in &stage.drop {
            self.live.remove(sub);
        }
        self.by_key = stage
            .next
            .iter()
            .map(|(subject, feed)| (feed.key.clone(), subject.clone()))
            .collect();
        self.by_subject = stage.next;
        self.applied = Some(stage.version);
        Ok(unsubscribes(stage.drop))
    }

    /// A staged subscribe or the flush failed. The set and `bindings_applied`
    /// stay as they were. Unsubscribe the returned ids that did subscribe.
    #[must_use = "drop the subscriptions this attempt opened"]
    pub fn apply_aborted(&mut self) -> Vec<Effect> {
        let Some(stage) = self.stage.take() else {
            return Vec::new();
        };
        for (_, sub) in &stage.add {
            self.live.remove(sub);
        }
        unsubscribes(stage.add)
    }

    /// Where a message from subscription `sub` goes. Call it on the IO
    /// thread; it reads only the envelope (PROTOCOL §9.4.3).
    ///
    /// A subscription not in the set is skipped. A bad envelope is [`Route::Md`]
    /// so the dispatcher counts it. `orderbook_delta` is [`Route::Book`]. A
    /// topic without a hook is skipped before its payload is decoded.
    pub fn route(&self, sub: SubId, msg: &[u8]) -> Route {
        let Some(route) = self.live.get(&sub) else {
            return Route::Skip;
        };
        if envelope::md_ts_ns(msg).is_err() {
            return Route::Md;
        }
        *route
    }

    /// Ask `md.view` for one page of the tape of feed `topic.ticker`
    /// (E04-D036, E08-D047). `ticker` is SYM `Info` for the uid; this path
    /// does not ask SYM. Nothing is applied to the live stream.
    #[must_use = "send the request and pass the reply to on_reply"]
    pub fn tape(&mut self, ticker: &str, topic: &str, query: TapeQuery) -> Result<Request, Error> {
        let subject = self.view_of(ticker, topic)?;
        let body = md::ViewRequest {
            limit: query.limit,
            before: query.before,
            since_ns: (query.since_ns != 0).then_some(query.since_ns),
        }
        .encode_to_vec();
        Ok(self.start(ReadKind::Tape, subject, body))
    }

    /// Ask `md.view` for the windowed book of feed `orderbook_delta.ticker`.
    /// A `not_synced` reply is an error; this call does not retry.
    #[must_use = "send the request and pass the reply to on_reply"]
    pub fn book_snapshot(&mut self, ticker: &str) -> Result<Request, Error> {
        let subject = self.view_of(ticker, TOPIC_BOOK)?;
        let body = md::ViewRequest::default().encode_to_vec();
        Ok(self.start(ReadKind::BookSnapshot, subject, body))
    }

    /// Ask `md.fetch` for the latest `limit` closed klines (E04-D019,
    /// E04-D056). `venue` is SYM `Info` for `uid`.
    #[must_use = "send the request and pass the reply to on_reply"]
    pub fn fetch_klines(
        &mut self,
        uid: u32,
        venue: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Request, Error> {
        let subject = self.fetch_of(venue)?;
        let body = md::FetchRequest {
            req: Some(fetch_request::Req::Klines(md::FetchKlines {
                interval: interval.to_owned(),
                limit,
                uid,
            })),
        }
        .encode_to_vec();
        Ok(self.start(ReadKind::Klines, subject, body))
    }

    /// Ask `md.fetch` for the open interest the exchange does not push.
    #[must_use = "send the request and pass the reply to on_reply"]
    pub fn fetch_open_interest(&mut self, uid: u32, venue: &str) -> Result<Request, Error> {
        let subject = self.fetch_of(venue)?;
        let body = md::FetchRequest {
            req: Some(fetch_request::Req::OpenInterest(md::FetchOpenInterest {
                uid,
            })),
        }
        .encode_to_vec();
        Ok(self.start(ReadKind::OpenInterest, subject, body))
    }

    /// The request for `ticket` ended. A ticket that is unknown, already
    /// answered, or dropped by [`Path::close`] returns `None`.
    #[must_use = "push the delivery on the TD buffer"]
    pub fn on_reply(
        &mut self,
        ticket: ReadTicket,
        reply: Result<&[u8], RequestError>,
    ) -> Option<Delivery> {
        let kind = self.reads.remove(&ticket)?;
        let reply = match kind {
            ReadKind::Tape => Reply::Tape(view_reply(reply).and_then(|rep| match rep.result {
                Some(view_reply::Result::Tape(page)) => Ok(page),
                _ => Err(empty(Via::View, "tape")),
            })),
            ReadKind::BookSnapshot => {
                Reply::BookSnapshot(view_reply(reply).and_then(|rep| match rep.result {
                    Some(view_reply::Result::Book(book)) => Ok(book),
                    _ => Err(empty(Via::View, "book")),
                }))
            }
            ReadKind::Klines => {
                Reply::Klines(fetch_reply(reply).and_then(|rep| match rep.result {
                    Some(fetch_reply::Result::Klines(klines)) => Ok(klines),
                    _ => Err(empty(Via::Fetch, "klines")),
                }))
            }
            ReadKind::OpenInterest => {
                Reply::OpenInterest(fetch_reply(reply).and_then(|rep| match rep.result {
                    Some(fetch_reply::Result::OpenInterest(oi)) => Ok(oi),
                    _ => Err(empty(Via::Fetch, "open interest")),
                }))
            }
        };
        Some(Delivery { ticket, reply })
    }

    /// Stop the path. Every subscription, staged ones included, is
    /// unsubscribed. In-flight reads are dropped. `bindings_applied` keeps its
    /// last value. A second call returns nothing.
    #[must_use = "unsubscribe every feed"]
    pub fn close(&mut self) -> Vec<Effect> {
        if self.closed {
            return Vec::new();
        }
        self.closed = true;
        self.reads.clear();
        self.live.clear();
        self.by_key.clear();
        let mut subs: Vec<(String, SubId)> = std::mem::take(&mut self.by_subject)
            .into_iter()
            .map(|(subject, feed)| (subject, feed.sub))
            .collect();
        if let Some(stage) = self.stage.take() {
            subs.extend(stage.add);
        }
        subs.sort();
        unsubscribes(subs)
    }

    fn view_of(&self, ticker: &str, topic: &str) -> Result<String, Error> {
        if topic.is_empty() {
            return Err(Error::NoFeed);
        }
        if ticker.is_empty() {
            return Err(Error::UnknownUid);
        }
        if self.closed {
            return Err(Error::Stopping);
        }
        let key = format!("{topic}.{ticker}");
        let feed = self
            .by_key
            .get(&key)
            .and_then(|subject| self.by_subject.get(subject))
            .ok_or(Error::NoFeed)?;
        feed.view.clone().ok_or(Error::BadSubject)
    }

    fn fetch_of(&self, venue: &str) -> Result<String, Error> {
        let subject = fetch_subject(venue)?;
        if self.closed {
            return Err(Error::Stopping);
        }
        Ok(subject)
    }

    fn start(&mut self, kind: ReadKind, subject: String, body: Vec<u8>) -> Request {
        self.next_ticket += 1;
        let ticket = ReadTicket(self.next_ticket);
        self.reads.insert(ticket, kind);
        Request {
            ticket,
            kind,
            subject,
            body,
            wait: self.recv,
        }
    }
}

fn index_feeds(feeds: &[stsctl::FeedBinding]) -> Result<BTreeMap<String, Spec>, Error> {
    let mut out: BTreeMap<String, Spec> = BTreeMap::new();
    let mut key_subject: HashMap<&str, &str> = HashMap::with_capacity(feeds.len());
    for binding in feeds {
        let spec = parse_feed(binding)?;
        let subject = binding.subject.as_str();
        if let Some(prev) = key_subject.get(binding.feed.as_str())
            && *prev != subject
        {
            return Err(Error::FeedOnTwoSubjects(binding.feed.clone()));
        }
        key_subject.insert(binding.feed.as_str(), subject);
        if let Some(prev) = out.get(subject) {
            if prev.key != spec.key {
                return Err(Error::SubjectForTwoFeeds(subject.to_owned()));
            }
            continue;
        }
        out.insert(subject.to_owned(), spec);
    }
    Ok(out)
}

fn parse_feed(binding: &stsctl::FeedBinding) -> Result<Spec, Error> {
    let key = binding.feed.as_str();
    let Some((topic, ticker)) = key.split_once('.') else {
        return Err(Error::FeedKey(key.to_owned()));
    };
    if topic.is_empty() || ticker.is_empty() {
        return Err(Error::FeedKey(key.to_owned()));
    }
    if binding.subject.is_empty() {
        return Err(Error::EmptySubject);
    }
    Ok(Spec {
        key: key.to_owned(),
        topic: topic.to_owned(),
        view: view_subject(&binding.subject),
    })
}

fn route_of(hooks: &Hooks, topic: &str) -> Route {
    if topic == TOPIC_BOOK {
        Route::Book
    } else if hooks.wants(topic) {
        Route::Md
    } else {
        Route::Skip
    }
}

/// `next` is strictly after `held`: epoch first, then seq.
fn newer(held: (i64, u64), next: (i64, u64)) -> bool {
    if next.0 != held.0 {
        return next.0 > held.0;
    }
    next.1 > held.1
}

fn is_token(value: &str) -> bool {
    !value.is_empty()
        && !value
            .chars()
            .any(|c| matches!(c, '.' | '*' | '>' | ' ' | '\t' | '\n' | '\r'))
}

fn unsubscribes(subs: Vec<(String, SubId)>) -> Vec<Effect> {
    subs.into_iter()
        .map(|(subject, sub)| Effect::Unsubscribe { sub, subject })
        .collect()
}

fn request_error(via: Via, err: RequestError) -> ReadError {
    match err {
        RequestError::Timeout => ReadError::Timeout { via },
        RequestError::NoResponder => ReadError::NoResponder { via },
        RequestError::Other(message) => ReadError::Request { via, message },
    }
}

fn empty(via: Via, what: &'static str) -> ReadError {
    ReadError::Empty { via, what }
}

fn view_reply(reply: Result<&[u8], RequestError>) -> Result<md::ViewReply, ReadError> {
    let body = reply.map_err(|err| request_error(Via::View, err))?;
    let rep = md::ViewReply::decode(body).map_err(|err| ReadError::Decode {
        via: Via::View,
        message: err.to_string(),
    })?;
    if !rep.error.is_empty() {
        return Err(ReadError::Reply {
            via: Via::View,
            error: rep.error,
            reason: String::new(),
        });
    }
    Ok(rep)
}

fn fetch_reply(reply: Result<&[u8], RequestError>) -> Result<md::FetchReply, ReadError> {
    let body = reply.map_err(|err| request_error(Via::Fetch, err))?;
    let rep = md::FetchReply::decode(body).map_err(|err| ReadError::Decode {
        via: Via::Fetch,
        message: err.to_string(),
    })?;
    if !rep.error.is_empty() {
        return Err(ReadError::Reply {
            via: Via::Fetch,
            error: rep.error,
            reason: rep.reason,
        });
    }
    Ok(rep)
}
