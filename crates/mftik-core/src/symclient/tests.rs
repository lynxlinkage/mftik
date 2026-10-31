//! SYM client against an in-memory SYM, driven by an injected clock. Nothing
//! sleeps. The cases follow PROTOCOL §9.4 item 8 and §7.3: the resolve window,
//! memory-only `info`, notification, revision check, reconnect, and the
//! terminal table.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use mftik_proto::wire::mftiko::sym::v1::{self as sym, reply, request};
use prost::Message;

use super::{
    CHECK_INTERVAL, Client, Config, Effect, Error, Fault, OpenError, Outcome, REFRESH_TRIES,
    REQUEST_TIMEOUT, ResolveId, Resolving, SUBJECT, Ticket, TimerToken, changed_subject, decode,
    info_from_proto, info_to_proto,
};
use crate::bookpath;
use crate::symbol::{Category, PRICE_TICK, Status};

const NOW: i64 = 1_000_000_000_000;
const INTERVAL_NS: i64 = 300_000_000_000;
const BTC: u32 = 1001;
const ETH: u32 = 1002;
const ADA: u32 = 1003;
const PERP: u32 = 2001;
const BTC_T: &str = "Bybit_Spot_BTCUSDT";
const ETH_T: &str = "Bybit_Spot_ETHUSDT";
const PERP_T: &str = "Bybit_Perp_BTCUSDT";
const SPOT_SUB: &str = "sym.v1.changed.Bybit.Spot";
const PERP_SUB: &str = "sym.v1.changed.Bybit.Perp";

#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Get(String),
    ByUid(u32, bool),
    Rev,
}

fn by(uid: u32) -> Call {
    Call::ByUid(uid, false)
}

fn get(ticker: &str) -> Call {
    Call::Get(ticker.to_owned())
}

fn call_of(msg: &sym::Request) -> Call {
    match msg.req.as_ref().expect("request is set") {
        request::Req::Get(get) => {
            assert!(!get.include_inactive, "resolve set include_inactive");
            Call::Get(get.universal_ticker.clone())
        }
        request::Req::GetByUid(by) => Call::ByUid(by.uid, by.include_inactive),
        request::Req::Revisions(_) => Call::Rev,
        other => panic!("unexpected request {other:?}"),
    }
}

fn listing(uid: u32, ticker: &str, tick: f64) -> sym::SymbolInfo {
    let mut filters = HashMap::new();
    filters.insert(PRICE_TICK.to_owned(), sym::Bound { value: Some(tick) });
    sym::SymbolInfo {
        universal_ticker: ticker.to_owned(),
        exch_ticker: ticker.rsplit('_').next().unwrap_or_default().to_owned(),
        base: "BTC".to_owned(),
        quote: "USDT".to_owned(),
        status: sym::SymbolStatus::Tradable as i32,
        filters,
        updated_at_ns: 10,
        uid,
        contract_value_ccy: "BTC".to_owned(),
        price_ccy: "USDT".to_owned(),
        ..Default::default()
    }
}

fn info_body(info: sym::SymbolInfo) -> Vec<u8> {
    sym::Reply {
        result: Some(reply::Result::Info(info)),
        ..Default::default()
    }
    .encode_to_vec()
}

fn revisions_body(revs: &BTreeMap<(String, String), i64>) -> Vec<u8> {
    let tables = revs
        .iter()
        .map(|((venue, category), revision)| sym::TableRevision {
            venue: venue.clone(),
            category: category.clone(),
            revision: *revision,
        })
        .collect();
    sym::Reply {
        result: Some(reply::Result::Revisions(sym::RevisionList { tables })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// natsx error reply framing.
fn error_body(code: u16, message: &str) -> Vec<u8> {
    let mut out = vec![0, 1];
    out.extend_from_slice(&code.to_be_bytes());
    out.extend_from_slice(&u16::try_from(message.len()).unwrap().to_be_bytes());
    out.extend_from_slice(message.as_bytes());
    out
}

fn changed(venue: &str, category: &str, revision: i64) -> Vec<u8> {
    sym::Changed {
        venue: venue.to_owned(),
        category: category.to_owned(),
        revision,
    }
    .encode_to_vec()
}

fn table_of(ticker: &str) -> (String, String) {
    let mut parts = ticker.split('_');
    let venue = parts.next().unwrap_or_default().to_owned();
    let category = parts.next().unwrap_or_default().to_owned();
    (venue, category)
}

/// SYM's two tables and the per-table revisions.
#[derive(Default)]
struct Sym {
    active: BTreeMap<u32, sym::SymbolInfo>,
    terminal: BTreeMap<u32, sym::SymbolInfo>,
    revs: BTreeMap<(String, String), i64>,
    /// Each `GetByUid` answer first commits this many more changes, one each.
    churn: u32,
}

impl Sym {
    fn seed(&mut self, info: sym::SymbolInfo) {
        self.revs
            .entry(table_of(&info.universal_ticker))
            .or_insert(1);
        self.terminal.remove(&info.uid);
        self.active.insert(info.uid, info);
    }

    fn set_tick(&mut self, uid: u32, tick: f64) {
        let row = self.active.get_mut(&uid).expect("active row");
        row.filters
            .insert(PRICE_TICK.to_owned(), sym::Bound { value: Some(tick) });
        row.updated_at_ns += 1;
    }

    fn bump(&mut self, ticker: &str) -> i64 {
        let rev = self.revs.entry(table_of(ticker)).or_insert(0);
        *rev += 1;
        *rev
    }

    fn delist(&mut self, uid: u32, tick: f64) -> i64 {
        let mut row = self.active.remove(&uid).expect("active row");
        row.status = sym::SymbolStatus::Delisted as i32;
        row.filters.clear();
        row.filters
            .insert(PRICE_TICK.to_owned(), sym::Bound { value: Some(tick) });
        row.updated_at_ns += 1;
        let ticker = row.universal_ticker.clone();
        self.terminal.insert(uid, row);
        self.bump(&ticker)
    }

    fn answer(&mut self, msg: &sym::Request) -> Vec<u8> {
        match msg.req.as_ref().expect("request is set") {
            request::Req::Get(get) => {
                let found = self
                    .active
                    .values()
                    .find(|row| row.universal_ticker == get.universal_ticker);
                match found {
                    Some(row) => info_body(row.clone()),
                    None => error_body(
                        2,
                        &format!(
                            "not_found: ticker {} not found in active table (include_inactive=false)",
                            get.universal_ticker
                        ),
                    ),
                }
            }
            request::Req::GetByUid(by) => {
                if self.churn > 0 {
                    let ticker = self
                        .active
                        .get(&by.uid)
                        .or_else(|| self.terminal.get(&by.uid))
                        .map(|row| row.universal_ticker.clone());
                    if let Some(ticker) = ticker {
                        self.churn -= 1;
                        self.bump(&ticker);
                    }
                }
                if let Some(row) = self.active.get(&by.uid) {
                    return info_body(row.clone());
                }
                if by.include_inactive {
                    if let Some(row) = self.terminal.get(&by.uid) {
                        return info_body(row.clone());
                    }
                    return error_body(
                        2,
                        &format!(
                            "not_found: uid {} not found in active or terminal table (include_inactive=true)",
                            by.uid
                        ),
                    );
                }
                error_body(
                    2,
                    &format!(
                        "not_found: uid {} not found in active table (include_inactive=false)",
                        by.uid
                    ),
                )
            }
            request::Req::Revisions(_) => revisions_body(&self.revs),
            other => panic!("unexpected request {other:?}"),
        }
    }
}

struct Held {
    ticket: Ticket,
    msg: sym::Request,
}

/// Performs effects the way a host would. Requests are answered at once by
/// [`Sym`] unless `hold` is set; then they wait in `held` for the test.
struct Harness {
    client: Client,
    sym: Sym,
    now: i64,
    hold: bool,
    hold_subs: bool,
    sub_fail: Option<String>,
    held: Vec<Held>,
    held_subs: Vec<(Ticket, String)>,
    calls: Vec<Call>,
    tags: Vec<Option<ResolveId>>,
    subjects: Vec<String>,
    unsubscribed: Vec<String>,
    stored: Vec<u32>,
    resolved: Vec<(ResolveId, Result<crate::symbol::Info, Error>)>,
    logs: Vec<String>,
    timers: Vec<(TimerToken, Duration, i64)>,
}

impl Harness {
    fn new() -> Self {
        Self {
            client: Client::open(Config::default()).expect("default config"),
            sym: Sym::default(),
            now: NOW,
            hold: false,
            hold_subs: false,
            sub_fail: None,
            held: Vec::new(),
            held_subs: Vec::new(),
            calls: Vec::new(),
            tags: Vec::new(),
            subjects: Vec::new(),
            unsubscribed: Vec::new(),
            stored: Vec::new(),
            resolved: Vec::new(),
            logs: Vec::new(),
            timers: Vec::new(),
        }
    }

    /// BTC and ETH on Bybit spot, BTC on Bybit perp.
    fn seeded() -> Self {
        let mut h = Self::new();
        h.sym.seed(listing(BTC, BTC_T, 0.1));
        h.sym.seed(listing(ETH, ETH_T, 0.2));
        h.sym.seed(listing(PERP, PERP_T, 0.5));
        h
    }

    fn run(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Request {
                    ticket,
                    request,
                    wait,
                    resolve,
                } => {
                    assert_eq!(wait, REQUEST_TIMEOUT);
                    self.calls.push(call_of(&request));
                    self.tags.push(resolve);
                    if self.hold {
                        self.held.push(Held {
                            ticket,
                            msg: request,
                        });
                        continue;
                    }
                    let body = self.sym.answer(&request);
                    let next = self.client.on_reply(self.now, ticket, Outcome::Body(&body));
                    self.run(next);
                }
                Effect::Subscribe { ticket, subject } => {
                    if self.hold_subs {
                        self.held_subs.push((ticket, subject));
                        continue;
                    }
                    let next = match self.sub_fail.take() {
                        Some(text) => self.client.on_subscribed(self.now, ticket, Err(&text)),
                        None => {
                            self.subjects.push(subject);
                            self.client.on_subscribed(self.now, ticket, Ok(()))
                        }
                    };
                    self.run(next);
                }
                Effect::Unsubscribe { subject } => self.unsubscribed.push(subject),
                Effect::ArmTimer {
                    token,
                    after,
                    deadline_ns,
                } => self.timers.push((token, after, deadline_ns)),
                Effect::Stored { uid } => self.stored.push(uid),
                Effect::Resolved { id, result } => self.resolved.push((id, result)),
                Effect::Log(line) => self.logs.push(line),
            }
        }
    }

    fn resolve(&mut self, ticker: &str) -> ResolveId {
        self.client.allow_resolve(true);
        let Resolving { id, effects } = self
            .client
            .resolve(self.now, ticker)
            .expect("resolve accepted");
        self.run(effects);
        id
    }

    fn result(&self, id: ResolveId) -> Result<crate::symbol::Info, Error> {
        let found: Vec<_> = self.resolved.iter().filter(|(got, _)| *got == id).collect();
        assert_eq!(found.len(), 1, "resolved {:?}", self.resolved);
        found[0].1.clone()
    }

    fn resolved_ok(&mut self, ticker: &str) -> crate::symbol::Info {
        let id = self.resolve(ticker);
        self.result(id).expect("resolve succeeded")
    }

    fn answer(&mut self, index: usize) {
        let held = self.held.remove(index);
        let body = self.sym.answer(&held.msg);
        let next = self
            .client
            .on_reply(self.now, held.ticket, Outcome::Body(&body));
        self.run(next);
    }

    fn reply(&mut self, index: usize, body: &[u8]) {
        let held = self.held.remove(index);
        let next = self
            .client
            .on_reply(self.now, held.ticket, Outcome::Body(body));
        self.run(next);
    }

    fn fail(&mut self, index: usize, text: &str) {
        let held = self.held.remove(index);
        let next = self
            .client
            .on_reply(self.now, held.ticket, Outcome::Failed(text));
        self.run(next);
    }

    fn drain(&mut self) {
        while !self.held.is_empty() {
            self.answer(0);
        }
    }

    fn notify(&mut self, subject: &str, ticker: &str, revision: i64) {
        let (venue, category) = table_of(ticker);
        let payload = changed(&venue, &category, revision);
        let next = self.client.on_changed(self.now, subject, &payload);
        self.run(next);
    }

    fn reconnect(&mut self) {
        let next = self.client.on_reconnect(self.now);
        self.run(next);
    }

    fn take_calls(&mut self) -> Vec<Call> {
        self.tags.clear();
        std::mem::take(&mut self.calls)
    }

    fn tick(&self, uid: u32) -> f64 {
        self.client.info(uid).expect("resolved").price_tick()
    }
}

#[test]
fn constants_and_config() {
    assert_eq!(SUBJECT, "sym");
    assert_eq!(CHECK_INTERVAL, Duration::from_secs(300));
    assert_eq!(REQUEST_TIMEOUT, Duration::from_secs(5));
    assert_eq!(REFRESH_TRIES, 8);
    assert_eq!(Config::default().check_interval, CHECK_INTERVAL);
    let zero = Client::open(Config {
        check_interval: Duration::ZERO,
    });
    assert_eq!(zero.err(), Some(OpenError::CheckInterval));
    assert_eq!(
        changed_subject("Bybit", "Spot").as_deref(),
        Some("sym.v1.changed.Bybit.Spot")
    );
    for (venue, category) in [
        ("", "Spot"),
        ("Bybit", ""),
        ("By.bit", "Spot"),
        ("Bybit", "Sp*t"),
        ("Bybit", ">"),
        ("By bit", "Spot"),
        ("Bybit", "Sp\tot"),
    ] {
        assert_eq!(
            changed_subject(venue, category),
            None,
            "{venue:?} {category:?}"
        );
    }
}

#[test]
fn resolve_is_refused_outside_init_and_start() {
    let mut h = Harness::seeded();
    assert!(!h.client.resolve_allowed());
    assert_eq!(h.client.resolve(NOW, BTC_T).err(), Some(Error::NotInInit));
    assert_eq!(
        Error::NotInInit.to_string(),
        "symclient: only OnInit and OnStart may call Resolve"
    );

    h.resolved_ok(BTC_T);
    assert!(h.client.resolve_allowed());
    h.client.allow_resolve(false);
    let before = h.calls.len();
    assert_eq!(h.client.resolve(NOW, ETH_T).err(), Some(Error::NotInInit));
    assert_eq!(h.calls.len(), before, "a refused resolve sent nothing");
    // Any hook may read memory.
    assert_eq!(h.tick(BTC), 0.1);
}

#[test]
fn a_ticker_that_is_not_canonical_sends_nothing() {
    let mut h = Harness::seeded();
    h.client.allow_resolve(true);
    for ticker in ["nope", "bybit_spot_btcusdt", "Bybit_Spot_BTC/USDT"] {
        match h.client.resolve(NOW, ticker) {
            Err(Error::InvalidTicker(message)) => {
                assert!(
                    message.starts_with("symbol: invalid universal ticker: "),
                    "{message}"
                );
            }
            other => panic!("{ticker}: {other:?}"),
        }
    }
    assert!(h.calls.is_empty());
}

#[test]
fn resolve_gets_subscribes_stores_and_settles_the_table() {
    let mut h = Harness::seeded();
    h.client.allow_resolve(true);
    let Resolving { id, effects } = h.client.resolve(NOW, BTC_T).expect("accepted");
    let [
        Effect::Request {
            request: first,
            wait,
            resolve,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("first effect {effects:?}");
    };
    assert_eq!(call_of(first), get(BTC_T));
    assert_eq!(*wait, REQUEST_TIMEOUT);
    assert_eq!(*resolve, Some(id));
    h.run(effects);

    // The first row of a table has no confirmed revision yet: one pass.
    assert_eq!(
        h.take_calls(),
        [get(BTC_T), Call::Rev, Call::Rev, by(BTC), Call::Rev]
    );
    assert_eq!(h.subjects, [SPOT_SUB]);
    assert_eq!(h.stored, [BTC, BTC]);
    assert!(h.logs.is_empty(), "{:?}", h.logs);
    let info = h.result(id).expect("resolved");
    assert_eq!(info.uid, BTC);
    assert_eq!(info.ticker.to_string(), BTC_T);
    assert_eq!(info.exch_ticker, "BTCUSDT");
    assert_eq!(info.status, Status::Tradable);
    assert_eq!(info.price_tick(), 0.1);
    assert_eq!(h.client.info(BTC), Ok(&info));
    assert_eq!(h.timers.len(), 1);
    let (_, after, deadline) = h.timers[0];
    assert_eq!(after, CHECK_INTERVAL);
    assert_eq!(deadline, NOW + INTERVAL_NS);
}

#[test]
fn requests_on_a_resolve_path_carry_its_id() {
    let mut h = Harness::seeded();
    let id = h.resolve(BTC_T);
    assert!(h.tags.iter().all(|tag| *tag == Some(id)), "{:?}", h.tags);
    h.take_calls();
    h.sym.bump(BTC_T);
    h.reconnect();
    assert_eq!(h.calls.len(), 4);
    assert!(h.tags.iter().all(Option::is_none), "{:?}", h.tags);
}

#[test]
fn later_resolves_reuse_the_subscription_and_the_confirmed_revision() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();

    let eth = h.resolved_ok(ETH_T);
    assert_eq!(eth.price_tick(), 0.2);
    assert_eq!(h.take_calls(), [get(ETH_T), Call::Rev]);
    assert_eq!(h.subjects, [SPOT_SUB]);

    // The same ticker again is one more Get.
    h.resolved_ok(BTC_T);
    assert_eq!(h.take_calls(), [get(BTC_T), Call::Rev]);

    let perp = h.resolved_ok(PERP_T);
    assert_eq!(perp.ticker.category, Category::Perp);
    assert_eq!(
        h.take_calls(),
        [get(PERP_T), Call::Rev, Call::Rev, by(PERP), Call::Rev]
    );
    assert_eq!(h.subjects, [SPOT_SUB, PERP_SUB]);
    assert_eq!(h.timers.len(), 1, "one check timer for the session");
}

#[test]
fn a_ticker_sym_does_not_list_is_not_found_and_not_tracked() {
    let mut h = Harness::seeded();
    let id = h.resolve("Bybit_Spot_SOLUSDT");
    let err = h.result(id).expect_err("not listed");
    let Error::NotFound(reason) = &err else {
        panic!("{err:?}");
    };
    assert!(
        reason.starts_with("not_found: ticker Bybit_Spot_SOLUSDT"),
        "{reason}"
    );
    assert!(
        err.to_string()
            .starts_with("symbol: no such instrument: not_found: ")
    );
    assert_eq!(h.calls, [get("Bybit_Spot_SOLUSDT")]);
    assert!(h.subjects.is_empty() && h.stored.is_empty() && h.timers.is_empty());
    assert_eq!(h.client.info(BTC), Err(Error::UnknownUid));
    // Nothing is tracked, so there is nothing to check.
    assert!(h.client.on_reconnect(NOW).is_empty());
}

fn failed_get(reply: impl FnOnce(&mut Harness)) -> Error {
    let mut h = Harness::seeded();
    h.hold = true;
    let id = h.resolve(BTC_T);
    reply(&mut h);
    assert!(h.held.is_empty(), "nothing else was asked");
    assert!(h.subjects.is_empty() && h.stored.is_empty() && h.timers.is_empty());
    assert_eq!(h.client.info(BTC), Err(Error::UnknownUid));
    h.result(id).expect_err("the resolve failed")
}

fn failed(message: &str) -> Error {
    Error::Failed(message.to_owned())
}

#[test]
fn a_failed_get_fails_the_resolve() {
    assert_eq!(
        failed_get(|h| h.fail(0, "timeout")),
        failed("symclient: request sym: timeout")
    );
    assert_eq!(
        failed_get(|h| h.reply(0, &error_body(2, "invalid: ticker"))),
        failed("symclient: invalid: ticker")
    );
    assert_eq!(
        failed_get(|h| h.reply(0, &error_body(2, "unavailable: db"))),
        failed("symclient: unavailable: db")
    );
    // Only a handler error with that word is not_found.
    assert_eq!(
        failed_get(|h| h.reply(0, &error_body(1, "not_found"))),
        failed("symclient: not_found")
    );
    assert_eq!(
        failed_get(|h| h.reply(0, &[0x00, 0x01])),
        failed("symclient: short error reply")
    );
    let reply = sym::Reply {
        error: "boom".to_owned(),
        result: Some(reply::Result::Info(listing(BTC, BTC_T, 0.1))),
        ..Default::default()
    };
    assert_eq!(
        failed_get(|h| h.reply(0, &reply.encode_to_vec())),
        failed("symclient: reply error field \"boom\"")
    );
    let reason = sym::Reply {
        reason: "why".to_owned(),
        ..Default::default()
    };
    assert_eq!(
        failed_get(|h| h.reply(0, &reason.encode_to_vec())),
        failed("symclient: reply error field \"\"")
    );
    let revisions = revisions_body(&BTreeMap::new());
    assert_eq!(
        failed_get(|h| h.reply(0, &revisions)),
        failed("symclient: empty info")
    );
    assert_eq!(
        failed_get(|h| h.reply(0, &info_body(listing(ETH, ETH_T, 0.2)))),
        failed("symclient: get Bybit_Spot_BTCUSDT returned Bybit_Spot_ETHUSDT")
    );
    assert_eq!(
        failed_get(|h| h.reply(0, &info_body(listing(0, BTC_T, 0.1)))),
        failed("symclient: uid 0 is not a valid uid")
    );
    match failed_get(|h| h.reply(0, &[0xff, 0xff])) {
        Error::Failed(message) => assert!(message.starts_with("symclient: reply: "), "{message}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn reply_framing() {
    assert_eq!(decode(Outcome::Body(&[])), Ok(sym::Reply::default()));
    assert_eq!(
        decode(Outcome::Body(&error_body(2, "not_found"))),
        Err(Fault::NotFound("not_found".to_owned()))
    );
    assert_eq!(
        decode(Outcome::Body(&error_body(2, "not_found: uid 9"))),
        Err(Fault::NotFound("not_found: uid 9".to_owned()))
    );
    assert_eq!(
        decode(Outcome::Body(&error_body(2, "not_foundx"))),
        Err(Fault::Other("symclient: not_foundx".to_owned()))
    );
    assert_eq!(
        decode(Outcome::Body(&error_body(3, ""))),
        Err(Fault::Other("symclient: ".to_owned()))
    );
    for (body, message) in [
        (
            vec![0x00, 0x01, 0x00, 0x02, 0x00],
            "symclient: short error reply",
        ),
        (
            vec![0x00, 0x02, 0x00, 0x02, 0x00, 0x00],
            "symclient: error reply version 2",
        ),
        (
            vec![0x00, 0x01, 0x00, 0x02, 0x00, 0x02, b'a'],
            "symclient: error reply length",
        ),
        (
            vec![0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0xff],
            "symclient: error reply is not utf-8",
        ),
    ] {
        assert_eq!(
            decode(Outcome::Body(&body)),
            Err(Fault::Other(message.to_owned())),
            "{body:?}"
        );
    }
    assert_eq!(
        decode(Outcome::Failed("nats: no responders")),
        Err(Fault::Other(
            "symclient: request sym: nats: no responders".to_owned()
        ))
    );
}

#[test]
fn a_failed_subscribe_fails_the_resolve_and_a_later_one_retries() {
    let mut h = Harness::seeded();
    h.sub_fail = Some("connection closed".to_owned());
    let id = h.resolve(BTC_T);
    assert_eq!(
        h.result(id),
        Err(failed(
            "symclient: subscribe sym.v1.changed.Bybit.Spot: connection closed"
        ))
    );
    assert_eq!(h.take_calls(), [get(BTC_T)]);
    assert!(h.stored.is_empty() && h.timers.is_empty());
    assert_eq!(h.client.info(BTC), Err(Error::UnknownUid));

    h.resolved_ok(BTC_T);
    assert_eq!(h.subjects, [SPOT_SUB]);
    assert_eq!(h.tick(BTC), 0.1);
}

#[test]
fn resolves_share_a_pending_subscribe() {
    let mut h = Harness::seeded();
    h.hold_subs = true;
    let btc = h.resolve(BTC_T);
    let eth = h.resolve(ETH_T);
    assert_eq!(h.held_subs.len(), 1, "one subscribe for the table");
    let (ticket, subject) = h.held_subs.remove(0);
    assert_eq!(subject, SPOT_SUB);
    let next = h.client.on_subscribed(NOW, ticket, Ok(()));
    h.run(next);
    assert_eq!(
        h.take_calls(),
        [
            get(BTC_T),
            get(ETH_T),
            Call::Rev,
            Call::Rev,
            by(BTC),
            by(ETH),
            Call::Rev
        ]
    );
    assert_eq!(h.result(btc).map(|info| info.uid), Ok(BTC));
    assert_eq!(h.result(eth).map(|info| info.uid), Ok(ETH));

    let mut h = Harness::seeded();
    h.hold_subs = true;
    let btc = h.resolve(BTC_T);
    let eth = h.resolve(ETH_T);
    let (ticket, _) = h.held_subs.remove(0);
    let next = h.client.on_subscribed(NOW, ticket, Err("closed"));
    h.run(next);
    let want = failed("symclient: subscribe sym.v1.changed.Bybit.Spot: closed");
    assert_eq!(h.result(btc), Err(want.clone()));
    assert_eq!(h.result(eth), Err(want));
    assert_eq!(h.client.info(ETH), Err(Error::UnknownUid));
    h.hold_subs = false;
    h.resolved_ok(ETH_T);
    assert_eq!(h.subjects, [SPOT_SUB]);
}

#[test]
fn a_failed_sync_still_resolves_and_the_next_check_retries() {
    let mut h = Harness::seeded();
    h.hold = true;
    let id = h.resolve(BTC_T);
    h.answer(0);
    assert_eq!(h.calls, [get(BTC_T), Call::Rev]);
    h.fail(0, "no responders");
    assert_eq!(h.result(id).map(|info| info.uid), Ok(BTC));
    assert_eq!(
        h.logs,
        ["symclient: sync Bybit Spot: symclient: request sym: no responders"]
    );
    h.hold = false;
    h.take_calls();
    // The table never confirmed a revision, so a check refetches it.
    h.reconnect();
    assert_eq!(h.take_calls(), [Call::Rev, Call::Rev, by(BTC), Call::Rev]);
    // Now it is confirmed.
    h.reconnect();
    assert_eq!(h.take_calls(), [Call::Rev]);
}

#[test]
fn a_notification_refetches_only_the_tracked_rows_of_its_table() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.resolved_ok(PERP_T);
    h.take_calls();
    h.stored.clear();

    h.sym.set_tick(BTC, 0.4);
    let rev = h.sym.bump(BTC_T);
    assert_eq!(h.tick(BTC), 0.1, "memory until the refetch");
    h.notify(SPOT_SUB, BTC_T, rev);
    // ETH is listed and never resolved; it is not asked for.
    assert_eq!(h.take_calls(), [Call::Rev, by(BTC), Call::Rev]);
    assert_eq!(h.stored, [BTC]);
    assert_eq!(h.tick(BTC), 0.4);
    assert_eq!(h.tick(PERP), 0.5);
    assert_eq!(h.client.info(ETH), Err(Error::UnknownUid));
    assert!(h.logs.is_empty(), "{:?}", h.logs);
}

#[test]
fn stale_and_foreign_notifications_do_nothing() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    let none: Vec<Effect> = Vec::new();
    // At or below the confirmed revision.
    assert_eq!(
        h.client
            .on_changed(NOW, SPOT_SUB, &changed("Bybit", "Spot", 1)),
        none
    );
    // Tables this session did not resolve.
    assert_eq!(
        h.client
            .on_changed(NOW, PERP_SUB, &changed("Bybit", "Perp", 9)),
        none
    );
    assert_eq!(
        h.client
            .on_changed(NOW, "sym.v1.changed.Okx.Spot", &changed("Okx", "Spot", 9)),
        none
    );
    assert_eq!(
        h.client.on_changed(
            NOW,
            "sym.v1.changed.Bybit.Bogus",
            &changed("Bybit", "Bogus", 9)
        ),
        none
    );
}

#[test]
fn a_malformed_notification_runs_a_revision_check() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();
    let cases: [(&str, Vec<u8>); 5] = [
        (SPOT_SUB, vec![0xff, 0xff]),
        (PERP_SUB, changed("Bybit", "Spot", 2)),
        (SPOT_SUB, changed("Bybit", "Spot", 0)),
        (SPOT_SUB, changed("Bybit", "Spot", -3)),
        ("sym.v1.changed.By.bit.Spot", changed("By.bit", "Spot", 2)),
    ];
    for (subject, payload) in cases {
        let effects = h.client.on_changed(NOW, subject, &payload);
        let [
            Effect::Request {
                request, resolve, ..
            },
        ] = effects.as_slice()
        else {
            panic!("{subject}: {effects:?}");
        };
        assert_eq!(call_of(request), Call::Rev);
        assert_eq!(*resolve, None);
        h.run(effects);
        // The revision is still the confirmed one, so nothing is refetched.
        assert_eq!(h.take_calls(), [Call::Rev], "{subject}");
    }
}

#[test]
fn checks_do_not_overlap() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();
    h.hold = true;
    h.reconnect();
    assert!(h.client.on_reconnect(NOW).is_empty(), "one check at a time");
    assert_eq!(h.take_calls(), [Call::Rev]);
    h.answer(0);
    // The second request runs once the first is answered.
    assert_eq!(h.take_calls(), [Call::Rev]);
    h.answer(0);
    assert!(h.held.is_empty() && h.calls.is_empty());
}

#[test]
fn a_delisted_row_is_kept_with_its_terminal_status() {
    let mut h = Harness::seeded();
    h.sym.seed(listing(ADA, "Bybit_Spot_ADAUSDT", 0.01));
    h.resolved_ok(BTC_T);
    h.resolved_ok(ETH_T);
    h.take_calls();

    let rev = h.sym.delist(BTC, 0.7);
    h.notify(SPOT_SUB, BTC_T, rev);
    assert_eq!(
        h.take_calls(),
        [
            Call::Rev,
            by(BTC),
            Call::ByUid(BTC, true),
            by(ETH),
            Call::Rev
        ]
    );
    let info = h.client.info(BTC).expect("still known");
    assert_eq!(info.status, Status::Delisted);
    assert!(info.status.terminal() && !info.status.active() && !info.is_tradable());
    assert_eq!(info.price_tick(), 0.7);
    let eth = h.client.info(ETH).expect("resolved");
    assert_eq!(eth.status, Status::Tradable);
    assert_eq!(eth.price_tick(), 0.2);
    assert_eq!(h.client.info(ADA), Err(Error::UnknownUid));
    assert!(h.logs.is_empty(), "{:?}", h.logs);

    // Later passes still find the last copy.
    h.sym.set_tick(ETH, 0.3);
    let rev = h.sym.bump(ETH_T);
    h.notify(SPOT_SUB, ETH_T, rev);
    assert_eq!(
        h.take_calls(),
        [
            Call::Rev,
            by(BTC),
            Call::ByUid(BTC, true),
            by(ETH),
            Call::Rev
        ]
    );
    assert_eq!(
        h.client.info(BTC).map(|info| info.status),
        Ok(Status::Delisted)
    );
    assert_eq!(h.tick(ETH), 0.3);
}

#[test]
fn a_row_missing_from_both_tables_keeps_the_last_copy() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();
    h.sym.active.remove(&BTC);
    let rev = h.sym.bump(BTC_T);
    h.notify(SPOT_SUB, BTC_T, rev);
    assert_eq!(h.take_calls(), [Call::Rev, by(BTC), Call::ByUid(BTC, true)]);
    assert_eq!(
        h.logs,
        [
            "symclient: reload Bybit Spot: symbol: no such instrument: not_found: uid 1001 not found in active or terminal table (include_inactive=true)"
        ]
    );
    let info = h.client.info(BTC).expect("kept");
    assert_eq!(info.status, Status::Tradable);
    assert_eq!(info.price_tick(), 0.1);
    // The revision was not confirmed, so the same notification tries again.
    h.notify(SPOT_SUB, BTC_T, rev);
    assert_eq!(h.take_calls(), [Call::Rev, by(BTC), Call::ByUid(BTC, true)]);
}

#[test]
fn the_revision_check_runs_on_the_injected_timer() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();
    let (token, _, deadline) = h.timers[0];

    // No notification arrives. The check is what notices.
    h.sym.set_tick(BTC, 1.5);
    h.sym.bump(BTC_T);
    h.now = deadline;
    let next = h.client.on_timer(h.now, token);
    h.run(next);
    assert_eq!(h.take_calls(), [Call::Rev, Call::Rev, by(BTC), Call::Rev]);
    assert_eq!(h.tick(BTC), 1.5);
    assert_eq!(h.timers.len(), 2);
    let (again, after, next_deadline) = h.timers[1];
    assert_eq!(after, CHECK_INTERVAL);
    assert_eq!(next_deadline, deadline + INTERVAL_NS);

    // The old token is spent.
    assert!(h.client.on_timer(h.now, token).is_empty());
    // An up-to-date table costs one Revisions.
    h.now = next_deadline;
    let next = h.client.on_timer(h.now, again);
    h.run(next);
    assert_eq!(h.take_calls(), [Call::Rev]);
    assert_eq!(h.timers.len(), 3);
}

#[test]
fn a_reconnect_runs_the_check_now() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.resolved_ok(PERP_T);
    h.take_calls();
    h.sym.set_tick(BTC, 2.0);
    h.sym.bump(BTC_T);
    assert_eq!(h.tick(BTC), 0.1);
    h.reconnect();
    // Only the table that moved is refetched.
    assert_eq!(h.take_calls(), [Call::Rev, Call::Rev, by(BTC), Call::Rev]);
    assert_eq!(h.tick(BTC), 2.0);
}

#[test]
fn nothing_tracked_means_no_check() {
    let mut h = Harness::seeded();
    assert!(h.client.on_reconnect(NOW).is_empty());
    assert!(h.client.on_changed(NOW, SPOT_SUB, &[0xff]).is_empty());
    assert!(h.client.on_timer(NOW, TimerToken(1)).is_empty());
    assert!(h.calls.is_empty());
}

#[test]
fn a_moving_table_repeats_the_pass_then_gives_up() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();

    // One change lands during the first pass. The second pass settles.
    h.sym.churn = 1;
    h.sym.set_tick(BTC, 0.9);
    let rev = h.sym.bump(BTC_T);
    h.notify(SPOT_SUB, BTC_T, rev);
    assert_eq!(
        h.take_calls(),
        [Call::Rev, by(BTC), Call::Rev, Call::Rev, by(BTC), Call::Rev]
    );
    assert!(h.logs.is_empty(), "{:?}", h.logs);
    assert_eq!(h.tick(BTC), 0.9);
    // The settled revision is 3; a notification for it is stale.
    assert!(
        h.client
            .on_changed(NOW, SPOT_SUB, &changed("Bybit", "Spot", 3))
            .is_empty()
    );

    // Every pass moves: eight passes, then the failure is logged.
    h.sym.churn = 100;
    let rev = h.sym.bump(BTC_T);
    assert_eq!(rev, 4);
    h.notify(SPOT_SUB, BTC_T, rev);
    let calls = h.take_calls();
    assert_eq!(calls.len(), 3 * REFRESH_TRIES as usize);
    assert_eq!(
        h.logs,
        [
            "symclient: reload Bybit Spot: symclient: revision moved during refresh: Bybit Spot from 11 to 12"
        ]
    );
    // Not confirmed: the next check refetches once SYM is still.
    h.sym.churn = 0;
    h.reconnect();
    assert_eq!(h.take_calls(), [Call::Rev, Call::Rev, by(BTC), Call::Rev]);
    h.reconnect();
    assert_eq!(h.take_calls(), [Call::Rev]);
}

#[test]
fn a_resolve_during_a_pass_waits_for_a_pass_that_includes_it() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();
    h.resolved.clear();
    h.sym.set_tick(BTC, 0.4);
    let rev = h.sym.bump(BTC_T);

    h.hold = true;
    h.notify(SPOT_SUB, BTC_T, rev);
    h.answer(0); // first Revisions: the pass lists BTC only
    let eth = h.resolve(ETH_T);
    assert_eq!(h.calls, [Call::Rev, by(BTC), get(ETH_T)]);
    assert_eq!(h.tags, [None, None, Some(eth)]);
    h.answer(1); // the Get: ETH is stored and joins the pass
    assert!(h.resolved.is_empty());
    assert_eq!(h.tick(ETH), 0.2);
    h.answer(0); // BTC
    h.answer(0); // second Revisions agrees, but ETH was not in the pass
    assert!(h.resolved.is_empty(), "the pass did not confirm");
    h.drain();
    assert_eq!(
        h.take_calls(),
        [
            Call::Rev,
            by(BTC),
            get(ETH_T),
            Call::Rev,
            Call::Rev,
            by(BTC),
            by(ETH),
            Call::Rev
        ]
    );
    assert_eq!(h.result(eth).map(|info| info.price_tick()), Ok(0.2));
    assert_eq!(h.tick(BTC), 0.4);
    // The table is confirmed at the current revision.
    assert!(
        h.client
            .on_changed(NOW, SPOT_SUB, &changed("Bybit", "Spot", rev))
            .is_empty()
    );
}

#[test]
fn a_notification_during_a_pass_is_folded_in() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();
    h.hold = true;

    // The pass reads the newer revision itself: no second pass.
    let rev = h.sym.bump(BTC_T);
    h.notify(SPOT_SUB, BTC_T, rev);
    h.sym.set_tick(BTC, 0.6);
    let rev = h.sym.bump(BTC_T);
    h.notify(SPOT_SUB, BTC_T, rev);
    assert_eq!(h.calls, [Call::Rev], "no second pass while one runs");
    h.drain();
    assert_eq!(h.take_calls(), [Call::Rev, by(BTC), Call::Rev]);
    assert_eq!(h.tick(BTC), 0.6);

    // The pass fails. The newer notification still gets its pass.
    let rev = h.sym.bump(BTC_T);
    h.notify(SPOT_SUB, BTC_T, rev);
    h.sym.set_tick(BTC, 0.7);
    let rev = h.sym.bump(BTC_T);
    h.notify(SPOT_SUB, BTC_T, rev);
    h.fail(0, "timeout");
    assert_eq!(
        h.logs,
        ["symclient: reload Bybit Spot: symclient: request sym: timeout"]
    );
    h.drain();
    assert_eq!(h.take_calls(), [Call::Rev, Call::Rev, by(BTC), Call::Rev]);
    assert_eq!(h.tick(BTC), 0.7);
    assert_eq!(h.logs.len(), 1);
}

#[test]
fn a_check_during_a_failed_pass_runs_one_more() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();
    h.hold = true;
    h.sym.set_tick(BTC, 0.8);
    let rev = h.sym.bump(BTC_T);
    h.notify(SPOT_SUB, BTC_T, rev);
    h.reconnect();
    assert_eq!(h.calls, [Call::Rev, Call::Rev]);
    h.answer(1); // the check: behind, but a pass is already running
    assert_eq!(h.held.len(), 1);
    h.fail(0, "timeout"); // the pass
    h.drain();
    assert_eq!(
        h.take_calls(),
        [Call::Rev, Call::Rev, Call::Rev, by(BTC), Call::Rev]
    );
    assert_eq!(h.tick(BTC), 0.8);

    // A failed pass that a check started logs as a revision check.
    h.sym.bump(BTC_T);
    h.reconnect();
    h.answer(0);
    h.fail(0, "down");
    assert_eq!(
        h.logs.last().map(String::as_str),
        Some("symclient: revision check: symclient: request sym: down")
    );
    // A failed check logs the same way.
    h.reconnect();
    h.fail(0, "down again");
    assert_eq!(
        h.logs.last().map(String::as_str),
        Some("symclient: revision check: symclient: request sym: down again")
    );
}

#[test]
fn a_refetched_row_must_be_the_one_asked_for() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();
    let rev = h.sym.bump(BTC_T);
    h.hold = true;

    h.notify(SPOT_SUB, BTC_T, rev);
    h.answer(0);
    h.reply(0, &info_body(listing(BTC, PERP_T, 0.1)));
    assert_eq!(
        h.logs,
        ["symclient: reload Bybit Spot: symclient: uid 1001 is Bybit_Perp_BTCUSDT, not Bybit Spot"]
    );

    h.notify(SPOT_SUB, BTC_T, rev);
    h.answer(0);
    h.reply(0, &info_body(listing(ETH, ETH_T, 0.2)));
    assert_eq!(
        h.logs.last().map(String::as_str),
        Some("symclient: reload Bybit Spot: symclient: get uid 1001 returned 1002")
    );
    assert!(h.held.is_empty());
    let info = h.client.info(BTC).expect("kept");
    assert_eq!(info.ticker.to_string(), BTC_T);
    assert_eq!(h.client.info(ETH), Err(Error::UnknownUid));
}

#[test]
fn close_ends_pending_resolves_and_ignores_the_rest() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    let (token, _, _) = h.timers[0];
    h.hold = true;
    let eth = h.resolve(ETH_T);
    let effects = h.client.close();
    assert_eq!(
        effects,
        [
            Effect::Resolved {
                id: eth,
                result: Err(Error::Closed)
            },
            Effect::Unsubscribe {
                subject: SPOT_SUB.to_owned()
            },
        ]
    );
    let before = h.calls.len();
    h.answer(0);
    assert_eq!(h.calls.len(), before);
    assert!(h.resolved.iter().all(|(id, _)| *id != eth));
    assert!(
        h.client
            .on_changed(NOW, SPOT_SUB, &changed("Bybit", "Spot", 9))
            .is_empty()
    );
    assert!(h.client.on_reconnect(NOW).is_empty());
    assert!(h.client.on_timer(NOW, token).is_empty());
    h.client.allow_resolve(true);
    assert!(!h.client.resolve_allowed());
    assert_eq!(h.client.resolve(NOW, ETH_T).err(), Some(Error::Closed));
    assert_eq!(h.tick(BTC), 0.1, "info still reads memory");
    assert!(h.client.close().is_empty());
}

#[test]
fn close_unsubscribes_a_pending_subscribe() {
    let mut h = Harness::seeded();
    h.hold_subs = true;
    let id = h.resolve(BTC_T);
    let (ticket, _) = h.held_subs.remove(0);
    assert_eq!(
        h.client.close(),
        [
            Effect::Resolved {
                id,
                result: Err(Error::Closed)
            },
            Effect::Unsubscribe {
                subject: SPOT_SUB.to_owned()
            },
        ]
    );
    assert!(h.client.on_subscribed(NOW, ticket, Ok(())).is_empty());
    assert_eq!(h.client.info(BTC), Err(Error::UnknownUid));
}

#[test]
fn unknown_and_spent_tickets_are_ignored() {
    let mut h = Harness::seeded();
    assert!(
        h.client
            .on_reply(NOW, Ticket(999), Outcome::Body(&[]))
            .is_empty()
    );
    assert!(h.client.on_subscribed(NOW, Ticket(999), Ok(())).is_empty());
    h.hold = true;
    h.resolve(BTC_T);
    let ticket = h.held[0].ticket;
    h.answer(0);
    let body = info_body(listing(BTC, BTC_T, 0.1));
    assert!(
        h.client
            .on_reply(NOW, ticket, Outcome::Body(&body))
            .is_empty()
    );
}

#[test]
fn stored_rows_feed_bookpath() {
    let mut h = Harness::seeded();
    let mut none = listing(1004, "Bybit_Spot_XRPUSDT", 0.0);
    none.filters.clear();
    h.sym.seed(none);
    let mut open = listing(1005, "Bybit_Spot_DOGEUSDT", 0.0);
    open.filters
        .insert(PRICE_TICK.to_owned(), sym::Bound { value: None });
    h.sym.seed(open);
    for ticker in [BTC_T, "Bybit_Spot_XRPUSDT", "Bybit_Spot_DOGEUSDT"] {
        h.resolved_ok(ticker);
    }

    let mut path = bookpath::Path::open(bookpath::Config {
        recv_window: Duration::from_secs(1),
    })
    .expect("bookpath config");
    for uid in &h.stored {
        let info = h.client.info(*uid).expect("stored rows are known");
        path.resolve(*uid, info.price_tick());
    }
    let delta = |uid| bookpath::Delta {
        uid,
        conn_epoch: 1,
        seq: 1,
        md_ts_ns: NOW,
        exch_ts_ns: NOW,
        reset: false,
        bids: Vec::new(),
        asks: Vec::new(),
    };
    let effects = path.on_delta(NOW, delta(BTC));
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, bookpath::Effect::RequestSnapshot { uid: BTC, .. })),
        "{effects:?}"
    );
    for uid in [1004, 1005] {
        assert_eq!(
            path.on_delta(NOW, delta(uid)),
            [bookpath::Effect::Log(format!(
                "book: uid {uid} has no price tick"
            ))]
        );
    }
}

#[test]
fn symbol_info_maps_every_field() {
    let mut msg = listing(BTC, "Okx_Option_BTCUSD-261225-6D4-C", 0.5);
    msg.contract_size = Some(0.01);
    msg.settle = Some("BTC".to_owned());
    msg.expiry_ns = 77;
    msg.strike = Some(6.4);
    msg.option_type = "C".to_owned();
    msg.status = sym::SymbolStatus::Settling as i32;
    msg.filters
        .insert("min_qty".to_owned(), sym::Bound { value: None });
    msg.updated_at_ns = 99;
    let info = info_from_proto(msg.clone()).expect("valid");
    assert_eq!(
        info_to_proto(&info),
        msg,
        "the event log's copy is the same row"
    );
    assert_eq!(info.uid, BTC);
    assert_eq!(info.ticker.category, Category::Option);
    assert_eq!(info.ticker.symbol, "BTCUSD-261225-6D4-C");
    assert_eq!(info.exch_ticker, "BTCUSD-261225-6D4-C");
    assert_eq!((info.base.as_str(), info.quote.as_str()), ("BTC", "USDT"));
    assert_eq!(info.contract_size, 0.01);
    assert_eq!(info.contract_value_ccy, "BTC");
    assert_eq!(info.price_ccy, "USDT");
    assert_eq!(info.settle, "BTC");
    assert_eq!(info.expiry_ns, 77);
    assert_eq!(info.strike, 6.4);
    assert_eq!(info.option_type, "C");
    assert_eq!(info.status, Status::Settling);
    assert_eq!(info.price_tick(), 0.5);
    assert!(info.filters["min_qty"].is_nan(), "an unset bound is NaN");
    assert_eq!(info.updated_at_ns, 99);

    let spot = info_from_proto(listing(ETH, ETH_T, 0.2)).expect("valid");
    assert_eq!(spot.contract_size, 0.0);
    assert_eq!(spot.strike, 0.0);
    assert!(spot.settle.is_empty());
    assert_eq!(info_to_proto(&spot), listing(ETH, ETH_T, 0.2));

    let mut bad = listing(BTC, BTC_T, 0.1);
    bad.filters
        .insert(String::new(), sym::Bound { value: Some(1.0) });
    assert_eq!(
        info_from_proto(bad).err().as_deref(),
        Some("symclient: filter name is empty")
    );
    let mut bad = listing(BTC, BTC_T, 0.1);
    bad.status = 0;
    assert_eq!(
        info_from_proto(bad).err().as_deref(),
        Some("symclient: status \"SYMBOL_STATUS_UNSPECIFIED\" cannot be decoded")
    );
    let mut bad = listing(BTC, BTC_T, 0.1);
    bad.status = 42;
    assert_eq!(
        info_from_proto(bad).err().as_deref(),
        Some("symclient: status \"42\" cannot be decoded")
    );
    let bad = listing(BTC, "bybit_spot_btcusdt", 0.1);
    let message = info_from_proto(bad).expect_err("refused");
    assert!(
        message.starts_with(
            "symclient: ticker \"bybit_spot_btcusdt\": symbol: invalid universal ticker: "
        ),
        "{message}"
    );
}

#[test]
fn info_of_an_unresolved_uid_is_an_error_and_asks_nothing() {
    let mut h = Harness::seeded();
    h.resolved_ok(BTC_T);
    h.take_calls();
    for uid in [0, ETH, PERP, 999_999] {
        assert_eq!(h.client.info(uid), Err(Error::UnknownUid));
    }
    assert_eq!(
        Error::UnknownUid.to_string(),
        "symclient: the instrument is not known to this session"
    );
    assert!(h.calls.is_empty());
}
