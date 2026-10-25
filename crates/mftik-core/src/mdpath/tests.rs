//! MD path tests. The path has no clock; the dispatcher case injects one and
//! waits on channels. The five-second bound only fails a stuck test.

use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use mftik_proto::wire::mftiko::common::v1 as common;
use mftik_proto::wire::mftiko::md::v1::{
    self as md, feed_event, fetch_reply, fetch_request, view_reply,
};
use mftik_proto::wire::mftiko::stsctl::v1 as stsctl;
use prost::Message;

use super::{
    Apply, Config, Data, Decoded, Delivery, Effect, Error, Event, Hooks, Md, OpenError, Path,
    ReadError, ReadKind, ReadTicket, Reply, Request, RequestError, Route, SubId, TOPIC_BOOK,
    TapeQuery, Topic, Via, fetch_subject, view_subject,
};
use crate::dispatch::{self, Dispatcher, RunEnd, StopRequest};

const WINDOW: Duration = Duration::from_secs(1);
const TICKER: &str = "Binance_Spot_BTCUSDT";
const INST: &str = "md.v1.f.md-tw-1";
const UID: u32 = 42;
const NOW: i64 = 1_700_000_000_000_000_000;
const SEC_NS: i64 = 1_000_000_000;
const EXCH: i64 = 5;
const FUSE: Duration = Duration::from_secs(5);

fn open(hooks: Hooks) -> Path {
    Path::open(Config {
        recv_window: WINDOW,
        hooks,
    })
    .expect("open")
}

fn subj(topic: &str) -> String {
    format!("{INST}.{topic}.{TICKER}")
}

fn bind(topic: &str) -> stsctl::FeedBinding {
    bind_on(topic, &subj(topic))
}

fn bind_on(topic: &str, subject: &str) -> stsctl::FeedBinding {
    stsctl::FeedBinding {
        feed: format!("{topic}.{TICKER}"),
        md_instance: "md-tw-1".to_owned(),
        subject: subject.to_owned(),
        state: 0,
    }
}

fn snap(epoch: i64, seq: u64, feeds: Vec<stsctl::FeedBinding>) -> stsctl::Bindings {
    stsctl::Bindings {
        version: Some(common::Version { epoch, seq }),
        feeds,
        selectors: Vec::new(),
        accounts: Vec::new(),
    }
}

fn version(epoch: i64, seq: u64) -> Option<common::Version> {
    Some(common::Version { epoch, seq })
}

#[track_caller]
fn staged(path: &mut Path, bindings: &stsctl::Bindings) -> Vec<Effect> {
    match path.apply(bindings).expect("apply") {
        Apply::Staged(effects) => effects,
        Apply::Unchanged => panic!("snapshot was not staged"),
    }
}

/// Apply and bind. Returns the subscribes and the unsubscribes.
#[track_caller]
fn commit(path: &mut Path, bindings: &stsctl::Bindings) -> (Vec<Effect>, Vec<Effect>) {
    let subscribes = staged(path, bindings);
    let unsubscribes = path.apply_bound().expect("apply_bound");
    (subscribes, unsubscribes)
}

fn subscribed(effects: &[Effect]) -> Vec<(String, SubId)> {
    effects
        .iter()
        .map(|effect| match effect {
            Effect::Subscribe { sub, subject } => (subject.clone(), *sub),
            Effect::Unsubscribe { .. } => panic!("unexpected unsubscribe: {effect:?}"),
        })
        .collect()
}

fn unsubscribed(effects: &[Effect]) -> Vec<(String, SubId)> {
    effects
        .iter()
        .map(|effect| match effect {
            Effect::Unsubscribe { sub, subject } => (subject.clone(), *sub),
            Effect::Subscribe { .. } => panic!("unexpected subscribe: {effect:?}"),
        })
        .collect()
}

fn subjects_of(pairs: &[(String, SubId)]) -> Vec<String> {
    pairs.iter().map(|(subject, _)| subject.clone()).collect()
}

fn sub_of(pairs: &[(String, SubId)], subject: &str) -> SubId {
    pairs
        .iter()
        .find(|(s, _)| s == subject)
        .map(|(_, sub)| *sub)
        .unwrap_or_else(|| panic!("no subscription for {subject}"))
}

fn frame(md_ts: i64, epoch: u64, seq: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 40 + payload.len()];
    out[0] = 1;
    out[8..16].copy_from_slice(&md_ts.to_le_bytes());
    out[16..24].copy_from_slice(&epoch.to_le_bytes());
    out[24..32].copy_from_slice(&seq.to_le_bytes());
    out[32..40].copy_from_slice(&0xfeed_u64.to_le_bytes());
    out[40..].copy_from_slice(payload);
    out
}

fn meta() -> Option<md::Meta> {
    Some(md::Meta { exch_ts_ns: EXCH })
}

fn feed(uid: u32, event: feed_event::Event) -> Vec<u8> {
    md::FeedEvent {
        uid,
        event: Some(event),
    }
    .encode_to_vec()
}

fn trade(uid: u32) -> Vec<u8> {
    feed(
        uid,
        feed_event::Event::Trade(md::Trade {
            meta: meta(),
            trade_id: "t1".to_owned(),
            price: 2.0,
            qty: 3.0,
            side: 1,
        }),
    )
}

fn delta(uid: u32) -> Vec<u8> {
    feed(
        uid,
        feed_event::Event::OrderBookDelta(md::OrderBookDelta {
            meta: meta(),
            bids: vec![md::Level {
                price: 1.0,
                qty: 1.0,
            }],
            asks: Vec::new(),
            reset: false,
        }),
    )
}

/// One payload per hook topic, with the `Data` the decoder should return.
fn every_topic() -> Vec<(Topic, feed_event::Event, Data)> {
    let best = md::BestQuote {
        meta: meta(),
        bid: 1.0,
        bid_qty: 2.0,
        ask: 3.0,
        ask_qty: 4.0,
    };
    let trade = md::Trade {
        meta: meta(),
        trade_id: "t".to_owned(),
        price: 1.0,
        qty: 1.0,
        side: 2,
    };
    let agg = md::AggTrade {
        meta: meta(),
        trade_id: "a".to_owned(),
        match_count: 3,
        ..Default::default()
    };
    let kline = md::Kline {
        meta: meta(),
        interval: "1m".to_owned(),
        close: 9.0,
        closed: true,
        ..Default::default()
    };
    let ticker = md::Ticker {
        meta: meta(),
        last: Some(7.0),
        ..Default::default()
    };
    let liq = md::Liquidation {
        meta: meta(),
        side: 1,
        price: 5.0,
        qty: 6.0,
    };
    let fund = md::FundingRate {
        meta: meta(),
        rate: 0.01,
        next_funding_ns: 99,
        interval_s: Some(28_800),
    };
    let oi = md::OpenInterest {
        meta: meta(),
        open_interest: 11.0,
        open_interest_value: None,
    };
    let greeks = md::Greeks {
        meta: meta(),
        delta: Some(0.5),
        ..Default::default()
    };
    let mark = md::MarkPrice {
        meta: meta(),
        mark_price: 8.0,
        index_price: Some(8.5),
    };
    vec![
        (
            Topic::BestQuote,
            feed_event::Event::BestQuote(best),
            Data::BestQuote(best),
        ),
        (
            Topic::Trade,
            feed_event::Event::Trade(trade.clone()),
            Data::Trade(trade),
        ),
        (
            Topic::AggTrade,
            feed_event::Event::AggTrade(agg.clone()),
            Data::AggTrade(agg),
        ),
        (
            Topic::Kline,
            feed_event::Event::Kline(kline.clone()),
            Data::Kline(kline),
        ),
        (
            Topic::Ticker,
            feed_event::Event::Ticker(ticker),
            Data::Ticker(ticker),
        ),
        (
            Topic::Liquidation,
            feed_event::Event::Liquidation(liq),
            Data::Liquidation(liq),
        ),
        (
            Topic::FundingRate,
            feed_event::Event::FundingRate(fund),
            Data::FundingRate(fund),
        ),
        (
            Topic::OpenInterest,
            feed_event::Event::OpenInterest(oi),
            Data::OpenInterest(oi),
        ),
        (
            Topic::Greeks,
            feed_event::Event::Greeks(greeks),
            Data::Greeks(greeks),
        ),
        (
            Topic::MarkPrice,
            feed_event::Event::MarkPrice(mark),
            Data::MarkPrice(mark),
        ),
    ]
}

fn with(mut hooks: Hooks, topic: Topic, on: bool) -> Hooks {
    let flag = match topic {
        Topic::BestQuote => &mut hooks.best_quote,
        Topic::Trade => &mut hooks.trade,
        Topic::AggTrade => &mut hooks.agg_trade,
        Topic::Kline => &mut hooks.kline,
        Topic::Ticker => &mut hooks.ticker,
        Topic::Liquidation => &mut hooks.liquidation,
        Topic::FundingRate => &mut hooks.funding_rate,
        Topic::OpenInterest => &mut hooks.open_interest,
        Topic::Greeks => &mut hooks.greeks,
        Topic::MarkPrice => &mut hooks.mark_price,
    };
    *flag = on;
    hooks
}

#[track_caller]
fn delivered(path: &mut Path, request: &Request, reply: Result<&[u8], RequestError>) -> Reply {
    let Delivery { ticket, reply } = path.on_reply(request.ticket, reply).expect("delivery");
    assert_eq!(ticket, request.ticket);
    reply
}

#[track_caller]
fn read_error(reply: Reply) -> ReadError {
    match reply {
        Reply::Tape(result) => result.expect_err("tape error"),
        Reply::BookSnapshot(result) => result.expect_err("book error"),
        Reply::Klines(result) => result.expect_err("klines error"),
        Reply::OpenInterest(result) => result.expect_err("open interest error"),
    }
}

// ---------- open ----------

#[test]
fn open_rejects_a_zero_recv_window() {
    let err = Path::open(Config {
        recv_window: Duration::ZERO,
        hooks: Hooks::ALL,
    })
    .expect_err("zero window");
    assert_eq!(err, OpenError::RecvWindow);
    assert_eq!(err.to_string(), "mdpath: recv window must be positive");
    let path = open(Hooks::ALL);
    assert_eq!(path.recv_window(), WINDOW);
    assert_eq!(path.hooks(), Hooks::ALL);
    assert_eq!(path.bindings_applied(), None, "nothing applied yet");
    assert!(path.subjects().is_empty());
}

// ---------- versions (PROTOCOL §9.3.2, E08-D046) ----------

#[test]
fn the_first_snapshot_applies_even_without_a_version() {
    let mut path = open(Hooks::ALL);
    let first = stsctl::Bindings {
        feeds: vec![bind("trade")],
        ..Default::default()
    };
    let (subscribes, unsubscribes) = commit(&mut path, &first);
    assert_eq!(subjects_of(&subscribed(&subscribes)), vec![subj("trade")]);
    assert!(unsubscribes.is_empty());
    assert_eq!(
        path.bindings_applied(),
        version(0, 0),
        "a missing version is epoch 0, seq 0"
    );

    let again = stsctl::Bindings {
        feeds: vec![bind("ticker")],
        version: version(0, 0),
        ..Default::default()
    };
    assert_eq!(path.apply(&again), Ok(Apply::Unchanged));
    assert_eq!(path.subjects(), vec![subj("trade")]);

    let mut zero = open(Hooks::ALL);
    let (subscribes, _) = commit(&mut zero, &snap(0, 0, vec![bind("ticker")]));
    assert_eq!(subscribed(&subscribes).len(), 1, "version 0 is the first");
    assert_eq!(zero.bindings_applied(), version(0, 0));
}

#[test]
fn only_a_strictly_newer_version_applies_epoch_before_seq() {
    let mut path = open(Hooks::ALL);
    commit(&mut path, &snap(1, 5, vec![bind("trade")]));
    assert_eq!(path.bindings_applied(), version(1, 5));

    for (epoch, seq) in [(1, 5), (1, 4), (0, 99), (-1, u64::MAX)] {
        assert_eq!(
            path.apply(&snap(epoch, seq, vec![bind("ticker")])),
            Ok(Apply::Unchanged),
            "({epoch}, {seq}) is not newer than (1, 5)"
        );
        assert_eq!(path.subjects(), vec![subj("trade")]);
        assert_eq!(path.bindings_applied(), version(1, 5));
    }

    commit(&mut path, &snap(1, 6, vec![bind("ticker")]));
    assert_eq!(path.bindings_applied(), version(1, 6));
    assert_eq!(path.subjects(), vec![subj("ticker")]);

    commit(&mut path, &snap(2, 0, vec![bind("trade")]));
    assert_eq!(
        path.bindings_applied(),
        version(2, 0),
        "a newer epoch wins over a smaller seq"
    );
    assert_eq!(path.subjects(), vec![subj("trade")]);
}

#[test]
fn a_newer_snapshot_with_the_same_set_is_success_with_no_effects() {
    let mut path = open(Hooks::ALL);
    let (subscribes, _) = commit(&mut path, &snap(1, 1, vec![bind("trade"), bind("ticker")]));
    let before = subscribed(&subscribes);

    let (subscribes, unsubscribes) =
        commit(&mut path, &snap(1, 2, vec![bind("ticker"), bind("trade")]));
    assert!(subscribes.is_empty(), "{subscribes:?}");
    assert!(unsubscribes.is_empty(), "{unsubscribes:?}");
    assert_eq!(path.bindings_applied(), version(1, 2));
    for (subject, sub) in &before {
        assert_eq!(
            path.route(*sub, &frame(NOW, 1, 1, &trade(UID))),
            Route::Md,
            "{subject} kept its subscription"
        );
    }
}

// ---------- selectors and bad snapshots (E08-D022, §9.2.9) ----------

#[test]
fn a_snapshot_with_selectors_is_refused_whatever_its_version() {
    let mut fresh = open(Hooks::ALL);
    let mut with_selector = snap(0, 0, vec![bind("trade")]);
    with_selector.selectors.push(stsctl::SelectorBinding {
        name: "front".to_owned(),
        subject_prefix: INST.to_owned(),
        ..Default::default()
    });
    assert_eq!(fresh.apply(&with_selector), Err(Error::Selector));
    assert_eq!(
        fresh.bindings_applied(),
        None,
        "the first snapshot was not taken"
    );
    assert!(fresh.subjects().is_empty());

    let mut path = open(Hooks::ALL);
    commit(&mut path, &snap(1, 1, vec![bind("trade")]));
    for (epoch, seq) in [(1, 2), (1, 0), (9, 9)] {
        let mut bindings = snap(epoch, seq, vec![bind("ticker")]);
        bindings.selectors.push(stsctl::SelectorBinding::default());
        let err = path.apply(&bindings).expect_err("selector");
        assert_eq!(err, Error::Selector);
        assert_eq!(err.to_string(), "mdpath: selectors are not supported");
    }
    assert_eq!(path.subjects(), vec![subj("trade")]);
    assert_eq!(path.bindings_applied(), version(1, 1));
}

#[test]
fn bad_feeds_are_refused_and_leave_the_set() {
    let mut path = open(Hooks::ALL);
    commit(&mut path, &snap(1, 1, vec![bind("trade")]));

    let cases = [
        (
            stsctl::FeedBinding {
                feed: "noseparator".to_owned(),
                subject: subj("trade"),
                ..Default::default()
            },
            Error::FeedKey("noseparator".to_owned()),
            "mdpath: feed \"noseparator\" is not topic.ticker",
        ),
        (
            stsctl::FeedBinding {
                feed: ".BTC".to_owned(),
                subject: subj("trade"),
                ..Default::default()
            },
            Error::FeedKey(".BTC".to_owned()),
            "mdpath: feed \".BTC\" is not topic.ticker",
        ),
        (
            stsctl::FeedBinding {
                feed: "trade.".to_owned(),
                subject: subj("trade"),
                ..Default::default()
            },
            Error::FeedKey("trade.".to_owned()),
            "mdpath: feed \"trade.\" is not topic.ticker",
        ),
        (
            stsctl::FeedBinding {
                feed: format!("trade.{TICKER}"),
                ..Default::default()
            },
            Error::EmptySubject,
            "mdpath: empty subject",
        ),
    ];
    for (feed, want, text) in cases {
        // Older than what is held: feeds are checked before the version.
        let err = path.apply(&snap(0, 1, vec![feed])).expect_err("bad feed");
        assert_eq!(err, want);
        assert_eq!(err.to_string(), text);
    }

    let two_subjects = snap(
        3,
        1,
        vec![
            bind("trade"),
            bind_on("trade", &format!("{}.other", subj("trade"))),
        ],
    );
    let err = path.apply(&two_subjects).expect_err("two subjects");
    assert_eq!(err, Error::FeedOnTwoSubjects(format!("trade.{TICKER}")));
    assert_eq!(
        err.to_string(),
        format!("mdpath: feed trade.{TICKER} is on two subjects")
    );

    let two_feeds = snap(3, 1, vec![bind("trade"), bind_on("ticker", &subj("trade"))]);
    let err = path.apply(&two_feeds).expect_err("two feeds");
    assert_eq!(err, Error::SubjectForTwoFeeds(subj("trade")));
    assert_eq!(
        err.to_string(),
        format!("mdpath: subject {} is listed for two feeds", subj("trade"))
    );

    assert_eq!(path.subjects(), vec![subj("trade")]);
    assert_eq!(path.bindings_applied(), version(1, 1));

    // The same feed twice is one subscription. Accounts are not feeds.
    let mut ok = snap(3, 2, vec![bind("bestquote"), bind("bestquote")]);
    ok.accounts.push(stsctl::AccountBinding {
        name: "a".to_owned(),
        api_id: 7,
        ..Default::default()
    });
    let (subscribes, _) = commit(&mut path, &ok);
    assert_eq!(
        subjects_of(&subscribed(&subscribes)),
        vec![subj("bestquote")]
    );
    assert_eq!(path.subjects(), vec![subj("bestquote")]);
}

// ---------- the subscribed set (E08-D046, E01-D006) ----------

#[test]
fn the_set_follows_each_snapshot_exactly() {
    let mut path = open(Hooks::ALL);
    let (subscribes, unsubscribes) = commit(
        &mut path,
        &snap(1, 1, vec![bind("trade"), bind("bestquote")]),
    );
    let first = subscribed(&subscribes);
    assert_eq!(subjects_of(&first), vec![subj("bestquote"), subj("trade")]);
    assert!(unsubscribes.is_empty());
    assert_eq!(path.subjects(), vec![subj("bestquote"), subj("trade")]);

    let (subscribes, unsubscribes) = commit(
        &mut path,
        &snap(1, 2, vec![bind("bestquote"), bind("ticker")]),
    );
    let second = subscribed(&subscribes);
    assert_eq!(subjects_of(&second), vec![subj("ticker")]);
    assert_eq!(
        unsubscribed(&unsubscribes),
        vec![(subj("trade"), sub_of(&first, &subj("trade")))]
    );
    assert_eq!(path.subjects(), vec![subj("bestquote"), subj("ticker")]);

    let msg = frame(NOW, 1, 1, &trade(UID));
    assert_eq!(
        path.route(sub_of(&first, &subj("trade")), &msg),
        Route::Skip
    );
    assert_eq!(
        path.route(sub_of(&first, &subj("bestquote")), &msg),
        Route::Md
    );
    assert_eq!(
        path.route(sub_of(&second, &subj("ticker")), &msg),
        Route::Md
    );

    let (subscribes, unsubscribes) = commit(&mut path, &snap(2, 0, vec![bind("trade")]));
    let third = subscribed(&subscribes);
    assert_eq!(subjects_of(&third), vec![subj("trade")]);
    assert_ne!(
        sub_of(&third, &subj("trade")),
        sub_of(&first, &subj("trade")),
        "a subject that comes back is a new subscription"
    );
    assert_eq!(
        unsubscribed(&unsubscribes),
        vec![
            (subj("bestquote"), sub_of(&first, &subj("bestquote"))),
            (subj("ticker"), sub_of(&second, &subj("ticker"))),
        ]
    );
    assert_eq!(path.subjects(), vec![subj("trade")]);

    let (subscribes, unsubscribes) = commit(&mut path, &snap(2, 1, Vec::new()));
    assert!(subscribes.is_empty());
    assert_eq!(
        subjects_of(&unsubscribed(&unsubscribes)),
        vec![subj("trade")]
    );
    assert!(path.subjects().is_empty());
}

#[test]
fn the_subject_is_the_binding_string() {
    let mut path = open(Hooks::ALL);
    // The feed key's ticker is not in the subject. Nothing is composed from it.
    let given = "md.v1.f.custom-inst.trade.NOT_THE_TICKER";
    let bare = "not.a.feed.subject";
    let (subscribes, _) = commit(
        &mut path,
        &snap(
            1,
            1,
            vec![bind_on("trade", given), bind_on("aggtrade", bare)],
        ),
    );
    assert_eq!(
        subjects_of(&subscribed(&subscribes)),
        vec![given.to_owned(), bare.to_owned()]
    );

    let request = path
        .tape(TICKER, "trade", TapeQuery::default())
        .expect("tape");
    assert_eq!(
        request.subject,
        "md.v1.view.custom-inst.trade.NOT_THE_TICKER"
    );
    assert_eq!(
        path.tape(TICKER, "aggtrade", TapeQuery::default()),
        Err(Error::BadSubject),
        "md.view cannot be named without the md.v1.f. prefix"
    );
}

#[test]
fn a_changed_feed_key_on_a_kept_subject_is_resubscribed() {
    let mut path = open(Hooks::ALL);
    let subject = subj("trade");
    let (subscribes, _) = commit(&mut path, &snap(1, 1, vec![bind_on("trade", &subject)]));
    let old = sub_of(&subscribed(&subscribes), &subject);

    let subscribes = staged(&mut path, &snap(1, 2, vec![bind_on("aggtrade", &subject)]));
    let new = sub_of(&subscribed(&subscribes), &subject);
    assert_ne!(old, new);
    let msg = frame(NOW, 1, 1, &trade(UID));
    assert_eq!(
        path.route(old, &msg),
        Route::Md,
        "the old one routes until bound"
    );
    assert_eq!(
        path.route(new, &msg),
        Route::Md,
        "the new one routes once staged"
    );

    let unsubscribes = path.apply_bound().expect("bound");
    assert_eq!(unsubscribed(&unsubscribes), vec![(subject.clone(), old)]);
    assert_eq!(path.route(old, &msg), Route::Skip);
    assert_eq!(path.route(new, &msg), Route::Md);
    assert_eq!(path.subjects(), vec![subject]);
    assert!(path.tape(TICKER, "aggtrade", TapeQuery::default()).is_ok());
    assert_eq!(
        path.tape(TICKER, "trade", TapeQuery::default()),
        Err(Error::NoFeed)
    );
}

#[test]
fn an_aborted_apply_keeps_the_set_and_the_version() {
    let mut path = open(Hooks::ALL);
    let (subscribes, _) = commit(&mut path, &snap(1, 1, vec![bind("trade")]));
    let kept = sub_of(&subscribed(&subscribes), &subj("trade"));

    let next = snap(1, 2, vec![bind("ticker")]);
    let opened = subscribed(&staged(&mut path, &next));
    let ticker = sub_of(&opened, &subj("ticker"));
    let msg = frame(NOW, 1, 1, &trade(UID));
    assert_eq!(path.route(ticker, &msg), Route::Md);

    let dropped = path.apply_aborted();
    assert_eq!(unsubscribed(&dropped), opened);
    assert_eq!(path.route(ticker, &msg), Route::Skip);
    assert_eq!(path.route(kept, &msg), Route::Md);
    assert_eq!(path.subjects(), vec![subj("trade")]);
    assert_eq!(path.bindings_applied(), version(1, 1));
    assert_eq!(
        path.tape(TICKER, "ticker", TapeQuery::default()),
        Err(Error::NoFeed)
    );

    // The same snapshot is staged again: it was never applied.
    let retry = subscribed(&staged(&mut path, &next));
    assert_eq!(subjects_of(&retry), vec![subj("ticker")]);
    assert_ne!(sub_of(&retry, &subj("ticker")), ticker);
    assert!(path.apply_bound().is_ok());
    assert_eq!(path.bindings_applied(), version(1, 2));
}

#[test]
fn one_apply_at_a_time() {
    let mut path = open(Hooks::ALL);
    assert_eq!(path.apply_bound(), Err(Error::ApplyNotInProgress));
    assert!(path.apply_aborted().is_empty());

    let _ = staged(&mut path, &snap(1, 1, vec![bind("trade")]));
    let err = path
        .apply(&snap(1, 2, vec![bind("ticker")]))
        .expect_err("in progress");
    assert_eq!(err, Error::ApplyInProgress);
    assert_eq!(path.apply_bound().map(|effects| effects.len()), Ok(0));
    assert_eq!(path.apply_bound(), Err(Error::ApplyNotInProgress));
    assert_eq!(path.subjects(), vec![subj("trade")]);
}

#[test]
fn reads_use_the_bound_set() {
    let mut path = open(Hooks::ALL);
    commit(&mut path, &snap(1, 1, vec![bind("trade")]));
    let _ = staged(&mut path, &snap(1, 2, vec![bind("liquidation")]));
    assert!(
        path.tape(TICKER, "trade", TapeQuery::default()).is_ok(),
        "a feed leaving in the staged snapshot is still readable"
    );
    assert_eq!(
        path.tape(TICKER, "liquidation", TapeQuery::default()),
        Err(Error::NoFeed)
    );
    let _ = path.apply_bound().expect("bound");
    assert!(
        path.tape(TICKER, "liquidation", TapeQuery::default())
            .is_ok()
    );
    assert_eq!(
        path.tape(TICKER, "trade", TapeQuery::default()),
        Err(Error::NoFeed)
    );
}

// ---------- close ----------

#[test]
fn close_unsubscribes_everything_and_stops_the_path() {
    let mut path = open(Hooks::ALL);
    let (subscribes, _) = commit(&mut path, &snap(1, 1, vec![bind("trade"), bind("ticker")]));
    let held = subscribed(&subscribes);
    let read = path
        .tape(TICKER, "trade", TapeQuery::default())
        .expect("tape");
    let pending = subscribed(&staged(&mut path, &snap(1, 2, vec![bind("bestquote")])));

    let mut all = held.clone();
    all.extend(pending.clone());
    all.sort();
    assert_eq!(unsubscribed(&path.close()), all);
    assert!(path.close().is_empty(), "a second close does nothing");

    let msg = frame(NOW, 1, 1, &trade(UID));
    for (_, sub) in &all {
        assert_eq!(path.route(*sub, &msg), Route::Skip);
    }
    assert!(path.subjects().is_empty());
    assert_eq!(
        path.bindings_applied(),
        version(1, 1),
        "the last version stays"
    );
    assert_eq!(path.apply_bound(), Err(Error::Stopping));
    assert!(path.apply_aborted().is_empty());
    let err = path
        .apply(&snap(1, 3, vec![bind("trade")]))
        .expect_err("closed");
    assert_eq!(err, Error::Stopping);
    assert_eq!(err.to_string(), "mdpath: the session is stopping");
    let mut with_selector = snap(1, 3, Vec::new());
    with_selector
        .selectors
        .push(stsctl::SelectorBinding::default());
    assert_eq!(
        path.apply(&with_selector),
        Err(Error::Selector),
        "selectors are checked first"
    );

    assert_eq!(
        path.tape(TICKER, "trade", TapeQuery::default()),
        Err(Error::Stopping)
    );
    assert_eq!(path.book_snapshot(TICKER), Err(Error::Stopping));
    assert_eq!(
        path.fetch_klines(UID, "Binance", "1m", 1),
        Err(Error::Stopping)
    );
    assert_eq!(path.fetch_open_interest(UID, "Bybit"), Err(Error::Stopping));
    assert_eq!(
        path.on_reply(read.ticket, Err(RequestError::Timeout)),
        None,
        "an in-flight read is dropped"
    );
}

// ---------- subjects (PROTOCOL §9.4.2, E04-D055, E04-D056) ----------

#[test]
fn view_and_fetch_subjects() {
    assert_eq!(
        view_subject("md.v1.f.custom.trade.NOT_THE_TICKER").as_deref(),
        Some("md.v1.view.custom.trade.NOT_THE_TICKER")
    );
    assert_eq!(view_subject("other.subject"), None);
    assert_eq!(view_subject("md.v1.f."), None, "nothing follows the prefix");
    assert_eq!(
        view_subject("x.md.v1.f.a.b.c"),
        None,
        "only a leading prefix"
    );

    assert_eq!(
        fetch_subject("Binance").as_deref(),
        Ok("md.v1.fetch.Binance")
    );
    for venue in [
        "",
        "Binance.Spot",
        "a*",
        "a>",
        "a b",
        "a\tb",
        "a\nb",
        "a\rb",
    ] {
        let err = fetch_subject(venue).expect_err("not a token");
        assert_eq!(err, Error::BadVenue(venue.to_owned()));
    }
    assert_eq!(
        fetch_subject("Binance.Spot").expect_err("dot").to_string(),
        "mdpath: venue \"Binance.Spot\" is not a subject token"
    );
}

// ---------- read requests (E08-D048) ----------

#[test]
fn tape_asks_md_view_and_waits_the_recv_window() {
    let mut path = open(Hooks::ALL);
    commit(&mut path, &snap(1, 1, vec![bind("trade")]));
    let before = md::Cursor {
        conn_epoch: 9,
        seq: 3,
    };
    let request = path
        .tape(
            TICKER,
            "trade",
            TapeQuery {
                limit: 12,
                before: Some(before),
                since_ns: 50,
            },
        )
        .expect("tape");
    assert_eq!(request.kind, ReadKind::Tape);
    assert_eq!(
        request.subject,
        format!("md.v1.view.md-tw-1.trade.{TICKER}")
    );
    assert_eq!(request.wait, WINDOW);
    assert_eq!(
        md::ViewRequest::decode(request.body.as_slice()).expect("body"),
        md::ViewRequest {
            limit: 12,
            before: Some(before),
            since_ns: Some(50),
        }
    );

    let newest = path
        .tape(TICKER, "trade", TapeQuery::default())
        .expect("tape");
    assert_eq!(
        md::ViewRequest::decode(newest.body.as_slice()).expect("body"),
        md::ViewRequest::default(),
        "since_ns 0 sets no bound and no before asks for the newest page"
    );
    assert_ne!(newest.ticket, request.ticket);
}

#[test]
fn book_snapshot_asks_the_orderbook_delta_view() {
    let mut path = open(Hooks::default());
    commit(&mut path, &snap(1, 1, vec![bind(TOPIC_BOOK)]));
    let request = path.book_snapshot(TICKER).expect("book");
    assert_eq!(request.kind, ReadKind::BookSnapshot);
    assert_eq!(
        request.subject,
        "md.v1.view.md-tw-1.orderbook_delta.Binance_Spot_BTCUSDT"
    );
    assert_eq!(request.wait, WINDOW);
    assert!(request.body.is_empty(), "an empty ViewRequest");
}

#[test]
fn fetch_uses_the_venue_the_caller_passes() {
    let mut path = open(Hooks::ALL);
    let klines = path.fetch_klines(UID, "Binance", "1m", 3).expect("klines");
    assert_eq!(klines.kind, ReadKind::Klines);
    assert_eq!(klines.subject, "md.v1.fetch.Binance");
    assert_eq!(klines.wait, WINDOW);
    assert_eq!(
        md::FetchRequest::decode(klines.body.as_slice()).expect("body"),
        md::FetchRequest {
            req: Some(fetch_request::Req::Klines(md::FetchKlines {
                interval: "1m".to_owned(),
                limit: 3,
                uid: UID,
            })),
        }
    );

    let oi = path.fetch_open_interest(7, "Bybit").expect("oi");
    assert_eq!(oi.kind, ReadKind::OpenInterest);
    assert_eq!(oi.subject, "md.v1.fetch.Bybit");
    assert_eq!(
        md::FetchRequest::decode(oi.body.as_slice()).expect("body"),
        md::FetchRequest {
            req: Some(fetch_request::Req::OpenInterest(md::FetchOpenInterest {
                uid: 7
            })),
        }
    );
    assert!(oi.ticket > klines.ticket);

    assert_eq!(
        path.fetch_klines(UID, "Binance.Spot", "1m", 3),
        Err(Error::BadVenue("Binance.Spot".to_owned()))
    );
    assert_eq!(
        path.fetch_open_interest(7, ""),
        Err(Error::BadVenue(String::new()))
    );
}

#[test]
fn bad_reads_fail_before_any_request() {
    let mut path = open(Hooks::ALL);
    commit(
        &mut path,
        &snap(1, 1, vec![bind("trade"), bind_on("aggtrade", "no.prefix")]),
    );
    let query = TapeQuery::default();
    assert_eq!(path.tape(TICKER, "", query), Err(Error::NoFeed));
    assert_eq!(path.tape("", "trade", query), Err(Error::UnknownUid));
    assert_eq!(
        path.tape("Other_Spot_X", "trade", query),
        Err(Error::NoFeed)
    );
    assert_eq!(path.tape(TICKER, "ticker", query), Err(Error::NoFeed));
    assert_eq!(path.tape(TICKER, "aggtrade", query), Err(Error::BadSubject));
    assert_eq!(path.book_snapshot(TICKER), Err(Error::NoFeed));
    assert_eq!(path.book_snapshot(""), Err(Error::UnknownUid));
    assert_eq!(
        Error::NoFeed.to_string(),
        "mdpath: the session has no such feed"
    );
    assert_eq!(
        Error::UnknownUid.to_string(),
        "mdpath: the instrument is not known to this session"
    );
    assert_eq!(
        Error::BadSubject.to_string(),
        "mdpath: feed subject has no md.v1.f. prefix"
    );
    let first = path.tape(TICKER, "trade", query).expect("tape");
    assert_eq!(first.ticket, ReadTicket(1), "failed reads took no ticket");
}

// ---------- read replies (E08-D048) ----------

#[test]
fn replies_are_decoded_for_the_ticket_that_asked() {
    let mut path = open(Hooks::ALL);
    commit(
        &mut path,
        &snap(1, 1, vec![bind("trade"), bind(TOPIC_BOOK)]),
    );

    let page = md::TapePage {
        head: Some(md::Cursor {
            conn_epoch: 7,
            seq: 2,
        }),
        events: vec![md::TapeEvent {
            conn_epoch: 7,
            seq: 2,
            ..Default::default()
        }],
        ..Default::default()
    };
    let tape = path
        .tape(TICKER, "trade", TapeQuery::default())
        .expect("tape");
    let body = md::ViewReply {
        error: String::new(),
        result: Some(view_reply::Result::Tape(page.clone())),
    }
    .encode_to_vec();
    assert_eq!(
        delivered(&mut path, &tape, Ok(&body)),
        Reply::Tape(Ok(page))
    );

    let book = md::BookSnapshot {
        uid: UID,
        conn_epoch: 2,
        seq: 5,
        ..Default::default()
    };
    let snap_req = path.book_snapshot(TICKER).expect("book");
    let body = md::ViewReply {
        error: String::new(),
        result: Some(view_reply::Result::Book(book.clone())),
    }
    .encode_to_vec();
    assert_eq!(
        delivered(&mut path, &snap_req, Ok(&body)),
        Reply::BookSnapshot(Ok(book))
    );

    let klines = md::Klines {
        klines: vec![md::Kline {
            interval: "1m".to_owned(),
            close: 2.0,
            ..Default::default()
        }],
    };
    let kl_req = path.fetch_klines(UID, "Binance", "1m", 1).expect("klines");
    let body = md::FetchReply {
        uid: UID,
        result: Some(fetch_reply::Result::Klines(klines.clone())),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        delivered(&mut path, &kl_req, Ok(&body)),
        Reply::Klines(Ok(klines))
    );

    let oi = md::OpenInterest {
        open_interest: 11.0,
        ..Default::default()
    };
    let oi_req = path.fetch_open_interest(7, "Bybit").expect("oi");
    let body = md::FetchReply {
        uid: 7,
        result: Some(fetch_reply::Result::OpenInterest(oi)),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        delivered(&mut path, &oi_req, Ok(&body)),
        Reply::OpenInterest(Ok(oi))
    );
}

#[test]
fn timeout_and_no_responder_reach_the_callback_as_errors() {
    let mut path = open(Hooks::ALL);
    commit(&mut path, &snap(1, 1, vec![bind("trade")]));

    let tape = path
        .tape(TICKER, "trade", TapeQuery::default())
        .expect("tape");
    let err = read_error(delivered(&mut path, &tape, Err(RequestError::Timeout)));
    assert_eq!(err, ReadError::Timeout { via: Via::View });
    assert_eq!(
        err.to_string(),
        "mdpath: md.view: context deadline exceeded"
    );

    let tape = path
        .tape(TICKER, "trade", TapeQuery::default())
        .expect("tape");
    let err = read_error(delivered(&mut path, &tape, Err(RequestError::NoResponder)));
    assert_eq!(
        err.to_string(),
        "mdpath: md.view: nats: no responders available for request"
    );

    let kl = path.fetch_klines(UID, "Binance", "1m", 1).expect("klines");
    let err = read_error(delivered(&mut path, &kl, Err(RequestError::Timeout)));
    assert_eq!(err, ReadError::Timeout { via: Via::Fetch });
    assert_eq!(
        err.to_string(),
        "mdpath: md.fetch: context deadline exceeded"
    );

    let oi = path.fetch_open_interest(7, "Bybit").expect("oi");
    let err = read_error(delivered(&mut path, &oi, Err(RequestError::NoResponder)));
    assert_eq!(err, ReadError::NoResponder { via: Via::Fetch });
    assert_eq!(
        err.to_string(),
        "mdpath: md.fetch: nats: no responders available for request"
    );

    let oi = path.fetch_open_interest(7, "Bybit").expect("oi");
    let err = read_error(delivered(
        &mut path,
        &oi,
        Err(RequestError::Other("connection closed".to_owned())),
    ));
    assert_eq!(err.to_string(), "mdpath: md.fetch: connection closed");
}

#[test]
fn reply_errors_and_missing_results_are_errors() {
    let mut path = open(Hooks::ALL);
    commit(
        &mut path,
        &snap(1, 1, vec![bind("trade"), bind(TOPIC_BOOK)]),
    );

    let view = |error: &str, result: Option<view_reply::Result>| {
        md::ViewReply {
            error: error.to_owned(),
            result,
        }
        .encode_to_vec()
    };
    let fetch = |error: &str, reason: &str, result: Option<fetch_reply::Result>| {
        md::FetchReply {
            error: error.to_owned(),
            reason: reason.to_owned(),
            uid: UID,
            result,
        }
        .encode_to_vec()
    };

    let book = path.book_snapshot(TICKER).expect("book");
    let err = read_error(delivered(&mut path, &book, Ok(&view("not_synced", None))));
    assert_eq!(err.to_string(), "mdpath: md.view: not_synced");

    let tape = path
        .tape(TICKER, "trade", TapeQuery::default())
        .expect("tape");
    let err = read_error(delivered(&mut path, &tape, Ok(&view("unsupported", None))));
    assert_eq!(err.to_string(), "mdpath: md.view: unsupported");

    let tape = path
        .tape(TICKER, "trade", TapeQuery::default())
        .expect("tape");
    let wrong = view(
        "",
        Some(view_reply::Result::Book(md::BookSnapshot::default())),
    );
    let err = read_error(delivered(&mut path, &tape, Ok(&wrong)));
    assert_eq!(err.to_string(), "mdpath: md.view: empty tape");

    let book = path.book_snapshot(TICKER).expect("book");
    let err = read_error(delivered(&mut path, &book, Ok(&view("", None))));
    assert_eq!(
        err,
        ReadError::Empty {
            via: Via::View,
            what: "book"
        }
    );
    assert_eq!(err.to_string(), "mdpath: md.view: empty book");

    let kl = path.fetch_klines(UID, "Binance", "1m", 1).expect("klines");
    let err = read_error(delivered(
        &mut path,
        &kl,
        Ok(&fetch("rate_limited", "weight", None)),
    ));
    assert_eq!(err.to_string(), "mdpath: md.fetch: rate_limited: weight");

    let kl = path.fetch_klines(UID, "Binance", "1m", 1).expect("klines");
    let err = read_error(delivered(&mut path, &kl, Ok(&fetch("invalid", "", None))));
    assert_eq!(err.to_string(), "mdpath: md.fetch: invalid");

    let kl = path.fetch_klines(UID, "Binance", "1m", 1).expect("klines");
    let other = fetch(
        "",
        "",
        Some(fetch_reply::Result::OpenInterest(
            md::OpenInterest::default(),
        )),
    );
    let err = read_error(delivered(&mut path, &kl, Ok(&other)));
    assert_eq!(err.to_string(), "mdpath: md.fetch: empty klines");

    let oi = path.fetch_open_interest(7, "Bybit").expect("oi");
    let err = read_error(delivered(&mut path, &oi, Ok(&fetch("", "", None))));
    assert_eq!(err.to_string(), "mdpath: md.fetch: empty open interest");

    let tape = path
        .tape(TICKER, "trade", TapeQuery::default())
        .expect("tape");
    let err = read_error(delivered(&mut path, &tape, Ok(&[0xff])));
    assert!(
        matches!(err, ReadError::Decode { via: Via::View, .. }),
        "{err:?}"
    );
    assert!(err.to_string().starts_with("mdpath: md.view: "), "{err}");

    let oi = path.fetch_open_interest(7, "Bybit").expect("oi");
    let err = read_error(delivered(&mut path, &oi, Ok(&[0xff])));
    assert!(
        matches!(
            err,
            ReadError::Decode {
                via: Via::Fetch,
                ..
            }
        ),
        "{err:?}"
    );
}

#[test]
fn each_ticket_is_answered_once() {
    let mut path = open(Hooks::ALL);
    let request = path.fetch_open_interest(7, "Bybit").expect("oi");
    assert!(
        path.on_reply(request.ticket, Err(RequestError::Timeout))
            .is_some()
    );
    assert_eq!(
        path.on_reply(request.ticket, Err(RequestError::Timeout)),
        None
    );
    assert_eq!(path.on_reply(ReadTicket(99), Ok(&[])), None);
}

// ---------- routing and decode (PROTOCOL §9.4.3, E08-D005, E08-D049) ----------

#[test]
fn a_topic_without_a_hook_is_skipped_before_decode() {
    let mut path = open(Hooks {
        trade: true,
        ..Hooks::default()
    });
    let (subscribes, _) = commit(
        &mut path,
        &snap(
            1,
            1,
            vec![
                bind("trade"),
                bind("ticker"),
                bind("kline_1m"),
                bind(TOPIC_BOOK),
            ],
        ),
    );
    let subs = subscribed(&subscribes);
    let garbage = frame(NOW, 1, 1, &[0xff]);

    assert_eq!(
        path.route(sub_of(&subs, &subj("ticker")), &garbage),
        Route::Skip
    );
    assert_eq!(
        path.route(sub_of(&subs, &subj("kline_1m")), &garbage),
        Route::Skip
    );
    assert_eq!(
        path.route(sub_of(&subs, &subj(TOPIC_BOOK)), &garbage),
        Route::Book
    );
    assert_eq!(
        path.route(sub_of(&subs, &subj("trade")), &garbage),
        Route::Md
    );
    assert_eq!(
        path.route(SubId(999), &garbage),
        Route::Skip,
        "unknown subscription"
    );

    match path.hooks().decode(&garbage) {
        Decoded::Log(line) => assert!(line.starts_with("md payload rejected: "), "{line}"),
        other => panic!("garbage decoded: {other:?}"),
    }
}

#[test]
fn a_bad_envelope_goes_to_the_dispatcher_to_be_counted() {
    let mut path = open(Hooks::default());
    let (subscribes, _) = commit(
        &mut path,
        &snap(1, 1, vec![bind("ticker"), bind(TOPIC_BOOK)]),
    );
    let subs = subscribed(&subscribes);
    let mut bad_version = frame(NOW, 1, 1, b"v");
    bad_version[0] = 9;
    let mut reserved = frame(NOW, 1, 1, b"r");
    reserved[3] = 0xab;
    for msg in [vec![0x7f], bad_version, reserved] {
        assert_eq!(
            path.route(sub_of(&subs, &subj("ticker")), &msg),
            Route::Md,
            "a topic without a hook still counts a bad envelope"
        );
        assert_eq!(
            path.route(sub_of(&subs, &subj(TOPIC_BOOK)), &msg),
            Route::Md,
            "a bad book envelope is dropped like any other"
        );
    }
}

#[test]
fn decode_hands_each_topic_to_its_hook() {
    for (topic, event, data) in every_topic() {
        let msg = frame(NOW, 3, 8, &feed(100, event));
        let want = Decoded::Hook(Event {
            md: Md {
                uid: 100,
                md_ts_ns: NOW,
                conn_epoch: 3,
                seq: 8,
            },
            data: data.clone(),
        });
        assert_eq!(Hooks::ALL.decode(&msg), want, "{topic:?} with every hook");
        let own = with(Hooks::default(), topic, true);
        assert_eq!(own.decode(&msg), want, "{topic:?} with its own hook");
        assert!(own.has(topic));
        assert_eq!(data.topic(), topic);
        let others = with(Hooks::ALL, topic, false);
        assert_eq!(others.decode(&msg), Decoded::Skip, "{topic:?} hook is off");
    }
}

#[test]
fn feed_topics_map_to_hooks() {
    let table = [
        ("bestquote", Some(Topic::BestQuote)),
        ("trade", Some(Topic::Trade)),
        ("aggtrade", Some(Topic::AggTrade)),
        ("kline_1m", Some(Topic::Kline)),
        ("kline_1mo", Some(Topic::Kline)),
        ("ticker", Some(Topic::Ticker)),
        ("liquidation", Some(Topic::Liquidation)),
        ("funding_rate", Some(Topic::FundingRate)),
        ("open_interest", Some(Topic::OpenInterest)),
        ("greeks", Some(Topic::Greeks)),
        ("mark_price", Some(Topic::MarkPrice)),
        ("kline_", None),
        (TOPIC_BOOK, None),
        ("unknown", None),
    ];
    for (feed_topic, want) in table {
        assert_eq!(Topic::of_feed(feed_topic), want, "{feed_topic}");
        assert_eq!(Hooks::ALL.wants(feed_topic), want.is_some(), "{feed_topic}");
        assert!(!Hooks::default().wants(feed_topic), "{feed_topic}");
    }
}

#[test]
fn decode_skips_book_deltas_and_empty_events() {
    let msg = frame(NOW, 1, 1, &delta(UID));
    assert_eq!(
        Hooks::ALL.decode(&msg),
        Decoded::Skip,
        "orderbook_delta is the book path's"
    );

    let empty = md::FeedEvent {
        uid: UID,
        event: None,
    }
    .encode_to_vec();
    assert_eq!(Hooks::ALL.decode(&frame(NOW, 1, 1, &empty)), Decoded::Skip);

    assert_eq!(
        Hooks::ALL.decode(&[1, 0, 0]),
        Decoded::Log(
            "md envelope rejected: envelope: message shorter than 40 bytes: 3 bytes".to_owned()
        )
    );
}

// ---------- the dispatcher (PROTOCOL §9.5, E08-D048, E08-D049) ----------

#[test]
fn routed_bytes_fit_the_dispatcher() {
    let mut path = open(Hooks {
        trade: true,
        ..Hooks::default()
    });
    let (subscribes, _) = commit(
        &mut path,
        &snap(1, 1, vec![bind("trade"), bind(TOPIC_BOOK)]),
    );
    let subs = subscribed(&subscribes);
    let trade_sub = sub_of(&subs, &subj("trade"));
    let book_sub = sub_of(&subs, &subj(TOPIC_BOOK));

    let (feed_tx, feed_rx) = mpsc::channel::<Decoded>();
    let (book_tx, book_rx) = mpsc::channel::<Vec<u8>>();
    let (td_tx, td_rx) = mpsc::channel::<Delivery>();
    let logs = Arc::new(Mutex::new(Vec::<String>::new()));
    let hooks = path.hooks();
    let disp = Dispatcher::new(dispatch::Config {
        recv_window: WINDOW,
        now_ns: NOW,
        ring_bytes: 4096,
        feed: Some(Box::new(move |msg| {
            let _ = feed_tx.send(hooks.decode(msg));
        })),
        book: Some(Box::new(move |msg| {
            let _ = book_tx.send(msg.to_vec());
        })),
        log: Some({
            let logs = Arc::clone(&logs);
            Arc::new(move |line: &str| {
                logs.lock().expect("logs").push(line.to_owned());
            })
        }),
        ..Default::default()
    });

    let push = |sub: SubId, msg: &[u8]| match path.route(sub, msg) {
        Route::Md => disp.push_md(msg).expect("push md"),
        Route::Book => disp.push_book(msg).expect("push book"),
        Route::Skip => {}
    };
    let mut bad_version = frame(NOW, 1, 1, b"v");
    bad_version[0] = 9;
    push(trade_sub, &[0x7f]);
    push(trade_sub, &bad_version);
    push(trade_sub, &frame(NOW - 2 * SEC_NS, 1, 1, &trade(UID)));
    push(book_sub, &frame(NOW - 2 * SEC_NS, 4, 6, &delta(UID)));
    push(trade_sub, &frame(NOW, 1, 2, &trade(UID)));

    let mut reads = open(Hooks::ALL);
    let request = reads.fetch_open_interest(7, "Bybit").expect("oi");
    let delivery = reads
        .on_reply(request.ticket, Err(RequestError::NoResponder))
        .expect("delivery");
    disp.push_td(move || {
        let _ = td_tx.send(delivery);
    })
    .expect("push td");

    let ended = start(&disp);
    let delivery = recv(&td_rx);
    assert_eq!(delivery.ticket, request.ticket);
    assert_eq!(
        delivery.reply,
        Reply::OpenInterest(Err(ReadError::NoResponder { via: Via::Fetch }))
    );
    let book = recv(&book_rx);
    assert_eq!(
        crate::dispatch::envelope::parse(&book).expect("header").seq,
        6,
        "a stale delta still reaches the book path"
    );
    match recv(&feed_rx) {
        Decoded::Hook(event) => {
            assert_eq!(event.md.seq, 2, "only the fresh trade is a hook");
            assert_eq!(event.md.uid, UID);
            assert!(matches!(event.data, Data::Trade(_)));
        }
        other => panic!("feed {other:?}"),
    }
    assert_eq!(
        disp.stats().md.dropped,
        3,
        "two bad envelopes and one stale trade; the stale delta is not a drop"
    );
    let rejected = logs
        .lock()
        .expect("logs")
        .iter()
        .filter(|line| line.starts_with("md envelope rejected: "))
        .count();
    assert_eq!(rejected, 2);

    disp.exit();
    assert_eq!(recv(&ended), RunEnd::Done);
    assert_eq!(disp.on_stop(&StopRequest::new(None)), Ok(()));
    disp.wait_exited();
    assert!(feed_rx.try_recv().is_err(), "nothing else was delivered");
}

fn start(disp: &Dispatcher) -> Receiver<RunEnd> {
    let (tx, rx) = mpsc::channel();
    let running = disp.clone();
    thread::spawn(move || {
        let _ = tx.send(running.run());
    });
    rx
}

#[track_caller]
fn recv<T>(rx: &Receiver<T>) -> T {
    rx.recv_timeout(FUSE).expect("nothing arrived")
}
