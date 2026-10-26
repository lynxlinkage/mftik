//! Behavior of the OMS, the ledger, and recon watermarks.
//!
//! `NOW_MS` is the caller's clock. Backoff is an effect the test fires by
//! calling `on_backoff`. Nothing sleeps.

use std::collections::HashMap;
use std::time::Duration;

use mftik_proto::wire::mftiko::{common::v1 as common, td::v1 as td};

use super::{
    AccountDecl, Bound, ClientOrderIdError, ClientOrderIds, Config, Effect, Error, HOLD_CAP, Hook,
    NewOrder, OpenError, Origin, Path, REASON_HOLD_FULL, REASON_OFFLINE, RECON_BACKOFF_MAX,
    RECON_BACKOFF_MIN, ReconOutcome, RejectedRequest, RoundOutcome, RoundTicket, evt_subject,
    order_subject, view_subject,
};
use crate::cloid::{self, EPOCH_MS};

const NOW_MS: i64 = EPOCH_MS + 1_000;
const WINDOW: Duration = Duration::from_millis(500);

struct Mint {
    seq: u32,
    calls: u32,
}

impl Mint {
    fn new() -> Self {
        Self { seq: 0, calls: 0 }
    }
}

impl ClientOrderIds for Mint {
    fn next(
        &mut self,
        algo_ids: &[u32],
        algo_id: u32,
        now_ms: i64,
    ) -> Result<String, ClientOrderIdError> {
        if !algo_ids.contains(&algo_id) {
            return Err(ClientOrderIdError::AlgoNotDeclared);
        }
        self.calls += 1;
        self.seq += 1;
        cloid::encode(algo_id, now_ms, self.seq)
            .map_err(|err| ClientOrderIdError::Other(err.to_string()))
    }
}

struct Script {
    seqs: Vec<u32>,
    at: usize,
}

impl ClientOrderIds for Script {
    fn next(
        &mut self,
        algo_ids: &[u32],
        algo_id: u32,
        now_ms: i64,
    ) -> Result<String, ClientOrderIdError> {
        if !algo_ids.contains(&algo_id) {
            return Err(ClientOrderIdError::AlgoNotDeclared);
        }
        let seq = self.seqs[self.at];
        self.at += 1;
        cloid::encode(algo_id, now_ms, seq)
            .map_err(|err| ClientOrderIdError::Other(err.to_string()))
    }
}

fn decl(api_id: i64, algo_ids: &[u32]) -> AccountDecl {
    AccountDecl {
        api_id,
        algo_ids: algo_ids.to_vec(),
    }
}

fn open(accounts: Vec<AccountDecl>) -> Path {
    Path::open(Config {
        session_id: "c0ffee".to_owned(),
        recv_window: WINDOW,
        accounts,
    })
    .unwrap()
}

fn listen(path: &mut Path) -> Vec<i64> {
    let effects = path.listen().unwrap();
    let Effect::SubscribeEvt { api_ids } = &effects[0] else {
        panic!("listen did not subscribe: {effects:?}");
    };
    let api_ids = api_ids.clone();
    assert!(matches!(path.listen_bound().unwrap(), Bound::Listening));
    assert!(path.listen().unwrap().is_empty());
    api_ids
}

fn order_for(api_id: i64, algo_id: u32, uid: u32) -> NewOrder {
    NewOrder {
        api_id,
        algo_id,
        uid,
        side: common::Side::Buy,
        order_type: common::OrderType::Limit,
        tif: common::TimeInForce::Gtc,
        qty: Some(2.0),
        quote_qty: None,
        price: Some(10.0),
        recv_window_ms: 250,
        reduce_only: false,
        params: HashMap::new(),
    }
}

fn submit(path: &mut Path, mint: &mut dyn ClientOrderIds, order: NewOrder) -> (String, Effect) {
    let submitted = path.submit(order, NOW_MS, mint).unwrap();
    assert_eq!(submitted.effects.len(), 1);
    let effect = submitted.effects.into_iter().next().unwrap();
    (submitted.client_order_id, effect)
}

fn ticket(effect: &Effect) -> RoundTicket {
    match effect {
        Effect::SendOrder { ticket, .. } => *ticket,
        other => panic!("not an order send: {other:?}"),
    }
}

fn send_order(effect: &Effect) -> &td::OrderRequest {
    match effect {
        Effect::SendOrder { request, .. } => request,
        other => panic!("not an order send: {other:?}"),
    }
}

fn ask(path: &mut Path, api_id: i64) -> u64 {
    let effects = path.request_recon(api_id).unwrap();
    assert_eq!(effects.len(), 1, "{effects:?}");
    match &effects[0] {
        Effect::SendRecon {
            generation,
            subject,
            wait,
            api_id: got,
            request,
        } => {
            assert_eq!(*got, api_id);
            assert_eq!(subject, &view_subject(api_id));
            assert_eq!(*wait, WINDOW);
            assert!(matches!(request.req, Some(td::view_request::Req::Recon(_))));
            *generation
        }
        other => panic!("not a recon send: {other:?}"),
    }
}

fn view(
    inc: u32,
    seq: u64,
    batch: u32,
    orders: Vec<td::Order>,
    positions: Vec<td::Position>,
    balances: Vec<td::Balance>,
) -> td::ViewReply {
    td::ViewReply {
        incarnation: inc,
        seq,
        cancel_batch: batch,
        reply: Some(td::view_reply::Reply::Recon(td::Recon {
            open_orders: orders,
            positions,
            balances,
            reason: i32::from(td::ReconReason::Reconnect),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn bare_view(inc: u32, seq: u64, batch: u32) -> td::ViewReply {
    view(inc, seq, batch, Vec::new(), Vec::new(), Vec::new())
}

fn row(id: &str, status: td::OrderStatus, uid: u32, qty: f64) -> td::Order {
    td::Order {
        client_order_id: id.to_owned(),
        status: i32::from(status),
        uid,
        qty,
        ..Default::default()
    }
}

fn event(inc: u32, seq: u64, body: Option<td::account_event::Event>) -> td::AccountEvent {
    td::AccountEvent {
        seq,
        source: Some(common::WorkerRef {
            incarnation: inc,
            ..Default::default()
        }),
        event: body,
    }
}

fn order_event(
    inc: u32,
    seq: u64,
    order: td::Order,
    cause: td::OrderCause,
    code: i32,
    reason: &str,
) -> td::AccountEvent {
    event(
        inc,
        seq,
        Some(td::account_event::Event::OrderUpdate(td::OrderUpdate {
            order: Some(order),
            cause: i32::from(cause),
            code,
            reason: reason.to_owned(),
        })),
    )
}

fn status_is(order: &td::Order, status: td::OrderStatus) -> bool {
    order.status == i32::from(status)
}

fn names(effects: &[Effect]) -> Vec<&'static str> {
    effects
        .iter()
        .map(|effect| match effect {
            Effect::Hook(hook) => match hook.as_ref() {
                Hook::Order(_) => "order",
                Hook::Fill(_) => "fill",
                Hook::Position(_) => "position",
                Hook::Balance(_) => "balance",
                Hook::Recon(_) => "recon",
                Hook::Reject(_) => "reject",
            },
            Effect::SendRecon { .. } => "recon_send",
            Effect::SendOrder { .. } => "order_send",
            Effect::ArmBackoff { .. } => "backoff",
            Effect::SubscribeEvt { .. } => "sub",
            Effect::UnsubscribeEvt { .. } => "unsub",
            Effect::Fail { .. } => "fail",
        })
        .collect()
}

/// Run each hook, then `end_hook`, which may append the next effect.
fn drive(
    path: &mut Path,
    mut effects: Vec<Effect>,
    mut on_hook: impl FnMut(&mut Path, &Hook),
) -> Vec<Effect> {
    let mut index = 0;
    while index < effects.len() {
        if let Effect::Hook(hook) = &effects[index] {
            let hook = (**hook).clone();
            on_hook(path, &hook);
            if let Some(next) = path.end_hook() {
                effects.insert(index + 1, next);
            }
        }
        index += 1;
    }
    effects
}

fn baseline(path: &mut Path, api_id: i64, inc: u32, seq: u64, batch: u32) {
    let generation = ask(path, api_id);
    let effects = path.on_recon_round(
        api_id,
        generation,
        ReconOutcome::Reply(bare_view(inc, seq, batch)),
    );
    let effects = drive(path, effects, |_, _| {});
    assert_eq!(names(&effects), ["recon"]);
}

fn id_of(algo: u32, seq: u32) -> String {
    cloid::encode(algo, NOW_MS, seq).unwrap()
}

#[test]
fn subjects_and_backoff_bounds_match_the_protocol() {
    assert_eq!(order_subject(7), "td.order.7");
    assert_eq!(view_subject(7), "td.view.7");
    assert_eq!(evt_subject(7), "td.evt.7");
    assert_eq!(HOLD_CAP, 65_536);
    assert_eq!(REASON_HOLD_FULL, "td buffer full");
    assert_eq!(REASON_OFFLINE, "account offline");
    assert_eq!(RECON_BACKOFF_MIN, Duration::from_secs(1));
    assert_eq!(RECON_BACKOFF_MAX, Duration::from_secs(30));
    assert_eq!(Origin::FromTd as i32, 1);
    assert_eq!(Origin::LocalReject as i32, 2);
    assert_eq!(Origin::Timeout as i32, 3);
    assert_eq!(RejectedRequest::Cancel as i32, 1);
    assert_eq!(RejectedRequest::BatchCancel as i32, 2);
}

#[test]
fn open_rejects_a_bad_config() {
    let window = WINDOW;
    let bad = |session_id: &str, recv_window: Duration, accounts: Vec<AccountDecl>| {
        let Err(error) = Path::open(Config {
            session_id: session_id.to_owned(),
            recv_window,
            accounts,
        }) else {
            panic!("a bad config was accepted");
        };
        error
    };
    assert_eq!(bad("", window, vec![]), OpenError::EmptySessionId);
    assert_eq!(bad("c0ffee", Duration::ZERO, vec![]), OpenError::RecvWindow);
    assert_eq!(
        bad("c0ffee", window, vec![decl(0, &[1])]),
        OpenError::ApiIdNotPositive { api_id: 0 }
    );
    assert_eq!(
        bad("c0ffee", window, vec![decl(-1, &[1])]),
        OpenError::ApiIdNotPositive { api_id: -1 }
    );
    assert_eq!(
        bad("c0ffee", window, vec![decl(7, &[1]), decl(7, &[2])]),
        OpenError::DuplicateApiId { api_id: 7 }
    );
    let path = open(vec![]);
    assert!(path.open_orders(7, 0).is_empty());
    assert!(path.order(7, "x").is_none());
    assert!(path.position(7, 1).is_none());
    assert!(path.balance(7, "USDT").is_none());
}

#[test]
fn trading_waits_until_listen_and_a_failed_subscribe_can_be_retried() {
    let mut path = open(vec![decl(7, &[1]), decl(8, &[2])]);
    let mut mint = Mint::new();
    assert_eq!(
        path.submit(order_for(7, 1, 1001), NOW_MS, &mut mint)
            .unwrap_err(),
        Error::NotListening
    );
    assert_eq!(
        path.cancel(7, &id_of(1, 1)).unwrap_err(),
        Error::NotListening
    );
    assert_eq!(
        path.batch_cancel(7, 1, &[]).unwrap_err(),
        Error::NotListening
    );
    assert_eq!(path.recon_all().unwrap_err(), Error::NotListening);
    assert_eq!(path.request_recon(7).unwrap_err(), Error::NotListening);
    assert_eq!(mint.calls, 0);

    let started = path.listen().unwrap();
    assert!(matches!(started[0], Effect::SubscribeEvt { .. }));
    assert_eq!(path.listen().unwrap_err(), Error::ListenInProgress);
    // The subscribe has not flushed. An event is held, then a failed flush drops it.
    assert!(path.on_account_event(7, event(1, 1, None)).is_empty());
    assert_eq!(path.held(), 1);
    path.listen_aborted();
    assert_eq!(path.held(), 0);
    path.listen_aborted();
    assert_eq!(path.listen_bound().unwrap_err(), Error::ListenNotInProgress);

    let ids = listen(&mut path);
    assert_eq!(ids, vec![7, 8]);
    assert_eq!(
        path.submit(order_for(99, 1, 1001), NOW_MS, &mut mint)
            .unwrap_err(),
        Error::UnknownAccount
    );
    assert_eq!(
        path.submit(order_for(7, 9, 1001), NOW_MS, &mut mint)
            .unwrap_err(),
        Error::AlgoNotDeclared
    );
    assert_eq!(mint.calls, 0);
    assert_eq!(
        path.cancel(7, "manual").unwrap_err(),
        Error::AlgoNotDeclared
    );
    assert_eq!(
        path.cancel(7, &id_of(2, 1)).unwrap_err(),
        Error::AlgoNotDeclared
    );
    assert_eq!(
        path.batch_cancel(7, 9, &[]).unwrap_err(),
        Error::AlgoNotDeclared
    );
    assert_eq!(path.request_recon(99).unwrap_err(), Error::UnknownAccount);
    assert_eq!(
        path.batch_cancel(7, 1, &[]).unwrap_err(),
        Error::NoCancelBatch
    );
}

#[test]
fn close_unsubscribes_and_drops_an_in_flight_round() {
    let mut path = open(vec![decl(7, &[1])]);
    let mut mint = Mint::new();
    listen(&mut path);
    baseline(&mut path, 7, 1, 0, 4);
    let (id, effect) = submit(&mut path, &mut mint, order_for(7, 1, 1001));
    let closed = path.close();
    match &closed[..] {
        [Effect::UnsubscribeEvt { api_ids }] => assert_eq!(api_ids, &vec![7]),
        other => panic!("{other:?}"),
    }
    assert!(
        path.on_order_round(ticket(&effect), RoundOutcome::NoResponder)
            .is_empty()
    );
    assert!(status_is(
        &path.order(7, &id).unwrap(),
        td::OrderStatus::PendingNew
    ));
    assert_eq!(
        path.submit(order_for(7, 1, 1001), NOW_MS, &mut mint)
            .unwrap_err(),
        Error::Stopping
    );
    assert!(path.on_backoff(7).is_empty());
    assert!(path.on_account_event(7, event(1, 1, None)).is_empty());
}

#[test]
fn close_during_subscribe_unsubscribes_before_the_flush() {
    let mut path = open(vec![decl(7, &[1]), decl(8, &[1])]);
    let _ = path.listen().unwrap();
    let closed = path.close();
    match &closed[..] {
        [Effect::UnsubscribeEvt { api_ids }] => assert_eq!(api_ids, &vec![7, 8]),
        other => panic!("{other:?}"),
    }
    assert_eq!(path.listen_bound().unwrap_err(), Error::ListenNotInProgress);
    assert_eq!(path.listen().unwrap_err(), Error::Stopping);
}

#[test]
fn submit_writes_pending_new_before_the_send() {
    let mut path = open(vec![decl(7, &[1])]);
    let mut mint = Mint::new();
    listen(&mut path);
    baseline(&mut path, 7, 3, 5, 0);
    let mut order = order_for(7, 1, 1001);
    order.reduce_only = true;
    order.params.insert("post".to_owned(), "1".to_owned());
    order.quote_qty = Some(4.0);
    let (id, effect) = submit(&mut path, &mut mint, order);
    let stored = path.order(7, &id).unwrap();
    assert!(status_is(&stored, td::OrderStatus::PendingNew));
    assert_eq!(stored.uid, 1001);
    assert_eq!(stored.qty, 2.0);
    assert_eq!(stored.quote_qty, Some(4.0));
    assert_eq!(stored.price, Some(10.0));
    assert_eq!(stored.side, i32::from(common::Side::Buy));
    let mut tampered = stored.clone();
    tampered.qty = -1.0;
    assert_eq!(path.order(7, &id).unwrap().qty, 2.0);
    assert_eq!(path.open_orders(7, 1).len(), 1);
    assert_eq!(path.open_orders(7, 0).len(), 1);
    assert!(path.open_orders(7, 2).is_empty());

    match &effect {
        Effect::SendOrder {
            api_id,
            subject,
            request,
            wait,
            ..
        } => {
            assert_eq!(*api_id, 7);
            assert_eq!(subject, "td.order.7");
            assert_eq!(*wait, WINDOW);
            let Some(td::order_request::Req::Submit(submit)) = &request.req else {
                panic!("not a submit");
            };
            assert_eq!(submit.session_id, "c0ffee");
            assert_eq!(submit.client_order_id, id);
            assert_eq!(submit.uid, 1001);
            assert_eq!(submit.qty, Some(2.0));
            assert_eq!(submit.quote_qty, Some(4.0));
            assert_eq!(submit.price, Some(10.0));
            assert_eq!(submit.recv_window_ms, 250);
            assert!(submit.reduce_only);
            assert_eq!(submit.params.get("post").map(String::as_str), Some("1"));
            assert_eq!(submit.tif, i32::from(common::TimeInForce::Gtc));
        }
        other => panic!("{other:?}"),
    }

    assert!(
        path.on_order_round(
            ticket(&effect),
            RoundOutcome::Reply(td::OrderReply {
                accepted: true,
                ..Default::default()
            }),
        )
        .is_empty()
    );
    assert!(status_is(
        &path.order(7, &id).unwrap(),
        td::OrderStatus::PendingNew
    ));
    assert_eq!(path.held(), 0);
}

#[test]
fn nan_is_absent_and_infinity_is_a_value() {
    let mut path = open(vec![decl(7, &[1])]);
    let mut mint = Mint::new();
    listen(&mut path);
    baseline(&mut path, 7, 1, 0, 1);
    let mut order = order_for(7, 1, 1001);
    order.qty = Some(f64::NAN);
    order.quote_qty = Some(f64::NAN);
    order.price = Some(f64::INFINITY);
    let (id, effect) = submit(&mut path, &mut mint, order);
    let stored = path.order(7, &id).unwrap();
    assert_eq!(stored.qty, 0.0);
    assert!(stored.quote_qty.is_none());
    assert_eq!(stored.price, Some(f64::INFINITY));
    let Some(td::order_request::Req::Submit(submit)) = &send_order(&effect).req else {
        panic!("not a submit");
    };
    assert!(submit.qty.is_none());
    assert!(submit.quote_qty.is_none());
    assert_eq!(submit.price, Some(f64::INFINITY));
}

#[test]
fn a_local_reject_is_visible_only_for_the_hook() {
    let mut path = open(vec![decl(7, &[1])]);
    let mut mint = Mint::new();
    listen(&mut path);
    baseline(&mut path, 7, 1, 0, 1);
    let (id, effect) = submit(&mut path, &mut mint, order_for(7, 1, 1001));
    let effects = path.on_order_round(
        ticket(&effect),
        RoundOutcome::Reply(td::OrderReply {
            accepted: false,
            code: i32::from(td::RejectCode::InvalidOrder),
            reason: "nope".to_owned(),
            ..Default::default()
        }),
    );
    let mut during = false;
    let effects = drive(&mut path, effects, |path, hook| {
        let Hook::Order(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.origin, Origin::LocalReject);
        assert_eq!(event.code, 106);
        assert_eq!(event.reason, "nope");
        assert_eq!(event.td.api_id, 7);
        assert_eq!(event.td.incarnation, 0);
        assert_eq!(event.td.seq, 0);
        assert_eq!(event.cause, i32::from(td::OrderCause::Unspecified));
        let seen = path.order(7, &id).unwrap();
        during = status_is(&seen, td::OrderStatus::Rejected) && path.open_orders(7, 0).is_empty();
    });
    assert_eq!(names(&effects), ["order"]);
    assert!(during);
    assert!(path.order(7, &id).is_none());
}

#[test]
fn no_responder_is_account_offline() {
    let mut path = open(vec![decl(7, &[1])]);
    let mut mint = Mint::new();
    listen(&mut path);
    baseline(&mut path, 7, 1, 0, 1);
    let (id, effect) = submit(&mut path, &mut mint, order_for(7, 1, 1001));
    let effects = path.on_order_round(ticket(&effect), RoundOutcome::NoResponder);
    let effects = drive(&mut path, effects, |_, hook| {
        let Hook::Order(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.origin, Origin::LocalReject);
        assert_eq!(event.code, i32::from(td::RejectCode::AccountOffline));
        assert_eq!(event.reason, REASON_OFFLINE);
        assert_eq!(event.td.seq, 0);
    });
    assert_eq!(names(&effects), ["order"]);
    assert!(path.order(7, &id).is_none());
}

#[test]
fn a_submit_timeout_becomes_unknown_and_is_not_retried() {
    let mut path = open(vec![decl(7, &[1])]);
    let mut mint = Mint::new();
    listen(&mut path);
    baseline(&mut path, 7, 1, 0, 1);
    let (id, effect) = submit(&mut path, &mut mint, order_for(7, 1, 1001));
    let effects = path.on_order_round(ticket(&effect), RoundOutcome::NoAnswer);
    let effects = drive(&mut path, effects, |path, hook| {
        let Hook::Order(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.origin, Origin::Timeout);
        assert_eq!(event.code, 0);
        assert!(event.reason.is_empty());
        assert_eq!(event.td.api_id, 7);
        assert!(status_is(
            &path.order(7, &id).unwrap(),
            td::OrderStatus::Unknown
        ));
        assert_eq!(path.open_orders(7, 0).len(), 1);
    });
    assert_eq!(names(&effects), ["order"]);
    let stored = path.order(7, &id).unwrap();
    assert!(status_is(&stored, td::OrderStatus::Unknown));
    assert_eq!(path.open_orders(7, 1).len(), 1);
    // A later update still replaces the row. The timeout is not queried again:
    // the only send is the original one.
    let effects = path.on_account_event(
        7,
        order_event(
            1,
            1,
            row(&id, td::OrderStatus::Rejected, 1001, 2.0),
            td::OrderCause::Rejected,
            120,
            "gone",
        ),
    );
    drive(&mut path, effects, |_, _| {});
    assert!(path.order(7, &id).is_none());
}

#[test]
fn an_update_replaces_the_whole_row_and_blocks_a_late_reject() {
    let mut path = open(vec![decl(7, &[1])]);
    let mut mint = Mint::new();
    listen(&mut path);
    baseline(&mut path, 7, 3, 5, 1);
    let (id, effect) = submit(&mut path, &mut mint, order_for(7, 1, 1001));
    let mut updated = row(&id, td::OrderStatus::Accepted, 1001, 9.0);
    updated.order_id = "ex-1".to_owned();
    updated.filled_qty = 0.5;
    updated.cancel_sent_ns = 5;
    let effects = path.on_account_event(
        7,
        order_event(3, 6, updated, td::OrderCause::Accepted, 0, ""),
    );
    let mut during_open = 0;
    drive(&mut path, effects, |path, hook| {
        let Hook::Order(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.origin, Origin::FromTd);
        assert_eq!(event.td.incarnation, 3);
        assert_eq!(event.td.seq, 6);
        assert_eq!(event.cause, i32::from(td::OrderCause::Accepted));
        let seen = path.order(7, &id).unwrap();
        assert!(status_is(&seen, td::OrderStatus::Accepted));
        assert_eq!(seen.qty, 9.0);
        assert!(seen.price.is_none());
        assert_eq!(seen.order_id, "ex-1");
        during_open = path.open_orders(7, 0).len();
    });
    assert_eq!(during_open, 1);
    let stored = path.order(7, &id).unwrap();
    assert_eq!(stored.filled_qty, 0.5);
    assert_eq!(stored.cancel_sent_ns, 5);
    assert!(stored.price.is_none());

    assert!(
        path.on_order_round(
            ticket(&effect),
            RoundOutcome::Reply(td::OrderReply {
                code: 106,
                reason: "late".to_owned(),
                ..Default::default()
            }),
        )
        .is_empty()
    );
    assert!(status_is(
        &path.order(7, &id).unwrap(),
        td::OrderStatus::Accepted
    ));
}

#[test]
fn terminal_orders_are_visible_in_the_hook_and_then_gone() {
    let mut path = open(vec![decl(7, &[1, 2])]);
    listen(&mut path);
    baseline(&mut path, 7, 1, 0, 1);
    let own = id_of(1, 1);
    let other = id_of(2, 1);
    let mut seq = 1_u64;
    for (nth, id) in [own.as_str(), other.as_str()].into_iter().enumerate() {
        let effects = path.on_account_event(
            7,
            order_event(
                1,
                seq,
                row(id, td::OrderStatus::Accepted, 1001, 1.0),
                td::OrderCause::Accepted,
                0,
                "",
            ),
        );
        seq += 1;
        let mut open_during = 0;
        drive(&mut path, effects, |path, _| {
            open_during = path.open_orders(7, 0).len();
            assert!(path.order(7, id).is_some());
        });
        assert_eq!(open_during, nth + 1);
    }
    // Algo 2 is declared, so both rows are live. A partial fill stays live.
    let effects = path.on_account_event(
        7,
        order_event(
            1,
            seq,
            row(&own, td::OrderStatus::PartiallyFilled, 1001, 1.0),
            td::OrderCause::Filled,
            0,
            "",
        ),
    );
    seq += 1;
    drive(&mut path, effects, |path, _| {
        assert_eq!(path.open_orders(7, 1).len(), 1);
    });
    assert!(status_is(
        &path.order(7, &own).unwrap(),
        td::OrderStatus::PartiallyFilled
    ));

    for status in [
        td::OrderStatus::Filled,
        td::OrderStatus::Cancelled,
        td::OrderStatus::Rejected,
        td::OrderStatus::Expired,
    ] {
        let id = id_of(1, seq as u32);
        let effects = path.on_account_event(
            7,
            order_event(
                1,
                seq,
                row(&id, td::OrderStatus::Accepted, 1001, 1.0),
                td::OrderCause::Accepted,
                0,
                "",
            ),
        );
        seq += 1;
        drive(&mut path, effects, |_, _| {});
        let effects = path.on_account_event(
            7,
            order_event(
                1,
                seq,
                row(&id, status, 1001, 1.0),
                td::OrderCause::Filled,
                0,
                "",
            ),
        );
        seq += 1;
        let mut during = false;
        drive(&mut path, effects, |path, hook| {
            let Hook::Order(event) = hook else {
                panic!("{hook:?}");
            };
            during = status_is(&event.order, status)
                && status_is(&path.order(7, &id).unwrap(), status)
                && path
                    .open_orders(7, 0)
                    .iter()
                    .all(|order| order.client_order_id != id);
        });
        assert!(during, "{status:?}");
        assert!(path.order(7, &id).is_none(), "{status:?}");
    }

    let unknown = id_of(1, 40);
    let effects = path.on_account_event(
        7,
        order_event(
            1,
            seq,
            row(&unknown, td::OrderStatus::Unknown, 1001, 1.0),
            td::OrderCause::OutcomeUnknown,
            0,
            "",
        ),
    );
    drive(&mut path, effects, |path, _| {
        assert!(
            path.open_orders(7, 1)
                .iter()
                .any(|order| order.client_order_id == unknown)
        );
    });
    assert!(status_is(
        &path.order(7, &unknown).unwrap(),
        td::OrderStatus::Unknown
    ));
    // The other algo's accepted order is still there. Algo 0 lists both declared algos.
    assert!(path.open_orders(7, 0).len() >= 2);
    assert_eq!(path.open_orders(7, 2).len(), 1);
}

#[test]
fn cancel_reject_and_timeout_leave_the_order() {
    let mut path = open(vec![decl(7, &[1])]);
    listen(&mut path);
    baseline(&mut path, 7, 1, 0, 1);
    let id = id_of(1, 3);
    let effects = path.on_account_event(
        7,
        order_event(
            1,
            1,
            row(&id, td::OrderStatus::Accepted, 1001, 2.0),
            td::OrderCause::Accepted,
            0,
            "",
        ),
    );
    drive(&mut path, effects, |_, _| {});

    let missing = path.cancel(7, &id_of(1, 9)).unwrap();
    match send_order(&missing[0]) {
        td::OrderRequest {
            req: Some(td::order_request::Req::Cancel(cancel)),
            ..
        } => {
            assert_eq!(cancel.uid, 0);
            assert_eq!(cancel.client_order_id, id_of(1, 9));
            assert_eq!(cancel.recv_window_ms, 0);
            assert_eq!(cancel.session_id, "c0ffee");
        }
        other => panic!("{other:?}"),
    }

    let effects = path.cancel(7, &id).unwrap();
    match send_order(&effects[0]) {
        td::OrderRequest {
            req: Some(td::order_request::Req::Cancel(cancel)),
            ..
        } => {
            assert_eq!(cancel.uid, 1001);
            assert_eq!(cancel.client_order_id, id);
        }
        other => panic!("{other:?}"),
    }
    assert!(
        path.on_order_round(ticket(&effects[0]), RoundOutcome::NoAnswer)
            .is_empty()
    );
    assert!(status_is(
        &path.order(7, &id).unwrap(),
        td::OrderStatus::Accepted
    ));

    let effects = path.cancel(7, &id).unwrap();
    let effects = path.on_order_round(
        ticket(&effects[0]),
        RoundOutcome::Reply(td::OrderReply {
            code: i32::from(td::RejectCode::OrderNotOpen),
            reason: "not open".to_owned(),
            ..Default::default()
        }),
    );
    let effects = drive(&mut path, effects, |path, hook| {
        let Hook::Reject(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.request, RejectedRequest::Cancel);
        assert_eq!(event.client_order_ids, vec![id.clone()]);
        assert_eq!(event.code, 107);
        assert_eq!(event.reason, "not open");
        assert_eq!(event.td.api_id, 7);
        assert_eq!(event.td.seq, 0);
        assert!(status_is(
            &path.order(7, &id).unwrap(),
            td::OrderStatus::Accepted
        ));
    });
    assert_eq!(names(&effects), ["reject"]);
    assert!(status_is(
        &path.order(7, &id).unwrap(),
        td::OrderStatus::Accepted
    ));

    let effects = path.on_account_event(
        7,
        order_event(
            1,
            2,
            row(&id, td::OrderStatus::Cancelled, 1001, 2.0),
            td::OrderCause::Cancelled,
            0,
            "",
        ),
    );
    drive(&mut path, effects, |_, _| {});
    assert!(path.order(7, &id).is_none());
}

#[test]
fn batch_cancel_splits_on_cancel_batch() {
    let mut path = open(vec![decl(7, &[1])]);
    let mut mint = Script {
        seqs: vec![3, 1, 2, 5, 4],
        at: 0,
    };
    listen(&mut path);
    baseline(&mut path, 7, 1, 0, 2);
    let mut ids = Vec::new();
    for uid in [101_u32, 102, 103, 104, 1001] {
        let (id, effect) = submit(&mut path, &mut mint, order_for(7, 1, uid));
        ids.push(id);
        assert!(
            path.on_order_round(
                ticket(&effect),
                RoundOutcome::Reply(td::OrderReply {
                    accepted: true,
                    ..Default::default()
                }),
            )
            .is_empty()
        );
    }
    let mut sorted = ids.clone();
    sorted.sort();
    assert_ne!(ids, sorted);

    assert!(path.batch_cancel(7, 1, &[999]).unwrap().is_empty());
    let one = path.batch_cancel(7, 1, &[1001]).unwrap();
    assert_eq!(one.len(), 1);
    match send_order(&one[0]) {
        td::OrderRequest {
            req: Some(td::order_request::Req::BatchCancel(batch)),
            ..
        } => {
            assert_eq!(
                batch
                    .orders
                    .iter()
                    .map(|order| order.client_order_id.as_str())
                    .collect::<Vec<_>>(),
                vec![
                    sorted
                        .iter()
                        .find(|id| path.order(7, id).unwrap().uid == 1001)
                        .unwrap()
                        .as_str()
                ]
            );
        }
        other => panic!("{other:?}"),
    }
    let _ = path.on_order_round(
        ticket(&one[0]),
        RoundOutcome::Reply(td::OrderReply {
            accepted: true,
            ..Default::default()
        }),
    );

    let effects = path.batch_cancel(7, 1, &[]).unwrap();
    assert_eq!(effects.len(), 3);
    let want = [
        sorted[0..2].to_vec(),
        sorted[2..4].to_vec(),
        sorted[4..5].to_vec(),
    ];
    for (effect, chunk) in effects.iter().zip(&want) {
        match send_order(effect) {
            td::OrderRequest {
                req: Some(td::order_request::Req::BatchCancel(batch)),
                ..
            } => {
                let got: Vec<_> = batch
                    .orders
                    .iter()
                    .map(|order| order.client_order_id.clone())
                    .collect();
                assert_eq!(&got, chunk);
            }
            other => panic!("{other:?}"),
        }
    }
    let refused = path.on_order_round(
        ticket(&effects[0]),
        RoundOutcome::Reply(td::OrderReply {
            code: 106,
            reason: "batch".to_owned(),
            ..Default::default()
        }),
    );
    let refused = drive(&mut path, refused, |_, hook| {
        let Hook::Reject(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.request, RejectedRequest::BatchCancel);
        assert_eq!(event.client_order_ids, want[0]);
        assert_eq!(event.code, 106);
        assert_eq!(event.reason, "batch");
    });
    assert_eq!(names(&refused), ["reject"]);
    assert!(
        path.on_order_round(
            ticket(&effects[1]),
            RoundOutcome::Reply(td::OrderReply {
                accepted: true,
                ..Default::default()
            }),
        )
        .is_empty()
    );
    assert_eq!(path.open_orders(7, 1).len(), 5);

    // cancel_batch 1 is one Cancel per order, with the order's uid.
    let generation = ask(&mut path, 7);
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(1, 0, 1)));
    // These pending orders were submitted before this request, so an empty snapshot drops them.
    drive(&mut path, effects, |_, _| {});
    let again = path.batch_cancel(7, 1, &[]).unwrap();
    // The empty snapshot dropped the pre-request pending orders, so there is nothing to cancel.
    assert!(again.is_empty());

    let mut singles = open(vec![decl(7, &[1])]);
    let mut mint = Mint::new();
    listen(&mut singles);
    baseline(&mut singles, 7, 1, 0, 1);
    let mut live = Vec::new();
    for uid in [3_u32, 4] {
        let (id, effect) = submit(&mut singles, &mut mint, order_for(7, 1, uid));
        live.push((id, uid));
        assert!(
            singles
                .on_order_round(
                    ticket(&effect),
                    RoundOutcome::Reply(td::OrderReply {
                        accepted: true,
                        ..Default::default()
                    }),
                )
                .is_empty()
        );
    }
    let effects = singles.batch_cancel(7, 1, &[]).unwrap();
    assert_eq!(effects.len(), 2);
    for effect in &effects {
        match send_order(effect) {
            td::OrderRequest {
                req: Some(td::order_request::Req::Cancel(cancel)),
                ..
            } => {
                assert_eq!(cancel.recv_window_ms, 0);
                assert!(
                    live.iter()
                        .any(|(id, uid)| cancel.client_order_id == *id && cancel.uid == *uid)
                );
            }
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn recon_filters_to_declared_algos_and_replaces_the_ledger() {
    let mut path = open(vec![decl(7, &[1]), decl(8, &[2])]);
    listen(&mut path);
    let own = id_of(1, 1);
    let other = id_of(2, 1);
    let filled = id_of(1, 9);
    let generation = ask(&mut path, 7);
    let reply = view(
        3,
        5,
        4,
        vec![
            row(&own, td::OrderStatus::Accepted, 1, 1.0),
            row(&other, td::OrderStatus::Accepted, 2, 1.0),
            row("manual", td::OrderStatus::Accepted, 3, 1.0),
            row(&filled, td::OrderStatus::Filled, 1, 1.0),
        ],
        vec![
            td::Position {
                uid: 3,
                size: 1.0,
                ..Default::default()
            },
            td::Position {
                uid: 3,
                size: 5.0,
                ..Default::default()
            },
            td::Position {
                uid: 9,
                size: 0.0,
                ..Default::default()
            },
        ],
        vec![td::Balance {
            asset: "USDT".to_owned(),
            free: 1.0,
            locked: 2.0,
            prelock: 3.0,
        }],
    );
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(reply));
    let mut during = false;
    let effects = drive(&mut path, effects, |path, hook| {
        let Hook::Recon(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.td.api_id, 7);
        assert_eq!(event.td.incarnation, 3);
        assert_eq!(event.td.seq, 5);
        assert_eq!(event.recon.reason, i32::from(td::ReconReason::Requested));
        // The hook keeps every own-algo row, including a terminal one. The book does not.
        assert_eq!(event.recon.open_orders.len(), 2);
        assert_eq!(event.recon.open_orders[0].client_order_id, own);
        assert_eq!(event.recon.open_orders[1].client_order_id, filled);
        assert_eq!(event.recon.positions.len(), 3);
        assert_eq!(event.recon.balances.len(), 1);
        let mut copy = path.position(7, 3).unwrap();
        copy.size = -1.0;
        assert_eq!(copy.size, -1.0);
        during = path.position(7, 3).unwrap().size == 5.0
            && path.position(7, 9).unwrap().size == 0.0
            && path.balance(7, "USDT").unwrap().prelock == 3.0
            && path.order(7, &own).is_some()
            && path.order(7, &other).is_none()
            && path.order(7, "manual").is_none();
    });
    assert_eq!(names(&effects), ["recon"]);
    assert!(during);
    assert!(status_is(
        &path.order(7, &own).unwrap(),
        td::OrderStatus::Accepted
    ));
    assert!(path.order(7, &filled).is_none());
    assert!(path.position(8, 3).is_none());
    assert!(path.balance(8, "USDT").is_none());
}

#[test]
fn ledger_updates_are_absolute_and_a_fill_changes_nothing() {
    let mut path = open(vec![decl(7, &[1]), decl(8, &[1])]);
    listen(&mut path);
    baseline(&mut path, 7, 1, 0, 4);
    baseline(&mut path, 8, 1, 0, 1);
    let id = id_of(1, 1);
    let other = id_of(2, 1);

    let mut priced = row(&id, td::OrderStatus::New, 1001, 1.0);
    priced.price = Some(10.0);
    priced.order_id = "old".to_owned();
    let effects = path.on_account_event(
        7,
        order_event(1, 1, priced, td::OrderCause::Acknowledged, 0, ""),
    );
    drive(&mut path, effects, |path, _| {
        assert_eq!(path.order(7, &id).unwrap().price, Some(10.0));
        assert_eq!(path.open_orders(7, 1).len(), 1);
    });

    let mut replaced = row(&id, td::OrderStatus::PartiallyFilled, 1001, 2.0);
    replaced.order_id = "ex-1".to_owned();
    replaced.filled_qty = 0.5;
    replaced.cancel_sent_ns = 5;
    let effects = path.on_account_event(
        7,
        order_event(1, 2, replaced, td::OrderCause::Filled, 0, ""),
    );
    drive(&mut path, effects, |_, _| {});
    let stored = path.order(7, &id).unwrap();
    assert!(stored.price.is_none());
    assert_eq!(stored.qty, 2.0);
    assert_eq!(stored.filled_qty, 0.5);
    assert_eq!(stored.order_id, "ex-1");

    // Another algo, then an update with no order. Both are consumed, so the next seq is live.
    assert!(
        path.on_account_event(
            7,
            order_event(
                1,
                3,
                row(&other, td::OrderStatus::Accepted, 2, 4.0),
                td::OrderCause::Accepted,
                0,
                ""
            ),
        )
        .is_empty()
    );
    assert!(path.order(7, &other).is_none());
    assert!(
        path.on_account_event(
            7,
            event(
                1,
                4,
                Some(td::account_event::Event::OrderUpdate(
                    td::OrderUpdate::default()
                ))
            ),
        )
        .is_empty()
    );

    let effects = path.on_account_event(
        7,
        event(
            1,
            5,
            Some(td::account_event::Event::PositionUpdate(
                td::PositionUpdate {
                    positions: vec![
                        td::Position {
                            uid: 3,
                            size: 1.0,
                            ..Default::default()
                        },
                        td::Position {
                            uid: 3,
                            size: 5.0,
                            ..Default::default()
                        },
                        td::Position {
                            uid: 9,
                            size: 0.0,
                            ..Default::default()
                        },
                    ],
                },
            )),
        ),
    );
    drive(&mut path, effects, |path, hook| {
        let Hook::Position(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.td.api_id, 7);
        assert_eq!(event.td.seq, 5);
        assert_eq!(path.position(7, 3).unwrap().size, 5.0);
        assert_eq!(path.position(7, 9).unwrap().size, 0.0);
        assert!(path.position(8, 3).is_none());
    });

    let effects = path.on_account_event(
        7,
        event(
            1,
            6,
            Some(td::account_event::Event::BalanceUpdate(td::BalanceUpdate {
                balances: vec![td::Balance {
                    asset: "USDT".to_owned(),
                    free: 1.0,
                    locked: 2.0,
                    prelock: 3.0,
                }],
            })),
        ),
    );
    drive(&mut path, effects, |_, _| {});
    let effects = path.on_account_event(
        7,
        event(
            1,
            7,
            Some(td::account_event::Event::BalanceUpdate(td::BalanceUpdate {
                balances: vec![td::Balance {
                    asset: "BTC".to_owned(),
                    free: 4.0,
                    locked: 5.0,
                    prelock: 6.0,
                }],
            })),
        ),
    );
    drive(&mut path, effects, |path, hook| {
        let Hook::Balance(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.td.seq, 7);
        assert_eq!(event.balances.len(), 1);
        let mut copy = path.balance(7, "BTC").unwrap();
        copy.free = 0.0;
        assert_eq!(path.balance(7, "BTC").unwrap().free, 4.0);
    });
    let usdt = path.balance(7, "USDT").unwrap();
    assert_eq!((usdt.free, usdt.locked, usdt.prelock), (1.0, 2.0, 3.0));

    let effects = path.on_account_event(
        7,
        event(
            1,
            8,
            Some(td::account_event::Event::Fill(td::Fill {
                client_order_id: id.clone(),
                fill_id: "f".to_owned(),
                qty: 1.0,
                price: 10.0,
                ..Default::default()
            })),
        ),
    );
    drive(&mut path, effects, |path, hook| {
        let Hook::Fill(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.td.api_id, 7);
        assert_eq!(event.td.seq, 8);
        assert_eq!(event.fill.qty, 1.0);
        assert_eq!(path.order(7, &id).unwrap().filled_qty, 0.5);
        assert_eq!(path.position(7, 3).unwrap().size, 5.0);
        assert_eq!(path.balance(7, "USDT").unwrap().prelock, 3.0);
    });

    let fresh = id_of(1, 2);
    let effects = path.on_account_event(
        7,
        event(
            1,
            9,
            Some(td::account_event::Event::Recon(td::Recon {
                reason: i32::from(td::ReconReason::Reconnect),
                open_orders: vec![row(&fresh, td::OrderStatus::Accepted, 1001, 4.0)],
                positions: vec![td::Position {
                    uid: 2,
                    size: 8.0,
                    ..Default::default()
                }],
                balances: vec![td::Balance {
                    asset: "ETH".to_owned(),
                    free: 9.0,
                    locked: 1.0,
                    prelock: 0.0,
                }],
                ..Default::default()
            })),
        ),
    );
    drive(&mut path, effects, |path, hook| {
        let Hook::Recon(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!(event.recon.reason, i32::from(td::ReconReason::Reconnect));
        assert_eq!(event.recon.open_orders.len(), 1);
        assert!(path.order(7, &id).is_none());
        assert!(path.position(7, 3).is_none());
        assert!(path.position(7, 9).is_none());
        assert!(path.balance(7, "USDT").is_none());
        assert!(path.balance(7, "BTC").is_none());
        assert_eq!(path.position(7, 2).unwrap().size, 8.0);
        assert_eq!(path.balance(7, "ETH").unwrap().free, 9.0);
    });
    // A pushed recon does not clear cancel_batch learned from td.view.
    let effects = path.batch_cancel(7, 1, &[]).unwrap();
    assert_eq!(effects.len(), 1);
    assert!(matches!(
        send_order(&effects[0]).req,
        Some(td::order_request::Req::BatchCancel(_))
    ));

    let effects = path.on_account_event(
        8,
        event(
            1,
            1,
            Some(td::account_event::Event::PositionUpdate(
                td::PositionUpdate {
                    positions: vec![td::Position {
                        uid: 3,
                        size: 7.0,
                        ..Default::default()
                    }],
                },
            )),
        ),
    );
    drive(&mut path, effects, |_, _| {});
    assert_eq!(path.position(8, 3).unwrap().size, 7.0);
    assert!(path.position(7, 3).is_none());
    assert_eq!(path.position(7, 2).unwrap().size, 8.0);
}

#[test]
fn held_events_follow_the_watermark() {
    let mut path = open(vec![decl(7, &[1])]);
    listen(&mut path);
    let id = id_of(1, 1);
    let generation = ask(&mut path, 7);
    for (inc, seq) in [(3, 4), (3, 6), (1, 9), (3, 8)] {
        assert!(
            path.on_account_event(
                7,
                order_event(
                    inc,
                    seq,
                    row(&id, td::OrderStatus::Accepted, 1001, 1.0),
                    td::OrderCause::Accepted,
                    0,
                    ""
                )
            )
            .is_empty()
        );
    }
    assert_eq!(path.held(), 4);
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(3, 5, 1)));
    let mut delivered = Vec::new();
    let effects = drive(&mut path, effects, |_, hook| {
        if let Hook::Order(event) = hook {
            delivered.push(event.td.seq);
        }
    });
    assert_eq!(names(&effects), ["recon", "order", "recon_send"]);
    assert_eq!(delivered, vec![6]);
    assert_eq!(path.held(), 1);

    let Effect::SendRecon { generation, .. } = effects[2] else {
        panic!("{effects:?}");
    };
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(3, 7, 1)));
    delivered.clear();
    let effects = drive(&mut path, effects, |_, hook| {
        if let Hook::Order(event) = hook {
            delivered.push(event.td.seq);
        }
    });
    assert_eq!(names(&effects), ["recon", "order"]);
    assert_eq!(delivered, vec![8]);
    assert_eq!(path.held(), 0);

    assert!(
        path.on_account_event(
            7,
            order_event(
                3,
                7,
                row(&id, td::OrderStatus::New, 1001, 1.0),
                td::OrderCause::Acknowledged,
                0,
                ""
            )
        )
        .is_empty()
    );
    assert!(status_is(
        &path.order(7, &id).unwrap(),
        td::OrderStatus::Accepted
    ));
    let effects = path.on_account_event(
        7,
        order_event(
            3,
            9,
            row(&id, td::OrderStatus::New, 1001, 1.0),
            td::OrderCause::Acknowledged,
            0,
            "",
        ),
    );
    let effects = drive(&mut path, effects, |_, hook| {
        let Hook::Order(event) = hook else {
            panic!("{hook:?}");
        };
        assert_eq!((event.td.incarnation, event.td.seq), (3, 9));
    });
    assert_eq!(names(&effects), ["order"]);
}

#[test]
fn a_newer_held_incarnation_asks_again_without_delivering_the_event() {
    let mut path = open(vec![decl(7, &[1])]);
    listen(&mut path);
    let stale = id_of(1, 1);
    let fresh = id_of(1, 2);
    let generation = ask(&mut path, 7);
    assert!(
        path.on_account_event(
            7,
            order_event(
                9,
                4,
                row(&fresh, td::OrderStatus::Accepted, 1001, 1.0),
                td::OrderCause::Accepted,
                0,
                ""
            )
        )
        .is_empty()
    );
    let reply = view(
        3,
        5,
        1,
        vec![row(&stale, td::OrderStatus::Accepted, 1001, 1.0)],
        Vec::new(),
        Vec::new(),
    );
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(reply));
    let effects = drive(&mut path, effects, |_path, hook| {
        let Hook::Recon(event) = hook else {
            panic!("held event became a hook: {hook:?}");
        };
        assert_eq!((event.td.incarnation, event.td.seq), (3, 5));
        assert_eq!(event.recon.open_orders[0].client_order_id, stale);
    });
    assert_eq!(names(&effects), ["recon", "recon_send"]);
    assert_eq!(path.held(), 1);

    let Effect::SendRecon { generation, .. } = effects[1] else {
        panic!("{effects:?}");
    };
    let reply = view(
        9,
        4,
        1,
        vec![row(&fresh, td::OrderStatus::Accepted, 1001, 1.0)],
        Vec::new(),
        Vec::new(),
    );
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(reply));
    let effects = drive(&mut path, effects, |_, hook| {
        assert!(matches!(hook, Hook::Recon(_)), "{hook:?}");
    });
    assert_eq!(names(&effects), ["recon"]);
    assert_eq!(path.held(), 0);
    let open = path.open_orders(7, 1);
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].client_order_id, fresh);
    assert!(path.order(7, &stale).is_none());
}

#[test]
fn a_gap_or_a_new_incarnation_asks_again_and_then_delivers() {
    let mut path = open(vec![decl(7, &[1])]);
    listen(&mut path);
    baseline(&mut path, 7, 1, 1, 1);
    let id = id_of(1, 1);
    let effects = path.on_account_event(
        7,
        order_event(
            1,
            3,
            row(&id, td::OrderStatus::Accepted, 1001, 1.0),
            td::OrderCause::Accepted,
            0,
            "",
        ),
    );
    assert_eq!(names(&effects), ["recon_send"]);
    assert_eq!(path.held(), 1);
    let Effect::SendRecon { generation, .. } = effects[0] else {
        panic!("{effects:?}");
    };
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(1, 2, 1)));
    let mut seq = 0;
    let effects = drive(&mut path, effects, |_, hook| {
        if let Hook::Order(event) = hook {
            seq = event.td.seq;
        }
    });
    assert_eq!(names(&effects), ["recon", "order"]);
    assert_eq!(seq, 3);
    assert_eq!(path.held(), 0);

    let mut restarted = open(vec![decl(7, &[1])]);
    listen(&mut restarted);
    baseline(&mut restarted, 7, 1, 1, 1);
    let id = id_of(1, 2);
    let effects = restarted.on_account_event(
        7,
        order_event(
            2,
            1,
            row(&id, td::OrderStatus::Accepted, 1001, 1.0),
            td::OrderCause::Accepted,
            0,
            "",
        ),
    );
    assert_eq!(names(&effects), ["recon_send"]);
    let Effect::SendRecon { generation, .. } = effects[0] else {
        panic!("{effects:?}");
    };
    let effects = restarted.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(2, 0, 1)));
    let mut stamp = (0, 0);
    let effects = drive(&mut restarted, effects, |_, hook| {
        if let Hook::Order(event) = hook {
            stamp = (event.td.incarnation, event.td.seq);
        }
    });
    assert_eq!(names(&effects), ["recon", "order"]);
    assert_eq!(stamp, (2, 1));
}

#[test]
fn an_older_live_incarnation_is_asked_and_then_dropped() {
    let mut path = open(vec![decl(7, &[1])]);
    listen(&mut path);
    baseline(&mut path, 7, 3, 5, 1);
    let id = id_of(1, 1);
    let effects = path.on_account_event(
        7,
        order_event(
            1,
            6,
            row(&id, td::OrderStatus::Accepted, 1001, 1.0),
            td::OrderCause::Accepted,
            0,
            "",
        ),
    );
    assert_eq!(names(&effects), ["recon_send"]);
    let Effect::SendRecon { generation, .. } = effects[0] else {
        panic!("{effects:?}");
    };
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(3, 5, 1)));
    let effects = drive(&mut path, effects, |_, hook| {
        assert!(matches!(hook, Hook::Recon(_)), "{hook:?}");
    });
    assert_eq!(names(&effects), ["recon"]);
    assert_eq!(path.held(), 0);
    assert!(path.order(7, &id).is_none());
    assert!(path.on_backoff(7).is_empty());
}

#[test]
fn a_stuck_snapshot_backs_off_instead_of_asking_again() {
    let mut path = open(vec![decl(7, &[1])]);
    listen(&mut path);
    baseline(&mut path, 7, 1, 1, 1);
    let effects = path.on_account_event(
        7,
        order_event(
            1,
            10,
            row(&id_of(1, 1), td::OrderStatus::Accepted, 1001, 1.0),
            td::OrderCause::Accepted,
            0,
            "",
        ),
    );
    assert_eq!(names(&effects), ["recon_send"]);
    let Effect::SendRecon { generation, .. } = effects[0] else {
        panic!("{effects:?}");
    };
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(1, 1, 1)));
    let effects = drive(&mut path, effects, |_, hook| {
        assert!(matches!(hook, Hook::Recon(_)), "{hook:?}");
    });
    assert_eq!(names(&effects), ["recon", "backoff"]);
    match &effects[1] {
        Effect::ArmBackoff { api_id, after } => {
            assert_eq!(*api_id, 7);
            assert_eq!(*after, Duration::from_secs(1));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(path.held(), 1);
    assert!(path.request_recon(7).unwrap().is_empty());
    let effects = path.on_backoff(7);
    assert_eq!(names(&effects), ["recon_send"]);
    assert!(path.on_backoff(7).is_empty());
    let Effect::SendRecon { generation, .. } = effects[0] else {
        panic!("{effects:?}");
    };
    let effects = path.on_recon_round(7, generation, ReconOutcome::Failed);
    assert_eq!(names(&effects), ["backoff"]);
    match &effects[0] {
        Effect::ArmBackoff { after, .. } => assert_eq!(*after, Duration::from_secs(2)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn recon_failures_double_until_thirty_seconds_and_then_deliver() {
    let mut path = open(vec![decl(7, &[1])]);
    listen(&mut path);
    let id = id_of(1, 1);
    assert!(
        path.on_account_event(
            7,
            order_event(
                3,
                6,
                row(&id, td::OrderStatus::Accepted, 1001, 1.0),
                td::OrderCause::Accepted,
                0,
                ""
            )
        )
        .is_empty()
    );
    assert_eq!(path.held(), 1);
    let generation = ask(&mut path, 7);
    assert!(
        path.on_recon_round(7, generation + 1, ReconOutcome::Failed)
            .is_empty()
    );
    let not_recon = td::ViewReply {
        reply: Some(td::view_reply::Reply::Leverage(td::LeverageInfo {
            uid: 1,
            leverage: None,
        })),
        ..Default::default()
    };
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(not_recon));
    assert_eq!(names(&effects), ["backoff"]);
    // A late body for the attempt that already failed does not apply.
    assert!(
        path.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(3, 5, 1)))
            .is_empty()
    );
    assert!(path.order(7, &id).is_none());

    let mut expect = Duration::from_secs(1);
    let mut effects = effects;
    for step in 0..7 {
        match &effects[0] {
            Effect::ArmBackoff { after, .. } => assert_eq!(*after, expect, "step {step}"),
            other => panic!("{other:?}"),
        }
        expect = (expect * 2).min(RECON_BACKOFF_MAX);
        effects = path.on_backoff(7);
        assert_eq!(names(&effects), ["recon_send"]);
        let Effect::SendRecon { generation, .. } = effects[0] else {
            panic!("{effects:?}");
        };
        if step == 6 {
            effects = path.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(3, 5, 1)));
            break;
        }
        effects = path.on_recon_round(7, generation, ReconOutcome::Failed);
        assert_eq!(names(&effects), ["backoff"]);
    }
    let mut seq = 0;
    let effects = drive(&mut path, effects, |_, hook| {
        if let Hook::Order(event) = hook {
            seq = event.td.seq;
            assert_eq!(event.td.incarnation, 3);
        }
    });
    assert_eq!(names(&effects), ["recon", "order"]);
    assert_eq!(seq, 6);
    assert_eq!(path.held(), 0);

    // A success resets the schedule. The next failure waits 1s again.
    let generation = ask(&mut path, 7);
    let effects = path.on_recon_round(7, generation, ReconOutcome::Failed);
    match &effects[0] {
        Effect::ArmBackoff { after, .. } => assert_eq!(*after, Duration::from_secs(1)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_second_ask_waits_only_when_the_watermark_stays_put() {
    let mut path = open(vec![decl(7, &[1])]);
    listen(&mut path);
    baseline(&mut path, 7, 1, 5, 1);
    let generation = ask(&mut path, 7);
    assert!(path.request_recon(7).unwrap().is_empty());
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(1, 6, 1)));
    let effects = drive(&mut path, effects, |_, _| {});
    assert_eq!(names(&effects), ["recon", "recon_send"]);

    let mut stuck = open(vec![decl(7, &[1])]);
    listen(&mut stuck);
    baseline(&mut stuck, 7, 1, 5, 1);
    let generation = ask(&mut stuck, 7);
    assert!(stuck.request_recon(7).unwrap().is_empty());
    let effects = stuck.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(1, 5, 1)));
    let effects = drive(&mut stuck, effects, |_, _| {});
    assert_eq!(names(&effects), ["recon", "backoff"]);
    match &effects[1] {
        Effect::ArmBackoff { after, .. } => assert_eq!(*after, Duration::from_secs(1)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn only_a_pending_new_submitted_after_the_request_survives_an_empty_gap() {
    let mut path = open(vec![decl(7, &[1])]);
    let mut mint = Mint::new();
    listen(&mut path);
    baseline(&mut path, 7, 1, 5, 4);
    let (before, before_send) = submit(&mut path, &mut mint, order_for(7, 1, 1001));
    let (listed, listed_send) = submit(&mut path, &mut mint, order_for(7, 1, 1001));
    for effect in [&before_send, &listed_send] {
        assert!(
            path.on_order_round(
                ticket(effect),
                RoundOutcome::Reply(td::OrderReply {
                    accepted: true,
                    ..Default::default()
                }),
            )
            .is_empty()
        );
    }
    let effects = path.on_account_event(
        7,
        event(
            1,
            6,
            Some(td::account_event::Event::PositionUpdate(
                td::PositionUpdate {
                    positions: vec![td::Position {
                        uid: 9,
                        size: 4.0,
                        ..Default::default()
                    }],
                },
            )),
        ),
    );
    drive(&mut path, effects, |_, _| {});
    let effects = path.on_account_event(
        7,
        event(
            1,
            7,
            Some(td::account_event::Event::BalanceUpdate(td::BalanceUpdate {
                balances: vec![td::Balance {
                    asset: "OLD".to_owned(),
                    free: 1.0,
                    locked: 2.0,
                    prelock: 3.0,
                }],
            })),
        ),
    );
    drive(&mut path, effects, |_, _| {});

    let generation = ask(&mut path, 7);
    let (late, _) = submit(&mut path, &mut mint, {
        let mut order = order_for(7, 1, 1001);
        order.qty = Some(3.0);
        order
    });
    let reply = view(
        1,
        7,
        4,
        vec![row(&listed, td::OrderStatus::Accepted, 1001, 9.0)],
        Vec::new(),
        vec![td::Balance {
            asset: "USDT".to_owned(),
            free: 1.0,
            locked: 2.0,
            prelock: 3.0,
        }],
    );
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(reply));
    drive(&mut path, effects, |path, hook| {
        let Hook::Recon(event) = hook else {
            panic!("{hook:?}");
        };
        let ids: Vec<_> = event
            .recon
            .open_orders
            .iter()
            .map(|order| order.client_order_id.clone())
            .collect();
        assert_eq!(ids, vec![listed.clone()]);
        assert!(path.order(7, &before).is_none());
        let listed_row = path.order(7, &listed).unwrap();
        assert!(status_is(&listed_row, td::OrderStatus::Accepted));
        assert_eq!(listed_row.qty, 9.0);
        let late_row = path.order(7, &late).unwrap();
        assert!(status_is(&late_row, td::OrderStatus::PendingNew));
        assert_eq!(late_row.qty, 3.0);
        assert!(path.position(7, 9).is_none());
        assert!(path.balance(7, "OLD").is_none());
        let mut usdt = path.balance(7, "USDT").unwrap();
        usdt.prelock = -1.0;
        assert_eq!(path.balance(7, "USDT").unwrap().prelock, 3.0);
        let open: Vec<_> = path
            .open_orders(7, 0)
            .into_iter()
            .map(|order| order.client_order_id)
            .collect();
        assert!(open.contains(&listed));
        assert!(open.contains(&late));
        assert!(!open.contains(&before));
    });

    let effects = path.on_account_event(
        7,
        event(
            1,
            8,
            Some(td::account_event::Event::Recon(td::Recon {
                positions: vec![td::Position {
                    uid: 4,
                    size: 8.0,
                    ..Default::default()
                }],
                ..Default::default()
            })),
        ),
    );
    drive(&mut path, effects, |_, _| {});
    assert!(path.order(7, &listed).is_none());
    assert!(path.order(7, &late).is_none());
    assert_eq!(path.position(7, 4).unwrap().size, 8.0);
    assert!(path.balance(7, "USDT").is_none());

    // A submit during backoff is before the next request's handoff, so the next empty
    // snapshot drops it. A submit after that handoff stays.
    let effects = path.on_account_event(
        7,
        order_event(
            1,
            20,
            row(&id_of(1, 8), td::OrderStatus::Accepted, 1001, 1.0),
            td::OrderCause::Accepted,
            0,
            "",
        ),
    );
    assert_eq!(names(&effects), ["recon_send"]);
    let Effect::SendRecon { generation, .. } = effects[0] else {
        panic!("{effects:?}");
    };
    let effects = path.on_recon_round(7, generation, ReconOutcome::Reply(bare_view(1, 8, 4)));
    let effects = drive(&mut path, effects, |_, _| {});
    assert_eq!(names(&effects), ["recon", "backoff"]);
    let (early, _) = submit(&mut path, &mut mint, order_for(7, 1, 1001));
    let effects = path.on_backoff(7);
    let Effect::SendRecon { generation, .. } = &effects[0] else {
        panic!("{effects:?}");
    };
    let (kept, _) = submit(&mut path, &mut mint, order_for(7, 1, 1001));
    let effects = path.on_recon_round(7, *generation, ReconOutcome::Reply(bare_view(1, 8, 4)));
    drive(&mut path, effects, |path, hook| {
        if let Hook::Recon(_) = hook {
            assert!(path.order(7, &early).is_none());
            assert!(status_is(
                &path.order(7, &kept).unwrap(),
                td::OrderStatus::PendingNew
            ));
        }
    });
}

#[test]
fn the_hold_cap_fails_the_session_once() {
    let mut path = open(vec![decl(7, &[1]), decl(8, &[1])]);
    let _ = path.listen().unwrap();
    let sample = event(1, 1, None);
    assert!(path.on_account_event(99, sample.clone()).is_empty());
    assert_eq!(path.held(), 0);
    for _ in 0..HOLD_CAP {
        assert!(path.on_account_event(7, sample.clone()).is_empty());
    }
    assert_eq!(path.held(), HOLD_CAP);
    let overflow = path.on_account_event(8, sample.clone());
    assert_eq!(
        overflow,
        vec![Effect::Fail {
            reason: REASON_HOLD_FULL.to_owned()
        }]
    );
    assert_eq!(path.held(), HOLD_CAP);
    assert!(path.on_account_event(8, sample).is_empty());
    assert_eq!(path.held(), HOLD_CAP);
    match path.listen_bound().unwrap() {
        Bound::Stopping(effects) => match &effects[..] {
            [Effect::UnsubscribeEvt { api_ids }] => assert_eq!(api_ids, &vec![7, 8]),
            other => panic!("{other:?}"),
        },
        Bound::Listening => panic!("a full hold was still listening"),
    }
    let mut mint = Mint::new();
    assert_eq!(
        path.submit(order_for(7, 1, 1001), NOW_MS, &mut mint)
            .unwrap_err(),
        Error::Stopping
    );
    assert!(path.on_backoff(7).is_empty());
    assert!(path.on_recon_round(7, 1, ReconOutcome::Failed).is_empty());
}

#[test]
fn hold_full_keeps_what_was_applied_and_drops_what_follows() {
    // E05-D051 (revised) through E05-D054. Live events applied before the hold
    // fills stay in the OMS and the ledger. The event that trips the cap, events
    // still in the hold, and anything fed after the failure — an in-sequence
    // event included — are not delivered. Replies after the failure are ignored.
    let mut path = open(vec![decl(7, &[1]), decl(8, &[1])]);
    let mut mint = Mint::new();
    listen(&mut path);
    baseline(&mut path, 7, 1, 10, 4);
    let generation = ask(&mut path, 8);

    let early = id_of(1, 40);
    let effects = path.on_account_event(
        7,
        order_event(
            1,
            11,
            row(&early, td::OrderStatus::New, 1001, 1.0),
            td::OrderCause::Accepted,
            0,
            "",
        ),
    );
    assert_eq!(names(&drive(&mut path, effects, |_, _| {})), ["order"]);
    let effects = path.on_account_event(
        7,
        event(
            1,
            12,
            Some(td::account_event::Event::PositionUpdate(
                td::PositionUpdate {
                    positions: vec![td::Position {
                        uid: 4,
                        size: 2.0,
                        ..Default::default()
                    }],
                },
            )),
        ),
    );
    assert_eq!(names(&drive(&mut path, effects, |_, _| {})), ["position"]);
    let effects = path.on_account_event(
        7,
        event(
            1,
            13,
            Some(td::account_event::Event::BalanceUpdate(td::BalanceUpdate {
                balances: vec![td::Balance {
                    asset: "usd".to_owned(),
                    free: 1.0,
                    locked: 2.0,
                    prelock: 3.0,
                }],
            })),
        ),
    );
    assert_eq!(names(&drive(&mut path, effects, |_, _| {})), ["balance"]);

    let (submitted, sent) = submit(&mut path, &mut mint, order_for(7, 1, 1001));
    let order_ticket = ticket(&sent);
    assert!(status_is(
        &path.order(7, &submitted).unwrap(),
        td::OrderStatus::PendingNew
    ));

    let held_id = id_of(1, 2);
    assert!(
        path.on_account_event(
            8,
            order_event(
                1,
                1,
                row(&held_id, td::OrderStatus::New, 1001, 1.0),
                td::OrderCause::Accepted,
                0,
                "",
            ),
        )
        .is_empty()
    );
    let filler = event(1, 1, None);
    for _ in 1..HOLD_CAP {
        assert!(path.on_account_event(8, filler.clone()).is_empty());
    }
    assert_eq!(path.held(), HOLD_CAP);
    assert!(path.order(8, &held_id).is_none());

    let trigger = id_of(1, 3);
    let overflow = path.on_account_event(
        8,
        order_event(
            1,
            2,
            row(&trigger, td::OrderStatus::New, 1001, 1.0),
            td::OrderCause::Accepted,
            0,
            "",
        ),
    );
    assert_eq!(
        overflow,
        vec![Effect::Fail {
            reason: REASON_HOLD_FULL.to_owned()
        }]
    );
    assert_eq!(path.held(), HOLD_CAP);
    assert!(path.order(8, &trigger).is_none());
    assert!(status_is(
        &path.order(7, &early).unwrap(),
        td::OrderStatus::New
    ));
    assert_eq!(path.order(7, &early).unwrap().qty, 1.0);
    assert_eq!(path.position(7, 4).unwrap().size, 2.0);
    let usd = path.balance(7, "usd").unwrap();
    assert_eq!((usd.free, usd.locked, usd.prelock), (1.0, 2.0, 3.0));

    // In sequence after the trigger, a gap, and a duplicate: none is applied,
    // and none starts a recon.
    let late = id_of(1, 41);
    assert!(
        path.on_account_event(
            7,
            order_event(
                1,
                14,
                row(&late, td::OrderStatus::New, 1001, 1.0),
                td::OrderCause::Accepted,
                0,
                "",
            ),
        )
        .is_empty()
    );
    assert!(path.order(7, &late).is_none());
    assert!(path.on_account_event(7, event(1, 99, None)).is_empty());
    assert!(path.on_account_event(7, event(1, 11, None)).is_empty());
    assert!(path.on_account_event(8, filler).is_empty());
    assert_eq!(path.held(), HOLD_CAP);
    assert!(path.end_hook().is_none());
    assert_eq!(path.held(), HOLD_CAP);
    assert!(path.order(8, &held_id).is_none());

    assert!(
        path.on_order_round(
            order_ticket,
            RoundOutcome::Reply(td::OrderReply {
                accepted: false,
                code: 101,
                reason: "down".to_owned(),
                ..Default::default()
            }),
        )
        .is_empty()
    );
    assert!(status_is(
        &path.order(7, &submitted).unwrap(),
        td::OrderStatus::PendingNew
    ));

    let planted = id_of(1, 50);
    let reply = view(
        1,
        1,
        4,
        vec![row(&planted, td::OrderStatus::Accepted, 1001, 9.0)],
        vec![td::Position {
            uid: 4,
            size: 9.0,
            ..Default::default()
        }],
        vec![td::Balance {
            asset: "eur".to_owned(),
            free: 4.0,
            locked: 0.0,
            prelock: 0.0,
        }],
    );
    assert!(
        path.on_recon_round(8, generation, ReconOutcome::Reply(reply))
            .is_empty()
    );
    assert!(path.order(8, &planted).is_none());
    assert!(path.position(8, 4).is_none());
    assert!(path.balance(8, "eur").is_none());
    assert!(path.on_backoff(8).is_empty());
    assert!(
        path.on_recon_round(8, generation, ReconOutcome::Failed)
            .is_empty()
    );
    assert_eq!(path.request_recon(7).unwrap_err(), Error::Stopping);
    assert_eq!(path.request_recon(8).unwrap_err(), Error::Stopping);
    assert_eq!(
        path.submit(order_for(7, 1, 1001), NOW_MS, &mut mint)
            .unwrap_err(),
        Error::Stopping
    );
    assert!(status_is(
        &path.order(7, &early).unwrap(),
        td::OrderStatus::New
    ));
    assert_eq!(path.position(7, 4).unwrap().size, 2.0);
    let usd = path.balance(7, "usd").unwrap();
    assert_eq!((usd.free, usd.locked, usd.prelock), (1.0, 2.0, 3.0));
    assert!(status_is(
        &path.order(7, &submitted).unwrap(),
        td::OrderStatus::PendingNew
    ));
    assert!(path.order(7, &late).is_none());
    assert!(path.order(8, &trigger).is_none());
    assert_eq!(path.held(), HOLD_CAP);
}
