//! Delivery of the 17 optional hooks (E04-03). Not a port of the Go tests.
//!
//! These call the trait methods the runtime calls. A session cannot reach
//! `on_book` without `ctx.resolve` or `on_reject` without `ctx.cancel`; both
//! are E04-04. The trait call is the delivery those paths make after the book
//! is synced or a cancel is refused locally.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

use mftik_proto::wire::mftiko::common::v1 as common;
use mftik_proto::wire::mftiko::md::v1 as md;
use mftik_proto::wire::mftiko::td::v1 as td;
use mftik_rt::{
    AggTrade, AggTradeHandler, BalanceEvent, BalanceHandler, BestQuote, BestQuoteHandler,
    BookHandler, BookUpdate, FillEvent, FillHandler, FundingRate, FundingRateHandler, Greeks,
    GreeksHandler, Handlers, Kline, KlineHandler, Liquidation, LiquidationHandler, MarkPrice,
    MarkPriceHandler, Md, OpenInterest, OpenInterestHandler, OrderEvent, OrderHandler, Origin,
    PositionEvent, PositionHandler, ReconEvent, ReconHandler, RejectEvent, RejectHandler,
    RejectedRequest, Strategy, Td, Ticker, TickerHandler, Trade, TradeHandler,
};
use pyo3::prelude::*;
use pyo3::types::PyDictMethods;

use crate::strategy::PyStrategy;
use crate::tests::{install_mftik, with_mftik};

const PRELUDE: &str = r#"
import json
import sys
import types

def _sync(self, ctx, ev=None):
    return None

class Strategy:
    pass

Strategy.__module__ = "mftik.strategy"
for _name in (
    "on_init",
    "on_start",
    "on_stop",
    "on_best_quote",
    "on_trade",
    "on_agg_trade",
    "on_kline",
    "on_ticker",
    "on_liquidation",
    "on_funding_rate",
    "on_open_interest",
    "on_greeks",
    "on_mark_price",
    "on_book",
    "on_order",
    "on_fill",
    "on_position",
    "on_balance",
    "on_recon",
    "on_reject",
):
    setattr(Strategy, _name, _sync)

_mod = types.ModuleType("mftik.strategy")
_mod.Strategy = Strategy
sys.modules["mftik.strategy"] = _mod
seen = []
held = []

def frozen(obj, name):
    try:
        setattr(obj, name, 0)
    except Exception:
        return "frozen"
    return "mutable"

def rec(hook, ev=None, **fields):
    if ev is not None:
        held.append(ev)
        fields["obj"] = id(ev)
        data = getattr(ev, "data", None)
        if data is not None:
            held.append(data)
            fields["data"] = id(data)
    seen.append(json.dumps({"hook": hook, **fields}, sort_keys=True))
"#;

const ALL: &str = r#"
class All(Strategy):
    def on_init(self, ctx):
        rec("init")
        return 1

    def on_start(self, ctx):
        rec("start")
        return 1

    def on_stop(self, ctx):
        rec("stop")
        return 1

    def on_best_quote(self, ctx, ev):
        rec(
            "best_quote",
            ev,
            bid=ev.data.bid,
            conn=ev.md.conn_epoch,
            frozen=frozen(ev.data, "bid"),
            md_ts=ev.md.md_ts_ns,
            seq=ev.md.seq,
            uid=ev.md.uid,
        )
        return 1

    def on_trade(self, ctx, ev):
        rec(
            "trade",
            ev,
            frozen=frozen(ev.data, "trade_id"),
            price=ev.data.price,
            seq=ev.md.seq,
            side=int(ev.data.side),
            trade_id=ev.data.trade_id,
            uid=ev.md.uid,
        )
        return 1

    def on_agg_trade(self, ctx, ev):
        rec(
            "agg_trade",
            ev,
            count=ev.data.match_count,
            price=ev.data.price,
            trade_id=ev.data.trade_id,
        )
        return 1

    def on_kline(self, ctx, ev):
        rec(
            "kline",
            ev,
            close=ev.data.close,
            closed=ev.data.closed,
            interval=ev.data.interval,
            quote=ev.data.quote_volume,
        )
        return 1

    def on_ticker(self, ctx, ev):
        rec("ticker", ev, last=ev.data.last)
        return 1

    def on_liquidation(self, ctx, ev):
        rec(
            "liquidation",
            ev,
            price=ev.data.price,
            qty=ev.data.qty,
            side=int(ev.data.side),
        )
        return 1

    def on_funding_rate(self, ctx, ev):
        rec("funding_rate", ev, next=ev.data.next_funding_ns, rate=ev.data.rate)
        return 1

    def on_open_interest(self, ctx, ev):
        rec("open_interest", ev, oi=ev.data.open_interest)
        return 1

    def on_greeks(self, ctx, ev):
        rec("greeks", ev, delta=ev.data.delta)
        return 1

    def on_mark_price(self, ctx, ev):
        rec("mark_price", ev, mark=ev.data.mark_price)
        return 1

    def on_book(self, ctx, ev):
        rec(
            "book",
            ev,
            exch=ev.exch_ts_ns,
            frozen=frozen(ev, "exch_ts_ns"),
            seq=ev.md.seq,
            uid=ev.md.uid,
        )
        return 1

    def on_order(self, ctx, ev):
        rec(
            "order",
            ev,
            api=ev.td.api_id,
            cause=int(ev.cause),
            cloid=ev.order.client_order_id,
            code=ev.code,
            frozen=frozen(ev, "reason"),
            origin=int(ev.origin),
            reason=ev.reason,
            seq=ev.td.seq,
            status=int(ev.order.status),
            uid=ev.order.uid,
        )
        return 1

    def on_fill(self, ctx, ev):
        rec(
            "fill",
            ev,
            api=ev.td.api_id,
            fill_id=ev.fill.fill_id,
            qty=ev.fill.qty,
            seq=ev.td.seq,
        )
        return 1

    def on_position(self, ctx, ev):
        rec(
            "position",
            ev,
            api=ev.td.api_id,
            seq=ev.td.seq,
            size=ev.positions[0].size,
            uid=ev.positions[0].uid,
        )
        return 1

    def on_balance(self, ctx, ev):
        rec(
            "balance",
            ev,
            api=ev.td.api_id,
            asset=ev.balances[0].asset,
            free=ev.balances[0].free,
            seq=ev.td.seq,
        )
        return 1

    def on_recon(self, ctx, ev):
        rec(
            "recon",
            ev,
            api=ev.td.api_id,
            asset=ev.recon.balances[0].asset,
            free=ev.recon.balances[0].free,
            inc=ev.td.incarnation,
            reason=int(ev.recon.reason),
            seq=ev.td.seq,
            size=ev.recon.positions[0].size,
        )
        return 1

    def on_reject(self, ctx, ev):
        rec(
            "reject",
            ev,
            api=ev.td.api_id,
            cloid=ev.client_order_ids[0],
            code=ev.code,
            reason=ev.reason,
            request=int(ev.request),
            seq=ev.td.seq,
        )
        return 1

obj = All()
"#;

#[test]
fn each_hook_delivers_a_new_frozen_event_and_ignores_the_return() {
    with_mftik(|py| {
        let (obj, seen) = load(py, ALL);
        let text = capture_stderr(py, || {
            mftik_rt::in_one_hook(PyStrategy::new(obj), |strategy, ctx| {
                strategy.on_init(ctx).unwrap();
                strategy.on_start(ctx).unwrap();
                deliver_one(strategy, ctx);
                strategy.on_best_quote(ctx, quote(2, 12.0));
                strategy.on_trade(ctx, trade("t2", 4));
                strategy.on_order(ctx, order("c2", 7, 5));
                strategy.on_stop(ctx);
            })
            .unwrap();
        });
        assert!(!text.contains("Traceback"), "delivery raised\n{text}");
        assert_delivered(&lines(py, &seen));
    });
}

#[test]
fn an_optional_hook_exception_panics_with_the_type_and_text() {
    with_mftik(|py| {
        let (obj, seen) = load(
            py,
            r#"
class Boom(Strategy):
    def on_trade(self, ctx, ev):
        seen.append(ev.data.trade_id)
        raise RuntimeError("trade broke")

obj = Boom()
"#,
        );
        let text = capture_stderr(py, || {
            mftik_rt::in_one_hook(PyStrategy::new(obj), |strategy, ctx| {
                let payload = catch_unwind(AssertUnwindSafe(|| {
                    strategy.on_trade(ctx, trade("t1", 3));
                }))
                .expect_err("the hook panics");
                assert_eq!(panic_message(payload.as_ref()), "RuntimeError: trade broke");
            })
            .unwrap();
        });
        assert!(text.contains("Traceback"), "{text}");
        assert!(text.contains("RuntimeError: trade broke"), "{text}");
        assert_eq!(lines(py, &seen), vec!["t1".to_owned()]);
    });
}

#[test]
fn async_hooks_are_rejected_before_on_init_and_register_nothing() {
    with_mftik(|py| {
        assert_rejected(
            py,
            r#"
class Case(Strategy):
    def on_init(self, ctx):
        seen.append("init")
        return None

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        return None

    async def on_trade(self, ctx, ev):
        seen.append("trade")

obj = Case()
"#,
            "TypeError: on_trade is async",
        );
        assert_rejected(
            py,
            r#"
class Case(Strategy):
    async def on_init(self, ctx):
        seen.append("init")

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        return None

    async def on_trade(self, ctx, ev):
        seen.append("trade")

obj = Case()
"#,
            "TypeError: on_init is async",
        );
        assert_rejected(
            py,
            r#"
class Case(Strategy):
    def on_init(self, ctx):
        seen.append("init")
        return None

    def on_start(self, ctx):
        return None

    async def on_stop(self, ctx):
        seen.append("stop")

    def on_trade(self, ctx, ev):
        seen.append("trade")
        return None

obj = Case()
"#,
            "TypeError: on_stop is async",
        );
    });
}

/// Conversion through the Python return. No threshold (E04-D034, E02-D004).
#[test]
fn hook_delivery_cost_best_quote_trade_order_event() {
    let _guard = crate::lock_python_tests();
    Python::initialize();
    let obj = Python::attach(|py| {
        install_mftik(py);
        load(
            py,
            r#"
class Fast(Strategy):
    def on_best_quote(self, ctx, ev):
        return ev.data.bid

    def on_trade(self, ctx, ev):
        return ev.data.trade_id

    def on_order(self, ctx, ev):
        return ev.order.client_order_id

obj = Fast()
"#,
        )
        .0
    });
    let mut quotes: Vec<_> = (0..1020).map(|seq| quote(seq, 11.0)).collect();
    let mut trades: Vec<_> = (0..1020).map(|seq| trade("t1", seq)).collect();
    let mut orders: Vec<_> = (0..1020).map(|seq| order("c1", seq, 4)).collect();
    let mut measured = (0, 0, 0);
    mftik_rt::in_one_hook(PyStrategy::new(obj), |strategy, ctx| {
        measured = (
            measure(&mut quotes, |ev| strategy.on_best_quote(ctx, ev)),
            measure(&mut trades, |ev| strategy.on_trade(ctx, ev)),
            measure(&mut orders, |ev| strategy.on_order(ctx, ev)),
        );
    })
    .unwrap();
    eprintln!(
        "hook ns/call best_quote={} trade={} order_event={}",
        measured.0, measured.1, measured.2
    );
    assert!(measured.0 > 0 && measured.1 > 0 && measured.2 > 0);
}

fn deliver_one(strategy: &mut PyStrategy, ctx: &mftik_rt::Context<PyStrategy>) {
    strategy.on_best_quote(ctx, quote(1, 11.0));
    strategy.on_trade(ctx, trade("t1", 3));
    strategy.on_agg_trade(
        ctx,
        AggTrade {
            md: stamp(5),
            data: md::AggTrade {
                trade_id: "ag".into(),
                price: 11.0,
                qty: 2.0,
                side: common::Side::Buy as i32,
                first_trade_id: "a".into(),
                last_trade_id: "b".into(),
                match_count: 3,
                ..Default::default()
            },
        },
    );
    strategy.on_kline(
        ctx,
        Kline {
            md: stamp(6),
            data: md::Kline {
                interval: "1m".into(),
                close: 11.0,
                quote_volume: Some(2.5),
                closed: true,
                ..Default::default()
            },
        },
    );
    strategy.on_ticker(
        ctx,
        Ticker {
            md: stamp(7),
            data: md::Ticker {
                last: Some(11.0),
                ..Default::default()
            },
        },
    );
    strategy.on_liquidation(
        ctx,
        Liquidation {
            md: stamp(8),
            data: md::Liquidation {
                side: common::Side::Buy as i32,
                price: 11.0,
                qty: 2.0,
                ..Default::default()
            },
        },
    );
    strategy.on_funding_rate(
        ctx,
        FundingRate {
            md: stamp(9),
            data: md::FundingRate {
                rate: 0.25,
                next_funding_ns: 5,
                ..Default::default()
            },
        },
    );
    strategy.on_open_interest(
        ctx,
        OpenInterest {
            md: stamp(10),
            data: md::OpenInterest {
                open_interest: 11.0,
                ..Default::default()
            },
        },
    );
    strategy.on_greeks(
        ctx,
        Greeks {
            md: stamp(11),
            data: md::Greeks {
                delta: Some(0.5),
                ..Default::default()
            },
        },
    );
    strategy.on_mark_price(
        ctx,
        MarkPrice {
            md: stamp(12),
            data: md::MarkPrice {
                mark_price: 11.0,
                ..Default::default()
            },
        },
    );
    strategy.on_book(
        ctx,
        BookUpdate {
            md: stamp(4),
            exch_ts_ns: 9,
        },
    );
    strategy.on_order(ctx, order("c1", 6, 4));
    strategy.on_fill(
        ctx,
        FillEvent {
            td: td_stamp(8),
            fill: td::Fill {
                uid: 7,
                fill_id: "f1".into(),
                client_order_id: "c1".into(),
                qty: 1.0,
                price: 11.0,
                ..Default::default()
            },
        },
    );
    strategy.on_position(
        ctx,
        PositionEvent {
            td: td_stamp(9),
            positions: vec![td::Position {
                uid: 7,
                size: 3.0,
                ..Default::default()
            }],
        },
    );
    strategy.on_balance(
        ctx,
        BalanceEvent {
            td: td_stamp(10),
            balances: vec![td::Balance {
                asset: "usdt".into(),
                free: 5.0,
                ..Default::default()
            }],
        },
    );
    strategy.on_recon(
        ctx,
        ReconEvent {
            td: td_stamp(5),
            recon: td::Recon {
                incarnation: 3,
                reason: i32::from(td::ReconReason::Requested),
                positions: vec![td::Position {
                    uid: 7,
                    size: 2.0,
                    ..Default::default()
                }],
                balances: vec![td::Balance {
                    asset: "usdt".into(),
                    free: 4.0,
                    ..Default::default()
                }],
                ..Default::default()
            },
        },
    );
    strategy.on_reject(
        ctx,
        RejectEvent {
            td: Td {
                api_id: 7,
                incarnation: 0,
                seq: 0,
            },
            request: RejectedRequest::Cancel,
            client_order_ids: vec!["c1".into()],
            code: 6,
            reason: "refused".into(),
        },
    );
}

fn assert_delivered(rows: &[String]) {
    assert_eq!(named(rows, "init").len(), 1, "{rows:?}");
    assert_eq!(named(rows, "start").len(), 1, "{rows:?}");
    assert_eq!(named(rows, "stop").len(), 1, "{rows:?}");

    let quotes = named(rows, "best_quote");
    assert_eq!(quotes.len(), 2, "{rows:?}");
    assert_eq!(field(quotes[0], "bid"), "11.0");
    assert_eq!(field(quotes[0], "uid"), "7");
    assert_eq!(field(quotes[0], "seq"), "1");
    assert_eq!(field(quotes[0], "conn"), "1");
    assert_eq!(field(quotes[0], "md_ts"), "11");
    assert_eq!(field(quotes[0], "frozen"), "frozen");
    assert_eq!(field(quotes[1], "bid"), "12.0");
    assert_eq!(field(quotes[1], "seq"), "2");
    assert_ne!(field(quotes[0], "obj"), field(quotes[1], "obj"));
    assert_ne!(field(quotes[0], "data"), field(quotes[1], "data"));

    let trades = named(rows, "trade");
    assert_eq!(trades.len(), 2, "{rows:?}");
    assert_eq!(field(trades[0], "trade_id"), "t1");
    assert_eq!(field(trades[0], "price"), "11.0");
    assert_eq!(field(trades[0], "side"), "1");
    assert_eq!(field(trades[0], "uid"), "7");
    assert_eq!(field(trades[0], "seq"), "3");
    assert_eq!(field(trades[0], "frozen"), "frozen");
    assert_eq!(field(trades[1], "trade_id"), "t2");
    assert_ne!(field(trades[0], "obj"), field(trades[1], "obj"));
    assert_ne!(field(trades[0], "data"), field(trades[1], "data"));

    let agg = only(rows, "agg_trade");
    assert_eq!(field(agg, "trade_id"), "ag");
    assert_eq!(field(agg, "count"), "3");
    assert_eq!(field(agg, "price"), "11.0");

    let kline = only(rows, "kline");
    assert_eq!(field(kline, "interval"), "1m");
    assert_eq!(field(kline, "close"), "11.0");
    assert_eq!(field(kline, "closed"), "true");
    assert_eq!(field(kline, "quote"), "2.5");

    let ticker = only(rows, "ticker");
    assert_eq!(field(ticker, "last"), "11.0");

    let liquidation = only(rows, "liquidation");
    assert_eq!(field(liquidation, "price"), "11.0");
    assert_eq!(field(liquidation, "qty"), "2.0");
    assert_eq!(field(liquidation, "side"), "1");

    let funding = only(rows, "funding_rate");
    assert_eq!(field(funding, "rate"), "0.25");
    assert_eq!(field(funding, "next"), "5");

    assert_eq!(field(only(rows, "open_interest"), "oi"), "11.0");
    assert_eq!(field(only(rows, "greeks"), "delta"), "0.5");
    assert_eq!(field(only(rows, "mark_price"), "mark"), "11.0");

    let book = only(rows, "book");
    assert_eq!(field(book, "exch"), "9");
    assert_eq!(field(book, "uid"), "7");
    assert_eq!(field(book, "seq"), "4");
    assert_eq!(field(book, "frozen"), "frozen");

    let orders = named(rows, "order");
    assert_eq!(orders.len(), 2, "{rows:?}");
    assert_eq!(field(orders[0], "cloid"), "c1");
    assert_eq!(field(orders[0], "code"), "4");
    assert_eq!(field(orders[0], "seq"), "6");
    assert_eq!(field(orders[0], "api"), "7");
    assert_eq!(field(orders[0], "uid"), "7");
    assert_eq!(field(orders[0], "reason"), "ack");
    assert_eq!(field(orders[0], "frozen"), "frozen");
    assert_eq!(
        field(orders[0], "origin"),
        (Origin::FromTd as i32).to_string()
    );
    assert_eq!(
        field(orders[0], "cause"),
        i32::from(td::OrderCause::Acknowledged).to_string()
    );
    assert_eq!(
        field(orders[0], "status"),
        (td::OrderStatus::New as i32).to_string()
    );
    assert_eq!(field(orders[1], "cloid"), "c2");
    assert_eq!(field(orders[1], "code"), "5");
    assert_ne!(field(orders[0], "obj"), field(orders[1], "obj"));

    let fill = only(rows, "fill");
    assert_eq!(field(fill, "fill_id"), "f1");
    assert_eq!(field(fill, "qty"), "1.0");
    assert_eq!(field(fill, "api"), "7");
    assert_eq!(field(fill, "seq"), "8");

    let position = only(rows, "position");
    assert_eq!(field(position, "size"), "3.0");
    assert_eq!(field(position, "uid"), "7");
    assert_eq!(field(position, "seq"), "9");

    let balance = only(rows, "balance");
    assert_eq!(field(balance, "asset"), "usdt");
    assert_eq!(field(balance, "free"), "5.0");
    assert_eq!(field(balance, "seq"), "10");

    let recon = only(rows, "recon");
    assert_eq!(field(recon, "size"), "2.0");
    assert_eq!(field(recon, "free"), "4.0");
    assert_eq!(field(recon, "asset"), "usdt");
    assert_eq!(field(recon, "api"), "7");
    assert_eq!(field(recon, "inc"), "3");
    assert_eq!(field(recon, "seq"), "5");
    assert_eq!(
        field(recon, "reason"),
        i32::from(td::ReconReason::Requested).to_string()
    );

    let reject = only(rows, "reject");
    assert_eq!(field(reject, "cloid"), "c1");
    assert_eq!(field(reject, "code"), "6");
    assert_eq!(field(reject, "reason"), "refused");
    assert_eq!(field(reject, "api"), "7");
    assert_eq!(field(reject, "seq"), "0");
    assert_eq!(
        field(reject, "request"),
        (RejectedRequest::Cancel as i32).to_string()
    );
}

fn assert_rejected(py: Python<'_>, extra: &str, message: &str) {
    let (obj, seen) = load(py, extra);
    let text = capture_stderr(py, || {
        mftik_rt::in_one_hook(PyStrategy::new(obj), |strategy, ctx| {
            let err = strategy.on_init(ctx).expect_err("async hook");
            assert_eq!(err.to_string(), message);
            let mut handlers = Handlers::<PyStrategy>::default();
            strategy.handlers(&mut handlers);
            assert!(
                handlers.registered().is_empty(),
                "registered {:?}",
                handlers.registered()
            );
        })
        .unwrap();
    });
    assert!(text.contains("Traceback"), "{text}");
    assert!(text.contains(message), "{text}");
    assert_eq!(seen.bind(py).len().unwrap(), 0, "user hook ran\n{text}");
}

fn load(py: Python<'_>, extra: &str) -> (Py<PyAny>, Py<PyAny>) {
    let script = format!("{PRELUDE}\n{extra}");
    let locals = pyo3::types::PyDict::new(py);
    let code = std::ffi::CString::new(script).expect("script");
    py.run(code.as_c_str(), Some(&locals), Some(&locals))
        .expect("define strategy");
    let obj = locals.get_item("obj").expect("obj").expect("obj is set");
    let seen = locals.get_item("seen").expect("seen").expect("seen is set");
    (obj.unbind(), seen.unbind())
}

fn lines(py: Python<'_>, seen: &Py<PyAny>) -> Vec<String> {
    seen.bind(py)
        .try_iter()
        .unwrap()
        .map(|item| item.unwrap().extract().unwrap())
        .collect()
}

fn named<'a>(rows: &'a [String], hook: &str) -> Vec<&'a str> {
    let needle = format!("\"hook\": \"{hook}\"");
    rows.iter()
        .filter(|row| row.contains(&needle))
        .map(String::as_str)
        .collect()
}

fn only<'a>(rows: &'a [String], hook: &str) -> &'a str {
    let found = named(rows, hook);
    assert_eq!(found.len(), 1, "{hook} in {rows:?}");
    found[0]
}

fn field<'a>(line: &'a str, key: &str) -> &'a str {
    let pat = format!("\"{key}\": ");
    let rest = line
        .split(&pat)
        .nth(1)
        .unwrap_or_else(|| panic!("missing {key} in {line}"));
    if let Some(text) = rest.strip_prefix('"') {
        text.split('"').next().unwrap()
    } else {
        rest.split([',', '}']).next().unwrap().trim()
    }
}

fn capture_stderr(py: Python<'_>, body: impl FnOnce()) -> String {
    let sys = py.import("sys").unwrap();
    let old = sys.getattr("stderr").unwrap().unbind();
    let buf = py
        .import("io")
        .unwrap()
        .getattr("StringIO")
        .unwrap()
        .call0()
        .unwrap();
    sys.setattr("stderr", &buf).unwrap();
    struct Restore {
        old: Py<PyAny>,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            Python::attach(|py| {
                if let Ok(sys) = py.import("sys") {
                    let _ = sys.setattr("stderr", self.old.bind(py));
                }
            });
        }
    }
    let _restore = Restore { old };
    body();
    buf.call_method0("getvalue").unwrap().extract().unwrap()
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_owned()
    }
}

fn measure<T>(events: &mut Vec<T>, mut call: impl FnMut(T)) -> u128 {
    for _ in 0..20 {
        call(events.pop().expect("warmup"));
    }
    let started = Instant::now();
    for _ in 0..1000 {
        call(events.pop().expect("sample"));
    }
    started.elapsed().as_nanos() / 1000
}

fn stamp(seq: u64) -> Md {
    Md {
        uid: 7,
        md_ts_ns: 11,
        conn_epoch: 1,
        seq,
    }
}

fn td_stamp(seq: u64) -> Td {
    Td {
        api_id: 7,
        incarnation: 3,
        seq,
    }
}

fn quote(seq: u64, bid: f64) -> BestQuote {
    BestQuote {
        md: stamp(seq),
        data: md::BestQuote {
            bid,
            bid_qty: 2.0,
            ask: bid + 1.0,
            ask_qty: 4.0,
            ..Default::default()
        },
    }
}

fn trade(id: &str, seq: u64) -> Trade {
    Trade {
        md: stamp(seq),
        data: md::Trade {
            trade_id: id.into(),
            price: 11.0,
            qty: 2.0,
            side: common::Side::Buy as i32,
            ..Default::default()
        },
    }
}

fn order(cloid: &str, seq: u64, code: i32) -> OrderEvent {
    OrderEvent {
        td: td_stamp(seq),
        origin: Origin::FromTd,
        order: td::Order {
            uid: 7,
            client_order_id: cloid.into(),
            status: td::OrderStatus::New as i32,
            qty: 1.0,
            ..Default::default()
        },
        cause: i32::from(td::OrderCause::Acknowledged),
        code,
        reason: "ack".into(),
    }
}
