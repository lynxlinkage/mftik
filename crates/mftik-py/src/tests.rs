//! Conversion tests for the Python types. Not a port of the Go tests.

use std::collections::HashMap;
use std::time::Instant;

use mftik_core::mdpath::{Md, ReadError, TapeQuery, Via};
use mftik_core::symbol::{self, Filters, Info, Status, UniversalTicker};
use mftik_core::tdpath::{
    BalanceEvent, FillEvent, NewOrder, OrderEvent, Origin, PositionEvent, ReconEvent, RejectEvent,
    RejectedRequest, Td,
};
use mftik_proto::wire::mftiko::common::v1::{self as common, Error as ProtoError};
use mftik_proto::wire::mftiko::md::v1::{self as md, feed_event};
use mftik_proto::wire::mftiko::td::v1 as td;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule, PyTuple};

use crate::error::{
    ContextError, Error as PyError, NotInInit, ReadError as PyReadError, context_error,
};
use crate::proto::ToPyMessage;
use crate::sdk::{
    PyBalanceEvent, PyBookUpdate, PyFillEvent, PyInfo, PyOrder, PyOrderEvent, PyPositionEvent,
    PyReconEvent, PyRejectEvent, PyTapeQuery,
};
use crate::{feed_to_py, init_module, read_error};

pub(crate) fn install_mftik(py: Python<'_>) {
    let module = PyModule::new(py, "mftik").unwrap();
    py.import("sys")
        .unwrap()
        .getattr("modules")
        .unwrap()
        .set_item("mftik", &module)
        .unwrap();
    init_module(py, &module).unwrap();
}

#[test]
fn topic_constants_are_the_runtime_strings() {
    with_mftik(|py| {
        let topic = py.import("mftik").unwrap().getattr("topic").unwrap();
        let pairs = [
            ("BEST_QUOTE", mftik_rt::topic::BEST_QUOTE),
            ("TRADE", mftik_rt::topic::TRADE),
            ("AGG_TRADE", mftik_rt::topic::AGG_TRADE),
            ("ORDER_BOOK_DELTA", mftik_rt::topic::ORDER_BOOK_DELTA),
            ("TICKER", mftik_rt::topic::TICKER),
            ("LIQUIDATION", mftik_rt::topic::LIQUIDATION),
            ("FUNDING_RATE", mftik_rt::topic::FUNDING_RATE),
            ("OPEN_INTEREST", mftik_rt::topic::OPEN_INTEREST),
            ("GREEKS", mftik_rt::topic::GREEKS),
            ("MARK_PRICE", mftik_rt::topic::MARK_PRICE),
        ];
        for (name, value) in pairs {
            let got: String = topic.getattr(name).unwrap().extract().unwrap();
            assert_eq!(got, value, "{name}");
        }
        let one_minute: String = topic
            .call_method1("kline", ("1m",))
            .unwrap()
            .extract()
            .unwrap();
        let empty: String = topic
            .call_method1("kline", ("",))
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(one_minute, "kline_1m");
        assert_eq!(empty, "kline_");
        assert_eq!(one_minute, mftik_rt::topic::kline("1m"));
    });
}

pub(crate) fn with_mftik(body: impl FnOnce(Python<'_>)) {
    let _guard = crate::lock_python_tests();
    // Tests embed an interpreter. The wheel does not: auto-initialize stays off.
    Python::initialize();
    Python::attach(|py| {
        install_mftik(py);
        body(py);
    });
}

fn trade(side: i32) -> md::Trade {
    md::Trade {
        meta: None,
        trade_id: "t1".to_owned(),
        price: 1.0,
        qty: 2.0,
        side,
    }
}

fn kline(quote_volume: Option<f64>) -> md::Kline {
    md::Kline {
        meta: None,
        interval: String::new(),
        open_time_ns: 0,
        open: 0.0,
        high: 0.0,
        low: 0.0,
        close: 0.0,
        volume: 0.0,
        quote_volume,
        closed: false,
    }
}

fn order(client_order_id: &str, price: Option<f64>, status: i32) -> td::Order {
    td::Order {
        uid: 0,
        order_id: String::new(),
        client_order_id: client_order_id.to_owned(),
        side: 0,
        order_type: 0,
        status,
        qty: 1.0,
        quote_qty: None,
        price,
        filled_qty: 0.0,
        avg_price: None,
        exch_ts_ns: 0,
        td_ts_ns: 0,
        cancel_sent_ns: 0,
    }
}

fn attr<'py>(obj: &Bound<'py, PyAny>, name: &str) -> Bound<'py, PyAny> {
    obj.getattr(name).unwrap()
}

fn none(obj: &Bound<'_, PyAny>, name: &str) -> bool {
    attr(obj, name).is_none()
}

#[test]
fn best_quote_copies_fields_and_leaves_meta_unset() {
    with_mftik(|py| {
        let quote = md::BestQuote {
            meta: None,
            bid: 1.0,
            bid_qty: 2.0,
            ask: 3.0,
            ask_qty: 4.0,
        };
        let obj = quote.to_py(py).unwrap();
        let obj = obj.bind(py);
        assert!(none(obj, "meta"));
        assert_eq!(attr(obj, "bid").extract::<f64>().unwrap(), 1.0);
        assert_eq!(attr(obj, "bid_qty").extract::<f64>().unwrap(), 2.0);
        assert_eq!(attr(obj, "ask").extract::<f64>().unwrap(), 3.0);
        assert_eq!(attr(obj, "ask_qty").extract::<f64>().unwrap(), 4.0);
        let again = quote.to_py(py).unwrap();
        assert!(obj.eq(again.bind(py)).unwrap());
        let text = obj.repr().unwrap();
        assert!(text.to_str().unwrap().starts_with("BestQuote("));
    });
}

#[test]
fn trade_keeps_known_and_unknown_sides() {
    with_mftik(|py| {
        let known = trade(common::Side::Buy as i32);
        let obj = known.to_py(py).unwrap();
        let side = attr(obj.bind(py), "side");
        assert_eq!(side.extract::<i32>().unwrap(), 1);
        assert_eq!(attr(&side, "name").extract::<String>().unwrap(), "SIDE_BUY");

        let unknown = attr(trade(99).to_py(py).unwrap().bind(py), "side");
        assert_eq!(unknown.extract::<i32>().unwrap(), 99);
        assert!(unknown.eq(99).unwrap());
    });
}

#[test]
fn optional_oneof_repeated_and_map_follow_the_field_rules() {
    with_mftik(|py| {
        assert!(none(
            kline(None).to_py(py).unwrap().bind(py),
            "quote_volume"
        ));
        assert_eq!(
            attr(kline(Some(1.0)).to_py(py).unwrap().bind(py), "quote_volume")
                .extract::<f64>()
                .unwrap(),
            1.0
        );

        let event = md::FeedEvent {
            uid: 1,
            event: Some(feed_event::Event::BestQuote(md::BestQuote {
                meta: None,
                bid: 1.0,
                bid_qty: 1.0,
                ask: 2.0,
                ask_qty: 1.0,
            })),
        };
        let obj = event.to_py(py).unwrap();
        let obj = obj.bind(py);
        assert_eq!(
            attr(obj, "event").extract::<String>().unwrap(),
            "best_quote"
        );
        assert!(!none(obj, "best_quote"));
        assert!(none(obj, "trade"));

        let delta = md::OrderBookDelta {
            meta: None,
            bids: vec![md::Level {
                price: 1.0,
                qty: 2.0,
            }],
            asks: Vec::new(),
            reset: false,
        };
        let obj = delta.to_py(py).unwrap();
        let first = attr(obj.bind(py), "bids");
        let second = attr(obj.bind(py), "bids");
        assert!(first.cast::<PyTuple>().is_ok());
        assert!(!first.is(&second));
        assert_eq!(first.len().unwrap(), 1);

        let mut details = HashMap::new();
        details.insert("a".to_owned(), "b".to_owned());
        let err = ProtoError {
            code: String::new(),
            message: String::new(),
            retryable: false,
            details,
        };
        let obj = err.to_py(py).unwrap();
        let first = attr(obj.bind(py), "details");
        let dict = first.cast::<PyDict>().unwrap();
        dict.set_item("a", "c").unwrap();
        let second = attr(obj.bind(py), "details");
        assert_eq!(
            second
                .cast::<PyDict>()
                .unwrap()
                .get_item("a")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "b"
        );
    });
}

#[test]
fn hook_payloads_and_pages_round_trip_a_field() {
    with_mftik(|py| {
        let page = md::TapePage {
            events: vec![md::TapeEvent {
                conn_epoch: 0,
                seq: 0,
                md_ts_ns: 0,
                event: None,
            }],
            more: true,
            next: None,
            continuous_since_ns: 0,
            gaps: Vec::new(),
            head: None,
        };
        assert!(
            attr(page.to_py(py).unwrap().bind(py), "more")
                .extract::<bool>()
                .unwrap()
        );

        let book = md::BookSnapshot {
            meta: None,
            bids: Vec::new(),
            asks: Vec::new(),
            conn_epoch: 0,
            seq: 2,
            md_ts_ns: 0,
            uid: 1,
        };
        let obj = book.to_py(py).unwrap();
        assert_eq!(attr(obj.bind(py), "uid").extract::<u32>().unwrap(), 1);
        assert_eq!(attr(obj.bind(py), "seq").extract::<u64>().unwrap(), 2);

        let klines = md::Klines {
            klines: vec![kline(None)],
        };
        assert_eq!(
            attr(klines.to_py(py).unwrap().bind(py), "klines")
                .len()
                .unwrap(),
            1
        );

        let obj = order("c1", None, td::OrderStatus::Accepted as i32)
            .to_py(py)
            .unwrap();
        assert!(none(obj.bind(py), "price"));
        assert_eq!(
            attr(obj.bind(py), "client_order_id")
                .extract::<String>()
                .unwrap(),
            "c1"
        );
        assert_eq!(
            attr(obj.bind(py), "status").extract::<i32>().unwrap(),
            td::OrderStatus::Accepted as i32
        );

        for (name, obj) in [
            (
                "position",
                td::Position {
                    uid: 0,
                    size: 0.0,
                    entry_price: None,
                    mark_price: None,
                    unrealized_pnl: None,
                    leverage: None,
                }
                .to_py(py)
                .unwrap(),
            ),
            (
                "balance",
                td::Balance {
                    asset: String::new(),
                    free: 0.0,
                    locked: 0.0,
                    prelock: 0.0,
                }
                .to_py(py)
                .unwrap(),
            ),
            (
                "fill",
                td::Fill {
                    uid: 0,
                    fill_id: String::new(),
                    order_id: String::new(),
                    client_order_id: String::new(),
                    side: 0,
                    price: 0.0,
                    qty: 0.0,
                    fee: 0.0,
                    fee_asset: String::new(),
                    liquidity: String::new(),
                    exch_ts_ns: 0,
                }
                .to_py(py)
                .unwrap(),
            ),
            (
                "recon",
                td::Recon {
                    incarnation: 0,
                    reason: 0,
                    open_orders: Vec::new(),
                    positions: Vec::new(),
                    balances: Vec::new(),
                    as_of_ns: 0,
                }
                .to_py(py)
                .unwrap(),
            ),
        ] {
            assert!(
                obj.bind(py).repr().unwrap().to_str().unwrap().contains('('),
                "{name}"
            );
        }
    });
}

fn sample_td() -> Td {
    Td {
        api_id: 1,
        incarnation: 2,
        seq: 3,
    }
}

#[test]
fn each_sdk_event_keeps_its_fields() {
    with_mftik(|py| {
        let md = Md {
            uid: 1,
            md_ts_ns: 2,
            conn_epoch: 3,
            seq: 4,
        };
        let quote = md::BestQuote {
            meta: None,
            bid: 1.0,
            bid_qty: 1.0,
            ask: 2.0,
            ask_qty: 1.0,
        };
        let feed = feed_to_py(py, md, quote).unwrap();
        let feed = feed.bind(py);
        assert_eq!(attr(&attr(feed, "md"), "uid").extract::<u32>().unwrap(), 1);
        assert_eq!(
            attr(&attr(feed, "data"), "bid").extract::<f64>().unwrap(),
            1.0
        );

        let book = PyBookUpdate::from_update(py, md, 5).unwrap();
        assert_eq!(
            attr(book.bind(py), "exch_ts_ns").extract::<i64>().unwrap(),
            5
        );

        let event = PyOrderEvent::from_event(
            py,
            &OrderEvent {
                td: sample_td(),
                origin: Origin::FromTd,
                order: order("c1", None, 0),
                cause: 99,
                code: 1,
                reason: "a".to_owned(),
            },
        )
        .unwrap();
        let event = event.bind(py);
        assert_eq!(
            attr(&attr(event, "td"), "api_id").extract::<i64>().unwrap(),
            1
        );
        assert_eq!(attr(event, "origin").extract::<i32>().unwrap(), 1);
        assert_eq!(attr(event, "cause").extract::<i32>().unwrap(), 99);
        assert_eq!(
            attr(&attr(event, "order"), "client_order_id")
                .extract::<String>()
                .unwrap(),
            "c1"
        );

        let fill = td::Fill {
            uid: 0,
            fill_id: "t1".to_owned(),
            order_id: String::new(),
            client_order_id: String::new(),
            side: 0,
            price: 0.0,
            qty: 0.0,
            fee: 0.0,
            fee_asset: String::new(),
            liquidity: String::new(),
            exch_ts_ns: 0,
        };
        let event = PyFillEvent::from_event(
            py,
            &FillEvent {
                td: sample_td(),
                fill,
            },
        )
        .unwrap();
        assert_eq!(
            attr(&attr(event.bind(py), "fill"), "fill_id")
                .extract::<String>()
                .unwrap(),
            "t1"
        );

        let position = td::Position {
            uid: 1,
            size: 0.0,
            entry_price: None,
            mark_price: None,
            unrealized_pnl: None,
            leverage: None,
        };
        let event = PyPositionEvent::from_event(
            py,
            &PositionEvent {
                td: sample_td(),
                positions: vec![position],
            },
        )
        .unwrap();
        assert_eq!(attr(event.bind(py), "positions").len().unwrap(), 1);

        let balance = td::Balance {
            asset: "a".to_owned(),
            free: 0.0,
            locked: 0.0,
            prelock: 0.0,
        };
        let event = PyBalanceEvent::from_event(
            py,
            &BalanceEvent {
                td: sample_td(),
                balances: vec![balance],
            },
        )
        .unwrap();
        assert_eq!(
            attr(
                &attr(event.bind(py), "balances").get_item(0).unwrap(),
                "asset"
            )
            .extract::<String>()
            .unwrap(),
            "a"
        );

        let recon = td::Recon {
            incarnation: 2,
            reason: 0,
            open_orders: Vec::new(),
            positions: Vec::new(),
            balances: Vec::new(),
            as_of_ns: 0,
        };
        let event = PyReconEvent::from_event(
            py,
            &ReconEvent {
                td: sample_td(),
                recon,
            },
        )
        .unwrap();
        assert_eq!(
            attr(&attr(event.bind(py), "recon"), "incarnation")
                .extract::<u32>()
                .unwrap(),
            2
        );

        let event = PyRejectEvent::from_event(
            py,
            &RejectEvent {
                td: sample_td(),
                request: RejectedRequest::BatchCancel,
                client_order_ids: vec!["c1".to_owned()],
                code: 1,
                reason: "a".to_owned(),
            },
        )
        .unwrap();
        let event = event.bind(py);
        assert_eq!(attr(event, "request").extract::<i32>().unwrap(), 2);
        assert_eq!(attr(event, "client_order_ids").len().unwrap(), 1);
        let first = attr(event, "client_order_ids");
        let second = attr(event, "client_order_ids");
        assert!(!first.is(&second));
    });
}

#[test]
fn info_order_and_tape_query_convert() {
    with_mftik(|py| {
        let mut filters = Filters::new();
        filters.insert(symbol::PRICE_TICK.to_owned(), 0.5);
        let info = Info {
            uid: 1,
            ticker: UniversalTicker::parse("Bybit_Perp_BTCUSDT").unwrap(),
            exch_ticker: "a".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            contract_size: 1.0,
            contract_value_ccy: "BTC".to_owned(),
            price_ccy: "USDT".to_owned(),
            settle: "USDT".to_owned(),
            expiry_ns: 0,
            strike: 0.0,
            option_type: String::new(),
            status: Status::Tradable,
            filters,
            updated_at_ns: 1,
        };
        let obj = PyInfo::from_info(py, &info).unwrap();
        let obj = obj.bind(py);
        assert_eq!(attr(obj, "status").extract::<String>().unwrap(), "TRADABLE");
        assert!(
            obj.call_method0("is_tradable")
                .unwrap()
                .extract::<bool>()
                .unwrap()
        );
        assert_eq!(
            obj.call_method0("price_tick")
                .unwrap()
                .extract::<f64>()
                .unwrap(),
            0.5
        );
        assert!(
            obj.call_method0("linear")
                .unwrap()
                .extract::<bool>()
                .unwrap()
        );
        let copied = attr(obj, "filters");
        copied.set_item(symbol::PRICE_TICK, 9.0).unwrap();
        assert_eq!(
            attr(obj, "filters")
                .get_item(symbol::PRICE_TICK)
                .unwrap()
                .extract::<f64>()
                .unwrap(),
            0.5
        );

        let mut params = HashMap::new();
        params.insert("a".to_owned(), "b".to_owned());
        let order = NewOrder {
            api_id: 1,
            algo_id: 2,
            uid: 3,
            side: common::Side::Sell,
            order_type: common::OrderType::Limit,
            tif: common::TimeInForce::Gtc,
            qty: Some(f64::NAN),
            quote_qty: None,
            price: Some(1.0),
            recv_window_ms: 4,
            reduce_only: true,
            params,
        };
        let obj = PyOrder::from_new(py, &order).unwrap();
        let view = obj.bind(py);
        assert!(attr(view, "qty").is_none());
        assert_eq!(attr(view, "price").extract::<f64>().unwrap(), 1.0);
        assert_eq!(
            attr(view, "side").extract::<i32>().unwrap(),
            common::Side::Sell as i32
        );
        attr(view, "params").set_item("a", "c").unwrap();
        assert_eq!(
            attr(view, "params")
                .get_item("a")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "b"
        );
        let built = obj.borrow(py).to_new().unwrap();
        assert_eq!(built.price, Some(1.0));
        assert!(built.qty.is_none());
        assert_eq!(built.params.get("a").map(String::as_str), Some("b"));

        let classes = py.import("sys").unwrap().getattr("modules").unwrap();
        let order_cls = classes.get_item("mftik").unwrap().getattr("Order").unwrap();
        let kwargs = PyDict::new(py);
        kwargs.set_item("side", 99).unwrap();
        let unknown = order_cls.call((), Some(&kwargs)).unwrap();
        let err = unknown
            .cast::<PyOrder>()
            .unwrap()
            .borrow()
            .to_new()
            .unwrap_err();
        assert_eq!(err.field, "side");
        assert_eq!(err.value, 99);

        let query = PyTapeQuery::from_query(
            py,
            TapeQuery {
                limit: 1,
                before: Some(md::Cursor {
                    conn_epoch: 2,
                    seq: 3,
                }),
                since_ns: 4,
            },
        )
        .unwrap();
        assert_eq!(query.borrow(py).to_query().before.unwrap().seq, 3);
    });
}

#[test]
fn context_and_read_errors_use_the_go_text() {
    with_mftik(|py| {
        let cases = [
            (
                mftik_rt::Error::NotInInit,
                "sdk: only OnInit and OnStart may call this",
                true,
            ),
            (
                mftik_rt::Error::UnknownUid,
                "sdk: the instrument is not known to this session",
                false,
            ),
            (
                mftik_rt::Error::UnknownAccount,
                "sdk: the account is not bound to this session",
                false,
            ),
            (
                mftik_rt::Error::AlgoNotDeclared,
                "sdk: the algo_id is not declared for this account",
                false,
            ),
            (
                mftik_rt::Error::NoFeed,
                "sdk: the session has no such feed",
                false,
            ),
            (
                mftik_rt::Error::Stopping,
                "sdk: the session is stopping",
                false,
            ),
            (
                mftik_rt::Error::NotWired("td"),
                "sdk: the td path is not wired yet",
                false,
            ),
            (mftik_rt::Error::Other("a".to_owned()), "a", false),
        ];
        for (err, text, _) in cases {
            let py_err = context_error(&err);
            assert_eq!(py_err.value(py).str().unwrap().to_str().unwrap(), text);
            assert!(
                py_err
                    .value(py)
                    .is_instance(&py.get_type::<PyError>())
                    .unwrap()
            );
        }
        let not_init = context_error(&mftik_rt::Error::NotInInit);
        assert!(
            not_init
                .value(py)
                .is_instance(&py.get_type::<NotInInit>())
                .unwrap()
        );
        let other = context_error(&mftik_rt::Error::Other("a".to_owned()));
        assert!(
            !other
                .value(py)
                .is_instance(&py.get_type::<NotInInit>())
                .unwrap()
        );

        let read = read_error(&ReadError::Timeout { via: Via::View });
        assert_eq!(
            read.value(py).str().unwrap().to_str().unwrap(),
            "mdpath: md.view: context deadline exceeded"
        );
        assert!(
            read.value(py)
                .is_instance(&py.get_type::<PyReadError>())
                .unwrap()
        );
        let missing = read_error(&ReadError::NoResponder { via: Via::Fetch });
        assert_eq!(
            missing.value(py).str().unwrap().to_str().unwrap(),
            "mdpath: md.fetch: nats: no responders available for request"
        );
        assert!(
            py.get_type::<ContextError>()
                .is_subclass(&py.get_type::<PyError>())
                .unwrap()
        );
    });
}

#[test]
fn conversion_cost_best_quote_trade_order_event() {
    with_mftik(|py| {
        let quote = md::BestQuote {
            meta: Some(md::Meta { exch_ts_ns: 1 }),
            bid: 1.0,
            bid_qty: 2.0,
            ask: 3.0,
            ask_qty: 4.0,
        };
        let sample_trade = trade(common::Side::Buy as i32);
        let event = OrderEvent {
            td: sample_td(),
            origin: Origin::LocalReject,
            order: order("c1", None, 0),
            cause: td::OrderCause::Rejected as i32,
            code: 1,
            reason: "a".to_owned(),
        };
        let rounds = 1000_u128;
        let started = Instant::now();
        for _ in 0..rounds {
            quote.to_py(py).unwrap();
        }
        let best_quote_ns = started.elapsed().as_nanos() / rounds;
        let started = Instant::now();
        for _ in 0..rounds {
            sample_trade.clone().to_py(py).unwrap();
        }
        let trade_ns = started.elapsed().as_nanos() / rounds;
        let started = Instant::now();
        for _ in 0..rounds {
            PyOrderEvent::from_event(py, &event).unwrap();
        }
        let order_event_ns = started.elapsed().as_nanos() / rounds;
        eprintln!(
            "conversion ns/call best_quote={best_quote_ns} trade={trade_ns} order_event={order_event_ns}"
        );
    });
}
