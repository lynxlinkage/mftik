//! The Python object as a [`mftik_rt::Strategy`] (E04-D003, E04-D004,
//! E04-D031–E04-D034).
//!
//! An optional hook is registered only when the strategy class's method is
//! not the `mftik.Strategy` method. That decision is made once, the first
//! time [`Strategy::handlers`] runs, which is before OnInit. An attribute
//! set on the instance does not count. An `async def` hook, including
//! `on_init`, `on_start`, and `on_stop`, is rejected then: no optional hook
//! is registered, and OnInit returns `TypeError: {hook} is async` without
//! calling the method. The SDK does not open an event loop.
//!
//! Each delivery builds a new frozen event and calls the method. A return
//! value is ignored. An exception in an optional hook panics with
//! `{type}: {str(e)}`, which the dispatcher reports as `hook panic: ...`.

use std::ffi::CString;
use std::sync::OnceLock;

use mftik_rt::{
    AggTrade, AggTradeHandler, BalanceEvent, BalanceHandler, BestQuote, BestQuoteHandler,
    BookHandler, BookUpdate, Context, FillEvent, FillHandler, FundingRate, FundingRateHandler,
    Greeks, GreeksHandler, Handlers, HookError, Kline, KlineHandler, Liquidation,
    LiquidationHandler, MarkPrice, MarkPriceHandler, OpenInterest, OpenInterestHandler, OrderEvent,
    OrderHandler, PositionEvent, PositionHandler, ReconEvent, ReconHandler, RejectEvent,
    RejectHandler, Strategy, Ticker, TickerHandler, Trade, TradeHandler,
};
use pyo3::prelude::*;

use crate::context::{call_hook, call_method, with_python_hook};
use crate::sdk::{
    PyBalanceEvent, PyBookUpdate, PyFillEvent, PyOrderEvent, PyPositionEvent, PyReconEvent,
    PyRejectEvent,
};
use crate::{describe, feed_to_py, write_traceback};

/// `codeobject.h`: `CO_COROUTINE` and `CO_ASYNC_GENERATOR`.
const CO_COROUTINE: i64 = 0x80;
const CO_ASYNC_GENERATOR: i64 = 0x200;

/// Required hooks first, then the optional hooks in registration order.
/// The first `async def` in this order is the one named in the `TypeError`.
const ASYNC_HOOKS: [&str; 20] = [
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
];

#[derive(Clone, Copy, Debug, Default)]
struct HookMask {
    on_best_quote: bool,
    on_trade: bool,
    on_agg_trade: bool,
    on_kline: bool,
    on_ticker: bool,
    on_liquidation: bool,
    on_funding_rate: bool,
    on_open_interest: bool,
    on_greeks: bool,
    on_mark_price: bool,
    on_book: bool,
    on_order: bool,
    on_fill: bool,
    on_position: bool,
    on_balance: bool,
    on_recon: bool,
    on_reject: bool,
}

struct Prepared {
    mask: HookMask,
    /// `TypeError: {hook} is async` when registration rejected the strategy.
    reject: Option<String>,
}

pub(crate) struct PyStrategy {
    obj: Py<PyAny>,
    prepared: OnceLock<Prepared>,
}

impl PyStrategy {
    pub(crate) fn new(obj: Py<PyAny>) -> Self {
        Self {
            obj,
            prepared: OnceLock::new(),
        }
    }

    fn prepared(&self) -> &Prepared {
        self.prepared
            .get_or_init(|| Python::attach(|py| prepare(py, &self.obj)))
    }

    fn call(&mut self, method: &str, ctx: &Context<Self>) -> Result<(), HookError> {
        let message = with_python_hook(ctx, |py, token| call_method(py, &self.obj, method, token));
        message.map_err(Into::into)
    }

    /// Converts inside the hook's GIL scope, then calls `method(ctx, event)`.
    fn deliver(
        &mut self,
        ctx: &Context<Self>,
        method: &'static str,
        make: impl FnOnce(Python<'_>) -> PyResult<Py<PyAny>>,
    ) {
        let message = with_python_hook(ctx, |py, token| match make(py) {
            Ok(event) => call_hook(py, &self.obj, method, token, event),
            Err(err) => Err(hook_failure(py, err)),
        });
        if let Err(message) = message {
            panic!("{message}");
        }
    }

    fn deliver_feed(
        &mut self,
        ctx: &Context<Self>,
        method: &'static str,
        md: mftik_rt::Md,
        data: impl crate::ToPyMessage,
    ) {
        self.deliver(ctx, method, move |py| feed_to_py(py, md, data));
    }
}

/// OnInit returns the load error. The entrypoint already wrote the traceback.
pub(crate) struct LoadFailed {
    pub(crate) error: String,
}

impl Strategy for LoadFailed {
    fn on_init(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> {
        Err(self.error.clone().into())
    }

    fn on_start(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }

    fn on_stop(&mut self, _ctx: &Context<Self>) {}
}

impl Strategy for PyStrategy {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        if let Some(message) = self.prepared().reject.clone() {
            return Err(message.into());
        }
        self.call("on_init", ctx)
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        self.call("on_start", ctx)
    }

    fn on_stop(&mut self, ctx: &Context<Self>) {
        if let Err(err) = self.call("on_stop", ctx) {
            panic!("{err}");
        }
    }

    fn handlers(&self, handlers: &mut Handlers<Self>) {
        let mask = self.prepared().mask;
        if mask.on_best_quote {
            handlers.best_quote();
        }
        if mask.on_trade {
            handlers.trade();
        }
        if mask.on_agg_trade {
            handlers.agg_trade();
        }
        if mask.on_kline {
            handlers.kline();
        }
        if mask.on_ticker {
            handlers.ticker();
        }
        if mask.on_liquidation {
            handlers.liquidation();
        }
        if mask.on_funding_rate {
            handlers.funding_rate();
        }
        if mask.on_open_interest {
            handlers.open_interest();
        }
        if mask.on_greeks {
            handlers.greeks();
        }
        if mask.on_mark_price {
            handlers.mark_price();
        }
        if mask.on_book {
            handlers.book();
        }
        if mask.on_order {
            handlers.order();
        }
        if mask.on_fill {
            handlers.fill();
        }
        if mask.on_position {
            handlers.position();
        }
        if mask.on_balance {
            handlers.balance();
        }
        if mask.on_recon {
            handlers.recon();
        }
        if mask.on_reject {
            handlers.reject();
        }
    }
}

fn hook_failure(py: Python<'_>, err: PyErr) -> String {
    let message = describe(py, &err);
    write_traceback(py, &err);
    message
}

macro_rules! feed_hook {
    ($trait:ident, $method:ident, $event:ty) => {
        impl $trait for PyStrategy {
            fn $method(&mut self, ctx: &Context<Self>, ev: $event) {
                let md = ev.md;
                let data = ev.data;
                self.deliver_feed(ctx, stringify!($method), md, data);
            }
        }
    };
}

feed_hook!(BestQuoteHandler, on_best_quote, BestQuote);
feed_hook!(TradeHandler, on_trade, Trade);
feed_hook!(AggTradeHandler, on_agg_trade, AggTrade);
feed_hook!(KlineHandler, on_kline, Kline);
feed_hook!(TickerHandler, on_ticker, Ticker);
feed_hook!(LiquidationHandler, on_liquidation, Liquidation);
feed_hook!(FundingRateHandler, on_funding_rate, FundingRate);
feed_hook!(OpenInterestHandler, on_open_interest, OpenInterest);
feed_hook!(GreeksHandler, on_greeks, Greeks);
feed_hook!(MarkPriceHandler, on_mark_price, MarkPrice);

impl BookHandler for PyStrategy {
    fn on_book(&mut self, ctx: &Context<Self>, ev: BookUpdate) {
        self.deliver(ctx, "on_book", move |py| {
            Ok(PyBookUpdate::from_update(py, ev.md, ev.exch_ts_ns)?.into_any())
        });
    }
}

macro_rules! event_hook {
    ($trait:ident, $method:ident, $event:ty, $pyty:ty) => {
        impl $trait for PyStrategy {
            fn $method(&mut self, ctx: &Context<Self>, ev: $event) {
                self.deliver(ctx, stringify!($method), move |py| {
                    Ok(<$pyty>::from_event(py, &ev)?.into_any())
                });
            }
        }
    };
}

event_hook!(OrderHandler, on_order, OrderEvent, PyOrderEvent);
event_hook!(FillHandler, on_fill, FillEvent, PyFillEvent);
event_hook!(PositionHandler, on_position, PositionEvent, PyPositionEvent);
event_hook!(BalanceHandler, on_balance, BalanceEvent, PyBalanceEvent);
event_hook!(ReconHandler, on_recon, ReconEvent, PyReconEvent);
event_hook!(RejectHandler, on_reject, RejectEvent, PyRejectEvent);

fn prepare(py: Python<'_>, obj: &Py<PyAny>) -> Prepared {
    let class = obj.bind(py).get_type().into_any();
    if let Some(reject) = first_async(py, &class) {
        return Prepared {
            mask: HookMask::default(),
            reject: Some(reject),
        };
    }
    let Some(base) = strategy_base(&class) else {
        return Prepared {
            mask: HookMask::default(),
            reject: None,
        };
    };
    Prepared {
        mask: read_mask(&class, &base),
        reject: None,
    }
}

fn read_mask(class: &Bound<'_, PyAny>, base: &Bound<'_, PyAny>) -> HookMask {
    HookMask {
        on_best_quote: overrides(class, base, "on_best_quote"),
        on_trade: overrides(class, base, "on_trade"),
        on_agg_trade: overrides(class, base, "on_agg_trade"),
        on_kline: overrides(class, base, "on_kline"),
        on_ticker: overrides(class, base, "on_ticker"),
        on_liquidation: overrides(class, base, "on_liquidation"),
        on_funding_rate: overrides(class, base, "on_funding_rate"),
        on_open_interest: overrides(class, base, "on_open_interest"),
        on_greeks: overrides(class, base, "on_greeks"),
        on_mark_price: overrides(class, base, "on_mark_price"),
        on_book: overrides(class, base, "on_book"),
        on_order: overrides(class, base, "on_order"),
        on_fill: overrides(class, base, "on_fill"),
        on_position: overrides(class, base, "on_position"),
        on_balance: overrides(class, base, "on_balance"),
        on_recon: overrides(class, base, "on_recon"),
        on_reject: overrides(class, base, "on_reject"),
    }
}

/// `type(obj).name is not Strategy.name` (E04-D031).
fn overrides(class: &Bound<'_, PyAny>, base: &Bound<'_, PyAny>, name: &str) -> bool {
    match (attr(class, name), attr(base, name)) {
        (Some(method), Some(base_method)) => !method.is(&base_method),
        _ => false,
    }
}

fn first_async(py: Python<'_>, class: &Bound<'_, PyAny>) -> Option<String> {
    for name in ASYNC_HOOKS {
        let Some(method) = attr(class, name) else {
            continue;
        };
        if is_async(&method) {
            return Some(reject_async(py, name));
        }
    }
    None
}

fn is_async(func: &Bound<'_, PyAny>) -> bool {
    let Some(code) = attr(func, "__code__") else {
        return false;
    };
    let Some(flags) = attr(&code, "co_flags") else {
        return false;
    };
    flags
        .extract::<i64>()
        .is_ok_and(|flags| flags & (CO_COROUTINE | CO_ASYNC_GENERATOR) != 0)
}

/// Raises so the traceback names the hook, then returns `TypeError: {hook} is async`.
///
/// `hook` is one of [`ASYNC_HOOKS`], so it needs no escaping.
fn reject_async(py: Python<'_>, hook: &str) -> String {
    let message = format!("TypeError: {hook} is async");
    let code = CString::new(format!("raise TypeError('{hook} is async')")).expect("hook name");
    let err = py
        .run(code.as_c_str(), None, None)
        .expect_err("async rejection raises");
    write_traceback(py, &err);
    message
}

fn strategy_base<'py>(class: &Bound<'py, PyAny>) -> Option<Bound<'py, PyAny>> {
    let mro = attr(class, "__mro__")?;
    for item in mro.try_iter().ok()? {
        let base = item.ok()?;
        if is_strategy_base(&base) {
            return Some(base);
        }
    }
    None
}

fn is_strategy_base(class: &Bound<'_, PyAny>) -> bool {
    let name = class
        .getattr("__name__")
        .ok()
        .and_then(|value| value.extract::<String>().ok());
    let module = class
        .getattr("__module__")
        .ok()
        .and_then(|value| value.extract::<String>().ok());
    name.as_deref() == Some("Strategy")
        && matches!(module.as_deref(), Some("mftik.strategy") | Some("mftik"))
}

fn attr<'py>(obj: &Bound<'py, PyAny>, name: &str) -> Option<Bound<'py, PyAny>> {
    match obj.hasattr(name) {
        Ok(true) => obj.getattr(name).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use pyo3::types::PyDictMethods;

    use super::*;

    use crate::{describe, write_traceback};

    fn raised(py: Python<'_>, code: &str) -> PyErr {
        let locals = pyo3::types::PyDict::new(py);
        py.run(
            std::ffi::CString::new(code).unwrap().as_c_str(),
            None,
            Some(&locals),
        )
        .expect_err("the snippet raises")
    }

    #[test]
    fn exception_text_is_the_type_and_str_and_system_exit_does_not_exit() {
        let _guard = crate::lock_python_tests();
        Python::initialize();
        Python::attach(|py| {
            let err = raised(py, "raise SystemExit('stop-via-sys')");
            assert_eq!(describe(py, &err), "SystemExit: stop-via-sys");
            write_traceback(py, &err);

            let err = raised(py, "raise KeyboardInterrupt('tick')");
            assert_eq!(describe(py, &err), "KeyboardInterrupt: tick");
            write_traceback(py, &err);

            let err = raised(py, "raise RuntimeError('init broke')");
            assert_eq!(describe(py, &err), "RuntimeError: init broke");
        });
    }

    fn registered_names(obj: Py<PyAny>) -> Vec<String> {
        let strategy = PyStrategy::new(obj);
        let mut handlers = Handlers::<PyStrategy>::default();
        strategy.handlers(&mut handlers);
        handlers
            .registered()
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    /// Subclass overrides, including ones inherited from another subclass, are
    /// registered. The base methods, an alias of a base method, and an instance
    /// assignment are not (E04-D031).
    #[test]
    fn only_subclass_overrides_are_registered() {
        let _guard = crate::lock_python_tests();
        Python::initialize();
        Python::attach(|py| {
            let script = r#"
import sys
import types

names = ("mftik", "mftik.strategy")
saved = {name: sys.modules.get(name) for name in names}
mod = types.ModuleType("mftik.strategy")
sys.modules["mftik"] = types.ModuleType("mftik")
sys.modules["mftik.strategy"] = mod
exec(
"""
class Strategy:
    def on_trade(self, ctx, ev):
        return None

    def on_kline(self, ctx, ev):
        return None

    def on_order(self, ctx, ev):
        return None

class Bare(Strategy):
    pass

class Sample(Strategy):
    def on_trade(self, ctx, ev):
        return None

class Child(Sample):
    def on_kline(self, ctx, ev):
        return None

class Mixin:
    def on_order(self, ctx, ev):
        return None

class Mixed(Mixin, Strategy):
    pass

class Alias(Strategy):
    on_trade = Strategy.on_trade

class Both(Strategy):
    on_trade = Strategy.on_trade

    def on_kline(self, ctx, ev):
        return None

assigned = Bare()

def on_trade(self, ctx, ev):
    return None

assigned.on_trade = on_trade
""",
    mod.__dict__,
)
for name in names:
    prior = saved[name]
    if prior is None:
        sys.modules.pop(name, None)
    else:
        sys.modules[name] = prior
"#;
            let locals = pyo3::types::PyDict::new(py);
            py.run(
                std::ffi::CString::new(script).unwrap().as_c_str(),
                Some(&locals),
                Some(&locals),
            )
            .expect("define strategy classes");
            let module = locals.get_item("mod").expect("mod").expect("mod is set");
            let bare = module.getattr("Bare").unwrap().call0().unwrap();
            let sample = module.getattr("Sample").unwrap().call0().unwrap();
            let child = module.getattr("Child").unwrap().call0().unwrap();
            let mixed = module.getattr("Mixed").unwrap().call0().unwrap();
            let alias = module.getattr("Alias").unwrap().call0().unwrap();
            let both = module.getattr("Both").unwrap().call0().unwrap();
            let assigned = module.getattr("assigned").unwrap();
            assert_eq!(registered_names(bare.unbind()), Vec::<String>::new());
            assert_eq!(
                registered_names(sample.unbind()),
                vec!["on_trade".to_owned()]
            );
            assert_eq!(
                registered_names(child.unbind()),
                vec!["on_trade".to_owned(), "on_kline".to_owned()]
            );
            assert_eq!(
                registered_names(mixed.unbind()),
                vec!["on_order".to_owned()]
            );
            assert_eq!(registered_names(alias.unbind()), Vec::<String>::new());
            assert_eq!(registered_names(both.unbind()), vec!["on_kline".to_owned()]);
            assert_eq!(registered_names(assigned.unbind()), Vec::<String>::new());
        });
    }
}
