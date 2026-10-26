//! The Python object as a [`mftik_rt::Strategy`] (E04-D003, E04-D004).
//!
//! Optional hooks are registered only when a subclass overrides them. That
//! decision is made once, the first time [`Strategy::handlers`] runs, which
//! is before OnInit. Converting each event and calling the Python method is
//! E04-03; until then a registered hook takes the GIL and returns.

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
use pyo3::types::{PyString, PyType};

use crate::context::{call_method, with_python_hook};

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

pub(crate) struct PyStrategy {
    obj: Py<PyAny>,
    mask: OnceLock<HookMask>,
}

impl PyStrategy {
    pub(crate) fn new(obj: Py<PyAny>) -> Self {
        Self {
            obj,
            mask: OnceLock::new(),
        }
    }

    fn mask(&self) -> HookMask {
        *self
            .mask
            .get_or_init(|| Python::attach(|py| read_mask(py, &self.obj)))
    }

    fn call(&mut self, method: &str, ctx: &Context<Self>) -> Result<(), HookError> {
        let message = with_python_hook(ctx, |py, token| call_method(py, &self.obj, method, token));
        message.map_err(Into::into)
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
        let mask = self.mask();
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

/// E04-03 replaces this with the event conversion and the Python call.
fn optional_hook_pending() {
    Python::attach(|_| {});
}

macro_rules! optional_hook {
    ($trait:ident, $method:ident, $event:ty) => {
        impl $trait for PyStrategy {
            fn $method(&mut self, _ctx: &Context<Self>, _ev: $event) {
                optional_hook_pending();
            }
        }
    };
}

optional_hook!(BestQuoteHandler, on_best_quote, BestQuote);
optional_hook!(TradeHandler, on_trade, Trade);
optional_hook!(AggTradeHandler, on_agg_trade, AggTrade);
optional_hook!(KlineHandler, on_kline, Kline);
optional_hook!(TickerHandler, on_ticker, Ticker);
optional_hook!(LiquidationHandler, on_liquidation, Liquidation);
optional_hook!(FundingRateHandler, on_funding_rate, FundingRate);
optional_hook!(OpenInterestHandler, on_open_interest, OpenInterest);
optional_hook!(GreeksHandler, on_greeks, Greeks);
optional_hook!(MarkPriceHandler, on_mark_price, MarkPrice);
optional_hook!(BookHandler, on_book, BookUpdate);
optional_hook!(OrderHandler, on_order, OrderEvent);
optional_hook!(FillHandler, on_fill, FillEvent);
optional_hook!(PositionHandler, on_position, PositionEvent);
optional_hook!(BalanceHandler, on_balance, BalanceEvent);
optional_hook!(ReconHandler, on_recon, ReconEvent);
optional_hook!(RejectHandler, on_reject, RejectEvent);

fn read_mask(py: Python<'_>, obj: &Py<PyAny>) -> HookMask {
    let mut mask = HookMask::default();
    let class = obj.bind(py).get_type();
    mask.on_best_quote = overrides(py, &class, "on_best_quote");
    mask.on_trade = overrides(py, &class, "on_trade");
    mask.on_agg_trade = overrides(py, &class, "on_agg_trade");
    mask.on_kline = overrides(py, &class, "on_kline");
    mask.on_ticker = overrides(py, &class, "on_ticker");
    mask.on_liquidation = overrides(py, &class, "on_liquidation");
    mask.on_funding_rate = overrides(py, &class, "on_funding_rate");
    mask.on_open_interest = overrides(py, &class, "on_open_interest");
    mask.on_greeks = overrides(py, &class, "on_greeks");
    mask.on_mark_price = overrides(py, &class, "on_mark_price");
    mask.on_book = overrides(py, &class, "on_book");
    mask.on_order = overrides(py, &class, "on_order");
    mask.on_fill = overrides(py, &class, "on_fill");
    mask.on_position = overrides(py, &class, "on_position");
    mask.on_balance = overrides(py, &class, "on_balance");
    mask.on_recon = overrides(py, &class, "on_recon");
    mask.on_reject = overrides(py, &class, "on_reject");
    mask
}

/// True when some class before `mftik.Strategy` defines `name` on itself.
fn overrides(py: Python<'_>, class: &Bound<'_, PyType>, name: &str) -> bool {
    let Ok(mro) = class.getattr("__mro__") else {
        return false;
    };
    let Ok(classes) = mro.try_iter() else {
        return false;
    };
    for item in classes {
        let Ok(base) = item else {
            return false;
        };
        if is_strategy_base(&base) {
            return false;
        }
        if defined_on(&base, py, name) {
            return true;
        }
    }
    false
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

fn defined_on(class: &Bound<'_, PyAny>, py: Python<'_>, name: &str) -> bool {
    let Ok(dict) = class.getattr("__dict__") else {
        return false;
    };
    let key = PyString::new(py, name);
    dict.call_method1("__contains__", (key,))
        .and_then(|value| value.extract::<bool>())
        .unwrap_or(false)
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
    /// registered. The base methods and an instance assignment are not.
    #[test]
    fn only_subclass_overrides_are_registered() {
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
            assert_eq!(registered_names(assigned.unbind()), Vec::<String>::new());
        });
    }
}
