//! `mftik.topic`: the market-data topic strings (E04-D053).
//!
//! The values are [`mftik_rt::topic`]. They are `str`, not an enum.
//! `kline(interval)` is `kline_{interval}`, including `kline("") == "kline_"`.

use mftik_rt::topic::{
    AGG_TRADE, BEST_QUOTE, FUNDING_RATE, GREEKS, LIQUIDATION, MARK_PRICE, OPEN_INTEREST,
    ORDER_BOOK_DELTA, TICKER, TRADE,
};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

/// The topic of klines of one interval, such as `1m` or `4h`.
#[pyfunction]
fn kline(interval: &str) -> String {
    mftik_rt::topic::kline(interval)
}

pub(crate) fn register(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = parent.py();
    let module = PyModule::new(py, "topic")?;
    module.setattr("__name__", "mftik._mftik.topic")?;
    module.setattr("__package__", "mftik._mftik")?;
    module.setattr("BEST_QUOTE", BEST_QUOTE)?;
    module.setattr("TRADE", TRADE)?;
    module.setattr("AGG_TRADE", AGG_TRADE)?;
    module.setattr("ORDER_BOOK_DELTA", ORDER_BOOK_DELTA)?;
    module.setattr("TICKER", TICKER)?;
    module.setattr("LIQUIDATION", LIQUIDATION)?;
    module.setattr("FUNDING_RATE", FUNDING_RATE)?;
    module.setattr("OPEN_INTEREST", OPEN_INTEREST)?;
    module.setattr("GREEKS", GREEKS)?;
    module.setattr("MARK_PRICE", MARK_PRICE)?;
    module.add_function(wrap_pyfunction!(kline, &module)?)?;
    parent.add_submodule(&module)?;
    parent.setattr("topic", &module)?;
    let sys = py.import("sys")?;
    sys.getattr("modules")?
        .set_item("mftik._mftik.topic", &module)?;
    Ok(())
}
