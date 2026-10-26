//! SDK pyclasses. Fields match the Rust structs, which match the Go SDK.
//!
//! `Order` numbers that are unset, including NaN, are `None`. `side`,
//! `order_type`, and `tif` are the generated proto enums. A value the prost
//! enum cannot store is kept on the Python object and rejected by [`PyOrder::to_new`].

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use mftik_core::mdpath::{Md, TapeQuery};
use mftik_core::symbol::{self, UniversalTicker};
use mftik_core::tdpath::{self, NewOrder};
use mftik_proto::wire::mftiko::common::v1::{OrderType, Side, TimeInForce};
use mftik_proto::wire::mftiko::md::v1 as md;
use mftik_proto::wire::mftiko::td::v1 as td;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyListMethods, PyTuple};

use crate::proto::{self, ToPyMessage, cursor_from_py, enum_value, repr_value};

const SDK_ENUMS: &str = r#"
import enum as _mftik_enum
def _mftik_reject_bool(cls):
    _orig = cls.__new__
    def _new(cls, value):
        if isinstance(value, bool):
            raise ValueError("bool is not an enum value")
        return _orig(cls, value)
    cls.__new__ = _new
    return cls
Origin = _mftik_reject_bool(_mftik_enum.IntEnum("Origin", {"from_td": 1, "local_reject": 2, "timeout": 3}))
RejectedRequest = _mftik_reject_bool(_mftik_enum.IntEnum(
    "RejectedRequest", {"cancel": 1, "batch_cancel": 2}
))
del _mftik_reject_bool
del _mftik_enum
"#;

fn enum_arg(obj: &Bound<'_, PyAny>) -> PyResult<i32> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("bool is not an enum value"));
    }
    obj.extract()
}

fn finite(value: Option<f64>) -> Option<f64> {
    value.filter(|number| !number.is_nan())
}

fn sdk_enum(py: Python<'_>, name: &str, value: i32) -> PyResult<Py<PyAny>> {
    enum_value(py, "mftik", name, value)
}

fn string_dict(py: Python<'_>, pairs: &BTreeMap<String, String>) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in pairs {
        dict.set_item(key, value)?;
    }
    Ok(dict.unbind())
}

fn float_dict(py: Python<'_>, pairs: &BTreeMap<String, f64>) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in pairs {
        dict.set_item(key, value)?;
    }
    Ok(dict.unbind())
}

fn message_tuple<T>(py: Python<'_>, items: &[T]) -> PyResult<Py<PyTuple>>
where
    T: ToPyMessage + Clone,
{
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(item.clone().to_py(py)?);
    }
    Ok(PyTuple::new(py, out)?.unbind())
}

fn proto_enum<T>(field: &'static str, value: i32) -> Result<T, OrderBuildError>
where
    T: TryFrom<i32>,
{
    T::try_from(value).map_err(|_| OrderBuildError { field, value })
}

/// `side`, `order_type`, or `tif` is not a known prost enumerator.
///
/// The Python object still holds the original int (E04-D022). `tdpath::NewOrder`
/// stores the prost enum, which has nowhere to put an unknown number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderBuildError {
    pub field: &'static str,
    pub value: i32,
}

impl fmt::Display for OrderBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unknown {} {}", self.field, self.value)
    }
}

impl std::error::Error for OrderBuildError {}

#[pyclass(frozen, eq, module = "mftik", name = "Md")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PyMd {
    #[pyo3(get)]
    uid: u32,
    #[pyo3(get)]
    md_ts_ns: i64,
    #[pyo3(get)]
    conn_epoch: u64,
    #[pyo3(get)]
    seq: u64,
}

#[pymethods]
impl PyMd {
    #[new]
    #[pyo3(signature = (*, uid = 0, md_ts_ns = 0, conn_epoch = 0, seq = 0))]
    fn new(uid: u32, md_ts_ns: i64, conn_epoch: u64, seq: u64) -> Self {
        Self {
            uid,
            md_ts_ns,
            conn_epoch,
            seq,
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Md(uid={}, md_ts_ns={}, conn_epoch={}, seq={})",
            repr_value(py, self.uid)?,
            repr_value(py, self.md_ts_ns)?,
            repr_value(py, self.conn_epoch)?,
            repr_value(py, self.seq)?,
        ))
    }
}

impl PyMd {
    fn from_md(md: Md) -> Self {
        Self {
            uid: md.uid,
            md_ts_ns: md.md_ts_ns,
            conn_epoch: md.conn_epoch,
            seq: md.seq,
        }
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "Td")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PyTd {
    #[pyo3(get)]
    api_id: i64,
    #[pyo3(get)]
    incarnation: u32,
    #[pyo3(get)]
    seq: u64,
}

#[pymethods]
impl PyTd {
    #[new]
    #[pyo3(signature = (*, api_id = 0, incarnation = 0, seq = 0))]
    fn new(api_id: i64, incarnation: u32, seq: u64) -> Self {
        Self {
            api_id,
            incarnation,
            seq,
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Td(api_id={}, incarnation={}, seq={})",
            repr_value(py, self.api_id)?,
            repr_value(py, self.incarnation)?,
            repr_value(py, self.seq)?,
        ))
    }
}

impl PyTd {
    fn from_td(td: tdpath::Td) -> Self {
        Self {
            api_id: td.api_id,
            incarnation: td.incarnation,
            seq: td.seq,
        }
    }
}

#[pyclass(frozen, module = "mftik", name = "Feed")]
pub struct PyFeed {
    md: Md,
    data: Py<PyAny>,
}

#[pymethods]
impl PyFeed {
    #[getter]
    fn md(&self, py: Python<'_>) -> PyResult<Py<PyMd>> {
        Py::new(py, PyMd::from_md(self.md))
    }

    #[getter]
    fn data(&self, py: Python<'_>) -> Py<PyAny> {
        self.data.clone_ref(py)
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<bool> {
        let Ok(other) = other.cast::<Self>() else {
            return Ok(false);
        };
        let other = other.borrow();
        Ok(self.md == other.md && self.data.bind(py).eq(other.data.bind(py))?)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Feed(md={}, data={})",
            repr_value(py, self.md(py)?)?,
            repr_value(py, self.data.bind(py))?
        ))
    }
}

/// One market-data event. `data` is the decoded payload pyclass.
pub fn feed_to_py(py: Python<'_>, md: Md, data: impl ToPyMessage) -> PyResult<Py<PyAny>> {
    let data = data.to_py(py)?;
    Ok(Py::new(py, PyFeed { md, data })?.into_any())
}

#[pyclass(frozen, eq, module = "mftik", name = "BookUpdate")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PyBookUpdate {
    md: Md,
    #[pyo3(get)]
    exch_ts_ns: i64,
}

#[pymethods]
impl PyBookUpdate {
    #[new]
    #[pyo3(signature = (*, md, exch_ts_ns = 0))]
    fn new(md: &PyMd, exch_ts_ns: i64) -> Self {
        Self {
            md: Md {
                uid: md.uid,
                md_ts_ns: md.md_ts_ns,
                conn_epoch: md.conn_epoch,
                seq: md.seq,
            },
            exch_ts_ns,
        }
    }

    #[getter]
    fn md(&self, py: Python<'_>) -> PyResult<Py<PyMd>> {
        Py::new(py, PyMd::from_md(self.md))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "BookUpdate(md={}, exch_ts_ns={})",
            repr_value(py, self.md(py)?)?,
            repr_value(py, self.exch_ts_ns)?
        ))
    }
}

impl PyBookUpdate {
    pub fn from_update(py: Python<'_>, md: Md, exch_ts_ns: i64) -> PyResult<Py<Self>> {
        Py::new(py, Self { md, exch_ts_ns })
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "OrderEvent")]
#[derive(Clone, PartialEq)]
pub struct PyOrderEvent {
    td: tdpath::Td,
    origin: i32,
    order: td::Order,
    cause: i32,
    #[pyo3(get)]
    code: i32,
    #[pyo3(get)]
    reason: String,
}

#[pymethods]
impl PyOrderEvent {
    #[getter]
    fn td(&self, py: Python<'_>) -> PyResult<Py<PyTd>> {
        Py::new(py, PyTd::from_td(self.td))
    }

    #[getter]
    fn origin(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        sdk_enum(py, "Origin", self.origin)
    }

    #[getter]
    fn order(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.order.clone().to_py(py)
    }

    #[getter]
    fn cause(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        enum_value(py, "mftik.td.v1", "OrderCause", self.cause)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "OrderEvent(td={}, origin={}, order={}, cause={}, code={}, reason={})",
            repr_value(py, self.td(py)?)?,
            repr_value(py, self.origin(py)?)?,
            repr_value(py, self.order(py)?)?,
            repr_value(py, self.cause(py)?)?,
            repr_value(py, self.code)?,
            repr_value(py, &self.reason)?,
        ))
    }
}

impl PyOrderEvent {
    pub fn from_event(py: Python<'_>, event: &tdpath::OrderEvent) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                td: event.td,
                origin: event.origin as i32,
                order: event.order.clone(),
                cause: event.cause,
                code: event.code,
                reason: event.reason.clone(),
            },
        )
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "FillEvent")]
#[derive(Clone, PartialEq)]
pub struct PyFillEvent {
    td: tdpath::Td,
    fill: td::Fill,
}

#[pymethods]
impl PyFillEvent {
    #[getter]
    fn td(&self, py: Python<'_>) -> PyResult<Py<PyTd>> {
        Py::new(py, PyTd::from_td(self.td))
    }

    #[getter]
    fn fill(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.fill.clone().to_py(py)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "FillEvent(td={}, fill={})",
            repr_value(py, self.td(py)?)?,
            repr_value(py, self.fill(py)?)?,
        ))
    }
}

impl PyFillEvent {
    pub fn from_event(py: Python<'_>, event: &tdpath::FillEvent) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                td: event.td,
                fill: event.fill.clone(),
            },
        )
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "PositionEvent")]
#[derive(Clone, PartialEq)]
pub struct PyPositionEvent {
    td: tdpath::Td,
    positions: Vec<td::Position>,
}

#[pymethods]
impl PyPositionEvent {
    #[getter]
    fn td(&self, py: Python<'_>) -> PyResult<Py<PyTd>> {
        Py::new(py, PyTd::from_td(self.td))
    }

    #[getter]
    fn positions(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        message_tuple(py, &self.positions)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "PositionEvent(td={}, positions={})",
            repr_value(py, self.td(py)?)?,
            repr_value(py, self.positions(py)?)?,
        ))
    }
}

impl PyPositionEvent {
    pub fn from_event(py: Python<'_>, event: &tdpath::PositionEvent) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                td: event.td,
                positions: event.positions.clone(),
            },
        )
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "BalanceEvent")]
#[derive(Clone, PartialEq)]
pub struct PyBalanceEvent {
    td: tdpath::Td,
    balances: Vec<td::Balance>,
}

#[pymethods]
impl PyBalanceEvent {
    #[getter]
    fn td(&self, py: Python<'_>) -> PyResult<Py<PyTd>> {
        Py::new(py, PyTd::from_td(self.td))
    }

    #[getter]
    fn balances(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        message_tuple(py, &self.balances)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "BalanceEvent(td={}, balances={})",
            repr_value(py, self.td(py)?)?,
            repr_value(py, self.balances(py)?)?,
        ))
    }
}

impl PyBalanceEvent {
    pub fn from_event(py: Python<'_>, event: &tdpath::BalanceEvent) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                td: event.td,
                balances: event.balances.clone(),
            },
        )
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "ReconEvent")]
#[derive(Clone, PartialEq)]
pub struct PyReconEvent {
    td: tdpath::Td,
    recon: td::Recon,
}

#[pymethods]
impl PyReconEvent {
    #[getter]
    fn td(&self, py: Python<'_>) -> PyResult<Py<PyTd>> {
        Py::new(py, PyTd::from_td(self.td))
    }

    #[getter]
    fn recon(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.recon.clone().to_py(py)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "ReconEvent(td={}, recon={})",
            repr_value(py, self.td(py)?)?,
            repr_value(py, self.recon(py)?)?,
        ))
    }
}

impl PyReconEvent {
    pub fn from_event(py: Python<'_>, event: &tdpath::ReconEvent) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                td: event.td,
                recon: event.recon.clone(),
            },
        )
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "RejectEvent")]
#[derive(Clone, PartialEq, Eq)]
pub struct PyRejectEvent {
    td: tdpath::Td,
    request: i32,
    client_order_ids: Vec<String>,
    #[pyo3(get)]
    code: i32,
    #[pyo3(get)]
    reason: String,
}

#[pymethods]
impl PyRejectEvent {
    #[getter]
    fn td(&self, py: Python<'_>) -> PyResult<Py<PyTd>> {
        Py::new(py, PyTd::from_td(self.td))
    }

    #[getter]
    fn request(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        sdk_enum(py, "RejectedRequest", self.request)
    }

    #[getter]
    fn client_order_ids(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        Ok(PyTuple::new(py, &self.client_order_ids)?.unbind())
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "RejectEvent(td={}, request={}, client_order_ids={}, code={}, reason={})",
            repr_value(py, self.td(py)?)?,
            repr_value(py, self.request(py)?)?,
            repr_value(py, self.client_order_ids(py)?)?,
            repr_value(py, self.code)?,
            repr_value(py, &self.reason)?,
        ))
    }
}

impl PyRejectEvent {
    pub fn from_event(py: Python<'_>, event: &tdpath::RejectEvent) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                td: event.td,
                request: event.request as i32,
                client_order_ids: event.client_order_ids.clone(),
                code: event.code,
                reason: event.reason.clone(),
            },
        )
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "UniversalTicker")]
#[derive(Clone, PartialEq, Eq)]
pub struct PyUniversalTicker {
    #[pyo3(get)]
    venue: String,
    #[pyo3(get)]
    category: String,
    #[pyo3(get)]
    symbol: String,
}

#[pymethods]
impl PyUniversalTicker {
    #[new]
    #[pyo3(signature = (*, venue, category, symbol))]
    fn new(venue: String, category: String, symbol: String) -> PyResult<Self> {
        let text = format!("{venue}_{category}_{symbol}");
        UniversalTicker::parse(&text).map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self {
            venue,
            category,
            symbol,
        })
    }

    fn __str__(&self) -> String {
        format!("{}_{}_{}", self.venue, self.category, self.symbol)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "UniversalTicker(venue={}, category={}, symbol={})",
            repr_value(py, &self.venue)?,
            repr_value(py, &self.category)?,
            repr_value(py, &self.symbol)?,
        ))
    }
}

impl PyUniversalTicker {
    fn from_ticker(ticker: &UniversalTicker) -> Self {
        Self {
            venue: ticker.venue.clone(),
            category: ticker.category.as_str().to_owned(),
            symbol: ticker.symbol.clone(),
        }
    }

    fn canonical(&self) -> String {
        format!("{}_{}_{}", self.venue, self.category, self.symbol)
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "Info")]
#[derive(Clone, PartialEq)]
pub struct PyInfo {
    #[pyo3(get)]
    uid: u32,
    ticker: PyUniversalTicker,
    #[pyo3(get)]
    exch_ticker: String,
    #[pyo3(get)]
    base: String,
    #[pyo3(get)]
    quote: String,
    #[pyo3(get)]
    contract_size: f64,
    #[pyo3(get)]
    contract_value_ccy: String,
    #[pyo3(get)]
    price_ccy: String,
    #[pyo3(get)]
    settle: String,
    #[pyo3(get)]
    expiry_ns: i64,
    #[pyo3(get)]
    strike: f64,
    #[pyo3(get)]
    option_type: String,
    #[pyo3(get)]
    status: String,
    filters: BTreeMap<String, f64>,
    #[pyo3(get)]
    updated_at_ns: i64,
}

#[pymethods]
impl PyInfo {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        ticker,
        uid = 0,
        exch_ticker = String::new(),
        base = String::new(),
        quote = String::new(),
        contract_size = 0.0,
        contract_value_ccy = String::new(),
        price_ccy = String::new(),
        settle = String::new(),
        expiry_ns = 0,
        strike = 0.0,
        option_type = String::new(),
        status = String::new(),
        filters = None,
        updated_at_ns = 0,
    ))]
    fn new(
        ticker: &PyUniversalTicker,
        uid: u32,
        exch_ticker: String,
        base: String,
        quote: String,
        contract_size: f64,
        contract_value_ccy: String,
        price_ccy: String,
        settle: String,
        expiry_ns: i64,
        strike: f64,
        option_type: String,
        status: String,
        filters: Option<HashMap<String, f64>>,
        updated_at_ns: i64,
    ) -> Self {
        Self {
            uid,
            ticker: ticker.clone(),
            exch_ticker,
            base,
            quote,
            contract_size,
            contract_value_ccy,
            price_ccy,
            settle,
            expiry_ns,
            strike,
            option_type,
            status,
            filters: filters.unwrap_or_default().into_iter().collect(),
            updated_at_ns,
        }
    }

    #[getter]
    fn ticker(&self, py: Python<'_>) -> PyResult<Py<PyUniversalTicker>> {
        Py::new(py, self.ticker.clone())
    }

    #[getter]
    fn filters(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        float_dict(py, &self.filters)
    }

    fn is_tradable(&self) -> bool {
        self.status == "TRADABLE"
    }

    fn price_tick(&self) -> f64 {
        self.filters.get(symbol::PRICE_TICK).copied().unwrap_or(0.0)
    }

    fn linear(&self) -> PyResult<bool> {
        Ok(self.as_symbol()?.linear())
    }

    fn inverse(&self) -> PyResult<bool> {
        Ok(self.as_symbol()?.inverse())
    }

    fn quanto(&self) -> PyResult<bool> {
        Ok(self.as_symbol()?.quanto())
    }

    fn coin_margined(&self) -> PyResult<bool> {
        Ok(self.as_symbol()?.coin_margined())
    }

    fn u_margined(&self) -> PyResult<bool> {
        Ok(self.as_symbol()?.u_margined())
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Info(uid={}, ticker={}, exch_ticker={}, base={}, quote={}, contract_size={}, contract_value_ccy={}, price_ccy={}, settle={}, expiry_ns={}, strike={}, option_type={}, status={}, filters={}, updated_at_ns={})",
            repr_value(py, self.uid)?,
            repr_value(py, self.ticker(py)?)?,
            repr_value(py, &self.exch_ticker)?,
            repr_value(py, &self.base)?,
            repr_value(py, &self.quote)?,
            repr_value(py, self.contract_size)?,
            repr_value(py, &self.contract_value_ccy)?,
            repr_value(py, &self.price_ccy)?,
            repr_value(py, &self.settle)?,
            repr_value(py, self.expiry_ns)?,
            repr_value(py, self.strike)?,
            repr_value(py, &self.option_type)?,
            repr_value(py, &self.status)?,
            repr_value(py, self.filters(py)?)?,
            repr_value(py, self.updated_at_ns)?,
        ))
    }
}

impl PyInfo {
    pub fn from_info(py: Python<'_>, info: &symbol::Info) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                uid: info.uid,
                ticker: PyUniversalTicker::from_ticker(&info.ticker),
                exch_ticker: info.exch_ticker.clone(),
                base: info.base.clone(),
                quote: info.quote.clone(),
                contract_size: info.contract_size,
                contract_value_ccy: info.contract_value_ccy.clone(),
                price_ccy: info.price_ccy.clone(),
                settle: info.settle.clone(),
                expiry_ns: info.expiry_ns,
                strike: info.strike,
                option_type: info.option_type.clone(),
                status: info.status.as_str().to_owned(),
                filters: info.filters.clone(),
                updated_at_ns: info.updated_at_ns,
            },
        )
    }

    fn as_symbol(&self) -> PyResult<symbol::Info> {
        let ticker = UniversalTicker::parse(&self.ticker.canonical())
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(symbol::Info {
            uid: self.uid,
            ticker,
            exch_ticker: self.exch_ticker.clone(),
            base: self.base.clone(),
            quote: self.quote.clone(),
            contract_size: self.contract_size,
            contract_value_ccy: self.contract_value_ccy.clone(),
            price_ccy: self.price_ccy.clone(),
            settle: self.settle.clone(),
            expiry_ns: self.expiry_ns,
            strike: self.strike,
            option_type: self.option_type.clone(),
            status: symbol::Status::Unspecified,
            filters: self.filters.clone(),
            updated_at_ns: self.updated_at_ns,
        })
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "Order")]
#[derive(Clone, PartialEq)]
pub struct PyOrder {
    #[pyo3(get)]
    api_id: i64,
    #[pyo3(get)]
    algo_id: u32,
    #[pyo3(get)]
    uid: u32,
    side: i32,
    order_type: i32,
    tif: i32,
    #[pyo3(get)]
    qty: Option<f64>,
    #[pyo3(get)]
    quote_qty: Option<f64>,
    #[pyo3(get)]
    price: Option<f64>,
    #[pyo3(get)]
    recv_window_ms: i64,
    #[pyo3(get)]
    reduce_only: bool,
    params: BTreeMap<String, String>,
}

#[pymethods]
impl PyOrder {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        api_id = 0,
        algo_id = 0,
        uid = 0,
        side = 0,
        order_type = 0,
        tif = 0,
        qty = None,
        quote_qty = None,
        price = None,
        recv_window_ms = 0,
        reduce_only = false,
        params = None,
    ))]
    fn new(
        api_id: i64,
        algo_id: u32,
        uid: u32,
        #[pyo3(from_py_with = enum_arg)] side: i32,
        #[pyo3(from_py_with = enum_arg)] order_type: i32,
        #[pyo3(from_py_with = enum_arg)] tif: i32,
        qty: Option<f64>,
        quote_qty: Option<f64>,
        price: Option<f64>,
        recv_window_ms: i64,
        reduce_only: bool,
        params: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            api_id,
            algo_id,
            uid,
            side,
            order_type,
            tif,
            qty: finite(qty),
            quote_qty: finite(quote_qty),
            price: finite(price),
            recv_window_ms,
            reduce_only,
            params: params.unwrap_or_default().into_iter().collect(),
        }
    }

    #[getter]
    fn side(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        enum_value(py, "mftik.common.v1", "Side", self.side)
    }

    #[getter]
    fn order_type(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        enum_value(py, "mftik.common.v1", "OrderType", self.order_type)
    }

    #[getter]
    fn tif(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        enum_value(py, "mftik.common.v1", "TimeInForce", self.tif)
    }

    #[getter]
    fn params(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        string_dict(py, &self.params)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Order(api_id={}, algo_id={}, uid={}, side={}, order_type={}, tif={}, qty={}, quote_qty={}, price={}, recv_window_ms={}, reduce_only={}, params={})",
            repr_value(py, self.api_id)?,
            repr_value(py, self.algo_id)?,
            repr_value(py, self.uid)?,
            repr_value(py, self.side(py)?)?,
            repr_value(py, self.order_type(py)?)?,
            repr_value(py, self.tif(py)?)?,
            repr_value(py, self.qty)?,
            repr_value(py, self.quote_qty)?,
            repr_value(py, self.price)?,
            repr_value(py, self.recv_window_ms)?,
            repr_value(py, self.reduce_only)?,
            repr_value(py, self.params(py)?)?,
        ))
    }
}

impl PyOrder {
    pub fn from_new(py: Python<'_>, order: &NewOrder) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                api_id: order.api_id,
                algo_id: order.algo_id,
                uid: order.uid,
                side: order.side as i32,
                order_type: order.order_type as i32,
                tif: order.tif as i32,
                qty: finite(order.qty),
                quote_qty: finite(order.quote_qty),
                price: finite(order.price),
                recv_window_ms: order.recv_window_ms,
                reduce_only: order.reduce_only,
                params: order
                    .params
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            },
        )
    }

    pub fn to_new(&self) -> Result<NewOrder, OrderBuildError> {
        Ok(NewOrder {
            api_id: self.api_id,
            algo_id: self.algo_id,
            uid: self.uid,
            side: proto_enum::<Side>("side", self.side)?,
            order_type: proto_enum::<OrderType>("order_type", self.order_type)?,
            tif: proto_enum::<TimeInForce>("tif", self.tif)?,
            qty: self.qty,
            quote_qty: self.quote_qty,
            price: self.price,
            recv_window_ms: self.recv_window_ms,
            reduce_only: self.reduce_only,
            params: self
                .params
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })
    }
}

#[pyclass(frozen, eq, module = "mftik", name = "TapeQuery")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PyTapeQuery {
    #[pyo3(get)]
    limit: u32,
    before: Option<md::Cursor>,
    #[pyo3(get)]
    since_ns: i64,
}

#[pymethods]
impl PyTapeQuery {
    #[new]
    #[pyo3(signature = (*, limit = 0, before = None, since_ns = 0))]
    fn new(py: Python<'_>, limit: u32, before: Option<Py<PyAny>>, since_ns: i64) -> PyResult<Self> {
        let before = match before {
            Some(value) if value.is_none(py) => None,
            Some(value) => Some(cursor_from_py(value.bind(py))?),
            None => None,
        };
        Ok(Self {
            limit,
            before,
            since_ns,
        })
    }

    #[getter]
    fn before(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.before {
            Some(cursor) => cursor.to_py(py),
            None => Ok(py.None()),
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "TapeQuery(limit={}, before={}, since_ns={})",
            repr_value(py, self.limit)?,
            repr_value(py, self.before(py)?)?,
            repr_value(py, self.since_ns)?,
        ))
    }
}

impl PyTapeQuery {
    pub fn from_query(py: Python<'_>, query: TapeQuery) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                limit: query.limit,
                before: query.before,
                since_ns: query.since_ns,
            },
        )
    }

    pub fn to_query(self) -> TapeQuery {
        TapeQuery {
            limit: self.limit,
            before: self.before,
            since_ns: self.since_ns,
        }
    }
}

pub fn register(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    proto::run_enums(py, module, SDK_ENUMS)?;
    module.getattr("Origin")?.setattr("__module__", "mftik")?;
    module
        .getattr("RejectedRequest")?
        .setattr("__module__", "mftik")?;
    module.add_class::<PyMd>()?;
    module.add_class::<PyTd>()?;
    module.add_class::<PyFeed>()?;
    module.add_class::<PyBookUpdate>()?;
    module.add_class::<PyOrderEvent>()?;
    module.add_class::<PyFillEvent>()?;
    module.add_class::<PyPositionEvent>()?;
    module.add_class::<PyBalanceEvent>()?;
    module.add_class::<PyReconEvent>()?;
    module.add_class::<PyRejectEvent>()?;
    module.add_class::<PyUniversalTicker>()?;
    module.add_class::<PyInfo>()?;
    module.add_class::<PyOrder>()?;
    module.add_class::<PyTapeQuery>()?;
    let index = module.index()?;
    index.append("Origin")?;
    index.append("RejectedRequest")?;
    let modules = py.import("sys")?.getattr("modules")?;
    if let Ok(package) = modules.get_item("mftik")
        && !package.is(module)
    {
        for name in ["Origin", "RejectedRequest"] {
            package.setattr(name, module.getattr(name)?)?;
        }
    }
    Ok(())
}
