//! Python `Context`, `Poster`, and `BookView` (E04-D005, E04-D011, E04-D051).
//!
//! A `Context` handed to a hook stores only a token. The token matches the
//! hook's thread-local scope, so using it after the hook returns, or from any
//! other thread, raises `mftik.ContextError` instead of touching the runtime.
//! [`Poster`] is the one object that may cross threads. A [`PyBookView`] is
//! the same kind of token: each method borrows the book again, and the levels
//! it returns are owned copies (E04-D051, E04-D052).

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use mftik_core::book::OrderBook;
use mftik_core::mdpath::ReadError as MdReadError;
use mftik_rt::{Context, Poster};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use crate::proto::ToPyMessage;
use crate::sdk::{PyInfo, PyOrder, PyTapeQuery};
use crate::strategy::PyStrategy;
use crate::{
    context_error, describe, invalid_book, invalid_context, read_error, require_callable,
    write_traceback,
};

static HOOK_DEPTH: AtomicUsize = AtomicUsize::new(0);
static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static ACTIVE: Cell<*const Active> = const { Cell::new(std::ptr::null()) };
}

struct Active {
    token: u64,
    ctx: *const Context<PyStrategy>,
}

/// The hook currently running on this thread. Depth is what the exit callback
/// reads from other threads (E04-D002).
pub(crate) struct HookScope {
    token: u64,
    current: *const Active,
    previous: *const Active,
}

impl HookScope {
    pub(crate) fn enter(ctx: &Context<PyStrategy>) -> Self {
        let token = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
        let current = Box::into_raw(Box::new(Active { token, ctx }));
        let previous = ACTIVE.with(|slot| slot.replace(current));
        HOOK_DEPTH.fetch_add(1, Ordering::SeqCst);
        Self {
            token,
            current,
            previous,
        }
    }
}

impl Drop for HookScope {
    fn drop(&mut self) {
        let installed = ACTIVE.with(|slot| slot.replace(self.previous));
        if installed == self.current && !self.current.is_null() {
            // SAFETY: `enter` allocated this box with `Box::into_raw` and this
            // is the matching free. The `Context` it points at is not freed
            // here; that reference belongs to the hook's caller and outlives
            // this scope.
            unsafe { drop(Box::from_raw(self.current.cast_mut())) };
        }
        HOOK_DEPTH.fetch_sub(1, Ordering::SeqCst);
        self.current = std::ptr::null();
    }
}

pub(crate) fn hook_depth() -> usize {
    HOOK_DEPTH.load(Ordering::SeqCst)
}

pub(crate) fn with_python_hook<T>(
    ctx: &Context<PyStrategy>,
    body: impl FnOnce(Python<'_>, u64) -> T,
) -> T {
    let scope = HookScope::enter(ctx);
    let token = scope.token;
    Python::attach(|py| body(py, token))
}

/// The live `Context` for `token` on this thread, or `None` when the token
/// does not belong to the hook running here.
fn context_ptr(token: u64) -> Option<*const Context<PyStrategy>> {
    ACTIVE.with(|slot| {
        let ptr = slot.get();
        if ptr.is_null() {
            return None;
        }
        // SAFETY: a non-null slot on this thread was stored by `HookScope::enter`
        // and points at that scope's `Active` until the scope drops. The token
        // is compared before the `Context` pointer is returned. Other threads
        // never see this slot.
        let active = unsafe { &*ptr };
        if active.token != token {
            return None;
        }
        Some(active.ctx)
    })
}

fn with_context<T>(token: u64, body: impl FnOnce(&Context<PyStrategy>) -> T) -> PyResult<T> {
    let Some(ptr) = context_ptr(token) else {
        return Err(invalid_context());
    };
    // SAFETY: `context_ptr` just matched `token` against this thread's scope,
    // which outlives the call.
    let ctx = unsafe { &*ptr };
    Ok(body(ctx))
}

/// Re-borrows the synced book. A dead token, another thread, or a book that
/// is no longer synced is [`invalid_book`] (E04-D005).
fn with_book<T>(token: u64, uid: u32, body: impl FnOnce(&OrderBook) -> T) -> PyResult<T> {
    let Some(ptr) = context_ptr(token) else {
        return Err(invalid_book());
    };
    // SAFETY: same as [`with_context`]: the pointer is this thread's live hook.
    let ctx = unsafe { &*ptr };
    let Some(view) = ctx.book(uid) else {
        return Err(invalid_book());
    };
    Ok(body(&view))
}

/// `(tick, qty)` from the best outward. A non-positive limit is empty
/// (E04-D051, E04-D052).
fn level_pairs(levels: impl IntoIterator<Item = (i64, f64)>) -> Vec<(i64, f64)> {
    levels.into_iter().collect()
}

fn mapped<T>(
    token: u64,
    body: impl FnOnce(&Context<PyStrategy>) -> Result<T, mftik_rt::Error>,
) -> PyResult<T> {
    with_context(token, body)?.map_err(|err| context_error(&err))
}

/// This thread's hook [`Context`], carried across [`Python::detach`].
///
/// `detach` requires [`Send`]. A raw pointer is not `Send`, and turning it
/// into a `usize` drops provenance. The closure runs on the calling thread.
/// The pointer lives in this wrapper so the closure captures the wrapper,
/// not the `Copy` field.
struct HookPtr(*const Context<PyStrategy>);

// SAFETY: `Python::detach` calls the closure on this thread. The pointer is
// dereferenced only there, while the `HookScope` that published it is still
// alive. It is never read from another thread.
unsafe impl Send for HookPtr {}

impl HookPtr {
    fn resolve(self, ticker: &str) -> Result<mftik_core::symbol::Info, mftik_rt::Error> {
        // SAFETY: see `Send`. `self` is moved into the `detach` closure,
        // which runs on the thread that created this pointer.
        let ctx = unsafe { &*self.0 };
        ctx.resolve(ticker)
    }
}

fn maybe_message<T>(py: Python<'_>, value: Option<T>) -> PyResult<Py<PyAny>>
where
    T: ToPyMessage,
{
    match value {
        Some(value) => value.to_py(py),
        None => Ok(py.None()),
    }
}

fn bad_delay() -> PyErr {
    PyValueError::new_err("delay must be a finite number of seconds >= 0")
}

pub(crate) fn delay_duration(seconds: f64) -> PyResult<Duration> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(bad_delay());
    }
    Duration::try_from_secs_f64(seconds).map_err(|_| bad_delay())
}

fn py_context(py: Python<'_>, token: u64) -> PyResult<Py<PyContext>> {
    Py::new(py, PyContext { token })
}

/// Runs `callback(ctx)` as a hook. A Python exception becomes a panic whose
/// message is `{type}: {str(e)}`, which the dispatcher reports as
/// `hook panic: ...` (E04-D004).
pub(crate) fn invoke_callback(callback: &Py<PyAny>, ctx: &Context<PyStrategy>) {
    let result = with_python_hook(ctx, |py, token| call_callable(py, callback, token));
    if let Err(message) = result {
        panic!("{message}");
    }
}

/// Runs `callback(ctx, value, err)` as a hook. Success is `(value, None)`.
/// Failure is `(None, ReadError)` and the text is the Rust `Display`.
/// A Python exception panics like [`invoke_callback`] (E04-D004). Arity is
/// not checked ahead of the call (E04-D054).
fn invoke_read<T: ToPyMessage>(
    callback: &Py<PyAny>,
    ctx: &Context<PyStrategy>,
    result: Result<T, MdReadError>,
) {
    let outcome = with_python_hook(ctx, |py, token| deliver_read(py, token, callback, result));
    if let Err(message) = outcome {
        panic!("{message}");
    }
}

fn deliver_read<T: ToPyMessage>(
    py: Python<'_>,
    token: u64,
    callback: &Py<PyAny>,
    result: Result<T, MdReadError>,
) -> Result<(), String> {
    let py_ctx = match py_context(py, token) {
        Ok(ctx) => ctx,
        Err(err) => return python_failure(py, err),
    };
    let (value, err_obj) = match result {
        Ok(msg) => match msg.to_py(py) {
            Ok(value) => (value, py.None()),
            Err(err) => return python_failure(py, err),
        },
        Err(err) => (
            py.None(),
            read_error(&err).value(py).clone().unbind().into_any(),
        ),
    };
    match callback.call1(py, (py_ctx, value, err_obj)) {
        Ok(_) => Ok(()),
        Err(err) => python_failure(py, err),
    }
}

fn python_failure(py: Python<'_>, err: PyErr) -> Result<(), String> {
    let message = describe(py, &err);
    write_traceback(py, &err);
    Err(message)
}

fn schedule<T>(
    token: u64,
    call: impl FnOnce(&Context<PyStrategy>) -> Result<T, mftik_rt::Error>,
) -> PyResult<T> {
    with_context(token, call)?.map_err(|err| crate::context_error(&err))
}

fn call_callable(py: Python<'_>, callback: &Py<PyAny>, token: u64) -> Result<(), String> {
    let py_ctx = match py_context(py, token) {
        Ok(ctx) => ctx,
        Err(err) => {
            let message = describe(py, &err);
            write_traceback(py, &err);
            return Err(message);
        }
    };
    match callback.call1(py, (py_ctx,)) {
        Ok(_) => Ok(()),
        Err(err) => {
            let message = describe(py, &err);
            write_traceback(py, &err);
            Err(message)
        }
    }
}

/// Calls `obj.method(ctx)`. The `Err` string is `{type}: {str(e)}` and the
/// traceback has been written to stderr (E04-D004).
pub(crate) fn call_method(
    py: Python<'_>,
    obj: &Py<PyAny>,
    method: &str,
    token: u64,
) -> Result<(), String> {
    let py_ctx = match py_context(py, token) {
        Ok(ctx) => ctx,
        Err(err) => {
            let message = describe(py, &err);
            write_traceback(py, &err);
            return Err(message);
        }
    };
    match obj.bind(py).call_method1(method, (py_ctx,)) {
        Ok(_) => Ok(()),
        Err(err) => {
            let message = describe(py, &err);
            write_traceback(py, &err);
            Err(message)
        }
    }
}

#[pyclass(frozen, module = "mftik._mftik", name = "Context")]
pub(crate) struct PyContext {
    token: u64,
}

#[pymethods]
impl PyContext {
    fn session_id(&self) -> PyResult<String> {
        with_context(self.token, |ctx| ctx.session_id().to_owned())
    }

    fn params<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        with_context(self.token, |ctx| PyBytes::new(py, ctx.params()))
    }

    fn exit(&self) -> PyResult<()> {
        with_context(self.token, |ctx| ctx.exit())
    }

    fn fail(&self, reason: String) -> PyResult<()> {
        with_context(self.token, |ctx| ctx.fail(reason))
    }

    fn volume_dir(&self) -> PyResult<String> {
        with_context(self.token, |ctx| ctx.volume_dir().to_owned())
    }

    fn input_dir(&self, name: &str) -> PyResult<String> {
        with_context(self.token, |ctx| ctx.input_dir(name))
    }

    /// SYM lookup. Only OnInit and OnStart succeed. The wait releases the GIL
    /// (E04-D002) and uses the runtime's own deadline (E04-D043).
    fn resolve(&self, py: Python<'_>, ticker: String) -> PyResult<Py<PyInfo>> {
        let Some(ptr) = context_ptr(self.token) else {
            return Err(invalid_context());
        };
        let hook = HookPtr(ptr);
        // The SYM wait blocks inside `detach`, so other Python threads run.
        let result = py.detach(move || hook.resolve(&ticker));
        match result {
            Ok(info) => PyInfo::from_info(py, &info),
            Err(err) => Err(context_error(&err)),
        }
    }

    fn info(&self, py: Python<'_>, uid: u32) -> PyResult<Py<PyInfo>> {
        let info = mapped(self.token, |ctx| ctx.info(uid))?;
        PyInfo::from_info(py, &info)
    }

    /// Places `order`. Anything that is not an `mftik.Order` is a `TypeError`
    /// (E04-D041); there is no field-by-field shortcut.
    fn submit(&self, order: &Bound<'_, PyAny>) -> PyResult<String> {
        let py_order = order
            .cast::<PyOrder>()
            .map_err(|_| PyTypeError::new_err("submit() argument must be mftik.Order"))?;
        let built = py_order
            .borrow()
            .to_new()
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        mapped(self.token, |ctx| ctx.submit(built))
    }

    fn cancel(&self, api_id: i64, client_order_id: &str) -> PyResult<()> {
        mapped(self.token, |ctx| ctx.cancel(api_id, client_order_id))
    }

    /// Go's variadic `BatchCancel`: each extra argument is one uid.
    #[pyo3(signature = (api_id, algo_id, *uids))]
    fn batch_cancel(&self, api_id: i64, algo_id: u32, uids: Vec<u32>) -> PyResult<()> {
        mapped(self.token, |ctx| ctx.batch_cancel(api_id, algo_id, &uids))
    }

    fn request_recon(&self, api_id: i64) -> PyResult<()> {
        mapped(self.token, |ctx| ctx.request_recon(api_id))
    }

    fn order(&self, py: Python<'_>, api_id: i64, client_order_id: &str) -> PyResult<Py<PyAny>> {
        let found = with_context(self.token, |ctx| ctx.order(api_id, client_order_id))?;
        maybe_message(py, found)
    }

    /// The runtime's order, with no extra sort (E04-D042). No open orders is
    /// an empty tuple.
    fn open_orders<'py>(
        &self,
        py: Python<'py>,
        api_id: i64,
        algo_id: u32,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let orders = with_context(self.token, |ctx| ctx.open_orders(api_id, algo_id))?;
        let mut items = Vec::with_capacity(orders.len());
        for order in orders {
            items.push(order.to_py(py)?);
        }
        PyTuple::new(py, items)
    }

    fn position(&self, py: Python<'_>, api_id: i64, uid: u32) -> PyResult<Py<PyAny>> {
        let found = with_context(self.token, |ctx| ctx.position(api_id, uid))?;
        maybe_message(py, found)
    }

    fn balance(&self, py: Python<'_>, api_id: i64, asset: &str) -> PyResult<Py<PyAny>> {
        let found = with_context(self.token, |ctx| ctx.balance(api_id, asset))?;
        maybe_message(py, found)
    }

    fn after(&self, py: Python<'_>, delay: f64, callback: Py<PyAny>) -> PyResult<()> {
        let delay = delay_duration(delay)?;
        require_callable(py, &callback)?;
        with_context(self.token, |ctx| {
            ctx.after(delay, move |_strategy, ctx| invoke_callback(&callback, ctx));
        })
    }

    fn post(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<()> {
        require_callable(py, &callback)?;
        with_context(self.token, |ctx| {
            ctx.post(move |_strategy, ctx| invoke_callback(&callback, ctx));
        })
    }

    fn poster(&self) -> PyResult<PyPoster> {
        with_context(self.token, |ctx| PyPoster {
            inner: ctx.poster(),
        })
    }

    /// The synced local book, or `None` while `md.view` has not caught up.
    fn book(&self, py: Python<'_>, uid: u32) -> PyResult<Option<Py<PyBookView>>> {
        let present = with_context(self.token, |ctx| ctx.book(uid).is_some())?;
        if !present {
            return Ok(None);
        }
        Ok(Some(Py::new(
            py,
            PyBookView {
                token: self.token,
                uid,
            },
        )?))
    }

    /// Page `md.view`. `callback(ctx, page, err)` runs on the dispatcher.
    /// A value that is not callable is `TypeError` before the read (E04-D054).
    fn tape(
        &self,
        py: Python<'_>,
        uid: u32,
        topic: String,
        query: Py<PyTapeQuery>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        require_callable(py, &callback)?;
        let query = (*query.borrow(py)).to_query();
        schedule(self.token, |ctx| {
            ctx.tape(uid, &topic, query, move |_strategy, ctx, result| {
                invoke_read(&callback, ctx, result);
            })
        })
    }

    /// Ask `md.view` for the windowed book. The callback shape matches [`Self::tape`].
    fn book_snapshot(&self, py: Python<'_>, uid: u32, callback: Py<PyAny>) -> PyResult<()> {
        require_callable(py, &callback)?;
        schedule(self.token, |ctx| {
            ctx.book_snapshot(uid, move |_strategy, ctx, result| {
                invoke_read(&callback, ctx, result);
            })
        })
    }

    /// Ask `md.fetch` for the latest `limit` closed klines.
    fn fetch_klines(
        &self,
        py: Python<'_>,
        uid: u32,
        interval: String,
        limit: u32,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        require_callable(py, &callback)?;
        schedule(self.token, |ctx| {
            ctx.fetch_klines(uid, &interval, limit, move |_strategy, ctx, result| {
                invoke_read(&callback, ctx, result);
            })
        })
    }

    /// Ask `md.fetch` for the open interest the exchange does not push.
    fn fetch_open_interest(&self, py: Python<'_>, uid: u32, callback: Py<PyAny>) -> PyResult<()> {
        require_callable(py, &callback)?;
        schedule(self.token, |ctx| {
            ctx.fetch_open_interest(uid, move |_strategy, ctx, result| {
                invoke_read(&callback, ctx, result);
            })
        })
    }
}

/// The local book inside the hook that received it (E04-D005, E04-D051).
///
/// Methods copy what they return. The object itself dies with the hook.
#[pyclass(frozen, module = "mftik", name = "BookView")]
pub(crate) struct PyBookView {
    token: u64,
    uid: u32,
}

#[pymethods]
impl PyBookView {
    fn tick_size(&self) -> PyResult<f64> {
        with_book(self.token, self.uid, |book| book.tick_size())
    }

    fn window(&self) -> PyResult<usize> {
        with_book(self.token, self.uid, |book| book.window())
    }

    fn price(&self, tick: i64) -> PyResult<f64> {
        with_book(self.token, self.uid, |book| book.price(tick))
    }

    fn best_bid_tick(&self) -> PyResult<Option<i64>> {
        with_book(self.token, self.uid, |book| book.best_bid_tick())
    }

    fn best_ask_tick(&self) -> PyResult<Option<i64>> {
        with_book(self.token, self.uid, |book| book.best_ask_tick())
    }

    fn qty(&self, bid: bool, price: f64) -> PyResult<Option<f64>> {
        with_book(self.token, self.uid, |book| book.qty(bid, price))
    }

    fn bids(&self, limit: i64) -> PyResult<Vec<(i64, f64)>> {
        with_book(self.token, self.uid, |book| level_pairs(book.bids(limit)))
    }

    fn asks(&self, limit: i64) -> PyResult<Vec<(i64, f64)>> {
        with_book(self.token, self.uid, |book| level_pairs(book.asks(limit)))
    }
}

#[pyclass(frozen, module = "mftik._mftik", name = "Poster")]
pub(crate) struct PyPoster {
    inner: Poster<PyStrategy>,
}

#[pymethods]
impl PyPoster {
    fn post(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<()> {
        require_callable(py, &callback)?;
        self.inner
            .post(move |_strategy, ctx| invoke_callback(&callback, ctx));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_rejects_non_finite_and_negative() {
        Python::initialize();
        Python::attach(|_| {
            assert!(delay_duration(f64::NAN).is_err());
            assert!(delay_duration(f64::INFINITY).is_err());
            assert!(delay_duration(f64::NEG_INFINITY).is_err());
            assert!(delay_duration(-0.01).is_err());
            assert!(delay_duration(f64::MAX).is_err());
            assert_eq!(delay_duration(0.0).unwrap(), Duration::ZERO);
            assert_eq!(delay_duration(1.5).unwrap(), Duration::from_millis(1500));
        });
    }

    #[test]
    fn level_pairs_are_tick_then_qty_from_the_best() {
        use mftik_core::book::Level;

        let mut book = OrderBook::new(8, 0.5).expect("book");
        book.apply(
            &[
                Level {
                    price: 100.0,
                    size: 2.0,
                },
                Level {
                    price: 99.0,
                    size: 1.0,
                },
            ],
            &[Level {
                price: 101.0,
                size: 3.0,
            }],
        );
        assert_eq!(level_pairs(book.bids(4)), vec![(200, 2.0), (198, 1.0)]);
        assert_eq!(level_pairs(book.asks(4)), vec![(202, 3.0)]);
        assert!(level_pairs(book.bids(0)).is_empty());
        assert!(level_pairs(book.asks(-1)).is_empty());
    }
}
