//! Python `Context` and `Poster` (E04-D005, E04-D011).
//!
//! A `Context` handed to a hook stores only a token. The token matches the
//! hook's thread-local scope, so using it after the hook returns, or from any
//! other thread, raises `mftik.ContextError` instead of touching the runtime.
//! [`Poster`] is the one object that may cross threads.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use mftik_rt::{Context, Poster};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::strategy::PyStrategy;
use crate::{describe, invalid_context, require_callable, write_traceback};

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

fn with_context<T>(token: u64, body: impl FnOnce(&Context<PyStrategy>) -> T) -> PyResult<T> {
    ACTIVE.with(|slot| {
        let ptr = slot.get();
        if ptr.is_null() {
            return Err(invalid_context());
        }
        // SAFETY: a non-null slot on this thread was stored by `HookScope::enter`
        // and points at that scope's `Active` until the scope drops. The token
        // must match, so a context from another hook is rejected before the
        // `Context` pointer is used. Other threads never see this slot.
        let active = unsafe { &*ptr };
        if active.token != token {
            return Err(invalid_context());
        }
        let ctx = unsafe { &*active.ctx };
        Ok(body(ctx))
    })
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
/// traceback has been written to stderr (E04-D004). A return value is ignored
/// (E04-D033).
pub(crate) fn call_method(
    py: Python<'_>,
    obj: &Py<PyAny>,
    method: &str,
    token: u64,
) -> Result<(), String> {
    invoke(py, obj, method, token, None)
}

/// Calls `obj.method(ctx, event)`. Same exception and return rules as
/// [`call_method`]. `event` is a new object for this delivery (E04-D034).
pub(crate) fn call_hook(
    py: Python<'_>,
    obj: &Py<PyAny>,
    method: &str,
    token: u64,
    event: Py<PyAny>,
) -> Result<(), String> {
    invoke(py, obj, method, token, Some(event))
}

fn invoke(
    py: Python<'_>,
    obj: &Py<PyAny>,
    method: &str,
    token: u64,
    event: Option<Py<PyAny>>,
) -> Result<(), String> {
    let py_ctx = match py_context(py, token) {
        Ok(ctx) => ctx,
        Err(err) => return fail(py, err),
    };
    let result = match event {
        Some(event) => obj.bind(py).call_method1(method, (py_ctx, event)),
        None => obj.bind(py).call_method1(method, (py_ctx,)),
    };
    match result {
        // Hooks have no result. A value the strategy returns is dropped.
        Ok(_) => Ok(()),
        Err(err) => fail(py, err),
    }
}

fn fail(py: Python<'_>, err: PyErr) -> Result<(), String> {
    let message = describe(py, &err);
    write_traceback(py, &err);
    Err(message)
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
}
