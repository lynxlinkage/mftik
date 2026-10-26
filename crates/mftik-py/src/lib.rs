//! Python extension module `mftik._mftik`.
//!
//! The import package is pure Python (`python/mftik/`). This module binds
//! `mftik-rt` (the session runtime) and the proto and SDK types. Protocol
//! logic stays in `mftik-core` and IO stays in `mftik-rt` (E04-D001).

mod context;
mod error;
mod proto;
mod sdk;
mod strategy;

use mftik_rt::Strategy;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::context::{PyContext, PyPoster};
use crate::error::ContextError;
use crate::strategy::{LoadFailed, PyStrategy};

pub use error::{context_error, read_error};
pub use proto::ToPyMessage;
pub use sdk::feed_to_py;

/// `mftik.Context` is only valid inside the hook that received it (E04-D005).
const CONTEXT_ERROR: &str = "mftik.Context is only valid inside the hook that received it";

/// Register SDK types, proto wrappers, and the error hierarchy on `module`.
pub fn init_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    error::register(py, module)?;
    sdk::register(py, module)?;
    proto::register(py, module)?;
    Ok(())
}

#[pymodule]
#[pyo3(name = "_mftik")]
fn mftik(m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_module(m.py(), m)?;
    m.add_class::<PyContext>()?;
    m.add_class::<PyPoster>()?;
    m.add_function(wrap_pyfunction!(_run, m)?)?;
    m.add_function(wrap_pyfunction!(_run_load_failed, m)?)?;
    m.add_function(wrap_pyfunction!(_registered_hooks, m)?)?;
    Ok(())
}

/// Hosts `strategy` until the process `_exit`s. `release` is `Hello.release`.
#[pyfunction]
fn _run(py: Python<'_>, strategy: Py<PyAny>, release: String) -> PyResult<()> {
    install_before_exit();
    let strategy = PyStrategy::new(strategy);
    py.detach(move || enter(strategy, release));
    Ok(())
}

/// Enters the runtime with an OnInit failure. Import, construction, and the
/// `Strategy` check use this so the process still speaks Hello (E04-D012).
#[pyfunction]
fn _run_load_failed(py: Python<'_>, message: String, release: String) -> PyResult<()> {
    install_before_exit();
    py.detach(move || enter(LoadFailed { error: message }, release));
    Ok(())
}

/// Optional hooks [`mftik_rt::Strategy::handlers`] would register for `strategy`.
#[pyfunction]
fn _registered_hooks(strategy: Py<PyAny>) -> PyResult<Vec<String>> {
    let strategy = PyStrategy::new(strategy);
    let mut handlers = mftik_rt::Handlers::<PyStrategy>::default();
    strategy.handlers(&mut handlers);
    Ok(handlers
        .registered()
        .into_iter()
        .map(str::to_owned)
        .collect())
}

fn enter<S: mftik_rt::Strategy>(strategy: S, release: String) {
    mftik_rt::run(strategy, Some(release.as_str()));
}

fn install_before_exit() {
    mftik_rt::exit::set_before_exit(before_exit);
}

/// Flushes Python's stdout and stderr. Skips when a hook is on the stack:
/// that thread holds the GIL, and attaching here would deadlock (E04-D002).
/// `HUNG` never reaches this callback.
fn before_exit(code: i32) {
    if code == mftik_core::c4::EXIT_HUNG || context::hook_depth() != 0 {
        return;
    }
    Python::attach(|py| {
        let Ok(sys) = py.import("sys") else {
            return;
        };
        for name in ["stdout", "stderr"] {
            if let Ok(stream) = sys.getattr(name) {
                let _ = stream.call_method0("flush");
            }
        }
    });
}

pub(crate) fn invalid_context() -> PyErr {
    ContextError::new_err(CONTEXT_ERROR)
}

pub(crate) fn describe(py: Python<'_>, err: &PyErr) -> String {
    let name = err
        .get_type(py)
        .name()
        .ok()
        .and_then(|value| value.extract::<String>().ok())
        .unwrap_or_else(|| "Exception".to_owned());
    let text = err
        .value(py)
        .str()
        .and_then(|value| value.extract::<String>())
        .unwrap_or_default();
    format!("{name}: {text}")
}

/// Writes the traceback to stderr without treating `SystemExit` as an exit.
///
/// The type, value, and traceback are passed separately. On Python 3.11 PyO3
/// keeps the traceback off the exception value, so printing the value alone
/// drops the stack.
pub(crate) fn write_traceback(py: Python<'_>, err: &PyErr) {
    let Ok(traceback) = py.import("traceback") else {
        return;
    };
    let tb = err.traceback(py).map(|traceback| traceback.into_any());
    let _ = traceback.call_method1("print_exception", (err.get_type(py), err.value(py), tb));
}

pub(crate) fn require_callable(py: Python<'_>, callback: &Py<PyAny>) -> PyResult<()> {
    if callback.bind(py).is_callable() {
        Ok(())
    } else {
        Err(PyTypeError::new_err("expected a callable"))
    }
}

#[cfg(test)]
mod tests;
