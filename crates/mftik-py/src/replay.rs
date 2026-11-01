//! `mftik.replay` (E07-D020–E07-D023): a Python strategy run again over one
//! incarnation of its event log through [`mftik_rt::replay`], without NATS
//! or C4.
//!
//! Hooks run on the calling thread with the GIL taken back for each one, as
//! in a session. A hook's exception ends it with `{type}: {str(e)}`, the
//! text the session recorded, so the engine compares class name and
//! message. Errors from the log (Resolve, reads) come back as the same
//! `mftik` classes because they pass through the same mapping.
//!
//! The engine keeps process state: the client order id counter, the note
//! session, and `Context.rand`'s `random.Random`. One replay runs at a time;
//! a call from another thread waits. A process that hosts a session, or a
//! hook, cannot replay.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};

use mftik_rt::{ReplayOptions, ReplayReport};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::context::{in_hook, reset_random};
use crate::error::ReplayError;
use crate::proto::repr_value;
use crate::strategy::PyStrategy;

pub(crate) const IN_SESSION: &str = "sdk: replay: this process hosts a session";
pub(crate) const IN_HOOK: &str = "sdk: replay: called inside a hook";

/// Set once `mftik.run` has entered the runtime in this process.
static SESSION: AtomicBool = AtomicBool::new(false);
/// One replay at a time in a process (see the module docs).
static ONE: Mutex<()> = Mutex::new(());

/// Called before a session enters the runtime.
pub(crate) fn mark_session() {
    SESSION.store(true, Ordering::SeqCst);
}

/// What [`mftik_rt::replay`] found. Read-only; the fields are the Rust
/// report's (E07-D021).
#[pyclass(frozen, module = "mftik", name = "ReplayReport")]
pub(crate) struct PyReplayReport {
    #[pyo3(get)]
    session_id: String,
    #[pyo3(get)]
    incarnation: u32,
    #[pyo3(get)]
    sdk_version: String,
    #[pyo3(get)]
    sdk_commit: String,
    #[pyo3(get)]
    inputs: usize,
    #[pyo3(get)]
    skipped: usize,
    #[pyo3(get)]
    missing: u64,
    #[pyo3(get)]
    orders: usize,
    #[pyo3(get)]
    reads: usize,
    mismatches: Vec<String>,
}

#[pymethods]
impl PyReplayReport {
    /// Each difference, in the order the replay found it. Empty means the
    /// replay made the same calls and ended each hook the same way.
    #[getter]
    fn mismatches<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(py, &self.mismatches)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "ReplayReport(session_id={}, incarnation={}, sdk_version={}, sdk_commit={}, \
             inputs={}, skipped={}, missing={}, orders={}, reads={}, mismatches={})",
            repr_value(py, &self.session_id)?,
            repr_value(py, self.incarnation)?,
            repr_value(py, &self.sdk_version)?,
            repr_value(py, &self.sdk_commit)?,
            repr_value(py, self.inputs)?,
            repr_value(py, self.skipped)?,
            repr_value(py, self.missing)?,
            repr_value(py, self.orders)?,
            repr_value(py, self.reads)?,
            repr_value(py, self.mismatches(py)?)?,
        ))
    }
}

impl From<ReplayReport> for PyReplayReport {
    fn from(report: ReplayReport) -> Self {
        Self {
            session_id: report.session_id,
            incarnation: report.incarnation,
            sdk_version: report.sdk_version,
            sdk_commit: report.sdk_commit,
            inputs: report.inputs,
            skipped: report.skipped,
            missing: report.missing,
            orders: report.orders,
            reads: report.reads,
            mismatches: report.mismatches,
        }
    }
}

/// `mftik.replay` after its arguments are checked: `strategy` is a new
/// instance, `incarnation` 0 is the newest, and empty directories are unset.
#[pyfunction]
pub(crate) fn _run_replay(
    py: Python<'_>,
    strategy: Py<PyAny>,
    dir: PathBuf,
    incarnation: u32,
    volume_dir: String,
    input_dir: String,
) -> PyResult<PyReplayReport> {
    if SESSION.load(Ordering::SeqCst) {
        return Err(ReplayError::new_err(IN_SESSION));
    }
    if in_hook() {
        return Err(ReplayError::new_err(IN_HOOK));
    }
    let strategy = PyStrategy::new(strategy);
    let opt = ReplayOptions {
        incarnation,
        volume_dir,
        input_dir,
    };
    // Wait for the lock without the GIL: the replay that holds it needs the
    // GIL for its hooks.
    let result = py.detach(move || {
        let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
        Python::attach(reset_random);
        mftik_rt::replay(strategy, dir, opt)
    });
    match result {
        Ok(report) => Ok(report.into()),
        Err(err) => Err(ReplayError::new_err(err.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use mftik_rt::Strategy;
    use pyo3::prelude::*;
    use pyo3::types::PyDict;
    use pyo3::wrap_pyfunction;

    use super::{_run_replay, IN_HOOK};
    use crate::strategy::PyStrategy;
    use crate::tests::with_mftik;

    /// A hook cannot replay: the engine would restore the client order id
    /// counter and the rand state of the session the hook belongs to.
    #[test]
    fn a_replay_inside_a_hook_is_a_replay_error() {
        with_mftik(|py| {
            let module = py.import("mftik").unwrap();
            module
                .add_function(wrap_pyfunction!(_run_replay, &module).unwrap())
                .unwrap();
            let locals = PyDict::new(py);
            py.run(
                cr#"
import mftik

seen = []

class Inner:
    pass

class Outer:
    def on_init(self, ctx):
        try:
            mftik._run_replay(Inner(), ".", 0, "", "")
        except mftik.ReplayError as err:
            seen.append(str(err))

obj = Outer()
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let obj = locals.get_item("obj").unwrap().unwrap().unbind();
            mftik_rt::in_one_hook(PyStrategy::new(obj), |strategy, ctx| {
                strategy.on_init(ctx).unwrap();
            })
            .unwrap();
            let seen: Vec<String> = locals.get_item("seen").unwrap().unwrap().extract().unwrap();
            assert_eq!(seen, vec![IN_HOOK.to_owned()]);
        });
    }
}
