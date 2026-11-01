//! Python `mftik.replay` and the read-only [`PyReplayReport`] (E07-D020–D024).
//!
//! [`_replay`] wraps [`mftik_rt::replay`]. The import package constructs the
//! strategy class and maps `None` options to the Rust defaults. Hook endings
//! are the `{type}: {message}` string a live session stores, so a replay
//! compares the exception class name and its text (E07-D023). Context
//! failures raised during the replay use the same `mftik` subclasses as a
//! live session.

use std::sync::{Mutex, MutexGuard, PoisonError};

use mftik_rt::{ReplayOptions, ReplayReport};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::context::reset_session_random;
use crate::error::ReplayError;
use crate::proto::repr_value;
use crate::strategy::PyStrategy;

/// One incarnation's replay. Fields match [`ReplayReport`]. `mismatches` is
/// a tuple, so the report is read-only (E07-D021).
#[pyclass(frozen, eq, module = "mftik", name = "ReplayReport")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyReplayReport {
    #[pyo3(get)]
    pub(crate) session_id: String,
    #[pyo3(get)]
    pub(crate) incarnation: u32,
    #[pyo3(get)]
    pub(crate) sdk_version: String,
    #[pyo3(get)]
    pub(crate) sdk_commit: String,
    #[pyo3(get)]
    pub(crate) inputs: usize,
    #[pyo3(get)]
    pub(crate) skipped: usize,
    #[pyo3(get)]
    pub(crate) missing: u64,
    #[pyo3(get)]
    pub(crate) orders: usize,
    #[pyo3(get)]
    pub(crate) reads: usize,
    pub(crate) mismatches: Vec<String>,
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

#[pymethods]
impl PyReplayReport {
    #[getter]
    fn mismatches<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(py, &self.mismatches)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let mismatches = PyTuple::new(py, &self.mismatches)?;
        Ok(format!(
            "ReplayReport(session_id={}, incarnation={}, sdk_version={}, sdk_commit={}, inputs={}, skipped={}, missing={}, orders={}, reads={}, mismatches={})",
            repr_value(py, self.session_id.as_str())?,
            repr_value(py, self.incarnation)?,
            repr_value(py, self.sdk_version.as_str())?,
            repr_value(py, self.sdk_commit.as_str())?,
            repr_value(py, self.inputs)?,
            repr_value(py, self.skipped)?,
            repr_value(py, self.missing)?,
            repr_value(py, self.orders)?,
            repr_value(py, self.reads)?,
            mismatches.repr()?.to_str()?,
        ))
    }
}

fn replay_gate() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs `strategy` over one incarnation in `dir`. `incarnation` 0 is the
/// newest file. Empty `volume_dir` and `input_dir` are the Rust defaults.
#[pyfunction]
#[pyo3(signature = (strategy, dir, incarnation=0, volume_dir="", input_dir=""))]
pub(crate) fn _replay(
    py: Python<'_>,
    strategy: Py<PyAny>,
    dir: String,
    incarnation: u32,
    volume_dir: &str,
    input_dir: &str,
) -> PyResult<PyReplayReport> {
    let _gate = replay_gate();
    reset_session_random();
    let strategy = PyStrategy::new(strategy);
    let opt = ReplayOptions {
        incarnation,
        volume_dir: volume_dir.to_owned(),
        input_dir: input_dir.to_owned(),
    };
    let report = py
        .detach(move || mftik_rt::replay(strategy, dir, opt))
        .map_err(|err| ReplayError::new_err(err.to_string()))?;
    Ok(report.into())
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use mftik_core::eventlog::{END_ERROR, STOP_DELIVER, init_head};
    use mftik_proto::wire::mftiko::stsctl::v1::{Bindings, InitPayload, StartPayload};
    use mftik_rt::eventlog::{Header, Kind, Record};
    use prost::Message;
    use pyo3::types::PyDict;

    use super::*;
    use crate::error::ReplayError;
    use crate::tests::with_mftik;

    fn temp_dir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "mftik-py-replay-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("temp dir");
        path
    }

    fn rec(kind: Kind, seq: u64, r#ref: u64, t_ns: i64, body: Vec<u8>) -> Record {
        Record {
            kind,
            flags: 0,
            seq,
            recv_ns: t_ns,
            t_ns,
            r#ref,
            body,
        }
    }

    fn end_of(seq: u64, input: u64, t_ns: i64) -> Record {
        rec(Kind::HOOK_END, seq, input, t_ns, Vec::new())
    }

    fn init_body() -> Vec<u8> {
        let mut body = init_head(0).to_vec();
        body.extend_from_slice(
            &InitPayload {
                session_id: "s1".into(),
                incarnation: 1,
                recv_window_ms: 500,
                hook_timeout_s: 1,
                ..Default::default()
            }
            .encode_to_vec(),
        );
        body
    }

    fn start_body() -> Vec<u8> {
        StartPayload {
            bindings: Some(Bindings::default()),
        }
        .encode_to_vec()
    }

    /// init, start, stop, onstop: a strategy that does nothing in every hook.
    fn noop_log() -> Vec<Record> {
        vec![
            rec(Kind::INIT, 1, 7, 1, init_body()),
            end_of(2, 1, 2),
            rec(Kind::START, 3, 0, 3, start_body()),
            end_of(4, 3, 4),
            Record {
                kind: Kind::STOP,
                flags: STOP_DELIVER,
                seq: 5,
                recv_ns: 5,
                t_ns: 5,
                r#ref: 0,
                body: Vec::new(),
            },
            rec(Kind::ONSTOP, 6, 0, 6, Vec::new()),
            end_of(7, 6, 7),
            rec(Kind::END, 8, 0, 8, 0u64.to_le_bytes().to_vec()),
        ]
    }

    fn write_log(dir: &Path, recs: &[Record]) {
        fs::create_dir_all(dir).expect("eventlog dir");
        let header = Header {
            format_version: 1,
            incarnation: 1,
            n: 1,
            created_ns: 1,
            session_id: "s1".into(),
            sdk_version: "1".into(),
            sdk_commit: "a".into(),
        };
        let mut bytes = Vec::new();
        header.encode(&mut bytes);
        for rec in recs {
            rec.encode(&mut bytes);
        }
        fs::write(dir.join(mftik_rt::eventlog::name(1, 1)), bytes).expect("log file");
    }

    fn make_strategy(py: Python<'_>, class_body: &str) -> Py<PyAny> {
        let script = format!(
            r#"
import sys
import types
_mod = types.ModuleType("mftik.strategy")
class Strategy:
    def on_init(self, ctx):
        return None
    def on_start(self, ctx):
        return None
    def on_stop(self, ctx):
        return None
_mod.Strategy = Strategy
sys.modules["mftik.strategy"] = _mod
{class_body}
obj = Cls()
"#
        );
        let locals = PyDict::new(py);
        py.run(
            CString::new(script).unwrap().as_c_str(),
            Some(&locals),
            Some(&locals),
        )
        .expect("define strategy");
        locals.get_item("obj").unwrap().expect("obj").unbind()
    }

    fn call_replay(
        py: Python<'_>,
        obj: Py<PyAny>,
        dir: &Path,
        incarnation: u32,
        volume_dir: &str,
        input_dir: &str,
    ) -> PyResult<PyReplayReport> {
        _replay(
            py,
            obj,
            dir.to_str().expect("utf-8").to_owned(),
            incarnation,
            volume_dir,
            input_dir,
        )
    }

    fn err_text(py: Python<'_>, err: &PyErr) -> String {
        err.value(py).str().unwrap().to_str().unwrap().to_owned()
    }

    #[test]
    fn an_empty_directory_raises_replay_error() {
        with_mftik(|py| {
            let dir = temp_dir("empty");
            let obj = make_strategy(py, "class Cls(Strategy):\n    pass\n");
            let err = call_replay(py, obj, &dir, 0, "", "").expect_err("empty");
            assert!(
                err.value(py)
                    .is_instance(&py.get_type::<ReplayError>())
                    .unwrap()
            );
            let text = err_text(py, &err);
            assert!(text.contains("has no event log of incarnation 0"), "{text}");
            let _ = fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn a_bad_header_raises_replay_error() {
        with_mftik(|py| {
            let dir = temp_dir("bad");
            fs::write(dir.join("1-1.mel"), b"not-a-log").expect("garbage");
            let obj = make_strategy(py, "class Cls(Strategy):\n    pass\n");
            let err = call_replay(py, obj, &dir, 0, "", "").expect_err("bad header");
            assert!(
                err.value(py)
                    .is_instance(&py.get_type::<ReplayError>())
                    .unwrap()
            );
            let text = err_text(py, &err);
            assert!(text.contains("sdk: replay:"), "{text}");
            let _ = fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn a_matching_strategy_replays_with_no_mismatch() {
        with_mftik(|py| {
            let dir = temp_dir("ok");
            write_log(&dir, &noop_log());
            let obj = make_strategy(py, "class Cls(Strategy):\n    pass\n");
            let report = call_replay(py, obj, &dir, 0, "", "").expect("replay");
            assert_eq!(report.session_id, "s1");
            assert_eq!(report.incarnation, 1);
            assert_eq!(report.sdk_version, "1");
            assert_eq!(report.sdk_commit, "a");
            assert_eq!(report.inputs, 3, "{report:?}");
            assert_eq!(report.skipped, 0);
            assert_eq!(report.missing, 0);
            assert_eq!(report.orders, 0);
            assert_eq!(report.reads, 0);
            assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
            let _ = fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn a_changed_strategy_produces_a_mismatch() {
        with_mftik(|py| {
            let dir = temp_dir("changed");
            write_log(&dir, &noop_log());
            let obj = make_strategy(
                py,
                r#"
class Cls(Strategy):
    def on_start(self, ctx):
        raise RuntimeError("changed")
"#,
            );
            let report = call_replay(py, obj, &dir, 0, "", "").expect("replay");
            assert!(
                report
                    .mismatches
                    .iter()
                    .any(|line| line.contains("rec_seq 3 start") && line.contains("changed")),
                "{:?}",
                report.mismatches
            );
            let _ = fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn hook_endings_compare_the_exception_class_and_message() {
        with_mftik(|py| {
            let dir = temp_dir("cls");
            let mut recs = noop_log();
            recs[3] = Record {
                kind: Kind::HOOK_END,
                flags: END_ERROR,
                seq: 4,
                recv_ns: 0,
                t_ns: 4,
                r#ref: 3,
                body: b"RuntimeError: changed".to_vec(),
            };
            write_log(&dir, &recs);
            let same = make_strategy(
                py,
                r#"
class Cls(Strategy):
    def on_start(self, ctx):
        raise RuntimeError("changed")
"#,
            );
            let matched = call_replay(py, same, &dir, 0, "", "").expect("same class");
            assert!(matched.mismatches.is_empty(), "{:?}", matched.mismatches);
            let other = make_strategy(
                py,
                r#"
class Cls(Strategy):
    def on_start(self, ctx):
        raise ValueError("changed")
"#,
            );
            let diverged = call_replay(py, other, &dir, 0, "", "").expect("other class");
            assert!(
                diverged
                    .mismatches
                    .iter()
                    .any(|line| line.contains("RuntimeError") && line.contains("ValueError")),
                "{:?}",
                diverged.mismatches
            );
            let _ = fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn a_recorded_mftik_exception_is_raised_as_the_same_class() {
        with_mftik(|py| {
            let dir = temp_dir("uid");
            let mut recs = noop_log();
            recs[3] = Record {
                kind: Kind::HOOK_END,
                flags: END_ERROR,
                seq: 4,
                recv_ns: 0,
                t_ns: 4,
                r#ref: 3,
                body: b"UnknownUid: sdk: the instrument is not known to this session".to_vec(),
            };
            write_log(&dir, &recs);
            let obj = make_strategy(
                py,
                r#"
class Cls(Strategy):
    def on_start(self, ctx):
        ctx.info(1)
"#,
            );
            let report = call_replay(py, obj, &dir, 0, "", "").expect("replay");
            assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
            let _ = fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn volume_and_input_dirs_are_what_the_options_set() {
        with_mftik(|py| {
            let dir = temp_dir("opts");
            write_log(&dir, &noop_log());
            let obj = make_strategy(
                py,
                r#"
class Cls(Strategy):
    def on_init(self, ctx):
        self.volume = ctx.volume_dir()
        self.input = ctx.input_dir("model")
"#,
            );
            let report = call_replay(py, obj.clone_ref(py), &dir, 0, "vol", "in").expect("replay");
            assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
            let bound = obj.bind(py);
            let volume: String = bound.getattr("volume").unwrap().extract().unwrap();
            let input: String = bound.getattr("input").unwrap().extract().unwrap();
            assert_eq!(volume, "vol");
            assert_eq!(input, "in/model");
            let _ = fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn a_second_replay_in_the_same_process_is_a_new_rand() {
        with_mftik(|py| {
            let dir = temp_dir("rand");
            write_log(&dir, &noop_log());
            let first = make_strategy(py, "class Cls(Strategy):\n    pass\n");
            let second = make_strategy(py, "class Cls(Strategy):\n    pass\n");
            let a = call_replay(py, first, &dir, 0, "", "").expect("first");
            let b = call_replay(py, second, &dir, 0, "", "").expect("second");
            assert!(a.mismatches.is_empty(), "{:?}", a.mismatches);
            assert!(b.mismatches.is_empty(), "{:?}", b.mismatches);
            let _ = fs::remove_dir_all(&dir);
        });
    }
}
