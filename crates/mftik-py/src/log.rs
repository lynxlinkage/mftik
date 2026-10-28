//! `mftik.Log` and `mftik.log` (E06-D005, E06-D006, E06-D007).
//!
//! Field values are converted while the GIL is held. Rendering and the
//! stderr write run with the GIL released.

use mftik_rt::log::{Field, Level, Value};
use pyo3::PyTypeInfo;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyString};

/// One logger. `session_id` of `None` omits the key. `Some` writes it,
/// including when the id is empty.
#[pyclass(frozen, module = "mftik._mftik", name = "Log")]
pub(crate) struct PyLog {
    pub(crate) session_id: Option<String>,
}

#[pymethods]
impl PyLog {
    #[pyo3(signature = (message, /, **fields))]
    fn debug(
        &self,
        py: Python<'_>,
        message: &Bound<'_, PyAny>,
        fields: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        emit(
            py,
            Level::Debug,
            self.session_id.as_deref(),
            message,
            fields,
        )
    }

    #[pyo3(signature = (message, /, **fields))]
    fn info(
        &self,
        py: Python<'_>,
        message: &Bound<'_, PyAny>,
        fields: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        emit(py, Level::Info, self.session_id.as_deref(), message, fields)
    }

    #[pyo3(signature = (message, /, **fields))]
    fn warn(
        &self,
        py: Python<'_>,
        message: &Bound<'_, PyAny>,
        fields: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        emit(py, Level::Warn, self.session_id.as_deref(), message, fields)
    }

    #[pyo3(signature = (message, /, **fields))]
    fn error(
        &self,
        py: Python<'_>,
        message: &Bound<'_, PyAny>,
        fields: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        emit(
            py,
            Level::Error,
            self.session_id.as_deref(),
            message,
            fields,
        )
    }
}

fn emit(
    py: Python<'_>,
    level: Level,
    session_id: Option<&str>,
    message: &Bound<'_, PyAny>,
    fields: Option<&Bound<'_, PyDict>>,
) -> PyResult<()> {
    let message = require_message(message)?;
    let fields = convert_fields(fields)?;
    let session = session_id.map(str::to_owned);
    py.detach(move || {
        mftik_rt::log::emit(level, &message, &fields, session.as_deref());
    });
    Ok(())
}

fn require_message(message: &Bound<'_, PyAny>) -> PyResult<String> {
    if !exact::<PyString>(message) {
        return Err(PyTypeError::new_err("log message must be str"));
    }
    message.extract()
}

fn convert_fields(fields: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<Field>> {
    let Some(fields) = fields else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(fields.len());
    for (key, value) in fields.iter() {
        let name: String = key.extract()?;
        out.push(Field {
            name: name.clone(),
            value: convert_value(&name, &value)?,
        });
    }
    Ok(out)
}

fn convert_value(name: &str, value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if value.is_none() {
        return Ok(Value::Null);
    }
    if exact::<PyBool>(value) {
        return Ok(Value::Bool(value.extract()?));
    }
    if exact::<PyInt>(value) {
        return convert_int(name, value);
    }
    if exact::<PyFloat>(value) {
        return Ok(Value::Float(value.extract()?));
    }
    if exact::<PyString>(value) {
        return Ok(Value::Str(value.extract()?));
    }
    Err(type_error(name))
}

fn convert_int(name: &str, value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if let Ok(number) = value.extract::<i64>() {
        return Ok(Value::Int(number));
    }
    let text: String = value.str()?.extract()?;
    if is_decimal(&text) {
        Ok(Value::Big(text))
    } else {
        Err(type_error(name))
    }
}

fn is_decimal(text: &str) -> bool {
    let digits = text.strip_prefix('-').unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn exact<T: PyTypeInfo>(value: &Bound<'_, PyAny>) -> bool {
    value.is_exact_instance_of::<T>()
}

fn type_error(name: &str) -> PyErr {
    PyTypeError::new_err(format!(
        "log field {name} must be str, int, float, bool, or None"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mftik_rt::log::capture;
    use pyo3::types::PyList;

    fn message<'py>(py: Python<'py>, text: &str) -> Bound<'py, PyAny> {
        PyString::new(py, text).into_any()
    }

    #[test]
    fn module_log_writes_json_without_session_id() {
        let _guard = crate::lock_python_tests();
        Python::initialize();
        Python::attach(|py| {
            let log = PyLog { session_id: None };
            let fields = PyDict::new(py);
            fields.set_item("k", "v1").unwrap();
            fields.set_item("n", 1).unwrap();
            fields.set_item("on", true).unwrap();
            fields.set_item("missing", py.None()).unwrap();
            let (_value, bytes) = capture(|| {
                log.info(py, &message(py, "hello"), Some(&fields)).unwrap();
            });
            let text = String::from_utf8(bytes).unwrap();
            assert!(
                text.contains(r#""k":"v1","n":1,"on":true,"missing":null,"message":"hello""#),
                "{text}"
            );
            assert!(!text.contains("session_id"), "{text}");
            assert!(text.starts_with(r#"{"level":"info","#), "{text}");
        });
    }

    #[test]
    fn session_log_keeps_an_empty_session_id() {
        let _guard = crate::lock_python_tests();
        Python::initialize();
        Python::attach(|py| {
            let log = PyLog {
                session_id: Some(String::new()),
            };
            let (_value, bytes) = capture(|| {
                log.warn(py, &message(py, "hello"), None).unwrap();
            });
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.contains(r#""level":"warn""#), "{text}");
            assert!(text.contains(r#""session_id":"""#), "{text}");
        });
    }

    #[test]
    fn rejected_values_are_type_errors() {
        let _guard = crate::lock_python_tests();
        Python::initialize();
        Python::attach(|py| {
            let log = PyLog { session_id: None };
            let err = log
                .info(py, py.eval(c"1", None, None).unwrap().as_ref(), None)
                .unwrap_err();
            assert!(err.is_instance_of::<PyTypeError>(py));
            assert!(err.to_string().contains("log message must be str"), "{err}");

            let locals = PyDict::new(py);
            py.run(
                c"class S(str):\n    pass\nclass N(int):\n    pass\n",
                None,
                Some(&locals),
            )
            .unwrap();
            let subclass = locals
                .get_item("S")
                .unwrap()
                .unwrap()
                .call1(("abc",))
                .unwrap();
            let err = log.info(py, &subclass, None).unwrap_err();
            assert!(err.to_string().contains("log message must be str"), "{err}");

            let fields = PyDict::new(py);
            fields.set_item("k", PyList::empty(py)).unwrap();
            let err = log
                .error(py, &message(py, "hello"), Some(&fields))
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("log field k must be str, int, float, bool, or None"),
                "{err}"
            );

            let number = locals.get_item("N").unwrap().unwrap().call1((3,)).unwrap();
            let fields = PyDict::new(py);
            fields.set_item("k", number).unwrap();
            let err = log
                .debug(py, &message(py, "hello"), Some(&fields))
                .unwrap_err();
            assert!(err.to_string().contains("log field k must be"), "{err}");
        });
    }

    #[test]
    fn a_wide_int_is_a_json_number() {
        let _guard = crate::lock_python_tests();
        Python::initialize();
        Python::attach(|py| {
            let log = PyLog {
                session_id: Some("s1".to_owned()),
            };
            let big = py.eval(c"1 << 80", None, None).unwrap();
            let digits: String = big.str().unwrap().extract().unwrap();
            let fields = PyDict::new(py);
            fields.set_item("k", &big).unwrap();
            let (_value, bytes) = capture(|| {
                log.info(py, &message(py, "hello"), Some(&fields)).unwrap();
            });
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.contains(&format!("\"k\":{digits}")), "{text}");
            assert!(text.contains(r#""session_id":"s1""#), "{text}");
        });
    }
}
