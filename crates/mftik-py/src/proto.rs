//! Prost messages as frozen pyclasses (E04-D021).
//!
//! `build.rs` reads the descriptor and writes the wrappers to `OUT_DIR`.
//! Field names match the proto. Conversion copies the prost value while the
//! GIL is held.

use std::ffi::CString;

use mftik_proto::wire::mftiko::md::v1::Cursor;
use pyo3::conversion::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyModule, PyStringMethods};

/// A prost message that can be copied into its frozen pyclass.
pub trait ToPyMessage: Sized {
    /// Copy `self` into a Python object. The caller holds the GIL.
    fn to_py(self, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

/// Wrap a proto enum number in the generated `IntEnum`, keeping unknown values.
pub fn enum_value(py: Python<'_>, module: &str, name: &str, value: i32) -> PyResult<Py<PyAny>> {
    let modules = py.import("sys")?.getattr("modules")?;
    let found = match modules.get_item(module) {
        Ok(found) => found,
        Err(_) if module == "mftik" => modules.get_item("mftik._mftik")?,
        Err(err) => return Err(err),
    };
    let class = found.getattr(name)?;
    Ok(class.call1((value,))?.unbind())
}

/// Python `repr` of a value, so text matches what Python would print.
pub fn repr_value<'py, T>(py: Python<'py>, value: T) -> PyResult<String>
where
    T: IntoPyObject<'py>,
{
    Ok(value.into_bound_py_any(py)?.repr()?.to_str()?.to_owned())
}

/// Exec enum classes into `module`'s dict.
pub fn run_enums(py: Python<'_>, module: &Bound<'_, PyModule>, code: &str) -> PyResult<()> {
    let code = CString::new(code).map_err(|err| PyValueError::new_err(err.to_string()))?;
    py.run(&code, Some(&module.dict()), None)
}

/// Copy a `Cursor` pyclass back into prost. Other proto messages are not
/// constructed from Python (E04-D024).
pub fn cursor_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Cursor> {
    Ok(obj.cast::<wrappers::PyMdV1Cursor>()?.borrow().inner)
}

#[allow(clippy::all, unused)]
mod wrappers {
    use pyo3::prelude::*;
    use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

    use super::{ToPyMessage, enum_value, repr_value, run_enums};

    include!(concat!(env!("OUT_DIR"), "/proto_gen.rs"));
}

pub use wrappers::register;
