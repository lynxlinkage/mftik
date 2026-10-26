//! Python extension module `mftik._mftik`.
//!
//! The pure-Python package is `mftik`. This crate only binds: protocol logic
//! stays in `mftik-core` and IO stays in `mftik-rt` (E04-D001).

mod error;
mod proto;
mod sdk;

use pyo3::prelude::*;

pub use error::{context_error, read_error};
pub use proto::ToPyMessage;
pub use sdk::feed_to_py;

/// Register SDK types, proto wrappers, and the error hierarchy on `module`.
pub fn init_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    error::register(py, module)?;
    sdk::register(py, module)?;
    proto::register(py, module)?;
    Ok(())
}

/// Native module imported by `python/mftik/__init__.py`.
#[pymodule]
fn _mftik(module: &Bound<'_, PyModule>) -> PyResult<()> {
    init_module(module.py(), module)
}

#[cfg(test)]
mod tests;
