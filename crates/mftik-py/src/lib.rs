//! Python extension module `mftik`.
//!
//! The import name matches the package name (E01-D003). Hooks and the rest of
//! the snake_case API land in later epics.

use pyo3::prelude::*;

#[pymodule]
mod mftik {
    #[allow(dead_code)]
    fn core_is_linked() -> &'static str {
        mftik_core::CRATE_NAME
    }
}
