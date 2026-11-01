//! `mftik.Error` and the subclasses whose text matches the Go SDK.

use mftik_core::mdpath::ReadError as MdReadError;
use mftik_rt::Error as ContextFailure;
use pyo3::prelude::*;

pyo3::create_exception!(mftik, Error, pyo3::exceptions::PyException);
pyo3::create_exception!(mftik, NotInInit, Error);
pyo3::create_exception!(mftik, UnknownUid, Error);
pyo3::create_exception!(mftik, UnknownAccount, Error);
pyo3::create_exception!(mftik, AlgoNotDeclared, Error);
pyo3::create_exception!(mftik, NoFeed, Error);
pyo3::create_exception!(mftik, Stopping, Error);
pyo3::create_exception!(mftik, ReadError, Error);
pyo3::create_exception!(
    mftik,
    ContextError,
    Error,
    "The Context is no longer valid for this call."
);
pyo3::create_exception!(
    mftik,
    ReplayError,
    Error,
    "The event log could not be replayed."
);

pub fn register(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("Error", py.get_type::<Error>())?;
    module.add("NotInInit", py.get_type::<NotInInit>())?;
    module.add("UnknownUid", py.get_type::<UnknownUid>())?;
    module.add("UnknownAccount", py.get_type::<UnknownAccount>())?;
    module.add("AlgoNotDeclared", py.get_type::<AlgoNotDeclared>())?;
    module.add("NoFeed", py.get_type::<NoFeed>())?;
    module.add("Stopping", py.get_type::<Stopping>())?;
    module.add("ReadError", py.get_type::<ReadError>())?;
    module.add("ContextError", py.get_type::<ContextError>())?;
    module.add("ReplayError", py.get_type::<ReplayError>())?;
    Ok(())
}

/// Map a Context failure onto the Python hierarchy. `Other` and `NotWired`
/// stay on the base class, with their text unchanged.
pub fn context_error(err: &ContextFailure) -> PyErr {
    let text = err.to_string();
    match err {
        ContextFailure::NotInInit => NotInInit::new_err(text),
        ContextFailure::UnknownUid => UnknownUid::new_err(text),
        ContextFailure::UnknownAccount => UnknownAccount::new_err(text),
        ContextFailure::AlgoNotDeclared => AlgoNotDeclared::new_err(text),
        ContextFailure::NoFeed => NoFeed::new_err(text),
        ContextFailure::Stopping => Stopping::new_err(text),
        ContextFailure::NotWired(_) | ContextFailure::Other(_) => Error::new_err(text),
        _ => Error::new_err(text),
    }
}

/// The read callback's error. One class; the text is the Rust `Display`.
pub fn read_error(err: &MdReadError) -> PyErr {
    ReadError::new_err(err.to_string())
}
