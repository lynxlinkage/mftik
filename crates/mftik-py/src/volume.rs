//! `mftik.volume` file IO (PROTOCOL §9.1.4, E08-D051, E08-D052, E04-D044).
//!
//! The public functions live in `python/mftik/volume.py`. They accept
//! `os.PathLike` and call these two, which run [`mftik_rt::volume`]. A `str`
//! is not bytes: it is not encoded.

use std::io;

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyOSError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

const STR_DATA: &str = "volume data must be bytes-like, not str";

/// Copies `bytes`, `bytearray`, or any other bytes-like buffer. `str` is a
/// [`TypeError`](PyTypeError); nothing here encodes text.
fn owned_bytes(data: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if data.is_instance_of::<PyString>() {
        return Err(PyTypeError::new_err(STR_DATA));
    }
    let buffer = PyBuffer::<u8>::get(data)?;
    buffer.to_vec(data.py())
}

fn os_error(err: io::Error) -> PyErr {
    PyOSError::new_err(err.to_string())
}

/// Atomically replaces `dir`/`name` with `data`. The GIL is released for the
/// filesystem calls. `dir` and `name` are already `str`; path conversion is
/// the Python wrapper's.
#[pyfunction]
pub(crate) fn volume_write(
    py: Python<'_>,
    dir: &str,
    name: &str,
    data: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let dir = dir.to_owned();
    let name = name.to_owned();
    let data = owned_bytes(data)?;
    py.detach(move || mftik_rt::volume::write(&dir, &name, &data))
        .map_err(os_error)
}

/// The file `name` from `volume_dir`, else from `input_dir`. `None` when
/// neither has it. The GIL is released while the files are read.
#[pyfunction]
pub(crate) fn volume_read<'py>(
    py: Python<'py>,
    volume_dir: &str,
    input_dir: &str,
    name: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let volume_dir = volume_dir.to_owned();
    let input_dir = input_dir.to_owned();
    let name = name.to_owned();
    match py
        .detach(move || mftik_rt::volume::read(&volume_dir, &input_dir, &name))
        .map_err(os_error)?
    {
        Some(data) => Ok(PyBytes::new(py, &data).into_any()),
        None => Ok(py.None().into_bound(py)),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use pyo3::types::{PyByteArray, PyBytes, PyString};

    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "mftik-py-volume-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("temp dir");
        path
    }

    fn py_bytes<'py>(py: Python<'py>, data: &[u8]) -> Bound<'py, PyAny> {
        PyBytes::new(py, data).into_any()
    }

    #[test]
    fn write_and_read_follow_the_volume_then_the_input() {
        Python::initialize();
        Python::attach(|py| {
            let volume = temp_dir("vol");
            let input = temp_dir("in");
            let volume_s = volume.to_str().expect("utf-8");
            let input_s = input.to_str().expect("utf-8");
            fs::create_dir(volume.join("nest")).expect("nest");

            volume_write(py, volume_s, "nest/leaf", &py_bytes(py, b"one")).unwrap();
            let nested = volume_read(py, volume_s, "", "nest/leaf").unwrap();
            assert_eq!(nested.cast::<PyBytes>().unwrap().as_bytes(), b"one");

            let array = PyByteArray::new(py, b"two");
            volume_write(py, volume_s, "nest/leaf", array.as_any()).unwrap();
            let replaced = volume_read(py, volume_s, input_s, "nest/leaf").unwrap();
            assert_eq!(replaced.cast::<PyBytes>().unwrap().as_bytes(), b"two");

            fs::write(input.join("only"), b"in").expect("input");
            let from_input = volume_read(py, volume_s, input_s, "only").unwrap();
            assert_eq!(from_input.cast::<PyBytes>().unwrap().as_bytes(), b"in");
            volume_write(py, volume_s, "only", &py_bytes(py, b"")).unwrap();
            let volume_wins = volume_read(py, volume_s, input_s, "only").unwrap();
            assert_eq!(volume_wins.cast::<PyBytes>().unwrap().as_bytes(), b"");

            assert!(
                volume_read(py, volume_s, input_s, "missing")
                    .unwrap()
                    .is_none()
            );
            assert!(!volume_read(py, "", input_s, "only").unwrap().is_none());
            let skipped = volume_read(py, "", "", "only").unwrap();
            assert!(skipped.is_none());

            let _ = fs::remove_dir_all(&volume);
            let _ = fs::remove_dir_all(&input);
        });
    }

    #[test]
    fn a_str_is_rejected_and_a_name_cannot_escape() {
        Python::initialize();
        Python::attach(|py| {
            let volume = temp_dir("bad");
            let volume_s = volume.to_str().expect("utf-8");
            let text = PyString::new(py, "text");
            let err = volume_write(py, volume_s, "n", text.as_any()).unwrap_err();
            assert!(err.is_instance_of::<PyTypeError>(py));
            assert_eq!(err.to_string(), format!("TypeError: {STR_DATA}"));

            for name in ["", ".", "..", "/abs", "a/../b", "a/./b", "a//b"] {
                let err = volume_write(py, volume_s, name, &py_bytes(py, b"x")).unwrap_err();
                assert!(err.is_instance_of::<PyOSError>(py), "{name}: {err}");
                assert!(
                    err.to_string()
                        .contains("volume: name must be a relative path"),
                    "{name}: {err}"
                );
                let err = volume_read(py, volume_s, "", name).unwrap_err();
                assert!(err.is_instance_of::<PyOSError>(py), "{name}: {err}");
            }
            let err = volume_write(py, "", "n", &py_bytes(py, b"x")).unwrap_err();
            assert!(err.to_string().contains("volume: empty directory"), "{err}");
            assert!(fs::read_dir(&volume).unwrap().next().is_none());
            let _ = fs::remove_dir_all(&volume);
        });
    }
}
