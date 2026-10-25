//! Parsing `MFTIKO_CTL_FD`, close-on-exec, and exit codes.
//!
//! The end-to-end checks, with the variable set and fd 3 handed over in a
//! child process, are in `tests/fd3.rs`.

use std::ffi::OsStr;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;

use super::{CtlFd, CtlFdError, DEFAULT_CTL_FD, EXIT_BAD_ENV, EXIT_BAD_FD, parse_ctl_fd};

fn parse(value: &str) -> Result<RawFd, CtlFdError> {
    parse_ctl_fd(Some(OsStr::new(value)))
}

fn cloexec(fd: RawFd) -> bool {
    // SAFETY: F_GETFD on an open fd.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(flags >= 0, "fd {fd} is open");
    flags & libc::FD_CLOEXEC != 0
}

#[test]
fn unset_or_empty_is_fd_3() {
    assert_eq!(DEFAULT_CTL_FD, 3);
    assert_eq!(parse_ctl_fd(None).expect("unset"), 3);
    assert_eq!(parse("").expect("empty"), 3);
}

#[test]
fn decimal_values_with_an_optional_sign_are_taken() {
    for (value, fd) in [
        ("3", 3),
        ("0", 0),
        ("7", 7),
        ("+5", 5),
        ("007", 7),
        ("-0", 0),
        ("2147483647", RawFd::MAX),
    ] {
        assert_eq!(parse(value).expect(value), fd, "{value:?}");
    }
}

#[test]
fn other_values_are_exit_10() {
    for value in [
        "-1",
        "x",
        "3x",
        " 3",
        "3 ",
        "+",
        "-",
        "0x3",
        "1_0",
        "3.0",
        // Beyond i32. Go's Atoi takes these up to int64 (E03-D031).
        "2147483648",
        "4294967299",
    ] {
        let err = parse(value).unwrap_err();
        assert!(
            matches!(&err, CtlFdError::Env { value: v } if v == value),
            "{value:?}: {err:?}"
        );
        assert_eq!(err.exit_code(), EXIT_BAD_ENV);
    }
    let not_utf8 = OsStr::from_bytes(b"3\xff");
    assert!(matches!(
        parse_ctl_fd(Some(not_utf8)),
        Err(CtlFdError::Env { .. })
    ));
    assert_eq!(EXIT_BAD_ENV, 10);
}

#[test]
fn adopt_sets_close_on_exec() {
    let (_shim, worker) = UnixStream::pair().expect("socketpair");
    let fd = OwnedFd::from(worker);
    let raw = fd.as_raw_fd();
    // SAFETY: F_SETFD on our own open fd. The shim hands fd 3 over like this.
    assert_eq!(unsafe { libc::fcntl(raw, libc::F_SETFD, 0) }, 0);
    assert!(!cloexec(raw));
    std::mem::forget(fd);
    // SAFETY: the OwnedFd above was forgotten, so `raw` has no other owner.
    let ctl = unsafe { CtlFd::adopt(raw) }.expect("adopt");
    assert_eq!(ctl.as_raw_fd(), raw);
    assert!(cloexec(raw));
}

#[test]
fn a_fd_that_is_not_open_is_exit_1() {
    // Far above RLIMIT_NOFILE, so it is never open.
    // SAFETY: the call fails, so nothing takes ownership of RawFd::MAX.
    let err = unsafe { CtlFd::adopt(RawFd::MAX) }.unwrap_err();
    assert!(
        matches!(&err, CtlFdError::Fd { fd: RawFd::MAX, source } if source.raw_os_error() == Some(libc::EBADF)),
        "{err:?}"
    );
    assert_eq!(err.exit_code(), EXIT_BAD_FD);
    assert_eq!(EXIT_BAD_FD, 1);
}

#[test]
fn a_clone_is_another_close_on_exec_fd() {
    let (_shim, worker) = UnixStream::pair().expect("socketpair");
    let ctl = CtlFd::from(OwnedFd::from(worker));
    let clone = ctl.try_clone().expect("dup");
    assert_ne!(clone.as_raw_fd(), ctl.as_raw_fd());
    assert!(cloexec(clone.as_raw_fd()));
}
