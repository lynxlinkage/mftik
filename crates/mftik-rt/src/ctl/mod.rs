//! The control fd: the worker's end of the C4 socketpair (PROTOCOL §5.1,
//! §5.4, §9.2.1).
//!
//! The shim leaves it at fd 3 without close-on-exec and sets
//! `MFTIKO_CTL_FD=3`. [`CtlFd::from_env`] reads that variable, 3 when it is
//! unset or empty, and sets close-on-exec before it returns. A process the
//! worker starts then neither holds the connection open after the worker
//! dies, which would hide the EOF from the shim, nor can it speak as the
//! worker (§9.2.1). The parsing and exit codes follow mftiko `sdk.Run`
//! (E03-D031).
//!
//! Writes use `send(2)` with `MSG_NOSIGNAL`, so a shim that has gone away is
//! an `EPIPE` error and not a `SIGPIPE` (E03-D032).

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

/// Environment variable that names the control fd (§5.4).
pub const CTL_FD_ENV: &str = "MFTIKO_CTL_FD";

/// The control fd when [`CTL_FD_ENV`] is unset or empty (§5.1).
pub const DEFAULT_CTL_FD: RawFd = 3;

/// Exit code for a [`CtlFdError::Env`]: `INIT_FAILED`, as `sdk.Run` (§5.3).
pub const EXIT_BAD_ENV: i32 = 10;

/// Exit code for a [`CtlFdError::Fd`]: the control connection cannot be
/// spoken. `sdk.Run` reaches it when it cannot write `Hello` (§5.3).
pub const EXIT_BAD_FD: i32 = 1;

/// Why the control fd could not be taken.
#[derive(Debug)]
pub enum CtlFdError {
    /// [`CTL_FD_ENV`] is not a non-negative decimal `i32`.
    Env { value: OsString },
    /// Setting close-on-exec failed, which means nothing is open at `fd`.
    Fd { fd: RawFd, source: io::Error },
}

impl CtlFdError {
    /// The exit code the worker ends with: [`EXIT_BAD_ENV`] or
    /// [`EXIT_BAD_FD`]. Nothing has been written to the shim.
    pub fn exit_code(&self) -> i32 {
        match self {
            CtlFdError::Env { .. } => EXIT_BAD_ENV,
            CtlFdError::Fd { .. } => EXIT_BAD_FD,
        }
    }
}

impl fmt::Display for CtlFdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CtlFdError::Env { value } => write!(f, "{CTL_FD_ENV} {value:?}"),
            CtlFdError::Fd { fd, source } => write!(f, "control fd {fd}: {source}"),
        }
    }
}

impl std::error::Error for CtlFdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CtlFdError::Env { .. } => None,
            CtlFdError::Fd { source, .. } => Some(source),
        }
    }
}

/// Parses the value of [`CTL_FD_ENV`]. Unset or empty is [`DEFAULT_CTL_FD`].
/// Otherwise it is decimal digits with an optional sign, as Go's
/// `strconv.Atoi`, and must not be negative.
pub fn parse_ctl_fd(value: Option<&OsStr>) -> Result<RawFd, CtlFdError> {
    let value = match value {
        None => return Ok(DEFAULT_CTL_FD),
        Some(value) if value.is_empty() => return Ok(DEFAULT_CTL_FD),
        Some(value) => value,
    };
    match value.to_str().map(str::parse::<RawFd>) {
        Some(Ok(fd)) if fd >= 0 => Ok(fd),
        _ => Err(CtlFdError::Env {
            value: value.to_owned(),
        }),
    }
}

/// The worker's end of the C4 socketpair.
///
/// `&CtlFd` reads and writes, so one fd can serve a reader thread and a
/// [`crate::frame::FrameWriter`] through an `Arc`. [`CtlFd::try_clone`]
/// gives a second fd for the same connection instead.
#[derive(Debug)]
pub struct CtlFd {
    fd: OwnedFd,
}

impl CtlFd {
    /// Takes the control fd named by [`CTL_FD_ENV`] and sets close-on-exec
    /// on it before returning.
    ///
    /// # Safety
    ///
    /// The returned value owns the fd and closes it on drop. Call this at
    /// most once, and only when nothing else in the process owns that fd:
    /// it is the one the shim handed over (§5.1).
    pub unsafe fn from_env() -> Result<CtlFd, CtlFdError> {
        let fd = parse_ctl_fd(std::env::var_os(CTL_FD_ENV).as_deref())?;
        // SAFETY: the caller upholds `adopt`'s contract for this fd.
        unsafe { CtlFd::adopt(fd) }
    }

    /// Sets close-on-exec on `fd` and takes ownership of it.
    ///
    /// # Safety
    ///
    /// Nothing else in the process may own `fd`. The returned value closes
    /// it on drop.
    pub unsafe fn adopt(fd: RawFd) -> Result<CtlFd, CtlFdError> {
        // SAFETY: fcntl on any integer is defined; a closed fd is EBADF.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            return Err(CtlFdError::Fd {
                fd,
                source: io::Error::last_os_error(),
            });
        }
        // SAFETY: fd is open, and the caller guarantees no one else owns it.
        Ok(CtlFd {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    /// A second fd for the same connection, with close-on-exec set.
    pub fn try_clone(&self) -> io::Result<CtlFd> {
        Ok(CtlFd {
            fd: self.fd.try_clone()?,
        })
    }
}

/// Wraps an fd the caller already owns, such as one end of
/// `UnixStream::pair`. Its close-on-exec flag is left as it is.
impl From<OwnedFd> for CtlFd {
    fn from(fd: OwnedFd) -> Self {
        CtlFd { fd }
    }
}

impl From<CtlFd> for OwnedFd {
    fn from(ctl: CtlFd) -> Self {
        ctl.fd
    }
}

impl AsFd for CtlFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for CtlFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl Read for &CtlFd {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // SAFETY: buf is valid for writes of buf.len() bytes.
        let n = unsafe { libc::read(self.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(n as usize)
    }
}

impl Read for CtlFd {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buf)
    }
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
const SEND_FLAGS: libc::c_int = libc::MSG_NOSIGNAL;
#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
const SEND_FLAGS: libc::c_int = 0;

impl Write for &CtlFd {
    /// `send(2)` with `MSG_NOSIGNAL`. An fd that is not a socket falls back
    /// to `write(2)`.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let fd = self.as_raw_fd();
        // SAFETY: buf is valid for reads of buf.len() bytes.
        let n = unsafe { libc::send(fd, buf.as_ptr().cast(), buf.len(), SEND_FLAGS) };
        if n >= 0 {
            return Ok(n as usize);
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::ENOTSOCK) {
            return Err(err);
        }
        // SAFETY: as above.
        let n = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(n as usize)
    }

    /// Nothing is buffered here.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Write for CtlFd {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self).flush()
    }
}

#[cfg(test)]
mod tests;
