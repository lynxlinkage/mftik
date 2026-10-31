//! The one way the session worker ends (PROTOCOL §5.3, E03-D005).
//!
//! Seal fd 3, which waits for the frame being written and refuses every later
//! one, Beat included, so no frame is cut by the exit (E03-D032). Close the
//! event log, if the session has one: `end` is written and the ring reaches
//! the file, waiting at most five seconds (§12.1). Flush stdout and stderr,
//! which carry the conformance lines and logs (§9.8). Then `_exit(2)`: no
//! atexit handler, no destructor, no finalizer runs, like Go's `os.Exit`,
//! and a watchdog or timer thread can end the process while another thread
//! holds the GIL.
//!
//! The Python binding registers [`set_before_exit`] so it can flush
//! `sys.stdout` and `sys.stderr` before `_exit` drops Python's buffers
//! (E04-D002). [`mftik_core::c4::EXIT_HUNG`] skips that callback: the hook is
//! still running on the thread that holds the GIL.

use std::io::{self, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, OnceLock, Weak};

use mftik_core::c4::EXIT_HUNG;

use crate::eventlog::Log;
use crate::frame::FrameWriter;

static BEFORE_EXIT: OnceLock<fn(i32)> = OnceLock::new();

static EVENT_LOG: OnceLock<Weak<Log>> = OnceLock::new();

/// Registers `callback` to run after the runtime flushes its own stdout and
/// stderr and before `_exit`. The first registration wins. [`exit_with`] does
/// not call it for [`EXIT_HUNG`]. A panic in the callback is discarded so the
/// process still exits.
pub fn set_before_exit(callback: fn(i32)) {
    let _ = BEFORE_EXIT.set(callback);
}

/// Registers the session's event log for [`exit_with`] to close. The first
/// registration wins.
pub(crate) fn close_at_exit(log: &Arc<Log>) {
    let _ = EVENT_LOG.set(Arc::downgrade(log));
}

/// Seals `out`, closes the event log, flushes stdout and stderr, runs the
/// binding callback unless this is `HUNG`, and calls `_exit(code)`.
pub fn exit_with<W: Write>(out: Option<&FrameWriter<W>>, code: i32) -> ! {
    if let Some(out) = out {
        let _ = out.seal();
    }
    if let Some(log) = EVENT_LOG.get().and_then(Weak::upgrade) {
        let _ = catch_unwind(AssertUnwindSafe(|| log.close()));
    }
    let _ = io::stdout().flush();
    let _ = io::stderr().flush();
    if code != EXIT_HUNG
        && let Some(callback) = BEFORE_EXIT.get().copied()
    {
        let _ = catch_unwind(AssertUnwindSafe(|| callback(code)));
    }
    // SAFETY: _exit has no preconditions. It does not return.
    unsafe { libc::_exit(code) }
}
