//! The one way the session worker ends (PROTOCOL §5.3, E03-D005).
//!
//! Seal fd 3, which waits for the frame being written and refuses every later
//! one, Beat included, so no frame is cut by the exit (E03-D032). Flush
//! stdout and stderr, which carry the conformance lines and logs (§9.8). Then
//! `_exit(2)`: no atexit handler, no destructor, no finalizer runs, like Go's
//! `os.Exit`, and a watchdog or timer thread can end the process while
//! another thread holds the GIL.

use std::io::{self, Write};

use crate::frame::FrameWriter;

/// Seals `out`, flushes stdout and stderr, and calls `_exit(code)`.
pub fn exit_with<W: Write>(out: Option<&FrameWriter<W>>, code: i32) -> ! {
    if let Some(out) = out {
        let _ = out.seal();
    }
    let _ = io::stdout().flush();
    let _ = io::stderr().flush();
    // SAFETY: _exit has no preconditions. It does not return.
    unsafe { libc::_exit(code) }
}
