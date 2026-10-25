//! C3/C4 framing: a 4-byte big-endian length, then one protobuf message
//! whose body is at most 1 MiB (PROTOCOL §4.1, §5.1, §9.2.1).
//!
//! [`FrameWriter`] is shared by every thread that writes fd 3. It holds one
//! lock for each whole frame, header and body, so frames from different
//! threads never interleave (E03-D003). [`FrameReader`] has one owner. It
//! tells a clean end between frames from an end inside a frame, and it
//! refuses an oversized frame from its header alone, before it reads or
//! allocates the body. Both follow mftiko `contract/wire/frame` (E03-D032).

use std::fmt;
use std::io::{self, Read, Write};
use std::sync::{Mutex, MutexGuard, PoisonError};

use prost::Message;

/// Length prefix: 4 bytes, big-endian.
pub const HEADER_LEN: usize = 4;

/// Largest message body on C3 and C4, not counting the header: 1 MiB.
pub const MAX_LEN: usize = 1 << 20;

/// Largest buffer a reader or writer keeps between frames. A bigger one,
/// needed for an occasional large frame, is dropped after that frame.
const KEEP_BUF: usize = 64 << 10;

/// Where a stream ended inside a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Part {
    Header,
    Body,
}

impl fmt::Display for Part {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Part::Header => "header",
            Part::Body => "body",
        })
    }
}

/// A frame that could not be written or read.
///
/// After [`FrameError::Truncated`], [`FrameError::TooLarge`] from a read, or
/// [`FrameError::Io`], the stream is unusable. After [`FrameError::Decode`]
/// the frame was read whole and the stream is still in step.
#[derive(Debug)]
pub enum FrameError {
    /// The body is longer than [`MAX_LEN`]. On a write nothing was written.
    /// On a read the header declared it and nothing of the body was read.
    TooLarge { len: usize },
    /// The stream ended inside a frame, after `got` of the `want` bytes of
    /// its header or body.
    Truncated { part: Part, got: usize, want: usize },
    /// The body is not a valid message of the requested type.
    Decode(prost::DecodeError),
    /// The underlying reader or writer failed.
    Io(io::Error),
    /// [`FrameWriter::seal`] was called. Nothing was written.
    Sealed,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::TooLarge { len } => {
                write!(f, "frame: body longer than 1 MiB: {len} bytes")
            }
            FrameError::Truncated { part, got, want } => write!(
                f,
                "frame: stream ended inside a frame: {part}, {got} of {want} bytes"
            ),
            FrameError::Decode(err) => write!(f, "frame: decode: {err}"),
            FrameError::Io(err) => write!(f, "frame: {err}"),
            FrameError::Sealed => f.write_str("frame: writer is sealed"),
        }
    }
}

impl std::error::Error for FrameError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FrameError::Decode(err) => Some(err),
            FrameError::Io(err) => Some(err),
            _ => None,
        }
    }
}

/// Writes one frame per message. Safe to share between threads.
///
/// [`FrameWriter::send`] encodes and writes under one lock, so a frame is
/// whole on the stream before another one starts (E03-D003). The lock is
/// held only for that frame. A send that blocks because the peer does not
/// read holds it until the frame is written.
pub struct FrameWriter<W> {
    state: Mutex<WriterState<W>>,
}

struct WriterState<W> {
    w: W,
    buf: Vec<u8>,
    sealed: bool,
}

impl<W: Write> FrameWriter<W> {
    pub fn new(w: W) -> Self {
        Self {
            state: Mutex::new(WriterState {
                w,
                buf: Vec::new(),
                sealed: false,
            }),
        }
    }

    /// Encodes `m` and writes it as one frame.
    ///
    /// A body longer than [`MAX_LEN`] is [`FrameError::TooLarge`] and
    /// nothing is written. An error from the writer is [`FrameError::Io`];
    /// the stream may then hold part of a frame.
    pub fn send<M: Message>(&self, m: &M) -> Result<(), FrameError> {
        let len = m.encoded_len();
        if len > MAX_LEN {
            return Err(FrameError::TooLarge { len });
        }
        let mut state = self.lock();
        if state.sealed {
            return Err(FrameError::Sealed);
        }
        let mut buf = std::mem::take(&mut state.buf);
        buf.clear();
        buf.reserve(HEADER_LEN + len);
        // MAX_LEN fits in u32.
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        m.encode(&mut buf)
            .expect("a Vec has room for any encoded length");
        debug_assert_eq!(buf.len(), HEADER_LEN + len);
        let result = state.w.write_all(&buf);
        if buf.capacity() <= KEEP_BUF {
            state.buf = buf;
        }
        result.map_err(FrameError::Io)
    }

    /// Waits for the frame being written, if any, then flushes the writer.
    ///
    /// Frames go to the writer whole, so on fd 3 this only waits for the
    /// lock. E03-D005 flushes fd 3 before `_exit`.
    pub fn flush(&self) -> Result<(), FrameError> {
        self.lock().w.flush().map_err(FrameError::Io)
    }

    /// [`FrameWriter::flush`], then refuses every later frame with
    /// [`FrameError::Sealed`].
    ///
    /// The exit path calls it before `_exit` so that no frame, Beat
    /// included, starts after the flush and is cut by the exit (E03-D005).
    pub fn seal(&self) -> Result<(), FrameError> {
        let mut state = self.lock();
        state.sealed = true;
        state.w.flush().map_err(FrameError::Io)
    }

    pub fn into_inner(self) -> W {
        self.state
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
            .w
    }

    // Encoding does not panic, and neither does the fd 3 writer, so a
    // poisoned lock comes only from a writer type that panics. The state is
    // still consistent, so it is taken as is.
    fn lock(&self) -> MutexGuard<'_, WriterState<W>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    fn kept_capacity(&self) -> usize {
        self.lock().buf.capacity()
    }
}

/// Reads frames one at a time. One owner per stream.
///
/// It makes at least two reads per frame, one for the header and one for the
/// body. Wrap the reader in a `BufReader` to make fewer. Reads interrupted by
/// a signal are retried.
pub struct FrameReader<R> {
    r: R,
    buf: Vec<u8>,
}

impl<R: Read> FrameReader<R> {
    pub fn new(r: R) -> Self {
        Self { r, buf: Vec::new() }
    }

    /// Reads the next frame and returns its body, which stays valid until
    /// the next call. A body can be empty: an empty message encodes to zero
    /// bytes.
    ///
    /// - `Ok(None)`: the stream ended between frames, which is how a peer
    ///   that closed its end looks (§5.1).
    /// - [`FrameError::Truncated`]: the stream ended inside a frame.
    /// - [`FrameError::TooLarge`]: the header declared more than
    ///   [`MAX_LEN`]; nothing of the body was read.
    /// - [`FrameError::Io`]: the reader failed.
    pub fn next_frame(&mut self) -> Result<Option<&[u8]>, FrameError> {
        self.drop_big_buffer();
        let mut header = [0u8; HEADER_LEN];
        let got = read_full(&mut self.r, &mut header)?;
        if got == 0 {
            return Ok(None);
        }
        if got < HEADER_LEN {
            return Err(FrameError::Truncated {
                part: Part::Header,
                got,
                want: HEADER_LEN,
            });
        }
        let len = u32::from_be_bytes(header) as usize;
        if len > MAX_LEN {
            return Err(FrameError::TooLarge { len });
        }
        self.buf.clear();
        self.buf.resize(len, 0);
        let got = read_full(&mut self.r, &mut self.buf)?;
        if got < len {
            return Err(FrameError::Truncated {
                part: Part::Body,
                got,
                want: len,
            });
        }
        Ok(Some(&self.buf))
    }

    /// Reads the next frame and decodes it as `M`. The errors are
    /// [`FrameReader::next_frame`]'s, plus [`FrameError::Decode`] when the
    /// body is not a valid `M`; that frame was read whole, so the next call
    /// can go on. Unknown fields are skipped (PROTOCOL §9.3).
    pub fn read_message<M: Message + Default>(&mut self) -> Result<Option<M>, FrameError> {
        let decoded = match self.next_frame()? {
            None => return Ok(None),
            Some(body) => M::decode(body),
        };
        self.drop_big_buffer();
        decoded.map(Some).map_err(FrameError::Decode)
    }

    pub fn into_inner(self) -> R {
        self.r
    }

    fn drop_big_buffer(&mut self) {
        if self.buf.capacity() > KEEP_BUF {
            self.buf = Vec::new();
        }
    }

    #[cfg(test)]
    fn kept_capacity(&self) -> usize {
        self.buf.capacity()
    }
}

/// Reads until `buf` is full or the stream ends, and returns how much was
/// read. Only an error from the reader is an error.
fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<usize, FrameError> {
    let mut got = 0;
    while got < buf.len() {
        match r.read(&mut buf[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(FrameError::Io(err)),
        }
    }
    Ok(got)
}

#[cfg(test)]
mod tests;
