//! Reads one event log file back, as Go `sdk/eventlog`'s `Reader`.

use std::fmt;
use std::fs;
use std::io::{self, BufReader, Read};
use std::path::Path;

use super::{
    FORMAT_VERSION, FRAME_HEAD, HEADER_FIXED, Header, Kind, MAGIC, MAX_FRAME, Record, cut_str,
};

/// Why a file could not be read further.
#[derive(Debug)]
pub enum ReadError {
    /// The file does not start with [`MAGIC`] and a whole header.
    Magic,
    /// The header's `format_version` is not one this reader knows.
    Version(u16),
    /// The file ends inside a frame or inside the header: the writer did not
    /// finish it.
    Truncated,
    /// A frame's `len` is under 36 or over [`MAX_FRAME`].
    Frame(u32),
    Io(io::Error),
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Magic => f.write_str("eventlog: not an event log"),
            Self::Version(v) => write!(f, "eventlog: unknown format version {v}"),
            Self::Truncated => f.write_str("eventlog: the file ends inside a frame"),
            Self::Frame(n) => write!(f, "eventlog: bad frame length {n}"),
            Self::Io(err) => write!(f, "eventlog: {err}"),
        }
    }
}

impl std::error::Error for ReadError {}

/// One file's header and frames. [`Reader::next_record`] skips a kind this reader
/// does not know.
pub struct Reader<R> {
    r: R,
    header: Header,
}

impl Reader<BufReader<fs::File>> {
    pub fn open(path: &Path) -> Result<Self, ReadError> {
        let file = fs::File::open(path).map_err(ReadError::Io)?;
        Self::new(BufReader::with_capacity(1 << 16, file))
    }
}

impl<R: Read> Reader<R> {
    /// Reads the header from `r`.
    pub fn new(mut r: R) -> Result<Self, ReadError> {
        let header = read_header(&mut r)?;
        Ok(Self { r, header })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The next frame of a known kind, or `None` at the end of the file on a
    /// frame boundary. A file that ends inside a frame, as a process killed
    /// in the middle of a write leaves it, is [`ReadError::Truncated`].
    pub fn next_record(&mut self) -> Result<Option<Record>, ReadError> {
        loop {
            let mut len = [0u8; 4];
            match fill(&mut self.r, &mut len)? {
                0 => return Ok(None),
                4 => {}
                _ => return Err(ReadError::Truncated),
            }
            let n = u32::from_le_bytes(len);
            if (n as usize) < FRAME_HEAD - 4 || n as usize > MAX_FRAME {
                return Err(ReadError::Frame(n));
            }
            let mut buf = vec![0u8; n as usize];
            if fill(&mut self.r, &mut buf)? != buf.len() {
                return Err(ReadError::Truncated);
            }
            let kind = Kind(u16::from_le_bytes([buf[0], buf[1]]));
            if !kind.known() {
                continue;
            }
            let word = |at: usize| buf[at..at + 8].try_into().expect("8 bytes");
            let record = Record {
                kind,
                flags: u16::from_le_bytes([buf[2], buf[3]]),
                seq: u64::from_le_bytes(word(4)),
                recv_ns: i64::from_le_bytes(word(12)),
                t_ns: i64::from_le_bytes(word(20)),
                r#ref: u64::from_le_bytes(word(28)),
                body: buf[FRAME_HEAD - 4..].to_vec(),
            };
            return Ok(Some(record));
        }
    }

    /// Every remaining frame of a known kind.
    pub fn read_all(&mut self) -> Result<Vec<Record>, ReadError> {
        let mut out = Vec::new();
        while let Some(record) = self.next_record()? {
            out.push(record);
        }
        Ok(out)
    }
}

fn read_header(r: &mut impl Read) -> Result<Header, ReadError> {
    let mut fixed = [0u8; HEADER_FIXED];
    if fill(r, &mut fixed)? != HEADER_FIXED || &fixed[..8] != MAGIC {
        return Err(ReadError::Magic);
    }
    let u32_at = |at: usize| u32::from_le_bytes(fixed[at..at + 4].try_into().expect("4 bytes"));
    let format_version = u16::from_le_bytes([fixed[8], fixed[9]]);
    if format_version != FORMAT_VERSION {
        return Err(ReadError::Version(format_version));
    }
    let total = usize::from(u16::from_le_bytes([fixed[10], fixed[11]]));
    if total < HEADER_FIXED + 6 {
        return Err(ReadError::Magic);
    }
    let mut rest = vec![0u8; total - HEADER_FIXED];
    if fill(r, &mut rest)? != rest.len() {
        return Err(ReadError::Truncated);
    }
    let (session_id, rest) = cut_str(&rest).ok_or(ReadError::Magic)?;
    let (sdk_version, rest) = cut_str(rest).ok_or(ReadError::Magic)?;
    // Fields after sdk_commit are a later header's; header_len skips them.
    let (sdk_commit, _) = cut_str(rest).ok_or(ReadError::Magic)?;
    let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
    Ok(Header {
        format_version,
        incarnation: u32_at(12),
        n: u32_at(16),
        created_ns: i64::from_le_bytes(fixed[20..28].try_into().expect("8 bytes")),
        session_id: text(session_id),
        sdk_version: text(sdk_version),
        sdk_commit: text(sdk_commit),
    })
}

/// Reads until `buf` is full or the input ends; returns the bytes read.
fn fill(r: &mut impl Read, buf: &mut [u8]) -> Result<usize, ReadError> {
    let mut got = 0;
    while got < buf.len() {
        match r.read(&mut buf[got..]) {
            Ok(0) => break,
            Ok(k) => got += k,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(ReadError::Io(err)),
        }
    }
    Ok(got)
}
