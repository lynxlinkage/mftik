//! The strategy event log (PROTOCOL §12, E07-D001–E07-D003).
//!
//! A file under `$MFTIKO_VOLUME_DIR/eventlog` is a header, then frames. A
//! frame is a fixed 40-byte head and the raw bytes it carries. The bytes match
//! mftiko's Go `sdk/eventlog` exactly, so `mftiko-eventlog dump` reads what
//! this writes (E07-D002); the header's `sdk_version` and `sdk_commit` are
//! mftik's own (E07-D003, [`Header::mftik`]).
//!
//! - [`Log`] (in `writer`): the ring and the writer thread, rotation and
//!   retention, as Go `sdk/internal/evlog` (§12.1).
//! - [`Reader`] (in `reader`): reads one file back. Tests check what the
//!   writer wrote with it.
//! - [`Recording`]: a [`Log`] as the dispatcher's [`Recorder`], which writes
//!   each hook's input and `hook_end` (E07-D007). The other recording
//!   points are not here.

mod reader;
mod writer;

#[cfg(test)]
mod scratch;
#[cfg(test)]
mod tests;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use mftik_core::eventlog::Kind;
use mftik_core::eventlog::{Recorder, put_str};
pub use reader::{ReadError, Reader};
#[cfg(test)]
pub(crate) use scratch::Scratch;
pub use writer::{Config, Log, Sink};

use crate::clock::Clock;

/// Opens every file.
pub const MAGIC: &[u8; 8] = b"MFTIKOEL";

/// The frame layout. A reader refuses a version it does not know; a new
/// kind, or more header fields after `sdk_commit`, does not change it.
pub const FORMAT_VERSION: u16 = 1;

/// The directory under the session volume.
pub const DIR: &str = "eventlog";

/// The file name extension: `{incarnation}-{n}.mel`.
pub const EXT: &str = ".mel";

/// A frame's bytes before its body.
pub const FRAME_HEAD: usize = 40;

/// The largest value of a frame's `len` field. A larger one is damage.
pub const MAX_FRAME: usize = 64 << 20;

/// Each header string is cut to this many bytes.
pub const MAX_HEADER_STRING: usize = 1024;

/// The header up to `session_id`.
const HEADER_FIXED: usize = 28;

/// The dispatcher's [`Recorder`]: frames go to the [`Log`], and their times
/// are the session's [`Clock`], which the prehook and the hook budget read
/// too (E03-D004).
pub struct Recording {
    log: Arc<Log>,
    clock: Clock,
}

impl Recording {
    pub fn new(log: Arc<Log>, clock: Clock) -> Self {
        Self { log, clock }
    }
}

impl Recorder for Recording {
    fn now_ns(&self) -> i64 {
        self.clock.now_ns()
    }

    fn append(
        &self,
        kind: Kind,
        flags: u16,
        recv_ns: i64,
        t_ns: i64,
        r#ref: u64,
        a: &[u8],
        b: &[u8],
    ) -> u64 {
        self.log.append(kind, flags, recv_ns, t_ns, r#ref, a, b)
    }
}

/// The header that opens each file (§12.2). `n` counts the files of one
/// incarnation from 1.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub format_version: u16,
    pub incarnation: u32,
    pub n: u32,
    pub created_ns: i64,
    pub session_id: String,
    pub sdk_version: String,
    pub sdk_commit: String,
}

impl Header {
    /// A header for `session_id`'s `incarnation` whose `sdk_version` and
    /// `sdk_commit` are this mftik build's (E07-D003). The writer sets `n`
    /// and `created_ns` for each file.
    pub fn mftik(incarnation: u32, session_id: &str) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            incarnation,
            n: 0,
            created_ns: 0,
            session_id: session_id.to_owned(),
            sdk_version: crate::RELEASE.to_owned(),
            sdk_commit: env!("VERGEN_GIT_SHA").to_owned(),
        }
    }

    /// Appends the encoded header to `dst`. A zero `format_version` is
    /// written as [`FORMAT_VERSION`]; each string is cut to
    /// [`MAX_HEADER_STRING`] bytes.
    pub fn encode(&self, dst: &mut Vec<u8>) {
        let version = if self.format_version == 0 {
            FORMAT_VERSION
        } else {
            self.format_version
        };
        let start = dst.len();
        dst.extend_from_slice(MAGIC);
        dst.extend_from_slice(&version.to_le_bytes());
        dst.extend_from_slice(&[0, 0]);
        dst.extend_from_slice(&self.incarnation.to_le_bytes());
        dst.extend_from_slice(&self.n.to_le_bytes());
        dst.extend_from_slice(&self.created_ns.to_le_bytes());
        for text in [&self.session_id, &self.sdk_version, &self.sdk_commit] {
            let bytes = text.as_bytes();
            put_str(dst, &bytes[..bytes.len().min(MAX_HEADER_STRING)]);
        }
        let len = u16::try_from(dst.len() - start).expect("a header is under 64 KiB");
        dst[start + 10..start + 12].copy_from_slice(&len.to_le_bytes());
    }
}

/// One frame (§12.3). `seq` is `rec_seq`: it counts every frame of one
/// incarnation from 1, dropped frames included, so a gap is a missing frame.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Record {
    pub kind: Kind,
    pub flags: u16,
    pub seq: u64,
    pub recv_ns: i64,
    pub t_ns: i64,
    pub r#ref: u64,
    pub body: Vec<u8>,
}

impl Record {
    /// Appends the encoded frame to `dst`.
    pub fn encode(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(&frame_head(
            self.kind,
            self.flags,
            self.seq,
            self.recv_ns,
            self.t_ns,
            self.r#ref,
            self.body.len(),
        ));
        dst.extend_from_slice(&self.body);
    }
}

/// The [`FRAME_HEAD`] bytes in front of a body of `body_len` bytes.
/// `body_len` is at most [`MAX_FRAME`] − 36; the writer drops a larger frame
/// before it gets here.
pub fn frame_head(
    kind: Kind,
    flags: u16,
    seq: u64,
    recv_ns: i64,
    t_ns: i64,
    r#ref: u64,
    body_len: usize,
) -> [u8; FRAME_HEAD] {
    let len = (FRAME_HEAD - 4 + body_len) as u32;
    let mut b = [0u8; FRAME_HEAD];
    b[0..4].copy_from_slice(&len.to_le_bytes());
    b[4..6].copy_from_slice(&kind.0.to_le_bytes());
    b[6..8].copy_from_slice(&flags.to_le_bytes());
    b[8..16].copy_from_slice(&seq.to_le_bytes());
    b[16..24].copy_from_slice(&recv_ns.to_le_bytes());
    b[24..32].copy_from_slice(&t_ns.to_le_bytes());
    b[32..40].copy_from_slice(&r#ref.to_le_bytes());
    b
}

/// The body of an `end` frame: how many frames were dropped before it.
pub fn end_body(dropped: u64) -> [u8; 8] {
    dropped.to_le_bytes()
}

/// One log file in a directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct File {
    pub path: PathBuf,
    pub incarnation: u32,
    pub n: u32,
}

/// The name of `incarnation`'s `n`-th file.
pub fn name(incarnation: u32, n: u32) -> String {
    format!("{incarnation}-{n}{EXT}")
}

/// Reads a name [`name`] wrote; any other spelling is not a log file.
pub fn parse_name(file: &str) -> Option<(u32, u32)> {
    let stem = file.strip_suffix(EXT)?;
    let (a, b) = stem.split_once('-')?;
    let incarnation = a.parse().ok()?;
    let n = b.parse().ok()?;
    (name(incarnation, n) == file).then_some((incarnation, n))
}

/// The log files in `dir`, oldest first: by incarnation, then by n. Other
/// names are skipped.
pub fn files(dir: &Path) -> io::Result<Vec<File>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let Some((incarnation, n)) = entry.file_name().to_str().and_then(parse_name) else {
            continue;
        };
        out.push(File {
            path: entry.path(),
            incarnation,
            n,
        });
    }
    out.sort_by_key(|f| (f.incarnation, f.n));
    Ok(out)
}

fn cut_str(b: &[u8]) -> Option<(&[u8], &[u8])> {
    let len = usize::from(u16::from_le_bytes(b.get(..2)?.try_into().ok()?));
    let text = b.get(2..2 + len)?;
    Some((text, &b[2 + len..]))
}
