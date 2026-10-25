//! Frame encoding, the end of a stream, size limits, and writer locking.
//!
//! In-memory readers and writers only; the socketpair tests are in
//! `tests/socketpair.rs`.

use std::io::{self, Read, Write};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use mftik_proto::wire::mftiko::worker::v1::{Beat, Report, Stage, WorkerUp, worker_up};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::Index;
use prost::Message;

use super::{FrameError, FrameReader, FrameWriter, HEADER_LEN, KEEP_BUF, MAX_LEN, Part};

fn up(msg: worker_up::Msg) -> WorkerUp {
    WorkerUp { msg: Some(msg) }
}

fn pattern(seed: usize, n: usize) -> Vec<u8> {
    (0..n).map(|i| (i * 7 + seed * 13) as u8).collect()
}

/// A `Report` whose encoding is exactly `size` bytes: 0, or 3 and more.
fn report_of_size(size: usize) -> Report {
    (0..=size)
        .rev()
        .take(8)
        .map(|p| Report {
            contract: String::new(),
            payload: pattern(size, p),
        })
        .find(|m| m.encoded_len() == size)
        .unwrap_or_else(|| panic!("no Report encodes to {size} bytes"))
}

fn raw_frame(body: &[u8]) -> Vec<u8> {
    let mut out = (body.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(body);
    out
}

fn encode_all<M: Message>(messages: &[M]) -> Vec<u8> {
    let w = FrameWriter::new(Vec::new());
    for m in messages {
        w.send(m).expect("send");
    }
    w.into_inner()
}

struct Counting<R> {
    r: R,
    n: usize,
}

impl<R: Read> Read for Counting<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.r.read(buf)?;
        self.n += n;
        Ok(n)
    }
}

/// Gives `before`, then fails.
struct FailingReader {
    before: Vec<u8>,
}

impl Read for FailingReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.before.is_empty() {
            return Err(io::Error::other("broken"));
        }
        let n = buf.len().min(self.before.len());
        buf[..n].copy_from_slice(&self.before[..n]);
        self.before.drain(..n);
        Ok(n)
    }
}

struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("broken"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// One byte per read, with an `Interrupted` before each.
struct Interrupting<'a> {
    data: &'a [u8],
    interrupt: bool,
}

impl Read for Interrupting<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.interrupt = !self.interrupt;
        if self.interrupt {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.data.is_empty() || buf.is_empty() {
            return Ok(0);
        }
        buf[0] = self.data[0];
        self.data = &self.data[1..];
        Ok(1)
    }
}

#[test]
fn a_frame_is_a_big_endian_length_then_the_body() {
    let beat = up(worker_up::Msg::Beat(Beat {
        seq: 1,
        progress: Vec::new(),
    }));
    // Length 4, then WorkerUp field 4 (beat), length 2, Beat field 1 = 1.
    assert_eq!(encode_all(&[beat]), [0, 0, 0, 4, 0x22, 0x02, 0x08, 0x01]);
    assert_eq!(encode_all(&[WorkerUp::default()]), [0, 0, 0, 0]);
}

#[test]
fn sizes_up_to_the_limit_round_trip() {
    let sizes = [0, 3, 100, KEEP_BUF + 1, MAX_LEN - 1, MAX_LEN];
    let sent: Vec<Report> = sizes.iter().map(|&n| report_of_size(n)).collect();
    let bytes = encode_all(&sent);
    let mut r = FrameReader::new(bytes.as_slice());
    for (n, want) in sizes.iter().zip(&sent) {
        let got: Report = r.read_message().expect("read").expect("a frame");
        assert_eq!(&got, want, "frame of {n} bytes");
    }
    assert!(r.next_frame().expect("end").is_none());
}

#[test]
fn a_body_over_the_limit_is_not_written() {
    let w = FrameWriter::new(Vec::new());
    let err = w.send(&report_of_size(MAX_LEN + 1)).unwrap_err();
    assert!(matches!(err, FrameError::TooLarge { len } if len == MAX_LEN + 1));
    assert!(w.into_inner().is_empty());
}

#[test]
fn an_oversized_header_is_refused_before_the_body_is_read() {
    for len in [MAX_LEN as u32 + 1, u32::MAX] {
        let mut stream = len.to_be_bytes().to_vec();
        stream.extend(vec![b'x'; 4096]);
        let mut r = FrameReader::new(Counting {
            r: stream.as_slice(),
            n: 0,
        });
        let err = r.next_frame().unwrap_err();
        assert!(matches!(err, FrameError::TooLarge { len: got } if got == len as usize));
        assert_eq!(r.into_inner().n, HEADER_LEN, "header {len}");
    }
}

#[test]
fn an_empty_body_is_a_frame() {
    let mut r = FrameReader::new([0u8, 0, 0, 0].as_slice());
    assert_eq!(r.next_frame().expect("read"), Some(&[][..]));
    assert!(r.next_frame().expect("end").is_none());
}

#[test]
fn a_body_that_does_not_decode_keeps_the_stream_in_step() {
    let next = up(worker_up::Msg::Stage(Stage {
        name: "running".into(),
    }));
    let mut stream = raw_frame(&[0xff, 0xff]); // a varint that never ends
    stream.extend(encode_all(std::slice::from_ref(&next)));
    let mut r = FrameReader::new(stream.as_slice());
    assert!(matches!(
        r.read_message::<WorkerUp>(),
        Err(FrameError::Decode(_))
    ));
    assert_eq!(r.read_message::<WorkerUp>().expect("read"), Some(next));
}

#[test]
fn reader_errors_are_not_ends_of_stream() {
    for before in [vec![], vec![0, 0], vec![0, 0, 0, 9, 1]] {
        let err = FrameReader::new(FailingReader {
            before: before.clone(),
        })
        .next_frame()
        .unwrap_err();
        assert!(
            matches!(&err, FrameError::Io(e) if e.to_string() == "broken"),
            "after {before:?}: {err}"
        );
    }
}

#[test]
fn writer_errors_are_returned() {
    let err = FrameWriter::new(FailingWriter)
        .send(&WorkerUp::default())
        .unwrap_err();
    assert!(matches!(&err, FrameError::Io(e) if e.to_string() == "broken"));
}

#[test]
fn interrupted_reads_are_retried() {
    let sent = [report_of_size(10), report_of_size(0), report_of_size(300)];
    let bytes = encode_all(&sent);
    let mut r = FrameReader::new(Interrupting {
        data: &bytes,
        interrupt: false,
    });
    for want in &sent {
        assert_eq!(
            r.read_message::<Report>().expect("read").as_ref(),
            Some(want)
        );
    }
    assert!(r.next_frame().expect("end").is_none());
}

#[test]
fn large_buffers_are_not_kept_between_frames() {
    let big = report_of_size(KEEP_BUF + 100);
    let small = report_of_size(100);

    let w = FrameWriter::new(Vec::new());
    w.send(&big).expect("big");
    assert_eq!(w.kept_capacity(), 0);
    w.send(&small).expect("small");
    assert!((1..=KEEP_BUF).contains(&w.kept_capacity()));

    let bytes = w.into_inner();
    let mut r = FrameReader::new(bytes.as_slice());
    r.read_message::<Report>().expect("big").expect("a frame");
    assert_eq!(r.kept_capacity(), 0);
    r.read_message::<Report>().expect("small").expect("a frame");
    assert!((1..=KEEP_BUF).contains(&r.kept_capacity()));
}

#[test]
fn errors_say_what_happened() {
    let truncated = FrameError::Truncated {
        part: Part::Body,
        got: 3,
        want: 10,
    };
    assert_eq!(
        truncated.to_string(),
        "frame: stream ended inside a frame: body, 3 of 10 bytes"
    );
    assert_eq!(
        FrameError::TooLarge { len: MAX_LEN + 1 }.to_string(),
        "frame: body longer than 1 MiB: 1048577 bytes"
    );
}

/// Blocks inside its first write until released.
struct Gate {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    first: bool,
    out: Vec<u8>,
    flushes: usize,
}

impl Write for Gate {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.first {
            self.first = false;
            self.entered.send(()).expect("test is waiting");
            self.release.recv().expect("test releases");
        }
        self.out.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes += 1;
        Ok(())
    }
}

#[test]
fn seal_waits_for_the_frame_in_progress_then_refuses_frames() {
    let (entered_tx, entered) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let w = Arc::new(FrameWriter::new(Gate {
        entered: entered_tx,
        release: release_rx,
        first: true,
        out: Vec::new(),
        flushes: 0,
    }));
    let report = report_of_size(1000);

    let sender = {
        let (w, report) = (w.clone(), report.clone());
        thread::spawn(move || w.send(&report))
    };
    entered.recv().expect("sender is inside its write");

    let (sealed_tx, sealed) = mpsc::channel();
    let sealer = {
        let w = w.clone();
        thread::spawn(move || {
            let result = w.seal();
            sealed_tx.send(()).expect("test is waiting");
            result
        })
    };
    assert!(
        sealed.recv_timeout(Duration::from_millis(50)).is_err(),
        "seal returned while a frame was half written"
    );
    release.send(()).expect("sender waits");
    sender.join().expect("sender").expect("send");
    sealer.join().expect("sealer").expect("seal");

    assert!(matches!(w.send(&report), Err(FrameError::Sealed)));
    assert!(w.flush().is_ok());

    let gate = Arc::into_inner(w).expect("threads are done").into_inner();
    assert_eq!(gate.out, encode_all(&[report]));
    assert_eq!(gate.flushes, 2);
}

/// Streams of whole and partial frames with small declared lengths, plus
/// headers near the limit and raw bytes.
fn near_frames() -> impl Strategy<Value = Vec<u8>> {
    let chunks = vec((0u32..96, vec(any::<u8>(), 0..96)), 0..6).prop_map(|chunks| {
        let mut out = Vec::new();
        for (len, bytes) in chunks {
            out.extend(len.to_be_bytes());
            out.extend(bytes);
        }
        out
    });
    let header = prop_oneof![
        Just(MAX_LEN as u32),
        Just(MAX_LEN as u32 + 1),
        Just(u32::MAX),
        any::<u32>(),
    ];
    let near_limit = (header, vec(any::<u8>(), 0..16)).prop_map(|(len, tail)| {
        let mut out = len.to_be_bytes().to_vec();
        out.extend(tail);
        out
    });
    prop_oneof![vec(any::<u8>(), 0..256), chunks, near_limit]
}

fn declared(rest: &[u8]) -> usize {
    u32::from_be_bytes(rest[..HEADER_LEN].try_into().expect("4 bytes")) as usize
}

proptest! {
    #[test]
    fn any_payloads_round_trip(payloads in vec(vec(any::<u8>(), 0..4096), 0..6)) {
        let sent: Vec<WorkerUp> = payloads
            .into_iter()
            .map(|payload| up(worker_up::Msg::Report(Report { contract: "stsctl.v1".into(), payload })))
            .collect();
        let bytes = encode_all(&sent);
        let mut r = FrameReader::new(bytes.as_slice());
        for want in &sent {
            let got = r.read_message::<WorkerUp>().expect("read");
            prop_assert_eq!(got.as_ref(), Some(want));
        }
        prop_assert!(r.next_frame().expect("end").is_none());
    }

    #[test]
    fn a_cut_stream_ends_cleanly_only_between_frames(
        bodies in vec(vec(any::<u8>(), 0..48), 1..6),
        cut in any::<Index>(),
    ) {
        let mut stream = Vec::new();
        let mut starts = Vec::new();
        for body in &bodies {
            starts.push(stream.len());
            stream.extend(raw_frame(body));
        }
        let cut = cut.index(stream.len() + 1);
        let whole = starts
            .iter()
            .zip(&bodies)
            .take_while(|(start, body)| *start + HEADER_LEN + body.len() <= cut)
            .count();
        let mut r = FrameReader::new(&stream[..cut]);
        for body in &bodies[..whole] {
            prop_assert_eq!(r.next_frame().expect("whole frame"), Some(&body[..]));
        }
        let start = starts.get(whole).copied().unwrap_or(stream.len());
        let into = cut - start;
        match r.next_frame() {
            Ok(None) => prop_assert_eq!(into, 0),
            Err(FrameError::Truncated { part: Part::Header, got, want }) => {
                prop_assert!(into > 0 && into < HEADER_LEN);
                prop_assert_eq!((got, want), (into, HEADER_LEN));
            }
            Err(FrameError::Truncated { part: Part::Body, got, want }) => {
                prop_assert!(into >= HEADER_LEN);
                prop_assert_eq!((got, want), (into - HEADER_LEN, bodies[whole].len()));
            }
            other => prop_assert!(false, "cut {}: {:?}", cut, other),
        }
    }

    #[test]
    fn any_stream_is_accounted_for(data in near_frames()) {
        let mut r = FrameReader::new(data.as_slice());
        let mut used = 0;
        loop {
            let end = match r.next_frame() {
                Ok(Some(body)) => {
                    prop_assert!(body.len() <= MAX_LEN);
                    let at = used + HEADER_LEN;
                    prop_assert_eq!(body, &data[at..at + body.len()]);
                    used = at + body.len();
                    continue;
                }
                end => end.map(|_| ()),
            };
            let rest = &data[used..];
            match end {
                Ok(()) => prop_assert!(rest.is_empty(), "clean end with {} bytes left", rest.len()),
                Err(FrameError::TooLarge { len }) => {
                    prop_assert!(rest.len() >= HEADER_LEN);
                    prop_assert_eq!(len, declared(rest));
                    prop_assert!(len > MAX_LEN);
                }
                Err(FrameError::Truncated { part: Part::Header, got, .. }) => {
                    prop_assert!(!rest.is_empty() && rest.len() < HEADER_LEN);
                    prop_assert_eq!(got, rest.len());
                }
                Err(FrameError::Truncated { part: Part::Body, got, want }) => {
                    prop_assert!(rest.len() >= HEADER_LEN);
                    prop_assert_eq!(want, declared(rest));
                    prop_assert_eq!(got, rest.len() - HEADER_LEN);
                    prop_assert!(got < want);
                }
                Err(other) => prop_assert!(false, "unexpected {:?}", other),
            }
            break;
        }
    }

    #[test]
    fn a_frame_that_does_not_decode_never_desyncs(garbage in vec(any::<u8>(), 0..64)) {
        let next = up(worker_up::Msg::Stage(Stage { name: "running".into() }));
        let mut stream = raw_frame(&garbage);
        stream.extend(encode_all(std::slice::from_ref(&next)));
        let mut r = FrameReader::new(stream.as_slice());
        let first = r.read_message::<WorkerUp>();
        prop_assert!(matches!(first, Ok(Some(_)) | Err(FrameError::Decode(_))), "{:?}", first);
        prop_assert_eq!(r.read_message::<WorkerUp>().expect("read"), Some(next));
        prop_assert!(r.read_message::<WorkerUp>().expect("end").is_none());
    }
}
