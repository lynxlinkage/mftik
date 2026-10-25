//! Beat contents, schedule, stop, and write failures.
//!
//! These run the thread with a short period. The one-second check against a
//! blocked dispatcher, over a socketpair, is in `tests/socketpair.rs`.

use std::io::{self, Write};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use mftik_proto::wire::mftiko::worker::v1::{Beat, Report, WorkerUp, worker_up};
use prost::Message;

use super::{BEAT_EVERY, BeatEnd, BeatThread, next_due};
use crate::ctl::CtlFd;
use crate::frame::{FrameError, FrameReader, FrameWriter, HEADER_LEN};

const SHORT: Duration = Duration::from_millis(20);
const WAIT: Duration = Duration::from_secs(5);

/// Hands each write, which is one whole frame, to the test with its time.
struct Recorder(mpsc::Sender<(Instant, Vec<u8>)>);

impl Write for Recorder {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _ = self.0.send((Instant::now(), buf.to_vec()));
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn beat_of(frame: &[u8]) -> Beat {
    let len = u32::from_be_bytes(frame[..HEADER_LEN].try_into().expect("header")) as usize;
    assert_eq!(frame.len(), HEADER_LEN + len, "one write is one frame");
    match WorkerUp::decode(&frame[HEADER_LEN..])
        .expect("WorkerUp")
        .msg
    {
        Some(worker_up::Msg::Beat(beat)) => beat,
        other => panic!("not a Beat: {other:?}"),
    }
}

#[test]
fn the_period_is_one_second() {
    assert_eq!(BEAT_EVERY, Duration::from_secs(1));
}

#[test]
fn beats_start_one_period_in_and_count_from_one() {
    let (tx, rx) = mpsc::channel();
    let start = Instant::now();
    let beat =
        BeatThread::spawn_every(Arc::new(FrameWriter::new(Recorder(tx))), SHORT).expect("spawn");
    let mut last = start;
    for seq in 1..=5 {
        let (at, frame) = rx.recv_timeout(WAIT).expect("a Beat");
        assert_eq!(
            beat_of(&frame),
            Beat {
                seq,
                progress: Vec::new()
            }
        );
        assert!(at >= last, "in order");
        last = at;
        if seq == 1 {
            assert!(at - start >= SHORT, "first Beat after {:?}", at - start);
        }
    }
    beat.stop();
    assert!(matches!(beat.join(), BeatEnd::Stopped));
}

#[test]
fn stop_wakes_a_thread_that_is_waiting() {
    let (tx, rx) = mpsc::channel();
    let beat = BeatThread::spawn_every(
        Arc::new(FrameWriter::new(Recorder(tx))),
        Duration::from_secs(3600),
    )
    .expect("spawn");
    let asked = Instant::now();
    beat.stop();
    assert!(matches!(beat.join(), BeatEnd::Stopped));
    assert!(asked.elapsed() < WAIT);
    assert!(rx.try_recv().is_err(), "no Beat before the first period");
}

struct Failing;

impl Write for Failing {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::BrokenPipe.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_failed_write_ends_the_thread() {
    let beat = BeatThread::spawn_every(Arc::new(FrameWriter::new(Failing)), SHORT).expect("spawn");
    match beat.join() {
        BeatEnd::Write(FrameError::Io(err)) => assert_eq!(err.kind(), io::ErrorKind::BrokenPipe),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_sealed_writer_ends_the_thread() {
    let (tx, rx) = mpsc::channel();
    let out = Arc::new(FrameWriter::new(Recorder(tx)));
    let beat = BeatThread::spawn_every(out.clone(), SHORT).expect("spawn");
    let (_, first) = rx.recv_timeout(WAIT).expect("a Beat");
    assert_eq!(beat_of(&first).seq, 1);
    out.seal().expect("seal");
    assert!(matches!(beat.join(), BeatEnd::Write(FrameError::Sealed)));
}

/// Writer threads and the Beat thread share one fd 3; every frame arrives
/// whole and each sender's frames stay in order (E03-D003).
#[test]
fn beats_and_large_frames_from_many_threads_stay_whole() {
    const WRITERS: u32 = 4;
    const EACH: u32 = 60;
    let (shim, worker) = UnixStream::pair().expect("socketpair");
    let out = Arc::new(FrameWriter::new(CtlFd::from(OwnedFd::from(worker))));
    let beat = BeatThread::spawn_every(out.clone(), Duration::from_millis(2)).expect("spawn");

    let payload = |id: u32, seq: u32| {
        // Every 20th frame is larger than the socket buffer.
        let len = if seq.is_multiple_of(20) {
            300 << 10
        } else {
            64
        };
        let mut p: Vec<u8> = (0..len)
            .map(|i| (i as u32 * 7 + id * 13 + seq) as u8)
            .collect();
        p[..4].copy_from_slice(&id.to_be_bytes());
        p[4..8].copy_from_slice(&seq.to_be_bytes());
        p
    };
    let writers: Vec<_> = (0..WRITERS)
        .map(|id| {
            let out = out.clone();
            thread::spawn(move || {
                for seq in 0..EACH {
                    let report = Report {
                        contract: "stsctl.v1".into(),
                        payload: payload(id, seq),
                    };
                    out.send(&WorkerUp {
                        msg: Some(worker_up::Msg::Report(report)),
                    })
                    .expect("send");
                }
            })
        })
        .collect();
    let closer = thread::spawn(move || {
        for w in writers {
            w.join().expect("writer");
        }
        beat.stop();
        assert!(matches!(beat.join(), BeatEnd::Stopped));
        drop(out); // the worker's end closes: the shim sees EOF
    });

    let mut frames = FrameReader::new(&shim);
    let mut next = [0u32; WRITERS as usize];
    let mut beats = 0u64;
    while let Some(m) = frames.read_message::<WorkerUp>().expect("read") {
        match m.msg {
            Some(worker_up::Msg::Report(r)) => {
                let id = u32::from_be_bytes(r.payload[..4].try_into().expect("id"));
                let seq = u32::from_be_bytes(r.payload[4..8].try_into().expect("seq"));
                assert_eq!(seq, next[id as usize], "writer {id} order");
                assert_eq!(r.payload, payload(id, seq), "writer {id} frame {seq}");
                next[id as usize] += 1;
            }
            Some(worker_up::Msg::Beat(b)) => {
                beats += 1;
                assert_eq!(b.seq, beats);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    closer.join().expect("closer");
    assert_eq!(next, [EACH; WRITERS as usize]);
    assert!(beats > 0, "no Beat among the frames");
}

#[test]
fn the_next_beat_is_one_period_later_when_on_time() {
    let t0 = Instant::now();
    let every = Duration::from_secs(1);
    let due = t0 + every;
    assert_eq!(next_due(due, due, every), due + every);
    assert_eq!(
        next_due(due, due + Duration::from_millis(999), every),
        due + every
    );
    assert_eq!(next_due(due, due + every, every), due + every);
}

#[test]
fn periods_a_write_ran_past_collapse_into_one_beat_now() {
    let t0 = Instant::now();
    let every = Duration::from_secs(1);
    let due = t0 + every;
    // Written 3.5 periods late: the grid point at 3 periods is due at once.
    let now = due + Duration::from_millis(3500);
    let next = next_due(due, now, every);
    assert_eq!(next, due + 3 * every);
    assert!(next <= now);
    // That one goes out at once; the one after is back on the grid.
    let after = now + Duration::from_millis(1);
    assert_eq!(next_due(next, after, every), due + 4 * every);
}
