//! When a Report goes out and what it carries. The thread is run against a
//! recorder; the end-to-end Reports over fd 3 are in `tests/ending.rs`.

use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use mftik_core::dispatch::{QueueStat as Queue, Stats};
use mftik_proto::wire::mftiko::common::v1::Version;
use mftik_proto::wire::mftiko::stsctl::v1::{EventLogStat, Report};
use mftik_proto::wire::mftiko::worker::v1::{WorkerUp, worker_up};
use prost::Message;

use super::{QUEUE_MD, QUEUE_TD, REPORT_EVERY, REPORT_POLL, Reporter, Sampler, Source};
use crate::frame::{FrameWriter, HEADER_LEN};

const WAIT: Duration = Duration::from_secs(5);

fn stats(td_age: u32, md_age: u32, slow: u64) -> Stats {
    Stats {
        td: Queue {
            depth: 2,
            dropped: 0,
            max_age_ms: td_age,
        },
        md: Queue {
            depth: 3,
            dropped: 7,
            max_age_ms: md_age,
        },
        hook_blocked_ms: 0,
        hook_slow: slow,
    }
}

fn v(seq: u64) -> Option<Version> {
    Some(Version { epoch: 1, seq })
}

#[test]
fn the_periods() {
    assert_eq!(REPORT_EVERY, Duration::from_secs(5));
    assert!(REPORT_POLL < REPORT_EVERY);
}

#[test]
fn the_first_read_is_sent_with_both_queues() {
    let mut s = Sampler::default();
    let report = s
        .sample(stats(4, 9, 1), v(1), None, Instant::now())
        .expect("first");
    assert_eq!(report.bindings_applied, v(1));
    assert_eq!(report.hook, "");
    assert_eq!(report.hook_slow, 1);
    let names: Vec<_> = report.queues.iter().map(|q| q.name.as_str()).collect();
    assert_eq!(names, [QUEUE_TD, QUEUE_MD]);
    assert_eq!(
        (report.queues[0].depth, report.queues[0].max_age_ms),
        (2, 4)
    );
    assert_eq!(
        (
            report.queues[1].depth,
            report.queues[1].dropped,
            report.queues[1].max_age_ms
        ),
        (3, 7, 9)
    );
}

#[test]
fn an_unchanged_read_waits_until_five_seconds_have_passed() {
    let mut s = Sampler::default();
    let t0 = Instant::now();
    assert!(s.sample(stats(0, 0, 0), v(1), None, t0).is_some());
    for ms in [1000, 2000, 4999] {
        let at = t0 + Duration::from_millis(ms);
        assert!(s.sample(stats(0, 0, 0), v(1), None, at).is_none(), "{ms}");
    }
    let at = t0 + REPORT_EVERY;
    assert_eq!(s.due_at(), Some(at));
    assert!(s.sample(stats(0, 0, 0), v(1), None, at).is_some());
    assert_eq!(s.due_at(), Some(at + REPORT_EVERY));
    // The five seconds count from the last Report sent.
    assert!(
        s.sample(stats(0, 0, 0), v(1), None, at + Duration::from_secs(4))
            .is_none()
    );
}

#[test]
fn any_change_is_sent_at_once() {
    let mut s = Sampler::default();
    let t0 = Instant::now();
    assert!(s.sample(stats(0, 0, 0), v(1), None, t0).is_some());
    let at = t0 + Duration::from_millis(1);
    let r = s.sample(stats(0, 0, 0), v(2), None, at).expect("bindings");
    assert_eq!(r.bindings_applied, v(2));
    let r = s.sample(stats(0, 0, 1), v(2), None, at).expect("hook_slow");
    assert_eq!(r.hook_slow, 1);
    let mut blocked = stats(0, 0, 1);
    blocked.hook_blocked_ms = 250;
    let r = s.sample(blocked, v(2), None, at).expect("hook_blocked_ms");
    assert_eq!(r.hook_blocked_ms, 250);
    let on = Some(EventLogStat::default());
    let r = s.sample(blocked, v(2), on, at).expect("event_log on");
    assert_eq!(r.event_log, on);
    let counted = Some(EventLogStat {
        records: 1,
        dropped: 0,
    });
    let r = s
        .sample(blocked, v(2), counted, at)
        .expect("event_log records");
    assert_eq!(r.event_log, counted);
}

/// The Report fields 1-5 encode to, from the pin before `event_log`
/// (mftiko a9414bc).
const REPORT_BEFORE_EVENT_LOG: &[u8] = &[
    0x0a, 0x04, 0x08, 0x01, 0x10, 0x01, // bindings_applied {epoch 1, seq 1}
    0x18, 0x02, // hook_blocked_ms 2
    0x20, 0x01, // hook_slow 1
    0x2a, 0x08, 0x0a, 0x02, b't', b'd', 0x10, 0x02, 0x20, 0x01, // td: depth 2, max_age 1
    0x2a, 0x0a, 0x0a, 0x02, b'm', b'd', 0x10, 0x03, 0x18, 0x07, 0x20, 0x01, // md: 3, 7, 1
];

#[test]
fn with_the_event_log_off_a_report_is_the_same_bytes() {
    let mut read = stats(1, 1, 1);
    read.hook_blocked_ms = 2;
    let report = Sampler::default()
        .sample(read, v(1), None, Instant::now())
        .expect("first");
    assert_eq!(report.event_log, None);
    assert_eq!(report.encode_to_vec(), REPORT_BEFORE_EVENT_LOG);
}

#[test]
fn with_the_event_log_on_a_report_adds_an_empty_event_log() {
    let mut read = stats(1, 1, 1);
    read.hook_blocked_ms = 2;
    let report = Sampler::default()
        .sample(read, v(1), Some(EventLogStat::default()), Instant::now())
        .expect("first");
    assert_eq!(report.event_log, Some(EventLogStat::default()));
    let mut want = REPORT_BEFORE_EVENT_LOG.to_vec();
    want.extend_from_slice(&[0x32, 0x00]); // event_log {}
    assert_eq!(report.encode_to_vec(), want);
}

#[test]
fn max_age_is_the_largest_since_the_last_report() {
    let mut s = Sampler::default();
    let t0 = Instant::now();
    let sec = |n| t0 + Duration::from_secs(n);
    let ages = |r: &Report| (r.queues[0].max_age_ms, r.queues[1].max_age_ms);
    let r = s.sample(stats(40, 5, 0), v(1), None, t0).expect("first");
    assert_eq!(ages(&r), (40, 5));
    // The same ages again: nothing changed, nothing sent.
    assert!(s.sample(stats(40, 5, 0), v(1), None, sec(1)).is_none());
    // Stats clears max_age on each read. The ages read at 1 s belong to the
    // next Report, so a quiet read keeps them.
    assert!(s.sample(stats(0, 0, 0), v(1), None, sec(2)).is_none());
    let r = s
        .sample(stats(0, 0, 0), v(1), None, sec(5))
        .expect("five seconds");
    assert_eq!(ages(&r), (40, 5));
    // Sent: the next period starts at zero, which is a change.
    let r = s
        .sample(stats(0, 0, 0), v(1), None, sec(6))
        .expect("back to zero");
    assert_eq!(ages(&r), (0, 0));
    // The larger of the reads between two Reports.
    assert!(s.sample(stats(0, 0, 0), v(1), None, sec(7)).is_none());
    let r = s
        .sample(stats(10, 20, 0), v(1), None, sec(8))
        .expect("aged");
    assert_eq!(ages(&r), (10, 20));
}

/// Hands each write, which is one whole frame, to the test.
struct Recorder(mpsc::Sender<Vec<u8>>);

impl Write for Recorder {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _ = self.0.send(buf.to_vec());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn report_of(frame: &[u8]) -> Report {
    match WorkerUp::decode(&frame[HEADER_LEN..])
        .expect("WorkerUp")
        .msg
    {
        Some(worker_up::Msg::Report(report)) => {
            assert_eq!(report.contract, "stsctl.v1");
            Report::decode(report.payload.as_slice()).expect("stsctl Report")
        }
        other => panic!("not a Report: {other:?}"),
    }
}

#[test]
fn the_thread_sends_at_once_wakes_on_bindings_and_stops_at_close() {
    let (tx, rx) = mpsc::channel();
    let seq = Arc::new(AtomicU64::new(1));
    let read = Arc::clone(&seq);
    let reporter = Reporter::spawn(
        Arc::new(FrameWriter::new(Recorder(tx))),
        Source {
            stats: Box::new(|| stats(0, 0, 0)),
            bindings_applied: Box::new(move || v(read.load(Ordering::SeqCst))),
            event_log: Box::new(|| None),
        },
    )
    .expect("spawn");
    let started = Instant::now();
    let first = report_of(&rx.recv_timeout(WAIT).expect("first Report"));
    assert_eq!(first.bindings_applied, v(1));
    assert!(started.elapsed() < REPORT_POLL, "the first is not polled");

    seq.store(2, Ordering::SeqCst);
    let woke = Instant::now();
    reporter.waker().send(()).expect("wake");
    let second = report_of(&rx.recv_timeout(WAIT).expect("woken Report"));
    assert_eq!(second.bindings_applied, v(2));
    assert!(woke.elapsed() < REPORT_POLL, "a wake does not wait a poll");

    reporter.close();
    seq.store(3, Ordering::SeqCst);
    let _ = reporter.waker().send(());
    assert!(
        rx.recv_timeout(REPORT_POLL * 2).is_err(),
        "no Report after close"
    );
}
