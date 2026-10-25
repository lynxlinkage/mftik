//! The C4 transport over a socketpair, the shim's end on one side and the
//! worker's [`CtlFd`] on the other (PROTOCOL §5.1, §9.2).

use std::io::Write;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use mftik_proto::wire::mftiko::worker::v1::{
    Done, Hello, Init, Live, Relay, Stage, Stop, Verb, WorkerDown, WorkerUp, worker_down, worker_up,
};
use mftik_rt::beat::{BeatEnd, BeatThread};
use mftik_rt::ctl::CtlFd;
use mftik_rt::frame::{FrameError, FrameReader, FrameWriter, MAX_LEN, Part};
use mftik_rt::reader::{ReadEvent, spawn_reader};

const WAIT: Duration = Duration::from_secs(5);

fn pair() -> (UnixStream, CtlFd) {
    let (shim, worker) = UnixStream::pair().expect("socketpair");
    (shim, CtlFd::from(OwnedFd::from(worker)))
}

fn read_into_channel(fd: CtlFd) -> (JoinHandle<()>, mpsc::Receiver<ReadEvent>) {
    let (tx, rx) = mpsc::channel();
    let thread = spawn_reader(fd, move |event| {
        let _ = tx.send(event);
    })
    .expect("spawn reader");
    (thread, rx)
}

fn down(msg: worker_down::Msg) -> WorkerDown {
    WorkerDown { msg: Some(msg) }
}

fn up(msg: worker_up::Msg) -> WorkerUp {
    WorkerUp { msg: Some(msg) }
}

fn stage(name: &str) -> WorkerUp {
    up(worker_up::Msg::Stage(Stage { name: name.into() }))
}

fn message(event: ReadEvent) -> WorkerDown {
    match event {
        ReadEvent::Message(m) => m,
        other => panic!("expected a message, got {other:?}"),
    }
}

#[test]
fn worker_down_arrives_in_order_then_eof() {
    let (shim, worker) = pair();
    let (thread, events) = read_into_channel(worker);
    let sent = [
        down(worker_down::Msg::Init(Init {
            contract: "stsctl.v1".into(),
            payload: b"init".to_vec(),
            deadline_ms: 3000,
            live_digest: "d1".into(),
        })),
        down(worker_down::Msg::Live(Live {
            contract: "stsctl.v1".into(),
            payload: b"live".to_vec(),
            digest: "d2".into(),
        })),
        // Larger than the socket buffer, so it is written and read in pieces.
        down(worker_down::Msg::Relay(Relay {
            contract: "stsctl.v1".into(),
            corr_id: b"c".to_vec(),
            payload: vec![0x5a; 300 << 10],
        })),
        down(worker_down::Msg::Stop(Stop {
            reason: "stop".into(),
            grace_ms: 5000,
        })),
    ];
    let shim_out = FrameWriter::new(shim);
    for m in &sent {
        shim_out.send(m).expect("send");
    }
    drop(shim_out);

    for want in &sent {
        assert_eq!(&message(events.recv_timeout(WAIT).expect("event")), want);
    }
    assert!(matches!(events.recv_timeout(WAIT), Ok(ReadEvent::Eof)));
    thread.join().expect("reader thread");
    assert!(events.recv().is_err(), "Eof is the last event");
}

#[test]
fn a_frame_cut_by_the_shim_is_failed() {
    let (mut shim, worker) = pair();
    let (thread, events) = read_into_channel(worker);
    shim.write_all(&[0, 0, 0, 10, 1, 2, 3]).expect("write");
    drop(shim);
    match events.recv_timeout(WAIT).expect("event") {
        ReadEvent::Failed(FrameError::Truncated {
            part: Part::Body,
            got: 3,
            want: 10,
        }) => {}
        other => panic!("{other:?}"),
    }
    thread.join().expect("reader thread");
}

#[test]
fn an_oversized_frame_is_failed() {
    let (mut shim, worker) = pair();
    let (thread, events) = read_into_channel(worker);
    shim.write_all(&(MAX_LEN as u32 + 1).to_be_bytes())
        .expect("write");
    match events.recv_timeout(WAIT).expect("event") {
        ReadEvent::Failed(FrameError::TooLarge { len }) => assert_eq!(len, MAX_LEN + 1),
        other => panic!("{other:?}"),
    }
    // The reader stopped without waiting for the shim to close.
    thread.join().expect("reader thread");
}

#[test]
fn a_frame_that_does_not_decode_ends_reading() {
    let (mut shim, worker) = pair();
    let (thread, events) = read_into_channel(worker);
    shim.write_all(&[0, 0, 0, 2, 0xff, 0xff]).expect("write");
    FrameWriter::new(&shim)
        .send(&down(worker_down::Msg::Stop(Stop::default())))
        .expect("send");
    assert!(matches!(
        events.recv_timeout(WAIT).expect("event"),
        ReadEvent::Failed(FrameError::Decode(_))
    ));
    thread.join().expect("reader thread");
    assert!(
        events.recv().is_err(),
        "nothing after the failure, as Go's c4 reader halts"
    );
}

#[test]
fn unknown_fields_and_messages_are_skipped() {
    let (shim, worker) = pair();
    let (thread, events) = read_into_channel(worker);
    let init = Init {
        contract: "stsctl.v1".into(),
        ..Init::default()
    };
    let known = down(worker_down::Msg::Init(init));
    // Field 99, varint 1, before the known Init.
    let mut with_unknown_field = vec![0x98, 0x06, 0x01];
    with_unknown_field.extend(prost::Message::encode_to_vec(&known));
    // Field 50, bytes "x": a WorkerDown this build does not know.
    let unknown_message = [0x92, 0x03, 0x01, b'x'];
    let mut stream = Vec::new();
    for body in [&with_unknown_field[..], &unknown_message[..]] {
        stream.extend((body.len() as u32).to_be_bytes());
        stream.extend_from_slice(body);
    }
    (&shim).write_all(&stream).expect("write");
    drop(shim);

    assert_eq!(message(events.recv_timeout(WAIT).expect("event")), known);
    assert_eq!(
        message(events.recv_timeout(WAIT).expect("event")),
        WorkerDown { msg: None }
    );
    assert!(matches!(events.recv_timeout(WAIT), Ok(ReadEvent::Eof)));
    thread.join().expect("reader thread");
}

#[test]
fn both_directions_share_one_connection() {
    let (shim, worker) = pair();
    let (thread, events) = read_into_channel(worker.try_clone().expect("dup"));
    let worker_out = FrameWriter::new(worker);
    let shim_out = FrameWriter::new(&shim);
    let mut shim_in = FrameReader::new(&shim);

    let hello = up(worker_up::Msg::Hello(Hello {
        kind: "session".into(),
        release: "test".into(),
        contracts: vec!["worker.v1".into(), "stsctl.v1".into()],
        pid: 42,
    }));
    worker_out.send(&hello).expect("hello");
    assert_eq!(
        shim_in.read_message::<WorkerUp>().expect("read"),
        Some(hello)
    );

    let init = down(worker_down::Msg::Init(Init {
        contract: "stsctl.v1".into(),
        ..Init::default()
    }));
    shim_out.send(&init).expect("init");
    assert_eq!(message(events.recv_timeout(WAIT).expect("event")), init);

    let done = up(worker_up::Msg::Done(Done {
        verb: Verb::Init as i32,
        ok: true,
        error: String::new(),
    }));
    worker_out.send(&done).expect("done");
    assert_eq!(
        shim_in.read_message::<WorkerUp>().expect("read"),
        Some(done)
    );

    // The shim goes away: the worker's reader sees EOF.
    shim.shutdown(std::net::Shutdown::Write).expect("shutdown");
    assert!(matches!(events.recv_timeout(WAIT), Ok(ReadEvent::Eof)));
    thread.join().expect("reader thread");

    // The worker goes away: the shim sees EOF between frames.
    drop(worker_out);
    assert!(shim_in.next_frame().expect("end").is_none());
}

/// PROTOCOL §9.2.3 and the E03-03 acceptance check: while the dispatcher is
/// stuck in a hook for 3 seconds, Beat still goes out once a second.
#[test]
fn beat_goes_out_every_second_while_the_dispatcher_is_blocked() {
    let (shim, worker) = pair();
    let out = Arc::new(FrameWriter::new(worker));

    let (arrived, arrivals) = mpsc::channel();
    let shim_side = thread::spawn(move || {
        let mut frames = FrameReader::new(&shim);
        while let Some(m) = frames.read_message::<WorkerUp>().expect("read") {
            arrived
                .send((Instant::now(), m))
                .expect("test is collecting");
        }
    });

    // Stands for the dispatcher's own state, which the stuck hook holds.
    let session = Arc::new(Mutex::new(()));
    let start = Instant::now();
    let beat = BeatThread::spawn(out.clone()).expect("spawn beat");
    let dispatcher = {
        let (out, session) = (out.clone(), session.clone());
        thread::spawn(move || {
            let _state = session.lock().expect("session");
            out.send(&stage("hook entered")).expect("send");
            thread::sleep(Duration::from_secs(3));
            out.send(&stage("hook returned")).expect("send");
        })
    };
    dispatcher.join().expect("dispatcher");
    beat.stop();
    assert!(matches!(beat.join(), BeatEnd::Stopped));
    drop(out);
    shim_side.join().expect("shim side");

    let frames: Vec<(Instant, WorkerUp)> = arrivals.into_iter().collect();
    let at = |name: &str| {
        frames
            .iter()
            .find(|(_, m)| *m == stage(name))
            .map(|(t, _)| *t)
            .unwrap_or_else(|| panic!("no {name:?}"))
    };
    let (entered, returned) = (at("hook entered"), at("hook returned"));
    let beats: Vec<(Instant, u64)> = frames
        .iter()
        .filter_map(|(t, m)| match &m.msg {
            Some(worker_up::Msg::Beat(b)) => Some((*t, b.seq)),
            _ => None,
        })
        .collect();

    let seqs: Vec<u64> = beats.iter().map(|(_, seq)| *seq).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>(), "seq");
    let during = beats
        .iter()
        .filter(|(t, _)| *t > entered && *t < returned)
        .count();
    assert!(
        (2..=3).contains(&during),
        "{during} Beats during the 3 s hook: {beats:?}"
    );
    let near_one_second =
        |d: Duration| d > Duration::from_millis(500) && d < Duration::from_millis(1500);
    assert!(near_one_second(beats[0].0 - start), "first Beat");
    for pair in beats.windows(2) {
        assert!(near_one_second(pair[1].0 - pair[0].0), "gap in {beats:?}");
    }
}
