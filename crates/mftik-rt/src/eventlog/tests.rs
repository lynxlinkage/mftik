use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use mftik_testkit::TempDir;

use super::writer::{DEFAULT_KEEP, DEFAULT_RING, create_file};
use super::*;

/// `AppendHeader`, `AppendFrame`, and `AppendEnd` of mftiko `sdk/eventlog`
/// at contract/proto/SOURCE, on the same inputs as the tests below.
const GO_HEADER: &str =
    "4d4654494b4f454c01002c000300000002000000e803000000000000020073310500312e322e300300616263";
const GO_LONG_HEADER_START: &str =
    "4d4654494b4f454c010022040100000001000000ffffffffffffffff000461616161616161616161";
const GO_HOOK_END: &str =
    "280000000f0001000300000000000000feffffffffffffff16000000000000000200000000000000626f6f6d";
const GO_END: &str = "2c0000001300000004000000000000000000000000000000e80300000000000000000000000000000500000000000000";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn header() -> Header {
    Header {
        format_version: 0,
        incarnation: 3,
        n: 0,
        created_ns: 0,
        session_id: "s1".to_owned(),
        sdk_version: "1.2.0".to_owned(),
        sdk_commit: "abc".to_owned(),
    }
}

fn read(path: &Path) -> (Header, Vec<Record>) {
    let mut r = Reader::open(path).expect("open");
    let records = r.read_all().expect("frames");
    (r.header().clone(), records)
}

fn fixed_ns() -> i64 {
    1_000
}

fn open(dir: &Path, f: impl FnOnce(&mut Config)) -> Log {
    let mut cfg = Config {
        dir: Some(dir.to_path_buf()),
        header: header(),
        now_ns: Some(fixed_ns),
        ..Config::default()
    };
    f(&mut cfg);
    Log::open(cfg).expect("open")
}

fn post(log: &Log, i: u64, body: &[u8]) -> u64 {
    log.append(Kind::POST, 0, i as i64, i as i64, i, &[], body)
}

// ---------- §12.2 header ----------

#[test]
fn header_bytes_match_go() {
    let mut b = Vec::new();
    Header {
        n: 2,
        created_ns: 1000,
        ..header()
    }
    .encode(&mut b);
    assert_eq!(hex(&b), GO_HEADER);
    assert_eq!(&b[..8], b"MFTIKOEL");
    assert_eq!(u16::from_le_bytes([b[8], b[9]]), 1);
    assert_eq!(usize::from(u16::from_le_bytes([b[10], b[11]])), b.len());
}

#[test]
fn header_strings_are_cut_at_1024_bytes() {
    let mut b = Vec::new();
    Header {
        incarnation: 1,
        n: 1,
        created_ns: -1,
        session_id: "a".repeat(1030),
        ..Header::default()
    }
    .encode(&mut b);
    assert_eq!(b.len(), 1058);
    assert_eq!(hex(&b[..40]), GO_LONG_HEADER_START);
    assert_eq!(hex(&b[b.len() - 8..]), "6161616100000000");
    let r = Reader::new(&b[..]).expect("header");
    assert_eq!(r.header().session_id, "a".repeat(1024));
    assert_eq!(r.header().created_ns, -1);
}

#[test]
fn header_round_trips_and_skips_later_fields() {
    let h = Header {
        format_version: FORMAT_VERSION,
        n: 2,
        created_ns: 1000,
        ..header()
    };
    let mut b = Vec::new();
    h.encode(&mut b);
    assert_eq!(Reader::new(&b[..]).expect("header").header(), &h);

    // A later header with one more field: header_len covers it.
    b.extend_from_slice(&[7, 7]);
    let len = b.len() as u16;
    b[10..12].copy_from_slice(&len.to_le_bytes());
    Record {
        kind: Kind::POST,
        seq: 1,
        r#ref: 1,
        ..Record::default()
    }
    .encode(&mut b);
    let mut r = Reader::new(&b[..]).expect("header");
    assert_eq!(r.header(), &h);
    let records = r.read_all().expect("frames");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].seq, 1);
}

#[test]
fn bad_headers_are_refused() {
    let r = Reader::new(&b"NOTALOG_and_more_bytes_here_padding"[..]);
    assert!(matches!(r, Err(ReadError::Magic)));
    assert!(matches!(Reader::new(&b"MFTIK"[..]), Err(ReadError::Magic)));

    let mut b = Vec::new();
    Header {
        format_version: 2,
        ..Header::default()
    }
    .encode(&mut b);
    assert!(matches!(Reader::new(&b[..]), Err(ReadError::Version(2))));

    let mut b = Vec::new();
    header().encode(&mut b);
    assert!(matches!(
        Reader::new(&b[..b.len() - 1]),
        Err(ReadError::Truncated)
    ));
}

#[test]
fn mftik_header_carries_this_build() {
    let h = Header::mftik(4, "s1");
    assert_eq!(h.sdk_version, crate::RELEASE);
    assert_eq!(h.sdk_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(h.sdk_commit, env!("VERGEN_GIT_SHA"));
    assert_eq!((h.incarnation, h.session_id.as_str()), (4, "s1"));
    assert_eq!(h.format_version, FORMAT_VERSION);
}

// ---------- §12.3 frames ----------

#[test]
fn frame_bytes_match_go() {
    let mut b = Vec::new();
    Record {
        kind: Kind::HOOK_END,
        flags: 1,
        seq: 3,
        recv_ns: -2,
        t_ns: 22,
        r#ref: 2,
        body: b"boom".to_vec(),
    }
    .encode(&mut b);
    assert_eq!(hex(&b), GO_HOOK_END);
    assert_eq!(u32::from_le_bytes(b[..4].try_into().unwrap()), 36 + 4);

    let mut b = Vec::new();
    Record {
        kind: Kind::END,
        seq: 4,
        t_ns: 1000,
        body: end_body(5).to_vec(),
        ..Record::default()
    }
    .encode(&mut b);
    assert_eq!(hex(&b), GO_END);
}

#[test]
fn frames_round_trip() {
    let mut b = Vec::new();
    header().encode(&mut b);
    let records = vec![
        Record {
            kind: Kind::INIT,
            seq: 1,
            recv_ns: 10,
            t_ns: 11,
            r#ref: 7,
            body: b"init".to_vec(),
            ..Record::default()
        },
        Record {
            kind: Kind::MD_FEED,
            seq: 2,
            recv_ns: 20,
            t_ns: 21,
            body: b"md".to_vec(),
            ..Record::default()
        },
        Record {
            kind: Kind::HOOK_END,
            flags: 1,
            seq: 3,
            t_ns: 22,
            r#ref: 2,
            body: b"boom".to_vec(),
            ..Record::default()
        },
        Record {
            kind: Kind::TIMER,
            seq: 4,
            recv_ns: 30,
            t_ns: 31,
            r#ref: 1,
            ..Record::default()
        },
    ];
    for r in &records {
        r.encode(&mut b);
    }
    let mut r = Reader::new(&b[..]).expect("header");
    assert_eq!(r.read_all().expect("frames"), records);
}

#[test]
fn unknown_kind_is_skipped() {
    let mut b = Vec::new();
    header().encode(&mut b);
    for (kind, seq) in [(Kind::POST, 1), (Kind(99), 2), (Kind::POST, 3)] {
        Record {
            kind,
            seq,
            body: b"aa".to_vec(),
            ..Record::default()
        }
        .encode(&mut b);
    }
    let seqs: Vec<u64> = Reader::new(&b[..])
        .expect("header")
        .read_all()
        .expect("frames")
        .iter()
        .map(|r| r.seq)
        .collect();
    assert_eq!(seqs, [1, 3]);
    assert_eq!(Kind(99).name(), None);
    assert_eq!(Kind::ORDER_CALL.name(), Some("order_call"));
    assert_eq!(Kind::END.name(), Some("end"));
    assert_eq!(Kind(0).name(), None);
}

#[test]
fn a_file_cut_inside_a_frame_keeps_the_frames_before_it() {
    let mut b = Vec::new();
    header().encode(&mut b);
    Record {
        kind: Kind::POST,
        seq: 1,
        r#ref: 1,
        ..Record::default()
    }
    .encode(&mut b);
    let mut full = b.clone();
    Record {
        kind: Kind::POST,
        seq: 2,
        r#ref: 2,
        body: b"abcd".to_vec(),
        ..Record::default()
    }
    .encode(&mut full);
    for cut in b.len() + 1..full.len() {
        let mut r = Reader::new(&full[..cut]).expect("header");
        assert_eq!(r.next_record().expect("first").expect("a frame").seq, 1);
        assert!(
            matches!(r.next_record(), Err(ReadError::Truncated)),
            "cut {cut}"
        );
    }
}

#[test]
fn impossible_lengths_are_damage() {
    for len in [0u32, 35, (MAX_FRAME + 1) as u32] {
        let mut b = Vec::new();
        header().encode(&mut b);
        b.extend_from_slice(&len.to_le_bytes());
        b.extend_from_slice(&[0; 64]);
        let mut r = Reader::new(&b[..]).expect("header");
        assert!(matches!(r.next_record(), Err(ReadError::Frame(n)) if n == len));
    }
}

#[test]
fn names_parse_only_as_written() {
    assert_eq!(name(7, 3), "7-3.mel");
    assert_eq!(parse_name("7-3.mel"), Some((7, 3)));
    for bad in [
        "02-3.mel", "2-x.mel", "2-3.txt", "23.mel", "-1-1.mel", "+1-1.mel",
    ] {
        assert_eq!(parse_name(bad), None, "{bad}");
    }
    let dir = TempDir::new("evnames");
    for f in [
        "10-1.mel",
        "2-10.mel",
        "2-2.mel",
        "2-1.mel",
        "notes.txt",
        "2-x.mel",
        "02-3.mel",
    ] {
        fs::write(dir.0.join(f), b"").unwrap();
    }
    let got: Vec<String> = files(&dir.0)
        .unwrap()
        .iter()
        .map(|f| f.path.file_name().unwrap().to_str().unwrap().to_owned())
        .collect();
    assert_eq!(got, ["2-1.mel", "2-2.mel", "2-10.mel", "10-1.mel"]);
}

// ---------- §12.1 writer ----------

#[test]
fn defaults_are_the_protocol_numbers() {
    assert_eq!(DEFAULT_RING, 8 << 20);
    assert_eq!(DEFAULT_KEEP, 256 << 20);
    assert_eq!(writer::DEFAULT_FLUSH, Duration::from_millis(100));
}

#[test]
fn writes_and_reads_back() {
    let vol = TempDir::new("evwrite");
    let dir = vol.0.join("eventlog");
    let log = open(&dir, |_| {});
    assert_eq!(log.append(Kind::INIT, 0, 10, 11, 7, b"ini", b"t"), 1);
    assert_eq!(log.append(Kind::MD_FEED, 0, 20, 21, 0, &[], b"md"), 2);
    assert_eq!(log.append(Kind::HOOK_END, 1, 0, 22, 2, &[], b"boom"), 3);
    log.close();

    let list = files(&dir).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].path, dir.join("3-1.mel"));
    let (h, records) = read(&list[0].path);
    assert_eq!(
        h,
        Header {
            format_version: 1,
            n: 1,
            created_ns: 1000,
            ..header()
        }
    );
    let got: Vec<(Kind, &[u8], u64)> = records
        .iter()
        .map(|r| (r.kind, r.body.as_slice(), r.seq))
        .collect();
    assert_eq!(
        got,
        [
            (Kind::INIT, &b"init"[..], 1),
            (Kind::MD_FEED, &b"md"[..], 2),
            (Kind::HOOK_END, &b"boom"[..], 3),
            (Kind::END, &[0u8; 8][..], 4),
        ]
    );
    assert_eq!(
        (records[0].recv_ns, records[0].t_ns, records[0].r#ref),
        (10, 11, 7)
    );
    assert_eq!(records[2].flags, 1);
    assert_eq!((records[3].recv_ns, records[3].t_ns), (0, 1000));
    assert_eq!(log.stats(), (4, 0));

    let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&dir), 0o750);
    assert_eq!(mode(&list[0].path), 0o640);
}

#[test]
fn nothing_touches_the_disk_before_a_frame_is_due() {
    let vol = TempDir::new("evlazy");
    let dir = vol.0.join("eventlog");
    let log = open(&dir, |_| {});
    log.flush();
    assert!(!dir.exists());
    log.close();
    assert_eq!(files(&dir).unwrap().len(), 1, "close writes end");
}

/// Files rotate by size, and the oldest go so the directory stays within keep.
#[test]
fn rotation_and_retention() {
    let vol = TempDir::new("evrotate");
    const KEEP: u64 = 256 << 10;
    let log = open(&vol.0, |c| c.keep_bytes = KEEP);
    let body = vec![b'x'; 10 << 10];
    const N: u64 = 100;
    for i in 0..N {
        post(&log, i + 1, &body);
        if i % 10 == 9 {
            log.flush();
        }
    }
    log.close();

    let list = files(&vol.0).unwrap();
    assert!(list.len() >= 2, "{list:?}");
    assert_ne!(list[0].n, 1, "the oldest file was kept");
    let mut total = 0;
    let mut seqs = Vec::new();
    for (i, f) in list.iter().enumerate() {
        let size = fs::metadata(&f.path).unwrap().len();
        assert!(size <= 64 << 10, "{:?} is {size} bytes", f.path);
        total += size;
        if i > 0 {
            assert_eq!(f.n, list[i - 1].n + 1);
        }
        let (h, records) = read(&f.path);
        assert_eq!(h.n, f.n);
        seqs.extend(records.iter().map(|r| r.seq));
    }
    assert!(total <= KEEP, "total {total}");
    assert!(seqs.windows(2).all(|w| w[1] == w[0] + 1), "{seqs:?}");
    assert_eq!(*seqs.last().unwrap(), N + 1);
}

/// Retention counts every incarnation's files, oldest first.
#[test]
fn retention_removes_older_incarnations_first() {
    let vol = TempDir::new("evkeep");
    for f in ["1-1.mel", "2-1.mel"] {
        fs::write(vol.0.join(f), vec![b'a'; 100 << 10]).unwrap();
    }
    let log = open(&vol.0, |c| c.keep_bytes = 256 << 10);
    post(&log, 1, b"a");
    log.close();
    let names: Vec<_> = files(&vol.0)
        .unwrap()
        .iter()
        .map(|f| (f.incarnation, f.n))
        .collect();
    assert_eq!(names, [(2, 1), (3, 1)]);
}

/// A frame larger than a file's size gets a file of its own.
#[test]
fn a_frame_over_the_file_size_has_its_own_file() {
    let vol = TempDir::new("evbig");
    let log = open(&vol.0, |c| c.keep_bytes = 256 << 10);
    post(&log, 1, b"a");
    post(&log, 2, &vec![b'b'; 100 << 10]);
    post(&log, 3, b"c");
    log.close();
    let per_file: Vec<Vec<u64>> = files(&vol.0)
        .unwrap()
        .iter()
        .map(|f| read(&f.path).1.iter().map(|r| r.seq).collect())
        .collect();
    assert_eq!(per_file, [vec![1], vec![2], vec![3, 4]]);
}

#[test]
fn numbers_after_existing_files() {
    let vol = TempDir::new("evnext");
    for f in ["3-1.mel", "3-2.mel", "4-7.mel"] {
        fs::write(vol.0.join(f), b"").unwrap();
    }
    let log = open(&vol.0, |_| {});
    post(&log, 1, b"");
    log.close();
    assert!(vol.0.join("3-3.mel").exists());
    assert_eq!(fs::read(vol.0.join("3-1.mel")).unwrap(), b"");
}

/// O_EXCL: a name already taken is never opened; the next try takes the next n.
#[test]
fn an_existing_name_is_not_overwritten() {
    let vol = TempDir::new("evexcl");
    fs::write(vol.0.join("taken"), b"keep").unwrap();
    let err = create_file(&vol.0.join("taken")).err().expect("O_EXCL");
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(fs::read(vol.0.join("taken")).unwrap(), b"keep");

    // 3-1.mel is a directory, so the listing skips it and the open fails.
    fs::create_dir(vol.0.join("3-1.mel")).unwrap();
    let lines = Arc::new(Mutex::new(Vec::new()));
    let log = open(&vol.0, |c| {
        let lines = lines.clone();
        c.log = Some(Arc::new(move |s: &str| {
            lines.lock().unwrap().push(s.to_owned())
        }));
    });
    post(&log, 1, b"a");
    log.flush();
    assert_eq!(log.stats(), (0, 1));
    thread::sleep(Duration::from_millis(1100));
    post(&log, 2, b"b");
    log.close();
    assert_eq!(log.stats(), (2, 1));
    let (h, records) = read(&vol.0.join("3-2.mel"));
    assert_eq!(h.n, 2);
    assert_eq!(records.iter().map(|r| r.seq).collect::<Vec<_>>(), [2, 3]);
    assert_eq!(lines.lock().unwrap().len(), 1);
}

/// A sink that blocks its first write until released.
struct Stuck {
    file: fs::File,
    entered: Option<mpsc::Sender<()>>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
}

impl Sink for Stuck {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
            if let Some(release) = self.release.lock().unwrap().take() {
                let _ = release.recv();
            }
        }
        io::Write::write_all(&mut self.file, bytes)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }
}

/// A full ring drops and counts; append returns while the disk is stuck.
#[test]
fn a_full_ring_drops_and_does_not_wait_for_the_disk() {
    let vol = TempDir::new("evstuck");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let entered = Arc::new(Mutex::new(Some(entered_tx)));
    let release = Arc::new(Mutex::new(Some(release_rx)));
    let log = Arc::new(open(&vol.0, |c| {
        c.ring_bytes = 4 << 10;
        c.flush_every = Duration::from_millis(1);
        c.create = Some(Box::new(move |path: &Path| {
            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            Ok(Box::new(Stuck {
                file,
                entered: entered.lock().unwrap().take(),
                release: release.clone(),
            }) as Box<dyn Sink>)
        }));
    }));
    let body = [b'y'; 100];
    post(&log, 1, &body);
    entered_rx.recv().unwrap();
    const N: u64 = 1000;
    let (done_tx, done_rx) = mpsc::channel();
    let appender = {
        let log = log.clone();
        thread::spawn(move || {
            for i in 0..N {
                post(&log, i + 2, &body);
            }
            done_tx.send(()).unwrap();
        })
    };
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("append waited for the disk");
    appender.join().unwrap();
    assert!(log.stats().1 > 0, "nothing was dropped");
    release_tx.send(()).unwrap();
    log.flush();
    log.close();
    let (written, dropped) = log.stats();
    assert_eq!(written + dropped, N + 2);

    let mut last = 0;
    let mut got = 0;
    for f in files(&vol.0).unwrap() {
        for r in read(&f.path).1 {
            assert!(r.seq > last, "seq {} after {last}", r.seq);
            last = r.seq;
            got += 1;
        }
    }
    assert_eq!((got, last), (written, N + 2));
    let end = read(&files(&vol.0).unwrap().last().unwrap().path)
        .1
        .pop()
        .unwrap();
    assert_eq!(end.kind, Kind::END);
    assert_eq!(end.body, end_body(dropped).to_vec());
}

#[test]
fn a_frame_larger_than_the_ring_is_dropped() {
    let vol = TempDir::new("evhuge");
    let log = open(&vol.0, |c| c.ring_bytes = 1 << 10);
    assert_eq!(post(&log, 1, &[b'z'; 1 << 10]), 1);
    assert_eq!(post(&log, 2, b"z"), 2);
    log.close();
    assert_eq!(log.stats(), (2, 1));
    let records = read(&vol.0.join("3-1.mel")).1;
    assert_eq!(records.iter().map(|r| r.seq).collect::<Vec<_>>(), [2, 3]);
    assert_eq!(records[1].body, end_body(1).to_vec());
}

#[test]
fn an_unwritable_dir_counts_dropped_and_logs_once() {
    let vol = TempDir::new("evbad");
    let plain = vol.0.join("plain");
    fs::write(&plain, b"").unwrap();
    for dir in [Some(plain.join("eventlog")), None] {
        let lines = Arc::new(Mutex::new(Vec::<String>::new()));
        let log = Log::open(Config {
            dir: dir.clone(),
            header: header(),
            log: Some({
                let lines = lines.clone();
                Arc::new(move |s: &str| lines.lock().unwrap().push(s.to_owned()))
            }),
            ..Config::default()
        })
        .unwrap();
        for i in 0..3 {
            post(&log, i, b"");
            log.flush();
        }
        log.close();
        assert_eq!(log.stats(), (0, 4), "{dir:?}");
        let lines = lines.lock().unwrap();
        assert_eq!(lines.len(), 1, "{dir:?}: {lines:?}");
        assert!(lines[0].starts_with("event log: "), "{lines:?}");
    }
}

#[test]
fn append_after_close_is_dropped() {
    let vol = TempDir::new("evafter");
    let log = open(&vol.0, |_| {});
    log.close();
    assert_eq!(post(&log, 1, b""), 2);
    log.close();
    assert_eq!(log.stats(), (1, 1));
}

/// Ops a sink saw, in order.
#[derive(Clone, Default)]
struct Ops(Arc<Mutex<Vec<String>>>);

struct Recording {
    file: fs::File,
    ops: Ops,
}

impl Sink for Recording {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.ops
            .0
            .lock()
            .unwrap()
            .push(format!("write {}", bytes.len()));
        io::Write::write_all(&mut self.file, bytes)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.ops.0.lock().unwrap().push("sync".to_owned());
        self.file.sync_all()
    }
}

/// Close writes end, then the ring reaches the file, then the last file is fsynced.
#[test]
fn close_writes_end_then_fsyncs() {
    let vol = TempDir::new("evsync");
    let ops = Ops::default();
    let log = open(&vol.0, |c| {
        c.flush_every = Duration::from_secs(3600);
        let ops = ops.clone();
        c.create = Some(Box::new(move |path: &Path| {
            ops.0.lock().unwrap().push("create".to_owned());
            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            Ok(Box::new(Recording {
                file,
                ops: ops.clone(),
            }) as Box<dyn Sink>)
        }));
    });
    post(&log, 1, b"aaaa");
    let started = Instant::now();
    log.close();
    assert!(started.elapsed() < Duration::from_secs(5));
    let mut header = Vec::new();
    Header {
        n: 1,
        created_ns: 1000,
        ..self::header()
    }
    .encode(&mut header);
    assert_eq!(
        *ops.0.lock().unwrap(),
        [
            "create".to_owned(),
            format!("write {}", header.len()),
            format!("write {}", 2 * FRAME_HEAD + 4 + 8),
            "write 0".to_owned(),
            "sync".to_owned(),
        ]
    );
    let records = read(&vol.0.join("3-1.mel")).1;
    assert_eq!(records.last().unwrap().kind, Kind::END);
}

/// The writer drains on its own when the ring is a quarter full, and every
/// flush interval otherwise.
#[test]
fn drains_at_a_quarter_and_on_the_interval() {
    let vol = TempDir::new("evkick");
    let log = open(&vol.0, |c| {
        c.ring_bytes = 4 << 10;
        c.flush_every = Duration::from_secs(3600);
    });
    let body = [b'q'; 200];
    for i in 0..4 {
        post(&log, i + 1, &body);
    }
    thread::sleep(Duration::from_millis(200));
    assert_eq!(log.stats(), (0, 0), "under a quarter, nothing is due");
    post(&log, 5, &body);
    wait_written(&log, 5);
    drop(log);

    let vol = TempDir::new("evtick");
    let log = open(&vol.0, |_| {});
    post(&log, 1, b"t");
    wait_written(&log, 1);
}

fn wait_written(log: &Log, n: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while log.stats().0 < n {
        assert!(Instant::now() < deadline, "written {:?}", log.stats());
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn concurrent_appends_number_each_frame_once() {
    let vol = TempDir::new("evconc");
    let log = Arc::new(open(&vol.0, |_| {}));
    const WORKERS: u64 = 8;
    const EACH: u64 = 500;
    let threads: Vec<_> = (0..WORKERS)
        .map(|w| {
            let log = log.clone();
            thread::spawn(move || {
                for i in 0..EACH {
                    log.append(Kind::POST, 0, w as i64, i as i64, w, &[], b"p");
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    log.close();
    let mut seqs: Vec<u64> = files(&vol.0)
        .unwrap()
        .iter()
        .flat_map(|f| read(&f.path).1)
        .map(|r| r.seq)
        .collect();
    seqs.sort_unstable();
    assert_eq!(seqs, (1..=WORKERS * EACH + 1).collect::<Vec<_>>());
}

#[test]
fn drop_closes() {
    let vol = TempDir::new("evdrop");
    let path: PathBuf = vol.0.join("3-1.mel");
    {
        let log = open(&vol.0, |_| {});
        post(&log, 1, b"d");
    }
    let records = read(&path).1;
    assert_eq!(records.last().unwrap().kind, Kind::END);
}
