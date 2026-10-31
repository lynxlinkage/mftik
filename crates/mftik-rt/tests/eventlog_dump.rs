//! Files this writer leaves are read cleanly by Go `mftiko-eventlog dump`
//! (PROTOCOL §12.7, E07-D002).
//!
//! The binary comes from `MFTIK_EVENTLOG`, else `mftiko-eventlog` on PATH:
//! `scripts/install-mftiko-eventlog` installs it from the mftiko commit in
//! contract/proto/SOURCE, as CI does. These tests fail when it is missing.

use std::fs;
use std::path::Path;

use mftik_proto::wire::mftiko::md::v1::ViewRequest;
use mftik_proto::wire::mftiko::stsctl::v1::InitPayload;
use mftik_rt::eventlog::{Config, Header, Kind, Log};
use mftik_testkit::{TempDir, eventlog_dump as dump};
use prost::Message;

fn fixed_ns() -> i64 {
    1_000
}

fn open(dir: &Path, keep_bytes: u64) -> Log {
    Log::open(Config {
        dir: Some(dir.to_path_buf()),
        header: Header::mftik(3, "s1"),
        keep_bytes,
        now_ns: Some(fixed_ns),
        ..Config::default()
    })
    .expect("open")
}

fn header_line(n: u32) -> String {
    format!(
        r#"{{"file":"3-{n}.mel","format_version":1,"incarnation":3,"n":{n},"created_ns":1000,"session_id":"s1","sdk_version":"{}","sdk_commit":"{}"}}"#,
        env!("CARGO_PKG_VERSION"),
        env!("VERGEN_GIT_SHA"),
    )
}

fn str16(text: &str) -> Vec<u8> {
    let mut b = (text.len() as u16).to_le_bytes().to_vec();
    b.extend_from_slice(text.as_bytes());
    b
}

/// One frame of each body shape the writer is given: protobuf, the
/// order_call layout, text, and empty; then the end frame from close.
fn write_session(dir: &Path) -> Log {
    let log = open(dir, 0);
    let init = InitPayload {
        session_id: "s1".to_owned(),
        incarnation: 3,
        recv_window_ms: 1000,
        hook_timeout_s: 1,
        ..InitPayload::default()
    }
    .encode_to_vec();
    log.append(Kind::INIT, 0, 10, 11, 7, &5u64.to_le_bytes(), &init);
    log.append(Kind::HOOK_END, 0, 0, 12, 1, &[], &[]);
    log.append(Kind::TIMER, 0, 20, 21, 1, &[], &[]);
    let mut call = Vec::new();
    call.extend_from_slice(&1i64.to_le_bytes());
    call.extend_from_slice(&2u64.to_le_bytes());
    call.extend_from_slice(&1u16.to_le_bytes());
    call.extend_from_slice(&str16("c1"));
    log.append(Kind::ORDER_CALL, 1, 0, 22, 1, &call, &[]);
    let view = ViewRequest {
        limit: 10,
        ..ViewRequest::default()
    }
    .encode_to_vec();
    log.append(Kind::READ_CALL, 1, 0, 23, 1, &str16("md.v1.view.a"), &view);
    log.append(Kind::RESOLVE, 6, 25, 24, 0, &str16("A"), b"not found");
    log.append(Kind::HOOK_END, 1, 0, 26, 3, &[], b"boom");
    log.append(Kind::POST, 0, 30, 31, 1, &[], &[]);
    log.append(Kind::HOOK_END, 2, 0, 32, 8, &[], b"bad");
    log.append(Kind::STOP, 1, 40, 41, 0, &[], &[]);
    log.append(Kind::ONSTOP, 0, 41, 42, 0, &[], &[]);
    log.append(Kind::HOOK_END, 0, 0, 43, 11, &[], &[]);
    log.close();
    log
}

const FRAMES: &[&str] = &[
    r#"{"rec_seq":1,"kind":"init","recv_ns":10,"hook_start_ns":11,"seed":7,"cloid_slot":5,"payload":{"session_id":"s1","incarnation":3,"recv_window_ms":1000,"hook_timeout_s":1}}"#,
    r#"{"rec_seq":2,"kind":"hook_end","hook_end_ns":12,"input":1,"ending":"returned"}"#,
    r#"{"rec_seq":3,"kind":"timer","recv_ns":20,"hook_start_ns":21,"timer":1}"#,
    r#"{"rec_seq":4,"kind":"order_call","call_ns":22,"call":1,"op":"submit","api_id":1,"submit_gen":2,"cloids":["c1"]}"#,
    r#"{"rec_seq":5,"kind":"read_call","call_ns":23,"read":1,"read_kind":"tape","subject":"md.v1.view.a","request":{"limit":10}}"#,
    r#"{"rec_seq":6,"kind":"resolve","call_ns":24,"reply_ns":25,"outcome":"not_found","error":"not found","ticker":"A"}"#,
    r#"{"rec_seq":7,"kind":"hook_end","hook_end_ns":26,"input":3,"ending":"panic","panic":"boom"}"#,
    r#"{"rec_seq":8,"kind":"post","recv_ns":30,"hook_start_ns":31,"post":1}"#,
    r#"{"rec_seq":9,"kind":"hook_end","hook_end_ns":32,"input":8,"ending":"error","error":"bad"}"#,
    r#"{"rec_seq":10,"kind":"stop","stop_ns":40,"onstop_asked_ns":41,"deliver":true}"#,
    r#"{"rec_seq":11,"kind":"onstop","recv_ns":41,"hook_start_ns":42}"#,
    r#"{"rec_seq":12,"kind":"hook_end","hook_end_ns":43,"input":11,"ending":"returned"}"#,
    r#"{"rec_seq":13,"kind":"end","end_ns":1000,"dropped":0}"#,
];

fn expected(n: u32, frames: &[&str]) -> String {
    let mut text = header_line(n);
    text.push('\n');
    for line in frames {
        text.push_str(line);
        text.push('\n');
    }
    text
}

#[test]
fn dump_reads_a_file_and_its_directory() {
    let vol = TempDir::new("evdump");
    let dir = vol.0.join("eventlog");
    let log = write_session(&dir);
    assert_eq!(log.stats(), (13, 0));
    let want = expected(1, FRAMES);

    let file = dir.join("3-1.mel");
    assert_eq!(dump(&[file.to_str().unwrap()]), (true, want.clone()));
    assert_eq!(dump(&[dir.to_str().unwrap()]), (true, want.clone()));
    assert_eq!(
        dump(&["-incarnation", "3", dir.to_str().unwrap()]),
        (true, want)
    );
    assert_eq!(
        dump(&["-incarnation", "4", dir.to_str().unwrap()]),
        (true, String::new())
    );
}

/// A file cut inside its last frame, as a SIGKILL leaves it: the frames
/// before the cut, then the damage line, and the dump does not fail.
#[test]
fn dump_reads_a_file_cut_inside_a_frame() {
    let vol = TempDir::new("evdumpcut");
    write_session(&vol.0);
    let path = vol.0.join("3-1.mel");
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
    let mut want = expected(1, &FRAMES[..FRAMES.len() - 1]);
    want.push_str(r#"{"file":"3-1.mel","error":"eventlog: the file ends inside a frame"}"#);
    want.push('\n');
    assert_eq!(dump(&[vol.0.to_str().unwrap()]), (true, want));
}

/// Rotated and retained files: each file opens with its header line, and
/// the frames that are left keep their rec_seq order without a gap.
#[test]
fn dump_reads_rotated_files() {
    let vol = TempDir::new("evdumprot");
    let log = open(&vol.0, 256 << 10);
    let body = vec![b'x'; 10 << 10];
    for i in 0..100u64 {
        log.append(Kind::POST, 0, 1, 2, i + 1, &[], &body);
        if i % 10 == 9 {
            log.flush();
        }
    }
    log.close();

    let (ok, text) = dump(&[vol.0.to_str().unwrap()]);
    assert!(ok, "{text}");
    let names: Vec<String> = fs::read_dir(&vol.0)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(names.len() >= 2, "{names:?}");
    assert!(!names.contains(&"3-1.mel".to_owned()), "{names:?}");

    let mut headers = 0;
    let mut seqs = Vec::new();
    for line in text.lines() {
        if line.starts_with(r#"{"file":"#) {
            headers += 1;
            continue;
        }
        let rest = line
            .strip_prefix(r#"{"rec_seq":"#)
            .unwrap_or_else(|| panic!("{line}"));
        let (seq, rest) = rest.split_once(',').unwrap();
        let seq: u64 = seq.parse().unwrap();
        if seq == 101 {
            assert_eq!(rest, r#""kind":"end","end_ns":1000,"dropped":0}"#);
        } else {
            assert_eq!(
                rest,
                format!(r#""kind":"post","recv_ns":1,"hook_start_ns":2,"post":{seq}}}"#)
            );
        }
        seqs.push(seq);
    }
    assert_eq!(headers, names.len());
    assert!(seqs.windows(2).all(|w| w[1] == w[0] + 1), "{seqs:?}");
    assert_eq!(seqs.last(), Some(&101));
}

/// Dropped frames leave rec_seq gaps and are counted in end.
#[test]
fn dump_shows_the_dropped_count() {
    let vol = TempDir::new("evdumpdrop");
    let log = Log::open(Config {
        dir: Some(vol.0.clone()),
        header: Header::mftik(3, "s1"),
        ring_bytes: 1 << 10,
        now_ns: Some(fixed_ns),
        ..Config::default()
    })
    .unwrap();
    log.append(Kind::POST, 0, 1, 2, 1, &[], &[]);
    log.append(Kind::POST, 0, 1, 2, 2, &[], &[b'x'; 1 << 10]);
    log.append(Kind::POST, 0, 1, 2, 3, &[], &[]);
    log.close();
    let want = expected(
        1,
        &[
            r#"{"rec_seq":1,"kind":"post","recv_ns":1,"hook_start_ns":2,"post":1}"#,
            r#"{"rec_seq":3,"kind":"post","recv_ns":1,"hook_start_ns":2,"post":3}"#,
            r#"{"rec_seq":4,"kind":"end","end_ns":1000,"dropped":1}"#,
        ],
    );
    assert_eq!(dump(&[vol.0.to_str().unwrap()]), (true, want));
}
