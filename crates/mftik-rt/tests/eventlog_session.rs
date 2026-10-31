//! A session's event log, end to end (PROTOCOL §12.5, E07-D007, E07-D008,
//! E07-D012).
//!
//! The log opens with `init` and `start`, each with its `hook_end`. Then the
//! strategy makes its own inputs: post 1 makes timer 1, whose run makes
//! post 2. Each input is written before its hook and a `hook_end` after it.
//! A hook that panics gets a `hook_end` with the panic, a `HUNG` hook gets
//! none, and every exit closes the log with `end`. A panic in OnInit or
//! OnStart is in the file before C4 hears of it. MD and TD inputs are
//! checked against nats-server by the path tests in `src/md` and `src/td`,
//! and every kind by `eventlog_kinds`.

use std::env;
use std::path::Path;
use std::thread;
use std::time::Duration;

use mftik_proto::wire::mftiko::common::v1::Outcome;
use mftik_proto::wire::mftiko::session::v1::EventLog;
use mftik_proto::wire::mftiko::stsctl::v1::{EventLogStat, InitPayload};
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::eventlog::{self, Kind, Reader, Record};
use mftik_rt::{Context, HookError, Strategy};
use mftik_testkit::*;
use prost::Message;
use serde_json::Value;

/// `InitPayload.params` names the hook that goes wrong, if one does: OnInit,
/// OnStart, or post 2.
#[derive(Default)]
struct Steps {
    scenario: String,
}

impl Strategy for Steps {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        self.scenario = String::from_utf8(ctx.params().to_vec())?;
        if self.scenario == "init-panic" {
            panic!("init boom");
        }
        Ok(())
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        if self.scenario == "start-panic" {
            panic!("start boom");
        }
        ctx.post(|_, ctx| {
            ctx.after(Duration::ZERO, |_, ctx| {
                ctx.post(|s, _| match s.scenario.as_str() {
                    "panic" => panic!("boom"),
                    "hang" => thread::sleep(Duration::from_secs(30)),
                    _ => {}
                });
            });
        });
        Ok(())
    }

    fn on_stop(&mut self, _ctx: &Context<Self>) {}
}

/// The worker process's entry. In a normal test run the variable is unset
/// and this passes at once.
#[test]
fn worker_entry() {
    if env::var_os(WORKER_ENV).is_some() {
        mftik_rt::run(Steps::default(), Some(TEST_RELEASE));
    }
}

/// Hello, then Init with the log on. Returns the InitPayload sent.
fn logged(shim: &mut Shim, nats: &Nats, scenario: &str) -> InitPayload {
    let mut payload = init_payload(&nats.creds, scenario);
    payload.event_log = Some(EventLog {
        enabled: true,
        keep_mb: 1,
    });
    hello(shim);
    init(shim, payload.clone(), 5000);
    payload
}

/// Hello, Init with the log on, Start, and Stage(running). Returns the
/// InitPayload sent.
fn running(shim: &mut Shim, nats: &Nats, scenario: &str) -> InitPayload {
    let payload = logged(shim, nats, scenario);
    expect_applied(shim, "live-1");
    expect_done(shim, Verb::Init, true, "");
    start(shim);
    expect_done(shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }
    payload
}

/// Every frame in the volume's log directory, oldest first.
fn frames(volume: &Path) -> Vec<Record> {
    let mut records = Vec::new();
    for file in eventlog::files(&volume.join(eventlog::DIR)).expect("log files") {
        let mut reader = Reader::open(&file.path).expect("log file");
        records.extend(reader.read_all().expect("frames"));
    }
    records
}

/// The first `n` frames in the file now, while the worker may still be
/// writing the ones after them.
fn on_disk(volume: &Path, n: usize) -> Vec<Record> {
    let files = eventlog::files(&volume.join(eventlog::DIR)).expect("log files");
    let file = files.first().expect("a log file");
    let mut reader = Reader::open(&file.path).expect("log file");
    (0..n)
        .map(|_| {
            reader
                .next_record()
                .expect("a whole frame")
                .expect("a frame")
        })
        .collect()
}

/// The frames as Go `mftiko-eventlog dump` prints them, after the header.
fn dumped(volume: &Path) -> Vec<Value> {
    let (ok, text) = eventlog_dump(&[volume.join(eventlog::DIR)]);
    assert!(ok, "{text}");
    text.lines()
        .skip(1)
        .map(|line| serde_json::from_str(line).expect("json"))
        .collect()
}

/// One line per frame: `rec_seq`, kind, `ref` and `flags` when set, and a
/// `hook_end`'s text. The `init` ref, a random seed, is left out.
fn lines(records: &[Record]) -> Vec<String> {
    records
        .iter()
        .map(|record| {
            let mut line = format!("{} {}", record.seq, record.kind.name().unwrap_or("?"));
            if record.r#ref != 0 && record.kind != Kind::INIT {
                line += &format!(" ref={}", record.r#ref);
            }
            if record.flags != 0 {
                line += &format!(" flags={}", record.flags);
            }
            if record.kind == Kind::HOOK_END && !record.body.is_empty() {
                line += &format!(" {:?}", String::from_utf8_lossy(&record.body));
            }
            line
        })
        .collect()
}

/// An input is stamped when it was queued and when its hook started, and
/// its `hook_end` comes no earlier. `end` closes the file.
#[track_caller]
fn assert_times(records: &[Record]) {
    for (at, record) in records.iter().enumerate() {
        match record.kind {
            Kind::HOOK_END => {
                let input = &records[record.r#ref as usize - 1];
                assert_eq!(input.seq, record.r#ref);
                assert!(input.t_ns <= record.t_ns, "{at}");
                assert_eq!(record.recv_ns, 0);
            }
            Kind::END => assert_eq!(at, records.len() - 1),
            _ => assert!(0 < record.recv_ns && record.recv_ns <= record.t_ns, "{at}"),
        }
    }
}

#[test]
fn each_input_is_followed_by_its_hook_end_and_the_exit_writes_end() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    let mut sent = running(&mut shim, &nats, "");
    // Ten frames reach the file while the session runs.
    let report = shim.wait_report(|r| r.event_log.as_ref().is_some_and(|s| s.records == 10));
    assert_eq!(
        report.event_log,
        Some(EventLogStat {
            records: 10,
            dropped: 0
        })
    );
    shim.send(worker_down::Msg::Stop(worker::Stop {
        grace_ms: 5000,
        reason: "operator".into(),
    }));
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);

    let records = frames(&ended.volume.0);
    assert_eq!(
        lines(&records),
        [
            "1 init",
            "2 hook_end ref=1",
            "3 start",
            "4 hook_end ref=3",
            "5 post ref=1",
            "6 hook_end ref=5",
            "7 timer ref=1",
            "8 hook_end ref=7",
            "9 post ref=2",
            "10 hook_end ref=9",
            "11 stop flags=1",
            "12 onstop",
            "13 hook_end ref=12",
            "14 end",
        ]
    );
    assert_times(&records);
    assert_eq!(records[13].body, 0u64.to_le_bytes());
    // The init body is the cloid counter, then the payload as sent without
    // its NATS credentials (E07-D010).
    sent.nats = None;
    assert_eq!(
        InitPayload::decode(&records[0].body[8..]).expect("init payload"),
        sent
    );
}

#[test]
fn a_hook_that_panics_still_gets_its_hook_end() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats, "panic");
    expect_ending(&mut shim, Outcome::Crashed, "hook panic: boom", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(20), "{}", ended.stderr);

    let records = frames(&ended.volume.0);
    assert_eq!(
        lines(&records),
        [
            "1 init",
            "2 hook_end ref=1",
            "3 start",
            "4 hook_end ref=3",
            "5 post ref=1",
            "6 hook_end ref=5",
            "7 timer ref=1",
            "8 hook_end ref=7",
            "9 post ref=2",
            r#"10 hook_end ref=9 flags=1 "boom""#,
            "11 stop",
            "12 onstop",
            "13 hook_end ref=12",
            "14 end",
        ]
    );
    assert_times(&records);
}

#[test]
fn an_on_init_panic_is_in_the_file_before_done_and_the_exit_writes_end() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    logged(&mut shim, &nats, "init-panic");
    let why = "hook panic: init boom";
    expect_done(&mut shim, Verb::Init, false, why);
    let panicked = ["1 init", r#"2 hook_end ref=1 flags=1 "init boom""#];
    assert_eq!(lines(&on_disk(&shim.volume.0, 2)), panicked);
    expect_ending(&mut shim, Outcome::InitFailed, why, Some(false));
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);

    let records = frames(&ended.volume.0);
    assert_eq!(lines(&records), [panicked[0], panicked[1], "3 end"]);
    assert_times(&records);
    let dumped = dumped(&ended.volume.0);
    assert_eq!(dumped[1]["ending"], "panic", "{}", dumped[1]);
    assert_eq!(dumped[1]["panic"], "init boom", "{}", dumped[1]);
}

#[test]
fn an_on_start_panic_is_in_the_file_before_done_and_on_stop_still_runs() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    logged(&mut shim, &nats, "start-panic");
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    start(&shim);
    let why = "hook panic: start boom";
    expect_done(&mut shim, Verb::Start, false, why);
    let panicked = [
        "1 init",
        "2 hook_end ref=1",
        "3 start",
        r#"4 hook_end ref=3 flags=1 "start boom""#,
    ];
    assert_eq!(lines(&on_disk(&shim.volume.0, 4)), panicked);
    expect_ending(&mut shim, Outcome::InitFailed, why, Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);

    let records = frames(&ended.volume.0);
    let mut want = panicked.to_vec();
    want.extend(["5 onstop", "6 hook_end ref=5", "7 end"]);
    assert_eq!(lines(&records), want);
    assert_times(&records);
}

#[test]
fn a_hung_hook_has_no_hook_end_and_the_exit_still_writes_end() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats, "hang");
    expect_ending(&mut shim, Outcome::Hung, "hook timeout", Some(false));
    let ended = shim.end();
    assert_eq!(ended.code, Some(21), "{}", ended.stderr);

    let records = frames(&ended.volume.0);
    assert_eq!(
        lines(&records),
        [
            "1 init",
            "2 hook_end ref=1",
            "3 start",
            "4 hook_end ref=3",
            "5 post ref=1",
            "6 hook_end ref=5",
            "7 timer ref=1",
            "8 hook_end ref=7",
            "9 post ref=2",
            "10 stop flags=2",
            "11 end",
        ]
    );
    assert_times(&records);
    // Go reads the watchdog's stop: no OnStop was asked for.
    let dumped = dumped(&ended.volume.0);
    let stop = &dumped[9];
    assert_eq!(stop["kind"], "stop");
    assert_eq!(stop["hung"], true, "{stop}");
    assert!(stop.get("deliver").is_none(), "{stop}");
    assert!(stop.get("onstop_asked_ns").is_none(), "{stop}");
    assert_eq!(dumped[10]["kind"], "end");
    assert_eq!(dumped[10]["dropped"], 0);
}
