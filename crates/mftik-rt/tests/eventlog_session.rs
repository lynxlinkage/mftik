//! A session's event log, end to end (PROTOCOL §12.5, E07-D007, E07-D008).
//!
//! The strategy makes its own inputs: post 1 makes timer 1, whose run makes
//! post 2. Each input is written before its hook and a `hook_end` after it.
//! A hook that panics gets a `hook_end` with the panic, a `HUNG` hook gets
//! none, and every exit closes the log with `end`. MD and TD inputs are
//! checked against nats-server by the path tests in `src/md` and `src/td`.

use std::env;
use std::path::Path;
use std::thread;
use std::time::Duration;

use mftik_proto::wire::mftiko::common::v1::Outcome;
use mftik_proto::wire::mftiko::session::v1::EventLog;
use mftik_proto::wire::mftiko::stsctl::v1::EventLogStat;
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::eventlog::{self, Kind, Reader, Record};
use mftik_rt::{Context, HookError, Strategy};
use mftik_testkit::*;

/// `InitPayload.params` names how post 2 ends.
#[derive(Default)]
struct Steps {
    scenario: String,
}

impl Strategy for Steps {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        self.scenario = String::from_utf8(ctx.params().to_vec())?;
        Ok(())
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
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

/// Hello, Init with the log on, Start, and Stage(running).
fn running(shim: &mut Shim, nats: &Nats, scenario: &str) {
    let mut payload = init_payload(&nats.creds, scenario);
    payload.event_log = Some(EventLog {
        enabled: true,
        keep_mb: 1,
    });
    hello(shim);
    init(shim, payload, 5000);
    expect_applied(shim, "live-1");
    expect_done(shim, Verb::Init, true, "");
    start(shim);
    expect_done(shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }
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

/// One line per frame: `rec_seq`, kind, `ref` and `flags` when set, and a
/// `hook_end`'s text.
fn lines(records: &[Record]) -> Vec<String> {
    records
        .iter()
        .map(|record| {
            let mut line = format!("{} {}", record.seq, record.kind.name().unwrap_or("?"));
            if record.r#ref != 0 {
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
    running(&mut shim, &nats, "");
    // Six frames reach the file while the session runs.
    let report = shim.wait_report(|r| r.event_log.as_ref().is_some_and(|s| s.records == 6));
    assert_eq!(
        report.event_log,
        Some(EventLogStat {
            records: 6,
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
            "1 post ref=1",
            "2 hook_end ref=1",
            "3 timer ref=1",
            "4 hook_end ref=3",
            "5 post ref=2",
            "6 hook_end ref=5",
            "7 end",
        ]
    );
    assert_times(&records);
    assert_eq!(records[6].body, 0u64.to_le_bytes());
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
            "1 post ref=1",
            "2 hook_end ref=1",
            "3 timer ref=1",
            "4 hook_end ref=3",
            "5 post ref=2",
            r#"6 hook_end ref=5 flags=1 "boom""#,
            "7 end",
        ]
    );
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
            "1 post ref=1",
            "2 hook_end ref=1",
            "3 timer ref=1",
            "4 hook_end ref=3",
            "5 post ref=2",
            "6 end",
        ]
    );
    assert_times(&records);
}
