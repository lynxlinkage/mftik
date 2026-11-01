//! Offline replay of one incarnation (PROTOCOL §12.6, E07-D016–E07-D019).
//!
//! Synthetic logs cover a matching strategy (zero mismatch), a changed
//! strategy, a gap in `rec_seq` (`missing`), and a hook that never returned
//! (`skipped`). A recorded session is replayed the same way, without NATS.

use std::env;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mftik_core::eventlog::{STOP_DELIVER, init_head};
use mftik_proto::wire::mftiko::common::v1::Outcome;
use mftik_proto::wire::mftiko::session::v1::EventLog;
use mftik_proto::wire::mftiko::stsctl::v1::{Bindings, InitPayload, StartPayload};
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::eventlog::{self, Header, Kind, Record};
use mftik_rt::{Context, HookError, ReplayError, ReplayOptions, ReplayReport, Strategy, replay};
use mftik_testkit::*;
use prost::Message;

/// A strategy that does nothing in every hook. The synthetic log is this
/// session: init, start, stop, onstop.
struct Noop;

impl Strategy for Noop {
    fn on_init(&mut self, _: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_start(&mut self, _: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_stop(&mut self, _: &Context<Self>) {}
}

/// OnStart posts, then a timer. The recorded session is this strategy.
struct Posted;

impl Strategy for Posted {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        let _ = (ctx.now_ns(), ctx.seed());
        Ok(())
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        ctx.post(|_, ctx| {
            ctx.after(Duration::ZERO, |_, _| {});
        });
        Ok(())
    }

    fn on_stop(&mut self, _: &Context<Self>) {}
}

/// Same hooks as [`Noop`], except OnStart returns an error.
struct Changed;

impl Strategy for Changed {
    fn on_init(&mut self, _: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_start(&mut self, _: &Context<Self>) -> Result<(), HookError> {
        Err("changed".into())
    }
    fn on_stop(&mut self, _: &Context<Self>) {}
}

struct Probe {
    seen: Arc<Mutex<(String, String)>>,
}

impl Strategy for Probe {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        *self.seen.lock().expect("seen") = (ctx.volume_dir().to_owned(), ctx.input_dir("model"));
        Ok(())
    }
    fn on_start(&mut self, _: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_stop(&mut self, _: &Context<Self>) {}
}

/// The worker process's entry. In a normal test run the variable is unset
/// and this passes at once.
#[test]
fn worker_entry() {
    if env::var_os(WORKER_ENV).is_some() {
        mftik_rt::run(Posted, Some(TEST_RELEASE));
    }
}

fn replay_one<S: Strategy>(
    strategy: S,
    dir: impl AsRef<Path>,
    opt: ReplayOptions,
) -> Result<ReplayReport, ReplayError> {
    static LOCK: Mutex<()> = Mutex::new(());
    let _gate = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    replay(strategy, dir, opt)
}

fn header() -> Header {
    Header {
        format_version: 1,
        incarnation: 1,
        n: 1,
        created_ns: 1,
        session_id: "s1".into(),
        sdk_version: "1".into(),
        sdk_commit: "a".into(),
    }
}

fn rec(kind: Kind, seq: u64, r#ref: u64, t_ns: i64, body: Vec<u8>) -> Record {
    Record {
        kind,
        flags: 0,
        seq,
        recv_ns: t_ns,
        t_ns,
        r#ref,
        body,
    }
}

fn end_of(seq: u64, input: u64, t_ns: i64) -> Record {
    Record {
        kind: Kind::HOOK_END,
        flags: 0,
        seq,
        recv_ns: 0,
        t_ns,
        r#ref: input,
        body: Vec::new(),
    }
}

fn init_body() -> Vec<u8> {
    let mut body = init_head(0).to_vec();
    body.extend_from_slice(
        &InitPayload {
            session_id: "s1".into(),
            incarnation: 1,
            recv_window_ms: 500,
            hook_timeout_s: 1,
            ..Default::default()
        }
        .encode_to_vec(),
    );
    body
}

fn start_body() -> Vec<u8> {
    StartPayload {
        bindings: Some(Bindings::default()),
    }
    .encode_to_vec()
}

/// init, start, stop, onstop: a strategy that does nothing in every hook.
fn noop_log() -> Vec<Record> {
    vec![
        rec(Kind::INIT, 1, 7, 1, init_body()),
        end_of(2, 1, 2),
        rec(Kind::START, 3, 0, 3, start_body()),
        end_of(4, 3, 4),
        Record {
            kind: Kind::STOP,
            flags: STOP_DELIVER,
            seq: 5,
            recv_ns: 5,
            t_ns: 5,
            r#ref: 0,
            body: Vec::new(),
        },
        rec(Kind::ONSTOP, 6, 0, 6, Vec::new()),
        end_of(7, 6, 7),
        rec(Kind::END, 8, 0, 8, 0u64.to_le_bytes().to_vec()),
    ]
}

fn write_log(dir: &Path, header: &Header, recs: &[Record]) {
    fs::create_dir_all(dir).expect("eventlog dir");
    let mut header = header.clone();
    header.n = 1;
    let mut bytes = Vec::new();
    header.encode(&mut bytes);
    for rec in recs {
        rec.encode(&mut bytes);
    }
    fs::write(dir.join(eventlog::name(header.incarnation, 1)), bytes).expect("log file");
}

fn temp_log(recs: &[Record]) -> TempDir {
    let dir = TempDir::new("replay");
    write_log(&dir.0, &header(), recs);
    dir
}

#[test]
fn a_matching_strategy_replays_with_no_mismatch() {
    let dir = temp_log(&noop_log());
    let report = replay_one(Noop, &dir.0, ReplayOptions::default()).expect("replay");
    assert_eq!(report.session_id, "s1");
    assert_eq!(report.incarnation, 1);
    assert_eq!(report.sdk_version, "1");
    assert_eq!(report.sdk_commit, "a");
    assert_eq!(report.inputs, 3, "{report:?}");
    assert_eq!(report.skipped, 0);
    assert_eq!(report.missing, 0);
    assert_eq!(report.orders, 0);
    assert_eq!(report.reads, 0);
    assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
}

#[test]
fn a_changed_strategy_produces_a_mismatch() {
    let dir = temp_log(&noop_log());
    let report = replay_one(Changed, &dir.0, ReplayOptions::default()).expect("replay");
    assert!(
        report
            .mismatches
            .iter()
            .any(|line| line.contains("rec_seq 3 start") && line.contains("changed")),
        "{:?}",
        report.mismatches
    );
}

#[test]
fn a_gap_in_rec_seq_is_counted_in_missing() {
    let mut recs = noop_log();
    recs.push(rec(
        Kind::BINDINGS,
        10,
        0,
        10,
        Bindings::default().encode_to_vec(),
    ));
    let dir = temp_log(&recs);
    let report = replay_one(Noop, &dir.0, ReplayOptions::default()).expect("replay");
    assert_eq!(report.missing, 1, "{report:?}");
    assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
}

#[test]
fn a_hook_without_hook_end_is_skipped_and_not_run() {
    let recs: Vec<_> = noop_log().into_iter().filter(|rec| rec.seq <= 6).collect();
    let dir = temp_log(&recs);
    let report = replay_one(Noop, &dir.0, ReplayOptions::default()).expect("replay");
    assert_eq!(report.skipped, 1, "{report:?}");
    assert_eq!(report.inputs, 2, "{report:?}");
}

#[test]
fn volume_and_input_dirs_are_what_the_options_set() {
    let dir = temp_log(&noop_log());
    let seen = Arc::new(Mutex::new((String::new(), String::new())));
    let report = replay_one(
        Probe {
            seen: Arc::clone(&seen),
        },
        &dir.0,
        ReplayOptions {
            volume_dir: "vol".into(),
            input_dir: "in".into(),
            ..ReplayOptions::default()
        },
    )
    .expect("replay");
    assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
    let (volume, input) = seen.lock().expect("seen").clone();
    assert_eq!(volume, "vol");
    assert_eq!(input, "in/model");
}

#[test]
fn a_directory_with_no_log_is_an_error() {
    let dir = TempDir::new("replay-empty");
    let err = replay_one(Posted, &dir.0, ReplayOptions::default()).expect_err("empty");
    assert!(
        err.to_string()
            .contains("has no event log of incarnation 0"),
        "{err}"
    );
}

#[test]
fn the_log_must_start_with_init() {
    let dir = temp_log(&[rec(Kind::START, 1, 0, 1, start_body())]);
    let err = replay_one(Posted, &dir.0, ReplayOptions::default()).expect_err("no init");
    assert!(
        err.to_string()
            .contains("the log does not start with the init frame"),
        "{err}"
    );
}

/// Hello, Init with the log on, Start, Stage(running).
fn running(shim: &mut Shim, nats: &Nats) -> InitPayload {
    let mut payload = init_payload(&nats.creds, "");
    payload.event_log = Some(EventLog {
        enabled: true,
        keep_mb: 1,
    });
    hello(shim);
    init(shim, payload.clone(), 5000);
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

#[test]
fn a_recorded_session_replays_with_no_mismatch_and_a_changed_strategy_mismatches() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats);
    // init, start, post, timer, each with hook_end.
    let report = shim.wait_report(|r| r.event_log.as_ref().is_some_and(|s| s.records == 8));
    assert_eq!(report.event_log.as_ref().map(|s| s.dropped), Some(0));
    shim.send(worker_down::Msg::Stop(worker::Stop {
        grace_ms: 5000,
        reason: "operator".into(),
    }));
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);

    let dir = ended.volume.0.join(eventlog::DIR);
    let matched = replay_one(Posted, &dir, ReplayOptions::default()).expect("replay");
    assert_eq!(matched.missing, 0, "{matched:?}");
    assert_eq!(matched.skipped, 0, "{matched:?}");
    assert!(
        matched.mismatches.is_empty(),
        "recorded then replayed: {:?}",
        matched.mismatches
    );
    assert_eq!(matched.inputs, 5, "{matched:?}");

    let diverged = replay_one(Changed, &dir, ReplayOptions::default()).expect("replay");
    assert!(
        !diverged.mismatches.is_empty(),
        "a changed strategy must mismatch"
    );
}
