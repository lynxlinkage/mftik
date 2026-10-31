//! Report and every way a running session ends, end to end (E03-09).
//!
//! Each path is checked by the frames the fake shim reads, in order, and the
//! exit code (PROTOCOL §5.3, §9.2.4, §9.2.5, §9.5.5, E08-D003, E08-D015):
//!
//! - Stop and fd 3 EOF wait for the hook in progress, send `Exiting(DONE)`,
//!   hand the TD buffer to its hooks, run OnStop, then `Stopped(true)`, 0.
//! - `exit()` and `fail()` from a hook, and `Command.fail`, end the same
//!   way with `DONE` (0) or `FAILED` (30).
//! - A hook panic is `CRASHED` (20): OnStop runs, the TD buffer does not.
//!   An OnStart error is `Done(START, false)` and `INIT_FAILED` (10) with
//!   OnStop.
//! - `HUNG` sends `Exiting(HUNG, "hook timeout")` and `Stopped(false)` and
//!   exits 21 while the hook still runs: no OnStop, no TD delivery.
//! - A full TD buffer is `FAILED` with `td buffer full`; the buffer is
//!   delivered, OnStop runs, exit 30.
//! - After EOF, an OnStop past the hook budget exits 21 without `Stopped`
//!   (E03-D027).
//!
//! The shim in `mftik-testkit` keeps every Report and fails a test that reads one
//! after `Exiting`.

use std::env;
use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use mftik_core::dispatch::TD_CAP;
use mftik_proto::wire::mftiko::common::v1::{Outcome, Version};
use mftik_proto::wire::mftiko::session::v1::EventLog;
use mftik_proto::wire::mftiko::stsctl::v1::{
    Command as StsCommand, EventLogStat, Fail, InitPayload, Report, command,
};
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::{Context, HookError, Strategy};
use mftik_testkit::*;
use prost::Message;

// ---------- the worker side ----------

/// How long the `slow` scenario's hook runs: past one Report poll, inside
/// the hook budget its tests set.
const SLOW: Duration = Duration::from_millis(2500);

/// `InitPayload.params` names the scenario.
#[derive(Default)]
struct Ender {
    scenario: String,
    delivered: u64,
}

fn line(hook: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{{\"hook\":\"{hook}\"}}");
    let _ = out.flush();
}

impl Strategy for Ender {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        self.scenario = String::from_utf8(ctx.params().to_vec())?;
        line("OnInit");
        Ok(())
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        line("OnStart");
        let queued = |s: &mut Ender, _: &Context<Ender>| {
            s.delivered += 1;
            line("Queued");
        };
        match self.scenario.as_str() {
            // The hook a stop waits for: it marks the volume as it returns,
            // and what it queues is delivered after Exiting.
            "slow" => ctx.post(move |_, ctx| {
                line("Slow");
                thread::sleep(SLOW);
                mftik_rt::volume::write(ctx.volume_dir(), "state", b"slow returned")
                    .expect("volume");
                ctx.post(queued);
            }),
            "exit" => ctx.post(move |_, ctx| {
                line("Hook");
                ctx.post(queued);
                ctx.exit();
            }),
            "fail" => ctx.post(move |_, ctx| {
                line("Hook");
                ctx.post(queued);
                ctx.fail("asked to fail");
            }),
            "panic" => ctx.post(move |_, ctx| {
                line("Hook");
                ctx.post(queued);
                panic!("boom");
            }),
            "hang" => ctx.post(move |_, ctx| {
                line("Hook");
                ctx.post(queued);
                thread::sleep(Duration::from_secs(30));
            }),
            // TD_CAP entries fit; the next one ends the session.
            "flood" => ctx.post(move |_, ctx| {
                line("Hook");
                for _ in 0..=TD_CAP {
                    ctx.post(|s, _| s.delivered += 1);
                }
            }),
            "start_error" => return Err("no venue".into()),
            "slow_hook" => ctx.post(|_, _| thread::sleep(Duration::from_millis(150))),
            _ => {}
        }
        Ok(())
    }

    fn on_stop(&mut self, _ctx: &Context<Self>) {
        match self.scenario.as_str() {
            "flood" => line(&format!("OnStop {}", self.delivered)),
            "stuck_stop" => {
                line("OnStop");
                thread::sleep(Duration::from_secs(30));
            }
            _ => line("OnStop"),
        }
    }
}

/// The worker process's entry. In a normal test run the variable is unset
/// and this passes at once.
#[test]
fn worker_entry() {
    if env::var_os(WORKER_ENV).is_some() {
        mftik_rt::run(Ender::default(), Some(TEST_RELEASE));
    }
}

// ---------- helpers ----------

/// Hello, Init with this hook budget, Start with bindings 1.1, and
/// Stage(running).
fn running(shim: &mut Shim, nats: &Nats, scenario: &str, hook_timeout_s: u32) {
    let mut payload = init_payload(&nats.creds, scenario);
    payload.hook_timeout_s = hook_timeout_s;
    running_with(shim, payload);
}

fn running_with(shim: &mut Shim, payload: InitPayload) {
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

/// A Report `want` accepts: one already read, or the next one that comes.
fn report_where(shim: &mut Shim, want: impl Fn(&Report) -> bool) -> Report {
    if let Some((_, report)) = shim.reports.iter().find(|(_, r)| want(r)) {
        return report.clone();
    }
    shim.wait_report(want)
}

fn version(seq: u64) -> Option<Version> {
    Some(Version { epoch: 1, seq })
}

/// Waits until the `slow` hook is running: a Report says a hook is blocked.
fn slow_hook_running(shim: &mut Shim) {
    report_where(shim, |r| r.hook_blocked_ms > 0);
}

#[track_caller]
fn assert_slow_returned(shim: &Shim) {
    assert_eq!(
        read_volume(&shim.volume.0),
        b"slow returned",
        "Exiting went out before the hook in progress returned"
    );
}

// ---------- Report ----------

#[test]
fn reports_carry_the_queues_the_bindings_and_the_hook_counters() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats, "slow_hook", 1);

    let first = shim.wait_report(|_| true);
    let names: Vec<_> = first.queues.iter().map(|q| q.name.as_str()).collect();
    assert_eq!(names, ["td", "md"]);
    assert_eq!(first.bindings_applied, version(1));
    assert_eq!(first.hook, "");
    let first_at = shim.reports[0].0;

    // A 150 ms hook is one hook_slow (§9.5.6).
    report_where(&mut shim, |r| r.hook_slow == 1);

    // A newer snapshot bound is reported without waiting for the period.
    shim.send(worker_down::Msg::Live(worker::Live {
        contract: "stsctl.v1".into(),
        payload: bindings(2).encode_to_vec(),
        digest: "live-2".into(),
    }));
    expect_applied(&mut shim, "live-2");
    let bound = report_where(&mut shim, |r| r.bindings_applied == version(2));
    assert_eq!(bound.hook_slow, 1);
    // Not newer: Applied again, and the version reported stays.
    shim.send(worker_down::Msg::Live(worker::Live {
        contract: "stsctl.v1".into(),
        payload: bindings(1).encode_to_vec(),
        digest: "live-1b".into(),
    }));
    expect_applied(&mut shim, "live-1b");

    // Quiet now. An unchanged Report is not sent on every poll; it is sent
    // again 5 s after the last one.
    let mut prev = shim.wait_report(|_| true);
    let gap = loop {
        let at = shim.reports.last().expect("a Report").0;
        let next = shim.wait_report(|_| true);
        if next == prev {
            break shim.reports.last().expect("a Report").0 - at;
        }
        assert!(at - first_at < Duration::from_secs(30), "never quiet");
        prev = next;
    };
    assert!(
        (Duration::from_millis(4900)..=Duration::from_millis(5500)).contains(&gap),
        "{gap:?} between two equal Reports"
    );
    assert_eq!(prev.bindings_applied, version(2));

    shim.send(worker_down::Msg::Stop(worker::Stop {
        grace_ms: 5000,
        reason: "operator".into(),
    }));
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit", "gate", "OnStart", "OnStop"]);
}

#[test]
fn reports_carry_event_log_only_while_it_is_on() {
    let nats = start_nats();
    let cases = [
        ("absent", None, None),
        (
            "off",
            Some(EventLog {
                enabled: false,
                keep_mb: 1,
            }),
            None,
        ),
        (
            "on",
            Some(EventLog {
                enabled: true,
                keep_mb: 1,
            }),
            Some(EventLogStat::default()),
        ),
    ];
    for (name, event_log, want) in cases {
        let mut shim = Shim::spawn(&nats.url);
        let mut payload = init_payload(&nats.creds, "");
        payload.event_log = event_log;
        running_with(&mut shim, payload);
        let report = shim.wait_report(|_| true);
        assert_eq!(report.event_log, want, "{name}");
        shim.send(worker_down::Msg::Stop(worker::Stop {
            grace_ms: 5000,
            reason: "operator".into(),
        }));
        expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
        assert!(
            shim.reports.iter().all(|(_, r)| r.event_log == want),
            "{name}"
        );
        let ended = shim.end();
        assert_eq!(ended.code, Some(0), "{name}: {}", ended.stderr);
    }
}

// ---------- endings ----------

#[test]
fn a_stop_waits_for_the_hook_then_delivers_td_before_on_stop() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats, "slow", 10);
    slow_hook_running(&mut shim);
    shim.send(worker_down::Msg::Stop(worker::Stop {
        grace_ms: 10_000,
        reason: "operator".into(),
    }));
    match shim.expect() {
        worker_up::Msg::Exiting(exiting) => {
            assert_slow_returned(&shim);
            assert_eq!(exiting.outcome, Outcome::Done as i32);
            assert_eq!(exiting.reason, "operator");
        }
        other => panic!("want Exiting, got {other:?}"),
    }
    match shim.expect() {
        worker_up::Msg::Stopped(s) => assert!(s.on_stop_completed),
        other => panic!("want Stopped, got {other:?}"),
    }
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert_eq!(
        hooks(&ended),
        ["OnInit", "gate", "OnStart", "Slow", "Queued", "OnStop"]
    );
}

#[test]
fn fd3_eof_waits_for_the_hook_then_delivers_td_before_on_stop() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats, "slow", 10);
    slow_hook_running(&mut shim);
    shim.sock
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown");
    match shim.expect() {
        worker_up::Msg::Exiting(exiting) => {
            assert_slow_returned(&shim);
            assert_eq!(exiting.outcome, Outcome::Done as i32);
            assert_eq!(exiting.reason, "shim closed");
        }
        other => panic!("want Exiting, got {other:?}"),
    }
    match shim.expect() {
        worker_up::Msg::Stopped(s) => assert!(s.on_stop_completed),
        other => panic!("want Stopped, got {other:?}"),
    }
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert_eq!(
        hooks(&ended),
        ["OnInit", "gate", "OnStart", "Slow", "Queued", "OnStop"]
    );
}

#[test]
fn exit_and_fail_from_a_hook_deliver_td_then_run_on_stop() {
    let nats = start_nats();
    for (scenario, outcome, reason, code) in [
        ("exit", Outcome::Done, "", 0),
        ("fail", Outcome::Failed, "asked to fail", 30),
    ] {
        let mut shim = Shim::spawn(&nats.url);
        running(&mut shim, &nats, scenario, 1);
        expect_ending(&mut shim, outcome, reason, Some(true));
        let ended = shim.end();
        assert_eq!(ended.code, Some(code), "{scenario}: {}", ended.stderr);
        assert_eq!(
            hooks(&ended),
            ["OnInit", "gate", "OnStart", "Hook", "Queued", "OnStop"],
            "{scenario}"
        );
    }
}

#[test]
fn command_fail_ends_failed_after_the_relay_echo() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats, "", 1);
    let cmd = StsCommand {
        cmd: Some(command::Cmd::Fail(Fail {
            reason: "venue gone".into(),
        })),
    };
    shim.send(worker_down::Msg::Relay(worker::Relay {
        contract: "stsctl.v1".into(),
        corr_id: b"corr-9".to_vec(),
        payload: cmd.encode_to_vec(),
    }));
    match shim.expect() {
        worker_up::Msg::Relay(relay) => assert_eq!(relay.corr_id, b"corr-9"),
        other => panic!("want the Relay echo, got {other:?}"),
    }
    expect_ending(&mut shim, Outcome::Failed, "venue gone", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(30), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit", "gate", "OnStart", "OnStop"]);
}

#[test]
fn a_hook_panic_is_crashed_with_on_stop_and_without_td() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats, "panic", 1);
    expect_ending(&mut shim, Outcome::Crashed, "hook panic: boom", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(20), "{}", ended.stderr);
    assert_eq!(
        hooks(&ended),
        ["OnInit", "gate", "OnStart", "Hook", "OnStop"]
    );
}

#[test]
fn an_on_start_error_is_init_failed_with_on_stop() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    hello(&mut shim);
    init(&shim, init_payload(&nats.creds, "start_error"), 5000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    start(&shim);
    expect_done(&mut shim, Verb::Start, false, "no venue");
    expect_ending(&mut shim, Outcome::InitFailed, "no venue", Some(true));
    // Report starts with Run, which this session never reached.
    assert!(shim.reports.is_empty(), "{:?}", shim.reports);
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit", "gate", "OnStart", "OnStop"]);
}

#[test]
fn hung_exits_21_while_the_hook_runs_without_on_stop_or_td() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats, "hang", 1);
    let started = Instant::now();
    expect_ending(&mut shim, Outcome::Hung, "hook timeout", Some(false));
    let ended = shim.end();
    // hook_timeout_s is 1; the hook sleeps 30 s and is not waited for.
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(ended.code, Some(21), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit", "gate", "OnStart", "Hook"]);
}

#[test]
fn a_full_td_buffer_is_failed_and_delivers_what_it_holds() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats, "flood", 10);
    expect_ending(&mut shim, Outcome::Failed, "td buffer full", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(30), "{}", ended.stderr);
    assert_eq!(
        hooks(&ended),
        [
            "OnInit".to_owned(),
            "gate".into(),
            "OnStart".into(),
            "Hook".into(),
            format!("OnStop {TD_CAP}"),
        ]
    );
    assert!(ended.stderr.contains("td buffer full"), "{}", ended.stderr);
}

#[test]
fn after_eof_an_on_stop_past_the_hook_budget_exits_21_without_stopped() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    running(&mut shim, &nats, "stuck_stop", 1);
    shim.sock
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown");
    let started = Instant::now();
    expect_ending(&mut shim, Outcome::Done, "shim closed", None);
    let ended = shim.end();
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(ended.code, Some(21), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit", "gate", "OnStart", "OnStop"]);
}
