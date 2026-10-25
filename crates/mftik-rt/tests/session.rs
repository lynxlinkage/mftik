//! `mftik_rt::run` end to end: a fake shim on a socketpair handed in as the
//! control fd, and a real nats-server with operator JWT auth (E03-05).
//!
//! Each test starts nats-server and re-runs this test binary as the worker,
//! filtered to `worker_entry`, with `MFTIK_RT_TEST_WORKER` set and the
//! worker's end of the socketpair at fd 3. The shim side reads every frame,
//! counts Beat, and checks the order of the rest and the exit code.
//!
//! nats-server comes from `MFTIK_NATS_SERVER`, else `PATH`. A test that
//! cannot find it fails, it does not skip. CI installs the pinned version
//! (see `.github/workflows/ci.yml`). The nats-server and shim helpers are in
//! `common`.

mod common;

use std::env;
use std::io::Write;
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use common::*;
use mftik_proto::wire::mftiko::common::v1::Outcome;
use mftik_proto::wire::mftiko::stsctl::v1::{Command as StsCommand, InitPayload};
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::{Context, HookError, Strategy};
use nkeys::KeyPair;
use prost::Message;

// ---------- the worker side ----------

/// The strategy the worker runs. `InitPayload.params` names the scenario.
#[derive(Default)]
struct Worker {
    scenario: String,
    thread: Option<ThreadId>,
}

impl Worker {
    fn line(&self, hook: &str) {
        let same = self.thread == Some(thread::current().id());
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "{{\"hook\":\"{hook}\",\"same_thread\":{same}}}");
        let _ = out.flush();
    }
}

impl Strategy for Worker {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        self.scenario = String::from_utf8(ctx.params().to_vec())?;
        self.thread = Some(thread::current().id());
        assert_eq!(ctx.session_id(), "c0ffee");
        mftik_rt::volume::write(ctx.volume_dir(), "state", b"saved")?;
        self.line("OnInit");
        if self.scenario == "init_error" {
            return Err("no such instrument".into());
        }
        Ok(())
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        self.line("OnStart");
        match self.scenario.as_str() {
            "self_exit" => ctx.after(Duration::from_millis(20), |s, ctx| {
                s.line("After");
                ctx.post(|s, ctx| {
                    s.line("Post");
                    ctx.exit();
                });
            }),
            "fail" => ctx.post(|_, ctx| ctx.fail("asked to fail")),
            "hang" => ctx.post(|s, _| {
                s.line("Hang");
                thread::sleep(Duration::from_secs(30));
            }),
            _ => {}
        }
        Ok(())
    }

    fn on_stop(&mut self, _ctx: &Context<Self>) {
        self.line("OnStop");
    }
}

/// The worker process's entry. In a normal test run the variable is unset
/// and this passes at once.
#[test]
fn worker_entry() {
    if env::var_os(WORKER_ENV).is_some() {
        mftik_rt::run(Worker::default(), Some(TEST_RELEASE));
    }
}

// ---------- the tests ----------

#[test]
fn hello_init_start_stop() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    up_to_running(&mut shim, &nats, "");
    // Live and Relay while running.
    shim.send(worker_down::Msg::Live(worker::Live {
        contract: "stsctl.v1".into(),
        payload: bindings(2).encode_to_vec(),
        digest: "live-2".into(),
    }));
    expect_applied(&mut shim, "live-2");
    shim.send(worker_down::Msg::Relay(worker::Relay {
        contract: "stsctl.v1".into(),
        corr_id: b"corr-1".to_vec(),
        payload: StsCommand::default().encode_to_vec(),
    }));
    match shim.expect() {
        worker_up::Msg::Relay(relay) => {
            assert_eq!(relay.corr_id, b"corr-1");
            assert_eq!(relay.contract, "stsctl.v1");
        }
        other => panic!("want the Relay echo, got {other:?}"),
    }
    // Beat goes on while the session runs.
    shim.wait_beat();
    shim.send(worker_down::Msg::Stop(worker::Stop {
        grace_ms: 5000,
        reason: "operator".into(),
    }));
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    assert_eq!(read_volume(&shim.volume.0), b"saved");
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit", "gate", "OnStart", "OnStop"]);
    assert_same_thread(&ended);
}

#[test]
fn a_zero_recv_window_or_hook_timeout_is_init_failed() {
    let nats = start_nats();
    for (edit, why) in [
        (
            (|p| p.recv_window_ms = 0) as fn(&mut InitPayload),
            "recv window",
        ),
        (|p| p.hook_timeout_s = 0, "hook timeout"),
    ] {
        let mut shim = Shim::spawn(&nats.url);
        hello(&mut shim);
        let mut payload = init_payload(&nats.creds, "");
        edit(&mut payload);
        init(&shim, payload, 5000);
        expect_done(&mut shim, Verb::Init, false, why);
        expect_ending(&mut shim, Outcome::InitFailed, why, Some(false));
        let ended = shim.end();
        assert_eq!(ended.code, Some(10), "{}", ended.stderr);
        assert!(hooks(&ended).is_empty());
    }
}

#[test]
fn an_on_init_error_is_init_failed() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    hello(&mut shim);
    init(&shim, init_payload(&nats.creds, "init_error"), 5000);
    expect_done(&mut shim, Verb::Init, false, "no such instrument");
    expect_ending(
        &mut shim,
        Outcome::InitFailed,
        "no such instrument",
        Some(false),
    );
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit"]);
}

#[test]
fn nats_is_retried_until_the_init_deadline() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    hello(&mut shim);
    // Another user key signs the nonce: the server refuses every dial.
    let mut creds = nats.creds.clone();
    creds.seed = KeyPair::new_user().seed().expect("seed");
    let started = Instant::now();
    init(&shim, init_payload(&creds, ""), 1500);
    expect_done(&mut shim, Verb::Init, false, "deadline");
    let took = started.elapsed();
    assert!(took >= Duration::from_millis(1400), "{took:?}");
    expect_ending(&mut shim, Outcome::InitFailed, "deadline", Some(false));
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    // Dials at 0 s and 1 s fail; the deadline ends the wait after them.
    assert!(
        ended.stderr.matches("nats connect failed").count() >= 2,
        "{}",
        ended.stderr
    );
    assert!(hooks(&ended).is_empty());
}

#[test]
fn sigterm_before_start_is_done_without_on_stop() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    hello(&mut shim);
    init(&shim, init_payload(&nats.creds, ""), 5000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    // SAFETY: kill(2) on our own child.
    assert_eq!(
        unsafe { libc::kill(shim.child.id() as i32, libc::SIGTERM) },
        0
    );
    expect_ending(&mut shim, Outcome::Done, "signal", Some(false));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit"]);
}

#[test]
fn sigterm_while_running_runs_on_stop() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    up_to_running(&mut shim, &nats, "");
    // SAFETY: kill(2) on our own child.
    assert_eq!(
        unsafe { libc::kill(shim.child.id() as i32, libc::SIGTERM) },
        0
    );
    expect_ending(&mut shim, Outcome::Done, "signal", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit", "gate", "OnStart", "OnStop"]);
}

#[test]
fn fd3_eof_while_running_is_a_graceful_stop() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    up_to_running(&mut shim, &nats, "");
    // The shim's write half goes away; it still reads the ending.
    shim.sock
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown");
    expect_ending(&mut shim, Outcome::Done, "shim closed", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit", "gate", "OnStart", "OnStop"]);
}

#[test]
fn the_strategy_ends_the_session_from_its_hooks() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    up_to_running(&mut shim, &nats, "self_exit");
    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert_eq!(
        hooks(&ended),
        ["OnInit", "gate", "OnStart", "After", "Post", "OnStop"]
    );
    assert_same_thread(&ended);

    let mut shim = Shim::spawn(&nats.url);
    up_to_running(&mut shim, &nats, "fail");
    expect_ending(&mut shim, Outcome::Failed, "asked to fail", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(30), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit", "gate", "OnStart", "OnStop"]);
}

#[test]
fn a_hook_past_hook_timeout_is_hung_and_exits_while_it_runs() {
    let nats = start_nats();
    let mut shim = Shim::spawn(&nats.url);
    up_to_running(&mut shim, &nats, "hang");
    let started = Instant::now();
    expect_ending(&mut shim, Outcome::Hung, "hook timeout", Some(false));
    // hook_timeout_s is 1; the hook sleeps 30 s and is not waited for.
    assert!(started.elapsed() < Duration::from_secs(10));
    let ended = shim.end();
    assert_eq!(ended.code, Some(21), "{}", ended.stderr);
    assert_eq!(hooks(&ended), ["OnInit", "gate", "OnStart", "Hang"]);
}
