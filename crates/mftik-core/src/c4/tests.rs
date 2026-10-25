//! C4 worker end, driven by an injected clock. Nothing sleeps and nothing does IO.
//!
//! `flow_*`, `deadline_*`, and `shutdown_*` follow the conformance checks of the same
//! names (PROTOCOL §9.8). The rest cover §5.2, §5.3, and §9.2 rule by rule.

use std::time::Duration;

use mftik_proto::wire::mftiko::common::v1::Outcome;
use mftik_proto::wire::mftiko::worker::v1::{
    self as worker, Controller, Hello, Verb, WorkerDown, WorkerUp, worker_down, worker_up,
};

use super::{
    CONTRACT_STS, CallError, Config, EXIT_CRASH, EXIT_DONE, EXIT_FAILED, EXIT_HUNG, EXIT_INIT,
    Effect, Machine, REASON_DEADLINE, REASON_INIT_AFTER_INIT, REASON_SHIM_CLOSED, REASON_SIGNAL,
    REASON_START_BEFORE_INIT, RunResult, SESSION_CONTRACTS, STAGE_RUNNING, TimerKind, TimerToken,
};
use crate::dispatch::{REASON_HUNG, RunEnd, StopError};

const NOW: i64 = 1_700_000_000_000_000_000;
const MS: i64 = 1_000_000;
const SEC: i64 = 1_000_000_000;
const HOOK: Duration = Duration::from_secs(1);
const OTHER: &str = "other.v1";

// ---------- WorkerDown ----------

fn down(msg: worker_down::Msg) -> WorkerDown {
    WorkerDown { msg: Some(msg) }
}

fn init_msg(digest: &str, deadline_ms: u32) -> WorkerDown {
    init_in(CONTRACT_STS, digest, deadline_ms)
}

fn init_in(contract: &str, digest: &str, deadline_ms: u32) -> WorkerDown {
    down(worker_down::Msg::Init(worker::Init {
        contract: contract.to_owned(),
        payload: b"init".to_vec(),
        deadline_ms,
        live_digest: digest.to_owned(),
    }))
}

fn start_msg(deadline_ms: u32) -> WorkerDown {
    start_in(CONTRACT_STS, deadline_ms)
}

fn start_in(contract: &str, deadline_ms: u32) -> WorkerDown {
    down(worker_down::Msg::Start(worker::Start {
        contract: contract.to_owned(),
        payload: b"start".to_vec(),
        deadline_ms,
    }))
}

fn stop_msg(reason: &str, grace_ms: u32) -> WorkerDown {
    down(worker_down::Msg::Stop(worker::Stop {
        reason: reason.to_owned(),
        grace_ms,
    }))
}

fn live_msg(digest: &str, payload: &str) -> WorkerDown {
    live_in(CONTRACT_STS, digest, payload)
}

fn live_in(contract: &str, digest: &str, payload: &str) -> WorkerDown {
    down(worker_down::Msg::Live(worker::Live {
        contract: contract.to_owned(),
        payload: payload.as_bytes().to_vec(),
        digest: digest.to_owned(),
    }))
}

fn relay_msg(contract: &str, corr: &str, payload: &str) -> WorkerDown {
    down(worker_down::Msg::Relay(worker::Relay {
        contract: contract.to_owned(),
        corr_id: corr.as_bytes().to_vec(),
        payload: payload.as_bytes().to_vec(),
    }))
}

fn controller_msg(attached: bool, epoch: i64) -> WorkerDown {
    down(worker_down::Msg::Controller(Controller { attached, epoch }))
}

// ---------- Effects ----------

fn up(msg: worker_up::Msg, fatal: bool) -> Effect {
    Effect::Send {
        msg: WorkerUp { msg: Some(msg) },
        fatal,
    }
}

fn done(verb: Verb, ok: bool, error: &str) -> Effect {
    up(
        worker_up::Msg::Done(worker::Done {
            verb: verb as i32,
            ok,
            error: error.to_owned(),
        }),
        ok,
    )
}

fn applied(digest: &str, fatal: bool) -> Effect {
    up(
        worker_up::Msg::Applied(worker::Applied {
            digest: digest.to_owned(),
        }),
        fatal,
    )
}

fn stage() -> Effect {
    up(
        worker_up::Msg::Stage(worker::Stage {
            name: STAGE_RUNNING.to_owned(),
        }),
        true,
    )
}

fn exiting(outcome: Outcome, reason: &str) -> Effect {
    up(
        worker_up::Msg::Exiting(worker::Exiting {
            outcome: outcome as i32,
            reason: reason.to_owned(),
            detail: String::new(),
        }),
        false,
    )
}

fn stopped(on_stop_completed: bool) -> Effect {
    up(
        worker_up::Msg::Stopped(worker::Stopped { on_stop_completed }),
        false,
    )
}

fn echo(corr: &str, payload: &str) -> Effect {
    up(
        worker_up::Msg::Relay(worker::Relay {
            contract: CONTRACT_STS.to_owned(),
            corr_id: corr.as_bytes().to_vec(),
            payload: payload.as_bytes().to_vec(),
        }),
        false,
    )
}

fn call_init(deadline_ns: Option<i64>) -> Effect {
    Effect::CallInit {
        payload: b"init".to_vec(),
        deadline_ns,
    }
}

fn call_start(deadline_ns: Option<i64>) -> Effect {
    Effect::CallStart {
        payload: b"start".to_vec(),
        deadline_ns,
    }
}

fn call_live(payload: &str) -> Effect {
    Effect::CallLive {
        payload: payload.as_bytes().to_vec(),
    }
}

fn call_relay(corr: &str, payload: &str) -> Effect {
    Effect::CallRelay {
        corr_id: corr.as_bytes().to_vec(),
        payload: payload.as_bytes().to_vec(),
    }
}

fn on_stop(deadline_ns: Option<i64>) -> Effect {
    Effect::CallOnStop { deadline_ns }
}

fn log(line: &str) -> Effect {
    Effect::Log(line.to_owned())
}

fn arm(kind: TimerKind, after: Duration, deadline_ns: i64, effect: &Effect) -> TimerToken {
    match effect {
        Effect::ArmTimer {
            token,
            after: got_after,
            deadline_ns: got_deadline,
        } => {
            assert_eq!(token.kind, kind);
            assert_eq!(*got_after, after);
            assert_eq!(*got_deadline, deadline_ns);
            *token
        }
        other => panic!("want ArmTimer {kind:?}, got {other:?}"),
    }
}

/// The INIT_FAILED ending of an Init or Start the worker will not run, with no OnStop.
fn refused(verb: Verb, why: &str) -> Vec<Effect> {
    let what = if verb == Verb::Init { "init" } else { "start" };
    vec![
        log(&format!("{what} failed: {why}")),
        done(verb, false, why),
        exiting(Outcome::InitFailed, why),
        stopped(false),
        Effect::Exit(EXIT_INIT),
    ]
}

fn has_send(effects: &[Effect], want: fn(&worker_up::Msg) -> bool) -> bool {
    effects.iter().any(|effect| match effect {
        Effect::Send {
            msg: WorkerUp { msg: Some(msg) },
            ..
        } => want(msg),
        _ => false,
    })
}

// ---------- Setup ----------

fn machine() -> Machine {
    let mut m = Machine::new(Config::session("r1", 4242));
    assert_eq!(m.hello().len(), 2);
    m
}

/// Init with live digest `L1` succeeded; `Done(INIT, ok)` has been sent.
fn initialized(hook: Duration) -> Machine {
    let mut m = machine();
    assert_eq!(m.on_down(NOW, init_msg("L1", 0)), vec![call_init(None)]);
    assert_eq!(
        m.init_returned(NOW, Ok(()), hook),
        vec![applied("L1", true), done(Verb::Init, true, "")]
    );
    m
}

/// `Done(START, ok)` and `Stage(running)` have been sent; Run is outstanding.
fn running(hook: Duration) -> Machine {
    let mut m = initialized(hook);
    assert_eq!(m.on_down(NOW, start_msg(0)), vec![call_start(None)]);
    assert_eq!(
        m.start_returned(NOW, Ok(())),
        vec![done(Verb::Start, true, ""), stage(), Effect::CallRun]
    );
    m
}

// ---------- Hello ----------

#[test]
fn hello_names_the_session_and_starts_beat() {
    let mut m = Machine::new(Config::session("v0.12.0", 4242));
    let hello = Hello {
        kind: "session".to_owned(),
        release: "v0.12.0".to_owned(),
        contracts: vec![
            "worker.v1".to_owned(),
            "stsctl.v1".to_owned(),
            "md.v1".to_owned(),
            "td.v1".to_owned(),
        ],
        pid: 4242,
    };
    assert_eq!(
        m.hello(),
        vec![up(worker_up::Msg::Hello(hello), true), Effect::StartBeat]
    );
    assert!(m.hello().is_empty());
    assert_eq!(
        SESSION_CONTRACTS,
        ["worker.v1", "stsctl.v1", "md.v1", "td.v1"]
    );
}

#[test]
fn hello_without_contracts_lists_the_contract() {
    let mut m = Machine::new(Config {
        kind: "conn".to_owned(),
        contract: "mdctl.v1".to_owned(),
        contracts: Vec::new(),
        release: String::new(),
        pid: 7,
        init_carries_live: false,
    });
    let hello = Hello {
        kind: "conn".to_owned(),
        release: String::new(),
        contracts: vec!["mdctl.v1".to_owned()],
        pid: 7,
    };
    assert_eq!(
        m.hello(),
        vec![up(worker_up::Msg::Hello(hello), true), Effect::StartBeat]
    );
}

// ---------- flow (PROTOCOL §9.8) ----------

#[test]
fn flow_start_before_init_is_init_failed() {
    let mut m = machine();
    assert_eq!(
        m.on_down(NOW, start_msg(0)),
        refused(Verb::Start, REASON_START_BEFORE_INIT)
    );
    assert_eq!(m.exit_code(), Some(EXIT_INIT));
    assert!(m.on_down(NOW, init_msg("L1", 0)).is_empty());
}

#[test]
fn flow_second_init_is_init_failed() {
    let mut m = initialized(HOOK);
    assert_eq!(
        m.on_down(NOW, init_msg("L1", 0)),
        refused(Verb::Init, REASON_INIT_AFTER_INIT)
    );
    assert_eq!(m.exit_code(), Some(EXIT_INIT));
}

#[test]
fn flow_stop_before_start_is_done_without_hooks() {
    let mut m = initialized(HOOK);
    assert_eq!(
        m.on_down(NOW, stop_msg("", 5000)),
        vec![
            Effect::StopBeat,
            exiting(Outcome::Done, REASON_SIGNAL),
            stopped(false),
            Effect::Exit(EXIT_DONE),
        ]
    );
    assert!(m.on_down(NOW, start_msg(0)).is_empty());
}

#[test]
fn flow_happy_path() {
    let mut m = machine();
    // A Live before Init is held. A Relay before Init is dropped.
    assert!(m.on_down(NOW, live_msg("L0", "early")).is_empty());
    assert_eq!(m.on_down(NOW, init_msg("L1", 0)), vec![call_init(None)]);
    assert!(
        m.on_down(NOW, relay_msg(CONTRACT_STS, "c0", "x"))
            .is_empty()
    );
    // Applied(live_digest), then the held Live, then Done(INIT, ok).
    assert_eq!(
        m.init_returned(NOW, Ok(()), HOOK),
        vec![applied("L1", true), call_live("early")]
    );
    assert_eq!(
        m.live_returned(NOW, Ok(())),
        vec![applied("L0", false), done(Verb::Init, true, "")]
    );
    assert_eq!(m.on_down(NOW, start_msg(0)), vec![call_start(None)]);
    assert_eq!(
        m.start_returned(NOW, Ok(())),
        vec![done(Verb::Start, true, ""), stage(), Effect::CallRun]
    );
    // Relay goes back up with its corr unchanged, then to the plane.
    assert_eq!(
        m.on_down(NOW, relay_msg(CONTRACT_STS, "ping", "")),
        vec![echo("ping", ""), call_relay("ping", "")]
    );
    assert_eq!(m.on_down(NOW, live_msg("L2", "b2")), vec![call_live("b2")]);
    assert_eq!(m.live_returned(NOW, Ok(())), vec![applied("L2", false)]);
    // Stop: Exiting, OnStop with the rest of the grace, Stopped, exit 0.
    let read = NOW + 10 * SEC;
    assert_eq!(
        m.on_down(read, stop_msg("ended_by_spec", 5000)),
        vec![Effect::StopBeat, Effect::Cancel]
    );
    assert_eq!(
        m.run_returned(read + SEC, RunResult::Returned),
        vec![
            exiting(Outcome::Done, "ended_by_spec"),
            on_stop(Some(read + 5 * SEC)),
        ]
    );
    assert_eq!(
        m.on_stop_returned(read + 2 * SEC, Ok(())),
        vec![stopped(true), Effect::Exit(EXIT_DONE)]
    );
    assert_eq!(m.exit_code(), Some(EXIT_DONE));
}

// ---------- deadline (PROTOCOL §9.8, E08-D007, E08-D008) ----------

#[test]
fn deadline_init_does_not_wait() {
    let mut m = machine();
    let fx = m.on_down(NOW, init_msg("L1", 200));
    assert_eq!(fx.len(), 2);
    let token = arm(
        TimerKind::InitDeadline,
        Duration::from_millis(200),
        NOW + 200 * MS,
        &fx[0],
    );
    assert_eq!(fx[1], call_init(Some(NOW + 200 * MS)));
    assert_eq!(
        m.on_timer(NOW + 200 * MS, token),
        refused(Verb::Init, REASON_DEADLINE)
    );
    // The hook returns later. Its result is not used.
    assert!(m.init_returned(NOW + 30 * SEC, Ok(()), HOOK).is_empty());
    assert_eq!(m.exit_code(), Some(EXIT_INIT));
}

#[test]
fn deadline_start_does_not_wait_or_run_on_stop() {
    let mut m = initialized(HOOK);
    let fx = m.on_down(NOW, start_msg(200));
    let token = arm(
        TimerKind::StartDeadline,
        Duration::from_millis(200),
        NOW + 200 * MS,
        &fx[0],
    );
    assert_eq!(fx[1..], [call_start(Some(NOW + 200 * MS))]);
    assert_eq!(
        m.on_timer(NOW + 200 * MS, token),
        refused(Verb::Start, REASON_DEADLINE)
    );
    assert!(m.start_returned(NOW + 30 * SEC, Ok(())).is_empty());
    assert!(m.on_stop_returned(NOW + 30 * SEC, Ok(())).is_empty());
}

#[test]
fn deadline_hook_that_notices_is_still_the_deadline() {
    // The plane saw its context end past the deadline and returned before the timer did.
    let mut m = initialized(HOOK);
    let fx = m.on_down(NOW, start_msg(200));
    let token = arm(
        TimerKind::StartDeadline,
        Duration::from_millis(200),
        NOW + 200 * MS,
        &fx[0],
    );
    assert_eq!(
        m.start_returned(NOW + 200 * MS, Err(CallError::Cancelled)),
        refused(Verb::Start, REASON_DEADLINE)
    );
    assert!(m.on_timer(NOW + 200 * MS, token).is_empty());

    let mut m = machine();
    m.on_down(NOW, init_msg("L1", 50));
    assert_eq!(
        m.init_returned(NOW + 60 * MS, Err(CallError::Cancelled), HOOK),
        refused(Verb::Init, REASON_DEADLINE)
    );
}

#[test]
fn deadline_result_before_the_timer_wins() {
    // Success that lands before the timer is success, even past the deadline instant.
    let mut m = initialized(HOOK);
    let fx = m.on_down(NOW, start_msg(200));
    let token = arm(
        TimerKind::StartDeadline,
        Duration::from_millis(200),
        NOW + 200 * MS,
        &fx[0],
    );
    assert_eq!(
        m.start_returned(NOW + 250 * MS, Ok(())),
        vec![done(Verb::Start, true, ""), stage(), Effect::CallRun]
    );
    assert!(m.on_timer(NOW + 250 * MS, token).is_empty());

    // Any other error is that error, and a Start failure runs OnStop (E08-D008).
    let mut m = initialized(HOOK);
    m.on_down(NOW, start_msg(200));
    assert_eq!(
        m.start_returned(NOW + 250 * MS, Err(CallError::Failed("no feed".to_owned()))),
        vec![
            log("start failed: no feed"),
            done(Verb::Start, false, "no feed"),
            exiting(Outcome::InitFailed, "no feed"),
            on_stop(None),
        ]
    );
}

#[test]
fn deadline_zero_is_no_limit() {
    let mut m = machine();
    assert_eq!(m.on_down(NOW, init_msg("L1", 0)), vec![call_init(None)]);
    // A Stop while Init blocks is a stop, not INIT_FAILED, and OnStop does not run.
    assert_eq!(
        m.on_down(NOW + 60 * SEC, stop_msg("ended_by_spec", 5000)),
        vec![Effect::StopBeat, Effect::Cancel]
    );
    assert_eq!(
        m.init_returned(NOW + 61 * SEC, Err(CallError::Cancelled), HOOK),
        vec![
            exiting(Outcome::Done, "ended_by_spec"),
            stopped(false),
            Effect::Exit(EXIT_DONE),
        ]
    );
}

#[test]
fn deadline_is_dropped_once_a_stop_is_read() {
    // Go waits for Start once its parent context has ended; the deadline no longer applies.
    let mut m = initialized(HOOK);
    let fx = m.on_down(NOW, start_msg(30_000));
    let token = arm(
        TimerKind::StartDeadline,
        Duration::from_secs(30),
        NOW + 30 * SEC,
        &fx[0],
    );
    let read = NOW + SEC;
    assert_eq!(
        m.on_down(read, stop_msg("deps_failed", 5000)),
        vec![Effect::StopBeat, Effect::Cancel]
    );
    assert!(m.on_timer(NOW + 30 * SEC, token).is_empty());
    assert_eq!(
        m.start_returned(NOW + 31 * SEC, Err(CallError::Cancelled)),
        vec![
            exiting(Outcome::Done, "deps_failed"),
            on_stop(Some(read + 5 * SEC)),
        ]
    );
    assert_eq!(
        m.on_stop_returned(NOW + 32 * SEC, Ok(())),
        vec![stopped(true), Effect::Exit(EXIT_DONE)]
    );

    let mut m = machine();
    let fx = m.on_down(NOW, init_msg("L1", 200));
    let token = arm(
        TimerKind::InitDeadline,
        Duration::from_millis(200),
        NOW + 200 * MS,
        &fx[0],
    );
    assert_eq!(
        m.on_sigterm(NOW + MS),
        vec![Effect::StopBeat, Effect::Cancel]
    );
    assert!(m.on_timer(NOW + 200 * MS, token).is_empty());
    assert_eq!(
        m.init_returned(NOW + SEC, Err(CallError::Failed("nats".to_owned())), HOOK),
        vec![
            exiting(Outcome::Done, REASON_SIGNAL),
            stopped(false),
            Effect::Exit(EXIT_DONE),
        ]
    );
}

#[test]
fn deadline_timer_with_another_token_is_ignored() {
    let mut m = machine();
    let fx = m.on_down(NOW, init_msg("L1", 200));
    let token = arm(
        TimerKind::InitDeadline,
        Duration::from_millis(200),
        NOW + 200 * MS,
        &fx[0],
    );
    let stale = TimerToken {
        id: token.id + 1,
        ..token
    };
    assert!(m.on_timer(NOW + 200 * MS, stale).is_empty());
    let other = TimerToken {
        kind: TimerKind::StartDeadline,
        ..token
    };
    assert!(m.on_timer(NOW + 200 * MS, other).is_empty());
    assert_eq!(
        m.on_timer(NOW + 200 * MS, token),
        refused(Verb::Init, REASON_DEADLINE)
    );
}

// ---------- shutdown (PROTOCOL §9.8, §5.3, E08-D003, E08-D009) ----------

#[test]
fn shutdown_stop_sends_exiting_before_stopped() {
    let mut m = running(HOOK);
    let fx = m.on_down(NOW, stop_msg("", 5000));
    assert_eq!(fx, vec![Effect::StopBeat, Effect::Cancel]);
    let mut fx = m.run_returned(NOW + SEC, RunResult::Returned);
    assert_eq!(
        fx,
        vec![
            exiting(Outcome::Done, REASON_SIGNAL),
            on_stop(Some(NOW + 5 * SEC))
        ]
    );
    // The hook budget is not used: a Stop's grace wins, and OnStop is waited for.
    assert!(!fx.iter().any(|e| matches!(e, Effect::ArmTimer { .. })));
    fx = m.on_stop_returned(NOW + 9 * SEC, Ok(()));
    assert_eq!(fx, vec![stopped(true), Effect::Exit(EXIT_DONE)]);
}

#[test]
fn shutdown_self_end_on_stop_past_hook_budget_is_exit_21() {
    let mut m = running(HOOK);
    let end = NOW + 3 * SEC;
    let fx = m.run_returned(end, RunResult::Returned);
    assert_eq!(fx.len(), 3);
    assert_eq!(fx[0], exiting(Outcome::Done, ""));
    let token = arm(TimerKind::OnStopBudget, HOOK, end + SEC, &fx[1]);
    assert_eq!(fx[2], on_stop(Some(end + SEC)));
    // Past the budget: no Stopped, exit 21, Exiting unchanged.
    assert_eq!(m.on_timer(end + SEC, token), vec![Effect::Exit(EXIT_HUNG)]);
    assert!(m.on_stop_returned(end + 30 * SEC, Ok(())).is_empty());
    assert_eq!(m.exit_code(), Some(EXIT_HUNG));
}

#[test]
fn shutdown_self_end_on_stop_that_notices_the_budget_is_exit_21() {
    let mut m = running(HOOK);
    let fx = m.run_returned(NOW, RunResult::Returned);
    let token = arm(TimerKind::OnStopBudget, HOOK, NOW + SEC, &fx[1]);
    assert_eq!(
        m.on_stop_returned(NOW + SEC, Err(CallError::Cancelled)),
        vec![Effect::Exit(EXIT_HUNG)]
    );
    assert!(m.on_timer(NOW + SEC, token).is_empty());
}

#[test]
fn shutdown_self_end_within_budget() {
    let mut m = running(Duration::from_secs(5));
    let fx = m.run_returned(NOW, RunResult::Returned);
    let token = arm(
        TimerKind::OnStopBudget,
        Duration::from_secs(5),
        NOW + 5 * SEC,
        &fx[1],
    );
    assert_eq!(fx[2], on_stop(Some(NOW + 5 * SEC)));
    assert_eq!(
        m.on_stop_returned(NOW + SEC, Ok(())),
        vec![stopped(true), Effect::Exit(EXIT_DONE)]
    );
    assert!(m.on_timer(NOW + 5 * SEC, token).is_empty());

    // An error inside the budget is Stopped(false); the code stays.
    let mut m = running(Duration::from_secs(5));
    m.run_returned(NOW, RunResult::Returned);
    assert_eq!(
        m.on_stop_returned(NOW + SEC, Err(CallError::Failed("disk".to_owned()))),
        vec![
            log("on stop: disk"),
            stopped(false),
            Effect::Exit(EXIT_DONE)
        ]
    );
}

#[test]
fn shutdown_zero_hook_timeout_is_no_limit() {
    let mut m = running(Duration::ZERO);
    assert_eq!(
        m.run_returned(NOW, RunResult::Returned),
        vec![exiting(Outcome::Done, ""), on_stop(None)]
    );
}

#[test]
fn shutdown_stop_during_self_end_on_stop_keeps_the_budget() {
    let mut m = running(HOOK);
    let fx = m.run_returned(NOW, RunResult::Returned);
    let token = arm(TimerKind::OnStopBudget, HOOK, NOW + SEC, &fx[1]);
    // Beat stops with the Stop, as in Go; the OnStop already running keeps its budget.
    assert_eq!(
        m.on_down(NOW + 100 * MS, stop_msg("bye", 5000)),
        vec![Effect::StopBeat]
    );
    assert_eq!(m.on_timer(NOW + SEC, token), vec![Effect::Exit(EXIT_HUNG)]);
}

#[test]
fn shutdown_grace_counts_from_the_first_stop() {
    let mut m = running(HOOK);
    let first = NOW + SEC;
    assert_eq!(
        m.on_down(first, stop_msg("bye", 5000)),
        vec![Effect::StopBeat, Effect::Cancel]
    );
    // A later Stop changes neither the deadline nor the reason.
    assert!(
        m.on_down(first + 2 * SEC, stop_msg("again", 60_000))
            .is_empty()
    );
    assert!(m.on_down(first + 3 * SEC, stop_msg("again", 0)).is_empty());
    assert_eq!(
        m.run_returned(first + 4 * SEC, RunResult::Returned),
        vec![
            exiting(Outcome::Done, "bye"),
            on_stop(Some(first + 5 * SEC))
        ]
    );
}

#[test]
fn shutdown_grace_zero_is_already_due() {
    let mut m = running(HOOK);
    m.on_down(NOW, stop_msg("ended_by_spec", 0));
    assert_eq!(
        m.run_returned(NOW + MS, RunResult::Returned),
        vec![exiting(Outcome::Done, "ended_by_spec"), on_stop(Some(NOW))]
    );
    // OnStop sees an expired context and returns; that is not HUNG.
    assert_eq!(
        m.on_stop_returned(NOW + 2 * MS, Err(CallError::Cancelled)),
        vec![
            log("on stop: context canceled"),
            stopped(false),
            Effect::Exit(EXIT_DONE),
        ]
    );
}

#[test]
fn shutdown_sigterm_is_a_stop_without_grace() {
    let mut m = running(Duration::from_millis(50));
    assert_eq!(m.on_sigterm(NOW), vec![Effect::StopBeat, Effect::Cancel]);
    assert!(m.on_sigterm(NOW).is_empty());
    assert_eq!(
        m.run_returned(NOW + SEC, RunResult::Returned),
        vec![exiting(Outcome::Done, REASON_SIGNAL), on_stop(None)]
    );
    assert_eq!(
        m.on_stop_returned(NOW + 2 * SEC, Ok(())),
        vec![stopped(true), Effect::Exit(EXIT_DONE)]
    );
}

#[test]
fn shutdown_stop_after_sigterm_names_the_reason_and_grace() {
    // Go's SIGTERM cancels the context without recording a reason; the Stop that follows does.
    let mut m = running(HOOK);
    m.on_sigterm(NOW);
    assert!(m.on_down(NOW + SEC, stop_msg("bye", 5000)).is_empty());
    assert_eq!(
        m.run_returned(NOW + 2 * SEC, RunResult::Returned),
        vec![exiting(Outcome::Done, "bye"), on_stop(Some(NOW + 6 * SEC))]
    );
}

#[test]
fn shutdown_first_halt_names_the_reason() {
    // Stop, then EOF: the Stop's reason.
    let mut m = running(HOOK);
    m.on_down(NOW, stop_msg("bye", 1000));
    assert!(m.on_eof(NOW).is_empty());
    assert_eq!(
        m.run_returned(NOW, RunResult::Returned)[0],
        exiting(Outcome::Done, "bye")
    );

    // A Stop with no reason, then EOF: still the Stop, which reads as "signal".
    let mut m = running(HOOK);
    m.on_down(NOW, stop_msg("", 1000));
    m.on_eof(NOW);
    assert_eq!(
        m.run_returned(NOW, RunResult::Returned)[0],
        exiting(Outcome::Done, REASON_SIGNAL)
    );
}

#[test]
fn shutdown_eof_is_a_graceful_stop() {
    // PROTOCOL §4.5 S2: EOF is a graceful stop. Unlike Go, OnStop takes the hook budget,
    // because nothing outside the worker can end it any more (§9.5.5, E03-D027).
    let mut m = running(HOOK);
    assert_eq!(m.on_eof(NOW), vec![Effect::StopBeat, Effect::Cancel]);
    let end = NOW + 2 * SEC;
    let fx = m.run_returned(end, RunResult::Returned);
    assert_eq!(fx.len(), 3);
    assert_eq!(fx[0], exiting(Outcome::Done, REASON_SHIM_CLOSED));
    let token = arm(TimerKind::OnStopBudget, HOOK, end + SEC, &fx[1]);
    assert_eq!(fx[2], on_stop(Some(end + SEC)));
    // Past the budget: exit 21, no Stopped, Exiting unchanged.
    assert_eq!(m.on_timer(end + SEC, token), vec![Effect::Exit(EXIT_HUNG)]);
    assert!(m.on_stop_returned(end + 30 * SEC, Ok(())).is_empty());
    assert_eq!(m.exit_code(), Some(EXIT_HUNG));

    let mut m = machine();
    assert_eq!(
        m.on_eof(NOW),
        vec![
            Effect::StopBeat,
            exiting(Outcome::Done, REASON_SHIM_CLOSED),
            stopped(false),
            Effect::Exit(EXIT_DONE),
        ]
    );
}

#[test]
fn shutdown_eof_on_stop_within_budget() {
    let mut m = running(HOOK);
    m.on_eof(NOW);
    let fx = m.run_returned(NOW, RunResult::Returned);
    let token = arm(TimerKind::OnStopBudget, HOOK, NOW + SEC, &fx[1]);
    assert_eq!(
        m.on_stop_returned(NOW + SEC / 2, Ok(())),
        vec![stopped(true), Effect::Exit(EXIT_DONE)]
    );
    assert!(m.on_timer(NOW + SEC, token).is_empty());

    // OnStop that notices the budget ran out: exit 21.
    let mut m = running(HOOK);
    m.on_eof(NOW);
    m.run_returned(NOW, RunResult::Returned);
    assert_eq!(
        m.on_stop_returned(NOW + SEC, Err(CallError::Cancelled)),
        vec![Effect::Exit(EXIT_HUNG)]
    );
}

#[test]
fn shutdown_eof_after_sigterm_bounds_the_running_on_stop() {
    // SIGTERM has no grace, so OnStop starts unbounded; EOF while it runs arms the budget.
    let mut m = running(HOOK);
    m.on_sigterm(NOW);
    assert_eq!(
        m.run_returned(NOW, RunResult::Returned),
        vec![exiting(Outcome::Done, REASON_SIGNAL), on_stop(None)]
    );
    let gone = NOW + 2 * SEC;
    let fx = m.on_eof(gone);
    assert_eq!(fx.len(), 1);
    let token = arm(TimerKind::OnStopBudget, HOOK, gone + SEC, &fx[0]);
    assert_eq!(m.on_timer(gone + SEC, token), vec![Effect::Exit(EXIT_HUNG)]);
    assert!(m.on_eof(gone + 2 * SEC).is_empty());
}

#[test]
fn shutdown_eof_after_stop_keeps_the_grace() {
    // A Stop was read: grace stays OnStop's deadline. Once the shim is gone, the worker
    // ends the process when that grace runs out, as the shim would have.
    let mut m = running(HOOK);
    m.on_down(NOW, stop_msg("bye", 5000));
    assert_eq!(
        m.run_returned(NOW, RunResult::Returned),
        vec![exiting(Outcome::Done, "bye"), on_stop(Some(NOW + 5 * SEC))]
    );
    let fx = m.on_eof(NOW + SEC);
    assert_eq!(fx.len(), 1);
    let token = arm(
        TimerKind::OnStopBudget,
        Duration::from_secs(4),
        NOW + 5 * SEC,
        &fx[0],
    );
    assert_eq!(
        m.on_timer(NOW + 5 * SEC, token),
        vec![Effect::Exit(EXIT_HUNG)]
    );

    // EOF before OnStop starts: the budget is armed with OnStop, at the end of grace.
    let mut m = running(HOOK);
    m.on_down(NOW, stop_msg("bye", 5000));
    assert!(m.on_eof(NOW).is_empty());
    let fx = m.run_returned(NOW + SEC, RunResult::Returned);
    assert_eq!(fx.len(), 3);
    assert_eq!(fx[0], exiting(Outcome::Done, "bye"));
    arm(
        TimerKind::OnStopBudget,
        Duration::from_secs(4),
        NOW + 5 * SEC,
        &fx[1],
    );
    assert_eq!(fx[2], on_stop(Some(NOW + 5 * SEC)));
}

#[test]
fn shutdown_eof_with_zero_hook_timeout_is_no_limit() {
    let mut m = running(Duration::ZERO);
    m.on_eof(NOW);
    assert_eq!(
        m.run_returned(NOW, RunResult::Returned),
        vec![exiting(Outcome::Done, REASON_SHIM_CLOSED), on_stop(None)]
    );
}

// ---------- Stop, SIGTERM before Start (E08-D037) ----------

#[test]
fn stop_before_init_ends_done_without_hooks() {
    let mut m = machine();
    assert_eq!(
        m.on_down(NOW, stop_msg("deps_failed", 5000)),
        vec![
            Effect::StopBeat,
            exiting(Outcome::Done, "deps_failed"),
            stopped(false),
            Effect::Exit(EXIT_DONE),
        ]
    );
    let mut m = initialized(HOOK);
    assert_eq!(
        m.on_sigterm(NOW),
        vec![
            Effect::StopBeat,
            exiting(Outcome::Done, REASON_SIGNAL),
            stopped(false),
            Effect::Exit(EXIT_DONE),
        ]
    );
}

#[test]
fn stop_during_init_that_succeeds_still_reports_init() {
    // Go: Done(INIT, ok), then waitStart sees the stop. No OnStart, no OnStop.
    let mut m = machine();
    m.on_down(NOW, init_msg("L1", 0));
    m.on_down(NOW, stop_msg("bye", 5000));
    assert_eq!(
        m.init_returned(NOW, Ok(()), HOOK),
        vec![
            applied("L1", true),
            done(Verb::Init, true, ""),
            exiting(Outcome::Done, "bye"),
            stopped(false),
            Effect::Exit(EXIT_DONE),
        ]
    );
}

#[test]
fn stop_during_start_runs_on_stop() {
    // Init succeeded, so the stop cleans up. No Done(START).
    let mut m = initialized(HOOK);
    m.on_down(NOW, start_msg(0));
    assert_eq!(
        m.on_down(NOW, stop_msg("deps_failed", 5000)),
        vec![Effect::StopBeat, Effect::Cancel]
    );
    assert_eq!(
        m.start_returned(NOW + SEC, Ok(())),
        vec![
            exiting(Outcome::Done, "deps_failed"),
            on_stop(Some(NOW + 5 * SEC))
        ]
    );
    assert_eq!(
        m.on_stop_returned(NOW + 2 * SEC, Ok(())),
        vec![stopped(true), Effect::Exit(EXIT_DONE)]
    );
}

// ---------- Init and Start failures (PROTOCOL §5.2, §5.3) ----------

#[test]
fn init_failure_does_not_run_on_stop() {
    let mut m = machine();
    m.on_down(NOW, init_msg("L1", 0));
    assert_eq!(
        m.init_returned(
            NOW,
            Err(CallError::Failed("unknown venue X".to_owned())),
            HOOK
        ),
        refused(Verb::Init, "unknown venue X")
    );
}

#[test]
fn init_in_another_contract_is_refused_without_a_call() {
    let mut m = machine();
    assert_eq!(
        m.on_down(NOW, init_in(OTHER, "L1", 0)),
        refused(Verb::Init, "contract other.v1")
    );
    // Init must name the contract; empty is refused too (Go).
    let mut m = machine();
    assert_eq!(
        m.on_down(NOW, init_in("", "L1", 0)),
        refused(Verb::Init, "contract ")
    );
}

#[test]
fn start_failure_runs_on_stop_without_a_deadline() {
    // The hook budget is for a self-end, not for a Start failure (E08-D009).
    let mut m = initialized(HOOK);
    m.on_down(NOW, start_msg(0));
    assert_eq!(
        m.start_returned(NOW, Err(CallError::Failed("hedge_mode".to_owned()))),
        vec![
            log("start failed: hedge_mode"),
            done(Verb::Start, false, "hedge_mode"),
            exiting(Outcome::InitFailed, "hedge_mode"),
            on_stop(None),
        ]
    );
    assert_eq!(
        m.on_stop_returned(NOW, Ok(())),
        vec![stopped(true), Effect::Exit(EXIT_INIT)]
    );

    let mut m = initialized(HOOK);
    m.on_down(NOW, start_msg(0));
    m.start_returned(NOW, Err(CallError::Failed("hedge_mode".to_owned())));
    assert_eq!(
        m.on_stop_returned(NOW, Err(CallError::Failed("disk".to_owned()))),
        vec![
            log("on stop: disk"),
            stopped(false),
            Effect::Exit(EXIT_INIT)
        ]
    );
}

#[test]
fn start_contract() {
    // Another contract fails Start after Init succeeded, so OnStop runs.
    let mut m = initialized(HOOK);
    assert_eq!(
        m.on_down(NOW, start_in(OTHER, 0)),
        vec![
            log("start failed: contract other.v1"),
            done(Verb::Start, false, "contract other.v1"),
            exiting(Outcome::InitFailed, "contract other.v1"),
            on_stop(None),
        ]
    );
    // An empty contract is accepted.
    let mut m = initialized(HOOK);
    assert_eq!(m.on_down(NOW, start_in("", 0)), vec![call_start(None)]);
}

// ---------- Run endings (PROTOCOL §5.3) ----------

#[test]
fn run_endings_and_exit_codes() {
    let cases = [
        (
            RunResult::Error("connection loop: boom".to_owned()),
            Outcome::Crashed,
            "connection loop: boom",
            EXIT_CRASH,
        ),
        (
            RunResult::Exit {
                outcome: Outcome::Failed,
                reason: "agent asked".to_owned(),
            },
            Outcome::Failed,
            "agent asked",
            EXIT_FAILED,
        ),
        (
            RunResult::Exit {
                outcome: Outcome::InitFailed,
                reason: "late".to_owned(),
            },
            Outcome::InitFailed,
            "late",
            EXIT_INIT,
        ),
        (
            RunResult::Exit {
                outcome: Outcome::Done,
                reason: "strategy exit".to_owned(),
            },
            Outcome::Done,
            "strategy exit",
            EXIT_DONE,
        ),
        (
            RunResult::Exit {
                outcome: Outcome::Unspecified,
                reason: "?".to_owned(),
            },
            Outcome::Crashed,
            "?",
            EXIT_CRASH,
        ),
        (
            RunResult::Exit {
                outcome: Outcome::Crashed,
                reason: "panic".to_owned(),
            },
            Outcome::Crashed,
            "panic",
            EXIT_CRASH,
        ),
    ];
    for (result, outcome, reason, code) in cases {
        // Each is the plane ending on its own, so OnStop takes the hook budget.
        let mut m = running(HOOK);
        let fx = m.run_returned(NOW, result);
        assert_eq!(fx.len(), 3, "{fx:?}");
        assert_eq!(fx[0], exiting(outcome, reason));
        arm(TimerKind::OnStopBudget, HOOK, NOW + SEC, &fx[1]);
        assert_eq!(fx[2], on_stop(Some(NOW + SEC)));
        assert_eq!(
            m.on_stop_returned(NOW, Ok(())),
            vec![stopped(true), Effect::Exit(code)]
        );
    }
}

#[test]
fn run_hung_skips_on_stop() {
    let mut m = running(HOOK);
    assert_eq!(
        m.run_returned(
            NOW,
            RunResult::Exit {
                outcome: Outcome::Hung,
                reason: "on_order".to_owned(),
            }
        ),
        vec![
            exiting(Outcome::Hung, "on_order"),
            stopped(false),
            Effect::Exit(EXIT_HUNG),
        ]
    );
}

#[test]
fn run_exit_wins_over_a_stop_but_an_error_does_not() {
    let mut m = running(HOOK);
    m.on_down(NOW, stop_msg("bye", 5000));
    assert_eq!(
        m.run_returned(
            NOW,
            RunResult::Exit {
                outcome: Outcome::Failed,
                reason: "td buffer full".to_owned(),
            }
        ),
        vec![
            exiting(Outcome::Failed, "td buffer full"),
            on_stop(Some(NOW + 5 * SEC)),
        ]
    );
    assert_eq!(
        m.on_stop_returned(NOW, Ok(())),
        vec![stopped(true), Effect::Exit(EXIT_FAILED)]
    );

    let mut m = running(HOOK);
    m.on_down(NOW, stop_msg("bye", 5000));
    assert_eq!(
        m.run_returned(NOW, RunResult::Error("hook panic: x".to_owned())),
        vec![exiting(Outcome::Done, "bye"), on_stop(Some(NOW + 5 * SEC))]
    );
}

#[test]
fn run_result_from_dispatch() {
    assert_eq!(RunResult::from(RunEnd::Done), RunResult::Returned);
    assert_eq!(
        RunResult::from(RunEnd::Failed {
            reason: "td buffer full".to_owned()
        }),
        RunResult::Exit {
            outcome: Outcome::Failed,
            reason: "td buffer full".to_owned(),
        }
    );
    assert_eq!(
        RunResult::from(RunEnd::Crashed {
            reason: "hook panic: x".to_owned()
        }),
        RunResult::Error("hook panic: x".to_owned())
    );
    assert_eq!(
        RunResult::from(RunEnd::Hung),
        RunResult::Exit {
            outcome: Outcome::Hung,
            reason: REASON_HUNG.to_owned(),
        }
    );
    assert_eq!(CallError::from(StopError::Deadline), CallError::Cancelled);
    assert_eq!(CallError::from(StopError::Cancelled), CallError::Cancelled);
    assert_eq!(
        CallError::from(StopError::OnStop("disk".to_owned())),
        CallError::Failed("disk".to_owned())
    );
}

// ---------- Live (E01-D011, PROTOCOL §9.2.9) ----------

#[test]
fn live_before_init_is_applied_in_order_before_done_init() {
    let mut m = machine();
    m.on_down(NOW, live_msg("A", "a"));
    m.on_down(NOW, live_msg("B", "b"));
    m.on_down(NOW, init_msg("L1", 0));
    assert!(m.on_down(NOW, live_msg("C", "c")).is_empty());
    assert_eq!(
        m.init_returned(NOW, Ok(()), HOOK),
        vec![applied("L1", true), call_live("a")]
    );
    // A Live that arrives after Init succeeded waits for Done(INIT, ok).
    assert!(m.on_down(NOW, live_msg("D", "d")).is_empty());
    assert_eq!(
        m.live_returned(NOW, Ok(())),
        vec![applied("A", false), call_live("b")]
    );
    // A refused Live is logged; nothing is sent for it.
    assert_eq!(
        m.live_returned(NOW, Err(CallError::Failed("selector".to_owned()))),
        vec![log("live not applied: selector"), call_live("c")]
    );
    assert_eq!(
        m.live_returned(NOW, Ok(())),
        vec![
            applied("C", false),
            done(Verb::Init, true, ""),
            call_live("d")
        ]
    );
    assert_eq!(m.live_returned(NOW, Ok(())), vec![applied("D", false)]);
    assert!(m.live_returned(NOW, Ok(())).is_empty());
}

#[test]
fn live_one_at_a_time_in_order() {
    let mut m = running(HOOK);
    assert_eq!(m.on_down(NOW, live_msg("A", "a")), vec![call_live("a")]);
    assert!(m.on_down(NOW, live_msg("B", "b")).is_empty());
    assert_eq!(
        m.live_returned(NOW, Ok(())),
        vec![applied("A", false), call_live("b")]
    );
    assert_eq!(m.live_returned(NOW, Ok(())), vec![applied("B", false)]);
}

#[test]
fn live_while_start_runs() {
    let mut m = initialized(HOOK);
    m.on_down(NOW, start_msg(0));
    assert_eq!(m.on_down(NOW, live_msg("A", "a")), vec![call_live("a")]);
    assert_eq!(m.live_returned(NOW, Ok(())), vec![applied("A", false)]);
}

#[test]
fn live_contract() {
    let mut m = running(HOOK);
    assert!(m.on_down(NOW, live_in(OTHER, "X", "x")).is_empty());
    assert_eq!(m.on_down(NOW, live_in("", "Y", "y")), vec![call_live("y")]);
}

#[test]
fn live_digest_of_init() {
    // Go sends Applied even when live_digest is empty.
    let mut m = machine();
    m.on_down(NOW, init_msg("", 0));
    assert_eq!(
        m.init_returned(NOW, Ok(()), HOOK),
        vec![applied("", true), done(Verb::Init, true, "")]
    );
    // A plane whose Init does not carry live config sends no Applied for it.
    let mut config = Config::session("r1", 1);
    config.init_carries_live = false;
    let mut m = Machine::new(config);
    m.hello();
    m.on_down(NOW, init_msg("L1", 0));
    assert_eq!(
        m.init_returned(NOW, Ok(()), HOOK),
        vec![done(Verb::Init, true, "")]
    );
}

#[test]
fn live_and_relay_still_reach_the_plane_while_stopping() {
    // Go's reader keeps calling the plane until Serve returns.
    let mut m = running(HOOK);
    m.run_returned(NOW, RunResult::Returned);
    assert_eq!(m.on_down(NOW, live_msg("A", "a")), vec![call_live("a")]);
    assert_eq!(
        m.on_down(NOW, relay_msg(CONTRACT_STS, "c1", "p")),
        vec![echo("c1", "p"), call_relay("c1", "p")]
    );
}

#[test]
fn stop_while_held_lives_drain() {
    let mut m = machine();
    m.on_down(NOW, live_msg("A", "a"));
    m.on_down(NOW, init_msg("L1", 0));
    m.init_returned(NOW, Ok(()), HOOK);
    // Init has returned, so there is nothing to cancel.
    assert_eq!(
        m.on_down(NOW, stop_msg("bye", 5000)),
        vec![Effect::StopBeat]
    );
    assert_eq!(
        m.live_returned(NOW, Ok(())),
        vec![
            applied("A", false),
            done(Verb::Init, true, ""),
            exiting(Outcome::Done, "bye"),
            stopped(false),
            Effect::Exit(EXIT_DONE),
        ]
    );
}

// ---------- Relay, Controller, Undeliverable (PROTOCOL §9.2.8, §9.8) ----------

#[test]
fn relay_is_echoed_after_init() {
    let mut m = machine();
    assert!(
        m.on_down(NOW, relay_msg(CONTRACT_STS, "c0", "x"))
            .is_empty()
    );
    m.on_down(NOW, init_msg("L1", 0));
    assert!(
        m.on_down(NOW, relay_msg(CONTRACT_STS, "c0", "x"))
            .is_empty()
    );
    m.init_returned(NOW, Ok(()), HOOK);
    assert_eq!(
        m.on_down(NOW, relay_msg(CONTRACT_STS, "c1", "drain")),
        vec![echo("c1", "drain"), call_relay("c1", "drain")]
    );
    assert!(m.on_down(NOW, relay_msg(OTHER, "c2", "x")).is_empty());
    // An empty contract is ours; the echo names the session's contract.
    assert_eq!(
        m.on_down(NOW, relay_msg("", "c3", "fail")),
        vec![echo("c3", "fail"), call_relay("c3", "fail")]
    );
}

#[test]
fn controller_is_recorded_at_any_time() {
    let mut m = machine();
    assert!(m.controller().is_none());
    // The shim sends it when the agent attaches, which is before Init.
    assert!(m.on_down(NOW, controller_msg(true, 3)).is_empty());
    assert_eq!(
        m.controller(),
        Some(&Controller {
            attached: true,
            epoch: 3
        })
    );
    assert_eq!(m.on_down(NOW, init_msg("L1", 0)), vec![call_init(None)]);
    m.on_down(NOW, controller_msg(false, 3));
    assert_eq!(
        m.controller(),
        Some(&Controller {
            attached: false,
            epoch: 3
        })
    );
    let undeliverable = down(worker_down::Msg::Undeliverable(worker::Undeliverable {
        corr_id: b"c1".to_vec(),
    }));
    assert!(m.on_down(NOW, undeliverable).is_empty());
    assert!(m.on_down(NOW, WorkerDown { msg: None }).is_empty());
}

// ---------- Init and Start that arrive out of turn ----------

#[test]
fn start_read_during_init_runs_after_it() {
    let mut m = machine();
    m.on_down(NOW, init_msg("L1", 0));
    assert!(m.on_down(NOW, start_msg(0)).is_empty());
    assert!(m.on_down(NOW, start_msg(0)).is_empty());
    assert_eq!(
        m.init_returned(NOW, Ok(()), HOOK),
        vec![
            applied("L1", true),
            done(Verb::Init, true, ""),
            call_start(None)
        ]
    );
}

#[test]
fn second_init_read_during_init_fails_after_it() {
    let mut m = machine();
    m.on_down(NOW, init_msg("L1", 0));
    assert!(m.on_down(NOW, init_msg("L1", 0)).is_empty());
    let mut want = vec![applied("L1", true), done(Verb::Init, true, "")];
    want.extend(refused(Verb::Init, REASON_INIT_AFTER_INIT));
    assert_eq!(m.init_returned(NOW, Ok(()), HOOK), want);
}

#[test]
fn stop_wins_over_a_start_read_during_init() {
    let mut m = machine();
    m.on_down(NOW, init_msg("L1", 0));
    m.on_down(NOW, start_msg(0));
    assert_eq!(
        m.on_down(NOW, stop_msg("bye", 5000)),
        vec![Effect::StopBeat, Effect::Cancel]
    );
    assert_eq!(
        m.init_returned(NOW, Ok(()), HOOK),
        vec![
            applied("L1", true),
            done(Verb::Init, true, ""),
            exiting(Outcome::Done, "bye"),
            stopped(false),
            Effect::Exit(EXIT_DONE),
        ]
    );
}

#[test]
fn init_and_start_after_start_are_ignored() {
    let mut m = initialized(HOOK);
    m.on_down(NOW, start_msg(0));
    assert!(m.on_down(NOW, init_msg("L1", 0)).is_empty());
    assert!(m.on_down(NOW, start_msg(0)).is_empty());
    m.start_returned(NOW, Ok(()));
    assert!(m.on_down(NOW, init_msg("L1", 0)).is_empty());
    assert!(m.on_down(NOW, start_msg(0)).is_empty());
}

// ---------- Frames, Beat, and the end ----------

#[test]
fn only_the_frames_go_returns_exit_other_for_are_fatal() {
    let mut m = machine();
    let mut all = m.on_down(NOW, live_msg("A", "a"));
    all.extend(m.on_down(NOW, init_msg("L1", 0)));
    all.extend(m.init_returned(NOW, Ok(()), HOOK));
    all.extend(m.live_returned(NOW, Ok(())));
    all.extend(m.on_down(NOW, start_msg(0)));
    all.extend(m.start_returned(NOW, Ok(())));
    all.extend(m.on_down(NOW, relay_msg(CONTRACT_STS, "c", "p")));
    all.extend(m.on_down(NOW, stop_msg("bye", 1000)));
    all.extend(m.run_returned(NOW, RunResult::Returned));
    all.extend(m.on_stop_returned(NOW, Ok(())));
    let fatal: Vec<bool> = all
        .iter()
        .filter_map(|effect| match effect {
            Effect::Send { fatal, .. } => Some(*fatal),
            _ => None,
        })
        .collect();
    // Applied(L1), Applied(A), Done(INIT), Done(START), Stage, Relay, Exiting, Stopped.
    assert_eq!(fatal, [true, false, true, true, true, false, false, false]);
}

#[test]
fn beat_stops_only_with_a_stop() {
    // A deadline ending and a self-end exit with Beat still on, as Go does.
    let mut m = machine();
    let fx = m.on_down(NOW, init_msg("L1", 50));
    let token = arm(
        TimerKind::InitDeadline,
        Duration::from_millis(50),
        NOW + 50 * MS,
        &fx[0],
    );
    assert!(!m.on_timer(NOW + 50 * MS, token).contains(&Effect::StopBeat));

    let mut m = running(Duration::ZERO);
    let mut fx = m.run_returned(NOW, RunResult::Returned);
    fx.extend(m.on_stop_returned(NOW, Ok(())));
    assert!(!fx.contains(&Effect::StopBeat));
    assert!(!has_send(&fx, |msg| matches!(msg, worker_up::Msg::Beat(_))));
}

#[test]
fn nothing_after_exit() {
    let mut m = running(HOOK);
    m.run_returned(
        NOW,
        RunResult::Exit {
            outcome: Outcome::Hung,
            reason: "x".to_owned(),
        },
    );
    assert_eq!(m.exit_code(), Some(EXIT_HUNG));
    assert!(m.hello().is_empty());
    assert!(m.on_down(NOW, stop_msg("bye", 0)).is_empty());
    assert!(m.on_down(NOW, live_msg("A", "a")).is_empty());
    assert!(m.on_eof(NOW).is_empty());
    assert!(m.on_sigterm(NOW).is_empty());
    assert!(m.run_returned(NOW, RunResult::Returned).is_empty());
    assert!(m.on_stop_returned(NOW, Ok(())).is_empty());
    assert!(m.live_returned(NOW, Ok(())).is_empty());
    assert!(m.init_returned(NOW, Ok(()), HOOK).is_empty());
    assert!(m.start_returned(NOW, Ok(())).is_empty());
}

#[test]
fn results_out_of_turn_are_ignored() {
    let mut m = initialized(HOOK);
    assert!(m.init_returned(NOW, Ok(()), HOOK).is_empty());
    assert!(m.start_returned(NOW, Ok(())).is_empty());
    assert!(m.run_returned(NOW, RunResult::Returned).is_empty());
    assert!(m.on_stop_returned(NOW, Ok(())).is_empty());
    assert!(m.live_returned(NOW, Ok(())).is_empty());
    assert_eq!(m.exit_code(), None);
}

#[test]
fn report_carries_the_contract() {
    let m = machine();
    assert_eq!(
        m.report(b"r".to_vec()),
        WorkerUp {
            msg: Some(worker_up::Msg::Report(worker::Report {
                contract: CONTRACT_STS.to_owned(),
                payload: b"r".to_vec(),
            })),
        }
    );
}

#[test]
fn call_error_text() {
    assert_eq!(CallError::Cancelled.to_string(), "context canceled");
    assert_eq!(CallError::Failed("x".to_owned()).to_string(), "x");
}
