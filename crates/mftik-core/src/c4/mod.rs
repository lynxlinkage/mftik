//! Worker end of C4, the shim ⇄ worker protocol on fd 3 (PROTOCOL §5, §9.2).
//!
//! A pure state machine (E03-D001). The semantics are mftiko `contract/wire/c4`
//! with the session worker's configuration from `sdk/run.go`. The host feeds it
//! decoded [`WorkerDown`] frames, fd 3 EOF, SIGTERM, the result of each plane
//! call it asked for, and the timers it armed, each with the injected `now_ns`
//! (E03-D004). It returns [`Effect`]s: frames to write, plane calls, Beat on or
//! off, timers, log lines, and the exit code. Frames, the fd, and the Beat
//! thread are E03-03 (`mftik-rt`).
//!
//! # Driving it
//!
//! ```text
//! let mut c4 = Machine::new(Config::session(release, pid));
//! perform(c4.hello());
//! // fd 3 reader:  perform(c4.on_down(now_ns(), msg));  at EOF perform(c4.on_eof(now_ns()));
//! // SIGTERM:      perform(c4.on_sigterm(now_ns()));
//!
//! fn perform(effects: Vec<Effect>) {
//!     for effect in effects {
//!         match effect {
//!             Effect::Send { msg, fatal } => if write(msg).is_err() && fatal { flush_and_exit(EXIT_OTHER) },
//!             Effect::CallInit { payload, deadline_ns } => on_dispatcher(move || {
//!                 let (result, hook_timeout) = plane.init(payload, deadline_ns);
//!                 perform(c4.init_returned(now_ns(), result, hook_timeout));
//!             }),
//!             Effect::ArmTimer { token, deadline_ns, .. } => arm(deadline_ns, move || perform(c4.on_timer(now_ns(), token))),
//!             Effect::Exit(code) => flush_and_exit(code), // E03-D005; nothing follows
//!             other => /* StartBeat, StopBeat, CallStart, CallRun, CallLive, CallRelay, CallOnStop, Cancel, Log */,
//!         }
//!     }
//! }
//! ```
//!
//! Perform effects in order. `CallInit`, `CallStart`, `CallRun`, and
//! `CallOnStop` block in the plane: run them without holding the machine, so
//! the reader keeps feeding `Stop` and `Live`, and report each with its
//! `*_returned` method. A deadline that fires first ends the session without
//! waiting; the late result is ignored (E08-D007). [`Effect::Cancel`] ends the
//! context of the Init, Start, or Run call in progress (Go cancels `life`).
//!
//! # Order of a session (PROTOCOL §5.2, §5.3)
//!
//! `Hello`, then Beat. `Init` → `CallInit` → `Applied(live_digest)`, the Lives
//! held so far, `Done(INIT, ok)`. `Start` → `CallStart` → `Done(START, ok)`,
//! `Stage(running)`, `CallRun`. The ending is `Exiting(outcome, reason)`, then
//! `CallOnStop` when §5.3 runs it, then `Stopped(on_stop_completed)`, then
//! [`Effect::Exit`]. Before `Init` has been taken, `Start` ends `INIT_FAILED`.
//! After `Done(INIT, ok)` and before `Start`, a second `Init` ends
//! `INIT_FAILED` and `Stop` ends `DONE` without `OnStart` or `OnStop`
//! (E08-D037).
//!
//! Like Go, a `Stop`, EOF, or SIGTERM before `Init` ends `DONE`, a `Live`
//! before `Init` succeeds is held, a `Relay` before then is dropped, and
//! `Controller` is recorded at any time. PROTOCOL §5.2 words E08-D037 more
//! broadly; see E03-D025.
//!
//! Unlike Go, and as PROTOCOL §9.5.5 says, OnStop after fd 3 EOF is bounded:
//! by the rest of a `Stop`'s grace if one was read, otherwise by
//! `hook_timeout_s`, past which the worker exits 21 (E03-D027).

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::fmt;
use std::time::Duration;

use mftik_proto::wire::mftiko::common::v1::Outcome;
use mftik_proto::wire::mftiko::worker::v1::{
    self as worker, Applied, Controller, Done, Exiting, Hello, Live, Report, Stage, Stopped, Verb,
    WorkerDown, WorkerUp, worker_down, worker_up,
};

use crate::dispatch::{REASON_HUNG, RunEnd, StopError};

/// `DONE` (PROTOCOL §5.3).
pub const EXIT_DONE: i32 = 0;
/// fd 3 could not be written where Go returns `ExitOther`. See [`Effect::Send`].
pub const EXIT_OTHER: i32 = 1;
/// `INIT_FAILED`: Init or Start failed or ran past its deadline.
pub const EXIT_INIT: i32 = 10;
/// `CRASHED`.
pub const EXIT_CRASH: i32 = 20;
/// `HUNG`, and an OnStop past the hook budget after a self-end (E08-D003).
pub const EXIT_HUNG: i32 = 21;
/// `FAILED`.
pub const EXIT_FAILED: i32 = 30;

/// How often Beat goes out once [`Effect::StartBeat`] (PROTOCOL §5.2, §9.2.3).
pub const BEAT_EVERY: Duration = Duration::from_secs(1);

/// `Hello.kind` of a session worker.
pub const KIND_SESSION: &str = "session";
/// The session worker's C5 contract. `Init`, `Start`, `Live`, and `Relay` name it.
pub const CONTRACT_STS: &str = "stsctl.v1";
/// `Hello.contracts` of a session worker, in Go's order (PROTOCOL §9.2.2).
pub const SESSION_CONTRACTS: [&str; 4] = ["worker.v1", "stsctl.v1", "md.v1", "td.v1"];

/// `Stage` sent after `Done(START, ok)`.
pub const STAGE_RUNNING: &str = "running";
/// `Done.error` and `Exiting.reason` when Init or Start ran past `deadline_ms`.
pub const REASON_DEADLINE: &str = "deadline";
/// `Exiting.reason` of a stop that carried no reason: SIGTERM, or a `Stop` with an empty one.
pub const REASON_SIGNAL: &str = "signal";
/// `Exiting.reason` when fd 3 reached EOF before any `Stop`.
pub const REASON_SHIM_CLOSED: &str = "shim closed";
/// `Start` before the first `Init` (E08-D037).
pub const REASON_START_BEFORE_INIT: &str = "c4: start before init";
/// A second `Init` (E08-D037).
pub const REASON_INIT_AFTER_INIT: &str = "c4: init after init";

/// Text of [`CallError::Cancelled`], as Go's `context.Canceled`.
const CANCELED: &str = "context canceled";
const NS_PER_MS: i64 = 1_000_000;

/// The process around the plane (Go `c4.Config`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// `Hello.kind`.
    pub kind: String,
    /// The C5 contract. `Init` must name it. `Start`, `Live`, and `Relay` name it or leave it empty.
    pub contract: String,
    /// `Hello.contracts`. Empty sends `[contract]`.
    pub contracts: Vec<String>,
    /// `Hello.release`.
    pub release: String,
    /// `Hello.pid`. The host reads it; this module does no IO.
    pub pid: i32,
    /// `Init.payload` is the first live config: `Applied(live_digest)` goes out before
    /// `Done(INIT, ok)` (E01-D011).
    pub init_carries_live: bool,
}

impl Config {
    /// The session worker, as `sdk/run.go` configures `c4.Serve`.
    pub fn session(release: impl Into<String>, pid: i32) -> Self {
        Self {
            kind: KIND_SESSION.to_owned(),
            contract: CONTRACT_STS.to_owned(),
            contracts: SESSION_CONTRACTS.iter().map(|c| (*c).to_owned()).collect(),
            release: release.into(),
            pid,
            init_carries_live: true,
        }
    }
}

/// Which armed timer fired.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimerKind {
    /// `Init.deadline_ms`.
    InitDeadline,
    /// `Start.deadline_ms`.
    StartDeadline,
    /// The hook budget of OnStop after the plane ended on its own (E08-D009).
    OnStopBudget,
}

/// Identity of one [`Effect::ArmTimer`]. Pass it back to [`Machine::on_timer`].
/// Only the latest arm is live.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimerToken {
    pub kind: TimerKind,
    pub id: u64,
}

/// Something the host must do, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Write one frame to fd 3. When `fatal` is set and the write fails, end the process with
    /// [`EXIT_OTHER`] and perform none of the effects after it (Go returns `ExitOther`). Other
    /// write errors are ignored. `fatal` frames are `Hello`, the Init `Applied`, `Done(INIT, ok)`,
    /// `Done(START, ok)`, and `Stage(running)`.
    Send { msg: WorkerUp, fatal: bool },
    /// Send `Beat` every [`BEAT_EVERY`] from a thread hooks do not block (E03-D003).
    StartBeat,
    /// Stop sending `Beat`. Go's Beat ends when a `Stop`, EOF, or SIGTERM cancels its context.
    StopBeat,
    /// Call the plane's Init with `Init.payload`. `deadline_ns` is `Init.deadline_ms` on the
    /// injected clock; `None` is no limit. Report with [`Machine::init_returned`].
    CallInit {
        payload: Vec<u8>,
        deadline_ns: Option<i64>,
    },
    /// Call the plane's Start with `Start.payload`. Report with [`Machine::start_returned`].
    CallStart {
        payload: Vec<u8>,
        deadline_ns: Option<i64>,
    },
    /// Call the plane's Run. Report with [`Machine::run_returned`].
    CallRun,
    /// Apply one `Live.payload`. One is in flight at a time. Report with [`Machine::live_returned`].
    CallLive { payload: Vec<u8> },
    /// Hand one Command to the plane. It must not block, and there is no result. The echo has
    /// already been sent (the session plane's `Relay` in `sdk/plane.go`).
    CallRelay { corr_id: Vec<u8>, payload: Vec<u8> },
    /// Call the plane's OnStop between `Exiting` and `Stopped`. Its context is not the one
    /// [`Effect::Cancel`] ended. `deadline_ns` is that context's deadline: the rest of the first
    /// `Stop`'s grace, or the hook budget after a self-end; `None` is no limit (E08-D009).
    /// Report with [`Machine::on_stop_returned`].
    CallOnStop { deadline_ns: Option<i64> },
    /// End the context of the Init, Start, or Run call in progress. It never applies to OnStop.
    Cancel,
    /// Call [`Machine::on_timer`] with `token` at `deadline_ns` on the injected clock.
    ArmTimer {
        token: TimerToken,
        after: Duration,
        deadline_ns: i64,
    },
    /// One log line (Go `Config.Log`). It never carries a payload.
    Log(String),
    /// Flush fd 3 and end the process with this code (PROTOCOL §5.3, E03-D005). Nothing follows.
    Exit(i32),
}

/// Why a plane call did not succeed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallError {
    /// The call failed. The text becomes `Done.error` and `Exiting.reason`, so it must not
    /// carry secrets.
    Failed(String),
    /// The call returned because its context ended: [`Effect::Cancel`], or its deadline passed
    /// (Go `ctx.Err()`). Past an Init or Start deadline that is the deadline ending. Past the
    /// OnStop hook budget it is exit 21.
    Cancelled,
}

impl fmt::Display for CallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::Failed(text) => write!(formatter, "{text}"),
            CallError::Cancelled => write!(formatter, "{CANCELED}"),
        }
    }
}

impl std::error::Error for CallError {}

/// [`crate::dispatch::Dispatcher::on_stop`] as an OnStop result: its deadline and cancel are
/// the context ending, anything else is a failure.
impl From<StopError> for CallError {
    fn from(error: StopError) -> Self {
        match error {
            StopError::Deadline | StopError::Cancelled => CallError::Cancelled,
            other => CallError::Failed(other.to_string()),
        }
    }
}

/// How the plane's Run ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunResult {
    /// Run returned without an error (Go `nil`): `DONE`, or the stop when one was read.
    Returned,
    /// Run asked for this ending (Go `*c4.Exit`). It wins over a stop already read.
    Exit { outcome: Outcome, reason: String },
    /// Run failed (Go plain error, a hook panic): `CRASHED` with this text, unless a stop was
    /// read first, which makes it `DONE`.
    Error(String),
}

/// [`crate::dispatch::Dispatcher::run`]'s ending as Go's `Disp.Run` returns it to c4.
impl From<RunEnd> for RunResult {
    fn from(end: RunEnd) -> Self {
        match end {
            RunEnd::Done => RunResult::Returned,
            RunEnd::Failed { reason } => RunResult::Exit {
                outcome: Outcome::Failed,
                reason,
            },
            RunEnd::Crashed { reason } => RunResult::Error(reason),
            RunEnd::Hung => RunResult::Exit {
                outcome: Outcome::Hung,
                reason: REASON_HUNG.to_owned(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    /// `Hello` sent; no `Init` taken yet.
    AwaitInit,
    /// `CallInit` outstanding.
    Init { deadline_ns: Option<i64> },
    /// Init succeeded; the Lives held before it are being applied before `Done(INIT, ok)`.
    InitLives,
    /// `Done(INIT, ok)` sent; no `Start` taken yet.
    AwaitStart,
    /// `CallStart` outstanding.
    Start { deadline_ns: Option<i64> },
    /// `CallRun` outstanding.
    Run,
    /// `Exiting` sent, `CallOnStop` outstanding. `budget_ns` is set when exit 21 can end it.
    OnStop { code: i32, budget_ns: Option<i64> },
    /// `Exit` emitted.
    Exited(i32),
}

/// An `Init` or `Start` read while Init runs. Go queues it on a channel that `waitStart` reads.
#[derive(Clone, Debug)]
enum Pending {
    Init,
    Start(worker::Start),
}

/// The worker's C4 session (Go `c4.Serve`). See the module docs.
#[derive(Debug)]
pub struct Machine {
    config: Config,
    phase: Phase,
    now_ns: i64,
    hello_sent: bool,
    beat_on: bool,

    init_digest: String,
    inited: bool,
    pending: Option<Pending>,
    init_lives: usize,
    hook_timeout_ns: i64,

    lives: VecDeque<Live>,
    live_busy: Option<String>,

    stopping: bool,
    /// The first `Stop` or EOF's reason (Go `halt`). SIGTERM does not set it.
    stop_reason: Option<String>,
    /// When the first `Stop` was read, and its `grace_ms` (Go `noteStop`).
    grace: Option<(i64, u32)>,
    /// fd 3 reached EOF. Nothing outside the worker can end it any more (E03-D027).
    shim_gone: bool,
    controller: Option<Controller>,

    armed: Option<TimerToken>,
    next_timer: u64,
}

impl Machine {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            phase: Phase::AwaitInit,
            now_ns: 0,
            hello_sent: false,
            beat_on: false,
            init_digest: String::new(),
            inited: false,
            pending: None,
            init_lives: 0,
            hook_timeout_ns: 0,
            lives: VecDeque::new(),
            live_busy: None,
            stopping: false,
            stop_reason: None,
            grace: None,
            shim_gone: false,
            controller: None,
            armed: None,
            next_timer: 0,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The last `Controller` the shim sent, `None` before the first (PROTOCOL §9.2.8). The
    /// shim answers a `Relay` that no agent can take with `Undeliverable`; stsctl has no
    /// worker-to-agent request (E09-D003), so this module ignores `Undeliverable`.
    pub fn controller(&self) -> Option<&Controller> {
        self.controller.as_ref()
    }

    /// The code [`Effect::Exit`] carried, once it has been emitted.
    pub fn exit_code(&self) -> Option<i32> {
        match self.phase {
            Phase::Exited(code) => Some(code),
            _ => None,
        }
    }

    /// The first call: `Hello`, then Beat. Later calls return nothing.
    pub fn hello(&mut self) -> Vec<Effect> {
        if self.hello_sent || self.exited() {
            return Vec::new();
        }
        self.hello_sent = true;
        let contracts = if self.config.contracts.is_empty() {
            vec![self.config.contract.clone()]
        } else {
            self.config.contracts.clone()
        };
        let hello = Hello {
            kind: self.config.kind.clone(),
            release: self.config.release.clone(),
            contracts,
            pid: self.config.pid,
        };
        self.beat_on = true;
        vec![send(worker_up::Msg::Hello(hello), true), Effect::StartBeat]
    }

    /// `Report` with this session's contract (Go `Session.Report`). The plane sends it itself.
    pub fn report(&self, payload: Vec<u8>) -> WorkerUp {
        WorkerUp {
            msg: Some(worker_up::Msg::Report(Report {
                contract: self.config.contract.clone(),
                payload,
            })),
        }
    }

    /// One frame read from fd 3.
    pub fn on_down(&mut self, now_ns: i64, msg: WorkerDown) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) {
            return out;
        }
        match msg.msg {
            Some(worker_down::Msg::Init(init)) => self.on_init_msg(init, &mut out),
            Some(worker_down::Msg::Start(start)) => self.on_start_msg(start, &mut out),
            Some(worker_down::Msg::Stop(stop)) => {
                if self.grace.is_none() {
                    self.grace = Some((now_ns, stop.grace_ms));
                }
                if self.stop_reason.is_none() {
                    self.stop_reason = Some(stop.reason);
                }
                self.halt(&mut out);
            }
            Some(worker_down::Msg::Live(live)) => self.on_live_msg(live, &mut out),
            Some(worker_down::Msg::Relay(relay)) => self.on_relay_msg(relay, &mut out),
            Some(worker_down::Msg::Controller(controller)) => self.controller = Some(controller),
            Some(worker_down::Msg::Undeliverable(_)) | None => {}
        }
        out
    }

    /// fd 3 reached EOF: the shim is gone, stop gracefully (PROTOCOL §4.5 S2, §9.2.7).
    ///
    /// With the shim gone nothing else can end the process, so from here on OnStop always has
    /// a budget past which the worker exits 21 without `Stopped`: the rest of a `Stop`'s grace
    /// if one was read, otherwise `hook_timeout_s` (PROTOCOL §9.5.5, E03-D027). An OnStop that
    /// is already running without a budget (after SIGTERM, or a `Stop`) gets one now. Go leaves
    /// OnStop unbounded here.
    pub fn on_eof(&mut self, now_ns: i64) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) {
            return out;
        }
        if self.stop_reason.is_none() {
            self.stop_reason = Some(REASON_SHIM_CLOSED.to_owned());
        }
        self.shim_gone = true;
        self.halt(&mut out);
        if let Phase::OnStop {
            code,
            budget_ns: None,
        } = self.phase
            && let (_, Some(at)) = self.stop_limit(false)
        {
            out.push(self.arm(TimerKind::OnStopBudget, at));
            self.phase = Phase::OnStop {
                code,
                budget_ns: Some(at),
            };
        }
        out
    }

    /// SIGTERM is a stop with no grace: OnStop has no deadline (E08-D006, E08-D009).
    pub fn on_sigterm(&mut self, now_ns: i64) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) {
            return out;
        }
        self.halt(&mut out);
        out
    }

    /// A timer from [`Effect::ArmTimer`] fired. A token that is no longer armed is ignored.
    /// The token decides; the clock is not read again.
    pub fn on_timer(&mut self, now_ns: i64, token: TimerToken) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) || self.armed != Some(token) {
            return out;
        }
        self.armed = None;
        // A stop already read waits for Init or Start instead (Go `runLimited`).
        let verb = match (token.kind, &self.phase) {
            (TimerKind::InitDeadline, Phase::Init { .. }) if !self.stopping => Some(Verb::Init),
            (TimerKind::StartDeadline, Phase::Start { .. }) if !self.stopping => Some(Verb::Start),
            (TimerKind::OnStopBudget, Phase::OnStop { .. }) => None,
            _ => return out,
        };
        match verb {
            // The hook may still be inside Init or Start; it is not waited for, and OnStop
            // does not run (E08-D007, E08-D008).
            Some(verb) => self.fail_phase(verb, REASON_DEADLINE.to_owned(), false, &mut out),
            // Exiting already names the outcome. No Stopped (E08-D003).
            None => self.exit(EXIT_HUNG, &mut out),
        }
        out
    }

    /// The plane's Init returned. `hook_timeout` is the OnStop budget after a self-end, the
    /// plane's reading of `hook_timeout_s`; zero is no limit. It is read only on success (Go
    /// `HookTimeout`, E08-D009). A result after the deadline ended the session is ignored.
    pub fn init_returned(
        &mut self,
        now_ns: i64,
        result: Result<(), CallError>,
        hook_timeout: Duration,
    ) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) {
            return out;
        }
        let Phase::Init { deadline_ns } = self.phase else {
            return out;
        };
        self.armed = None;
        match result {
            Ok(()) => {
                self.hook_timeout_ns = duration_ns(hook_timeout);
                if self.config.init_carries_live {
                    let digest = self.init_digest.clone();
                    out.push(applied(digest, true));
                }
                self.inited = true;
                self.init_lives = self.lives.len();
                self.phase = Phase::InitLives;
                self.pump(&mut out);
                if self.init_lives == 0 {
                    self.init_done(&mut out);
                }
            }
            Err(_) if self.stopping => {
                let reason = self.stop_text();
                self.finish(Outcome::Done, reason, false, EXIT_DONE, false, &mut out);
            }
            Err(CallError::Cancelled) if expired(deadline_ns, now_ns) => {
                self.fail_phase(Verb::Init, REASON_DEADLINE.to_owned(), false, &mut out);
            }
            Err(error) => self.fail_phase(Verb::Init, error.to_string(), false, &mut out),
        }
        out
    }

    /// The plane's Start returned. A result after the deadline ended the session is ignored.
    pub fn start_returned(&mut self, now_ns: i64, result: Result<(), CallError>) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) {
            return out;
        }
        let Phase::Start { deadline_ns } = self.phase else {
            return out;
        };
        self.armed = None;
        match result {
            // A Start that ends because of a stop is a stop, and Init needs cleaning up.
            _ if self.stopping => {
                let reason = self.stop_text();
                self.finish(Outcome::Done, reason, true, EXIT_DONE, false, &mut out);
            }
            Ok(()) => {
                out.push(done(Verb::Start, true, String::new(), true));
                out.push(send(
                    worker_up::Msg::Stage(Stage {
                        name: STAGE_RUNNING.to_owned(),
                    }),
                    true,
                ));
                self.phase = Phase::Run;
                out.push(Effect::CallRun);
            }
            Err(CallError::Cancelled) if expired(deadline_ns, now_ns) => {
                self.fail_phase(Verb::Start, REASON_DEADLINE.to_owned(), false, &mut out);
            }
            // Init succeeded, so OnStop runs (E08-D008).
            Err(error) => self.fail_phase(Verb::Start, error.to_string(), true, &mut out),
        }
        out
    }

    /// The plane's Run returned (PROTOCOL §5.3).
    pub fn run_returned(&mut self, now_ns: i64, result: RunResult) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) || self.phase != Phase::Run {
            return out;
        }
        match result {
            RunResult::Exit { outcome, reason } => self.exit_as(outcome, reason, &mut out),
            _ if self.stopping => {
                let reason = self.stop_text();
                self.finish(Outcome::Done, reason, true, EXIT_DONE, false, &mut out);
            }
            RunResult::Error(text) => {
                self.finish(Outcome::Crashed, text, true, EXIT_CRASH, true, &mut out)
            }
            RunResult::Returned => self.finish(
                Outcome::Done,
                String::new(),
                true,
                EXIT_DONE,
                true,
                &mut out,
            ),
        }
        out
    }

    /// The plane applied, or refused, the Live from the last [`Effect::CallLive`]. Success
    /// sends `Applied(digest)`; a refusal is logged and nothing is sent, so the agent sends it
    /// again (E01-D011).
    pub fn live_returned(&mut self, now_ns: i64, result: Result<(), CallError>) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) {
            return out;
        }
        let Some(digest) = self.live_busy.take() else {
            return out;
        };
        match result {
            Ok(()) => out.push(applied(digest, false)),
            Err(error) => out.push(Effect::Log(format!("live not applied: {error}"))),
        }
        if self.phase == Phase::InitLives {
            self.init_lives = self.init_lives.saturating_sub(1);
            if self.init_lives == 0 {
                self.init_done(&mut out);
            }
        }
        if !self.exited() {
            self.pump(&mut out);
        }
        out
    }

    /// The plane's OnStop returned. Success is `Stopped(true)`; an error is logged and is
    /// `Stopped(false)`. The exit code does not change, because `Exiting` was sent, except
    /// [`CallError::Cancelled`] past the hook budget, which is exit 21 without `Stopped`.
    pub fn on_stop_returned(&mut self, now_ns: i64, result: Result<(), CallError>) -> Vec<Effect> {
        let mut out = Vec::new();
        if !self.begin(now_ns) {
            return out;
        }
        let Phase::OnStop { code, budget_ns } = self.phase else {
            return out;
        };
        self.armed = None;
        match result {
            Ok(()) => {
                out.push(stopped(true));
                self.exit(code, &mut out);
            }
            Err(CallError::Cancelled) if expired(budget_ns, now_ns) => {
                self.exit(EXIT_HUNG, &mut out);
            }
            Err(error) => {
                out.push(Effect::Log(format!("on stop: {error}")));
                out.push(stopped(false));
                self.exit(code, &mut out);
            }
        }
        out
    }

    fn begin(&mut self, now_ns: i64) -> bool {
        self.now_ns = now_ns;
        !self.exited()
    }

    fn exited(&self) -> bool {
        matches!(self.phase, Phase::Exited(_))
    }

    fn on_init_msg(&mut self, init: worker::Init, out: &mut Vec<Effect>) {
        match self.phase {
            Phase::AwaitInit => self.begin_init(init, out),
            Phase::Init { .. } | Phase::InitLives => {
                if self.pending.is_none() {
                    self.pending = Some(Pending::Init);
                }
            }
            Phase::AwaitStart => {
                self.fail_phase(Verb::Init, REASON_INIT_AFTER_INIT.to_owned(), false, out)
            }
            // Go stops reading Init and Start once Start has been taken.
            _ => {}
        }
    }

    fn on_start_msg(&mut self, start: worker::Start, out: &mut Vec<Effect>) {
        match self.phase {
            // Init never ran, so OnStop does not (E08-D037, E08-D008).
            Phase::AwaitInit => {
                self.fail_phase(Verb::Start, REASON_START_BEFORE_INIT.to_owned(), false, out)
            }
            Phase::Init { .. } | Phase::InitLives => {
                if self.pending.is_none() {
                    self.pending = Some(Pending::Start(start));
                }
            }
            Phase::AwaitStart => self.begin_start(start, out),
            _ => {}
        }
    }

    fn begin_init(&mut self, init: worker::Init, out: &mut Vec<Effect>) {
        if init.contract != self.config.contract {
            let why = format!("contract {}", init.contract);
            self.fail_phase(Verb::Init, why, false, out);
            return;
        }
        self.init_digest = init.live_digest;
        let deadline_ns = self.deadline(init.deadline_ms);
        if let Some(at) = deadline_ns {
            out.push(self.arm(TimerKind::InitDeadline, at));
        }
        self.phase = Phase::Init { deadline_ns };
        out.push(Effect::CallInit {
            payload: init.payload,
            deadline_ns,
        });
    }

    fn begin_start(&mut self, start: worker::Start, out: &mut Vec<Effect>) {
        if !start.contract.is_empty() && start.contract != self.config.contract {
            let why = format!("contract {}", start.contract);
            self.fail_phase(Verb::Start, why, true, out);
            return;
        }
        let deadline_ns = self.deadline(start.deadline_ms);
        if let Some(at) = deadline_ns {
            out.push(self.arm(TimerKind::StartDeadline, at));
        }
        self.phase = Phase::Start { deadline_ns };
        out.push(Effect::CallStart {
            payload: start.payload,
            deadline_ns,
        });
    }

    /// Init and the Lives held before it are done: `Done(INIT, ok)`, then a stop or the queued
    /// `Init` / `Start`. A stop wins over a queued `Start` (E08-D037: no OnStart, no OnStop).
    fn init_done(&mut self, out: &mut Vec<Effect>) {
        out.push(done(Verb::Init, true, String::new(), true));
        self.phase = Phase::AwaitStart;
        if self.stopping {
            let reason = self.stop_text();
            self.finish(Outcome::Done, reason, false, EXIT_DONE, false, out);
            return;
        }
        match self.pending.take() {
            Some(Pending::Start(start)) => self.begin_start(start, out),
            Some(Pending::Init) => {
                self.fail_phase(Verb::Init, REASON_INIT_AFTER_INIT.to_owned(), false, out)
            }
            None => {}
        }
    }

    fn on_live_msg(&mut self, live: Live, out: &mut Vec<Effect>) {
        if !self.ours(&live.contract) {
            return;
        }
        self.lives.push_back(live);
        if self.inited {
            self.pump(out);
        }
    }

    /// Start the next held Live when none is in flight.
    fn pump(&mut self, out: &mut Vec<Effect>) {
        if self.live_busy.is_some() {
            return;
        }
        if let Some(live) = self.lives.pop_front() {
            self.live_busy = Some(live.digest);
            out.push(Effect::CallLive {
                payload: live.payload,
            });
        }
    }

    /// A Command. One before Init has succeeded is dropped: Commands are full snapshots and
    /// the agent sends the next one after Init (PROTOCOL §6). The echo goes up unchanged
    /// before the plane gets it (PROTOCOL §9.8).
    fn on_relay_msg(&mut self, relay: worker::Relay, out: &mut Vec<Effect>) {
        if !self.inited || !self.ours(&relay.contract) {
            return;
        }
        let echo = worker::Relay {
            contract: self.config.contract.clone(),
            corr_id: relay.corr_id.clone(),
            payload: relay.payload.clone(),
        };
        out.push(send(worker_up::Msg::Relay(echo), false));
        out.push(Effect::CallRelay {
            corr_id: relay.corr_id,
            payload: relay.payload,
        });
    }

    /// The first `Stop`, EOF, or SIGTERM. Beat stops and the call in progress is cancelled
    /// (Go cancels `life`). Waiting for `Init` or `Start` ends `DONE` at once.
    fn halt(&mut self, out: &mut Vec<Effect>) {
        if self.stopping {
            return;
        }
        self.stopping = true;
        if self.beat_on {
            self.beat_on = false;
            out.push(Effect::StopBeat);
        }
        match self.phase {
            Phase::Init { .. } | Phase::Start { .. } | Phase::Run => out.push(Effect::Cancel),
            Phase::AwaitInit | Phase::AwaitStart => {
                let reason = self.stop_text();
                self.finish(Outcome::Done, reason, false, EXIT_DONE, false, out);
            }
            Phase::InitLives | Phase::OnStop { .. } | Phase::Exited(_) => {}
        }
    }

    /// An Init or Start the worker will not run: `Done(verb, false)`, then `INIT_FAILED`.
    /// `on_stop` is false for Init and for a deadline (E08-D007, E08-D008).
    fn fail_phase(&mut self, verb: Verb, why: String, on_stop: bool, out: &mut Vec<Effect>) {
        let what = if verb == Verb::Init { "init" } else { "start" };
        out.push(Effect::Log(format!("{what} failed: {why}")));
        out.push(done(verb, false, why.clone(), false));
        self.finish(Outcome::InitFailed, why, on_stop, EXIT_INIT, false, out);
    }

    /// Run asked for this ending. HUNG skips OnStop; the others are the plane ending on its own.
    fn exit_as(&mut self, outcome: Outcome, reason: String, out: &mut Vec<Effect>) {
        match outcome {
            Outcome::Failed => self.finish(outcome, reason, true, EXIT_FAILED, true, out),
            Outcome::Hung => self.finish(outcome, reason, false, EXIT_HUNG, false, out),
            Outcome::InitFailed => self.finish(outcome, reason, true, EXIT_INIT, true, out),
            Outcome::Done => self.finish(outcome, reason, true, EXIT_DONE, true, out),
            Outcome::Crashed | Outcome::Unspecified => {
                self.finish(Outcome::Crashed, reason, true, EXIT_CRASH, true, out)
            }
        }
    }

    /// `Exiting`, then OnStop when `on_stop`, then `Stopped`, then the exit (PROTOCOL §5.3).
    /// `self_end` is the plane ending on its own. That ending, and any ending after fd 3
    /// reached EOF, gives OnStop the hook budget (E08-D009, E03-D027).
    fn finish(
        &mut self,
        outcome: Outcome,
        reason: String,
        on_stop: bool,
        code: i32,
        self_end: bool,
        out: &mut Vec<Effect>,
    ) {
        self.armed = None;
        self.pending = None;
        out.push(send(
            worker_up::Msg::Exiting(Exiting {
                outcome: outcome as i32,
                reason,
                detail: String::new(),
            }),
            false,
        ));
        if !on_stop {
            out.push(stopped(false));
            self.exit(code, out);
            return;
        }
        let (deadline_ns, budget_ns) = self.stop_limit(self_end);
        if let Some(at) = budget_ns {
            out.push(self.arm(TimerKind::OnStopBudget, at));
        }
        self.phase = Phase::OnStop { code, budget_ns };
        out.push(Effect::CallOnStop { deadline_ns });
    }

    /// OnStop's deadline, and the budget past which exit 21 does not wait for it.
    ///
    /// A `Stop` was read: the rest of its grace, waited for while the shim is there to end the
    /// process when grace runs out; once the shim is gone, the worker ends it at the same
    /// point. The plane ended on its own, or the shim is gone: the hook budget, not waited
    /// for. Otherwise (SIGTERM, a Start failure) there is no deadline (E08-D009, E03-D027).
    fn stop_limit(&self, self_end: bool) -> (Option<i64>, Option<i64>) {
        if let Some((read_ns, grace_ms)) = self.grace {
            let deadline = read_ns.saturating_add(i64::from(grace_ms).saturating_mul(NS_PER_MS));
            return (Some(deadline), self.shim_gone.then_some(deadline));
        }
        if (self_end || self.shim_gone) && self.hook_timeout_ns > 0 {
            let deadline = self.now_ns.saturating_add(self.hook_timeout_ns);
            return (Some(deadline), Some(deadline));
        }
        (None, None)
    }

    fn exit(&mut self, code: i32, out: &mut Vec<Effect>) {
        self.armed = None;
        self.phase = Phase::Exited(code);
        out.push(Effect::Exit(code));
    }

    fn stop_text(&self) -> String {
        match &self.stop_reason {
            Some(reason) if !reason.is_empty() => reason.clone(),
            _ => REASON_SIGNAL.to_owned(),
        }
    }

    fn ours(&self, contract: &str) -> bool {
        contract.is_empty() || contract == self.config.contract
    }

    fn deadline(&self, ms: u32) -> Option<i64> {
        (ms > 0).then(|| {
            self.now_ns
                .saturating_add(i64::from(ms).saturating_mul(NS_PER_MS))
        })
    }

    fn arm(&mut self, kind: TimerKind, deadline_ns: i64) -> Effect {
        self.next_timer += 1;
        let token = TimerToken {
            kind,
            id: self.next_timer,
        };
        self.armed = Some(token);
        let after = Duration::from_nanos(deadline_ns.saturating_sub(self.now_ns).max(0) as u64);
        Effect::ArmTimer {
            token,
            after,
            deadline_ns,
        }
    }
}

fn expired(deadline_ns: Option<i64>, now_ns: i64) -> bool {
    deadline_ns.is_some_and(|deadline| now_ns >= deadline)
}

fn duration_ns(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

fn send(msg: worker_up::Msg, fatal: bool) -> Effect {
    Effect::Send {
        msg: WorkerUp { msg: Some(msg) },
        fatal,
    }
}

fn done(verb: Verb, ok: bool, error: String, fatal: bool) -> Effect {
    send(
        worker_up::Msg::Done(Done {
            verb: verb as i32,
            ok,
            error,
        }),
        fatal,
    )
}

fn applied(digest: String, fatal: bool) -> Effect {
    send(worker_up::Msg::Applied(Applied { digest }), fatal)
}

fn stopped(on_stop_completed: bool) -> Effect {
    send(
        worker_up::Msg::Stopped(Stopped { on_stop_completed }),
        false,
    )
}
