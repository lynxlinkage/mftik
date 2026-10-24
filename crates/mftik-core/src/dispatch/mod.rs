//! Session dispatcher: TD FIFO, MD ring, prehook, and hook budget.
//!
//! The host injects time with [`Dispatcher::set_now_ns`] and arms timers from
//! the deadlines [`Timer::after`] reports. NATS, OS timers, and the C4
//! connection stay outside this crate (E02-D001).
//!
//! [`Dispatcher::run`] blocks the caller. Hooks run one at a time on a
//! dedicated thread. A watchdog thread publishes [`RunEnd::Hung`] when the
//! injected clock says the running hook has used up `hook_timeout`. The host
//! then sends `Exiting(HUNG, "hook timeout")` and exits 21, and it does not
//! run the strategy `on_stop` (E02-D002, PROTOCOL §9.5).
//!
//! Hooks may call [`Dispatcher::push_td`], [`Dispatcher::push_md`],
//! [`Dispatcher::push_book`], [`Dispatcher::exit`], [`Dispatcher::fail`],
//! [`Dispatcher::set_now_ns`], [`Dispatcher::stats`], and [`Timer::fire`].
//! They must not call [`Dispatcher::run`], [`Dispatcher::on_stop`], or
//! [`Dispatcher::wait_exited`]: those wait for this same thread.

mod envelope;
mod ring;

use std::any::Any;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use ring::Ring;

/// TD buffer capacity (E08-D015). Not a session-spec field. A full buffer ends the session `FAILED`.
pub const TD_CAP: usize = 65536;

/// MD ring size, one preallocated buffer (E08-D016).
pub const MD_BYTES: usize = 32 << 20;

/// Soft hook limit (E08-D017). A hook that runs longer counts one `hook_slow`.
pub const SLOW_AFTER: Duration = Duration::from_millis(100);

/// [`SLOW_AFTER`] in nanoseconds. Equality does not count; one nanosecond over does.
pub const SLOW_AFTER_NS: i64 = 100_000_000;

/// `Exiting` reason when the TD buffer cannot take another entry.
pub const REASON_TD_FULL: &str = "td buffer full";

/// `Exiting` reason when a hook reaches `hook_timeout` (E08-D003).
pub const REASON_HUNG: &str = "hook timeout";

const NS_PER_MS: u128 = 1_000_000;

/// Why [`Dispatcher::run`] returned. The host sends `Exiting` with this outcome
/// and, except for [`RunEnd::Hung`], then calls [`Dispatcher::on_stop`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunEnd {
    /// `Exiting(DONE)`, deliver the TD buffer, run `on_stop`, exit 0.
    Done,
    /// `Exiting(FAILED, reason)`, deliver the TD buffer, run `on_stop`, exit 30.
    Failed { reason: String },
    /// `Exiting(CRASHED, reason)`, do not deliver the TD buffer, run `on_stop`, exit 20.
    Crashed { reason: String },
    /// `Exiting(HUNG, "hook timeout")`, do not run `on_stop`, exit 21.
    ///
    /// [`Dispatcher::run`] returns while the hook is still on the dispatcher
    /// thread. [`Dispatcher::on_stop`] completes only after that hook returns,
    /// and it does not call the strategy callback.
    Hung,
}

impl RunEnd {
    pub fn reason(&self) -> &str {
        match self {
            RunEnd::Done => "",
            RunEnd::Failed { reason } | RunEnd::Crashed { reason } => reason,
            RunEnd::Hung => REASON_HUNG,
        }
    }

    /// `DONE` and `FAILED` hand the queued TD entries to hooks before `on_stop` (E08-D003).
    pub fn delivers_td(&self) -> bool {
        matches!(self, RunEnd::Done | RunEnd::Failed { .. })
    }
}

/// Failure of [`Dispatcher::on_stop`]. The `Exiting` outcome already sent does not change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopError {
    /// Injected time reached the stop deadline between TD entries.
    Deadline,
    /// The host cancelled the stop between TD entries.
    Cancelled,
    /// A TD hook panicked during the stop drain. The strategy `on_stop` still ran
    /// unless the deadline had also expired.
    HookPanic(String),
    /// The strategy `on_stop` returned an error. The `Exiting` outcome does not change.
    OnStop(String),
    /// The strategy `on_stop` panicked (`dispatch: onstop panic: ...`).
    OnStopPanic(String),
    /// `unsubscribe_td` or another stop step panicked (`dispatch: stop: ...`).
    /// Later stop steps, including `close` of the TD buffer, did not run.
    StopPanic(String),
}

impl fmt::Display for StopError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StopError::Deadline => write!(formatter, "deadline exceeded"),
            StopError::Cancelled => write!(formatter, "cancelled"),
            StopError::HookPanic(message)
            | StopError::OnStop(message)
            | StopError::OnStopPanic(message)
            | StopError::StopPanic(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for StopError {}

/// [`Dispatcher::push_td`] rejected the entry. MD pushes do not return [`PushError::Full`]:
/// a ring drop is backpressure and yields `Ok(())`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushError {
    /// The TD buffer is at [`TD_CAP`]. The entry was not queued. The session is ending `FAILED`.
    Full,
    /// The session is no longer accepting this kind of entry.
    Closed,
}

impl fmt::Display for PushError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::Full => write!(formatter, "dispatch: td buffer full"),
            PushError::Closed => write!(formatter, "dispatch: stopping"),
        }
    }
}

impl std::error::Error for PushError {}

/// One buffer's depth, dropped count, and the oldest event handed to a hook since the
/// previous [`Dispatcher::stats`] call (PROTOCOL §6.1). Depth is entries waiting, not the
/// hook that is running. `dropped` accumulates. `max_age_ms` is cleared by `stats`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueStat {
    pub depth: u32,
    pub dropped: u64,
    pub max_age_ms: u32,
}

/// Both buffers and the hook counters. `hook_blocked_ms` is 0 when no hook is running.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub td: QueueStat,
    pub md: QueueStat,
    pub hook_blocked_ms: u64,
    pub hook_slow: u64,
}

/// Snapshot passed to the strategy `on_stop`. `deadline_ns` is on the same clock as
/// [`Dispatcher::set_now_ns`]. `None` means the host set no deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StopInfo {
    pub deadline_ns: Option<i64>,
    pub now_ns: i64,
}

/// Budget for [`Dispatcher::on_stop`]. The dispatcher checks it between TD entries.
/// It does not abort a hook that is already inside the callback; the host does that
/// when `on_stop` itself overruns `hook_timeout` or the stop grace (E08-D009).
#[derive(Clone, Debug)]
pub struct StopRequest {
    deadline_ns: Option<i64>,
    cancel: Arc<AtomicBool>,
}

impl StopRequest {
    pub fn new(deadline_ns: Option<i64>) -> Self {
        Self {
            deadline_ns,
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn deadline_ns(&self) -> Option<i64> {
        self.deadline_ns
    }

    /// End the TD drain at the next entry boundary.
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

impl Default for StopRequest {
    fn default() -> Self {
        Self::new(None)
    }
}

pub type LogCallback = Arc<dyn Fn(&str) + Send + Sync>;
pub type ArmCallback = Arc<dyn Fn(u64, i64) + Send + Sync>;
pub type DisarmCallback = Arc<dyn Fn(u64) + Send + Sync>;
pub type OnStopHook = Box<dyn FnMut(&StopInfo) -> Result<(), String> + Send>;
pub type MdHook = Box<dyn FnMut(&[u8]) + Send>;

/// Session inputs. Zero [`Config::ring_bytes`] selects [`MD_BYTES`]. Zero
/// [`Config::hook_timeout`] does not arm the hard limit.
pub struct Config {
    pub recv_window: Duration,
    pub hook_timeout: Duration,
    /// Injected clock at construction. The same origin as `md_ts_ns`.
    pub now_ns: i64,
    /// MD ring size. Zero selects [`MD_BYTES`].
    pub ring_bytes: usize,
    /// Strategy callback. `Err` is reported on [`StopError::OnStop`] and does not
    /// change the `Exiting` outcome. A panic is [`StopError::OnStopPanic`].
    pub on_stop: Option<OnStopHook>,
    /// Runs on the dispatcher thread before the TD drain, for `DONE` and `FAILED` only.
    pub unsubscribe_td: Option<Box<dyn FnMut() + Send>>,
    /// Market-data bytes alias the ring and are valid only for this call.
    pub feed: Option<MdHook>,
    /// `orderbook_delta`. Not filtered by the stale-window prehook. Bytes alias the ring.
    pub book: Option<MdHook>,
    pub log: Option<LogCallback>,
    /// Host arms an OS timer and later calls [`Timer::fire`]. `deadline_ns` is injected time.
    pub arm_timer: Option<ArmCallback>,
    pub disarm_timer: Option<DisarmCallback>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            recv_window: Duration::ZERO,
            hook_timeout: Duration::ZERO,
            now_ns: 0,
            ring_bytes: 0,
            on_stop: None,
            unsubscribe_td: None,
            feed: None,
            book: None,
            log: None,
            arm_timer: None,
            disarm_timer: None,
        }
    }
}

/// One session's dispatcher. Push methods are safe for concurrent use.
/// Hook callbacks run on the dispatcher thread, one at a time.
#[derive(Clone)]
pub struct Dispatcher {
    shared: Arc<Shared>,
}

/// A timer whose callback runs on the TD buffer. Fires that arrive while one run
/// is already queued run once (E08-D003).
#[derive(Clone)]
pub struct Timer {
    shared: Arc<Shared>,
    id: usize,
}

struct Shared {
    mu: Mutex<State>,
    cv: Condvar,
    on_stop: Slot<OnStopHook>,
    unsubscribe_td: Slot<Box<dyn FnMut() + Send>>,
    feed: Slot<MdHook>,
    book: Slot<MdHook>,
    log: Option<LogCallback>,
    arm_timer: Option<ArmCallback>,
    disarm_timer: Option<DisarmCallback>,
    watch_name: String,
}

struct State {
    now_ns: i64,
    recv_window_ns: i64,
    hook_timeout_ns: i64,
    td: TdQueue,
    ring: Ring,
    timers: Vec<TimerSlot>,
    outcome: Option<RunEnd>,
    outcome_set: bool,
    stopping: bool,
    hung: bool,
    td_closed: bool,
    md_closed: bool,
    in_stop: bool,
    hook_on: bool,
    hook_start_ns: i64,
    hook_slow: u64,
    td_max_ns: i64,
    md_max_ns: i64,
    pre_drop: u64,
    stop_panic: Option<String>,
    run_started: bool,
    run_result: Option<RunEnd>,
    stop_req: Option<StopRequest>,
    stop_res: Option<Result<(), StopError>>,
    exited: bool,
}

struct TimerSlot {
    queued: bool,
    dead: bool,
    deadline_ns: Option<i64>,
    cb: Option<Box<dyn FnMut() + Send>>,
}

struct TdQueue {
    buf: Vec<Option<TdItem>>,
    head: usize,
    len: usize,
}

struct TdItem {
    at_ns: i64,
    kind: TdKind,
}

enum TdKind {
    Once(Box<dyn FnOnce() + Send>),
    Timer(usize),
}

struct Slot<T> {
    inner: Mutex<Option<T>>,
}

impl<T> Slot<T> {
    fn new(value: Option<T>) -> Self {
        Self {
            inner: Mutex::new(value),
        }
    }

    fn is_some(&self) -> bool {
        lock(&self.inner).is_some()
    }

    fn with_mut<R>(&self, body: impl FnOnce(&mut T) -> R) -> Option<R> {
        let taken = lock(&self.inner).take();
        struct Restore<'a, T> {
            slot: &'a Mutex<Option<T>>,
            value: Option<T>,
        }
        impl<T> Drop for Restore<'_, T> {
            fn drop(&mut self) {
                if let Some(value) = self.value.take() {
                    *lock(self.slot) = Some(value);
                }
            }
        }
        let mut restore = Restore {
            slot: &self.inner,
            value: taken,
        };
        let value = restore.value.as_mut()?;
        Some(body(value))
    }
}

impl Dispatcher {
    pub fn new(config: Config) -> Self {
        let tag = next_thread_tag();
        let shared = Arc::new(Shared {
            mu: Mutex::new(State {
                now_ns: config.now_ns,
                recv_window_ns: duration_to_ns(config.recv_window),
                hook_timeout_ns: duration_to_ns(config.hook_timeout),
                td: TdQueue::new(),
                ring: Ring::new(config.ring_bytes),
                timers: Vec::new(),
                outcome: None,
                outcome_set: false,
                stopping: false,
                hung: false,
                td_closed: false,
                md_closed: false,
                in_stop: false,
                hook_on: false,
                hook_start_ns: 0,
                hook_slow: 0,
                td_max_ns: 0,
                md_max_ns: 0,
                pre_drop: 0,
                stop_panic: None,
                run_started: false,
                run_result: None,
                stop_req: None,
                stop_res: None,
                exited: false,
            }),
            cv: Condvar::new(),
            on_stop: Slot::new(config.on_stop),
            unsubscribe_td: Slot::new(config.unsubscribe_td),
            feed: Slot::new(config.feed),
            book: Slot::new(config.book),
            log: config.log,
            arm_timer: config.arm_timer,
            disarm_timer: config.disarm_timer,
            watch_name: format!("mftik-w{tag}"),
        });
        Self { shared }
    }

    /// Move the injected clock. The watchdog reads this; it does not sleep or read the wall clock.
    /// The host must keep calling it while a hook runs, or a hard timeout cannot fire.
    pub fn set_now_ns(&self, now_ns: i64) {
        let mut state = lock(&self.shared.mu);
        state.now_ns = now_ns;
        self.shared.cv.notify_all();
    }

    pub fn now_ns(&self) -> i64 {
        lock(&self.shared.mu).now_ns
    }

    /// Hard limit passed to the host for an `on_stop` that follows a self-exit (E08-D009).
    pub fn hook_timeout(&self) -> Duration {
        let ns = lock(&self.shared.mu).hook_timeout_ns;
        if ns <= 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(ns as u64)
        }
    }

    /// Queue `hook` on the TD buffer. [`PushError::Full`] means the buffer is full and the
    /// session is ending `FAILED`; `hook` was not queued.
    pub fn push_td<F>(&self, hook: F) -> Result<(), PushError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.shared.enqueue(TdKind::Once(Box::new(hook)))
    }

    /// Copy one market-data message into the ring. A dropped message returns `Ok(())`.
    pub fn push_md(&self, msg: &[u8]) -> Result<(), PushError> {
        self.shared.push_md(msg, false)
    }

    /// Copy one `orderbook_delta` into the ring. It skips the stale-window prehook.
    pub fn push_book(&self, msg: &[u8]) -> Result<(), PushError> {
        self.shared.push_md(msg, true)
    }

    /// Timer callback. It runs on the TD buffer, on the dispatcher thread.
    pub fn timer<F>(&self, hook: F) -> Timer
    where
        F: FnMut() + Send + 'static,
    {
        let mut state = lock(&self.shared.mu);
        let id = state.timers.len();
        state.timers.push(TimerSlot {
            queued: false,
            dead: false,
            deadline_ns: None,
            cb: Some(Box::new(hook)),
        });
        Timer {
            shared: Arc::clone(&self.shared),
            id,
        }
    }

    /// End `DONE` after the current hook. The first stop wins.
    pub fn exit(&self) {
        self.shared.request_stop(RunEnd::Done);
    }

    /// End `FAILED` after the current hook. The first stop wins.
    pub fn fail(&self, reason: impl Into<String>) {
        self.shared.request_stop(RunEnd::Failed {
            reason: reason.into(),
        });
    }

    pub fn stats(&self) -> Stats {
        self.shared.stats()
    }

    /// Block until the session should send `Exiting`. Hooks run on the spawned
    /// dispatcher thread. A second call waits for the same result.
    pub fn run(&self) -> RunEnd {
        let start = {
            let mut state = lock(&self.shared.mu);
            if state.run_started {
                false
            } else {
                state.run_started = true;
                true
            }
        };
        if start {
            self.spawn_threads();
        }
        self.shared.wait_result()
    }

    /// TD delivery and the strategy callback, on the dispatcher thread.
    /// Call this after [`Dispatcher::run`] returns and the host has sent `Exiting`.
    /// After [`RunEnd::Hung`], this returns `Ok(())` once the stuck hook returns
    /// and does not call the strategy callback.
    pub fn on_stop(&self, request: &StopRequest) -> Result<(), StopError> {
        self.shared.on_stop(request)
    }

    /// Block until the dispatcher thread has left its loop. [`Dispatcher::run`] can
    /// return earlier, on [`RunEnd::Hung`].
    pub fn wait_exited(&self) {
        self.shared.wait_exited();
    }

    fn spawn_threads(&self) {
        let tag = self.shared.watch_name.trim_start_matches("mftik-w");
        let shared = Arc::clone(&self.shared);
        thread::Builder::new()
            .name(format!("mftik-d{tag}"))
            .spawn(move || shared.drive())
            .expect("spawn dispatcher thread");
        let shared = Arc::clone(&self.shared);
        let watch_name = self.shared.watch_name.clone();
        thread::Builder::new()
            .name(watch_name)
            .spawn(move || shared.watch())
            .expect("spawn watchdog thread");
    }
}

impl Timer {
    pub fn id(&self) -> u64 {
        self.id as u64
    }

    /// Deadline of the last [`Timer::after`], on the injected clock. `None` if `after` has not run.
    pub fn deadline_ns(&self) -> Option<i64> {
        lock(&self.shared.mu).timers[self.id].deadline_ns
    }

    /// Ask the host to fire at `now + delay`. A delay of zero is the current instant.
    /// A timer stopped by session shutdown returns `None` and does not arm.
    /// A run already queued is not duplicated; the host timer is still rearmed.
    pub fn after(&self, delay: Duration) -> Option<i64> {
        let delay_ns = duration_to_ns(delay);
        let deadline = {
            let mut state = lock(&self.shared.mu);
            if state.timers[self.id].dead {
                return None;
            }
            let deadline = state.now_ns.saturating_add(delay_ns);
            state.timers[self.id].deadline_ns = Some(deadline);
            deadline
        };
        self.shared.arm_timer(self.id as u64, deadline);
        Some(deadline)
    }

    /// Queue one run unless one is already queued, the TD buffer is closed, or the session is `HUNG`.
    /// A full buffer ends the session `FAILED` and does not queue this run.
    ///
    /// Stopping a timer does not by itself ignore `fire`. The host should stop calling it after
    /// [`Config::disarm_timer`]. This matches the Go dispatcher, whose `Fire` does not read the
    /// stopped flag.
    pub fn fire(&self) {
        let mut full = false;
        {
            let mut state = lock(&self.shared.mu);
            let queued = state.timers[self.id].queued;
            if queued || state.td_closed || state.hung {
                return;
            }
            if state.td.len() == TD_CAP {
                state.set_stop(failed_full());
                full = true;
                self.shared.cv.notify_all();
            } else {
                state.timers[self.id].queued = true;
                let at_ns = state.now_ns;
                state.td.push(TdItem {
                    at_ns,
                    kind: TdKind::Timer(self.id),
                });
                self.shared.cv.notify_all();
            }
        }
        if full {
            self.shared.log(REASON_TD_FULL);
        }
    }
}

impl Shared {
    fn drive(&self) {
        struct ExitGuard<'a>(&'a Shared);
        impl Drop for ExitGuard<'_> {
            fn drop(&mut self) {
                let mut state = lock(&self.0.mu);
                state.exited = true;
                self.0.cv.notify_all();
            }
        }
        let _exit = ExitGuard(self);
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.loop_body())) {
            self.finish(RunEnd::Crashed {
                reason: format!("dispatch: loop: {}", panic_message(payload.as_ref())),
            });
        }
    }

    fn watch(&self) {
        let mut state = lock(&self.mu);
        loop {
            if state.exited {
                return;
            }
            if hook_exceeded(&state) {
                state.hung = true;
                drop(state);
                self.finish(RunEnd::Hung);
                state = lock(&self.mu);
                continue;
            }
            state = wait(&self.cv, state);
        }
    }

    fn loop_body(&self) {
        loop {
            enum Next {
                Td(TdItem),
                Md,
                Stop,
                Hung,
            }
            let next = {
                let mut state = lock(&self.mu);
                loop {
                    if state.hung {
                        break Next::Hung;
                    }
                    if state.stopping {
                        break Next::Stop;
                    }
                    if !state.td.is_empty() {
                        let item = take_td(&mut state);
                        break Next::Td(item);
                    }
                    if state.ring.waiting() > 0 {
                        debug_assert!(!state.ring.is_pinned());
                        break Next::Md;
                    }
                    state = wait(&self.cv, state);
                }
            };
            match next {
                Next::Hung => {
                    self.finish(RunEnd::Hung);
                    return;
                }
                Next::Stop => {
                    self.phase1();
                    return;
                }
                Next::Td(item) => self.run_td(item),
                Next::Md => self.run_md(),
            }
        }
    }

    fn phase1(&self) {
        let end = {
            let mut state = lock(&self.mu);
            state.md_closed = true;
            state.ring.discard();
            state.outcome.clone().unwrap_or(RunEnd::Done)
        };
        self.stop_timers();
        self.finish(end.clone());
        let request = {
            let mut state = lock(&self.mu);
            while state.stop_req.is_none() {
                state = wait(&self.cv, state);
            }
            state.stop_req.clone().expect("stop request is set")
        };
        let result = self.phase2(&request, end.delivers_td());
        let mut state = lock(&self.mu);
        state.stop_res = Some(result);
        self.cv.notify_all();
    }

    fn phase2(&self, request: &StopRequest, deliver: bool) -> Result<(), StopError> {
        {
            let mut state = lock(&self.mu);
            state.in_stop = true;
        }
        match catch_unwind(AssertUnwindSafe(|| self.phase2_inner(request, deliver))) {
            Ok(result) => result,
            Err(payload) => Err(StopError::StopPanic(format!(
                "dispatch: stop: {}",
                panic_message(payload.as_ref())
            ))),
        }
    }

    fn phase2_inner(&self, request: &StopRequest, deliver: bool) -> Result<(), StopError> {
        if self.stop_expired(request) {
            self.close_td();
            return Err(self.stop_reason(request));
        }
        let delivered = if deliver {
            let _ = self.unsubscribe_td.with_mut(|callback| callback());
            self.drain(request)
        } else {
            Ok(())
        };
        self.close_td();
        self.stop_timers();
        if self.stop_expired(request) {
            return Err(self.stop_reason(request));
        }
        if self.on_stop.is_some() {
            let now_ns = self.now_ns();
            let info = StopInfo {
                deadline_ns: request.deadline_ns(),
                now_ns,
            };
            self.run_on_stop(&info)?;
        }
        delivered
    }

    fn drain(&self, request: &StopRequest) -> Result<(), StopError> {
        loop {
            if self.stop_expired(request) {
                return Err(self.stop_reason(request));
            }
            let item = {
                let mut state = lock(&self.mu);
                if state.td.is_empty() {
                    return Ok(());
                }
                take_td(&mut state)
            };
            self.run_td(item);
            if let Some(message) = self.take_stop_panic() {
                return Err(StopError::HookPanic(message));
            }
        }
    }

    fn run_on_stop(&self, info: &StopInfo) -> Result<(), StopError> {
        self.begin_hook_at(info.now_ns);
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.on_stop.with_mut(|callback| callback(info))
        }));
        self.finish_hook(info.now_ns);
        match result {
            Ok(Some(Ok(()))) | Ok(None) => Ok(()),
            Ok(Some(Err(message))) => Err(StopError::OnStop(message)),
            Err(payload) => Err(StopError::OnStopPanic(format!(
                "dispatch: onstop panic: {}",
                panic_message(payload.as_ref())
            ))),
        }
    }

    fn run_td(&self, item: TdItem) {
        match item.kind {
            TdKind::Once(hook) => self.run_hook(hook),
            TdKind::Timer(id) => self.run_hook(|| self.invoke_timer(id)),
        }
    }

    fn invoke_timer(&self, id: usize) {
        let callback = {
            let mut state = lock(&self.mu);
            state.timers[id].cb.take()
        };
        struct Restore<'a> {
            shared: &'a Shared,
            id: usize,
            callback: Option<Box<dyn FnMut() + Send>>,
        }
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                if let Some(callback) = self.callback.take() {
                    let mut state = lock(&self.shared.mu);
                    if let Some(slot) = state.timers.get_mut(self.id) {
                        slot.cb = Some(callback);
                    }
                }
            }
        }
        let mut restore = Restore {
            shared: self,
            id,
            callback,
        };
        if let Some(callback) = restore.callback.as_mut() {
            callback();
        }
    }

    fn run_md(&self) {
        let peeked = {
            let mut state = lock(&self.mu);
            state.ring.peek().map(|(book, len)| {
                (
                    book,
                    len,
                    state.ring.as_ptr(),
                    state.now_ns,
                    state.recv_window_ns,
                )
            })
        };
        let Some((book, len, ptr, now_ns, window_ns)) = peeked else {
            return;
        };
        struct Release<'a>(&'a Shared);
        impl Drop for Release<'_> {
            fn drop(&mut self) {
                lock(&self.0.mu).ring.release();
            }
        }
        // Declared before the slice so the slice ends before the pin is released.
        let _release = Release(self);
        // SAFETY: `ptr`/`len` is the pinned prefix of the ring. The bytes are
        // `UnsafeCell`, the allocation does not move, and pushers write only
        // past `len` until `Release` drops. The slice does not escape this
        // function: callbacks take `&[u8]` for the call.
        let msg = unsafe { std::slice::from_raw_parts(ptr, len) };

        let ts = match envelope::md_ts_ns(msg) {
            Ok(ts) => ts,
            Err(err) => {
                lock(&self.mu).pre_drop += 1;
                self.log(&format!("md envelope rejected: {err}"));
                return;
            }
        };
        let (stale, age) = stale(ts, now_ns, window_ns);
        if book {
            if self.book.is_some() {
                self.note_md(age);
                self.run_hook(|| {
                    let _ = self.book.with_mut(|callback| callback(msg));
                });
            }
            return;
        }
        if stale {
            lock(&self.mu).pre_drop += 1;
            return;
        }
        if self.feed.is_some() {
            self.note_md(age);
            self.run_hook(|| {
                let _ = self.feed.with_mut(|callback| callback(msg));
            });
        }
    }

    fn run_hook(&self, hook: impl FnOnce()) {
        let start = self.begin_hook();
        let result = catch_unwind(AssertUnwindSafe(hook));
        self.finish_hook(start);
        if let Err(payload) = result {
            self.note_panic(payload.as_ref());
        }
    }

    fn begin_hook(&self) -> i64 {
        let mut state = lock(&self.mu);
        let start = state.now_ns;
        state.hook_on = true;
        state.hook_start_ns = start;
        self.cv.notify_all();
        start
    }

    fn begin_hook_at(&self, start: i64) {
        let mut state = lock(&self.mu);
        state.hook_on = true;
        state.hook_start_ns = start;
        self.cv.notify_all();
    }

    fn finish_hook(&self, start: i64) {
        let slow = {
            let mut state = lock(&self.mu);
            let elapsed = age_ns(state.now_ns, start);
            state.hook_on = false;
            let slow = elapsed > SLOW_AFTER_NS;
            if slow {
                state.hook_slow += 1;
            }
            self.cv.notify_all();
            slow
        };
        if slow {
            self.log("hook exceeded 100ms");
        }
    }

    fn note_panic(&self, payload: &dyn Any) {
        let message = format!("hook panic: {}", panic_message(payload));
        let mut state = lock(&self.mu);
        if state.in_stop {
            if state.stop_panic.is_none() {
                state.stop_panic = Some(message);
            }
            return;
        }
        if state.hung || state.outcome_set {
            return;
        }
        state.outcome = Some(RunEnd::Crashed { reason: message });
        state.outcome_set = true;
        state.stopping = true;
        self.cv.notify_all();
    }

    fn note_md(&self, age: i64) {
        let mut state = lock(&self.mu);
        if age > state.md_max_ns {
            state.md_max_ns = age;
        }
    }

    fn take_stop_panic(&self) -> Option<String> {
        let mut state = lock(&self.mu);
        state.stop_panic.take()
    }

    fn close_td(&self) {
        lock(&self.mu).td_closed = true;
    }

    fn stop_timers(&self) {
        let ids = {
            let mut state = lock(&self.mu);
            let mut ids = Vec::new();
            for (id, timer) in state.timers.iter_mut().enumerate() {
                let armed = timer.deadline_ns.is_some();
                let was_dead = timer.dead;
                timer.dead = true;
                if armed && !was_dead {
                    ids.push(id as u64);
                }
            }
            ids
        };
        for id in ids {
            self.disarm_timer(id);
        }
    }

    fn stop_expired(&self, request: &StopRequest) -> bool {
        if let Some(deadline) = request.deadline_ns
            && self.now_ns() >= deadline
        {
            return true;
        }
        request.cancel.load(Ordering::Relaxed)
    }

    fn stop_reason(&self, request: &StopRequest) -> StopError {
        if let Some(deadline) = request.deadline_ns
            && self.now_ns() >= deadline
        {
            return StopError::Deadline;
        }
        StopError::Cancelled
    }

    fn now_ns(&self) -> i64 {
        lock(&self.mu).now_ns
    }

    fn enqueue(&self, kind: TdKind) -> Result<(), PushError> {
        let mut full = false;
        let result = {
            let mut state = lock(&self.mu);
            if state.td_closed || state.hung {
                Err(PushError::Closed)
            } else if state.td.len() == TD_CAP {
                state.set_stop(failed_full());
                full = true;
                self.cv.notify_all();
                Err(PushError::Full)
            } else {
                let at_ns = state.now_ns;
                state.td.push(TdItem { at_ns, kind });
                self.cv.notify_all();
                Ok(())
            }
        };
        if full {
            self.log(REASON_TD_FULL);
        }
        result
    }

    fn push_md(&self, msg: &[u8], book: bool) -> Result<(), PushError> {
        let mut state = lock(&self.mu);
        if state.md_closed || state.hung {
            return Err(PushError::Closed);
        }
        if state.ring.push(msg, book) {
            self.cv.notify_all();
        }
        Ok(())
    }

    fn request_stop(&self, end: RunEnd) {
        let mut state = lock(&self.mu);
        state.set_stop(end);
        self.cv.notify_all();
    }

    fn stats(&self) -> Stats {
        let mut state = lock(&self.mu);
        let blocked_ms = if state.hook_on {
            ms64(age_ns(state.now_ns, state.hook_start_ns))
        } else {
            0
        };
        let stats = Stats {
            td: QueueStat {
                depth: u32_len(state.td.len()),
                dropped: 0,
                max_age_ms: ms32(state.td_max_ns),
            },
            md: QueueStat {
                depth: u32_len(state.ring.waiting()),
                dropped: state.ring.dropped() + state.pre_drop,
                max_age_ms: ms32(state.md_max_ns),
            },
            hook_blocked_ms: blocked_ms,
            hook_slow: state.hook_slow,
        };
        state.td_max_ns = 0;
        state.md_max_ns = 0;
        stats
    }

    fn finish(&self, end: RunEnd) {
        let mut state = lock(&self.mu);
        if state.run_result.is_none() {
            state.run_result = Some(end);
            self.cv.notify_all();
        }
    }

    fn wait_result(&self) -> RunEnd {
        let mut state = lock(&self.mu);
        while state.run_result.is_none() {
            state = wait(&self.cv, state);
        }
        state.run_result.clone().expect("run result is set")
    }

    fn on_stop(&self, request: &StopRequest) -> Result<(), StopError> {
        let mut state = lock(&self.mu);
        if let Some(result) = &state.stop_res {
            return result.clone();
        }
        if state.stop_req.is_none() {
            state.stop_req = Some(request.clone());
            self.cv.notify_all();
        }
        while state.stop_res.is_none() && !state.exited {
            state = wait(&self.cv, state);
        }
        state.stop_res.clone().unwrap_or(Ok(()))
    }

    fn wait_exited(&self) {
        let mut state = lock(&self.mu);
        while !state.exited {
            state = wait(&self.cv, state);
        }
    }

    fn arm_timer(&self, id: u64, deadline_ns: i64) {
        if let Some(arm) = &self.arm_timer {
            arm(id, deadline_ns);
        }
    }

    fn disarm_timer(&self, id: u64) {
        if let Some(disarm) = &self.disarm_timer {
            disarm(id);
        }
    }

    fn log(&self, message: &str) {
        if let Some(log) = &self.log {
            log(message);
        }
    }
}

impl State {
    fn set_stop(&mut self, end: RunEnd) {
        if self.outcome_set || self.hung {
            return;
        }
        self.outcome = Some(end);
        self.outcome_set = true;
        self.stopping = true;
    }
}

impl TdQueue {
    fn new() -> Self {
        Self {
            buf: (0..TD_CAP).map(|_| None).collect(),
            head: 0,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn push(&mut self, item: TdItem) {
        let index = (self.head + self.len) % self.buf.len();
        debug_assert!(self.buf[index].is_none());
        self.buf[index] = Some(item);
        self.len += 1;
    }

    fn pop(&mut self) -> Option<TdItem> {
        if self.len == 0 {
            return None;
        }
        let item = self.buf[self.head].take();
        self.head = (self.head + 1) % self.buf.len();
        self.len -= 1;
        item
    }
}

fn take_td(state: &mut State) -> TdItem {
    let item = state.td.pop().expect("td queue is not empty");
    if let TdKind::Timer(id) = &item.kind {
        let id = *id;
        if let Some(slot) = state.timers.get_mut(id) {
            slot.queued = false;
        }
    }
    let age = age_ns(state.now_ns, item.at_ns);
    if age > state.td_max_ns {
        state.td_max_ns = age;
    }
    item
}

fn hook_exceeded(state: &State) -> bool {
    if state.hung || state.in_stop || !state.hook_on || state.hook_timeout_ns <= 0 {
        return false;
    }
    // The Go watchdog is `time.NewTimer(limit)`, which fires once the budget has
    // elapsed. One nanosecond before the deadline is still inside the budget.
    age_ns(state.now_ns, state.hook_start_ns) >= state.hook_timeout_ns
}

/// `age > window` drops. An event exactly `window` old is kept (PROTOCOL §9.5).
fn stale(ts: i64, now: i64, window_ns: i64) -> (bool, i64) {
    let age = age_ns(now, ts);
    (age > window_ns, age)
}

fn age_ns(now: i64, at: i64) -> i64 {
    now.saturating_sub(at).max(0)
}

fn failed_full() -> RunEnd {
    RunEnd::Failed {
        reason: REASON_TD_FULL.to_owned(),
    }
}

fn duration_to_ns(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

fn u32_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

fn ms32(ns: i64) -> u32 {
    if ns <= 0 {
        return 0;
    }
    let ms = (ns as u128) / NS_PER_MS;
    u32::try_from(ms).unwrap_or(u32::MAX)
}

fn ms64(ns: i64) -> u64 {
    if ns <= 0 {
        return 0;
    }
    u64::try_from((ns as u128) / NS_PER_MS).unwrap_or(u64::MAX)
}

fn panic_message(payload: &dyn Any) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_owned()
    }
}

fn next_thread_tag() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn wait<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests;
