//! Dispatcher tests use an injected clock. They wait on gates and channels.
//! The five-second bound only fails a stuck test; it is not part of the clock.

use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use super::envelope::encode_v1;
use super::*;

const T0: i64 = 1_000_000_000;
const FUSE: Duration = Duration::from_secs(5);
const MS: i64 = 1_000_000;

fn build(setup: impl FnOnce(&mut Config)) -> Dispatcher {
    let mut config = Config {
        ring_bytes: 4096,
        now_ns: T0,
        recv_window: Duration::from_secs(60),
        ..Default::default()
    };
    setup(&mut config);
    Dispatcher::new(config)
}

fn start(dispatcher: &Dispatcher) -> Receiver<RunEnd> {
    let (tx, rx) = mpsc::channel();
    let running = dispatcher.clone();
    thread::Builder::new()
        .name("test-run".into())
        .spawn(move || {
            let _ = tx.send(running.run());
        })
        .expect("spawn run");
    rx
}

#[track_caller]
fn recv_end(rx: &Receiver<RunEnd>) -> RunEnd {
    match rx.recv_timeout(FUSE) {
        Ok(end) => end,
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("run did not return"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("run dropped the result"),
    }
}

#[track_caller]
fn wait_exited(dispatcher: &Dispatcher) {
    let (tx, rx) = mpsc::channel();
    let dispatcher = dispatcher.clone();
    thread::spawn(move || {
        dispatcher.wait_exited();
        let _ = tx.send(());
    });
    match rx.recv_timeout(FUSE) {
        Ok(()) => {}
        Err(_) => panic!("dispatcher thread did not exit"),
    }
}

fn stop(dispatcher: &Dispatcher) -> Result<(), StopError> {
    let result = dispatcher.on_stop(&StopRequest::new(None));
    wait_exited(dispatcher);
    result
}

fn md_at(ts: i64, payload: &[u8]) -> Vec<u8> {
    encode_v1(ts, payload)
}

fn md(payload: &[u8]) -> Vec<u8> {
    md_at(T0, payload)
}

fn payload_at(msg: &[u8]) -> Option<u8> {
    msg.get(envelope::SIZE).copied()
}

struct Gate {
    entered: Mutex<bool>,
    entered_cv: Condvar,
    open: Mutex<bool>,
    open_cv: Condvar,
}

struct Opener(Arc<Gate>);

impl Drop for Opener {
    fn drop(&mut self) {
        self.0.open();
    }
}

impl Gate {
    fn pair() -> (Arc<Self>, Opener) {
        let gate = Arc::new(Self {
            entered: Mutex::new(false),
            entered_cv: Condvar::new(),
            open: Mutex::new(false),
            open_cv: Condvar::new(),
        });
        (Arc::clone(&gate), Opener(gate))
    }

    fn hold(self: &Arc<Self>) {
        {
            let mut entered = lock(&self.entered);
            *entered = true;
            self.entered_cv.notify_all();
        }
        let mut open = lock(&self.open);
        let start = Instant::now();
        while !*open {
            let left = FUSE.saturating_sub(start.elapsed());
            assert!(!left.is_zero(), "hook was not released");
            let (next, timeout) = self
                .open_cv
                .wait_timeout(open, left)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            open = next;
            if timeout.timed_out() && !*open {
                panic!("hook was not released");
            }
        }
    }

    fn wait_entered(&self) {
        let mut entered = lock(&self.entered);
        let start = Instant::now();
        while !*entered {
            let left = FUSE.saturating_sub(start.elapsed());
            assert!(!left.is_zero(), "hook did not start");
            let (next, timeout) = self
                .entered_cv
                .wait_timeout(entered, left)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            entered = next;
            if timeout.timed_out() && !*entered {
                panic!("hook did not start");
            }
        }
    }

    fn open(&self) {
        let mut open = lock(&self.open);
        *open = true;
        self.open_cv.notify_all();
    }
}

fn comm_names() -> Vec<String> {
    let mut names = Vec::new();
    let Ok(tasks) = fs::read_dir("/proc/self/task") else {
        return names;
    };
    for task in tasks.flatten() {
        if let Ok(name) = fs::read_to_string(task.path().join("comm")) {
            names.push(name.trim().to_owned());
        }
    }
    names
}

/// Thread names, once one of them is `name`. A new thread renames itself after it starts,
/// so for a moment it still carries its parent's name; poll instead of reading once.
fn comm_names_with(name: &str) -> Vec<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let names = comm_names();
        if names.iter().any(|n| n == name) || std::time::Instant::now() >= deadline {
            return names;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn constants_match_the_session_contract() {
    assert_eq!(TD_CAP, 65536);
    assert_eq!(MD_BYTES, 32 << 20);
    assert_eq!(SLOW_AFTER, Duration::from_millis(100));
    assert_eq!(SLOW_AFTER.as_nanos(), SLOW_AFTER_NS as u128);
    assert_eq!(REASON_TD_FULL, "td buffer full");
    assert_eq!(REASON_HUNG, "hook timeout");
}

#[test]
fn td_hooks_run_before_md_hooks() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let (done_tx, done_rx) = mpsc::channel();
    let order_feed = Arc::clone(&order);
    let dispatcher = build(|config| {
        config.feed = Some(Box::new(move |msg| {
            let label = match payload_at(msg) {
                Some(b'a') => "md-a",
                Some(b'b') => "md-b",
                _ => "md-other",
            };
            lock(&order_feed).push(label);
            if payload_at(msg) == Some(b'b') {
                let _ = done_tx.send(());
            }
        }));
    });
    dispatcher.push_md(&md(b"a")).expect("md a");
    let order_td = Arc::clone(&order);
    dispatcher
        .push_td(move || lock(&order_td).push("td-1"))
        .expect("td 1");
    dispatcher.push_md(&md(b"b")).expect("md b");
    let order_td = Arc::clone(&order);
    dispatcher
        .push_td(move || lock(&order_td).push("td-2"))
        .expect("td 2");
    let rx = start(&dispatcher);
    done_rx.recv_timeout(FUSE).expect("both md hooks");
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(lock(&order).as_slice(), ["td-1", "td-2", "md-a", "md-b"]);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn td_arriving_during_an_md_hook_runs_before_the_next_md() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let (gate, _opener) = Gate::pair();
    let (done_tx, done_rx) = mpsc::channel();
    let order_feed = Arc::clone(&order);
    let gate_feed = Arc::clone(&gate);
    let dispatcher = build(|config| {
        config.feed = Some(Box::new(move |msg| {
            let byte = payload_at(msg);
            let label = match byte {
                Some(b'a') => "md-a",
                Some(b'b') => "md-b",
                _ => "md-other",
            };
            lock(&order_feed).push(label);
            if byte == Some(b'a') {
                gate_feed.hold();
            }
            if byte == Some(b'b') {
                let _ = done_tx.send(());
            }
        }));
    });
    dispatcher.push_md(&md(b"a")).expect("md a");
    let rx = start(&dispatcher);
    gate.wait_entered();
    let order_td = Arc::clone(&order);
    dispatcher
        .push_td(move || lock(&order_td).push("td"))
        .expect("td");
    dispatcher.push_md(&md(b"b")).expect("md b");
    gate.open();
    done_rx.recv_timeout(FUSE).expect("md b");
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(lock(&order).as_slice(), ["md-a", "td", "md-b"]);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn one_timer_coalesces_while_it_is_queued() {
    let (gate, _opener) = Gate::pair();
    let gate_hold = Arc::clone(&gate);
    let dispatcher = build(|_| {});
    dispatcher.push_td(move || gate_hold.hold()).expect("hold");
    let ran = Arc::new(AtomicUsize::new(0));
    let ran_hook = Arc::clone(&ran);
    let timer = dispatcher.timer(move || {
        ran_hook.fetch_add(1, Ordering::Relaxed);
    });
    let rx = start(&dispatcher);
    gate.wait_entered();
    timer.fire();
    timer.fire();
    timer.fire();
    assert_eq!(dispatcher.stats().td.depth, 1);
    let marked = Arc::new(AtomicBool::new(false));
    let marked_hook = Arc::clone(&marked);
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            marked_hook.store(true, Ordering::Relaxed);
            dispatcher_hook.exit();
        })
        .expect("marker");
    assert_eq!(dispatcher.stats().td.depth, 2);
    gate.open();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(ran.load(Ordering::Relaxed), 1);
    assert!(marked.load(Ordering::Relaxed));
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn a_timer_can_queue_its_next_run_from_its_callback() {
    let slot = Arc::new(Mutex::new(None::<Timer>));
    let ran = Arc::new(AtomicUsize::new(0));
    let dispatcher = build(|_| {});
    let slot_hook = Arc::clone(&slot);
    let ran_hook = Arc::clone(&ran);
    let dispatcher_hook = dispatcher.clone();
    let timer = dispatcher.timer(move || {
        let n = ran_hook.fetch_add(1, Ordering::Relaxed);
        if n == 0 {
            if let Some(timer) = lock(&slot_hook).clone() {
                timer.fire();
            }
            let dispatcher_exit = dispatcher_hook.clone();
            dispatcher_hook
                .push_td(move || dispatcher_exit.exit())
                .expect("marker");
        }
    });
    *lock(&slot) = Some(timer.clone());
    timer.fire();
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(ran.load(Ordering::Relaxed), 2);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn after_arms_on_the_injected_clock_and_a_stopped_timer_does_not_arm() {
    let armed = Arc::new(Mutex::new(Vec::new()));
    let disarmed = Arc::new(Mutex::new(Vec::new()));
    let armed_cb = Arc::clone(&armed);
    let disarmed_cb = Arc::clone(&disarmed);
    let ran = Arc::new(AtomicUsize::new(0));
    let dispatcher = build(|config| {
        config.arm_timer = Some(Arc::new(move |id, deadline| {
            lock(&armed_cb).push((id, deadline));
        }));
        config.disarm_timer = Some(Arc::new(move |id| {
            lock(&disarmed_cb).push(id);
        }));
    });
    let ran_hook = Arc::clone(&ran);
    let timer = dispatcher.timer(move || {
        ran_hook.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(timer.after(Duration::from_nanos(50)), Some(T0 + 50));
    assert_eq!(timer.deadline_ns(), Some(T0 + 50));
    assert_eq!(lock(&armed).as_slice(), [(0, T0 + 50)]);
    // A queued run is not duplicated, but the host deadline is still replaced.
    timer.fire();
    assert_eq!(timer.after(Duration::ZERO), Some(T0));
    assert_eq!(dispatcher.stats().td.depth, 1);
    assert_eq!(lock(&armed).as_slice(), [(0, T0 + 50), (0, T0)]);
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.exit())
        .expect("exit");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(timer.after(Duration::from_nanos(1)), None);
    assert_eq!(lock(&disarmed).as_slice(), [0]);
    // Fire ignores the stopped flag until the TD buffer itself is closed.
    timer.fire();
    assert!(stop(&dispatcher).is_ok());
    assert_eq!(ran.load(Ordering::Relaxed), 2);
}

#[test]
fn prehook_drops_stale_md_keeps_the_exact_window_and_always_hands_books() {
    let window = Duration::from_nanos(100);
    let (gate, _opener) = Gate::pair();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let books = Arc::new(Mutex::new(Vec::new()));
    let (done_tx, done_rx) = mpsc::channel();
    let gate_feed = Arc::clone(&gate);
    let seen_feed = Arc::clone(&seen);
    let books_feed = Arc::clone(&books);
    let dispatcher = build(|config| {
        config.recv_window = window;
        config.feed = Some(Box::new(move |msg| {
            if let Some(byte) = payload_at(msg) {
                lock(&seen_feed).push(byte);
                if byte == b'a' {
                    gate_feed.hold();
                }
                if byte == b'z' {
                    let _ = done_tx.send(());
                }
            }
        }));
        config.book = Some(Box::new(move |msg| {
            if let Some(byte) = payload_at(msg) {
                lock(&books_feed).push(byte);
            }
        }));
    });
    dispatcher.push_md(&md_at(T0, b"a")).expect("a");
    let rx = start(&dispatcher);
    gate.wait_entered();
    let now = T0 + 101;
    dispatcher.set_now_ns(now);
    dispatcher.push_md(&md_at(T0, b"b")).expect("stale");
    dispatcher.push_book(&md_at(T0, b"k")).expect("old book");
    dispatcher.push_md(&md_at(now - 100, b"e")).expect("exact");
    dispatcher
        .push_md(&md_at(now - 100 - 1, b"p"))
        .expect("past");
    dispatcher.push_md(&md_at(now, b"z")).expect("fresh");
    gate.open();
    done_rx.recv_timeout(FUSE).expect("fresh md");
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(lock(&seen).as_slice(), [b'a', b'e', b'z']);
    assert_eq!(lock(&books).as_slice(), [b'k']);
    assert_eq!(dispatcher.stats().md.dropped, 2);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn ages_are_taken_when_the_hook_starts_and_stats_clears_them() {
    let (done_tx, done_rx) = mpsc::channel();
    let dispatcher = build(|config| {
        config.feed = Some(Box::new(move |_| {
            let _ = done_tx.send(());
        }));
    });
    dispatcher.push_td(|| {}).expect("td");
    dispatcher.set_now_ns(T0 + 40 * MS);
    let ts = dispatcher.now_ns() - 50 * MS;
    dispatcher.push_md(&md_at(ts, b"a")).expect("md");
    let rx = start(&dispatcher);
    done_rx.recv_timeout(FUSE).expect("md");
    let stats = dispatcher.stats();
    assert_eq!(stats.td.max_age_ms, 40);
    assert_eq!(stats.md.max_age_ms, 50);
    assert_eq!(stats.hook_blocked_ms, 0);
    let again = dispatcher.stats();
    assert_eq!(again.td.max_age_ms, 0);
    assert_eq!(again.md.max_age_ms, 0);
    assert_eq!(again.md.dropped, stats.md.dropped);
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn max_age_counts_whole_milliseconds_and_saturates() {
    let (done_tx, done_rx) = mpsc::channel();
    let dispatcher = build(|config| {
        config.feed = Some(Box::new(move |_| {
            let _ = done_tx.send(());
        }));
    });
    dispatcher.push_td(|| {}).expect("td");
    dispatcher.set_now_ns(T0 + 1_500_000);
    dispatcher
        .push_md(&md_at(dispatcher.now_ns(), b"a"))
        .expect("md");
    let rx = start(&dispatcher);
    done_rx.recv_timeout(FUSE).expect("md");
    assert_eq!(dispatcher.stats().td.max_age_ms, 1);
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());

    let (done_tx, done_rx) = mpsc::channel();
    let huge = (u32::MAX as i64 + 5) * MS;
    let dispatcher = build(|config| {
        config.now_ns = 0;
        config.feed = Some(Box::new(move |_| {
            let _ = done_tx.send(());
        }));
    });
    dispatcher.push_td(|| {}).expect("td");
    dispatcher.set_now_ns(huge);
    dispatcher.push_md(&md_at(huge, b"b")).expect("md");
    let rx = start(&dispatcher);
    done_rx.recv_timeout(FUSE).expect("md");
    assert_eq!(dispatcher.stats().td.max_age_ms, u32::MAX);
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn the_soft_limit_is_strict_and_blocked_time_is_only_while_a_hook_runs() {
    let (gate, _opener) = Gate::pair();
    let gate_hold = Arc::clone(&gate);
    let logs = Arc::new(Mutex::new(Vec::new()));
    let logs_cb = Arc::clone(&logs);
    let dispatcher = build(|config| {
        config.hook_timeout = Duration::ZERO;
        config.log = Some(Arc::new(move |line| lock(&logs_cb).push(line.to_owned())));
    });
    dispatcher.push_td(move || gate_hold.hold()).expect("hold");
    let dispatcher_exact = dispatcher.clone();
    dispatcher
        .push_td(move || {
            dispatcher_exact.set_now_ns(dispatcher_exact.now_ns() + SLOW_AFTER_NS);
        })
        .expect("exact");
    let dispatcher_over = dispatcher.clone();
    dispatcher
        .push_td(move || {
            dispatcher_over.set_now_ns(dispatcher_over.now_ns() + SLOW_AFTER_NS + 1);
        })
        .expect("over");
    let (done_tx, done_rx) = mpsc::channel();
    dispatcher
        .push_td(move || {
            let _ = done_tx.send(());
        })
        .expect("done");
    let rx = start(&dispatcher);
    gate.wait_entered();
    dispatcher.set_now_ns(T0 + 150 * MS);
    let mid = dispatcher.stats();
    assert_eq!(mid.hook_blocked_ms, 150);
    assert_eq!(mid.hook_slow, 0);
    gate.open();
    done_rx.recv_timeout(FUSE).expect("hooks finished");
    let after = dispatcher.stats();
    assert_eq!(after.hook_blocked_ms, 0);
    assert_eq!(after.hook_slow, 2);
    assert_eq!(
        lock(&logs)
            .iter()
            .filter(|line| line.as_str() == "hook exceeded 100ms")
            .count(),
        2
    );
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn a_hook_shorter_than_the_hard_limit_is_not_hung() {
    let dispatcher = build(|config| {
        config.hook_timeout = Duration::from_nanos(1000);
    });
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.set_now_ns(T0 + 999))
        .expect("inside");
    let ran = Arc::new(AtomicBool::new(false));
    let ran_hook = Arc::clone(&ran);
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            ran_hook.store(true, Ordering::Relaxed);
            dispatcher_hook.exit();
        })
        .expect("marker");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(ran.load(Ordering::Relaxed));
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn the_watchdog_publishes_hung_when_the_injected_budget_is_used_up() {
    let (gate, _opener) = Gate::pair();
    let gate_hold = Arc::clone(&gate);
    let strategy_stopped = Arc::new(AtomicBool::new(false));
    let strategy_hook = Arc::clone(&strategy_stopped);
    let later = Arc::new(AtomicBool::new(false));
    let later_hook = Arc::clone(&later);
    let dispatcher = build(|config| {
        config.hook_timeout = Duration::from_nanos(1000);
        config.on_stop = Some(Box::new(move |_| {
            strategy_hook.store(true, Ordering::Relaxed);
            Ok(())
        }));
    });
    dispatcher.push_td(move || gate_hold.hold()).expect("hold");
    dispatcher
        .push_td(move || later_hook.store(true, Ordering::Relaxed))
        .expect("later");
    let rx = start(&dispatcher);
    gate.wait_entered();
    dispatcher.set_now_ns(T0 + 1000);
    assert_eq!(recv_end(&rx), RunEnd::Hung);
    let names = comm_names_with(&dispatcher.shared.watch_name);
    assert!(
        names
            .iter()
            .any(|name| name == &dispatcher.shared.watch_name),
        "watchdog thread missing from {names:?}"
    );
    assert_eq!(
        dispatcher.push_td(|| {}).expect_err("td closed after hung"),
        PushError::Closed
    );
    assert_eq!(
        dispatcher
            .push_md(&md(b"a"))
            .expect_err("md closed after hung"),
        PushError::Closed
    );
    let dispatcher_stop = dispatcher.clone();
    let stopped = thread::spawn(move || dispatcher_stop.on_stop(&StopRequest::new(None)));
    gate.open();
    assert_eq!(stopped.join().expect("on_stop thread"), Ok(()));
    assert!(!strategy_stopped.load(Ordering::Relaxed));
    assert!(!later.load(Ordering::Relaxed));
    wait_exited(&dispatcher);
}

#[test]
fn exit_during_a_hook_that_then_overruns_is_hung() {
    let (gate, _opener) = Gate::pair();
    let gate_hold = Arc::clone(&gate);
    let strategy_stopped = Arc::new(AtomicBool::new(false));
    let strategy_hook = Arc::clone(&strategy_stopped);
    let dispatcher = build(|config| {
        config.hook_timeout = Duration::from_nanos(1000);
        config.on_stop = Some(Box::new(move |_| {
            strategy_hook.store(true, Ordering::Relaxed);
            Ok(())
        }));
    });
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            dispatcher_hook.exit();
            gate_hold.hold();
        })
        .expect("exit then wait");
    let rx = start(&dispatcher);
    gate.wait_entered();
    dispatcher.set_now_ns(T0 + 1000);
    assert_eq!(recv_end(&rx), RunEnd::Hung);
    gate.open();
    assert!(stop(&dispatcher).is_ok());
    assert!(!strategy_stopped.load(Ordering::Relaxed));
}

#[test]
fn a_zero_hook_timeout_does_not_arm_the_watchdog() {
    let ran = Arc::new(AtomicBool::new(false));
    let dispatcher = build(|config| {
        config.hook_timeout = Duration::ZERO;
    });
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.set_now_ns(T0 + 3_600 * 1_000_000_000))
        .expect("jump");
    let ran_hook = Arc::clone(&ran);
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            ran_hook.store(true, Ordering::Relaxed);
            dispatcher_hook.exit();
        })
        .expect("marker");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(ran.load(Ordering::Relaxed));
    assert!(dispatcher.stats().hook_slow >= 1);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn bad_envelopes_are_counted_and_do_not_end_the_session() {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let (seen_tx, seen_rx) = mpsc::channel();
    let (good_tx, good_rx) = mpsc::channel();
    let logs_cb = Arc::clone(&logs);
    let dispatcher = build(|config| {
        config.log = Some(Arc::new(move |line| {
            lock(&logs_cb).push(line.to_owned());
            let _ = seen_tx.send(());
        }));
        config.feed = Some(Box::new(move |msg| {
            if payload_at(msg) == Some(b'k') {
                let _ = good_tx.send(());
            }
        }));
        config.book = Some(Box::new(|_| {}));
    });
    dispatcher.push_md(&[0x7f]).expect("short");
    let mut bad_version = md(b"v");
    bad_version[0] = 2;
    dispatcher.push_book(&bad_version).expect("bad version");
    let rx = start(&dispatcher);
    seen_rx.recv_timeout(FUSE).expect("first log");
    seen_rx.recv_timeout(FUSE).expect("second log");
    assert!(rx.try_recv().is_err(), "a bad envelope ended the session");
    dispatcher.push_md(&md(b"k")).expect("good");
    good_rx.recv_timeout(FUSE).expect("good md");
    assert_eq!(dispatcher.stats().md.dropped, 2);
    let lines = lock(&logs).clone();
    assert_eq!(lines.len(), 2);
    assert!(
        lines
            .iter()
            .all(|line| line.contains("md envelope rejected"))
    );
    assert!(lines[0].contains("envelope: message shorter than 40 bytes: 1 bytes"));
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn a_missing_callback_skips_a_fresh_event_and_still_counts_a_stale_one() {
    let dispatcher = build(|config| {
        config.recv_window = Duration::from_nanos(100);
        config.feed = None;
        config.book = None;
    });
    let rx = start(&dispatcher);
    dispatcher.push_md(&md_at(T0, b"a")).expect("fresh");
    dispatcher.push_md(&md_at(T0 - 1000, b"b")).expect("stale");
    dispatcher.push_book(&md_at(T0 - 1000, b"k")).expect("book");
    let start_at = Instant::now();
    loop {
        let stats = dispatcher.stats();
        if stats.md.depth == 0 && stats.md.dropped == 1 {
            break;
        }
        assert!(
            start_at.elapsed() < FUSE,
            "md was not drained, stats {stats:?}"
        );
        thread::yield_now();
    }
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn the_td_buffer_fails_the_session_at_65536_and_delivers_those_entries_on_stop() {
    let (gate, _opener) = Gate::pair();
    let gate_hold = Arc::clone(&gate);
    let logs = Arc::new(Mutex::new(Vec::new()));
    let logs_cb = Arc::clone(&logs);
    let ran = Arc::new(AtomicUsize::new(0));
    let timer_ran = Arc::new(AtomicBool::new(false));
    let dispatcher = build(|config| {
        config.log = Some(Arc::new(move |line| lock(&logs_cb).push(line.to_owned())));
    });
    dispatcher.push_td(move || gate_hold.hold()).expect("hold");
    let rx = start(&dispatcher);
    gate.wait_entered();
    for i in 0..TD_CAP {
        let ran = Arc::clone(&ran);
        dispatcher
            .push_td(move || {
                ran.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap_or_else(|err| panic!("push {i}: {err}"));
    }
    let rejected = Arc::new(AtomicBool::new(false));
    let rejected_hook = Arc::clone(&rejected);
    let err = dispatcher
        .push_td(move || rejected_hook.store(true, Ordering::Relaxed))
        .expect_err("overflow");
    assert_eq!(err, PushError::Full);
    assert_eq!(err.to_string(), "dispatch: td buffer full");
    let timer_hook = Arc::clone(&timer_ran);
    let timer = dispatcher.timer(move || timer_hook.store(true, Ordering::Relaxed));
    timer.fire();
    assert_eq!(dispatcher.stats().td.depth, TD_CAP as u32);
    assert_eq!(
        dispatcher
            .push_td(|| {})
            .expect_err("still full before close"),
        PushError::Full
    );
    assert_eq!(
        lock(&logs).as_slice(),
        ["td buffer full", "td buffer full", "td buffer full"]
    );
    gate.open();
    assert_eq!(
        recv_end(&rx),
        RunEnd::Failed {
            reason: REASON_TD_FULL.to_owned(),
        }
    );
    assert_eq!(ran.load(Ordering::Relaxed), 0);
    assert!(stop(&dispatcher).is_ok());
    assert_eq!(ran.load(Ordering::Relaxed), TD_CAP);
    assert!(!rejected.load(Ordering::Relaxed));
    assert!(!timer_ran.load(Ordering::Relaxed));
    assert_eq!(
        dispatcher.push_td(|| {}).expect_err("closed"),
        PushError::Closed
    );
    assert_eq!(
        dispatcher.push_md(&md(b"a")).expect_err("md closed"),
        PushError::Closed
    );
}

#[test]
fn done_failed_and_crashed_follow_the_stop_sequence() {
    let done = ending(Kind::Done);
    assert_eq!(done.end, RunEnd::Done);
    assert_eq!(done.order, ["hook", "unsub", "td", "onstop"]);
    assert!(done.same_thread);
    assert!(done.md_accepted_in_hook);
    assert!(!done.queued_md_delivered);

    let failed = ending(Kind::Failed);
    assert_eq!(
        failed.end,
        RunEnd::Failed {
            reason: "because".to_owned(),
        }
    );
    assert_eq!(failed.order, ["hook", "unsub", "td", "onstop"]);

    let crashed = ending(Kind::Crashed);
    assert_eq!(
        crashed.end,
        RunEnd::Crashed {
            reason: "hook panic: boom".to_owned(),
        }
    );
    assert_eq!(crashed.order, ["hook", "onstop"]);
    assert!(crashed.same_thread);
}

#[derive(Clone, Copy)]
enum Kind {
    Done,
    Failed,
    Crashed,
}

struct Ending {
    end: RunEnd,
    order: Vec<&'static str>,
    same_thread: bool,
    md_accepted_in_hook: bool,
    queued_md_delivered: bool,
}

fn ending(kind: Kind) -> Ending {
    let order = Arc::new(Mutex::new(Vec::new()));
    let threads = Arc::new(Mutex::new(Vec::new()));
    let md_in_hook = Arc::new(AtomicBool::new(false));
    let md_delivered = Arc::new(AtomicBool::new(false));
    let order_unsub = Arc::clone(&order);
    let order_stop = Arc::clone(&order);
    let threads_stop = Arc::clone(&threads);
    let md_feed = Arc::clone(&md_delivered);
    let dispatcher = build(|config| {
        config.feed = Some(Box::new(move |_| {
            md_feed.store(true, Ordering::Relaxed);
        }));
        config.unsubscribe_td = Some(Box::new(move || {
            lock(&order_unsub).push("unsub");
        }));
        config.on_stop = Some(Box::new(move |_| {
            lock(&threads_stop).push(thread::current().id());
            lock(&order_stop).push("onstop");
            Ok(())
        }));
    });
    let order_hook = Arc::clone(&order);
    let threads_hook = Arc::clone(&threads);
    let dispatcher_hook = dispatcher.clone();
    let md_hook = Arc::clone(&md_in_hook);
    dispatcher.push_md(&md(b"q")).expect("queued md");
    dispatcher
        .push_td(move || {
            lock(&threads_hook).push(thread::current().id());
            lock(&order_hook).push("hook");
            md_hook.store(
                dispatcher_hook.push_md(&md(b"h")).is_ok(),
                Ordering::Relaxed,
            );
            match kind {
                Kind::Done => dispatcher_hook.exit(),
                Kind::Failed => dispatcher_hook.fail("because"),
                Kind::Crashed => panic!("boom"),
            }
        })
        .expect("hook");
    if !matches!(kind, Kind::Crashed) {
        let order_td = Arc::clone(&order);
        let threads_td = Arc::clone(&threads);
        dispatcher
            .push_td(move || {
                lock(&threads_td).push(thread::current().id());
                lock(&order_td).push("td");
            })
            .expect("td");
    } else {
        dispatcher.push_td(|| {}).expect("td that must not run");
    }
    let rx = start(&dispatcher);
    let end = recv_end(&rx);
    let stop_result = dispatcher.on_stop(&StopRequest::new(None));
    wait_exited(&dispatcher);
    assert_eq!(stop_result, Ok(()));
    let ids = lock(&threads).clone();
    let same_thread = !ids.is_empty() && ids.iter().all(|id| *id == ids[0]);
    Ending {
        end,
        order: lock(&order).clone(),
        same_thread,
        md_accepted_in_hook: md_in_hook.load(Ordering::Relaxed),
        queued_md_delivered: md_delivered.load(Ordering::Relaxed),
    }
}

#[test]
fn on_stop_sees_closed_buffers_and_the_same_dispatcher_thread() {
    let threads = Arc::new(Mutex::new(Vec::new()));
    let md_closed = Arc::new(AtomicBool::new(false));
    let td_closed = Arc::new(AtomicBool::new(false));
    let threads_stop = Arc::clone(&threads);
    let md_flag = Arc::clone(&md_closed);
    let td_flag = Arc::clone(&td_closed);
    let slot = Arc::new(Mutex::new(None::<Dispatcher>));
    let slot_stop = Arc::clone(&slot);
    let dispatcher = build(|config| {
        config.on_stop = Some(Box::new(move |_info| {
            lock(&threads_stop).push(thread::current().id());
            let dispatcher = lock(&slot_stop).clone().expect("dispatcher");
            md_flag.store(dispatcher.push_md(&md(b"x")).is_err(), Ordering::Relaxed);
            td_flag.store(dispatcher.push_td(|| {}).is_err(), Ordering::Relaxed);
            Ok(())
        }));
    });
    *lock(&slot) = Some(dispatcher.clone());
    let threads_hook = Arc::clone(&threads);
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            lock(&threads_hook).push(thread::current().id());
            dispatcher_hook.exit();
        })
        .expect("exit");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
    assert!(md_closed.load(Ordering::Relaxed));
    assert!(td_closed.load(Ordering::Relaxed));
    let ids = lock(&threads).clone();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], ids[1]);
    assert_ne!(ids[0], thread::current().id());
}

#[test]
fn the_first_stop_wins_over_a_later_stop_and_a_later_panic() {
    let dispatcher = build(|_| {});
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            dispatcher_hook.exit();
            panic!("boom");
        })
        .expect("hook");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());

    let dispatcher = build(|_| {});
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            dispatcher_hook.fail("because");
            panic!("boom");
        })
        .expect("hook");
    let rx = start(&dispatcher);
    assert_eq!(
        recv_end(&rx),
        RunEnd::Failed {
            reason: "because".to_owned(),
        }
    );
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn a_stop_deadline_ends_the_td_drain_before_on_stop() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let order_unsub = Arc::clone(&order);
    let order_stop = Arc::clone(&order);
    let dispatcher = build(|config| {
        config.unsubscribe_td = Some(Box::new(move || lock(&order_unsub).push("unsub")));
        config.on_stop = Some(Box::new(move |_| {
            lock(&order_stop).push("onstop");
            Ok(())
        }));
    });
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.exit())
        .expect("exit");
    let order_first = Arc::clone(&order);
    let dispatcher_first = dispatcher.clone();
    dispatcher
        .push_td(move || {
            lock(&order_first).push("first");
            dispatcher_first.set_now_ns(T0 + 100);
        })
        .expect("first");
    let order_second = Arc::clone(&order);
    dispatcher
        .push_td(move || lock(&order_second).push("second"))
        .expect("second");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    let request = StopRequest::new(Some(T0 + 100));
    let err = dispatcher.on_stop(&request);
    wait_exited(&dispatcher);
    assert_eq!(err, Err(StopError::Deadline));
    assert_eq!(lock(&order).as_slice(), ["unsub", "first"]);
}

#[test]
fn a_stop_that_is_already_due_skips_unsubscribe_and_on_stop() {
    let called = Arc::new(AtomicBool::new(false));
    let called_unsub = Arc::clone(&called);
    let called_stop = Arc::clone(&called);
    let dispatcher = build(|config| {
        config.unsubscribe_td = Some(Box::new(move || {
            called_unsub.store(true, Ordering::Relaxed);
        }));
        config.on_stop = Some(Box::new(move |_| {
            called_stop.store(true, Ordering::Relaxed);
            Ok(())
        }));
    });
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.exit())
        .expect("exit");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    let err = dispatcher.on_stop(&StopRequest::new(Some(T0)));
    wait_exited(&dispatcher);
    assert_eq!(err, Err(StopError::Deadline));
    assert!(!called.load(Ordering::Relaxed));
    assert_eq!(
        dispatcher.push_td(|| {}).expect_err("closed"),
        PushError::Closed
    );
}

#[test]
fn cancel_between_td_entries_stops_the_drain() {
    let ran = Arc::new(AtomicUsize::new(0));
    let dispatcher = build(|_| {});
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.exit())
        .expect("exit");
    let request = StopRequest::new(None);
    let request_hook = request.clone();
    let ran_first = Arc::clone(&ran);
    dispatcher
        .push_td(move || {
            ran_first.fetch_add(1, Ordering::Relaxed);
            request_hook.cancel();
        })
        .expect("first");
    let ran_second = Arc::clone(&ran);
    dispatcher
        .push_td(move || {
            ran_second.fetch_add(1, Ordering::Relaxed);
        })
        .expect("second");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(dispatcher.on_stop(&request), Err(StopError::Cancelled));
    wait_exited(&dispatcher);
    assert_eq!(ran.load(Ordering::Relaxed), 1);
}

#[test]
fn a_panic_during_the_td_drain_still_runs_on_stop() {
    let stopped = Arc::new(AtomicBool::new(false));
    let stopped_hook = Arc::clone(&stopped);
    let dispatcher = build(|config| {
        config.on_stop = Some(Box::new(move |_| {
            stopped_hook.store(true, Ordering::Relaxed);
            Ok(())
        }));
    });
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.exit())
        .expect("exit");
    dispatcher.push_td(|| panic!("boom")).expect("panic");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(
        dispatcher.on_stop(&StopRequest::new(None)),
        Err(StopError::HookPanic("hook panic: boom".to_owned()))
    );
    wait_exited(&dispatcher);
    assert!(stopped.load(Ordering::Relaxed));
}

#[test]
fn an_on_stop_error_is_returned_and_outranks_a_drain_panic() {
    let dispatcher = build(|config| {
        config.on_stop = Some(Box::new(|_| Err("nope".to_owned())));
    });
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.exit())
        .expect("exit");
    dispatcher.push_td(|| panic!("boom")).expect("panic");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(
        dispatcher.on_stop(&StopRequest::new(None)),
        Err(StopError::OnStop("nope".to_owned()))
    );
    wait_exited(&dispatcher);
}

#[test]
fn an_on_stop_panic_is_returned_to_the_host() {
    let dispatcher = build(|config| {
        config.on_stop = Some(Box::new(|_| panic!("boom")));
    });
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.exit())
        .expect("exit");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(
        dispatcher.on_stop(&StopRequest::new(None)),
        Err(StopError::OnStopPanic(
            "dispatch: onstop panic: boom".to_owned()
        ))
    );
    wait_exited(&dispatcher);
    assert_eq!(
        dispatcher.on_stop(&StopRequest::new(None)),
        Err(StopError::OnStopPanic(
            "dispatch: onstop panic: boom".to_owned()
        ))
    );
}

#[test]
fn an_unsubscribe_panic_skips_the_rest_of_the_stop() {
    let stopped = Arc::new(AtomicBool::new(false));
    let stopped_hook = Arc::clone(&stopped);
    let drained = Arc::new(AtomicBool::new(false));
    let dispatcher = build(|config| {
        config.unsubscribe_td = Some(Box::new(|| panic!("boom")));
        config.on_stop = Some(Box::new(move |_| {
            stopped_hook.store(true, Ordering::Relaxed);
            Ok(())
        }));
    });
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.exit())
        .expect("exit");
    let drained_hook = Arc::clone(&drained);
    dispatcher
        .push_td(move || drained_hook.store(true, Ordering::Relaxed))
        .expect("td");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(
        dispatcher.on_stop(&StopRequest::new(None)),
        Err(StopError::StopPanic("dispatch: stop: boom".to_owned()))
    );
    wait_exited(&dispatcher);
    assert!(!stopped.load(Ordering::Relaxed));
    assert!(!drained.load(Ordering::Relaxed));
    assert!(dispatcher.push_td(|| {}).is_ok());
}

#[test]
fn stopping_discards_queued_md_without_counting_a_drop() {
    let delivered = Arc::new(AtomicBool::new(false));
    let delivered_feed = Arc::clone(&delivered);
    let dispatcher = build(|config| {
        config.feed = Some(Box::new(move |_| {
            delivered_feed.store(true, Ordering::Relaxed);
        }));
    });
    dispatcher.push_md(&md(b"a")).expect("queued");
    let accepted = Arc::new(AtomicBool::new(false));
    let accepted_hook = Arc::clone(&accepted);
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            accepted_hook.store(
                dispatcher_hook.push_md(&md(b"b")).is_ok(),
                Ordering::Relaxed,
            );
            dispatcher_hook.exit();
        })
        .expect("exit");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(accepted.load(Ordering::Relaxed));
    assert!(!delivered.load(Ordering::Relaxed));
    assert_eq!(dispatcher.stats().md.dropped, 0);
    assert!(stop(&dispatcher).is_ok());
    assert!(!delivered.load(Ordering::Relaxed));
}

#[test]
fn depth_does_not_include_the_hook_that_is_running() {
    let (gate, _opener) = Gate::pair();
    let gate_td = Arc::clone(&gate);
    let dispatcher = build(|_| {});
    dispatcher.push_td(move || gate_td.hold()).expect("hold");
    let rx = start(&dispatcher);
    gate.wait_entered();
    dispatcher.push_td(|| {}).expect("one");
    dispatcher.push_td(|| {}).expect("two");
    assert_eq!(dispatcher.stats().td.depth, 2);
    assert_eq!(dispatcher.stats().td.dropped, 0);
    gate.open();
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.exit())
        .expect("exit");
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn a_small_ring_drops_the_oldest_market_data() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (done_tx, done_rx) = mpsc::channel();
    let seen_feed = Arc::clone(&seen);
    let dispatcher = build(|config| {
        config.ring_bytes = 82;
        config.feed = Some(Box::new(move |msg| {
            if let Some(byte) = payload_at(msg) {
                lock(&seen_feed).push(byte);
                if byte == b'c' {
                    let _ = done_tx.send(());
                }
            }
        }));
    });
    for byte in *b"abc" {
        dispatcher.push_md(&md(&[byte])).expect("push");
    }
    let before = dispatcher.stats();
    assert_eq!(before.md.dropped, 1);
    assert_eq!(before.md.depth, 2);
    let rx = start(&dispatcher);
    done_rx.recv_timeout(FUSE).expect("c");
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(lock(&seen).as_slice(), [b'b', b'c']);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn an_oversize_market_data_message_is_dropped_and_is_not_an_error() {
    let (done_tx, done_rx) = mpsc::channel();
    let dispatcher = build(|config| {
        config.ring_bytes = 80;
        config.feed = Some(Box::new(move |msg| {
            if payload_at(msg) == Some(b'k') {
                let _ = done_tx.send(());
            }
        }));
    });
    let big = vec![1u8; 81];
    assert!(dispatcher.push_md(&big).is_ok());
    assert_eq!(dispatcher.stats().md.dropped, 1);
    dispatcher.push_md(&md(b"k")).expect("small");
    let rx = start(&dispatcher);
    done_rx.recv_timeout(FUSE).expect("small");
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn pinned_market_data_bytes_stay_put_while_later_pushes_drop() {
    let (hold_gate, _hold_opener) = Gate::pair();
    let (check_tx, check_rx) = mpsc::channel();
    let original = md(b"a");
    let hold = Arc::clone(&hold_gate);
    let original_feed = original.clone();
    let dispatcher = build(|config| {
        config.ring_bytes = 90;
        config.feed = Some(Box::new(move |msg| {
            if payload_at(msg) != Some(b'a') {
                return;
            }
            hold.hold();
            let same = msg == original_feed.as_slice();
            let _ = check_tx.send(same);
        }));
    });
    dispatcher.push_md(&original).expect("a");
    let rx = start(&dispatcher);
    hold_gate.wait_entered();
    dispatcher.push_md(&md(b"b")).expect("b");
    dispatcher.push_md(&md(b"c")).expect("c");
    assert_eq!(dispatcher.stats().md.depth, 1);
    hold_gate.open();
    assert!(check_rx.recv_timeout(FUSE).expect("checked"));
    // c is delivered next. Exit once it has been handed off: depth returns to 0.
    let start_at = Instant::now();
    loop {
        if dispatcher.stats().md.depth == 0 {
            break;
        }
        assert!(start_at.elapsed() < FUSE, "remaining md was not delivered");
        thread::yield_now();
    }
    assert_eq!(dispatcher.stats().md.dropped, 1);
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn a_future_md_timestamp_is_kept_with_age_zero() {
    let (done_tx, done_rx) = mpsc::channel();
    let dispatcher = build(|config| {
        config.recv_window = Duration::from_nanos(10);
        config.feed = Some(Box::new(move |_| {
            let _ = done_tx.send(());
        }));
    });
    dispatcher.push_md(&md_at(T0 + 50, b"a")).expect("future");
    let rx = start(&dispatcher);
    done_rx.recv_timeout(FUSE).expect("kept");
    let stats = dispatcher.stats();
    assert_eq!(stats.md.dropped, 0);
    assert_eq!(stats.md.max_age_ms, 0);
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn two_threads_can_push_td_entries() {
    let dispatcher = build(|_| {});
    let left = dispatcher.clone();
    let right = dispatcher.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let count_left = Arc::clone(&count);
    let count_right = Arc::clone(&count);
    let t1 = thread::spawn(move || {
        for _ in 0..4 {
            let count_left = Arc::clone(&count_left);
            left.push_td(move || {
                count_left.fetch_add(1, Ordering::Relaxed);
            })
            .expect("left");
        }
    });
    let t2 = thread::spawn(move || {
        for _ in 0..4 {
            let count_right = Arc::clone(&count_right);
            right
                .push_td(move || {
                    count_right.fetch_add(1, Ordering::Relaxed);
                })
                .expect("right");
        }
    });
    t1.join().expect("left thread");
    t2.join().expect("right thread");
    let seen = Arc::new(AtomicUsize::new(0));
    let seen_hook = Arc::clone(&seen);
    let count_hook = Arc::clone(&count);
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            seen_hook.store(count_hook.load(Ordering::Relaxed), Ordering::Relaxed);
            dispatcher_hook.exit();
        })
        .expect("marker");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert_eq!(seen.load(Ordering::Relaxed), 8);
    assert!(stop(&dispatcher).is_ok());
}

#[test]
fn on_stop_reports_the_deadline_it_was_given() {
    let seen = Arc::new(Mutex::new(None));
    let seen_hook = Arc::clone(&seen);
    let dispatcher = build(|config| {
        config.on_stop = Some(Box::new(move |info| {
            *lock(&seen_hook) = Some(*info);
            Ok(())
        }));
    });
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || dispatcher_hook.exit())
        .expect("exit");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    let request = StopRequest::new(Some(T0 + 5_000));
    assert!(dispatcher.on_stop(&request).is_ok());
    wait_exited(&dispatcher);
    assert_eq!(
        *lock(&seen),
        Some(StopInfo {
            deadline_ns: Some(T0 + 5_000),
            now_ns: T0,
        })
    );
}

#[test]
fn hooks_on_the_way_to_a_normal_stop_share_one_thread() {
    let ids = Arc::new(Mutex::new(Vec::<ThreadId>::new()));
    let ids_unsub = Arc::clone(&ids);
    let ids_td = Arc::clone(&ids);
    let ids_stop = Arc::clone(&ids);
    let dispatcher = build(|config| {
        config.unsubscribe_td = Some(Box::new(move || {
            lock(&ids_unsub).push(thread::current().id());
        }));
        config.on_stop = Some(Box::new(move |_| {
            lock(&ids_stop).push(thread::current().id());
            Ok(())
        }));
    });
    let ids_hook = Arc::clone(&ids);
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            lock(&ids_hook).push(thread::current().id());
            dispatcher_hook.exit();
        })
        .expect("hook");
    dispatcher
        .push_td(move || lock(&ids_td).push(thread::current().id()))
        .expect("td");
    let rx = start(&dispatcher);
    assert_eq!(recv_end(&rx), RunEnd::Done);
    assert!(stop(&dispatcher).is_ok());
    let ids = lock(&ids).clone();
    assert_eq!(ids.len(), 4);
    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_ne!(ids[0], thread::current().id());
}

/// `drive` on a test thread, as `run` would be on the caller's thread (E03-D003).
fn drive_on(dispatcher: &Dispatcher) -> thread::JoinHandle<ThreadId> {
    let driving = dispatcher.clone();
    thread::Builder::new()
        .name("test-drive".into())
        .spawn(move || {
            driving.drive();
            thread::current().id()
        })
        .expect("spawn drive")
}

fn wait_run_on(dispatcher: &Dispatcher) -> Receiver<RunEnd> {
    let (tx, rx) = mpsc::channel();
    let waiting = dispatcher.clone();
    thread::spawn(move || {
        let _ = tx.send(waiting.wait_run());
    });
    rx
}

#[test]
fn drive_runs_hooks_drain_and_on_stop_on_the_driving_thread() {
    let ids = Arc::new(Mutex::new(Vec::new()));
    let ids_stop = Arc::clone(&ids);
    let dispatcher = build(|config| {
        config.on_stop = Some(Box::new(move |_| {
            lock(&ids_stop).push(thread::current().id());
            Ok(())
        }));
    });
    // The waiter starts first: wait_run does not start the loop.
    let rx = wait_run_on(&dispatcher);
    assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    let ids_hook = Arc::clone(&ids);
    let dispatcher_hook = dispatcher.clone();
    dispatcher
        .push_td(move || {
            lock(&ids_hook).push(thread::current().id());
            dispatcher_hook.fail("bye");
        })
        .expect("hook");
    let ids_td = Arc::clone(&ids);
    dispatcher
        .push_td(move || lock(&ids_td).push(thread::current().id()))
        .expect("td");
    let driver = drive_on(&dispatcher);
    assert_eq!(
        recv_end(&rx),
        RunEnd::Failed {
            reason: "bye".into()
        }
    );
    assert_eq!(dispatcher.run(), recv_end(&wait_run_on(&dispatcher)));
    let names = comm_names_with(&dispatcher.shared.watch_name);
    assert!(
        names
            .iter()
            .any(|name| name == &dispatcher.shared.watch_name),
        "watchdog thread missing from {names:?}"
    );
    assert!(stop(&dispatcher).is_ok());
    let driver_id = driver.join().expect("drive thread");
    let ids = lock(&ids).clone();
    assert_eq!(ids, vec![driver_id; 3]);
}

#[test]
fn drive_publishes_hung_while_the_hook_holds_the_driving_thread() {
    let (gate, _opener) = Gate::pair();
    let gate_hold = Arc::clone(&gate);
    let strategy_stopped = Arc::new(AtomicBool::new(false));
    let strategy_hook = Arc::clone(&strategy_stopped);
    let dispatcher = build(|config| {
        config.hook_timeout = Duration::from_nanos(1000);
        config.on_stop = Some(Box::new(move |_| {
            strategy_hook.store(true, Ordering::Relaxed);
            Ok(())
        }));
    });
    dispatcher.push_td(move || gate_hold.hold()).expect("hold");
    let driver = drive_on(&dispatcher);
    let rx = wait_run_on(&dispatcher);
    gate.wait_entered();
    dispatcher.set_now_ns(T0 + 1000);
    assert_eq!(recv_end(&rx), RunEnd::Hung);
    assert!(!driver.is_finished());
    gate.open();
    driver.join().expect("drive thread");
    assert_eq!(dispatcher.on_stop(&StopRequest::new(None)), Ok(()));
    assert!(!strategy_stopped.load(Ordering::Relaxed));
}

#[test]
fn drive_after_run_returns_at_once() {
    let dispatcher = build(|_| {});
    let rx = start(&dispatcher);
    dispatcher.exit();
    assert_eq!(recv_end(&rx), RunEnd::Done);
    // The loop is already started; drive does not start a second one.
    dispatcher.drive();
    assert!(stop(&dispatcher).is_ok());
}
