//! The event log costs a hook no allocation, off or on (PROTOCOL §12.1,
//! E07-D009).
//!
//! This binary's global allocator counts the allocations each thread makes.
//! The dispatcher runs each hook on the test thread with `run_one`: an MD
//! message, a TD entry pushed with its input, a TD entry that names its input
//! once it runs, a post, and a timer. Off, the dispatcher has no recorder, so
//! it has no clock to read either. On, the frames go through [`Recording`]
//! into a real [`Log`], whose writer thread moves them to a file meanwhile.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mftik_core::dispatch::{self, Dispatcher, Input};
use mftik_core::eventlog::{Kind, Recorder};
use mftik_rt::clock::Clock;
use mftik_rt::eventlog::{Config, Header, Log, Recording};
use mftik_testkit::TempDir;

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    /// The dispatcher a late-input entry names its input to: its hook must
    /// capture nothing, or pushing it would allocate.
    static LATE: Cell<Option<&'static Dispatcher>> = const { Cell::new(None) };
}

fn count() {
    let _ = ALLOCATIONS.try_with(|made| made.set(made.get() + 1));
}

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

// SAFETY: every method forwards to `System` with the caller's arguments.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: as for `alloc`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        // SAFETY: the caller upholds `GlobalAlloc::realloc`'s contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `GlobalAlloc::dealloc`'s contract.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The injected clock, which md_ts is on. It does not move, so no hook is slow.
const NOW: i64 = 1_700_000_000_000_000_000;
const ROUNDS: u64 = 10_000;
/// Hooks per round, each an input and a `hook_end` when the log is on.
const HOOKS: u64 = 5;

fn dispatcher(rec: Option<Arc<dyn Recorder>>) -> Dispatcher {
    Dispatcher::new(dispatch::Config {
        recv_window: Duration::from_secs(3600),
        now_ns: NOW,
        ring_bytes: 1 << 20,
        feed: Some(Box::new(|msg| {
            std::hint::black_box(msg);
        })),
        rec,
        ..dispatch::Config::default()
    })
}

/// A v1 envelope at [`NOW`] and 120 bytes of payload.
fn md_message() -> Vec<u8> {
    let mut msg = vec![b'x'; 160];
    msg[..40].fill(0);
    msg[0] = 1;
    msg[8..16].copy_from_slice(&NOW.to_le_bytes());
    msg
}

fn td_input(api_id: u64) -> Input {
    Input {
        kind: Kind::TD_EVENT,
        r#ref: api_id,
        body: Bytes::from_static(&[b'x'; 120]),
        ..Input::default()
    }
}

/// Runs [`ROUNDS`] rounds of every hook and returns the allocations they made
/// on this thread, after one round that warms the queues up.
fn allocations_per_round(disp: &Dispatcher) -> u64 {
    LATE.set(Some(Box::leak(Box::new(disp.clone()))));
    let msg = md_message();
    let timer = disp.timer(|| {});
    let round = || {
        disp.push_md(&msg).expect("md");
        assert!(disp.run_one());
        disp.push_in(td_input(7), || {}).expect("td");
        assert!(disp.run_one());
        disp.push_td(|| {
            let late = LATE.get().expect("the dispatcher");
            late.record_input(&td_input(8));
        })
        .expect("late td");
        assert!(disp.run_one());
        disp.post(|| {}).expect("post");
        assert!(disp.run_one());
        timer.fire();
        assert!(disp.run_one());
        assert!(!disp.run_one());
    };
    round();

    let probe = allocations();
    drop(std::hint::black_box(vec![0u8; 1]));
    assert_eq!(allocations(), probe + 1, "the counter sees this thread");

    let before = allocations();
    for _ in 0..ROUNDS {
        round();
    }
    allocations() - before
}

#[test]
fn off_a_hook_allocates_nothing() {
    let disp = dispatcher(None);
    let made = allocations_per_round(&disp);
    assert_eq!(made, 0, "{made} allocations in {ROUNDS} rounds");
    assert_eq!(disp.record_now(), None);
    assert_eq!(disp.number_timer(), 0);
}

#[test]
fn on_a_hook_allocates_nothing() {
    let vol = TempDir::new("hookalloc");
    let log = Arc::new(
        Log::open(Config {
            dir: Some(vol.0.clone()),
            header: Header::mftik(1, "s1"),
            ..Config::default()
        })
        .expect("open"),
    );
    let disp = dispatcher(Some(Arc::new(Recording::new(
        Arc::clone(&log),
        Clock::new(),
    ))));
    let made = allocations_per_round(&disp);
    log.close();
    let (written, dropped) = log.stats();
    assert_eq!(written + dropped, 2 * HOOKS * (ROUNDS + 1) + 1);
    assert!(written > 0);
    assert_eq!(made, 0, "{made} allocations in {ROUNDS} rounds");
}
