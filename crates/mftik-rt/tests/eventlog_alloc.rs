//! Appending a frame makes no heap allocation (PROTOCOL §12.1, E07-D001).
//!
//! This binary's global allocator counts the allocations each thread makes.
//! The appends run on the test thread while the writer thread moves the ring
//! to files: kicks at a quarter, ring wrap-around, and drops if it fills.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Duration;

use mftik_rt::eventlog::{Config, Header, Kind, Log};
use mftik_testkit::TempDir;

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
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

#[test]
fn append_does_not_allocate() {
    let vol = TempDir::new("evalloc");
    let log = Log::open(Config {
        dir: Some(vol.0.clone()),
        header: Header::mftik(1, "s1"),
        ring_bytes: 64 << 10,
        flush_every: Duration::from_millis(1),
        ..Config::default()
    })
    .expect("open");
    let head = [b'a'; 8];
    let body = [b'b'; 200];

    let probe = allocations();
    drop(std::hint::black_box(vec![0u8; 1]));
    assert_eq!(allocations(), probe + 1, "the counter sees this thread");

    let before = allocations();
    let mut last = 0;
    for i in 0..100_000u64 {
        last = log.append(Kind::MD_FEED, 0, i as i64, i as i64, i, &head, &body);
    }
    let made = allocations() - before;

    log.close();
    let (written, dropped) = log.stats();
    assert_eq!(last, 100_000);
    assert_eq!(written + dropped, 100_001);
    assert!(written > 1000, "the writer moved little: {written}");
    assert_eq!(made, 0, "append allocated {made} times");
}
