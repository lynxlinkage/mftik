//! A JSON line makes no heap allocation (E06-D011, E06-D014).
//!
//! This binary's global allocator counts the allocations each thread makes.
//! The first line on a thread creates that thread's buffer, so each test
//! writes one line before it starts counting.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::{self, Read};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::thread;

use mftik_rt::log::{COMPONENT, Field, Level, Logger, SDK_VERSION, Style, Value, emit};

const LINES: usize = 1000;

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

fn field(name: &str, value: Value) -> Field {
    Field {
        name: name.to_owned(),
        value,
    }
}

/// Every value kind, an escaped string, and floats on each of the paths
/// that turn ryu's text into zerolog's.
fn fields() -> Vec<Field> {
    vec![
        field("text", Value::Str("abc".to_owned())),
        field("escaped", Value::Str("a\"b\\c\n\u{7f} café".to_owned())),
        field("count", Value::Int(-42)),
        field("wide", Value::Big("18446744073709551616".to_owned())),
        field("ratio", Value::Float(0.1)),
        field("whole", Value::Float(100.0)),
        field("small", Value::Float(1.5e-6)),
        field("tiny", Value::Float(1e-7)),
        field("large", Value::Float(1e16)),
        field("huge", Value::Float(1e21)),
        field("nan", Value::Float(f64::NAN)),
        field("on", Value::Bool(true)),
        field("off", Value::Bool(false)),
        field("missing", Value::Null),
    ]
}

#[test]
fn logger_lines_do_not_allocate() {
    let logger = Logger {
        min: Level::Trace as i8,
        style: Style::Json,
        no_color: true,
        component: COMPONENT.to_owned(),
        version: "v1".to_owned(),
        commit: "c1".to_owned(),
        sdk_version: SDK_VERSION.to_owned(),
        session_id: Some("s1".to_owned()),
    };
    let fields = fields();
    let levels = [
        Level::Trace,
        Level::Debug,
        Level::Info,
        Level::Warn,
        Level::Error,
        Level::Fatal,
        Level::Panic,
    ];
    let mut sink = Vec::with_capacity(LINES * 1024);
    logger.write(Level::Info, "first", &fields, 0, &mut sink);

    // 7 ms apart, so the lines cross several seconds.
    let mut now_ns = 1_700_000_000_000_000_000;
    let before = allocations();
    for level in levels.iter().cycle().take(LINES) {
        logger.write(*level, "hello", &fields, now_ns, &mut sink);
        now_ns += 7_000_000;
    }
    let made = allocations() - before;

    assert_eq!(made, 0, "{made} allocations in {LINES} lines");
    let text = String::from_utf8(sink).expect("utf-8");
    assert_eq!(text.lines().count(), LINES + 1);
    let last = text.lines().last().expect("a line");
    assert!(
        last.starts_with(r#"{"level":"fatal","time":"2023-11-14T22:13:26.993Z","#),
        "{last}"
    );
    assert!(
        last.ends_with(concat!(
            r#""session_id":"s1","text":"abc","escaped":"a\"b\\c\n\u007f café","#,
            r#""count":-42,"wide":18446744073709551616,"ratio":0.1,"whole":100,"#,
            r#""small":0.0000015,"tiny":1e-7,"large":10000000000000000,"huge":1e+21,"#,
            r#""nan":"NaN","on":true,"off":false,"missing":null,"message":"hello"}"#,
        )),
        "{last}"
    );
}

/// `emit` with the default level and format writes to fd 2, which points at
/// a pipe for the length of the test.
#[test]
fn emitted_lines_reach_stderr_without_allocating() {
    let fields = fields();
    let (mut reader, writer) = io::pipe().expect("pipe");
    let drain = thread::spawn(move || {
        let mut text = String::new();
        reader.read_to_string(&mut text).expect("read stderr");
        text
    });
    let redirect = Redirect::stderr_to(writer.as_fd());
    drop(writer);
    emit(Level::Info, "first", &fields, Some("s1"));

    let before = allocations();
    for _ in 0..LINES {
        emit(Level::Info, "hello", &fields, Some("s1"));
    }
    let made = allocations() - before;
    drop(redirect);

    let text = drain.join().expect("drain");
    assert_eq!(made, 0, "{made} allocations in {LINES} lines");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), LINES + 1, "{text}");
    for line in &lines[1..] {
        assert!(line.starts_with(r#"{"level":"info","time":""#), "{line}");
        assert!(
            line.contains(r#","component":"strategy","version":"","commit":"","#),
            "{line}"
        );
        assert!(
            line.contains(r#","session_id":"s1","text":"abc","#),
            "{line}"
        );
        assert!(
            line.ends_with(r#","missing":null,"message":"hello"}"#),
            "{line}"
        );
    }
}

/// Fd 2 points at another descriptor until this drops.
struct Redirect {
    saved: OwnedFd,
}

impl Redirect {
    fn stderr_to(target: BorrowedFd<'_>) -> Self {
        let saved = io::stderr().as_fd().try_clone_to_owned().expect("dup fd 2");
        dup2(target, 2);
        Self { saved }
    }
}

impl Drop for Redirect {
    fn drop(&mut self) {
        dup2(self.saved.as_fd(), 2);
    }
}

fn dup2(from: BorrowedFd<'_>, to: RawFd) {
    // SAFETY: `from` is open, and `to` is fd 2, which no `OwnedFd` holds.
    let rc = unsafe { libc::dup2(from.as_raw_fd(), to) };
    assert_eq!(rc, to, "dup2: {}", io::Error::last_os_error());
}
