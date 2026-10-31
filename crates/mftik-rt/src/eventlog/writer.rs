//! The event log writer (§12.1, E07-D001), as Go `sdk/internal/evlog`.
//!
//! [`Log::append`] copies one frame into a preallocated ring and returns. It
//! does not allocate and does not wait for the disk: a frame the ring cannot
//! take is dropped and counted, and its `rec_seq` is still used. A thread of
//! its own moves the ring to files every 100 ms, or when the ring is a
//! quarter full; it starts a new file when the current one would pass its
//! size and deletes the oldest files so the directory stays within the keep
//! limit. [`Log::close`] appends the `end` frame, waits at most five seconds
//! for the ring to reach the file, and fsyncs the last file.

use std::fmt::Display;
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::{FRAME_HEAD, Header, Kind, MAX_FRAME, end_body, files, frame_head, name};

/// The keep limit when the spec's `keep_mb` is 0.
pub const DEFAULT_KEEP: u64 = 256 << 20;
/// The ring between the hooks and the disk.
pub const DEFAULT_RING: usize = 8 << 20;
/// How long a frame may wait in a quiet ring.
pub const DEFAULT_FLUSH: Duration = Duration::from_millis(100);

/// A file is at least this big before it rotates, whatever the keep limit.
const MIN_FILE: u64 = 64 << 10;
/// A file is at most this big before it rotates.
const MAX_FILE: u64 = 64 << 20;
/// After a failed directory, open, or write, the writer tries again this much later.
const RETRY_AFTER: Duration = Duration::from_secs(1);
/// [`Log::close`] waits this long for the ring to reach the file.
const CLOSE_WAIT: Duration = Duration::from_secs(5);

/// Where one file's bytes go. [`Config::create`] returns one.
pub trait Sink: Send {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;
    fn sync(&mut self) -> io::Result<()>;
}

impl Sink for fs::File {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        Write::write_all(self, bytes)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

/// Creates the file at a path. The default is `O_EXCL` with mode 0640.
pub type Create = Box<dyn FnMut(&Path) -> io::Result<Box<dyn Sink>> + Send>;

/// The writer's own failure lines.
pub type LogLine = Arc<dyn Fn(&str) + Send + Sync>;

/// Opens a [`Log`]. Zero values select the defaults.
#[derive(Default)]
pub struct Config {
    /// `$MFTIKO_VOLUME_DIR/eventlog`. `None` (no volume directory) drops
    /// every frame.
    pub dir: Option<PathBuf>,
    /// The session, the incarnation, and the SDK; the writer sets `n` and
    /// `created_ns` of each file.
    pub header: Header,
    /// The keep limit in bytes; 0 is [`DEFAULT_KEEP`].
    pub keep_bytes: u64,
    pub ring_bytes: usize,
    pub flush_every: Duration,
    pub create: Option<Create>,
    /// Unix ns for `created_ns` and the `end` frame; `None` is the wall clock.
    pub now_ns: Option<fn() -> i64>,
    /// `None` writes the SDK's own log line.
    pub log: Option<LogLine>,
}

/// One session's event log writer. [`Log::append`], [`Log::flush`], and
/// [`Log::stats`] are safe from any thread. Dropping it closes it.
pub struct Log {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

struct Shared {
    state: Mutex<State>,
    /// The writer thread waits here for a kick, a flush, or the close.
    wake: Condvar,
    /// Flush and close wait here for the writer thread to move the ring.
    moved: Condvar,
    ring: Ring,
    kick_at: u64,
    now_ns: fn() -> i64,
    log: LogLine,
}

#[derive(Default)]
struct State {
    /// Bytes before `head` are in a file or were dropped.
    head: u64,
    /// The next frame starts here.
    tail: u64,
    seq: u64,
    dropped: u64,
    written: u64,
    closed: bool,
    stop: bool,
    exited: bool,
    /// A quarter-full kick was sent and the writer has not drained since.
    kicked: bool,
    /// The writer thread should drain now.
    kick: bool,
}

impl Log {
    /// Starts the writer thread. It does not touch the disk until the first
    /// frame is due, and it does not fail: a directory or file it cannot
    /// write counts the frames as dropped, logs once, and tries again a
    /// second later.
    pub fn open(cfg: Config) -> io::Result<Self> {
        let ring = if cfg.ring_bytes == 0 {
            DEFAULT_RING
        } else {
            cfg.ring_bytes
        };
        let keep = if cfg.keep_bytes == 0 {
            DEFAULT_KEEP
        } else {
            cfg.keep_bytes
        };
        let every = if cfg.flush_every.is_zero() {
            DEFAULT_FLUSH
        } else {
            cfg.flush_every
        };
        let now_ns = cfg.now_ns.unwrap_or(wall_ns);
        let log = cfg
            .log
            .unwrap_or_else(|| Arc::new(|line: &str| crate::log::note(line)));
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            moved: Condvar::new(),
            ring: Ring::new(ring),
            kick_at: (ring / 4) as u64,
            now_ns,
            log: log.clone(),
        });
        let disk = Disk {
            dir: cfg.dir,
            header: cfg.header,
            keep,
            file_max: (keep / 4).clamp(MIN_FILE, MAX_FILE),
            create: cfg.create.unwrap_or_else(|| Box::new(create_file)),
            now_ns,
            log,
            file: None,
            file_n: 0,
            file_size: 0,
            file_frames: 0,
            retry_at: None,
            failing: false,
        };
        let thread = thread::Builder::new()
            .name("mftik-eventlog".to_owned())
            .spawn({
                let shared = shared.clone();
                move || run(&shared, disk, every)
            })?;
        Ok(Self {
            shared,
            thread: Mutex::new(Some(thread)),
        })
    }

    /// Adds one frame whose body is `a` followed by `b`, and returns its
    /// `rec_seq`. The number is used even when the frame is dropped (the ring
    /// is full, the frame is larger than the ring or [`MAX_FRAME`], or the log
    /// is closed), so a reader sees the gap. It copies into the ring and
    /// returns: no allocation, no disk.
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &self,
        kind: Kind,
        flags: u16,
        recv_ns: i64,
        t_ns: i64,
        r#ref: u64,
        a: &[u8],
        b: &[u8],
    ) -> u64 {
        let mut st = self.shared.lock();
        let (seq, kick) = self
            .shared
            .append_locked(&mut st, kind, flags, recv_ns, t_ns, r#ref, a, b);
        drop(st);
        if kick {
            self.shared.wake.notify_one();
        }
        seq
    }

    /// Frames in files, and frames dropped: the ring was full, the file could
    /// not be written, or the log was already closed. Both accumulate.
    pub fn stats(&self) -> (u64, u64) {
        let st = self.shared.lock();
        (st.written, st.dropped)
    }

    /// Waits until every frame appended before it is in a file or counted as
    /// dropped.
    pub fn flush(&self) {
        let mut st = self.shared.lock();
        let target = st.tail;
        while st.head < target && !st.exited {
            st.kick = true;
            self.shared.wake.notify_one();
            st = self
                .shared
                .moved
                .wait(st)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Appends the `end` frame and waits until the ring is in files, at most
    /// five seconds; the last file is fsynced. A frame appended after this is
    /// dropped. A second close does nothing.
    pub fn close(&self) {
        let shared = &self.shared;
        let mut st = shared.lock();
        if st.closed {
            return;
        }
        let end = end_body(st.dropped);
        let now = (shared.now_ns)();
        shared.append_locked(&mut st, Kind::END, 0, 0, now, 0, &end, &[]);
        st.closed = true;
        st.stop = true;
        shared.wake.notify_one();
        let deadline = Instant::now() + CLOSE_WAIT;
        while !st.exited {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                drop(st);
                (shared.log)("event log: close did not finish writing");
                return;
            }
            st = shared
                .moved
                .wait_timeout(st, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        drop(st);
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(thread) = thread {
            let _ = thread.join();
        }
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        self.close();
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[allow(clippy::too_many_arguments)]
    fn append_locked(
        &self,
        st: &mut State,
        kind: Kind,
        flags: u16,
        recv_ns: i64,
        t_ns: i64,
        r#ref: u64,
        a: &[u8],
        b: &[u8],
    ) -> (u64, bool) {
        st.seq += 1;
        let seq = st.seq;
        let body = a.len().saturating_add(b.len());
        let free = self.ring.len as u64 - (st.tail - st.head);
        if st.closed || body > MAX_FRAME - (FRAME_HEAD - 4) || (FRAME_HEAD + body) as u64 > free {
            st.dropped += 1;
            return (seq, false);
        }
        let head = frame_head(kind, flags, seq, recv_ns, t_ns, r#ref, body);
        // SAFETY: the state lock is held, and the frame fits in the free part
        // of the ring, [tail, head + len), which the writer thread does not read.
        unsafe {
            let pos = self.ring.put(st.tail, &head);
            let pos = self.ring.put(pos, a);
            st.tail = self.ring.put(pos, b);
        }
        if !st.kicked && st.tail - st.head >= self.kick_at {
            st.kicked = true;
            st.kick = true;
            return (seq, true);
        }
        (seq, false)
    }
}

/// The ring's bytes. Positions grow without bound; a position's byte is at
/// `pos % len`. Under the state lock, appends write `[tail, head + len)`; the
/// writer thread reads `[head, tail)` without the lock and only then moves
/// `head`, under the lock. The two ranges never overlap, and the lock orders
/// each write before the read of the same bytes.
struct Ring {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: the bytes are reached only as described on `Ring`.
unsafe impl Send for Ring {}
// SAFETY: as above.
unsafe impl Sync for Ring {}

impl Ring {
    fn new(len: usize) -> Self {
        let bytes = vec![0u8; len].into_boxed_slice();
        Self {
            ptr: Box::into_raw(bytes).cast::<u8>(),
            len,
        }
    }

    /// Copies `p` to `pos` and returns the position after it.
    ///
    /// # Safety
    /// The state lock is held and `[pos, pos + p.len())` is free.
    unsafe fn put(&self, mut pos: u64, mut p: &[u8]) -> u64 {
        while !p.is_empty() {
            let off = (pos % self.len as u64) as usize;
            let n = p.len().min(self.len - off);
            // SAFETY: `off + n <= len`, and the caller owns those bytes.
            unsafe { std::ptr::copy_nonoverlapping(p.as_ptr(), self.ptr.add(off), n) };
            p = &p[n..];
            pos += n as u64;
        }
        pos
    }

    /// `[from, to)` as one or two slices.
    ///
    /// # Safety
    /// `head <= from <= to <= tail`, read by the writer thread before it moves `head`.
    unsafe fn range(&self, from: u64, to: u64) -> (&[u8], &[u8]) {
        let off = (from % self.len as u64) as usize;
        let n = (to - from) as usize;
        let first = n.min(self.len - off);
        // SAFETY: both slices are in the buffer and no append writes them
        // until `head` passes `to`.
        unsafe {
            (
                std::slice::from_raw_parts(self.ptr.add(off), first),
                std::slice::from_raw_parts(self.ptr, n - first),
            )
        }
    }

    /// The whole frame at `pos`, its length field included.
    ///
    /// # Safety
    /// As for [`Ring::range`], with a frame starting at `pos`.
    unsafe fn frame_len(&self, pos: u64) -> u64 {
        let mut b = [0u8; 4];
        for (i, byte) in b.iter_mut().enumerate() {
            let off = ((pos + i as u64) % self.len as u64) as usize;
            // SAFETY: `off < len`, inside a filled frame.
            *byte = unsafe { *self.ptr.add(off) };
        }
        4 + u64::from(u32::from_le_bytes(b))
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        // SAFETY: `ptr` and `len` came from the boxed slice in `new`.
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(self.ptr, self.len)) });
    }
}

/// What only the writer thread touches.
struct Disk {
    dir: Option<PathBuf>,
    header: Header,
    keep: u64,
    file_max: u64,
    create: Create,
    now_ns: fn() -> i64,
    log: LogLine,
    file: Option<Box<dyn Sink>>,
    file_n: u32,
    file_size: u64,
    file_frames: u64,
    retry_at: Option<Instant>,
    failing: bool,
}

/// Marks the writer thread gone however it ends, so flush and close stop waiting.
struct Exited<'a>(&'a Shared);

impl Drop for Exited<'_> {
    fn drop(&mut self) {
        self.0.lock().exited = true;
        self.0.moved.notify_all();
    }
}

fn run(shared: &Shared, mut disk: Disk, every: Duration) {
    let _exited = Exited(shared);
    let mut tick = Instant::now() + every;
    loop {
        let stop = {
            let mut st = shared.lock();
            loop {
                if st.stop {
                    break true;
                }
                if st.kick {
                    st.kick = false;
                    break false;
                }
                let now = Instant::now();
                if now >= tick {
                    tick = now + every;
                    break false;
                }
                st = shared
                    .wake
                    .wait_timeout(st, tick - now)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
        };
        drain(shared, &mut disk);
        if stop {
            disk.close_file(true);
            return;
        }
    }
}

fn drain(shared: &Shared, disk: &mut Disk) {
    let (head, tail) = {
        let mut st = shared.lock();
        st.kicked = false;
        (st.head, st.tail)
    };
    if head == tail {
        return;
    }
    let (written, lost) = disk.write_out(&shared.ring, head, tail);
    let mut st = shared.lock();
    st.head = tail;
    st.written += written;
    st.dropped += lost;
    drop(st);
    shared.moved.notify_all();
}

impl Disk {
    /// Writes the frames in `[from, to)`. A frame that would take the current
    /// file past its size starts a new file; a frame larger than that size
    /// gets a file of its own.
    fn write_out(&mut self, ring: &Ring, from: u64, to: u64) -> (u64, u64) {
        let (mut written, mut lost) = (0, 0);
        let mut pos = from;
        // SAFETY (each block below): `[from, to)` is the drained range of
        // `drain`, and `pos` is always a frame start inside it.
        while pos < to {
            if self.file.is_some()
                && self.file_frames > 0
                && self.file_size + unsafe { ring.frame_len(pos) } > self.file_max
            {
                self.close_file(false);
            }
            if !self.open_file() {
                return (written, lost + count_frames(ring, pos, to));
            }
            let mut end = pos;
            let mut k = 0;
            let mut size = self.file_size;
            while end < to {
                let n = unsafe { ring.frame_len(end) };
                if (self.file_frames > 0 || k > 0) && size + n > self.file_max {
                    break;
                }
                size += n;
                end += n;
                k += 1;
            }
            let (first, second) = unsafe { ring.range(pos, end) };
            let file = self.file.as_mut().expect("open_file opened it");
            match file.write_all(first).and_then(|()| file.write_all(second)) {
                Ok(()) => {
                    written += k;
                    self.file_size = size;
                    self.file_frames += k;
                    self.failing = false;
                }
                Err(err) => {
                    lost += k;
                    self.fail(err);
                }
            }
            pos = end;
        }
        (written, lost)
    }

    fn open_file(&mut self) -> bool {
        if self.file.is_some() {
            return true;
        }
        if self.retry_at.is_some_and(|at| Instant::now() < at) {
            return false;
        }
        let Some(dir) = self.dir.clone() else {
            self.fail("no volume directory");
            return false;
        };
        if let Err(err) = DirBuilder::new().recursive(true).mode(0o750).create(&dir) {
            self.fail(err);
            return false;
        }
        self.retain(&dir);
        let n = self.next_n(&dir);
        let mut file = match (self.create)(&dir.join(name(self.header.incarnation, n))) {
            Ok(file) => file,
            Err(err) => {
                if err.kind() == io::ErrorKind::AlreadyExists {
                    self.file_n = n;
                }
                self.fail(err);
                return false;
            }
        };
        let mut header = self.header.clone();
        header.n = n;
        header.created_ns = (self.now_ns)();
        let mut bytes = Vec::new();
        header.encode(&mut bytes);
        if let Err(err) = file.write_all(&bytes) {
            self.file_n = n;
            self.fail(err);
            return false;
        }
        self.file = Some(file);
        self.file_n = n;
        self.file_size = bytes.len() as u64;
        self.file_frames = 0;
        true
    }

    /// One past the highest file of this incarnation, so a worker that comes
    /// back with the same incarnation does not overwrite what is there.
    fn next_n(&self, dir: &Path) -> u32 {
        if self.file_n > 0 {
            return self.file_n + 1;
        }
        let highest = files(dir)
            .unwrap_or_default()
            .iter()
            .filter(|f| f.incarnation == self.header.incarnation)
            .map(|f| f.n)
            .max()
            .unwrap_or(0);
        highest + 1
    }

    /// Deletes the oldest files, of any incarnation, until the files left and
    /// one more full file fit in the keep limit. It runs before a file is
    /// created, so the file being written is never one of them.
    fn retain(&self, dir: &Path) {
        let Ok(list) = files(dir) else {
            return;
        };
        let sizes: Vec<u64> = list
            .iter()
            .map(|f| fs::metadata(&f.path).map_or(0, |m| m.len()))
            .collect();
        let mut total: u64 = sizes.iter().sum();
        for (f, size) in list.iter().zip(sizes) {
            if total + self.file_max <= self.keep {
                break;
            }
            if fs::remove_file(&f.path).is_ok() {
                total -= size;
            }
        }
    }

    fn close_file(&mut self, last: bool) {
        if let Some(mut file) = self.file.take()
            && last
        {
            let _ = file.sync();
        }
    }

    /// Drops the batch's file, waits a second before the next try, and logs
    /// the first failure of a run of them.
    fn fail(&mut self, err: impl Display) {
        self.file = None;
        self.retry_at = Some(Instant::now() + RETRY_AFTER);
        if !self.failing {
            self.failing = true;
            (self.log)(&format!("event log: {err}"));
        }
    }
}

fn count_frames(ring: &Ring, mut pos: u64, to: u64) -> u64 {
    let mut k = 0;
    while pos < to {
        // SAFETY: as in `Disk::write_out`.
        pos += unsafe { ring.frame_len(pos) };
        k += 1;
    }
    k
}

pub(super) fn create_file(path: &Path) -> io::Result<Box<dyn Sink>> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o640)
        .open(path)?;
    Ok(Box::new(file))
}

fn wall_ns() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(since) => i64::try_from(since.as_nanos()).unwrap_or(i64::MAX),
        Err(before) => -i64::try_from(before.duration().as_nanos()).unwrap_or(i64::MAX),
    }
}
