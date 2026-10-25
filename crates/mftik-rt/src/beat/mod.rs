//! Beat once a second from a std thread of its own (PROTOCOL §5.2, §9.2.3,
//! E03-D003).
//!
//! The thread does not go through tokio or the dispatcher, and it does not
//! wait for any other thread. It waits only for the fd 3 write lock, which
//! another sender holds for the length of one frame. A hook that blocks does
//! not stop Beat, even while it holds the GIL, which this thread never
//! takes. A write that blocks because the shim does not read does.
//!
//! As in mftiko `contract/wire/c4`, the first Beat goes out one period after
//! the thread starts, `seq` starts at 1 and grows by one per Beat sent, and
//! `progress` is empty. Periods a blocked write ran past collapse into one
//! Beat sent at once, then the schedule resumes on its grid, as a Go ticker
//! keeps one tick and drops the rest. The caller starts the thread after
//! `Hello` (E03-D033).

use std::io::{self, Write};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use mftik_proto::wire::mftiko::worker::v1::{Beat, WorkerUp, worker_up};

use crate::frame::{FrameError, FrameWriter};

/// How often Beat goes out (§5.2, §9.2.3).
pub const BEAT_EVERY: Duration = Duration::from_secs(1);

/// Why the Beat thread returned.
#[derive(Debug)]
pub enum BeatEnd {
    /// [`BeatThread::stop`] was called.
    Stopped,
    /// A Beat could not be written, which includes a sealed writer. No later
    /// Beat is tried, as in Go.
    Write(FrameError),
}

/// The Beat thread. Dropping the handle leaves the thread running, like a
/// dropped `JoinHandle`.
pub struct BeatThread {
    stop: Arc<Stop>,
    thread: JoinHandle<BeatEnd>,
}

impl BeatThread {
    /// Starts sending one Beat every [`BEAT_EVERY`] on `out`.
    pub fn spawn<W>(out: Arc<FrameWriter<W>>) -> io::Result<BeatThread>
    where
        W: Write + Send + 'static,
    {
        Self::spawn_every(out, BEAT_EVERY)
    }

    pub(crate) fn spawn_every<W>(out: Arc<FrameWriter<W>>, every: Duration) -> io::Result<Self>
    where
        W: Write + Send + 'static,
    {
        assert!(!every.is_zero(), "beat period must not be zero");
        let stop = Arc::new(Stop::default());
        let thread = {
            let stop = stop.clone();
            thread::Builder::new()
                .name("mftik-beat".into())
                .spawn(move || run(&out, &stop, every))?
        };
        Ok(BeatThread { stop, thread })
    }

    /// Asks the thread to stop and returns at once. A Beat already due or
    /// being written still goes out; no later one does.
    pub fn stop(&self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
    }

    /// Waits for the thread to return. It returns only after
    /// [`BeatThread::stop`] or a failed write.
    pub fn join(self) -> BeatEnd {
        match self.thread.join() {
            Ok(end) => end,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.thread.is_finished()
    }
}

#[derive(Default)]
struct Stop {
    stopped: Mutex<bool>,
    wake: Condvar,
}

impl Stop {
    /// Waits until `due` on the monotonic clock. True when stopped first.
    fn wait_until(&self, due: Instant) -> bool {
        let mut stopped = self.stopped.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if *stopped {
                return true;
            }
            let now = Instant::now();
            if now >= due {
                return false;
            }
            stopped = self
                .wake
                .wait_timeout(stopped, due - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

fn run<W: Write>(out: &FrameWriter<W>, stop: &Stop, every: Duration) -> BeatEnd {
    let mut due = Instant::now() + every;
    let mut seq: u64 = 0;
    loop {
        if stop.wait_until(due) {
            return BeatEnd::Stopped;
        }
        seq += 1;
        let beat = WorkerUp {
            msg: Some(worker_up::Msg::Beat(Beat {
                seq,
                progress: Vec::new(),
            })),
        };
        if let Err(err) = out.send(&beat) {
            return BeatEnd::Write(err);
        }
        due = next_due(due, Instant::now(), every);
    }
}

/// When the next Beat is due, after the one due at `due` was written by
/// `now`: one period later, or, when the write ran past that, the latest
/// grid point not after `now`, which is due at once.
fn next_due(due: Instant, now: Instant, every: Duration) -> Instant {
    let next = due + every;
    if next >= now {
        return next;
    }
    let behind = (now - next).as_nanos();
    let step = every.as_nanos();
    u64::try_from(behind / step * step).map_or(now, |skip| next + Duration::from_nanos(skip))
}

#[cfg(test)]
mod tests;
