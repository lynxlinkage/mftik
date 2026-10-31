//! `stsctl.Report` on fd 3 while Run runs (PROTOCOL §9.2.4, §9.5.6).
//!
//! A std thread of its own, like Beat (E03-D003): it never waits for a hook
//! or the IO thread, only for the fd 3 write lock. Every [`REPORT_POLL`] it
//! reads the dispatcher's [`Stats`] and the bindings last bound, and sends a
//! Report when that differs from the last one sent, or when [`REPORT_EVERY`]
//! has passed since it, so one goes out at least every 5 seconds. A newer
//! `bindings_applied` wakes it at once (E03-D091).
//!
//! `QueueStat.max_age_ms` is the oldest event handed to a hook since the
//! previous Report: [`Stats`] clears it on every read, so the polls between
//! two Reports keep the largest (E03-D092).
//!
//! `event_log` is set only while the strategy event log is on (E20-D007, Go
//! `eventLogStat`); with it off the Report is what it was before.
//!
//! [`Reporter::close`] comes before `Exiting` goes out: it waits for a Report
//! being written and lets none start after it, so no Report follows
//! `Exiting` (§5.3, E03-D093).

#[cfg(test)]
mod tests;

use std::io::{self, Write};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use mftik_core::c4::CONTRACT_STS;
use mftik_core::dispatch::Stats;
use mftik_proto::wire::mftiko::common::v1::Version;
use mftik_proto::wire::mftiko::stsctl::v1::{EventLogStat, QueueStat, Report};
use mftik_proto::wire::mftiko::worker::v1 as worker;
use mftik_proto::wire::mftiko::worker::v1::{WorkerUp, worker_up};
use prost::Message;

use crate::frame::FrameWriter;

/// How often the Report thread reads the dispatcher.
pub const REPORT_POLL: Duration = Duration::from_secs(1);

/// The longest gap between two Reports (§9.2.4).
pub const REPORT_EVERY: Duration = Duration::from_secs(5);

/// `QueueStat.name` of the TD buffer and the MD ring (E08-D003).
pub const QUEUE_TD: &str = "td";
pub const QUEUE_MD: &str = "md";

/// What a Report is read from.
pub(crate) struct Source {
    pub(crate) stats: Box<dyn FnMut() -> Stats + Send>,
    pub(crate) bindings_applied: Box<dyn FnMut() -> Option<Version> + Send>,
    pub(crate) event_log: Box<dyn FnMut() -> Option<EventLogStat> + Send>,
}

/// The Report thread. See the module docs.
pub(crate) struct Reporter {
    closed: Arc<Mutex<bool>>,
    wake: Sender<()>,
}

impl Reporter {
    /// Starts the thread; the first Report goes out at once.
    pub(crate) fn spawn<W>(out: Arc<FrameWriter<W>>, source: Source) -> io::Result<Reporter>
    where
        W: Write + Send + 'static,
    {
        let closed = Arc::new(Mutex::new(false));
        let (wake, woken) = mpsc::channel();
        let gate = Arc::clone(&closed);
        thread::Builder::new()
            .name("mftik-report".into())
            .spawn(move || run(&out, source, &gate, &woken))?;
        Ok(Reporter { closed, wake })
    }

    /// A handle that wakes the thread to read again, for `bindings_applied`.
    /// It never blocks.
    pub(crate) fn waker(&self) -> Sender<()> {
        self.wake.clone()
    }

    /// No Report after this returns: a Report being written finishes first.
    pub(crate) fn close(&self) {
        *lock(&self.closed) = true;
        let _ = self.wake.send(());
    }
}

fn run<W: Write>(
    out: &FrameWriter<W>,
    mut source: Source,
    closed: &Mutex<bool>,
    woken: &Receiver<()>,
) {
    let mut sampler = Sampler::default();
    let mut next = Instant::now();
    loop {
        // The next poll, or sooner when the 5 s Report is due before it.
        let until = sampler.due_at().map_or(next, |due| due.min(next));
        match woken.recv_timeout(until.saturating_duration_since(Instant::now())) {
            Ok(()) => while woken.try_recv().is_ok() {},
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        let now = Instant::now();
        if now >= next {
            next = now + REPORT_POLL;
        }
        // Held while the frame is written, so `close` waits for it.
        let closed = lock(closed);
        if *closed {
            return;
        }
        let stats = (source.stats)();
        let applied = (source.bindings_applied)();
        let event_log = (source.event_log)();
        if let Some(report) = sampler.sample(stats, applied, event_log, Instant::now())
            && out.send(&frame(&report)).is_err()
        {
            // Sealed or the shim is gone; PROTOCOL §9.7 takes over.
            return;
        }
    }
}

/// Decides when a Report goes out, and keeps the largest `max_age_ms` of the
/// reads in between.
#[derive(Debug, Default)]
pub(crate) struct Sampler {
    last: Option<(Report, Instant)>,
    td_age_ms: u32,
    md_age_ms: u32,
}

impl Sampler {
    /// When an unchanged Report goes out anyway: [`REPORT_EVERY`] after the
    /// last one. `None` before the first.
    pub(crate) fn due_at(&self) -> Option<Instant> {
        self.last.as_ref().map(|(_, at)| *at + REPORT_EVERY)
    }

    /// One read. `Some` is the Report to send now.
    pub(crate) fn sample(
        &mut self,
        stats: Stats,
        bindings_applied: Option<Version>,
        event_log: Option<EventLogStat>,
        now: Instant,
    ) -> Option<Report> {
        self.td_age_ms = self.td_age_ms.max(stats.td.max_age_ms);
        self.md_age_ms = self.md_age_ms.max(stats.md.max_age_ms);
        let report = Report {
            bindings_applied,
            // The dispatcher does not name its hooks (E03-D094).
            hook: String::new(),
            hook_blocked_ms: stats.hook_blocked_ms,
            hook_slow: stats.hook_slow,
            queues: vec![
                queue(QUEUE_TD, stats.td.depth, stats.td.dropped, self.td_age_ms),
                queue(QUEUE_MD, stats.md.depth, stats.md.dropped, self.md_age_ms),
            ],
            event_log,
        };
        if let Some((last, at)) = &self.last
            && *last == report
            && now.saturating_duration_since(*at) < REPORT_EVERY
        {
            return None;
        }
        self.td_age_ms = 0;
        self.md_age_ms = 0;
        self.last = Some((report.clone(), now));
        Some(report)
    }
}

fn queue(name: &str, depth: u32, dropped: u64, max_age_ms: u32) -> QueueStat {
    QueueStat {
        name: name.to_owned(),
        depth,
        dropped,
        max_age_ms,
    }
}

/// `worker.Report` with the session's contract (Go `Session.Report`).
pub(crate) fn frame(report: &Report) -> WorkerUp {
    WorkerUp {
        msg: Some(worker_up::Msg::Report(worker::Report {
            contract: CONTRACT_STS.to_owned(),
            payload: report.encode_to_vec(),
        })),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
