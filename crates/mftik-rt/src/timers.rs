//! One timer table for the session, on the IO thread (E03-D003, E03-D004).
//!
//! The sans-IO paths name their timers with a deadline on the injected
//! clock: C4's `ArmTimer`, the dispatcher's `Timer::after`, bookpath's
//! `ArmTimer`, the tdpath recon backoff, and symclient's revision check. Each
//! gets a [`TimerKey`] here. Arming a key again replaces the earlier arm, and
//! a replaced or disarmed arm never fires, which is the "only the latest arm
//! is live" rule every one of those paths states. The deadline is turned into
//! a monotonic instant once, with [`Clock::instant_at`], so a wall clock
//! step does not move it (E03-D004).
//!
//! The fire callback runs on the IO thread. It must not block: it hands the
//! token back to its owner (a channel send, `Timer::fire`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use mftik_core::bookpath;
use tokio::task::AbortHandle;
use tokio::time::Instant;

use crate::clock::Clock;
use crate::io::Io;

/// Which timer an arm is. One live arm per key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimerKey {
    /// The C4 machine's armed timer: an Init or Start deadline, or the
    /// OnStop budget. The machine arms one at a time.
    C4,
    /// A `dispatch::Timer`, by its id.
    Dispatch(u64),
    /// A bookpath timer of one instrument.
    Book { uid: u32, kind: bookpath::TimerKind },
    /// The tdpath recon backoff of one account.
    TdBackoff { api_id: i64 },
    /// The symclient revision check.
    Sym,
}

/// The table. Clones share it.
#[derive(Clone)]
pub struct Timers {
    io: Io,
    clock: Clock,
    table: Arc<Mutex<Table>>,
}

#[derive(Default)]
struct Table {
    next: u64,
    armed: HashMap<TimerKey, (u64, AbortHandle)>,
}

impl Timers {
    pub fn new(io: Io, clock: Clock) -> Self {
        Self {
            io,
            clock,
            table: Arc::default(),
        }
    }

    /// Calls `fire` on the IO thread once `now_ns` reaches `deadline_ns`,
    /// unless `key` is armed again or disarmed first. A deadline that has
    /// passed fires at once.
    pub fn arm<F>(&self, key: TimerKey, deadline_ns: i64, fire: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let at = Instant::from_std(self.clock.instant_at(deadline_ns));
        let table = Arc::clone(&self.table);
        let mut guard = lock(&self.table);
        guard.next += 1;
        let arm = guard.next;
        // The task takes the lock before it fires, so it cannot see the table
        // before this arm is in it.
        let task = self.io.spawn(async move {
            tokio::time::sleep_until(at).await;
            let live = {
                let mut table = lock(&table);
                match table.armed.get(&key) {
                    Some((id, _)) if *id == arm => {
                        table.armed.remove(&key);
                        true
                    }
                    _ => false,
                }
            };
            if live {
                fire();
            }
        });
        if let Some((_, old)) = guard.armed.insert(key, (arm, task.abort_handle())) {
            old.abort();
        }
    }

    /// The arm of `key`, if any, does not fire.
    pub fn disarm(&self, key: TimerKey) {
        if let Some((_, task)) = lock(&self.table).armed.remove(&key) {
            task.abort();
        }
    }

    /// Whether `key` is armed and has not fired.
    pub fn is_armed(&self, key: TimerKey) -> bool {
        lock(&self.table).armed.contains_key(&key)
    }
}

fn lock(table: &Mutex<Table>) -> MutexGuard<'_, Table> {
    table.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::{Duration, Instant as StdInstant};

    use super::*;

    fn table() -> (Timers, Clock, tokio::runtime::Runtime) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let clock = Clock::at(1_000_000_000, StdInstant::now());
        let timers = Timers::new(Io::from_handle(rt.handle().clone()), clock);
        (timers, clock, rt)
    }

    fn after(clock: &Clock, ms: u64) -> i64 {
        clock.now_ns() + (ms * 1_000_000) as i64
    }

    #[test]
    fn keys_fire_once_at_their_deadline_and_rearming_replaces() {
        let (timers, clock, rt) = table();
        let (tx, rx) = mpsc::channel();
        let keys = [
            TimerKey::C4,
            TimerKey::Dispatch(3),
            TimerKey::Book {
                uid: 1001,
                kind: bookpath::TimerKind::Backoff,
            },
            TimerKey::TdBackoff { api_id: 7 },
            TimerKey::Sym,
        ];
        for (n, key) in keys.into_iter().enumerate() {
            let tx = tx.clone();
            timers.arm(key, after(&clock, 10 + n as u64), move || {
                let _ = tx.send((key, "first"));
            });
        }
        // C4 again: the first arm of that key must not fire.
        let tx_c4 = tx.clone();
        timers.arm(TimerKey::C4, after(&clock, 30), move || {
            let _ = tx_c4.send((TimerKey::C4, "second"));
        });
        timers.disarm(TimerKey::Sym);
        let start = StdInstant::now();
        rt.block_on(async { tokio::time::sleep(Duration::from_millis(80)).await });
        drop(tx);
        let fired: Vec<_> = rx.iter().collect();
        assert!(start.elapsed() >= Duration::from_millis(30));
        assert_eq!(fired.len(), 4, "{fired:?}");
        assert!(fired.contains(&(TimerKey::C4, "second")));
        assert!(!fired.contains(&(TimerKey::C4, "first")));
        assert!(!fired.iter().any(|(key, _)| *key == TimerKey::Sym));
        assert!(fired.contains(&(TimerKey::TdBackoff { api_id: 7 }, "first")));
        assert!(!timers.is_armed(TimerKey::C4));
    }

    #[test]
    fn a_past_deadline_fires_at_once() {
        let (timers, clock, rt) = table();
        let (tx, rx) = mpsc::channel();
        timers.arm(
            TimerKey::Dispatch(1),
            clock.now_ns() - 1_000_000,
            move || {
                let _ = tx.send(());
            },
        );
        assert!(timers.is_armed(TimerKey::Dispatch(1)));
        rt.block_on(async { tokio::time::sleep(Duration::from_millis(5)).await });
        assert!(rx.try_recv().is_ok());
        assert!(!timers.is_armed(TimerKey::Dispatch(1)));
    }
}
