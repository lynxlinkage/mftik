//! The context of one plane call: Go's `ctx` for Init, Start, and Run.
//!
//! C4 gives Init and Start a `deadline_ns` on the injected clock and ends the
//! call in progress with `Effect::Cancel`. The plane checks [`CallCtx::is_done`]
//! between its steps and waits with [`CallCtx::done`] where it blocks: the
//! NATS connect loop now, a resolve once SYM is wired (E03-08). The deadline
//! is a monotonic instant taken once from the [`Clock`] (E03-D004).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

use crate::clock::Clock;

/// One call's context. Clones share it.
#[derive(Clone, Debug)]
pub struct CallCtx {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    deadline_ns: Option<i64>,
    deadline: Option<Instant>,
    cancelled: watch::Sender<bool>,
}

impl CallCtx {
    /// A context that ends at `deadline_ns` on `clock`, or never when `None`.
    pub fn new(clock: &Clock, deadline_ns: Option<i64>) -> Self {
        let deadline = deadline_ns.map(|ns| Instant::from_std(clock.instant_at(ns)));
        Self::with_deadline(deadline_ns, deadline)
    }

    /// A context with a deadline on tokio's clock, for tests that pause it.
    pub fn with_deadline(deadline_ns: Option<i64>, deadline: Option<Instant>) -> Self {
        Self {
            inner: Arc::new(Inner {
                deadline_ns,
                deadline,
                cancelled: watch::Sender::new(false),
            }),
        }
    }

    /// A context with no deadline.
    pub fn background() -> Self {
        Self::with_deadline(None, None)
    }

    /// Ends the context (C4 `Effect::Cancel`). Idempotent.
    pub fn cancel(&self) {
        self.inner.cancelled.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.inner.cancelled.borrow()
    }

    /// The deadline on the injected clock, as C4 armed it.
    pub fn deadline_ns(&self) -> Option<i64> {
        self.inner.deadline_ns
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.inner.deadline
    }

    /// What is left before the deadline: zero once it has passed, `None`
    /// without one.
    pub fn remaining(&self) -> Option<Duration> {
        self.inner
            .deadline
            .map(|at| at.saturating_duration_since(Instant::now()))
    }

    /// Cancelled, or past the deadline (Go `ctx.Err() != nil`).
    pub fn is_done(&self) -> bool {
        self.is_cancelled() || self.remaining().is_some_and(|left| left.is_zero())
    }

    /// Waits until the context is done.
    pub async fn done(&self) {
        let mut cancelled = self.inner.cancelled.subscribe();
        let cancel = async move {
            // The sender lives in `inner`, which `self` keeps alive.
            let _ = cancelled.wait_for(|c| *c).await;
        };
        match self.inner.deadline {
            Some(at) => {
                tokio::select! {
                    _ = cancel => {}
                    _ = tokio::time::sleep_until(at) => {}
                }
            }
            None => cancel.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn the_deadline_and_cancel_both_end_it() {
        let ctx = CallCtx::with_deadline(Some(5), Some(Instant::now() + Duration::from_secs(2)));
        assert!(!ctx.is_done());
        assert_eq!(ctx.remaining(), Some(Duration::from_secs(2)));
        let start = Instant::now();
        ctx.done().await;
        assert_eq!(start.elapsed(), Duration::from_secs(2));
        assert!(ctx.is_done());
        assert!(!ctx.is_cancelled());
        assert_eq!(ctx.deadline_ns(), Some(5));

        let ctx = CallCtx::background();
        assert_eq!(ctx.remaining(), None);
        let waiter = ctx.clone();
        let task = tokio::spawn(async move { waiter.done().await });
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert!(!task.is_finished());
        ctx.cancel();
        task.await.expect("done");
        assert!(ctx.is_done());
    }
}
