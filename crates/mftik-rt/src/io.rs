//! The IO thread: one tokio `current_thread` runtime on a thread of its own
//! (E03-D003).
//!
//! NATS and every timer run here. Hooks never do: they run on the thread
//! that called [`crate::run`], and a hook that blocks does not stop NATS
//! reads (PROTOCOL §9.9). Other threads hand work in with [`Io::spawn`], or
//! wait for it with [`Io::block_on`].

use std::future::Future;
use std::io;
use std::thread;

use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot;

/// The IO thread's handle. Clones share the thread.
#[derive(Clone, Debug)]
pub struct Io {
    handle: Handle,
}

impl Io {
    /// Starts the runtime on a thread named `mftik-io`. The thread runs
    /// until the process exits.
    pub fn start() -> io::Result<Io> {
        let rt = Builder::new_current_thread().enable_all().build()?;
        let handle = rt.handle().clone();
        thread::Builder::new()
            .name("mftik-io".into())
            .spawn(move || rt.block_on(std::future::pending::<()>()))?;
        Ok(Io { handle })
    }

    /// An IO handle on a runtime the caller drives, for tests.
    pub fn from_handle(handle: Handle) -> Io {
        Io { handle }
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    pub fn spawn<F>(&self, fut: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(fut)
    }

    /// Runs `fut` on the IO thread and blocks the calling thread until it
    /// finishes. Must not be called from the IO thread itself.
    pub fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(async move {
            let _ = tx.send(fut.await);
        });
        rx.blocking_recv()
            .expect("the IO thread runs until the process exits")
    }
}
