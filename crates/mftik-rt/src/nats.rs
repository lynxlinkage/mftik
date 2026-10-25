//! The session's NATS connection (PROTOCOL §9.4.1, E03-D006, E08-D050).
//!
//! async-nats, authenticated with `InitPayload.nats.user_jwt` and a nonce
//! signature by the nkey `seed`, with `custom_inbox_prefix` set to
//! `inbox_prefix`, at `MFTIKO_NATS_URL`. No other credential is used
//! (E09-D006). Like the Go SDK's `nats.Connect` options: the client name is
//! `mftiko-session` and reconnects never give up.
//!
//! [`connect`] dials until the connection is up or the Init context ends:
//! `deadline_ms`, or Stop, which cancels it; with no deadline it retries until
//! one of those (E08-D050, E08-D007). The schedule is Go's `connectNATS`:
//! wait 1 s after a failed dial, doubling up to 30 s, each wait and each
//! dial cut to what is left of the deadline, a dial waiting at most 1 s. A
//! dial error is not passed on, because its text can quote the credentials;
//! the log says `nats connect failed`.
//!
//! [`dial`] takes the [`Reconnected`] callback that Go's `nats.ReconnectHandler`
//! is: every connect after the first calls it, and the SYM path passes it on to
//! `symclient` (PROTOCOL §7.3, E03-D084).

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_nats::{AuthError, Client, ConnectOptions, Event};
use mftik_proto::wire::mftiko::stsctl::v1::NatsCredentials;

use crate::call::CallCtx;

/// Environment variable with the NATS address (PROTOCOL §5.4).
pub const NATS_URL_ENV: &str = "MFTIKO_NATS_URL";

/// The client name, as the Go SDK.
pub const CLIENT_NAME: &str = "mftiko-session";

/// The first wait after a failed dial. Doubles up to [`CONNECT_BACKOFF_MAX`].
pub const CONNECT_BACKOFF_MIN: Duration = Duration::from_secs(1);
pub const CONNECT_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// How long one dial waits, or less when the deadline is nearer.
pub const CONNECT_ATTEMPT: Duration = Duration::from_secs(1);

/// The log line for a failed dial.
pub const CONNECT_FAILED: &str = "nats connect failed";

/// Messages one subscription may hold before async-nats drops them as a slow
/// consumer. Go's nats.go buffers without a message limit; async-nats
/// defaults to 65536. The subscription tasks copy each message straight into
/// the dispatcher, so this only fills when the IO thread is starved, and
/// drops belong in the 32 MiB MD ring and the TD buffer, not here (E03-D068).
pub const SUBSCRIPTION_CAPACITY: usize = 1 << 20;

/// The Init context ended before a dial succeeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ended;

/// Called on the IO thread each time the connection is back after a loss. It
/// must not block.
pub type Reconnected = Arc<dyn Fn() + Send + Sync>;

/// One dial with the session's credentials. A seed that is not an nkey seed
/// fails the dial, as `nats.UserJWTAndSeed` does. `on_reconnect` runs on each
/// reconnect of the connection this dial makes, not on its first connect.
pub async fn dial(
    url: &str,
    creds: &NatsCredentials,
    timeout: Duration,
    on_reconnect: Option<Reconnected>,
) -> Option<Client> {
    let key_pair = Arc::new(nkeys::KeyPair::from_seed(&creds.seed).ok()?);
    let connected_once = Arc::new(AtomicBool::new(false));
    let options = ConnectOptions::with_jwt(creds.user_jwt.clone(), move |nonce| {
        let key_pair = Arc::clone(&key_pair);
        async move { key_pair.sign(&nonce).map_err(AuthError::new) }
    })
    .event_callback(move |event| {
        // async-nats reports the first connect as `Connected` too.
        let reconnect =
            matches!(event, Event::Connected) && connected_once.swap(true, Ordering::SeqCst);
        let on_reconnect = on_reconnect.clone().filter(|_| reconnect);
        async move {
            if let Some(f) = on_reconnect {
                f();
            }
        }
    })
    .name(CLIENT_NAME)
    .custom_inbox_prefix(&creds.inbox_prefix)
    .max_reconnects(None)
    .subscription_capacity(SUBSCRIPTION_CAPACITY)
    .connection_timeout(timeout);
    // connection_timeout bounds the TCP and TLS steps; the timeout here
    // bounds the whole handshake as well.
    match tokio::time::timeout(timeout, options.connect(url)).await {
        Ok(Ok(client)) => Some(client),
        _ => None,
    }
}

/// Dials with `dial` until it succeeds or `call` is done. `note` gets one
/// [`CONNECT_FAILED`] per failed dial.
pub async fn connect<C, D, F>(
    call: &CallCtx,
    mut dial: D,
    note: &(dyn Fn(&str) + Sync),
) -> Result<C, Ended>
where
    D: FnMut(Duration) -> F,
    F: Future<Output = Option<C>>,
{
    let mut delay = CONNECT_BACKOFF_MIN;
    loop {
        if call.is_done() {
            return Err(Ended);
        }
        let attempt = attempt_timeout(call);
        let got = tokio::select! {
            biased;
            _ = call.done() => return Err(Ended),
            got = dial(attempt) => got,
        };
        if let Some(client) = got {
            if call.is_done() {
                return Err(Ended);
            }
            return Ok(client);
        }
        if call.is_done() {
            return Err(Ended);
        }
        note(CONNECT_FAILED);
        let mut wait = delay;
        if let Some(left) = call.remaining() {
            if left.is_zero() {
                return Err(Ended);
            }
            wait = wait.min(left);
        }
        tokio::select! {
            biased;
            _ = call.done() => return Err(Ended),
            _ = tokio::time::sleep(wait) => {}
        }
        delay = (delay * 2).min(CONNECT_BACKOFF_MAX);
    }
}

fn attempt_timeout(call: &CallCtx) -> Duration {
    match call.remaining() {
        Some(left) if left.is_zero() => Duration::from_millis(1),
        Some(left) => left.min(CONNECT_ATTEMPT),
        None => CONNECT_ATTEMPT,
    }
}

/// The subject of [`barrier`]'s round trip (E03-D062, E03-D076). The session
/// may publish on `md.v1.fetch.>` (E08-D018), and no venue is `_flush` (a
/// venue is an uppercase letter and alphanumerics, PROTOCOL §2), so the
/// server answers with no responders.
pub const BARRIER_SUBJECT: &str = "md.v1.fetch._flush";

/// Go's `FlushTimeout`: returns once the server has read everything this
/// connection sent before the call, SUBs included, within `timeout`.
///
/// async-nats's `flush` only writes the socket. A request is answered only
/// after the server has read every SUB before it on the same connection, so
/// writing, then waiting for any answer to a request on [`BARRIER_SUBJECT`]
/// (no responders counts), is the PING/PONG Go waits for. A timeout or a
/// connection error fails it, with the text after `flush: `.
pub async fn barrier(client: &Client, timeout: Duration) -> Result<(), String> {
    let round_trip = async {
        client.flush().await.map_err(|err| err.to_string())?;
        let probe = async_nats::Request::new()
            .payload(bytes::Bytes::new())
            .timeout(Some(timeout));
        match client.send_request(BARRIER_SUBJECT, probe).await {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == async_nats::client::RequestErrorKind::NoResponders => Ok(()),
            Err(err) => Err(err.to_string()),
        }
    };
    match tokio::time::timeout(timeout, round_trip).await {
        Ok(result) => result,
        Err(_) => Err("timeout".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::time::Instant;

    use super::*;

    /// Records (start offset, timeout) of each dial and fails the first
    /// `fails` of them.
    struct Dialer {
        start: Instant,
        fails: usize,
        dials: Mutex<Vec<(Duration, Duration)>>,
    }

    impl Dialer {
        fn new(fails: usize) -> Self {
            Self {
                start: Instant::now(),
                fails,
                dials: Mutex::new(Vec::new()),
            }
        }

        async fn dial(&self, timeout: Duration) -> Option<u32> {
            let mut dials = self.dials.lock().unwrap();
            dials.push((self.start.elapsed(), timeout));
            (dials.len() > self.fails).then_some(dials.len() as u32)
        }

        fn dials(&self) -> Vec<(Duration, Duration)> {
            self.dials.lock().unwrap().clone()
        }
    }

    fn secs(s: f64) -> Duration {
        Duration::from_secs_f64(s)
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_deadline_the_backoff_doubles_up_to_thirty_seconds() {
        let dialer = Dialer::new(7);
        let notes = Mutex::new(0);
        let note = |line: &str| {
            assert_eq!(line, CONNECT_FAILED);
            *notes.lock().unwrap() += 1;
        };
        let call = CallCtx::background();
        let got = connect(&call, |t| dialer.dial(t), &note).await;
        assert_eq!(got, Ok(8));
        let starts: Vec<_> = dialer.dials().iter().map(|d| d.0).collect();
        // Waits 1, 2, 4, 8, 16, 30, 30.
        let want = [0.0, 1.0, 3.0, 7.0, 15.0, 31.0, 61.0, 91.0].map(secs);
        assert_eq!(starts, want);
        assert!(dialer.dials().iter().all(|d| d.1 == CONNECT_ATTEMPT));
        assert_eq!(*notes.lock().unwrap(), 7);
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_cuts_waits_and_dials_and_ends_the_loop() {
        let dialer = Dialer::new(usize::MAX);
        let call = CallCtx::with_deadline(Some(1), Some(Instant::now() + secs(2.5)));
        let got = connect(&call, |t| dialer.dial(t), &|_| {}).await;
        assert_eq!(got, Err(Ended));
        // Dials at 0 and 1; the wait after the second is cut to 1.5 s, and the
        // deadline ends the loop before a third dial.
        assert_eq!(
            dialer.dials(),
            vec![(secs(0.0), secs(1.0)), (secs(1.0), secs(1.0))]
        );
        assert_eq!(Instant::now() - dialer.start, secs(2.5));
    }

    #[tokio::test(start_paused = true)]
    async fn a_short_deadline_shortens_the_dial() {
        let dialer = Dialer::new(0);
        let call = CallCtx::with_deadline(Some(1), Some(Instant::now() + secs(0.25)));
        let got = connect(&call, |t| dialer.dial(t), &|_| {}).await;
        assert_eq!(got, Ok(1));
        assert_eq!(dialer.dials(), vec![(secs(0.0), secs(0.25))]);
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_ends_a_wait_at_once() {
        let dialer = Arc::new(Dialer::new(usize::MAX));
        let call = CallCtx::background();
        let task = {
            let call = call.clone();
            let dialer = Arc::clone(&dialer);
            tokio::spawn(async move { connect(&call, |t| dialer.dial(t), &|_| {}).await })
        };
        tokio::time::sleep(secs(2.5)).await;
        call.cancel();
        assert_eq!(task.await.expect("connect task"), Err(Ended));
        assert_eq!(dialer.dials().len(), 2);
        assert_eq!(Instant::now() - dialer.start, secs(2.5));
    }

    #[tokio::test(start_paused = true)]
    async fn a_done_context_does_not_dial() {
        let dialer = Dialer::new(0);
        let call = CallCtx::background();
        call.cancel();
        let got = connect(&call, |t| dialer.dial(t), &|_| {}).await;
        assert_eq!(got, Err(Ended));
        assert!(dialer.dials().is_empty());
    }
}
