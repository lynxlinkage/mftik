//! The thread that reads fd 3 (PROTOCOL §5.2, §9.2.7).
//!
//! It decodes each frame as a `WorkerDown` and hands it to the caller's
//! sink in the order it arrived, so Stop and Live are taken while a hook
//! blocks. What a message means is the C4 state machine's business
//! (E03-D001). The first end of the stream, clean or not, is the last event:
//! like the reader in mftiko `contract/wire/c4`, any error stops reading,
//! and the caller treats both as the shim having gone away (§9.2.7,
//! E03-D032).

use std::io::{self, BufReader, Read};
use std::thread::{self, JoinHandle};

use mftik_proto::wire::mftiko::worker::v1::WorkerDown;

use crate::frame::{FrameError, FrameReader};

/// What the reader thread hands to its sink.
#[derive(Debug)]
pub enum ReadEvent {
    /// One decoded frame. Unknown fields are skipped; a message type this
    /// build does not know arrives with `msg` set to `None` (PROTOCOL §9.3).
    Message(WorkerDown),
    /// The shim closed its end between frames. Last event.
    Eof,
    /// The stream ended inside a frame, a frame was too large or did not
    /// decode, or the read failed. Last event.
    Failed(FrameError),
}

/// Spawns the thread that reads `r` and calls `sink` once per event.
///
/// The sink runs on the reader thread and should not block for long: frames
/// behind it wait. A std `mpsc::Sender` works as a sink:
/// `move |event| { let _ = tx.send(event); }`. The thread returns after
/// [`ReadEvent::Eof`] or [`ReadEvent::Failed`] and drops `r`.
pub fn spawn_reader<R, F>(r: R, mut sink: F) -> io::Result<JoinHandle<()>>
where
    R: Read + Send + 'static,
    F: FnMut(ReadEvent) + Send + 'static,
{
    thread::Builder::new()
        .name("mftik-ctl-read".into())
        .spawn(move || {
            let mut frames = FrameReader::new(BufReader::new(r));
            loop {
                match frames.read_message::<WorkerDown>() {
                    Ok(Some(m)) => sink(ReadEvent::Message(m)),
                    Ok(None) => return sink(ReadEvent::Eof),
                    Err(err) => return sink(ReadEvent::Failed(err)),
                }
            }
        })
}
