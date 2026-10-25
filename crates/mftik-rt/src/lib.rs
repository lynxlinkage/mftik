//! Session worker IO.
//!
//! This crate moves C4 frames on fd 3 and sends Beat (E03-D001, E03-D002).
//! The C4 protocol itself (Hello, Init, Start, Stop, the ending) is a sans-IO
//! state machine in `mftik-core`; this crate only carries its frames.
//!
//! - [`frame`]: a 4-byte big-endian length and one protobuf message of at
//!   most 1 MiB (PROTOCOL §5.1, §9.2.1). One write lock per whole frame
//!   (E03-D003).
//! - [`ctl`]: the control fd from `MFTIKO_CTL_FD`, 3 by default, with
//!   close-on-exec set as soon as it is taken (§9.2.1).
//! - [`reader`]: a thread that decodes `WorkerDown` and reports the end of
//!   the stream (§5.2, §9.2.7).
//! - [`beat`]: a std thread that sends one Beat per second without waiting
//!   for any other thread (§9.2.3, E03-D003).

pub mod beat;
pub mod ctl;
pub mod frame;
pub mod reader;
