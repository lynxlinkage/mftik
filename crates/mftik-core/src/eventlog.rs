//! What the dispatcher writes to the strategy event log (PROTOCOL §12,
//! E07-D007).
//!
//! The file format and the writer thread are mftik-rt's
//! (`mftik_rt::eventlog`); this crate does no IO (E02-D001). [`Kind`] names
//! the frames, the constants are their flags, and the functions encode the
//! bodies that are not a message as received (§12.4). A [`Recorder`] is where
//! [`crate::dispatch`] appends the frames of the hooks it runs: each input
//! before its hook, and a `hook_end` after (§12.5).

/// What a frame holds (§12.4). Numbers are only added; a reader skips a kind
/// it does not know.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Kind(pub u16);

impl Kind {
    pub const INIT: Kind = Kind(1);
    pub const START: Kind = Kind(2);
    pub const BINDINGS: Kind = Kind(3);
    pub const RESOLVE: Kind = Kind(4);
    pub const MD_FEED: Kind = Kind(5);
    pub const MD_BOOK: Kind = Kind(6);
    pub const TD_EVENT: Kind = Kind(7);
    pub const TD_VIEW: Kind = Kind(8);
    pub const ORDER_REPLY: Kind = Kind(9);
    pub const READ_REPLY: Kind = Kind(10);
    pub const TIMER: Kind = Kind(11);
    pub const POST: Kind = Kind(12);
    pub const STOP: Kind = Kind(13);
    pub const ONSTOP: Kind = Kind(14);
    pub const HOOK_END: Kind = Kind(15);
    pub const ORDER_CALL: Kind = Kind(16);
    pub const ORDER_PUBLISH: Kind = Kind(17);
    pub const READ_CALL: Kind = Kind(18);
    pub const END: Kind = Kind(19);

    /// Whether this reader knows the kind.
    pub fn known(self) -> bool {
        (Self::INIT.0..=Self::END.0).contains(&self.0)
    }

    /// The kind's name in `dump`, or `None` for a kind this reader does not know.
    pub fn name(self) -> Option<&'static str> {
        const NAMES: [&str; 19] = [
            "init",
            "start",
            "bindings",
            "resolve",
            "md_feed",
            "md_book",
            "td_event",
            "td_view",
            "order_reply",
            "read_reply",
            "timer",
            "post",
            "stop",
            "onstop",
            "hook_end",
            "order_call",
            "order_publish",
            "read_call",
            "end",
        ];
        self.known().then(|| NAMES[usize::from(self.0) - 1])
    }
}

/// `hook_end` flags: the hook panicked, and the body is the panic's text.
pub const END_PANIC: u16 = 1;
/// `hook_end` flags: the hook returned an error, and the body is its text.
pub const END_ERROR: u16 = 2;

/// `bindings` flags: the snapshot came in a Command, and the body is the
/// Command's `bindings` field. Without it the body is the Live payload.
pub const FROM_COMMAND: u16 = 1;

/// `stop` flags: td.evt was dropped and the TD buffer is delivered before
/// OnStop (DONE and FAILED). Without it the ending delivers nothing.
pub const STOP_DELIVER: u16 = 1;
/// `stop` flags: the watchdog. A hook passed `hook_timeout_s`, and that hook
/// has no `hook_end`.
pub const STOP_HUNG: u16 = 2;

/// `order_call` flags: the operation is in the low byte, and each round of a
/// BatchCancel adds [`FROM_BATCH`].
pub const OP_SUBMIT: u16 = 1;
pub const OP_CANCEL: u16 = 2;
pub const OP_BATCH_CANCEL: u16 = 3;
pub const OP_RECON: u16 = 4;
pub const FROM_BATCH: u16 = 0x100;

/// `read_call` flags: which read.
pub const READ_TAPE: u16 = 1;
pub const READ_BOOK_SNAPSHOT: u16 = 2;
pub const READ_KLINES: u16 = 3;
pub const READ_OPEN_INTEREST: u16 = 4;

/// The flags of `resolve`, `order_reply` and `read_reply`: the outcome. [`OK`]
/// carries the reply; every other outcome carries the error's text.
pub mod outcome {
    pub const OK: u16 = 0;
    pub const NO_RESPONDERS: u16 = 1;
    pub const TIMEOUT: u16 = 2;
    pub const CANCELED: u16 = 3;
    pub const FAILED: u16 = 4;
    pub const NOT_IN_INIT: u16 = 5;
    pub const NOT_FOUND: u16 = 6;
    pub const STOPPING: u16 = 7;
}

/// Appends `bytes` as a §12 string: a u16 length, then the bytes, cut at
/// 65535.
pub fn put_str(dst: &mut Vec<u8>, bytes: &[u8]) {
    let bytes = &bytes[..bytes.len().min(usize::from(u16::MAX))];
    dst.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    dst.extend_from_slice(bytes);
}

/// Appends an `order_call` body: the account, the td.view generation a
/// Submit recorded (0 for the other calls), and the client order ids the call
/// covers. RequestRecon lists none.
pub fn order_call<S: AsRef<str>>(dst: &mut Vec<u8>, api_id: i64, submit_gen: u64, cloids: &[S]) {
    dst.extend_from_slice(&api_id.to_le_bytes());
    dst.extend_from_slice(&submit_gen.to_le_bytes());
    dst.extend_from_slice(&(cloids.len() as u16).to_le_bytes());
    for id in cloids {
        put_str(dst, id.as_ref().as_bytes());
    }
}

/// The bytes an `init` body has before the InitPayload: the client order id
/// counter as Init found it.
pub fn init_head(slot: u64) -> [u8; 8] {
    slot.to_le_bytes()
}

/// The bytes a `td_view` body has before the ViewReply: the request's
/// generation.
pub fn td_view_head(generation: u64) -> [u8; 8] {
    generation.to_le_bytes()
}

/// Where the dispatcher appends frames: the session's event log writer.
pub trait Recorder: Send + Sync {
    /// Unix nanoseconds. Every time the dispatcher puts in a frame is read here.
    fn now_ns(&self) -> i64;

    /// Adds one frame whose body is `a` followed by `b` and returns its
    /// `rec_seq`, which is never 0. It must not wait for the disk.
    #[allow(clippy::too_many_arguments)]
    fn append(
        &self,
        kind: Kind,
        flags: u16,
        recv_ns: i64,
        t_ns: i64,
        r#ref: u64,
        a: &[u8],
        b: &[u8],
    ) -> u64;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_order_call_body_is_the_account_the_generation_and_the_ids() {
        let mut body = Vec::new();
        order_call(&mut body, 7, 2, &["a1", "b"]);
        assert_eq!(
            body,
            [
                &7i64.to_le_bytes()[..],
                &2u64.to_le_bytes(),
                &[2, 0],
                &[2, 0, b'a', b'1'],
                &[1, 0, b'b'],
            ]
            .concat()
        );
        body.clear();
        order_call::<&str>(&mut body, 7, 0, &[]);
        assert_eq!(body[8..], [0; 10], "a recon: no generation, no ids");
    }

    #[test]
    fn a_string_is_cut_at_its_length_field() {
        let mut out = Vec::new();
        put_str(&mut out, &[b'x'; 0x1_0001]);
        assert_eq!(out.len(), 2 + 0xffff);
        assert_eq!(out[..2], [0xff, 0xff]);
    }
}
