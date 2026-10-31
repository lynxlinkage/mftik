//! What the dispatcher writes to the strategy event log (PROTOCOL §12,
//! E07-D007).
//!
//! The file format and the writer thread are mftik-rt's
//! (`mftik_rt::eventlog`); this crate does no IO (E02-D001). [`Kind`] names
//! the frames. A [`Recorder`] is where [`crate::dispatch`] appends the frames
//! of the hooks it runs: each input before its hook, and a `hook_end` after
//! (§12.5).

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
