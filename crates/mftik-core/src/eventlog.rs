//! What the dispatcher writes to the strategy event log (PROTOCOL §12,
//! E07-D007).
//!
//! The file format and the writer thread are mftik-rt's
//! (`mftik_rt::eventlog`); this crate does no IO (E02-D001). [`Kind`] names
//! the frames, the constants are their flags, and the functions encode the
//! bodies that are not a message as received, or cut a field out of one
//! (§12.4). A [`Recorder`] is where [`crate::dispatch`] appends the frames of
//! the hooks it runs: each input before its hook, and a `hook_end` after
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

    /// Whether this kind is a hook's input. Its `t_ns` is hook_start_ns, and a
    /// `hook_end` whose ref is its `rec_seq` follows unless the hook never
    /// returned (PROTOCOL §12.6).
    pub fn is_input(self) -> bool {
        matches!(
            self,
            Self::INIT
                | Self::START
                | Self::MD_FEED
                | Self::MD_BOOK
                | Self::TD_EVENT
                | Self::TD_VIEW
                | Self::ORDER_REPLY
                | Self::READ_REPLY
                | Self::TIMER
                | Self::POST
                | Self::ONSTOP
        )
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

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.name() {
            Some(name) => f.write_str(name),
            None => write!(f, "kind_{}", self.0),
        }
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
/// The operation in an `order_call`'s flags, without [`FROM_BATCH`].
pub const OP_MASK: u16 = 0xff;
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

/// The protobuf message `msg` with every occurrence of field `number` left
/// out, as Go's `withoutField`: the `init` body is the InitPayload without
/// its NATS credentials (E07-D010). The walk stops at bytes it cannot parse,
/// a group included; these messages have none.
pub fn without_field(mut msg: &[u8], number: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(msg.len());
    while let Some((n, len, _)) = next_field(msg) {
        if n != number {
            out.extend_from_slice(&msg[..len]);
        }
        msg = &msg[len..];
    }
    out
}

/// The last occurrence of the length-delimited field `number` in `msg`, its
/// bytes as they are there (Go `fieldBytes`): a Command's `bindings`. `None`
/// when there is none, or `msg` does not parse.
pub fn field_bytes(mut msg: &[u8], number: u32) -> Option<&[u8]> {
    let mut found = None;
    while !msg.is_empty() {
        let (n, len, value) = next_field(msg)?;
        if n == number && value.is_some() {
            found = value;
        }
        msg = &msg[len..];
    }
    found
}

/// The first field of `msg`: its number, its length with the key, and the
/// value of a length-delimited field.
fn next_field(msg: &[u8]) -> Option<(u32, usize, Option<&[u8]>)> {
    const MAX_FIELD: u32 = (1 << 29) - 1;
    let (key, key_len) = varint(msg)?;
    let number = u32::try_from(key >> 3)
        .ok()
        .filter(|n| (1..=MAX_FIELD).contains(n))?;
    let rest = &msg[key_len..];
    let (len, value) = match key & 7 {
        0 => (varint(rest)?.1, None),
        1 => (8, None),
        2 => {
            let (n, len_len) = varint(rest)?;
            let end = len_len.checked_add(usize::try_from(n).ok()?)?;
            (end, Some(rest.get(len_len..end)?))
        }
        5 => (4, None),
        _ => return None,
    };
    (len <= rest.len()).then_some((number, key_len + len, value))
}

fn varint(b: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0;
    for (i, &byte) in b.iter().take(10).enumerate() {
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte < 0x80 {
            return Some((value, i + 1));
        }
    }
    None
}

/// Why a body did not parse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError {
    kind: &'static str,
}

impl ParseError {
    fn new(kind: &'static str) -> Self {
        Self { kind }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "eventlog: bad {} body", self.kind)
    }
}

impl std::error::Error for ParseError {}

/// An `order_call` body: the account, the td.view generation a Submit
/// recorded, and the client order ids the call covers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrderCall {
    pub api_id: i64,
    pub submit_gen: u64,
    pub cloids: Vec<String>,
}

/// The operation of an `order_call`'s flags.
pub fn op_name(flags: u16) -> String {
    match flags & OP_MASK {
        OP_SUBMIT => "submit".into(),
        OP_CANCEL => "cancel".into(),
        OP_BATCH_CANCEL => "batch_cancel".into(),
        OP_RECON => "recon".into(),
        n => format!("op_{n}"),
    }
}

/// The read of a `read_call`'s flags.
pub fn read_name(flags: u16) -> String {
    match flags {
        READ_TAPE => "tape".into(),
        READ_BOOK_SNAPSHOT => "book_snapshot".into(),
        READ_KLINES => "klines".into(),
        READ_OPEN_INTEREST => "open_interest".into(),
        n => format!("read_{n}"),
    }
}

/// Decodes an `order_call` body.
pub fn parse_order_call(body: &[u8]) -> Result<OrderCall, ParseError> {
    if body.len() < 18 {
        return Err(ParseError::new("order_call"));
    }
    let api_id = i64::from_le_bytes(body[0..8].try_into().expect("8 bytes"));
    let submit_gen = u64::from_le_bytes(body[8..16].try_into().expect("8 bytes"));
    let n = u16::from_le_bytes(body[16..18].try_into().expect("2 bytes")) as usize;
    let mut rest = &body[18..];
    let mut cloids = Vec::with_capacity(n);
    for _ in 0..n {
        let (id, next) = cut_str(rest).ok_or_else(|| ParseError::new("order_call"))?;
        cloids.push(String::from_utf8_lossy(id).into_owned());
        rest = next;
    }
    if !rest.is_empty() {
        return Err(ParseError::new("order_call"));
    }
    Ok(OrderCall {
        api_id,
        submit_gen,
        cloids,
    })
}

/// Decodes a `read_call` body: the subject, then the request as sent.
pub fn parse_read_call(body: &[u8]) -> Result<(&str, &[u8]), ParseError> {
    let (subject, rest) = cut_str(body).ok_or_else(|| ParseError::new("read_call"))?;
    let subject = std::str::from_utf8(subject).map_err(|_| ParseError::new("read_call"))?;
    Ok((subject, rest))
}

/// Decodes a `resolve` body: the ticker, then SymbolInfo or the error text.
pub fn parse_resolve(body: &[u8]) -> Result<(&str, &[u8]), ParseError> {
    let (ticker, rest) = cut_str(body).ok_or_else(|| ParseError::new("resolve"))?;
    let ticker = std::str::from_utf8(ticker).map_err(|_| ParseError::new("resolve"))?;
    Ok((ticker, rest))
}

/// Decodes an `init` body: the client order id counter, then the InitPayload.
pub fn parse_init(body: &[u8]) -> Result<(u64, &[u8]), ParseError> {
    if body.len() < 8 {
        return Err(ParseError::new("init"));
    }
    Ok((
        u64::from_le_bytes(body[..8].try_into().expect("8 bytes")),
        &body[8..],
    ))
}

/// Decodes a `td_view` body: the request's generation, then the ViewReply.
pub fn parse_td_view(body: &[u8]) -> Result<(u64, &[u8]), ParseError> {
    if body.len() < 8 {
        return Err(ParseError::new("td_view"));
    }
    Ok((
        u64::from_le_bytes(body[..8].try_into().expect("8 bytes")),
        &body[8..],
    ))
}

fn cut_str(b: &[u8]) -> Option<(&[u8], &[u8])> {
    let len = usize::from(u16::from_le_bytes(b.get(..2)?.try_into().ok()?));
    let text = b.get(2..2 + len)?;
    Some((text, &b[2 + len..]))
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
    use mftik_proto::wire::mftiko::common::v1::Version;
    use mftik_proto::wire::mftiko::stsctl::v1::{
        Bindings, Command, Fail, InitPayload, NatsCredentials, command,
    };
    use prost::Message;

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

    #[test]
    fn without_field_leaves_out_every_occurrence_and_keeps_the_rest_as_it_was() {
        let creds = NatsCredentials {
            user_jwt: "jwt".into(),
            seed: "seed".into(),
            inbox_prefix: "_INBOX_s1".into(),
        };
        let init = InitPayload {
            session_id: "s1".into(),
            incarnation: 2,
            params: vec![1, 2],
            nats: Some(creds.clone()),
            recv_window_ms: 500,
            hook_timeout_s: 1,
            ..Default::default()
        };
        // A second `nats` after the others, and a fixed64 and a fixed32
        // field this payload does not know.
        let mut msg = init.encode_to_vec();
        msg.extend_from_slice(&[(9 << 3) | 2, creds.encoded_len() as u8]);
        msg.extend_from_slice(&creds.encode_to_vec());
        msg.extend_from_slice(&[(13 << 3) | 1, 1, 0, 0, 0, 0, 0, 0, 0]);
        msg.extend_from_slice(&[(14 << 3) | 5, 1, 0, 0, 0]);

        let out = without_field(&msg, 9);
        let mut want = init.clone();
        want.nats = None;
        let mut known = want.encode_to_vec();
        known.extend_from_slice(&msg[msg.len() - 14..]);
        assert_eq!(out, known);
        for secret in ["jwt", "seed", "_INBOX_s1"] {
            assert!(!out.windows(secret.len()).any(|w| w == secret.as_bytes()));
        }
        // Bytes that do not parse end the walk: what came before is kept.
        let cut = without_field(&init.encode_to_vec()[..7], 9);
        assert_eq!(cut, [(1 << 3) | 2, 2, b's', b'1', 2 << 3, 2]);
    }

    #[test]
    fn field_bytes_is_the_last_occurrence_as_it_was_sent() {
        let first = Bindings {
            version: Some(Version { epoch: 1, seq: 1 }),
            ..Default::default()
        };
        let last = Bindings {
            version: Some(Version { epoch: 1, seq: 2 }),
            ..Default::default()
        };
        let cmd = Command {
            cmd: Some(command::Cmd::Bindings(last.clone())),
        };
        assert_eq!(
            field_bytes(&cmd.encode_to_vec(), 1),
            Some(&last.encode_to_vec()[..])
        );
        let mut twice = Command {
            cmd: Some(command::Cmd::Bindings(first)),
        }
        .encode_to_vec();
        twice.extend_from_slice(&cmd.encode_to_vec());
        assert_eq!(field_bytes(&twice, 1), Some(&last.encode_to_vec()[..]));
        let fail = Command {
            cmd: Some(command::Cmd::Fail(Fail { reason: "r".into() })),
        };
        assert_eq!(field_bytes(&fail.encode_to_vec(), 1), None);
        assert_eq!(field_bytes(&twice[..twice.len() - 1], 1), None);
        assert_eq!(field_bytes(&[], 1), None);
    }

    #[test]
    fn parse_round_trips_the_bodies_a_replay_reads() {
        let mut body = Vec::new();
        order_call(&mut body, 7, 2, &["a1", "b"]);
        let call = parse_order_call(&body).expect("order_call");
        assert_eq!(call.api_id, 7);
        assert_eq!(call.submit_gen, 2);
        assert_eq!(call.cloids, ["a1", "b"]);
        assert!(parse_order_call(&body[..10]).is_err());

        let mut read = Vec::new();
        put_str(&mut read, b"md.v1.view.x");
        read.extend_from_slice(&[1, 2, 3]);
        let (subject, req) = parse_read_call(&read).expect("read_call");
        assert_eq!(subject, "md.v1.view.x");
        assert_eq!(req, [1, 2, 3]);

        let mut resolve = Vec::new();
        put_str(&mut resolve, b"BTC");
        resolve.extend_from_slice(&[9]);
        let (ticker, rest) = parse_resolve(&resolve).expect("resolve");
        assert_eq!(ticker, "BTC");
        assert_eq!(rest, [9]);

        let mut init = init_head(3).to_vec();
        init.extend_from_slice(&[4, 5]);
        let (slot, payload) = parse_init(&init).expect("init");
        assert_eq!(slot, 3);
        assert_eq!(payload, [4, 5]);
        assert!(parse_init(&[1, 2, 3]).is_err());

        let mut view = td_view_head(11).to_vec();
        view.extend_from_slice(&[8]);
        let (generation, reply) = parse_td_view(&view).expect("td_view");
        assert_eq!(generation, 11);
        assert_eq!(reply, [8]);

        assert!(Kind::INIT.is_input());
        assert!(Kind::ONSTOP.is_input());
        assert!(!Kind::BINDINGS.is_input());
        assert!(!Kind::RESOLVE.is_input());
        assert_eq!(Kind::TIMER.to_string(), "timer");
        assert_eq!(op_name(OP_SUBMIT | FROM_BATCH), "submit");
        assert_eq!(read_name(READ_TAPE), "tape");
    }
}
