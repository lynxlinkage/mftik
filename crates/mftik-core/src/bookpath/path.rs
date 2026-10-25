//! Snapshot sync, backoff, and the pending buffer.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use mftik_proto::wire::mftiko::md::v1::{self as md, feed_event};
use prost::Message;

use crate::book::{DEFAULT_N, Level, OrderBook};
use crate::dispatch::envelope;

/// Pending deltas kept while a snapshot is outstanding (E04-D024, E08-D058).
pub const BUFFER_MAX: usize = 10_000;

/// Age of one buffer attempt, from its first delta, inclusive.
pub const BUFFER_AGE: Duration = Duration::from_secs(10);

/// First wait after a snapshot read fails. It doubles until [`BACKOFF_MAX`].
pub const BACKOFF_MIN: Duration = Duration::from_secs(1);

/// Cap on the snapshot retry. 16s doubled is 32s, which lands on this cap.
pub const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// What [`Path::open`] needs. There is no clock and no callback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// `InitPayload.recv_window_ms`. Zero is rejected. The same value is the
    /// `md.view` wait on [`Effect::RequestSnapshot`].
    pub recv_window: Duration,
}

/// Why [`Path::open`] refused the config.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenError {
    /// `recv_window` is zero.
    RecvWindow,
}

impl fmt::Display for OpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecvWindow => formatter.write_str("bookpath: recv window must be positive"),
        }
    }
}

impl std::error::Error for OpenError {}

/// One `orderbook_delta` after the envelope and payload have been decoded.
///
/// A size of zero deletes that price. `reset` drops the local book and does
/// not apply `bids` or `asks`.
#[derive(Clone, Debug, PartialEq)]
pub struct Delta {
    pub uid: u32,
    pub conn_epoch: u64,
    pub seq: u64,
    pub md_ts_ns: i64,
    pub exch_ts_ns: i64,
    pub reset: bool,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// `md.view`'s `BookSnapshot`, in book levels (`qty` is [`Level::size`]).
///
/// `uid` of zero means the body did not name an instrument. Any other uid
/// must be the uid that was requested.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    pub uid: u32,
    pub conn_epoch: u64,
    pub seq: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// How an in-flight `md.view` ended.
#[derive(Clone, Debug, PartialEq)]
pub enum SnapshotReply {
    /// A snapshot body. The cursor is the book's cursor before folding.
    Book(Snapshot),
    /// The reply had no book and no error string.
    Empty,
    /// `not_synced`, timeout, no responder, or any other failed read.
    /// An empty string is treated as [`SnapshotReply::Empty`].
    Failed(String),
}

/// One delta applied to a synced book and still inside `recv_window`.
///
/// Deltas that waited in the buffer are not updates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Update {
    pub uid: u32,
    pub conn_epoch: u64,
    pub seq: u64,
    pub md_ts_ns: i64,
    pub exch_ts_ns: i64,
}

/// Which armed timer fired.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimerKind {
    /// Drop the buffer when it has been open for [`BUFFER_AGE`].
    BufferAge,
    /// End one snapshot backoff and read again.
    Backoff,
}

/// Identity of one [`Effect::ArmTimer`]. Pass it back to [`Path::on_timer`].
///
/// `generation` is the staleness token (`buf_gen` or the snapshot generation).
/// `id` distinguishes two arms that share a generation. Only the latest arm
/// of that kind is live.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimerToken {
    pub uid: u32,
    pub kind: TimerKind,
    pub generation: u64,
    pub id: u64,
}

/// Something E03 must do. Hooks see state that already includes the event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Ask `md.view` for this instrument's book. `wait` is the recv window.
    /// Deliver the reply with [`Path::on_snapshot`] and this `generation`.
    RequestSnapshot {
        uid: u32,
        generation: u64,
        wait: Duration,
    },
    /// Fire [`Path::on_timer`] at `deadline_ns` on the injected clock.
    /// A buffer-age timer that arrives early is ignored and stays armed.
    ArmTimer {
        token: TimerToken,
        after: Duration,
        deadline_ns: i64,
    },
    /// Strategy `on_book`. [`Path::view`] already contains this delta.
    Hook(Update),
    /// One log line. The text matches the Go SDK, except a payload decode,
    /// which uses the prost error.
    Log(String),
}

/// One session's local books.
///
/// [`Path::view`] is `None` until a snapshot is installed and the buffer that
/// followed it was continuous. After [`Path::close`] it stays `None`.
#[derive(Debug)]
pub struct Path {
    recv: Duration,
    now_ns: i64,
    ticks: HashMap<u32, f64>,
    by: HashMap<u32, Inst>,
    told_zero: bool,
    closed: bool,
    next_timer: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Told {
    None,
    Unresolved,
    Tick,
}

#[derive(Debug)]
struct Inst {
    ob: Option<OrderBook>,
    synced: bool,
    epoch: u64,
    seq: u64,
    buf: Vec<Pending>,
    buf_at: i64,
    have: bool,
    buf_gen: u64,
    floor_epoch: u64,
    floor_seq: u64,
    have_floor: bool,
    generation: u64,
    inflight: bool,
    wait: Duration,
    backing: bool,
    told: Told,
    age_id: u64,
    backoff_id: u64,
}

#[derive(Debug)]
struct Pending {
    epoch: u64,
    seq: u64,
    md_ts: i64,
    exch_ts: i64,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

impl Inst {
    fn new() -> Self {
        Self {
            ob: None,
            synced: false,
            epoch: 0,
            seq: 0,
            buf: Vec::new(),
            buf_at: 0,
            have: false,
            buf_gen: 0,
            floor_epoch: 0,
            floor_seq: 0,
            have_floor: false,
            generation: 0,
            inflight: false,
            wait: Duration::ZERO,
            backing: false,
            told: Told::None,
            age_id: 0,
            backoff_id: 0,
        }
    }
}

impl Path {
    /// Empty path. The first delta for an instrument asks for the snapshot.
    pub fn open(config: Config) -> Result<Self, OpenError> {
        if config.recv_window.is_zero() {
            return Err(OpenError::RecvWindow);
        }
        Ok(Self {
            recv: config.recv_window,
            now_ns: 0,
            ticks: HashMap::new(),
            by: HashMap::new(),
            told_zero: false,
            closed: false,
            next_timer: 0,
        })
    }

    /// `InitPayload.recv_window_ms` stored at [`Path::open`].
    pub fn recv_window(&self) -> Duration {
        self.recv
    }

    /// Remember SYM `price_tick` for `uid`. Replaces a previous value.
    ///
    /// A non-finite or non-positive tick is stored and later refused. This
    /// does not call SYM.
    pub fn resolve(&mut self, uid: u32, price_tick: f64) {
        self.ticks.insert(uid, price_tick);
    }

    /// Drop a resolved tick. The book stays until a later event unsyncs it.
    pub fn forget(&mut self, uid: u32) {
        self.ticks.remove(&uid);
    }

    /// The live ring while the instrument is synced. `None` after [`Path::close`].
    ///
    /// The borrow lasts until the next `&mut self` call.
    pub fn view(&self, uid: u32) -> Option<&OrderBook> {
        if self.closed {
            return None;
        }
        let inst = self.by.get(&uid)?;
        if !inst.synced {
            return None;
        }
        inst.ob.as_ref()
    }

    /// Later events, replies, and timers do nothing. `view` stays `None`.
    ///
    /// An in-flight `md.view` is abandoned. Its reply is ignored.
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// One decoded delta. `now_ns` is the injected clock for this call.
    pub fn on_delta(&mut self, now_ns: i64, delta: Delta) -> Vec<Effect> {
        if !self.begin(now_ns) {
            return Vec::new();
        }
        let mut out = Vec::new();
        self.ingest_delta(delta, &mut out);
        out
    }

    /// One `md.f` message, the bytes [`crate::dispatch::Dispatcher::push_book`]
    /// hands to the book callback.
    ///
    /// A bad envelope or payload is logged and does not move a cursor. The
    /// next decoded delta for that instrument then sees a gap.
    pub fn on_message(&mut self, now_ns: i64, msg: &[u8]) -> Vec<Effect> {
        if !self.begin(now_ns) {
            return Vec::new();
        }
        let mut out = Vec::new();
        let header = match envelope::parse(msg) {
            Ok(header) => header,
            Err(err) => {
                out.push(Effect::Log(format!("book: envelope: {err}")));
                return out;
            }
        };
        let md_ts_ns = header.md_ts_ns;
        let conn_epoch = header.conn_epoch;
        let seq = header.seq;
        let event = match md::FeedEvent::decode(header.payload) {
            Ok(event) => event,
            Err(err) => {
                out.push(Effect::Log(format!("book: payload: {err}")));
                return out;
            }
        };
        match event.event {
            Some(feed_event::Event::OrderBookDelta(delta)) => {
                self.ingest_delta(
                    Delta {
                        uid: event.uid,
                        conn_epoch,
                        seq,
                        md_ts_ns,
                        exch_ts_ns: delta.meta.as_ref().map(|meta| meta.exch_ts_ns).unwrap_or(0),
                        reset: delta.reset,
                        bids: to_levels(&delta.bids),
                        asks: to_levels(&delta.asks),
                    },
                    &mut out,
                );
            }
            _ => self.ingest_non_delta(event.uid, conn_epoch, seq, &mut out),
        }
        out
    }

    /// A feed event that is not `orderbook_delta`.
    ///
    /// The book is marked unsynced and a snapshot is requested immediately,
    /// without waiting out a backoff.
    pub fn on_non_delta(
        &mut self,
        now_ns: i64,
        uid: u32,
        conn_epoch: u64,
        seq: u64,
    ) -> Vec<Effect> {
        if !self.begin(now_ns) {
            return Vec::new();
        }
        let mut out = Vec::new();
        self.ingest_non_delta(uid, conn_epoch, seq, &mut out);
        out
    }

    /// Reply to [`Effect::RequestSnapshot`]. A stale `generation`, or a reply
    /// that arrives when nothing is in flight, is ignored.
    pub fn on_snapshot(
        &mut self,
        now_ns: i64,
        uid: u32,
        generation: u64,
        reply: SnapshotReply,
    ) -> Vec<Effect> {
        if !self.begin(now_ns) {
            return Vec::new();
        }
        let mut out = Vec::new();
        let accept = matches!(
            self.by.get(&uid),
            Some(inst) if inst.generation == generation && inst.inflight
        );
        if !accept {
            return out;
        }
        if let Some(inst) = self.by.get_mut(&uid) {
            inst.inflight = false;
        }
        let snap = match reply {
            SnapshotReply::Book(snap) => snap,
            SnapshotReply::Empty => {
                self.fail(uid, "bookpath: empty snapshot", &mut out);
                return out;
            }
            SnapshotReply::Failed(message) => {
                if message.is_empty() {
                    self.fail(uid, "bookpath: empty snapshot", &mut out);
                } else {
                    self.fail(uid, &message, &mut out);
                }
                return out;
            }
        };
        if snap.uid != 0 && snap.uid != uid {
            let message = format!("bookpath: snapshot uid {}", snap.uid);
            self.fail(uid, &message, &mut out);
            return out;
        }
        let behind = match self.by.get(&uid) {
            Some(inst) => {
                inst.have_floor
                    && !at_least(snap.conn_epoch, snap.seq, inst.floor_epoch, inst.floor_seq)
            }
            None => true,
        };
        if behind {
            self.fail(uid, "bookpath: snapshot is behind the feed", &mut out);
            return out;
        }
        // A tick that disappeared while the read was in flight drops the
        // reply. There is no backoff; the next delta asks again once the
        // tick is usable.
        let Some(tick) = self.gate(uid, &mut out) else {
            return out;
        };
        let mut ob = match OrderBook::new(DEFAULT_N, tick) {
            Ok(ob) => ob,
            Err(err) => {
                let message = err.to_string();
                self.fail(uid, &message, &mut out);
                return out;
            }
        };
        ob.apply(&snap.bids, &snap.asks);
        let Some((epoch, seq)) = self.fold(uid, &mut ob, snap.conn_epoch, snap.seq) else {
            self.fail(uid, "bookpath: snapshot is behind the feed", &mut out);
            return out;
        };
        if let Some(inst) = self.by.get_mut(&uid) {
            inst.ob = Some(ob);
            inst.synced = true;
            inst.epoch = epoch;
            inst.seq = seq;
            inst.have_floor = false;
            inst.wait = Duration::ZERO;
            inst.told = Told::None;
        }
        out
    }

    /// A timer armed by [`Effect::ArmTimer`]. The host fires each token once.
    pub fn on_timer(&mut self, now_ns: i64, token: TimerToken) -> Vec<Effect> {
        if !self.begin(now_ns) {
            return Vec::new();
        }
        let mut out = Vec::new();
        match token.kind {
            TimerKind::BufferAge => self.on_buffer_age(token, &mut out),
            TimerKind::Backoff => self.on_backoff(token, &mut out),
        }
        out
    }

    fn begin(&mut self, now_ns: i64) -> bool {
        self.now_ns = now_ns;
        !self.closed
    }

    fn ingest_delta(&mut self, delta: Delta, out: &mut Vec<Effect>) {
        if self.note_zero(delta.uid, out) {
            return;
        }
        let uid = delta.uid;
        self.ensure(uid);
        if self.gate(uid, out).is_none() {
            return;
        }
        if delta.reset {
            self.on_reset(uid, delta.conn_epoch, delta.seq, out);
            return;
        }
        self.on_pending(
            uid,
            Pending {
                epoch: delta.conn_epoch,
                seq: delta.seq,
                md_ts: delta.md_ts_ns,
                exch_ts: delta.exch_ts_ns,
                bids: delta.bids,
                asks: delta.asks,
            },
            out,
        );
    }

    fn ingest_non_delta(&mut self, uid: u32, conn_epoch: u64, seq: u64, out: &mut Vec<Effect>) {
        if self.note_zero(uid, out) {
            return;
        }
        self.ensure(uid);
        if self.gate(uid, out).is_none() {
            return;
        }
        self.lose(uid, conn_epoch, seq, None, out);
    }

    fn note_zero(&mut self, uid: u32, out: &mut Vec<Effect>) -> bool {
        if uid != 0 {
            return false;
        }
        if !self.told_zero {
            self.told_zero = true;
            out.push(Effect::Log("book: uid 0".to_owned()));
        }
        true
    }

    fn on_pending(&mut self, uid: u32, item: Pending, out: &mut Vec<Effect>) {
        let synced = self.by.get(&uid).is_some_and(|inst| inst.synced);
        if !synced {
            if !self.buffer(uid, item, out) {
                self.fetch(uid, out);
            }
            return;
        }
        let Some((epoch, seq)) = self.by.get(&uid).map(|inst| (inst.epoch, inst.seq)) else {
            return;
        };
        if !newer(item.epoch, item.seq, epoch, seq) {
            return;
        }
        if item.epoch != epoch || item.seq != seq.wrapping_add(1) {
            self.lose(uid, item.epoch, item.seq, Some(item), out);
            return;
        }
        let applied = self.by.get_mut(&uid).is_some_and(|inst| {
            let Some(ob) = inst.ob.as_mut() else {
                return false;
            };
            ob.apply(&item.bids, &item.asks);
            inst.epoch = item.epoch;
            inst.seq = item.seq;
            true
        });
        if !applied {
            self.lose(uid, item.epoch, item.seq, Some(item), out);
            return;
        }
        self.emit(uid, &item, out);
    }

    fn on_reset(&mut self, uid: u32, epoch: u64, seq: u64, out: &mut Vec<Effect>) {
        let stale = self
            .by
            .get(&uid)
            .is_some_and(|inst| reset_is_stale(inst, epoch, seq));
        if stale {
            return;
        }
        if let Some(inst) = self.by.get_mut(&uid) {
            inst.synced = false;
            inst.ob = None;
            inst.buf.clear();
            inst.have = false;
            inst.buf_gen = inst.buf_gen.wrapping_add(1);
            inst.age_id = 0;
        }
        self.raise_floor(uid, epoch, seq);
        self.invalidate(uid);
        self.fetch(uid, out);
    }

    /// `item` is the delta that broke the cursor. `None` means a non-delta:
    /// the buffer already held is kept, and the snapshot still has to reach
    /// this cursor.
    fn lose(
        &mut self,
        uid: u32,
        epoch: u64,
        seq: u64,
        item: Option<Pending>,
        out: &mut Vec<Effect>,
    ) {
        if let Some(inst) = self.by.get_mut(&uid) {
            inst.synced = false;
            inst.ob = None;
        }
        self.raise_floor(uid, epoch, seq);
        self.invalidate(uid);
        if let Some(item) = item
            && self.buffer(uid, item, out)
        {
            return;
        }
        self.fetch(uid, out);
    }

    fn emit(&self, uid: u32, item: &Pending, out: &mut Vec<Effect>) {
        if !within(self.now_ns, item.md_ts, self.recv) {
            return;
        }
        out.push(Effect::Hook(Update {
            uid,
            conn_epoch: item.epoch,
            seq: item.seq,
            md_ts_ns: item.md_ts,
            exch_ts_ns: item.exch_ts,
        }));
    }

    /// Keep one unsynced delta. `true` means the cap already started a new
    /// snapshot, so the caller must not start a second one.
    fn buffer(&mut self, uid: u32, item: Pending, out: &mut Vec<Effect>) -> bool {
        let generation = match self.by.get_mut(&uid) {
            Some(inst) if !inst.have => {
                inst.buf_gen = inst.buf_gen.wrapping_add(1);
                inst.buf_at = self.now_ns;
                inst.have = true;
                Some(inst.buf_gen)
            }
            Some(_) => None,
            None => return false,
        };
        if let Some(generation) = generation {
            let token = self.mint_timer(uid, TimerKind::BufferAge, generation);
            out.push(self.arm_effect(token, BUFFER_AGE));
        }
        if let Some(inst) = self.by.get_mut(&uid) {
            inst.buf.push(item);
        }
        if !self.over(uid) {
            return false;
        }
        self.drop_buf(uid);
        self.invalidate(uid);
        self.fetch(uid, out);
        true
    }

    fn over(&self, uid: u32) -> bool {
        let Some(inst) = self.by.get(&uid) else {
            return false;
        };
        inst.buf.len() >= BUFFER_MAX
            || (inst.have && age_reached(self.now_ns, inst.buf_at, BUFFER_AGE))
    }

    fn drop_buf(&mut self, uid: u32) {
        let floor = self
            .by
            .get(&uid)
            .and_then(|inst| max_pending(&inst.buf).map(|item| (item.epoch, item.seq)));
        if let Some(inst) = self.by.get_mut(&uid) {
            inst.buf.clear();
            inst.have = false;
            inst.buf_gen = inst.buf_gen.wrapping_add(1);
            inst.age_id = 0;
        }
        if let Some((epoch, seq)) = floor {
            self.raise_floor(uid, epoch, seq);
        }
    }

    fn on_buffer_age(&mut self, token: TimerToken, out: &mut Vec<Effect>) {
        let ready = matches!(
            self.by.get(&token.uid),
            Some(inst)
                if inst.age_id == token.id
                    && inst.buf_gen == token.generation
                    && inst.have
                    && age_reached(self.now_ns, inst.buf_at, BUFFER_AGE)
        );
        if !ready {
            return;
        }
        self.drop_buf(token.uid);
        self.invalidate(token.uid);
        self.fetch(token.uid, out);
    }

    fn fetch(&mut self, uid: u32, out: &mut Vec<Effect>) {
        if self.closed {
            return;
        }
        if let Some(inst) = self.by.get(&uid) {
            if inst.backing || inst.inflight {
                return;
            }
        } else {
            return;
        }
        if self.gate(uid, out).is_none() {
            return;
        }
        let Some(generation) = self.by.get(&uid).map(|inst| inst.generation) else {
            return;
        };
        if let Some(inst) = self.by.get_mut(&uid) {
            inst.inflight = true;
        }
        out.push(Effect::RequestSnapshot {
            uid,
            generation,
            wait: self.recv,
        });
    }

    fn fail(&mut self, uid: u32, message: &str, out: &mut Vec<Effect>) {
        out.push(Effect::Log(format!("book: uid {uid}: {message}")));
        self.backoff(uid, out);
    }

    fn backoff(&mut self, uid: u32, out: &mut Vec<Effect>) {
        if self.closed {
            return;
        }
        let Some(inst) = self.by.get(&uid) else {
            return;
        };
        if inst.backing {
            return;
        }
        let wait = grow(inst.wait);
        let generation = inst.generation;
        if let Some(inst) = self.by.get_mut(&uid) {
            inst.wait = wait;
            inst.backing = true;
        }
        let token = self.mint_timer(uid, TimerKind::Backoff, generation);
        out.push(self.arm_effect(token, wait));
    }

    fn on_backoff(&mut self, token: TimerToken, out: &mut Vec<Effect>) {
        let accept = match self.by.get(&token.uid) {
            Some(inst) => inst.backoff_id == token.id && inst.generation == token.generation,
            None => false,
        };
        if !accept {
            return;
        }
        if let Some(inst) = self.by.get_mut(&token.uid) {
            inst.backing = false;
            inst.backoff_id = 0;
        }
        self.fetch(token.uid, out);
    }

    fn invalidate(&mut self, uid: u32) {
        let Some(inst) = self.by.get_mut(&uid) else {
            return;
        };
        inst.generation = inst.generation.wrapping_add(1);
        inst.inflight = false;
        inst.backing = false;
    }

    fn raise_floor(&mut self, uid: u32, epoch: u64, seq: u64) {
        let Some(inst) = self.by.get_mut(&uid) else {
            return;
        };
        if inst.have_floor && !newer(epoch, seq, inst.floor_epoch, inst.floor_seq) {
            return;
        }
        inst.have_floor = true;
        inst.floor_epoch = epoch;
        inst.floor_seq = seq;
    }

    /// Apply buffered deltas that continue from the snapshot.
    ///
    /// A hole keeps the tail, raises the floor to that delta, and leaves the
    /// book uninstalled. Deltas at or below the snapshot cursor are skipped.
    fn fold(
        &mut self,
        uid: u32,
        ob: &mut OrderBook,
        mut epoch: u64,
        mut seq: u64,
    ) -> Option<(u64, u64)> {
        let mut buf = {
            let inst = self.by.get_mut(&uid)?;
            std::mem::take(&mut inst.buf)
        };
        let mut index = 0;
        while index < buf.len() {
            if !newer(buf[index].epoch, buf[index].seq, epoch, seq) {
                index += 1;
                continue;
            }
            if buf[index].epoch != epoch || buf[index].seq != seq.wrapping_add(1) {
                let hole_epoch = buf[index].epoch;
                let hole_seq = buf[index].seq;
                let tail = buf.split_off(index);
                if let Some(inst) = self.by.get_mut(&uid) {
                    inst.buf = tail;
                }
                self.raise_floor(uid, hole_epoch, hole_seq);
                return None;
            }
            ob.apply(&buf[index].bids, &buf[index].asks);
            epoch = buf[index].epoch;
            seq = buf[index].seq;
            index += 1;
        }
        if let Some(inst) = self.by.get_mut(&uid) {
            inst.have = false;
        }
        Some((epoch, seq))
    }

    /// Tick size from the host's resolved table. A missing uid, or a tick that
    /// cannot build a ring, stays unsynced and is logged once.
    fn gate(&mut self, uid: u32, out: &mut Vec<Effect>) -> Option<f64> {
        let tick = self.ticks.get(&uid).copied();
        let inst = self.by.get_mut(&uid)?;
        match tick {
            Some(tick) if tick.is_finite() && tick > 0.0 => Some(tick),
            Some(_) => {
                if inst.told != Told::Tick {
                    inst.told = Told::Tick;
                    out.push(Effect::Log(format!("book: uid {uid} has no price tick")));
                }
                None
            }
            None => {
                if inst.told != Told::Unresolved {
                    inst.told = Told::Unresolved;
                    out.push(Effect::Log(format!("book: uid {uid} is not resolved")));
                }
                None
            }
        }
    }

    fn ensure(&mut self, uid: u32) {
        self.by.entry(uid).or_insert_with(Inst::new);
    }

    fn mint_timer(&mut self, uid: u32, kind: TimerKind, generation: u64) -> TimerToken {
        self.next_timer = self.next_timer.wrapping_add(1);
        let id = self.next_timer;
        if let Some(inst) = self.by.get_mut(&uid) {
            match kind {
                TimerKind::BufferAge => inst.age_id = id,
                TimerKind::Backoff => inst.backoff_id = id,
            }
        }
        TimerToken {
            uid,
            kind,
            generation,
            id,
        }
    }

    fn arm_effect(&self, token: TimerToken, after: Duration) -> Effect {
        Effect::ArmTimer {
            token,
            after,
            deadline_ns: deadline_ns(self.now_ns, after),
        }
    }
}

fn reset_is_stale(inst: &Inst, epoch: u64, seq: u64) -> bool {
    if inst.synced {
        return !newer(epoch, seq, inst.epoch, inst.seq);
    }
    if inst.have_floor && !newer(epoch, seq, inst.floor_epoch, inst.floor_seq) {
        return true;
    }
    max_pending(&inst.buf).is_some_and(|last| !newer(epoch, seq, last.epoch, last.seq))
}

fn to_levels(levels: &[md::Level]) -> Vec<Level> {
    levels
        .iter()
        .map(|level| Level {
            price: level.price,
            size: level.qty,
        })
        .collect()
}

fn max_pending(buf: &[Pending]) -> Option<&Pending> {
    let mut best = buf.first()?;
    for item in &buf[1..] {
        if newer(item.epoch, item.seq, best.epoch, best.seq) {
            best = item;
        }
    }
    Some(best)
}

fn newer(epoch: u64, seq: u64, floor_epoch: u64, floor_seq: u64) -> bool {
    epoch > floor_epoch || (epoch == floor_epoch && seq > floor_seq)
}

fn at_least(epoch: u64, seq: u64, floor_epoch: u64, floor_seq: u64) -> bool {
    epoch > floor_epoch || (epoch == floor_epoch && seq >= floor_seq)
}

fn grow(wait: Duration) -> Duration {
    if wait.is_zero() {
        return BACKOFF_MIN;
    }
    match wait.checked_mul(2) {
        Some(doubled) if doubled <= BACKOFF_MAX => doubled,
        _ => BACKOFF_MAX,
    }
}

fn within(now: i64, at: i64, window: Duration) -> bool {
    i128::from(now) - i128::from(at) <= duration_ns(window)
}

fn age_reached(now: i64, at: i64, window: Duration) -> bool {
    i128::from(now) - i128::from(at) >= duration_ns(window)
}

fn duration_ns(duration: Duration) -> i128 {
    i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX)
}

fn deadline_ns(now: i64, after: Duration) -> i64 {
    let sum = i128::from(now) + duration_ns(after);
    if sum >= i128::from(i64::MAX) {
        i64::MAX
    } else if sum <= i128::from(i64::MIN) {
        i64::MIN
    } else {
        i64::try_from(sum).expect("sum is inside i64")
    }
}
