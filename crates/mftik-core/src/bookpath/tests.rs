//! Local book sync, driven by an injected clock. Nothing sleeps.
//!
//! Values are fixed and small. The cases follow PROTOCOL §9.5: snapshot,
//! gap, epoch, reset, backoff, and the 10000 / 10s buffer.

use std::time::Duration;

use mftik_proto::wire::mftiko::md::v1::{self as md, feed_event};
use prost::Message;

use super::{
    BACKOFF_MAX, BUFFER_AGE, BUFFER_MAX, Config, Delta, Effect, OpenError, Path, Snapshot,
    SnapshotReply, TimerKind, TimerToken, Update,
};
use crate::book::{DEFAULT_N, Level};

const UID: u32 = 7;
const UID_B: u32 = 8;
const NOW: i64 = 30_000_000_000;
const SEC_NS: i64 = 1_000_000_000;
const SEC: Duration = Duration::from_secs(1);
const PX: f64 = 10.0;
const EXCH: i64 = 3;

#[derive(Debug)]
enum Want {
    Log(&'static str),
    Prefix(&'static str),
    Snap {
        uid: u32,
        generation: u64,
    },
    Age {
        uid: u32,
        generation: u64,
    },
    Backoff {
        uid: u32,
        generation: u64,
        after: Duration,
    },
    Hook {
        uid: u32,
        epoch: u64,
        seq: u64,
        md_ts: i64,
        exch: i64,
    },
}

fn wlog(text: &'static str) -> Want {
    Want::Log(text)
}

fn wsnap(generation: u64) -> Want {
    Want::Snap {
        uid: UID,
        generation,
    }
}

fn wage(generation: u64) -> Want {
    Want::Age {
        uid: UID,
        generation,
    }
}

fn wback(generation: u64, secs: u64) -> Want {
    Want::Backoff {
        uid: UID,
        generation,
        after: Duration::from_secs(secs),
    }
}

fn whook(seq: u64, md_ts: i64) -> Want {
    Want::Hook {
        uid: UID,
        epoch: 1,
        seq,
        md_ts,
        exch: EXCH,
    }
}

fn expect(now: i64, effects: &[Effect], want: &[Want]) {
    assert_eq!(effects.len(), want.len(), "effects {effects:?}");
    for (effect, item) in effects.iter().zip(want) {
        match (effect, item) {
            (Effect::Log(line), Want::Log(text)) => assert_eq!(line, text),
            (Effect::Log(line), Want::Prefix(text)) => {
                assert!(line.starts_with(text), "{line}");
            }
            (
                Effect::RequestSnapshot {
                    uid,
                    generation,
                    wait,
                },
                Want::Snap {
                    uid: want_uid,
                    generation: want_generation,
                },
            ) => {
                assert_eq!(*uid, *want_uid);
                assert_eq!(*generation, *want_generation);
                assert_eq!(*wait, SEC);
            }
            (
                Effect::ArmTimer {
                    token,
                    after,
                    deadline_ns,
                },
                Want::Age {
                    uid: want_uid,
                    generation: want_generation,
                },
            ) => {
                assert_eq!(token.uid, *want_uid);
                assert_eq!(token.kind, TimerKind::BufferAge);
                assert_eq!(token.generation, *want_generation);
                assert_ne!(token.id, 0);
                assert_eq!(*after, BUFFER_AGE);
                assert_eq!(*deadline_ns, deadline(now, BUFFER_AGE));
            }
            (
                Effect::ArmTimer {
                    token,
                    after,
                    deadline_ns,
                },
                Want::Backoff {
                    uid: want_uid,
                    generation: want_generation,
                    after: want_after,
                },
            ) => {
                assert_eq!(token.uid, *want_uid);
                assert_eq!(token.kind, TimerKind::Backoff);
                assert_eq!(token.generation, *want_generation);
                assert_ne!(token.id, 0);
                assert_eq!(after, want_after);
                assert_eq!(*deadline_ns, deadline(now, *want_after));
            }
            (
                Effect::Hook(update),
                Want::Hook {
                    uid,
                    epoch,
                    seq,
                    md_ts,
                    exch,
                },
            ) => {
                assert_eq!(
                    update,
                    &Update {
                        uid: *uid,
                        conn_epoch: *epoch,
                        seq: *seq,
                        md_ts_ns: *md_ts,
                        exch_ts_ns: *exch,
                    }
                );
            }
            (effect, item) => panic!("effect {effect:?} is not {item:?}"),
        }
    }
}

fn deadline(now: i64, after: Duration) -> i64 {
    now + i64::try_from(after.as_nanos()).expect("timer fits in i64")
}

fn snap_gen(effects: &[Effect]) -> u64 {
    let mut found = None;
    for effect in effects {
        if let Effect::RequestSnapshot { generation, .. } = effect {
            assert!(found.is_none(), "two snapshots: {effects:?}");
            found = Some(*generation);
        }
    }
    found.unwrap_or_else(|| panic!("no snapshot in {effects:?}"))
}

fn age_token(effects: &[Effect]) -> TimerToken {
    token_of(effects, TimerKind::BufferAge)
}

fn backoff_token(effects: &[Effect]) -> TimerToken {
    token_of(effects, TimerKind::Backoff)
}

fn token_of(effects: &[Effect], kind: TimerKind) -> TimerToken {
    let mut found = None;
    for effect in effects {
        if let Effect::ArmTimer { token, .. } = effect
            && token.kind == kind
        {
            assert!(found.is_none(), "two {kind:?} timers: {effects:?}");
            found = Some(*token);
        }
    }
    found.unwrap_or_else(|| panic!("no {kind:?} timer in {effects:?}"))
}

struct Harness {
    path: Path,
    now: i64,
}

impl Harness {
    fn bare() -> Self {
        let path = Path::open(Config { recv_window: SEC }).expect("open");
        Self { path, now: NOW }
    }

    fn new() -> Self {
        let mut harness = Self::bare();
        harness.path.resolve(UID, 1.0);
        harness
    }

    fn send(&mut self, epoch: u64, seq: u64, bid: f64) -> Vec<Effect> {
        self.send_at(epoch, seq, self.now, bid)
    }

    fn send_at(&mut self, epoch: u64, seq: u64, md_ts: i64, bid: f64) -> Vec<Effect> {
        self.path.on_delta(
            self.now,
            Delta {
                uid: UID,
                conn_epoch: epoch,
                seq,
                md_ts_ns: md_ts,
                exch_ts_ns: EXCH,
                reset: false,
                bids: vec![Level {
                    price: PX,
                    size: bid,
                }],
                asks: Vec::new(),
            },
        )
    }

    fn reset(&mut self, epoch: u64, seq: u64, bid: f64) -> Vec<Effect> {
        self.path.on_delta(
            self.now,
            Delta {
                uid: UID,
                conn_epoch: epoch,
                seq,
                md_ts_ns: self.now,
                exch_ts_ns: EXCH,
                reset: true,
                bids: vec![Level {
                    price: PX,
                    size: bid,
                }],
                asks: Vec::new(),
            },
        )
    }

    fn reply(&mut self, generation: u64, epoch: u64, seq: u64, bid: f64) -> Vec<Effect> {
        self.path.on_snapshot(
            self.now,
            UID,
            generation,
            SnapshotReply::Book(Snapshot {
                uid: UID,
                conn_epoch: epoch,
                seq,
                bids: vec![Level {
                    price: PX,
                    size: bid,
                }],
                asks: Vec::new(),
            }),
        )
    }

    fn fail(&mut self, generation: u64, message: &str) -> Vec<Effect> {
        self.path.on_snapshot(
            self.now,
            UID,
            generation,
            SnapshotReply::Failed(message.to_owned()),
        )
    }

    fn fire(&mut self, token: TimerToken) -> Vec<Effect> {
        self.path.on_timer(self.now, token)
    }

    fn qty(&self) -> Option<f64> {
        self.path.view(UID)?.qty(true, PX)
    }

    fn sync(&mut self, epoch: u64, seq: u64, bid: f64) {
        let effects = self.send(epoch, seq, bid);
        let generation = snap_gen(&effects);
        let effects = self.reply(generation, epoch, seq, bid);
        expect(self.now, &effects, &[]);
        assert_eq!(self.qty(), Some(bid));
    }
}

#[test]
fn open_rejects_a_zero_recv_window() {
    let err = Path::open(Config {
        recv_window: Duration::ZERO,
    })
    .expect_err("zero window");
    assert_eq!(err, OpenError::RecvWindow);
    assert_eq!(err.to_string(), "bookpath: recv window must be positive");
    let path = Path::open(Config { recv_window: SEC }).expect("open");
    assert_eq!(path.recv_window(), SEC);
}

#[test]
fn snapshot_hides_the_book_and_the_next_delta_is_the_hook() {
    let mut book = Harness::new();
    let effects = book.send(1, 1, 1.0);
    expect(book.now, &effects, &[wage(1), wsnap(0)]);
    assert!(book.path.view(UID).is_none());
    let generation = snap_gen(&effects);

    let effects = book.path.on_snapshot(
        book.now,
        UID,
        generation,
        SnapshotReply::Book(Snapshot {
            uid: UID,
            conn_epoch: 1,
            seq: 1,
            bids: vec![Level {
                price: PX,
                size: 1.0,
            }],
            asks: vec![Level {
                price: 12.0,
                size: 2.0,
            }],
        }),
    );
    expect(book.now, &effects, &[]);
    let view = book.path.view(UID).expect("synced");
    assert_eq!(view.window(), DEFAULT_N);
    assert_eq!(view.tick_size(), 1.0);
    assert_eq!(view.qty(true, PX), Some(1.0));
    assert_eq!(view.qty(false, 12.0), Some(2.0));
    assert!(book.path.view(UID_B).is_none());

    let effects = book.send(1, 2, 4.0);
    expect(book.now, &effects, &[whook(2, book.now)]);
    assert_eq!(book.qty(), Some(4.0));
    assert_eq!(
        book.path.view(UID).and_then(|view| view.qty(false, 12.0)),
        Some(2.0)
    );

    let effects = book.reply(generation, 1, 1, 9.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(4.0));
}

#[test]
fn buffered_deltas_at_or_below_the_snapshot_fold_without_a_hook() {
    let mut book = Harness::new();
    book.send(1, 1, 9.0);
    let effects = book.send(1, 2, 4.0);
    expect(book.now, &effects, &[]);
    let effects = book.reply(0, 1, 1, 1.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(4.0));
}

#[test]
fn a_gap_or_a_new_epoch_asks_for_another_snapshot() {
    let mut book = Harness::new();
    book.sync(1, 2, 1.0);
    let effects = book.send(1, 4, 8.0);
    expect(book.now, &effects, &[wage(2), wsnap(1)]);
    assert!(book.path.view(UID).is_none());
    let effects = book.reply(1, 1, 4, 8.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(8.0));
    let effects = book.send(1, 5, 3.0);
    expect(book.now, &effects, &[whook(5, book.now)]);
    assert_eq!(book.qty(), Some(3.0));

    let effects = book.send(2, 1, 6.0);
    expect(book.now, &effects, &[wage(3), wsnap(2)]);
    assert!(book.path.view(UID).is_none());
    let effects = book.reply(2, 2, 1, 6.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(6.0));
    let effects = book.send(2, 2, 7.0);
    expect(
        book.now,
        &effects,
        &[Want::Hook {
            uid: UID,
            epoch: 2,
            seq: 2,
            md_ts: book.now,
            exch: EXCH,
        }],
    );
    assert_eq!(book.qty(), Some(7.0));
}

#[test]
fn a_duplicate_or_older_cursor_is_ignored() {
    let mut book = Harness::new();
    book.sync(1, 2, 1.0);
    let effects = book.send(1, 2, 9.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(1.0));
    let effects = book.send(1, 1, 9.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(1.0));
    let effects = book.send_at(0, 9, book.now, 9.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(1.0));
}

#[test]
fn the_top_of_uint64_is_continuous_and_zero_is_not_newer() {
    let mut book = Harness::new();
    book.sync(1, u64::MAX - 1, 1.0);
    let effects = book.send(1, u64::MAX, 2.0);
    expect(book.now, &effects, &[whook(u64::MAX, book.now)]);
    assert_eq!(book.qty(), Some(2.0));

    // Lexicographic newer requires seq > cursor. Zero is not above the top.
    let effects = book.send(1, 0, 3.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(2.0));
}

#[test]
fn a_stale_delta_is_applied_and_only_a_fresh_one_hooks() {
    let mut book = Harness::new();
    book.sync(1, 1, 1.0);
    let effects = book.send_at(1, 2, book.now - 2 * SEC_NS, 2.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(2.0));

    let edge = book.now - SEC_NS;
    let effects = book.send_at(1, 3, edge, 3.0);
    expect(book.now, &effects, &[whook(3, edge)]);
    assert_eq!(book.qty(), Some(3.0));

    let late = edge - 1;
    let effects = book.send_at(1, 4, late, 4.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(4.0));

    let ahead = book.now + 1;
    let effects = book.send_at(1, 5, ahead, 5.0);
    expect(book.now, &effects, &[whook(5, ahead)]);
    assert_eq!(book.qty(), Some(5.0));

    // The stale write is already in the book when a later hook runs.
    let effects = book.send_at(1, 6, book.now - 2 * SEC_NS, 6.0);
    expect(book.now, &effects, &[]);
    let effects = book.path.on_delta(
        book.now,
        Delta {
            uid: UID,
            conn_epoch: 1,
            seq: 7,
            md_ts_ns: book.now,
            exch_ts_ns: EXCH,
            reset: false,
            bids: Vec::new(),
            asks: vec![Level {
                price: 12.0,
                size: 1.0,
            }],
        },
    );
    expect(book.now, &effects, &[whook(7, book.now)]);
    assert_eq!(book.qty(), Some(6.0));
}

#[test]
fn reset_drops_the_book_and_rejects_an_older_snapshot() {
    let mut book = Harness::new();
    book.sync(1, 1, 1.0);
    let effects = book.reset(1, 2, 9.0);
    expect(book.now, &effects, &[wsnap(1)]);
    assert!(book.path.view(UID).is_none());

    let effects = book.reply(1, 1, 1, 1.0);
    expect(
        book.now,
        &effects,
        &[
            wlog("book: uid 7: bookpath: snapshot is behind the feed"),
            wback(1, 1),
        ],
    );
    assert!(book.path.view(UID).is_none());

    book.now += SEC_NS;
    let token = backoff_token(&effects);
    let effects = book.fire(token);
    expect(book.now, &effects, &[wsnap(1)]);
    let effects = book.send(1, 3, 3.0);
    expect(book.now, &effects, &[wage(3)]);
    let effects = book.reply(1, 1, 2, 2.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(3.0));
}

#[test]
fn an_old_reset_is_ignored() {
    let mut book = Harness::new();
    book.sync(1, 2, 1.0);
    let effects = book.reset(1, 2, 9.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(1.0));

    let mut book = Harness::new();
    book.send(1, 1, 1.0);
    book.send(1, 2, 2.0);
    let effects = book.reset(1, 1, 9.0);
    expect(book.now, &effects, &[]);
    let effects = book.reply(0, 1, 1, 1.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(2.0));
}

#[test]
fn the_first_event_can_be_a_reset_and_its_levels_are_not_applied() {
    let mut book = Harness::new();
    let effects = book.reset(1, 4, 9.0);
    expect(book.now, &effects, &[wsnap(1)]);
    let effects = book.reply(1, 1, 4, 2.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(2.0));
}

#[test]
fn not_synced_backs_off_until_thirty_seconds_and_a_success_resets_it() {
    let mut book = Harness::new();
    let effects = book.send(1, 1, 1.0);
    let mut generation = snap_gen(&effects);
    let mut waits = Vec::new();
    for secs in [1_u64, 2, 4, 8, 16, 30, 30] {
        let effects = book.fail(generation, "mdpath: md.view: not_synced");
        expect(
            book.now,
            &effects,
            &[
                wlog("book: uid 7: mdpath: md.view: not_synced"),
                wback(generation, secs),
            ],
        );
        assert!(book.path.view(UID).is_none());
        let token = backoff_token(&effects);
        waits.push(secs);
        if secs == 1 {
            let effects = book.send(1, 2, 2.0);
            expect(book.now, &effects, &[]);
        }
        book.now += i64::try_from(Duration::from_secs(secs).as_nanos()).expect("secs");
        let effects = book.fire(token);
        expect(book.now, &effects, &[wsnap(generation)]);
        generation = snap_gen(&effects);
    }
    assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30]);
    assert_eq!(Duration::from_secs(30), BACKOFF_MAX);

    let effects = book.reply(generation, 1, 2, 2.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(2.0));

    let effects = book.send(1, 4, 4.0);
    generation = snap_gen(&effects);
    let effects = book.fail(generation, "mdpath: md.view: not_synced");
    expect(
        book.now,
        &effects,
        &[
            wlog("book: uid 7: mdpath: md.view: not_synced"),
            wback(generation, 1),
        ],
    );
}

#[test]
fn a_late_backoff_timer_does_not_cancel_the_new_wait() {
    let mut book = Harness::new();
    let effects = book.send(1, 1, 1.0);
    let generation = snap_gen(&effects);
    let effects = book.fail(generation, "down");
    let first = backoff_token(&effects);
    book.now += SEC_NS;
    let effects = book.fire(first);
    expect(book.now, &effects, &[wsnap(generation)]);
    let effects = book.fail(generation, "down");
    let second = backoff_token(&effects);
    assert_ne!(first.id, second.id);
    assert_eq!(first.generation, second.generation);
    let effects = book.fire(first);
    expect(book.now, &effects, &[]);
    let effects = book.fire(second);
    expect(book.now, &effects, &[wsnap(generation)]);
}

#[test]
fn the_buffer_cap_discards_the_inflight_snapshot() {
    let mut book = Harness::new();
    let mut requests = 0;
    let last = u64::try_from(BUFFER_MAX).unwrap();
    for seq in 1..=last {
        let effects = book.send(1, seq, 1.0);
        requests += effects
            .iter()
            .filter(|effect| matches!(effect, Effect::RequestSnapshot { .. }))
            .count();
        if seq + 1 == last {
            assert_eq!(requests, 1, "the cap is the 10000th delta");
        }
    }
    assert_eq!(requests, 2);
    let effects = book.reply(0, 1, 1, 9.0);
    expect(book.now, &effects, &[]);
    assert!(book.path.view(UID).is_none());
    let effects = book.reply(1, 1, last, 4.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(4.0));
}

#[test]
fn buffer_age_discards_the_inflight_snapshot() {
    let mut book = Harness::new();
    let effects = book.send(1, 1, 1.0);
    let token = age_token(&effects);
    book.now += 10 * SEC_NS - 1;
    let effects = book.fire(token);
    expect(book.now, &effects, &[]);
    let effects = book.reply(0, 1, 1, 9.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(9.0));

    let mut book = Harness::new();
    let effects = book.send(1, 1, 1.0);
    let token = age_token(&effects);
    book.now += 10 * SEC_NS;
    let effects = book.fire(token);
    expect(book.now, &effects, &[wsnap(1)]);
    let effects = book.reply(0, 1, 1, 9.0);
    expect(book.now, &effects, &[]);
    assert!(book.path.view(UID).is_none());
    let effects = book.reply(1, 1, 1, 2.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(2.0));
    let effects = book.fire(token);
    expect(book.now, &effects, &[]);
}

#[test]
fn a_later_delta_drops_a_buffer_that_has_been_open_for_ten_seconds() {
    let mut book = Harness::new();
    book.send(1, 1, 1.0);
    book.now += 10 * SEC_NS;
    let effects = book.send(1, 2, 2.0);
    expect(book.now, &effects, &[wsnap(1)]);
    let effects = book.reply(0, 1, 2, 9.0);
    expect(book.now, &effects, &[]);
    assert!(book.path.view(UID).is_none());
    let effects = book.reply(1, 1, 2, 4.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(4.0));
}

#[test]
fn the_clock_moving_back_does_not_drop_the_buffer() {
    let mut book = Harness::new();
    let effects = book.send(1, 1, 1.0);
    let token = age_token(&effects);
    book.now -= SEC_NS;
    let effects = book.send(1, 2, 2.0);
    expect(book.now, &effects, &[]);
    let effects = book.fire(token);
    expect(book.now, &effects, &[]);
    let effects = book.reply(0, 1, 1, 1.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(2.0));
}

#[test]
fn an_unresolved_uid_or_a_bad_tick_does_not_query() {
    let mut book = Harness::bare();
    let effects = book.send(1, 1, 1.0);
    expect(book.now, &effects, &[wlog("book: uid 7 is not resolved")]);
    let effects = book.send(1, 2, 2.0);
    expect(book.now, &effects, &[]);
    assert!(book.path.view(UID).is_none());

    book.path.resolve(UID, 0.0);
    let effects = book.send(1, 3, 3.0);
    expect(book.now, &effects, &[wlog("book: uid 7 has no price tick")]);
    let effects = book.send(1, 4, 4.0);
    expect(book.now, &effects, &[]);

    book.path.resolve(UID_B, f64::NAN);
    let effects = book.path.on_delta(
        book.now,
        Delta {
            uid: UID_B,
            conn_epoch: 1,
            seq: 1,
            md_ts_ns: book.now,
            exch_ts_ns: EXCH,
            reset: false,
            bids: vec![Level {
                price: PX,
                size: 1.0,
            }],
            asks: Vec::new(),
        },
    );
    expect(book.now, &effects, &[wlog("book: uid 8 has no price tick")]);

    book.path.resolve(UID, 1.0);
    let effects = book.send(1, 5, 5.0);
    expect(book.now, &effects, &[wage(1), wsnap(0)]);
}

#[test]
fn a_tick_that_vanishes_before_the_snapshot_does_not_install_or_back_off() {
    let mut book = Harness::new();
    book.send(1, 1, 1.0);
    book.path.resolve(UID, 0.0);
    let effects = book.reply(0, 1, 1, 1.0);
    expect(book.now, &effects, &[wlog("book: uid 7 has no price tick")]);
    assert!(book.path.view(UID).is_none());

    book.path.resolve(UID, 1.0);
    let effects = book.send(1, 2, 2.0);
    expect(book.now, &effects, &[wsnap(0)]);
    let effects = book.reply(0, 1, 1, 1.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(2.0));
}

#[test]
fn a_bad_tick_does_not_unsync_a_book_that_is_already_up() {
    let mut book = Harness::new();
    book.sync(1, 1, 1.0);
    book.path.resolve(UID, -1.0);
    let effects = book.send(1, 2, 9.0);
    expect(book.now, &effects, &[wlog("book: uid 7 has no price tick")]);
    assert_eq!(book.qty(), Some(1.0));
    book.path.resolve(UID, 1.0);
    let effects = book.send(1, 2, 4.0);
    expect(book.now, &effects, &[whook(2, book.now)]);
    assert_eq!(book.qty(), Some(4.0));
}

#[test]
fn a_hole_in_the_buffer_waits_for_a_later_snapshot() {
    let mut book = Harness::new();
    book.send(1, 1, 1.0);
    book.send(1, 3, 3.0);
    let effects = book.reply(0, 1, 1, 1.0);
    expect(
        book.now,
        &effects,
        &[
            wlog("book: uid 7: bookpath: snapshot is behind the feed"),
            wback(0, 1),
        ],
    );
    assert!(book.path.view(UID).is_none());
    book.now += SEC_NS;
    let effects = book.fire(backoff_token(&effects));
    expect(book.now, &effects, &[wsnap(0)]);
    let effects = book.reply(0, 1, 3, 3.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(3.0));
}

#[test]
fn a_reordered_buffer_is_a_hole() {
    let mut book = Harness::new();
    book.send(1, 2, 2.0);
    book.send(1, 1, 1.0);
    let effects = book.reply(0, 1, 0, 9.0);
    expect(
        book.now,
        &effects,
        &[
            wlog("book: uid 7: bookpath: snapshot is behind the feed"),
            wback(0, 1),
        ],
    );
    assert!(book.path.view(UID).is_none());
}

#[test]
fn an_age_drop_uses_the_newest_buffered_cursor_as_the_floor() {
    let mut book = Harness::new();
    let armed = book.send(1, 5, 5.0);
    let token = age_token(&armed);
    book.send(1, 3, 3.0);
    book.now += 10 * SEC_NS;
    let effects = book.fire(token);
    expect(book.now, &effects, &[wsnap(1)]);
    let effects = book.reply(1, 1, 4, 4.0);
    expect(
        book.now,
        &effects,
        &[
            wlog("book: uid 7: bookpath: snapshot is behind the feed"),
            wback(1, 1),
        ],
    );
    book.now += SEC_NS;
    let effects = book.fire(backoff_token(&effects));
    expect(book.now, &effects, &[wsnap(1)]);
    let effects = book.reply(1, 1, 5, 5.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(5.0));
}

#[test]
fn two_instruments_keep_their_own_books() {
    let mut book = Harness::new();
    book.path.resolve(UID_B, 1.0);
    let effects = book.send(1, 1, 1.0);
    expect(book.now, &effects, &[wage(1), wsnap(0)]);
    let effects = book.path.on_delta(
        book.now,
        Delta {
            uid: UID_B,
            conn_epoch: 4,
            seq: 2,
            md_ts_ns: book.now,
            exch_ts_ns: EXCH,
            reset: false,
            bids: vec![Level {
                price: 4.0,
                size: 5.0,
            }],
            asks: Vec::new(),
        },
    );
    expect(
        book.now,
        &effects,
        &[
            Want::Age {
                uid: UID_B,
                generation: 1,
            },
            Want::Snap {
                uid: UID_B,
                generation: 0,
            },
        ],
    );
    book.reply(0, 1, 1, 1.0);
    let effects = book.path.on_snapshot(
        book.now,
        UID_B,
        0,
        SnapshotReply::Book(Snapshot {
            uid: UID_B,
            conn_epoch: 4,
            seq: 2,
            bids: vec![Level {
                price: 4.0,
                size: 5.0,
            }],
            asks: Vec::new(),
        }),
    );
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(1.0));
    assert_eq!(
        book.path.view(UID_B).and_then(|view| view.qty(true, 4.0)),
        Some(5.0)
    );
}

#[test]
fn close_hides_the_book_and_ignores_later_events() {
    let mut book = Harness::new();
    let armed = book.send(1, 1, 1.0);
    let token = age_token(&armed);
    book.reply(0, 1, 1, 1.0);
    assert_eq!(book.qty(), Some(1.0));
    book.path.close();
    assert!(book.path.view(UID).is_none());
    expect(book.now, &book.send(1, 2, 9.0), &[]);
    expect(book.now, &book.fire(token), &[]);
    expect(book.now, &book.reply(0, 1, 2, 9.0), &[]);
    expect(book.now, &book.path.on_message(book.now, &[]), &[]);
    book.path.close();
    assert!(book.path.view(UID).is_none());
}

#[test]
fn levels_outside_the_ring_are_left_to_the_book() {
    let mut book = Harness::new();
    book.send(1, 1, 1.0);
    let span = u32::try_from(DEFAULT_N).unwrap();
    let inside = 100.0 - f64::from(span - 1);
    let outside = 100.0 - f64::from(span);
    let effects = book.path.on_snapshot(
        book.now,
        UID,
        0,
        SnapshotReply::Book(Snapshot {
            uid: 0,
            conn_epoch: 1,
            seq: 1,
            bids: vec![
                Level {
                    price: 100.0,
                    size: 1.0,
                },
                Level {
                    price: inside,
                    size: 2.0,
                },
                Level {
                    price: outside,
                    size: 3.0,
                },
            ],
            asks: Vec::new(),
        }),
    );
    expect(book.now, &effects, &[]);
    let view = book.path.view(UID).expect("synced");
    assert_eq!(view.qty(true, 100.0), Some(1.0));
    assert_eq!(view.qty(true, inside), Some(2.0));
    assert_eq!(view.qty(true, outside), None);
}

#[test]
fn a_message_matches_the_decoded_delta() {
    let mut from_delta = Harness::new();
    let decoded = from_delta.send(1, 1, 1.0);
    let mut from_bytes = Harness::new();
    let event = md::FeedEvent {
        uid: UID,
        event: Some(feed_event::Event::OrderBookDelta(md::OrderBookDelta {
            meta: Some(md::Meta { exch_ts_ns: EXCH }),
            bids: vec![md::Level {
                price: PX,
                qty: 1.0,
            }],
            asks: Vec::new(),
            reset: false,
        })),
    };
    let bytes = from_bytes.path.on_message(
        from_bytes.now,
        &frame(from_bytes.now, 1, 1, &event.encode_to_vec()),
    );
    assert_eq!(bytes, decoded);

    let mut bare = Harness::new();
    let first = md::FeedEvent {
        uid: UID,
        event: Some(feed_event::Event::OrderBookDelta(md::OrderBookDelta {
            bids: vec![md::Level {
                price: PX,
                qty: 1.0,
            }],
            ..Default::default()
        })),
    };
    let effects = bare
        .path
        .on_message(bare.now, &frame(bare.now, 1, 1, &first.encode_to_vec()));
    expect(bare.now, &effects, &[wage(1), wsnap(0)]);
    bare.reply(0, 1, 1, 1.0);
    let second = md::FeedEvent {
        uid: UID,
        event: Some(feed_event::Event::OrderBookDelta(md::OrderBookDelta {
            bids: vec![md::Level {
                price: PX,
                qty: 2.0,
            }],
            ..Default::default()
        })),
    };
    let effects = bare
        .path
        .on_message(bare.now, &frame(bare.now, 1, 2, &second.encode_to_vec()));
    expect(
        bare.now,
        &effects,
        &[Want::Hook {
            uid: UID,
            epoch: 1,
            seq: 2,
            md_ts: bare.now,
            exch: 0,
        }],
    );
    assert_eq!(bare.qty(), Some(2.0));
}

#[test]
fn a_bad_envelope_or_payload_does_not_move_the_cursor() {
    let mut book = Harness::new();
    let effects = book.path.on_message(book.now, &[1, 2, 3]);
    expect(
        book.now,
        &effects,
        &[wlog(
            "book: envelope: envelope: message shorter than 40 bytes: 3 bytes",
        )],
    );
    let effects = book
        .path
        .on_message(book.now, &frame(book.now, 1, 1, &[0x80]));
    expect(book.now, &effects, &[Want::Prefix("book: payload: ")]);
    assert!(book.path.view(UID).is_none());

    let event = md::FeedEvent {
        uid: 0,
        event: Some(feed_event::Event::OrderBookDelta(md::OrderBookDelta {
            bids: vec![md::Level {
                price: PX,
                qty: 1.0,
            }],
            ..Default::default()
        })),
    };
    let bytes = frame(book.now, 1, 1, &event.encode_to_vec());
    let effects = book.path.on_message(book.now, &bytes);
    expect(book.now, &effects, &[wlog("book: uid 0")]);
    let effects = book.path.on_message(book.now, &bytes);
    expect(book.now, &effects, &[]);
}

#[test]
fn a_non_delta_resyncs_without_waiting_out_backoff() {
    let mut book = Harness::new();
    book.sync(1, 1, 1.0);
    let event = md::FeedEvent {
        uid: UID,
        event: Some(feed_event::Event::Trade(md::Trade {
            price: PX,
            qty: 1.0,
            ..Default::default()
        })),
    };
    let effects = book
        .path
        .on_message(book.now, &frame(book.now, 1, 4, &event.encode_to_vec()));
    expect(book.now, &effects, &[wsnap(1)]);
    assert!(book.path.view(UID).is_none());
    let effects = book.reply(0, 1, 4, 9.0);
    expect(book.now, &effects, &[]);
    assert!(book.path.view(UID).is_none());
    let effects = book.reply(1, 1, 4, 4.0);
    expect(book.now, &effects, &[]);
    assert_eq!(book.qty(), Some(4.0));
}

#[test]
fn an_empty_reply_or_the_wrong_uid_backs_off() {
    let mut book = Harness::new();
    book.send(1, 1, 1.0);
    let effects = book
        .path
        .on_snapshot(book.now, UID, 0, SnapshotReply::Empty);
    expect(
        book.now,
        &effects,
        &[wlog("book: uid 7: bookpath: empty snapshot"), wback(0, 1)],
    );

    let mut book = Harness::new();
    book.send(1, 1, 1.0);
    let effects = book
        .path
        .on_snapshot(book.now, UID, 0, SnapshotReply::Failed(String::new()));
    expect(
        book.now,
        &effects,
        &[wlog("book: uid 7: bookpath: empty snapshot"), wback(0, 1)],
    );

    let mut book = Harness::new();
    book.send(1, 1, 1.0);
    let effects = book.path.on_snapshot(
        book.now,
        UID,
        0,
        SnapshotReply::Book(Snapshot {
            uid: UID_B,
            conn_epoch: 1,
            seq: 1,
            bids: vec![Level {
                price: PX,
                size: 1.0,
            }],
            asks: Vec::new(),
        }),
    );
    expect(
        book.now,
        &effects,
        &[wlog("book: uid 7: bookpath: snapshot uid 8"), wback(0, 1)],
    );
    assert!(book.path.view(UID).is_none());
}

#[test]
fn success_clears_the_once_only_log() {
    let mut book = Harness::bare();
    let effects = book.send(1, 1, 1.0);
    expect(book.now, &effects, &[wlog("book: uid 7 is not resolved")]);
    book.path.resolve(UID, 1.0);
    book.sync(1, 1, 1.0);
    book.path.forget(UID);
    let effects = book.send(1, 2, 2.0);
    expect(book.now, &effects, &[wlog("book: uid 7 is not resolved")]);
    assert_eq!(book.qty(), Some(1.0));
}

fn frame(md_ts: i64, epoch: u64, seq: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0_u8; 40 + payload.len()];
    out[0] = 1;
    out[8..16].copy_from_slice(&md_ts.to_le_bytes());
    out[16..24].copy_from_slice(&epoch.to_le_bytes());
    out[24..32].copy_from_slice(&seq.to_le_bytes());
    out[40..].copy_from_slice(payload);
    out
}
