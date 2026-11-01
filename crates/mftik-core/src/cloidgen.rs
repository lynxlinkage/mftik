//! Process-wide `client_order_id` generator (PROTOCOL §9.4).
//!
//! One counter for the process, shared by every algo and every account.
//! A millisecond has 1024 ids, seq 0..=1023. When that millisecond is used
//! up, `ts_ms` borrows the next one. Borrowing is not capped, so `ts_ms`
//! can run ahead of the `now_ms` the caller passed. A clock step backwards
//! does not move `ts_ms` back.
//!
//! The counter is not persisted. A new process starts at seq 0 again. A
//! collision with an earlier process is the account worker's idempotency
//! problem, not something this module prevents.
//!
//! `now_ms` is the caller's clock, in Unix milliseconds. [`next`] does not
//! read a clock. The counter word is the next id to issue, updated with one
//! compare-and-swap and no mutex. The zero word is epoch, seq 0, so the
//! first id of a fresh process is seq 0.

use std::sync::atomic::{AtomicU64, Ordering};

use mftik_proto::wire::mftiko::session::v1::AccountDecl;

use crate::cloid::{self, CloidError};

/// Algo is not listed in the account's `algo_ids`. [`next`] returns this
/// without taking a sequence number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextError {
    AlgoNotDeclared { algo: u32 },
    Encode(CloidError),
}

impl NextError {
    pub fn is_algo_not_declared(&self) -> bool {
        matches!(self, NextError::AlgoNotDeclared { .. })
    }
}

impl std::fmt::Display for NextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NextError::AlgoNotDeclared { algo } => {
                write!(f, "cloidgen: algo {algo} is not declared for the account")
            }
            NextError::Encode(err) => std::fmt::Display::fmt(err, f),
        }
    }
}

impl std::error::Error for NextError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NextError::Encode(err) => Some(err),
            NextError::AlgoNotDeclared { .. } => None,
        }
    }
}

/// Next id to issue. High bits are `ts_ms - epoch`, low [`cloid::SEQ_BITS`]
/// are `seq`. Storing the next id, rather than the last one issued, is what
/// lets a zero word hand out epoch seq 0.
static NEXT_SLOT: AtomicU64 = AtomicU64::new(0);

/// Next `client_order_id` for `algo` on `account` at `now_ms`.
///
/// `algo` must be listed in `account.algo_ids`. The id is [`cloid::encode`]
/// of version 1. A listed algo that does not fit the layout returns the
/// encode error and does not take a number.
pub fn next(account: &AccountDecl, algo: u32, now_ms: i64) -> Result<String, NextError> {
    if !declared(account, algo) {
        return Err(NextError::AlgoNotDeclared { algo });
    }
    if !(cloid::ALGO_MIN..=cloid::ALGO_MAX).contains(&algo) {
        return cloid::encode(algo, cloid::EPOCH_MS, 0).map_err(NextError::Encode);
    }
    let (ts_ms, seq) = issue(now_ms)?;
    cloid::encode(algo, ts_ms, seq).map_err(NextError::Encode)
}

fn declared(account: &AccountDecl, algo: u32) -> bool {
    account.algo_ids.contains(&algo)
}

struct Proposal {
    ts_ms: i64,
    seq: u32,
    packed: u64,
}

fn propose(old: u64, now_off: i64) -> Result<Proposal, NextError> {
    let ts = (old >> cloid::SEQ_BITS) as i64;
    let seq = (old & u64::from(cloid::SEQ_MAX)) as u32;
    let (issue_ts, issue_seq) = if now_off > ts {
        (now_off, 0)
    } else {
        (ts, seq)
    };
    if !(0..cloid::TS_SPAN).contains(&issue_ts) {
        return Err(window_err(issue_ts));
    }
    let (next_ts, next_seq) = if issue_seq == cloid::SEQ_MAX {
        (issue_ts + 1, 0)
    } else {
        (issue_ts, issue_seq + 1)
    };
    let packed = ((next_ts as u64) << cloid::SEQ_BITS) | u64::from(next_seq);
    Ok(Proposal {
        ts_ms: cloid::EPOCH_MS + issue_ts,
        seq: issue_seq,
        packed,
    })
}

/// The process counter as it stands: the next id it would issue, packed as
/// above. The event log's `init` frame carries it.
pub fn slot() -> u64 {
    NEXT_SLOT.load(Ordering::SeqCst)
}

/// Sets the process counter to the `init` frame's slot so a replay issues
/// the same client order ids (PROTOCOL §12.6).
pub fn restore(slot: u64) {
    NEXT_SLOT.store(slot, Ordering::SeqCst);
}

/// Take the next `(ts_ms, seq)`.
///
/// `now_ms` before the counter, including a clock before the epoch, keeps
/// the counter's `ts_ms`. `now_ms` ahead of the counter jumps to that
/// millisecond and seq 0. A millisecond whose seq is exhausted borrows the
/// next one. A `ts_ms` [`cloid::encode`] cannot represent is refused and
/// the counter is left as it was.
fn issue(now_ms: i64) -> Result<(i64, u32), NextError> {
    // A now_ms before the epoch is behind every ts the counter can hold.
    // Subtracting a now_ms near i64::MIN from the epoch would overflow.
    let now_off = if now_ms >= cloid::EPOCH_MS {
        now_ms - cloid::EPOCH_MS
    } else {
        -1
    };
    loop {
        let old = NEXT_SLOT.load(Ordering::SeqCst);
        let proposal = propose(old, now_off)?;
        if NEXT_SLOT
            .compare_exchange(old, proposal.packed, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Ok((proposal.ts_ms, proposal.seq));
        }
    }
}

/// [`cloid::encode`]'s error for a timestamp offset outside the 40-bit window.
fn window_err(ts_off: i64) -> NextError {
    let ts_ms = cloid::EPOCH_MS.wrapping_add(ts_off);
    match cloid::encode(cloid::ALGO_MIN, ts_ms, 0) {
        Err(err) => NextError::Encode(err),
        Ok(_) => NextError::Encode(CloidError::Ts { ts_ms }),
    }
}

#[cfg(test)]
fn test_set_slot(slot: u64) {
    NEXT_SLOT.store(slot, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};
    use std::thread;

    use super::*;
    use crate::cloid::{self, Id};

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn hold() -> MutexGuard<'static, ()> {
        let guard = TEST_LOCK.lock().unwrap_or_else(|err| err.into_inner());
        test_set_slot(0);
        guard
    }

    fn account(algos: &[u32]) -> AccountDecl {
        AccountDecl {
            name: String::new(),
            api_id: 1,
            td_instance: String::new(),
            algo_ids: algos.to_vec(),
        }
    }

    fn take(account: &AccountDecl, algo: u32, now_ms: i64) -> Id {
        let s = next(account, algo, now_ms).unwrap();
        let id = cloid::decode(&s).unwrap();
        assert_eq!(id.ver, cloid::VERSION);
        assert_eq!(id.algo, algo);
        assert_eq!(id.decimal(), s);
        id
    }

    #[test]
    fn fresh_counter_hands_out_seq_zero_at_the_callers_millisecond() {
        let _lock = hold();
        let acct = account(&[1]);
        let now = cloid::EPOCH_MS + 5;
        let id = take(&acct, 1, now);
        assert_eq!(id.ts_ms, now);
        assert_eq!(id.seq, 0);
        let id = take(&acct, 1, now);
        assert_eq!(id.ts_ms, now);
        assert_eq!(id.seq, 1);
    }

    #[test]
    fn accounts_and_algos_share_one_counter() {
        let _lock = hold();
        let a = account(&[1]);
        let b = account(&[2, 3]);
        let now = cloid::EPOCH_MS + 8;
        let first = take(&a, 1, now);
        let second = take(&b, 2, now);
        let third = take(&b, 3, now);
        assert_eq!(
            (first.ts_ms, first.seq, second.seq, third.seq),
            (now, 0, 1, 2)
        );
        assert_eq!(second.ts_ms, now);
        assert_eq!(third.ts_ms, now);
    }

    #[test]
    fn exhausted_millisecond_borrows_the_next_one() {
        let _lock = hold();
        let acct = account(&[1, 2]);
        let now = cloid::EPOCH_MS + 3;
        let slot = (3_u64 << cloid::SEQ_BITS) | u64::from(cloid::SEQ_MAX);
        test_set_slot(slot);
        let last = take(&acct, 1, now);
        assert_eq!(last.ts_ms, now);
        assert_eq!(last.seq, cloid::SEQ_MAX);
        let borrowed = take(&acct, 2, now);
        assert_eq!(borrowed.ts_ms, now + 1);
        assert_eq!(borrowed.seq, 0);
        let after = take(&acct, 1, now);
        assert_eq!(after.ts_ms, now + 1);
        assert_eq!(after.seq, 1);
    }

    #[test]
    fn clock_moves_forward_and_a_step_back_does_not() {
        let _lock = hold();
        let acct = account(&[1]);
        let now = cloid::EPOCH_MS + 50;
        let id = take(&acct, 1, now);
        assert_eq!((id.ts_ms, id.seq), (now, 0));
        let id = take(&acct, 1, now - 10);
        assert_eq!((id.ts_ms, id.seq), (now, 1));
        let id = take(&acct, 1, cloid::EPOCH_MS - 1);
        assert_eq!((id.ts_ms, id.seq), (now, 2));
        let id = take(&acct, 1, now + 4);
        assert_eq!((id.ts_ms, id.seq), (now + 4, 0));
        let id = take(&acct, 1, now);
        assert_eq!((id.ts_ms, id.seq), (now + 4, 1));
    }

    #[test]
    fn clock_before_the_epoch_on_a_fresh_counter_uses_the_epoch() {
        let _lock = hold();
        let id = take(&account(&[4]), 4, i64::MIN);
        assert_eq!(id.ts_ms, cloid::EPOCH_MS);
        assert_eq!(id.seq, 0);
    }

    #[test]
    fn undeclared_algo_takes_no_number() {
        let _lock = hold();
        let acct = account(&[1, 2]);
        let now = cloid::EPOCH_MS + 10;
        for algo in [0, 3, cloid::ALGO_MAX] {
            let err = next(&acct, algo, now).unwrap_err();
            assert!(err.is_algo_not_declared(), "{algo} {err}");
        }
        let empty = account(&[]);
        assert!(next(&empty, 1, now).unwrap_err().is_algo_not_declared());
        assert_eq!(slot(), 0);
        let id = take(&acct, 1, now);
        assert_eq!((id.ts_ms, id.seq), (now, 0));
    }

    #[test]
    fn listed_algo_that_does_not_fit_takes_no_number() {
        let _lock = hold();
        let now = cloid::EPOCH_MS + 10;
        let err = next(&account(&[0]), 0, now).unwrap_err();
        assert!(!err.is_algo_not_declared());
        let want = cloid::encode(0, cloid::EPOCH_MS, 0).unwrap_err();
        assert_eq!(err, NextError::Encode(want));
        let err = next(&account(&[cloid::ALGO_MAX + 1]), cloid::ALGO_MAX + 1, now).unwrap_err();
        assert!(!err.is_algo_not_declared());
        assert_eq!(slot(), 0);
        let id = take(&account(&[1]), 1, now);
        assert_eq!((id.ts_ms, id.seq), (now, 0));
    }

    #[test]
    fn timestamp_outside_the_window_does_not_consume() {
        let _lock = hold();
        let acct = account(&[1]);
        let bad = cloid::EPOCH_MS + cloid::TS_SPAN;
        let err = next(&acct, 1, bad).unwrap_err();
        let want = cloid::encode(1, bad, 0).unwrap_err();
        assert_eq!(err, NextError::Encode(want));
        let err = next(&acct, 1, i64::MAX).unwrap_err();
        assert!(!err.is_algo_not_declared());
        assert_eq!(slot(), 0);
        let id = take(&acct, 1, cloid::EPOCH_MS);
        assert_eq!((id.ts_ms, id.seq), (cloid::EPOCH_MS, 0));
    }

    #[test]
    fn last_millisecond_of_the_window_does_not_borrow_past_it() {
        let _lock = hold();
        let last = cloid::TS_SPAN - 1;
        let packed = ((last as u64) << cloid::SEQ_BITS) | u64::from(cloid::SEQ_MAX);
        test_set_slot(packed);
        let acct = account(&[1]);
        let id = take(&acct, 1, cloid::EPOCH_MS);
        assert_eq!(id.ts_ms, cloid::EPOCH_MS + last);
        assert_eq!(id.seq, cloid::SEQ_MAX);
        let stuck = slot();
        assert!(next(&acct, 1, cloid::EPOCH_MS).is_err());
        assert!(next(&acct, 1, cloid::EPOCH_MS + last).is_err());
        assert_eq!(slot(), stuck);
    }

    #[test]
    fn concurrent_calls_take_distinct_slots() {
        let _lock = hold();
        let now = cloid::EPOCH_MS + 80;
        let acct = account(&[1, 2]);
        let got = Mutex::new(Vec::new());
        thread::scope(|scope| {
            for worker in 0..4 {
                let acct = &acct;
                let got = &got;
                scope.spawn(move || {
                    let algo = if worker % 2 == 0 { 1 } else { 2 };
                    for _ in 0..4 {
                        let s = next(acct, algo, now).unwrap();
                        got.lock().unwrap().push((algo, s));
                    }
                });
            }
        });
        let got = got.into_inner().unwrap();
        assert_eq!(got.len(), 16);
        let mut slots = Vec::new();
        for (algo, s) in got {
            let id = cloid::decode(&s).unwrap();
            assert_eq!(id.algo, algo);
            assert_eq!(id.ts_ms, now);
            slots.push(id.seq);
        }
        slots.sort_unstable();
        assert_eq!(slots, (0..16).collect::<Vec<_>>());
    }
}
