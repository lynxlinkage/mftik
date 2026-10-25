//! Semantic checks for the tick ring. Values are fixed and small.
//!
//! The ring matches mftiko `sdk/book`: best-price-first updates, a window of
//! `n` ticks, slots that are never cleared, and Go's `int64` tick conversion.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use super::{DEFAULT_N, Error, Level, OrderBook, tick};

thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

fn bump() {
    ALLOCS.with(|count| count.set(count.get().saturating_add(1)));
}

struct CountAlloc;

// SAFETY: every method forwards to the system allocator. The count is a
// thread-local integer and does not allocate.
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountAlloc = CountAlloc;

fn allocations_during(mut body: impl FnMut()) -> usize {
    body();
    ALLOCS.with(|count| count.set(0));
    body();
    ALLOCS.with(Cell::get)
}

fn new_book(n: usize, tick_size: f64) -> OrderBook {
    OrderBook::new(n, tick_size).unwrap_or_else(|err| panic!("{err}"))
}

fn lv(price: f64, size: f64) -> Level {
    Level { price, size }
}

fn bids(book: &OrderBook) -> Vec<(i64, f64)> {
    book.bids(book.window() as i64).collect()
}

fn asks(book: &OrderBook) -> Vec<(i64, f64)> {
    book.asks(book.window() as i64).collect()
}

#[test]
fn new_rejects_a_bad_window_or_tick_size() {
    for n in [0, 3, 6, 2047] {
        let err = OrderBook::new(n, 1.0).unwrap_err();
        assert_eq!(err, Error::Window);
        assert_eq!(err.to_string(), "book: n must be a power of two");
    }
    for tick_size in [0.0, -0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let err = OrderBook::new(4, tick_size).unwrap_err();
        assert_eq!(err, Error::TickSize, "tick={tick_size}");
        assert_eq!(err.to_string(), "book: tick size must be positive");
    }
    assert!(OrderBook::new(1, 1.0).is_ok());
    assert!(OrderBook::new(2, f64::MIN_POSITIVE).is_ok());
    assert!(OrderBook::new(8, 1e-320).is_ok());
    assert_eq!(DEFAULT_N, 2048);
    assert!(DEFAULT_N.is_power_of_two());
}

#[test]
fn new_book_is_empty() {
    let book = new_book(8, 0.5);
    assert_eq!(book.window(), 8);
    assert_eq!(book.tick_size(), 0.5);
    assert_eq!(book.price(3), 1.5);
    assert_eq!(book.best_bid_tick(), None);
    assert_eq!(book.best_ask_tick(), None);
    assert_eq!(book.qty(true, 1.0), None);
    assert_eq!(book.qty(false, 1.0), None);
    assert!(bids(&book).is_empty());
    assert!(asks(&book).is_empty());

    let wide = new_book(DEFAULT_N, 0.01);
    assert_eq!(wide.window(), DEFAULT_N);
}

#[test]
fn tick_rounds_half_away_from_zero() {
    let cases = [
        (0.0, 1.0, 0),
        (-0.0, 1.0, 0),
        (0.5, 1.0, 1),
        (-0.5, 1.0, -1),
        (1.5, 1.0, 2),
        (-1.5, 1.0, -2),
        (2.5, 1.0, 3),
        (-2.5, 1.0, -3),
        (0.05, 0.1, 1),
        (-0.05, 0.1, -1),
        (100.0, 0.01, 10_000),
        // Float64 division, not an exact decimal quotient. 0.15 / 0.1 is just
        // under 1.5, so the half-away rule lands on 1.
        (0.15, 0.1, 1),
        (0.25, 0.1, 3),
        (0.3, 0.1, 3),
        (1.25, 0.5, 3),
        (1.0, 0.1, 10),
    ];
    for (price, step, want) in cases {
        assert_eq!(tick(price, step), want, "tick({price}, {step})");
    }

    let just_under_half = f64::from_bits(0.5_f64.to_bits() - 1);
    assert_eq!(tick(just_under_half, 1.0), 0);
    assert_eq!(tick(-just_under_half, 1.0), 0);
    let just_past_neg_half = f64::from_bits((-0.5_f64).to_bits() + 1);
    assert_eq!(tick(just_past_neg_half, 1.0), -1);
}

#[test]
fn tick_conversion_matches_go_int64() {
    assert_eq!(tick(f64::NAN, 1.0), i64::MIN);
    assert_eq!(tick(f64::INFINITY, 1.0), i64::MIN);
    assert_eq!(tick(f64::NEG_INFINITY, 1.0), i64::MIN);
    assert_eq!(tick(1.0, 0.0), i64::MIN);
    assert_eq!(tick(-1.0, 0.0), i64::MIN);
    assert_eq!(tick(0.0, 0.0), i64::MIN);
    assert_eq!(tick(f64::MAX, 1.0), i64::MIN);
    assert_eq!(tick(f64::MIN, 1.0), i64::MIN);
    assert_eq!(tick(9_223_372_036_854_775_808.0, 1.0), i64::MIN);
    assert_eq!(tick(-9_223_372_036_854_775_808.0, 1.0), i64::MIN);

    let below_two_63 = f64::from_bits(9_223_372_036_854_775_808.0_f64.to_bits() - 1);
    assert_eq!(below_two_63, 9_223_372_036_854_774_784.0);
    assert_eq!(tick(below_two_63, 1.0), 9_223_372_036_854_774_784);
}

#[test]
fn out_of_window_level_is_dropped_and_does_not_clobber_the_best() {
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(100.0, 1.0), lv(93.0, 2.0)], &[]);
    assert_eq!(book.qty(true, 93.0), Some(2.0));
    // 100 - 92 is n ticks worse and shares the best's index.
    book.apply(&[lv(92.0, 9.0)], &[]);
    assert_eq!(book.qty(true, 92.0), None);
    assert_eq!(book.qty(true, 100.0), Some(1.0));
    assert_eq!(book.best_bid_tick(), Some(100));
}

#[test]
fn better_price_moves_the_best_on_each_side() {
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(100.0, 1.0)], &[lv(110.0, 1.0)]);
    book.apply(&[lv(105.0, 4.0)], &[lv(108.0, 3.0)]);
    assert_eq!(book.best_bid_tick(), Some(105));
    assert_eq!(book.best_ask_tick(), Some(108));
    assert_eq!(book.qty(true, 105.0), Some(4.0));
    assert_eq!(book.qty(false, 108.0), Some(3.0));
    assert_eq!(bids(&book), vec![(105, 4.0), (100, 1.0)]);
    assert_eq!(asks(&book), vec![(108, 3.0), (110, 1.0)]);
}

#[test]
fn aliased_slot_is_not_read_as_another_tick() {
    let mut book = new_book(8, 1.0);
    // Tick -3 lives at index 5. A later best at 6 puts tick 5, the same index, inside the window.
    book.apply(&[lv(0.0, 1.0), lv(-3.0, 7.0)], &[]);
    book.apply(&[lv(6.0, 2.0)], &[]);
    assert_eq!(book.qty(true, 5.0), None);
    assert_eq!(book.qty(true, -3.0), None);
    assert_eq!(bids(&book), vec![(6, 2.0), (0, 1.0)]);
}

#[test]
fn deleting_the_best_walks_past_gaps() {
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(100.0, 1.0), lv(99.0, 2.0), lv(97.0, 3.0)], &[]);
    book.apply(&[lv(99.0, 0.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(100));
    assert_eq!(book.qty(true, 99.0), None);

    book.apply(&[lv(100.0, 0.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(97));
    assert_eq!(book.qty(true, 98.0), None);
    assert_eq!(bids(&book), vec![(97, 3.0)]);

    book.apply(&[lv(97.0, 0.0)], &[]);
    assert_eq!(book.best_bid_tick(), None);
    assert!(bids(&book).is_empty());
}

#[test]
fn same_update_applies_the_better_price_first() {
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(100.0, 1.0), lv(99.0, 2.0)], &[]);
    // 92 is 8 ticks under 100, so it is outside until 100 is deleted. The
    // delete is the better price and is applied first; 92 is then one tick
    // inside the new best.
    book.apply(&[lv(92.0, 4.0), lv(100.0, 0.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(99));
    assert_eq!(book.qty(true, 92.0), Some(4.0));
    assert_eq!(bids(&book), vec![(99, 2.0), (92, 4.0)]);
}

#[test]
fn dropped_level_does_not_come_back_when_the_window_slides() {
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(100.0, 1.0), lv(99.0, 2.0)], &[]);
    book.apply(&[lv(92.0, 4.0)], &[]);
    assert_eq!(book.qty(true, 92.0), None);
    book.apply(&[lv(100.0, 0.0)], &[]);
    assert_eq!(book.qty(true, 92.0), None);
    book.apply(&[lv(92.0, 4.0)], &[]);
    assert_eq!(book.qty(true, 92.0), Some(4.0));

    // Written while the best was still nearby, then left behind, then covered
    // again: the slot was never cleared, so the size is still there.
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(100.0, 1.0), lv(99.0, 2.0)], &[]);
    book.apply(&[lv(108.0, 3.0)], &[]);
    assert_eq!(book.qty(true, 99.0), None);
    assert_eq!(book.qty(true, 100.0), None);
    book.apply(&[lv(108.0, 0.0)], &[]);
    assert_eq!(book.best_bid_tick(), None);
    book.apply(&[lv(106.0, 1.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(106));
    assert_eq!(book.qty(true, 99.0), Some(2.0));
    assert_eq!(bids(&book), vec![(106, 1.0), (99, 2.0)]);

    // A level that never fit is still absent after the window comes back.
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(100.0, 1.0), lv(92.0, 9.0)], &[]);
    book.apply(&[lv(100.0, 0.0)], &[]);
    book.apply(&[lv(98.0, 1.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(98));
    assert_eq!(book.qty(true, 92.0), None);
}

#[test]
fn ask_window_drops_prices_too_far_above_the_best() {
    let mut book = new_book(8, 1.0);
    book.apply(&[], &[lv(10.0, 1.0), lv(17.0, 2.0)]);
    book.apply(&[], &[lv(18.0, 3.0)]);
    assert_eq!(book.qty(false, 18.0), None);
    book.apply(&[], &[lv(9.0, 4.0)]);
    assert_eq!(book.best_ask_tick(), Some(9));
    assert_eq!(asks(&book), vec![(9, 4.0), (10, 1.0)]);

    let mut book = new_book(8, 1.0);
    book.apply(&[], &[lv(10.0, 1.0), lv(12.0, 2.0), lv(14.0, 3.0)]);
    book.apply(&[], &[lv(10.0, 0.0)]);
    assert_eq!(book.best_ask_tick(), Some(12));
    assert_eq!(asks(&book), vec![(12, 2.0), (14, 3.0)]);

    // The far edge of the bid window, 7 ticks under the best, survives a delete.
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(100.0, 1.0), lv(93.0, 6.0)], &[]);
    book.apply(&[lv(100.0, 0.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(93));
    assert_eq!(bids(&book), vec![(93, 6.0)]);
}

#[test]
fn iterator_is_best_first_and_honors_the_limit() {
    let mut book = new_book(16, 1.0);
    let levels: Vec<_> = (0..10)
        .map(|i| lv(100.0 - i as f64, (i + 1) as f64))
        .collect();
    book.apply(&levels, &[]);

    assert_eq!(
        book.bids(3).collect::<Vec<_>>(),
        vec![(100, 1.0), (99, 2.0), (98, 3.0)]
    );
    assert!(book.bids(0).next().is_none());
    assert!(book.bids(-5).next().is_none());
    assert_eq!(book.bids(100).count(), 10);

    let mut top = book.bids(1);
    assert_eq!(top.next(), Some((100, 1.0)));
    assert_eq!(top.next(), None);
    assert_eq!(top.next(), None);

    // A gap is not a level. The third occupied tick is 97, not 98.
    let mut gapped = new_book(8, 1.0);
    gapped.apply(&[lv(100.0, 1.0), lv(98.0, 2.0), lv(97.0, 3.0)], &[]);
    assert_eq!(
        gapped.bids(3).map(|(t, _)| t).collect::<Vec<_>>(),
        vec![100, 98, 97]
    );
}

#[test]
fn bad_sizes_delete_and_do_not_invent_a_level() {
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(4.0, -0.0)], &[]);
    assert_eq!(book.best_bid_tick(), None);

    book.apply(&[lv(10.0, 4.0)], &[]);
    book.apply(&[lv(10.0, 7.0)], &[]);
    assert_eq!(book.qty(true, 10.0), Some(7.0));

    book.apply(
        &[
            lv(10.0, -1.0),
            lv(9.0, f64::NAN),
            lv(8.0, f64::INFINITY),
            lv(7.0, -0.0),
        ],
        &[],
    );
    assert_eq!(book.best_bid_tick(), None);
    assert!(bids(&book).is_empty());

    let mut book = new_book(8, 1.0);
    book.apply(&[lv(10.0, 1.0)], &[]);
    book.apply(&[lv(12.0, 0.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(10));
    assert_eq!(book.qty(true, 10.0), Some(1.0));

    book.apply(&[], &[]);
    assert_eq!(book.best_bid_tick(), Some(10));

    book.apply(&[lv(3.0, 1e-320)], &[]);
    assert_eq!(book.qty(true, 3.0), Some(1e-320));
}

#[test]
fn equal_ticks_keep_input_order_and_the_later_one_wins() {
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(10.0, 1.0), lv(10.0, 5.0), lv(9.0, 3.0)], &[]);
    assert_eq!(book.qty(true, 10.0), Some(5.0));
    assert_eq!(book.qty(true, 9.0), Some(3.0));

    let mut book = new_book(8, 1.0);
    book.apply(&[lv(10.0, 5.0), lv(10.0, 1.0)], &[]);
    assert_eq!(book.qty(true, 10.0), Some(1.0));
}

#[test]
fn qty_looks_up_the_rounded_tick() {
    let mut book = new_book(8, 0.5);
    book.apply(&[lv(1.0, 4.0)], &[]);
    assert_eq!(tick(1.0, 0.5), 2);
    assert_eq!(tick(0.75, 0.5), 2);
    assert_eq!(tick(1.25, 0.5), 3);
    assert_eq!(book.qty(true, 1.0), Some(4.0));
    assert_eq!(book.qty(true, 0.75), Some(4.0));
    assert_eq!(book.qty(true, 1.25), None);
    assert_eq!(book.price(2), 1.0);
}

#[test]
fn single_slot_window_keeps_only_the_best() {
    let mut book = new_book(1, 1.0);
    book.apply(&[lv(5.0, 1.0), lv(4.0, 2.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(5));
    assert_eq!(book.qty(true, 4.0), None);
    book.apply(&[lv(9.0, 3.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(9));
    assert_eq!(book.qty(true, 5.0), None);
    assert_eq!(bids(&book), vec![(9, 3.0)]);
}

#[test]
fn long_update_uses_the_same_best_first_order() {
    // Price 1 is first in the slice and outside the window of 100. More than
    // 32 levels forces the allocating path. Best-first must drop 1 before it
    // is written, so a later window that would cover it still does not.
    let mut levels = vec![lv(1.0, 9.0)];
    for price in 37..=100 {
        levels.push(lv(price as f64, 1.0));
    }
    assert!(levels.len() > 32);

    let mut book = new_book(64, 1.0);
    book.apply(&levels, &[]);
    assert_eq!(book.best_bid_tick(), Some(100));
    assert_eq!(book.qty(true, 1.0), None);
    assert_eq!(book.qty(true, 37.0), Some(1.0));
    assert_eq!(book.qty(true, 100.0), Some(1.0));

    let deletes: Vec<_> = (38..=100).map(|price| lv(price as f64, 0.0)).collect();
    assert!(deletes.len() > 32);
    book.apply(&deletes, &[]);
    assert_eq!(book.best_bid_tick(), Some(37));
    assert_eq!(book.qty(true, 1.0), None);
    assert_eq!(book.qty(true, 37.0), Some(1.0));
}

#[test]
fn non_finite_prices_follow_go_int64_wrap() {
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(f64::NAN, 1.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(i64::MIN));
    assert_eq!(book.qty(true, f64::NAN), Some(1.0));

    // 10 - i64::MIN wraps to a negative distance, so the window check does
    // not drop the level. The iterator still only walks eight ticks from 10.
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(10.0, 1.0)], &[]);
    book.apply(&[lv(f64::NAN, 2.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(10));
    assert_eq!(book.qty(true, f64::NAN), Some(2.0));
    assert_eq!(bids(&book), vec![(10, 1.0)]);

    // The largest in-range tick and i64::MIN share index 0. The write lands,
    // but the best is not moved, so the side reads as empty.
    let mut book = new_book(8, 1.0);
    let max_tick = 9_223_372_036_854_774_784.0_f64;
    book.apply(&[lv(max_tick, 1.0)], &[]);
    assert_eq!(book.best_bid_tick(), Some(9_223_372_036_854_774_784));
    book.apply(&[lv(-9_223_372_036_854_775_808.0, 4.0)], &[]);
    assert_eq!(book.best_bid_tick(), None);
    assert_eq!(book.qty(true, -9_223_372_036_854_775_808.0), None);
}

#[test]
fn sides_stay_independent_when_the_book_crosses() {
    let mut book = new_book(8, 1.0);
    book.apply(&[lv(105.0, 1.0)], &[lv(100.0, 2.0)]);
    assert_eq!(book.best_bid_tick(), Some(105));
    assert_eq!(book.best_ask_tick(), Some(100));
    assert_eq!(bids(&book), vec![(105, 1.0)]);
    assert_eq!(asks(&book), vec![(100, 2.0)]);

    // A bid inside the bid window does not move the ask. A delete on an empty
    // ask does not invent a best ask.
    book.apply(&[lv(104.0, 3.0)], &[lv(9.0, 0.0)]);
    assert_eq!(book.best_bid_tick(), Some(105));
    assert_eq!(book.qty(true, 104.0), Some(3.0));
    assert_eq!(book.best_ask_tick(), Some(100));

    let mut fresh = new_book(8, 1.0);
    fresh.apply(&[lv(1.0, 1.0), lv(2.0, 0.0)], &[lv(9.0, 0.0)]);
    assert_eq!(fresh.best_bid_tick(), Some(1));
    assert_eq!(fresh.best_ask_tick(), None);
    assert_eq!(bids(&fresh), vec![(1, 1.0)]);
}

#[test]
fn small_apply_and_walk_do_not_allocate() {
    let mut book = new_book(16, 1.0);
    let seed: Vec<_> = (0..10)
        .map(|i| lv(100.0 - i as f64, (i + 1) as f64))
        .collect();
    book.apply(&seed, &[]);
    let inc = [lv(100.0, 2.0), lv(97.0, 1.0)];
    let allocs = allocations_during(|| {
        book.apply(&inc, &[]);
        for level in book.bids(3) {
            std::hint::black_box(level);
        }
    });
    assert_eq!(allocs, 0);

    let many: Vec<_> = (0..33).map(|i| lv(100.0 - i as f64, 1.0)).collect();
    let allocs = allocations_during(|| book.apply(&many, &[]));
    assert!(allocs > 0, "a 33-level apply allocates its sort buffers");
}
