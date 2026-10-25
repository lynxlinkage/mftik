//! Tick-indexed order book.
//!
//! One instrument, two fixed rings (bids and asks). A price is a tick,
//! `round(price / tick_size)` with halves away from zero, stored at
//! `tick & (n - 1)`. Slots are never cleared: a read is empty when the stored
//! tick is not the one being read, and a size of zero is a delete.
//!
//! [`OrderBook::apply`] orders each side from its own best price and keeps or
//! drops each level against the best tick as it stands at that moment. A level
//! `n` or more ticks worse than the best is outside the ring and is not
//! written. The window sliding later does not bring a dropped level back.
//!
//! This is the ring from mftiko `sdk/book`. Snapshot sync, gaps, and `md.view`
//! belong to bookpath. No IO.
//!
//! ```
//! use mftik_core::book::{tick, Level, OrderBook};
//!
//! let mut book = OrderBook::new(8, 1.0)?;
//! book.apply(
//!     &[
//!         Level { price: 100.0, size: 1.0 },
//!         Level { price: 99.0, size: 2.0 },
//!     ],
//!     &[],
//! );
//! assert_eq!(book.best_bid_tick(), Some(tick(100.0, 1.0)));
//! # Ok::<(), mftik_core::book::Error>(())
//! ```

/// Ring length used when the caller does not choose one.
///
/// Not a venue snapshot depth. PROTOCOL §9.5 uses this same default as the
/// MD conn worker.
pub const DEFAULT_N: usize = 2048;

/// How many levels [`Side::apply`] orders without a heap allocation.
/// A snapshot is larger than this and allocates; an increment is not.
const STACK_N: usize = 32;

/// `-2^63`. The most negative finite value that still fits in `i64`.
const I64_MIN_F64: f64 = -9223372036854775808.0;

/// `2^63`. The first magnitude that does not fit in `i64`.
const I64_OVERFLOW_F64: f64 = 9223372036854775808.0;

/// Why [`OrderBook::new`] refused a book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// `n` is zero, not a power of two, or does not fit in `i64`.
    Window,
    /// Tick size is not finite and strictly positive.
    TickSize,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Window => "book: n must be a power of two",
            Self::TickSize => "book: tick size must be positive",
        })
    }
}

impl std::error::Error for Error {}

/// One price update. A size of zero deletes the level.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

/// One ring entry.
///
/// There is no validity flag. Slots are never cleared.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Slot {
    pub tick: i64,
    pub size: f64,
}

/// Round `price / tick_size` to a tick, half away from zero.
///
/// The quotient is rounded with [`f64::round`], then converted the way Go's
/// amd64 and arm64 `int64(math.Round(...))` converts: a non-finite value, or a
/// value outside the `i64` range, becomes [`i64::MIN`]. `-2^63` itself is in
/// range. [`OrderBook::new`] rejects a tick size that is not finite and
/// positive; this function does not, matching `sdk/book.Tick`.
#[inline]
#[must_use]
pub fn tick(price: f64, tick_size: f64) -> i64 {
    round_to_i64(price / tick_size)
}

#[inline]
fn round_to_i64(value: f64) -> i64 {
    let rounded = value.round();
    if !rounded.is_finite() || rounded >= I64_OVERFLOW_F64 || rounded < I64_MIN_F64 {
        i64::MIN
    } else {
        rounded as i64
    }
}

/// One instrument's book. Bids and asks each have their own ring and best tick.
#[derive(Clone)]
pub struct OrderBook {
    bid_side: Side,
    ask_side: Side,
    tick_size: f64,
}

impl OrderBook {
    /// Build a book of `n` slots per side.
    ///
    /// `n` must be a power of two and at least 1, and must fit in `i64`
    /// (Go's ring length is an `int`, and the window check compares against
    /// it). `tick_size` must be finite and strictly positive.
    #[inline]
    pub fn new(n: usize, tick_size: f64) -> Result<Self, Error> {
        if n == 0 || !n.is_power_of_two() || i64::try_from(n).is_err() {
            return Err(Error::Window);
        }
        if !(tick_size.is_finite() && tick_size > 0.0) {
            return Err(Error::TickSize);
        }
        Ok(Self {
            bid_side: Side::new(n),
            ask_side: Side::new(n),
            tick_size,
        })
    }

    /// Ring length. The most levels one side can hold.
    #[inline]
    #[must_use]
    pub fn window(&self) -> usize {
        self.bid_side.n
    }

    /// Price grid this book was built with.
    #[inline]
    #[must_use]
    pub fn tick_size(&self) -> f64 {
        self.tick_size
    }

    /// Grid price of a tick: `tick * tick_size`.
    #[inline]
    #[must_use]
    pub fn price(&self, tick: i64) -> f64 {
        (tick as f64) * self.tick_size
    }

    /// Best bid tick, if that side has a level.
    #[inline]
    #[must_use]
    pub fn best_bid_tick(&self) -> Option<i64> {
        self.bid_side.best_tick()
    }

    /// Best ask tick, if that side has a level.
    #[inline]
    #[must_use]
    pub fn best_ask_tick(&self) -> Option<i64> {
        self.ask_side.best_tick()
    }

    /// Size at `price`, when that tick is inside the current window and the
    /// slot holds it.
    ///
    /// `bid` selects the side. The price is mapped with [`tick`].
    #[inline]
    #[must_use]
    pub fn qty(&self, bid: bool, price: f64) -> Option<f64> {
        let tick = tick(price, self.tick_size);
        if bid {
            self.bid_side.qty(tick, true)
        } else {
            self.ask_side.qty(tick, false)
        }
    }

    /// Write one increment.
    ///
    /// Each side is applied from its own best price first. Each level is kept
    /// or dropped against the best tick at that moment, so a delete of the
    /// best in the same update can pull a worse level into the window. The
    /// caller's slice order is not changed. Bids are applied before asks; the
    /// sides do not interact.
    #[inline]
    pub fn apply(&mut self, bids: &[Level], asks: &[Level]) {
        self.bid_side.apply(bids, self.tick_size, true);
        self.ask_side.apply(asks, self.tick_size, false);
    }

    /// Bids from the best outward.
    ///
    /// At most `limit` occupied levels, and at most one window. A non-positive
    /// limit yields nothing.
    #[inline]
    #[must_use]
    pub fn bids(&self, limit: i64) -> Iter<'_> {
        self.bid_side.iter(limit, true)
    }

    /// Asks from the best outward.
    ///
    /// At most `limit` occupied levels, and at most one window. A non-positive
    /// limit yields nothing.
    #[inline]
    #[must_use]
    pub fn asks(&self, limit: i64) -> Iter<'_> {
        self.ask_side.iter(limit, false)
    }
}

impl std::fmt::Debug for OrderBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrderBook")
            .field("window", &self.window())
            .field("tick_size", &self.tick_size)
            .field("best_bid", &self.best_bid_tick())
            .field("best_ask", &self.best_ask_tick())
            .finish()
    }
}

/// Occupied levels from the best outward.
///
/// [`Iterator::next`] does not allocate. The walk stops after `limit` occupied
/// levels or after one window of ticks, whichever comes first.
#[derive(Clone)]
pub struct Iter<'a> {
    slots: &'a [Slot],
    mask: u64,
    tick: i64,
    step: i64,
    need: i64,
    span: i64,
}

impl Iterator for Iter<'_> {
    type Item = (i64, f64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.need > 0 && self.span > 0 {
            let slot = self.slots[self.index()];
            let current = self.tick;
            self.tick = self.tick.wrapping_add(self.step);
            self.span -= 1;
            if slot.tick == current && slot.size > 0.0 {
                self.need -= 1;
                return Some((current, slot.size));
            }
        }
        None
    }
}

impl std::iter::FusedIterator for Iter<'_> {}

impl Iter<'_> {
    #[inline]
    fn index(&self) -> usize {
        (self.tick as u64 & self.mask) as usize
    }

    fn empty(side: &Side) -> Iter<'_> {
        Iter {
            slots: &side.slots,
            mask: side.mask,
            tick: 0,
            step: 0,
            need: 0,
            span: 0,
        }
    }
}

#[derive(Clone)]
struct Side {
    slots: Vec<Slot>,
    best: i64,
    n: usize,
    mask: u64,
}

impl Side {
    fn new(n: usize) -> Self {
        Self {
            slots: vec![Slot { tick: 0, size: 0.0 }; n],
            best: 0,
            n,
            mask: (n as u64) - 1,
        }
    }

    #[inline]
    fn index(&self, tick: i64) -> usize {
        (tick as u64 & self.mask) as usize
    }

    #[inline]
    fn has_best(&self) -> bool {
        let slot = &self.slots[self.index(self.best)];
        slot.tick == self.best && slot.size > 0.0
    }

    #[inline]
    fn best_tick(&self) -> Option<i64> {
        if self.has_best() {
            Some(self.best)
        } else {
            None
        }
    }

    #[inline]
    fn write(&mut self, tick: i64, size: f64) {
        let index = self.index(tick);
        self.slots[index] = Slot { tick, size };
    }

    fn apply(&mut self, levels: &[Level], tick_size: f64, bid: bool) {
        let n = levels.len();
        if n == 0 {
            return;
        }
        if n > STACK_N {
            let mut ticks = vec![0_i64; n];
            let mut idx = vec![0_usize; n];
            self.apply_sorted(levels, tick_size, bid, &mut ticks, &mut idx);
            return;
        }
        let mut ticks = [0_i64; STACK_N];
        let mut idx = [0_usize; STACK_N];
        self.apply_sorted(levels, tick_size, bid, &mut ticks[..n], &mut idx[..n]);
    }

    /// Order indexes so the better price is applied first. Equal ticks keep
    /// the caller's order, and the later one overwrites.
    fn apply_sorted(
        &mut self,
        levels: &[Level],
        tick_size: f64,
        bid: bool,
        ticks: &mut [i64],
        idx: &mut [usize],
    ) {
        let n = levels.len();
        debug_assert_eq!(ticks.len(), n);
        debug_assert_eq!(idx.len(), n);
        for (i, level) in levels.iter().enumerate() {
            ticks[i] = tick(level.price, tick_size);
            idx[i] = i;
        }
        for i in 1..n {
            let v = idx[i];
            let mut j = i;
            while j > 0 && worse(bid, ticks[idx[j - 1]], ticks[v]) {
                idx[j] = idx[j - 1];
                j -= 1;
            }
            idx[j] = v;
        }
        for &i in idx.iter() {
            self.apply_tick(ticks[i], levels[i].size, bid);
        }
    }

    /// Store one level against the best tick as it stands now.
    ///
    /// A price better than the best moves the best. A price `n` or more ticks
    /// worse is outside the ring and is not written: that tick shares an index
    /// with the best. Distance uses wrapping subtraction, matching Go `int64`.
    #[inline]
    fn apply_tick(&mut self, tick: i64, mut size: f64, bid: bool) {
        if size < 0.0 || size.is_nan() || size.is_infinite() {
            size = 0.0;
        }
        if !self.has_best() {
            if size == 0.0 {
                return;
            }
            self.write(tick, size);
            self.best = tick;
            return;
        }
        if better(bid, tick, self.best) {
            if size == 0.0 {
                return;
            }
            self.write(tick, size);
            self.best = tick;
            return;
        }
        if tick != self.best {
            let dist = if bid {
                self.best.wrapping_sub(tick)
            } else {
                tick.wrapping_sub(self.best)
            };
            if dist >= self.n as i64 {
                return;
            }
        }
        self.write(tick, size);
        if tick == self.best && size == 0.0 {
            self.retreat(bid);
        }
    }

    /// Next occupied tick after the best was deleted. The old best is left in
    /// place when the rest of the window is empty; [`Self::has_best`] is then
    /// false because that slot's size is zero.
    fn retreat(&mut self, bid: bool) {
        let step = if bid { -1 } else { 1 };
        let mut t = self.best.wrapping_add(step);
        for _ in 1..self.n {
            let slot = &self.slots[self.index(t)];
            if slot.tick == t && slot.size > 0.0 {
                self.best = t;
                return;
            }
            t = t.wrapping_add(step);
        }
    }

    #[inline]
    fn qty(&self, tick: i64, bid: bool) -> Option<f64> {
        if !self.has_best() || better(bid, tick, self.best) {
            return None;
        }
        let dist = if bid {
            self.best.wrapping_sub(tick)
        } else {
            tick.wrapping_sub(self.best)
        };
        if dist >= self.n as i64 {
            return None;
        }
        let slot = &self.slots[self.index(tick)];
        if slot.tick != tick || slot.size <= 0.0 {
            return None;
        }
        Some(slot.size)
    }

    fn iter(&self, limit: i64, bid: bool) -> Iter<'_> {
        if !self.has_best() || limit <= 0 {
            return Iter::empty(self);
        }
        let n = self.n as i64;
        let limit = limit.min(n);
        let step = if bid { -1 } else { 1 };
        Iter {
            slots: &self.slots,
            mask: self.mask,
            tick: self.best,
            step,
            need: limit,
            span: n,
        }
    }
}

#[inline]
fn better(bid: bool, tick: i64, best: i64) -> bool {
    if bid { tick > best } else { tick < best }
}

/// Whether `a` should be applied after `b` (b is the better price).
#[inline]
fn worse(bid: bool, a: i64, b: i64) -> bool {
    better(bid, b, a)
}

#[cfg(test)]
mod tests;
