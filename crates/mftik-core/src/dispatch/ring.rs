//! MD ring: one preallocated buffer of variable-length messages (E08-D016).
//!
//! Live messages are packed at the front. The message a hook is reading stays
//! pinned at `buf[..pin_n]` until the hook returns; those bytes are not moved
//! and not overwritten. A push that does not fit drops the oldest messages
//! that are not pinned, sliding the rest down, until it fits. If the pinned
//! message still leaves too little room, or the new message is larger than
//! the whole ring, the new message is dropped and counted. Dropping never
//! blocks the caller.
//!
//! The bytes live in [`UnsafeCell`] so the dispatcher can hand out the pinned
//! prefix while another thread copies a new message into the free suffix.
//! Those ranges do not overlap, and the pinned prefix is not written until
//! the hook returns.

use std::cell::UnsafeCell;

use super::MD_BYTES;

#[derive(Clone, Copy)]
struct MdMsg {
    n: usize,
    book: bool,
}

pub(crate) struct Ring {
    buf: Box<[UnsafeCell<u8>]>,
    msgs: Vec<MdMsg>,
    pinned: bool,
    pin_n: usize,
    used: usize,
    dropped: u64,
}

impl Ring {
    pub(crate) fn new(bytes: usize) -> Self {
        let bytes = if bytes == 0 { MD_BYTES } else { bytes };
        Self {
            buf: (0..bytes).map(|_| UnsafeCell::new(0)).collect(),
            msgs: Vec::new(),
            pinned: false,
            pin_n: 0,
            used: 0,
            dropped: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.dropped
    }

    pub(crate) fn is_pinned(&self) -> bool {
        self.pinned
    }

    /// Copies `msg` into the ring. `false` means `msg` was dropped and counted.
    pub(crate) fn push(&mut self, msg: &[u8], book: bool) -> bool {
        if msg.len() > self.buf.len() {
            self.dropped += 1;
            return false;
        }
        while self.free() < msg.len() {
            if !self.drop_unpinned() {
                self.dropped += 1;
                return false;
            }
        }
        let end = self.used + msg.len();
        // SAFETY: `used..end` is the free suffix. It does not overlap the pinned
        // prefix, and this call holds the dispatcher lock, so no other push
        // writes it. The hook's shared slice covers only `buf[..pin_n]`.
        unsafe {
            std::ptr::copy_nonoverlapping(msg.as_ptr(), self.buf_ptr().add(self.used), msg.len());
        }
        self.used = end;
        self.msgs.push(MdMsg { n: msg.len(), book });
        true
    }

    fn free(&self) -> usize {
        self.buf.len() - self.used
    }

    /// Drops the oldest message that is not pinned and slides the later bytes down.
    /// The pinned prefix stays where it is. `false` means there is no such message.
    fn drop_unpinned(&mut self) -> bool {
        let (idx, off) = if self.pinned {
            if self.msgs.len() < 2 {
                return false;
            }
            (1, self.pin_n)
        } else if self.msgs.is_empty() {
            return false;
        } else {
            (0, 0)
        };
        let n = self.msgs[idx].n;
        self.msgs.remove(idx);
        let from = off + n;
        debug_assert!(from <= self.used);
        // SAFETY: `from..used` and the destination `off..` are the unpinned
        // suffix. The pinned prefix, if any, starts at 0 and ends at `off`
        // when a message is pinned. This call holds the dispatcher lock.
        unsafe {
            std::ptr::copy(
                self.buf_ptr().add(from),
                self.buf_ptr().add(off),
                self.used - from,
            );
        }
        self.used -= n;
        self.dropped += 1;
        true
    }

    /// Pins the oldest message. The bytes are `buf[..n]` and stay put until [`release`](Self::release).
    pub(crate) fn peek(&mut self) -> Option<(bool, usize)> {
        if self.pinned || self.msgs.is_empty() {
            return None;
        }
        self.pinned = true;
        self.pin_n = self.msgs[0].n;
        Some((self.msgs[0].book, self.pin_n))
    }

    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.buf_ptr()
    }

    fn buf_ptr(&self) -> *mut u8 {
        self.buf.as_ptr().cast_mut().cast()
    }

    pub(crate) fn release(&mut self) {
        if !self.pinned {
            return;
        }
        let n = self.pin_n;
        if !self.msgs.is_empty() {
            self.msgs.remove(0);
        }
        // SAFETY: the pinned prefix is no longer borrowed. The caller releases
        // it only after the hook's slice has ended. This call holds the lock.
        unsafe {
            std::ptr::copy(self.buf_ptr().add(n), self.buf_ptr(), self.used - n);
        }
        self.used -= n;
        self.pinned = false;
        self.pin_n = 0;
    }

    /// Drops every message, including a pinned one. The caller must already be done with the slice.
    pub(crate) fn discard(&mut self) {
        self.msgs.clear();
        self.used = 0;
        self.pinned = false;
        self.pin_n = 0;
    }

    /// Messages not currently pinned.
    pub(crate) fn waiting(&self) -> usize {
        let mut n = self.msgs.len();
        if self.pinned && n > 0 {
            n -= 1;
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(fill: u8, n: usize) -> Vec<u8> {
        vec![fill; n]
    }

    fn prefix(ring: &Ring, n: usize) -> Vec<u8> {
        // SAFETY: ring tests do not share the buffer with another thread.
        unsafe { std::slice::from_raw_parts(ring.as_ptr(), n).to_vec() }
    }

    #[test]
    fn drops_the_oldest_message_when_the_next_one_does_not_fit() {
        let mut ring = Ring::new(50);
        assert!(ring.push(&bytes(b'a', 20), false));
        assert!(ring.push(&bytes(b'b', 20), false));
        assert!(ring.push(&bytes(b'c', 20), false));
        assert_eq!(ring.dropped(), 1);
        assert_eq!(ring.waiting(), 2);
        let (_, n) = ring.peek().expect("oldest");
        assert_eq!(prefix(&ring, n), bytes(b'b', 20));
        ring.release();
        let (_, n) = ring.peek().expect("next");
        assert_eq!(prefix(&ring, n), bytes(b'c', 20));
    }

    #[test]
    fn drops_several_oldest_messages_to_make_room() {
        let mut ring = Ring::new(50);
        assert!(ring.push(&bytes(b'a', 20), false));
        assert!(ring.push(&bytes(b'b', 20), false));
        assert!(ring.push(&bytes(b'c', 10), false));
        assert!(ring.push(&bytes(b'd', 30), false));
        assert_eq!(ring.dropped(), 2);
        assert_eq!(ring.waiting(), 2);
        let (_, n) = ring.peek().expect("oldest");
        assert_eq!(prefix(&ring, n), bytes(b'c', 10));
        ring.release();
        let (_, n) = ring.peek().expect("next");
        assert_eq!(prefix(&ring, n), bytes(b'd', 30));
    }

    #[test]
    fn a_pinned_message_is_not_dropped_or_moved() {
        let mut ring = Ring::new(50);
        assert!(ring.push(&bytes(b'a', 20), false));
        assert!(ring.push(&bytes(b'b', 20), false));
        let (_, n) = ring.peek().expect("pin");
        let held = prefix(&ring, n);
        assert!(ring.push(&bytes(b'c', 20), false));
        assert_eq!(ring.dropped(), 1);
        assert_eq!(prefix(&ring, n), held);
        assert_eq!(held, bytes(b'a', 20));
        assert_eq!(ring.waiting(), 1);
        ring.release();
        let (_, n) = ring.peek().expect("after pin");
        assert_eq!(prefix(&ring, n), bytes(b'c', 20));
    }

    #[test]
    fn a_pin_that_leaves_no_room_drops_the_new_message() {
        let mut ring = Ring::new(50);
        assert!(ring.push(&bytes(b'a', 40), false));
        assert!(ring.peek().is_some());
        assert!(!ring.push(&bytes(b'b', 20), false));
        assert_eq!(ring.dropped(), 1);
        assert_eq!(ring.waiting(), 0);
        assert!(ring.is_pinned());
    }

    #[test]
    fn a_message_larger_than_the_ring_is_dropped_and_an_exact_fit_is_kept() {
        let mut ring = Ring::new(50);
        assert!(!ring.push(&bytes(b'z', 51), false));
        assert!(ring.push(&bytes(b'a', 50), false));
        assert_eq!(ring.dropped(), 1);
        assert_eq!(ring.waiting(), 1);
        let (book, n) = ring.peek().expect("exact");
        assert!(!book);
        assert_eq!(n, 50);
    }

    #[test]
    fn book_flag_survives_the_queue() {
        let mut ring = Ring::new(30);
        assert!(ring.push(&bytes(b'a', 10), true));
        assert!(ring.push(&bytes(b'b', 10), false));
        let (book, _) = ring.peek().expect("book");
        assert!(book);
        ring.release();
        let (book, _) = ring.peek().expect("md");
        assert!(!book);
    }

    #[test]
    fn discard_forgets_a_pinned_message_without_counting_another_drop() {
        let mut ring = Ring::new(40);
        assert!(ring.push(&bytes(b'a', 10), false));
        assert!(ring.push(&bytes(b'b', 10), false));
        assert!(ring.peek().is_some());
        ring.discard();
        assert_eq!(ring.waiting(), 0);
        assert!(!ring.is_pinned());
        assert_eq!(ring.dropped(), 0);
        assert!(ring.peek().is_none());
    }

    #[test]
    fn zero_selects_the_32_mib_session_ring() {
        let ring = Ring::new(0);
        assert_eq!(ring.capacity(), MD_BYTES);
        assert_eq!(MD_BYTES, 32 << 20);
    }
}
