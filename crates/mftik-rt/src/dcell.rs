//! State that only the dispatcher thread touches (E03-D064, E03-D067).
//!
//! The dispatcher runs hooks and the TD-buffer entries that drive bookpath one
//! at a time on one thread (E08-D003), so that state needs no lock, as in Go.
//! Rust still needs the owning struct to be `Sync`, because the IO thread
//! shares it for other fields. [`DispatcherCell`] gives that without a lock:
//! it pins itself to the first thread that touches it and panics on any
//! other, and a `RefCell` inside turns an overlapping borrow (a hook holding
//! a book while something mutates the books) into a panic at that line
//! instead of a deadlock that the watchdog would report as HUNG.

use std::cell::{Ref, RefCell, RefMut};
use std::fmt;
use std::sync::OnceLock;
use std::thread::{self, ThreadId};

/// A `RefCell` owned by one thread, the dispatcher's. See the module docs.
pub(crate) struct DispatcherCell<T> {
    owner: OnceLock<ThreadId>,
    cell: RefCell<T>,
}

// SAFETY: every access to `cell` goes through `check`, which binds the cell to
// the first thread that touches it and panics on every other thread before
// the `RefCell` is reached. So the `RefCell` is only ever used from one
// thread, which is what its `!Sync` guards against. `T: Send` lets the cell
// be created and dropped on another thread.
unsafe impl<T: Send> Sync for DispatcherCell<T> {}

impl<T> DispatcherCell<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            owner: OnceLock::new(),
            cell: RefCell::new(value),
        }
    }

    /// Shared access. Panics off the owning thread, or while a
    /// [`DispatcherCell::borrow_mut`] is alive.
    pub(crate) fn borrow(&self) -> Ref<'_, T> {
        self.check();
        self.cell.borrow()
    }

    /// Exclusive access. Panics off the owning thread, or while any borrow is
    /// alive.
    pub(crate) fn borrow_mut(&self) -> RefMut<'_, T> {
        self.check();
        self.cell.borrow_mut()
    }

    fn check(&self) {
        let me = thread::current().id();
        let owner = *self.owner.get_or_init(|| me);
        assert!(
            owner == me,
            "dispatcher-only state touched from {:?} ({me:?}); it belongs to {owner:?}",
            thread::current().name(),
        );
    }
}

impl<T> fmt::Debug for DispatcherCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DispatcherCell")
            .field("owner", &self.owner.get())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn the_first_thread_owns_it() {
        let cell = Arc::new(DispatcherCell::new(1));
        *cell.borrow_mut() += 1;
        assert_eq!(*cell.borrow(), 2);
        let other = Arc::clone(&cell);
        let off =
            thread::spawn(move || catch_unwind(AssertUnwindSafe(|| *other.borrow())).is_err());
        assert!(off.join().expect("join"), "a second thread must panic");
        assert_eq!(*cell.borrow(), 2);
    }

    #[test]
    fn an_owner_is_not_taken_by_creating_or_dropping() {
        let cell = thread::spawn(|| DispatcherCell::new(String::from("made elsewhere")))
            .join()
            .expect("join");
        assert_eq!(cell.borrow().as_str(), "made elsewhere");
        drop(cell);
    }

    #[test]
    fn mutating_while_a_view_is_held_panics_instead_of_blocking() {
        let cell = DispatcherCell::new(0);
        let view = cell.borrow();
        assert!(catch_unwind(AssertUnwindSafe(|| *cell.borrow_mut() = 1)).is_err());
        assert_eq!(*view, 0);
    }
}
