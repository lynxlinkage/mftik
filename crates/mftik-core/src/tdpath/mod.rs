//! Session path to TD: OMS, ledger, and recon (PROTOCOL §9.4, §9.6).
//!
//! This is a pure state machine (E02-D001). The caller feeds account events,
//! order replies, recon replies, and backoff ticks. The machine returns the
//! actions to perform: subscribe, send `td.order` / `td.view`, arm the recon
//! backoff, deliver a hook, or fail the session. NATS and timers stay in E03.
//!
//! # Driving it
//!
//! Hooks see state that already includes the event being delivered. A terminal
//! order is still returned by [`Path::order`] for the duration of that hook,
//! and [`Path::open_orders`] already omits it. Call [`Path::end_hook`] when the
//! strategy returns. That drops the terminal order and continues a recon
//! release. One release step is one hook, in watermark order.
//!
//! ```text
//! let effects = path.on_account_event(api_id, event);
//! dispatch(effects);
//!
//! fn dispatch(effects: Vec<Effect>) {
//!     for effect in effects {
//!         match effect {
//!             Effect::Hook(hook) => {
//!                 strategy(hook); // may call submit / cancel / reads
//!                 dispatch_one(path.end_hook());
//!             }
//!             other => perform_io(other),
//!         }
//!     }
//! }
//! ```
//!
//! Do not feed another `td.evt`, order reply, or recon reply until the current
//! hook has returned and `end_hook` has been called. Submit, cancel, batch
//! cancel, request recon, and the reads are the calls a hook may make.
//! `SendOrder` effects are already in publish order; E03 publishes each one
//! before the next and lets the replies overlap. Match a reply to its
//! [`RoundTicket`].
//!
//! A `Fail` or [`Path::close`] drops outstanding waits. Later replies and
//! backoff ticks are ignored.
//!
//! With the event log on (PROTOCOL §12), feed each `td.evt` through
//! [`Path::on_account_event_in`] with its `td_event` input and call
//! [`Path::take_input`] for every hook: the input of the event that hook
//! delivers, held or live, comes back with it (E07-D007). An event that is
//! dropped, or runs no strategy hook, never hands its input back.
//!
//! # Client order ids
//!
//! [`Path::submit`] asks a [`ClientOrderIds`] for the next id. The Go SDK calls
//! `cloidgen.Next` for that, one counter per process (PROTOCOL §9.4, E08-D014).
//! The counter is E02-05. [`encode`] is the platform layout; OMS reads the algo
//! field back out of an id.

mod path;

#[cfg(test)]
mod tests;

pub use crate::cloid::encode;
pub use path::{
    AccountDecl, BalanceEvent, Bound, ClientOrderIdError, ClientOrderIds, Config, Effect, Error,
    FillEvent, HOLD_CAP, Hook, NewOrder, OpenError, OrderEvent, Origin, Path, PositionEvent,
    REASON_HOLD_FULL, REASON_OFFLINE, RECON_BACKOFF_MAX, RECON_BACKOFF_MIN, ReconEvent,
    ReconOutcome, RejectEvent, RejectedRequest, RoundOutcome, RoundTicket, Submitted, Td,
    evt_subject, order_subject, view_subject,
};
