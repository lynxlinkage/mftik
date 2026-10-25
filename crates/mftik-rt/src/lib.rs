//! Session worker runtime.
//!
//! [`run`] hosts a [`Strategy`]: it joins fd 3, the C4 state machine, the
//! dispatcher, and NATS into a session worker (PROTOCOL §5, §9; E03-D001,
//! E03-D002). The protocol logic, C4, the dispatcher, and the MD, TD, and SYM
//! paths, are sans-IO machines in `mftik-core`; this crate does their IO.
//!
//! - [`Strategy`], the optional hook traits, and [`Context`]: what E04 binds,
//!   mirroring mftiko `sdk/strategy.go` and `sdk/context.go`.
//! - [`run`] (in `session`): the threads and the C4 driver. The dispatcher
//!   runs on the thread that calls `run` (E03-D003). Every exit seals fd 3
//!   and calls `_exit` ([`exit`], E03-D005).
//! - [`clock`]: the injected `now_ns` (E03-D004). [`timers`]: one timer
//!   table on the IO thread. [`io`]: the tokio `current_thread` runtime.
//! - [`nats`]: the connection with the session's credentials, retried inside
//!   `deadline_ms` (E03-D006, E08-D050). [`call`]: a plane call's context.
//! - [`volume`]: the strategy's atomic volume writes and read order
//!   (§9.1.4).
//! - [`frame`], [`ctl`], [`reader`], [`beat`]: C4 frames on fd 3 and the
//!   Beat thread (E03-D031–E03-D033).
//!
//! The MD path is in `md` (E03-06). The TD and SYM paths are wired by
//! E03-07 and E03-08 and Report by E03-09; the plane's calls into the paths
//! are in `wiring`.

pub mod beat;
pub mod call;
pub mod clock;
mod context;
pub mod ctl;
mod dcell;
pub mod exit;
pub mod frame;
pub mod io;
mod md;
pub mod nats;
mod plane;
pub mod reader;
mod session;
mod strategy;
pub mod timers;
pub mod volume;
mod wiring;

pub use context::{Context, Error, Poster};
pub use md::BookView;
pub use mftik_core::book::OrderBook;
pub use mftik_core::mdpath::{ReadError, TapeQuery};
pub use mftik_core::symbol::Info;
pub use plane::{CLOCK_TICK, GATE_LINE, INPUT_DIR_ENV, VOLUME_DIR_ENV};
pub use session::{RELEASE, run};
pub use strategy::{
    AggTrade, AggTradeHandler, BalanceEvent, BalanceHandler, BestQuote, BestQuoteHandler,
    BookHandler, BookUpdate, Feed, FillEvent, FillHandler, FundingRate, FundingRateHandler, Greeks,
    GreeksHandler, Handlers, HookError, Kline, KlineHandler, Liquidation, LiquidationHandler,
    MarkPrice, MarkPriceHandler, Md, OpenInterest, OpenInterestHandler, Order, OrderEvent,
    OrderHandler, Origin, PositionEvent, PositionHandler, ReconEvent, ReconHandler, RejectEvent,
    RejectHandler, RejectedRequest, Strategy, Td, Ticker, TickerHandler, Trade, TradeHandler,
    topic,
};
