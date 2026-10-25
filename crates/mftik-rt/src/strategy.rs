//! The Strategy Actor a session worker hosts, as mftiko `sdk/strategy.go`
//! (E00-D001, E08-D003, E08-D005).
//!
//! [`Strategy`] has the three hooks every strategy has. The others are
//! optional, one trait each, like Go's handler interfaces. Go finds them with
//! a type assertion; Rust has none, so a strategy names the ones it
//! implements in [`Strategy::handlers`]. Each registration requires the
//! trait, so a strategy cannot register a hook it does not implement. A topic
//! no registered hook takes is skipped before it is decoded (E08-D005).
//!
//! ```
//! use mftik_rt::{BestQuote, BestQuoteHandler, Context, Handlers, HookError, Strategy};
//!
//! struct Quoter;
//!
//! impl Strategy for Quoter {
//!     fn on_init(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> { Ok(()) }
//!     fn on_start(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> { Ok(()) }
//!     fn on_stop(&mut self, _ctx: &Context<Self>) {}
//!     fn handlers(&self, h: &mut Handlers<Self>) {
//!         h.best_quote();
//!     }
//! }
//!
//! impl BestQuoteHandler for Quoter {
//!     fn on_best_quote(&mut self, ctx: &Context<Self>, ev: BestQuote) {
//!         let _ = (ctx, ev.md.uid);
//!     }
//! }
//! ```
//!
//! Hooks run one at a time on the dispatcher, which is the thread that called
//! [`crate::run`] (E03-D003), so a strategy needs no locks. Before a hook
//! runs, its event is already in the state [`Context`] reads (E08-D005,
//! E08-D053–E08-D056).

use mftik_core::mdpath;
use mftik_proto::wire::mftiko::md::v1 as md;

use crate::context::Context;

pub use mftik_core::mdpath::Md;
/// A new order, in the units of `td.Submit` (Go `sdk.Order`). `None`, or
/// NaN, leaves an optional number unset.
pub use mftik_core::tdpath::NewOrder as Order;
pub use mftik_core::tdpath::{
    BalanceEvent, FillEvent, OrderEvent, Origin, PositionEvent, ReconEvent, RejectEvent,
    RejectedRequest, Td,
};

/// What `on_init` and `on_start` return on failure. The text becomes
/// `Done.error` and `Exiting.reason`, so it must not carry secrets.
pub type HookError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A session's strategy (Go `sdk.Strategy`).
pub trait Strategy: Send + Sized + 'static {
    /// Runs after spawn, before market data or accounts are ready. Parse
    /// params, resolve the instruments the strategy trades
    /// ([`Context::resolve`]), read state from the volume or inputs. An error
    /// ends the session `INIT_FAILED`.
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError>;

    /// Runs once every MD and TD lease is READY. Orders may be placed from
    /// here on. An error ends the session `INIT_FAILED`, and `on_stop` runs.
    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError>;

    /// Runs when the session ends `DONE`, `FAILED`, or `CRASHED`, not after
    /// `HUNG` (PROTOCOL §5.3). On `DONE` and `FAILED` the TD events already
    /// received are delivered first (E08-D003).
    fn on_stop(&mut self, ctx: &Context<Self>);

    /// Registers the optional hooks this strategy implements. Called once,
    /// before `on_init`. The default registers none.
    fn handlers(&self, handlers: &mut Handlers<Self>) {
        let _ = handlers;
    }
}

/// One market data event: the envelope and the decoded payload, which the
/// strategy owns (Go `sdk.Feed`).
#[derive(Clone, Debug, PartialEq)]
pub struct Feed<T> {
    pub md: Md,
    pub data: T,
}

pub type BestQuote = Feed<md::BestQuote>;
pub type Trade = Feed<md::Trade>;
pub type AggTrade = Feed<md::AggTrade>;
/// Every `kline_<interval>` topic; the interval is in `data`.
pub type Kline = Feed<md::Kline>;
pub type Ticker = Feed<md::Ticker>;
pub type Liquidation = Feed<md::Liquidation>;
pub type FundingRate = Feed<md::FundingRate>;
pub type OpenInterest = Feed<md::OpenInterest>;
pub type Greeks = Feed<md::Greeks>;
pub type MarkPrice = Feed<md::MarkPrice>;

/// The local book of `md.uid` now includes the delta at
/// `(md.conn_epoch, md.seq)`; read it with [`Context::book`] (E08-D057).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BookUpdate {
    pub md: Md,
    pub exch_ts_ns: i64,
}

macro_rules! handlers {
    ($(
        $(#[$doc:meta])*
        $trait:ident :: $method:ident ( $event:ty ) => $field:ident;
    )*) => {
        $(
            $(#[$doc])*
            pub trait $trait: Strategy {
                fn $method(&mut self, ctx: &Context<Self>, ev: $event);
            }
        )*

        /// The optional hooks a strategy registered in [`Strategy::handlers`].
        pub struct Handlers<S> {
            $(pub(crate) $field: Option<fn(&mut S, &Context<S>, $event)>,)*
        }

        impl<S> Default for Handlers<S> {
            fn default() -> Self {
                Self { $($field: None,)* }
            }
        }

        impl<S: Strategy> Handlers<S> {
            $(
                #[doc = concat!("Registers [`", stringify!($trait), "::", stringify!($method), "`].")]
                pub fn $field(&mut self) -> &mut Self
                where
                    S: $trait,
                {
                    self.$field = Some(<S as $trait>::$method);
                    self
                }
            )*
        }
    };
}

handlers! {
    BestQuoteHandler::on_best_quote(BestQuote) => best_quote;
    TradeHandler::on_trade(Trade) => trade;
    AggTradeHandler::on_agg_trade(AggTrade) => agg_trade;
    KlineHandler::on_kline(Kline) => kline;
    TickerHandler::on_ticker(Ticker) => ticker;
    LiquidationHandler::on_liquidation(Liquidation) => liquidation;
    FundingRateHandler::on_funding_rate(FundingRate) => funding_rate;
    OpenInterestHandler::on_open_interest(OpenInterest) => open_interest;
    GreeksHandler::on_greeks(Greeks) => greeks;
    MarkPriceHandler::on_mark_price(MarkPrice) => mark_price;
    /// Called after an `orderbook_delta` was applied to the local book.
    /// Deltas are applied however old they are; the hook is called only for
    /// one within `recv_window_ms` (E08-D003).
    BookHandler::on_book(BookUpdate) => book;
    /// Every change of the strategy's own orders, including a Submit refused
    /// locally and a Submit whose answer timed out (E08-D005).
    OrderHandler::on_order(OrderEvent) => order;
    /// Fills of the strategy's own orders.
    FillHandler::on_fill(FillEvent) => fill;
    /// The account's position changes; not per algo.
    PositionHandler::on_position(PositionEvent) => position;
    /// The account's balance changes; not per algo.
    BalanceHandler::on_balance(BalanceEvent) => balance;
    /// The account snapshot: once after OnStart, after every reconnect or
    /// account worker incarnation, and as the answer to
    /// [`Context::request_recon`] (E06-D003).
    ReconHandler::on_recon(ReconEvent) => recon;
    /// Cancel requests refused locally (E08-D005).
    RejectHandler::on_reject(RejectEvent) => reject;
}

impl<S: Strategy> Handlers<S> {
    /// The strategy's market data hooks, for mdpath's skip-before-decode
    /// (E08-D005). `orderbook_delta` is not among them: the local book takes
    /// every delta (E08-D057).
    pub fn md_hooks(&self) -> mdpath::Hooks {
        mdpath::Hooks {
            best_quote: self.best_quote.is_some(),
            trade: self.trade.is_some(),
            agg_trade: self.agg_trade.is_some(),
            kline: self.kline.is_some(),
            ticker: self.ticker.is_some(),
            liquidation: self.liquidation.is_some(),
            funding_rate: self.funding_rate.is_some(),
            open_interest: self.open_interest.is_some(),
            greeks: self.greeks.is_some(),
            mark_price: self.mark_price.is_some(),
        }
    }
}

/// Market data topics (PROTOCOL §2), for [`Context::tape`].
pub mod topic {
    pub const BEST_QUOTE: &str = "bestquote";
    pub const TRADE: &str = "trade";
    pub const AGG_TRADE: &str = "aggtrade";
    pub const ORDER_BOOK_DELTA: &str = "orderbook_delta";
    pub const TICKER: &str = "ticker";
    pub const LIQUIDATION: &str = "liquidation";
    pub const FUNDING_RATE: &str = "funding_rate";
    pub const OPEN_INTEREST: &str = "open_interest";
    pub const GREEKS: &str = "greeks";
    pub const MARK_PRICE: &str = "mark_price";

    /// The topic of klines of one interval, such as `1m` or `4h`.
    pub fn kline(interval: &str) -> String {
        format!("kline_{interval}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Plain;

    impl Strategy for Plain {
        fn on_init(&mut self, _: &Context<Self>) -> Result<(), HookError> {
            Ok(())
        }
        fn on_start(&mut self, _: &Context<Self>) -> Result<(), HookError> {
            Ok(())
        }
        fn on_stop(&mut self, _: &Context<Self>) {}
    }

    struct Some2;

    impl Strategy for Some2 {
        fn on_init(&mut self, _: &Context<Self>) -> Result<(), HookError> {
            Ok(())
        }
        fn on_start(&mut self, _: &Context<Self>) -> Result<(), HookError> {
            Ok(())
        }
        fn on_stop(&mut self, _: &Context<Self>) {}
        fn handlers(&self, h: &mut Handlers<Self>) {
            h.trade().kline().order();
        }
    }

    impl TradeHandler for Some2 {
        fn on_trade(&mut self, _: &Context<Self>, _: Trade) {}
    }
    impl KlineHandler for Some2 {
        fn on_kline(&mut self, _: &Context<Self>, _: Kline) {}
    }
    impl OrderHandler for Some2 {
        fn on_order(&mut self, _: &Context<Self>, _: OrderEvent) {}
    }

    fn registered<S: Strategy>(s: &S) -> Handlers<S> {
        let mut h = Handlers::default();
        s.handlers(&mut h);
        h
    }

    #[test]
    fn only_registered_hooks_are_on() {
        let none = registered(&Plain);
        assert_eq!(none.md_hooks(), mdpath::Hooks::default());
        assert!(none.order.is_none() && none.book.is_none());

        let some = registered(&Some2);
        let md = some.md_hooks();
        assert!(md.trade && md.kline);
        assert!(!md.best_quote && !md.mark_price);
        assert!(some.order.is_some());
        assert!(some.fill.is_none() && some.book.is_none());
        assert_eq!(topic::kline("1m"), "kline_1m");
    }
}
