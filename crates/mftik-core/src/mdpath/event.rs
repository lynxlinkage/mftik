//! Feed events and the topic-to-hook table (E08-D005).

use mftik_proto::wire::mftiko::md::v1::{self as md, feed_event};
use prost::Message;

use crate::dispatch::envelope;

/// The `orderbook_delta` feed topic. It has no hook here: its messages go to
/// [`crate::bookpath`] (E08-D057).
pub const TOPIC_BOOK: &str = "orderbook_delta";

const KLINE_PREFIX: &str = "kline_";

/// One strategy hook (E08-D005). `orderbook_delta` is not one; see [`TOPIC_BOOK`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Topic {
    BestQuote,
    Trade,
    AggTrade,
    /// Every `kline_<interval>` feed. The interval is in the payload.
    Kline,
    Ticker,
    Liquidation,
    FundingRate,
    OpenInterest,
    Greeks,
    MarkPrice,
}

impl Topic {
    /// The hook for the topic half of a feed key. `orderbook_delta`, a bare
    /// `kline_`, and unknown topics have none.
    pub fn of_feed(topic: &str) -> Option<Self> {
        match topic {
            "bestquote" => Some(Self::BestQuote),
            "trade" => Some(Self::Trade),
            "aggtrade" => Some(Self::AggTrade),
            "ticker" => Some(Self::Ticker),
            "liquidation" => Some(Self::Liquidation),
            "funding_rate" => Some(Self::FundingRate),
            "open_interest" => Some(Self::OpenInterest),
            "greeks" => Some(Self::Greeks),
            "mark_price" => Some(Self::MarkPrice),
            _ => match topic.strip_prefix(KLINE_PREFIX) {
                Some(interval) if !interval.is_empty() => Some(Self::Kline),
                _ => None,
            },
        }
    }
}

/// The hooks the strategy implements. A feed whose topic is off is skipped
/// before its payload is decoded (E08-D005).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Hooks {
    pub best_quote: bool,
    pub trade: bool,
    pub agg_trade: bool,
    pub kline: bool,
    pub ticker: bool,
    pub liquidation: bool,
    pub funding_rate: bool,
    pub open_interest: bool,
    pub greeks: bool,
    pub mark_price: bool,
}

impl Hooks {
    /// Every hook on.
    pub const ALL: Self = Self {
        best_quote: true,
        trade: true,
        agg_trade: true,
        kline: true,
        ticker: true,
        liquidation: true,
        funding_rate: true,
        open_interest: true,
        greeks: true,
        mark_price: true,
    };

    pub fn has(&self, topic: Topic) -> bool {
        match topic {
            Topic::BestQuote => self.best_quote,
            Topic::Trade => self.trade,
            Topic::AggTrade => self.agg_trade,
            Topic::Kline => self.kline,
            Topic::Ticker => self.ticker,
            Topic::Liquidation => self.liquidation,
            Topic::FundingRate => self.funding_rate,
            Topic::OpenInterest => self.open_interest,
            Topic::Greeks => self.greeks,
            Topic::MarkPrice => self.mark_price,
        }
    }

    /// Whether a feed with this topic has a hook. `orderbook_delta` does not.
    pub fn wants(&self, feed_topic: &str) -> bool {
        Topic::of_feed(feed_topic).is_some_and(|topic| self.has(topic))
    }

    /// One message the dispatcher handed to its feed callback, after the
    /// envelope check and the stale prehook.
    ///
    /// The envelope is read again for the cursor. The digest is not checked.
    /// The instrument is the payload's `uid`, not the subject (E08-D065).
    pub fn decode(&self, msg: &[u8]) -> Decoded {
        let header = match envelope::parse(msg) {
            Ok(header) => header,
            Err(err) => return Decoded::Log(format!("md envelope rejected: {err}")),
        };
        let event = match md::FeedEvent::decode(header.payload) {
            Ok(event) => event,
            Err(err) => return Decoded::Log(format!("md payload rejected: {err}")),
        };
        let data = match event.event {
            Some(feed_event::Event::BestQuote(data)) => Data::BestQuote(data),
            Some(feed_event::Event::Trade(data)) => Data::Trade(data),
            Some(feed_event::Event::AggTrade(data)) => Data::AggTrade(data),
            Some(feed_event::Event::Kline(data)) => Data::Kline(data),
            Some(feed_event::Event::Ticker(data)) => Data::Ticker(data),
            Some(feed_event::Event::Liquidation(data)) => Data::Liquidation(data),
            Some(feed_event::Event::FundingRate(data)) => Data::FundingRate(data),
            Some(feed_event::Event::OpenInterest(data)) => Data::OpenInterest(data),
            Some(feed_event::Event::Greeks(data)) => Data::Greeks(data),
            Some(feed_event::Event::MarkPrice(data)) => Data::MarkPrice(data),
            // orderbook_delta is the book path's. An empty oneof is not an event.
            Some(feed_event::Event::OrderBookDelta(_)) | None => return Decoded::Skip,
        };
        if !self.has(data.topic()) {
            return Decoded::Skip;
        }
        Decoded::Hook(Event {
            md: Md {
                uid: event.uid,
                md_ts_ns: header.md_ts_ns,
                conn_epoch: header.conn_epoch,
                seq: header.seq,
            },
            data,
        })
    }
}

/// What every market data event carries besides its payload. `uid` is from
/// the payload; the rest is from the envelope (PROTOCOL §7.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Md {
    pub uid: u32,
    pub md_ts_ns: i64,
    pub conn_epoch: u64,
    pub seq: u64,
}

/// One decoded payload. `meta.exch_ts_ns` is inside it.
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    BestQuote(md::BestQuote),
    Trade(md::Trade),
    AggTrade(md::AggTrade),
    Kline(md::Kline),
    Ticker(md::Ticker),
    Liquidation(md::Liquidation),
    FundingRate(md::FundingRate),
    OpenInterest(md::OpenInterest),
    Greeks(md::Greeks),
    MarkPrice(md::MarkPrice),
}

impl Data {
    pub fn topic(&self) -> Topic {
        match self {
            Self::BestQuote(_) => Topic::BestQuote,
            Self::Trade(_) => Topic::Trade,
            Self::AggTrade(_) => Topic::AggTrade,
            Self::Kline(_) => Topic::Kline,
            Self::Ticker(_) => Topic::Ticker,
            Self::Liquidation(_) => Topic::Liquidation,
            Self::FundingRate(_) => Topic::FundingRate,
            Self::OpenInterest(_) => Topic::OpenInterest,
            Self::Greeks(_) => Topic::Greeks,
            Self::MarkPrice(_) => Topic::MarkPrice,
        }
    }
}

/// One event for the strategy hook named by `data.topic()`.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    pub md: Md,
    pub data: Data,
}

/// Result of [`Hooks::decode`].
#[derive(Clone, Debug, PartialEq)]
pub enum Decoded {
    /// Run the strategy hook for this event.
    Hook(Event),
    /// Not a hook: `orderbook_delta`, an empty oneof, or a topic that is off.
    Skip,
    /// Not delivered. A bad payload is not an envelope drop and is not
    /// counted in `QueueStat.dropped` (E08-D049). The prost error text differs
    /// from Go `proto.Unmarshal`.
    Log(String),
}
