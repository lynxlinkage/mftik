//! Watch one instrument's trades and book. This strategy does not trade.
//!
//! The Rust form of `examples/python/md_watch` (E08-D006). `InitPayload.params`
//! is the strategy's own JSON; the platform does not read it. The ticker is a
//! universal ticker (`Venue_Category_Symbol`); this file does not name a venue
//! or a product.
//!
//! ```json
//! {"ticker": "Ex_Spot_ABC", "every_s": 30}
//! ```
//!
//! OnInit resolves the ticker. `on_trade` and `on_book` each write one JSON
//! line, with the fields of the Python example's lines. `on_book` reads the
//! local book, which is not there until `md.view` has synced it. `every_s` is
//! the `after` heartbeat, in seconds. Lines go to stderr through
//! [`mftik_rt::log`].
//!
//! The trade and book counts are kept in the volume file `counts`, as JSON
//! `{"trades":N,"books":N}`. OnInit starts from that file when it is there,
//! and logs the counts it starts from. The heartbeat and OnStop replace the
//! file atomically. Without a volume directory nothing is kept.

use std::io;
use std::time::Duration;

use mftik_rt::log::{self, Field, Level, Value};
use mftik_rt::{
    BookHandler, BookUpdate, Context, Handlers, HookError, OrderBook, Strategy, Trade,
    TradeHandler, volume,
};
use serde_json::Value as Json;

/// The volume file that holds [`Counts`].
const COUNTS: &str = "counts";

#[derive(Debug, PartialEq)]
struct Params {
    ticker: String,
    every: Duration,
}

/// `InitPayload.params`, with the Python example's checks and messages.
fn parse(raw: &[u8]) -> Result<Params, String> {
    if raw.is_empty() {
        return Err("md_watch: params are required".into());
    }
    let data: Json = serde_json::from_slice(raw).map_err(|err| format!("md_watch: {err}"))?;
    let Json::Object(data) = data else {
        return Err("md_watch: params must be a JSON object".into());
    };
    let ticker = match data.get("ticker") {
        Some(Json::String(ticker)) if !ticker.is_empty() => ticker.clone(),
        _ => return Err("md_watch: ticker is required".into()),
    };
    let Some(every_s) = data.get("every_s") else {
        return Err("md_watch: every_s is required".into());
    };
    let every_s = every_s
        .as_f64()
        .ok_or_else(|| format!("md_watch: not a number: {every_s}"))?;
    if !(every_s.is_finite() && every_s > 0.0) {
        return Err("md_watch: every_s must be a finite number above 0".into());
    }
    let every =
        Duration::try_from_secs_f64(every_s).map_err(|err| format!("md_watch: every_s: {err}"))?;
    Ok(Params { ticker, every })
}

/// The trades and books this session has counted, across incarnations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counts {
    trades: u64,
    books: u64,
}

impl Counts {
    /// The volume file's counts. No file is zero.
    fn parse(raw: Option<&[u8]>) -> Result<Self, String> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        let data: Json =
            serde_json::from_slice(raw).map_err(|err| format!("md_watch: {COUNTS}: {err}"))?;
        let count = |name: &str| {
            data.get(name)
                .and_then(Json::as_u64)
                .ok_or_else(|| format!("md_watch: {COUNTS}: {name} must be a non-negative integer"))
        };
        Ok(Self {
            trades: count("trades")?,
            books: count("books")?,
        })
    }

    fn to_json(self) -> String {
        format!(r#"{{"trades":{},"books":{}}}"#, self.trades, self.books)
    }
}

fn int(value: impl Into<i64>) -> Value {
    Value::Int(value.into())
}

/// A `u64` as Python logs an int: decimal text once it does not fit in `i64`.
fn count(value: u64) -> Value {
    i64::try_from(value).map_or_else(|_| Value::Big(value.to_string()), Value::Int)
}

/// One info line: the Python example's `ctx.log.info(hook, hook=hook, ...)`.
fn line(
    ctx: &Context<MdWatch>,
    hook: &str,
    fields: impl IntoIterator<Item = (&'static str, Value)>,
) {
    let fields: Vec<Field> = [("hook", Value::Str(hook.to_owned()))]
        .into_iter()
        .chain(fields)
        .map(|(name, value)| Field {
            name: name.to_owned(),
            value,
        })
        .collect();
    log::emit(Level::Info, hook, &fields, Some(ctx.session_id()));
}

/// The best level of one side as `bid`, `bid_tick`, and `bid_qty` (or the
/// `ask` names). An empty side adds nothing.
fn top(fields: &mut Vec<(&'static str, Value)>, book: &OrderBook, bid: bool) {
    let (mut levels, names) = if bid {
        (book.bids(1), ["bid", "bid_tick", "bid_qty"])
    } else {
        (book.asks(1), ["ask", "ask_tick", "ask_qty"])
    };
    let Some((tick, qty)) = levels.next() else {
        return;
    };
    fields.push((names[0], Value::Float(book.price(tick))));
    fields.push((names[1], Value::Int(tick)));
    fields.push((names[2], Value::Float(qty)));
}

#[derive(Default)]
struct MdWatch {
    uid: u32,
    every: Duration,
    counts: Counts,
}

impl MdWatch {
    fn count_fields(&self) -> [(&'static str, Value); 2] {
        [
            ("trades", count(self.counts.trades)),
            ("books", count(self.counts.books)),
        ]
    }

    /// Replaces the volume file.
    fn save(&self, ctx: &Context<Self>) -> io::Result<()> {
        if ctx.volume_dir().is_empty() {
            return Ok(());
        }
        volume::write(ctx.volume_dir(), COUNTS, self.counts.to_json().as_bytes())
    }

    fn on_timer(&mut self, ctx: &Context<Self>) {
        let uid = ("uid", int(self.uid));
        line(ctx, "OnTimer", [uid].into_iter().chain(self.count_fields()));
        if let Err(err) = self.save(ctx) {
            ctx.fail(format!("md_watch: write {COUNTS}: {err}"));
            return;
        }
        ctx.after(self.every, Self::on_timer);
    }
}

impl Strategy for MdWatch {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        let params = parse(ctx.params())?;
        let info = ctx.resolve(&params.ticker)?;
        let saved = volume::read(ctx.volume_dir(), "", COUNTS)?;
        self.counts = Counts::parse(saved.as_deref())?;
        self.uid = info.uid;
        self.every = params.every;
        let head = [
            ("ticker", Value::Str(params.ticker)),
            ("uid", int(info.uid)),
        ];
        line(ctx, "OnInit", head.into_iter().chain(self.count_fields()));
        Ok(())
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        ctx.after(self.every, Self::on_timer);
        line(ctx, "OnStart", [("uid", int(self.uid))]);
        Ok(())
    }

    fn on_stop(&mut self, ctx: &Context<Self>) {
        let saved = self.save(ctx);
        line(ctx, "OnStop", self.count_fields());
        if let Err(err) = saved {
            let message = format!("md_watch: write {COUNTS}: {err}");
            log::emit(Level::Error, &message, &[], Some(ctx.session_id()));
        }
    }

    fn handlers(&self, h: &mut Handlers<Self>) {
        h.trade().book();
    }
}

impl TradeHandler for MdWatch {
    fn on_trade(&mut self, ctx: &Context<Self>, ev: Trade) {
        if ev.md.uid != self.uid {
            return;
        }
        self.counts.trades += 1;
        let trade = ev.data;
        line(
            ctx,
            "OnTrade",
            [
                ("uid", int(ev.md.uid)),
                ("seq", count(ev.md.seq)),
                ("trade_id", Value::Str(trade.trade_id)),
                ("price", Value::Float(trade.price)),
                ("qty", Value::Float(trade.qty)),
                ("side", int(trade.side)),
            ],
        );
    }
}

impl BookHandler for MdWatch {
    fn on_book(&mut self, ctx: &Context<Self>, ev: BookUpdate) {
        if ev.md.uid != self.uid {
            return;
        }
        self.counts.books += 1;
        let mut fields = vec![("uid", int(ev.md.uid)), ("seq", count(ev.md.seq))];
        match ctx.book(self.uid) {
            None => fields.push(("synced", Value::Bool(false))),
            Some(view) => {
                fields.push(("synced", Value::Bool(true)));
                top(&mut fields, &view, true);
                top(&mut fields, &view, false);
            }
        }
        line(ctx, "OnBook", fields);
    }
}

fn main() {
    mftik_rt::run(MdWatch::default(), None)
}

#[cfg(test)]
mod tests {
    use mftik_core::book::Level;

    use super::*;

    #[test]
    fn params_take_a_ticker_and_a_period() {
        let params = parse(br#"{"ticker":"Ex_Spot_ABC","every_s":30}"#).expect("params");
        assert_eq!(params.ticker, "Ex_Spot_ABC");
        assert_eq!(params.every, Duration::from_secs(30));
        let params = parse(br#"{"ticker":"Ex_Spot_ABC","every_s":0.5}"#).expect("params");
        assert_eq!(params.every, Duration::from_millis(500));
    }

    #[test]
    fn params_are_refused_with_the_python_messages() {
        let cases: [(&[u8], &str); 9] = [
            (b"", "md_watch: params are required"),
            (b"[]", "md_watch: params must be a JSON object"),
            (br#"{"every_s":1}"#, "md_watch: ticker is required"),
            (
                br#"{"ticker":"","every_s":1}"#,
                "md_watch: ticker is required",
            ),
            (
                br#"{"ticker":1,"every_s":1}"#,
                "md_watch: ticker is required",
            ),
            (
                br#"{"ticker":"Ex_Spot_ABC"}"#,
                "md_watch: every_s is required",
            ),
            (
                br#"{"ticker":"Ex_Spot_ABC","every_s":true}"#,
                "md_watch: not a number: true",
            ),
            (
                br#"{"ticker":"Ex_Spot_ABC","every_s":0}"#,
                "md_watch: every_s must be a finite number above 0",
            ),
            (
                br#"{"ticker":"Ex_Spot_ABC","every_s":-1}"#,
                "md_watch: every_s must be a finite number above 0",
            ),
        ];
        for (raw, want) in cases {
            assert_eq!(
                parse(raw).expect_err(want),
                want,
                "{}",
                String::from_utf8_lossy(raw)
            );
        }
        let err = parse(b"{").expect_err("not json");
        assert!(err.starts_with("md_watch: "), "{err}");
    }

    #[test]
    fn counts_start_at_zero_and_read_back_what_they_write() {
        assert_eq!(Counts::parse(None), Ok(Counts::default()));
        let counts = Counts {
            trades: 1,
            books: 2,
        };
        assert_eq!(counts.to_json(), r#"{"trades":1,"books":2}"#);
        assert_eq!(Counts::parse(Some(counts.to_json().as_bytes())), Ok(counts));
    }

    #[test]
    fn a_bad_counts_file_is_refused() {
        let cases: [(&[u8], &str); 4] = [
            (
                br#"{"trades":1}"#,
                "md_watch: counts: books must be a non-negative integer",
            ),
            (
                br#"{"trades":-1,"books":1}"#,
                "md_watch: counts: trades must be a non-negative integer",
            ),
            (
                br#"{"trades":1.5,"books":1}"#,
                "md_watch: counts: trades must be a non-negative integer",
            ),
            (
                b"[]",
                "md_watch: counts: trades must be a non-negative integer",
            ),
        ];
        for (raw, want) in cases {
            assert_eq!(Counts::parse(Some(raw)), Err(want.to_owned()));
        }
        let err = Counts::parse(Some(b"")).expect_err("empty");
        assert!(err.starts_with("md_watch: counts: "), "{err}");
    }

    #[test]
    fn a_count_past_i64_is_decimal_text() {
        assert_eq!(count(1), Value::Int(1));
        assert_eq!(count(u64::MAX), Value::Big(u64::MAX.to_string()));
    }

    #[test]
    fn top_writes_the_best_level_of_one_side() {
        let mut book = OrderBook::new(8, 1.0).expect("book");
        book.apply(
            &[Level {
                price: 1.0,
                size: 2.0,
            }],
            &[Level {
                price: 2.0,
                size: 1.0,
            }],
        );
        let mut fields = Vec::new();
        top(&mut fields, &book, true);
        top(&mut fields, &book, false);
        assert_eq!(
            fields,
            [
                ("bid", Value::Float(1.0)),
                ("bid_tick", Value::Int(1)),
                ("bid_qty", Value::Float(2.0)),
                ("ask", Value::Float(2.0)),
                ("ask_tick", Value::Int(2)),
                ("ask_qty", Value::Float(1.0)),
            ]
        );
        let empty = OrderBook::new(8, 1.0).expect("book");
        let mut fields = Vec::new();
        top(&mut fields, &empty, true);
        assert!(fields.is_empty());
    }
}
