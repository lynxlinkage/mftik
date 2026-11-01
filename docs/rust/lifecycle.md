# Lifecycle

A session is three required hooks on `Strategy` and any of the 17 optional hooks the strategy registers. Hooks run one at a time on the thread that called `run`. The strategy does not take a lock to protect its own fields from the SDK. Another thread reaches the strategy only through a `Poster`, and that callback also runs on this thread.

The Rust method names are the Go names in snake_case. The Go SDK is the reference implementation. Optional hooks are one trait each, like Go's handler interfaces. Rust has no type assertion, so a strategy names the ones it implements in `Strategy::handlers`. Each registration requires the trait.

| Rust | Go | Trait |
| --- | --- | --- |
| `on_init` | `OnInit` | `Strategy` |
| `on_start` | `OnStart` | `Strategy` |
| `on_stop` | `OnStop` | `Strategy` |
| `on_best_quote` | `OnBestQuote` | `BestQuoteHandler` |
| `on_trade` | `OnTrade` | `TradeHandler` |
| `on_agg_trade` | `OnAggTrade` | `AggTradeHandler` |
| `on_kline` | `OnKline` | `KlineHandler` |
| `on_ticker` | `OnTicker` | `TickerHandler` |
| `on_liquidation` | `OnLiquidation` | `LiquidationHandler` |
| `on_funding_rate` | `OnFundingRate` | `FundingRateHandler` |
| `on_open_interest` | `OnOpenInterest` | `OpenInterestHandler` |
| `on_greeks` | `OnGreeks` | `GreeksHandler` |
| `on_mark_price` | `OnMarkPrice` | `MarkPriceHandler` |
| `on_book` | `OnBook` | `BookHandler` |
| `on_order` | `OnOrder` | `OrderHandler` |
| `on_fill` | `OnFill` | `FillHandler` |
| `on_position` | `OnPosition` | `PositionHandler` |
| `on_balance` | `OnBalance` | `BalanceHandler` |
| `on_recon` | `OnRecon` | `ReconHandler` |
| `on_reject` | `OnReject` | `RejectHandler` |

`ctx` is `&Context<Self>`. `on_init` and `on_start` return `Result<(), HookError>`. `HookError` is `Box<dyn Error + Send + Sync + 'static>`. Its `Display` text becomes `Done.error` and `Exiting.reason`. `on_stop` and the optional hooks return nothing.

```rust
use mftik_rt::{Context, Handlers, HookError, Strategy, Trade, TradeHandler};

struct Watch;

impl Strategy for Watch {
    fn on_init(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_start(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_stop(&mut self, _ctx: &Context<Self>) {}
    fn handlers(&self, h: &mut Handlers<Self>) {
        h.trade();
    }
}

impl TradeHandler for Watch {
    fn on_trade(&mut self, ctx: &Context<Self>, ev: Trade) {
        let _ = (ctx, ev.md.uid);
    }
}
```

## Which optional hooks run

An optional hook is registered only when `handlers` calls the matching method, for example `h.trade()`. The decision is made once, before `on_init`. A topic with no registered hook is skipped before its payload is decoded, so an unused feed does not spend time on a message the strategy will not see. The default `handlers` registers none.

## Order of the session

1. The caller constructs the strategy and passes it to `run`.
2. Hello is sent. `Hello.release` is the `release` argument of `run`, or `mftik_rt::RELEASE`.
3. `on_init` runs. Parse `params`, call `resolve` for each instrument, and read the volume. Market data is not flowing yet, and orders are not available yet.
4. `on_start` runs once the market-data and account leases are ready. Orders may be placed from here on. The first account snapshot is requested after this hook and arrives later as `on_recon`.
5. Optional hooks run until the session ends.
6. `on_stop` runs for the endings that include it. See the table below.

`on_init` and `on_start` each have a deadline from the platform (`Init.deadline_ms` and `Start.deadline_ms`). If the deadline passes, the session ends `INIT_FAILED` with reason `deadline` without waiting for the hook to return. A start deadline does not run `on_stop`. A start failure that is not a timeout does run `on_stop`, because `on_init` already succeeded. An `on_init` failure does not run `on_stop`.

A stop that arrives after `on_init` has succeeded and before `on_start` has succeeded ends `DONE` with exit code 0. `on_start` and `on_stop` do not run.

## Market-data hooks

`on_best_quote`, `on_trade`, `on_agg_trade`, `on_kline`, `on_ticker`, `on_liquidation`, `on_funding_rate`, `on_open_interest`, `on_greeks`, and `on_mark_price` receive a `Feed`. `ev.md` is the envelope (`Md`: `uid`, `md_ts_ns`, `conn_epoch`, `seq`). `ev.data` is the decoded protobuf message for that topic. `on_kline` receives every kline interval; the interval is on the payload. Field names are the proto names.

`on_book` receives a `BookUpdate`, not a `Feed`. The SDK has already applied that `orderbook_delta` to the local book. Read the book with `book`. The hook is called only when the book is synced and the event is still inside the session's receive window. An older delta is still applied, so the book does not skip levels, but `on_book` is not called for it. While the book is not synced, `book` returns `None` and `on_book` is not called.

Market-data events sit in a fixed ring. When the ring cannot hold a new event, the oldest events are dropped. An event whose exchange timestamp is older than the session's `recv_window_ms`, measured when the dispatcher takes it off the ring, is dropped before decode. `orderbook_delta` is the exception to that drop: it is applied anyway, and only the hook call is skipped. Dropped market data is counted. The strategy does not see a gap notification for a dropped trade. The book path resynchronizes itself when its own sequence breaks.

## Account hooks

`on_order` receives an `OrderEvent` for every change of one of this strategy's orders, including a submit the SDK refused locally and a submit whose answer timed out. `on_fill` receives a `FillEvent`. `on_position` receives a `PositionEvent` for the account, not for one algo. `on_balance` receives a `BalanceEvent`, also for the whole account. `on_recon` receives a `ReconEvent`: the snapshot after `on_start`, a snapshot after a reconnect or a new account incarnation, and the answer to `request_recon`. `on_reject` receives a `RejectEvent` when a cancel or a batch cancel is refused locally.

The local view is updated before the hook runs. Inside `on_order`, `on_position`, `on_balance`, and `on_recon`, the reads `order`, `open_orders`, `position`, and `balance` already include that event. `on_fill` does not change those four reads. The SDK does not derive a position or a balance from a fill. A terminal order is still visible to `order` inside `on_order` and is removed from `open_orders` when that hook returns.

`md_watch` shows `on_trade`, `on_book`, and `after`.

## Which buffer runs next

Two queues feed the dispatcher. The account queue holds account events, order replies, timers, `post` callbacks, and the replies from `tape`, `book_snapshot`, `fetch_klines`, and `fetch_open_interest`. It is not dropped. Its cap is 65536 items. One more ends the session `FAILED` with reason `td buffer full`. `on_stop` runs. Events that were already applied stay visible to `on_stop`. The event that filled the queue, and events taken after the session has failed, are not delivered.

The market-data queue is the ring described above. After each hook, a waiting account event runs before the next market-data event. A fill and a trade that are both queued are delivered as `on_order` or `on_fill` first, then the trade.

## Ending the session from a hook

| Outcome | When | `on_stop` | Exit code |
| --- | --- | --- | --- |
| `DONE` | A stop from the platform, or `exit` | runs | 0 |
| `INIT_FAILED` | `on_init` or `on_start` returned `Err`, panicked, or a deadline expired | only when start failed and it was not a timeout | 10 |
| `CRASHED` | A hook after start panicked | runs | 20 |
| `HUNG` | A hook ran past `hook_timeout_s` | does not run | 21 |
| `FAILED` | `fail`, the platform asked the session to fail, or the account queue overflowed | runs | 30 |

Exit code 1 is the worker giving up before it can keep speaking the control protocol, for example when it cannot take the control file descriptor. A process killed by a signal has no outcome from this table. The platform records the signal. `HUNG` from the platform's own heartbeat watchdog is that kind of kill: the process is signaled, and the platform treats the outcome as `HUNG` with reason `no_beat`.

`CRASHED` does not deliver account events that were still queued. `DONE` and `FAILED` do deliver the account events received before the stop, and then run `on_stop`. The event that overflowed the account queue is not one of those events. The reason for that overflow is `td buffer full`.

A deadline on init or start uses reason `deadline`, does not wait for the hook, does not run `on_stop`, and exits 10. A hook that exceeds `hook_timeout_s` uses reason `hook timeout` and exits 21. `on_stop` is not called.

When the strategy ends the session itself, or the control connection closes, `on_stop` is also under `hook_timeout_s`. If that hook exceeds the budget, the process exits 21 and the platform is told that `on_stop` did not finish. The outcome already sent stays what it was. It is not rewritten to `HUNG`.

`exit` requests outcome `DONE`. `fail` requests outcome `FAILED` and keeps the reason string. Call one of them and return. The first request wins. The session finishes the current hook, then follows the ending above, including `on_stop` for both of those outcomes. `std::process::exit` is not `exit`. It skips the session protocol.

An `Err` from `on_init` or `on_start` ends the session `INIT_FAILED`. `Done.error` is the error's `Display` text.

A panic from `on_init` or `on_start` is also `INIT_FAILED`. `Done.error` is `hook panic: {message}`. A panic from any later hook, including a read callback, an `after` callback, and a `post` callback, is `CRASHED` with reason `hook panic: {message}`, and `on_stop` still runs. The message is the `&str` or `String` the panic carried, or `unknown panic`.

A panic from `on_stop` does not change the outcome or the exit code. The runtime logs `on stop: hook panic: ...`. The platform records that `on_stop` did not finish.
