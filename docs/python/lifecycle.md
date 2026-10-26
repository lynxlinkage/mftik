# Lifecycle

A session is three required hooks and any of the 17 optional hooks the class overrides. Hooks run one at a time on the thread that called `run`, which for `python -m mftik.run` is the main thread. The strategy does not take a lock to protect its own attributes from the SDK. Another thread reaches the strategy only through a `Poster`, and that callback also runs on this thread. See [Threads](threads.md).

The Python names are the Go names in snake_case. The Go SDK is the reference implementation.

| Python | Go | Call |
| --- | --- | --- |
| `on_init` | `OnInit` | `(self, ctx)` |
| `on_start` | `OnStart` | `(self, ctx)` |
| `on_stop` | `OnStop` | `(self, ctx)` |
| `on_best_quote` | `OnBestQuote` | `(self, ctx, ev)` |
| `on_trade` | `OnTrade` | `(self, ctx, ev)` |
| `on_agg_trade` | `OnAggTrade` | `(self, ctx, ev)` |
| `on_kline` | `OnKline` | `(self, ctx, ev)` |
| `on_ticker` | `OnTicker` | `(self, ctx, ev)` |
| `on_liquidation` | `OnLiquidation` | `(self, ctx, ev)` |
| `on_funding_rate` | `OnFundingRate` | `(self, ctx, ev)` |
| `on_open_interest` | `OnOpenInterest` | `(self, ctx, ev)` |
| `on_greeks` | `OnGreeks` | `(self, ctx, ev)` |
| `on_mark_price` | `OnMarkPrice` | `(self, ctx, ev)` |
| `on_book` | `OnBook` | `(self, ctx, ev)` |
| `on_order` | `OnOrder` | `(self, ctx, ev)` |
| `on_fill` | `OnFill` | `(self, ctx, ev)` |
| `on_position` | `OnPosition` | `(self, ctx, ev)` |
| `on_balance` | `OnBalance` | `(self, ctx, ev)` |
| `on_recon` | `OnRecon` | `(self, ctx, ev)` |
| `on_reject` | `OnReject` | `(self, ctx, ev)` |

`ctx` is a `Context`. A return value is ignored. An `async def` hook is rejected, including `on_init`, `on_start`, and `on_stop`. The session ends `INIT_FAILED` with a `TypeError`, and the SDK does not start an event loop.

## Which optional hooks run

An optional hook is registered only when the class overrides it: `type(strategy).on_trade is not Strategy.on_trade`. The decision is made once, before `on_init`. Assigning a function onto the instance later does not register it. A topic with no registered hook is skipped before its payload is decoded, so an unused feed does not spend time on a message the strategy will not see.

The base implementations of the optional hooks return immediately. Leaving them in place means the strategy does not receive that event.

## Order of the session

1. The class is constructed with no arguments.
2. Hello is sent. `Hello.release` is the `release` argument of `run`, or `MFTIK_RELEASE`, or `__version__`.
3. `on_init` runs. Parse `params`, call `resolve` for each instrument, and read the volume. Market data is not flowing yet, and orders are not available yet.
4. `on_start` runs once the market-data and account leases are ready. Orders may be placed from here on. The first account snapshot is requested after this hook and arrives later as `on_recon`.
5. Optional hooks run until the session ends.
6. `on_stop` runs for the endings that include it. See the table in [Errors and exit codes](errors.md).

`on_init` and `on_start` each have a deadline from the platform (`Init.deadline_ms` and `Start.deadline_ms`). If the deadline passes, the session ends `INIT_FAILED` with reason `deadline` without waiting for the hook to return. A start deadline does not run `on_stop`. A start failure that is not a timeout does run `on_stop`, because `on_init` already succeeded. An `on_init` failure does not run `on_stop`.

A stop that arrives after `on_init` has succeeded and before `on_start` has succeeded ends `DONE` with exit code 0. `on_start` and `on_stop` do not run.

## Market-data hooks

`on_best_quote`, `on_trade`, `on_agg_trade`, `on_kline`, `on_ticker`, `on_liquidation`, `on_funding_rate`, `on_open_interest`, `on_greeks`, and `on_mark_price` receive a `Feed`. `ev.md` is the envelope (`Md`). `ev.data` is the decoded protobuf message for that topic: best quote, trade, aggregated trade, kline, ticker, liquidation, funding rate, open interest, greeks, or mark price. `on_kline` receives every kline interval; the interval is on the payload. Field names are the proto names. See [API reference](reference.md).

`on_book` receives a `BookUpdate`, not a `Feed`. The SDK has already applied that `orderbook_delta` to the local book. Read the book with `book`. The hook is called only when the book is synced and the event is still inside the session's receive window. An older delta is still applied, so the book does not skip levels, but `on_book` is not called for it. While the book is not synced, `book` returns `None` and `on_book` is not called.

Market-data events sit in a fixed ring. When the ring cannot hold a new event, the oldest events are dropped. An event whose exchange timestamp is older than the session's `recv_window_ms`, measured when the dispatcher takes it off the ring, is dropped before decode. `orderbook_delta` is the exception to that drop: it is applied anyway, and only the hook call is skipped. Dropped market data is counted. The strategy does not see a gap notification for a dropped trade. The book path resynchronizes itself when its own sequence breaks.

## Account hooks

`on_order` receives an `OrderEvent` for every change of one of this strategy's orders, including a submit the SDK refused locally and a submit whose answer timed out. `on_fill` receives a `FillEvent`. `on_position` receives a `PositionEvent` for the account, not for one algo. `on_balance` receives a `BalanceEvent`, also for the whole account. `on_recon` receives a `ReconEvent`: the snapshot after `on_start`, a snapshot after a reconnect or a new account incarnation, and the answer to `request_recon`. `on_reject` receives a `RejectEvent` when a cancel or a batch cancel is refused locally.

The local view is updated before the hook runs. Inside `on_order`, `on_position`, `on_balance`, and `on_recon`, the reads `order`, `open_orders`, `position`, and `balance` already include that event. `on_fill` does not change those four reads. The SDK does not derive a position or a balance from a fill. A terminal order is still visible to `order` inside `on_order` and is removed from `open_orders` when that hook returns.

## Which buffer runs next

Two queues feed the dispatcher. The account queue holds account events, order replies, timers, `post` callbacks, and the replies from `tape`, `book_snapshot`, `fetch_klines`, and `fetch_open_interest`. It is not dropped. Its cap is 65536 items. One more ends the session `FAILED` with reason `td buffer full`. `on_stop` runs. Events that were already applied stay visible to `on_stop`. The event that filled the queue, and events taken after the session has failed, are not delivered.

The market-data queue is the ring described above. After each hook, a waiting account event runs before the next market-data event. A fill and a trade that are both queued are delivered as `on_order` or `on_fill` first, then the trade.

## Ending the session from a hook

`exit` requests outcome `DONE`. `fail` requests outcome `FAILED` and keeps the reason string. Call one of them and return. The first request wins. The session finishes the current hook, then follows the ending in [Errors and exit codes](errors.md), including `on_stop` for both of those outcomes. `sys.exit` is not `exit`. It raises `SystemExit`, and that exception is the hook failure described on that page.

An exception from `on_stop` does not change the outcome or the exit code. The traceback goes to stderr. The platform records that `on_stop` did not finish.
