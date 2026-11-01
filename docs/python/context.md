# Context

Every hook receives a `Context`. It is the only way to read the session and to request work. The methods below raise `ContextError` when the object is used outside that hook. `now` is the exception: outside a hook it is the time now. The subclasses of `Error` are listed in [Errors and exit codes](errors.md). Orders, the local book, and reads are also covered there when they fail.

Instruments are UIDs after the first lookup. Resolve a ticker once, keep the UID, and call `info` afterwards. Do not resolve on every order.

## Session

`session_id` returns the session id. It is empty when the session record is not available.

`log` returns a `Log` for this hook. Reading it outside the hook raises `ContextError`. The object copies `session_id`, including when that id is empty, and may be kept after the hook returns. `debug`, `info`, `warn`, and `error` take the message as a positional-only `str` and the other fields as keywords: `ctx.log.info("msg", key=value)`. A field has to be a `str`, `int`, `float`, `bool`, or `None`. Any other value raises `TypeError` and is not turned into a string. Each call at or above `MFTIKO_LOG_LEVEL` writes one JSON object to stderr. A call below it returns at once and checks neither the message nor the fields, so it raises no `TypeError`. Outside a hook, use the module-level `log`, which omits `session_id`. See [API reference](reference.md).

`params` returns the strategy's own parameter bytes from the session spec. The platform does not interpret them. A strategy that wants structured settings decodes this bytes value itself, usually as JSON.

`volume_dir` returns the writable directory, kept across incarnations of the same session. It is empty when the session has none. `input_dir` returns the directory where the named input was unpacked, or an empty string when there is no input directory or `name` is not a relative path inside it. Pass that same relative file name to `read`. Writing files is described in [Volume and inputs](volume.md).

`exit` requests outcome `DONE`. `fail` requests outcome `FAILED` and stores the reason. See [Lifecycle](lifecycle.md).

## Time and random draws

`now` returns when the hook running on this thread started, in nanoseconds on the session clock, the same origin as `md_ts_ns`. Every call in that hook returns the same value. With the event log on it is the `t_ns` of the hook's input. Outside any hook it is the time now. It does not raise `ContextError`.

`rand` returns one `random.Random` for the session, seeded with the `init` frame's seed. The first call creates it; later calls return the same object. Calling it before OnInit, when there is no seed, raises `Error` with the message `sdk: the session has no seed before OnInit`. A stale context still raises `ContextError`. `sampler` logs `now` twice and one draw on every hook. See [Examples](examples.md).

## Instruments

`resolve` looks up a universal ticker in the symbol service and returns an `Info`. Only `on_init` and `on_start` may call it. Any other hook raises `NotInInit` and sends no request. The message is `sdk: only OnInit and OnStart may call this`. The call may block until the hook's deadline. It releases the GIL while it waits. There is no timeout argument. The runtime uses the hook deadline it already has.

A ticker the symbol service does not know is one the platform does not support. `resolve` then raises `Error`. The message starts with `symbol: no such instrument:`. Leaving that exception uncaught in `on_init` or `on_start` ends the session `INIT_FAILED`.

`info` returns the current row for a UID this session has already resolved. It reads memory and does not block. A UID this session has not resolved raises `UnknownUid`, message `sdk: the instrument is not known to this session`. That error does not by itself end the session. A row that later moves to the terminal table stays readable and is marked inactive. `is_tradable` is false for that row.

The SDK tracks only instruments this session has resolved. It refreshes those rows when the symbol service notifies it, about every five minutes, and on reconnect. An event that mentions some other UID, such as an old position in a recon snapshot, is still delivered. The SDK does not look that UID up in the background and does not hold the event.

## Orders

`submit` places an `Order` and returns its `client_order_id`. The argument must be an `Order`. Any other type raises `TypeError` with the message `submit() argument must be mftik.Order`. There is no keyword form of submit. Before `submit` returns, the order is already in `open_orders` as pending-new. What happens next arrives as `on_order`. An exception from `submit` means nothing was sent.

The client order id is a decimal string. Its fields are a version of 1, the algo id, a millisecond timestamp, and a sequence. One counter serves the whole process. The algo id has to be one of the ids declared for that account. Otherwise `submit` raises `AlgoNotDeclared`, message `sdk: the algo_id is not declared for this account`, and no id is taken. An account that is not bound to this session raises `UnknownAccount`, message `sdk: the account is not bound to this session`.

`side`, `order_type`, and `tif` on the `Order` are read back as proto enums. An int that is not one of those enumerators can sit on the object, and `submit` then raises `ValueError` (`unknown side N`, `unknown order_type N`, or `unknown tif N`). A bool raises `TypeError` (`bool is not an enum value`) at construction.

A submit the account worker accepts is only an acknowledgement. Later states arrive as `on_order`. A submit that is refused locally, including no responder, also arrives as `on_order` with `origin` `local_reject`. A submit that gets no answer within the session's receive window arrives as `on_order` with `origin` `timeout`. The SDK does not query the account and does not send that order again.

`cancel` asks the account worker to cancel one order of this strategy, by account and client order id. A local refusal arrives as `on_reject`. A cancel that gets no answer is not reported as a timeout. The order stays as it was until an account event arrives.

`batch_cancel` cancels the open orders of one algo on one account. Extra positional arguments are UIDs. With no UIDs, every open order of that algo is eligible. With UIDs, only those instruments are. The SDK splits the set using the account's cancel batch size, and falls back to single cancels when the account has no batch cancel. A local refusal arrives as `on_reject`.

`request_recon` asks for a fresh account snapshot. It arrives as `on_recon`.

`order` returns one of this strategy's orders, a copy, or `None`. Inside `on_order` a terminal order is still visible. `open_orders` returns a tuple of the account's live orders for that algo. An algo id of 0 means every algo this session declared on the account. Terminal orders are omitted. The tuple is empty when nothing is open. The order is the runtime's order. It is not sorted again. `position` returns the account's position for a UID, or `None`. `balance` returns the account's balance of an asset, or `None`. Each call returns a copy. These reads do not block.

`Stopping`, message `sdk: the session is stopping`, is raised when the session is already stopping and the call cannot be accepted. `NoFeed`, message `sdk: the session has no such feed`, is raised when a read names a feed this session does not have.

`maker` is the order example. Its first `on_recon` reads `position` and `open_orders`, may call `batch_cancel`, then `submit`s one limit order. `on_order` calls `cancel` when that order is accepted. See [Examples](examples.md).

## Local book

`book` returns a `BookView` for a UID once the market-data snapshot is in place, or `None` while that book is not synced. The view borrows the hook's book. The books change only between hooks, so the view does not change underneath the hook, and it must not be used after the hook returns. See [Threads](threads.md).

`tick_size` is the price of one tick. `price` maps a tick index to a price (`tick * tick_size`). `window` is how many ticks the book keeps. `best_bid_tick` and `best_ask_tick` return the best tick on that side, or `None` when the side is empty. `qty` returns the size at a price on the bid side or the ask side, or `None` when that price is outside the window or the level is empty. Pass `True` for the bid side. `bids` and `asks` return a list of `(tick, qty)` pairs from the best price outward, at most `limit` levels. A limit that is not positive returns an empty list. The list is a copy and remains valid after the hook returns.

`on_book` is how the strategy hears that a delta was applied. The view is how it reads the result. See [Lifecycle](lifecycle.md).

## Reads

`tape`, `book_snapshot`, `fetch_klines`, and `fetch_open_interest` ask another service and return before the answer arrives. The answer is a later hook. The callback is `callback(ctx, value, err)`. On success `value` is the message and `err` is `None`. On failure `value` is `None` and `err` is a `ReadError`. A callback that is not callable raises `TypeError` before the request is sent. The number of parameters is not checked ahead of time. A callback that raises when it runs ends the session `CRASHED`.

Each read waits up to the session's `recv_window_ms`. A timeout or a missing responder is a `ReadError` passed to the callback, not an exception from the method that sent the request. The message text matches the Go SDK, for example `mdpath: md.view: context deadline exceeded` and `mdpath: md.fetch: nats: no responders available for request`.

`tape` pages the tape for a UID and a topic string. Build the topic with the constants in `topic`, or with `kline` for one interval. The query is a `TapeQuery`: `limit`, an optional `before` cursor, and `since_ns`. The callback's value is a tape page. The SDK does not fetch the tape on its own and does not hold live events until history arrives. Live events are delivered immediately. A strategy that wants history calls `tape` and treats a `(conn_epoch, seq)` at or below the page head as a duplicate.

`book_snapshot` asks for the windowed book. A `not_synced` reply is an error to the callback. The SDK's own local book is maintained separately and retries on its own.

`fetch_klines` asks for the latest `limit` closed klines of one `interval`. `fetch_open_interest` asks for the open interest the exchange does not push. Both go to the market-data fetch service. The venue is the instrument's venue. The strategy does not choose a region.

## Timers

`after` runs a callback on the dispatcher after `delay` seconds. `delay` must be a finite number of seconds greater than or equal to zero. Anything else, including NaN and infinity, raises `ValueError` with the message `delay must be a finite number of seconds >= 0`. Zero is allowed. Each call is its own timer. The callback receives the `Context` for that later hook. A callback that is not callable raises `TypeError` immediately. Before the dispatcher exists, or once the session is stopping, nothing is scheduled.

`post` and `Poster.post` are described in [Threads](threads.md).
