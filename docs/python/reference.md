# API reference

This page names every public symbol of the `mftik` package. The other pages explain how a session uses them. The text is hand-written. It is not generated from docstrings.

A name that starts with one underscore is part of the native module. Strategy code does not call it. Generated protobuf classes live under `common`, `md`, and `td`. Their fields follow the proto files. This page states the mapping and the types that appear in signatures. It does not list every generated field.

## Package

`__version__` is the installed distribution's version. A checkout that has not been installed reports the fallback written next to the package.

`run` hosts one `Strategy` until the process exits. It does not return. `run(strategy, release=None, *, version="", commit="")` sends `release` as `Hello.release` when that argument is set. `version` and `commit` are the strategy's log fields. Empty strings are still written. They do not change `Hello.release`. See [Install and run](install.md).

`log` is the module-level `Log`. Use it outside a hook. It does not carry `session_id`. `debug`, `info`, `warn`, and `error` take a positional-only message and keyword fields: `log.info("msg", key=value)`. A field value must be a `str`, `int`, `float`, `bool`, or `None`. Anything else, including a subclass or a list, raises `TypeError`. A call below `MFTIKO_LOG_LEVEL` returns at once without checking the message or the fields, so it raises no `TypeError`. Lines are JSON objects on stderr. `MFTIKO_LOG_LEVEL` defaults to info. `MFTIKO_LOG_FORMAT=console` selects the console layout.

`Strategy` is the base class. Construct it with no arguments. `on_init`, `on_start`, and `on_stop` are required. The 17 optional hooks are listed in [Lifecycle](lifecycle.md).

`topic` is the market-data topic strings. `volume` is `read` and `write` for the session directories. `main` is the entry of `python -m mftik.pack`. A hook does not call it.

`common`, `md`, and `td` are the proto packages. Each has a `v1` submodule: `mftik.common.v1`, `mftik.md.v1`, and `mftik.td.v1`. Importing `mftik` registers those modules. There is no protobuf dependency, and there is no bytes marshal of these messages.

## Context

Every hook receives a `Context`. The methods below raise `ContextError` outside that hook, except `now`, which is the time now. Behavior is in [Context](context.md).

| Method | Role |
| --- | --- |
| `session_id` | Session id, or empty when the record is absent |
| `log` | `Log` for this hook. It carries `session_id` |
| `params` | The strategy's parameter bytes |
| `exit` | Request outcome `DONE` |
| `fail` | Request outcome `FAILED` and store the reason |
| `volume_dir` | Writable directory, or empty |
| `input_dir` | Directory of one named input, or empty |
| `now` | The running hook's start, ns; the time now outside a hook |
| `rand` | The session's `random.Random`, seeded from the `init` frame |
| `resolve` | Look up a universal ticker; only `on_init` and `on_start` |
| `info` | The current `Info` row for a UID this session resolved |
| `submit` | Place an `Order`; returns the client order id |
| `cancel` | Cancel one order by account and client order id |
| `batch_cancel` | Cancel open orders of one algo, optionally limited to UIDs |
| `request_recon` | Ask for a fresh account snapshot |
| `order` | One of this strategy's orders, or `None` |
| `open_orders` | Live orders of one algo; algo id 0 is every declared algo |
| `position` | The account's position for a UID, or `None` |
| `balance` | The account's balance of an asset, or `None` |
| `after` | Run a callback on the dispatcher after a delay in seconds |
| `post` | Queue a callback on the dispatcher |
| `poster` | The `Poster` that may leave the hook |
| `book` | A `BookView`, or `None` while the book is not synced |
| `tape` | Ask for a tape page; the answer is a later callback |
| `book_snapshot` | Ask for a book snapshot |
| `fetch_klines` | Ask for klines of one interval |
| `fetch_open_interest` | Ask for open interest |

`after` rejects a delay that is negative, NaN, infinite, or too large. The message is `delay must be a finite number of seconds >= 0`.

`submit` requires an `Order`. Any other type raises `TypeError` (`submit() argument must be mftik.Order`).

## Poster

`Poster` is the only object that may be used from another thread. `Poster.post` queues a callable. The callable later runs on the dispatcher with a new `Context`. `post` drops the callback when the dispatcher is missing or the account buffer is closed. See [Threads](threads.md).

## Book view

`BookView` reads the local book inside the hook that called `book`.

`tick_size` is the price of one tick. `price` maps a tick index to a price (`tick * tick_size`). `window` is how many ticks the book keeps. `best_bid_tick` and `best_ask_tick` return the best tick on that side, or `None` when the side is empty. `qty` returns the size at a price. Pass `True` for the bid side. It returns `None` when the price is outside the window or the level is empty.

`bids` and `asks` return a list of `(tick, qty)` pairs from the best price outward, at most `limit` levels. A limit that is not positive returns an empty list. The lists are copies. The view itself does not outlive the hook.

## Errors

`Error` is the base of the SDK's context failures. The subclasses are not subclasses of each other.

| Exception | When |
| --- | --- |
| `NotInInit` | `resolve` outside `on_init` and `on_start` |
| `UnknownUid` | `info` for a UID this session has not resolved |
| `UnknownAccount` | The account is not bound to this session |
| `AlgoNotDeclared` | The algo id is not declared for that account |
| `NoFeed` | The session has no such feed |
| `Stopping` | The session is already stopping |
| `ReadError` | A read callback's failure. Not raised by the method that sent the request |
| `ContextError` | A `Context` or `BookView` used outside its hook |

The messages are in [Errors and exit codes](errors.md).

`Origin` is where an `on_order` event came from. `from_td` is 1, `local_reject` is 2, and `timeout` is 3. An unknown int raises `ValueError`. A submit that times out arrives with `origin` `timeout`, status unknown, and the SDK does not query the account or send the order again. A missing responder is a local reject.

`RejectedRequest` is which call an `on_reject` event refused. `cancel` is 1 and `batch_cancel` is 2. An unknown int raises `ValueError`. A cancel that gets no answer is not reported. Wait for the account event.

## Market-data and account values

These objects compare with `__eq__` and print with `__repr__`. `UniversalTicker` also implements `__str__`, which is `venue_category_symbol` with `_` between the three parts. `Bybit_Perp_BTCUSDT` is one such string.

`Md`, `Td`, `BookUpdate`, `UniversalTicker`, `Info`, `Order`, and `TapeQuery` are built with keyword-only `__new__`. `Feed` and the account events are built by the runtime. Python code does not construct them.

`Md` carries `uid`, `md_ts_ns`, `conn_epoch`, and `seq`. `Td` carries `api_id`, `incarnation`, and `seq`. `Feed` carries `md` and `data`. `data` is the decoded proto message for that topic. `BookUpdate` carries `md` and `exch_ts_ns`. Order-book deltas are applied even when `on_book` is not called. `on_book` runs when the book is synced and the update is inside the receive window.

`OrderEvent` carries `td`, `origin`, `order`, `cause`, `code`, and `reason`. `order` is the account's order message. `cause` is why the account emitted it. `code` and `reason` describe a refusal. The order is stored before `on_order` runs. Inside that hook, `order` still returns a terminal order. `open_orders` omits it. The row is dropped when the hook returns.

`FillEvent` carries `td` and `fill`. A fill does not change what `order`, `position`, or `balance` return. `PositionEvent` carries `td` and `positions`. `BalanceEvent` carries `td` and `balances`. Those rows are stored before the hook runs. `ReconEvent` carries `td` and `recon`. Before `on_recon` runs, that snapshot is already what `order`, `position`, and `balance` return. A reply to `request_recon` keeps a local pending-new order that the snapshot omitted when the order was submitted at or after the request. `RejectEvent` carries `td`, `request`, `client_order_ids`, `code`, and `reason`. `request` says whether the refused call was a cancel or a batch cancel. `client_order_ids` are the ids the refusal names.

## Instruments

`UniversalTicker` is keyword-only: `venue`, `category`, and `symbol`. Parsing a string is strict. The three parts are required.

`Info` is one symbol-service row. `ticker` is required at construction. The other fields default.

| Field or method | Meaning |
| --- | --- |
| `uid` | Instrument id. Zero is not a valid id |
| `ticker` | The universal ticker |
| `exch_ticker` | The venue's own symbol text |
| `base` | Base currency |
| `quote` | Quote currency |
| `contract_size` | Contract size |
| `contract_value_ccy` | Currency of the contract value |
| `price_ccy` | Currency of the price |
| `settle` | Settlement currency |
| `expiry_ns` | Expiry, nanoseconds, or zero |
| `strike` | Option strike |
| `option_type` | `C`, `P`, or empty |
| `status` | `PRELISTING`, `TRADABLE`, `HALTED`, `SETTLING`, `EXPIRED`, `DELISTED`, `UNKNOWN`, or empty when unspecified |
| `filters` | A new dict on each read. Known keys include `price_tick`, `qty_step`, `min_qty`, `max_qty`, `min_notional`, `max_notional`, `min_price`, and `max_price` |
| `updated_at_ns` | When the row was updated, nanoseconds |
| `is_tradable` | True only when `status` is `TRADABLE` |
| `price_tick` | The `price_tick` filter, or `0.0` when that filter is absent. NaN stays NaN |
| `linear` | `settle` is `quote`, `contract_value_ccy` is `base`, `price_ccy` is `quote`, and `base` and `quote` are both set |
| `inverse` | `base` and `quote` differ, `settle` is `base`, `contract_value_ccy` is `quote`, and `price_ccy` is `quote` |
| `quanto` | `settle` is set and is neither `base` nor `quote`. The other margin flags are false |
| `coin_margined` | `settle` is set and equals `base` |
| `u_margined` | `settle` is set, is not `base`, and the row is not quanto |

`resolve` of an unknown ticker raises `Error`. The message starts with `symbol: no such instrument:`.

## Orders and tape queries

`Order` is keyword-only and frozen. `params` is copied. A NaN `qty`, `quote_qty`, or `price` is stored as `None`.

| Field | Meaning |
| --- | --- |
| `api_id` | Account |
| `algo_id` | Algo declared for that account |
| `uid` | Instrument |
| `side` | Read back as the proto enum |
| `order_type` | Read back as the proto enum |
| `tif` | Time in force, read back as the proto enum |
| `qty` | Base quantity, or `None` |
| `quote_qty` | Quote quantity, or `None` |
| `price` | Limit price, or `None` |
| `recv_window_ms` | Receive window for this order, milliseconds |
| `reduce_only` | Whether the order may only reduce a position |
| `params` | String pairs copied onto the order |

An int that is not a stored enumerator can sit on `side`, `order_type`, or `tif`. `submit` then raises `ValueError`. A bool in one of those fields raises `TypeError` at construction. See [Errors and exit codes](errors.md).

The client order id is a decimal of version, algo id, millisecond timestamp, and sequence. The version is 1. One counter serves the process. An undeclared algo raises `AlgoNotDeclared` and takes no id.

`TapeQuery` is keyword-only. `limit` is how many events to return. `before` is a `Cursor` or `None`. `since_ns` is the oldest exchange time to include, nanoseconds. The callback is `(ctx, value, err)`. A callback that is not callable raises `TypeError` before the request is sent. The number of parameters is not checked until the callback runs.

## Topics

`topic` does not build NATS subjects. The values are the topic strings the runtime subscribes to.

| Name | Value |
| --- | --- |
| `BEST_QUOTE` | `bestquote` |
| `TRADE` | `trade` |
| `AGG_TRADE` | `aggtrade` |
| `ORDER_BOOK_DELTA` | `orderbook_delta` |
| `TICKER` | `ticker` |
| `LIQUIDATION` | `liquidation` |
| `FUNDING_RATE` | `funding_rate` |
| `OPEN_INTEREST` | `open_interest` |
| `GREEKS` | `greeks` |
| `MARK_PRICE` | `mark_price` |

`kline` takes an interval and returns `kline_` plus that interval. `kline("1m")` is `kline_1m`. `kline("")` is `kline_`.

## Volume

`read` looks in the volume directory, then the input directory, and returns bytes or `None`. `write` replaces one file atomically. See [Volume and inputs](volume.md).

`volume_read` and `volume_write` are the same operations on the native module. Strategy code calls `read` and `write`.

## Pack

`main` parses the pack command and pushes the image. The printed digest is `StrategyRef.code`. The manifest's contract list comes from `session_contracts`: `worker.v1`, `stsctl.v1`, `md.v1`, and `td.v1`. See [Pack and deploy](pack.md).

## Native module

These names are on `mftik._mftik`. They are how the interpreter entry and the packer reach the runtime. A strategy does not call them.

`_run` hosts a strategy until the process exits. `run` calls it after it has resolved the release string.

`_run_load_failed` is the path that still speaks Hello and ends `INIT_FAILED` when `module:Class` cannot be loaded. The error text is the argument.

`_registered_hooks` returns the optional hook names the class overrides. The decision matches registration: a method counts when `type(strategy).on_x is not Strategy.on_x`.

`session_contracts` returns the contract names a worker Hello and a packed manifest both list.

## Proto messages

Messages and enums under `common`, `md`, and `td` are frozen. Field names match the proto files. Python code does not construct a message. Calling one raises `TypeError`.

An unset message field is `None`. Each arm of a `oneof` is its own attribute, `None` when another arm is set. The attribute whose name is the proto `oneof` returns the selected arm's name, or `None` when none is set. A repeated field is a tuple. A map field is a new dict on each read.

Proto enums keep an unknown int and reject a bool with `ValueError` (`bool is not an enum value`). `Origin` and `RejectedRequest` reject both.

Types that show up in the signatures above:

| Type | Where |
| --- | --- |
| `Side`, `OrderType`, `TimeInForce` | `mftik.common.v1`, read from an `Order` |
| `OrderCause` | `mftik.td.v1`, the `cause` of an `OrderEvent` |
| `Balance`, `Fill`, `Position`, `Recon` | `mftik.td.v1`, account snapshots and events |
| `BookSnapshot`, `TapePage`, `Klines`, `Cursor`, `OpenInterest` | `mftik.md.v1`, read replies and `TapeQuery.before` |
| `BestQuote`, `Trade` | `mftik.md.v1`, the `data` of those feeds |

The account `Order` message is `mftik.td.v1.Order`. It is not `mftik.Order`, which is the order a strategy submits.
