# Context

Every hook receives `&Context<Self>`. It is the only way to read the session and to request work. The object lives for that hook. Orders, the local book, and reads are also covered in [Lifecycle](lifecycle.md) when they fail.

Instruments are UIDs after the first lookup. Resolve a ticker once, keep the UID, and call `info` afterwards. Do not resolve on every order.

## Session

`session_id` returns the session id. It is empty when the session record is not available.

`params` returns the strategy's own parameter bytes from the session spec. The platform does not interpret them. A strategy that wants structured settings decodes this bytes value itself, usually as JSON.

`volume_dir` returns the writable directory, kept across incarnations of the same session. It is empty when the session has none. `input_dir` returns the directory where the named input was unpacked, or an empty string when there is no input directory or `name` is not a relative path inside it. Pass that same relative file name to `mftik_rt::volume::read`. Writing files is described below.

`exit` requests outcome `DONE`. `fail` requests outcome `FAILED` and stores the reason. See [Lifecycle](lifecycle.md).

## Instruments

`resolve` looks up a universal ticker in the symbol service and returns an `Info`. Only `on_init` and `on_start` may call it. Any other hook returns `Error::NotInInit` and sends no request. The message is `sdk: only OnInit and OnStart may call this`. The call may block until the hook's deadline. There is no timeout argument. The runtime uses the hook deadline it already has.

A ticker the symbol service does not know is one the platform does not support. `resolve` then returns `Error` whose text starts with `symbol: no such instrument:`. Leaving that error uncaught in `on_init` or `on_start` ends the session `INIT_FAILED`.

`info` returns the current row for a UID this session has already resolved. It reads memory and does not block. A UID this session has not resolved is `Error::UnknownUid`, message `sdk: the instrument is not known to this session`. That error does not by itself end the session. A row that later moves to the terminal table stays readable and is marked inactive. `is_tradable` is false for that row.

The SDK tracks only instruments this session has resolved. It refreshes those rows when the symbol service notifies it, about every five minutes, and on reconnect. An event that mentions some other UID, such as an old position in a recon snapshot, is still delivered. The SDK does not look that UID up in the background and does not hold the event.

```rust
use mftik_rt::{Context, HookError, Strategy};

struct Hello {
    uid: u32,
}

impl Strategy for Hello {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        let info = ctx.resolve("Ex_Spot_ABC")?;
        self.uid = info.uid;
        let _ = info.is_tradable();
        Ok(())
    }
    fn on_start(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_stop(&mut self, _ctx: &Context<Self>) {}
}
```

## Orders

`submit` places an `Order` (`mftik_core`'s `NewOrder`, re-exported) and returns its `client_order_id`. Before `submit` returns, the order is already in `open_orders` as pending-new. What happens next arrives as `on_order`. An `Err` from `submit` means nothing was sent.

The client order id is a decimal string. Its fields are a version of 1, the algo id, a millisecond timestamp, and a sequence. One counter serves the whole process. The algo id has to be one of the ids declared for that account. Otherwise `submit` returns `Error::AlgoNotDeclared`, message `sdk: the algo_id is not declared for this account`, and no id is taken. An account that is not bound to this session is `Error::UnknownAccount`, message `sdk: the account is not bound to this session`.

A submit the account worker accepts is only an acknowledgement. Later states arrive as `on_order`. A submit that is refused locally, including no responder, also arrives as `on_order` with `origin` `Origin::LocalReject`. A submit that gets no answer within the session's receive window arrives as `on_order` with `origin` `Origin::Timeout`. The SDK does not query the account and does not send that order again.

`cancel` asks the account worker to cancel one order of this strategy, by account and client order id. A local refusal arrives as `on_reject`. A cancel that gets no answer is not reported as a timeout. The order stays as it was until an account event arrives.

`batch_cancel` cancels the open orders of one algo on one account. Extra UIDs restrict the set. With no UIDs, every open order of that algo is eligible. With UIDs, only those instruments are. The SDK splits the set using the account's cancel batch size, and falls back to single cancels when the account has no batch cancel. A local refusal arrives as `on_reject`.

`request_recon` asks for a fresh account snapshot. It arrives as `on_recon`.

`order` returns one of this strategy's orders, a copy, or `None`. Inside `on_order` a terminal order is still visible. `open_orders` returns the account's live orders for that algo. An algo id of 0 means every algo this session declared on the account. Terminal orders are omitted. The list is empty when nothing is open. The order is the runtime's order. It is not sorted again. `position` returns the account's position for a UID, or `None`. `balance` returns the account's balance of an asset, or `None`. Each call returns a copy. These reads do not block.

`Error::Stopping`, message `sdk: the session is stopping`, is returned when the session is already stopping and the call cannot be accepted. `Error::NoFeed`, message `sdk: the session has no such feed`, is returned when a read names a feed this session does not have.

```rust
use mftik_rt::{Context, Order, Strategy};

fn place<S: Strategy>(ctx: &Context<S>, order: Order) {
    if let Ok(id) = ctx.submit(order) {
        let _ = ctx.cancel(1, &id);
    }
    let _ = ctx.batch_cancel(1, 0, &[]);
    let _ = ctx.request_recon(1);
    let _ = ctx.order(1, "1");
    let _ = ctx.open_orders(1, 0);
    let _ = ctx.position(1, 7);
    let _ = ctx.balance(1, "USDT");
}
```

## Local book

`book` returns a `BookView` for a UID once the market-data snapshot is in place, or `None` while that book is not synced. The view borrows the hook's book and derefs to `OrderBook`. The books change only between hooks, so the view does not change underneath the hook, and it must not be used after the hook returns.

`tick_size` is the price of one tick. `price` maps a tick index to a price (`tick * tick_size`). `window` is how many ticks the book keeps. `best_bid_tick` and `best_ask_tick` return the best tick on that side, or `None` when the side is empty. `qty` returns the size at a price on the bid side or the ask side, or `None` when that price is outside the window or the level is empty. Pass `true` for the bid side. `bids` and `asks` iterate `(tick, qty)` pairs from the best price outward, at most `limit` levels. A limit that is not positive yields nothing.

`on_book` is how the strategy hears that a delta was applied. The view is how it reads the result. See [Lifecycle](lifecycle.md).

```rust
use mftik_rt::{BookHandler, BookUpdate, Context, Handlers, HookError, Strategy};

struct Watch {
    uid: u32,
}

impl Strategy for Watch {
    fn on_init(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_start(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_stop(&mut self, _ctx: &Context<Self>) {}
    fn handlers(&self, h: &mut Handlers<Self>) {
        h.book();
    }
}

impl BookHandler for Watch {
    fn on_book(&mut self, ctx: &Context<Self>, ev: BookUpdate) {
        let Some(book) = ctx.book(ev.md.uid) else {
            return;
        };
        let _ = (
            book.tick_size(),
            book.best_bid_tick(),
            book.best_ask_tick(),
            book.bids(1).next(),
            book.asks(1).next(),
        );
    }
}
```

## Reads

`tape`, `book_snapshot`, `fetch_klines`, and `fetch_open_interest` ask another service and return before the answer arrives. The answer is a later hook. The callback is `FnOnce(&mut S, &Context<S>, Result<T, ReadError>) + Send + 'static`. On success the `Result` is `Ok` with the message. On failure it is `Err(ReadError)`. A callback that panics when it runs ends the session `CRASHED`.

Each read waits up to the session's `recv_window_ms`. A timeout or a missing responder is a `ReadError` passed to the callback, not an error from the method that sent the request. The message text matches the Go SDK, for example `mdpath: md.view: context deadline exceeded` and `mdpath: md.fetch: nats: no responders available for request`.

`tape` pages the tape for a UID and a topic string. Build the topic with the constants in `mftik_rt::topic`, or with `topic::kline` for one interval. The query is a `TapeQuery`: `limit`, an optional `before` cursor, and `since_ns`. The callback's value is a tape page. The SDK does not fetch the tape on its own and does not hold live events until history arrives. Live events are delivered immediately. A strategy that wants history calls `tape` and treats a `(conn_epoch, seq)` at or below the page head as a duplicate.

`book_snapshot` asks for the windowed book. A `not_synced` reply is an error to the callback. The SDK's own local book is maintained separately and retries on its own.

`fetch_klines` asks for the latest `limit` closed klines of one `interval`. `fetch_open_interest` asks for the open interest the exchange does not push. Both go to the market-data fetch service. The venue is the instrument's venue. The strategy does not choose a region.

```rust
use mftik_rt::{topic, Context, Strategy, TapeQuery};

fn history<S: Strategy>(ctx: &Context<S>, uid: u32) {
    let _ = ctx.tape(uid, topic::TRADE, TapeQuery::default(), |_s, _ctx, reply| {
        let _ = reply;
    });
    let _ = ctx.book_snapshot(uid, |_s, _ctx, reply| {
        let _ = reply;
    });
    let _ = ctx.fetch_klines(uid, "1m", 10, |_s, _ctx, reply| {
        let _ = reply;
    });
    let _ = ctx.fetch_open_interest(uid, |_s, _ctx, reply| {
        let _ = reply;
    });
}
```

## Timers and `post`

`after` runs a callback on the dispatcher after `delay`. Zero is allowed. Each call is its own timer. The callback receives the `Context` for that later hook. Before the dispatcher exists, or once the session is stopping, nothing is scheduled.

`post` hands a callback to the dispatcher's account queue from the hook thread. `Poster`, from `ctx.poster()`, is the handle that may cross threads: `Poster::post` queues the same kind of callback. It runs on the dispatcher, one at a time, like any hook. Before Init has the dispatcher, or once the account queue is closed, the callback is dropped.

```rust
use std::time::Duration;

use mftik_rt::{Context, HookError, Strategy};

struct Hello;

impl Strategy for Hello {
    fn on_init(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        ctx.after(Duration::from_secs(1), Self::on_timer);
        let poster = ctx.poster();
        poster.post(|_s, _ctx| {});
        Ok(())
    }
    fn on_stop(&mut self, _ctx: &Context<Self>) {}
}

impl Hello {
    fn on_timer(&mut self, ctx: &Context<Self>) {
        ctx.after(Duration::from_secs(1), Self::on_timer);
    }
}
```

## Volume and inputs

A session has one writable directory and zero or more read-only inputs (PROTOCOL §9.1.4). The SDK does not load them at startup. The strategy calls `mftik_rt::volume::read` and `mftik_rt::volume::write` itself, usually from `on_init` and from `on_stop`.

`volume::write` replaces `dir`/`name` with `data` atomically. A temporary file is created in the destination directory, written, synced, closed, renamed onto `name`, and the directory is synced. A reader sees the previous complete file or the new bytes, never a prefix of the new bytes. A failure before the rename deletes the temporary file when it can. A crash may leave a temporary file whose name starts with `.vol-`. That file is not the saved state. An empty `dir` is `ErrorKind::InvalidInput` (`volume: empty directory`) and writes nothing.

`volume::read` takes the volume directory, the input directory, and a file `name`. It returns `Ok(None)` when neither directory has it. The volume copy wins, including a volume file that is empty. If the volume file is absent, the input file is used. An empty directory string is skipped. It is not replaced with the process's current directory. A name that is empty, absolute, or not already a clean relative path is `InvalidInput` (`volume: name must be a relative path inside the directory`), even when both directories are empty. Use `/` as the separator. A name may contain intermediate directories (`nest/leaf`). `.` and `..` are rejected. `input_dir` uses the same rule and returns an empty string for a name it rejects.

An I/O error other than a missing file stops the search. The second directory is not tried after a permission error on the first.

[`examples/rust/md_watch`](../../examples/rust/md_watch) restores trade and book counts from the volume file `counts` and replaces that file from the heartbeat and `on_stop`.

```rust
use mftik_rt::{volume, Context, HookError, Strategy};

struct Saved;

impl Strategy for Saved {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        let _ = volume::read(ctx.volume_dir(), "", "counts")?;
        Ok(())
    }
    fn on_start(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }
    fn on_stop(&mut self, ctx: &Context<Self>) {
        if ctx.volume_dir().is_empty() {
            return;
        }
        let _ = volume::write(ctx.volume_dir(), "counts", br#"{"trades":0,"books":0}"#);
    }
}
```

Do not write anywhere except the volume directory and the system temporary directory, and do not override `MFTIKO_VOLUME_DIR` or `MFTIKO_INPUT_DIR`. The context methods already return the paths the platform prepared. The SDK does not enforce a quota. The platform measures the volume directory and, when it exceeds `Limits.volume_mb` (a zero in the spec means the platform default, 1024 MiB), stops the session with outcome `FAILED` and reason `volume_over_limit`.
