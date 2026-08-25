# MD venue subscriptions — one feed key is not one WebSocket topic

An STS session subscribes to `topic.UniversalTicker`. MD refcounts that key,
opens a `stream_*` pump on first subscriber, and tears the pump down at zero.
That ledger is honest about **who wants the product feed**. It is not honest
about **what is subscribed at the venue**.

A hook and a venue channel are not 1-1. One MD topic can need several
WebSocket streams to assemble a shared model; several MD topics can be
satisfied by a single venue topic. When a product refcount hits zero,
`UNSUBSCRIBE` on the wire is therefore not a mechanical consequence — the
same venue topic may still be feeding another pump, or the pump that just
died may have been only one of two streams that pump was holding.

This is the work to do before adding feeds that share a venue topic
(`funding_rate` on Bybit's `tickers.{symbol}`) or before treating
refcount-zero as permission to drop a venue subscription.

OKX's public socket already keeps a wire-side set. Bybit's private stream
already derives the live set for reconnect. Neither is a complete model
for I2 as written below: both decide "already subscribed" *after* a
concurrent caller can have started the same round trip, and MD's pumps
race that path by construction.

## What is already true

Facts this design rests on, all of them checkable in the tree today.

**MD's unit is the product feed.** `Dispatcher` keys subscriptions by
`(topic, UniversalTicker)` — exactly what `Topics.md_feed` renders
(`ticker.BinanceFuture_Perp_BTCUSDT`). `_subscribe_feed` calls
`ensure_feed` on the first STS link and `_stop_feed_if_unused` stops the
pump at zero. Two sessions on the same key share one pump. Two keys never
share a pump, even when they name the same instrument.

**STS hooks stay 1-1 with MD topics.** `on_ticker` is `ticker`,
`on_best_quote` is `bestquote`. The mismatch is not hook ↔ topic. It is
`stream_*` ↔ the venue's own stream names.

**MD does not name venue channels.** `VenueSession._open` resolves a topic
to a connector method (`stream_ticker`, `stream_best_quote`, …). A venue
that cannot serve a topic has no such method and the subscribe is refused
by name. Which Binance streams make a `Ticker` is the connector's
business; see `mftik.exchange.base`.

**`_open` does not subscribe.** It returns an async generator. The
connector's `subscribe_*` runs on the first iteration, inside the pump
task `ensure_feed` just spawned (`venue.py`). The comment there — opened
here so a missing method fails the attach — is about `hasattr` and topic
parsing, not about the venue ack. Two pumps started one after another
therefore race their `subscribe_*` calls. Ticker and bestquote on the
same instrument are the headline case.

**One-to-many already exists.** `BinanceFuturePublicClient._tickers` opens
`@ticker` *and* `@bookTicker` for one MD `ticker` feed, because the 24h
ticker has no quote. Nothing is emitted until both halves have arrived.
One product key, two venue subscriptions, two sockets (futures splits
those names across `/market` and `/public`).

**Many-to-one already exists.** `stream_trades` and `stream_agg_trades` on
Binance futures both call `subscribe_agg_trades` — there is no raw tape,
only `@aggTrade`. `stream_ticker` and `stream_best_quote` both read
`@bookTicker`. Bybit's linear `tickers.{symbol}` already carries
`fundingRate` on the same topic the ticker pump reads; a funding-only
delta is dropped today because it has no price.

**A venue topic is a set membership, not a second pipe.** One WebSocket
either is or is not subscribed to a given wire identity. A second
`SUBSCRIBE` for the same identity does not give a second stream of
prints. Binance typically acks it as a no-op. Bybit's *private* socket
answers a duplicate subscribe with an error.

**OKX public already shares.** `OkxPublicStream` keeps `_subscribed`,
skips the frame when `ch.arg_key(arg)` is already in the set, and
re-fills the set in `_restore` (`okx/feed.py`). That is the public
reference for the *shape*. It is not a complete I2: the check is
`wanted = [arg for arg in args if key not in self._subscribed]`, then
`await self.request(...)`, then `update`. Two concurrent `_subscribe`
calls both see the key absent and both send. Bybit private is the same
window (`account.py`: check, `await _send_subscribe`, then update).

**Binance and Bybit public do not share.** `BinanceStreamSocket.subscribe`
and `BybitPublicStream._subscribe` append a `_Sub` and send `SUBSCRIBE`
every time. `_push` then copies the one venue print to every matching
`_Sub`. Two pumps that want `@bookTicker` therefore send the name twice,
restore it twice on reconnect, and still receive each print once. That
duplicate send is also what *accidentally* redraws a venue snapshot for
the late joiner — see below.

**A late joiner gets no snapshot.** Bybit and OKX push a snapshot when a
subscription *starts*, then deltas. Under a shared wire ledger the
second consumer of a live identity joins mid-stream. Today's
duplicate-`SUBSCRIBE` papers over this by asking the venue for another
snapshot. Sharing without a local replay means:

- a `funding_rate` pump joining an already-live `tickers.BTCUSDT`
  publishes nothing until the next `fundingRate` field — possibly hours;
- `subscribe_book_deltas` joining a live `orderbook.N` can never build a
  book (it will only ever see deltas);
- a *folded* book (`subscribe_order_book`, `BybitBook` / OKX `_books`)
  is the exception: the next applied delta already yields a whole book,
  so the late joiner waits one update, not forever.

"Not REST inside a push" (below) forecloses polling the venue for a
fresh snapshot. The remaining answers are "silent until the venue
pushes" or a per-identity cache the socket replays to joiners. Folded
books are already the second shape.

**Something already unsubscribes.** `BybitPublicStream._resync` →
`_resubscribe` and `OkxPublicStream._resubscribe` send `UNSUBSCRIBE`
then `SUBSCRIBE` on a wire identity when a book gap is detected. That
is a second writer to wire state. A shared ledger must not swallow the
resync `SUBSCRIBE` as a duplicate (or the snapshot never arrives) and
must not treat the resync `UNSUBSCRIBE` as "this name is free" for
another consumer. The round trip already blinds every co-reader of that
identity.

**`unsubscribe()` is a loaded gun.** `BinanceStreamSocket.unsubscribe`
and `BybitPublicStream.unsubscribe` close every `_Sub` whose index
intersects the named topics; Bybit also drops the shared `_books`
entry. Tests call both. Gate is worse: `unsubscribe(channel, payload)`
closes every sub on the *channel* and ignores the payload
(`gate/future/client.py`, `gate/spot/client.py`), so one contract's
unsubscribe takes the others with it. These methods are the I3
violation as public API. They have to be restricted or re-keyed before
any optional last-consumer unsubscribe lands.

**A wire identity is not a string on every venue.** Binance and Bybit
name streams as strings. OKX's unit is a subscribe arg, reduced by
`ch.arg_key()` to `(channel, instId, instType)`. Gate's unit is a
payload item *inside* a channel, not the channel. The second ledger's
key is **per-socket and opaque**. Keying Gate on the channel recreates
the `unsubscribe()` bug.

**Liveness is derived, not counted.** Both existing shared
implementations decide "still wanted" by scanning `_subs`
(`BybitPrivateStream._wanted()` is the named shape).
`EventStream.close()` is idempotent (`stream.py`). A numeric
refcount plus a double-close is a silent blinding.

**`_restore` order is load-bearing.** OKX clears `_subscribed` *before*
the restore request, then updates after the ack. A failed restore
leaves the set empty, so the next `subscribe_*` re-sends. Updating the
set first, then failing the request, sticks the name as subscribed
with nothing on the wire.

## Why product refcount zero is the wrong unsubscribe signal

```
one STS session
  md:
    - ticker.BinanceFuture_Perp_BTCUSDT      # pump A
    - bestquote.BinanceFuture_Perp_BTCUSDT   # pump B
```

MD's ledger after attach:

| Product key | refcount |
|---|---|
| `ticker.BinanceFuture_Perp_BTCUSDT` | 1 |
| `bestquote.BinanceFuture_Perp_BTCUSDT` | 1 |

What the futures sockets actually hold (today, without sharing):

| Venue stream | Who opened it |
|---|---|
| `@ticker` | pump A |
| `@bookTicker` | pump A *and* pump B (two `_Sub`s, one data path) |

Detach `bestquote` only. MD refcount for that key goes `1 → 0`. The
bestquote pump stops. `@bookTicker` is still required by the ticker
pump. A naive `UNSUBSCRIBE @bookTicker` at product-zero would blind the
feed that is still live.

The other direction is the same bug. Detach `ticker` only: product
refcount for `ticker` is zero, but that pump was holding `@ticker` *and*
`@bookTicker`. `@ticker` can go; `@bookTicker` cannot, because
bestquote is still up.

Two sessions on the *same* product key do not create this problem. That
is what MD's refcount already does: the venue pump exists once, and
zero means stop it. The gap is **across product keys that share or
split venue topics**. That cross-key case is the test; "Dispatcher
keys on a tuple" is not.

A future `funding_rate.Bybit_Perp_BTCUSDT` next to
`ticker.Bybit_Perp_BTCUSDT` is the same many-to-one on
`tickers.BTCUSDT`. Treating either key's zero as "drop the Bybit
topic" is wrong while the other pump is alive.

## Where the second ledger belongs

Not in MD.

MD must not grow a table of hook → venue channels. That table is
per-venue, changes when a venue splits hosts or folds fields into an
existing topic, and is exactly the shared interface
`mftik.exchange.base` refused: a contract a caller has to branch on is
not one it can rely on. `VenueSession` already stops at `stream_*`.

The second ledger lives on **the socket that sends `SUBSCRIBE`**, keyed
by an opaque per-socket identity (string, `arg_key` tuple, or
channel+payload item). Bybit private already derives the live set;
OKX public already stores one. Neither reserves before the ack.

| Ledger | Owner | Key | Zero means |
|---|---|---|---|
| Product | MD `Dispatcher` | `topic.UniversalTicker` | stop that `stream_*` pump |
| Wire | venue feed / socket | opaque identity on that socket | (optional) `UNSUBSCRIBE` that identity |

A pump still opens whatever `stream_*` it needs. Each `subscribe_*`
either sends `SUBSCRIBE` (first local consumer of that identity) or
only hangs another `EventStream` on the existing subscription. When MD
stops a pump, the iterator closes its streams; the socket *derives*
whether the identity still has a `_Sub`. Only the last consumer of an
identity is allowed to think about `UNSUBSCRIBE`.

**Reservation, then ack, then commit.** Checking `_subscribed` and
awaiting the venue is a race: two pumps both find the identity absent
and both send. The fix is to reserve the identity *before* the await
(or hold a per-socket lock — `BinanceFutureStream._socket_lock` already
guards socket creation for the same kind of double-open), and roll the
reservation back if the ack fails. Without that, I2 as "at most one
SUBSCRIBE" is not delivered by a straight port of the code this
document used to cite as the model.

Reconnect `_restore` sends each live identity **once**, from
`_wanted()`-shaped scan of `_subs`, not from flattening every `_Sub`'s
args. Clear the reserved set before the restore request so a failed
restore can be retried.

## Invariants

Each is meant to be a test.

- **I1** Stopping one product pump never drops a wire identity another
  live pump still reads. (The cross-key case above — `bestquote` to
  zero must not `UNSUBSCRIBE @bookTicker` while `ticker` is up.)
- **I2** A venue socket holds a given wire identity at most once. Two
  concurrent `subscribe_*` calls for the same identity send one frame,
  not two. Reservation (or a lock) is part of I2, not an
  implementation hint.
- **I3** A resync (`UNSUBSCRIBE` then `SUBSCRIBE` on a gapped book)
  is not a ledger open/close. It is not swallowed as a duplicate
  subscribe, and it does not mark the identity free for a co-reader.
- **I4** `_restore` after reconnect subscribes the set that
  `BybitPrivateStream._wanted()` would return: each live identity
  once, in first-seen order. The reserved set is empty if that
  request fails.
- **I5** A late joiner of a live identity is either handed a cached
  snapshot (folded books) or is defined as silent until the venue's
  next qualifying push. It does not send a second `SUBSCRIBE` to
  redraw one.
- **I6** MD still does not import venue stream vocabularies. Sharing,
  reservation, resync, and (later) unsubscribing are invisible above
  `stream_*`.

I2 is false on Binance and Bybit public, and false under concurrency
on OKX public and Bybit private. I3 is already live: resync is a
second writer, and `unsubscribe()` closes every intersecting `_Sub`.

## What to build

**First: share, with a reservation, and say what a late joiner sees.**

- **OKX public** is the existing reference. It needs I2's reservation
  (or a lock) plus rollback, and I4 (`_wanted()` instead of flattening
  `sub.args`). Its key is already `arg_key`.
- **Bybit private** needs the same reservation; `_wanted()` is already
  the restore shape.
- **Binance public, Bybit public, Gate** get a wire ledger for the
  first time. Gate's key is the payload item inside the channel, not
  the channel name. Binance's concurrent socket creation already uses
  `_socket_lock`; the subscribe reservation can follow that or be its
  own per-identity future.

Late-joiner policy, per `subscribe_*`, has to be chosen when the
method starts sharing — not later:

| Method | Late joiner |
|---|---|
| Folded book (`subscribe_order_book`) | Replay from `BybitBook` / OKX `_books` on the next fold, or immediately if a book is already complete |
| Unfolded book (`subscribe_book_deltas`) | Do not share this path with the folder, or refuse a second consumer — a joiner can never recover |
| Bybit / Gate `tickers` split into `ticker` + `funding_rate` | Silent until the field the pump cares about next appears. Document that; do not REST-fill |
| Binance `@bookTicker` shared by `ticker` + `bestquote` | Every push is a snapshot, so the next print is enough |

That is also what a Bybit `funding_rate` pump needs: it must read
`tickers.{symbol}` without a second subscribe, and without going
through `stream_ticker` (that pump drops unquoted deltas — the
funding updates).

**Then, only if a live unsubscribe is worth the race:** when the last
`EventStream` for an identity closes, send `UNSUBSCRIBE`. Before that
lands, restrict or re-key the existing `unsubscribe()` methods so they
cannot close a co-reader's `_Sub` or (on Gate) every sub on the
channel. The private stream's comment still applies — do this only
with a story for reconnect overlapping the unsubscribe, and without
confusing it with resync. Until that story exists, leaving the venue
topic up on an already-open socket is the known, cheaper wrong.

**MD's refcount and lifecycle do not change** — same feed keys, same
`ensure_feed` / `stop_feed`. A new product topic (`funding_rate`,
later `open_interest`) is still new surface: `TOPIC_*` and an `_open`
branch in `apps/md/.../venue.py`, `MD_*` in `protocol/messages.py`, a
shared model, and a row in the STS hook table. That is not a wire-ledger
change.

## What this is not

**Not a product-level merge.** Subscribing `ticker.` must not start
delivering `on_funding_rate`, and the reverse. Two product keys remain
two pumps, two STS hooks, two refcounts. Sharing is a socket
implementation detail.

**Not REST inside a push.** Filling a live feed's missing field from
REST (Gate's `funding_next_apply` on every ticker, or a snapshot for a
late joiner) is a different contract, and not one the tree states. A
feed yields what the venue pushed. A query answers once. See
`Strategy` on `md.fetch` versus the feed hooks, and
`mftik_md.fetch.readers`. Composing two *pushes* into one model
(`@ticker` + `@bookTicker`) is already allowed; polling REST to invent
a field is not. The late-joiner snapshot, if there is one, is an
in-process replay of a push the socket already applied.

**Not MD growing venue-specific refcounts.** If a comment or a
dashboard needs to explain why `@bookTicker` is still up after
`bestquote` went to zero, the answer is read off the socket's wire
ledger, not off `Dispatcher.refcounts()`.
