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

MDS-1 shipped on `exchange/wire-ledger` (`c555593`, [#16]). The tree now
shares a wire identity across pumps and reserves it before the ack — on
every venue except Gate, where the key it shipped is still too coarse to
deliver I2. MDS-1b through MDS-5 are open; MDS-6 is deliberately parked.
This stays the design record rather than becoming a changelog: what the
tickets say is what was built, and where a ticket and the tree disagree the
tree is the bug.

[#16]: https://github.com/lynxlinkage/mftik/pull/16

## Epic

**Move the subscribe decision off MD's product refcount and onto a
per-socket ledger of opaque wire identities, so several product feeds can
share one venue topic — and drop one — without blinding each other.**

### Why now

The next feeds on the list are funding and open interest, and every venue
serves them on a topic something else already reads:

| Venue | Funding arrives on | Open interest arrives on |
|---|---|---|
| BinanceFuture | `@markPrice` (`r`, `T`); `subscribe_mark_prices` exists, unwired | No USD-M WS channel; REST `/fapi/v1/openInterest` |
| Bybit perp | `tickers.{symbol}` — the topic `stream_ticker` reads | Same `tickers.{symbol}` |
| GateFutures | `futures.tickers` (`funding_rate`, parsed then dropped) | `total_size` on the same ticker |
| OKX SWAP | Dedicated `funding-rate` channel | Dedicated `open-interest` channel |
| Spot / Paper | N/A — refuse | N/A — refuse |

Bybit is the shape that forces this work. A `funding_rate` product feed has
to read `tickers.BTCUSDT` while `ticker` is already reading it, must not go
through `stream_ticker` (that pump drops unquoted deltas, which is exactly
what a funding-only update is), and must not send a second `SUBSCRIBE`.
Two product keys, one wire identity, and neither key's refcount describes
the identity.

### The problem

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

What the futures sockets hold:

| Venue stream | Who needs it |
|---|---|
| `@ticker` | pump A |
| `@bookTicker` | pump A *and* pump B |

Detach `bestquote` only. MD refcount for that key goes `1 → 0`. The
bestquote pump stops. `@bookTicker` is still required by the ticker pump.
A naive `UNSUBSCRIBE @bookTicker` at product-zero would blind a feed that
is still live.

The other direction is the same bug. Detach `ticker` only: product refcount
for `ticker` is zero, but that pump was holding `@ticker` *and*
`@bookTicker`. `@ticker` can go; `@bookTicker` cannot, because bestquote is
still up.

Two sessions on the *same* product key do not create this problem. That is
what MD's refcount already does correctly: the venue pump exists once, and
zero means stop it. The gap is **across product keys that share or split
venue topics**. That cross-key case is the test; "Dispatcher keys on a
tuple" is not.

### The shape that stays

- MD's unit stays the product feed. `Dispatcher` keys `(topic,
  UniversalTicker)`, `ensure_feed` on the first STS link,
  `_stop_feed_if_unused` at zero.
- STS hooks stay 1-1 with MD topics. `on_ticker` is `ticker`,
  `on_best_quote` is `bestquote`. The mismatch is not hook ↔ topic; it is
  `stream_*` ↔ the venue's own stream names.
- `VenueSession._open` stays a resolve-to-`stream_*`. A venue that cannot
  serve a topic has no such method and the subscribe is refused by name.
- Feeds keep yielding what the venue pushed. REST stays `fetch_*` /
  `md.fetch`.
- `EventStream.close()` stays idempotent (`stream.py`), so liveness is
  derived by scanning `_subs`, never counted.

### Non-goals

- **Not a product-level merge.** Subscribing `ticker.` must not start
  delivering `on_funding_rate`, and the reverse. Two product keys remain
  two pumps, two STS hooks, two refcounts. Sharing is a socket
  implementation detail.
- **Not REST inside a push.** Filling a live feed's missing field from
  REST (Gate's `funding_next_apply` on every ticker, or a snapshot for a
  late joiner) is a different contract, and not one the tree states. A feed
  yields what the venue pushed; a query answers once. See `Strategy` on
  `md.fetch` versus the feed hooks, and `mftik_md.fetch.readers`. Composing
  two *pushes* into one model (`@ticker` + `@bookTicker`) is already
  allowed; polling REST to invent a field is not. A late-joiner snapshot,
  if there is one, is an in-process replay of a push the socket already
  applied.
- **Not MD growing venue-specific refcounts.** If a dashboard needs to
  explain why `@bookTicker` is still up after `bestquote` went to zero, the
  answer is read off the socket's wire ledger, not off
  `Dispatcher.refcounts()`.
- **Not new product topics.** `funding_rate` and `open_interest` are the
  reason this epic exists and are not in it. See *Out of scope*.

### Invariants

Each is meant to be a test.

1. **I1 — Cross-key safety.** Stopping one product pump never drops a wire
   identity another live pump still reads. `bestquote` to zero must not
   `UNSUBSCRIBE @bookTicker` while `ticker` is up.
2. **I2 — One identity, one subscribe.** A venue socket holds a given wire
   identity at most once. Two *concurrent* `subscribe_*` calls for the same
   identity send one frame, not two. Reservation before the ack is part of
   I2, not an implementation hint.
3. **I3 — Resync is not a ledger op.** A book-gap `UNSUBSCRIBE` then
   `SUBSCRIBE` is not swallowed as a duplicate subscribe, and does not mark
   the identity free for a co-reader.
4. **I4 — Restore is a unique set.** `_restore` after reconnect subscribes
   each live identity once, in first-seen order — the shape
   `BybitPrivateStream._wanted()` returns. A fresh socket starts reserving
   nothing: *both* halves of the reservation — the acked `_held` and the
   in-flight futures — are empty after `clear()`, and a failed restore
   leaves them empty. `_held` alone is not the reserved set. (Today
   `clear()` only empties `_held`; MDS-1b.)
5. **I5 — A late joiner is defined.** Joining a live identity either
   replays a cached snapshot or is documented as silent until the venue's
   next qualifying push. It does not send a second `SUBSCRIBE` to redraw
   one.
6. **I6 — MD stays out of venue vocabulary.** MD does not import venue
   stream names. Sharing, reservation, resync and (later) unsubscribing are
   invisible above `stream_*`.

### Where the second ledger belongs

Not in MD.

MD must not grow a table of hook → venue channels. That table is per-venue,
changes when a venue splits hosts or folds fields into an existing topic,
and is exactly the shared interface `mftik.exchange.base` refused: a
contract a caller has to branch on is not one it can rely on.

| Ledger | Owner | Key | Zero means |
|---|---|---|---|
| Product | MD `Dispatcher` | `topic.UniversalTicker` | stop that `stream_*` pump |
| Wire | venue feed / socket | opaque identity on that socket | (later) `UNSUBSCRIBE` that identity |

A wire identity is **per-socket and opaque**: a Binance stream name, a
Bybit topic string, an OKX `ch.arg_key()` tuple `(channel, instId,
instType)`, and on Gate a `(channel, payload item)` — one contract, not one
call. Keying Gate on the channel alone recreates the `unsubscribe()` bug
below.

Gate is the one venue where the tree does not yet match that. MDS-1 keyed it
`(channel, tuple(payload))` — the whole call's payload
(`gate/spot/client.py`, `gate/future/client.py`) — so
`subscribe_tickers("BTC_USDT")` and
`subscribe_tickers("BTC_USDT", "ETH_USDT")` are two different keys and
`BTC_USDT` goes out twice, on the first subscribe and again on every
restore. I2 is still broken on Gate. MDS-4 owns the re-key.

Binance futures holds one ledger per endpoint socket (`/public`,
`/market`), which is right: they are two connections, and `st.group_of`
decides which one a name lands on.

### Current → target

| | Before MDS-1 | Now (MDS-1) | Target |
|---|---|---|---|
| Duplicate `subscribe_*` | second `SUBSCRIBE` on Binance / Bybit public / Gate | one frame, second stream attached | unchanged |
| Concurrent `subscribe_*` | both callers sent (OKX, Bybit private) | leader sends, waiter awaits | unchanged |
| Reconnect `_restore` | flattened every `_Sub`, duplicates on the wire | `first_seen` unique set | unchanged |
| Failed restore | set could stick as "subscribed" | `_held` cleared before the request, `_inflight` not | MDS-1b |
| Gate identity | none | `(channel, whole payload)` — overlapping calls still double-subscribe | MDS-4 |
| OKX / futures sharing tests | n/a | none — OKX has no WS stub at all | MDS-1b |
| Late joiner | accidental snapshot from the duplicate frame | **nothing** — silent, and a folded book is reset | MDS-2 |
| Book-gap resync | direct `request`, bypasses the ledger | same, unpinned by any test | MDS-3 |
| `unsubscribe()` | closed every intersecting `_Sub` | same, and now frees the ledger key too | MDS-4 |
| Refcount-zero → wire | never wired | never wired | MDS-6, or never |

## What is already true

Facts this design rests on, all of them checkable in the tree today.

**`_open` does not subscribe.** It returns an async generator. The
connector's `subscribe_*` runs on the first iteration, inside the pump task
`ensure_feed` just spawned (`apps/md/src/mftik_md/session/venue.py`). The
comment there — opened here so a missing method fails the attach — is about
`hasattr` and topic parsing, not about the venue ack. Two pumps started one
after another therefore race their `subscribe_*` calls. Ticker and bestquote
on the same instrument are the headline case, and that race is why I2 needs
a reservation rather than a check.

**One-to-many already exists.** `BinanceFuturePublicClient._tickers` opens
`@ticker` *and* `@bookTicker` for one MD `ticker` feed, because the 24h
ticker has no quote. Nothing is emitted until both halves have arrived. One
product key, two venue subscriptions, two sockets.

**Many-to-one already exists.** `stream_trades` and `stream_agg_trades` on
Binance futures both call `subscribe_agg_trades` — there is no raw tape,
only `@aggTrade`. `stream_ticker` and `stream_best_quote` both read
`@bookTicker`.

**A venue topic is a set membership, not a second pipe.** One WebSocket
either is or is not subscribed to a given wire identity. A second
`SUBSCRIBE` for the same identity does not give a second stream of prints.
Binance typically acks it as a no-op. Bybit's *private* socket answers a
duplicate subscribe with an error.

**Every venue socket now shares — with two caveats.** `WireLedger`
(`packages/common/src/mftik/exchange/wire.py`) holds `_held` after the ack
and `_inflight` futures as the reservation before the send. It is wired into
`BinanceStreamSocket`, `BybitPublicStream`, `BybitPrivateStream`,
`OkxPublicStream`, `OkxPrivateStream`, `GateSpotWebSocket` and
`GateFuturesWebSocket`. `first_seen` is the `_wanted()` shape for restore.
Concurrent callers of one key wait on the leader's future; a failed send
rolls the reservation back and fails the waiters.

The caveats are that Gate's key is the whole call's payload rather than the
contract (MDS-4), and that `clear()` empties only `_held` and leaves
`_inflight` holding futures from a socket that is gone (MDS-1b). Both are
narrower than the pre-MDS-1 behaviour and neither is what the invariants
claim.

**A late joiner gets no snapshot.** Bybit and OKX push a snapshot when a
subscription *starts*, then deltas. Before MDS-1, the duplicate `SUBSCRIBE`
accidentally redrew one. Now the second consumer of a live identity joins
mid-stream with nothing. Three cases, and they differ:

- a `funding_rate` pump joining an already-live `tickers.BTCUSDT` publishes
  nothing until the next `fundingRate` field — possibly hours;
- `subscribe_book_deltas` joining a live `orderbook.N` can never build a
  book; it will only ever see deltas;
- a *folded* book (`subscribe_order_book`) is the recoverable one: the next
  applied delta already yields a whole book, so the joiner waits one update
  — **except** that `subscribe_order_book` assigns
  `self._books[topic] = BybitBook(...)` unconditionally
  (`bybit/feed.py:339`, and `okx/feed.py:241` for `OkxBook`), so a second
  caller wipes the fold the first one depends on and no fresh snapshot is
  coming.

That last one is a regression MDS-1 introduced and MDS-2 owns.

**Something already unsubscribes.** `BybitPublicStream._resync` →
`_resubscribe` and `OkxPublicStream._resubscribe` send `UNSUBSCRIBE` then
`SUBSCRIBE` on a wire identity when a book gap is detected. Both call
`self.request` directly, so they bypass the ledger — correct by
construction, and pinned by nothing. The round trip blinds every co-reader
of that identity for one RTT.

**`unsubscribe()` is a loaded gun.** `BinanceStreamSocket.unsubscribe` and
`BybitPublicStream.unsubscribe` close every `_Sub` whose index intersects
the named topics; Bybit also drops the shared `_books` entry. Gate is worse:
`unsubscribe(channel, payload)` closes every sub on the *channel* and
ignores the payload (`gate/future/client.py`, `gate/spot/client.py`), so one
contract's unsubscribe takes the others with it. `BinanceFutureStream.unsubscribe`
inherits the Binance behaviour by fanning names out to each group's socket.
MDS-1 made all of them call `WireLedger.discard`, which means they now also
hand the identity back while a co-reader is still on it. They are the I1
violation as public API — reachable only from tests today, since MD's detach
path never calls them, but reachable.

**Liveness is derived, not counted.** Both pre-existing shared
implementations decided "still wanted" by scanning `_subs`
(`BybitPrivateStream._wanted()` is the named shape) and `WireLedger` keeps
that: it stores no consumer count. `EventStream.close()` is idempotent, so a
numeric refcount plus a double-close would be a silent blinding.

**Handover already decouples pump lifetime from refcount.** `docs/MdHandover.md`
pins feeds during a blue/green swap: a pinned feed pumps with refcount zero,
and `_stop_feed_if_unused` respects the pin. Product-zero is already not
"stop", which is one more reason it cannot be "unsubscribe". Blue and green
are separate processes with separate sockets, so their ledgers are
independent — a handover is not a shared-identity case.

## Tickets

Each ticket leaves the tree shippable. Later tickets may be empty behaviour
so earlier ones can merge.

### MDS-1 — Per-socket wire ledger with reservation — **shipped** (`c555593`)

**Goal.** Two consumers of one venue identity on one socket cause one
`SUBSCRIBE`, including when they race, and a reconnect replays each
identity once.

**Scope.** New `packages/common/src/mftik/exchange/wire.py`. Wired into
`binance/feed.py`, `bybit/feed.py`, `bybit/account.py`, `okx/feed.py`,
`okx/account.py`, `gate/spot/client.py`, `gate/future/client.py`. Tests in
`packages/common/tests/test_wire_ledger.py` plus a sharing case on the
venues that already had a WebSocket stub.

**Problem.** OKX public and Bybit private already skipped a duplicate
subscribe, but both did check-then-`await`: two concurrent callers each saw
the key absent and each sent. Binance public, Bybit public and Gate did not
share at all — they sent the name twice, replayed it twice on reconnect, and
relied on the venue no-op. Meanwhile MD's pumps race `subscribe_*` by
construction, because `_open` defers the real call into the pump task.

**Solution.** `WireLedger[K]` with `_held` (acked) and `_inflight`
(reserved, a future per key). `acquire(keys, send)` reserves under a lock
*before* awaiting `send`, so the second caller of an in-flight key waits on
the leader instead of sending. A failed send drops the reservation and fails
the waiters, so a later `acquire` retries. `clear()` runs *before* a restore
request, so a failed restore leaves the set empty rather than sticking a
name as subscribed with nothing on the wire. `first_seen` gives `_restore`
the unique, first-seen-order set. `discard` exists for an explicit venue
unsubscribe and is deliberately not called by resync.

**Verify.** What shipped, exactly — not "a case per venue":

- `test_wire_ledger.py`: first-seen order, held-skip, concurrent single
  send, rollback, waiter failure, clear-before-restore, discard.
- `test_two_consumers_share_one_venue_subscription`: Bybit private, Bybit
  public, Binance spot, Gate spot, Gate futures.
- `test_concurrent_consumers_share_one_venue_subscription`: Bybit private.
- `test_reconnect_resubscribes_a_shared_topic_once`: Bybit private and
  Bybit public. `test_reconnect_resubscribes_a_shared_stream_once`:
  Binance spot.

Not covered: OKX public, OKX private, Binance futures. MDS-1b.

**Depends.** Nothing. Blocks every other ticket.

### MDS-1b — Finish MDS-1: `_inflight` on clear, and the untested sockets

**Goal.** `clear()` empties the whole reservation, so I4 is true as written;
and every socket MDS-1 touched has a sharing test.

**Scope.** `packages/common/src/mftik/exchange/wire.py` (`clear`), its tests,
a new `packages/common/tests/okx_stub.py`, an OKX socket test module, and a
futures case in `packages/common/tests/test_binance_future_feed.py`.

**Problem.** Two gaps, both left by MDS-1.

`clear()` empties `_held` and leaves `_inflight` alone. A future parked
there belongs to a round trip on a socket that no longer exists, and
`acquire` treats it as "somebody is already sending this" — so the next
caller becomes its waiter and sends no frame of its own. The window is real
on a flapping socket: reconnect does **not** cancel pending requests (only
`close()` does, `socket.py:158-161`), it goes straight
`_open` → `_on_open` → `_restore`. So a drop *during* a restore leaves the
previous lap's `request` parked on `asyncio.wait_for(..., ack_timeout)`,
and the new lap's `acquire` waits behind it instead of re-sending. It
self-heals — the old `wait_for` eventually raises, the failure path fails
the waiters, the lap fails and retries — but the cost is a wasted reconnect
lap and up to `ack_timeout` of a feed that is not restored, for a socket
that is already unhealthy. `_held` being empty is not the invariant; the
reservation being empty is.

Second, MDS-1 changed `OkxPublicStream` and `OkxPrivateStream` and tested
neither, because there is no OKX WebSocket stub in the tree at all —
`tests/*stub*.py` is Binance, Binance futures, Bybit, Gate and Gate futures.
OKX is also the only implementation whose `send` does a key → arg reverse
lookup (`by_key[key]` in `okx/feed.py` and `okx/account.py`); a wrong
mapping there subscribes to the wrong channel and says nothing. Binance
futures is untested for a different reason: it is the venue in this epic's
headline example and its two-socket split means one ledger per endpoint,
which nothing asserts.

**Solution.** Make `clear()` fail every `_inflight` future with a
`ConnectionError`-shaped exception and drop the map, so a caller parked on a
dead leader is released immediately rather than at `ack_timeout`, and the
next `acquire` sends. `clear()` has to stay synchronous because `_teardown`
is a plain `def` on every socket — `_restore` is `async` and could await,
but the sync caller decides the signature — so `_inflight` mutation has to
be safe outside `self._lock`; the simplest correct form is to swap the dict
out under a plain reference and resolve the futures from the copy.

Failing the waiters is only half of it: the *leader* also has to learn its
reservation was voided. Today `acquire`'s success path does
`self._held.update(to_send)` unconditionally, so a send that acks after a
`clear()` marks the key held on a socket that no longer exists — and the
next `subscribe_*` would then skip it and subscribe to nothing. Give the
ledger a generation counter that `clear()` bumps, snapshot it when the
reservation is taken, and commit to `_held` only if it is unchanged. A
leader from an older generation drops its result on the floor, which is the
right outcome: its frame went to a dead connection.

For OKX, write `okx_stub.py` against the same shape as `bybit_stub.py`
(subscribe/unsubscribe acks, a `subscribed` set, `frames_for`, `push`,
`drop`), then the sharing and restore cases. For Binance futures, add a
sharing case beside `test_unsubscribing_goes_to_the_socket_that_carries_it`
using the existing `future_public_stream` / `future_market_stream`
fixtures.

**What this ticket deliberately does not fix.** Failing fast makes an
existing hole easier to fall into. A `subscribe_*` that raises propagates
out of `acquire`, up through the connector, into `VenueSession._pump`, which
catches `Exception`, logs `MD %s pump failed`, and returns — leaving the
`self._feeds` entry in place, so every later `ensure_feed` for that key
early-returns and the pump is never restarted. The product refcount says the
feed is alive and nothing is reading the socket. `ensure_feed` opens the
source outside the task on purpose, "so an unsupported topic … fails the
subscribe call instead of dying silently in a background pump", but `_open`
only constructs the generator; the connector's `subscribe_*` runs on the
first iteration, which is inside the pump. So that guard does not cover this
class at all. Before MDS-1b a waiter reached that path after burning
`ack_timeout`; after it, immediately. Restarting a dead pump is an MD-side
fix, not a ledger one — see *Out of scope*.

**Verify.**

- `clear()` with a key in flight: the parked waiter raises at once, and the
  next `acquire` for that key sends a frame.
- Ledger-level: reserve, `clear()`, `acquire` same key → exactly one new
  send, no wait on the dead future.
- A leader whose `send` returns *after* a `clear()` does not add its key to
  `_held`, and a later `acquire` for that key still sends.
- Socket-level: drop a Bybit public socket while a first-time subscribe for
  a topic is awaiting its ack, then drop again during the restore; the
  topic ends up subscribed without waiting out `ack_timeout`.
- OKX public: two consumers on `tickers.BTC-USDT` → one `subscribe` frame,
  both receive; reconnect replays it once; the arg reaching the wire has
  the `instId` the caller asked for, not another key's.
- OKX private: two `subscribe_orders` → one frame, both receive.
- Binance futures: two consumers on `btcusdt@bookTicker` → one `SUBSCRIBE`
  on `/public` and none on `/market`; the same name on the other socket
  would be a separate identity. **This is where the epic's headline case is
  counted** — `ticker.` and `bestquote.` reach `@bookTicker` through this
  client, and MDS-5 asserts the MD half without counting frames.

**Depends.** MDS-1. Independent of MDS-2, MDS-3 and MDS-4.

### MDS-2 — Late-joiner policy, per `subscribe_*`

**Goal.** Every shared `subscribe_*` states what its second consumer sees,
and no shared path is silently unrecoverable. I5.

**Scope.** `bybit/feed.py` (`subscribe_order_book`, `subscribe_book_deltas`,
`_fold_book`), `okx/feed.py` (`subscribe_order_book`, `_fold_book`),
docstrings on the ticker-family methods. No MD change.

**Problem.** MDS-1 removed the duplicate `SUBSCRIBE` that was accidentally
redrawing a venue snapshot, and nothing replaced it. Two concrete failures.
First, `subscribe_order_book` assigns a fresh `BybitBook` / `OkxBook` on
every call, so a second consumer resets a fold the first consumer is reading
and no new snapshot is coming — the book recovers only by the gap path
firing a resync, which is a round trip of blindness for a case that should
need none. Second, `subscribe_book_deltas` joining an identity a folder
already holds sees deltas from mid-stream forever and can never build a
book; it does not fail, it just never becomes correct.

**Solution.** Fix the reset first: `setdefault`, not assign, so an existing
fold survives a joiner. Then replay — if the book is already complete, push
one `snapshot()` into the new `EventStream` before returning it, so the
joiner starts from state the socket already had. That is an in-process
replay of a push, not REST.

**The replay and the `_subs.append` must be in the same event-loop step,
with no `await` between them.** Read the snapshot, build the stream, push
the snapshot, append the `_Sub` — one synchronous block after `acquire`
returns. Append first, or yield in the middle, and `_push` can enqueue a
newer book ahead of the replay, so the joiner reads a fresh book and then
an older one. Nothing downstream can detect that; it just quietly rewinds.

The two mixed orders are not symmetric, and both need an answer:

- **Folder first, then `subscribe_book_deltas`.** Refuse. The joiner sees
  deltas from mid-stream forever and can never build a book — it does not
  fail, it just never becomes correct, which is the worst shape available.
- **`subscribe_book_deltas` first, then folder.** Allow, at a stated cost.
  `_books` has no entry, `setdefault` builds a stale one, the first delta
  makes `apply` return `False` and triggers a resync — so the topic is
  blind for one RTT and the existing raw consumer pays for it too. That is
  acceptable and refusing is not: `subscribe_order_book` is MD's normal
  path and `subscribe_book_deltas` is the escape hatch, so the escape hatch
  must not be able to lock out the main path. Make the resync deliberate
  and log it as "folding a raw-held topic" rather than letting it surface
  as a gap warning.

Then write the policy down per method, because it has to be chosen when a
method starts sharing and not later:

| Method | Late joiner |
|---|---|
| Folded book (`subscribe_order_book`), folder already present | Replay `BybitBook` / OKX `_books` if complete, else the next fold |
| Folded book joining a raw-held topic | One deliberate resync; blind for one RTT, including for the raw consumer |
| Unfolded book (`subscribe_book_deltas`) joining a folder | Refuse — a joiner can never recover |
| Unfolded book joining another unfolded consumer | Shared, no replay, deltas from here on |
| Bybit / Gate `tickers` split across `ticker` + `funding_rate` | Silent until the field that pump reads next appears. Documented, not REST-filled |
| Binance `@bookTicker` shared by `ticker` + `bestquote` | Every push is a snapshot, so the next print is enough |
| OKX `bbo-tbt`, `books5` | Always-snapshot channels; next push is enough |

**Verify.**

- Two `subscribe_order_book` calls on one live topic: one `SUBSCRIBE`
  frame; the first stream keeps delivering; the second receives a complete
  book without the venue pushing anything new.
- The same, with the first book mid-fold: the joiner's first book equals
  the first consumer's latest, and the fold's `update_id` / `seq_id` is not
  reset.
- Replay ordering: push a newer book to the topic immediately after the
  joiner subscribes; the joiner reads the replay first and the newer book
  second, never the reverse.
- `subscribe_book_deltas` after `subscribe_order_book` on the same identity
  raises, with a message that names the folder.
- `subscribe_order_book` after `subscribe_book_deltas` on the same identity
  succeeds, sends no second `SUBSCRIBE` from `acquire`, triggers exactly one
  resync, and both consumers are correct once the snapshot lands.
- `subscribe_book_deltas` alone still works, shares with a second delta
  consumer, and neither gets a replay.
- A gap on the shared topic still resyncs exactly once (MDS-3's assertion,
  re-run here to catch a `setdefault` that broke `resyncing`).

**Depends.** MDS-1. Independent of every other open ticket; expect textual
conflicts with MDS-3 in `bybit/feed.py` and `okx/feed.py`.

### MDS-3 — Pin resync as a force path, not a ledger operation

**Goal.** I3 becomes a test, so a later refactor cannot route resync
through `acquire` or `discard`.

**Scope.** `bybit/feed.py._resubscribe`, `okx/feed.py._resubscribe`, and a
test per venue. Possibly a rename (`_force_resubscribe`) to make the intent
unmissable at the call site.

**Problem.** Resync is a second writer to wire state, and both mistakes are
one-line changes an unaware refactor would make. Route it through
`acquire` and the identity is already `_held`, so no frame goes out, so no
snapshot arrives, so the gapped book stays dead — silently, because the
subscribe "succeeded". Call `discard` on the way out and a co-reader's
identity is marked free, so the next `acquire` for it sends a duplicate
frame and, once MDS-6 exists, a last-consumer close could unsubscribe a
topic somebody else is reading. Today the code is correct only because it
happens to call `self.request` directly.

**Solution.** Keep the direct-`request` path and state why in the docstring.
Assert the ledger is untouched across a resync: `held()` before equals
`held()` after, and the frame count is exactly one `UNSUBSCRIBE` plus one
`SUBSCRIBE`. Record the co-reader cost (blind for one RTT) next to
`_resync`, so nobody reaches for a per-consumer resync as the fix.

**Verify.**

- Bybit: two folded consumers on `orderbook.50.BTCUSDT`; force a gap;
  exactly one `unsubscribe` frame and one extra `subscribe`; `held()`
  unchanged; both consumers recover on the fresh snapshot. Two folded
  rather than one folded plus one raw, because MDS-2 refuses that pair.
- OKX: the same against `books` with a `prevSeqId` gap.
- A resync while a concurrent `subscribe_*` for the same identity is
  in flight does not produce two `SUBSCRIBE` frames from `acquire`.

**Depends.** MDS-1. Independent of every other open ticket; expect textual
conflicts with MDS-2 in `bybit/feed.py` and `okx/feed.py`.

### MDS-4 — Re-key Gate to the payload item, and make `unsubscribe()` last-reader-only

**Goal.** Gate holds one identity per contract, not per call, so I2 holds
there too; and no public method can close a co-reader's stream or hand back
an identity another `_Sub` still holds. I1 and I2 at the socket boundary.

**Scope.** Two halves, one re-key.

- Subscribe side: `gate/spot/client.py._subscribe`,
  `gate/future/client.py._subscribe` and their `_resubscribe`, moving the
  ledger key from `(channel, tuple(payload))` to one key per payload item.
- Unsubscribe side: `binance/feed.py.unsubscribe`,
  `bybit/feed.py.unsubscribe`, `binance/future/feed.py.unsubscribe`,
  `gate/spot/client.py.unsubscribe`, `gate/future/client.py.unsubscribe` —
  the last-reader rule, and making each one's local half survive a send
  that fails.
- Tests that already call these: `test_binance_spot_client.py`,
  `test_binance_future_feed.py` (`test_unsubscribing_goes_to_the_socket_that_carries_it`),
  `test_gate_spot_client.py`.

**Problem.** Two defects, and they share a cause: the key is the call, not
the thing.

On Gate, MDS-1 keyed the ledger by the whole payload tuple, so
`subscribe_tickers("BTC_USDT")` and
`subscribe_tickers("BTC_USDT", "ETH_USDT")` reserve two different
identities and `BTC_USDT` is subscribed twice — on the first call and again
on every reconnect replay. I2 is simply not delivered on Gate, and a verify
built from two disjoint payloads would never notice.

On the unsubscribe side, Binance and Bybit close every `_Sub` whose index
intersects the named identities, and Gate ignores the payload entirely and
closes the whole channel. `BinanceFutureStream.unsubscribe` inherits this
by fanning out to each group's socket — and BinanceFuture is the venue in
this epic's headline example. Before MDS-1 that was already wrong; after
MDS-1 it is worse, because each one now calls `WireLedger.discard` and so
also hands back an identity a surviving reader depends on.

Neither defect is reachable from MD today — the detach path never calls
these methods (see MDS-5) — but both are reachable from the public API, and
both have to be safe before MDS-6 can exist.

**Solution.** Re-key Gate to the payload item: one ledger key per
`(channel, item)`, so overlapping calls share the item they have in common
and `_resubscribe` sends each contract once. The `_Sub` still records the
call's full payload for routing; only the ledger identity gets finer.

Make `unsubscribe()` mean "I am the last reader", and **raise** when it is
not — do not no-op with a warning. A silent no-op is a new member of the
category this whole document argues against: an operation that appears to
succeed and quietly does nothing. On Gate the re-key makes the common case
legal rather than an error, because unsubscribing one contract no longer
touches another's identity; close only the `_Sub`s whose payload matches and
discard only those keys.

Two edge cases the current code answers by accident, and this ticket has to
answer on purpose.

**An unsubscribe that cannot reach the venue.** `BinanceFutureStream.unsubscribe`
forwards only when `socket is not None and socket.connected`, which reads
like a disconnect guard and is not one. `_connected` goes False in exactly
three places — `__init__`, `close()`, and `_fail()` — and the reconnect loop
touches none of them; `_open()` only rebinds `_conn`. So during a reconnect
gap `connected` is still True, and the one path that clears it, `_fail()`,
calls `_teardown()` on the next line, which has already closed every `_Sub`.
Both branches are therefore correct no-ops, but for a reason worth writing
in the docstring: `socket is None` means the group was never opened, and
`not connected` means the socket already gave up and took its streams with
it. Neither can strand a `_Sub`.

The strandable case is the one the guard lets through. In the reconnect gap
`connected` is True and `_conn` still points at the dead connection
(`_conn = None` only in `close()`), so `_ensure_connected()` passes;
`_pumping` is False for that whole window, so `request()` takes the
`handshake()` path and writes straight to a socket that is gone. It raises,
and `BinanceStreamSocket.unsubscribe` raises with it — *before*
`self._ledger.discard(names)` and before `sub.stream.close()`. The caller
sees a failure, the `_Sub` is still in `_subs`, and `_restore` replays the
name after the reconnect. The subscription comes back from the dead.

So this is an ordering problem, not a branching one. Close the matching
`_Sub`s in a `finally` so restore cannot resurrect the name, and let the
wire error propagate. Discard the ledger keys only after the venue acks:
a rejected `UNSUBSCRIBE` means the socket is still carrying the identity,
and a follow-up `SUBSCRIBE` (which Bybit refuses) must not go out.
Closing the stream is the local fact that cannot fail; the reservation
is a claim about the venue.

**A Gate `_Sub` that spans several items.** Routing stays at channel
granularity (`_push` matches `s.channel == resp.channel`), so a `_Sub` built
by `subscribe_tickers("BTC_USDT", "ETH_USDT")` cannot be half-unsubscribed:
drop `BTC_USDT` on the wire and it survives, still matching the channel,
silently missing one contract. Do not document that as a cost — make it
unrepresentable. The last-reader rule already covers it if the `_Sub`'s
payload is read as its claim: `unsubscribe(channel, ["BTC_USDT"])` raises
when a live `_Sub` on that channel claims `BTC_USDT` and is not itself fully
covered by this call. A multi-item `_Sub` is then closed by naming all of its
items, and never by naming some.

The existing tests unsubscribe an identity nobody else holds, so they stay
legal and unchanged.

**Verify.**

- Gate spot: `subscribe_tickers("BTC_USDT")` then
  `subscribe_tickers("BTC_USDT", "ETH_USDT")` → the second call sends
  `ETH_USDT` only; `BTC_USDT` appears in exactly one subscribe frame.
- Gate spot reconnect after those two calls: each contract replayed once.
- Gate futures: subscribe `BTC_USDT` and `ETH_USDT` on `futures.trades`
  separately; `unsubscribe(TRADES, ["BTC_USDT"])` leaves the ETH stream
  open and its ledger key held.
- Binance spot: two consumers on `btcusdt@aggTrade`; `unsubscribe` raises
  and both streams keep delivering; with one consumer it closes and sends
  `UNSUBSCRIBE`.
- Binance futures: two consumers on `btcusdt@bookTicker` (`/public`);
  `unsubscribe` raises; the `/market` socket is untouched either way.
- Binance futures, group never opened: `unsubscribe("btcusdt@aggTrade")`
  with no `/market` socket returns quietly and sends nothing.
- Binance, unsubscribe inside the reconnect gap: subscribe, drop the
  connection, call `unsubscribe` before it reconnects. The call raises, the
  stream is closed anyway, the ledger key is free, and the restore after
  reconnect does **not** replay that name.
- Gate spot, partial unsubscribe of a multi-item `_Sub`:
  `subscribe_tickers("BTC_USDT", "ETH_USDT")` then
  `unsubscribe(TICKERS, ["BTC_USDT"])` raises, and naming both closes it.
- Bybit public: the same as Binance spot, and the shared `_books` entry is
  not dropped while a co-reader holds the topic.
- Existing `test_unsubscribe_closes_the_streams_reading_it`,
  `test_unsubscribe_closes_the_stream` and
  `test_unsubscribing_goes_to_the_socket_that_carries_it` pass unchanged.

**Depends.** MDS-1. Blocks MDS-6. Blocks nothing in MDS-5.

### MDS-5 — Cross-key scenarios at the MD boundary

**Goal.** I1 and I6 asserted where the epic's claim actually lives: two MD
product pumps over one venue identity, with a detach.

**Scope.** `apps/md/tests/test_md_venue_feeds.py` for the attach half — it
already drives topic → venue stream resolution. The detach scenarios want
their own file (`test_md_shared_venue_topics.py`), because
`test_md_detach_disconnect.py` is about a detach not blocking the control
plane, not about what a detach leaves subscribed. No production change
expected; if one is needed, it is a bug this ticket found.

**Stay at the MD layer.** These tests assert against `VenueSession` and
`Dispatcher` with a recording `FakePublic`, not against sockets.
`test_md_venue_feeds.py`'s connector is a hand-written `FakePublic` whose
`stream_*` methods return `_once()` generators; there is no socket under it
and no frame to count. Counting frames from here would mean standing up a
real `BinanceFuturePublicClient`, two `FakeBinanceStream`s for `/public` and
`/market`, and a `StubSymbols` — a different harness than this file uses,
for an assertion that belongs one layer down anyway. **The wire-frame count
is MDS-1b's Binance futures case.** What MD can and should assert is which
`stream_*` the connector was asked for, which sources were closed, and which
pumps stay fed. Extend `FakePublic` to record both — the calls, and a
`finally` in `_once()` that records the closure — and assert on that. Every
scenario below is phrased against those recordings; if one of them needs a
frame count, it is in the wrong ticket.

**Problem.** Every ticket above is socket-local. None of them says anything
about MD. The claim that matters is the one in *The problem* section:
`ticker.` and `bestquote.` on one BinanceFuture instrument open one
`@bookTicker`, and dropping `bestquote` leaves `ticker` fed. Nothing in the
tree tests it. The claim splits cleanly in two: *one identity* is counted on
the socket in MDS-1b, and *the survivor keeps eating* is asserted here,
where MD's refcount is the thing that could get it wrong.

The detach half is testable **now**, and passes now. A detach never reaches
a socket's `unsubscribe()`: `_stop_feed_if_unused` calls
`VenueSession.stop_feed`, which cancels the pump task, which runs the
generator's `finally: stream.close()`, which fires `_drop`. Nothing on that
path sends a frame — the only callers of a socket `unsubscribe()` anywhere
in the tree are tests. So S2 and S3 assert behaviour that is already
correct, which is exactly why they are worth writing: the epic's central
claim is true today and nothing would notice if a later change made it
false.

**Solution.** Scenario tests, named and stable:

- **S1 — Split.** Attach `ticker.` and `bestquote.` for one instrument.
  Assert two pumps exist, each asked the connector for its own `stream_*`,
  and both product feeds publish. That the two land on one wire identity is
  MDS-1b's assertion, not this one.
- **S2 — Detach one key.** Drop `bestquote`. Its pump stops, its source is
  closed, and the product refcount is zero — while the `ticker` source is
  *not* closed and `ticker.` publishes on the next push. Whether the venue
  identity is still subscribed is MDS-1b's question; MD's obligation is to
  not close the survivor.
- **S3 — Detach the other.** From S1, drop `ticker` instead. `bestquote`
  keeps publishing.
- **S4 — Same key, two sessions.** Two STS links on `ticker.` share one
  pump — the connector is asked for `stream_ticker` once — and dropping one
  link leaves the pump running and the other link fed. This already works;
  the test states that the new ledger did not change it.
- **S5 — Many-to-one on one venue topic.** Two product topics that resolve
  to one venue stream: two product feeds, and dropping one leaves the other
  publishing. Again the pair-to-one-identity mapping is checked at the
  socket layer; here the claim is that MD's refcount does not close the
  survivor.
- **S6 — MD names no venue channel.** MD imports connectors and REST
  readers because it must (`session/factory.py`, `fetch/readers.py`); what
  it must never import is a venue's stream-name vocabulary. Assert that no
  module under `apps/md/src` imports a venue `channels` or `streams`
  module. Verified true today, so this is a lint-shaped test that starts
  green and stays that way.

**Verify.** The scenarios exist and pass against the names MDS-1 actually
shipped. Do not write them against imagined helpers.

**Depends.** MDS-1, and nothing else. All six scenarios can be written as
soon as the ledger is in, which is now. MDS-4 does not gate S2/S3 — the
detach path does not go through `unsubscribe()` — and MDS-4 carries its own
tests for the API it changes.

### MDS-6 — Last-consumer `UNSUBSCRIBE` — parked

**Goal.** An identity nothing reads stops arriving, without racing
reconnect, resync or a handover pin.

**Scope.** `WireLedger` plus each socket's `_drop`. Not started, and not
scheduled.

**Problem.** An idle topic on an already-open socket costs bandwidth and
parse time, and a long-lived MD process accumulates them. That is the whole
upside, and it is small.

The downside is a race with three writers. `BybitPrivateStream._drop`
already documents one: TD closes and reopens the order stream around a
reconnect, and a socket that had unsubscribed in between would miss
whatever arrived in the gap. Resync is the second — MDS-3 exists because an
unsubscribe and a resync look identical on the wire. Handover pins are the
third: `docs/MdHandover.md` has a feed pumping at refcount zero, so "no
consumer" is not a stable fact during a swap.

**Solution.** Only with a story for all three. Derive the last reader by
scanning `_subs` (never a counter — `EventStream.close()` is idempotent and
a double-close would decrement twice). Send `UNSUBSCRIBE` then `discard`,
under the same lock `acquire` uses, so a concurrent `acquire` for that
identity either waits and re-sends or is serialised behind the close.

Until that story exists, leaving the venue topic up on an already-open
socket is the known, cheaper wrong.

**Verify.** Not specified. Write it with the ticket, not before.

**Depends.** MDS-1b, MDS-2, MDS-3, MDS-4 — the reservation has to be
complete (MDS-1b) before a close can be serialised against it. Blocked on a
decision that the bandwidth is worth the race, which has not been made.

## Order

```
MDS-1 wire ledger + reservation             [shipped, c555593]
  ├── MDS-1b _inflight on clear, OKX + futures coverage
  ├── MDS-2  late-joiner policy
  ├── MDS-3  pin resync as a force path
  ├── MDS-4  Gate per-item key + last-reader-only unsubscribe()
  └── MDS-5  cross-key scenarios (all six)

MDS-1b + MDS-2 + MDS-3 + MDS-4
  └── MDS-6  last-consumer UNSUBSCRIBE      [parked]
```

Everything below MDS-1 is unblocked today and nothing in that row gates
anything else in it. The coupling that exists is textual: MDS-2 and MDS-3
both edit `bybit/feed.py` and `okx/feed.py`, so expect conflicts rather than
ordering constraints.

Recommended order if they land one at a time:

1. **MDS-2** — the only one with a live regression behind it. MDS-1 removed
   the accidental snapshot and `subscribe_order_book` still resets the fold.
2. **MDS-4** — I2 is not actually delivered on Gate. The epic claims it is.
3. **MDS-5** — test-only, no production change, and it stays inside
   `test_md_venue_feeds.py`'s existing `FakePublic` idiom, which is what
   keeps it cheap. Runs in parallel with any of the others.
4. **MDS-1b** — correctness plus the two sockets MDS-1 changed blind, and
   the home of the headline frame count. The OKX half is the largest single
   piece of work in the epic because it needs a stub that does not exist.
5. **MDS-3** — a guard rail on behaviour that is already correct.

MDS-6 is not scheduled and its `Verify` is deliberately empty.

## Docs that stay right

`docs/MdHandover.md` needs no edit. "A feed exists because somebody holds
it" is still true of the product ledger, and feed pinning is still the
exception to refcount-zero. This epic adds a second ledger below it and
changes neither `ensure_feed` nor `_stop_feed_if_unused`.

Update a doc in the ticket that makes its sentence false, not in a mop-up
ticket.

## Funding rate (product topic)

`TOPIC_FUNDING_RATE` is a product key. `_open` resolves it to
`stream_funding_rate`; STS delivers `MD_FUNDING_RATE` to
`on_funding_rate`. History is a fetch, not a feed:
`mds.fetch_funding_history` → `on_fetch_funding_history`. The shared
`FundingRate` is `rate` + `ts` only — no `next_funding_time`, no
`next_funding_rate`.

A late joiner on a ticker-shared wire (Bybit `tickers.{symbol}`, Gate
`futures.tickers`) is silent until the next rate-bearing delta. The pump
is not REST-filled. Two product keys remain two pumps; sharing is a
socket detail. Split venues (Paper, Gate spot, Binance spot) refuse the
feed by missing method. Unified venues (Bybit, OKX) refuse spot inside
the method.

## Out of scope (later epics)

- **`open_interest` as a product topic.** Same shape. Note that
  BinanceFuture USD-M has no WS channel for it, so that venue would have to
  refuse the feed rather than poll REST inside a push.
- **`next_funding_time`.** Bybit sends it on the ticker and the model drops
  it (`extra="ignore"`); Gate does not send it on the wire at all
  (`funding_next_apply` is REST, in unix seconds). Deriving it needs either
  a model change plus a venue that pushes it, or a symbol-plane field —
  not a feed that polls.
- **A shared `Ticker` carrying funding.** The shared model is
  bid/ask/last/ts. Conversion belongs on the venue wire models, not on
  `Ticker`.
- **A dead pump that never restarts.** Its own epic, and the largest
  silent-failure left in MD. `VenueSession._pump` catches `Exception`, logs,
  and returns with the `self._feeds` entry still present, so `ensure_feed`
  early-returns forever: the product refcount claims a live feed that has no
  reader. Any `subscribe_*` failure reaches it — MDS-1b only changes how
  fast. Fixing it means deciding what a dead pump should do (drop the
  `_feeds` entry and let the next `ensure_feed` rebuild, or retry with
  backoff and a give-up that tells STS), which is a control-plane question
  this epic has no business answering.
- **Per-identity metrics or a dashboard of the wire ledger.** `held()` is
  enough to answer "why is `@bookTicker` still up" from a REPL.
- **Cross-socket sharing.** Two sockets to the same host are two ledgers.
  Collapsing Binance futures' `/public` and `/market` into one connection
  is a venue-shape question, not a ledger one.
