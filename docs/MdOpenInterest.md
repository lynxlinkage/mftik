# Open interest — product topic and a snapshot fetch

`funding_rate` is already a product key. A session subscribes
`funding_rate.UniversalTicker`, MD resolves it to
`stream_funding_rate`, STS delivers `MD_FUNDING_RATE` to
`on_funding_rate`. History is a different call:
`mds.fetch_funding_history` → `on_fetch_funding_history`. The shared
`FundingRate` is `rate` + `ts`. A venue that cannot push the feed has
no method, and the subscribe is refused by name.

`open_interest` is the same shape. This is the design record for that
product, not a changelog: what the tickets say is what gets built, and
where a ticket and the tree disagree the tree is the bug.

`docs/MdVenueSubscriptions.md` is why two product pumps can share one
venue topic. That epic is the prerequisite, not this one. MDS-1 already
ships the wire ledger; this epic does not wait on MDS-2 through MDS-6.

GitHub: epic [#52](https://github.com/lynxlinkage/mftik/issues/52).
Tickets [OI-1](https://github.com/lynxlinkage/mftik/issues/53) through
[OI-4](https://github.com/lynxlinkage/mftik/issues/56).

## Epic

**`open_interest` as a product feed and a one-shot snapshot, on the
same rails as `funding_rate` / `fetch_best_quote`. A venue that
cannot push it refuses the subscribe. A venue that cannot answer a
snapshot refuses the query. Nothing polls REST inside a push.**

### Why now

The venue survey is done, and the tree already has the pattern. What
is left is to name the product and fill the venues that actually
publish it.

| Venue | Live OI arrives on | Current snapshot |
|---|---|---|
| Bybit perp | `tickers.{symbol}` — same wire as `ticker` and `funding_rate` | `GET /v5/market/tickers` (`openInterest`) |
| OKX SWAP | Dedicated `open-interest` channel, about every 3s | `GET /api/v5/public/open-interest` |
| GateFutures | `futures.tickers.total_size` — same wire as `ticker` and `funding_rate` | REST ticker `total_size` |
| BinanceFuture | **No WS.** `@ticker` / `@markPrice` do not carry it | `GET /fapi/v1/openInterest` |
| BinanceDelivery | **No WS.** | `GET /dapi/v1/openInterest` |
| Spot / Paper | N/A — refuse | N/A — refuse |

Bybit and Gate are the shared-wire case MDS-1 was built for. OKX is a
new channel on an existing public socket. Binance is fetch-only: a
strategy that wants a series there uses a timer and
`fetch_open_interest`, which is honest, and is not MD inventing a
stream the venue does not have.

### The shape that stays

- MD's unit stays the product feed. `Dispatcher` keys
  `(topic, UniversalTicker)`. `_open` resolves `open_interest` to
  `stream_open_interest`.
- STS hooks stay 1-1 with MD topics. `on_open_interest` is
  `open_interest`. Sharing a venue topic with `ticker` is a socket
  detail, not a merge of hooks.
- A venue that cannot serve the feed has no `stream_open_interest`,
  and the subscribe is refused by name. Unified venues (Bybit, OKX)
  refuse **spot** *inside* the method — and only spot. Every contract
  book they list has open interest, a dated future included, which is
  where this parts company with `stream_funding_rate`.
- Feeds keep yielding what the venue pushed. REST stays `fetch_*` /
  `md.fetch`. Filling a live feed from REST is not a contract this
  tree states.
- `on_fetch_open_interest` is a snapshot of *now*, like
  `on_fetch_bestquote`. It is not history. History would be a third
  name (`fetch_open_interest_history`) and is out of scope.

### Non-goals

- **Bucketed OI history.** Binance `/futures/data/openInterestHist`,
  Bybit `GET /v5/market/open-interest` (requires `intervalTime`),
  Gate `GET /futures/{settle}/contract_stats` and the
  `futures.contract_stats` WebSocket, OKX rubik stats. Those are a
  later query, if anything. Do not implement them under
  `fetch_open_interest`.
- **REST inside a push.** Binance has no OI stream. The feed is
  refused there. Tardis polling `/fapi/v1/openInterest` every six
  seconds and labelling it `@openInterest` is not a model this tree
  copies.
- **`futures.contract_stats` as the Gate live feed.** That channel
  is interval-bucketed (`["BTC_USDT","1m"]`). Live OI on Gate is
  `total_size` on `futures.tickers`.
- **A shared `Ticker` carrying OI.** The shared ticker is
  bid/ask/last/ts. Conversion belongs on the venue wire models.
- **`oiUsd` / `openInterestValue` / `singleOpenInterest` on the
  shared print.** Same reason `FundingRate` dropped
  `next_funding_time`.
- **MDS tickets.** Late-joiner book folds, Gate re-key, last-consumer
  `UNSUBSCRIBE` stay in `docs/MdVenueSubscriptions.md`.

### Invariants

Each is meant to be a test.

1. **I1 — Push is a push.** `stream_open_interest` yields a print the
   venue sent. It does not GET. BinanceFuture and BinanceDelivery have
   no such method.
2. **I2 — Snapshot is a snapshot.** `fetch_open_interest` answers
   once, current OI, on `md.fetch`. It does not subscribe, and it does
   not take an interval.
3. **I3 — Two keys, two pumps.** Subscribing `open_interest.` does
   not start delivering `on_ticker` or `on_funding_rate`, and the
   reverse. Detaching one leaves the others fed.
4. **I4 — Refuse by name, and only spot.** Split venues (Paper, Gate
   spot, Binance spot) have no `stream_open_interest` /
   `fetch_open_interest`. Unified venues refuse a **spot** ticker inside
   the method; a dated future is answered, not refused. The subscribe
   fails at attach; the query comes back `MD_VENUE_UNSUPPORTED_READ`,
   which a reader earns by raising `NoReaderError` — that type is what
   `mftik_md.errors.BY_TYPE` maps to the code, and a bare `ValueError`
   lands on `MD_VENUE_CALL_FAILED` instead.
5. **I5 — Thin model, comparable unit.** `OpenInterest` is `qty` + `ts`.
   `qty` is **one side, in base** on every base-denominated book — Bybit
   linear, OKX SWAP, GateFutures, BinanceFuture — so the same underlying
   compares across the four without the caller knowing whose contract is
   whose. BinanceDelivery is the stated exception and says so: its
   contract is USD, so `qty` there is **contracts**, like every other
   public size this tree reports on that venue.
6. **I6 — Late joiner is silent on a shared ticker.** A second pump
   joining Bybit `tickers.{symbol}` or Gate `futures.tickers` publishes
   nothing until the next OI-bearing delta. It is not REST-filled.
7. **I7 — MD names no venue channel.** Same lint as MDS-5 S6.

### Shared model

```
class OpenInterest(InstrumentScoped):
    qty: Decimal
    ts: float
```

`qty` is **one side's** open interest — the market convention, and what
Binance, Bybit and Gate already report — in the unit the venue's other
public sizes already use:

| Venue | Native field | Shared `qty` | Comparable |
|---|---|---|---|
| BinanceFuture | REST `openInterest` | as sent (base) | yes — its own book |
| Bybit linear | `openInterest` | as sent (base) | yes vs Gate |
| GateFutures | `total_size` | `total_size * contract_size` (base) | yes vs Bybit |
| OKX SWAP | `oiCcy` (else `oi * ctVal`) | base, as sent | unit yes; ~½ of Bybit/Gate |
| BinanceDelivery | REST `openInterest` | as sent (contracts) | n/a — different product |

Those four are why a unit is stated at all. A strategy holding
`open_interest` on BTC across Bybit, OKX, GateFutures and BinanceFuture
compares four numbers, and a venue whose native figure needs a factor
applies it **in its own reader** rather than leaving every caller to
know which venue counts which way.

**OKX is one side, and is not halved.** Measured 2026-09-03 on
BTC-USDT perp, using the fields this table names:

| Venue | Native | Shared `qty` (BTC) |
|---|---|---|
| BinanceFuture | `openInterest` | 108144.552 |
| Bybit linear | `openInterest` | 55688.648 |
| GateFutures | `total_size * 0.0001` | 58278.184 |
| OKX SWAP | `oiCcy` | 29187.818 |

Bybit and Gate agree. OKX is half of those two, not double. Binance
is its own book (~1.9× Bybit, ~3.7× OKX), not a 2× of anyone. A
systematic 2× on OKX alone would have meant both sides and a
halving in `OkxReader`; that is not what landed.

Bybit now also publishes `singleOpenInterest` (27844.324, half of
`openInterest`) and Gate's contract row has `position_size`
(29139.092 base, half of `total_size`). Those pair with OKX's
`oiCcy`. This ticket does not change the chosen fields, and does
not invent a halving on Bybit or Gate either.

**BinanceDelivery is contracts, and that is the answer rather than a
gap.** It is this tree's only `Inverse` book — a USD-margined contract
on a different instrument from BinanceFuture's `Perp` — and nothing
here asks the two to be compared. The mechanics agree with the
decision: `contractSize` is USD per contract, not base, so
contracts → base would need a mark price the OI endpoint does not
carry, and every other public size this tree reports on that venue is
already contracts — book, tape and liquidations all
(`binance/delivery/public.py`), with klines taking
`quote_per_contract` rather than converting. A second GET to convert
this one field would make it disagree with the rest of its own venue
to buy a comparison nobody asked for. So `qty` there is contracts by
decision, not by shortfall, and a later ticket "fixing" it is
reopening a settled question.

`ts` is the venue's event time when the print carries one, local
receive time when it does not — same sentence as `FundingRate`.

Bybit inverse (`openInterest` in USD) is not a category this venue
lists. If it ever is, that conversion is a ticket of its own.

## What is already true

**`funding_rate` is the template.** Topic → `stream_*` → STS hook;
history (there) / snapshot (here) → `mds.fetch_*` → `on_fetch_*`.
Missing method refuses. Unified venues refuse spot in the method. A
late joiner on a ticker-shared wire is silent. Two product keys stay
two pumps.

**The wire ledger exists.** MDS-1 (`c555593`) shares one venue
identity across pumps. Bybit `tickers.{symbol}` and Gate
`futures.tickers` can grow a third reader (`open_interest`) without a
second `SUBSCRIBE`. MDS-5 already detaches `funding_rate` from
`ticker` at the MD boundary; OI is the same scenario with a new
topic name.

**The fields are already on the wire and already dropped.**
`BybitTicker` and `GateFuturesTicker` use `extra="ignore"`. Neither
parses `openInterest` / `total_size`. `to_funding_rate` is the
shape `to_open_interest` copies.

**OKX already has the dedicated-channel shape.**
`subscribe_funding_rate` + `OkxFundingRate.to_funding_rate` is the
OKX half. `open-interest` is a new arg on the same public socket.

**Binance mark-price is not a hiding place.**
`BinanceFutureMarkPrice` has `r` / `T`, not OI.
`subscribe_mark_prices` does not become this feed.

**Gate `contract_stats` is the wrong tool for a live print.** The
channel requires an interval. REST `/contract_stats` is the same
bucketed series. Current OI is `total_size` on the ticker (and
`position_size` on the contract row — same long-side figure).

**Fetch already refuses a missing reader method.**
`FetchSession` looks up `read` on the venue reader the same way
`VenueSession._open` looks up `stream_*`. OI-1 can register the kind
before any reader grows `fetch_open_interest`; the query comes back
`MD_VENUE_UNSUPPORTED_READ` until OI-2.

## Tickets

Each ticket leaves the tree shippable. Later tickets may be empty
behaviour so earlier ones can merge.

### OI-1 — Shared model, protocol, STS hooks, MD topic — [#53](https://github.com/lynxlinkage/mftik/issues/53)

**Goal.** The product exists as a name. A session can subscribe
`open_interest.` and call `mds.fetch_open_interest`. Every venue
refuses both, by missing method. Nothing publishes.

**Scope.**

- `OpenInterest` on `mftik.exchange.models` (`qty` + `ts`), exported
  from `mftik.exchange`.
- Protocol: `MD_OPEN_INTEREST`, `MD_FETCH_OPEN_INTEREST`,
  `MD_OPEN_INTEREST_RESULT`, `MdFetchOpenInterest`,
  `MdOpenInterestResult` (`open_interest: OpenInterest | None`).
  `None` only on failure — an `ok` result with `qty` of zero is a
  real print.
  Re-exported from `mftik.protocol` — `funding_rate` carries six
  entries there, and a name that is not exported is not a name.
- `Strategy.on_open_interest` / `on_fetch_open_interest` and the
  class docstring. `StrategyMds.fetch_open_interest`, whose docstring
  counts its reads ("Four reads:") and now counts five.
- `VenueSession`: `TOPIC_OPEN_INTEREST` → `stream_open_interest` →
  `MD_OPEN_INTEREST`. `MarketDataConnector` keeps the method off the
  protocol, same as `stream_funding_rate` — its docstring enumerates
  the optional streams and gains a sixth.
- `FetchSession._KINDS` for the new request. No reader grows the
  method here.
- STS `MD_HANDLERS` / `MD_FETCH_HANDLERS`.

**Problem.** Without the name, every venue ticket edits the same
protocol files and the same STS dispatch table. The refuse-by-name
rule also has nothing to refuse.

**Solution.** Land the contract with no venue methods. Attach and
query fail the way `funding_rate` already fails on Paper. Factory
tests pin `not hasattr(..., "stream_open_interest")` on every
client, including the perps — OI-3 / OI-4 flip the ones that grow
it; BinanceFuture / BinanceDelivery stay false for the life of this
epic.

**Verify.**

- `OpenInterest` round-trips `model_dump(mode="json")` the way
  `FundingRate` does.
- `test_feed_topic_publishes_its_message_type` grows
  `("open_interest", MD_OPEN_INTEREST)` on a `FakePublic` that has
  `stream_open_interest`.
- `test_a_venue_without_open_interest_refuses_that_topic` — missing
  method, `ValueError` matching `stream_open_interest`, `feed_count`
  stays 0.
- `test_md_events_reach_every_hook` delivers `MD_OPEN_INTEREST` to
  `on_open_interest`.
- `mds.fetch_open_interest` is acked, and the result hook fires with
  `ok` False / `MD_VENUE_UNSUPPORTED_READ` against today's readers
  (`test_md_fetch.py` / `test_mds_query.py` style).
- Factory: every venue client, including GateFutures / Bybit / Okx /
  BinanceFuture / BinanceDelivery, `not hasattr` `stream_open_interest`.

**Depends.** Nothing in this epic. Does not wait on MDS-2..6.

### OI-2 — Snapshot fetch on every contract venue — [#54](https://github.com/lynxlinkage/mftik/issues/54)

**Goal.** `mds.fetch_open_interest(ticker)` returns current OI on
every contract venue the tree trades. Spot and paper stay
`MD_VENUE_UNSUPPORTED_READ`.

**Scope.** `fetch_open_interest` on each contract `*PublicRest` and
the matching MD reader. No WebSocket. No interval argument.

| Reader | Endpoint | Field → `qty` |
|---|---|---|
| BinanceFuture | `GET /fapi/v1/openInterest` | `openInterest` (base) |
| BinanceDelivery | `GET /dapi/v1/openInterest` | `openInterest` (contracts) |
| Bybit | `GET /v5/market/tickers` | `openInterest` (base). **Not** `/v5/market/open-interest` |
| OKX | `GET /api/v5/public/open-interest` | `oiCcy`, else `oi * contract_size` |
| GateFutures | `GET /futures/{settle}/tickers` | `total_size * contract_size` |

Bybit already has `fetch_ticker_row`. Parse `openInterest` on
`BybitTicker` here (needed again by OI-4; landing it on the wire
model once is the point). Gate parses `total_size` on
`GateFuturesTicker` the same way — but has nothing that hands the row
back: `GateFuturesPublicRest.fetch_ticker` converts to a shared
`Ticker` and drops it, so this ticket adds the row-returning read that
Bybit already has. OKX's endpoint is scoped by `instType`, not by
`instId` alone, and its path constant lands in `okx/channels.py`
beside the other REST paths — the same file OI-3 adds a channel to.

**Problem.** Binance will never grow the feed. A strategy that needs
OI there has only this call. Bybit's dedicated OI REST is a history
series and would silently become the wrong product if someone
reached for the name.

**Solution.** One reader method, one REST GET, one `OpenInterest`.
Bybit spot / OKX spot raise `NoReaderError` inside the method — that
type, not a bare `ValueError`, is what the plane maps to
`MD_VENUE_UNSUPPORTED_READ`, and it is what
`BybitReader.fetch_funding_history` already raises. Only spot is
refused: a Bybit dated future has open interest and is answered. Gate
uses the ticker row, not `/contract_stats`.

**Verify.**

- Per-venue read tests next to the funding-history ones
  (`test_md_binance_future_reads.py`,
  `test_md_binance_delivery_reads.py`, `test_md_bybit_reads.py`,
  `test_md_okx_reads.py`, `test_md_gate_future_reads.py`): path,
  query, `qty`, `ts`, `universal_ticker`.
- Bybit / OKX spot → `MD_VENUE_UNSUPPORTED_READ`, asserted through
  `normalize(NoReaderError(...))` the way `test_md_bybit_reads.py`
  already asserts it for funding history.
- Bybit dated future is answered, not refused — the opposite of
  `test_a_dated_future_has_no_funding_rate_stream`.
- Gate / paper / Binance spot readers still have no method.
- `test_md_fetch.py` / `test_mds_query.py`: an `ok` result carries
  one `OpenInterest`; a missing method still fails as in OI-1.
- A unit test that Bybit's reader does not call
  `/v5/market/open-interest`.
- **One live calibration, recorded in this doc rather than in a test.**
  `qty` for one underlying on OKX against Bybit, BinanceFuture and
  GateFutures. Those three agree or the reading is wrong; a systematic
  2x on OKX alone means its figure is both sides, and the halving plus
  its reason land in `OkxReader` and in the shared-model table. This is
  the ticket that owns I5's comparability, and it does not merge on an
  unmeasured guess.

**Depends.** OI-1. Independent of OI-3 and OI-4.

### OI-3 — OKX live feed — [#55](https://github.com/lynxlinkage/mftik/issues/55)

**Goal.** `open_interest.Okx_Perp_*` pushes `OpenInterest` from the
dedicated `open-interest` channel. Spot is refused inside the
method.

**Scope.** `okx/channels.py` (`OPEN_INTEREST`, `open_interest()` —
the same file OI-2 puts `MARKET_OPEN_INTEREST` in),
`OkxOpenInterest` + `to_open_interest`, `subscribe_open_interest`,
`OkxPublicClient.stream_open_interest`. Factory `hasattr` flips for
Okx only.

**Problem.** OKX is the only venue whose live OI is not folded into
something else. Implementing it after Bybit/Gate would bury a clean
channel in a shared-wire ticket.

**Solution.** Copy `stream_funding_rate`. SWAP only — which here is
*refuse spot* under another name, not the narrowing OI-4 rejects: this
venue lists Spot and Perp (`venues.py`), and `Perp` is the only
category that resolves to SWAP. `oiCcy` when present; otherwise
`oi * contract_size`. Skip a row with no usable size rather than
inventing zero. Whatever factor OI-2's calibration settles applies
here too — the channel and the endpoint report the same figure, and
one of them silently disagreeing with the other is the bug this
sentence exists to catch.

If OKX ever lists FUTURES here, the open question is the **unit**, not
the refusal: `oi * ctVal` is base, while that instrument's book, tape
and quotes would still be contracts, and I5 says `qty` uses the unit
the venue's other public sizes already use. That is a ticket, not a
line in this one.

**Verify.**

- `test_okx_public.py`: a push on `ch.open_interest("BTC-USDT-SWAP")`
  yields `OpenInterest` with base `qty` and the venue `ts`;
  `next` fields stay off the shared print.
- A row with empty `oi` / `oiCcy` is skipped.
- Spot raises before the iterator runs
  (`test_spot_has_no_funding_rate_stream` shape).
- Factory: Okx `hasattr` `stream_open_interest`; every other client
  still does not.
- `test_okx_feed.py`: subscribe arg is the `open-interest` channel
  with the asked `instId`, not `tickers`.

**Depends.** OI-1. Independent of OI-2 and OI-4.

### OI-4 — Bybit and Gate live feeds on the shared ticker wire — [#56](https://github.com/lynxlinkage/mftik/issues/56)

**Goal.** `open_interest` on Bybit perp and GateFutures reads the
ticker topic already subscribed for `ticker` / `funding_rate`,
sends no second `SUBSCRIBE`, and yields only when the delta names
OI. Spot (Bybit) is refused inside the method; a dated future is
not.

**Scope.** `BybitTicker.to_open_interest` /
`GateFuturesTicker.to_open_interest` (fields may already be on the
model from OI-2). `stream_open_interest` on both public clients,
sharing `subscribe_tickers`. Factory `hasattr` flips for Bybit and
GateFutures. MDS-5-style detach cases for the new topic.

**Problem.** `stream_ticker` drops an unquoted Bybit delta, which is
exactly an OI-only update. Going through that pump would hide the
print. A second `SUBSCRIBE` for `tickers.BTCUSDT` would violate
MDS I2. Gate's `total_size` is currently discarded.

**Solution.** A third pump on the same `subscribe_tickers` call,
mirroring `_funding_rates` — except in what it refuses. The guard is
its own category set, **not** `FUNDING_CATEGORIES`: that set is `{Perp}`
because a dated future settles at expiry instead of paying a hook, and
a dated future still has open interest. Yield when `to_open_interest`
returns a row; skip otherwise. Gate multiplies `total_size` by
`contract_size`.
Late joiner: silent until the next OI-bearing push — documented on
the method, not REST-filled. MDS-2's ticker-family row gains
`open_interest` next to `funding_rate`; that is a doc edit in this
ticket, not a MDS-2 dependency.

**Verify.**

- Bybit: a delta with only `openInterest` yields OI and does not
  yield a `Ticker`; a quoted delta without `openInterest` yields
  neither OI. Two consumers (`subscribe_tickers` from ticker and
  from OI) send one subscribe frame
  (`test_two_consumers_share_one_venue_subscription` shape).
- Bybit spot raises before the iterator runs. A dated future does
  **not** — the mirror image of
  `test_a_dated_future_has_no_funding_rate_stream`, and the reason
  this topic gets its own category set. (`Bybit_Future_*` is not a
  category the registry builds; it is reachable through
  `UniversalTicker.parse`, which is what that funding test already
  uses.)
- Gate: a `futures.tickers` push with `total_size` yields base
  `qty`; a push without it is skipped. Same one-frame sharing
  against `stream_ticker` / `stream_funding_rate`.
- `test_md_shared_venue_topics.py`: detach `open_interest` leaves
  `ticker` (and `funding_rate` if up) fed; detach `ticker` leaves
  `open_interest` fed.
- Factory: Bybit and GateFutures `hasattr` `stream_open_interest`;
  BinanceFuture, BinanceDelivery, Paper, Gate spot, Binance spot
  still do not.
- Late-joiner policy is in the method docstring: silent, not
  REST-filled.

**Depends.** OI-1. Independent of OI-2 and OI-3. Expect textual
overlap with OI-2 on `BybitTicker` / `GateFuturesTicker` if they
land in either order — add the field once, convert in both paths.

## Order

```
OI-1  model + protocol + empty hooks          [unblocks the rest]
  ├── OI-2  snapshot fetch, every contract venue
  ├── OI-3  OKX dedicated open-interest channel
  └── OI-4  Bybit + Gate on the shared ticker wire
```

Nothing below OI-1 gates anything else in that row. The coupling that
exists is textual, in two files: OI-2 and OI-4 both touch the Bybit /
Gate ticker models, and OI-2 and OI-3 both touch `okx/channels.py` —
the REST path and the WS channel sit in the same module.

Recommended order if they land one at a time:

1. **OI-1** — the name. Without it the others fight over
   `messages.py` and the STS tables.
2. **OI-2** — every contract venue becomes queryable, including the
   two that will never stream. Smallest risk, widest use.
3. **OI-3** — clean channel, no sharing.
4. **OI-4** — third pump on a live shared identity. MDS-1 already
   makes that legal; this ticket is the product, not the ledger.

## Epic acceptance

All four `Verify` sections green, and the matrix holds:

| | Feed `open_interest.` | Fetch `fetch_open_interest` |
|---|---|---|
| Bybit perp | pushes | snapshot |
| OKX SWAP | pushes | snapshot |
| GateFutures | pushes | snapshot |
| BinanceFuture | refused at attach | snapshot |
| BinanceDelivery | refused at attach | snapshot |
| Spot / Paper | refused at attach | `MD_VENUE_UNSUPPORTED_READ` |

A strategy can subscribe OI on OKX / Bybit / GateFutures, query it
on those three plus both Binance contract venues, and never sees a
Binance pump pretending to stream.

## Docs that stay right

`docs/MdVenueSubscriptions.md` already points here — its *Out of
scope* entry reads "Moved to `docs/MdOpenInterest.md`". Its "Why now"
table stays: that is why the ledger epic existed, not a promise this
epic re-opens MDS tickets. Its MDS-2 late-joiner row now names
`open_interest` beside `funding_rate`, which is OI-4's edit and is
done.

`docs/MdHandover.md` needs no edit. Product-zero and pinning do not
change.

Update a doc in the ticket that makes its sentence false, not in a
mop-up ticket. OI-4 owns the MDS-2 late-joiner row that today names
only `funding_rate`.

## Out of scope (later epics)

- **`fetch_open_interest_history`.** Interval series, 30-day caps,
  venue-specific buckets. A different query.
- **Bybit inverse OI.** The venue does not list Inverse. Native
  `openInterest` there is USD, not base.
- **OKX expiry / option OI.** The venue lists Spot + Perp only.
- **A dead pump that never restarts.** Still the MDS out-of-scope
  item. A failed `subscribe_open_interest` dies the same way
  `subscribe_funding_rate` already can.
- **Paper OI.** Paper has no positions to aggregate. If a later
  paper epic wants a synthetic figure, it is a paper question.
