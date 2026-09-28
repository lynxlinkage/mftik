# Feed end — control event, not a feed

`on_feed_end` is how MD tells one STS that a subscription is over.
Dated expiry is one of those outcomes. A dead pump, a venue that
never connected, and a topic the venue does not publish are the
others.

This is not a product topic. It is not listed in `md_ids`. It is not
on the tape. STS receives it because `MD_HANDLERS` maps `md.feed.end`
to `on_feed_end`. Nothing is subscribed. `stop_feed` and detach do
not publish it.

`docs/MdVenueSubscriptions.md` is why two product pumps can share one
venue socket. This page is the control event for a feed that will
not keep printing. Last-reader unsubscribe already lives on the
public socket (MDS-6). ATM / strike-roll are not this.

## The shape

Wire type `md.feed.end` on `md.{session_id}`. Payload is `FeedEnd`:

| Field | Meaning |
|---|---|
| `universal_ticker` | The instrument, canonical |
| `topic` | The one product key (`ticker`, `greeks`, `kline_1h`, …) |
| `state` | `down`, `expired`, or `rejected` |
| `code` | `symbol_not_found`, `transport`, `connect`, `error`, `expired`, or `unsupported` |
| `reason` | One sentence |
| `expiry` | Listed settlement time. Set only when `code` is `expired` |
| `ts` | When MD emitted the print |

One print per `(session, topic)`. A session that held `ticker` and
`greeks` on one option gets two calls. A session that held only
`greeks` does not hear about `ticker`. MD publishes only to a
session whose lease is still up.

`down` means this attempt is over. `rejected` and `expired` mean
subscribing again gets the same answer. `reason` carries the venue's
own words, and its code when the exception has one.

A feed that cannot be opened fails the attach RPC. The code is the
specific refusal (`VENUE_SYMBOL_NOT_FOUND`,
`MD_VENUE_UNSUPPORTED_READ`, `MD_VENUE_NOT_CONNECTED`,
`invalid_feed`, …) and the deploy rolls back. `md.feed.end` is only
for a feed that did open and later ended.

A rebuilt STS re-attaches the feeds saved on its md record. If MD
refuses that attach, the session is marked `failed` with the error
and is not retried. A timeout, MD not answering yet, stays
`interrupted` so the next boot tries again. The saved feed list is
not edited here.

## When a pump ends

The pump task classifies its own exit. `stop_feed` (unsubscribe,
detach, or the cancel inside an expiry cut) sets `feed.stop` and
does not notify.

- `SymbolNotFoundError` — `down` / `symbol_not_found`. Other feeds
  on the same socket stay up.
- The iterator ends and nobody called `stop_feed` — `down` /
  `transport`. That socket gave up. MD retires each feed whose
  iterator ended, and only those. Another socket on the same
  connector keeps running, and so does a sibling still inside its
  publish. The connector stays. A later subscribe of a retired topic
  opens a fresh pump on it. The connector is dropped only once that
  session has no feeds left, and only if a newer session has not
  already replaced it.
- Any other exception — `down` / `error`. `reason` is the exception
  text, plus a venue code or label when the text does not already
  contain it.

Before the notify, MD clears that key's refcount and removes the
`Feed`, in one locked section with no `await`. A subscribe that
arrives after that opens a new pump. One that arrives before it is
included in the notify and cleared with the others.

## When the subscribe never starts

Attach is all or nothing. Any of these fails the RPC, detaches the
session, and does not publish `md.feed.end`. A feed already opened
earlier in the same attach is stopped with the rest.

- The feed key does not parse — `invalid_feed`, before the lease.
- The symbol plane has no such instrument — `VENUE_SYMBOL_NOT_FOUND`.
  The miss is not cached, so a later subscribe asks again and can
  still arm an expiry watch.
- `_open` refuses the topic — `MD_VENUE_UNSUPPORTED_READ`.
- Venue `connect()` fails — `MD_VENUE_NOT_CONNECTED`.

A runtime `md.subscribe` has no RPC reply to fail, so the same
refusals still publish `md.feed.end` to the sessions that joined
the key, and the refcount is cleared.

- Listed time already past — `expired` / `expired`, and only to the
  session that asked. It never entered the refcount. Attach still
  succeeds for the other feeds.

## When the instrument expires

On attach (and on a later `md.subscribe`), MD reads the symbol
plane for each ticker via `SymbolClient.get(..., include_inactive=True)`.
The default `get()` stays active-only — strategies and TD still
see a settled book as missing. The hourly refresh marks a settled
contract untradable rather than deleting it; only the expiry watch
asks for that row, so a rebuilt MD can still fire `on_feed_end`.

- Listed time in the future — open the pumps, sleep until it.
- Listed time already past, or inactive and past — do not
  `ensure_feed`. Tombstone the ticker, publish `expired` once per
  requested topic to the session that asked.
- Lookup failed — open if attach already asked for the feed, retry
  the plane; a later success that is past then cuts.
- Last reader leaves before expiry — the watch is cancelled. No
  print. A later attach re-arms.
- The timer fires — drop every feed on that ticker and publish
  `expired` per topic to each session that held that topic.

After the cut, attach / `md.subscribe` of the same ticker is
refused and notified. The tombstone is process memory; a wrong
listed time is cleared by restarting MD.

Spot and perpetual books have `expiry is None` and never arrive
with `code=expired`.

`MdAttachResult.subscriptions` lists only pumps that stayed up. The
hint that a requested feed was already settled is `on_feed_end`, not
an extra field on the attach result.

## What this is not

- Not a venue `UNSUBSCRIBE` of an idle live book. The public socket
  already does that after the last reader leaves (MDS-6).
- Not a way to subscribe ATM and roll the strike. That is a later
  architecture.
- Not tape. Warm-up still only has `trade` / `aggtrade`.
- Not an `up` status. The first print on the feed hook is the sign
  the pump is producing. Socket reconnect, while retries remain,
  does not publish this event.

## Tests

`apps/md/tests/test_md_expiry.py` — already expired, timer then
refuse resubscribe, detach-before-expiry, no listed expiry, second
attach after the tombstone, runtime sub/unsub order, detach during
subscribe, inactive settled row.

`apps/md/tests/test_md_feed_end.py` — symbol miss, transport,
two sockets on one connector, stop without a print, subscribe
during notify, unsupported topic with a second waiter, connect
failure, attach that fails when one feed cannot open.

`apps/sts/tests/test_md_events.py` — `md.feed.end` reaches
`on_feed_end`.

`apps/sym/tests/test_plane.py` —
`test_client_get_finds_inactive_settled_instrument`.
