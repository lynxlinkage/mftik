# Instrument expiry — control event, not a feed

A dated future or listed option has `SymbolInfo.expiry`. When that
time is reached, MD drops every feed on that ticker, will not reopen
them on this process, and notifies the sessions that held a feed
(or asked for one after it had already settled).

This is not a product topic. It is not listed in `md_ids`. It is not
on the tape. STS receives it because `MD_HANDLERS` maps `md.expiry`
to `on_expiry`, the same way a feed type maps to its hook — except
nothing is subscribed.

`docs/MdVenueSubscriptions.md` is why two product pumps can share one
venue socket. This page is the control event that retires the
instrument. Last-reader unsubscribe (MDS-6) and ATM / strike-roll
are not this.

## The shape

Wire type `md.expiry` on `md.{session_id}`. Payload is `Expiry`:

| Field | Meaning |
|---|---|
| `universal_ticker` | The instrument, canonical |
| `expiry` | Listed settlement time from the symbol plane |
| `topics` | Product keys that were cut (`ticker`, `greeks`, `kline_1h`, …) |
| `ts` | When MD emitted the print |

`Strategy.on_expiry` is the hook. One print per instrument per MD
process. A later MD process looks the listed time up again and will
not reopen a settled book; the rebuilt session may be notified once
more.

Spot and perpetual books have `expiry is None` and never arrive
here.

## When MD cuts

On attach (and on a later `md.subscribe`), MD reads the symbol
plane for each ticker. A single-instrument `get` includes inactive
rows: the hourly refresh marks a settled contract untradable rather
than deleting it, and the listed time is how a rebuilt MD still
fires `on_expiry` instead of treating the miss as "no expiry".

- Listed time in the future — open the pumps, sleep until it.
- Listed time already past, or inactive and past — do not
  `ensure_feed`. Tombstone the ticker, publish `md.expiry`.
- Lookup failed — open if attach already asked for the feed, retry
  the plane; a later success that is past then cuts.
- Last reader leaves before expiry — the watch is cancelled. No
  print. A later attach re-arms.

After the cut, attach / `md.subscribe` of the same ticker is
refused and notified. The tombstone is process memory; a wrong
listed time is cleared by restarting MD.

`MdAttachResult.subscriptions` lists only pumps that stayed up. The
hint that a requested feed was already settled is `on_expiry`, not
an extra field on the attach result.

## What this is not

- Not a venue `UNSUBSCRIBE` of an idle live book. That is MDS-6.
- Not a way to subscribe ATM and roll the strike. That is a later
  architecture.
- Not tape. Warm-up still only has `trade` / `aggtrade`.

## Tests

`apps/md/tests/test_md_expiry.py` — already expired, timer then
refuse resubscribe, detach-before-expiry, no listed expiry, second
attach after the tombstone, runtime sub/unsub order, detach during
subscribe, inactive settled row.

`apps/sts/tests/test_md_events.py` — `md.expiry` reaches
`on_expiry`.

`apps/sym/tests/test_plane.py` —
`test_client_get_finds_inactive_settled_instrument`.
