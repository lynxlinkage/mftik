# MD handover — replacing a market-data process without a hole in the tape

A deploy restarts MD. While it is down nothing is subscribed to the venue, so
nothing is recorded, and the prints from that window do not exist anywhere:
`mftik_md.fetch.readers` serves klines, book and quote, and no venue here
serves a tape lookup. An `aggtrade` not recorded is gone.

This describes how a new MD takes a feed over from the old one while both are
running, so there is no window. It is the second half of a problem whose first
half is already done — see **What the continuity fix already bought**, which is
also why this is an optimisation rather than an emergency.

Nothing here is built yet.

## What is already true

Facts this design rests on, all of them checkable in the tree today.

**The tape is not lost on restart.** `TapeRecorder` writes Redis streams, and
Redis is long-lived substrate on another host — the production compose file does
not manage it. A restart of the MD container leaves the stream intact.

**The fan-out is addressed per session.** `Dispatcher.publish` sends each update
to `Topics.md_session(session_id)` for every subscribed link, and only then calls
`recorder.append`. Both writes a handover must fence are in that one method,
which is a convenience: they can be gated together.

**A feed exists because somebody holds it.** `_subscribe_feed` calls
`ensure_feed` on the first subscriber and `_stop_feed_if_unused` tears it down at
refcount zero. Feed lifetime is derived entirely from attached STS links. A
process with no links pumps nothing — which is the single biggest obstacle here,
because a green MD has no links by definition.

**MD instances are already competing consumers.** `run_rpc` serves
`Topics.MD`, one shared subject, and `liveness.py` says so outright: several
processes of a plane serve the same subject, which is why per-`(plane, session)`
alive keys exist to tell "a peer owns this" from "nobody does". So a second MD
coming up is not a new idea — but today it would immediately start stealing
attach requests, which a warming-up instance must not do.

**`reap_orphans` is already safe against a peer.** It reads the alive key rather
than assuming an unowned row is dead. A green MD booting will not reap blue's
sessions. This one needs no work; it is listed because it looks like it should.

**Fencing exists, on a different axis.** `LeaseHeartbeat`, `LeaseAck` and
`MdLeaseAck` fence *a session's* ownership: the lease is held by STS, keyed by
`session_id`, and MD is the resource that ACKs it. `Topics.td_backfill` puts it
plainly — only the process holding the lease may place an order.

That axis answers "which STS session owns this attach". It cannot answer "which
MD process may write this feed", because both MDs would be serving the same
session. The handover needs a second, orthogonal lease.

| | Existing lease | Feed lease (new) |
|---|---|---|
| Keyed by | `session_id` | feed (`topic.UniversalTicker`) |
| Held by | STS | an MD instance |
| Fences | orders, attach ownership | tape append + fan-out |
| Renewed by | `LeaseHeartbeat` on `sts.md.*` | MD instance heartbeat |

## What the continuity fix already bought

`tape_mark_recording` used to reset `continuous_since_ms` on every start and
wipe `stopped_ms`. A four-second deploy therefore discarded two intact hours of
tape, because the reader drops everything before the mark.

It now distinguishes two cases. A recorder that ran its shutdown left a
`stopped_ms`; with the new start stamp that makes the interruption a *measured*
fact, so continuity is kept and the hole is appended to the `gaps` coverage
field. A recorder that vanished left nothing, the hole is unmeasurable, and
continuity restarts as before. `StrategyTape.read` takes `max_gap_ms`
(30s default) and reports `TapeSlice.gaps` either way.

Two consequences for this document:

1. The remaining cost of a deploy is a real but small hole — seconds — that
   every reader is now told about. That is worth closing, not urgent.
2. **A failed handover has somewhere safe to land.** If green dies mid-cutover
   and blue re-takes the feed, blue stamps `stopped`/`started` and the result is
   a measured gap. Without the fix, every abort would have cost the full
   warm-up window, which would make the handover a mechanism you could not
   safely test in production.

## Invariants

The design is only interesting insofar as it holds these. Each is meant to be a
test.

- **I1** At most one instance appends to a given feed's tape at any time.
- **I2** At most one instance fans a given feed out to a given session stream.
- **I3** No duplicate `trade_id` in a feed's tape across a handover seam.
- **I4** No unrecorded interval across a *successful* handover: green's buffer
  must begin strictly before blue's last write.
- **I5** A handover that cannot establish I4 aborts, leaving blue in charge.
- **I6** A handover that fails after cutover degrades to a *measured* gap, never
  an unmeasured one.

I2 is the strictest of these and the one to design against. A duplicated tape
record is history that is wrong; a duplicated fan-out message is a live print
delivered twice to a strategy that is placing orders on it.

## The shape: cooperative handover, fencing as backstop

The instinct is to make this a fencing race — green takes a higher token, blue's
writes start getting rejected. That is the right primitive for a *failover*,
where the old owner may be dead, hung, or lying. It is the wrong primitive for a
*deploy*, where blue is healthy, cooperating, and about to be asked to exit.

Fencing alone also cannot deliver I2 cheaply. To reject a stale write you need
the resource to check the token on every write, and the write here is every
print on every feed. A Redis round trip per print is not affordable, and an
in-process token check is only as fresh as the last renewal — which leaves
exactly the overlap window I2 forbids.

So the primary mechanism is a **handshake**, and the token is the backstop:

- Blue is asked to yield a feed and confirms it has stopped, naming the last
  record it wrote. There is no window because there is no race — blue stops
  before green starts, and says so.
- The token exists for the case where blue does not answer. Then the handover
  **aborts**: green shuts down, blue keeps the feed. Forcing a takeover from an
  unresponsive peer is a failover, and this is not one.

Stated as a rule: *a deploy handover is cooperative; when cooperation fails the
answer is abort, not force.*

## Protocol

```
green boots with role=standby
   ├─ does NOT serve Topics.MD                    (blue keeps answering attach)
   ├─ does NOT run reap_loop                      (nothing to reap; blue is up)
   └─ does NOT append or fan out                  (I1, I2)

green → blue   HandoverOffer{instance_id}
blue  → green  HandoverFeeds{feeds[], sessions[]} (blue's refcounted feed set)

green opens each feed on the venue, pinned rather than refcounted,
      and buffers prints in memory. Still writing nothing.

per feed, once green's earliest buffered print is older than blue's
last write (I4 — otherwise this feed is not ready and may not cut over):

   green → blue   YieldFeed{feed, token}
   blue           stops fan-out and tape append for this feed
   blue  → green  FeedYielded{feed, last_stream_id, last_trade_id, at_ms}
   green          drops buffered prints <= last_trade_id            (I3)
                  appends the remainder, then continues live        (I4)

once every feed is yielded:
   green subscribes sts.md.{session_id} for each session, builds its links
   green begins ACKing on md.{session_id}; blue stops
   green starts serving Topics.MD; blue stops serving
   green releases the feed pins — refcount takes over from here
   blue exits
```

The cutover is **per feed**, not per process. A feed that cannot satisfy I4 —
because green could not open it, or the venue was slow — does not block the
others, and does not get taken. It stays with blue, and blue's exit is what
finally interrupts it, with a measured gap (I6).

## The hard parts

### 1. Green has no subscribers, so it pumps nothing

This is the one most likely to be underestimated. Feed existence is derived from
refcount (`_subscribe_feed` → first subscriber → `ensure_feed`). A green MD with
no attached links has refcount zero everywhere and opens nothing, so there is
nothing to hand over.

The fix is a second reason a feed may be open — a handover pin — which
`_stop_feed_if_unused` must respect alongside refcount, and which is released
once the links have migrated. Two lifetime sources instead of one; the current
invariant "refcount zero means stop" becomes "refcount zero and unpinned".

**Where the feed list comes from.** `HandoverFeeds` from blue is the
authoritative answer, and blue is by assumption reachable. The obvious fallback
is not available: `persist_live` records `venues=venues`, not feeds, so the
`sessions` table cannot reconstruct which feeds were open. Either the handover
simply requires blue to answer — acceptable, since it aborts safely if not — or
`persist_live` starts recording the feed list. **Open question**, and the
cheaper answer is probably the first.

### 2. Green must not serve RPC while warming up

`broker.serve(Topics.MD, ...)` is a shared subject with competing consumers, so
a green MD that starts `run_rpc` immediately begins answering `attach` for
sessions whose feeds it does not have. `role=standby` must gate `run_rpc`
entirely, and the transition to serving is part of the cutover, not of boot.

The same applies to `reap_loop`. It is safe (it checks alive keys) but pointless
before cutover, and cheaper to leave off.

### 3. Fan-out and tape must flip together, per feed

Both live in `Dispatcher.publish`. Gating them on one per-feed check keeps them
atomic with respect to each other, which is what I1 and I2 need. The check must
be in-process — this is the hot path — with the lease as the authority behind
it. Fan-out is the strict one: a double-delivered print reaches a strategy that
trades.

### 4. Session migration

`StsLink` lives in blue's memory and cannot be serialised across. But it does
not have to be: STS heartbeats on `sts.md.{session_id}`, so green can subscribe
to the same topic and build an equivalent link by listening, without STS doing
anything or knowing a swap happened. The token in `LeaseHeartbeat` is STS's own
and is unaffected by which MD is serving it.

Double-ACK during the overlap is harmless — the ACK is a liveness signal, not
data — as is both instances renewing the same `mark_alive` key.

**What must be verified before building this**: STS's own grace period for a
missing `MdLeaseAck`. If green's first ACK arrives later than STS tolerates, the
session tears down and the whole exercise is self-defeating. The overlap should
be arranged so ACKs are continuous — green starts ACKing before blue stops —
but the tolerance sets the budget for the entire cutover and should be measured,
not assumed.

### 5. Abort and failure

| Failure | Result |
|---|---|
| Green cannot open a feed | That feed is not offered for cutover; blue keeps it |
| Green's buffer starts too late (I4 fails) | That feed is not cut over |
| Blue does not answer `YieldFeed` | Abort: green exits, blue keeps everything |
| Green dies before any cutover | Nothing happened; blue never lost a feed |
| Green dies after cutting over some feeds | Blue re-takes them; `stopped`/`started` stamps make it a **measured gap** (I6) |
| Blue dies mid-handover | Unmeasured gap — the pre-existing behaviour, no worse |

The second-to-last row is the one the continuity fix makes survivable, and it is
why that work came first.

## What this does not solve

- **Non-graceful death.** OOM and SIGKILL do not run a handover, do not stamp
  `stopped_ms`, and still produce an unmeasured gap. This closes the planned
  case only.
- **Venue-side gaps.** A disconnect from the exchange is not something a second
  MD process fixes.
- **STS and TD.** Neither can be blue/greened this way, and this design must not
  be read as a precedent for them: a strategy session holds positions and places
  orders, so two copies is two copies deciding to trade. Their restart cost is
  the warm-up, which is what the tape and the continuity fix address instead.
- **Frontend, API.** Stateless enough that an ordinary rolling restart covers
  them; not in scope.

## Suggested staging

Each stage is useful alone and leaves the tree in a shippable state.

1. **Standby role.** `role=standby` gates `run_rpc` and `reap_loop`. A second MD
   can be started against production and does nothing. Verifiable on its own.
2. **Feed pinning.** A pinned feed opens and pumps without a subscriber, and
   `_stop_feed_if_unused` respects the pin. Green can be told to open a feed and
   observed receiving prints, still writing nothing.
3. **The feed lease and the write gate.** `Dispatcher.publish` consults a
   per-feed token. With one MD this changes nothing observable — which is the
   point, and it is where I1/I2 get their tests.
4. **The yield handshake.** `YieldFeed` / `FeedYielded`, dedup on `trade_id`,
   buffer flush. Testable with two MDs against fakeredis and a paper venue.
5. **Session migration and RPC cutover.** The last step, and the one gated on
   measuring STS's ACK tolerance first.

## Open questions

1. Does `persist_live` need to record feeds, or is "blue must answer" enough?
   (See *The hard parts, 1*.)
2. What is STS's actual grace for a missing `MdLeaseAck`, and does it bound the
   cutover comfortably? Measure before stage 5.
3. Is the pinned-feed buffer bounded by time or by record count? The tape uses
   both (`DEFAULT_RETENTION_S`, `DEFAULT_MAXLEN`) for good reasons that apply
   here too.
4. Should a handover be startable from the UI, or only from the host-side deploy
   path? The button is not the hard part and should not drive the design.
