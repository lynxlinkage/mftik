# What the system actually asks a broker for

Before redefining the broker's interface around NATS, this is the enumeration
of what the interface is currently asked to do — counted from call sites rather
than recalled from the design.

The conclusion is narrower than expected, and in one place the count is not the
interesting part.

Of ten patterns, eight are already NATS' own primitives, or close enough that a
rewrite would move them sideways. Two are worth the work, for opposite reasons:
one is still shaped like a Redis hash and sits on every strategy's read path;
the other is native at the transport and has no shared abstraction at all, so
two domains wrote the same one twice.

## How this was counted

Two interfaces, and telling them apart is what makes the rest of this document
short:

- **`BrokerTransport`** (`transport/base.py`, 32 abstract methods) — one
  implementation and one caller, `client.py`. Since #84 removes the second
  implementation, this is an internal seam rather than an interface. Changing
  it is invisible to every domain.
- **`Broker`** (`client.py`) — what `sts`, `md`, `td`, `sym`, `api` and the
  strategy runtime see.

`test_broker_is_the_only_transport.py` enforces the separation: nothing under
an `src` tree may import `redis` or `nats`, touch `.js`/`.nc`, or read
`.key_prefix`. So the surface below is the whole surface.

Counting broker-qualified calls across `apps/*/src` and `packages/*/src`:

| | |
|---|---|
| Files touching the broker at all | 37 — common 12, api 9, td 6, md 5, sts 3, sym 1, paper 1 |
| Files touching `state_*` / `lease_*` / `counter_*` / `tape_*` | **7** |

The seven: `apps/md/tape.py`, `apps/sts/session/manager.py`,
`apps/td/backfill/executor.py`, `apps/td/session/session.py`,
`packages/common/liveness.py`, `packages/common/strategy/tape.py`,
`packages/common/strategy/ledger.py`.

## The ten patterns

| # | Pattern | Broker surface | Where NATS stands |
|---|---|---|---|
| 1 | Fan-out | `publish` / `subscribe` | Native. Core pub/sub over a stream with per-subject bounds. |
| 2 | Fan-out with a replay tail | `publish_log` / `fetch_log_buffer` | Native shape, one avoidable round trip — see below. |
| 3 | Request/response | `request` / `probe` | Native. Core request-reply, and no-responders answers a request to nobody at once instead of at the timeout. |
| 4 | Queued work, competing consumers | `post` / `serve` | Native. A work-queue stream, plus a core queue group. |
| 5 | Durable append log per feed | `tape_append` / `tape_tail` / `tape_trim_before` | Native. One stream per feed, so "newest N" is sequence arithmetic. |
| 6 | Bidirectional stream, fenced | hand-rolled from `subscribe` + `publish` | Native transport, **no shared abstraction**. Built twice. See below. |
| 7 | Liveness | `heartbeat_loop` | Built on 1 and 8. Nothing of its own. |
| 8 | Lease / distributed lock | `lease_*` | KV with per-message TTL and compare-and-set on revision. Already better than the Redis original, which documented losing a race this one wins. |
| 9 | Monotonic counter | `counter_next` | KV compare-and-set loop. Not a Redis artifact — a server-side counter exists in NATS 2.12 and this node's floor is 2.11. |
| 10 | **Shared mutable state** | `state_*` | **The one that is still Redis-shaped.** |

Patterns 1 and 3 and 4 are the traffic — roughly half of all broker calls in
the tree are `publish`, `subscribe`, `request`, `probe`, `post` and `serve`.
There is nothing in them to make native. They already are.

### 6 is the pattern the broker named and then under-specified

The session fencing lease between STS and both MD and TD *is* a bidirectional
stream, and it is as load-bearing as anything here: it is what decides that a
strategy session has gone and its feeds and its account link should be torn
down.

It does not go through `broker.bistream()`. Both domains build it by hand, and
they build the same thing:

| | MD (`apps/md/session/manager.py:625`) | TD (`apps/td/session/manager.py:784`) |
|---|---|---|
| inbound | `subscribe(Topics.sts_md_session(sid))` | `subscribe(Topics.sts_td_session(sid))` |
| outbound | `publish(Topics.md_session(sid))` | `publish(Topics.td_session(api_id, sid))` |
| fencing token | `link.last_token = hb.token`, echoed in `MdLeaseAck` | the same, in `TD_LEASE_ACK` |
| grace | `_watch_timeout` at 0.5s against `LEASE_GRACE_S` (5.0) | the same |
| expiry action | `detach(reason="lease_expired")` | `detach(reason="lease_expired")` |

That is exactly the shape `bistream_pair` offers — a named pair of up and down
topics, one side subscribing and the other publishing — written twice by hand,
with a `StsLink` dataclass, a watchdog and a token each time.

So the reading is not "delete the unused API". It is that **the broker
identified this pattern and then offered too little of it to be worth using.**
`BidirectionalStream` carries envelopes in two directions and stops there. What
the two call sites needed on top of it, and therefore wrote themselves, is:

- a **fencing token** echoed back on the ack, so a stale writer is detectable;
- a **liveness grace**, separate from the transport's own connection state,
  because a peer that stopped heartbeating has gone even though the subject is
  still there;
- an **expiry action**, since noticing is not the point — detaching is.

That is a leased session link, not a byte pipe. A native redesign should either
build that primitive once — pattern 8 (`lease_*`) already has the token and TTL
half of it, on a KV key rather than over a link — or delete `bistream` and say
in the interface that this pattern belongs to the domains. What it should not
do is carry `BidirectionalStream` across unchanged: the two call sites that
needed this pattern both looked at it and wrote their own instead.

The duplication is worth pricing on its own: `StsLink`, `_lease_loop`,
`_watch_timeout` and `LEASE_GRACE_S` exist twice, in two apps, and a fix to the
fencing logic has to be made in both.

### 2 costs one round trip more than it needs to

`publish_log` publishes and then purges the subject to `keep=maxlen`, because
the fan-out stream's `max_msgs_per_subject` is `FANOUT_MAX_MSGS_PER_SUBJECT`
(256) while a log ring is `log_buffer_maxlen` (100). Two bounds, one stream, so
the smaller one is enforced by hand on every line.

Giving logs their own stream with `max_msgs_per_subject = log_buffer_maxlen`
lets the server hold the ring and the purge goes. In practice there is one
value: `client.py:489` passes `config.log_buffer_maxlen` unless a caller
overrides, and none does.

## Pattern 10, which is the actual subject

`state_*` is a Redis hash with the serial numbers filed off. One KV key per
field, `state_all` reassembling a hash, `state_replace` emulating a `MULTI`.
Everything that made it awkward follows from that choice: no cross-key
transaction so `state_put_many` is not a snapshot; a lock in-process to restore
"last issued wins"; a scan to find out which fields to drop.

**What it costs today.** `state_all` is five round trips and a JetStream
consumer created and destroyed: `bucket.status()`, `stream_info` for the
subject counts, `pull_subscribe`, `fetch`, `delete_consumer`. In a clustered
NATS, creating a consumer is a raft operation.

And it is on the read path of every strategy:

- `strategy/ledger.py:93` — `LedgerView.view()` calls `state_all` per access.
  A strategy asking for a balance pays the whole thing.
- `strategy/oms.py:87` — `OmsView.view()`, the same, for the order book.

**What the write side looks like.** `Session.publish_oms`
(`apps/td/session/session.py:508`) is named "publish" and publishes nothing: it
is a `state_replace`. TD writes the book; STS re-reads it when it next wants to
know. Write-then-poll, because a Redis hash has no other mode.

**What native looks like.** NATS KV has `watch`, `watchall` and `history` —
verified present in the pinned nats-py. A state model designed for it is push,
not pull: TD writes, and STS is told. The strategy's `view()` reads a locally
maintained projection instead of paying five round trips to rebuild a hash the
writer already had in memory.

That is not a tidier spelling of the same thing. It is a different cost
structure for the domain that reads most, and it is unreachable from a hash.

Three questions the design has to answer, none of which this document settles:

- **Ordering and gaps.** A watch that misses an update because a client
  reconnected has to resynchronise. KV history gives a revision per key; what
  the projection does with a gap is the real design.
- **The unit.** Key-per-field is what makes a single-field write cheap and a
  multi-field write non-atomic. A key per *name* holding the whole record
  inverts both. With a watch feeding a projection, the read cost that motivated
  key-per-field partly goes away, so this is worth re-deciding rather than
  inheriting.
- **Who may write.** `_state_lock`'s docstring asserts a state name has exactly
  one writer, the session that owns that account. If that holds, the transport
  can cache the field set it last wrote and `state_replace` stops reading
  before it writes. If it does not hold, the lock is already insufficient and
  that is a bug independent of any redesign.

## What a redesign does not touch

Most of the thirty-seven files use only patterns 1–5 and 7. Those are already
NATS primitives, and neither redesign reaches them.

The integration surface for **pattern 10** is the seven files listed above, and
the two strategy-facing views inside them — `strategy/ledger.py` and
`strategy/oms.py` — are where the behaviour actually changes.

The integration surface for **pattern 6** is two files, both called
`session/manager.py`, one in `apps/md` and one in `apps/td`. It is small, but
unlike pattern 10 it is a change to how a session is torn down, which is the
path a bad change breaks quietly: a fencing bug does not fail a request, it
lets two writers believe they own the same feed.

This is the part worth stating plainly, because "redefine the interface and
integrate it into sts/md/td/sym/api" sounds like thirty-seven files and is
nine.

## Sequencing, and why not now

The expensive half of this work is already done. #77 and #80 pushed the store's
vocabulary out of the domains and put a test in front of it. What is left is
not a big-bang; the seam has been bought and paid for.

What is missing is the thing that should shape the design: **no part of this
system has run in production.** Four defects surfaced in review on 2026-09-08
(#82, #83), every one of them invisible until a node had been up a while. An
interface designed today is designed from round-trip counts — including every
number in this document. An interface designed after two weeks of real traffic
is designed from where the time actually goes.

Deferring costs almost nothing here, which is unusual enough to be worth using:
`test_broker_is_the_only_transport.py` keeps the seam from rotting while it
waits.

So:

1. **#84** — remove the Redis transport. Frees `base.py` from lowest-common-
   denominator semantics; prerequisite either way.
2. **Round-trip work behind `client.py`** — cache the bucket status that is
   re-fetched per call, keep the last-written field set so `state_replace`
   stops reading first, move tape coverage off its TTL renewal, give logs their
   own stream. No domain file changes.
3. **Production, and two weeks of it.**
4. **Then** redesign pattern 10 against measurements, with those seven files as
   the integration surface.

Step 4 is what produces the clean version. Doing it before step 3 produces a
clean version of a guess.

**Pattern 6 does not have to wait for step 3**, and that is the one place this
sequencing bends. It needs no measurement — the argument for it is that the
same fencing logic exists twice and a fix has to be made in both — and the
duplication is a live correctness risk rather than a cost. It is also the one
piece that production would make *harder* to change rather than easier, since
teardown is what a deploy exercises. If anything here should happen before the
first deploy, it is this and not pattern 10.
