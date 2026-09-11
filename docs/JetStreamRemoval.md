# Taking JetStream out of the broker

The broker's store families — ledger, OMS, tape, logs, leases, counters —
sit on JetStream streams and KV buckets. Live fan-out and request-reply
already do not. This is the design for moving every store family off
JetStream so NATS is only the bus, and for what replaces each promise.

It is written before the change so the commits that follow can be read
against it. Nothing here is a code change; the first commit of the work
is this file. [`docs/Broker.md`](Broker.md) still describes what the
tree does today.

The destination is not "core NATS pretending to be a store". Core NATS
has no last-value, no compare-and-set, no key TTL, no history. Each
family moves to the place that already is its authority, or to a store
that is allowed to be regional. JetStream is deleted because those
homes exist, not because pub/sub grew the missing primitives.

## Destination

```
NATS core     fan-out, request-reply, session heartbeats,
              ledger / OMS / tape RPC
Regional Redis   tape only — MD writes, MD reads
Postgres      session rows, session_logs, everything it already holds
JetStream     gone
```

`-js` becomes optional and then absent. A NATS without JetStream
accepts the connection and answers everything a plane does.

`src` trees still must not import `nats`. They also still must not
import `redis`, except the MD tape module that *is* the regional disk
— the same exemption `packages/common/src/mftik/broker/` has today
for the transport. STS never sees a `REDIS_URL`.

## Invariants that replace CAS

JetStream KV exists because several processes of one plane serve one
subject, and a process cannot tell "a peer owns this" from "nobody
does" by looking at itself. That assumption is withdrawn.

**One instance name is one process.** `MFTIK_INSTANCE=td-jp-1` names
exactly one OS process. A second process with the same name — copied
compose, `--scale`, a rolling deploy that overlaps — is not a
supported topology. It is a misconfiguration. The broker will not
detect it. Two venue sessions on one key will both look healthy
under the heartbeat protocol, because each link is green.

**A credential routes to one TD instance.** `apis.instance_id` and
`Topics.td(instance)` already do this. A JP key does not land on a
TW TD.

**An STS session is pinned to one STS instance.** Rebuild already
skips a row whose `instance` is someone else
(`apps/sts/src/mftik_sts/session/manager.py`). An unpinned row is
rebuildable by whoever boots first; that is the last `claim_alive`
race. Pinning is required, not optional.

**Deploy does not overlap.** The old process is gone before the new
one serves the same instance name. Overlap is the same-name second
process.

These are deployment contracts, not broker primitives. The risk of
dropping KV is that violating them is silent. That is accepted.

## What each JetStream object becomes

Today's objects, from [`docs/Broker.md`](Broker.md) and
[`docs/BrokerProvisioning.md`](BrokerProvisioning.md):

| Today | Kind | Replacement |
|---|---|---|
| `{prefix}_ps` (`mft_ps`) | stream, `{prefix}.ps.>` | Deleted. Live `publish` / `subscribe` are already core NATS. The stream is a tail nobody in the business path reads. |
| `{prefix}_log` (`mft_log`) | stream, `{prefix}.log.>` | Deleted. `publish_log` becomes core `publish`. A late WebSocket reads `session_logs` (already filled by `log_persist`). |
| `{prefix}_tape_{feed}` | stream per recorded feed | Regional Redis next to the MD that pumps the feed. STS does not open Redis. |
| KV `state` | bucket | Deleted. TD memory is the book. STS reads it over `td.account.{api_id}`. |
| KV `lease` | bucket | Deleted. Session fencing is heartbeat + ack. Ownership is the instance contract. |
| KV `counter` | bucket | Deleted. The cid slot is allocated inside the one STS process that owns the session. |
| KV `tapecov` | bucket | Deleted. Coverage lives next to the prints, in the same regional Redis. |

Ephemeral pull consumers (`state_all`, tape tail, log buffer) leave
with the objects they read.

### Fan-out tail (`mft_ps`)

**Today.** `connect()` ensures a catch-all stream. `nc.publish` does
not wait for its ack. Live subscribers start at now.

**After.** No stream. Same live promise: a message published while
nobody is subscribed is gone.

**Contract.** Unchanged for callers of `publish` / `subscribe`.
`subscribe` on `log.` / `status.` no longer dual-listens on a log
stream; those topics are ordinary fan-out (see Logs).

### Logs (`mft_log`)

**Today.** `publish_log` is `js.publish` onto `{prefix}.log.>` with
`max_msgs_per_subject = 256`. `fetch_log_buffer` is a pull consumer
for a late `/ws/...` that wants the last hundred lines.

**After.** Planes `publish` the same topics (`log.{domain}.{id}`).
`run_log_persist` already subscribes and writes `session_logs`.
The API socket that opens after a deploy reads that table, then
attaches to the live subject. Alert workers still must not drain
the late buffer into Discord
([`docs/Alert.md`](Alert.md)).

**Contract.** Live subscribers still see lines as they are
published. A late subscriber is promised the persisted tail, not
an in-broker ring. A persist worker that is down loses the late
window; it does not lose the live one.

**Risk.** The hundred-line ring was in-process-to-NATS latency.
Postgres is a disk. That is the cost of not keeping a second copy
on JetStream. The persist worker is now on the path a freshly
opened UI socket cares about.

### Ledger and OMS (KV `state`)

TD's memory is already the authority. The venue can rebuild it on
recon. KV is a cache so STS can `state_get` without asking TD, and
so `reserve` can ack only after the cache has the pre-lock.

**After.** TD does not write `td.ledger.{api_id}` or `td.oms.{api_id}`.
`write_ledger` / `write_order` / `publish_oms` stop being store
writes. `td.balance.update` / `td.order.update` stay as core
fan-out: "it moved", carrying the row they already carry.

STS reads over request-reply on `Topics.td_account(api_id)` — the
subject that already exists for account reads that must not stall
`td.order`. Types already reserved:

- `td.ledger.view` → `LedgerView` (whole book, or one asset)
- `td.oms.view` → `OmsView` (live orders and positions)
- `td.oms.order` → one order by `client_order_id`

`StrategyLedger.view` / `available` / `free` / `prelock` and
`StrategyOms.view` / `order` become these RPCs. The
`StateProjection` pair STS starts per `api_id` goes. Attach still
seeds from `ReconDone` / `view_for_sts()`; that is the first
snapshot, not a second store.

**Contract.**

- The book a strategy reads is the book in the TD that holds the
  account. There is one writer. Two strategies on one `api_id`
  see one ledger because they ask one process.
- A sizing read after `reserve` sees the pre-lock without waiting
  for a KV ack. Submit no longer awaits `write_ledger`.
- `available` remains a floor. TD re-checks before the venue
  call. Two strategies can still race each other; they cannot
  race two caches.
- TD down is no-responders, at once. Sizing fails closed. There
  is no stale KV to trade against.
- `state_put_many` is not a snapshot — that sentence leaves with
  the family. One RPC is one picture.
- `oms.order` after submit is still a direct read of the authority
  (now TD memory, not `state_get`). A miss is still a money bug.

**Must not.**

- Do not serve these on `td.order.{api_id}`. A venue REST
  `EnsureLeverage` on `td.account` must not block a ledger view:
  the view is a memory read and has to stay one. Parallel
  handlers or a split subject; not a single `async for` behind
  a 5s REST call.
- Do not anycast. The subject is the account's owner, the same
  process that serves `td.order.{api_id}`.

**Risk.** Every `available()` / `oms.order` is a core RTT to TD
instead of a KV get. Decision-point frequency (chase, twap, oco,
cross_arb) can pay that. Polling `view()` as if it were a tick
cannot. A full `OmsView` is larger than a ledger; the hot path
is the single-order RPC. SYM's 1 MiB listing problem is not this
payload.

### Tape (per-feed streams + KV `tapecov`)

MD is the only process that sees every print. No venue here
serves a tape lookup — an unrecorded `aggtrade` is gone
(`apps/md/src/mftik_md/tape.py`). That is why tape is not the
same move as the ledger: TD can recon from the venue; MD cannot.

TW and JP each have a Redis that does not replicate to the
other. The MD that pumps a feed writes that region's Redis.
Coverage (`continuous_since_ms`, `recording`, `gaps`) is a key
next to the stream, not a leftover JetStream bucket.

STS does not know which region holds the tape and must not.
`md_feeds_of` flattens instance away because which MD serves a
feed is a deployment's business. `StrategyTape.read(ticker)`
stays "this feed". The STS session maps the feed to the MD
instance it already attached — the process that has been
recording — and `request`s that instance.

```
strategy   tape.read(ticker)
STS        feed → attached MD instance
           request  Topics.md(instance)   type=md.tape.tail
MD         XREVRANGE + coverage on local Redis
           reply    chunked prints + TapeSlice fields
```

**Contract.**

- Warm-up history is the history of the MD that holds the feed,
  surviving that MD's process restart as long as the region's
  Redis does.
- Same-region handover (`md-jp-1` → `md-jp-2`) appends to the
  same Redis key. No deque copy. Coverage still stamps a window
  where nobody was subscribed.
- Cross-region is two tapes. Asking the wrong MD is an empty
  slice (`recording=false`), which is a normal answer, not a
  fallback to the other region.
- Anycast `Topics.MD` and `md.fetch` are the wrong subjects.
  `md.fetch` is public venue data and unkeyed on purpose. Tape
  is the opposite.
- A list-shaped `md:` (any instance) must remember which
  instance won the attach. A second anycast is a coin flip
  against an empty Redis.
- `DEFAULT_LIMIT` (200k) does not fit one core NATS message
  (~40 MB vs 1 MiB). The RPC is cursor-chunked. The strategy
  still sees one `TapeSlice`.

**Must not.**

- STS must not take a `REDIS_URL`. A session in TW warming up
  on a JP feed would otherwise open the wrong disk, or need a
  region map the strategy layer is forbidden to have.
- Append still must not fail the live fan-out. A Redis blip
  loses records, not ticks.

**Risk.** Redis is a new dependency, scoped to MD, not a second
`BrokerTransport`. Losing the region's Redis loses that region's
warm-up; live ticks are unaffected. A tape_keeper is still what
keeps a feed pumping when no trading strategy is attached — it
does not become optional. [`docs/MdHandover.md`](MdHandover.md)
rested on "the tape is not lost on MD restart because JetStream
is on another host"; the same sentence is now true of regional
Redis, and false of a memory deque.

### Session fencing (not KV)

This is already core pub/sub: STS publishes `LeaseHeartbeat` on
`sts.td.{session}` / `sts.md.{session}`; TD and MD ack
(`LeaseAck`, `MdLeaseAck`) and echo the token.

**Today.** TD/MD run a sibling `_watch_timeout` every 0.5s
against wall-clock grace (MD 3s, TD 5s). STS counts MD ack
silence the same way, and only for MD — a quiet TD does not
stop the strategy. The same STS loop also `mark_alive`s a KV
key.

**After.** Misses, not a watchdog task.

- STS publishes a heartbeat. The peer acks. Three consecutive
  heartbeats without an ack from an instance that has acked at
  least once → that peer is down. Stop trading. MD already
  has this idea; TD gets the same rule.
- TD/MD: two heartbeat intervals without a lease → the STS
  session is gone. Detach. The receive loop counts; there is
  no sibling poller.

**Contract.**

- Arm on the first ack. A session heartbeats before the peer
  has attached; counting from start kills every deploy.
- Per instance, not per session. One of two MDs going quiet
  stops the session. The other staying green is not a live
  session.
- One missed ack is a dropped core message. Three is the fuse.
  The ack still echoes the token it is answering.
- "Renew" means the link is still valid. It is not
  `lease_hold`.

**Risk.** Core NATS is at-most-once. A burst of loss looks like
a dead peer. Three at 1 Hz is ~3s, in the same band as today's
MD grace. Tightening to one miss will false-trigger.

### Ownership, rebuild, orphans (KV `lease` and `counter`)

The heartbeat answers "is this already-attached link still
up?". It cannot answer "may I be the first to build this
account / rebuild this row?", because at that moment there is
no link to count misses on.

Under the instance contracts above, those questions go away:

| Today's key | Who asked | After |
|---|---|---|
| `td:owner:{api_id}` | TD `claim_owner` before `create()` | The attach subject is one instance, and that instance is one process. `self._accounts` is the only map. |
| `{plane}:alive:{session}` | `mark_alive` / `claim_alive` / `is_alive` | Rebuild is pinned. Orphan is "this row names me and I do not have it locally". |
| `backfill:lock:{api_id}` | advisory, already on `td.backfill.{instance}` | The instance subject is enough. Two walks on one key were a rate-limit courtesy. |
| `cid:slot` (`counter_next`) | one allocation per STS session | The one STS process that accepted the session assigns the slot. |

**Contract.** A second process of `td-jp-1` is forbidden, not
refused. `claim_owner`'s comment — owner must be a process id
because two processes can share an instance name — describes
the topology this document outlaws. `liveness.py` as a SET NX
policy layer is deleted with the bucket.

Orphan reaping does not subscribe to someone else's heartbeat.
The reaper is the pinned instance coming back, looking at its
own memory against its own rows.

**Risk.** This is the load-bearing trade. A same-name second
process will dual-open an account and both heartbeats will
succeed. There is no remaining lock. Operators who `--scale`
a plane, or overlap a restart, recreate the bug
[`docs/Instances.md`](Instances.md) §7 described, with nothing
in process to name the holder.

Unpinned STS rows are the other hole: two differently named
STS instances will both rebuild. Pin every live session, or
keep a race. The design chooses pin.

## What leaves `Broker`

These methods are the store families. After the moves they
have no transport behind them and come off the façade:

`state_*`, `state_watch`, `state_projection`,
`lease_*`, `counter_next`,
`tape_append`, `tape_tail`, `tape_trim_before`,
`tape_mark_recording`, `tape_mark_stopped`, `tape_coverage`,
`fetch_log_buffer`.

`publish_log` may stay as a name for "publish onto a log
topic" or collapse to `publish`. Continuity arithmetic
(`encode_tape_gaps`, `TapeSlice`) moves to the MD tape module
and `StrategyTape`, which already own the meaning.

`LeasedSessionLink` stays, with the miss counter in the
heartbeat / receive loops instead of `_watch_timeout`.

What a plane may say, once the store families are gone, is
fan-out, request-reply, and a fenced session link. That is
core NATS. `BrokerTransport` shrinks to those three. A second
transport is not justified; the tape Redis is not one.

## Properties that go away

Callers no longer need to know:

- NATS per-message TTL is whole seconds with a one-second
  floor (ADR-43). Nothing uses `Nats-TTL`.
- A multi-field KV write is not a snapshot.
- KV key character set, `:` → `.`.
- Stream reshape / `allow_msg_ttl` cannot be disabled
  ([`docs/BrokerProvisioning.md`](BrokerProvisioning.md)).
- Server floor 2.11 and client floor 2.14 for TTL and
  `opt_start_time`. Core NATS does not need them.
- `NATS_KV_REPLICAS` / `NATS_KV_PLACEMENT_CLUSTER`. There is
  no KV to pin. Cross-region cost on the hot path was the
  ledger put; that put is gone.

New properties they do need:

- Ledger / OMS / tape reads fail immediately when the owning
  plane is down.
- Tape RPC is chunked. A single reply is not the slice.
- One instance name, one process. The broker will not say
  otherwise.

## Risks, collected

1. **Same-name second process.** Silent dual venue session.
   Heartbeats stay green. No KV will fire.
2. **Overlapped restart of one instance name.** Same as (1).
   Stop then start.
3. **Unpinned STS session.** Two STS instances rebuild one
   row. Pin is mandatory.
4. **Tape asked of the wrong MD.** Empty warm-up that looks
   like "never recorded". Session-level routing, not anycast.
5. **Regional Redis loss.** Warm-up for that region is gone.
   Ticks are not. Restore Redis; do not read the other
   region's keys.
6. **`td.account` serialised behind leverage REST.** Sizing
   waits on venue. Views must be non-blocking memory reads.
7. **1 MiB tape reply.** Must chunk or the warm-up raises.
8. **Three-miss false trigger.** Packet loss, not a dead
   peer. Do not count from zero before the first ack.
9. **Late UI log depends on persist.** A down `log_persist`
   makes a fresh socket start empty. Live tail still works.
10. **`src` grows a Redis import in MD.** The tree-walk
    guard must exempt that module the way it exempts
    `mftik.broker`, and nowhere else. A Redis client in STS
    or `packages/common/src/mftik/strategy` is a regression.

## What this is not

Not a second `BrokerTransport`. Redis is MD's tape disk.
Bringing the Redis transport back — lease, state, log, a CI
matrix — is the thing [`docs/RedisRemoval.md`](RedisRemoval.md)
removed.

Not a per-region NATS. One broker, one `NATS_URL`. Regions
split only the tape disk.

Not MD-memory deques. A process restart would drop two hours
of prints the venue cannot rebuild. Regional Redis is the
durability; RPC is only the read path.

Not "pubsub is CAS". The heartbeat protocol replaces the
session watchdog. The instance contracts replace
`lease_take`. Mixing those sentences is how dual-open comes
back.

## Order

1. This document, and the contract pointers in the docs
   listed below.
2. Session link: miss counts, drop `_watch_timeout`, STS
   applies the same rule to TD acks as to MD.
3. Ledger and OMS RPC on `td.account`; delete KV writes and
   `StateProjection`.
4. MD Redis tape + per-instance RPC; delete tape streams and
   `tapecov`. `StrategyTape` asks the session, not
   `broker.tape_*`.
5. Late WS from `session_logs`; `publish_log` → `publish`;
   delete the log stream and `fetch_log_buffer`.
6. Delete `liveness.py` SET NX, `lease_*`, `counter_next`,
   the `state` / `lease` / `counter` buckets.
7. Delete `mft_ps`. Stop ensuring streams in `connect()`.
   Drop `-js` / `-sd` from compose and the node template
   once nothing creates a stream.

Two and three can overlap. Four needs the instance-remembered
attach before anycast tape reads can be refused. Six depends
on two (so `mark_alive` is no longer in the heartbeat loop)
and on the pin invariant being true in the database, not
only in prose.

## Docs this updates

| Doc | What changes |
|---|---|
| [`Broker.md`](Broker.md) | Points here. Today's JetStream table stays as a description of the tree until the code moves. Deployment `-js` is marked as current, not destination. |
| [`BrokerPatterns.md`](BrokerPatterns.md) | A/B/F/G named as families this document retires. |
| [`BrokerProvisioning.md`](BrokerProvisioning.md) | Stream/bucket migration is moot once those objects are gone. Overlap deploys are forbidden for a different reason. |
| [`MdHandover.md`](MdHandover.md) | Tape durability is regional Redis. Same-name competing consumers are not a given. |
| [`Instances.md`](Instances.md) | One process per instance name is an invariant. Tape is not in the broker. `claim_*` is not the long-term guard. |
| [`RedisRemoval.md`](RedisRemoval.md) | Redis may return only as MD's tape disk. |
| [`Alert.md`](Alert.md) | Late replay is `session_logs`, not `fetch_log_buffer`. |
| [`README.md`](../README.md) | Tape / lease sentences match the destination. |
