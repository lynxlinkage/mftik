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
                 (AOF + volume; see Tape)
Postgres      session rows, session_logs
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
supported topology.

The broker will not lock it out with KV. It *will* refuse to start
if someone is already serving that instance name: on boot, each
plane `probe`s its own subject (`Topics.td(instance)`,
`Topics.md(instance)`, `Topics.sts(instance)`). `probe` is already
"is anyone there?" (`client.py`). A responder means this process
exits and names who answered. That catches `--scale` and an
overlapped restart. Two processes that pass `probe` in the same
window can still both start; that remaining race is accepted, and
both heartbeats will look green.

**A credential routes to one TD instance.** `apis.instance_id` and
`Topics.td(instance)` already do this. A JP key does not land on a
TW TD.

**An STS session has one STS instance, and it is derived, not
chosen by a race.** Rebuild already skips a row whose `instance` is
someone else. Today `persist_live` writes `instance=request.instance`
(`manager.py`), so an unpinned deploy stores `null`, and a null row
is rebuilt by whichever STS wins `claim_alive` — a different one on
the next restart. Once `claim_alive` is gone, two STS would both
take it.

The answer is not to stamp whichever STS happened to accept the
create. A credential is bound to one TD (`apis.instance_id`, NOT
NULL), and a TD instance has a `region`. That is enough to derive
the STS:

```
sts_sessions.td (api_ids) → apis.instance_id → instances.region
                          → the enabled STS instance in that region
```

- `instance` on the row keeps meaning what the deploy asked for
  (`Instances.md`). Null keeps meaning "derive".
- Rebuild: a null row is rebuilt by the STS the derivation names,
  and by nobody else. No `claim_alive`, no backfill, no migration
  that guesses — the scan computes placement from the row's
  credentials each time, and gets the same answer each time.
- Create: an unnamed deploy is sent by the API to the derived
  instance's subject. The `Topics.STS` pool subject is not needed
  for creates once every create has a name.
- Derivation must be unique or it refuses. Two conditions:
  every `api_id` on the row resolves to the same region, and that
  region has exactly one enabled STS. A cross-region session
  (a TW key and a JP key), a region with two STS, or a session
  with no TD at all, has no derived answer: the deploy must name
  an instance, and an existing null row of that shape stays
  `INTERRUPTED` on the Attention list until a person names one.
  It is not rebuilt by a coin flip.

This makes `instances.region` load-bearing for STS placement.
[`Instances.md`](Instances.md) declared it a label nothing routes
on; that sentence changes. It stays free text, but editing a TD's
region now moves where that TD's null sessions come back.

**Deploy does not overlap.** The old process is gone before the
new one serves the same instance name. Overlap is what `probe`
is there to refuse.

## What each JetStream object becomes

Today's objects, from [`docs/Broker.md`](Broker.md) and
[`docs/BrokerProvisioning.md`](BrokerProvisioning.md):

| Today | Kind | Replacement |
|---|---|---|
| `{prefix}_ps` (`mft_ps`) | stream, `{prefix}.ps.>` | Deleted. Live `publish` / `subscribe` are already core NATS. The stream is a tail nobody in the business path reads. |
| `{prefix}_log` (`mft_log`) | stream, `{prefix}.log.>` | Deleted. `log.{domain}.{id}` late-replays from `session_logs`. `status.sts` late-replays from the session list (REST), not that table. |
| `{prefix}_tape_{feed}` | stream per recorded feed | Regional Redis next to the MD that pumps the feed. STS does not open Redis. |
| KV `state` | bucket | Deleted. TD memory is the book. STS reads it over `td.account.{api_id}`. |
| KV `lease` | bucket | Deleted. Session fencing is heartbeat + ack. Ownership is the instance contract plus boot `probe`. |
| KV `counter` | bucket | Deleted. `cid_slot` is 16 bits of the session id, derived where it is needed. |
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
`max_msgs_per_subject = 256`. Two caller families share that
stream:

- `log.{sts|td|md}.{id}` — session lines. `log_persist` matches
  `log.*.*` and writes `session_logs`. `/ws/sts/...` /
  `/ws/td/...` / `/ws/md/...` replay via `fetch_log_buffer`.
- `status.sts` — the session board. STS
  `publish_log(Topics.status_sts(), maxlen=200)` after every DB
  write (`manager.py`). `/ws/status/sts` replays the last 200
  with `fetch_log_buffer` (`ws.py`). `log_persist` does **not**
  store this topic.

**After.**

- Session logs: planes `publish` `log.{domain}.{id}`. A late
  `/ws/{domain}/{id}` reads `session_logs`, then the live
  subject. Alert workers still must not drain that tail into
  Discord ([`docs/Alert.md`](Alert.md)).
- Status: live `publish` on `status.sts` is enough for an open
  socket. A late `/ws/status/sts` reads the session list over
  REST, then attaches to the live subject. The row is always
  written before the publish (`manager.py`); that comment is
  the recovery path. Deleting the log stream without this
  makes a socket that opens after a fail miss the session
  until the next full page load.

**Contract.** Live subscribers still see lines as they are
published. A late *session-log* subscriber is promised the
persisted tail. A late *status* subscriber is promised the
current rows, not a ring of status envelopes. A persist
worker that is down loses the session-log late window; it
does not lose status (REST) or the live tail.

**Risk.** The hundred-line session-log ring was
in-process-to-NATS latency. Postgres is a disk. Status must
not be folded into `session_logs` by accident — the persist
pattern would have to change, and the board already has a
better source.

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

**Attach result must name the instance.** `MdAttachResult`
(`messages.py`) has `session_id`, `subscriptions`, `refcounts`
— no `instance`. Only `MdLeaseAck` carries it. `tape.read` in
`on_start` runs on the heels of attach (`session.py`); waiting
for the first ack is a race. Add `instance` to `MdAttachResult`
so the session can route a warm-up without watching heartbeats.

**A feed this session never attached.** Today's `read()` may
ask for any feed; an empty slice means "nothing has ever
subscribed". After the move the session only knows instances
it attached. A ticker that is not on this session's `md` map
**raises** — STS must not anycast to invent an owner.
"Nothing recorded" remains MD's empty `TapeSlice` when the
right instance has no prints.

**Redis is durable only with AOF and a volume.** Default Redis
is memory plus an occasional RDB snapshot. A restart then
loses everything since the last snapshot — the deque loss this
section refuses. Each region's Redis runs `appendonly yes` and
stores `/data` on a volume. Production compose lives on the
servers, not in this repo; the roll has to add that service
there, the same way it added NATS `-sd`.

**Contract.**

- Warm-up history is the history of the MD that holds the feed,
  surviving that MD's process restart as long as the region's
  Redis (AOF + volume) does.
- Same-region handover (`md-jp-1` → `md-jp-2`) appends to the
  same Redis key. No deque copy. Coverage still stamps a window
  where nobody was subscribed.
- Cross-region is two tapes. Asking the wrong MD is an empty
  slice (`recording=false`), which is a normal answer, not a
  fallback to the other region.
- Anycast `Topics.MD` and `md.fetch` are the wrong subjects.
  `md.fetch` is public venue data and unkeyed on purpose. Tape
  is the opposite.
- `DEFAULT_LIMIT` (200k) does not fit one core NATS message
  (~40 MB vs 1 MiB). The RPC is cursor-chunked. The strategy
  still sees one `TapeSlice`.

**Must not.**

- STS must not take a `REDIS_URL`. A session in TW warming up
  on a JP feed would otherwise open the wrong disk, or need a
  region map the strategy layer is forbidden to have.
- Append still must not fail the live fan-out. A Redis blip
  loses records, not ticks.

**Risk.** Redis is a new production service, scoped to MD, not
a second `BrokerTransport`. Losing the region's Redis, or
running it without AOF, loses that region's warm-up; live
ticks are unaffected. A tape_keeper is still what keeps a
feed pumping when no trading strategy is attached — it does
not become optional. [`docs/MdHandover.md`](MdHandover.md)
rested on "the tape is not lost on MD restart because
JetStream is on another host"; the same sentence is now true
of regional Redis with AOF, and false of a memory deque or
RDB-only Redis.

### Session fencing (not KV)

This is already core pub/sub: STS publishes `LeaseHeartbeat` on
`sts.td.{session}` / `sts.md.{session}`; TD and MD ack
(`LeaseAck`, `MdLeaseAck`) and echo the token.

**Today.** TD/MD run a sibling `_watch_timeout` every 0.5s
against wall-clock grace (MD 3s, TD 5s). STS counts MD ack
silence the same way, and only for MD — a quiet TD does not
stop the strategy. The same STS loop also `mark_alive`s a KV
key.

**After.** Still a timeout. `LeasedSessionLink._pump` is
`async for env in subscribe(...)` (`link.py`). When STS dies,
no message arrives and the loop does not wake, so it cannot
"count silence" by itself. Detecting a missing heartbeat
needs `wait_for` around the next message, or the sibling
timer. That is the same clock in a different shape. Keep a
timeout; count **intervals**, not raw wall-clock grace.

Both directions use **three** missed intervals. 1 Hz → ~3s,
in today's MD band, looser than inventing a 2-interval /
~2s peer side (one drop plus jitter — the false-trigger
this document already refuses). Today's TD 5s tightens to
3s; that is the behaviour change, and it is symmetric.

`heartbeat_interval` is an STS constructor argument
(`session.py`, default 1.0). TD/MD do not have it. Carry
it on `LeaseHeartbeat` (or freeze it as a protocol
constant). A peer that has not seen a heartbeat yet uses
the constant / default so attach-before-first-hb still
has a timeout.

- STS → peer: three heartbeats without an ack from an
  instance that has acked at least once → that peer is
  down. Stop trading. TD gets the same rule MD already
  has. A quiet TD **does** stop the strategy.
- Peer → STS: three intervals without a lease → detach.

**Contract.**

- Arm on the first ack (STS) / first heartbeat (TD/MD).
  Counting from start kills every deploy.
- Per instance, not per session. One of two MDs going quiet
  stops the session. The other staying green is not a live
  session.
- One missed interval is a dropped core message. Three is
  the fuse. The ack still echoes the token it is answering.
- "Renew" means the link is still valid. It is not
  `lease_hold`.

**Risk.** Core NATS is at-most-once. A burst of loss looks
like a dead peer. Do not count from zero before the first
ack / heartbeat. Do not use one miss.

**TD restart ends every session on it.** After KV goes, a
TD that boots and finds rows in its name with no local
link reaps them (strikes still apply — see Ownership).
STS then records three TD-misses and fails. That is the
opposite of today's "a quiet TD does not stop the
strategy", and it is intended: there is no book in KV to
keep trading against.

### Ownership, rebuild, orphans (KV `lease` and `counter`)

The heartbeat answers "is this already-attached link still
up?". It cannot answer "may I be the first to build this
account / rebuild this row?", because at that moment there is
no link to count misses on.

Under the instance contracts above, those questions go away:

| Today's key | Who asked | After |
|---|---|---|
| `td:owner:{api_id}` | TD `claim_owner` before `create()` | One instance, one process, plus boot `probe`. `self._accounts` is the only in-process map. |
| `{plane}:alive:{session}` | `mark_alive` / `claim_alive` / `is_alive` | Rebuild goes to the named STS, or to the one derived from the row's TD region. Orphan is "this row names me and I do not have it locally", with strikes. |
| `backfill:lock:{api_id}` | advisory per `api_id` | `td.backfill.{instance}` plus an in-process `set[api_id]`. The subject stops the other instance; the set stops two concurrent walks of the same account on this one. |
| `cid:slot` (`counter_next`) | one allocation per new STS session | Nothing asks. The slot is 16 bits of the session id (`slot_for_session`). |

**`cid_slot` is derived, not allocated.** `owns()` compares
`slot_of(cid) == self.session.cid_slot` (`strategy/base.py`)
(superseded: v1 packs the session id itself; `cid_slot` /
`slot_for_session` are gone.)
so a session can ignore fills on `td.{api_id}.global` that
belong to another session on the same account. Two STS
instances (`sts-tw`, `sts-jp`) can hold the same `api_id` at
once — STS is not sharded by account.

An allocator is what a *guarantee* costs. Two STS processes
create sessions at once, so a counter that is not shared
gives two live sessions the same slot; a shared one is a
round trip and a row to keep seeded. `slot_for_session`
takes neither: `blake2b(session_id, 2)` is the same 16 bits
in every process and every release, so the sessions cannot
disagree, and a rebuild lands on the slot its resting orders
already carry without anyone having reserved it.

What that gives up is exclusivity. Slots are drawn from
`SLOT_SPACE` rather than walked, so two sessions can share
one, and `owns()` then reads the other's fills as its own.
It costs something only when both trade the same account —
`td.{api_id}.global` is per credential, so sessions on
different accounts never see each other. Measured against
the platform's own history (peak 2 live sessions per
account, ~1770 order-placing sessions a year) that is
~2.7% a year, accepted deliberately in exchange for
deleting the allocator. `sts_sessions.cid_slot` stays: rows
older than this derivation carry a slot it cannot reproduce,
and rebuild still reads the row, never recomputes.

**Orphan strikes stay.** TD's `_is_orphan` (`manager.py`)
counts `_ORPHAN_STRIKES` so a row that exists between two
attaches — live in the database, link not built yet — is
not reaped on the first scan. "This row names me and I do
not have it locally" without strikes closes that window
and detaches a strategy that is still coming up. The
reaper still does not subscribe to someone else's
heartbeat.

A TD restart reaps every row in its name (after strikes)
and the attached STS sessions fail on three TD-misses.
Write that down: it is a behaviour change, and it is
correct once the book is only in TD memory.

**Contract.** A second process of `td-jp-1` is forbidden.
Boot `probe` refuses the common cases and names who is
already serving. Simultaneous start still dual-opens;
heartbeats stay green. `liveness.py` as a SET NX policy
layer is deleted with the bucket.

**Risk.** `probe` is not a lock. Two boots in the same
window recreate [`Instances.md`](Instances.md) §7. That
is narrower than "the broker will not notice", and it is
what remains. Null STS rows are not a hole: the rebuild
scan derives their instance from the TD region, and a row
whose derivation is not unique waits for a person instead
of being taken by two STS.

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

`LeasedSessionLink` stays. Silence is still a timeout
(`wait_for` or the sibling timer); the change is counting
three intervals both ways, not deleting the clock.

What a plane may say, once the store families are gone, is
fan-out, request-reply, and a fenced session link. That is
core NATS. `BrokerTransport` shrinks to those three. A second
transport is not justified; the tape Redis is not one. The
cid sequence is Postgres, next to the row it already lands
on.

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
- One instance name, one process. Boot `probe` refuses if
  that subject already has a responder.
- `cid_slot` is derived from the session id, so it is the
  same in every process without one being asked.
- An STS session's instance is named or derived from its TD
  region. `instances.region` routes STS placement.
- Tape `read` of a feed this session did not attach raises.
- `/ws/status/sts` late-replays from REST, not `session_logs`.

## Risks, collected

1. **Same-name second process that passed `probe` together.**
   Dual venue session; heartbeats stay green. `--scale` and
   overlapped restart should die at boot instead.
2. **Null STS row whose derivation is not unique.** Cross-
   region credentials, two STS in one region, or no TD. It
   is not rebuilt; it waits on the Attention list. The
   deploy path refuses the same shapes without a name, so
   only rows from before this rule can look like that.
3. **Tape asked of the wrong MD, or of a feed never
   attached.** Empty warm-up that looks like "never
   recorded", or a raise. Session-level routing; attach
   result carries `instance`.
4. **Regional Redis without AOF, or volume loss.** Warm-up
   for that region is gone. Ticks are not. Restore Redis;
   do not read the other region's keys.
5. **`td.account` serialised behind leverage REST.** Sizing
   waits on venue. Views must be non-blocking memory reads.
6. **1 MiB tape reply.** Must chunk or the warm-up raises.
7. **Three-miss false trigger.** Packet loss, not a dead
   peer. Do not count from zero before the first ack. Do
   not use two intervals on one side.
8. **Late session-log UI depends on persist.** A down
   `log_persist` makes a fresh `/ws/sts/{id}` start empty.
   Live tail still works. Status is a different socket.
9. **`src` grows a Redis import in MD.** The tree-walk
   guard must exempt that module the way it exempts
   `mftik.broker`, and nowhere else. A Redis client in STS
   or `packages/common/src/mftik/strategy` is a regression.
10. **Recomputing `cid_slot` on rebuild.** The row is the
    only truth for a session created before the derivation
    existed. Read it; deriving would disown its live orders.

## What this is not

Not a second `BrokerTransport`. Redis is MD's tape disk.
Bringing the Redis transport back — lease, state, log, a CI
matrix — is the thing [`docs/RedisRemoval.md`](RedisRemoval.md)
removed.

Not a per-region NATS. One broker, one `NATS_URL`. Regions
split only the tape disk.

Not MD-memory deques, and not Redis with only RDB.
A process restart or a snapshot-only Redis would drop two
hours of prints the venue cannot rebuild. Regional Redis
with AOF and a volume is the durability; RPC is only the
read path.

Not "pubsub is CAS". The heartbeat protocol replaces
wall-clock grace with a counted timeout. Boot `probe` plus
the instance contracts replace `lease_take`. Mixing those
sentences is how dual-open comes back.

Not a process-local cid counter. That is how `owns()`
breaks.

## Order

1. This document, and the contract pointers in the docs
   listed below.
2. Session link: three missed intervals both ways, timeout
   kept (`wait_for` or the sibling timer), interval on
   `LeaseHeartbeat` or as a protocol constant. STS applies
   the same rule to TD acks as to MD.
3. Ledger and OMS RPC on `td.account`; delete KV writes and
   `StateProjection`.
4. `MdAttachResult.instance`; MD Redis (AOF + volume) +
   per-instance tape RPC; delete tape streams and `tapecov`.
   `StrategyTape` asks the session, not `broker.tape_*`.
   Unattached feeds raise.
5. Late `/ws/{domain}/{id}` from `session_logs`; late
   `/ws/status/sts` from the REST session list;
   `publish_log` → `publish`; delete the log stream and
   `fetch_log_buffer`.
6. Rebuild scan derives a null row's instance from its TD
   region and rebuilds only when that is this process; deploy
   sends an unnamed create to the derived subject and refuses
   when the derivation is not unique; cid sequence
   (`nextval % 65536`); boot `probe` on each plane's
   instance subject. Then delete `liveness.py` SET NX,
   `lease_*`, `counter_next`, the `state` / `lease` /
   `counter` buckets.
7. Delete `mft_ps`. Stop ensuring streams in `connect()`.
   Drop `-js` / `-sd` from compose and the node template
   once nothing creates a stream. Add regional Redis to
   the production compose (not in this repo).

Two and three can overlap. Four needs `MdAttachResult.instance`
before tape reads can be routed. Six depends on two (so
`mark_alive` is no longer in the heartbeat loop) and on
the derivation filter being in the rebuild scan.

## Docs this updates

| Doc | What changes |
|---|---|
| [`Broker.md`](Broker.md) | Points here. Today's JetStream table stays as a description of the tree until the code moves. Deployment `-js` is marked as current, not destination. |
| [`BrokerPatterns.md`](BrokerPatterns.md) | A/B/F/G named as families this document retires. |
| [`BrokerProvisioning.md`](BrokerProvisioning.md) | Stream/bucket migration is moot once those objects are gone. Overlap deploys are forbidden for a different reason. |
| [`MdHandover.md`](MdHandover.md) | Tape durability is regional Redis. Same-name competing consumers are not a given. |
| [`Instances.md`](Instances.md) | One process per instance name is an invariant. Tape is not in the broker. `claim_*` is not the long-term guard. `region` routes STS placement for unnamed sessions. |
| [`RedisRemoval.md`](RedisRemoval.md) | Redis may return only as MD's tape disk. |
| [`Alert.md`](Alert.md) | Late session-log replay is `session_logs`. Late `/ws/status/sts` is the REST session list. |
| [`README.md`](../README.md) | Tape / lease sentences match the destination. |
