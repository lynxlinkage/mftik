# Plane instances — one node, many td / md / sts

A node runs one process per plane. The operator sees three cards on Home and
`strategy.yml` names accounts and feeds without ever saying *where* they run.
That is fine while every process is on one box. It stops being fine the moment
a credential may only be used from one jurisdiction, or a feed is worth reading
from a host next to the venue.

This describes how one API plane addresses several instances of the same plane
— `td-us`, `td-jp`, `md-jp-1`, `md-jp-2`, `sts-tw` — and how a deploy chooses
between them.

Nothing here is built yet. The files and symbols named below are what the
change rests on, all of them checkable in the tree today.

**Terminology.** *Plane* is the README's word for a domain: STS, TD, MD, SYM,
Paper, API. *Instance* is one process of a plane. The word "plane" is already
overloaded — `Api` in `packages/db/src/mftik_db/models/api.py` uses it for
Binance spot / USD-M / COIN-M, which are venues here — so this document says
"instance" and never "node", which means a whole stack.

## What is already true

Facts the design rests on.

**The data plane is already addressed per resource.** `Topics.td_order(api_id)`
and `Topics.td_account(api_id)` name an account; `Topics.md_session(session_id)`
and `Topics.sts_md_session(session_id)` name a session. The docstrings in
`packages/common/src/mftik/protocol/topics.py` say why: `serve` is a competing
consumer, so a shared subject would let a process that does not hold the
account answer for it. Everything after an attach is already routed to exactly
one owner.

**The control plane is not.** `Topics.TD`, `Topics.STS`, `Topics.MD`,
`Topics.SYM` and `Topics.PAPER` are five flat constants, and every process of a
plane serves its own on one subject — `apps/td/src/mftik_td/app.py:45`,
`apps/md/src/mftik_md/app.py:47`, `apps/sts/src/mftik_sts/app.py:43`. That
subject is a Redis list read with `BLPOP`. Start a second MD and which one
takes an attach is a lottery.

**This is already written down.** `docs/MdHandover.md` states it outright under
*The hard parts, 2*: a green MD that starts `run_rpc` immediately begins
answering `attach` for sessions whose feeds it does not have. That document
needs a `role=standby` gate for the length of a deploy. This one needs the same
subject split permanently, for a different reason, and the two should land on
one mechanism rather than two.

**Runtime feed changes are already session-scoped.** `MD_SUBSCRIBE` and
`MD_UNSUBSCRIBE` are read off `sts.md.{session_id}` in
`apps/md/src/mftik_md/session/manager.py:671`, not off `Topics.MD`. So adding
and dropping a feed mid-run needs no work here — only the attach does.

**The health probe already exists.** `/stats` sends a `HealthCheck` on each
plane's subject with a 1.5s timeout and reports the plane healthy if a reply
comes back (`apps/api/src/mftik_api/routes/stats.py:33`). Asking one instance
whether it is up is that call with a different subject, so instance health
needs no new mechanism — only a list of names to ask.

**`Account` is the shape the declared table wants.** An operator-created named
row, referred to by name in `strategy.yml`, turned into an id at deploy by
`_resolve_td` (`apps/api/src/mftik_api/routes/sts.py:670`), which refuses an
unknown name with a sentence naming it. Declaring an instance is the same
motion against a different table.

**`td:` has already survived this migration once.** It went from a list of
api ids to a mapping of account name to settings. `_TD_LIST_HINT` in
`packages/common/src/mftik/protocol/strategy_yml.py` is the sentence
`mftik check` prints for a document still using the old shape, and
`_refuse_collapsing_td_keys` is the parse-level guard against a mapping whose
keys `safe_load` would silently fold. `md:` needs the same two things, and can
copy both.

**Postgres is off the hot path.** `apps/td/src/mftik_td/oms/oms.py` and
`oms/ledger.py` contain no `session_scope` call — OMS state and the balance
ledger live in Redis and in memory. `history.py` is a batching background
writer. The database carries control-plane rows and trade history only.

## The two drivers are not the same requirement

They are usually stated together and they want different things.

**Compliance** is about which host opens the socket to the venue. A TD in
Tokyo holding a JP credential satisfies "this key is only used from JP"
regardless of where the broker is. Instance routing is necessary and
sufficient. This document delivers it.

**Colocation** is about the loop `MD → STS → TD → venue`, and every leg of
that loop crosses Redis: order entry is a `BLPOP` request-reply on
`td.order.{api_id}`, and every tick is a pub/sub message on
`md.{session_id}`. `BrokerConfig.from_env` reads one `REDIS_URL`
(`packages/common/src/mftik/broker/config.py:49`) and there is no per-instance
override. So a single broker means **exactly one region can be colocated
properly**; instances elsewhere pay a WAN round trip inside the loop, and
moving MD next to the venue buys nothing — it relocates the long leg from
"MD to venue" to "MD to Redis" rather than removing it.

Instance routing is therefore necessary for colocation and not sufficient for
it. A per-region broker is a much deeper change — `Broker()` is constructed
once per process and `StsSession` holds a single `self.broker` for TD RPC, MD
pub/sub, lease, eventlog and logs; the tape lives in Redis streams, so a split
broker splits warm-up history; and the `instances` table would stay global on
the shared Postgres while a health probe could not leave its own Redis, so Home
could name every instance and vouch for none outside its own region. That
change is the federated-nodes design, and `docker-compose.peer.yml` is already
most of it. **Out of scope here, and it should stay a separate document.**

## Non-goals

- **Not a per-region broker.** One `REDIS_URL`, one `DATABASE_URL`, unchanged.
  Every subject named below is a new name inside the existing keyspace.
- **Not two owners for one credential.** An `api_id` keeps exactly one TD
  owner. The lease, the OMS and the `client_order_id` slot all rest on that,
  and serving one key from two TDs is two processes deciding to trade.
- **Not high availability.** A named instance that is down fails the deploy
  with a sentence. Failing over to a peer is `docs/MdHandover.md`'s problem and
  wants the cooperative handshake described there, not a retry here.
- **Not provisioning.** The node never starts, stops or restarts a plane, and
  never will as part of this. A declared row is a statement of what operations
  should have deployed; reconciling it with reality happens wherever the
  compose file lives. All the node owes anyone is an unambiguous report of the
  mismatch.
- **Not strategy-code federation.** `qualify.py`'s `origin` (`private::Tiny`,
  `node1::Tiny`) names where a strategy's *source* came from. It has nothing to
  do with which process runs it, and the two namespaces must not be merged.

## Invariants

Each is meant to be a test.

- **PI-1** An attach reaches the instance the deploy named, or the deploy
  fails. It is never served by a different instance of the same plane.
- **PI-2** A deploy that names an instance fails before any plane is asked to
  do anything unless that instance is both *declared* and *answering*, and the
  two failures say different things: an undeclared name is a typo, a declared
  name that does not answer is an outage. Neither waits or retries — the node
  does not make an instance exist.
- **PI-3** A session's MD feeds may be split across instances. Each feed is
  held by exactly one.
- **PI-4** One instance detaching a session does not tear down another
  instance's attach for that same session.
- **PI-5** A `strategy.yml` with no instance named behaves exactly as it does
  today: any instance of the plane may answer.
- **PI-6** An instance that dies is reaped by its own rows only. A peer's rows
  and liveness keys are untouched.

PI-4 is the strict one and the one to design against. Three separate pieces of
today's code violate it the moment two MDs serve one session — see *The hard
parts*.

## Instance identity

Every process reads `MFTIK_INSTANCE` at boot and defaults it to the plane name
(`md`, `td`, `sts`). An existing deployment therefore keeps working unchanged
and its instance is called `md`, which is also what `PI-5` needs.

A declared row says what should exist. A probe says whether it answers. Those
are the only two facts, and Home is the difference between them:

| | Answers a probe | Silent |
|---|---|---|
| **Declared** | Connected | **Down** — the row is what remembers it should be here |
| **Not declared** | Not shown | Does not exist |

*Declared and silent* is the whole reason for the table. An MD that is
OOM-killed and never restarts would otherwise simply vanish from Home, with
nothing anywhere remembering it was supposed to be there. That is the failure
`liveness.py` and the orphan reapers exist to prevent one level down ("a session
the UI shows as running that nobody can stop"), and a whole plane disappearing
quietly is the worse version of it.

**The API checks; it does not guarantee.** Nothing here starts, stops or
restarts a plane, and a declared row that nobody deployed stays *down* forever
rather than provoking the node into fixing it. Making the row true is
operations work, done wherever the compose file lives; the node's whole job is
to state the mismatch clearly enough that a person knows to go and do it. That
is why a deploy naming an absent instance refuses immediately instead of
waiting or retrying — waiting implies something is on its way, and nothing is.

### Declared: the `instances` table

Operator-owned, and the authority on *names*.

| Column | Why |
|---|---|
| `name` | `td-jp-1`. Unique. What `strategy.yml` and `apis` refer to |
| `domain` | `td` / `md` / `sts` |
| `region` | Operator label. Free text; nothing routes on it |
| `enabled` | Retire an instance without deleting the rows that reference it |
| `created_by`, `created_at` | As every other operator-created table has |

This is the shape `Account` already has: an operator-created named row that
`strategy.yml` refers to by name and that `_resolve_td`
(`apps/api/src/mftik_api/routes/sts.py:670`) turns into an id at deploy,
refusing with `unknown td account name` when the name is not there. Instance
resolution is the same function against a different table.

**A row is not a precondition for starting.** A process comes up and serves
whether or not it is declared. Being declared is what makes it addressable by
name and visible on Home, not what lets it run — the node has no way to stop an
undeclared process and no business trying.

The cost is that an undeclared instance is invisible rather than wrong-looking.
A process deployed with `MFTIK_INSTANCE=td-jp-l` — lowercase L — does not appear
anywhere; all the operator sees is the declared `td-jp-1` reading *down*, with
no hint as to why. A registry of self-announcing processes would have shown the
typo sitting next to its intended twin. That is a real diagnostic and it is
given up deliberately, because a registry is a TTL, a heartbeat writer and a
second identity to keep straight, and this is the one place it would have paid.
The recovery, if it is ever wanted, needs nothing new: every plane already runs
`heartbeat_loop` publishing to `sys.heartbeat` with a `source` field, and
nothing in the tree subscribes to it today.

**`name` and `domain` are immutable. There is no rename.** A process learns its
name from `MFTIK_INSTANCE` in its own environment, set in a compose file on the
host — which for this deployment lives outside the repository and which the API
has never read and cannot write. The authority for the name is on the far side
of a boundary the node cannot cross, so a row edited here would not reach the
process that answers to it.

That is the opposite of `PATCH /apis/{api_id}`, which does rename, and the
docstring at `apps/api/src/mftik_api/routes/apis.py:161` says exactly why it is
safe to: `strategy.yml` resolves an account name to an `api_id` at deploy, so
the name is a lookup label and the integer is the address. An instance name is
not a label. It *is* the address — it is the subject string `td.td-jp-1` — and
half of it is held in a process's environment.

An id indirection does not rescue this. Serving `td.{instance_id}` means the
process must learn its id, which it can only do by looking itself up by the
name in its environment; the bootstrap join key is still the name. Putting the
id in the environment instead moves the immutable thing rather than removing
it, and makes the compose file a list of opaque integers.

So a rename in the world is four steps — declare the new row, redeploy with the
new `MFTIK_INSTANCE`, move `apis.instance_id` across, retire the old row — and
that is a feature. Every intermediate state is honest: the new row reads *down*
until its process is up, the old one reads *down* once its process is gone, and
at no point does anything read healthy while being wrong. A UI rename is wrong
in both directions the instant it is saved and says nothing.

What the UI may edit is what nothing routes on: `region`, `enabled`, and any
notes. `enabled=false` drains rather than evicts — new deploys refuse to name
the instance, sessions already attached keep running — because nothing else in
this tree tears down live work to satisfy a configuration change.

### Reported: the health probe

There is no presence registry. State is asked for when it is wanted, on the
instance's own unicast subject, and the answer is the current one rather than
one up to a TTL old.

`/stats` already does exactly this, three times: `_HEALTH_PROBES` in
`apps/api/src/mftik_api/routes/stats.py:33` sends a `HealthCheck` on each
plane's subject with a 1.5s timeout and calls the plane healthy if a reply
comes back. The change is to send one per declared row, to `td.{name}` rather
than `td`, concurrently.

`HealthStatus` returns `{status, service}` today and grows the fields a
registry payload would have carried:

| Field | Why |
|---|---|
| `name`, `domain` | What the process believes it is. Compared against the row it answered for |
| `role` | `standby` / `named` / `active` — see *Roles* |
| `version` | So a half-finished rolling deploy is visible |
| `venues` | Which venues this MD can reach. A deploy naming a feed the instance cannot serve should fail at deploy, not at subscribe |
| `api_ids` | Which accounts this TD currently holds |

Reporting `name` and `domain` back is not redundant with having addressed the
probe by name. A process started with `MFTIK_INSTANCE=td-jp-1` but running the
`md` command answers on the subject and says so, and the mismatch is the
diagnosis.

`region` is deliberately not among them. It is an operator's statement about a
deployment, and a process put in the wrong datacentre would report whatever its
environment says rather than where it is. Neither can be verified, but the
declaration is at least a stable record of intent — and this is the dashboard
compliance is read from.

Two consequences of probing rather than registering. A probe cannot see a
process it was not told to ask about, which is the cost priced above. And a
probe to a dead subject is not free — see *The hard parts, 6*, which is the one
piece of new machinery this approach does need.

## Roles

`docs/MdHandover.md` needs a green MD to answer no attach while it warms up.
This document needs an instance to answer only work addressed to it by name.
Those look like two switches and are one, because **standby has to gate the
unicast subject too**: blue and green are both `md-jp-1` and both serve
`md.md-jp-1`, so a green that gated only the anycast subject would still take
a named attach for feeds it does not have.

So it is one ordered role, not two booleans — which would admit a meaningless
fourth state and put a two-way interaction at every serve site:

| Role | Serves `md` | Serves `md.{instance}` | Runs `reap_loop` | Set by |
|---|---|---|---|---|
| `standby` | ✗ | ✗ | ✗ | The cutover protocol. Temporary |
| `named` | ✗ | ✓ | ✓ | Configuration. Permanent |
| `active` | ✓ | ✓ | ✓ | Configuration. Permanent, and the default |

Configuration names the *target* role. A process boots into `standby` if it is
joining as green, otherwise straight into its target; the cutover is the
transition to it. Blue runs the same transition backwards — to `standby`,
which stops it taking new attaches while its existing links keep running,
because `standby` gates `run_rpc` and never the dispatcher.

**Cutover ordering costs a poll.** `Broker.serve` checks its stop event only
at the top of the loop, so a serve loop that has been told to stop sits in
`BLPOP` for up to `serve_poll_seconds` — one second in production — and *will
still take a request off the list* in that window. Blue and green must
therefore never serve one subject at the same time: blue leaves, the poll is
waited out, green enters. Two subjects, so two waits.

The gap that leaves is safe, and this is why the ordering is affordable at all.
These subjects are Redis lists, not pub/sub: `Topics.td_order`'s docstring
makes the point that a request sent while nobody owns the subject waits in the
list rather than vanishing the way a pub/sub message would. MD attach is given `timeout + 5.0` in
`deploy_strategy`, so a two-second gap is invisible. It is still two seconds of
`docs/MdHandover.md`'s cutover budget, which is already bounded by STS's
tolerance for a missing `MdLeaseAck` — one number, two claims on it.

`standby` is MD-only in practice. `docs/MdHandover.md` is explicit that STS and
TD must not be blue/greened, since two copies of a strategy session is two
copies deciding to trade. A TD in `standby` is not dangerous, only useless — it
answers nothing — so it should be refused at boot to fail fast on a
configuration that does nothing, not because something unsafe would follow.

## Subject naming

`Topics.TD` becomes `Topics.td(instance)` returning `td.{instance}`, and the
same for `sts`, `md`, `sym`, `paper`. The bare `td` stays as the anycast
subject, and an `active` instance serves **both** its own and the bare one —
that is what makes PI-5 hold without a special case, and what lets this ship
before anything names an instance. `active` is the default precisely so that
shipping stage 3 changes nothing observable; an operator who wants an instance
to take only work addressed to it sets `named`. See *Roles*.

Two more subjects need the treatment for different reasons:

- **`md.fetch`** is deliberately unkeyed, and the docstring argues the case
  well: a read is owned by nobody, so competing consumers are the point. That
  argument is about *correctness* and survives. What it does not cover is
  latency or jurisdiction, both of which are the reason this document exists.
  Add `md.fetch.{instance}`; keep the unkeyed one as the default.
- **`log.md.{venue}`** collides across instances — two MDs on Bybit write the
  same channel and `/ws/md/{venue}` cannot tell them apart
  (`apps/api/src/mftik_api/ws.py:146`). It becomes
  `log.md.{instance}.{venue}`.

`Topics.td_backfill()` stays unkeyed. Its docstring's reasoning is the real
one — the work is idempotent, unowned, and an account with no live attach still
needs it — and none of that changes here.

## Choosing an instance

**MD, in `strategy.yml`.** The feed list becomes a mapping, exactly as `td:`
did:

```yaml
md:
  md-jp-1:
    - bestquote.Binance_Spot_BTCUSDT
  md-jp-2:
    - aggtrade.Bybit_Perp_BTCUSDT
```

A plain list keeps meaning "any MD" (PI-5). `StrategySpec._md_feeds` already
normalizes every entry through `Topics.normalize_md_feed` because a ticker
typed two ways would refcount as two feeds; that normalization stays, per
instance. The duplicate-key and merge-key guards `_refuse_collapsing_td_keys`
applies to `td:` must be applied to `md:` too — the same silent fold is
available here, and a feed list that loses half its entries to an anchor is
worse than one that fails to parse.

**TD, on the `apis` row.** Not in `strategy.yml`. A credential is bound to a
region as a matter of fact, not as a matter of what a strategy author typed,
and compliance is a property of the key. `Api` gets a nullable `instance_id`
foreign key and TD attach routes by it. A foreign key rather than a name
string, because a typo in a free-text column is a credential that silently
never attaches; deleting an instance a credential still points at is refused
rather than cascaded, the way `list_live_for_origin` already refuses to delete
a registry entry a live session is using. `TdSettings` is `extra="forbid"` and
empty today; if a per-deploy override is ever genuinely wanted it is one
optional field there, but the default must come from the row.

**STS, at deploy.** `POST /sts/deploy/{type}` grows an optional instance
parameter. It does not belong in the document: the same `strategy.yml` should
be deployable to `sts-tw` and to `sts-jp` without editing it.

## The hard parts

### 1. Three places assume one MD owns a session

All three are live bugs the moment PI-3 holds, and none of them is caught by
the current tests because the current tests never run two MDs.

**The liveness key is per `(plane, session)`.** `_ALIVE_DOMAIN` in
`apps/md/src/mftik_md/session/manager.py` is `SessionDomain.MD.value`, so two
MDs attached to one session write and clear *one* key. The first to detach
calls `clear_alive`, and the survivor's reaper — `_ORPHAN_STRIKES = 2` — tears
down a link that was healthy. The key must become `md:{instance}`, and
`reap_orphans` follows it.

**`mark_session_done` closes every row.** `MdSessionRepository.mark_done_session`
(`packages/db/src/mftik_db/repositories/session.py:477`) selects on
`session_id` alone and marks every live row done, whatever venue or instance
wrote it. It needs the instance in the predicate.

**`md_sessions` is unique on `(venue, session_id)`.** Split one venue's feeds
across two instances and the second attach collides. The constraint becomes
`(instance, venue, session_id)`.

### 2. The deploy fans out, and the rollback has to unwind it

`deploy_strategy` in `apps/api/src/mftik_api/orchestrate.py` sends exactly one
`MD_SESSION_ATTACH`. It becomes one per named instance, each carrying its own
subset of feeds, and `MdAttachResult.refcounts` merges across the replies.

The rollback is the part that gets harder. Today MD attach either happened or
did not, so the `except` block's job is to fail the STS session and stop. With
N attaches, a failure on the third leaves two live, and those must be detached
before the STS fail — otherwise the reaper is what eventually cleans them, two
scans and up to a minute later, with the feeds live in between.

Resolution happens first, before any plane is asked to do anything (PI-2). It
is two checks, not one: `deploy_strategy` resolves every named instance against
the `instances` table, then probes each one, then — for MD — checks the
answering instance lists the venue the feed needs. The two failures are
different sentences, because they are different problems: an undeclared name is
a typo the operator should fix in the document, and a declared name that does
not answer is a machine they should go and look at. Failing at attach time
instead of here means the operator gets a lease timeout in place of either.

### 3. Splitting a venue across instances duplicates the wire

`docs/MdVenueSubscriptions.md` is about not opening a venue topic twice, and
its ledger is per socket in one process. Two MD instances both reading
`tickers.BTCUSDT` on Bybit is two sockets and two rate-limit budgets, and no
ledger spans them — nor should one, because a shared ledger across a WAN is a
worse idea than a duplicated subscription.

This is a real cost, and it is the cost being bought deliberately: an operator
who splits one venue's feeds across `md-jp-1` and `md-jp-2` is asking for two
connections. What must not happen is paying it by accident. The health reply's
`venues` field and the deploy-time check are what make the split explicit, and
Home showing both instances is what makes it visible afterwards.

### 4. `md_ids` changes shape

`StsSessionRow.md_ids` is a JSON list of feed keys, and `StsCreateSessionRequest.md`
is `list[str]`. Both become instance-keyed. `attached_api_ids` in
`strategy_yml.py` is the precedent for the compat shim — it reads either `td`
or the older `td_api_ids` off a row — and the same trick keeps old rows
rebuildable. `SessionManager._rebuild` at
`apps/sts/src/mftik_sts/session/manager.py:838` reads `md_ids` off the row and
must handle both shapes for one release.

The seven per-strategy templates in
`packages/common/src/mftik/protocol/strategy_catalog.py` all write `md:` as a
list. They stay valid under PI-5 and should stay lists — a template is what a
person starts from, and starting them all on an instance name teaches the
wrong default.

### 5. Everything grows an instance dimension in tests

The repo tests attach, detach, lease expiry, orphan reaping and rebuild per
plane, and each of those files gains a two-instance case. `test_md_shared_venue_topics.py`,
`test_md_orphan_reaper.py`, `test_md_lease_resilience.py`, `test_td_orphan_reaper.py`
and `test_lease_resilience.py` are where PI-4 and PI-6 get their tests. This is
not incidental work; it is where the bugs in *The hard parts, 1* would have been
caught.

### 6. Probing a dead instance leaks, and the obvious fix is wrong

`Broker.request` deletes the *reply* key in its `finally`
(`packages/common/src/mftik/broker/client.py:757`, under `reply_ttl_seconds`).
It does not remove the request. That request was `RPUSH`ed onto
`{key_prefix}:rpc:{subject}`, which has no TTL, and if nothing is serving the
subject nothing ever pops it.

So a dashboard that probes a down instance every few seconds writes a record
per probe into a list nobody will drain. At a 5s refresh that is roughly 17k
entries a day per down instance, and production Redis is capped at 512mb with
`maxmemory-policy noeviction` — the same Redis that carries order RPC, the
ledger and liveness. Filling it does not degrade the dashboard, it stops the
writes that trade. And when the instance finally boots, the first thing it
does is drain a heap of expired health checks.

**A blanket TTL on rpc queues is the wrong fix**, and the tree says so in two
places. `Topics.td_order`'s docstring: a request sent while nobody owns the
subject "waits in the list rather than vanishing the way a pub/sub message
would". `Broker.post`'s: "A request left in the list because nothing is serving
the subject yet is not lost: the next consumer to come up takes it, which is
the recovery a pub/sub message could not offer." An attach should wait. A
backfill should wait.

A health check is the one RPC where waiting has no value — an answer that
arrives after the question stopped being asked tells nobody anything. So it
gets its own subject, and expiry is correct semantics *there* rather than a
compromise. `Envelope` already carries `ts`, so the serving side can also drop
a probe older than its own timeout, which costs one comparison and closes the
case where a queue outlives its expiry.

This is the only genuinely new machinery a probe-based design needs, and it has
to land with the first probe rather than after it — the leak is invisible until
the Redis it shares with order entry is full.

## Schema

Four migrations, all additive.

| Migration | Change |
|---|---|
| `instances` | New table: `name` (unique), `domain`, `region`, `enabled`, `created_by`, `created_at` |
| `apis.instance_id` | Nullable FK to `instances.id`. Null means any TD |
| `md_sessions.instance` | `String(64)`, plus `uq_md_sessions_venue_session` → `(instance, venue, session_id)` |
| `sts_sessions.md_ids` | JSON list → instance-keyed mapping, read through a compat shim |

`md_sessions.instance` is a plain string and deliberately **not** a foreign
key: it is history, it records the name as it was at the time, and retiring an
instance must not break the rows that describe what it did. `md_sessions.venue`
is a plain string for the same reason.

`SessionInfo` gains `instance` so the session lists can show it.

## API and UI

`/stats` is the visible half of the request. `_HEALTH_PROBES` in
`apps/api/src/mftik_api/routes/stats.py:33` hardcodes three subjects and the
route returns three `DomainStats`. It becomes: read the `instances` table, join
emit one row per declared instance, and probe each on its own unicast subject
concurrently — so the page costs one timeout however many instances are down,
not one per instance. `DomainStats` gains `instance` and `region`, and its
`healthy` boolean has to admit a third state: a declared instance that does not
answer is *down*, which is a fact worth rendering differently from a plane that
was never asked.

Home renders that on a fixed three-column grid (`repeat(3, minmax(0, 1fr))` at
`frontend/src/routes/+page.svelte:105`) with a `d.domain === 'sts'` special
case for the link. It becomes grouped by plane with N cards under each.

`/md/sessions` and `/td/sessions` need no routing work. They are anycast RPCs
answered from the database, and any instance gives the same answer — only the
new `instance` field has to reach the response.

`mftik check` stays a local parse. The `instances` table does make a name
checkable without asking any plane — one ordinary HTTP query, no broker — so
this is now a choice rather than a limitation. It stays local because the
command's value is that it works offline, and the deploy refuses an unknown
name anyway (PI-2). If a connected check is ever wanted it belongs behind the
same flag as the rest of `mftik`'s node round-trips, not in the default path.

## Suggested staging

Each stage is useful alone and leaves the tree shippable.

1. **Instance identity.** The `instances` table with its CRUD, and
   `MFTIK_INSTANCE` read at boot. Nothing routes on either yet.
2. **Home lists instances**, and the expiring health subject that makes probing
   safe (*The hard parts, 6*) lands with it, not after. `/stats` probes one row
   per declared instance; the grid groups by plane. Declaring two MDs and
   starting one shows a connected row and a **down** row, which is the whole
   point and is also what makes the rest debuggable.
3. **Unicast subjects and the role enum.** `Topics.td(instance)` and friends,
   plus `standby` / `named` / `active` gating which serve loops exist. Every
   instance defaults to `active`, so no caller uses the unicast subject yet and
   nothing changes observably — which is the point. This is also the stage that
   owns the two `docs/MdHandover.md` edits below, because it is what makes
   their sentences false.
4. **The three PI-4 fixes.** Per-instance liveness key, `mark_done_session`
   predicate, the `md_sessions` constraint. Do this *before* anything can
   split a session across instances, not after.
5. **TD routing.** `apis.instance`, attach routes by it. Smallest of the
   routing changes and the one compliance actually needs.
6. **MD routing.** The `strategy.yml` mapping, the deploy fan-out, the
   rollback unwind, `md_ids`. The largest stage and the one to do last.
7. **STS selection.** The deploy parameter. Independent of everything above.

Stage 5 alone closes the compliance requirement. Stages 6 and 7 are what make
a single session span instances, and can wait.

## Docs that stay right

`docs/MdHandover.md` needs two edits, and both belong to stage 3 rather than to
a mop-up ticket.

*The hard parts, 2* says `broker.serve(Topics.MD, ...)` is a shared subject
with competing consumers, which is the whole reason a green MD needs
`role=standby`. Once `Topics.md(instance)` exists that sentence is half true —
the anycast subject is still shared, the unicast one is not — and the section's
conclusion gets stronger rather than weaker: standby must gate **both**, for
the reason given under *Roles*. `role=standby` becomes one value of the enum
there instead of a boolean of its own.

Its protocol sketch reads `green starts serving Topics.MD; blue stops serving`.
That ordering is backwards once a poll can still take a request after a stop:
it becomes blue stops, the poll is waited out, green starts — per subject. The
line and the cutover budget around it both need updating.

Its *What is already true* entry — "MD instances are already competing
consumers" — stays right either way.

`docs/MdVenueSubscriptions.md` needs no edit. Its ledger is per socket in one
process and stays that way; *The hard parts, 3* above is a new fact about what
happens between processes, not a correction to anything it claims.

`README.md` needs no edit. Its plane table says a node owns one job per plane,
not one process per plane, and that stays true.

`docs/StrategyEnvironment.md` and `docs/CLI.md` mention `strategy.yml` but
neither documents the `md:` shape. Stage 6 owns re-checking that, since it is
the ticket that would make such a sentence false.

## Open questions

1. Should `active` really be the default? It is what makes PI-5 free and stage
   3 a no-op, but it also means a misconfigured instance silently answers work
   meant for a peer — the failure this document exists to remove. The
   alternative is `named` by default and an explicit opt-in to the anycast
   pool, which is safer and breaks every existing deployment on upgrade.
   *Roles* assumes `active`; this is the assumption to challenge first.
2. How often does Home probe, and does it probe on view or on a timer? Every
   probe to a down instance is a write to a queue that expires rather than
   drains, so the refresh interval is a cost as well as a freshness knob.
3. What does an MD instance do with a feed for a venue it cannot reach — refuse
   at attach, or refuse at deploy from the health reply's `venues`? Refusing at
   deploy is a better message and a staler fact.
4. Does `api_ids` belong in the health reply at all? `td_sessions` is already
   the source of truth for it, and a reply that has to assemble it makes the
   probe do real work rather than answer instantly.
5. What must be true before an instance row can be deleted? `apis.instance_id`
   is a foreign key and refuses on its own, but `md_sessions.instance` is a
   plain string by design and enforces nothing, so a live session can name an
   instance being retired. That wants an application check of the kind
   `list_live_for_origin` already performs for registry entries — the question
   is whether it blocks the delete or only warns.
6. Should `md.fetch.{instance}` be preferred automatically when a session's
   feeds all name one instance? The unkeyed subject is the better default for
   correctness and the wrong one for a colocated read.
