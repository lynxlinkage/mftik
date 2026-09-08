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
subject is anycast — whichever process of the pool is free takes each
request. Start a second MD and which one takes an attach is a lottery.

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
ledger live in the broker's shared state and in memory. `history.py` is a batching background
writer. The database carries control-plane rows and trade history only.

## The two drivers are not the same requirement

They are usually stated together and they want different things.

**Compliance** is about which host opens the socket to the venue. A TD in
Tokyo holding a JP credential satisfies "this key is only used from JP"
regardless of where the broker is. Instance routing is necessary and
sufficient. This document delivers it.

**Colocation** is about the loop `MD → STS → TD → venue`, and every leg of
that loop crosses the broker: order entry is a request-reply on
`td.order.{api_id}`, and every tick is a fan-out message on `md.{session_id}`.
`BrokerConfig.from_env` reads one broker URL and there is no per-instance
override. So a single broker means **exactly one region can be colocated
properly**; instances elsewhere pay a WAN round trip inside the loop, and
moving MD next to the venue buys nothing — it relocates the long leg from
"MD to venue" to "MD to the broker" rather than removing it.

Instance routing is therefore necessary for colocation and not sufficient for
it. A per-region broker is a much deeper change — `Broker()` is constructed
once per process and `StsSession` holds a single `self.broker` for TD RPC, MD
fan-out, lease, eventlog and logs; the tape lives in the broker's own streams,
so a split broker splits warm-up history; and the `instances` table would stay
global on the shared Postgres while a health probe could not leave its own
broker, so Home
could name every instance and vouch for none outside its own region. That
change is the federated-nodes design, and `docker-compose.peer.yml` is already
most of it. **Out of scope here, and it should stay a separate document.**

## Non-goals

- **Not a per-region broker.** One broker, one `DATABASE_URL`, unchanged.
  Every subject named below is a new name in the broker this node already has.
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
- **PI-7** An `api_id` is held by exactly one TD *process*, enforced rather
  than asserted. Two processes configured with the same `MFTIK_INSTANCE` do not
  both take it — the second is refused and says who holds it.
- **PI-8** A session whose feeds are split across MD instances notices when any
  one of them stops answering. Losing part of the picture never leaves the
  strategy running on the rest.

PI-4 is the strict one and the one to design against. Three separate pieces of
today's code violate it the moment two MDs serve one session — see *The hard
parts*.

## Instance identity

Every process reads `MFTIK_INSTANCE` at boot and defaults it to the plane name
(`md`, `td`, `sts`). An existing deployment therefore keeps working unchanged
and its instance is called `md`, which is also what `PI-5` needs.

The name is one subject segment — `[a-z][a-z0-9-]{0,63}` — and both sides
enforce it: `POST /instances` refuses a declaration that does not match, and a
process refuses to start on an `MFTIK_INSTANCE` that does not. It has to be
both, because the two names are only ever compared for equality and neither
side can rewrite the other: there is no rename. A process that came up on
`MD-JP-1` would serve `health.md.MD-JP-1`, a name no declaration can hold, and
read as *down* on Home forever while being perfectly healthy. Failing at boot
turns that into a traceback naming the variable, while somebody is still
looking at the deploy.

This catches the names that could never have worked, not the ones that are
merely wrong — see the `td-jp-l` typo below, which is legal and undetectable.

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
| `created_at` | As every other table has |
| `created_by` | FK to `users.id`, **nullable**. Null means the migration created it, not a person — see *Bootstrap* |

#### Bootstrap

A fresh node needs `td`, `md` and `sts` to exist before anything can be
addressed or shown, and they arrive in the same data migration that creates the
table. Nothing is needed in `docker-compose.yml`: both it
(`docker-compose.yml:45`) and the template `mftik node-init` writes
(`packages/common/src/mftik/cli/templates/docker-compose.yml:87`) already run a
`migrate` service on `mftik-db-migrate`, and every plane waits on
`service_completed_successfully`. Writing rows from a migration is also already
how this repo makes data changes — `0029_binance_um_cm_rename.py` and `0030`
both rewrite stored values.

Three rows, not five. `sym` and `paper` are not instanced (*Which planes are
instanced*), so they get none.

**This is why `created_by` is nullable.** `migrate` waits only on Postgres,
while `seed` — which creates the Owner row — waits on `migrate` completing. So
on an empty database there is no `users` row at migration time, and a `NOT NULL`
`created_by` would fail the upgrade on exactly the deployment that has never
been upgraded before. Null is the honest value: nobody created these three.

The order inside the one migration is forced by the foreign key: create the
table, insert the three rows, add `apis.instance_id` nullable, point every
existing row at `td`, then set `NOT NULL`. The `td` row has to exist before a
`NOT NULL` FK has anything to reference.

An existing single-process deployment therefore upgrades into a node whose Home
shows three connected instances named `td`, `md` and `sts`, because
`MFTIK_INSTANCE` already defaults to the plane name and those processes answer
to it. That is what makes INS-1's claim — that it routes nothing and changes
nothing observable — true rather than merely intended.

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
| `named` | ✗ | ✓ | ✓ | Configuration. Permanent. **TD is always this** |
| `active` | ✓ | ✓ | ✓ | Configuration. Permanent. The MD and STS default |

Configuration names the *target* role. A process boots into `standby` if it is
joining as green, otherwise straight into its target; the cutover is the
transition to it. Blue runs the same transition backwards — to `standby`,
which stops it taking new attaches while its existing links keep running,
because `standby` gates `run_rpc` and never the dispatcher.

**Cutover ordering costs nothing on the serve loop.** A subscription is
cancellable and the loop stops when it is told. Blue and green must never
serve one subject at the same time: blue leaves, green enters.

The gap that leaves has to be covered. An unserved subject answers with no
responders rather than parking, which is better for a plane that is genuinely
down and worse for one that is three seconds from being up. Two things cover
it. The transport re-asks a subject that reported nobody, for half the
caller's own timeout and up to a second — enough for a handover, deliberately
not enough to hide an outage. Past that the caller's own retry takes over:
`_attach_with_retry` re-sends within `_ATTACH_BUDGET_S`, and the budget is
written as a budget for exactly this reason. MD attach is given
`timeout + 5.0` in `deploy_strategy`, so a two-second gap is invisible. It is
still two seconds of `docs/MdHandover.md`'s cutover budget, which is already
bounded by STS's tolerance for a missing `MdLeaseAck` — one number, two
claims on it.

`standby` is MD-only in practice. `docs/MdHandover.md` is explicit that STS and
TD must not be blue/greened, since two copies of a strategy session is two
copies deciding to trade. A TD in `standby` is not dangerous, only useless — it
answers nothing — so it should be refused at boot to fail fast on a
configuration that does nothing, not because something unsafe would follow.

## Subject naming

`Topics.TD` becomes `Topics.td(instance)` returning `td.{instance}`, and the
same for `sts` and `md`. **Those three planes and no others** — see *Which
planes are instanced* below.

MD and STS keep the bare subject as an anycast pool, and an `active` instance
serves both its own and the bare one; that is what makes PI-5 hold without a
special case, and what lets this ship before anything names an instance.

**TD has no anycast subject.** Its routing key is the `apis` row, which always
resolves (see *Schema*), so there is no such thing as unaddressed TD work. Only
three things ever reached `Topics.TD`: health, which is now probed per
instance; attach and detach, which carry an `api_id`; and `TD_SESSION_LIST`,
which turns out to need no plane at all — `handle_session_list` calls
`SessionManager.list_sessions`, and that method is one database query with no
in-memory state, in a route file that already opens `session_scope` for
`_api_labels`. `/td/sessions` reads the table directly and the RPC goes. So TD
instances default to `named`; `active` is an MD and STS default.

That end state arrives in **INS-5**, not in INS-3. Adding the named subject and
removing the shared one are separate changes, because three callers still send
attach and detach to the bare subject and none can name an instance until an
`api_id` resolves to one. INS-3 adds; INS-5 moves the callers and takes away.

Two more subjects change:

- **`td.backfill`** becomes `td.backfill.{instance}`, always. Its docstring's
  correctness argument survives untouched — the work is idempotent, unowned,
  and an account with no live attach still needs it — but that argument says
  nothing about *where the socket opens from*, and this is the one unowned job
  that carries a credential: `BackfillSession`'s own docstring says "any TD can
  **load the credential** and ask", `reader.py` builds each reader from
  `row.api_key` / `row.api_secret`, and `backfill_cron` sweeps every account
  with history on a timer. Left unkeyed, a US TD periodically opens a venue
  connection with a JP-only key. That is the compliance requirement failing on
  a schedule, not at an edge.

  The docstring's objection to keying does not apply. It argues that a keyed
  subject parks a request until the account's *owner* takes it, which for a
  retired account is forever. The key here is the **instance**, and an instance
  is up whether or not anybody is trading that account. For a jurisdiction-bound
  credential, "wait until `td-jp-1` is back" is the correct behaviour, not a
  regression.
- **`log.md.{venue}`** collides across instances — two MDs on Bybit write the
  same channel and `/ws/md/{venue}` cannot tell them apart
  (`apps/api/src/mftik_api/ws.py:146`). It becomes
  `log.md.{instance}.{venue}`.

### Which planes are instanced

`sts`, `td`, `md`. Not `sym`, not `paper`, and `md.fetch` stays unkeyed with no
per-instance variant.

**SYM** is off the hot path. `SymbolClient` caches in-process behind a TTL and
its module docstring says why: "Listings are near-static by definition, so a
process refetches on a miss or when its TTL lapses, **not per order**." Five
processes each hold a cache; a miss is rare and absorbs whatever the round trip
costs.

**Paper** is a simulated venue, and the point of it is one shared book. Two
paper engines are not a scaled paper plane, they are two unrelated markets.

**`md.fetch`** is the mirror of `td.backfill` and lands the other way for the
one reason that matters: it carries no credential. Klines, book snapshots and
quotes are public, so jurisdiction does not apply, and only latency argues for
pinning a read to an instance. *The two drivers are not the same requirement*
has already shown that with a single broker a pinned read wins nothing — the
caller's request has already crossed to the broker before any MD picks it up.
Pinning it now would be building for the per-region broker this document puts
out of scope. Deferred, and this paragraph is the record of why.

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
and compliance is a property of the key. `Api` gets an `instance_id` foreign
key — **`NOT NULL` from the first migration, not eventually** — and TD attach
routes by it. `MFTIK_INSTANCE` already defaults to the plane name, so the
migration declares an instance called `td` and points every existing row at it;
an existing single-process deployment upgrades without touching anything and
never sees a null. There is deliberately no "unassigned credential" state to
fall through to an anycast TD, because there is no anycast TD. A foreign key rather than a name
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

### 6. Probing a dead instance leaves nothing behind

`Broker.request` cleans up its own reply inbox and nothing else. `probe` is
core request-reply, which stores nothing anywhere, so an unanswered probe has
left no trace by the time the caller gives up.

A health check is the one RPC where waiting has no value — an answer that
arrives after the question stopped being asked tells nobody anything. So it
gets its own subject. `Envelope` already carries `ts`, so the serving side
can also drop a probe older than its own timeout, which costs one comparison
and is the second line of defence — see *How NATS answers* in
`docs/Broker.md`. An attach should wait. A backfill should wait.
`Broker.post`'s docstring: "A request left because nothing is serving the
subject yet is not lost: the next consumer to come up takes it, which is the
recovery a fan-out message could not offer, and the one place a durable
queue keeps that promise."

### 7. Nothing enforces one TD per `api_id`

`SessionManager.attach` (`apps/td/src/mftik_td/session/manager.py:204`) starts
with `acct = self._accounts.get(request.api_id)` and builds the account if it
is missing. `self._accounts` is process-local memory. **There is no
cross-process guard on `api_id` at all.**

Two processes configured with the same `MFTIK_INSTANCE` — a copy-pasted compose
block, a `--scale` — each build a `TradingAccount` and each run
`_serve_orders` on `td.order.{api_id}`. That subject is anycast, so they become
competing consumers and the account's order flow is split between two processes
that each believe they own it. Each keeps its own OMS, its own ledger and its
own reservations, and both publish to `td.oms.{api_id}` and
`td.ledger.{api_id}` — so the strategy watches its balances alternate between
two half-pictures.

This is latent rather than live today: nothing in the tree configures replicas.
There is no `replicas`, `scale` or `deploy:` key in `docker-compose.yml`,
`docker-compose.peer.yml` or the CLI's template, and no test runs two
`SessionManager`s of one plane at once. **So this is not a defect being
inherited — it is one this design would create**, because this design is the
first thing that makes running several TDs an ordinary operation rather than a
typo. Declaring "an `api_id` keeps exactly one TD owner" in *Non-goals* while
nothing enforces it is not good enough once that is true.

The primitive exists and is used for exactly this shape of problem one level
down. STS guards rebuild with `claim_alive`'s `SET NX` so two processes cannot
restore one session, and `liveness.py`'s docstring gives the reason: "several
processes of a plane serve the same RPC subject as competing consumers, so one
finding a live row it does not own has no way of knowing, by itself, whether a
peer is running it."

TD is the only plane with no key of its own — it imports `is_alive` and nothing
else, reading STS's key to reap orphans and never claiming anything. So where
*The hard parts, 1* found MD's liveness key needed fixing, TD's finding is the
opposite: there is none to fix and one to add. `attach` takes a `SET NX` claim
on `api_id` before building the account, renews it while held, releases it at
refcount zero, and refuses the attach naming the holder when it cannot.

### 8. STS cannot tell that an MD stopped

`_on_md_lease_ack` (`apps/sts/src/mftik_sts/session/session.py:679`) stores the
token and logs `MD lease established` once. The token goes nowhere:

```
204:  self._ack_tokens: dict[int, int] = {}
205:  self._md_ack_token: int | None = None
683:  self._md_ack_token = ack.token
813:  self._ack_tokens[api_id] = ack.token
```

Four occurrences, all writes. Neither field is read anywhere in STS or in its
tests. **The lease is one-directional in effect**: MD and TD watch STS's
heartbeat and tear down when it stops, and STS does nothing at all with the
acknowledgements coming back. An MD that dies today simply stops delivering;
`on_best_quote` quietly never fires again and nothing says so.

That answers `docs/MdHandover.md`'s open question 2 — "What is STS's actual
grace for a missing `MdLeaseAck`?" — with: there is none, because there is no
watchdog.

Splitting feeds turns this from a pre-existing gap into a blocker. Today one MD
holds all of a session's feeds, so its death costs the strategy *everything*,
and a strategy receiving nothing generally does nothing. Under PI-3 the session
loses only the feeds one instance held and keeps receiving the rest —
`CrossArb` quotes one venue and hedges on another, so losing the hedge venue
leaves it quoting against a price that is frozen rather than stale-and-known.
**Half a picture is more dangerous than none, because it keeps acting.**

So STS grows the mirror of the watchdog MD already runs (`_watch_timeout`
against `LEASE_GRACE_S`), per attached *instance* rather than per session, and
a grace exceeded goes to `_fail_from_infrastructure("md feed")` — a path that
already exists and today is only ever reached by a pump exception. This has to
land before INS-7 (it is INS-6), since INS-7 is what first makes half a picture
possible.

## Schema

Four migrations, all additive.

| Migration | Change |
|---|---|
| `instances` | New table: `name` (unique), `domain`, `region`, `enabled`, `created_at`, nullable `created_by`. Seeds `td` / `md` / `sts` in the same revision — see *Bootstrap* |
| `apis.instance_id` | FK to `instances.id`, **`NOT NULL`**. Existing rows point at the instance named `td` |
| `md_sessions.instance` | `String(64)`, plus `uq_md_sessions_venue_session` → `(instance, venue, session_id)` |
| `sts_sessions.instance` | `String(64)`, nullable. Which STS was asked to run this. Null is legacy and unpinned |
| `sts_sessions.md_ids` | JSON list → instance-keyed mapping, read through a compat shim |

`md_sessions.instance` and `sts_sessions.instance` are plain strings and
deliberately **not** foreign keys: they are history, they record the name as it
was at the time, and retiring an instance must not break the rows that describe
what it did. `md_sessions.venue` is a plain string for the same reason.
`apis.instance_id` is the exception because it is not history — it is live
routing, and a typo there is a credential that silently never attaches.

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

`/md/sessions` needs no routing work: it is an anycast RPC answered from the
database, and any MD gives the same answer — only the new `instance` field has
to reach the response. `/td/sessions` is the same query and stops being an RPC
at all, since TD has no anycast subject to send it on and the API can run the
query itself.

`mftik check` stays a local parse. The `instances` table does make a name
checkable without asking any plane — one ordinary HTTP query, no broker — so
this is now a choice rather than a limitation. It stays local because the
command's value is that it works offline, and the deploy refuses an unknown
name anyway (PI-2). If a connected check is ever wanted it belongs behind the
same flag as the rest of `mftik`'s node round-trips, not in the default path.

## Tickets

Eight, prefixed `INS-` so they do not collide with the `PI-` invariants they
close. Each leaves the tree shippable; `Verify` names the tests that say so.

### INS-1 — The `instances` table, its migration, and `MFTIK_INSTANCE`

**Goal.** A fresh node comes up with `td`, `md` and `sts` declared. Every
process knows its own name. Nothing routes on either.

**Scope.**

- `mftik_db.models.instance.Instance` — `name` (unique), `domain`, `region`,
  `enabled`, `created_at`, nullable `created_by`.
- One revision `0031`, in the order the foreign key forces: create the table,
  insert `td` / `md` / `sts`, add `apis.instance_id` nullable, point every row
  at `td`, set `NOT NULL`. Same revision adds `md_sessions.instance` with the
  `(instance, venue, session_id)` constraint, `sts_sessions.instance`, and the
  `sts_sessions.md_ids` shape change.
- `InstanceRepository`; `GET/POST/PATCH/DELETE /instances`. `PATCH` accepts
  `region`, `enabled`, notes and **refuses `name` and `domain`**.
- `MFTIK_INSTANCE` read in each `app.py`, defaulting to the plane name.

**Problem.** Everything else needs names to exist and a process to know its
own. Nothing can be tested before that.

**Solution.** Table and env only. No subject changes, no probes, no routing.

**Verify.**

- Migration on an empty database creates three rows and no `users` row is
  required — the case a `NOT NULL` `created_by` would have failed.
- Migration on a database with `apis` rows leaves every one pointing at `td`,
  and `apis.instance_id` is `NOT NULL` afterwards.
- `PATCH /instances/{id}` with a `name` is a 4xx; with `region` it is a 200.
- Deleting an instance an `apis` row references is refused (`test_apis_venue.py`
  style).
- Each `app.py` logs the instance it read; absent env yields the plane name.

**Depends.** Nothing.

### INS-2 — Home probes declared instances, and probing is safe

**Goal.** Home lists one row per declared instance and says *connected* or
*down*. Probing a down instance leaks nothing.

**Scope.**

- The expiring health subject and its `Envelope.ts` staleness drop
  (*The hard parts, 6*). **This half is not optional and not deferrable.**
- `HealthStatus` grows `name`, `domain`, `role`, `version`, `venues`.
- `/stats` reads `instances`, probes each concurrently, returns one
  `DomainStats` per instance with `instance`, `region` and a state that admits
  *down*.
- `frontend/src/routes/+page.svelte` — grouped by plane, N cards.

**Problem.** Without this nobody can see what the later tickets are doing, and
probing without the expiring subject fills the store that also carries order
entry.

**Solution.** Probe on demand; no registry, no TTL'd presence key.

**Verify.**

- Two declared MDs, one running: `/stats` returns one connected and one down.
- A probe to a subject nobody serves leaves the queue at length zero after the
  key's expiry — the leak test, and the reason this ticket is not just UI.
- A probe whose `ts` is older than its timeout is dropped by the server, not
  answered (`test_broker_poll.py` style).
- `/stats` with three down instances returns within one timeout, not three
  (concurrency, not serial).
- `test_stats_status_coverage.py` grows the instance dimension.

**Depends.** INS-1.

### INS-3 — Unicast subjects and the role enum

**Goal.** Every instanced plane also serves `{plane}.{instance}`, and a role
decides which subjects each process serves at all. Nothing addresses an
instance yet, so nothing changes observably. `/td/sessions` stops being an RPC.

**Scope.**

- `Topics.td(instance)` / `sts(instance)` / `md(instance)`. Not `sym`, not
  `paper` (*Which planes are instanced*).
- `Role` — `standby` / `named` / `active` — from `MFTIK_ROLE`, defaulting to
  `active`, gating which serve loops each `app.py` builds and whether
  `reap_loop` runs. `standby` is refused at boot on TD and STS.
- `run_rpc` takes the subject it serves; one task per subject the role grants,
  rather than one loop over several, so a failure in one does not stop the
  other.
- `routes/td.py` runs its own query; `TD_SESSION_LIST` and
  `handle_session_list` go.

**Problem.** This is the addressing every later ticket uses, and it is the one
that must be observably inert — every instance defaults to `active` and keeps
serving the pool it always did.

**Solution.** Add the named subjects beside the shared ones. Take nothing away.

**Verify.**

- An MD serving `active` answers on `md` and on `md.md-jp-1`; the same MD as
  `named` answers only the second, and the anycast request is still **in its
  list** — waiting for a peer rather than lost, which is what makes a named
  instance safe to run beside an active one.
- `standby` builds no serve loop at all, answers neither subject, and runs no
  reaper.
- `MFTIK_ROLE=standby` on TD or STS is a boot failure naming the plane.
- `/td/sessions` returns its rows from an app with no `state.broker` — the
  assertion that the RPC is gone rather than merely unused.
- `test_session_create.py`'s attach case runs against `td.{instance}`.

**Depends.** INS-1. Independent of INS-2.

**Two things this ticket does not do**, both moved after they were found to
depend on work that comes later. The error was the same each time: a deletion
or a rename was put in the ticket that *adds* the capability, when it actually
depends on the ticket that *moves the callers*.

- **Deleting `Topics.TD` and TD's anycast loop moves to INS-5.** Three callers
  still send attach and detach to the bare subject — `deploy_strategy`, STS's
  rebuild attach, and `StsSession`'s detach — and none can address an instance
  until `apis.instance_id` is resolved, which is INS-5's own scope. Deleting
  the subject here would leave attach with nowhere to go. So TD keeps
  `active` through this ticket and becomes `named` in INS-5, when its callers
  move in the same change.
- **`log.md.{venue}` → `log.md.{instance}.{venue}` moves to INS-7.** The links
  into that page are built from a session's venue list (`strategy/+page.svelte`
  and `strategy/[sessionId]`), which is derived from `md_ids` — and `md_ids`
  does not record which instance holds a feed until INS-7. Renaming the channel
  here would mean a UI that cannot be navigated correctly until then, and the
  collision it fixes is not live until two MDs serve one venue, which is also
  INS-7.

### INS-4 — The three MD defects (closes PI-4, PI-6)

**Goal.** Two MDs can hold one session without corrupting each other's rows.

**Scope.**

- `_ALIVE_DOMAIN` becomes `md:{instance}`.
- `MdSessionRepository.mark_done_session` takes the instance in its predicate,
  and `get_live` / `create_live` / `attach_live` / `mark_done` key on the
  triple the INS-1 constraint already declares.
- `persist_live_session` records the instance.
- **A fourth, found while building the third.** The reap scan was
  instance-blind in its own right: it lists every live row and, for one it does
  not hold a link for, marks it done. With two MDs that closes a healthy
  peer's rows on every scan. Filtering the scan to this instance is the wrong
  fix — it would leave the rows of an MD that died outright live forever, with
  no process anywhere in a position to notice, which is the recovery the
  reaper exists for. The scan stays global and decides each row against *its
  own* instance's key.

**Problem.** All three are silent today and become live the moment INS-7 can
split a session. Landing them after INS-7 means a window where a detach on one
MD tears down another's healthy link.

**Solution.** Fix before the thing that exposes them, not after.

**Verify.**

- Two MDs attached to one session; one detaches; the other's link survives, its
  rows stay `live`, and the liveness key it lives behind is untouched — PI-4.
- A reap scan does not close a session **only the peer holds** — PI-6. The
  shape matters: two instances on the *same* session proves nothing, because
  this instance's own key exists for that session too and the wrong key still
  answers "alive". The bug only shows when the peer holds a session this one
  does not.
- An instance that died outright is still reaped by a peer, so the global scan
  keeps the recovery it exists for.
- Each fix is checked by regressing it and watching the right test fail. A
  test that passes before and after is not evidence.

**Depends.** INS-1. Independent of INS-2 and INS-3.

### INS-5 — TD routing and the `api_id` claim (closes PI-7)

**Goal.** A credential is only ever used from the instance it names, and only
one process holds it. **This ticket closes the compliance requirement.**

**Scope.**

- Attach resolves `apis.instance_id` → `td.{instance}`.
- `Topics.td_backfill(instance)`; `request_backfill` and `backfill_cron` resolve
  the account's instance before posting.
- `SessionManager.attach` takes a `SET NX` claim on `api_id` before building
  the `TradingAccount`, renews it while held, releases at refcount zero, and
  refuses naming the holder.
- With the callers moved, `Topics.TD` and TD's anycast serve loop go, and TD
  drops to `named`. INS-3 could not do this: attach had nowhere else to go
  until an `api_id` resolved to an instance, which is the line above.

**Problem.** Backfill loads the credential and `backfill_cron` sweeps every
account on a timer, so an unkeyed subject fails compliance on a schedule. And
nothing stops two same-named processes owning one `api_id`.

**Solution.** Route by the row; enforce the claim rather than assert it.

**Verify.**

- `backfill_cron` posts each account to its own instance's queue, and a
  JP-only credential never reaches the US queue — the requirement stated as
  the sentence it is in.
- An account whose `apis` row is gone is **skipped** rather than swept onto a
  shared subject. New behaviour, and the reason the subject is keyed at all.
- A deploy resolves each credential to its own instance, and refuses
  `unknown_api` rather than falling back to a subject any TD could take.
- Two managers, same `api_id`: the second `attach` raises, names the holder,
  and opens no venue session; the first keeps the account. The refusal's text
  points at `MFTIK_INSTANCE`, because that is the configuration that causes
  it.
- Closing the first manager lets the second take the account without waiting
  out a TTL — a redeploy that had to would be an outage nobody caused.
- The claim primitives on their own: a refresh does not re-create a lapsed
  claim, and does not write its token back over a rival that has taken over.
- Each of those is checked by removing the claim and watching the right tests
  fail.
- `test_cid_ownership.py` and `test_session_oms.py` unchanged — the claim must
  not alter single-owner behaviour.

**Depends.** INS-1, INS-3.

### INS-6 — The STS market-data watchdog (closes PI-8)

**Goal.** A session notices an MD that stopped acknowledging, instead of
running on whatever still arrives.

**Scope.**

- STS tracks last-ACK per attached MD instance, mirroring MD's `_watch_timeout`
  against `LEASE_GRACE_S`.
- Grace exceeded → `_fail_from_infrastructure("md feed")`, the path that exists
  and is today only reached by a pump exception.
- `_md_ack_token` becomes read rather than written-only.

**Problem.** `_md_ack_token` is written in four places and read in none, so a
dead MD is invisible to STS. After INS-7 that becomes a session trading on half
a picture.

**Solution.** Land it before the fan-out; it is worth having with one MD.

**Verify.**

- ACKs stop; the session fails within the grace, and the reason **names the
  instance** that went quiet.
- One instance goes quiet while another keeps acknowledging: the session still
  fails, and the reason names only the one at fault. A watchdog on a single
  timestamp passes every other test here and fails this one — which is the
  point of keying per instance.
- A session that has heard no MD at all is **not** failed. Every deploy passes
  through that window: a session heartbeats before MD has attached to hear it,
  so the watchdog arms on the first acknowledgement rather than at start.
- A live feed never trips it.
- An MD that does not name itself is still watched, under the plane name — a
  rolling upgrade has one on each side of the new field.
- Checked by regressing the watchdog *and* by regressing the per-instance
  keying, separately.

**Depends.** INS-1. Independent of INS-3, INS-4 and INS-5.

### INS-7 — MD routing (closes PI-1, PI-3)

**Goal.** `strategy.yml` can name which MD serves which feeds, and a deploy
that names a missing one fails before anything is attached.

**Scope.**

- `StrategySpec.md` accepts a mapping; a list still means "any MD" (PI-5). The
  duplicate-key and merge-key guards `_refuse_collapsing_td_keys` applies to
  `td:` extended to `md:`.
- `deploy_strategy` resolves names against the table, then probes, then checks
  the venue; fans out one attach per instance; unwinds partial attaches before
  failing STS.
- `md_ids` read through a compat shim, in `_rebuild` too.
- `MdSubscribe` carries the instance so only the holder acts on it.
- The MD log line names its writer. **Not** the channel split this ticket
  originally called for: `log.md.{venue}` stays, and `Log` grows an
  `instance`. The stated problem — two MDs on one venue writing one channel
  that cannot tell them apart — is solved either way, but a channel per
  instance has nowhere honest to put an *unpinned* attach's lines, because not
  naming an instance is exactly what unpinned means. Null is a real answer
  there; `log.md.*.Bybit` is not. It also keeps the UI's links working, which
  is what made this wait for INS-7 in the first place.

**Problem.** The largest change and the one with a rollback that did not
previously have to unwind anything.

**Solution.** Last, on top of INS-4's fixes.

**Verify.**

- A `md:` mapping across two instances attaches each to its own feeds (PI-3);
  a plain list, and a mapping under `*`, both use the shared pool (PI-5).
- An undeclared name, a disabled one and a declared-but-silent one fail with
  **three different codes**, before any attach (PI-2). Collapsing the first
  and third into one message tells the operator neither: one is fixed in the
  document, the other by deploying something.
- A failure on the second of two attaches unwinds the first before failing the
  session.
- A duplicate key or a merge key under `md:` is a parse error, not a silently
  folded map. Worse here than under `td:`: a folded key loses a whole
  instance's feed list, and the deploy that follows attaches fewer feeds than
  the document asks for and says nothing.
- The same feed named by two instances is a parse error — each would open a
  pump and fan it out, and refcounting cannot notice because each instance
  counts its own.
- A row written before this ticket still rebuilds. Already covered by
  `test_an_interrupted_session_comes_back`, which seeds exactly that flat list
  — the shim is what keeps it passing. The new test is the other half: a
  *pinned* row rebuilds on the instance it names and never asks the pool.
- Checked by regressing the refusal split and the unwind separately.

**Depends.** INS-1, INS-3, INS-4, INS-6.

### INS-8 — STS selection and rebuild placement

**Goal.** A run pinned to `sts-tw` comes back on `sts-tw`, or does not come
back.

**Scope.**

- `POST /sts/deploy/{type}` takes an optional instance, in the body rather
  than the document: the same `strategy.yml` should be deployable to `sts-tw`
  and to `sts-jp` without editing it. The create goes to that instance's
  subject, and the same two PI-2 checks run first.
- `sts_sessions.instance` records **what the deploy asked for**, not where the
  session landed. Recording the latter would make an unpinned deploy pinned
  the moment it ran, and retiring that instance would strand a session nobody
  ever asked to put there.
- The rebuild scan filters on the instance — own name, or null for legacy and
  unpinned rows — before `claim_alive`.

**Problem.** The scan filters on `restart`, on the strategy building, on
`rebuildable` and on `claim_alive`, never on placement, and placement was not
recorded at all. Two STS booting race for every interrupted row.

**Solution.** Record it and filter on it. A session pinned to an instance that
no longer exists stays `INTERRUPTED` and waits for a person — the node checks
and does not guarantee.

**Verify.**

- Two STS with different names scan one interrupted row pinned to the first:
  only the first rebuilds. `claim_alive` already made the race *safe*; this is
  what makes it deterministic.
- A row pinned to a name nobody runs is rebuilt by nobody and stays
  `INTERRUPTED`, waiting for a person rather than moving itself.
- A row with a null instance is still rebuilt by whoever claims it.
- A deploy that names an STS creates on that instance's subject and records
  the name; an unpinned one uses the pool and records null.
- Naming an instance of the wrong *domain* is refused — `md-jp-1` is declared
  and answering, and is still not an STS.
- Checked by regressing the filter and watching the two placement tests fail.

**Depends.** INS-1. Independent of everything else.

### Order

```
INS-1  table + migration + MFTIK_INSTANCE      [unblocks everything]
  ├── INS-2  probes + Home + the expiring health subject
  ├── INS-3  unicast subjects, roles, TD anycast deleted
  │     └── INS-5  TD routing + api_id claim      [closes compliance]
  ├── INS-4  the three MD defects
  ├── INS-6  STS market-data watchdog
  ├── INS-8  STS selection + rebuild placement
  └── INS-7  MD routing                          [needs 3, 4, 6]
```

INS-5 alone closes the compliance requirement, and only INS-1 and INS-3 stand
in front of it. INS-7 is the only ticket with more than one parent, and it is
last for that reason rather than by size.

## Docs that stay right

`docs/MdHandover.md` needs two edits, and both belong to INS-3 rather than to
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

Its open question 2 — "What is STS's actual grace for a missing `MdLeaseAck`,
and does it bound the cutover comfortably? Measure before stage 5" — has an
answer that needs no measuring: **there is no grace, because STS never reads
the acknowledgements** (*The hard parts, 8*). The cutover is unbounded by STS
today, and becomes bounded by whatever watchdog PI-8 installs — so that
question should be rewritten to depend on PI-8 rather than on a measurement.

Its *What is already true* entry — "MD instances are already competing
consumers" — stays right either way.

`docs/MdVenueSubscriptions.md` needs no edit. Its ledger is per socket in one
process and stays that way; *The hard parts, 3* above is a new fact about what
happens between processes, not a correction to anything it claims.

`README.md` needs no edit. Its plane table says a node owns one job per plane,
not one process per plane, and that stays true.

`docs/StrategyEnvironment.md` and `docs/CLI.md` mention `strategy.yml` but
neither documents the `md:` shape. INS-7 owns re-checking that, since it is
the ticket that would make such a sentence false.

## Reading a log that spans two instances

An event log is written by whichever STS ran the session, so on a node with
several it does not live in one place — and one session's can genuinely span
two of them, because a rebuild elsewhere leaves the earlier parts on the volume
of the process that died.

That is why it is **not** addressed the way stop and fail are.
`Topics.sts_control(session_id)` works for those because they need the process
*holding* the session; a finished session has no holder, and its log is still
worth reading. The disk outlives the holder.

So the API asks **every declared STS** and merges. `StsEventLogPart` names the
instance it lives on, the listing is sorted oldest-first by modification time
across instances, and each read is addressed to the instance the listing said
has that part — file names collide, since every process writes the same
`{session}.jsonl`, so the name alone cannot say where a part is.

An instance that does not answer contributes nothing rather than failing the
request: the log may be entirely on one that did. Nobody answering is a 502,
because "we could not ask" and "there is no log" are different answers.

The ordering rests on the hosts' clocks agreeing to within the gap between a
session stopping on one and resuming on another — seconds at worst. The
alternative, reporting one instance's parts as the whole log, is wrong every
time rather than under skew.

## Open questions

1. Should `active` really be the MD and STS default? TD has been settled — it
   is `named`, because its routing key always resolves — but MD and STS still
   need the anycast pool for PI-5, and `active` means a misconfigured instance
   silently answers work meant for a peer. The alternative is `named` by
   default with an explicit opt-in, which is safer and breaks every existing
   deployment on upgrade.
2. How often does Home probe, and does it probe on view or on a timer? Every
   probe to a down instance is a write to a queue that expires rather than
   drains, so the refresh interval is a cost as well as a freshness knob.
3. What does an MD instance do with a feed for a venue it cannot reach — refuse
   at attach, or refuse at deploy from the health reply's `venues`? Refusing at
   deploy is a better message and a staler fact.
4. Does `api_ids` belong in the health reply at all? `td_sessions` is already
   the source of truth for it, and a reply that has to assemble it makes the
   probe do real work rather than answer instantly.
5. ~~What must be true before an instance row can be deleted?~~ **Settled: a
   live session naming it blocks, and the delete is refused with a 409.** The
   foreign key on `apis.instance_id` already blocks, so warning here would make
   one kind of reference refuse and another shrug — and the operator has an
   obvious way forward either way. Only the instance's *own* plane is counted:
   an `md_sessions` row naming `sts-tw` describes some MD that happened to be
   called that. History does not block, which is the point of those columns
   being plain strings.
