# Alert — live logs to a Discord webhook

A log line already fans out. STS, TD, and MD publish through
`publish_sts_log` / `publish_td_log` / `publish_md_log`
(`packages/common/src/mftik/protocol/session_log.py`) onto
`log.sts.{session_id}`, `log.td.{api_id}`, `log.md.{venue}`
(`packages/common/src/mftik/protocol/topics.py`). The UI tails those
channels over WebSocket. `run_log_persist`
(`apps/api/src/mftik_api/log_persist.py`) batch-inserts the same
lines into `session_logs`. Discord is how the Owner signs in
(`docs/Auth.md`). None of that turns a `warn` line, or a captured
number, into a webhook that still fires after the next deploy of the
same strategy kind.

This change keeps matching at **the API**, not in the trading
processes. The Owner wires a layered graph: which logs, how they are
judged, which Discord webhook they inject. STS is keyed by
`sts_sessions.type` — the qualified registry key, the kind id —
so a session that ends does not take the Alert with it. A new deploy
of `private::Tiny` walks into the same Source. The webhook fires on a
window, not once per line.

ALT-1 through ALT-8 shipped in 0.3.0. ALT-9 — the same graph from a
terminal — is the open one. This stays the design record rather than
becoming a changelog: what the tickets say is what was built, and
where a ticket and the tree disagree the tree is the bug.

## Epic

**Owner-managed layered graph from live domain logs to Discord
webhooks, keyed for STS by `type`, matched in the API process, fired
on a window.**

### The problem

An operator who wants "tell me when any CrossArb run logs `error`"
has to sit on `/ws/sts/{session_id}` and already know the id. That
id is minted in `deploy_strategy` as `uuid4().hex`
(`apps/api/src/mftik_api/orchestrate.py`) and is the primary key of
one row in `sts_sessions`. Stop and deploy again and it is a
different topic. Binding an Alert to the session is how the Alert
dies when the strategy goes offline — the case this epic exists to
stop.

The kind is already on the row. `sts_sessions.type` is the qualified
registry key (`CrossArb`, `private::Tiny`, `node1::Tiny`), indexed,
and what `list_live_for_origin` prefix-matches when a registry
delete is refused (`packages/db/src/mftik_db/models/session.py`,
`packages/db/src/mftik_db/repositories/session.py`). The short
`strategy` column is `Strategy.name` and can collide across remotes.
There is no surrogate strategy id. There was a `strategies.id`
once; 0024 folded that table into the session and said the integer
cannot be reconstructed.

The log line does not carry `type`. `Log` is `level` + `message`
(`packages/common/src/mftik/protocol/messages.py`).
`StsSessionStatus` carries `strategy` (the short name) and not
`type`. A subscriber on `log.sts.{session_id}` that wants the kind
has to join `sts_sessions`. Packing `type` into `session_id` would
make the join unnecessary by turning the primary key into a smart
key. `session_id` is `String(64)` and `type` is `String(128)`; the
hex id is already 32 characters; `::` is a filename and URL hazard
(eventlog sanitizes `[^A-Za-z0-9._-]`).
`packages/common/src/mftik/strategy/client_order_id.py` already
says the class is recovered from the session row, not from the
packed id — `cid_slot` identifies the *session*, because two runs
of the same class would otherwise mint the same `client_order_id`
in the same millisecond. The venue forced that packing. A log
envelope is ours. The fact travels beside the id, not inside it.

Scanning `session_logs` would be late (`LOG_PERSIST_FLUSH_INTERVAL`
defaults to 2s) and would re-fire history the first time a regex
changed. Putting `httpx.post` in STS, TD, or MD would put a Discord
timeout on the path that places orders. The jsonl `EventLog` is an
audit of what a session saw and did
(`packages/common/src/mftik/strategy/eventlog.py`); matching it
would make an optional disk trail the control plane for alerts.

### The shape that stays

- Redis `log.{domain}.{stream_id}` is still the fan-out. Topics do
  not grow a fourth segment. `parse_log_topic` stays a split on
  `.` with maxsplit 2.
- `session_id` stays `uuid4().hex`. It is an opaque identity.
- `sts_sessions.type` stays the kind id. No surrogate, no hash, no
  second catalog table whose natural key is this string.
- `run_log_persist` stays its own worker. Matching is a third
  subscriber on the same pub/sub, not a hook inside the flush.
- STS `EventLog` stays opt-in audit on `STS_EVENTLOG_DIR`. It does
  not become an Alert input.
- One Owner. Settings stays identity and keys.

### Non-goals

- Owner-authored Python in a Matcher. The Signal example
  (extract a float, compare to 0.99) is `extract`, not `eval`.
  `docs/StrategyEnvironment.md` just refused un-gated execution in
  STS; Alert Python would run in the API process next to
  `apis.api_secret` and the webhook URL.
- Matcher → Matcher edges. The graph is three layers. A compound
  condition is one Matcher. Two join tables make a
  matcher → matcher row unrepresentable.
- Encoding `type` into `session_id`. The cid analogy does not hold:
  we control the envelope.
- Scanning or backfilling `session_logs`. Live `log.*` only.
- Webhook delivery from STS, TD, or MD.
- `status.sts` as a Source. `failed` / `interrupted` without
  scraping log text is a later epic; the seam is named in Model
  and no ticket builds it.
- A second sink (Telegram, Slack). The column is `kind` so that
  is a rename of a value, not of the entity.
- Per-user ACL. The node is single-tenant (`README.md`).
- A freeform graph canvas. The UI is three columns.
- A named deploy / strategy instance id that survives a yaml
  edit. Kind is `type`. Two live `CrossArb` sessions with
  different yaml share a Source.

### Invariants

1. **The kind id is `type`, stored as the string.** An STS Source
   selector is a qualified key or `*`. It is not `session_id` and
   not `strategy`. TD is `api_id`, MD is `venue`. Those are already
   the `stream_id`s on `log.td.*` and `log.md.*`.
2. **Session id stays opaque; type travels beside it.** STS log
   lines and `StsSessionStatus` snapshots carry `type` as an
   optional field. Old lines without it fall back to
   `sts_sessions`. The primary key does not parse.
3. **Matching is live `log.*` only.** The worker does not replay
   `Broker.fetch_log_buffer` (100 lines, for late WebSockets). It
   does not walk `session_logs`. An API restart accepts a brief
   gap, the same way persist does. Changing a regex does not
   Discord the last week.
4. **The graph is three layers.** Two join tables express the
   only legal wires: `alert_source_matcher` and
   `alert_matcher_alert`. There is no row shape for
   matcher → matcher or source → alert. Foreign keys and
   `ON DELETE CASCADE` are what make that true. The test
   harness already runs `PRAGMA foreign_keys=ON`
   (`packages/db/tests/db_harness.py`); the runtime engine
   does not — `packages/db/src/mftik_db/session.py` is a
   bare `create_async_engine` — so on a SQLite-backed node
   the cascades would silently not fire. ALT-2 adds the
   connect listener there. Postgres needs nothing.
5. **Fire budget is per Alert, and it is one POST per
   quiesce window.** Two Matchers that inject the same
   webhook share one buffer and one timer. There is no
   `max_fires_per_interval`: under quiesce a window ends
   in exactly one flush, so a cap of 1 could never bind.
   Discord 429 is a delivery row, not a retry loop.
6. **The webhook URL never appears in audit `result`, process
   logs, or a command line.** GET returns a mask.
   Create/update/delete audit the Alert id and name. Same class
   of secret as `apis.api_secret`, which `ApiOut` already refuses
   to echo. The third place is the CLI's: an argument is written
   to shell history and is readable in `ps` while the process
   runs, so there is no `--webhook-url` flag to type it into
   (ALT-9).
7. **Matcher search never runs on the API event loop.**
   `run_log_persist` and `run_alert_match` are asyncio tasks
   in one thread. `re.search` plus a deadline is not a thing
   — once the C loop starts, nothing preempts it, and a
   catastrophic backtrack freezes persist, the three
   WebSocket bridges, and every HTTP request. The engine is
   the `regex` package, searched with `timeout=` and
   `concurrent=True`, on one dedicated matcher thread
   (`ThreadPoolExecutor(max_workers=1)`), one dispatch per
   line. `concurrent=True` is the load-bearing half: the
   package holds the GIL during matching unless it is
   passed, and a thread hop that keeps the GIL frees
   nothing. A timeout or exception is not a match; the line
   is dropped. TD and STS do not import the matcher.
8. **A type-bound Source outlives a session.** Current and future
   live sessions of that `type` match. A finished session goes
   idle — no more lines — and does not delete the Source. A
   registry rename (`node1::Tiny` → `node2::Tiny`) is a new kind
   id; the old selector does not follow it.

### Current → target

```
today
  STS/TD/MD ──publish_log──► log.{domain}.{stream_id}
                                ├── WS tail
                                └── run_log_persist ──► session_logs
  StsSessionStatus.strategy = short name
  Log = level + message
  Discord = login only

target
  STS/TD/MD ──publish_log──► log.{domain}.{stream_id}
                                ├── WS tail
                                ├── run_log_persist ──► session_logs
                                └── run_alert_match
                                      │
                                      ├─ resolve Source (td/md stream_id;
                                      │   sts payload.type or session cache)
                                      ├─ run connected Matchers
                                      └─ inject Alert buffer
                                           └── quiesce window ──► Discord
  StsSessionStatus.type = qualified key
  Log.type = same, optional, STS only
```

---

## Model

### Three layers, not a general graph

```
Source          Matcher              Alert
td:12           level warn|error ──► #ops     window 30s
md:Gate         extract risk>0.99 ┬─► #signals
sts:CrossArb                      │
sts:*                             └──► #ops
```

A Source is a subscription. A Matcher is a judgement. An Alert is
a Discord webhook plus the fire policy. The Owner may send many
Sources into one Matcher, give each Source its own Matcher, and
point those Matchers at the same or different Alerts. That is the
DAG they asked for. It is layered: there is no path that is not
Source → Matcher → Alert.

The Alert *is* the webhook. v1 `kind` is `discord_webhook`. The
column exists so a later sink is a new value.

### Kind id is `type`

`qualify` / `split_qualified`
(`packages/common/src/mftik/registry/qualify.py`) already define
the string:

| `type` | Meaning |
|---|---|
| `NoopStrategy`, `CrossArb` | Bundled, no origin prefix |
| `private::Tiny`, `public::HelloStrategy` | This node's trees |
| `node1::Tiny` | A pulled copy; the remote's name leads |

Two remotes shipping the same class are two kinds. That is why
delete-while-live matches `{origin}::` and not the short name.
Alert inherits the same key. Do not introduce `strategy_kind_id`.

`type` may be null on a session that failed before the document
was recorded (the column says so). Those runs do not match a
type-bound Source. A wildcard `sts:*` still can, if the line
arrives.

### Session id stays opaque

`deploy_strategy` mints `uuid4().hex` and then passes
`strategy_type` into `StsCreateSessionRequest`. Both facts are
known at the same moment. The id stays 32 hex characters.
`String(64)` on `sts_sessions`, `td_sessions`, `md_sessions`,
`orders`, and `fills` does not change.

What changes is the *line*. `Log` grows an optional `type`.
`StsSessionStatus` grows `type`. `SessionInfo` grows `type` so
the sentence already on `StsSessionStatus` stays true: every
field on the status snapshot is also on the REST/RPC row the UI
loaded first. `StrategyOut` already has `type`; the list the
strategies page uses does not need a new column.

`Log.model_config` is `extra="allow"` today, so a kwarg would
already serialize. Making `type` a declared optional field is
what stops the worker fishing in extras, and what stops a
collision with `Envelope.type` (`"log"`) from being accidental.
On the wire:

```
envelope.type     = "log"
envelope.payload.type = "private::Tiny"   # optional, STS only
```

TD and MD lines leave `payload.type` unset. Their kind is the
stream.

### Sources

`alert_sources`

| Column | Meaning |
|---|---|
| `id` | Surrogate |
| `created_by` | Owner id |
| `domain` | `sts` \| `td` \| `md` |
| `selector` | See below |

Selector grammar:

| Domain | Selector | Matches |
|---|---|---|
| `sts` | `private::Tiny` | Every live (and future live) session whose `type` equals that string |
| `sts` | `*` | Every STS line |
| `td` | `12` | `log.td.12` |
| `td` | `*` | Every TD line |
| `md` | `Gate` | `log.md.Gate` |
| `md` | `*` | Every MD line |

`(domain, selector)` is unique. Two rows that say the same thing
are a mistake, not a feature. The repository refuses a selector
that `split_qualified` would reject when `domain='sts'` and the
value is not `*` and contains `::` more than once — the same
rule `list_live_for_origin` already uses.

A 32-character hex `session_id` has no `::`, so it is a
legal selector — indistinguishable from a bundled type like
`CrossArb`. It is stored, and it never matches, because
resolution compares against `payload.type` / `sts_sessions.type`
and there is no code path that reads
`sts_sessions.session_id` as a Source. Requiring 422 here
would mean validating against the catalog, which forbids
pre-wiring a type from a remote this node has not pulled.
The UI picker does not offer session ids; a hex selector
can only arrive through the API.

The STS picker is not the session table. It is
`GET /sts/types` (`apps/api/src/mftik_api/routes/sts.py`) union
the distinct `type` values on currently live `sts_sessions`.
A badge may say how many are live. The value stored is the
type string.

### Matchers

`alert_matchers`

| Column | Meaning |
|---|---|
| `id` | Surrogate |
| `created_by` | Owner id |
| `name` | Owner label, unique |
| `kind` | `level` \| `regex` \| `extract` |
| `spec` | JSON, shape depends on `kind` |

```
kind: level
  levels: ["warn", "error"]

kind: regex
  pattern: "risk value"
  # `regex` package, compiled at write, searched on the message

kind: extract
  pattern: 'risk value = \{\%f\}", ([\d.]+)'
  group: 1
  as: float          # float | int | str
  op: ">"            # > >= < <= == !=
  value: 0.99
```

A match injects an *event*, not a bool: `domain`, `stream_id`,
`session_id`, `type` (STS), `source`, `level`, `message`,
`envelope_id`, `ts`, and `captures` (extract only). The Alert
template can show `0.995`. The Warn/Error case is `level` and
does not need a regex; `Log.level` is already on the payload.

Write-time: compile with `regex`, refuse a bad one with 422,
cap length (512). Run-time: the compiled pattern's own
`search(message, timeout=0.01, concurrent=True)`, on the one
dedicated matcher thread, one dispatch per line. Not the
module-level `regex.search(pattern, ...)` — that goes through
the library's own cache and throws away the compile this epic
just paid for. A timeout is not a match and is logged. Stdlib
`re` is not used for Matcher patterns — it cannot abort.

### Alerts

`alerts`

| Column | Meaning | Default |
|---|---|---|
| `id` | Surrogate | |
| `created_by` | Owner id, same as `sts_sessions.created_by` | |
| `name` | Owner label, unique | |
| `kind` | `discord_webhook` | |
| `webhook_url` | The secret | |
| `enabled` | | true |
| `flush_interval_s` | Quiesce window from the first buffered event | 30 |
| `max_events_in_payload` | Lines in the embed; rest is `+N more` | 15 |
| `max_buffer_events` | Hard cap on the in-memory list; further injects increment `dropped_count` | 200 |
| `dedupe` | After folding by `envelope_id`, same `message` in the window counts as one | true |

Quiesce, not clock-aligned: the first inject starts the timer,
the window flushes, the timer resets. An error at 12:00:01 does
not wait for 12:01. Clock alignment is not a column in this epic.
A window ends in one POST. Fold by `envelope_id` first (one
line, two Matchers, one Alert → one event), then message
dedupe if set. At expiry: fold, render `+N more` if the
folded set is larger than `max_events_in_payload`, POST,
write a delivery, **clear**. There are no leftovers waiting
for a later window — a next window is only armed by a new
inject, so held events would strand.

Message dedupe is for identical repeats (`warn: disconnected`
two hundred times). A flapping Signal that logs
`risk value = 0.995` then `0.996` is two events; that is
intended. The count is honest. `max_buffer_events` and
`+N more` are what save the process and the embed.

`immediate` + cooldown — the mode a `status.sts` Source would
want — is not a column in this epic either.

The URL is stored like `apis.api_secret`: a `Text` column, never
on a GET body. The response carries `webhook_masked`, something
like `https://discord.com/api/webhooks/…/***`. A later rotation
is PATCH with a new URL; GET never confirms the old one.

### Edges

Two tables, not a polymorphic `alert_edges`. A
`(from_kind, from_id)` pair cannot carry a foreign key, so
"delete cascades the edges" would be application code on both
dialects and an orphan would be a forgotten path. Listing the
graph is two queries. That is cheaper than a CHECK and a
SQLite fork — and SQLite enforces CHECK natively anyway; it
is foreign keys that need the pragma, which the harness sets
and ALT-2 adds to the runtime engine.

`alert_source_matcher`

| Column | Meaning |
|---|---|
| `source_id` | FK → `alert_sources.id` `ON DELETE CASCADE` |
| `matcher_id` | FK → `alert_matchers.id` `ON DELETE CASCADE` |

Primary key `(source_id, matcher_id)`.

`alert_matcher_alert`

| Column | Meaning |
|---|---|
| `matcher_id` | FK → `alert_matchers.id` `ON DELETE CASCADE` |
| `alert_id` | FK → `alerts.id` `ON DELETE CASCADE` |

Primary key `(matcher_id, alert_id)`.

There is no table that can express matcher → matcher. A
Matcher with no Sources is idle and legal — the Owner may
wire it next.

Do not store the graph as a JSON blob on one row. The hot
path is "this line, which Matchers?" and that is an index
on `(domain, selector)` plus a walk of the two join tables.

### Deliveries

`alert_deliveries`

| Column | Meaning |
|---|---|
| `id` | Surrogate |
| `alert_id` | FK → `alerts.id` `ON DELETE CASCADE` |
| `window_start` | When the quiesce timer started |
| `event_count` | How many events this fire folded (after `envelope_id`, then message dedupe) |
| `dropped_count` | Injects discarded because `max_buffer_events` was already full |
| `http_status` | Discord's response, or null if the POST never left |
| `error` | Short failure, never the URL |
| `ts` | |

This is how the UI answers "why did #ops not speak". It is not
the event log; it is the fire log. Deleting an Alert deletes
its deliveries — the record of why #ops went quiet goes with
the webhook. That is intended; export first if the history
matters. Cap retention in a later ticket if the table grows
— not here. The UI can still answer "why did it not speak
just now" while the Alert exists.

### Live match path

`run_alert_match` lives in the API process next to
`run_log_persist` and `run_backfill_cron`
(`apps/api/src/mftik_api/main.py` `lifespan`). It
`psubscribe`s `log.*`. Pub/sub is fan-out: persist still sees
every line.

```
line arrives
  parse_log_topic → (domain, stream_id)
  if domain == sts:
      type = payload.type or cache[stream_id]
      # cache miss: SELECT type FROM sts_sessions WHERE session_id=?
  sources = lookup(domain, stream_id or type) plus (domain, '*')
  candidates = successors(sources)
  hits = await evaluate(line, candidates)  # one hop, one thread; ALT-5
  for matcher in hits:
      for alert in successors(matcher):
          if envelope_id already in buffer[alert]:
              continue                   # one line, many Matchers, one event
          if len(buffer[alert]) >= max_buffer_events:
              dropped_count[alert] += 1
              continue
          buffer[alert].append(event)
          arm quiesce timer if first
```

One `evaluate` per line, not one per Matcher: every
regex/extract candidate for that line is judged inside the
single call. `level` candidates are a set lookup and stay on
the loop, so the common Warn/Error path never queues behind
a regex at all.

The cache is `(session_id → type | None)`. Load live rows at
start. A miss reads the row once and stores the result,
**including null**, with a TTL — a session whose `type` is
null (the column says those exist) must not SELECT on every
line. `status.sts` snapshots that carry `type` (ALT-1)
refresh it. A terminal status keeps the mapping for a short
TTL so a tail line after `done` still resolves. The cache is
a convenience for unstamped lines and for a worker that
started mid-session; new STS lines should not need it.
`Strategy.log` and the deploy lines in `orchestrate.py` are
why they will not.

Do not call `fetch_log_buffer` on start. Those hundred lines
exist so a browser that opens `/ws/sts/{id}` a second late
still sees deploy. Replaying them into Discord would re-fire
every restart.

### Auth

| Actor | Alerts |
|---|---|
| Session / API key | read and write |
| Registry key | none |
| Anonymous | none |

`/alerts` is not in `REGISTRY_READ_PATHS`
(`apps/api/src/mftik_api/auth/middleware.py`). A peer that can
read published strategies cannot mint a webhook on this node.
Deny is the default; adding the router is enough to put it
behind the Owner gate **when the gate is on**.

The table above describes the gated node. Auth is off by
default (`MFTIK_AUTH_ENABLED`, `auth/middleware.py`). On a
default node `/alerts` is reachable by anyone who can reach
the API, webhook writes included — the same as `/apis`
today. ALT-3's registry-key test turns the gate on.

Audit operations: `alert.create`, `alert.update`, `alert.delete`,
`alert.test`, and the same four for sources, matchers, and
edges as needed. `result` is `id=… name=…`, never the URL.

### Threat model

The webhook URL is a capability. Anyone who has it writes to
that Discord channel. It is stored in Postgres the way venue
`api_secret` is stored today — plaintext `Text`, not returned
— and that is accepted for the same reason, not because a
webhook is "just a URL". Process logs and audit `result` are
the places it would leak without anyone meaning to; those are
the places invariant 6 names.

A Matcher does not run on the event loop. `level` is a set
membership (cheap, in-process). `regex` / `extract` use the
`regex` package with `timeout=` and `concurrent=True`, on one
dedicated matcher thread. They do not `import`, they do not
open sockets, they do not see `webhook_url`. A backtrack is
aborted by `timeout=`; until that fires it occupies that one
thread, not persist, not the WebSocket bridges, not HTTP —
and `concurrent=True` is what makes that sentence true,
because without it the thread keeps the GIL and the hop buys
nothing.

One thread rather than a pool, and that is the stronger
choice, not the cheaper one. Matching is CPU-bound: four
threads do not finish four times sooner, they interleave
through the GIL and add latency to everything else in the
process — and if `concurrent=True` were ever dropped, four
GIL-holding threads would be worse for the loop than one. A
single-worker executor is also the cap the semaphore was
standing in for, expressed once. Stdlib `re` is not the
engine.

Owner-authored Python would be arbitrary code in the process
that holds every venue secret this node has. The Owner can
already `push` a tree STS will `import`. That is a different
process, behind `gate.py`. This epic does not add a third.

Discord itself rate-limits webhooks (on the order of tens per
minute, 2000 characters). A 30s quiesce window and a 15-line
embed are how the Owner does not discover that limit as a
429. The worker treats a 429 as a delivery row with that
status and does not busy-retry inside the same window. A
rolling cap independent of the quiesce window is a later
epic — `max_fires_per_interval` is not a column, because
under quiesce it could never bind.

Auth off is the default (see Auth). A webhook written on
an ungated node is as exposed as a venue `api_secret`
written the same way.

### Operations

- **Egress.** The API container must reach `discord.com`. An
  air-gapped node fails delivery; the delivery row says so.
  Matching still runs.
- **Restart.** In-flight buffers die with the process. The next
  live line starts a new window. That is the same honesty
  persist already has (an unflushed batch at a hard kill is
  gone unless the broker still holds it — and we will not
  replay the ring).
- **Reload.** Creating or editing a Source / Matcher / Alert
  must be visible to the worker without an API restart. The
  worker holds the graph in memory and **polls** on a short
  interval. Not a poke from the write path: a poke reaches
  only the process that handled the write, and the worker is
  a task, not the only writer this node will ever have.
  Compiled patterns are cached by pattern string, so a
  refresh recompiles what changed and nothing else. A line
  that races a write may use the old graph once; that is
  acceptable.
- **Backpressure.** Matching is serialized on one thread and
  the worker awaits each line, so a slow line delays the next
  by at most `timeout=` × the regex Matchers wired to it. If
  matching falls far enough behind, redis-py buffers and
  Redis eventually drops the subscriber over
  `client-output-buffer-limit pubsub`; the worker reconnects
  and matching has a gap — the gap invariant 3 already
  accepts, now reachable from a bad pattern and not only from
  a restart. A Matcher that always times out is the way to
  get there, which is why ALT-5 disables one that does.
- **Shutdown.** `lifespan` shuts the matcher executor down
  with `cancel_futures=True` and does not wait on it: a
  thread already inside a search cannot be cancelled, and
  `timeout=0.01` caps how long it could still be there.
- **Disk.** Six tables (three nodes, two joins, deliveries),
  low cardinality. Deliveries are the only ones that grow
  with time.

---

## Tickets

Each ticket leaves the tree shippable. Later tickets may be
empty behaviour (no graph, worker no-ops) so earlier ones can
merge.

### ALT-1 — Stamp `type` on STS log and status

**Scope.** `Log` and `StsSessionStatus` in
`packages/common/src/mftik/protocol/messages.py`;
`SessionInfo` in the same file (the status docstring's
"every field is also on SessionInfo");
`publish_sts_log` in
`packages/common/src/mftik/protocol/session_log.py`;
the `Session` object and `_publish_status` /
`publish_sts_log` call sites in
`apps/sts/src/mftik_sts/session/session.py` and
`apps/sts/src/mftik_sts/session/manager.py`;
`Strategy.log` in
`packages/common/src/mftik/strategy/base.py` (this is
how a strategy emits its own lines — the Signal example
comes from here, not from the session lifecycle);
`SessionView` in
`packages/common/src/mftik/strategy/session.py` — the
Protocol `Strategy.log` reads (`session_id`, `cid_slot`,
`broker`, `symbols`, `event_log`), and where `type`
belongs;
`deploy_strategy` in
`apps/api/src/mftik_api/orchestrate.py` (the deploy
lines, published from the API, which already has
`strategy_type`).
Tests: `apps/sts/tests/test_status_events.py`, existing
session-start log assertions, a new test that
`Strategy.log` and a deploy start line carry `type`, and
a test that `type=` in `**extra` cannot override the
session's type.
Optional kwargs so an old caller still works.

**Problem.** A subscriber on `log.sts.{session_id}` cannot
tell which kind produced the line. `StsSessionStatus` has
`strategy` (short name) and not `type`. Packing the kind
into `session_id` is refused in Non-goals; without a stamp
the match worker's only answer is a DB join on every line.

`Session` today keeps `strategy_name` and not `type`.
`persist_live_session` already writes `request.type`. The
in-memory object drops the fact the row just stored.

Scoping only `session.py` and `manager.py` would leave
the majority of matched lines unstamped.
`Strategy.log` (`base.py:490`) forwards `**extra` straight
into `publish_sts_log`. Deploy start / created / failed
lines in `orchestrate.py` fire before STS has spoken, from
a process that already knows `strategy_type`. Those two
are the path. Lifecycle lines are the minority.

`Log` is `extra="allow"`. Once `type` is a declared field
the worker trusts, `self.log("x", type="CrossArb")` from
any strategy routes into someone else's Source. Not a
security boundary (the Owner wrote the strategy) but a
silent mis-route.

`Strategy.log` reads `self.session`, which is the
`SessionView` Protocol — and the doubles that satisfy it are
structural, not `isinstance`-checked. `FakeSession` in
`apps/sts/tests/test_noop_strategy.py` has neither `broker`
nor `type`; the fakes in `test_rebuild.py`,
`test_sts_session.py` and `test_session_failed.py` are the
same shape. A bare `self.session.type` turns this ticket
into an `AttributeError` in tests that have nothing to do
with alerts.

**Solution.**

- `Log.type: str | None = None` — declared, optional, STS
  only. TD/MD publishers do not set it.
- `publish_sts_log(..., type: str | None = None)` is the
  only way `Log.type` is set. A `type` key in `**extra` is
  dropped (or refused); it never wins over the argument
  and it never supplies one when the argument is omitted.
- `Session` stores `type` from the create request (nullable,
  same as the column). Session-side `publish_sts_log`
  calls pass `type=self.type`.
- `SessionView` grows `type: str | None` beside
  `session_id` and `cid_slot`.
- `Strategy.log` passes
  `type=getattr(self.session, "type", None)` — `getattr`,
  not attribute access, because the doubles are structural
  — and does not forward a caller-supplied `type` in
  `**extra`.
- `deploy_strategy` passes `type=strategy_type` on every
  `publish_sts_log` it already makes (start, created,
  failed). `session_id` stays `uuid4().hex`.
- `StsSessionStatus.type: str | None = None`.
  `_publish_status` takes `type=` and fills it. Same for
  `SessionInfo`.
- Callers that do not know the type omit the kwarg. Lines
  published during a deploy that failed before `type` was
  recorded stay unset.

Do not add `type` to `session_logs`. Persist keeps
`level` / `message` / `stream_id`. Matching is not that
table's job.

**Verify.**

- A live session of `private::Tiny` publishes a start line
  whose payload `type` is `private::Tiny`.
- `Strategy.log("risk value = {%f}, 0.995")` on that
  session publishes `payload.type=private::Tiny`.
- `deploy start` / `STS created` lines from
  `orchestrate.py` carry `type=strategy_type` when it was
  passed in.
- `status.sts` snapshots for that session include
  `type=private::Tiny` on live and on failed.
- `SessionInfo` from a list call includes `type` when the
  row has one.
- `publish_sts_log(..., extra_type_via_kwargs)` — a
  `type=` inside `**extra` does not appear as
  `payload.type` unless the explicit argument set it, and
  cannot change an explicit argument.
- `self.log("x", type="CrossArb")` on a `private::Tiny`
  session still stamps `private::Tiny`.
- A strategy bound to a session object that has no `type`
  (the fakes under `apps/sts/tests`) still logs: payload
  `type` is null and nothing raises. The existing
  strategy tests pass unchanged.
- `publish_sts_log` without `type=` still publishes; payload
  `type` is null.
- TD/MD helpers do not grow a `type` argument.
- `test_the_lifecycle_is_announced_as_snapshots` still
  applies; new field is optional so old assertions on the
  known keys hold.
- `session_id` in `orchestrate.py` is still `uuid4().hex`.
  No test starts accepting a prefixed id.

### ALT-2 — Tables

**Scope.** Alembic 0026 (revises 0025), models under
`packages/db/src/mftik_db/models/alert.py`, repositories,
exports in `models/__init__.py` and
`repositories/__init__.py`; the SQLite connect listener in
`packages/db/src/mftik_db/session.py` (see invariant 4).
Tests in `packages/db/tests` on both dialects the suite
already runs.

**Problem.** The graph and the fire log have nowhere to
live. Putting them in a JSON column on one "pipeline" row
makes "which Matchers care about `log.td.12`" a
deserialization of everything.

**Solution.** Six tables as in Model: three nodes, two
join tables, deliveries. Integer PKs. `alerts.created_by`
(and `created_by` on sources and matchers) is the Owner
id as `ForeignKey("users.id", ondelete="CASCADE")` — the
same shape as `sts_sessions.created_by`; single-tenant
today, and the row says whose it is either way. Unique
`(domain, selector)`, unique matcher `name`, unique Alert
`name`. Join tables are composite PKs with real FKs and
`ON DELETE CASCADE` both ways. Deleting an Alert deletes
its deliveries; that is intended (see Deliveries).
`webhook_url` is `Text`, not unique (two Alerts may not
share a URL as a schema promise — they might, and the
fire budgets stay per row).

There is no `alert_edges` table and no CHECK that lists
legal `(from_kind, to_kind)` pairs. SQLite would have
enforced that CHECK; the reason for two tables is the
foreign key, not the CHECK.

Because the cascade is the enforcement, the runtime engine
has to honour it: `session.py` builds the engine with no
connect listener today, so a SQLite-backed node would drop
a Matcher and keep its join rows. Add the
`PRAGMA foreign_keys=ON` listener there, the same one
`db_harness.py` installs for tests.

Selector validation lives in the repository or a small
helper next to it, not in Alembic: `domain` in
`{sts,td,md}`, `selector` either `*` or a non-empty
string, STS non-wildcard with `::` must `split_qualified`.
A hex session id is accepted. It will never match.

**Verify.**

- Fresh migrate creates the six tables and no
  `alert_edges`.
- There is no schema that can insert matcher → matcher
  or source → alert.
- Two Sources with `(sts, private::Tiny)` violate unique.
- Delete a Matcher deletes its rows in both join tables
  (FK CASCADE, including under SQLite with the harness
  pragma) and does not delete the Alert.
- Delete an Alert deletes its deliveries and its
  `alert_matcher_alert` rows.
- `alerts.created_by` is not null and is a FK to `users`.
- An engine built by `session.py` against SQLite (not the
  harness engine) cascades a Matcher delete into both join
  tables — the listener, not just the test pragma.
- SQLite and Postgres tests both run. `spec` is JSON,
  same as `st_paras`.

### ALT-3 — CRUD, mask, audit, test fire

**Scope.** `apps/api/src/mftik_api/routes/alerts.py`,
schemas, `include_router` in `main.py` and
`routes/__init__.py`. Tests under
`apps/api/tests/test_alerts_route.py`. No worker yet; test
fire hits Discord (or a stub) directly.

**Problem.** The Owner has no way to put a webhook on the
node or to prove the URL works. Writing the worker first
would match against an empty graph forever.

**Solution.** REST on the three nodes and the two join
tables:

```
GET/POST          /alerts
GET/PATCH/DELETE  /alerts/{id}
POST              /alerts/{id}/test

GET/POST          /alerts/sources
DELETE            /alerts/sources/{id}

GET/POST          /alerts/matchers
PATCH/DELETE      /alerts/matchers/{id}

PUT/DELETE        /alerts/sources/{id}/matchers/{matcher_id}
PUT/DELETE        /alerts/matchers/{id}/alerts/{alert_id}
GET               /alerts/{id}/deliveries
```

No `/alerts/edges`. Wiring is the join row. `PUT` is
idempotent: drawing the same wire twice is 200, not 409.

GET Alert never returns `webhook_url`. Create and PATCH
accept it; the response is `webhook_masked`. PATCH without
the field leaves the stored URL alone — the same way a
venue credential is not re-sent to rename an account.

`POST /alerts/{id}/test` sends a fixed embed ("test fire
from this node", Alert name, no customer log text) and
writes a delivery row. The HTTP client is injectable so
the test suite does not need Discord.

`record_audit` on mutating calls. `result` contains ids
and names. A test asserts the raw `result` string does
not contain the URL that was POSTed.

Registry keys: a request with `via=key:…` of kind
`registry` is 403 on every `/alerts` path. The middleware
already assigns `DEFAULT_SCOPE` here; the test is that
this prefix was not added to `REGISTRY_READ_PATHS`.
**The test enables the gate** (`MFTIK_AUTH_ENABLED=1`).
Auth is off by default; without the flag the request
never meets a principal and the 403 cannot happen.

**Verify.**

- Create Alert → GET lists it with `webhook_masked`, not
  the URL.
- PATCH name only → URL unchanged, still masked.
- PATCH URL → new mask, test fire uses the new one
  (assert on the stub's requested URL).
- Test fire writes a delivery with `event_count=0`. A
  matched window always folds at least one event, so zero
  already is the marker — no extra column, no flag.
- Audit row for create: operation `alert.create`,
  `result` has name, not the webhook host path.
- `PUT` source→matcher 200; there is no route that
  wires a source to an Alert.
- Matcher create with a bad `regex` pattern 422,
  nothing stored. Compile uses the `regex` package,
  not stdlib `re`.
- Registry-key client, gate on, 403s GET `/alerts`.
- Registry-key client, gate off, is not this test.

### ALT-4 — Match worker, Source resolution

**Scope.** `apps/api/src/mftik_api/alert_match.py`,
started from `lifespan` beside persist. Tests in
`apps/api/tests/test_alert_match.py` with a fake broker
(same shape as `apps/api/tests/test_log_persist.py`).

**Problem.** Persist already `psubscribe`s `log.*`. A
second subscriber is how a match exception does not
run inside `flush_rows` — it is not how the event loop
stays free (see invariant 7). Without resolution, an
STS line is only a session id.

**Solution.** Subscribe `log.*`. Reuse `parse_log_topic`
and the envelope walk persist already does; do not import
`flush_rows`. Resolve Sources:

- `td` / `md`: `stream_id` equality or `*`.
- `sts`: `payload.type` if set, else the
  `session_id → type | None` cache, else a single-row
  read that **stores null too** (TTL), else no
  type-bound Source (wildcard still applies). A
  cached null is not a miss.

Judging a line is one `await evaluate(line, candidates)`
returning the Matchers that hit — ALT-5 owns the evaluator,
its thread, and its timeout; this ticket calls it once per
line and buffers what comes back. The loop is over `hits`,
not over candidates with a call inside.

Load the graph at start and poll as in Operations. This
ticket injects into an in-memory list per Alert and does
**not** POST Discord — ALT-6 owns fire. A test hook
(or the deliveries table left empty plus a
`pending_events` accessor) is how ALT-5/6 assert.

A Matcher that is not yet implemented (`kind` unknown to
this build) is skipped and logged, not a crash. ALT-5
fills the kinds; this ticket may treat every kind as
"accept all" *or* as "skip" — pick skip, so a regex
Matcher created in ALT-3 does not dump every line into
the buffer before ALT-5 ships.

Do not subscribe `status.sts` for matching. Cache refresh
from status is allowed (the field ALT-1 added); injecting
an Alert from a status snapshot is the later epic.

**Verify.**

- A TD line on `log.td.12` injects only Alerts wired from
  `td:12` or `td:*`.
- An STS line with `payload.type=private::Tiny` injects
  `sts:private::Tiny` and `sts:*`, not `sts:CrossArb`.
- An STS line with no payload type, session row
  `type=CrossArb`, injects `sts:CrossArb` via cache/DB.
- An STS line with no payload type, session row
  `type` null: one SELECT, then further lines of that
  session do not hit the database. Wildcard still
  applies; `sts:CrossArb` does not.
- Persist tests still pass; killing the match worker in a
  test does not prevent a persist flush.
- Worker start does not drain `fetch_log_buffer`.
- Unknown matcher kind: no inject, worker continues.

### ALT-5 — `level`, `regex`, `extract`

**Scope.** A small matcher module the worker calls
(`packages/common` or `apps/api` — API is enough; these
are not a protocol). Wired into ALT-4's skip branch.
Tests: compile refuse, timeout, each kind's true/false.

**Problem.** Warn/Error is a level set. The Signal
example is a capture and a comparison. Both can be
written as Python. Both must not be.

**Solution.** Three evaluators, one per `kind`. `level`
is membership after lowercasing (the persist path already
stringifies level) and stays on the event loop — it is
a set lookup. `regex` and `extract` use the `regex`
package (`regex` on PyPI, not in the lock today; declare
it in `apps/api/pyproject.toml`). `extract` is search,
group, coerce (`float` / `int` / `str`), compare. Failed
coerce or missing group is not a match. Pattern compile
on write stays in ALT-3; the worker compiles once when it
refreshes the graph, keyed by pattern string, not per line.

Search is the **compiled pattern's own**
`search(message, timeout=0.01, concurrent=True)`, run on a
module-level
`ThreadPoolExecutor(max_workers=1, thread_name_prefix="alert-match")`
— one dispatch per *line*, judging every regex/extract
candidate for that line inside the single call:

```python
hits = await loop.run_in_executor(_EXEC, _eval_all, message, candidates)
```

One thread, not a pool, and no semaphore: `max_workers=1`
plus awaiting each line means exactly one match call in
flight, which is what the semaphore was standing in for. It
also keeps lines in arrival order, so ALT-6's `envelope_id`
fold and `+N more` truncation are deterministic and its
tests reproducible. `asyncio.to_thread` is deliberately not
used — it submits to the process-wide default executor,
shared with any library that offloads, so a matching burst
would either starve them or be starved by them.

`concurrent=True` is required, not decoration: the `regex`
package holds the GIL through a match unless it is passed
(safe here — `str` is immutable, which is the precondition
the flag exists for). Without it the hop happens and the
loop still stalls, and the Verify assertion below (and S9)
would be testing something untrue.

A `TimeoutError` is not a match and is logged. A Matcher
that times out N times in a row is disabled in memory,
logged once, and shown in the UI: threads are not the
answer to a pattern that always times out — four of them
stall at 400 lines/s instead of 100, which is the same
stall. No column for it; the row stays clean and the next
graph poll re-enables it. The worker holds the disabled set
on its handle and `GET /alerts/matchers` reports it as
`disabled_reason` — the worker is a task in this same
process, so no table is needed to get the fact to the route.
Null before this ticket ships. Stdlib `re` is not imported in
this module. A deadline around `re.search` is not an
implementation — it cannot be built.

**Verify.**

- `levels: ["warn","error"]` matches `WARN` and `error`,
  not `info`.
- `regex: "risk value"` matches the Signal sample line.
- `extract` on
  `'"risk value = {%f}", 0.995'` with the stated pattern
  and `> 0.99` injects and `captures` contains `0.995`
  (or the float). `0.50` does not inject.
- `extract` with `as: float` and a non-numeric group
  does not inject and does not raise out of the worker.
- A pathological pattern is cut by `timeout=`. Assert the
  loop stayed live structurally, not on wall-clock: a
  Matcher with a long `timeout=`, a coroutine scheduled
  before the search, and the assertion that it ran while
  the search was still in the matcher thread. Dropping
  `concurrent=True` is what makes that assertion fail.
  The next line still evaluates.
- One line with three regex Matchers dispatches to the
  executor once, not three times (count the submissions).
- N consecutive timeouts disables that Matcher; the other
  Matchers on the same line keep evaluating; a graph poll
  re-enables it.
- The matcher module does not import `re` and does not
  call `asyncio.to_thread`. There is no code path that
  `eval`s `spec`.
- `apps/api/pyproject.toml` lists `regex`.

### ALT-6 — Windowed flush and Discord POST

**Scope.** The per-Alert buffer and timer inside the match
worker; an HTTP client for Discord's webhook JSON; writes
to `alert_deliveries`. Tests stub the client. Declare
`httpx` in `apps/api/pyproject.toml` — it reaches this
package only transitively through `mftik` today
(`packages/common/pyproject.toml`). A direct import
needs a direct pin.

**Problem.** Injecting one embed per line is how a flapping
`warn` spends the Discord budget and the Owner's attention.
The Owner asked for "received, then fire at the next
moment" — a window, not a trigger.

**Solution.** First inject for an enabled Alert starts
`flush_interval_s`. At expiry, fold by `envelope_id`,
then by `message` if `dedupe` is set, render an embed
(Alert name, `event_count`, `dropped_count`, window,
per-event level + type/venue + message + captures;
`+N more` when the folded set is larger than
`max_events_in_payload`), POST, write a delivery,
**clear**. One window, one POST. Disabled Alerts drop
injects and do not arm a timer. Injects past
`max_buffer_events` increment `dropped_count` and are
not stored.

There is no leftover-to-next-window path and no
`max_fires_per_interval`. A next window starts only
when a new inject arrives after clear.

Embed colour follows the worst level in the window
(error > warn > info). Truncate to stay under 2000
characters. Discord 429 / 5xx: delivery row with that
status, no tight retry loop.

Test fires (ALT-3) stay a direct POST and a delivery;
they do not go through the buffer.

**Verify.**

- Three matching lines inside 30s → one POST, delivery
  `event_count=3` (or 1 if they deduped).
- One line hitting two Matchers wired to the same
  Alert → `event_count=1` (`envelope_id` fold), even
  with `dedupe=false`.
- Same message three times with `dedupe=true` →
  `event_count=1`.
- `risk value = 0.995` then `0.996` with `dedupe=true`
  → `event_count=2` (intended).
- Sixteen distinct messages, `max_events_in_payload=15`
  → embed contains `+1 more`; buffer is cleared.
- `max_buffer_events=2`, five injects → delivery
  `dropped_count=3`.
- Disabled Alert: stub sees no POST.
- Stub raises / returns 429 → delivery has the status,
  worker still matches the next line.
- Stub is never called with a URL that equals the
  unmasked secret in a log assertion — the test looks
  at the request the stub received, not at `caplog`.

### ALT-7 — `/alerts` UI

**Scope.** `frontend/src/routes/alerts/+page.svelte`,
client helpers in `frontend/src/lib/api.ts`, a nav entry
in `frontend/src/routes/+layout.svelte`. STS picker uses
`GET /sts/types` plus live distinct types (the strategies
list already loads `type`). TD picker from the APIs /
accounts list the `/apis` page uses. MD picker from
`GET /venues` or `GET /sym/venues`. No Settings section.

**Problem.** Settings is the Owner's identities and keys.
A DAG does not belong there. Without a page the API is
only curl.

**Solution.** Three columns: Sources, Matchers, Alerts.
Drawing a wire is picking the other end, not dragging on
a canvas. Alert form: name, webhook URL (write-only;
masked once saved), window fields, enable, Test. Matcher
form: kind + spec fields that match the three shapes.
Source form: domain + a picker, or `*`.

STS options are catalog `type` values, not session ids.
A live count may sit next to the type. The session list
on `/strategy` is not reused as the picker — that list
is "this run".

A deliveries panel on the selected Alert. Empty graph is
an empty state, not a placeholder pipeline. A Matcher the
worker disabled for repeated timeouts renders from
`disabled_reason` on the matcher list (ALT-5) — a Matcher
that silently stopped judging is the one thing here an Owner
must not have to guess at.

Playwright: one smoke that `page.route`s `**/api/alerts*`
(and the picker endpoints it needs) and asserts the
render, the same way
`frontend/e2e/strategy-page.spec.ts` stubs
`authenticated: false`. `apps/api/tests/auth_harness.py`
is a Python API helper, not a browser one. This smoke
does not sign anyone in. Not a full graph editor spec.

**Verify.**

- Nav shows Alert; `/alerts` is the document title
  pattern the layout already uses (`sectionLabel`).
- Creating an Alert, a `level` Matcher, a `sts:*`
  Source, and the two join PUTs is possible without the
  network panel showing a raw webhook URL on GET.
- STS picker lists `NoopStrategy` (bundled) and does
  not list a session hex.
- Settings has no Alert section.
- The e2e spec fulfills `**/api/auth/status` with
  `authenticated: false` and never calls the live API.

### ALT-8 — Integration scenarios

**Scope.** Tests that name the routes and the worker
together, added as ALT-3–6 land, not against imagined
paths. See the list below.

**Problem.** Each ticket's Verify is local. The claim
that matters is: a live `private::Tiny` warn reaches
#ops after a redeploy under a new `session_id`, and a
regex edit does not Discord last Tuesday.

**Solution.** The scenarios in the next section. Write
them against the real handlers and a fake broker /
fake Discord, the way `test_log_persist.py` already
fakes `psubscribe`.

**Verify.** The scenarios exist and pass. Do not invent
a second HTTP shape in the tests.

### ALT-9 — `mftik alert`

**Scope.** `packages/common/src/mftik/cli/alert.py`, the
`Command` row and `_setup_alert` in
`packages/common/src/mftik/cli/app.py`, `put` / `patch` on
`Client` in `cli/client.py`. Tests in
`packages/common/tests/test_cli_alert.py`, against a mock
transport the way `test_cli_env.py` does.

**Problem.** ALT-7 gave the graph a page and nothing else, so
wiring a webhook on a headless node means curl and a hand-built
JSON body. The endpoints are already there; what is missing is
the surface that does not need a browser.

**Solution.** One command with verbs, named `alert` in the
singular. The split in this CLI is not singular against plural
but listing against namespace: `profiles` and `logs` are
terminal and take no verb, `env` is an entry point and is
singular. `source` and `matcher` nest under `alert` rather than
taking two more top-level names — a Source with nothing wired
to it is a subscription nobody reads.

```
mftik alert list | graph | add | rm | test | deliveries | types
mftik alert wire   --source <id> --matcher <id>
mftik alert wire   --matcher <id> --alert <id>
mftik alert unwire  … same grammar
mftik alert source  list | add | rm
mftik alert matcher list | add | rm
```

The webhook URL is prompted for with `getpass`, or piped in
with `--webhook-url-stdin`. There is no flag that takes it —
see invariant 6. The stdin path is an explicit flag rather than
an `isatty()` guess, because a cron job or CI runner has no TTY
*and* often nothing on stdin, and guessing there reads an empty
string and sends a 422 that has to be worked backwards from.

`wire` names an edge with two of three ids.
`--source X --alert Y` fails locally, without a request, and
prints the two commands that do what was meant. The schema
already makes that edge unrepresentable; this is so a person
does not learn it from a 404.

`--value` is coerced against `--as` before the body is built.
A spec whose value is the wrong type stores fine and dies at
match time, in the worker, on a line nobody is watching.
`matcher list` prints `disabled_reason` to stderr for the same
reason — a Matcher the worker disabled must not read like one
that is judging.

No `mftik alert edit`. `PATCH /alerts/{id}` exists and the UI
uses it; rotating a webhook from a terminal is `rm` then `add`
until someone needs otherwise.

**Verify.**

- `alert list` prints `webhook_masked` and no test asserts a
  raw URL anywhere in stdout or stderr.
- `alert add --webhook-url <url>` is not a valid invocation.
- `alert add` with no TTY and no `--webhook-url-stdin` fails
  and names the flag.
- `--webhook-url-stdin` with an empty pipe says the pipe was
  empty rather than sending one.
- `wire --source X --alert Y` exits non-zero having made no
  request at all.
- `matcher add --as float --value high` exits non-zero with
  nothing stored.
- `source add --domain sts` with 32 hex characters warns on
  stderr and stores it anyway — the API cannot tell, a person
  can, and it will never match.
- `alert test` exits non-zero when the delivery carries an
  error, or a CI job that "checked the webhook" checked
  nothing.
- Every path the CLI builds matches a route in
  `contracts/openapi.json` — method and body fields included.

---

## Integration scenarios (ALT-8)

These are the cases the tickets above have to remain
true under. Names are stable; add the test when the
route exists.

### S1 — Redeploy keeps the Alert

Deploy `private::Tiny`, produce a matching line, flush.
Stop. Deploy again (new `session_id`, same `type`).
Produce the same kind of line. The second fire happens
without editing the Source. The two delivery rows name
the same Alert.

### S2 — Session id is not a Source

A Source `sts:{old_session_id}` is **accepted** (a hex
id is a legal selector; see Sources). It never matches.
There is no code path that reads
`sts_sessions.session_id` when resolving a Source. A
line from that session matches `sts:{its type}` and
`sts:*` only.

### S3 — Short name is not a Source

Two trees, `private::Tiny` and `node1::Tiny`, both
`Strategy.name == "tiny"`. A Source `sts:tiny` is
accepted and matches neither, unless some type is
literally `tiny`. Each qualified key matches only its
own lines.

### S4 — Live only, no history

Insert rows into `session_logs` that would match.
Start the worker. Discord stub sees nothing. Changing
a regex and saving similarly does not walk the table.

### S5 — No ring-buffer replay

Publish 10 matching lines, then start the worker, then
publish nothing more. Stub sees nothing. Publish one
new matching line; stub sees one fire after the
window (or the inject lands in the buffer).

### S6 — Warn/Error level

`level` Matcher `["warn","error"]` wired from `sts:*`.
`info` does not inject. `error` does.

### S7 — Extract Signal

The sample line
`'"risk value = {%f}", 0.995'` with the stated
`extract` spec injects; `0.50` does not. The embed
contains the captured value.

### S8 — Shared Alert buffer

Two Matchers, one Alert. Ten matching lines in one
window (after `envelope_id` fold) → one POST. This
proves the two Matchers share the quiesce buffer, not
a fire-count cap that cannot bind.

### S9 — Persist isolation

A Matcher search that hits `timeout=` does not prevent
`run_log_persist` from flushing the same line. Assert it
structurally, not against the clock — at `timeout=0.01`
a wall-clock assertion is a 10ms race and will flake.
Give the test Matcher a long `timeout=`, schedule a
coroutine before the search, and assert it ran while the
search was still on the matcher thread. Dropping
`concurrent=True` is what makes that fail. The worker
processes the next line.

### S10 — Secret stays secret

Create an Alert, list it, audit it, test-fire it,
match-fire it. None of GET body, audit `result`, or
worker `caplog` contain the webhook URL string.

### S11 — Wildcard and specific

`sts:*` and `sts:CrossArb` both wired to different
Matchers, same Alert or different. A CrossArb line
hits both Sources. A Tiny line hits only `*`.

### S12 — Null type

A session row with `type` null and a line without
payload `type` does not hit `sts:CrossArb`. It does
hit `sts:*` if that Source exists.

### S13 — Registry key

Gate on (`MFTIK_AUTH_ENABLED=1`). A registry-scoped
key cannot GET or POST `/alerts`. An API key can.

### S14 — Test fire is not a match

`POST /alerts/{id}/test` produces a delivery and a
Discord POST without a log line. It does not require
a Source.

---

## Order

```
ALT-1 stamp type on log/status
  └── ALT-4 match worker + resolve
        └── ALT-5 level / regex / extract
              └── ALT-6 window + Discord
                    └── ALT-8 integration

ALT-2 tables
  └── ALT-3 CRUD + mask + audit + test
        ├── ALT-6 (needs Alert rows and the client)
        ├── ALT-7 UI (can mock fire; needs CRUD)
        └── ALT-9 CLI (same endpoints; needs CRUD)
```

ALT-1 and ALT-2 start in parallel. ALT-4 needs the
stamp (or it is cache-only for STS, which is the
fallback, not the path). ALT-7 may merge once ALT-3
exists; it must not invent a second JSON shape the
route does not return. ALT-8 is written against the
names ALT-3–6 actually shipped. ALT-9 sits beside
ALT-7 rather than after it: they are two front ends on
one set of endpoints, and neither is the other's
prerequisite.

## Docs that become wrong when this ships

| File | What it says today | After |
|---|---|---|
| `StsSessionStatus` docstring in `packages/common/src/mftik/protocol/messages.py` | Every field is also on `SessionInfo` | Still true, because ALT-1 adds `type` to both. Update the field list in the same ticket. |
| `frontend/src/routes/+layout.svelte` | Nav is Home … Settings | Alert is a top-level item. Update in ALT-7. |
| `frontend/src/routes/+layout.svelte` `SITE_DESCRIPTION` | "strategy sessions, API keys, and audit" | Alerts are part of the control plane. Update in ALT-7. |

Update those in the ticket that makes the sentence
false, not in a mop-up ticket. `docs/Auth.md` stays
right: Discord the identity provider is not Discord
the webhook.

## Out of scope (later epics)

- `status.sts` as a Source (`failed` / `interrupted`
  without scraping a log line). The snapshot will
  already carry `type` after ALT-1.
- Sandboxed Python Matchers (subprocess, no network,
  millisecond timeout).
- A second `kind` on Alert (Telegram, Slack).
- Encoding `type` into `session_id`.
- A named deploy / instance id distinct from `type`.
- Clock-aligned flush; `immediate` + cooldown.
- A rolling fire cap independent of the quiesce
  window (the column `max_fires_per_interval` would
  have been, and under quiesce it could never bind).
- Playwright beyond the ALT-7 smoke.
- Retention / compaction of `alert_deliveries`.
- `type` on `session_logs` (history search, not match).
- `mftik alert edit`. Rotating a webhook from a
  terminal is `rm` then `add` until it is not enough.
