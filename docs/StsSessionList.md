# STS session list — tabs and a page, not a pile

The STS page is the place an operator deploys a strategy and then lives with
the runs. Pause, stop, ack, open YAML, pull logs: those actions sit on the
same table that lists every session this node has ever recorded. That was
fine while the table was short. It is not fine now: as this is written the
production node has recorded 39 sessions and 36 of them are finished, so
the three rows an operator can act on are the hardest three to find. The
ratio is what grows.

Nothing here is built yet. The files and endpoints named below are what the
change rests on, all of them checkable in the tree today.

## Motivation

`/sts` renders every deploy in one table. `frontend/src/routes/sts/+page.svelte`
calls `api.strategies()`, which hits `GET /sts/strategies`. That handler in
`apps/api/src/mftik_api/routes/sts.py` loads

```
StsSessionRepository.list_sessions(status=None, limit=limit)
```

with `limit` defaulting to 100. `status=None` means every status: `live`
(Pause / Stop), `failed` and `interrupted` (Ack), `done` and `ack` (YAML /
Logs / Download). They share one scan.

TD and MD already split Live / History. Board already filters by status
(`live`, `done,ack`, and so on). STS is the page that most needs the same
cut — it is the only list with statuses an operator must act on — and it
is the one that does not have it.

The 100-row cap is silent. `list_sessions` says so in the source: callers
that need everything in a status must pass a limit large enough to say so,
or the default truncates. The response is a bare `strategies` array. There
is no `has_more`, no cursor, no way for the page to ask for the 101st row.
At 39 rows the cap has not bitten yet. When it does, older deploys simply
stop appearing and nothing says so — a failure that arrives without a
symptom, which is why it belongs in the same change as the tabs.

The complaint is density, not DOM cost. Virtualizing the table would leave
the operator scrolling through finished runs to find the one that failed.
The rows that do not belong on the same screen should not be on it.

`GET /sts/sessions` already takes `status`. It is the wrong endpoint for
this table, and the reason is not the columns. It returns `SessionOut`,
which does carry `paused`; what it lacks is `type` and the deploy document
the YAML button reads. The disqualifying part is where its rows come from.
That route asks the STS process over RPC, and STS answers it by reading
this same table (`SessionManager.list_sessions` → `list_db_sessions`) — a
database list routed through a process that can stop answering. On
2026-08-18 STS's RPC subject went silent for seven hours while its sessions
went on trading, and every call to that endpoint timed out. History has no
reason to be reachable only while STS is up.

The strategies list is the one that already knows how to build a row.

## Target

- The default view is the rows an operator acts on, not history.
- History is reachable and complete. Page two exists. Nothing is dropped
  because a default `LIMIT` ran out.
- One list endpoint keeps serving this table. The page does not move to
  `GET /sts/sessions`.
- History and Attention do not depend on the STS process. Only Live does,
  and only for `paused`.
- A live status event on `/ws/status/sts` must not reset a History cursor
  the operator has already walked.

## Solution

Three tabs on `/sts`, using the same `.tabs` the other session pages already
share in `frontend/src/app.css`:

| Tab | `status` query | What the row is for |
|---|---|---|
| Live | `live` | Pause / Resume / Stop |
| Attention | `failed,interrupted` | Ack |
| History | `done,ack` | YAML / Logs / Download |

`paused` is not a status. It is a flag on a `live` row, filled from the
STS process the way the list already does — and that fill becomes
conditional. `list_strategies` opens with a blocking `request_domain` to
`Topics.STS`, five seconds of default timeout in front of a query that does
not need it. Once the table is tabbed, only a request whose status set
includes `live` has any use for the answer. Skipping the probe otherwise is
one condition, and it is what makes History and Attention readable while
STS is down.

The endpoint stays `GET /sts/strategies`. A new route would be a second
list of the same rows. The change is query parameters and a `has_more`
flag on `StrategyListResponse`.

| Query | Meaning |
|---|---|
| `status` | Comma union, parsed the way Board already parses it (`_parse_statuses` in `apps/api/src/mftik_api/routes/board.py`). Omitted means every status — that is what `test_an_attach_failure_still_appears_on_the_list` asserts, and it stays true. |
| `before` | `session_id` of the last row on the previous page. One opaque cursor rather than a `(timestamp, id)` pair a caller can get half right — see below. |
| `limit` | Page size. Default 50, `ge=1`, `le=500` — the same bounds Board uses for its session list. |

Keyset, not offset. Fills and session logs already page on `(ts, id)` for
the reason Board states in the route: an offset shifts under rows that
arrive while somebody reads. Sessions have the same problem the moment a
new deploy lands above an open History. The handler fetches `limit + 1`
and sets `has_more` from the extra row, the same trick, no second `COUNT`.

The cursor is a session id and not the `(before_ts, before_id)` pair those
two send, because the analogy stops at the column type. `session_logs.ts`
is a `Float`, so its cursor compares a float to a float and is exact.
`sts_sessions.created_at` is a `DateTime(timezone=True)`, so a float cursor
has to be converted back before anything can be compared with it — and that
conversion is not stable across the two dialects this suite runs on.
`list_strategies` emits `row.created_at.timestamp()` directly rather than
through `_epoch()`, the helper further down the same file that exists
because a naive datetime's `.timestamp()` reads as local time. Postgres
returns an aware datetime and sqlite returns a naive one, so the same
cursor names two instants an offset apart depending on which the test ran
against — and the sqlite leg, where pagination tests are cheapest to write,
is the wrong one. None of that is a hazard worth carrying to page two of a
list of deploys.

A session id carries no arithmetic. It is the primary key, so the caller
does not have to send the ordering value at all: the query resolves it.

An unknown `before` is a 422, not an empty page. Rows do leave this table —
`created_by` is `ON DELETE CASCADE`, so removing a user takes their
sessions with them — and a cursor that silently returned nothing would read
as "history ends here", which is the failure this whole change exists to
stop.

`list_sessions` in `packages/db/src/mftik_db/repositories/session.py` grows
one optional `before_session` kwarg — the `before` query parameter, under
the name the other repositories already use for a cursor — and a stable
order:

```
anchor = (SELECT created_at FROM sts_sessions
          WHERE session_id = :before_session)

ORDER BY created_at DESC, session_id DESC
WHERE created_at < anchor
   OR (created_at = anchor AND session_id < :before_session)
```

Today the order is `created_at DESC` alone. Two sessions created in the
same timestamp can swap, skip, or repeat across a cursor. The id is the
tie-breaker because it is unique on this table and already on the row the
client holds.

*On this table* is why the change goes on `StsSessionRepository` and not on
the mixin the three domains share. `list_sessions` is defined on
`_SessionListMixin`, and `session_id` is unique for STS alone. `td_sessions`
is one row per `(session_id, api_id)` keyed by an integer `id` — the
production node has two live TD rows sharing one session id as this is
written — so `ORDER BY created_at DESC, session_id DESC` is not a total
order there and the same cursor cannot mean the same thing. STS overrides;
TD, MD and Board keep today's method untouched. If either of the other two
ever wants paging, its tie-breaker is `id` and the argument is a different
one.

The frontend talks to the same client helper, with options:

```
api.strategies({ status, before, limit })
```

Tab switch, Refresh, and a successful deploy replace page one. A deploy
also switches to Live: the row it just created is a `live` one, and an
operator who deployed while reading History would otherwise watch nothing
happen. Load more appends, using the last rendered row's `session_id` as
the cursor, and only while `has_more` is true. Pause / Stop / Ack update or
remove that one row. They do not refetch History and throw away pages the
operator already loaded.

The status socket is the part that will otherwise fight the cursor. For a
session the page has never seen, `applyStatus` calls `fetchUnknownSession`,
which calls `refresh()` — and `refresh()` reloads the whole list from the
first page. On History that discards every Load more. The rule after this
change:

- The event's status is not in the current tab, and the row is not on
  screen → ignore it.
- The event's status left the current tab and the row is on screen →
  remove the row (live → done leaves Live; failed → ack leaves Attention).
- The event belongs to the current tab and the row is on screen → patch
  `status` / `paused` / `reason` in place.
- The event belongs to Live or Attention and the row is new → reload
  page one of that tab. History does not insert from the socket. Refresh
  is enough; inserting "somewhere" would invent an order the cursor does
  not have.

Two guards already in that file survive the change, and the last rule is
only affordable because they do. `lastEventTs` drops an event older than
the one already applied to a row, so a straggler cannot trigger a reload.
`pendingSessions` records unknown ids while a page-one fetch is in flight
so concurrent announcements share that one request. An id that arrives
after the request has gone out stays in the set and queues a trailing
reload — joining a fetch that already left is how a restart announcing
two rebuilt sessions 150ms apart would drop the second until Refresh.

```
tabs ──► GET /sts/strategies ──► list_sessions ──► sts_sessions

/ws/status/sts ──► applyStatus
                     ├── in current tab, known row ──► patch
                     ├── left current tab          ──► remove
                     └── new, Live / Attention     ──► reload page one
```

## Action plan

The contract exists before the UI consumes it. Each step leaves the tree
in a shippable state; the page keeps working until step 4 lands.

1. **Repository.** The cursor kwarg and `ORDER BY created_at DESC,
   session_id DESC` on `StsSessionRepository.list_sessions`, overriding the
   mixin rather than widening it. Tests in
   `packages/db/tests/test_sts_session_repository.py`.
2. **API.** `status`, `before`, bounded `limit`, and `has_more` on
   `StrategyListResponse`; the pause-state probe only when the status set
   includes `live`. Tests in `apps/api/tests/test_sts_strategies.py`.
   `just openapi` so `contracts/openapi.json` matches.
3. **Client.** `api.strategies(opts)` in `frontend/src/lib/api.ts`. The
   no-argument call still means "first page of everything", which is what
   the current page does, until step 4.
4. **Page.** Tabs, Load more, and the WebSocket rules above on
   `frontend/src/routes/sts/+page.svelte`.
5. **Playwright.** The UI contract, under a runner this repository does
   not have yet (see **Integration test**).

Out of scope for this change:

- Paginating `GET /sts/sessions` or Board's session list. They have the
  same silent `limit`; they are not this table.
- Virtualizing the table. The wrong rows should not be in the DOM, not
  merely cheap to paint.
- A count badge on Attention. Not for want of a number — `GET /stats`
  already returns `failed`, `interrupted` and `ack` per domain, the client
  already has `api.stats()`, and the home page already calls it. It is out
  because that count and these rows would arrive in different responses and
  drift apart: the socket patches a row in place and would leave the badge
  saying 1 over a tab showing none. A badge that lies about something
  needing attention is worse than no badge. It goes in when the count comes
  back with the rows.
- A migration. `status` is already indexed and the table is 39 rows. A
  composite `(status, created_at, session_id)` is an optimisation if
  History ever needs one, not a prerequisite. The list the cursor is
  borrowed from does carry one — `ix_session_logs_domain_stream_ts_id` —
  because that table takes a row per log line and this one takes a row per
  deploy. The analogy stops at the index, the same place it stops at the
  column type.

## Unittest

Pytest pins the list contract. The frontend typecheck does not see a
query string.

Repository (`packages/db/tests/test_sts_session_repository.py`):

- Several statuses in one call still return the union. That case already
  exists (`test_list_sessions_accepts_several_statuses`); it stays.
- Three rows, `limit=2`: the first page is the two newest, the cursor of
  the last of those returns only the third, and the two pages do not
  overlap. The handler's `limit + 1` trick is an API concern; the repo
  test asks for two and then for the rest.
- Two rows given the same `created_at`: the order is `session_id DESC`,
  and a cursor on the first of those two returns the other exactly once.
- A cursor naming a row that is not there returns nothing — the handler
  above it turns that into a 422, and it can only do so if the repository
  reports it rather than falling back to the first page.
- `status=[]` matches nothing. An empty union is not "skip the filter".

API (`apps/api/tests/test_sts_strategies.py`):

- `test_an_attach_failure_still_appears_on_the_list` still calls the
  handler with no `status` and no cursor. Both the successful deploy and
  the attach failure are present; `has_more` is false. That test exists
  so a later `WHERE type IS NOT NULL` cannot hide a failed attach. The
  new fields must not become a reason to weaken it.
- `status=failed,interrupted` returns only those statuses.
- Three rows, `limit=2`: the first page has `has_more=true`; the second
  page, using the last row's `session_id` as `before`, is the remaining id
  and `has_more=false`.
- `status=done,ack` never touches the broker. `QuietBroker` in that file
  stands in for an STS with nothing live; a second stand-in that raises on
  `request` stands in for one that is not answering at all, and the call
  still returns its rows. That is the Target property, and the API is the
  only place it can be asserted.
- `status=faild` and `status= , ` are 422, not an empty page.

## Integration test (Playwright)

`frontend/package.json` has no Playwright, and no other e2e runner.
`npm run check` is a typecheck. Auth.md already records that gap: the
assertions the UI depends on are made on the API, because there is
nowhere else to make them. Tabs, Load more, and a socket that must not
reset a cursor are UI contracts. The API cannot see them. This work
introduces the runner.

`@playwright/test` is added under `frontend/`. Specs live in
`frontend/e2e/`. A `just` recipe runs them. The tests intercept
`GET /sts/strategies` and `/ws/status/sts`. They do not start STS, they
do not deploy N strategies, and they do not share the operator's
database. A spec that needs a live process to prove a tab clicked is a
spec that will not be run.

Cases:

- The default tab is Live. Only `live` rows render. Pause and Stop are
  on those rows; Ack is not.
- Attention renders `failed` and `interrupted`, with Ack. History
  renders `done` and `ack`.
- History with `has_more`: Load more sends the last row's `session_id` as
  the cursor, appends the next page, and leaves page one in the table.
- A status event whose new status leaves the current tab removes that
  row. An event for a session that belongs to another tab does not
  reload History and does not drop rows Load more already added.
- Refresh and a tab switch replace the list. The next strategies request
  carries no `before`.
- Two status events for unseen sessions share one in-flight fetch and
  at most one trailing reload — not a request per event.
- An unseen session that arrives after that fetch has already left
  queues a trailing reload, and the row appears without Refresh.
- Acking every visible Attention row while `has_more` is true still
  shows Load more; the cursor is the last dropped id.

`npm run check` stays the typecheck. Playwright is the UI contract.
Pytest remains the API and repository contract.
