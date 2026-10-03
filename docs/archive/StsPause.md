# STS pause — not a status, and not one operation

A live session can be paused. The UI has a button, the API has two
routes, STS has two RPC types, and every bundled strategy implements
`on_pause` / `on_resume`. None of that is a status, none of it is
persisted, and none of it agrees on what "paused" means.

Stop is the halt an operator can name. This change removes the other
one.

Nothing here is built yet. The files and endpoints named below are what
the change rests on, all of them checkable in the tree today.

## Motivation

`paused` is not a column. `StsSessionRow` in
`packages/db/src/mftik_db/models/session.py` has `status` and `reason`.
Alembic has never added `paused`. The flag lives on
`Strategy._paused` in `packages/common/src/mftik/strategy/base.py`, is
set by `on_pause` / `on_resume`, and is read back by
`SessionManager.list_sessions` only while that process still holds the
instance. A restart, a rebuild, or a process that is not answering
leaves no record. `st_facts` does not keep it either, so a session
restored after STS comes back is live and unpaused whether or not
anyone had pressed Pause.

The list pays for that flag. `GET /sts/strategies` in
`apps/api/src/mftik_api/routes/sts.py` already reads `sts_sessions`.
When the status set includes `live` it also does a blocking
`request_domain` to `Topics.STS` so it can fill `paused` from the
in-memory strategies. The default timeout on that call is five seconds
(`apps/api/src/mftik_api/broker_rpc.py`, `timeout: float = 5.0`).
`docs/StsSessionList.md` made History and Attention skip the probe:
those tabs have no use for the answer. Live still does it, and only
for `paused`. A Live page that needed nothing but the database still
waits on that call.

That wait is not theoretical. On 2026-08-18 STS's RPC serve loop
returned after one `BLPOP` raised, and the process went on trading
for seven hours with nothing able to list, pause or stop a session.
`docker ps` showed it up. The write-up is in the tree:
`packages/common/src/mftik/runtime.py` (why `run_until_stopped`
exists) and `apps/sts/tests/test_rpc_loop_survives.py` (the loop
that must not die that way again).

The hook is not one operation. `StsSession.pause` in
`apps/sts/src/mftik_sts/session/session.py` records a lifecycle line
and calls `strategy.on_pause`. It does not stop dispatching MD or TD
events. `Timer._should_fire` in
`packages/common/src/mftik/strategy/timer.py` is the only framework
gate: a due token holds while `strategy.paused` is true. Everything
else is per-class, and the classes already disagree:

| Strategy | What pause does |
|---|---|
| CrossArb | Cancels resting quotes and stops placing. MD keeps the hedge touch warm. A fill that races the cancel is still hedged. |
| ChaseOrder | The resting order stays. The chase timer is cancelled, so it stops being repriced. |
| OneCancelOther | Both legs stay. A fill still cancels the other. Only a new place is held back. The source says leaving both live through a pause is the one outcome an OCO must not have. |
| TwapStrategy / NoopStrategy | Cancel the timer and log. |
| MacdDollarBars / TapeKeeper | Inherit the flag. They never consult it. |

An operator who presses Pause on the Live tab therefore gets a
different book depending on the type. CrossArb pulls quotes.
OneCancelOther keeps both working orders. Chase leaves size on the
venue and stops managing it. That is not a control-plane verb; it is
four local inventions behind one button.

The CLI never grew the verb. `packages/common/src/mftik/cli/sessions.py`
prints a `PAUSED` column on `ps` and implements `stop`. There is no
pause or resume command. `docs/CLI.md` does not mention either.

The UI is the only caller. `frontend/src/routes/sts/+page.svelte` has
`togglePause`, a Pause / Resume button on every `live` row, and a
`paused` badge that outranks `running`. Those call `api.pauseSts` /
`api.resumeSts` in `frontend/src/lib/api.ts`, which hit
`POST /sts/sessions/{id}/pause` and `/resume`. `_control` in the STS
routes forwards them as `sts.session.pause` / `sts.session.resume`.
`SessionManager.pause` / `resume` flip the flag and publish a
`StsSessionStatus` snapshot with `paused` true or false. That is the
whole product.

Keeping the hook "until someone defines it" leaves Live coupled to STS
for a field the database does not have, and leaves every new strategy
to invent a fourth meaning. Stop already tears the session down.

## Target

- A live session is running or it is not. Stop ends it. There is no
  third control-plane state.
- Until a CrossArb-owned cancel-quotes verb exists, pulling quotes
  means Stop and a redeploy from the YAML on the row. Stop is
  irreversible: the session ends.
- `GET /sts/strategies` does not talk to STS. Live, Attention and
  History are the same database list.
- `on_pause` and `on_resume` are not part of `Strategy`. A bundled
  class does not mention pause. `Timer` fires or it is cancelled; it
  does not consult a pause flag.
- `POST /sts/sessions/{id}/pause` and `/resume` are gone.
  `sts.session.pause` and `sts.session.resume` are gone.
- Wire types that carried `paused` no longer do. No stub field that
  is always false.
- The Live tab's actions are Stop, YAML, Logs, Download. The badge
  for a live row is `running`.
- No migration. There is no column to drop.

## Solution

Delete the verb. Do not replace it with a status, a `st_facts` key, or
a no-op hook.

The list is the first thing that gets simpler. `_includes_live` and
the `paused_by_session` probe in `list_strategies` exist only to write
`StrategyOut.paused`. After the field is gone the handler is:

```
GET /sts/strategies ──► list_sessions ──► sts_sessions
```

`list_strategies` takes no broker. The probe's stand-ins
(`QuietBroker` / `DeadBroker`) go with it. The property is
structural: a handler that cannot talk to STS cannot wait on one.

Status events lose the field for the same reason they gained it.
`StsSessionStatus` in `packages/common/src/mftik/protocol/messages.py`
is a full snapshot so a consumer that missed a line is not permanently
wrong. `paused` was on that snapshot because it was also on
`SessionInfo`. Both drop it. `SessionManager._publish_status` stops
taking `paused`. Create, stop, fail, ack, and rebuild keep publishing
`status` / `reason` / `strategy` / `created_by` / `finished_at`.

The surrounding prose in that file is part of the same edit. Three
comments name the verb:

- `StsSessionControlRequest`'s docstring: "pause / resume / stop /
  fail" becomes "stop / fail".
- Its `reason` field: "Ignored by pause / resume / stop" becomes
  "Ignored by stop".
- `StsSessionStatus` argues for snapshots-not-deltas with
  `{"event": "paused"}` as the counter-example. The argument stays;
  the example becomes `{"event": "stopped"}`.

```
/ws/status/sts ──► applyStatus
                     ├── patch status / reason
                     └── paused is not a field
```

The session object shrinks by two methods. `StsSession.pause` /
`resume` and `SessionManager.pause` / `resume` go. The RPC router in
`apps/sts/src/mftik_sts/rpc/router.py` drops
`STS_SESSION_PAUSE` / `STS_SESSION_RESUME`. The constants themselves
are defined in `messages.py` and re-exported from
`packages/common/src/mftik/protocol/__init__.py` (the import list and
`__all__`). Dropping the router entries without that file is what
breaks every other import.

Both `_control` helpers stay, on purpose. They are the validate /
reply / audit path, not a dispatcher that needs two verbs to justify
itself. STS `_control` in `apps/sts/src/mftik_sts/rpc/sessions.py`
keeps a one-key dict (`stop`) and the `unknown_action` branch — a
misspelled action string is still a programming error. Do not inline
`stop_session` into `handle_session_stop` just to avoid a one-key
dict. API `_control` in `apps/api/src/mftik_api/routes/sts.py` keeps
serving `stop_session`; that is its one remaining caller. Fail and
ack already have their own handlers and stay that way. Audit
operations `sts.session.pause` and `sts.session.resume` stop being
written because the routes that recorded them are gone; existing
audit rows stay.

`Strategy` loses `_paused`, the `paused` property, and the two hooks.
The docstring's process-control list becomes `on_start`, `on_ready`,
`on_stop`. Chase and TWAP already cancel their timer on stop. OCO
already cancels both legs on stop — the pause path that left them up
is the one being deleted. MacdDollar and TapeKeeper have nothing to
delete beyond the inherited no-ops.

CrossArb is more than the hook. The `on_pause` cancel loop is the
same work `on_stop` already does; that stays on stop. The four
live-path guards are separate edits in
`apps/sts/src/mftik_sts/impl/cross_arb.py`:

- `on_best_quote`, hedge tick: `if not self.paused: await
  self._maintain_quotes()` becomes an unconditional call. The
  comment "Paused: keep the touch warm, but do not place or reprice"
  goes with the branch.
- Quote-venue tick: drop `and not self.paused` from the conjunct
  that decides whether to re-arm.
- `_maintain_quotes` and `_place_quote`:
  `if self.paused or self._stopping` becomes `if self._stopping`.

The module docstring's **Pause / resume.** paragraph is deleted in
the same edit.

`Timer._should_fire` is deleted, and so is the hold branch in
`TimerToken._run` that calls it. After pause is gone,
`_should_fire` would be `not self._closed`. `close()` already sets
`_closed` and then cancels every token. `_run` already returns when
`_cancelled` is set, on the line immediately before it consults
`_should_fire`. The 50ms poll — commented "Paused / closed: hold
the due time until resumed or cancel" — is therefore reachable only
in the race where `close()` has set `_closed` but not yet cancelled
that token, a token that the next line of `close()` is about to
cancel. Holding the due time through that race is not a behaviour.
Leaving a polling loop whose comment describes a deleted feature is
worse. `token()` and `_track` still raise if the timer is closed;
that stays. A strategy that wants a timer off cancels the token, the
way pause already did by hand in Chase / TWAP / Noop.

The page drops `togglePause`, the Pause / Resume button, and the
`paused` badge branch that currently sits above `running`. The
comment above that cell ("stale `paused` from the live-session probe
must never mask them") goes with the branch. The rest of the
comment — terminal statuses first, do not infer failed from a null
`type` — stays. `applyStatus` patches `status` and `reason` only.
`frontend/src/lib/api.ts` drops `pauseSts` / `resumeSts` and the
`paused` field on `StrategyRow`, `Session`, and `StsControl`.
`frontend/src/lib/logging/status.ts` drops it from
`StsSessionStatusEvent`. The CLI `ps` table drops the `PAUSED`
column.

`just openapi` follows, so `contracts/openapi.json` no longer lists
the two routes or the field.

`docs/StsSessionList.md` is the list page's contract, not a shipping
diary of the tabbing PR. Every sentence this change falsifies is
rewritten in the same step as the handler. That is ten places, not
three — they are listed under Action.

What this is not:

- A new "halted" status. Status is already `live` / `done` /
  `failed` / `interrupted` / `ack`. Pause was deliberately not one of
  those, and promoting it would need the column this change is glad
  not to have.
- A per-strategy action API. CrossArb's "cancel quotes, keep the
  session" is a real idea and a different change: a verb that
  CrossArb owns, not a hook every class must interpret. Chase and
  OCO would not implement it, which is the point. Until that verb
  exists, the operator-facing answer is the Target one: Stop and
  redeploy.
- Soft-delete of the field. Leaving `paused: false` on every
  response keeps the probe's reason for existing and tells the next
  reader the verb is still a thing.

Unrelated uses of the English word stay. Tape coverage, backfill
cursors, and event-log download all "resume" a walk. Those are not
this flag. `frontend/src/lib/components/RegistryStrategies.svelte`
reuses `.badge.paused` as the diverged colour. The class name is
paint; the CSS rule in `frontend/src/app.css` can keep serving it.

```
operator ──► Stop ──► sts.session.stop ──► session.stop ──► on_stop
                                                              │
                                                              ▼
                                                    done + reason=operator_stop

(no pause path)
```

## Action

The contract exists before the UI consumes it. Each step leaves the
tree shippable. There is no schema step.

1. **Document.** This file. `docs/StsSessionList.md` is updated in
   step 5, when the list handler actually stops probing.
2. **Strategy and timer.** Drop `on_pause`, `on_resume`, `_paused`,
   and `paused` from `packages/common/src/mftik/strategy/base.py`.
   Delete `Timer._should_fire` and the hold branch in
   `TimerToken._run` that calls it (`timer.py`, the 50ms poll).
   `token()` / `_track` still refuse a closed timer. Delete the
   hook overrides in
   `apps/sts/src/mftik_sts/impl/{cross_arb,chase,oco,twap,noop}.py`.
   CrossArb also loses the module **Pause / resume.** paragraph and
   the four live-path guards named under Solution. OCO's
   `self.paused` check in `_maybe_place` goes with the hook.
3. **Session and RPC.** Remove `StsSession.pause` / `resume`,
   `SessionManager.pause` / `resume`, the two RPC handlers, and the
   router entries. Drop `STS_SESSION_PAUSE` / `STS_SESSION_RESUME`
   from `messages.py` and from
   `packages/common/src/mftik/protocol/__init__.py` (the import list
   and `__all__`). `_publish_status` and `StsSessionControlResult`
   lose `paused`. Rewrite the three `messages.py` comments named
   under Solution. Both `_control` helpers stay, as above.
4. **API.** Remove the two POST routes and the Live pause probe.
   Drop `paused` from `StrategyOut`, `SessionOut`, and
   `StsControlResponse` in `apps/api/src/mftik_api/schemas.py`.
   `just openapi`.
5. **List doc.** Rewrite every sentence in
   `docs/StsSessionList.md` that this change makes false. As of
   this writing, ten:

   | Where | What it says today | After |
   |---|---|---|
   | Intro | "Pause, stop, ack, open YAML, pull logs" | Stop, ack, open YAML, pull logs |
   | Motivation, status→action | `live` is Pause / Stop | `live` is Stop |
   | Motivation, `SessionOut` | "does carry `paused`" | Drop the clause. The disqualifier is still that the route asks STS; the row also will not carry `paused`. |
   | Target | "Only Live does, and only for `paused`" | No tab depends on the STS process |
   | Solution, tab table | Live is Pause / Resume / Stop | Live is Stop |
   | Solution, probe paragraph | "`paused` is not a status… filled from the STS process" and why the probe is conditional | Delete the paragraph. No tab probes STS. |
   | Solution, row updates | "Pause / Stop / Ack update or remove that one row" | Stop / Ack |
   | Solution, socket patch | `status` / `paused` / `reason` | `status` / `reason` |
   | Action plan, API step | "the pause-state probe only when the status set includes `live`" | No probe |
   | Integration test | "Pause and Stop are on those rows" | Stop is on those rows; Pause is not |

6. **Client and page.** `api.ts`, `status.ts`, `+page.svelte`
   (button, badge, `togglePause`, and the stale-probe sentence in
   the status-cell comment), `frontend/e2e/sts-session-list.spec.ts`.
   CLI `sessions.py` drops the column.

Out of scope:

- A migration. Nothing on `sts_sessions` changes.
- Per-strategy halt / cancel-quotes. That is a new verb with one
  implementer, not a resurrection of `on_pause`. No timeline here.
- Paginating `GET /sts/sessions`. That route still asks STS because
  it is the process list, not this table. It loses `paused` on the
  row because the field is gone, not because the route moves to the
  database.
- Rewriting old audit rows that recorded `sts.session.pause`.
- Renaming `.badge.paused` in the registry sync table.
- Inlining either `_control` down to `stop`. See Solution.

## Test

Pytest pins the list and the lifecycle. Playwright already owns the
Live tab; the Pause button is part of that contract and has to leave
it. `npm run check` sees the dropped fields.

Strategy / timer (`apps/sts/tests/test_strategy_lifecycle.py`,
`apps/sts/tests/test_timer.py`, `apps/sts/tests/test_cross_arb.py`,
`apps/sts/tests/test_oco.py`):

- `test_session_strategy_process_control_1_1` walks
  `on_start` → `on_ready` → `on_stop`. There is no pause step.
- `test_noop_pause_cancels_and_resume_rearms` is deleted. Stop still
  cancels the noop timer; that is `on_stop`.
- `test_skips_while_paused` is deleted. `_should_fire` is gone. A
  cancelled token still does not fire; that case already exists.
- `test_pause_cancels_quotes_but_keeps_listening` and
  `test_resume_replaces_quotes` (`test_cross_arb.py`) are deleted.
  CrossArb's stop still cancels quotes.
- `test_a_paused_strategy_places_nothing_until_it_resumes`
  (`test_oco.py`) is deleted. OCO's stop still cancels both legs. A
  paused OCO that places nothing is not a behaviour anyone can
  reach.

Status (`apps/sts/tests/test_status_events.py`):

- `test_the_lifecycle_is_announced_as_snapshots` is create → stop.
  The payloads are `("live",)` then `("done",)` — no `paused` key.
  Fail and ack snapshots stay as they are, minus the field.

Failed-session list (`apps/sts/tests/test_session_failed.py`):

- `test_list_sessions_reports_the_reason` keeps the
  `(session_id, status, reason)` assertion. It drops
  `assert rows[0].paused is None` and the comment "A closed session
  has no live strategy, so pause state is unknown." `SessionInfo`
  has no such field.

API (`apps/api/tests/test_sts_strategies.py`):

- `test_an_attach_failure_still_appears_on_the_list` still calls the
  handler with no `status`. Both rows are present; `has_more` is
  false; nothing on the response is named `paused`.
- `test_live_is_the_database_alone` lists `status=live` with no
  broker argument. The handler takes none. That is the Target
  property, and the API is the only place it can be asserted.
- `QuietBroker` / `DeadBroker` are gone. Nothing lists live
  sessions over RPC to decorate this table.

CLI (`packages/common/tests/test_cli_sessions.py`,
`packages/common/tests/test_cli_run.py`):

- `ps` still prints session, strategy, status. There is no fourth
  column. Fixtures that stuffed `"paused": false` drop it.

Playwright (`frontend/e2e/sts-session-list.spec.ts`):

- The default tab is Live. Only `live` rows render. Stop is on
  those rows; Pause is not; Ack is not. The current
  `getByRole('button', { name: 'Pause' })` is visible assertion
  on the Live tab is the one that flips.
- The Attention-tab `Pause` `toHaveCount(0)` assertion is deleted,
  not kept as a vacuum. A test that names a verb nobody can press
  tells the next reader the verb is still a thing.
- Fixture rows and status-event payloads drop `paused`.
- A status event still patches `status` / `reason` in place.
- The other tab / cursor / trailing-reload cases in that file do
  not mention pause today and stay as they are.

`npm run check` stays the typecheck. Playwright is the UI contract.
Pytest remains the API, RPC, and strategy contract.
