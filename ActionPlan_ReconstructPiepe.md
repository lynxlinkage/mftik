# Action Plan — Rebuilding STS sessions after a restart

## The problem

An STS restart ends every running strategy. The sessions are recorded
(`interrupted`, with the reason), but nothing puts them back, and there is no
mechanism that could: the row says a session existed, not that it should exist
again.

The goal is that an STS deploy or crash is not something a user has to notice.
A strategy that was running before the restart is running after it, with the
same session id, the same log stream, and the same row in the table.

## What is already true

Facts established while adding the terminal statuses. These are the ground the
plan stands on, not assumptions.

- **The rebuild inputs are already persisted.** `sts_sessions` holds
  `strategy`, `td_api_ids`, `md_ids` and `st_paras` — the whole deploy spec.
  Nothing new has to be recorded for a rebuild to know what to construct.
- **`strategies` rows survive.** A rebuild that reuses the session id needs no
  new row and no schema change there; the existing 1-1 foreign key still
  points at the right session.
- **Attach is idempotent.** `TdSessionManager.attach` returns early when
  `session_id in acct.links`, and `MdSessionManager.attach` when the link
  exists. A re-sent attach after a caller timeout is a no-op, so the rebuild
  can retry without double-counting refcounts.
- **RPC requests queue.** Domain RPC is a Redis list drained with `BLPOP`, so a
  request sent while TD or MD is still starting waits rather than vanishing.
  Only the caller's timeout bounds it (`BROKER_REQUEST_TIMEOUT`, default 5s).
- **Per-session ordering is fixed and enforced.** STS session first, then MD
  attach, then TD attach: TD blocks until it observes the STS lease heartbeat
  (`timed out waiting for STS lease heartbeat`), and MD the same via
  `MdLeaseAck`. A rebuild has to follow the same order for the same reason.
- **Process ordering is undefined.** No domain waits for another at boot;
  `sts` depends only on Redis in compose, and nothing depends on `sts`. The
  rebuild cannot assume TD or MD is up when it starts.
- **There is a claim primitive.** Every live session already holds
  `{prefix}:sts:alive:{session_id}` with a 30s TTL, renewed on the session
  heartbeat. Several STS processes serve the same RPC subject, so a rebuild
  needs exactly this to decide who owns a recovery.

## Decisions taken

**The rebuild runs in STS, at boot.** Not in the API. STS is the only process
that can construct a session without an RPC to itself, it already owns the
rows and the liveness keys, and recovery should not depend on a stateless HTTP
facade that takes no part in trading. What STS gains over the API path is one
attach sequence plus rollback — it does not need to mint an id, call STS, or
write a `strategies` row.

**The session id is reused.** The alternative — mint a new id, make
`strategies` one-to-many — keeps a cleaner audit trail but changes the schema,
the UI's one-row-per-deploy model, and splits the log stream in two. Reuse
keeps the log continuous: `log.sts.{session_id}` is the key for both the Redis
buffer and the `session_logs` table, so "session stopped" and the rebuilt
"session started" land on one timeline and the UI needs no change at all.

The cost is that the row stops recording that it was interrupted, since it
flips back to `live`. That history lives in the log instead. Note the
weakness: log lines reach Postgres via the **API's** `log_persist`, so if the
API is also down during the restart those lines exist only in the Redis ring
buffer (500 lines, 24h). Good enough as a user-visible trail, weaker than a
column as an audit record.

## What "invisible" does and does not mean

Achievable at the STS layer: the session survives, the log is continuous, the
status push flips the table row back to `running` on its own, and the frontend
needs no change.

Not achievable at the TD layer, and this should not be promised. The lease
grace is 5s in TD and 3s in MD, while a container restart takes longer. TD
will expire the lease, detach, and — when the refcount reaches zero —
`_destroy_account`, dropping the venue connection. The rebuild re-attaches and
TD reconnects. On the paper venue this costs nothing; on a real venue it is a
websocket reconnect and a gap in market data and order updates. Anyone
debugging "the first seconds after a rebuild look empty" should find that
written down here rather than rediscover it.

---

## Phase 1 — Make a session reconstructible

No rebuild logic. Everything here is independently correct and can ship alone.

### 1.1 Persist `cid_slot`

The blocker. `cid_slot` is allocated fresh from a Redis `INCR` in
`create_session` and stored nowhere, while `owns()` is

```python
slot_of(client_order_id) == self.session.cid_slot
```

A rebuilt session with a new slot does not recognise its own pre-restart
orders: their fills and order updates are discarded as another session's, OMS
reconciliation shows resting orders it does not own, and the strategy cannot
cancel them through its own logic. A rebuild without this produces a strategy
blind to its own position, which is worse than no rebuild.

- Add `cid_slot` to `sts_sessions` (migration).
- Write it in `create_session`.
- Reuse it on rebuild rather than allocating.

### 1.2 A path back to `live`

`StsSessionRepository` only moves rows to terminal statuses (`mark_finished`).
A rebuild needs the reverse: set `live`, clear `finished_at`, clear `reason`.

### 1.3 Somewhere to keep facts a strategy cannot re-derive

See "Three kinds of state" below for why this is needed and why it is not a
snapshot. Concretely:

- A JSON column on `sts_sessions`, separate from `st_paras` so configuration
  and runtime facts do not blur into each other.
- `Strategy.remember(key, value)`, written at the moment a fact becomes true
  rather than at shutdown — which is what makes it survive `kill -9`, the case
  a shutdown-time snapshot cannot cover.
- The stored dict handed back to `on_rebuild`.

### 1.4 Reap orphans as `interrupted`, not `failed`

A process killed outright leaves the row `live` until the reaper writes
`failed` + `process died: no session heartbeat`. For rebuild purposes those
sessions are the same category as a graceful shutdown — nothing was wrong with
the strategy, it did not choose to stop — but they currently sit in `failed`
next to `oco_insufficient_balance`, separable only by matching the reason
string.

Changing the reaper to write `interrupted` makes the candidate set exactly
`status = 'interrupted'`, with the reason still distinguishing a crash from a
clean shutdown.

*Caveat to handle in phase 3: if the strategy caused the crash, rebuilding
loops. That argues for an attempt cap, not for mislabelling the status.*

---

## Phase 2 — Rebuild on boot

### 2.1 Claim

For each `interrupted` row, `SET NX` the liveness key. Winner rebuilds; anyone
else skips. This is the same key the reaper already reads, so ownership has one
meaning across the system rather than two.

`mark_alive` currently uses a plain `SET` and must not be changed to `NX`
wholesale — a running session renewing its own key needs the overwrite.

### 2.2 Reconstruct

Same order as deploy, for the same reasons:

1. Build the session locally from the row (`strategy`, `st_paras`,
   `td_api_ids`, `md_ids`, `cid_slot`), start it, lease heartbeat begins.
2. MD attach, with retry.
3. TD attach per `api_id`, with retry.
4. Row back to `live`; the status push announces it and the UI updates itself.

Retry rather than a readiness gate: TD and MD may not be up yet, attach is
idempotent, and the requests queue. Bounded attempts with backoff, then give
up and leave the row `interrupted` for the next boot to try.

Rollback on partial failure must mirror deploy's — an attached TD with no live
session is worse than no rebuild.

### 2.3 The strategy hook

Reconstruction restores the session, not the strategy's opinion of the world.
What to do about orders resting at the venue, a half-filled OCO or a chase
mid-flight is per-strategy work and is deliberately **not** written here. What
the framework owes each strategy is the hook, its input, and the guarantee
that adoption is possible at all.

```python
async def on_rebuild(self, remembered: dict[str, str]) -> None:
    """This session ran before and is being restored.

    Recon follows as usual — treat what it brings as your own, not as
    another session's. `remembered` carries whatever this strategy wrote
    with `remember()`.
    """
```

Not a pure marker: ChaseOrder proves the hook needs an input (below).

This remains the largest risk in the feature. Phase 2 should not be described
as "rebuild works" until at least one strategy has filled it in.

#### Three kinds of state

The distinction that decides what may be persisted at all:

| Kind | Example | Where it comes back from |
|---|---|---|
| Externally observable, changes | resting orders, filled qty, position, balances | **recon** — never a snapshot |
| Configuration | side, qty, `expiry_s`, `must_exec` | `st_paras`, already persisted |
| Established once at runtime | `_ref_start`, `_started_ms` | **nowhere today** — see 1.3 |

A snapshot of the first kind lies. Between the crash and the rebuild an order
can fill, be cancelled by the venue, or be rejected; restoring "cid X is
resting" restores a belief that is false, and the strategy then acts on it.
That is why this is not `serialize()` / `deserialize()` — a general pair
invites dumping object state, which drags the first kind in with the third.

The third kind has the property that makes persisting it safe: **it never
changes once set**. There is no staleness and no write-through problem, which
is why `remember()` writes at the moment of establishment rather than at
shutdown.

#### Why ChaseOrder forces this

`chase.py` already guards against re-anchoring:

```python
async def on_recon_done(self, msg):
    if msg.api_id != self._primary_api_id() or self._armed:
        # Recon runs again after a venue reconnect. The chase is already
        # under way by then, and re-anchoring would hand it a fresh
        # expiry budget it did not earn.
        return
```

After a rebuild `_armed` is False on a fresh object, so that guard does not
fire. The chase re-arms: `_started_ms` becomes now — a full expiry budget it
did not earn — and `_ref_start`, the anchor `_slippage_bps()` measures
against, moves to wherever the market is now. A chase that had already run
40bps against you forgets it and allows another full budget of slippage. The
guard is silently widened, which is worse than an error.

Neither value is recoverable from OMS, from `st_paras`, or from the market:
they are facts about the past that only the dead process knew. Both are
write-once. They are exactly what 1.3 exists for.

Note what is *not* the problem: the price being different on the first
`on_best_quote` after a rebuild. A chase chases; the market moving during the
outage is the thing it is built to handle. Only the missing anchor is a defect.

#### Leaving orders resting, or cancelling them

Two questions, and only one of them is the strategy's to answer.

**Adoption is the framework's floor, not a policy choice.** Cancelling on
shutdown cannot be relied on. It needs TD, and TD may already be gone — this
is observed, not hypothetical:

```
strategy.oco | cancel not accepted by TD cid=... [108 TD_NO_ACK]
```

The domains have no shutdown ordering between them, and `kill -9` runs no
cleanup at all. So a rebuild must be able to find its own orders still resting
however the session ended. That capability is mandatory regardless of policy.

**Whether to cancel on shutdown is per-strategy, and the two strategies we
have want opposite answers.**

*OCO: cancel.* Its own docstring names simultaneous double-fill as the one
risk it cannot rule out — a window of nanoseconds, because the cancel goes out
the instant the first fill is seen. Leaving both legs unmanaged across a
restart stretches that window to the length of the outage: one leg fills, the
other is not cancelled, the market keeps moving, and the pair that existed to
give one outcome gives two. That cost dominates the lost queue position.
`on_stop` already cancels both legs, so this needs no change — only the
understanding that it is best-effort and the rebuild handles what survived.

*ChaseOrder: it does not matter.* One order, no pair invariant. Left resting,
the worst case is that it fills, which is what was wanted; recon brings the
fill back. It reprices constantly anyway, so it re-queues by design and loses
little either way.

#### Known consequence: a rebuilt OCO can refuse to re-place

If the cancel did land and the rebuild re-places, the legality check runs
against a fresh quote — and the market may have moved through one of the legs
while STS was down. The pair is then marketable-on-arrival, is refused before
anything is sent, and the session ends `oco_illegal`.

That is arguably correct: a post-only leg at that price would be refused by
the venue anyway. But "my OCO turned into a failure because STS restarted" is
a surprise, and it needs either a reason that says so plainly or a different
judgement inside OCO for the rebuild path. Decide it when OCO's hook is
written; do not leave it to be discovered.

---

## Phase 3 — Deliberate rebuild, not automatic

Three questions the earlier phases deliberately left open, answered once
there was a working rebuild to answer them about.

### 3.1 Age

Restoring is for a restart, where the gap is seconds to minutes. Beyond that a
session comes back to a market that has moved on and to orders the venue may
have expired, which is a decision for a person rather than something to do to
them at boot. Default 30 minutes, `STS_REBUILD_MAX_AGE_S` to widen.

This was not hypothetical: the first run of the rebuild against the real stack
restored a session left over from a SIGKILL test hours earlier, alongside the
one that had just stopped.

Out of window is not an error and is not treated as one. Nothing failed — it
is the configured policy — so the session is skipped with a warning and its
row is left exactly as it was, still saying what happened to it.

### 3.2 Intent — `restart: always | never`

A field in `strategy.yml`, beside `td` / `md` / `sts` rather than inside them,
because it describes the deploy and not the strategy's parameters. Stored on
the row at create time.

Default `always`. Two gates already stand in front of a rebuild — the operator
enabling it and the strategy class supporting it — so a deploy that reaches
this question is one whose run was cut short and would rather continue.
`never` is for the one-shot that would be wrong to resume.

An unrecognised value is refused rather than read as `always`. Resuming a run
that asked not to be resumed is the one direction this must not fail in.

### 3.3 Attempt cap

`rebuild_count` on the row, three attempts. Counted *before* the attempt: a
strategy that takes the process down with it is exactly the loop this exists
to break, and a count written afterwards would never record it.

### 3.4 Scan limit

The listing helper defaults to 100 rows, which the rebuild scan was silently
inheriting. It now asks for 1000 and warns when it hits that, because a
truncated scan leaves sessions looking exactly like sessions nobody asked to
restore.

---

## Where this ended up

Phases 1–3 are implemented. What is deliberately still not done:

- **The flag defaults to off.** `STS_REBUILD_ON_BOOT=1` opts in. Turning it on
  is safe — `Strategy.rebuildable` keeps sessions of classes that have not
  implemented `on_rebuild` out of it — but it is a decision for whoever runs
  the stack, and `docker-compose.yml` does not set it.
- **Adoption is unit-tested only.** The paper venue has no bestquote stream,
  so neither ChaseOrder nor OneCancelOther can quote or place on this stack.
  Everything around adoption was exercised end to end — the claim, the window,
  the intent, the cid slot, the remembered facts, the restore path itself —
  but taking back orders that are genuinely resting at a venue has never run
  outside a test. That is the largest remaining gap, and closing it needs
  either a simulated venue with a bestquote stream or a real venue test
  account.

## Order of work

1. Phase 1 whole — no behaviour change on its own. 1.3 ships the storage and
   `remember()`; a strategy calling it before any rebuild exists is harmless.
2. Phase 2.1 + 2.2 behind something that defaults to off, so a rebuild that
   goes wrong is not the default experience.
3. One strategy fills in 2.3, and only then is the feature real.
4. Phase 3 as the answers appear.
