# Audit identity — who acted, not which user

The audit page is a table of control-plane mutations. The columns today
are When, User, Operation, Result. User is `audits.user_id`. This
instance has one Owner. Seed and an empty-database setup both insert the
first `users` row without an id, so autoincrement gives it `1`;
`MFTIK_DEFAULT_USER_ID` defaults to `1` as well, and that is who every
request is when the gate is off. The number is not a schema promise —
delete the row and the sequence will not go back — but it is not
information. What an operator needs is which *proof* did the thing:
password, Discord, Google, or which API key. Registry keys are in that
list of proofs and will display if a row ever carries one; they do not
appear today because nothing they can reach writes an audit.

The files and endpoints named below are what the change rests on.

## Motivation

`frontend/src/routes/audit/+page.svelte` used to render `a.user_id`.
`GET /audits` in `apps/api/src/mftik_api/routes/audits.py` returned
`AuditOut`: `id`, `user_id`, `operation`, `result`, `created_at`. The
table behind it (`packages/db/src/mftik_db/models/audit.py`) had those
same four meaningful columns. `record_audit` in
`apps/api/src/mftik_api/audit_util.py` took `user_id` and nothing else.

The request already knows more. `AuthMiddleware` attaches a `Principal`
with `via` (`password` / `discord` / `google` / `key:{name}` /
`disabled`) and, for a bearer token, `key_id`. `docs/Auth.md` says that
`via` is what makes the audits table useful under machine credentials.
Login and logout already stuff `via=` into the free-text `result`. STS
deploys, venue API mutations, and key mint/revoke do not. A CI key that
deploys a strategy looks identical to the Owner clicking Deploy after a
Discord login.

`Principal.machine` sets `via=f"key:{name}"` and does not say whether
the key is `api` or `registry`. The name is enough to tell keys apart;
the kind is what tells an operator which kind of secret it was. Kind is
on the `auth_keys` row the middleware just loaded. It is thrown away
before the handler runs.

Stuffing `via=` into `result` for every mutation would avoid a
migration. It would also put the actor in the same string the page
already shows as Result, so Identity and Result would repeat each
other unless something parsed the prefix back out. The actor is a
column of its own.

## Target

- The Audit table's second column is the proof that made the request,
  not the Owner's id.
- New rows record that proof at write time. Old rows stay as they are
  and render as an em dash.
- A key is shown as an API key or a Registry key plus the name it was
  minted under. Renaming is not a thing this table does; revoke leaves
  the row, so the name on the audit line still resolves.
- `GET /auth/me`'s `via` string does not change.
- Registry peer reads stay unaudited. A poll every few seconds is not
  a control-plane mutation. `auth_keys.last_used_at` is where that
  activity already lives.
- `user_id` stays on the row and on the wire. The page stops showing
  it.

## Solution

Snapshot the principal onto the audit row.

`Principal` grows `key_kind: str | None`. `Principal.machine` takes
`kind` and keeps `via=f"key:{name}"`. Middleware passes `key.kind`
when it resolves a bearer token. Browser sessions and the disabled
stand-in leave `key_kind` null.

Alembic `0025_audit_via` revises `0024_fold_strategies` and adds three
nullable columns to `audits`. No backfill. A model column without this
migration fails on insert against a live database.

| Column | Type | Why |
|---|---|---|
| `via` | `VARCHAR(64)` | The same string `Principal.via` already is. |
| `key_id` | `INTEGER` FK `auth_keys.id` `ON DELETE SET NULL` | The key that acted, if one did. Revoke keeps the row; a true delete must not take the audit with it. |
| `key_kind` | `VARCHAR(16)` | `api` or `registry`, snapshotted, so the display does not have to join. |

`record_audit` still takes `user_id`. It also takes `principal` and an
optional `via` override. The override wins — setup and password login
do not have a session yet, and they know the proof. Otherwise `via`,
`key_id`, and `key_kind` come off the principal.

```
async def record_audit(
    *,
    user_id: int,
    operation: str,
    result: str,
    principal: Principal | None = None,
    via: str | None = None,
) -> None:
```

Call sites that already have a principal pass it. Login and setup pass
`via="password"` or the provider name. STS and venue-API handlers gain
`principal: PrincipalDep = ANONYMOUS` the way they already default
`OwnerId` to `DEFAULT_USER_ID`, so a unit test that calls the function
directly does not have to build a request. `_control` in
`apps/api/src/mftik_api/routes/sts.py` takes the principal through.
`result` on login/logout can drop the redundant `via=`; `ip=` stays.

`AuditOut` grows `via` and `key_kind` (and `key_id`). `GET /audits`
fills them. `just openapi` follows.

The page column is Identity. A formatter, not a join:

| `via` | `key_kind` | Shown as |
|---|---|---|
| `password` | | Password |
| `discord` / `google` | | Discord / Google |
| `key:{name}` | `api` | `API key · {name}` |
| `key:{name}` | `registry` | `Registry key · {name}` |
| `key:{name}` | null | `Key · {name}` |
| `disabled` | | Auth disabled |
| null | | — |

```
request ──► AuthMiddleware ──► Principal (via, key_id, key_kind)
                                    │
                                    ▼
                              record_audit
                                    │
                                    ▼
                         audits.via / key_kind
                                    │
                                    ▼
                         GET /audits ──► Identity
```

## Action

The contract exists before the UI consumes it. Each step leaves the
tree shippable.

1. **Document.** This file.
2. **Principal.** `key_kind` on
   `apps/api/src/mftik_api/auth/principal.py`; middleware sets it.
3. **Schema.** Migration `0025_audit_via`; model and
   `AuditRepository.record`.
4. **Write path.** `record_audit` and every call site in
   `auth/routes.py`, `routes/sts.py`, `routes/apis.py`.
5. **API.** `AuditOut`, `GET /audits`, `just openapi`.
6. **Page.** `Audit` in `frontend/src/lib/api.ts`; column rename and
   formatter on `frontend/src/routes/audit/+page.svelte`.

Out of scope:

- Auditing `GET /registry/v1/strategies`. The column is ready if that
  ever becomes a mutation worth recording.
- Changing `/auth/me` `via` from `key:{name}` to `api:{name}`.
- Storing the OAuth `label`. One Owner, one linked Discord, one
  linked Google: the provider name is the identity.
- Parsing `via=` out of old `result` strings.

## Test

Pytest pins the write and the list. The page formatter is a few
branches; `npm run check` sees the new fields. No Playwright case —
the UI contract is a column label and a string.

Repository / model (`packages/db/tests/test_models.py`):

- `audits` has `via`, `key_id`, `key_kind`. `key_id` points at
  `auth_keys.id`.

API, password (`apps/api/tests/test_auth_setup.py` or a sibling):

- `POST /auth/login/password` writes `via=password`, `key_kind` null.
  `result` still has `ip=` and no longer needs `via=`.

API, machine key (`apps/api/tests/test_auth_keys.py` or a sibling):

- An API key hitting a route that already audits (venue create, or
  STS deploy if the harness can) writes `via=key:{name}` and
  `key_kind=api`.
- `GET /audits` returns those fields. The Owner's `user_id` is still
  the seeded id.

Stubs that monkeypatch `record_audit` (`test_apis_rename.py`, STS
ack / eventlog) keep working: new kwargs are optional. Direct calls
to `rename_api` without a principal write `via=anonymous` or leave
it null — either is fine; those tests assert `operation` and
`result`, not the actor.

Out of this file's tests: a registry-key read producing an audit
row. That path is not audited.
