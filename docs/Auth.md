# Auth — one owner, many proofs

An MFT instance is single-tenant. One person owns it. That person may prove
who they are several ways, and may mint machine credentials for scripts and
peer nodes. Nobody else gets a `users` row.

This document is the in-app replacement for the Traefik gate described in
[CICD.md](CICD.md). **It is built and validated locally first.** Production
keeps `discord-auth-chain@docker` untouched until the cutover in step 8;
everything before that ships to production inert (see `MFT_AUTH_ENABLED`
below). Local compose and `docker-compose.peer.yml` have no gate at all
today, which is why they are where this gets exercised.

STS, TD, MD, SYM and paper never authenticate HTTP. They trust the broker.
Auth lives only on the API gateway.

Paths here are the ones FastAPI sees. Traefik strips `/api` in production, so
the routers are mounted at the root: `/sts`, `/registry/v1`, `/auth/...`.

## Model

```
no owner, or an owner with no password
  └── POST /auth/setup {username, password}
        → the sole users row (the Owner)
        → the password identity (root, cannot unlink)
        → session cookie

later, while that session is live
  └── connect Discord / Google
        → extra auth_identities rows
        → same user_id
        → never INSERT into users
```

`users` is 0 or 1 row. OAuth is not a sign-up path.

Setup is gated on **the Owner having no password**, not on `users` being
empty. `scripts/seed_paper_apis.py` already creates a passwordless owner, and
local compose runs it before the API even starts (`api` depends on `seed`
completing), so "users is empty" is false on the first `just up` and a naive
409 would lock you out of a stack you just built. Same story on any database
seeded before this feature existed. So:

| State | `POST /auth/setup` |
|---|---|
| No `users` row | INSERT the Owner |
| Owner exists, `password_hash IS NULL` | UPDATE it — set username + hash |
| Owner exists, has a password | **409** |

| Concept | What it is |
|---|---|
| **Owner** | The single `users` row. Every `created_by` / `owner_id` / audit points here. |
| **Identity** | A way to prove you *are* that Owner. Password is intrinsic. OAuth is linked later. |
| **Session** | Browser proof, httpOnly cookie, full power including minting keys and linking OAuth. |
| **API key** | Machine proof for scripts / CLI. Acts as the Owner on every domain route. Cannot change identities or mint keys. |
| **Registry key** | Machine proof for another MFT node. Can only read the peer-facing registry routes. |

Discord / Google / password are not three users. After connect, Discord login
issues a session for the same Owner. An API key is not a second person; it is
a scoped credential the Owner issued.

Concurrent browser sessions may exist. Optional later: a new login invalidates
the others. API keys and registry keys must keep working either way — a peer
sync must not kick the UI, and logging into the UI must not drop a pull.

## Identities

The password identity *is* the Owner. It is created at setup and cannot be
removed (the password can be changed). OAuth identities are optional and
unlinkable.

```
owner id=1  username=yite
  ├── password / yite            setup; stored on users.password_hash
  ├── discord  / <snowflake>     Settings → Connect
  └── google   / <sub>           Settings → Connect
```

Storage splits that way on purpose: putting the password hash on `users`
makes it impossible to delete the root identity by deleting a row.
`GET /auth/me` still lists password as an identity so the UI treats them alike.

Login identifier is **username**, not email. `users.email` becomes optional
(copied from an OAuth profile for display). Email is never a join key and
never auto-links an OAuth account — emails change and collide. Linking
happens only via Connect while a session is live.

Hashing is argon2id — a new `argon2-cffi` dependency on `apps/api`.

## Login vs Connect

OAuth has two purposes on one callback. Which one is decided by a server-side
record, **never by anything in the callback URL**.

| Situation | Result |
|---|---|
| Session live, that Discord/Google not linked | **Connect** — insert `auth_identities`, still Owner 1 |
| Session live, already linked | 204, idempotent |
| No session, already linked | **Login** — new session, still Owner 1 |
| No session, not linked | **403** — log in with username/password, then Connect in settings |
| No Owner yet, OAuth hit | **403** — run setup first |

Connect without a session is treated as an unlinked login: 403, no Owner
created. That is the whole "no second owner" rule. OAuth either attaches to
the existing Owner or is refused.

### The state record

`state` is an unguessable nonce and nothing else. Everything the callback
needs is stored under it when the flow starts:

```
auth_oauth_states
  state       PRIMARY KEY   -- random, single use
  provider    'discord' | 'google'
  mode        'login' | 'connect'
  verifier                  -- PKCE
  session_id  NULL FK       -- the session that started a connect
  expires_at                -- minutes, not hours
```

The callback looks the row up, deletes it, and acts on `mode` from the row.
A `connect` row additionally requires that the session presenting the callback
is the same `session_id` that started it.

A readable `state=connect` in the URL would be an account-takeover bug, not a
style issue: anyone could walk the Owner's browser into
`/auth/callback/discord?code=<theirs>&state=connect` and link **their** Discord
to the Owner, then log in with it forever after. That is exactly the rule this
document exists to enforce, undone by the query string.

A table rather than Redis so there is one storage story and no new client or
signing key. Rows are deleted on use and swept by `expires_at`.

## Credentials

Three proofs, distinguished by prefix so the gateway does not guess:

| Proof | On the wire | Scopes |
|---|---|---|
| Session cookie `mft_session` | Cookie, httpOnly, SameSite=Lax, Secure *(see below)* | Everything, including `/auth/keys` and Connect |
| API key `mft_ak_…` | `Authorization: Bearer` | Every domain route and `/ws/*`. Not identity or key admin. |
| Registry key `mft_rk_…` | `Authorization: Bearer` | `/registry/v1/strategies` and `/registry/v1/strategies/{name}` only |

Keys are shown once at creation. The database stores a SHA-256 (or HMAC)
plus a short prefix for the UI. Revoke is a `revoked_at` timestamp, not a
delete.

Resolution: a Bearer token is one lookup on its prefix — `auth_keys.kind`
says what it is, and the `mft_ak_` / `mft_rk_` prefix is a consistency check,
not a second query. No Bearer, then the session cookie. Neither, anonymous.

`Secure` is environment-dependent. Local development is `http://localhost:5173`
end to end; Chrome and Firefox treat localhost as a secure context and accept
Secure cookies over http, but Safari does not, and the failure looks like
"login silently does nothing". `httpOnly` and `SameSite=Lax` are unconditional.

Anonymous is allowed only on `/health`, `/auth/status`, `/auth/setup`,
`/auth/login/*`, `/auth/callback/*`, and `GET /registry/v1/info`. `/health`
stays public so compose and CI can probe it. Session keepalive therefore must
**not** use `/health` (today `frontend/src/lib/auth.ts` pings `/api/health`
because the Traefik chain wraps the whole origin). It pings `/auth/me`.

### MFT_AUTH_ENABLED

Off by default. When off, the middleware returns an Owner principal built from
`MFT_DEFAULT_USER_ID` and every route behaves exactly as it does today.

This exists because merging to `main` deploys to production, and production
still has the Traefik chain in front. Shipping a live in-app gate behind that
chain locks everybody out, and unrecoverably: the chain answers every
non-navigation request with 401, so `POST /auth/login/password` never reaches
FastAPI. The flag is what lets this land as small PRs on `main` instead of a
long-lived branch. Local compose turns it on in step 2, once `/ws` carries the
cookie and `/login` exists to get back in with — before both, "on" is a stack
you cannot use. Production turns it on in step 8, in the same deploy that
removes the chain.

`/auth/status` reports the flag as `enabled`, so the UI can tell the
difference between "signed in" and "there is nothing to sign in to". Without
it the two look identical from the browser and every auth control becomes a
button that does nothing.

## Where it lives

Not a new workspace package. `packages/common` must not import FastAPI.

```
packages/db                         users + identities / sessions / keys / oauth states
apps/api/src/mft_api/auth/
  principal.py                      Principal(user_id, via, scopes, key_id)
  passwords.py                      argon2id
  oauth.py                          Discord / Google, PKCE, the state record
  sessions.py                       opaque cookie ids in Postgres
  keys.py                           generate / hash / verify
  middleware.py                     HTTP + WebSocket entry
  deps.py                           require_session / require_api / require_registry
  routes.py                         /auth/*
packages/common  registry/sync.py   send Authorization on peer pulls

frontend/src/
  routes/login/+page.svelte         claim or sign in; OAuth buttons later
  routes/settings/+page.svelte      identities, and the keys this node issued
  routes/+layout.svelte             sign out, and hide it when the gate is off
  lib/api.ts                        the /auth/* calls and their types
  lib/auth.ts                       401 handling and session keepalive
  lib/ws.ts                         same-origin sockets, so the cookie travels
frontend/vite.config.ts             proxy /ws as well as /api
```

Starlette HTTP middleware does not run for WebSockets, and after cutover
nothing else authenticates `/ws/*`. In-app auth must use an **ASGI**
middleware (or the same `authenticate()` called from every `ws.py` entry) so
HTTP and WS share one parser.

Handlers take `principal.user_id`. `MFT_DEFAULT_USER_ID` remains only as a
seed/migration default and as the `MFT_AUTH_ENABLED=0` identity, never as a
request identity once the flag is on.

`session` is already an overloaded word here — `sts_sessions`, `td_sessions`,
`md_sessions`, and `mft_db.session.session_scope` for the SQLAlchemy one.
`auth/sessions.py` will be managing auth sessions inside `async with
session_scope() as db:` blocks. Name locals accordingly.

`Principal.via` (`password` / `discord` / `google` / `key:<name>`) is what
makes the existing `audits` table useful under machine credentials. Write an
audit on login, logout, connect, unlink, and key mint/revoke — otherwise the
trail cannot tell the Owner apart from a CI key acting as the Owner.

## Unauthenticated answers

**401, always.** No proof or expired proof gets the same answer whatever
asked, with a JSON body the SPA can act on.

Nothing needs the 302-on-navigation split the Traefik chain forced. After
cutover, a document navigation to `mft.lynkora.com/sts` reaches the *frontend*
container, which is not gated — the SPA loads, its first `fetch` gets 401, and
it routes to `/login` client-side. Locally there was never a gate in front of
the document at all. `location.reload()` was only ever a way to hand an
expired session back to an external redirect that lived outside the app; there
is no such redirect any more.

`frontend/src/lib/auth.ts` — the reload marker, the cooldown, the
visibility-gated keepalive, the comment about a subresource burning the CSRF
cookie slot — is entirely an artifact of that chain. It shrinks to a 401
handler and a keepalive ping. That cleanup is step 9, after the chain is gone;
until then the file is still describing production accurately.

Which gate produced a 401 is part of the answer, because until the cutover
both exist and they want opposite things from the browser. The app's own 401
carries `x-mft-auth: login-required`; the chain's does not. `$lib/auth` reads
that and either routes to `/login` or performs the reload. A build flag would
have had to be kept in sync with a deploy, and would be wrong for exactly the
window where being wrong locks somebody out.

Strip any client-supplied identity headers (`X-Auth-*` and anything this
module later invents) before parsing. The current Traefik chain does this;
the app must keep doing it after the chain is gone.

## The UI

There is more of this than the file list suggests, and it is easy to miss:
this app has never had a login page or a settings page, because it has never
had a login. Three things below assume a UI that does not exist yet, so each
step names the screen it needs rather than discovering it during the work.

**`/login`** — one form doing two jobs. `/auth/status` says which, and it is
public precisely so this page can ask before proving anything: an Owner with
no password is claimed here, and afterwards the same fields sign in. Steps 5
and 6 add a button per provider beside them.

**`/settings`** — the account page, and a new nav entry. Two lists:

- *Identities.* Password, Discord, Google, each with Connect or Disconnect.
  Password is the root identity and cannot be removed, and the UI has to say
  so rather than offering a button that 400s. Show *which* account is linked
  (Discord gives a username and avatar under `identify`), because "connected"
  alone does not let anyone notice the wrong account is attached.
- *Keys.* Name, kind, prefix, created, last used, and a revoke. The list can
  never show a secret because the database does not have one.

**Minting a key is a one-time reveal.** The value is returned once, by the
call that creates it, and is a SHA-256 everywhere after that. So the UI needs
a deliberate moment — the key, a copy button, and a plain statement that
closing this is the end of it — not a row that quietly appears in a table.
Getting this wrong is not a cosmetic bug: it produces keys nobody has, and a
user who works around it by never revoking anything.

**Sign out** lives in the layout, next to the version badge, and is hidden
when `/auth/status` reports `enabled: false`. With the gate off every request
is already the Owner, so signing out would end nothing and land on a page
that bounces straight back. Same reason `/login` says the gate is off instead
of showing a form that appears to work.

## Schema

```
users
  id, username UNIQUE NULL, password_hash NULL,
  display_name, email NULL, created_at

auth_identities
  id, user_id FK,
  provider  ('discord' | 'google'),
  subject,              -- Discord snowflake / Google sub
  email NULL,
  UNIQUE (provider, subject)

auth_sessions
  id,                   -- cookie value, or a hash of it
  user_id FK,
  created_at, last_seen_at, expires_at,
  user_agent, ip

auth_keys
  id, user_id FK,
  kind ('api' | 'registry'),
  name, prefix, key_hash,
  scopes,
  created_at, last_used_at, revoked_at NULL

auth_oauth_states                    -- see "The state record"
```

`username` and `password_hash` are nullable, and `email` stops being
`NOT NULL`. That is not laxness: the row seeded before this feature existed
has none of them, and `ALTER TABLE … ADD COLUMN username NOT NULL UNIQUE` on a
populated table fails outright. `just check-migrations` builds the models from
the migrations against a scratch database and will catch getting this wrong.
Postgres permits many NULLs under a unique constraint, so uniqueness still
holds for the one real username.

Idle window slides on use, same order of magnitude as today's 30 minutes.
Write `last_seen_at` only when the window has actually moved — on the order of
a minute — rather than on every request; the Traefik chain already does this
and the reason is noted in `frontend/src/lib/auth.ts`.

Opaque session rows rather than JWTs, so logout and (optional) single-seat
login are a delete, not a blacklist.

## HTTP surface

```
GET  /auth/status                 {enabled, setup_required, providers, authenticated, username?}
POST /auth/setup                  {username, password} → session; 409 only if a password is already set
POST /auth/login/password         {username, password} → session
GET  /auth/login/{discord|google} mints a state record, mode=login
GET  /auth/callback/{discord|google}
POST /auth/logout
GET  /auth/me

# via=session only
GET  /auth/connect/{discord|google}   mints a state record, mode=connect, bound to this session
POST /auth/password                   change password
DELETE /auth/identities/{id}          unlink OAuth; refuse provider=password
POST /auth/keys                       {kind: api|registry, name}
GET  /auth/keys                       prefixes only, never the secret
DELETE /auth/keys/{id}
```

Rate-limit `/auth/setup` and `POST /auth/login/password`. Those are the only
guessable secrets. Key verification is **not** rate-limited — a 256-bit random
token is not guessable, and throttling it would only break a CI burst.

Every PR here changes route security metadata, so `just openapi` and
`just check-contracts` are part of each one, not a final tidy-up.

## Registry

`connect_remote()` today GETs the peer with no header.
`GET /registry/v1/strategies/{name}` returns source. That is the hole a
registry key closes.

Split the routes:

| Path | Who |
|---|---|
| `GET /registry/v1/info` | Public. `handshake_info()` is versions only, and a peer should fail fast on protocol mismatch before it bothers with a key. |
| `GET /registry/v1/strategies` | Registry key, API key, or session |
| `GET /registry/v1/strategies/{name}` | Same — this is the source dump |
| `/private`, `/add`, `/remotes*` | Session or API key. **403** for a registry key |

`POST /registry/v1/remotes` grows a `token` (the peer's `mft_rk_…`).
`remotes.toml` currently maps `name → url` only; it must store the token
(mode 0600) and `_load_remotes` must still read the flat form. `diff_remote`
and later syncs send

```
Authorization: Bearer mft_rk_…
```

A registry key that can start STS sessions is a bug. Tests should say so.

## Browser, CORS, WebSockets

Locally REST already works: `apiBase()` returns `/api`, the Vite proxy
forwards to `:8000`, and the cookie is scoped to the frontend origin. CORS
needs no change here — it is same-origin through the proxy.

WebSockets are not. `wsBaseUrl()` reads `PUBLIC_API_URL`, which local compose
sets to `http://localhost:8000` while the document is on `:5173`, so the
socket is cross-origin and the cookie is never sent. Every `/ws/*` endpoint
fails auth locally.

Fix it in the proxy, not with a ticket endpoint: add `'/ws'` to
`vite.config.ts` with `ws: true` and **no** `rewrite` (unlike `/api`, these
paths must arrive verbatim), and let `wsBaseUrl()` return the current origin.
A `/auth/ws-ticket` would mean a fourth credential type and another endpoint
to secure, all to work around a dev-server config.

Production is one host (`mft.lynkora.com`): document, `/api` and `/ws` share an
origin, so the session cookie rides the handshake with no change. Pinning CORS
away from `origins=["*"]` (which browsers reject for credentialed requests
anyway) belongs to the cutover, step 8.

## Cutover from Traefik — deferred

Not part of the local work. Recorded here so the local steps do not paint it
into a corner.

Today every router on `mft.lynkora.com` uses `discord-auth-chain@docker`
([discord-forward-auth](https://github.com/yitech/discord-forward-auth)). The
API has no auth of its own; see [CICD.md](CICD.md).

The production API service also has **no volume**, while local compose mounts
`mft_data:/var/lib/mft` and sets `MFT_DATA`. Registry state — including peer
tokens once they exist — currently lives in the container's writable layer and
dies with every deploy. That is an existing bug independent of auth, and it
must be fixed before registry keys reach production.

At cutover: turn on `MFT_AUTH_ENABLED`, pin CORS, and remove the chain from
all three routers **in one deploy**. Traefik keeps TLS. Do not run both gates.
Then drop `MFT_DEFAULT_USER_ID` from the request path.

The first setup on production is username/password against the existing
passwordless Owner. The Discord account that gets in today via the `admin`
group is then **connected**, not used as bootstrap.

`CICD.md` and the header comment in `deploy/docker-compose.yml` both state
that the API has no authentication of its own. Both are accurate now and
become traps the moment the chain comes off.

## Order of work

Local first. Steps 1–6 land on `main` with `MFT_AUTH_ENABLED` off, so
production keeps behaving exactly as it does today.

Each step names its frontend work. Left implicit, it does not go away — it
turns up mid-step as something that has to be built before the step can be
demonstrated at all, which is what happened to `/login` in step 2.

```
1. api  Schema + ASGI middleware + password setup/login.
        setup_required = Owner has no password. Environment-dependent Secure.
        Handlers take principal.user_id. Public /health.
   ui   Keepalive → /auth/me.

2. api  x-mft-auth on the gate's own 401. enabled on /auth/status.
   ui   Vite proxy /ws + wsBaseUrl() same-origin — without this every socket
        fails auth locally, so nothing above can be verified end to end.
        /login (claim or sign in). 401 → /login when the app's gate answered.
        Sign out in the layout. MFT_AUTH_ENABLED on in local compose.

3. api  API keys (mft_ak_).
   ui   /settings with the key list, and the one-time reveal on mint.

4. api  Registry keys (mft_rk_) + connect_remote Authorization + remotes.toml
        token. Lock the public source dump.
   ui   A token field on the registry's connect form; registry keys minted
        from the same screen as step 3, under their own kind.

5. api  Discord OAuth — login and connect, over the state record.
   ui   Identities list on /settings with Connect / Disconnect, showing which
        account is linked. A provider button on /login.

6. api  Google OAuth, same two paths.
   ui   Same two places, one more row and one more button.

── production, later ──

7. Give the production api service a volume for MFT_DATA.
8. One deploy: MFT_AUTH_ENABLED=1, pin CORS, remove discord-auth-chain.
9. ui   Strip the reload machinery from frontend/src/lib/auth.ts — it is an
        artifact of the chain and has nothing left to do.
        Update CICD.md and deploy/docker-compose.yml.
```

Step 2 is deliberately separate from step 1 rather than folded into it: get
the HTTP path fully working locally before touching WebSockets, so a failure
is one cookie-delivery path at a time.

## Tests that define the contract

- Owner exists with `password_hash IS NULL` → `setup_required` is true,
  `POST /auth/setup` succeeds, `users` is still one row, and every existing
  `owner_id` FK still resolves.
- Second `POST /auth/setup`, password already set → 409.
- OAuth callback with an unknown, expired, or already-consumed `state` → 403,
  no new `auth_identities` row.
- `connect` callback presented by a different session than the one that
  started it → 403.
- OAuth callback with no session and no linked identity → 403, `users` still
  0 or 1.
- Connect while session live → one `auth_identities` row, same `user_id`.
- Unlink refuses the password identity.
- Registry key on `/sts` (or `/registry/v1/add`, `/auth/keys`) → 403.
- API key on `POST /auth/keys` or Connect → 403.
- No cookie, no Bearer → 401, on `fetch` and on a WS handshake alike.
- Peer GET `/registry/v1/strategies/{name}` without Bearer → 401.
- `MFT_AUTH_ENABLED=0` → every route answers as the Owner with no credential,
  and `/auth/status` says `enabled: false`.
- The gate's own 401 carries `x-mft-auth`. This is the only thing telling the
  two gates apart while both are live, and nothing else would notice it
  disappearing.

The frontend has no test runner — `npm run check` is a typecheck. So the
assertions the UI depends on are made here, on the API: `enabled`, the 401
header, and the shape of `/auth/status`. What cannot be asserted that way is
the one-time key reveal, which is why it is called out under **The UI** rather
than left to be inferred from `POST /auth/keys` returning a secret once.
- `MFT_AUTH_ENABLED=0` → every route answers as the Owner, no credential
  required.
