# The `mftik` client

A node is something you host. This is what you run against one from the machine
you write strategies on.

```bash
pip install mftik
```

The package is `packages/common` — the same one every service in this workspace
installs. It carries three things: the shared library, `mftik.strategy` (what a
strategy is written against), and `mftik.cli`.

## Why one package and not two

`pip install mftik` has to give you `import mftik` and the `mftik` command. A
separate `mftik-cli` distribution would mean a strategy author installs two
things to do one job, and the CLI's whole purpose is to act on the same
registry, protocol and strategy types the library already models — the code it
would import is here.

The cost is that the CLI's dependencies land in every service image, since the
services install this package too. So the CLI is built out of what the library
already needs: `argparse` from the stdlib, `httpx` for HTTP, `pyyaml` for
`strategy.yml`. Adding `typer` or `click` would put a CLI framework in the
trading containers to save a few lines of parser setup, which is not a trade
worth making.

## Where the API is

Two URL shapes, and the difference is not the client's to guess.

| Deployment | API | WebSockets |
|---|---|---|
| Local (`docker-compose.yml`) | `http://localhost:8000` | `ws://localhost:8000/ws` |
| Deployed (`deploy/docker-compose.yml`) | `https://host/api` | `wss://host/ws` |

Traefik routes `/api/*` to the app **after stripping the prefix**, and routes
`/ws/*` to the same app **without stripping anything**. So a deployed node's API
base and its socket base do not share a path, and `Node.ws_base` is derived from
the origin rather than from the API base beside it.

`mftik connect` probes `/health` and then `/api/health` — the health route is
public on both, which is what lets this run before there is a credential. The
answer is stored, so every later command reads it instead of trying both.

## Profiles

`~/.config/mftik/config.toml` (or `$XDG_CONFIG_HOME/mftik/config.toml`, or
whatever `MFTIK_CONFIG` names).

```toml
default = "prod"

[profiles.prod]
url = "https://node.example.com/api"
token = "mftik_ak_..."

[profiles.local]
url = "http://localhost:8000"
```

The file holds bearer tokens, so it is opened at `0600` and written into that
handle — not created under the umask and narrowed afterwards, which leaves a
window where it is world-readable. Same reasoning, same code shape, as
`mftik.registry.store`'s `remotes.toml`.

Which profile a command acts on: `--profile`, then `MFTIK_PROFILE`, then the
default set by the last `connect`. Each step is something the user chose, in
descending order of how recently they chose it.

A node running with `MFTIK_AUTH_ENABLED=0` issues no key, and a profile without
a `token` is normal rather than broken.

## Authentication

`POST /auth/keys` is gated on a browser session (`SessionDep`), not on a key —
which is the point of scoped credentials, and it means the CLI cannot mint one
by presenting another. So `connect` does what a browser would:

1. `GET /auth/status` — is the gate on, and has the instance been claimed
2. `POST /auth/login/password` — get a session cookie
3. `POST /auth/keys` — mint an `mftik_ak_` key, returned exactly once
4. store it, then `POST /auth/logout` to drop the cookie

The cookie is never written to disk. What is stored is the key, which is what
every later command sends as `Authorization: Bearer`. `--token` skips the whole
flow for a key that already exists, which is what CI should use — and it is the
only path that needs no terminal, so a prompt reached without a tty says so and
points at it.

The key is named `mftik-cli@{hostname}`, so revoking the laptop that was lost
does not mean revoking every machine. `/auth/me` reports `via` as
`key:{name}` — which is what lets `mftik whoami` say *which* credential got you
in, not just that one did.

`--setup` claims an unclaimed node. It is opt-in because claiming decides who
owns the instance and is not undoable from this side; without it, a node with
no Owner is an error that tells you the flag exists.

Revoking is a node-side act. `mftik disconnect` forgets the key here; the row on
the node stays live until it is revoked there, and the command says so rather
than letting a user believe otherwise.

**A key cannot manage keys.** `GET/POST/DELETE /auth/keys` are gated on a
browser session — a key that could mint another key would make a leaked one
unbounded. A `mftik key ls` would therefore have to ask for the password on
every call, which is worse than opening the UI. Left to the UI unless that
changes.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | fine |
| 1 | something the user can fix — a bad argument, a refusal, a 404 |
| 2 | the node did not answer |
| 130 | interrupted |

1 and 2 are separate so a CI job can retry the second and not the first.

An error is one line on stderr, never a traceback: a stack trace is the right
answer for a bug in this tool and the wrong one for a typo'd URL, and almost
everything reaching the top level is the second.

## Commands

Built:

```
mftik connect <url>         authenticate this machine against a node
mftik whoami                who this machine is, to the node it points at
mftik profiles              the nodes this machine is connected to
mftik disconnect <name>     forget one, and the key it issued
mftik check <path> [cfg]    the import gate and on_initialized, offline
```

`check` does not talk to a node. It runs four layers and stops at the first
refusal: the import gate, the naming rules `add` uses, `parse_strategy_yml`
if a document was given or `<path>/strategy.yml` exists, then `load_class`
and — when there is a document — `on_initialized`. Without a document those
last two steps about the config are skipped, and the command says so.

Landing next — see the implementation plan for ordering:

```
mftik push <path>           copy a strategy tree into the node's registry
mftik run <target> [cfg]    push, deploy, and tail the session's log
mftik ps / logs / stop      what is running, and what it is saying
mftik init [dir]            scaffold a project against a connected node
```

`run` pushes by default, because the iteration loop is edit-then-run and a
separate push step is one a person forgets exactly once before it costs them a
confusing session. `--no-push` deploys what is already on the node.

## What the node had to learn first

STS imports the registry into a running process. The API writes to it from a
different one. Everything below follows from that.

**A push has to reach the process, not just the disk.** `POST /registry/v1/add`
now sends `sts.registry.reload`, and answers with `loaded` saying whether STS
came back able to resolve the strategy. Three outcomes, and the client should
say which:

| `loaded` | `load_error` | What happened |
|---|---|---|
| true | — | stored and deployable |
| false | "STS did not reload…" | stored; STS did not answer. Deployable after a restart |
| false | "STS did not load it as…" | stored; STS reloaded and rejected the tree. Its log says why |

Only the first means `run` can proceed.

**A re-push has to load the new code.** `mftik.registry.load` keyed each tree's
module name on its path, which does not change when the source behind it does —
so a replaced tree returned the module the previous load left in `sys.modules`,
successfully, with the old code. The tag now covers the tree's digest as well.

**A delete has to stop the deploy.** `DELETE /registry/v1/strategies/{name}
?origin=` is new, along with `RegistryStore.remove`. `origin` is required:
`public` and `private` can hold trees of the same name and a default would pick
between them on a guess. It refuses while a live session is running the
strategy — that session holds its own instance and would outlive the files,
which is exactly why the refusal belongs before the delete.

**Registration had to stop being add-only.** `load_local_registry` never
removed anything, which was invisible while losing a tree meant restarting the
process that held it. With deletes and disconnects it stopped being invisible:
the files went, the qualified key stayed, and deploying it built a session from
source that was nowhere on disk. A reload now drops keys the store no longer
has — including a tree that stopped importing, which was loadable and is not
any more.

**A read scope had to become a read scope.** The gate resolved a request's
required scope from its path alone. Adding any write under
`/registry/v1/strategies` would therefore have handed it to every peer this
node has issued a registry key to, since that prefix is the one they are
allowed to read. `required_scope` now takes the method.
