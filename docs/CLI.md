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
flow for a key that already exists, which is what CI should use.

Revoking is a node-side act. `mftik disconnect` forgets the key here; the row on
the node stays live until it is revoked there, and the command says so rather
than letting a user believe otherwise.

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
mftik profiles              the nodes this machine is connected to
mftik disconnect <name>     forget one, and the key it issued
```

Landing next — see the implementation plan for ordering:

```
mftik connect <url>         authenticate against a node, once
mftik init [dir]            scaffold a project: mftik.toml + a strategy + strategy.yml
mftik check <path>          run the import gate offline, before pushing anything
mftik push <path>           copy a strategy tree into the node's registry
mftik run <path> <config>   push, deploy, and tail the session's log
mftik ps / logs / stop      what is running, and what it is saying
```

`run` pushes by default, because the iteration loop is edit-then-run and a
separate push step is one a person forgets exactly once before it costs them a
confusing session. `--no-push` deploys what is already on the node.

### One thing `run` needs that does not exist yet

STS imports the registry once, at boot (`mftik_sts/app.py`), and
`mftik.registry.load` keys each tree's module name on its *path*. Re-pushing a
strategy under the same name therefore hits a cached module: the edit lands on
disk and the old code runs. Two changes fix it, and `run` is not honest without
them — the tree's digest belongs in the module tag, and `POST /registry/v1/add`
needs to tell STS to reload.
