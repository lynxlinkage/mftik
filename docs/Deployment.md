# Deployment

Two layers, and the line between them is what changes when.

| Layer | What | Where declared | Rolled by |
|---|---|---|---|
| Infra | NATS, regional Redis | `deployment/sets/infra.json` | a person, rarely |
| Planes | td, sts, md, sym, paper | `deployment/sets/planes.json` | a final tag |
| API | api, frontend | `deployment/docker-compose.yml` | a final tag |

The bus and the tape disk have no reason to restart because a strategy changed.
A tag that rolled them would take every session down to ship a sizing fix, and
losing the tape is losing a warm-up window no venue can rebuild. So the release
workflow applies the plane sets and the API compose, and never touches infra.

Everything on Strategon is an **AssignmentSet**, not a plain assignment — both
layers, including the single-member ones. A set is what makes per-member
identity the control plane's job rather than a copy-pasted block per process.

## Sites

Two sites, because some venues refuse Japanese IPs and because a credential's
jurisdiction is not negotiable. That is the whole reason TW exists; it is not a
capacity decision, and the numbers say so:

| Machine | Site | CPU / RAM | Venue connect | RTT to cp |
|---|---|---|---|---|
| cp | jp | 2 / 4 GB | 3–4 ms | — |
| yite | tw | 16 / 33 GB | 113–147 ms | 35 ms |

yite has eight times the cores and thirty times the venue latency. Nothing that
can run in JP should run in TW, and the split exists only for the traffic that
must originate from a TW address.

`sym` and `paper` are the exception in the other direction: both are off the hot
path — SYM re-pulls listings hourly behind `SymbolClient`'s cache, and paper is
a book with no venue behind it at all — so they sit on the machine with spare
memory and cost nothing by being 35 ms from the JP planes.

cp is also the compose host (`mftik.lynkora.com`) and carries Traefik, the API,
the frontend, SeaweedFS and the Strategon control plane in 4 GB. Plane memory
limits are caps, not reservations, but this is the machine to check before
adding anything to JP.

## NATS

One server per site, and a **gateway** between them, not a cluster:

```
jp   nats-jp on cp     100.108.10.2:4222   gateway :7222
tw   nats-tw on yite   100.65.26.119:4222  gateway :7222
```

Cluster routes gossip a full mesh and assume datacentre latency; cp↔yite is
35 ms. A gateway propagates interest instead and keeps each site's clients on
their own server — a JP plane's request-reply is answered in JP unless the only
responder is in TW. That is the same locality the compliance split needs, so the
transport is doing the routing rather than the application.

There is no `cluster` block. A gateway's name *is* the cluster name for a
single-server site; `varz` reports `cluster: jp` from the gateway declaration
alone.

There is no `jetstream` block either, and no `-js` or `-sd`. Every store family
left JetStream (`docs/JetStreamRemoval.md`): tape is the regional Redis, the
ledger is TD memory, session rows are Postgres and `session_id` is six hex
digits packed into every `client_order_id`. That is what
makes one server per site a complete site instead of a degraded cluster — there
is no meta group to keep a quorum for, so there is nothing to lose by not having
three of them. `NATS_KV_REPLICAS` and `NATS_KV_PLACEMENT_CLUSTER` left with the
buckets they placed.

The `leafnodes` listener is gone. Every plane connects as an ordinary client.

`deployment/nats/nats.conf` is one file for every server; variance is env only,
which the set template fills per member. It is gitignored because it carries the
account passwords.

## Redis

One per site, next to the MD that pumps that site's feeds, on loopback only.

STS never gets a `REDIS_URL` — a TW session warming up on a JP feed would
otherwise open the wrong disk. Not listening on Tailscale is what enforces that,
and one set per role (below) is what keeps the variable out of the STS template.

Durability is `appendonly yes` plus a directory that survives a version roll.
`dir ./` is the agent's strategy directory, which is the exec driver's cwd and
sits outside `releases/`. `maxmemory` is bounded with `noeviction`: full means
new prints are refused, which MD already tolerates because append must not fail
the live fan-out. Eviction would instead silently shorten a warm-up window a
strategy has already been told the coverage of.

The binary is Ubuntu's `redis-server` 8.2.1 taken from cp. Both machines are
24.04 with glibc 2.39 and every shared library it needs, so one artifact runs on
both.

## Why one set per role

`template.env` is shared by a set's members — only the *values* vary, through
`${member.vars.X}`. A single eight-member plane set would therefore have to
declare `REDIS_URL` for STS as well as MD, and mount the registry volume into
every plane.

So: five plane sets, five artifact families, one image uploaded once and
registered under each. It also means a TD roll — which now reaps every session
in that TD's name — is not coupled to an MD roll, and `maxUnavailable: 1` keeps
the two regions of one role from going down together.

Newer control planes key live assignments by member name
(`status.assignmentKey: "member"`) and no longer reserve the family, so the
five sets *could* share one `mftik` family. They do not: the whole gain is
four fewer `RegisterArtifact` calls on a tag, and the migration re-slots every
member. `just s7n-status` shows which key each set is on.

## Instance names

A member's name is the assignment slot, the WorkDir segment, `MFTIK_INSTANCE`,
and the `instances.name` row the dashboard and STS placement read:

| Set | Members |
|---|---|
| `td` | `td-jp` (cp), `td-tw` (yite) |
| `sts` | `sts-jp` (cp), `sts-tw` (yite) |
| `md` | `md-jp` (cp), `md-tw` (yite) |
| `sym` | `sym-tw` (yite) |
| `paper` | `paper-tw` (yite) |

`instances.name` is immutable by design (`packages/db/src/mftik_db/models/instance.py`),
so these names are a new row each, not a rename. `sym` and `paper` have no
`instances` row and no `MFTIK_INSTANCE`: neither is instanced — SYM is behind a
cache and one shared book is the point of paper.

The region on the row is load-bearing, not a label: an unnamed STS session is
placed on the unique enabled STS in the region of its credential's TD
(`docs/JetStreamRemoval.md`). Two STS in one region, or credentials from two
regions on one session, has no derived answer and waits for a person.

## Secrets

The database URLs are not in the spec file and not in GitHub. They are two rows
in Strategon's secret catalog, and the plane sets name them by token:

```json
"DATABASE_URL": "secret.mftik-database-url",
"DATABASE_URL_SYNC": "secret.mftik-database-url-sync"
```

The control plane swaps the token for the ciphertext's plaintext when it
writes the member's assignment, southbound only — `GetSecret` returns length
and key id, never the value. So `deployment/sets/planes.json` is complete on
its own, `just s7n-plan` can print exactly what a tag will apply, and the
release workflow carries one secret: the Strategon token. `DATABASE_URL` and
`DATABASE_URL_SYNC` came off the repository's Actions secrets on 2026-09-19.

Putting one there reads the value from stdin, never argv:

```sh
ssh root@cp 'grep ^DATABASE_URL= /opt/mftik/deploy/.env | cut -d= -f2-' \
    | just s7n-secret-put mftik-database-url
```

`apply` checks every `secret.*` it is about to reference against `ListSecrets`
first. A missing one is refused before the first set is touched — the
alternative is a member that starts, fails closed on resolve, and takes its
region's TD down with it while the set rolls back.

Deleting a secret does not touch the sets that name it; they fail closed on
their next roll. Rotate by `put` under the same name, then re-apply.

## Volumes

Strategon volumes are machine-level, named, and outlive any assignment. The
spec declares them per machine, and `apply` creates any that are missing
before it touches a set:

```json
"volumes": [
  { "machine": "cp",   "name": "mftik-data" },
  { "machine": "yite", "name": "mftik-data" }
]
```

STS mounts it at `/var/lib/mftik` and keeps the strategy registry and the
session event logs under it — the same tree the API's compose volume holds,
which is what `docs/StrategyEnvironment.md` assumes when it puts node extras
next to the registry. Nothing else mounts anything: the tape is the regional
Redis, the ledger is TD memory, SYM and paper hold nothing.

A mount must name a declared volume; `apply` refuses one that does not.
Creating whatever a template asks for would turn a typo into an STS booting on
an empty registry, and the registry is the one directory a node cannot
rebuild.

Before this, the registry lived at `.../<member>/work/registry`, which the OCI
driver bind-mounts and which survives a version roll — but not a member
rename, and not a set delete, both of which are "recreate". The first apply
with the volume comes up empty; copy `work/registry` and `work/eventlog` into
it on each machine before deploying anything that names a strategy.

## Where a process may write

The OCI driver bind-mounts the work directory, the shared directory, the
config file, and the template's `volumeMounts`. Writes anywhere else land
inside `releases/<version>/rootfs` and are **deleted with that release** — 87 MB
of session jsonl was once found inside a `v0.7.6` rootfs, from the days
`STS_EVENTLOG_DIR` pointed at `/var/log/mftik/sts`.

Durable per driver:

| Driver | cwd | Survives a roll |
|---|---|---|
| exec (NATS, Redis) | `<base>/<member>` | that directory; `releases/` lives inside it |
| OCI (planes) | `<base>/<member>/work` | `work/`, `shared/`, and every volume |

The agent expands `${WORK_DIR}`, `${SHARED_DIR}` and `${VOLUME:*}` in both
args and env, so a path under the work directory no longer has to be spelled
out as `/var/lib/strategon-agent/strategies/<member>/work`. OCI rejects
`${RELEASE_DIR}` and `${BINARY}` in args, because neither path is bound into
the container. `${CONFIG}` is.

Every plane set has `captureStdio: true`: the agent keeps each member's
stdout and stderr durably, which is the only write path for set-owned slots —
the per-strategy `SetStdioCapture` call is rejected on them.

## Applying

One script, `scripts/s7n.py`, with the token in `STRATEGON_API_KEY`. Before it
applies anything it reads: the control plane version, every member's machine
(present and reachable), every `secret.*` the rendered env names, and the
declared volumes. Then it registers artifacts, waits for ingest to reach
`READY`, applies each set, and waits for each to report `Ready` at the
generation it was given.

Infra, by hand, from a checkout with `deployment/nats/nats.conf` present:

```sh
export STRATEGON_API_KEY=str_live_...
python3 scripts/s7n.py apply deployment/sets/infra.json \
    --binary nats=/tmp/nats-server   --config nats=deployment/nats/nats.conf \
    --binary redis=/tmp/redis-server --config redis=deployment/redis/redis.conf
```

Drop the `--binary` flags when the artifact version has not changed; keep
`--config` when the conf has, and bump `configVersion` in the spec first — the
version is how the agent knows to fetch it.

Planes are the same script with the image and the tag, which is what the
`planes` job in `.github/workflows/release.yml` runs. A version already in the
catalog is not uploaded again — the same version at a different digest is
refused outright — so a rollback, or a spec change under the running tag, is
just:

```sh
just s7n-plan v0.9.4        # preflight + print what would be applied
just s7n-planes v0.9.4      # apply it
just s7n-status             # phase and members of every set
```

## What a final tag does

1. `test` → `build` (per-arch, by digest) → `merge` (manifest list, moves `:latest`)
2. `planes` — register the image under five families, apply five sets. Needs
   only `STRATEGON_API_KEY`; the database URLs are resolved on the control
   plane.
3. `api` — roll the host compose over SSH, with the previous file kept as
   `docker-compose.bak.<sha>.yml`. Still SSH, because the API needs Traefik's
   `web` network and labels, which an OCI assignment cannot declare.

A prerelease (`v1.2.3-rc1`) builds and tags images and stops there. Infra is not
in this list on purpose.
