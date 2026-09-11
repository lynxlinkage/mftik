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
ledger is TD memory, session rows and `cid_slot` are Postgres. That is what
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

`ApplyAssignmentSet` reserves the member slots *and* the family slot on every
member's machine, so two sets cannot share an artifact family. And
`template.env` is shared by a set's members — only the *values* vary, through
`${member.vars.X}`. A single eight-member plane set would therefore have to
declare `REDIS_URL` for STS as well as MD.

So: five plane sets, five artifact families, one image uploaded once and
registered under each. It also means a TD roll — which now reaps every session
in that TD's name — is not coupled to an MD roll, and `maxUnavailable: 1` keeps
the two regions of one role from going down together.

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

## Where a process may write

The OCI driver bind-mounts exactly three things — the work directory, the shared
directory, and the config file — and there is no way to declare another. Writes
anywhere else land inside `releases/<version>/rootfs` and are **deleted with that
release**.

That is why the plane env points `MFTIK_DATA` and `STS_EVENTLOG_DIR` under
`.../<member>/work/`. Before this they were `/var/lib/mftik` and
`/var/log/mftik/sts`, so every tag threw away the strategy registry and the
session event logs — 87 MB of jsonl was found inside a `v0.7.6` rootfs.

Durable per driver:

| Driver | cwd | Survives a roll |
|---|---|---|
| exec (NATS, Redis) | `<base>/<member>` | that directory; `releases/` lives inside it |
| OCI (planes) | `<base>/<member>/work` | `work/`, `shared/` |

OCI also rejects `${RELEASE_DIR}` and `${BINARY}` in args, because neither path
is bound into the container. `${CONFIG}` is.

## Applying

Infra, by hand, from a checkout with `deployment/nats/nats.conf` present:

```sh
export STRATEGON_API_KEY=str_live_...
python3 scripts/s7n_apply_sets.py --spec deployment/sets/infra.json \
    --binary nats=/tmp/nats-server   --config nats=deployment/nats/nats.conf \
    --binary redis=/tmp/redis-server --config redis=deployment/redis/redis.conf
```

Drop the `--binary` flags when the artifact version has not changed; keep
`--config` when the conf has, and bump `configVersion` in the spec first — the
version is how the agent knows to fetch it.

Planes are the same script with the image and the tag, which is what the
`planes` job in `.github/workflows/release.yml` runs. Running it by hand is the
same command, and is how a rollback to an older tag is done without retagging.

## What a final tag does

1. `test` → `build` (per-arch, by digest) → `merge` (manifest list, moves `:latest`)
2. `planes` — register the image under five families, apply five sets
3. `api` — roll the host compose, with the previous file kept as `docker-compose.bak.<sha>.yml`

A prerelease (`v1.2.3-rc1`) builds and tags images and stops there. Infra is not
in this list on purpose.
