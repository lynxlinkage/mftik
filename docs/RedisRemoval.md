# Removing the Redis transport

The broker has had two transports since #80 landed the strategy pattern and
made NATS the default. This is the design for taking the second one out, and
the reasoning for doing it now rather than after the first deploy.

It is written before the change so the commits that follow can be read against
it. Nothing here is a code change; the first commit of the PR is this file.

## Why now

The argument for keeping a second transport was that it is the rollback: NATS
is days old, and a store that has carried real traffic is what you flip to when
the new one fails. That argument does not survive contact with the facts.

**Neither transport has run in production.** Not NATS, which landed on
2026-09-08, and not Redis either. A rollback target that has never carried
production traffic is not a rollback target; it is a second unknown. The
`transport` field in `BrokerConfig` says Redis "is what a rollback selects
without a code change", and that sentence has never been true.

**The flip is not cheap even on paper.** Leases, KV state and the tape all live
in the store. `BROKER_TRANSPORT=redis` on a running fleet is not a failover, it
is a cold start against an empty store: every lease gone, every recorded feed's
continuity window reset, every strategy's order book reloaded from the venue.
That is the same blast radius as fixing forward, without the fix.

**What Redis was actually worth was something else, and it is nearly spent.**
It was the reference implementation — the known shape that made the NATS
defects legible. Every one of the four bugs found on 2026-09-08 reads as a
comparison: Redis pipelines `EXPIRE` onto both the tape key and the coverage
key, and `tape_append` on NATS wrote only the tape subject; `HDEL` removes a
field and leaves nothing behind, and both of KV's removals write a marker that
`state_all` then pays for. Having a correct second answer is what turned "this
code looks fine" into a diff.

That value is real, and it is highest at exactly the moment NATS is least
proven — which was last week, not next month. The four defects it helped find
are merged (#82, #83). Holding a whole transport, its suite, and 43% of CI to
keep finding a fifth is not the trade it was three weeks ago.

**And it will never be cheaper to remove than today.** There is no production
compose to edit, no runbook to rewrite, no operator who has learned the flag.
Every week of deferral adds a little.

## What this is not

This is not a change to how NATS behaves. No subject, stream, bucket, key or
timeout moves. The suite that runs on NATS today must run unchanged on NATS
afterwards, and a diff that alters a NATS code path is out of scope for this
PR — if the port left something wrong, it gets its own change, the way #83 did.

It is also not a rewrite of `client.py`. See the first decision below.

## Decisions

**The `BrokerTransport` ABC stays.** `base.py` is 533 lines and almost all of
it is contract: 32 abstract methods and the docstrings that say what each one
promises. That is the specification `nats.py` is checked against, and deleting
it would not delete the obligation, only the place it is written down. It is
also what `client.py` — 728 lines — is written to, so collapsing the interface
means rewriting the broker on top of a concrete class to save an indirection
nobody is paying for. An interface with one implementation is speculative
generality when the interface is invented for a hypothetical second
implementation; this one is the salvaged specification of a real one.

**The registry goes; `build()` stays.** `transport/__init__.py` maps a name to
a class so `BROKER_TRANSPORT` can pick one. With one transport the map is a
one-entry dict and the variable is a way to spell a startup crash. `names()`
and `check_registry()` go with it. `build(config)` stays as a plain factory
returning `NatsTransport(config)`, so no call site changes and the seam is
still there if a second transport is ever justified again.

**`BROKER_TRANSPORT` is removed rather than pinned.** A variable that accepts
exactly one value is worse than no variable: it reads like a choice. It goes
from `BrokerConfig`, `from_env`, `.env.example` and the compose comment.

**Five config fields go with it.** Four say "Redis only" in their own
docstrings — `redis_url`, `health_check_interval`, `command_retries` and
`serve_poll_seconds` — and searching for that phrase is how they were found,
which is why the fifth was nearly missed.

`reply_ttl_seconds` (`BROKER_REPLY_TTL`) is not marked as anything. Its only
reader is `RedisTransport.send_reply`, which does
`redis.expire(inbox, config.reply_ttl_seconds)`: a Redis reply inbox is a key
with a lifetime, and the NATS reply path publishes to the protocol's own reply
subject, where there is no inbox to expire. Left in place it is parsed from the
environment, stored, and read by nothing — the same "a knob that reads like a
choice" this document objects to two paragraphs above, which makes keeping it
inconsistent as well as dead.

The lesson generalises: a field is Redis-only if its *reader* is, and the
docstrings are a convenience rather than the index. `consumer_idle_seconds`
left with the JetStream fan-out consumer; live `subscribe` is core NATS and
reads use `_READ_CONSUMER_IDLE_S`. Everything else transport-neutral stays.

Note that `serve_poll_seconds` is referenced in a comment in
`apps/td/src/mftik_td/session/manager.py` — that comment needs rewriting, not
just the field deleting.

**`redacted_url` stays and its tests are retargeted.** `test_redacted_url.py`
reads like a Redis test because it builds Redis URLs, but the function lives in
`base.py` and `nats.py:389` already calls it — `describe()` on both transports
goes through it. What it tests is that a credential in a connection string does
not reach a log line, and a NATS URL takes `user:password@` too. The tests keep
their assertions and change their fixtures.

**The boundary test stays, including its ban on importing `redis`.**
`test_broker_is_the_only_transport.py` is what keeps store vocabulary out of
the domains, and that is worth as much with one transport as with two — more,
because there is no longer a second implementation to notice a leak. Keeping
`redis` in `FORBIDDEN_MODULES` after the dependency is gone costs one string
and stops the client coming back in through a side door. The module docstring
is written in the past tense of the port and needs rewriting; the checks do not.

**Three test modules go entirely**: `test_redis_transport.py` (264),
`test_broker_retry.py` (111 — Redis pool health checks and `ConnectionError`
retries, neither of which exists on NATS) and `test_broker_poll.py` (47 — it
exists to assert `serve_poll_seconds` bounds a teardown).

**`broker_harness.py` loses its fork.** `transport_name()`, `only_on()` and
`TEST_POLL_SECONDS` go. `MIN_LEASE_TTL` stays as a constant but stops being a
conditional: it is 1.0 because that is NATS' TTL floor (ADR-43), and a test
that watches a lease lapse should still say what it means rather than a number.
`test_nats_transport.py` loses its `pytestmark = only_on("nats")`.

## Inventory

Source, all under `packages/common/src/mftik/broker/`:

| Path | Lines | Action |
|---|---|---|
| `transport/redis.py` | 556 | delete |
| `transport/__init__.py` | 86 | shrink to a factory |
| `config.py` | 97 | drop 5 fields and their env reads |
| `transport/base.py` | 533 | keep; prose only where it compares stores |
| `client.py` | 728 | unchanged |

Tests, under `packages/common/tests/`:

| Path | Lines | Action |
|---|---|---|
| `test_redis_transport.py` | 264 | delete |
| `test_broker_retry.py` | 111 | delete |
| `test_broker_poll.py` | 47 | delete |
| `broker_harness.py` | 257 | drop the transport fork |
| `test_redacted_url.py` | 93 | keep, retarget to NATS URLs |
| `test_broker_is_the_only_transport.py` | 123 | keep, rewrite the docstring |
| `test_nats_transport.py` | 987 | drop the `only_on` marker |

Packaging and infrastructure:

- `packages/common/pyproject.toml` — drop `redis>=5.0`.
- `.github/workflows/tests.yml` — drop the `redis` service container and the
  "Test on the Redis transport" step.
- `docker-compose.yml` — drop the `redis` service, and the `x-broker` anchor
  collapses to NATS alone; the comment explaining why both are waited for goes
  with it.
- `docker-compose.peer.yml` — drop the `redis` port override.
- `.env.example` — drop `BROKER_TRANSPORT` and `REDIS_URL`.
- `justfile` — drop the `test-redis` recipe and fix the comment above `test`.

## What this does to CI

The suite runs three times today: NATS, Redis, and `packages` on the stdlib
loop. The last measured run was 672s, and the Redis pass was 291s of it — 43%.
Removing it leaves ~380s, about six minutes, with no other change and no
coverage lost that is not the Redis transport's own.

Parallelising the remaining passes into a matrix is a separate change and
should stay separate: it is a CI-shape decision that is just as valid with two
transports as with one, and mixing it in here would make a mechanical deletion
hard to read.

## Docs

`docs/Broker.md` is the real work. It is built around a per-operation table
mapping each Redis command to its NATS equivalent, and that table is the
clearest description of the transport we have — it just stops being a
comparison. It becomes a description of what NATS does, keeping the reasoning
in each cell. The section "Three things NATS cannot do that Redis could" keeps
every fact in it (the one-second lease floor, `state_put_many` not being a
snapshot, the restricted character set) under a title that states them as
properties rather than as regressions, because a caller needs to know them
whether or not anything else ever did it differently.

Prose elsewhere that is load-bearing and must change:

- `README.md:43` — "over NATS by default and over Redis if you point it there".
- `docs/Instances.md` — three passages comparing cutover and reply-inbox
  behaviour across the two stores; each collapses to the NATS half.

Two of these are already stale from #80 and this is the moment to fix them,
not new debt from this change:

- `docs/MdHandover.md:19` says `TapeRecorder` writes Redis streams, under a
  heading that promises the facts are "checkable in the tree today". On NATS it
  is a JetStream stream per feed.
- `docs/Alert.md:81` calls `log.{domain}.{stream_id}` a Redis fan-out. The
  topic shape is transport-neutral and unchanged; only the word is wrong.

Deliberately left alone:

- `docs/EventLoop.md` — the uvloop benchmarks were measured against a real
  Redis and the numbers are a record of a measurement that happened. Rewriting
  them to say NATS would be a lie about an experiment.
- `docs/archive/` — archived by definition.
- `docs/Auth.md:133` — "a table rather than Redis" is the rationale for a
  decision taken when Redis was the only store. It explains why the table
  exists and stays true as history.

## Order of the commits

1. This document.
2. Delete the transport and its tests; shrink the registry to a factory; drop
   the config fields and the `redis` dependency.
3. Drop the transport fork out of `broker_harness.py` and the `only_on`
   markers; retarget `test_redacted_url.py`; rewrite the boundary test's
   docstring.
4. Infrastructure: CI step and service, both compose files, `.env.example`,
   `justfile`.
5. Docs.

Two and three are the ones that can break the suite; four and five cannot.
Keeping them apart is what makes a bisect useful if the deletion took something
with it that nothing named.

## What we give up, stated plainly

A differential oracle. If a NATS behaviour is wrong in a way the tests do not
describe, there is no longer a second implementation whose answer differs. The
tests become the only specification, which is a real reduction in the kind of
bug that can be caught by reading — and the four found on 2026-09-08 were all
found exactly that way.

The mitigation is not a second transport. It is that `base.py` keeps saying
what each operation promises in prose, and `docs/Broker.md` keeps saying why
each NATS mechanism was chosen. Those are the parts of the comparison worth
keeping, and neither needs a running Redis to stay true.
