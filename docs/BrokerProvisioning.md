# Who creates the streams, and when

`docs/BrokerPatterns.md` asks what primitives the system needs. This asks a
different question about the same objects: who brings them into existence, at
what point in a deploy, and what happens when two versions disagree about their
shape.

Today the answer is "whichever process gets there first, on connect, using the
constants it was compiled with". That is one answer applied to three kinds of
object that want three different ones.

## Three lifetimes, one mechanism

| | Objects | Created by | Set is known at |
|---|---|---|---|
| **Declared** | the fan-out stream, the log stream, four KV buckets (`state`, `lease`, `counter`, `tapecov`) | `connect()` and `_bucket()`, on first use | deploy time |
| **Instantiated** | one tape stream per recorded feed | `_ensure_tape_stream`, when MD first records that feed | runtime |
| **Ephemeral** | a consumer per read | the operation | per call |

The third tier is settled and correct. Read consumers are deleted when the read
finishes (`_close_reader`, from #83), and `_READ_CONSUMER_IDLE_S` is the backstop
for a process that died mid-read rather than the policy. Live `subscribe` is
core NATS. Since #85 removed the work queue there is no durable consumer left
at all. Nothing to move.

The first tier is the one that wants to be a migration. The second is the
interesting case, and it is not the one it looks like.

## The concrete hazard in tier one

`_ensure_stream` (`transport/nats.py`):

```python
try:
    await self.js.add_stream(config)
except nats.js.errors.BadRequestError:
    # Already there with a different shape — an older node's, or
    # this node's before a constant here changed. Updating is right:
    # the config in this file is what the code assumes, and a stream
    # that disagrees is what silently breaks a retention promise.
    await self.js.update_stream(config)
```

That reasoning is sound while exactly one version of the code is running. It
stops being sound during a rolling deploy, which is the only time two versions
ever run at once — and a rolling deploy is precisely when the constants differ.

Two things follow. **Old and new fight**: each process asserts its own compiled
config on connect, so a stream's shape oscillates for as long as the deploy
takes. And **narrowing is destructive**: an `update_stream` that lowers
`max_msgs` or `max_age` does not schedule a change, it discards what no longer
fits, immediately.

The frequency is worse than "once per deploy". `self._ensured` is per transport
instance, not per process, and `connect()` runs `_ensure_fanout_stream()` and
`_ensure_log_stream()` every time. `apps/api` alone constructs seven `Broker`
objects — one per WebSocket bridge, one per background worker — so a single API
process makes that assertion seven times.

This is the shape of problem migrations exist for: one actor, in order,
versioned, reviewed.

## The precedent is already in the tree

Nothing here needs inventing. `docker-compose.yml` runs `mftik-db-migrate` as a
service gated on `postgres: service_healthy`, before any plane starts. CI has a
step called *Migrations match the models* that runs `alembic upgrade head` and
then `alembic check`, so the declared schema and the code's assumptions cannot
drift apart without a red build.

The broker wants the same two pieces:

- a `mftik-broker-migrate` entrypoint, gated on `nats: service_healthy`, which
  declares the two streams and the four buckets;
- a CI check asserting the declared config equals the module constants, so
  editing `FANOUT_MAX_MSGS_PER_SUBJECT` without editing the declaration fails
  the build rather than silently reshaping a production stream on next deploy.

Both are parameterised by `key_prefix` (`BROKER_KEY_PREFIX`, default `mft`),
which names every stream and bucket a node owns — the same way a migration is
parameterised by a database URL.

## The rule that makes tier two safe

Tape streams cannot be declared: the feed set is venue symbols times recorded
topics, discovered at runtime. But they do not need to be, because **the set is
dynamic and the shape is not**.

`_ensure_tape_stream` takes `maxlen` and `ttl_seconds` per call, and
`transport/nats.py` explains the per-feed stream partly on that basis — that
retention "is per feed in the interface". No caller uses it that way.
`_build_recorder` (`apps/md/app.py`) reads `MD_TAPE_MAXLEN` and
`MD_TAPE_RETENTION_S` once and hands the same two numbers to every feed. The
load-bearing reason for a stream per feed is the other one the comment gives:
sequences are the stream's, so "the newest N records" is subtraction rather than
a scan past every other feed's prints.

So the rule is not "declare everything". It is:

> **A migration owns shape. Runtime may create instances of a declared shape,
> and may never change one.**

Concretely: delete the `update_stream` fallback. A stream that exists with the
wrong shape is a deploy that has not been run, and should fail loudly at
startup naming the difference. Silently reshaping it is the behaviour that makes
the rolling-deploy fight possible in the first place.

## The hazard is not hypothetical — it shipped, and was caught

While this document was open, #88 tried to move `allow_msg_ttl=True` off the
fan-out stream and onto a new log stream. On a fresh server that is correct. On
any server that had ever run the previous code it is not, and the failure is not
the silent narrowing described above — it is worse, and simpler.

Reproduced against a live `nats:2.11-alpine`, creating the stream the way the
previous code declared it and then applying the new config:

```
add_stream    -> BadRequestError code=400 err_code=10058
                 'stream name already in use with a different configuration'
update_stream -> ServerError    code=500 err_code=10052
                 'message TTL status can not be disabled'
```

`_ensure_stream` catches `BadRequestError` and nothing else, so the `ServerError`
propagates straight out of `connect()`. Not a reshaped stream, not lost
messages: **every plane fails to boot**, and only on servers that already have
the stream — which is every server except a developer's fresh one.

That sharpens the argument rather than merely illustrating it. The paragraph
above says `update_stream` applies a change you did not intend to apply. This
says something stronger: **some config changes cannot be applied to a live
stream at all**, and for those the "already there with a different shape, so
update it" fallback has no answer to fall back to. It cannot succeed, so it
raises, from inside `connect()`, on every process at once.

#88 fixed its own instance by putting `allow_msg_ttl=True` back. The mechanism
that let a one-line stream-config edit become a fleet-wide boot failure is
untouched, and `_ensure_stream` still reads exactly as quoted above.

It is also the strongest available argument for the create-not-update rule. A
migration that fails is a deploy step that fails, in one place, before anything
is running. The same failure inside `connect()` is every plane, at once, after
the old ones have already stopped.

## What it costs, stated honestly

`broker_harness` depends on lazy creation. Every test broker gets a `key_prefix`
nobody else has, streams and buckets appear on first use, and everything under
the prefix is dropped on the way out. Making tier one migration-only means every
test broker must run the migration first.

It is probably cheap — a handful of `add_stream` calls — but it is real work in
a file that sixty-odd test modules across six packages share, and it is the only
part of this proposal that is not moving code from one place to another.

That cost is worth naming clearly because it is the argument someone will make
against the whole change, and it should be argued against the real number rather
than an imagined one.
