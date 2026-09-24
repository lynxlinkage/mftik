# STS artifacts — a local object store on each STS

Each STS keeps its own objects. A relative path is one object: opaque
bytes, plus the metadata of that file. The platform does not interpret
the bytes. A model checkpoint is one thing a strategy may store there.

The store is `packages/common/src/mftik/strategy/artifacts.py`. STS
serves it over RPC (`apps/sts/src/mftik_sts/rpc/artifacts.py`); the
HTTP routes are `apps/api/src/mftik_api/routes/artifacts.py`. The CLI
is `mftik artifact`. The page is `/artifacts`.

## What it is

A small local S3, scoped to one STS process and the volume that process
already mounts.

- One key, one object. `write` replaces the key it was given and no
  other.
- The body is bytes. Size, mtime, content digest, and the path are
  metadata beside it.
- A strategy chooses the path. The deploy document does not bind one.

Three stores that already exist stay what they are.

- The registry (`packages/common/src/mftik/registry/`) is the `.py`
  tree STS imports. Its digest is those files. An object is not part of
  a tree and is not imported.
- `remember()` writes into `sts_sessions.st_facts`
  (`packages/common/src/mftik/strategy/base.py`), a JSON column of
  key → value (`packages/db/src/mftik_db/models/session.py`). That is a
  row in the shared database, readable from any plane and from the
  board. An object is bytes on one machine's disk, replaced whole, and
  nothing else can see it without asking that machine.
- The tape (`apps/md/src/mftik_md/tape.py`, read from
  `packages/common/src/mftik/strategy/tape.py`) is the prints MD
  recorded. A strategy reads it. It does not write it.

## Disk

Each STS has its own volume. In `deployment/sets/planes.json`,
`mftik-data` is mounted at `/var/lib/mftik`. `sts-jp` runs on `cp` and
`sts-tw` runs on `yite`. Those two disks are not the same disk.

`MFTIK_DATA` owns exactly two subdirectories of what it names —
`registry/` (`RegistryStore`, `packages/common/src/mftik/registry/store.py`)
and `env/` (`packages/common/src/mftik/environment.py`). Nothing scans
the rest, so an `artifacts/` beside them is a sibling of the registry
tree and not part of it, whether `MFTIK_DATA` is the volume root as in
`docker-compose.yml` or `/var/lib/mftik/registry` as in `planes.json`.

```
/var/lib/mftik/artifacts/weights/model.pt
/var/lib/mftik/artifacts/sessions/{session_id}/weights/model.pt
```

The directory is named by `STS_ARTIFACT_DIR`, in the same way
`STS_EVENTLOG_DIR` names the event log
(`packages/common/src/mftik/strategy/eventlog.py`). Unset, it is
`/var/lib/mftik/artifacts`. The event log stays off until its variable
is set, because a missing audit trail is a legal mode. An object store
that is off cannot accept an upload, so the default here is the path
rather than nothing.

Both compose files set it explicitly, because neither can be left to
that default. `planes.json` puts the event log at
`/var/lib/mftik/eventlog`, on the data volume; `docker-compose.yml`
puts it at `/var/log/mftik/sts`, on a named volume of its own. Event
logs sit beside the registry on the production plane set and nowhere
near it in the dev stack, so "beside the event log" names no single
place and the variable has to say which.

The API process does not open this directory. An event log is already
read by asking the STS that holds the file: `_ask_eventlog` in
`apps/api/src/mftik_api/routes/sts.py` addresses
`Topics.sts(instance)` (`packages/common/src/mftik/protocol/topics.py`,
the subject `sts.{instance}`). An object is asked for the same way.

`_ask_eventlog` also accepts `instance=None` and falls back to the
shared `Topics.STS` subject, which `_eventlog_info` uses when the
instance table is empty or unreadable — deliberately, because a log is
worth reading when the database is not. Artifacts split that fallback
by verb. A read may take it: whichever process answers either holds the
object or does not, and a node with one STS has never had another
subject. A write may not. A `PUT` or a `DELETE` sent to the shared
subject lands on whichever disk answered first, which is not a place
anybody chose, so those refuse with 503 when the instance list cannot
be read.

`weights/model.pt` is the object an operator uploaded.
`sessions/{session_id}/weights/model.pt` is the object that session
wrote. They are different keys. A later session that reads
`weights/model.pt` still receives the upload.

## Paths

A key is a relative path under the store root. `/` separates segments.
Refused: an absolute path, an empty segment (which is also a leading or
trailing `/`, and a doubled one), a `.` or `..` segment, and any NUL or
control character. After joining, the result is resolved and checked to
be under the root, which is what catches a symlink already planted in
the tree — the directory is a mounted volume and the store is not the
only thing that can write to it.

`log_parts` in the event log is the same intent reached a stronger way:
it matches a name against a directory listing rather than validating a
string, so a caller cannot name a file the listing would not have
offered. `read`, `stat` and `rm` can do that too and do. `write` cannot
— it creates a name nothing has listed yet — so for `write` the string
rules above are the whole of the defence, and they are what keeps a key
from naming the registry or the event log sitting next to it.

Keys are compared as bytes. A macOS dev machine will fold
`Weights/Model.pt` onto `weights/model.pt` and Linux will not; the
store does not pretend otherwise, and `ls` shows what the directory
holds.

## Strategy

`self.artifacts` is bound on `Strategy` in `bind()` the way `self.tape`
is, in `packages/common/src/mftik/strategy/base.py`. Every call raises
when the strategy is not bound, the way `remember()` does — a strategy
unit test holds no session, and `self.session_id` is `str | None`
(`base.py`), so without that guard the first thing a test double writes
is the key `sessions/None/weights/model.pt`.

The calls belong in `on_start`, `on_stop`, and `on_rebuild`. Each one
reads or replaces a whole object. They do not belong on a hot hook such
as `on_agg_trade`.

A strategy runs inside the STS process, on the machine holding the
disk, so these are direct file operations and no broker hop. The bytes
move on a worker thread — `asyncio.to_thread`, the same place the event
log does its writing — because a 200 MB checkpoint read on the event
loop stalls the dispatch of every book update behind it.

A process that is killed outright does not run `on_stop`. The last
object that finished replacing is the one still on disk. That is the
same reason `remember()` is written when the fact becomes true rather
than on the way out.

```python
opened = await self.artifacts.read("weights/model.pt")
# None when that key has no object.
# opened.body, opened.size, opened.mtime, opened.digest, opened.path

await self.artifacts.stat("weights/model.pt")
# The same metadata, without the body.

await self.artifacts.write(
    f"sessions/{self.session_id}/weights/model.pt",
    body,
)
```

`read` returns the key it was given. It does not fall back from a
session key to an uploaded key, or the other way around. `stat` is the
call that answers size and mtime without pulling a large body into the
process.

Loading a torch checkpoint is the strategy's work. The store returns
bytes:

```python
model.load_state_dict(
    torch.load(io.BytesIO(opened.body), weights_only=True)
)
```

A cursor, an optimizer state, or the registry digest of the code that
produced the weights, if the strategy wants them, are inside `body`.
The platform does not open a `.pt`.

## Replacing an object

A replace writes a temporary file in the same directory, `fsync`s it,
computes the sha256 as the bytes go by, and `os.replace`s it onto the
key. The metadata published for that key is the metadata of the bytes
just replaced into place. A crash halfway through leaves the previous
complete object, or leaves the key absent if it had never been written.
A reader never observes a short file under the real key.

The temporary file carries the writer's own token in its name —
`.{key}.{token}.part`. Two writers racing one key is allowed and the
last `os.replace` wins silently, which is what "replaces the key it was
given" means; an operator `put` landing while the session's `on_stop`
writes is that case. Sharing one temp path is the only way that race
produces a corrupt object, and a per-writer name is what removes it.

The digest is not stored beside the object. A sidecar file is a second
write the atomic replace does not cover, and a crash between the two
leaves a digest describing the previous body — a digest that is wrong
is worse than one that costs something to get. It is cached in the
process, keyed by `(size, mtime_ns, inode)` of the file it was taken
from, and recomputed on a miss. A replace fills that cache with the
digest it already computed, so the common path hashes once; a restart
empties it, and the first `ls` after a restart pays for a rehash of
what it lists.

## Moving bytes between the API and an STS

One object does not fit in one broker message. NATS defaults
`max_payload` to 1 MiB and `deployment/nats/nats.conf` does not raise
it; a model checkpoint, the case this exists for, is two or three
orders of magnitude over that. The event log already answers this by
slicing: `_EVENTLOG_CHUNK_BYTES` is 256 KiB
(`apps/api/src/mftik_api/routes/sts.py`) and a download is streamed a
slice at a time so neither the API nor the broker ever holds the file.

Reads follow it exactly. `GET` of an object asks
`Topics.sts(instance)` for `(path, offset, length)` and streams each
slice out as it arrives, the way `_eventlog_chunks` does, stopping at
the first failure — the headers have already gone out, so a mid-stream
failure ends the response and says so in the process log rather than
becoming a status code.

Writes need a handle, because the API cannot hand STS a whole body
either. Four messages, on the same instance subject:

- `begin(path)` validates the key, opens `.{key}.{token}.part`, and
  returns the token.
- `chunk(token, offset, data)` appends at that offset. The offset is
  explicit so a retried chunk is idempotent rather than doubled.
- `commit(token)` `fsync`s, takes the digest, `os.replace`s, and
  returns the metadata of what landed.
- `abort(token)` unlinks the part file.

An upload that is neither committed nor aborted — the API process died,
the operator closed the laptop — leaves a part file nobody will finish.
STS sweeps `.part` files idle longer than an hour on the same timer
that reaps orphaned sessions (`reap_loop`,
`apps/sts/src/mftik_sts/app.py`). They are hidden from `ls` by their
leading dot, and they are not objects: no key names them.

## CLI

Subcommands, as `mftik env` is a subcommand of its own
(`packages/common/src/mftik/cli/env.py`, registered in
`packages/common/src/mftik/cli/app.py`). One parser for put and rm
would put a delete next to a typo on a list. The command talks to the
connected profile, and the node forwards to the named STS.

When more than one STS is declared, `--instance` is required. `sts-jp`
and `sts-tw` do not share a disk, so a put with no instance has no
single place to land.

```
mftik artifact put ./model.pt weights/model.pt --instance sts-jp
mftik artifact ls --instance sts-jp
mftik artifact rm weights/model.pt --instance sts-jp
```

The first path of `put` is a file on the laptop. The second is the key
on that STS. `put` of an existing key replaces that object. The CLI
sends the file as one HTTP body; the API is what slices it.

`ls` lists the uploaded tree. It does not list `sessions/`, and `put`
and `rm` refuse a key under that prefix. A tree an operator cannot see
is one they must not be able to delete from: without the refusal, `rm`
takes a running session's checkpoint away and no listing anywhere shows
that it is gone.

## HTTP

The CLI and the UI both use these routes. A session key or an API key
may read and write. A registry key may not: the routes are outside
`REGISTRY_READ_PATHS` in
`apps/api/src/mftik_api/auth/middleware.py`, so `required_scope` stays
`api`. Every one of them writes an audit row — `record_audit` from
`mftik_api.audit_util`, as `download_eventlog` calls it. Reads
included: `download_eventlog` audits a read for the same reason, and
pulling a trained model off a box is the more sensitive of the two
verbs here, not the less.

- `GET /sts/artifacts?instance=sts-jp` lists that machine's uploaded
  objects. `sessions/` is not in the list. Each row is path, size,
  mtime, digest.
- `PUT /sts/artifacts?instance=sts-jp&path=weights/model.pt` uploads
  the body and replaces that key.
- `GET` of that same query downloads the body. Metadata is the list,
  or a stat of the same key.
- `DELETE` of that same query removes the key.
- `GET /sts/sessions/{session_id}/artifacts` lists
  `sessions/{session_id}/` on every declared STS, and each row names
  the instance that answered.

That last one asks all of them rather than the one instance, for the
reason `_eventlog_info` already does. `sts_sessions.instance` is
nullable by design — "an unpinned deploy still is not [pinned]"
(`packages/db/src/mftik_db/models/session.py`) — so for those rows
there is no instance in the row to name. And a null row's placement is
*derived*: `rebuild_interrupted` restores a named row only on its own
instance, but a null row goes to "the STS its TD region derives to"
(`apps/sts/src/mftik_sts/session/manager.py`), which moves when the
credential moves (`td_instance`, `apps/sts/src/mftik_sts/db.py`). Such
a session can write objects on one disk, be interrupted, and write more
on another — the same two-disks case the event log fans out for.

The rows are not merged into one tree, which is where this differs from
`_merged_parts`. Event log parts are slices of one file and have to be
ordered into it; objects are whole and independent, and two instances
holding `sessions/{id}/weights/model.pt` hold two different objects.
The listing says which disk each came from and leaves them apart. An
instance that does not answer contributes nothing and is reported as
not having answered, so its silence is never rendered as an empty
store.

## UI

A nav item, Artifact, beside Registry in
`frontend/src/routes/+layout.svelte`, with the matching branch in the
`href` chain in `frontend/src/lib/components/NavGlyph.svelte`. It is
not a section of Registry and not a section of Settings. Registry is
source trees. Settings is the node extras.

The page starts by choosing an STS instance. The table is the uploaded
objects on that instance: path, size, mtime, digest. Add picks a local
file, takes the key, and PUTs. Delete asks, then removes that key.

The session page
(`frontend/src/routes/strategy/[sessionId]/+page.svelte`) gains a
block that lists the objects this session has written: path, size,
mtime, and the instance that holds each. It does not upload and it does
not delete. Those two actions belong to the catalog page, on the
uploaded tree.
