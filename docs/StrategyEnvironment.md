# Strategy environment — node extras on the data volume

A strategy may need third-party packages (`numpy`, `sklearn`, …). Today the
registry copies source, not a venv, and the import gate allows only the
stdlib and the SDK (`mftik`, `mftik_sts`). A third-party import is refused
because it would be a missing module after a pull. The node's Python
environment is whatever the image baked in.

This change keeps the environment at **the node**, not the tree. The Owner
maintains a whitelist via the API. The platform does not review packages.
STS reads installed extras from a directory on the existing data volume, so
a restart does not `pip install`. A **new** registry connect whose extras
this node does not contain is refused. An **already-connected** remote that
later drifts stays connected; the incompatibility is a deploy (and rebuild)
error on the tree that needs what is missing.

Nothing here is built yet. The files and endpoints named below are what the
change rests on, all of them checkable in the tree today.

## Epic

**Owner-curated node extras, persisted on `MFTIK_DATA`, checked at connect
for new remotes and at deploy for everything else.**

### The problem

Strategies that need scientific Python cannot enter the registry. Relaxing
the gate without a node-level promise would make `mftik check` on a laptop
with numpy pass and a pull onto a bare node fail. Installing into the STS
process at boot would block `load_local_registry` and therefore
`STS_REBUILD_ON_BOOT`: a tree that `import numpy`s is skipped, and rebuild
logs "no strategy named X in this build". Putting extras in
`packages/common` would land them in every service image.

### The shape that stays

- The registry still copies `.py` and optional `strategy.yml`. Digest is
  still source. No venv ships in a tree.
- STS still `import`s trees into its own process (`mftik.registry.load`).
- One image, services differ by `command:`. Extras live on the data volume,
  not in `/app/.venv`.
- `gate.py` still scans without executing the tree. Dynamic imports stay
  refused. Shadowing `mftik` / `mftik_sts` stays refused.

### Non-goals

- Per-strategy venvs or worker processes. Two numpys in one interpreter
  are impossible; isolation is a later epic.
- A platform catalog of allowed PyPI names. The Owner's stamp on the
  volume is the whitelist. The gate's job is declaration and presence,
  not taste.
- A Postgres table for extras. The registry already lives on
  `MFTIK_DATA`; the env list is the same kind of node-local state.
- `pip` / `uv` on every STS start. Apply is a separate, restart-bounded
  action. Crash restart reads the volume.
- Auto-installing a peer's extras on connect. Importing them is an Owner
  action that first returns a diff and only installs after confirm. A
  registry key cannot write the stamp.
- Reviewing or pinning the whole of PyPI.
- Async apply (`202` + poll). That reintroduces a desired queue. Writes
  are synchronous with a hard cap; Caddy's timeout is raised for those
  paths.

### Invariants

1. **The stamp is the list, and a generation is a directory.** Handshake,
   connect, deploy, and the node-side gate read
   `$MFTIK_DATA/env/applied.json`. Not a listing of `site-packages/`,
   not Postgres. Apply writes a **new** `gen-{N}/` tree; it never
   mutates the generation STS is using. Failure leaves the previous
   stamp, the previous directory, and the previous `current` symlink.
   `applied.json` is written with `tmp` + `os.replace`.
2. **Restart does not install.** `$MFTIK_DATA/env/` outlives the container.
   STS puts the generation **the stamp names** on `sys.path` —
   `gen-{generation}/site-packages`, created empty on first boot — and
   loads. Not the `current` symlink: commit writes the stamp and
   retargets the symlink as two steps, and a process that read the
   symlink could report one set of extras while importing another.
   Missing, ABI-mismatched, or absent-from-disk overlay = stdlib +
   SDK, same as today — deploy then uses `incompatible_environment`,
   not a raw `ImportError`.
3. **A tree must declare what it imports.** Third-party `import X` is
   legal only if `X` is in the union of `requires` on every `Strategy`
   subclass in the tree. Undeclared extras are refused offline, even
   when the node has them. A name in `requires` must not also be a
   module the tree ships.
4. **New connect compares names, not pins.** Remote applied extras
   **names** must be ⊆ this node's applied extras names. A version
   difference is a warning on `diff`, not a connect refusal. Failure
   (missing names) does not write `remotes.toml`.
5. **Already-connected drift is per-tree, at deploy.** Sync may copy a
   tree this node cannot run. `POST /sts/deploy/{type}` and rebuild refuse
   with `incompatible_environment`, not `unknown_strategy`.
6. **Registry keys do not mutate environment.** Same reason they do not
   `POST /registry/v1/add`. `GET /registry/v1/info` may show extras:
   they are not a secret, and a peer has to fail fast before it needs a
   key — same as `mftik_version` today (`docs/Auth.md`).
7. **A live session owns the interpreter.** Apply that would change or
   remove an already-importable extra is **409** while any STS session
   is `live`, unless the Owner sends an explicit force flag. Adding a
   name that is not yet on `sys.path` is not that case. The API is
   what decides this (ENV-5) — the installer has no broker and cannot
   see sessions — and an STS that does not answer counts as live.

### Current → target

```
today
  tree ──gate──► stdlib + mftik + mftik_sts
  STS  sys.path = image venv
  connect  handshake protocol only
  deploy   resolve() or unknown_strategy

target
  tree ──gate──► stdlib + SDK + declared requires (two-pass scan)
  add (own) / new connect ──► requires ⊆ applied extra *names*
  already-connected pull ──► store the tree anyway
  STS  sys.path = image venv + $MFTIK_DATA/env/gen-{stamped}/site-packages
  deploy / rebuild ──► requires ⊆ applied extras
                       else incompatible_environment
```

---

## Model

### One list, on the volume

There is no `node_environment` table. Strategies, `remotes.toml`, and
now extras are all node-local files under `MFTIK_DATA`. A second copy
in Postgres would drift from the stamp handshake actually reports.

| | Stamp (`applied.json`) |
|---|---|
| Who writes | Owner API, only after a successful apply (atomic replace) |
| Who reads | STS, API, `/info`, connect, deploy |
| Empty / missing | legal; node is today's image |
| Failed apply | previous stamp, previous `gen-{N}`, previous `current` |

`env_generation` increments on a successful apply. Mutations are
apply-then-stamp against a **new** generation directory. There is no
desired queue.

### Volume

API and STS already share `mftik_data` as `/var/lib/mftik`
(`docker-compose.yml`, `packages/common/src/mftik/cli/templates/docker-compose.yml`).
The overlay is a directory under that tree, not a second named volume,
so a node that already persists the registry persists extras the same way.

```
$MFTIK_DATA/
  registry/                 unchanged
  env/
    applied.json            {generation, packages, python, platform, bytes}
    current                 symlink -> gen-3/site-packages; for a person
    gen-3/site-packages/    the generation the stamp names
    gen-2/site-packages/    previous; live process may still hold it
    apply.lock              fcntl.flock, not O_EXCL
```

`applied.json` is the env list. Do not derive extras by listing
directories. A failed `--target` into a live tree would leave "stamp
says numpy, disk is half a numpy"; writing `gen-{N}` and switching
only on success makes that structurally impossible.

`current` points at the generation's `site-packages` itself, not at
`gen-{N}`, so a person who follows it lands on an importable directory.
It is **not** what STS puts on `sys.path`. Commit publishes by writing
the stamp and retargets the symlink afterwards as a convenience, so the
two can disagree for an instant — and the half that matters is a
removal, where a reader following the symlink would report a package it
can no longer import, pass the deploy check, and die on
`ModuleNotFoundError`. Deriving the path from `generation` moves the
extras a process reports and the extras it can import together.

STS, on start:

1. Ensure `gen-0/site-packages` and the `current` symlink exist if this
   is a bare node. Read the stamp, then insert
   `gen-{stamp.generation}/site-packages` on `sys.path`. A generation the
   stamp names but the disk does not have is treated as no extras at all
   — reporting what this process cannot import helps nobody. Then
   `importlib.invalidate_caches()`.
2. Read the stamp **into memory**. It is read at boot and on every
   reload, never per deploy: one process must not answer two different
   extras questions a second apart because a `PUT` landed between them.
   ENV-8 compares against this copy, and it is the copy `/info`
   reports. If the file is missing, extras are empty (the empty
   `current` is fine).
3. If the stamp's `python` / `platform` do not match this interpreter
   (`sys.version_info[:2]`, `sysconfig.get_platform()`), **do not**
   treat extras as present: log, keep the empty-or-old path behaviour
   as "no extras", and let deploy use ENV-8. Do not import a 3.12
   wheel under 3.13 and call it an unknown strategy.

It does not spawn `pip`. Image upgrades that change Python or arch
invalidate the overlay via the tag, not via a silent `ImportError`.

Two API workers must not apply at once. `apply.lock` is
`fcntl.flock` on a file opened for the duration of apply. A dead
API container releases the lock; an `O_EXCL` cookie would not.
The second request **409**s (do not wait out a ten-minute install
behind Caddy).

`applied.json` is written to a sibling tmp and `os.replace`d onto
the name. `current` is retargeted the same way (symlink in a tmp
name, `os.replace`).

Keep the previous `gen-{N-1}` until the next successful apply so a
live process that still has that path in `sys.modules` is not
unlinked underneath. Older than that may be deleted.

Commit records the byte size of `env/` in the stamp, and `GET
/environment` reports that number rather than re-walking the tree —
a gigabyte overlay is not a free `du` on every poll. It is there so
the Owner can see numpy/torch sitting on the same volume as the
registry.

### `requires` on the class

A sequence of import names, not the string attribute
`_class_str_attr` already reads for `requires_mftik`
(`packages/common/src/mftik/registry/gate.py`). That helper only
accepts `ast.Constant` strings. ENV-1 adds `_class_str_seq`.

```python
class Signal(Strategy):
    name = "ml_signal"
    requires_mftik = "0.2.0"
    requires = ("numpy", "sklearn")
```

Names are **import names** (`sklearn`, not `scikit-learn`). A
`requires` entry that is not a Python identifier is refused with a
sentence that says so — `scikit-learn` fails at `mftik check`, not
at push. Apply may carry a `dist` when the PyPI name differs; that
is translation, not a catalog.

`check_files` returns every subclass it found. `pick_class`
(`inspect.py`) still refuses more than one. Until then, the extras
set used for the import pass is the **union** of every subclass's
`requires`. A tree that will later be refused for two classes must
not have a different extras answer at the gate.

### Two checks, two moments

| Moment | What is compared | Failure |
|---|---|---|
| `mftik check` (offline) | third-party imports ⊆ union of `requires`; names are identifiers; no `requires` name is a local module | gate refusal, exit 1 |
| Own `add` / `mftik push` | `requires` ⊆ **applied extra names** | 400, tree not stored |
| New `connect` | remote extra **names** ⊆ local applied extra names | 400, remote not remembered |
| Sync of an existing remote | static gate only (declaration, no dynamic import) | a single bad tree is skipped; the remote stays |
| Deploy / rebuild | that tree's `requires` ⊆ local applied extra names | `incompatible_environment` |

First connect compares **the peer's advertised extra names**, not
pins and not the union of every public tree. Deploy compares **this
tree**. A peer that later installs `torch` for a strategy this node
will never run must not block deploys of trees that only need
`numpy`. A peer that bumps `numpy` 2.2.1 → 2.2.2 must not refuse a
new connect; `diff_remote` may list the version drift as a warning.

### Auth

| Actor | Environment |
|---|---|
| Session / API key | read and write |
| Registry key | read extras only via `GET /registry/v1/info` |
| Anonymous | `GET /registry/v1/info` (already public) |

Writes go under `/environment`, which is not in `REGISTRY_READ_PATHS`
(`apps/api/src/mftik_api/auth/middleware.py`). A registry key that can
write the stamp would install packages into this node's STS.

### Threat model

`PUT /environment` runs an installer whose result lands on
`sys.path` of the process that holds venue keys. That is arbitrary
code in STS. The Owner can already `push` an arbitrary tree that
STS will `import`, so the risk is the same class of act — accepted
for that reason, not because extras are "just metadata".

`POST /environment/import` is **not** the same. The package names
come from a peer. A typosquat on their stamp becomes this node's
`sys.path`. Import therefore returns a diff and does not install
until the Owner confirms (ENV-9).

The overlay also goes **ahead** of the stdlib and the SDK on
`sys.path`, so an extra installed as `json` or `mftik` would shadow
the real module for every session in the process. `gate.py` refuses
a strategy that declares one in `requires`; the write API refuses
the same set of names, which is the layer that matters — that is
where a person types it. One list, `PROVIDED_BY_NODE`, is read by
both. Apply itself:

- pins with `==` (no bare names, no ranges at install time);
- `uv pip install --target … --only-binary=:all:` so a sdist
  `setup.py` never runs as the API user;
- talks to whatever `UV_INDEX_URL` names. Air-gapped nodes set
  that to an internal index or they cannot apply; the 5xx says so.

### Operations

- **Egress.** Apply needs the API container to reach the index.
  Offline nodes are not a special protocol; they fail apply with
  the installer stderr.
- **Time.** A write is synchronous. Cap it (ten minutes is the
  starting number). The published Caddy in
  `packages/common/src/mftik/cli/templates/docker-compose.yml` must
  allow that for `/environment` — the default reverse-proxy
  timeout will otherwise close the socket while `uv` is still
  running, and the client will retry a write that may still be
  holding `apply.lock`. `202` + poll is a later epic.
- **Disk.** numpy/torch are gigabytes on the same `mftik_data`
  volume as the registry. No quota in this epic. `GET
  /environment` includes the size of `env/`. Keep one previous
  generation; delete older trees after a successful switch.
- **ABI.** Overlay wheels match the image (`python:3.12` today,
  `Dockerfile`). A 3.13 image or an arm64 overlay on amd64 is a
  stamp mismatch, not a mystery import. `GET /environment` reports
  it as `abi_ok: false` with both tags, and the remedy is a fresh
  apply — an image bump is a re-install, and the Owner has to be
  told that rather than left to infer it from empty extras.

---

## Tickets

Each ticket leaves the tree shippable. Later tickets may be empty
behaviour (generation 0, no extras) so earlier ones can merge.

### ENV-1 — Gate reads `requires` (two-pass)

**Scope.** `packages/common/src/mftik/registry/gate.py` — this is a
rewrite of `check_files`, not a parameter on `_check_module`.
`inspect.py` only if the inspected record should surface `requires`.
`packages/common/tests/test_registry_gate.py`,
`packages/common/tests/test_cli_check.py`.

**Problem.** The only third-party answer is "never". A tree cannot
declare the extras it will need, so later node-side checks have nothing
to compare, and `mftik check` cannot distinguish "forgot to declare
numpy" from "numpy is forbidden forever".

`check_files` today walks files in path order and raises on the
first illegal import (`gate.py`). `requires` lives on a class that
may be in a later file. `helpers.py` with `import numpy` is refused
before `strategy.py` is parsed. Passing a set into `_check_module`
does not fix that.

`_class_str_attr` (`gate.py`) only returns `ast.Constant` strings.
`requires = ("numpy", "sklearn")` is a tuple; the existing helper
cannot read it. The phrase "same shape as `requires_mftik`" is
false for the reader.

**Solution.** Two passes over the tree:

1. Parse every `.py` (UTF-8 / syntax still fail here). Collect
   `Strategy` subclasses and each class's `requires` via a new
   `_class_str_seq` (tuple, list, or a single string). The extras
   allowlist for pass 2 is the **union** of those sequences. More
   than one subclass is still `pick_class`'s refusal
   (`inspect.py`); the gate must have a defined extras set before
   that runs.
2. Walk imports as today. `_check_module` allows a top-level name
   when it is in that union. Still allowed: stdlib, `HOST_PACKAGES`,
   files in the tree. Still refused: names in neither, `importlib` /
   `__import__`, shadowing the SDK.

Also refuse:

- a `requires` entry that is not a Python identifier, with a
  sentence that names import-name vs dist-name (`sklearn`, not
  `scikit-learn`);
- a `requires` name that is also a local module (`_check_module`
  returns early for locals today — a tree that ships `numpy.py` and
  declares `requires = ("numpy",)` would pass the gate and load
  the tree's file instead of the overlay).

`mftik check` stays offline. It does not ask a node. It does not look
at the laptop's site-packages.

**Verify.**

- `import numpy` without `requires` → refused (existing test).
- `requires = ("numpy",)` and `import numpy` in the **same** file →
  `check_files` returns the class with `requires == ("numpy",)`.
- `helpers.py` imports numpy, `strategy.py` (sorts later) declares
  `requires = ("numpy",)` → **passes**. Reverse the declaration
  (helpers has the class, strategy imports numpy) → still passes.
  Helpers import numpy and **no** class declares it → refused.
- `requires = ("numpy",)` and `import sklearn` → refused.
- `requires = ("scikit-learn",)` → refused as not an identifier,
  message mentions import vs dist.
- Tree contains `numpy.py` and `requires = ("numpy",)` → refused
  (shadow).
- `requires = ("numpy",)` as a list, and as a single string
  `"numpy"` → both read.
- `import importlib` still refused when `requires` lists it.
- `mftik check` on the undeclared tree is exit 1; on the declared
  tree, exit 0 (naming and `on_initialized` unchanged).
- Bundled strategies and existing fixtures do not set `requires`;
  they still pass.

### ENV-2 — Registry records carry `requires`

**Scope.** `AddedStrategy` in
`packages/common/src/mftik/registry/store.py`,
`_scan_tree` / `add`, `apps/api/src/mftik_api/schemas.py`
(`RegistryStrategyOut` and detail), list/add responses,
`packages/common/tests/test_registry_store.py`,
`apps/api/tests/test_registry_add.py`,
`packages/common/tests/test_registry_sync.py`.

**Problem.** Deploy and connect must compare a tree to the node without
executing it. `requires_mftik` is already on the record; extras are not.
A scan that only lives in the AST is lost after `add` unless it is
stored beside the digest.

**Solution.** `AddedStrategy.requires: tuple[str, ...]` (empty default).
Filled from the class at `add` and `_scan_tree` (the chosen class after
`pick_class`, so the stored tuple is that class's `requires`, not a
union of a tree that `inspect` would have refused). Wire it on
`RegistryStrategyOut` so a listing and a peer's detail both show it.
Digest stays the `.py` files only — `requires` is derived, like
`requires_mftik`.

**Verify.**

- `add` of a tree with `requires = ("numpy",)` returns that tuple.
- Re-scan of a tree already on disk (`_scan_tree`) recovers the same.
- A tree without the attribute stores `()`.
- Sync fixtures that build a fake peer include `requires` (empty is
  fine) so later tickets do not invent a second list shape.

### ENV-3 — Volume stamp and generation directories

**Scope.** Read/write helpers for `$MFTIK_DATA/env/applied.json`,
`current`, `gen-{N}/`, and `apply.lock` in `packages/common`. Tests
on a temp `MFTIK_DATA`. No Alembic, no `packages/db` model.

**Problem.** Handshake, connect, and deploy need one list of extras
this node actually has. Listing `site-packages/` is wrong after a
removal. A Postgres table would be a second copy. In-place
`--target` into a shared tree plus "leave the stamp on failure"
still leaves a half-written numpy next to a stamp that claims the
old set — and next to a live `sys.path`.

**Solution.** Layout as in Model. Stamp shape:

```json
{
  "generation": 3,
  "python": [3, 12],
  "platform": "linux-x86_64",
  "bytes": 412313344,
  "packages": {
    "numpy": {"version": "2.2.1", "dist": "numpy", "source": "manual"},
    "sklearn": {"version": "1.6.1", "dist": "scikit-learn", "source": "peer:other"}
  }
}
```

`generation` 0 / missing file = no extras. `packages` keys are import
names. `dist` is the PyPI name when it differs — it is what an
installer is given, so it has to survive on the record and over the
wire, not only in the request that first asked for the package.
`bytes` is the size of `env/`, measured once at commit. Readers that
only need names use the keys.

`fcntl.flock` on `apply.lock` (shared named volume, same host; a
killed API process releases it). Stamp and `current` use tmp +
`os.replace`. Helpers expose: read stamp, begin generation `N+1`
directory, commit (measure `bytes`, replace stamp, retarget
`current`, drop `gen-{N-2}`), abort (rm the new directory, leave
stamp).

**Verify.**

- Missing file → extras `{}`, generation `0`.
- Round-trip write/read preserves versions, `source`, `python`,
  `platform`.
- Commit is atomic: a reader never sees stamp generation 3 pointing
  at a missing `gen-3`.
- Abort after a partial `gen-4/` leaves stamp and `current` on 3.
- Leftover files in an old generation that the stamp does not name
  are not reported as extras.
- No migration in `packages/db`. Fresh seed unchanged.

### ENV-4 — Apply into a new generation

**Scope.** Installer function in `packages/common` (so the API can
call it). **Not** a `pip` call inside `amain`. Tests that plant a
stub package on a temp volume; no PyPI.

**Problem.** If STS installs at start, trees that need extras fail
`load_class`, drop out of `_REGISTRY`, and rebuild treats them as
withdrawn. Healthchecks time out while wheels download. The image
venv grows mutable and diverges from the GHCR digest. In-place
`--target` on failure or DELETE poisons the tree a live session
is importing from.

**Solution.** Apply takes the **target package set** (replace or
upsert). Pins at install time are `==`. Flags:
`--only-binary=:all:`. Index is `UV_INDEX_URL`.

1. Refuse a set that changes or removes a name already in the stamp
   unless the caller passes `allow_disruptive`. The installer does
   not decide this and must not try: it lives in `packages/common`,
   has no broker, and cannot see a session. ENV-5 asks STS and
   passes the flag. Adding a name that is not in the stamp is never
   disruptive and needs nothing.
2. `fcntl.flock` `apply.lock`. Second caller 409s.
3. Create `gen-{N+1}/site-packages` (empty). `uv pip install
   --target` **that** directory. Do not write `/app/.venv`. Do not
   write the live `current` tree. On installer failure: abort
   (ENV-3), unlock, raise with stderr.
4. On success: prune, measure, then commit the stamp (`generation`,
   `python`, `platform`, `bytes`, packages from `importlib.metadata`
   against that target). Writing the stamp **is** the publish, and it is
   the last thing that can fail an apply; retargeting `current` after it
   is cosmetic and must never raise, or `__exit__` would go on to abort
   an overlay the stamp is already naming. Unlock.
5. **Do not** restart STS here. ENV-4a tells STS to see the new
   generation. A `force`d change or removal still needs a process
   restart — the old modules stay in `sys.modules` and no reload
   evicts them — and nothing in this stack can restart a container.
   So apply returns a **restart-required** flag and ENV-5 carries it
   to the Owner. It must never be implied by a bare 200: `force` is
   the path with no 409 to read it off, and it is the path where a
   live session is running code the stamp no longer describes.

Hard cap on the installer (ten minutes). Kill and abort on expiry
so a Caddy retry does not stack.

**Verify.**

- Successful apply writes `gen-1/site-packages/foo` and a stamp
  that names 1; `current` resolves to `gen-1/site-packages`. Second
  success is
  `gen-2`; `gen-0` or `gen-1`'s predecessor is removed per the
  keep-one rule.
- Installer failure: stamp, `current`, and `gen-N` unchanged; the
  failed directory is gone.
- A set that changes or removes a stamped name without
  `allow_disruptive` → raises before the `flock`, no directory, no
  subprocess. Whether a session is actually live is ENV-5's
  question and is tested there.
- STS boot is not in this ticket's installer tests; ENV-4a covers
  `sys.path`. Assert apply itself never runs from `amain`
  (subprocess call count 0 on boot, in 4a).

### ENV-4a — STS puts the stamped generation on `sys.path` and reloads it

**Scope.** `apps/sts/src/mftik_sts/app.py` before
`load_local_registry`; `apps/sts/src/mftik_sts/rpc/registry.py` (or
a sibling handler); protocol if the reload payload grows.
`importlib.invalidate_caches()`. Tests in `apps/sts/tests/`.

**Problem.** ENV-4 step "caller restarts STS or asks it to reload"
had no owner. `amain` today only loads the registry. A bare node
has no `site-packages` at first boot; an `if exists: sys.path.insert`
never inserts, and the first apply is invisible until restart.
`site-packages` that appear after start also need
`invalidate_caches()` or Python keeps the negative cache.
`sts.registry.reload` (`apps/sts/src/mftik_sts/rpc/registry.py`)
only runs `load_local_registry`. PUT `/environment` returning 200
while deploy still answers `incompatible_environment` is the
failure this ticket exists to prevent.

Sessions are `StsSession` objects in this process
(`session/manager.py`), not workers. Swapping `current` under a
live import of `numpy.linalg` is a `ModuleNotFoundError` on a
real position. That is why ENV-5 409s when changing extras with
live sessions and ENV-4 refuses without the flag; this ticket
still has to make *new* extras visible without a restart.

**Solution.**

- Boot: create `gen-0` and `current` if needed, read the stamp, and
  insert `gen-{stamp.generation}/site-packages`,
  `invalidate_caches()`, then `load_local_registry`. Two cases put a
  fresh empty directory on the path instead and treat extras as
  empty: the stamp's `python` / `platform` do not match (a 3.13
  process must not load 3.12 wheels — leave the overlay on disk for
  a matching image), or the stamp names a generation that is not
  there. In both, `extras_names()` is empty regardless of what the
  stamp lists; deploy then answers `incompatible_environment`.
- Reload: extend `sts.registry.reload` **or** add
  `sts.env.reload` that (1) re-reads the stamp into memory, (2)
  ensures `current` is on `sys.path`, (3) `invalidate_caches()`,
  (4) `load_local_registry()`, (5) returns the qualified keys plus
  the generation it now believes. Prefer extending the existing
  RPC so `add` and apply share one "did STS pick it up" answer.
  The scan stays on the event loop for the same reason the
  current handler comments: it mutates `sys.modules`.

  Because the path follows the stamp, this reload is what makes a
  committed generation importable at all — `sys.path`, the negative
  import cache, `_REGISTRY`, and the in-memory stamp all move here,
  in one moment rather than four. That is the point: a node that
  skipped it reports the old extras *and* imports the old overlay,
  which is a consistent node one generation behind, not a node
  lying about itself.

- Step (5)'s generation is what ENV-5 compares against the stamp
  to answer `restart_required`. A reload that lands makes them
  equal; a `force`d change leaves them equal too — which is why
  `restart_required` is set by apply, not inferred from this
  number alone.

**Verify.**

- Boot on a bare volume: `gen-0/site-packages` exists and is on
  `sys.path`, `current` exists but is *not* the path entry, no
  installer ran, bundled strategies load.
- Boot with a planted `gen-1` + stamp + `current` → numpy stub
  tree loads.
- Stamp names `gen-2` while `current` still points at `gen-1`
  (crash between the two writes): `sys.path` is `gen-2`, extras are
  `gen-2`'s. Point the symlink anywhere; nothing changes.
- Stamp names a generation that is not on disk: extras empty, that
  path not inserted, log line exists.
- Boot with a stamp whose `python` is `[3, 11]`: extras treated
  empty, overlay not imported, log line exists.
- After apply (test plants `gen-2` and commits), the reload RPC
  makes a previously unloadable numpy tree appear in `loaded`.
  Without the reload, it does not.
- The same reload moves the in-memory stamp: extras reported
  before it are the old set, after it the new one. This is the
  assertion that stops the reload being written as a no-op
  because `current` "already works".
- `amain` still never calls the installer.

### ENV-5 — Environment API and handshake extras

**Scope.** `apps/api/src/mftik_api/routes/` (new router),
`schemas.py`, mount in `main.py`, `RegistryInfoOut` +
`mftik.registry.protocol.handshake_info`,
`docs/Auth.md` (the `/info` row that says "versions only"),
`apps/api/tests/test_auth_registry_keys.py`, new API tests. The
router needs `BrokerDep`: it asks STS for live sessions (invariant
7) and sends the ENV-4a reload, so it is not a pure filesystem
route.
Audit writes for mutations (`docs/AuditIdentity.md` vocabulary: `via`,
not just `user_id`). `just openapi` — `contracts/openapi.json` is
generated; `just check-contracts` and
`.github/workflows/{tests,publish,release}.yml` fail the job when
it is stale. That is this ticket, not ENV-10.
`packages/common/src/mftik/cli/templates/` Caddyfile / compose
timeout for `/environment`.

**Problem.** The Owner has no way to maintain the whitelist. Peers
cannot see what this node actually has. `/info` is the handshake
`connect_remote` already GETs. Apply without telling STS is a
200 that does not deploy.

**Solution.**

```
GET    /environment                 stamp, size, ABI status, restart flag
PUT    /environment                 replace the set, apply, reload STS, new stamp
POST   /environment/packages        upsert one name, apply, reload, new stamp
DELETE /environment/packages/{name} remove, apply, reload, new stamp

PUT is a full replace. Upsert, delete, and import-confirm merge
into the stamp *after* `apply.lock` is held — two tabs that both
read `{numpy}` and add different names must not silently drop
one of the adds.
```

`GET /environment` answers three things the Owner cannot see
anywhere else:

- the stamp — packages, generation, `python` / `platform` — and
  `bytes` for `env/`, read from the stamp rather than re-walked
  (ENV-3 measures it at commit);
- `abi_ok`. A stamp whose tags do not match the running image
  reports `false` and names both. Without it, a 3.12 → 3.13 image
  bump reads to the Owner as "my extras vanished", `/info` says
  `extras: {}`, every deploy says `incompatible_environment`, and
  the remedy — re-apply, which rebuilds the generation against the
  new interpreter — is nowhere on screen;
- `restart_required`. A read-only `sts.registry.generation` RPC
  answers with the generation STS believes it is on. GET must not
  send `sts.registry.reload` — opening Settings must not re-import
  every tree in a process that may have live positions. When STS
  trails the stamp — a `force`d change, or a write-time reload that
  never landed — say so, and say the fix is restarting the STS
  container. Until then a live session keeps running the modules it
  already imported.

`DELETE` also answers with the stored trees whose `requires` the
removal breaks (ENV-2 put `requires` on the record). The Owner
learns which strategies stopped being deployable at the moment of
the removal, not the next time somebody opens the picker.

Each write **is** apply (ENV-4) then ENV-4a's reload, analogue of
`POST /registry/v1/add` reporting `loaded`. There is no
`POST /environment/apply`. Failure before commit: previous stamp,
generation unchanged, 4xx/5xx with installer stderr. Reload
failure after a successful commit: 200 with a `loaded`-style
field false and a sentence (files/stamp are on disk; STS will
see them on next restart) — same posture as add when STS does
not answer.

`force` query/body on writes that change or remove. The API is
what knows, because it has the broker: it asks STS for `live`
sessions the way `routes/sts.py` already does
(`ListSessionsRequest(domain="sts", status=...)`,
`apps/api/src/mftik_api/routes/sts.py`), then passes
`allow_disruptive` into ENV-4. Three rules:

- Sessions live, no `force` → 409 with the session ids, installer
  not started.
- **STS does not answer** → 409 as well. Fail closed. An STS that
  is restarting is exactly when a removal is least safe, and a
  silent broker must not be read as "nobody is trading".
- **Re-check between install and commit.** The install takes
  minutes, and `apply.lock` does not stop a deploy from landing
  during it — a session that appeared meanwhile would have
  `current` swapped underneath it at commit. If one did, abort the
  generation (ENV-3; the directory is disposable, nothing was
  published) and answer 409. `force` skips both checks and returns
  `restart_required`.

Session or API key. Registry key: 403 on every path (not under
`/registry/v1/strategies`, so `required_scope` stays `api` and a
registry key fails as it does on `/private`).

`handshake_info()` grows:

```json
{
  "protocol": "mftik.registry",
  "protocol_version": 1,
  "protocol_min": 1,
  "mftik_version": "0.0.13",
  "extras": {
    "numpy": {"version": "2.2.1", "dist": "numpy"},
    "sklearn": {"version": "1.6.1", "dist": "scikit-learn"}
  },
  "env_generation": 3
}
```

`extras` and `env_generation` come from `applied.json` (ENV-3),
and only if this process's python/platform match the stamp.
Absent or mismatched → `extras: {}`, `env_generation: 0`. Old
peers ignore unknown keys; `check_handshake` does not require
them. Do not bump `PROTOCOL_VERSION` for an additive field.

Each `extras` value is an **object**, not a bare version string.
`dist` is the reason: the key is an import name, and ENV-9 hands
what it fetched to an installer. `sklearn` is not on PyPI. A peer
that published `{"sklearn": "1.6.1"}` would be advertising a
package no node could install, which is the same as advertising
nothing. `source` is *not* published — where this node got a
package is its own bookkeeping, and the peer it names may be one
the reader cannot reach. The names themselves are public (connect
compares keys). Exact pins are not: an anonymous `GET /info`
keeps the keys and drops `version` / `dist` (`"numpy": {}`). A
registry key, API key, or session gets the objects above.
`connect_remote` only needs the names, so a first connect without
a key still works.

`RegistryInfoOut` accepts both shapes: a string value from a node
that predates this is read as `{"version": v, "dist": <the key>}`.
That guess is right for `numpy` and wrong for `sklearn`, so ENV-9
marks a guessed `dist` in its preview rather than installing it
quietly.

`RegistryInfoOut` must accept the new fields (optional with defaults)
so a peer running this code can still talk to a node that has not
upgraded.

**Verify.**

- `GET /environment` on a fresh node: packages `{}`, generation `0`,
  size present.
- PUT with a stub installer: GET and `/info` extras match; generation
  is 1; reload was sent; `loaded` true when the fake STS answers.
- PUT, installer fails: generation `0`; no reload.
- PUT, commit ok, STS silent: stamp is 1; response says STS did
  not reload (add's wording).
- Change/remove with a live session, no `force`: 409, session id
  listed, installer never started.
- Same, but STS does not answer the list RPC: also 409. Fail
  closed, installer never started.
- A session appears between install and commit: generation
  aborted, stamp and `current` unchanged, 409.
- `PUT` naming a package `json`, `logging`, `mftik`, or any other
  name the node already provides: **400** before the lock, installer
  never started.
- Same write with `force`: 200, stamp moves, `restart_required`
  true.
- Stamp tags do not match the interpreter: `abi_ok` false with
  both tags; a fresh apply returns it to true.
- `DELETE` of an extra a stored tree requires: response names
  that tree.
- Registry key: GET `/registry/v1/info` sees extras; GET/POST
  `/environment` is 403.
- Unauthenticated `/environment` is 401 when the gate is on.
- Mutation writes an audit row with `via`.
- `just check-contracts` is clean after `just openapi`.

### ENV-6 — New connect refuses missing extra *names*

**Scope.** `packages/common/src/mftik/registry/sync.py`
(`connect_remote`), `protocol.py` if comparison lives next to
`check_handshake`, API `POST /registry/v1/remotes`,
`packages/common/tests/test_registry_sync.py`,
`packages/common/tests/test_registry_remotes.py`.

**Problem.** `connect_remote` handshakes protocol versions, remembers
the remote, then copies every public tree. A peer whose extras this
node does not have would leave trees on disk that cannot load, or
(once the own-`add` check exists) fail halfway through the copy and
still have written `remotes.toml`. The comment there already says a
typo must not leave a broken remote.

Refusing on pin inequality would break every new connect when a
peer patches numpy 2.2.1 → 2.2.2, which fights the per-tree
philosophy of ENV-8.

**Solution.** After `check_handshake`, compare **names**:

- Remote `extras` keys must be ⊆ local applied extras keys. Only
  the keys — connect never looks inside the value, so both the
  object and the legacy string shape compare the same.
- Version differences are not a connect error. `diff_remote` may
  add a warning row (name, theirs, ours).
- On missing names: raise `RegistryError` listing them. Do **not**
  `put_remote`. Do **not** copy trees.
- On success: existing flow.

**Verify.**

- Fake peer `/info` with `extras: {numpy: {version 2.2.1, dist
  numpy}}`, local applied empty → connect raises, `remotes.toml`
  absent, `pulled/` empty. The same peer on the legacy flat shape
  raises identically — connect reads keys.
- Local applied has `numpy` at `2.2.2`, peer advertises `2.2.1` →
  connect **succeeds**.
- Peer with empty extras → connect succeeds on a bare node.
- Existing tests that stub `/info` with only protocol fields still
  pass (`extras` default empty).

### ENV-7 — Own add checks applied extras; existing remotes do not

**Scope.** `RegistryStore.add`, a hook so the API can pass the stamp
in (the store reads neither Postgres nor the volume itself),
`apps/api/src/mftik_api/routes/registry.py` `add_strategy` and the
pull loop, ENV-2's `requires` field.

**Problem.** `mftik push` of an undeployable tree stores files and
then STS skips them (`loaded: false` / `load_error`). That is the
wrong layer for "this node does not have sklearn": the tree is fine;
the node is short. Conversely, applying the same check to
`store.add(..., origin=peer)` on a later sync would make an
already-connected remote unable to receive a new tree, which this
epic sends to deploy instead.

**Solution.** `add` grows an optional `applied_extras: Mapping[str, str]
| None`.

- `origin` in `public` / `private`: if `requires` is not ⊆
  `applied_extras` (when the argument is not `None`), raise
  `RegistryError`. The API always passes the stamp. CLI `check` does
  not call `add`.
- `origin` is a remote name already in `remotes.toml`: do **not**
  apply this check. Static gate only (ENV-1).
- First connect copies only after ENV-6 succeeded, so those adds are
  remote-origin but the names were already a subset; passing or
  skipping the check is equivalent. Skip by "already a remote" so the
  first copy and a later sync share one branch.

**Verify.**

- Private add, `requires=("numpy",)`, applied `{}` → 400, no files.
- Same add, applied `{numpy: ...}` → 200, `loaded` follows today's
  reload rules.
- `store.add` with origin `node1` while `node1` is a remote, tree
  requires `torch`, applied `{}` → stored.
- Tests in `test_registry_add.py` pass the applied map explicitly so
  they do not depend on a real volume.

### ENV-8 — Deploy and rebuild: `incompatible_environment`

**Scope.** STS session create
(`apps/sts/src/mftik_sts/rpc/sessions.py`,
`session/manager.py` `create_session` / `rebuild_interrupted`),
protocol error code next to `unknown_strategy`,
`apps/api/src/mftik_api/orchestrate.py` /
`routes/sts.py` (map the code; do not use 404),
picker may list the type with a reason
(`StrategyTemplate` / `list_strategy_types`),
`apps/sts/tests/test_session_failed.py` pattern,
rebuild tests in `apps/sts/tests/test_rebuild.py`.

**Problem.** If `load_local_registry` skips a tree that failed to
`import numpy`, `resolve` raises `KeyError` and create answers
`unknown_strategy`. The API turns that into 404. The operator thinks
the strategy was deleted. Rebuild logs "no strategy named X in this
build" — the same lie. An ABI-mismatched overlay must take this
path, not a raw import exception.

**Solution.** Before `resolve` / `load_class` on deploy and rebuild:

1. If the type is bundled, no extras check (bundled trees do not
   import third-party names).
2. Else load the store record (ENV-2). If `requires` ⊈ applied
   extra **names** from STS's in-memory stamp (ENV-4a: read at
   boot and on reload — deploy does not stat the volume, so two
   creates a second apart cannot disagree), or that stamp is
   ABI-mismatched or missing, fail with `incompatible_environment`
   and a sentence that lists the missing names.
3. Only then construct the class. `ModuleNotFoundError` after a
   passed check is a bug (stamp lied or ABI slipped past boot);
   keep it as `create_failed` / skip-with-traceback, not as 404.

`load_local_registry` may still skip an import error so one broken
tree does not take the process down. The deploy path must **not**
rely on the class already being in `_REGISTRY` to distinguish
"missing extra" from "missing tree". Prefer: record present +
requires check, then `load_class` if the key is absent (or always
load by digest as today).

HTTP: `incompatible_environment` is **409** (or 400). Not 404.
`unknown_strategy` stays 404 for a type the store does not have.

Picker: keep listing pulled types (`_deployable_templates` already
lists the store). Attach `requires` and whether applied extras cover
them, so the UI can badge "needs sklearn" (ENV-10). Deploy still
refuses if they ignore the badge.

**Verify.**

- Store has `peer::Signal` requiring `numpy`; applied empty; deploy
  → domain error `incompatible_environment`, no session row `live`.
- Same type, applied has `numpy`; deploy proceeds to today's attach
  path (may fail later for other reasons; env is not one of them).
- Stamp python tag mismatches; deploy a numpy tree →
  `incompatible_environment`, not `unknown_strategy`.
- Delete the tree, deploy → `unknown_strategy` / 404 (regression).
- Rebuild candidate whose `requires` is no longer applied: not
  rebuilt, log/reason names the extra, `rebuild_count` counts (it
  was an attempt).
- Bundled `noop` deploy on a bare node: unchanged.

### ENV-9 — Import a peer's extras (diff, then confirm)

**Scope.** `POST /environment/import` with body `{url, token?}` (or
`{name}` only when the remote is already stored). GET
`{url}/registry/v1/info`. ENV-5 auth and audit. Confirm is a
second call or `confirm: true` on the same route — pick one and
test both the dry-run and the apply.

**Problem.** Connect refused because the peer has `numpy` and this
node does not. The Owner should not retype pins. Auto-merge on
connect — or import that applies in one shot — lets a peer
(typosquat) write this node's trading `sys.path`.

**Solution.** Two steps, Owner in the middle:

1. Fetch extras from the peer's `/info` — name → `{version, dist}`
   with a key, name → `{}` without one (ENV-5). Compute the union with the local stamp. Return the diff:
   added names with the `version` **and the `dist` that will be
   installed**, same-name same-version (kept), same-name
   different-version (conflict). Do not lock, do not install.
   Conflicts are 409 **on confirm**, not on the preview — the
   preview lists them.
2. Confirm: if any same-name different-version remains, 409,
   stamp unchanged, installer not started. Otherwise apply the
   union (ENV-4, installing `dist==version`, `source=peer:<url-or-name>`
   on new rows) and ENV-4a reload.

Two kinds of row cannot be installed, and they must not share a
message:

- **guessed `dist`** — a peer on the legacy flat shape gives a
  version and no dist, so the fallback is the import name. Right
  for `numpy`, wrong for `sklearn`, and the difference is a package
  name this node is about to fetch from an index. The Owner
  supplies the real one in the diff and confirms.
- **unpinned** — the peer published `{name: {}}`. That is what an
  authenticated `/info` gives an anonymous caller, so it is the
  normal answer from any peer with the gate on that has not issued
  us a key. There is no version, and no field in the diff supplies
  one: telling the Owner to "set dist" would clear the blocker and
  hand the installer an empty pin. The blocker says what is true —
  ask that node for a registry key and import again with it.

An unpinned name this node already has is **kept**, not a conflict.
There is no peer version to conflict with, and refusing a confirm
over a value the peer never published would be a refusal the Owner
cannot act on.

Prefer body `{url, token?}` so a refused connect can preview
without writing `remotes.toml`. The token is what turns names into
pins (`docs/Auth.md`) — and, for a peer that has put its whole
origin behind a proxy, what gets a response at all. Import never
reads the source dump. Do not `put_remote` on import.

**Verify.**

- Preview of peer `{numpy: {version 2.2.1, dist numpy}}` vs empty
  local: diff lists numpy; stamp still generation `0`; connect
  still refused.
- Peer advertises `sklearn` with `dist: scikit-learn`: confirm
  installs `scikit-learn==1.6.1`, and the stamp keys it under
  `sklearn`. A tree with `requires = ("sklearn",)` then deploys.
- Peer on the legacy flat shape `{sklearn: "1.6.1"}`: preview
  marks the `dist` as guessed; confirm without a correction is
  refused, not installed as `sklearn`.
- Peer answering `{numpy: {}}` (names only, no key): row is
  `pinned: false`, not `guessed`; the blocker names the registry
  key and does not mention `dist`; a `dist` override does **not**
  unblock it; installer never runs.
- Same peer, but this node already has `numpy`: the row is kept,
  `conflicts` is empty, confirm is not blocked.
- Confirm with stub installer ok → stamp has numpy; connect
  succeeds.
- Confirm, installer fails → generation `0`; connect refused.
- Local `numpy==2.2.1`, peer `1.26.4`: preview lists a conflict;
  confirm → 409, installer not called.
- Registry key: 403.

### ENV-10 — CLI and UI

**Scope.** `packages/common/src/mftik/cli/` (`check` message already
covers gate; `push` surfaces add's env refusal; `run` must not
treat env as `unknown_strategy`). Frontend: Settings
(`routes/settings/+page.svelte`) or Registry, picker badge,
connect error copy, import diff + confirm,
`frontend/src/lib/api.ts`. `docs/CLI.md` for any new flags.
`api.ts` is hand-written. OpenAPI is **not** this ticket (ENV-5).

**Problem.** The contract is HTTP and STS. Operators use the CLI and
the picker. A 400 that says `RegistryError` without the missing names
will be retried as a network fault. A picker that lists
`peer::Signal` with no hint will deploy into 409 in a loop.

**Solution.**

- `mftik check`: still offline; refusals already print the gate
  sentence (including identifier / shadow). Mention `requires` in
  `docs/CLI.md` under `check`.
- `mftik push` / `run`: print the node's missing extras (exit 1).
- Optional `mftik check --against` (later if not in this ticket): GET
  `/environment` and compare. Not required to close the epic
  if push already does it.
- Settings (or Registry): list the stamp (packages, generation,
  size), add / remove (each call applies), Import shows the diff
  then confirm — including which rows had their `dist` guessed.
- Surface `abi_ok` and `restart_required` as banners, not as
  table columns. Both are states where the API is healthy and the
  stamp is right and deploys still fail; a list of package names
  explains neither. A removal's response names the trees it
  broke — show them.
- Connect form: on missing names, show them and a link to import.
  Version drift is not an error here (ENV-6).
- Picker: badge when `requires` is not covered; deploy button may
  stay enabled — the API is the authority.

**Verify.**

- CLI unit tests in `packages/common/tests/test_cli_*.py` style:
  fake HTTP, assert stderr contains `numpy`, exit 1, no traceback.
- Frontend: component/e2e only as far as this repo already tests
  Settings and the picker (`frontend/e2e/` if a spec exists for
  registry connect). Do not block the epic on a new Playwright
  suite; ENV-11 covers the behaviour.

### ENV-11 — Integration tests

**Scope.** New tests that cross API, store, handshake, connect, and
deploy. No live PyPI. Stub extras are directories on `tmp_path`
under `env/gen-{N}/site-packages`. Fake peers are the `httpx` stubs
already used in `test_registry_sync.py` and `test_registry_remotes.py`.
Fake STS/broker is `ReloadingBroker` in `test_registry_add.py` plus
the session-create path used in `test_deploy_refused.py`.

This ticket is last because it is how the epic is accepted, not
because the earlier tickets skip tests. Unit tests stay on ENV-1–10.
These scenarios fail if the seams were wired wrong even when each
unit passed.

**Problem.** The failure modes that matter are sequences: apply
fails and neither stamp nor live generation change; import preview
vs confirm; already-connected sync of a heavier tree; rebuild after
the Owner deletes an extra; crash mid-apply; concurrent apply;
ABI mismatch; a removal while STS is silent; a session that
arrives while the installer is still running. No single module
owns those.

**Solution.** One module (or a small package)
`apps/api/tests/test_environment_flow.py` that owns the sequences
below, plus `apps/sts/tests/test_environment_rebuild.py` for the
rebuild branch that cannot be faked at the API. Shared helpers:
plant `gen-N` + stamp + `current`, build a tree with `requires`,
fake `/info`.

**Verify.** The scenarios in the next section. All of them green is
the epic's acceptance. A scenario that needs a real wheel is marked
and excluded from default CI.

---

## Integration scenarios (ENV-11)

Planted extra `numpy` means
`{data}/env/gen-1/site-packages/numpy/__init__.py`, `current` →
`gen-1/site-packages`, and an `applied.json` that lists `numpy`
with a matching `python` / `platform`. It is not the scientific
library. Apply's real installer is unit-tested in ENV-4 against a
local stub; these
sequences assume apply has been represented by the ENV-3 commit
helpers (or apply with the installer monkeypatched).

### S1 — Regression: bare node, stdlib tree

Push / add a tree with no `requires`. Deploy `private::Tiny` (or
whatever the type is). Session creates. `/registry/v1/info` extras
`{}`. Bundled `noop` still deploys.

**Guards:** ENV-1 did not break trees that never mention extras.

### S2 — Declare, push, apply, deploy

1. Tree `import numpy` without `requires` → add/check refused.
2. Same tree with `requires = ("numpy",)` → `mftik check` (or
   `inspect_files`) ok; add to a bare node → 400, nothing on disk.
3. PUT `/environment` with a **failing** installer → `/info` extras
   still `{}`; generation `0`; `current` still the empty gen;
   add still 400.
4. PUT again with the stub installer succeeding → `/info` has numpy;
   reload ran; add 200; deploy 200.

**Guards:** failed apply does not move the stamp or `current`;
declaration ≠ presence.

### S3 — Restart does not install

With S2's overlay on the volume, start STS (or call `amain`'s load
path). Installer call count is 0. `current` is on `sys.path`. The
numpy tree is in `_REGISTRY`. An `interrupted` row for that
strategy with `restart=always` rebuilds, not "unknown strategy".

**Guards:** ENV-4 / 4a.

### S4 — New connect blocked on names

Local applied empty. Fake peer `/info` extras `{numpy: "1.0"}` and
one public tree. `POST /registry/v1/remotes` (or `connect_remote`)
errors. `remotes.toml` has no entry. `pulled/` empty.

**Guards:** ENV-6; no half-connected remote.

### S4b — New connect allows pin drift

Local applied `{numpy: 2.2.2}`. Peer `/info` `{numpy: 2.2.1}`.
Connect succeeds. Diff may mention the versions; it is not an
error.

**Guards:** Invariant 4.

### S5 — Import extras, then connect

S4's peer. Preview `POST /environment/import` with `{url, token?}`
— no `remotes.toml`, stamp unchanged. Confirm with stub installer
→ stamp lists numpy; `/info` matches; connect copies the tree.
Do not `put_remote` on import.

Run it once more with the peer advertising `sklearn`
(`dist: scikit-learn`): the preview shows the dist, confirm asks
the installer for `scikit-learn==1.6.1`, and the stamp keys it
under `sklearn` so a tree with `requires = ("sklearn",)` deploys.
This is the case a flat `{name: version}` handshake could not
have served.

Confirm with failing installer → generation `0`; connect refused.

**Guards:** ENV-9 preview ≠ apply; ENV-6 reads the stamp; import
names and dist names are not the same list.

### S6 — Already connected, new heavier tree

Local applied `{numpy}`. Remote connected, pulled a numpy tree.
Peer now lists a second tree with `requires = ("torch",)` and
advertises `torch` on `/info`. Sync / reconnect **succeeds**. Both
trees are on disk. Deploy the numpy type succeeds. Deploy the torch
type → `incompatible_environment` (not 404). Picker still lists both.

**Guards:** ENV-7 remote branch; ENV-8; first-connect rule is not
re-applied as a total remote failure.

### S7 — Owner removes an extra

S2's numpy tree stored. `DELETE /environment/packages/numpy` with
no live session (apply succeeds; stamp no longer lists numpy;
`current` points at a generation without it). Deploy that type →
`incompatible_environment`. Rebuild of an interrupted session for
that type does not come back as live; the reason names the extra.

Same DELETE while a session is live, no `force` → 409, stamp
unchanged, installer never started. Same again with `force` →
200, stamp moves, `restart_required` true, and the live session
keeps the numpy it already imported until the container restarts.
The response names the trees the removal broke.

**Guards:** stamp + live-session rule, not leftover files; ENV-5
owns the check, ENV-4 only obeys the flag.

### S8 — Pin clash on import confirm

Stamp has `numpy` `2.2.1`. Peer `/info` extras `{numpy: "1.26.4"}`.
Preview lists the conflict. Confirm → 409, stamp unchanged,
installer not called.

**Guards:** ENV-9 merge policy.

### S9 — Registry key cannot write extras

Mint a registry key (`docs/Auth.md`). PUT `/environment` and
`POST /environment/import` → 403. GET `/registry/v1/info` still 200
and includes extras. GET `/registry/v1/strategies` still 200 with
that key.

**Guards:** ENV-5 auth; `REGISTRY_READ_PATHS` unchanged.

### S10 — Undeclared import is refused even when the node has it

Applied overlay has a `sklearn` stub. Tree `import sklearn` with
empty `requires`. Add → gate 400 (ENV-1), not an env error.

**Guards:** Owner whitelist does not replace the tree declaration.

### S11 — `loaded` after add still means STS imported it

Private add of a numpy tree with applied numpy. Fake STS reload
returns the qualified key → `loaded: true`. Fake STS returns `[]` →
`loaded: false` with today's `load_error` sentence. Env is not a
third `loaded` meaning.

**Guards:** `docs/CLI.md` reload table stays true.

### S12 — Crash mid-apply

Begin `gen-2/`, write a half tree, **do not** commit. Kill the
apply (raise, or abort helper). Restart STS. Stamp and `current`
still name `gen-1`. Numpy tree still loads. No installer on boot.
Orphan `gen-2/` is not on `sys.path` and is not in `/info`.

**Guards:** generation directories; invariant 1.

### S13 — Concurrent apply

Two API workers call PUT at once. One holds `fcntl.flock`. The
other 409s. Exactly one new generation is committed. (If the
second waited instead of 409, this test would hang on the cap.)

**Guards:** ENV-3 lock.

### S14 — Overlay present, python tag mismatch

Plant a valid `gen-1` numpy overlay. Stamp says `python: [3, 11]`.
This interpreter is 3.12. Boot: extras empty as far as `/info` and
deploy are concerned. Deploy of a numpy tree →
`incompatible_environment`, not an import traceback. Installer
was not run. `GET /environment` says `abi_ok: false` and names
both tags — the stamp's `[3, 11]` and the running `[3, 12]` — so
the Owner is not left reading "extras vanished". A fresh apply on
this interpreter returns `abi_ok` to true at generation 2.

**Guards:** ENV-4a ABI rule; ENV-8; ENV-5 makes the reason
visible instead of only its symptom.

### S15 — `helpers.py` declares via a later class file

Tree: `helpers.py` is `import numpy` only; `strategy.py` has the
class and `requires = ("numpy",)`. `mftik check` / add (with
applied numpy) succeeds. This is ENV-1's two-pass, at the
integration seam.

**Guards:** ENV-1 must not still be a single walk.

### S16 — Removal while STS is silent

Stamp lists numpy. STS does not answer the session-list RPC (the
broker is stubbed to time out). `DELETE
/environment/packages/numpy` without `force` → 409, installer
never started, stamp unchanged. With `force` → 200 and
`restart_required`.

**Guards:** ENV-5 fails closed; a restarting STS is not "nobody
is trading".

### S17 — A session arrives mid-install

Begin an apply that changes a stamped pin (`allow_disruptive`
granted; no live sessions at the start). While the stubbed
installer is running, create a live session. At commit the
re-check finds it: generation aborted, stamp and `current` still
name the old one, 409.

**Guards:** `apply.lock` does not exclude deploys; ENV-5's second
check is what does.

---

## Order

```
ENV-1 gate requires (two-pass)
  └── ENV-2 record requires
        ├── ENV-3 stamp + gen dirs + flock
        │     └── ENV-4 apply into gen-N
        │           └── ENV-4a STS sys.path + reload
        │                 └── ENV-5 API + handshake + openapi
        │                       ├── ENV-6 new connect (names)
        │                       ├── ENV-7 add vs remote add
        │                       ├── ENV-8 deploy / rebuild
        │                       └── ENV-9 import preview + confirm
        │                             └── ENV-10 CLI / UI
        └── ENV-11 integration (after 6–9 at minimum; 10 optional)
```

ENV-1 and ENV-3 can start in parallel. ENV-11 should not be written
against imagined APIs: add the scenarios as the routes land, or keep
them skipped until the names exist — do not invent a second HTTP
shape in the tests.

## Docs that become wrong when this ships

| File | What it says today | After |
|---|---|---|
| `docs/CLI.md` | `check` is gate + naming + yml + `on_initialized` | Gate also accepts declared `requires` (two-pass). Push can fail because the stamp does not list an extra. |
| `docs/Auth.md` | `/registry/v1/info` is "versions only" | Versions, `env_generation`, and applied extra **names**. Exact pins (`version`, `dist`) only for an authenticated caller. `source` is not published. |
| `packages/common/src/mftik/registry/gate.py` module docstring | "a third-party import would be a missing module after a pull" | Third-party imports must be declared; the destination node's applied extras decide presence. `check_files` is two-pass. |

Update those in the ticket that makes the sentence false (ENV-1, ENV-5,
ENV-7), not in a mop-up ticket.

## Out of scope (later epics)

- Strategy worker processes / per-tree lockfiles.
- `mftik check --against` if ENV-10 ships without it.
- Baking extras into the GHCR image. The volume is the extra; the
  image stays the SDK.
- Frontend Playwright coverage beyond what ENV-10 already touches.
- Compiling sdists on the node (`--only-binary=:all:` refuses them).
- `202` + poll for apply; volume quotas.
