# Pack and deploy

`python -m mftik.pack` builds a strategy image and pushes it. The program is `mftik.pack`, and `main` is its entry. A strategy author does not import the packer from a hook. The image is an OCI image used as an archive. The platform does not run it as a container. It unpacks the rootfs and executes the entrypoint.

The command prints the image manifest digest on stdout. That digest is what the session spec stores in `StrategyRef.code` (PROTOCOL §8.1). The platform pulls by digest. It does not follow a tag that later moves. A tag is only a name a person can look up. Put the digest, not the tag, in the spec.

A failure raises `PackError` inside the tool. The command prints one line, `mftik.pack: ...`, on stderr and exits 1. The line does not include a registry credential.

## Command

`python -m mftik.pack -dir DIR -target module:Class -name NAME -repo HOST/REPO [-tag TAG] [-requirements FILE] [-env K=V] [-plain-http] [-python 3.12] [-wheel PATH] [-no-push]`

`-dir` is the strategy directory. The top-level module of `-target` has to live there, either as `module.py` or as `module/__init__.py`. `-target` is the same `module:Class` string that `python -m mftik.run` takes. Dotted module names are allowed when that top-level package is in the directory.

`-name` is the manifest's name. It must be non-empty and must not contain control characters. `-repo` is `host/repository`, with no URL scheme. The host may include a port. `-tag` is optional. It must start with a letter, a digit, or an underscore, and the rest may be letters, digits, underscore, dot, or hyphen, up to 128 characters.

`-python` selects the interpreter inside the image. The allowed values are `3.11`, `3.12`, and `3.13`. The default is `3.12`. The packer downloads a pinned linux x86_64 build of that interpreter and checks its checksum. The image is `linux` and `amd64` regardless of the machine running the packer. The packer compiles bytecode with the pinned interpreter, so it has to run that interpreter: on linux x86_64 it does. Elsewhere it falls back to the packer's own Python, which must be the exact pinned patch (3.11.17, 3.12.15, or 3.13.17), or the command fails with `cannot run the pinned CPython`. On a Mac, either match that patch or run the packer in a `linux/amd64` container. The resulting worker still runs on linux.

`-wheel` is a local `mftik` wheel. Without it, the packer downloads the wheel that matches the installed package version and the chosen interpreter. `-requirements` is a requirements file. Dependencies are downloaded as manylinux_2_28 wheels for that interpreter. A dependency that is only a source distribution is an error. The packer does not compile it.

`-env` may be repeated. Each value is `K=V`. The packer rejects a name that starts with `MFTIKO_`, and it rejects `PYTHONPATH`, `MFTIK_RELEASE`, and `PYTHONDONTWRITEBYTECODE`, which it sets itself. It also rejects a duplicate key, a newline, or a null byte. `-plain-http` talks HTTP to the registry. A local registry uses it. The default is HTTPS.

`-no-push` builds the image and prints the digest, and does not open a connection. `-repo` is still required, because the image name is checked. [Examples](examples.md) packs `md_watch` that way.

Credentials come from Docker's config file for that registry host, including a credential helper. The push supports basic authentication and a bearer token exchange. An anonymous push is what happens when no credential is configured. Do not put a credential in `-env` or in the strategy directory.

Downloaded interpreters and wheels are cached under `~/.cache/mftik/pack`, keyed by checksum.

## What the image contains

The image has three layers. The first is the interpreter and the SDK. The second is `site-packages` from `-requirements` (an empty directory when there are no requirements). The third is the strategy tree, prefixed as `app`. Python sources are compiled into bytecode so the unpacked tree can stay read-only.

The manifest says `schema` `mftiko.worker.v1`, `kind` `strategy`, `runtime` `python`, `os` `linux`, and `arch` `amd64`. The entrypoint is `python/bin/python3 -m mftik.run` plus the `-target` string. The environment sets `PYTHONPATH` to `app:site-packages`, `MFTIK_RELEASE` to the packed SDK version, and `PYTHONDONTWRITEBYTECODE` to `1`, then the `-env` pairs. `Hello.contracts`, and the manifest's contract list, are `worker.v1`, `stsctl.v1`, `md.v1`, and `td.v1`. The packer reads that list from the installed SDK rather than keeping a second copy.

The platform checks the manifest before it starts the process. A contract the platform requires and the image does not list, a wrong operating system or architecture, or a rootfs that fails the archive checks ends the session `INIT_FAILED` without running the strategy.

## Deploy a session

A session is created through the mftiko API, usually with the `mftiko` CLI (v0.2.0 or later). The CLI talks only to the API. It needs a context and a token; see mftiko's `docs/cli.md`. The steps below are the first testnet run of `md_watch` (E05-06).

1. Pack and push. The repository is the full image path. Its prefix has to be on the API's allowed list of registry prefixes, or `session create` fails with `/strategy/repository is not allowed`. The STS agent pulls with the platform's registry credentials, so the image has to be readable with them.

```bash examples/python/md_watch/README.md:43-48
python -m mftik.pack \
  -dir examples/python/md_watch \
  -target md_watch:MdWatch \
  -name md_watch \
  -repo ghcr.io/lynxlinkage/strategies/md_watch \
  -tag v0.14.0
```

2. Write the session file. Its fields are the API's `SessionCreate` in snake_case. `strategy` is the `repository` and a `ref`. The `ref` is the digest the packer printed. A tag also works: the API resolves it once and stores the digest. `params` is the strategy's own JSON. `feeds` names each feed and its MD instance. `sts_instance` and `md_instance` come from `mftiko instance list`. A strategy that trades also lists `accounts`.

```yaml examples/python/md_watch/session.yaml:1-13
name: md-watch-btc
sts_instance: sts-jp-1
strategy:
  repository: ghcr.io/lynxlinkage/strategies/md_watch
  ref: sha256:0037ffce4ec733f5036bf290030f8b391af83f29ae240704b956993929e38d4a
params:
  ticker: BinanceUM_Perp_BTCUSDT
  every_s: 10
feeds:
  - feed: orderbook_delta.BinanceUM_Perp_BTCUSDT
    md_instance: md-jp-1
restart:
  mode: never
```

3. Create the session, end it, then read its record.

```bash examples/python/md_watch/README.md:52-57
mftiko session create -f examples/python/md_watch/session.yaml --wait running
mftiko session show <session_id>
mftiko session end <session_id> --wait
mftiko session delete <session_id>
mftiko record list
mftiko record show <record_id>
```

`create` prints the session id and the resolved digest. `--wait running` returns once the session is `running`. `session show` lists the phase, each feed's ack, and where the worker's stdout and stderr are. `session end --wait` sends Stop. The worker runs `on_stop` and exits, and the session ends `done` with reason `ended_by_spec`.

The API does not serve the worker's log. It is a file on the STS host, at the path `session show` prints. `session delete` removes that file, so copy the log before deleting the session. The record's `final` is written when the session is deleted. Until then, `record show` reports the state `created`.

On mftiko v0.2.0, the final status after `session end` has no `last_exit`, so the record does not show `on_stop_completed` (BullionBear/mftiko#433). Until that is fixed, `on_stop`'s own log line is what shows that it finished.
