# Pack and deploy

`mftiko-pack -bin` builds a strategy image and pushes it. It ships with the `mftiko` CLI. A strategy author does not call it from a hook. The image is an OCI image used as an archive. The platform does not run it as a container. It unpacks the rootfs and executes the entrypoint.

Build a linux x86_64 GNU binary, then pack that file. The packer does not compile Rust. The worker's supported platform is Ubuntu 22.04 or later on x86_64 (glibc 2.35). There is no musl image. A binary built on macOS is Mach-O; the packer refuses it.

Every binary that calls `mftik_rt::run` embeds what the packer reads: the `mftiko.contracts=` marker, and on Linux a Go build info record that gives the manifest's `sdk.version` (E08-D001, E08-D005). Depend on a release after 1.2.0; packing a binary linked to v1.2.0 fails because that tag does not embed them.

The command prints the image manifest digest on stdout. That digest is what the session spec stores in `strategy.ref` (PROTOCOL §8.1). The platform pulls by digest. It does not follow a tag that later moves. A tag is only a name a person can look up. Put the digest, not the tag, in the spec.

A failure prints one line, `mftiko-pack: ...`, on stderr and exits 1. The line does not include a registry credential.

## Command

```text
mftiko-pack -bin FILE -name NAME -repo REGISTRY/REPOSITORY [-kind strategy|md.conn|td.account] [-tag TAG] [-env K=V]... [-plain-http]
```

`-bin` is the strategy binary, usually `target/release/md_watch`. `-name` is the manifest's name. It must be non-empty. `-repo` is `host/repository`, with no URL scheme. The host may include a port. `-kind` defaults to `strategy`. `-tag` is optional. The digest does not depend on it.

`-env` may be repeated. Each value is `K=V`. The packer rejects a name that starts with `MFTIKO_`, a duplicate key, an empty name, or a name that contains `=`. `-plain-http` talks HTTP to the registry. A local registry uses it. The default is HTTPS.

```bash
cargo build --release -p mftik-md-watch
mftiko-pack -bin target/release/md_watch -name md_watch -repo registry.example/acme/md_watch
```

Credentials come from Docker's config file (`~/.docker/config.json`) for that registry host, including a credential helper. The push supports basic authentication and a bearer token exchange. An anonymous push is what happens when no credential is configured. Do not put a credential in `-env`.

## What the image contains

The image has one layer: `bin/strategy`, mode 0755, uid and gid 0, mtime 0. The bytes are the packed file.

The manifest says `schema` `mftiko.worker.v1`, `kind` `strategy`, `runtime` `go`, `os` and `arch` from the ELF (linux/amd64 for this platform), and `entrypoint` `["bin/strategy"]`. `sdk.lang` is `go`. `sdk.version` is `v` plus the `mftik-rt` version the binary was built with, for example `v1.2.0`. PROTOCOL §8.2 uses `runtime` only for display and statistics; the platform does not read `runtime` or `sdk.lang`. `Hello.contracts`, and the manifest's contract list, are `worker.v1`, `stsctl.v1`, `md.v1`, and `td.v1`, taken from the marker in the binary. `-env` pairs become the manifest `env`. The platform adds its own `MFTIKO_*` variables at launch (PROTOCOL §4.6). The worker does not inherit the packer's environment.

A binary without the contracts marker fails with `artifact: no embedded contracts list`. A binary without Go build info fails with `artifact: no sdk version in buildinfo`. An ELF the packer does not accept fails with `artifact: unsupported ELF os/arch`.

The platform checks the manifest before it starts the process. A contract the platform requires and the image does not list, a wrong operating system or architecture, or a rootfs that fails the archive checks ends the session `INIT_FAILED` without running the strategy.

## Deploy a session

A session is created through the mftiko API, usually with the `mftiko` CLI (v0.3.0 or later). The CLI talks only to the API. It needs a context and a token; see mftiko's `docs/cli.md`.

1. Pack and push. The repository is the full image path. Its prefix has to be on the API's allowed list of registry prefixes, or `session create` fails with `/strategy/repository is not allowed`. The STS agent pulls with the platform's registry credentials, so the image has to be readable with them.

```bash
cargo build --release -p mftik-md-watch
mftiko-pack -bin target/release/md_watch -name md_watch -repo registry.example/acme/md_watch
```

2. Write the session file. Its fields are the API's `SessionCreate` in snake_case. `strategy` is the `repository` and a `ref`. The `ref` is the digest the packer printed. A tag also works: the API resolves it once and stores the digest. `params` is the strategy's own JSON. `feeds` names each feed and its MD instance. `sts_instance` and `md_instance` come from `mftiko instance list`. A strategy that trades also lists `accounts`.

```yaml
name: md-watch
sts_instance: sts-1
strategy:
  repository: registry.example/acme/md_watch
  ref: sha256:0000000000000000000000000000000000000000000000000000000000000000
params:
  ticker: Ex_Spot_ABC
  every_s: 30
feeds:
  - feed: trade.Ex_Spot_ABC
    md_instance: md-1
  - feed: orderbook_delta.Ex_Spot_ABC
    md_instance: md-1
restart:
  mode: never
```

3. Create the session, end it, then read its record.

```bash
mftiko session create -f session.yaml --wait running
mftiko session show <session_id>
mftiko session logs <session_id>
mftiko session end <session_id> --wait
mftiko session export-logs <session_id>
mftiko session delete <session_id>
mftiko record list
mftiko record show <record_id>
```

`create` prints the session id and the resolved digest. `--wait running` returns once the session is `running`. `session show` lists the phase, each feed's ack, and where the worker's stdout and stderr are. `session end --wait` sends Stop. The worker runs `on_stop` and exits, and the session ends `done` with reason `ended_by_spec`. The status's `last_exit` then has `on_stop_completed: true`.

`session logs` prints the worker's stdout and stderr. `-f` follows them, `--tail N` starts from the last N lines, and `--stream` picks one. `session export-logs` downloads both as a tar.gz. The log lives on the STS host, and `session delete` removes it, so export it first if you need to keep it. The record's `final` is written when the session is deleted. Until then, `record show` reports the state `created`.
