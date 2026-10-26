# Pack and deploy

`python -m mftik.pack` builds a strategy image and pushes it. The program is `mftik.pack`, and `main` is its entry. A strategy author does not import the packer from a hook. The image is an OCI image used as an archive. The platform does not run it as a container. It unpacks the rootfs and executes the entrypoint.

The command prints the image manifest digest on stdout. That digest is what the session spec stores in `StrategyRef.code` (PROTOCOL §8.1). The platform pulls by digest. It does not follow a tag that later moves. A tag is only a name a person can look up. Put the digest, not the tag, in the spec.

A failure raises `PackError` inside the tool. The command prints one line, `mftik.pack: ...`, on stderr and exits 1. The line does not include a registry credential.

## Command

`python -m mftik.pack -dir DIR -target module:Class -name NAME -repo HOST/REPO [-tag TAG] [-requirements FILE] [-env K=V] [-plain-http] [-python 3.12] [-wheel PATH]`

`-dir` is the strategy directory. The top-level module of `-target` has to live there, either as `module.py` or as `module/__init__.py`. `-target` is the same `module:Class` string that `python -m mftik.run` takes. Dotted module names are allowed when that top-level package is in the directory.

`-name` is the manifest's name. It must be non-empty and must not contain control characters. `-repo` is `host/repository`, with no URL scheme. The host may include a port. `-tag` is optional. It must start with a letter, a digit, or an underscore, and the rest may be letters, digits, underscore, dot, or hyphen, up to 128 characters.

`-python` selects the interpreter inside the image. The allowed values are `3.11`, `3.12`, and `3.13`. The default is `3.12`. The packer downloads a pinned linux x86_64 build of that interpreter and checks its checksum. The image is `linux` and `amd64` regardless of the machine running the packer. A macOS checkout can pack. The resulting worker still runs on linux.

`-wheel` is a local `mftik` wheel. Without it, the packer downloads the wheel that matches the installed package version and the chosen interpreter. `-requirements` is a requirements file. Dependencies are downloaded as manylinux_2_28 wheels for that interpreter. A dependency that is only a source distribution is an error. The packer does not compile it.

`-env` may be repeated. Each value is `K=V`. The packer rejects a name that starts with `MFTIKO_`, and it rejects `PYTHONPATH`, `MFTIK_RELEASE`, and `PYTHONDONTWRITEBYTECODE`, which it sets itself. It also rejects a duplicate key, a newline, or a null byte. `-plain-http` talks HTTP to the registry. A local registry uses it. The default is HTTPS.

Credentials come from Docker's config file for that registry host, including a credential helper. The push supports basic authentication and a bearer token exchange. An anonymous push is what happens when no credential is configured. Do not put a credential in `-env` or in the strategy directory.

Downloaded interpreters and wheels are cached under `~/.cache/mftik/pack`, keyed by checksum.

## What the image contains

The image has three layers. The first is the interpreter and the SDK. The second is `site-packages` from `-requirements` (an empty directory when there are no requirements). The third is the strategy tree, prefixed as `app`. Python sources are compiled into bytecode so the unpacked tree can stay read-only.

The manifest says `schema` `mftiko.worker.v1`, `kind` `strategy`, `runtime` `python`, `os` `linux`, and `arch` `amd64`. The entrypoint is `python/bin/python3 -m mftik.run` plus the `-target` string. The environment sets `PYTHONPATH` to `app:site-packages`, `MFTIK_RELEASE` to the packed SDK version, and `PYTHONDONTWRITEBYTECODE` to `1`, then the `-env` pairs. `Hello.contracts`, and the manifest's contract list, are `worker.v1`, `stsctl.v1`, `md.v1`, and `td.v1`. The packer reads that list from the installed SDK rather than keeping a second copy.

The platform checks the manifest before it starts the process. A contract the platform requires and the image does not list, a wrong operating system or architecture, or a rootfs that fails the archive checks ends the session `INIT_FAILED` without running the strategy.
