# Install and run

`mftik` 1.1.0 is the Python SDK. It requires Python 3.11, 3.12, or 3.13. The published artifacts are abi3 wheels for manylinux_2_28 x86_64 and macOS arm64. There is no source distribution. The macOS wheel is for development, type checking, and `python -m mftik.pack`. A session worker runs from the linux image that packer builds, not from a macOS process.

Install the package from PyPI with `pip install mftik`. The import name is `mftik`. `__version__` is the installed distribution's version. This release is a rewrite. Programs written for the 0.12 packages that remain on PyPI do not run on it.

People working in this repository build the extension locally with uv and maturin (`uv sync`, then `uv run maturin develop`). A strategy author who only runs a session does not need that toolchain. The packer downloads a pinned CPython and the published wheel when it builds an image.

## The process

The platform starts one process:

`python -m mftik.run module:Class`

`module` is an importable module name. `Class` is an attribute of that module. The runtime constructs the class with no arguments. The object must be an instance of `Strategy`. Anything else is a load failure, described below.

`run` is the same entry as a function. `run(strategy)` hosts that instance and does not return. The process ends with the status in [Errors and exit codes](errors.md). The function takes an optional `release` string, which is sent as `Hello.release` and shown as the worker's release. The module entrypoint has no flag for it. It uses the environment variable `MFTIK_RELEASE` when that variable is set, and otherwise `__version__`. A packed image sets `MFTIK_RELEASE` to the SDK version that was packed. See [Pack and deploy](pack.md).

`run` also takes keyword-only `version` and `commit` strings. They are the strategy's `version` and `commit` on each log line. The default is an empty string. They are not `Hello.release`. `log` writes one JSON object per line to stderr. Inside a hook, use `ctx.log`. Outside a hook, use the module-level `log`. The level and the layout come from `MFTIKO_LOG_LEVEL` and `MFTIKO_LOG_FORMAT`.

The class has to implement `on_init`, `on_start`, and `on_stop`. Those three, and the optional hooks, are the [lifecycle](lifecycle.md). The object is constructed before any of them runs, with no arguments, so configuration belongs in `on_init`, read from `params`. The strategies under `examples/python` all read their configuration in `on_init`. See [Examples](examples.md).

## When the class cannot be loaded

A bad command line, an import error, a constructor error, or an object that is not a `Strategy` does not skip the session protocol. The process still sends Hello. The session ends `INIT_FAILED`. `Done.error` is `load module:Class: TypeName: message`. The traceback is written to stderr and is not part of that error string. The process does not exit 1 before Hello.

`python -m mftik.run` with the wrong number of arguments uses the same path. The message says the usage is `python -m mftik.run module:Class`.

## What the strategy is given

`on_init` runs after the process is up and before market data or accounts are ready. Read `params` there, resolve the instruments the strategy trades, and read files from the [volume](volume.md). `on_start` runs once every market-data and account lease is ready. Orders may be placed from `on_start` onward. From then until the session ends, optional hooks deliver market data and account events. The rules for calling the SDK from those hooks are in [Context](context.md) and [Threads](threads.md).

Standard output and standard error are line buffered, so a printed line is not left in a block buffer when the process exits. Do not put secrets in log lines or in exception text. Exception text is copied into the session's error fields. See [Errors and exit codes](errors.md).
