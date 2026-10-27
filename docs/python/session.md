# Session spec

A session is created from a `SessionCreate` request. The `mftiko` CLI reads it from a YAML or JSON file whose fields are `SessionCreate` in snake_case (`mftiko session create -f FILE`). The API checks the request and stores it as the `SessionSpec` the platform runs. This page says what each field means to a Python strategy. The authority is mftiko's `contract/openapi/api/v1.yaml` (`SessionCreate`) and PROTOCOL §3. The defaults below are mftiko v0.2.1's. A default marked "STS default" comes from the STS deployment and can differ per instance.

`name`, `sts_instance`, and `strategy` are required. Every other field may be left out. A number left out, or written as 0, takes its default.

## Examples

`md_watch` reads market data only. Its file is in [Pack and deploy](pack.md#deploy-a-session). `maker` trades on one account:

```yaml examples/python/maker/session.yaml:1-22
name: maker-abc
labels:
  owner: research
sts_instance: sts-1
strategy:
  repository: registry.example/acme/maker
  ref: v0.14.0
params:
  ticker: Ex_Spot_ABC
  api_id: 7
  algo_id: 1
  qty: 1
  price: 10
accounts:
  - name: main
    api_id: 7
    algo_ids: [1]
restart:
  mode: on_failure
  max_restarts: 3
  window_s: 600
grace_ms: 15000
```

`stateful` counts fills on an account, starts from an earlier session's state, and sets limits:

```yaml examples/python/stateful/session.yaml:1-20
name: stateful-abc
sts_instance: sts-1
strategy:
  repository: registry.example/acme/stateful
  ref: v0.14.0
params:
  ticker: Ex_Spot_ABC
  api_id: 7
  every_s: 30
accounts:
  - name: main
    api_id: 7
    algo_ids: [2]
inputs:
  - name: state
    repository: registry.example/acme/stateful-state
    digest: sha256:9f2c4e7a1b3d5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8
limits:
  volume_mb: 64
  hook_timeout_s: 5
```

The registry, the account, and the input digest in these two files are placeholders. Replace them with your own.

## Identity

`name` is 1 to 128 characters. It is a label for people. The platform identifies the session by the id that `create` prints.

`labels` is a map of strings, at most 32 entries. A key matches `^[A-Za-z0-9][A-Za-z0-9._/-]{0,62}$` and a value is at most 256 characters. The platform does not read them.

`sts_instance` is the STS instance that runs the worker. Its plane must be `sts`; `mftiko instance list` shows the names. The session stays on that instance for its whole life, and so does its volume.

## Strategy

`strategy.repository` is the full image path, registry host included, for example `ghcr.io/acme/strategies/maker`. Its prefix must be on the API's allowed list, or the request fails with `/strategy/repository is not allowed`.

`strategy.ref` is a tag or a digest. The API resolves a tag once, when the session is created, and stores the digest. A tag that moves later does not change a session that already exists. The manifest's `kind` must be `strategy`. [Pack and deploy](pack.md) builds such an image.

## Params

`params` is any JSON object. The platform does not read it. The API stores it as compact JSON, and `ctx.params()` returns those bytes. The strategy parses them, usually in `on_init`. An exception there ends the session `INIT_FAILED`. Each example's README lists the params it expects.

## Feeds

`feeds` is the market data the session subscribes to. Each entry is `feed` and `md_instance`.

`feed` is `<topic>.<UniversalTicker>`, for example `orderbook_delta.BinanceUM_Perp_BTCUSDT`. The topic decides the hook. `orderbook_delta` drives the local book and `on_book`. `trade` drives `on_trade`. The other topic strings are in [API reference](reference.md#topics), and each hook in [Lifecycle](lifecycle.md). The API does not check that the instrument exists. The strategy still calls `resolve` for the UID that events carry.

`md_instance` is the MD instance that publishes the feed, from `mftiko instance list`.

## Accounts

`accounts` is the trading accounts the session may use. Each entry is `name`, `api_id`, and `algo_ids`.

`api_id` is an account that already exists on the platform (`mftiko account list`). `algo_ids` is at least one id from 1 to 1023, without repeats. An order carries one of these algo ids. Any other id raises `AlgoNotDeclared` without sending anything. See [Context](context.md#orders).

Two sessions that are not finished may not share an algo id on the same account. The API rejects an overlap with 409 `algo_overlap`. The check is best effort. For sessions on different STS instances, avoiding an overlap is the user's job.

The strategy does not learn its account from this list. Pass the `api_id` and the `algo_id` in `params` too, as `maker` does, and keep the two in step.

A session with no accounts cannot place orders. That is the right shape for a market-data strategy such as `md_watch`.

## Restart

`restart.mode` is `on_failure`, the default, or `never`.

`on_failure` restarts a worker that ended `CRASHED`, `HUNG`, or by a signal the platform did not send. It does not restart one that ended `DONE`, `FAILED`, or `INIT_FAILED`. The wait between restarts starts at 1 second and grows to at most 60 seconds. More than `max_restarts` restarts within `window_s` seconds ends the session `FAILED` with reason `restart_limit`. Left at 0, they are 5 restarts in 300 seconds.

A restart is a new process, a new incarnation of the same session. Nothing in memory survives it. What a strategy needs to keep goes in the volume. See [Volume and inputs](volume.md).

## Timeouts

`deps_timeout_s` is how long the session may wait for its feeds and accounts before `on_start`. Past it, the session ends `FAILED` with reason `deps_timeout`. 0 is 60 seconds.

`start_timeout_s` is the deadline of `on_init`, and separately of `on_start`. Past it, the session ends `INIT_FAILED` with reason `deadline`. 0 is the STS default, 60 seconds.

`grace_ms` is the time from Stop to the end of the process. It covers the whole shutdown, `on_stop` included. When it runs out, the platform sends SIGTERM and then SIGKILL, and the exit records `on_stop_completed` as false. 0 is the STS default, 10 seconds. Raise it when `on_stop` writes a large file.

`recv_window_ms` is how old a market-data event may be when the dispatcher takes it off the ring. Older events are dropped before their hook. `orderbook_delta` is not dropped this way. The same value bounds each `md.view`, `md.fetch`, and order request. 0 is the STS default, 1000 ms. See [Lifecycle](lifecycle.md#market-data-hooks) and [Context](context.md#reads).

## Limits

`limits.hook_timeout_s` is the hard limit for one hook. A hook that runs past it ends the worker `HUNG`, and `on_stop` does not run. Under `on_failure`, a `HUNG` worker is restarted. 0 is the STS default, 10 seconds. See [Threads](threads.md).

`limits.volume_mb` is the size the volume directory may reach. Past it, the session ends `FAILED` with reason `volume_over_limit`. 0 is the STS default, 1024 MiB.

`limits.memory_mb` and `limits.gomaxprocs` are for Go workers. `mftik` does not read either of them. See [Runtime notes](runtime.md#memory).

## Inputs

`inputs` is read-only data the platform unpacks before the process starts. Each entry is `name`, `repository`, and `digest`.

`name` matches `^[a-z0-9][a-z0-9_-]{0,63}$` and is unique in the spec. The strategy finds the files at `ctx.input_dir(name)`. `repository` is the full path of a data artifact. It is not checked against the allowed list. `digest` pins it.

A finished session's volume is packed into a data artifact, and its digest is the session status's `state_out`. Give that digest to the next session as an input to carry the state over. The `stateful` example reads its `state` input this way, when its volume is empty. See [Volume and inputs](volume.md).

## Not in the file

There is no `desired_state`. A new session always starts running, and `mftiko session end` ends it. There are no `selectors`. The API does not accept them.
