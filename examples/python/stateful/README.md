# stateful

A strategy that keeps a counter of fills it has handled, and carries that
counter to the next incarnation.

```json
{"ticker": "Ex_Spot_ABC", "api_id": 7, "every_s": 30}
```

`ticker` is a universal ticker. `api_id` is the account whose fills count.
`every_s` is how often the timer saves, in seconds, and must be above 0.
The instrument and the account come from params.

Startup reads one file, `fills`, in this order (PROTOCOL §9.1.4):

1. The volume directory, which the platform keeps across incarnations.
2. The `state` input (`ctx.input_dir("state")`), when the volume file is absent.
3. Zero, when neither directory has the file.

The bytes are JSON, `{"fills": N}`. `on_fill` adds one for that account and
instrument. The `after` timer and `OnStop` each replace the volume file
atomically (`mftik.volume.write`). A crash before the replace leaves the
previous file. Stdout is one JSON line per hook.

```bash
python -m mftik.run stateful:Stateful
```

```bash
python -m mftik.pack \
  -dir examples/python/stateful \
  -target stateful:Stateful \
  -name stateful \
  -repo 127.0.0.1:9/mftik/stateful \
  -no-push
```
