# sampler

A strategy that stamps each hook with `ctx.now` and one draw from
`ctx.rand`. It never submits or cancels an order, and it does not
resolve an instrument.

`InitPayload.params` is JSON. The strategy reads it; nothing here is a
fixed venue or product.

```json
{"every_s": 30}
```

`every_s` is the heartbeat period in seconds and must be above 0.
`OnInit` keeps the session's `random.Random`. Each hook logs `now`
twice (the same start time), whether `rand` returned the same object,
and one `getrandbits(64)` draw. Each line is one JSON object on stderr.

```bash
python -m mftik.run sampler:Sampler
```

```bash
python -m mftik.pack \
  -dir examples/python/sampler \
  -target sampler:Sampler \
  -name sampler \
  -repo 127.0.0.1:9/mftik/sampler \
  -no-push
```
