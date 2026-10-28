# pretrained

A strategy that loads a pretrained model from a read-only session input. It
never submits or cancels an order, and it does not use `params`.

The model is a directory with `weights.json` at its top, a JSON object. It is
uploaded once as a data artifact. `mftiko-pack data` prints the manifest
digest on stdout:

```bash
mftiko-pack data -dir ./model -repo registry.example/acme/models/abc -tag v1
```

The session file names that digest under `inputs`. The platform unpacks the
artifact read-only before the process starts, and `ctx.input_dir("model")` is
where it lands. [`session.yaml`](session.yaml) is a session file for this
strategy. Its registry and digest are placeholders.

`OnInit` reads `weights.json` and keeps it in memory. A missing input or file
ends the session `INIT_FAILED`. The line it logs has the file's top-level
keys and the sha256 of the bytes it read. Each line is one JSON object on
stderr.

```bash
python -m mftik.run pretrained:Pretrained
```

```bash
python -m mftik.pack \
  -dir examples/python/pretrained \
  -target pretrained:Pretrained \
  -name pretrained \
  -repo 127.0.0.1:9/mftik/pretrained \
  -no-push
```
