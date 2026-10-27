# md_watch

A market-data strategy. It resolves one instrument, logs each trade and each
synced book, and logs a heartbeat from an `after` timer. It never submits or
cancels an order. Live deployment (E05-06) starts with this strategy.

`InitPayload.params` is JSON. The strategy reads it; nothing here is a fixed
venue or product.

```json
{"ticker": "Ex_Spot_ABC", "every_s": 30}
```

`ticker` is a universal ticker, `Venue_Category_Symbol`. `every_s` is the
heartbeat period in seconds and must be above 0. `OnInit` calls `resolve`.
`on_trade` logs the trade. `on_book` reads `BookView` (`bids`, `asks`,
`price`) once `md.view` has synced the local book; before that the line has
`"synced": false`. Each line is one JSON object on stdout.

```bash
python -m mftik.run md_watch:MdWatch
```

Packing (the image is not uploaded when `-no-push` is set; `-repo` is still
required and is not contacted):

```bash
python -m mftik.pack \
  -dir examples/python/md_watch \
  -target md_watch:MdWatch \
  -name md_watch \
  -repo 127.0.0.1:9/mftik/md_watch \
  -no-push
```

## Deploying

The image is pushed and the session is run with the `mftiko` CLI (v0.2.0 or
later). [`session.yaml`](session.yaml) is the session from the first testnet
run (E05-06): the digest is the one this command printed then.

```bash
python -m mftik.pack \
  -dir examples/python/md_watch \
  -target md_watch:MdWatch \
  -name md_watch \
  -repo ghcr.io/lynxlinkage/strategies/md_watch \
  -tag v0.14.0
```

```bash
mftiko session create -f examples/python/md_watch/session.yaml --wait running
mftiko session show <session_id>
mftiko session end <session_id> --wait
mftiko session delete <session_id>
mftiko record list
mftiko record show <record_id>
```
