# Examples

Five strategies live under `examples/python`: `md_watch`, `maker`, `stateful`, `pretrained`, and `sampler`. Each directory has an English README and `test_e2e.py`. A session starts the class with `python -m mftik.run module:Class`. The class is constructed with no arguments. Settings come from `params`, which is the session's `InitPayload.params` JSON. `pretrained` reads an input instead. The platform does not interpret that JSON. None of these files names a venue or a product.

Every listing below is a range of one of those files. The info string is the path and the inclusive line numbers. `scripts/check-python-doc-snippets` compares the listing to those lines. A hook records itself with `ctx.log`. That is one JSON object per line on stderr. The runtime's gate line stays on stdout.

| Strategy | Start | What it shows |
| --- | --- | --- |
| `md_watch` | `python -m mftik.run md_watch:MdWatch` | Trades, the local book, and an `after` heartbeat. It does not send orders |
| `maker` | `python -m mftik.run maker:Maker` | `submit`, `cancel`, `batch_cancel`, `open_orders`, and `position` |
| `stateful` | `python -m mftik.run stateful:Stateful` | A fill count restored with `read` and replaced with `write` |
| `pretrained` | `python -m mftik.run pretrained:Pretrained` | A model read in `on_init` from a read-only input |
| `sampler` | `python -m mftik.run sampler:Sampler` | `now` and `rand` on every hook. It does not send orders |

## md_watch

`on_init` decodes `params` and calls `resolve`. The UID from that `Info` is what the later hooks filter on.

```python examples/python/md_watch/md_watch.py:89-95
    def on_init(self, ctx: mftik.Context) -> None:
        params = _parse(ctx.params())
        info = ctx.resolve(params.ticker)
        self._ticker = params.ticker
        self._uid = info.uid
        self._every = params.every_s
        _log(ctx, {"hook": "OnInit", "ticker": params.ticker, "uid": info.uid})
```

`on_start` arms one timer. The callback runs on the dispatcher. It logs, then arms the next period. Each `after` is its own timer.

```python examples/python/md_watch/md_watch.py:97-99
    def on_start(self, ctx: mftik.Context) -> None:
        ctx.after(self._every, self._on_timer)
        _log(ctx, {"hook": "OnStart", "uid": self._uid})
```

```python examples/python/md_watch/md_watch.py:136-140
    def _on_timer(self, ctx: mftik.Context) -> None:
        _log(
            ctx, {"hook": "OnTimer", "uid": self._uid, "trades": self._trades, "books": self._books}
        )
        ctx.after(self._every, self._on_timer)
```

`on_trade` reads the `Feed`. `ev.md` is the envelope. `ev.data` is the decoded trade. A trade for another UID is ignored.

```python examples/python/md_watch/md_watch.py:104-120
    def on_trade(self, _ctx: mftik.Context, ev: mftik.Feed) -> None:
        if ev.md.uid != self._uid:
            return
        trade = ev.data
        self._trades += 1
        _log(
            _ctx,
            {
                "hook": "OnTrade",
                "uid": ev.md.uid,
                "seq": ev.md.seq,
                "trade_id": trade.trade_id,
                "price": trade.price,
                "qty": trade.qty,
                "side": int(trade.side),
            },
        )
```

`on_book` reads the local book. `book` returns `None` until the snapshot is in place. `bids` and `asks` return copies of `(tick, qty)`. `price` turns a tick into a price.

```python examples/python/md_watch/md_watch.py:70-76
def _top(view: mftik.BookView, bid: bool) -> dict[str, Any]:
    levels = view.bids(1) if bid else view.asks(1)
    if not levels:
        return {}
    tick, qty = levels[0]
    prefix = "bid" if bid else "ask"
    return {prefix: view.price(tick), f"{prefix}_tick": tick, f"{prefix}_qty": qty}
```

```python examples/python/md_watch/md_watch.py:122-134
    def on_book(self, ctx: mftik.Context, ev: mftik.BookUpdate) -> None:
        if ev.md.uid != self._uid:
            return
        self._books += 1
        view = ctx.book(self._uid)
        fields: dict[str, Any] = {"hook": "OnBook", "uid": ev.md.uid, "seq": ev.md.seq}
        if view is None:
            fields["synced"] = False
        else:
            fields["synced"] = True
            fields.update(_top(view, True))
            fields.update(_top(view, False))
        _log(ctx, fields)
```

The process is started, and the image is built without a registry push, as the example README shows. `-repo` is still required. `-no-push` does not contact it. See [Pack and deploy](pack.md).

```bash examples/python/md_watch/README.md:21-21
python -m mftik.run md_watch:MdWatch
```

```bash examples/python/md_watch/README.md:28-33
python -m mftik.pack \
  -dir examples/python/md_watch \
  -target md_watch:MdWatch \
  -name md_watch \
  -repo 127.0.0.1:9/mftik/md_watch \
  -no-push
```

## maker

The first `on_recon` for the account reads `position` and `open_orders`. Open orders of that algo are cleared with `batch_cancel`. Then `submit` places one limit buy. `side`, `order_type`, and `tif` are the proto enums. A later `on_order` calls `cancel` once that order is accepted. `on_fill` logs the fill and does not change the stored order, position, or balance.

```python examples/python/maker/maker.py:121-164
    def on_recon(self, ctx: mftik.Context, ev: mftik.ReconEvent) -> None:
        if ev.td.api_id != self._api_id:
            return
        position = ctx.position(self._api_id, self._uid)
        orders = ctx.open_orders(self._api_id, self._algo_id)
        ids = [order.client_order_id for order in orders]
        _log(
            ctx,
            {
                "hook": "OnRecon",
                "api_id": self._api_id,
                "uid": self._uid,
                "position": None if position is None else position.size,
                "open": ",".join(ids),
            },
        )
        if ids and not self._placed:
            try:
                ctx.batch_cancel(self._api_id, self._algo_id)
            except mftik.Error as exc:
                _log(ctx, {"hook": "batch_cancel_error", "reason": str(exc)})
                return
            _log(ctx, {"hook": "batch_cancel", "open": len(ids)})
        if self._placed:
            return
        try:
            cloid = ctx.submit(
                mftik.Order(
                    api_id=self._api_id,
                    algo_id=self._algo_id,
                    uid=self._uid,
                    side=Side.SIDE_BUY,
                    order_type=OrderType.ORDER_TYPE_LIMIT,
                    tif=TimeInForce.TIME_IN_FORCE_GTC,
                    qty=self._qty,
                    price=self._price,
                )
            )
        except mftik.Error as exc:
            _log(ctx, {"hook": "submit_error", "reason": str(exc)})
            return
        self._placed = True
        self._cloid = cloid
        _log(ctx, {"hook": "submit", "cloid": cloid, "qty": self._qty, "price": self._price})
```

```python examples/python/maker/maker.py:166-180
    def on_order(self, ctx: mftik.Context, ev: mftik.OrderEvent) -> None:
        cloid = ev.order.client_order_id
        if cloid != self._cloid or not self._cloid:
            return
        _log(ctx, {"hook": "OnOrder", "cloid": cloid, "status": int(ev.order.status)})
        if ev.order.status != OrderStatus.ORDER_STATUS_ACCEPTED or self._cancelled:
            return
        self._cancelled = True
        try:
            ctx.cancel(self._api_id, cloid)
        except mftik.Error as exc:
            _log(ctx, {"hook": "cancel_error", "cloid": cloid, "reason": str(exc)})
            self._cancelled = False
            return
        _log(ctx, {"hook": "cancel", "cloid": cloid})
```

```python examples/python/maker/maker.py:182-195
    def on_fill(self, _ctx: mftik.Context, ev: mftik.FillEvent) -> None:
        if ev.td.api_id != self._api_id or ev.fill.uid != self._uid:
            return
        _log(
            _ctx,
            {
                "hook": "OnFill",
                "uid": ev.fill.uid,
                "fill_id": ev.fill.fill_id,
                "cloid": ev.fill.client_order_id,
                "qty": ev.fill.qty,
                "price": ev.fill.price,
            },
        )
```

`maker` is started with `python -m mftik.run maker:Maker`. Its pack command is the same shape as `md_watch`, with `-dir examples/python/maker` and `-target maker:Maker`.

## stateful

`on_init` restores the fill count. `read` checks the volume directory first, then the `state` input, and returns `None` when neither has `fills`. The count starts at 0 in that last case.

```python examples/python/stateful/stateful.py:108-119
    def on_init(self, ctx: mftik.Context) -> None:
        params = _parse(ctx.params())
        info = ctx.resolve(params.ticker)
        self._ticker = params.ticker
        self._api_id = params.api_id
        self._uid = info.uid
        self._every = params.every_s
        raw = mftik.volume.read(ctx.volume_dir(), ctx.input_dir(_INPUT), _STATE)
        self._fills = _count(raw)
        _log(
            ctx, {"hook": "OnInit", "ticker": params.ticker, "uid": info.uid, "fills": self._fills}
        )
```

`on_fill` adds one for that account and instrument.

```python examples/python/stateful/stateful.py:125-129
    def on_fill(self, _ctx: mftik.Context, ev: mftik.FillEvent) -> None:
        if ev.td.api_id != self._api_id or ev.fill.uid != self._uid:
            return
        self._fills += 1
        _log(_ctx, {"hook": "OnFill", "fills": self._fills, "fill_id": ev.fill.fill_id})
```

`on_stop` and the timer both replace `fills` with `write`. The bytes are JSON. The timer then arms the next `after`.

```python examples/python/stateful/stateful.py:131-141
    def on_stop(self, ctx: mftik.Context) -> None:
        self._save(ctx)
        _log(ctx, {"hook": "OnStop", "fills": self._fills})

    def _on_timer(self, ctx: mftik.Context) -> None:
        self._save(ctx)
        _log(ctx, {"hook": "OnTimer", "fills": self._fills})
        ctx.after(self._every, self._on_timer)

    def _save(self, ctx: mftik.Context) -> None:
        mftik.volume.write(ctx.volume_dir(), _STATE, _dump(self._fills))
```

`stateful` is started with `python -m mftik.run stateful:Stateful`. Its pack command uses `-dir examples/python/stateful` and `-target stateful:Stateful`.

## pretrained

`on_init` reads `weights.json` from `ctx.input_dir("model")` and keeps it in memory. The input is a data artifact uploaded with `mftiko-pack data`. [Volume and inputs](volume.md#pretrained-models) quotes the hook, the upload command, and the session file's `inputs`.

`pretrained` is started with `python -m mftik.run pretrained:Pretrained`. Its pack command uses `-dir examples/python/pretrained` and `-target pretrained:Pretrained`. Besides its own `test_e2e.py`, `tests/python/data_input_e2e.py` packs it and runs it from the rootfs against a real data artifact.

## sampler

`on_init` keeps the session's `random.Random`. Each hook logs `now` twice (the same start time), whether `rand` returned that same object, and one `getrandbits(64)` draw.

```python examples/python/sampler/sampler.py:73-77
    def on_init(self, ctx: mftik.Context) -> None:
        params = _parse(ctx.params())
        self._every = params.every_s
        self._rng = ctx.rand()
        self._stamp(ctx, "OnInit")
```

```python examples/python/sampler/sampler.py:79-81
    def on_start(self, ctx: mftik.Context) -> None:
        ctx.after(self._every, self._on_timer)
        self._stamp(ctx, "OnStart")
```

```python examples/python/sampler/sampler.py:90-104
    def _stamp(self, ctx: mftik.Context, hook: str) -> None:
        rng = self._rng
        if rng is None:
            raise RuntimeError("sampler: rand is missing")
        now = ctx.now()
        _log(
            ctx,
            {
                "hook": hook,
                "now": now,
                "again": ctx.now(),
                "same": ctx.rand() is rng,
                "draw": rng.getrandbits(64),
            },
        )
```

`sampler` is started with `python -m mftik.run sampler:Sampler`. Its pack command is the same shape as `md_watch`. `replay` of a recorded `eventlog` directory against this class is the Python replay example. See [Replay](replay.md).

```bash examples/python/sampler/README.md:20-20
python -m mftik.run sampler:Sampler
```

```bash examples/python/sampler/README.md:24-29
python -m mftik.pack \
  -dir examples/python/sampler \
  -target sampler:Sampler \
  -name sampler \
  -repo 127.0.0.1:9/mftik/sampler \
  -no-push
```

