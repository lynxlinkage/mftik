# maker

A trading strategy that shows the order calls. It does not read the book.
The limit price and the size come from params, as do the account and the
instrument. Nothing in this directory names a venue or a product.

```json
{"ticker": "Ex_Spot_ABC", "api_id": 7, "algo_id": 1, "qty": 1, "price": 10}
```

`ticker` is a universal ticker. `api_id` is the session account. `algo_id`
is one algo declared for that account (1 through 1023). `qty` and `price`
must be finite and above 0.

`OnInit` resolves the ticker. The first `on_recon` reads `position` and
`open_orders`. Open orders for that algo are cleared with `batch_cancel`
(an earlier run can leave one on the exchange). It then `submit`s one
limit buy. `on_order` calls `cancel` when that order is accepted. `on_fill`
logs the fill. Each step is one JSON line on stdout.

```bash
python -m mftik.run maker:Maker
```

```bash
python -m mftik.pack \
  -dir examples/python/maker \
  -target maker:Maker \
  -name maker \
  -repo 127.0.0.1:9/mftik/maker \
  -no-push
```
