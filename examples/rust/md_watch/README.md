# md_watch (Rust)

The Rust form of [`examples/python/md_watch`](../../python/md_watch), on `mftik-rt` (E08-D006). It resolves one instrument, logs each trade and each synced book, and logs a heartbeat from an `after` timer. It never submits or cancels an order. Unlike the Python example, it keeps its trade and book counts in the volume.

`InitPayload.params` is JSON. The strategy reads it; nothing here is a fixed venue or product.

```json
{"ticker": "Ex_Spot_ABC", "every_s": 30}
```

`ticker` is a universal ticker, `Venue_Category_Symbol`. `every_s` is the heartbeat period in seconds and must be above 0. `on_init` calls `resolve`. `on_trade` logs the trade. `on_book` reads the local book (`bids`, `asks`, `price`) once `md.view` has synced it; before that the line has `"synced": false`. Each line is one JSON object on stderr, with the fields of the Python example's line. The `OnInit` line also has the counts the strategy starts from.

The counts are the volume file `counts`, `{"trades":N,"books":N}`. `on_init` reads it, and starts from zero when it is not there. The heartbeat and `on_stop` replace it atomically, so the next incarnation of the session continues from them. A session without a volume directory keeps nothing.

```bash
cargo build --release -p mftik-md-watch
```

The binary is `target/release/md_watch`. The platform starts it with the control socket on fd 3; run from a shell, it exits 1. See [`docs/rust`](../../../docs/rust/README.md).

## Test

`test_e2e.py` runs the binary under the Python examples' harness, [`examples/python/harness.py`](../../python/harness.py): a fake shim, nats-server, and fake SYM and MD peers. It uses the Python example's scenario and checks, then reads the `counts` file. It needs `nats-server` (`MFTIK_NATS_SERVER`, or on `PATH`) and a Python 3.11 or newer that `cargo test -p mftik-py` can build against.

```bash
python3 examples/rust/md_watch/test_e2e.py
```

CI's `conformance-rootfs-rust` job packs the release binary with `mftiko-pack -bin`, pulls and unpacks the image, and runs this test with `MFTIK_MD_WATCH_ROOTFS` set to the rootfs (E08-D009). The worker is then the image's `bin/strategy`, started from the rootfs with only the shim's variables.

## Packing

```bash
cargo build --release -p mftik-md-watch
mftiko-pack -bin target/release/md_watch -name md_watch -repo registry.example/acme/md_watch
```

`mftiko-pack` prints the manifest digest, which is the session's `strategy.ref`. [Pack and deploy](../../../docs/rust/pack.md) has the session file and the `mftiko` commands.
