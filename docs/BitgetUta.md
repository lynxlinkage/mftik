# Bitget UTA — unified venue, Spot + linear perps

Bitget is one venue with one HMAC+passphrase credential. Identity is
`{Spot, Perp}`. Routing is not: the wire still splits `USDT-FUTURES` and
`USDC-FUTURES`. Classic v2, Coin-M, dated futures, margin, RSA, demo hosts,
and funding-account balances are out of scope.

## Extra verification (measured)

| # | Fact | Measured answer | Ticket |
|---|---|---|---|
| V1 | Private WS login `timestamp` unit | **Unix-seconds.** Published login example `"1538054050"`; Java sample `System.currentTimeMillis() / 1000`. REST is milliseconds. Docs prose says ms; we do **not** send ms on login. | BG-2 |
| V2 | `USDC-FUTURES` perpetual `symbol` | **`BTCPERP`, `ETHPERP`, …** — not `BTCUSDC` and not a second `BTCUSDT`. `quoteCoin=USDC`. Zero overlap with USDT-FUTURES. Platform ticker is `Bitget_Perp_BTCUSDC` because `symbol = base+quote`. `exch_ticker` is `BTCPERP`. Identity holds. | BG-3 |
| V3 | Mix of perpetual and delivery | Live USDT-FUTURES and USDC-FUTURES rows are all `type=perpetual`. No delivery rows now. The Perp source still drops `type != "perpetual"`. | BG-3 |
| V4 | Public WS `instType` | `spot` / `usdt-futures` / `usdc-futures`. **Three sockets.** USDC does not share the USDT socket. | BG-4 |
| V5 | Where funding and OI arrive | **Ticker fields** (`fundingRate`, `openInterest`) on REST tickers and WS `ticker`. Shared-wire like Bybit (MDS-1). `stream_funding_rate` / `stream_open_interest` are second pumps on the ticker subscribe. REST: `GET /api/v3/market/history-fund-rate`, `GET /api/v3/market/open-interest`. | BG-4 |
| V6 | Spot market-buy `qty` unit | Docs: spot market **buy** `qty` is quote; limit and market sell are base; USDT/USDC perps are base. **Decision:** if only base `qty` on a spot market buy, **refuse**; if `quote_qty` is set, send that as `qty`. | BG-5 |
| V7 | `holdMode` default | Cannot measure without a demo key. Settings example: `one_way_mode`. Connect reads settings and refuses if `holdMode` is missing. Do not guess. | BG-5 |
| V8 | `accountMode` that can trade | Accept `unified` and `hybrid`. Refuse `upgrading` / `switching` / anything else. Hybrid accepted from docs + settings example; live place not run (no demo key in CI). | BG-5 |
| V9 | UTA assets vs funding | Only `GET /api/v3/account/assets`. Never `funding-assets`. | BG-5 |
| V10 | GET query signing order | Query keys **sorted alphabetically**. Signed query is put on the path so httpx cannot reserialise. | BG-2 |
| V11 | Error code table is v3 | v3 codes only. Unmapped codes pass through. At least `40085`. | BG-6 |

V1 and V2 did not contradict the constants this doc assumed for identity.
V2 changed the *venue* symbol for USDC-M (`BTCPERP`), not the platform ticker
(`Bitget_Perp_BTCUSDC`).

## Acceptance matrix

| | Trade | ticker | trade | book | quote | kline | aggtrade | liq | funding | OI snapshot |
|---|---|---|---|---|---|---|---|---|---|---|
| **Bitget Spot** | yes | yes | yes | yes | yes | yes | — | — | — | — |
| **Bitget Perp (USDT)** | yes | yes | yes | yes | yes | yes | — | yes | yes (ticker) | yes (REST + ticker) |
| **Bitget Perp (USDC)** | yes | yes | yes | yes | yes | yes | — | yes | yes (ticker) | yes (REST + ticker) |

`yes` means the adapter serves it. `—` means refused by name.

A live UTA place/cancel pass was **not** run in this environment (no demo
key). Public instruments / tickers / candles / OI / funding were probed
against `api.bitget.com` when the constants above were locked.

## Invariants

I1–I10 are tests: registry (`test_venues.py`), `product_of`
(`test_bitget_protocol.py`), one Perp source (`test_sources.py`),
connect gate / qty / hedge / UTA balances (`test_bitget_private.py`),
refuse-by-name (`test_bitget_public.py`, `test_md_bitget_reads.py`),
no Bitget channel names outside `mftik.exchange.bitget`
(`test_md_imports_no_venue_channel_or_stream_module`).
