# Deribit — unified venue, Spot + linear/inverse perps + dated

Deribit is one venue with one HMAC credential (Client ID + Client Secret,
no passphrase). Identity is `{Spot, Perp, Inverse, Future}`. Routing is
not a second socket: one public WS and one private WS, and the wire name
(`instrument_name`) picks the book. Options, combos, Starbase/FIX, demo
hosts, subaccount switching, and wallet transfers are out of scope.

The wallet is **per currency**, not per product. There is no funding
account and no UTA-style split. Connect reads
`private/get_account_summaries` and reports one `Balance` per currency.

## Extra verification (measured)

| # | Fact | Measured answer | Ticket |
|---|---|---|---|
| V1 | WS `client_signature` timestamp unit | **Unix-milliseconds.** Published vector `1576074319000` / `1iqt2wls` / empty data → `56590594f97921b09b18f166befe0d1319b198bbcdad7ca73382de2f88fe9aa1`. String is `timestamp + "\\n" + nonce + "\\n" + data`. HTTP `deri-hmac-sha256` also signs `METHOD\\nURI\\nBODY\\n` — a different formula. | DRB-2 |
| V2 | Platform symbol vs wire name | **`base+quote`.** `BTC_USDC` → `Deribit_Spot_BTCUSDC` (`exch_ticker=BTC_USDC`). `BTC_USDC-PERPETUAL` → `Deribit_Perp_BTCUSDC` (`exch_ticker=BTC_USDC-PERPETUAL`). `BTC-PERPETUAL` → `Deribit_Inverse_BTCUSD`. Dated: wire `BTC-6SEP26` / `BTC_USDC-6SEP26` → `Deribit_Future_BTCUSD-260906` / `Deribit_Future_BTCUSDC-260906`. The `_` stays on the wire only; platform `expiry_code` is `YYMMDD`. | DRB-3 |
| V3 | Linear vs inverse vs dated | Linear perp: `kind=future`, `settlement_period=perpetual`, `instrument_type=linear`. Inverse perp: `reversed` / `quote=USD` / `settlement=BTC` / `min_trade_amount=10` USD (`BTC-PERPETUAL`). Dated rows have `settlement_period` in `{day, week, month}` — linear quote USDC, inverse quote USD. Live 2026-09-06: 38 linear perps, 2 inverse perps, 54 linear dated, 24 inverse dated. Options stay unlisted. | DRB-3 |
| V4 | Public / private sockets | **One of each.** Channel carries `instrument_name` or `kind.currency`. USDC and USD books do not split hosts. | DRB-4 |
| V5 | Where funding and OI arrive | **Ticker fields** (`current_funding`, `funding_8h`, `open_interest`) on REST `public/ticker` and WS `ticker.{name}.100ms`. Shared-wire like Bybit/Bitget (MDS-1). Dedicated `perpetual.{name}` is not subscribed. REST history: `public/get_funding_rate_history` (perp and inverse; dated returns HTTP 400). OI snapshot: the ticker row, including dated. Dated tickers omit funding fields. | DRB-4 |
| V6 | Order `amount` unit | Linear and spot are **base coin**. Inverse and inverse-dated are **USD** (`min_trade_amount=10`, `contract_size=10` on BTC). Docs that say “perpetual amount is USD” describe inverse. **`quote_qty` is refused.** v1 sends `amount`, never `contracts`. | DRB-5 |
| V7 | `post_only` default | **`true` on `private/buy` / `private/sell`.** Non-`POST_ONLY` must send `post_only=false`. `POST_ONLY` sends `post_only=true` and `reject_post_only=true` (CBE otherwise `post_only_not_allowed`). | DRB-5 |
| V8 | Margin models that can trade | Accept `segregated_sm`, `segregated_pm`, `cross_sm`, `cross_pm`. Missing model is logged, not refused. One-way net positions; no `posSide`. | DRB-5 |
| V9 | Wallet read | Only `private/get_account_summaries` / `user.portfolio.{currency}`. `free=available_funds`, `locked=max(0, equity - available_funds)`. Never a funding-account call. Transfers stay out of scope. | DRB-5 |
| V10 | `currency=any` on instruments | Legal. `public/get_instruments` is 1 rps / 10k credits. SYM uses `currency=any` four times (one source per book; three of them share `kind=future`). `currency=USDT&kind=future` was empty on 2026-09-06; empty is not a bug. | DRB-3 |
| V11 | Error codes | JSON-RPC `error.code` integers. Unmapped codes pass through. At least `10000`, `10004`, `10009`, `10028`, `11050`, `11060`. | DRB-6 |
| V12 | CBE-routed spot | `is_cbe_routed` / `is_csr` **present only when true**. Live: `SOL_USDC`, `PAXG_USDC`, `SOL_ETH`; `BNB_USDC` inactive. Native spots omit both fields. Test for presence, not `== false`. Still listed and tradeable. | DRB-3 |

V2 and V3 did not contradict the constants this doc assumed for identity.

## Acceptance matrix

| | Trade | ticker | trade | book | quote | kline | aggtrade | liq | funding | OI snapshot |
|---|---|---|---|---|---|---|---|---|---|---|
| **Deribit Spot** | yes | yes | yes | yes | yes | yes | — | — | — | — |
| **Deribit Perp (linear)** | yes | yes | yes | yes | yes | yes | — | — | yes (ticker) | yes (REST + ticker) |
| **Deribit Inverse** | yes | yes | yes | yes | yes | yes | — | — | yes (ticker) | yes (REST + ticker) |
| **Deribit Future (dated)** | yes | yes | yes | yes | yes | yes | — | — | — | yes (REST + ticker) |

`yes` means the adapter serves it. `—` means refused by name.

A live place/cancel pass was **not** run in this environment (no test
key). Public instruments / tickers were probed against
`www.deribit.com` when the constants above were locked.

## Invariants

I1–I10 are tests: registry (`test_venues.py`), listing filter
(`test_deribit_protocol.py`), one source per book (`test_sources.py`),
connect / qty / post_only / balances (`test_deribit_private.py`),
refuse-by-name (`test_deribit_public.py`, `test_md_deribit_reads.py`),
no Deribit channel names outside `mftik.exchange.deribit`
(`test_md_imports_no_venue_channel_or_stream_module`).
V1 and subscribe correlation are `test_deribit_socket.py` and the
public-stream cases in `test_deribit_public.py`.
