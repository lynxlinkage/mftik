# Deribit — unified venue, Spot + linear/inverse perps + dated + listed Option

Deribit is one venue with one HMAC credential (Client ID + Client Secret,
no passphrase). Identity is `{Spot, Perp, Inverse, Future, Option}`.
Routing is not a second socket: one public WS and one private WS, and
the wire name (`instrument_name`) picks the book. Options are listed on
the symbol plane. TD refuses Option until sizing is measured. MD serves
ticker / trade / book / quote / kline / OI / greeks; funding stays
refused.
Combos, Starbase/FIX, demo hosts, subaccount switching, and wallet
transfers stay out of scope.

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
| V10 | `currency=any` on instruments | Legal. `public/get_instruments` is 1 rps / 10k credits. SYM uses `currency=any` five times (one source per book; three share `kind=future`; Option uses `kind=option&expired=false`). `currency=USDT&kind=future` was empty on 2026-09-06; empty is not a bug. | DRB-3 |
| V11 | Error codes | JSON-RPC `error.code` integers. Unmapped codes pass through. At least `10000`, `10004`, `10009`, `10028`, `11050`, `11060`. | DRB-6 |
| V12 | CBE-routed spot | `is_cbe_routed` / `is_csr` **present only when true**. Live: `SOL_USDC`, `PAXG_USDC`, `SOL_ETH`; `BNB_USDC` inactive. Native spots omit both fields. Test for presence, not `== false`. Still listed and tradeable. | DRB-3 |
| V13 | Option identity and quote | One `Option` book. Platform quote is `counter_currency` (inverse `USD`, linear `USDC`), not `quote_currency` (inverse options quote the coin). Wire `BTC-13SEP26-70000-C` → `Deribit_Option_BTCUSD-260913-70000-C`; `BTC_USDC-13SEP26-70000-C` → `Deribit_Option_BTCUSDC-260913-70000-C`; `AVAX_USDC-13SEP26-6d4-C` → `Deribit_Option_AVAXUSDC-260913-6D4-C`. `strike` and `option_type` (`C`/`P`) are columns on `symbol_ticker` as well as fields of the ticker. Live 2026-09-13: 5124 rows (~4.3MB; 3302 linear / 1822 reversed). Longest platform ticker 38, longest wire 25. Filters store base `tick_size` and `min_trade_amount` only. TD refuses Option by name. MD serves ticker / trade / book / quote / kline / OI / greeks on the existing channels; greeks and OI ride `ticker.{instrument}.100ms` (IV is a decimal fraction; Deribit percent ÷ 100). Funding stays refused. | DRB-7 |
| V14 | Option empty sides and greeks units | Live 2026-09-27. An empty side is `best_*_price: 0.0`, `best_*_amount: 0.0` and `*_iv: 0.0` on `ticker` (`null` in book summary). MD maps an empty side to `0` on `Ticker` (no `last` fallback on Option), to `price == qty == 0` on `BestQuote` (still pushed), and to `None` on `Greeks.*_iv`. Greeks: BS / Black-76 on `underlying_price` (the forward named by `underlying_index`, not `index_price`), per 1 base, in USD for inverse **and** linear; delta not premium-adjusted; vega per vol point; theta per calendar day (near expiry it is capped at −V: 1-day OTM −0.41349 vs analytic −2.57); rho per rate point. Greeks rounded to 5 dp — ATM BTC gamma `4.59e-5` arrives `5.0e-5`. `mark_price` is BTC on inverse, USDC on linear. Those units are the shared `Greeks` convention. | DRB-7 |
| V15 | Concurrent attach on the public socket | PR #122 test report: ~11 feeds attaching at once put the socket in a reconnect loop (`cannot call recv while another coroutine is already running recv`, 36–47 reconnects per run; feeds died silently). Two readers: pumps raced `DeribitPublicClient.feed()` / `connect()`, and any `request` made between read loops fell into `handshake` and called `recv` next to `_restore`. Now one connect under a lock, and only connect / reconnect code may `handshake`; everyone else waits for the read loop (`_ready`). Pinned by `test_deribit_socket_race.py`. | DRB-4 |

V2 and V3 did not contradict the constants this doc assumed for identity.

## Acceptance matrix

| | Trade | ticker | trade | book | quote | kline | aggtrade | liq | funding | OI snapshot | greeks |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **Deribit Spot** | yes | yes | yes | yes | yes | yes | — | — | — | — | — |
| **Deribit Perp (linear)** | yes | yes | yes | yes | yes | yes | — | — | yes (ticker) | yes (REST + ticker) | — |
| **Deribit Inverse** | yes | yes | yes | yes | yes | yes | — | — | yes (ticker) | yes (REST + ticker) | — |
| **Deribit Future (dated)** | yes | yes | yes | yes | yes | yes | — | — | — | yes (REST + ticker) | — |
| **Deribit Option** | — | yes | yes | yes | yes | yes | — | — | — | yes (REST + ticker) | yes (ticker) |

`yes` means the adapter serves it. `—` means refused by name.

Dated futures and options carry `SymbolInfo.expiry`. MD drops every
feed on that ticker at the listed time and notifies STS with
`md.feed.end` (`state=expired`) → `on_feed_end`, one print per topic.
That is not a subscribed topic; see `docs/MdExpiry.md`.

A live place/cancel pass was **not** run in this environment (no test
key). Public instruments / tickers were probed against
`www.deribit.com` when the constants above were locked.

## Invariants

I1–I10 are tests: registry (`test_venues.py`), listing filter
(`test_deribit_protocol.py`), one source per book (`test_sources.py`),
connect / qty / post_only / balances (`test_deribit_private.py`),
refuse-by-name for Trade / funding (`test_deribit_public.py`,
`test_md_deribit_reads.py`; Option MD is served),
no Deribit channel names outside `mftik.exchange.deribit`
(`test_md_imports_no_venue_channel_or_stream_module`).
V1 and subscribe correlation are `test_deribit_socket.py` and the
public-stream cases in `test_deribit_public.py`.
