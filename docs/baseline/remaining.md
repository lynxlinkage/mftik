# remaining — 清場後的基線（RM-10、issue #173）

> **基準：** `refactor/process-planes`，RM-01 到 RM-09（#266、#268、#262、#270、#263、#264、#269、#261、#267，加 bug 修正 #265）全部合併之後。
>
> 「§」和「F」指 `ARCHITECTURE_CHANGE_PLAN.md` 的章節與決策編號，票號用 `REFACTOR_TICKETS.md` 的編號加 issue 號。現況的狀態權威見 `docs/baseline/state-authority.md`（B0-04），協定型別的去向見 `docs/baseline/protocol.md`（B0-03）；這兩份是 `main` @ `a0cbfb2` 的 as-is 快照，**不隨 RM 更新**，所以它們描述的 lease、attach、rebuild 在本文的基準上已經不存在。

## 1. 怎麼讀這份文件

RM 的目標是「三個平面都還能啟動，只是沒有 session 機制」。這份文件回答「那還剩下什麼」，給 B2 和 IF 當起點。

每一節一個平面，表格的最後一欄是**去向**，只有三種值：

- **搬進新層（票號）** —— 代碼要動，由那張票負責。
- **保留不動** —— 重構不碰它，行為也不變。
- **沒有票負責** —— 本文不編新票，集中列在 §10。

另外用兩個標記：

- **〔生產死碼〕** —— 模組還在、`ruff` 和測試都綠，但生產路徑上沒有任何建構者或呼叫端了。RM 刻意留下來給後面的批次搬，不是漏刪。這是 RM 之後最容易誤讀的一類，所以逐個標出來。
- **〔零呼叫端〕** —— 單一函式沒有呼叫端（含測試以外），但所在模組是活的。

行數是本文基準上 `wc -l` 的實測值。

## 2. STS 平面（`apps/sts/src/mftik_sts/`）

平面進程啟動後服務 health、registry、env、artifacts、eventlog 這些不依賴 session 的 RPC；`sts.session.*` 五個型別都還註冊在 router 上，handler 一律 `raise NotImplementedError("IF-04")`。

| 模組 | 行數 | 去向 |
|---|---|---|
| `app.py` | 314 | 保留，接線由 IF-04（#182）、B4-02（#202）改寫成 controller。開機的 schema 守衛（`schema_is_current`、`_schema_wait_s`）與 `run_rpc` 依 RM-04 的補正留下；`sweep_loop` 是 RM-04 從 `reap_loop` 拆出來的 artifact 清理，留在平面 |
| `rpc/router.py` | 119 | 保留；session 型別改名時隨 IF-01（#179）、IF-04 改 |
| `rpc/sessions.py` | 70 | 全部是占位 → IF-04（#182） |
| `rpc/health.py` | 22 | 保留不動。IF-02（#180）說要挑一個小 RPC 當 `serve(broker, subject, handler)` 的範例，這是它 |
| `rpc/env.py` | 183 | 保留不動（RM-04 明文留下） |
| `rpc/registry.py` | 328 | 保留不動 |
| `rpc/artifacts.py` | 336 | 保留。B5-07（#216）搬的是**策略端**寫 artifact 的路徑；這裡是 operator / API 的上傳路徑，新架構的平面不再持有 session，它掛在哪裡還沒有票 → §10 第 3 項 |
| `rpc/eventlog.py` | 214 | 保留 → B5-02（#211）。`info` 回的 `live` 旗標自 RM-04 起恆為 false，要等 B4-02 有 worker status 才能答得出來（檔案裡已註明） |
| `runtime_env.py` | 222 | 保留不動。`ensure_deployable`〔零呼叫端〕——它原本的三個呼叫點全在被刪掉的 `session/manager.py` 裡，現在只有 `test_sts_runtime_env.py`（16 個）在測。准入檢查要接回來的地方是 IF-04（#182）、B4-02（#202） |
| `registry_catchup.py` | 58 | 保留不動 |
| `session/session.py` | 882 | 〔生產死碼〕RM-04 刪掉 `apps/sts/src/mftik_sts/db.py` 和 manager 之後，`StsSession` 只剩測試會建構。整份搬進 ingress / strategy 兩條 thread → B4-03（#203） |
| `session/__init__.py` | 7 | 同上 |
| `impl/chase.py`、`cross_arb.py`、`macd_dollar.py`、`noop.py`、`oco.py`、`twap.py` | 974 / 879 / 969 / 403 / 804 / 529 | 保留 → B5-08（#217）。六支都還在 `on_recon_done` 才開始交易，RM-02 的補正把這一步和那 111 個策略測試一起移到 B5-08 |
| `impl/tape_keeper.py` | 103 | 退役 → B8-05（#242）。常駐訂閱上線後連模組和測試一起刪 |
| `impl/__init__.py` | 199 | 保留不動（策略目錄的 catalog） |
| `strategy.py`、`timer.py` | 16 / 12 | 保留不動。`mftik_sts.strategy` / `mftik_sts.timer` 是 `Strategy` 和 `TimerToken` 的舊 import 路徑，磁碟上的策略樹還在用（`mftik.registry.gate` 也認這個模組名） |

**沒有留下的**：`spawn.py`、`worker.py`、`session/manager.py`、`db.py`。

## 3. TD 平面（`apps/td/src/mftik_td/`）

平面進程啟動後只服務 `td.health`，加上一個常駐的 backfill session。`td.session.attach` / `detach` / `list` 和 `td.order.*`、`td.oms.*`、`td.ledger.*` 的 handler 全部隨 `session/manager.py` 消失——**型別還在 protocol 裡，但 TD 側沒有任何 handler**，所以打過去會收到 `unknown_type` 的 `td.error`。

| 模組 | 行數 | 去向 |
|---|---|---|
| `app.py` | 193 | 保留，接線由 IF-12（#190）、B4-05（#205）改寫。目前只接 RPC、health、heartbeat 和 `BackfillSession` |
| `rpc/router.py` | 48 | 保留；只剩 `td.health` 一個 handler → IF-11（#189）接回下單與帳號 RPC |
| `rpc/health.py` | 22 | 保留不動。RM-06 拿掉了它的 `api_ids` describe（那份清單來自被刪的 manager） |
| `session/session.py` | 1,440 | 〔生產死碼〕每個帳號的 OMS、ledger、recon、下單與事件廣播。整份搬進交易層 → B4-05（#205）、B6-02（#220）。它還在往 `td.{api_id}.global`、`td.oms.{api_id}` 發佈，但現在沒有進程會建構它 |
| `session/factory.py` | 329 | 〔生產死碼〕`api_id` → venue client → `Session` 那一層。F35 的常駐層要用 → B4-05（#205）。RM-06 的補正把它和 `test_venue_factory.py`（17 個）一起從刪除清單移出 |
| `session/settled.py` | 71 | 〔生產死碼〕`view_when_settled` 是 RM-06 從 `_handle_recon` 抽出來保留的等 settled 邏輯，目前只有 `session/__init__.py` 匯出它，沒有呼叫端 → IF-11（#189）的 `view(settled=True)`、B6-08（#226） |
| `session/__init__.py` | 17 | 同上 |
| `oms/oms.py`、`oms/ledger.py`、`oms/view.py`、`oms/__init__.py` | 147 / 154 / 5 / 17 | 〔生產死碼，經 `session.py`〕搬進交易層 → B4-05（#205）、B6-02（#220） |
| `history.py` | 403 | 一半活、一半死。`Scope`、`order_row`、`fill_row` 由 `backfill/executor.py` 在用；`HistoryWriter` 的唯一建構者是 `session/session.py`〔生產死碼〕→ B4-05（#205） |
| `backfill/executor.py`、`reader.py`、`session.py`、`trigger.py`、`__init__.py` | 457 / 889 / 210 / 98 / 36 | 搬進常駐層 → B6-05（#223）。`trigger.py` 的 docstring 還把「detach」當成要 backfill 的理由之一（`trigger.py:1`、`:3`，以及 `session.py:154`、`:186`）；detach 在 RM-06 之後不存在，排程是唯一的觸發來源，文字由 B6-05 一併改 |
| `db.py` | 19 | `get_api` 由 `app.py` 接線給 backfill；`count_live_for_api`〔零呼叫端〕—— `routes/apis.py:delete_api` 用的是 `TdSessionRepository.count_live_for_api`，不是這個包裝。RM-06 明文把它列進「留下」，所以本票不刪，記在這裡 → §10 第 1 項 |
| `errors.py` | 769 | 保留不動（venue 錯誤正規化） |
| `publish/__init__.py` | 1 | 只有一行 docstring 的空套件，RM-06 的「留下」列了 `publish/`。實際的發佈在 `session/session.py` 裡 → B4-05（#205）一併處理 |

**沒有留下的**：`session/manager.py`、`rpc/sessions.py`。

## 4. MD 平面（`apps/md/src/mftik_md/`）

整個 `session/` 套件消失，MD 現在只服務 `md.health`、`md.tape.tail` 和 `md.fetch`。沒有 venue 連線、沒有 fan-out、也沒有在錄——錄製的唯一觸發點是被刪掉的 `Dispatcher.publish`。

| 模組 | 行數 | 去向 |
|---|---|---|
| `app.py` | 254 | 保留，接線由 IF-09（#187）、IF-10（#188）、B4-06（#206）改寫。RM-05 把它改成不帶 `SessionManager`，`reap_loop` 隨 `reap_orphans` 一起刪 |
| `rpc/router.py` | 58 | 保留；RM-05 改成持有 `TapeStore` 而不是 `SessionManager` → IF-09、IF-10 |
| `rpc/health.py` | 27 | 保留不動 |
| `rpc/tape.py` | 113 | 保留；key 改成 `atom_id` → B7-04（#236） |
| `tape.py` | 183 | 一半活、一半死。`app.py` 仍然建構 `TapeRecorder`，但只為了拿它的 `store` 給 `md.tape.tail` 讀；`append` / `started` / `stopped` 的呼叫端都在被刪的 `Dispatcher` 與 `SessionManager` 裡，所以**沒有任何東西在錄**（`app.py` 裡已註明）。tape append 的介面由 IF-10（#188）定，錄製接回連線 worker 是 B4-06（#206）、B7-04（#236） |
| `tape_store.py` | 257 | 保留；讀取端（`rpc/tape.py`）是活的，寫入端同上 → B7-04（#236） |
| `fetch/readers.py`、`fetch/session.py`、`fetch/__init__.py` | 1,119 / 437 / 22 | 搬進獨立的 fetch worker → B7-05（#237） |
| `errors.py` | 279 | 保留不動 |

**沒有留下的**：`session/`（`manager.py`、`dispatcher.py`、`venue.py`、`factory.py`、`__init__.py`）、`rpc/sessions.py`、`db.py`。

`apps/md/pyproject.toml` 原本還宣告 `mftik-db`，但 `apps/md` 的 src 和 tests 都已經沒有任何 `mftik_db` 的 import（唯一的使用者 `db.py` 隨 RM-05 刪除）。本票一併移除，並更新 `uv.lock`。

## 5. API（`apps/api/src/mftik_api/`）

API 沒有被 RM 拆掉，只有四個路由改成 501 占位（`routes/sts.py` 三個、`routes/md.py` 一個），加上一個守衛變成 no-op。其餘 10,000 行左右（auth、alerts、artifacts、registry、board、logs、instances、sym、stats）**保留不動**，這裡只列會動或讀起來會誤導的部分。

| 模組 / 符號 | 去向 |
|---|---|
| `orchestrate.py`（208 行） | 〔生產死碼〕RM-08 刪掉 `deploy_strategy` 之後，整個模組沒有任何生產 import 了，留下的 `mint_session_id`、`_sts_target`、`_check_sts_instance`、`_check_md_instances`、`_answers`、`_td_instance`、`_md_venues` 只有測試在呼叫。它們是 §8.1 第 1 步的驗證，由 IF-13（#191）重接 |
| `routes/sts.py:deploy` | 501 → IF-13（#191） |
| `routes/sts.py:list_sessions`、`stop_session` | 501 → IF-04（#182） |
| `routes/md.py:list_sessions` | 501 → IF-09（#187）。**這個路由現在沒有任何客戶端**：前端和 CLI 都不呼叫它，只有 `contracts/openapi.json` 記著它 |
| `routes/environment.py:_require_no_live_sessions` | no-op。現在沒有任何東西能讓 session 是 live 的，所以守衛直接通過（函式裡已寫明為什麼不改成查 `sts_sessions`）。IF-04（#182）／B4-02（#202）落地之後要重新接上，否則 extras 的破壞性變更會在有 session 跑著的時候放行 |
| `routes/td.py`、`routes/stats.py`、`routes/board.py`、`ws.py` | 保留不動。`td_sessions`、`md_sessions` 依 F38 從 B10 起停寫、保留唯讀 → B10-01（#249） |
| `routes/sts.py:ack_session`、`routes/stats.py:get_stats`、`schemas.py` | `interrupted` 這個狀態值的讀取端 → B10-01（#249）。寫入端已隨 RM-04 的 `manager.py` 一起消失，所以現在不可能再出現新的 `interrupted` row。RM-01 的補正把這裡的讀取端寫成 `routes/sts.py` 的「`_ATTENTION`」，代碼裡沒有這個符號——`attention` 是前端送的 `status=failed,interrupted` 篩選字串 |
| `backfill_cron.py`、`registry_catchup.py`、`sts_fanout.py` | 保留不動。registry 的 fan-out 與開機補差額在計畫裡沒有對應的層 → §10 第 2 項 |

## 6. SDK 與共用套件（`packages/common/src/mftik/`）

| 模組 | 去向 |
|---|---|
| `strategy/base.py`（572） | → IF-06（#184）。`Strategy.on_recon_done` 的 hook 定義**刻意留著**：RM-02 的補正算出 224 個策略實作測試裡有 111 個靠它驅動，所以 hook 和六支內建策略的實作一起留到 B5-08（#217）。`rebuildable`、`on_rebuild`、`remember`、`send_recon` 都已刪除 |
| `strategy/oms.py`（712）、`ledger.py`（289）、`mds.py`（329）、`tape.py`（508）、`timer.py`（247）、`artifacts.py`（755）、`eventlog.py`（491）、`session.py`（62）、`symbols.py`（144）、`client_order_id.py`（172） | → IF-06（#184）、B5-01 到 B5-07。`tape.read(on_print=…)` 依 RM-03 的補正保留參數本身，只拿掉每筆讓出的 `breathe` / `slice_deadline` |
| `protocol/messages.py`（1,831）、`topics.py`（351）、`__init__.py`（658） | → IF-01（#179）。lease 相關的 model、envelope 別名與 type 常數（含舊名 `STS_HEARTBEAT`）已隨 RM-07 刪除；`md.session.attach` / `detach` / `list` 和 `td.session.attach` / `detach` / `list` 的**常數還在、兩側都沒有實作了**〔生產死碼〕，由 IF-01 換成 intent。`Topics.md_session` 保留成 subject 名稱產生器（`test_nats_transport`、`test_query_codes`、`scripts/loop_bench.py` 拿它當名字用），發佈端已隨 RM-05 消失 |
| `protocol/strategy_yml.py`（502） | → IF-07（#185）。RM-09 之後 `StrategySpec.restart` 的預設值是 `never`、`RESTART_MODES` 只有 `{never}`，`restart: always` 會得到指向新寫法的錯誤訊息。`on_failure`、`max_restarts`、`restart_window_s` 等新欄位由 IF-07 加 |
| `protocol/envelope.py`（69）、`reject_codes.py`（198）、`query_codes.py`（166）、`session_log.py`（115）、`strategy_catalog.py`（273） | 保留；`pv` 欄位與 `protocol_mismatch` 由 IF-01（#179）加 |
| `broker/client.py`（245）、`transport/nats.py`（363）、`transport/base.py`（144）、`request.py`、`errors.py`、`config.py` | 保留 → IF-02（#180）加 `mftik.broker.handler`。`link.py` 和 `Broker.leased_link` 已隨 RM-07 刪除 |
| `exchange/`（40,856 行，八個 venue） | 保留 → IF-08（#186）每個 venue 加 `atoms.py`、B7-02a 到 B7-02g（#228–#234）實作。`exchange/wire.py`（768）搬進連線 worker 的 reconciler → B7 |
| `registry/`、`symbols/`、`environment.py`、`envapply.py`、`envimport.py`、`instance.py`、`health.py`、`runtime.py` | 保留不動 |

**還不存在的新層**：`mftik.clock`（B2-02，#175）、`mftik.broker.handler`（IF-02）、`mftik.procman`（IF-03，#181）。

## 7. CLI（`packages/common/src/mftik/cli/`）

| 指令 / 模組 | 去向 |
|---|---|
| `sessions.py:list_sessions`（`mftik ps`） | 打 `GET /sts/sessions`，現在是 501 → B10-03／IF-04（#182） |
| `sessions.py:stop_session`（`mftik stop`） | 打 `POST /sts/sessions/{id}/stop`，現在是 501 → IF-04（#182） |
| `run.py:run`（`mftik run`） | 打 `POST /sts/deploy/{type}`，現在是 501。接著還會 `POST /sts/sessions/{id}/stop`（Ctrl-C 的路徑）→ IF-15（#193）加 `--wait` / `--no-wait`、IF-13（#191）接上 start。RM-09 已經把 `deploy_http_timeout` 的估算換成固定的短 timeout |
| `sessions.py:logs` / `follow_logs`（`mftik logs`） | 保留不動（讀 `/logs/sts/…` 與 `/ws/sts/…`，兩者都還在） |
| `app.py`、`check.py`、`push.py`、`rm.py`、`env.py`、`connect.py`、`node.py`、`alert.py`、`artifact.py`、`init.py`、`tree.py`、`config.py`、`profiles.py`、`client.py`、`output.py`、`exits.py`、`registry_migrate.py` | 保留不動。`mftik workers`、`mftik md restart`、`mftik td drain`、`mftik intents gc` 是 IF-15（#193）新增 |

## 8. 前端（`frontend/src/`）

沒有任何 RM 票改前端。三個會打到 501 的呼叫還在，由 B10-03（#251）和對應的 IF 票接回：

| 位置 | 去向 |
|---|---|
| `lib/api.ts:deploySts` → `routes/sts/+page.svelte:deploy`、`routes/strategy/+page.svelte:deploy` | 501 → IF-13（#191）、B10-03（#251） |
| `lib/api.ts:stopSts` → `routes/sts/+page.svelte`、`routes/strategy/+page.svelte`、`routes/strategy/[sessionId]/+page.svelte` | 501 → IF-04（#182）、B10-03（#251） |
| `lib/api.ts:stsSessions`（`GET /sts/sessions`） | 501，而且**前端自己已經沒有呼叫端**——頁面讀的是 `GET /sts/strategies`（DB 讀取，照常運作）。這個 export 隨 B10-03（#251）處理 |
| `interrupted` 這個狀態值：`app.css`、`routes/+page.svelte`、`routes/sts/+page.svelte`、`routes/strategy/+page.svelte`、`routes/strategy/[sessionId]/+page.svelte`、`routes/board/+page.svelte`、`routes/board/[sessionId]/+page.svelte`、`lib/api.ts`、`lib/logging/status.ts` | 讀取端 → B10-03（#251） |
| `routes/md/[venue]/+page.svelte`、`routes/td/[apiId]/+page.svelte` | 目前只是 log viewer，不打 `/md/sessions`。改成顯示 worker 與 intent → B10-03（#251） |
| `e2e/*.spec.ts` | 全部 mock `/api/sts/deploy/**` 與 session 路由，所以照常通過 → B10-03（#251） |
| 其餘頁面與 `lib/` | 保留不動 |

## 9. `packages/db`

RM 只刪各平面 `db.py` 的包裝與接線，repository 一律保留（大量 API 呼叫端）。唯一被刪的 repository 方法是 RM-01 的 `remember`、`bump_rebuild_count`、`reset_rebuild_count`。

| 項目 | 去向 |
|---|---|
| `models/session.py:StsSessionRow.st_facts` | 欄位還在，**沒有任何寫入路徑**（只有三個 API 測試的 fixture 在建 row 時給 `st_facts={}`）→ B10-01（#249）drop |
| `models/session.py:StsSessionRow.rebuild_count` | 同上，改名成 `restart_count` → B10-01（#249） |
| `models/session.py:SessionStatus.INTERRUPTED` | 寫入端已消失，讀取端見 §5 → B10-01（#249） |
| `models/session.py:MdSessionRow`、`TdSessionRow` | 寫入接線已刪，唯讀保留到 B10 → B10-01（#249） |
| `repositories/instance.py:derived_sts` | 保留。唯一的生產呼叫者是 `orchestrate.py:_sts_target`，而那整個模組是生產死碼（§5），所以現在只有 `test_derived_sts.py`（6 個，參數化後 12 個）在跑它 → IF-13（#191）重接。和 RM-04 刪掉的 `apps/sts/src/mftik_sts/db.py:derived_sts` 同名但不是同一個函式 |
| 其餘 models / repositories / migrations | 保留不動；新欄位與新表由 IF-14（#192）加 |

## 10. 沒有票涵蓋的事

1. **TD `db.py:count_live_for_api` 沒有呼叫端。** RM-06 的 commit 說明把它和 `get_api` 一起列進「留下」，但只有 `get_api` 被接線。刪它很安全，不過它落在 RM-06 的範圍裡，本票選擇記下來而不是動手；B6-01（#219）或 IF-12（#190）把常駐層接起來時，會知道要嘛用它、要嘛刪它。
2. **registry 的權威與開機補差額沒有對應的層。** `state-authority.md` §12 第 2 項已經記過。RM 之後更明顯了：`apps/api/src/mftik_api/sts_fanout.py`（721 行）和兩個 `registry_catchup.py` 是唯一橫跨 API 與 STS 的 session 外機制，而 §3.4 的新層表沒有它。
3. **平面側的 artifact 上傳路徑在新架構掛哪裡。** `state-authority.md` §12 第 3 項。B5-07（#216）只說把策略端的 artifacts 搬進新 worker。
4. **`md.session.attach` / `list` 與 `td.session.attach` / `detach` / `list` 的型別常數留在 protocol 裡，兩側都沒有實作。** IF-01（#179）的範圍是「B0-03 標成保留或改名的型別都對到新名字」，所以這幾個應該在那裡消失；本票只記下它們現在是沒有實作的常數。

## 11. 票面與代碼不符之處（RM 的處理）

RM 的票是在 `main` @ `a0cbfb2` 上寫的，落地時有幾處以代碼為準。合併後的狀態記在這裡，免得之後讀票的人以為有漏做：

1. **RM-01 整份刪除 `_spawn_rebuild` 那一塊。** 票面把 `manager.py` 的 rebuild 私有輔助列在補正裡、說「RM-04 整份刪除 `manager.py`，RM-01 單獨驗收時要接受它們仍在」；實際落地時 RM-01 就把整塊刪掉了。
2. **RM-02 保留 `Strategy.on_recon_done`，和 issue 內文相反。** 依 RM-02 的補正第三點（採用第二個選項），hook 定義與六支內建策略的實作留到 B5-08（#217）。共同驗收第 1 條的 grep 要排除 `on_recon_done`。
3. **RM-04 整份刪除 `worker.py`，並把 `reap_loop` 拆成留下的 `sweep_loop`。** 票面只列了 `worker.py` 裡的 `arm_parent_death`、`_watch_lifeline`；`app.py` 的 `reap_loop` 同時做 orphan reaper 和 artifact 清理，後者和 session 無關，所以拆出來保留。
4. **RM-06 只保留 `test_order_rpc.py` 的 1 個案例，附錄 A 寫 4 個。** 附錄 A 把 `test_a_malformed_ticker_is_left_to_the_instrument_check`、`test_a_gate_market_buy_sized_in_base_is_unsupported_shape`、`test_reduce_only_passes_on_a_contract_ticker` 算成「不經 manager」；但它們測的是 `manager.py` 的模組層函式 `_wrong_instrument`、`_place_order_request`、`_refusal_code`、`_reduce_only_unsupported`，而 RM-06 的補正明文把這些列進刪除範圍。**補正勝過附錄 A**，三個案例隨代碼刪除，由 B6-02（#220）重新實作時補回。留下的是 `test_no_td_serving_times_out`，它只測 broker 對無人服務的 subject 會逾時。
5. **RM-04 保留 `test_eventlog.py` 的 15 個與 `test_oms_wait_cids.py` 的 3 個 `StsSession` 案例，附錄 A 列的是刪除。** 這兩個檔案直接建構 `StsSession`，而 `StsSession` 依「留下」保留到 B4-03，所以案例仍然會過。本票按 RM-04 的決定記錄，不刪：`test_eventlog.py` 現在 21 個（只有 `test_lease_ack_is_recorded` 隨 lease 刪除）、`test_oms_wait_cids.py` 11 個全留。B5-02（#211）與 B6-08（#226）改寫時會一起處理。
6. **RM-08 另外刪了 `test_environment_flow.py` 的 `test_s16_silent_sts_is_not_idle` 與 `test_s17_session_arriving_mid_install_aborts`，附錄 A 只列 3 個。** 這兩個測的是 `_require_no_live_sessions` 會向 STS 問 live session；守衛變成 no-op 之後主題不存在。守衛重新接上時由 IF-04（#182）／B4-02（#202）補回。
7. **RM-09 的 `StrategySpec.restart` 預設值。** 票面範圍寫「刪除 `RESTART_ALWAYS`（也是預設值）」、補正寫「`RESTART_NEVER` 和 `RESTART_MODES` 保留，內容和預設值在 IF-07 改」。落地時直接把預設改成 `never`（F11 的新語意），而不是留一個指向被刪常數的預設值。

## 12. 重現

```sh
# RM 的共同驗收第 1 條：票上列出的符號在 apps/、packages/ 裡 grep 不到
rg -nw -g '!**/migrations/**' \
  -e rebuild_interrupted -e rebuild_session -e adopt_interrupted -e on_rebuild \
  -e rebuildable -e remember_fact -e bump_rebuild_count -e reset_rebuild_count \
  -e _lease_heartbeat_loop -e _fail_from_infrastructure -e send_recon -e STS_RECON \
  -e breathe -e slice_deadline -e SLICE_S \
  -e SubprocessSpawner -e arm_parent_death -e reap_orphans -e _create_in_process \
  -e VenueSession -e StsLink -e Dispatcher -e VenuePublicFactory \
  -e _serve_orders -e _handle_order_rpc -e _handle_recon \
  -e LeasedSessionLink -e LeaseHeartbeat -e STS_LEASE_HEARTBEAT -e MD_LEASE_ACK \
  -e deploy_strategy -e RESTART_ALWAYS -e deploy_http_timeout \
  apps packages

# 〔生產死碼〕的判定：生產樹裡誰建構它
rg -n 'StsSession\(|Session\(|HistoryWriter\(|TapeRecorder\(' --glob '!**/tests/**' apps
rg -n 'from mftik_api.orchestrate|from mftik_api import orchestrate' --glob '!**/tests/**' apps

# 四個 501 路由與它們的客戶端
rg -n 'status_code=501' apps/api/src
rg -n 'deploySts|stopSts|stsSessions' frontend/src
rg -n '/sts/sessions|/sts/deploy' packages/common/src/mftik/cli

# MD 已經不需要 mftik-db
rg -n 'mftik_db' apps/md
```
