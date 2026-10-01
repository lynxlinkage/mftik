# ARCHITECTURE_CHANGE_PLAN — 平面進程化重構

> **狀態：v0.26（2026-10-01）**。§12 的待決事項已全部定案（F1 到 F38）；工作票見 `docs/REFACTOR_TICKETS.md`。
>
> **基準：** `main` @ `a0cbfb2`。§1 的「現況」，以及本文引用的檔案、symbol、行數和測試數，都在這個 commit 上查證過。重構在 `refactor/process-planes` 分支上進行，所有改動先合併到這個分支。README 與 `docs/` 已經過時，不作為依據。
>
> **v0.26 更正：** v0.25 以前的版本，誤用了 PR #153（`fix/sts-start-deadline` @ `8ddfc23`，未合併、已關閉）的代碼當作現況。PR #153 加入的 8 秒 / 300 秒啟動期限、`start_deadline` kill、abort 重試與 `abort_target` 欄位，都不在 main 上；相關敘述、刪除清單和 migration 已依 main 改正，行數和測試數也已重算。`deployment/nats/nats.conf` 被 `.gitignore` 排除，不在 repo 裡，§5.3 引用的是本機那份。

### 已定案

| # | 決策 | 影響 |
|---|---|---|
| F1 | 本版是**破壞性變更**，不做任何兼容層 | 協定、DB、SDK 一次切換。批次只需要在分支上保持測試全綠，不必各自可部署（§11） |
| F2 | 切換到本版前，使用者必須先停掉所有運行中的策略，平面上不能有 running session | 不需要遷移 live session，也不需要舊 worker 和新 controller 互通。切換前有 preflight 檢查（B10） |
| F3 | `on_start` 本來就允許長時間 warm-up | `on_start` 是獨佔階段，平台不在它執行期間要求任何準時的工作（§5.3） |
| F4 | 所有資源投入本重構，不在現行版本做止血修正 | 現行版本的長 hook 問題要到 B10 上線才會消失 |
| F5 | 不引入尚未成熟的技術 | 排除單一執行檔打包（PyApp、scie），也就是 §4.5 的選項 C |
| F6 | 部署採用 §4.5 的選項 A：OCI 加 `oci_host_pid`，由 controller 直接 spawn shim | Strategon 的改動（S-1 到 S-3）追蹤於 [BullionBear/strategon#60](https://github.com/BullionBear/strategon/issues/60)。**本文其後的設計都假設 #60 已完成** |
| F7 | 記憶體防護：mftik 端先做 `oom_score_adj` 分級、可選的 `RLIMIT_DATA`、准入控制（§4.7） | cgroup 上限由 [strategon#61](https://github.com/BullionBear/strategon/issues/61) 提供，**不是本重構的前提**；#61 上線後再重估每個平面的上限 |
| F8 | STS session worker = ingress thread（main thread，接收連線）+ strategy thread（送出連線，直接 publish 送單）；回覆走 ingress 的 inbox；交付策略照 §5.3 的預設 | ingress 與 session 同生共死（§5.3 的 I1 到 I4） |
| F9 | 刪除 `breathe` / `slice_deadline`；SDK 提供 `offload`，有 thread（預設）和 process 兩種模式 | §5.5 |
| F10 | 刪除 `on_rebuild`、`remember()`、`rebuildable`、`st_facts`；策略不再碰 Postgres，session 狀態由 Supervisor 寫入；crash 後先保證 `on_stop`（做得到的話）並由平台清場，再依 deploy 設定 fail 或從 `on_start` 重新掛起，同時發 alert | §5.2 |
| F11 | `restart` 預設 `never`；`on_failure` 只對 A 類 crash（策略例外、`on_stop` 有跑）生效；`max_restarts=5`、`restart_window_s=600` | §5.2 |
| F12 | `on_ready` 等所有有宣告的就緒條件成立才呼叫（TD 是硬條件、MD 是軟條件）；`on_ready` 之前下單會拋出 `NotReady`；deploy 改成 202 加狀態進度；`start_timeout_s` 只算 `on_start`（預設 60、上限 3600 秒），另設 `ready_timeout_s`（預設 30 秒） | §5.2 |
| F13 | 刪除 `send_recon`、`STS_RECON`、`on_recon_done`；需要等帳本收斂時改用 `self.oms.view(settled=True)`；新增 `on_resync(api_id, cause, view)`，只由平台在事件流可能有缺口時觸發；OMS / ledger 的權威維持在 TD 帳號 worker 的記憶體 | §5.2、§7.1 |
| F14 | MD / TD 失聯只通知、不自動 fail；新增 `on_md_update`、`on_td_update`、`self.md.state()`、`self.td.state()`；帳號 `unavailable` 時下單在本地回傳 False（`td_unavailable`）；MD 連線 worker 和 TD 帳號 worker 單向廣播狀態，靜默 10 秒視為失聯 | §5.6 |
| F15 | hook 時間預算：一般 hook 的阻塞時間超過 1 秒只發警告，超過 30 秒視為 B 類 crash；`on_start` 只受 `start_timeout_s` 約束；`on_ready`、`on_stop` 的牆鐘時間上限各 10 秒 | §5.3 |
| F16 | 策略實作測試（224 個）RM 不刪，保留到 B5 再改寫到 `StrategyHarness` 上 | 這批測試不碰 NATS、幾乎沒有真的 sleep，不會吃掉 B2 的兩分鐘預算（§9.3） |
| F17 | MD 連線 worker 一條 websocket 一個進程 | 接受「使用中的連線數 × 單一進程 RSS」的記憶體代價（§4.7、§6.3） |
| F18 | reconciler 跑在連線 worker 裡；controller 只推完整的 desired 清單，帶 `generation = (controller_epoch, seq)`，worker 只接受更大的值 | §6.2、§6.3 |
| F19 | mftik feed 對交易所 atom 是一對多。跨連線的組合由 MD 發佈原子事件、STS 端以平台通用的 join 組合，不設組合 worker；組成的 atom 任一 down，這個 feed 就是 down | §6.1、§5.6 |
| F20 | 範圍 3.1 的「把每個行情作為原子行情寫下」指的是把行情定義成 atom，不是錄 tape。tape 改以 `atom_id` 為 key，錄 `trade`、`aggtrade`、`liquidation` | §6.3 |
| F21 | MD 是行情的權威，地位對應 TD 之於 ledger：解碼、book 的 fold、late joiner 的快照都在 MD 連線 worker，`md.a.*` 上傳的是平台 model | STS 不接觸交易所原文，也不持有 fold 狀態（§6.1） |
| F22 | MD 不做連線遷移：atom 放上某條連線後就留在那裡，直到沒有 demand。controller 滾動不影響連線 | 刪除 make-before-break、I-MD1、連線整併；每個 atom 任何時刻只有一個發佈者（§4.6、§6.2、§6.3） |
| F23 | 不提供 `on_feed_gap`：漏收由策略自己記錄，平台不替策略記 | MD 斷線、連線 worker 重啟、STS ingress 重連一律只以 `on_md_update` 的 down → live 通知（§5.3、§5.6）；`all` 類 feed 佇列溢出時，策略以 `event.seq` 自己偵測（F25） |
| F24 | 既有 MD 連線 worker 換版只靠人工逐條 `restart`；平台不自動重啟舊版連線，也不為了換版打斷運行中的策略 | 升版時才知道需不需要換，而且換版本身可能有相容問題（§4.6）。另提供列出「仍在跑舊版代碼的 worker」的指令 |
| F25 | MD 事件帶 per-atom 的 `event.seq`，漏收由策略自己偵測 | `seq` 在同一個連線 worker incarnation 內連續，`on_md_update` 收到 `live` 之後重新起算；`all` 類跳號代表漏收，`latest` 類跳號是設計上的覆蓋（§5.3） |
| F26 | 跨版本只靠版號：每則 NATS 訊息帶 `pv`，格式一改就升版，不同 `pv` 一律以 `protocol_mismatch` 拒絕；不做版內相容，也不做 schema 比對 | 停止不依賴協定（SIGTERM），任何版本組合都停得掉。升版順序見 §4.6 |
| F27 | TD 帳號 worker 換版後由人工逐帳號觸發 drain-replace，平台不自動換版 | 理由同 F24（§4.6） |
| F28 | `docs/Deployment.md` 不封存，依現況重寫；venue 實測表（`Deribit`、`BitgetUta`）封存到 `docs/archive/` | B1 依現況重寫 Deployment，B10 依新架構更新（§10） |
| F29 | shim 用 Python | 只用標準庫，不 import pydantic、nats 等第三方套件，以壓低每個 shim 的 RSS（prototype 約 10–15 MB），這部分算進 §4.7 的預算 |
| F30 | 兩分鐘預算以 GitHub Actions 的 `ubuntu-latest` 為準 | 只算 `just test`（unit + component）那一步；integration 另開 job（§9.1） |
| F31 | 測試照樣用 NATS，不引入 broker fake；連線和收到之後的行為分開測，每個 xdist worker 共用一條 NATS 連線 | handler 和傳輸分開寫，行為測試直接呼叫 handler（§9.2） |
| F32 | 刪除 §8.2 的規則 4：STS controller 的報告整個停止時不回收任何東西，不從訊號缺席推論主機失聯 | 機器永久消失時由人工 `mftik intents gc --instance`（暫定）。P7 只剩 F14 的失聯通知這個只通知、不回收的例外 |
| F33 | selector：`md:` 新增 `select:`（`option_chain`、`rolling_future`），由 MD orchestrator 以純函數推導；策略只用 `on_universe_change`；暫不做 pin；不加 `required` | §6.4 |
| F34 | TD 帳號 worker 以帳號（`api_id`）為單位，一個進程持有該帳號所有私有連線 | §7.1 |
| F35 | 帳號 worker 對每個啟用帳號常駐，維持溫熱的 HTTP 連線池；refcount（intent）只開關交易層（私有 websocket、OMS、recon），不 linger；backfill 是帳號 worker 用連線池處理的一次性 request，不另開 job worker | §7.1、§7.2 |
| F36 | 帳號的 at-most-one 不用 DB lease：同 instance 由 Supervisor 以 PID 確認，跨 instance 靠 `api_id` → instance 的靜態綁定；`st_facts` 在 B10 drop | §7.1、§8.4 |
| F37 | cancel-on-disconnect 預設關閉、逐帳號開啟；只用倒數計時型機制，當作 TD worker 的死人開關；不用 Deribit COD 和 Bybit DCP | §7.1 |
| F38 | intent 兼任歷史：session 結束時 intent 列不刪、改記 `released_at`；`md_sessions` / `td_sessions` 從 B10 起停寫、保留唯讀；前端 MD/TD 頁改成顯示 worker 與 intent | §8.4；前端資料來自 procman 回報和 worker 狀態廣播 |

## 0. 摘要

這次重構把 STS/MD/TD 的執行單位，從「平面進程裡的 coroutine，或綁在平面進程上的子進程」改成「由 procman 持有、與平面 controller 生命週期解耦的 worker 進程」。

- **平面 controller 只負責控制面**：把宣告收斂成實際狀態。它可以隨每個 tag 滾動，不會中斷任何 worker。滾動時先 detach，新版啟動後再 reattach。
- **資料面只在 worker 和 NATS 之間流動**：行情、下單、回報都不經過 controller。
- **MD 的管理單位改成交易所的原子訂閱**。平台 topic（`bestquote.X`）變成原子訂閱的投影。
- **STS 取消 rebuild**。進程不會因為部署而停止，所以不需要重建。
- **API 只負責開始與結束**。中間發生的一切由各平面的 orchestrator 自行收斂。
- **測試先清掉、再依新標準重寫**。預設測試集要在兩分鐘內跑完。

整體上，這是把 K8s 的 controller、kubelet、shim 那套模型搬到進程層級，再依 STS/MD/TD 各自的語意特化。

| 本次範圍 | 章節 | 批次 |
|---|---|---|
| 1. 封存 `docs/`，只保留架構設計 | §10 | B1 |
| 2. STS/MD/TD 進程化；進程管理層、shim、各平面 orchestrator、API 只管開始與結束 | §3、§4、§5、§7、§8 | B3、B4、B5、B6 |
| 3. MD 拆成 orchestrator 與 reconciler；原子行情；從宣告式推導訂閱 | §6 | B7、B8、B9 |
| 4. STS 不再 rebuild；重新設計續約與 `breathe` | §5.3、§5.4、§5.5、§8.2 | B4、B5 |
| 5. TD 每個進程一個私有 websocket，以 ledger 為主 | §7 | B6 |
| 6. 單元測試兩分鐘內跑完；先移除、再立標準 | §9 | B0、B2 |

---

## 1. 現況（以代碼為準）

### 1.1 各平面的執行單位

| 平面 | 執行單位 | 生命週期綁在 | 證據 |
|---|---|---|---|
| STS | **已經是一個 session 一個 OS 進程**（`SubprocessSpawner`） | STS 父進程。三重綁定：lifeline pipe EOF、`PR_SET_PDEATHSIG(SIGTERM)`、`getppid()` 比對 | `apps/sts/src/mftik_sts/spawn.py`、`worker.py`（`arm_parent_death`、`_watch_lifeline`） |
| STS | 父進程一停，所有 worker 跟著退出，再靠 rebuild 補回 | `STS_REBUILD_ON_BOOT`、重建時間窗 1800s、最多 3 次、`Strategy.rebuildable`、`on_rebuild`、`st_facts`、`rebuild_count`、`restart` | `session/manager.py`（`rebuild_interrupted`、`_spawn_rebuild`、`adopt_interrupted`） |
| MD | 單一進程。每個 venue 一個 `VenueSession` / connector，**一條公用 socket 承載所有 session 的所有 feed** | MD 進程 | `mftik_md/session/venue.py`、`manager.py`（1,533 行） |
| MD | 管理單位是 product key `(topic, UniversalTicker)`。`Dispatcher` 對每個 session 發 `md.{session_id}`，fan-out 成本 = session 數 × feed 數 | — | `session/dispatcher.py` |
| MD | wire ledger 藏在各 adapter 的 socket 內，MD 刻意不碰 venue 詞彙（MdVenueSubscriptions I6） | — | `exchange/wire.py` |
| MD | 到期處理是每個 ticker 一個 sleep task。訂閱只在 attach 時推導一次 | — | `_expiry_tasks`、`_subscribe_feed` |
| TD | 單一進程。每個 `api_id` 一個 `Session`（OMS 和 ledger 都在記憶體），由 STS link 做 refcount | TD 進程。TD 滾動會 reap 該 instance 名下所有 session | `mftik_td/session/manager.py`（1,711 行） |
| API | `deploy_strategy` 依序執行：STS create（`on_start`、`on_ready` 在這一步就跑完）→ MD attach → TD attach，失敗就以 `_detach_md`、`_fail_sts` 回滾。create 的 RPC timeout 寫死 10 秒 | — | `mftik_api/orchestrate.py`（538 行） |

### 1.2 長時間 hook（Deribit、ML）出問題的結構性原因

以下是我從代碼讀出的根因，請對照你的分析修正。

1. **活性和進度共用同一個訊號。** STS session 的 `_lease_heartbeat_loop` 和策略 hook 跑在同一個 event loop 上。ack 連續 1s × 3 次沒看到（`LEASE_HEARTBEAT_INTERVAL_S=1.0`、`LEASE_MISS_LIMIT=3`），session 就會自我 fail。MD 和 TD 那端的 lease 也用同一個時間窗。CPU-bound 的 hook（模型載入、推論、期權鏈全量重算）只要佔住 loop 約 3 秒，session 就會被判死。`breathe` / `slice_deadline` 是為了不讓策略餓死 heartbeat 而存在的補丁，問題本身並沒有解決。
2. **啟動沒有上限，但 API 只等 10 秒。** STS 的 create 要等 `on_start`、`on_ready` 跑完才回覆，STS 端沒有任何 deadline；API 的 create RPC 寫死 10 秒（`orchestrate.py`），CLI 的 `deploy_http_timeout` 也以 10 秒（`_STS_CREATE_S`）估算。`on_start` 超過 10 秒時 deploy 回 504，但 session 照樣起來（#132）。長 `on_start` 在每次 rebuild 時都要重新付一次這個成本。
3. **生命週期綁在平面進程上。** 任何一次部署都等於所有 session 中斷再 rebuild。rebuild 還要求策略自己支援 `rebuildable`。
4. **MD 單一 socket 承載所有訂閱。** 期權鏈的量級是一條鏈幾十到幾百個 channel。這個量級下，解碼的 CPU 負擔和重連風暴（V15）都集中在同一個 loop 上。

### 1.3 測試現況

- 共 3,377 個測試函數（靜態計數，參數化展開前）：`packages/common` 1,642、`apps/sts` 557、`apps/api` 438、`apps/td` 302、`apps/md` 211、`packages/db` 137、`apps/sym` 84、`apps/paper` 6。
- 直接依賴 session 機制（三個平面的 `SessionManager`，以及 `orchestrate`）的 566 個。策略實作測試（chase、oco、macd_dollar、cross_arb、twap、noop、tape_keeper）224 個，帶有 rebuild 語意。
- `pytest_sessionstart` 強制要求真的 NATS server（「the broker has no fake」）。DB 測試以 sqlite 參數化，CI 再加跑 Postgres。
- 測試裡有 424 處 `asyncio.sleep(>0)`、329 處 ≥1s 的 `timeout=`，`test_session_processes.py` 會 spawn 真的子進程。
- **各測試模組的實際耗時還沒量過**，這是 B0 的工作。

---

## 2. 目標、非目標、原則

### 2.1 目標

- **G1** 平面 controller 重啟或更新時，不中斷任何 worker：先 detach，新版再 reattach。
- **G2** worker 的生命週期不依賴 controller 是否存活，而且永遠不會變成 orphan：一定有 shim 持有它。
- **G3** 長 hook 不影響活性判定，也不影響平台在同一個 worker 裡的其他工作（收訊息、RPC timeout、控制）。進度慢是一個可觀測的狀態，除非策略自己宣告上限，否則不是 kill 的理由。
- **G4** MD 的管理單位是交易所的原子訂閱，平台 topic 是它的投影。宣告式設定可以推導出動態集合，例如 ATM 期權鏈和轉倉。
- **G5** API 只管開始與結束，中間的變化由平面內部的 orchestrator 收斂。
- **G6** `just test`（預設 tier）在本機兩分鐘內跑完。

### 2.2 非目標

- 多主機排程。placement 只在單一 instance 內進行。
- SYM 和 Paper 平面的結構調整。它們只配合新協定。
- venue adapter 的 wire 程式碼。可以重用，只在 B7 抽出 atom 相關的介面。
- Rust 化。Supervisor 和 shim 之間是語言無關的 NDJSON 協定，之後換語言不影響其他元件；本次 shim 用 Python（F29）。

### 2.3 原則

- **P1 控制面與資料面分離。** 資料不經過 controller。controller 掛掉代表暫時無法**改變**狀態，不代表系統無法**運作**。
- **P2 Spec / Status，level-triggered。** 宣告寫在 Spec，系統只寫 Status。用 `generation` / `observedGeneration` 判斷收斂到哪一版。所有動作都必須冪等。
- **P3 活性不等於進度。** 活性由 shim 證明：進程存在、能回應。進度（hook 延遲、feed 是否過期）是 Status 上的 condition。
- **P4 At-most-one。** 同一個 session、account 或原子訂閱的發佈者，任何時刻只能有一個 incarnation 產生副作用。fencing token 是 `(id, incarnation)`。STS 和 TD 的替換順序是 delete-before-create；MD 不做連線遷移（F22），連線 worker 原地重啟時同樣是 delete-before-create。
- **P5 Fail-static。** controller 消失時，worker 維持最後一份 desired，不在資訊不足時做破壞性動作。
- **P6 平面特化。** procman 只認識「進程」。STS/MD/TD 的語意全部留在各自的 orchestrator 裡。
- **P7 不以「訊號消失」推論狀態。** 能由權威來源直接觀測的（worker 是否存在、feed 最後一筆的時間），就不用週期訊號的缺席去推斷。唯一例外是 MD/TD 廣播靜默時發給策略的失聯通知（§5.6）：它只通知，不回收任何資源。資源回收一律依權威觀測（§8.2、F32）。

---

## 3. 分層與元件

```
                 ┌──────────────── API（只管 start / end）────────────────┐
                 │ 驗證 spec → 寫 SessionSpec → TD/MD intent → STS start  │
                 └──────┬──────────────────┬──────────────────┬───────────┘
                        │ 控制 subject（NATS request-reply）  │
               ┌────────▼───────┐ ┌────────▼───────┐ ┌────────▼───────┐
 plane         │ STS controller │ │ MD controller  │ │ TD controller  │  ← Strategon OCI assignment（oci_host_pid）
 controller    │ orchestrator + │ │ orchestrator + │ │ orchestrator + │    每個 tag 滾動
               │ Supervisor     │ │ Supervisor     │ │ Supervisor     │    開機時 reattach
               └────────┬───────┘ └────────┬───────┘ └────────┬───────┘
                        │ shim socket（NDJSON，${WORK_DIR}/run/*.sock）
 由 host init 收養 ─┬─ shim ─ worker  sts/session/a1b2c3
                    ├─ shim ─ worker  md/conn/Deribit/public/0
                    └─ shim ─ worker  td/account/42
 資料面：worker ⇄ NATS ⇄ worker（永不經過 controller）
```

### 3.1 Worker 種類

| 平面 | kind | 身分 | 擁有 | 直接服務／發佈的 subject |
|---|---|---|---|---|
| STS | `session` | `session_id` | 策略實例、event log、timer、artifacts handle。ingress thread 持有接收連線，strategy thread 持有送出連線（§5.3） | 服務 `sts.ctl.{session_id}`（stop、fail、status）；訂閱 `md.a.*` 和 `td.*` |
| MD | `conn` | `(venue, endpoint, n)` | **一條**公用 websocket、該連線的 reconciler、decoder、tape append | 發佈 `md.a.{venue}.{atom}` |
| MD | `fetch` | instance | REST readers | 服務 `md.fetch` |
| TD | `account` | `api_id` | 常駐：HTTP 連線池、backfill。有 intent 時加上交易層：私有 websocket、OMS、ledger、recon、槓桿快取（F35） | 服務 `td.order.{api_id}`、`td.oms.*`、`td.ledger.*`；發佈 `td.{api_id}.global` |

### 3.2 和 K8s 的對照

| K8s | 這裡 |
|---|---|
| etcd + API server | Postgres 上的 Spec/Status 表，加上各平面 controller 的控制 subject |
| controller-manager | 各平面的 orchestrator |
| kubelet | Supervisor（嵌在 controller 裡） |
| containerd-shim | mftik-shim |
| Pod | worker |
| ownerReferences GC | intent 的 `owner=session_id`，owner 進入 terminal 時由 MD/TD 回收 |
| readiness probe | session conditions（`MdReady`、`TdReady`） |
| Deployment rolling update | TD 帳號 drain-replace；MD 連線不遷移（F22），換代碼見 §4.6 |
| StatefulSet at-most-one | STS/TD 的 incarnation fencing |

### 3.3 狀態的權威

每一種狀態只有一個權威：只有它能寫，其他人一律向它讀或聽它廣播。重啟或失聯後，也只由它收斂。B0 會另外整理一份現況版（as-is），標出和這張表不一樣的地方。

**控制面（宣告）**

| 狀態 | 權威（唯一寫入者） | 存放 | 讀取者 | 重啟或失聯後怎麼收斂 |
|---|---|---|---|---|
| session spec：策略、參數、`restart`、timeout | API | Postgres `sts_sessions`（Spec 欄位） | STS controller | DB 本身就是權威 |
| session status：phase、conditions、incarnation、`restart_count`、失敗原因 | STS controller 的 Supervisor | Postgres `sts_sessions`（Status 欄位）；即時版發佈在 `sts.status.{session_id}` | API、UI、CLI | controller 重啟後由 reattach 對帳重算（§4.4） |
| MD intent：session 要哪些 feed 和 selector | API（start）、STS controller（自癒時重新 put）、session worker（執行期間的 subscribe，經 `md.intent.patch`） | Postgres `md_intents` | MD controller | level-triggered；owner 依 §8.2 規則 3 回收 |
| TD intent：session 用哪些帳號 | API、STS controller | Postgres `td_intents` | TD controller | 同上 |
| 常駐訂閱 | 設定檔 | Postgres `md_standing_subscriptions` | MD controller | — |
| `api_id` → instance 綁定、帳號設定（例如 cancel-on-disconnect） | 使用者經 API | Postgres `apis` | TD controller | — |
| listing：合約、到期、strike | SYM | Postgres `symbol_*` | MD controller（selector、到期）、TD | 每小時刷新 |

**進程層**

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 |
|---|---|---|---|---|
| worker 是否存在、exit code、signal | shim（親眼看到） | `${WORK_DIR}/run/<id>.sock`、`<id>.exit.json` | Supervisor | controller 重啟時 reattach 讀回 |
| 每個 instance 存活中的 worker 集合 | Supervisor | `procman.report.{plane}.{instance}`（不落地） | MD/TD orchestrator（intent 回收） | 報告停止時不回收任何東西（F32） |
| worker 的代碼版本 | Supervisor（`WorkerSpec.code_ref`） | `supervisor.json` | CLI（列出舊版 worker） | — |

**MD**

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 |
|---|---|---|---|---|
| 每條連線的 desired atom 與 generation | MD controller | 記憶體，可由 intent、常駐訂閱和 selector 狀態重算 | 連線 worker | controller 重啟後重算；新 generation 推出前，worker 維持舊的（P5） |
| selector 的 universe、epoch、置中狀態 | MD controller | Postgres（selector 狀態表） | MD controller；session 經 `md.universe.{session_id}` | 從 DB 接續，不重新置中 |
| 連線上實際訂閱成功的 atom（observed） | 連線 worker 的 reconciler，以交易所 ack 為準 | 記憶體 | 連線 worker、狀態廣播 | 重連後歸零，下一輪 diff 補齊 |
| 行情內容，包括 fold 後的 book（F21） | 連線 worker | 記憶體 → `md.a.*` | STS session | 重連後由交易所的 snapshot 重建 |
| feed 狀態 live / down | 連線 worker | `md.w.*` 廣播 | session ingress | 廣播靜默 10 秒視為 down，只通知（§5.6） |
| per-atom `seq` | 連線 worker | envelope | 策略自行偵測跳號（F25） | 換 incarnation 後重新起算 |
| tape 與 coverage | 持有該 atom 的連線 worker | Redis（每個 region 一台） | MD 的讀取 RPC → 策略 | 空洞記在 coverage |

**TD**

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 |
|---|---|---|---|---|
| 交易所上的掛單、部位、餘額（最終真相） | 交易所 | — | TD 的 recon | — |
| OMS、ledger（預扣、available） | TD 帳號 worker 的交易層（F13） | 記憶體 | session（`oms.view`、帳號事件） | 重啟後以 `reconcile()` 從交易所重建，發出 `td.account.reset` → 策略收到 `on_resync` |
| 交易層開或關 | desired：TD controller（依 intent）；observed：帳號 worker | 記憶體 | — | controller 不在時，worker 維持最後一份（P5） |
| 帳號狀態 ready / degraded / unavailable | TD 帳號 worker | `td.account.state.{api_id}` 廣播 | session ingress | 靜默 10 秒視為 unavailable，只通知 |
| 訂單歷史、成交、資金流水 | TD 帳號 worker（live 寫入加 backfill） | Postgres `orders`、`fills`、`cash_flows`、`backfill_cursors` | API、UI | backfill 依交易所補正 |

**STS session**

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 |
|---|---|---|---|---|
| 策略內部狀態 | session worker | 記憶體，不落地（F10） | 策略 | 重新掛起時從 `on_start` 全新開始 |
| `client_order_id` 序號 | session worker | 記憶體（`session24 \| ts_sec28 \| seq8`） | TD | R2 保證不撞號 |
| event log | session worker 的 ingress | 檔案（`STS_EVENTLOG_DIR`） | 事後分析 | — |
| artifacts | session worker | 檔案（`STS_ARTIFACT_DIR`） | 策略、API | — |
| hook 進度、offload 進度、交付的丟棄計數 | session worker 的 ingress | status progress（`sts.status.{session_id}`） | UI | — |

**版本**

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 |
|---|---|---|---|---|
| 協定版本 `pv` | 代碼常數 | envelope | 每個收件者 | 不符就以 `protocol_mismatch` 拒絕（F26） |

### 3.4 新增的抽象層

模組路徑暫定，IF 批次（§11）會先把每一層的介面定下來，實作留給後面的批次。

| 層 | 模組（暫定） | 內容 | 取代 |
|---|---|---|---|
| 時間 | `mftik.clock` | `Clock`（`now`、`monotonic`、`sleep`）、`FakeClock` | 散落的 `time.time()`、`asyncio.sleep` |
| 進程管理 | `mftik.procman` | `WorkerSpec`、`Supervisor`、shim 與它的 NDJSON 協定、`procman.report` | `spawn.py` 與 `worker.py` 的 lifeline、各平面的 reaper |
| 訊息處理 | `mftik.broker.handler` | `Handler`：收到解碼後的訊息 → 回覆與副作用；`serve(broker, subject, handler)` | rpc 模組裡傳輸和邏輯混寫的做法（F31） |
| 協定 v2 | `mftik.protocol` | envelope 的 `pv`、intent、`md.a.*`、`md.w.*`、`md.universe.*`、`td.account.state.*`、`td.account.reset`、`td.order.cancel_session`、`procman.report.*` | lease、attach、per-session fan-out |
| STS controller | `mftik_sts.controller` | `StsOrchestrator`：SessionSpec 與 worker status 的 reconcile；crash 分類與重啟策略 | `session/manager.py` |
| STS session worker | `mftik_sts.session_worker` | `Ingress`、`StrategyRunner`、交付策略、event log、offload pool | `session/session.py`、`worker.py` |
| SDK | `mftik.strategy` | 新 hook（`on_ready(ready)`、`on_md_update`、`on_td_update`、`on_resync`、`on_universe_change`）、`offload`、`offload_pool`、`oms.view(settled)`、`md.state / universe / current / subscribe`、`td.state`、`StrategyHarness` | rebuild、recon、`breathe` 相關 API |
| MD adapter | `mftik.exchange.<venue>.atoms` | `Atom`、`AtomPlan`、`atoms_for`、`decode`、`capacity`、`join_policy`；`TickerStats` | `VenueSession._open` 解析成 `stream_*` 的做法 |
| MD controller | `mftik_md.controller` | `MdOrchestrator`、placement、selector 的 `evaluate`、到期 | `session/manager.py`、`dispatcher.py`、`_expiry_tasks` |
| MD 連線 worker | `mftik_md.conn` | `ConnWorker`、`Reconciler`（`reconcile(desired, observed) -> actions`）、tape append、狀態廣播 | `session/venue.py`，以及 adapter 內的 `WireLedger` 用法 |
| MD fetch worker | `mftik_md.fetch` | 既有的 readers 搬進獨立 worker | MD 進程內的 fetch |
| TD 帳號 worker | `mftik_td.account` | 常駐層（HTTP 連線池、backfill）、交易層（私有連線、OMS、ledger、recon）、`cancel_session`、死人開關 | `session/manager.py` 的 lease 與 refcount、`session/session.py` 的生命週期部分 |
| TD controller | `mftik_td.controller` | `desired_accounts`、intent → 交易層開關、drain-replace | `session/manager.py` |
| API | `mftik_api.orchestrate` | `start` / `end`、intent repository | `deploy_strategy` 與補償邏輯 |
| DB | `mftik_db` | SessionSpec / Status 欄位、`md_intents`、`td_intents`、`md_standing_subscriptions`、selector 狀態 | `st_facts`；`md_sessions` / `td_sessions` 的寫入 |

---

## 4. procman（進程管理層）

### 4.1 元件

這一層沿用你 prototype（Async Supervisor + Shim）的分工，再依 STS/MD/TD 各自特化：

- **Supervisor**（`mftik.procman`，Python 函式庫）：嵌在每個平面的 controller 裡，負責所有決策：要不要啟動、何時重啟、如何判定失敗、reattach。三個平面共用同一套函式庫，差別只在 WorkerSpec 和重啟策略（§4.3）。
- **`mftik-shim`**：每個 worker 一個常駐的小進程，是 worker 真正的父進程。
  - Supervisor 先用 `subprocess.Popen` 啟動一個只活一瞬間的中間進程，由它 fork 出 shim 並 `setsid`。所以 shim 從一開始就不是 controller 的子進程。
  - 不能用 `asyncio.create_subprocess_exec` 啟動 shim：它的 transport 在 close 或被 GC 時會殺掉 child（prototype §7.1）。
- **runner**：worker 的入口，`python -m mftik_<plane>.worker`。

v0.1 規劃的常駐 daemon（procd）取消。前提是 Strategon 的 OCI driver 支援 `oci_host_pid`（F6，strategon#60）。

### 4.2 shim 的不變式

- **S1** shim 是 worker 唯一的父進程。Linux 上以 `PR_SET_CHILD_SUBREAPER` 讓 worker 的子孫也由 shim 收屍。shim 自己被 host init 收養、回收。
- **S2** shim 消失時，worker 自行 graceful stop。觸發條件是 status pipe 寫入得到 EPIPE，或 `PDEATHSIG=SIGTERM`（指向 shim），哪個先到都算。
  - graceful 讓 `on_stop` 有機會撤單，比 SIGKILL 好。prototype 實測約 0.85 秒內停止。
  - 現行綁在 controller 上的 lifeline pipe 和 `MFTIK_STS_PARENT_PID` 移除。
- **S3** shim reap worker 之後寫 `<id>.exit.json`（tmp 檔再 rename），要等 Supervisor 送出 `release` 才退出。controller 不在線時，exit code 和 signal 也不會遺失。
- **S4** shim 持有 worker 的 stdio 和 status pipe，寫成 log 並輪替。Supervisor 不在時，worker 不會因為 SIGPIPE 或 buffer 寫滿而卡住。
- **S5** shim 開一個 unix socket，協定是 NDJSON：`status`、`signal`（對 worker 的 process group `killpg`）、`watch`、`release`。
  - socket 放在 `${WORK_DIR}/run/<worker_id>.sock`。
  - Supervisor 靠 socket 路徑找回 worker，不記 pid，所以也不用擔心 pid 被重用。
- **S6** status pipe 的單筆訊息不超過 `PIPE_BUF`（4096 bytes），保證寫入是原子的；pipe 滿了就丟掉這一筆，下一次 heartbeat 會帶完整狀態補上。
  - STS worker 的 heartbeat 由 ingress thread 寫入（§5.3），不經過策略的 loop。
  - MD/TD worker 不跑使用者代碼，heartbeat 就跟主 loop 綁在一起：loop 卡住就代表 worker 壞了。
- **S7** shim 收到 SIGTERM 時轉送給 worker，自己不先退出。shim 不認識 STS/MD/TD，也不做重啟決策。

### 4.3 Spec、狀態機、重啟策略

```python
@dataclass(frozen=True)
class WorkerSpec:
    id: str                 # "sts/session/a1b2c3"、"md/conn/Deribit/public/0"、"td/account/42"
    plane: Literal["sts", "md", "td"]
    kind: str
    incarnation: int        # 由 controller 分配
    argv: list[str]
    env: dict[str, str]
    code_ref: str           # worker 所屬的 release 版本，等於 spawn 它的 controller 版本（§4.5）
    restart: Literal["never", "on_failure"]
    start_timeout_s: float
    hb_timeout_s: float | None   # None：不以 heartbeat 判死
    oom_score_adj: int           # 依 kind 分級（§4.7）
    rlimit_data_bytes: int | None  # 可選，由 shim 在 exec 前套用（§4.7）
    stop_grace_s: float
    labels: dict[str, str]
```

狀態機沿用 prototype：

```
STOPPED ─▶ STARTING ─ready─▶ RUNNING ─SIGTERM─▶ STOPPING ─▶ STOPPED
              │ 死亡／逾時        │ 死亡／heartbeat 逾時
              ▼                  ▼
            FAILED            CRASHED ─▶ BACKOFF ─▶ STARTING
                                 └─（window 內重啟超標）─▶ FATAL
存活中 ─shim 消失─▶ LOST
```

| kind | restart | heartbeat 判死 | 說明 |
|---|---|---|---|
| STS `session` | 依 deploy 的 `restart`：`never` → failed；`on_failure` → 平台清場後從 `on_start` 重新掛起（§5.2，F10） | 只看 ingress thread 的 beat（`hb_timeout_s` 只用來抓整個進程卡死）；hook 時間預算依 F15；ingress 與 session 同生共死（§5.3 I1 到 I4） | 不 rebuild：重新掛起不帶任何舊狀態（範圍 4、F3、F10） |
| MD `conn` | `on_failure`：指數 backoff，加上 restart intensity | loop heartbeat 逾時 → kill → 重啟 | 重啟後由 reconciler 自動補回訂閱 |
| TD `account` | `on_failure`：同上 | 同上 | Supervisor 確認舊 PID 消失後才啟動新 incarnation（§7.1，F36） |

MD/TD 用 readiness 區分初始化失敗和運行中崩潰（prototype §4）：ready 之前死掉記為 FAILED，不重啟（通常是設定錯或帳號被拒）；ready 之後死掉記為 CRASHED，依策略重啟。

### 4.4 Detach / Reattach 協定

**controller 停止**（Strategon 送 SIGTERM）時呼叫 `Supervisor.close("detach")`：

1. 停止接受新的控制 RPC。
2. **不對任何 worker 送信號。**
3. flush 尚未寫出的 Status，以 exit 0 結束。

**controller 啟動**時呼叫 `Supervisor.start()`：

1. 先同步載入本機狀態（`${WORK_DIR}/run/supervisor.json`），再逐一做 async reattach（prototype §7.4）。
2. 對 `run/` 下每個 socket 呼叫 `status`，比對 `id` 和 `incarnation`。
3. 和 DB 裡的 desired 對帳：

| desired | worker | 動作 |
|---|---|---|
| 有 | running | adopt |
| 有 | 不在、已 exited 或 LOST | STS：以 exit 資訊標成 failed，不重建。MD/TD：依 restart 策略處理 |
| 沒有，或已 terminal | running | stop，接著 release |

4. 對帳完成才開始服務控制 subject。整個過程中 worker 照常運作（P1）。

只有明確呼叫 `close("stop")`（整台主機下線）才會停掉 worker。

### 4.5 部署拓撲：Strategon 實際上怎麼跑進程（F6）

以下依 `BullionBear/strategon` @ `8d18d95` 的代碼。

| 事實 | 證據 |
|---|---|
| strategy 啟動時會 `setsid`；agent 重啟後以 `(pid, starttime)` adopt 回來 | `internal/agent/driver/driver.go`、`exec_linux.go` |
| OCI driver 不是 runc 或 containerd。agent 把自己 re-exec 成 `--oci-init`，以 `CLONE_NEWUSER\|CLONE_NEWNS\|CLONE_NEWPID\|CLONE_NEWUTS` 加 `setsid` 起進程，`pivot_root` 進 rootfs，使用 host network。監督走 host PID 和 pidfd | `userns_linux.go`、`oci_linux.go`、`oci_init_linux.go` |
| `captureStdio: true`（mftik 的 plane sets 全都開著）時，Strategon 的 tee 是新 PID namespace 的 PID 1，payload 是它的 child | `stdio_tee_linux.go`（`waitPayloadAndReap(pid1)`）、`docs/ARCHITECTURE.md`「OCI runtime」 |
| 部署時先進入 DRAINING：對 tee 的 process group 送 SIGTERM → 等 `stopGraceSeconds` → SIGKILL，再 SWITCHING 到新的 rootfs | `reconciler/deploy.go`（`gracefulStop`）、`supervisor/stop.go` |
| release GC 只保留新版、前一版和 current（`--release-retention` 預設 3） | `deploy.go`、`cmd/agent/main.go` |
| 同一個 volume 同時只能被一個 running assignment 掛載 | `reconciler/volumes.go`（`volumeWriterConflictErr`） |
| Strategon 停止時只對 process group 送信號，不用 `cgroup.kill` | `exec_linux.go`、`supervisor/stop.go` |
| agent 的 systemd unit 沒有設 `KillMode`（預設 `control-group`），也沒有傳 `--cgroup-root`。所以 strategy 都在 agent 的 unit cgroup 裡，而 `install-agent.sh` 升級時會執行 `systemctl restart` | `deploy/install-agent.sh` |
| 沒有 `--cgroup-root` 時，`setupCgroup` 直接回傳 -1。plane sets 裡的 `limits.memoryBytes`（768 MiB）**目前沒有生效**。就算設了 `--cgroup-root`，因為沒有啟用 `subtree_control`，上限很可能仍然寫不進去；`max_open_files` 也從未套用（追蹤於 strategon#61） | `exec_linux.go`、`deployment/sets/planes.json` |

**推論：**

- **OCI driver 底下由平面 spawn 出來的 shim / worker，在平面滾動時一定會被殺掉。** `setsid` 只能逃出 process group，逃不出 PID namespace。controller 退出後 tee 跟著退出，tee 是這個 namespace 的 init，它一死，kernel 就會 SIGKILL namespace 裡所有進程。v0.1 寫的「容器被換掉」不精確，真正的機制是 PID namespace。
- **agent 升級本身就會殺掉所有 strategy。** 這和本重構無關，現在就存在：依 `install-agent.sh` 安裝的主機上，agent 用 `systemctl restart` 升級，而 `KillMode=control-group` 會殺掉 unit cgroup 裡的所有進程，包括 mftik 各平面、NATS、Redis。代碼註解說 strategy 能撐過 agent 的 self-update，但部署方式讓這個保證不成立。請在主機上用 `systemctl show strategon-agent -p KillMode` 和 `cat /proc/<plane pid>/cgroup` 確認。
- **就算拿掉 PID namespace，OCI 還有第二個問題。** worker 會留在舊版的 mount namespace 裡，而舊 rootfs 在兩次滾動後就被 GC 刪掉。worker 之後才需要的檔案（lazy import、`/etc/ssl/certs`、glibc 延遲 dlopen 的 `libgcc_s`）會失敗。這是潛伏錯誤，不會在部署當下出現。
- **v0.1 建議的「runtime 和 controller 兩個 assignment 共用一個 volume」會被單一 writer 規則拒絕。**

**選項：**

| | 做法 | Strategon 改動 | worker 代碼版本 | 主要代價 |
|---|---|---|---|---|
| **(A)** | **OCI 新增 `oci_host_pid` 選項，controller 直接 spawn shim** | 小，三項，見下方 S-1 到 S-3 | 等於 spawn 它的 controller 版本：新 session 用新版，舊 session 繼續用舊版 | 失去 PID namespace 隔離 |
| (B) | runtime 和 controller 拆成兩個 assignment，彼此以 NATS 溝通。**worker 不是 Strategon assignment**：Strategon 只看得到 runtime 那一個進程，worker 經由 shim 掛在 runtime 底下 | 只需要 S-3 | 等於 runtime image 的版本。**任何 worker 代碼變更都要滾動 runtime，所有 worker 跟著停止** | G1 縮水成「只改 orchestration 時不中斷」；runtime 必須是永不退出的極簡程式 |
| (C) | 平面改用 EXEC driver，mftik 打包成單一執行檔 | 無 | 每個版本各自解壓的目錄 | **依 F5 排除**：單一執行檔打包不夠成熟 |
| (D) | 每個 worker 都是一個 Strategon assignment | 大，見下方 | — | 粒度不合 |

**(D) 為什麼不適合：** Strategon 的 assignment 是「某個版本在某台機器上的長期部署」，worker 則是短命、帶 session 參數的執行。

- **每個 assignment 各自解壓一份 image** 到 `<base>/<strategy>/releases/<v>/rootfs`（`artifact.go`）。開一個 session 就要解壓一次，耗時又佔磁碟。
- **assignment 只要還是 desired，退出就一定會被重啟**：crash 走 backoff，正常退出也直接重啟（`reconciler.go` 的 `handleExit`）。沒有 `never` 這種策略。STS session 是一次性的，controller 得在它退出時搶在重啟之前刪掉 assignment。
- **volume 單一 writer。** 每個 STS session 都需要 `mftik-data`（registry、artifacts、event log），但同一時間只能有一個 session 掛載它。
- **啟動路徑經過中央 control plane。** 流程是 cp（JP，4 GB）→ agent stream → DOWNLOADING → VERIFYING → STARTING → HEALTH_CHECKING，延遲以秒計，而且 cp 成為開 session 的單點依賴。

要讓 (D) 成立，Strategon 需要一種新的 workload：共用 rootfs、可設定不重啟、允許多個 writer 共用 volume、由 agent 本地 API 建立。這等於把 procman 做進 Strategon，可以當作長期方向，但不在本次範圍。

**採用 (A)（F6）。** 理由：

- 只用成熟的 Linux 機制，Strategon 的改動小，而且 repo 在你手上。
- 語意和你的 prototype、和 K8s 都一致：管理器更新時，舊 worker 跑舊代碼，新 worker 跑新代碼，不需要任何額外的打包。
- STS session 與 MD 連線沿用舊版代碼跑到結束、TD 帳號的 drain-replace（§4.6），都靠「新舊版本的 worker 可以並存」，只有 (A) 給得出來。

**(A) 需要的 Strategon 改動：**

以下三項都追蹤於 [strategon#60](https://github.com/BullionBear/strategon/issues/60)：

- **S-1 `oci_host_pid` 選項。**
  - 新增 per-assignment 的 `oci_host_pid`，作法比照 `capture_stdio`，以 `agent_version >= 6` 為門檻。
  - OCI 的 cloneflags 去掉 `CLONE_NEWPID`。
  - oci-init 的 `/proc` 改成 recursive bind host 的 `/proc`。unprivileged user namespace 不能替 host 的 PID namespace 掛新的 procfs。
  - probe 一併更新。
  - 需要在 cp 和 yite 的 kernel 上驗證。
- **S-2 release GC 不刪仍在使用中的 rootfs。** GC 之前掃一次 `/proc/*/root`，以 `(dev, inode)` 比對各 release 的 rootfs 目錄，仍被任何進程當成 root 的 release 就保留。這解決上面「舊 rootfs 被刪」的潛伏錯誤。agent 能不能讀其他 user namespace 裡進程的 `/proc/<pid>/root`，需要驗證。讀不到的話改用 pin 檔：payload 在 work 目錄寫下仍需要的版本，GC 會尊重它；agent 另外以 `STRATEGON_RELEASE_VERSION` 告訴 payload 自己是哪一版。mftik 這端兩種情況都要能配合。
- **S-3 agent 的 unit 加 `KillMode=process`。** 另一種做法是啟用 `--cgroup-root`，並把 strategy 的 cgroup 放在 unit 之外。不論選 A 還是 B 都需要這一項。

**(A) 的 mftik 端：**

- controller 直接 spawn shim，和 prototype 一樣。shim 和 worker 留在 spawn 它們的那一版 controller 的 mount namespace 裡，所以 `WorkerSpec.code_ref` 就是該 release 的版本號。
- shim 由 host init（或最近的 subreaper）收養。Strategon 的 SIGTERM 和 SIGKILL 只送到 controller 的 process group，碰不到 shim。
- **記憶體：** 只要 `--cgroup-root` 沒開，就沒有任何記憶體上限；開了之後，worker 和 controller 共用該 assignment 的 `memory.max`。在 strategon#61 讓上限真正生效之前，由 §4.7 的機制防護（F7）。
- **本機開發：** compose 的容器同樣有 PID namespace，重建容器一樣會殺掉 worker。所以開發和 integration 測試用純進程跑平面（`just planes`）。compose 只用在不需要驗證 reattach 的場景。

**變體 (A')：** 如果要保留 PID 隔離，可以讓每個 slot 有一個常駐的 pause 進程持有 PID namespace，新版本以 `setns` 加入，也就是 K8s pod sandbox 的做法。代價是 Strategon 的改動明顯變大。

### 4.6 各平面的升級語意

下表是**切換到本版之後**的日常升級語意。切換本身依 F2 處理：先停掉所有策略，平面清空後再整批換版。

| 平面 | controller 滾動 | worker 代碼升級 |
|---|---|---|
| STS | 不影響（reattach） | 已在跑的 session 繼續用舊版，直到它結束；新 session 用新版 |
| MD | 不影響 | 已在跑的連線繼續用舊版，不遷移（F22）；新開的連線用新版。既有連線要換上新版時（例如 decode 的 bug fix），由人工對單一連線下 `restart`（F24），平台不會自動重啟舊版連線：斷線數秒，策略收到 `on_md_update` 的 down → live，tape 記錄空洞。**這取代了 `MdHandover.md` 的設計** |
| TD | 不影響 | 換版後由人工逐帳號觸發 drain-replace（F27），平台不自動換版：新單一律以可重試的 `td_draining` 拒絕，等 in-flight ack 收齊後停止，以新 incarnation 啟動並 recon，再恢復收單。期間 session 的 `TdReady` 會短暫變成 false |

**版本收斂（F24）：** 只要還有某個 release 的 worker 在跑，S-2 就會保留該 release 的 rootfs。CLI 提供一個指令（暫定 `mftik workers --stale`）列出跑在非最新 release 上的 worker，由人決定何時重啟，或等它自然結束。

**跨版本（F26）：** 切換之後新舊版本的 worker 會並存，但不做版內相容，規則只有三條：

1. NATS 上的每則訊息都帶 `pv`（協定版號，整數）。線上格式一有變動就升版。
2. 收到不同 `pv` 的訊息一律以 `protocol_mismatch` 明確拒絕，不嘗試解析。session 啟動時，若它的 `pv` 和要用到的 MD/TD worker 不同，同樣拒絕。
3. 停止不依賴協定：Supervisor 以 SIGTERM 停 worker，走 shim 或直接對 host PID 送訊號（`oci_host_pid`），任何版本組合都停得掉。

升 `pv` 的順序：先停掉舊 `pv` 的 STS session，讓 `on_stop` 的撤單還送得到同版的 TD；再人工重啟 TD 帳號（F27）和 MD 連線（F24）；最後才開新 session。`on_stop` 沒撤乾淨的單，由 controller 發出的 `cancel_session`（F10）補上。

### 4.7 記憶體防護（F7）

在 Strategon 的 cgroup 上限（strategon#61）可用之前，mftik 先用三個不需要特權的機制防護。#61 上線後這三項仍然保留：cgroup 是整個平面的總量，這三項負責平面內部誰先犧牲、誰不准進來。

**1. `oom_score_adj` 分級**

shim fork 出 worker 之後、`exec` 之前，由子進程寫自己的 `/proc/self/oom_score_adj`。往上調不需要特權。不論是整台主機的 global OOM，還是之後 cgroup 內的 OOM，kernel 都依 RSS 加上這個值挑人，所以先被殺的會是策略，而不是行情或下單。

| 進程 | 初始值 | 理由 |
|---|---|---|
| STS `offload` 子進程（§5.5） | +900 | 比 session 本身先被犧牲；session 只會收到 `OffloadWorkerLost`，不會跟著死 |
| STS `session` | +800 | ML 和使用者代碼最可能失控，也最該先被犧牲 |
| MD `conn`、`fetch` | +300 | 重啟後 reconciler 會補回訂閱 |
| TD `account` | +100 | 持有 ledger 和在途的單，最不該被殺 |
| controller、shim | 0（不調） | 被殺會失去管理能力；而且調低需要 `CAP_SYS_RESOURCE` |

初始值在 B4 依實測的 RSS 調整。

MD 連線 worker 是一條 websocket 一個進程（F17），worker 數等於使用中的連線數。以每個 60–90 MB 估計（B4 實測），10 條連線就是 0.6–0.9 GB，已經超過現行整個 MD 平面的 768 MB，MD 的 `memory_budget_mb` 要依此設定。

每個 worker 另外有一個 Python shim（F29），約 10–15 MB，也要算進各平面的預算。

TD 帳號 worker 對每個啟用帳號常駐（F35），所以 TD 平面固定佔用「啟用帳號數 ×（worker 加 shim）」，和有沒有 session 無關。

**2. 可選的 `RLIMIT_DATA`**

`WorkerSpec.rlimit_data_bytes` 有設定時，由 shim 在 `exec` 之前套用。超過上限時 Python 拋出 `MemoryError`，不是被 SIGKILL，策略還有機會留下 log 再 fail，session 的 reason 也能寫清楚。

- STS 的值來自 strategy.yml 的 `limits.memory_mb`。預設不設。
- 不用 `RLIMIT_AS`：numpy、torch 會預留大量虛擬位址，容易誤判。

**3. 准入控制**

每個平面的 orchestrator 持有一份預算：`max_workers`，以及依 kind 估算的 `memory_budget_mb`，以 instance 的環境變數設定。

- 預估值來自 Supervisor 回報的實際 RSS：開了 `oci_host_pid` 之後，controller 可以直接讀 worker 的 `/proc/<pid>/status`。
- 超過預算時，start 直接以 `capacity_exceeded` 拒絕，不會先把 worker 開起來、再讓 OOM 收拾。

**觀測：** `procman.report.*` 帶上每個 worker 的 RSS。#61 上線後，Strategon 另外回報每個 slot 的 `memory.current` 和 `oom_kill`。shim 看到 worker 被 SIGKILL 時，Supervisor 會比對這個計數，判斷是不是 OOM。

**#61 上線之後：**

- 每個平面的 `memoryBytes` 重新估算為 controller 加上 worker 預算總和，再加一段餘量。
- MD/TD 不設 `cpuMillicores`，因為 CFS throttling 會在整個 period 內卡住整個 slot。

---

## 5. STS

### 5.1 Controller：session manager 加 orchestrator

- desired 來源是 `SessionSpec`（DB 列）。placement 很單純，就是本 instance。
- 每個 session 有一個 reconcile：比較 desired phase 和 worker status，決定 create、stop、標記 terminal。
- 服務 `sts.{instance}`：start、end、list、artifacts、env。session 層級的控制（stop、fail、status）由 worker 自己在 `sts.ctl.{session_id}` 服務，現在的 `Topics.sts_control` 已經是這個方向。
- 執行期間的訂閱變更（策略呼叫 `self.md.subscribe`，以及 B9 的 selector 事件），由 worker 直接找 MD orchestrator，不經過 API。

### 5.2 Session 生命週期

`pending → starting → running → stopping → done | failed`，另外有 `restarting`（F10）

- 刪除 `interrupted`（等待 rebuild 的狀態）。策略不再碰 Postgres，session 的狀態一律由 controller 的 Supervisor 寫入，依據是 worker 回報的狀態和 shim 的 exit 紀錄（F10）。
- **`on_start` 與 `on_ready`（F12）：**

  | 階段 | 平台 | 策略 |
  |---|---|---|
  | `on_start` | MD 的 feed 已經訂閱，ingress 在收資料但不交付；TD 還沒訂閱 | 載入模型、讀 artifacts、`tape.read`。可以很長，也可以同步。**不能下單**，SDK 會拋出 `NotReady` |
  | 等待就緒 | `on_start` 結束後才訂閱 TD 並送出 recon；接著等就緒條件成立 | — |
  | `on_ready(ready)` | 只呼叫一次。之後才開始交付事件：`latest` 類只給最新一筆，`all` 類依序交付 | `self.oms`、`self.ledger` 已經是 recon 之後的狀態，可以直接開始下單 |

  - **帳本同步（F13）：**
    - OMS / ledger 的權威是 TD 帳號 worker 的記憶體。策略隨時可以用 `await self.oms.view()` / `self.ledger.view()` 取得最新狀態；需要等狀態為 UNKNOWN 的單收斂時，改用 `view(settled=True)`。
    - 策略不再主動 recon：`send_recon`、`STS_RECON`、`on_recon_done` 全部刪除。
    - 平台內部的 recon 只用在兩個地方：就緒條件（TdReady），以及下面的 `on_resync`。
  - **`on_resync(api_id, cause, view)`（F13）：**
    - 只在事件流可能有缺口時由平台觸發，而且只會發生在 `on_ready` 之後。
    - 觸發點只有兩個：
      - `cause="reconnect"`：ingress 的 NATS 斷線後重連，斷線期間的 fill 或 order update 可能遺失。
      - `cause="account_reset"`：TD 帳號 worker 換了 incarnation（§7.1），帳本是從交易所重建的。
    - `view` 是收斂後的帳本，策略應該拿它校正自己由事件累積出來的狀態。例如 chase 必須確認自己追的那張單是否還掛著，否則漏收一筆 fill 就可能重複下單。
    - TD 自己因交易所重連而跑的 `reconcile()` 不觸發 `on_resync`，因為它的變化已經透過 order update 和 OMS view 推送給策略。
  - 內建策略在 `on_start` 裡自行等 recon 的 timer、以及只在 `on_recon_done` 才開始交易的寫法，B5 一律改成在 `on_ready` 開始交易。
- **就緒條件只針對有宣告的部分。** strategy.yml 的 `md:`、`td:` 都是選填，沒宣告的那一項沒有東西要等，條件直接視為成立。

  | strategy.yml 宣告 | 要等什麼 | `on_ready` 的時機 |
  |---|---|---|
  | 都沒有 | 不等 | `on_start` 結束後立刻觸發 |
  | 只有 md（例如 tape_keeper） | 每個 feed 就緒 | 全部就緒，或 `ready_timeout_s` 到期（附缺少的 feed 清單） |
  | 只有 td | 每個帳號 recon 完成 | 全部完成；逾時則 failed |
  | 兩者都有 | 兩邊都等 | 帳號必須全部完成；feed 全部就緒或逾時都可以 |

  - **TdReady 是硬條件：** `ready_timeout_s` 內有帳號沒完成 recon，session 就記為 failed。這是初始化失敗，依 F11 不重啟。帳號狀態不明時不能交易。
  - **MdReady 是軟條件：** 逾時仍然呼叫 `on_ready`，在 `ready.missing_feeds` 列出缺少的 feed 並寫一條 warning，由策略決定是等、降級還是 `fail`。
  - **MdReady 的判斷依 §6.1 的 `join_policy`：** 交易所在訂閱時會推 snapshot 的（quote、ticker、book、greeks 等），收到第一筆才算就緒；不推 snapshot 的（trade、aggtrade、liquidation），MD 確認訂閱成功就算就緒，不等第一筆成交。期權鏈等 selector 回報的是覆蓋率，不是單一布林值。
  - **feed 訂閱不到不是就緒問題。** symbol 不存在、交易所不支援該 topic 等情況，在登記 MD intent 時就被拒絕，deploy 當下就失敗。
  - **執行期間動態加入的 feed 不影響 `on_ready`**，個別 feed 的就緒狀態用事件通知。
  - 不加逐 feed 的 `required` 標記（F33）。selector 在 `ready_timeout_s` 內推導不出結果，或成員一直沒資料，都列在 `ready.missing_feeds`，由策略在 `on_ready` 判斷。
- **啟動改成非同步（F12）：**
  - `POST /sts/deploy/{type}` 驗證、寫入 SessionSpec、登記 intent、啟動 worker 之後就**回 202**：`{session_id, status: "starting"}`。
  - 啟動進度寫在 session row 的 status 和 conditions 上，同時發佈到 `sts.status.{session_id}`。UI 透過現有的 WS 和 board 顯示，例如「on_start 執行中 42 秒」、「MdReady 12/14」、「等待帳號 42 的 recon」。
  - 啟動失敗時由 Supervisor 寫入原因：`on_start` 拋例外、`start_timeout_s` 逾時、TD 沒有就緒、worker 在啟動期間 crash。API 不做回滾，intent 依 §8.2 回收。
  - CLI 的 `mftik run` 預設 `--wait`：追蹤 status 直到 `running` 或 `failed`，接著 tail log。`--no-wait` 只回傳 session_id。
  - **兩個 timeout：**

    | 設定 | 計算範圍 | 預設 / 上限 | 超過時 |
    |---|---|---|---|
    | `start_timeout_s` | 只算 `on_start` | 60 / 3600 秒 | kill，failed（初始化失敗，不重啟） |
    | `ready_timeout_s` | 從 `on_start` 結束算起 | 30 秒 | TD 沒就緒 → failed；只有 MD 沒到齊 → 照樣呼叫 `on_ready`，附缺少的 feed 清單 |

  - 刪除 API 寫死的 10 秒 create timeout（隨 `deploy_strategy` 一起刪除），以及 CLI 的 `deploy_http_timeout` 和它的預算常數（`_STS_CREATE_S` 等）。

**crash 之後（F10）**

先清場，再決定 fail 還是重新掛起。重新掛起時，從 `on_start` 全新開始，不帶任何舊狀態。

| crash 類型 | `on_stop` | 平台清場 |
|---|---|---|
| A：策略代碼拋出例外，進程還活著 | **保證呼叫**：ingress 把 `on_stop` 排進策略 loop（受 `ON_STOP_TIMEOUT_S` 限制），之後進程以「crashed」結束 | 仍然執行，當作保險 |
| B：一般 hook 阻塞策略 loop 超過 30 秒（F15），或 stop 時策略 loop 卡住超過 grace | **無法呼叫**：loop 卡住，跑不了任何策略代碼；shim kill | 執行 |
| C：進程死亡（OOM、segfault、SIGKILL） | **無法呼叫**：進程已經不在 | 執行 |

**平台清場：** Supervisor 呼叫 TD 的 `td.order.cancel_session(session_id)`（§7.1），撤掉所有 `client_order_id` 裡 session 欄位等於這個 session 的掛單，並等到全部確認。B 和 C 只能靠這一步代替 `on_stop`。部位無法用撤單處理，原樣保留。

**接著依序：**

1. Supervisor 把 `sts_sessions` 寫成 `restarting`，記下 reason、exit 資訊、重啟次數，並在 `log.sts.{session_id}` 發一條 `error` 等級的 log。既有的 Alert 管線（Discord）可以直接比對這條 log。
2. 決定 fail 還是重新掛起（F11）：
   - deploy 設定 `restart: never`（**預設**）→ `failed`。
   - **只有 A 類 crash 有資格重新掛起。** B（loop 卡死）、C（進程死亡）在平台清場後一律 `failed` 並發 alert。這兩類的成因通常是同一份資料、同一段代碼，重啟很可能重演；而且策略自己的 `on_stop` 沒有跑到。
   - crash 發生在 `on_ready` 之前，也就是初始化失敗 → `failed`，因為通常是設定錯，重啟沒有幫助。
   - 在 `restart_window_s`（預設 600）內的重啟次數超過 `max_restarts`（預設 5）→ `failed` 並發 alert（沿用 prototype 的 FATAL）。
   - 清場沒有全部確認 → `failed` 並發 alert，不能在狀態不明的掛單旁邊重新開始。
   - 其他情況（`restart: on_failure` 的 A 類 crash）→ 重新掛起。
3. 指數 backoff，**最短 1 秒**，然後以同一個 `session_id`、incarnation + 1 啟動新的 worker，從第 0 階段完整走一遍（§5.3）。

**不變式：**

- **R1** 舊 incarnation 確認死亡（有 shim 的 exit 紀錄）且清場完成之後，新 incarnation 才能啟動。兩者不會並存。
- **R2** backoff 至少 1 秒。`client_order_id` 的組成是 `session(24) | ts_sec(28) | seq(8)`，seq 每個 incarnation 都從 0 開始；舊的最後一張單和新的第一張單一定落在不同秒，所以不會撞號。
- **R3** 新 incarnation 的 recon 不會看到舊的掛單（已經清場），但會看到既有部位。策略的 `on_start` / `on_ready` 必須能接受「開始時已經有部位」，這是 `restart: on_failure` 的使用前提，要寫進 SDK 文件。
- **R4** 重新掛起的期間，MD/TD 的 intent 不回收。Supervisor 的存活報告列的是「desired 為 running 的 session」，包含 `restarting`，而不只是當下活著的 worker（§8.2）。

### 5.3 Hook 執行模型（G3，F8）

**問題不只在續約。** 拿掉 per-session lease（§8.2）之後，策略 hook 佔住 event loop 仍然會造成三個平台層級的問題：

1. **NATS socket 沒人讀。**
   - core NATS 的 fire-and-forget 指的是投遞語意：不 ack、不重送、publisher 不等 subscriber。server 仍然得把每則訊息寫進每個 subscriber 的 TCP 連線。
   - loop 被佔住時沒人讀 socket，以期權鏈的流量，雙方的 kernel buffer 幾秒內就滿。
   - NATS 2.11 對每條連線設了上限：寫入阻塞超過 `write_deadline`（預設 10 秒），或積壓超過 `max_pending`（預設 64 MB），server 就以 slow consumer 為由**切斷整條連線**。本機的 `deployment/nats/nats.conf` 沒有覆寫這兩個值（這個檔案被 `.gitignore` 排除，不在 repo 裡）。
   - 斷線期間發佈的訊息不會重送，成交回報也會一起遺失。
2. **假 timeout。** uvloop 每一輪先跑到期的 timer，才 poll I/O。所以等 ack 的 timer 會比已經到達 socket 的回覆先觸發。
3. **恢復後交付的是過期資料。**

**定案：STS session worker 由兩條 thread 組成（F8）。** MD/TD worker 不跑使用者代碼，維持一個 loop 即可；STS controller 也不需要。

| | ingress thread | strategy thread |
|---|---|---|
| 位置 | main thread，自己的 uvloop | 第二條 thread，策略的 uvloop |
| 連線 | 接收連線：MD atom、TD 帳號事件、`sts.ctl.{session_id}`、回覆 inbox | 送出連線：下單、撤單、`td.account`、`md.fetch`、log |
| 負責 | 持續讀 socket；event log（收到就記）；依交付策略排隊或 conflate；RPC timeout 以真實時間計算；控制訊號；對 shim 的 heartbeat 和 progress | 策略 hook、timer；從 ingress 取事件並在這裡解碼；**直接 publish 送單** |
| 不做 | 不解碼行情、不跑使用者代碼、不做阻塞 I/O | 不讀接收連線 |

**下單路徑（無跨 thread）：**

1. 策略 thread 在自己的送出連線上直接 publish，`reply` 指向 ingress 的 inbox。
2. publish 後**強制 flush**（`_flush_pending(force_flush=True)`）。nats-py 的 `publish` 只是寫進 buffer，不 flush 的話，送完單接著做長計算，單會一直躺在 buffer 裡。
3. TD 不需要修改：它照常回覆到 `msg.reply`。
4. ack 由 ingress 收下，記進 event log，timeout 以真實時間判斷，再以 `call_soon_threadsafe` 交回策略的 future。跨 thread 只發生在回程。
5. 送單之前，必須先在共用的 pending 表登記 future，避免回覆比登記先到。
6. 需要驗證：NATS 的 no-responders（503）在 `reply` 不屬於送出連線時是否照常送達。

**ingress thread 的生命週期：與進程同生共死，也就是與 session 同生共死。**

| 階段 | ingress thread | strategy thread |
|---|---|---|
| 0 啟動 | **最先啟動**：建立接收連線、訂閱 `sts.ctl.{session_id}`、開始對 shim 送 heartbeat | 尚未啟動 |
| 1 載入 | 訂閱 MD feed 並開始消化 | 建立送出連線；import 策略、驗證參數 |
| 2 `on_start` | 繼續收，但**不交付任何事件**；status 回報「`on_start` 已跑 N 秒」 | 跑 `on_start`，可以同步、可以很長（F3） |
| 3 就緒 | `on_start` 結束後才訂閱 TD 帳號事件並觸發 recon；`MdReady`、`TdReady` 都成立後通知策略 | 收到 `on_ready` |
| 4 running | 持續消化、記錄、交付 | hook；直接送單 |
| 5 stopping | 收到 stop（控制訊號或 SIGTERM）後轉交策略；**繼續收 ack 和 fill，直到 `on_stop` 結束** | 跑 `on_stop`；撤單的回覆經 ingress 回來 |
| 6 收尾 | 寫最後狀態、flush event log、NATS drain、關閉；進程退出 | 已結束 |

TD 訂閱延後到 `on_start` 之後的原因：帳號事件是整個帳號的廣播，其他 session 的成交也會進來。如果在很長的 `on_start` 期間就開始累積，量可能很大，而 TD 事件又不能丟。延後訂閱再立刻 recon，就能直接拿到當下的快照。

**異常時：**

| 狀況 | 處理 |
|---|---|
| ingress 意外結束 | 整個進程 fail-fast，以非 0 結束；不在進程內重啟（不 rebuild） |
| 策略 thread 拋出例外 | ingress 走收尾流程，status 記為 failed |
| 策略卡住超過 stop grace | ingress 已回報卡在哪個 hook；由 shim / Supervisor 送 SIGKILL |
| NATS 斷線重連 | thread 不變；斷線時每個 feed 收到 `on_md_update(feed, "down", "ingress_reconnect")`，重連後收到 `live`（F23）；對每個帳號做平台 recon，收斂後觸發 `on_resync(cause="reconnect")`（F13） |
| shim 消失（EPIPE 或 PDEATHSIG 送來的 SIGTERM） | 當作 stop 處理 |

**不變式：**

- **I1** ingress 先於策略啟動、晚於策略結束。所以 `on_stop` 撤單時的回覆和成交一定收得到。
- **I2** ingress 的生命週期等於進程，也等於 session。它不跨 session，也不在進程內重啟。
- **I3** ingress 跑在 main thread，因為 Python 的 signal handler 只在 main thread 執行。SDK 禁止策略自己註冊 signal handler。
- **I4** ingress 不跑使用者代碼，也不做阻塞 I/O。寫檔交給 event log 的 writer。

**交付策略（F8，每個 feed 可以在 strategy.yml 覆寫）：**

| 類型 | 預設 |
|---|---|
| ticker、bestquote、greeks、funding、OI、orderbook | `latest`：只留最新一筆，在解碼前就 conflate（orderbook 每次推送完整 snapshot） |
| kline | 以 `(feed, bar 開盤時間)` 為 key 保留最新，不會丟掉任何一根收盤 bar |
| trade、aggtrade、liquidation | `all`：有界佇列，溢出時丟最舊的，寫 warning log 並累加 status 上的丟棄計數；策略以 `event.seq` 自己偵測跳號（F25） |
| TD 事件、`feed_end`、RPC 回覆 | `all`，不丟；溢出時視為異常並 fail session |

每個事件帶 `recv_ts`，策略可以用 `event.age` 判斷資料延遲了多久。MD 的事件另外帶 `seq`（F25）：per-atom，在同一個連線 worker incarnation 內連續，`on_md_update` 收到 `live` 之後重新起算。`all` 類 feed 跳號代表漏收（佇列溢出或 NATS 層的遺失），由策略自己記錄；`latest` 類的跳號是設計上的覆蓋，不代表漏收。

**event log 併入 ingress：**

- 入站事件在**收到時**就記錄，帶 seq 和 `recv_ts`，可以寫原始 bytes 加一個小 header。每筆事件之後的去向另外標記：`delivered`、`superseded`（被 `latest` 蓋掉）、`dropped`。
- 出站紀錄由策略 thread 在送出的那一刻，以 thread-safe 的 `put_nowait` 交給 writer。
- 寫檔維持在 writer thread。佇列滿了照現行規則：丟棄、計數、seq 留洞。
- 沒設 `STS_EVENTLOG_DIR` 時，只關掉寫檔這一步，ingress 本身照常運作。

**GIL：**

- 純 Python 的 CPU-bound hook 每過 switch interval（預設 5 ms）會被迫釋放 GIL，ingress 仍然讀得到 socket。numpy、torch 本身就會釋放 GIL。
- ingress 只搬 bytes、不解碼，把它需要的 GIL 時間壓到最低。
- 長時間持有 GIL 的 C 擴充仍然會餓死 ingress，這類運算應該交給 `offload` 的 process 模式（§5.5）。
- switch interval 是否調小，B4 實測後再決定。

**其他：**

- **hook 時間預算（F15）：** 一般 hook 量**阻塞時間**，也就是佔住策略 loop、沒有 `await` 讓出的連續時間。`await self.offload(...)` 期間 loop 是空的，不算阻塞，所以 hook 裡可以照常使用 `offload`。生命週期 hook 量**牆鐘時間**，因為生命週期本身就在等它們跑完。

  | hook | 量什麼 | 上限 | 超過時 |
  |---|---|---|---|
  | 一般 hook（`on_ticker`、`on_order_update`、timer 回呼等） | 阻塞時間 | 1 秒 | warning log（Alert 抓得到）、`HookSlow` 計數加一，不 kill |
  | 一般 hook | 阻塞時間 | 30 秒（硬上限） | 視為 B 類 crash：kill、平台清場、failed（§5.2） |
  | `on_start` | 牆鐘時間 | `start_timeout_s`（預設 60、上限 3600 秒，F12） | 初始化失敗：failed，不重啟 |
  | `on_ready` | 牆鐘時間 | 10 秒 | 初始化失敗：failed，不重啟 |
  | `on_stop` | 牆鐘時間 | 10 秒（等於現在的 `ON_STOP_TIMEOUT_S`） | 不再等，直接走平台清場，然後 kill |

  - 阻塞時間由策略 loop 自行量測，例如在每次 dispatch 前後記錄時間，或用一個固定間隔的 timer 測 loop lag。ingress 讀取這個數字，並在 status 的 progress 裡回報「卡在哪個 hook、多久了」。
  - 1 秒只發警告，因為 ingress 接手 I/O 之後，hook 慢不會讓平台出錯，只會讓策略自己反應變慢。30 秒則必須強制結束：一個卡住 30 秒的 loop 連撤單都做不到，放著它比結束它更危險。
  - 不提供 `limits.hook_timeout_s` 這類讓策略自訂上限的設定。
- MD/TD 失聯不再直接 fail session，改成通知策略（F14，§5.6）。
- `breathe` / `slice_deadline` 刪除，重計算改用 `offload`（F9，§5.5）。

### 5.4 移除清單（範圍 4）

| 類別 | 項目 |
|---|---|
| Rebuild | `rebuild_interrupted`、`rebuild_session`、`adopt_interrupted`、`_spawn_rebuild`、`_rebuild_after_exit`、`_settle_rebuild`、`rebuild_on_worker_exit`、`STS_REBUILD_ON_BOOT`、`STS_REBUILD_MAX_AGE_S`、worker 的 `rebuild` role |
| Strategy API | `Strategy.rebuildable`、`on_rebuild`、`remember()`（F10）；`chase` 寫入和讀回 `started_ms`、滑價錨定價的兩段改為只存在記憶體 |
| Worker 的 DB 存取 | `persist_live`、`mark_done`、`mark_live`、`remember_fact`、`bump_rebuild_count`、`reset_rebuild_count`、`load_session`、`list_db_sessions`、`td_instance`、`derive_sts` 等傳給 worker 端 `SessionManager` 的 DB 函式全部移除；session row 改由 Supervisor 寫（F10） |
| strategy.yml | `restart` 的舊值 `always` / `never`（rebuild 語意）。改為 `never`（預設）/ `on_failure`，另加 `max_restarts`、`restart_window_s`（F11） |
| DB | `st_facts` 刪除（F36）；`rebuild_count` 改名為 `restart_count`；`restart` 欄位保留，改存新語意（F11） |
| 與父進程綁定 | `LIFELINE_FD_ENV`、`arm_parent_death`、`_watch_lifeline`、`MFTIK_STS_PARENT_PID`（pdeathsig 改指向 shim） |
| Manager | `reap_orphans`（由 reattach 對帳取代）、雙模式 `SessionManager`（`_create_in_process`，worker 與 parent 共用同一個類別） |
| Recon API（F13） | `Strategy.send_recon`、`on_recon_done`、`STS_RECON`、`StsSession._recon_sent` 與 `_on_lease_ack` 裡的自動 recon；TD 的 `_handle_recon` 改成平台內部 recon 和 `view(settled=True)` 共用 |
| 續約 | `_lease_heartbeat_loop`、`_md_acks` / `_td_acks` 的 stale 判定、`_heartbeat_overslept`、`_shift_peer_acks`、`_fail_from_infrastructure("md feed …")`（§8.2） |
| 讓出 loop（F9） | `breathe`、`slice_deadline`、`SLICE_S`、`tape.read(on_print=…)` 每筆讓出的邏輯；由 `offload` 取代（§5.5） |

### 5.5 offload（F9）

**目的：** ingress 保護的是平台這一側。策略自己的 loop 仍然是單 thread，如果某個 hook 同步算了 20 秒，這段期間策略自己的成交 hook、timer、`on_stop` 都要等。`offload` 把重的計算整段移出策略的 loop，讓策略在計算時還能反應。它取代 `breathe` / `slice_deadline`（§5.4）。

**API：**

```python
# thread 模式（預設）：會釋放 GIL 的運算，可以直接用已載入的物件
signal = await self.offload(self.model.predict, features)

# process 模式：純 Python 的重計算、長時間持有 GIL 的 C 擴充、記憶體風險高的推論
surface = await self.offload(fit_vol_surface, chain, isolate=True)

# process 模式加常駐狀態：子進程啟動時執行一次 init（例如載入模型），之後每次呼叫重用
self.ml = await self.offload_pool(init=load_model, init_args=(path,), workers=1)
y = await self.ml.call(predict, x)      # 在子進程執行 predict(state, x)，state 是 init 的回傳值
```

**兩種模式：**

| | thread（預設） | process（`isolate=True` / `offload_pool`） |
|---|---|---|
| 執行位置 | 每個 session 一個 thread pool | 每個 session 一個 process pool，用 `spawn` 啟動。worker 進程裡已經有多條 thread，`fork` 不安全 |
| 傳遞資料 | 共用記憶體，不需要 pickle | 函式、參數、結果都必須可 pickle；函式必須是模組層級 |
| 中斷 | 不能。stop 時取消的只是等待結果的 coroutine，thread 會繼續跑完 | 能。stop 時直接 terminate |
| 對 ingress 的影響 | 純 Python 運算會和 ingress 搶 GIL，所以這類運算應該用 process 模式 | 完全不影響 |
| 記憶體 | 和 session 共用 | 子進程 `oom_score_adj` 再往上調到 +900，比 session 本身先被犧牲；可以用 `limits.offload_memory_mb` 設 `RLIMIT_DATA` |
| 子進程死掉時 | — | 呼叫端收到 `OffloadWorkerLost`（帶原因）；pool 在下次呼叫時重建，`offload_pool` 會重新執行 init |

**規則：**

- offload 出去的函式不能呼叫 SDK，例如 `submit_order`、`log`。SDK 會檢查呼叫者所在的 thread，不在策略 thread 上就拒絕。
- 輸入要明確傳入、結果要明確回傳。thread 模式下不要在函式裡改策略物件的狀態，因為策略的 loop 同時還在跑。
- 例外照常傳回給 `await` 的呼叫端。
- 平行度由 strategy.yml 設定：`limits.offload_threads`（預設 2）、`limits.offload_processes`（預設 1）。

**生命週期（對齊 §5.3 的階段）：**

- pool 在第一次呼叫時才建立；`on_start` 裡也可以用，例如用 `offload_pool` 在子進程載入模型。
- 第 5 階段（stopping）：`on_stop` 照常執行，還在進行中的 offload 不會自動取消。
- 第 6 階段（收尾）：
  - process 模式：`shutdown(cancel_futures=True)`，再 terminate 剩下的子進程。
  - thread 模式：無法中斷，進程會等它跑完才退出。超過 stop grace 就由 shim 送 SIGKILL。
- process pool 的子進程啟動時設 `PDEATHSIG`，並確認 parent pid 沒變。session worker 死掉，子進程跟著結束，不會留下孤兒。

**可觀測性：**

- ingress 的 progress 列出進行中的 offload：函式名稱、模式、已執行時間。例如「offload `predict`（process）已跑 41 秒」。
- event log 記錄 `offload_start` / `offload_end`：函式名稱、模式、耗時、結果（ok / error / cancelled / lost）。參數只記大小，不記內容。
- 准入控制（§4.7）計算 session 記憶體時，把 process 模式的子進程也算進去。Supervisor 讀取 worker 的整棵子進程樹。

**取代既有用法：**

| 原本 | 改成 |
|---|---|
| `breathe` / `slice_deadline` 切片 | 整段計算交給 `offload` |
| `tape.read(on_print=…)` 每筆讓出 | `await self.offload(fold, tape.records)` |


### 5.6 MD / TD 失聯（F14）

`on_ready` 之後，MD 或 TD 中途斷掉時策略會看到什麼。現在的做法是 `_lease_heartbeat_loop` 3 秒沒看到 ack 就 `_fail_from_infrastructure`，這套機制隨 lease 一起刪除（§8.2）。

**原則：**

1. **失聯只通知，不自動 fail。** 斷線可能只有幾秒，策略能不能撐過去由策略自己判斷：要等、要降級，還是呼叫 `self.fail()`。平台不提供「失聯多久就 fail」的設定。
2. **訊號來自權威來源**（P7）。平台只轉發 MD / TD 自己知道的狀態變化。資料新不新鮮，由策略看 `event.age` 判斷；冷門合約很久沒有報價是正常的。
3. **TD 不可用時，下單在本地直接拒絕，不送出。**

**狀態廣播（取代 lease）：** MD 連線 worker 和 TD 帳號 worker 各自週期性**單向**廣播自己的狀態，STS 只負責聽，不回 ack，也沒有 per-session 的狀態。

| 廣播者 | subject | 內容 | 頻率 |
|---|---|---|---|
| MD 連線 worker | `md.w.{instance}.{worker_id}`（暫定） | incarnation、連線狀態、狀態版本號；atom 的狀態變化另外以事件發出 | 狀態變化時立即發一次，平時每 2 秒一次 |
| TD 帳號 worker | `td.account.state.{api_id}`（暫定） | incarnation、`ready` / `degraded` / `unavailable`、狀態版本號 | 同上 |

- 廣播由 **worker 自己**發出，不是 controller。controller 滾動期間 worker 照常運作，廣播不會中斷（P1）。
- ingress 依版本號判斷有沒有漏掉狀態事件；有漏的話，就向該 worker 查詢完整狀態。
- 某個 worker 的廣播**靜默超過 10 秒**（漏了 5 次），就把它負責的 feed 標成 `down`、帳號標成 `unavailable`。這是 P7 唯一的例外，只用來處理整台主機失聯，因為那時沒有任何人能發出狀態。它只產生通知，不會回收任何資源，所以誤判的代價很低。

**各種情況：**

| 情況 | 策略收到 | 恢復後 |
|---|---|---|
| MD 連線 worker crash 或重連（含交易所斷線） | 受影響的每個 feed 收到 `on_md_update(feed, "down", reason)` | 重新訂閱成功後收到 `on_md_update(feed, "live", …)`。中間缺了什麼，由策略依這兩個通知自己記錄（F23） |
| feed 永久結束（到期、下市） | `on_feed_end`（沿用現有） | 不會恢復 |
| TD 帳號 worker 重啟或換版 | `on_td_update(api_id, "unavailable", reason)` | 先 `on_resync(api_id, "account_reset", view)`，再 `on_td_update(api_id, "ready", …)` |
| TD 帳號的交易所私有連線斷線 | `on_td_update(api_id, "degraded", reason)`：可能還能下單，但成交回報會延遲 | `ready`；TD 自己的 reconcile 結果照常以 order update 送達 |
| MD / TD controller 重啟 | 什麼都不會收到（P1） | — |
| STS 自己的 ingress 斷線重連 | 所有 feed 收到 `on_md_update(feed, "down", "ingress_reconnect")` | 所有 feed 收到 `live`；每個帳號收到 `on_resync(cause="reconnect")` |
| 整台主機失聯 | 廣播靜默 10 秒後，收到 `down` / `unavailable` | 廣播恢復後回到 `live` / `ready`；帳號另外收到 `on_resync` |

**策略的介面：**

```python
async def on_md_update(self, feed: str, state: str, reason: str) -> None: ...    # "live" | "down"
async def on_td_update(self, api_id: int, state: str, reason: str) -> None: ...  # "ready" | "degraded" | "unavailable"
# 沿用：on_feed_end。F13 新增：on_resync
# 沒有 on_feed_gap（F23）：漏收由策略依 on_md_update 自己記錄

self.md.state(feed)       # 隨時可查目前狀態
self.td.state(api_id)
```

- **組合型 feed（F19）：** 一個 feed 由多個 atom 組成時，任何一個 atom `down`，這個 feed 就是 `down`；全部回到 live 才是 `live`。
- `on_md_update` / `on_td_update` 只處理**連線與可用性的狀態變化**，不是行情或訂單事件。行情走 `on_ticker` 等，訂單走 `on_order_update`。SDK 文件要寫清楚這個區別。
- **下單：** 帳號 `unavailable` 時，`submit_order` / `cancel_order` 立刻回傳 False，reject 原因是 `td_unavailable`，不送出，和現有「False = 沒送到交易所」的語意一致。`degraded` 時照常送出。
- **conditions：** `MdReady`、`TdReady` 在 `on_ready` 之後繼續反映即時狀態，UI 和 board 看得到。每次狀態轉換都寫一條 warning log，Alert 管線可以直接比對。


---

## 6. MD

### 6.1 原子訂閱（Atom）

**定義：** `Atom = (venue, endpoint, channel)`，其中 channel **逐字等於交易所的 subscribe 參數**，不是平台宣告的寫法。

| venue | 例子 |
|---|---|
| Deribit | `(Deribit, public, "ticker.BTC-27DEC26-100000-C.100ms")`、`(Deribit, public, "book.BTC-PERPETUAL.none.20.100ms")` |
| BinanceUM | `(BinanceUM, market, "btcusdt@bookTicker")` |
| OKX | `(OKX, public, "tickers:BTC-USDT-SWAP")`，由 `arg_key()` 正規化成字串 |
| Gate | `(GateFutures, public, "futures.tickers:BTC_USDT")`，每個合約一個 atom。**MDS-4 的 identity 問題在這個定義下自然消失** |

- `atom_id` 是正規化字串，subject 用它的穩定 hash：`md.a.{venue}.{hash}`，因為 channel 字串本身帶有 `.`。MD 維護 hash 和 atom 的對照表。
- 每個 venue adapter 提供以下純函數，放在 `mftik.exchange.<venue>`：
  - `atoms_for(topic, ticker, opts) -> AtomPlan`：平台 topic 對應到哪些 atom、用哪個 projector。取代現在 `_open` 解析成 `stream_*` 的做法。mftik feed 對 atom 本來就是一對多（F19），例如 Binance UM/CM 的 ticker 需要 `@ticker` 加 `@bookTicker`。反方向的多對一（Deribit 的 ticker、greeks、OI 共用 `ticker.*` channel）由 `decode` 從一個 frame 產出多個事件處理。
  - `decode(atom, frame) -> list[Event]`：一個 frame 產出的正規化事件，使用平台既有的 model（`Ticker`、`Greeks`、`OpenInterest` 等）。
  - `capacity(endpoint)`：每條連線的 atom 上限、訊息速率、subscribe 的批次大小和速率限制。
  - `join_policy(atom)`：late joiner 的語意，沿用 MdVenueSubscriptions I5。
- **STS 這端的宣告和 hook 都不變。** 仍然寫 `bestquote.Deribit_Option_...`。MD 在 intent 登記時解析出 atom，回傳 `{feed: [atom_id]}`。session 訂閱對應的 `md.a.*` subject，再依 envelope type 路由到 hook，只送出它宣告過的類型。
- **跨連線組合（F19）：** 組成同一個 feed 的 atom 可能落在不同連線上，也就是不同進程（F17）。MD 只發佈原子事件，不在 MD 裡組合；組合在 STS 端以平台通用的純函數完成。
  - 例子：Binance UM/CM 的 `ticker` = `join(BestQuote, TickerStats)`。`@bookTicker` 對應既有的 `BestQuote`，`@ticker` 對應新的 venue 中立模型 `TickerStats`（不含 bid/ask）。每次 stats 到達時帶上最新報價輸出 `Ticker`，報價還沒到就不輸出，沿用現行規則。
  - join 不含 venue 代碼、運算量可以忽略，放在 ingress 上不違反 I4。
  - 目前所有 venue 裡，跨連線組合只有這兩處（`_merge` 只出現在 Binance future 和 delivery）。
  - 不另設組合 worker：多一跳 NATS、多一個進程與生命週期，而且它的輸出不是交易所的 subscription，會破壞 atom 的定義。
- **MD 是行情的權威（F21）**，地位對應 TD 之於 ledger。解碼、book 的 fold（快照加增量）、late joiner 拿到的快照，都由連線 worker 負責；`md.a.*` 上傳的是平台 model（`OrderBook`、`Ticker`、`Trade` 等）。STS 不接觸交易所原文，也不持有 fold 狀態。這和現行分工相同：MD 現在發出的就是 `model_dump()` 之後的平台 model。
- **這推翻了 MdVenueSubscriptions 的 I6（「MD stays out of venue vocabulary」）。** 從此 MD 在 atom 層講 venue 詞彙；STS 仍然只看得到不透明的 `atom_id`。

### 6.2 MD orchestrator（controller）

- **需求來源（demand）：**
  1. session intent：某個 session 要哪些 feed。
  2. 常駐訂閱：在設定檔中宣告、不屬於任何 session 的訂閱。專為錄 tape 而存在的 `tape_keeper` 策略因此可以退役。
  3. selector：ATM 期權鏈、轉倉（§6.4，F33）。session intent 和常駐訂閱都可以帶 selector。
- `desired_atoms = ⋃ demand`，每個 atom 記錄它的 owner 集合。owner 進入 terminal 時由 GC 移除。
- **placement**：依 `(venue, endpoint)` 的 capacity 把 atom 分配到連線上。分配有黏性：新 atom 優先放進已有的連線，容量不夠時才開新的連線 worker。atom 放上去之後就不搬（F22）；連線上沒有任何 atom 時，該 worker 結束。不做整併，代價是連線可能碎片化，每條碎片多佔一個進程的 RSS（§4.7）。
- 把每條連線的 desired set 連同 `generation` 推給對應的連線 worker（F18）：
  - 每次都推完整清單，不推增量（level-triggered）。
  - `generation = (controller_epoch, seq)`。`controller_epoch` 在 controller 每次啟動時於 DB 遞增；worker 只接受比手上更大的 generation，所以新舊 controller 短暫重疊時，舊的推送不會蓋掉新的。
- **到期**：依 SYM listing 判斷某商品已到期時，從 desired 移除對應 atom，並對它的 owner 發出 `md.feed.end(expired)`。這取代 `_expiry_tasks`。
- controller 重啟時，從 DB 裡的 intent 和常駐設定重算 desired。重算完成、推出新 generation 之前，worker 保持上一份 desired（P5）。

### 6.3 連線 worker 與 reconciler

- 一個 worker 對應一條 websocket（F17）。reconciler 跑在 worker 裡面（F18），理由：
  - observed 以交易所 ack 為準、以連線 epoch 為鍵，只有 worker 看得到。
  - 重連後的補訂，以及 book 出現缺口時的 resync（先退訂再重訂），都是對單一 atom 的 reconciler 動作，可以在本地完成。現在 Deribit、Bybit、OKX、Bitget 的 adapter 也是在 socket 裡做。
  - controller 重啟時訂閱不會抖動。
- **reconciler：** 比對 `desired(gen)` 和 `observed`。observed 以交易所 ack 為準，並以連線 epoch 為鍵，舊 epoch 晚到的 ack 一律丟棄。差異合併成批次，經 token bucket 限速後送出。重連後 observed 歸零，下一輪 diff 會自動補齊。每個 atom 回報 `pending`、`subscribed`、`first_msg_at`、`last_msg_at`、`error`。現行的 `WireLedger` 就是這個 observed set，可以搬過來繼續用。
- **狀態廣播（F14）：** 連線 worker 單向廣播自己的狀態（incarnation、連線狀態、狀態版本號），狀態變化時立即發一次，平時每 2 秒一次；atom 的狀態變化另外以事件發出（§5.6）。
- **解碼與發佈：** 每個 frame 只解碼一次，發佈到 atom subject，附上 per-atom 序號和 `owner=(worker_id, incarnation)`。需要錄的 atom（`trade`、`aggtrade`、`liquidation`，F20）append 到該區域的 Redis tape，key 改成 `atom_id`。
- **不做連線遷移（F22）：** 每個 atom 任何時刻只在一條連線上，所以只有一個發佈者，訂閱端不需要去重。連線 worker 原地重啟時，Supervisor 確認舊進程結束後才啟動新的（delete-before-create）。envelope 上的 `owner` 只用於診斷。
- tape 只由持有該 atom 的連線 worker append。重連或原地重啟造成的空洞會被量測，記錄在 coverage。
- **tape 的範圍（F20）：**
  - 以 atom 為單位錄製，只錄歷史無法回補的 trade 類：`trade`、`aggtrade`、`liquidation`。book 和報價類不錄：單筆約 1500 bytes 對 200 bytes，而且下一則推送就是完整狀態。
  - 有 demand 的 atom 才錄，demand 包含常駐訂閱。coverage 以 atom 為單位記錄。
  - Binance UM 的 `trade` 和 `aggtrade` feed 都來自 `@aggTrade`，改成 atom 之後只錄一份。
  - 讀取仍由 MD 服務，STS 不開 Redis。

### 6.4 從宣告式推導的訂閱：selector（F33，B9）

**宣告：** `md:` 除了靜態 feed，再加上 `select:` 項目。session 的 strategy.yml 和 MD 的常駐訂閱都可以用。

```yaml
md:
  md-jp:
    - ticker.Deribit_Perp_BTCUSD
    - select: btc_chain
      kind: option_chain
      venue: Deribit
      underlying: BTC
      ref: ticker.Deribit_Perp_BTCUSD   # 參考價；由 selector 自己持有，策略不必另外宣告
      expiries: {nearest: 2, min_tte: 2h}
      strikes: {atm: 5}                 # 每個 expiry 取 ATM ± 5 檔，用該 expiry 實際掛牌的 strike
      sides: [C, P]
      topics: [ticker, greeks]
      recenter: {strikes: 1, min_dwell: 60s}
    - select: btc_q
      kind: rolling_future
      venue: Deribit
      underlying: BTC
      tenor: quarterly                  # weekly | monthly | quarterly
      roll_before: 3d
      topics: [ticker, trade]
```

**推導（MD orchestrator）：**

- 每種 selector 是純函數 `evaluate(listing, ref, now, prev) -> Selection | Hold`。listing 來自 SYM，ref 是 MD 自己的行情。
- `option_chain`：
  - 依 `expiries` 選出 expiry；離到期不到 `min_tte` 的跳過，所以期權鏈在舊 expiry 結束前就會移到下一個。
  - 每個 expiry 以 ref 找出最近的掛牌 strike 當中心，取上下 `atm` 檔，各 strike 的 C/P 依 `sides`。
  - **防抖動：** ref 離目前中心超過 `recenter.strikes` 檔才重新置中，兩次置中至少間隔 `min_dwell`。
- `rolling_future`：
  - 依到期日把合約分成 weekly、monthly、quarterly，取對應 tenor 最近的一張當 current。
  - 到期前 `roll_before` 時 current 切到下一張。**舊合約保留到到期**才移除，轉倉期間兩邊行情都在。
- **fail-static（P5）：** listing 過期、ref 斷線、MD controller 不在時，一律維持上一份結果。
- **持久化：** `prev`（上一次的 Selection、epoch、置中狀態）存在 DB。controller 重啟後從這裡接著算，不會重新置中。
- **共用：** 規格相同（spec hash 相同）的 selector 只算一份，所有 owner 拿到同一個 universe 和 epoch。
- **容量：** 部署時就能算出 atom 上限。例子裡的期權鏈是 2 × 11 × 2 = 44 個 atom：Deribit 的 ticker 和 greeks 共用 `ticker.*` channel，所以不會變成 88 個。這個上限用於 deploy 的容量檢查（§4.7 的准入控制）。

**給策略：**

```python
async def on_universe_change(self, name: str, change) -> None: ...
# change.added、change.removed、change.epoch；rolling_future 另有 change.current

self.md.universe("btc_chain")   # 目前選中的合約集合
self.md.current("btc_q")        # rolling_future 目前的 current
```

- 只有這一個 hook，轉倉也走它（`change.current`），不另設 `on_roll`。
- **I-SEL1：** 某個合約出現在 `added` 之前，策略不會收到它的事件；出現在 `removed` 之後，也不會再收到。做法是 ingress 收到 `md.universe.{session_id}` 之後，先訂閱新合約的 atom subject、交付 hook，再退訂被移除的合約，並丟掉佇列裡屬於它們的事件。
- 期權鏈的成員到期時，照常收到 `on_feed_end(expired)`，接著是 `removed` 含該合約的 `on_universe_change`。

**不做的事：**

- **pin：** 期貨的舊合約本來就保留到到期；期權目前 TD 不能下單，沒有東西需要 pin。所以 MD 不必向 TD 查部位。等 TD 支援期權下單時，再加由策略明確宣告的 pin。
- **`required`：** 不加。selector 在 `ready_timeout_s` 內推導不出結果，或成員一直沒資料，都列在 `ready.missing_feeds`，由策略在 `on_ready` 自行判斷（F12）。

### 6.5 被取代或移除的部分

- `Dispatcher` 的 per-session fan-out（`md.{session_id}`）、`StsLink`、MD 端的 lease loop。
- 每個 venue 一條 socket 的 `VenueSession` 模型、`_expiry_tasks`。
- 各 adapter socket 類別裡的訂閱管理。adapter 只留下傳輸和 decode。
- `docs/MdHandover.md`（由 §4.6 取代）。
- `docs/MdVenueSubscriptions.md` 的 I6（由 §6.1 推翻）。

---

## 7. TD

### 7.1 帳號 worker（範圍 5）

- **單位（F34）：** 一個進程對應一個 `api_id`，持有該帳號所有私有連線。Bybit（`/v5/trade` 加 private stream）和 Binance UM/CM（WS API 加 user stream）一個帳號就有兩條 websocket；拆成兩個進程，OMS 和 ledger 就得跨進程同步。
- **兩層生命週期（F35）：**
  - **常駐層：** TD instance 名下每個啟用的帳號，都有一個常駐的帳號 worker，和有沒有 session 無關。它一啟動就對交易所建立 HTTP 連線並保持溫熱。現行的 httpx client 用預設設定，閒置 5 秒（`keepalive_expiry`）就會關掉連線，也沒有預熱；所以要把 keepalive 調長，並由 adapter 定義一個輕量請求（例如 server time）定期送出。recon、槓桿查詢、backfill 和走 HTTP 的下單都共用這個連線池。
  - **交易層：** 有 TdIntent（refcount > 0）時才啟動：連私有 websocket、recon、OMS / ledger 上線、訂閱 `td.order.{api_id}`，之後 TdReady 才成立。最後一個 intent 消失就立刻關掉交易層，不 linger；常駐層不受影響。F11 的 `restarting` 期間 intent 不會被回收（R4），所以策略重啟不會讓交易層抖動。
  - 下單依 venue 的設計走 HTTP 連線池或 websocket（例如 OKX 走 REST，Bybit 走 `/v5/trade`）。
- **backfill（F35）：** 排程或 detach 觸發的一次性 backfill request，由帳號 worker 用常駐的連線池處理，不另開 job worker，帳號沒有 session 時也能做。同一個帳號同一時間最多一個 backfill，併發受限，不佔滿連線池。
- **直接服務** `td.order.{api_id}`、`td.oms.*`、`td.ledger.*`，並發佈 `td.{api_id}.global`。下單路徑不經過 controller。
- **`td.order.cancel_session(session_id)`**（F10）：撤掉 OMS 裡所有 `client_order_id` 的 session 欄位等於該 session 的掛單。還在 `PENDING_NEW` 或 `UNKNOWN` 的單，等 `chase_unknown` 收斂後一併處理。全部確認後才回覆成功，逾時則回覆未確認的清單。這是 STS crash 時的平台清場（§5.2），也可以當作人工的 kill switch。
- **at-most-one（F36）：** 帳號 worker 會重啟，新舊 incarnation 不能同時收單。不用 DB lease：
  - **同一個 instance 內：** Supervisor 確認舊 worker 的 PID 已經消失，才啟動新的 incarnation。開了 `oci_host_pid` 之後 controller 看得到 host 的 `/proc`（§4.5），這是直接觀測，不是猜測。reattach 時也先掃 `/proc`，確認沒有同 id 的 worker 才 spawn。
  - **跨 instance：** `api_id` → instance 是 DB 裡的靜態綁定（`ApiRepository.instance_name`），TD controller 只啟動自己名下的帳號。同名 instance 重複啟動，由現有的 `refuse_if_serving` 拒絕。
  - **不用 DB lease 的理由：** 兩個 site 共用同一個 `DATABASE_URL`。DB lease 會讓 TW 的交易依賴跨 site 的 DB 連線，DB 或 Tailscale 一斷就停止交易；現行代碼也刻意沒有這種依賴。
  - STS session 不需要這個機制：`session_id` 終生只有一個 incarnation。
- **crash 之後：** 新的 incarnation 用既有的 `reconcile()` 從 venue 重建 ledger 和 OMS，在途的單由 `chase_unknown` 收斂。接著發出 `td.account.reset(incarnation)`。各 session 的 ingress 收到後做平台 recon，收斂後觸發 `on_resync(cause="account_reset")`（F13）；這段期間 `TdReady` 會短暫變成 false。
- **狀態廣播（F14）：** 帳號 worker 在 `td.account.state.{api_id}` 單向廣播 `ready` / `degraded` / `unavailable` 和 incarnation，狀態變化時立即發一次，平時每 2 秒一次（§5.6）。
- **帳本查詢：** `td.account` 服務 `oms.view` / `ledger.view`，並支援 `settled=True`：有狀態 UNKNOWN 的單時，等它們收斂（或逾時）才回覆，沿用現在 `_handle_recon` 的等待邏輯。
- **cancel-on-disconnect（F37）：**
  - 預設關閉，逐帳號開啟。設定掛在帳號上，不放在 strategy.yml，因為帳號 worker 是多個 session 共用的。
  - 語意是「TD worker 的死人開關」，不是「socket 斷線就撤」。只用倒數計時型機制（Binance UM/CM 以 symbol 為單位、Bitget UTA、OKX、Gate）：交易層啟用且有掛單時，帳號 worker 定期刷新倒數；進程死掉或卡住、刷新停止，交易所才撤單。一般的重連不會觸發。
  - 不用 Deribit 的 COD（每次重連都會撤單），也不用 Bybit 的 DCP（只開放給機構客戶，需另外申請）。
  - 計畫內的換版（F27）在 drain-replace 之前先延長倒數，新 incarnation 接手後再恢復。
  - 被交易所撤掉的單，照常以 `on_order_update(cancelled)` 送給策略；帳號重啟時另外有 `on_resync`。不需要新的 hook。
  - 各家的倒數範圍和刷新間隔，在 B6 實測後寫進 adapter。

### 7.2 TD orchestrator

- `desired_accounts` = 本 instance 名下所有啟用的帳號（F35），不再由 intent 決定。
- intent 只決定交易層：controller 以 level-triggered 的方式，把「這個帳號目前有沒有 intent」推給帳號 worker；controller 不在時，worker 維持最後一份（P5）。
- crash 時以退避重啟。這和 STS 不同：TD 的帳號是基礎設施，不是一次性的執行。
- 換版由人工逐帳號觸發 drain-replace（F27），見 §4.6。

---

## 8. API、協定、持久化

### 8.1 Start / End（範圍 2.4）

**Start**

1. 驗證：解析 yml；把帳號名稱解析成 `api_id`；解析 MD instance；以 dry-run 把 feed 解析成 atom，並檢查容量上限。
2. 寫入 `SessionSpec`，`status=pending`、`generation=1`。
3. `td.intent.put(session_id, api_ids)`，冪等。
4. `md.intent.put(session_id, feeds)`，冪等。
5. `sts.session.start(session_id)`；API 回 202 `{session_id, status: "starting"}`，之後的進度看 status 和 conditions（§5.2、F12）。

**End**

1. `sts.session.end(session_id, reason)`：worker 執行 `on_stop` 後退出，狀態進入 terminal。
2. `md.intent.delete`、`td.intent.delete`，冪等。
3. 兜底：MD/TD orchestrator 會依 §8.2 的規則回收 intent。session 自己 `exit` 或 `fail` 時不經過 API，靠的就是這條路徑。

啟動失敗的回滾直接走 End。因此 `deploy_strategy` 的同步流程和它的回滾（`_detach_md`、`_fail_sts`）一起刪除，API 寫死的 10 秒 create timeout（#132 的成因）也隨之消失。驗證步驟（`_sts_target`、`_check_sts_instance`、`_check_md_instances`、`_td_instance`）留給新的 start 重用。

### 8.2 續約（lease）的取捨

**結論：刪除 per-session lease，不保留任何形式的 session 級續約。** 只保留一個由 STS controller 的 Supervisor 發布、以 instance 為單位的存活報告，作為 intent 回收的權威依據。不從報告的缺席推論主機失聯（F32）。TD 帳號 worker 也不用 lease：它的 at-most-one 由 Supervisor 以 PID 確認（§7.1，F36）。

**現在的 lease 提供了什麼，新架構由誰接手：**

| 現在由 lease 提供 | 證據 | 新架構的來源 | 為什麼更好 |
|---|---|---|---|
| STS 死掉時，MD/TD 回收資源 | `LeasedSessionLink`：三次 heartbeat 沒到就 `on_expired` → detach | Supervisor 的 worker 存活報告，加上 intent 的 owner GC | 權威、即時：shim 親眼看到 worker 退出，不必等 3 秒的缺席推論 |
| STS 發現 MD/TD 死掉時自我 fail | `_stale_keys(self._md_acks, grace)` → `_fail_from_infrastructure` | per-feed 的 `last_msg_at` 與序號（資料是否新鮮）、`TdReady`、下單 RPC 的結果 | ack 只證明 MD 進程的 loop 還在轉，**不代表 venue 的資料在流**。socket 靜默斷流時 ack 照樣正常，所以它本來就是錯的代理指標 |
| fencing | heartbeat 帶遞增的 `token`，MD/TD 在 ack 裡回傳 | 結構性保證：一個 `session_id` 終生只有一個 incarnation（不 rebuild）；帳號 worker 由 Supervisor 以 PID 確保唯一（§7.1，F36） | 現在的 token 實際上沒有 fence 任何東西：TD 只把它記在 `link.last_token`，下單路徑從來不檢查 |
| attach 的交握 | MD/TD attach 會等第一個 heartbeat 才算成功 | `*.intent.put` 是同步 RPC，回覆即代表登記完成 | 少一個時間窗 |

**lease 的代價：**

- 把「活性」綁在策略 loop 的「進度」上，這正是 Deribit / ML 長 hook 出事的直接原因。
- 每一跳都有一條 3 秒保險絲：STS→MD、STS→TD，以及兩者的 ack 回程。
- 為了不讓它誤判而長出的補丁：`breathe`、`_heartbeat_overslept`、`_shift_peer_acks`、reaper 的 strike 計數，以及對應的 watchdog 測試。

**取代後的回收規則（MD/TD orchestrator 共用）：**

1. intent 一律帶 `owner = (sts_instance, session_id)`。
2. 每個 STS controller 的 Supervisor 定期發布 `procman.report.sts.{instance}`，內容是目前存活的 worker id 集合加上 generation。controller 滾動的幾秒鐘內報告會暫停，這段期間什麼都不回收（P5）。
3. 某個 owner 在**連續兩份報告**中都不存在 → 回收它的 intent。報告列的是 desired 為 running 的 session（包含 `restarting`，§5.2 R4），所以重新掛起的期間不會被回收。這是權威觀測，幾秒內完成。
4. **報告整個停止時不回收任何東西（F32）。** STS controller 沒有報告時，沒有人能權威地說 session 是否還活著；常見原因是 controller crash loop 或壞版本等待 Strategon 回滾，這時 session 還在跑，回收會切斷它們的行情。整台機器重開時，STS Supervisor 會權威地發現 session 已死並清場；機器永久消失時，由人工執行 `mftik intents gc --instance <name>`（暫定）。代價只是在人處理之前，訂閱和帳號 worker 多留一陣子。回收 intent 本來就不會撤單，交易所上的掛單要靠 D27。
5. 回收錯了也能自癒：主機恢復後，STS controller 的 reconcile 會替每個 running session 重新 `intent.put`。這是 level-triggered 的狀態對帳，不是週期續約，controller 不在線時也不會造成任何東西過期。

### 8.3 協定對照

| 現在 | 之後 |
|---|---|
| `sts.session.create`（同步，等 `on_start` 跑完） | `sts.session.start`（非同步 accept）＋ `sts.session.status` 事件 |
| `md.session.attach`，加上 `sts.md.{sid}` 上的 lease | `md.intent.put` / `md.intent.delete`，帶 owner |
| `td.session.attach`，加上 `sts.td.{sid}` 上的 lease | `td.intent.put` / `td.intent.delete`，帶 owner |
| `md.{session_id}`（per-session fan-out） | `md.a.{venue}.{atom_hash}`（per-atom） |
| `md.subscribe` / `md.unsubscribe` | `md.intent.patch` |
| `STS_LEASE_HEARTBEAT`、`MD_LEASE_ACK`、`TD_LEASE_ACK`、`LeasedSessionLink` | **刪除**。由 `procman.report.{plane}.{instance}` 和 intent owner GC 取代（§8.2） |
| `md.feed.end` | 保留，以 owner 為對象。沒有 gap 相關的協定訊息，也沒有 `on_feed_gap`（F23） |
| —（新增） | `md.universe.{session_id}`：selector 的變更事件，帶 name、added、removed、current、epoch（§6.4） |
| `health.*`、instance subject | 保留 |

依 F1，舊協定不保留相容層，切換時一次換掉。切換之後依 F26：每則訊息帶 `pv`，格式一改就升版，不做版內相容；收到不同 `pv` 的訊息一律以 `protocol_mismatch` 拒絕。

### 8.4 持久化

- **`sts_sessions` 改成 SessionSpec / Status：**
  - 新增 `generation`、`observed_generation`、`worker_incarnation`、`conditions JSON`。
  - `rebuild_count` 改名為 `restart_count`，`restart` 保留並改存新語意（F11）；刪除 `st_facts`（F36）。
- **新增表：**
  - `md_intents(session_id, instance, feeds, atoms, generation, created_at, released_at)`
  - `md_standing_subscriptions`
  - `td_intents(session_id, api_id, created_at, released_at)`
  - selector 狀態：`(spec_hash, universe, epoch, center, updated_at)`（§6.4）
  - `apis` 加上帳號設定欄位，例如 cancel-on-disconnect（F37）
- **intent 兼任歷史（F38）：** session 結束時 intent 列不刪，改記 `released_at`。`md_sessions` / `td_sessions` 從 B10 起停寫，保留唯讀，只用來查切換前的歷史。
- Supervisor 的本機狀態（shim socket、exit 紀錄、`supervisor.json`）放在 `${WORK_DIR}/run/`，不放 DB。

---

## 9. 測試標準（範圍 6）

### 9.1 Tier

| tier | marker | 允許 | 禁止 | 單一測試上限 | 跑在 |
|---|---|---|---|---|---|
| unit | （預設） | 純函數、直接呼叫的 handler、in-memory fake（broker 除外）、`FakeClock` | 網路、子進程、真的 sleep、NATS、DB 檔案 | 50 ms | `just test` |
| component | `component` | 共用連線的真 NATS（F31）、sqlite `:memory:`、`FakeClock`、in-proc 的 procman fake、loopback websocket 上的 venue stub | 每個測試自己連 NATS、子進程、Postgres、wall-clock sleep | 500 ms | `just test` |
| integration | `integration` | 真 NATS、Postgres、真的 Supervisor 和 shim 子進程 | — | 10 s | `just test-int`、CI |
| e2e | `e2e` | compose stack | — | — | release 前 |

**預算（F30）：** 以 GitHub Actions 的 `ubuntu-latest` 為準：`just test`（unit 加 component，`pytest -n auto`）那一步的 wall time 在 120 秒內，不含 `uv sync` 和服務啟動；integration tier 另開 job，不算在內。CI 設閘門：這一步超過 120 秒，或任何一個 unit 測試的 call phase 超過 50 ms，就判定失敗。B0 的基線也在 GitHub Actions 上量。

### 9.2 規則

1. **時間一律注入。** controller、worker、reconciler 都透過 `Clock` 協定（`now`、`monotonic`、`sleep`）取得時間。測試用 `FakeClock.advance()` 推進。在 unit 和 component tier 呼叫 `asyncio.sleep(x > 0)` 會被 conftest 攔下並報錯。
2. **broker：連線和收到之後的行為分開測，不做 fake（F31）。**
   - **不引入 in-memory broker。** 之前的 fake（fakeredis）在 bb005db 被移除，理由是它掩蓋了兩個真實的 bug：trim 的精確度，以及 subscription 不是建立當下就生效。這個理由仍然成立。
   - **連線測試**（component tier，真 NATS）：只有兩類。一是 broker 本身的語意，也就是現有的 `test_nats_transport`、`test_broker*`（no-responders、subscription 何時生效、取消時不遺漏）；二是每種 worker 一個接線 smoke，證明它服務了該服務的 subject。
   - **行為測試**（unit tier，不碰 NATS）：worker 的訊息處理和傳輸分開寫，handler 是「收到解碼後的訊息 → 回覆與副作用」，測試直接呼叫 handler。下單驗證、ledger 預扣、FOK 語意、unknown 追查這類邏輯都屬於這裡。
   - **共用連線：** 每個 xdist worker 只連一次 NATS（session 級 fixture，pytest-asyncio 用 session 級 event loop），每個測試拿到自己 `key_prefix` 的 broker 包裝，teardown 時退訂該 prefix 下的所有訂閱，不關連線。
3. **reconciler 和 generator 以表格驅動的純函數測試：** `reconcile(desired, observed) -> actions`、`evaluate(listing, refs) -> desired`。
4. **worker 在 component tier 以 in-process 方式測**：直接跑 `amain`，procman 用 fake。真的子進程只出現在 integration tier。
5. **策略以 `StrategyHarness` 測。** 這是一個 in-process 的假 session，可以注入事件、斷言送出的單，不依賴任何平面。
6. **DB：** repository 在 component tier 用 sqlite 測；Postgres 方言只在 integration tier（CI）跑。
7. **每個 bug fix 都附上能重現問題的最低 tier 測試。**
8. **每個 tier 都設 `pytest-timeout`。** 測試只能依賴明確的 event 或 future，不能依賴 task 的排程順序。

### 9.3 移除與保留

- **RM 刪除：** 直接依賴三個平面 session 機制，或依賴 `orchestrate` 的測試，566 個。初版清單見附錄 A，依 import 自動分類，B0 之後定稿。
- **策略實作測試（224 個，F16）：** 保留到 B5，再改寫到 `StrategyHarness` 上。B2 到 B5 之間策略代碼不會變，沒有必要提早拿掉這張安全網。這批測試不依賴 NATS，真的 sleep 只有一處（`test_chase` 的 0.2 秒），放在 unit tier 不會威脅預算。7 個檔案裡有 6 個、約 50 行引用了 F9、F10、F13 要刪的 API（rebuild、`remember`、`on_recon_done`、`send_recon`、`breathe`），這部分在 B5 隨 API 一起改寫或刪除。
- **其他測試**（venue adapter、registry、CLI、db、auth 等）保留，但依 B0 量出的耗時重新分 tier。
- **用到 NATS 的測試（靜態計數，參數化展開前）：** 75 個檔案、650 個測試函數。其中 53 個檔案、483 個在 RM 刪除清單裡；剩下 22 個檔案、167 個。這 167 個裡，broker 本身的語意測試 38 個，留作連線測試；其餘 129 個（`test_plane`、`test_backfill_executor`、`test_tape_read`、`test_ledger_view` 等）是透過 NATS 測行為，依 F31 改寫成直接呼叫 handler。§9.3 之前寫的「約 900 個」是錯的。

---

## 10. 文件（範圍 1）

**B1 的做法：**

- `docs/*.md` 全部移到 `docs/archive/`，加上一份索引，記錄每份文件封存的日期和取代它的文件。
- `docs/` 根目錄只保留：
  - `ARCHITECTURE.md`：新建。各決策定案後，從本文萃取出目標架構。
  - `ARCHITECTURE_CHANGE_PLAN.md`：本文。
  - `TESTING.md`：B2 產出。
  - `REFACTOR_TICKETS.md`：本重構的工作票，B10 完成後封存。
  - `Deployment.md`：不封存（F28）。B1 依現況重寫（Strategon plane sets、OCI、每個 site 一台 NATS 加 gateway），B10 再依新架構更新。
- README 改成簡短的指引加 quick start。完整重寫放在 B10。

**分類建議：**

| 類型 | 文件 | 建議 |
|---|---|---|
| 舊模型的設計紀錄 | `JetStreamRemoval`、`RedisRemoval`、`BrokerProvisioning`、`BrokerPatterns`、`MdHandover`、`MdVenueSubscriptions`、`MdExpiry`、`MdOpenInterest`、`StsPause`、`StsSessionList`、`Instances`、`EventLoop`、`Broker` | 封存 |
| 功能設計紀錄 | `Alert`、`Artifact`、`AuditIdentity`、`Auth`、`StrategyEnvironment`、`CLI` | 封存 |
| venue 實測事實 | `Deribit`、`BitgetUta`（Extra verification 表） | 封存（F28） |
| 維運 | `Deployment` | 不封存，B1 依現況重寫，B10 依新架構更新（F28） |

---

## 11. 批次

依 F1、F2，這是一次切換的破壞性版本。**批次是開發里程碑，不是各自的生產部署**：每個批次結束時分支上的測試要全綠、行為要能在 compose 上展示，但不需要和舊版互通，也不需要單獨上線。上線只有一次，就是 B10。

順序的考量：

- **先清場、再定介面（RM、IF）。** 動工前先把要重寫的代碼和它們的測試刪掉，新的抽象層先只定義介面、回傳 null data。這樣重構的範圍、新增的抽象層（§3.4）和每種狀態的權威（§3.3），在寫實作之前就看得見。
- procman 是所有後續工作的前提，所以在介面之後最先實作。
- 既然沒有相容層，就不必「先在舊結構上換協定、再拆進程」。B4 直接做一條**端到端的最小骨架**：新協定、三種 worker、API start/end，只接 paper venue。之後各平面在這個骨架上補齊。
- 骨架完成後，STS、TD、MD 三條線可以並行。

```
B0 ─▶ RM 清場 ─┬─▶ B2 測試 ─┐
               └─▶ IF 介面 ─┴─▶ B3 procman ─▶ B4（骨架）─┬─▶ B5 STS ─────────────────┐
B1（獨立）                                               ├─▶ B6 TD ──────────────────┤
                                                         └─▶ B7 MD atom ─▶ B8 MD 編排 ┴─▶ B9 Selector ─▶ B10 切換
```

每個批次拆成的工作票（描述、範圍、驗收、依賴）見 `docs/REFACTOR_TICKETS.md`。

| 批次 | 目標 | 範圍 | 完成條件 | 依賴決策 |
|---|---|---|---|---|
| **B0 基線** | 量測，凍結現況 | 以 `pytest --durations=0 --junitxml` 跑現行測試（NATS 加 sqlite）；從 `protocol/messages.py` 盤點 subject 和 RPC type；打 tag `arch/baseline` | 每個模組的耗時寫進附錄 C；附錄 A 定稿 | — |
| **B1 文件** | 封存 `docs/` | §10 | `docs/` 根目錄只剩架構文件和依現況重寫的 `Deployment.md` | — |
| **RM 清場** | 刪掉要重寫的代碼和它們的測試 | §5.4、§8.1、§8.2 列出的刪除項，以及附錄 A、B；還有呼叫端需要的地方，留下 IF 的 stub | 清單上的符號在 repo 裡 grep 不到；剩下的測試全綠；各平面能 import、能啟動到「沒有 session 機制」的狀態 | — |
| **IF 介面** | 新抽象層只定義介面，回傳 null data | §3.4 的每一層：型別、函式簽名、寫明不變式的 docstring；附 `xfail(strict=True)` 的契約測試當作之後的驗收 | 每個介面都能 import、`ruff` 通過；契約測試以 xfail 存在；B3 以後的每張票都能指到對應的介面 | — |
| **B2 測試重置** | 建立測試標準 | 附錄 A 的測試已在 RM 隨代碼刪除；寫 `TESTING.md`；加入 `Clock` / `FakeClock`、每個 xdist worker 共用一條 NATS 連線的 fixture、handler 與傳輸分開的規範（F31）、tier marker、`pytest-xdist`、`pytest-timeout`、耗時閘門；`just test` / `just test-int`；CI 拆分 | 剩下的測試在 `just test` 下少於 120 秒；CI 的 integration tier 全綠 | — |
| **B3 procman** | Supervisor、shim；以 strategon#60 為前提 | mftik：`mftik.procman`（以你的 prototype 為底）、shim、Spec / 狀態機 / 重啟策略、reattach、`procman.report.*`（含 worker RSS）、shim 套用 `oom_score_adj` 和 `RLIMIT_DATA`。Strategon 端由 #60 完成：`oci_host_pid`、release GC 檢查 in-use rootfs、agent unit `KillMode=process` | integration：controller 以 detach 結束後 worker 存活，新 controller 能 reattach；殺掉 shim 後 worker 自行 graceful stop；新舊版本的 worker 能並存；在 cp 和 yite 上以 `oci_host_pid` 實際滾動一次；GC 不刪仍在使用的 rootfs；重啟 agent 不影響任何 strategy；`/proc/<pid>/oom_score_adj` 符合 §4.7 的分級 | — |
| **B4 端到端骨架** | 新協定與三種 worker 跑通一條路徑 | 協定 v2（§8.3）：envelope 帶 `pv` 並在不符時拒絕（F26）；intent、owner GC、存活報告，不再有 lease；API start/end（§8.1）；STS session worker 的雙 thread 模型、`on_start` 獨佔、readiness gate；TD 帳號 worker；MD 連線 worker；**只接 paper venue**；平面以純進程執行；三個 orchestrator 的准入控制 | paper 上 deploy → `on_start` → `on_ready` → 下單 → 成交回報 → end 全程走新路徑；三個 controller 各自滾動都不中斷；一個 30 秒 CPU-bound 的 hook 不會讓 session fail、不會讓 NATS 斷線、不會造成假 ack timeout；送單不跨 thread、ack 回程的跨 thread 延遲已量測；no-responders 在跨連線 reply 時的行為已驗證；各 kind 的 RSS 已量測，§4.7 的初始值據此調整；超過預算的 start 以 `capacity_exceeded` 拒絕 | — |
| **B5 STS 補齊** | SDK 功能完整，取消 rebuild；crash 與重新掛起（F10） | artifacts、tape、event log、fetch、timer 搬到新 worker；交付策略（`latest` / `all`）；`offload` / `offload_pool`（§5.5）；hook 時間預算的量測與處理（F15）；`on_md_update` / `on_td_update`（§5.6）；crash 分類、平台清場、`restart` / `max_restarts` / `window`、alert（§5.2）；worker 端所有 DB 存取移除；刪除 §5.4 的清單；`StrategyHarness` 和策略測試改寫 | 所有內建策略在 `StrategyHarness` 上測試全綠；A、B、C 三類 crash 都能清場；`on_failure` 能從 `on_start` 重新掛起，且 R1 到 R4 成立；STS worker 不持有任何 DB 連線；process 模式的 offload 在 stop 時被 terminate、子進程 OOM 時 session 收到 `OffloadWorkerLost` 但不會跟著死；舊的 session 機制代碼全部刪除 | — |
| **B6 TD 補齊** | 所有 venue 的帳號 worker | 各 venue 的帳號 worker：常駐層（溫熱的 HTTP 連線池、backfill）與隨 intent 開關的交易層（F35）；私有連線、OMS、ledger、recon；`td.order.cancel_session`；Supervisor 的 PID fence（F36）；drain-replace；cancel-on-disconnect 的倒數刷新（F37） | 殺掉帳號 worker 後能重啟、recon，`TdReady` 經歷 false 再回到 true；drain-replace 期間沒有遺失或重複的單；交易層隨 intent 開關時，常駐層和連線池不受影響；沒有 session 的帳號也能 backfill | — |
| **B7 MD atom 模型** | 所有 venue 改成 atom | adapter 提供 `atoms_for`、`decode`、`capacity`、`join_policy`；per-atom subject；`TickerStats` 與 STS 端的 join（F19）；tape 改以 `atom_id` 為 key，加錄 `liquidation`（F20） | 所有 venue 現有的 product topic 都改由 atom 提供 | — |
| **B8 MD 編排** | orchestrator 加 reconciler 完整版 | placement（黏性、不遷移）；連線 worker 內的 reconciler；以 listing 驅動到期；常駐訂閱；`tape_keeper` 退役；連線 worker 的人工原地重啟與列出舊版 worker 的指令（F24） | 滾動 MD controller 時，連線、行情與 tape 都不中斷；原地重啟連線 worker 造成的 tape 空洞都有量測紀錄 | — |
| **B9 Selector** | ATM 期權鏈、轉倉 | §6.4：`option_chain`、`rolling_future` 的 `evaluate`、防抖動、`prev` 持久化、部署時的容量上限、相同規格共用；`md.universe.{session_id}`；SDK 的 `on_universe_change`、`self.md.universe`、`self.md.current` | 期權鏈在 ref 移動時依防抖動規則重新置中；轉倉在 `roll_before` 切換 current，舊合約保留到到期；controller 重啟後 universe 和 epoch 不變；I-SEL1 成立 | — |
| **B10 切換** | 唯一一次上線 | 先升級 agent（含 S-1 到 S-3，這一步本身會殺掉所有 strategy，所以必須在停掉所有策略之後做），再讓 plane sets 開啟 `oci_host_pid: true`；strategon#61 若已上線，依 §4.7 重估並設定每個平面的 `memoryBytes`；DB migration（含 drop 舊欄位）；**preflight：任何 `sts_sessions` 仍是 live 狀態就拒絕套用**；runbook；README、`ARCHITECTURE.md` 定稿；前端狀態頁 | 依 runbook 在空的平面上完成切換，舊版可以回滾到 `arch/baseline` | D21 |

---

## 12. 待釐清

**A. 部署與進程管理（卡住 B3、B4）**

v0.1 的 D2（worker 代碼版本）和 D3（procd 粒度）已經由 F6 解決，不再單獨列出。

- **D1** 已定案（F6）。Strategon 的改動追蹤於 strategon#60。
- **D4** 已定案（F7）。cgroup 上限追蹤於 strategon#61，不是本重構的前提。
- **D5** 已定案（F29）。
- **D26** 已定案（F32）。
- **D29** 已定案（F26）。

**B. STS**

- **D6** 已定案（F10、F11）。
- **D7** 已定案（F12）。
- **D8** 已定案（F15）。
- **D9** 已定案（F14）。
- **D10** 已定案（F16）。
- **D25** 已定案（F8、F9）。
- **D28** 已定案（F25）。

**C. MD**

- **D11** 已定案（F17）。
- **D12** 已定案（F18）。
- **D13** 已定案（F21）。
- **D14** 已定案（F19）。
- **D15** 已定案（F24）。
- **D16** 已定案（F20）。
- **D17** 已定案（F33）。

**D. TD**

- **D18** 已定案（F34）。
- **D19** 已定案（F35）。
- **D30** 已定案（F27）。
- **D27** 已定案（F37）。
- **D20** 已定案（F36）。

**E. API 與資料**

- **D21** 已定案（F38）。

**F. 文件與測試**

- **D22** 已定案（F28）。
- **D23** 已定案（F30）。
- **D24** 已定案（F31）。

---

## 附錄 A：RM 測試刪除清單（初版，依 import 自動分類，B0 後定稿）

**`apps/sts/tests`（233）**：
`test_attach_refused`、`test_boot_schema_guard`、`test_detach_is_not_awaited`、`test_detach_refcount`、`test_environment_rebuild`、`test_eventlog`、`test_md_ack_watchdog`、`test_md_events`、`test_mds_query`、`test_oms_wait_cids`、`test_orphan_reaper`、`test_private_events`、`test_rebuild`、`test_recon_oms`、`test_rpc_loop_survives`、`test_session_control_addressing`、`test_session_failed`、`test_session_processes`、`test_status_events`、`test_stop_ordering`、`test_strategy_lifecycle`、`test_sts_cid`、`test_sts_incompatible_environment`、`test_sts_runtime_env`、`test_sts_session`、`test_td_ack_watchdog`

**`apps/md/tests`（125）**：
`test_md_detach_disconnect`、`test_md_expiry`、`test_md_feed_end`、`test_md_fetch`、`test_md_lease_resilience`、`test_md_orphan_reaper`、`test_md_session`、`test_md_shared_venue_topics`、`test_md_tape`、`test_md_two_instances`、`test_md_venue_factory`、`test_md_venue_feeds`

**`apps/td/tests`（139）**：
`test_account_ownership`、`test_backfill_triggers`、`test_cid_ownership`、`test_connector_capabilities`、`test_detach_rpc`、`test_history_wiring`、`test_lease_resilience`、`test_leverage_rpc`、`test_order_rpc`、`test_recon_snapshot`、`test_session_create`、`test_session_leverage`、`test_session_oms`、`test_stream_rejects`、`test_td_orphan_reaper`、`test_venue_factory`

**`apps/api/tests`（69）**：
`test_deploy_refused`、`test_environment_flow`、`test_md_instance_deploy`、`test_orchestrate_log_type`、`test_registry_add`、`test_td_instance_routing`、`test_td_sessions_route`

**`packages/common/tests`**：
`test_plane_serves_its_subject`。`test_wire_ledger`、`test_last_reader_release` 和 broker lease 相關的測試，隨 B4（lease）、B7（wire ledger）刪除代碼時一起刪。

**策略實作（224，F16：RM 不刪，B5 改寫）**：
`test_chase`、`test_cross_arb`、`test_macd_dollar`、`test_noop_strategy`、`test_oco`、`test_tape_keeper`、`test_twap`

## 附錄 B：預計刪除的主要代碼（初估）

| 檔案 | 現在 | 去向 |
|---|---|---|
| `apps/sts/src/mftik_sts/session/manager.py` | 2,418 行 | 拆成 controller 的 orchestrator（預估少於 600 行）和 worker 端；rebuild、reaper、雙模式全部刪除 |
| `apps/sts/src/mftik_sts/spawn.py`、`worker.py` | 551 行 | 由 procman 取代；worker 只剩 session 執行 |
| `apps/md/src/mftik_md/session/*` | 約 2,200 行 | 由 orchestrator、連線 worker、reconciler 重寫 |
| `apps/td/src/mftik_td/session/manager.py` | 1,711 行 | 拆成 TD orchestrator 和帳號 worker；lease、reaper 刪除 |
| `apps/api/src/mftik_api/orchestrate.py` | 538 行 | 只剩 start/end，預估少於 250 行 |
| `packages/common/src/mftik/exchange/wire.py` 和各 adapter socket 的訂閱管理 | — | 搬進連線 worker 的 reconciler |

## 附錄 C：B0 量測結果

（待填）
