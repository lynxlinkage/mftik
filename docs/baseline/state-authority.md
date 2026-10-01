# state-authority — 現況的狀態權威表（as-is）（B0-04、issue #157）

> **基準：** `main` @ `a0cbfb2`（`ARCHITECTURE_CHANGE_PLAN.md` 的基準 commit）。B0-01 的 `arch/baseline` tag 還沒打，所以本文一律以 commit hash 稱呼基準。
>
> `refactor/process-planes` 相對 `a0cbfb2` 只多了文件檔，`apps/` 和 `packages/` 完全沒有差異，所以本文引用的行號在兩個 ref 上都成立。
>
> 「§」指 `ARCHITECTURE_CHANGE_PLAN.md` 的章節，「F」指同一份文件的決策編號。協定層的對照見 `docs/baseline/protocol.md`（B0-03）。

## 1. 盤點方法與用詞

**表的形狀和 §3.3 一樣**，六節（控制面、進程層、MD、TD、STS session、版本）、同樣的列、同樣的四個欄位，另加第五欄「和 §3.3 的差異與負責的票」。這樣兩張表可以並排著讀。

- **列的順序和 §3.3 完全相同**，連現況不存在的狀態也保留成一列，權威那格寫「不存在」。
- **差異欄寫「—」** 表示這一列和 §3.3 的目標一致，不需要任何票。
- **票號**用 `REFACTOR_TICKETS.md` 的編號加 issue 號，例如 RM-04（#167）。只列真的會改這一列的票，不把整個批次抄上來。沒有票負責的差異，在 §12 單獨列出，不編一張不存在的票。

**「權威」怎麼認定。** 以**哪個 OS 進程執行寫入**為準，不是哪個函式。這是這份盤點的重點：現況有好幾種狀態是兩個進程寫同一份資料，而 §3.3 的前提是「每一種狀態只有一個權威」。所以 STS 平面進程和 STS session worker 一律分開算，即使它們共用同一份 `SessionManager` 代碼。

**三個進程名稱**在本文固定這樣用：

| 名稱 | 是什麼 | 入口 |
|---|---|---|
| STS 平面進程 | Strategon assignment 跑的那個 `sts` 進程，持有 worker 的 `WorkerSlot` | `apps/sts/src/mftik_sts/app.py:amain` |
| STS session worker | 一個 session 一個 OS 進程，策略真正跑的地方 | `python -m mftik_sts.worker <sid> create\|rebuild`（`apps/sts/src/mftik_sts/spawn.py:214`–`:218`） |
| MD / TD 平面進程 | 單一進程，session 是它裡面的物件 | `apps/md/src/mftik_md/app.py:amain`、`apps/td/src/mftik_td/app.py:amain` |

**生產部署上這些進程落在哪台機器、env 從哪來、secret 在哪**，見 `docs/Deployment.md`（B1-02 已依現況重寫）。本文只在狀態權威會因此不同的地方引用它（§10）。

## 2. 控制面（宣告）

| 狀態 | 權威（唯一寫入者） | 存放 | 讀取者 | 重啟或失聯後怎麼收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| session spec：策略、參數、`restart`、timeout | **STS 平面進程**，不是 API。`session/manager.py:SessionManager._create_via_worker` 在 spawn 之前寫 row（`manager.py:588`）。worker 也呼叫同一個函式，但 `apps/sts/src/mftik_sts/db.py:persist_live_session` 先 SELECT，已存在就原樣回傳（`db.py:31`–`:33`），所以插入的是平面。API 只 mint id：`orchestrate.py:mint_session_id` 只 SELECT 找沒用過的六位 hex（`orchestrate.py:60`–`:67`），然後用 RPC 叫 STS create（`orchestrate.py:114`–`:136`） | Postgres `sts_sessions` 的 `created_by`、`type`、`yaml_text`、`td`、`md_ids`、`st_paras`、`restart`、`instance`（`packages/db/src/mftik_db/models/session.py:74`–`:156`）。**沒有任何 timeout 欄位**——deploy 的時間預算寫死在 API（create RPC 10 秒，`orchestrate.py:135`），`restart` 只有 `always` 一個有效語意（`protocol/strategy_yml.py:RESTART_ALWAYS`） | STS 平面（rebuild 掃描、reaper、placement）、STS session worker、API（list、board、stats、registry、ws、alerts） | DB 本身就是權威，不需要重算 | §3.3 的權威是 API，現況是 STS 平面 → RM-08（#171）、IF-13（#191）、IF-14（#192）。timeout 不在 spec 裡 → IF-07（#185）加 `start_timeout_s` / `ready_timeout_s`，IF-14 落到 DB |
| session status：phase、conditions、incarnation、`restart_count`、失敗原因 | **三個進程都寫。** STS 平面：`manager.py:801`（start 失敗→`failed`）、`:877`（worker 非 0 退出→`interrupted`）、`:1202`（force-stop 殺掉→`failed`）、`:1396`（`reap_orphans`→`interrupted`）、`:2197`（shutdown 預先寫 `interrupted`）。STS session worker：`manager.py:1297`（`close` 寫終態）、`:1942`（rebuild 的 `mark_live`）、`:2308`（worker 自己 shutdown 寫 `interrupted`），接線在 `worker.py:246`–`:251`。API：只寫 `ack`，`routes/sts.py:422` → `repositories/session.py:323`（`mark_ack`，只接受 `failed` / `interrupted`） | 一個字串欄位 `sts_sessions.status`（`models/session.py:87`）加 `reason`、`finished_at`、`rebuild_count`。**沒有** phase、conditions、incarnation、`observed_generation`。即時版發在**全域**的 `status.sts`（`manager.py:420`、`protocol/topics.py:144`），不是 per-session 的 subject | API（board、stats、`/ws/status/sts`）、前端、STS 自己（rebuild 掃描） | 沒有 reattach。平面重啟時雙方各寫一次 `interrupted`（§9.1），開機時 `rebuild_interrupted()` 把 `interrupted` 的 row 從 `on_start` 重跑（`manager.py:1423`）；留在 `live` 的孤兒由 `reap_orphans` 連兩輪改成 `interrupted`（`manager.py:1351`，`_ORPHAN_STRIKES = 2`，`:169`） | 多寫入者；欄位缺 phase / conditions / incarnation；subject 是全域不是 per-session；恢復是 rebuild 不是 reattach → RM-01（#164）、RM-04（#167）、IF-04（#182）、IF-14（#192）、B4-02（#202）、B5-09（#218）。`interrupted` 這個值本身的讀取端清理在 B10-01（#249）與 B10-03（#251） |
| MD intent：session 要哪些 feed 和 selector | **MD 平面進程**（`apps/md/src/mftik_md/session/manager.py:SessionManager.attach`，`manager.py:228`）。觸發來源有兩個：API deploy（`orchestrate.py:199`）、STS rebuild（`apps/sts/.../session/manager.py:_attach_md`，`:1992`）。執行期間的 `md.subscribe` 有 handler（`manager.py:1443`）但**沒有生產發送者**（見 `docs/baseline/protocol.md` 4.3） | MD 進程記憶體：`SessionManager._links[session_id].subscriptions`（`manager.py:168`）與 `Dispatcher._subs`（`dispatcher.py:38`）。Postgres `md_sessions` 只記 `(instance, session_id, venues)`，寫了之後沒有人讀回去重建（`manager.py:544`–`:547` 的註解明說） | `Dispatcher.publish`（`dispatcher.py:115`）、`VenueSession` 的 pump | 沒有 level-triggered 的 desired。記憶體隨進程消失，而且在 MD 重啟的情形下 session 會先被 STS 判死（§9.4） | 完全沒有 intent 這一層，也沒有 selector → RM-05（#168）、IF-01（#179）、IF-09（#187）、IF-13（#191）、IF-14（#192）、B4-07（#207）。`md_sessions` 停寫在 B10-01（#249） |
| TD intent：session 用哪些帳號 | **TD 平面進程**（`apps/td/src/mftik_td/session/manager.py:SessionManager.attach`，`manager.py:217`；`detach` 在 `:517`）。觸發來源同樣是 API deploy（`orchestrate.py:244`）與 STS rebuild（`_attach_td`，`:2068`） | TD 進程記憶體：`TradingAccount.links`（`manager.py:132`），refcount 就是 `len(links)`（`:158`）。Postgres `td_sessions` 一列一個 `(session_id, api_id)`，同樣只寫不讀回 | `TradingAccount.refcount` 決定交易層存不存在；`detach` 到 0 時 `_destroy_account` | 同上，記憶體隨進程消失（§9.5） | 沒有 intent，是 refcount → RM-06（#169）、IF-01（#179）、IF-12（#190）、IF-13（#191）、IF-14（#192）、B4-07（#207）。停寫在 B10-01（#249） |
| 常駐訂閱 | **不存在。** 等效做法是跑一個 `tape_keeper` 策略 session，用它的 attach 把 feed 的 refcount 撐住；模組 docstring 自己就說「這是那個 somebody」（`apps/sts/src/mftik_sts/impl/tape_keeper.py:1`–`:10`） | 那個 session 自己的 `sts_sessions` row。沒有 `md_standing_subscriptions` 表 | 和一般 session 的路徑完全相同 | 和一般 session 一樣：STS 重啟後靠 rebuild 回來（`TapeKeeper.rebuildable = True`，`tape_keeper.py:48`） | 目標是設定檔加一張表 → IF-14（#192）建表、B8-05（#242）讓 `tape_keeper` 退役 |
| `api_id` → instance 綁定、帳號設定 | **API**（`apps/api/src/mftik_api/routes/apis.py:create_api`，`:89`；刪除 `:253`） | Postgres `apis`，`instance_id` 是 NOT NULL（`packages/db/src/mftik_db/models/api.py:33`–`:75`）。**沒有任何帳號設定欄位**（cancel-on-disconnect 之類） | TD（`apps/td/src/mftik_td/db.py:instance_name`）、STS（`apps/sts/src/mftik_sts/db.py:td_instance`，docstring 說明為什麼不把它抄進 session document）、API（`orchestrate.py:_td_instance`）；repository 在 `packages/db/src/mftik_db/repositories/api.py:38` | DB 本身就是權威 | 綁定這件事**一致**。帳號設定欄位不存在 → IF-14（#192）加欄位、B6-07（#225）使用它 |
| listing：合約、到期、strike | **SYM 平面**（`apps/sym/src/mftik_sym/plane.py:SymbolPlane.refresh`，`:63`；`refresh_loop` 在 `:145`） | Postgres `symbol_ticker`、`symbol_filter`（`packages/db/src/mftik_db/models/symbol.py:70`、`:137`） | MD **不直接查表**，走 `SymbolClient` 的 broker RPC（`apps/md/.../session/manager.py:_resolve_expiry`，`:977` → `packages/common/src/mftik/symbols/client.py:SymbolClient.get`）；TD、STS 也一樣 | 每 `SYM_REFRESH_INTERVAL` 秒重拉一次，預設 3600（`apps/sym/src/mftik_sym/app.py:34`、`:39`） | **—**（§3.3 寫的「每小時刷新」成立。讀取走 RPC 而不是直接查表，不改變權威） |

## 3. 進程層

現況只有 STS 有「worker 進程」這件事；MD 和 TD 的執行單位是平面進程裡的物件，所以這一節的三列對 MD / TD 一律不適用。**沒有 shim**：worker 的父進程就是 STS 平面進程本身。

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| worker 是否存在、exit code、signal | **STS 平面進程**，靠自己是父進程看到。`SubprocessSpawner.spawn` 用 `asyncio.create_subprocess_exec` 起 worker（`spawn.py:214`–`:226`），`_on_worker_exit` 以 `process.wait()` 收 exit code（`manager.py:840`） | 平面進程記憶體：`_workers: dict[str, WorkerSlot]`（`manager.py:311`，`WorkerSlot` 在 `spawn.py:50`）。exit code 不落地任何地方 | 只有 `SessionManager` 自己 | **收不回來。** 平面一消失，`_workers` 和未讀的 exit code 一起沒了；剩下的只有 DB row 的狀態，由 `reap_orphans` 事後推論（§9.2） | 目標是 shim 親眼看到並寫 `<id>.exit.json`，controller 重啟時 reattach 讀回 → IF-03（#181）、B3-01（#194）、B3-03（#196）。另外 §4.1 明說新的 shim **不可以**用 `asyncio.create_subprocess_exec`（transport 關閉時會殺 child），現況的 worker 正是用它起的 |
| 每個 instance 存活中的 worker 集合 | **不存在**對應的權威回報。三個平面各自用「DB 掃描加兩次 strike」反推：STS `reap_orphans`（`manager.py:1351`）、MD `reap_orphans`（`apps/md/.../session/manager.py:464`）、TD `reap_orphans`（`apps/td/.../session/manager.py:409`），都是每 60 秒一輪（`apps/sts/.../app.py:171`、`apps/md/.../app.py:106`、`apps/td/.../app.py:103`）。另外有一條 `sys.heartbeat` 的平面存活廣播，但**沒有任何訂閱者**（見 `docs/baseline/protocol.md` 4.2） | 不落地。STS 另外有一條 beat pipe：worker 每個 lease interval 寫一個 byte，平面讀它判斷 worker 的 loop 是不是卡住（`spawn.py:36`–`:40`、`:113`–`:128`，門檻 `BEAT_SILENCE_S = 3` 秒，`manager.py:72`） | 各平面自己的 reaper | 新進程重新開始掃，連兩輪沒看到就把 row 收掉 | 目標是 Supervisor 發 `procman.report.{plane}.{instance}` → IF-03（#181）、B3-04（#197）、B4-07（#207）。「不以訊號缺席推論狀態」（P7、F32）現在正好相反：三個 reaper 都是這樣推論的 |
| worker 的代碼版本 | **不存在。** worker 一定和平面同一份代碼，因為 worker 是平面 `exec` 出來的（`spawn.py:214` 用 `sys.executable` 加 `-m mftik_sts.worker`），而平面換版等於整個 assignment 重啟，所有 worker 跟著重啟 | — | — | — | 目標是 `WorkerSpec.code_ref` 加 `supervisor.json`，並能列出跑在舊版代碼上的 worker → B3-07（#200）、IF-15（#193）、B8-06（#243） |

## 4. MD

現況沒有 atom 這個單位，也沒有連線 worker。管理單位是 `FeedKey = (topic, UniversalTicker)`（`apps/md/src/mftik_md/session/dispatcher.py:18`–`:20`），一個 venue 一條公用 socket（`session/venue.py:VenueSession`，`:136`）。

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| 每條連線的 desired atom 與 generation | **MD 平面進程**，但單位是 feed 的 refcount 而不是 desired 清單，而且**沒有 generation**。`Dispatcher._subs: dict[FeedKey, set[session_id]]`（`dispatcher.py:38`）；refcount 由 0 變 1 時 `VenueSession.ensure_feed` 開 pump（`venue.py:176`），掉回 0 時 `_stop_feed_if_unused` 收（`manager.py:1367`） | MD 進程記憶體。不落地，也沒有可以重算的來源（intent 不存在） | `VenueSession` 的 pump、`Dispatcher.publish` | 進程重啟後歸零，而且沒有任何東西會重新推導（§9.4） | 沒有 atom、沒有 desired 清單、沒有 `generation`、沒有一條連線一個進程 → RM-05（#168）、IF-08（#186）、IF-09（#187）、IF-10（#188）、B7-01（#227）、B8-01（#238）、B8-02（#239）、B8-03（#240） |
| selector 的 universe、epoch、置中狀態 | **不存在。** 訂閱清單在 attach 時由 `strategy.yml` 的 feed 列表一次決定（`manager.py:attach`，`:228`），不從宣告推導動態集合 | — | — | — | 整件事是新增 → IF-09（#187）、IF-14（#192，狀態表）、B9-01 到 B9-04（#245–#248） |
| 連線上實際訂閱成功的 atom（observed） | **分兩層，都不是 MD 的 session 層。** wire 層：每條 socket 一個 `WireLedger`，`_held` 是已經 SUBSCRIBE 並拿到 ack 的 key（`packages/common/src/mftik/exchange/wire.py:178`–`:193`）。MD 層：`VenueSession._feeds` 是真的在跑的 pump（`venue.py:151`、`:176`）。MD 刻意不碰 venue 詞彙，所以它看不到 wire 層的 key | 都在 MD 進程記憶體；`WireLedger` 在 venue connector 裡面 | socket 自己（`_restore`、`resync_channel`）；MD 只看 `_feeds` | 斷線重連時 `WireLedger.clear()` 把 `_held` 清空並 bump `_generation`（`wire.py:198`–`:223`），然後 adapter 的 `_restore` 從自己的 `_subs` 重放 SUBSCRIBE（例：`packages/common/src/mftik/exchange/binance/feed.py:_restore`、`bybit/feed.py:_restore`、`okx/feed.py:_restore`）。**重放的來源是 adapter 的讀者清單，不是 MD 的 refcount** | 目標是連線 worker 裡的純函數 reconciler，以交易所 ack 為準並比對 desired → IF-10（#188）、B8-03（#240）；`wire.py` 整份搬進 reconciler（附錄 B） |
| 行情內容，包括 fold 後的 book | **venue adapter 的 socket**，不是 MD 的 session 層。book 的 fold 狀態在 adapter 裡：`BybitBook` 存在 `BybitPublicStream._books`（`packages/common/src/mftik/exchange/bybit/feed.py:154`–`:204`）、OKX 是 `OkxBook`（`okx/feed.py:213`）。`VenueSession._pump` 只把 connector 吐出來的平台 model 包成 envelope 轉出去（`venue.py:289`–`:293`） | adapter 記憶體 → `md.{session_id}`（per-session fan-out，`dispatcher.py:129`） | STS session worker 的 `_pump_md_session` | 缺口時單一 channel resync（`bybit/feed.py:_resync`，`:562`）；重連時每個 topic 換一個新的 book 物件（`bybit/feed.py:627`–`:628`） | 權威的位置其實和 F21 想要的一致（解碼與 fold 在最靠近 socket 的那一層），差別是現在那一層在 MD 進程裡而不是獨立的連線 worker，而且 subject 是 per-session 不是 per-atom → RM-05（#168）、IF-08（#186）、IF-10（#188）、B7-02a–g（#228–#234） |
| feed 狀態 live / down | **兩個互不相同的機制，都不是「連線 worker 廣播」。** 單一 feed 結束：`SessionManager._emit_feed_end` 發 `md.feed.end` 到 `md.{session_id}`（`manager.py:1301`），STS 轉成 `Strategy.on_feed_end`。整個 MD instance 失聯：靠 lease ack 的缺席——MD 每個 heartbeat 回一次 `md.lease.ack`（`manager.py:1436` 的 `_lease_loop`），STS 在 `heartbeat_interval * PEER_MISS_LIMIT` = 3 秒內沒看到就把 **session 判 `failed`**（`apps/sts/.../session/session.py:785`–`:799` → `_fail_from_infrastructure`，`:479`） | `md.{session_id}` 上的訊息；STS 端的 `_md_acks` 時鐘（`session.py:271`） | STS session worker | 重新 attach 後第一次收到 ack 才重新 arm（`session.py:918`） | §3.3 是「只通知，不回收」（F14、P7）；現況是**直接讓 session 死**，而且 `failed` 不是 rebuild 的候選，所以沒有自動復原 → RM-02（#165）、IF-06（#184）、B5-05（#214）、B8-06（#243） |
| per-atom `seq` | **不存在。** `Envelope` 的欄位只有 `id`、`type`、`source`、`session_id`、`reply_to`、`ts`、`payload`（`packages/common/src/mftik/protocol/envelope.py:25`–`:36`），沒有 `seq`。策略無法偵測漏收 | — | — | — | 新增 → IF-01（#179）、IF-05（#183）、IF-10（#188）、B5-01（#210） |
| tape 與 coverage | **MD 平面進程**，接在 fan-out 之後：`Dispatcher.publish` → `TapeRecorder.append`（`dispatcher.py:144`–`:145`、`apps/md/src/mftik_md/tape.py:100`）。coverage 的兩個邊界由 feed 的第一個 / 最後一個訂閱者觸發：`_open_first` → `TapeRecorder.started`（`manager.py:800`、呼叫在 `:896`–`:897`、`tape.py:126`）、`_stamp_stopped` → `stopped`（`manager.py:1294`、呼叫在 `:1298`–`:1299`、`tape.py:142`） | Redis，每個 site 一台：`tape:{feed}` 是 stream（`tape_store.py:65`、`:103`），`tapecov:{feed}` 是 hash，欄位有 `continuous_since_ms`、`recording`、`stopped_ms`、`gaps`，`gaps` 最多 32 段（`tape_store.py:25`、`:69`、`:181`）。key 是 `topic.UniversalTicker` 的 feed key，不是 atom。錄哪些 topic 由 `MD_TAPE_TOPICS` 決定，代碼預設 `("aggtrade", "trade")`（`tape.py:41`、`app.py:150`–`:157`）；`REDIS_URL` 沒設就整個關掉錄製（`app.py:171`–`:177`） | **STS 不開 Redis**（`tape_store.py:5`–`:6` 明說），策略一律經 `md.tape.tail` RPC（`packages/common/src/mftik/strategy/tape.py:473` → `apps/md/src/mftik_md/rpc/tape.py:43`） | Redis 的耐久性是 `appendonly yes` 加 `appendfsync everysec`（`deployment/redis/redis.conf:22`–`:23`），所以 Redis 自己重啟最多掉約一秒的 print；滿了是 `noeviction`，拒絕新 print 而不是刪舊 key（`redis.conf:37`–`:38`）。MD 重啟的洞：乾淨停止有 `stopped_ms`，下一次 `mark_recording` 把洞長度寫進 `gaps`；被強制殺掉沒有 `stopped_ms`，就只能把 `continuous_since_ms` 重設，洞的長度量不出來（`tape_store.py:181`–`:220`） | 權威的位置一致（持有 feed 的那個進程寫），差別是 key 不是 `atom_id`、沒有錄 `liquidation`、而且生產上只錄 `aggtrade` → B7-04（#236）。F20 要的 `trade`、`aggtrade`、`liquidation` 三種，現在代碼預設兩種、生產一種 |

## 5. TD

現況一個 `api_id` 一個 `Session` 物件（`apps/td/src/mftik_td/session/session.py:Session`），全部住在同一個 TD 平面進程裡；沒有帳號 worker，也沒有「常駐層 / 交易層」的分界——`Session` 在第一次 attach 時整個建起來，refcount 歸零時整個銷毀。

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| 交易所上的掛單、部位、餘額（最終真相） | **交易所** | — | TD 的 `reconcile()`（`session.py:413`）：`fetch_open_orders` + `fetch_balances` +（有的話）`fetch_positions` | — | **—** |
| OMS、ledger（預扣、available） | **TD 平面進程**裡那個 `api_id` 的 `Session`。`self.oms` 在 `session.py:160`（類別 `apps/td/src/mftik_td/oms/oms.py:31`），`self.ledger` 在 `session.py:166`（類別 `oms/ledger.py:42`） | 只在記憶體。`publish_oms` 和 `write_ledger` 都是空實作（`session.py:503`–`:505`、`:1139`–`:1141`），所以**沒有任何快照落地**。ledger 的內容是 `_venue`（交易所回報的 free）、`_prelocked`（每個資產的預扣總額）、`_by_cid`（哪一張 cid 扣了多少），`available()` 是 free 減 prelock（`oms/ledger.py:53`–`:73`） | 策略經 `td.account.{api_id}` 的 `td.oms.view` / `td.ledger.view` RPC 讀（`packages/common/src/mftik/strategy/oms.py:203`），**SDK 不保留本地鏡像**；帳號事件走 `td.{api_id}.global` | `Session.start()` 一定先跑一次 `reconcile()`（`session.py:327`），私有 socket 重連時再跑一次（`_on_venue_reconnect`，`:401`）。`apply_reconcile` **整批替換** OMS 的三個 dict，只留 `status.is_open()` 的單（`oms/oms.py:67`–`:87`）。預扣**不受 recon 影響**，理由寫在 `session.py:450`–`:453` | 權威的位置和 F13 一致（記憶體），差別是它在平面進程裡而不是帳號 worker 裡，而且**重啟後沒有 `td.account.reset`、策略收不到 `on_resync`**，見 §9.5 → RM-06（#169）、IF-11（#189）、B4-05（#205）、B6-02（#220）、B6-06（#224） |
| 交易層開或關 | **TD 平面進程**，用 refcount 而不是 desired / observed 兩層。`attach` 第一次建 `TradingAccount` 並 `trading.start()`（`manager.py:217`–`:250`），`detach` 到 refcount 0 時 `_destroy_account`（`manager.py:517`、`:628`） | TD 進程記憶體 | `TradingAccount.refcount`（`manager.py:158`） | 沒有 desired 可以 fail-static：進程重啟後 `_accounts` 是空的，要等下一次 attach 才會有帳號（§9.5） | 目標是 controller 推 desired、worker 保留最後一份（P5），而且常駐層對啟用帳號常駐（F35） → RM-06（#169）、IF-11（#189）、IF-12（#190）、B6-01（#219） |
| 帳號狀態 ready / degraded / unavailable | **不存在。** 沒有 `td.account.state.{api_id}` 這個 subject，也沒有 `on_td_update`。最接近的是 `td.global.keepalive`（`manager.py:666`、`:679`），但它**沒有任何 handler**（見 `docs/baseline/protocol.md` 3.7）；策略唯一會察覺 TD 的方式是 `td.lease.ack` 停了，而那條路直接讓 session `failed`（`apps/sts/.../session/session.py:801`–`:816`） | — | — | — | 整件事是新增 → IF-01（#179）、IF-06（#184）、B5-05（#214）、B6-06（#224） |
| 訂單歷史、成交、資金流水 | **TD 平面進程**，兩條路寫同兩張表：live stream 經 `HistoryWriter`（`apps/td/src/mftik_td/history.py:136`，`record_order` 在 `:211`、`record_fill` 在 `:230`），backfill 經 `BackfillExecutor._persist`（`apps/td/src/mftik_td/backfill/executor.py:347`）與 `_persist_fills`（`:364`）。`session_id` 只有送單那一刻寫得進去，理由在 `session.py:507`–`:521` | Postgres `orders`、`fills`、`backfill_cursors`（`packages/db/src/mftik_db/models/history.py:98`、`:158`、`:263`）。**`cash_flows` 有表、有 repository，但整個生產代碼沒有任何寫入路徑**（`repositories/history.py:416` 的 `bulk_insert_ignore` 只有測試在用） | API（board、PnL、`backfill_cron`） | `HistoryWriter` 是有界佇列，滿了丟（`history.py:236`）；正常停止會 drain 加一次 flush（`history.py:185`–`:207`），被強制殺掉就掉在佇列裡，靠 backfill 補（`history.py:185`–`:190` 的註解） | 權威一致。差別只有「資金流水」這一欄在現況是空的 → **沒有票**，見 §12 |

## 6. STS session

session worker 是單一 event loop，沒有 ingress thread 和 strategy thread 的分工（F8 是新增的），所以 §3.3 裡寫「session worker 的 ingress」的那幾列，現況都只是「session worker」。

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| 策略內部狀態 | **STS session worker**，但**有一部分落地**：`Strategy.remember(key, value)` 把字串寫進 `sts_sessions.st_facts`（`packages/common/src/mftik/strategy/base.py:Strategy.remember` → `apps/sts/.../session/session.py:StsSession.remember`，`:432` → `apps/sts/src/mftik_sts/db.py:remember_fact`，`:54` → `packages/db/src/mftik_db/repositories/session.py:remember`，`:295`） | 記憶體，加上 `sts_sessions.st_facts`（`models/session.py:154`）。生產上有 `chase` 在用它記 `started_ms` 和滑價錨定價 | 策略自己；rebuild 時由 `_rebuild_one` 讀回來餵給 `on_rebuild`（`manager.py:1935`–`:1939`） | rebuild 時**不是**從 `on_start` 全新開始：先 `strategy.on_rebuild(st_facts)`，再 `mark_live`、`session.start()`（`manager.py:1938`–`:1951`）。交易所那一側的真相靠 recon 取回，不從 DB | §3.3 和 F10 要求「記憶體，不落地」「重新掛起時從 `on_start` 全新開始」；現況有 `st_facts` 與 `on_rebuild` → RM-01（#164）刪寫入路徑、B10-01（#249）drop 欄位 |
| `client_order_id` 序號 | **STS session worker**。`StrategyOms._next_client_order_id` 第一次用到時才建 `ClientOrderIdFactory`（`packages/common/src/mftik/strategy/oms.py:197`–`:201`），`bind` 會把它設回 `None`（`oms.py:132`–`:136`） | 記憶體。**版位是 `ver4 \| session24 \| ts_sec28 \| seq8`**，packed 成 uint64、上線走十進位字串（`packages/common/src/mftik/strategy/client_order_id.py:1`–`:27`、`pack` 在 `:91`） | TD（`cid_owner`、`Strategy.owns`，`strategy/base.py:298`–`:317`） | **序號每個 incarnation 都從 0 重新開始**（`client_order_id.py:152`–`:153`），沒有任何東西記住上一個 incarnation 用到哪。唯一的防撞是 `ts_sec` 桶：seq 低 8 bit 繞回時把秒數往前推（`client_order_id.py:163`–`:168`）。跨 session 的唯一性來自 session 欄位，不是 seq（docstring `:144`–`:146`） | 權威位置一致。差異在跨 incarnation 的不撞號保證：§5.3 的 R2 靠「重啟 backoff 至少 1 秒」推出新舊兩張單一定落在不同秒，**現況沒有這個 backoff**——`_schedule_rebuild` 收到 worker 非 0 退出就立刻排 rebuild（`manager.py:904`–`:924`）。另外 R2 和 §3.3 寫的版位少了 `ver` 這個 nibble，見 §11 → B4-03（#203） |
| event log | **STS session worker**（不是 ingress，沒有 ingress）。`EventLog.record` 在 worker 的 event loop 上入佇列，`_drain` 是 asyncio task，真正寫檔在 `asyncio.to_thread`（`packages/common/src/mftik/strategy/eventlog.py:207`、`:322`） | 檔案 `{STS_EVENTLOG_DIR}/{session_id}.jsonl`（`eventlog.py:55`、`:143`）。佇列大小 `STS_EVENTLOG_QUEUE`、輪替 `STS_EVENTLOG_MAX_BYTES` / `STS_EVENTLOG_BACKUPS`（`eventlog.py:58`、`:61`–`:62`）。沒設 `STS_EVENTLOG_DIR` 就整個關掉 | 事後分析；API 經 `sts.eventlog.info` / `sts.eventlog.read` 向平面要（`apps/sts/src/mftik_sts/rpc/eventlog.py`） | 檔案留在 volume 上，新進程**append 同一個檔**，但 `_seq` 從 0 重新起算（`eventlog.py:152`），所以同一個檔裡會出現重複的 seq | 權威一致，差別是沒有 ingress thread、也沒有 `delivered` / `superseded` / `dropped` 的交付標記 → IF-05（#183）、B5-01（#210）、B5-02（#211） |
| artifacts | **兩個進程都寫。** 策略在 session worker 裡直接寫磁碟（`packages/common/src/mftik/strategy/artifacts.py:675`–`:742` 的 `StrategyArtifacts`）；operator / API 上傳走平面的 RPC（`apps/sts/src/mftik_sts/rpc/artifacts.py` 的 begin / chunk / commit） | 檔案，根目錄 `STS_ARTIFACT_DIR`（`artifacts.py:36`–`:37`）。session 私有的 key 放在 `sessions/{session_id}/`，不出現在 operator 的目錄（`artifacts.py:42`–`:44`） | 策略（本地磁碟）、API（經平面 RPC） | 檔案留在 volume 上；只有程序內的 digest 快取是冷的（`artifacts.py:149`）。平面的 `reap_loop` 會清掉過期的 `.part`（`apps/sts/src/mftik_sts/app.py:196`–`:198`） | 權威多了一個（平面也寫），但那是 operator 上傳的路徑，不是策略狀態 → **沒有票改這一列**，見 §12 |
| hook 進度、offload 進度、交付的丟棄計數 | **不存在。** `status.sts` 的 payload 只有 `session_id`、`status`、`strategy`、`reason`、`created_by`、`finished_at`、`type`（`manager.py:403`–`:412`），全是生命週期終態，沒有任何進度欄位。最接近的是策略自己呼叫 `log()` 寫人看的 log | — | — | — | 整件事是新增（`offload` 本身也還不存在） → IF-05（#183）、B4-02（#202）、B5-03（#212）、B5-04（#213） |

## 7. 版本

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| 協定版本 `pv` | **不存在。** `Envelope` 沒有版本欄位（`packages/common/src/mftik/protocol/envelope.py:25`–`:36`），也沒有 `protocol_mismatch` 這個 reject code。收件端多半連 `type` 都不比對，直接拿 payload 去 validate（見 `docs/baseline/protocol.md` 4.4） | — | — | 現況的跨版本策略是「所有平面一起換同一個 tag」。`MFTIK_DIST_VERSION` 只用來組套件版號（`packages/common/hatch_version.py:7`），執行期沒有人比對它 | 新增 → IF-01（#179）、B4-01（#201） |

## 8. §3.3 沒有列、現況有的狀態

這些狀態在現況存在、§3.3 沒有對應的列。列在這裡是為了讓「§3.3 的表涵蓋了全部狀態嗎」這個問題有答案；其中幾項沒有任何票負責，一併記在 §12。

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 |
|---|---|---|---|---|
| 策略代碼（registry） | **API**。`RegistryStore` 是檔案系統上的樹（`packages/common/src/mftik/registry/store.py:1`、`RegistryStore.from_env` 在 `:87`，根目錄 env 是 `MFTIK_DATA`，`:32`），API 的 push / delete 寫它，再 fan-out 到每個 STS（`apps/api/src/mftik_api/sts_fanout.py`） | 檔案。API 一份、每個 STS 平面各一份副本 | STS 平面（`ensure_deployable`）、session worker（import 策略） | **STS 開機時向 API 要差額**：`catch_up_until_matched` 一直重試到 API 回報這個磁碟已經一致（`apps/sts/src/mftik_sts/registry_catchup.py:26`–`:40`），由 `app.py:379`–`:384` 啟動 |
| STS 進程認定自己有哪些 extras | **STS 平面進程**，開機和每次 registry reload 時讀一次並留在記憶體（`apps/sts/src/mftik_sts/runtime_env.py:1`–`:19`，刻意不每次重開 `applied.json`） | 磁碟上的 stamp，加上進程記憶體的那一份 | `ensure_deployable`、`/info`、`sts.env.sync` | `refresh()` 重算；deploy 時用這份記憶體副本判斷環境相不相容 |
| wire 層的訂閱帳本 | **每條 socket 的 `WireLedger`**（`packages/common/src/mftik/exchange/wire.py:178`） | venue connector 的記憶體 | socket 自己 | 重連時 `clear()` 後由 adapter `_restore` 重放。§3.3 把 observed 歸給「連線 worker 的 reconciler」，現況多了這一層獨立於 MD 的帳本（§1.1 有提到，但 §3.3 沒有列） |
| session 的 log 歷史 | **API**（`apps/api/src/mftik_api/log_persist.py`，訂閱 `log.*.*` 寫表） | Postgres `session_logs`（`packages/db/src/mftik_db/models/session_log.py:14`） | API 的 logs / ws / alert 比對 | 表本身是權威；平面重啟不影響 |
| 稽核、alert、auth、instance 宣告、使用者、帳號命名 | **API** | Postgres `audits`、`alert_*`、`auth_*`、`instances`、`users`、`accounts`。`instances` 的初始列由 alembic migration 寫（`packages/db/src/mftik_db/migrations/versions/0031_plane_instances.py`） | API、前端 | DB 是權威 |

## 9. 重啟恢復（逐一查證）

上面的表每一列只有一格寫收斂。這一節把「一個進程重啟時實際發生什麼」整條走完，因為差異大多在這裡。

### 9.1 STS 平面進程正常重啟（部署滾動）

**停止**（`SessionManager.close_all`，`manager.py:2141`）：

1. 取消 settle / rebuild / create / escalation 的背景 task。
2. `_stop_workers`（`manager.py:2180`）：**先把每個已啟動 worker 的 row 預先寫成 `interrupted`**（`:2197`），理由在 `:2189`–`:2192`——teardown 可能撐不過 SIGKILL 的期限，先寫錯一次比留下一個永遠 `live` 的 row 好。然後 SIGTERM 全部 worker，等 `WORKER_STOP_WAIT_S = 8.0` 秒（`spawn.py:46`），逾時 SIGKILL（`:2221`–`:2227`），最後關掉 lifeline pipe。
3. `_close_in_process`（`manager.py:2279`）對平面自己持有的 in-process session 做同一件事。

**worker 那一側**：收到 SIGTERM → `stop.set()`（`worker.py:230`–`:234`）→ `hold_until_quiet` 轉去跑 worker 自己的 `close_all` → `_close_in_process` **同樣把 row 寫成 `interrupted`**（`manager.py:2308`）。所以一次正常 shutdown 裡，同一個 row 被平面和 worker 各寫一次。這是 §2 那張表說「三個進程都寫 status」最直接的證據。

**開機**（`apps/sts/src/mftik_sts/app.py:amain`，`:296`）：

1. `schema_is_current()` 等 DB schema 到位（`:297`）。
2. 建 `SessionManager`，帶 `SubprocessSpawner()` 和 `rebuild_on_worker_exit=_rebuild_enabled()`（`:340`–`:341`）。
3. 起 RPC、heartbeat、`reap_loop`（60 秒一輪，`:368`）、health，以及 registry catch-up（`:379`–`:384`）。
4. `STS_REBUILD_ON_BOOT=1` 時，背景跑 `_rebuild_on_boot` → `rebuild_interrupted()`（`:389`、`:207`）；不 await，所以 RPC 不會被擋住。

**rebuild 的候選是 `status = interrupted`，只有這個值**（`manager.py:1423`–`:1432`，掃描有上限 `_REBUILD_SCAN_LIMIT`，截斷時會發警告）。每個候選要過六道閘（`_rebuild_claimed`，`manager.py:1549`）：

| 閘 | 條件 | 位置 |
|---|---|---|
| 年齡 | 距 `created_at` 不超過 `STS_REBUILD_MAX_AGE_S`，預設 1800 秒 | `manager.py:1553`–`:1564`、`app.py:146`–`:164` |
| placement | row 指名這個 instance，或由 TD region 推導到它 | `manager.py:1566`–`:1575`、`_placement_is_mine` 在 `:1877` |
| restart | row 的 `restart == "always"` | `manager.py:1576`–`:1583` |
| 次數 | `rebuild_count < _REBUILD_MAX_ATTEMPTS = 3` | `manager.py:1585`–`:1591`、`:149` |
| id 版本 | `session_id` 是 v1 的六位 hex | `manager.py:1593`–`:1600` |
| 策略 | 策略存在、`ensure_deployable` 通過、`strategy.rebuildable` 為真 | `manager.py:1610`–`:1662` |

過關就先 `bump_rebuild_count`（寫在嘗試**之前**，理由在 `repositories/session.py:265`–`:271`），再 spawn 一個 `role="rebuild"` 的 worker（`manager.py:1688`）。worker 裡 `adopt_interrupted` → `_rebuild_one`（`manager.py:1899`）：建新的 `StsSession` → `on_rebuild(st_facts)` → `mark_live` → `session.start()`（**重跑 `on_start`**）→ attach MD、再逐個 attach TD。attach 被拒 → `failed`（不再重試）；attach 逾時 → 退回 `interrupted` 等下次開機（`manager.py:1958`–`:1976`）。成功後 300 秒（`_REBUILD_SETTLE_S`，`:159`）還活著，就 `reset_rebuild_count`。

**恢復回來的只有**：row 上的設定（`td`、`md_ids`、`st_paras`、`type`、`created_by`）和 `st_facts`。**沒有恢復的**：策略物件的欄位、`Timer` 的 task、`StrategyOms` 的 inflight / done 集合與 cid 計數器（`bind` 會清掉，`oms.py:132`–`:138`）、`EventLog._seq`、MD / TD 的 ack 時鐘。交易所那一側靠 recon 取回。

### 9.2 STS 平面進程被強制殺掉

兩種結果，差別在 worker 有沒有跟著死：

- **只有平面死、worker 活著**：worker 由 lifeline EOF（`spawn.py:29`–`:34`、`worker.py:152`）或 `PDEATHSIG`（`worker.py:73`–`:92`）察覺，自己 graceful stop 並寫自己的 row。
- **worker 跟著一起死**（整個 cgroup 被殺，這是生產上的情形，見 `docs/Deployment.md` 的「重啟 agent 會殺掉所有東西」）：row 停在 `live`，沒有人寫它。由新進程的 `reap_orphans` 連續兩輪（`_ORPHAN_STRIKES = 2`，約 60–120 秒）都沒看到本機持有它，才改成 `interrupted`（`manager.py:1390`–`:1400`）——註解明寫為什麼是 `interrupted` 而不是 `failed`：這樣 rebuild 的候選集合才剛好等於 `status = interrupted`。之後走 §9.1 的 rebuild。

### 9.3 session worker 自己死掉

`_on_worker_exit`（`manager.py:827`）：

- exit code 0 → **什麼都不做**。worker 已經自己寫了終態 row，平面重寫會蓋掉策略留下的 reason（`:865`–`:871`）。
- 非 0，而且 row 還是 `live` → 寫 `interrupted` + 發 `status.sts`（`:874`–`:895`），然後在 `rebuild_on_worker_exit` 開著時立刻 `_schedule_rebuild`。
- 先被 force-stop 升級過（`stop_escalated`）→ 寫 `failed`，不 rebuild（`:854`–`:864`）。

**這條路沒有任何 backoff**：`_schedule_rebuild` 直接建 task 跑 `rebuild_session`（`manager.py:904`–`:924`）。這是 §6 那張表說 cid 的不撞號和 R2 的前提不同的原因。

### 9.4 MD 平面重啟

1. 記憶體全丟：`_links`、`Dispatcher._subs`、`_venues`、`_expiry_tasks`、`WireLedger`。
2. **沒有任何東西從 `md_sessions` 重建**。`reap_orphans` 的註解把這點寫得很清楚：「nothing rebuilds from one — STS re-attaching on rebuild is what writes the row live again」（`apps/md/.../session/manager.py:544`–`:547`）。
3. 實際發生的事是 **STS 端把 session 殺掉**：MD 的 `md.lease.ack` 一停，STS 的 heartbeat loop 在 `heartbeat_interval * PEER_MISS_LIMIT` = 3 秒（`LEASE_HEARTBEAT_INTERVAL_S = 1.0`、`LEASE_MISS_LIMIT = 3`，`packages/common/src/mftik/protocol/messages.py:825`、`:829`）內呼叫 `_fail_from_infrastructure("md feed from …")`（`session.py:785`–`:799`），session 變成 **`failed`**。
4. `failed` 不在 rebuild 的候選集合裡（§9.1），worker 也是 exit 0（它自己寫完了 row），所以 `_on_worker_exit` 也不會排 rebuild。**結論：MD 重啟會讓所有附著的 session 死掉，而且不會自動回來，要人工重新 deploy。**
5. 新的 MD 上，`reap_orphans` 把名下還是 `live` 的 `md_sessions` row 收掉。
6. tape：乾淨停止時 `_stamp_stopped` 寫下 `stopped_ms`，下一次 `mark_recording` 把洞的長度寫進 `gaps`；被強制殺掉沒有 `stopped_ms`，只能把 `continuous_since_ms` 重設，洞量不出來（`tape_store.py:181`–`:220`）。Redis 裡已經寫下的 print 不受影響。

### 9.5 TD 平面重啟（OMS 在重啟後長什麼樣）

**丟掉的**：`_accounts`、每個 `Session`、`Oms`、`Ledger`、`cid_owner`、`_leverage` 快取。OMS 和 ledger **都沒有落地**（`publish_oms`、`write_ledger` 是空實作，`session.py:503`、`:1139`），所以沒有快照可讀。

**誰察覺**：和 §9.4 一樣，STS 在 3 秒內把 session 判 `failed`（`session.py:801`–`:816`），不會自動回來。

**誰重新接上**：沒有人自動接。`_accounts` 只在 `attach` 時建立（`manager.py:217`–`:250`），所以要等下一次 deploy（或某個 STS rebuild 的 `_attach_td`）。TD 還會先擋住 attach，直到它看到那個 session 的 lease heartbeat（`manager.py:279`–`:291`）。

**第一次 attach 之後 OMS 的內容**（`Session.start()` → `reconcile()`，`session.py:327`）：

| 項目 | 重啟後 |
|---|---|
| 交易所上還掛著的單 | **有**。`fetch_open_orders` 拉回來，`apply_reconcile` 整批替換 `_orders`，只留 `status.is_open()` 的（`oms/oms.py:67`–`:87`） |
| 餘額、部位 | **有**。同一次 recon 的 `fetch_balances` /（有的話）`fetch_positions` |
| 終態的單 | **沒有**，而且本來就不該有——open orders 不含它們，OMS 也只存 open |
| `cid_owner`（哪一張單屬於哪個 session） | **空的**。註解明寫 recon 撿回來的單沒有 entry，所以別的 session 來撤它不會被擋（`manager.py:144`–`:148`）。策略那一側要自己用 `Strategy.owns()` 解 cid 裡的 session 欄位（`strategy/base.py:298`–`:317`） |
| ledger 的預扣（`_prelocked`、`_by_cid`） | **空的**。`apply_venue_many` 只更新 venue 的總額，不動預扣（`oms/ledger.py:91`）。所以重啟後 `available()` 會比重啟前寬鬆，因為預扣不見了 |
| `_pending_since` / `_unknown_since` / `_cancel_since` 這些 watchdog 的計時 | **歸零**。`reconcile()` 自己也會清掉它們（`session.py:454`–`:459`） |
| `HistoryWriter` 佇列裡還沒 flush 的 order / fill | **遺失**。正常停止會 drain 加 flush（`history.py:185`–`:207`），強制殺掉就掉了，靠 backfill 補（`history.py:185`–`:190`） |

**`chase_unknown` 在重啟後幫不上忙。** 它是 `_chase_loop` 每 `PENDING_SWEEP_INTERVAL_S = 1.0` 秒跑一次（`session.py:383`–`:399`、`:58`）：對每張 UNKNOWN 先用 `fetch_order_by_client_order_id` 單張查（`resolve_unknown`，`session.py:814`，venue 不支援就留著等 recon，`:836`–`:844`）；最老的一張超過 `UNKNOWN_FORCE_RECON_S = 10` 秒還沒解決就強制一次 `reconcile()`（逾時上限 `UNKNOWN_FORCE_RECON_TIMEOUT_S = 15`，之後每 `UNKNOWN_FORCE_RECON_INTERVAL_S = 60` 秒再試；`session.py:63`、`:74`、`:78`、`:976`–`:985`）。關鍵是**它只認得這個 incarnation 自己標成 UNKNOWN 的單**——重啟後 OMS 是空的，上一個 incarnation 留下的 UNKNOWN 沒有任何記錄，所以它們只會以 open order 的身分出現在第一次 recon 裡，或者根本不出現（已成交、已撤、或仍然查不到）。

同樣的限制也在 `reconcile()` 的補救路徑上：recon 前是 UNKNOWN、recon 後不在 open orders 裡的單，會補發一筆 CANCELED（或 `_unknown_if_missing` 記下的狀態）的終態事件（`session.py:439`–`:443`、`:463`–`:473`、`_announce_recon_settled` 在 `:477`）。但 `was_unknown` 是從**當下的 OMS** 讀的，所以重啟後那一份是空的，這個補救對跨重啟的 UNKNOWN 不生效。

**策略那一側只會收到一次 recon 快照**：第一次收到 TD 的 lease ack 時，平台自動幫策略發一次 `sts.recon`（`session.py:1171`–`:1176` → `strategy/base.py:355`–`:372`），TD 用 `_handle_recon` 回一份當下的書（`manager.py:796`，**不為了這個 attach 去跟交易所 recon**），策略在 `on_recon_done` 收到。**沒有 `td.account.reset`、沒有 `on_resync`**，所以策略無法分辨「我是新 deploy」和「TD 重啟過，書被重建了」。

### 9.6 API、NATS、Redis 重啟

- **API**：session 不在 API 上，deploy 是一次性的 RPC。重啟只影響正在進行的 deploy（`deploy_strategy` 中斷，它的回滾也跟著消失），以及 `backfill_cron` 的排程重新起算。`registry` 在磁碟上。
- **NATS**：沒有 JetStream（見 `docs/Deployment.md` 的 NATS 一節），所有訊息都是 at-most-once。重啟期間只要 lease ack 斷超過 3 秒，所有 session 都會走 §9.4 / §9.5 的路被判 `failed`。
- **Redis**：只有 MD 連它（`apps/md/src/mftik_md/app.py:171`；`STS 從來沒有 REDIS_URL`）。`appendonly yes` 加 `appendfsync everysec`（`deployment/redis/redis.conf:22`–`:23`），所以最多掉約一秒的 print。重啟**不會**在 coverage 上留下任何記錄——`gaps` 只有 MD 那一側的 start / stop 會寫。

## 10. 生產部署上這些狀態落在哪

TODO

## 11. 計畫與代碼不符之處

TODO

## 12. 沒有票涵蓋的差異

TODO

## 13. 重現這份盤點

TODO
