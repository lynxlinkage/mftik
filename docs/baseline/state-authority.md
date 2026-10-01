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

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| 交易所上的掛單、部位、餘額（最終真相） | TODO | TODO | TODO | TODO | TODO |
| OMS、ledger（預扣、available） | TODO | TODO | TODO | TODO | TODO |
| 交易層開或關 | TODO | TODO | TODO | TODO | TODO |
| 帳號狀態 ready / degraded / unavailable | TODO | TODO | TODO | TODO | TODO |
| 訂單歷史、成交、資金流水 | TODO | TODO | TODO | TODO | TODO |

## 6. STS session

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| 策略內部狀態 | TODO | TODO | TODO | TODO | TODO |
| `client_order_id` 序號 | TODO | TODO | TODO | TODO | TODO |
| event log | TODO | TODO | TODO | TODO | TODO |
| artifacts | TODO | TODO | TODO | TODO | TODO |
| hook 進度、offload 進度、交付的丟棄計數 | TODO | TODO | TODO | TODO | TODO |

## 7. 版本

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| 協定版本 `pv` | TODO | TODO | TODO | TODO | TODO |

## 8. §3.3 沒有列、現況有的狀態

TODO

## 9. 重啟恢復（逐一查證）

### 9.1 STS 平面進程正常重啟

TODO

### 9.2 STS 平面進程被強制殺掉

TODO

### 9.3 session worker 自己死掉

TODO

### 9.4 MD 平面重啟

TODO

### 9.5 TD 平面重啟

TODO

### 9.6 API、NATS、Redis 重啟

TODO

## 10. 生產部署上這些狀態落在哪

TODO

## 11. 計畫與代碼不符之處

TODO

## 12. 沒有票涵蓋的差異

TODO

## 13. 重現這份盤點

TODO
