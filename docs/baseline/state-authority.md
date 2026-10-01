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
| 常駐訂閱 | **不存在。** 等效做法是跑一個 `tape_keeper` 策略 session，用它的 attach 把 feed 的 refcount 撐住；模組 docstring 自己就說「這是那個 somebody」（`apps/sts/src/mftik_sts/impl/tape_keeper.py:1`–`:10`） | 那個 session 自己的 `sts_sessions` row。沒有 `md_standing_subscriptions` 表 | 和一般 session 的路徑完全相同 | 和一般 session 一樣：STS 重啟後靠 rebuild 回來（`TapeKeeper.rebuildable = True`，`tape_keeper.py:50`） | 目標是設定檔加一張表 → IF-14（#192）建表、B8-05（#242）讓 `tape_keeper` 退役 |
| `api_id` → instance 綁定、帳號設定 | **API**（`apps/api/src/mftik_api/routes/apis.py:create_api`，`:89`；刪除 `:253`） | Postgres `apis`，`instance_id` 是 NOT NULL（`packages/db/src/mftik_db/models/api.py:33`–`:75`）。**沒有任何帳號設定欄位**（cancel-on-disconnect 之類） | TD（`apps/td/src/mftik_td/db.py:instance_name`）、STS（`apps/sts/src/mftik_sts/db.py:td_instance`，docstring 說明為什麼不把它抄進 session document）、API（`orchestrate.py:_td_instance`）；repository 在 `packages/db/src/mftik_db/repositories/api.py:38` | DB 本身就是權威 | 綁定這件事**一致**。帳號設定欄位不存在 → IF-14（#192）加欄位、B6-07（#225）使用它 |
| listing：合約、到期、strike | **SYM 平面**（`apps/sym/src/mftik_sym/plane.py:SymbolPlane.refresh`，`:63`；`refresh_loop` 在 `:145`） | Postgres `symbol_ticker`、`symbol_filter`（`packages/db/src/mftik_db/models/symbol.py:70`、`:137`） | MD **不直接查表**，走 `SymbolClient` 的 broker RPC（`apps/md/.../session/manager.py:_resolve_expiry`，`:977` → `packages/common/src/mftik/symbols/client.py:SymbolClient.get`）；TD、STS 也一樣 | 每 `SYM_REFRESH_INTERVAL` 秒重拉一次，預設 3600（`apps/sym/src/mftik_sym/app.py:34`、`:39`） | **—**（§3.3 寫的「每小時刷新」成立。讀取走 RPC 而不是直接查表，不改變權威） |

## 3. 進程層

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| worker 是否存在、exit code、signal | TODO | TODO | TODO | TODO | TODO |
| 每個 instance 存活中的 worker 集合 | TODO | TODO | TODO | TODO | TODO |
| worker 的代碼版本 | TODO | TODO | TODO | TODO | TODO |

## 4. MD

| 狀態 | 權威 | 存放 | 讀取者 | 收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| 每條連線的 desired atom 與 generation | TODO | TODO | TODO | TODO | TODO |
| selector 的 universe、epoch、置中狀態 | TODO | TODO | TODO | TODO | TODO |
| 連線上實際訂閱成功的 atom（observed） | TODO | TODO | TODO | TODO | TODO |
| 行情內容，包括 fold 後的 book | TODO | TODO | TODO | TODO | TODO |
| feed 狀態 live / down | TODO | TODO | TODO | TODO | TODO |
| per-atom `seq` | TODO | TODO | TODO | TODO | TODO |
| tape 與 coverage | TODO | TODO | TODO | TODO | TODO |

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
