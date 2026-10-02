# REFACTOR_TICKETS — 平面進程化重構的工作票

> **對應 `ARCHITECTURE_CHANGE_PLAN.md` v0.31。** 所有改動先合併到 `refactor/process-planes` 分支。票裡的 F 編號、§ 章節、附錄都指那份文件。
>
> 每張票都有描述、範圍、驗收、依賴。驗收寫成別人能檢查的事：測試名稱、grep 結果、量測數字、文件章節。

## 怎麼用這份文件

**編號：** `<批次>-<序號>`。每張票都已開成 GitHub issue（#154 到 #253，以及後來加的 #275 到 #277、#363，label 為 `refactor` 和 `batch:<批次>`），標題後的括號是 issue 編號。批次依序是 B0、B1、RM、B2、IF、B3 到 B10（計畫 §11）。依賴只列直接依賴。

**RM（清場）的共同驗收：**

1. 票上列出的符號，在 `apps/`、`packages/` 裡 grep 不到。`migrations/`、`docs/archive/` 除外。
2. 依賴被刪代碼的測試一起刪，剩下的測試全綠。
3. 呼叫端要嘛一起刪，要嘛改成占位：RPC 回 `NotImplementedError("<IF 票號>")`，HTTP 路由回 501。占位由對應的 IF 票接上介面。不留任何「暫時」的相容層（F1）。

**IF（介面）的共同驗收：**

1. 模組能 import，`ruff` 通過。
2. 實作只回傳 null data：`None`、空集合，或 `NotImplementedError("<IF 票號>")`。
3. docstring 寫明不變式，以及這一層持有哪些狀態的權威（§3.3）。
4. 附一組 `@pytest.mark.xfail(strict=True)` 的契約測試，描述之後要成立的行為。實作票讓它們通過時，`strict` 會逼人拿掉 xfail 標記，契約就變成正式測試。

**實作票（B3 以後）的共同驗收：** 對應 IF 的契約測試全部轉綠；新測試遵守 `TESTING.md`（B2-01）。

## 總覽

| 批次 | 票數 | 目的 |
|---|---|---|
| B0 基線 | 6 | 凍結現況、量測、盤點協定與狀態權威、確定刪除影響面 |
| B1 文件 | 4 | 封存舊文件，重寫 Deployment，萃取 `ARCHITECTURE.md` |
| RM 清場 | 10 | 刪掉要重寫的代碼和測試，留下「沒有 session 機制」的基線 |
| B2 測試 | 5 | 測試標準、`FakeClock`、共用 NATS 連線、tier 與 CI 閘門 |
| IF 介面 | 16 | 新抽象層只定義介面，回傳 null data，附 xfail 契約測試 |
| B3 procman | 7 | shim、Supervisor、reattach、報告、准入、Strategon 實機驗證 |
| B4 骨架 | 10 | paper 上跑通 deploy → 下單 → 成交 → end；`pv` 的 deploy 比對與 transport 檢查 |
| B5 STS | 11 | 交付策略、event log、offload、hook 預算、失聯通知、crash 與重啟、策略測試改寫、策略樹版本釘住、主機磁碟的 operator 路徑 |
| B6 TD | 8 | 常駐層、交易層、`cancel_session`、drain-replace、backfill、狀態廣播、cancel-on-disconnect |
| B7 MD atom | 11 | atom 模型、各 venue adapter、通用 join、tape、fetch worker |
| B8 MD 編排 | 7 | orchestrator、placement、reconciler、到期、常駐訂閱、手動 restart |
| B9 Selector | 4 | option_chain、rolling_future、狀態持久化、`md.universe` |
| B10 切換 | 5 | migration、preflight、前端、上線、文件定稿 |

```
B0 ─▶ RM ─┬─▶ B2 ─┐
          └─▶ IF ─┴─▶ B3 ─▶ B4 ─┬─▶ B5 ───────────┐
B1（獨立）                       ├─▶ B6 ───────────┤
                                 └─▶ B7 ─▶ B8 ─────┴─▶ B9 ─▶ B10
外部依賴：strategon#60 → B3-06、B10-04；strategon#61（非必要）→ B10-04
```

---

## B0 基線

### B0-01 打 `arch/baseline` tag（#154）

- **描述：** 凍結重構前的代碼，也就是 B10 的回滾目標。
- **範圍：** 在計畫的基準 commit（`main` @ `a0cbfb2`）打 annotated tag `arch/baseline`。
- **驗收：** tag 已推到 origin；CI 在這個 commit 是綠的。
- **依賴：** —

### B0-02 在 GitHub Actions 量測現行測試耗時（#155）

- **描述：** 建立兩分鐘預算的起點（F30），並找出慢的真正原因。
- **範圍：** CI 加一個一次性的 job，跑 `pytest --durations=0 --junitxml`，依模組彙整。
- **驗收：**
  - 附錄 C 填入每個測試模組的耗時，以及 `ubuntu-latest` 上的總 wall time。
  - 最慢的 50 個測試各標出主因：lease 心跳、真的 sleep、NATS 往返、子進程、Postgres、其他。
  - 回答「慢是不是 NATS 造成的」，這是 F31 的前提。
- **依賴：** B0-01

### B0-03 協定盤點（#156）

- **描述：** 列出現行所有訊息型別和 subject，定下每一個在新協定裡的去向。
- **範圍：** `packages/common/src/mftik/protocol/messages.py`、`topics.py`，以及各平面的 `rpc/router.py`。
- **驗收：** `docs/baseline/protocol.md` 列出每個 type 和 subject 的發送者、接收者與去向（保留、改名、刪除），並對照 §8.3。每一個型別都有去向。
- **依賴：** B0-01

### B0-04 現況的狀態權威表（as-is）（#157）

- **描述：** 對照 §3.3 的目標表，寫出每種狀態現在由誰寫、存在哪、重啟後怎麼恢復。
- **範圍：** 三個平面、API、SDK、DB、Redis。
- **驗收：** `docs/baseline/state-authority.md` 有一張和 §3.3 同欄位的表；和目標不同的每一列，都指到負責改它的票。
- **依賴：** B0-01

### B0-05 刪除影響面盤點（#158）

- **描述：** RM 每張票要刪的符號，先反查所有呼叫端和測試，讓 RM 的範圍在動手前確定。
- **範圍：** §5.4、§8.1、§8.2 的刪除清單；附錄 A、B。
- **驗收：** 每張 RM 票的範圍補上完整的呼叫端清單（`檔案:函式`）；附錄 A、B 定稿。
- **依賴：** B0-01

### B0-06 確認主機上的事實（#159）

- **描述：** 驗證 §4.5 的兩個推論：agent 升級會殺掉所有 strategy，以及 cgroup 記憶體上限沒有生效。
- **範圍：** cp、yite 兩台主機。
- **驗收：** `systemctl show strategon-agent -p KillMode`、`cat /proc/<plane pid>/cgroup`、`/proc/<pid>/status` 的結果記進 §4.5；與推論不符的地方回頭修正計畫。
- **依賴：** —

---

## B1 文件

### B1-01 封存 `docs/`（#160）

- **範圍：** 除了 `ARCHITECTURE_CHANGE_PLAN.md`、`REFACTOR_TICKETS.md`、`Deployment.md`，`docs/*.md` 全部移到 `docs/archive/`，包括 `Deribit`、`BitgetUta` 實測表（F28）。另外新增 `docs/archive/INDEX.md`，記錄每份文件的封存日期和取代它的文件。
- **驗收：** `docs/` 根目錄只剩 §10 列出的檔案；INDEX 涵蓋每個封存檔；repo 內指向舊路徑的連結都已更新。
- **依賴：** —

### B1-02 依現況重寫 `Deployment.md`（#161）

- **描述：** F28。以 `deployment/sets/*.json`、本機的 `deployment/nats/nats.conf`（被 `.gitignore` 排除，不在 repo 裡；Deployment.md 要寫明它在哪、怎麼產生）和 Strategon 現在的實際行為為準。
- **驗收：** 涵蓋 plane sets（成員、機器、env、volume；`limits.memoryBytes` 目前沒有生效）、OCI 與 `captureStdio`、每個 site 一台 NATS 加 gateway、secret 的位置、部署與回滾指令。每一節都能對應到 repo 裡的檔案。
- **依賴：** B0-06

### B1-03 README 縮成指引（#162）

- **驗收：** README 只剩一句話的專案說明、目錄導覽、quick start（`just up`、`just test`）、文件索引；過時的段落全部刪除。
- **依賴：** B1-01

### B1-04 萃取 `ARCHITECTURE.md`（#163）

- **描述：** F1 到 F38 都已定案。從計畫萃取出目標架構，只寫「是什麼」，不寫討論過程。
- **驗收：** 涵蓋原則（§2.3）、分層與狀態權威（§3）、procman（§4）、各平面的 worker 與 controller、協定、測試標準摘要。兩份文件衝突的地方，以計畫為準並修正。
- **依賴：** —

---

## RM 清場

RM 結束時，三個平面都還能啟動，只是沒有 session 機制。要搬移、不刪除的代碼，在各票的「留下」列出，由後面的批次搬進新的層。

**呼叫端清單（B0-05，#158）：** 每張票的「呼叫端」是在 `main` @ `a0cbfb2` 上逐一反查出來的，格式是 `檔案:函式`。清單只列**非測試**的呼叫端；測試那一側在附錄 A 定稿。各票另有「B0-05 補正」，列出盤點時發現、原本範圍沒寫到的符號或呼叫端。盤點時確認：§5.4、§8.1、§8.2 和附錄 A、B 上的每一個符號都存在於 `a0cbfb2`，沒有任何一條指到 PR #153（`8ddfc23`，已關閉）才有的代碼。

### RM-01 STS：刪除 rebuild（#164）

- **範圍：**
  - `apps/sts/src/mftik_sts/session/manager.py`：`rebuild_interrupted`、`rebuild_session`、`adopt_interrupted`、`_spawn_rebuild`、`_rebuild_after_exit`、`_settle_rebuild`、`rebuild_on_worker_exit`
  - `apps/sts/src/mftik_sts/app.py`：`STS_REBUILD_ON_BOOT`、`STS_REBUILD_MAX_AGE_S` 與相關的啟動流程；`packages/common/src/mftik/cli/templates/docker-compose.yml`、`deployment/sets/planes.json` 裡的 `STS_REBUILD_ON_BOOT`
  - `apps/sts/src/mftik_sts/worker.py`：`rebuild` role、`adopt_interrupted` 的呼叫
  - SDK：`strategy/base.py` 的 `rebuildable`、`on_rebuild`、`remember()`；`strategy/session.py` 的 `remember`
  - 內建策略 `impl/*.py`：`rebuildable`、`on_rebuild`。`chase` 的 `started_ms` 和滑價錨定價改成只存在記憶體
  - `apps/sts/src/mftik_sts/db.py` 的 `remember_fact`、`bump_rebuild_count`、`reset_rebuild_count`，以及 `packages/db/.../repositories/session.py` 的對應方法。欄位本身留到 B10-01 的 migration
  - 測試：`test_rebuild`、`test_environment_rebuild`、`packages/db/tests/test_sts_session_repository.py` 裡和 `remember` / `rebuild_count` 相關的案例，以及策略測試裡的 rebuild 案例。F16 只允許刪 rebuild 案例，其餘留到 B5
- **呼叫端（`main` @ `a0cbfb2`）：**
  - `rebuild_interrupted`（定義 `session/manager.py:SessionManager.rebuild_interrupted`）：`apps/sts/src/mftik_sts/app.py:_rebuild_on_boot`
  - `rebuild_session`（`manager.py:SessionManager.rebuild_session`）：`manager.py:SessionManager._rebuild_after_exit`、`manager.py:SessionManager.rebuild_interrupted`
  - `adopt_interrupted`（`manager.py:SessionManager.adopt_interrupted`）：`apps/sts/src/mftik_sts/worker.py:_start`（`role == "rebuild"`）
  - `_spawn_rebuild`：`manager.py:SessionManager._rebuild_claimed`
  - `_rebuild_after_exit`：`manager.py:SessionManager._schedule_rebuild`
  - `_settle_rebuild`：`manager.py:SessionManager._watch_rebuild_settle`
  - `rebuild_on_worker_exit`（建構參數，`manager.py:SessionManager.__init__`）：`apps/sts/src/mftik_sts/app.py:amain`，值來自 `app.py:_rebuild_enabled`
  - `STS_REBUILD_ON_BOOT`：`app.py:_rebuild_enabled`（讀取）、`app.py:amain` 與 `app.py` 的開機 log；設定值在 `docker-compose.yml:185`、`deployment/sets/planes.json:57`、`packages/common/src/mftik/cli/templates/docker-compose.yml:216`
  - `STS_REBUILD_MAX_AGE_S`：`app.py:_rebuild_max_age_s`（讀取）；`rebuild_max_age_s` 建構參數由 `app.py:amain` 傳入，`manager.py:SessionManager._rebuild_claimed` 使用
  - `rebuildable`：定義 `packages/common/src/mftik/strategy/base.py:Strategy`；判斷點 `manager.py:SessionManager._rebuild_claimed`；設定點 `impl/chase.py:ChaseOrder`、`impl/oco.py:OneCancelOther`、`impl/cross_arb.py:CrossArb`、`impl/tape_keeper.py:TapeKeeper`（`True`）、`impl/macd_dollar.py:MacdDollarBars`（`False`）
  - `on_rebuild`：定義 `strategy/base.py:Strategy.on_rebuild`；呼叫點 `manager.py:SessionManager._rebuild_one`；實作 `impl/chase.py:ChaseOrder.on_rebuild`、`impl/oco.py:OneCancelOther.on_rebuild`、`impl/cross_arb.py:CrossArb.on_rebuild`、`impl/tape_keeper.py:TapeKeeper.on_rebuild`
  - `remember`：`strategy/base.py:Strategy.remember` → `strategy/session.py:SessionView.remember` → `apps/sts/src/mftik_sts/session/session.py:StsSession.remember` → `manager.py:SessionManager._remember_fact` → `apps/sts/src/mftik_sts/db.py:remember_fact` → `packages/db/src/mftik_db/repositories/session.py:StsSessionRepository.remember`。策略端呼叫者：`impl/chase.py:ChaseOrder.on_recon_done`、`impl/chase.py:ChaseOrder.on_best_quote`
  - `remember_fact` / `bump_rebuild_count` / `reset_rebuild_count`（`apps/sts/src/mftik_sts/db.py`）：`app.py:amain`、`worker.py:amain`（兩處都傳給 `SessionManager`）
  - `StsSessionRepository.bump_rebuild_count` / `reset_rebuild_count` / `remember`：除 `apps/sts/src/mftik_sts/db.py` 之外沒有其他呼叫端，可以安全刪除
  - `chase` 的 `started_ms` / `ref_start`：常數 `impl/chase.py:_FACT_STARTED_MS`、`_FACT_REF_START`；寫入 `ChaseOrder.on_recon_done`、`ChaseOrder.on_best_quote`；讀回 `ChaseOrder.on_rebuild`；使用 `ChaseOrder._elapsed_s`、`ChaseOrder._slippage_bps`、`ChaseOrder._on_tick`、`ChaseOrder._expired`
- **B0-05 補正：**
  - `manager.py` 的 rebuild 私有輔助原範圍沒列到：`_schedule_rebuild`、`_note_rebuild_row`、`_find_interrupted`、`_rebuild_claimed`、`_unwind_failed_rebuild`、`_watch_rebuild_settle`、`_rebuild_one`、`_abandon_rebuild`、`_remember_fact`。RM-04 整份刪除 `manager.py`，所以實際上會一起消失；RM-01 單獨驗收時要接受它們仍在。
  - `apps/sts/src/mftik_sts/session/session.py` 的 `StsSession.remember`、`__init__` 的 `remember` 參數與 `RememberHandler` 型別，落在 RM-04 的「留下」範圍裡，原本沒有人負責刪。歸入本票。
  - `packages/common/README.md` 的 rebuild 段落（`on_rebuild`、`rebuildable`、`remember`）要一起改寫。RM-03 已經列了這個檔案，本票也會動到它。
  - `apps/sts/src/mftik_sts/impl/chase.py:ChaseOrder._restoring`、`ChaseOrder._adopt`、`impl/oco.py:OneCancelOther._restoring` / `_adopt`、`impl/cross_arb.py:CrossArb._restoring` 只在 `on_rebuild` 設旗標、在 `on_recon_done` 讀取。`on_rebuild` 刪掉之後這些分支恆為 false，要和 RM-02 的 `on_recon_done` 一起處理。
  - 策略測試中依賴 rebuild API 的案例（F16 允許刪的部分，`main` @ `a0cbfb2` 實測）：`test_chase.py` 6 個（`test_a_rebuilt_chase_does_not_restart_its_expiry_budget`、`test_a_rebuilt_chase_keeps_the_slippage_it_already_ran`、`test_a_rebuilt_chase_adopts_what_it_left_resting`、`test_an_order_that_finished_while_away_is_counted_not_adopted`、`test_another_sessions_orders_are_not_adopted`、`test_unreadable_facts_do_not_stop_the_rebuild`，另有 `test_arming_keeps_the_clock_and_the_anchor` 斷言 `remember` 的內容）、`test_cross_arb.py:test_rebuild_is_a_clean_restart`、`test_tape_keeper.py:test_it_is_rebuildable`、`test_oco.py` 透過 `_restore` 的 9 個案例。`test_oco.py` 那 9 個的主題是「把留在交易所的兩腳接回來」，不是 rebuild；它們要改寫成不經 `on_rebuild` 的版本，不能直接刪。
  - `interrupted` 這個 session 狀態（§5.2 說要刪）沒有任何 RM 票負責。它在 `packages/db/src/mftik_db/models/session.py:SessionStatus`、`SessionStatus.terminal`、`packages/db/src/mftik_db/repositories/session.py:StsSessionRepository.mark_ack` 的接受集合、`apps/api/src/mftik_api/routes/sts.py` 的 `_ATTENTION` 與 `ack_session`、`routes/stats.py:get_stats`、`apps/api/src/mftik_api/schemas.py`，以及前端 9 個檔案（`routes/+page.svelte`、`routes/sts/+page.svelte`、`routes/strategy/+page.svelte`、`routes/strategy/[sessionId]/+page.svelte`、`routes/board/+page.svelte`、`routes/board/[sessionId]/+page.svelte`、`lib/api.ts`、`lib/logging/status.ts`、`app.css`）。RM 只刪寫入端（都在 `manager.py` 裡，隨 RM-04 整份刪除），讀取端留到 B10-01（DB）與 B10-03（前端）；請在那兩張票補上。
- **驗收：** 共同驗收；`sts_sessions.st_facts` 沒有任何寫入路徑；策略測試仍然全綠。
- **依賴：** B0-05
- **決策：** F10、範圍 4

### RM-02 STS：刪除續約、自動 recon 與 recon API（#165）

- **範圍：**
  - `apps/sts/src/mftik_sts/session/session.py`：`_lease_heartbeat_loop`、`_md_acks`、`_td_acks`、`_heartbeat_overslept`、`_shift_peer_acks`、`_fail_from_infrastructure`（MD / TD feed 的那幾條路徑）、`_recon_sent`、`_on_lease_ack`
  - SDK：`Strategy.send_recon`；protocol 的 `STS_RECON`，以及 `apps/td/.../session/manager.py` 裡處理它的分支。`Strategy.on_recon_done` 的 hook 定義**不在本票刪除**，見下面的補正
  - 內建策略（chase、cross_arb、macd_dollar、noop、oco、twap）在 `on_recon_done` 才開始交易的寫法，改成在現有的 `on_ready` 開始——依下面的補正，這一步和那 111 個策略測試一起移到 B5-08
  - 測試：`test_md_ack_watchdog`、`test_td_ack_watchdog`、`test_recon_oms`
- **呼叫端（`main` @ `a0cbfb2`，以下都在 `apps/sts/src/mftik_sts/session/session.py`，除另有註明）：**
  - `_lease_heartbeat_loop`：`StsSession.start`
  - `_md_acks`：寫入 `StsSession._touch_md_ack`、`StsSession._on_md_lease_ack`；讀取 `StsSession._shift_peer_acks`、`StsSession._lease_heartbeat_loop`、`StsSession._refresh_md_ack_from_print`；宣告 `StsSession.__init__`
  - `_td_acks`：寫入 `StsSession._on_lease_ack`；讀取 `StsSession._shift_peer_acks`、`StsSession._lease_heartbeat_loop`；宣告 `StsSession.__init__`
  - `_heartbeat_overslept`、`_shift_peer_acks`：唯一呼叫端都是 `StsSession._lease_heartbeat_loop`
  - `_fail_from_infrastructure`：6 個呼叫點 —— `StsSession._lease_heartbeat_loop`（3 處：`"lease heartbeat"`、MD stale、TD stale）、`StsSession._pump_md_session`（`"md feed"`）、`StsSession._pump_td_session`（`"td session feed api_id=…"`）、`StsSession._pump_td_global`（`"td global feed api_id=…"`）
  - `_recon_sent`：`StsSession.__init__`、`StsSession._on_lease_ack`
  - `_on_lease_ack`：`StsSession._pump_td_session`
  - `send_recon`（定義 `packages/common/src/mftik/strategy/base.py:Strategy.send_recon`）：平台側唯一呼叫端是 `StsSession._on_lease_ack`
  - `on_recon_done`（定義 `strategy/base.py:Strategy.on_recon_done`）：平台側 `StsSession._on_recon_done`；實作在全部 6 支內建策略 —— `impl/chase.py:ChaseOrder.on_recon_done`、`impl/oco.py:OneCancelOther.on_recon_done`、`impl/cross_arb.py:CrossArb.on_recon_done`、`impl/twap.py:TwapStrategy.on_recon_done`、`impl/macd_dollar.py:MacdDollarBars.on_recon_done`、`impl/noop.py:NoopStrategy.on_recon_done`
  - `STS_RECON`（定義 `packages/common/src/mftik/protocol/messages.py`，匯出 `protocol/__init__.py`）：發送端 `strategy/base.py:Strategy.send_recon`；接收端 `apps/td/src/mftik_td/session/manager.py:SessionManager._lease_loop._on_message`
  - `_handle_recon`（`apps/td/src/mftik_td/session/manager.py:SessionManager._handle_recon`）：唯一呼叫端 `SessionManager._lease_loop._on_message`；相關的等待邏輯在 `SessionManager._arm_recon_deadline`、`SessionManager._flush_recon_waiters`、`SessionManager._publish_recon_done`、`TradingAccount.recon_waiters`
- **B0-05 補正：**
  - 原範圍沒列到的 lease 輔助，一併刪除：`StsSession._on_md_lease_ack`、`StsSession._touch_md_ack`、`StsSession._refresh_md_ack_from_print`（由 `StsSession._on_market_data` 呼叫，用行情當 ack 的替代訊號）、`StsSession._stale_keys`。
  - `_fail_from_infrastructure` 要說清楚刪到哪裡：§5.4 只寫了 `_fail_from_infrastructure("md feed …")`，但 main 上 `_pump_td_session` 和 `_pump_td_global` 也會呼叫它。依 F14，這三條 pump 路徑都改成通知（B5-05），所以整個方法在本票刪除，三個呼叫點改成記 log。
  - **`on_recon_done` 和 F16 衝突。** `main` @ `a0cbfb2` 上，224 個策略實作測試裡有 **111 個**是靠 `on_recon_done` 把策略驅動起來的：`test_oco.py` 40 個（多半經 `_armed` / `_placed` / `_restore`）、`test_cross_arb.py` 24 個（經 `_armed`）、`test_macd_dollar.py` 21 個（經 `_warm`）、`test_twap.py` 17 個（經 `_arm`）、`test_chase.py` 9 個。F16 說「策略實作測試 RM 不刪」、RM-01 的驗收說「策略測試仍然全綠」，但本票刪掉 `on_recon_done` 之後這 111 個一定會紅。三個選項裡本票採第二個：
    1. 把 111 個測試改寫成在 `on_ready` 驅動——等於提前做 B5-08。
    2. **（採用）** 本票只刪平台側的自動 recon（`_on_lease_ack` 裡的 `send_recon`、`_recon_sent`）與 `Strategy.send_recon`、`STS_RECON`，**把 `Strategy.on_recon_done` 的 hook 定義與 6 支內建策略的實作留到 B5-08**，連同那 111 個測試一起改寫。這樣 RM 的驗收「清單上的符號 grep 不到」要把 `on_recon_done` 排除，並在 B5-08 補上。
    3. 連策略測試一起刪——違反 F16，不採用。
  - `ReconDone` model（`protocol/messages.py:ReconDone`、`ReconDoneEnvelope`）**不刪**：§5.2 保留平台內部 recon，RM-06 也保留 `view(settled=True)` 的等待邏輯。原範圍沒說，這裡補明。
- **驗收：** 共同驗收（`on_recon_done` 除外，見補正）；STS session 不再對 MD / TD 送任何週期性 heartbeat，也不再由平台代替策略發 `sts.recon`。
- **依賴：** B0-05
- **決策：** F13、§8.2

### RM-03 SDK：刪除 `breathe` / `slice_deadline`（#166）

- **範圍：** `strategy/base.py`、`strategy/tape.py`（`breathe`、`slice_deadline`、`SLICE_S`、`tape.read(on_print=…)` 每筆讓出的邏輯）、`strategy/__init__.py` 的匯出、`impl/macd_dollar.py` 的用法、`packages/common/README.md`。
- **呼叫端（`main` @ `a0cbfb2`）：**
  - `breathe`（定義 `packages/common/src/mftik/strategy/tape.py:breathe`）：`strategy/tape.py:StrategyTape.read`、`strategy/tape.py:_flush_log`、`apps/sts/src/mftik_sts/impl/macd_dollar.py:MacdDollarBars._warm_up.on_print`
  - `slice_deadline`（`strategy/tape.py:slice_deadline`）：`strategy/tape.py:breathe`、`strategy/tape.py:StrategyTape.read`、`impl/macd_dollar.py:MacdDollarBars._warm_up`
  - `SLICE_S`（`strategy/tape.py`）：`strategy/tape.py:slice_deadline`
  - 匯出：`strategy/__init__.py` 的 import 與 `__all__`；文件 `strategy/base.py:Strategy` 的 docstring、`packages/common/README.md`
  - `on_print`（`strategy/tape.py:StrategyTape.read` 的參數，型別 `strategy/tape.py:PrintHandler`，交付 `strategy/tape.py:_deliver`）：唯一的生產端呼叫者是 `impl/macd_dollar.py:MacdDollarBars._warm_up`
- **B0-05 補正：**
  - **`on_print` 的去向要先講定。** §5.4 / §5.5 寫的是刪「每筆讓出的邏輯」，沒說參數本身留不留。本票的定案是：**留下 `on_print` 參數和 `PrintHandler`、`_deliver`，只拿掉 `read` 裡的 `slice_deadline` / `breathe` 呼叫**。理由是 `on_print` 本身是「不要把整份 tape 複製成 list」的 API，和讓出 loop 無關，B5-07 還會用到。
  - 原範圍沒列到的測試，本票要一起改：
    - `packages/common/tests/test_strategy_public_api.py`：2 個案例 `test_the_pacing_helpers_are_public`、`test_breathe_yields_only_once_the_slice_is_spent` 直接 `from mftik.strategy import breathe, slice_deadline`。這個檔案不在附錄 A 裡，本票刪掉這 2 個案例（檔案其餘案例保留）。
    - `apps/sts/tests/test_tape_read.py:test_parse_yields_the_loop_between_records` monkeypatch `SLICE_S`，隨讓出邏輯一起刪。同檔另有 15 個案例經 `_read` 用 `on_print=`、以及 `test_omitting_on_print_keeps_the_prints_on_the_slice`，依上面的定案**不受影響**。
    - `apps/sts/tests/test_macd_dollar.py:test_warm_up_ingest_yields_the_loop` monkeypatch `SLICE_S`，是策略實作測試（F16）裡唯一真的會被 RM 弄紅的案例。本票刪掉這一個案例；同檔 `FakeTape.read` 的 `on_print` 參數依定案保留。
  - `strategy/tape.py:_flush_log` 也呼叫 `breathe`（原範圍只提到 `read`）。它是 tape 讀取時寫 log 的路徑，`breathe` 拿掉後改成直接 `await`。
- **驗收：** 共同驗收。`macd_dollar` 原本切片的計算改成一次算完；`offload` 到 B5-03 才有，這段期間接受同步計算。
- **依賴：** B0-05
- **決策：** F9

### RM-04 STS：刪除進程綁定、reaper、雙模式 manager 與 worker 端 DB 存取（#167）

- **範圍：**
  - `apps/sts/src/mftik_sts/spawn.py`：`SubprocessSpawner`、`LIFELINE_FD_ENV`、`MFTIK_STS_PARENT_PID`
  - `apps/sts/src/mftik_sts/worker.py`：`arm_parent_death`、`_watch_lifeline`
  - `apps/sts/src/mftik_sts/session/manager.py`：整份刪除，包括 `reap_orphans`、`_create_in_process`，以及傳給 worker 端的 `persist_live`、`mark_done`、`mark_live`、`load_session`、`list_db_sessions`、`td_instance`、`derive_sts`
  - `app.py` 的對應接線。`sts.{instance}` 的 start / end 改成占位（IF-04）
  - 測試：附錄 A 的 STS 清單
- **留下（之後搬移）：** `session/session.py` 裡扣掉 RM-02 之後的下單與事件分派（B4-03）；`impl/`；`rpc/artifacts.py`、`rpc/eventlog.py`、`rpc/env.py`、`rpc/registry.py`、`registry_catchup.py`、`runtime_env.py`；`app.py` 的 `run_rpc`、`_dispatch_request`、`schema_is_current`、`_schema_wait_s`。
- **呼叫端（`main` @ `a0cbfb2`）：**
  - `SubprocessSpawner`（`spawn.py:SubprocessSpawner`）：`apps/sts/src/mftik_sts/app.py:amain`（`spawner=` 參數）
  - `LIFELINE_FD_ENV`（`spawn.py`）：寫入 `spawn.py:SubprocessSpawner.spawn`；讀取 `worker.py:_lifeline_fd`、`worker.py:_lifeline_already_closed`
  - `PARENT_PID_ENV`（值是 `"MFTIK_STS_PARENT_PID"`，`spawn.py`）：寫入 `spawn.py:SubprocessSpawner.spawn`；讀取 `worker.py:arm_parent_death`
  - `arm_parent_death`：`worker.py:main`；內部呼叫 `worker.py:set_pdeathsig`
  - `_watch_lifeline`：`worker.py:amain`；內部呼叫 `worker.py:_lifeline_eof` → `worker.py:_lifeline_fd`
  - `reap_orphans`（`session/manager.py:SessionManager.reap_orphans`）：`apps/sts/src/mftik_sts/app.py:reap_loop`
  - `_create_in_process`：`session/manager.py:SessionManager.create_session`；收尾在 `SessionManager._close_in_process`、`SessionManager._reap_failed`
  - worker 端 DB 函式（定義都在 `apps/sts/src/mftik_sts/db.py`）：`persist_live_session`、`mark_session_finished`、`mark_session_live`、`load_session`、`list_sessions`、`td_instance`、`derived_sts` —— 兩個接線點 `apps/sts/src/mftik_sts/app.py:amain` 與 `apps/sts/src/mftik_sts/worker.py:amain`，兩者都把它們當建構參數傳給 `SessionManager.__init__`（`persist_live`、`mark_done`、`mark_live`、`load_session`、`list_db_sessions`、`td_instance`、`derive_sts`）。`td_instance` 另外由 `SessionManager._create_in_process` 與 `SessionManager._rebuild_one` 轉交給 `StsSession.__init__`
  - `sts.{instance}` 的 start / end：`apps/sts/src/mftik_sts/rpc/router.py:dispatch` → `rpc/sessions.py`（改成占位，IF-04）
- **B0-05 補正：**
  - 原範圍寫「`MFTIK_STS_PARENT_PID`」，但代碼裡的符號是 `spawn.py:PARENT_PID_ENV`（環境變數名只是它的值）。`spawn.py` 還有一個 `RESULT_FD_ENV`（`worker.py:write_result` 讀取），也隨 `spawn.py` 一起刪。`worker.py` 的 `set_pdeathsig`、`_lifeline_fd`、`_lifeline_already_closed`、`_lifeline_eof` 同上。
  - **`sts_db` 的包裝函式可以刪，`packages/db` 的 repository 方法不能。** `StsSessionRepository.mark_done` / `mark_live` / `list_sessions` / `get_by_session_id` / `create_live` / `mark_finished` / `count_by_instance` / `list_live_for_origin` 還有大量 API 呼叫端：`apps/api/src/mftik_api/routes/sts.py`（`list_strategies`、`get_strategy`、`strategy_yaml`、`ack_session`、`_control`、`_load_sts_row`）、`routes/board.py`（`list_board_sessions`、`get_board_session`、`list_board_fills`、`export_board_fills_csv`）、`routes/stats.py:get_stats`、`routes/registry.py`（`delete_strategy`、`disconnect_remote`）、`ws.py:status_replay`、`alert_match.py:lookup_session_type`、`orchestrate.py:mint_session_id`。本票只動 `apps/sts/src/mftik_sts/db.py` 與接線。
  - `apps/sts/src/mftik_sts/app.py` 要留下 `run_rpc`、`_dispatch_request` 和開機的 schema 守衛（`schema_is_current`、`_schema_wait_s`），否則驗收的「STS 平面能啟動」做不到。附錄 A 原本把 `test_boot_schema_guard`（6 個）和 `test_rpc_loop_survives`（2 個）列進刪除清單，已在附錄 A 定稿時移出。
  - `apps/sts/src/mftik_sts/session/session.py` 在本票之後會留下對 `md.{session_id}`（`Topics.md_session`）的訂閱，而發佈端由 RM-05 刪除。這段死代碼依「留下」刻意保留到 B4-03，不違反共同驗收，但 RM-10 的 `remaining.md` 要記上。
- **驗收：** 共同驗收；STS 平面能啟動，並服務 health、registry、env、artifacts 這些不依賴 session 的 RPC。
- **依賴：** RM-01、RM-02
- **決策：** F6、F10

### RM-05 MD：刪除 session 機制（#168）

- **範圍：**
  - `apps/md/src/mftik_md/session/`（`manager.py`、`dispatcher.py`、`venue.py`、`factory.py`）：per-session fan-out `md.{session_id}`、`VenueSession`、`_expiry_tasks`、執行期 `md.subscribe` 的處理、`reap_orphans`，以及 `persist_live`、`mark_done`、`list_db_sessions` 的接線
  - `apps/md/src/mftik_md/rpc/sessions.py`
  - 測試：附錄 A 的 MD 清單
- **留下：** `fetch/`（B7-05）、`tape.py`、`tape_store.py`、`rpc/tape.py`（B7-04 改 key）、`rpc/health.py`、`rpc/router.py`（扣掉 session handler）、`mftik.exchange.*` 的各 venue adapter（B7-02）。
- **呼叫端（`main` @ `a0cbfb2`）：**
  - `SessionManager`（`apps/md/src/mftik_md/session/manager.py`）：建構於 `apps/md/src/mftik_md/app.py:amain`；`reap_orphans` 由 `app.py:reap_loop` 呼叫；`close_all` 由 `app.py:amain` 收尾；`app.py:run_rpc` 把它交給 `rpc/router.py:dispatch`，再分到 `rpc/sessions.py` 的三個 handler、`rpc/health.py:handle_health` 和 `rpc/tape.py:handle_tape_tail`（後者只用 `SessionManager.tape_store`）
  - `StsLink`（`session/manager.py:StsLink`）：`SessionManager.attach`、`SessionManager._stop_link`、`SessionManager._lease_loop`、`SessionManager._enqueue_feed_op`、`SessionManager._subscribe_runtime`、`SessionManager._unsubscribe_runtime`、`SessionManager._subscribe_feed`、`SessionManager._unsubscribe_feed`、`SessionManager._open_subscribed`、`SessionManager._open_first`；另由 `session/dispatcher.py:Dispatcher.register_link`、`Dispatcher.__init__` 持有
  - `Dispatcher`（`session/dispatcher.py:Dispatcher`）：`SessionManager.__init__`、`SessionManager.dispatcher`
  - per-session fan-out `md.{session_id}`（`Topics.md_session`）：發佈端唯一是 `session/dispatcher.py:Dispatcher.publish`
  - `VenueSession`（`session/venue.py:VenueSession`）：`SessionManager.__init__`（`_venues`）、`SessionManager._ensure_venue`、`SessionManager._disconnect_venue`、`SessionManager._on_feed_end`、`SessionManager._drop_key_locked`、`SessionManager._retire_key`
  - `_expiry_tasks`：`SessionManager.__init__`、`SessionManager._schedule_arm`、`SessionManager._run_expiry`、`SessionManager._expire_ticker`、`SessionManager._disarm_if_idle`、`SessionManager.close_all`
  - `VenuePublicFactory` / `PaperPublicFactory` / `ConnectorFactory`（`session/factory.py`）：`apps/md/src/mftik_md/app.py:amain`；匯出在 `session/__init__.py`
  - `persist_live` / `mark_done` / `list_db_sessions`：接線點 `apps/md/src/mftik_md/app.py:amain`，值來自 `apps/md/src/mftik_md/db.py:persist_live_session`、`mark_session_done`、`list_sessions`
  - `rpc/sessions.py`：`handle_session_attach`、`handle_session_detach`、`handle_session_list`、`_error`；註冊於 `apps/md/src/mftik_md/rpc/router.py:dispatch`
- **B0-05 補正：**
  - 原範圍沒提到 `apps/md/src/mftik_md/app.py`。它是 `SessionManager`、`VenuePublicFactory` 和 `reap_loop` 的唯一接線點，不改的話 MD 平面連啟動都不行。`app.py:reap_loop` 隨 `reap_orphans` 一起刪。
  - 原範圍沒提到 `apps/md/src/mftik_md/rpc/router.py`。它 import `rpc/sessions.py`，而且把 `SessionManager` 傳給 health 與 tape 的 handler。本票要把 router 改成不帶 `SessionManager` 的版本；`rpc/tape.py:handle_tape_tail` 目前用的只有 `sessions.tape_store`，改成直接持有 `TapeStore`。
  - `Topics.md_session` 這個 helper 還有三個非 MD 的引用：`packages/common/tests/test_nats_transport.py` 的 3 個 broker 語意案例、`packages/common/tests/test_query_codes.py:test_a_caller_gets_its_own_reply_channel`、`scripts/loop_bench.py`（`case_fanout`、`case_pipelined_fanout`、`case_subscribe`）。這些只拿它當 subject 名稱產生器。要嘛本票一併改掉，要嘛共同驗收第 1 條對 `Topics.md_session` 放行（`scripts/` 本來就不在 grep 範圍內，但會在執行時壞掉）。
  - `session/venue.py:MarketDataConnector`（protocol）和 `session/venue.py:Feed`、`OnEnd` 隨檔案一起刪；`fetch/` 不依賴它們（`fetch/readers.py` 有自己的 `ReaderFactory` / `VenueReader`），所以「留下 `fetch/`」成立。
- **驗收：** 共同驗收；MD 平面能啟動，並服務 `md.fetch` 和 tape 讀取。
- **依賴：** B0-05
- **決策：** F17、F21、F22

### RM-06 TD：刪除 session manager 的 attach、lease、refcount、reaper（#169）

- **範圍：**
  - `apps/td/src/mftik_td/session/manager.py`：attach / detach、refcount、lease、`reap_orphans`、`persist_live` / `mark_done` / `list_db_sessions` 的接線
  - `_handle_recon`：等 settled 的邏輯抽成獨立函式保留下來，留給 IF-11 的 `view(settled=True)`；其餘刪除
  - `apps/td/src/mftik_td/rpc/sessions.py`
  - 測試：附錄 A 的 TD 清單
- **留下：** `session/session.py`（每個帳號的 OMS、ledger、recon、下單，B4-05 搬進交易層）、`session/factory.py`（`SessionFactory`、`VenueSessionFactory`、`PaperSessionFactory`——建構 `Session` 的那一層，B4-05 搬進常駐層）、`oms/`、`backfill/`（B6-05）、`history.py`、`publish/`、`rpc/health.py`。
- **呼叫端（`main` @ `a0cbfb2`，以下未註明檔案者都在 `apps/td/src/mftik_td/session/manager.py`）：**
  - `SessionManager`：建構於 `apps/td/src/mftik_td/app.py:amain`；`reap_orphans` 由 `app.py:reap_loop` 呼叫；`active_api_ids` 由 `app.py:amain` 的 health `describe` 用；`close_all` 由 `app.py:amain` 收尾；`app.py:run_rpc` 把它交給 `rpc/router.py:dispatch`，再分到 `rpc/sessions.py` 與 `rpc/health.py:handle_health`
  - `attach`：`apps/td/src/mftik_td/rpc/sessions.py:handle_session_attach`；別名 `SessionManager.create_session`
  - `detach`：`apps/td/src/mftik_td/rpc/sessions.py:handle_session_detach`
  - `refcount`：`TradingAccount.refcount`（property）、`SessionManager.refcount`；讀取端 `apps/td/src/mftik_td/rpc/sessions.py:handle_session_detach`
  - lease：`SessionManager._lease_loop`（內含 `_on_heartbeat`、`_on_message`、`_expire`、`_died`、`_ack`）由 `SessionManager.attach` 啟動；`SessionManager._stop_link` 收尾；`SessionManager._strike` 與 `SessionManager.reap_orphans` 讀 lease loop 的存活
  - `persist_live` / `mark_done` / `list_db_sessions` / `td_instance`：接線點 `apps/td/src/mftik_td/app.py:amain`，值來自 `apps/td/src/mftik_td/db.py:persist_live_session`、`mark_session_done`、`list_sessions`、`instance_name`；`mark_done` 的呼叫點在 `SessionManager.detach`
  - `_handle_recon`：`SessionManager._lease_loop._on_message`（`env.type == STS_RECON`）。要抽出保留的等待邏輯牽到 `SessionManager._arm_recon_deadline`、`SessionManager._flush_recon_waiters`、`SessionManager._publish_recon_done`、`SessionManager._on_order_settled`、`TradingAccount.recon_waiters`
  - `rpc/sessions.py`：`handle_session_attach`、`handle_session_detach`；註冊於 `apps/td/src/mftik_td/rpc/router.py:dispatch`
- **B0-05 補正：**
  - 原範圍沒提到 `apps/td/src/mftik_td/app.py`（`SessionManager`、`VenueSessionFactory`、`reap_loop` 的唯一接線點）與 `apps/td/src/mftik_td/rpc/router.py`（import `rpc/sessions.py`、把 `SessionManager` 傳給 health）。兩者都要改，否則 TD 平面起不來。
  - **原範圍漏掉 manager.py 裡的下單與帳號 RPC。** 附錄 A 把 `test_order_rpc`（46）、`test_session_oms`、`test_session_leverage`、`test_stream_rejects`、`test_cid_ownership`、`test_leverage_rpc` 列進刪除清單，但這些測的是 `SessionManager._serve_orders`、`_serve_account`、`_dispatch_account_rpc`、`_handle_order_rpc`、`_handle_order_submit`、`_handle_order_cancel`、`_handle_account_rpc`、`_handle_ledger_view`、`_handle_oms_view`、`_handle_oms_order`、`_reply_order_ack`、`_reply_leverage_ack`、`_on_order_settled`，以及模組層的 `_wrong_instrument`、`_place_order_request`、`_refusal_code`、`_reduce_only_unsupported`。它們都在 `manager.py`，由 attach 啟動，所以會隨本票消失。範圍要明寫它們刪除、由 IF-11 接介面、B6-02 / B6-08 重新實作，否則會出現「代碼還在、測試沒了」的狀態。
  - `session/factory.py`（含 `VenueSessionFactory`、`PaperSessionFactory`、`SessionFactory`）原本不在刪除清單也不在留下清單。它是「`api_id` → venue client → `Session`」那一層，F35 的常駐層需要它，所以列入「留下」。對應的 `apps/td/tests/test_venue_factory.py`（17 個）一併從附錄 A 移出。
  - `td_sessions` 表的讀取端不動：`apps/api/src/mftik_api/routes/td.py:list_sessions`、`routes/apis.py:delete_api`、`routes/stats.py:get_stats` 都還在用 `TdSessionRepository`，F38 說它從 B10 起停寫、保留唯讀。本票只刪 TD 平面側的寫入接線。
- **驗收：** 共同驗收；`session/session.py` 仍能單獨建構，並以 paper 跑 OMS / ledger 的單元測試。
- **依賴：** B0-05
- **決策：** F34、F35、F36

### RM-07 刪除 lease 協定與 `LeasedSessionLink`（#170）

- **範圍：** `packages/common/src/mftik/broker/link.py`（`LeasedSessionLink`），以及 `broker/client.py`、`broker/transport/base.py`、`broker/__init__.py` 的相關部分；protocol 的 `STS_LEASE_HEARTBEAT`、`MD_LEASE_ACK`、`TD_LEASE_ACK`、`LeaseHeartbeat` 與對應的 envelope；`test_broker.py` 的 leased link 案例、`test_lease_resilience`、`test_md_lease_resilience`。
- **呼叫端（`main` @ `a0cbfb2`）：**
  - `LeasedSessionLink`（`packages/common/src/mftik/broker/link.py:LeasedSessionLink`）：`broker/client.py:Broker.leased_link`；直接建構的有 `apps/md/src/mftik_md/session/manager.py:SessionManager._lease_loop`、`apps/td/src/mftik_td/session/manager.py:SessionManager._lease_loop`。匯出在 `broker/__init__.py`，`broker/transport/base.py` 的 docstring 引用它
  - `LeaseHeartbeat`（`packages/common/src/mftik/protocol/messages.py:LeaseHeartbeat`）：發送端 `apps/sts/src/mftik_sts/session/session.py:StsSession._lease_heartbeat_loop`；解析端 `broker/link.py:LeasedSessionLink.run._pump`；ack 工廠 `md/session/manager.py:SessionManager._lease_loop._ack`、`td/session/manager.py:SessionManager._lease_loop._ack`、`td/session/manager.py:SessionManager._lease_loop._on_heartbeat`、`md/session/manager.py:SessionManager._lease_loop._on_heartbeat`；型別別名 `broker/link.py` 的 `AckFactory`、`HeartbeatHook`
  - `STS_LEASE_HEARTBEAT`：`broker/link.py:LeasedSessionLink.run._pump`、`apps/sts/.../session/session.py:StsSession._lease_heartbeat_loop`
  - `MD_LEASE_ACK`：發送 `md/session/manager.py:SessionManager._lease_loop._ack`；接收 `apps/sts/.../session/session.py:StsSession._pump_md_session`
  - `TD_LEASE_ACK`：發送 `td/session/manager.py:SessionManager._lease_loop._ack`；接收 `apps/sts/.../session/session.py:StsSession._pump_td_session`
  - `test_broker.py` 的 leased link 案例共 2 個：`test_leased_link_acks_and_expires`、`test_leased_link_does_not_expire_before_first_heartbeat`（同檔其餘 6 個是 broker 語意，依 §9.3 留作連線測試）
- **B0-05 補正：** 原範圍沒列到的 lease 型別，一併刪除並從 `protocol/__init__.py` 的 import 與 `__all__` 移除：`LeaseAck`（`protocol/messages.py:LeaseAck`，用於 `td/.../manager.py:SessionManager._lease_loop._ack` 與 `apps/sts/.../session/session.py:StsSession._on_lease_ack`）、`MdLeaseAck`（`protocol/messages.py:MdLeaseAck`，用於 `md/.../manager.py:SessionManager._lease_loop._ack` 與 `StsSession._on_md_lease_ack`）、envelope 別名 `LeaseHeartbeatEnvelope`、`LeaseAckEnvelope`、`MdLeaseAckEnvelope`，以及舊名別名 `STS_HEARTBEAT`（`protocol/messages.py`，等於 `STS_LEASE_HEARTBEAT`）。`protocol/messages.py:MdAttachResult` 的 docstring 也引用 `MdLeaseAck`。
- **驗收：** 共同驗收；protocol 和 broker 裡沒有任何 lease 相關的型別或函式。
- **依賴：** RM-02、RM-05、RM-06
- **決策：** §8.2

### RM-08 API：刪除同步 deploy 與它的回滾（#171）

- **範圍：** `apps/api/src/mftik_api/orchestrate.py` 的 `deploy_strategy`（同步執行 create → MD attach → TD attach，create 的 RPC timeout 寫死 10 秒，也就是 #132 的成因），以及它失敗時的回滾 `_detach_md`、`_fail_sts`。`routes/sts.py` 的 deploy 路由改成回 501，等 IF-13。測試：附錄 A 的 API 清單。
- **留下（IF-13 重用）：** `mint_session_id`、`_sts_target`、`_check_sts_instance`、`_check_md_instances`、`_answers`、`_td_instance`、`_md_venues`。這些是 §8.1 第 1 步的驗證。
- **呼叫端（`main` @ `a0cbfb2`）：**
  - `deploy_strategy`（`apps/api/src/mftik_api/orchestrate.py:deploy_strategy`）：唯一的生產端呼叫者是 `apps/api/src/mftik_api/routes/sts.py:deploy`
  - `_detach_md`、`_fail_sts`：唯一呼叫端都是 `orchestrate.py:deploy_strategy`
  - `deploy_strategy` 內的巢狀 `sts_log` 隨之刪除
  - 留下清單的現有呼叫端（都在 `orchestrate.py:deploy_strategy` 裡，IF-13 要重接）：`mint_session_id`、`_sts_target`、`_check_sts_instance`、`_check_md_instances`（它呼叫 `_answers`）、`_td_instance`、`_md_venues`（兩處）；`_check_sts_instance` 也呼叫 `_answers`
  - `_md_venues` 除 `deploy_strategy` 之外沒有其他呼叫端，`_check_md_instances` 也一樣——留下是為了 IF-13 重用，不是因為有別的呼叫者
- **B0-05 補正：** 附錄 A 原本把 `test_registry_add`（23）、`test_environment_flow`（19）、`test_td_instance_routing`（4）、`test_td_sessions_route`（3）整個檔案列進刪除清單，但實測只有 `test_registry_add` 的 3 個（`test_incompatible_environment_deploy_is_409`、`test_unknown_strategy_deploy_is_still_404`、`test_cross_arb_deploy_refuses_sts_account_not_in_td`）和 `test_environment_flow` 的 3 個（`test_s1_bare_node_stdlib_tree`、`test_s2_declare_then_apply_then_add`、`test_s6_already_connected_can_pull_a_heavier_tree`）會碰到 deploy 路由；`test_td_instance_routing` 的 4 個用的是留下的 `_td_instance` 與 `backfill_cron.sweep`，`test_td_sessions_route` 的 3 個只讀 `td_sessions`。剩下的 36 個測的正是本票驗收要求「照常運作」的 registry add 與 environment declare/apply/push。附錄 A 已依此定稿。
- **驗收：** 共同驗收；API 其他路由（auth、alerts、artifacts、registry、board、logs）照常運作。
- **依賴：** B0-05
- **決策：** F12、§8.1

### RM-09 刪除 strategy.yml 與 CLI 的舊欄位（#172）

- **範圍：** `protocol/strategy_yml.py` 的 `RESTART_ALWAYS`（rebuild 語意的 `restart: always`，也是 `StrategySpec.restart` 的預設值）；`cli/run.py` 的 `deploy_http_timeout` 和它的預算常數 `_STS_CREATE_S`、`_ATTACH_RPC_SLACK_S`、`_DEFAULT_ATTACH_S`、`_HTTP_SLACK_S`。新欄位在 IF-07 加。
- **呼叫端（`main` @ `a0cbfb2`）：**
  - `RESTART_ALWAYS`（`packages/common/src/mftik/protocol/strategy_yml.py`）：`strategy_yml.py` 的 `RESTART_MODES`、`strategy_yml.py:StrategySpec._restart_mode`（`value is None` 時的回傳值）；欄位預設值是字面量 `StrategySpec.restart = "always"`；匯出在 `protocol/__init__.py` 的 import 與 `__all__`。rebuild 端的讀取者是 `apps/sts/src/mftik_sts/session/manager.py:SessionManager._rebuild_claimed`（`row.restart != "always"`），由 RM-01 刪除
  - `deploy_http_timeout`（`packages/common/src/mftik/cli/run.py:deploy_http_timeout`）：唯一呼叫端 `cli/run.py:run`（`connected(..., timeout=...)`）
  - `_STS_CREATE_S`、`_ATTACH_RPC_SLACK_S`、`_HTTP_SLACK_S`：只在 `cli/run.py:deploy_http_timeout` 裡；`_DEFAULT_ATTACH_S` 是它的 `attach_s` 預設值
- **B0-05 補正：**
  - 原範圍沒說 `cli/run.py:run` 改用什麼 timeout。deploy 在 F12 之後回 202，所以 `run` 改成固定的短 HTTP timeout（具體值由 IF-15 的 `--wait` / `--no-wait` 一併定），不再依 spec 估算。
  - `packages/common/tests/test_cli_run.py` 的 2 個案例 `test_deploy_http_timeout_with_no_attaches`、`test_deploy_http_timeout_with_one_feed` 直接 import `deploy_http_timeout`。這個檔案不在附錄 A，本票刪掉這 2 個案例。
  - `RESTART_NEVER` 和 `RESTART_MODES` 保留（`never` 在 F11 是新語意的預設值），但 `RESTART_MODES` 的內容和 `StrategySpec.restart` 的預設值要在 IF-07 改。
- **驗收：** 共同驗收；`mftik check` 遇到含舊欄位的文件時，給出明確的錯誤訊息，並告訴使用者新寫法。
- **依賴：** RM-08
- **決策：** F11、F12

### RM-10 清場後的基線（#173）

- **描述：** 確認 RM 之後還剩下什麼，作為 B2 和 IF 的起點。
- **驗收：**
  - 附錄 A 的測試全部刪除或改寫；剩餘的測試數和各模組耗時（GitHub Actions）記在附錄 C 的「RM 之後」欄。
  - `docs/baseline/remaining.md` 列出每個平面還剩哪些模組，以及各自的去向：搬進哪個新的層，或保留不動。
  - 打 tag `arch/cleared`。
- **依賴：** RM-01 到 RM-09

---

## B2 測試標準

### B2-01 `TESTING.md`（#174）

- **驗收：** 寫明 tier（§9.1）、規則（§9.2）、預算與閘門（F30）、handler 和傳輸分開的寫法（F31），以及 IF 的 `xfail(strict=True)` 契約測試慣例；每條規則附一個範例。
- **依賴：** —

### B2-02 `Clock` / `FakeClock`（#175）

- **描述：** §9.2 規則 1。這也是第一個新增的抽象層（§3.4）。
- **範圍：** `mftik.clock`；conftest 在 unit 和 component tier 攔截 `asyncio.sleep(x > 0)`。
- **驗收：** `FakeClock.advance()` 能推進 `sleep` 和 timer；unit 測試裡呼叫真的 sleep 會失敗，並指出呼叫位置。
- **依賴：** RM-10

### B2-03 共用 NATS 連線的 fixture（#176）

- **範圍：** `broker_harness`：每個 xdist worker 只連一次 NATS（session 級 fixture，pytest-asyncio 用 session 級 event loop）；每個測試拿到自己 `key_prefix` 的 broker；teardown 時退訂該 prefix 下的訂閱，不關連線。
- **驗收：** broker 語意測試（`test_nats_transport`、`test_broker*`）改用新 fixture 後全綠；整套測試期間的 NATS 連線數等於 xdist worker 數，以 NATS monitoring 的 `/connz` 驗證。
- **依賴：** RM-10
- **決策：** F31

### B2-04 tier、平行化、閘門與 CI（#177）

- **範圍：** marker（`component`、`integration`、`e2e`）、`pytest-xdist`、`pytest-timeout`、`just test` / `just test-int`；CI 拆成 unit + component 和 integration 兩個 job；120 秒與單一 unit 測試 50 ms 的閘門。
- **驗收：** 故意放慢的測試會讓 CI 失敗；`just test` 在 GitHub Actions 上少於 120 秒。
- **依賴：** B2-02、B2-03
- **決策：** F30

### B2-05 處理剩下借 NATS 測行為的測試（#178）

- **描述：** RM 之後還剩 250 個借 NATS 測行為的測試（`test_plane`、`test_backfill_executor`、`test_tape_read`、`test_ledger_view`、`test_md_fetch`、`test_venue_factory` 等）。之後會重寫的模組，先標成 integration；不在重構範圍、拆開需要改模組本身的（例如 SYM 的 `test_plane`），也先標成 integration。數字依附錄 A 的 B0-05 定稿版；票面原本寫的 129 是依初版整檔清單算的。
- **驗收：** 每個測試都有 tier；`just test` 裡沒有借 NATS 測行為的測試；每個標成 integration 的測試，都註明之後由哪張票改寫。
- **依賴：** B2-04
- **決策：** F31

---

## IF 介面

每張票對應 §3.4 的一層。除了特別註明「這張票是真的實作」的，都只定義介面、回傳 null data。

### IF-01 協定 v2 的型別（#179）

- **範圍：** `mftik.protocol`：envelope 加 `pv`；`md.intent.put` / `delete` / `patch`、`td.intent.put` / `delete`、`sts.session.start` / `end` / `status`、`md.a.*`（atom subject 與 hash）、`md.w.*`、`md.universe.*`、`td.account.state.*`、`td.account.reset`、`td.order.cancel_session`、`procman.report.*` 的 model 與 `Topics`；reject code `protocol_mismatch`。
- **驗收：** 共同驗收；B0-03 盤點裡標成「保留」或「改名」的型別，都對到新的名字；契約測試：`pv` 不符時，收件端以 `protocol_mismatch` 拒絕。
- **依賴：** RM-10、B0-03
- **決策：** F26、§8.3

### IF-02 `mftik.broker.handler`（#180）

- **範圍：** `Handler` 協定（收到解碼後的訊息 → 回覆與副作用）、`serve(broker, subject, handler)`。
- **驗收：** 共同驗收；挑一個現有的小 RPC（例如 health）改用這個介面，當作範例。
- **依賴：** RM-10
- **決策：** F31

### IF-03 `mftik.procman`（#181）

- **範圍：** `WorkerSpec`（§4.3）、狀態機的 enum 與轉移表、`Supervisor`（`start`、`close(mode)`、`spawn`、`stop`、`status`、`report`）、shim 的 NDJSON 訊息（`status`、`signal`、`watch`、`release`）、exit 紀錄格式、`procman.report` 的 payload。
- **驗收：** 共同驗收；契約測試涵蓋 shim 不變式 S1 到 S7、§4.4 的 reattach 對帳表、FAILED 與 CRASHED 的區分、重啟 intensity。
- **依賴：** RM-10
- **決策：** F6、F7、F29

### IF-04 STS controller（#182）

- **範圍：** `mftik_sts.controller`：`StsOrchestrator.reconcile(spec, status) -> actions`、crash 分類（A、B、C）、重啟策略（`never` / `on_failure`、`max_restarts`、`restart_window_s`、backoff）、`sts.{instance}` 的 start / end / list handler。
- **驗收：** 共同驗收；契約測試涵蓋 R1 到 R4、F11 的每一條規則、`restarting` 期間 intent 不回收。
- **依賴：** IF-01、IF-02、IF-03
- **決策：** F10、F11、F12

### IF-05 STS session worker（#183）

- **範圍：** `mftik_sts.session_worker`：`Ingress`、`StrategyRunner`、生命週期階段 0 到 6 的 enum、交付策略（`latest`、kline、`all`）、事件的 `recv_ts` / `seq` / `age`、event log 標記（`delivered`、`superseded`、`dropped`）、hook 時間預算的回報格式。
- **驗收：** 共同驗收；契約測試涵蓋 I1 到 I4、F15 表的每一列、§5.3 的交付策略表、TD 事件溢出時 fail。
- **依賴：** IF-01、IF-02
- **決策：** F8、F15、F23、F25

### IF-06 SDK 表面（#184）

- **範圍：** `mftik.strategy`：
  - hook：`on_ready(ready)`（含 `ready.missing_feeds`）、`on_md_update`、`on_td_update`、`on_resync`、`on_universe_change`
  - 呼叫：`self.offload`、`self.offload_pool`、`self.oms.view(settled=)`、`self.md.state` / `universe` / `current` / `subscribe`、`self.td.state`
  - 例外與計數：`NotReady`、`OffloadWorkerLost`、`HookSlow`
  - `StrategyHarness` 的 API
  - hook 預設是 no-op，SDK 呼叫回傳 null data
- **驗收：** 共同驗收；所有內建策略仍能 import 和實例化；契約測試涵蓋：`on_ready` 之前下單拋 `NotReady`、I-SEL1、帳號 `unavailable` 時下單在本地回 False（`td_unavailable`）。
- **依賴：** RM-01、RM-02、RM-03
- **決策：** F9、F12、F13、F14、F33

### IF-07 strategy.yml v2（#185）

- **範圍：** `protocol/strategy_yml.py`：`restart: never | on_failure`、`max_restarts`、`restart_window_s`、`start_timeout_s`（預設 60、上限 3600）、`ready_timeout_s`（30）、`limits`（`memory_mb`、`offload_threads`、`offload_processes`、`offload_memory_mb`）、每個 feed 的交付策略覆寫、`md:` 裡的 `select:`（`option_chain`、`rolling_future`）。
- **驗收：** **這張票是真的實作**（只是 schema）：§6.4 的 YAML 範例能解析；非法值有明確的錯誤訊息；`mftik check` 認得新欄位。
- **依賴：** RM-09
- **決策：** F9、F11、F12、F33

### IF-08 MD adapter 的 atom 介面（#186）

- **範圍：** `Atom`、`AtomPlan`、`atoms_for(topic, ticker, opts)`、`decode(atom, frame) -> list[Event]`、`capacity(endpoint)`、`join_policy(atom)`；venue 中立的 `TickerStats` model；每個 venue 一個空的 `atoms.py`。
- **驗收：** 共同驗收；契約測試：Binance UM 的 `ticker` 解析成 `@ticker` 加 `@bookTicker` 兩個 atom；Deribit 的 `ticker.*` 一個 frame 產出 Ticker、Greeks、OpenInterest。
- **依賴：** RM-05
- **決策：** F19、F21

### IF-09 MD controller 與 selector（#187）

- **範圍：**
  - `mftik_md.controller`：`MdOrchestrator`（desired atom 由 intent、常駐訂閱、selector 組成，並記錄 owner 集合）、`place(desired, conns, capacity) -> placement`（黏性、不遷移）、`generation = (controller_epoch, seq)`、到期
  - `mftik_md.selector`：`evaluate(listing, ref, now, prev) -> Selection | Hold`、`OptionChainSpec`、`RollingFutureSpec`
- **驗收：** 共同驗收；契約測試涵蓋：placement 不搬 atom（F22）、selector 的防抖動、`min_tte`、轉倉時舊合約保留到到期、fail-static。
- **依賴：** IF-01、IF-08
- **決策：** F17、F18、F22、F33

### IF-10 MD 連線 worker（#188）

- **範圍：** `mftik_md.conn`：`ConnWorker`、`Reconciler`（純函數 `reconcile(desired, observed) -> actions`）、狀態廣播、tape append 的介面、原地重啟的入口。
- **驗收：** 共同驗收；契約測試涵蓋：舊 epoch 的 ack 被丟棄、重連後 observed 歸零並補齊、book 缺口只 resync 單一 atom、`seq` 在同一個 incarnation 內連續。
- **依賴：** IF-01、IF-08
- **決策：** F17、F18、F20、F21、F24、F25

### IF-11 TD 帳號 worker（#189）

- **範圍：** `mftik_td.account`：
  - `ResidentLayer`：HTTP 連線池、保活的 hook、backfill handler
  - `TradingLayer`：`activate()` / `deactivate()`
  - `td.order.*`、`td.oms.*`、`td.ledger.*`、`cancel_session`、`oms.view(settled)` 的 handler
  - 狀態廣播
  - `DeadMansSwitch` 介面，每個 venue 一個實作位置
- **驗收：** 共同驗收；契約測試涵蓋：交易層開關不影響常駐層、`cancel_session` 的確認語意、`settled=True` 會等 UNKNOWN 的單收斂。
- **依賴：** IF-01、IF-02、RM-06
- **決策：** F34、F35、F37

### IF-12 TD controller（#190）

- **範圍：** `mftik_td.controller`：`desired_accounts`（本 instance 名下所有啟用的帳號）、intent → 交易層開關（level-triggered）、drain-replace 的入口。
- **驗收：** 共同驗收；契約測試涵蓋：controller 不在時 worker 維持最後一份 desired（P5）、新 incarnation 只在舊 PID 消失後才啟動（F36）。
- **依賴：** IF-03、IF-11
- **決策：** F27、F35、F36

### IF-13 API start / end 與 intent repository（#191）

- **範圍：** `mftik_api.orchestrate`：`start(spec)` 回 202 `{session_id, status}`、`end(session_id, reason)`；intent repository（`put`、`delete`、`patch`、`release`）；`routes/sts.py` 的 deploy 路由接上，回 202 和 null 的進度。
- **驗收：** 共同驗收；`contracts/openapi.json` 已更新，CI 的 contracts 檢查通過；契約測試涵蓋 §8.1 的 start / end 流程，以及啟動失敗時不回滾。
- **依賴：** IF-01、IF-14
- **決策：** F12、F38

### IF-14 DB schema（#192）

- **範圍：** `mftik_db`：
  - `sts_sessions` 的 Spec / Status 欄位：`generation`、`observed_generation`、`worker_incarnation`、`conditions`、`restart_count`
  - 新表：`md_intents`、`td_intents`（都含 `released_at`）、`md_standing_subscriptions`、selector 狀態
  - `apis` 的帳號設定（cancel-on-disconnect）
  - alembic migration 只加不刪，刪除留給 B10-01
- **驗收：** **這張票是真的實作**：migration 在 sqlite 和 Postgres 上都能 upgrade；CI 的 *Migrations match the models* 通過。
- **依賴：** RM-10
- **決策：** F11、F33、F37、F38

### IF-15 CLI（#193）

- **範圍：** `mftik run --wait / --no-wait`、`mftik workers [--stale]`、`mftik md restart <conn>`、`mftik td drain <api_id>`、`mftik intents gc --instance <name>`。先只印 not implemented。
- **驗收：** 共同驗收；`mftik --help` 列出新指令。
- **依賴：** IF-01
- **決策：** F12、F24、F27、F32

### IF-16 STS 代碼身分與主機磁碟（#275）

- **描述：** F39、F40（§5.7）。RM-10 留下的兩題：策略 registry 與代碼版本的權威，以及 operator 寫 artifact 的路徑歸誰。
- **範圍：**
  - `mftik_sts.hostdisk`：
    - `TreeReplica`：`put(digest, files)`、name → digest 的索引、`path_of(digest)`、`gc(keep)`
    - 版本釘住：從本 instance 非 terminal 的 SessionSpec 算出要保留的 digest 與 env generation
    - `deployable(spec) -> Deployability`：digest 和 generation 在不在這台磁碟上、`requires` 和 extras 是否相符，不 import
    - `probe(digest, env_generation) -> ProbeResult`：一次性子進程，import 後回報
  - `WorkerSpec.labels` 的 key：`strategy_digest`、`env_generation`
  - SessionSpec 的 `strategy_digest`、`env_generation` 欄位。IF-14 還沒合併就併進它的 migration，已經合併就另加一個只加不刪的 migration
  - controller 上 `sts.registry.sync`、`sts.registry.reload`、`api.registry.catchup`、`sts.env.sync` 的 handler 簽名
- **驗收：** 共同驗收；契約測試涵蓋：
  - push 同名新版本之後，被釘住的舊 digest 仍然載入得到
  - GC 不刪被釘住的 digest，也不刪被釘住的 env generation
  - 重新掛起用 SessionSpec 釘住的 digest，不用索引裡的目前版本
  - `requires_mftik` 和當下 release 不相容時，重新掛起記為 failed
  - controller 進程的 `sys.modules` 裡沒有任何策略樹的模組
  - 探測子進程 import 失敗時，以 `skipped` 帶原因回報
- **依賴：** IF-03、IF-04、IF-14
- **決策：** F39、F40

---

## B3 procman

### B3-01 shim（#194）

- **範圍：** 只用 Python 標準庫（F29）：double-fork 加 `setsid`、`PR_SET_CHILD_SUBREAPER`、對 worker 設 `PDEATHSIG`、`oom_score_adj`、`RLIMIT_DATA`、stdio 與 status pipe 的 log 輪替、NDJSON socket、`exit.json`、SIGTERM 轉送。
- **驗收：** IF-03 裡 S1 到 S7 的契約測試轉綠（integration tier，真的子進程）；shim 的 RSS 實測值寫進 §4.7。
- **依賴：** IF-03、B2-04

### B3-02 Supervisor 狀態機與重啟策略（#195）

- **驗收：** 狀態機、FAILED / CRASHED 的區分、backoff、intensity、heartbeat 逾時的契約測試轉綠。
- **依賴：** B3-01

### B3-03 detach / reattach（#196）

- **範圍：** `supervisor.json`、socket 掃描、`/proc` 掃描（避免重複 spawn，F36）、§4.4 的對帳表。
- **驗收：** integration：controller 以 detach 結束後 worker 存活；新的 controller reattach，而且不會重複 spawn；殺掉 shim 後 worker 自行 graceful stop。
- **依賴：** B3-02

### B3-04 `procman.report` 與 RSS（#197）

- **驗收：** 報告包含 worker 集合、generation，以及每個 worker 整棵子進程樹的 RSS；controller 滾動期間報告暫停。
- **依賴：** B3-02、IF-01

### B3-05 准入控制（#198）

- **驗收：** 超過 `max_workers` 或 `memory_budget_mb` 時，以 `capacity_exceeded` 拒絕，不先啟動 worker。
- **依賴：** B3-04
- **決策：** F7

### B3-06 在 strategon#60 上做實機驗證（#199）

- **描述：** 外部依賴 strategon#60（S-1 到 S-3）。
- **驗收：** 在 cp 和 yite 上以 `oci_host_pid` 實際滾動一次 controller，確認：worker 存活；release GC 不刪仍在使用的 rootfs；重啟 agent 不影響任何 strategy；新舊版本的 worker 能並存；`/proc/<pid>/oom_score_adj` 符合 §4.7。
- **依賴：** B3-03、strategon#60
- **決策：** F6

### B3-07 release pin 與代碼版本（#200）

- **範圍：** 讀取 `STRATEGON_RELEASE_VERSION`、寫 pin 檔（S-2 的 fallback）、`WorkerSpec.code_ref`；作為 `mftik workers --stale` 的資料來源。
- **驗收：** CLI 列出每個 worker 的代碼版本；仍有 worker 在跑的 release 都在 pin 檔裡。
- **依賴：** B3-03、IF-15
- **決策：** F24、F27

---

## B4 端到端骨架（只接 paper）

### B4-01 協定 v2 接上 broker（#201）

- **驗收：** IF-01 的 `pv` 契約測試轉綠；所有平面送出的訊息都帶 `pv`。
- **依賴：** IF-01、B2-04

### B4-02 STS controller（#202）

- **範圍：** SessionSpec 的 reconcile、start / end、和 Supervisor 的整合、status 與 conditions 的寫入、`sts.status.{session_id}`。
- **驗收：** IF-04 裡和啟動、停止相關的契約測試轉綠。crash 與重新掛起留給 B5-06。
- **依賴：** IF-04、B3-03、B4-01

### B4-03 STS session worker（雙 thread）（#203）

- **範圍：** ingress / strategy thread、階段 0 到 6、`on_start` 獨佔、readiness gate（MdReady 軟、TdReady 硬）、`NotReady`、直接 publish 加強制 flush、ack 經 inbox 和 `call_soon_threadsafe` 交回、ingress 對 shim 的 heartbeat；把 `session/session.py` 留下的下單與事件分派搬進來。
- **驗收：** I1 到 I4 的契約測試轉綠；一個 30 秒 CPU-bound 的 hook 不會讓 session fail、不會讓 NATS 斷線、不會造成假的 ack timeout。
- **依賴：** IF-05、IF-06、B4-02
- **決策：** F3、F8、F12

### B4-04 實測三件事（#204）

- **驗收：** 結果記進 §5.3：
  1. `reply` 不屬於送出連線時，no-responders 是否照常送達。
  2. ack 回程的跨 thread 延遲。
  3. GIL switch interval 要不要調。
- **依賴：** B4-03

### B4-05 TD 帳號 worker（paper）（#205）

- **範圍：** 把 `session/session.py` 搬進交易層；paper 的常駐層；`td.order.*` 的 handler。
- **驗收：** paper 上 `td.order.*` 的契約測試轉綠。
- **依賴：** IF-11、IF-12、B3-03

### B4-06 MD 連線 worker（paper）（#206）

- **驗收：** paper 的 atom 發佈到 `md.a.*`，session 收得到。
- **依賴：** IF-08、IF-10、B3-03

### B4-07 MD / TD controller 的最小版（#207）

- **範圍：** intent 的 put / delete；依 `procman.report` 回收（§8.2 規則 3）。
- **驗收：** session 結束後幾秒內 intent 被回收；`restarting` 期間不回收；STS controller 的報告停止時不回收任何東西（F32）。
- **依賴：** IF-09、IF-12、B3-04

### B4-08 API start / end 與 `mftik run --wait`（#208）

- **驗收：** deploy 回 202；`mftik run` 追蹤到 `running` 或 `failed` 後 tail log。
- **依賴：** IF-13、IF-14、IF-15、B4-02

### B4-09 骨架驗收（#209）

- **驗收：** 在 compose 上：
  - paper 的 deploy → `on_start` → `on_ready` → 下單 → 成交回報 → end，全程走新路徑。
  - 三個 controller 各自滾動，都不中斷。
  - 各 kind 的 RSS 實測寫進 §4.7，並調整初始值。
  - 超過預算的 start 被拒。
- **依賴：** B4-03 到 B4-08、B3-05

### B4-10 `pv` 檢查：deploy 時比對與 NATS header（#363）

- **描述：** #282 的定案（F41）。`pv` 從 envelope 搬到 NATS header，由 transport 在解碼之前擋；另在 deploy 時先比對 session 會用到的 controller 與 worker，讓執行中的丟棄只是最後一道防線。
- **範圍：**
  - transport（`broker/transport/nats.py`）：`publish`、`publish_with_reply`、`request` 一律蓋上 header `Mftik-Pv`。每個入站 frame（`subscribe`、`subscribe_core`、`serve` 收到的 request、`request` 收到的 reply）在 decode 之前檢查 header；缺少或不符就丟棄，記 log（依 subject 與 `pv` 限流），以 `(subject, pv)` 計數，並記下每個 subject 最近一次被丟棄的 `pv`，給 deploy 時的比對用
  - `request` 收到 `pv` 不符的 reply 時，對呼叫端拋出本地錯誤（暫名 `ProtocolMismatch`），不等 timeout；線上不送任何錯誤
  - 刪除 `Envelope.pv`、`reject_if_pv_mismatch`、STS ingress 的 `_pv_ok`、`_on_ctl` 與 `procman_reports.py` 裡的 `pv` 比對；API orchestrate 改接 transport 的本地錯誤，回 `protocol_mismatch`。`_on_md` 每個 frame 只剩一次 `json.loads`（F8）
  - `WorkerSpec` 加 `pv`：spawn 時填入 controller 的常數，跟著 spec 持久化，reattach 後不變；`ProcmanWorker` 帶出 `pv`；controller 自己的丟棄計數放進 `procman.report`
  - API start：把 `PROTOCOL_VERSION` 和目標 STS controller、session 會用到的 MD / TD controller、session 的 TD 帳號 worker（依 `api_id`）比對。任何一個不符，就以 `protocol_mismatch` 拒絕，訊息指出是哪個元件、它的 `pv`；不 spawn worker、不寫 intent
  - `scripts/` 裡直接用 `nats` 的腳本改走 transport，或自己蓋 header
- **驗收：**
  - `git grep` 在 `apps/`、`packages/` 找不到 `Envelope` 的 `pv` 欄位、`reject_if_pv_mismatch`、`_pv_ok`
  - integration：對 `md.a.*`、一個 `serve` subject、一個 request 的 reply 各送一則不帶 header 和一則 `pv` 不同的 frame；都沒有進到 handler、線上沒有錯誤回覆，計數各加一；同一個 subject 與 `pv` 在限流窗內只記一行 log
  - integration：request 收到 `pv` 不符的 reply 時，呼叫端在 timeout 之前拿到本地錯誤
  - API：目標 STS controller、MD / TD controller、TD 帳號 worker 任何一個 `pv` 不同時，start 回 `protocol_mismatch`，沒有新 intent、Supervisor 沒有新 worker；controller 的報告因 `pv` 被丟棄時同樣回 `protocol_mismatch`，不是 `unavailable`
  - reattach 之後，`procman.report` 裡每個 worker 的 `pv` 仍是 spawn 它的那個版本
  - IF-01（#179）留下的 `protocol_mismatch` 契約測試改寫成上面的行為
- **依賴：** B4-01、B3-04、B4-07、B4-08
- **決策：** F26、F41

---

## B5 STS 補齊

### B5-01 交付策略（#210）

- **範圍：** `latest`、kline、`all`；有界佇列與丟棄計數；`event.seq`、`recv_ts`、`event.age`。
- **驗收：** IF-05 交付策略的契約測試轉綠。
- **依賴：** B4-03
- **決策：** F8、F25

### B5-02 event log 併入 ingress（#211）

- **驗收：** 入站事件一收到就記錄，帶 `delivered` / `superseded` / `dropped` 標記；寫檔在 writer thread；沒設 `STS_EVENTLOG_DIR` 時只關掉寫檔。
- **依賴：** B5-01

### B5-03 offload（#212）

- **範圍：** thread 和 process 模式、`offload_pool`、子進程的 `PDEATHSIG`、`OffloadWorkerLost`、`limits.*`、event log 與 progress。
- **驗收：** stop 時 process 模式的子進程被 terminate；子進程 OOM 時，session 收到 `OffloadWorkerLost` 而不會跟著死；從非策略 thread 呼叫 SDK 會被拒。
- **依賴：** B4-03
- **決策：** F9

### B5-04 hook 時間預算（#213）

- **驗收：** F15 表的每一列都有測試：1 秒警告與 `HookSlow`、30 秒時的 B 類 crash、`on_ready` / `on_stop` 的 10 秒上限。
- **依賴：** B4-03
- **決策：** F15

### B5-05 MD / TD 失聯通知（#214）

- **範圍：** `on_md_update`、`on_td_update`、`md.state`、`td.state`、10 秒靜默判定、ingress 重連時的 down / live 與 `on_resync(reconnect)`；帳號 `unavailable` 時下單在本地回 False。
- **驗收：** §5.6「各種情況」表的每一列都有測試。
- **依賴：** B4-03、B4-05、B4-06
- **決策：** F14、F23

### B5-06 crash 分類、清場與重新掛起（#215）

- **驗收：** A、B、C 三類 crash 都能清場；`on_failure` 能從 `on_start` 重新掛起，而且 R1 到 R4 成立；alert log 被既有的 Alert 管線比對到。
- **依賴：** B4-02、B6-03
- **決策：** F10、F11

### B5-07 搬移 artifacts、tape 讀取、fetch、timer（#216）

- **驗收：** 這些功能在新的 worker 上都能用；`tape.read` 不再每筆讓出，SDK 文件附上「大量資料用 `offload` 處理」的範例。
- **依賴：** B4-03、B4-06、B5-03

### B5-08 `StrategyHarness` 與策略測試改寫（#217）

- **驗收：** 所有內建策略在 `StrategyHarness` 上測試全綠（F16）；內建策略都在 `on_ready` 開始交易。
- **依賴：** B5-01、IF-06
- **決策：** F16

### B5-09 STS worker 不持有任何 DB 連線（#218）

- **驗收：** session worker 進程沒有任何 DB 連線，以 driver 的連線計數或 `/proc/<pid>/net` 驗證。
- **依賴：** B5-06
- **決策：** F10

### B5-10 策略樹與 extras 的版本釘住（#276）

- **範圍：**
  - STS 磁碟的 registry 副本改成以 digest 定址，取代 `RegistryStore` 在 STS 端的 `<origin>/<name>/` 原地替換。API 端的 store 不變
  - API 的 start 從自己的 registry 與 env 解析 `(strategy_digest, env_generation)`，寫進 SessionSpec
  - worker 依 digest 載入；controller 的可部署檢查不 import
  - import 探測子進程取代平面進程內的 `load_local_registry` 與 `runtime_env.refresh`
  - GC 與 env 的 `_prune_generations` 改成保留被釘住的版本
  - `mftik workers --stale` 加上 digest 的比對
  - B10 切換時，STS 磁碟的副本由開機 catch-up 依新布局重建，舊的 `<origin>/<name>/` 目錄刪除
- **驗收：** IF-16 的契約測試轉綠；另外在 integration tier 驗證：
  - session 跑著時 push 同名新版本：這個 session 的重新掛起仍跑舊 digest，新 session 跑新 digest，`mftik workers --stale` 列出前者
  - env apply 之後，跑在舊 generation 上的 session 仍然能 lazy import
  - controller 進程從頭到尾沒有 import 任何策略樹
- **依賴：** IF-16、B3-07、B4-02、B4-03、B5-06
- **決策：** F39

### B5-11 STS controller 服務 operator 的主機磁碟路徑（#277）

- **範圍：**
  - `rpc/artifacts.py`、`rpc/eventlog.py` 在 B4-02 改寫 `app.py` 之後，仍由 controller 掛在 `sts.{instance}` 上服務
  - `sweep_loop`（清理未 commit 的上傳）歸 controller
  - 上傳 token 改成可由磁碟上的 `.{name}.{token}.part` 找回，controller 重啟後接得上
  - event log 的讀取對上 B5-02 的檔案布局
- **驗收：**
  - 上傳途中滾動 controller，重啟後用同一個 token 能接著傳完
  - session 跑在 worker 上時，artifact 的 list / read / delete 與 event log 讀取照常
  - 策略在 worker 裡寫的 artifact，operator 經 controller 讀得到
  - API 的 artifact 與 event log 路由不變，CI 的 contracts 檢查通過
- **依賴：** B4-02、B5-02
- **決策：** F40

---

## B6 TD 補齊

### B6-01 常駐層：溫熱的 HTTP 連線池（#219）

- **範圍：** 各 venue REST client 的 keepalive 調整，以及 adapter 的輕量保活請求；連線池由帳號 worker 持有。
- **驗收：** 閒置 10 分鐘後的第一個 REST 請求不需要重新握手，以連線重用計數驗證；每個 venue 的保活間隔寫在 adapter。
- **依賴：** B4-05
- **決策：** F35

### B6-02 交易層（各 venue）（#220）

- **範圍：** Binance（spot / UM / CM）、Bybit、OKX、Deribit、Gate、Bitget 的私有連線、OMS、ledger、recon；依 intent 開關。
- **驗收：** 每個 venue 在 testnet 或 paper 上都能開關交易層；開關時常駐層不受影響。
- **依賴：** B6-01
- **決策：** F34、F35

### B6-03 `td.order.cancel_session`（#221）

- **驗收：** 撤掉該 session 的所有掛單並等到確認；`PENDING_NEW` / `UNKNOWN` 的單收斂後一併處理；逾時時回覆未確認的清單。
- **依賴：** B4-05
- **決策：** F10

### B6-04 drain-replace（人工觸發）（#222）

- **驗收：** `mftik td drain <api_id>` 期間，新單以可重試的 `td_draining` 拒絕；沒有遺失或重複的單；`TdReady` 經歷 false 後回到 true。
- **依賴：** B6-02、B3-03
- **決策：** F27

### B6-05 backfill 由帳號 worker 處理（#223）

- **範圍：** 把 `backfill/` 搬進常駐層；排程和 detach 的觸發；併發限制。
- **驗收：** 沒有 session 的帳號也能 backfill；backfill 進行中，下單延遲不受影響（附量測）。
- **依賴：** B6-01
- **決策：** F35

### B6-06 帳號狀態廣播與 reset（#224）

- **驗收：** `td.account.state.*` 依 F14 廣播；殺掉帳號 worker 後，它會重啟、recon、發出 `td.account.reset`，策略收到 `on_resync(account_reset)`。
- **依賴：** B6-02、B5-05
- **決策：** F13、F14

### B6-07 cancel-on-disconnect（倒數計時型）（#225）

- **範圍：** Binance UM / CM（逐 symbol）、Bitget UTA、OKX、Gate 的死人開關；帳號層級的設定；drain-replace 之前延長倒數。
- **驗收：** 在 testnet 上 kill -9 帳號 worker，倒數到期時交易所撤單；一般重連不會觸發；各家參數寫進 adapter。
- **依賴：** B6-02、B6-04
- **決策：** F37

### B6-08 `oms.view(settled=True)`（#226）

- **驗收：** 有 UNKNOWN 的單時，等到收斂或逾時才回覆。
- **依賴：** B6-02
- **決策：** F13

---

## B7 MD atom

### B7-01 atom 註冊與 subject（#227）

- **驗收：** `atom_id` 的正規化和 hash 穩定；同一個 atom 在 controller 重啟前後得到相同的 subject。
- **依賴：** IF-08

### B7-02a 到 B7-02g 各 venue 的 atom 實作（#228–#234）

每個 venue 一張：**a** Deribit、**b** Binance（spot / UM / CM）、**c** Bybit、**d** OKX、**e** Gate（spot / futures）、**f** Bitget、**g** Paper。

- **驗收（每張相同）：** 該 venue 現有的每個 product topic 都改由 atom 提供；book 的 fold 和缺口 resync 搬進 `decode` / reconciler；capacity 的實測值寫進 adapter。
- **B7-02a 另外：** 修掉原 #151：`DeribitSocket._read_loop` 的 `retries` 只在剛斷掉的那條連線收過 frame 時才歸零，連續幾次 setup 失敗後，之後一次普通斷線就可能耗盡 `max_retries`。改成 setup 成功、且之後收到 frame 就歸零；回歸測試涵蓋 `_open` 失敗和 `_on_open` / `_restore` 失敗兩條路徑。
- **依賴：** B7-01、IF-10
- **決策：** F19、F21

### B7-03 STS 端的通用 join 與組合型 feed 的狀態（#235）

- **驗收：** Binance UM / CM 的 `ticker` = `join(BestQuote, TickerStats)`；報價還沒到前不輸出；任一組成 atom down 時，feed 就是 down。
- **依賴：** B7-02b、B5-05
- **決策：** F19

### B7-04 tape 改以 atom 為 key，加錄 liquidation（#236）

- **驗收：** coverage 以 atom 為單位；Binance UM 的 `trade` 和 `aggtrade` 只錄一份；讀取仍經由 MD，STS 不開 Redis。
- **依賴：** B7-02a 到 B7-02g
- **決策：** F20

### B7-05 MD fetch worker（#237）

- **驗收：** `md.fetch` 由獨立的 fetch worker 服務；MD controller 滾動時不中斷。
- **依賴：** B3-03

---

## B8 MD 編排

### B8-01 orchestrator：desired 與 owner（#238）

- **驗收：** desired atom 由 intent、常駐訂閱、selector 組成，每個 atom 有 owner 集合；owner 進入 terminal 後由 GC 移除。
- **依賴：** B7-01、B4-07

### B8-02 placement 與連線 worker 的生命週期（#239）

- **驗收：** 容量不夠時才開新連線；atom 一旦放上去就不搬（F22）；連線上沒有 atom 時 worker 結束；新 atom 只放到 `pv` 和 controller 相同的連線 worker 上，沒有就開新的（F41）。
- **依賴：** B8-01、B4-10
- **決策：** F17、F22、F41

### B8-03 reconciler 完整版（#240）

- **驗收：** generation 規則（F18）、連線 epoch、token bucket 限速、單一 atom 的 resync；重連後自動補齊。
- **依賴：** B8-02、B7-02a 到 B7-02g
- **決策：** F18

### B8-04 以 listing 驅動到期（#241）

- **驗收：** 到期的合約從 desired 移除，並對它的 owner 發出 `md.feed.end(expired)`。
- **依賴：** B8-01

### B8-05 常駐訂閱，`tape_keeper` 退役（#242）

- **驗收：** 設定檔裡的常駐訂閱生效，tape 照常錄；`impl/tape_keeper.py` 和它的測試刪除。
- **依賴：** B8-01、B7-04

### B8-06 狀態廣播、原地重啟、列出舊版 worker（#243）

- **驗收：** `md.w.*` 依 F14 廣播；`mftik md restart <conn>` 原地重啟並在 coverage 記錄 tape 空洞；`mftik workers --stale` 列出跑在舊版代碼上的 worker。
- **依賴：** B8-03、B3-07
- **決策：** F14、F24

### B8-07 MD 編排驗收（#244）

- **驗收：** 滾動 MD controller 時，連線、行情和 tape 都不中斷；原地重啟連線 worker 造成的 tape 空洞都有量測紀錄。
- **依賴：** B8-01 到 B8-06

---

## B9 Selector

### B9-01 `option_chain`（#245）

- **驗收：** 依 `expiries`、`min_tte`、`strikes.atm` 選出合約；ref 移動時依 `recenter` 的規則重新置中；ref 斷線或 listing 過期時維持上一份。
- **依賴：** IF-09、B8-01
- **決策：** F33

### B9-02 `rolling_future`（#246）

- **驗收：** 依到期日分出 weekly / monthly / quarterly；`roll_before` 時 current 切到下一張；舊合約保留到到期才移除。
- **依賴：** IF-09、B8-01
- **決策：** F33

### B9-03 狀態持久化、規格共用、部署時的上限（#247）

- **驗收：** controller 重啟後 universe 和 epoch 不變、不重新置中；規格相同的 selector 只算一份；部署時算出 atom 上限並做容量檢查。
- **依賴：** B9-01、B9-02、IF-14

### B9-04 `md.universe` 與 SDK（#248）

- **驗收：** I-SEL1 成立：合約出現在 `added` 之前，策略收不到它的事件；出現在 `removed` 之後也收不到。`self.md.universe`、`self.md.current` 正確。
- **依賴：** B9-03、B5-01
- **決策：** F33

---

## B10 切換

### B10-01 DB migration（刪除部分）（#249）

- **範圍：** `rebuild_count` 改名為 `restart_count`；drop `st_facts`；`md_sessions`、`td_sessions` 停寫。
- **驗收：** migration 在 Postgres 的正式資料快照上演練過一次；models 與 migration 一致。
- **依賴：** IF-14、B5-09
- **決策：** F11、F36、F38

### B10-02 preflight（#250）

- **驗收：** 任何 `sts_sessions` 仍是 live 狀態時，拒絕套用切換。
- **依賴：** B10-01
- **決策：** F2

### B10-03 前端 MD / TD 頁（#251）

- **範圍：** `frontend/src/routes/md`、`frontend/src/routes/td` 改成顯示 worker 和 intent（F38），以及對應的 API。
  - MD 頁：每個連線 worker 的 venue / endpoint、狀態、incarnation、代碼版本、atom 數、RSS，以及每個 atom 的 owner 和最後一筆資料時間。
  - TD 頁：每個帳號 worker 的狀態、交易層開或關、session 數、incarnation、代碼版本、RSS。
- **驗收：** 資料來自 `procman.report` 和 worker 狀態廣播；`just frontend-check` 和 frontend e2e 通過。
- **依賴：** B8-06、B6-06
- **決策：** F38

### B10-04 runbook 與上線（#252）

- **範圍：** 先升級 agent（S-1 到 S-3），這一步會殺掉所有 strategy，所以必須在停掉所有策略之後做；再讓 plane sets 開啟 `oci_host_pid: true`；strategon#61 已上線的話，依 §4.7 設定每個平面的 `memoryBytes`。
- **驗收：** 依 runbook 在空的平面上完成切換；回滾到 `arch/baseline` 演練過一次。
- **依賴：** B10-02、B3-06，以及 B5、B6、B8、B9 全部完成
- **決策：** F2、F6

### B10-05 文件定稿（#253）

- **驗收：** README、`ARCHITECTURE.md`、`Deployment.md` 更新為新架構；`REFACTOR_TICKETS.md` 和 `docs/baseline/` 封存到 `docs/archive/`。
- **依賴：** B10-04
