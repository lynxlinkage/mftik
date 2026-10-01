# REFACTOR_TICKETS — 平面進程化重構的工作票

> **對應 `ARCHITECTURE_CHANGE_PLAN.md` v0.25。** 所有改動先合併到 `refactor/process-planes` 分支。票裡的 F 編號、§ 章節、附錄都指那份文件。
>
> 每張票都有描述、範圍、驗收、依賴。驗收寫成別人能檢查的事：測試名稱、grep 結果、量測數字、文件章節。

## 怎麼用這份文件

**編號：** `<批次>-<序號>`。批次依序是 B0、B1、RM、B2、IF、B3 到 B10（計畫 §11）。依賴只列直接依賴。

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
| IF 介面 | 15 | 新抽象層只定義介面，回傳 null data，附 xfail 契約測試 |
| B3 procman | 7 | shim、Supervisor、reattach、報告、准入、Strategon 實機驗證 |
| B4 骨架 | 9 | paper 上跑通 deploy → 下單 → 成交 → end |
| B5 STS | 9 | 交付策略、event log、offload、hook 預算、失聯通知、crash 與重啟、策略測試改寫 |
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

### B0-01 打 `arch/baseline` tag

- **描述：** 凍結重構前的代碼，也就是 B10 的回滾目標。
- **範圍：** 在計畫的基準 commit（`main` @ `a0cbfb2`）打 annotated tag `arch/baseline`。
- **驗收：** tag 已推到 origin；CI 在這個 commit 是綠的。
- **依賴：** —

### B0-02 在 GitHub Actions 量測現行測試耗時

- **描述：** 建立兩分鐘預算的起點（F30），並找出慢的真正原因。
- **範圍：** CI 加一個一次性的 job，跑 `pytest --durations=0 --junitxml`，依模組彙整。
- **驗收：**
  - 附錄 C 填入每個測試模組的耗時，以及 `ubuntu-latest` 上的總 wall time。
  - 最慢的 50 個測試各標出主因：lease 心跳、真的 sleep、NATS 往返、子進程、Postgres、其他。
  - 回答「慢是不是 NATS 造成的」，這是 F31 的前提。
- **依賴：** B0-01

### B0-03 協定盤點

- **描述：** 列出現行所有訊息型別和 subject，定下每一個在新協定裡的去向。
- **範圍：** `packages/common/src/mftik/protocol/messages.py`、`topics.py`，以及各平面的 `rpc/router.py`。
- **驗收：** `docs/baseline/protocol.md` 列出每個 type 和 subject 的發送者、接收者與去向（保留、改名、刪除），並對照 §8.3。每一個型別都有去向。
- **依賴：** B0-01

### B0-04 現況的狀態權威表（as-is）

- **描述：** 對照 §3.3 的目標表，寫出每種狀態現在由誰寫、存在哪、重啟後怎麼恢復。
- **範圍：** 三個平面、API、SDK、DB、Redis。
- **驗收：** `docs/baseline/state-authority.md` 有一張和 §3.3 同欄位的表；和目標不同的每一列，都指到負責改它的票。
- **依賴：** B0-01

### B0-05 刪除影響面盤點

- **描述：** RM 每張票要刪的符號，先反查所有呼叫端和測試，讓 RM 的範圍在動手前確定。
- **範圍：** §5.4、§8.1、§8.2 的刪除清單；附錄 A、B。
- **驗收：** 每張 RM 票的範圍補上完整的呼叫端清單（`檔案:函式`）；附錄 A、B 定稿。
- **依賴：** B0-01

### B0-06 確認主機上的事實

- **描述：** 驗證 §4.5 的兩個推論：agent 升級會殺掉所有 strategy，以及 cgroup 記憶體上限沒有生效。
- **範圍：** cp、yite 兩台主機。
- **驗收：** `systemctl show strategon-agent -p KillMode`、`cat /proc/<plane pid>/cgroup`、`/proc/<pid>/status` 的結果記進 §4.5；與推論不符的地方回頭修正計畫。
- **依賴：** —

---

## B1 文件

### B1-01 封存 `docs/`

- **範圍：** 除了 `ARCHITECTURE_CHANGE_PLAN.md`、`REFACTOR_TICKETS.md`、`Deployment.md`，`docs/*.md` 全部移到 `docs/archive/`，包括 `Deribit`、`BitgetUta` 實測表（F28）。另外新增 `docs/archive/INDEX.md`，記錄每份文件的封存日期和取代它的文件。
- **驗收：** `docs/` 根目錄只剩 §10 列出的檔案；INDEX 涵蓋每個封存檔；repo 內指向舊路徑的連結都已更新。
- **依賴：** —

### B1-02 依現況重寫 `Deployment.md`

- **描述：** F28。以 `deployment/sets/*.json`、`deployment/nats/nats.conf` 和 Strategon 現在的實際行為為準。
- **驗收：** 涵蓋 plane sets（成員、機器、env、volume；`limits.memoryBytes` 目前沒有生效）、OCI 與 `captureStdio`、每個 site 一台 NATS 加 gateway、secret 的位置、部署與回滾指令。每一節都能對應到 repo 裡的檔案。
- **依賴：** B0-06

### B1-03 README 縮成指引

- **驗收：** README 只剩一句話的專案說明、目錄導覽、quick start（`just up`、`just test`）、文件索引；過時的段落全部刪除。
- **依賴：** B1-01

### B1-04 萃取 `ARCHITECTURE.md`

- **描述：** F1 到 F38 都已定案。從計畫萃取出目標架構，只寫「是什麼」，不寫討論過程。
- **驗收：** 涵蓋原則（§2.3）、分層與狀態權威（§3）、procman（§4）、各平面的 worker 與 controller、協定、測試標準摘要。兩份文件衝突的地方，以計畫為準並修正。
- **依賴：** —

---

## RM 清場

RM 結束時，三個平面都還能啟動，只是沒有 session 機制。要搬移、不刪除的代碼，在各票的「留下」列出，由後面的批次搬進新的層。

### RM-01 STS：刪除 rebuild

- **範圍：**
  - `apps/sts/src/mftik_sts/session/manager.py`：`rebuild_interrupted`、`rebuild_session`、`adopt_interrupted`、`_spawn_rebuild`、`_rebuild_after_exit`、`_settle_rebuild`、`rebuild_on_worker_exit`
  - `apps/sts/src/mftik_sts/app.py`：`STS_REBUILD_ON_BOOT`、`STS_REBUILD_MAX_AGE_S` 與相關的啟動流程；`packages/common/src/mftik/cli/templates/docker-compose.yml`、`deployment/sets/planes.json` 裡的 `STS_REBUILD_ON_BOOT`
  - `apps/sts/src/mftik_sts/worker.py`：`rebuild` role、`adopt_interrupted` 的呼叫
  - SDK：`strategy/base.py` 的 `rebuildable`、`on_rebuild`、`remember()`；`strategy/session.py` 的 `remember`
  - 內建策略 `impl/*.py`：`rebuildable`、`on_rebuild`。`chase` 的 `started_ms` 和滑價錨定價改成只存在記憶體
  - `apps/sts/src/mftik_sts/db.py` 的 `remember_fact`、`bump_rebuild_count`、`reset_rebuild_count`，以及 `packages/db/.../repositories/session.py` 的對應方法。欄位本身留到 B10-01 的 migration
  - 測試：`test_rebuild`、`test_environment_rebuild`、`packages/db/tests/test_sts_session_repository.py` 裡和 `remember` / `rebuild_count` 相關的案例，以及策略測試裡的 rebuild 案例。F16 只允許刪 rebuild 案例，其餘留到 B5
- **驗收：** 共同驗收；`sts_sessions.st_facts` 沒有任何寫入路徑；策略測試仍然全綠。
- **依賴：** B0-05
- **決策：** F10、範圍 4

### RM-02 STS：刪除續約、自動 recon 與 recon API

- **範圍：**
  - `apps/sts/src/mftik_sts/session/session.py`：`_lease_heartbeat_loop`、`_md_acks`、`_td_acks`、`_heartbeat_overslept`、`_shift_peer_acks`、`_fail_from_infrastructure`（MD / TD feed 的那幾條路徑）、`_recon_sent`、`_on_lease_ack`
  - SDK：`Strategy.send_recon`、`on_recon_done`；protocol 的 `STS_RECON`，以及 `apps/td/.../session/manager.py` 裡處理它的分支
  - 內建策略（chase、cross_arb、macd_dollar、noop、oco、twap）在 `on_recon_done` 才開始交易的寫法，改成在現有的 `on_ready` 開始。新的就緒語意在 B5 驗證
  - 測試：`test_md_ack_watchdog`、`test_td_ack_watchdog`、`test_recon_oms`
- **驗收：** 共同驗收；STS session 不再對 MD / TD 送任何週期性 heartbeat。
- **依賴：** B0-05
- **決策：** F13、§8.2

### RM-03 SDK：刪除 `breathe` / `slice_deadline`

- **範圍：** `strategy/base.py`、`strategy/tape.py`（`breathe`、`slice_deadline`、`SLICE_S`、`tape.read(on_print=…)` 每筆讓出的邏輯）、`strategy/__init__.py` 的匯出、`impl/macd_dollar.py` 的用法、`packages/common/README.md`。
- **驗收：** 共同驗收。`macd_dollar` 原本切片的計算改成一次算完；`offload` 到 B5-03 才有，這段期間接受同步計算。
- **依賴：** B0-05
- **決策：** F9

### RM-04 STS：刪除進程綁定、reaper、雙模式 manager 與 worker 端 DB 存取

- **範圍：**
  - `apps/sts/src/mftik_sts/spawn.py`：`SubprocessSpawner`、`LIFELINE_FD_ENV`、`MFTIK_STS_PARENT_PID`
  - `apps/sts/src/mftik_sts/worker.py`：`arm_parent_death`、`_watch_lifeline`
  - `apps/sts/src/mftik_sts/session/manager.py`：整份刪除，包括 `reap_orphans`、`_create_in_process`，以及傳給 worker 端的 `persist_live`、`mark_done`、`mark_live`、`load_session`、`list_db_sessions`、`td_instance`、`derive_sts`
  - `app.py` 的對應接線。`sts.{instance}` 的 start / end 改成占位（IF-04）
  - 測試：附錄 A 的 STS 清單
- **留下（之後搬移）：** `session/session.py` 裡扣掉 RM-02 之後的下單與事件分派（B4-03）；`impl/`；`rpc/artifacts.py`、`rpc/eventlog.py`、`rpc/env.py`、`rpc/registry.py`、`registry_catchup.py`、`runtime_env.py`。
- **驗收：** 共同驗收；STS 平面能啟動，並服務 health、registry、env、artifacts 這些不依賴 session 的 RPC。
- **依賴：** RM-01、RM-02
- **決策：** F6、F10

### RM-05 MD：刪除 session 機制

- **範圍：**
  - `apps/md/src/mftik_md/session/`（`manager.py`、`dispatcher.py`、`venue.py`、`factory.py`）：per-session fan-out `md.{session_id}`、`VenueSession`、`_expiry_tasks`、執行期 `md.subscribe` 的處理、`reap_orphans`，以及 `persist_live`、`mark_done`、`list_db_sessions` 的接線
  - `apps/md/src/mftik_md/rpc/sessions.py`
  - 測試：附錄 A 的 MD 清單
- **留下：** `fetch/`（B7-05）、`tape.py`、`tape_store.py`、`rpc/tape.py`（B7-04 改 key）、`mftik.exchange.*` 的各 venue adapter（B7-02）。
- **驗收：** 共同驗收；MD 平面能啟動，並服務 `md.fetch` 和 tape 讀取。
- **依賴：** B0-05
- **決策：** F17、F21、F22

### RM-06 TD：刪除 session manager 的 attach、lease、refcount、reaper

- **範圍：**
  - `apps/td/src/mftik_td/session/manager.py`：attach / detach、refcount、lease、`reap_orphans`、`persist_live` / `mark_done` / `list_db_sessions` 的接線
  - `_handle_recon`：等 settled 的邏輯抽成獨立函式保留下來，留給 IF-11 的 `view(settled=True)`；其餘刪除
  - `apps/td/src/mftik_td/rpc/sessions.py`
  - 測試：附錄 A 的 TD 清單
- **留下：** `session/session.py`（每個帳號的 OMS、ledger、recon、下單，B4-05 搬進交易層）、`oms/`、`backfill/`（B6-05）、`history.py`。
- **驗收：** 共同驗收；`session/session.py` 仍能單獨建構，並以 paper 跑 OMS / ledger 的單元測試。
- **依賴：** B0-05
- **決策：** F34、F35、F36

### RM-07 刪除 lease 協定與 `LeasedSessionLink`

- **範圍：** `packages/common/src/mftik/broker/link.py`（`LeasedSessionLink`），以及 `broker/client.py`、`broker/transport/base.py`、`broker/__init__.py` 的相關部分；protocol 的 `STS_LEASE_HEARTBEAT`、`MD_LEASE_ACK`、`TD_LEASE_ACK`、`LeaseHeartbeat` 與對應的 envelope；`test_broker.py` 的 leased link 案例、`test_lease_resilience`、`test_md_lease_resilience`。
- **驗收：** 共同驗收；protocol 和 broker 裡沒有任何 lease 相關的型別或函式。
- **依賴：** RM-02、RM-05、RM-06
- **決策：** §8.2

### RM-08 API：刪除同步 deploy 與補償邏輯

- **範圍：** `apps/api/src/mftik_api/orchestrate.py` 的 `deploy_strategy`、`cancel_create_followups`、`_abort_timed_out_create`、`_send_abort`、`_poll_terminal`、`_remember_pending_abort`、`_forget_pending_abort`、`_list_pending_aborts`、`resume_pending_aborts`、`_schedule_abort_retry`、`_retry_abort`、`_ensure_start_failed`、`_schedule_failed_row`、`_mark_start_failed`、`_retry_failed_row`；`main.py` 啟動時對 `resume_pending_aborts` 的呼叫；`abort_target` 的寫入。`routes/sts.py` 的 deploy 路由改成回 501，等 IF-13。測試：附錄 A 的 API 清單。
- **驗收：** 共同驗收；API 其他路由（auth、alerts、artifacts、registry、board、logs）照常運作。
- **依賴：** B0-05
- **決策：** F12、§8.1

### RM-09 刪除 strategy.yml 與 CLI 的舊欄位

- **範圍：** `protocol/strategy_yml.py` 的 `RESTART_ALWAYS` 和 `START_TIMEOUT_DEFAULT_S` 的 8 / 300 秒；`create_rpc_timeout`（`protocol/messages.py`、`cli/run.py`）；`deploy_http_timeout`（`cli/run.py`）。新欄位在 IF-07 加。
- **驗收：** 共同驗收；`mftik check` 遇到含舊欄位的文件時，給出明確的錯誤訊息，並告訴使用者新寫法。
- **依賴：** RM-08
- **決策：** F11、F12

### RM-10 清場後的基線

- **描述：** 確認 RM 之後還剩下什麼，作為 B2 和 IF 的起點。
- **驗收：**
  - 附錄 A 的測試全部刪除或改寫；剩餘的測試數和各模組耗時（GitHub Actions）記在附錄 C 的「RM 之後」欄。
  - `docs/baseline/remaining.md` 列出每個平面還剩哪些模組，以及各自的去向：搬進哪個新的層，或保留不動。
  - 打 tag `arch/cleared`。
- **依賴：** RM-01 到 RM-09

---

## B2 測試標準

### B2-01 `TESTING.md`

- **驗收：** 寫明 tier（§9.1）、規則（§9.2）、預算與閘門（F30）、handler 和傳輸分開的寫法（F31），以及 IF 的 `xfail(strict=True)` 契約測試慣例；每條規則附一個範例。
- **依賴：** —

### B2-02 `Clock` / `FakeClock`

- **描述：** §9.2 規則 1。這也是第一個新增的抽象層（§3.4）。
- **範圍：** `mftik.clock`；conftest 在 unit 和 component tier 攔截 `asyncio.sleep(x > 0)`。
- **驗收：** `FakeClock.advance()` 能推進 `sleep` 和 timer；unit 測試裡呼叫真的 sleep 會失敗，並指出呼叫位置。
- **依賴：** RM-10

### B2-03 共用 NATS 連線的 fixture

- **範圍：** `broker_harness`：每個 xdist worker 只連一次 NATS（session 級 fixture，pytest-asyncio 用 session 級 event loop）；每個測試拿到自己 `key_prefix` 的 broker；teardown 時退訂該 prefix 下的訂閱，不關連線。
- **驗收：** broker 語意測試（`test_nats_transport`、`test_broker*`）改用新 fixture 後全綠；整套測試期間的 NATS 連線數等於 xdist worker 數，以 NATS monitoring 的 `/connz` 驗證。
- **依賴：** RM-10
- **決策：** F31

### B2-04 tier、平行化、閘門與 CI

- **範圍：** marker（`component`、`integration`、`e2e`）、`pytest-xdist`、`pytest-timeout`、`just test` / `just test-int`；CI 拆成 unit + component 和 integration 兩個 job；120 秒與單一 unit 測試 50 ms 的閘門。
- **驗收：** 故意放慢的測試會讓 CI 失敗；`just test` 在 GitHub Actions 上少於 120 秒。
- **依賴：** B2-02、B2-03
- **決策：** F30

### B2-05 處理剩下借 NATS 測行為的測試

- **描述：** RM 之後還剩約 130 個借 NATS 測行為的測試（`test_plane`、`test_backfill_executor`、`test_tape_read`、`test_ledger_view` 等）。之後會重寫的模組，先標成 integration；不在重構範圍、拆開需要改模組本身的（例如 SYM 的 `test_plane`），也先標成 integration。
- **驗收：** 每個測試都有 tier；`just test` 裡沒有借 NATS 測行為的測試；每個標成 integration 的測試，都註明之後由哪張票改寫。
- **依賴：** B2-04
- **決策：** F31

---

## IF 介面

每張票對應 §3.4 的一層。除了特別註明「這張票是真的實作」的，都只定義介面、回傳 null data。

### IF-01 協定 v2 的型別

- **範圍：** `mftik.protocol`：envelope 加 `pv`；`md.intent.put` / `delete` / `patch`、`td.intent.put` / `delete`、`sts.session.start` / `end` / `status`、`md.a.*`（atom subject 與 hash）、`md.w.*`、`md.universe.*`、`td.account.state.*`、`td.account.reset`、`td.order.cancel_session`、`procman.report.*` 的 model 與 `Topics`；reject code `protocol_mismatch`。
- **驗收：** 共同驗收；B0-03 盤點裡標成「保留」或「改名」的型別，都對到新的名字；契約測試：`pv` 不符時，收件端以 `protocol_mismatch` 拒絕。
- **依賴：** RM-10、B0-03
- **決策：** F26、§8.3

### IF-02 `mftik.broker.handler`

- **範圍：** `Handler` 協定（收到解碼後的訊息 → 回覆與副作用）、`serve(broker, subject, handler)`。
- **驗收：** 共同驗收；挑一個現有的小 RPC（例如 health）改用這個介面，當作範例。
- **依賴：** RM-10
- **決策：** F31

### IF-03 `mftik.procman`

- **範圍：** `WorkerSpec`（§4.3）、狀態機的 enum 與轉移表、`Supervisor`（`start`、`close(mode)`、`spawn`、`stop`、`status`、`report`）、shim 的 NDJSON 訊息（`status`、`signal`、`watch`、`release`）、exit 紀錄格式、`procman.report` 的 payload。
- **驗收：** 共同驗收；契約測試涵蓋 shim 不變式 S1 到 S7、§4.4 的 reattach 對帳表、FAILED 與 CRASHED 的區分、重啟 intensity。
- **依賴：** RM-10
- **決策：** F6、F7、F29

### IF-04 STS controller

- **範圍：** `mftik_sts.controller`：`StsOrchestrator.reconcile(spec, status) -> actions`、crash 分類（A、B、C）、重啟策略（`never` / `on_failure`、`max_restarts`、`restart_window_s`、backoff）、`sts.{instance}` 的 start / end / list handler。
- **驗收：** 共同驗收；契約測試涵蓋 R1 到 R4、F11 的每一條規則、`restarting` 期間 intent 不回收。
- **依賴：** IF-01、IF-02、IF-03
- **決策：** F10、F11、F12

### IF-05 STS session worker

- **範圍：** `mftik_sts.session_worker`：`Ingress`、`StrategyRunner`、生命週期階段 0 到 6 的 enum、交付策略（`latest`、kline、`all`）、事件的 `recv_ts` / `seq` / `age`、event log 標記（`delivered`、`superseded`、`dropped`）、hook 時間預算的回報格式。
- **驗收：** 共同驗收；契約測試涵蓋 I1 到 I4、F15 表的每一列、§5.3 的交付策略表、TD 事件溢出時 fail。
- **依賴：** IF-01、IF-02
- **決策：** F8、F15、F23、F25

### IF-06 SDK 表面

- **範圍：** `mftik.strategy`：
  - hook：`on_ready(ready)`（含 `ready.missing_feeds`）、`on_md_update`、`on_td_update`、`on_resync`、`on_universe_change`
  - 呼叫：`self.offload`、`self.offload_pool`、`self.oms.view(settled=)`、`self.md.state` / `universe` / `current` / `subscribe`、`self.td.state`
  - 例外與計數：`NotReady`、`OffloadWorkerLost`、`HookSlow`
  - `StrategyHarness` 的 API
  - hook 預設是 no-op，SDK 呼叫回傳 null data
- **驗收：** 共同驗收；所有內建策略仍能 import 和實例化；契約測試涵蓋：`on_ready` 之前下單拋 `NotReady`、I-SEL1、帳號 `unavailable` 時下單在本地回 False（`td_unavailable`）。
- **依賴：** RM-01、RM-02、RM-03
- **決策：** F9、F12、F13、F14、F33

### IF-07 strategy.yml v2

- **範圍：** `protocol/strategy_yml.py`：`restart: never | on_failure`、`max_restarts`、`restart_window_s`、`start_timeout_s`（預設 60、上限 3600）、`ready_timeout_s`（30）、`limits`（`memory_mb`、`offload_threads`、`offload_processes`、`offload_memory_mb`）、每個 feed 的交付策略覆寫、`md:` 裡的 `select:`（`option_chain`、`rolling_future`）。
- **驗收：** **這張票是真的實作**（只是 schema）：§6.4 的 YAML 範例能解析；非法值有明確的錯誤訊息；`mftik check` 認得新欄位。
- **依賴：** RM-09
- **決策：** F9、F11、F12、F33

### IF-08 MD adapter 的 atom 介面

- **範圍：** `Atom`、`AtomPlan`、`atoms_for(topic, ticker, opts)`、`decode(atom, frame) -> list[Event]`、`capacity(endpoint)`、`join_policy(atom)`；venue 中立的 `TickerStats` model；每個 venue 一個空的 `atoms.py`。
- **驗收：** 共同驗收；契約測試：Binance UM 的 `ticker` 解析成 `@ticker` 加 `@bookTicker` 兩個 atom；Deribit 的 `ticker.*` 一個 frame 產出 Ticker、Greeks、OpenInterest。
- **依賴：** RM-05
- **決策：** F19、F21

### IF-09 MD controller 與 selector

- **範圍：**
  - `mftik_md.controller`：`MdOrchestrator`（desired atom 由 intent、常駐訂閱、selector 組成，並記錄 owner 集合）、`place(desired, conns, capacity) -> placement`（黏性、不遷移）、`generation = (controller_epoch, seq)`、到期
  - `mftik_md.selector`：`evaluate(listing, ref, now, prev) -> Selection | Hold`、`OptionChainSpec`、`RollingFutureSpec`
- **驗收：** 共同驗收；契約測試涵蓋：placement 不搬 atom（F22）、selector 的防抖動、`min_tte`、轉倉時舊合約保留到到期、fail-static。
- **依賴：** IF-01、IF-08
- **決策：** F17、F18、F22、F33

### IF-10 MD 連線 worker

- **範圍：** `mftik_md.conn`：`ConnWorker`、`Reconciler`（純函數 `reconcile(desired, observed) -> actions`）、狀態廣播、tape append 的介面、原地重啟的入口。
- **驗收：** 共同驗收；契約測試涵蓋：舊 epoch 的 ack 被丟棄、重連後 observed 歸零並補齊、book 缺口只 resync 單一 atom、`seq` 在同一個 incarnation 內連續。
- **依賴：** IF-01、IF-08
- **決策：** F17、F18、F20、F21、F24、F25

### IF-11 TD 帳號 worker

- **範圍：** `mftik_td.account`：
  - `ResidentLayer`：HTTP 連線池、保活的 hook、backfill handler
  - `TradingLayer`：`activate()` / `deactivate()`
  - `td.order.*`、`td.oms.*`、`td.ledger.*`、`cancel_session`、`oms.view(settled)` 的 handler
  - 狀態廣播
  - `DeadMansSwitch` 介面，每個 venue 一個實作位置
- **驗收：** 共同驗收；契約測試涵蓋：交易層開關不影響常駐層、`cancel_session` 的確認語意、`settled=True` 會等 UNKNOWN 的單收斂。
- **依賴：** IF-01、IF-02、RM-06
- **決策：** F34、F35、F37

### IF-12 TD controller

- **範圍：** `mftik_td.controller`：`desired_accounts`（本 instance 名下所有啟用的帳號）、intent → 交易層開關（level-triggered）、drain-replace 的入口。
- **驗收：** 共同驗收；契約測試涵蓋：controller 不在時 worker 維持最後一份 desired（P5）、新 incarnation 只在舊 PID 消失後才啟動（F36）。
- **依賴：** IF-03、IF-11
- **決策：** F27、F35、F36

### IF-13 API start / end 與 intent repository

- **範圍：** `mftik_api.orchestrate`：`start(spec)` 回 202 `{session_id, status}`、`end(session_id, reason)`；intent repository（`put`、`delete`、`patch`、`release`）；`routes/sts.py` 的 deploy 路由接上，回 202 和 null 的進度。
- **驗收：** 共同驗收；`contracts/openapi.json` 已更新，CI 的 contracts 檢查通過；契約測試涵蓋 §8.1 的 start / end 流程，以及啟動失敗時不回滾。
- **依賴：** IF-01、IF-14
- **決策：** F12、F38

### IF-14 DB schema

- **範圍：** `mftik_db`：
  - `sts_sessions` 的 Spec / Status 欄位：`generation`、`observed_generation`、`worker_incarnation`、`conditions`、`restart_count`
  - 新表：`md_intents`、`td_intents`（都含 `released_at`）、`md_standing_subscriptions`、selector 狀態
  - `apis` 的帳號設定（cancel-on-disconnect）
  - alembic migration 只加不刪，刪除留給 B10-01
- **驗收：** **這張票是真的實作**：migration 在 sqlite 和 Postgres 上都能 upgrade；CI 的 *Migrations match the models* 通過。
- **依賴：** RM-10
- **決策：** F11、F33、F37、F38

### IF-15 CLI

- **範圍：** `mftik run --wait / --no-wait`、`mftik workers [--stale]`、`mftik md restart <conn>`、`mftik td drain <api_id>`、`mftik intents gc --instance <name>`。先只印 not implemented。
- **驗收：** 共同驗收；`mftik --help` 列出新指令。
- **依賴：** IF-01
- **決策：** F12、F24、F27、F32

---

## B3 procman

### B3-01 shim

- **範圍：** 只用 Python 標準庫（F29）：double-fork 加 `setsid`、`PR_SET_CHILD_SUBREAPER`、對 worker 設 `PDEATHSIG`、`oom_score_adj`、`RLIMIT_DATA`、stdio 與 status pipe 的 log 輪替、NDJSON socket、`exit.json`、SIGTERM 轉送。
- **驗收：** IF-03 裡 S1 到 S7 的契約測試轉綠（integration tier，真的子進程）；shim 的 RSS 實測值寫進 §4.7。
- **依賴：** IF-03、B2-04

### B3-02 Supervisor 狀態機與重啟策略

- **驗收：** 狀態機、FAILED / CRASHED 的區分、backoff、intensity、heartbeat 逾時的契約測試轉綠。
- **依賴：** B3-01

### B3-03 detach / reattach

- **範圍：** `supervisor.json`、socket 掃描、`/proc` 掃描（避免重複 spawn，F36）、§4.4 的對帳表。
- **驗收：** integration：controller 以 detach 結束後 worker 存活；新的 controller reattach，而且不會重複 spawn；殺掉 shim 後 worker 自行 graceful stop。
- **依賴：** B3-02

### B3-04 `procman.report` 與 RSS

- **驗收：** 報告包含 worker 集合、generation，以及每個 worker 整棵子進程樹的 RSS；controller 滾動期間報告暫停。
- **依賴：** B3-02、IF-01

### B3-05 准入控制

- **驗收：** 超過 `max_workers` 或 `memory_budget_mb` 時，以 `capacity_exceeded` 拒絕，不先啟動 worker。
- **依賴：** B3-04
- **決策：** F7

### B3-06 在 strategon#60 上做實機驗證

- **描述：** 外部依賴 strategon#60（S-1 到 S-3）。
- **驗收：** 在 cp 和 yite 上以 `oci_host_pid` 實際滾動一次 controller，確認：worker 存活；release GC 不刪仍在使用的 rootfs；重啟 agent 不影響任何 strategy；新舊版本的 worker 能並存；`/proc/<pid>/oom_score_adj` 符合 §4.7。
- **依賴：** B3-03、strategon#60
- **決策：** F6

### B3-07 release pin 與代碼版本

- **範圍：** 讀取 `STRATEGON_RELEASE_VERSION`、寫 pin 檔（S-2 的 fallback）、`WorkerSpec.code_ref`；作為 `mftik workers --stale` 的資料來源。
- **驗收：** CLI 列出每個 worker 的代碼版本；仍有 worker 在跑的 release 都在 pin 檔裡。
- **依賴：** B3-03、IF-15
- **決策：** F24、F27

---

## B4 端到端骨架（只接 paper）

### B4-01 協定 v2 接上 broker

- **驗收：** IF-01 的 `pv` 契約測試轉綠；所有平面送出的訊息都帶 `pv`。
- **依賴：** IF-01、B2-04

### B4-02 STS controller

- **範圍：** SessionSpec 的 reconcile、start / end、和 Supervisor 的整合、status 與 conditions 的寫入、`sts.status.{session_id}`。
- **驗收：** IF-04 裡和啟動、停止相關的契約測試轉綠。crash 與重新掛起留給 B5-06。
- **依賴：** IF-04、B3-03、B4-01

### B4-03 STS session worker（雙 thread）

- **範圍：** ingress / strategy thread、階段 0 到 6、`on_start` 獨佔、readiness gate（MdReady 軟、TdReady 硬）、`NotReady`、直接 publish 加強制 flush、ack 經 inbox 和 `call_soon_threadsafe` 交回、ingress 對 shim 的 heartbeat；把 `session/session.py` 留下的下單與事件分派搬進來。
- **驗收：** I1 到 I4 的契約測試轉綠；一個 30 秒 CPU-bound 的 hook 不會讓 session fail、不會讓 NATS 斷線、不會造成假的 ack timeout。
- **依賴：** IF-05、IF-06、B4-02
- **決策：** F3、F8、F12

### B4-04 實測三件事

- **驗收：** 結果記進 §5.3：
  1. `reply` 不屬於送出連線時，no-responders 是否照常送達。
  2. ack 回程的跨 thread 延遲。
  3. GIL switch interval 要不要調。
- **依賴：** B4-03

### B4-05 TD 帳號 worker（paper）

- **範圍：** 把 `session/session.py` 搬進交易層；paper 的常駐層；`td.order.*` 的 handler。
- **驗收：** paper 上 `td.order.*` 的契約測試轉綠。
- **依賴：** IF-11、IF-12、B3-03

### B4-06 MD 連線 worker（paper）

- **驗收：** paper 的 atom 發佈到 `md.a.*`，session 收得到。
- **依賴：** IF-08、IF-10、B3-03

### B4-07 MD / TD controller 的最小版

- **範圍：** intent 的 put / delete；依 `procman.report` 回收（§8.2 規則 3）。
- **驗收：** session 結束後幾秒內 intent 被回收；`restarting` 期間不回收；STS controller 的報告停止時不回收任何東西（F32）。
- **依賴：** IF-09、IF-12、B3-04

### B4-08 API start / end 與 `mftik run --wait`

- **驗收：** deploy 回 202；`mftik run` 追蹤到 `running` 或 `failed` 後 tail log。
- **依賴：** IF-13、IF-14、IF-15、B4-02

### B4-09 骨架驗收

- **驗收：** 在 compose 上：
  - paper 的 deploy → `on_start` → `on_ready` → 下單 → 成交回報 → end，全程走新路徑。
  - 三個 controller 各自滾動，都不中斷。
  - 各 kind 的 RSS 實測寫進 §4.7，並調整初始值。
  - 超過預算的 start 被拒。
- **依賴：** B4-03 到 B4-08、B3-05

---

## B5 STS 補齊

### B5-01 交付策略

- **範圍：** `latest`、kline、`all`；有界佇列與丟棄計數；`event.seq`、`recv_ts`、`event.age`。
- **驗收：** IF-05 交付策略的契約測試轉綠。
- **依賴：** B4-03
- **決策：** F8、F25

### B5-02 event log 併入 ingress

- **驗收：** 入站事件一收到就記錄，帶 `delivered` / `superseded` / `dropped` 標記；寫檔在 writer thread；沒設 `STS_EVENTLOG_DIR` 時只關掉寫檔。
- **依賴：** B5-01

### B5-03 offload

- **範圍：** thread 和 process 模式、`offload_pool`、子進程的 `PDEATHSIG`、`OffloadWorkerLost`、`limits.*`、event log 與 progress。
- **驗收：** stop 時 process 模式的子進程被 terminate；子進程 OOM 時，session 收到 `OffloadWorkerLost` 而不會跟著死；從非策略 thread 呼叫 SDK 會被拒。
- **依賴：** B4-03
- **決策：** F9

### B5-04 hook 時間預算

- **驗收：** F15 表的每一列都有測試：1 秒警告與 `HookSlow`、30 秒時的 B 類 crash、`on_ready` / `on_stop` 的 10 秒上限。
- **依賴：** B4-03
- **決策：** F15

### B5-05 MD / TD 失聯通知

- **範圍：** `on_md_update`、`on_td_update`、`md.state`、`td.state`、10 秒靜默判定、ingress 重連時的 down / live 與 `on_resync(reconnect)`；帳號 `unavailable` 時下單在本地回 False。
- **驗收：** §5.6「各種情況」表的每一列都有測試。
- **依賴：** B4-03、B4-05、B4-06
- **決策：** F14、F23

### B5-06 crash 分類、清場與重新掛起

- **驗收：** A、B、C 三類 crash 都能清場；`on_failure` 能從 `on_start` 重新掛起，而且 R1 到 R4 成立；alert log 被既有的 Alert 管線比對到。
- **依賴：** B4-02、B6-03
- **決策：** F10、F11

### B5-07 搬移 artifacts、tape 讀取、fetch、timer

- **驗收：** 這些功能在新的 worker 上都能用；`tape.read` 不再每筆讓出，SDK 文件附上「大量資料用 `offload` 處理」的範例。
- **依賴：** B4-03、B4-06、B5-03

### B5-08 `StrategyHarness` 與策略測試改寫

- **驗收：** 所有內建策略在 `StrategyHarness` 上測試全綠（F16）；內建策略都在 `on_ready` 開始交易。
- **依賴：** B5-01、IF-06
- **決策：** F16

### B5-09 STS worker 不持有任何 DB 連線

- **驗收：** session worker 進程沒有任何 DB 連線，以 driver 的連線計數或 `/proc/<pid>/net` 驗證。
- **依賴：** B5-06
- **決策：** F10

---

## B6 TD 補齊

### B6-01 常駐層：溫熱的 HTTP 連線池

- **範圍：** 各 venue REST client 的 keepalive 調整，以及 adapter 的輕量保活請求；連線池由帳號 worker 持有。
- **驗收：** 閒置 10 分鐘後的第一個 REST 請求不需要重新握手，以連線重用計數驗證；每個 venue 的保活間隔寫在 adapter。
- **依賴：** B4-05
- **決策：** F35

### B6-02 交易層（各 venue）

- **範圍：** Binance（spot / UM / CM）、Bybit、OKX、Deribit、Gate、Bitget 的私有連線、OMS、ledger、recon；依 intent 開關。
- **驗收：** 每個 venue 在 testnet 或 paper 上都能開關交易層；開關時常駐層不受影響。
- **依賴：** B6-01
- **決策：** F34、F35

### B6-03 `td.order.cancel_session`

- **驗收：** 撤掉該 session 的所有掛單並等到確認；`PENDING_NEW` / `UNKNOWN` 的單收斂後一併處理；逾時時回覆未確認的清單。
- **依賴：** B4-05
- **決策：** F10

### B6-04 drain-replace（人工觸發）

- **驗收：** `mftik td drain <api_id>` 期間，新單以可重試的 `td_draining` 拒絕；沒有遺失或重複的單；`TdReady` 經歷 false 後回到 true。
- **依賴：** B6-02、B3-03
- **決策：** F27

### B6-05 backfill 由帳號 worker 處理

- **範圍：** 把 `backfill/` 搬進常駐層；排程和 detach 的觸發；併發限制。
- **驗收：** 沒有 session 的帳號也能 backfill；backfill 進行中，下單延遲不受影響（附量測）。
- **依賴：** B6-01
- **決策：** F35

### B6-06 帳號狀態廣播與 reset

- **驗收：** `td.account.state.*` 依 F14 廣播；殺掉帳號 worker 後，它會重啟、recon、發出 `td.account.reset`，策略收到 `on_resync(account_reset)`。
- **依賴：** B6-02、B5-05
- **決策：** F13、F14

### B6-07 cancel-on-disconnect（倒數計時型）

- **範圍：** Binance UM / CM（逐 symbol）、Bitget UTA、OKX、Gate 的死人開關；帳號層級的設定；drain-replace 之前延長倒數。
- **驗收：** 在 testnet 上 kill -9 帳號 worker，倒數到期時交易所撤單；一般重連不會觸發；各家參數寫進 adapter。
- **依賴：** B6-02、B6-04
- **決策：** F37

### B6-08 `oms.view(settled=True)`

- **驗收：** 有 UNKNOWN 的單時，等到收斂或逾時才回覆。
- **依賴：** B6-02
- **決策：** F13

---

## B7 MD atom

### B7-01 atom 註冊與 subject

- **驗收：** `atom_id` 的正規化和 hash 穩定；同一個 atom 在 controller 重啟前後得到相同的 subject。
- **依賴：** IF-08

### B7-02a 到 B7-02g 各 venue 的 atom 實作

每個 venue 一張：**a** Deribit、**b** Binance（spot / UM / CM）、**c** Bybit、**d** OKX、**e** Gate（spot / futures）、**f** Bitget、**g** Paper。

- **驗收（每張相同）：** 該 venue 現有的每個 product topic 都改由 atom 提供；book 的 fold 和缺口 resync 搬進 `decode` / reconciler；capacity 的實測值寫進 adapter。
- **B7-02a 另外：** 修掉原 #151：`DeribitSocket._read_loop` 的 `retries` 只在剛斷掉的那條連線收過 frame 時才歸零，連續幾次 setup 失敗後，之後一次普通斷線就可能耗盡 `max_retries`。改成 setup 成功、且之後收到 frame 就歸零；回歸測試涵蓋 `_open` 失敗和 `_on_open` / `_restore` 失敗兩條路徑。
- **依賴：** B7-01
- **決策：** F19、F21

### B7-03 STS 端的通用 join 與組合型 feed 的狀態

- **驗收：** Binance UM / CM 的 `ticker` = `join(BestQuote, TickerStats)`；報價還沒到前不輸出；任一組成 atom down 時，feed 就是 down。
- **依賴：** B7-02b、B5-05
- **決策：** F19

### B7-04 tape 改以 atom 為 key，加錄 liquidation

- **驗收：** coverage 以 atom 為單位；Binance UM 的 `trade` 和 `aggtrade` 只錄一份；讀取仍經由 MD，STS 不開 Redis。
- **依賴：** B7-02a 到 B7-02g
- **決策：** F20

### B7-05 MD fetch worker

- **驗收：** `md.fetch` 由獨立的 fetch worker 服務；MD controller 滾動時不中斷。
- **依賴：** B3-03

---

## B8 MD 編排

### B8-01 orchestrator：desired 與 owner

- **驗收：** desired atom 由 intent、常駐訂閱、selector 組成，每個 atom 有 owner 集合；owner 進入 terminal 後由 GC 移除。
- **依賴：** B7-01、B4-07

### B8-02 placement 與連線 worker 的生命週期

- **驗收：** 容量不夠時才開新連線；atom 一旦放上去就不搬（F22）；連線上沒有 atom 時 worker 結束。
- **依賴：** B8-01
- **決策：** F17、F22

### B8-03 reconciler 完整版

- **驗收：** generation 規則（F18）、連線 epoch、token bucket 限速、單一 atom 的 resync；重連後自動補齊。
- **依賴：** B8-02、B7-02a 到 B7-02g
- **決策：** F18

### B8-04 以 listing 驅動到期

- **驗收：** 到期的合約從 desired 移除，並對它的 owner 發出 `md.feed.end(expired)`。
- **依賴：** B8-01

### B8-05 常駐訂閱，`tape_keeper` 退役

- **驗收：** 設定檔裡的常駐訂閱生效，tape 照常錄；`impl/tape_keeper.py` 和它的測試刪除。
- **依賴：** B8-01、B7-04

### B8-06 狀態廣播、原地重啟、列出舊版 worker

- **驗收：** `md.w.*` 依 F14 廣播；`mftik md restart <conn>` 原地重啟並在 coverage 記錄 tape 空洞；`mftik workers --stale` 列出跑在舊版代碼上的 worker。
- **依賴：** B8-03、B3-07
- **決策：** F14、F24

### B8-07 MD 編排驗收

- **驗收：** 滾動 MD controller 時，連線、行情和 tape 都不中斷；原地重啟連線 worker 造成的 tape 空洞都有量測紀錄。
- **依賴：** B8-01 到 B8-06

---

## B9 Selector

### B9-01 `option_chain`

- **驗收：** 依 `expiries`、`min_tte`、`strikes.atm` 選出合約；ref 移動時依 `recenter` 的規則重新置中；ref 斷線或 listing 過期時維持上一份。
- **依賴：** IF-09、B8-01
- **決策：** F33

### B9-02 `rolling_future`

- **驗收：** 依到期日分出 weekly / monthly / quarterly；`roll_before` 時 current 切到下一張；舊合約保留到到期才移除。
- **依賴：** IF-09、B8-01
- **決策：** F33

### B9-03 狀態持久化、規格共用、部署時的上限

- **驗收：** controller 重啟後 universe 和 epoch 不變、不重新置中；規格相同的 selector 只算一份；部署時算出 atom 上限並做容量檢查。
- **依賴：** B9-01、B9-02、IF-14

### B9-04 `md.universe` 與 SDK

- **驗收：** I-SEL1 成立：合約出現在 `added` 之前，策略收不到它的事件；出現在 `removed` 之後也收不到。`self.md.universe`、`self.md.current` 正確。
- **依賴：** B9-03、B5-01
- **決策：** F33

---

## B10 切換

### B10-01 DB migration（刪除部分）

- **範圍：** `rebuild_count` 改名為 `restart_count`；drop `st_facts`、`abort_target`；`md_sessions`、`td_sessions` 停寫。
- **驗收：** migration 在 Postgres 的正式資料快照上演練過一次；models 與 migration 一致。
- **依賴：** IF-14、B5-09
- **決策：** F11、F36、F38

### B10-02 preflight

- **驗收：** 任何 `sts_sessions` 仍是 live 狀態時，拒絕套用切換。
- **依賴：** B10-01
- **決策：** F2

### B10-03 前端 MD / TD 頁

- **範圍：** `frontend/src/routes/md`、`frontend/src/routes/td` 改成顯示 worker 和 intent（F38），以及對應的 API。
  - MD 頁：每個連線 worker 的 venue / endpoint、狀態、incarnation、代碼版本、atom 數、RSS，以及每個 atom 的 owner 和最後一筆資料時間。
  - TD 頁：每個帳號 worker 的狀態、交易層開或關、session 數、incarnation、代碼版本、RSS。
- **驗收：** 資料來自 `procman.report` 和 worker 狀態廣播；`just frontend-check` 和 frontend e2e 通過。
- **依賴：** B8-06、B6-06
- **決策：** F38

### B10-04 runbook 與上線

- **範圍：** 先升級 agent（S-1 到 S-3），這一步會殺掉所有 strategy，所以必須在停掉所有策略之後做；再讓 plane sets 開啟 `oci_host_pid: true`；strategon#61 已上線的話，依 §4.7 設定每個平面的 `memoryBytes`。
- **驗收：** 依 runbook 在空的平面上完成切換；回滾到 `arch/baseline` 演練過一次。
- **依賴：** B10-02、B3-06，以及 B5、B6、B8、B9 全部完成
- **決策：** F2、F6

### B10-05 文件定稿

- **驗收：** README、`ARCHITECTURE.md`、`Deployment.md` 更新為新架構；`REFACTOR_TICKETS.md` 和 `docs/baseline/` 封存到 `docs/archive/`。
- **依賴：** B10-04
