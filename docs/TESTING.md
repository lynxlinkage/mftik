# 測試

怎麼跑、怎麼寫測試。設計上的決定在
[ARCHITECTURE_CHANGE_PLAN.md](ARCHITECTURE_CHANGE_PLAN.md) §9（F30、F31、F47、F16），
票在 [REFACTOR_TICKETS.md](REFACTOR_TICKETS.md)。B3 以後的實作票，新測試照這份寫
（該文件開頭的共同驗收）。

下面的數字、marker、recipe 以程式為準：`pyproject.toml` 的
`[tool.pytest.ini_options]`、根目錄 `conftest.py`、
`packages/common/tests/tier_budget.py`、`packages/common/tests/sleep_guard.py`、
`packages/common/tests/nats_guard.py`、`packages/common/tests/broker_harness.py`、
`justfile`、`.github/workflows/tests.yml`。

## 怎麼跑

### `just test` 與 `just test-int`

`just test` 是 unit 加 component：

```bash
uv run --all-packages pytest packages apps -q -n auto -m "not integration and not e2e"
```

session 開始時 `conftest.py` 會確認 NATS 有在聽。suite 沒有 broker fake。
`justfile` 寫的起法是 `just up nats`。

這一步在 CI 上記 wall time。超過 120 秒，`scripts/check_wall_budget.py` 讓
job 失敗（F30）。判定走 `tier_budget.on_ci`：`CI` 或 `GITHUB_ACTIONS` 有值，
而且不是空字串、`0`、`false`、`no`、`off`。本機這些變數不成立，同一支腳本以
0 結束。120 秒不含 `uv sync`、服務啟動、lint、OpenAPI 比對，也不含下面的
stdlib-loop pass。

`just test-int` 是 integration 與 e2e，不加 xdist：

```bash
uv run --all-packages pytest packages apps -q -m "integration or e2e"
```

這些測試共用一個資料庫，案例之間會清空，所以串行。

### 一個 tier 或一個檔案

`just test` 不接受路徑。直接叫 pytest，過濾跟 recipe 同一套：

| 想跑 | marker 運算式 |
|---|---|
| unit 加 component（`just test` 的集合） | `not integration and not e2e` |
| 只有 unit | `not component and not integration and not e2e` |
| 只有 component | `component and not integration and not e2e` |
| integration | `integration` |
| e2e | `e2e` |
| 一個檔案 | `uv run --all-packages pytest path/to/test_file.py -q` |

Postgres 參數自己帶 `integration`。函式上若另有 `component`，該參數仍然是
integration：`tier_budget.tier_of` 的順序是 e2e、integration、component，
剩下的才是 unit。只想要 component 時把 `integration` 排除，避免把 Postgres
那一組也選進來。

要跟 `just test` 一樣平行，加上 `-n auto`。`just test-int` 的那一組維持串行。

### event loop

預設 uvloop。正式環境的程序都跑 uvloop，suite 跟它們用同一個 loop。
`MFTIK_TEST_LOOP=asyncio` 改跑標準庫 loop。其他值是 `UsageError`。環境裡沒有
uvloop 也是 `UsageError`，不會靜默換回標準庫。一次只註冊一個 loop factory，
測試 id 不會因為 loop 變成兩倍。

CI 的 `unit` job 在 `just test` 之後另跑一輪，只跑 `packages`，tier 過濾與
`just test` 相同，環境變數 `MFTIK_TEST_LOOP=asyncio`：

```bash
uv run --all-packages pytest packages -q -n auto -m "not integration and not e2e"
```

這一輪不計入 120 秒。call-phase 的 warning 規則仍然適用，因為這一步沒有清掉
`CI`。它不跑 `packages` 的 integration 測試，也不跑 `apps`。見文末。

### Postgres

`just test` 不跑 Postgres。repository 測試的 sqlite 參數是
`sqlite+aiosqlite:///:memory:`，留在 component。

設了 `TEST_POSTGRES_URL`，同一批測試多一組 Postgres 參數，marker 是
`integration`，落在 `just test-int`。CI 的 integration job 另設
`MFTIK_REQUIRE_POSTGRES`：URL 沒設，session 直接失敗，不會悄悄少一種方言。

`just test-pg` 會再跑一輪，並對 compose 的 `postgres` 服務執行 `createdb`，
然後用 `TEST_POSTGRES_URL`（沒設則用 recipe 裡的預設）啟動 pytest。只用在
你自己的 scratch 資料庫。不要對共享資料或正式資料跑，也不要把主機名稱或連線
字串寫進 repo。CI 不走這條 recipe：integration job 自己設好
`TEST_POSTGRES_URL` 之後跑 `just test-int`。

### CI

`.github/workflows/tests.yml` 兩個 job，都在 `ubuntu-latest`，各自起 NATS：

- `unit`：`just lint` 那句 ruff（`uv run --all-packages ruff check packages apps conftest.py`）、`contracts/openapi.json` 比對、`just test`、上面的 stdlib-loop pass。
- `integration`：先 `alembic upgrade head` 與 `alembic check`，再 `just test-int`。

`release.yml` 的測試 job 同樣跑 `just test` 與 `just test-int`。

## Tier

§9.1 的四層。允許與禁止是選 tier 的依據；上限與「跑在哪」是程式在執行的閘門。

| tier | marker | 允許 | 禁止 | call phase 上限 | 跑在 |
|---|---|---|---|---|---|
| unit | （沒有） | 純函式、直接呼叫的 handler、in-memory fake（broker 除外）、`FakeClock` | 網路、子進程、真的 sleep、NATS、DB 檔案 | 50 ms | `just test` |
| component | `component` | 共用連線的真 NATS、sqlite `:memory:`、`FakeClock`、in-process 的 procman fake、loopback websocket 上的 venue stub | 每個測試自己連 NATS、子進程、Postgres、wall-clock sleep | 500 ms | `just test` |
| integration | `integration` | 真 NATS、Postgres、真的 Supervisor 和 shim 子進程 | — | 10 s | `just test-int` |
| e2e | `e2e` | compose stack | — | 沒有單測上限 | `just test-int` 會收集這個 marker |

怎麼選：

1. 不開 socket、不生子進程、不睡牆鐘、不寫 DB 檔：不打 marker。
2. 要真的 NATS，但只借這個 xdist worker 已經握著的那條連線，或是 sqlite `:memory:`、loopback 上的 venue stub：`component`。
3. 要自己的 NATS socket、Postgres、或真的子進程：`integration`。
4. 要整個 compose stack：`e2e`。`apps/sts/tests/test_b4_09_e2e.py` 在 `MFTIK_E2E_COMPOSE` 不是 `1` 時 skip。

§9.1 把 e2e 的「跑在」寫成 release 前。recipe 用 `-m "integration or e2e"`，
所以 PR 的 integration job 也會收集 `e2e`。compose 那支在沒設環境變數時
skip；用來證明 tier 豁免的短測試（`packages/common/tests/test_clock.py` 的
`test_e2e_tier_may_sleep_for_real`、`packages/common/tests/test_private_nats.py`）
以及 `apps/sts/tests/test_session_worker_integration.py` 的 30 秒 hold，會在
每次 `just test-int` 裡真的跑。

## 預算與閘門

兩種時鐘。

**Wall time（F30）。** `just test` 那一步，在 `ubuntu-latest` 上 120 秒
（`tier_budget.WALL_LIMIT_S`）。CI 上超過就失敗。本機 `on_ci()` 為假，
`scripts/check_wall_budget.py` 以 0 結束。stdlib-loop pass、lint、OpenAPI、
`uv sync`、服務啟動都不算進這 120 秒。

**Call phase（F47）。** pytest 記在 `call` 上的秒數，不含 fixture 的 setup。
上限在 `tier_budget.CALL_LIMIT_S`：unit 0.05 秒、component 0.5 秒、
integration 10 秒、e2e 沒有。比較的是「超過」：剛好 50 ms 仍算過。

本機上，unit 或 component 超限，該測試失敗。CI 上，這兩個 tier 維持
passed，終端機印出 `call-phase budget warnings`，並輸出 `::warning::`
（annotation 最多 20 條，`tier_budget.ANNOTATION_CAP`；摘要裡每一筆都在）。
integration 的 10 秒在本機和 CI 都讓該測試失敗。已經失敗的測試維持原來的
失敗。帶 `xfail` marker 的測試，這道閘門不改寫報告。

**pytest-timeout。** 測試掛住、沒有 call 時間可以量的時候的後盾（§9.2 規則
8）。`pyproject.toml` 設 `timeout_func_only = true`，方法是 `thread`，所以
量的是呼叫本身。秒數在 `tier_budget.HANG_TIMEOUT_S`，都高於 call-phase
上限，避免卡在上限上把 xdist worker 殺掉（`Not properly terminated`）：

| tier | pytest-timeout |
|---|---|
| unit | 5 秒 |
| component | 30 秒 |
| integration | 60 秒 |
| e2e | 0（關掉） |

測試自己寫了 `@pytest.mark.timeout`，`apply_timeouts` 會留著，不覆蓋。

**`asyncio.sleep`。** unit 與 component 裡，repo 內的程式呼叫
`asyncio.sleep(x > 0)` 會 raise `sleep_guard.RealSleepForbidden`，訊息帶
呼叫點的檔案、行號、函式名。`asyncio.sleep(0)` 只是讓出，放行。
`site-packages` 裡的呼叫（nats-py 的 ping、websockets 的 keepalive）不計。
`integration` 與 `e2e` 放行。`component` 不是豁免。

還是要牆鐘的 unit 或 component 測試：

```python
@pytest.mark.real_sleep(reason="why this test waits on the wall clock")
```

`reason` 必須是非空字串。`packages/common/tests/test_nats_transport.py` 裡等
真的副作用的案例是這個 marker 的用法。

unit 與 component 再開一條自己的 NATS socket，會 raise
`nats_guard.PrivateNatsForbidden`。共用 client 的名字以 `mftik-pytest-`
開頭，這條放行。`integration` 與 `e2e` 可以自己連。

## 規則

每一條對應 §9.2。範例是 repo 裡已經在跑的測試。

### 1. 時間注入

controller、worker、reconciler 從 `mftik.clock.Clock` 讀時間：`now`、
`monotonic`、`sleep`。測試用 `FakeClock.advance` 推進。

`packages/common/tests/test_clock.py` 的 `test_advance_wakes_sleep`：
`FakeClock` 上 `sleep(5)`，`advance(4)` 還不醒，`advance(1)` 之後醒，
`monotonic` 與 `now` 都是 5。中間的 `asyncio.sleep(0)` 只是讓 coroutine 跑到
await。

`packages/common/tests/test_procman_supervisor.py` 的
`test_start_timeout_fires_at_the_deadline_and_kills` 把 `FakeClock` 交給
狀態機：59 秒還在 `STARTING`，`advance(60)` 之後才到 deadline。

`apps/sts/src/mftik_sts/impl/chase.py` 仍有自己的 `asyncio.sleep`。碰到那條
路徑的案例在 `apps/sts/tests/test_chase.py`，檔案標 `component`，個案再標
`real_sleep`。這些策略測試改寫到 `StrategyHarness` 是 B5-08。

### 2. 連線和收到之後的行為分開，不做 broker fake（F31）

沒有 in-memory broker。

**連線測試**用真的 NATS 和共用連線，標 `component`。broker 自己的語意在
`packages/common/tests/test_nats_transport.py` 與 `test_broker*.py`。
`test_a_request_is_answered_by_its_handler` 走 `broker` fixture，確認一則
request 由它的 handler 回答。

**行為測試**不碰 NATS，直接呼叫 handler。
`packages/common/tests/test_broker_handler.py` 的
`test_a_handler_is_a_call_from_one_message_to_its_answer` 呼叫
`health_handler(...)(probe)`，沒有 subject、沒有 inbox。同一檔後面經過
`serve` 的案例標 `integration`，因為那個本地 `broker` fixture 用的是
`a_broker`（私有連線）。

**共用連線：** 每個 xdist worker 一條。`broker_harness.nats_connection` 是
session fixture，綁在 session event loop；沒有 xdist 時 worker 名是 `gw0`。
每個測試的 `broker` fixture 拿自己的 `key_prefix`，結束時退訂這個 prefix，
不關連線。client 名稱是 `mftik-pytest-<worker>`。
`packages/common/tests/test_broker_shared_connection.py` 的
`test_two_prefixes_share_the_connection_and_leave_it_up` 斷言兩個 prefix
共用同一條 socket，退訂之後連線還在。用這條 fixture 的 async 測試加上
`broker_harness.session_loop`（`pytest.mark.asyncio(loop_scope="session")`）。
`packages/common/tests/conftest.py` 只把 `broker` 與 `nats_connection` 註冊給
`packages/common/tests` 底下的測試。同名的本地 fixture 會蓋掉它；
`apps/` 裡許多 `broker` 仍呼叫 `a_broker`，那些檔案標 `integration`
（例如 `apps/td/tests/test_order_rpc.py`）。

每種 worker 一支接線 smoke，證明它服務自己的 subject。
`packages/common/tests/test_plane_serves_its_subject.py` 仍開私有連線，所以
標 `integration`：unit 與 component 不允許那條 socket。

### 3. reconciler 與 generator 用表格

純函式，一列一個案例：`reconcile(desired, observed) -> actions`、
`evaluate(listing, refs) -> desired`。

已在跑的表格：`packages/common/tests/test_procman_contract.py` 的
`test_reattach_follows_the_section_4_4_table`。`_reattach_rows()` 列出
plane、desired、observed 與預期的 `ReattachAction`，`reattach_action` 對
每一列回傳那個 action。

MD 的 `mftik_md.conn.reconcile(desired, observed)` 與
`mftik_md.selector.evaluate(listing, ref, now, prev, spec=...)` 是同一種
形狀。`evaluate` 目前 `raise NotImplementedError`，案例在
`apps/md/tests/test_selector.py`，以 `xfail(strict=True)` 掛著，`reason`
指向 B9-01 或 B9-02。連線 diff 的案例在 `apps/md/tests/test_conn.py`，
`reason` 指向 B8-03。

### 4. worker 以 in-process 測，真的子進程只在 integration

`apps/sts/tests/test_controller_flow.py` 用 `FakeSupervisor` 與 `FakeClock`
驅動 `StsOrchestrator`。`test_converge_spawns_once_and_observe_publishes_running`
看一次 spawn 與 running 的快照。沒有 shim、沒有 sleep、沒有 marker，
call-phase 預算是 unit 的 50 ms。

真的子進程標 `integration`。`apps/sts/tests/test_session_worker_integration.py`
用真的 `Supervisor` 起 session worker。`packages/common/tests/test_procman_supervisor.py`
裡 spawn shim 的案例標 `integration`；相位變化本身仍是 unit，走 `FakeClock`。

§9.2 規則 4 把「直接跑 worker、procman 用 fake」寫在 component。上面這支
controller 測試沒有 NATS、也沒有 sqlite，程式把它留在 unit。

### 5. 策略以 `StrategyHarness` 測

目標是一個 in-process 的假 session：注入事件、斷言送出的單，不依賴任何平面
（§9.2 規則 5，F16）。

型別在 `mftik.strategy.harness.StrategyHarness`（IF-06）。驅動方法目前都
`raise NotImplementedError("IF-06")`。實作、以及內建策略測試的改寫，是
B5-08（#217），還沒合併。不要照一份還沒實作的呼叫順序寫新測試。

既有的策略測試先留著，例如 `apps/sts/tests/test_chase.py`。要成立的行為寫在
`packages/common/tests/test_strategy_sdk_contract.py`，每一支都是
`xfail(strict=True)`，`reason` 寫把牠轉綠的票。

### 6. sqlite 在 component，Postgres 在 integration

repository 在 component 用 sqlite `:memory:`（`packages/db/tests/db_harness.py`
的 `SQLITE_URL`）。`tier_budget.database_params` 把 sqlite 參數標
`component`、Postgres 參數標 `integration`。
`packages/db/tests/test_sts_session_repository.py` 的 `db` fixture 吃
`database_url`，同一支測試因此落在兩個 tier：sqlite 進 `just test`，
Postgres 在設了 `TEST_POSTGRES_URL` 時進 `just test-int`。

sqlite 不檢查 `VARCHAR` 長度，也沒有 decimal。欄位太短這類問題靠 Postgres
那一輪，也就是 CI 的 integration job。

### 7. 每個 bug fix 附一支能重現的、最低 tier 的測試

`apps/api/tests/test_stats_status_coverage.py` 的
`test_every_status_is_counted_by_domain_stats` 是 unit。`failed` 曾經沒有
自己的計數，那些 session 從首頁消失；下一個新狀態若沒被算進去，這支會失敗。

做得到 unit 就留在 unit。要真的子進程才看得到的，例如發行版版本對不對
（#94），在 `packages/common/tests/test_dist_version.py`，整檔
`integration`，因為 `uv build` 是子進程。

### 8. 每個 tier 都有 pytest-timeout；只等明確的 event 或 future

秒數見上面的 pytest-timeout 表。測試不依賴 task 誰先被排到。

`packages/common/tests/test_settled_subject.py` 的
`test_serve_stays_sequential_unless_the_handler_returns_detached` 用
`asyncio.Event`（`first`、`release`、`both_on_the_wire`）把「第二則已經在
線上、handler 還沒開始處理」釘住。它標 `component`，走共用的 `broker`。

## IF 的契約測試

`REFACTOR_TICKETS.md` 的 IF 共同驗收：介面先回 null data，並附
`@pytest.mark.xfail(strict=True)`。`reason` 寫之後讓它轉綠的票。`strict`
成立時，行為一旦通過，這支變成 unexpected pass，整次 pytest 失敗。實作那張
票拿掉 marker，契約才變成正式測試。

一張實作票只拿掉自己轉綠的 marker。`reason` 還指著別張票的，留著。

```python
@pytest.mark.xfail(
    strict=True, reason="B8-03 zeroes observed on reconnect and backfills"
)
def test_a_reconnect_zeroes_observed_and_the_next_diff_backfills() -> None:
    ...
```

這支在 `apps/md/tests/test_conn.py`。同一慣例的其他檔案：
`apps/md/tests/test_selector.py`（B9）、`apps/md/tests/test_controller.py`（B8）、
`apps/sts/tests/test_session_worker_contract.py`、
`apps/td/tests/test_account_contract.py`、
`packages/common/tests/test_strategy_sdk_contract.py`（B5-08 等）、
`packages/common/tests/test_broker_handler.py` 的
`test_the_three_planes_answer_through_this_layer`、
`packages/common/tests/test_cli_operator_contract.py`、
`packages/common/tests/test_exchange_atoms.py`。

## 量測測試

時間上限是健全檢查。CI 上偶發超過時，放寬上限，不刪測試（#344）。

`apps/sts/tests/test_b4_04_measure.py`（`integration`）：

- `test_submit_without_td_waits_out_the_ack`：`1.6 < elapsed_s < 3.0`
- `test_ack_hop_smoke`：idle / md 的 hop p99 `< 0.1` 秒；CPU hook 的 hop p99 `< spin_s + 0.25`，hook hold p99 `< 0.05`
- `test_gil_switch_interval_smoke`：heartbeat overrun 的 max `< 0.5`，inbox delay 的 max `< 0.25`

`packages/common/tests/test_b4_04_no_responders.py` 裡
`probe_product_connections` 的 `same_elapsed_s < 0.2` 也是這種上限。同一檔
另外鎖住「503 只寫在送出連線上」：nats-py 或 nats-server 升級後這裡失敗，是
預期的訊號。

## 已知缺口

CI 的 stdlib-loop pass 只跑 `packages`，而且帶
`-m "not integration and not e2e"`。`packages` 的 integration 測試不在
asyncio loop 上跑。它們仍由 integration job 以預設的 uvloop 跑。正式環境
全部是 uvloop，所以不另補一輪 stdlib integration（#303）。
