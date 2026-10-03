# ARCHITECTURE

目標架構，摘自 [ARCHITECTURE_CHANGE_PLAN.md](ARCHITECTURE_CHANGE_PLAN.md) v0.38（F1–F47）。本文只寫系統是什麼。括號裡的 § 與 F 編號指那份計畫。F42 的重啟曲線、以及附錄 D 收錄的常數，不在這裡複述。工作票見 [REFACTOR_TICKETS.md](REFACTOR_TICKETS.md)。

STS、MD、TD 的執行單位是 worker 進程。平面 controller 只做控制面，可以隨 release 滾動而不中斷 worker。行情、下單、回報只在 worker 與 NATS 之間流動。API 只負責開始與結束。（§0、§2.1、F1）

## 1. 原則

（§2.3）

- **P1 控制面與資料面分離。** 資料不經過 controller。controller 不在，代表暫時不能改變狀態，不代表系統不能運作。
- **P2 Spec / Status，level-triggered。** 宣告寫在 Spec，系統只寫 Status。以 `generation` / `observedGeneration` 判斷收斂到哪一版。動作必須冪等。
- **P3 活性不等於進度。** 活性由 shim 證明：進程在、而且能回應。進度（hook 延遲、feed 是否過期）是 Status 上的 condition。
- **P4 At-most-one。** 同一個 session、account，或同一個原子訂閱的發佈者，任何時刻只有一個 incarnation 產生副作用。fencing token 是 `(id, incarnation)`。STS 與 TD 的替換是 delete-before-create。MD 不做連線遷移（F22）；連線 worker 原地重啟時同樣是 delete-before-create。
- **P5 Fail-static。** controller 消失時，worker 維持最後一份 desired，不在資訊不足時做破壞性動作。
- **P6 平面特化。** procman 只認識進程。STS、MD、TD 的語意留在各自的 orchestrator。
- **P7 不以訊號消失推論狀態。** 能由權威來源直接觀測的（worker 是否存在、feed 最後一筆的時間），就不用週期訊號的缺席去推斷。唯一例外是 MD／TD 廣播靜默時發給策略的失聯通知（§5.6、F14）：只通知，不回收資源。資源回收一律依權威觀測（§8.2、F32）。

不做的事（§2.2）：多主機排程（placement 只在單一 instance 內）；SYM 與 Paper 的結構調整（它們只配合新協定）；重寫 venue adapter 的 wire 程式（只抽出 atom 介面）；把 Supervisor 或 shim 改寫成 Rust。shim 用 Python，只用標準庫（F29）。

切換是破壞性的：協定、DB、SDK 一次換掉，沒有相容層（F1）。切換前必須先停掉所有運行中的策略，平面上不能有 running session（F2）。

## 2. 分層與狀態權威

（§3）

```
API（只做 start / end）
    │  控制 subject（NATS request-reply）
    ▼
STS controller          MD controller           TD controller
orchestrator+Supervisor orchestrator+Supervisor orchestrator+Supervisor
    │  shim socket（NDJSON，${WORK_DIR}/run/*.sock）
    ▼
shim → worker           shim → worker           shim → worker
資料面：worker ⇄ NATS ⇄ worker（不經過 controller）
```

每個平面的 controller 是一個 Strategon OCI assignment，開 `oci_host_pid`，每個 tag 滾動；開機時 reattach。shim 由 host init 收養。（§3、F6）

### 2.1 Worker

（§3.1）

| 平面 | kind | 身分 | 擁有 | 直接服務／發佈 |
|---|---|---|---|---|
| STS | `session` | `session_id` | 策略實例、event log、timer、artifact handle。ingress thread 持有接收連線，strategy thread 持有送出連線（§5.3） | 服務 `sts.ctl.{session_id}`（stop、fail、status）；訂閱 `md.a.*` 與 `td.*` |
| MD | `conn` | `(venue, endpoint, n)` | 一條公用 websocket、該連線的 reconciler、decoder、tape append | 發佈 `md.a.{venue}.{hash}`（§6.1） |
| MD | `fetch` | instance | REST readers | 服務 `md.fetch` |
| TD | `account` | `api_id` | 常駐：HTTP 連線池、backfill。有 intent 時加上交易層：私有 websocket、OMS、ledger、recon、槓桿快取（F35） | 服務 `td.order.{api_id}`、`td.account.{api_id}`、`td.oms.*`、`td.ledger.*`；發佈 `td.{api_id}.global` |

worker id 的形狀是 `sts/session/{session_id}`、`md/conn/{venue}/{endpoint}/{n}`、`td/account/{api_id}`。（§4.3）

和 K8s 的對照：Postgres 上的 Spec／Status 表加各平面的控制 subject，對應 etcd 與 API server；orchestrator 是 controller-manager；嵌在 controller 裡的 Supervisor 是 kubelet；`mftik-shim` 是 containerd-shim；worker 是 Pod。intent 的 `owner = session_id` 在 owner 進入 terminal 後回收，角色接近 ownerReferences。session 的 `MdReady`、`TdReady` 是 readiness。STS／TD 的 incarnation fencing 是 StatefulSet 的 at-most-one。MD 連線不遷移（F22）。（§3.2）

### 2.2 狀態的權威

每一種狀態只有一個寫入者。其他人向它讀，或聽它廣播。重啟或失聯之後，只由它收斂。（§3.3）

**控制面（宣告）**

| 狀態 | 權威 | 存放 | 誰讀 | 失聯後 |
|---|---|---|---|---|
| session spec：策略、參數、`restart`、timeout、釘住的 `(strategy_digest, env_generation)` | API（start 時寫入釘住的代碼身分，F39） | Postgres `sts_sessions` 的 Spec 欄位 | STS controller | DB 就是權威。釘住的一組在 session 生命週期內不變，重新掛起沿用 |
| session status：phase、conditions、incarnation、`restart_count`、失敗原因 | STS controller 的 Supervisor | `sts_sessions` 的 Status 欄位；即時版在 `sts.status.{session_id}` | API、UI、CLI | controller 重啟後由 reattach 對帳重算（§4.4） |
| MD intent：session 要哪些 feed 與 selector | API（start）、STS controller（自癒時重新 put）、session worker（執行期間的 subscribe，經 `md.intent.patch`） | Postgres `md_intents` | MD controller | level-triggered；owner 依 §8.2 規則 3 回收 |
| TD intent：session 用哪些帳號 | API、STS controller | Postgres `td_intents` | TD controller | 同上 |
| 常駐訂閱 | 設定檔 | Postgres `md_standing_subscriptions` | MD controller | — |
| `api_id` → instance 綁定、帳號設定（例如 cancel-on-disconnect） | 使用者經 API。綁定不可經 API 修改（F45） | Postgres `apis` | TD controller | — |
| listing：合約、到期、strike | SYM | Postgres `symbol_*` | MD controller（selector、到期）、TD | 每小時刷新 |
| 策略樹目錄：有哪些 name、各自目前的 digest | API（push、delete、pull） | API 主機的 `MFTIK_DATA/registry` | STS controller（同步副本）、API 的 deploy 驗證 | — |
| extras 目錄：目前的 generation 與 pins | API（env apply） | API 主機的 `MFTIK_DATA/env/applied.json` | STS controller（同步副本） | — |

**進程層**

| 狀態 | 權威 | 存放 | 誰讀 | 失聯後 |
|---|---|---|---|---|
| worker 是否存在、exit code、signal | shim（親眼看到） | `${WORK_DIR}/run/<id>.sock`、`<id>.exit.json` | Supervisor | controller 重啟時 reattach 讀回 |
| 每個 instance 上、回收 intent 所依據的 worker 集合 | Supervisor；STS 的 orchestrator 會把 `restarting`、當下沒有進程的 session 補進同一份報告（R4） | `procman.report.{plane}.{instance}`（不落地） | MD／TD orchestrator | 報告整個停止時不回收任何東西（F32）。缺席次數見 §8.2 |
| worker 實際跑的代碼：`code_ref`、`strategy_digest`、`env_generation`，以及 `pv` | Supervisor（spawn 時記下；後兩者在 `WorkerSpec.labels`；`pv` 是 spawn 它的 controller 的常數，F41） | `supervisor.json`、`procman.report` | CLI（`mftik workers --stale`）、API（deploy 時比對 `pv`） | — |

**MD**

| 狀態 | 權威 | 存放 | 誰讀 | 失聯後 |
|---|---|---|---|---|
| 每條連線的 desired atom 與 generation | MD controller | 記憶體，可由 intent、常駐訂閱和 selector 狀態重算 | 連線 worker | controller 重啟後重算；新 generation 推出前，worker 維持舊的（P5） |
| selector 的 universe、epoch、置中狀態 | MD controller | Postgres（selector 狀態表） | MD controller；session 經 `md.universe.{session_id}` | 從 DB 接續，不重新置中 |
| 連線上實際訂閱成功的 atom（observed） | 連線 worker 的 reconciler，以交易所 ack 為準 | 記憶體 | 連線 worker、狀態廣播 | 重連後歸零，下一輪 diff 補齊 |
| 行情內容，包括 fold 後的 book（F21） | 連線 worker | 記憶體 → `md.a.*` | STS session | 重連後由交易所的 snapshot 重建 |
| feed 狀態 live / down | 連線 worker | `md.w.*` 廣播 | session ingress | 廣播靜默超過 F14 的窗口視為 down，只通知（§5.6） |
| per-atom `seq` | 連線 worker | envelope 的 optional 欄位 | 策略自行偵測不連續（F25） | 以 (atom, 連線 epoch) 起算。重連、單一 atom 的 resync、換 incarnation 之後從 1 重新起算 |
| tape 與 coverage | 持有該 atom 的連線 worker | Redis（每個 region 一台） | MD 的讀取 RPC → 策略 | 空洞記在 coverage |

**TD**

| 狀態 | 權威 | 存放 | 誰讀 | 失聯後 |
|---|---|---|---|---|
| 交易所上的掛單、部位、餘額 | 交易所 | — | TD 的 recon | — |
| OMS、ledger（預扣、available） | TD 帳號 worker 的交易層（F13） | 記憶體 | session（`oms.view`、帳號事件） | 重啟後以 `reconcile()` 從交易所重建，發出 `td.account.reset`，策略收到 `on_resync` |
| 交易層開或關 | desired：TD controller（依 intent）；observed：帳號 worker | 記憶體 | — | controller 不在時，worker 維持最後一份（P5） |
| 帳號狀態 ready / degraded / unavailable | TD 帳號 worker | `td.account.state.{api_id}` 廣播 | session ingress | 靜默超過 F14 的窗口視為 unavailable，只通知 |
| 訂單歷史、成交、資金流水 | TD 帳號 worker（live 寫入加 backfill） | Postgres `orders`、`fills`、`cash_flows`、`backfill_cursors` | API、UI | backfill 依交易所補正 |

**STS session**

| 狀態 | 權威 | 存放 | 誰讀 | 失聯後 |
|---|---|---|---|---|
| 策略內部狀態 | session worker | 記憶體，不落地（F10） | 策略 | 重新掛起時從 `on_start` 全新開始 |
| `client_order_id` 序號 | session worker | 記憶體（`session24 \| ts_sec28 \| seq8`） | TD | R2：不撞號 |
| event log | session worker 的 ingress | 檔案（`STS_EVENTLOG_DIR`） | 事後分析；API 經 STS controller 讀（F40） | — |
| hook 進度、offload 進度、交付的丟棄計數 | session worker 的 ingress | `sts.status.{session_id}` | UI | — |

**STS 主機磁碟（F39、F40）**

| 狀態 | 權威 | 存放 | 誰讀 | 失聯後 |
|---|---|---|---|---|
| 策略樹與 extras 的副本 | STS controller（只依 API 的 fan-out 與開機 catch-up 寫入） | STS volume 的 `registry/trees/<digest>/`、`env/gen-{N}` | session worker（載入）、controller（可部署檢查） | 開機向 API 補差額；被非 terminal session 釘住的版本不回收 |
| artifacts | 主機上的 artifact volume。兩條寫入路徑共用 `ArtifactStore`（part 檔加 rename）：operator 經 STS controller，策略在自己的 worker 裡直接寫 | 檔案（`STS_ARTIFACT_DIR`） | 策略（本地磁碟）、API（經 STS controller） | 檔案留在 volume 上；未 commit 的上傳由 controller 清理 |

**版本**

| 狀態 | 權威 | 存放 | 誰讀 | 失聯後 |
|---|---|---|---|---|
| 協定版本 `pv` | 代碼常數 | NATS header `Mftik-Pv`。worker 的 `pv` 記在 `WorkerSpec`，經 `procman.report` 帶出 | 每個進程的 transport；API（deploy 時） | deploy 時比對，不符以 `protocol_mismatch` 拒絕。執行中不符的 frame 由 transport 丟棄並計數（F26、F41） |

**報告缺席是權威觀測，但不是「主機已死」。** `procman.report` 整份停止時，沒有人能說 session 是否還活著，這時不回收任何 intent（F32、P7）。常見原因是 controller crash loop，或壞版本等著回滾；session 還在跑，回收會切斷行情。機器重開時，STS Supervisor 會從 shim 的 exit 紀錄看到 session 已死並清場。機器永久消失時，由人工執行 `mftik intents gc --instance <name>`。回收 intent 不會撤單。

### 2.3 層

（§3.4）

| 層 | 模組 | 持有的權威 |
|---|---|---|
| 時間 | `mftik.clock` | 無。`Clock` 提供 `now`、`monotonic`、`sleep`；測試用 `FakeClock` |
| 進程管理 | `mftik.procman` | shim 看見的進程事實；Supervisor 寫入的 worker status |
| 訊息處理 | `mftik.broker.handler` | 無。handler 是「解碼後的訊息 → 回覆與副作用」 |
| 協定 | `mftik.protocol` | 只有 `pv` 這個代碼常數 |
| STS controller | `mftik_sts.controller` | SessionSpec 與 worker status 的 reconcile；crash 分類與重啟 |
| STS session worker | `mftik_sts.session_worker` | 策略記憶體、event log、交付 |
| SDK | `mftik.strategy` | 無。hook 與 `StrategyHarness` |
| STS 主機磁碟 | `mftik_sts.hostdisk` | 以 digest 定址的副本、版本釘住與 GC；controller 上的 registry、env、artifact、event log handler（§5.7） |
| MD adapter | `mftik.exchange.<venue>.atoms` | 無。`Atom`、`atoms_for`、`decode`、`capacity`、`join_policy` 是純函數 |
| MD controller | `mftik_md.controller` | desired atom、selector |
| MD 連線 worker | `mftik_md.conn` | observed 訂閱、行情、tape、狀態廣播 |
| MD fetch worker | `mftik_md.fetch` | 無持久權威；服務 `md.fetch` |
| TD 帳號 worker | `mftik_td.account` | 交易層的 OMS 與 ledger；帳號狀態廣播 |
| TD controller | `mftik_td.controller` | 哪些帳號該有常駐 worker、交易層的 desired |
| API | `mftik_api.orchestrate` | start／end 寫下的 Spec 與 intent |
| DB | `mftik_db` | 上表裡存放在 Postgres 的那些列 |

## 3. procman

（§4）

Supervisor（`mftik.procman`）嵌在每個平面的 controller 裡，負責要不要啟動、何時重啟、如何判定失敗、reattach。三個平面共用這套函式庫，差別在 `WorkerSpec` 和重啟策略。（§4.1、§4.3）

`mftik-shim` 是每個 worker 一個常駐小進程，也是 worker 真正的父進程。Supervisor 先 `Popen` 一個短命的中間進程，由它 fork 出 shim 並 `setsid`，所以 shim 從一開始就不是 controller 的子進程。不用 `asyncio.create_subprocess_exec` 啟動 shim。runner 的入口是 `python -m mftik_<plane>.worker`。沒有常駐的 procd。（§4.1、F6）

### 3.1 shim

（§4.2）

- **S1** shim 是 worker 唯一的父進程。Linux 上以 `PR_SET_CHILD_SUBREAPER` 收 worker 子孫的屍。shim 自己被 host init 收養。
- **S2** shim 消失時，worker 自行 graceful stop。觸發是 status pipe 寫入得到 EPIPE，或 `PDEATHSIG=SIGTERM`（指向 shim），哪個先到都算。
- **S3** shim reap worker 之後寫 `<id>.exit.json`（tmp 檔再 rename），要等 Supervisor 送出 `release` 才退出。controller 不在線時，exit code 和 signal 也不遺失。
- **S4** shim 持有 worker 的 stdio 和 status pipe，寫成 log 並輪替。Supervisor 不在時，worker 不會因為 SIGPIPE 或 buffer 寫滿而卡住。
- **S5** shim 開 unix socket，協定是 NDJSON：`status`、`signal`（對 worker 的 process group `killpg`）、`watch`、`release`。路徑是 `${WORK_DIR}/run/<worker_id>.sock`。Supervisor 靠 socket 路徑找回 worker，不記 pid。
- **S6** status pipe 的單筆訊息不超過 `PIPE_BUF`（4096 bytes），寫入是原子的。pipe 滿了就丟掉這一筆，下一次 heartbeat 帶完整狀態。STS 的 heartbeat 由 ingress thread 寫，不經過策略的 loop。MD／TD 不跑使用者代碼，heartbeat 跟主 loop 綁在一起：loop 卡住就代表 worker 壞了。
- **S7** shim 收到 SIGTERM 時轉送給 worker，自己不先退出。shim 不認識 STS／MD／TD，也不做重啟決策。

### 3.2 Spec 與狀態機

（§4.3）

`WorkerSpec` 是 frozen 的：`id`、`plane`（`sts` / `md` / `td`）、`kind`、`incarnation`（controller 分配）、`argv`、`env`、`code_ref`（spawn 它的 controller 的 release）、`restart`（`never` / `on_failure`）、`start_timeout_s`、`hb_timeout_s`（`None` 表示不以 heartbeat 判死）、`oom_score_adj`、`rlimit_data_bytes`（可選，shim 在 exec 前套用）、`stop_grace_s`、`labels`。STS session 的 `labels` 帶 `strategy_digest` 與 `env_generation`（F39）；procman 不解讀。`pv` 記在 spec 上，經 `procman.report` 帶出（F41）。

```
STOPPED ─▶ STARTING ─ready─▶ RUNNING ─SIGTERM─▶ STOPPING ─▶ STOPPED
              │ 死亡／逾時        │ 死亡／heartbeat 逾時
              ▼                  ▼
            FAILED            CRASHED ─▶ BACKOFF ─▶ STARTING
                                 └─（window 內重啟超標）─▶ FATAL
存活中 ─shim 消失─▶ LOST
```

`FATAL` 只屬於 STS（F11）。MD `conn`、TD `account`、MD `fetch` 不進入 `FATAL`（F42）。

| kind | restart | heartbeat | 失敗時 |
|---|---|---|---|
| STS `session` | deploy 的 `restart`。預設 `never` → `failed`。`on_failure` 只對 A 類 crash 生效：平台清場後從 `on_start` 重新掛起（F10、F11） | 只看 ingress thread 的 beat。`hb_timeout_s` 只抓整個進程卡死。hook 預算另計（F15） | `max_restarts` 預設 5，`restart_window_s` 預設 600，backoff 最短 1 秒。超標 → `failed` 並發 alert。B、C 類與 `on_ready` 之前的失敗不重啟 |
| MD `conn`、TD `account`、MD `fetch` | `on_failure`，不設 `FATAL`（F42） | loop heartbeat。timeout 與 F14 的靜默窗口相同，常數見附錄 D | crash 後依有上限的指數 backoff 加重試 jitter 再啟動；連續 `RUNNING` 一段時間後次數歸零；次數到門檻發 crash-loop 告警，歸零時解除。常數見附錄 D |

ready 只代表本地初始化完成：argv 與設定解析完、憑證載入、NATS subject 答得到（F42）。交易所連線不算。ready 之前死掉記為 `FAILED`，不重啟，所以 `FAILED` 只會是設定錯誤（例如 `apis` 列不存在、venue 不認得）。交易所連不上，或 API key 被拒，worker 照樣 ready，經 F14 的廣播報 `down` / `unavailable`，在進程內依同一條曲線重試。key 被拒時報 `unavailable(auth_rejected)`，不再重試認證。連線維持住一段時間後，進程內的重試次數歸零。這段時間的常數見附錄 D。

STS 維持 F11：窗口超標進入 `FATAL`，不套用上面的無 `FATAL` 曲線。

### 3.3 Detach / reattach

（§4.4）

controller 停止（SIGTERM）時呼叫 `Supervisor.close("detach")`：停止接受新的控制 RPC；不對任何 worker 送信號；flush 尚未寫出的 Status；以 exit 0 結束。

controller 啟動時呼叫 `Supervisor.start()`：先同步載入 `${WORK_DIR}/run/supervisor.json`，再對 `run/` 下每個 socket 呼叫 `status`，比對 `id` 和 `incarnation`，然後和 DB 裡的 desired 對帳。對帳完成才開始服務控制 subject。整個過程中 worker 照常運作（P1）。

| desired | worker | 動作 |
|---|---|---|
| 有 | running | adopt |
| 有 | 不在、已 exited 或 LOST | STS：以 exit 資訊標成 `failed`，不重建。MD／TD：依該 kind 的重啟策略處理 |
| 沒有，或已 terminal | running | stop，接著 release |

只有明確呼叫 `close("stop")`（整台主機下線）才會停掉 worker。

controller 在線時看見的 STS crash 走 §5.2 的 F11，和上面這張 reattach 對帳表是兩條路徑。

### 3.4 部署與升級

（F6、F7、§4.6。§4.5 的主機實測不是本文件的範圍。）

每個平面是一個 OCI assignment，開 `oci_host_pid`。controller 直接 spawn shim。shim 和 worker 留在 spawn 它們的那一版 controller 的 mount namespace 裡，所以 `WorkerSpec.code_ref` 就是該 release。Strategon 的 SIGTERM 和 SIGKILL 只送到 controller 的 process group，碰不到 shim。shim 由 host init（或最近的 subreaper）收養。每個平面在自己的 cgroup 裡，並有記憶體上限。仍被任何 worker 當作 root 的 release 不被刪除（S-2）。

開發與 integration 要驗證 reattach 時，平面以行程執行。容器自己的 PID namespace 會在容器結束時殺掉裡面的 worker，那種跑法只用於不必驗證 reattach 的場景。（§4.5）

日常升級（切換本身依 F2：先停掉所有策略，平面清空後再整批換版）：

| 平面 | controller 滾動 | worker 代碼升級 |
|---|---|---|
| STS | 不影響（reattach） | 已在跑的 session 繼續用舊版，直到它結束；新 session 用新版。策略樹與 extras 也一樣：session 一直用 start 時釘住的 digest 與 generation，包括重新掛起（F39） |
| MD | 不影響 | 已在跑的連線繼續用舊版，不遷移（F22）。新開的連線用新版。既有連線要換版時，由人工對單一連線 `restart`（F24），平台不自動重啟舊版連線。斷線期間策略收到 `on_md_update` 的 down → live，tape 記錄空洞 |
| TD | 不影響 | 換版後由人工逐帳號觸發 drain-replace（F27），平台不自動換版。新單一律以可重試的 `td_draining` 拒絕，等 in-flight ack 收齊後停止，以新 incarnation 啟動並 recon，再恢復收單。期間 session 的 `TdReady` 會短暫變成 false |

`mftik workers --stale` 列出跑在非最新 release 上的 worker，以及 digest 已經不是目前版本的 session。何時重啟由人決定，或等它自然結束。（F24、F39）

跨版本只靠 `pv`（F26、F41、§8.3）。升 `pv` 的順序：先停掉舊 `pv` 的 STS session，讓 `on_stop` 的撤單還送得到同版的 TD；再人工重啟 TD 帳號（F27）和 MD 連線（F24）；最後才開新 session。`on_stop` 沒撤乾淨的單，由 `cancel_session`（F10）補上。這個順序走完之前，用到舊 `pv` worker 的 start 會在 deploy 時被擋下。

停止不依賴協定：Supervisor 以 SIGTERM 停 worker，走 shim，或直接對 host PID 送訊號。任何版本組合都停得掉。（§4.6）

### 3.5 記憶體

（§4.7、F7、F43。實測的 Pss、VmRSS、各 kind 的 MiB 估計不在這裡；見計畫 §4.7 與附錄 D。）

cgroup 的上限是整個平面的總量。平面內部另外有三項：

1. **`oom_score_adj` 分級。** shim fork 出 worker 之後、exec 之前，由子進程寫自己的 `/proc/self/oom_score_adj`。往上調不需要特權。kernel 依 RSS 加上這個值挑人。分數：offload 子進程 +900，STS session +800，MD `conn` 與 `fetch` +300，TD `account` +100，controller 與 shim 不調（0）。先被犧牲的是策略，不是行情或下單。子進程先於 session；session 只收到 `OffloadWorkerLost`，不會跟著死。
2. **可選的 `RLIMIT_DATA`。** `WorkerSpec.rlimit_data_bytes` 有設定時，shim 在 exec 之前套用。超過上限時 Python 拋出 `MemoryError`，不是 SIGKILL。STS 的值來自 strategy.yml 的 `limits.memory_mb`，預設不設。不用 `RLIMIT_AS`。
3. **准入。** 每個平面的 orchestrator 持有 `max_workers` 與依 kind 估算的 `memory_budget_mb`，來自環境變數 `PROCMAN_MAX_WORKERS` 與 `PROCMAN_MEMORY_BUDGET_MB`。兩個都沒設，或是空白，就是沒有預算。有設記憶體上限時，kind 的估計用 §4.7 的表，shim 的固定開銷另外加上。STS session 再預留 P × 子進程估計值，P 是 `limits.offload_processes`；有設 `offload_memory_mb` 就用它（那是子進程的 `RLIMIT_DATA`），否則用 spawn 子進程的基準 Pss。子進程不算進 `max_workers`（F43）。超過預算時，start 以 `capacity_exceeded` 拒絕，不會先把 worker 開起來。

`procman.report` 的 `rss_bytes` 是該 worker 行程樹的 Pss，不含 shim，也不是 `/proc/<pid>/status` 的 `VmRSS`。cgroup 可用時，slot 另有 `memory.current` 與 `oom_kill`；shim 看到 worker 被 SIGKILL 時，Supervisor 比對這個計數，判斷是不是 OOM。

MD 連線 worker 是一條 websocket 一個進程（F17），worker 數等於使用中的連線數。不做連線整併，碎片連線各佔一個進程（§6.2）。TD 帳號 worker 對每個啟用帳號常駐（F35），TD 平面的固定佔用和有沒有 session 無關。每個 worker 另有一個 shim，算進該平面的預算（F29）。

## 4. STS

### 4.1 Controller

（§5.1、F39、F40）

desired 是 `SessionSpec`（DB 列）。placement 就是本 instance。每個 session 一輪 reconcile：比較 desired phase 和 worker status，決定 create、stop、標記 terminal。

controller 服務 `sts.{instance}`：start、end、list，以及 operator 對主機磁碟的路徑（registry、env、artifacts、event log 讀取）。controller 不 import 策略代碼。session 層級的 stop、fail、status 由 worker 在 `sts.ctl.{session_id}` 服務。

執行期間的訂閱變更（`self.md.subscribe`，以及 selector 事件）由 worker 直接找 MD，不經過 API。

### 4.2 Session 生命週期

（§5.2）

`pending → starting → running → stopping → done | failed`，另外有 `restarting`（F10）。沒有 `interrupted`。策略不碰 Postgres。session 列由 controller 的 Supervisor 寫，依據是 worker 回報和 shim 的 exit 紀錄。

**`on_start` 與 `on_ready`（F12、F3）**

| 階段 | 平台 | 策略 |
|---|---|---|
| `on_start` | MD 的 feed 已經訂閱。ingress 在收資料但不交付。TD 還沒訂閱 | 載入模型、讀 artifacts、`tape.read`。可以很長，也可以同步。不能下單，SDK 拋出 `NotReady` |
| 等待就緒 | `on_start` 結束後才訂閱 TD 並送出 recon，然後等就緒條件 | — |
| `on_ready(ready)` | 只呼叫一次。之後才開始交付：`latest` 類只給最新一筆，`all` 類依序交付 | `self.oms`、`self.ledger` 已經是 recon 之後的狀態，可以下單 |

帳本的權威是 TD 帳號 worker 的記憶體（F13）。策略用 `await self.oms.view()` / `self.ledger.view()` 取最新狀態；要等狀態為 UNKNOWN 的單收斂時用 `view(settled=True)`。沒有 `send_recon`、`STS_RECON`、`on_recon_done`。平台內部的 recon 只用在 TdReady，以及下面的 `on_resync`。

**`on_resync(api_id, cause, view)`（F13、§5.2）** 只在 `on_ready` 之後、事件流可能有缺口時由平台觸發。§5.2 列出的觸發點：

- `cause="reconnect"`：ingress 的 NATS 斷線後重連。
- `cause="account_reset"`：TD 帳號 worker 換了 incarnation（§7.1）。`view` 是收斂後的帳本。

TD 自己因交易所重連而跑的 `reconcile()` 不觸發 `on_resync`；那些變化已經透過 order update 和 OMS view 推送。§5.6 的「整台主機失聯」恢復列另外寫帳號會收到 `on_resync`。那一列和上面這兩個觸發點衝突，本文不把「靜默恢復本身再送一次 `on_resync`」寫成定案。

**就緒只針對有宣告的部分。** strategy.yml 的 `md:`、`td:` 都是選填。沒宣告的那一項視為成立。

| 宣告 | 要等什麼 | `on_ready` |
|---|---|---|
| 都沒有 | 不等 | `on_start` 結束後立刻 |
| 只有 md | 每個 feed 就緒 | 全部就緒，或 `ready_timeout_s` 到期（附缺少的 feed） |
| 只有 td | 每個帳號 recon 完成 | 全部完成；逾時則 failed |
| 兩者都有 | 兩邊都等 | 帳號必須全部完成；feed 全部就緒或逾時都可以 |

TdReady 是硬條件：`ready_timeout_s` 內有帳號沒完成 recon，session 記為 failed。這是初始化失敗，依 F11 不重啟。MdReady 是軟條件：逾時仍然呼叫 `on_ready`，在 `ready.missing_feeds` 列出缺少的 feed。判斷依 §6.1 的 `join_policy`：訂閱時會推 snapshot 的（quote、ticker、book、greeks 等）要收到第一筆；不推 snapshot 的（trade、aggtrade、liquidation）在 MD 確認訂閱成功時就算就緒。selector 回報的是覆蓋率。feed 訂閱不到（symbol 不存在、交易所不支援該 topic）在登記 MD intent 時就被拒絕，deploy 當下失敗。執行期間動態加入的 feed 不影響 `on_ready`。不加逐 feed 的 `required`（F33）。

**啟動是非同步的（F12）。** `POST /sts/deploy/{type}` 在驗證、寫入 SessionSpec、登記 intent、啟動 worker 之後回 202：`{session_id, status: "starting"}`。進度寫在 session row 的 status 和 conditions，同時發佈到 `sts.status.{session_id}`。啟動失敗由 Supervisor 寫入原因：`on_start` 拋例外、`start_timeout_s` 逾時、TD 沒有就緒、worker 在啟動期間 crash。API 不做另一套回滾；啟動失敗走 End（§8.1、F46）。`mftik run` 預設 `--wait`，追蹤到 `running` 或 `failed` 再 tail log；`--no-wait` 只回 session id。

| 設定 | 計算範圍 | 預設 / 上限 | 超過時 |
|---|---|---|---|
| `start_timeout_s` | 只算 `on_start` | 60 / 3600 秒 | kill，failed，不重啟 |
| `ready_timeout_s` | 從 `on_start` 結束算起 | 30 秒 | TD 沒就緒 → failed；只有 MD 沒到齊 → 照樣 `on_ready`，附缺少的 feed |

**crash 之後（F10、F11）** 先清場，再決定 fail 還是重新掛起。重新掛起從 `on_start` 全新開始，不帶任何舊狀態。

| 類型 | `on_stop` | 平台清場 |
|---|---|---|
| A：策略代碼拋出例外，進程還活著 | 保證呼叫。ingress 把 `on_stop` 排進策略 loop，受 `on_stop` 的牆鐘上限限制，之後進程以 crashed 結束 | 仍然執行 |
| B：一般 hook 阻塞策略 loop 超過 30 秒（F15），或 stop 時策略 loop 卡住超過 grace | 無法呼叫。shim kill | 執行 |
| C：進程死亡（OOM、segfault、SIGKILL） | 無法呼叫 | 執行 |

平台清場：Supervisor 呼叫 `td.order.cancel_session(session_id)`（§7.1），撤掉所有 `client_order_id` 裡 session 欄位等於這個 session 的掛單，並等到全部確認。B 和 C 只能靠這一步代替 `on_stop`。部位無法用撤單處理，原樣保留。

接著：

1. Supervisor 把 `sts_sessions` 寫成 `restarting`，記下 reason、exit 資訊、重啟次數，並在 `log.sts.{session_id}` 發一條 `error` log。
2. `restart: never`（預設）→ `failed`。只有 A 類有資格重新掛起。B、C、`on_ready` 之前的初始化失敗、窗口內超過 `max_restarts`、清場沒有全部確認，都是 `failed` 並發 alert。其餘（`restart: on_failure` 的 A 類）重新掛起。
3. 指數 backoff，最短 1 秒，然後以同一個 `session_id`、incarnation + 1 啟動新的 worker，從第 0 階段再走一遍（§5.3）。

不變式：

- **R1** 舊 incarnation 確認死亡（有 shim 的 exit 紀錄）且清場完成之後，新 incarnation 才能啟動。兩者不會並存。
- **R2** backoff 至少 1 秒。`client_order_id` 是 `session(24) | ts_sec(28) | seq(8)`，seq 每個 incarnation 從 0 開始。舊的最後一張單和新的第一張單落在不同秒。
- **R3** 新 incarnation 的 recon 不會看到舊的掛單（已經清場），但會看到既有部位。`restart: on_failure` 的策略必須能接受開始時已經有部位。
- **R4** 重新掛起期間，MD／TD 的 intent 不回收。存活報告列的是 desired 為 running 的 session，包含 `restarting`，不只是當下活著的 worker。

### 4.3 Hook 與交付

（§5.3、F8）

session worker 由兩條 thread 組成。MD／TD worker 與 STS controller 維持一個 loop。

| | ingress thread | strategy thread |
|---|---|---|
| 位置 | main thread，自己的 uvloop | 第二條 thread，策略的 uvloop |
| 連線 | 接收：MD atom、TD 帳號事件、`sts.ctl.{session_id}`、回覆 inbox | 送出：下單、撤單、`td.account`、`md.fetch`、log |
| 負責 | 持續讀 socket；event log（收到就記）；依交付策略排隊或 conflate；RPC timeout 以真實時間計算；控制訊號；對 shim 的 heartbeat 和 progress | 策略 hook、timer；從 ingress 取事件並在這裡解碼；直接 publish 送單 |
| 不做 | 不解碼行情、不跑使用者代碼、不做阻塞 I/O | 不讀接收連線 |

下單不跨 thread。策略 thread 在自己的送出連線上直接 publish，`reply` 指向 ingress 的 inbox，並且強制 flush。送單之前先在共用的 pending 表登記 future。ack 由 ingress 收下，記進 event log，timeout 用 ingress 的時鐘判斷，再以 `call_soon_threadsafe` 交回策略的 future。no-responders 的 503 留在送出連線上，不路由到 `reply`。

階段：0 ingress 最先啟動並開始 heartbeat；1 載入策略、訂閱 MD；2 `on_start` 期間收但不交付；3 `on_start` 結束後才訂 TD 並 recon，就緒後 `on_ready`；4 running；5 stopping 時 ingress 繼續收 ack 和 fill，直到 `on_stop` 結束；6 flush event log、NATS drain、進程退出。TD 訂閱延後是因為帳號事件是整個帳號的廣播，不能在很長的 `on_start` 裡堆積，也不能丟；延後再立刻 recon，拿到的是當下的快照。

ingress 意外結束：整個進程 fail-fast，非 0 退出，不在進程內重啟。策略 thread 拋例外：ingress 走收尾，status 為 failed。策略卡住超過 stop grace：由 shim／Supervisor 送 SIGKILL。shim 消失（EPIPE 或 PDEATHSIG）當作 stop。NATS 斷線重連：thread 不變；每個 feed 收到 `on_md_update(feed, "down", "ingress_reconnect")`，重連後收到 `live`（F23）；每個帳號做平台 recon，收斂後 `on_resync(cause="reconnect")`（F13）。

- **I1** ingress 先於策略啟動、晚於策略結束。
- **I2** ingress 的生命週期等於進程，等於 session。不跨 session，不在進程內重啟。
- **I3** ingress 在 main thread。SDK 禁止策略自己註冊 signal handler。
- **I4** ingress 不跑使用者代碼，也不做阻塞 I/O。寫檔交給 event log 的 writer。

**交付（每個 feed 可在 strategy.yml 覆寫）：**

| 類型 | 預設 |
|---|---|
| ticker、bestquote、greeks、funding、OI、orderbook | `latest`：只留最新一筆，在解碼前 conflate |
| kline | 以 `(feed, bar 開盤時間)` 為 key 保留最新，不丟掉任何一根收盤 bar。開盤時間從已解析的 dict 讀（v0.35） |
| trade、aggtrade、liquidation | `all`：有界佇列，溢出時丟最舊的，寫 warning，累加丟棄計數。策略以 `event.seq` 自己偵測跳號（F25） |
| TD 事件、`feed_end`、RPC 回覆 | must-deliver：共用一條 FIFO，佇列長度與行情分開（常數見附錄 D）。溢出視為異常並 fail session。`bind_delivery` 綁上 `seq` 與 `age`（v0.35） |

每個事件帶 `recv_ts`，策略用 `event.age` 看延遲。MD 事件另帶 per-atom `seq`（F25）：以 (atom, 連線 epoch) 起算，同一條連線上連續；重連、單一 atom 的 resync、換 incarnation 之後從 1 起算。策略只看 `seq != last + 1`。`seq` 不是排序鍵，也不是去重鍵，`(atom_id, seq)` 不保證唯一。`all` 類的不連續是漏收；`latest` 類的跳號是覆蓋。不提供 `on_feed_gap`（F23）。

event log 在入站的當下就記，帶 seq 和 `recv_ts`，並標記 `delivered`、`superseded` 或 `dropped`。出站由策略 thread 以 `put_nowait` 交給 writer thread。佇列滿了就丟棄、計數、seq 留洞。沒設 `STS_EVENTLOG_DIR` 時只關掉寫檔，ingress 照常運作。

GIL 的 switch interval 維持解譯器預設。長時間持有 GIL 的運算交給 `offload` 的 process 模式（§5.5）。

**hook 時間預算（F15）。** 一般 hook 量阻塞時間（佔住策略 loop、沒有 `await` 讓出的連續時間）。`await self.offload(...)` 不算阻塞。生命週期 hook 量牆鐘時間。不提供讓策略自訂上限的設定。

| hook | 量什麼 | 上限 | 超過時 |
|---|---|---|---|
| 一般 hook | 阻塞時間 | 1 秒 | warning、`HookSlow` 加一，不 kill |
| 一般 hook | 阻塞時間 | 30 秒 | B 類 crash |
| `on_start` | 牆鐘 | `start_timeout_s`（F12） | 初始化失敗，不重啟 |
| `on_ready` | 牆鐘 | 10 秒 | 初始化失敗，不重啟 |
| `on_stop` | 牆鐘 | 10 秒 | 不再等，走平台清場，然後 kill |

阻塞時間由策略 loop 自己量。ingress 把它放進 status progress（卡在哪個 hook、多久了）。

### 4.4 offload

（§5.5、F9、F43）

`offload` 把重計算移出策略的 loop。它取代 `breathe` / `slice_deadline`。

- thread 模式（預設）：會釋放 GIL 的運算，共用記憶體，不 pickle。stop 時取消的只是等待的 coroutine，thread 會跑完。
- process 模式（`isolate=True` 或 `offload_pool`）：`spawn` 啟動。函式、參數、結果必須可 pickle，函式必須是模組層級。stop 時 terminate。子進程的 `oom_score_adj` 是 +900。死掉時呼叫端收到 `OffloadWorkerLost`；pool 在下次呼叫時重建，`offload_pool` 會重新執行 init。

`limits.offload_threads` 預設 2。`limits.offload_processes` 是這個 session 同時存在的 offload 子進程總上限，預設 0：沒有宣告就不能用 process 模式。`offload_pool(workers=N)` 建立時預留 N 個。`isolate=True` 背後的 pool 在第一次使用時建立，拿剩下的額度，至少 1 個。不夠就在呼叫處拋出 `OffloadQuotaExceeded`，不縮小。pool 要在 `on_start` 裡、第一次 `isolate=True` 之前建立。`StrategyHarness` 套用同樣的限制。

offload 出去的函式不能呼叫 SDK。SDK 檢查呼叫者所在的 thread。例外傳回 `await` 的呼叫端。pool 在第一次呼叫時建立，`on_start` 裡可以用。stopping 時進行中的 offload 不自動取消。收尾時 process 模式 `shutdown(cancel_futures=True)` 再 terminate；thread 模式等它跑完，超過 stop grace 由 shim SIGKILL。子進程啟動時設 `PDEATHSIG`，並確認 parent pid 沒變。

ingress 的 progress 列出進行中的 offload（函式、模式、已執行時間）。event log 記 `offload_start` / `offload_end`（函式、模式、耗時、ok / error / cancelled / lost）；參數只記大小。

### 4.5 MD／TD 失聯

（§5.6、F14、F19、F23）

`on_ready` 之後，MD 或 TD 中途斷掉只通知，不自動 fail。平台不提供「失聯多久就 fail」的設定。策略自己決定等、降級，或 `self.fail()`。資料新不新鮮由策略看 `event.age`。TD 不可用時，下單在本地直接拒絕，不送出。

MD 連線 worker 與 TD 帳號 worker 各自單向廣播，STS 不回 ack，也沒有 per-session 的狀態。廣播由 worker 自己發，不是 controller，所以 controller 滾動期間不會中斷（P1）。

| 廣播者 | subject | 內容 | 頻率 |
|---|---|---|---|
| MD 連線 worker | `md.w.{instance}.{worker_id}` | incarnation、連線狀態、狀態版本號（`md.worker.state`）。atom 的狀態變化另以 `md.atom.state` 發出 | 狀態變化時立即一次，平時每 2 秒一次 |
| TD 帳號 worker | `td.account.state.{api_id}` | incarnation、`ready` / `degraded` / `unavailable`、狀態版本號 | 同上 |

ingress 依版本號判斷有沒有漏掉狀態事件；有漏的話，向該 worker 查詢完整狀態。某個 worker 的廣播靜默超過 10 秒（漏了 5 次），把它負責的 feed 標成 `down`、帳號標成 `unavailable`。這是 P7 唯一的例外，只產生通知，不回收資源。

| 情況 | 策略收到 | 恢復後 |
|---|---|---|
| MD 連線 worker crash 或重連（含交易所斷線） | 受影響的每個 feed：`on_md_update(feed, "down", reason)` | 重新訂閱成功後 `on_md_update(feed, "live", …)`。中間缺了什麼由策略自己記錄（F23） |
| feed 永久結束（到期、下市） | `on_feed_end` | 不會恢復 |
| TD 帳號 worker 重啟或換版 | `on_td_update(api_id, "unavailable", reason)` | 先 `on_resync(api_id, "account_reset", view)`，再 `on_td_update(api_id, "ready", …)` |
| TD 私有連線斷線 | `on_td_update(api_id, "degraded", reason)` | `ready`。TD 自己的 reconcile 仍以 order update 送達 |
| MD／TD controller 重啟 | 什麼都不會收到（P1） | — |
| STS ingress 斷線重連 | 所有 feed：`on_md_update(feed, "down", "ingress_reconnect")` | 所有 feed `live`；每個帳號 `on_resync(cause="reconnect")` |
| 整台主機失聯 | 靜默 10 秒後 `down` / `unavailable` | 廣播恢復後回到 `live` / `ready` |

```python
async def on_md_update(self, feed: str, state: str, reason: str) -> None: ...  # "live" | "down"
async def on_td_update(self, api_id: int, state: str, reason: str) -> None: ...  # "ready" | "degraded" | "unavailable"
self.md.state(feed)
self.td.state(api_id)
```

一個 feed 由多個 atom 組成時，任何一個 atom `down`，這個 feed 就是 `down`；全部回到 live 才是 `live`（F19）。這兩個 hook 只處理連線與可用性，不是行情或訂單。帳號 `unavailable` 時，`submit_order` / `cancel_order` 立刻回傳 False，原因是 `td_unavailable`，不送出。`degraded` 時照常送出。`MdReady`、`TdReady` 在 `on_ready` 之後繼續反映即時狀態。每次狀態轉換寫一條 warning log。

### 4.6 代碼身分與主機磁碟

（§5.7、F39、F40）

「這個 worker 跑哪一份代碼」是三個軸：

| 軸 | 識別 | 目錄的權威 | 主機上的位置 | 何時變 |
|---|---|---|---|---|
| 平台 release | `WorkerSpec.code_ref` | Strategon release（F6） | release 的 rootfs | controller 換版 |
| 策略樹 | `strategy_digest` | API 的 registry store | STS volume 的 registry 副本 | push、delete、pull |
| extras | `env_generation` | API 的 `env/applied.json` | STS volume 的 `env/gen-{N}` | env apply |

內建策略（`mftik_sts.impl`）沒有 digest，代碼就是 release。procman 只認 `code_ref`（P6）。STS orchestrator 把後兩者寫進 `WorkerSpec.labels`。

desired：API 在 start 時從自己的 registry 與 env 解析 `(strategy_digest, env_generation)`，寫進 SessionSpec。這一組在 session 生命週期內不變，F11 的重新掛起也沿用，不讀磁碟上現在的版本。重新掛起時的平台 release 是當下 controller 的版本，spawn 前先檢查策略樹宣告的 `requires_mftik`；不相容就 failed 並發 alert。observed：Supervisor 在 spawn 時記下三個軸，經 `procman.report` 回報。

STS 磁碟副本以 digest 定址。樹在 `registry/trees/<digest>/`，name → digest 的索引另存。push 新版本只改索引，不覆蓋正在被使用的樹。GC 只刪沒有被本 instance 任何非 terminal SessionSpec 釘住、也不是索引裡目前版本的 digest。extras 的世代修剪用同一條規則。API 的 registry store 只保留每個 name 的目前版本。STS 磁碟遺失時，被釘住的舊 digest 無法從 API 補回，重新掛起以 `strategy_unavailable` failed。

使用者代碼只在 session worker 裡執行。worker 在第 1 階段以 `load_class(trees/<digest>, digest=…)` 載入；失敗是初始化失敗（F12）。deploy 時的可部署檢查只看 digest 和 generation 在不在這台磁碟上、`requires` 和 extras 是否相符，不 import。push 之後「這棵樹能不能 import」的回報，由 controller 起一個一次性的探測子進程；它不是受管 worker，跑完即結束。`sts.registry.sync`、`sts.env.sync`、開機的 `api.registry.catchup` 由 controller 服務，只寫磁碟副本，不碰執行中的 worker。`sts.registry.reload` 是重新掃描索引，不是重新 import。

controller 同時服務 `sts.artifact.*`（list、read、begin / chunk / commit / abort、delete）、event log 讀取，以及清理沒有 commit 的上傳。不另開 files worker。上傳 token 由磁碟上的 `.{name}.{token}.part` 找回，controller 重啟後可以接著傳。artifact 的權威是主機上的 volume。operator 動不到 `sessions/`。策略可以寫任何 key，包括覆蓋 operator 上傳的 key。event log 的寫入者是 session worker 的 ingress，controller 只讀。

## 5. MD

### 5.1 Atom

（§6.1、F17、F19、F21）

`Atom = (venue, endpoint, channel)`。channel 逐字等於交易所的 subscribe 參數。`atom_id` 是正規化字串；subject 用它的穩定 hash，因為 channel 含有 `.`：`md.a.{venue}.{hash}`。MD 維護 hash 和 atom 的對照。

每個 venue adapter（`mftik.exchange.<venue>`）提供純函數：

- `atoms_for(topic, ticker, opts) -> AtomPlan`：平台 topic 對應哪些 atom、用哪個 projector。一個 feed 對 atom 可以是一對多。
- `decode(atom, frame) -> list[Event]`：一個 frame 可以產出多個平台 model。
- `capacity(endpoint)`：每條連線的 atom 上限、訊息速率、subscribe 的批次與速率。
- `join_policy(atom)`：late joiner 的語意。

STS 的宣告仍然是平台 topic（例如 `bestquote.…`）。MD 在 intent 登記時解析出 atom，回傳 `{feed: [atom_id]}`。session 訂閱對應的 `md.a.*`，再依 envelope type 路由到 hook，只送出它宣告過的類型。

跨連線的組合不在 MD 做，也不另設組合 worker。MD 只發佈原子事件；STS ingress 以平台通用的純函數 join。join 不含 venue 代碼。組成的 atom 任一 `down`，這個 feed 就是 `down`（F19）。

MD 是行情的權威，地位對應 TD 之於 ledger。解碼、book 的 fold、late joiner 的快照都在連線 worker。`md.a.*` 上是平台 model。STS 不接觸交易所原文，也不持有 fold 狀態。atom 層講 venue 詞彙；STS 只看得到不透明的 `atom_id`。

### 5.2 Controller

（§6.2、F18、F22、F44）

demand 有三個來源：session intent、常駐訂閱（不屬於任何 session；專為錄 tape 的 `tape_keeper` 因此可以退役）、selector（§6.4）。session intent 和常駐訂閱都可以帶 selector。`desired_atoms` 是 demand 的聯集，每個 atom 記錄 owner 集合。owner 進入 terminal 時由 GC 移除。

placement 依 `(venue, endpoint)` 的 capacity 把 atom 分配到連線上，而且有黏性：新 atom 優先放進已有的連線，容量不夠才開新的連線 worker。放上去之後不搬（F22）。連線上沒有任何 atom 時，該 worker 結束。不做整併。placement 只看 `max_atoms`。`max_messages_per_second` 是實測的滿載訊號：連線 worker 在 `md.w.*` 回報 msg/s，超過上限的 80% 就不再放新 atom；已在上面的不搬。

每條連線的 desired 連同 `generation` 推給連線 worker。每次推完整清單，不推增量。`generation = (controller_epoch, seq)`。worker 只接受比手上更大的 generation。`controller_epoch` 存在 `md_controller(instance PK, controller_epoch)`，controller 每次啟動以一句 UPSERT 遞增。新舊 controller 短暫重疊時，舊的推送不會蓋掉新的。

到期：依 SYM listing 判斷商品已到期時，從 desired 移除對應 atom，並對 owner 發出 `md.feed.end(expired)`。

controller 重啟時，從 DB 裡的 intent 和常駐設定重算 desired。重算完成、推出新 generation 之前，worker 保持上一份（P5）。

某個 `(venue, endpoint)` 上還有 `pv` 不同的連線 worker 時，落在那裡的 `md.intent.put` 以 `protocol_mismatch` 拒絕，不另開新 worker。訊息列出要 `restart` 的 worker（F24、F41）。另開會讓同一個 atom 有兩個發佈者和兩個 tape writer（違反 F22）。

### 5.3 連線 worker

（§6.3、F17、F18、F20、F25、F42）

一個 worker 一條 websocket。reconciler 跑在 worker 裡。observed 以交易所 ack 為準，並以連線 epoch 為鍵；舊 epoch 晚到的 ack 丟棄。差異合併成批次，經 token bucket 限速後送出。重連後 observed 歸零，下一輪 diff 補齊。每個 atom 回報 `pending`、`subscribed`、`first_msg_at`、`last_msg_at`、`error`。

socket 斷線後在進程內重連，等待和 crash 重啟同一條曲線（F42，常數見附錄 D）。交易所整體斷線時，jitter 用來錯開同一台主機上的連線。

每個 frame 只解碼一次，發佈到 atom subject。envelope 帶 per-atom `seq`，以 (atom, 連線 epoch) 起算。`owner` 不上線：envelope 的 `source` 是 worker id，incarnation 在 `md.w.*`。

每個 atom 任何時刻只在一條連線上，只有一個發佈者，訂閱端不去重。原地重啟時，Supervisor 確認舊進程結束後才啟動新的（delete-before-create，P4）。

tape 以 `atom_id` 為 key，只錄 `trade`、`aggtrade`、`liquidation`（F20）。book 和報價不錄。有 demand（含常駐訂閱）的 atom 才錄。coverage 以 atom 為單位。重連或原地重啟的空洞記在 coverage。讀取仍由 MD 服務，STS 不開 Redis。Binance UM 的 `trade` 和 `aggtrade` 都來自同一個 channel 時，改成 atom 之後只錄一份。

狀態廣播的 subject 與靜默規則見 §5.6。crash 與 ready 見 §4.3（F42）。

### 5.4 Selector

（§6.4、F33、F44）

`md:` 除了靜態 feed，還有 `select:`。session 的 strategy.yml 和常駐訂閱都可以用。兩種：`option_chain`、`rolling_future`。

每種 selector 是純函數 `evaluate(listing, ref, now, prev) -> Selection | Hold`。listing 來自 SYM，ref 是 MD 自己的行情。規格相同（spec hash 相同）的 selector 只算一份，所有 owner 拿到同一個 universe 和 epoch。

`option_chain`：依 `expiries` 選 expiry，離到期不到 `min_tte` 的跳過。每個 expiry 以 ref 找出最近的掛牌 strike 當中心，取上下 `atm` 檔，各 strike 的 C／P 依 `sides`。ref 落在兩檔正中間時，取靠近目前中心的一檔；沒有中心時取較低的一檔。debounce 期間新出現的 expiry，跟著存下的中心點，取它自己的 strike 序列上離中心價最近的一檔。距離一律量在這一輪套用 `min_tte` 之後的最近 expiry 的 strike 序列上；存下的中心點不在這個序列上時，先對到最近的一檔。ref 離目前中心超過 `recenter.strikes` 檔才重新置中，兩次置中至少間隔 `min_dwell`。

`rolling_future`：依到期日把合約分成 weekly、monthly、quarterly，取對應 tenor 最近的一張當 current。分類交給 venue adapter 的 `tenor_of`。週五日曆只套用在 adapter 宣告適用的 venue。沒有分類能力的 venue，部署 `rolling_future` 時在 deploy 失敗。到期前 `roll_before` 時 current 切到下一張；舊合約保留到到期才移除。

listing 過期、ref 斷線、MD controller 不在時，維持上一份結果（P5）。listing 回來 0 個 instrument，一律當成讀取失敗，回 `Hold(listing_empty)`，維持上一份。每個 expiry 都落在 `min_tte` 內則讓 universe 變空，那是另一件事。

`prev`（上一次的 Selection、epoch、置中狀態）存在 DB。controller 重啟後從這裡接著算，不重新置中。置中時間 `Center.at` 是 selection 狀態裡的明確欄位，只在重新置中時改寫，不以列的 `updated_at` 代用。

部署時就能算出 atom 上限，用於准入（§4.7）。Deribit 的 ticker 和 greeks 共用 `ticker.*` channel，所以不會把兩個 topic 算成兩倍 atom。

策略只有 `on_universe_change(name, change)`。`change` 帶 `added`、`removed`、`epoch`；`rolling_future` 另有 `current`。不另設 `on_roll`。`self.md.universe(name)` 是目前選中的合約，`self.md.current(name)` 是 `rolling_future` 的 current。

**I-SEL1：** 某個合約出現在 `added` 之前，策略不會收到它的事件；出現在 `removed` 之後，也不會再收到。ingress 收到 `md.universe.{session_id}` 之後，先訂閱新合約的 atom subject、交付 hook，再退訂被移除的合約，並丟掉佇列裡屬於它們的事件。成員到期時先 `on_feed_end(expired)`，接著是含該合約的 `removed`。

不做 pin：期貨舊合約本來就留到到期；期權在 TD 能下單之前，MD 不向 TD 查部位。不做 `required`：selector 在 `ready_timeout_s` 內推導不出結果，或成員一直沒資料，都列在 `ready.missing_feeds`（F12）。

## 6. TD

### 6.1 帳號 worker

（§7.1、F34、F35、F36、F37、F45）

帳號綁定的 TD instance（`apis.instance_id`）不可經 API 修改。`PATCH /apis` 只改名。欄位 NOT NULL，沒有未綁定狀態。要搬帳號，就刪除後在新 instance 重建（新的 `api_id`）。日後若加改綁的 route：帳號還有未釋放的 intent，或舊 instance 的報告還列著它的 worker 時，一律拒絕；主機永久消失時才允許 `--force`（F45）。等舊 worker 從報告消失再 spawn，會依賴報告缺席，和 F32 衝突。

一個進程對應一個 `api_id`，持有該帳號所有私有連線。一個帳號有兩條 websocket 時不拆進程，否則 OMS 和 ledger 要跨進程同步（F34）。

兩層生命週期（F35）：

- **常駐層：** 本 instance 名下每個啟用帳號都有一個常駐 worker，和有沒有 session 無關。啟動就對交易所建立 HTTP 連線並保持溫熱。keepalive 長到不會在閒置幾秒後關掉，並由 adapter 定義一個輕量請求定期送出。recon、槓桿查詢、backfill、走 HTTP 的下單都共用這個連線池。
- **交易層：** 有未釋放的 TdIntent 時才啟動：連私有 websocket、recon、OMS／ledger 上線、訂閱 `td.order.{api_id}`，之後 TdReady 才成立。最後一個 intent 消失就立刻關掉，不 linger。常駐層不受影響。F11 的 `restarting` 期間 intent 不回收（R4），策略重啟不會讓交易層抖動。

下單依 venue 走 HTTP 連線池或 websocket。controller 以 level-triggered 的方式把「這個帳號目前有沒有 intent」推給帳號 worker，訊息是 `td.account.{api_id}` 上的 `td.account.trading`。controller 不在時，worker 維持最後一份（P5）。

backfill 是排程或 detach 觸發的一次性 request，由帳號 worker 用常駐連線池處理，不另開 job worker。沒有 session 也能做。同一個帳號同一時間最多一個 backfill。

直接服務 `td.order.{api_id}`（下單、撤單、`td.order.cancel_session`）、`td.account.{api_id}`（`oms.view` / `ledger.view`，含 `settled=True`）、並發佈 `td.{api_id}.global`、`td.oms.*`、`td.ledger.*`。下單路徑不經過 controller。`settled=True` 在有狀態 UNKNOWN 的單時，等它們收斂或逾時才回覆。

`td.order.cancel_session(session_id)`（F10）：撤掉 OMS 裡所有 `client_order_id` 的 session 欄位等於該 session 的掛單。還在 `PENDING_NEW` 或 `UNKNOWN` 的單，等 `chase_unknown` 收斂後一併處理。全部確認後才回覆成功，逾時則回覆未確認的清單。這是 STS crash 的平台清場，也可以當人工的 kill switch。

**at-most-one（F36）不用 DB lease。** 同一個 instance 內，Supervisor 確認舊 worker 的 PID 已經消失，才啟動新的 incarnation。`oci_host_pid` 之下 controller 看得到 host 的 `/proc`。reattach 時也先掃 `/proc`，確認沒有同 id 的 worker 才 spawn。跨 instance：`api_id` → instance 是 `apis` 裡的靜態綁定，TD controller 只啟動自己名下的帳號。同名 instance 重複啟動，由 `refuse_if_serving` 拒絕。兩個 site 共用同一個資料庫時，不用 DB lease，避免交易依賴跨 site 的 DB 連線。

crash 之後：新 incarnation 用 `reconcile()` 從 venue 重建 ledger 和 OMS，在途的單由 `chase_unknown` 收斂，然後發出 `td.account.reset(incarnation)`。各 session 的 ingress 做平台 recon，收斂後 `on_resync(cause="account_reset")`（F13）。這段期間 `TdReady` 短暫變成 false。重啟策略見 §4.3（F42）。

**cancel-on-disconnect（F37）** 預設關閉，逐帳號開啟，設定掛在帳號上，不在 strategy.yml。語意是 TD worker 的死人開關，不是 socket 斷線就撤。只用倒數計時型機制。交易層啟用且有掛單時，帳號 worker 定期刷新倒數；進程死掉或卡住、刷新停止，交易所才撤單。一般重連不觸發。交易層關閉時送 `timeout=0` 解除倒數，不是單純停止刷新。關閉交易層不撤單。交易層重新打開、有掛單時再設倒數。不用 Deribit 的 COD，也不用 Bybit 的 DCP。計畫內的換版（F27）在 drain-replace 之前先延長倒數，新 incarnation 接手後再恢復。被交易所撤掉的單，以 `on_order_update(cancelled)` 送給策略；帳號重啟另有 `on_resync`。Gate 現貨（`Gate`）與合約（`GateFutures`）是兩個 venue，各有自己的 `api_id` 和 worker，各自刷新整個市場的倒數。

狀態廣播的 subject 與靜默規則見 §5.6。

### 6.2 Controller

（§7.2、F27、F35）

`desired_accounts` 是本 instance 名下所有啟用的帳號，不再由 intent 決定。intent 只決定交易層。crash 時以 F42 的退避重啟，不進入 `FATAL`。帳號是基礎設施，不是一次性的執行。換版由人工逐帳號觸發 drain-replace（§4.6）。

## 7. API

（§8.1、§8.2、F12、F32、F38、F46）

API 驗證 spec、寫下 SessionSpec 與 intent、叫 STS start 或 end。中間的變化由各平面的 orchestrator 收斂（G5）。

**Start**

1. 驗證：解析 yml；帳號名稱解析成 `api_id`；解析 MD instance；以 dry-run 把 feed 解析成 atom，並檢查容量上限。deploy 時把 API 自己的 `pv` 和這次會用到的 STS controller、MD／TD controller、TD 帳號 worker 比對，不符就以 `protocol_mismatch` 拒絕，不 spawn、不寫 intent（F41）。MD 連線的 `pv` 由 MD controller 在 `md.intent.put` 時擋（F41、§4.6）。
2. 寫入 `SessionSpec`，`status=pending`、`generation=1`，並釘住 `(strategy_digest, env_generation)`（F39）。
3. `td.intent.put(session_id, api_ids)`，冪等。
4. `md.intent.put(session_id, feeds)`，冪等。
5. `sts.session.start(session_id)`。API 回 202 `{session_id, status: "starting"}`。之後的進度看 status 和 conditions。

**End（F46）**

1. `sts.session.end(session_id, reason)`：STS controller 接受就回覆。API 回 202，不等 `on_stop`。`mftik stop` 輪詢 status 直到 terminal。
2. worker 執行 `on_stop`、平台清場後退出，狀態進入 terminal。
3. session 進入 terminal 時（stop、`exit`、`fail` 都一樣），STS controller 寫該 session 的 MD／TD intent 的 `released_at`（F38），再盡力送 `md.intent.delete`、`td.intent.delete`，冪等。
4. 兜底是下面的報告回收。

啟動失敗的回滾走同一條 End。沒有 per-session lease，也沒有 session 級續約（§8.2）。

**intent 回收（MD／TD orchestrator 共用）**

1. intent 帶 `owner = (sts_instance, session_id)`。同一個 owner 的再次 put 取代該 owner 的 desired，不對同一個 owner 做 refcount。交易層的開關看的是還有沒有未釋放的 owner（F35）。
2. 每個 STS controller 的 Supervisor 定期發布 `procman.report.sts.{instance}`。內容是 desired 為 running 的 session（包含 `restarting`，R4），加上這份報告自己的 `generation`，以及每個 worker 的 `code_ref`、`pv`、incarnation、phase、ready、`rss_bytes`，STS 另帶 `strategy_digest` 與 `env_generation`。Supervisor 放入 `starting`、`running`、`stopping`；orchestrator 補上沒有進程的 `restarting`。報告不落地。controller 滾動的幾秒鐘內報告會暫停，這段期間什麼都不回收（P5）。
3. 某個 owner 在同一個 publisher 的連續兩份報告中都不存在 → 回收它的 intent。這是權威觀測。報告中斷（同一個 publisher 暫停，或網路分區）不清空已累積的缺席次數：中斷前後的兩份報告都是缺席觀測（F44）。換 publisher（`generation` 從頭算）時重新計數（F44）。
4. 報告整個停止時不回收任何東西（F32）。機器永久消失時由人工 `mftik intents gc --instance <name>`。
5. 回收錯了也能自癒：主機恢復後，STS controller 的 reconcile 會替每個 running session 重新 `intent.put`。controller 不在線時不會有東西因為沒續約而過期。

`md_sessions` / `td_sessions` 從切換起停寫、保留唯讀，只用來查切換前的歷史。前端的 MD／TD 頁顯示 worker 與 intent，資料來自 procman 回報和 worker 狀態廣播（F38）。

## 8. 協定

（§8.3、F1、F25、F26、F41）

每則 NATS 訊息的 body 是一個 envelope：`id`、`type`、`source`、`session_id`、`reply_to`、`ts`、`payload`，以及 optional 的 `seq`（只有 MD 連線 worker 在 `md.a.*` 上設，F25）。body 不帶 `pv`。`pv` 在 header `Mftik-Pv`，由 transport 蓋上。線上格式一有變動就升這個整數，不做版內相容，也不做 schema 比對。

`pv` 在兩個地方擋（F41）：

- **deploy 時：** 比對的對象見上面的 Start。worker 的 `pv` 經 `procman.report` 帶出。controller 自己的報告若因 `pv` 不符被丟棄，transport 留下的紀錄同樣讓 start 回 `protocol_mismatch`，而不是 `unavailable`。
- **執行中：** transport 在任何解碼之前檢查 header。缺少或不符的 frame 直接丟棄，記 log（依 subject 與 `pv` 限流）並計數，不回錯誤。request 收到不符的 reply 時，transport 對呼叫端拋出本地錯誤，不等 timeout。送錯版本的 request 會在呼叫端 timeout；廣播只被計數。所以 deploy 時的比對要先擋。

handler（`mftik.broker.handler`）拿到的是已經解碼的訊息。這層不持有任何狀態的權威。和 `pv` 有關的兩條不變式維持不變，因為不符的 frame 在解碼之前就被丟了，handler 看不到，也不負責回版本錯誤：

- **H5** handler 的例外只代價一則回覆，不代價整個 subject。`serve` 記 log 然後繼續，不替 handler 發明一則錯誤回覆。要回答的 handler 自己回一個錯誤 envelope。
- **H6** 這一層不解讀 payload。handler 拿到的是未定型的 envelope，自己驗證要讀的欄位。

行為測試直接呼叫 handler；連線測試才碰 NATS（F31、§9）。

| 用途 | subject 或 type |
|---|---|
| STS 控制面 | `sts.{instance}`：`sts.session.start`、`sts.session.end`、list、registry、env、artifact、event log |
| session 控制 | `sts.ctl.{session_id}`：stop、fail、status |
| session 進度 | `sts.status.{session_id}`；彙總頻道 `status.sts` 仍在 |
| session log | `log.sts.{session_id}` |
| MD intent | `md.intent.put` / `md.intent.delete` / `md.intent.patch`，帶 owner |
| 行情 | `md.a.{venue}.{hash}` |
| MD 狀態 | `md.w.{instance}.{worker_id}`：`md.worker.state`、`md.atom.state` |
| selector | `md.universe.{session_id}`：name、added、removed、current、epoch |
| feed 結束 | `md.feed.end`，以 owner 為對象。沒有 gap 訊息 |
| 歷史讀取 | `md.fetch` |
| TD intent | `td.intent.put` / `td.intent.delete`，帶 owner |
| 下單與清場 | `td.order.{api_id}`，含 `td.order.cancel_session` |
| 帳本讀取與交易層開關 | `td.account.{api_id}`：`oms.view`、`ledger.view`、`td.account.trading` |
| 帳號廣播 | `td.{api_id}.global`、`td.oms.*`、`td.ledger.*`、`td.account.state.{api_id}`、`td.account.reset` |
| drain-replace | `td.{instance}` 上的 `td.account.drain`（F27） |
| 存活報告 | `procman.report.{plane}.{instance}` |
| 健康 | `health.*` 與 instance subject 保留 |

刪除的有：per-session lease 與 `STS_LEASE_HEARTBEAT` / `MD_LEASE_ACK` / `TD_LEASE_ACK`、`md.session.attach` / `td.session.attach`、`md.{session_id}` 的 per-session fan-out、`md.subscribe` / `md.unsubscribe`（改成 `md.intent.patch`）、`STS_RECON`、envelope body 裡的 `pv`、`owner` 欄位。舊協定不留相容層（F1）。

## 9. 資料

（§8.4、§3.3）

`sts_sessions` 分成 SessionSpec 與 Status。Spec 含策略、參數、`restart`（`never` / `on_failure`）、timeout、`generation`，以及 start 時釘住的 `strategy_digest`、`env_generation`（F39）。Status 含 `observed_generation`、phase、`worker_incarnation`、`conditions`、`restart_count`、失敗原因。沒有 `st_facts`（F36）。策略內部狀態不落地（F10）。`restart_count` 記的是 F11 的重新掛起次數。

新增：

- `md_intents(session_id, instance, feeds, atoms, generation, created_at, released_at)`
- `md_standing_subscriptions`
- `td_intents(session_id, api_id, created_at, released_at)`
- selector 狀態：`(spec_hash, universe, epoch, center, updated_at)`，其中 `Center.at` 是置中時間的明確欄位（F44）
- `md_controller(instance PK, controller_epoch)`（F44）
- `apis` 上的帳號設定，例如 cancel-on-disconnect（F37）。`instance_id` 不可經 API 改（F45）

intent 兼任歷史：session 結束時列不刪，改記 `released_at`（F38）。誰可以寫哪一列，見上面的狀態權威表（§3.3）。`md_sessions` / `td_sessions` 停寫、保留唯讀。

Supervisor 的本機狀態（shim socket、exit 紀錄、`supervisor.json`）在 `${WORK_DIR}/run/`，不放 DB。tape 在每個 region 一台 Redis，key 是 `atom_id`，只有持有該 atom 的連線 worker append（F20、F21）。訂單、成交、資金流水在 Postgres，由 TD 帳號 worker 寫（live 加 backfill）。

## 10. 測試標準

（§9、F30、F31、F47）

測試分四層：unit（預設，純函數、直接呼叫的 handler、`FakeClock`；禁止網路、子進程、真的 sleep、NATS、DB 檔案）、component（marker `component`：共用連線的真 NATS、sqlite `:memory:`、in-proc 的 procman fake、loopback 上的 venue stub）、integration（marker `integration`：真 NATS、Postgres、真的 Supervisor 和 shim）、e2e（marker `e2e`：compose stack）。unit 與 component 跑在 `just test`，integration 跑在 `just test-int`。`just test` 的 wall time 以 GitHub Actions 的 `ubuntu-latest` 為準，120 秒是 CI 上的硬閘門，不含依賴安裝和服務啟動。單一測試的 call phase 上限在本機判定失敗，在 CI 上 unit 與 component 只輸出 warning；integration 的上限在 CI 上仍判定失敗（F47）。時間一律經 `Clock` 注入。不引入 broker fake：連線語意用真 NATS 測，收到之後的行為直接呼叫 handler。reconciler 和 selector 以表格驅動的純函數測試。策略以 `StrategyHarness` 測。repository 的 component 用 sqlite，Postgres 方言只在 integration。每個 bug fix 都附能重現問題的最低 tier 測試。細則見 [TESTING.md](TESTING.md)。

## 11. 數值

F42 的重啟曲線（MD 連線、TD 帳號、MD fetch 的 crash backoff、crash-loop 門檻、歸零條件、heartbeat timeout、進程內重連）以及其餘暫定常數，以計畫的附錄 D 為準。本文不抄那張表。已經寫在前面的數字，是 F11、F12、F14、F15、F43 與 §4.7 的分數表自己定下來的行為，不是附錄 D 的暫定值。

## 實作進度

實作進度見 [REFACTOR_TICKETS.md](REFACTOR_TICKETS.md)。部署的主機事實見 [Deployment.md](Deployment.md)。
