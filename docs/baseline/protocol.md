# protocol — 現行協定盤點（B0-03、issue #156）

> **基準：** `main` @ `a0cbfb2`（`ARCHITECTURE_CHANGE_PLAN.md` v0.26 的基準 commit）。B0-01 的 `arch/baseline` tag 還沒打，所以本文一律以 commit hash 稱呼基準。
>
> `refactor/process-planes` 相對 `a0cbfb2` 只多了三個文件檔（`git diff --stat a0cbfb2 refactor/process-planes`：`ARCHITECTURE_CHANGE_PLAN.md`、`REFACTOR_TICKETS.md`、`baseline/closed-branches.md`），`apps/` 和 `packages/` 完全沒有差異。因此本文引用的行號在兩個 ref 上都成立。
>
> 「§」指 `ARCHITECTURE_CHANGE_PLAN.md` 的章節，「F」指同一份文件的決策編號。

## 1. 盤點方法與用詞

**盤點的兩個軸。** 這套協定裡「訊息型別」和「subject」是兩件獨立的事：

- **型別**是 `Envelope.type` 這個字串（`packages/common/src/mftik/protocol/envelope.py:31`）。收件端靠它分派——各平面的 `rpc/router.py` 都是一張 `type → handler` 的表。常數定義在 `packages/common/src/mftik/protocol/messages.py`。
- **subject**是 broker 的 channel 名稱，由 `packages/common/src/mftik/protocol/topics.py` 的 `Topics` 組出來。

同一個 subject 上會跑多種型別（`sts.{instance}` 承載 17 種請求），同一個型別也會走多個 subject（`md.session.attach` 視 `strategy.yml` 有沒有指名 instance，送到 anycast 的 `md` 或具名的 `md.{instance}`）。所以下面分兩章列，型別表註明它走哪個 subject。

**「發送者」「接收者」怎麼認定。** 一律以 `apps/`、`packages/`、`scripts/` 裡的非測試代碼為準：

- **發送者** = 建出帶該 `type` 的 envelope 並 `publish` / `request` 出去的地方。
- **接收者** = 對該 `type` 做分派或驗證 payload 的地方。注意 **RPC 的回覆型別，接收端通常不檢查 `type`**，它直接 `model_validate(reply.payload)`（例如 `packages/common/src/mftik/strategy/oms.py:653` 讀 `OrderAck`）。所以「型別常數沒有被 import」不等於「這個訊息沒有接收者」，第 4 章把兩者分開講。
- 只出現在 `tests/` 的發送者，在本文算**沒有生產發送者**，並在第 4 章單獨列出。

**「去向」的四種值：**

| 去向 | 意思 |
|---|---|
| 保留 | 新協定仍然需要，名字不變 |
| 改名 | 語意保留，subject 或型別名稱改掉（列出改成什麼） |
| 刪除 | 新協定不再有這件事 |
| 新增 | 現在沒有，新協定才有（只在第 5 章出現） |

每一列的去向後面標出依據：`§x.y` 表示計畫明寫，`§x.y 推論` 表示由該章節推得但沒有逐項點名，`無依據` 表示計畫全文都沒提到，由本文提出建議並在第 5.3 節彙整。

---

## 2. Subject 盤點

### 2.1 Request-reply（`serve` / `request`）

| subject | `Topics` 成員 | 服務者 | 呼叫者 | 去向 |
|---|---|---|---|---|
| `sts` | `Topics.STS`（`topics.py:21`） | STS，role 有 `serves_anycast` 時（`mftik/instance.py:177`、`:187`） | API `routes/sts.py:354`（list）、`routes/environment.py:165`（list）、`routes/sts.py:623`／`:664`（沒指名 instance 的 eventlog）、`sts_fanout.py:196`／`:204`（沒有任何已啟用 instance 時的 fallback） | 保留（§5.1） |
| `sts.{instance}` | `Topics.sts`（`topics.py:44`） | STS（`instance.py:186`、`apps/sts/src/mftik_sts/app.py:86`） | API `orchestrate.py:116`（create）、`routes/sts.py:963`（force_stop）、`routes/sts.py:623`（eventlog）、`routes/artifacts.py`（artifacts）、`sts_fanout.py:515`（registry／env） | 保留（§5.1） |
| `sts.control.{session_id}` | `Topics.sts_control`（`topics.py:165`） | STS session manager `session/manager.py:2375`；worker 模式只收 stop／fail（`worker.py:257`） | API `routes/sts.py:856`（stop）、`orchestrate.py:287`（回滾時的 fail） | **改名**為 `sts.ctl.{session_id}`（§3.1 表、§5.1） |
| `md` | `Topics.MD`（`topics.py:22`） | MD（`instance.py:177`、`apps/md/src/mftik_md/app.py:74`） | API `orchestrate.py:191`（未指定 instance 的 attach）、`routes/md.py:28`（list）；STS `session/manager.py:2008`（rebuild attach）、`session/session.py:604`（detach） | 保留（§3.1 推論；anycast 的角色由 intent 取代） |
| `md.{instance}` | `Topics.md`（`topics.py:49`） | MD（`instance.py:186`） | API `orchestrate.py:191`；STS `session/manager.py:2010`、`session/session.py:606`；策略 `strategy/tape.py:470`（`md.tape.tail`） | 保留（§3.1 推論） |
| `md.fetch` | `Topics.md_fetch`（`topics.py:202`） | MD `fetch/session.py:239`（`FetchSession`，不綁 session、不綁 lease） | 策略 `strategy/mds.py:258`；`scripts/fetch_md.py:156` | 保留（§3.1「MD `fetch`：服務 `md.fetch`」） |
| `td.{instance}` | `Topics.td`（`topics.py:27`） | TD（`instance.py:186`、`apps/td/src/mftik_td/app.py:70`） | API `orchestrate.py:236`（attach）；STS `session/manager.py:2074`（rebuild attach）、`session/session.py:587`（detach） | 保留（§3.1 推論；attach／detach 改成 intent） |
| `td.order.{api_id}` | `Topics.td_order`（`topics.py:230`） | TD 帳號 `session/manager.py:975` | 策略 `strategy/oms.py:627` | 保留（§3.1、§7.1） |
| `td.account.{api_id}` | `Topics.td_account`（`topics.py:247`） | TD 帳號 `session/manager.py:1005` | 策略 `strategy/oms.py:212`／`:251`、`strategy/ledger.py:119`／`:193` | 保留（§7.1「帳本查詢」） |
| `td.backfill.{instance}` | `Topics.td_backfill`（`topics.py:258`） | TD `backfill/session.py:125` | API `backfill_cron.py:92`；TD `backfill/trigger.py:55` | 保留（§7.1、F35） |
| `sym` | `Topics.SYM`（`topics.py:23`） | SYM `apps/sym/src/mftik_sym/app.py:62` | API `routes/sym.py:45`／`:116`；`SymbolClient._request`（`symbols/client.py:289`）（MD、TD、STS 都用） | 保留（§3.3「listing：SYM」；SYM 不在本次重構範圍） |
| `paper` | `Topics.PAPER`（`topics.py:24`） | paper `apps/paper/src/mftik_paper/app.py:201` | `exchange/paper/remote.py:214`（TD 私有）、`exchange/paper/remote_public.py:119`（MD／SYM 公開） | 保留（B4「只接 paper venue」） |
| `health.{domain}.{instance}` | `Topics.health`（`topics.py:59`） | `mftik/health.py:69`（`serve_health`，STS／TD／MD 各一） | API `routes/stats.py:53`（dashboard）、`orchestrate.py:429`（deploy 前檢查 instance 在不在） | 保留（§8.3 最後一列「`health.*`、instance subject 保留」） |
| `api.registry.catchup` | 無（`mftik_api/registry_catchup.py:31`，subject 直接等於型別字串） | API `registry_catchup.py:100` | STS `registry_catchup.py:34` | **無依據**，建議保留 |

### 2.2 Pub/sub

| subject | `Topics` 成員 | 發佈者 | 訂閱者 | 去向 |
|---|---|---|---|---|
| `sts.td.{session_id}` | `Topics.sts_td_session`（`topics.py:187`） | STS session `session/session.py:759`（lease heartbeat）；策略 `strategy/base.py:365`（`sts.recon`） | TD `session/manager.py:758`（`LeasedSessionLink.rx`，`mftik/broker/link.py:125`） | **刪除**（§8.3、§8.2） |
| `sts.md.{session_id}` | `Topics.sts_md_session`（`topics.py:192`） | STS session `session/session.py:763` | MD `session/manager.py:1436` 的 `_lease_loop` | **刪除**（§8.3、§8.2） |
| `md.{session_id}` | `Topics.md_session`（`topics.py:197`） | MD `session/dispatcher.py:129`（行情）、`session/manager.py:1329`（`md.feed.end`）、`session/manager.py:1503`（lease ack） | STS session `session/session.py:834`（`_pump_md_session`） | **刪除**，改成 per-atom 的 `md.a.{venue}.{atom_hash}`（§8.3） |
| `md.fetch.reply.{caller}` | `Topics.md_fetch_reply`（`topics.py:219`） | MD `fetch/session.py:421`（發到 request 自己帶的 `reply_channel`） | STS session `session/session.py:862`（`_pump_fetch_replies`）；`scripts/fetch_md.py:143` | 保留（§3.1 推論，`md.fetch` 的回程） |
| `td.{api_id}.global` | `Topics.td_global`（`topics.py:154`） | TD `session/session.py:1428`（`_publish_global`）、`session/manager.py:675`（keepalive） | STS session `session/session.py:1077`（`_pump_td_global`）；API `ws.py:404`（以 `td.*.global` pattern，`topics.py:139`） | 保留（§3.1「發佈 `td.{api_id}.global`」、§7.1） |
| `td.{api_id}.{session_id}` | `Topics.td_session`（`topics.py:160`） | TD `session/manager.py:758`（`LeasedSessionLink.tx`，lease ack）、`session/manager.py:872`（`td.recon.done`） | STS session `session/session.py:1056`（`_pump_td_session`） | **刪除**（§8.3 刪 lease；§2.3「per-session fan-out」） |
| `status.sts` | `Topics.status_sts`（`topics.py:144`） | STS `session/manager.py:420`；API `routes/sts.py:456`（ack 之後補一筆） | API `ws.py:297`（`/ws/status/sts`） | 保留（§8.3 的 `sts.session.status` 走這條；見 5.1 第 1 列） |
| `log.sts.{session_id}` | `Topics.log_sts`（`topics.py:99`） | 三個平面與 API，都經 `protocol/session_log.py:41` | API `ws.py:162`（`/ws/sts/{id}`）、`log_persist.py:144`、`alert_match.py:509`（後兩者用 `log.*.*` pattern，`topics.py:134`） | 保留（§5.2 要求重啟時在這裡發 `error` log） |
| `log.td.{api_id}` | `Topics.log_td`（`topics.py:104`） | 同上（`session_log.py:64`） | 同上 | 保留（**無依據**，但 §5.2 的 log 管線依賴它） |
| `log.md.{venue}` | `Topics.log_md`（`topics.py:109`） | 同上（`session_log.py:93`） | 同上 | 保留（**無依據**） |
| `sys.heartbeat` | `Topics.HEARTBEAT`（`topics.py:97`） | 五個平面都發：`mftik/broker/client.py:237`（`heartbeat_loop`），由 `mftik/runtime.py:86`、`sts/app.py:359`、`td/app.py:194`、`md/app.py:269`、`sym/app.py:104`、`paper/app.py:267` 啟動 | **無** | **刪除**（**無依據**；見 4.2） |
| `paper.{api_key}.orders` | `Topics.paper_orders`（`topics.py:74`） | paper `app.py:67` | `exchange/paper/remote.py:209` | 保留 |
| `paper.{api_key}.fills` | `Topics.paper_fills`（`topics.py:78`） | paper `app.py:74` | 同上 | 保留 |
| `paper.{api_key}.balances` | `Topics.paper_balances`（`topics.py:82`） | paper `app.py:81` | 同上 | 保留 |
| `paper.public.orderbook.{symbol}` | `Topics.paper_order_book`（`topics.py:86`） | paper `app.py:112` | `exchange/paper/remote_public.py:114` | 保留 |

### 2.3 定義了但在整個 repo 完全沒有呼叫端的 `Topics` 成員

以下都是 `grep` 全 repo（含測試、frontend、deployment、scripts）零引用：

| 成員 | 組出的 subject | 說明 | 去向 |
|---|---|---|---|
| `Topics.td_ledger`（`topics.py:291`） | `td.ledger.{api_id}` | JetStream 移除之後 TD 不再發這個 fan-out；`mftik_td/session/session.py:503` 的 `publish_oms` 已經是 `return` 的空實作 | **刪除** |
| `Topics.td_oms`（`topics.py:296`） | `td.oms.{api_id}` | 同上 | **刪除** |
| `Topics.private_order`（`topics.py:346`） | `private.order.{account}` | 沒有任何發佈者或訂閱者 | **刪除** |
| `Topics.private_balance`（`topics.py:350`） | `private.balance.{account}` | 同上 | **刪除** |
| `Topics.CMD_TRADING`（`topics.py:91`） | `cmd.trading` | 原始碼裡自己註明「Legacy / reserved」 | **刪除** |
| `Topics.CMD_STRATEGY`（`topics.py:92`） | `cmd.strategy` | 同上 | **刪除** |
| `Topics.CMD_MARKET_DATA`（`topics.py:93`） | `cmd.market_data` | 同上 | **刪除** |
| `Topics.log_session`（`topics.py:115`） | `log.sts.{session_id}` | `log_sts` 的 deprecated alias，只有 `packages/common/tests/test_envelope.py:73` 引用 | **刪除**（subject 本身由 `log_sts` 保留） |

`Topics.md_feed`、`parse_md_feed`、`normalize_md_feed` 不是 subject，是 feed key 的組裝與解析（`topics.py:301`、`:314`、`:330`），有大量呼叫端，保留；B7 之後 key 改成 `atom_id`（F20）。

---

## 3. 訊息型別盤點

### 3.1 STS 控制面（API ⇄ STS）

常數在 `messages.py:1588`–`:1615`。接收端一律是 `apps/sts/src/mftik_sts/rpc/router.py:69`–`:90` 的 `_HANDLERS` 表。

| 型別 | subject | 發送者 | 接收者 | 去向 |
|---|---|---|---|---|
| `sts.health` | `sts.{instance}`（開機自檢）與 `health.sts.{instance}`（dashboard 與 deploy 前檢查） | `mftik/health.py:145`（`refuse_if_serving`，打具名 control subject）；API `routes/stats.py:56`、`orchestrate.py:432`（打 `health.*`） | `rpc/health.py:14`（control subject）；`mftik/health.py:81`（`health.*`，回覆型別由 `:60` 的 f-string 組出） | 保留（§8.3） |
| `sts.error` | 回覆 | `rpc/router.py:111`、`rpc/sessions.py:218`、`rpc/registry.py:238`／`:296`、`rpc/env.py:145`／`:156`、`rpc/eventlog.py:209`、`rpc/artifacts.py:340`、`session/manager.py:2392` | API `broker_rpc.py:19`／`:53`（`_DEFAULT_ERROR_TYPES`） | 保留（**無依據**；F26 的 `protocol_mismatch` 需要一個錯誤型別） |
| `sts.session.create` | `sts.{instance}` | API `orchestrate.py:130` | `rpc/sessions.py:36` | **改名**為 `sts.session.start`，語意從同步等 `on_start` 變成非同步 accept（§8.3 第 1 列、§8.1） |
| `sts.session.list` | `sts`（anycast，**不是**具名 subject） | API `routes/sts.py:357`（subject 在 `:354`）、`routes/environment.py:168`（subject 在 `:165`） | `rpc/sessions.py:71` | 保留（§5.1「服務 `sts.{instance}`：start、end、list、artifacts、env」。注意 §5.1 把 list 歸給具名 subject，代碼走的是 anycast——以單一 instance 的部署而言兩者等價，多 instance 時 anycast 只會拿到其中一台的清單） |
| `sts.session.stop` | `sts.control.{session_id}` | API `routes/sts.py:379` → `:856` | `rpc/sessions.py:98`；worker 模式的白名單在 `worker.py:257` | **改名**為 `sts.session.end`（§8.1「End」步驟 1）；subject 同時改成 `sts.ctl.{session_id}` |
| `sts.session.force_stop` | `sts.{instance}` | API `routes/sts.py:970` | `rpc/sessions.py:106` | **刪除**（§5.2、§8.2 推論：強制停止改由 Supervisor 直接 SIGKILL worker，不再是一個協定訊息） |
| `sts.session.fail` | `sts.control.{session_id}` | API `orchestrate.py:290`（attach 回滾）、`orchestrate.py:494`（`_fail_sts`） | `rpc/sessions.py:119` | 保留（§3.1「服務 `sts.ctl.{session_id}`（stop、fail、status）」），subject 改名 |
| `sts.session.status` | `status.sts` | STS `session/manager.py:415`；API `routes/sts.py:451`、`ws.py:117`（後者是給晚到 socket 的 replay，沒上線） | API `ws.py:297` | 保留（§8.3 第 1 列把它列在「之後」欄，但它**現在就存在**；見 5.4 第 1 項） |
| `sts.eventlog.info` | `sts.{instance}` 或 `sts`（`routes/sts.py:623` 依有沒有指名 instance 二選一） | API `routes/sts.py:626` | `rpc/eventlog.py:62` | 保留（**無依據**；§11 B5 只說 event log 搬到新 worker） |
| `sts.eventlog.read` | `sts.{instance}` 或 `sts`（`routes/sts.py:664`） | API `routes/sts.py:675` | `rpc/eventlog.py:110` | 保留（同上） |
| `sts.artifact.list` | `sts.{instance}` | API `routes/artifacts.py:155` | `rpc/artifacts.py:69` | 保留（§5.1「artifacts」） |
| `sts.artifact.read` | `sts.{instance}` | API `routes/artifacts.py:463` | `rpc/artifacts.py:112` | 保留（同上） |
| `sts.artifact.begin` | `sts.{instance}` | API `routes/artifacts.py:278` | `rpc/artifacts.py:153` | 保留（同上） |
| `sts.artifact.chunk` | `sts.{instance}` | API `routes/artifacts.py:304` | `rpc/artifacts.py:182` | 保留（同上） |
| `sts.artifact.commit` | `sts.{instance}` | API `routes/artifacts.py:316` | `rpc/artifacts.py:226` | 保留（同上） |
| `sts.artifact.abort` | `sts.{instance}` | API `routes/artifacts.py:442` | `rpc/artifacts.py:263` | 保留（同上） |
| `sts.artifact.delete` | `sts.{instance}` | API `routes/artifacts.py:360` | `rpc/artifacts.py:289` | 保留（同上） |
| `sts.registry.reload` | `sts.{instance}` | API `sts_fanout.py:369` | `rpc/registry.py:275` | 保留（**無依據**） |
| `sts.registry.sync` | `sts.{instance}` | API `sts_fanout.py:570` | `rpc/registry.py:221` | 保留（**無依據**） |
| `sts.registry.loaded` | `sts.{instance}` | API `sts_fanout.py:624` | `rpc/registry.py:259` | 保留（**無依據**） |
| `sts.registry.generation` | `sts.{instance}` | API `sts_fanout.py:698` | `rpc/registry.py:314` | 保留（**無依據**） |
| `sts.env.sync` | `sts.{instance}` | API `sts_fanout.py:660` | `rpc/env.py:123` | 保留（§5.1「env」） |
| `api.registry.catchup` | 同名 subject | STS `registry_catchup.py:37` | API `registry_catchup.py:60`（`handle_catchup`） | 保留（**無依據**） |

### 3.2 STS → TD / STS → MD 的資料面型別

常數在 `messages.py:1647`–`:1653`。

| 型別 | subject | 發送者 | 接收者 | 去向 |
|---|---|---|---|---|
| `sts.lease.heartbeat` | `sts.td.{sid}`、`sts.md.{sid}` | STS `session/session.py:752` | `mftik/broker/link.py:125`（`LeasedSessionLink`），由 TD `session/manager.py:758` 與 MD `session/manager.py:1436` 掛起 | **刪除**（§8.3 第 6 列） |
| `sts.recon` | `sts.td.{sid}` | 策略 `strategy/base.py:368`；第一次收到 TD lease ack 時自動觸發，`sts/session/session.py:1176` | TD `session/manager.py:705` | **刪除**（F13、§5.2） |
| `sts.detach` | `sts.td.{sid}` | **沒有生產發送者**（只有 `apps/td/tests/test_lease_resilience.py:317`） | TD `session/manager.py:711` | **刪除**（F13／§8.3 推論；實務上已被 `td.session.detach` RPC 取代，理由寫在 `messages.py:198` 的 `TdDetachRequest` docstring） |
| `sts.order.submit` | `td.order.{api_id}` | 策略 `strategy/oms.py:548` | TD `session/manager.py:1263` | 保留（§3.1、§7.1 的下單路徑） |
| `sts.order.cancel` | `td.order.{api_id}` | 策略 `strategy/oms.py:593` | TD `session/manager.py:1266` | 保留（同上） |
| `sts.ensure_leverage` | `td.account.{api_id}` | 策略 `strategy/ledger.py:187` | TD `session/manager.py:1127` | 保留（§3.1「槓桿快取」） |

### 3.3 TD（`messages.py:1483`–`:1502`）

TD 的 `rpc/router.py:32`–`:36` 只掛三個型別（health、attach、detach）；其餘在 `session/manager.py` 的各個 serve／lease loop 裡分派。

| 型別 | subject | 發送者 | 接收者 | 去向 |
|---|---|---|---|---|
| `td.health` | `td.{instance}`（開機自檢）與 `health.td.{instance}` | `mftik/health.py:145`（`refuse_if_serving`）；API `routes/stats.py:56`、`orchestrate.py:432`（走 `health.*`） | `rpc/health.py:18`；`mftik/health.py:81` | 保留（§8.3） |
| `td.error` | 回覆 | `rpc/router.py:58`、`rpc/sessions.py:127` | API `broker_rpc.py:19`；STS `session/manager.py:2086`（`error_type`） | 保留（**無依據**） |
| `td.session.attach` | `td.{instance}` | API `orchestrate.py:244`；STS `session/manager.py:2082` | `rpc/sessions.py:32` | **改名**為 `td.intent.put`，並帶 owner（§8.3 第 3 列、§8.1 步驟 3） |
| `td.session.detach` | `td.{instance}` | STS `session/session.py:592` | `rpc/sessions.py:71` | **改名**為 `td.intent.delete`（§8.3 第 3 列、§8.1 End 步驟 2） |
| `td.session.list` | — | **沒有** | **沒有** | **刪除**（見 4.1） |
| `td.lease.ack` | `td.{api_id}.{sid}` | TD `session/manager.py:751` | STS `session/session.py:1057` | **刪除**（§8.3 第 6 列） |
| `td.recon.done` | `td.{api_id}.{sid}` | TD `session/manager.py:880` | STS `session/session.py:1060` | **刪除**（F13；`on_recon_done` 一併刪除） |
| `td.oms.view` | `td.account.{api_id}` | 請求：策略 `strategy/oms.py:215`；回覆：TD `session/manager.py:1092`／`:1098` | TD `session/manager.py:1051`；回覆由 `strategy/oms.py` 讀 `OmsView` | 保留，並新增 `settled=True`（§7.1「帳本查詢」）。注意 §3.1／§7.1 寫「服務 `td.oms.*`」，實際服務在 `td.account.{api_id}`（見 5.4 第 2 項） |
| `td.oms.order` | `td.account.{api_id}` | 請求：`strategy/oms.py:254`；回覆：TD `session/manager.py:1109`／`:1115`／`:1119` | TD `session/manager.py:1054` | 保留（同上） |
| `td.ledger.view` | `td.account.{api_id}` | 請求：`strategy/ledger.py:114`；回覆：TD `session/manager.py:1068`／`:1081` | TD `session/manager.py:1048` | 保留（同上） |
| `td.order.update` | `td.{api_id}.global` | TD `session/session.py:1357` | STS `session/session.py:139`（`TD_GLOBAL_HANDLERS` → `on_order_update`） | 保留（§3.1） |
| `td.fill` | `td.{api_id}.global` | TD `session/session.py:1309` | STS `session/session.py:140`；API `ws.py:418`（board bridge） | 保留（同上） |
| `td.order.ack` | `td.order.{api_id}` 的回覆 | TD `session/manager.py:1471` | 策略 `strategy/oms.py:653`（讀 payload，不比對 `type`） | 保留（同上） |
| `td.leverage.ack` | `td.account.{api_id}` 的回覆 | TD `session/manager.py:1246` | 策略 `strategy/ledger.py:208` | 保留（同上） |
| `td.order.reject` | `td.{api_id}.global` | TD `session/session.py:1176` | STS `session/session.py:141` | 保留（同上） |
| `td.cancel.reject` | `td.{api_id}.global` | TD `session/session.py:1204` | STS `session/session.py:142` | 保留（同上） |
| `td.balance.update` | `td.{api_id}.global` | TD `session/session.py:1110` | STS `session/session.py:143` | 保留（同上） |
| `td.position.update` | `td.{api_id}.global` | TD `session/session.py:1384` | STS `session/session.py:144` | 保留（同上） |
| `td.backfill` | `td.backfill.{instance}` | API `backfill_cron.py:88`；TD `backfill/trigger.py:50` | TD `backfill/session.py:125` | 保留（§7.1、F35） |
| `td.backfill.result` | 上面的回覆 | TD `backfill/session.py:202` | API `backfill_cron.py:103`；TD `backfill/trigger.py:74` | 保留（同上） |

### 3.4 MD（`messages.py:1655`–`:1689`）

| 型別 | subject | 發送者 | 接收者 | 去向 |
|---|---|---|---|---|
| `md.health` | `md.{instance}`（開機自檢）與 `health.md.{instance}` | `mftik/health.py:145`（`refuse_if_serving`）；API `routes/stats.py:56`、`orchestrate.py:432` | `rpc/health.py:14`；`mftik/health.py:81` | 保留（§8.3） |
| `md.error` | 回覆 | `rpc/router.py:63`、`rpc/sessions.py:147`、`rpc/tape.py:113` | API `broker_rpc.py:19`；STS `session/manager.py:2023` | 保留（**無依據**） |
| `md.session.attach` | `md` 或 `md.{instance}` | API `orchestrate.py:199`；STS `session/manager.py:2019` | `rpc/sessions.py:34` | **改名**為 `md.intent.put`，帶 owner（§8.3 第 2 列、§8.1 步驟 4） |
| `md.session.detach` | 同上 | API `orchestrate.py:461`（回滾）；STS `session/session.py:610` | `rpc/sessions.py:75` | **改名**為 `md.intent.delete`（§8.3 第 2 列） |
| `md.session.list` | `md` | API `routes/md.py:31` | `rpc/sessions.py:117` | 保留（**無依據**；對應的 as-is 權威是 `md_intents`，§8.4） |
| `md.tape.tail` | `md.{instance}` | 策略 `strategy/tape.py:473` | `rpc/tape.py:43` | 保留（§3.3「tape 與 coverage … MD 的讀取 RPC → 策略」，沒有點名型別） |
| `md.lease.ack` | `md.{session_id}` | MD `session/manager.py:1503` | STS `session/session.py:835` | **刪除**（§8.3 第 6 列） |
| `md.orderbook` | `md.{session_id}` | MD `session/venue.py:251` | STS `session/session.py:107` | **保留型別、改 subject**：改發在 `md.a.{venue}.{atom_hash}`（§8.3 第 4 列、§6.1） |
| `md.ticker` | 同上 | `session/venue.py:253` | `session/session.py:106` | 同上 |
| `md.trade` | 同上 | `session/venue.py:255` | `session/session.py:109` | 同上 |
| `md.aggtrade` | 同上 | `session/venue.py:257` | `session/session.py:110` | 同上 |
| `md.kline` | 同上 | `session/venue.py:275` | `session/session.py:108` | 同上 |
| `md.bestquote` | 同上 | `session/venue.py:259` | `session/session.py:111` | 同上 |
| `md.liquidation` | 同上 | `session/venue.py:261` | `session/session.py:112` | 同上 |
| `md.funding_rate` | 同上 | `session/venue.py:263` | `session/session.py:113` | 同上 |
| `md.open_interest` | 同上 | `session/venue.py:265` | `session/session.py:114` | 同上 |
| `md.greeks` | 同上 | `session/venue.py:267` | `session/session.py:115` | 同上 |
| `md.feed.end` | `md.{session_id}` | MD `session/manager.py:1322` | STS `session/session.py:116` | 保留，收件對象改成 owner；不另外有 gap 訊息、沒有 `on_feed_gap`（§8.3 第 7 列、F23） |
| `md.subscribe` | `sts.md.{sid}` | **沒有生產發送者**（只有 `apps/md/tests/test_md_expiry.py` 與 `test_md_lease_resilience.py:159`） | MD `session/manager.py:1443` | **刪除**。§8.3 第 5 列說改成 `md.intent.patch`，但這裡沒有東西可以改名（見 4.1、5.4 第 3 項） |
| `md.unsubscribe` | `sts.md.{sid}` | **沒有生產發送者**（只有 `test_md_expiry.py:628`） | MD `session/manager.py:1457` | 同上 |
| `md.detach` | `sts.md.{sid}` | **完全沒有發送者**，連測試都沒有 | MD `session/manager.py:1469` | **刪除**（§8.3 推論；已被 `md.session.detach` RPC 取代，理由在 `messages.py:900` 的 `MdDetachRequest` docstring） |
| `md.fetch.klines` | `md.fetch` | 策略 `strategy/mds.py`；`scripts/fetch_md.py` | MD `fetch/session.py:82`（`_KINDS`） | 保留（§3.1） |
| `md.fetch.orderbook` | `md.fetch` | 同上 | `fetch/session.py:99` | 保留 |
| `md.fetch.bestquote` | `md.fetch` | 同上 | `fetch/session.py:113` | 保留 |
| `md.fetch.funding_history` | `md.fetch` | 同上 | `fetch/session.py:127` | 保留 |
| `md.fetch.open_interest` | `md.fetch` | 策略 `strategy/mds.py`（`scripts/fetch_md.py` 不支援這個） | `fetch/session.py:141` | 保留 |
| `md.query.ack` | `md.fetch` 的回覆 | MD `fetch/session.py:340` | 策略 `strategy/mds.py:282` | 保留 |
| `md.klines.result` | `md.fetch.reply.{caller}` | MD `fetch/session.py:85` | STS `session/session.py:124`；`scripts/fetch_md.py` | 保留 |
| `md.orderbook.result` | 同上 | `fetch/session.py:102` | `session/session.py:125` | 保留 |
| `md.bestquote.result` | 同上 | `fetch/session.py:116` | `session/session.py:126` | 保留 |
| `md.funding_history.result` | 同上 | `fetch/session.py:130` | `session/session.py:127` | 保留 |
| `md.open_interest.result` | 同上 | `fetch/session.py:144` | `session/session.py:131` | 保留 |

### 3.5 SYM（`messages.py:1895`–`:1899`）

| 型別 | subject | 發送者 | 接收者 | 去向 |
|---|---|---|---|---|
| `sym.health` | — | **沒有** | **沒有**（`sym/rpc.py:128` 的 `_HANDLERS` 沒有這一項；SYM 不在 `INSTANCED_PLANES`，`mftik/instance.py:54`，所以沒有 `serve_health`） | **刪除**（見 4.1） |
| `sym.error` | 回覆 | `sym/rpc.py:122` | API `routes/sym.py:30`；`symbols/client.py:292`（後者只比對 `.error` 後綴，見 4.4） | 保留 |
| `sym.list` | `sym` | API `routes/sym.py:116`；`symbols/client.py:236`／`:256` | `sym/rpc.py:129` | 保留 |
| `sym.venues` | `sym` | API `routes/sym.py:45`；`symbols/client.py:158` | `sym/rpc.py:130` | 保留 |
| `sym.refresh` | `sym` | `symbols/client.py:163`（**只有 SDK，API 沒有對應路由**） | `sym/rpc.py:131` | 保留 |

### 3.6 Paper（`messages.py:1505`–`:1520`）

全部由 `apps/paper/src/mftik_paper/rpc.py:43`–`:67` 的 `dispatch` 分派，subject 一律是 `paper`；三個 stream 型別走 `paper.{api_key}.*`。

| 型別 | subject | 發送者 | 接收者 | 去向 |
|---|---|---|---|---|
| `paper.error` | 回覆 | `rpc.py:279` | `exchange/paper/remote.py:217`、`remote_public.py:122` | 保留 |
| `paper.auth` | `paper` | `exchange/paper/remote.py:79` | `rpc.py:45` | 保留 |
| `paper.place_order` | `paper` | `remote.py:108` | `rpc.py:47` | 保留 |
| `paper.cancel_order` | `paper` | `remote.py:121` | `rpc.py:49` | 保留 |
| `paper.cancel_by_client_order_id` | `paper` | `remote.py:136` | `rpc.py:51` | 保留 |
| `paper.fetch_order` | `paper` | `remote.py:151` | `rpc.py:53` | 保留 |
| `paper.fetch_open_orders` | `paper` | `remote.py:166` | `rpc.py:55` | 保留 |
| `paper.fetch_balances` | `paper` | `remote.py:182` | `rpc.py:57` | 保留 |
| `paper.fetch_instruments` | `paper` | `remote_public.py:44`／`:59` | `rpc.py:59` | 保留 |
| `paper.fetch_ticker` | `paper` | `remote_public.py:71` | `rpc.py:61` | 保留 |
| `paper.fetch_order_book` | `paper` | `remote_public.py:88` | `rpc.py:63` | 保留 |
| `paper.order` | `paper.{api_key}.orders` | `app.py:69` | `remote.py:210` | 保留 |
| `paper.fill` | `paper.{api_key}.fills` | `app.py:76` | `remote.py:210` | 保留 |
| `paper.balance` | `paper.{api_key}.balances` | `app.py:83` | `remote.py:210` | 保留 |
| `paper.orderbook` | `paper.public.orderbook.{symbol}` | `app.py:112` | `remote_public.py:114` | 保留 |
| `paper`（`messages.py:1505`） | — | — | — | **刪除**：這是 subject 名稱不是型別，而且和 `Topics.PAPER`（`topics.py:24`）重複；整個 repo 沒有任何地方從 `mftik.protocol` import 它 |

### 3.7 上線但沒有常數的 wire type

這三個字串在線上流通，`messages.py` 裡沒有對應常數。新協定若要靠 `pv` 全面擋版（F26），這些也得有定義。

| 型別字串 | subject | 發送者 | 接收者 | 去向 |
|---|---|---|---|---|
| `"log"` | `log.sts.*`、`log.td.*`、`log.md.*` | `protocol/session_log.py:37`／`:60`／`:89`；API `ws.py:81`（replay）、`ws.py:191`（welcome）、`ws.py:213`（socket 回寫） | API `ws.py:162`、`log_persist.py:144`、`alert_match.py:509` | 保留，建議補上常數 |
| `"heartbeat"` | `sys.heartbeat` | `mftik/broker/client.py:234` | **無** | **刪除**（見 4.2） |
| `"td.global.keepalive"` | `td.{api_id}.global` | TD `session/manager.py:679` | **無 handler**。STS `session/session.py:1090` 的 `TD_GLOBAL_HANDLERS.get()` 找不到，落到 `_on_message` 只記 event log | **刪除**（**無依據**；註解 `session/manager.py:669` 自己說它已經不再刷新任何 KV claim） |

---

## 4. 定義了但沒有發送者或沒有接收者

### 4.1 兩邊都沒有（純死代碼）

| 名稱 | 定義 | 證據 | 去向 |
|---|---|---|---|
| `TD_SESSION_LIST`（`td.session.list`） | `messages.py:1487` | 除了 `protocol/__init__.py:115`／`:478` 的 re-export 和 `docs/Instances.md:411` 的敘述，整個 repo 零引用。TD 的 `rpc/router.py:32` 沒有掛它 | **刪除** |
| `SYM_HEALTH`（`sym.health`） | `messages.py:1895` | 只在 `protocol/__init__.py:92`／`:354` 出現。`sym/rpc.py:128` 沒掛，SYM 也不在 `INSTANCED_PLANES` 所以沒有 `serve_health` | **刪除** |
| `STS_HEARTBEAT` | `messages.py:1648` | `STS_LEASE_HEARTBEAT` 的 alias（註解寫「alias for older names」），零引用 | **刪除**（本體也刪） |
| `CreateSessionRequest` / `CreateSessionResult` | `messages.py:227`–`:228` | 註解寫「Backward-compatible aliases used by older call sites / tests」，但**連測試都沒有用** | **刪除** |
| `CreateSessionRequestEnvelope` / `CreateSessionResultEnvelope` | `messages.py:1407`–`:1408` | 同上 | **刪除** |
| `Topics.td_ledger` / `td_oms` / `private_order` / `private_balance` / `CMD_TRADING` / `CMD_STRATEGY` / `CMD_MARKET_DATA` | 見 2.3 | 見 2.3 | **刪除** |

### 4.2 有發送者、沒有接收者

| 名稱 | 發送者 | 證據 | 去向 |
|---|---|---|---|
| `sys.heartbeat` + 型別 `"heartbeat"` | 五個平面全部在發（見 2.2） | 全 repo 唯一另一處提到它的是 `apps/api/tests/test_log_persist.py:22`，而那行斷言的正是 `parse_log_topic("sys.heartbeat") is None` —— 連 log persister 都明確不收它 | **刪除**。存活性在新架構由 `procman.report.{plane}.{instance}` 提供（§8.2 規則 2） |
| `"td.global.keepalive"` | TD `session/manager.py:679` | 見 3.7 | **刪除** |

### 4.3 有接收者、沒有生產發送者

這四個是 ticket 特別要求查證的那一類。前三個都在 `sts.md.{session_id}` 上，第四個在 `sts.td.{session_id}` 上。

| 名稱 | 接收者 | 發送者 | 證據 |
|---|---|---|---|
| `md.subscribe` | MD `session/manager.py:1443` | **只有測試**：`apps/md/tests/test_md_expiry.py:274`／`:478`／`:587`／`:616`、`apps/md/tests/test_md_lease_resilience.py:159` | **ticket 的說法成立。** `messages.py:968` 的 `MdSubscribe` docstring 自己就寫「Nothing sends one today — MD has handled this since before there was a caller」。另外查證：`StrategyMds`（`strategy/mds.py`）只有五個 `fetch_*` 方法，沒有 `subscribe`；API 的 `routes/md.py` 也沒有訂閱路由 |
| `md.unsubscribe` | MD `session/manager.py:1457` | **只有測試**：`test_md_expiry.py:628` | 同上 |
| `md.detach` | MD `session/manager.py:1469` | **完全沒有**，連測試都沒有 | 已被 `md.session.detach` 這個 request-reply 取代。理由寫在 `messages.py:900`：session stream 的唯一讀者是 lease loop，發在那裡的 detach 剛好在最需要的時候會掉 |
| `sts.detach` | TD `session/manager.py:711` | **只有測試**：`apps/td/tests/test_lease_resilience.py:317` | 和 `md.detach` 同一個理由，已被 `td.session.detach` 取代（`messages.py:198`） |

### 4.4 型別常數沒有被收件端 import，但訊息有接收者

這類**不是**死代碼，只是因為 RPC 回覆的讀取方式不比對 `type`：

- `td.backfill.result`：收件端 `backfill_cron.py:103`、`backfill/trigger.py:74` 直接 `TdBackfillResult.model_validate(reply.payload)`。
- `md.query.ack`：收件端 `strategy/mds.py:282`、`scripts/fetch_md.py:161` 同樣只讀 payload。
- `td.order.ack`、`td.leverage.ack`、`td.oms.view`、`td.ledger.view`：同樣只讀 payload。
- `sym.list` / `sym.venues` / `sym.refresh` 的回覆：`symbols/client.py:292` 更寬鬆，它只做 `reply.type.endswith(".error")` 的後綴判斷，連平面名稱都不比對。

這也是 F26 要處理的面向之一：現在**沒有任何一個收件端會因為 `type` 不認識而拒絕一則回覆**，它只會拿 payload 去 validate，錯了就丟 `ValidationError`。API 的 `request_domain`（`broker_rpc.py:45`–`:56`）是唯一有白名單的地方，而它的白名單只有三個 error 型別，成功的回覆一律照收。

---

## 5. 對照 §8.3

### 5.1 §8.3 每一列與代碼的對照

| §8.3「現在」 | 代碼是否如此 | 備註 |
|---|---|---|
| `sts.session.create`（同步，等 `on_start` 跑完） | **是** | 同步等待的證據：`messages.py:270` 的 `StsCreateSessionResult` docstring 明寫 `on_start` / `on_ready` 在這個回覆送出前就跑完了。API 的 timeout 寫死 10 秒（`orchestrate.py:135`） |
| `md.session.attach`，加上 `sts.md.{sid}` 上的 lease | **是** | attach：`rpc/sessions.py:34`；lease：`session/manager.py:1436` |
| `td.session.attach`，加上 `sts.td.{sid}` 上的 lease | **是** | attach：`rpc/sessions.py:32`；lease：`session/manager.py:758` |
| `md.{session_id}`（per-session fan-out） | **是** | `Topics.md_session`，`dispatcher.py:129` 逐個 session 發一次 |
| `md.subscribe` / `md.unsubscribe` | **型別存在，但沒有生產發送者** | 見 4.3。所以這一列實際上是「刪除」而不是「改名」 |
| `STS_LEASE_HEARTBEAT`、`MD_LEASE_ACK`、`TD_LEASE_ACK`、`LeasedSessionLink` | **是** | 四者分別在 `messages.py:1647`、`:1661`、`:1488`、`mftik/broker/link.py` |
| `md.feed.end` | **是** | `session/manager.py:1322`；`FeedEnd` model 在 `exchange/models.py:532` |
| `health.*`、instance subject | **是** | `Topics.health`（`topics.py:59`）、`Topics.td`／`sts`／`md`（`topics.py:27`／`:44`／`:49`） |

### 5.2 §8.3 沒列、但計畫其他章節有交代的

| 現行 subject／型別 | 去向與依據 |
|---|---|
| `sts.control.{session_id}` | 改名 `sts.ctl.{session_id}`（§3.1 表格、§5.1；§5.1 還特別說「現在的 `Topics.sts_control` 已經是這個方向」） |
| `sts.session.stop` | 改名 `sts.session.end`（§8.1「End」步驟 1） |
| `sts.recon`、`td.recon.done` | 刪除（F13、§5.2、§5.4 移除清單第 707 行） |
| `td.{api_id}.{session_id}` | 刪除（§2.3 列出 `mftik.protocol` v2 的刪除項包含「per-session fan-out」） |
| `md.orderbook`…`md.greeks` | 型別保留、subject 換成 `md.a.{venue}.{atom_hash}`（§6.1、§6.3；§6.3 說明為什麼用 hash：atom_id 本身含 `.`） |
| `td.order.{api_id}`、`td.{api_id}.global`、`td.backfill.{instance}` | 保留（§3.1 表格、§7.1、F35） |
| `md.fetch`、`md.fetch.reply.*` | 保留（§3.1 的 MD `fetch` worker） |
| `sts.{instance}` 上的 list／artifacts／env | 保留（§5.1） |
| `md.tape.tail` | 保留（§3.3「tape 與 coverage」一列的「MD 的讀取 RPC → 策略」） |
| `health.{domain}.{instance}` | 保留（§8.3 最後一列） |

### 5.3 計畫全文都沒有交代的（本文標為「無依據」）

以下現行 subject／型別，在 `ARCHITECTURE_CHANGE_PLAN.md` v0.26 裡找不到任何一句話決定它的去向。本文給出建議，但 IF-01（#179）定型別時需要先確認：

1. **registry 與 env 的同步鏈：** `sts.registry.reload`、`sts.registry.sync`、`sts.registry.loaded`、`sts.registry.generation`、`api.registry.catchup`。§5.1 只說 `sts.{instance}` 服務「env」，沒提 registry。這是五個型別、一個 API 端服務的 subject，而且 `api.registry.catchup` 是**整個系統唯一一個由平面呼叫 API 的 subject**，方向和其他所有 RPC 相反。建議：保留。
2. **event log 的讀取：** `sts.eventlog.info`、`sts.eventlog.read`。§11 的 B5 只說「event log 搬到新 worker」，沒說協定。建議：保留。
3. **`md.session.list`：** API 的 `/md/sessions` 靠它。§8.4 說 as-is 的 `md_sessions` 表從 B10 起停寫、只留唯讀，但沒說這個 RPC 怎麼辦。建議：改成對 `md_intents` 的查詢。
4. **`sts.session.list`：** §5.1 寫了「list」，但沒說它的 payload（`ListSessionsRequest` / `SessionInfo`）要不要跟著 SessionSpec/Status 的新欄位（`generation`、`conditions`，§8.4）一起改。建議：保留並擴充欄位。
5. **`sts.session.force_stop`：** 現在是「worker 不答 stop 時，API 轉向 parent 要求強殺」（`messages.py:1593` 的註解）。新架構的 Supervisor 直接看得到 worker 的 PID（§7.1、F36），本文判斷它會被刪除，但計畫沒明說。
6. **log 管線：** `log.sts.*`、`log.td.*`、`log.md.*` 三個 subject 與型別 `"log"`。§5.2 要求重啟時在 `log.sts.{session_id}` 發一條 `error` log，所以 `log.sts.*` 隱含保留；`log.td.*` 與 `log.md.*` 完全沒被提到。
7. **`status.sts`：** 見 5.4 第 1 項。
8. **`sys.heartbeat` 與型別 `"heartbeat"`：** 沒有訂閱者（4.2）。建議刪除。
9. **`"td.global.keepalive"`：** 沒有 handler（3.7）。建議刪除。
10. **錯誤型別 `sts.error` / `td.error` / `md.error` / `sym.error` / `paper.error`：** F26 要求不同 `pv` 一律以 `protocol_mismatch` 拒絕，但沒說這個拒絕用哪個型別表達。建議：沿用各平面的 error 型別，`RpcError.code` 填 `protocol_mismatch`。
11. **SYM 與 paper 平面的全部協定：** `sym.*` 五個、`paper.*` 十六個。兩者都不是本次重構的平面，計畫也沒列它們。建議：整批保留。

### 5.4 計畫與代碼不符之處

1. **`sts.session.status` 被寫成新東西，但它現在就存在。** §8.3 第 1 列的「之後」欄是「`sts.session.start`（非同步 accept）＋ `sts.session.status` 事件」。`STS_SESSION_STATUS = "sts.session.status"` 定義在 `messages.py:1598`，由 `apps/sts/src/mftik_sts/session/manager.py:415` 發在 `status.sts` 上，API 在 `ws.py:297` 訂閱轉給 `/ws/status/sts`。它的 payload `StsSessionStatus`（`messages.py:796`）已經刻意設計成「完整快照而非 delta」。所以這一列應該讀成「`sts.session.create` 改名 `sts.session.start`，並改成非同步；既有的 `sts.session.status` 保留、成為進度的主要通道」，而不是新增一個事件。

2. **§3.1 與 §7.1 說 TD 帳號 worker「服務 `td.oms.*`、`td.ledger.*`」，這和代碼有兩層落差。** （§3.1 表格第 4 列、§7.1 第 963 行）
   - `td.oms.view`、`td.oms.order`、`td.ledger.view` 是**型別**，它們服務在 `td.account.{api_id}` 這個 subject 上（`session/manager.py:1005` serve，`:1048`–`:1056` 分派）。沒有任何 subject 叫 `td.oms.*` 或 `td.ledger.*` 被 serve。
   - 真正叫 `td.oms.{api_id}` / `td.ledger.{api_id}` 的 subject 由 `Topics.td_oms`／`td_ledger`（`topics.py:296`／`:291`）組出，但**兩者在全 repo 零呼叫端**，而 `publish_oms`（`mftik_td/session/session.py:503`）是空實作。`docs/JetStreamRemoval.md:186` 記錄了這個變更。
   - 影響：如果照 §3.1 的字面去設計新協定，會把一個已經刪掉的 fan-out 當成要保留的東西。建議把這兩列改寫成「服務 `td.order.{api_id}` 與 `td.account.{api_id}`（`oms.view` / `oms.order` / `ledger.view` / `ensure_leverage`）；發佈 `td.{api_id}.global`」。另外 `strategy/base.py:67`–`:68` 和 `strategy/ledger.py:9` 的 docstring 也還在說「snapshots arrive on `td.ledger.{api_id}`」，同樣過期。

3. **§8.3 把 `md.subscribe` / `md.unsubscribe` 當成要改名的現行機制。** 實際上兩者沒有生產發送者（4.3），`MdSubscribe` 的 docstring 自己就說沒有人送。所以 `md.intent.patch` 是**新增**的能力，不是舊東西改名；現行的兩個型別該直接刪。這個差別對 IF-01 有實際影響：如果當成「改名」，驗收會去找對應的舊名字並以為有呼叫端要改，實際上一個都沒有。

4. **§5.1 說「策略呼叫 `self.md.subscribe`」，這個 API 現在不存在，而且屬性名也不對。** 策略持有的是 `self.mds`（`strategy/base.py:223`），類別是 `StrategyMds`（`strategy/mds.py`），它只有 `fetch_klines`、`fetch_order_book`、`fetch_best_quote`、`fetch_funding_history`、`fetch_open_interest` 五個方法，沒有 `subscribe` / `unsubscribe`。這和第 3 項是同一件事的兩個端：SDK 沒有入口，所以 subject 上沒有發送者。

5. **§8.3 說「`health.*`、instance subject 保留」，但 `sym.health` 是個例外。** `SYM_HEALTH` 定義了卻沒有任何一端實作（4.1）。`health.*` 的 serve 只有 STS／TD／MD 三個，因為 `serve_health` 的呼叫端限於 `INSTANCED_PLANES`（`mftik/instance.py:54`）。保留 `health.*` 的同時應該刪掉 `SYM_HEALTH`。

6. **§5.1 說 list 服務在 `sts.{instance}`，代碼送的是 anycast 的 `sts`。** `routes/sts.py:354` 和 `routes/environment.py:165` 都用 `Topics.STS`。單 instance 部署時兩者等價；多 instance 時 anycast 只會拿到搶到這則請求的那一台的 in-memory session 清單，所以 `/sts/sessions` 在多 instance 下已經是不完整的。這和 §8.4 把 session 清單的權威移到 `sts_sessions` 表的方向一致，但 §5.1 的字面敘述和現狀不符。

7. **`sts.session.force_stop` 走的是 plane subject，不是 session 的控制 subject。** §3.1 把 stop／fail／status 都歸給 `sts.ctl.{session_id}`。`force_stop` 在代碼裡刻意不走那條路：`messages.py:1593`–`:1595` 的註解說「Parent-only. … The worker does not serve it: a blocked loop is the reason the first call timed out」，所以 API 送到 `Topics.sts(owner)`（`routes/sts.py:963`）。這不算計畫寫錯，但 §3.1 的表格會讓人以為 `sts.ctl` 是 session 控制的全部。

---

## 6. 統計

`messages.py` 裡以 `^[A-Z][A-Z0-9_]+ = ` 開頭的常數共 111 個，扣掉 8 個不是 wire 名稱的（`PROBE_MAX_AGE_SECONDS`、`LEASE_HEARTBEAT_INTERVAL_S`、`LEASE_MISS_LIMIT`、`STS_REASON_OPERATOR_STOP`、`STS_REASON_STOP_TIMED_OUT`、`ON_STOP_TIMEOUT_S`、`STOP_CONTROL_TIMEOUT_S`、`STOP_FORCE_RPC_TIMEOUT_S`）之後是 **103** 個。按前綴：MD 32、STS 29、TD 20、PAPER 16、SYM 5、API 1。

| 類別 | 數量 |
|---|---|
| 候選常數 | 103 |
| 其中不是 `Envelope.type`（是 subject 名稱） | 1（`PAPER`，和 `Topics.PAPER` 重複） |
| 其中只是別名 | 1（`STS_HEARTBEAT` = `STS_LEASE_HEARTBEAT`） |
| **實際的型別常數** | **102**（相異型別字串 101） |
| 有生產發送者也有接收者 | 95 |
| 純死代碼（兩邊都沒有） | 3（`TD_SESSION_LIST`、`SYM_HEALTH`、`STS_HEARTBEAT`） |
| 有接收者、沒有生產發送者 | 4（`MD_SUBSCRIBE`、`MD_UNSUBSCRIBE`、`MD_DETACH`、`STS_DETACH`） |
| 有發送者、沒有接收者 | 0（兩個這類的 wire type 都沒有常數，見下） |
| 上線但沒有常數的 wire type | 3（`"log"` 保留、`"heartbeat"` 刪除、`"td.global.keepalive"` 刪除） |

| 去向 | 數量 |
|---|---|
| 刪除 | 常數 14（`PAPER`、`STS_HEARTBEAT`、`STS_LEASE_HEARTBEAT`、`STS_RECON`、`STS_DETACH`、`STS_SESSION_FORCE_STOP`、`TD_SESSION_LIST`、`TD_LEASE_ACK`、`TD_RECON_DONE`、`MD_LEASE_ACK`、`MD_SUBSCRIBE`、`MD_UNSUBSCRIBE`、`MD_DETACH`、`SYM_HEALTH`）＋ 無常數的 wire type 2 |
| 改名 | 6（`sts.session.create`→`start`、`sts.session.stop`→`end`、`md.session.attach`／`detach`→`md.intent.put`／`delete`、`td.session.attach`／`detach`→`td.intent.put`／`delete`） |
| 保留型別、改 subject | 10（`md.orderbook`…`md.greeks`，改發在 `md.a.{venue}.{atom_hash}`） |
| 保留 | 其餘 72 |

| subject 類別 | 數量 |
|---|---|
| 實際在用的 subject 形狀（含 pattern 訂閱） | 29（request-reply 14、pub/sub 15） |
| 其中去向為刪除 | 3（`sts.td.{sid}`、`sts.md.{sid}`、`td.{api_id}.{sid}`）＋ `md.{session_id}` 改成 per-atom ＋ `sys.heartbeat` |
| 其中去向為改名 | 1（`sts.control.{sid}` → `sts.ctl.{sid}`） |
| 定義了但全 repo 零呼叫端的 `Topics` 成員 | 8（見 2.3） |
| 計畫全文沒有交代去向的 subject／型別群組 | 11（見 5.3） |

## 7. 重現這份盤點

```sh
git -C . diff --stat a0cbfb2 refactor/process-planes   # 只有三個 docs 檔有差

# 型別常數清單
rg -n '^[A-Z][A-Z0-9_]+ = ' packages/common/src/mftik/protocol/messages.py

# 某個型別的所有生產引用（排除測試與 re-export）
rg -l '\bMD_SUBSCRIBE\b' apps packages frontend contracts scripts \
  | grep -vE '(^|/)tests?/|test_|/messages\.py$|/__init__\.py$'

# 所有設定 type= 的發送點
rg -n 'type=(MD_|TD_|STS_|API_|SYM_|PAPER)[A-Z_]*' --glob '!**/tests/**' --glob '!test_*' -o apps packages scripts

# 所有字面值 wire type
rg -n 'type="[a-z][a-z0-9_.]*"' --glob '!**/tests/**' --glob '!test_*' -o apps packages scripts

# Topics 成員的使用次數
rg -n 'Topics\.[A-Za-z_]+' --glob '!**/tests/**' --glob '!test_*' -o apps packages scripts frontend \
  | sed 's/.*:Topics\./Topics./' | sort | uniq -c

# 所有 broker 收發點
rg -n '\.serve\(|\.subscribe\(|\.publish\(|\.request\(|\.probe\(' --glob '!**/tests/**' --glob '!test_*' apps packages scripts
```
