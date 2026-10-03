# 封存索引

`docs/` 根目錄只留 §10 列出的架構文件與 `Deployment.md`。下表是 `docs/archive/` 裡的每一份文件：封存日期、§10 的類型，以及取代它的文件。類型沿用 `ARCHITECTURE_CHANGE_PLAN.md` §10 的三類。`README.md` 不在那三類裡，它是 2026-08-20 就封存的舊版倉庫 README（`2faf4bc`）。

取代文件指到計畫的具體章節，或寫明沒有對應章節。不寫「見計畫」。

| 檔名 | 原路徑 | 封存日期 | 類型 | 取代文件 |
|---|---|---|---|---|
| `JetStreamRemoval.md` | `docs/JetStreamRemoval.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §3.3（tape 在區域 Redis，ledger／OMS 在 TD）、§8.2（刪除 lease）、§8.3（傳輸維持 core NATS，不恢復 JetStream stream／KV） |
| `RedisRemoval.md` | `docs/RedisRemoval.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §3.3、§6.3（Redis 只作各 region 的 tape，不再是 `BrokerTransport`） |
| `BrokerProvisioning.md` | `docs/BrokerProvisioning.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §8.3（沒有 stream／KV 要在部署時遷移）。NATS 怎麼擺見 [Deployment.md](../Deployment.md)（B1-02） |
| `BrokerPatterns.md` | `docs/BrokerPatterns.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §8.2（刪除 `LeasedSessionLink`）、§8.3（協定對照）、§9.2（handler 與傳輸分開） |
| `MdHandover.md` | `docs/MdHandover.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §4.6（F22、F24）。§6.5 寫明由 §4.6 取代本文件 |
| `MdVenueSubscriptions.md` | `docs/MdVenueSubscriptions.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §6.1（推翻 I6）、§6.3（reconciler 取代 socket 內的訂閱帳） |
| `MdExpiry.md` | `docs/MdExpiry.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §6.4、§6.5（`_expiry_tasks` 刪除，到期改由 listing 驅動）、§8.3（`md.feed.end` 保留，對象改為 owner） |
| `MdOpenInterest.md` | `docs/MdOpenInterest.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §6.1（product topic 改成 atom；OI 與 ticker 共用的 channel 由 `decode` 拆成多個事件） |
| `StsPause.md` | `docs/StsPause.md` | 2026-10-03 | 舊模型的設計紀錄 | 無對應章節：計畫不提 session pause／`on_pause`，現行代碼也沒有這條路徑。本文件是未落地的移除提案 |
| `StsSessionList.md` | `docs/StsSessionList.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §5.2、§8.1（session 狀態與 start／end）。清單頁的分頁契約計畫沒有重寫 |
| `Instances.md` | `docs/Instances.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §3.1、§3.3、§8.3（instance subject 與 `health.*` 保留）、F36、F45。角色列舉與 unicast 閘門沒有另寫 |
| `EventLoop.md` | `docs/EventLoop.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §5.3 沿用 uvloop（ingress 與策略各一條）。「為什麼換、換了多少」的量測沒有收進計畫 |
| `Broker.md` | `docs/Broker.md` | 2026-10-03 | 舊模型的設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §3.4（`mftik.broker.handler`）、§8.3、§9.2 |
| `Alert.md` | `docs/Alert.md` | 2026-10-03 | 功能設計紀錄 | 崩潰與 crash-loop 告警見 [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §5.2（F10）與 F42。Discord webhook 的分層比對沒有對應章節 |
| `Artifact.md` | `docs/Artifact.md` | 2026-10-03 | 功能設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §5.7（F40）與 §3.3 的 artifacts 列 |
| `AuditIdentity.md` | `docs/AuditIdentity.md` | 2026-10-03 | 功能設計紀錄 | 無對應章節：計畫不寫審計列的 `via`／證明身分。`ARCHITECTURE.md`（B1-04，尚未完成）的萃取範圍也不含審計 |
| `Auth.md` | `docs/Auth.md` | 2026-10-03 | 功能設計紀錄 | 無對應章節：計畫不重寫認證。`ARCHITECTURE.md`（B1-04，尚未完成）的萃取範圍（§2.3、§3、§4、各平面、協定、測試摘要）不含這份模型 |
| `StrategyEnvironment.md` | `docs/StrategyEnvironment.md` | 2026-10-03 | 功能設計紀錄 | [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §5.7（F39：extras 的 `env_generation`、版本釘住與可部署檢查） |
| `CLI.md` | `docs/CLI.md` | 2026-10-03 | 功能設計紀錄 | 新指令見 [ARCHITECTURE_CHANGE_PLAN.md](../ARCHITECTURE_CHANGE_PLAN.md) §5.2、§8.1、F24、F27、F32、F46。profile、登入與 exit code 沒有對應章節 |
| `Deribit.md` | `docs/Deribit.md` | 2026-10-03 | venue 實測事實 | 無：實測事實，保留於封存（F28） |
| `BitgetUta.md` | `docs/BitgetUta.md` | 2026-10-03 | venue 實測事實 | 無：實測事實，保留於封存（F28） |
| `README.md` | `README.md`（倉庫根目錄） | 2026-08-20 | 舊版倉庫 README | [README.md](../../README.md)（現行根目錄；B1-03 #162 縮成指引，B10-05 定稿） |
