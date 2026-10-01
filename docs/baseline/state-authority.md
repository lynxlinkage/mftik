# state-authority — 現況的狀態權威表（as-is）（B0-04、issue #157）

> **基準：** `main` @ `a0cbfb2`（`ARCHITECTURE_CHANGE_PLAN.md` 的基準 commit）。B0-01 的 `arch/baseline` tag 還沒打，所以本文一律以 commit hash 稱呼基準。
>
> `refactor/process-planes` 相對 `a0cbfb2` 只多了文件檔，`apps/` 和 `packages/` 完全沒有差異，所以本文引用的行號在兩個 ref 上都成立。
>
> 「§」指 `ARCHITECTURE_CHANGE_PLAN.md` 的章節，「F」指同一份文件的決策編號。協定層的對照見 `docs/baseline/protocol.md`（B0-03）。

## 1. 盤點方法與用詞

TODO

## 2. 控制面（宣告）

| 狀態 | 權威（唯一寫入者） | 存放 | 讀取者 | 重啟或失聯後怎麼收斂 | 和 §3.3 的差異與負責的票 |
|---|---|---|---|---|---|
| session spec：策略、參數、`restart`、timeout | TODO | TODO | TODO | TODO | TODO |
| session status：phase、conditions、incarnation、`restart_count`、失敗原因 | TODO | TODO | TODO | TODO | TODO |
| MD intent：session 要哪些 feed 和 selector | TODO | TODO | TODO | TODO | TODO |
| TD intent：session 用哪些帳號 | TODO | TODO | TODO | TODO | TODO |
| 常駐訂閱 | TODO | TODO | TODO | TODO | TODO |
| `api_id` → instance 綁定、帳號設定 | TODO | TODO | TODO | TODO | TODO |
| listing：合約、到期、strike | TODO | TODO | TODO | TODO | TODO |

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
